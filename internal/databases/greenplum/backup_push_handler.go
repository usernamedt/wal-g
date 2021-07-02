package greenplum

import (
	"encoding/json"
	"fmt"
	"path"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/blang/semver"
	"github.com/greenplum-db/gp-common-go-libs/cluster"
	"github.com/greenplum-db/gp-common-go-libs/gplog"
	"github.com/jackc/pgx"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/postgres"
	"github.com/wal-g/wal-g/utility"
)

// BackupArguments holds all arguments parsed from cmd to this handler class
type BackupArguments struct {
	isPermanent    bool
	userData       string
	segmentFwdArgs []SegmentFwdArg
	segmentCfgPath string
}

type SegmentUserData struct {
	ContentID int `json:"content_id"`
}

func NewSegmentUserData(contentID int) SegmentUserData {
	return SegmentUserData{ContentID: contentID}
}

// QuotedString will do json.Marshal-ing followed by quoting in order to escape special control characters
// in the resulting JSON so it can be transferred as the cmdline argument to a segment
func (d SegmentUserData) QuotedString() string {
	unescapedJson, err := json.Marshal(d)
	if err != nil {
		panic(err)
	}
	return strconv.Quote(string(unescapedJson))
}

// SegmentFwdArg describes the specific WAL-G
// arguments that is going to be forwarded to the segments
type SegmentFwdArg struct {
	Name  string
	Value string
}

// BackupWorkers holds the external objects that the handler uses to get the backup data / write the backup data
type BackupWorkers struct {
	Uploader *internal.Uploader
	Conn     *pgx.Conn
}

// CurBackupInfo holds all information that is harvest during the backup process
type CurBackupInfo struct {
	backupName    string
	pgBackupNames []string
}

// BackupHandler is the main struct which is handling the backup process
type BackupHandler struct {
	arguments     BackupArguments
	workers       BackupWorkers
	globalCluster *cluster.Cluster
	curBackupInfo CurBackupInfo
}

func (bh *BackupHandler) buildCommand(contentID int) string {
	segment := bh.globalCluster.ByContent[contentID][0]
	cmd := []string{
		"WALG_LOG_LEVEL=DEVEL",
		fmt.Sprintf("PGPORT=%d", segment.Port),
		"wal-g",
		fmt.Sprintf("backup-push %s", segment.DataDir),
		fmt.Sprintf("--add-user-data=%s", NewSegmentUserData(contentID).QuotedString()),
		fmt.Sprintf("--config=%s", bh.formatConfigPath(contentID)),
	}

	for _, arg := range bh.arguments.segmentFwdArgs {
		cmd = append(cmd, fmt.Sprintf("--%s=%s", arg.Name, arg.Value))
	}

	cmdLine := strings.Join(cmd, " ")
	tracelog.DebugLogger.Printf("Command to run on segment %d: %s", contentID, cmdLine)
	return cmdLine
}

// HandleBackupPush handles the backup being read from filesystem and being pushed to the repository
func (bh *BackupHandler) HandleBackupPush() {
	folder := bh.workers.Uploader.UploadingFolder
	bh.workers.Uploader.UploadingFolder = folder.GetSubFolder(utility.BaseBackupPath)
	bh.curBackupInfo.backupName = "backup" + time.Now().Format(utility.BackupTimeFormat)

	tracelog.InfoLogger.Println("Running wal-g on segments")
	gplog.InitializeLogging("wal-g", "")
	remoteOutput := bh.globalCluster.GenerateAndExecuteCommand("Running wal-g",
		cluster.ON_SEGMENTS|cluster.INCLUDE_MASTER,
		func(contentID int) string {
			return bh.buildCommand(contentID)
		})
	bh.globalCluster.CheckClusterError(remoteOutput, "Unable to run wal-g", func(contentID int) string {
		return "Unable to run wal-g"
	})

	err := bh.connect()
	tracelog.ErrorLogger.FatalOnError(err)
	err = bh.createRestorePoint(bh.curBackupInfo.backupName)
	tracelog.ErrorLogger.FatalOnError(err)

	err = bh.extractPgBackupNames()
	tracelog.ErrorLogger.FatalOnError(err)
	sentinelDto := NewBackupSentinelDto(bh.curBackupInfo)
	tracelog.InfoLogger.Println("Uploading sentinel file")
	tracelog.DebugLogger.Println(sentinelDto.String())
	err = internal.UploadSentinel(bh.workers.Uploader, sentinelDto, bh.curBackupInfo.backupName)
	if err != nil {
		tracelog.ErrorLogger.Printf("Failed to upload sentinel file for backup: %s", bh.curBackupInfo.backupName)
		tracelog.ErrorLogger.FatalError(err)
	}
}

func (bh *BackupHandler) extractPgBackupNames() (err error) {
	backupNames := make([]string, 0)
	objects, _, err := bh.workers.Uploader.UploadingFolder.ListFolder()
	if err != nil {
		return err
	}
	patternBackupSentinelName := fmt.Sprintf("%s_seg-?[0-9]+_base_%[2]s(_D_%[2]s)?_backup_stop_sentinel.json",
		bh.curBackupInfo.backupName, postgres.PatternTimelineAndLogSegNo)
	regexpBackupSentinelName := regexp.MustCompile(patternBackupSentinelName)
	for _, obj := range objects {
		matched := regexpBackupSentinelName.FindString(obj.GetName())
		if matched != "" {
			backupNames = append(backupNames, postgres.FetchPgBackupName(obj))
		}
	}
	bh.curBackupInfo.pgBackupNames = backupNames
	return
}

func (bh *BackupHandler) connect() (err error) {
	tracelog.DebugLogger.Println("Connecting to Greenplum master.")
	bh.workers.Conn, err = postgres.Connect()
	if err != nil {
		return
	}
	return
}

func (bh *BackupHandler) createRestorePoint(restorePointName string) (err error) {
	tracelog.InfoLogger.Printf("Creating restore point with name %s", restorePointName)
	queryRunner, err := NewPgQueryRunner(bh.workers.Conn)
	if err != nil {
		return
	}
	_, err = queryRunner.CreateGreenplumRestorePoint(restorePointName)
	return
}

func (bh *BackupHandler) formatConfigPath(contentID int) string {
	configName := fmt.Sprintf("wal-g-seg%s.yaml", strconv.Itoa(contentID))
	return path.Join(bh.arguments.segmentCfgPath, configName)
}

func getGpCluster() (globalCluster *cluster.Cluster, err error) {
	tracelog.DebugLogger.Println("Initializing tmp connection to read Greenplum info")
	tmpConn, err := postgres.Connect()
	if err != nil {
		return globalCluster, err
	}

	queryRunner, err := NewPgQueryRunner(tmpConn)
	if err != nil {
		return globalCluster, err
	}

	versionStr, err := queryRunner.GetGreenplumVersion()
	if err != nil {
		return globalCluster, err
	}
	tracelog.DebugLogger.Printf("Greenplum version: %s", versionStr)
	versionStart := strings.Index(versionStr, "(Greenplum Database ") + len("(Greenplum Database ")
	versionEnd := strings.Index(versionStr, ")")
	versionStr = versionStr[versionStart:versionEnd]
	pattern := regexp.MustCompile(`\d+\.\d+\.\d+`)
	threeDigitVersion := pattern.FindStringSubmatch(versionStr)[0]
	semVer, err := semver.Make(threeDigitVersion)
	if err != nil {
		return globalCluster, err
	}

	segConfigs, err := queryRunner.GetGreenplumSegmentsInfo(semVer)
	if err != nil {
		return globalCluster, err
	}
	globalCluster = cluster.NewCluster(segConfigs)

	return globalCluster, nil
}

// NewBackupHandler returns a backup handler object, which can handle the backup
func NewBackupHandler(arguments BackupArguments) (bh *BackupHandler, err error) {
	uploader, err := internal.ConfigureUploader()
	if err != nil {
		return bh, err
	}
	globalCluster, err := getGpCluster()
	if err != nil {
		return bh, err
	}

	bh = &BackupHandler{
		arguments: arguments,
		workers: BackupWorkers{
			Uploader: uploader,
		},
		globalCluster: globalCluster,
	}
	return bh, err
}

// NewBackupArguments creates a BackupArgument object to hold the arguments from the cmd
func NewBackupArguments(isPermanent bool, userData string, fwdArgs []SegmentFwdArg, segmentCfgPath string) BackupArguments {
	return BackupArguments{
		isPermanent:    isPermanent,
		userData:       userData,
		segmentCfgPath: segmentCfgPath,
		segmentFwdArgs: fwdArgs,
	}
}
