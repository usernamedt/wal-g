package pg

import (
	"fmt"
	"github.com/wal-g/wal-g/utility"

	"github.com/wal-g/wal-g/internal/databases/postgres"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
)

const (
	backupFetchShortDescription = "Fetches a backup from storage"

	MaskFlag = "mask"
	RestoreSpecFlag = "restore-spec"
	ReverseDeltaUnpackFlag = "reverse-unpack"
	SkipRedundantTarsFlag = "skip-redundant-tars"
	TargetUserDataFlag = "target-user-data"

	MaskFlagDescription         = `Fetches only files which path relative to destination_directory
matches given shell file pattern.
For information about pattern syntax view: https://golang.org/pkg/path/filepath/#Match`
	RestoreSpecDescription        = "Path to file containing tablespace restore specification"
	ReverseDeltaUnpackDescription = "Unpack delta backups in reverse order (beta feature)"
	SkipRedundantTarsDescription  = "Skip tars with no useful data (requires reverse delta unpack)"
	TargetUserDataDescription     = "Fetch storage backup which has the specified user data"
)

var fileMask string
var restoreSpec string
var reverseDeltaUnpack bool
var skipRedundantTars bool
var fetchTargetUserData string

var backupFetchCmd = &cobra.Command{
	Use:   "backup-fetch destination_directory [backup_name | --target-user-data <data>]",
	Short: backupFetchShortDescription, // TODO : improve description
	Args:  cobra.RangeArgs(1, 2),
	Run: func(cmd *cobra.Command, args []string) {
		if fetchTargetUserData == "" {
			fetchTargetUserData = viper.GetString(internal.FetchTargetUserDataSetting)
		}
		targetBackupSelector, err := CreateTargetFetchBackupSelector(cmd, args, fetchTargetUserData)
		tracelog.ErrorLogger.FatalOnError(err)

		folder, err := internal.ConfigureFolder()
		tracelog.ErrorLogger.FatalOnError(err)

		var pgFetcher func(folder storage.Folder, backup internal.Backup)
		reverseDeltaUnpack = reverseDeltaUnpack || viper.GetBool(internal.UseReverseUnpackSetting)
		skipRedundantTars = skipRedundantTars || viper.GetBool(internal.SkipRedundantTarsSetting)
		if reverseDeltaUnpack {
			pgFetcher = postgres.GetPgFetcherNew(args[0], fileMask, restoreSpec, skipRedundantTars)
		} else {
			pgFetcher = postgres.GetPgFetcherOld(args[0], fileMask, restoreSpec)
		}

		internal.HandleBackupFetch(folder, targetBackupSelector, utility.BaseBackupPath, pgFetcher)
	},
}

// CreateTargetFetchBackupSelector creates the BackupSelector to select the backup to fetch
func CreateTargetFetchBackupSelector(cmd *cobra.Command,
	args []string, targetUserData string) (internal.BackupSelector, error) {
	targetName := ""
	if len(args) >= 2 {
		targetName = args[1]
	}

	backupSelector, err := internal.NewTargetBackupSelector(targetUserData, targetName, postgres.NewGenericMetaFetcher())
	if err != nil {
		fmt.Println(cmd.UsageString())
		return nil, err
	}
	return backupSelector, nil
}

func init() {
	backupFetchCmd.Flags().StringVar(&fileMask, MaskFlag, "", MaskFlagDescription)
	backupFetchCmd.Flags().StringVar(&restoreSpec, RestoreSpecFlag, "", RestoreSpecDescription)
	backupFetchCmd.Flags().BoolVar(&reverseDeltaUnpack, ReverseDeltaUnpackFlag, false, ReverseDeltaUnpackDescription)
	backupFetchCmd.Flags().BoolVar(&skipRedundantTars, SkipRedundantTarsFlag, false, SkipRedundantTarsDescription)
	backupFetchCmd.Flags().StringVar(&fetchTargetUserData, TargetUserDataFlag, "", TargetUserDataDescription)
	cmd.AddCommand(backupFetchCmd)
}
