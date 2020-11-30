package internal

import (
	"bytes"
	"fmt"
	"github.com/jackc/pgx"
	"github.com/pkg/errors"
	"github.com/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/utility"
)

type WalVerifyResult struct {
	WalIntegrityCheckResult WalIntegrityCheckResult `json:"wal_integrity_check"`
	TimelineVerifyResult    TimelineCheckResult     `json:"timeline_check"`
}

func newWalVerifyResult(
	walIntegrityCheckResult WalIntegrityCheckResult,
	timelineVerifyResult TimelineCheckResult,
) WalVerifyResult {
	return WalVerifyResult{
		WalIntegrityCheckResult: walIntegrityCheckResult,
		TimelineVerifyResult:    timelineVerifyResult}
}

type NoCorrectBackupFoundError struct {
	error
}

func newNoCorrectBackupFoundError() NoCorrectBackupFoundError {
	return NoCorrectBackupFoundError{errors.Errorf("Could not find any correct backup in storage")}
}

func (err NoCorrectBackupFoundError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

// QueryCurrentWalSegment() gets start WAL segment from Postgres cluster
func QueryCurrentWalSegment() WalSegmentDescription {
	conn, err := Connect()
	tracelog.ErrorLogger.FatalfOnError("Failed to establish a connection to Postgres cluster %v", err)

	queryRunner, err := newPgQueryRunner(conn)
	tracelog.ErrorLogger.FatalfOnError("Failed to initialize PgQueryRunner %v", err)

	currentSegmentNo, err := getCurrentWalSegmentNo(queryRunner)
	tracelog.ErrorLogger.FatalfOnError("Failed to get current WAL segment number %v", err)

	currentTimeline, err := getCurrentTimeline(conn)
	tracelog.ErrorLogger.FatalfOnError("Failed to get current timeline %v", err)

	err = conn.Close()
	tracelog.WarningLogger.PrintOnError(err)

	// currentSegment is the current WAL segment of the cluster
	return WalSegmentDescription{Timeline: currentTimeline, Number: currentSegmentNo}
}

// HandleWalVerify queries the current cluster WAL segment and timeline
// and travels through WAL segments in storage in reversed chronological order (starting from that segment)
// to find any missing WAL segments that could potentially fail the PITR procedure
func HandleWalVerify(rootFolder storage.Folder, currentWalSegment WalSegmentDescription, outputWriter WalVerifyOutputWriter) {
	walFolder := rootFolder.GetSubFolder(utility.WalPath)
	storageFileNames, err := getFolderFilenames(walFolder)
	tracelog.ErrorLogger.FatalfOnError("Failed to get WAL folder filenames %v", err)

	// check that current timeline is the newest (highest)
	timelineCheckResult := verifyCurrentTimeline(currentWalSegment.Timeline, storageFileNames)

	timelineSwitchMap, err := createTimelineSwitchMap(currentWalSegment.Timeline, walFolder)
	tracelog.ErrorLogger.FatalfOnError("Failed to initialize timeline history map %v", err)

	stopWalSegmentNo, err := getEarliestBackupStartSegmentNo(timelineSwitchMap, currentWalSegment.Timeline, rootFolder)
	if err != nil {
		tracelog.WarningLogger.Printf("Failed to detect earliest backup WAL segment no: '%v',"+
			"will scan until the 000000010000000000000001 segment.\n", err)
		stopWalSegmentNo = 1
	}

	// uploadingSegmentRangeSize is needed to determine max amount of missing WAL segments
	// after the last found WAL segment which can be marked as "uploading"
	uploadingSegmentRangeSize, err := getMaxUploadConcurrency()
	tracelog.ErrorLogger.FatalOnError(err)

	// check that WAL segments range [stopWalSegmentNo, currentWalSegment.Number] has no gaps
	walIntegrityCheckResult, err := verifyWalIntegrity(storageFileNames, currentWalSegment,
		stopWalSegmentNo, timelineSwitchMap, uploadingSegmentRangeSize)
	tracelog.ErrorLogger.FatalfOnError("Failed to verify WAL integrity: %v", err)

	err = outputWriter.Write(newWalVerifyResult(walIntegrityCheckResult, timelineCheckResult))
	tracelog.ErrorLogger.FatalOnError(err)
}

// get the current wal segment number of the cluster
func getCurrentWalSegmentNo(queryRunner *PgQueryRunner) (WalSegmentNo, error) {
	lsnStr, err := queryRunner.getCurrentLsn()
	if err != nil {
		return 0, err
	}
	lsn, err := pgx.ParseLSN(lsnStr)
	if err != nil {
		return 0, err
	}
	return newWalSegmentNo(lsn - 1), nil
}

// get the current timeline of the cluster
func getCurrentTimeline(conn *pgx.Conn) (uint32, error) {
	timeline, err := readTimeline(conn)
	if err != nil {
		return 0, err
	}
	return timeline, nil
}

// getEarliestBackupStartSegmentNo returns the starting segmentNo of the earliest available correct backup
func getEarliestBackupStartSegmentNo(timelineSwitchMap map[WalSegmentNo]*TimelineHistoryRecord,
	currentTimeline uint32,
	rootFolder storage.Folder) (WalSegmentNo, error) {
	backups, err := GetBackups(rootFolder)
	if err != nil {
		return 0, err
	}

	backupDetails, err := GetBackupsDetails(rootFolder, backups)
	if err != nil {
		return 0, err
	}

	// switchLsnByTimeline is used for fast lookup of the timeline switch LSN
	switchLsnByTimeline := make(map[uint32]uint64, len(timelineSwitchMap))
	for _, historyRecord := range timelineSwitchMap {
		switchLsnByTimeline[historyRecord.timeline] = historyRecord.lsn
	}
	earliestBackup, err := findEarliestBackup(currentTimeline, backupDetails, switchLsnByTimeline)
	if err != nil {
		return 0, err
	}

	tracelog.InfoLogger.Printf("Detected earliest available backup: %s\n",
		earliestBackup.BackupName)
	return newWalSegmentNo(earliestBackup.StartLsn), nil
}

// findEarliestBackup finds earliest correct backup available in storage.
func findEarliestBackup(
	currentTimeline uint32,
	backupDetails []BackupDetail,
	switchLsnByTimeline map[uint32]uint64,
) (*BackupDetail, error) {
	var earliestBackup *BackupDetail
	for _, backup := range backupDetails {
		backupTimelineId, err := ParseTimelineFromBackupName(backup.BackupName)
		if err != nil {
			return nil, err
		}

		if ok := checkBackupIsCorrect(currentTimeline, backup.BackupName,
			backupTimelineId, backup.StartLsn, switchLsnByTimeline); !ok {
			continue
		}

		if earliestBackup == nil || earliestBackup.StartLsn > backup.StartLsn {
			// create local variable so the reference won't break
			newEarliestBackup := backup
			earliestBackup = &newEarliestBackup
		}
	}
	if earliestBackup == nil {
		return nil, newNoCorrectBackupFoundError()
	}
	return earliestBackup, nil
}

// checkBackupIsCorrect checks that backup start LSN is valid.
// Backup start LSN is considered valid if
// it belongs to range [backup timeline start LSN, backup timeline end LSN]
func checkBackupIsCorrect(
	currentTimeline uint32,
	backupName string,
	backupTimeline uint32,
	backupStartLsn uint64,
	switchLsnByTimeline map[uint32]uint64,
) bool {
	// perform the check only if .history file exists
	if len(switchLsnByTimeline) > 0 {
		// if backup start LSN is less than timeline start LSN => incorrect backup
		if backupTimeline > 1 {
			backupTimelineStartLsn, ok := switchLsnByTimeline[backupTimeline-1]
			if ok && backupStartLsn < backupTimelineStartLsn {
				tracelog.WarningLogger.Printf(
					"checkBackupIsCorrect: %s: backup start LSN %d "+
						"is less than the backup timeline start LSN.\n",
					backupName, backupStartLsn)
				return false
			}
		}

		// if backup belongs to the current timeline, skip the rest of the checks
		if backupTimeline == currentTimeline {
			return true
		}

		// if backup timeline is not present in current .history file => incorrect backup
		timelineSwitchLsn, ok := switchLsnByTimeline[backupTimeline]
		if !ok {
			tracelog.WarningLogger.Printf(
				"checkBackupIsCorrect: %s: backup timeline %d "+
					"is not present in .history file and is not current.\n",
				backupName, backupTimeline)
			return false
		}

		// if backup start LSN is higher than switch LSN of the previous timeline => incorrect backup
		if backupStartLsn >= timelineSwitchLsn {
			tracelog.WarningLogger.Printf(
				"checkBackupIsCorrect: %s: backup start LSN %d "+
					"is higher than the backup timeline end LSN.\n",
				backupName, backupStartLsn)
			return false
		}
	}
	return true
}

// marshalEnumToJSON is used to write the string enum representation
// instead of int enum value to JSON
func marshalEnumToJSON(enum fmt.Stringer) ([]byte, error) {
	buffer := bytes.NewBufferString(fmt.Sprintf(`"%s"`, enum))
	return buffer.Bytes(), nil
}
