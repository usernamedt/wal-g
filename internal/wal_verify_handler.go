package internal

import (
	"fmt"
	"github.com/jackc/pgx"
	"github.com/pkg/errors"
	"github.com/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/utility"
)

type LastSegmentMissingError struct {
	error
}

func newLastSegmentMissingError() LastSegmentMissingError {
	return LastSegmentMissingError{errors.Errorf("Could not reach the last element of skippable segment sequence.\n")}
}

func (err LastSegmentMissingError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

func HandleWalVerify(folder storage.Folder) {
	folder = folder.GetSubFolder(utility.WalPath)
	conn, err := Connect()
	tracelog.ErrorLogger.FatalfOnError("Failed to establish a connection to postgres", err)
	queryRunner, err := newPgQueryRunner(conn)
	tracelog.ErrorLogger.FatalfOnError("Failed to initialize PgQueryRunner", err)

	currentSegmentNo, err := getCurrentWalSegmentNo(queryRunner)
	tracelog.ErrorLogger.FatalfOnError("Failed to get current WAL segment number", err)
	currentTimeline, err := getCurrentTimeline(conn)
	tracelog.ErrorLogger.FatalfOnError("Failed to get current timeline", err)
	currentSegment := &WalSegmentDescription{timeline: currentTimeline, number: currentSegmentNo}

	walSegmentRunner, err := newWalSegmentRunner(true, currentSegment, folder)
	tracelog.ErrorLogger.FatalfOnError("Failed to initialize WAL segment runner", err)

	// maxConcurrency is needed to determine max amount of missing WAL segments
	// after the last found WAL segment which can be skipped
	maxConcurrency, err := getMaxUploadConcurrency()
	tracelog.ErrorLogger.FatalOnError(err)
	tracelog.InfoLogger.Println("Started WAL segment sequence walk...")

	// [firstSegment, lastSegment] is the continuous WAL segment range with no missing segments
	firstSegment, lastSegment, err := GetPitrRange(walSegmentRunner, maxConcurrency)
	tracelog.ErrorLogger.FatalfOnError("Error during WAL segment sequence walk", err)

	tracelog.InfoLogger.Println("WAL SEGMENTS INFO")
	tracelog.InfoLogger.Println("Cluster current: " + currentSegment.GetFileName())
	tracelog.InfoLogger.Println("PITR: Storage last available: " + lastSegment.GetFileName())
	tracelog.InfoLogger.Println("PITR: Storage first available: " + firstSegment.GetFileName())

	//TODO: print available backups in the resulting WAL segments range
	// We can do PITR starting from the backup in range [firstSegment, lastSegment]
}

func GetPitrRange(runner *WalSegmentRunner, uploadingSegmentRangeSize int) (*WalSegmentDescription, *WalSegmentDescription, error) {
	// Run to the latest WAL segment available in storage
	err := runToLastStorageSegment(runner)
	if err != nil {
		return nil, nil, err
	}
	rangeStart := runner.GetCurrentWalSegment()

	// "Uploading" segment sequence may contain missing segments because they are still being uploaded.
	// Last element of "uploading" segments sequence should always exist and can not be skipped.
	// New rangeStart might be chosen if there is some skipped segments after current rangeStart
	// because we need a continuous sequence for PITR.
	// lastAvailableSegment only used in case of error
	rangeStart, lastAvailableSegment, err := traverseUploadingSegments(runner, uploadingSegmentRangeSize)
	if _, ok := err.(LastSegmentMissingError); ok {
		// LastSegmentMissingError means that the last segment in uploading WAL segments sequence
		// is not available in storage so we need to return range [rangeStart, lastAvailableSegment]
		return rangeStart, lastAvailableSegment, nil
	}
	if err != nil {
		return nil, nil, err
	}
	// Run to the first storage WAL segment (in sequence)
	err = runToFirstStorageSegment(runner)
	if err != nil {
		return nil, nil, err
	}
	rangeEnd := runner.GetCurrentWalSegment()
	return rangeEnd, rangeStart, nil
}

func traverseUploadingSegments(runner *WalSegmentRunner, maxSegmentsToSkip int) (*WalSegmentDescription, *WalSegmentDescription, error) {
	firstExistingSegment := runner.GetCurrentWalSegment()
	lastExistingSegment := runner.GetCurrentWalSegment()
	previousSegmentExists := true
	for i := 0; i < maxSegmentsToSkip; i++ {
		nextSegment, err := runner.MoveNext()
		// WalSegmentNotFoundError means we reached the end of continuous WAL segments sequence
		if _, ok := err.(WalSegmentNotFoundError); ok {
			runner.ForceSwitchToNextSegment()
			previousSegmentExists = false
			tracelog.WarningLogger.Println("runToFirstStorageSegment: Skipped segment " +
				runner.GetCurrentWalSegment().GetFileName())
			continue
		}
		if err != nil {
			return nil, nil, err
		}
		if !previousSegmentExists {
			firstExistingSegment = nextSegment
			previousSegmentExists = true
		}
		lastExistingSegment = nextSegment
		tracelog.DebugLogger.Println("Walked segment " + nextSegment.GetFileName())
	}
	if !previousSegmentExists {
		// Last segment needs to exist and can't be skipped
		// so we should throw an error and return continuous segments sequence
		return firstExistingSegment, lastExistingSegment, newLastSegmentMissingError()
	}
	return firstExistingSegment, lastExistingSegment, nil
}

func runToFirstStorageSegment(runner *WalSegmentRunner) error {
	for runner.currentWalSegment.number > 0 {
		nextSegment, err := runner.MoveNext()
		// WalSegmentNotFoundError means we reached the end of continuous WAL segments sequence
		if _, ok := err.(WalSegmentNotFoundError); ok {
			return nil
		}
		if err != nil {
			return err
		}
		tracelog.DebugLogger.Println("Walked segment " + nextSegment.GetFileName())
	}
	// zero segment number ???
	return nil
}

func runToLastStorageSegment(runner *WalSegmentRunner) error {
	for runner.currentWalSegment.number > 0 {
		_, err := runner.MoveNext()
		if _, ok := err.(WalSegmentNotFoundError); ok {
			// force switch to previous WAL segment
			tracelog.WarningLogger.Println("runToLastStorageSegment: Skipped segment " +
				runner.currentWalSegment.GetFileName())
			runner.ForceSwitchToNextSegment()
			continue
		}
		return err
	}
	// zero segment number ???
	return nil
}

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

func getCurrentTimeline(conn *pgx.Conn) (uint32, error) {
	timeline, err := readTimeline(conn)
	if err != nil {
		return 0, err
	}
	return timeline, nil
}
