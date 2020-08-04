package internal

import (
	"github.com/jackc/pgx"
	"github.com/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/utility"
)

func HandleWalVerify(folder storage.Folder) {
	folder = folder.GetSubFolder(utility.WalPath)
	conn, err := Connect()
	tracelog.ErrorLogger.FatalfOnError("Failed to establish a connection to postgres", err)
	queryRunner, err := newPgQueryRunner(conn)
	tracelog.ErrorLogger.FatalfOnError("Failed to initialize PgQueryRunner", err)

	currentWalSegmentNo, err := getCurrentWalSegmentNo(queryRunner)
	tracelog.ErrorLogger.FatalfOnError("Failed to get current WAL segment number", err)
	currentTimeline, err := getCurrentTimeline(conn)
	tracelog.ErrorLogger.FatalfOnError("Failed to get current timeline", err)
	currentWalSegment := &WalSegmentDescription{timeline: currentTimeline, number: currentWalSegmentNo}

	// firstWalSegmentNo is the number of the first wal segment
	// in the wal segments sequence ending with currentWalSegmentNo
	// so we can do PITR starting from the backup in range [firstWalSegmentNo, currentWalSegmentNo]
	walSegmentRunner, err := newWalSegmentRunner(true, currentWalSegment, folder)
	tracelog.ErrorLogger.FatalfOnError("Failed to initialize WAL segment runner", err)

	// maxConcurrency is needed to determine max amount of missing WAL segments
	// after the last found WAL segment which can be skipped
	maxConcurrency, err := getMaxUploadConcurrency()
	tracelog.ErrorLogger.FatalOnError(err)
	tracelog.InfoLogger.Println("Started WAL segment sequence walk...")
	firstWalSegment, lastWalSegment, err := FindAvailableRange(walSegmentRunner, maxConcurrency)
	tracelog.ErrorLogger.FatalfOnError("Error during WAL segment sequence walk", err)

	tracelog.InfoLogger.Println("WAL SEGMENTS INFO")
	tracelog.InfoLogger.Println("Cluster current: " + currentWalSegment.GetFileName())
	tracelog.InfoLogger.Println("PITR: Storage last available: " + lastWalSegment.GetFileName())
	tracelog.InfoLogger.Println("PITR: Storage first available: " + firstWalSegment.GetFileName())

	//TODO: print available backups in the resulting WAL segments range
}

func FindAvailableRange(runner *WalSegmentRunner, maxSkippedSegmentsAfterLast int) (*WalSegmentDescription, *WalSegmentDescription, error) {
	// run to the latest WAL segment available in storage
	last, err := runToLastStorageSegment(runner)
	if err != nil {
		return nil, nil, err
	}
	// then run to the first WAL segment
	first, err := runToFirstStorageSegment(runner, maxSkippedSegmentsAfterLast)
	if err != nil {
		return nil, nil, err
	}
	return first, last, nil
}

func runToFirstStorageSegment(runner *WalSegmentRunner, maxSkippedSegmentCountAfterStart int) (*WalSegmentDescription, error) {
	startSegment := runner.GetCurrentWalSegment()
	for runner.currentWalSegment.number > 0 {
		nextSegment, err := runner.GetNext()
		// WalSegmentNotFoundError means we reached the end of continuous WAL segments sequence
		if _, ok := err.(WalSegmentNotFoundError); ok {
			// check if we can ignore the missing segment
			nextSegmentNo := runner.GetCurrentWalSegment().number - 1
			if int(startSegment.number - nextSegmentNo) < maxSkippedSegmentCountAfterStart {
				runner.ForceSwitchToNextSegment()
				tracelog.WarningLogger.Println("runToFirstStorageSegment: Skipped segment " +
					runner.GetCurrentWalSegment().GetFileName())
			}
			return runner.GetCurrentWalSegment(), nil
		}
		if err != nil {
			return nil, err
		}
		tracelog.DebugLogger.Println("Walked segment " + nextSegment.GetFileName())
	}
	// zero segment number ???
	return runner.GetCurrentWalSegment(), nil
}

func runToLastStorageSegment(runner *WalSegmentRunner) (*WalSegmentDescription, error) {
	for runner.currentWalSegment.number > 0 {
		nextSegment, err := runner.GetNext()
		if _, ok := err.(WalSegmentNotFoundError); ok {
			// force switch to previous WAL segment
			tracelog.WarningLogger.Println("runToLastStorageSegment: Skipped segment " +
				runner.currentWalSegment.GetFileName())
			runner.ForceSwitchToNextSegment()
			continue
		}
		return nextSegment, err
	}
	// zero segment number ???
	return runner.GetCurrentWalSegment(), nil
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
