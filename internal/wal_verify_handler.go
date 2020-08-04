package internal

import (
	"fmt"
	"github.com/jackc/pgx"
	"github.com/pkg/errors"
	"github.com/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/utility"
	"os"
	"runtime/pprof"
)

type SequenceTraverseError struct {
	error
}

func newSequenceTraverseError() SequenceTraverseError {
	return SequenceTraverseError{errors.Errorf("Could not reach the last element of skippable segment sequence.\n")}
}

func (err SequenceTraverseError) Error() string {
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
	// currentSegment is the current WAL segment of the cluster
	currentSegment := &WalSegmentDescription{timeline: currentTimeline, number: currentSegmentNo}

	fileNames, err := getWalFolderFilenames(folder)
	tracelog.ErrorLogger.FatalfOnError("Failed to get wal folder filenames", err)
	walSegmentRunner, err := NewWalSegmentRunner(true, currentSegment, folder, fileNames)
	tracelog.ErrorLogger.FatalfOnError("Failed to initialize WAL segment runner", err)

	// maxConcurrency is needed to determine max amount of missing WAL segments
	// after the last found WAL segment which can be skipped ("uploading" segment sequence size)
	maxConcurrency, err := getMaxUploadConcurrency()
	tracelog.ErrorLogger.FatalOnError(err)
	tracelog.InfoLogger.Println("Cluster current WAL segment: " + currentSegment.GetFileName())
	tracelog.InfoLogger.Println("Started WAL segment sequence walk...")
	cpuProfile, _ := os.Create("cpuprofile")
	memProfile, _ := os.Create("memprofile")
	pprof.StartCPUProfile(cpuProfile)

	// TODO: print available backups in the resulting WAL segments range
	// We can do PITR starting from the backup in range [firstSegment, lastSegment]
	_, _, err = ProcessSinglePitrSequence(walSegmentRunner, maxConcurrency)
	tracelog.ErrorLogger.FatalfOnError("Error during WAL segment sequence walk", err)
	for {
		_, _, err = ProcessSinglePitrSequence(walSegmentRunner, 0)
		if _, ok := err.(ReachedZeroSegmentError); ok {
			tracelog.InfoLogger.Println("Reached zero WAL segment. Exiting...")
			break
		}
		tracelog.ErrorLogger.FatalfOnError("Error during WAL segment sequence walk", err)
	}
	pprof.StopCPUProfile()
	pprof.WriteHeapProfile(memProfile)
}

func ProcessSinglePitrSequence(runner *WalSegmentRunner, maxUploadSequenceSize int) (*WalSegmentDescription, *WalSegmentDescription, error) {
	// [firstSegment, lastSegment] is the continuous WAL segment range with no missing segments
	firstSegment, lastSegment, err := GetPitrRange(runner, maxUploadSequenceSize)
	if err != nil {
		return nil, nil, err
	}

	tracelog.InfoLogger.Println("WAL segments continuous sequence START: " + lastSegment.GetFileName())
	tracelog.InfoLogger.Println("WAL segments continuous sequence END: " + firstSegment.GetFileName())
	return nil, nil, err
}

func GetPitrRange(runner *WalSegmentRunner, uploadingSegmentRangeSize int) (*WalSegmentDescription, *WalSegmentDescription, error) {
	// Run to the latest WAL segment available in storage
	err := runToLastStorageSegment(runner)
	if err != nil {
		return nil, nil, err
	}
	rangeStart := runner.GetCurrent()

	// "Uploading" segment sequence may contain missing segments because they are still being uploaded.
	// Last element of "uploading" segments sequence should always exist and can not be skipped.
	// New rangeStart might be chosen if there is some skipped segments after current rangeStart
	// because we need a continuous sequence for PITR.
	// lastAvailableSegment only used in case of error
	rangeStart, lastAvailableSegment, err := traverseUploadingSegments(runner, uploadingSegmentRangeSize)
	if _, ok := err.(SequenceTraverseError); ok {
		// SequenceTraverseError means that the last segment in uploading WAL segments sequence
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
	rangeEnd := runner.GetCurrent()
	return rangeEnd, rangeStart, nil
}

func traverseUploadingSegments(runner *WalSegmentRunner, maxSegmentsToSkip int) (*WalSegmentDescription, *WalSegmentDescription, error) {
	firstExistingSegment := runner.GetCurrent()
	lastExistingSegment := runner.GetCurrent()
	previousSegmentExists := true
	for i := 0; i < maxSegmentsToSkip; i++ {
		nextSegment, err := runner.MoveNext()
		if err != nil {
			switch err := err.(type) {
			case WalSegmentNotFoundError:
				// WalSegmentNotFoundError means we reached the end of continuous WAL segments sequence
				runner.ForceSwitchToNext()
				previousSegmentExists = false
				tracelog.WarningLogger.Println("traverseUploadingSegments: Skipped missing segment " +
					runner.GetCurrent().GetFileName())
				continue
			case ReachedZeroSegmentError:
				// Can't continue because reached segment with zero number
				// so throw an error and return continuous segments sequence
				return firstExistingSegment, lastExistingSegment, newSequenceTraverseError()
			default:
				return nil, nil, err
			}
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
		return firstExistingSegment, lastExistingSegment, newSequenceTraverseError()
	}
	return firstExistingSegment, lastExistingSegment, nil
}

func runToFirstStorageSegment(runner *WalSegmentRunner) error {
	for {
		nextSegment, err := runner.MoveNext()
		if err != nil {
			switch err := err.(type) {
			case WalSegmentNotFoundError:
				// WalSegmentNotFoundError means we reached the end of continuous WAL segments sequence.
				// Stop at this point
				return nil
			case ReachedZeroSegmentError:
				// Can't continue because reached segment with zero number, stop at this point
				return nil
			default:
				return err
			}
		}
		tracelog.DebugLogger.Println("Walked segment " + nextSegment.GetFileName())
	}
}

func runToLastStorageSegment(runner *WalSegmentRunner) error {
	for {
		if _, err := runner.MoveNext(); err != nil {
			if _, ok := err.(WalSegmentNotFoundError); ok {
				// force switch to the previous WAL segment
				runner.ForceSwitchToNext()
				tracelog.WarningLogger.Println("runToLastStorageSegment: Skipped missing segment " +
					runner.currentWalSegment.GetFileName())
				continue
			}
			return err
		}
		return nil
	}
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
