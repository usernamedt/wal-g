package internal

import (
	"fmt"
	"github.com/jackc/pgx"
	"github.com/pkg/errors"
	"github.com/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/utility"
	"regexp"
)

var walSegmentNameRegexp *regexp.Regexp

func init() {
	walSegmentNameRegexp = regexp.MustCompile("^[0-9A-F]{24}$")
}

type LastSegmentNotFoundError struct {
	error
}

func newLastSegmentNotFoundError() LastSegmentNotFoundError {
	return LastSegmentNotFoundError{errors.Errorf("Could not reach the last element of segment sequence.\n")}
}

func (err LastSegmentNotFoundError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

// QueryCurrentWalSegment() gets start WAL segment from Postgres cluster
func QueryCurrentWalSegment() *WalSegmentDescription {
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
	return &WalSegmentDescription{timeline: currentTimeline, number: currentSegmentNo}
}

// HandleWalVerify queries the current cluster WAL segment and timeline
// and travels through WAL segments in storage in reversed chronological order (starting from that segment)
// to find any missing WAL segments that could potentially fail the PITR procedure
func HandleWalVerify(rootFolder storage.Folder, startWalSegment *WalSegmentDescription) {
	walFolder := rootFolder.GetSubFolder(utility.WalPath)
	fileNames, err := getFolderFilenames(walFolder)
	tracelog.ErrorLogger.FatalfOnError("Failed to get wal folder filenames %v", err)

	segments := getSegmentsFromFiles(fileNames)
	timelineHistoryMap, err := createTimelineHistoryMap(startWalSegment.timeline, walFolder)
	tracelog.ErrorLogger.FatalfOnError("Failed to initialize timeline history map %v", err)
	walSegmentRunner := NewWalSegmentRunner(true, startWalSegment, timelineHistoryMap, segments, 0)

	// maxConcurrency is needed to determine max amount of missing WAL segments
	// after the last found WAL segment which can be skipped ("uploading" segment sequence size)
	maxConcurrency, err := getMaxUploadConcurrency()
	tracelog.ErrorLogger.FatalOnError(err)

	tracelog.InfoLogger.Printf("Start WAL segment: %s", startWalSegment.GetFileName())
	sequenceStart, sequenceEnd, delayedSegments, uploadingSegments, err := FindNextContinuousSequence(walSegmentRunner, maxConcurrency)
	if _, ok := err.(ReachedZeroSegmentError); ok {
		tracelog.InfoLogger.Println("Reached zero WAL segment. Exiting...")
		return
	}
	tracelog.ErrorLogger.FatalfOnError("Error during WAL segment sequence walk %v", err)
	if len(delayedSegments) >0 {
		tracelog.InfoLogger.Printf("Missing WAL segments (probably delayed):\n")
		PrintSegments(delayedSegments)
	}
	if len(uploadingSegments) >0 {
		tracelog.InfoLogger.Printf("Missing WAL segments (probably uploading):\n")
		PrintSegments(uploadingSegments)
	}
	PrintSequenceInfo(sequenceStart, sequenceEnd)

	missingSegments := make([]*WalSegmentDescription,0)
	for {
		sequenceStart, sequenceEnd, skipped, _, err := FindNextContinuousSequence(walSegmentRunner, 0)
		missingSegments = append(missingSegments, skipped...)
		if _, ok := err.(ReachedZeroSegmentError); ok {
			PrintSequenceInfo(sequenceStart, sequenceEnd)
			tracelog.InfoLogger.Println("Reached zero WAL segment. Exiting...")
			break
		}
		tracelog.ErrorLogger.FatalfOnError("Error during WAL segment sequence walk %v", err)
		PrintSequenceInfo(sequenceStart, sequenceEnd)
	}
	if len(missingSegments) >0 {
		tracelog.WarningLogger.Printf("Missing WAL segments:\n")
		PrintSegments(missingSegments)
	}
}

func PrintSegments(segments []*WalSegmentDescription) {
	for _, segment := range segments {
		tracelog.InfoLogger.Println(segment.GetFileName())
	}
}

func PrintSequenceInfo(start, end *WalSegmentDescription) {
	tracelog.InfoLogger.Println("WAL segments continuous sequence START: " + start.GetFileName())
	tracelog.InfoLogger.Println("WAL segments continuous sequence END: " + end.GetFileName())

	// TODO: maybe print available backups in the resulting WAL segments range
	// We can do PITR starting from the backup in range [start, end]
}

func FindNextContinuousSequence(runner *WalSegmentRunner, uploadingSegmentRangeSize int) (*WalSegmentDescription,
	*WalSegmentDescription, []*WalSegmentDescription, []*WalSegmentDescription, error) {
	// Run to the latest WAL segment available in storage
	skippedStartSegments, err := runToFirstFoundSegment(runner)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	startSegment := runner.GetCurrent()

	// New startSegment might be chosen if there is some skipped segments after current startSegment
	// because we need a continuous sequence
	skippedUploadingSegments, startSegment, lastExistingSegment, err := traverseUploadingSegments(runner, uploadingSegmentRangeSize)
	if _, ok := err.(LastSegmentNotFoundError); ok {
		// LastSegmentNotFoundError means that the last segment in uploading WAL segments sequence
		// is not available in storage so we need to return range [startSegment, lastExistingSegment]
		return startSegment, lastExistingSegment, skippedStartSegments, skippedUploadingSegments, nil
	}
	if err != nil {
		return nil, nil, nil, nil, err
	}

	// Run to the first storage WAL segment (in sequence)
	err = runToFirstNotFoundSegment(runner)
	if err != nil {
		return nil, nil, nil, nil, err
	}
	endSegment := runner.GetCurrent()
	return endSegment, startSegment, skippedStartSegments, skippedUploadingSegments, nil
}

// traverseUploadingSegments is used to walk through WAL segments sequence sections that is probably
// being uploaded. It may contain missing segments because they are still being uploaded.
// Last element of "uploading" segments sequence should always exist and can not be skipped.
func traverseUploadingSegments(runner *WalSegmentRunner, uploadingSequenceRange int) ([]*WalSegmentDescription,
	*WalSegmentDescription, *WalSegmentDescription, error) {
	firstExistingSegment := runner.GetCurrent()
	lastExistingSegment := runner.GetCurrent()
	previousSegmentExists := true
	skippedUploadingSegments := make([]*WalSegmentDescription, 0)

	for i := 0; i < uploadingSequenceRange; i++ {
		currentSegment, err := runner.MoveNext()
		if err != nil {
			switch err := err.(type) {
			case WalSegmentNotFoundError:
				// WalSegmentNotFoundError means we reached the end of continuous WAL segments sequence
				runner.ForceMoveNext()
				previousSegmentExists = false
				tracelog.DebugLogger.Printf("Skipped missing segment %s, probably still uploading\n",
					runner.GetCurrent().GetFileName())
				skippedUploadingSegments = append(skippedUploadingSegments, runner.GetCurrent())
				continue
			case ReachedZeroSegmentError:
				// Can't continue because reached segment with zero number
				// so throw an error and return continuous segments sequence
				return skippedUploadingSegments, firstExistingSegment, lastExistingSegment, newLastSegmentNotFoundError()
			default:
				return nil, nil, nil, err
			}
		}
		if !previousSegmentExists {
			// if previous segment was skipped we should change firstExistingSegment to current
			// because we need continuous segment sequence
			firstExistingSegment = currentSegment
			previousSegmentExists = true
		}
		lastExistingSegment = currentSegment
		tracelog.DebugLogger.Println("Walked segment " + currentSegment.GetFileName())
	}
	if !previousSegmentExists {
		// Last segment needs to exist and can't be skipped
		// so we should throw an error and return continuous segments sequence
		return skippedUploadingSegments, firstExistingSegment, lastExistingSegment, newLastSegmentNotFoundError()
	}
	return skippedUploadingSegments, firstExistingSegment, lastExistingSegment, nil
}

// runToFirstNotFoundSegment travels the continuous WAL segments section and exits
// if missing segment or segment with zero number encountered.
func runToFirstNotFoundSegment(runner *WalSegmentRunner) error {
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

// runToFirstFoundSegment should find the first non-missing segment and exit
func runToFirstFoundSegment(runner *WalSegmentRunner) ([]*WalSegmentDescription, error) {
	skippedSegments := make([]*WalSegmentDescription, 0)
	for {
		if _, err := runner.MoveNext(); err != nil {
			if _, ok := err.(WalSegmentNotFoundError); ok {
				// force switch to the next WAL segment
				runner.ForceMoveNext()
				tracelog.DebugLogger.Printf("Skipped missing segment %s\n",
					runner.currentWalSegment.GetFileName())
				skippedSegments = append(skippedSegments, runner.GetCurrent())
				continue
			}
			return skippedSegments, err
		}
		return skippedSegments, nil
	}
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
