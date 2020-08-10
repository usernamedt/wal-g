package internal

import (
	"fmt"
	"github.com/jackc/pgx"
	"github.com/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/utility"
)

type MissingSegmentStatus int

const (
	Lost MissingSegmentStatus = iota + 1
	ProbablyUploading
	ProbablyDelayed
)

type MissingSegmentDescription struct {
	WalSegmentDescription
	status MissingSegmentStatus
}

func (status MissingSegmentStatus) String() string {
	return [...]string{"", "Lost", "ProbablyUploading", "ProbablyDelayed"}[status]
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
	storageFileNames, err := getFolderFilenames(walFolder)
	tracelog.ErrorLogger.FatalfOnError("Failed to get wal folder filenames %v", err)

	storageSegments := getSegmentsFromFiles(storageFileNames)
	timelineHistoryMap, err := createTimelineHistoryMap(startWalSegment.timeline, walFolder)
	tracelog.ErrorLogger.FatalfOnError("Failed to initialize timeline history map %v", err)
	walSegmentRunner := NewWalSegmentRunner(true, startWalSegment, timelineHistoryMap, storageSegments, 0)

	// maxConcurrency is needed to determine max amount of missing WAL segments
	// after the last found WAL segment which can be skipped ("uploading" segment sequence size)
	maxConcurrency, err := getMaxUploadConcurrency()
	tracelog.ErrorLogger.FatalOnError(err)

	tracelog.InfoLogger.Printf("Start WAL segment: %s", startWalSegment.GetFileName())

	segmentScanner := NewWalSegmentsScanner(walSegmentRunner, maxConcurrency)
	err = Scan(segmentScanner)
	if _, ok := err.(ReachedZeroSegmentError); !ok {
		tracelog.ErrorLogger.FatalfOnError("Failed to do WAL segments scan %v", err)
	}
	printWalVerifyReport(segmentScanner.missingSegments, segmentScanner.existingSegments)
}

func printWalVerifyReport(missingSegments []*MissingSegmentDescription, existingSegments []*WalSegmentDescription) {
	existingSequences := make([]*WalSegmentsSequence, 0)
	currentSequence := NewSegmentsSequence(existingSegments[0].timeline, existingSegments[0].number)
	for _, segment := range existingSegments[1:] {
		if currentSequence.timelineId != segment.timeline || currentSequence.minSegmentNo != segment.number.next() {
			existingSequences = append(existingSequences, currentSequence)
			currentSequence = NewSegmentsSequence(segment.timeline, segment.number)
		} else {
			currentSequence.AddWalSegmentNo(segment.number)
		}
	}
	fmt.Println("EXISTING SEGMENTS SEQ:")
	printSequences(existingSequences)
	missingSequences := make([]*WalSegmentsSequence, 0)
	currentSequence = NewSegmentsSequence(missingSegments[0].timeline, missingSegments[0].number)
	currentStatus := missingSegments[0].status
	for _, segment := range missingSegments[1:] {
		if segment.status != currentStatus {
			fmt.Println(segment.status.String() +  " SEGMENTS:")
			printSequences(missingSequences)
			missingSequences = make([]*WalSegmentsSequence, 0)
			currentSequence = NewSegmentsSequence(segment.timeline, segment.number)
			currentStatus = segment.status
		}

		if currentSequence.timelineId != segment.timeline ||
			currentSequence.minSegmentNo != segment.number.next() {
			missingSequences = append(missingSequences, currentSequence)
			currentSequence = NewSegmentsSequence(segment.timeline, segment.number)
			currentStatus = segment.status
		} else {
			currentSequence.AddWalSegmentNo(segment.number)
		}
	}
	fmt.Println(currentStatus.String() +  " SEGMENTS:")
	printSequences(missingSequences)
}

func Scan(scanner *WalSegmentsScanner) error {
	// Run to the latest WAL segment available in storage
	scanConfig := SegmentScanConfig{
		infiniteScan: true,
		stopOnFirstFoundSegment: true,
		missingSegmentHandleFunc: scanner.addDelayedMissingSegment,
	}
	err := scanner.scanSegments(scanConfig)
	if err != nil {
		return err
	}

	// New startSegment might be chosen if there is some skipped segments after current startSegment
	// because we need a continuous sequence
	scanConfig = SegmentScanConfig{
		scanSegmentsCount: scanner.uploadingSegmentRangeSize,
		stopOnFirstFoundSegment: true,
		missingSegmentHandleFunc: scanner.addUploadingMissingSegment,
	}
	err = scanner.scanSegments(scanConfig)
	if err != nil {
		return err
	}

	scanConfig = SegmentScanConfig{
		infiniteScan: true,
		missingSegmentHandleFunc: scanner.addLostMissingSegment,
	}
	// Run to the first storage WAL segment (in sequence)
	return scanner.scanSegments(scanConfig)
}

func printSequences(sequences []*WalSegmentsSequence) {
	for _, seq := range sequences {
		tracelog.InfoLogger.Printf("TL %d [%s, %s]\n",
			seq.timelineId,
			seq.minSegmentNo.getFilename(seq.timelineId),
			seq.maxSegmentNo.getFilename(seq.timelineId),
			)
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

