package internal

import (
	"github.com/jackc/pgx"
	"github.com/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/utility"
)

type ScannedSegmentStatus int

const (
	Lost ScannedSegmentStatus = iota + 1
	ProbablyUploading
	ProbablyDelayed
	Found
)

type ScannedSegmentDescription struct {
	WalSegmentDescription
	status ScannedSegmentStatus
}

func (status ScannedSegmentStatus) String() string {
	return [...]string{"", "Lost", "ProbablyUploading", "ProbablyDelayed", "Found"}[status]
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
	tracelog.ErrorLogger.FatalfOnError("Failed to get WAL folder filenames %v", err)

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
	err = ScanStorage(segmentScanner)
	if _, ok := err.(ReachedZeroSegmentError); !ok {
		tracelog.ErrorLogger.FatalfOnError("Failed to perform WAL segments scan %v", err)
	}
	printWalVerifyReport(segmentScanner.scannedSegments)
}

func printWalVerifyReport(scannedSegments []*ScannedSegmentDescription) {
	currentSequence := NewSegmentsSequence(scannedSegments[0].timeline, scannedSegments[0].number)
	currentStatus := scannedSegments[0].status
	for _, segment := range scannedSegments[1:] {
		if segment.status != currentStatus ||
			currentSequence.timelineId != segment.timeline ||
			currentSequence.minSegmentNo != segment.number.next(){
			printSequence(currentSequence, currentStatus)
			currentSequence = NewSegmentsSequence(segment.timeline, segment.number)
			currentStatus = segment.status
		} else {
			currentSequence.AddWalSegmentNo(segment.number)
		}
	}
	printSequence(currentSequence, currentStatus)
}

func ScanStorage(scanner *WalSegmentsScanner) error {
	// Run to the latest WAL segment available in storage
	scanConfig := SegmentScanConfig{
		unlimitedScan:           true,
		stopOnFirstFoundSegment: true,
		missingSegmentHandler:   scanner.addDelayedMissingSegment,
	}
	err := scanner.Scan(scanConfig)
	if err != nil {
		return err
	}

	// New startSegment might be chosen if there is some skipped segments after current startSegment
	// because we need a continuous sequence
	scanConfig = SegmentScanConfig{
		scanSegmentsLimit:       scanner.uploadingSegmentRangeSize,
		stopOnFirstFoundSegment: true,
		missingSegmentHandler:   scanner.addUploadingMissingSegment,
	}
	err = scanner.Scan(scanConfig)
	if err != nil {
		return err
	}

	scanConfig = SegmentScanConfig{
		unlimitedScan:         true,
		missingSegmentHandler: scanner.addLostMissingSegment,
	}
	// Run to the first storage WAL segment (in sequence)
	return scanner.Scan(scanConfig)
}

func printSequence(seq *WalSegmentsSequence, status ScannedSegmentStatus) {
	tracelog.InfoLogger.Printf("TL %d [%s, %s] STATUS %s\n",
			seq.timelineId,
			seq.maxSegmentNo.getFilename(seq.timelineId),
			seq.minSegmentNo.getFilename(seq.timelineId),
			status.String())
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

