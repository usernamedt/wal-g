package internal

import (
	"github.com/jackc/pgx"
	"github.com/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/utility"
	"sort"
)

type ScannedSegmentStatus int

const (
	// Surely lost missing segment
	Lost ScannedSegmentStatus = iota + 1
	// Missing but probably still uploading segment
	ProbablyUploading
	// Missing but probably delayed segment
	ProbablyDelayed
	// Segment exists in storage
	Found
)

type ScannedSegmentDescription struct {
	WalSegmentDescription
	status ScannedSegmentStatus
}

func (status ScannedSegmentStatus) String() string {
	return [...]string{"", "MISSING_LOST", "MISSING_UPLOADING", "MISSING_DELAYED", "FOUND"}[status]
}

type WalIntegrityScanResultRow struct {
	TimelineId    uint32               `json:"timeline_id"`
	StartSegment  string               `json:"start_segment"`
	EndSegment    string               `json:"end_segment"`
	SegmentsCount int                  `json:"segments_count"`
	Status        ScannedSegmentStatus `json:"status"`
}

func newWalIntegrityScanResultRow(sequence *WalSegmentsSequence, status ScannedSegmentStatus) *WalIntegrityScanResultRow {
	return &WalIntegrityScanResultRow{
		TimelineId:    sequence.timelineId,
		StartSegment:  sequence.minSegmentNo.getFilename(sequence.timelineId),
		EndSegment:    sequence.maxSegmentNo.getFilename(sequence.timelineId),
		Status:        status,
		SegmentsCount: len(sequence.walSegmentNumbers),
	}
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
func HandleWalVerify(rootFolder storage.Folder, startWalSegment WalSegmentDescription, outputWriter WalVerifyOutputWriter) {
	tracelog.InfoLogger.Printf("Received start WAL segment: %s", startWalSegment.GetFileName())
	walFolder := rootFolder.GetSubFolder(utility.WalPath)
	storageFileNames, err := getFolderFilenames(walFolder)
	tracelog.ErrorLogger.FatalfOnError("Failed to get WAL folder filenames %v", err)

	storageSegments := getSegmentsFromFiles(storageFileNames)
	timelineSwitchMap, err := createTimelineSwitchMap(startWalSegment.Timeline, walFolder)
	tracelog.ErrorLogger.FatalfOnError("Failed to initialize timeline history map %v", err)
	walSegmentRunner := NewWalSegmentRunner(startWalSegment, storageSegments, 1, timelineSwitchMap)

	// maxConcurrency is needed to determine max amount of missing WAL segments
	// after the last found WAL segment which can be skipped ("uploading" segment sequence size)
	maxConcurrency, err := getMaxUploadConcurrency()
	tracelog.ErrorLogger.FatalOnError(err)

	segmentScanner := NewWalSegmentsScanner(walSegmentRunner, maxConcurrency)
	err = runWalIntegrityScan(segmentScanner)
	tracelog.ErrorLogger.FatalfOnError("Failed to perform WAL segments scan %v", err)

	integrityReport, err := createIntegrityScanResult(segmentScanner.scannedSegments)
	tracelog.ErrorLogger.FatalOnError(err)

	err = outputWriter.Write(integrityReport)
	tracelog.ErrorLogger.FatalOnError(err)
}

// runWalIntegrityScan invokes three storage scan series:
// 1. At first, it runs scan until it finds some segment in WAL storage
// and marks all encountered missing segments as "missing, probably delayed"
// 2. Then it scans exactly uploadingSegmentRangeSize count of segments,
// if found any missing segments it marks them as "missing, probably still uploading"
// 3. Final scan without any limit (until stopSegment is reached),
// all missing segments encountered in this scan are considered as "missing, lost"
func runWalIntegrityScan(scanner *WalSegmentsScanner) error {
	// Run to the latest WAL segment available in storage, mark all missing segments as delayed
	err := scanner.scan(SegmentScanConfig{
		unlimitedScan:           true,
		stopOnFirstFoundSegment: true,
		missingSegmentHandler:   scanner.addMissingDelayedSegment,
	})
	if err != nil {
		return err
	}

	// Traverse potentially uploading segments, mark all missing segments as probably uploading
	err = scanner.scan(SegmentScanConfig{
		scanSegmentsLimit:       scanner.uploadingSegmentRangeSize,
		stopOnFirstFoundSegment: true,
		missingSegmentHandler:   scanner.addMissingUploadingSegment,
	})
	if err != nil {
		return err
	}

	// Run until stop segment, and mark all missing segments as lost
	return scanner.scan(SegmentScanConfig{
		unlimitedScan:         true,
		missingSegmentHandler: scanner.addMissingLostSegment,
	})
}

// createIntegrityScanResult groups scanned segments
// with the same timeline and status into segment sequences to minify the output
func createIntegrityScanResult(scannedSegments []ScannedSegmentDescription) ([]*WalIntegrityScanResultRow, error) {
	if len(scannedSegments) == 0 {
		return nil, nil
	}

	// scannedSegments are expected to be ordered by segment No
	sort.Slice(scannedSegments, func(i, j int) bool {
		return scannedSegments[i].Number < scannedSegments[j].Number
	})

	scanResultRows := make([]*WalIntegrityScanResultRow, 0)
	currentSequence := NewSegmentsSequence(scannedSegments[0].Timeline, scannedSegments[0].Number)
	currentStatus := scannedSegments[0].status

	for i := 1; i < len(scannedSegments); i++ {
		segment := scannedSegments[i]

		// switch to the new sequence on segment Status change or timeline id change
		if segment.status != currentStatus || currentSequence.timelineId != segment.Timeline {
			scanResultRows = append(scanResultRows, newWalIntegrityScanResultRow(currentSequence, currentStatus))
			currentSequence = NewSegmentsSequence(segment.Timeline, segment.Number)
			currentStatus = segment.status
		} else {
			currentSequence.AddWalSegmentNo(segment.Number)
		}
	}

	scanResultRows = append(scanResultRows, newWalIntegrityScanResultRow(currentSequence, currentStatus))
	return scanResultRows, nil
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
