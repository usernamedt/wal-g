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

type WalIntegrityReportRow struct {
	TimelineId    uint32               `json:"timeline_id"`
	StartSegment  string               `json:"start_segment"`
	EndSegment    string               `json:"end_segment"`
	SegmentsCount int                  `json:"segments_count"`
	Status        ScannedSegmentStatus `json:"Status"`
}

func newWalIntegrityReportRow(sequence *WalSegmentsSequence, status ScannedSegmentStatus) *WalIntegrityReportRow {
	return &WalIntegrityReportRow{
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
	walSegmentRunner := NewWalSegmentRunner(startWalSegment, storageSegments, 0, timelineSwitchMap)

	// maxConcurrency is needed to determine max amount of missing WAL segments
	// after the last found WAL segment which can be skipped ("uploading" segment sequence size)
	maxConcurrency, err := getMaxUploadConcurrency()
	tracelog.ErrorLogger.FatalOnError(err)

	segmentScanner := NewWalSegmentsScanner(walSegmentRunner, maxConcurrency)
	err = segmentScanner.ScanStorage()
	tracelog.ErrorLogger.FatalfOnError("Failed to perform WAL segments scan %v", err)

	consistencyReport, err := createIntegrityReport(segmentScanner.scannedSegments)
	tracelog.ErrorLogger.FatalOnError(err)

	err = outputWriter.Write(consistencyReport)
	tracelog.ErrorLogger.FatalOnError(err)
}

func createIntegrityReport(scannedSegments []ScannedSegmentDescription) ([]*WalIntegrityReportRow, error) {
	if len(scannedSegments) == 0 {
		return nil, nil
	}

	// scannedSegments are expected to be ordered by segment No
	sort.Slice(scannedSegments, func(i, j int) bool {
		return scannedSegments[i].Number < scannedSegments[j].Number
	})

	report := make([]*WalIntegrityReportRow, 0)
	currentSequence := NewSegmentsSequence(scannedSegments[0].Timeline, scannedSegments[0].Number)
	currentStatus := scannedSegments[0].status

	for i := 1; i < len(scannedSegments); i++ {
		segment := scannedSegments[i]

		// switch to the new sequence on segment Status change or timeline id change
		if segment.status != currentStatus || currentSequence.timelineId != segment.Timeline {
			report = append(report, newWalIntegrityReportRow(currentSequence, currentStatus))
			currentSequence = NewSegmentsSequence(segment.Timeline, segment.Number)
			currentStatus = segment.status
		} else {
			currentSequence.AddWalSegmentNo(segment.Number)
		}
	}

	report = append(report, newWalIntegrityReportRow(currentSequence, currentStatus))
	return report, nil
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
