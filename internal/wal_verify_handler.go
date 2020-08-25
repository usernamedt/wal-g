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

type WalConsistencyScanReportRow struct {
	WalSegmentsSequence
	ScannedSegmentStatus
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
func HandleWalVerify(rootFolder storage.Folder, startWalSegment WalSegmentDescription) {
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

	tracelog.InfoLogger.Printf("Start WAL segment: %s", startWalSegment.GetFileName())

	segmentScanner := NewWalSegmentsScanner(walSegmentRunner, maxConcurrency)
	err = segmentScanner.ScanStorage()
	tracelog.ErrorLogger.FatalfOnError("Failed to perform WAL segments scan %v", err)
	
	printWalVerifyReport(segmentScanner.scannedSegments)
}

func printWalVerifyReport(scannedSegments []ScannedSegmentDescription) {
	if len(scannedSegments) == 0 {
		return
	}

	// scannedSegments are expected to be ordered by segment No
	sort.Slice(scannedSegments, func(i, j int) bool {
		return scannedSegments[i].Number < scannedSegments[j].Number
	})

	output := make([]*WalConsistencyScanReportRow, 0)
	currentSequence := NewSegmentsSequence(scannedSegments[0].Timeline, scannedSegments[0].Number)
	currentStatus := scannedSegments[0].status

	for i := 1; i < len(scannedSegments); i++ {
		segment := scannedSegments[i]

		// switch to the new sequence on segment status change or timeline id change
		if segment.status != currentStatus || currentSequence.timelineId != segment.Timeline {
			output = append(output, &WalConsistencyScanReportRow{*currentSequence, currentStatus})
			currentSequence = NewSegmentsSequence(segment.Timeline, segment.Number)
			currentStatus = segment.status
		} else {
			currentSequence.AddWalSegmentNo(segment.Number)
		}
	}

	output = append(output, &WalConsistencyScanReportRow{*currentSequence, currentStatus})
	for _, row := range output {
		printSequence(row)
	}
}

func printSequence(row *WalConsistencyScanReportRow) {
	tracelog.InfoLogger.Printf("TL %d [%s, %s] STATUS %s\n",
		row.timelineId,
		row.maxSegmentNo.getFilename(row.timelineId),
		row.minSegmentNo.getFilename(row.timelineId),
		row.ScannedSegmentStatus)
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
