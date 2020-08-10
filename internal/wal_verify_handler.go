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

	segmentScanner := NewWalSegmentsScanner(walSegmentRunner, maxConcurrency)
	err = segmentScanner.Scan()
	if _, ok := err.(ReachedZeroSegmentError); !ok {
		tracelog.ErrorLogger.FatalfOnError("Failed to do WAL segments scan %v", err)
	}
	fmt.Println("EXISTING SEGMENTS:")
	printSegments(segmentScanner.existingSegments)
	fmt.Println("MISSING SEGMENTS:")
	printMissingSegments(segmentScanner.missingSegments)
}

func printMissingSegments(segments []*MissingSegmentDescription) {
	for _, segment := range segments {
		tracelog.InfoLogger.Println(segment.GetFileName() + ": " + segment.status.String())
	}
}

func printSegments(segments []*WalSegmentDescription) {
	for _, segment := range segments {
		tracelog.InfoLogger.Println(segment.GetFileName())
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
