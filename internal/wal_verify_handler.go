package internal

import (
	"bufio"
	"fmt"
	"github.com/jackc/pgx"
	"github.com/pkg/errors"
	"github.com/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/compression"
	"github.com/wal-g/wal-g/utility"
	"io"
	"regexp"
	"strconv"
)

var walHistoryRecordRegexp *regexp.Regexp

func init() {
	walHistoryRecordRegexp = regexp.MustCompile("^(\\d+)\\t(.+)\\t(.+)$")
}

type HistoryFileNotFoundError struct {
	error
}

func newHistoryFileNotFoundError(historyFileName string) HistoryFileNotFoundError {
	return HistoryFileNotFoundError{errors.Errorf("History file '%s' does not exist.\n", historyFileName)}
}

func (err HistoryFileNotFoundError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

type WalSegmentNotFoundError struct {
	error
}

func newWalSegmentNotFoundError(segmentFileName string) WalSegmentNotFoundError {
	return WalSegmentNotFoundError{
		errors.Errorf("Segment file '%s' does not exist in storage.\n", segmentFileName)}
}

func (err WalSegmentNotFoundError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

type WalHistoryRecord struct {
	timeline uint32
	lsn      uint64
	comment  string
}

func newWalHistoryRecordFromString(row string) (*WalHistoryRecord, error) {
	matchResult := walHistoryRecordRegexp.FindStringSubmatch(row)
	if matchResult == nil {
		return nil, nil
	}
	timeline, err := strconv.ParseUint(matchResult[1], 10, sizeofInt32)
	if err != nil {
		return nil, err
	}
	lsn, err := pgx.ParseLSN(matchResult[2])
	if err != nil {
		return nil, err
	}
	comment := matchResult[3]
	return &WalHistoryRecord{timeline: uint32(timeline), lsn: lsn, comment: comment}, nil
}

type WalSegmentRunner struct {
	currentWalSegmentNo WalSegmentNo
	currentTimeline     uint32
	folder              storage.Folder
	timelineSwitchMap   map[WalSegmentNo]*WalHistoryRecord
}

func newWalSegmentRunner(currentWalSegmentNo WalSegmentNo, startTimeline uint32, folder storage.Folder) (*WalSegmentRunner, error) {
	timelineSwitchMap, err := initTimelineSwitchMap(startTimeline, folder)
	if err != nil {
		return nil, err
	}
	return &WalSegmentRunner{currentWalSegmentNo, startTimeline,
		folder, timelineSwitchMap}, nil
}

func initTimelineSwitchMap(startTimeline uint32, folder storage.Folder) (
	walSegmentSwitches map[WalSegmentNo]*WalHistoryRecord,	err error) {
	historyReadCloser, err := getHistoryFile(startTimeline, folder)
	if _, ok := err.(HistoryFileNotFoundError); ok {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	historyRecords, err := parseHistoryFile(historyReadCloser)
	if err != nil {
		return nil, err
	}
	walSegmentSwitches = make(map[WalSegmentNo]*WalHistoryRecord, 0)
	for _, record := range historyRecords {
		walSegmentNo := newWalSegmentNo(record.lsn)
		walSegmentSwitches[walSegmentNo] = record
	}
	return walSegmentSwitches, nil
}

func (r *WalSegmentRunner) Run() (
	firstWalSegmentNo WalSegmentNo, firstTimeline uint32,
	lastWalSegmentNo WalSegmentNo, lastTimeLine uint32,	err error) {
	// we need to find latest WAL segment available in storage to start running
	lastWalSegmentNo, lastTimeLine, err = r.findLastAvailableStorageSegment()
	if err != nil {
		return 0,0,0,0,err
	}
	for r.currentWalSegmentNo > 0 {
		nextSegmentNo, timeline, err := r.nextSegment()
		// WalSegmentNotFoundError means we reached the end of continuous WAL segments sequence
		// so we can finish now
		if _, ok := err.(WalSegmentNotFoundError); ok {
			return r.currentWalSegmentNo, r.currentTimeline, lastWalSegmentNo, lastTimeLine, nil
		}
		if err != nil {
			return 0,0,0,0, err
		}
		fmt.Println("Walked segment: " + nextSegmentNo.getFilename(timeline))
	}
	// zero segment number ???
	return r.currentWalSegmentNo, r.currentTimeline, lastWalSegmentNo, lastTimeLine, nil
}

func (r *WalSegmentRunner) findLastAvailableStorageSegment() (walSegmentNumber WalSegmentNo, timeline uint32, err error) {
	for r.currentWalSegmentNo > 0 {
		nextSegmentNo, timeline, err := r.nextSegment()
		if _, ok := err.(WalSegmentNotFoundError); ok {
			// force switch to previous WAL segment
			r.currentWalSegmentNo = r.currentWalSegmentNo.previous()
			continue
		}
		return nextSegmentNo, timeline, err
	}
	return r.currentWalSegmentNo, r.currentTimeline, nil
}

func (r *WalSegmentRunner) nextSegment() (WalSegmentNo, uint32, error) {
	nextSegmentNo := r.currentWalSegmentNo.previous()
	nextSegmentFilename := nextSegmentNo.getFilename(r.currentTimeline)
	fileExists, err := checkWalSegmentExistsInStorage(nextSegmentFilename, r.folder)
	if err != nil {
		return 0, 0, err
	}
	// if failed to fetch WAL file, try to get .history file in case of timeline switch
	if !fileExists {
		if record, ok := r.getTimelineSwitchRecord(nextSegmentNo); ok {
			r.currentTimeline = record.timeline
			return r.nextSegment()
		}
		return 0, 0, newWalSegmentNotFoundError(nextSegmentFilename)
	}
	r.currentWalSegmentNo = nextSegmentNo
	return r.currentWalSegmentNo, r.currentTimeline, nil
}

func (r *WalSegmentRunner) getTimelineSwitchRecord(walSegmentNo WalSegmentNo) (*WalHistoryRecord, bool) {
	if r.timelineSwitchMap == nil {
		return nil, false
	}
	record, ok := r.timelineSwitchMap[walSegmentNo]
	return record, ok
}

func parseHistoryFile(readCloser io.ReadCloser) (historyRecords []*WalHistoryRecord, err error) {
	scanner := bufio.NewScanner(readCloser)
	historyRecords = make([]*WalHistoryRecord, 0)
	for scanner.Scan() {
		record, err := newWalHistoryRecordFromString(scanner.Text())
		if record == nil {
			break
		}
		if err != nil {
			return nil, err
		}
		historyRecords = append(historyRecords, record)
	}
	return historyRecords, nil
}

func getHistoryFile(timeline uint32, folder storage.Folder) (io.ReadCloser, error) {
	historyFileName := fmt.Sprintf(walHistoryFileFormat, timeline)
	reader, err := DownloadAndDecompressStorageFile(folder, historyFileName)
	if _, ok := err.(ArchiveNonExistenceError); ok {
		return nil, newHistoryFileNotFoundError(historyFileName)
	}
	if err != nil {
		return nil, errors.Wrapf(err, "Error during .history file '%s' downloading.", historyFileName)
	}
	return reader, nil
}

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

	// firstWalSegmentNo is the number of the first wal segment
	// in the wal segments sequence ending with currentWalSegmentNo
	// so we can do PITR starting from the backup in range [firstWalSegmentNo, currentWalSegmentNo]
	walSegmentRunner, err := newWalSegmentRunner(currentWalSegmentNo, currentTimeline, folder)
	tracelog.ErrorLogger.FatalfOnError("Failed to initialize WAL segment runner", err)

	firstWalSegmentNo, firstTimeline, lastWalSegmentNo, lastTimeline, err := walSegmentRunner.Run()
	tracelog.ErrorLogger.FatalfOnError("Error during WAL segment sequence walk", err)

	fmt.Println("CURRENT WAL SEGMENT: " + currentWalSegmentNo.getFilename(currentTimeline))
	fmt.Println("LAST AVAILABLE WAL SEGMENT: " + lastWalSegmentNo.getFilename(lastTimeline))
	fmt.Println("FIRST AVAILABLE WAL SEGMENT: " + firstWalSegmentNo.getFilename(firstTimeline))

	//
	//getBackupsFunc := func() ([]BackupTime, error) {
	//	return getBackups(folder)
	//}
	//writeBackupListFunc := func(backups []BackupTime) {
	//	WriteBackupList(backups, os.Stdout)
	//}
	//logging := Logging{
	//	InfoLogger:  tracelog.InfoLogger,
	//	ErrorLogger: tracelog.ErrorLogger,
	//}
	//
	//HandleBackupList(getBackupsFunc, writeBackupListFunc, logging)
}

func checkWalSegmentExistsInStorage(filename string, folder storage.Folder) (bool, error) {
	for _, decompressor := range putCachedDecompressorInFirstPlace(compression.Decompressors) {
		_, exists, err := TryDownloadFile(folder, filename+"."+decompressor.FileExtension())
		if err != nil {
			return false, err
		}
		if !exists {
			continue
		}
		_ = SetLastDecompressor(decompressor)
		return true, nil
	}
	return false, nil
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
