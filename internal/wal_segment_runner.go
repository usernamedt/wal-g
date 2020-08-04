package internal

import (
	"bufio"
	"fmt"
	"github.com/jackc/pgx"
	"github.com/pkg/errors"
	"github.com/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/compression"
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

type TimelineSwitchMap map[WalSegmentNo]*WalHistoryRecord

type WalSegmentDescription struct {
	number WalSegmentNo
	timeline uint32
}

func (desc *WalSegmentDescription) GetFileName() string {
	return desc.number.getFilename(desc.timeline)
}

type WalSegmentRunner struct {
	runBackward       bool
	currentWalSegment *WalSegmentDescription
	folder            storage.Folder
	timelineSwitchMap map[WalSegmentNo]*WalHistoryRecord
}

func newWalSegmentRunner(
	runBackward bool,
	startWalSegment *WalSegmentDescription,
	folder storage.Folder,
)(*WalSegmentRunner, error) {
	timelineSwitchMap, err := createTimelineSwitchMap(startWalSegment.timeline, folder)
	if err != nil {
		return nil, err
	}
	return &WalSegmentRunner{runBackward,startWalSegment,folder,
		timelineSwitchMap}, nil
}

func (r *WalSegmentRunner) GetCurrentWalSegment() *WalSegmentDescription {
	result := *r.currentWalSegment
	return &result
}

func (r *WalSegmentRunner) GetNext() (*WalSegmentDescription, error) {
	nextSegment := r.getNextSegment()
	fileExists, err := checkWalSegmentExistsInStorage(nextSegment.GetFileName(), r.folder)
	if err != nil {
		return nil, err
	}
	if !fileExists {
		return nil, newWalSegmentNotFoundError(nextSegment.GetFileName())
	}
	r.currentWalSegment = nextSegment
	// return separate struct so it won't change after GetNext() call
	returnWalSegment := *r.currentWalSegment
	return &returnWalSegment, nil
}

func (r *WalSegmentRunner) getNextSegment() *WalSegmentDescription {
	var nextSegmentNo WalSegmentNo
	nextTimeline := r.currentWalSegment.timeline
	if r.runBackward {
		if record, ok := r.getTimelineSwitchRecord(r.currentWalSegment.number); ok {
			// switch timeline if current WAL segment number found in .history record
			nextTimeline = record.timeline
		}
		nextSegmentNo = r.currentWalSegment.number.previous()
	} else {
		nextSegmentNo = r.currentWalSegment.number.next()
	}
	return &WalSegmentDescription{timeline: nextTimeline, number: nextSegmentNo}
}

func (r *WalSegmentRunner) ForceSwitchToNextSegment() {
	nextSegment := r.getNextSegment()
	r.currentWalSegment = nextSegment
}

func (r *WalSegmentRunner) getTimelineSwitchRecord(walSegmentNo WalSegmentNo) (*WalHistoryRecord, bool) {
	if r.timelineSwitchMap == nil {
		return nil, false
	}
	record, ok := r.timelineSwitchMap[walSegmentNo]
	return record, ok
}

func createTimelineSwitchMap(startTimeline uint32, folder storage.Folder) (TimelineSwitchMap, error) {
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
	err = historyReadCloser.Close()
	if err != nil {
		return nil, err
	}
	timeLineSwitchMap := make(TimelineSwitchMap, 0)
	for _, record := range historyRecords {
		walSegmentNo := newWalSegmentNo(record.lsn)
		timeLineSwitchMap[walSegmentNo] = record
	}
	return timeLineSwitchMap, nil
}

func parseHistoryFile(historyReader io.Reader) (historyRecords []*WalHistoryRecord, err error) {
	scanner := bufio.NewScanner(historyReader)
	historyRecords = make([]*WalHistoryRecord, 0)
	for scanner.Scan() {
		nextRow := scanner.Text()
		if nextRow == "" {
			// skip empty rows in .history file
			continue
		}
		record, err := newWalHistoryRecordFromString(nextRow)
		if record == nil {
			break
		}
		if err != nil {
			return nil, err
		}
		historyRecords = append(historyRecords, record)
	}
	return historyRecords, err
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

func checkWalSegmentExistsInStorage(filename string, folder storage.Folder) (bool, error) {
	// this code fragment is partially borrowed from DownloadAndDecompressStorageFile()
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
