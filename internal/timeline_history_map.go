package internal

import (
	"bufio"
	"fmt"
	"github.com/jackc/pgx"
	"github.com/pkg/errors"
	"github.com/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
	"io"
	"strconv"
)

type HistoryFileNotFoundError struct {
	error
}

func newHistoryFileNotFoundError(historyFileName string) HistoryFileNotFoundError {
	return HistoryFileNotFoundError{errors.Errorf("History file '%s' does not exist.\n", historyFileName)}
}

func (err HistoryFileNotFoundError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

// TimelineHistoryMap represents .history file
type TimelineHistoryMap map[WalSegmentNo]*TimelineHistoryRecord

// TimelineHistoryRecord represents entry in .history file
type TimelineHistoryRecord struct {
	timeline uint32
	lsn      uint64
	comment  string
}

func newHistoryRecordFromString(row string) (*TimelineHistoryRecord, error) {
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
	return &TimelineHistoryRecord{timeline: uint32(timeline), lsn: lsn, comment: comment}, nil
}

// createTimelineHistoryMap tries to fetch and parse .history file
func createTimelineHistoryMap(startTimeline uint32, folder storage.Folder) (TimelineHistoryMap, error) {
	timeLineHistoryMap := make(TimelineHistoryMap, 0)
	historyReadCloser, err := getHistoryFileFromStorage(startTimeline, folder)
	if _, ok := err.(HistoryFileNotFoundError); ok {
		// return empty map if not found any history
		return timeLineHistoryMap, nil
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

	// store records in a map for fast lookup by wal segment number
	for _, record := range historyRecords {
		walSegmentNo := newWalSegmentNo(record.lsn)
		timeLineHistoryMap[walSegmentNo] = record
	}
	return timeLineHistoryMap, nil
}

func parseHistoryFile(historyReader io.Reader) (historyRecords []*TimelineHistoryRecord, err error) {
	scanner := bufio.NewScanner(historyReader)
	historyRecords = make([]*TimelineHistoryRecord, 0)
	for scanner.Scan() {
		nextRow := scanner.Text()
		if nextRow == "" {
			// skip empty rows in .history file
			continue
		}
		record, err := newHistoryRecordFromString(nextRow)
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

func getHistoryFileFromStorage(timeline uint32, folder storage.Folder) (io.ReadCloser, error) {
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
