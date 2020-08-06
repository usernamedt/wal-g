package internal

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/compression"
	"regexp"
)

var walHistoryRecordRegexp *regexp.Regexp

func init() {
	walHistoryRecordRegexp = regexp.MustCompile("^(\\d+)\\t(.+)\\t(.+)$")
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

type ReachedZeroSegmentError struct {
	error
}

func newReachedZeroSegmentError() ReachedZeroSegmentError {
	return ReachedZeroSegmentError{errors.Errorf("Reached segment with zero number.\n")}
}

func (err ReachedZeroSegmentError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

type WalSegmentDescription struct {
	number   WalSegmentNo
	timeline uint32
}

func (desc *WalSegmentDescription) GetFileName() string {
	return desc.number.getFilename(desc.timeline)
}

// WalSegmentRunner is used for sequential iteration over WAL segments in the storage
type WalSegmentRunner struct {
	// runBackwards controls the direction of WalSegmentRunner
	runBackwards       bool
	currentWalSegment  *WalSegmentDescription
	walFolder          storage.Folder
	walFolderFilenames map[string]bool
	timelineHistoryMap TimelineHistoryMap
	cachedDecompressor compression.Decompressor
}

func NewWalSegmentRunner(
	runBackwards bool,
	startWalSegment *WalSegmentDescription,
	walFolder storage.Folder,
	fileNames map[string]bool,
) (*WalSegmentRunner, error) {
	historyMap, err := createTimelineHistoryMap(startWalSegment.timeline, walFolder)
	if err != nil {
		return nil, err
	}
	return &WalSegmentRunner{runBackwards, startWalSegment, walFolder,
		fileNames, historyMap, nil}, nil
}

func (r *WalSegmentRunner) GetCurrent() *WalSegmentDescription {
	return &*r.currentWalSegment
}

// MoveNext tries to get the next segment from storage
func (r *WalSegmentRunner) MoveNext() (*WalSegmentDescription, error) {
	if r.runBackwards && r.currentWalSegment.number <= 1 {
		return nil, newReachedZeroSegmentError()
	}
	nextSegment := r.getNextSegment()
	var fileExists bool
	r.cachedDecompressor, fileExists = checkFileExistsInStorage(nextSegment.GetFileName(), r.walFolderFilenames, r.cachedDecompressor)
	if !fileExists {
		return nil, newWalSegmentNotFoundError(nextSegment.GetFileName())
	}
	r.currentWalSegment = nextSegment
	// return separate struct so it won't change after MoveNext() call
	return r.GetCurrent(), nil
}

// ForceMoveNext do a force-switch to the next segment without accessing storage
func (r *WalSegmentRunner) ForceMoveNext() {
	nextSegment := r.getNextSegment()
	r.currentWalSegment = nextSegment
}

// getNextSegment calculates the next segment
func (r *WalSegmentRunner) getNextSegment() *WalSegmentDescription {
	var nextSegmentNo WalSegmentNo
	nextTimeline := r.currentWalSegment.timeline
	if r.runBackwards {
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

// getTimelineSwitchRecord checks if there is a record in .history file for provided wal segment number
func (r *WalSegmentRunner) getTimelineSwitchRecord(walSegmentNo WalSegmentNo) (*TimelineHistoryRecord, bool) {
	if r.timelineHistoryMap == nil {
		return nil, false
	}
	record, ok := r.timelineHistoryMap[walSegmentNo]
	return record, ok
}

// checkFileExistsInStorage checks that file with provided name exists in storage folder files
func checkFileExistsInStorage(filename string, storageFiles map[string]bool,
	lastDecompressor compression.Decompressor) (compression.Decompressor, bool) {
	// this code fragment is partially borrowed from DownloadAndDecompressStorageFile()
	for _, decompressor := range putDecompressorInFirstPlace(lastDecompressor, compression.Decompressors) {
		_, exists := storageFiles[filename+"."+decompressor.FileExtension()]
		if !exists {
			continue
		}
		return decompressor, true
	}
	return lastDecompressor, false
}

// getFolderFilenames returns a set of filenames in provided storage folder
func getFolderFilenames(folder storage.Folder) (map[string]bool, error) {
	objects, _, err := folder.ListFolder()
	if err != nil {
		return nil, err
	}
	result := make(map[string]bool, len(objects))
	for _, object := range objects {
		result[object.GetName()] = true
	}
	return result, nil
}
