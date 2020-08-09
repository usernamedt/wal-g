package internal

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/utility"
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
	switchTimelines    bool
	currentWalSegment  *WalSegmentDescription
	walFolderSegments  map[WalSegmentDescription]bool
	timelineHistoryMap map[WalSegmentNo]*TimelineHistoryRecord
	stopSegmentNo      WalSegmentNo
}

func NewWalSegmentRunner(
	switchTimelines bool,
	startWalSegment *WalSegmentDescription,
	timelineHistoryMap map[WalSegmentNo]*TimelineHistoryRecord,
	segments map[WalSegmentDescription]bool,
	stopSegmentNo WalSegmentNo,
) *WalSegmentRunner {
	return &WalSegmentRunner{switchTimelines, startWalSegment,
		segments, timelineHistoryMap, stopSegmentNo}
}

func (r *WalSegmentRunner) GetCurrent() *WalSegmentDescription {
	return &*r.currentWalSegment
}

// MoveNext tries to get the next segment from storage
func (r *WalSegmentRunner) MoveNext() (*WalSegmentDescription, error) {
	if r.currentWalSegment.number <= r.stopSegmentNo {
		return nil, newReachedZeroSegmentError()
	}
	nextSegment := r.getNextSegment()
	var fileExists bool
	if _, fileExists = r.walFolderSegments[*nextSegment]; !fileExists {
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
	nextTimeline := r.currentWalSegment.timeline
	if r.switchTimelines {
		if record, ok := r.getTimelineSwitchRecord(r.currentWalSegment.number); ok {
			// switch timeline if current WAL segment number found in .history record
			nextTimeline = record.timeline
		}
	}
	nextSegmentNo := r.currentWalSegment.number.previous()
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

// getFolderFilenames returns a set of filenames in provided storage folder
func getFolderFilenames(folder storage.Folder) ([]string, error) {
	objects, _, err := folder.ListFolder()
	if err != nil {
		return nil, err
	}
	filenames := make([]string, 0, len(objects))
	for _, object := range objects {
		filenames = append(filenames, object.GetName())
	}
	return filenames, nil
}

func getSegmentsFromFiles(filenames []string) map[WalSegmentDescription]bool {
	walSegments := make(map[WalSegmentDescription]bool)
	for _, filename := range filenames {
		baseName := utility.TrimFileExtension(filename)
		timeline, segmentNo, err := ParseWALFilename(baseName)
		if _, ok := err.(NotWalFilenameError); ok {
			// non-wal segment file, skip it
			continue
		}
		segment := WalSegmentDescription{timeline: timeline, number: WalSegmentNo(segmentNo)}
		walSegments[segment] = true
	}
	return walSegments
}
