package internal

import (
	"fmt"
	"github.com/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/utility"
)

type TimelineInfo struct {
	id                uint32
	walSegmentNumbers map[WalSegmentNo]bool
	minSegmentNo      WalSegmentNo
	maxSegmentNo      WalSegmentNo
	historyRecords    []*TimelineHistoryRecord
}

func newTimelineInfo(id uint32, historyRecords []*TimelineHistoryRecord, segmentNo WalSegmentNo) *TimelineInfo {
	walSegmentNumbers := make(map[WalSegmentNo]bool)
	walSegmentNumbers[segmentNo] = true
	return &TimelineInfo{id: id, historyRecords: historyRecords,
		walSegmentNumbers: walSegmentNumbers, minSegmentNo: segmentNo, maxSegmentNo: segmentNo}
}

func (info *TimelineInfo) addWalSegmentNo(number WalSegmentNo) {
	info.walSegmentNumbers[number] = true
	if info.minSegmentNo > number {
		info.minSegmentNo = number
	}
	if info.maxSegmentNo < number {
		info.maxSegmentNo = number
	}
}

func (info *TimelineInfo) GetMissingSegments(walFolder storage.Folder) ([]*WalSegmentDescription, error) {
	maxWalSegment := &WalSegmentDescription{number: info.maxSegmentNo, timeline: info.id}

	walSegments := make(map[WalSegmentDescription]bool, len(info.walSegmentNumbers))
	for number := range info.walSegmentNumbers {
		segment := WalSegmentDescription{number: number, timeline: info.id}
		walSegments[segment] = true
	}
	walSegmentRunner, err := NewWalSegmentRunner(false, maxWalSegment, walFolder, walSegments, info.minSegmentNo)
	if err != nil {
		return nil, err
	}
	missingSegments := make([]*WalSegmentDescription, 0)
	for {
		if _, err := walSegmentRunner.MoveNext(); err != nil {
			switch err := err.(type) {
			case WalSegmentNotFoundError:
				// force switch to the next WAL segment
				walSegmentRunner.ForceMoveNext()
				missingSegments = append(missingSegments, walSegmentRunner.currentWalSegment)
				continue
			case ReachedZeroSegmentError:
				// Can't continue because reached segment with zero number, stop at this point
				return missingSegments, nil
			default:
				return nil, err
			}
		}
	}
}

func HandleWalShow(rootFolder storage.Folder) {
	walFolder := rootFolder.GetSubFolder(utility.WalPath)
	filenames, err := getFolderFilenames(walFolder)
	tracelog.ErrorLogger.FatalfOnError("Failed to get wal folder filenames %v", err)
	walSegments := getSegmentsFromFiles(filenames)
	timelines, err := getTimelines(rootFolder, walSegments)
	tracelog.ErrorLogger.FatalfOnError("Failed to timelines %v", err)
	for _, tl := range timelines {
		fmt.Printf("TL %d COUNT %d RANGE %d START %s END %s\n", tl.id, len(tl.walSegmentNumbers), tl.maxSegmentNo-tl.minSegmentNo+1, tl.minSegmentNo.getFilename(tl.id), tl.maxSegmentNo.getFilename(tl.id))
		missingSegments, err := tl.GetMissingSegments(walFolder)
		tracelog.ErrorLogger.FatalfOnError(" %v", err)
		if len(missingSegments) == 0 {
			fmt.Printf("OK\n")
		} else {
			for _, segment := range missingSegments {
				fmt.Printf("MISSING: %s\n", segment.GetFileName())
			}
		}
	}
}

func getTimelines(folder storage.Folder, segments map[WalSegmentDescription]bool) (map[uint32]*TimelineInfo, error) {
	timelines := make(map[uint32]*TimelineInfo)
	for segment := range segments {
		if timelineInfo, ok := timelines[segment.timeline]; ok {
			timelineInfo.addWalSegmentNo(segment.number)
			continue
		}

		var historyRecords []*TimelineHistoryRecord
		historyRecords, err := getTimeLineHistoryRecords(segment.timeline, folder)
		if _, ok := err.(HistoryFileNotFoundError); !ok {
			return nil, err
		}
		timelines[segment.timeline] = newTimelineInfo(segment.timeline, historyRecords, segment.number)
	}
	return timelines, nil
}
