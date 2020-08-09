package internal

import (
	"encoding/json"
	"github.com/jedib0t/go-pretty/table"
	"github.com/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/utility"
	"io"
	"sort"
)

const (
	TimelineOkStatus          = "OK"
	TimelineLostSegmentStatus = "LOST_SEG"
)

type TimelineInfo struct {
	Id               uint32          `json:"id"`
	ParentId         uint32          `json:"parentId"`
	SwitchPointLsn   uint64          `json:"switchPointLsn"`
	StartSegment     string          `json:"startSegment"`
	EndSegment       string          `json:"endSegment"`
	SegmentsCount    int             `json:"segmentsCount"`
	MissingSegments  []string        `json:"missingSegments"`
	Backups          []*BackupDetail `json:"availableBackups,omitempty"`
	SegmentRangeSize uint64          `json:"segmentRangeSize"`
	Status           string          `json:"status"`
}

func newTimelineInfo(walSegments *WalSegmentsSequence, historyRecords []*TimelineHistoryRecord, folder storage.Folder) (*TimelineInfo, error) {
	timelineInfo := &TimelineInfo{
		Id:               walSegments.timelineId,
		StartSegment:     walSegments.minSegmentNo.getFilename(walSegments.timelineId),
		EndSegment:       walSegments.maxSegmentNo.getFilename(walSegments.timelineId),
		SegmentsCount:    len(walSegments.walSegmentNumbers),
		SegmentRangeSize: uint64(walSegments.maxSegmentNo-walSegments.minSegmentNo) + 1,
		Status:           TimelineOkStatus,
	}
	missingSegments, err := walSegments.GetMissingSegments(folder)
	if err != nil {
		return nil, err
	}
	timelineInfo.MissingSegments = make([]string, 0, len(missingSegments))
	for _, segment := range missingSegments {
		timelineInfo.MissingSegments = append(timelineInfo.MissingSegments, segment.GetFileName())
	}

	if len(timelineInfo.MissingSegments) > 0 {
		timelineInfo.Status = TimelineLostSegmentStatus
	}

	// set parent timeline id and timeline switch LSN if have .history record available
	if len(historyRecords) > 0 {
		switchHistoryRecord := historyRecords[len(historyRecords)-1]
		timelineInfo.ParentId = switchHistoryRecord.timeline
		timelineInfo.SwitchPointLsn = switchHistoryRecord.lsn
	}
	return timelineInfo, nil
}

type WalSegmentsSequence struct {
	timelineId        uint32
	walSegmentNumbers map[WalSegmentNo]bool
	minSegmentNo      WalSegmentNo
	maxSegmentNo      WalSegmentNo
}

func newSegmentsSequence(id uint32, segmentNo WalSegmentNo) *WalSegmentsSequence {
	walSegmentNumbers := make(map[WalSegmentNo]bool)
	walSegmentNumbers[segmentNo] = true

	return &WalSegmentsSequence{
		timelineId:        id,
		walSegmentNumbers: walSegmentNumbers,
		minSegmentNo:      segmentNo,
		maxSegmentNo:      segmentNo,
	}
}

func (data *WalSegmentsSequence) addWalSegmentNo(number WalSegmentNo) {
	data.walSegmentNumbers[number] = true
	if data.minSegmentNo > number {
		data.minSegmentNo = number
	}
	if data.maxSegmentNo < number {
		data.maxSegmentNo = number
	}
}

func (data *WalSegmentsSequence) GetMissingSegments(walFolder storage.Folder) ([]*WalSegmentDescription, error) {
	maxWalSegment := &WalSegmentDescription{number: data.maxSegmentNo, timeline: data.timelineId}

	walSegments := make(map[WalSegmentDescription]bool, len(data.walSegmentNumbers))
	for number := range data.walSegmentNumbers {
		segment := WalSegmentDescription{number: number, timeline: data.timelineId}
		walSegments[segment] = true
	}
	timelineHistoryMap, err := createTimelineHistoryMap(data.timelineId, walFolder)
	if err != nil {
		return nil, err
	}
	walSegmentRunner := NewWalSegmentRunner(false, maxWalSegment, timelineHistoryMap, walSegments, data.minSegmentNo)
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

func HandleWalShow(rootFolder storage.Folder, showBackups bool, outputWriter WalShowOutputWriter) {
	walFolder := rootFolder.GetSubFolder(utility.WalPath)
	filenames, err := getFolderFilenames(walFolder)
	tracelog.ErrorLogger.FatalfOnError("Failed to get wal folder filenames %v\n", err)

	walSegments := getSegmentsFromFiles(filenames)
	segmentsByTimelines, err := groupSegmentsByTimelines(walSegments)
	tracelog.ErrorLogger.FatalfOnError("Failed to group segments by timelines %v\n", err)

	timelineInfos := make([]*TimelineInfo, 0, len(segmentsByTimelines))
	for _, segmentsSequence := range segmentsByTimelines {
		historyRecords, err := getTimeLineHistoryRecords(segmentsSequence.timelineId, walFolder)
		if err != nil {
			if _, ok := err.(HistoryFileNotFoundError); !ok {
				tracelog.ErrorLogger.Fatalf("Error while loading .history file %v\n", err)
			}
		}

		info, err := newTimelineInfo(segmentsSequence, historyRecords, rootFolder)
		timelineInfos = append(timelineInfos, info)
	}

	if showBackups {
		backups, err := getBackups(rootFolder)
		tracelog.ErrorLogger.FatalfOnError("Failed to get backups: %v\n", err)
		backupDetails, err := getBackupDetails(rootFolder, backups)
		tracelog.ErrorLogger.FatalfOnError("Failed to get backups details: %v\n", err)
		for _, info := range timelineInfos {
			info.Backups, err = getBackupsInRange(info.StartSegment, info.EndSegment, info.Id, backupDetails)
			tracelog.ErrorLogger.FatalOnError(err)
		}
	}

	// order timelines by ID
	sort.Slice(timelineInfos, func(i, j int) bool {
		return timelineInfos[i].Id < timelineInfos[j].Id
	})

	err = outputWriter.Write(timelineInfos)
	tracelog.ErrorLogger.FatalfOnError("Error writing output: %v\n", err)
}

func groupSegmentsByTimelines(segments map[WalSegmentDescription]bool) (map[uint32]*WalSegmentsSequence, error) {
	segmentsByTimelines := make(map[uint32]*WalSegmentsSequence)
	for segment := range segments {
		if timelineInfo, ok := segmentsByTimelines[segment.timeline]; ok {
			timelineInfo.addWalSegmentNo(segment.number)
			continue
		}
		segmentsByTimelines[segment.timeline] = newSegmentsSequence(segment.timeline, segment.number)
	}
	return segmentsByTimelines, nil
}

func getBackupsInRange(start, end string, timeline uint32, backups []BackupDetail) ([]*BackupDetail, error) {
	filteredBackups := make([]*BackupDetail, 0)

	for _, backup := range backups {
		backupTimeline, _, err := ParseWALFilename(backup.WalFileName)
		if err != nil {
			return nil, err
		}
		startSegment := newWalSegmentNo(backup.StartLsn).getFilename(backupTimeline)
		endSegment := newWalSegmentNo(backup.FinishLsn).getFilename(backupTimeline)
		if timeline == backupTimeline && startSegment >= start && endSegment <= end {
			filteredBackup := backup
			filteredBackups = append(filteredBackups, &filteredBackup)
		}
	}
	return filteredBackups, nil
}

type WalShowOutputWriter interface {
	Write(timelineInfos []*TimelineInfo) error
}

type WalShowJsonOutputWriter struct {
	output io.Writer
}

func (writer *WalShowJsonOutputWriter) Write(timelineInfos []*TimelineInfo) error {
	bytes, err := json.Marshal(timelineInfos)
	if err != nil {
		return err
	}
	_, err = writer.output.Write(bytes)
	return err
}

type WalShowTableOutputWriter struct {
	output io.Writer
	includeBackups bool
}

func (writer *WalShowTableOutputWriter) Write(timelineInfos []*TimelineInfo) error {
	tableWriter := table.NewWriter()
	tableWriter.SetOutputMirror(writer.output)
	defer tableWriter.Render()
	header := table.Row{"TLI", "Parent TLI", "Switchpoint LSN", "Start segment",
		"End segment", "Segment range", "Segments count", "Status"}
	if writer.includeBackups {
		header = append(header, "Backups count")
	}
	tableWriter.AppendHeader(header)

	for _, tl := range timelineInfos {
		row := table.Row{tl.Id, tl.ParentId, tl.SwitchPointLsn, tl.StartSegment,
			tl.EndSegment, tl.SegmentRangeSize, tl.SegmentsCount, tl.Status}
		if writer.includeBackups {
			row = append(row, len(tl.Backups))
		}
		tableWriter.AppendRow(row)
	}

	return nil
}

func NewWalShowOutputWriter(outputType WalShowOutputType, output io.Writer, includeBackups bool) WalShowOutputWriter {
	switch outputType {
	case TableOutput:
		return &WalShowTableOutputWriter{output: output, includeBackups: includeBackups}
	case JsonOutput:
		return &WalShowJsonOutputWriter{output: output}
	default:
		return &WalShowTableOutputWriter{output: output, includeBackups: includeBackups}
	}
}

type WalShowOutputType int

const (
	TableOutput WalShowOutputType = iota + 1
	JsonOutput
)
