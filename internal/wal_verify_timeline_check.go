package internal

import (
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/utility"
)

type TimelineCheckResult struct {
	Status                   TimelineCheckStatus `json:"status"`
	CurrentTimelineId        uint32              `json:"current_timeline_id"`
	HighestStorageTimelineId uint32              `json:"highest_storage_timeline_id"`
}

func newTimelineCheckResult(currentTimeline, highestTimeline uint32) TimelineCheckResult {
	result := TimelineCheckResult{
		CurrentTimelineId:        currentTimeline,
		HighestStorageTimelineId: highestTimeline,
		Status:                   TimelineWarning,
	}
	if highestTimeline > 0 {
		if currentTimeline == highestTimeline {
			result.Status = TimelineOk
		} else {
			result.Status = TimelineFailure
		}
	}
	return result
}

type TimelineCheckStatus int

const (
	// Current timeline id matches the highest timeline id found in storage
	TimelineOk TimelineCheckStatus = iota + 1
	// Could not determine if current timeline matches the highest in storage
	TimelineWarning
	// Mismatch: current timeline is not equal to the highest timeline id found in storage
	TimelineFailure
)

func (timelineStatus TimelineCheckStatus) String() string {
	return [...]string{"", "OK", "WARNING", "FAILURE"}[timelineStatus]
}

// MarshalJSON marshals the TimelineCheckStatus enum as a quoted json string
func (timelineStatus TimelineCheckStatus) MarshalJSON() ([]byte, error) {
	return marshalEnumToJSON(timelineStatus)
}

// TODO: Unit tests
func verifyCurrentTimeline(currentTimeline uint32, storageFileNames []string) TimelineCheckResult {
	highestTimeline := tryFindHighestStorageTimelineId(storageFileNames)
	return newTimelineCheckResult(currentTimeline, highestTimeline)
}

// TODO: Unit tests
func tryFindHighestStorageTimelineId(storageFilenames []string) (highestTimelineId uint32) {
	for _, fileName := range storageFilenames {
		fileTimeline, ok := tryParseTimelineIdFromFileName(fileName)
		if !ok {
			tracelog.WarningLogger.Printf(
				"Could not parse the timeline Id from %s. Skipping...",
				fileName)
			continue
		}

		if highestTimelineId < fileTimeline {
			highestTimelineId = fileTimeline
		}
	}
	return highestTimelineId
}

func tryParseTimelineIdFromFileName(fileName string) (timelineId uint32, success bool) {
	// try to parse timeline id from WAL segment file
	baseName := utility.TrimFileExtension(fileName)
	fileTimeline, _, err := ParseWALFilename(baseName)
	if err == nil {
		return fileTimeline, true
	}

	// try to parse timeline id from .history file
	matchResult := walHistoryFileRegexp.FindStringSubmatch(baseName)
	if matchResult == nil || len(matchResult) < 2 {
		return 0, false
	}
	fileTimeline, err = ParseTimelineFromString(matchResult[1])
	return fileTimeline, err == nil
}
