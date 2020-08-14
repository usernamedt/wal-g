package internal_test

import (
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal"
	"testing"
)

// TestSegmentSequenceSearchMissingInRange verifies that FindMissingSegments searches for missing
// segments only in range [minSegmentNo, maxSegmentNo]
func TestSegmentSequenceSingleElement(t *testing.T) {
	segmentNo := 5000
	timelineId := uint32(1)

	segmentSequence := internal.NewSegmentsSequence(timelineId, internal.WalSegmentNo(segmentNo))

	missingSegments, err := segmentSequence.FindMissingSegments()
	assert.NoError(t, err)

	assert.Equal(t, 0, len(missingSegments))
}

// TestSegmentSequenceSearchMissingInRange verifies that FindMissingSegments searches for missing
// segments only in range [minSegmentNo, maxSegmentNo]
func TestSegmentSequenceNoMissingSegments(t *testing.T) {
	minSegmentNo := 5000
	maxSegmentNo := 10000
	// no missing segments
	missingSegments := make(map[internal.WalSegmentNo]bool)

	foundMissing, err := testFindMissingElementsInSequence(t, minSegmentNo, maxSegmentNo, missingSegments)
	assert.NoError(t, err)
	
	// check that there are no missing segments found
	assert.True(t, len(foundMissing) == 0)
}

// TestSegmentSequenceSearchMissingInRange verifies that FindMissingSegments searches for missing
// segments only in range [minSegmentNo, maxSegmentNo]
func TestSegmentSequenceSearchMissingInRange(t *testing.T) {
	minSegmentNo := 5000
	maxSegmentNo := 10000
	totalSegmentRange := maxSegmentNo - minSegmentNo
	segmentNumbersToGetLost := make(map[internal.WalSegmentNo]bool)
	// add every even segment number of first quarter to segmentNumbersToGetLost
	// start with 1, because there is no zero segment number in Postgres
	for i := minSegmentNo+1; i < minSegmentNo + totalSegmentRange / 4; i += 2 {
		segmentNumbersToGetLost[internal.WalSegmentNo(i)] = true
	}
	// make second quarter get lost completely
	for i := minSegmentNo + totalSegmentRange / 4; i < minSegmentNo + totalSegmentRange / 2; i++ {
		segmentNumbersToGetLost[internal.WalSegmentNo(i)] = true
	}
	foundMissing, err := testFindMissingElementsInSequence(t, minSegmentNo, maxSegmentNo, segmentNumbersToGetLost)
	assert.NoError(t, err)

	// check that there are no duplicates in list

	assert.True(t, len(foundMissing) == len(segmentNumbersToGetLost))
	for number := range segmentNumbersToGetLost {
		_, exists := foundMissing[number]
		assert.True(t, exists)
	}
}

func testFindMissingElementsInSequence(t *testing.T, minSegmentNo, maxSegmentNo int,
	lostSegmentNumbers map[internal.WalSegmentNo]bool) (map[internal.WalSegmentNo]bool, error) {
	timelineId := uint32(1)

	segmentNumbers := make([]internal.WalSegmentNo, 0)
	for i := minSegmentNo; i < maxSegmentNo; i++ {
		newSegmentNo := internal.WalSegmentNo(i)
		// if this segment number is not in lost set, add it
		if _, exists := lostSegmentNumbers[newSegmentNo]; !exists {
			segmentNumbers = append(segmentNumbers, internal.WalSegmentNo(i))
		}
	}

	segmentSequence := internal.NewSegmentsSequence(timelineId, segmentNumbers[0])
	for _, segmentNo := range segmentNumbers[1:] {
		segmentSequence.AddWalSegmentNo(segmentNo)
	}

	missingSegments, err := segmentSequence.FindMissingSegments()
	if err != nil {
		return nil, err
	}

	// convert missingSegments list to set
	missingNumbersSet := make(map[internal.WalSegmentNo]bool)
	for _, segment := range missingSegments {
		missingNumbersSet[segment.Number] = true
	}

	// check that FindMissingSegments() returned only unique elements
	assert.True(t, len(missingSegments) == len(missingNumbersSet))
	return missingNumbersSet, nil
}

func TestTimelineInfo(t *testing.T) {
	timelineId := uint32(1)
	segmentNo := internal.WalSegmentNo(500)
	walSegment := internal.WalSegmentDescription{Number: segmentNo, Timeline: timelineId}
	walSegmentFilename := walSegment.GetFileName()

	walSegmentSequence := internal.NewSegmentsSequence(timelineId, segmentNo)
	historyRecords := make([]*internal.TimelineHistoryRecord,0)

	timelineInfo, err := internal.NewTimelineInfo(walSegmentSequence, historyRecords)
	assert.NoError(t, err)

	expectedTimelineInfo := internal.TimelineInfo{
		Id: timelineId,
		ParentId: 0,
		SwitchPointLsn: 0,
		StartSegment: walSegmentFilename,
		EndSegment: walSegmentFilename,
		SegmentsCount: 1,
		MissingSegments: make([]string, 0),
		Backups: nil,
		SegmentRangeSize: 1,
		Status: internal.TimelineOkStatus,
	}

	assert.True(t, cmp.Equal(expectedTimelineInfo, *timelineInfo))
}
