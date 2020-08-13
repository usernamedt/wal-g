package internal_test

import (
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal"
	"testing"
)

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
