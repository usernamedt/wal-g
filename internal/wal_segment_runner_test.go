package internal_test

import (
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal"
	"testing"
)

func TestWalSegmentRunner(t *testing.T) {
	timelineId := uint32(1)
	minSegmentNo := internal.WalSegmentNo(300)
	maxSegmentNo := internal.WalSegmentNo(600)

	walSegments := make(map[internal.WalSegmentDescription]bool, 0)
	for i:=minSegmentNo; i <= maxSegmentNo; i++ {
		segment := internal.WalSegmentDescription{Number: i, Timeline: timelineId}
		walSegments[segment] = true
	}

	startSegment := internal.WalSegmentDescription{Number: maxSegmentNo, Timeline: timelineId}

	walSegmentRunner := internal.NewWalSegmentRunner(startSegment, walSegments, minSegmentNo)

	actualNext, err := walSegmentRunner.Next()
	assert.NoError(t, err)

	expectedNext := internal.WalSegmentDescription{Number: maxSegmentNo - 1, Timeline: timelineId}
	assert.Equal(t, expectedNext, actualNext)

	prevSegment := actualNext
	outputSegments := make(map[internal.WalSegmentDescription]bool)
	for {
		nextSegment , err := walSegmentRunner.Next()
		if err != nil {
			if _, ok := err.(internal.ReachedStopSegmentError); !ok {
				assert.FailNow(t, "TestWalSegmentRunner: unexpected error %v ", err)
			}
			break
		}
		_, exists := outputSegments[nextSegment]
		// each element should be returned only one time
		assert.False(t, exists)

		expectedNext = internal.WalSegmentDescription{Number: prevSegment.Number - 1, Timeline: prevSegment.Timeline}
		assert.Equal(t, expectedNext, nextSegment)

		outputSegments[nextSegment] = true
		prevSegment = nextSegment
	}

	expectedLastSegment := internal.WalSegmentDescription{Number: minSegmentNo, Timeline: timelineId}
	assert.Equal(t, expectedLastSegment, prevSegment)
}


func TestWalSegmentRunnerMissingSegments(t *testing.T) {
	timelineId := uint32(1)
	minSegmentNo := internal.WalSegmentNo(300)
	maxSegmentNo := internal.WalSegmentNo(600)

	walSegments := make(map[internal.WalSegmentDescription]bool, 0)
	for i:=minSegmentNo; i <= maxSegmentNo; i++ {
		segment := internal.WalSegmentDescription{Number: i, Timeline: timelineId}
		walSegments[segment] = true
	}

	missingSegments := make(map[internal.WalSegmentNo]bool)
	// make first half missing entirely
	for i:= minSegmentNo + 1; i < minSegmentNo + maxSegmentNo / 2; i++ {
		missingSegments[i] = true
	}
	// make every odd number missing in second half
	for i:= minSegmentNo + maxSegmentNo / 2 + 1; i < maxSegmentNo; i+=2 {
		missingSegments[i]= true
	}

	startSegment := internal.WalSegmentDescription{Number: maxSegmentNo, Timeline: timelineId}

	walSegmentRunner := internal.NewWalSegmentRunner(startSegment, walSegments, minSegmentNo)

	actualNext, err := walSegmentRunner.Next()
	assert.NoError(t, err)

	expectedNext := internal.WalSegmentDescription{Number: maxSegmentNo - 1, Timeline: timelineId}
	assert.Equal(t, expectedNext, actualNext)

	prevSegment := actualNext
	outputSegments := make(map[internal.WalSegmentDescription]bool)
	SegmentRunnerLoop:
	for {
		nextSegment , err := walSegmentRunner.Next()
		if err != nil {
			switch err := err.(type) {
			case internal.WalSegmentNotFoundError:
				walSegmentRunner.ForceMoveNext()
				_, exists := missingSegments[walSegmentRunner.Current().Number]
				assert.True(t, exists)

				_, exists = outputSegments[nextSegment]
				// each element should be returned only one time
				assert.False(t, exists)
			case internal.ReachedStopSegmentError:
				break SegmentRunnerLoop
			default:
				assert.FailNow(t, "TestWalSegmentRunner: unexpected error %v ", err)
			}
		}
		_, exists := outputSegments[nextSegment]
		// each element should be returned only one time
		assert.False(t, exists)

		expectedNext = internal.WalSegmentDescription{Number: prevSegment.Number - 1, Timeline: prevSegment.Timeline}
		assert.Equal(t, expectedNext, nextSegment)

		outputSegments[nextSegment] = true
		prevSegment = nextSegment
	}

	expectedLastSegment := internal.WalSegmentDescription{Number: minSegmentNo, Timeline: timelineId}
	assert.Equal(t, expectedLastSegment, prevSegment)
}