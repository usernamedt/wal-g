package internal_test

import (
	"bytes"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal"
	"reflect"
	"testing"
)

type WalVerifyTestSetup struct {
	expectedIntegrityCheck internal.WalVerifyCheckResult
	// highestTimelineId stores the highest timeline id among testTimelineSetups
	expectedTimelineCheck internal.WalVerifyCheckResult
	// wal-verify should scan the [startWalSegment, endWalSegment] range
	expectedStartSegment internal.WalSegmentDescription
	expectedEndSegment   internal.WalSegmentDescription

	// currentWalSegment represents the current cluster wal segment
	currentWalSegment internal.WalSegmentDescription
	timelineSetups    []*TestTimelineSetup
	storageFiles      map[string]*bytes.Buffer
}

func init() {
	// set upload disk concurrency to non-zero value
	viper.Set(internal.UploadConcurrencySetting, "1")
}

// MockWalVerifyOutputWriter is used to capture wal-verify command output
type MockWalVerifyOutputWriter struct {
	lastResult map[internal.WalVerifyCheckType]internal.WalVerifyCheckResult
	// number of time Write() function has been called
	writeCallsCount int
}

func (writer *MockWalVerifyOutputWriter) Write(
	result map[internal.WalVerifyCheckType]internal.WalVerifyCheckResult) error {
	writer.lastResult = result
	writer.writeCallsCount += 1
	return nil
}

// the following tests cover only cases without any backups in storage,
// so the scan should proceed up to the very first segment number (0000000X0000000000000001)

func TestWalVerify_EmptyStorage(t *testing.T) {
	var timelineSetups []*TestTimelineSetup = nil

	currentSegment := internal.WalSegmentDescription{
		Number:   10,
		Timeline: 3,
	}
	expectedStartSegment := internal.WalSegmentDescription{
		Number:   1,
		Timeline: currentSegment.Timeline,
	}
	expectedEndSegment := internal.WalSegmentDescription{
		Number:   currentSegment.Number.Previous(),
		Timeline: currentSegment.Timeline,
	}

	expectedIntegrityCheck := internal.WalVerifyCheckResult{
		Status: internal.StatusWarning,
		Details: internal.IntegrityCheckDetails{
			{
				TimelineId:    3,
				StartSegment:  expectedStartSegment.GetFileName(),
				EndSegment:    expectedEndSegment.GetFileName(),
				SegmentsCount: 9,
				Status:        internal.ProbablyDelayed,
			},
		},
	}

	expectedTimelineCheck := internal.WalVerifyCheckResult{
		Status: internal.StatusWarning,
		Details: internal.TimelineCheckDetails{
			CurrentTimelineId:        currentSegment.Timeline,
			HighestStorageTimelineId: 0,
		},
	}

	testWalVerify(t, WalVerifyTestSetup{
		expectedIntegrityCheck: expectedIntegrityCheck,
		expectedTimelineCheck: expectedTimelineCheck,
		expectedStartSegment:  expectedStartSegment,
		expectedEndSegment:    expectedEndSegment,
		currentWalSegment:     currentSegment,
		timelineSetups:        timelineSetups,
	})
}

func TestWalVerify_OnlyGarbageInStorage(t *testing.T) {
	var timelineSetups []*TestTimelineSetup = nil

	storageFiles := map[string]*bytes.Buffer{
		"some_garbage_file":        new(bytes.Buffer),
		"00000007000000000000000K": new(bytes.Buffer),
		" ":                        new(bytes.Buffer),
	}
	currentSegment := internal.WalSegmentDescription{
		Number:   10,
		Timeline: 3,
	}
	expectedStartSegment := internal.WalSegmentDescription{
		Number:   1,
		Timeline: currentSegment.Timeline,
	}
	expectedEndSegment := internal.WalSegmentDescription{
		Number:   currentSegment.Number.Previous(),
		Timeline: currentSegment.Timeline,
	}

	expectedIntegrityCheck := internal.WalVerifyCheckResult{
		Status: internal.StatusWarning,
		Details: internal.IntegrityCheckDetails{
			{
				TimelineId:    3,
				StartSegment:  expectedStartSegment.GetFileName(),
				EndSegment:    expectedEndSegment.GetFileName(),
				SegmentsCount: 9,
				Status:        internal.ProbablyDelayed,
			},
		},
	}

	expectedTimelineCheck := internal.WalVerifyCheckResult{
		Status: internal.StatusWarning,
		Details: internal.TimelineCheckDetails{
			CurrentTimelineId: currentSegment.Timeline,
			// WAL storage folder is empty so highest found timeline should be zero
			HighestStorageTimelineId: 0,
		},
	}

	testWalVerify(t, WalVerifyTestSetup{
		expectedIntegrityCheck: expectedIntegrityCheck,
		expectedTimelineCheck: expectedTimelineCheck,
		expectedStartSegment:  expectedStartSegment,
		expectedEndSegment:    expectedEndSegment,
		currentWalSegment:     currentSegment,
		timelineSetups:        timelineSetups,
		storageFiles: storageFiles,
	})
}

func TestWalVerify_SingleTimeline_Ok(t *testing.T) {
	timelineSetups := []*TestTimelineSetup{
		{
			existSegments: []string{
				"000000050000000000000001",
				"000000050000000000000002",
				"000000050000000000000003",
				"000000050000000000000004",
			},
		},
	}

	storageFiles := make(map[string]*bytes.Buffer)
	currentSegment := internal.WalSegmentDescription{
		Number:   5,
		Timeline: 5,
	}
	expectedStartSegment := internal.WalSegmentDescription{
		Number:   1,
		Timeline: currentSegment.Timeline,
	}
	expectedEndSegment := internal.WalSegmentDescription{
		Number:   currentSegment.Number.Previous(),
		Timeline: currentSegment.Timeline,
	}

	expectedIntegrityCheck := internal.WalVerifyCheckResult{
		Status: internal.StatusOk,
		Details: internal.IntegrityCheckDetails{
			{
				TimelineId:    5,
				StartSegment:  expectedStartSegment.GetFileName(),
				EndSegment:    expectedEndSegment.GetFileName(),
				SegmentsCount: 4,
				Status:        internal.Found,
			},
		},
	}

	expectedTimelineCheck := internal.WalVerifyCheckResult{
		Status: internal.StatusOk,
		Details: internal.TimelineCheckDetails{
			CurrentTimelineId: currentSegment.Timeline,
			HighestStorageTimelineId: currentSegment.Timeline,
		},
	}

	testWalVerify(t, WalVerifyTestSetup{
		expectedIntegrityCheck: expectedIntegrityCheck,
		expectedTimelineCheck:  expectedTimelineCheck,
		expectedStartSegment:   expectedStartSegment,
		expectedEndSegment:     expectedEndSegment,
		currentWalSegment:      currentSegment,
		timelineSetups:         timelineSetups,
		storageFiles:           storageFiles,
	})
}

func TestWalVerify_SingleTimeline_SomeDelayed(t *testing.T) {
	timelineSetups := []*TestTimelineSetup{
		{
			existSegments: []string{
				"000000050000000000000001",
				"000000050000000000000002",
				"000000050000000000000003",
				"000000050000000000000004",
			},
		},
	}

	storageFiles := make(map[string]*bytes.Buffer)
	currentSegment := internal.WalSegmentDescription{
		Number:   25,
		Timeline: 5,
	}
	expectedStartSegment := internal.WalSegmentDescription{
		Number:   1,
		Timeline: currentSegment.Timeline,
	}
	expectedEndSegment := internal.WalSegmentDescription{
		Number:   currentSegment.Number.Previous(),
		Timeline: currentSegment.Timeline,
	}

	expectedIntegrityCheck := internal.WalVerifyCheckResult{
		Status: internal.StatusWarning,
		Details: internal.IntegrityCheckDetails{
			{
				TimelineId:    5,
				StartSegment:  expectedStartSegment.GetFileName(),
				EndSegment:    "000000050000000000000004",
				SegmentsCount: 4,
				Status:        internal.Found,
			},
			{
				TimelineId:    5,
				StartSegment:  "000000050000000000000005",
				EndSegment:    expectedEndSegment.GetFileName(),
				SegmentsCount: 20,
				Status:        internal.ProbablyDelayed,
			},
		},
	}

	expectedTimelineCheck := internal.WalVerifyCheckResult{
		Status: internal.StatusOk,
		Details: internal.TimelineCheckDetails{
			CurrentTimelineId: currentSegment.Timeline,
			HighestStorageTimelineId: currentSegment.Timeline,
		},
	}

	testWalVerify(t, WalVerifyTestSetup{
		expectedIntegrityCheck: expectedIntegrityCheck,
		expectedTimelineCheck:  expectedTimelineCheck,
		expectedStartSegment:   expectedStartSegment,
		expectedEndSegment:     expectedEndSegment,
		currentWalSegment:      currentSegment,
		timelineSetups:         timelineSetups,
		storageFiles:           storageFiles,
	})
}

func testWalVerify(t *testing.T, setup WalVerifyTestSetup) {
	expectedResult := map[internal.WalVerifyCheckType]internal.WalVerifyCheckResult{
		internal.WalVerifyTimelineCheck:  setup.expectedTimelineCheck,
		internal.WalVerifyIntegrityCheck: setup.expectedIntegrityCheck,
	}

	walFilenames := concatWalFilenames(setup.timelineSetups)
	for _, timeline := range setup.timelineSetups {
		if timeline.historyFileContents == "" {
			continue
		}
		historyFileName, historyContents, err := timeline.GetHistory()
		assert.NoError(t, err)
		setup.storageFiles[historyFileName] = historyContents
	}

	result := executeWalVerify(
		walFilenames,
		setup.storageFiles,
		setup.currentWalSegment)

	compareResults(t, expectedResult, result)
}

// executeWalShow invokes the HandleWalVerify() with fake storage filled with
// provided wal segments and other wal folder files
func executeWalVerify(
	walFilenames []string,
	walFolderFiles map[string]*bytes.Buffer,
	currentWalSegment internal.WalSegmentDescription,
) map[internal.WalVerifyCheckType]internal.WalVerifyCheckResult {
	for _, name := range walFilenames {
		// we don't use the WAL file contents so let it be it empty inside
		walFolderFiles[name] = new(bytes.Buffer)
	}

	folder := setupTestStorageFolder(walFolderFiles)
	mockOutputWriter := &MockWalVerifyOutputWriter{}
	checkTypes := []internal.WalVerifyCheckType{
		internal.WalVerifyTimelineCheck, internal.WalVerifyIntegrityCheck}

	internal.HandleWalVerify(checkTypes, folder, currentWalSegment, mockOutputWriter)

	return mockOutputWriter.lastResult
}

func compareResults(
	t *testing.T,
	expected map[internal.WalVerifyCheckType]internal.WalVerifyCheckResult,
	returned map[internal.WalVerifyCheckType]internal.WalVerifyCheckResult) {

	assert.Equal(t, len(expected), len(returned))

	for checkType, checkResult := range returned {
		assert.Contains(t, expected, checkType)
		assert.Equal(t, expected[checkType].Status, checkResult.Status,
			"Result status doesn't match the expected status")

		assert.True(t, reflect.DeepEqual(expected[checkType].Details, checkResult.Details),
			"Result details don't match the expected values")
	}
}
