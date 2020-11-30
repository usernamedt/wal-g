package internal

import (
	"sort"
)

type WalIntegrityCheckResult struct {
	Status           WalIntegrityCheckStatus            `json:"status"`
	SegmentSequences []*WalIntegrityScanSegmentSequence `json:"segment_sequences"`
}

func newWalIntegrityCheckResult(segmentSequences []*WalIntegrityScanSegmentSequence) WalIntegrityCheckResult {
	result := WalIntegrityCheckResult{SegmentSequences: segmentSequences, Status: IntegrityOk}
	for _, row := range segmentSequences {
		switch row.Status {
		case Lost:
			result.Status = IntegrityFailure
			return result
		case ProbablyDelayed, ProbablyUploading:
			result.Status = IntegrityWarning
		}
	}
	return result
}

// WalIntegrityScanSegmentSequence is a continuous sequence of segments
// with the same timeline and status
type WalIntegrityScanSegmentSequence struct {
	TimelineId    uint32               `json:"timeline_id"`
	StartSegment  string               `json:"start_segment"`
	EndSegment    string               `json:"end_segment"`
	SegmentsCount int                  `json:"segments_count"`
	Status        ScannedSegmentStatus `json:"status"`
}

func newWalIntegrityScanSegmentSequence(sequence *WalSegmentsSequence,
	status ScannedSegmentStatus) *WalIntegrityScanSegmentSequence {
	return &WalIntegrityScanSegmentSequence{
		TimelineId:    sequence.timelineId,
		StartSegment:  sequence.minSegmentNo.getFilename(sequence.timelineId),
		EndSegment:    sequence.maxSegmentNo.getFilename(sequence.timelineId),
		Status:        status,
		SegmentsCount: len(sequence.walSegmentNumbers),
	}
}

type WalIntegrityCheckStatus int

const (
	// No missing segments in storage
	IntegrityOk WalIntegrityCheckStatus = iota + 1
	// Storage contains some ProbablyUploading or ProbablyDelayed missing segments
	IntegrityWarning
	// Storage contains lost missing segments
	IntegrityFailure
)

func (storageStatus WalIntegrityCheckStatus) String() string {
	return [...]string{"", "OK", "WARNING", "FAILURE"}[storageStatus]
}

// MarshalJSON marshals the WalIntegrityCheckStatus enum as a quoted json string
func (storageStatus WalIntegrityCheckStatus) MarshalJSON() ([]byte, error) {
	return marshalEnumToJSON(storageStatus)
}

func verifyWalIntegrity(storageFileNames []string,
	startWalSegment WalSegmentDescription,
	stopWalSegmentNo WalSegmentNo,
	timelineSwitchMap map[WalSegmentNo]*TimelineHistoryRecord,
	uploadingSegmentRangeSize int,
) (WalIntegrityCheckResult, error) {
	storageSegments := getSegmentsFromFiles(storageFileNames)
	walSegmentRunner := NewWalSegmentRunner(startWalSegment, storageSegments, stopWalSegmentNo, timelineSwitchMap)

	segmentScanner := NewWalSegmentScanner(walSegmentRunner)
	err := runWalIntegrityScan(segmentScanner, uploadingSegmentRangeSize)
	if err != nil {
		return WalIntegrityCheckResult{}, err
	}

	integrityScanSegmentSequences, err := collapseSegmentsByStatusAndTimeline(segmentScanner.ScannedSegments)
	if err != nil {
		return WalIntegrityCheckResult{}, err
	}

	return newWalIntegrityCheckResult(integrityScanSegmentSequences), nil
}

// runWalIntegrityScan invokes the following storage scan series
// (on each iteration scanner continues from the position where it stopped)
// 1. At first, it runs scan until it finds some segment in WAL storage
// and marks all encountered missing segments as "missing, probably delayed"
// 2. Then it scans exactly uploadingSegmentRangeSize count of segments,
// if found any missing segments it marks them as "missing, probably still uploading"
// 3. Final scan without any limit (until stopSegment is reached),
// all missing segments encountered in this scan are considered as "missing, lost"
func runWalIntegrityScan(scanner *WalSegmentScanner, uploadingSegmentRangeSize int) error {
	// Run to the latest WAL segment available in storage, mark all missing segments as delayed
	err := scanner.Scan(SegmentScanConfig{
		UnlimitedScan:           true,
		StopOnFirstFoundSegment: true,
		MissingSegmentStatus:    ProbablyDelayed,
	})
	if err != nil {
		return err
	}

	// Traverse potentially uploading segments, mark all missing segments as probably uploading
	err = scanner.Scan(SegmentScanConfig{
		ScanSegmentsLimit:    uploadingSegmentRangeSize,
		MissingSegmentStatus: ProbablyUploading,
	})
	if err != nil {
		return err
	}

	// Run until stop segment, and mark all missing segments as lost
	return scanner.Scan(SegmentScanConfig{
		UnlimitedScan:        true,
		MissingSegmentStatus: Lost,
	})
}

// collapseSegmentsByStatusAndTimeline collapses scanned segments
// with the same timeline and status into segment sequences to minify the output
func collapseSegmentsByStatusAndTimeline(scannedSegments []ScannedSegmentDescription) ([]*WalIntegrityScanSegmentSequence, error) {
	if len(scannedSegments) == 0 {
		return nil, nil
	}

	// make sure that ScannedSegments are ordered
	sort.Slice(scannedSegments, func(i, j int) bool {
		return scannedSegments[i].Number < scannedSegments[j].Number
	})

	segmentSequences := make([]*WalIntegrityScanSegmentSequence, 0)
	currentSequence := NewSegmentsSequence(scannedSegments[0].Timeline, scannedSegments[0].Number)
	currentStatus := scannedSegments[0].status

	for i := 1; i < len(scannedSegments); i++ {
		segment := scannedSegments[i]

		// switch to the new sequence on segment Status change or timeline id change
		if segment.status != currentStatus || currentSequence.timelineId != segment.Timeline {
			segmentSequences = append(segmentSequences, newWalIntegrityScanSegmentSequence(currentSequence, currentStatus))
			currentSequence = NewSegmentsSequence(segment.Timeline, segment.Number)
			currentStatus = segment.status
		} else {
			currentSequence.AddWalSegmentNo(segment.Number)
		}
	}

	segmentSequences = append(segmentSequences, newWalIntegrityScanSegmentSequence(currentSequence, currentStatus))
	return segmentSequences, nil
}
