package internal

import "github.com/wal-g/tracelog"

type SegmentScanConfig struct {
	infiniteScan bool
	// scanSegmentsCount is used in case of infiniteScan set to false
	scanSegmentsCount       int
	stopOnFirstFoundSegment bool

	missingSegmentHandleFunc func(segment *WalSegmentDescription)
}

type WalSegmentsScanner struct {
	missingSegments  []*MissingSegmentDescription
	existingSegments []*WalSegmentDescription

	walSegmentRunner          *WalSegmentRunner
	uploadingSegmentRangeSize int
}

func NewWalSegmentsScanner(walSegmentRunner *WalSegmentRunner, uploadingSegmentRangeSize int) *WalSegmentsScanner {
	return &WalSegmentsScanner{
		missingSegments:  make([]*MissingSegmentDescription, 0),
		existingSegments: make([]*WalSegmentDescription, 0),

		walSegmentRunner:          walSegmentRunner,
		uploadingSegmentRangeSize: uploadingSegmentRangeSize,
	}
}

func (scanner *WalSegmentsScanner) addExistingSegment(description *WalSegmentDescription) {
	tracelog.DebugLogger.Println("Walked segment " + description.GetFileName())
	scanner.existingSegments = append(scanner.existingSegments, description)
}

func (scanner *WalSegmentsScanner) addLostMissingSegment(description *WalSegmentDescription) {
	tracelog.DebugLogger.Printf("Skipped missing segment (lost) %s\n",
		scanner.walSegmentRunner.GetCurrent().GetFileName())
	missingSegment := &MissingSegmentDescription{*description, Lost}
	scanner.missingSegments = append(scanner.missingSegments, missingSegment)
}

func (scanner *WalSegmentsScanner) addUploadingMissingSegment(description *WalSegmentDescription) {
	tracelog.DebugLogger.Printf("Skipped missing segment (probably uploading) %s\n",
		scanner.walSegmentRunner.GetCurrent().GetFileName())
	missingSegment := &MissingSegmentDescription{*description, ProbablyUploading}
	scanner.missingSegments = append(scanner.missingSegments, missingSegment)
}

func (scanner *WalSegmentsScanner) addDelayedMissingSegment(description *WalSegmentDescription) {
	tracelog.DebugLogger.Printf("Skipped missing segment (probably delayed) %s\n",
		scanner.walSegmentRunner.GetCurrent().GetFileName())
	missingSegment := &MissingSegmentDescription{*description, ProbablyDelayed}
	scanner.missingSegments = append(scanner.missingSegments, missingSegment)
}

func (scanner *WalSegmentsScanner) scanSegments(config SegmentScanConfig) error {
	for i := 0; config.infiniteScan || i < config.scanSegmentsCount; i++ {
		currentSegment, err := scanner.walSegmentRunner.MoveNext()
		if err != nil {
			switch err := err.(type) {
			case WalSegmentNotFoundError:
				scanner.walSegmentRunner.ForceMoveNext()
				config.missingSegmentHandleFunc(scanner.walSegmentRunner.GetCurrent())
				continue
			default:
				return err
			}
		}
		scanner.addExistingSegment(currentSegment)
		if config.stopOnFirstFoundSegment {
			return nil
		}
	}
	return nil
}

func (scanner *WalSegmentsScanner) GetMissingSegmentsDescriptions() []*WalSegmentDescription {
	result := make([]*WalSegmentDescription, 0, len(scanner.missingSegments))
	for _, segment := range scanner.missingSegments {
		result = append(result, &segment.WalSegmentDescription)
	}
	return result
}
