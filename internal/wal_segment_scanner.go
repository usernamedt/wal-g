package internal

import "github.com/wal-g/tracelog"

type SegmentScanConfig struct {
	unlimitedScan bool
	// scanSegmentsLimit is used in case of unlimitedScan set to false
	scanSegmentsLimit       int
	stopOnFirstFoundSegment bool

	missingSegmentHandler func(segment *WalSegmentDescription)
}

type WalSegmentsScanner struct {
	scannedSegments  []*ScannedSegmentDescription
	walSegmentRunner          *WalSegmentRunner
	uploadingSegmentRangeSize int
}

func NewWalSegmentsScanner(walSegmentRunner *WalSegmentRunner, uploadingSegmentRangeSize int) *WalSegmentsScanner {
	return &WalSegmentsScanner{
		scannedSegments:  make([]*ScannedSegmentDescription, 0),

		walSegmentRunner:          walSegmentRunner,
		uploadingSegmentRangeSize: uploadingSegmentRangeSize,
	}
}

func (scanner *WalSegmentsScanner) addFoundSegment(description *WalSegmentDescription) {
	tracelog.DebugLogger.Println("Walked segment " + description.GetFileName())
	foundSegment := &ScannedSegmentDescription{*description, Found}
	scanner.scannedSegments = append(scanner.scannedSegments, foundSegment)
}

func (scanner *WalSegmentsScanner) addLostMissingSegment(description *WalSegmentDescription) {
	tracelog.DebugLogger.Printf("Skipped missing segment (lost) %s\n",
		scanner.walSegmentRunner.GetCurrent().GetFileName())
	missingSegment := &ScannedSegmentDescription{*description, Lost}
	scanner.scannedSegments = append(scanner.scannedSegments, missingSegment)
}

func (scanner *WalSegmentsScanner) addUploadingMissingSegment(description *WalSegmentDescription) {
	tracelog.DebugLogger.Printf("Skipped missing segment (probably uploading) %s\n",
		scanner.walSegmentRunner.GetCurrent().GetFileName())
	missingSegment := &ScannedSegmentDescription{*description, ProbablyUploading}
	scanner.scannedSegments = append(scanner.scannedSegments, missingSegment)
}

func (scanner *WalSegmentsScanner) addDelayedMissingSegment(description *WalSegmentDescription) {
	tracelog.DebugLogger.Printf("Skipped missing segment (probably delayed) %s\n",
		scanner.walSegmentRunner.GetCurrent().GetFileName())
	missingSegment := &ScannedSegmentDescription{*description, ProbablyDelayed}
	scanner.scannedSegments = append(scanner.scannedSegments, missingSegment)
}

func (scanner *WalSegmentsScanner) Scan(config SegmentScanConfig) error {
	for i := 0; config.unlimitedScan || i < config.scanSegmentsLimit; i++ {
		currentSegment, err := scanner.walSegmentRunner.MoveNext()
		if err != nil {
			switch err := err.(type) {
			case WalSegmentNotFoundError:
				scanner.walSegmentRunner.ForceMoveNext()
				config.missingSegmentHandler(scanner.walSegmentRunner.GetCurrent())
				continue
			default:
				return err
			}
		}
		scanner.addFoundSegment(currentSegment)
		if config.stopOnFirstFoundSegment {
			return nil
		}
	}
	return nil
}

func (scanner *WalSegmentsScanner) GetMissingSegmentsDescriptions() []*WalSegmentDescription {
	result := make([]*WalSegmentDescription, 0, len(scanner.scannedSegments))
	for _, segment := range scanner.scannedSegments {
		result = append(result, &segment.WalSegmentDescription)
	}
	return result
}
