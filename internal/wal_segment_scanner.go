package internal

import "github.com/wal-g/tracelog"

// WalSegmentsScanner is used to scan the WAL segments storage
type WalSegmentsScanner struct {
	scannedSegments           []ScannedSegmentDescription
	walSegmentRunner          *WalSegmentRunner
	uploadingSegmentRangeSize int
}

// SegmentScanConfig is used to configure the single scan() call of the WalSegmentsScanner
type SegmentScanConfig struct {
	unlimitedScan bool
	// scanSegmentsLimit is used in case of unlimitedScan set to false
	scanSegmentsLimit       int
	stopOnFirstFoundSegment bool

	missingSegmentHandler func(segment WalSegmentDescription)
}

func NewWalSegmentsScanner(walSegmentRunner *WalSegmentRunner, uploadingSegmentRangeSize int) *WalSegmentsScanner {
	return &WalSegmentsScanner{
		scannedSegments: make([]ScannedSegmentDescription, 0),

		walSegmentRunner:          walSegmentRunner,
		uploadingSegmentRangeSize: uploadingSegmentRangeSize,
	}
}

func (scanner *WalSegmentsScanner) scan(config SegmentScanConfig) error {
	// scan may have a limited number of iterations, or may be unlimited
	for i := 0; config.unlimitedScan || i < config.scanSegmentsLimit; i++ {
		currentSegment, err := scanner.walSegmentRunner.Next()
		if err != nil {
			switch err := err.(type) {
			case WalSegmentNotFoundError:
				scanner.walSegmentRunner.ForceMoveNext()
				config.missingSegmentHandler(scanner.walSegmentRunner.Current())
				continue
			case ReachedStopSegmentError:
				return nil
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

func (scanner *WalSegmentsScanner) addFoundSegment(description WalSegmentDescription) {
	tracelog.DebugLogger.Println("Walked segment " + description.GetFileName())
	foundSegment := ScannedSegmentDescription{description, Found}
	scanner.scannedSegments = append(scanner.scannedSegments, foundSegment)
}

func (scanner *WalSegmentsScanner) addMissingLostSegment(description WalSegmentDescription) {
	tracelog.DebugLogger.Printf("Skipped missing segment (lost) %s\n",
		scanner.walSegmentRunner.Current().GetFileName())
	missingSegment := ScannedSegmentDescription{description, Lost}
	scanner.scannedSegments = append(scanner.scannedSegments, missingSegment)
}

func (scanner *WalSegmentsScanner) addMissingUploadingSegment(description WalSegmentDescription) {
	tracelog.DebugLogger.Printf("Skipped missing segment (probably uploading) %s\n",
		scanner.walSegmentRunner.Current().GetFileName())
	missingSegment := ScannedSegmentDescription{description, ProbablyUploading}
	scanner.scannedSegments = append(scanner.scannedSegments, missingSegment)
}

func (scanner *WalSegmentsScanner) addMissingDelayedSegment(description WalSegmentDescription) {
	tracelog.DebugLogger.Printf("Skipped missing segment (probably delayed) %s\n",
		scanner.walSegmentRunner.Current().GetFileName())
	missingSegment := ScannedSegmentDescription{description, ProbablyDelayed}
	scanner.scannedSegments = append(scanner.scannedSegments, missingSegment)
}
