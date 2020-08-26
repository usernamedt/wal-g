package internal

import "github.com/wal-g/tracelog"

// WalSegmentScanner is used to scan the WAL segments storage
type WalSegmentScanner struct {
	scannedSegments  []ScannedSegmentDescription
	walSegmentRunner *WalSegmentRunner
}

// SegmentScanConfig is used to configure the single Scan() call of the WalSegmentScanner
type SegmentScanConfig struct {
	unlimitedScan bool
	// scanSegmentsLimit is used in case of unlimitedScan is set to false
	scanSegmentsLimit       int
	stopOnFirstFoundSegment bool

	missingSegmentHandler func(segment WalSegmentDescription)
}

func NewWalSegmentScanner(walSegmentRunner *WalSegmentRunner) *WalSegmentScanner {
	return &WalSegmentScanner{
		scannedSegments:  make([]ScannedSegmentDescription, 0),
		walSegmentRunner: walSegmentRunner,
	}
}

func (scanner *WalSegmentScanner) Scan(config SegmentScanConfig) error {
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

// GetMissingSegmentsDescriptions returns a slice containing WalSegmentDescription of each missing segment
func (scanner *WalSegmentScanner) GetMissingSegmentsDescriptions() []WalSegmentDescription {
	result := make([]WalSegmentDescription, 0)
	for _, segment := range scanner.scannedSegments {
		if segment.status != Found {
			result = append(result, segment.WalSegmentDescription)
		}
	}
	return result
}

func (scanner *WalSegmentScanner) addFoundSegment(description WalSegmentDescription) {
	tracelog.DebugLogger.Println("Found segment " + description.GetFileName())
	foundSegment := ScannedSegmentDescription{description, Found}
	scanner.scannedSegments = append(scanner.scannedSegments, foundSegment)
}

func (scanner *WalSegmentScanner) addMissingLostSegment(description WalSegmentDescription) {
	tracelog.DebugLogger.Printf("Missing segment (lost) %s\n",
		scanner.walSegmentRunner.Current().GetFileName())
	missingSegment := ScannedSegmentDescription{description, Lost}
	scanner.scannedSegments = append(scanner.scannedSegments, missingSegment)
}

func (scanner *WalSegmentScanner) addMissingUploadingSegment(description WalSegmentDescription) {
	tracelog.DebugLogger.Printf("Missing segment (probably uploading) %s\n",
		scanner.walSegmentRunner.Current().GetFileName())
	missingSegment := ScannedSegmentDescription{description, ProbablyUploading}
	scanner.scannedSegments = append(scanner.scannedSegments, missingSegment)
}

func (scanner *WalSegmentScanner) addMissingDelayedSegment(description WalSegmentDescription) {
	tracelog.DebugLogger.Printf("Missing segment (probably delayed) %s\n",
		scanner.walSegmentRunner.Current().GetFileName())
	missingSegment := ScannedSegmentDescription{description, ProbablyDelayed}
	scanner.scannedSegments = append(scanner.scannedSegments, missingSegment)
}
