package internal

import "github.com/wal-g/tracelog"

type SegmentScanConfig struct {
	infiniteScan bool
	// scanSegmentsCount is used in case of infiniteScan set to false
	scanSegmentsCount       int
	stopOnFirstFoundSegment bool

	missingSegmentHandleFunc func (segment *WalSegmentDescription)
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

func (sf *WalSegmentsScanner) addExistingSegment(description *WalSegmentDescription) {
	tracelog.DebugLogger.Println("Walked segment " + description.GetFileName())
	sf.existingSegments = append(sf.existingSegments, description)
}

func (sf *WalSegmentsScanner) addLostMissingSegment(description *WalSegmentDescription) {
	tracelog.DebugLogger.Printf("Skipped missing segment (lost) %s\n",
		sf.walSegmentRunner.GetCurrent().GetFileName())
	missingSegment := &MissingSegmentDescription{*description, Lost}
	sf.missingSegments = append(sf.missingSegments, missingSegment)
}

func (sf *WalSegmentsScanner) addUploadingMissingSegment(description *WalSegmentDescription) {
	tracelog.DebugLogger.Printf("Skipped missing segment (probably uploading) %s\n",
		sf.walSegmentRunner.GetCurrent().GetFileName())
	missingSegment := &MissingSegmentDescription{*description, ProbablyUploading}
	sf.missingSegments = append(sf.missingSegments, missingSegment)
}

func (sf *WalSegmentsScanner) addDelayedMissingSegment(description *WalSegmentDescription) {
	tracelog.DebugLogger.Printf("Skipped missing segment (probably delayed) %s\n",
		sf.walSegmentRunner.GetCurrent().GetFileName())
	missingSegment := &MissingSegmentDescription{*description, ProbablyDelayed}
	sf.missingSegments = append(sf.missingSegments, missingSegment)
}

func (sf *WalSegmentsScanner) Scan() error {
	// Run to the latest WAL segment available in storage
	scanConfig := SegmentScanConfig{
		infiniteScan: true,
		stopOnFirstFoundSegment: true,
		missingSegmentHandleFunc: sf.addDelayedMissingSegment,
	}
	err := sf.scanSegments(scanConfig)
	if err != nil {
		return err
	}

	// New startSegment might be chosen if there is some skipped segments after current startSegment
	// because we need a continuous sequence
	scanConfig = SegmentScanConfig{
		scanSegmentsCount: sf.uploadingSegmentRangeSize,
		stopOnFirstFoundSegment: true,
		missingSegmentHandleFunc: sf.addUploadingMissingSegment,
	}
	err = sf.scanSegments(scanConfig)
	if err != nil {
		return err
	}

	scanConfig = SegmentScanConfig{
		infiniteScan: true,
		missingSegmentHandleFunc: sf.addLostMissingSegment,
	}
	// Run to the first storage WAL segment (in sequence)
	return sf.scanSegments(scanConfig)
}

func (sf *WalSegmentsScanner) scanSegments(config SegmentScanConfig) error {
	for i := 0; config.infiniteScan || i < config.scanSegmentsCount; i++ {
		currentSegment, err := sf.walSegmentRunner.MoveNext()
		if err != nil {
			switch err := err.(type) {
			case WalSegmentNotFoundError:
				sf.walSegmentRunner.ForceMoveNext()
				config.missingSegmentHandleFunc(sf.walSegmentRunner.GetCurrent())
				continue
			default:
				return err
			}
		}
		sf.addExistingSegment(currentSegment)
		if config.stopOnFirstFoundSegment {
			return nil
		}
	}
	return nil
}
