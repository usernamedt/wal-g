package internal

import (
	"github.com/pkg/errors"
	"github.com/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
	"time"
)

func getDeltaMap(folder storage.Folder, timeline uint32, firstUsedLSN, firstNotUsedLSN uint64) (PagedFileDeltaMap, error) {
	tracelog.InfoLogger.Printf("Timeline: %d, FirstUsedLsn: %d, FirstNotUsedLsn: %d\n", timeline, firstUsedLSN, firstNotUsedLSN)
	tracelog.InfoLogger.Printf("First WAL should participate in building delta map: %s", newWalSegmentNo(firstUsedLSN).getFilename(timeline))
	tracelog.InfoLogger.Printf("First WAL shouldn't participate in building delta map: %s", newWalSegmentNo(firstNotUsedLSN).getFilename(timeline))
	deltaMap := NewPagedFileDeltaMap()
	firstUsedDeltaNo, firstNotUsedDeltaNo := getDeltaRange(firstUsedLSN, firstNotUsedLSN)
	// Get locations from [firstUsedDeltaNo, lastUsedDeltaNo). We use lastUsedDeltaNo in next step
	time.Sleep(10 * time.Second)
	err := deltaMap.getLocationsFromDeltas(folder, timeline, firstUsedDeltaNo, firstNotUsedDeltaNo.previous())
	if err != nil {
		return deltaMap, errors.Wrapf(err, "Error during fetch locations from delta files.\n")
	}

	time.Sleep(10 * time.Second)
	// Handle last delta file separately for fetch locations and walParser from it
	lastDeltaFile, err := getDeltaFile(folder, firstNotUsedDeltaNo.previous().getFilename(timeline))
	if err != nil {
		return deltaMap, errors.Wrapf(err, "Error during downloading last delta file.\n")
	}
	time.Sleep(10 * time.Second)
	deltaMap.AddLocationsToDelta(lastDeltaFile.Locations)

	time.Sleep(10 * time.Second)
	firstUsedWalSegmentNo, lastUsedWalSegmentNo := getWalSegmentRange(firstNotUsedDeltaNo, firstUsedLSN, firstNotUsedLSN)

	time.Sleep(10 * time.Second)
	// we handle WAL files from [firstUsedWalSegmentNo, lastUsedWalSegmentNo]
	err = deltaMap.getLocationsFromWals(folder, timeline, firstUsedWalSegmentNo, lastUsedWalSegmentNo, lastDeltaFile.WalParser)
	if err != nil {
		return deltaMap, errors.Wrapf(err, "Error during fetch locations from wal segments.\n")
	}
	return deltaMap, nil
}

func getDeltaRange(firstUsedLsn, firstNotUsedLsn uint64) (DeltaNo, DeltaNo) {
	firstUsedDeltaNo := newDeltaNoFromLsn(firstUsedLsn)
	firstNotUsedDeltaNo := newDeltaNoFromLsn(firstNotUsedLsn)
	return firstUsedDeltaNo, firstNotUsedDeltaNo
}

func getWalSegmentRange(firstNotUsedDeltaNo DeltaNo, firstUsedLsn, firstNotUsedLsn uint64) (WalSegmentNo, WalSegmentNo) {
	firstUsedWalSegmentNo := getFirstUsedWalSegmentNo(firstNotUsedDeltaNo, firstUsedLsn)
	lastUsedLsn := firstNotUsedLsn - 1
	lastUsedWalSegmentNo := newWalSegmentNo(lastUsedLsn)
	return firstUsedWalSegmentNo, lastUsedWalSegmentNo
}

func getFirstUsedWalSegmentNo(firstNotUsedDeltaNo DeltaNo, firstUsedLsn uint64) WalSegmentNo {
	firstUsedLsnSegmentNo := newWalSegmentNo(firstUsedLsn)
	firstNotUsedDeltaSegmentNo := firstNotUsedDeltaNo.firstWalSegmentNo()
	if firstUsedLsnSegmentNo > firstNotUsedDeltaSegmentNo {
		return firstUsedLsnSegmentNo
	}
	return firstNotUsedDeltaSegmentNo
}