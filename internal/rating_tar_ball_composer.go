package internal

import (
	"archive/tar"
	"fmt"
	"github.com/jackc/pgx"
	"github.com/pkg/errors"
	"github.com/wal-g/wal-g/internal/crypto"
	"os"
	"sort"
	"sync"
)

type RatedComposeFileInfo struct {
	ComposeFileInfo
	updateRating uint64
	updatesCount uint64
	// for regular files this value should match their size on the disk
	// for increments this value is the estimated size of the increment that is going to be created
	expectedSize uint64
}

type TarFilesCollection struct {
	files        []*RatedComposeFileInfo
	expectedSize uint64
}

func newTarFilesCollection() *TarFilesCollection {
	return &TarFilesCollection{files: make([]*RatedComposeFileInfo, 0), expectedSize: 0}
}

func (collection *TarFilesCollection) AddFile(file *RatedComposeFileInfo) {
	collection.files = append(collection.files, file)
	collection.expectedSize += file.expectedSize
}

// RatingTarBallComposer receives all files and tar headers
// that are going to be written to the backup,
// calculates update rating and returns tar headers and files,
// ordered by the update rating
// It also should compose tarballs in the future,
// but atm tarballs composing logic is in the bundle.go
type RatingTarBallComposer struct {
	incrementBaseLsn *uint64
	deltaMap         PagedFileDeltaMap
	deltaMapMutex    sync.Mutex
	deltaMapComplete bool

	tarBallQueue *TarBallQueue

	mutex                  sync.Mutex
	headersToCompose       []*tar.Header
	files                  []*RatedComposeFileInfo
	tarSizeThreshold       uint64
	composeRatingEvaluator ComposeRatingEvaluator
	addFileQueue           chan *ComposeFileInfo
	addFileWaitGroup       sync.WaitGroup
	crypter                crypto.Crypter

	FileStats BundleFileStatistics
	Files     *StatisticsBundleFileList

	tarFilePacker *TarBallFilePacker
}

func NewRatingTarBallComposer(
	tarSizeThreshold uint64, updateRatingEvaluator ComposeRatingEvaluator,
	incrementBaseLsn *uint64, deltaMap PagedFileDeltaMap, tarBallQueue *TarBallQueue,
	crypter crypto.Crypter, conn *pgx.Conn) (*RatingTarBallComposer, error) {

	deltaMapComplete := true
	if deltaMap == nil {
		deltaMapComplete = false
		deltaMap = NewPagedFileDeltaMap()
	}
	fileStats, err := newBundleFileStatistics(conn)
	if err != nil {
		return nil, err
	}
	fileList := newStatisticsBundleFileList(fileStats)
	composer := &RatingTarBallComposer{
		headersToCompose:       make([]*tar.Header, 0),
		files:                  make([]*RatedComposeFileInfo, 0),
		tarSizeThreshold:       tarSizeThreshold,
		incrementBaseLsn:       incrementBaseLsn,
		composeRatingEvaluator: updateRatingEvaluator,
		deltaMapComplete:       deltaMapComplete,
		deltaMap:               deltaMap,
		tarBallQueue:           tarBallQueue,
		crypter:                crypter,
		FileStats:              fileStats,
		Files:                  fileList,
		tarFilePacker:          newTarBallFilePacker(deltaMap, incrementBaseLsn, fileList),
	}
	maxUploadDiskConcurrency, err := getMaxUploadDiskConcurrency()
	if err != nil {
		return nil, err
	}
	composer.addFileQueue = make(chan *ComposeFileInfo, maxUploadDiskConcurrency)
	fmt.Println("Going to start workers")
	for i := 0; i < maxUploadDiskConcurrency; i++ {
		composer.addFileWaitGroup.Add(1)
		fmt.Println("Started worker")
		go composer.launchAddFileWorker(composer.addFileQueue)
	}

	return composer, nil
}

type AddFileTask struct {
	path           string
	info           os.FileInfo
	wasInBase      bool
	fileInfoHeader *tar.Header
	updatesCount   uint64
}

func (c *RatingTarBallComposer) AddFile(info *ComposeFileInfo) {
	c.addFileQueue <- info
}

func (c *RatingTarBallComposer) AddHeader(fileInfoHeader *tar.Header, info os.FileInfo) {
	c.headersToCompose = append(c.headersToCompose, fileInfoHeader)
	c.Files.AddFile(fileInfoHeader, info, false)
}

func (c *RatingTarBallComposer) SkipFile(tarHeader *tar.Header, fileInfo os.FileInfo) {
	c.Files.AddSkippedFile(tarHeader, fileInfo)
}

func (c *RatingTarBallComposer) PackTarballs() (map[string][]string, error) {
	close(c.addFileQueue)
	fmt.Println("Waiting for walk to finish")
	c.addFileWaitGroup.Wait()
	fmt.Println("Walk finished!")
	c.tarFilePacker.UpdateDeltaMap(c.deltaMap)
	headers, tarFilesCollections := c.compose()
	headersTarName, headersNames, err := c.writeHeaders(headers)
	if err != nil {
		return nil, err
	}
	tarFileSets := make(map[string][]string, 0)
	tarFileSets[headersTarName] = headersNames

	for _, tarFilesCollection := range tarFilesCollections {
		tarBall := c.tarBallQueue.Deque()
		tarBall.SetUp(c.crypter)
		for _, composeFileInfo := range tarFilesCollection.files {
			tarFileSets[tarBall.Name()] = append(tarFileSets[tarBall.Name()], composeFileInfo.header.Name)
		}
		// tarFilesCollection closure
		tarFilesCollectionLocal := tarFilesCollection
		go func() {
			for _, fileInfo := range tarFilesCollectionLocal.files {
				err := c.tarFilePacker.PackFileIntoTar(&fileInfo.ComposeFileInfo, tarBall)
				if err != nil {
					panic(err)
				}
			}
			err := c.tarBallQueue.FinishTarBall(tarBall)
			if err != nil {
				panic(err)
			}
		}()
	}

	return tarFileSets, nil
}

func (c *RatingTarBallComposer) GetFiles() SentinelFileList {
	return c.Files
}

func (c *RatingTarBallComposer) launchAddFileWorker(tasks <-chan *ComposeFileInfo) {
	for task := range tasks {
		err := c.addFile(task)
		if err != nil {
			panic(err)
		}
	}
	fmt.Println("Closed worker...")
	c.addFileWaitGroup.Done()
}

func (c *RatingTarBallComposer) addFile(cfi *ComposeFileInfo) error {
	expectedFileSize, err := c.getExpectedFileSize(cfi)
	if err != nil {
		return err
	}
	updatesCount := c.FileStats.getFileUpdateCount(cfi.path)
	updateRating := c.composeRatingEvaluator.Evaluate(cfi.path, updatesCount, cfi.wasInBase)
	ratedComposeFileInfo := &RatedComposeFileInfo{*cfi, updateRating, updatesCount, expectedFileSize}
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.files = append(c.files, ratedComposeFileInfo)
	return nil
}

func (c *RatingTarBallComposer) sortFiles() {
	sort.Slice(c.files, func(i, j int) bool {
		return c.files[i].updateRating < c.files[j].updateRating
	})
}

func (c *RatingTarBallComposer) compose() ([]*tar.Header, []*TarFilesCollection) {
	c.sortFiles()
	tarFilesCollections := make([]*TarFilesCollection, 0)
	currentFilesCollection := newTarFilesCollection()
	prevUpdateRating := uint64(0)

	for _, file := range c.files {
		// if the estimated size of the current collection exceeds the threshold,
		// or if the updateRating just went to non-zero from zero,
		// start packing to the new tar files collection
		if currentFilesCollection.expectedSize > c.tarSizeThreshold ||
			prevUpdateRating == 0 && file.updateRating > 0 {
			tarFilesCollections = append(tarFilesCollections, currentFilesCollection)
			currentFilesCollection = newTarFilesCollection()
		}
		currentFilesCollection.AddFile(file)
		prevUpdateRating = file.updateRating
	}

	tarFilesCollections = append(tarFilesCollections, currentFilesCollection)
	return c.headersToCompose, tarFilesCollections
}

func (c *RatingTarBallComposer) getExpectedFileSize(cfi *ComposeFileInfo) (uint64, error) {
	if !cfi.isIncremented {
		return uint64(cfi.fileInfo.Size()), nil
	}
	if !c.deltaMapComplete {
		err := c.scanDeltaMapFor(cfi.path, cfi.fileInfo.Size())
		if err != nil {
			return 0, err
		}
	}
	bitmap, err := c.deltaMap.GetDeltaBitmapFor(cfi.path)
	if _, ok := err.(NoBitmapFoundError); ok {
		// this file has changed after the start of backup and will be skipped
		// so the expected size in tar is zero
		return 0, nil
	}
	if err != nil {
		return 0, errors.Wrapf(err, "getExpectedFileSize: failed to find corresponding bitmap '%s'\n", cfi.path)
	}
	incrementBlocksCount := bitmap.GetCardinality()
	// expected header size = length(IncrementFileHeader) + sizeOf(fileSize) + sizeOf(diffBlockCount) + sizeOf(blockNo)*incrementBlocksCount
	incrementHeaderSize := uint64(len(IncrementFileHeader)) + sizeofInt64 + sizeofInt32 + (incrementBlocksCount * sizeofInt32)
	incrementPageDataSize := incrementBlocksCount * uint64(DatabasePageSize)
	fmt.Printf("Increment: %s, expected size: %d, blocks: %d \n", cfi.path, incrementHeaderSize+incrementPageDataSize, incrementBlocksCount)
	return incrementHeaderSize + incrementPageDataSize, nil
}

func (c *RatingTarBallComposer) scanDeltaMapFor(filePath string, fileSize int64) error {
	locations, err := ReadIncrementLocations(filePath, fileSize, *c.incrementBaseLsn)
	if err != nil {
		return err
	}
	c.deltaMapMutex.Lock()
	defer c.deltaMapMutex.Unlock()
	if len(locations) == 0 {
		return nil
	}
	c.deltaMap.AddLocationsToDelta(locations)
	return nil
}

func (c *RatingTarBallComposer) writeHeaders(headers []*tar.Header) (string, []string, error) {
	headersTarBall := c.tarBallQueue.Deque()
	headersTarBall.SetUp(c.crypter)
	headersNames := make([]string, 0, len(headers))
	for _, header := range headers {
		err := headersTarBall.TarWriter().WriteHeader(header)
		headersNames = append(headersNames, header.Name)
		if err != nil {
			return "", nil, errors.Wrap(err, "addToBundle: failed to write header")
		}
	}
	c.tarBallQueue.EnqueueBack(headersTarBall)
	return headersTarBall.Name(), headersNames, nil
}
