package internal

import (
	"archive/tar"
	"fmt"
	"github.com/pkg/errors"
	"os"
	"sort"
	"sync"
)

// TarBallComposer receives all files and tar headers
// that are going to be written to the backup,
// calculates update rating and returns tar headers and files,
// ordered by the update rating
// It also should compose tarballs in the future,
// but atm tarballs composing logic is in the bundle.go
type TarBallComposer struct {
	incrementBaseLsn *uint64
	forceIncremental bool
	deltaMap PagedFileDeltaMap
	deltaMapMutex sync.Mutex
	deltaMapComplete bool

	mutex            sync.Mutex
	headersToCompose       []*tar.Header
	files                  []*ComposeFileInfo
	tarSizeThreshold       uint64
	composeRatingEvaluator ComposeRatingEvaluator
	addFileQueue chan *AddFileTask
	addFileWaitGroup sync.WaitGroup
}

type AddFileTask struct {
	path           string
	info           os.FileInfo
	wasInBase      bool
	fileInfoHeader *tar.Header
	updatesCount   uint64
}

func (c *TarBallComposer) addFileToComposer(tasks <-chan *AddFileTask) {
	for task := range tasks {
		err := c.addFile(task)
		if err != nil {
			panic(err)
		}
	}
	fmt.Println("Closed worker...")
	c.addFileWaitGroup.Done()
}

func NewTarBallComposer(tarSizeThreshold uint64, updateRatingEvaluator ComposeRatingEvaluator,
	incrementBaseLsn *uint64, forceIncremental bool, deltaMap PagedFileDeltaMap) (*TarBallComposer, error) {
	deltaMapComplete := true
	if deltaMap == nil {
		deltaMapComplete = false
		deltaMap = NewPagedFileDeltaMap()
	}
	composer := &TarBallComposer{
		headersToCompose: make([]*tar.Header,0),
		files: make([]*ComposeFileInfo,0),
		tarSizeThreshold: tarSizeThreshold,
		composeRatingEvaluator: updateRatingEvaluator,
		incrementBaseLsn: incrementBaseLsn,
		forceIncremental: forceIncremental,
		deltaMapComplete: deltaMapComplete,
		deltaMap: deltaMap,
	}
	maxUploadDiskConcurrency, err := getMaxUploadDiskConcurrency()
	if err != nil {
		return nil, err
	}
	composer.addFileQueue = make(chan *AddFileTask, maxUploadDiskConcurrency)
	fmt.Println("Going to start workers")
	for i := 0; i < maxUploadDiskConcurrency; i++ {
		composer.addFileWaitGroup.Add(1)
		fmt.Println("Started worker")
		go composer.addFileToComposer(composer.addFileQueue)
	}

	return composer, nil
}

type ComposeFileInfo struct {
	path string
	fileInfo os.FileInfo
	wasInBase bool
	updateRating uint64
	header *tar.Header
	updatesCount uint64
	// for regular files this value should match their size on the disk
	// for increments this value is the estimated size of the increment that is going to be created
	expectedSize uint64
}

func newComposeFileInfo(path string, fileInfo os.FileInfo, wasInBase bool,
	header *tar.Header, updatesCount, updateRating, expectedFileSize uint64) *ComposeFileInfo {
	return &ComposeFileInfo{path: path, fileInfo: fileInfo, wasInBase: wasInBase,
		updateRating: updateRating, header: header, updatesCount: updatesCount, expectedSize: expectedFileSize}
}

type ComposeRatingEvaluator interface {
	Evaluate(path string, updatesCount uint64, wasInBase bool) uint64
}

type DefaultComposeRatingEvaluator struct {
	incrementFromFiles BackupFileList
}

func NewDefaultComposeRatingEvaluator(incrementFromFiles BackupFileList) *DefaultComposeRatingEvaluator {
	return &DefaultComposeRatingEvaluator{incrementFromFiles: incrementFromFiles}
}

type TarFilesCollection struct {
	files        []*ComposeFileInfo
	expectedSize uint64
}

func newTarFilesCollection() *TarFilesCollection {
	return &TarFilesCollection{files: make([]*ComposeFileInfo,0), expectedSize: 0}
}

func (collection *TarFilesCollection) AddFile(file *ComposeFileInfo) {
	collection.files = append(collection.files, file)
	collection.expectedSize += file.expectedSize
}

func (c *TarBallComposer) AddHeader(fileInfoHeader *tar.Header) {
	c.headersToCompose = append(c.headersToCompose, fileInfoHeader)
}

func (c *TarBallComposer) AddFile(path string, fileInfo os.FileInfo, wasInBase bool,
	header *tar.Header, updatesCount uint64) {
	c.addFileQueue <- &AddFileTask{path: path, info: fileInfo, wasInBase: wasInBase, fileInfoHeader: header, updatesCount: updatesCount}
}

func (c *TarBallComposer) addFile(task *AddFileTask) error {
	expectedFileSize, err := c.getExpectedFileSize(task.path, task.info, task.wasInBase)
	if err != nil {
		return err
	}
	updateRating := c.composeRatingEvaluator.Evaluate(task.path, task.updatesCount, task.wasInBase)
	newFile := newComposeFileInfo(task.path, task.info, task.wasInBase, task.fileInfoHeader, task.updatesCount, updateRating, expectedFileSize)
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.files = append(c.files, newFile)
	return nil
}

func (c *TarBallComposer) sortFiles() {
	sort.Slice(c.files, func (i,j int) bool {
		return c.files[i].updateRating < c.files[j].updateRating
	})
}

func (c *TarBallComposer) Compose() ([]*tar.Header, []*TarFilesCollection) {
	c.sortFiles()
	tarFilesCollections := make([]*TarFilesCollection,0)
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

func (evaluator *DefaultComposeRatingEvaluator) Evaluate(path string, updatesCount uint64, wasInBase bool) uint64 {
	if !wasInBase {
		return updatesCount
	}
	prevUpdateCount := evaluator.incrementFromFiles[path].UpdatesCount
	if prevUpdateCount == 0 {
		return updatesCount
	}
	return (updatesCount * 100) / prevUpdateCount
}

func (c *TarBallComposer) getExpectedFileSize(filePath string, fileInfo os.FileInfo, wasInBase bool) (uint64, error) {
	incrementBaseLsn := c.incrementBaseLsn
	isIncremented := checkIfIncremented(incrementBaseLsn, fileInfo, filePath, wasInBase, c.forceIncremental)
	if !isIncremented {
		return uint64(fileInfo.Size()), nil
	}
	if !c.deltaMapComplete {
		err := c.scanDeltaMapFor(filePath, fileInfo.Size())
		if err != nil {
			return 0, err
		}
	}
	bitmap, err := c.deltaMap.GetDeltaBitmapFor(filePath)
	if _, ok := err.(NoBitmapFoundError); ok {
		// this file has changed after the start of backup and will be skipped
		// so the expected size in tar is zero
		return 0, nil
	}
	if err != nil {
		return 0, errors.Wrapf(err, "getExpectedFileSize: failed to find corresponding bitmap '%s'\n", filePath)
	}
	incrementBlocksCount := bitmap.GetCardinality()
	// expected header size = length(IncrementFileHeader) + sizeOf(fileSize) + sizeOf(diffBlockCount) + sizeOf(blockNo)*incrementBlocksCount
	incrementHeaderSize := uint64(len(IncrementFileHeader)) + sizeofInt64 + sizeofInt32 + (incrementBlocksCount * sizeofInt32)
	incrementPageDataSize := incrementBlocksCount * uint64(DatabasePageSize)
	fmt.Printf("Increment: %s, expected size: %d, blocks: %d \n", filePath, incrementHeaderSize+incrementPageDataSize, incrementBlocksCount)
	return incrementHeaderSize + incrementPageDataSize, nil
}

func (c *TarBallComposer) scanDeltaMapFor(filePath string, fileSize int64) error {
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