package internal

import (
	"archive/tar"
	"github.com/jackc/pgx"
	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/walparser"
	"os"
	"sort"
)

// StatisticsTarBallComposer receives all files and tar headers
// that are going to be written to the backup,
// calculates update rating and returns tar headers and files,
// ordered by the update rating
// It also should compose tarballs in the future,
// but atm tarballs composing logic is in the bundle.go
type StatisticsTarBallComposer struct {
	headersToCompose       []*tar.Header
	files                  []*ComposeFileInfo
	tarSizeThreshold       uint64
	composeRatingEvaluator ComposeRatingEvaluator
	relationsStats         map[walparser.RelFileNode]PgRelationStat
	deltaMap           *PagedFileDeltaMap
}

func NewStatisticsTarBallComposer(tarSizeThreshold uint64, updateRatingEvaluator ComposeRatingEvaluator) *StatisticsTarBallComposer {
	return &StatisticsTarBallComposer{headersToCompose: make([]*tar.Header,0), files: make([]*ComposeFileInfo,0),
		tarSizeThreshold: tarSizeThreshold, composeRatingEvaluator: updateRatingEvaluator}
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

func (c *StatisticsTarBallComposer) AddHeader(fileInfoHeader *tar.Header) {
	c.headersToCompose = append(c.headersToCompose, fileInfoHeader)
}

func (c *StatisticsTarBallComposer) AddFile(path string, fileInfo os.FileInfo, wasInBase bool,
	header *tar.Header, updatesCount uint64, expectedFileSize uint64) {
	updateRating := c.composeRatingEvaluator.Evaluate(path, updatesCount, wasInBase)
	newFile := newComposeFileInfo(path, fileInfo, wasInBase, header, updatesCount, updateRating, expectedFileSize)
	c.files = append(c.files, newFile)
}

func (c *StatisticsTarBallComposer) sortFiles() {
	sort.Slice(c.files, func (i,j int) bool {
		return c.files[i].updateRating < c.files[j].updateRating
	})
}

func (c *StatisticsTarBallComposer) Compose() ([]*tar.Header, []*TarFilesCollection) {
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


// CollectStatistics collects statistics for each relFileNode
func (c *StatisticsTarBallComposer) CollectStatistics(conn *pgx.Conn) error {
	databases, err := getDatabaseInfos(conn)
	if err != nil {
		return errors.Wrap(err, "CollectStatistics: Failed to get db names.")
	}

	result := make(map[walparser.RelFileNode]PgRelationStat)
	for _, db := range databases {
		databaseOption := func (c *pgx.ConnConfig) error {
			c.Database = db.name
			return nil
		}
		dbConn, err := Connect(databaseOption)
		if err != nil {
			tracelog.WarningLogger.Printf("Failed to collect statistics for database: %s\n'%v'\n", db.name, err)
			continue
		}

		queryRunner, err := newPgQueryRunner(dbConn)
		if err != nil {
			return errors.Wrap(err, "CollectStatistics: Failed to build query runner.")
		}
		pgStatRows, err := queryRunner.getStatistics(&db)
		if err != nil {
			return errors.Wrap(err, "CollectStatistics: Failed to collect statistics.")
		}
		for relFileNode, statRow:= range pgStatRows {
			result[relFileNode] = statRow
		}
	}
	c.relationsStats = result
	return nil
}

func getDatabaseInfos(conn *pgx.Conn) ([]PgDatabaseInfo, error) {
	queryRunner, err := newPgQueryRunner(conn)
	if err != nil {
		return nil, errors.Wrap(err, "getDatabaseInfos: Failed to build query runner.")
	}
	return queryRunner.getDatabaseInfos()
}

func (c *StatisticsTarBallComposer) getFileUpdateCount(filePath string) uint64 {
	relFileNode, err := GetRelFileNodeFrom(filePath)
	if err != nil {
		// TODO: try parse _vm, _fsm etc
		// and assign the update count from corresponding tables
		return 0
	}
	fileStat, ok := c.relationsStats[*relFileNode]
	if !ok {
		return 0
	}
	return fileStat.deletedTuplesCount + fileStat.updatedTuplesCount + fileStat.insertedTuplesCount
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
