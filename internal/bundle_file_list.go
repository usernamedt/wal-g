package internal

import (
	"archive/tar"
	"github.com/jackc/pgx"
	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/walparser"
	"os"
	"sync"
)

type BundleFileList interface {
	AddSkippedFile(tarHeader *tar.Header, fileInfo os.FileInfo)
	AddFile(tarHeader *tar.Header, fileInfo os.FileInfo, isIncremented bool)
	GetSentinelMap() *sync.Map
}

type RegularBundleFileList struct {
	sync.Map
}

func (list *RegularBundleFileList) AddSkippedFile(tarHeader *tar.Header, fileInfo os.FileInfo) {
	list.Store(tarHeader.Name,
		BackupFileDescription{IsSkipped: true, IsIncremented: false, MTime: fileInfo.ModTime()})
}

func (list *RegularBundleFileList) AddFile(tarHeader *tar.Header, fileInfo os.FileInfo, isIncremented bool) {
	list.Store(tarHeader.Name,
		BackupFileDescription{IsSkipped: false, IsIncremented: isIncremented, MTime: fileInfo.ModTime()})
}

func (list *RegularBundleFileList) GetSentinelMap() *sync.Map {
	return &list.Map
}

func newStatisticsBundleFileList(fileStat BundleFileStatistics) *StatisticsBundleFileList {
	return &StatisticsBundleFileList{fileStats: fileStat}
}

type StatisticsBundleFileList struct {
	sync.Map
	fileStats BundleFileStatistics
}

func (fl *StatisticsBundleFileList) AddSkippedFile(tarHeader *tar.Header, fileInfo os.FileInfo) {
	updatesCount := fl.fileStats.getFileUpdateCount(tarHeader.Name)
	fl.Store(tarHeader.Name,
		BackupFileDescription{IsSkipped: true, IsIncremented: false,
			MTime: fileInfo.ModTime(), UpdatesCount: updatesCount})
}

func (fl *StatisticsBundleFileList) AddFile(tarHeader *tar.Header, fileInfo os.FileInfo, isIncremented bool) {
	updatesCount := fl.fileStats.getFileUpdateCount(tarHeader.Name)
	fl.Store(tarHeader.Name,
		BackupFileDescription{IsSkipped: false, IsIncremented: isIncremented,
			MTime: fileInfo.ModTime(), UpdatesCount: updatesCount})
}

func (fl *StatisticsBundleFileList) GetSentinelMap() *sync.Map {
	return &fl.Map
}

type BundleFileStatistics map[walparser.RelFileNode]PgRelationStat

func NewBundleFileStatistics(conn *pgx.Conn) (BundleFileStatistics, error) {
	databases, err := getDatabaseInfos(conn)
	if err != nil {
		return nil, errors.Wrap(err, "CollectStatistics: Failed to get db names.")
	}

	result := make(map[walparser.RelFileNode]PgRelationStat)
	// CollectStatistics collects statistics for each relFileNode
	for _, db := range databases {
		databaseOption := func(c *pgx.ConnConfig) error {
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
			return nil, errors.Wrap(err, "CollectStatistics: Failed to build query runner.")
		}
		pgStatRows, err := queryRunner.getStatistics(&db)
		if err != nil {
			return nil, errors.Wrap(err, "CollectStatistics: Failed to collect statistics.")
		}
		for relFileNode, statRow := range pgStatRows {
			result[relFileNode] = statRow
		}
	}
	return result, nil
}

func (relStat *BundleFileStatistics) getFileUpdateCount(filePath string) uint64 {
	if relStat == nil {
		return 0
	}
	relFileNode, err := GetRelFileNodeFrom(filePath)
	if err != nil {
		// TODO: try parse _vm, _fsm etc
		// and assign the update count from corresponding tables
		return 0
	}
	fileStat, ok := (*relStat)[*relFileNode]
	if !ok {
		return 0
	}
	return fileStat.deletedTuplesCount + fileStat.updatedTuplesCount + fileStat.insertedTuplesCount
}

func getDatabaseInfos(conn *pgx.Conn) ([]PgDatabaseInfo, error) {
	queryRunner, err := newPgQueryRunner(conn)
	if err != nil {
		return nil, errors.Wrap(err, "getDatabaseInfos: Failed to build query runner.")
	}
	return queryRunner.getDatabaseInfos()
}
