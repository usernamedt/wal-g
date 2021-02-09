package mysql

import (
	"os"
	"path"
	"path/filepath"

	"github.com/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
)

type indexHandler struct {
	dstDir  string
	binlogs []string
}

func newIndexHandler(dstDir string) *indexHandler {
	ih := new(indexHandler)
	ih.dstDir = dstDir
	return ih
}

func (ih *indexHandler) handleBinlog(binlogPath string) error {
	ih.binlogs = append(ih.binlogs, path.Base(binlogPath))
	return nil
}

func (ih *indexHandler) createIndexFile() error {
	indexFile, err := os.Create(filepath.Join(ih.dstDir, "binlogs_order"))
	if err != nil {
		return err
	}
	defer indexFile.Close()
	for _, binlog := range ih.binlogs {
		_, err = indexFile.WriteString(binlog + "\n")
		if err != nil {
			return err
		}
	}
	return nil
}

func HandleBinlogFetch(folder storage.Folder, backupName string, untilTs string) {
	dstDir, err := internal.GetLogsDstSettings(internal.MysqlBinlogDstSetting)
	tracelog.ErrorLogger.FatalOnError(err)

	startTs, endTs, err := getTimestamps(folder, backupName, untilTs)
	tracelog.ErrorLogger.FatalOnError(err)

	handler := newIndexHandler(dstDir)

	tracelog.InfoLogger.Printf("Fetching binlogs since %s until %s", startTs, endTs)
	err = fetchLogs(folder, dstDir, startTs, endTs, handler)
	tracelog.ErrorLogger.FatalfOnError("Failed to fetch binlogs: %v", err)

	err = handler.createIndexFile()
	tracelog.ErrorLogger.FatalfOnError("Failed to create binlog index file: %v", err)
}
