package mysql

import (
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/utility"
)

const BinlogPath = "binlog_" + utility.VersionStr + "/"

const TimeMysqlFormat = "2006-01-02 15:04:05"

func isMaster(db *sql.DB) bool {
	rows, err := db.Query("SHOW SLAVE STATUS")
	tracelog.ErrorLogger.FatalOnError(err)
	defer utility.LoggedClose(rows, "")
	return !rows.Next()
}

func getMySQLCurrentBinlogFileLocal(db *sql.DB) (fileName string) {
	rows, err := db.Query("SHOW MASTER STATUS")
	tracelog.ErrorLogger.FatalOnError(err)
	defer utility.LoggedClose(rows, "")
	var logFileName string
	for rows.Next() {
		err = utility.ScanToMap(rows, map[string]interface{}{"File": &logFileName})
		tracelog.ErrorLogger.FatalOnError(err)
		return logFileName
	}
	tracelog.ErrorLogger.Fatalf("Failed to obtain current binlog file")
	return ""
}

func getMySQLCurrentBinlogFileFromMaster(db *sql.DB) (fileName string) {
	rows, err := db.Query("SHOW SLAVE STATUS")
	tracelog.ErrorLogger.FatalOnError(err)
	defer utility.LoggedClose(rows, "")
	var logFileName string
	for rows.Next() {
		err = utility.ScanToMap(rows, map[string]interface{}{"Relay_Master_Log_File": &logFileName})
		tracelog.ErrorLogger.FatalOnError(err)
		return logFileName
	}
	tracelog.ErrorLogger.Fatalf("Failed to obtain master's current binlog file")
	return ""
}

func getMySQLCurrentBinlogFile(db *sql.DB) (fileName string) {
	takeFromMaster, err := internal.GetBoolSettingDefault(internal.MysqlTakeBinlogsFromMaster, false)
	tracelog.ErrorLogger.FatalOnError(err)
	if takeFromMaster && !isMaster(db) {
		return getMySQLCurrentBinlogFileFromMaster(db)
	}
	return getMySQLCurrentBinlogFileLocal(db)
}

func getMySQLConnection() (*sql.DB, error) {
	datasourceName, err := internal.GetRequiredSetting(internal.MysqlDatasourceNameSetting)
	if err != nil {
		return nil, err
	}
	db, err := getMySqlConnectionFromDatasource(datasourceName)
	if err != nil {
		fallbackDatasourceName := replaceHostInDatasourceName(datasourceName, "localhost")
		if fallbackDatasourceName != datasourceName {
			tracelog.ErrorLogger.Println(err.Error())
			tracelog.ErrorLogger.Println("Failed to connect using provided host, trying localhost")

			db, err = getMySqlConnectionFromDatasource(datasourceName)
		}
	}
	return db, err
}

func getMySqlConnectionFromDatasource(datasourceName string) (*sql.DB, error) {
	if caFile, ok := internal.GetSetting(internal.MysqlSslCaSetting); ok {
		rootCertPool := x509.NewCertPool()
		pem, err := ioutil.ReadFile(caFile)
		if err != nil {
			return nil, err
		}
		if ok := rootCertPool.AppendCertsFromPEM(pem); !ok {
			return nil, fmt.Errorf("Failed to load certificate from %s", caFile)
		}
		err = mysql.RegisterTLSConfig("custom", &tls.Config{
			RootCAs: rootCertPool,
		})
		if err != nil {
			return nil, err
		}
		if strings.Contains(datasourceName, "?tls=") || strings.Contains(datasourceName, "&tls=") {
			return nil, fmt.Errorf("MySQL datasource string contains tls option. It can't be used with %v option", internal.MysqlSslCaSetting)
		}
		if strings.Contains(datasourceName, "?") {
			datasourceName += "&tls=custom"
		} else {
			datasourceName += "?tls=custom"
		}
	}
	db, err := sql.Open("mysql", datasourceName)
	return db, err
}

func replaceHostInDatasourceName(datasourceName string, newHost string) string {
	var userData, dbNameAndParams string

	splitName := strings.SplitN(datasourceName, "@", 2)
	if len(splitName) == 2 {
		userData = splitName[0]
	} else {
		userData = ""
	}
	splitName = strings.SplitN(datasourceName, "/", 2)
	if len(splitName) == 2 {
		dbNameAndParams = splitName[1]
	} else {
		dbNameAndParams = ""
	}

	return userData + "@" + newHost + "/" + dbNameAndParams
}

type StreamSentinelDto struct {
	BinLogStart    string    `json:"BinLogStart,omitempty"`
	BinLogEnd      string    `json:"BinLogEnd,omitempty"`
	StartLocalTime time.Time `json:"StartLocalTime,omitempty"`
	StopLocalTime  time.Time `json:"StopLocalTime,omitempty"`
}

func (s *StreamSentinelDto) String() string {
	b, err := json.Marshal(s)
	if err != nil {
		return "-"
	}
	return string(b)
}

type binlogHandler interface {
	handleBinlog(binlogPath string) error
}

func fetchLogs(folder storage.Folder, dstDir string, startTs time.Time, endTs time.Time, handler binlogHandler) error {
	logFolder := folder.GetSubFolder(BinlogPath)
	logsToFetch, err := getLogsCoveringInterval(logFolder, startTs)
	if err != nil {
		return err
	}
	for _, logFile := range logsToFetch {
		binlogName := utility.TrimFileExtension(logFile.GetName())
		binlogPath := path.Join(dstDir, binlogName)
		tracelog.InfoLogger.Printf("downloading %s into %s", binlogName, binlogPath)
		if err = internal.DownloadFileTo(logFolder, binlogName, binlogPath); err != nil {
			tracelog.ErrorLogger.Printf("failed to download %s: %v", binlogName, err)
			return err
		}
		timestamp, err := GetBinlogStartTimestamp(binlogPath)
		if err != nil {
			return err
		}
		err = handler.handleBinlog(binlogPath)
		if err != nil {
			return err
		}
		if timestamp.After(endTs) {
			break
		}
	}
	return nil
}

func getBinlogSinceTs(folder storage.Folder, backup *internal.BackupMetaFetcher) (time.Time, error) {
	startTs := utility.MaxTime // far future
	var streamSentinel StreamSentinelDto
	err := backup.FetchSentinel(&streamSentinel)
	if err != nil {
		return time.Time{}, err
	}
	tracelog.InfoLogger.Printf("Backup sentinel: %s", streamSentinel)

	// case when backup was uploaded before first binlog
	sentinels, _, err := folder.GetSubFolder(utility.BaseBackupPath).ListFolder()
	if err != nil {
		return time.Time{}, err
	}
	for _, sentinel := range sentinels {
		if strings.HasPrefix(sentinel.GetName(), backup.BackupName) {
			tracelog.InfoLogger.Printf("Backup sentinel file: %s (%s)", sentinel.GetName(), sentinel.GetLastModified())
			if sentinel.GetLastModified().Before(startTs) {
				startTs = sentinel.GetLastModified()
			}
		}
	}
	// case when binlog was uploaded before backup
	binlogs, _, err := folder.GetSubFolder(BinlogPath).ListFolder()
	if err != nil {
		return time.Time{}, err
	}
	for _, binlog := range binlogs {
		if strings.HasPrefix(binlog.GetName(), streamSentinel.BinLogStart) {
			tracelog.InfoLogger.Printf("Backup start binlog: %s (%s)", binlog.GetName(), binlog.GetLastModified())
			if binlog.GetLastModified().Before(startTs) {
				startTs = binlog.GetLastModified()
			}
		}
	}
	return startTs, nil
}

// getLogsCoveringInterval lists the operation logs that cover the interval
func getLogsCoveringInterval(folder storage.Folder, start time.Time) ([]storage.Object, error) {
	logFiles, _, err := folder.ListFolder()
	if err != nil {
		return nil, err
	}
	sort.Slice(logFiles, func(i, j int) bool {
		return logFiles[i].GetLastModified().Before(logFiles[j].GetLastModified())
	})
	var logsToFetch []storage.Object
	for _, logFile := range logFiles {
		if start.Before(logFile.GetLastModified()) || start.Equal(logFile.GetLastModified()) {
			logsToFetch = append(logsToFetch, logFile)
		}
	}
	return logsToFetch, nil
}
