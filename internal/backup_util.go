package internal

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/utility"
	"sort"
	"strings"
)

type NoBackupsFoundError struct {
	error
}

func NewNoBackupsFoundError() NoBackupsFoundError {
	return NoBackupsFoundError{errors.New("No backups found")}
}

func (err NoBackupsFoundError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

// TODO : unit tests
func getLatestBackupName(folder storage.Folder) (string, error) {
	sortTimes, err := GetBackups(folder)
	if err != nil {
		return "", err
	}

	return sortTimes[0].BackupName, nil
}

func GetBackupSentinelObjects(folder storage.Folder) ([]storage.Object, error) {
	objects, _, err := folder.GetSubFolder(utility.BaseBackupPath).ListFolder()
	if err != nil {
		return nil, err
	}
	sentinelObjects := make([]storage.Object, 0, len(objects))
	for _, object := range objects {
		if !strings.HasSuffix(object.GetName(), utility.SentinelSuffix) {
			continue
		}
		sentinelObjects = append(sentinelObjects, object)
	}

	return sentinelObjects, nil
}

// TODO : unit tests
// GetBackups receives backup descriptions and sorts them by time
func GetBackups(folder storage.Folder) (backups []BackupTime, err error) {
	return GetBackupsWithTarget(folder, utility.BaseBackupPath)
}

func GetBackupsWithTarget(folder storage.Folder, targetPath string) (backups []BackupTime, err error) {
	backups, _, err = GetBackupsAndGarbageWithTarget(folder, targetPath)
	if err != nil {
		return nil, err
	}

	count := len(backups)
	if count == 0 {
		return nil, NewNoBackupsFoundError()
	}
	return
}

func GetBackupsAndGarbage(folder storage.Folder) (backups []BackupTime, garbage []string, err error) {
	return GetBackupsAndGarbageWithTarget(folder, utility.BaseBackupPath)
}

// TODO : unit tests
func GetBackupsAndGarbageWithTarget(folder storage.Folder, targetPath string) (backups []BackupTime, garbage []string, err error) {
	backupObjects, subFolders, err := folder.GetSubFolder(targetPath).ListFolder()
	if err != nil {
		return nil, nil, err
	}

	sortTimes := GetBackupTimeSlices(backupObjects)
	garbage = getGarbageFromPrefix(subFolders, sortTimes)

	return sortTimes, garbage, nil
}

// TODO : unit tests
func GetBackupTimeSlices(backups []storage.Object) []BackupTime {
	sortTimes := make([]BackupTime, len(backups))
	for i, object := range backups {
		key := object.GetName()
		if !strings.HasSuffix(key, utility.SentinelSuffix) {
			continue
		}
		time := object.GetLastModified()
		sortTimes[i] = BackupTime{utility.StripRightmostBackupName(key), time,
			utility.StripWalFileName(key)}
	}
	sort.Slice(sortTimes, func(i, j int) bool {
		return sortTimes[i].Time.After(sortTimes[j].Time)
	})
	return sortTimes
}

// TODO : unit tests
func getGarbageFromPrefix(folders []storage.Folder, nonGarbage []BackupTime) []string {
	garbage := make([]string, 0)
	var keyFilter = make(map[string]string)
	for _, k := range nonGarbage {
		keyFilter[k.BackupName] = k.BackupName
	}
	for _, folder := range folders {
		backupName := utility.StripPrefixName(folder.GetPath())
		if _, ok := keyFilter[backupName]; ok {
			continue
		}
		garbage = append(garbage, backupName)
	}
	return garbage
}

func SentinelNameFromBackup(backupName string) string {
	return backupName + utility.SentinelSuffix
}
