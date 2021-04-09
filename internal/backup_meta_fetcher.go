package internal

import (
	"encoding/json"
	"github.com/pkg/errors"
	"github.com/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/utility"
	"io/ioutil"
)

// BackupMetaFetcher provides basic functionality
// to fetch backup-related information from storage
type BackupMetaFetcher struct {
	BackupFolder storage.Folder
	BackupName   string
}

func NewBackupMetaFetcher(baseBackupFolder storage.Folder, name string) *BackupMetaFetcher {
	return &BackupMetaFetcher{baseBackupFolder, name}
}

// getStopSentinelPath returns sentinel path.
func (backup *BackupMetaFetcher) getStopSentinelPath() string {
	return SentinelNameFromBackup(backup.BackupName)
}

func (backup *BackupMetaFetcher) getMetadataPath() string {
	return backup.BackupName + "/" + utility.MetadataFileName
}

// SentinelExists checks that the sentinel file of the specified backup exists.
func (backup *BackupMetaFetcher) SentinelExists() (bool, error) {
	return backup.BackupFolder.Exists(backup.getStopSentinelPath())
}

// TODO : unit tests
func (backup *BackupMetaFetcher) FetchSentinel(sentinelDto interface{}) error {
	sentinelDtoData, err := backup.fetchSentinelBytes()
	if err != nil {
		return errors.Wrap(err, "failed to fetch sentinel")
	}
	err = json.Unmarshal(sentinelDtoData, sentinelDto)
	return errors.Wrap(err, "failed to unmarshal sentinel")
}

// TODO : unit tests
func (backup *BackupMetaFetcher) fetchSentinelBytes() ([]byte, error) {
	backupReaderMaker := NewStorageReaderMaker(backup.BackupFolder, backup.getStopSentinelPath())
	backupReader, err := backupReaderMaker.Reader()
	if err != nil {
		return make([]byte, 0), err
	}
	sentinelDtoData, err := ioutil.ReadAll(backupReader)
	if err != nil {
		return sentinelDtoData, errors.Wrap(err, "failed to fetch sentinel")
	}
	return sentinelDtoData, nil
}

// TODO : unit tests
func (backup *BackupMetaFetcher) FetchMetadata(metadataDto interface{}) error {
	sentinelDtoData, err := backup.fetchSentinelBytes()
	if err != nil {
		return errors.Wrap(err, "failed to fetch sentinel")
	}
	err = json.Unmarshal(sentinelDtoData, metadataDto)
	return errors.Wrap(err, "failed to unmarshal sentinel")
}

// TODO : unit tests
func (backup *BackupMetaFetcher) fetchMetadataBytes() ([]byte, error) {
	backupReaderMaker := NewStorageReaderMaker(backup.BackupFolder, backup.getMetadataPath())
	backupReader, err := backupReaderMaker.Reader()
	if err != nil {
		return make([]byte, 0), err
	}
	metadata, err := ioutil.ReadAll(backupReader)
	if err != nil {
		return nil, errors.Wrap(err, "failed to fetch sentinel")
	}
	return metadata, nil
}

func GetBackupMetaFetcherByName(backupName, subfolder string, folder storage.Folder) (*BackupMetaFetcher, error) {
	baseBackupFolder := folder.GetSubFolder(subfolder)

	var backup *BackupMetaFetcher
	if backupName == LatestString {
		latest, err := getLatestBackupName(folder)
		if err != nil {
			return nil, err
		}
		tracelog.InfoLogger.Printf("LATEST backup is: '%s'\n", latest)

		backup = NewBackupMetaFetcher(baseBackupFolder, latest)
	} else {
		backup = NewBackupMetaFetcher(baseBackupFolder, backupName)

		exists, err := backup.SentinelExists()
		if err != nil {
			return nil, err
		}
		if !exists {
			return nil, NewBackupNonExistenceError(backupName)
		}
	}
	return backup, nil
}
