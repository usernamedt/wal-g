package postgres

import (
	"fmt"
	"github.com/wal-g/storages/storage"
	"github.com/wal-g/wal-g/internal"
)

type GenericPostgresBackup struct {
	name string
	metadata internal.GenericMetadata
}

func (b GenericPostgresBackup) SetMetadata(meta internal.GenericMetadata) error {
	panic("implement me")
}

func (b GenericPostgresBackup) GetBackupName() string {
	return b.name
}

func (b GenericPostgresBackup) GetMetadata() (internal.GenericMetadata, error) {
	return b.metadata, nil
}

func NewGenericBackupProvider() GenericBackupProvider {
	return GenericBackupProvider{}
}

type GenericBackupProvider struct {}

func (gbp GenericBackupProvider) GetGenericBackup(backupName string, folder storage.Folder) (internal.GenericBackup, error) {
	var backup = NewBackup(folder, backupName)
	meta, err := backup.FetchMeta()
	if err != nil {
		return nil, err
	}

	abstractMeta := internal.GenericMetadata{
		UncompressedSize: meta.UncompressedSize,
		CompressedSize: meta.CompressedSize,
		SystemIdentifier: meta.SystemIdentifier,
		Hostname: meta.Hostname,
		StartTime: meta.StartTime,
		FinishTime: meta.FinishTime,
		IsPermanent: meta.IsPermanent,
		IsIncremental: false, //todo
		FetchIncrementDetails: makeFetchIncrementDetails(backup),
		UserData: meta.UserData,
	}
	return GenericPostgresBackup{backupName, abstractMeta}, nil
}

func makeFetchIncrementDetails(backup *Backup) func() (internal.IncrementDetails, error) {
	var sentinel *BackupSentinelDto

	return func() (internal.IncrementDetails, error) {
		if sentinel == nil {
			var err error
			_, err = backup.GetSentinel()
			if err != nil {
				return internal.IncrementDetails{}, err
			}
			sentinel = backup.SentinelDto
		}

		return loadIncrementDetails(sentinel)
	}
}

func loadIncrementDetails(sentinel *BackupSentinelDto) (internal.IncrementDetails, error) {
	if sentinel.IsIncremental() {
		return internal.IncrementDetails{
			IncrementFrom: *sentinel.IncrementFrom,
			IncrementFullName: *sentinel.IncrementFullName,
			IncrementCount: *sentinel.IncrementCount,
		}, nil
	}
	return internal.IncrementDetails{}, fmt.Errorf("failed to fetch increment details: backup is not incremental")
}