package mysql

import (
	"github.com/wal-g/storages/storage"
	"github.com/wal-g/wal-g/internal"
)

type GenericMetaInteractor struct {
	GenericMetaFetcher
	GenericMetaSetter
}

func NewGenericMetaInteractor() GenericMetaInteractor {
	return GenericMetaInteractor{
		GenericMetaFetcher: NewGenericMetaFetcher(),
		GenericMetaSetter:  NewGenericMetaSetter(),
	}
}

type GenericMetaFetcher struct {}

func NewGenericMetaFetcher() GenericMetaFetcher {
	return GenericMetaFetcher{}
}

func (mf GenericMetaFetcher) Fetch(backupName string, backupFolder storage.Folder) (internal.GenericMetadata, error) {
	var backup = internal.NewBackup(backupFolder, backupName)
	var sentinel StreamSentinelDto
	err := backup.FetchSentinel(&sentinel)
	if err != nil {
		return internal.GenericMetadata{}, err
	}

	return internal.GenericMetadata{
		BackupName:            backupName,
		UncompressedSize:      sentinel.UncompressedSize,
		CompressedSize:        sentinel.CompressedSize,
		Hostname:              sentinel.Hostname,
		StartTime:             sentinel.StartLocalTime,
		FinishTime:            sentinel.StopLocalTime,
		IsPermanent:           sentinel.IsPermanent,
		FetchIncrementDetails: func() (bool, internal.IncrementDetails, error) {
			return false, internal.IncrementDetails{}, nil
		},
		UserData:              sentinel.UserData,
	}, nil
}

type GenericMetaSetter struct {}

func NewGenericMetaSetter() GenericMetaSetter {
	return GenericMetaSetter{}
}

func (ms GenericMetaSetter) SetUserData(backupName string, backupFolder storage.Folder, userData interface{}) error {
	panic("NOT IMPLEMENTED :(")
}

func (mf GenericMetaFetcher) SetIsPermanent(backupName string, backupFolder storage.Folder, isPermanent bool) error {
	panic("NOT IMPLEMENTED :(")
}