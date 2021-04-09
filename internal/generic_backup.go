package internal

import (
	"github.com/wal-g/storages/storage"
	"time"
)

type GenericBackup interface {
	Name() string
	GetMetadata() (GenericMetadata, error)
	SetUserData(userData interface{}) error
	SetIsPermanent(isPermanent bool) error
}

type GenericMetadata struct {
	UncompressedSize int64
	CompressedSize   int64
	SystemIdentifier *uint64
	Hostname         string
	StartTime        time.Time
	FinishTime       time.Time

	IsPermanent   bool
	IsIncremental bool

	// need to use separate func
	// because to avoid useless sentinel load (in Postgres)
	FetchIncrementDetails func() (IncrementDetails, error)

	UserData interface{}
}

type IncrementDetails struct {
	IncrementFrom     string
	IncrementFullName string
	IncrementCount    int
}

type GenericBackupProvider interface {
	GetGenericBackup(backupName string, baseBackupFolder storage.Folder) (GenericBackup, error)
}
