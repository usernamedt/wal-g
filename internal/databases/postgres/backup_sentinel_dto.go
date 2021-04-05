package postgres

import (
	"github.com/wal-g/wal-g/internal"
	"sync"
	"time"
)

// BackupSentinelDto describes file structure of json sentinel
type BackupSentinelDto struct {
	IncrementDetails
	Files       internal.BackupFileList `json:"Files"`
	TarFileSets TarFileSets             `json:"TarFileSets"`

	PgVersion        int             `json:"PgVersion"`
	BackupStartLSN    *uint64        `json:"LSN"`
	BackupFinishLSN  *uint64         `json:"FinishLSN"`
	TablespaceSpec   *TablespaceSpec `json:"Spec"`

	SystemIdentifier *uint64 `json:"SystemIdentifier,omitempty"`
	UncompressedSize int64           `json:"UncompressedSize"`
	CompressedSize   int64           `json:"CompressedSize"`

	UserData interface{} `json:"UserData,omitempty"`
}

func NewBackupSentinelDto(
	backupStartLSN, backupFinishLSN uint64,
	bc *BackupConfig,
	pgVersion int,
	tablespaceSpec *TablespaceSpec,
	systemIdentifier *uint64,
	uncompressedSize, compressedSize int64,
	files *sync.Map,
	tarFileSets TarFileSets,
) *BackupSentinelDto {
	sentinel := &BackupSentinelDto{
		BackupStartLSN:   &backupStartLSN,
		PgVersion:        pgVersion,
		TablespaceSpec:   tablespaceSpec,
	}
	sentinel.IncrementDetails = NewIncrementDetails(
		bc.previousBackupSentinelDto, bc.previousBackupName, bc.incrementCount)

	sentinel.setFiles(files)
	sentinel.BackupFinishLSN = &backupFinishLSN
	sentinel.UserData = internal.UnmarshalSentinelUserData(bc.userData)
	sentinel.SystemIdentifier = systemIdentifier
	sentinel.UncompressedSize = uncompressedSize
	sentinel.CompressedSize = compressedSize
	sentinel.TarFileSets = tarFileSets
	return sentinel
}

// Extended metadata should describe backup in more details, but be small enough to be downloaded often
type ExtendedMetadataDto struct {
	StartTime        time.Time `json:"start_time"`
	FinishTime       time.Time `json:"finish_time"`
	DatetimeFormat   string    `json:"date_fmt"`
	Hostname         string    `json:"hostname"`

	DataDir          string    `json:"data_dir"`
	PgVersion        int       `json:"pg_version"`
	StartLsn         uint64    `json:"start_lsn"`
	FinishLsn        uint64    `json:"finish_lsn"`

	IsPermanent      bool      `json:"is_permanent"`
	SystemIdentifier *uint64   `json:"system_identifier"`

	UncompressedSize int64 `json:"uncompressed_size"`
	CompressedSize   int64 `json:"compressed_size"`

	UserData interface{} `json:"user_data,omitempty"`
}

func (dto *BackupSentinelDto) setFiles(p *sync.Map) {
	dto.Files = make(internal.BackupFileList)
	p.Range(func(k, v interface{}) bool {
		key := k.(string)
		description := v.(internal.BackupFileDescription)
		dto.Files[key] = description
		return true
	})
}
