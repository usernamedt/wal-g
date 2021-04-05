package internal

import (
	"github.com/wal-g/wal-g/internal/databases/postgres"
	"time"
)

type BasicMetadata struct {
	UncompressedSize int64
	CompressedSize   int64
	SystemIdentifier *uint64
	Hostname         string
	StartTime        time.Time
	FinishTime       time.Time

	IsPermanent bool

	UserData interface{}
}

func NewBasicMetaFromPgMeta(pgMeta postgres.ExtendedMetadataDto) BasicMetadata {
	return BasicMetadata{
		UncompressedSize: pgMeta.UncompressedSize,
		CompressedSize: pgMeta.CompressedSize,
		SystemIdentifier: pgMeta.SystemIdentifier,
		Hostname: pgMeta.Hostname,
		StartTime: pgMeta.StartTime,
		FinishTime: pgMeta.FinishTime,
		IsPermanent: pgMeta.IsPermanent,
		UserData: pgMeta.UserData,
	}
}
