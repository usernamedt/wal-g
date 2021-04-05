package internal

import "github.com/wal-g/wal-g/internal/databases/postgres"

// BackupDetails is used to append ExtendedMetadataDto details to BackupTime struct
type BackupDetail struct {
	BackupTime
	postgres.ExtendedMetadataDto
}
