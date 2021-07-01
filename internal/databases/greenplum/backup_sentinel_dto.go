package greenplum

import "encoding/json"

// BackupSentinelDto describes file structure of json sentinel
type BackupSentinelDto struct {
	RestorePoint *string   `json:"RestorePoint,omitempty"`
	BackupNames  *[]string `json:"BackupNames,omitempty"`
}

func (s *BackupSentinelDto) String() string {
	b, err := json.Marshal(s)
	if err != nil {
		return "-"
	}
	return string(b)
}

// NewBackupSentinelDto returns new BackupSentinelDto instance
func NewBackupSentinelDto(curBackupInfo CurBackupInfo) BackupSentinelDto {
	sentinel := BackupSentinelDto{
		RestorePoint: &curBackupInfo.backupName,
		BackupNames:  &curBackupInfo.pgBackupNames,
	}
	return sentinel
}
