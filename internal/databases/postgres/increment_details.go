package postgres

type IncrementDetails struct {
	IncrementFromLSN  *uint64 `json:"DeltaFromLSN,omitempty"`
	IncrementFrom     *string `json:"DeltaFrom,omitempty"`
	IncrementFullName *string `json:"DeltaFullName,omitempty"`
	IncrementCount    *int    `json:"DeltaCount,omitempty"`
}

func NewIncrementDetails(previousBackupSentinelDto BackupSentinelDto, previousBackupName string, incrementCount int) IncrementDetails {
	details := IncrementDetails{}
	if previousBackupSentinelDto.BackupStartLSN != nil {
		details.IncrementFrom = &previousBackupName
		if previousBackupSentinelDto.IsIncremental() {
			details.IncrementFullName = previousBackupSentinelDto.IncrementFullName
		} else {
			details.IncrementFullName = &previousBackupName
		}
		details.IncrementCount = &incrementCount
	}
	return details
}

// TODO : unit tests
// TODO : get rid of panic here
// IsIncremental checks that sentinel represents delta backup
func (d IncrementDetails) IsIncremental() bool {
	// If we have increment base, we must have all the rest properties.
	if d.IncrementFrom != nil {
		if d.IncrementFromLSN == nil || d.IncrementFullName == nil || d.IncrementCount == nil {
			panic("Inconsistent BackupSentinelDto")
		}
	}
	return d.IncrementFrom != nil
}
