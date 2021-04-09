package internal

import (
	"github.com/pkg/errors"
	"github.com/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/utility"
)

type BackupMarkHandler struct {
	backupProvider GenericBackupProvider
	storageRootFolder storage.Folder
	baseBackupFolder storage.Folder
}

func NewBackupMarkHandler(backupProvider GenericBackupProvider, storageRootFolder storage.Folder) BackupMarkHandler {
	return BackupMarkHandler{
		backupProvider: backupProvider,
		storageRootFolder: storageRootFolder,
		baseBackupFolder: storageRootFolder.GetSubFolder(utility.BaseBackupPath),
	}
}

// MarkBackup marks a backup as permanent or impermanent
func (h *BackupMarkHandler) MarkBackup(backupName string, toPermanent bool) {
	tracelog.InfoLogger.Printf("Retrieving previous related backups to be marked: toPermanent=%t", toPermanent)
	backupsToMark, err := h.GetBackupsToMark(backupName, toPermanent)

	tracelog.ErrorLogger.FatalfOnError("Failed to get previous backups: %v", err)
	tracelog.InfoLogger.Printf("Retrieved backups to be marked, marking: %v", backupsToMark)

	for _, backup := range backupsToMark {
		err = backup.SetIsPermanent(toPermanent)
		tracelog.ErrorLogger.FatalfOnError("Failed to mark backups: %v", err)
	}
}

// GetBackupsToMark retrieves all previous permanent or
// impermanent backups, including itself, any previous delta backups and
// initial full backup, in increasing order beginning from full backup,
// returning backups ready to be marked
//
// For example, when marking backups from impermanent to permanent, we retrieve
// all currently impermanent backups and return them as a slice
func (h *BackupMarkHandler) GetBackupsToMark(backupName string, toPermanent bool) ([]GenericBackup, error) {
	backup, err := h.backupProvider.GetGenericBackup(backupName, h.baseBackupFolder)
	if err != nil {
		return nil, err
	}

	meta, err := backup.GetMetadata()
	if err != nil {
		return nil, err
	}
	//raise error when backup already has that type
	if toPermanent == meta.IsPermanent {
		permanentType := "permanent"
		if !meta.IsPermanent {
			permanentType = "impermanent"
		}
		return nil, newBackupAlreadyThisTypePermanentError(backupName, permanentType)
	}

	if toPermanent {
		return h.getBackupsToMarkPermanent(backupName)
	} else {
		return h.getBackupsToMarkImpermanent(backupName)
	}
}

func (h *BackupMarkHandler) getBackupsToMarkPermanent(backupName string) ([]GenericBackup, error) {
	var backupsToMark []GenericBackup
	backup, err := h.backupProvider.GetGenericBackup(backupName, h.baseBackupFolder)
	if err != nil {
		return nil, err
	}

	meta, err := backup.GetMetadata()
	if err != nil {
		return nil, err
	}

	// only return backups that we want to update
	if !meta.IsPermanent {
		backupsToMark = append(backupsToMark, backup)
	}

	// return when no longer incremental
	if !meta.IsIncremental {
		return backupsToMark, nil
	}

	incrementDetails, err := meta.FetchIncrementDetails()
	if err != nil {
		return nil, err
	}

	// mark previous backup
	previousImpermanentBackups, err := h.getBackupsToMarkPermanent(incrementDetails.IncrementFrom)
	if err != nil {
		return nil, err
	}

	previousImpermanentBackups = append(previousImpermanentBackups, backupsToMark...)
	return previousImpermanentBackups, nil
}

func (h *BackupMarkHandler) getBackupsToMarkImpermanent(backupName string) ([]GenericBackup, error) {
	backup, err := h.backupProvider.GetGenericBackup(backupName, h.baseBackupFolder)
	if err != nil {
		return nil, err
	}

	permanentBackups := GetPermanentBackups(h.storageRootFolder, h.backupProvider)
	//  del current backup from
	delete(permanentBackups, getBackupNumber(backupName))

	reverseLinks, err := h.getGraphFromBaseToIncrement()
	if err != nil {
		return nil, err
	}

	if backupHasPermanentInFuture(&reverseLinks, backupName, &permanentBackups) {
		return nil, newBackupHasPermanentBackupInFutureError(backupName)
	}

	return []GenericBackup{backup}, nil
}

func getBackupNumber(backupName string) string {
	return backupName[len(utility.BackupNamePrefix) : len(utility.BackupNamePrefix)+24]
}

//backup has permanent in future only when one of the next backups is permanent
func backupHasPermanentInFuture(reverseLinks *map[string][]string, backupName string, permanentBackups *map[string]bool) bool {
	//if there is no next backups
	if _, ok := (*reverseLinks)[backupName]; !ok {
		return false
	}

	//if one of the next backups is permanent
	for _, b := range (*reverseLinks)[backupName] {
		if _, ok := (*permanentBackups)[getBackupNumber(b)]; ok {
			return true
		}
	}

	return false
}

//return graph where nodes - backup names, edges - links from base backups to increment backups
func (h *BackupMarkHandler) getGraphFromBaseToIncrement() (map[string][]string, error) {
	backups, err := GetBackups(h.storageRootFolder)
	if err != nil {
		return nil, err
	}

	reverseLinks := make(map[string][]string)
	for _, b := range backups {
		incrementFrom, isIncrement, err := h.getMetadataFromBackup(b.BackupName)
		if err != nil {
			return nil, err
		}

		if isIncrement {
			reverseLinks[incrementFrom] = append(reverseLinks[incrementFrom], b.BackupName)
		}
	}

	return reverseLinks, nil
}

func (h *BackupMarkHandler) getMetadataFromBackup(backupName string) (incrementFrom string, isIncrement bool, err error) {
	backup, err := h.backupProvider.GetGenericBackup(backupName, h.baseBackupFolder)
	if err != nil {
		return "", false, err
	}
	meta, err := backup.GetMetadata()
	if err != nil {
		return "", false, err
	}

	if !meta.IsIncremental {
		return "", false, nil
	}

	incrementDetails, err := meta.FetchIncrementDetails()
	if err != nil {
		return "", false, err
	}

	return incrementDetails.IncrementFrom, true, nil
}

type BackupAlreadyThisTypePermanentError struct {
	error
}

//raise when user try make permanent/impermanent already permanent/impermanent backup,
func newBackupAlreadyThisTypePermanentError(backupName string, permanentType string) BackupAlreadyThisTypePermanentError {
	return BackupAlreadyThisTypePermanentError{errors.Errorf("Backup '%s' is already %s.", backupName, permanentType)}
}

type BackupHasPermanentBackupInFutureError struct {
	error
}

func newBackupHasPermanentBackupInFutureError(backupName string) BackupHasPermanentBackupInFutureError {
	return BackupHasPermanentBackupInFutureError{errors.Errorf("Can't mark backup '%s' as impermanent. There is permanent increment backup.", backupName)}
}

func GetPermanentBackups(folder storage.Folder, backupProvider GenericBackupProvider) map[string]bool {
	tracelog.InfoLogger.Println("retrieving permanent objects")
	backupTimes, err := GetBackups(folder)
	if err != nil {
		return map[string]bool{}
	}

	permanentBackups := map[string]bool{}
	for _, backupTime := range backupTimes {
		backup, err := backupProvider.GetGenericBackup(backupTime.BackupName, folder.GetSubFolder(utility.BaseBackupPath))
		if err != nil {
			tracelog.ErrorLogger.Printf("failed to get backup by name with error %s, ignoring...", err.Error())
			continue
		}
		meta, err := backup.GetMetadata()
		if err != nil {
			tracelog.ErrorLogger.Printf("failed to fetch backup meta for backup %s with error %s, ignoring...",
				backupTime.BackupName, err.Error())
			continue
		}
		if meta.IsPermanent {
			permanentBackups[backupTime.BackupName[len(utility.BackupNamePrefix):len(utility.BackupNamePrefix)+24]] = true
		}
	}
	return permanentBackups
}
