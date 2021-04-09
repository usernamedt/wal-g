package internal

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/pkg/errors"
	"github.com/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/utility"
)

const LatestString = "LATEST"

// Select the name of storage backup chosen according to the internal rules
type BackupSelector interface {
	Select(folder storage.Folder) (string, error)
}

// Select the latest backup from storage
type LatestBackupSelector struct {
}

func NewLatestBackupSelector() LatestBackupSelector {
	return LatestBackupSelector{}
}

func (s LatestBackupSelector) Select(folder storage.Folder) (string, error) {
	return getLatestBackupName(folder)
}

// Select backup which has the provided user data
type UserDataBackupSelector struct {
	userData interface{}
	backupProvider GenericBackupProvider
}

func NewUserDataBackupSelector(userDataRaw string, backupProvider GenericBackupProvider) UserDataBackupSelector {
	return UserDataBackupSelector{
		userData: UnmarshalSentinelUserData(userDataRaw),
		backupProvider: backupProvider,
	}
}

func (s UserDataBackupSelector) Select(folder storage.Folder) (string, error) {
	backup, err := s.findBackupByUserData(s.userData, folder)
	if err != nil {
		return "", err
	}
	return backup.Name(), nil
}

// Find backup with UserData exactly matching the provided one
func (s UserDataBackupSelector) findBackupByUserData(userData interface{}, folder storage.Folder) (GenericBackup, error) {
	foundBackups, err := searchInMetadata(
		func(d GenericMetadata) bool {
			return reflect.DeepEqual(userData, d.UserData)
		}, folder, s.backupProvider)
	if err != nil {
		return nil, errors.Wrapf(err, "UserData search failed")
	}

	if len(foundBackups) == 0 {
		return nil, errors.New("no backups found with specified user data")
	}

	if len(foundBackups) > 1 {
		var backupNames []string
		for idx := range foundBackups {
			backupNames = append(backupNames, foundBackups[idx].Name())
		}
		return nil, fmt.Errorf("too many backups (%d) found with specified user data: %s\n",
			len(backupNames), strings.Join(backupNames, " "))
	}

	return foundBackups[0], nil
}

// Search backups in storage using specified criteria
func searchInMetadata(
	criteria func(GenericMetadata) bool,
	folder storage.Folder,
	backupProvider GenericBackupProvider,
) ([]GenericBackup, error) {
	backups, err := GetBackupSentinelObjects(folder)
	if err != nil {
		return nil, err
	}

	backupTimes := GetBackupTimeSlices(backups)
	foundBackups := make([]GenericBackup, 0)

	for _, backupTime := range backupTimes {
		backup, err := backupProvider.GetGenericBackup(backupTime.BackupName, folder.GetSubFolder(utility.BaseBackupPath))
		if err != nil {
			tracelog.WarningLogger.Printf("Failed to get backup %s, error: %s\n",
				backupTime.BackupName, err.Error())
			continue
		}

		meta, err := backup.GetMetadata()
		if err != nil {
			tracelog.WarningLogger.Printf("Failed to get metadata of backup %s, error: %s\n",
				backupTime.BackupName, err.Error())
		} else if criteria(meta) {
			foundBackups = append(foundBackups, backup)
		}
	}
	return foundBackups, nil
}

// Select backup by provided backup name
type BackupNameSelector struct {
	backupName string
}

func NewBackupNameSelector(backupName string) (BackupNameSelector, error) {
	return BackupNameSelector{backupName: backupName}, nil
}

func (s BackupNameSelector) Select(folder storage.Folder) (string, error) {
	_, err := GetBackupMetaFetcherByName(s.backupName, utility.BaseBackupPath, folder)
	if err != nil {
		return "", err
	}
	return s.backupName, nil
}

func NewTargetBackupSelector(targetUserData, targetName string, backupProvider GenericBackupProvider) (BackupSelector, error) {
	var err error
	switch {
	case targetName != "" && targetUserData != "":
		err = errors.New("Incorrect arguments. Specify target backup name OR target userdata, not both.")

	case targetName == LatestString:
		tracelog.InfoLogger.Printf("Selecting the latest backup...\n")
		return NewLatestBackupSelector(), nil

	case targetName != "":
		tracelog.InfoLogger.Printf("Selecting the backup with name %s...\n", targetName)
		return NewBackupNameSelector(targetName)

	case targetUserData != "":
		tracelog.InfoLogger.Println("Selecting the backup with the specified user data...")
		return NewUserDataBackupSelector(targetUserData, backupProvider), nil

	default:
		err = errors.New("Insufficient arguments.")
	}
	return nil, err
}
