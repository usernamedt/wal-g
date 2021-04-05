package internal

import (
	"bytes"
	"fmt"
	"github.com/pkg/errors"
	"github.com/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/utility"
	"os/exec"
)

type BackupNonExistenceError struct {
	error
}

func NewBackupNonExistenceError(backupName string) BackupNonExistenceError {
	return BackupNonExistenceError{errors.Errorf("Backup '%s' does not exist.", backupName)}
}

func (err BackupNonExistenceError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

func GetCommandStreamFetcher(cmd *exec.Cmd) func(folder storage.Folder, backup Backup) {
	return func(folder storage.Folder, backup Backup) {
		stdin, err := cmd.StdinPipe()
		tracelog.ErrorLogger.FatalfOnError("Failed to fetch backup: %v\n", err)
		stderr := &bytes.Buffer{}
		cmd.Stderr = stderr
		err = cmd.Start()
		tracelog.ErrorLogger.FatalfOnError("Failed to start restore command: %v\n", err)
		err = downloadAndDecompressStream(&backup, stdin)
		cmdErr := cmd.Wait()
		if cmdErr != nil {
			tracelog.ErrorLogger.Printf("Restore command output:\n%s", stderr.String())
			err = cmdErr
		}
		tracelog.ErrorLogger.FatalfOnError("Failed to fetch backup: %v\n", err)
	}
}

// StreamBackupToCommandStdin downloads and decompresses backup stream to cmd stdin.
func StreamBackupToCommandStdin(cmd *exec.Cmd, backup *Backup) error {
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return fmt.Errorf("Failed to fetch backup: %v\n", err)
	}
	err = cmd.Start()
	if err != nil {
		return fmt.Errorf("Failed to start command: %v\n", err)
	}
	err = downloadAndDecompressStream(backup, stdin)
	if err != nil {
		return fmt.Errorf("Failed to download and decompress stream: %v\n", err)
	}
	return cmd.Wait()
}

// TODO : unit tests
// HandleBackupFetch is invoked to perform wal-g backup-fetch
func HandleBackupFetch(folder storage.Folder, targetBackupSelector BackupSelector, fetcher func(folder storage.Folder, backup Backup)) {
	backupName, err := targetBackupSelector.Select(folder)
	tracelog.ErrorLogger.FatalOnError(err)
	tracelog.DebugLogger.Printf("HandleBackupFetch(%s, folder,)\n", backupName)
	backup, err := GetBackupByName(backupName, utility.BaseBackupPath, folder)
	tracelog.ErrorLogger.FatalfOnError("Failed to fetch backup: %v\n", err)

	fetcher(folder, *backup)
}

func GetBackupByName(backupName, subfolder string, folder storage.Folder) (*Backup, error) {
	baseBackupFolder := folder.GetSubFolder(subfolder)

	var backup *Backup
	if backupName == LatestString {
		latest, err := getLatestBackupName(folder)
		if err != nil {
			return nil, err
		}
		tracelog.InfoLogger.Printf("LATEST backup is: '%s'\n", latest)

		backup = NewBackup(baseBackupFolder, latest)
	} else {
		backup = NewBackup(baseBackupFolder, backupName)

		exists, err := backup.CheckExistence()
		if err != nil {
			return nil, err
		}
		if !exists {
			return nil, NewBackupNonExistenceError(backupName)
		}
	}
	return backup, nil
}
