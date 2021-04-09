package internal

import (
	"bytes"
	"fmt"
	"github.com/pkg/errors"
	"github.com/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
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

func GetCommandStreamFetcher(cmd *exec.Cmd) func(folder storage.Folder, backupName string) {
	return func(folder storage.Folder, backupName string) {
		stdin, err := cmd.StdinPipe()
		tracelog.ErrorLogger.FatalfOnError("Failed to fetch backup: %v\n", err)
		stderr := &bytes.Buffer{}
		cmd.Stderr = stderr
		err = cmd.Start()
		tracelog.ErrorLogger.FatalfOnError("Failed to start restore command: %v\n", err)
		err = downloadAndDecompressStream(backupName, folder, stdin)
		cmdErr := cmd.Wait()
		if cmdErr != nil {
			tracelog.ErrorLogger.Printf("Restore command output:\n%s", stderr.String())
			err = cmdErr
		}
		tracelog.ErrorLogger.FatalfOnError("Failed to fetch backup: %v\n", err)
	}
}

// StreamBackupToCommandStdin downloads and decompresses backup stream to cmd stdin.
func StreamBackupToCommandStdin(cmd *exec.Cmd, baseBackupFolder storage.Folder, backupName string) error {
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return fmt.Errorf("Failed to fetch backup: %v\n", err)
	}
	err = cmd.Start()
	if err != nil {
		return fmt.Errorf("Failed to start command: %v\n", err)
	}
	err = downloadAndDecompressStream(backupName, baseBackupFolder, stdin)
	if err != nil {
		return fmt.Errorf("Failed to download and decompress stream: %v\n", err)
	}
	return cmd.Wait()
}

// TODO : unit tests
// HandleBackupFetch is invoked to perform wal-g backup-fetch
func HandleBackupFetch(folder storage.Folder, targetBackupSelector BackupSelector, fetcher func(folder storage.Folder, backupName string)) {
	backupName, err := targetBackupSelector.Select(folder)
	tracelog.ErrorLogger.FatalOnError(err)
	tracelog.DebugLogger.Printf("HandleBackupFetch(%s, folder,)\n", backupName)

	fetcher(folder, backupName)
}