package internal

import (
	"bytes"
	"fmt"
	"os/exec"

	"github.com/pkg/errors"
	"github.com/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
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
		err = downloadAndDecompressStream(backup, stdin)
		cmdErr := cmd.Wait()
		if err != nil || cmdErr != nil {
			tracelog.ErrorLogger.Printf("Restore command output:\n%s", stderr.String())
		}
		if cmdErr != nil {
			err = cmdErr
		}
		tracelog.ErrorLogger.FatalfOnError("Failed to fetch backup: %v\n", err)
	}
}

// StreamBackupToCommandStdin downloads and decompresses backup stream to cmd stdin.
func StreamBackupToCommandStdin(cmd *exec.Cmd, backup Backup) error {
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return fmt.Errorf("failed to fetch backup: %v", err)
	}
	tracelog.DebugLogger.Printf("Running command: %s", cmd.Args)
	err = cmd.Start()
	if err != nil {
		return fmt.Errorf("failed to start command: %v", err)
	}
	err = downloadAndDecompressStream(backup, stdin)
	if err != nil {
		return errors.Wrap(err, "failed to download and decompress stream")
	}
	err = cmd.Wait()
	if err != nil {
		return err
	}
	if cmd.ProcessState != nil && !cmd.ProcessState.Success() {
		return fmt.Errorf("command exited with non-zero code: %d", cmd.ProcessState.ExitCode())
	}
	return nil
}

// TODO : unit tests
// HandleBackupFetch is invoked to perform wal-g backup-fetch
func HandleBackupFetch(folder storage.Folder,
	targetBackupSelector BackupSelector,
	backupPath string,
	fetcher func(folder storage.Folder, backup Backup)) {
	backupName, err := targetBackupSelector.Select(folder.GetSubFolder(backupPath))
	tracelog.ErrorLogger.FatalOnError(err)
	tracelog.DebugLogger.Printf("HandleBackupFetch(%s, folder,)\n", backupName)
	backup, err := GetBackupByName(backupName, backupPath, folder)
	tracelog.ErrorLogger.FatalfOnError("Failed to fetch backup: %v\n", err)

	fetcher(folder, backup)
}
