package mysql

import (
	"github.com/wal-g/wal-g/internal/databases/postgres"
	"os/exec"

	"github.com/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
)

func HandleBackupFetch(folder storage.Folder, targetBackupSelector internal.BackupSelector, restoreCmd *exec.Cmd, prepareCmd *exec.Cmd) {
	postgres.HandleBackupFetch(folder, targetBackupSelector, postgres.GetCommandStreamFetcher(restoreCmd))
	if prepareCmd != nil {
		err := prepareCmd.Run()
		tracelog.ErrorLogger.FatalfOnError("failed to prepare fetched backup: %v", err)
	}
}
