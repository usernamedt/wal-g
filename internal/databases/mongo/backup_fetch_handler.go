package mongo

import (
	"context"
	"github.com/wal-g/wal-g/internal/databases/postgres"
	"os/exec"

	"github.com/wal-g/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

func HandleBackupFetch(ctx context.Context, folder storage.Folder, backupName string, restoreCmd *exec.Cmd) error {
	backup, err := postgres.GetBackupByName(backupName, utility.BaseBackupPath, folder)
	if err != nil {
		return err
	}
	return postgres.StreamBackupToCommandStdin(restoreCmd, backup)
}
