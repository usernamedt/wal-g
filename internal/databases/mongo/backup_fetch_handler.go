package mongo

import (
	"context"
	"github.com/wal-g/wal-g/internal"
	"os/exec"

	"github.com/wal-g/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

func HandleBackupFetch(ctx context.Context, folder storage.Folder, backupName string, restoreCmd *exec.Cmd) error {
	return internal.StreamBackupToCommandStdin(restoreCmd, folder.GetSubFolder(utility.BaseBackupPath), backupName)
}
