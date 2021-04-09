package internal

import (
	"github.com/wal-g/wal-g/utility"
)

func HandleBackupMark(uploader *Uploader, backupName string, toPermanent bool, backupProvider GenericBackupProvider) {
	folder := uploader.UploadingFolder
	baseBackupFolder := uploader.UploadingFolder.GetSubFolder(utility.BaseBackupPath)
	uploader.UploadingFolder = baseBackupFolder

	markHandler := NewBackupMarkHandler(backupProvider, folder)
	markHandler.MarkBackup(backupName, toPermanent)
}
