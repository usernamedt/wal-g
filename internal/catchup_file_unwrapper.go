package internal

import (
	"archive/tar"
	"github.com/pkg/errors"
	"io"
	"os"
)

// CatchupFileUnwrapper is used for catchup backups
type CatchupFileUnwrapper struct {
	BackupFileUnwrapper
}

func (u *CatchupFileUnwrapper) UnwrapNewFile(reader io.Reader, fileInfo *tar.Header, file *os.File) error {
	if u.options.isIncremented {
		err := CreateFileFromIncrement(reader, file)
		return errors.Wrapf(err, "Interpret: failed to create file from increment '%s'", file.Name())
	}

	return u.writeLocalFile(reader, fileInfo, file)
}

func (u *CatchupFileUnwrapper) UnwrapExistingFile(reader io.Reader, fileInfo *tar.Header, file *os.File) error {
	if u.options.isIncremented {
		err := WritePagesFromIncrement(reader, file, true)
		return errors.Wrapf(err, "Interpret: failed to write increment to file '%s'", file.Name())
	}

	// clear the local file because there is a newer version for it
	err := u.clearLocalFile(file)
	if err != nil {
		return err
	}

	return u.writeLocalFile(reader, fileInfo, file)
}
