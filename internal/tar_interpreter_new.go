package internal

import (
	"archive/tar"
	"github.com/wal-g/wal-g/utility"
	"io"
	"os"

	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
)

// TODO : unit tests
func (tarInterpreter *FileTarInterpreter) unwrapRegularFileNew(fileReader io.Reader, fileInfo *tar.Header, targetPath string) error {
	if tarInterpreter.FilesToUnwrap != nil {
		if _, ok := tarInterpreter.FilesToUnwrap[fileInfo.Name]; !ok {
			// don't have to unwrap it this time
			tracelog.DebugLogger.Printf("Don't have to unwrap '%s' this time\n", fileInfo.Name)
			return nil
		}
	}
	fileDescription, haveFileDescription := tarInterpreter.Sentinel.Files[fileInfo.Name]
	isIncremented := haveFileDescription && fileDescription.IsIncremented
	localFileInfo, _ := getLocalFileInfo(targetPath)
	isPageFile := isPagedFile(localFileInfo, targetPath)

	fileOptions := &BackupFileOptions{isIncremented: isIncremented, isPageFile: isPageFile}
	fileUnwrapper := getFileUnwrapper(tarInterpreter, fileOptions)
	if localFileInfo, _ := getLocalFileInfo(targetPath); localFileInfo != nil {
		return handleExistFile(fileReader, fileInfo, targetPath, fileUnwrapper)
	}
	return handleNewFile(fileReader, fileInfo, targetPath, fileUnwrapper)
}

func getFileUnwrapper(tarInterpreter *FileTarInterpreter, options *BackupFileOptions) IBackupFileUnwrapper {
	// todo: clearer catchup backup detection logic
	isCatchup := tarInterpreter.createNewIncrementalFiles
	if isCatchup {
		return NewFileUnwrapper(CatchupBackupFileUnwrapper, options)
	}
	return NewFileUnwrapper(DefaultBackupFileUnwrapper, options)
}

// unwrap the file from tar to existing local file
func handleExistFile(fileReader io.Reader, fileInfo *tar.Header, targetPath string, unwrapper IBackupFileUnwrapper) error {
	localFile, err := os.OpenFile(targetPath, os.O_RDWR, 0666)
	if err != nil {
		return err
	}
	defer utility.LoggedClose(localFile, "")
	return unwrapper.UnwrapExistingFile(fileReader, fileInfo, localFile)
}

// unwrap file from tar to new local file
func handleNewFile(fileReader io.Reader, fileInfo *tar.Header, targetPath string, unwrapper IBackupFileUnwrapper) error {
	localFile, err := createLocalFile(targetPath, fileInfo.Name)
	if err != nil {
		return err
	}
	defer utility.LoggedClose(localFile, "")
	return unwrapper.UnwrapNewFile(fileReader, fileInfo, localFile)
}

func getLocalFileInfo(filename string) (fileInfo os.FileInfo, err error) {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return nil, err
	}
	if info.IsDir() {
		return nil, errors.New("Requested file is directory. Aborting.")
	}
	return info, nil
}

func createLocalFile(targetPath, name string) (*os.File, error) {
	err := PrepareDirs(name, targetPath)
	if err != nil {
		return nil, errors.Wrap(err, "Interpret: failed to create all directories")
	}
	file, err := os.OpenFile(targetPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create new file: '%s'", targetPath)
	}
	return file, nil
}
