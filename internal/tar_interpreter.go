package internal

import (
	"archive/tar"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/wal-g/wal-g/utility"

	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
)

// TarInterpreter behaves differently
// for different file types.
type TarInterpreter interface {
	Interpret(reader io.Reader, header *tar.Header) error
}

// FileTarInterpreter extracts input to disk.
type FileTarInterpreter struct {
	DBDataDirectory string
	Sentinel        BackupSentinelDto
	FilesToUnwrap   map[string]bool

	createNewIncrementalFiles bool
}

func NewFileTarInterpreter(
	dbDataDirectory string, sentinel BackupSentinelDto, filesToUnwrap map[string]bool, createNewIncrementalFiles bool,
) *FileTarInterpreter {
	return &FileTarInterpreter{dbDataDirectory, sentinel, filesToUnwrap, createNewIncrementalFiles}
}

// TODO : unit tests
func (tarInterpreter *FileTarInterpreter) unwrapRegularFile(fileReader io.Reader, fileInfo *tar.Header, targetPath string) error {
	if tarInterpreter.FilesToUnwrap != nil {
		if _, ok := tarInterpreter.FilesToUnwrap[fileInfo.Name]; !ok {
			// don't have to unwrap it this time
			tracelog.DebugLogger.Printf("Don't have to unwrap '%s' this time\n", fileInfo.Name)
			return nil
		}
	}

	localFileExists := false
	isPageFile := false
	if localFileInfo, err := getLocalFileInfo(targetPath); err == nil {
		localFileExists = true
		if isPagedFile(localFileInfo, targetPath) {
			isPageFile = true
		}
	}
	fileDescription, haveFileDescription := tarInterpreter.Sentinel.Files[fileInfo.Name]
	isIncremented := haveFileDescription && fileDescription.IsIncremented
	// todo: clearer catchup backup handling logic
	// currently there is a hack to determine if we are dealing with catchup backup
	isCatchupBackup := tarInterpreter.createNewIncrementalFiles

	if localFileExists && isPageFile && isIncremented {
		err := WritePagesFromIncrement(targetPath, fileReader, isCatchupBackup)
		return errors.Wrapf(err, "Interpret: failed to write pages to file '%s'", targetPath)
	}

	if localFileExists && isPageFile && isCatchupBackup{
		return writeFileToDisk(fileReader, fileInfo, targetPath)
	}

	if localFileExists && isPageFile {
		err := FillMissingPagesFromBase(targetPath, fileReader)
		return errors.Wrapf(err, "Interpret: failed to write pages to file '%s'", targetPath)
	}

	if localFileExists && isCatchupBackup {
		// if we are unpacking catchup backup
		// then we need to write regular file from delta backup
		return writeFileToDisk(fileReader, fileInfo, targetPath)
	}

	if localFileExists {
		// if it is a regular file, just skip it
		// because we have newer version already on the disk
		return nil
	}

	if isIncremented {
		err := CreateFileFromIncrement(fileInfo.Name, targetPath, fileReader)
		return errors.Wrapf(err, "Interpret: failed to create file from increment for '%s'", targetPath)
	}

	// write entire file to disk
	return writeFileToDisk(fileReader, fileInfo, targetPath)
}

// Interpret extracts a tar file to disk and creates needed directories.
// Returns the first error encountered. Calls fsync after each file
// is written successfully.
func (tarInterpreter *FileTarInterpreter) Interpret(fileReader io.Reader, fileInfo *tar.Header) error {
	tracelog.DebugLogger.Println("Interpreting: ", fileInfo.Name)
	targetPath := path.Join(tarInterpreter.DBDataDirectory, fileInfo.Name)
	switch fileInfo.Typeflag {
	case tar.TypeReg, tar.TypeRegA:
		return tarInterpreter.unwrapRegularFile(fileReader, fileInfo, targetPath)
	case tar.TypeDir:
		err := os.MkdirAll(targetPath, 0755)
		if err != nil {
			return errors.Wrapf(err, "Interpret: failed to create all directories in %s", targetPath)
		}
		if err = os.Chmod(targetPath, os.FileMode(fileInfo.Mode)); err != nil {
			return errors.Wrap(err, "Interpret: chmod failed")
		}
	case tar.TypeLink:
		if err := os.Link(fileInfo.Name, targetPath); err != nil {
			return errors.Wrapf(err, "Interpret: failed to create hardlink %s", targetPath)
		}
	case tar.TypeSymlink:
		if err := os.Symlink(fileInfo.Name, targetPath); err != nil {
			return errors.Wrapf(err, "Interpret: failed to create symlink %s", targetPath)
		}
	}
	return nil
}

// get fileinfo of local file on the disk, or error if failed
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

func writeFileToDisk(fileReader io.Reader, fileInfo *tar.Header, targetPath string) error {
	err := PrepareDirs(fileInfo.Name, targetPath)
	if err != nil {
		return errors.Wrap(err, "Interpret: failed to create all directories")
	}
	file, err := os.OpenFile(targetPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		return errors.Wrapf(err, "failed to create new file: '%s'", targetPath)
	}

	_, err = io.Copy(file, fileReader)
	if err != nil {
		err1 := file.Close()
		if err1 != nil {
			tracelog.ErrorLogger.Printf("Interpret: failed to close file '%s' because of error: %v", targetPath, err1)
		}
		err1 = os.Remove(targetPath)
		if err1 != nil {
			tracelog.ErrorLogger.Fatalf("Interpret: failed to remove file '%s' because of error: %v", targetPath, err1)
		}
		return errors.Wrap(err, "Interpret: copy failed")
	}
	defer utility.LoggedClose(file, "")

	mode := os.FileMode(fileInfo.Mode)
	if err = os.Chmod(file.Name(), mode); err != nil {
		return errors.Wrap(err, "Interpret: chmod failed")
	}

	err = file.Sync()
	return errors.Wrap(err, "Interpret: fsync failed")
}

// PrepareDirs makes sure all dirs exist
func PrepareDirs(fileName string, targetPath string) error {
	if fileName == targetPath {
		return nil // because it runs in the local directory
	}
	base := filepath.Base(fileName)
	dir := strings.TrimSuffix(targetPath, base)
	err := os.MkdirAll(dir, 0755)
	return err
}
