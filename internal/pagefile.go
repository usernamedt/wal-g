//
// This file provides low level routines for handling incremental backup
// Incremental file format is:
// 4 bytes header with designation information, format version and magic number
// 8 bytes uint file size
// 4 bytes uint changed pages count N
// (N * 4) bytes for Block Numbers of changed pages
// (N * DatabasePageSize) bytes for changed page data
//

package internal

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path"
	"regexp"
	"strings"

	"github.com/wal-g/wal-g/utility"

	"github.com/RoaringBitmap/roaring"
	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/ioextensions"
	"github.com/wal-g/wal-g/internal/walparser"
	"github.com/wal-g/wal-g/internal/walparser/parsingutil"
)

const (
	DatabasePageSize            = walparser.BlockSize
	sizeofInt32                 = 4
	sizeofInt16                 = 2
	sizeofInt64                 = 8
	SignatureMagicNumber byte   = 0x55
	invalidLsn           uint64 = 0
	validFlags                  = 7
	layoutVersion               = 4
	headerSize                  = 24

	DefaultTablespace    = "base"
	GlobalTablespace     = "global"
	NonDefaultTablespace = "pg_tblspc"
)

// InvalidBlockError indicates that file contain invalid page and cannot be archived incrementally
type InvalidBlockError struct {
	error
}

func newInvalidBlockError(blockNo uint32) InvalidBlockError {
	return InvalidBlockError{errors.Errorf("block %d is invalid", blockNo)}
}

func (err InvalidBlockError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

type InvalidIncrementFileHeaderError struct {
	error
}

func newInvalidIncrementFileHeaderError() InvalidIncrementFileHeaderError {
	return InvalidIncrementFileHeaderError{errors.New("Invalid increment file header")}
}

func (err InvalidIncrementFileHeaderError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

type UnknownIncrementFileHeaderError struct {
	error
}

func newUnknownIncrementFileHeaderError() UnknownIncrementFileHeaderError {
	return UnknownIncrementFileHeaderError{errors.New("Unknown increment file header")}
}

func (err UnknownIncrementFileHeaderError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

type UnexpectedTarDataError struct {
	error
}

func newUnexpectedTarDataError() UnexpectedTarDataError {
	return UnexpectedTarDataError{errors.New("Expected end of Tar")}
}

func (err UnexpectedTarDataError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

var pagedFilenameRegexp *regexp.Regexp

func init() {
	pagedFilenameRegexp = regexp.MustCompile("^(\\d+)([.]\\d+)?$")
}

// TODO : unit tests
// isPagedFile checks basic expectations for paged file
func isPagedFile(info os.FileInfo, filePath string) bool {

	// For details on which file is paged see
	// https://www.postgresql.org/message-id/flat/F0627DEB-7D0D-429B-97A9-D321450365B4%40yandex-team.ru#F0627DEB-7D0D-429B-97A9-D321450365B4@yandex-team.ru
	if info.IsDir() ||
		((!strings.Contains(filePath, DefaultTablespace)) && (!strings.Contains(filePath, NonDefaultTablespace))) ||
		info.Size() == 0 ||
		info.Size()%int64(DatabasePageSize) != 0 ||
		!pagedFilenameRegexp.MatchString(path.Base(filePath)) {
		return false
	}
	return true
}

func ReadIncrementalFile(filePath string, fileSize int64, lsn uint64, deltaBitmap *roaring.Bitmap) (fileReader io.ReadCloser, size int64, err error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, 0, err
	}

	fileReadSeekCloser := &ioextensions.ReadSeekCloserImpl{
		Reader: NewDiskLimitReader(file),
		Seeker: file,
		Closer: file,
	}

	pageReader := &IncrementalPageReader{fileReadSeekCloser, fileSize, lsn, nil, nil}
	incrementSize, err := pageReader.initialize(deltaBitmap)
	if err != nil {
		return nil, 0, err
	}
	return pageReader, incrementSize, nil
}

// CreateFileFromIncrement creates empty local page file
// and fills it with the pages from the provided increment
func CreateFileFromIncrement(fileName string, targetPath string, increment io.Reader) error {
	tracelog.DebugLogger.Printf("Generating file from increment %s\n", targetPath)
	err := PrepareDirs(fileName, targetPath)
	if err != nil {
		return errors.Wrap(err, "Interpret: failed to create all directories")
	}

	file, err := os.OpenFile(targetPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		return errors.Wrap(err, "Interpret: failed to create file")
	}

	defer utility.LoggedClose(file, "")
	defer file.Sync()

	fileSize, diffBlockCount, diffMap, err := getIncrementFileData(increment)
	if err != nil {
		return err
	}

	// set represents all block numbers with non-empty pages
	deltaBlockNumbers := make(map[uint32]bool, diffBlockCount)
	for i := uint32(0); i < diffBlockCount; i++ {
		blockNo := binary.LittleEndian.Uint32(diffMap[i*sizeofInt32 : (i+1)*sizeofInt32])
		deltaBlockNumbers[blockNo] = true
	}
	pageCount := uint32(fileSize / uint64(DatabasePageSize))

	emptyPage := make([]byte, DatabasePageSize)
	page := make([]byte, DatabasePageSize)
	for i := uint32(0); i < pageCount; i++ {
		if deltaBlockNumbers[i] {
			_, err = io.ReadFull(increment, page)
			if err != nil {
				return err
			}
			_, err = file.WriteAt(page, int64(i)*int64(DatabasePageSize))
			if err != nil {
				return err
			}

		} else {
			_, err = file.WriteAt(emptyPage, int64(i)*int64(DatabasePageSize))
			if err != nil {
				return err
			}
		}
	}

	all, _ := increment.Read(make([]byte, 1))
	if all > 0 {
		return newUnexpectedTarDataError()
	}

	return nil
}

// FillMissingPagesFromBase fills missing pages of local file with their base version
func FillMissingPagesFromBase(fileName string, base io.Reader) error {
	tracelog.DebugLogger.Printf("Filling empty pages from base: %s\n", fileName)

	file, err := openFile(fileName, false)
	if err != nil {
		return err
	}

	defer utility.LoggedClose(file, "")
	defer file.Sync()

	filePageCount, err := getFilePageCount(file)
	if err != nil {
		return err
	}

	emptyPageHeader := make([]byte, headerSize)
	pageHeader := make([]byte, headerSize)
	page := make([]byte, DatabasePageSize)

	for i := int64(0); i < filePageCount; i++ {
		_, err := io.ReadFull(base, page)
		if err != nil {
			// if we reached end of base file, stop
			if err == io.EOF {
				break
			}
			return err
		}

		isMissingPage, err := checkIfMissingPage(file, i, pageHeader, emptyPageHeader)
		if err != nil {
			return err
		}
		// if it is non-empty (not missing page), then proceed to the next one
		if isMissingPage {
			_, err = file.WriteAt(page, i*int64(DatabasePageSize))
			if err != nil {
				return err
			}
		}
	}

	all, _ := base.Read(make([]byte, 1))
	if all > 0 {
		tracelog.DebugLogger.Printf("Skipping pages after end of the local file %s, possibly the pagefile was truncated.\n", fileName)
	}

	return nil
}

// WritePagesFromIncrement writes pages from delta according to diffMap
func WritePagesFromIncrement(fileName string, increment io.Reader, overwrite bool) error {
	tracelog.DebugLogger.Printf("Writing pages from increment: %s\n", fileName)
	file, err := openFile(fileName, overwrite)
	if err != nil {
		return err
	}

	defer utility.LoggedClose(file, "")
	defer file.Sync()

	filePageCount, err := getFilePageCount(file)
	if err != nil {
		return err
	}

	emptyPageHeader := make([]byte, headerSize)
	pageHeader := make([]byte, headerSize)
	page := make([]byte, DatabasePageSize)

	_, diffBlockCount, diffMap, err := getIncrementFileData(increment)
	if err != nil {
		return err
	}

	for i := uint32(0); i < diffBlockCount; i++ {
		blockNo := binary.LittleEndian.Uint32(diffMap[i*sizeofInt32 : (i+1)*sizeofInt32])
		if blockNo >= uint32(filePageCount) {
			continue
		}
		_, err = io.ReadFull(increment, page)
		if err != nil {
			return err
		}

		if !overwrite {
			isMissingPage, err := checkIfMissingPage(file, int64(blockNo), pageHeader, emptyPageHeader)
			if err != nil {
				return err
			}
			// if it is non-empty (not missing page), then proceed to the next one
			if !isMissingPage {
				continue
			}
		}

		_, err = file.WriteAt(page, int64(blockNo)*int64(DatabasePageSize))
		if err != nil {
			return err
		}
	}

	all, _ := increment.Read(make([]byte, 1))
	if all > 0 {
		return newUnexpectedTarDataError()
	}

	return nil
}

func checkIfMissingPage(file *os.File, blockNo int64, pageHeader, emptyPageHeader []byte) (bool, error) {
	_, err := file.ReadAt(pageHeader, blockNo*int64(DatabasePageSize))
	if err != nil {
		return false, err
	}

	return bytes.Equal(pageHeader, emptyPageHeader), nil
}

func getFilePageCount(file *os.File) (int64, error) {
	localFileInfo, err := file.Stat()
	if err != nil {
		return 0, errors.Wrap(err, "error getting fileInfo")
	}

	return localFileInfo.Size() / int64(DatabasePageSize), nil
}

func openFile(fileName string, createNew bool) (*os.File, error) {
	openFlags := os.O_RDWR

	if createNew {
		openFlags = openFlags | os.O_CREATE
	}

	file, err := os.OpenFile(fileName, openFlags, 0666)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, errors.Wrap(err, "incremented file should always exist")
		}
		return nil, errors.Wrap(err, "can't open file to base")
	}

	return file, nil
}

func getIncrementFileData(increment io.Reader) (uint64, uint32, []byte, error) {
	err := ReadIncrementFileHeader(increment)
	if err != nil {
		return 0, 0, nil, err
	}

	var fileSize uint64
	var diffBlockCount uint32
	err = parsingutil.ParseMultipleFieldsFromReader([]parsingutil.FieldToParse{
		{Field: &fileSize, Name: "fileSize"},
		{Field: &diffBlockCount, Name: "diffBlockCount"},
	}, increment)
	if err != nil {
		return 0, 0, nil, err
	}

	diffMap := make([]byte, diffBlockCount*sizeofInt32)

	_, err = io.ReadFull(increment, diffMap)
	if err != nil {
		return 0, 0, nil, err
	}
	return fileSize, diffBlockCount, diffMap, nil
}

func ReadIncrementFileHeader(reader io.Reader) error {
	header := make([]byte, sizeofInt32)
	_, err := io.ReadFull(reader, header)
	if err != nil {
		return err
	}

	if header[0] != 'w' || header[1] != 'i' || header[3] != SignatureMagicNumber {
		return newInvalidIncrementFileHeaderError()
	}
	if header[2] != '1' {
		return newUnknownIncrementFileHeaderError()
	}
	return nil
}
