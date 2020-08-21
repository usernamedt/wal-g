package internal

import (
	"archive/tar"
	"context"
	"fmt"
	"github.com/RoaringBitmap/roaring"
	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/ioextensions"
	"github.com/wal-g/wal-g/utility"
	"golang.org/x/sync/errgroup"
	"io"
	"io/ioutil"
	"os"
)

type SkippedFileError struct {
	error
}

func newSkippedFileError(path string) SkippedFileError {
	return SkippedFileError{errors.Errorf("File is skipped from the current backup: %s\n", path)}
}

func (err SkippedFileError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

type IgnoredFileError struct {
	error
}

func newIgnoredFileError(path string) IgnoredFileError {
	return IgnoredFileError{errors.Errorf("File is ignored: %s\n", path)}
}

func (err IgnoredFileError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

// TarBallFilePacker is used to pack bundle file into tarball.
type TarBallFilePacker struct {
	deltaMap            PagedFileDeltaMap
	incrementFromLsn    *uint64
	files               BundleFiles
	verifyPageChecksums bool
}

func newTarBallFilePacker(deltaMap PagedFileDeltaMap, incrementFromLsn *uint64, files BundleFiles) *TarBallFilePacker {
	return &TarBallFilePacker{
		deltaMap:            deltaMap,
		incrementFromLsn:    incrementFromLsn,
		files:               files,
		verifyPageChecksums: false,
	}
}

func (p *TarBallFilePacker) getDeltaBitmapFor(filePath string) (*roaring.Bitmap, error) {
	if p.deltaMap == nil {
		return nil, nil
	}
	return p.deltaMap.GetDeltaBitmapFor(filePath)
}

func (p *TarBallFilePacker) UpdateDeltaMap(deltaMap PagedFileDeltaMap) {
	p.deltaMap = deltaMap
}

// TODO : unit tests
func (p *TarBallFilePacker) PackFileIntoTar(cfi *ComposeFileInfo, tarBall TarBall) error {
	fileReadCloser, err := p.createFileReadCloser(cfi)
	if err != nil {
		switch err.(type) {
		case SkippedFileError:
			p.files.AddSkippedFile(cfi.header, cfi.fileInfo)
			return nil
		case IgnoredFileError:
			return nil
		default:
			return err
		}
	}
	errorGroup, _ := errgroup.WithContext(context.Background())

	if p.verifyPageChecksums {
		var secondReadCloser io.ReadCloser
		// newTeeReadCloser is used to provide the fileReadCloser to two consumers:
		// fileReadCloser is needed for PackFileTo, secondReadCloser is for the page verification
		fileReadCloser, secondReadCloser = newTeeReadCloser(fileReadCloser)
		errorGroup.Go(func() (err error) {
			corruptBlocks, err := verifyFile(cfi.path, cfi.fileInfo, secondReadCloser, cfi.isIncremented)
			if err != nil {
				return err
			}
			p.files.AddFileWithCorruptBlocks(cfi.header, cfi.fileInfo, cfi.isIncremented, corruptBlocks)
			return nil
		})
	} else {
		p.files.AddFile(cfi.header, cfi.fileInfo, cfi.isIncremented)
	}

	errorGroup.Go(func() error {
		defer utility.LoggedClose(fileReadCloser, "")
		packedFileSize, err := PackFileTo(tarBall, cfi.header, fileReadCloser)
		if err != nil {
			return errors.Wrap(err, "PackFileIntoTar: operation failed")
		}
		if packedFileSize != cfi.header.Size {
			return newTarSizeError(packedFileSize, cfi.header.Size)
		}
		return nil
	})

	return errorGroup.Wait()
}

func (p *TarBallFilePacker) createFileReadCloser(cfi *ComposeFileInfo) (io.ReadCloser, error) {
	var fileReadCloser io.ReadCloser
	if cfi.isIncremented {
		bitmap, err := p.getDeltaBitmapFor(cfi.path)
		if _, ok := err.(NoBitmapFoundError); ok { // this file has changed after the start of backup, so just skip it
			return nil, newSkippedFileError(cfi.path)
		} else if err != nil {
			return nil, errors.Wrapf(err, "PackFileIntoTar: failed to find corresponding bitmap '%s'\n", cfi.path)
		}
		fileReadCloser, cfi.header.Size, err = ReadIncrementalFile(cfi.path, cfi.fileInfo.Size(), *p.incrementFromLsn, bitmap)
		if os.IsNotExist(err) { // File was deleted before opening
			// We should ignore file here as if it did not exist.
			return nil, newIgnoredFileError(cfi.path)
		}
		switch err.(type) {
		case nil:
			fileReadCloser = &ioextensions.ReadCascadeCloser{
				Reader: &io.LimitedReader{
					R: io.MultiReader(fileReadCloser, &ioextensions.ZeroReader{}),
					N: cfi.header.Size,
				},
				Closer: fileReadCloser,
			}
		case InvalidBlockError: // fallback to full file backup
			tracelog.WarningLogger.Printf("failed to read file '%s' as incremented\n", cfi.header.Name)
			cfi.isIncremented = false
			fileReadCloser, err = startReadingFile(cfi.header, cfi.fileInfo, cfi.path, fileReadCloser)
			if err != nil {
				return nil, err
			}
		default:
			return nil, errors.Wrapf(err, "PackFileIntoTar: failed reading incremental file '%s'\n", cfi.path)
		}
	} else {
		var err error
		fileReadCloser, err = startReadingFile(cfi.header, cfi.fileInfo, cfi.path, fileReadCloser)
		if err != nil {
			return nil, err
		}
	}
	return fileReadCloser, nil
}

// TODO : unit tests
func startReadingFile(fileInfoHeader *tar.Header, info os.FileInfo, path string, fileReader io.ReadCloser) (io.ReadCloser, error) {
	fileInfoHeader.Size = info.Size()
	file, err := os.Open(path)
	if err != nil {
		return nil, errors.Wrapf(err, "startReadingFile: failed to open file '%s'\n", path)
	}
	diskLimitedFileReader := NewDiskLimitReader(file)
	fileReader = &ioextensions.ReadCascadeCloser{
		Reader: &io.LimitedReader{
			R: io.MultiReader(diskLimitedFileReader, &ioextensions.ZeroReader{}),
			N: fileInfoHeader.Size,
		},
		Closer: file,
	}
	return fileReader, nil
}

func verifyFile(path string, fileInfo os.FileInfo, fileReader io.Reader, isIncremented bool) ([]uint32, error) {
	if !isPagedFile(fileInfo, path) {
		_, err := io.Copy(ioutil.Discard, fileReader)
		return nil, err
	}

	if isIncremented {
		return VerifyPagedFileIncrement(path, fileInfo, fileReader)
	}
	return VerifyPagedFileBase(path, fileInfo, fileReader)
}

// TeeReadCloser creates two io.ReadClosers from one
func newTeeReadCloser(readCloser io.ReadCloser) (io.ReadCloser, io.ReadCloser) {
	pipeReader, pipeWriter := io.Pipe()

	// teeReader is used to provide the readCloser to two consumers
	teeReader := io.TeeReader(readCloser, pipeWriter)
	// MultiCloser closes both pipeWriter and readCloser on Close() call
	closer := ioextensions.NewMultiCloser([]io.Closer{readCloser, pipeWriter})
	return &ioextensions.ReadCascadeCloser{Reader: teeReader, Closer: closer}, pipeReader
}
