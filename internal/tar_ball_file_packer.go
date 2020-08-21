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

func newTarBallFilePacker(deltaMap PagedFileDeltaMap, incrementFromLsn *uint64, files BundleFiles,
	verifyPageChecksums bool) *TarBallFilePacker {
	return &TarBallFilePacker{
		deltaMap:            deltaMap,
		incrementFromLsn:    incrementFromLsn,
		files:               files,
		verifyPageChecksums: verifyPageChecksums,
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
	fileReader, err := p.createFileReader(cfi)
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
	pipeReader, pipeWriter := io.Pipe()
	teeReader := io.TeeReader(fileReader, pipeWriter)

	errorGroup, _ := errgroup.WithContext(context.Background())
	errorGroup.Go(func() error {
		// Close the pipeWriter after the TeeReader completes to trigger EOF
		defer utility.LoggedClose(fileReader, "")
		defer pipeWriter.Close()

		packedFileSize, err := PackFileTo(tarBall, cfi.header, teeReader)
		if err != nil {
			return errors.Wrap(err, "PackFileIntoTar: operation failed")
		}
		if packedFileSize != cfi.header.Size {
			return newTarSizeError(packedFileSize, cfi.header.Size)
		}
		return nil
	})

	if p.verifyPageChecksums {
		errorGroup.Go(func() (err error) {
			corruptBlocks, err := verifyFile(cfi.path, cfi.fileInfo, pipeReader, cfi.isIncremented)
			if err != nil {
				return err
			}
			p.files.AddFileWithCorruptBlocks(cfi.header, cfi.fileInfo, cfi.isIncremented, corruptBlocks)
			return nil
		})
		return errorGroup.Wait()
	}

	p.files.AddFile(cfi.header, cfi.fileInfo, cfi.isIncremented)
	return errorGroup.Wait()
}

func (p *TarBallFilePacker) createFileReader(cfi *ComposeFileInfo) (io.ReadCloser, error) {
	var fileReader io.ReadCloser
	if cfi.isIncremented {
		bitmap, err := p.getDeltaBitmapFor(cfi.path)
		if _, ok := err.(NoBitmapFoundError); ok { // this file has changed after the start of backup, so just skip it
			return nil, newSkippedFileError(cfi.path)
		} else if err != nil {
			return nil, errors.Wrapf(err, "PackFileIntoTar: failed to find corresponding bitmap '%s'\n", cfi.path)
		}
		fileReader, cfi.header.Size, err = ReadIncrementalFile(cfi.path, cfi.fileInfo.Size(), *p.incrementFromLsn, bitmap)
		if os.IsNotExist(err) { // File was deleted before opening
			// We should ignore file here as if it did not exist.
			return nil, newIgnoredFileError(cfi.path)
		}
		switch err.(type) {
		case nil:
			fileReader = &ioextensions.ReadCascadeCloser{
				Reader: &io.LimitedReader{
					R: io.MultiReader(fileReader, &ioextensions.ZeroReader{}),
					N: cfi.header.Size,
				},
				Closer: fileReader,
			}
		case InvalidBlockError: // fallback to full file backup
			tracelog.WarningLogger.Printf("failed to read file '%s' as incremented\n", cfi.header.Name)
			cfi.isIncremented = false
			fileReader, err = startReadingFile(cfi.header, cfi.fileInfo, cfi.path, fileReader)
			if err != nil {
				return nil, err
			}
		default:
			return nil, errors.Wrapf(err, "PackFileIntoTar: failed reading incremental file '%s'\n", cfi.path)
		}
	} else {
		var err error
		fileReader, err = startReadingFile(cfi.header, cfi.fileInfo, cfi.path, fileReader)
		if err != nil {
			return nil, err
		}
	}
	return fileReader, nil
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
