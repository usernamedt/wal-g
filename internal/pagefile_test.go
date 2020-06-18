package internal_test

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/utility"
)

const (
	pagedFileName               = "../test/testdata/base_paged_file.bin"
	pagedFileSizeInBytes        = 65536
	pagedFileBlockCount         = pagedFileSizeInBytes / internal.DatabasePageSize
	sampleLSN            uint64 = 0xc6bd4600
	smallLSN             uint64 = 0
	bigLSN                      = sampleLSN * 2
)

var regularIncrementBuf = readIncrementToBuffer(sampleLSN)
var smallLsnIncrementBuf = readIncrementToBuffer(smallLSN)
var bigLsnIncrementBuf = readIncrementToBuffer(bigLSN)

// In this test series we use actual postgres paged file which
// We compute increment with LSN taken from the middle of a file
// Resulting increment is than applied to copy of the same file partially wiped
// Then incremented file is binary compared to the origin
func TestIncrementingFile(t *testing.T) {
	incrementReader := bytes.NewReader(regularIncrementBuf)
	postgresApplyIncrementTest(incrementReader, t)
}

// This test covers the case of empty increment
func TestIncrementingFileBigLSN(t *testing.T) {
	incrementReader := bytes.NewReader(bigLsnIncrementBuf)
	postgresApplyIncrementTest(incrementReader, t)
}

// This test covers the case when increment is bigger than original file
func TestIncrementingFileSmallLSN(t *testing.T) {
	incrementReader := bytes.NewReader(smallLsnIncrementBuf)
	postgresApplyIncrementTest(incrementReader, t)
}

func postgresApplyIncrementTest(incrementReader io.Reader, t *testing.T) {
	tmpFileName := pagedFileName + "_tmp"
	copyFile(pagedFileName, tmpFileName)
	defer os.Remove(tmpFileName)
	tmpFile, _ := os.OpenFile(tmpFileName, os.O_RDWR, 0666)
	tmpFile.WriteAt(make([]byte, 12345), 477421568-12345)
	tmpFile.Close()
	err := internal.ApplyFileIncrement(tmpFileName, incrementReader, false)
	assert.NoError(t, err)
	_, err = incrementReader.Read(make([]byte, 1))
	assert.Equalf(t, io.EOF, err, "Not read to the end")
	compare := deepCompare(pagedFileName, tmpFileName)
	assert.Truef(t, compare, "Increment could not restore file")
}

// In this test series we test that
// increment is being correctly created
// for various LSN
func TestReadingIncrement(t *testing.T) {
	postgresReadIncrementTest(sampleLSN, t)
}

func TestReadingIncrementBigLSN(t *testing.T) {
	postgresReadIncrementTest(bigLSN, t)
}

func TestReadingIncrementSmallLSN(t *testing.T) {
	postgresReadIncrementTest(smallLSN, t)
}

// this test checks that increment is being read correctly
func postgresReadIncrementTest(localLSN uint64, t *testing.T) {
	fileInfo, err := os.Stat(pagedFileName)
	if err != nil {
		fmt.Print(err.Error())
	}
	reader, size, err := internal.ReadIncrementalFile(pagedFileName, fileInfo.Size(), localLSN, nil)
	if err != nil {
		fmt.Print(err.Error())
	}
	buf, _ := ioutil.ReadAll(reader)
	assert.Falsef(t, localLSN != 0 && int64(len(buf)) >= fileInfo.Size(), "Increment is too big")

	assert.Falsef(t, localLSN == 0 && int64(len(buf)) <= fileInfo.Size(), "Increment is expected to be bigger than file")
	// We also check that increment correctly predicted it's size
	// This is important for Tar archiver, which writes size in the header
	assert.Equalf(t, len(buf), int(size), "Increment has wrong size")
}

func TestReadIncrementFileHeader_Valid(t *testing.T) {
	readIncrementFileHeaderTest(t, internal.IncrementFileHeader, nil)
}

func TestReadIncrementFileHeader_InvalidIncrementFileHeaderError(t *testing.T) {
	dataArray := [][]byte{
		{'w', 'i', '1', 0x56},
		{'x', 'i', '1', internal.SignatureMagicNumber},
		{'w', 'j', '1', internal.SignatureMagicNumber},
	}
	for _, data := range dataArray {
		readIncrementFileHeaderTest(t, data, internal.InvalidIncrementFileHeaderError{})
	}
}

func TestReadIncrementFileHeader_UnknownIncrementFileHeaderError(t *testing.T) {
	readIncrementFileHeaderTest(t, []byte{'w', 'i', '2', internal.SignatureMagicNumber}, internal.UnknownIncrementFileHeaderError{})
}

func readIncrementFileHeaderTest(t *testing.T, headerData []byte, expectedErr error) {
	err := internal.ReadIncrementFileHeader(bytes.NewReader(headerData))
	assert.IsType(t, err, expectedErr)
}

// In this test series we test that new page file
// is being correctly created from increment file
// with different increment cases
func TestCreatingFileFromIncrement(t *testing.T) {
	// separate reader to get values from increment for assertion
	incrementReader := bytes.NewReader(regularIncrementBuf)
	assertDataIncrementReader := bytes.NewReader(regularIncrementBuf)
	postgresCreateFileFromIncrementTest(assertDataIncrementReader, incrementReader, t)
}

func TestCreatingFileFromIncrementBigLSN(t *testing.T) {
	// separate reader to get values from increment for assertion
	incrementReader := bytes.NewReader(bigLsnIncrementBuf)
	assertDataIncrementReader := bytes.NewReader(bigLsnIncrementBuf)
	postgresCreateFileFromIncrementTest(assertDataIncrementReader, incrementReader, t)
}

func TestCreatingFileFromIncrementSmallLSN(t *testing.T) {
	// separate reader to get values from increment for assertion
	incrementReader := bytes.NewReader(smallLsnIncrementBuf)
	assertDataIncrementReader := bytes.NewReader(smallLsnIncrementBuf)
	postgresCreateFileFromIncrementTest(assertDataIncrementReader, incrementReader, t)
}

func postgresCreateFileFromIncrementTest(incrementAssertDataReader io.Reader, incrementReader io.Reader, t *testing.T) {
	fileSize, diffBlockCount, _, _ := internal.GetIncrementHeaderFields(incrementAssertDataReader)
	mockFile := NewMockReaderAtWriterAt(make([]byte, 0))

	err := internal.CreateFileFromIncrement(incrementReader, mockFile)
	assert.NoError(t, err)
	assert.Equal(t, fileSize, uint64(len(mockFile.content)))

	sourceFile, _ := os.Open(pagedFileName)
	defer utility.LoggedClose(sourceFile, "")
	emptyPage := make([]byte, internal.DatabasePageSize)
	emptyBlockCount := uint32(0)
	dataBlockCount := uint32(0)

	for index, data := range mockFile.getBlocks() {
		readBytes := make([]byte, internal.DatabasePageSize)
		sourceFile.ReadAt(readBytes, index*internal.DatabasePageSize)
		if bytes.Equal(emptyPage, data) {
			emptyBlockCount += 1
			continue
		}
		// Verify that each written block corresponds
		// to the actual block in the original page file.
		assert.Equal(t, readBytes, data)
		dataBlockCount += 1
	}
	// Make sure that we wrote exactly the same amount of blocks that was written to the increment header.
	// These two numbers may not be equal if the increment was taken
	// while the database cluster was running, but in this test case they should match.
	assert.Equal(t, diffBlockCount, dataBlockCount)
}

// todo
func TestWritingIncrementToCompletedFile(t *testing.T) {
	incrementReader := bytes.NewReader(regularIncrementBuf)
	postgresWriteIncrementTestCompletedFile(incrementReader, t)
}

// todo
func TestWritingIncrementToCompletedFileSmallLSN(t *testing.T) {
	incrementReader := bytes.NewReader(smallLsnIncrementBuf)
	postgresWriteIncrementTestCompletedFile(incrementReader, t)
}

// todo
func TestWritingIncrementToCompletedFileBigLSN(t *testing.T) {
	incrementReader := bytes.NewReader(bigLsnIncrementBuf)
	postgresWriteIncrementTestCompletedFile(incrementReader, t)
}

func postgresWriteIncrementTestCompletedFile(incrementReader io.Reader, t *testing.T) {
	content := createPageFileContent(1, pagedFileBlockCount)
	mockFile := NewMockReaderAtWriterAt(content)

	err := internal.WritePagesFromIncrement(incrementReader, mockFile, false)

	assert.NoError(t, err)
	// check that no bytes were written to the mock file
	assert.Equal(t, 0, mockFile.bytesWritten)
}

// todo
func TestWritingIncrementToEmptyFile(t *testing.T) {
	incrementReader := bytes.NewReader(regularIncrementBuf)
	assertDataIncrementReader := bytes.NewReader(regularIncrementBuf)
	postgresWritePagesTestEmptyFile(assertDataIncrementReader, incrementReader, t)
}

// todo
func TestWritingIncrementToEmptyFileSmallLSN(t *testing.T) {
	incrementReader := bytes.NewReader(smallLsnIncrementBuf)
	assertDataIncrementReader := bytes.NewReader(smallLsnIncrementBuf)
	postgresWritePagesTestEmptyFile(assertDataIncrementReader, incrementReader, t)
}

// todo
func TestWritingIncrementToEmptyFileBigLSN(t *testing.T) {
	//pass
	return
}

func postgresWritePagesTestEmptyFile(incrementAssertDataReader io.Reader, incrementReader io.Reader, t *testing.T) {
	fileSize, diffBlockCount, _, _ := internal.GetIncrementHeaderFields(incrementAssertDataReader)
	content := make([]byte, internal.DatabasePageSize*pagedFileBlockCount)
	mockFile := NewMockReaderAtWriterAt(content)
	err := internal.WritePagesFromIncrement(incrementReader, mockFile, false)
	assert.NoError(t, err)
	assert.Equal(t, fileSize, uint64(len(mockFile.content)))

	sourceFile, _ := os.Open(pagedFileName)
	defer utility.LoggedClose(sourceFile, "")
	emptyPage := make([]byte, internal.DatabasePageSize)
	emptyBlockCount := uint32(0)
	dataBlockCount := uint32(0)

	for index, data := range mockFile.getBlocks() {
		readBytes := make([]byte, internal.DatabasePageSize)
		sourceFile.ReadAt(readBytes, index*internal.DatabasePageSize)
		if bytes.Equal(emptyPage, data) {
			emptyBlockCount += 1
			continue
		}
		assert.Equal(t, readBytes, data)
		dataBlockCount += 1
	}
	assert.Equal(t, diffBlockCount, dataBlockCount)
}

func TestRestoringPagesToCompletedFile(t *testing.T) {
	pagedFile, _ := os.Open(pagedFileName)
	fileReader := io.Reader(pagedFile)
	defer utility.LoggedClose(pagedFile, "")
	content := createPageFileContent(1, pagedFileBlockCount)
	mockFile := NewMockReaderAtWriterAt(content)

	err := internal.RestoreMissingPages(fileReader, mockFile)

	assert.NoError(t, err)
	// check that no bytes were written to the mock file
	assert.Equal(t, 0, mockFile.bytesWritten)
}

func TestRestoringPagesToEmptyFile(t *testing.T) {
	pagedFile, _ := os.Open(pagedFileName)
	fileReader := io.Reader(pagedFile)
	defer utility.LoggedClose(pagedFile, "")
	content := make([]byte, internal.DatabasePageSize*pagedFileBlockCount)
	mockFile := NewMockReaderAtWriterAt(content)

	err := internal.RestoreMissingPages(fileReader, mockFile)

	assert.NoError(t, err)
	dataBlockCount := int64(0)
	for index, data := range mockFile.getBlocks() {
		readBytes := make([]byte, internal.DatabasePageSize)
		pagedFile.ReadAt(readBytes, index*internal.DatabasePageSize)
		assert.Equal(t, readBytes, data)
		dataBlockCount += 1
	}
	assert.Equal(t, dataBlockCount, pagedFileBlockCount)
}

func readIncrementToBuffer(localLSN uint64) []byte {
	fileInfo, _ := os.Stat(pagedFileName)
	reader, _, _ := internal.ReadIncrementalFile(pagedFileName, fileInfo.Size(), localLSN, nil)
	buf, _ := ioutil.ReadAll(reader)
	return buf
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer utility.LoggedClose(in, "")

	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer utility.LoggedClose(out, "")

	_, err = io.Copy(out, in)
	if err != nil {
		return err
	}
	return out.Close()
}

const chunkSize = 64

func deepCompare(file1, file2 string) bool {
	// Check file size ...

	f1, err := os.Open(file1)
	if err != nil {
		log.Fatal(err)
	}

	f2, err := os.Open(file2)
	if err != nil {
		log.Fatal(err)
	}
	return deepCompareReaders(f1, f2)
}

func deepCompareReaders(r1, r2 io.Reader) bool {
	var chunkNumber = 0
	for {
		b1 := make([]byte, chunkSize)
		_, err1 := r1.Read(b1)

		b2 := make([]byte, chunkSize)
		_, err2 := r2.Read(b2)

		if err1 != nil || err2 != nil {
			if err1 == io.EOF && err2 == io.EOF {
				return true
			} else if err1 == io.EOF || err2 == io.EOF {
				return false
			} else {
				log.Fatal(err1, err2)
			}
		}

		if !bytes.Equal(b1, b2) {
			log.Printf("Bytes at %v differ\n", chunkNumber*chunkSize)
			log.Println(b1)
			log.Println(b2)
			return false
		}
		chunkNumber++
	}
}

type MockFileInfo struct {
	name string
	size int64
}

func (mfi *MockFileInfo) Name() string {
	return mfi.name
}
func (mfi *MockFileInfo) Size() int64 {
	return mfi.size
}
func (mfi *MockFileInfo) Mode() os.FileMode {
	return 0
}
func (mfi *MockFileInfo) ModTime() time.Time {
	return time.Time{}
}
func (mfi *MockFileInfo) IsDir() bool {
	return false
}
func (mfi *MockFileInfo) Sys() interface{} {
	return nil
}

type MockReadWriterAt struct {
	mockFileInfo *MockFileInfo
	bytesWritten int
	content      []byte
}

func NewMockReaderAtWriterAt(content []byte) *MockReadWriterAt {
	mockFileInfo := &MockFileInfo{name: pagedFileName + "_mock", size: int64(len(content))}
	return &MockReadWriterAt{mockFileInfo: mockFileInfo, content: content}
}

func (mrw *MockReadWriterAt) WriteAt(b []byte, offset int64) (n int, err error) {
	bytesCount := uint64(offset) + uint64(len(b))
	for uint64(len(mrw.content)) < bytesCount {
		// silly slice expand method but for tests seems fine
		mrw.content = append(mrw.content, 0)
	}
	copy(mrw.content[offset:], b)
	mrw.bytesWritten += len(b)
	return len(b), nil
}

func (mrw *MockReadWriterAt) getBlocks() map[int64][]byte {
	totalBlockCount := int64(len(mrw.content)) / internal.DatabasePageSize
	result := make(map[int64][]byte, totalBlockCount)
	for i := int64(0); i < totalBlockCount; i++ {
		result[i] = make([]byte, internal.DatabasePageSize)
		_, _ = mrw.ReadAt(result[i], i*internal.DatabasePageSize)
	}
	return result
}

func (mrw *MockReadWriterAt) ReadAt(b []byte, offset int64) (n int, err error) {
	block := mrw.content[offset : offset+int64(len(b))]
	copy(b, block)
	return len(block), nil
}

func (mrw *MockReadWriterAt) Stat() (os.FileInfo, error) {
	return mrw.mockFileInfo, nil
}

func (mrw *MockReadWriterAt) Name() string {
	return "test"
}

func createPageFileContent(value byte, pageCount int64) []byte {
	pageFileContent := make([]byte, internal.DatabasePageSize*pageCount)
	for i := 0; i < len(pageFileContent); i++ {
		pageFileContent[i] = value
	}
	return pageFileContent
}
