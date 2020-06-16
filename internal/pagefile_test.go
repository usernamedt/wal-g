package internal_test

import (
	"bytes"
	"fmt"
	"github.com/wal-g/wal-g/internal/walparser/parsingutil"
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
	pagedFileName        = "../test/testdata/base_paged_file.bin"
	sampleLSN     uint64 = 0xc6bd4600
)

// In this test we use actual postgres paged file which
// We compute increment with LSN taken from the middle of a file
// Resulting increment is than applied to copy of the same file partially wiped
// Then incremented file is binary compared to the origin
func TestIncrementingFile(t *testing.T) {
	localLSN := sampleLSN
	postgresApplyIncrementTest(localLSN, t)
}

// This test covers the case of empty increment
func TestIncrementingFileBigLSN(t *testing.T) {
	localLSN := sampleLSN * 2
	postgresApplyIncrementTest(localLSN, t)
}

// This test converts the case when increment is bigger than original file
func TestIncrementingFileSmallLSN(t *testing.T) {
	localLSN := uint64(0)
	postgresApplyIncrementTest(localLSN, t)
}

func TestReadingIncrement(t *testing.T) {
	lsnToTest := []uint64{0,sampleLSN, sampleLSN * 2}
	for _, lsn := range lsnToTest {
		postgresReadIncrementTest(lsn, t)
	}
}

// this test checks that restored file is correct
func postgresApplyIncrementTest(localLSN uint64, t *testing.T) {
	buf := readIncrementToBuffer(localLSN)
	tmpFileName := pagedFileName + "_tmp"
	copyFile(pagedFileName, tmpFileName)
	defer os.Remove(tmpFileName)
	tmpFile, _ := os.OpenFile(tmpFileName, os.O_RDWR, 0666)
	tmpFile.WriteAt(make([]byte, 12345), 477421568-12345)
	tmpFile.Close()
	newReader := bytes.NewReader(buf)
	err := internal.ApplyFileIncrement(tmpFileName, newReader, false)
	assert.NoError(t, err)
	_, err = newReader.Read(make([]byte, 1))
	assert.Equalf(t, io.EOF, err, "Not read to the end")
	compare := deepCompare(pagedFileName, tmpFileName)
	assert.Truef(t, compare, "Increment could not restore file")
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
	return deepCompareReaders(f1,f2)
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

func readIncrementFileHeaderTest(t *testing.T, headerData []byte, expectedErr error) {
	err := internal.ReadIncrementFileHeader(bytes.NewReader(headerData))
	assert.IsType(t, err, expectedErr)
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

// todo
func TestCreatingFileFromIncrement(t *testing.T) {
	localLSN := sampleLSN
	postgresCreateFileFromIncrementTest(localLSN, t)
}
// todo
func TestCreatingFileFromIncrementBigLSN(t *testing.T) {
	localLSN := sampleLSN * 2
	postgresCreateFileFromIncrementTest(localLSN, t)
}
// todo
func TestCreatingFileFromIncrementSmallLSN(t *testing.T) {
	localLSN := uint64(0)
	postgresCreateFileFromIncrementTest(localLSN, t)
}

// todo
func TestWritingIncrementToCompletedFile(t *testing.T) {
	localLSN := sampleLSN
	postgresWriteIncrementTestCompletedFile(localLSN, t)
}
// todo
func TestWritingIncrementToCompletedFileSmallLSN(t *testing.T) {
	localLSN := uint64(0)
	postgresWriteIncrementTestCompletedFile(localLSN, t)
}

// todo
func TestWritingIncrementToEmptyFile(t *testing.T) {
	localLSN := sampleLSN
	postgresWritePagesTestEmptyFile(localLSN, t)
}
// todo
func TestWritingIncrementToEmptyFileSmallLSN(t *testing.T) {
	localLSN := uint64(0)
	postgresWritePagesTestEmptyFile(localLSN, t)
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
	mockFileInfo        *MockFileInfo
	bytesWritten        int
	blocks              map[int64][]byte
	maxWrittenByteIndex uint64
}

func NewMockReaderAtWriterAt(blocksWritten map[int64][]byte) *MockReadWriterAt {
	mockFileInfo := &MockFileInfo{name: pagedFileName + "_mock", size: 65536}
	return &MockReadWriterAt{blocks: blocksWritten, mockFileInfo: mockFileInfo}
}

func (mrw *MockReadWriterAt) WriteAt(b []byte, offset int64) (n int, err error) {
	mrw.bytesWritten += len(b)
	blockIndex := offset / internal.DatabasePageSize
	newBlock := make([]byte, internal.DatabasePageSize)
	copy(newBlock, b)
	mrw.blocks[blockIndex] = newBlock

	lastByteIndex := uint64(offset) + uint64(len(b))
	if lastByteIndex > mrw.maxWrittenByteIndex {
		mrw.maxWrittenByteIndex = lastByteIndex
	}

	return len(b), nil
}

func (mrw *MockReadWriterAt) ReadAt(b []byte, offset int64) (n int, err error) {
	blockIndex := offset / internal.DatabasePageSize
	block, _ := mrw.blocks[blockIndex]
	copy(b, block)
	return len(block), nil
}

func (mrw *MockReadWriterAt) Stat() (os.FileInfo, error) {
	return mrw.mockFileInfo, nil
}

func (mrw *MockReadWriterAt) Name() string {
	return "test"
}

func postgresCreateFileFromIncrementTest(localLSN uint64, t *testing.T) {
	buf := readIncrementToBuffer(localLSN)
	incrementReader := bytes.NewReader(buf)
	err := internal.ReadIncrementFileHeader(incrementReader)
	assert.NoError(t, err)

	var fileSize uint64
	var diffBlockCount uint32
	err = parsingutil.ParseMultipleFieldsFromReader([]parsingutil.FieldToParse{
		{Field: &fileSize, Name: "fileSize"},
		{Field: &diffBlockCount, Name: "diffBlockCount"},
	}, incrementReader)
	assert.NoError(t, err)

	newReader := bytes.NewReader(buf)
	mockFile := NewMockReaderAtWriterAt(make(map[int64][]byte,0))

	err = internal.CreateFileFromIncrement(newReader, mockFile)
	assert.NoError(t, err)
	assert.Equal(t, fileSize, mockFile.maxWrittenByteIndex)

	sourceFile, _ := os.Open(pagedFileName)
	defer utility.LoggedClose(sourceFile, "")
	emptyPage := make([]byte, internal.DatabasePageSize)
	emptyBlockCount := uint32(0)
	dataBlockCount := uint32(0)

	for index, data := range mockFile.blocks {
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

func createPageOf(value byte) []byte {
	page := make([]byte, internal.DatabasePageSize)
	for i:=int64(0); i< internal.DatabasePageSize; i++ {
		page[i] = value
	}
	return page
}

func postgresWritePagesTestEmptyFile(localLSN uint64, t *testing.T) {
	buf := readIncrementToBuffer(localLSN)
	incrementReader := bytes.NewReader(buf)
	err := internal.ReadIncrementFileHeader(incrementReader)
	assert.NoError(t, err)

	pagedFile, _ := os.Open(pagedFileName)
	defer utility.LoggedClose(pagedFile, "")
	pagedFileInfo, _ := pagedFile.Stat()
	pagedFileBlockCount := pagedFileInfo.Size() / internal.DatabasePageSize
	pagedFileBlocks := make(map[int64][]byte, pagedFileBlockCount)
	for i := int64(0); i < pagedFileBlockCount; i++ {
		// write each page with zeros
		pagedFileBlocks[i] = make([]byte, internal.DatabasePageSize)
	}

	var fileSize uint64
	var diffBlockCount uint32
	err = parsingutil.ParseMultipleFieldsFromReader([]parsingutil.FieldToParse{
		{Field: &fileSize, Name: "fileSize"},
		{Field: &diffBlockCount, Name: "diffBlockCount"},
	}, incrementReader)
	assert.NoError(t, err)

	newReader := bytes.NewReader(buf)

	mockFile := NewMockReaderAtWriterAt(pagedFileBlocks)

	err = internal.WritePagesFromIncrement(newReader, mockFile, false)
	assert.NoError(t, err)
	assert.Equal(t, fileSize, mockFile.maxWrittenByteIndex)

	sourceFile, _ := os.Open(pagedFileName)
	defer utility.LoggedClose(sourceFile, "")
	emptyPage := make([]byte, internal.DatabasePageSize)
	emptyBlockCount := uint32(0)
	dataBlockCount := uint32(0)

	for index, data := range mockFile.blocks {
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

func postgresWriteIncrementTestCompletedFile(localLSN uint64, t *testing.T) {
	buf := readIncrementToBuffer(localLSN)
	incrementReader := bytes.NewReader(buf)
	err := internal.ReadIncrementFileHeader(incrementReader)
	assert.NoError(t, err)

	pagedFile, _ := os.Open(pagedFileName)
	defer utility.LoggedClose(pagedFile, "")
	pagedFileInfo, _ := pagedFile.Stat()
	pagedFileBlockCount := pagedFileInfo.Size() / internal.DatabasePageSize
	pagedFileBlocks := make(map[int64][]byte, pagedFileBlockCount)
	for i := int64(0); i < pagedFileBlockCount; i++ {
		pagedFileBlocks[i] = createPageOf(1)
	}

	var fileSize uint64
	var diffBlockCount uint32
	err = parsingutil.ParseMultipleFieldsFromReader([]parsingutil.FieldToParse{
		{Field: &fileSize, Name: "fileSize"},
		{Field: &diffBlockCount, Name: "diffBlockCount"},
	}, incrementReader)
	assert.NoError(t, err)

	newReader := bytes.NewReader(buf)
	mockFile := NewMockReaderAtWriterAt(pagedFileBlocks)
	err = internal.WritePagesFromIncrement(newReader, mockFile, false)
	assert.NoError(t, err)
	// check that no bytes were written to the mock file
	assert.Equal(t,0, mockFile.bytesWritten)
}

func TestRestoringPagesToCompletedFile(t *testing.T) {
	pagedFile, _ := os.Open(pagedFileName)
	defer utility.LoggedClose(pagedFile, "")
	pagedFileInfo, _ := pagedFile.Stat()
	pagedFileBlockCount := pagedFileInfo.Size() / internal.DatabasePageSize
	pagedFileBlocks := make(map[int64][]byte, pagedFileBlockCount)
	for i := int64(0); i < pagedFileBlockCount; i++ {
		pagedFileBlocks[i] = createPageOf(1)
	}

	mockFile := NewMockReaderAtWriterAt(pagedFileBlocks)
	err := internal.RestoreMissingPages(pagedFile, mockFile)
	assert.NoError(t, err)
	// check that no bytes were written to the mock file
	assert.Equal(t, 0, mockFile.bytesWritten)
}

func TestRestoringPagesToEmptyFile(t *testing.T) {
	pagedFile, _ := os.Open(pagedFileName)
	defer utility.LoggedClose(pagedFile, "")
	pagedFileInfo, _ := pagedFile.Stat()
	pagedFileBlockCount := pagedFileInfo.Size() / internal.DatabasePageSize
	pagedFileBlocks := make(map[int64][]byte, pagedFileBlockCount)
	for i := int64(0); i < pagedFileBlockCount; i++ {
		pagedFileBlocks[i] = make([]byte, internal.DatabasePageSize)
	}

	mockFile := NewMockReaderAtWriterAt(pagedFileBlocks)
	err := internal.RestoreMissingPages(pagedFile, mockFile)
	assert.NoError(t, err)

	dataBlockCount := int64(0)
	for index, data := range mockFile.blocks {
		readBytes := make([]byte, internal.DatabasePageSize)
		pagedFile.ReadAt(readBytes, index*internal.DatabasePageSize)
		assert.Equal(t, readBytes, data)
		dataBlockCount += 1
	}
	assert.Equal(t, dataBlockCount, pagedFileBlockCount)
}