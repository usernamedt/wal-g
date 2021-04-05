package postgres_test

import (
	"bytes"
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/compression"
	"github.com/wal-g/wal-g/internal/compression/lz4"
	"github.com/wal-g/wal-g/internal/compression/lzma"
	"github.com/wal-g/wal-g/testtools"
	"github.com/wal-g/wal-g/utility"
)

func TestWallFetchCachesLastDecompressor(t *testing.T) {
	folder := testtools.MakeDefaultInMemoryStorageFolder().GetSubFolder(utility.WalPath)

	type TestData struct {
		filename     string
		decompressor compression.Decompressor
	}

	testData := []TestData{{"00000001000000000000007C", lz4.Decompressor{}},
		{"00000001000000000000007F", lzma.Decompressor{}}}

	for _, data := range testData {
		walFilename, decompressor := data.filename, data.decompressor

		assert.NoError(t, folder.PutObject(walFilename+"."+decompressor.FileExtension(),
			bytes.NewReader([]byte("test data"))))

		_, err := internal.DownloadAndDecompressStorageFile(folder, walFilename)
		assert.NoError(t, err)

		last, err := internal.GetLastDecompressor()

		assert.NoError(t, err)
		assert.Equal(t, decompressor, last)
	}
}

func TestSetLastDecompressorWorkWell(t *testing.T) {
	for _, decompressor := range compression.Decompressors {
		_ = internal.SetLastDecompressor(decompressor)
		last, err := internal.GetLastDecompressor()

		assert.NoError(t, err)
		assert.Equal(t, last, decompressor)
	}
}

func TestTryDownloadWALFile_Exist(t *testing.T) {
	expectedData := []byte("mock")
	folder := testtools.MakeDefaultInMemoryStorageFolder().GetSubFolder(utility.WalPath)
	folder.PutObject(WalFilename, bytes.NewBuffer(expectedData))
	archiveReader, exist, err := internal.TryDownloadFile(folder, WalFilename)
	assert.NoError(t, err)
	assert.True(t, exist)
	actualData, err := ioutil.ReadAll(archiveReader)
	assert.NoError(t, err)
	assert.Equal(t, expectedData, actualData)
}

func TestTryDownloadWALFile_NotExist(t *testing.T) {
	folder := testtools.MakeDefaultInMemoryStorageFolder()
	reader, exist, err := internal.TryDownloadFile(folder, WalFilename)
	assert.Nil(t, reader)
	assert.False(t, exist)
	assert.NoError(t, err)
}
