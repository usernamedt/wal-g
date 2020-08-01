package internal

import (
	"archive/tar"
	"github.com/jackc/pgx"
	"os"
)

type TarBallComposer interface {
	AddFile(info *ComposeFileInfo)
	AddHeader(header *tar.Header, fileInfo os.FileInfo)
	SkipFile(tarHeader *tar.Header, fileInfo os.FileInfo)
	PackTarballs() (map[string][]string, error)
	GetFiles() SentinelFileList
}

type ComposeFileInfo struct {
	path          string
	fileInfo      os.FileInfo
	wasInBase     bool
	header        *tar.Header
	isIncremented bool
}

func NewComposeFileInfo(path string, fileInfo os.FileInfo, wasInBase, isIncremented bool,
	header *tar.Header) *ComposeFileInfo {
	return &ComposeFileInfo{path: path, fileInfo: fileInfo,
		wasInBase: wasInBase, header: header, isIncremented: isIncremented}
}

type TarBallComposerType int

const (
	RegularComposer TarBallComposerType = iota + 1
	RatingComposer
)

func NewTarBallComposer(composerType TarBallComposerType, bundle *Bundle, conn *pgx.Conn) (TarBallComposer, error) {
	switch composerType {
	case RegularComposer:
		return NewRegularTarBallComposer(bundle.IncrementFromLsn, bundle.DeltaMap,
			bundle.TarBallQueue, bundle.Crypter), nil
	case RatingComposer:
		return NewRatingTarBallComposer(
			uint64(bundle.tarSizeThreshold),
			NewDefaultComposeRatingEvaluator(bundle.IncrementFromFiles),
			bundle.IncrementFromLsn,
			bundle.DeltaMap,
			bundle.TarBallQueue,
			bundle.Crypter,
			conn)
	default:
		return NewRegularTarBallComposer(bundle.IncrementFromLsn, bundle.DeltaMap,
			bundle.TarBallQueue, bundle.Crypter), nil
	}
}