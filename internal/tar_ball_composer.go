package internal

import (
	"archive/tar"
	"errors"
	"github.com/jackc/pgx"
	"os"
)

// TarBallComposer is used to compose files into tarballs.
type TarBallComposer interface {
	AddFile(info *ComposeFileInfo)
	AddHeader(header *tar.Header, fileInfo os.FileInfo) error
	SkipFile(tarHeader *tar.Header, fileInfo os.FileInfo)
	PackTarballs() (TarFileSets, error)
	GetFiles() BundleFiles
}

// ComposeFileInfo holds data which is required to pack a file to some tarball
type ComposeFileInfo struct {
	path          string
	fileInfo      os.FileInfo
	wasInBase     bool
	header        *tar.Header
	isIncremented bool
}

type TarFileSets map[string][]string

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

// TarBallComposerMaker is used to make an instance of TarBallComposer
type TarBallComposerMaker interface {
	Make(bundle *Bundle) (TarBallComposer, error)
}

func NewTarBallComposerMaker(composerType TarBallComposerType, conn *pgx.Conn) (TarBallComposerMaker, error) {
	switch composerType {
	case RegularComposer:
		return NewRegularTarBallComposerMaker(), nil
	case RatingComposer:
		relFileStats, err := newRelFileStatistics(conn)
		if err != nil {
			return nil, err
		}
		return NewRatingTarBallComposerMaker(relFileStats)
	default:
		return nil, errors.New("NewTarBallComposerMaker: Unknown TarBallComposerType")
	}
}
