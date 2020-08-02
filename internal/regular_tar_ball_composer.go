package internal

import (
	"archive/tar"
	"github.com/wal-g/wal-g/internal/crypto"
	"os"
)

type RegularTarBallComposer struct {
	tarBallQueue  *TarBallQueue
	tarFilePacker *TarBallFilePacker
	crypter       crypto.Crypter
	files         *RegularBundleFileList
	tarFileSets   TarFileSets
}

func NewRegularTarBallComposer(
	tarBallQueue *TarBallQueue,
	tarBallFilePacker *TarBallFilePacker,
	files *RegularBundleFileList,
	crypter crypto.Crypter,
) *RegularTarBallComposer {
	return &RegularTarBallComposer{
		tarBallQueue:  tarBallQueue,
		tarFilePacker: tarBallFilePacker,
		crypter:       crypter,
		files:         files,
		tarFileSets:   make(TarFileSets),
	}
}

func (c *RegularTarBallComposer) AddFile(info *ComposeFileInfo) {
	tarBall := c.tarBallQueue.Deque()
	tarBall.SetUp(c.crypter)
	c.tarFileSets[tarBall.Name()] = append(c.tarFileSets[tarBall.Name()], info.header.Name)
	go func() {
		// TODO: Refactor this functional mess
		// And maybe do a better error handling
		err := c.tarFilePacker.PackFileIntoTar(info, tarBall)
		if err != nil {
			panic(err)
		}
		err = c.tarBallQueue.CheckSizeAndEnqueueBack(tarBall)
		if err != nil {
			panic(err)
		}
	}()
}

func (c *RegularTarBallComposer) AddHeader(fileInfoHeader *tar.Header, info os.FileInfo) {
	tarBall := c.tarBallQueue.Deque()
	tarBall.SetUp(c.crypter)
	defer c.tarBallQueue.EnqueueBack(tarBall)
	c.tarFileSets[tarBall.Name()] = append(c.tarFileSets[tarBall.Name()], fileInfoHeader.Name)
	err := tarBall.TarWriter().WriteHeader(fileInfoHeader)
	if err != nil {
		panic(err)
	}
	c.files.AddFile(fileInfoHeader, info, false)
}

func (c *RegularTarBallComposer) SkipFile(tarHeader *tar.Header, fileInfo os.FileInfo) {
	c.files.AddSkippedFile(tarHeader, fileInfo)
}

func (c *RegularTarBallComposer) PackTarballs() (TarFileSets, error) {
	return c.tarFileSets, nil
}

func (c *RegularTarBallComposer) GetFiles() BundleFileList {
	return c.files
}