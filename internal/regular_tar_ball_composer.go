package internal

import (
	"archive/tar"
	"github.com/wal-g/wal-g/internal/crypto"
	"os"
)

type RegularTarBallComposer struct {
	tarBallQueue *TarBallQueue
	tarFilePacker *TarBallFilePacker
	crypter crypto.Crypter
	Files     *RegularSentinelFileList
	tarFileSets map[string][]string
}

func NewRegularTarBallComposer(
	incrementBaseLsn *uint64, deltaMap PagedFileDeltaMap, tarBallQueue *TarBallQueue,
	crypter crypto.Crypter) *RegularTarBallComposer {
	filesList := &RegularSentinelFileList{}
	return &RegularTarBallComposer{
		tarBallQueue:  tarBallQueue,
		tarFilePacker: newTarBallFilePacker(deltaMap, incrementBaseLsn, filesList),
		crypter:       crypter,
		Files:         filesList,
		tarFileSets:   make(map[string][]string),
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
	c.Files.AddFile(fileInfoHeader, info, false)
}

func (c *RegularTarBallComposer) SkipFile(tarHeader *tar.Header, fileInfo os.FileInfo) {
	c.Files.AddSkippedFile(tarHeader, fileInfo)
}

func (c *RegularTarBallComposer) PackTarballs() (map[string][]string, error) {
	return c.tarFileSets, nil
}

func (c *RegularTarBallComposer) GetFiles() SentinelFileList {
	return c.Files
}