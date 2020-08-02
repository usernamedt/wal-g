package internal

import (
	"github.com/pkg/errors"
	"sync"
)

type TarBallQueue struct {
	TarSizeThreshold int64

	tarballQueue       chan TarBall
	uploadQueue        chan TarBall
	LastCreatedTarball TarBall
	parallelTarballs   int
	maxUploadQueue     int
	mutex              sync.Mutex
	started            bool

	TarBallMaker TarBallMaker
}

func newTarBallQueue(tarSizeThreshold int64, tarBallMaker TarBallMaker) *TarBallQueue {
	return &TarBallQueue{
		TarSizeThreshold: tarSizeThreshold,
		TarBallMaker:     tarBallMaker,
	}
}

func (tarQueue *TarBallQueue) StartQueue() error {
	if tarQueue.started {
		panic("Trying to start already started Queue")
	}
	var err error
	tarQueue.parallelTarballs, err = getMaxUploadDiskConcurrency()
	if err != nil {
		return err
	}
	tarQueue.maxUploadQueue, err = getMaxUploadQueue()
	if err != nil {
		return err
	}

	tarQueue.tarballQueue = make(chan TarBall, tarQueue.parallelTarballs)
	tarQueue.uploadQueue = make(chan TarBall, tarQueue.parallelTarballs+tarQueue.maxUploadQueue)
	for i := 0; i < tarQueue.parallelTarballs; i++ {
		tarQueue.NewTarBall(true)
		tarQueue.tarballQueue <- tarQueue.LastCreatedTarball
	}

	tarQueue.started = true
	return nil
}

func (tarQueue *TarBallQueue) Deque() TarBall {
	if !tarQueue.started {
		panic("Trying to deque from not started Queue")
	}
	return <-tarQueue.tarballQueue
}

func (tarQueue *TarBallQueue) FinishQueue() error {
	if !tarQueue.started {
		panic("Trying to stop not started Queue")
	}
	tarQueue.started = false

	// We have to deque exactly this count of workers
	for i := 0; i < tarQueue.parallelTarballs; i++ {
		tarBall := <-tarQueue.tarballQueue
		if tarBall.TarWriter() == nil {
			// This had written nothing
			continue
		}
		err := tarBall.CloseTar()
		if err != nil {
			return errors.Wrap(err, "HandleWalkedFSObject: failed to close tarball")
		}
		tarBall.AwaitUploads()
	}

	// At this point no new tarballs should be put into uploadQueue
	for len(tarQueue.uploadQueue) > 0 {
		select {
		case otb := <-tarQueue.uploadQueue:
			otb.AwaitUploads()
		default:
		}
	}

	return nil
}

func (tarQueue *TarBallQueue) EnqueueBack(tarBall TarBall) {
	tarQueue.tarballQueue <- tarBall
}

func (tarQueue *TarBallQueue) CheckSizeAndEnqueueBack(tarBall TarBall) error {
	if tarBall.Size() > tarQueue.TarSizeThreshold {
		tarQueue.mutex.Lock()
		defer tarQueue.mutex.Unlock()

		err := tarBall.CloseTar()
		if err != nil {
			return errors.Wrap(err, "HandleWalkedFSObject: failed to close tarball")
		}

		tarQueue.uploadQueue <- tarBall
		for len(tarQueue.uploadQueue) > tarQueue.maxUploadQueue {
			select {
			case otb := <-tarQueue.uploadQueue:
				otb.AwaitUploads()
			default:
			}
		}

		tarQueue.NewTarBall(true)
		tarBall = tarQueue.LastCreatedTarball
	}
	tarQueue.tarballQueue <- tarBall
	return nil
}

func (tarQueue *TarBallQueue) FinishTarBall(tarBall TarBall) error {
	tarQueue.mutex.Lock()
	defer tarQueue.mutex.Unlock()
	err := tarBall.CloseTar()
	if err != nil {
		return errors.Wrap(err, "FinishTarBall: failed to close tarball")
	}

	tarQueue.uploadQueue <- tarBall
	for len(tarQueue.uploadQueue) > tarQueue.maxUploadQueue {
		select {
		case otb := <-tarQueue.uploadQueue:
			otb.AwaitUploads()
		default:
		}
	}

	tarQueue.NewTarBall(true)
	tarQueue.tarballQueue <- tarQueue.LastCreatedTarball
	return nil
}

// NewTarBall starts writing new tarball
func (tarQueue *TarBallQueue) NewTarBall(dedicatedUploader bool) TarBall {
	tarQueue.LastCreatedTarball = tarQueue.TarBallMaker.Make(dedicatedUploader)
	return tarQueue.LastCreatedTarball
}
