package app

import (
	"time"
)

type pieceWork struct {
	index   int
	success bool
	retry   int
	result  DataPiece
}

func (p *Peer) attemptUploadPiece(key string, val string, idx int, retry int, workQueue *chan pieceWork) {
	ok := p.node.Put(key, val)
	if ok {
		*workQueue <- pieceWork{index: idx, success: true, retry: retry}
		time.Sleep(UploadInterval)
	} else {
		*workQueue <- pieceWork{index: idx, success: false, retry: retry}
	}
}

func (this *Peer) attemptDownloadPiece(key string, idx int, retry int, workQueue *chan pieceWork) {
	ok, val := this.node.Get(key)
	if ok {
		*workQueue <- pieceWork{index: idx, success: true, result: []byte(val)}
	} else {
		*workQueue <- pieceWork{index: idx, success: false}
	}
}

func (p *Peer) Upload(keyPack *KeyPackage, filePack *FilePackage) bool {
	if keyPack.size != filePack.size {
		printWarn("Upload failed, key and file package don't match.\n")
		return false
	}

	workQueue := make(chan pieceWork, WorkQueueBuffer)

	for i := 0; i < keyPack.size; i++ {
		go p.attemptUploadPiece(
			keyPack.GetPiece(i),
			filePack.GetPiece(i),
			i, 0, &workQueue,
		)
	}

	done := make(chan bool)
	go func() {
		ticker := time.NewTicker(TickerInterval)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				printInfo("uploading...\n")
			}
		}
	}()

	tot := 0
	for tot != keyPack.size {
		select {
		case piece := <-workQueue:
			idx := piece.index
			if piece.success {
				tot++
				printInfo("finish upload piece %d/%d\n", idx+1, keyPack.size)
				time.Sleep(UploadInterval)
			} else {
				printWarn("fail to upload piece %d/%d\n", idx+1, keyPack.size)
				if piece.retry > RetryTimes {
					printWarn("upload piece %d/%d\n failed.\n", idx, keyPack.size)
				} else {
					printInfo("retrying...")
					go p.attemptUploadPiece(
						keyPack.GetPiece(idx),
						filePack.GetPiece(idx),
						idx, piece.retry+1, &workQueue,
					)
					time.Sleep(UploadInterval)
				}
			}
		case <-time.After(UploadTimeout):
			printWarn("Upload file time out.\n")
			return false
		}
	}

	printSucc("Upload file network succeeded.\n")
	return true
}

func (p *Peer) DownLoad(keyPack *KeyPackage) (bool, []byte) {
	ret := make([]byte, keyPack.len)
	workQueue := make(chan pieceWork, WorkQueueBuffer)
	for i := 0; i < keyPack.size; i++ {
		go p.attemptDownloadPiece(
			keyPack.GetPiece(i),
			i, 0, &workQueue,
		)
	}

	checker := make([]HashPiece, keyPack.size)

	tot := 0
	for tot != keyPack.size {
		select {
		case piece := <-workQueue:
			idx := piece.index
			if piece.success {
				tot++
				chunkLeft := idx * PieceSize
				chunkRigh := minInt(chunkLeft+PieceSize, keyPack.len)
				copy(ret[chunkLeft:chunkRigh], piece.result)
				pieceHash := PiecesHash(piece.result, "_"+Itoa(piece.index))
				checker[idx] = pieceHash
				printInfo("finish download piece %d/%d\n", piece.index+1)
				time.Sleep(DownloadInterval)
			} else {
				printWarn("fail to download piece %d/%d\n", piece.index+1)
				time.Sleep(DownloadInterval)
				if piece.retry > RetryTimes {
					printWarn("Download file failed.\n")
					return false, DataPiece{}
				} else {
					printInfo("retrying...\n")
					go p.attemptDownloadPiece(
						keyPack.GetPiece(idx),
						idx, piece.retry+1, &workQueue,
					)
				}
			}
		case <-time.After(DownloadTimeout):
			printWarn("Download file failed, time out.\n")
			return false, DataPiece{}
		}
	}

	printSucc("Download file succeeded.\n")
	printInfo("Checking file integrity...\n")

	var checkerPieces string
	for i := 0; i < keyPack.size; i++ {
		checkerPieces += Btoa(checker[i])
	}
	buf := []byte(checkerPieces)
	for i := 0; i < keyPack.size; i++ {
		copy(checker[i][:], buf[i*40:(i+1)*40])
		if checker[i] != keyPack.key[i] {
			printSucc("Intergrity checking failed.\n")
			return false, []byte{}
		}
	}

	printSucc("Intergrity checking succeeded.\n")
	return true, ret
}
