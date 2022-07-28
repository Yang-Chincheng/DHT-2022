package app

import (
	"crypto/sha1"
	"fmt"
	"time"

	"github.com/fatih/color"
)

var (
	Cprim  = color.New(color.FgBlue)
	Cseco  = color.New(color.FgBlack)
	Cinfo  = color.New(color.FgCyan)
	Csucc  = color.New(color.FgGreen)
	Cwarn  = color.New(color.FgRed)
	Cdark  = color.New(color.FgBlack)
	Clight = color.New(color.FgWhite)
	Cmute  = color.New(color.FgHiBlue)

	printPrim  = color.New(color.FgBlue).PrintfFunc()
	printInfo  = color.New(color.FgCyan).PrintfFunc()
	printSucc  = color.New(color.FgGreen).PrintfFunc()
	printWarn  = color.New(color.FgRed).PrintfFunc()
	printDark  = color.New(color.FgBlack).PrintfFunc()
	printLight = color.New(color.FgWhite).PrintfFunc()
	printMuted = color.New(color.FgHiBlue).PrintfFunc()
)

const (
	NIL = ""

	SHA1Len    = 20
	SHA1StrLen = 40

	DefaultTorrentPath  = "torrent/"
	DefaultUploadPath   = "upload/"
	DefaultDownloadPath = "download/"
	DefaultMusicPath    = "music/"

	DefaultFileName  = "file"
	DefaultMusicName = "music"
	DefaultAlbumName = "Default Album"

	SongDelim = '$'

	PieceSize       = 1048576 //1MB
	WorkQueueBuffer = 1024

	AfterLoginSleep = time.Second
	AfterQuitSleep  = time.Second

	UploadTimeout   = time.Second
	DownloadTimeout = time.Second

	RetryTimes = 3

	TickerInterval = 2 * time.Second

	UploadInterval        = 100 * time.Millisecond
	DownloadInterval      = 100 * time.Millisecond
	DownloadWriteInterval = time.Second
	UploadFileInterval    = time.Second
)

func minInt(x, y int) int {
	if x < y {
		return x
	} else {
		return y
	}
}

func Btoa(x interface{}) string {
	switch v := x.(type) {
	case byte:
		return fmt.Sprint(v)
	case []byte:
		return fmt.Sprint(v)
	default:
		return NIL
	}
}

func Itoa(x int) string {
	return fmt.Sprint(x)
}

func PiecesHash(piece DataPiece, attach string) HashPiece {
	piece = append(piece, []byte(attach)...)
	return sha1.Sum(piece)
}

func MakeMagnet(infoHash string) string {
	return "magnet:?xt=urn:btih:" + infoHash
}
