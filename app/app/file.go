package app

import (
	"errors"
	"io"
	"os"
)

type HashPiece = [20]byte
type DataPiece = []byte

type KeyPackage struct {
	size int
	len  int
	hash HashPiece
	key  []HashPiece
}

func (p *KeyPackage) GetPiece(idx int) string {
	var ret HashPiece
	for i := 0; i < 20; i++ {
		ret[i] = p.key[idx][i] ^ p.hash[i]
	}
	return Btoa(ret)
}

type FilePackage struct {
	size int
	data []DataPiece
}

func (p *FilePackage) GetPiece(idx int) string {
	return string(p.data[idx])
}

func SaveFile(path, name string, ctx []byte) error {
	file, err := os.Create(path + name)
	if err != nil {
		return errors.New("Create File Failed.")
	} else {
		file.Write(ctx)
		return nil
	}
}

func UploadFileProcessing(name, path, seed string) (*KeyPackage, *FilePackage, string, string) {
	filePack, length := PackFile(path)

	Cinfo.Println("Data Packaged Finish. Total Piece: ", filePack.size)

	var pieces string
	for i := 0; i < filePack.size; i++ {
		pieces += Btoa(PiecesHash(filePack.data[i], "_"+Itoa(i)))
	}

	torrent := bencodeTorrent{
		Announce: "We have a new torrent here.",
		Info: bencodeInfo{
			Length:      length,
			Pieces:      pieces,
			PieceLength: PieceSize,
			Name:        name,
		},
	}

	keyPack, magnet := torrent.Generate()

	Cinfo.Println("Torrent Resolved Finish: ")
	torrent.Info.Display()

	err := SaveFile(seed, name+".magnet", []byte(magnet))
	if err != nil {
		printWarn("Save magnet url failed.")
	}

	ctx, err := torrent.Save(seed, name+".torrent")
	if err != nil {
		printWarn("Save torrent failed.")
	}

	return keyPack, filePack, magnet, ctx
}

func DownloadFileProcessing(seed, name string) (*KeyPackage, string) {
	var torrent bencodeTorrent
	err := torrent.Load(seed, name)
	if err != nil {
		printWarn("Torrent Open Failed in path: ", seed, err.Error())
		return nil, NIL
	}

	keyPack, _ := torrent.Generate()

	Cinfo.Println("Torrent Resolved Finish: ")
	torrent.Info.Display()

	return keyPack, torrent.Info.Name
}

func PackFile(path string) (*FilePackage, int) {
	file, err := os.Open(path)
	length := 0
	ret := new(FilePackage)
	if err != nil {
		Cwarn.Println("File Open Failed in path: ", path, err.Error())
		return nil, 0
	}
	for {
		buf := make([]byte, PieceSize)
		bufSize, err := file.Read(buf)
		if err != nil && err != io.EOF {
			Cwarn.Println("File Read Error.")
			return nil, 0
		}
		if bufSize == 0 {
			break
		}
		ret.size++
		length += bufSize
		ret.data = append(ret.data, buf[:bufSize])
	}
	return ret, length
}
