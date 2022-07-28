package app

import (
	"bytes"
	"crypto/sha1"
	"os"

	"github.com/jackpal/bencode-go"
)

//credit to https://github.com/veggiedefender/torrent-client/blob/master/torrentfile/torrentfile.go

type bencodeInfo struct {
	Pieces      string `bencode:"pieces"`
	PieceLength int    `bencode:"piece length"`
	Length      int    `bencode:"length"`
	Name        string `bencode:"name"`
}

func (u *bencodeInfo) Display() {
	Cinfo.Println("\n", "* FileName: ", u.Name, " Size:", u.Length, " bytes", "\n")
}

func (u *bencodeInfo) InfoHash() HashPiece {
	var buf bytes.Buffer
	bencode.Marshal(&buf, *u)
	return sha1.Sum(buf.Bytes())
}

type bencodeTorrent struct {
	Announce string      `bencode:"announce"`
	Info     bencodeInfo `bencode:"info"`
}

func (t *bencodeTorrent) Generate() (*KeyPackage, string) {
	ret := new(KeyPackage)
	buf := []byte(t.Info.Pieces)
	ret.size = len(buf) / 40
	ret.hash = t.Info.InfoHash()
	ret.len = t.Info.Length
	ret.key = make([]HashPiece, ret.size)
	for i := 0; i < ret.size; i++ {
		copy(ret.key[i][:], buf[40*i:40*(i+1)])
	}
	return ret, Btoa(ret.hash)
}

func (t *bencodeTorrent) Save(path, name string) (string, error) {
	writer := bytes.NewBufferString("")
	err := bencode.Marshal(writer, t.Info)
	if err != nil {
		return NIL, err
	}
	ctx := writer.String()
	file, err := os.Create(path + name)
	if err != nil {
		return NIL, err
	} else {
		file.Write([]byte(ctx))
		return ctx, nil
	}
}

func (t *bencodeTorrent) Load(path, name string) error {
	file, err := os.Open(path + name + ".torrent")
	if err != nil {
		return err
	}
	err = bencode.Unmarshal(file, *t)
	if err != nil {
		return err
	}
	return nil
}
