package app

import (
	"bytes"
	"fmt"
	"os"
	"time"

	"github.com/jackpal/bencode-go"
)

var user Peer
var port int
var bootstrapping string

func Index() {
	printPrim("Welcome to nFDM.\n")

	printMuted("May I have your PORT and BOOSTRAPPING ADDRESS?")
	fmt.Scanln(&port, &bootstrapping)
	user.Login(port, bootstrapping)

	for {
		printPrim("try free for p2p file sharing.\n")
		printMuted("type 'help' for more information.\n")

		var cmd, fp, sp, fn, sn, mg string
		var args [3]string
		fmt.Scanln(&cmd, &args[0], &args[1], &args[2])

		for i := 0; i < 3; i++ {
			switch args[i][1:3] {
			case "fp":
				fp = args[i][4:]
			case "sp":
				sp = args[i][4:]
			case "sn":
				sn = args[i][4:]
			case "fn":
				fn = args[i][4:]
			case "mg":
				mg = args[i][4:]
			}
		}

		switch cmd {
		case "help":
			Help()
		case "quit":
			user.Logout()
		case "upload":
			if fp == "" {
				fp = DefaultUploadPath
			}
			if sp == "" {
				sp = DefaultTorrentPath
			}
			var fileList []string
			if fn == "" {
				dir, err := os.Open(fp)
				if err != nil {
					Cwarn.Println("Open File Directory Error!")
				} else {
					list, _ := dir.Readdir(-1)
					for _, file := range list {
						fileList = append(fileList, file.Name())
					}
				}
			} else {
				fileList = append(fileList, fn)
			}
			for _, fileName := range fileList {
				Cinfo.Println("\nStart Upload File: ", fileName)
				keyPackage, dataPackage, magnet, torrentStr := UploadFileProcessing(fp+fileName, fileName, sp)
				ok := user.Upload(&keyPackage, &dataPackage)
				if ok {
					Cinfo.Println("Finish Upload and create torrent to: ", sp+fileName+".torrent")
					user.node.Put(magnet, torrentStr)
					Cinfo.Println("Magnet URL: ", magnet, " saved to: ", sp+fileName+"-magnet.txt")
				}
				time.Sleep(UploadFileInterval)
			}

		case "download":
			if fp == "" {
				fp = DefaultDownloadPath
			}
			if sp == "" {
				sp = DefaultTorrentPath
			}
			if sn == "" {
				sn = DefaultFileName + ".torrent"
			}
			if mg != "" {
				ok, torrentStr := user.node.Get(mg)
				if ok {
					reader := bytes.NewBufferString(torrentStr)
					torrent := bencodeTorrent{}
					err := bencode.Unmarshal(reader, &torrent)
					if err != nil {
						Cwarn.Println("Failed to analysis Magnet URL: torrent broken.")
					} else {
						torrent.Save(sp + sn)
						Csucc.Println("Magnet to Torrent Success! saved to: ", sp+sn)
					}
				} else {
					Cwarn.Println("Failed to analysis Magnet URL: torrent not founded.")
				}
			}
			keyPackage, fileName := DownloadFileProcessing(sp + sn)

			fileIO, err := os.Create(fp + fileName)

			if err != nil {
				Cwarn.Println("File Path invalid in ", fp)
				continue
			}
			ok, data := user.DownLoad(&keyPackage)
			if ok {
				fileIO.Write(data)
				time.Sleep(DownloadWriteInterval)
				Cinfo.Println("Finish Download to: ", fp+fileName)
			}
		}
	}
}
