package app

import (
	"DHT-app/src/dht"
	"os"
)

type Peer struct {
	addr string
	node dht.DhtNode
}

func Help() {

}

func (p *Peer) Login(port int, bootstrapping string) {
	os.Mkdir("upload", os.ModePerm)
	os.Mkdir("download", os.ModePerm)
	os.Mkdir("torrent", os.ModePerm)

	p.node = dht.NewNode(port)
	go p.node.Run()

	if bootstrapping == NIL {
		p.node.Create()
		printSucc("Create network succeeded.\n")
	} else {
		ok := p.node.Join(bootstrapping)
		if ok {
			printSucc("Join network succeeded.\n")
		} else {
			printWarn("Join network failed.\n")
			return
		}
	}
	Cinfo.Println("Login Succeeded.\n")
}

func (p *Peer) Logout() {
	p.node.Quit()
	printPrim("Bye.\n")
}
