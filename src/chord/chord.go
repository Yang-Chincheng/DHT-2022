package chord

type ChordNode struct {
	base *chordBaseNode
}

func (n *ChordNode) Initialize(addr string) {
	n.base = new(chordBaseNode)
	n.base.initialize(addr)
}

func (n *ChordNode) Run() {
	n.base.launch()
}

func (n *ChordNode) Create() {
	n.base.create()
}

func (n *ChordNode) Join(addr string) bool {
	return n.base.join(addr)
}

func (n *ChordNode) Quit() {
	n.base.quit()
}

func (n *ChordNode) ForceQuit() {
	n.base.forceQuit()
}

func (n *ChordNode) Ping(addr string) bool {
	return n.base.ping(addr)
}

func (n *ChordNode) Put(key, value string) bool {
	return n.base.put(key, value)
}

func (n *ChordNode) Get(key string) (bool, string) {
	return n.base.get(key)
}

func (n *ChordNode) Delete(key string) bool {
	return n.base.del(key)
}

// func (n *ChordNode) Print(key string) {
// 	n.base.print(key)
// }
