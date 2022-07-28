package kademlia

type KademliaNode struct {
	impl *kademliaImpl
}

func NewKademliaNode(addr Address) *KademliaNode {
	ret := &KademliaNode{new(kademliaImpl)}
	ret.impl.initialize(addr)
	return ret
}

func (k *KademliaNode) Run() {
	k.impl.launch()
}

func (k *KademliaNode) Create() {
	return
}

func (k *KademliaNode) Join(addr Address) bool {
	if k.impl.online {
		logger(k.impl.addr).Warn("node have joined")
		return false
	}
	if !k.impl.ping(addr) {
		logger(k.impl.addr).Warn("invalid bootstrapping node")
		return false
	}
	k.impl.router.AddContact(*NewContact(addr))
	k.impl.iterativeFindNode(k.impl.addr)
	k.impl.maintain()
	return true
}

func (k *KademliaNode) Quit() {
	if !k.impl.online {
		logger(k.impl.addr).Warn("node have quitted")
		return
	}
	k.impl.shutdown()
	k.impl.reset()
}

func (k *KademliaNode) ForceQuit() {
	k.impl.shutdown()
	k.impl.reset()
}

func (k *KademliaNode) Ping(addr Address) bool {
	return k.impl.ping(addr)
}

func (k *KademliaNode) Put(key KeyType, value ValueType) bool {
	k.impl.iterativeStore(key, value)
	return true
}

func (k *KademliaNode) Get(key KeyType) (bool, ValueType) {
	ok, value := k.impl.iterativeFindValue(key)
	return ok, value
}

func (k *KademliaNode) Delete(key KeyType) bool {
	// under kademlia protocol, the DELETE operation is ill-supported
	// here just leave it out for simplicity
	return true
}
