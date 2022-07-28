package kademlia

import "time"

type protocol struct {
	node *kademliaImpl
}

type RpcHeader struct {
	Sender Contact
}

type PingRequst struct {
	RpcHeader
}
type PingReply struct {
	RpcHeader
}

func (p *protocol) rpcPing(c Contact) bool {
	request := PingRequst{
		RpcHeader: RpcHeader{Sender: p.node.router.host},
	}
	reply := new(PingReply)
	err := p.node.call(c.Addr, "KademliaService", "HandlePing", request, reply)
	return err != nil
}

func (p *protocol) HandlePing(request PingRequst, reply *PingReply) error {
	*reply = PingReply{
		RpcHeader: RpcHeader{Sender: p.node.router.host},
	}
	p.node.TransferDataToNewNodes(request.Sender)
	p.node.router.AddContact(request.Sender)
	return nil
}

type FindNodeRequest struct {
	RpcHeader
	ID Identifer
}
type FindNodeReply struct {
	LookupRet
}

func (p *protocol) rpcFindNode(c Contact, _ KeyType, id Identifer) (LookupRet, error) {
	request := FindNodeRequest{
		RpcHeader: RpcHeader{Sender: p.node.router.host},
		ID:        id,
	}
	reply := new(FindNodeReply)
	err := p.node.call(c.Addr, "KademliaService", "HandleFindNode", request, reply)
	return reply.LookupRet, err
}

func (p *protocol) HandleFindNode(request FindNodeRequest, reply *FindNodeReply) error {
	reply.Found, reply.FoundBy, reply.Cont, reply.Value =
		false, Contact{}, p.node.primitiveFindNode(request.Sender, request.ID), NIL
	return nil
}

type FindValueRequest struct {
	RpcHeader
	Key KeyType
}
type FindValueReply struct {
	LookupRet
}

func (p *protocol) rpcFindValue(c Contact, key string, _ Identifer) (LookupRet, error) {
	request := FindValueRequest{
		RpcHeader: RpcHeader{Sender: p.node.router.host},
		Key:       key,
	}
	reply := new(FindValueReply)
	err := p.node.call(c.Addr, "KademliaService", "HandleFindValue", request, reply)
	return reply.LookupRet, err
}

func (p *protocol) HandleFindValue(request FindValueRequest, reply *FindValueReply) error {
	reply.Found, reply.FoundBy, reply.Cont, reply.Value =
		p.node.primitiveFindValue(request.Sender, request.Key)
	return nil
}

type StoreRequest struct {
	RpcHeader
	Key        KeyType
	Val        ValueType
	Cached     bool
	ExpireTime time.Duration
}
type StoreReply struct{}

func (p *protocol) rpcStore(c Contact, key KeyType, value ValueType, cached bool, expire time.Duration) error {
	if value == NIL {
		panic("rpc store invalid value")
	}
	request := StoreRequest{
		RpcHeader:  RpcHeader{Sender: p.node.router.host},
		Key:        key,
		Val:        value,
		Cached:     cached,
		ExpireTime: expire,
	}
	reply := new(StoreReply)
	err := p.node.call(c.Addr, "KademliaService", "HandleStore", request, reply)
	return err
}

func (p *protocol) HandleStore(request StoreRequest, reply *StoreReply) error {
	p.node.primitiveStore(
		request.Sender,
		request.Key, request.Val,
		request.Cached,
		request.ExpireTime,
	)
	return nil
}
