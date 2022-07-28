package kademlia

import (
	"container/heap"
	"math"
	"sort"
	"sync/atomic"
	"time"
)

type kademliaImpl struct {
	networkNode
	router    *bucketList
	origin    *storage
	replicate *storage
	cache     *storage
}

type LookupRet struct {
	Found   bool
	FoundBy Contact
	Cont    []ContWithDist
	Value   ValueType
}

type LookupRpc func(Contact, KeyType, Identifer) (LookupRet, error)

func (k *kademliaImpl) initialize(address Address) {
	k.addr = address
	k.proto = &protocol{k}
	k.online = false
	k.quitSignal = make(chan bool)
	k.router = NewBucketList(address, k.proto)
	k.origin = NewStorage()
	k.cache = NewStorage()
	k.replicate = NewStorage()
}

func (k *kademliaImpl) reset() {
	k.quitSignal = make(chan bool)
	k.router = NewBucketList(k.addr, k.proto)
	k.origin = NewStorage()
	k.cache = NewStorage()
	k.replicate = NewStorage()
}

func (k *kademliaImpl) Lookup(key KeyType, id Identifer, rpcFunc LookupRpc) (bool, []ContWithDist, ValueType, error) {
	ch := make(chan LookupRet, Alpha)
	visit := make(map[Address]bool)
	pending := new(ContactHeap)
	retList := []ContWithDist{}

	visit[k.addr] = true
	initial := k.router.GetClosestContacts(id, K)
	*pending = append(*pending, initial...)
	heap.Init(pending)

	var inFlight int32 = 0
	sendRpcUpto := func(alpha int) {
		for pending.Len() > 0 && int(atomic.LoadInt32(&inFlight)) < alpha {
			c := heap.Pop(pending).(ContWithDist).Cont
			if _, ok := visit[c.Addr]; !ok {
				visit[c.Addr] = true
				atomic.AddInt32(&inFlight, 1)
				go func() {
					if res, err := rpcFunc(c, key, id); err == nil {
						ch <- res
					} else {
						atomic.AddInt32(&inFlight, -1)
					}
				}()
			}
		}
	}

	sendRpcUpto(Alpha)
	for atomic.LoadInt32(&inFlight) > 0 {
		select {
		case res := <-ch:
			if res.Found {
				retCont := minInSlice(retList, res.FoundBy)
				return true, []ContWithDist{retCont}, res.Value, nil
			}
			for _, v := range res.Cont {
				if _, ok := visit[v.Cont.Addr]; !ok {
					retList = append(retList, v)
					heap.Push(pending, v)
				}
			}
		case <-time.After(LookupTimeOut):
			break
		}
		atomic.AddInt32(&inFlight, -1)
		sendRpcUpto(Alpha)
	}
	// tch <- true

	sort.Slice(retList, func(i, j int) bool {
		return retList[i].Dist.Cmp(retList[j].Dist) < 0
	})
	retList = retList[:minInt(K, len(retList))]
	return false, retList, NIL, nil
}

// respond to STORE RPCs
func (k *kademliaImpl) primitiveStore(sender Contact, key KeyType, val ValueType, cached bool, expireTime time.Duration) {
	if cached {
		k.cache.Put(key, val, expireTime)
	} else {
		k.TransferDataToNewNodes(sender)
		k.replicate.Put(key, val, ExpireTime)
	}
	k.router.AddContact(sender)
}

// respond to FIND_NODE RPCs
func (k *kademliaImpl) primitiveFindNode(sender Contact, id Identifer) []ContWithDist {
	k.TransferDataToNewNodes(sender)
	// fmt.Printf("ADDING sender %s to %s\n", sender.Addr, k.addr)
	k.router.AddContact(sender)
	return k.router.GetClosestContacts(id, K)
}

// respond to FIND_VALUE RPCs
func (k *kademliaImpl) primitiveFindValue(sender Contact, key KeyType) (bool, Contact, []ContWithDist, ValueType) {
	k.TransferDataToNewNodes(sender)
	k.router.AddContact(sender)
	if v, ok := k.replicate.Get(key); ok {
		return true, k.router.host, []ContWithDist{}, v
	} else if v, ok := k.cache.Get(key); ok {
		return true, k.router.host, []ContWithDist{}, v
	} else {
		return false, Contact{}, k.router.GetClosestContacts(hash(key), K), NIL
	}
}

// store data in ORIGINATOR storage and spread it
func (k *kademliaImpl) iterativeStore(key KeyType, val ValueType) {
	k.router.Touch(hash(key))
	k.origin.Put(key, val, 0)
	k.TransferDataToCloserNodes(key, val, true)
}

// start an iterative lookup process for nodes
func (k *kademliaImpl) iterativeFindNode(addr Address) []ContWithDist {
	k.router.Touch(hash(addr))
	_, contacts, _, _ := k.proto.node.Lookup(NIL, hash(addr), k.proto.rpcFindNode)
	return contacts
}

// start an iterative lookup process for a value
func (k *kademliaImpl) iterativeFindValue(key KeyType) (bool, ValueType) {
	k.router.Touch(hash(key))
	if v, ok := k.origin.Get(key); ok {
		return true, v
	} else if v, ok := k.replicate.Get(key); ok {
		return true, v
	} else if v, ok := k.cache.Get(key); ok {
		return true, v
	} else {
		found, contacts, val, _ := k.Lookup(key, hash(key), k.proto.rpcFindValue)
		if found {
			// if val == NIL {
			// 	panic("invalid value after lookup")
			// }
			if target := contacts[0].Cont; target.Addr != NIL {
				// cache data to the cloest node that doesnt have it
				idx1 := k.router.ContactIndex(target)
				idx2 := k.router.ContactIndex(k.router.host)
				sepNum := minInt(absInt(idx1-idx2), 20)
				expireTime := ExpireTime / time.Duration(math.Pow(2, float64(sepNum)))
				k.proto.rpcStore(target, key, val, true, expireTime)
			}
			return true, val
		}
		return false, NIL
	}
}
