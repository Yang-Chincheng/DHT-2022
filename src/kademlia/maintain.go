package kademlia

import (
	"time"
)

// transfer data in REPLICATE storage to the SENDER,
// if it's a newly recieved contact
//
// used for transfering data to newly joined nodes
func (k *kademliaImpl) TransferDataToNewNodes(sender Contact) {
	if _, v := k.router.FindBucket(sender.ID); v == nil {
		ch := make(chan bool, Alpha)
		k.replicate.ForEachKeyValue(
			func(key KeyType, val ValueType) {
				mindis := k.router.GetClosestDistance(hash(key))
				if Distance(hash(key), hash(k.addr)).Cmp(mindis) < 0 {
					go func() {
						ch <- true
						k.proto.rpcStore(k.router.host, key, val, false, 0)
						<-ch
					}()
				}
			},
		)
	}
}

// transfer a (key, value) data pair to nodes that closer to hash(KEY),
// enable node lookup by setting ENABLELOOKUP to true
//
// used for spreading data to the right nodes for them
func (k *kademliaImpl) TransferDataToCloserNodes(key KeyType, val ValueType, enableLookup bool) {
	_, b := k.router.FindBucket(hash(key))
	var contacts []ContWithDist
	if enableLookup && time.Now().After(b.timeStamp.Add(RefreshInterval)) {
		_, contacts, _, _ = k.Lookup(key, hash(key), k.proto.rpcFindNode)
	} else {
		contacts = k.router.GetClosestContacts(hash(key), K)
	}
	ch := make(chan bool, Alpha)
	for _, v := range contacts {
		go func(c Contact) {
			ch <- true
			k.proto.rpcStore(c, key, val, false, 0)
			<-ch
		}(v.Cont)
	}
}

func (b *bucketList) RefreshBucket() {
	ch := make(chan bool, Alpha)
	b.ForEachBucket(func(kb *kBucket) {
		if time.Now().After(kb.timeStamp.Add(RefreshInterval)) {
			kb.Touch()
			go func() {
				ch <- true
				randID := RandomID(kb.bucketRange)
				temp := kb.CopyContact()
				for _, c := range temp {
					ret, _ := b.proto.rpcFindNode(c, NIL, randID)
					for _, v := range ret.Cont {
						b.AddContact(v.Cont)
					}
				}
				<-ch
			}()
		}
	})
}

func (s *storage) RepublishData(republishFunc func(KeyType, ValueType)) {
	now := time.Now()
	s.lock.RLock()
	tmp := []KeyType{}
	for k, v := range s.store {
		if now.After(v.repubTimeStamp.Add(RepublishInterval)) {
			republishFunc(k, v.value)
			tmp = append(tmp, k)
		}
	}
	s.lock.RUnlock()
	for _, k := range tmp {
		s.Touch(k)
	}
}

func (s *storage) ExpireData() {
	now := time.Now()
	s.lock.RLock()
	tmp := []KeyType{}
	for k, v := range s.store {
		if now.After(v.repubTimeStamp.Add(v.expireDura)) {
			tmp = append(tmp, k)
		}
	}
	s.lock.RUnlock()
	for _, k := range tmp {
		s.Remove(k)
	}
}

func (k *kademliaImpl) maintain() {
	go func() {
		ticker := time.NewTicker(RefreshInterval)
		defer ticker.Stop()
		for {
			select {
			case <-k.quitSignal:
				return
			case <-ticker.C:
				k.router.RefreshBucket()
			}
		}
	}()

	go func() {
		ticker := time.NewTicker(RepublishInterval)
		defer ticker.Stop()
		repubFunc := func(kt KeyType, vt ValueType) {
			k.TransferDataToCloserNodes(kt, vt, false)
		}
		for {
			select {
			case <-k.quitSignal:
				return
			case <-ticker.C:
				k.origin.RepublishData(repubFunc)
			}
		}
	}()

	go func() {
		ticker := time.NewTicker(RepublishInterval)
		defer ticker.Stop()
		repubFunc := func(kt KeyType, vt ValueType) {
			k.TransferDataToCloserNodes(kt, vt, true)
		}
		for {
			select {
			case <-k.quitSignal:
				return
			case <-ticker.C:
				k.replicate.RepublishData(repubFunc)
			}
		}
	}()

	go func() {
		ticker := time.NewTicker(ExpireTime)
		defer ticker.Stop()
		for {
			select {
			case <-k.quitSignal:
				return
			case <-ticker.C:
				k.replicate.ExpireData()
			}
		}
	}()

	go func() {
		ticker := time.NewTicker(ExpireTime)
		defer ticker.Stop()
		for {
			select {
			case <-k.quitSignal:
				return
			case <-ticker.C:
				k.cache.ExpireData()
			}
		}
	}()

	// go func() {
	// 	ticker := time.NewTicker(5 * time.Second)
	// 	for {
	// 		<-ticker.C
	// 		// k.router.Print()
	// 	}
	// }()
}
