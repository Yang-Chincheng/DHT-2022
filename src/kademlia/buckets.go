package kademlia

import (
	"container/list"
	"sort"
	"sync"
	"time"
)

type Contact struct {
	Addr Address
	ID   Identifer
}

func NewContact(addr Address) *Contact {
	ret := new(Contact)
	ret.Addr = addr
	ret.ID = hash(addr)
	return ret
}

// kBucket of Kademlia's Routing Table
type kBucket struct {
	timeStamp   time.Time
	contacts    *list.List
	bucketRange IDRange

	contLock sync.RWMutex
}

func NewKBucket(initRange *IDRange) *kBucket {
	ret := new(kBucket)
	ret.contacts = list.New()
	ret.timeStamp = time.Now()
	if initRange != nil {
		ret.bucketRange = *initRange
	} else {
		ret.bucketRange = IDRange{low: NewID(0), high: IDpow2(M)}
	}
	return ret
}

func (b *kBucket) Touch() {
	b.timeStamp = time.Now()
}

func (b *kBucket) Contain(id Identifer) bool {
	return b.bucketRange.contain(id)
}

func (b *kBucket) Size() int {
	b.contLock.RLock()
	defer b.contLock.RUnlock()
	return b.contacts.Len()
}

func (b *kBucket) LeastRecent() Contact {
	b.contLock.RLock()
	defer b.contLock.RUnlock()
	return b.contacts.Front().Value.(Contact)
}

func (b *kBucket) MostRecent() Contact {
	b.contLock.RLock()
	defer b.contLock.RUnlock()
	return b.contacts.Back().Value.(Contact)
}

func (b *kBucket) CopyContact() []Contact {
	ret := make([]Contact, 0)
	b.ForEachContact(func(c Contact) {
		ret = append(ret, c)
	})
	return ret
}

func (b *kBucket) FindContact(contact Contact) (*list.Element, Contact) {
	b.contLock.RLock()
	defer b.contLock.RUnlock()
	for v := b.contacts.Front(); v != nil; v = v.Next() {
		if contact.Addr == v.Value.(Contact).Addr {
			return v, v.Value.(Contact)
		}
	}
	return nil, Contact{}
}

func (b *kBucket) ForEachContact(foo func(c Contact)) {
	b.contLock.RLock()
	defer b.contLock.RUnlock()
	for v := b.contacts.Front(); v != nil; v = v.Next() {
		foo(v.Value.(Contact))
	}
}

func (b *kBucket) AddContact(contact Contact) {
	b.contLock.Lock()
	b.contacts.PushBack(contact)
	b.contLock.Unlock()
}

func (b *kBucket) EvictContact(contact Contact) {
	if v, _ := b.FindContact(contact); v != nil {
		b.contLock.Lock()
		b.contacts.Remove(v)
		b.contLock.Unlock()
	}
}

func (b *kBucket) UpdateContact(contact Contact) {
	if v, _ := b.FindContact(contact); v != nil {
		b.contLock.Lock()
		b.contacts.MoveToBack(v)
		b.contLock.Unlock()
	}
}

func (b *kBucket) Split() (*kBucket, *kBucket) {
	mid := b.bucketRange.midpoint()
	k1 := NewKBucket(&IDRange{low: b.bucketRange.low, high: mid})
	k2 := NewKBucket(&IDRange{low: mid, high: b.bucketRange.high})
	b.ForEachContact(func(c Contact) {
		if c.ID.Cmp(mid) < 0 {
			k1.AddContact(c)
		} else {
			k2.AddContact(c)
		}
	})
	return k1, k2
}

func (b *kBucket) Depth() int {
	if b.Size() == 0 {
		return M
	} else {
		len := M
		fir := b.LeastRecent().ID
		b.ForEachContact(func(c Contact) {
			len = minInt(len, SharedPrefixLen(fir, c.ID))
		})
		return len
	}
}

// Routing Table of Kademlia Protocol
type bucketList struct {
	proto    *protocol
	host     Contact
	buckets  *list.List
	buckLock sync.RWMutex
}

func NewBucketList(addr Address, pro *protocol) *bucketList {
	ret := new(bucketList)
	ret.proto = pro
	ret.host = *NewContact(addr)
	ret.buckets = list.New()
	ret.buckets.PushBack(NewKBucket(nil))
	return ret
}

func (b *bucketList) Touch(id Identifer) {
	_, v := b.FindBucket(id)
	v.Touch()
}

func (b *bucketList) FindBucket(id Identifer) (*list.Element, *kBucket) {
	b.buckLock.RLock()
	defer b.buckLock.RUnlock()
	for v := b.buckets.Front(); v != nil; v = v.Next() {
		if v.Value.(*kBucket).Contain(id) {
			return v, v.Value.(*kBucket)
		}
	}
	return nil, nil
}

func (b *bucketList) ForEachBucket(foo func(*kBucket)) {
	b.buckLock.RLock()
	defer b.buckLock.RUnlock()
	for v := b.buckets.Front(); v != nil; v = v.Next() {
		foo(v.Value.(*kBucket))
	}
}

func (b *bucketList) AddContact(c Contact) {
	if c.Addr == b.host.Addr {
		return
	}
	ele, buck := b.FindBucket(c.ID)
	if buck != nil {
		if v, _ := buck.FindContact(c); v != nil {
			buck.UpdateContact(c)
		} else {
			if buck.Size() < K {
				buck.AddContact(c)
			} else {
				if buck.Contain(b.host.ID) || buck.Depth()%B != 0 {
					k1, k2 := buck.Split()
					if k1.Contain(c.ID) {
						k1.AddContact(c)
					} else {
						k2.AddContact(c)
					}
					b.buckets.InsertAfter(k2, ele)
					b.buckets.InsertAfter(k1, ele)
					b.buckets.Remove(ele)
				} else {
					oldest := buck.LeastRecent()
					if !b.proto.rpcPing(oldest) {
						buck.EvictContact(oldest)
						buck.AddContact(c)
					} else {
						buck.UpdateContact(oldest)
					}
				}
			}
		}
	}
}

func (b *bucketList) GetClosestDistance(id Identifer) Identifer {
	return b.GetClosestContacts(id, 1)[0].Dist
}

func (b *bucketList) GetClosestContacts(id Identifer, num int) []ContWithDist {
	// s := make([]BuckWithDist, 0, num)
	// b.ForEachBucket(func(bk *kBucket) {
	// 	s = append(s, BuckWithDist{
	// 		bk, Distance(id, bk.bucketRange.low),
	// 	})
	// })
	// sort.Slice(s, func(i, j int) bool {
	// 	return s[i].dist.Cmp(s[j].dist) < 0
	// })

	// ret := make([]ContWithDist, 0, num)
	// for _, v := range s {
	// 	v.buck.ForEachContact(func(c Contact) {
	// 		ret = append(ret, ContWithDist{
	// 			c, Distance(id, c.ID),
	// 		})
	// 	})
	// 	if len(ret) >= num {
	// 		break
	// 	}
	// }

	ret := make([]ContWithDist, 0)
	b.ForEachBucket(func(kb *kBucket) {
		kb.ForEachContact(func(c Contact) {
			ret = append(ret, ContWithDist{c, Distance(c.ID, id)})
		})
	})
	sort.Slice(ret, func(i, j int) bool {
		return ret[i].Dist.Cmp(ret[j].Dist) < 0
	})
	return ret[:minInt(num, len(ret))]
}

func (b *bucketList) ContactIndex(c Contact) int {
	cnt := 0
	b.ForEachBucket(func(bk *kBucket) {
		if bk.bucketRange.high.Cmp(c.ID) <= 0 {
			cnt += bk.Size()
		}
		if bk.bucketRange.contain(c.ID) {
			bk.ForEachContact(func(contact Contact) {
				if contact.ID.Cmp(c.ID) < 0 {
					cnt++
				}
			})
		}
	})
	return cnt
}
