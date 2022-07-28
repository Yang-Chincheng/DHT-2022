package kademlia

import (
	"crypto/sha1"
	"math/big"
	"math/rand"
	"time"
)

const (
	NIL = ""

	M     = 160
	K     = 20
	B     = 5
	Alpha = 3

	PingAttempt   = 4
	DialAttempt   = 4
	DialTimeOut   = 300 * time.Millisecond
	PingTimeOut   = 500 * time.Millisecond
	LookupTimeOut = 500 * time.Millisecond

	ExpireTime        = 40 * time.Second
	RefreshInterval   = 30 * time.Second
	RepublishInterval = 30 * time.Second
)

type (
	Address   = string
	Identifer = *big.Int
	KeyType   = string
	ValueType = string
)

var IDGenerator *rand.Rand

func init() {
	IDGenerator = rand.New(rand.NewSource(time.Now().Unix()))
}

type DataPair struct {
	Key KeyType
	Val ValueType
}

func minInt(x, y int) int {
	if x < y {
		return x
	} else {
		return y
	}
}

func absInt(x int) int {
	if x < 0 {
		return -x
	} else {
		return x
	}
}

func minInSlice(s []ContWithDist, skip Contact) ContWithDist {
	if len(s) == 0 {
		return ContWithDist{}
	}
	ret := s[0]
	for _, v := range s {
		if v.Cont.Addr != skip.Addr {
			if v.Dist.Cmp(ret.Dist) < 0 {
				ret = v
			}
		}
	}
	return ret
}

func hash(key string) Identifer {
	hasher := sha1.New()
	hasher.Write([]byte(key))
	return new(big.Int).SetBytes(hasher.Sum(nil))
}

func NewID(x int64) Identifer {
	return big.NewInt(x)
}

func RandomID(interval IDRange) Identifer {
	ret := new(big.Int).Rand(
		IDGenerator,
		interval.length(),
	)
	return ret.Add(ret, interval.low)
}

func IDpow2(x int) Identifer {
	return new(big.Int).Exp(NewID(2), big.NewInt(int64(x)), nil)
}

func Distance(x, y Identifer) Identifer {
	return new(big.Int).Xor(x, y)
}

func SharedPrefixLen(x, y Identifer) int {
	return M - new(big.Int).Xor(x, y).BitLen()
}

type IDRange struct {
	low, high Identifer
}

func (r *IDRange) contain(id Identifer) bool {
	return id.Cmp(r.low) >= 0 && id.Cmp(r.high) < 0
}

func (r *IDRange) midpoint() Identifer {
	return new(big.Int).Div(new(big.Int).Add(r.low, r.high), NewID(2))
}

func (r *IDRange) length() Identifer {
	return new(big.Int).Sub(r.high, r.low)
}

type ContWithDist struct {
	Cont Contact
	Dist Identifer
}

type BuckWithDist struct {
	buck *kBucket
	dist Identifer
}

type ContactHeap []ContWithDist

func (h ContactHeap) Len() int           { return len(h) }
func (h ContactHeap) Less(i, j int) bool { return h[i].Dist.Cmp(h[j].Dist) < 0 }
func (h ContactHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *ContactHeap) Push(x interface{}) {
	*h = append(*h, x.(ContWithDist))
}
func (h *ContactHeap) Pop() interface{} {
	n := len(*h)
	defer func() {
		(*h) = (*h)[:n-1]
	}()
	return (*h)[n-1]
}
