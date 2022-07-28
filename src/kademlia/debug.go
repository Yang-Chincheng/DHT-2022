package kademlia

import (
	"fmt"
	"os"

	log "github.com/sirupsen/logrus"
)

func init() {
	file, _ := os.Create("debug.log")
	log.SetOutput(file)
	log.SetLevel(log.WarnLevel)
}

func logger(addr Address) *log.Entry {
	return log.WithField("addr", addr)
}

func errLogger(addr Address, err error) *log.Entry {
	return log.WithField("addr", addr).WithError(err)
}

func (b *bucketList) Print() {
	b.buckLock.RLock()
	defer b.buckLock.RUnlock()
	fmt.Printf("[Routing Table] %s\n", b.host.Addr)
	for v := b.buckets.Front(); v != nil; v = v.Next() {
		v.Value.(*kBucket).Print()
	}
	fmt.Println()
}

func (b *kBucket) Print() {
	b.contLock.RLock()
	defer b.contLock.RUnlock()
	for v := b.contacts.Front(); v != nil; v = v.Next() {
		fmt.Printf("%s ", v.Value.(Contact).Addr)
	}
	fmt.Println()
}
