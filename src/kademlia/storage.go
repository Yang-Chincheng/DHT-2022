package kademlia

import (
	"sync"
	"time"
)

type storeData struct {
	value          ValueType
	repubTimeStamp time.Time
	expireDura     time.Duration
}

type storage struct {
	lock  sync.RWMutex
	store map[KeyType]storeData
}

func NewStorage() *storage {
	ret := new(storage)
	ret.store = make(map[KeyType]storeData)
	return ret
}

func (s *storage) Get(key KeyType) (ValueType, bool) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	if data, ok := s.store[key]; ok {
		// if data.value == NIL {
		// 	panic("invalid data")
		// }
		return data.value, true
	} else {
		return NIL, false
	}
}

func (s *storage) Put(key KeyType, val ValueType, expire time.Duration) {
	s.lock.Lock()
	defer s.lock.Unlock()
	// if val == NIL {
	// 	panic("invalid data")
	// }
	s.store[key] = storeData{val, time.Now(), expire}
}

func (s *storage) Remove(key KeyType) {
	s.lock.Lock()
	defer s.lock.Unlock()
	delete(s.store, key)
}

func (s *storage) Touch(key KeyType) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if v, ok := s.store[key]; ok {
		v.repubTimeStamp = time.Now()
		s.store[key] = v
	}
}

func (s *storage) ForEachKeyValue(foo func(KeyType, ValueType)) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	for k, v := range s.store {
		foo(k, v.value)
	}
}
