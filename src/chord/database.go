package chord

import (
	"sync"
)

type StoreType map[KeyType]ValueType
type FilterType func(string) bool

type databaseNode struct {
	dataLock   sync.RWMutex
	backupLock sync.RWMutex
	data       StoreType
	backup     StoreType
}

func (n *databaseNode) storeInit() {
	n.data = make(StoreType)
	n.backup = make(StoreType)
}

func (n *databaseNode) storeReset() {
	n.data = make(StoreType)
	n.backup = make(StoreType)
}

func (n *databaseNode) PutData(p DataPair, _ *string) error {
	n.dataLock.Lock()
	defer n.dataLock.Unlock()
	n.data[p.Key] = p.Val
	return nil
}

func (n *databaseNode) PutBackup(p DataPair, _ *string) error {
	n.backupLock.Lock()
	defer n.backupLock.Unlock()
	n.backup[p.Key] = p.Val
	return nil
}

func (n *databaseNode) GetData(k KeyType, v *ValueType) error {
	n.dataLock.RLock()
	defer n.dataLock.RUnlock()
	*v = n.data[k]
	return nil
}

func (n *databaseNode) GetBackup(k KeyType, v *ValueType) error {
	n.backupLock.RLock()
	defer n.backupLock.RUnlock()
	*v = n.backup[k]
	return nil
}

func (n *databaseNode) SetData(mp StoreType, _ *string) error {
	n.data = make(map[string]string)
	for k, v := range mp {
		n.data[k] = v
	}
	return nil
}

func (n *databaseNode) SetBackup(mp StoreType, _ *string) error {
	n.backup = make(map[string]string)
	for k, v := range mp {
		n.backup[k] = v
	}
	return nil
}

func (n *databaseNode) DeleteData(k KeyType, _ *string) error {
	n.dataLock.Lock()
	defer n.dataLock.Unlock()
	delete(n.data, k)
	return nil
}

func (n *databaseNode) DeleteBackup(k KeyType, _ *string) error {
	n.backupLock.Lock()
	defer n.backupLock.Unlock()
	delete(n.backup, k)
	return nil
}

func (n *databaseNode) AppendData(mp StoreType, _ *string) error {
	n.dataLock.Lock()
	defer n.dataLock.Unlock()
	for k, v := range mp {
		n.data[k] = v
	}
	return nil
}

func (n *databaseNode) AppendBackup(mp StoreType, _ *string) error {
	n.backupLock.Lock()
	defer n.backupLock.Unlock()
	for k, v := range mp {
		n.backup[k] = v
	}
	return nil
}

func (n *databaseNode) FilterData(filter FilterType, res *StoreType) error {
	n.dataLock.Lock()
	defer n.dataLock.Unlock()
	for k, v := range n.data {
		if !filter(k) {
			delete(n.data, k)
			(*res)[k] = v
		}
	}
	return nil
}

func (n *databaseNode) FilterBackup(filter FilterType, res *StoreType) error {
	n.backupLock.Lock()
	defer n.backupLock.Unlock()
	for k, v := range n.backup {
		if !filter(k) {
			delete(n.backup, k)
			(*res)[k] = v
		}
	}
	return nil
}

func (n *databaseNode) CopyData(_ string, mp *StoreType) error {
	n.dataLock.RLock()
	defer n.dataLock.RUnlock()
	for k, v := range n.data {
		(*mp)[k] = v
	}
	return nil
}

func (n *databaseNode) CopyBackup(_ string, mp *StoreType) error {
	n.backupLock.RLock()
	defer n.backupLock.RUnlock()
	for k, v := range n.backup {
		(*mp)[k] = v
	}
	return nil
}

func (n *databaseNode) ClearData(_ string, _ *string) error {
	n.dataLock.Lock()
	defer n.dataLock.Unlock()
	n.data = make(StoreType)
	return nil
}

func (n *databaseNode) ClearBackup(_ string, _ *string) error {
	n.backupLock.Lock()
	defer n.backupLock.Unlock()
	n.backup = make(StoreType)
	return nil
}

// func (n *databaseNode) dataSize() int {
// 	n.dataLock.RLock()
// 	defer n.dataLock.RUnlock()
// 	return len(n.data)
// }
