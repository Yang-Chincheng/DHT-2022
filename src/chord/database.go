package chord

import (
	"sync"
)

type databaseNode struct {
	dataLock   sync.RWMutex
	backupLock sync.RWMutex
	data       map[string]string
	backup     map[string]string
}

func (n *databaseNode) storeInit() {
	n.data = make(map[string]string)
	n.backup = make(map[string]string)
}

func (n *databaseNode) storeReset() {
	n.data = make(map[string]string)
	n.backup = make(map[string]string)
}

func (n *databaseNode) PutData(p dataPair, _ interface{}) error {
	n.dataLock.Lock()
	defer n.dataLock.Unlock()
	n.data[p.k] = p.v
	return nil
}

func (n *databaseNode) PutBackup(p dataPair, _ interface{}) error {
	n.backupLock.Lock()
	defer n.backupLock.Unlock()
	n.backup[p.k] = p.v
	return nil
}

func (n *databaseNode) GetData(k string, v *string) error {
	n.dataLock.RLock()
	defer n.dataLock.RUnlock()
	*v = n.data[k]
	return nil
}

func (n *databaseNode) GetBackup(k string, v *string) error {
	n.backupLock.RLock()
	defer n.backupLock.RUnlock()
	*v = n.backup[k]
	return nil
}

func (n *databaseNode) SetData(mp map[string]string, _ interface{}) error {
	n.data = make(map[string]string)
	for k, v := range mp {
		n.data[k] = v
	}
	return nil
}

func (n *databaseNode) SetBackup(mp map[string]string, _ interface{}) error {
	n.backup = make(map[string]string)
	for k, v := range mp {
		n.backup[k] = v
	}
	return nil
}

func (n *databaseNode) DeleteData(k string, _ interface{}) error {
	n.dataLock.Lock()
	defer n.dataLock.Unlock()
	delete(n.data, k)
	return nil
}

func (n *databaseNode) DeleteBackup(k string, _ interface{}) error {
	n.backupLock.Lock()
	defer n.backupLock.Unlock()
	delete(n.backup, k)
	return nil
}

func (n *databaseNode) AppendData(mp map[string]string, _ interface{}) error {
	n.dataLock.Lock()
	defer n.dataLock.Unlock()
	for k, v := range mp {
		n.data[k] = v
	}
	return nil
}

func (n *databaseNode) AppendBackup(mp map[string]string, _ interface{}) error {
	n.backupLock.Lock()
	defer n.backupLock.Unlock()
	for k, v := range mp {
		n.backup[k] = v
	}
	return nil
}

func (n *databaseNode) FilterData(filter func(string) bool, res map[string]string) error {
	n.dataLock.Lock()
	defer n.dataLock.Unlock()
	for k, v := range n.data {
		if !filter(k) {
			delete(n.data, k)
			res[k] = v
		}
	}
	return nil
}

func (n *databaseNode) FilterBackup(filter func(string) bool, res map[string]string) error {
	n.backupLock.Lock()
	defer n.backupLock.Unlock()
	for k, v := range n.backup {
		if !filter(k) {
			delete(n.backup, k)
			res[k] = v
		}
	}
	return nil
}

func (n *databaseNode) CopyData(_ interface{}, mp map[string]string) error {
	for k, v := range n.data {
		mp[k] = v
	}
	return nil
}

func (n *databaseNode) CopyBackup(_ interface{}, mp map[string]string) error {
	for k, v := range n.backup {
		mp[k] = v
	}
	return nil
}
