package chord

import (
	"errors"
	"math/big"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

type chordBaseNode struct {
	networkNode
	databaseNode

	succLock   sync.RWMutex
	predLock   sync.RWMutex
	fingerLock sync.RWMutex

	succList [succListLen]string
	pred     string
	finger   [M]string
}

func (n *chordBaseNode) initialize(ip string) {
	n.serverInit(ip, "ChordService", n)
	n.storeInit()
}

func (n *chordBaseNode) reset() {
	n.storeReset()
	n.pred = ""
	n.succList = [succListLen]string{}
	n.finger = [M]string{}
}

func (n *chordBaseNode) GetPredecessor(_ interface{}, reply *string) error {
	n.predLock.RLock()
	defer n.predLock.RUnlock()
	*reply = n.pred
	return nil
}

func (n *chordBaseNode) GetSuccList(_ interface{}, reply []string) error {
	n.succLock.RLock()
	defer n.succLock.RUnlock()
	copy(reply, n.succList[:])
	return nil
}

func (n *chordBaseNode) GetSuccessor(_ interface{}, reply *string) error {
	n.succLock.RLock()
	defer n.succLock.RUnlock()
	for _, succ := range n.succList {
		if n.ping(succ) {
			*reply = succ
			return nil
		}
	}
	logrus.Errorf("[%s] no available successor in the list", n.addr)
	*reply = ""
	return errors.New("no available successor")
}

func (n *chordBaseNode) UpdateSuccessor(succ string, _ interface{}) error {
	// update finger table
	n.fingerLock.Lock()
	n.finger[0] = succ
	n.fingerLock.Unlock()
	// update successor list
	n.succLock.Lock()
	n.succList[0] = succ
	if succ != n.addr {
		list := make([]string, succListLen)
		err := n.call(succ, "ChordService", "GetSuccList", nil, list)
		if err == nil {
			copy(n.succList[1:], list)
		}
	}
	n.succLock.Unlock()
	return nil
}

func (n *chordBaseNode) UpdatePredecessor(pred string, _ interface{}) error {
	n.predLock.Lock()
	defer n.predLock.Unlock()
	n.pred = pred
	return nil
}

func (n *chordBaseNode) ClosestPrecedingFinger(id *big.Int, reply *string) error {
	n.fingerLock.RLock()
	defer n.fingerLock.RUnlock()
	for i := M - 1; i >= 0; i-- {
		if n.ping(n.finger[i]) && contain(hash(n.finger[i]), hash(n.addr), id, "()") {
			*reply = n.finger[i]
			return nil
		}
	}
	logrus.Infof("[%s] successor failed", n.addr)
	err := n.GetSuccessor(nil, reply)
	return err
}

func (n *chordBaseNode) TransferQuit(pred string, _ interface{}) error {
	var succ string
	err := n.GetSuccessor(nil, &succ)
	if err != nil {
		logrus.Errorf("[%s] transfer data after quit failed, error message %v", n.addr, err)
		return err
	}
	n.dataLock.Lock()
	n.backupLock.Lock()
	defer n.dataLock.Unlock()
	defer n.backupLock.Unlock()
	n.AppendData(n.backup, nil)
	err = n.call(succ, "ChordService", "SetBackup", n.data, nil)
	if err != nil {
		logrus.Warnf("[%s] transfer data after quit warning", n.addr)
	}
	n.backup = make(map[string]string)
	err = n.call(pred, "ChrodService", "CopyData", nil, n.backup)
	if err != nil {
		logrus.Warnf("[%s] transfer data after quit warning", n.addr)
	}
	return nil
}

func (n *chordBaseNode) TransferJoin(pred string, _ interface{}) error {
	var succ string
	err := n.GetSuccessor(nil, &succ)
	if err != nil {
		logrus.Errorf("[%s] transfer data after quit failed, error message %v", n.addr, err)
		return err
	}
	n.dataLock.Lock()
	n.backupLock.Lock()
	defer n.dataLock.Unlock()
	defer n.backupLock.Unlock()
	err = n.call(pred, "ChordService", "SetBackup", n.backup, nil)
	if err != nil {
		logrus.Warnf("[%s] transfer data after join warning", n.addr)
	}
	filter := func(id string) bool {
		return contain(hash(id), hash(n.addr), hash(succ), "[)")
	}
	var temp map[string]string
	n.FilterData(filter, temp)
	err = n.call(pred, "ChordService", "SetData", temp, nil)
	if err != nil {
		logrus.Warnf("[%s] transfer data after join warning", n.addr)
	}
	n.SetBackup(temp, nil)
	err = n.call(succ, "ChordService", "SetBackup", n.data, nil)
	if err != nil {
		logrus.Warnf("[%s] transfer data after join warning", n.addr)
	}
	return nil
}

func (n *chordBaseNode) FindSuccessor(id *big.Int, reply *string) error {
	var succ, next string
	err := n.GetSuccessor(nil, &succ)
	if err == nil && contain(id, hash(n.addr), hash(succ), "[)") {
		logrus.Infof("[%s] find successor of %v succeeded", n.addr, *id)
		*reply = succ
		return nil
	}
	err = n.ClosestPrecedingFinger(id, &next)
	if err != nil {
		logrus.Errorf("[%s] find successor of %v failed, error message %v", n.addr, *id, err)
		return err
	}
	err = n.call(next, "ChordService", "FindSuccessor", id, reply)
	if err != nil {
		logrus.Errorf("[%s] find successor of %v failed, error message %v", n.addr, *id, err)
	} else {
		logrus.Infof("[%s] find successor of %v succeeded", n.addr, *id)
	}
	return err
}

func (n *chordBaseNode) Stablize(_, _ interface{}) error {
	var succ, p string
	err := n.GetSuccessor(nil, &succ)
	if err != nil {
		logrus.Errorf("[%s] stablize failed, error message %v", n.addr, err)
		return err
	}
	err = n.call(succ, "ChordService", "GetPredecessor", "", &p)
	if err == nil && n.ping(p) && contain(hash(p), hash(n.addr), hash(succ), "()") {
		logrus.Infof("[%s] successor updated", n.addr)
		succ = p
		n.UpdateSuccessor(succ, nil)
	}
	n.call(succ, "ChordService", "Notify", n.addr, nil)
	return nil
}

func (n *chordBaseNode) Notify(p string, _ interface{}) error {
	var pred string
	n.GetPredecessor(nil, &pred)
	if !n.ping(pred) {
		n.UpdatePredecessor(p, nil)
		n.TransferQuit(p, nil)
	} else {
		if contain(hash(p), hash(pred), hash(n.addr), "()") {
			n.UpdatePredecessor(p, nil)
		}
	}
	return nil
}

func (n *chordBaseNode) FixFinger(x int, _ interface{}) error {
	var next string
	err := n.FindSuccessor(getStart(n.addr, x), &next)
	if err == nil {
		n.fingerLock.Lock()
		defer n.fingerLock.Unlock()
		n.finger[x] = next
	}
	return nil
}

func (n *chordBaseNode) maintain() {
	go func() {
		for {
			select {
			case <-n.quitMsg:
				return
			default:
				n.Stablize(nil, nil)
			}
			time.Sleep(stablizePauseTime)
		}
	}()
	go func() {
		idx := 0
		for {
			select {
			case <-n.quitMsg:
				return
			default:
				n.FixFinger(idx, nil)
				idx = (idx + 1) % M
			}
			time.Sleep(fixfingerPauseTime)
		}
	}()
}

func (n *chordBaseNode) initFingerTable(succ string) {
	n.fingerLock.Lock()
	defer n.fingerLock.Unlock()
	n.finger[0] = succ
	for i := 1; i < M; i++ {
		if contain(getStart(n.addr, i), hash(n.addr), hash(n.finger[i-1]), "[)") {
			n.finger[i] = n.finger[i-1]
		} else {
			n.call(succ, "ChordService", "FindSuccessor", getStart(n.addr, i), &n.finger[i])
		}
	}
}

func (n *chordBaseNode) create() bool {
	if n.online {
		logrus.Infof("[%s] create failed, node have joined", n.addr)
		return false
	}
	n.UpdateSuccessor(n.addr, nil)
	n.UpdatePredecessor(n.addr, nil)
	for i := 0; i < M; i++ {
		n.finger[i] = n.addr
	}
	n.online = true
	n.maintain()
	return true
}

func (n *chordBaseNode) join(address string) bool {
	if n.online {
		logrus.Infof("[%s] join failed, node have joined", n.addr)
		return false
	}
	var succ string
	n.call(address, "ChordService", "FindSuccessor", hash(n.addr), &succ)
	if succ != n.addr {
		n.call(succ, "ChordService", "TransferJoin", n.addr, nil)
	}
	n.UpdateSuccessor(succ, nil)
	n.UpdatePredecessor("", nil)
	n.initFingerTable(succ)
	n.online = true
	n.maintain()
	return true
}

func (n *chordBaseNode) quit() {
	if !n.online {
		logrus.Warnf("[%s] node have quited", n.addr)
		return
	}
	n.shutdown(maintainerNum)
	var pred, succ string
	err := n.GetPredecessor(nil, &pred)
	if err != nil {
		logrus.Warnf("[%s] quit warning, error message %v", err)
	}
	err = n.GetSuccessor(nil, &succ)
	if err != nil {
		logrus.Errorf("[%s] quit failed, error message %v", err)
		return
	}
	n.call(succ, "ChordService", "Notify", pred, nil)
	n.reset()
}

func (n *chordBaseNode) forceQuit() {
	if !n.online {
		logrus.Warnf("[%s] node have quited", n.addr)
		return
	}
	n.shutdown(maintainerNum)
	n.reset()
}

func (n *chordBaseNode) get(key string) (bool, string) {
	var succ string
	err := n.FindSuccessor(hash(key), &succ)
	if err != nil {
		logrus.Errorf("[%s] get key %s failed, error message %v", n.addr, key, err)
		return false, ""
	}
	var val string
	err = n.call(succ, "ChordService", "GetData", nil, &val)
	if err != nil {
		logrus.Errorf("[%s] get key %s failed, error message %v", n.addr, key, err)
		return false, ""
	}
	logrus.Infof("[%s] get key %s successed, value %s", n.addr, key, val)
	return true, val
}

func (n *chordBaseNode) put(key, val string) bool {
	var succ string
	err := n.FindSuccessor(hash(key), &succ)
	if err != nil {
		logrus.Errorf("[%s] put key-val pair (%s, %s) failed, error message %v", n.addr, key, val, err)
		return false
	}
	err = n.call(succ, "ChordService", "PutData", dataPair{k: key, v: val}, nil)
	if err != nil {
		logrus.Errorf("[%s] put key-val pair (%s, %s) in data failed, error message %v", n.addr, key, val, err)
		return false
	}
	err = n.call(succ, "ChordService", "PutBackup", dataPair{k: key, v: val}, nil)
	if err != nil {
		logrus.Errorf("[%s] put key-val pair (%s, %s) in backup failed, error message %v", n.addr, key, val, err)
		return false
	}
	return true
}

func (n *chordBaseNode) del(key string) bool {
	var succ string
	err := n.FindSuccessor(hash(key), &succ)
	if err != nil {
		logrus.Errorf("[%s] delete key %s failed, error message %v", n.addr, key, err)
		return false
	}
	err = n.call(succ, "ChordService", "DeleteData", key, nil)
	if err != nil {
		logrus.Errorf("[%s] delete key %s in data failed, error message %v", n.addr, key, err)
		return false
	}
	err = n.call(succ, "ChordService", "DeleteBackup", key, nil)
	if err != nil {
		logrus.Errorf("[%s] delete key %s in backup failed, error message %v", n.addr, key, err)
		return false
	}
	return true
}
