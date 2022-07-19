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
	n.pred = NIL
	n.succList = [succListLen]string{}
	n.finger = [M]string{}
}

func (n *chordBaseNode) GetPredecessor(_ string, reply *string) error {
	n.predLock.RLock()
	defer n.predLock.RUnlock()
	*reply = n.pred
	return nil
}

func (n *chordBaseNode) GetSuccList(_ string, reply *[succListLen]string) error {
	n.succLock.RLock()
	defer n.succLock.RUnlock()
	for i := 0; i < succListLen; i++ {
		(*reply)[i] = n.succList[i]
	}
	return nil
}

func (n *chordBaseNode) GetSuccessor(_ string, reply *string) error {
	n.succLock.RLock()
	defer n.succLock.RUnlock()
	for _, succ := range n.succList {
		if n.ping(succ) {
			*reply = succ
			return nil
		}
	}
	logrus.Errorf("[%s] no available successor in the list", n.addr)
	*reply = NIL
	return errors.New("no available successor")
}

func (n *chordBaseNode) UpdateSuccessor(succ string, _ *string) error {
	n.fingerLock.Lock()
	n.finger[0] = succ
	n.fingerLock.Unlock()
	n.succLock.Lock()
	n.succList[0] = succ
	n.succLock.Unlock()
	if succ != n.addr {
		list := [succListLen]string{}
		err := n.call(succ, "ChordService", "GetSuccList", NIL, &list)
		if err == nil {
			n.succLock.Lock()
			copy(n.succList[1:], list[:])
			n.succLock.Unlock()
		}
	}
	return nil
}

func (n *chordBaseNode) UpdatePredecessor(pred string, _ *string) error {
	n.predLock.Lock()
	defer n.predLock.Unlock()
	n.pred = pred
	return nil
}

func (n *chordBaseNode) TransferQuit(pred string, _ *string) error {
	var succ string
	err := n.GetSuccessor(NIL, &succ)
	if err != nil {
		logrus.Errorf("[%s] transfer data after quit failed, error message %v", n.addr, err)
		return err
	}
	n.backupLock.RLock()
	n.AppendData(n.backup, nil)
	n.backupLock.RUnlock()
	n.dataLock.RLock()
	err = n.call(succ, "ChordService", "SetBackup", n.data, nil)
	n.dataLock.RUnlock()
	if err != nil {
		logrus.Warnf("[%s] transfer data after quit warning", n.addr)
	}
	n.ClearBackup(NIL, nil)
	n.backupLock.Lock()
	err = n.call(pred, "ChordService", "CopyData", NIL, &n.backup)
	n.backupLock.Unlock()
	if err != nil {
		logrus.Warnf("[%s] transfer data after quit warning", n.addr)
	}
	return nil
}

func (n *chordBaseNode) TransferJoin(pred string, _ *string) error {
	var succ string
	err := n.GetSuccessor(NIL, &succ)
	if err != nil {
		logrus.Errorf("[%s] transfer data after quit failed, error message %v", n.addr, err)
		return err
	}
	n.backupLock.RLock()
	err = n.call(pred, "ChordService", "SetBackup", n.backup, nil)
	n.backupLock.RUnlock()
	if err != nil {
		logrus.Warnf("[%s] transfer data after join warning", n.addr)
	}
	filter := func(id string) bool {
		return contain(hash(id), hash(pred), hash(n.addr), "(]")
	}
	temp := make(map[string]string)
	err = n.FilterData(filter, &temp)
	if err != nil {
		logrus.Errorf("[%s] transfer data after join warning, error message %v", n.addr, err)
	}
	err = n.call(pred, "ChordService", "SetData", temp, nil)
	if err != nil {
		logrus.Warnf("[%s] transfer data after join warning", n.addr)
	}
	n.SetBackup(temp, nil)
	n.dataLock.RLock()
	err = n.call(succ, "ChordService", "SetBackup", n.data, nil)
	n.dataLock.RUnlock()
	if err != nil {
		logrus.Warnf("[%s] transfer data after join warning", n.addr)
	}
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
	err := n.GetSuccessor(NIL, reply)
	return err
}

func (n *chordBaseNode) FindSuccessor(id *big.Int, reply *string) error {
	var succ, next string
	err := n.GetSuccessor(NIL, &succ)
	if err == nil && contain(id, hash(n.addr), hash(succ), "(]") {
		logrus.Infof("[%s] find successor of %v succeeded", n.addr, id.String())
		*reply = succ
		return nil
	}
	err = n.ClosestPrecedingFinger(id, &next)
	if err != nil {
		logrus.Errorf("[%s] find successor of %v failed, error message %v", n.addr, id.String(), err)
		return err
	}
	err = n.call(next, "ChordService", "FindSuccessor", id, reply)
	if err != nil {
		logrus.Errorf("[%s] find successor of %v failed, error message %v", n.addr, *id, err)
	} else {
		logrus.Infof("[%s] find successor of %v succeeded", n.addr, id.String())
	}
	return err
}

func (n *chordBaseNode) Stablize(_ string, _ *string) error {
	var succ, p string
	err := n.GetSuccessor(NIL, &succ)
	if err != nil {
		logrus.Errorf("[%s] stablize failed, error message %v", n.addr, err)
		return err
	}
	err = n.call(succ, "ChordService", "GetPredecessor", NIL, &p)
	if err == nil && n.ping(p) && contain(hash(p), hash(n.addr), hash(succ), "()") {
		logrus.Infof("[%s] successor updated", n.addr)
		succ = p
	}
	n.UpdateSuccessor(succ, nil)
	n.call(succ, "ChordService", "Notify", n.addr, nil)
	return nil
}

func (n *chordBaseNode) Notify(p string, _ *string) error {
	var pred string
	n.GetPredecessor(NIL, &pred)
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

func (n *chordBaseNode) FixFinger(x int, _ *string) error {
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
				n.Stablize(NIL, nil)
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
	if n.onRing {
		logrus.Infof("[%s] create failed, node have onRing", n.addr)
		return false
	}
	n.UpdateSuccessor(n.addr, nil)
	n.UpdatePredecessor(n.addr, nil)
	for i := 0; i < M; i++ {
		n.finger[i] = n.addr
	}
	n.onRing = true
	n.maintain()
	return true
}

func (n *chordBaseNode) join(address string) bool {
	if n.onRing {
		logrus.Infof("[%s] join failed, node have onRing", n.addr)
		return false
	}
	var succ string
	n.call(address, "ChordService", "FindSuccessor", hash(n.addr), &succ)
	if succ != n.addr {
		n.call(succ, "ChordService", "TransferJoin", n.addr, nil)
	}
	list := [succListLen]string{}
	n.call(succ, "ChordService", "GetSuccList", NIL, &list)
	n.UpdateSuccessor(succ, nil)
	n.UpdatePredecessor(NIL, nil)
	n.initFingerTable(succ)
	n.onRing = true
	n.maintain()
	return true
}

func (n *chordBaseNode) quit() {
	if !n.onRing {
		logrus.Warnf("[%s] node have quited", n.addr)
		return
	}
	var pred, succ string
	err := n.GetPredecessor(NIL, &pred)
	if err != nil {
		logrus.Warnf("[%s] quit warning, error message %v", n.addr, err)
	}
	n.shutdown(maintainerNum)
	err = n.GetSuccessor(NIL, &succ)
	if err == nil {
		n.call(succ, "ChordService", "Notify", pred, nil)
	}
	n.reset()
}

func (n *chordBaseNode) forceQuit() {
	if !n.onRing {
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
		return false, NIL
	}
	var val string
	err = n.call(succ, "ChordService", "GetData", key, &val)
	if err != nil {
		logrus.Errorf("[%s] get key %s failed, error message %v", n.addr, key, err)
		return false, NIL
	}
	logrus.Infof("[%s] get key %s successed, value %s", n.addr, key, val)
	return true, val
}

func (n *chordBaseNode) put(key, val string) bool {
	var succ, next string
	err := n.FindSuccessor(hash(key), &succ)
	if err != nil {
		logrus.Errorf("[%s] put key-val pair (%s, %s) failed, error message %v", n.addr, key, val, err)
		return false
	}
	err = n.call(succ, "ChordService", "PutData", DataPair{Key: key, Val: val}, nil)
	if err != nil {
		logrus.Errorf("[%s] put key-val pair (%s, %s) in data failed, error message %v", n.addr, key, val, err)
		return false
	}
	err = n.call(succ, "ChordService", "GetSuccessor", NIL, &next)
	if err != nil {
		logrus.Errorf("[%s] put key-val pair (%s, %s) in backup failed, error message %v", n.addr, key, val, err)
		return false
	}
	err = n.call(next, "ChordService", "PutBackup", DataPair{Key: key, Val: val}, nil)
	if err != nil {
		logrus.Errorf("[%s] put key-val pair (%s, %s) in backup failed, error message %v", n.addr, key, val, err)
		return false
	}
	return true
}

func (n *chordBaseNode) del(key string) bool {
	var succ, next string
	err := n.FindSuccessor(hash(key), &succ)
	if err != nil {
		logrus.Errorf("[%s] delete key %s failed, error message: %v", n.addr, key, err)
		return false
	}
	err = n.call(succ, "ChordService", "DeleteData", key, nil)
	if err != nil {
		logrus.Errorf("[%s] delete key %s in data failed, error message: %v", n.addr, key, err)
		return false
	}
	err = n.call(succ, "ChordService", "GetSuccessor", NIL, &next)
	if err != nil {
		logrus.Errorf("[%s] delete key %s in backup failed, error message: %v", n.addr, key, err)
		return false
	}
	err = n.call(next, "ChordService", "DeleteBackup", key, nil)
	if err != nil {
		logrus.Errorf("[%s] delete key %s in backup failed, error message: %v", n.addr, key, err)
		return false
	}
	return true
}

// func (n *chordBaseNode) print() {
// 	n.succLock.RLock()
// 	n.predLock.RLock()
// 	n.dataLock.RLock()
// 	n.backupLock.RLock()
// 	defer n.succLock.RUnlock()
// 	defer n.predLock.RUnlock()
// 	defer n.dataLock.RUnlock()
// 	defer n.backupLock.RUnlock()
// 	var bluePrint = color.New(color.FgBlue).PrintfFunc()
// 	bluePrint("[%s] node info\n", n.addr)
// 	bluePrint("succ: %v\n", simplify(n.succList[:]...))
// 	bluePrint("pred: %v\n", simplify(n.pred))
// }
