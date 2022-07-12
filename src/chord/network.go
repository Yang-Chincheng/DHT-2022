package chord

import (
	"net"
	"net/rpc"
	"time"

	"github.com/sirupsen/logrus"
)

type networkNode struct {
	nPtr     interface{}
	addr     string
	server   *rpc.Server
	listener net.Listener
}

func (n *networkNode) initServer(ipaddr string, ptr interface{}) {
	n.addr, n.nPtr = ipaddr, ptr
}

func (n *networkNode) launch() error {
	n.server = rpc.NewServer()
	err := n.server.RegisterName("server:"+n.addr, n.nPtr)
	if err != nil {
		logrus.Errorf("[%s] launch failed while register, error message: %v", n.addr, err)
		return err
	}
	n.listener, err = net.Listen("tcp", n.addr)
	if err != nil {
		logrus.Errorf("[%s] launch failed while listen, error message: %v", n.addr, err)
		return err
	}
	go n.listen()
	return nil
}

func (n *networkNode) listen() error {
	for {
		conn, err := n.listener.Accept()
		if err != nil {
			logrus.Errorf("[%s] listen failed while accept, error message: %v", n.addr, err)
			return err
		}
		go n.server.ServeConn(conn)
	}
}

func (n *networkNode) call(address string, method string, request interface{}, reply interface{}) error {
	client, err := rpc.Dial("tcp", n.addr)
	if err != nil {
		logrus.Errorf("[%s] rpc failed while dail, error message: %v", n.addr, err)
		return err
	}
	defer func() {
		if err := client.Close(); err != nil {
			logrus.Errorf("[%s] rpc failed while close, error message: %v", n.addr, err)
		}
	}()
	err = client.Call(address+"."+method, request, reply)
	if err != nil {
		logrus.Errorf("[%s] rpc failed while call, error message: %v", n.addr, err)
		return err
	}
	return nil
}

func (n *networkNode) ping(address string) bool {
	for i := 1; i <= pingAttempt; i++ {
		pingMsg := make(chan error)
		go func() {
			client, err := rpc.Dial("tcp", address)
			if err == nil {
				logrus.Tracef("[%s] ping %s succeeded in attempt%d", n.addr, address, i)
				defer client.Close()
			} else {
				logrus.Tracef("[%s] ping %s failed in attempt%d", n.addr, address, i)
			}
			pingMsg <- err
		}()
		select {
		case err := <-pingMsg:
			if err != nil {
				logrus.Infof("[%s] ping %s failed, error message: %v", n.addr, address, err)
				return false
			} else {
				logrus.Infof("[%s] ping %s succeeded", n.addr, address)
				return true
			}
		case <-time.After(pingTimeOut):
			logrus.Tracef("[%s] ping %s time out in attempt%d", n.addr, address, i)
		}
	}
	logrus.Infof("[%s] ping %s time out", n.addr, address)
	return false
}
