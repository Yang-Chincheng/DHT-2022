package chord

import (
	"errors"
	"net"
	"net/rpc"
	"time"

	"github.com/sirupsen/logrus"
)

type networkNode struct {
	nPtr     interface{}
	service  string
	addr     string
	server   *rpc.Server
	listener net.Listener
	onRing   bool
	quitMsg  chan bool
}

func (n *networkNode) serverInit(ipaddr, service string, ptr interface{}) {
	n.addr, n.service, n.nPtr = ipaddr, service, ptr
	n.quitMsg = make(chan bool, maintainerNum)
}

func (n *networkNode) launch() error {
	n.server = rpc.NewServer()
	err := n.server.RegisterName(n.service, n.nPtr)
	if err != nil {
		logrus.Errorf("[%s] launch failed while register, error message: %v", n.addr, err)
		return err
	}
	n.listener, err = net.Listen("tcp", n.addr)
	if err != nil {
		logrus.Errorf("[%s] launch failed while listen, error message: %v", n.addr, err)
		return err
	}
	go n.connect()
	return nil
}

func (n *networkNode) connect() error {
	for {
		var (
			conn net.Conn
			err  error
		)
		acceptMsg := make(chan error, 1)
		go func() {
			conn, err = n.listener.Accept()
			acceptMsg <- err
		}()
		select {
		case <-n.quitMsg:
			logrus.Infof("[%s] server go offline", n.addr)
			return nil
		case <-acceptMsg:
			if err != nil {
				logrus.Errorf("[%s] connect failed while accept, error message: %v", n.addr, err)
				return err
			} else {
				logrus.Infof("[%s] connect succeeed", n.addr)
				go n.server.ServeConn(conn)
			}
		}
	}
}

func (n *networkNode) dial(address string) (*rpc.Client, error) {
	if address == NIL {
		return nil, errors.New("invalid address")
	}
	var (
		client *rpc.Client
		err    error
	)
	for i := 1; i <= dialAttempt; i++ {
		dialMsg := make(chan error, 1)
		go func() {
			client, err = rpc.Dial("tcp", address)
			dialMsg <- err
		}()
		select {
		case <-dialMsg:
			if err != nil {
				logrus.Infof("[%s] dial %s failed, error message %v", n.addr, address, err)
				return nil, err
			} else {
				logrus.Infof("[%s] dial %s succeeded", n.addr, address)
				return client, err
			}
		case <-time.After(dialTimeOut):
			logrus.Tracef("[%s] dial %s time out in attempt%d", n.addr, address, i)
		}
	}
	logrus.Infof("[%s] dial %s time out", n.addr, address)
	return nil, errors.New("dial time out")
}

func (n *networkNode) call(address string, service string, method string, request interface{}, reply interface{}) error {
	client, err := n.dial(address)
	if err != nil {
		logrus.Errorf("[%s] rpc failed while dail %s, error message: %v", n.addr, address, err)
		return err
	}
	logrus.Infof("[%s] remote call to method %s with request %v, reply %v", n.addr, method, request, reply)
	err = client.Call(service+"."+method, request, reply)
	if err != nil {
		logrus.Errorf("[%s] rpc failed while call %s at %s, error message: %v", n.addr, method, address, err)
		return err
	}
	if err := client.Close(); err != nil {
		logrus.Errorf("[%s] rpc failed while close, error message: %v", n.addr, err)
		return err
	}
	return nil
}

func (n *networkNode) ping(address string) bool {
	if address == NIL {
		return false
	}
	var (
		client *rpc.Client
		err    error
	)
	for i := 1; i <= pingAttempt; i++ {
		pingMsg := make(chan error, 1)
		go func() {
			client, err = rpc.Dial("tcp", address)
			if err == nil {
				logrus.Tracef("[%s] ping %s succeeded in attempt%d", n.addr, address, i)
				defer client.Close()
			} else {
				logrus.Tracef("[%s] ping %s failed in attempt%d", n.addr, address, i)
			}
			pingMsg <- err
		}()
		select {
		case <-pingMsg:
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

func (n *networkNode) shutdown(num int) {
	n.onRing = false
	for i := 0; i < num; i++ {
		n.quitMsg <- true
	}
	err := n.listener.Close()
	if err != nil {
		logrus.Errorf("[%s] shutdown failed, error message %v", n.addr, err)
	}
}
