package chord

import (
	"errors"
	"net"
	"net/rpc"
	"time"

	log "github.com/sirupsen/logrus"
)

type networkNode struct {
	nPtr     interface{}
	service  string
	addr     Address
	server   *rpc.Server
	listener net.Listener
	onRing   bool
	quitMsg  chan bool
}

func (n *networkNode) serverInit(ipaddr Address, service string, ptr interface{}) {
	n.addr, n.service, n.nPtr = ipaddr, service, ptr
	n.quitMsg = make(chan bool)
}

func (n *networkNode) launch() error {
	n.server = rpc.NewServer()
	err := n.server.RegisterName(n.service, n.nPtr)
	if err != nil {
		errLogger(n.addr, err).Error("launch failed while register")
		// logrus.Errorf("[%s] launch failed while register, error message: %v", n.addr, err)
		return err
	}
	n.listener, err = net.Listen("tcp", n.addr)
	if err != nil {
		errLogger(n.addr, err).Error("launch failed while listen")
		// logrus.Errorf("[%s] launch failed while listen, error message: %v", n.addr, err)
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
			logger(n.addr).Info("server go offline")
			// logrus.Infof("[%s] server go offline", n.addr)
			return nil
		case <-acceptMsg:
			if err != nil {
				errLogger(n.addr, err).Error("connect failed while accept")
				// logrus.Errorf("[%s] connect failed while accept, error message: %v", n.addr, err)
				return err
			} else {
				logger(n.addr).Info("connect succeeded")
				// logrus.Infof("[%s] connect succeeed", n.addr)
				go n.server.ServeConn(conn)
			}
		}
	}
}

func (n *networkNode) dial(address Address) (*rpc.Client, error) {
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
				errLogger(n.addr, err).WithField("target", address).Info("dail failed")
				// logrus.Infof("[%s] dial %s failed, error message %v", n.addr, address, err)
				return nil, err
			} else {
				logger(n.addr).WithField("target", address).Info("dail succeeded")
				// logrus.Infof("[%s] dial %s succeeded", n.addr, address)
				return client, err
			}
		case <-time.After(dialTimeOut):
			logger(n.addr).WithField("target", address).Tracef("dail time out in attempt%d", i)
			// logrus.Tracef("[%s] dial %s time out in attempt%d", n.addr, address, i)
		}
	}
	logger(n.addr).WithField("target", address).Info("dail time out")
	// logrus.Infof("[%s] dial %s time out", n.addr, address)
	return nil, errors.New("dial time out")
}

func (n *networkNode) call(address Address, service string, method string, request interface{}, reply interface{}) error {
	client, err := n.dial(address)
	if err != nil {
		errLogger(n.addr, err).WithField("target", address).Error("rpc failed while dailing")
		// logrus.Errorf("[%s] rpc failed while dail %s, error message: %v", n.addr, address, err)
		return err
	}
	var rpcLogger = logger(n.addr).WithFields(log.Fields{
		"target":  address,
		"service": service,
		"method":  method,
	})
	logger(n.addr).WithField("request", request).
		Tracef("remote call sending request")
	// logrus.Infof("[%s] remote call to method %s with request %v, reply %v", n.addr, method, request, reply)
	err = client.Call(service+"."+method, request, reply)
	if err != nil {
		rpcLogger.WithError(err).Error("rpc failed while calling")
		// logrus.Errorf("[%s] rpc failed while call %s at %s, error message: %v", n.addr, method, address, err)
		return err
	}
	rpcLogger.WithField("reply", reply).Tracef("remote call got reply")
	if err := client.Close(); err != nil {
		rpcLogger.WithError(err).Error("rpc failed while closing")
		// logrus.Errorf("[%s] rpc failed while close, error message: %v", n.addr, err)
		return err
	}
	return nil
}

func (n *networkNode) ping(address Address) bool {
	if address == NIL {
		return false
	}
	var (
		client     *rpc.Client
		err        error
		pingLogger = logger(n.addr).WithField("target", address)
	)
	for i := 1; i <= pingAttempt; i++ {
		pingMsg := make(chan error, 1)
		go func() {
			client, err = rpc.Dial("tcp", address)
			if err == nil {
				pingLogger.Tracef("ping succeded in attempt%d", i)
				// logrus.Tracef("[%s] ping %s succeeded in attempt%d", n.addr, address, i)
				defer client.Close()
			} else {
				pingLogger.Tracef("ping failed in atttempt%d", i)
				// logrus.Tracef("[%s] ping %s failed in attempt%d", n.addr, address, i)
			}
			pingMsg <- err
		}()
		select {
		case <-pingMsg:
			if err != nil {
				pingLogger.Info("ping failed")
				// logrus.Infof("[%s] ping %s failed, error message: %v", n.addr, address, err)
				return false
			} else {
				pingLogger.Info("ping succeeded")
				// logrus.Infof("[%s] ping %s succeeded", n.addr, address)
				return true
			}
		case <-time.After(pingTimeOut):
			pingLogger.Tracef("ping time out in attempt%d", i)
			// logrus.Tracef("[%s] ping %s time out in attempt%d", n.addr, address, i)
		}
	}
	pingLogger.Info("ping time out")
	// logrus.Infof("[%s] ping %s time out", n.addr, address)
	return false
}

func (n *networkNode) shutdown(num int) {
	n.onRing = false
	close(n.quitMsg)
	// for i := 0; i < num; i++ {
	// 	n.quitMsg <- true
	// }
	err := n.listener.Close()
	if err != nil {
		errLogger(n.addr, err).Error("shutdown failed")
		// logrus.Errorf("[%s] shutdown failed, error message %v", n.addr, err)
	}
}
