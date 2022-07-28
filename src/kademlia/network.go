package kademlia

import (
	"errors"
	"net"
	"net/rpc"
	"time"

	log "github.com/sirupsen/logrus"
)

type networkNode struct {
	proto      *protocol
	addr       Address
	server     *rpc.Server
	listener   net.Listener
	online     bool
	quitSignal chan bool
}

func (n *networkNode) launch() error {
	n.server = rpc.NewServer()
	err := n.server.RegisterName("KademliaService", n.proto)
	if err != nil {
		errLogger(n.addr, err).Error("launch failed while register")
		return err
	}
	n.listener, err = net.Listen("tcp", n.addr)
	if err != nil {
		errLogger(n.addr, err).Error("launch failed while listen")
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
		conn, err = n.listener.Accept()
		select {
		case <-n.quitSignal:
			logger(n.addr).Info("server go offline")
			return nil
		default:
			if err != nil {
				errLogger(n.addr, err).Error("connect failed while accept")
				return err
			} else {
				logger(n.addr).Info("connect succeeded")
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
	for i := 1; i <= DialAttempt; i++ {
		dialMsg := make(chan error, 1)
		go func() {
			client, err = rpc.Dial("tcp", address)
			dialMsg <- err
		}()
		select {
		case <-dialMsg:
			if err != nil {
				errLogger(n.addr, err).WithField("target", address).Info("dail failed")
				return nil, err
			} else {
				logger(n.addr).WithField("target", address).Info("dail succeeded")
				return client, err
			}
		case <-time.After(DialTimeOut):
			logger(n.addr).WithField("target", address).Tracef("dail time out in attempt%d", i)
		}
	}
	logger(n.addr).WithField("target", address).Info("dail time out")
	return nil, errors.New("dial time out")
}

func (n *networkNode) call(address Address, service string, method string, request interface{}, reply interface{}) error {
	client, err := n.dial(address)
	if err != nil {
		errLogger(n.addr, err).WithField("target", address).Error("rpc failed while dailing")
		return err
	}
	var rpcLogger = logger(n.addr).WithFields(log.Fields{
		"target":  address,
		"service": service,
		"method":  method,
	})
	logger(n.addr).WithField("request", request).
		Tracef("remote call sending request")
	err = client.Call(service+"."+method, request, reply)
	if err != nil {
		rpcLogger.WithError(err).Error("rpc failed while calling")
		return err
	}
	rpcLogger.WithField("reply", reply).Tracef("remote call got reply")
	if err := client.Close(); err != nil {
		rpcLogger.WithError(err).Error("rpc failed while closing")
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
	for i := 1; i <= PingAttempt; i++ {
		pingMsg := make(chan error, 1)
		go func() {
			client, err = rpc.Dial("tcp", address)
			if err == nil {
				pingLogger.Tracef("ping succeded in attempt%d", i)
				defer client.Close()
			} else {
				pingLogger.Tracef("ping failed in atttempt%d", i)
			}
			pingMsg <- err
		}()
		select {
		case <-pingMsg:
			if err != nil {
				pingLogger.Info("ping failed")
				return false
			} else {
				pingLogger.Info("ping succeeded")
				return true
			}
		case <-time.After(PingTimeOut):
			pingLogger.Tracef("ping time out in attempt%d", i)
		}
	}
	pingLogger.Info("ping time out")
	return false
}

func (n *networkNode) shutdown() {
	n.online = false
	close(n.quitSignal)
	err := n.listener.Close()
	if err != nil {
		errLogger(n.addr, err).Error("shutdown failed")
	}
}
