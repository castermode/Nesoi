package server

import (
	"net"
	"bufio"
	"fmt"
	"sync"
	"sync/atomic"
	"math/rand"
)

const (
	defaultReaderSize = 16 * 1024
	defaultWriterSize = 16 * 1024
)

var (
	globalConnID uint32
)

type Server struct {
	cfg 		*Config
	listener	net.Listener
	rwlock		*sync.RWMutex
	clients 	map[uint32]*clientConn
}


func NewServer(cfg *Config) (*Server, error) {
	svr := &Server {
		cfg:		cfg,
		rwlock:		&sync.RWMutex{},
		clients:	make(map[uint32]*clientConn),
	}
	
	var err error
	svr.listener, err = net.Listen("tcp", svr.cfg.Addr)
	if err != nil {
		return nil, err
		
	}
	
	return svr, nil
}

func randomBuf(size int) []byte {
	buf := make([]byte, size)
	for i := 0; i < size; i++ {
		buf[i] = byte(rand.Intn(127))
		if buf[i] == 0 || buf[i] == byte('$') {
			buf[i]++
		}
	}
	return buf
}

func (svr *Server) newClientConn(c net.Conn) *clientConn {
	cc := &clientConn{
		svr:          svr,
		conn:         c,
		connid:       atomic.AddUint32(&globalConnID, 1),
		salt:         randomBuf(20),
		rb:           bufio.NewReaderSize(c, defaultReaderSize),
		wb:           bufio.NewWriterSize(c, defaultWriterSize),
	}
	
	return cc
}

// Start starts the TCP server, accepting new clients and creating service
// go-routine for each. 
func (svr *Server) Start() error {
	defer func() {
		svr.Stop()
	}()
	
	for {
		fmt.Println("before conn!")
		c, err := svr.listener.Accept()
		if err != nil {
			return err
		} 
		
		fmt.Println("one conn!")
		cc := svr.newClientConn(c)
		svr.rwlock.Lock()
		svr.clients[cc.connid] = cc
		svr.rwlock.Unlock()
		go cc.Start()
	}
	
	return nil
}

func (svr *Server) Stop() {
	if svr.listener != nil {
		svr.listener.Close()
		svr.listener = nil
	}
}
