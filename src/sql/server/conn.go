package server

import (
	"bufio"
	"encoding/binary"
	"io"
	"net"
	"time"

	"github.com/castermode/Nesoi/src/sql/mysql"
	"github.com/golang/glog"
	"github.com/juju/errors"
)

var defaultCapability = mysql.ClientLongPassword | mysql.ClientLongFlag |
	mysql.ClientConnectWithDB | mysql.ClientProtocol41 |
	mysql.ClientTransactions | mysql.ClientSecureConnection | mysql.ClientFoundRows |
	mysql.ClientMultiStatements | mysql.ClientMultiResults | mysql.ClientLocalFiles |
	mysql.ClientConnectAtts

type clientConn struct {
	svr        *Server
	conn       net.Conn
	connid     uint32
	capability uint32
	salt       []byte

	rb       *bufio.Reader
	wb       *bufio.Writer
	sequence uint8
}

func (cc *clientConn) Start() {
	defer func() {
		cc.Stop()
	}()

	if err := cc.handshake(); err != nil {
		glog.Error("Handshake error: ", err.Error())
		return
	}

	glog.Info("Connection ", cc.conn.RemoteAddr(), " has handshaked, starting accept package")
	for {
		time.Sleep(time.Second)
		//		fmt.Println("for....")
		//		data, err := c.readPacket()
		//		if err != nil {
		//			return
		//		}

		//		// deal message
		//		if err = c.dealMessage(data); err != nil {
		//			return
		//		}
	}
}

func (cc *clientConn) Stop() {
	cc.svr.rwlock.Lock()
	delete(cc.svr.clients, cc.connid)
	cc.svr.rwlock.Unlock()
}

func (cc *clientConn) readOnePacket() ([]byte, error) {
	var header [4]byte

	if _, err := io.ReadFull(cc.rb, header[:]); err != nil {
		return nil, err
	}

	sequence := uint8(header[3])
	if sequence != cc.sequence {
		return nil, errors.New("invalid sequence")
	}

	cc.sequence++

	length := int(uint32(header[0]) | uint32(header[1])<<8 | uint32(header[2])<<16)

	data := make([]byte, length)
	if _, err := io.ReadFull(cc.rb, data); err != nil {
		return nil, err
	}
	return data, nil
}

func (cc *clientConn) readPacket() ([]byte, error) {
	data, err := cc.readOnePacket()
	if err != nil {
		return nil, err
	}

	if len(data) < mysql.MaxPayloadLen {
		return data, nil
	}

	// handle muliti-packet
	for {
		buf, err := cc.readOnePacket()
		if err != nil {
			return nil, err
		}

		data = append(data, buf...)

		if len(buf) < mysql.MaxPayloadLen {
			break
		}
	}

	return data, nil
}

func (cc *clientConn) writePacket(data []byte) error {
	length := len(data) - 4

	for length >= mysql.MaxPayloadLen {
		data[0] = 0xff
		data[1] = 0xff
		data[2] = 0xff
		data[3] = cc.sequence

		if n, err := cc.wb.Write(data[:4+mysql.MaxPayloadLen]); err != nil {
			return mysql.ErrBadConn
		} else if n != (4 + mysql.MaxPayloadLen) {
			return mysql.ErrBadConn
		} else {
			cc.sequence++
			length -= mysql.MaxPayloadLen
			data = data[mysql.MaxPayloadLen:]
		}
	}

	data[0] = byte(length)
	data[1] = byte(length >> 8)
	data[2] = byte(length >> 16)
	data[3] = cc.sequence

	if n, err := cc.wb.Write(data); err != nil {
		return mysql.ErrBadConn
	} else if n != len(data) {
		return mysql.ErrBadConn
	} else {
		cc.sequence++
		return nil
	}
}

func (cc *clientConn) flush() error {
	return cc.wb.Flush()
}

func (cc *clientConn) writeError(e error) error {
	m := mysql.NewErrf(mysql.ErrUnknown, e.Error())

	data := make([]byte, 4, 16+len(m.Message))
	data = append(data, mysql.ErrHeader)
	data = append(data, byte(m.Code), byte(m.Code>>8))
	if cc.capability&mysql.ClientProtocol41 > 0 {
		data = append(data, '#')
		data = append(data, m.State...)
	}

	data = append(data, m.Message...)

	err := cc.writePacket(data)
	if err != nil {
		return err
	}
	return cc.flush()
}

func (cc *clientConn) writeInitialHandshake() error {
	data := make([]byte, 4, 128)

	// min version 10
	data = append(data, 10)
	// server version[00]
	data = append(data, mysql.ServerVersion...)
	data = append(data, 0)
	// connection id
	data = append(data, byte(cc.connid), byte(cc.connid>>8), byte(cc.connid>>16), byte(cc.connid>>24))
	// auth-plugin-data-part-1
	data = append(data, cc.salt[0:8]...)
	// filler [00]
	data = append(data, 0)
	// capability flag lower 2 bytes, using default capability here
	data = append(data, byte(defaultCapability), byte(defaultCapability>>8))
	// charset, utf-8 default
	data = append(data, uint8(mysql.DefaultCollationID))
	//status
	data = append(data, dumpUint16(mysql.ServerStatusAutocommit)...)
	// below 13 byte may not be used
	// capability flag upper 2 bytes, using default capability here
	data = append(data, byte(defaultCapability>>16), byte(defaultCapability>>24))
	// filler [0x15], for wireshark dump, value is 0x15
	data = append(data, 0x15)
	// reserved 10 [00]
	data = append(data, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
	// auth-plugin-data-part-2
	data = append(data, cc.salt[8:]...)
	// filler [00]
	data = append(data, 0)
	err := cc.writePacket(data)
	if err != nil {
		return err
	}
	return cc.flush()

}

type handshakeResponse41 struct {
	capability uint32
}

func handshakeResponseParse(p *handshakeResponse41, data []byte) error {
	// capability
	capability := binary.LittleEndian.Uint32(data[:4])
	p.capability = capability

	return nil
}

func (cc *clientConn) readHandshakeResponse() error {
	data, err := cc.readPacket()
	if err != nil {
		return err
	}

	var p handshakeResponse41
	if err = handshakeResponseParse(&p, data); err != nil {
		return err
	}
	cc.capability = p.capability & defaultCapability

	// @t0do: do auth
	return nil
}

func (cc *clientConn) handshake() error {
	if err := cc.writeInitialHandshake(); err != nil {
		return err
	}
	
	if err := cc.readHandshakeResponse(); err != nil {
		cc.writeError(err)
		return err
	}
	
	
	data := make([]byte, 4, 32)
	data = append(data, mysql.OKHeader)
	data = append(data, 0, 0)
	if cc.capability&mysql.ClientProtocol41 > 0 {
		data = append(data, dumpUint16(mysql.ServerStatusAutocommit)...)
		data = append(data, 0, 0)
	}

	err := cc.writePacket(data)
	cc.sequence = 0
	if err != nil {
		return err
	}

	return cc.flush()
}

//func (cc *clientConn) readOnePacket() ([]byte, error) {
//	var header [4]byte

//}

//func (cc *clientConn) readPacket() ([]byte, error) {

//}

//func (cc *clientConn) writePacket() ([]byte, error) {

//}

//func (cc *clientConn) dealMessage(data []byte) error {

//}
