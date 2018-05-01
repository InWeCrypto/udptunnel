package udptunnel

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"net"
	"sync"
	"time"

	"github.com/dynamicgo/config"
	"github.com/dynamicgo/slf4go"
	"github.com/inwecrypto/sha3"
)

// Builtin method
const (
	KeepAlive = "/udptnnel/keepalive/0.0.1"
)

// udp tunnel defined errors
var (
	ErrPeerTimeout = errors.New("Peer keep-alive timeout")
	ErrListen      = errors.New("Tunnel already listen or connect")
)

// Peer .
type Peer interface {
	// Call peer handler with data
	Call(path string, data []byte) (int, error)

	Stop()
}

// Tunnel .
type Tunnel interface {
	Listen(addr *net.UDPAddr) error
	Handle(path string, handler TunnelFunc) Tunnel
	Peer(raddr *net.UDPAddr) (Peer, error)
	Context() context.Context
}

// TunnelFunc .
type TunnelFunc func(ctx context.Context, peer Peer, data []byte)

type tunnelImpl struct {
	sync.Mutex                          // listen locker
	slf4go.Logger                       // mixin logger
	conn          *net.UDPConn          // udp connection
	handlers      map[string]TunnelFunc // path handlers
	peers         map[string]Peer       // peer pool
	ctx           context.Context       // context
	keepAlive     time.Duration         // keepalive packet send duration
	recvbuffsize  int                   // max recv buff size
	handleTimeout time.Duration         // handle process timeout duration
}

// New create new server side udp tunnel
func New(ctx context.Context, conf *config.Config) Tunnel {
	tunnel := &tunnelImpl{
		Logger:        slf4go.Get("udptunnel"),
		handlers:      make(map[string]TunnelFunc),
		peers:         make(map[string]Peer),
		ctx:           ctx,
		keepAlive:     conf.GetDuration("udptunnel.keepalive", 10) * time.Second,
		recvbuffsize:  int(conf.GetInt64("udptunnel.buffsize", 1000)),
		handleTimeout: conf.GetDuration("udptunnel.handletimeout", 10) * time.Second,
	}

	return tunnel.Handle(KeepAlive, tunnel.handleKeepAlive)
}

func (tunnel *tunnelImpl) handleKeepAlive(ctx context.Context, peer Peer, data []byte) {

}

func (tunnel *tunnelImpl) Context() context.Context {
	return tunnel.ctx
}

func (tunnel *tunnelImpl) Listen(addr *net.UDPAddr) error {

	tunnel.Lock()
	defer tunnel.Unlock()

	return tunnel.listenWitoutLock(addr)
}

func (tunnel *tunnelImpl) listenWitoutLock(addr *net.UDPAddr) error {
	if tunnel.conn != nil {
		return ErrListen
	}

	conn, err := net.ListenUDP("udp", addr)

	if err != nil {
		return err
	}

	tunnel.conn = conn

	go tunnel.recvLoop()

	return nil
}

func (tunnel *tunnelImpl) Handle(path string, handler TunnelFunc) Tunnel {
	tunnel.Lock()
	defer tunnel.Unlock()

	hasher := sha3.NewKeccak256()
	hasher.Write([]byte(path))
	data := hasher.Sum(nil)

	tunnel.handlers[hex.EncodeToString(data[0:4])] = handler

	return tunnel
}

// Sign get method sign
func Sign(path string) string {
	hasher := sha3.NewKeccak256()
	hasher.Write([]byte(path))
	data := hasher.Sum(nil)

	return hex.EncodeToString(data[0:4])
}

func (tunnel *tunnelImpl) Peer(raddr *net.UDPAddr) (Peer, error) {
	tunnel.Lock()
	defer tunnel.Unlock()

	if tunnel.conn == nil {
		if err := tunnel.listenWitoutLock(&net.UDPAddr{IP: net.IPv4zero, Port: 0}); err != nil {
			return nil, err
		}
	}

	peer := tunnel.newPeer(raddr)

	tunnel.peers[raddr.String()] = peer

	go tunnel.recvLoop()

	return peer, nil
}

func (tunnel *tunnelImpl) recvLoop() {
	for {
		select {
		case <-tunnel.ctx.Done():
			if err := tunnel.conn.Close(); err != nil {
				tunnel.ErrorF("close tunnel conn err %s", err)
			}
			return
		default:
		}

		data := make([]byte, tunnel.recvbuffsize)

		len, raddr, err := tunnel.conn.ReadFromUDP(data)

		if err != nil {
			tunnel.ErrorF("tunnel recv err %s", err)
			continue
		}

		go tunnel.dispatch(data[:len], raddr)

	}
}

type packetHeader struct {
	ID     []byte // packet invoke id
	Length uint16 // packet length
}

func (tunnel *tunnelImpl) dispatch(data []byte, raddr *net.UDPAddr) {

	if len(data) < 6 {
		tunnel.ErrorF("recv invalid packet from %s with length %d", raddr, len(data))
		tunnel.TraceF("recv invalid packet:\n %s", hex.EncodeToString(data))
		return
	}

	header := &packetHeader{}

	header.ID = data[:4]

	buff := data[4:6]

	header.Length = binary.BigEndian.Uint16(buff)

	if int(header.Length+6) > len(data) {
		tunnel.ErrorF("recv invalid packet from %s with header length %d and recv length %d", raddr, header.Length, len(data))
		tunnel.TraceF("recv invalid packet:\n %s", hex.EncodeToString(data))
		return
	}

	data = data[6 : header.Length+6]

	id := hex.EncodeToString(header.ID)

	tunnel.Lock()
	defer tunnel.Unlock()

	peer, ok := tunnel.peers[raddr.String()]

	if !ok {
		peer = tunnel.newPeer(raddr)
		tunnel.peers[raddr.String()] = peer
	}

	handler, ok := tunnel.handlers[id]

	if !ok {
		tunnel.TraceF("recv unhandled request %s from %s", id, raddr)
		return
	}

	ctx := tunnel.ctx
	var cancel context.CancelFunc

	if tunnel.handleTimeout != 0 {
		ctx, cancel = context.WithTimeout(tunnel.ctx, tunnel.handleTimeout)
	}

	if cancel != nil {
		cancel = nil
	}

	go handler(ctx, peer, data)
}

type peerImpl struct {
	slf4go.Logger                    // mixin logger
	conn          *net.UDPConn       // peer conn
	cancel        context.CancelFunc // cancel func for keepalive goroutine
	ctx           context.Context
	addr          *net.UDPAddr
}

func (tunnel *tunnelImpl) newPeer(addr *net.UDPAddr) Peer {
	peer := &peerImpl{
		Logger: slf4go.Get("udptunnel-peer"),
		conn:   tunnel.conn,
		addr:   addr,
	}

	ctx, cancel := context.WithCancel(tunnel.ctx)

	peer.cancel = cancel
	peer.ctx = ctx

	go peer.keepAlive(ctx, tunnel.keepAlive)

	return peer
}

func (peer *peerImpl) keepAlive(ctx context.Context, duration time.Duration) {

	ticker := time.NewTicker(duration)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_, err := peer.Call(KeepAlive, nil)

			if err != nil {
				peer.ErrorF("send keepalive error %s", err)
				go peer.Stop()
			}
		}
	}
}

func (peer *peerImpl) Stop() {
	peer.cancel()
}

func (peer *peerImpl) Call(path string, data []byte) (int, error) {

	hasher := sha3.NewKeccak256()
	hasher.Write([]byte(path))

	id := hasher.Sum(nil)[0:4]

	length := make([]byte, 2)

	binary.BigEndian.PutUint16(length, uint16(len(data)))

	buff := append(id, length...)
	buff = append(buff, data...)

	return peer.conn.WriteToUDP(buff, peer.addr)
}
