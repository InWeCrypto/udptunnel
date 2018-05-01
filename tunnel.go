package udptunnel

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"io"
	"math"
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
	ErrPeerTimeout   = errors.New("Peer keep-alive timeout")
	ErrListen        = errors.New("Tunnel already listen or connect")
	ErrMessageLength = errors.New("message length error")
)

// Message .
type Message struct {
	ID   [4]byte
	Body []byte
}

// id
var (
	KeepAliveID = IDFromString(KeepAlive)
)

// IDFromString .
func IDFromString(id string) (result [4]byte) {
	hasher := sha3.NewKeccak256()
	hasher.Write([]byte(id))
	data := hasher.Sum(nil)

	copy(result[:], data[0:4])

	return
}

// IDFromString .
func (message *Message) IDFromString(id string) {
	hasher := sha3.NewKeccak256()
	hasher.Write([]byte(id))
	data := hasher.Sum(nil)

	copy(message.ID[:], data[0:4])
}

// Marshal .
func (message *Message) Marshal(writer io.Writer) error {

	length := make([]byte, 2)

	body := message.Body

	if len(body)+6 > math.MaxUint16 {
		body = body[:math.MaxUint16-6]
	}

	_, err := writer.Write(message.ID[:])

	if err != nil {
		return err
	}

	binary.BigEndian.PutUint16(length, uint16(len(body)))

	_, err = writer.Write(length)

	if err != nil {
		return err
	}

	_, err = writer.Write(body)

	if err != nil {
		return err
	}

	return nil
}

// Unmarshal .
func (message *Message) Unmarshal(reader io.Reader) error {

	_, err := reader.Read(message.ID[:])

	if err != nil {
		return err
	}

	buff := make([]byte, 2)

	_, err = reader.Read(buff)

	if err != nil {
		return err
	}

	length := binary.BigEndian.Uint16(buff)

	if length+6 > math.MaxUint16 {
		return ErrMessageLength
	}

	return nil
}

//Tunnel udp tunnel interface
type Tunnel interface {
	Remote() *net.UDPAddr
	Context() context.Context
	Send(message *Message) error
	Recv() <-chan *Message
}

// Server Tunnel server endpoint
type Server interface {
	Close()
	Accept() <-chan Tunnel
}

type serverImpl struct {
	*tunnelManager
	cancel context.CancelFunc
}

//ListenAndServe create new server
func ListenAndServe(ctx context.Context, conf *config.Config) (Server, error) {

	laddr, err := net.ResolveUDPAddr("udp", conf.GetString("udptunnel.laddr", ":1812"))

	if err != nil {
		return nil, err
	}

	conn, err := net.ListenUDP("udp", laddr)

	if err != nil {
		return nil, err
	}

	sctx, cancel := context.WithCancel(ctx)

	tm := newTunnelManager(sctx,
		conn,
		conf.GetDuration("udptunnel.heatbeat", 5)*time.Second,
		int(conf.GetInt64("udptunnel.recv.qsize", 100)),
		int(conf.GetInt64("udptunnel.recv.buffsize", 1024)),
		int(conf.GetInt64("udptunnel.notify.qsize", 1024)),
	)

	return &serverImpl{
		tunnelManager: tm,
		cancel:        cancel,
	}, nil
}

func (impl *serverImpl) Close() {
	impl.cancel()
}

func (impl *serverImpl) Accept() <-chan Tunnel {
	return impl.notify
}

// Client tunnel client endpoint
type Client interface {
	Close()
	Connect(raddr string) (Tunnel, error)
}

type clientImpl struct {
	*tunnelManager
	cancel context.CancelFunc
}

//NewClient .
func NewClient(ctx context.Context, conf *config.Config) (Client, error) {
	laddr, err := net.ResolveUDPAddr("udp", ":1812")

	if err != nil {
		return nil, err
	}

	laddr.Port = 0

	conn, err := net.ListenUDP("udp", laddr)

	if err != nil {
		return nil, err
	}

	sctx, cancel := context.WithCancel(ctx)

	tm := newTunnelManager(sctx,
		conn,
		conf.GetDuration("udptunnel.heatbeat", 5)*time.Second,
		int(conf.GetInt64("udptunnel.recv.qsize", 100)),
		int(conf.GetInt64("udptunnel.recv.buffsize", 1024)),
		int(conf.GetInt64("udptunnel.notify.qsize", 1024)),
	)

	return &clientImpl{
		tunnelManager: tm,
		cancel:        cancel,
	}, nil
}

func (impl *clientImpl) Close() {
	impl.cancel()
}

func (impl *clientImpl) Connect(raddr string) (Tunnel, error) {
	addr, err := net.ResolveUDPAddr("udp", raddr)
	if err != nil {
		return nil, err
	}

	tunnel, _ := impl.Get(addr)

	return tunnel, nil
}

type tunnelManager struct {
	sync.RWMutex
	slf4go.Logger
	tunnels      map[string]Tunnel
	ctx          context.Context
	conn         *net.UDPConn
	recvqsize    int
	recvbuffsize int
	heatBeat     time.Duration
	notify       chan Tunnel
}

func newTunnelManager(
	ctx context.Context,
	conn *net.UDPConn,
	heatBeat time.Duration,
	recvqsize int,
	recvbuffsize int,
	notifyqsize int) *tunnelManager {

	tunnelManager := &tunnelManager{
		Logger:       slf4go.Get("tunnelmg"),
		ctx:          ctx,
		conn:         conn,
		tunnels:      make(map[string]Tunnel),
		recvqsize:    recvqsize,
		heatBeat:     heatBeat,
		recvbuffsize: recvbuffsize,
	}

	if notifyqsize >= 0 {
		tunnelManager.notify = make(chan Tunnel, notifyqsize)
	}

	go tunnelManager.recvLoop()

	return tunnelManager
}

func (tm *tunnelManager) recvLoop() {
	go func() {
		<-tm.ctx.Done()
		tm.conn.Close()
	}()

	for {

		buff := make([]byte, tm.recvbuffsize)

		length, raddr, err := tm.conn.ReadFromUDP(buff)

		if err != nil {
			tm.ErrorF("recv message :%s", err)
			return
		}

		buff = buff[:length]

		message := &Message{}

		if err := message.Unmarshal(bytes.NewBuffer(buff)); err != nil {
			tm.ErrorF("unmarshal message from %s err :%s", raddr, err)
			tm.TraceF("invalid message data :%s", hex.EncodeToString(buff))
			continue
		}

		if bytes.Equal(message.ID[:], KeepAliveID[:]) {
			tm.DebugF("recv heatbeat from %s", raddr)
			continue
		}

		tunnel, ok := tm.Get(raddr)

		if tm.notify != nil && ok {
			tm.notify <- tunnel
		}

		tunnel.(*tunnelImpl).recvq <- message
	}
}

func (tm *tunnelManager) Get(raddr *net.UDPAddr) (Tunnel, bool) {
	tm.RLock()
	tunnel, ok := tm.tunnels[raddr.String()]
	tm.RUnlock()

	if !ok {
		tunnel = tm.makeTunnel(raddr, tm.recvqsize)
		tm.Lock()
		tm.tunnels[raddr.String()] = tunnel
		tm.Unlock()
	}

	return tunnel, !ok
}

func (tm *tunnelManager) Del(raddr *net.UDPAddr) {
	tm.Lock()
	defer tm.Unlock()
	delete(tm.tunnels, raddr.String())
}

type tunnelImpl struct {
	conn  *net.UDPConn
	ctx   context.Context
	raddr *net.UDPAddr
	recvq chan *Message
	tm    *tunnelManager
}

func (tm *tunnelManager) makeTunnel(raddr *net.UDPAddr, recvqszie int) Tunnel {

	tunnel := &tunnelImpl{
		conn:  tm.conn,
		ctx:   tm.ctx,
		raddr: raddr,
		recvq: make(chan *Message, recvqszie),
		tm:    tm,
	}

	go tunnel.heatBeat(tm.heatBeat)

	return tunnel
}

func (tunnel *tunnelImpl) heatBeat(duration time.Duration) {

	ticker := time.NewTicker(duration)
	defer ticker.Stop()

	for {
		select {
		case <-tunnel.ctx.Done():
			tunnel.tm.Del(tunnel.raddr)
			return
		case <-ticker.C:
			tunnel.sendHeatBeat()
		}
	}
}

func (tunnel *tunnelImpl) sendHeatBeat() error {
	message := &Message{}

	message.IDFromString(KeepAlive)

	return tunnel.Send(message)
}

func (tunnel *tunnelImpl) Remote() *net.UDPAddr {
	return tunnel.raddr
}

func (tunnel *tunnelImpl) Context() context.Context {
	return tunnel.ctx
}

func (tunnel *tunnelImpl) Send(message *Message) error {
	var buff bytes.Buffer
	err := message.Marshal(&buff)

	if err != nil {
		return err
	}

	_, err = tunnel.conn.WriteToUDP(buff.Bytes(), tunnel.raddr)

	return err
}

func (tunnel *tunnelImpl) Recv() <-chan *Message {
	return tunnel.recvq
}
