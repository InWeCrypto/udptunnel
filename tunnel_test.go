package udptunnel

import (
	"context"
	"net"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dynamicgo/config"
)

var (
	server Tunnel
	client Tunnel
)

var peer Peer

func init() {

	conf, err := config.New([]byte(`{}`))

	if err != nil {
		panic(err)
	}

	server = New(context.Background(), conf)

	server.Handle("/test", func(ctx context.Context, peer Peer, data []byte) {

	})

	err = server.Listen(&net.UDPAddr{IP: net.IPv4zero, Port: 1812})

	if err != nil {
		panic(err)
	}

	client = New(context.Background(), conf)

	peer, err = client.Peer(&net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 1812})

	if err != nil {
		panic(err)
	}

}

func TestKeepAliveSign(t *testing.T) {
	println(Sign("test"))
}

func TestUpdate(t *testing.T) {
	_, err := peer.Call("/test", []byte{1, 2})

	require.NoError(t, err)
}

func TestTimeout(t *testing.T) {

	ctx, cancel := context.WithCancel(context.Background())

	client.Handle(KeepAlive, func(ctx context.Context, peer Peer, data []byte) {
		cancel()
	})

	<-ctx.Done()

	client.Handle(KeepAlive, func(ctx context.Context, peer Peer, data []byte) {})
}
