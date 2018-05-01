package udptunnel

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dynamicgo/config"
)

var client Client
var server Server

func init() {
	conf, err := config.New([]byte(`{}`))

	if err != nil {
		panic(err)
	}

	server, err = ListenAndServe(context.Background(), conf)

	if err != nil {
		panic(err)
	}

	client, err = NewClient(context.Background(), conf)

	if err != nil {
		panic(err)
	}

	go func() {
		for tunnel := range server.Accept() {
			go func() {
				for message := range tunnel.Recv() {
					println("recv message")

					if err := tunnel.Send(message); err != nil {
						panic(err)
					}
				}
			}()
		}
	}()
}

func TestSend(t *testing.T) {
	tunnel, err := client.Connect("127.0.0.1:1812")

	require.NoError(t, err)

	msg := &Message{}
	msg.IDFromString("test")

	tunnel.Send(msg)

	msg2 := <-tunnel.Recv()

	require.Equal(t, msg, msg2)
}
