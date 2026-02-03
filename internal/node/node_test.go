package node

import (
	"encoding/json"
	"net"
	"testing"

	"wavenet/internal/model"
)

func TestHandleConnectionProcessesMessageWithNetPipe(t *testing.T) {
	n := New(Config{
		Name:          "A",
		TCPPort:       "9001",
		DiscoveryPort: "9999",
	})

	serverConn, clientConn := net.Pipe()
	defer clientConn.Close()

	done := make(chan struct{})
	go func() {
		defer close(done)
		n.handleConnection(serverConn)
	}()

	msg := model.Message{
		ID:      "msg-1",
		Payload: "hello",
		TTL:     3,
		Path:    []string{"Client"},
	}

	if err := json.NewEncoder(clientConn).Encode(msg); err != nil {
		t.Fatalf("encode failed: %v", err)
	}
	clientConn.Close()
	<-done

	ok, reason := n.seen.CheckAndMark("msg-1", 3)
	if ok {
		t.Fatal("expected message to already be marked as seen")
	}
	if reason != "already seen" {
		t.Fatalf("unexpected reason after processing: %s", reason)
	}
}
