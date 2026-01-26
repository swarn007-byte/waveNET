package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"time"

	"wavenet/internal/model"
)

func main() {
	target := flag.String("target", "localhost:9001", "TCP address of the first node to inject the message into.")
	id := flag.String("id", "sos-001", "Unique message ID.")
	payload := flag.String("payload", "Trapped near river bridge, need help", "Human-readable message text.")
	ttl := flag.Int("ttl", 5, "How many hops the message is allowed to take.")
	flag.Parse()

	conn, err := net.DialTimeout("tcp", *target, 2*time.Second)
	if err != nil {
		fmt.Println("Failed to connect:", err)
		return
	}
	defer conn.Close()

	msg := model.Message{
		ID:      *id,
		Payload: *payload,
		TTL:     *ttl,
		Path:    []string{"Client"},
	}

	if err := json.NewEncoder(conn).Encode(msg); err != nil {
		fmt.Println("Encode error:", err)
		return
	}

	fmt.Printf("Message %s sent to %s\n", msg.ID, *target)
}
