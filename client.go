package main

import (
	"encoding/json"
	"fmt"
	"net"
)

type Message struct {
	ID      string   `json:"id"`
	Payload string   `json:"payload"`
	TTL     int      `json:"ttl"`
	Path    []string `json:"path"`
}

func main() {
	conn, err := net.Dial("tcp", "localhost:9000")
	if err != nil {
		fmt.Println("Failed to connect:", err)
		return
	}
	defer conn.Close()

	msg := Message{
		ID:      "msg-001",
		Payload: "Need help, trapped near river bridge",
		TTL:     5,
		Path:    []string{"NodeA"},
	}

	encoder := json.NewEncoder(conn)
	err = encoder.Encode(msg)
	if err != nil {
		fmt.Println("Encode error:", err)
		return
	}

	fmt.Println("Message sent to node")
}