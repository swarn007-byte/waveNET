package main

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"strings"
)

type Message struct {
	ID      string   `json:"id"`
	Payload string   `json:"payload"`
	TTL     int      `json:"ttl"`
	Path    []string `json:"path"`
}

var (
	nodeName string
	peers    []string
	seen     = make(map[string]bool)
)

func main() {
	// go run node.go <name> <myPort> <peer1,peer2,peer3>
	nodeName = os.Args[1]
	myPort := os.Args[2]
	if len(os.Args) > 3 {
		peers = strings.Split(os.Args[3], ",")
	}

	listener, err := net.Listen("tcp", ":"+myPort)
	if err != nil {
		fmt.Println("Failed to listen:", err)
		return
	}
	defer listener.Close()
	fmt.Println(nodeName, "listening on port", myPort, "| peers:", peers)

	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	var msg Message
	if err := json.NewDecoder(conn).Decode(&msg); err != nil {
		return
	}

	fmt.Println(nodeName, "received", msg.ID, "TTL:", msg.TTL, "Path:", msg.Path)

	if msg.TTL <= 0 {
		fmt.Println(nodeName, "dropping (TTL expired)")
		return
	}
	if seen[msg.ID] {
		fmt.Println(nodeName, "dropping (already seen)")
		return
	}

	seen[msg.ID] = true
	msg.TTL--
	msg.Path = append(msg.Path, nodeName)

	for _, peer := range peers {
		go forward(peer, msg)
	}
}

func forward(peerAddr string, msg Message) {
	conn, err := net.Dial("tcp", peerAddr)
	if err != nil {
		fmt.Println(nodeName, "-> failed to reach", peerAddr)
		return
	}
	defer conn.Close()
	json.NewEncoder(conn).Encode(msg)
}