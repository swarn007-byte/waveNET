package node

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"wavenet/internal/dashboard"
	"wavenet/internal/discovery"
	"wavenet/internal/flood"
	"wavenet/internal/gateway"
	"wavenet/internal/model"
)

type Config struct {
	Name          string
	TCPPort       string
	DiscoveryPort string
	AnnounceEvery time.Duration
	PeerTTL       time.Duration
	GatewayMode   bool
	GatewayLog    string
	SeedPeers     []string
	DashboardURL  string
}

// Node owns the TCP relay server plus discovery/background helpers.
type Node struct {
	cfg             Config
	logger          *log.Logger
	seen            *flood.SeenStore
	peers           *discovery.PeerStore
	gatewayLogger   *gateway.Logger
	dashboardClient *dashboard.Client
}

func New(cfg Config) *Node {
	logger := log.New(os.Stdout, "", 0)
	n := &Node{
		cfg:             cfg,
		logger:          logger,
		seen:            flood.NewSeenStore(),
		peers:           discovery.NewPeerStore(),
		dashboardClient: dashboard.NewClient(cfg.DashboardURL, logger),
	}

	if cfg.GatewayMode {
		n.gatewayLogger = gateway.NewLogger(cfg.GatewayLog)
	}

	for _, seed := range cfg.SeedPeers {
		n.peers.AddStatic("seed", hostPart(seed), portPart(seed))
	}

	return n
}

func (n *Node) Run(ctx context.Context) error {
	listener, err := net.Listen("tcp", ":"+n.cfg.TCPPort)
	if err != nil {
		return err
	}
	defer listener.Close()

	n.logger.Printf("[%s] listening for TCP messages on %s", n.cfg.Name, n.cfg.TCPPort)
	if len(n.cfg.SeedPeers) > 0 {
		n.logger.Printf("[%s] using seed peers: %s", n.cfg.Name, strings.Join(n.cfg.SeedPeers, ", "))
	}

	discoveryService := discovery.NewService(discovery.ServiceConfig{
		NodeName:      n.cfg.Name,
		TCPPort:       n.cfg.TCPPort,
		DiscoveryPort: n.cfg.DiscoveryPort,
		AnnounceEvery: n.cfg.AnnounceEvery,
		PeerTTL:       n.cfg.PeerTTL,
		Logger:        n.logger,
		OnPeerUp: func(peer discovery.Peer) {
			n.logger.Printf("[%s] discovered peer %s at %s", n.cfg.Name, peer.Name, peer.Address)
			n.sendEvent("peer_discovered", "", 0, nil, []string{peer.Address}, fmt.Sprintf("Discovered %s", peer.Address))
		},
		OnPeerDown: func(peer discovery.Peer) {
			n.logger.Printf("[%s] peer expired %s at %s", n.cfg.Name, peer.Name, peer.Address)
			n.sendEvent("peer_expired", "", 0, nil, []string{peer.Address}, fmt.Sprintf("Expired %s", peer.Address))
		},
	}, n.peers)

	go func() {
		if err := discoveryService.Run(ctx); err != nil {
			n.logger.Printf("[%s] discovery stopped: %v", n.cfg.Name, err)
		}
	}()

	var wg sync.WaitGroup
	go func() {
		<-ctx.Done()
		_ = listener.Close()
	}()

	for {
		conn, err := listener.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				wg.Wait()
				return nil
			default:
				continue
			}
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			n.handleConnection(conn)
		}()
	}
}

func (n *Node) handleConnection(conn net.Conn) {
	defer conn.Close()

	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))

	var msg model.Message
	if err := json.NewDecoder(conn).Decode(&msg); err != nil {
		n.logger.Printf("[%s] dropped unreadable message: %v", n.cfg.Name, err)
		return
	}

	n.logger.Printf("[%s] received %s | TTL=%d | path so far: %s", n.cfg.Name, msg.ID, msg.TTL, strings.Join(msg.Path, " -> "))

	shouldProcess, reason := n.seen.CheckAndMark(msg.ID, msg.TTL)
	if !shouldProcess {
		n.logger.Printf("[%s] dropping %s (%s)", n.cfg.Name, msg.ID, reason)
		n.sendEvent("dropped", msg.ID, msg.TTL, msg.Path, nil, reason)
		return
	}

	msg.TTL--
	msg.Path = append(msg.Path, n.cfg.Name)
	n.sendEvent("received", msg.ID, msg.TTL, msg.Path, nil, "Message accepted")

	if n.cfg.GatewayMode {
		n.logger.Printf("[%s][GATEWAY] %s delivered - simulated handoff to external service", n.cfg.Name, msg.ID)
		n.sendEvent("gateway_delivered", msg.ID, msg.TTL, msg.Path, nil, "Gateway recorded delivery")
		if err := n.gatewayLogger.Record(n.cfg.Name, msg); err != nil {
			n.logger.Printf("[%s] gateway log write failed: %v", n.cfg.Name, err)
		}
	}

	peers := n.peers.Addresses()
	if len(peers) == 0 {
		n.logger.Printf("[%s] no known peers to forward to - message %s stops here", n.cfg.Name, msg.ID)
		n.sendEvent("stopped", msg.ID, msg.TTL, msg.Path, nil, "No known peers")
		return
	}

	n.logger.Printf("[%s] forwarding %s to %s", n.cfg.Name, msg.ID, strings.Join(peers, ", "))
	n.sendEvent("forwarding", msg.ID, msg.TTL, msg.Path, peers, "Forwarding to peers")

	var forwarded int
	for _, peer := range peers {
		if samePort(peer, n.cfg.TCPPort) {
			continue
		}
		if err := n.forward(peer, msg); err == nil {
			forwarded++
		}
	}

	if forwarded == 0 {
		n.logger.Printf("[%s] no reachable peers to forward to - message %s stops here", n.cfg.Name, msg.ID)
		n.sendEvent("stopped", msg.ID, msg.TTL, msg.Path, peers, "No reachable peers")
	}
}

func (n *Node) forward(peerAddr string, msg model.Message) error {
	conn, err := net.DialTimeout("tcp", peerAddr, 2*time.Second)
	if err != nil {
		n.logger.Printf("[%s] failed to reach %s: %v", n.cfg.Name, peerAddr, err)
		n.sendEvent("forward_failed", msg.ID, msg.TTL, msg.Path, []string{peerAddr}, err.Error())
		return err
	}
	defer conn.Close()

	_ = conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	if err := json.NewEncoder(conn).Encode(msg); err != nil {
		n.logger.Printf("[%s] failed to send %s to %s: %v", n.cfg.Name, msg.ID, peerAddr, err)
		n.sendEvent("forward_failed", msg.ID, msg.TTL, msg.Path, []string{peerAddr}, err.Error())
		return err
	}

	return nil
}

func (n *Node) sendEvent(kind, messageID string, ttl int, path []string, peers []string, detail string) {
	n.dashboardClient.Send(model.DashboardEvent{
		Time:      time.Now().Format(time.RFC3339),
		Node:      n.cfg.Name,
		Kind:      kind,
		MessageID: messageID,
		TTL:       ttl,
		Path:      path,
		Peers:     peers,
		Detail:    detail,
	})
}

func hostPart(address string) string {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return address
	}
	return host
}

func portPart(address string) string {
	_, port, err := net.SplitHostPort(address)
	if err != nil {
		return ""
	}
	return port
}

func samePort(address, ownPort string) bool {
	_, port, err := net.SplitHostPort(address)
	if err != nil {
		return false
	}
	return port == ownPort
}

// PostDashboardEvent is separated so tests can exercise the event endpoint
// without starting the full browser UI.
func PostDashboardEvent(url string, event model.DashboardEvent) error {
	body, err := json.Marshal(event)
	if err != nil {
		return err
	}

	resp, err := http.Post(url, "application/json", strings.NewReader(string(body)))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	return nil
}
