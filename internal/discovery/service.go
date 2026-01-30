package discovery

import (
	"context"
	"encoding/json"
	"log"
	"net"
	"strings"
	"syscall"
	"time"

	"wavenet/internal/model"
)

type ServiceConfig struct {
	NodeName      string
	TCPPort       string
	DiscoveryPort string
	AnnounceEvery time.Duration
	PeerTTL       time.Duration
	Logger        *log.Logger
	OnPeerUp      func(Peer)
	OnPeerDown    func(Peer)
}

type Service struct {
	cfg   ServiceConfig
	peers *PeerStore
}

func NewService(cfg ServiceConfig, peers *PeerStore) *Service {
	return &Service{cfg: cfg, peers: peers}
}

func (s *Service) Run(ctx context.Context) error {
	errCh := make(chan error, 2)

	go func() { errCh <- s.listenLoop(ctx) }()
	go func() { errCh <- s.announceLoop(ctx) }()
	go s.cleanupLoop(ctx)

	select {
	case <-ctx.Done():
		return nil
	case err := <-errCh:
		return err
	}
}

func (s *Service) listenLoop(ctx context.Context) error {
	addr := &net.UDPAddr{IP: net.IPv4zero, Port: mustInt(s.cfg.DiscoveryPort)}
	listenConfig := net.ListenConfig{
		Control: func(network, address string, c syscall.RawConn) error {
			var controlErr error
			if err := c.Control(func(fd uintptr) {
				controlErr = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_REUSEADDR, 1)
				if controlErr != nil {
					return
				}
				controlErr = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_REUSEPORT, 1)
			}); err != nil {
				return err
			}
			return controlErr
		},
	}

	packetConn, err := listenConfig.ListenPacket(ctx, "udp4", addr.String())
	if err != nil {
		return err
	}
	conn := packetConn.(*net.UDPConn)
	defer conn.Close()

	buf := make([]byte, 2048)
	for {
		_ = conn.SetReadDeadline(time.Now().Add(1 * time.Second))
		n, remoteAddr, err := conn.ReadFromUDP(buf)
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				select {
				case <-ctx.Done():
					return nil
				default:
					continue
				}
			}
			return err
		}

		var ann model.DiscoveryAnnouncement
		if err := json.Unmarshal(buf[:n], &ann); err != nil {
			continue
		}

		if ann.Type != "announce" || ann.Name == s.cfg.NodeName {
			continue
		}

		host := remoteAddr.IP.String()
		if strings.TrimSpace(host) == "" {
			continue
		}

		if s.peers.Upsert(ann.Name, host, ann.TCPPort, time.Now()) && s.cfg.OnPeerUp != nil {
			s.cfg.OnPeerUp(Peer{Name: ann.Name, Address: net.JoinHostPort(host, ann.TCPPort), LastSeen: time.Now()})
		}
	}
}

func (s *Service) announceLoop(ctx context.Context) error {
	listenConfig := net.ListenConfig{
		Control: func(network, address string, c syscall.RawConn) error {
			var controlErr error
			if err := c.Control(func(fd uintptr) {
				controlErr = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_BROADCAST, 1)
			}); err != nil {
				return err
			}
			return controlErr
		},
	}

	packetConn, err := listenConfig.ListenPacket(ctx, "udp4", ":0")
	if err != nil {
		return err
	}
	conn := packetConn.(*net.UDPConn)
	defer conn.Close()

	targets := []*net.UDPAddr{
		{IP: net.IPv4bcast, Port: mustInt(s.cfg.DiscoveryPort)},
		{IP: net.ParseIP("127.0.0.1"), Port: mustInt(s.cfg.DiscoveryPort)},
	}
	warnedTargets := make(map[string]bool)

	ticker := time.NewTicker(s.cfg.AnnounceEvery)
	defer ticker.Stop()

	for {
		payload, _ := json.Marshal(model.DiscoveryAnnouncement{
			Type:    "announce",
			Name:    s.cfg.NodeName,
			TCPPort: s.cfg.TCPPort,
		})

		for _, target := range targets {
			if _, err := conn.WriteToUDP(payload, target); err != nil && s.cfg.Logger != nil {
				if !warnedTargets[target.String()] {
					s.cfg.Logger.Printf("[%s] discovery announce error to %s: %v", s.cfg.NodeName, target.String(), err)
					warnedTargets[target.String()] = true
				}
			}
		}

		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func (s *Service) cleanupLoop(ctx context.Context) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			removed := s.peers.RemoveExpired(s.cfg.PeerTTL)
			for _, peer := range removed {
				if s.cfg.OnPeerDown != nil {
					s.cfg.OnPeerDown(peer)
				}
			}
		}
	}
}

func mustInt(value string) int {
	port, _ := net.LookupPort("udp", value)
	return port
}
