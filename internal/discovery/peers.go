package discovery

import (
	"fmt"
	"slices"
	"sync"
	"time"
)

// Peer describes one live TCP endpoint discovered on the LAN.
type Peer struct {
	Name     string
	Address  string
	LastSeen time.Time
	Static   bool
}

// PeerStore keeps peer discovery state safe across goroutines.
type PeerStore struct {
	mu    sync.RWMutex
	peers map[string]Peer
}

func NewPeerStore() *PeerStore {
	return &PeerStore{
		peers: make(map[string]Peer),
	}
}

// Upsert adds a new peer or refreshes an existing peer's last-seen time.
// It returns true only when this is the first time we have seen that address.
func (p *PeerStore) Upsert(name, host, tcpPort string, seenAt time.Time) bool {
	return p.upsert(name, host, tcpPort, seenAt, false)
}

func (p *PeerStore) AddStatic(name, host, tcpPort string) bool {
	return p.upsert(name, host, tcpPort, time.Now(), true)
}

func (p *PeerStore) upsert(name, host, tcpPort string, seenAt time.Time, static bool) bool {
	address := fmt.Sprintf("%s:%s", host, tcpPort)

	p.mu.Lock()
	defer p.mu.Unlock()

	existing, existed := p.peers[address]
	if existed && existing.Static {
		static = true
	}

	p.peers[address] = Peer{
		Name:     name,
		Address:  address,
		LastSeen: seenAt,
		Static:   static,
	}

	return !existed
}

// RemoveExpired deletes peers that have stopped announcing themselves.
func (p *PeerStore) RemoveExpired(maxAge time.Duration) []Peer {
	cutoff := time.Now().Add(-maxAge)

	p.mu.Lock()
	defer p.mu.Unlock()

	var removed []Peer
	for address, peer := range p.peers {
		if peer.Static {
			continue
		}
		if peer.LastSeen.Before(cutoff) {
			removed = append(removed, peer)
			delete(p.peers, address)
		}
	}

	return removed
}

// Addresses returns a stable snapshot of current peers.
func (p *PeerStore) Addresses() []string {
	p.mu.RLock()
	defer p.mu.RUnlock()

	addresses := make([]string, 0, len(p.peers))
	for _, peer := range p.peers {
		addresses = append(addresses, peer.Address)
	}
	slices.Sort(addresses)
	return addresses
}

func (p *PeerStore) Has(address string) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()
	_, ok := p.peers[address]
	return ok
}
