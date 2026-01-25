package model

// Message is the payload that travels through the WaveNET mesh.
//
// We keep the structure small and JSON-friendly so nodes can send it over
// ordinary TCP connections without any extra libraries.
type Message struct {
	ID      string   `json:"id"`
	Payload string   `json:"payload"`
	TTL     int      `json:"ttl"`
	Path    []string `json:"path"`
}

// DiscoveryAnnouncement is the small UDP packet nodes broadcast so other
// devices on the same LAN can learn how to contact them over TCP.
type DiscoveryAnnouncement struct {
	Type    string `json:"type"`
	Name    string `json:"name"`
	TCPPort string `json:"tcp_port"`
}

// DashboardEvent is sent from nodes to the optional dashboard process so
// message flow is easy to watch in a browser.
type DashboardEvent struct {
	Time      string   `json:"time"`
	Node      string   `json:"node"`
	Kind      string   `json:"kind"`
	MessageID string   `json:"message_id"`
	TTL       int      `json:"ttl"`
	Path      []string `json:"path,omitempty"`
	Peers     []string `json:"peers,omitempty"`
	Detail    string   `json:"detail"`
}
