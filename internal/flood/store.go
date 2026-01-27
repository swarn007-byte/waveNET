package flood

import "sync"

// SeenStore tracks which message IDs this node has already processed.
//
// Flooding algorithms naturally create duplicate deliveries because multiple
// neighbors may send us the same message. This store prevents re-processing.
type SeenStore struct {
	mu   sync.Mutex
	seen map[string]bool
}

func NewSeenStore() *SeenStore {
	return &SeenStore{
		seen: make(map[string]bool),
	}
}

// CheckAndMark returns whether the message should continue through this node.
//
// The function is written as one critical section so the "check" and "mark"
// happen atomically, which avoids races when multiple goroutines receive the
// same message at nearly the same time.
func (s *SeenStore) CheckAndMark(messageID string, ttl int) (bool, string) {
	if ttl <= 0 {
		return false, "ttl expired"
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.seen[messageID] {
		return false, "already seen"
	}

	s.seen[messageID] = true
	return true, ""
}
