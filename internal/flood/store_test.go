package flood

import "testing"

func TestCheckAndMarkRejectsExpiredTTL(t *testing.T) {
	store := NewSeenStore()

	ok, reason := store.CheckAndMark("msg-1", 0)
	if ok {
		t.Fatal("expected ttl-expired message to be rejected")
	}
	if reason != "ttl expired" {
		t.Fatalf("unexpected reason: %s", reason)
	}
}

func TestCheckAndMarkRejectsDuplicateMessage(t *testing.T) {
	store := NewSeenStore()

	ok, reason := store.CheckAndMark("msg-1", 3)
	if !ok || reason != "" {
		t.Fatalf("expected first message to be accepted, got ok=%v reason=%q", ok, reason)
	}

	ok, reason = store.CheckAndMark("msg-1", 3)
	if ok {
		t.Fatal("expected duplicate message to be rejected")
	}
	if reason != "already seen" {
		t.Fatalf("unexpected reason: %s", reason)
	}
}
