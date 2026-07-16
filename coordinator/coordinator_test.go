package coordinator

import (
	"Two-Phase-Commit/participants"
	"Two-Phase-Commit/protocol"
	"Two-Phase-Commit/wal"
	"path/filepath"
	"testing"
	"time"
)

type mockParticipant struct {
	vote    protocol.MessageType
	prepErr error
}

func (m *mockParticipant) Prepare(tx protocol.Transaction) (protocol.MessageType, error) {
	time.Sleep(10 * time.Millisecond)
	return m.vote, m.prepErr
}

func (m *mockParticipant) Commit(txID string) error {
	return nil
}

func (m *mockParticipant) Abort(txID string) error {
	return nil
}

func TestCoordinator_HappyPath(t *testing.T) {
	dir := t.TempDir()
	w, _ := wal.NewWAL(filepath.Join(dir, "coord.wal"))
	defer w.Close()

	parts := []participants.Participant{
		&mockParticipant{vote: protocol.VOTE_YES},
		&mockParticipant{vote: protocol.VOTE_YES},
	}

	c := NewCoordinator(w, parts, 1*time.Second)

	err := c.Begin(protocol.Transaction{ID: "tx1", Payload: "test"})
	if err != nil {
		t.Fatalf("expected successful commit, got error: %v", err)
	}

	entries, _ := w.ReadAll()
	if len(entries) != 3 {
		t.Fatalf("expected 3 WAL entries (STARTED, GLOBAL_COMMIT, DONE), got %d", len(entries))
	}
	if entries[0].Event != "STARTED" || entries[1].Event != "GLOBAL_COMMIT" || entries[2].Event != "DONE" {
		t.Errorf("unexpected WAL sequence: %v", entries)
	}
}

func TestCoordinator_OneNoVote_Aborts(t *testing.T) {
	dir := t.TempDir()
	w, _ := wal.NewWAL(filepath.Join(dir, "coord.wal"))
	defer w.Close()

	parts := []participants.Participant{
		&mockParticipant{vote: protocol.VOTE_YES},
		&mockParticipant{vote: protocol.VOTE_NO}, // should trigger an abort
	}

	c := NewCoordinator(w, parts, 1*time.Second)

	err := c.Begin(protocol.Transaction{ID: "tx2", Payload: "test"})
	if err == nil {
		t.Fatalf("expected abort error, got nil")
	}

	entries, _ := w.ReadAll()
	if len(entries) != 3 {
		t.Fatalf("expected 3 WAL entries, got %d", len(entries))
	}
	if entries[1].Event != "GLOBAL_ABORT" {
		t.Errorf("expected GLOBAL_ABORT, got %s", entries[1].Event)
	}
}

func TestCoordinator_Timeout_Aborts(t *testing.T) {
	dir := t.TempDir()
	w, _ := wal.NewWAL(filepath.Join(dir, "coord.wal"))
	defer w.Close()

	slowPart := &mockParticipant{vote: protocol.VOTE_YES}

	parts := []participants.Participant{
		&slowWrapper{slowPart, 50 * time.Millisecond},
	}

	c := NewCoordinator(w, parts, 10*time.Millisecond)

	err := c.Begin(protocol.Transaction{ID: "tx3", Payload: "test"})
	if err == nil {
		t.Fatalf("expected timeout/abort error, got nil")
	}

	entries, _ := w.ReadAll()
	if len(entries) < 2 || entries[1].Event != "GLOBAL_ABORT" {
		t.Errorf("expected GLOBAL_ABORT due to timeout, got sequence: %v", entries)
	}
}

type slowWrapper struct {
	p     *mockParticipant
	delay time.Duration
}

func (s *slowWrapper) Prepare(tx protocol.Transaction) (protocol.MessageType, error) {
	time.Sleep(s.delay)
	return s.p.Prepare(tx)
}

func (s *slowWrapper) Commit(txID string) error { return nil }
func (s *slowWrapper) Abort(txID string) error  { return nil }

func TestCoordinator_Recovery(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "coord.wal")

	w, _ := wal.NewWAL(path)
	w.Append(wal.LogEntry{TxID: "tx4", Event: "STARTED"})
	w.Close()

	w2, _ := wal.NewWAL(path)
	defer w2.Close()
	c := NewCoordinator(w2, []participants.Participant{}, 1*time.Second)

	err := c.Recover()
	if err != nil {
		t.Fatalf("recovery failed: %v", err)
	}

	entries, _ := w2.ReadAll()
	if len(entries) != 3 {
		t.Fatalf("expected 3 entries after recovery, got %d", len(entries))
	}
	if entries[1].Event != "GLOBAL_ABORT" || entries[2].Event != "DONE" {
		t.Errorf("expected GLOBAL_ABORT and DONE, got %s, %s", entries[1].Event, entries[2].Event)
	}
}
