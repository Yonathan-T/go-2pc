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
		&mockParticipant{vote: protocol.VOTE_NO}}

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
func TestCoordinator_CrashAfterGlobalCommit_RecoversData(t *testing.T) {
	dir := t.TempDir()
	coordWalPath := filepath.Join(dir, "coord.wal")
	p1WalPath := filepath.Join(dir, "p1.wal")
	p2WalPath := filepath.Join(dir, "p2.wal")

	w1, _ := wal.NewWAL(p1WalPath)
	p1 := participants.NewNode("P1", w1)
	defer w1.Close()

	w2, _ := wal.NewWAL(p2WalPath)
	p2 := participants.NewNode("P2", w2)
	defer w2.Close()

	tx := protocol.Transaction{
		ID:   "tx-500",
		Data: map[string]string{"server:status": "online"},
	}
	_, _ = p1.Prepare(tx)
	_, _ = p2.Prepare(tx)

	cw1, _ := wal.NewWAL(coordWalPath)
	_ = cw1.Append(wal.LogEntry{TxID: tx.ID, Event: "STARTED"})
	_ = cw1.Append(wal.LogEntry{TxID: tx.ID, Event: "GLOBAL_COMMIT"})
	cw1.Close()

	if p1.GetState("tx-500") != "PREPARED" || p2.GetState("tx-500") != "PREPARED" {
		t.Fatalf("expected participants to be PREPARED before recovery")
	}

	if _, ok := p1.GetData("server:status"); ok {
		t.Fatalf("data should NOT be visible before coordinator commits!")
	}

	cw2, _ := wal.NewWAL(coordWalPath)
	defer cw2.Close()

	coordRebooted := NewCoordinator(cw2, []participants.Participant{p1, p2}, 1*time.Second)
	if err := coordRebooted.Recover(); err != nil {
		t.Fatalf("coordinator recovery failed: %v", err)
	}

	if p1.GetState("tx-500") != "COMMITTED" {
		t.Errorf("expected P1 COMMITTED, got %s", p1.GetState("tx-500"))
	}
	if val, ok := p1.GetData("server:status"); !ok || val != "online" {
		t.Errorf("expected server:status=online on P1, got val=%s, ok=%v", val, ok)
	}
	if val, ok := p2.GetData("server:status"); !ok || val != "online" {
		t.Errorf("expected server:status=online on P2, got val=%s, ok=%v", val, ok)
	}
}
