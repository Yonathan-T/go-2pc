package participants

import (
	"Two-Phase-Commit/protocol"
	"Two-Phase-Commit/wal"
	"path/filepath"
	"testing"
)

func TestParticipant_HappyPath(t *testing.T) {
	dir := t.TempDir()
	w, _ := wal.NewWAL(filepath.Join(dir, "p1.wal"))
	defer w.Close()

	p := NewNode("P1", w)
	tx := protocol.Transaction{ID: "tx1"}

	vote, err := p.Prepare(tx)
	if err != nil || vote != protocol.VOTE_YES {
		t.Fatalf("expected VOTE_YES, got %v (err: %v)", vote, err)
	}

	err = p.Commit("tx1")
	if err != nil {
		t.Fatalf("expected commit success, got: %v", err)
	}

	if p.GetState("tx1") != "COMMITTED" {
		t.Fatalf("expected COMMITTED state, got %s", p.GetState("tx1"))
	}
}

func TestParticipant_FailPrepare(t *testing.T) {
	dir := t.TempDir()
	w, _ := wal.NewWAL(filepath.Join(dir, "p2.wal"))
	defer w.Close()

	p := NewNode("P2", w)
	p.FailPrepare = true // Simulate a failure condition

	tx := protocol.Transaction{ID: "tx2"}

	vote, err := p.Prepare(tx)
	if err != nil || vote != protocol.VOTE_NO {
		t.Fatalf("expected VOTE_NO, got %v (err: %v)", vote, err)
	}

	if p.GetState("tx2") != "ABORTED" {
		t.Fatalf("expected ABORTED state, got %s", p.GetState("tx2"))
	}
}

func TestParticipant_Recovery(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "p3.wal")
	
	// Pre-populate WAL 
	w, _ := wal.NewWAL(path)
	w.Append(wal.LogEntry{TxID: "tx3", Event: "PREPARED"})
	w.Close()

	// Recover
	w2, _ := wal.NewWAL(path)
	defer w2.Close()

	p := NewNode("P3", w2)
	err := p.Recover()
	if err != nil {
		t.Fatalf("recovery failed: %v", err)
	}

	if p.GetState("tx3") != "PREPARED" {
		t.Fatalf("expected tx3 to recover to PREPARED, got %s", p.GetState("tx3"))
	}
}
