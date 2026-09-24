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
	p.FailPrepare = true
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

	w, _ := wal.NewWAL(path)
	w.Append(wal.LogEntry{TxID: "tx3", Event: "PREPARED"})
	w.Close()

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
func TestParticipant_DataRecovery(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "p_data.wal")

	w1, _ := wal.NewWAL(path)
	p1 := NewNode("P1", w1)

	tx := protocol.Transaction{
		ID:   "tx-100",
		Data: map[string]string{"balance": "500", "currency": "USD"},
	}
	_, _ = p1.Prepare(tx)
	_ = p1.Commit("tx-100")
	w1.Close()

	w2, _ := wal.NewWAL(path)
	defer w2.Close()

	pRebooted := NewNode("P1-Rebooted", w2)
	if err := pRebooted.Recover(); err != nil {
		t.Fatalf("recovery failed: %v", err)
	}

	val, ok := pRebooted.GetData("balance")
	if !ok || val != "500" {
		t.Fatalf("expected balance=500 after recovery, got val=%s, ok=%v", val, ok)
	}

	curr, ok := pRebooted.GetData("currency")
	if !ok || curr != "USD" {
		t.Fatalf("expected currency=USD after recovery, got curr=%s, ok=%v", curr, ok)
	}
}
