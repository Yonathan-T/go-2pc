package wal

import (
	"os"
	"path/filepath"
	"testing"
)

func TestAppendAndReadAll(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")

	w, err := NewWAL(path)
	if err != nil {
		t.Fatalf("failed to create WAL: %v", err)
	}

	entries := []LogEntry{
		{TxID: "tx-1", Event: "PREPARED"},
		{TxID: "tx-1", Event: "COMMITTED"},
		{TxID: "tx-2", Event: "PREPARED"},
		{TxID: "tx-2", Event: "ABORTED"},
	}

	for _, e := range entries {
		if err := w.Append(e); err != nil {
			t.Fatalf("failed to append entry: %v", err)
		}
	}

	w.Close()

	w2, err := NewWAL(path)
	if err != nil {
		t.Fatalf("failed to reopen WAL: %v", err)
	}
	defer w2.Close()

	got, err := w2.ReadAll()
	if err != nil {
		t.Fatalf("failed to read entries: %v", err)
	}

	if len(got) != len(entries) {
		t.Fatalf("expected %d entries, got %d", len(entries), len(got))
	}

	for i, entry := range got {
		if entry.TxID != entries[i].TxID {
			t.Errorf("entry %d: expected TxID %q, got %q", i, entries[i].TxID, entry.TxID)
		}
		if entry.Event != entries[i].Event {
			t.Errorf("entry %d: expected Event %q, got %q", i, entries[i].Event, entry.Event)
		}
		if entry.Timestamp.IsZero() {
			t.Errorf("entry %d: timestamp should not be zero", i)
		}
	}
}
func TestReadAllEmptyFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "empty.wal")

	w, err := NewWAL(path)
	if err != nil {
		t.Fatalf("failed to create WAL: %v", err)
	}
	defer w.Close()

	got, err := w.ReadAll()
	if err != nil {
		t.Fatalf("expected no error on empty WAL, got: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("expected 0 entries on empty WAL, got %d", len(got))
	}
}
func TestReadAllNoFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "doesnt_exist.wal")

	w := &WAL{path: path}

	got, err := w.ReadAll()
	if err != nil {
		t.Fatalf("expected no error on missing file, got: %v", err)
	}
	if got != nil {
		t.Fatalf("expected nil entries on missing file, got %d", len(got))
	}
}

func TestSyncActuallyPersists(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sync_test.wal")

	w, err := NewWAL(path)
	if err != nil {
		t.Fatalf("failed to create WAL: %v", err)
	}

	w.Append(LogEntry{TxID: "tx-99", Event: "PREPARED"})
	w.Close()

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("failed to read raw file: %v", err)
	}

	if len(raw) == 0 {
		t.Fatal("file is empty — Sync() didn't persist the entry")
	}
}
