package wal

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"
)

type LogEntry struct {
	TxID      string    `json:"tx_id"`
	Event     string    `json:"event"`
	Timestamp time.Time `json:"timestamp"`
}

type WAL struct {
	mu   sync.Mutex
	file *os.File
	path string
}

func NewWAL(path string) (*WAL, error) {
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL file: %w", err)
	}
	return &WAL{
		file: file,
		path: path,
	}, nil
}

func (w *WAL) Append(entry LogEntry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if entry.Timestamp.IsZero() {
		entry.Timestamp = time.Now()
	}

	data, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("failed to marshal log entry: %w", err)
	}

	if _, err := w.file.Write(append(data, '\n')); err != nil {
		return fmt.Errorf("failed to write to WAL: %w", err)
	}
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync WAL: %w", err)
	}

	return nil
}

func (w *WAL) ReadAll() ([]LogEntry, error) {
	file, err := os.Open(w.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to open WAL for reading: %w", err)
	}
	defer file.Close()

	var entries []LogEntry
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var entry LogEntry
		if err := json.Unmarshal(line, &entry); err != nil {
			return nil, fmt.Errorf("failed to parse log entry: %w", err)
		}

		entries = append(entries, entry)
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("failed to read WAL: %w", err)
	}

	return entries, nil
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.file.Close()
}
