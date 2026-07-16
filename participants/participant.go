package participants

import (
	"Two-Phase-Commit/protocol"
	"Two-Phase-Commit/wal"
	"fmt"
	"sync"
)

type Node struct {
	id          string
	wal         *wal.WAL
	mu          sync.Mutex
	state       map[string]string //transaction states: PREPARED, COMMITTED, ABORTED
	FailPrepare bool
}

func NewNode(id string, w *wal.WAL) *Node {
	return &Node{
		id:    id,
		wal:   w,
		state: make(map[string]string),
	}
}

func (p *Node) Prepare(tx protocol.Transaction) (protocol.MessageType, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.FailPrepare {
		if err := p.wal.Append(wal.LogEntry{TxID: tx.ID, Event: "ABORTED"}); err != nil {
			return protocol.VOTE_NO, err
		}
		p.state[tx.ID] = "ABORTED"
		return protocol.VOTE_NO, nil
	}

	if err := p.wal.Append(wal.LogEntry{TxID: tx.ID, Event: "PREPARED"}); err != nil {
		return protocol.VOTE_NO, fmt.Errorf("failed to write WAL: %w", err)
	}

	p.state[tx.ID] = "PREPARED"
	return protocol.VOTE_YES, nil
}

func (p *Node) Commit(txID string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.state[txID] == "COMMITTED" {
		return nil
	}

	if p.state[txID] != "PREPARED" {
		return fmt.Errorf("participant %s cannot commit tx %s from state %s", p.id, txID, p.state[txID])
	}

	if err := p.wal.Append(wal.LogEntry{TxID: txID, Event: "COMMITTED"}); err != nil {
		return fmt.Errorf("failed to log COMMITTED: %w", err)
	}

	fmt.Printf("[%s] Applied transaction %s successfully!\n", p.id, txID)

	p.state[txID] = "COMMITTED"
	return nil
}

func (p *Node) Abort(txID string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.state[txID] == "ABORTED" {
		return nil
	}

	if err := p.wal.Append(wal.LogEntry{TxID: txID, Event: "ABORTED"}); err != nil {
		return fmt.Errorf("failed to log ABORTED: %w", err)
	}

	fmt.Printf("[%s] Rolled back transaction %s.\n", p.id, txID)

	p.state[txID] = "ABORTED"
	return nil
}

func (p *Node) Recover() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	entries, err := p.wal.ReadAll()
	if err != nil {
		return fmt.Errorf("failed to read WAL: %w", err)
	}

	for _, entry := range entries {
		p.state[entry.TxID] = entry.Event
	}

	for txID, state := range p.state {
		if state == "PREPARED" {
			fmt.Printf("[%s] RECOVERY: Transaction %s is UNCERTAIN (PREPARED but no decision). Needs coordinator.\n", p.id, txID)
		}
	}

	return nil
}

func (p *Node) GetState(txID string) string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.state[txID]
}
