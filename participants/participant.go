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
	state       map[string]string
	data        map[string]string
	staging     map[string]map[string]string
	stagingOps  map[string]protocol.Operation
	FailPrepare bool
}

func NewNode(id string, w *wal.WAL) *Node {
	return &Node{
		id:         id,
		wal:        w,
		state:      make(map[string]string),
		data:       make(map[string]string),
		staging:    make(map[string]map[string]string),
		stagingOps: make(map[string]protocol.Operation),
	}
}

func (p *Node) Prepare(tx protocol.Transaction) (protocol.MessageType, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.FailPrepare {
		if err := p.wal.Append(wal.LogEntry{
			TxID:  tx.ID,
			Event: "ABORTED",
		}); err != nil {
			return protocol.VOTE_NO, err
		}
		p.state[tx.ID] = "ABORTED"
		return protocol.VOTE_NO, nil
	}

	var op *protocol.Operation
	if tx.Payload != "" {
		parsedOp, err := protocol.ParseCommand(tx.Payload)
		if err != nil {
			return protocol.VOTE_NO, err
		}
		p.stagingOps[tx.ID] = parsedOp
		op = &parsedOp
	}

	if len(tx.Data) > 0 {
		p.staging[tx.ID] = make(map[string]string)
		for key, value := range tx.Data {
			p.staging[tx.ID][key] = value
		}
	}

	if err := p.wal.Append(wal.LogEntry{
		TxID:  tx.ID,
		Event: "PREPARED",
		Op:    op,
		Data:  tx.Data,
	}); err != nil {
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

	if err := p.wal.Append(wal.LogEntry{
		TxID:  txID,
		Event: "COMMITTED",
	}); err != nil {
		return fmt.Errorf("failed to log COMMITTED: %w", err)
	}

	if op, exists := p.stagingOps[txID]; exists {
		switch op.Type {
		case protocol.OpSet:
			p.data[op.Key] = op.Val
		case protocol.OpDel:
			delete(p.data, op.Key)
		}
		delete(p.stagingOps, txID)
	}

	if stagedChanges, exists := p.staging[txID]; exists {
		for key, value := range stagedChanges {
			p.data[key] = value
		}
		delete(p.staging, txID)
	}

	fmt.Printf("[%s] Applied transaction %s successfully! current DB state: %v\n", p.id, txID, p.data)

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

	delete(p.stagingOps, txID)
	delete(p.staging, txID)

	fmt.Printf("[%s] Rolled back transaction %s. Staged changes discarded.\n", p.id, txID)

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

		switch entry.Event {
		case "PREPARED":
			if entry.Op != nil {
				p.stagingOps[entry.TxID] = *entry.Op
			}
			if len(entry.Data) > 0 {
				p.staging[entry.TxID] = make(map[string]string)
				for k, v := range entry.Data {
					p.staging[entry.TxID][k] = v
				}
			}
		case "COMMITTED":
			if op, exists := p.stagingOps[entry.TxID]; exists {
				switch op.Type {
				case protocol.OpSet:
					p.data[op.Key] = op.Val
				case protocol.OpDel:
					delete(p.data, op.Key)
				}
				delete(p.stagingOps, entry.TxID)
			}
			if staged, exists := p.staging[entry.TxID]; exists {
				for k, v := range staged {
					p.data[k] = v
				}
				delete(p.staging, entry.TxID)
			}
		case "ABORTED":
			delete(p.stagingOps, entry.TxID)
			delete(p.staging, entry.TxID)
		}
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

func (p *Node) GetData(key string) (string, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	val, exists := p.data[key]
	return val, exists
}

func (p *Node) GetAllData() map[string]string {
	p.mu.Lock()
	defer p.mu.Unlock()
	data := make(map[string]string)
	for k, v := range p.data {
		data[k] = v
	}
	return data
}
