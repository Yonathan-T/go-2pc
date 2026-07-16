package coordinator

import (
	"Two-Phase-Commit/participants"
	"Two-Phase-Commit/protocol"
	"Two-Phase-Commit/wal"
	"context"
	"fmt"
	"sync"
	"time"
)

// Coordinator orchestrates the Two-Phase Commit protocol.
type Coordinator struct {
	wal          *wal.WAL
	participants []participants.Participant
	timeout      time.Duration
}

// NewCoordinator creates a new 2PC Coordinator.
func NewCoordinator(w *wal.WAL, parts []participants.Participant, timeout time.Duration) *Coordinator {
	return &Coordinator{
		wal:          w,
		participants: parts,
		timeout:      timeout,
	}
}

// Begin starts a new transaction using the 2PC protocol.
func (c *Coordinator) Begin(tx protocol.Transaction) error {
	// 1. Log STARTED. This marks the beginning of the transaction.
	if err := c.wal.Append(wal.LogEntry{TxID: tx.ID, Event: "STARTED"}); err != nil {
		return fmt.Errorf("failed to log STARTED: %w", err)
	}

	// 2. Phase 1: Send PREPARE to all participants concurrently.
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()

	// We use a channel to collect votes.
	type voteResult struct {
		vote protocol.MessageType
		err  error
	}
	votes := make(chan voteResult, len(c.participants))

	for _, p := range c.participants {
		go func(part participants.Participant) {
			resCh := make(chan voteResult, 1)
			go func() {
				vote, err := part.Prepare(tx)
				resCh <- voteResult{vote: vote, err: err}
			}()

			select {
			case res := <-resCh:
				votes <- res
			case <-ctx.Done():
				votes <- voteResult{err: ctx.Err()}
			}
		}(p)
	}

	allYes := true
	for i := 0; i < len(c.participants); i++ {
		res := <-votes
		if res.err != nil || res.vote != protocol.VOTE_YES {
			allYes = false
		}
	}

	if allYes {
		if err := c.wal.Append(wal.LogEntry{TxID: tx.ID, Event: "GLOBAL_COMMIT"}); err != nil {
			return fmt.Errorf("failed to log GLOBAL_COMMIT: %w", err)
		}

		var wg sync.WaitGroup
		for _, p := range c.participants {
			wg.Add(1)
			go func(part participants.Participant) {
				defer wg.Done()
				_ = part.Commit(tx.ID)
			}(p)
		}
		wg.Wait()

		if err := c.wal.Append(wal.LogEntry{TxID: tx.ID, Event: "DONE"}); err != nil {
			return fmt.Errorf("failed to log DONE: %w", err)
		}

		return nil
	}

	if err := c.wal.Append(wal.LogEntry{TxID: tx.ID, Event: "GLOBAL_ABORT"}); err != nil {
		return fmt.Errorf("failed to log GLOBAL_ABORT: %w", err)
	}

	var wg sync.WaitGroup
	for _, p := range c.participants {
		wg.Add(1)
		go func(part participants.Participant) {
			defer wg.Done()
			_ = part.Abort(tx.ID)
		}(p)
	}
	wg.Wait()

	if err := c.wal.Append(wal.LogEntry{TxID: tx.ID, Event: "DONE"}); err != nil {
		return fmt.Errorf("failed to log DONE: %w", err)
	}

	return fmt.Errorf("transaction %s aborted", tx.ID)
}

func (c *Coordinator) Recover() error {
	entries, err := c.wal.ReadAll()
	if err != nil {
		return fmt.Errorf("failed to read WAL for recovery: %w", err)
	}

	lastState := make(map[string]string)
	for _, e := range entries {
		lastState[e.TxID] = e.Event
	}

	for txID, state := range lastState {
		switch state {
		case "STARTED":
			// Crashed during voting. z safe default is to abort.
			fmt.Printf("Recovering %s: STARTED -> ABORTING\n", txID)
			c.wal.Append(wal.LogEntry{TxID: txID, Event: "GLOBAL_ABORT"})
			for _, p := range c.participants {
				p.Abort(txID)
			}
			c.wal.Append(wal.LogEntry{TxID: txID, Event: "DONE"})

		case "GLOBAL_COMMIT":
			// Crashed while committing. so it Must ensure everyone commits.
			fmt.Printf("Recovering %s: GLOBAL_COMMIT -> Re-sending COMMIT\n", txID)
			for _, p := range c.participants {
				p.Commit(txID)
			}
			c.wal.Append(wal.LogEntry{TxID: txID, Event: "DONE"})

		case "GLOBAL_ABORT":
			// Crashed while aborting. must ensure everyone aborts.
			fmt.Printf("Recovering %s: GLOBAL_ABORT -> Re-sending ABORT\n", txID)
			for _, p := range c.participants {
				p.Abort(txID)
			}
			c.wal.Append(wal.LogEntry{TxID: txID, Event: "DONE"})

		case "DONE":
			//  nothing to do here bud.
		}
	}

	return nil
}
