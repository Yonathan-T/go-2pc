package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"Two-Phase-Commit/coordinator"
	"Two-Phase-Commit/participants"
	"Two-Phase-Commit/protocol"
	"Two-Phase-Commit/wal"
)

func main() {
	fmt.Println("Starting 2PC Demonstration...")

	if err := os.MkdirAll("logs", 0755); err != nil {
		log.Fatalf("failed to create logs directory: %v", err)
	}

	coordWAL, err := wal.NewWAL("logs/coordinator.wal")
	if err != nil {
		log.Fatalf("failed to create coord WAL: %v", err)
	}
	defer coordWAL.Close()

	p1WAL, err := wal.NewWAL("logs/participant_1.wal")
	if err != nil {
		log.Fatalf("failed to create p1 WAL: %v", err)
	}
	defer p1WAL.Close()

	p2WAL, err := wal.NewWAL("logs/participant_2.wal")
	if err != nil {
		log.Fatalf("failed to create p2 WAL: %v", err)
	}
	defer p2WAL.Close()

	// 2. Initialize Participants
	part1 := participants.NewNode("P1", p1WAL)
	part2 := participants.NewNode("P2", p2WAL)
	parts := []participants.Participant{part1, part2}

	coord := coordinator.NewCoordinator(coordWAL, parts, 2*time.Second)

	fmt.Println("--- Running Recovery ---")
	part1.Recover()
	part2.Recover()
	coord.Recover()
	fmt.Println("--- Recovery Complete ---")

	fmt.Println("\n--- Starting Transaction tx-001 ---")
	tx := protocol.Transaction{ID: "tx-001", Payload: "transfer $500"}
	err = coord.Begin(tx)
	if err != nil {
		fmt.Printf("Transaction failed/aborted: %v\n", err)
	} else {
		fmt.Println("Transaction committed successfully!")
	}

	// Simulate a failing transaction (simulate a failure on part2)
	fmt.Println("\n--- Starting Transaction tx-fail (simulated NO vote) ---")
	part2.FailPrepare = true
	txFail := protocol.Transaction{ID: "tx-fail", Payload: "transfer $999"}
	err = coord.Begin(txFail)
	if err != nil {
		fmt.Printf("Transaction failed/aborted: %v\n", err)
	} else {
		fmt.Println("Transaction committed successfully!")
	}
}
