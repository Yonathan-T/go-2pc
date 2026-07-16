# Two-Phase Commit (2PC) in Go

A hands-on, local implementation of the classic Two-Phase Commit (2PC) protocol in Go, built for learning and exploring distributed consensus and transaction management.

Based on the Princeton COS 418 Distributed Systems lecture:  
[L6-2pc.pdf (Princeton COS 418)](https://www.cs.princeton.edu/courses/archive/fall16/cos418/docs/L6-2pc.pdf)

## What i Implemented

This project successfully demonstrates the core mechanics of 2PC:

*   **The Two Phases:** Phase 1 (Voting/Prepare) and Phase 2 (Decision/Commit or Abort).
*   **Write-Ahead Logging (WAL):** Both the Coordinator and Participants use a strict, append-only, `sync`-forced WAL to ensure decisions are persisted *before* taking action.
*   **Crash Recovery:** On startup, components read their WAL to determine the last known state. The coordinator re-sends missing decisions, and participants rebuild their transaction state.
*   **Timeouts & Concurrency:** The coordinator uses Go routines and Context timeouts to query participants concurrently without blocking indefinitely.
*   **Failure Simulation:** The ability to inject artificial failures (e.g., forcing a participant to vote `NO` or simulating a timeout delay) to test protocol safety.

## What i Skipped (For Now)

To keep this as an approachable learning tool, some real-world complexities were simplified:

*   **Network Transport:** The system runs in a single process. The Coordinator and Participants communicate via direct method calls and channels, rather than over a real network (TCP/RPC).
*   **Locking & Isolation:** We don't simulate row-level or table-level locks that a real database would require during the `PREPARED` state to prevent concurrent transaction interference.
*   **Three-Phase Commit (3PC):** The protocol is vulnerable to the standard 2PC blocking problem (where the coordinator crashes after participants vote yes, leaving them locked indefinitely). We didn't implement 3PC or Paxos to solve this.

## How to Run

1.  **Clone the repo and navigate to it.**
2.  **Run the main demonstration:**
    ```bash
    go run main.go
    ```
    This will execute a happy-path transaction, followed by a simulated failure (Abort) transaction.
3.  **Inspect the Logs:**
    Look inside the `logs/` directory to see the actual Write-Ahead Log outputs in JSON format.
    ```bash
    cat logs/coordinator.wal
    cat logs/participant_1.wal
    ```
4.  **Run the Test Suite:**
    The project includes unit tests for the WAL, Coordinator, and Participants, covering happy paths, NO votes, timeouts, and crash recovery.
    ```bash
    go test ./... -v
    ```
