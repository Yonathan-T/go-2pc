# Two-Phase Commit (2PC) in Go

A hands-on, multi-process distributed implementation of the classic Two-Phase Commit (2PC) protocol in Go, built for learning and exploring distributed consensus, crash recovery, and transaction management.

Based on the Princeton COS 418 Distributed Systems lecture:  
[[L6-2pc.pdf (Princeton COS 418)]](https://www.cs.princeton.edu/courses/archive/fall16/cos418/docs/L6-2pc.pdf)

## What i Implemented

This project successfully demonstrates the core mechanics of 2PC across independent microservices:

*   **The Two Phases:** Phase 1 (Voting/Prepare) and Phase 2 (Decision/Commit or Abort).
*   **gRPC Microservices:** The Coordinator and Participants run as standalone processes communicating over real TCP sockets using Protocol Buffers (`proto/twopc.proto`).
*   **Write-Ahead Logging (WAL):** Both the Coordinator and Participants use a strict, append-only, `sync`-forced WAL (`file.Sync()`) to ensure decisions are persisted to disk *before* taking action.
*   **Crash Recovery:** On startup, components replay their WAL to rebuild data state and recover uncommitted transactions after node crashes or power loss.
*   **Uncommitted Staging Isolation:** Prepared mutations are staged in memory without polluting active database state until a global commit decision is reached.
*   **Redis-Style Commands:** Interactive query engine supporting `SET <key> <val>`, `DEL <key>`, `GET <key>`, and `KEYS`. Reads query nodes directly, while writes run atomic 2PC consensus.
*   **Interactive Terminal UI:** Built with Charm's Bubble Tea and Lip Gloss, featuring non-blocking background heartbeats that track participant health and display `ONLINE` / `OFFLINE` status in real-time.

## What i Skipped (For Now)

To keep this as an approachable learning tool, some real-world complexities were simplified:

*   **Three-Phase Commit (3PC) & Paxos:** The protocol remains vulnerable to the classic 2PC coordinator crash scenario (where participants are blocked waiting if the coordinator fails during commit). We didn't implement 3PC or Raft/Paxos leader election to solve coordinator failure.
*   **Range Sharding:** Currently, all participant nodes replicate the entire key space rather than partitioning data across key ranges.

## How to Run

1.  **Clone the repo and navigate to it.**
2.  **Start Participant 1 (Terminal 1):**
    ```bash
    go run ./cmd/participant -id P1 -port 50051 -wal logs/p1.wal
    ```
3.  **Start Participant 2 (Terminal 2):**
    ```bash
    go run ./cmd/participant -id P2 -port 50052 -wal logs/p2.wal
    ```
4.  **Start the Coordinator TUI (Terminal 3):**
    ```bash
    go run ./cmd/coordinator
    ```
    Both nodes will show `ONLINE`. You can execute `SET`, `GET`, `DEL`, or `KEYS`. If you kill a participant (`Ctrl+C`), its indicator will switch to `OFFLINE` in real-time, causing writes to abort until revived.
5.  **Run the Test Suite:**
    ```bash
    go test ./... -v
    ```
