package protocol

import "time"

type MessageType int

type Transaction struct {
	ID      string
	Payload string
}

type Message struct {
	Type        MessageType
	Transaction Transaction
	SenderID    string
	Timestamp   time.Time
}

const (
	PREPARE MessageType = iota
	VOTE_YES
	VOTE_NO
	COMMIT
	ABORT
)

func (m MessageType) String() string {
	switch m {
	case PREPARE:
		return "PREPARE"
	case VOTE_YES:
		return "VOTE_YES"
	case VOTE_NO:
		return "VOTE_NO"
	case COMMIT:
		return "COMMIT"
	case ABORT:
		return "ABORT"
	default:
		return "UNKNOWN"
	}
}
