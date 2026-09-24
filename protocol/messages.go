package protocol

import (
	"fmt"
	"strings"
	"time"
)

type MessageType int
type OpType string

type Transaction struct {
	ID      string
	Payload string
	Data    map[string]string
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
const (
	OpSet OpType = "SET"
	OpGet OpType = "GET"
	OpDel OpType = "DEL"
)

type Operation struct {
	Type OpType
	Key  string
	Val  string
}

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
func ParseCommand(input string) (Operation, error) {
	parts := strings.Fields(strings.TrimSpace(input))
	if len(parts) == 0 {
		return Operation{}, fmt.Errorf("empty command")
	}

	cmd := strings.ToUpper(parts[0])
	switch OpType(cmd) {
	case OpSet:
		if len(parts) < 3 {
			return Operation{}, fmt.Errorf("SET requires key and value (usage: SET <key> <val>)")
		}
		val := strings.Join(parts[2:], " ")
		val = strings.Trim(val, "\"'")
		return Operation{Type: OpSet, Key: parts[1], Val: val}, nil

	case OpDel:
		if len(parts) < 2 {
			return Operation{}, fmt.Errorf("DEL requires a key (usage: DEL <key>)")
		}
		return Operation{Type: OpDel, Key: parts[1]}, nil

	case OpGet:
		if len(parts) < 2 {
			return Operation{}, fmt.Errorf("GET requires a key (usage: GET <key>)")
		}
		return Operation{Type: OpGet, Key: parts[1]}, nil

	default:
		return Operation{}, fmt.Errorf("unknown command %q (expected SET, GET, or DEL)", cmd)
	}
}
