package participants

import "Two-Phase-Commit/protocol"

type Participant interface {
	Prepare(tx protocol.Transaction) (protocol.MessageType, error)
	Commit(txID string) error
	Abort(txID string) error
}
