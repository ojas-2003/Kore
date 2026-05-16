package types

type MessageType uint8

const (
	MessageTypePod   MessageType = 1
	MessageTypeNode  MessageType = 2
	MessageTypeEvent MessageType = 3
)

type Message struct {
	Type    MessageType `json:"type"`
	Payload []byte      `json:"payload"` // JSON-encoded resource
}
