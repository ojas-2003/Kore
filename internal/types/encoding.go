package types

import (
	"encoding/json"
	"fmt"
)

func Encode(msgType MessageType, resource any) ([]byte, error) {
	// Step 1: serialise the resource itself
	payload, err := json.Marshal(resource)
	if err != nil {
		return nil, fmt.Errorf("encoding payload: %w", err)
	}

	// Step 2: wrap in envelope
	msg := Message{
		Type:    msgType,
		Payload: payload,
	}

	// Step 3: serialise the envelope
	return json.Marshal(msg)
}

// Decode unpacks a Message envelope and returns the type + raw payload.
// Caller then json.Unmarshal the payload into the right struct.
func Decode(data []byte) (MessageType, []byte, error) {
	var msg Message
	if err := json.Unmarshal(data, &msg); err != nil {
		return 0, nil, fmt.Errorf("decoding envelope: %w", err)
	}
	return msg.Type, msg.Payload, nil
}

// DecodePod is a convenience helper for decoding a Pod payload.
func DecodePod(payload []byte) (*Pod, error) {
	var pod Pod
	if err := json.Unmarshal(payload, &pod); err != nil {
		return nil, fmt.Errorf("decoding pod: %w", err)
	}
	return &pod, nil
}
