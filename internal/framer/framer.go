package framer

import (
	"encoding/binary"
	"io"
	"net"
)

func WriteMessage(conn net.Conn, payload []byte) error {
	length := uint32(len(payload))
	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, length)
	_, err := conn.Write(append(header, payload...))
	return err
}

func ReadMessage(conn net.Conn) ([]byte, error) {
	// Step 1: read exactly 4 bytes for the length header
	header := make([]byte, 4)
	if _, err := io.ReadFull(conn, header); err != nil {
		return nil, err
	}

	// Step 2: decode the length
	length := binary.BigEndian.Uint32(header)

	// Step 3: read exactly `length` bytes for the payload
	payload := make([]byte, length)
	if _, err := io.ReadFull(conn, payload); err != nil {
		return nil, err
	}

	return payload, nil
}
