package main

import (
	"fmt"
	"kore/internal/framer"
	"kore/internal/types"
	"net"
)

func main() {
	listener, err := net.Listen("tcp", ":8080")
	if err != nil {
		panic(err)
	}
	fmt.Println("Listening on :8080")

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("accept error:", err)
			continue
		}
		fmt.Println("new connection from", conn.RemoteAddr())
		go handleConn(conn)
	}
}

func handleConn(conn net.Conn) {
	defer conn.Close()
	for {
		// receive framed bytes
		data, err := framer.ReadMessage(conn)
		if err != nil {
			fmt.Println("connection closed:", err)
			return
		}

		// decode the envelope
		msgType, payload, err := types.Decode(data)
		if err != nil {
			fmt.Println("decode error:", err)
			continue
		}

		switch msgType {
		case types.MessageTypePod:
			pod, err := types.DecodePod(payload)
			if err != nil {
				fmt.Println("pod decode error:", err)
				continue
			}
			fmt.Printf("received Pod: name=%s image=%s\n",
				pod.Name, pod.Spec.Image)

			// echo back the same pod
			resp, _ := types.Encode(types.MessageTypePod, pod)
			framer.WriteMessage(conn, resp)

		default:
			fmt.Printf("unknown message type: %d\n", msgType)
		}
	}
}
