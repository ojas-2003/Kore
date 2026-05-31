package main

import (
	"fmt"
	"kore/internal/framer"
	"kore/internal/types"
	"net"
)

func main() {
	conn, err := net.Dial("tcp", "localhost:8080")
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	// create a pod
	pod := types.Pod{
		ObjectMeta: types.ObjectMeta{
			Name:            "nginx-1",
			Namespace:       "default",
			ResourceVersion: 1,
		},
		Spec: types.PodSpec{
			Image:    "nginx:latest",
			NodeName: "",
		},
	}

	// encode and send
	data, err := types.Encode(types.MessageTypePod, pod)
	if err != nil {
		panic(err)
	}
	framer.WriteMessage(conn, data)
	fmt.Println("sent pod")

	// read echo
	resp, _ := framer.ReadMessage(conn)
	msgType, payload, _ := types.Decode(resp)
	if msgType == types.MessageTypePod {
		received, _ := types.DecodePod(payload)
		fmt.Printf("server echoed: name=%s image=%s resourceVersion=%d\n",
			received.Name, received.Spec.Image, received.ResourceVersion)
	}
	pod2 := types.Pod{
		ObjectMeta: types.ObjectMeta{Name: "x", ResourceVersion: 99999},
		Spec:       types.PodSpec{Image: "ubuntu:22.04"},
	}
	data, _ = types.Encode(1, pod2)
	fmt.Printf("JSON wire size: %d bytes\n", len(data))
}
