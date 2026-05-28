package main

import (
	"context"
	"kore/internal/kubelet"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	nodeName := os.Getenv("KORE_NODE_NAME")
	if nodeName == "" {
		nodeName = "node-1"
	}

	runtime, err := kubelet.NewDockerRuntime()
	if err != nil {
		log.Fatalf("docker runtime: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		log.Println("shutting down kubelet")
		cancel()
	}()

	kl := kubelet.New(nodeName, "http://localhost:8080", runtime)
	log.Printf("kore-kubelet starting on node %s", nodeName)
	if err := kl.Run(ctx); err != nil {
		log.Fatalf("kubelet: %v", err)
	}
}
