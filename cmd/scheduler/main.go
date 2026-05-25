package main

import (
	"context"
	"kore/internal/scheduler"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())

	// Graceful shutdown on Ctrl+C
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		log.Println("shutdown signal received")
		cancel()
	}()

	apiServerURL := "http://localhost:8080"
	sched := scheduler.New(apiServerURL)

	log.Println("kore-scheduler starting")
	if err := sched.Run(ctx); err != nil {
		log.Fatalf("scheduler: %v", err)
	}
	log.Println("kore-scheduler stopped")
}
