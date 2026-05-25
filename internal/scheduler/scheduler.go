package scheduler

import (
	"context"
	"kore/internal/client"
	"kore/internal/types"
	"log"
	"time"
)

type Scheduler struct {
	client *client.Client
	queue  *Queue
}

func New(apiServerURL string) *Scheduler {
	return &Scheduler{
		client: client.New(apiServerURL),
		queue:  NewQueue(),
	}
}

// Run starts the scheduler. Returns when ctx is cancelled.
func (s *Scheduler) Run(ctx context.Context) error {
	// Spin off the worker goroutine that consumes the queue.
	go s.scheduleLoop(ctx)

	// The main goroutine runs the watch — feeds the queue.
	return s.watchLoop(ctx)
}

// watchLoop implements list-then-watch. It populates the queue with
// unscheduled pods, then watches for new ones forever.
func (s *Scheduler) watchLoop(ctx context.Context) error {
	for {
		// STEP 1: List the current world.
		pods, rev, err := s.client.ListPods(ctx, "default")
		if err != nil {
			log.Printf("list pods: %v; retrying in 2s", err)
			select {
			case <-time.After(2 * time.Second):
				continue
			case <-ctx.Done():
				return nil
			}
		}

		// Enqueue every unscheduled pod we found in the list.
		for _, pod := range pods {
			if pod.Spec.NodeName == "" {
				log.Printf("queueing unscheduled pod %s (from list)", pod.Name)
				s.queue.Add(pod)
			}
		}

		// STEP 2: Watch from the revision the list returned.
		events, err := s.client.WatchPods(ctx, "default", rev)
		if err != nil {
			log.Printf("watch start: %v; retrying", err)
			continue
		}

		// Process events until the channel closes (disconnect or shutdown).
		for event := range events {
			pod, ok := event.Object.(*types.Pod)
			if !ok {
				continue
			}
			switch event.Type {
			case types.EventAdded, types.EventModified:
				if pod.Spec.NodeName == "" {
					s.queue.Add(pod)
				}
			case types.EventDeleted:
				s.queue.Remove(pod)
			}
		}

		// If we got here, the watch dropped. Loop back and re-list.
		// This is the standard "watch interrupted, resync via list" recovery.
		if ctx.Err() != nil {
			return nil
		}
		log.Println("watch ended unexpectedly; re-listing")
	}
}

// scheduleLoop pulls pods from the queue and tries to place each one.
func (s *Scheduler) scheduleLoop(ctx context.Context) {
	for {
		pod := s.queue.Pop()
		if pod == nil || ctx.Err() != nil {
			return
		}
		s.scheduleOne(ctx, pod)
	}
}

// scheduleOne tries to bind a single pod to a node.
func (s *Scheduler) scheduleOne(ctx context.Context, pod *types.Pod) {
	// Re-fetch state — pods/nodes may have changed since we queued.
	nodes, err := s.client.ListNodes(ctx)
	if err != nil {
		log.Printf("list nodes: %v", err)
		s.requeue(pod)
		return
	}
	allPods, _, err := s.client.ListPods(ctx, "default")
	if err != nil {
		log.Printf("list pods: %v", err)
		s.requeue(pod)
		return
	}

	chosen, err := schedule(pod, nodes, allPods)
	if err != nil {
		log.Printf("cannot schedule %s: %v; will retry", pod.Name, err)
		s.requeue(pod)
		return
	}

	pod.Spec.NodeName = chosen.Name
	if err := s.client.UpdatePod(ctx, pod); err != nil {
		// Conflict = someone else modified the pod. Re-list will pick it up
		// again if still unscheduled.
		log.Printf("bind %s -> %s failed: %v", pod.Name, chosen.Name, err)
		return
	}
	log.Printf("scheduled %s -> %s", pod.Name, chosen.Name)
}

func (s *Scheduler) requeue(pod *types.Pod) {
	// Delay before requeue to avoid tight retry loops.
	go func() {
		time.Sleep(5 * time.Second)
		s.queue.Add(pod)
	}()
}
