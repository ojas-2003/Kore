package scheduler

import (
	"container/list"
	"kore/internal/types"
	"sync"
	"time"
)

// Queue is an FIFO of pods waiting to be scheduled.
// Re-queued pods get a small delay to avoid hot loops on persistent failures.
type Queue struct {
	mu    sync.Mutex
	cond  *sync.Cond
	items *list.List
	inSet map[string]bool // dedupe — don't enqueue the same pod twice
}

type queueItem struct {
	pod     *types.Pod
	addedAt time.Time
}

func NewQueue() *Queue {
	q := &Queue{
		items: list.New(),
		inSet: make(map[string]bool),
	}
	q.cond = sync.NewCond(&q.mu)
	return q
}

func podKey(p *types.Pod) string {
	return p.Namespace + "/" + p.Name
}

// Add a pod to the queue if not already present.
func (q *Queue) Add(pod *types.Pod) {
	q.mu.Lock()
	defer q.mu.Unlock()

	key := podKey(pod)
	if q.inSet[key] {
		return
	}
	q.inSet[key] = true
	q.items.PushBack(&queueItem{pod: pod, addedAt: time.Now()})
	q.cond.Signal()
}

// Pop blocks until an item is available, then returns it.
// Returns nil if the queue is shut down.
func (q *Queue) Pop() *types.Pod {
	q.mu.Lock()
	defer q.mu.Unlock()

	for q.items.Len() == 0 {
		q.cond.Wait()
	}
	front := q.items.Front()
	q.items.Remove(front)
	item := front.Value.(*queueItem)
	delete(q.inSet, podKey(item.pod))
	return item.pod
}

// Remove drops a pod from the queue if present (e.g., pod was deleted).
func (q *Queue) Remove(pod *types.Pod) {
	q.mu.Lock()
	defer q.mu.Unlock()
	key := podKey(pod)
	if !q.inSet[key] {
		return
	}
	delete(q.inSet, key)
	for e := q.items.Front(); e != nil; e = e.Next() {
		if podKey(e.Value.(*queueItem).pod) == key {
			q.items.Remove(e)
			return
		}
	}
}
