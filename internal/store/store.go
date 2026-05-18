package store

import (
	"fmt"
	"kore/internal/types"
	"sync"
)

type EventType string

const (
	EventAdded    EventType = "ADDED"
	EventModified EventType = "MODIFIED"
	EventDeleted  EventType = "DELETED"
)

type Event struct {
	Type   EventType
	Object types.Object // interface — see below
}

type Store struct {
	mu       sync.RWMutex
	pods     map[string]*types.Pod // key = "namespace/name"
	version  uint64                // global resource version counter
	watchers []chan Event          // one channel per active watcher
}

func New() *Store {
	return &Store{
		pods: make(map[string]*types.Pod),
	}
}

func key(namespace, name string) string {
	return namespace + "/" + name
}

func (s *Store) nextVersion() uint64 {
	s.version++
	return s.version
}

func (s *Store) CreatePod(pod *types.Pod) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	k := key(pod.Namespace, pod.Name)
	if _, exists := s.pods[k]; exists {
		return fmt.Errorf("pod %s already exists", k)
	}

	pod.ResourceVersion = s.nextVersion()
	s.pods[k] = pod
	s.notify(Event{Type: EventAdded, Object: pod})
	return nil
}

func (s *Store) GetPod(namespace, name string) (*types.Pod, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	pod, ok := s.pods[key(namespace, name)]
	if !ok {
		return nil, fmt.Errorf("pod %s/%s not found", namespace, name)
	}
	return pod, nil
}

func (s *Store) ListPods(namespace string) []*types.Pod {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := []*types.Pod{}
	for _, pod := range s.pods {
		if pod.Namespace == namespace {
			result = append(result, pod)
		}
	}
	return result
}

func (s *Store) UpdatePod(pod *types.Pod) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	k := key(pod.Namespace, pod.Name)
	existing, ok := s.pods[k]
	if !ok {
		return fmt.Errorf("pod %s not found", k)
	}

	// Optimistic concurrency check — this is ResourceVersion's only job
	if pod.ResourceVersion != existing.ResourceVersion {
		return fmt.Errorf(
			"conflict: sent resourceVersion %d but current is %d",
			pod.ResourceVersion, existing.ResourceVersion,
		)
	}

	pod.ResourceVersion = s.nextVersion()
	s.pods[k] = pod
	s.notify(Event{Type: EventModified, Object: pod})
	return nil
}

func (s *Store) DeletePod(namespace, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	k := key(namespace, name)
	pod, ok := s.pods[k]
	if !ok {
		return fmt.Errorf("pod %s not found", k)
	}

	delete(s.pods, k)
	s.notify(Event{Type: EventDeleted, Object: pod})
	return nil
}

// Watch returns a channel that receives events from this point forward.
// The caller must call the returned cancel func when done,
// otherwise the channel leaks forever.
func (s *Store) Watch() (events <-chan Event, cancel func()) {
	ch := make(chan Event, 64) // buffered — slow watchers don't block writers

	s.mu.Lock()
	s.watchers = append(s.watchers, ch)
	s.mu.Unlock()

	cancel = func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		for i, w := range s.watchers {
			if w == ch {
				s.watchers = append(s.watchers[:i], s.watchers[i+1:]...)
				close(ch)
				return
			}
		}
	}
	return ch, cancel
}

// notify sends an event to all registered watchers.
// Must be called with mu held (write lock).
func (s *Store) notify(e Event) {
	for _, ch := range s.watchers {
		select {
		case ch <- e:
		default:
			// watcher is too slow — drop the event rather than block the writer
		}
	}
}
