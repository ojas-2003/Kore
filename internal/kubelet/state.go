package kubelet

import "sync"

// podState tracks what the kubelet has actually done for each pod.
type podState struct {
	containerID string
	image       string
}

// localState is the kubelet's memory of the containers it manages.
// This is "actual state" — what's really running on this node.
type localState struct {
	mu   sync.Mutex
	pods map[string]*podState // key = namespace/name
}

func newLocalState() *localState {
	return &localState{pods: make(map[string]*podState)}
}

func (l *localState) get(key string) (*podState, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	s, ok := l.pods[key]
	return s, ok
}

func (l *localState) set(key string, s *podState) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.pods[key] = s
}

func (l *localState) delete(key string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.pods, key)
}

// keys returns all pod keys the kubelet currently manages.
func (l *localState) keys() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]string, 0, len(l.pods))
	for k := range l.pods {
		out = append(out, k)
	}
	return out
}
