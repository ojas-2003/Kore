package kubelet

import (
	"context"
	"kore/internal/client"
	"kore/internal/types"
	"log"
	"strings"
	"time"
)

type Kubelet struct {
	nodeName string
	client   *client.Client
	runtime  Runtime
	state    *localState
}

func New(nodeName, apiServerURL string, runtime Runtime) *Kubelet {
	return &Kubelet{
		nodeName: nodeName,
		client:   client.New(apiServerURL),
		runtime:  runtime,
		state:    newLocalState(),
	}
}

func (k *Kubelet) Run(ctx context.Context) error {
	if err := k.registerNode(ctx); err != nil {
		return err
	}

	go k.heartbeatLoop(ctx)
	go k.resyncLoop(ctx) // ← NEW: the level-triggered safety net

	return k.watchLoop(ctx)
}

// watchLoop — identical skeleton to the scheduler's. List, then watch,
// reconciling each pod assigned to this node.
func (k *Kubelet) watchLoop(ctx context.Context) error {
	for {
		pods, rev, err := k.client.ListPods(ctx, "default")
		if err != nil {
			log.Printf("list pods: %v; retry in 2s", err)
			if !sleep(ctx, 2*time.Second) {
				return nil
			}
			continue
		}

		// Reconcile everything in the initial list.
		for _, pod := range pods {
			if pod.Spec.NodeName == k.nodeName {
				k.reconcile(ctx, pod)
			}
		}

		events, err := k.client.WatchPods(ctx, "default", rev)
		if err != nil {
			log.Printf("watch: %v; retry", err)
			continue
		}

		for event := range events {
			pod, ok := event.Object.(*types.Pod)
			if !ok || pod.Spec.NodeName != k.nodeName {
				continue // not mine — ignore
			}
			switch event.Type {
			case types.EventAdded, types.EventModified:
				k.reconcile(ctx, pod)
			case types.EventDeleted:
				k.teardown(ctx, pod)
			}
		}

		if ctx.Err() != nil {
			return nil
		}
		log.Println("watch ended; re-listing")
	}
}

// reconcile drives one pod toward its desired state.
// This is the four-quadrant table from the chapter intro, in code.
func (k *Kubelet) reconcile(ctx context.Context, pod *types.Pod) {
	key := pod.Namespace + "/" + pod.Name
	existing, known := k.state.get(key)

	// Is it already running what it should?
	if known {
		running, _ := k.runtime.IsRunning(ctx, existing.containerID)
		if running && existing.image == pod.Spec.Image {
			return // desired == actual, nothing to do
		}
		// Image changed or container died — tear down and restart.
		k.runtime.StopContainer(ctx, existing.containerID)
		k.state.delete(key)
	}

	// Start the container.
	log.Printf("starting pod %s (image=%s)", key, pod.Spec.Image)
	k.updateStatus(ctx, pod, types.PodPending, "", "", "pulling image and starting")

	containerID, err := k.runtime.StartContainer(ctx, key, pod.Spec.Image)
	if err != nil {
		log.Printf("failed to start %s: %v", key, err)
		k.updateStatus(ctx, pod, types.PodFailed, "", "", err.Error())
		return
	}

	podIP, _ := k.runtime.ContainerIP(ctx, containerID)
	k.state.set(key, &podState{containerID: containerID, image: pod.Spec.Image})
	k.updateStatus(ctx, pod, types.PodRunning, containerID, podIP, "")
	log.Printf("pod %s running as container %s ip=%s", key, containerID[:12], podIP)
}

// teardown stops a pod's container when the pod is deleted.
func (k *Kubelet) teardown(ctx context.Context, pod *types.Pod) {
	key := pod.Namespace + "/" + pod.Name
	existing, known := k.state.get(key)
	if !known {
		return
	}
	log.Printf("stopping pod %s", key)
	k.runtime.StopContainer(ctx, existing.containerID)
	k.state.delete(key)
}

// updateStatus writes the pod's actual state back to the API server.
func (k *Kubelet) updateStatus(ctx context.Context, pod *types.Pod, phase types.PodPhase, containerID, podIP, msg string) {
	// Re-fetch to get the latest resourceVersion (avoid conflicts).
	fresh, err := k.client.GetPod(ctx, pod.Namespace, pod.Name)
	if err != nil {
		return
	}
	fresh.Status.Phase = phase
	fresh.Status.ContainerID = containerID
	fresh.Status.PodIP = podIP
	fresh.Status.Message = msg
	if err := k.client.UpdatePod(ctx, fresh); err != nil {
		log.Printf("status update for %s failed: %v", pod.Name, err)
	}
}

// sleep returns false if the context was cancelled during the wait.
func sleep(ctx context.Context, d time.Duration) bool {
	select {
	case <-time.After(d):
		return true
	case <-ctx.Done():
		return false
	}
}

func (k *Kubelet) registerNode(ctx context.Context) error {
	node := &types.Node{
		ObjectMeta: types.ObjectMeta{Name: k.nodeName},
		Status: types.NodeStatus{
			// Hardcoded capacity for now. A real kubelet detects this
			// from the actual machine (cgroups, /proc/cpuinfo, etc.).
			Capacity:    types.ResourceList{CPU: 4000, Memory: 8000000000},
			Allocatable: types.ResourceList{CPU: 4000, Memory: 8000000000},
		},
	}

	// Try to create; if it already exists, that's fine.
	_, err := k.client.CreateNode(ctx, node)
	if err != nil {
		log.Printf("node %s may already exist: %v", k.nodeName, err)
	} else {
		log.Printf("registered node %s", k.nodeName)
	}
	return nil
}

func (k *Kubelet) heartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// A real heartbeat updates a LastHeartbeatTime field in status.
			// The node controller (Ch.7) watches for stale heartbeats to
			// detect dead nodes. For now we just log.
			log.Printf("heartbeat: node %s alive", k.nodeName)
		}
	}
}

// resyncLoop periodically re-reconciles every managed pod, regardless of
// whether any watch event fired. This is the level-triggered safety net:
// it catches containers that died on their own, were killed externally,
// or any drift between desired and actual state.
func (k *Kubelet) resyncLoop(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for _, key := range k.state.keys() {
				parts := strings.SplitN(key, "/", 2)
				if len(parts) != 2 {
					continue
				}
				namespace, name := parts[0], parts[1]

				// Re-fetch the desired state from the API server —
				// the spec might have changed AND we want fresh data.
				pod, err := k.client.GetPod(ctx, namespace, name)
				if err != nil {
					// Pod no longer exists in the API → tear down its container.
					log.Printf("resync: pod %s gone from API, tearing down", key)
					k.teardownByKey(ctx, key)
					continue
				}

				// Only reconcile if it's still assigned to us.
				if pod.Spec.NodeName == k.nodeName {
					k.reconcile(ctx, pod)
				}
			}
		}
	}
}

func (k *Kubelet) teardownByKey(ctx context.Context, key string) {
	existing, known := k.state.get(key)
	if !known {
		return
	}
	k.runtime.StopContainer(ctx, existing.containerID)
	k.state.delete(key)
}
