package etcdstore

import (
	"context"
	"encoding/json"
	"fmt"
	"kore/internal/types"
	"strings"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

const nodePrefix = "/resources/nodes/"

type Store struct {
	client *clientv3.Client
}

func New(endpoints []string) (*Store, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, fmt.Errorf("connecting to etcd: %w", err)
	}
	return &Store{client: cli}, nil
}

func (s *Store) Close() error {
	return s.client.Close()
}

func podKey(namespace, name string) string {
	return fmt.Sprintf("/resources/pods/%s/%s", namespace, name)
}

func podPrefix(namespace string) string {
	return fmt.Sprintf("/resources/pods/%s/", namespace)
}

// Create a pod. Fails if it already exists.
func (s *Store) CreatePod(ctx context.Context, pod *types.Pod) error {
	key := podKey(pod.Namespace, pod.Name)

	data, err := json.Marshal(pod)
	if err != nil {
		return fmt.Errorf("encoding pod: %w", err)
	}

	// Transaction: only write if the key doesn't exist yet.
	// This is etcd's primitive for compare-and-swap.
	txn, err := s.client.Txn(ctx).
		If(clientv3.Compare(clientv3.CreateRevision(key), "=", 0)).
		Then(clientv3.OpPut(key, string(data))).
		Commit()

	resp := txn
	if err != nil {
		return fmt.Errorf("etcd txn: %w", err)
	}
	if !resp.Succeeded {
		return fmt.Errorf("pod %s/%s already exists", pod.Namespace, pod.Name)
	}

	// etcd assigns the revision; we read it back and stamp the pod
	pod.ResourceVersion = uint64(resp.Header.Revision)
	return nil
}

// Get a single pod by namespace/name.
func (s *Store) GetPod(ctx context.Context, namespace, name string) (*types.Pod, error) {
	resp, err := s.client.Get(ctx, podKey(namespace, name))
	if err != nil {
		return nil, fmt.Errorf("etcd get: %w", err)
	}
	if len(resp.Kvs) == 0 {
		return nil, fmt.Errorf("pod %s/%s not found", namespace, name)
	}

	var pod types.Pod
	if err := json.Unmarshal(resp.Kvs[0].Value, &pod); err != nil {
		return nil, fmt.Errorf("decoding pod: %w", err)
	}
	// The ModRevision is the revision at which this key was last modified.
	// That's our ResourceVersion.
	pod.ResourceVersion = uint64(resp.Kvs[0].ModRevision)
	return &pod, nil
}

// List all pods in a namespace.
func (s *Store) ListPods(ctx context.Context, namespace string) ([]*types.Pod, uint64, error) {
	resp, err := s.client.Get(ctx, podPrefix(namespace), clientv3.WithPrefix())
	if err != nil {
		return nil, 0, fmt.Errorf("etcd list: %w", err)
	}

	pods := make([]*types.Pod, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		var pod types.Pod
		if err := json.Unmarshal(kv.Value, &pod); err != nil {
			continue // skip malformed entries
		}
		pod.ResourceVersion = uint64(kv.ModRevision)
		pods = append(pods, &pod)
	}

	// resp.Header.Revision is the current global revision at the time of this list.
	// Used to start a watch from exactly the right point — see Step 6.
	return pods, uint64(resp.Header.Revision), nil
}

func (s *Store) UpdatePod(ctx context.Context, pod *types.Pod) error {
	key := podKey(pod.Namespace, pod.Name)

	data, err := json.Marshal(pod)
	if err != nil {
		return fmt.Errorf("encoding pod: %w", err)
	}

	// Conditional write: only succeed if the key's current ModRevision
	// matches what the caller saw.
	resp, err := s.client.Txn(ctx).
		If(clientv3.Compare(
			clientv3.ModRevision(key),
			"=",
			int64(pod.ResourceVersion),
		)).
		Then(clientv3.OpPut(key, string(data))).
		Commit()
	if err != nil {
		return fmt.Errorf("etcd txn: %w", err)
	}
	if !resp.Succeeded {
		return fmt.Errorf(
			"conflict: pod %s/%s was modified (sent rv=%d)",
			pod.Namespace, pod.Name, pod.ResourceVersion,
		)
	}

	pod.ResourceVersion = uint64(resp.Header.Revision)
	return nil
}

func (s *Store) DeletePod(ctx context.Context, namespace, name string) error {
	resp, err := s.client.Delete(ctx, podKey(namespace, name))
	if err != nil {
		return fmt.Errorf("etcd delete: %w", err)
	}
	if resp.Deleted == 0 {
		return fmt.Errorf("pod %s/%s not found", namespace, name)
	}
	return nil
}

// Watch streams pod events from a given revision onward.
// Pass startRev=0 to mean "start from the current revision."
// In practice, callers use the revision returned by ListPods to do list-then-watch.
func (s *Store) WatchPods(ctx context.Context, namespace string, startRev uint64) <-chan types.Event {
	out := make(chan types.Event, 64)

	go func() {
		defer close(out)

		opts := []clientv3.OpOption{clientv3.WithPrefix()}
		if startRev > 0 {
			opts = append(opts, clientv3.WithRev(int64(startRev+1)))
		}

		watchChan := s.client.Watch(ctx, podPrefix(namespace), opts...)

		for watchResp := range watchChan {
			if watchResp.Err() != nil {
				return
			}
			for _, ev := range watchResp.Events {
				event := types.Event{}

				switch ev.Type {
				case clientv3.EventTypePut:
					var pod types.Pod
					if err := json.Unmarshal(ev.Kv.Value, &pod); err != nil {
						continue
					}
					pod.ResourceVersion = uint64(ev.Kv.ModRevision)

					if ev.IsCreate() {
						event.Type = types.EventAdded
					} else {
						event.Type = types.EventModified
					}
					event.Object = &pod

				case clientv3.EventTypeDelete:
					event.Type = types.EventDeleted
					// For deletes, PrevKv has the previous value if you enable it
					if ev.PrevKv != nil {
						var pod types.Pod
						json.Unmarshal(ev.PrevKv.Value, &pod)
						event.Object = &pod
					} else {
						// Just emit the key path; caller can extract name
						event.Object = &types.Pod{
							ObjectMeta: types.ObjectMeta{
								Namespace: namespace,
								Name:      strings.TrimPrefix(string(ev.Kv.Key), podPrefix(namespace)),
							},
						}
					}
				}

				select {
				case out <- event:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	return out
}

func nodeKey(name string) string {
	return fmt.Sprintf("/resources/nodes/%s", name)
}

// ---- Node CRUD ----

// CreateNode writes a node. Fails if it already exists.
func (s *Store) CreateNode(ctx context.Context, node *types.Node) error {
	key := nodeKey(node.Name)

	data, err := json.Marshal(node)
	if err != nil {
		return fmt.Errorf("encoding node: %w", err)
	}

	txn, err := s.client.Txn(ctx).
		If(clientv3.Compare(clientv3.CreateRevision(key), "=", 0)).
		Then(clientv3.OpPut(key, string(data))).
		Commit()

	resp := txn
	if err != nil {
		return fmt.Errorf("etcd txn: %w", err)
	}
	if !resp.Succeeded {
		return fmt.Errorf("node %s already exists", node.Name)
	}

	node.ResourceVersion = uint64(resp.Header.Revision)
	return nil
}

// GetNode fetches a single node by name.
func (s *Store) GetNode(ctx context.Context, name string) (*types.Node, error) {
	resp, err := s.client.Get(ctx, nodeKey(name))
	if err != nil {
		return nil, fmt.Errorf("etcd get: %w", err)
	}
	if len(resp.Kvs) == 0 {
		return nil, fmt.Errorf("node %s not found", name)
	}

	var node types.Node
	if err := json.Unmarshal(resp.Kvs[0].Value, &node); err != nil {
		return nil, fmt.Errorf("decoding node: %w", err)
	}
	node.ResourceVersion = uint64(resp.Kvs[0].ModRevision)
	return &node, nil
}

// ListNodes returns all nodes plus the cluster revision at snapshot time.
func (s *Store) ListNodes(ctx context.Context) ([]*types.Node, uint64, error) {
	resp, err := s.client.Get(ctx, nodePrefix, clientv3.WithPrefix())
	if err != nil {
		return nil, 0, fmt.Errorf("etcd list: %w", err)
	}

	nodes := make([]*types.Node, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		var node types.Node
		if err := json.Unmarshal(kv.Value, &node); err != nil {
			continue
		}
		node.ResourceVersion = uint64(kv.ModRevision)
		nodes = append(nodes, &node)
	}
	return nodes, uint64(resp.Header.Revision), nil
}

// UpdateNode does a conditional write keyed on ResourceVersion.
// Used both for spec changes and status heartbeats from the kubelet (Ch.6).
func (s *Store) UpdateNode(ctx context.Context, node *types.Node) error {
	key := nodeKey(node.Name)

	data, err := json.Marshal(node)
	if err != nil {
		return fmt.Errorf("encoding node: %w", err)
	}

	resp, err := s.client.Txn(ctx).
		If(clientv3.Compare(
			clientv3.ModRevision(key),
			"=",
			int64(node.ResourceVersion),
		)).
		Then(clientv3.OpPut(key, string(data))).
		Commit()

	if err != nil {
		return fmt.Errorf("etcd txn: %w", err)
	}
	if !resp.Succeeded {
		return fmt.Errorf("conflict: node %s was modified (sent rv=%d)", node.Name, node.ResourceVersion)
	}

	node.ResourceVersion = uint64(resp.Header.Revision)
	return nil
}

// DeleteNode removes a node.
func (s *Store) DeleteNode(ctx context.Context, name string) error {
	resp, err := s.client.Delete(ctx, nodeKey(name))
	if err != nil {
		return fmt.Errorf("etcd delete: %w", err)
	}
	if resp.Deleted == 0 {
		return fmt.Errorf("node %s not found", name)
	}
	return nil
}

// ---- Deployment CRUD ----

func deploymentKey(namespace, name string) string {
	return fmt.Sprintf("/resources/deployments/%s/%s", namespace, name)
}

func deploymentPrefix(namespace string) string {
	return fmt.Sprintf("/resources/deployments/%s/", namespace)
}

func (s *Store) CreateDeployment(ctx context.Context, dep *types.Deployment) error {
	key := deploymentKey(dep.Namespace, dep.Name)

	data, err := json.Marshal(dep)
	if err != nil {
		return fmt.Errorf("encoding deployment: %w", err)
	}

	txn, err := s.client.Txn(ctx).
		If(clientv3.Compare(clientv3.CreateRevision(key), "=", 0)).
		Then(clientv3.OpPut(key, string(data))).
		Commit()

	resp := txn
	if err != nil {
		return fmt.Errorf("etcd txn: %w", err)
	}
	if !resp.Succeeded {
		return fmt.Errorf("deployment %s/%s already exists", dep.Namespace, dep.Name)
	}

	dep.ResourceVersion = uint64(resp.Header.Revision)
	return nil
}

func (s *Store) GetDeployment(ctx context.Context, namespace, name string) (*types.Deployment, error) {
	resp, err := s.client.Get(ctx, deploymentKey(namespace, name))
	if err != nil {
		return nil, fmt.Errorf("etcd get: %w", err)
	}
	if len(resp.Kvs) == 0 {
		return nil, fmt.Errorf("deployment %s/%s not found", namespace, name)
	}

	var dep types.Deployment
	if err := json.Unmarshal(resp.Kvs[0].Value, &dep); err != nil {
		return nil, fmt.Errorf("decoding deployment: %w", err)
	}
	dep.ResourceVersion = uint64(resp.Kvs[0].ModRevision)
	return &dep, nil
}

func (s *Store) ListDeployments(ctx context.Context, namespace string) ([]*types.Deployment, uint64, error) {
	resp, err := s.client.Get(ctx, deploymentPrefix(namespace), clientv3.WithPrefix())
	if err != nil {
		return nil, 0, fmt.Errorf("etcd list: %w", err)
	}

	deps := make([]*types.Deployment, 0, len(resp.Kvs))
	for _, kv := range resp.Kvs {
		var dep types.Deployment
		if err := json.Unmarshal(kv.Value, &dep); err != nil {
			continue
		}
		dep.ResourceVersion = uint64(kv.ModRevision)
		deps = append(deps, &dep)
	}
	return deps, uint64(resp.Header.Revision), nil
}

func (s *Store) UpdateDeployment(ctx context.Context, dep *types.Deployment) error {
	key := deploymentKey(dep.Namespace, dep.Name)

	data, err := json.Marshal(dep)
	if err != nil {
		return fmt.Errorf("encoding deployment: %w", err)
	}

	resp, err := s.client.Txn(ctx).
		If(clientv3.Compare(clientv3.ModRevision(key), "=", int64(dep.ResourceVersion))).
		Then(clientv3.OpPut(key, string(data))).
		Commit()
	if err != nil {
		return fmt.Errorf("etcd txn: %w", err)
	}
	if !resp.Succeeded {
		return fmt.Errorf("conflict: deployment %s/%s was modified (sent rv=%d)", dep.Namespace, dep.Name, dep.ResourceVersion)
	}

	dep.ResourceVersion = uint64(resp.Header.Revision)
	return nil
}

func (s *Store) DeleteDeployment(ctx context.Context, namespace, name string) error {
	resp, err := s.client.Delete(ctx, deploymentKey(namespace, name))
	if err != nil {
		return fmt.Errorf("etcd delete: %w", err)
	}
	if resp.Deleted == 0 {
		return fmt.Errorf("deployment %s/%s not found", namespace, name)
	}
	return nil
}
