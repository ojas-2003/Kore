package client

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"kore/internal/types"
	"net/http"
	"strconv"
	"strings"
)

type Client struct {
	baseURL string
	http    *http.Client
}

func New(baseURL string) *Client {
	return &Client{
		baseURL: baseURL,
		http:    &http.Client{}, // no timeout — watches are long-lived
	}
}

// ListPods returns all pods in a namespace plus the cluster revision
// at which this snapshot was taken. The caller uses that revision to
// start a watch from the right point — this is list-then-watch.
func (c *Client) ListPods(ctx context.Context, namespace string) ([]*types.Pod, uint64, error) {
	url := fmt.Sprintf("%s/pods/%s", c.baseURL, namespace)
	req, _ := http.NewRequestWithContext(ctx, "GET", url, nil)

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()

	var pods []*types.Pod
	if err := json.NewDecoder(resp.Body).Decode(&pods); err != nil {
		return nil, 0, err
	}

	rev, _ := strconv.ParseUint(resp.Header.Get("X-Resource-Version"), 10, 64)
	return pods, rev, nil
}

func (c *Client) GetPod(ctx context.Context, namespace, name string) (*types.Pod, error) {
	url := fmt.Sprintf("%s/pods/%s/%s", c.baseURL, namespace, name)
	req, _ := http.NewRequestWithContext(ctx, "GET", url, nil)

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("pod %s/%s not found", namespace, name)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("get pod failed: status %d", resp.StatusCode)
	}

	var pod types.Pod
	if err := json.NewDecoder(resp.Body).Decode(&pod); err != nil {
		return nil, err
	}
	return &pod, nil
}

func (c *Client) CreateNode(ctx context.Context, node *types.Node) (*types.Node, error) {
	body, _ := json.Marshal(node)
	req, _ := http.NewRequestWithContext(ctx, "POST", c.baseURL+"/nodes", strings.NewReader(string(body)))
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusConflict {
		return nil, fmt.Errorf("node %s already exists", node.Name)
	}
	if resp.StatusCode != http.StatusCreated {
		return nil, fmt.Errorf("create node failed: status %d", resp.StatusCode)
	}

	var created types.Node
	if err := json.NewDecoder(resp.Body).Decode(&created); err != nil {
		return nil, err
	}
	return &created, nil
}

func (c *Client) ListNodes(ctx context.Context) ([]*types.Node, error) {
	req, _ := http.NewRequestWithContext(ctx, "GET", c.baseURL+"/nodes", nil)
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var nodes []*types.Node
	json.NewDecoder(resp.Body).Decode(&nodes)
	return nodes, nil
}

// WatchPods returns a channel that streams pod events from a given revision.
// The channel closes when the context is cancelled or the connection drops.
func (c *Client) WatchPods(ctx context.Context, namespace string, fromRev uint64) (<-chan types.Event, error) {
	url := fmt.Sprintf("%s/watch/pods/%s?resourceVersion=%d", c.baseURL, namespace, fromRev)
	req, _ := http.NewRequestWithContext(ctx, "GET", url, nil)

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != 200 {
		resp.Body.Close()
		return nil, fmt.Errorf("watch failed: status %d", resp.StatusCode)
	}

	out := make(chan types.Event, 64)
	go func() {
		defer close(out)
		defer resp.Body.Close()

		scanner := bufio.NewScanner(resp.Body)
		for scanner.Scan() {
			line := scanner.Text()
			if !strings.HasPrefix(line, "data: ") {
				continue
			}
			// Unmarshal into a concrete struct so Object becomes *types.Pod,
			// not map[string]interface{} (which is what happens when unmarshaling
			// into the types.Event.Object interface field directly).
			var wire struct {
				Type   types.EventType `json:"Type"`
				Object *types.Pod      `json:"Object"`
			}
			if err := json.Unmarshal([]byte(line[6:]), &wire); err != nil {
				continue
			}
			event := types.Event{Type: wire.Type, Object: wire.Object}
			select {
			case out <- event:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, nil
}

func (c *Client) CreatePod(ctx context.Context, pod *types.Pod) (*types.Pod, error) {
	body, _ := json.Marshal(pod)
	req, _ := http.NewRequestWithContext(ctx, "POST", c.baseURL+"/pods", strings.NewReader(string(body)))
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusConflict {
		return nil, fmt.Errorf("pod already exists")
	}
	if resp.StatusCode != http.StatusCreated {
		return nil, fmt.Errorf("create pod failed: status %d", resp.StatusCode)
	}

	var created types.Pod
	if err := json.NewDecoder(resp.Body).Decode(&created); err != nil {
		return nil, err
	}
	return &created, nil
}

func (c *Client) DeletePod(ctx context.Context, namespace, name string) error {
	url := fmt.Sprintf("%s/pods/%s/%s", c.baseURL, namespace, name)
	req, _ := http.NewRequestWithContext(ctx, "DELETE", url, nil)

	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("delete pod failed: status %d", resp.StatusCode)
	}
	return nil
}

func (c *Client) ListDeployments(ctx context.Context, namespace string) ([]*types.Deployment, uint64, error) {
	url := fmt.Sprintf("%s/deployments/%s", c.baseURL, namespace)
	req, _ := http.NewRequestWithContext(ctx, "GET", url, nil)

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()

	var deps []*types.Deployment
	if err := json.NewDecoder(resp.Body).Decode(&deps); err != nil {
		return nil, 0, err
	}
	rev, _ := strconv.ParseUint(resp.Header.Get("X-Resource-Version"), 10, 64)
	return deps, rev, nil
}

func (c *Client) ListServices(ctx context.Context, namespace string) ([]*types.Service, uint64, error) {
	url := fmt.Sprintf("%s/services/%s", c.baseURL, namespace)
	req, _ := http.NewRequestWithContext(ctx, "GET", url, nil)

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()

	var svcs []*types.Service
	if err := json.NewDecoder(resp.Body).Decode(&svcs); err != nil {
		return nil, 0, err
	}
	rev, _ := strconv.ParseUint(resp.Header.Get("X-Resource-Version"), 10, 64)
	return svcs, rev, nil
}

func (c *Client) GetEndpoints(ctx context.Context, namespace, name string) (*types.Endpoints, error) {
	url := fmt.Sprintf("%s/endpoints/%s/%s", c.baseURL, namespace, name)
	req, _ := http.NewRequestWithContext(ctx, "GET", url, nil)

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("endpoints %s/%s not found", namespace, name)
	}
	var ep types.Endpoints
	if err := json.NewDecoder(resp.Body).Decode(&ep); err != nil {
		return nil, err
	}
	return &ep, nil
}

func (c *Client) GetService(ctx context.Context, namespace, name string) (*types.Service, error) {
	url := fmt.Sprintf("%s/services/%s/%s", c.baseURL, namespace, name)
	req, _ := http.NewRequestWithContext(ctx, "GET", url, nil)

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("service %s/%s not found", namespace, name)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("get service failed: status %d", resp.StatusCode)
	}

	var svc types.Service
	if err := json.NewDecoder(resp.Body).Decode(&svc); err != nil {
		return nil, err
	}
	return &svc, nil
}

// UpsertEndpoints creates or replaces the Endpoints object for a service.
func (c *Client) UpsertEndpoints(ctx context.Context, ep *types.Endpoints) error {
	body, _ := json.Marshal(ep)
	url := fmt.Sprintf("%s/endpoints/%s/%s", c.baseURL, ep.Namespace, ep.Name)
	req, _ := http.NewRequestWithContext(ctx, "PUT", url, strings.NewReader(string(body)))
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("upsert endpoints failed: status %d", resp.StatusCode)
	}
	return nil
}

// UpdatePod sends a PUT request with the pod's current ResourceVersion.
// Returns a conflict error if someone else modified the pod in the meantime.
func (c *Client) UpdatePod(ctx context.Context, pod *types.Pod) error {
	body, _ := json.Marshal(pod)
	url := fmt.Sprintf("%s/pods/%s/%s", c.baseURL, pod.Namespace, pod.Name)
	req, _ := http.NewRequestWithContext(ctx, "PUT", url, strings.NewReader(string(body)))
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusConflict {
		return fmt.Errorf("conflict: pod was modified")
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("update failed: status %d", resp.StatusCode)
	}
	return nil
}
