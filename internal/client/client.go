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
			var event types.Event
			if err := json.Unmarshal([]byte(line[6:]), &event); err != nil {
				continue
			}
			select {
			case out <- event:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, nil
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
