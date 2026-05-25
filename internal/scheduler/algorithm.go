package scheduler

import (
	"fmt"
	"kore/internal/types"
)

// schedule picks a node for a pod, or returns an error if none fit.
// The algorithm is two phases: filter, then score.
func schedule(pod *types.Pod, nodes []*types.Node, pods []*types.Pod) (*types.Node, error) {
	// Phase 1: Filter — keep only nodes the pod COULD run on.
	feasible := filter(pod, nodes, pods)
	if len(feasible) == 0 {
		return nil, fmt.Errorf("no node has capacity for pod %s/%s", pod.Namespace, pod.Name)
	}

	// Phase 2: Score — rank the feasible nodes, highest wins.
	return score(pod, feasible, pods), nil
}

// filter applies hard constraints. A node is feasible if and only if
// the pod fits given the resources already claimed by other pods on it.
func filter(pod *types.Pod, nodes []*types.Node, allPods []*types.Pod) []*types.Node {
	var result []*types.Node
	for _, node := range nodes {
		used := usedResources(node.Name, allPods)
		free := types.ResourceList{
			CPU:    node.Status.Allocatable.CPU - used.CPU,
			Memory: node.Status.Allocatable.Memory - used.Memory,
		}
		if free.CPU >= pod.Spec.Resources.CPU && free.Memory >= pod.Spec.Resources.Memory {
			result = append(result, node)
		}
	}
	return result
}

// score ranks nodes — least loaded wins.
// This is where you'd plug in ML later (see our earlier conversation).
func score(pod *types.Pod, nodes []*types.Node, allPods []*types.Pod) *types.Node {
	var best *types.Node
	var bestScore int64 = -1

	for _, node := range nodes {
		used := usedResources(node.Name, allPods)
		free := node.Status.Allocatable.CPU - used.CPU + (node.Status.Allocatable.Memory-used.Memory)/1000000
		// Higher free capacity → higher score. Trivial but correct.
		if free > bestScore {
			bestScore = free
			best = node
		}
	}
	return best
}

// usedResources sums the requested resources of all pods already on a node.
func usedResources(nodeName string, allPods []*types.Pod) types.ResourceList {
	var sum types.ResourceList
	for _, p := range allPods {
		if p.Spec.NodeName == nodeName {
			sum.CPU += p.Spec.Resources.CPU
			sum.Memory += p.Spec.Resources.Memory
		}
	}
	return sum
}
