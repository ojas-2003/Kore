package controller

import (
	"context"
	"fmt"
	"kore/internal/client"
	"kore/internal/types"
	"log"
)

type EndpointsController struct {
	client *client.Client
}

func NewEndpointsController(c *client.Client) *EndpointsController {
	return &EndpointsController{client: c}
}

// reconcile rebuilds the Endpoints for one Service from current pod state.
func (ec *EndpointsController) reconcile(ctx context.Context, svc *types.Service) {
	pods, _, err := ec.client.ListPods(ctx, svc.Namespace)
	if err != nil {
		return
	}

	var subsets []string
	for _, pod := range pods {
		if !matchesSelector(pod.Labels, svc.Spec.Selector) {
			continue
		}
		if pod.Status.Phase != types.PodRunning || pod.Status.PodIP == "" {
			continue // only route to ready pods with a real IP
		}
		subsets = append(subsets, fmt.Sprintf("%s:%d", pod.Status.PodIP, svc.Spec.TargetPort))
	}

	endpoints := &types.Endpoints{
		ObjectMeta: types.ObjectMeta{Name: svc.Name, Namespace: svc.Namespace},
		Subsets:    subsets,
	}

	// Upsert — create or update the Endpoints object.
	if err := ec.client.UpsertEndpoints(ctx, endpoints); err != nil {
		log.Printf("endpoints %s: %v", svc.Name, err)
		return
	}
	log.Printf("endpoints %s -> %v", svc.Name, subsets)
}

// matchesSelector returns true if all selector labels are present on the pod.
func matchesSelector(podLabels, selector map[string]string) bool {
	for k, v := range selector {
		if podLabels[k] != v {
			return false
		}
	}
	return true
}
