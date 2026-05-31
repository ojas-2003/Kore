package apiserver

import (
	"encoding/json"
	"fmt"
	"kore/internal/etcdstore"
	"kore/internal/types"
	"net/http"
	"sync/atomic"
	"time"
)

var ipCounter atomic.Uint32

func init() {
	// Time-based seed so restarts don't re-issue the same IPs.
	ipCounter.Store(uint32(time.Now().Unix() & 0xFE))
}

func allocateClusterIP() string {
	n := ipCounter.Add(1)
	// 127.96.x.x — entire 127.0.0.0/8 is loopback on macOS and Linux,
	// so these addresses are bindable without root or ifconfig aliases.
	return fmt.Sprintf("127.96.%d.%d", (n>>8)&0xFF, n&0xFF)
}

type Server struct {
	store *etcdstore.Store
}

func New(s *etcdstore.Store) *Server {
	return &Server{store: s}
}

func respond(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(body)
}

func errorResponse(w http.ResponseWriter, status int, msg string) {
	respond(w, status, map[string]string{"error": msg})
}

// POST /pods
func (s *Server) CreatePod(w http.ResponseWriter, r *http.Request) {
	var pod types.Pod
	if err := json.NewDecoder(r.Body).Decode(&pod); err != nil {
		errorResponse(w, http.StatusBadRequest, "invalid pod JSON")
		return
	}
	if err := s.store.CreatePod(r.Context(), &pod); err != nil {
		errorResponse(w, http.StatusConflict, err.Error())
		return
	}
	respond(w, http.StatusCreated, pod)
}

// GET /pods/{namespace}/{name}
func (s *Server) GetPod(w http.ResponseWriter, r *http.Request) {
	namespace := r.PathValue("namespace")
	name := r.PathValue("name")

	pod, err := s.store.GetPod(r.Context(), namespace, name)
	if err != nil {
		errorResponse(w, http.StatusNotFound, err.Error())
		return
	}
	respond(w, http.StatusOK, pod)
}

// GET /pods/{namespace}
func (s *Server) ListPods(w http.ResponseWriter, r *http.Request) {
	namespace := r.PathValue("namespace")
	pods, rev, err := s.store.ListPods(r.Context(), namespace)
	if err != nil {
		errorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.Header().Set("X-Resource-Version", fmt.Sprintf("%d", rev))
	respond(w, http.StatusOK, pods)
}

// PUT /pods/{namespace}/{name}
func (s *Server) UpdatePod(w http.ResponseWriter, r *http.Request) {
	var pod types.Pod
	if err := json.NewDecoder(r.Body).Decode(&pod); err != nil {
		errorResponse(w, http.StatusBadRequest, "invalid pod JSON")
		return
	}
	pod.Namespace = r.PathValue("namespace")
	pod.Name = r.PathValue("name")
	if err := s.store.UpdatePod(r.Context(), &pod); err != nil {
		errorResponse(w, http.StatusConflict, err.Error())
		return
	}
	respond(w, http.StatusOK, pod)
}

// DELETE /pods/{namespace}/{name}
func (s *Server) DeletePod(w http.ResponseWriter, r *http.Request) {
	namespace := r.PathValue("namespace")
	name := r.PathValue("name")

	if err := s.store.DeletePod(r.Context(), namespace, name); err != nil {
		errorResponse(w, http.StatusNotFound, err.Error())
		return
	}
	respond(w, http.StatusOK, map[string]string{"status": "deleted"})
}

// GET /watch/pods/{namespace}
// Uses Server-Sent Events — connection stays open, server pushes one JSON line per event.
func (s *Server) WatchPods(w http.ResponseWriter, r *http.Request) {
	namespace := r.PathValue("namespace")

	var startRev uint64
	if rv := r.URL.Query().Get("resourceVersion"); rv != "" {
		fmt.Sscanf(rv, "%d", &startRev)
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")

	flusher, _ := w.(http.Flusher)
	events := s.store.WatchPods(r.Context(), namespace, startRev)

	for event := range events {
		data, _ := json.Marshal(event)
		fmt.Fprintf(w, "data: %s\n\n", data)
		flusher.Flush()
	}
}

// POST /nodes
func (s *Server) CreateNode(w http.ResponseWriter, r *http.Request) {
	var node types.Node
	if err := json.NewDecoder(r.Body).Decode(&node); err != nil {
		errorResponse(w, http.StatusBadRequest, "invalid node JSON")
		return
	}
	if node.Name == "" {
		errorResponse(w, http.StatusBadRequest, "node name is required")
		return
	}
	if err := s.store.CreateNode(r.Context(), &node); err != nil {
		errorResponse(w, http.StatusConflict, err.Error())
		return
	}
	respond(w, http.StatusCreated, node)
}

// GET /nodes/{name}
func (s *Server) GetNode(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	node, err := s.store.GetNode(r.Context(), name)
	if err != nil {
		errorResponse(w, http.StatusNotFound, err.Error())
		return
	}
	respond(w, http.StatusOK, node)
}

// GET /nodes
func (s *Server) ListNodes(w http.ResponseWriter, r *http.Request) {
	nodes, rev, err := s.store.ListNodes(r.Context())
	if err != nil {
		errorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.Header().Set("X-Resource-Version", fmt.Sprintf("%d", rev))
	respond(w, http.StatusOK, nodes)
}

// PUT /nodes/{name}
func (s *Server) UpdateNode(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")

	var node types.Node
	if err := json.NewDecoder(r.Body).Decode(&node); err != nil {
		errorResponse(w, http.StatusBadRequest, "invalid node JSON")
		return
	}
	// Guard against the URL and body disagreeing on which node this is.
	if node.Name != name {
		errorResponse(w, http.StatusBadRequest, "node name in URL and body do not match")
		return
	}
	if err := s.store.UpdateNode(r.Context(), &node); err != nil {
		errorResponse(w, http.StatusConflict, err.Error())
		return
	}
	respond(w, http.StatusOK, node)
}

// DELETE /nodes/{name}
func (s *Server) DeleteNode(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if err := s.store.DeleteNode(r.Context(), name); err != nil {
		errorResponse(w, http.StatusNotFound, err.Error())
		return
	}
	respond(w, http.StatusOK, map[string]string{"status": "deleted"})
}

// POST /services
func (s *Server) CreateService(w http.ResponseWriter, r *http.Request) {
	var svc types.Service
	if err := json.NewDecoder(r.Body).Decode(&svc); err != nil {
		errorResponse(w, http.StatusBadRequest, "invalid service JSON")
		return
	}
	if svc.Spec.ClusterIP == "" {
		svc.Spec.ClusterIP = allocateClusterIP()
	}
	if err := s.store.CreateService(r.Context(), &svc); err != nil {
		errorResponse(w, http.StatusConflict, err.Error())
		return
	}
	respond(w, http.StatusCreated, svc)
}

// GET /services/{namespace}/{name}
func (s *Server) GetService(w http.ResponseWriter, r *http.Request) {
	svc, err := s.store.GetService(r.Context(), r.PathValue("namespace"), r.PathValue("name"))
	if err != nil {
		errorResponse(w, http.StatusNotFound, err.Error())
		return
	}
	respond(w, http.StatusOK, svc)
}

// GET /services/{namespace}
func (s *Server) ListServices(w http.ResponseWriter, r *http.Request) {
	svcs, rev, err := s.store.ListServices(r.Context(), r.PathValue("namespace"))
	if err != nil {
		errorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.Header().Set("X-Resource-Version", fmt.Sprintf("%d", rev))
	respond(w, http.StatusOK, svcs)
}

// PUT /services/{namespace}/{name}
func (s *Server) UpdateService(w http.ResponseWriter, r *http.Request) {
	var svc types.Service
	if err := json.NewDecoder(r.Body).Decode(&svc); err != nil {
		errorResponse(w, http.StatusBadRequest, "invalid service JSON")
		return
	}
	svc.Namespace = r.PathValue("namespace")
	svc.Name = r.PathValue("name")
	if err := s.store.UpdateService(r.Context(), &svc); err != nil {
		errorResponse(w, http.StatusConflict, err.Error())
		return
	}
	respond(w, http.StatusOK, svc)
}

// DELETE /services/{namespace}/{name}
func (s *Server) DeleteService(w http.ResponseWriter, r *http.Request) {
	if err := s.store.DeleteService(r.Context(), r.PathValue("namespace"), r.PathValue("name")); err != nil {
		errorResponse(w, http.StatusNotFound, err.Error())
		return
	}
	respond(w, http.StatusOK, map[string]string{"status": "deleted"})
}

// GET /endpoints/{namespace}/{name}
func (s *Server) GetEndpoints(w http.ResponseWriter, r *http.Request) {
	ep, err := s.store.GetEndpoints(r.Context(), r.PathValue("namespace"), r.PathValue("name"))
	if err != nil {
		errorResponse(w, http.StatusNotFound, err.Error())
		return
	}
	respond(w, http.StatusOK, ep)
}

// PUT /endpoints/{namespace}/{name}  — used by the endpoints controller
func (s *Server) UpsertEndpoints(w http.ResponseWriter, r *http.Request) {
	var ep types.Endpoints
	if err := json.NewDecoder(r.Body).Decode(&ep); err != nil {
		errorResponse(w, http.StatusBadRequest, "invalid endpoints JSON")
		return
	}
	ep.Namespace = r.PathValue("namespace")
	ep.Name = r.PathValue("name")
	if err := s.store.UpsertEndpoints(r.Context(), &ep); err != nil {
		errorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}
	respond(w, http.StatusOK, ep)
}

// POST /deployments
func (s *Server) CreateDeployment(w http.ResponseWriter, r *http.Request) {
	var dep types.Deployment
	if err := json.NewDecoder(r.Body).Decode(&dep); err != nil {
		errorResponse(w, http.StatusBadRequest, "invalid deployment JSON")
		return
	}
	if err := s.store.CreateDeployment(r.Context(), &dep); err != nil {
		errorResponse(w, http.StatusConflict, err.Error())
		return
	}
	respond(w, http.StatusCreated, dep)
}

// GET /deployments/{namespace}/{name}
func (s *Server) GetDeployment(w http.ResponseWriter, r *http.Request) {
	dep, err := s.store.GetDeployment(r.Context(), r.PathValue("namespace"), r.PathValue("name"))
	if err != nil {
		errorResponse(w, http.StatusNotFound, err.Error())
		return
	}
	respond(w, http.StatusOK, dep)
}

// GET /deployments/{namespace}
func (s *Server) ListDeployments(w http.ResponseWriter, r *http.Request) {
	deps, rev, err := s.store.ListDeployments(r.Context(), r.PathValue("namespace"))
	if err != nil {
		errorResponse(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.Header().Set("X-Resource-Version", fmt.Sprintf("%d", rev))
	respond(w, http.StatusOK, deps)
}

// PUT /deployments/{namespace}/{name}
func (s *Server) UpdateDeployment(w http.ResponseWriter, r *http.Request) {
	var dep types.Deployment
	if err := json.NewDecoder(r.Body).Decode(&dep); err != nil {
		errorResponse(w, http.StatusBadRequest, "invalid deployment JSON")
		return
	}
	dep.Namespace = r.PathValue("namespace")
	dep.Name = r.PathValue("name")
	if err := s.store.UpdateDeployment(r.Context(), &dep); err != nil {
		errorResponse(w, http.StatusConflict, err.Error())
		return
	}
	respond(w, http.StatusOK, dep)
}

// DELETE /deployments/{namespace}/{name}
func (s *Server) DeleteDeployment(w http.ResponseWriter, r *http.Request) {
	if err := s.store.DeleteDeployment(r.Context(), r.PathValue("namespace"), r.PathValue("name")); err != nil {
		errorResponse(w, http.StatusNotFound, err.Error())
		return
	}
	respond(w, http.StatusOK, map[string]string{"status": "deleted"})
}
