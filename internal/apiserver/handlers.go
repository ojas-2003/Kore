package apiserver

import (
	"encoding/json"
	"fmt"
	"kore/internal/etcdstore"
	"kore/internal/types"
	"net/http"
)

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
