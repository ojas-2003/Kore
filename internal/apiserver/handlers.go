package apiserver

import (
	"encoding/json"
	"fmt"
	"kore/internal/store"
	"kore/internal/types"
	"net/http"
)

type Server struct {
	store *store.Store
}

func New(s *store.Store) *Server {
	return &Server{store: s}
}

// respond is a helper to write JSON responses.
func respond(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(body)
}

// errorResponse writes a JSON error body.
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
	if err := s.store.CreatePod(&pod); err != nil {
		errorResponse(w, http.StatusConflict, err.Error())
		return
	}
	respond(w, http.StatusCreated, pod)
}

// GET /pods/{namespace}/{name}
func (s *Server) GetPod(w http.ResponseWriter, r *http.Request) {
	namespace := r.PathValue("namespace")
	name := r.PathValue("name")

	pod, err := s.store.GetPod(namespace, name)
	if err != nil {
		errorResponse(w, http.StatusNotFound, err.Error())
		return
	}
	respond(w, http.StatusOK, pod)
}

// GET /pods/{namespace}
func (s *Server) ListPods(w http.ResponseWriter, r *http.Request) {
	namespace := r.PathValue("namespace")
	pods := s.store.ListPods(namespace)
	respond(w, http.StatusOK, pods)
}

// DELETE /pods/{namespace}/{name}
func (s *Server) DeletePod(w http.ResponseWriter, r *http.Request) {
	namespace := r.PathValue("namespace")
	name := r.PathValue("name")

	if err := s.store.DeletePod(namespace, name); err != nil {
		errorResponse(w, http.StatusNotFound, err.Error())
		return
	}
	respond(w, http.StatusOK, map[string]string{"status": "deleted"})
}

// GET /watch/pods
// Uses Server-Sent Events — the connection stays open,
// server pushes a JSON line per event.
func (s *Server) WatchPods(w http.ResponseWriter, r *http.Request) {
	// Tell the client: this is a streaming response, don't buffer it
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	events, cancel := s.store.Watch()
	defer cancel()

	// flusher lets us push data to the client without closing the connection
	flusher, ok := w.(http.Flusher)
	if !ok {
		errorResponse(w, http.StatusInternalServerError, "streaming not supported")
		return
	}

	for {
		select {
		case event, ok := <-events:
			if !ok {
				return // channel closed
			}
			data, _ := json.Marshal(event)
			fmt.Fprintf(w, "data: %s\n\n", data)
			flusher.Flush() // push to client immediately

		case <-r.Context().Done():
			return // client disconnected
		}
	}
}
