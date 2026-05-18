package main

import (
	"fmt"
	"kore/internal/apiserver"
	"kore/internal/store"
	"net/http"
)

func main() {
	s := store.New()
	srv := apiserver.New(s)

	mux := http.NewServeMux()

	// Pod endpoints
	mux.HandleFunc("POST /pods", srv.CreatePod)
	mux.HandleFunc("GET /pods/{namespace}/{name}", srv.GetPod)
	mux.HandleFunc("GET /pods/{namespace}", srv.ListPods)
	mux.HandleFunc("DELETE /pods/{namespace}/{name}", srv.DeletePod)
	mux.HandleFunc("GET /watch/pods", srv.WatchPods)

	fmt.Println("kore api server listening on :8080")
	http.ListenAndServe(":8080", mux)
}
