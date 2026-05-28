package main

import (
	"fmt"
	"log"
	"net/http"

	"kore/internal/apiserver"
	"kore/internal/etcdstore"
)

func main() {
	store, err := etcdstore.New([]string{"localhost:2379"})
	if err != nil {
		log.Fatalf("etcd: %v", err)
	}
	defer store.Close()

	srv := apiserver.New(store)
	mux := http.NewServeMux()
	mux.HandleFunc("POST /pods", srv.CreatePod)
	mux.HandleFunc("GET /pods/{namespace}/{name}", srv.GetPod)
	mux.HandleFunc("GET /pods/{namespace}", srv.ListPods)
	mux.HandleFunc("PUT /pods/{namespace}/{name}", srv.UpdatePod)
	mux.HandleFunc("DELETE /pods/{namespace}/{name}", srv.DeletePod)
	mux.HandleFunc("GET /watch/pods/{namespace}", srv.WatchPods)
	mux.HandleFunc("POST /nodes", srv.CreateNode)
	mux.HandleFunc("GET /nodes", srv.ListNodes)
	mux.HandleFunc("GET /nodes/{name}", srv.GetNode)
	mux.HandleFunc("PUT /nodes/{name}", srv.UpdateNode)
	mux.HandleFunc("DELETE /nodes/{name}", srv.DeleteNode)

	mux.HandleFunc("POST /deployments", srv.CreateDeployment)
	mux.HandleFunc("GET /deployments/{namespace}/{name}", srv.GetDeployment)
	mux.HandleFunc("GET /deployments/{namespace}", srv.ListDeployments)
	mux.HandleFunc("PUT /deployments/{namespace}/{name}", srv.UpdateDeployment)
	mux.HandleFunc("DELETE /deployments/{namespace}/{name}", srv.DeleteDeployment)

	fmt.Println("kore api server :8080")
	http.ListenAndServe(":8080", mux)
}
