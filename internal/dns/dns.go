package dns

import (
	"context"
	"kore/internal/client"
	"log"
	"strings"

	mdns "github.com/miekg/dns"
)

type Server struct {
	client *client.Client
}

func New(c *client.Client) *Server {
	return &Server{client: c}
}

func (s *Server) Run(ctx context.Context, addr string) error {
	mdns.HandleFunc("cluster.local.", s.handle)
	mdns.HandleFunc("svc.cluster.local.", s.handle)

	server := &mdns.Server{Addr: addr, Net: "udp"}
	log.Printf("kore-dns listening on %s", addr)
	return server.ListenAndServe()
}

func (s *Server) handle(w mdns.ResponseWriter, r *mdns.Msg) {
	m := new(mdns.Msg)
	m.SetReply(r)

	for _, q := range r.Question {
		if q.Qtype != mdns.TypeA {
			continue
		}
		// Parse "web.default.svc.cluster.local." -> service "web", ns "default"
		name := strings.TrimSuffix(q.Name, ".")
		parts := strings.Split(name, ".")
		if len(parts) < 2 {
			continue
		}
		svcName, namespace := parts[0], "default"
		if len(parts) >= 2 {
			namespace = parts[1]
		}

		svc, err := s.client.GetService(context.Background(), namespace, svcName)
		if err != nil {
			continue // NXDOMAIN
		}

		rr, err := mdns.NewRR(q.Name + " A " + svc.Spec.ClusterIP)
		if err == nil {
			m.Answer = append(m.Answer, rr)
		}
	}

	w.WriteMsg(m)
}
