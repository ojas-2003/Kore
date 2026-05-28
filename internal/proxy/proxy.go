package proxy

import (
	"context"
	"io"
	"kore/internal/client"
	"kore/internal/types"
	"log"
	"math/rand"
	"net"
	"sync"
)

// Proxy listens on a local port per Service and forwards each connection
// to a randomly chosen backing pod. This is the userspace equivalent of
// what kube-proxy programs into iptables.
type Proxy struct {
	client    *client.Client
	mu        sync.Mutex
	listeners map[string]*serviceListener // key = service name
}

type serviceListener struct {
	listener net.Listener
	backends []string // current "ip:port" endpoints
	mu       sync.Mutex
}

func New(c *client.Client) *Proxy {
	return &Proxy{
		client:    c,
		listeners: make(map[string]*serviceListener),
	}
}

// syncService ensures there's a listener for this service, forwarding to
// its current endpoints.
func (p *Proxy) syncService(ctx context.Context, svc *types.Service, endpoints []string) {
	p.mu.Lock()
	defer p.mu.Unlock()

	sl, exists := p.listeners[svc.Name]
	if !exists {
		// Start a listener on the service's ClusterIP:Port.
		// On a laptop we listen on 127.0.0.1:<port> as a stand-in for the
		// ClusterIP, since the ClusterIP isn't a real local address.
		addr := svc.Spec.ClusterIP // on Linux you can bind the real ClusterIP
		ln, err := net.Listen("tcp", addr)
		if err != nil {
			log.Printf("proxy listen %s: %v", addr, err)
			return
		}
		sl = &serviceListener{listener: ln}
		p.listeners[svc.Name] = sl
		go p.acceptLoop(ctx, sl)
		log.Printf("proxy: listening for service %s on %s", svc.Name, addr)
	}

	// Update the backend list (used by acceptLoop for each new connection).
	sl.mu.Lock()
	sl.backends = endpoints
	sl.mu.Unlock()
}

// acceptLoop handles incoming connections to a service, forwarding each
// to a randomly chosen backend pod.
func (p *Proxy) acceptLoop(ctx context.Context, sl *serviceListener) {
	for {
		conn, err := sl.listener.Accept()
		if err != nil {
			return // listener closed
		}
		go p.forward(conn, sl)
	}
}

// forward picks a backend and splices the two connections together.
func (p *Proxy) forward(client net.Conn, sl *serviceListener) {
	defer client.Close()

	sl.mu.Lock()
	backends := sl.backends
	sl.mu.Unlock()

	if len(backends) == 0 {
		return // no healthy pods — connection refused, effectively
	}

	// Random load balancing — exactly what kube-proxy's iptables mode does.
	backend := backends[rand.Intn(len(backends))]

	upstream, err := net.Dial("tcp", backend)
	if err != nil {
		return
	}
	defer upstream.Close()

	// Splice bytes both directions until either side closes.
	// This is the userspace version of what the kernel does for free
	// in iptables mode.
	done := make(chan struct{}, 2)
	go func() { io.Copy(upstream, client); done <- struct{}{} }()
	go func() { io.Copy(client, upstream); done <- struct{}{} }()
	<-done
}
