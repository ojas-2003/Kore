package types

type ObjectMeta struct {
	Name            string `json:"name"`
	Namespace       string `json:"namespace"`
	ResourceVersion uint64 `json:"resourceVersion"`
}

type Capacity struct {
	CPU    int64 `json:"cpu"`
	Memory int64 `json:"memory"`
}

type Object interface {
	GetName() string
	GetNamespace() string
	GetResourceVersion() uint64
}

type EventType string

const (
	EventAdded    EventType = "ADDED"
	EventModified EventType = "MODIFIED"
	EventDeleted  EventType = "DELETED"
)

type Event struct {
	Type   EventType
	Object Object
}

// Implement Object on Pod
func (p *Pod) GetName() string            { return p.Name }
func (p *Pod) GetNamespace() string       { return p.Namespace }
func (p *Pod) GetResourceVersion() uint64 { return p.ResourceVersion }

type Node struct {
	ObjectMeta
	Spec   NodeSpec   `json:"spec"`
	Status NodeStatus `json:"status"`
}

type NodeSpec struct {
	// For now, nothing. Real k8s puts unschedulable flags here, taints, etc.
}

type NodeStatus struct {
	Capacity    ResourceList `json:"capacity"`    // total CPU/mem the node has
	Allocatable ResourceList `json:"allocatable"` // what's left to give out
}

type ResourceList struct {
	CPU    int64 `json:"cpu"`    // millicores — 1000 = 1 full CPU
	Memory int64 `json:"memory"` // bytes
}

type PodSpec struct {
	Image     string       `json:"image"`
	NodeName  string       `json:"nodeName"`  // empty = unscheduled
	Resources ResourceList `json:"resources"` // requested CPU/mem
}

func (n *Node) GetName() string            { return n.Name }
func (n *Node) GetNamespace() string       { return n.Namespace }
func (n *Node) GetResourceVersion() uint64 { return n.ResourceVersion }

type Pod struct {
	ObjectMeta
	Spec   PodSpec   `json:"spec"`
	Status PodStatus `json:"status"` // NEW
}

type PodStatus struct {
	Phase       PodPhase `json:"phase"`
	ContainerID string   `json:"containerID,omitempty"`
	Message     string   `json:"message,omitempty"`
}

type PodPhase string

const (
	PodPending   PodPhase = "Pending"   // accepted, not yet running
	PodRunning   PodPhase = "Running"   // container is up
	PodSucceeded PodPhase = "Succeeded" // container exited 0
	PodFailed    PodPhase = "Failed"    // container exited non-zero, or couldn't start
)
