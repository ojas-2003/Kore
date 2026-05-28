package types

type ObjectMeta struct {
	Name            string            `json:"name"`
	Namespace       string            `json:"namespace"`
	UID             string            `json:"uid,omitempty"`
	ResourceVersion uint64            `json:"resourceVersion"`
	Labels          map[string]string `json:"labels,omitempty"`
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

type PodPhase string

const (
	PodPending   PodPhase = "Pending"   // accepted, not yet running
	PodRunning   PodPhase = "Running"   // container is up
	PodSucceeded PodPhase = "Succeeded" // container exited 0
	PodFailed    PodPhase = "Failed"    // container exited non-zero, or couldn't start
)

type PodStatus struct {
	Phase       PodPhase `json:"phase"`
	ContainerID string   `json:"containerID,omitempty"`
	PodIP       string   `json:"podIP,omitempty"` // NEW
	Message     string   `json:"message,omitempty"`
}

type Service struct {
	ObjectMeta
	Spec ServiceSpec `json:"spec"`
}

type ServiceSpec struct {
	Selector   map[string]string `json:"selector"`   // which pods back this service, e.g. {"app":"web"}
	ClusterIP  string            `json:"clusterIP"`  // the virtual IP (assigned by the API server)
	Port       int               `json:"port"`       // the port the service listens on
	TargetPort int               `json:"targetPort"` // the port on the pods
}

// Endpoints is the live list of pod IPs backing a Service.
// Maintained by the endpoints controller — never set by the user.
type Endpoints struct {
	ObjectMeta
	Subsets []string `json:"subsets"` // list of "ip:port" backing the service
}

func (s *Service) GetName() string            { return s.Name }
func (s *Service) GetNamespace() string       { return s.Namespace }
func (s *Service) GetResourceVersion() uint64 { return s.ResourceVersion }

type Deployment struct {
	ObjectMeta
	Spec   DeploymentSpec   `json:"spec"`
	Status DeploymentStatus `json:"status"`
}

type DeploymentSpec struct {
	Replicas int               `json:"replicas"`
	Selector map[string]string `json:"selector"`
	Template PodTemplate       `json:"template"`
}

type PodTemplate struct {
	Labels map[string]string `json:"labels"`
	Spec   PodSpec           `json:"spec"`
}

type DeploymentStatus struct {
	Replicas          int `json:"replicas"`
	ReadyReplicas     int `json:"readyReplicas"`
	AvailableReplicas int `json:"availableReplicas"`
}

func (d *Deployment) GetName() string            { return d.Name }
func (d *Deployment) GetNamespace() string       { return d.Namespace }
func (d *Deployment) GetResourceVersion() uint64 { return d.ResourceVersion }
