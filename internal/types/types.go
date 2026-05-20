package types

type ObjectMeta struct {
	Name            string `json:"name"`
	Namespace       string `json:"namespace"`
	ResourceVersion uint64 `json:"resourceVersion"`
}

type Pod struct {
	ObjectMeta
	Spec PodSpec `json:"spec"`
}

type Node struct {
	Name     string   `json:"name"`
	Capacity Capacity `json:"capacity"`
}

type Capacity struct {
	CPU    int64 `json:"cpu"`
	Memory int64 `json:"memory"`
}

type PodSpec struct {
	Image    string `json:"image"`
	NodeName string `json:"nodeName"` // empty = unscheduled
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
