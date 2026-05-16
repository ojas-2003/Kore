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
