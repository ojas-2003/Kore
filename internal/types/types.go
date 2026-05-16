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

type PodSpec struct {
	Image    string `json:"image"`
	NodeName string `json:"nodeName"` // empty = unscheduled
}
