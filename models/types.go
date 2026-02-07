package models

import (
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

type ResourceRef struct {
	Kind       string
	APIVersion string
	Name       string
	Namespace  string
	UID        string
	GVK        schema.GroupVersionKind
}

type ResourceNode struct {
	Ref             ResourceRef
	Labels          map[string]string
	Annotations     map[string]string
	OwnerReferences []metav1.OwnerReference
	Selectors       map[string]string
	CreationTime    time.Time
	ResourceVersion string

	ReferencedBy []ResourceRef
	References   []ResourceRef
	Owns         []ResourceRef
	OwnedBy      *ResourceRef

	DiscoveredAt    time.Time
	ConfidenceScore int
}

type Collection struct {
	ID        string
	Name      string
	Namespace string

	PrimaryResource *ResourceNode

	Resources map[string]*ResourceNode

	Environment string
	Team        string
	AppName     string

	ConfidenceScore int
	Status          CollectionStatus
	DiscoveredAt    time.Time

	DependsOn []string
}

type CollectionStatus string

const (
	StatusActive        CollectionStatus = "active"
	StatusOrphaned      CollectionStatus = "orphaned"
	StatusUngrouped     CollectionStatus = "ungrouped"
	StatusLowConfidence CollectionStatus = "low-confidence"
)

type ConfidenceLevel string

const (
	ConfidenceHigh   ConfidenceLevel = "high"   // 70-100
	ConfidenceMedium ConfidenceLevel = "medium" // 40-69
	ConfidenceLow    ConfidenceLevel = "low"    // 0-39
)

type RelationshipType string

const (
	RelationOwnership RelationshipType = "ownership" // ownerReference
	RelationReference RelationshipType = "reference" // ConfigMap/Secret mount
	RelationSelector  RelationshipType = "selector"  // Service selector
	RelationLabel     RelationshipType = "label"     // Shared labels
	RelationNaming    RelationshipType = "naming"    // Name pattern
)

type Relationship struct {
	Source     ResourceRef
	Target     ResourceRef
	Type       RelationshipType
	Confidence int
	Metadata   map[string]string
}

type DiscoveryReport struct {
	ScanTime          time.Time
	ClusterName       string
	NamespacesScanned []string

	TotalResources     int
	Collections        []*Collection
	OrphanedResources  []*ResourceNode
	UngroupedResources []*ResourceNode

	Statistics DiscoveryStatistics
}

type DiscoveryStatistics struct {
	CollectionCount       int
	HighConfidenceCount   int
	MediumConfidenceCount int
	LowConfidenceCount    int
	OrphanCount           int

	ResourcesByKind map[string]int
}

func GetConfidenceLevel(score int) ConfidenceLevel {
	switch {
	case score >= 70:
		return ConfidenceHigh
	case score >= 40:
		return ConfidenceMedium
	default:
		return ConfidenceLow
	}
}

func (r *ResourceRef) Key() string {
	if r.Namespace != "" {
		return r.Kind + "/" + r.Namespace + "/" + r.Name
	}
	return r.Kind + "/" + r.Name
}
