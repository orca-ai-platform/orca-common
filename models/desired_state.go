package models

import (
	"time"
)

// ADDED: Desired resource state
type DesiredResource struct {
	ID            int                    `json:"id"`
	CollectionID  string                 `json:"collection_id"`
	Kind          string                 `json:"kind"`
	APIVersion    string                 `json:"api_version"`
	Namespace     string                 `json:"namespace"`
	Name          string                 `json:"name"`
	UID           string                 `json:"uid,omitempty"`
	Spec          map[string]interface{} `json:"spec"`
	Metadata      map[string]interface{} `json:"metadata"`
	CreatedBy     string                 `json:"created_by"`
	CreatedAt     time.Time              `json:"created_at"`
	UpdatedAt     time.Time              `json:"updated_at"`
	Status        string                 `json:"status"` // pending, applied, drift, error
	LastAppliedAt *time.Time             `json:"last_applied_at,omitempty"`
	ApplyError    string                 `json:"apply_error,omitempty"`
}

// ADDED: Resource change record
type ResourceChange struct {
	ID                int                    `json:"id"`
	DesiredResourceID int                    `json:"desired_resource_id"`
	ChangeType        string                 `json:"change_type"` // create, update, delete
	Diff              map[string]interface{} `json:"diff"`
	AppliedBy         string                 `json:"applied_by"`
	AppliedAt         time.Time              `json:"applied_at"`
	Status            string                 `json:"status"` // success, failed, pending
	ErrorMessage      string                 `json:"error_message,omitempty"`
}

// ADDED: Pending change waiting for approval
type PendingChange struct {
	ID                int                    `json:"id"`
	DesiredResourceID int                    `json:"desired_resource_id"`
	ChangeType        string                 `json:"change_type"`
	Diff              map[string]interface{} `json:"diff"`
	RequestedBy       string                 `json:"requested_by"`
	RequestedAt       time.Time              `json:"requested_at"`
	Status            string                 `json:"status"` // pending, approved, rejected, applied
	ReviewedBy        string                 `json:"reviewed_by,omitempty"`
	ReviewedAt        *time.Time             `json:"reviewed_at,omitempty"`
	ReviewComment     string                 `json:"review_comment,omitempty"`
}

// ADDED: Diff result between desired and live state
type ResourceDiff struct {
	ResourceKey    string                 `json:"resource_key"` // namespace/name/kind
	DriftDetected  bool                   `json:"drift_detected"`
	ChangeType     string                 `json:"change_type"` // none, create, update, delete
	DesiredState   map[string]interface{} `json:"desired_state,omitempty"`
	LiveState      map[string]interface{} `json:"live_state,omitempty"`
	Diff           []DiffOperation        `json:"diff,omitempty"`
	CanAutoApply   bool                   `json:"can_auto_apply"`
	RiskLevel      string                 `json:"risk_level"` // low, medium, high
	ImpactAnalysis string                 `json:"impact_analysis,omitempty"`
}

// ADDED: Individual diff operation (JSON Patch format)
type DiffOperation struct {
	Op    string      `json:"op"`   // add, remove, replace
	Path  string      `json:"path"` // JSON path
	Value interface{} `json:"value,omitempty"`
	From  interface{} `json:"from,omitempty"`
}

// ADDED: Apply plan (dry-run result)
type ApplyPlan struct {
	Resources      []ResourceDiff `json:"resources"`
	TotalChanges   int            `json:"total_changes"`
	Creates        int            `json:"creates"`
	Updates        int            `json:"updates"`
	Deletes        int            `json:"deletes"`
	HighRiskCount  int            `json:"high_risk_count"`
	RequiresReview bool           `json:"requires_review"`
	EstimatedTime  string         `json:"estimated_time"`
}
