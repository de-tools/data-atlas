package domain

import "time"

type WorkflowStatus string

const (
	WorkflowStatusPending   WorkflowStatus = "pending"
	WorkflowStatusFinished  WorkflowStatus = "finished"
	WorkflowStatusFailed    WorkflowStatus = "failed"
	WorkflowStatusCancelled WorkflowStatus = "cancelled"
)

type Workflow struct {
	ID                string
	Workspace         string
	Status            WorkflowStatus
	CreatedAt         time.Time
	UpdatedAt         time.Time
	LastProcessedDate time.Time
	Error             *string
}
