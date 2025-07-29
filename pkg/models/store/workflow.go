package store

import "time"

type Workflow struct {
	Account         string
	Workspace       string
	CreatedAt       time.Time
	LastProcessedAt *time.Time
	Error           *string
}

type WorkflowIdentity struct {
	Workspace string
}
