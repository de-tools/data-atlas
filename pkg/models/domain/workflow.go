package domain

import "time"

type Workflow struct {
	Workspace         string
	CreatedAt         time.Time
	LastProcessedDate *time.Time
}
