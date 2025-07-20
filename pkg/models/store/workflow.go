package store

import "time"

type Workflow struct {
	ID                string
	Workspace         string
	Status            string
	CreatedAt         time.Time
	UpdatedAt         time.Time
	LastProcessedDate time.Time
	Error             *string
}
