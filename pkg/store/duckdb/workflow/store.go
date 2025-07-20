package workflow

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/de-tools/data-atlas/pkg/models/store"
)

type Store interface {
	ListWorkflows(ctx context.Context, statuses []string) ([]*store.Workflow, error)
	CreateWorkflow(ctx context.Context, workflow *store.Workflow) error
	UpdateWorkflowStatus(ctx context.Context, workflowID string, status string, error *string) error
	ProgressWorkflow(ctx context.Context, workflowID string, lastProcessedDate time.Time) error
}

type defaultStore struct {
	db *sql.DB
}

func NewStore(db *sql.DB) (Store, error) {
	if db == nil {
		return nil, fmt.Errorf("database connection is nil")
	}
	return &defaultStore{
		db: db,
	}, nil
}

func (s *defaultStore) ListWorkflows(ctx context.Context, statuses []string) ([]*store.Workflow, error) {
	return []*store.Workflow{}, nil
}

func (s *defaultStore) CreateWorkflow(ctx context.Context, workflow *store.Workflow) error {
	return fmt.Errorf("not implemented yet")
}

func (s *defaultStore) UpdateWorkflowStatus(
	ctx context.Context,
	workflowID string,
	status string,
	error *string,
) error {
	return fmt.Errorf("not implemented yet")
}

func (s *defaultStore) ProgressWorkflow(ctx context.Context, workflowID string, lastProcessedDate time.Time) error {
	return fmt.Errorf("not implemented yet")
}
