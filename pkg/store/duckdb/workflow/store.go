package workflow

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/de-tools/data-atlas/pkg/store/duckdb"

	"github.com/de-tools/data-atlas/pkg/models/store"
	"github.com/rs/zerolog"
)

type Store interface {
	ListWorkflows(ctx context.Context, workspaces []string) ([]*store.Workflow, error)
	CreateWorkflow(ctx context.Context, workflow store.WorkflowIdentity) (*store.Workflow, error)
	UpdateWorkflow(ctx context.Context, workflow store.WorkflowIdentity, lastProcessedAt time.Time) error
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

func (d *defaultStore) ListWorkflows(ctx context.Context, workspaces []string) ([]*store.Workflow, error) {
	logger := zerolog.Ctx(ctx)
	query := `
		SELECT 
			workspace, created_at, last_processed_record_at
		FROM 
			workflow_state`

	var err error
	var rows *sql.Rows
	if len(workspaces) > 0 {
		rows, err = d.db.QueryContext(ctx, query+` WHERE workspace = ANY ($1)`, workspaces)
	} else {
		rows, err = d.db.QueryContext(ctx, query)
	}

	if err != nil {
		return nil, fmt.Errorf("query workflows: %w", err)
	}

	defer func(rows *sql.Rows) {
		err := rows.Close()
		if err != nil {
			logger.Warn().Err(err).Msg("failed to close workflow query rows")
		}
	}(rows)

	var workflows []*store.Workflow
	for rows.Next() {
		w := &store.Workflow{}
		err := rows.Scan(&w.Workspace, &w.CreatedAt, &w.LastProcessedAt)
		if err != nil {
			return nil, fmt.Errorf("scan workflow: %w", err)
		}
		workflows = append(workflows, w)
	}

	return workflows, rows.Err()
}

func (d *defaultStore) CreateWorkflow(ctx context.Context, workflow store.WorkflowIdentity) (*store.Workflow, error) {
	query := `
        SELECT workspace, created_at, last_processed_record_at 
        FROM workflow_state 
        WHERE workspace = $1`

	var existing store.Workflow
	err := d.db.QueryRowContext(ctx, query, workflow.Workspace).Scan(
		&existing.Workspace,
		&existing.CreatedAt,
		&existing.LastProcessedAt,
	)

	if err == nil {
		return &existing, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("query existing workflow: %w", err)
	}

	wf := &store.Workflow{
		Workspace: workflow.Workspace,
		CreatedAt: time.Now(),
	}

	insertQuery := `
        INSERT INTO workflow_state (workspace, created_at) 
        VALUES ($1, $2)`

	_, err = d.db.ExecContext(ctx, insertQuery, wf.Workspace, wf.CreatedAt)
	if err != nil {
		return nil, fmt.Errorf("insert workflow: %w", err)
	}

	return wf, nil
}

func (d *defaultStore) UpdateWorkflow(
	ctx context.Context,
	workflow store.WorkflowIdentity,
	lastProcessedAt time.Time,
) error {
	query := `
        UPDATE 
            workflow_state 
        SET 
            last_processed_record_at = $1
        WHERE 
            workspace = $2`

	tx := duckdb.GetTransaction(ctx)
	var result sql.Result
	var err error
	if tx == nil {
		result, err = d.db.ExecContext(ctx, query, lastProcessedAt, workflow.Workspace)
	} else {
		result, err = tx.ExecContext(ctx, query, lastProcessedAt, workflow.Workspace)
	}

	if err != nil {
		return fmt.Errorf("update workflow: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("get rows affected: %w", err)
	}

	if rows == 0 {
		return fmt.Errorf("workflow not found fo workspace: %s", workflow.Workspace)
	}

	return nil
}
