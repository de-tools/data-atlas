package workflow

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/de-tools/data-atlas/pkg/store/duckdb"

	"github.com/de-tools/data-atlas/pkg/adapters"
	"github.com/de-tools/data-atlas/pkg/models/store"
	"github.com/de-tools/data-atlas/pkg/services/account/workspace"
	"github.com/de-tools/data-atlas/pkg/store/duckdb/usage"
	"github.com/de-tools/data-atlas/pkg/store/duckdb/workflow"
	"github.com/rs/zerolog"
)

type Runner struct {
	workflow      *store.Workflow
	db            *sql.DB
	workflowStore workflow.Store
	costManager   workspace.CostManager
	usageStore    usage.Store
	done          chan struct{}
	progress      chan RunnerProgress
	config        RunnerConfig
}

type RunnerConfig struct {
	BatchInterval time.Duration
	SleepInterval time.Duration
}

type RunnerProgress struct {
	ProcessedRecords int64
	TotalRecords     int64
	LastProcessedAt  time.Time
}

func NewRunner(
	wf *store.Workflow,
	db *sql.DB,
	workflowStore workflow.Store,
	costManager workspace.CostManager,
	usageStore usage.Store,
) *Runner {
	return &Runner{
		workflow:      wf,
		db:            db,
		workflowStore: workflowStore,
		costManager:   costManager,
		usageStore:    usageStore,
		done:          make(chan struct{}),
		progress:      make(chan RunnerProgress, 100),
		config: RunnerConfig{
			BatchInterval: 7 * 24 * time.Hour,
			SleepInterval: 10 * time.Second,
		},
	}
}

func (r *Runner) Done() <-chan struct{} {
	return r.done
}

func (r *Runner) Progress() <-chan RunnerProgress {
	return r.progress
}

func (r *Runner) Run(ctx context.Context) {
	logger := zerolog.Ctx(ctx).With().Str("workspace", r.workflow.Workspace).Logger()
	defer close(r.done)
	defer close(r.progress)

	lastProcessedTime := r.workflow.LastProcessedAt
	stats, err := r.costManager.GetUsageStats(ctx, lastProcessedTime)
	if err != nil {
		logger.Error().Err(err).Msg("failed to get usage stats")
		return
	}

	var startTime time.Time
	switch {
	case lastProcessedTime != nil:
		startTime = *lastProcessedTime
	case stats != nil && stats.FirstRecordTime != nil:
		startTime = *stats.FirstRecordTime
	default:
		startTime = time.Now().Add(-r.config.BatchInterval)
	}

	ws := r.workflow.Workspace
	processedRecords := int64(0)
	for {
		select {
		case <-ctx.Done():
			logger.Info().Msg("Workflow sync stopped")
			return
		default:
			time.Sleep(r.config.SleepInterval)

			endTime := startTime.Add(r.config.BatchInterval)

			records, err := r.costManager.GetUsage(ctx, startTime, endTime)

			if err != nil {
				logger.Error().Err(err).Msg("sync, failed to get usage records")
				continue
			}

			if len(records) == 0 {
				logger.Info().Msg("sync, no records found")
				// Don't require the same empty window on the next iteration
				startTime = endTime
				continue
			}

			dbRecords := make(map[string]store.UsageRecord)
			for _, record := range records {
				// Super simple protection against duplicate records
				dbRecords[record.ID] = adapters.MapDomainResourceCostToStoreUsageRecord(record)
			}

			filteredDbRecords := make([]store.UsageRecord, 0)
			for _, record := range dbRecords {
				filteredDbRecords = append(filteredDbRecords, record)
			}

			err = r.updateWorkflow(ctx, ws, endTime, filteredDbRecords)
			if err != nil {
				continue
			}

			processedRecords += int64(len(filteredDbRecords))
			r.progress <- RunnerProgress{
				ProcessedRecords: processedRecords,
				TotalRecords:     stats.RecordsCount,
				LastProcessedAt:  endTime,
			}

			startTime = endTime
			logger.Info().Int64("processed_records", processedRecords).Msg("sync, processed records")
		}
	}
}

func (r *Runner) updateWorkflow(ctx context.Context, ws string, endTime time.Time, records []store.UsageRecord) error {
	logger := zerolog.Ctx(ctx).With().Str("workspace", r.workflow.Workspace).Logger()

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		logger.Error().Err(err).Msg("sync, failed to instantiate transaction")
		return err
	}

	// Defer rollback - will be no-op if transaction is committed
	defer tx.Rollback()

	ctxWithTx := duckdb.WithTransaction(ctx, tx)
	// Store records in DuckDB
	if err := r.usageStore.Add(ctxWithTx, ws, records); err != nil {
		logger.Error().Err(err).Msg("sync, failed to store usage records")
		return err
	}

	// Update workflow state in DuckDB
	if err := r.workflowStore.UpdateWorkflow(ctxWithTx, store.WorkflowIdentity{
		Workspace: ws,
	}, endTime); err != nil {
		logger.Error().Err(err).Msg("sync, failed to update workflow state")
		return err
	}

	// Commit transaction only after all operations succeed
	if err := tx.Commit(); err != nil {
		logger.Error().Err(err).Msg("sync, failed to commit workflow update")
		return fmt.Errorf("commit transaction: %w", err)
	}

	return nil
}
