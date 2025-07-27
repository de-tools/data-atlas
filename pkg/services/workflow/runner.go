package workflow

import (
	"context"
	"database/sql"
	"time"

	"github.com/de-tools/data-atlas/pkg/store/duckdb"

	"github.com/databricks/databricks-sql-go/logger"

	"github.com/de-tools/data-atlas/pkg/adapters"
	"github.com/de-tools/data-atlas/pkg/models/domain"
	"github.com/de-tools/data-atlas/pkg/models/store"
	"github.com/de-tools/data-atlas/pkg/services/account/workspace"
	"github.com/de-tools/data-atlas/pkg/store/duckdb/usage"
	"github.com/de-tools/data-atlas/pkg/store/duckdb/workflow"
	"github.com/rs/zerolog"
	"golang.org/x/exp/maps"
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
	zerolog.Ctx(ctx).With().Str("workspace", r.workflow.Workspace).Logger()
	defer close(r.done)
	defer close(r.progress)

	lastProcessedTime := r.workflow.LastProcessedAt
	stats, err := r.costManager.GetUsageStats(ctx, lastProcessedTime)
	if err != nil {
		logger.Error().Err(err).Msg("failed to get usage stats")
		return
	}

	startTime := *stats.FirstRecordTime
	resources := maps.Keys(workspace.SupportedResources)
	ws := r.workflow.Workspace
	processedRecords := int64(0)
	for {
		select {
		case <-ctx.Done():
			logger.Info().Msg("Workflow sync stopped")
			return
		default:
			endTime := startTime.Add(r.config.BatchInterval)

			records, err := r.costManager.GetResourcesCost(ctx, domain.WorkspaceResources{
				WorkspaceName: ws,
				Resources:     resources,
			}, startTime, endTime)

			if err != nil {
				logger.Error().Err(err).Msg("failed to get usage records")
				time.Sleep(r.config.SleepInterval)
				continue
			}

			if len(records) == 0 {
				logger.Info().Msg("no records found")
				time.Sleep(r.config.SleepInterval)
				continue
			}

			dbRecords := make([]store.UsageRecord, len(records))
			for _, record := range records {
				dbRecords = append(dbRecords, adapters.MapDomainResourceCostToStoreUsageRecord(record))
			}

			tx, err := r.db.BeginTx(ctx, nil)
			if err != nil {
				logger.Error().Err(err).Msg("failed to instantiate transaction")
				time.Sleep(r.config.SleepInterval)
				continue
			}

			ctxWithTx := duckdb.WithTransaction(ctx, tx)
			// Store records in DuckDB
			if err := r.usageStore.Add(ctxWithTx, ws, dbRecords); err != nil {
				logger.Error().Err(err).Msg("failed to store usage records")
				continue
			}

			// Update workflow state in DuckDB
			if err := r.workflowStore.UpdateWorkflow(ctxWithTx, store.WorkflowIdentity{
				Workspace: ws,
			}, endTime); err != nil {
				logger.Error().Err(err).Msg("failed to update workflow state")
				continue
			}

			processedRecords += int64(len(records))
			r.progress <- RunnerProgress{
				ProcessedRecords: processedRecords,
				TotalRecords:     stats.RecordsCount,
				LastProcessedAt:  endTime,
			}

			startTime = endTime
		}
	}
}
