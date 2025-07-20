package workflow

import (
	"context"

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
	workflow      *domain.Workflow
	workflowStore workflow.Store
	costManager   workspace.CostManager
	usageStore    usage.Store
	done          chan struct{}
}

func NewRunner(
	wf *domain.Workflow,
	workflowStore workflow.Store,
	costManager workspace.CostManager,
	usageStore usage.Store,
) *Runner {
	return &Runner{
		workflow:      wf,
		workflowStore: workflowStore,
		costManager:   costManager,
		usageStore:    usageStore,
		done:          make(chan struct{}),
	}
}

func (r *Runner) Done() <-chan struct{} {
	return r.done
}
func (r *Runner) Run(ctx context.Context) {
	logger := zerolog.Ctx(ctx)

	// 🛑❌🚀✅⚠️💥
	defer close(r.done)
	endDate := r.workflow.LastProcessedDate
	resources := maps.Keys(workspace.SupportedResources)

	for {
		select {
		case <-ctx.Done():
			logger.Printf("Workflow %s stopped", r.workflow.ID)
			err := r.workflowStore.UpdateWorkflowStatus(ctx,
				r.workflow.ID,
				string(domain.WorkflowStatusCancelled),
				nil,
			)
			if err != nil {
				logger.Err(err).
					Str("workflow_id", r.workflow.ID).
					Msg("failed to update workflow status")
			}
			return
		default:
			startDate := endDate.AddDate(0, 0, -7) // Go back 7 days

			records, err := r.costManager.GetResourcesCost(ctx, domain.WorkspaceResources{
				WorkspaceName: r.workflow.Workspace,
				Resources:     resources,
			}, startDate, endDate)

			if err != nil {
				errorMsg := err.Error()
				if err := r.workflowStore.UpdateWorkflowStatus(ctx, r.workflow.ID, string(domain.WorkflowStatusFailed), &errorMsg); err != nil {
					logger.Printf("Failed to mark workflow %s as failed: %v", r.workflow.ID, err)
				}
				break
			}

			// No more records found, mark as finished
			if len(records) == 0 {
				if err := r.workflowStore.UpdateWorkflowStatus(ctx, r.workflow.ID, string(domain.WorkflowStatusFinished), nil); err != nil {
					logger.Printf("Failed to mark workflow %s as finished: %v", r.workflow.ID, err)
				}
				break
			}

			dbRecords := make([]store.UsageRecord, len(records))
			for _, record := range records {
				dbRecords = append(dbRecords, adapters.MapDomainResourceCostToStoreUsageRecord(record))
			}

			err = r.usageStore.Add(ctx, dbRecords)
			if err == nil {
				r.workflow.LastProcessedDate = endDate
				if err = r.workflowStore.ProgressWorkflow(ctx, r.workflow.ID, endDate); err != nil {
					logger.Printf("Failed to update workflow %s last processed date: %v", r.workflow.ID, err)
				}

				endDate = startDate
			}
		}
	}
}
