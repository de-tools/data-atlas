package workflow

import (
	"context"
	"database/sql"
	"fmt"
	"sync"

	"github.com/de-tools/data-atlas/pkg/models/store"

	"github.com/de-tools/data-atlas/pkg/services/account"

	"github.com/de-tools/data-atlas/pkg/models/domain"
	"github.com/de-tools/data-atlas/pkg/store/duckdb/usage"
	"github.com/de-tools/data-atlas/pkg/store/duckdb/workflow"
)

type Controller interface {
	Start(ctx context.Context, workspace string) error
	Cancel(ctx context.Context, workspace string) error
}

type workflowDescriptor struct {
	cancelFunc context.CancelFunc
	wf         *store.Workflow
	runner     *Runner
}

type DefaultController struct {
	workflowStore      workflow.Store
	db                 *sql.DB
	explorer           account.Explorer
	embeddedUsageStore usage.Store

	mu        sync.Mutex
	workflows map[string]workflowDescriptor
}

func NewController(
	db *sql.DB,
	explorer account.Explorer,
	workflowStore workflow.Store,
	embeddedUsageStore usage.Store,
) *DefaultController {
	ctrl := &DefaultController{
		db:                 db,
		workflowStore:      workflowStore,
		explorer:           explorer,
		embeddedUsageStore: embeddedUsageStore,
		workflows:          make(map[string]workflowDescriptor),
	}

	return ctrl
}

func (ctrl *DefaultController) Init(ctx context.Context) error {
	workflows, err := ctrl.workflowStore.ListWorkflows(ctx, []string{})
	if err != nil {
		return err
	}

	for _, wf := range workflows {
		ctrl.startWorkflow(ctx, wf)
	}

	return nil
}

func (ctrl *DefaultController) Start(ctx context.Context, workspace string) error {
	wf, err := ctrl.workflowStore.CreateWorkflow(ctx, store.WorkflowIdentity{
		Workspace: workspace,
	})
	if err != nil {
		return err
	}

	ctrl.startWorkflow(ctx, wf)
	return nil
}

func (ctrl *DefaultController) Cancel(_ context.Context, workspace string) error {
	ctrl.mu.Lock()
	defer ctrl.mu.Unlock()

	desc, ok := ctrl.workflows[workspace]
	if !ok {
		return fmt.Errorf("workflow not running: %s", workspace)
	}
	desc.cancelFunc()
	<-desc.runner.Done()

	delete(ctrl.workflows, workspace)
	return nil
}

func (ctrl *DefaultController) startWorkflow(ctx context.Context, wf *store.Workflow) {
	ctrl.mu.Lock()
	defer ctrl.mu.Unlock()

	ctx, cancel := context.WithCancel(ctx)

	costExplorer, err := ctrl.explorer.GetWorkspaceCostManager(ctx, domain.Workspace{Name: wf.Workspace})
	if err != nil {
		cancel()
		return
	}

	runner := NewRunner(wf, ctrl.db, ctrl.workflowStore, costExplorer, ctrl.embeddedUsageStore)
	ctrl.workflows[wf.Workspace] = workflowDescriptor{
		cancelFunc: cancel,
		wf:         wf,
		runner:     runner,
	}

	go runner.Run(ctx)
}
