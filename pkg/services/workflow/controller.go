package workflow

import (
	"context"
	"fmt"
	"sync"

	"github.com/de-tools/data-atlas/pkg/services/account"

	"github.com/de-tools/data-atlas/pkg/adapters"
	"github.com/de-tools/data-atlas/pkg/models/domain"
	"github.com/de-tools/data-atlas/pkg/store/duckdb/usage"
	"github.com/de-tools/data-atlas/pkg/store/duckdb/workflow"
)

type Controller interface {
	Register(ctx context.Context, wf domain.Workflow) error
	List(ctx context.Context, status domain.WorkflowStatus) ([]domain.Workflow, error)
	Cancel(ctx context.Context, id string) error
}

type workflowDescriptor struct {
	cancelFunc context.CancelFunc
	wf         *domain.Workflow
	runner     *Runner
}

type DefaultController struct {
	workflowStore      workflow.Store
	explorer           account.Explorer
	embeddedUsageStore usage.Store

	mu        sync.Mutex
	workflows map[string]workflowDescriptor
}

func NewController(
	explorer account.Explorer,
	workflowStore workflow.Store,
	embeddedUsageStore usage.Store,
) *DefaultController {
	ctrl := &DefaultController{
		workflowStore:      workflowStore,
		explorer:           explorer,
		embeddedUsageStore: embeddedUsageStore,
		workflows:          make(map[string]workflowDescriptor),
	}

	return ctrl
}

func (ctrl *DefaultController) Init(ctx context.Context) error {
	workflows, err := ctrl.workflowStore.ListWorkflows(ctx, []string{
		string(domain.WorkflowStatusPending),
	})

	if err != nil {
		return err
	}

	for _, wf := range workflows {
		w := adapters.MapStoreWorkflowToDomain(wf)
		ctrl.startWorkflow(ctx, w)
	}

	return nil
}

func (ctrl *DefaultController) Register(ctx context.Context, wf *domain.Workflow) error {
	wf.Status = domain.WorkflowStatusPending
	err := ctrl.workflowStore.CreateWorkflow(ctx, adapters.MapDomainWorkflowToStore(wf))
	if err != nil {
		return err
	}

	ctrl.startWorkflow(ctx, wf)
	return nil
}

func (ctrl *DefaultController) Cancel(_ context.Context, id string) error {
	ctrl.mu.Lock()
	defer ctrl.mu.Unlock()

	desc, ok := ctrl.workflows[id]
	if !ok {
		return fmt.Errorf("workflow not running: %s", id)
	}
	desc.cancelFunc()
	<-desc.runner.Done()

	delete(ctrl.workflows, id)
	return nil
}

func (ctrl *DefaultController) startWorkflow(ctx context.Context, wf *domain.Workflow) {
	ctrl.mu.Lock()
	defer ctrl.mu.Unlock()

	ctx, cancel := context.WithCancel(ctx)

	costExplorer, err := ctrl.explorer.GetWorkspaceCostManager(ctx, domain.Workspace{Name: wf.Workspace})
	if err != nil {
		cancel()
		return
	}

	runner := NewRunner(wf, ctrl.workflowStore, costExplorer, ctrl.embeddedUsageStore)
	ctrl.workflows[wf.ID] = workflowDescriptor{
		cancelFunc: cancel,
		wf:         wf,
		runner:     runner,
	}

	// TODO: consider using this for status updates
	go runner.Run(ctx)
}
