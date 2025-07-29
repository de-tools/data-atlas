package adapters

import (
	"github.com/de-tools/data-atlas/pkg/models/domain"
	"github.com/de-tools/data-atlas/pkg/models/store"
)

func MapStoreWorkflowToDomain(w *store.Workflow) *domain.Workflow {
	if w == nil {
		return nil
	}

	return &domain.Workflow{
		ID:                w.ID,
		Workspace:         w.Workspace,
		Status:            domain.WorkflowStatus(w.Status),
		CreatedAt:         w.CreatedAt,
		UpdatedAt:         w.UpdatedAt,
		LastProcessedDate: w.LastProcessedAt,
		Error:             w.Error,
	}
}

func MapDomainWorkflowToStore(dw *domain.Workflow) *store.Workflow {
	return &store.Workflow{
		ID:              dw.ID,
		Workspace:       dw.Workspace,
		Status:          string(dw.Status),
		CreatedAt:       dw.CreatedAt,
		UpdatedAt:       dw.UpdatedAt,
		LastProcessedAt: dw.LastProcessedDate,
		Error:           dw.Error,
	}
}
