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
		Workspace:         w.Workspace,
		CreatedAt:         w.CreatedAt,
		LastProcessedDate: w.LastProcessedAt,
	}
}

func MapDomainWorkflowToStore(dw *domain.Workflow) *store.Workflow {
	return &store.Workflow{
		Workspace:       dw.Workspace,
		CreatedAt:       dw.CreatedAt,
		LastProcessedAt: dw.LastProcessedDate,
	}
}
