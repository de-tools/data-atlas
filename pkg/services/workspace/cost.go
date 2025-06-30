package workspace

import (
	"context"

	"github.com/de-tools/data-atlas/pkg/models/domain"
)

type CostManager interface {
	GetResourceCost(ctx context.Context, res domain.WorkspaceResource, interval int) ([]domain.ResourceCost, error)
}
