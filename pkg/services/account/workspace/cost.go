package workspace

import (
	"context"
	"fmt"
	"github.com/de-tools/data-atlas/pkg/adapters"
	"github.com/de-tools/data-atlas/pkg/store/usage"
	"time"

	"github.com/de-tools/data-atlas/pkg/models/domain"
)

type CostManager interface {
	GetResourceCost(ctx context.Context, res domain.WorkspaceResource, startTime, endTime time.Time) ([]domain.ResourceCost, error)
}

type workspaceCostManager struct {
	usageStore usage.Store
}

func NewCostManager(usageStore usage.Store) CostManager {
	return &workspaceCostManager{
		usageStore: usageStore,
	}
}

func (w *workspaceCostManager) GetResourceCost(ctx context.Context, res domain.WorkspaceResource, startTime, endTime time.Time) ([]domain.ResourceCost, error) {
	if !startTime.Before(endTime) {
		return nil, fmt.Errorf("invalid time range: start time (%s) must be before end time (%s)",
			startTime.Format("2006-01-02"),
			endTime.Format("2006-01-02"))
	}

	records, err := w.usageStore.GetResourceUsage(ctx, res.ResourceName, startTime, endTime)
	if err != nil {
		return nil, err
	}

	var costs []domain.ResourceCost
	for _, record := range records {
		costs = append(costs, adapters.MapStoreUsageRecordToDomainCost(record))
	}
	return costs, nil
}
