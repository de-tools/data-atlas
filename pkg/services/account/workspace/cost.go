package workspace

import (
	"context"
	"fmt"
	"time"

	"github.com/de-tools/data-atlas/pkg/adapters"
	"github.com/de-tools/data-atlas/pkg/store/usage"

	"github.com/de-tools/data-atlas/pkg/models/domain"
)

type CostManager interface {
	GetResourcesCost(
		ctx context.Context,
		res domain.WorkspaceResources,
		startTime, endTime time.Time,
	) ([]domain.ResourceCost, error)
}

type workspaceCostManager struct {
	usageStore usage.Store
}

func NewCostManager(usageStore usage.Store) CostManager {
	return &workspaceCostManager{
		usageStore: usageStore,
	}
}

func (w *workspaceCostManager) GetResourcesCost(
	ctx context.Context,
	res domain.WorkspaceResources,
	startTime, endTime time.Time,
) ([]domain.ResourceCost, error) {
	if !startTime.Before(endTime) {
		return nil, fmt.Errorf("invalid time range: start time (%s) must be before end time (%s)",
			startTime.Format("2006-01-02"),
			endTime.Format("2006-01-02"))
	}

	resourceTypes := validResourceTypes(res.Resources)
	records, err := w.usageStore.GetResourcesUsage(ctx, resourceTypes, startTime, endTime)
	if err != nil {
		return nil, err
	}

	var costs []domain.ResourceCost
	for _, record := range records {
		costs = append(costs, adapters.MapStoreUsageRecordToDomainCost(record))
	}

	return costs, nil
}
