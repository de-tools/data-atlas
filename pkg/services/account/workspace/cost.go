package workspace

import (
	"context"
	"fmt"
	"time"

	"github.com/de-tools/data-atlas/pkg/adapters"
	"github.com/de-tools/data-atlas/pkg/models/domain"
	"github.com/de-tools/data-atlas/pkg/models/store"
)

type CostManager interface {
	GetResourcesCost(
		ctx context.Context,
		res domain.WorkspaceResources,
		startTime, endTime time.Time,
	) ([]domain.ResourceCost, error)
	GetUsageStats(ctx context.Context, startTime *time.Time) (*domain.UsageStats, error)
	GetUsage(ctx context.Context, startTime, endTime time.Time) ([]domain.ResourceCost, error)
}

// UsageStore is the minimal interface required by CostManager for reading usage
// Implemented by both Databricks SQL and DuckDB usage stores
// Note: It excludes mutating methods like Add
// so we can plug different storage backends seamlessly.
type UsageStore interface {
	GetResourcesUsage(ctx context.Context, resources []string, startTime, endTime time.Time) ([]store.UsageRecord, error)
	GetUsage(ctx context.Context, startTime, endTime time.Time) ([]store.UsageRecord, error)
	GetUsageStats(ctx context.Context, startTime *time.Time) (*store.UsageStats, error)
}

type workspaceCostManager struct {
	usageStore UsageStore
}

func NewCostManager(usageStore UsageStore) CostManager {
	return &workspaceCostManager{
		usageStore: usageStore,
	}
}

func (w *workspaceCostManager) GetUsageStats(ctx context.Context, startTime *time.Time) (*domain.UsageStats, error) {
	stats, err := w.usageStore.GetUsageStats(ctx, startTime)
	if err != nil {
		return nil, err
	}

	return adapters.MapUsageStatsStoreToDomain(stats), nil
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

	var records []store.UsageRecord
	var err error
	// Case when we call /resources/cost without resources
	if len(resourceTypes) == 0 {
		records, err = w.usageStore.GetUsage(ctx, startTime, endTime)
	} else {
		records, err = w.usageStore.GetResourcesUsage(ctx, resourceTypes, startTime, endTime)
	}

	if err != nil {
		return nil, err
	}

	var costs []domain.ResourceCost
	for _, record := range records {
		costs = append(costs, adapters.MapStoreUsageRecordToDomainCost(record))
	}

	return costs, nil
}

func (w *workspaceCostManager) GetUsage(
	ctx context.Context,
	startTime, endTime time.Time,
) ([]domain.ResourceCost, error) {
	if !startTime.Before(endTime) {
		return nil, fmt.Errorf("invalid time range: start time (%s) must be before end time (%s)",
			startTime.Format("2006-01-02"),
			endTime.Format("2006-01-02"))
	}

	records, err := w.usageStore.GetUsage(ctx, startTime, endTime)
	if err != nil {
		return nil, err
	}

	var costs []domain.ResourceCost
	for _, record := range records {
		costs = append(costs, adapters.MapStoreUsageRecordToDomainCost(record))
	}

	return costs, nil
}
