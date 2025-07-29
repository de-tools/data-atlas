package adapters

import (
	"fmt"
	"maps"

	"github.com/de-tools/data-atlas/pkg/models/api"
	"github.com/de-tools/data-atlas/pkg/models/domain"
	"github.com/de-tools/data-atlas/pkg/models/store"
)

func MapStoreUsageRecordToDomainCost(usage store.UsageRecord) domain.ResourceCost {
	return domain.ResourceCost{
		StartTime: usage.StartTime,
		EndTime:   usage.EndTime,
		Resource: domain.ResourceDef{
			Platform:    "Databricks",
			Name:        usage.ID,
			Service:     usage.Resource,
			Description: fmt.Sprintf("Databricks %s %s", usage.Resource, usage.ID),
			Metadata:    maps.Clone(usage.Metadata),
		},
		Costs: []domain.CostComponent{{
			Type:        "compute",
			Value:       usage.Quantity,
			Unit:        usage.Unit,
			Rate:        usage.Rate,
			TotalAmount: usage.Quantity * usage.Rate,
			Currency:    usage.Currency,
			SKU:         usage.SKU,
			Description: fmt.Sprintf("DBUs consumed (SKU: %s)", usage.SKU),
		}},
	}
}

func MapDomainResourceCostToStoreUsageRecord(cost domain.ResourceCost) store.UsageRecord {
	computeCost := cost.Costs[0]

	return store.UsageRecord{
		ID:        cost.Resource.Name,
		Resource:  cost.Resource.Service,
		StartTime: cost.StartTime,
		EndTime:   cost.EndTime,
		Quantity:  computeCost.Value,
		Unit:      computeCost.Unit,
		Rate:      computeCost.Rate,
		Currency:  computeCost.Currency,
		SKU:       computeCost.SKU,
		Metadata:  maps.Clone(cost.Resource.Metadata),
	}
}

func MapResourceCostDomainToApi(record domain.ResourceCost) api.ResourceCost {
	apiCost := api.ResourceCost{
		StartTime: record.StartTime,
		EndTime:   record.EndTime,
		Resource:  MapResourceDefinitionDomainToApi(record.Resource),
		Costs:     []api.CostComponent{},
	}

	for _, c := range record.Costs {
		apiCost.Costs = append(apiCost.Costs, MapCostComponentDomainToApi(c))
	}

	return apiCost
}

func MapCostComponentDomainToApi(c domain.CostComponent) api.CostComponent {
	return api.CostComponent{
		Type:        c.Type,
		Value:       c.Value,
		Unit:        c.Unit,
		Rate:        c.Rate,
		TotalAmount: c.TotalAmount,
		Currency:    c.Currency,
		Description: c.Description,
	}
}

func MapResourceDefinitionDomainToApi(def domain.ResourceDef) api.ResourceDef {
	return api.ResourceDef{
		Platform:    def.Platform,
		Name:        def.Name,
		Service:     def.Service,
		Description: def.Description,
		Metadata:    def.Metadata,
	}
}

func MapUsageStatsStoreToDomain(stats *store.UsageStats) *domain.UsageStats {
	if stats == nil {
		return nil
	}

	return &domain.UsageStats{
		RecordsCount:    stats.RecordsCount,
		FirstRecordTime: stats.FirstRecordTime,
	}
}
