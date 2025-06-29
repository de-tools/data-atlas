package adapters

import (
	"fmt"

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
			Metadata:    usage.Metadata,
		},
		Costs: []domain.CostComponent{{
			Type:        "compute",
			Value:       usage.Quantity,
			Unit:        usage.Unit,
			Rate:        usage.Rate,
			TotalAmount: usage.Quantity * usage.Rate,
			Currency:    usage.Currency,
			Description: fmt.Sprintf("DBUs consumed (SKU: %s)", usage.SKU),
		}},
	}
}
