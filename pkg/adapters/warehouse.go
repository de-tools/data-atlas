package adapters

import (
	"github.com/databricks/databricks-sdk-go/service/sql"
	"github.com/de-tools/data-atlas/pkg/models/domain"
)

// MapEndpointInfoToWarehouseMetadata converts Databricks SDK GetWarehouseResponse to our WarehouseMetadata
func MapEndpointInfoToWarehouseMetadata(warehouse *sql.GetWarehouseResponse) *domain.WarehouseMetadata {
	if warehouse == nil {
		return nil
	}
	return &domain.WarehouseMetadata{
		ID:               warehouse.Id,
		Name:             warehouse.Name,
		Size:             string(warehouse.WarehouseType),
		State:            string(warehouse.State),
		MinNumClusters:   warehouse.MinNumClusters,
		MaxNumClusters:   warehouse.MaxNumClusters,
		AutoStopMins:     warehouse.AutoStopMins,
		EnableServerless: warehouse.EnableServerlessCompute,
	}
}
