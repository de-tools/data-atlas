package workspace

import (
	"context"
	"fmt"

	"github.com/databricks/databricks-sdk-go"
	"github.com/databricks/databricks-sdk-go/config"
	"github.com/databricks/databricks-sdk-go/service/sql"

	"github.com/de-tools/data-atlas/pkg/models/domain"
)

type Explorer interface {
	ListSupportedResources(ctx context.Context) ([]domain.WorkspaceResource, error)
	GetWarehouseMetadata(ctx context.Context, warehouseID string) (*domain.WarehouseMetadata, error)
	ListWarehouses(ctx context.Context) ([]domain.WarehouseMetadata, error)
}

type workspaceExplorer struct {
	ws     domain.Workspace
	config *config.Config
	client *databricks.WorkspaceClient
}

func NewExplorer(config *config.Config, ws domain.Workspace) Explorer {
	// Create Databricks client - we'll handle errors in the methods that need it
	client, _ := databricks.NewWorkspaceClient(&databricks.Config{
		Host:  config.Host,
		Token: config.Token,
	})

	return &workspaceExplorer{
		ws:     ws,
		config: config,
		client: client,
	}
}

func (w *workspaceExplorer) ListSupportedResources(
	_ context.Context,
) ([]domain.WorkspaceResource, error) {
	var resources []domain.WorkspaceResource
	for resourceName, _ := range domain.SupportedResources {
		resources = append(resources, domain.WorkspaceResource{WorkspaceName: w.ws.Name, ResourceName: resourceName})
	}
	return resources, nil
}

// GetWarehouseMetadata retrieves detailed metadata for a specific warehouse
func (w *workspaceExplorer) GetWarehouseMetadata(ctx context.Context, warehouseID string) (*domain.WarehouseMetadata, error) {
	if w.client == nil {
		return nil, fmt.Errorf("databricks client not initialized")
	}

	warehouse, err := w.client.Warehouses.GetById(ctx, warehouseID)
	if err != nil {
		return nil, fmt.Errorf("failed to get warehouse %s: %w", warehouseID, err)
	}

	return mapEndpointInfoToWarehouseMetadata(warehouse), nil
}

// ListWarehouses retrieves metadata for all warehouses in the workspace
func (w *workspaceExplorer) ListWarehouses(ctx context.Context) ([]domain.WarehouseMetadata, error) {
	if w.client == nil {
		return nil, fmt.Errorf("databricks client not initialized")
	}

	warehouses, err := w.client.Warehouses.ListAll(ctx, sql.ListWarehousesRequest{})
	if err != nil {
		return nil, fmt.Errorf("failed to list warehouses: %w", err)
	}

	var result []domain.WarehouseMetadata
	for _, warehouse := range warehouses {
		// Get detailed info for each warehouse since ListAll might not return all fields
		detailed, err := w.client.Warehouses.GetById(ctx, warehouse.Id)
		if err != nil {
			continue // Skip warehouses we can't get details for
		}
		result = append(result, *mapEndpointInfoToWarehouseMetadata(detailed))
	}

	return result, nil
}

// mapEndpointInfoToWarehouseMetadata converts Databricks SDK GetWarehouseResponse to our WarehouseMetadata
func mapEndpointInfoToWarehouseMetadata(warehouse *sql.GetWarehouseResponse) *domain.WarehouseMetadata {
	return &domain.WarehouseMetadata{
		ID:               warehouse.Id,
		Name:             warehouse.Name,
		Size:             string(warehouse.WarehouseType),
		State:            string(warehouse.State),
		MinNumClusters:   int(warehouse.MinNumClusters),
		MaxNumClusters:   int(warehouse.MaxNumClusters),
		AutoStopMins:     int(warehouse.AutoStopMins),
		EnableServerless: warehouse.EnableServerlessCompute,
	}
}

func validResourceTypes(types []string) []string {
	var supportedTypes []string
	for _, rt := range types {
		if _, ok := domain.SupportedResources[rt]; ok {
			supportedTypes = append(supportedTypes, rt)
		}
	}
	return supportedTypes
}
