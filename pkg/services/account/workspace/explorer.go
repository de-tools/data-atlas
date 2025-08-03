package workspace

import (
	"context"

	"github.com/databricks/databricks-sdk-go/config"

	"github.com/de-tools/data-atlas/pkg/models/domain"
)

var SupportedResources = map[string]string{
	"sharing_materialization": "sharing_materialization_id",
	"central_clean_room":      "central_clean_room_id",
	"budget_policy":           "budget_policy_id",
	"job":                     "job_id",
	"job_run":                 "job_run_id",
	"dlt_update":              "dlt_update_id",
	"dlt_maintenance":         "dlt_maintenance_id",
	"instance_pool":           "instance_pool_id",
	"app":                     "app_id",
	"database_instance":       "database_instance_id",
	"ai_runtime_pool":         "ai_runtime_pool_id",
	"cluster":                 "cluster_id",
	"endpoint":                "endpoint_id",
	"warehouse":               "warehouse_id",
	"dlt_pipeline":            "dlt_pipeline_id",
	"metastore":               "metastore_id",
}

type Explorer interface {
	ListSupportedResources(ctx context.Context) ([]domain.WorkspaceResource, error)
}

type workspaceExplorer struct {
	ws     domain.Workspace
	config *config.Config
}

func NewExplorer(config *config.Config, ws domain.Workspace) Explorer {
	return &workspaceExplorer{ws: ws, config: config}
}

func (w *workspaceExplorer) ListSupportedResources(
	_ context.Context,
) ([]domain.WorkspaceResource, error) {
	var resources []domain.WorkspaceResource
	for resourceName, _ := range SupportedResources {
		resources = append(resources, domain.WorkspaceResource{WorkspaceName: w.ws.Name, ResourceName: resourceName})
	}
	return resources, nil
}

func validResourceTypes(types []string) []string {
	var supportedTypes []string
	for _, rt := range types {
		if _, ok := SupportedResources[rt]; ok {
			supportedTypes = append(supportedTypes, rt)
		}
	}
	return supportedTypes
}
