package workspace

import (
	"context"

	"github.com/databricks/databricks-sdk-go/config"

	"github.com/de-tools/data-atlas/pkg/models/domain"
)

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
	for resourceName, _ := range domain.SupportedResources {
		resources = append(resources, domain.WorkspaceResource{WorkspaceName: w.ws.Name, ResourceName: resourceName})
	}
	return resources, nil
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
