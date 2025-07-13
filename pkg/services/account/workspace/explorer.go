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
	return []domain.WorkspaceResource{
		{WorkspaceName: w.ws.Name, ResourceName: "warehouse"},
		{WorkspaceName: w.ws.Name, ResourceName: "cluster"},
		{WorkspaceName: w.ws.Name, ResourceName: "job"},
	}, nil
}
