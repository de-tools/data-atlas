package workspace

import (
	"context"

	"github.com/de-tools/data-atlas/pkg/models/domain"
)

type Explorer interface {
	ListSupportedResources(ctx context.Context) ([]domain.WorkspaceResource, error)
}

type workspaceExplorer struct {
}

func NewExplorer() Explorer {
	return &workspaceExplorer{}
}

func (w *workspaceExplorer) ListSupportedResources(
	_ context.Context,
) ([]domain.WorkspaceResource, error) {
	return []domain.WorkspaceResource{
		{WorkspaceName: "", ResourceName: "warehouse"},
		{WorkspaceName: "", ResourceName: "cluster"},
		{WorkspaceName: "", ResourceName: "job"},
	}, nil
}
