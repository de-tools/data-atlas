package account

import (
	"context"

	"github.com/de-tools/data-atlas/pkg/models/domain"
	"github.com/de-tools/data-atlas/pkg/services/config"
	"github.com/de-tools/data-atlas/pkg/services/workspace"
)

type Explorer interface {
	ListWorkspaces(ctx context.Context) ([]domain.Workspace, error)
	GetWorkspaceExplorer(ctx context.Context, ws domain.Workspace) (workspace.Explorer, error)
	GetWorkspaceCostManager(ctx context.Context, ws domain.Workspace) (workspace.CostManager, error)
}

type accountExplorer struct {
	registry config.Registry
}

func NewExplorer(registry config.Registry) Explorer {
	return &accountExplorer{registry: registry}
}

func (a *accountExplorer) ListWorkspaces(ctx context.Context) ([]domain.Workspace, error) {
	profiles, err := a.registry.GetProfiles(ctx)
	if err != nil {
		return nil, err
	}
	var workspaces []domain.Workspace
	for _, profile := range profiles {
		workspaces = append(workspaces, domain.Workspace{Name: profile})
	}
	return workspaces, nil
}

func (a *accountExplorer) GetWorkspaceExplorer(ctx context.Context, ws domain.Workspace) (workspace.Explorer, error) {
	//TODO implement me
	panic("implement me")
}

func (a *accountExplorer) GetWorkspaceCostManager(
	ctx context.Context,
	ws domain.Workspace,
) (workspace.CostManager, error) {
	//TODO implement me
	panic("implement me")
}
