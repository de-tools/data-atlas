package account

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/de-tools/data-atlas/pkg/models/domain"
	workspace2 "github.com/de-tools/data-atlas/pkg/services/account/workspace"
	"github.com/de-tools/data-atlas/pkg/services/config"
	"github.com/de-tools/data-atlas/pkg/store/pricing"
	"github.com/de-tools/data-atlas/pkg/store/usage"
	"log"
)

const (
	defaultHttpPath = "/sql/1.0/warehouses/warehouse"
)

type Explorer interface {
	ListWorkspaces(ctx context.Context) ([]domain.Workspace, error)
	GetWorkspaceExplorer(ctx context.Context, ws domain.Workspace) (workspace2.Explorer, error)
	GetWorkspaceCostManager(ctx context.Context, ws domain.Workspace) (workspace2.CostManager, error)
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

func (a *accountExplorer) GetWorkspaceExplorer(ctx context.Context, ws domain.Workspace) (workspace2.Explorer, error) {
	cfg, err := a.registry.GetConfig(ctx, ws.Name)
	if err != nil {
		return nil, err
	}

	return workspace2.NewExplorer(cfg, ws), nil
}

func (a *accountExplorer) GetWorkspaceCostManager(
	ctx context.Context,
	ws domain.Workspace,
) (workspace2.CostManager, error) {
	cfg, err := a.registry.GetConfig(ctx, ws.Name)
	if err != nil {
		return nil, err
	}

	dsn := fmt.Sprintf("token:%s@%s%s", cfg.Token, cfg.Host, defaultHttpPath)

	db, err := sql.Open("databricks", dsn)
	if err != nil {
		log.Fatalf("failed to connect to Databricks: %v", err)
	}

	store := usage.NewStore(db, pricing.NewStore())
	costManager := workspace2.NewCostManager(store)

	return costManager, nil
}
