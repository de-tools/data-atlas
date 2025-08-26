package account

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"

	"github.com/de-tools/data-atlas/pkg/store/databrickssql/pricing"
	databricksusage "github.com/de-tools/data-atlas/pkg/store/databrickssql/usage"
	"github.com/de-tools/data-atlas/pkg/store/duckdb"
	duckdbusage "github.com/de-tools/data-atlas/pkg/store/duckdb/usage"

	"github.com/databricks/databricks-sdk-go/config"
	_ "github.com/databricks/databricks-sql-go" // Required for databricks sql
	"github.com/de-tools/data-atlas/pkg/models/domain"
	"github.com/de-tools/data-atlas/pkg/models/store"
	"github.com/de-tools/data-atlas/pkg/services/account/workspace"
	dataatlasconfig "github.com/de-tools/data-atlas/pkg/services/config"
)

type Explorer interface {
	ListWorkspaces(ctx context.Context) ([]domain.Workspace, error)
	GetWorkspaceExplorer(ctx context.Context, ws domain.Workspace) (workspace.Explorer, error)
	// GetWorkspaceCostManagerCached returns a DuckDB-backed cost manager
	GetWorkspaceCostManagerCached(ctx context.Context, ws domain.Workspace) (workspace.CostManager, error)
	// GetWorkspaceCostManagerRemote returns a Databricks-backed cost manager
	GetWorkspaceCostManagerRemote(ctx context.Context, ws domain.Workspace) (workspace.CostManager, error)
}

type accountExplorer struct {
	registry dataatlasconfig.Registry
}

func NewExplorer(registry dataatlasconfig.Registry) Explorer {
	return &accountExplorer{registry: registry}
}

func (a *accountExplorer) ListWorkspaces(ctx context.Context) ([]domain.Workspace, error) {
	profiles, err := a.registry.GetProfiles(ctx)
	if err != nil {
		return nil, err
	}
	var workspaces []domain.Workspace
	for _, profile := range profiles {
		workspaces = append(workspaces, domain.Workspace{Name: profile.Name})
	}
	return workspaces, nil
}

func (a *accountExplorer) GetWorkspaceExplorer(ctx context.Context, ws domain.Workspace) (workspace.Explorer, error) {
	cfg, err := a.registry.GetConfig(ctx, domain.ConfigProfile{Name: ws.Name, Type: domain.ProfileTypeWorkspace})
	if err != nil {
		return nil, err
	}

	return workspace.NewExplorer(cfg, ws), nil
}

func (a *accountExplorer) GetWorkspaceCostManagerCached(
	ctx context.Context,
	ws domain.Workspace,
) (workspace.CostManager, error) {
	// DuckDB-backed CostManager for API read paths
	db, err := duckdb.NewDB(duckdb.Settings{DbPath: "data-atlas.db"})
	if err != nil {
		return nil, fmt.Errorf("failed to create DuckDB instance: %w", err)
	}
	usageStore, err := duckdbusage.NewWorkspaceStore(db, ws.Name)
	if err != nil {
		return nil, fmt.Errorf("failed to create DuckDB usage store: %w", err)
	}
	return workspace.NewCostManager(usageStore), nil
}

func (a *accountExplorer) GetWorkspaceCostManagerRemote(
	ctx context.Context,
	ws domain.Workspace,
) (workspace.CostManager, error) {
	cfg, err := a.registry.GetConfig(ctx, domain.ConfigProfile{Name: ws.Name, Type: domain.ProfileTypeWorkspace})
	if err != nil {
		return nil, err
	}

	warehouses, err := listWarehouses(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("listing warehouses: %w", err)
	}

	if len(warehouses) == 0 {
		return nil, fmt.Errorf("no warehouses found for workspace %s", ws.Name)
	}

	defaultWarehouse := warehouses[0]
	// Prepare the DSN for the Databricks SQL connection, removing any protocol prefix
	host := strings.TrimPrefix(strings.TrimPrefix(cfg.Host, "https://"), "http://")
	// Check if the host already has a port number, if not, add the default port for HTTPS
	if !strings.Contains(host, ":") {
		host = host + ":443"
	}

	token, err := getToken(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("getting token: %w", err)
	}

	dsn := fmt.Sprintf("token:%s@%s%s", token, host, fmt.Sprintf("/sql/1.0/warehouses/%s", defaultWarehouse.ID))

	db, err := sql.Open("databricks", dsn)
	if err != nil {
		log.Fatalf("failed to connect to Databricks: %v", err)
	}

	usageStore := databricksusage.NewStore(db, pricing.NewStore())
	costManager := workspace.NewCostManager(usageStore)
	return costManager, nil
}

func listWarehouses(ctx context.Context, cfg *config.Config) ([]store.Warehouse, error) {
	client := &http.Client{}

	host := strings.TrimPrefix(strings.TrimPrefix(cfg.Host, "https://"), "http://")
	url := fmt.Sprintf("https://%s/api/2.0/sql/warehouses", host)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}

	token, err := getToken(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("getting token: %w", err)
	}

	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", token))

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("executing request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	var response store.WarehousesResponse
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return nil, fmt.Errorf("decoding response: %w", err)
	}

	return response.Warehouses, nil
}

func getToken(ctx context.Context, cfg *config.Config) (string, error) {
	if cfg == nil {
		return "", fmt.Errorf("config cannot be nil")
	}

	if cfg.Token != "" {
		return cfg.Token, nil
	}

	ts := cfg.GetTokenSource()
	if ts == nil {
		return "", fmt.Errorf("no token source configured")
	}

	token, err := ts.Token(ctx)
	if err != nil {
		return "", fmt.Errorf("getting token: %w", err)
	}

	return token.AccessToken, nil
}
