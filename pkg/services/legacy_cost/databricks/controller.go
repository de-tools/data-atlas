package databricks

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/url"

	"github.com/de-tools/data-atlas/pkg/services/legacy_cost"

	_ "github.com/databricks/databricks-sql-go"
	"github.com/de-tools/data-atlas/pkg/models/domain"
)

type controller struct {
	analyzers map[string]legacy_cost.Analyzer
}

func ControllerFactory(_ context.Context, configPath string) (legacy_cost.Controller, error) {
	cfg, err := LoadConfig(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load config: %w", err)
	}

	dsn := fmt.Sprintf("token:%s@%s%s", cfg.Token, cfg.Host, cfg.HTTPPath)

	params := url.Values{}
	if cfg.Catalog != "" {
		params.Set("catalog", cfg.Catalog)
	}
	if cfg.Schema != "" {
		params.Set("schema", cfg.Schema)
	}
	if qp := params.Encode(); qp != "" {
		dsn = dsn + "?" + qp
	}

	db, err := sql.Open("databricks", dsn)
	if err != nil {
		log.Fatalf("failed to connect to Databricks: %v", err)
	}

	return NewDatabricksController(
		NewDatabricksAnalyzer(db, "warehouse_id", "sqlwarehouse", "SQL Warehouse", 0.22),
		NewDatabricksAnalyzer(db, "cluster_id", "cluster", "Cluster", 0.22),
		NewDatabricksAnalyzer(db, "job_id", "job", "Job", 0.22),
	)
}

func NewDatabricksController(analyzers ...legacy_cost.Analyzer) (legacy_cost.Controller, error) {
	c := &controller{analyzers: make(map[string]legacy_cost.Analyzer)}
	for _, a := range analyzers {
		rt := a.GetResourceType()
		if _, exists := c.analyzers[rt]; exists {
			return nil, fmt.Errorf("duplicate analyzer for resource type: %s", rt)
		}
		c.analyzers[rt] = a
	}
	if len(c.analyzers) == 0 {
		return nil, fmt.Errorf("at least one analyzer must be provided")
	}
	return c, nil
}

func (c *controller) EstimateResourceCost(ctx context.Context, resourceType string, days int) (*domain.Report, error) {
	an, err := c.getAnalyzer(resourceType)
	if err != nil {
		return nil, err
	}
	return an.GenerateReport(ctx, days)
}

func (c *controller) GetRawResourceCost(
	ctx context.Context,
	resourceType string,
	days int,
) ([]domain.ResourceCost, error) {
	an, err := c.getAnalyzer(resourceType)
	if err != nil {
		return nil, err
	}
	return an.CollectUsage(ctx, days)
}

func (c *controller) GetSupportedResources() []string {
	keys := make([]string, 0, len(c.analyzers))
	for k := range c.analyzers {
		keys = append(keys, k)
	}
	return keys
}

func (c *controller) getAnalyzer(resourceType string) (legacy_cost.Analyzer, error) {
	an, ok := c.analyzers[resourceType]
	if !ok {
		return nil, fmt.Errorf("unsupported resource type: %s", resourceType)
	}
	return an, nil
}
