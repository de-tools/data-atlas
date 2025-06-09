package snowflake

import (
	"database/sql"
	"fmt"
	"log"

	"github.com/de-tools/data-atlas/pkg/models/domain"
	"github.com/de-tools/data-atlas/pkg/services/cost"
	"github.com/de-tools/data-atlas/pkg/services/cost/analyzers/snowflake"
	sf "github.com/snowflakedb/gosnowflake"
)

type controller struct {
	analyzers map[string]cost.Analyzer
}

// ControllerFactory creates a new Snowflake controller from a config file
func ControllerFactory(configPath string) (cost.Controller, error) {
	// Load configuration
	cfg, err := LoadConfig(configPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load config: %w", err)
	}

	dsn, err := sf.DSN(cfg)
	if err != nil {
		log.Fatalf("failed to create DSN: %v", err)
	}

	db, err := sql.Open("snowflake", dsn)
	if err != nil {
		log.Fatalf("failed to connect: %v", err)
	}

	return NewSnowflakeController(
		snowflake.NewWarehouseAnalyzer(db),
		snowflake.NewQueryAnalyzer(db, 50),
		snowflake.NewApplicationAnalyzer(db),
		snowflake.NewBlockStorageAnalyzer(db),
	)
}

// NewSnowflakeController creates a new instance of SnowflakeController with provided analyzers
func NewSnowflakeController(analyzers ...cost.Analyzer) (cost.Controller, error) {
	controller := &controller{
		analyzers: make(map[string]cost.Analyzer),
	}

	for _, a := range analyzers {
		resourceType := a.GetResourceType()
		if _, exists := controller.analyzers[resourceType]; exists {
			return nil, fmt.Errorf("duplicate analyzer for resource type: %s", resourceType)
		}
		controller.analyzers[resourceType] = a
	}

	if len(controller.analyzers) == 0 {
		return nil, fmt.Errorf("at least one analyzer must be provided")
	}

	return controller, nil
}

// EstimateResourceCost estimates the cost for a specific resource type over the specified duration
func (c *controller) EstimateResourceCost(resourceType string, days int) (*domain.Report, error) {
	analyzer, err := c.getAnalyzer(resourceType)
	if err != nil {
		return nil, err
	}

	costs, err := analyzer.GenerateReport(days)
	if err != nil {
		return nil, fmt.Errorf("failed to collect usage for %s: %w", resourceType, err)
	}

	return costs, nil
}

func (c *controller) GetRawResourceCost(resourceType string, days int) ([]domain.ResourceCost, error) {
	analyzer, err := c.getAnalyzer(resourceType)
	if err != nil {
		return nil, err
	}

	return analyzer.CollectUsage(days)
}

// GetSupportedResources returns a list of supported resource types
func (c *controller) GetSupportedResources() []string {
	resources := make([]string, 0, len(c.analyzers))
	for resourceType := range c.analyzers {
		resources = append(resources, resourceType)
	}
	return resources
}

func (c *controller) getAnalyzer(resourceType string) (cost.Analyzer, error) {
	analyzer, exists := c.analyzers[resourceType]
	if !exists {
		return nil, fmt.Errorf("unsupported resource type: %s", resourceType)
	}
	return analyzer, nil
}
