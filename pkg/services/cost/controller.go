package cost

import (
	"context"

	"github.com/de-tools/data-atlas/pkg/adapters"
	"github.com/de-tools/data-atlas/pkg/store/sql"

	"fmt"

	_ "github.com/databricks/databricks-sql-go"
	"github.com/de-tools/data-atlas/pkg/models/domain"
)

type controller struct {
	analyzers map[string]sql.UsageAnalyzer
}

func NewController(analyzers ...sql.UsageAnalyzer) (interface{}, error) {
	c := &controller{analyzers: make(map[string]sql.UsageAnalyzer)}

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

func (c *controller) GetRawResourceCost(
	ctx context.Context,
	resourceType string,
	days int,
) ([]domain.ResourceCost, error) {
	an, err := c.getAnalyzer(resourceType)
	if err != nil {
		return nil, err
	}

	records, err := an.CollectUsage(ctx, days)
	if err != nil {
		return nil, err
	}

	costs := make([]domain.ResourceCost, 0, len(records))
	for _, record := range records {
		costs = append(costs, adapters.MapStoreUsageRecordToDomainCost(record))
	}
	return costs, nil
}

func (c *controller) GetSupportedResources() []string {
	keys := make([]string, 0, len(c.analyzers))
	for k := range c.analyzers {
		keys = append(keys, k)
	}
	return keys
}

func (c *controller) getAnalyzer(resourceType string) (sql.UsageAnalyzer, error) {
	an, ok := c.analyzers[resourceType]
	if !ok {
		return nil, fmt.Errorf("unsupported resource type: %s", resourceType)
	}
	return an, nil
}
