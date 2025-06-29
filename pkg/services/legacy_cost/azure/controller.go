package azure

import (
	"context"
	"fmt"

	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/costmanagement/armcostmanagement"
	"github.com/de-tools/data-atlas/pkg/models/domain"
	"github.com/de-tools/data-atlas/pkg/services/legacy_cost"
	"github.com/de-tools/data-atlas/pkg/services/legacy_cost/azure/analyzers"
)

type controller struct {
	analyzers map[string]legacy_cost.Analyzer
}

func ControllerFactory(ctx context.Context, profile string) (legacy_cost.Controller, error) {
	cfg, err := LoadConfig(ctx, profile)
	if err != nil {
		return nil, fmt.Errorf("failed to load Azure credentials: %w", err)
	}

	clientFactory, err := armcostmanagement.NewClientFactory(cfg.Credentials, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create cost management client factory: %w", err)
	}

	return NewAzureController(
		analyzers.NewQueryAnalyzer(clientFactory, analyzers.AnalyzerConfig{
			ServiceName:    "Microsoft.Compute",
			ResourceType:   "azure_vm",
			DisplayName:    "Virtual Machines",
			SubscriptionID: cfg.SubscriptionID,
		}),
		analyzers.NewQueryAnalyzer(clientFactory, analyzers.AnalyzerConfig{
			ServiceName:    "Microsoft.Sql",
			ResourceType:   "azure_sql",
			DisplayName:    "SQL Database",
			SubscriptionID: cfg.SubscriptionID,
		}),
		analyzers.NewQueryAnalyzer(clientFactory, analyzers.AnalyzerConfig{
			ServiceName:    "Microsoft.Storage",
			ResourceType:   "azure_storage",
			DisplayName:    "Storage",
			SubscriptionID: cfg.SubscriptionID,
		}),
	)
}

func NewAzureController(analyzers ...legacy_cost.Analyzer) (legacy_cost.Controller, error) {
	ctrl := &controller{
		analyzers: make(map[string]legacy_cost.Analyzer),
	}

	for _, a := range analyzers {
		resourceType := a.GetResourceType()
		if _, exists := ctrl.analyzers[resourceType]; exists {
			return nil, fmt.Errorf("duplicate analyzer for resource type: %s", resourceType)
		}
		ctrl.analyzers[resourceType] = a
	}

	if len(ctrl.analyzers) == 0 {
		return nil, fmt.Errorf("at least one analyzer must be provided")
	}

	return ctrl, nil
}

func (c *controller) GetSupportedResources() []string {
	resources := make([]string, 0, len(c.analyzers))
	for resourceType := range c.analyzers {
		resources = append(resources, resourceType)
	}
	return resources
}

func (c *controller) GetRawResourceCost(
	ctx context.Context,
	resourceType string,
	days int,
) ([]domain.ResourceCost, error) {
	analyzer, err := c.getAnalyzer(resourceType)
	if err != nil {
		return nil, err
	}
	return analyzer.CollectUsage(ctx, days)
}

func (c *controller) EstimateResourceCost(ctx context.Context, resourceType string, days int) (*domain.Report, error) {
	analyzer, err := c.getAnalyzer(resourceType)
	if err != nil {
		return nil, err
	}
	return analyzer.GenerateReport(ctx, days)
}

func (c *controller) getAnalyzer(resourceType string) (legacy_cost.Analyzer, error) {
	analyzer, exists := c.analyzers[resourceType]
	if !exists {
		return nil, fmt.Errorf("unsupported resource type: %s", resourceType)
	}
	return analyzer, nil
}
