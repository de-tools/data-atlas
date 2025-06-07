package cost

import (
	"context"

	"github.com/de-tools/data-atlas/pkg/models/domain"
)

type Analyzer interface {
	GetResourceType() string
	CollectUsage(ctx context.Context, days int) ([]domain.ResourceCost, error)
	GenerateReport(ctx context.Context, days int) (*domain.Report, error)
}

// Controller defines the interface for resource cost controllers
type Controller interface {
	// EstimateResourceCost estimates the cost for a specific resource type over the specified duration in days
	EstimateResourceCost(ctx context.Context, resourceType string, days int) (*domain.Report, error)
	GetRawResourceCost(ctx context.Context, resourceType string, days int) ([]domain.ResourceCost, error)
	// GetSupportedResources returns a list of supported resource types
	GetSupportedResources() []string
}
