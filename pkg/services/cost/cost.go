package cost

import "github.com/de-tools/data-atlas/pkg/models/domain"

type Analyzer interface {
	GetResourceType() string
	CollectUsage(days int) ([]domain.ResourceCost, error)
	GenerateReport(days int) (*domain.Report, error)
}

// Controller defines the interface for resource cost controllers
type Controller interface {
	// EstimateResourceCost estimates the cost for a specific resource type over the specified duration in days
	EstimateResourceCost(resourceType string, days int) (*domain.Report, error)
	GetRawResourceCost(resourceType string, days int) ([]domain.ResourceCost, error)
	// GetSupportedResources returns a list of supported resource types
	GetSupportedResources() []string
}
