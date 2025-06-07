package aws_ce

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/costexplorer"
	"github.com/aws/aws-sdk-go-v2/service/costexplorer/types"
	"github.com/de-tools/data-atlas/pkg/models/domain"
	"github.com/de-tools/data-atlas/pkg/services/cost"
)

var supportedServices = map[string]string{
	"EC2": "Amazon Elastic Compute Cloud - Compute",
	"S3":  "Amazon Simple Storage Service",
	"RDS": "Amazon Relational Database Service",
}

type controller struct {
	client *costexplorer.Client
}

func ControllerFactory(ctx context.Context, profile string) (cost.Controller, error) {
	cfg, err := LoadConfig(ctx, profile)
	if err != nil {
		return nil, fmt.Errorf("unable to load AWS SDK config: %w", err)
	}

	return &controller{
		client: costexplorer.NewFromConfig(*cfg),
	}, nil
}

func (c *controller) GetSupportedResources() []string {
	resources := make([]string, 0, len(supportedServices))
	for service := range supportedServices {
		resources = append(resources, service)
	}
	return resources
}

func (c *controller) GetRawResourceCost(
	_ context.Context,
	resourceType string,
	days int,
) ([]domain.ResourceCost, error) {
	serviceFilter, ok := supportedServices[resourceType]
	if !ok {
		return nil, fmt.Errorf("unsupported resource type: %s", resourceType)
	}

	end := time.Now()
	start := end.AddDate(0, 0, -days)

	input := &costexplorer.GetCostAndUsageInput{
		TimePeriod: &types.DateInterval{
			Start: aws.String(start.Format("2006-01-02")),
			End:   aws.String(end.Format("2006-01-02")),
		},
		Granularity: types.GranularityDaily,
		Metrics: []string{
			"UnblendedCost",
			"UsageQuantity",
			"NormalizedUsageAmount",
		},
		Filter: &types.Expression{
			And: []types.Expression{
				{
					Dimensions: &types.DimensionValues{
						Key:    types.DimensionService,
						Values: []string{serviceFilter},
					},
				},
				{
					Not: &types.Expression{
						Dimensions: &types.DimensionValues{
							Key:    types.DimensionRecordType,
							Values: []string{"Credit", "Refund"},
						},
					},
				},
			},
		},
		GroupBy: []types.GroupDefinition{
			{
				Type: types.GroupDefinitionTypeDimension,
				Key:  aws.String("USAGE_TYPE"),
			},
			{
				Type: types.GroupDefinitionTypeDimension,
				Key:  aws.String("REGION"),
			},
		},
	}

	result, err := c.client.GetCostAndUsage(context.TODO(), input)
	if err != nil {
		return nil, fmt.Errorf("failed to get cost and usage: %w", err)
	}

	return c.transformCostAndUsageResult(result, resourceType)
}

func (c *controller) EstimateResourceCost(ctx context.Context, resourceType string, days int) (*domain.Report, error) {
	costs, err := c.GetRawResourceCost(ctx, resourceType, days)
	if err != nil {
		return nil, err
	}

	report := &domain.Report{
		Title: fmt.Sprintf("%s Cost Analysis", resourceType),
		Period: domain.TimePeriod{
			Start:    time.Now().AddDate(0, 0, -days),
			End:      time.Now(),
			Duration: days,
		},
		Sections:    make([]domain.ReportSection, 0),
		TotalAmount: 0,
		Currency:    "USD",
	}

	// Group costs by region
	regionCosts := make(map[string][]domain.ResourceCost)
	for _, cost := range costs {
		region := cost.Resource.Metadata.Region
		regionCosts[region] = append(regionCosts[region], cost)
	}

	// Create sections for each region
	for region, regionalCosts := range regionCosts {
		section := c.createRegionalSection(region, regionalCosts, resourceType)
		report.Sections = append(report.Sections, section)

		for _, cost := range regionalCosts {
			for _, component := range cost.Costs {
				report.TotalAmount += component.TotalAmount
			}
		}
	}

	return report, nil
}

func (c *controller) transformCostAndUsageResult(
	result *costexplorer.GetCostAndUsageOutput,
	resourceType string,
) ([]domain.ResourceCost, error) {
	var costs []domain.ResourceCost

	for _, resultByTime := range result.ResultsByTime {
		startTime, err := time.Parse("2006-01-02", *resultByTime.TimePeriod.Start)
		if err != nil {
			return nil, fmt.Errorf("failed to parse start time: %w", err)
		}

		endTime, err := time.Parse("2006-01-02", *resultByTime.TimePeriod.End)
		if err != nil {
			return nil, fmt.Errorf("failed to parse end time: %w", err)
		}

		for _, group := range resultByTime.Groups {
			resource := c.createResource(group.Keys, group.Metrics, resourceType)
			costComponents := c.createCostComponents(group.Metrics, resourceType)

			costs = append(costs, domain.ResourceCost{
				StartTime: startTime,
				EndTime:   endTime,
				Resource:  resource,
				Costs:     costComponents,
			})
		}
	}

	return costs, nil
}

func (c *controller) createResource(
	keys []string,
	metrics map[string]types.MetricValue,
	resourceType string,
) domain.Resource {
	resource := domain.Resource{
		Platform: "AWS",
		Service:  resourceType,
		Tags:     make(map[string]string),
		Metadata: struct {
			ID        string
			AccountID string
			UserID    string
			Region    string
		}{},
	}

	for i, key := range keys {
		switch i {
		case 0: // USAGE_TYPE
			resource.Name = key
			resource.Description = c.getResourceDescription(resourceType, key)
		case 1: // REGION
			resource.Metadata.Region = key
		case 2: // Environment tag
			resource.Tags["Environment"] = key
		}
	}

	return resource
}

func (c *controller) getResourceDescription(resourceType, usageType string) string {
	switch resourceType {
	case "EC2":
		return fmt.Sprintf("EC2 Instance (%s)", usageType)
	case "S3":
		if contains(usageType, "Storage") {
			return "S3 Storage"
		}
		return "S3 Operations"
	case "RDS":
		return fmt.Sprintf("RDS Instance (%s)", usageType)
	default:
		return usageType
	}
}

func (c *controller) createCostComponents(
	metrics map[string]types.MetricValue,
	resourceType string,
) []domain.CostComponent {
	var components []domain.CostComponent

	if unblendedCost, ok := metrics["UnblendedCost"]; ok {
		amount, _ := strconv.ParseFloat(*unblendedCost.Amount, 64)
		usage, _ := strconv.ParseFloat(*metrics["UsageQuantity"].Amount, 64)

		var rate float64
		if usage > 0 {
			rate = amount / usage
		}

		costType := c.getCostType(resourceType, *metrics["UsageQuantity"].Unit)

		component := domain.CostComponent{
			Type:        costType,
			Value:       usage,
			Unit:        *metrics["UsageQuantity"].Unit,
			TotalAmount: amount,
			Rate:        rate,
			Currency:    *unblendedCost.Unit,
			Description: fmt.Sprintf("%s usage cost for %v %s", resourceType, usage, *metrics["UsageQuantity"].Unit),
		}

		components = append(components, component)
	}

	return components
}

func (c *controller) getCostType(resourceType, unit string) string {
	switch resourceType {
	case "EC2":
		return "compute"
	case "S3":
		if unit == "GB" {
			return "storage"
		}
		return "operations"
	case "RDS":
		if unit == "GB" {
			return "storage"
		}
		return "database"
	default:
		return "service"
	}
}

func (c *controller) createRegionalSection(
	region string,
	costs []domain.ResourceCost,
	resourceType string,
) domain.ReportSection {
	section := domain.ReportSection{
		Title:    fmt.Sprintf("%s Costs in %s", resourceType, region),
		Summary:  make(map[string]interface{}),
		Details:  make([]domain.ReportDetail, 0),
		Metadata: map[string]interface{}{"region": region},
	}

	var totalCost float64
	usageByType := make(map[string]float64)

	for _, cost := range costs {
		for _, component := range cost.Costs {
			totalCost += component.TotalAmount
			usageByType[component.Type] += component.Value
		}
	}

	section.Summary["total_cost"] = totalCost
	section.Summary["resource_count"] = len(costs)
	section.Summary["usage_types"] = len(usageByType)

	for usageType, value := range usageByType {
		detail := domain.ReportDetail{
			Name:        usageType,
			Value:       value,
			Unit:        c.getUnitForUsageType(resourceType, usageType),
			Description: fmt.Sprintf("Total %s usage", usageType),
		}
		section.Details = append(section.Details, detail)
	}

	return section
}

func (c *controller) getUnitForUsageType(resourceType, usageType string) string {
	switch {
	case resourceType == "EC2" && usageType == "compute":
		return "instance-hours"
	case resourceType == "S3" && usageType == "storage":
		return "GB-months"
	case resourceType == "S3" && usageType == "operations":
		return "requests"
	default:
		return "units"
	}
}

func contains(s, substr string) bool {
	return strings.Contains(strings.ToLower(s), strings.ToLower(substr))
}
