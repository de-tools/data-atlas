package analyzers

import (
	"context"
	"fmt"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/to"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/costmanagement/armcostmanagement"
	"github.com/de-tools/data-atlas/pkg/models/domain"
	"github.com/de-tools/data-atlas/pkg/services/cost"
)

type AnalyzerConfig struct {
	ServiceName    string // e.g. "Microsoft.Compute", "Microsoft.Sql"
	ResourceType   string // e.g. "azure_vm", "azure_sql"
	DisplayName    string // e.g. "Virtual Machines", "SQL Database"
	SubscriptionID string
}

type analyzer struct {
	costFactory *armcostmanagement.ClientFactory
	config      AnalyzerConfig
	scope       string
}

func NewQueryAnalyzer(factory *armcostmanagement.ClientFactory, config AnalyzerConfig) cost.Analyzer {
	return &analyzer{
		costFactory: factory,
		config:      config,
		scope:       fmt.Sprintf("/subscriptions/%s", config.SubscriptionID),
	}
}

func (a *analyzer) GetResourceType() string {
	return a.config.ResourceType
}

func (a *analyzer) CollectUsage(ctx context.Context, days int) ([]domain.ResourceCost, error) {
	client := a.costFactory.NewQueryClient()

	timeFrom := time.Now().AddDate(0, 0, -days)
	timeTo := time.Now()

	exportType := armcostmanagement.ExportTypeActualCost
	granularity := armcostmanagement.GranularityTypeDaily
	timeframe := armcostmanagement.TimeframeTypeCustom
	dimension := armcostmanagement.QueryColumnTypeDimension

	params := armcostmanagement.QueryDefinition{
		Type: &exportType,
		Dataset: &armcostmanagement.QueryDataset{
			Granularity: &granularity,
			Grouping: []*armcostmanagement.QueryGrouping{
				{
					Name: to.Ptr(a.config.DisplayName),
					Type: &dimension,
				},
			},
		},
		Timeframe: &timeframe,
		TimePeriod: &armcostmanagement.QueryTimePeriod{
			From: &timeFrom,
			To:   &timeTo,
		},
	}

	result, err := client.Usage(ctx, a.scope, params, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to query costs: %w", err)
	}

	var costs []domain.ResourceCost
	// Transform the result to ResourceCost objects
	for _, row := range result.Properties.Rows {
		if len(row) < 4 {
			continue
		}

		cost := domain.ResourceCost{
			StartTime: timeFrom,
			EndTime:   timeTo,
			Resource: domain.Resource{
				Platform:    "Azure",
				Service:     a.config.ServiceName,
				Name:        fmt.Sprintf("%v", row[0]),
				Description: fmt.Sprintf("Azure %s Usage", a.config.DisplayName),
				Metadata: struct {
					ID        string
					AccountID string
					UserID    string
					Region    string
				}{
					ID:        fmt.Sprintf("%v", row[0]),
					AccountID: a.config.SubscriptionID,
					Region:    fmt.Sprintf("%v", row[1]),
				},
			},
			Costs: []domain.CostComponent{{
				Type:        "compute",
				Value:       float64(row[2].(float64)),
				Unit:        "Units",
				TotalAmount: row[3].(float64),
				Currency:    "USD",
				Description: fmt.Sprintf("%s resource usage", a.config.DisplayName),
			}},
		}
		costs = append(costs, cost)
	}

	return costs, nil
}

func (a *analyzer) GenerateReport(ctx context.Context, days int) (*domain.Report, error) {
	costs, err := a.CollectUsage(ctx, days)
	if err != nil {
		return nil, err
	}

	var totalCost float64
	details := make([]domain.ReportDetail, 0)

	for _, cost := range costs {
		for _, component := range cost.Costs {
			totalCost += component.TotalAmount
			details = append(details, domain.ReportDetail{
				Name:        cost.Resource.Name,
				Value:       component.TotalAmount,
				Unit:        "USD",
				Description: fmt.Sprintf("%s cost for %s", a.config.DisplayName, cost.Resource.Name),
			})
		}
	}

	return &domain.Report{
		Title: fmt.Sprintf("Azure %s Cost Analysis", a.config.DisplayName),
		Period: domain.TimePeriod{
			Start:    time.Now().AddDate(0, 0, -days),
			End:      time.Now(),
			Duration: days,
		},
		Sections: []domain.ReportSection{{
			Title:   fmt.Sprintf("%s Usage", a.config.DisplayName),
			Details: details,
			Summary: map[string]interface{}{
				"Total Cost": totalCost,
			},
		}},
		TotalAmount: totalCost,
		Currency:    "USD",
	}, nil
}
