package analyzers

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssdk "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/de-tools/data-atlas/pkg/models/domain"
)

type rdsAnalyzer struct {
	client *rds.Client
}

func NewRDSAnalyzer(cfg awssdk.Config) *rdsAnalyzer {
	return &rdsAnalyzer{
		client: rds.NewFromConfig(cfg),
	}
}

func (a *rdsAnalyzer) GetResourceType() string {
	return "RDS"
}

func (a *rdsAnalyzer) CollectUsage(ctx context.Context, days int) ([]domain.ResourceCost, error) {
	resp, err := a.client.DescribeDBInstances(ctx, &rds.DescribeDBInstancesInput{})
	if err != nil {
		return nil, fmt.Errorf("failed to describe RDS instances: %w", err)
	}

	var costs []domain.ResourceCost
	for _, instance := range resp.DBInstances {
		hourlyRate := getRDSInstanceTypePrice(*instance.DBInstanceClass)
		storageRate := getRDSStoragePrice()

		cost := domain.ResourceCost{
			StartTime: time.Now().AddDate(0, 0, -days),
			EndTime:   time.Now(),
			Resource: domain.Resource{
				Platform:    "AWS",
				Service:     "RDS",
				Name:        aws.ToString(instance.DBInstanceIdentifier),
				Description: fmt.Sprintf("RDS Instance (%s, %s)", *instance.DBInstanceClass, *instance.Engine),
				Metadata: struct {
					ID        string
					AccountID string
					UserID    string
					Region    string
				}{
					ID:     aws.ToString(instance.DBInstanceArn),
					Region: aws.ToString(instance.AvailabilityZone),
				},
			},
			Costs: []domain.CostComponent{
				{
					Type:        "compute",
					Value:       float64(24 * days),
					Unit:        "hours",
					Rate:        hourlyRate,
					TotalAmount: hourlyRate * float64(24*days),
					Currency:    "USD",
					Description: fmt.Sprintf("Instance costs for %s", *instance.DBInstanceClass),
				},
				{
					Type:        "storage",
					Value:       float64(*instance.AllocatedStorage),
					Unit:        "GB",
					Rate:        storageRate,
					TotalAmount: storageRate * float64(*instance.AllocatedStorage) * float64(days) / 30,
					Currency:    "USD",
					Description: fmt.Sprintf("Storage costs for %d GB", *instance.AllocatedStorage),
				},
			},
		}
		costs = append(costs, cost)
	}
	return costs, nil
}

func (a *rdsAnalyzer) GenerateReport(ctx context.Context, days int) (*domain.Report, error) {
	costs, err := a.CollectUsage(ctx, days)
	if err != nil {
		return nil, err
	}

	report := &domain.Report{
		Title: "RDS Analysis",
		Period: domain.TimePeriod{
			Start:    time.Now().AddDate(0, 0, -days),
			End:      time.Now(),
			Duration: days,
		},
		Currency: "USD",
	}

	instanceSummary := make(map[string]*struct {
		totalCost     float64
		computeCost   float64
		storageCost   float64
		storageGB     float64
		hoursUsed     float64
		firstSeen     time.Time
		lastSeen      time.Time
		daysActive    int
		instanceClass string
	})

	for _, cost := range costs {
		instanceID := cost.Resource.Metadata.ID
		summary, exists := instanceSummary[instanceID]
		if !exists {
			summary = &struct {
				totalCost     float64
				computeCost   float64
				storageCost   float64
				storageGB     float64
				hoursUsed     float64
				firstSeen     time.Time
				lastSeen      time.Time
				daysActive    int
				instanceClass string
			}{
				firstSeen:  cost.StartTime,
				lastSeen:   cost.EndTime,
				daysActive: days,
			}
			instanceSummary[instanceID] = summary
		}

		for _, component := range cost.Costs {
			summary.totalCost += component.TotalAmount
			if component.Type == "compute" {
				summary.computeCost += component.TotalAmount
				summary.hoursUsed += component.Value
			} else if component.Type == "storage" {
				summary.storageCost += component.TotalAmount
				summary.storageGB = component.Value
			}
		}
	}

	for instanceID, summary := range instanceSummary {
		section := domain.ReportSection{
			Title: fmt.Sprintf("Instance: %s", instanceID),
			Summary: map[string]interface{}{
				"Days Active":    summary.daysActive,
				"Total Cost":     summary.totalCost,
				"Compute Cost":   summary.computeCost,
				"Storage Cost":   summary.storageCost,
				"Storage Size":   summary.storageGB,
				"Daily Average":  summary.totalCost / float64(summary.daysActive),
				"Average Hourly": summary.computeCost / summary.hoursUsed,
				"Hours Used":     summary.hoursUsed,
			},
			Details: []domain.ReportDetail{
				{
					Name:        "Compute Hours",
					Value:       summary.hoursUsed,
					Unit:        "hours",
					Description: "Total compute hours used",
				},
				{
					Name:        "Storage",
					Value:       summary.storageGB,
					Unit:        "GB",
					Description: "Allocated storage",
				},
				{
					Name:        "Total Cost",
					Value:       summary.totalCost,
					Unit:        "USD",
					Description: "Total cost for the period",
				},
			},
			Metadata: map[string]interface{}{
				"InstanceID": instanceID,
				"DaysActive": summary.daysActive,
			},
		}
		report.TotalAmount += summary.totalCost
		report.Sections = append(report.Sections, section)
	}

	return report, nil
}

// Pricing helpers
func getRDSInstanceTypePrice(instanceClass string) float64 {
	prices := map[string]float64{
		"db.t3.micro":   0.017,
		"db.t3.small":   0.034,
		"db.t3.medium":  0.068,
		"db.m5.large":   0.171,
		"db.m5.xlarge":  0.342,
		"db.m5.2xlarge": 0.684,
		"db.r5.large":   0.226,
		"db.r5.xlarge":  0.452,
		"db.r5.2xlarge": 0.904,
	}
	if price, ok := prices[instanceClass]; ok {
		return price
	}
	return 0.10
}

func getRDSStoragePrice() float64 {
	return 0.115 // per GB-month for gp2
}
