package analyzers

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssdk "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/de-tools/data-atlas/pkg/models/domain"
)

type ec2Analyzer struct {
	client *ec2.Client
}

func NewEC2Analyzer(cfg awssdk.Config) *ec2Analyzer {
	return &ec2Analyzer{
		client: ec2.NewFromConfig(cfg),
	}
}

func (a *ec2Analyzer) GetResourceType() string {
	return "EC2"
}

func (a *ec2Analyzer) CollectUsage(ctx context.Context, days int) ([]domain.ResourceCost, error) {
	resp, err := a.client.DescribeInstances(ctx, &ec2.DescribeInstancesInput{
		Filters: []types.Filter{
			{
				Name:   aws.String("instance-state-name"),
				Values: []string{"running"},
			},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to describe EC2 instances: %w", err)
	}

	var costs []domain.ResourceCost
	for _, reservation := range resp.Reservations {
		for _, instance := range reservation.Instances {
			hourlyRate := getInstanceTypePrice(string(instance.InstanceType))

			instanceName := aws.ToString(instance.InstanceId)
			for _, tag := range instance.Tags {
				if aws.ToString(tag.Key) == "Name" {
					instanceName = aws.ToString(tag.Value)
					break
				}
			}

			cost := domain.ResourceCost{
				StartTime: time.Now().AddDate(0, 0, -days),
				EndTime:   time.Now(),
				Resource: domain.Resource{
					Platform: "AWS",
					Service:  "EC2",
					Name:     instanceName,
					Description: fmt.Sprintf(
						"EC2 Instance %s (%s)",
						aws.ToString(instance.InstanceId),
						instance.InstanceType,
					),
					Metadata: struct {
						ID        string
						AccountID string
						UserID    string
						Region    string
					}{
						ID:     aws.ToString(instance.InstanceId),
						Region: aws.ToString(instance.Placement.AvailabilityZone),
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
						Description: fmt.Sprintf("Compute costs for %s", instance.InstanceType),
					},
				},
			}
			costs = append(costs, cost)
		}
	}
	return costs, nil
}

func (a *ec2Analyzer) GenerateReport(ctx context.Context, days int) (*domain.Report, error) {
	costs, err := a.CollectUsage(ctx, days)
	if err != nil {
		return nil, err
	}

	report := &domain.Report{
		Title: "EC2 Analysis",
		Period: domain.TimePeriod{
			Start:    time.Now().AddDate(0, 0, -days),
			End:      time.Now(),
			Duration: days,
		},
		Currency: "USD",
	}

	instanceSummary := make(map[string]*struct {
		totalCost  float64
		hoursUsed  float64
		firstSeen  time.Time
		lastSeen   time.Time
		daysActive int
	})

	for _, cost := range costs {
		instanceID := cost.Resource.Metadata.ID
		summary, exists := instanceSummary[instanceID]
		if !exists {
			summary = &struct {
				totalCost  float64
				hoursUsed  float64
				firstSeen  time.Time
				lastSeen   time.Time
				daysActive int
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
				summary.hoursUsed += component.Value
			}
		}
	}

	for instanceID, summary := range instanceSummary {
		section := domain.ReportSection{
			Title: fmt.Sprintf("Instance: %s", instanceID),
			Summary: map[string]interface{}{
				"Days Active":    summary.daysActive,
				"Total Cost":     summary.totalCost,
				"Hours Used":     summary.hoursUsed,
				"Daily Average":  summary.totalCost / float64(summary.daysActive),
				"Average Hourly": summary.totalCost / summary.hoursUsed,
			},
			Details: []domain.ReportDetail{
				{
					Name:        "Hours",
					Value:       summary.hoursUsed,
					Unit:        "hours",
					Description: "Total compute hours used",
				},
				{
					Name:        "Cost",
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

func getInstanceTypePrice(instanceType string) float64 {
	// Mock prices - in production, use AWS Price List API
	prices := map[string]float64{
		"t2.micro":   0.0116,
		"t2.small":   0.023,
		"t2.medium":  0.0464,
		"t3.micro":   0.0104,
		"t3.small":   0.0208,
		"t3.medium":  0.0416,
		"m5.large":   0.096,
		"m5.xlarge":  0.192,
		"m5.2xlarge": 0.384,
		"r5.large":   0.126,
		"r5.xlarge":  0.252,
		"r5.2xlarge": 0.504,
	}
	if price, ok := prices[instanceType]; ok {
		return price
	}
	return 0.05 // default fallback price
}
