package analyzers

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awssdk "github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/de-tools/data-atlas/pkg/models/domain"
)

type s3Analyzer struct {
	client *s3.Client
}

func NewS3Analyzer(cfg awssdk.Config) *s3Analyzer {
	return &s3Analyzer{
		client: s3.NewFromConfig(cfg),
	}
}

func (a *s3Analyzer) GetResourceType() string {
	return "S3"
}

func (a *s3Analyzer) CollectUsage(ctx context.Context, days int) ([]domain.ResourceCost, error) {
	resp, err := a.client.ListBuckets(ctx, &s3.ListBucketsInput{})
	if err != nil {
		return nil, fmt.Errorf("failed to list S3 buckets: %w", err)
	}

	var costs []domain.ResourceCost
	for _, bucket := range resp.Buckets {
		// Get bucket location
		locResp, err := a.client.GetBucketLocation(ctx, &s3.GetBucketLocationInput{
			Bucket: bucket.Name,
		})
		if err != nil {
			continue
		}
		region := string(locResp.LocationConstraint)
		if region == "" {
			region = "us-east-1"
		}

		// Get bucket size and object count
		var totalSize int64
		var objectCount int64
		var continuationToken *string

		for {
			objResp, err := a.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
				Bucket:            bucket.Name,
				ContinuationToken: continuationToken,
			})
			if err != nil {
				break
			}

			for _, obj := range objResp.Contents {
				totalSize += *obj.Size
				objectCount++
			}

			if !*objResp.IsTruncated {
				break
			}
			continuationToken = objResp.NextContinuationToken
		}

		storageGB := float64(totalSize) / (1024 * 1024 * 1024)
		storageRate := getS3StorageRate(region, "STANDARD")
		requestRates := getS3RequestRates(region)

		cost := domain.ResourceCost{
			StartTime: time.Now().AddDate(0, 0, -days),
			EndTime:   time.Now(),
			Resource: domain.ResourceDef{
				Platform:    "AWS",
				Service:     "S3",
				Name:        aws.ToString(bucket.Name),
				Description: "S3 Bucket (Standard Storage)",
				Metadata: map[string]string{
					"id":     aws.ToString(bucket.Name),
					"region": region,
				},
			},
			Costs: []domain.CostComponent{
				{
					Type:        "storage",
					Value:       storageGB,
					Unit:        "GB",
					Rate:        storageRate,
					TotalAmount: storageGB * storageRate * float64(days) / 30,
					Currency:    "USD",
					Description: fmt.Sprintf("Standard storage for %0.2f GB", storageGB),
				},
				{
					Type:        "requests_get",
					Value:       float64(objectCount * int64(days)),
					Unit:        "requests",
					Rate:        requestRates.get,
					TotalAmount: float64(objectCount*int64(days)) * requestRates.get,
					Currency:    "USD",
					Description: "GET requests",
				},
				{
					Type:        "requests_put",
					Value:       float64(objectCount * int64(days) / 10),
					Unit:        "requests",
					Rate:        requestRates.put,
					TotalAmount: float64(objectCount*int64(days)/10) * requestRates.put,
					Currency:    "USD",
					Description: "PUT, COPY, POST, or LIST requests",
				},
			},
		}
		costs = append(costs, cost)
	}
	return costs, nil
}

func (a *s3Analyzer) GenerateReport(ctx context.Context, days int) (*domain.Report, error) {
	costs, err := a.CollectUsage(ctx, days)
	if err != nil {
		return nil, err
	}

	report := &domain.Report{
		Title: "S3 Analysis",
		Period: domain.TimePeriod{
			Start:    time.Now().AddDate(0, 0, -days),
			End:      time.Now(),
			Duration: days,
		},
		Currency: "USD",
	}

	bucketSummary := make(map[string]*struct {
		totalCost    float64
		storageCost  float64
		requestsCost float64
		storageGB    float64
		getRequests  int64
		putRequests  int64
		firstSeen    time.Time
		lastSeen     time.Time
		daysActive   int
	})

	for _, cost := range costs {
		bucketName := cost.Resource.Metadata["id"]
		summary, exists := bucketSummary[bucketName]
		if !exists {
			summary = &struct {
				totalCost    float64
				storageCost  float64
				requestsCost float64
				storageGB    float64
				getRequests  int64
				putRequests  int64
				firstSeen    time.Time
				lastSeen     time.Time
				daysActive   int
			}{
				firstSeen:  cost.StartTime,
				lastSeen:   cost.EndTime,
				daysActive: days,
			}
			bucketSummary[bucketName] = summary
		}

		for _, component := range cost.Costs {
			summary.totalCost += component.TotalAmount
			switch component.Type {
			case "storage":
				summary.storageCost += component.TotalAmount
				summary.storageGB = component.Value
			case "requests_get":
				summary.requestsCost += component.TotalAmount
				summary.getRequests = int64(component.Value)
			case "requests_put":
				summary.requestsCost += component.TotalAmount
				summary.putRequests = int64(component.Value)
			}
		}
	}

	for bucketName, summary := range bucketSummary {
		section := domain.ReportSection{
			Title: fmt.Sprintf("Bucket: %s", bucketName),
			Summary: map[string]interface{}{
				"Days Active":   summary.daysActive,
				"Total Cost":    summary.totalCost,
				"Storage Cost":  summary.storageCost,
				"Requests Cost": summary.requestsCost,
				"Storage Size":  summary.storageGB,
				"Daily Average": summary.totalCost / float64(summary.daysActive),
				"GET Requests":  summary.getRequests,
				"PUT Requests":  summary.putRequests,
			},
			Details: []domain.ReportDetail{
				{
					Name:        "Storage",
					Value:       summary.storageGB,
					Unit:        "GB",
					Description: "Total storage used",
				},
				{
					Name:        "GET Requests",
					Value:       float64(summary.getRequests),
					Unit:        "requests",
					Description: "Total GET requests",
				},
				{
					Name:        "PUT Requests",
					Value:       float64(summary.putRequests),
					Unit:        "requests",
					Description: "Total PUT requests",
				},
				{
					Name:        "Total Cost",
					Value:       summary.totalCost,
					Unit:        "USD",
					Description: "Total cost for the period",
				},
			},
			Metadata: map[string]interface{}{
				"BucketName": bucketName,
				"DaysActive": summary.daysActive,
			},
		}
		report.TotalAmount += summary.totalCost
		report.Sections = append(report.Sections, section)
	}

	return report, nil
}

// Pricing helpers
func getS3StorageRate(region, storageClass string) float64 {
	standardRates := map[string]float64{
		"us-east-1":      0.023,
		"us-east-2":      0.023,
		"us-west-1":      0.026,
		"us-west-2":      0.023,
		"eu-west-1":      0.024,
		"eu-central-1":   0.025,
		"ap-northeast-1": 0.025,
		"ap-southeast-1": 0.025,
		"ap-southeast-2": 0.025,
	}

	if rate, ok := standardRates[region]; ok {
		return rate
	}
	return 0.023
}

type s3RequestRates struct {
	get float64
	put float64
}

func getS3RequestRates(region string) s3RequestRates {
	rates := map[string]s3RequestRates{
		"us-east-1": {
			get: 0.0000004,
			put: 0.000005,
		},
		"us-west-2": {
			get: 0.0000004,
			put: 0.000005,
		},
	}

	if rate, ok := rates[region]; ok {
		return rate
	}
	return s3RequestRates{get: 0.0000004, put: 0.000005}
}
