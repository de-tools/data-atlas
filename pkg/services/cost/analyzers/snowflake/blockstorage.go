package snowflake

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/de-tools/data-atlas/pkg/models/domain"
)

type blockStorageAnalyzer struct {
	db *sql.DB
}

func NewBlockStorageAnalyzer(db *sql.DB) *blockStorageAnalyzer {
	return &blockStorageAnalyzer{
		db: db,
	}
}

func (ba *blockStorageAnalyzer) GetResourceType() string {
	return "block_storage"
}

func (ba *blockStorageAnalyzer) CollectUsage(days int) ([]domain.ResourceCost, error) {
	query := `
		SELECT
			additional_iops,
			additional_throughput,
			bytes,
			compute_pool_name,
			storage_type,
			usage_date
		FROM snowflake.account_usage.block_storage_history
		WHERE usage_date >= dateadd(day, ?, current_date())
	`

	rows, err := ba.db.Query(query, -days)
	if err != nil {
		return nil, fmt.Errorf("block storage query failed: %w", err)
	}
	defer rows.Close()

	var costs []domain.ResourceCost
	for rows.Next() {
		var iops, throughput int
		var bytes int64
		var poolName, storageType string
		var usageDate time.Time

		if err := rows.Scan(&iops, &throughput, &bytes, &poolName, &storageType, &usageDate); err != nil {
			return nil, err
		}

		cost := domain.ResourceCost{
			StartTime: usageDate,
			EndTime:   usageDate.Add(24 * time.Hour),
			Resource: domain.Resource{
				Platform:    "Snowflake",
				Service:     "BlockStorage",
				Name:        poolName,
				Description: fmt.Sprintf("Block Storage for %s (%s)", poolName, storageType),
				Tags: map[string]string{
					"storage_type": storageType,
					"pool":         poolName,
				},
				Metadata: struct {
					ID        string
					AccountID string
					UserID    string
					Region    string
				}{
					ID: fmt.Sprintf("%s-%s-%s", poolName, storageType, usageDate.Format("2006-01-02")),
				},
			},
			Costs: []domain.CostComponent{
				{
					Type:        "storage",
					Value:       float64(bytes),
					Unit:        "bytes",
					Rate:        40.0 / (1024 * 1024 * 1024 * 1024), // $40 per TB
					TotalAmount: float64(bytes) * 40 / (1024 * 1024 * 1024 * 1024),
					Currency:    "USD",
					Description: "Block storage capacity",
				},
				{
					Type:        "iops",
					Value:       float64(iops),
					Unit:        "iops",
					Rate:        0.05,
					TotalAmount: float64(iops) * 0.05,
					Currency:    "USD",
					Description: "Additional IOPS",
				},
				{
					Type:        "throughput",
					Value:       float64(throughput),
					Unit:        "MB/s",
					Rate:        0.02,
					TotalAmount: float64(throughput) * 0.02,
					Currency:    "USD",
					Description: "Additional throughput",
				},
			},
		}
		costs = append(costs, cost)
	}
	return costs, nil
}

func (ba *blockStorageAnalyzer) GenerateReport(days int) (*domain.Report, error) {
	costs, err := ba.CollectUsage(days)
	if err != nil {
		return nil, err
	}

	// Initialize report
	report := &domain.Report{
		Title: "Block Storage Analysis",
		Period: domain.TimePeriod{
			Start:    time.Now().AddDate(0, 0, -days),
			End:      time.Now(),
			Duration: days,
		},
		Currency: "USD",
	}

	// Process data
	poolSummary := make(map[string]map[string]*struct {
		maxBytes            int64
		currentBytes        int64
		totalStorageCost    float64
		totalIOPSCost       float64
		totalThroughputCost float64
		daysWithData        int
	})

	for _, cost := range costs {
		poolName := cost.Resource.Name
		storageType := cost.Resource.Tags["storage_type"]

		if _, exists := poolSummary[poolName]; !exists {
			poolSummary[poolName] = make(map[string]*struct {
				maxBytes            int64
				currentBytes        int64
				totalStorageCost    float64
				totalIOPSCost       float64
				totalThroughputCost float64
				daysWithData        int
			})
		}

		if _, exists := poolSummary[poolName][storageType]; !exists {
			poolSummary[poolName][storageType] = &struct {
				maxBytes            int64
				currentBytes        int64
				totalStorageCost    float64
				totalIOPSCost       float64
				totalThroughputCost float64
				daysWithData        int
			}{}
		}

		summary := poolSummary[poolName][storageType]

		for _, component := range cost.Costs {
			switch component.Type {
			case "storage":
				bytes := int64(component.Value)
				summary.maxBytes = max(summary.maxBytes, bytes)
				summary.currentBytes = bytes
				summary.totalStorageCost += component.TotalAmount
			case "iops":
				summary.totalIOPSCost += component.TotalAmount
			case "throughput":
				summary.totalThroughputCost += component.TotalAmount
			}
		}
		summary.daysWithData++
	}

	// Create report sections
	for poolName, storageTypes := range poolSummary {
		section := domain.ReportSection{
			Title: fmt.Sprintf("Compute Pool: %s", poolName),
			Summary: map[string]interface{}{
				"Pool": poolName,
			},
		}

		for storageType, summary := range storageTypes {
			totalCost := summary.totalStorageCost + summary.totalIOPSCost + summary.totalThroughputCost
			report.TotalAmount += totalCost

			section.Details = append(section.Details,
				domain.ReportDetail{
					Name:        "Storage Type",
					Value:       storageType,
					Description: "Type of storage used",
				},
				domain.ReportDetail{
					Name:        "Current Storage",
					Value:       float64(summary.currentBytes) / (1024 * 1024 * 1024),
					Unit:        "GB",
					Description: "Current storage usage",
				},
				domain.ReportDetail{
					Name:        "Max Storage",
					Value:       float64(summary.maxBytes) / (1024 * 1024 * 1024),
					Unit:        "GB",
					Description: "Maximum storage used",
				},
				domain.ReportDetail{
					Name:        "Storage Cost",
					Value:       summary.totalStorageCost,
					Unit:        "USD",
					Description: "Total cost for storage",
				},
				domain.ReportDetail{
					Name:        "IOPS Cost",
					Value:       summary.totalIOPSCost,
					Unit:        "USD",
					Description: "Total cost for IOPS",
				},
				domain.ReportDetail{
					Name:        "Throughput Cost",
					Value:       summary.totalThroughputCost,
					Unit:        "USD",
					Description: "Total cost for throughput",
				},
			)
		}

		report.Sections = append(report.Sections, section)
	}

	return report, nil
}

func max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
