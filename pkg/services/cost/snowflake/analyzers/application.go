package analyzers

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/de-tools/data-atlas/pkg/models/domain"
)

type applicationAnalyzer struct {
	db *sql.DB
}

func NewApplicationAnalyzer(db *sql.DB) *applicationAnalyzer {
	return &applicationAnalyzer{
		db: db,
	}
}

func (aa *applicationAnalyzer) GetResourceType() string {
	return "application"
}

func (aa *applicationAnalyzer) CollectUsage(ctx context.Context, days int) ([]domain.ResourceCost, error) {
	query := `
		SELECT
			application_id,
			application_name,
			credits_used,
			credits_used_breakdown,
			listing_global_name,
			storage_bytes,
			storage_bytes_breakdown,
			usage_date
		FROM snowflake.account_usage.application_daily_usage_history
		WHERE usage_date >= dateadd(day, ?, current_date())
	`

	rows, err := aa.db.Query(query, -days)
	if err != nil {
		return nil, fmt.Errorf("application usage query failed: %w", err)
	}
	defer rows.Close()

	var costs []domain.ResourceCost
	for rows.Next() {
		var appID, appName string
		var globalName *string
		var credits float64
		var storageBytes int64
		var creditsBreakdown, storageBreakdown string
		var usageDate time.Time

		if err := rows.Scan(&appID, &appName, &credits, &creditsBreakdown, &globalName,
			&storageBytes, &storageBreakdown, &usageDate); err != nil {
			return nil, err
		}

		cost := domain.ResourceCost{
			StartTime: usageDate,
			EndTime:   usageDate.Add(24 * time.Hour),
			Resource: domain.Resource{
				Platform:    "Snowflake",
				Service:     "Application",
				Name:        appName,
				Description: fmt.Sprintf("Snowflake Application %s (%s)", appName, *globalName),
				Tags: map[string]string{
					"global_name": strPtrToStr(globalName),
				},
				Metadata: struct {
					ID        string
					AccountID string
					UserID    string
					Region    string
				}{
					ID: appID,
				},
			},
			Costs: []domain.CostComponent{
				{
					Type:        "compute",
					Value:       credits,
					Unit:        "credits",
					Rate:        3.0,
					TotalAmount: credits * 3,
					Currency:    "USD",
					Description: "Application compute credits",
				},
				{
					Type:        "storage",
					Value:       float64(storageBytes),
					Unit:        "bytes",
					Rate:        40.0 / (1024 * 1024 * 1024 * 1024), // $40 per TB
					TotalAmount: float64(storageBytes) * 40 / (1024 * 1024 * 1024 * 1024),
					Currency:    "USD",
					Description: "Application storage",
				},
			},
		}
		costs = append(costs, cost)
	}
	return costs, nil
}

func (aa *applicationAnalyzer) GenerateReport(ctx context.Context, days int) (*domain.Report, error) {
	costs, err := aa.CollectUsage(ctx, days)
	if err != nil {
		return nil, err
	}

	report := &domain.Report{
		Title: "Application Analysis",
		Period: domain.TimePeriod{
			Start:    time.Now().AddDate(0, 0, -days),
			End:      time.Now(),
			Duration: days,
		},
		Currency: "USD",
	}

	// Group by application
	appSummary := make(map[string]*struct {
		totalCredits float64
		totalCost    float64
		storageBytes int64
		firstSeen    time.Time
		lastSeen     time.Time
		daysActive   int
	})

	for _, cost := range costs {
		appID := cost.Resource.Metadata.ID
		summary, exists := appSummary[appID]
		if !exists {
			summary = &struct {
				totalCredits float64
				totalCost    float64
				storageBytes int64
				firstSeen    time.Time
				lastSeen     time.Time
				daysActive   int
			}{
				firstSeen:  cost.StartTime,
				lastSeen:   cost.EndTime,
				daysActive: 0,
			}
			appSummary[appID] = summary
		}

		for _, component := range cost.Costs {
			summary.totalCost += component.TotalAmount
			if component.Type == "compute" {
				summary.totalCredits += component.Value
			} else if component.Type == "storage" {
				summary.storageBytes = int64(component.Value)
			}
		}
		summary.daysActive++
	}

	// Create sections for each application
	for appID, summary := range appSummary {
		section := domain.ReportSection{
			Title: fmt.Sprintf("Application: %s", appID),
			Summary: map[string]interface{}{
				"Days Active":   summary.daysActive,
				"Total Cost":    summary.totalCost,
				"Total Credits": summary.totalCredits,
				"Storage (GB)":  float64(summary.storageBytes) / (1024 * 1024 * 1024),
				"Daily Average": summary.totalCost / float64(summary.daysActive),
			},
			Details: []domain.ReportDetail{
				{
					Name:        "Credits",
					Value:       summary.totalCredits,
					Unit:        "credits",
					Description: "Total compute credits used",
				},
				{
					Name:        "Storage",
					Value:       float64(summary.storageBytes) / (1024 * 1024 * 1024),
					Unit:        "GB",
					Description: "Current storage usage",
				},
				{
					Name:        "Cost",
					Value:       summary.totalCost,
					Unit:        "USD",
					Description: "Total cost for the period",
				},
				{
					Name:        "Average Daily Cost",
					Value:       summary.totalCost / float64(summary.daysActive),
					Unit:        "USD/day",
					Description: "Average daily cost",
				},
			},
			Metadata: map[string]interface{}{
				"ApplicationID": appID,
				"DaysActive":    summary.daysActive,
			},
		}

		report.TotalAmount += summary.totalCost
		report.Sections = append(report.Sections, section)
	}

	return report, nil
}

func strPtrToStr(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}
