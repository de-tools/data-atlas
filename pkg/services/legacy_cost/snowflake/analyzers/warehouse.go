package analyzers

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/de-tools/data-atlas/pkg/models/domain"
)

type warehouseAnalyzer struct {
	db *sql.DB
}

func NewWarehouseAnalyzer(db *sql.DB) *warehouseAnalyzer {
	return &warehouseAnalyzer{
		db: db,
	}
}

func (wa *warehouseAnalyzer) GetResourceType() string {
	return "warehouse"
}

func (wa *warehouseAnalyzer) CollectUsage(_ context.Context, days int) ([]domain.ResourceCost, error) {
	//language=SQL
	query := `
		SELECT
			warehouse_name,
			credits_used,
			start_time,
			end_time
		FROM snowflake.account_usage.warehouse_metering_history
		WHERE start_time >= dateadd(day, ?, current_timestamp())
	`

	rows, err := wa.db.Query(query, -days)
	if err != nil {
		return nil, fmt.Errorf("warehouse usage query failed: %w", err)
	}

	defer rows.Close()

	var usages []domain.ResourceCost
	for rows.Next() {
		var name string
		var credits float64
		var start, end time.Time

		if err := rows.Scan(&name, &credits, &start, &end); err != nil {
			return nil, err
		}

		usage := domain.ResourceCost{
			StartTime: start,
			EndTime:   end,
			Resource: domain.ResourceDef{
				Platform:    "Snowflake",
				Service:     "Warehouse",
				Name:        name,
				Description: fmt.Sprintf("Snowflake Warehouse %s", name),
				Metadata: map[string]string{
					"id": name,
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
					Description: "Warehouse compute credits",
				},
			},
		}
		usages = append(usages, usage)
	}
	return usages, nil
}

func (wa *warehouseAnalyzer) GenerateReport(ctx context.Context, days int) (*domain.Report, error) {
	usages, err := wa.CollectUsage(ctx, days)
	if err != nil {
		return nil, err
	}

	report := &domain.Report{
		Title: "Warehouse Analysis",
		Period: domain.TimePeriod{
			Start:    time.Now().AddDate(0, 0, -days),
			End:      time.Now(),
			Duration: days,
		},
		Currency: "USD",
	}

	// Group by warehouse
	warehouseSummary := make(map[string]*struct {
		totalCredits float64
		totalCost    float64
		firstSeen    time.Time
		lastSeen     time.Time
	})

	for _, usage := range usages {
		summary, exists := warehouseSummary[usage.Resource.Name]
		if !exists {
			summary = &struct {
				totalCredits float64
				totalCost    float64
				firstSeen    time.Time
				lastSeen     time.Time
			}{
				firstSeen: usage.StartTime,
				lastSeen:  usage.EndTime,
			}
			warehouseSummary[usage.Resource.Name] = summary
		}

		for _, cost := range usage.Costs {
			if cost.Type == "compute" {
				summary.totalCredits += cost.Value
				summary.totalCost += cost.TotalAmount
			}
		}
	}

	// Create sections for each warehouse
	for name, summary := range warehouseSummary {
		section := domain.ReportSection{
			Title: fmt.Sprintf("Warehouse: %s", name),
			Summary: map[string]interface{}{
				"Total Credits": summary.totalCredits,
				"Total Cost":    summary.totalCost,
			},
			Details: []domain.ReportDetail{
				{
					Name:        "Active Period Start",
					Value:       summary.firstSeen.Format("2006-01-02"),
					Description: "First activity in the period",
				},
				{
					Name:        "Active Period End",
					Value:       summary.lastSeen.Format("2006-01-02"),
					Description: "Last activity in the period",
				},
				{
					Name:        "Credits Used",
					Value:       summary.totalCredits,
					Unit:        "credits",
					Description: "Total compute credits consumed",
				},
				{
					Name:        "Cost",
					Value:       summary.totalCost,
					Unit:        "USD",
					Description: "Total cost for the period",
				},
			},
			Metadata: map[string]interface{}{
				"WarehouseName": name,
			},
		}

		report.TotalAmount += summary.totalCost
		report.Sections = append(report.Sections, section)
	}

	return report, nil
}
