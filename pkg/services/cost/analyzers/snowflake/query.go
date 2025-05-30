package snowflake

import (
	"database/sql"
	"fmt"
	"github.com/de-tools/data-atlas/pkg/models/domain"
	"time"
)

type queryAnalyzer struct {
	db    *sql.DB
	limit int
}

func NewQueryAnalyzer(db *sql.DB, limit int) *queryAnalyzer {
	return &queryAnalyzer{
		db:    db,
		limit: limit,
	}
}

func (qa *queryAnalyzer) GetResourceType() string {
	return "query"
}

func (qa *queryAnalyzer) CollectUsage(days int) ([]domain.ResourceCost, error) {
	query := `
		SELECT 
			query_id,
			warehouse_name,
			execution_time,
			bytes_scanned,
			credits_used_cloud_services,
			query_text,
			start_time
		FROM snowflake.account_usage.query_history
		WHERE start_time >= dateadd(days, ?, current_timestamp())
		  AND credits_used_cloud_services > 0
		ORDER BY credits_used_cloud_services DESC
		LIMIT ?
	`

	rows, err := qa.db.Query(query, -days, qa.limit)
	if err != nil {
		return nil, fmt.Errorf("query history query failed: %w", err)
	}
	defer rows.Close()

	var costs []domain.ResourceCost
	for rows.Next() {
		var queryID, warehouseName, queryText string
		var execTime, credits float64
		var bytesScanned int64
		var startTime time.Time

		if err := rows.Scan(&queryID, &warehouseName, &execTime, &bytesScanned,
			&credits, &queryText, &startTime); err != nil {
			return nil, err
		}

		cost := domain.ResourceCost{
			StartTime: startTime,
			EndTime:   startTime.Add(time.Duration(execTime) * time.Millisecond),
			Resource: domain.Resource{
				Platform:    "Snowflake",
				Service:     "Query",
				Name:        fmt.Sprintf("%s - %s", warehouseName, queryID),
				Description: truncateString(queryText, 100),
				Tags: map[string]string{
					"warehouse": warehouseName,
					"query_id":  queryID,
				},
				Metadata: struct {
					ID        string
					AccountID string
					UserID    string
					Region    string
				}{
					ID: queryID,
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
					Description: "Query compute credits",
				},
			},
		}
		costs = append(costs, cost)
	}
	return costs, nil
}

func (qa *queryAnalyzer) GenerateReport(days int) (*domain.Report, error) {
	costs, err := qa.CollectUsage(days)
	if err != nil {
		return nil, err
	}

	report := &domain.Report{
		Title: "Query Analysis",
		Period: domain.TimePeriod{
			Start:    time.Now().AddDate(0, 0, -days),
			End:      time.Now(),
			Duration: days,
		},
		Currency: "USD",
	}

	// Group queries by warehouse for better organization
	warehouseQueries := make(map[string][]domain.ResourceCost)
	for _, cost := range costs {
		warehouse := cost.Resource.Tags["warehouse"]
		warehouseQueries[warehouse] = append(warehouseQueries[warehouse], cost)
	}

	for warehouse, queries := range warehouseQueries {
		section := domain.ReportSection{
			Title: fmt.Sprintf("Warehouse: %s", warehouse),
			Summary: map[string]interface{}{
				"Query Count": len(queries),
			},
			Metadata: map[string]interface{}{
				"Warehouse": warehouse,
			},
		}

		var warehouseTotalCost float64
		var warehouseTotalCredits float64

		for _, query := range queries {
			queryDetails := []domain.ReportDetail{
				{
					Name:        "Query ID",
					Value:       query.Resource.Metadata.ID,
					Description: query.Resource.Description,
				},
				{
					Name:        "Execution Time",
					Value:       query.EndTime.Sub(query.StartTime).Seconds(),
					Unit:        "seconds",
					Description: "Query execution duration",
				},
			}

			for _, component := range query.Costs {
				warehouseTotalCost += component.TotalAmount
				warehouseTotalCredits += component.Value

				queryDetails = append(queryDetails, domain.ReportDetail{
					Name:        component.Type,
					Value:       component.Value,
					Unit:        component.Unit,
					Description: component.Description,
				})
			}

			section.Details = append(section.Details, queryDetails...)
		}

		section.Summary["Total Cost"] = warehouseTotalCost
		section.Summary["Total Credits"] = warehouseTotalCredits
		report.TotalAmount += warehouseTotalCost

		report.Sections = append(report.Sections, section)
	}

	return report, nil
}

func truncateString(s string, length int) string {
	if len(s) <= length {
		return s
	}
	return s[:length] + "..."
}
