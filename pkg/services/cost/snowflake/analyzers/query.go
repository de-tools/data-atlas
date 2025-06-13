package analyzers

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/de-tools/data-atlas/pkg/models/domain"
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

func (qa *queryAnalyzer) CollectUsage(_ context.Context, days int) ([]domain.ResourceCost, error) {
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

func (qa *queryAnalyzer) GenerateReport(ctx context.Context, days int) (*domain.Report, error) {
	costs, err := qa.CollectUsage(ctx, days)
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

	// Group by query ID
	querySummary := make(map[string]*struct {
		warehouse      string
		queryText      string
		totalCredits   float64
		totalCost      float64
		execTime       float64
		bytesScanned   int64
		executionCount int
		firstSeen      time.Time
		lastSeen       time.Time
	})

	for _, cost := range costs {
		queryID := cost.Resource.Metadata.ID
		summary, exists := querySummary[queryID]
		if !exists {
			summary = &struct {
				warehouse      string
				queryText      string
				totalCredits   float64
				totalCost      float64
				execTime       float64
				bytesScanned   int64
				executionCount int
				firstSeen      time.Time
				lastSeen       time.Time
			}{
				warehouse:      cost.Resource.Tags["warehouse"],
				queryText:      cost.Resource.Description,
				firstSeen:      cost.StartTime,
				lastSeen:       cost.StartTime,
				executionCount: 0,
			}
			querySummary[queryID] = summary
		}

		duration := cost.EndTime.Sub(cost.StartTime)
		summary.execTime += duration.Seconds()
		summary.executionCount++

		if cost.StartTime.Before(summary.firstSeen) {
			summary.firstSeen = cost.StartTime
		}
		if cost.StartTime.After(summary.lastSeen) {
			summary.lastSeen = cost.StartTime
		}

		for _, component := range cost.Costs {
			summary.totalCost += component.TotalAmount
			if component.Type == "compute" {
				summary.totalCredits += component.Value
			}
		}
	}

	// Create a section for each query
	for queryID, summary := range querySummary {
		section := domain.ReportSection{
			Title: fmt.Sprintf("Query: %s (Warehouse: %s)", queryID, summary.warehouse),
			Summary: map[string]interface{}{
				"Warehouse":     summary.warehouse,
				"Executions":    summary.executionCount,
				"Query":         summary.queryText,
				"Total Cost":    summary.totalCost,
				"Total Credits": summary.totalCredits,
				"Total Runtime": summary.execTime,
				"First Run":     summary.firstSeen.Format("2006-01-02 15:04:05"),
				"Last Run":      summary.lastSeen.Format("2006-01-02 15:04:05"),
			},
			Details: []domain.ReportDetail{
				{
					Name:        "Execution Count",
					Value:       summary.executionCount,
					Unit:        "times",
					Description: "Number of executions",
				},
				{
					Name:        "Total Runtime",
					Value:       summary.execTime,
					Unit:        "seconds",
					Description: "Total execution time",
				},
				{
					Name:        "Average Runtime",
					Value:       summary.execTime / float64(summary.executionCount),
					Unit:        "seconds",
					Description: "Average execution time per run",
				},
				{
					Name:        "Credits Used",
					Value:       summary.totalCredits,
					Unit:        "credits",
					Description: "Total compute credits",
				},
				{
					Name:        "Cost",
					Value:       summary.totalCost,
					Unit:        "USD",
					Description: "Total cost",
				},
				{
					Name:        "Cost per Execution",
					Value:       summary.totalCost / float64(summary.executionCount),
					Unit:        "USD",
					Description: "Average cost per execution",
				},
			},
			Metadata: map[string]interface{}{
				"QueryID":    queryID,
				"Warehouse":  summary.warehouse,
				"Executions": summary.executionCount,
			},
		}

		report.TotalAmount += summary.totalCost
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
