package databricks

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/de-tools/data-atlas/pkg/models/domain"
)

type Analyzer struct {
	db            *sql.DB
	metadataField string  // e.g. "warehouse_id", "cluster_id", "job_id"
	resourceType  string  // e.g. "sql_warehouse", "cluster", "job"
	serviceName   string  // e.g. "SQL Warehouse", "Cluster", "Job"
	dbuRate       float64 // USD per DBU, e.g. 0.22
}

// NewDatabricksAnalyzer constructs an analyzer for any usage_metadata field.
func NewDatabricksAnalyzer(
	db *sql.DB,
	metadataField, resourceType, serviceName string,
	dbuRate float64,
) *Analyzer {
	return &Analyzer{
		db:            db,
		metadataField: metadataField,
		resourceType:  resourceType,
		serviceName:   serviceName,
		dbuRate:       dbuRate,
	}
}

func (a *Analyzer) GetResourceType() string {
	return a.resourceType
}

func (a *Analyzer) CollectUsage(_ context.Context, days int) ([]domain.ResourceCost, error) {
	query := fmt.Sprintf(`
		SELECT
		  usage_metadata.%[1]s    AS id,
		  usage_start_time,
		  usage_end_time,
		  usage_quantity,
		  usage_unit,
		  sku_name
		FROM system.billing.usage
		WHERE usage_metadata.%[1]s IS NOT NULL
		  AND usage_start_time >= date_sub(current_timestamp(), ?)
		ORDER BY usage_start_time DESC
	`, a.metadataField)

	rows, err := a.db.Query(query, days)
	if err != nil {
		return nil, fmt.Errorf("%s usage query failed: %w", a.serviceName, err)
	}
	defer rows.Close()

	var out []domain.ResourceCost
	for rows.Next() {
		var (
			id, unit, sku string
			start, end    time.Time
			qty           float64
		)
		if err := rows.Scan(&id, &start, &end, &qty, &unit, &sku); err != nil {
			return nil, err
		}

		rc := domain.ResourceCost{
			StartTime: start,
			EndTime:   end,
			Resource: domain.ResourceDef{
				Platform:    "Databricks",
				Service:     a.serviceName,
				Name:        id,
				Description: fmt.Sprintf("Databricks %s %s", a.serviceName, id),
				Metadata:    map[string]string{"id": id},
			},
			Costs: []domain.CostComponent{{
				Type:        "compute",
				Value:       qty,
				Unit:        unit,
				Rate:        a.dbuRate,
				TotalAmount: qty * a.dbuRate,
				Currency:    "USD",
				Description: fmt.Sprintf("DBUs consumed (SKU: %s)", sku),
			}},
		}
		out = append(out, rc)
	}

	return out, nil
}

func (a *Analyzer) GenerateReport(ctx context.Context, days int) (*domain.Report, error) {
	usages, err := a.CollectUsage(ctx, days)
	if err != nil {
		return nil, err
	}

	type sum struct {
		qty, cost float64
		first     time.Time
		last      time.Time
	}
	m := make(map[string]*sum)
	for _, u := range usages {
		id := u.Resource.Metadata["id"]
		if _, ok := m[id]; !ok {
			m[id] = &sum{first: u.StartTime, last: u.EndTime}
		}
		s := m[id]
		for _, c := range u.Costs {
			if c.Type == "compute" {
				s.qty += c.Value
				s.cost += c.TotalAmount
			}
		}
		if u.StartTime.Before(s.first) {
			s.first = u.StartTime
		}
		if u.EndTime.After(s.last) {
			s.last = u.EndTime
		}
	}

	report := &domain.Report{
		Title: fmt.Sprintf("%s Analysis", a.serviceName),
		Period: domain.TimePeriod{
			Start:    time.Now().AddDate(0, 0, -days),
			End:      time.Now(),
			Duration: days,
		},
		Currency: "USD",
	}

	for id, s := range m {
		section := domain.ReportSection{
			Title: fmt.Sprintf("%s: %s", a.serviceName, id),
			Summary: map[string]interface{}{
				"Total DBUs": s.qty,
				"Total Cost": s.cost,
			},
			Details: []domain.ReportDetail{
				{Name: "Period Start", Value: s.first.Format("2006-01-02"), Description: "First usage"},
				{Name: "Period End", Value: s.last.Format("2006-01-02"), Description: "Last usage"},
				{Name: "DBUs Used", Value: s.qty, Unit: "DBU", Description: "Total DBUs consumed"},
				{Name: "Cost", Value: s.cost, Unit: "USD", Description: "Total cost"},
			},
			Metadata: map[string]interface{}{
				fmt.Sprintf("%sID", a.metadataField): id,
			},
		}
		report.Sections = append(report.Sections, section)
		report.TotalAmount += s.cost
	}

	return report, nil
}
