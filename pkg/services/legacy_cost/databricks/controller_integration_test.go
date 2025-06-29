//go:build integration
// +build integration

package databricks

import (
	"context"
	"fmt"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

// ./cost analyze --platform=databricks --profile=./etc/profiles/databricks.yaml --resource_type=sqlwarehouse --duration=1
func TestController(t *testing.T) {
	// Given
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New failed: %v", err)
	}
	defer db.Close()

	now := time.Now().Truncate(time.Second)
	start := now.Add(-2 * time.Hour)
	end := now

	cols := []string{
		"id", "usage_start_time", "usage_end_time",
		"usage_quantity", "usage_unit", "sku_name",
	}
	mock.
		ExpectQuery(regexp.QuoteMeta(`
SELECT
  usage_metadata.warehouse_id    AS id,
  usage_start_time,
  usage_end_time,
  usage_quantity,
  usage_unit,
  sku_name
FROM system.billing.usage
WHERE usage_metadata.warehouse_id IS NOT NULL
  AND usage_start_time >= date_sub(current_timestamp(), ?)
ORDER BY usage_start_time DESC`)).
		WithArgs(1).
		WillReturnRows(sqlmock.NewRows(cols).AddRow("wh1", start, end, 10.0, "DBU", "sku1"))

	analyzer := NewDatabricksAnalyzer(db,
		"warehouse_id", "sqlwarehouse", "SQL Warehouse", 0.22,
	)
	ctrl, err := NewDatabricksController(analyzer)
	if err != nil {
		t.Fatalf("controller creation failed: %v", err)
	}

	// When
	report, err := ctrl.EstimateResourceCost(
		context.Background(), "sqlwarehouse", 1,
	)
	if err != nil {
		t.Fatalf("EstimateResourceCost failed: %v", err)
	}

	// Then: report fields
	if report.Title != "SQL Warehouse Analysis" {
		t.Errorf("expected Title=%q, got %q", "SQL Warehouse Analysis", report.Title)
	}
	if report.Period.Duration != 1 {
		t.Errorf("expected Period.Duration=1, got %d", report.Period.Duration)
	}
	if report.Currency != "USD" {
		t.Errorf("expected Currency=USD, got %s", report.Currency)
	}
	wantTotal := 10.0 * 0.22
	if report.TotalAmount != wantTotal {
		t.Errorf("expected TotalAmount=%.2f, got %.2f", wantTotal, report.TotalAmount)
	}

	// Then: exactly one section
	if len(report.Sections) != 1 {
		t.Fatalf("expected 1 section, got %d", len(report.Sections))
	}
	sec := report.Sections[0]

	// Then: section title
	expectedTitle := "SQL Warehouse: wh1"
	if sec.Title != expectedTitle {
		t.Errorf("expected Section.Title=%q, got %q", expectedTitle, sec.Title)
	}

	// Then: summary entries
	summary := sec.Summary
	if got, ok := summary["Total DBUs"].(float64); !ok || got != 10.0 {
		t.Errorf("expected Summary[\"Total DBUs\"]=10.0, got %v", summary["Total DBUs"])
	}
	if got, ok := summary["Total Cost"].(float64); !ok || got != wantTotal {
		t.Errorf("expected Summary[\"Total Cost\"]=%.2f, got %v", wantTotal, summary["Total Cost"])
	}

	// Then: details
	if len(sec.Details) != 4 {
		t.Fatalf("expected 4 details, got %d", len(sec.Details))
	}
	checkDetail := func(i int, name string, val interface{}) {
		d := sec.Details[i]
		if d.Name != name {
			t.Errorf("detail[%d].Name expected %q, got %q", i, name, d.Name)
		}
		if fmt.Sprint(d.Value) != fmt.Sprint(val) {
			t.Errorf("detail[%d].Value expected %v, got %v", i, val, d.Value)
		}
	}
	checkDetail(0, "Period Start", start.Format("2006-01-02"))
	checkDetail(1, "Period End", end.Format("2006-01-02"))
	checkDetail(2, "DBUs Used", 10.0)
	checkDetail(3, "Cost", wantTotal)

	// Then: mock expectations
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled sqlmock expectations: %v", err)
	}
}
