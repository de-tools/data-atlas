package databricks

import (
	"fmt"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestDatabricksAnalyzer_CollectUsage_ShouldReturnResourceCosts(t *testing.T) {
	// Given: a sqlmock DB with one row of usage data
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to open sqlmock: %v", err)
	}
	defer db.Close()

	field := "warehouse_id"
	// Prepare columns
	cols := []string{"id", "usage_start_time", "usage_end_time", "usage_quantity", "usage_unit", "sku_name"}
	// Create one row
	start := time.Date(2025, 6, 10, 12, 0, 0, 0, time.UTC)
	end := start.Add(2 * time.Hour)
	row := sqlmock.NewRows(cols).
		AddRow("wid123", start, end, 5.5, "DBU", "skux")

	// Expect query with metadataField substitution and one argument
	query := regexp.QuoteMeta(fmt.Sprintf(`
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
		ORDER BY usage_start_time DESC`, field))
	mock.ExpectQuery(query).
		WithArgs(7).
		WillReturnRows(row)

	a := NewDatabricksAnalyzer(db, field, "sqlwarehouse", "SQL Warehouse", 0.22)

	// When
	costs, err := a.CollectUsage(7)

	// Then
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(costs) != 1 {
		t.Fatalf("expected 1 cost, got %d", len(costs))
	}
	c := costs[0]
	if c.Resource.Name != "wid123" {
		t.Errorf("expected resource ID wid123, got %s", c.Resource.Name)
	}
	if len(c.Costs) != 1 {
		t.Errorf("expected 1 CostComponent, got %d", len(c.Costs))
	}
	comp := c.Costs[0]
	if comp.Value != 5.5 || comp.Unit != "DBU" {
		t.Errorf("unexpected CostComponent: %+v", comp)
	}
	// Rate and TotalAmount
	expectedTotal := 5.5 * 0.22
	if comp.TotalAmount != expectedTotal {
		t.Errorf("expected total %f, got %f", expectedTotal, comp.TotalAmount)
	}
	// Ensure all expectations met
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations: %v", err)
	}
}
