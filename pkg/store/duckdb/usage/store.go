package usage

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/de-tools/data-atlas/pkg/store/duckdb"

	"github.com/de-tools/data-atlas/pkg/models/store"
)

// Store supports both ingestion (Add) and read (Get*) operations for usage records in DuckDB
// For read operations, bind the store to a specific workspace via NewWorkspaceStore
// Note: Add still accepts workspace parameter to minimize changes in workflow runner.
type Store interface {
	Add(ctx context.Context, workspace string, records []store.UsageRecord) error
	GetResourcesUsage(ctx context.Context, resources []string, startTime, endTime time.Time) ([]store.UsageRecord, error)
	GetUsage(ctx context.Context, startTime, endTime time.Time) ([]store.UsageRecord, error)
	GetUsageStats(ctx context.Context, startTime *time.Time) (*store.UsageStats, error)
}

type usageStore struct {
	db        *sql.DB
	workspace string // optional; required for read methods
}

func NewStore(db *sql.DB) (Store, error) {
	if db == nil {
		return nil, fmt.Errorf("database connection is nil")
	}
	return &usageStore{
		db: db,
	}, nil
}

// NewWorkspaceStore returns a Store bound to a specific workspace for read operations
func NewWorkspaceStore(db *sql.DB, workspace string) (Store, error) {
	if db == nil {
		return nil, fmt.Errorf("database connection is nil")
	}
	if workspace == "" {
		return nil, fmt.Errorf("workspace is required for read store")
	}
	return &usageStore{
		db:        db,
		workspace: workspace,
	}, nil
}

func (u *usageStore) Add(ctx context.Context, workspace string, records []store.UsageRecord) error {
	if len(records) == 0 {
		return nil
	}

	tx := duckdb.GetTransaction(ctx)
	query := `
		INSERT INTO usage_records (
			id, workspace, resource, metadata, quantity, unit,
			sku, rate, currency, start_time, end_time
		) VALUES (
			?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
		)`

	var stmt *sql.Stmt
	var err error
	if tx == nil {
		stmt, err = u.db.PrepareContext(ctx, query)
	} else {
		stmt, err = tx.PrepareContext(ctx, query)
	}

	if err != nil {
		return fmt.Errorf("prepare statement: %w", err)
	}
	defer stmt.Close()

	for _, record := range records {
		metadata, err := json.Marshal(record.Metadata)
		if err != nil {
			return fmt.Errorf("marshal metadata: %w", err)
		}

		_, err = stmt.ExecContext(ctx,
			record.ID,
			workspace,
			record.ResourceType,
			metadata,
			record.Quantity,
			record.Unit,
			record.SKU,
			record.Rate,
			record.Currency,
			record.StartTime,
			record.EndTime,
		)

		if err != nil {
			return fmt.Errorf("insert record: %w", err)
		}
	}

	return nil
}

func (u *usageStore) ensureWorkspace() error {
	if u.workspace == "" {
		return fmt.Errorf("read operation requires workspace-bound store; use NewWorkspaceStore")
	}
	return nil
}

func (u *usageStore) GetUsage(ctx context.Context, startTime, endTime time.Time) ([]store.UsageRecord, error) {
	if err := u.ensureWorkspace(); err != nil {
		return nil, err
	}
	query := `
		SELECT id, resource, metadata, quantity, unit, sku, rate, currency, start_time, end_time
		FROM usage_records
		WHERE workspace = ? AND start_time >= ? AND start_time < ?
		ORDER BY start_time DESC
	`
	rows, err := u.db.QueryContext(ctx, query, u.workspace, startTime, endTime)
	if err != nil {
		return nil, fmt.Errorf("query usage: %w", err)
	}
	defer rows.Close()
	return scanUsageRows(rows)
}

func (u *usageStore) GetResourcesUsage(ctx context.Context, resources []string, startTime, endTime time.Time) ([]store.UsageRecord, error) {
	if err := u.ensureWorkspace(); err != nil {
		return nil, err
	}
	if len(resources) == 0 {
		return []store.UsageRecord{}, nil
	}
	// Build placeholders for IN clause
	placeholders := make([]string, 0, len(resources))
	args := make([]interface{}, 0, 2+len(resources))
	args = append(args, u.workspace, startTime, endTime)
	for range resources {
		placeholders = append(placeholders, "?")
	}
	// Adjust args to match order: workspace, start, end, resources...
	args = append([]interface{}{u.workspace, startTime, endTime}, toInterfaceSlice(resources)...)

	query := fmt.Sprintf(`
		SELECT id, resource, metadata, quantity, unit, sku, rate, currency, start_time, end_time
		FROM usage_records
		WHERE workspace = ? AND start_time >= ? AND start_time < ? AND resource IN (%s)
		ORDER BY start_time DESC
	`, join(placeholders, ","))

	rows, err := u.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query usage by resources: %w", err)
	}
	defer rows.Close()
	return scanUsageRows(rows)
}

func (u *usageStore) GetUsageStats(ctx context.Context, startTime *time.Time) (*store.UsageStats, error) {
	if err := u.ensureWorkspace(); err != nil {
		return nil, err
	}
	query := `SELECT COUNT(*) as total_records, MIN(start_time) as earliest_record FROM usage_records WHERE workspace = ?`
	args := []interface{}{u.workspace}
	if startTime != nil {
		query += " AND start_time > ?"
		args = append(args, *startTime)
	}
	var total int64
	var earliest sql.NullTime
	if err := u.db.QueryRowContext(ctx, query, args...).Scan(&total, &earliest); err != nil {
		return nil, fmt.Errorf("get usage stats: %w", err)
	}
	var first *time.Time
	if earliest.Valid {
		t := earliest.Time
		first = &t
	}
	return &store.UsageStats{RecordsCount: total, FirstRecordTime: first}, nil
}

func scanUsageRows(rows *sql.Rows) ([]store.UsageRecord, error) {
	records := make([]store.UsageRecord, 0)
	for rows.Next() {
		var (
			id, resource, unit, sku, currency string
			metadataRaw                       []byte
			qty, rate                         float64
			start, end                        time.Time
		)
		if err := rows.Scan(&id, &resource, &metadataRaw, &qty, &unit, &sku, &rate, &currency, &start, &end); err != nil {
			return nil, err
		}
		md := map[string]string{}
		if len(metadataRaw) > 0 {
			_ = json.Unmarshal(metadataRaw, &md)
		}
		records = append(records, store.UsageRecord{
			ID:        id,
			Resource:  resource,
			Metadata:  md,
			Quantity:  qty,
			Unit:      unit,
			SKU:       sku,
			Rate:      rate,
			Currency:  currency,
			StartTime: start,
			EndTime:   end,
		})
	}
	return records, nil
}

func toInterfaceSlice(ss []string) []interface{} {
	res := make([]interface{}, len(ss))
	for i, s := range ss {
		res[i] = s
	}
	return res
}

func join(items []string, sep string) string {
	if len(items) == 0 {
		return ""
	}
	out := items[0]
	for i := 1; i < len(items); i++ {
		out += sep + items[i]
	}
	return out
}
