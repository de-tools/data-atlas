package usage

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"github.com/de-tools/data-atlas/pkg/store/duckdb"

	"github.com/de-tools/data-atlas/pkg/models/store"
)

type Store interface {
	Add(ctx context.Context, workspace string, records []store.UsageRecord) error
}

type usageStore struct {
	db *sql.DB
}

func NewStore(db *sql.DB) (Store, error) {
	if db == nil {
		return nil, fmt.Errorf("database connection is nil")
	}
	return &usageStore{
		db: db,
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
			record.Resource,
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
