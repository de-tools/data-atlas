package usage

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/de-tools/data-atlas/pkg/models/store"
	"github.com/de-tools/data-atlas/pkg/store/duckdb"
	_ "github.com/marcboeker/go-duckdb/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fixture struct {
	db    *sql.DB
	store Store
}

func setupTestDB(t *testing.T) *sql.DB {
	db, err := duckdb.NewDB(duckdb.Settings{DbPath: ":memory:"})
	require.NoError(t, err)
	return db
}

func setupFixture(t *testing.T) *fixture {
	db := setupTestDB(t)
	store, err := NewStore(db)
	require.NoError(t, err)

	t.Cleanup(func() {
		db.Close()
	})

	return &fixture{
		db:    db,
		store: store,
	}
}

func TestUsageStore_Add(t *testing.T) {
	f := setupFixture(t)
	ctx := context.Background()

	t.Run("success - add records", func(t *testing.T) {
		workspace := "test-workspace"
		records := []store.UsageRecord{
			{
				ID:       "record1",
				Resource: "compute",
				Metadata: map[string]string{
					"instance_type": "t2.micro",
				},
				Quantity:  1.5,
				Unit:      "hours",
				SKU:       "sku1",
				Rate:      0.5,
				Currency:  "USD",
				StartTime: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
				EndTime:   time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC),
			},
			{
				ID:       "record2",
				Resource: "storage",
				Metadata: map[string]string{
					"type": "ssd",
				},
				Quantity:  100,
				Unit:      "GB",
				SKU:       "sku2",
				Rate:      0.1,
				Currency:  "USD",
				StartTime: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
				EndTime:   time.Date(2024, 1, 1, 1, 0, 0, 0, time.UTC),
			},
		}

		err := f.store.Add(ctx, workspace, records)
		require.NoError(t, err)

		// Verify records were stored
		var count int
		err = f.db.QueryRow("SELECT COUNT(*) FROM usage_records WHERE workspace = ?", workspace).Scan(&count)
		require.NoError(t, err)
		assert.Equal(t, 2, count)
	})

	t.Run("success - empty records", func(t *testing.T) {
		err := f.store.Add(ctx, "test-workspace", nil)
		require.NoError(t, err)
	})

	t.Run("error - duplicate records", func(t *testing.T) {
		workspace := "test-workspace"
		records := []store.UsageRecord{
			{
				ID:        "duplicate",
				Resource:  "compute",
				Quantity:  1.0,
				StartTime: time.Now(),
				EndTime:   time.Now(),
			},
		}

		err := f.store.Add(ctx, workspace, records)
		require.NoError(t, err)

		err = f.store.Add(ctx, workspace, records)
		assert.Error(t, err)
	})
}
