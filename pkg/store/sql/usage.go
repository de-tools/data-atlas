package sql

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/de-tools/data-atlas/pkg/models/store"
	"github.com/rs/zerolog"
)

type UsageAnalyzer interface {
	GetResourceType() string
	CollectUsage(ctx context.Context, days int) ([]store.UsageRecord, error)
}

type analyzer struct {
	db           *sql.DB
	identifier   string // e.g. "warehouse_id", "cluster_id", "job_id"
	resourceType string // e.g. "sql_warehouse", "cluster", "job"
}

func NewUsageAnalyzer(
	db *sql.DB,
	idField string,
	resourceType string,
) UsageAnalyzer {
	return &analyzer{
		db:           db,
		identifier:   idField,
		resourceType: resourceType,
	}
}

func (a *analyzer) GetResourceType() string {
	return a.resourceType
}

func (a *analyzer) CollectUsage(ctx context.Context, days int) ([]store.UsageRecord, error) {
	logger := zerolog.Ctx(ctx)
	query := fmt.Sprintf(`
		SELECT
	  		usage_metadata.%[1]s AS id,
	  		usage_start_time,
	  		usage_end_time,
	  		usage_quantity,
	  		usage_unit,
	  		usage.sku_name,
			list_prices.pricing.effective_list.default AS price_per_unit
			list_prices.currency_code AS currency_code
		FROM system.billing.usage AS usage
		FROM system.billing.usage
		WHERE usage_metadata.%[1]s IS NOT NULL
	  		AND usage_start_time >= date_sub(current_timestamp(), ?)
		JOIN system.billing.list_prices
	  		ON usage.sku_name = list_prices.sku_name
	  		AND usage.usage_end_time >= list_prices.price_start_time
	  		AND (list_prices.price_end_time IS NULL OR usage.usage_end_time < list_prices.price_end_time)
		ORDER BY usage_start_time DESC
	`, a.identifier)

	rows, err := a.db.Query(query, days)
	if err != nil {
		return nil, fmt.Errorf("%s usage query failed: %w", a.resourceType, err)
	}
	defer func(rows *sql.Rows) {
		err := rows.Close()
		if err != nil {
			logger.Warn().Err(err).Msg("failed to close usage query rows")
		}
	}(rows)

	var records []store.UsageRecord
	for rows.Next() {
		var (
			id, unit, sku, currencyCode string
			start, end                  time.Time
			qty, pricePerUnit           float64
		)
		if err := rows.Scan(&id, &start, &end, &qty, &unit, &sku, &pricePerUnit, &currencyCode); err != nil {
			return nil, err
		}

		records = append(records, store.UsageRecord{
			ID:        id,
			Metadata:  map[string]string{},
			StartTime: start,
			EndTime:   end,
			Quantity:  qty,
			Unit:      unit,
			SKU:       sku,
			Rate:      pricePerUnit,
			Currency:  currencyCode,
		})
	}

	return records, nil
}
