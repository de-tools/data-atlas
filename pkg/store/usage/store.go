package usage

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/de-tools/data-atlas/pkg/store/pricing"

	"github.com/de-tools/data-atlas/pkg/models/store"
	"github.com/rs/zerolog"
)

type Store interface {
	GetResourcesUsage(
		ctx context.Context,
		resource []string,
		startTime time.Time,
		endTime time.Time,
	) ([]store.UsageRecord, error)
}

type usageStore struct {
	db           *sql.DB
	pricingStore pricing.Store
}

func NewStore(
	db *sql.DB,
	pricingStore pricing.Store,
) Store {
	return &usageStore{
		db:           db,
		pricingStore: pricingStore,
	}
}

func (u *usageStore) GetResourcesUsage(ctx context.Context, resources []string, startTime time.Time, endTime time.Time) ([]store.UsageRecord, error) {
	logger := zerolog.Ctx(ctx)

	var conditions []string
	for _, resourceType := range resources {
		idField := fmt.Sprintf("usage_metadata.%s_id", resourceType)
		conditions = append(conditions, fmt.Sprintf("%s IS NOT NULL", idField))
	}

	query := `
		SELECT
			COALESCE(` + buildCoalesceList(resources, "_id") + `) AS id,
			(
				CASE
					` + buildResourceTypeCase(resources) + `
					ELSE 'unknown'
				END
			) AS resource_type,
			usage_start_time,
			usage_end_time,
			usage_quantity,
			usage_unit,
			sku_name
		FROM system.billing.usage
		WHERE (` + strings.Join(conditions, " OR ") + `)
			AND usage_start_time >= ?
			AND usage_start_time < ?
		ORDER BY usage_start_time DESC
	`

	startTimeFormatted := startTime.Format("2006-01-02 15:04:05")
	endTimeFormatted := endTime.Format("2006-01-02 15:04:05")

	rows, err := u.db.Query(query, startTimeFormatted, endTimeFormatted)
	if err != nil {
		return nil, fmt.Errorf("usage query failed: %w", err)
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
			id, resourceType, unit, sku string
			start, end                  time.Time
			qty                         float64
		)
		if err := rows.Scan(&id, &resourceType, &start, &end, &qty, &unit, &sku); err != nil {
			return nil, err
		}

		price := u.pricingStore.GetSkuPrice(ctx, sku)

		records = append(records, store.UsageRecord{
			ID: id,
			Metadata: map[string]string{
				"resource_type": resourceType,
			},
			StartTime: start,
			EndTime:   end,
			Quantity:  qty,
			Unit:      unit,
			SKU:       sku,
			Rate:      price.PricePerUnit,
			Currency:  price.CurrencyCode,
		})
	}

	return records, nil
}

func buildCoalesceList(resourceTypes []string, suffix string) string {
	var fields []string
	for _, rt := range resourceTypes {
		fields = append(fields, fmt.Sprintf("usage_metadata.%s%s", rt, suffix))
	}
	return strings.Join(fields, ", ")
}

func buildResourceTypeCase(resourceTypes []string) string {
	var cases []string
	for _, rt := range resourceTypes {
		cases = append(cases, fmt.Sprintf("WHEN usage_metadata.%s_id IS NOT NULL THEN '%s'", rt, rt))
	}
	return strings.Join(cases, "\n")
}
