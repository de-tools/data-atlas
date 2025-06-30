package usage

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"time"

	"github.com/de-tools/data-atlas/pkg/store/pricing"

	"github.com/de-tools/data-atlas/pkg/models/store"
	"github.com/rs/zerolog"
)

type Store interface {
	GetResourceUsage(
		ctx context.Context,
		resource string,
		startTime time.Time,
		endTime time.Time,
	) ([]store.UsageRecord, error)
	GetDailyUsage(
		ctx context.Context,
		resource string,
		startTime time.Time,
		endTime time.Time,
	) ([]store.DailyUsageAggregate, error)
	GetMonthlyUsage(
		ctx context.Context,
		resource string,
		startTime time.Time,
		endTime time.Time,
	) ([]store.MonthlyUsageAggregate, error)
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

func (u *usageStore) GetResourceUsage(
	ctx context.Context,
	resource string,
	startTime time.Time,
	endTime time.Time,
) ([]store.UsageRecord, error) {
	logger := zerolog.Ctx(ctx)
	id := fmt.Sprintf("%s_id", resource)
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
		  AND usage_start_time >= timestamp(?1)
		  AND usage_start_time < timestamp(?2)
		ORDER BY usage_start_time DESC
	`, id)

	startTimeFormatted := startTime.Format("2006-01-02 15:04:05")
	endTimeFormatted := endTime.Format("2006-01-02 15:04:05")

	rows, err := u.db.Query(query, startTimeFormatted, endTimeFormatted)
	if err != nil {
		return nil, fmt.Errorf("%s usage query failed: %w", resource, err)
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
			id, unit, sku string
			start, end    time.Time
			qty           float64
		)
		if err := rows.Scan(&id, &start, &end, &qty, &unit, &sku); err != nil {
			return nil, err
		}

		price := u.pricingStore.GetSkuPrice(ctx, sku)

		records = append(records, store.UsageRecord{
			ID:        id,
			Metadata:  map[string]string{},
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

func (u *usageStore) GetDailyUsage(
	ctx context.Context,
	resource string,
	startTime time.Time,
	endTime time.Time,
) ([]store.DailyUsageAggregate, error) {
	logger := zerolog.Ctx(ctx)
	id := fmt.Sprintf("%s_id", resource)
	query := fmt.Sprintf(`
		SELECT
            usage_date,
            SUM(usage_quantity) as total_usage,
            usage_unit,
            sku_name,
            COUNT(*) as record_count
        FROM system.billing.usage
        WHERE usage_metadata.%[1]s IS NOT NULL
            AND usage_date >= ?1
            AND usage_date < ?2
        GROUP BY 
            usage_date,
            usage_unit,
            sku_name
        ORDER BY usage_date DESC
    `, id)

	startTimeFormatted := startTime.Format("2006-01-02 15:04:05")
	endTimeFormatted := endTime.Format("2006-01-02 15:04:05")

	rows, err := u.db.Query(query, startTimeFormatted, endTimeFormatted)
	if err != nil {
		return nil, fmt.Errorf("aggregated %s usage query failed: %w", resource, err)
	}

	defer func(rows *sql.Rows) {
		err := rows.Close()
		if err != nil {
			logger.Warn().Err(err).Msg("failed to close aggregated usage query rows")
		}
	}(rows)

	dailyAggregates := make(map[time.Time]*store.DailyUsageAggregate)

	for rows.Next() {
		var (
			usageDate   time.Time
			totalUsage  float64
			unit, sku   string
			recordCount int
		)
		if err := rows.Scan(&usageDate, &totalUsage, &unit, &sku, &recordCount); err != nil {
			return nil, fmt.Errorf("failed to scan aggregated usage row: %w", err)
		}

		price := u.pricingStore.GetSkuPrice(ctx, sku)
		totalCost := totalUsage * price.PricePerUnit
		agg, exists := dailyAggregates[usageDate]
		if !exists {
			agg = &store.DailyUsageAggregate{
				Date:     usageDate,
				Resource: resource,
				Unit:     unit,
				Currency: price.CurrencyCode,
			}
			dailyAggregates[usageDate] = agg
		}

		agg.TotalUsage += totalUsage
		agg.TotalCost += totalCost
	}

	var result []store.DailyUsageAggregate
	for _, agg := range dailyAggregates {
		result = append(result, *agg)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Date.After(result[j].Date)
	})

	return result, nil
}

func (u *usageStore) GetMonthlyUsage(
	ctx context.Context,
	resource string,
	startTime time.Time,
	endTime time.Time,
) ([]store.MonthlyUsageAggregate, error) {
	logger := zerolog.Ctx(ctx)
	id := fmt.Sprintf("%s_id", resource)

	query := fmt.Sprintf(`
        SELECT
            YEAR(usage_date) as year,
            MONTH(usage_date) as month,
            SUM(usage_quantity) as total_usage,
            usage_unit,
            sku_name,
            COUNT(*) as record_count
        FROM system.billing.usage
        WHERE usage_metadata.%[1]s IS NOT NULL
            AND usage_date >= ?1
            AND usage_date < ?2
        GROUP BY 
            YEAR(usage_date),
            MONTH(usage_date),
            usage_unit,
            sku_name
        ORDER BY year DESC, month DESC
    `, id)

	startDateStr := startTime.Format("2006-01-02")
	endDateStr := endTime.Format("2006-01-02")

	rows, err := u.db.Query(query, startDateStr, endDateStr)
	if err != nil {
		return nil, fmt.Errorf("aggregated yearly %s usage query failed: %w", resource, err)
	}
	defer func(rows *sql.Rows) {
		err := rows.Close()
		if err != nil {
			logger.Warn().Err(err).Msg("failed to close aggregated usage query rows")
		}
	}(rows)

	// Map to store aggregates by year-month
	monthlyAggregates := make(map[string]*store.MonthlyUsageAggregate)

	for rows.Next() {
		var (
			year, month int
			totalUsage  float64
			unit, sku   string
			recordCount int
		)
		if err := rows.Scan(&year, &month, &totalUsage, &unit, &sku, &recordCount); err != nil {
			return nil, fmt.Errorf("failed to scan aggregated usage row: %w", err)
		}

		key := fmt.Sprintf("%d-%02d", year, month)
		price := u.pricingStore.GetSkuPrice(ctx, sku)
		totalCost := totalUsage * price.PricePerUnit

		agg, exists := monthlyAggregates[key]
		if !exists {
			agg = &store.MonthlyUsageAggregate{
				Year:     year,
				Month:    time.Month(month),
				Resource: resource,
				Unit:     unit,
				Currency: price.CurrencyCode,
			}
			monthlyAggregates[key] = agg
		}

		agg.TotalUsage += totalUsage
		agg.TotalCost += totalCost
	}

	var result []store.MonthlyUsageAggregate
	for _, agg := range monthlyAggregates {
		result = append(result, *agg)
	}

	sort.Slice(result, func(i, j int) bool {
		if result[i].Year != result[j].Year {
			return result[i].Year > result[j].Year
		}
		return result[i].Month > result[j].Month
	})

	return result, nil
}
