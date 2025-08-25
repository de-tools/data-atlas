package store

import "time"

type UsageStats struct {
	RecordsCount    int64
	FirstRecordTime *time.Time
}

type UsageRecord struct {
	ID           string
	ResourceID   string
	ResourceType string
	Metadata     map[string]string
	Quantity     float64
	Unit         string
	SKU          string
	Rate         float64
	Currency     string
	StartTime    time.Time
	EndTime      time.Time
}

type DailyUsageAggregate struct {
	Date       time.Time
	Resource   string
	TotalUsage float64
	TotalCost  float64
	Unit       string
	Currency   string
}

type MonthlyUsageAggregate struct {
	Year       int
	Month      time.Month
	Resource   string
	TotalUsage float64
	TotalCost  float64
	Unit       string
	Currency   string
}
