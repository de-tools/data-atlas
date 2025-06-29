package store

import "time"

type UsageRecord struct {
	ID        string
	Resource  string
	Metadata  map[string]string
	Quantity  float64
	Unit      string
	SKU       string
	Rate      float64
	Currency  string
	StartTime time.Time
	EndTime   time.Time
}
