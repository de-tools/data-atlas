package domain

import "time"

type UsageStats struct {
	RecordsCount    int64
	FirstRecordTime *time.Time
}

type CostComponent struct {
	Type        string  // compute
	Value       float64 // 2
	Unit        string  // machines
	TotalAmount float64 // Value * Rate
	Rate        float64 // 0.0042
	Currency    string  // USD
	SKU         string
	Description string // "price for 2 x t4g.nano"
}

type ResourceDef struct {
	Platform    string            // AWS
	Service     string            // EC2
	Name        string            // t4g.nano
	Description string            // "AWS EC2 t4g.nano"
	Tags        map[string]string // org -> data_engineering
	Metadata    map[string]string // ID, AccountID, UserID, Region
}

type ResourceCost struct {
	StartTime time.Time
	EndTime   time.Time
	Resource  ResourceDef
	Costs     []CostComponent
}
