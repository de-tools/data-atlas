package domain

import "time"

type CostComponent struct {
	Type        string  // compute
	Value       float64 // 2
	Unit        string  // machines
	TotalAmount float64 // Value * Rate
	Rate        float64 // 0.0042
	Currency    string  // USD
	Description string  // "price for 2 x t4g.nano"
}

type Resource struct {
	Platform    string            // AWS
	Service     string            // EC2
	Name        string            // t4g.nano
	Description string            // "AWS EC2 t4g.nano"
	Tags        map[string]string // org -> data_engineering
	Metadata    struct {
		ID        string // uuid
		AccountID string // 12321312
		UserID    string // 35435543
		Region    string // us-east-1
	}
}

type ResourceCost struct {
	StartTime time.Time // 12.03.25
	EndTime   time.Time // 14.03.25
	Resource  Resource
	Costs     []CostComponent
}
