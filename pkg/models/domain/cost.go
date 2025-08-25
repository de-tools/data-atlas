package domain

import (
	"maps"
	"slices"
	"time"
)

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
	SKU         string  // PREMIUM_DATABRICKS_STORAGE_EUROPE_IRELAND
	Description string  // "price for 2 x t4g.nano"
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
	ID        string
	StartTime time.Time
	EndTime   time.Time
	Resource  ResourceDef
	Costs     []CostComponent
}

var SupportedResources = map[string]string{
	"sharing_materialization": "sharing_materialization_id",
	"central_clean_room":      "central_clean_room_id",
	"budget_policy":           "budget_policy_id",
	"job":                     "job_id",
	"job_run":                 "job_run_id",
	"dlt_update":              "dlt_update_id",
	"dlt_maintenance":         "dlt_maintenance_id",
	"instance_pool":           "instance_pool_id",
	"app":                     "app_id",
	"database_instance":       "database_instance_id",
	"ai_runtime_pool":         "ai_runtime_pool_id",
	"cluster":                 "cluster_id",
	"endpoint":                "endpoint_id",
	"warehouse":               "warehouse_id",
	"dlt_pipeline":            "dlt_pipeline_id",
	"metastore":               "metastore_id",
}

var SupportedResourcesList = slices.Collect(maps.Keys(SupportedResources))
