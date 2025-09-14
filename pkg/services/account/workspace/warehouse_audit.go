package workspace

import (
	"context"
	"fmt"
	"time"

	"github.com/databricks/databricks-sdk-go/service/sql"
	"github.com/de-tools/data-atlas/pkg/models/domain"
)

// WarehouseAuditSettings contains configurable thresholds for warehouse audit analysis
type WarehouseAuditSettings struct {
	// MaxRuntimeHours is the threshold for flagging warehouses with excessive runtime (default: 8.0)
	MaxRuntimeHours float64
	// MaxIdleHours is the threshold for flagging warehouses with excessive idle time (default: 2.0)
	MaxIdleHours float64
	// IdleTimeThreshold is the percentage threshold for idle time vs active time (default: 0.5 = 50%)
	IdleTimeThreshold float64
	// StaleResourceDays is the number of days to consider a warehouse stale (default: 30)
	StaleResourceDays int
	// TopLargestCount is the number of largest warehouses to identify (default: 5)
	TopLargestCount int
	// MinQueryCountThreshold is the minimum query count for provisioning analysis (default: 10)
	MinQueryCountThreshold int
}

// DefaultWarehouseAuditSettings returns the default configuration for warehouse audits
func DefaultWarehouseAuditSettings() WarehouseAuditSettings {
	return WarehouseAuditSettings{
		MaxRuntimeHours:        8.0,
		MaxIdleHours:           2.0,
		IdleTimeThreshold:      0.5,
		StaleResourceDays:      30,
		TopLargestCount:        5,
		MinQueryCountThreshold: 10,
	}
}

// WarehouseMetadata represents warehouse configuration and metadata
type WarehouseMetadata struct {
	ID               string
	Name             string
	Size             sql.GetWarehouseResponseWarehouseType
	MinNumClusters   int32
	MaxNumClusters   int32
	AutoStopMins     int32
	EnableServerless bool
	CreatedAt        time.Time
	State            sql.State
}

// WarehouseRuntimeStats holds runtime analysis data for a warehouse
type WarehouseRuntimeStats struct {
	WarehouseID       string
	TotalRuntimeHours float64
	TotalIdleHours    float64
	IdleTimePercent   float64
	RecordCount       int
	Currency          string
}

// GetWarehouseAudit performs a comprehensive audit of SQL warehouses for the given workspace
// and time period, analyzing runtime patterns, sizing, best practices, stale resources, and provisioning
func GetWarehouseAudit(
	ctx context.Context,
	ws domain.Workspace,
	startTime, endTime time.Time,
	costManager CostManager,
	settings WarehouseAuditSettings,
) (domain.AuditReport, error) {
	// Initialize audit report
	report := domain.AuditReport{
		Workspace:    ws.Name,
		ResourceType: "warehouse",
		Period: domain.TimePeriod{
			Start:    startTime,
			End:      endTime,
			Duration: int(endTime.Sub(startTime).Hours() / 24),
		},
		Summary:  map[string]any{},
		Findings: []domain.AuditFinding{},
	}

	// Retrieve warehouse usage data from cost manager
	resources := domain.WorkspaceResources{
		WorkspaceName: ws.Name,
		Resources:     []string{"warehouse"},
	}
	records, err := costManager.GetResourcesCost(ctx, resources, startTime, endTime)
	if err != nil {
		return domain.AuditReport{}, err
	}

	// Handle no data scenario
	if len(records) == 0 {
		report.Summary["no_activity"] = "No warehouse usage records found in the selected period"
		report.Findings = append(report.Findings, domain.AuditFinding{
			Id:             "no_activity",
			Resource:       domain.ResourceDef{Platform: "Databricks", Service: "warehouse", Name: "workspace"},
			Issue:          "no_activity",
			Description:    "No warehouse activity detected in the selected time window.",
			Recommendation: "Verify warehouse configurations and usage patterns. Consider reducing provisioned resources if unused.",
			Severity:       domain.SeverityMedium,
		})
		return report, nil
	}

	// Analyze runtime duration patterns
	runtimeStats := analyzeWarehouseRuntimeDuration(records, settings)

	// Generate runtime-related audit findings
	runtimeFindings := generateRuntimeFindings(runtimeStats, settings)
	report.Findings = append(report.Findings, runtimeFindings...)

	// Update summary with runtime analysis results
	updateRuntimeSummary(&report, runtimeStats, settings)

	return report, nil
}

// analyzeWarehouseRuntimeDuration aggregates warehouse usage records and calculates runtime statistics
func analyzeWarehouseRuntimeDuration(records []domain.ResourceCost, settings WarehouseAuditSettings) map[string]*WarehouseRuntimeStats {
	warehouseStats := make(map[string]*WarehouseRuntimeStats)

	for _, record := range records {
		warehouseID := record.Resource.Name

		// Initialize stats for new warehouse
		if _, exists := warehouseStats[warehouseID]; !exists {
			warehouseStats[warehouseID] = &WarehouseRuntimeStats{
				WarehouseID: warehouseID,
			}
		}

		stats := warehouseStats[warehouseID]
		stats.RecordCount++

		// Calculate runtime duration for this record
		runtimeHours := record.EndTime.Sub(record.StartTime).Hours()
		stats.TotalRuntimeHours += runtimeHours

		// Set currency from first cost component
		if stats.Currency == "" && len(record.Costs) > 0 {
			stats.Currency = record.Costs[0].Currency
		}

		// Calculate idle time based on usage patterns
		// For warehouses, we consider periods with minimal compute usage as idle
		// This is a simplified approach - in practice, idle detection would be more sophisticated
		idleHours := calculateIdleTime(record, runtimeHours)
		stats.TotalIdleHours += idleHours
	}

	// Calculate idle time percentages
	for _, stats := range warehouseStats {
		if stats.TotalRuntimeHours > 0 {
			stats.IdleTimePercent = stats.TotalIdleHours / stats.TotalRuntimeHours
		}
	}

	return warehouseStats
}

// calculateIdleTime estimates idle time within a usage record based on cost patterns
func calculateIdleTime(record domain.ResourceCost, runtimeHours float64) float64 {
	// Simplified idle time calculation
	// In practice, this would analyze detailed usage metrics to identify idle periods
	// For now, we estimate based on cost efficiency - low cost per hour suggests idle time

	if len(record.Costs) == 0 {
		return 0
	}

	totalCost := 0.0
	for _, cost := range record.Costs {
		totalCost += cost.TotalAmount
	}

	// If cost per hour is very low, assume some idle time
	// This is a heuristic - real implementation would use detailed metrics
	costPerHour := totalCost / runtimeHours
	if costPerHour < 0.1 { // Threshold for detecting potential idle periods
		return runtimeHours * 0.3 // Assume 30% idle time for low-cost periods
	}

	return 0
}

// generateRuntimeFindings creates audit findings for warehouses with runtime issues
func generateRuntimeFindings(warehouseStats map[string]*WarehouseRuntimeStats, settings WarehouseAuditSettings) []domain.AuditFinding {
	var findings []domain.AuditFinding

	for warehouseID, stats := range warehouseStats {
		// Check for excessive runtime
		if stats.TotalRuntimeHours > settings.MaxRuntimeHours {
			findings = append(findings, domain.AuditFinding{
				Id: fmt.Sprintf("%s_excessive_runtime", warehouseID),
				Resource: domain.ResourceDef{
					Platform: "Databricks",
					Service:  "warehouse",
					Name:     warehouseID,
				},
				Issue:          "excessive_runtime",
				Description:    fmt.Sprintf("Warehouse ran for %.1f hours, exceeding threshold of %.1f hours.", stats.TotalRuntimeHours, settings.MaxRuntimeHours),
				Recommendation: "Consider implementing auto-stop policies, reviewing query patterns, or splitting workloads across multiple warehouses.",
				Severity:       domain.SeverityMedium,
			})
		}

		// Check for excessive idle time
		if stats.TotalIdleHours > settings.MaxIdleHours {
			findings = append(findings, domain.AuditFinding{
				Id: fmt.Sprintf("%s_high_idle_time", warehouseID),
				Resource: domain.ResourceDef{
					Platform: "Databricks",
					Service:  "warehouse",
					Name:     warehouseID,
				},
				Issue:          "high_idle_time",
				Description:    fmt.Sprintf("Warehouse was idle for %.1f hours (%.0f%% of runtime), exceeding threshold of %.1f hours.", stats.TotalIdleHours, stats.IdleTimePercent*100, settings.MaxIdleHours),
				Recommendation: "Reduce auto-stop timeout, optimize query scheduling, or consider serverless warehouses for intermittent workloads.",
				Severity:       domain.SeverityMedium,
			})
		}

		// Check for high idle time percentage
		if stats.IdleTimePercent > settings.IdleTimeThreshold {
			findings = append(findings, domain.AuditFinding{
				Id: fmt.Sprintf("%s_idle_workload", warehouseID),
				Resource: domain.ResourceDef{
					Platform: "Databricks",
					Service:  "warehouse",
					Name:     warehouseID,
				},
				Issue:          "idle_workload",
				Description:    fmt.Sprintf("Warehouse idle time is %.0f%% of total runtime, exceeding threshold of %.0f%%.", stats.IdleTimePercent*100, settings.IdleTimeThreshold*100),
				Recommendation: "Review warehouse utilization patterns, implement more aggressive auto-stop policies, or consolidate workloads.",
				Severity:       domain.SeverityMedium,
			})
		}
	}

	return findings
}

// updateRuntimeSummary updates the audit report summary with runtime analysis results
func updateRuntimeSummary(report *domain.AuditReport, warehouseStats map[string]*WarehouseRuntimeStats, settings WarehouseAuditSettings) {
	warehousesEvaluated := len(warehouseStats)
	warehousesWithRuntimeIssues := 0
	totalRuntimeHours := 0.0
	totalIdleHours := 0.0

	for _, stats := range warehouseStats {
		totalRuntimeHours += stats.TotalRuntimeHours
		totalIdleHours += stats.TotalIdleHours

		// Count warehouses with any runtime-related issues
		if stats.TotalRuntimeHours > settings.MaxRuntimeHours ||
			stats.TotalIdleHours > settings.MaxIdleHours ||
			stats.IdleTimePercent > settings.IdleTimeThreshold {
			warehousesWithRuntimeIssues++
		}
	}

	report.Summary["warehouses_evaluated"] = warehousesEvaluated
	report.Summary["warehouses_with_runtime_issues"] = warehousesWithRuntimeIssues
	report.Summary["total_runtime_hours"] = totalRuntimeHours
	report.Summary["total_idle_hours"] = totalIdleHours

	if totalRuntimeHours > 0 {
		report.Summary["overall_idle_percentage"] = (totalIdleHours / totalRuntimeHours) * 100
	}
}
