package workspace

import (
	"context"
	"fmt"
	"time"

	"github.com/de-tools/data-atlas/pkg/models/domain"
	"github.com/rs/zerolog/log"
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
	explorer Explorer,
	settings WarehouseAuditSettings,
) (domain.AuditReport, error) {
	logger := log.Ctx(ctx)
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

	// Analyze warehouse sizes and generate size-related findings
	if explorer != nil {
		sizeAnalysis, err := analyzeWarehouseSizes(ctx, records, explorer, settings)
		if err != nil {
			logger.Err(err).Msg("faield to run analyzeWarehouseSizes")
		} else {
			sizeFindings := generateSizeFindings(sizeAnalysis, settings)
			report.Findings = append(report.Findings, sizeFindings...)

			// Update summary with size analysis results
			updateSizeSummary(&report, sizeAnalysis, settings)
		}

		// Analyze best practices compliance and generate findings
		bestPracticesAnalysis, err := analyzeBestPracticesCompliance(ctx, records, explorer, settings)
		if err != nil {
			logger.Err(err).Msg("failed to run analyzeBestPracticesCompliance")
		} else {
			bestPracticesFindings := generateBestPracticesFindings(bestPracticesAnalysis, settings)
			report.Findings = append(report.Findings, bestPracticesFindings...)

			// Update summary with best practices analysis results
			updateBestPracticesSummary(&report, bestPracticesAnalysis, settings)
		}
	}

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

// WarehouseSizeInfo represents warehouse size and configuration information
type WarehouseSizeInfo struct {
	WarehouseID  string
	Name         string
	Size         string // e.g., "2X-Small", "Small", "Medium", "Large", etc.
	MinClusters  int
	MaxClusters  int
	NodeCount    int // Calculated based on size and cluster configuration
	IsServerless bool
	TotalCost    float64
	UsageHours   float64
	Currency     string
	SizeScore    float64 // Calculated score for ranking by size
}

// analyzeWarehouseSizes extracts warehouse size information using explorer and usage records
func analyzeWarehouseSizes(ctx context.Context, records []domain.ResourceCost, explorer Explorer, settings WarehouseAuditSettings) (map[string]*WarehouseSizeInfo, error) {
	warehouseSizes := make(map[string]*WarehouseSizeInfo)

	// Get unique warehouse IDs from usage records
	warehouseIDs := make(map[string]bool)
	for _, record := range records {
		warehouseIDs[record.Resource.Name] = true
	}

	// Fetch metadata for each warehouse
	for warehouseID := range warehouseIDs {
		metadata, err := explorer.GetWarehouseMetadata(ctx, warehouseID)
		if err != nil {
			// Skip warehouses where we can't get metadata
			continue
		}

		sizeInfo := &WarehouseSizeInfo{
			WarehouseID:  warehouseID,
			Name:         metadata.Name,
			Size:         metadata.Size,
			MinClusters:  metadata.MinNumClusters,
			MaxClusters:  metadata.MaxNumClusters,
			IsServerless: metadata.EnableServerless,
		}

		// Calculate node count based on size and cluster configuration
		sizeInfo.NodeCount = calculateNodeCount(sizeInfo.Size, sizeInfo.MaxClusters)

		// Calculate size score for ranking (higher score = larger warehouse)
		sizeInfo.SizeScore = calculateSizeScore(sizeInfo.Size, sizeInfo.NodeCount, sizeInfo.MaxClusters)

		warehouseSizes[warehouseID] = sizeInfo
	}

	// Accumulate cost and usage data from records
	for _, record := range records {
		warehouseID := record.Resource.Name
		if sizeInfo, exists := warehouseSizes[warehouseID]; exists {
			// Accumulate cost and usage data
			for _, cost := range record.Costs {
				sizeInfo.TotalCost += cost.TotalAmount
				if sizeInfo.Currency == "" {
					sizeInfo.Currency = cost.Currency
				}
			}

			// Calculate usage hours
			usageHours := record.EndTime.Sub(record.StartTime).Hours()
			sizeInfo.UsageHours += usageHours
		}
	}

	return warehouseSizes, nil
}

// calculateNodeCount estimates the number of nodes based on warehouse size and cluster configuration
func calculateNodeCount(size string, maxClusters int) int {
	// Base node count per cluster based on warehouse size
	baseNodes := getBaseNodeCountForSize(size)

	// Total nodes = base nodes per cluster * max clusters
	if maxClusters > 0 {
		return baseNodes * maxClusters
	}

	return baseNodes
}

// getBaseNodeCountForSize returns the base number of nodes per cluster for a given warehouse size
func getBaseNodeCountForSize(size string) int {
	switch size {
	case "2X-Small":
		return 1
	case "X-Small":
		return 2
	case "Small":
		return 4
	case "Medium":
		return 8
	case "Large":
		return 16
	case "X-Large":
		return 32
	case "2X-Large":
		return 64
	case "3X-Large":
		return 128
	case "4X-Large":
		return 256
	default:
		// Default to small if size is unknown
		return 4
	}
}

// calculateSizeScore calculates a numeric score for ranking warehouses by size
func calculateSizeScore(size string, nodeCount, maxClusters int) float64 {
	// Base score from warehouse size
	baseScore := float64(getBaseNodeCountForSize(size))

	// Multiply by cluster count for total capacity
	clusterMultiplier := float64(maxClusters)
	if clusterMultiplier == 0 {
		clusterMultiplier = 1
	}

	return baseScore * clusterMultiplier
}

// generateSizeFindings creates audit findings for warehouse size analysis
func generateSizeFindings(warehouseSizes map[string]*WarehouseSizeInfo, settings WarehouseAuditSettings) []domain.AuditFinding {
	var findings []domain.AuditFinding

	// Create a slice of warehouses for sorting by size
	type warehouseRanking struct {
		info *WarehouseSizeInfo
		rank int
	}

	var rankings []warehouseRanking
	for _, sizeInfo := range warehouseSizes {
		rankings = append(rankings, warehouseRanking{info: sizeInfo})
	}

	// Sort by size score (descending - largest first)
	for i := 0; i < len(rankings)-1; i++ {
		for j := i + 1; j < len(rankings); j++ {
			if rankings[i].info.SizeScore < rankings[j].info.SizeScore {
				rankings[i], rankings[j] = rankings[j], rankings[i]
			}
		}
	}

	// Assign ranks and identify top largest warehouses
	for i, ranking := range rankings {
		ranking.rank = i + 1
		rankings[i] = ranking

		// Flag top N largest warehouses for review
		if ranking.rank <= settings.TopLargestCount {
			sizeInfo := ranking.info

			description := fmt.Sprintf("Warehouse ranks #%d in size with %s configuration",
				ranking.rank, sizeInfo.Size)

			if sizeInfo.MaxClusters > 1 {
				description += fmt.Sprintf(" (up to %d clusters, ~%d total nodes)",
					sizeInfo.MaxClusters, sizeInfo.NodeCount)
			} else {
				description += fmt.Sprintf(" (~%d nodes)", sizeInfo.NodeCount)
			}

			if sizeInfo.TotalCost > 0 {
				description += fmt.Sprintf(", total cost: %.2f %s", sizeInfo.TotalCost, sizeInfo.Currency)
			}

			findings = append(findings, domain.AuditFinding{
				Id: fmt.Sprintf("%s_oversized_warehouse", sizeInfo.WarehouseID),
				Resource: domain.ResourceDef{
					Platform: "Databricks",
					Service:  "warehouse",
					Name:     sizeInfo.WarehouseID,
				},
				Issue:          "oversized_warehouse",
				Description:    description,
				Recommendation: "Review if warehouse size is appropriate for workload complexity. Consider right-sizing based on query patterns and performance requirements.",
				Severity:       domain.SeverityMedium,
			})
		}
	}

	return findings
}

// updateSizeSummary updates the audit report summary with size analysis results
func updateSizeSummary(report *domain.AuditReport, warehouseSizes map[string]*WarehouseSizeInfo, settings WarehouseAuditSettings) {
	warehousesWithSizeIssues := 0
	totalCostAnalyzed := 0.0
	var currency string

	// Count warehouses flagged for size review (top N largest)
	type sizeRanking struct {
		sizeScore float64
		cost      float64
		currency  string
	}

	var rankings []sizeRanking
	for _, sizeInfo := range warehouseSizes {
		rankings = append(rankings, sizeRanking{
			sizeScore: sizeInfo.SizeScore,
			cost:      sizeInfo.TotalCost,
			currency:  sizeInfo.Currency,
		})

		totalCostAnalyzed += sizeInfo.TotalCost
		if currency == "" {
			currency = sizeInfo.Currency
		}
	}

	// Sort by size score to identify top largest
	for i := 0; i < len(rankings)-1; i++ {
		for j := i + 1; j < len(rankings); j++ {
			if rankings[i].sizeScore < rankings[j].sizeScore {
				rankings[i], rankings[j] = rankings[j], rankings[i]
			}
		}
	}

	// Count top N as having size issues (flagged for review)
	if len(rankings) > settings.TopLargestCount {
		warehousesWithSizeIssues = settings.TopLargestCount
	} else {
		warehousesWithSizeIssues = len(rankings)
	}

	report.Summary["warehouses_with_size_issues"] = warehousesWithSizeIssues
	report.Summary["total_cost_analyzed"] = totalCostAnalyzed

	if currency != "" {
		report.Summary["currency"] = currency
	}
}

// WarehouseBestPracticesInfo represents best practices compliance information for a warehouse
type WarehouseBestPracticesInfo struct {
	WarehouseID      string
	Name             string
	AutoStopMins     int
	HasAutoStop      bool
	EnableServerless bool
	WarehouseType    string
	ComplianceScore  float64
	MissingPractices []string
	TotalCost        float64
	UsageHours       float64
	Currency         string
}

// analyzeBestPracticesCompliance evaluates warehouse configurations against best practices
func analyzeBestPracticesCompliance(ctx context.Context, records []domain.ResourceCost, explorer Explorer, settings WarehouseAuditSettings) (map[string]*WarehouseBestPracticesInfo, error) {
	bestPracticesInfo := make(map[string]*WarehouseBestPracticesInfo)

	// Get unique warehouse IDs from usage records
	warehouseIDs := make(map[string]bool)
	for _, record := range records {
		warehouseIDs[record.Resource.Name] = true
	}

	// Fetch metadata for each warehouse and analyze best practices
	for warehouseID := range warehouseIDs {
		metadata, err := explorer.GetWarehouseMetadata(ctx, warehouseID)
		if err != nil {
			// Skip warehouses where we can't get metadata
			continue
		}

		info := &WarehouseBestPracticesInfo{
			WarehouseID:      warehouseID,
			Name:             metadata.Name,
			AutoStopMins:     metadata.AutoStopMins,
			HasAutoStop:      metadata.AutoStopMins > 0,
			EnableServerless: metadata.EnableServerless,
			WarehouseType:    metadata.Size, // Using Size as warehouse type for now
			MissingPractices: []string{},
		}

		// Evaluate best practices compliance
		evaluateBestPractices(info)

		bestPracticesInfo[warehouseID] = info
	}

	// Accumulate cost and usage data from records
	for _, record := range records {
		warehouseID := record.Resource.Name
		if info, exists := bestPracticesInfo[warehouseID]; exists {
			// Accumulate cost and usage data
			for _, cost := range record.Costs {
				info.TotalCost += cost.TotalAmount
				if info.Currency == "" {
					info.Currency = cost.Currency
				}
			}

			// Calculate usage hours
			usageHours := record.EndTime.Sub(record.StartTime).Hours()
			info.UsageHours += usageHours
		}
	}

	return bestPracticesInfo, nil
}

// evaluateBestPractices calculates compliance score and identifies missing best practices
func evaluateBestPractices(info *WarehouseBestPracticesInfo) {
	totalPractices := 0
	compliantPractices := 0

	// Check auto-stop configuration
	totalPractices++
	if info.HasAutoStop {
		compliantPractices++
		// Check if auto-stop timeout is reasonable (not too high)
		if info.AutoStopMins > 120 { // More than 2 hours
			info.MissingPractices = append(info.MissingPractices, "auto_stop_timeout_too_high")
		}
	} else {
		info.MissingPractices = append(info.MissingPractices, "auto_stop_disabled")
	}

	// Check for serverless configuration (best practice for intermittent workloads)
	totalPractices++
	if info.EnableServerless {
		compliantPractices++
	} else {
		// Only flag as missing if it's a Pro warehouse (serverless requires Pro)
		if info.WarehouseType != "2X-Small" && info.WarehouseType != "X-Small" {
			info.MissingPractices = append(info.MissingPractices, "serverless_not_enabled")
		} else {
			// For small warehouses, serverless might not be necessary
			compliantPractices++
		}
	}

	// Note: Budget alerts and spending limits are not available in the current warehouse metadata
	// These would require additional API calls to workspace settings or billing APIs
	// For now, we'll add placeholders that can be implemented when those APIs are available

	// Placeholder for budget alerts (would require additional API integration)
	totalPractices++
	info.MissingPractices = append(info.MissingPractices, "budget_alerts_unknown")

	// Placeholder for spending limits (would require additional API integration)
	totalPractices++
	info.MissingPractices = append(info.MissingPractices, "spending_limits_unknown")

	// Calculate compliance score
	if totalPractices > 0 {
		info.ComplianceScore = float64(compliantPractices) / float64(totalPractices)
	}
}

// generateBestPracticesFindings creates audit findings for warehouses with best practices issues
func generateBestPracticesFindings(bestPracticesInfo map[string]*WarehouseBestPracticesInfo, settings WarehouseAuditSettings) []domain.AuditFinding {
	var findings []domain.AuditFinding

	for warehouseID, info := range bestPracticesInfo {
		// Generate findings for each missing best practice
		for _, missingPractice := range info.MissingPractices {
			var finding domain.AuditFinding

			switch missingPractice {
			case "auto_stop_disabled":
				finding = domain.AuditFinding{
					Id: fmt.Sprintf("%s_auto_stop_disabled", warehouseID),
					Resource: domain.ResourceDef{
						Platform: "Databricks",
						Service:  "warehouse",
						Name:     warehouseID,
					},
					Issue:          "auto_stop_disabled",
					Description:    "Warehouse does not have auto-stop configured, which may lead to unnecessary costs from idle resources.",
					Recommendation: "Enable auto-stop with an appropriate timeout (recommended: 10-30 minutes for interactive workloads, 60-120 minutes for batch workloads).",
					Severity:       domain.SeverityHigh,
				}

			case "auto_stop_timeout_too_high":
				finding = domain.AuditFinding{
					Id: fmt.Sprintf("%s_auto_stop_timeout_too_high", warehouseID),
					Resource: domain.ResourceDef{
						Platform: "Databricks",
						Service:  "warehouse",
						Name:     warehouseID,
					},
					Issue:          "auto_stop_timeout_too_high",
					Description:    fmt.Sprintf("Warehouse auto-stop timeout is set to %d minutes, which may be too high for cost optimization.", info.AutoStopMins),
					Recommendation: "Consider reducing auto-stop timeout to 10-30 minutes for interactive workloads or 60-120 minutes for batch workloads.",
					Severity:       domain.SeverityMedium,
				}

			case "serverless_not_enabled":
				finding = domain.AuditFinding{
					Id: fmt.Sprintf("%s_serverless_not_enabled", warehouseID),
					Resource: domain.ResourceDef{
						Platform: "Databricks",
						Service:  "warehouse",
						Name:     warehouseID,
					},
					Issue:          "serverless_not_enabled",
					Description:    "Warehouse is not configured for serverless compute, which could provide better cost efficiency for variable workloads.",
					Recommendation: "Consider enabling serverless compute for better cost optimization, especially for intermittent or unpredictable workloads.",
					Severity:       domain.SeverityLow,
				}

			case "budget_alerts_unknown":
				finding = domain.AuditFinding{
					Id: fmt.Sprintf("%s_budget_alerts_unknown", warehouseID),
					Resource: domain.ResourceDef{
						Platform: "Databricks",
						Service:  "warehouse",
						Name:     warehouseID,
					},
					Issue:          "budget_alerts_unknown",
					Description:    "Unable to verify if budget alerts are configured for this warehouse.",
					Recommendation: "Ensure budget alerts are configured to monitor warehouse spending and prevent cost overruns.",
					Severity:       domain.SeverityLow,
				}

			case "spending_limits_unknown":
				finding = domain.AuditFinding{
					Id: fmt.Sprintf("%s_spending_limits_unknown", warehouseID),
					Resource: domain.ResourceDef{
						Platform: "Databricks",
						Service:  "warehouse",
						Name:     warehouseID,
					},
					Issue:          "spending_limits_unknown",
					Description:    "Unable to verify if spending limits are configured for this warehouse.",
					Recommendation: "Consider setting spending limits to prevent unexpected cost increases and maintain budget control.",
					Severity:       domain.SeverityLow,
				}
			}

			if finding.Id != "" {
				findings = append(findings, finding)
			}
		}

		// Generate finding for overall low compliance score
		if info.ComplianceScore < 0.5 { // Less than 50% compliance
			findings = append(findings, domain.AuditFinding{
				Id: fmt.Sprintf("%s_low_compliance_score", warehouseID),
				Resource: domain.ResourceDef{
					Platform: "Databricks",
					Service:  "warehouse",
					Name:     warehouseID,
				},
				Issue:          "low_compliance_score",
				Description:    fmt.Sprintf("Warehouse has a low best practices compliance score of %.0f%%. Missing practices: %v", info.ComplianceScore*100, info.MissingPractices),
				Recommendation: "Review and implement missing best practices to improve cost efficiency and governance.",
				Severity:       domain.SeverityMedium,
			})
		}
	}

	return findings
}

// updateBestPracticesSummary updates the audit report summary with best practices analysis results
func updateBestPracticesSummary(report *domain.AuditReport, bestPracticesInfo map[string]*WarehouseBestPracticesInfo, settings WarehouseAuditSettings) {
	warehousesWithBestPracticeIssues := 0
	totalComplianceScore := 0.0
	warehousesWithAutoStop := 0
	warehousesWithServerless := 0

	for _, info := range bestPracticesInfo {
		// Count warehouses with best practice issues
		if len(info.MissingPractices) > 0 || info.ComplianceScore < 0.5 {
			warehousesWithBestPracticeIssues++
		}

		totalComplianceScore += info.ComplianceScore

		// Count specific best practices
		if info.HasAutoStop {
			warehousesWithAutoStop++
		}
		if info.EnableServerless {
			warehousesWithServerless++
		}
	}

	report.Summary["warehouses_with_best_practice_issues"] = warehousesWithBestPracticeIssues
	report.Summary["warehouses_with_auto_stop"] = warehousesWithAutoStop
	report.Summary["warehouses_with_serverless"] = warehousesWithServerless

	// Calculate average compliance score
	if len(bestPracticesInfo) > 0 {
		avgComplianceScore := totalComplianceScore / float64(len(bestPracticesInfo))
		report.Summary["average_compliance_score"] = fmt.Sprintf("%.1f%%", avgComplianceScore*100)
	}
}
