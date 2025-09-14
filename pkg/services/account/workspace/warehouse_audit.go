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

		// Analyze stale resources and generate findings
		staleResourcesAnalysis, err := analyzeStaleResources(ctx, records, explorer, settings, startTime, endTime)
		if err != nil {
			logger.Err(err).Msg("failed to run analyzeStaleResources")
		} else {
			staleResourcesFindings := generateStaleResourcesFindings(staleResourcesAnalysis, settings)
			report.Findings = append(report.Findings, staleResourcesFindings...)

			// Update summary with stale resources analysis results
			updateStaleResourcesSummary(&report, staleResourcesAnalysis, settings)
		}

		// Analyze provisioning patterns and generate findings
		provisioningAnalysis, err := analyzeProvisioningPatterns(ctx, records, explorer, settings)
		if err != nil {
			logger.Err(err).Msg("failed to run analyzeProvisioningPatterns")
		} else {
			provisioningFindings := generateProvisioningFindings(provisioningAnalysis, settings)
			report.Findings = append(report.Findings, provisioningFindings...)

			// Update summary with provisioning analysis results
			updateProvisioningSummary(&report, provisioningAnalysis, settings)
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

// WarehouseStaleResourceInfo represents stale resource analysis information for a warehouse
type WarehouseStaleResourceInfo struct {
	WarehouseID       string
	Name              string
	LastActivityTime  *time.Time
	CreatedTime       *time.Time
	DaysSinceActivity int
	DaysSinceCreation int
	QueryCount        int
	HasActivity       bool
	IsStale           bool
	IsOrphaned        bool
	NeverStarted      bool
	TotalCost         float64
	UsageHours        float64
	Currency          string
}

// analyzeStaleResources identifies warehouses with no recent activity or that are orphaned
func analyzeStaleResources(ctx context.Context, records []domain.ResourceCost, explorer Explorer, settings WarehouseAuditSettings, startTime, endTime time.Time) (map[string]*WarehouseStaleResourceInfo, error) {
	staleResourcesInfo := make(map[string]*WarehouseStaleResourceInfo)

	// Get unique warehouse IDs from usage records
	warehouseIDs := make(map[string]bool)
	for _, record := range records {
		warehouseIDs[record.Resource.Name] = true
	}

	// If we have an explorer, also get all warehouses from metadata to detect orphaned ones
	if explorer != nil {
		allWarehouses, err := explorer.ListWarehouses(ctx)
		if err == nil {
			for _, warehouse := range allWarehouses {
				warehouseIDs[warehouse.ID] = true
			}
		}
	}

	// Analyze each warehouse for stale resource patterns
	for warehouseID := range warehouseIDs {
		info := &WarehouseStaleResourceInfo{
			WarehouseID: warehouseID,
			Name:        warehouseID, // Default to ID, will be updated if metadata is available
		}

		// Get warehouse metadata if explorer is available
		if explorer != nil {
			metadata, err := explorer.GetWarehouseMetadata(ctx, warehouseID)
			if err == nil {
				info.Name = metadata.Name
				// Note: CreatedTime would need to be added to warehouse metadata
				// For now, we'll work with what's available
			}
		}

		staleResourcesInfo[warehouseID] = info
	}

	// Analyze usage records to determine activity patterns
	for _, record := range records {
		warehouseID := record.Resource.Name
		if info, exists := staleResourcesInfo[warehouseID]; exists {
			info.HasActivity = true
			info.QueryCount++ // Each record represents some query activity

			// Track the most recent activity time
			if info.LastActivityTime == nil || record.EndTime.After(*info.LastActivityTime) {
				info.LastActivityTime = &record.EndTime
			}

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

	// Calculate stale resource metrics
	now := time.Now()
	for _, info := range staleResourcesInfo {
		// Calculate days since last activity
		if info.LastActivityTime != nil {
			info.DaysSinceActivity = int(now.Sub(*info.LastActivityTime).Hours() / 24)
		} else {
			// No activity recorded in the analysis period
			info.DaysSinceActivity = int(now.Sub(startTime).Hours() / 24)
		}

		// Determine if warehouse is stale (no activity in the last N days)
		info.IsStale = info.DaysSinceActivity >= settings.StaleResourceDays

		// Determine if warehouse is orphaned (created but never used)
		info.IsOrphaned = !info.HasActivity

		// Determine if warehouse was never started (no usage records at all)
		info.NeverStarted = info.QueryCount == 0 && !info.HasActivity
	}

	return staleResourcesInfo, nil
}

// generateStaleResourcesFindings creates audit findings for stale and orphaned warehouses
func generateStaleResourcesFindings(staleResourcesInfo map[string]*WarehouseStaleResourceInfo, settings WarehouseAuditSettings) []domain.AuditFinding {
	var findings []domain.AuditFinding

	for warehouseID, info := range staleResourcesInfo {
		// Generate finding for stale warehouses (no activity in 30+ days)
		if info.IsStale && info.HasActivity {
			description := fmt.Sprintf("Warehouse has not been active for %d days", info.DaysSinceActivity)
			if info.LastActivityTime != nil {
				description += fmt.Sprintf(" (last activity: %s)", info.LastActivityTime.Format("2006-01-02"))
			}
			if info.TotalCost > 0 {
				description += fmt.Sprintf(", total cost in analysis period: %.2f %s", info.TotalCost, info.Currency)
			}

			findings = append(findings, domain.AuditFinding{
				Id: fmt.Sprintf("%s_stale_warehouse", warehouseID),
				Resource: domain.ResourceDef{
					Platform: "Databricks",
					Service:  "warehouse",
					Name:     warehouseID,
				},
				Issue:          "stale_warehouse",
				Description:    description,
				Recommendation: "Review if this warehouse is still needed. Consider deleting unused warehouses to reduce management overhead and potential costs.",
				Severity:       domain.SeverityMedium,
			})
		}

		// Generate finding for orphaned warehouses (created but never used)
		if info.IsOrphaned {
			description := "Warehouse exists but has no recorded usage activity in the analysis period"
			if info.DaysSinceActivity > 0 {
				description += fmt.Sprintf(" (no activity for %d+ days)", info.DaysSinceActivity)
			}

			findings = append(findings, domain.AuditFinding{
				Id: fmt.Sprintf("%s_orphaned_warehouse", warehouseID),
				Resource: domain.ResourceDef{
					Platform: "Databricks",
					Service:  "warehouse",
					Name:     warehouseID,
				},
				Issue:          "orphaned_warehouse",
				Description:    description,
				Recommendation: "Investigate if this warehouse was created for a specific purpose that hasn't been implemented yet, or if it can be safely deleted.",
				Severity:       domain.SeverityHigh,
			})
		}

		// Generate finding for warehouses with zero query activity
		if info.QueryCount == 0 && info.HasActivity {
			description := "Warehouse has usage records but no query executions detected"
			if info.UsageHours > 0 {
				description += fmt.Sprintf(" (%.1f hours of runtime without queries)", info.UsageHours)
			}

			findings = append(findings, domain.AuditFinding{
				Id: fmt.Sprintf("%s_zero_query_activity", warehouseID),
				Resource: domain.ResourceDef{
					Platform: "Databricks",
					Service:  "warehouse",
					Name:     warehouseID,
				},
				Issue:          "zero_query_activity",
				Description:    description,
				Recommendation: "Review warehouse configuration and usage patterns. Warehouses running without executing queries may indicate misconfiguration or inefficient usage.",
				Severity:       domain.SeverityMedium,
			})
		}

		// Generate finding for warehouses that were created but never started
		if info.NeverStarted {
			description := "Warehouse was created but has never been started or used"

			findings = append(findings, domain.AuditFinding{
				Id: fmt.Sprintf("%s_never_started", warehouseID),
				Resource: domain.ResourceDef{
					Platform: "Databricks",
					Service:  "warehouse",
					Name:     warehouseID,
				},
				Issue:          "never_started",
				Description:    description,
				Recommendation: "Consider deleting this warehouse if it's not needed, or investigate why it was created but never used.",
				Severity:       domain.SeverityHigh,
			})
		}
	}

	return findings
}

// updateStaleResourcesSummary updates the audit report summary with stale resources analysis results
func updateStaleResourcesSummary(report *domain.AuditReport, staleResourcesInfo map[string]*WarehouseStaleResourceInfo, settings WarehouseAuditSettings) {
	staleWarehousesCount := 0
	orphanedWarehousesCount := 0
	neverStartedCount := 0
	zeroQueryActivityCount := 0
	totalStaleResourceCost := 0.0
	var currency string

	for _, info := range staleResourcesInfo {
		if info.IsStale {
			staleWarehousesCount++
			totalStaleResourceCost += info.TotalCost
			if currency == "" {
				currency = info.Currency
			}
		}

		if info.IsOrphaned {
			orphanedWarehousesCount++
		}

		if info.NeverStarted {
			neverStartedCount++
		}

		if info.QueryCount == 0 && info.HasActivity {
			zeroQueryActivityCount++
		}
	}

	report.Summary["stale_warehouses_count"] = staleWarehousesCount
	report.Summary["orphaned_warehouses_count"] = orphanedWarehousesCount
	report.Summary["never_started_warehouses_count"] = neverStartedCount
	report.Summary["zero_query_activity_count"] = zeroQueryActivityCount
	report.Summary["total_stale_resource_cost"] = totalStaleResourceCost

	if currency != "" {
		report.Summary["stale_resource_currency"] = currency
	}

	// Calculate potential savings from stale resources
	if totalStaleResourceCost > 0 {
		// Estimate potential monthly savings if stale resources were removed
		// This is a rough estimate based on the analysis period
		analysisPeriodDays := report.Period.Duration
		if analysisPeriodDays > 0 {
			monthlySavings := (totalStaleResourceCost / float64(analysisPeriodDays)) * 30
			report.Summary["potential_monthly_savings_from_stale_resources"] = monthlySavings
		}
	}
}

// WarehouseProvisioningInfo represents provisioning analysis information for a warehouse
type WarehouseProvisioningInfo struct {
	WarehouseID            string
	Name                   string
	Size                   string
	NodeCount              int
	MaxClusters            int
	QueryCount             int
	TotalCost              float64
	UsageHours             float64
	Currency               string
	AvgResourceUtilization float64
	QueryComplexityScore   float64
	ProvisioningScore      float64 // Higher score = more over-provisioned
	IsOverProvisioned      bool
	IsUnderProvisioned     bool
	RecommendedSize        string
	PotentialSavings       float64
}

// analyzeProvisioningPatterns analyzes correlation between query complexity and warehouse size
func analyzeProvisioningPatterns(ctx context.Context, records []domain.ResourceCost, explorer Explorer, settings WarehouseAuditSettings) (map[string]*WarehouseProvisioningInfo, error) {
	provisioningInfo := make(map[string]*WarehouseProvisioningInfo)

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

		info := &WarehouseProvisioningInfo{
			WarehouseID: warehouseID,
			Name:        metadata.Name,
			Size:        metadata.Size,
			MaxClusters: metadata.MaxNumClusters,
			NodeCount:   calculateNodeCount(metadata.Size, metadata.MaxNumClusters),
		}

		provisioningInfo[warehouseID] = info
	}

	// Analyze usage patterns from records
	for _, record := range records {
		warehouseID := record.Resource.Name
		if info, exists := provisioningInfo[warehouseID]; exists {
			info.QueryCount++

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

			// Estimate query complexity based on cost patterns and duration
			queryComplexity := estimateQueryComplexity(record, usageHours)
			info.QueryComplexityScore += queryComplexity
		}
	}

	// Calculate provisioning metrics for each warehouse
	for _, info := range provisioningInfo {
		if info.QueryCount > 0 {
			// Average query complexity
			info.QueryComplexityScore = info.QueryComplexityScore / float64(info.QueryCount)

			// Calculate average resource utilization
			info.AvgResourceUtilization = calculateResourceUtilization(info)

			// Calculate provisioning score (mismatch between size and complexity)
			info.ProvisioningScore = calculateProvisioningScore(info)

			// Determine if warehouse is over/under-provisioned
			analyzeProvisioningMismatch(info, settings)

			// Generate size recommendations
			info.RecommendedSize = recommendWarehouseSize(info)

			// Estimate potential savings
			info.PotentialSavings = estimatePotentialSavings(info)
		}
	}

	return provisioningInfo, nil
}

// estimateQueryComplexity estimates query complexity based on cost patterns and execution time
func estimateQueryComplexity(record domain.ResourceCost, usageHours float64) float64 {
	if len(record.Costs) == 0 || usageHours == 0 {
		return 0
	}

	totalCost := 0.0
	for _, cost := range record.Costs {
		totalCost += cost.TotalAmount
	}

	// Calculate cost per hour as a proxy for query complexity
	costPerHour := totalCost / usageHours

	// Normalize complexity score (higher cost per hour = more complex queries)
	// This is a simplified heuristic - in practice, you'd analyze actual query metrics
	complexityScore := costPerHour * 10 // Scale factor to get reasonable scores

	// Cap the complexity score to prevent outliers
	if complexityScore > 100 {
		complexityScore = 100
	}

	return complexityScore
}

// calculateResourceUtilization calculates average resource utilization based on cost efficiency
func calculateResourceUtilization(info *WarehouseProvisioningInfo) float64 {
	if info.UsageHours == 0 || info.TotalCost == 0 {
		return 0
	}

	// Calculate cost per hour
	costPerHour := info.TotalCost / info.UsageHours

	// Estimate utilization based on warehouse size and cost efficiency
	// Larger warehouses should have higher cost per hour when fully utilized
	expectedCostPerHour := getExpectedCostPerHour(info.Size)

	if expectedCostPerHour == 0 {
		return 0
	}

	// Utilization = actual cost per hour / expected cost per hour for full utilization
	utilization := costPerHour / expectedCostPerHour

	// Cap utilization at 100%
	if utilization > 1.0 {
		utilization = 1.0
	}

	return utilization
}

// getExpectedCostPerHour returns the expected cost per hour for a warehouse size at full utilization
func getExpectedCostPerHour(size string) float64 {
	// These are rough estimates based on typical Databricks pricing
	// In practice, you'd use actual pricing data from your environment
	switch size {
	case "2X-Small":
		return 2.0
	case "X-Small":
		return 4.0
	case "Small":
		return 8.0
	case "Medium":
		return 16.0
	case "Large":
		return 32.0
	case "X-Large":
		return 64.0
	case "2X-Large":
		return 128.0
	case "3X-Large":
		return 256.0
	case "4X-Large":
		return 512.0
	default:
		return 8.0 // Default to Small
	}
}

// calculateProvisioningScore calculates a score indicating provisioning mismatch
func calculateProvisioningScore(info *WarehouseProvisioningInfo) float64 {
	// Score based on the ratio of warehouse size to query complexity
	warehouseSizeScore := getWarehouseSizeScore(info.Size)

	if info.QueryComplexityScore == 0 {
		// If no query complexity detected, large warehouses are likely over-provisioned
		return warehouseSizeScore
	}

	// Higher score = more over-provisioned (large warehouse, simple queries)
	// Lower score = potentially under-provisioned (small warehouse, complex queries)
	provisioningScore := warehouseSizeScore / (info.QueryComplexityScore + 1)

	return provisioningScore
}

// getWarehouseSizeScore returns a numeric score for warehouse size
func getWarehouseSizeScore(size string) float64 {
	switch size {
	case "2X-Small":
		return 1.0
	case "X-Small":
		return 2.0
	case "Small":
		return 4.0
	case "Medium":
		return 8.0
	case "Large":
		return 16.0
	case "X-Large":
		return 32.0
	case "2X-Large":
		return 64.0
	case "3X-Large":
		return 128.0
	case "4X-Large":
		return 256.0
	default:
		return 4.0 // Default to Small
	}
}

// analyzeProvisioningMismatch determines if a warehouse is over or under-provisioned
func analyzeProvisioningMismatch(info *WarehouseProvisioningInfo, settings WarehouseAuditSettings) {
	// Only analyze warehouses with sufficient query activity
	if info.QueryCount < settings.MinQueryCountThreshold {
		return
	}

	// Over-provisioned: high provisioning score (large warehouse, simple queries)
	if info.ProvisioningScore > 10.0 && info.AvgResourceUtilization < 0.3 {
		info.IsOverProvisioned = true
	}

	// Under-provisioned: low provisioning score with high complexity and utilization
	if info.ProvisioningScore < 0.5 && info.QueryComplexityScore > 50 && info.AvgResourceUtilization > 0.8 {
		info.IsUnderProvisioned = true
	}
}

// recommendWarehouseSize suggests an appropriate warehouse size based on usage patterns
func recommendWarehouseSize(info *WarehouseProvisioningInfo) string {
	// If warehouse is appropriately sized, return current size
	if !info.IsOverProvisioned && !info.IsUnderProvisioned {
		return info.Size
	}

	currentSizeScore := getWarehouseSizeScore(info.Size)

	if info.IsOverProvisioned {
		// Recommend smaller size based on query complexity
		targetScore := info.QueryComplexityScore / 10
		if targetScore < 1 {
			targetScore = 1
		}
		return getSizeFromScore(targetScore)
	}

	if info.IsUnderProvisioned {
		// Recommend larger size based on utilization and complexity
		targetScore := currentSizeScore * 2
		if targetScore > 256 {
			targetScore = 256
		}
		return getSizeFromScore(targetScore)
	}

	return info.Size
}

// getSizeFromScore returns the warehouse size closest to the given score
func getSizeFromScore(score float64) string {
	if score <= 1.5 {
		return "2X-Small"
	} else if score <= 3.0 {
		return "X-Small"
	} else if score <= 6.0 {
		return "Small"
	} else if score <= 12.0 {
		return "Medium"
	} else if score <= 24.0 {
		return "Large"
	} else if score <= 48.0 {
		return "X-Large"
	} else if score <= 96.0 {
		return "2X-Large"
	} else if score <= 192.0 {
		return "3X-Large"
	} else {
		return "4X-Large"
	}
}

// estimatePotentialSavings calculates potential cost savings from right-sizing
func estimatePotentialSavings(info *WarehouseProvisioningInfo) float64 {
	if !info.IsOverProvisioned || info.RecommendedSize == info.Size {
		return 0
	}

	currentSizeScore := getWarehouseSizeScore(info.Size)
	recommendedSizeScore := getWarehouseSizeScore(info.RecommendedSize)

	// Estimate savings as the cost reduction from smaller warehouse
	if recommendedSizeScore < currentSizeScore && info.TotalCost > 0 {
		savingsRatio := (currentSizeScore - recommendedSizeScore) / currentSizeScore
		return info.TotalCost * savingsRatio
	}

	return 0
}

// generateProvisioningFindings creates audit findings for provisioning mismatches
func generateProvisioningFindings(provisioningInfo map[string]*WarehouseProvisioningInfo, settings WarehouseAuditSettings) []domain.AuditFinding {
	var findings []domain.AuditFinding

	for warehouseID, info := range provisioningInfo {
		// Skip warehouses with insufficient query activity
		if info.QueryCount < settings.MinQueryCountThreshold {
			continue
		}

		// Generate finding for over-provisioned warehouses
		if info.IsOverProvisioned {
			description := fmt.Sprintf("Warehouse appears over-provisioned: %s size with %.1f average query complexity score and %.0f%% resource utilization",
				info.Size, info.QueryComplexityScore, info.AvgResourceUtilization*100)

			if info.QueryCount > 0 {
				description += fmt.Sprintf(" (%d queries analyzed)", info.QueryCount)
			}

			recommendation := fmt.Sprintf("Consider downsizing to %s to optimize costs", info.RecommendedSize)
			if info.PotentialSavings > 0 {
				recommendation += fmt.Sprintf(". Potential savings: %.2f %s", info.PotentialSavings, info.Currency)
			}

			findings = append(findings, domain.AuditFinding{
				Id: fmt.Sprintf("%s_over_provisioned", warehouseID),
				Resource: domain.ResourceDef{
					Platform: "Databricks",
					Service:  "warehouse",
					Name:     warehouseID,
				},
				Issue:          "over_provisioned",
				Description:    description,
				Recommendation: recommendation,
				Severity:       domain.SeverityMedium,
			})
		}

		// Generate finding for under-provisioned warehouses
		if info.IsUnderProvisioned {
			description := fmt.Sprintf("Warehouse may be under-provisioned: %s size with %.1f query complexity score and %.0f%% resource utilization",
				info.Size, info.QueryComplexityScore, info.AvgResourceUtilization*100)

			if info.QueryCount > 0 {
				description += fmt.Sprintf(" (%d queries analyzed)", info.QueryCount)
			}

			recommendation := fmt.Sprintf("Consider upgrading to %s to improve performance for complex workloads", info.RecommendedSize)

			findings = append(findings, domain.AuditFinding{
				Id: fmt.Sprintf("%s_under_provisioned", warehouseID),
				Resource: domain.ResourceDef{
					Platform: "Databricks",
					Service:  "warehouse",
					Name:     warehouseID,
				},
				Issue:          "under_provisioned",
				Description:    description,
				Recommendation: recommendation,
				Severity:       domain.SeverityMedium,
			})
		}

		// Generate finding for warehouses running simple queries on large clusters
		if info.QueryComplexityScore < 20 && getWarehouseSizeScore(info.Size) >= 16 { // Large or bigger
			description := fmt.Sprintf("Large warehouse (%s) running simple queries (complexity score: %.1f)",
				info.Size, info.QueryComplexityScore)

			if info.AvgResourceUtilization < 0.5 {
				description += fmt.Sprintf(" with low utilization (%.0f%%)", info.AvgResourceUtilization*100)
			}

			findings = append(findings, domain.AuditFinding{
				Id: fmt.Sprintf("%s_simple_queries_large_cluster", warehouseID),
				Resource: domain.ResourceDef{
					Platform: "Databricks",
					Service:  "warehouse",
					Name:     warehouseID,
				},
				Issue:          "simple_queries_large_cluster",
				Description:    description,
				Recommendation: "Consider using a smaller warehouse for simple queries, or consolidate complex workloads to justify the large cluster size.",
				Severity:       domain.SeverityMedium,
			})
		}
	}

	return findings
}

// updateProvisioningSummary updates the audit report summary with provisioning analysis results
func updateProvisioningSummary(report *domain.AuditReport, provisioningInfo map[string]*WarehouseProvisioningInfo, settings WarehouseAuditSettings) {
	overProvisionedCount := 0
	underProvisionedCount := 0
	totalPotentialSavings := 0.0
	totalResourceUtilization := 0.0
	warehousesAnalyzed := 0
	var currency string

	for _, info := range provisioningInfo {
		// Only count warehouses with sufficient query activity
		if info.QueryCount >= settings.MinQueryCountThreshold {
			warehousesAnalyzed++
			totalResourceUtilization += info.AvgResourceUtilization

			if info.IsOverProvisioned {
				overProvisionedCount++
				totalPotentialSavings += info.PotentialSavings
				if currency == "" {
					currency = info.Currency
				}
			}

			if info.IsUnderProvisioned {
				underProvisionedCount++
			}
		}
	}

	report.Summary["over_provisioned_count"] = overProvisionedCount
	report.Summary["under_provisioned_count"] = underProvisionedCount
	report.Summary["warehouses_analyzed_for_provisioning"] = warehousesAnalyzed

	if warehousesAnalyzed > 0 {
		avgUtilization := totalResourceUtilization / float64(warehousesAnalyzed)
		report.Summary["average_resource_utilization"] = fmt.Sprintf("%.1f%%", avgUtilization*100)
	}

	if totalPotentialSavings > 0 {
		report.Summary["potential_savings_from_rightsizing"] = totalPotentialSavings
		if currency != "" {
			report.Summary["potential_savings_currency"] = currency
		}
	}
}
