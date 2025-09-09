package workspace

import (
	"context"
	"fmt"
	"time"

	"github.com/de-tools/data-atlas/pkg/models/domain"
)

// DLTAuditSettings holds configurable thresholds for DLT audit findings.
type DLTAuditSettings struct {
	// MaintenanceRatioThreshold is the minimum share of maintenance events (0..1) to flag overhead.
	MaintenanceRatioThreshold float64

	// MinMaintenanceEvents is the minimum count of maintenance events to consider for overhead findings.
	MinMaintenanceEvents int

	// LongRunAvgSecondsThreshold is the average update duration in seconds above which updates are flagged as long-running.
	LongRunAvgSecondsThreshold float64
}

// GetDLTAudit computes DLT pipelines audit for a workspace over the period.
func GetDLTAudit(
	ctx context.Context,
	ws domain.Workspace,
	startTime, endTime time.Time,
	costManager CostManager,
	settings DLTAuditSettings,
) (domain.AuditReport, error) {
	// Collect DLT-related usage
	resources := domain.WorkspaceResources{WorkspaceName: ws.Name, Resources: []string{"dlt_pipeline", "dlt_update", "dlt_maintenance"}}
	records, err := costManager.GetResourcesCost(ctx, resources, startTime, endTime)
	if err != nil {
		return domain.AuditReport{}, err
	}

	// Aggregate per pipeline
	type agg struct {
		totalCost        float64
		totalUsage       float64
		updatesCount     int
		maintenanceCount int
		totalDurationSec float64
		maxDurationSec   float64
		currency         string
	}
	pipelines := map[string]*agg{}

	for _, rec := range records {
		id := rec.Resource.Name
		if _, ok := pipelines[id]; !ok {
			pipelines[id] = &agg{}
		}
		a := pipelines[id]
		for _, c := range rec.Costs {
			a.totalCost += c.TotalAmount
			a.totalUsage += c.Value
			if a.currency == "" {
				a.currency = c.Currency
			}
		}
		dur := rec.EndTime.Sub(rec.StartTime).Seconds()
		a.totalDurationSec += dur
		if dur > a.maxDurationSec {
			a.maxDurationSec = dur
		}
		rt := rec.Resource.Service
		if rt == "dlt_update" {
			a.updatesCount++
		}
		if rt == "dlt_maintenance" {
			a.maintenanceCount++
		}
	}

	report := domain.AuditReport{
		Workspace:    ws.Name,
		ResourceType: "dlt_pipeline",
		Period: domain.TimePeriod{
			Start:    startTime,
			End:      endTime,
			Duration: int(endTime.Sub(startTime).Hours() / 24),
		},
		Summary:  map[string]any{},
		Findings: []domain.AuditFinding{},
	}

	if len(records) == 0 {
		report.Summary["no_activity"] = "No DLT usage records found in the selected period"
		report.Findings = append(report.Findings, domain.AuditFinding{
			Id:             "no_activity",
			Resource:       domain.ResourceDef{Platform: "Databricks", Service: "dlt_pipeline", Name: "workspace"},
			Issue:          "no_activity",
			Description:    "No DLT pipeline activity detected in the selected time window.",
			Recommendation: "Verify schedules/triggers and pipeline health. Consider reducing provisioned resources if unused.",
			Severity:       domain.SeverityMedium,
		})
		return report, nil
	}

	var totalPipelines, highMaintenance, longRuns int

	for pid, a := range pipelines {
		totalPipelines++
		maintenanceRatio := 0.0
		totalEvents := a.updatesCount + a.maintenanceCount
		if totalEvents > 0 {
			maintenanceRatio = float64(a.maintenanceCount) / float64(totalEvents)
		}
		avgRunSec := 0.0
		runEvents := a.updatesCount
		if runEvents > 0 {
			avgRunSec = a.totalDurationSec / float64(runEvents)
		}

		if maintenanceRatio > settings.MaintenanceRatioThreshold && a.maintenanceCount >= settings.MinMaintenanceEvents {
			highMaintenance++
			report.Findings = append(report.Findings, domain.AuditFinding{
				Id:             fmt.Sprintf("%s_high_maintenance_overhead", pid),
				Resource:       domain.ResourceDef{Platform: "Databricks", Service: "dlt_pipeline", Name: pid},
				Issue:          "maintenance_overhead",
				Description:    fmt.Sprintf("Maintenance events constitute %.0f%% of pipeline activity.", maintenanceRatio*100),
				Recommendation: "Reduce pipeline maintenance frequency, consolidate tasks, or review autoscaling/pool warmups.",
				Severity:       domain.SeverityMedium,
			})
		}

		if avgRunSec > settings.LongRunAvgSecondsThreshold {
			longRuns++
			report.Findings = append(report.Findings, domain.AuditFinding{
				Id:             fmt.Sprintf("%s_long_running_updates", pid),
				Resource:       domain.ResourceDef{Platform: "Databricks", Service: "dlt_pipeline", Name: pid},
				Issue:          "long_running_updates",
				Description:    fmt.Sprintf("Average update duration is %.1f hours (max %.1f h).", avgRunSec/3600, a.maxDurationSec/3600),
				Recommendation: "Consider incremental model optimization, Z-ordering, optimized autoscaling, and proper cluster sizing.",
				Severity:       domain.SeverityMedium,
			})
		}
	}

	report.Summary["pipelines_evaluated"] = totalPipelines
	report.Summary["pipelines_with_high_maintenance_overhead"] = highMaintenance
	report.Summary["pipelines_with_long_running_updates"] = longRuns

	return report, nil
}
