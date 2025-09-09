package workspace

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/de-tools/data-atlas/pkg/models/domain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type mockCostManager struct{ mock.Mock }

func (m *mockCostManager) GetResourcesCost(
	ctx context.Context,
	res domain.WorkspaceResources,
	startTime, endTime time.Time,
) ([]domain.ResourceCost, error) {
	args := m.Called(ctx, res, startTime, endTime)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]domain.ResourceCost), args.Error(1)
}
func (m *mockCostManager) GetUsageStats(ctx context.Context, startTime *time.Time) (*domain.UsageStats, error) {
	return nil, nil
}
func (m *mockCostManager) GetUsage(ctx context.Context, startTime, endTime time.Time) ([]domain.ResourceCost, error) {
	return nil, nil
}

func TestGetDLTAudit_NoRecords(t *testing.T) {
	ctx := context.Background()
	ws := domain.Workspace{Name: "ws1"}
	start := time.Date(2025, 9, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2025, 9, 8, 0, 0, 0, 0, time.UTC)

	cm := new(mockCostManager)
	cm.On("GetResourcesCost", mock.Anything, domain.WorkspaceResources{WorkspaceName: ws.Name, Resources: []string{"dlt_pipeline", "dlt_update", "dlt_maintenance"}}, start, end).
		Return([]domain.ResourceCost{}, nil)

	settings := DLTAuditSettings{MaintenanceRatioThreshold: 0.3, MinMaintenanceEvents: 3, LongRunAvgSecondsThreshold: 2 * 3600}
	report, err := GetDLTAudit(ctx, ws, start, end, cm, settings)
	assert.NoError(t, err)
	assert.Equal(t, ws.Name, report.Workspace)
	assert.Equal(t, "dlt_pipeline", report.ResourceType)
	assert.Equal(t, 1, len(report.Findings))
	assert.Equal(t, "no_activity", report.Findings[0].Issue)
	assert.Equal(t, domain.SeverityMedium, report.Findings[0].Severity)
	assert.Equal(t, "Databricks", report.Findings[0].Resource.Platform)
	assert.Equal(t, "dlt_pipeline", report.Findings[0].Resource.Service)
	assert.Equal(t, "workspace", report.Findings[0].Resource.Name)
	assert.Equal(t, "No DLT usage records found in the selected period", report.Summary["no_activity"])
	cm.AssertExpectations(t)
}

func TestGetDLTAudit_MixedPipelines_FindingsAndSummary(t *testing.T) {
	ctx := context.Background()
	ws := domain.Workspace{Name: "ws1"}
	start := time.Date(2025, 9, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2025, 9, 8, 0, 0, 0, 0, time.UTC)

	pidA := "pipeline-a"
	pidB := "pipeline-b"

	records := []domain.ResourceCost{
		// Pipeline A: 2 updates (1h each), 3 maintenance (0.5h each) => total 3.5h, avgRun = 3.5h/2 = 1.75h (<2h)
		{
			StartTime: start,
			EndTime:   start.Add(1 * time.Hour),
			Resource:  domain.ResourceDef{Platform: "Databricks", Service: "dlt_update", Name: pidA},
			Costs:     []domain.CostComponent{{Currency: "USD", TotalAmount: 1, Value: 1}},
		},
		{
			StartTime: start.Add(2 * time.Hour),
			EndTime:   start.Add(3 * time.Hour),
			Resource:  domain.ResourceDef{Platform: "Databricks", Service: "dlt_update", Name: pidA},
			Costs:     []domain.CostComponent{{Currency: "USD", TotalAmount: 1, Value: 1}},
		},
		{
			StartTime: start.Add(4 * time.Hour),
			EndTime:   start.Add(4*time.Hour + 30*time.Minute),
			Resource:  domain.ResourceDef{Platform: "Databricks", Service: "dlt_maintenance", Name: pidA},
			Costs:     []domain.CostComponent{{Currency: "USD", TotalAmount: 0.5, Value: 0.5}},
		},
		{
			StartTime: start.Add(5 * time.Hour),
			EndTime:   start.Add(5*time.Hour + 30*time.Minute),
			Resource:  domain.ResourceDef{Platform: "Databricks", Service: "dlt_maintenance", Name: pidA},
			Costs:     []domain.CostComponent{{Currency: "USD", TotalAmount: 0.5, Value: 0.5}},
		},
		{
			StartTime: start.Add(6 * time.Hour),
			EndTime:   start.Add(6*time.Hour + 30*time.Minute),
			Resource:  domain.ResourceDef{Platform: "Databricks", Service: "dlt_maintenance", Name: pidA},
			Costs:     []domain.CostComponent{{Currency: "USD", TotalAmount: 0.5, Value: 0.5}},
		},
		// Pipeline B: 1 update of 3h (max 3h) => avgRun 3h > 2h, long running
		{
			StartTime: start.Add(24 * time.Hour),
			EndTime:   start.Add(27 * time.Hour),
			Resource:  domain.ResourceDef{Platform: "Databricks", Service: "dlt_update", Name: pidB},
			Costs:     []domain.CostComponent{{Currency: "USD", TotalAmount: 3, Value: 3}},
		},
	}

	cm := new(mockCostManager)
	cm.On("GetResourcesCost", mock.Anything, domain.WorkspaceResources{WorkspaceName: ws.Name, Resources: []string{"dlt_pipeline", "dlt_update", "dlt_maintenance"}}, start, end).
		Return(records, nil)

	settings := DLTAuditSettings{MaintenanceRatioThreshold: 0.3, MinMaintenanceEvents: 3, LongRunAvgSecondsThreshold: 2 * 3600}
	report, err := GetDLTAudit(ctx, ws, start, end, cm, settings)
	assert.NoError(t, err)

	// Summary checks
	assert.Equal(t, 2, report.Summary["pipelines_evaluated"])
	assert.Equal(t, 1, report.Summary["pipelines_with_high_maintenance_overhead"])
	assert.Equal(t, 1, report.Summary["pipelines_with_long_running_updates"])

	// Findings checks: should contain one for pidA maintenance_overhead and one for pidB long_running_updates
	issuesByPipeline := map[string]map[string]bool{}
	for _, f := range report.Findings {
		if _, ok := issuesByPipeline[f.Resource.Name]; !ok {
			issuesByPipeline[f.Resource.Name] = map[string]bool{}
		}
		issuesByPipeline[f.Resource.Name][f.Issue] = true
	}

	assert.True(t, issuesByPipeline[pidA]["maintenance_overhead"], "expected maintenance_overhead for pipeline A")
	assert.True(t, issuesByPipeline[pidB]["long_running_updates"], "expected long_running_updates for pipeline B")

	// Check long run description mentions hours
	var longRunDesc string
	for _, f := range report.Findings {
		if f.Issue == "long_running_updates" && f.Resource.Name == pidB {
			longRunDesc = f.Description
		}
	}
	assert.NotEmpty(t, longRunDesc)
	assert.Contains(t, longRunDesc, "hours")

	cm.AssertExpectations(t)
}

func TestGetDLTAudit_CostManagerError(t *testing.T) {
	ctx := context.Background()
	ws := domain.Workspace{Name: "ws1"}
	start := time.Date(2025, 9, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2025, 9, 8, 0, 0, 0, 0, time.UTC)

	cm := new(mockCostManager)
	errExpected := fmt.Errorf("error")
	cm.On("GetResourcesCost", mock.Anything, domain.WorkspaceResources{WorkspaceName: ws.Name, Resources: []string{"dlt_pipeline", "dlt_update", "dlt_maintenance"}}, start, end).
		Return(nil, errExpected)

	settings := DLTAuditSettings{MaintenanceRatioThreshold: 0.3, MinMaintenanceEvents: 3, LongRunAvgSecondsThreshold: 2 * 3600}
	_, err := GetDLTAudit(ctx, ws, start, end, cm, settings)
	assert.ErrorIs(t, err, errExpected)
	cm.AssertExpectations(t)
}
