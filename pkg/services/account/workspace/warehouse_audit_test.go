package workspace

import (
	"context"
	"testing"
	"time"

	"github.com/de-tools/data-atlas/pkg/models/domain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// MockCostManager is a mock implementation of CostManager for testing
type MockCostManager struct {
	mock.Mock
}

func (m *MockCostManager) GetResourcesCost(ctx context.Context, res domain.WorkspaceResources, startTime, endTime time.Time) ([]domain.ResourceCost, error) {
	args := m.Called(ctx, res, startTime, endTime)
	return args.Get(0).([]domain.ResourceCost), args.Error(1)
}

func (m *MockCostManager) GetUsageStats(ctx context.Context, startTime *time.Time) (*domain.UsageStats, error) {
	args := m.Called(ctx, startTime)
	return args.Get(0).(*domain.UsageStats), args.Error(1)
}

func (m *MockCostManager) GetUsage(ctx context.Context, startTime, endTime time.Time) ([]domain.ResourceCost, error) {
	args := m.Called(ctx, startTime, endTime)
	return args.Get(0).([]domain.ResourceCost), args.Error(1)
}

func TestGetWarehouseAudit_RuntimeDurationAnalysis(t *testing.T) {
	ctx := context.Background()
	ws := domain.Workspace{Name: "test-workspace"}
	startTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	endTime := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)
	settings := DefaultWarehouseAuditSettings()

	t.Run("successful runtime analysis with findings", func(t *testing.T) {
		mockCostManager := new(MockCostManager)

		// Mock warehouse usage records with excessive runtime and idle time
		records := []domain.ResourceCost{
			{
				ID:        "record1",
				StartTime: startTime,
				EndTime:   startTime.Add(10 * time.Hour), // 10 hours runtime (exceeds 8h threshold)
				Resource: domain.ResourceDef{
					Platform: "Databricks",
					Service:  "warehouse",
					Name:     "warehouse-1",
				},
				Costs: []domain.CostComponent{
					{
						Type:        "compute",
						Value:       1.0,
						Unit:        "hours",
						TotalAmount: 0.5, // Low cost suggests idle time
						Rate:        0.05,
						Currency:    "USD",
					},
				},
			},
			{
				ID:        "record2",
				StartTime: startTime.Add(12 * time.Hour),
				EndTime:   startTime.Add(15 * time.Hour), // 3 hours runtime
				Resource: domain.ResourceDef{
					Platform: "Databricks",
					Service:  "warehouse",
					Name:     "warehouse-2",
				},
				Costs: []domain.CostComponent{
					{
						Type:        "compute",
						Value:       1.0,
						Unit:        "hours",
						TotalAmount: 15.0, // High cost suggests active usage
						Rate:        5.0,
						Currency:    "USD",
					},
				},
			},
		}

		mockCostManager.On("GetResourcesCost", ctx, mock.AnythingOfType("domain.WorkspaceResources"), startTime, endTime).Return(records, nil)

		report, err := GetWarehouseAudit(ctx, ws, startTime, endTime, mockCostManager, settings)

		assert.NoError(t, err)
		assert.Equal(t, "test-workspace", report.Workspace)
		assert.Equal(t, "warehouse", report.ResourceType)
		assert.Equal(t, 2, report.Summary["warehouses_evaluated"])
		assert.Greater(t, report.Summary["warehouses_with_runtime_issues"], 0)
		assert.Greater(t, len(report.Findings), 0)

		// Check for excessive runtime finding
		hasExcessiveRuntimeFinding := false
		for _, finding := range report.Findings {
			if finding.Issue == "excessive_runtime" && finding.Resource.Name == "warehouse-1" {
				hasExcessiveRuntimeFinding = true
				assert.Contains(t, finding.Description, "10.0 hours")
				break
			}
		}
		assert.True(t, hasExcessiveRuntimeFinding, "Should have excessive runtime finding for warehouse-1")

		mockCostManager.AssertExpectations(t)
	})

	t.Run("no warehouse activity", func(t *testing.T) {
		mockCostManager := new(MockCostManager)

		// Mock empty records
		records := []domain.ResourceCost{}
		mockCostManager.On("GetResourcesCost", ctx, mock.AnythingOfType("domain.WorkspaceResources"), startTime, endTime).Return(records, nil)

		report, err := GetWarehouseAudit(ctx, ws, startTime, endTime, mockCostManager, settings)

		assert.NoError(t, err)
		assert.Equal(t, "No warehouse usage records found in the selected period", report.Summary["no_activity"])
		assert.Len(t, report.Findings, 1)
		assert.Equal(t, "no_activity", report.Findings[0].Issue)

		mockCostManager.AssertExpectations(t)
	})
}

func TestAnalyzeWarehouseRuntimeDuration(t *testing.T) {
	startTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	settings := DefaultWarehouseAuditSettings()

	t.Run("aggregates warehouse usage correctly", func(t *testing.T) {
		records := []domain.ResourceCost{
			{
				StartTime: startTime,
				EndTime:   startTime.Add(5 * time.Hour),
				Resource: domain.ResourceDef{
					Name: "warehouse-1",
				},
				Costs: []domain.CostComponent{
					{Currency: "USD", TotalAmount: 10.0},
				},
			},
			{
				StartTime: startTime.Add(6 * time.Hour),
				EndTime:   startTime.Add(8 * time.Hour),
				Resource: domain.ResourceDef{
					Name: "warehouse-1",
				},
				Costs: []domain.CostComponent{
					{Currency: "USD", TotalAmount: 5.0},
				},
			},
		}

		stats := analyzeWarehouseRuntimeDuration(records, settings)

		assert.Len(t, stats, 1)
		assert.Contains(t, stats, "warehouse-1")

		warehouseStats := stats["warehouse-1"]
		assert.Equal(t, "warehouse-1", warehouseStats.WarehouseID)
		assert.Equal(t, 7.0, warehouseStats.TotalRuntimeHours) // 5 + 2 hours
		assert.Equal(t, 2, warehouseStats.RecordCount)
		assert.Equal(t, "USD", warehouseStats.Currency)
	})
}

func TestGenerateRuntimeFindings(t *testing.T) {
	settings := DefaultWarehouseAuditSettings()

	t.Run("generates findings for runtime violations", func(t *testing.T) {
		warehouseStats := map[string]*WarehouseRuntimeStats{
			"warehouse-excessive": {
				WarehouseID:       "warehouse-excessive",
				TotalRuntimeHours: 12.0, // Exceeds 8h threshold
				TotalIdleHours:    1.0,
				IdleTimePercent:   0.08,
			},
			"warehouse-idle": {
				WarehouseID:       "warehouse-idle",
				TotalRuntimeHours: 6.0,
				TotalIdleHours:    4.0,  // Exceeds 2h threshold
				IdleTimePercent:   0.67, // Exceeds 50% threshold
			},
			"warehouse-good": {
				WarehouseID:       "warehouse-good",
				TotalRuntimeHours: 4.0,
				TotalIdleHours:    1.0,
				IdleTimePercent:   0.25,
			},
		}

		findings := generateRuntimeFindings(warehouseStats, settings)

		// Should have findings for excessive runtime, high idle time, and idle workload
		assert.GreaterOrEqual(t, len(findings), 3)

		// Check for specific findings
		issueTypes := make(map[string]bool)
		for _, finding := range findings {
			issueTypes[finding.Issue] = true
		}

		assert.True(t, issueTypes["excessive_runtime"])
		assert.True(t, issueTypes["high_idle_time"])
		assert.True(t, issueTypes["idle_workload"])
	})
}
