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

// MockExplorer is a mock implementation of Explorer for testing
type MockExplorer struct {
	mock.Mock
}

func (m *MockExplorer) ListSupportedResources(ctx context.Context) ([]domain.WorkspaceResource, error) {
	args := m.Called(ctx)
	return args.Get(0).([]domain.WorkspaceResource), args.Error(1)
}

func (m *MockExplorer) GetWarehouseMetadata(ctx context.Context, warehouseID string) (*domain.WarehouseMetadata, error) {
	args := m.Called(ctx, warehouseID)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*domain.WarehouseMetadata), args.Error(1)
}

func (m *MockExplorer) ListWarehouses(ctx context.Context) ([]domain.WarehouseMetadata, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]domain.WarehouseMetadata), args.Error(1)
}

func TestGetWarehouseAudit_RuntimeDurationAnalysis(t *testing.T) {
	ctx := context.Background()
	ws := domain.Workspace{Name: "test-workspace"}
	startTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	endTime := time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC)
	settings := DefaultWarehouseAuditSettings()

	t.Run("successful runtime and size analysis with findings", func(t *testing.T) {
		mockCostManager := new(MockCostManager)
		mockExplorer := new(MockExplorer)

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

		// Mock warehouse metadata
		warehouse1Metadata := &domain.WarehouseMetadata{
			ID:               "warehouse-1",
			Name:             "Large Analytics Warehouse",
			Size:             "Large",
			MinNumClusters:   1,
			MaxNumClusters:   4,
			EnableServerless: false,
		}

		warehouse2Metadata := &domain.WarehouseMetadata{
			ID:               "warehouse-2",
			Name:             "Medium Dev Warehouse",
			Size:             "Medium",
			MinNumClusters:   1,
			MaxNumClusters:   2,
			EnableServerless: false,
		}

		mockCostManager.On("GetResourcesCost", ctx, mock.AnythingOfType("domain.WorkspaceResources"), startTime, endTime).Return(records, nil)
		mockExplorer.On("GetWarehouseMetadata", ctx, "warehouse-1").Return(warehouse1Metadata, nil)
		mockExplorer.On("GetWarehouseMetadata", ctx, "warehouse-2").Return(warehouse2Metadata, nil)

		report, err := GetWarehouseAudit(ctx, ws, startTime, endTime, mockCostManager, mockExplorer, settings)

		assert.NoError(t, err)
		assert.Equal(t, "test-workspace", report.Workspace)
		assert.Equal(t, "warehouse", report.ResourceType)
		assert.Equal(t, 2, report.Summary["warehouses_evaluated"])
		assert.Greater(t, report.Summary["warehouses_with_runtime_issues"], 0)
		assert.Greater(t, len(report.Findings), 0)

		// Check for excessive runtime finding and size findings
		hasExcessiveRuntimeFinding := false
		hasOversizedWarehouseFinding := false
		for _, finding := range report.Findings {
			if finding.Issue == "excessive_runtime" && finding.Resource.Name == "warehouse-1" {
				hasExcessiveRuntimeFinding = true
				assert.Contains(t, finding.Description, "10.0 hours")
			}
			if finding.Issue == "oversized_warehouse" {
				hasOversizedWarehouseFinding = true
				assert.Contains(t, finding.Description, "ranks #")
			}
		}
		assert.True(t, hasExcessiveRuntimeFinding, "Should have excessive runtime finding for warehouse-1")
		assert.True(t, hasOversizedWarehouseFinding, "Should have oversized warehouse findings")

		// Check size analysis summary fields
		assert.Contains(t, report.Summary, "warehouses_with_size_issues")
		assert.Contains(t, report.Summary, "total_cost_analyzed")

		mockCostManager.AssertExpectations(t)
		mockExplorer.AssertExpectations(t)
	})

	t.Run("no warehouse activity", func(t *testing.T) {
		mockCostManager := new(MockCostManager)
		mockExplorer := new(MockExplorer)

		// Mock empty records
		records := []domain.ResourceCost{}
		mockCostManager.On("GetResourcesCost", ctx, mock.AnythingOfType("domain.WorkspaceResources"), startTime, endTime).Return(records, nil)

		report, err := GetWarehouseAudit(ctx, ws, startTime, endTime, mockCostManager, mockExplorer, settings)

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

func TestAnalyzeWarehouseSizes(t *testing.T) {
	ctx := context.Background()
	startTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	settings := DefaultWarehouseAuditSettings()

	t.Run("analyzes warehouse sizes correctly", func(t *testing.T) {
		mockExplorer := new(MockExplorer)

		records := []domain.ResourceCost{
			{
				StartTime: startTime,
				EndTime:   startTime.Add(5 * time.Hour),
				Resource: domain.ResourceDef{
					Name: "warehouse-large",
				},
				Costs: []domain.CostComponent{
					{Currency: "USD", TotalAmount: 100.0},
				},
			},
			{
				StartTime: startTime.Add(6 * time.Hour),
				EndTime:   startTime.Add(8 * time.Hour),
				Resource: domain.ResourceDef{
					Name: "warehouse-small",
				},
				Costs: []domain.CostComponent{
					{Currency: "USD", TotalAmount: 10.0},
				},
			},
		}

		// Mock warehouse metadata
		largeMeta := &domain.WarehouseMetadata{
			ID:             "warehouse-large",
			Name:           "Large Warehouse",
			Size:           "Large",
			MaxNumClusters: 4,
		}

		smallMeta := &domain.WarehouseMetadata{
			ID:             "warehouse-small",
			Name:           "Small Warehouse",
			Size:           "Small",
			MaxNumClusters: 1,
		}

		mockExplorer.On("GetWarehouseMetadata", ctx, "warehouse-large").Return(largeMeta, nil)
		mockExplorer.On("GetWarehouseMetadata", ctx, "warehouse-small").Return(smallMeta, nil)

		sizeAnalysis, err := analyzeWarehouseSizes(ctx, records, mockExplorer, settings)

		assert.NoError(t, err)
		assert.Len(t, sizeAnalysis, 2)

		// Check large warehouse
		largeInfo := sizeAnalysis["warehouse-large"]
		assert.Equal(t, "warehouse-large", largeInfo.WarehouseID)
		assert.Equal(t, "Large Warehouse", largeInfo.Name)
		assert.Equal(t, "Large", largeInfo.Size)
		assert.Equal(t, 4, largeInfo.MaxClusters)
		assert.Equal(t, 100.0, largeInfo.TotalCost)
		assert.Greater(t, largeInfo.SizeScore, 0.0)

		// Check small warehouse
		smallInfo := sizeAnalysis["warehouse-small"]
		assert.Equal(t, "warehouse-small", smallInfo.WarehouseID)
		assert.Equal(t, "Small Warehouse", smallInfo.Name)
		assert.Equal(t, "Small", smallInfo.Size)
		assert.Equal(t, 1, smallInfo.MaxClusters)
		assert.Equal(t, 10.0, smallInfo.TotalCost)

		// Large warehouse should have higher size score
		assert.Greater(t, largeInfo.SizeScore, smallInfo.SizeScore)

		mockExplorer.AssertExpectations(t)
	})
}

func TestGenerateSizeFindings(t *testing.T) {
	settings := DefaultWarehouseAuditSettings()
	settings.TopLargestCount = 2 // Only flag top 2 largest

	t.Run("generates findings for largest warehouses", func(t *testing.T) {
		warehouseSizes := map[string]*WarehouseSizeInfo{
			"warehouse-huge": {
				WarehouseID: "warehouse-huge",
				Name:        "Huge Warehouse",
				Size:        "X-Large",
				MaxClusters: 8,
				NodeCount:   256, // 32 * 8
				SizeScore:   256.0,
				TotalCost:   1000.0,
				Currency:    "USD",
			},
			"warehouse-large": {
				WarehouseID: "warehouse-large",
				Name:        "Large Warehouse",
				Size:        "Large",
				MaxClusters: 4,
				NodeCount:   64, // 16 * 4
				SizeScore:   64.0,
				TotalCost:   500.0,
				Currency:    "USD",
			},
			"warehouse-small": {
				WarehouseID: "warehouse-small",
				Name:        "Small Warehouse",
				Size:        "Small",
				MaxClusters: 1,
				NodeCount:   4, // 4 * 1
				SizeScore:   4.0,
				TotalCost:   50.0,
				Currency:    "USD",
			},
		}

		findings := generateSizeFindings(warehouseSizes, settings)

		// Should have findings for top 2 largest warehouses
		assert.Len(t, findings, 2)

		// Check that all findings are for oversized warehouses
		for _, finding := range findings {
			assert.Equal(t, "oversized_warehouse", finding.Issue)
			assert.Contains(t, finding.Description, "ranks #")
			assert.Contains(t, finding.Description, "total cost:")
		}

		// Verify the largest warehouses are flagged
		flaggedWarehouses := make(map[string]bool)
		for _, finding := range findings {
			flaggedWarehouses[finding.Resource.Name] = true
		}

		assert.True(t, flaggedWarehouses["warehouse-huge"])
		assert.True(t, flaggedWarehouses["warehouse-large"])
		assert.False(t, flaggedWarehouses["warehouse-small"]) // Should not be flagged
	})
}

func TestCalculateNodeCount(t *testing.T) {
	t.Run("calculates node count correctly", func(t *testing.T) {
		testCases := []struct {
			size        string
			maxClusters int
			expected    int
		}{
			{"Small", 1, 4},
			{"Medium", 2, 16}, // 8 * 2
			{"Large", 4, 64},  // 16 * 4
			{"X-Large", 1, 32},
			{"Unknown", 1, 4}, // Default to small
		}

		for _, tc := range testCases {
			result := calculateNodeCount(tc.size, tc.maxClusters)
			assert.Equal(t, tc.expected, result, "Size: %s, MaxClusters: %d", tc.size, tc.maxClusters)
		}
	})
}

func TestCalculateSizeScore(t *testing.T) {
	t.Run("calculates size score correctly", func(t *testing.T) {
		testCases := []struct {
			size        string
			nodeCount   int
			maxClusters int
			expected    float64
		}{
			{"Small", 4, 1, 4.0},    // 4 * 1
			{"Medium", 16, 2, 16.0}, // 8 * 2
			{"Large", 64, 4, 64.0},  // 16 * 4
		}

		for _, tc := range testCases {
			result := calculateSizeScore(tc.size, tc.nodeCount, tc.maxClusters)
			assert.Equal(t, tc.expected, result, "Size: %s, NodeCount: %d, MaxClusters: %d", tc.size, tc.nodeCount, tc.maxClusters)
		}
	})
}
