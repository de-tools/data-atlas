package workspace

import (
	"context"
	"testing"
	"time"

	"github.com/de-tools/data-atlas/pkg/models/domain"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// MockCostManager for testing
type MockCostManager struct {
	mock.Mock
}

func (m *MockCostManager) GetResourcesCost(ctx context.Context, resources domain.WorkspaceResources, startTime, endTime time.Time) ([]domain.ResourceCost, error) {
	args := m.Called(ctx, resources, startTime, endTime)
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

// MockExplorer for testing
type MockExplorer struct {
	mock.Mock
}

func (m *MockExplorer) ListSupportedResources(ctx context.Context) ([]domain.WorkspaceResource, error) {
	args := m.Called(ctx)
	return args.Get(0).([]domain.WorkspaceResource), args.Error(1)
}

func (m *MockExplorer) GetWarehouseMetadata(ctx context.Context, warehouseID string) (*domain.WarehouseMetadata, error) {
	args := m.Called(ctx, warehouseID)
	return args.Get(0).(*domain.WarehouseMetadata), args.Error(1)
}

func (m *MockExplorer) ListWarehouses(ctx context.Context) ([]domain.WarehouseMetadata, error) {
	args := m.Called(ctx)
	return args.Get(0).([]domain.WarehouseMetadata), args.Error(1)
}

// Test the main GetWarehouseAudit function
func TestGetWarehouseAudit(t *testing.T) {
	ctx := context.Background()
	ws := domain.Workspace{Name: "test-workspace"}
	startTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	endTime := time.Date(2024, 1, 8, 0, 0, 0, 0, time.UTC)
	settings := DefaultWarehouseAuditSettings()

	t.Run("no warehouse activity", func(t *testing.T) {
		mockCostManager := new(MockCostManager)
		mockExplorer := new(MockExplorer)

		// No records returned
		mockCostManager.On("GetResourcesCost", ctx, mock.AnythingOfType("domain.WorkspaceResources"), startTime, endTime).Return([]domain.ResourceCost{}, nil)

		report, err := GetWarehouseAudit(ctx, ws, startTime, endTime, mockCostManager, mockExplorer, settings)

		assert.NoError(t, err)
		assert.Equal(t, "test-workspace", report.Workspace)
		assert.Equal(t, "warehouse", report.ResourceType)
		assert.Len(t, report.Findings, 1) // Should have no_activity finding
		assert.Equal(t, "no_activity", report.Findings[0].Issue)
		assert.Contains(t, report.Summary, "no_activity")

		mockCostManager.AssertExpectations(t)
	})

	t.Run("successful audit with findings", func(t *testing.T) {
		mockCostManager := new(MockCostManager)
		mockExplorer := new(MockExplorer)

		// Create test records
		records := []domain.ResourceCost{
			{
				Resource:  domain.ResourceDef{Name: "warehouse-1"},
				StartTime: startTime,
				EndTime:   startTime.Add(10 * time.Hour), // Long runtime
				Costs: []domain.CostComponent{
					{TotalAmount: 50.0, Currency: "USD"},
				},
			},
		}

		// Create test metadata
		warehouseMetadata := &domain.WarehouseMetadata{
			ID:               "warehouse-1",
			Name:             "Test Warehouse",
			Size:             "Large",
			MaxNumClusters:   1,
			AutoStopMins:     0, // No auto-stop
			EnableServerless: false,
		}

		mockCostManager.On("GetResourcesCost", ctx, mock.AnythingOfType("domain.WorkspaceResources"), startTime, endTime).Return(records, nil)
		mockExplorer.On("ListWarehouses", ctx).Return([]domain.WarehouseMetadata{*warehouseMetadata}, nil)
		mockExplorer.On("GetWarehouseMetadata", ctx, "warehouse-1").Return(warehouseMetadata, nil)

		report, err := GetWarehouseAudit(ctx, ws, startTime, endTime, mockCostManager, mockExplorer, settings)

		assert.NoError(t, err)
		assert.Equal(t, "test-workspace", report.Workspace)
		assert.Equal(t, "warehouse", report.ResourceType)
		assert.Greater(t, len(report.Findings), 0)

		// Check summary fields
		assert.Contains(t, report.Summary, "warehouses_evaluated")
		assert.Contains(t, report.Summary, "total_cost_analyzed")
		assert.Contains(t, report.Summary, "total_findings")
		assert.Contains(t, report.Summary, "audit_status")

		// Should have findings for excessive runtime and missing auto-stop
		hasRuntimeFinding := false
		hasAutoStopFinding := false
		for _, finding := range report.Findings {
			if finding.Issue == "excessive_runtime" {
				hasRuntimeFinding = true
			}
			if finding.Issue == "auto_stop_disabled" {
				hasAutoStopFinding = true
			}
		}
		assert.True(t, hasRuntimeFinding, "Should have excessive runtime finding")
		assert.True(t, hasAutoStopFinding, "Should have auto-stop finding")

		mockCostManager.AssertExpectations(t)
		mockExplorer.AssertExpectations(t)
	})

	t.Run("cost manager error", func(t *testing.T) {
		mockCostManager := new(MockCostManager)
		mockExplorer := new(MockExplorer)

		mockCostManager.On("GetResourcesCost", ctx, mock.AnythingOfType("domain.WorkspaceResources"), startTime, endTime).Return([]domain.ResourceCost{}, assert.AnError)

		_, err := GetWarehouseAudit(ctx, ws, startTime, endTime, mockCostManager, mockExplorer, settings)

		assert.Error(t, err)
		mockCostManager.AssertExpectations(t)
	})
}

// Test runtime analysis
func TestAnalyzeRuntimeDuration(t *testing.T) {
	settings := DefaultWarehouseAuditSettings()

	t.Run("generates excessive runtime finding", func(t *testing.T) {
		records := []domain.ResourceCost{
			{
				Resource:  domain.ResourceDef{Name: "warehouse-1"},
				StartTime: time.Now(),
				EndTime:   time.Now().Add(10 * time.Hour), // Exceeds 8 hour threshold
				Costs: []domain.CostComponent{
					{TotalAmount: 100.0, Currency: "USD"},
				},
			},
		}

		findings := analyzeRuntimeDuration(records, settings)

		assert.Len(t, findings, 1)
		assert.Equal(t, "excessive_runtime", findings[0].Issue)
		assert.Equal(t, "warehouse-1", findings[0].Resource.Name)
		assert.Contains(t, findings[0].Description, "10.0 hours")
	})

	t.Run("no findings for normal runtime", func(t *testing.T) {
		records := []domain.ResourceCost{
			{
				Resource:  domain.ResourceDef{Name: "warehouse-1"},
				StartTime: time.Now(),
				EndTime:   time.Now().Add(2 * time.Hour), // Within threshold
				Costs: []domain.CostComponent{
					{TotalAmount: 20.0, Currency: "USD"},
				},
			},
		}

		findings := analyzeRuntimeDuration(records, settings)

		assert.Len(t, findings, 0)
	})
}

// Test size analysis
func TestAnalyzeWarehouseSizes(t *testing.T) {
	settings := DefaultWarehouseAuditSettings()

	t.Run("identifies oversized warehouses", func(t *testing.T) {
		records := []domain.ResourceCost{
			{
				Resource: domain.ResourceDef{Name: "large-warehouse"},
				Costs: []domain.CostComponent{
					{TotalAmount: 1000.0, Currency: "USD"},
				},
			},
			{
				Resource: domain.ResourceDef{Name: "small-warehouse"},
				Costs: []domain.CostComponent{
					{TotalAmount: 10.0, Currency: "USD"},
				},
			},
		}

		warehouseMetadata := map[string]domain.WarehouseMetadata{
			"large-warehouse": {
				ID:             "large-warehouse",
				Name:           "Large Warehouse",
				Size:           "X-Large",
				MaxNumClusters: 2,
			},
			"small-warehouse": {
				ID:             "small-warehouse",
				Name:           "Small Warehouse",
				Size:           "Small",
				MaxNumClusters: 1,
			},
		}

		findings := analyzeWarehouseSizes(records, warehouseMetadata, settings)

		// Should identify the large warehouse as oversized (top 5 largest)
		assert.Greater(t, len(findings), 0)

		hasOversizedFinding := false
		for _, finding := range findings {
			if finding.Issue == "oversized_warehouse" && finding.Resource.Name == "large-warehouse" {
				hasOversizedFinding = true
				assert.Contains(t, finding.Description, "X-Large")
			}
		}
		assert.True(t, hasOversizedFinding, "Should identify large warehouse as oversized")
	})
}

// Test best practices analysis
func TestAnalyzeBestPracticesCompliance(t *testing.T) {
	settings := DefaultWarehouseAuditSettings()

	t.Run("identifies missing auto-stop", func(t *testing.T) {
		records := []domain.ResourceCost{
			{
				Resource: domain.ResourceDef{Name: "warehouse-1"},
				Costs: []domain.CostComponent{
					{TotalAmount: 50.0, Currency: "USD"},
				},
			},
		}

		warehouseMetadata := map[string]domain.WarehouseMetadata{
			"warehouse-1": {
				ID:               "warehouse-1",
				Name:             "Test Warehouse",
				Size:             "Medium",
				AutoStopMins:     0, // No auto-stop
				EnableServerless: false,
			},
		}

		findings := analyzeBestPracticesCompliance(records, warehouseMetadata, settings)

		assert.Greater(t, len(findings), 0)

		hasAutoStopFinding := false
		for _, finding := range findings {
			if finding.Issue == "auto_stop_disabled" {
				hasAutoStopFinding = true
				assert.Equal(t, "warehouse-1", finding.Resource.Name)
			}
		}
		assert.True(t, hasAutoStopFinding, "Should identify missing auto-stop")
	})
}

// Test stale resources analysis
func TestAnalyzeStaleResources(t *testing.T) {
	settings := DefaultWarehouseAuditSettings()
	startTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	endTime := time.Date(2024, 1, 8, 0, 0, 0, 0, time.UTC)

	t.Run("identifies stale warehouses", func(t *testing.T) {
		// Old activity record
		oldTime := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC)
		records := []domain.ResourceCost{
			{
				Resource:  domain.ResourceDef{Name: "stale-warehouse"},
				StartTime: oldTime,
				EndTime:   oldTime.Add(1 * time.Hour),
				Costs: []domain.CostComponent{
					{TotalAmount: 10.0, Currency: "USD"},
				},
			},
		}

		warehouseMetadata := map[string]domain.WarehouseMetadata{
			"stale-warehouse": {
				ID:   "stale-warehouse",
				Name: "Stale Warehouse",
				Size: "Small",
			},
		}

		findings := analyzeStaleResources(records, warehouseMetadata, settings, startTime, endTime)

		assert.Greater(t, len(findings), 0)

		hasStaleFinding := false
		for _, finding := range findings {
			if finding.Issue == "stale_warehouse" {
				hasStaleFinding = true
				assert.Equal(t, "stale-warehouse", finding.Resource.Name)
			}
		}
		assert.True(t, hasStaleFinding, "Should identify stale warehouse")
	})
}

// Test default settings
func TestDefaultWarehouseAuditSettings(t *testing.T) {
	settings := DefaultWarehouseAuditSettings()

	assert.Equal(t, 8.0, settings.MaxRuntimeHours)
	assert.Equal(t, 2.0, settings.MaxIdleHours)
	assert.Equal(t, 0.5, settings.IdleTimeThreshold)
	assert.Equal(t, 30, settings.StaleResourceDays)
	assert.Equal(t, 5, settings.TopLargestCount)
	assert.Equal(t, 10, settings.MinQueryCountThreshold)
}
