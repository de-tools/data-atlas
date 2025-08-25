package workspace

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/de-tools/data-atlas/pkg/services/account/workspace"

	"github.com/de-tools/data-atlas/pkg/models/api"
	"github.com/de-tools/data-atlas/pkg/models/domain"
	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type mockAccountExplorer struct {
	mock.Mock
}

func (m *mockAccountExplorer) ListWorkspaces(ctx context.Context) ([]domain.Workspace, error) {
	args := m.Called(ctx)
	return args.Get(0).([]domain.Workspace), args.Error(1)
}

func (m *mockAccountExplorer) GetWorkspaceExplorer(
	ctx context.Context,
	ws domain.Workspace,
) (workspace.Explorer, error) {
	args := m.Called(ctx, ws)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(workspace.Explorer), args.Error(1)
}

func (m *mockAccountExplorer) GetWorkspaceCostManagerCached(
	ctx context.Context,
	ws domain.Workspace,
) (workspace.CostManager, error) {
	args := m.Called(ctx, ws)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(workspace.CostManager), args.Error(1)
}

func (m *mockAccountExplorer) GetWorkspaceCostManagerRemote(
	ctx context.Context,
	ws domain.Workspace,
) (workspace.CostManager, error) {
	args := m.Called(ctx, ws)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(workspace.CostManager), args.Error(1)
}

type mockWorkspaceExplorer struct {
	mock.Mock
}

func (m *mockWorkspaceExplorer) ListSupportedResources(ctx context.Context) ([]domain.WorkspaceResource, error) {
	args := m.Called(ctx)
	return args.Get(0).([]domain.WorkspaceResource), args.Error(1)
}

type mockWorkspaceCostManager struct {
	mock.Mock
}

func (m *mockWorkspaceCostManager) GetResourcesCost(
	ctx context.Context,
	resource domain.WorkspaceResources,
	startTime, endTime time.Time,
) ([]domain.ResourceCost, error) {
	args := m.Called(ctx, resource, startTime, endTime)
	return args.Get(0).([]domain.ResourceCost), args.Error(1)
}

func (m *mockWorkspaceCostManager) GetUsageStats(ctx context.Context, startTime *time.Time) (*domain.UsageStats, error) {
	return nil, nil
}

func (m *mockWorkspaceCostManager) GetUsage(ctx context.Context, startTime, endTime time.Time) ([]domain.ResourceCost, error) {
	return nil, nil
}

type mockWorkflowController struct{}

func (m *mockWorkflowController) Start(ctx context.Context, workspace string) error  { return nil }
func (m *mockWorkflowController) Cancel(ctx context.Context, workspace string) error { return nil }

func setupRouter(explorer *mockAccountExplorer, workflowController *mockWorkflowController) *Router {
	return NewWorkspaceRouter(explorer, workflowController)
}

func TestListWorkspaces(t *testing.T) {
	tests := []struct {
		name           string
		setupMock      func(*mockAccountExplorer)
		expectedStatus int
		expectedBody   []api.Workspace
	}{
		{
			name: "successful response",
			setupMock: func(m *mockAccountExplorer) {
				m.On("ListWorkspaces", mock.Anything).Return(
					[]domain.Workspace{{Name: "ws1"}, {Name: "ws2"}},
					nil,
				)
			},
			expectedStatus: http.StatusOK,
			expectedBody: []api.Workspace{
				{Name: "ws1"},
				{Name: "ws2"},
			},
		},
		{
			name: "empty workspaces list",
			setupMock: func(m *mockAccountExplorer) {
				m.On("ListWorkspaces", mock.Anything).Return(
					[]domain.Workspace{},
					nil,
				)
			},
			expectedStatus: http.StatusOK,
			expectedBody:   []api.Workspace{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			accountExplorer := new(mockAccountExplorer)
			tt.setupMock(accountExplorer)
			workflowController := new(mockWorkflowController)
			router := setupRouter(accountExplorer, workflowController)

			req := httptest.NewRequest("GET", "/workspaces", nil)
			rec := httptest.NewRecorder()

			router.ListWorkspaces(rec, req)

			assert.Equal(t, tt.expectedStatus, rec.Code)

			var response []api.Workspace
			err := json.NewDecoder(rec.Body).Decode(&response)
			assert.NoError(t, err)
			assert.Equal(t, tt.expectedBody, response)

			accountExplorer.AssertExpectations(t)
		})
	}
}

func TestListResources(t *testing.T) {
	tests := []struct {
		name           string
		workspace      string
		setupMock      func(*mockAccountExplorer, *mockWorkspaceExplorer)
		expectedStatus int
		expectedBody   []api.WorkspaceResource
	}{
		{
			name:      "successful response",
			workspace: "test-workspace",
			setupMock: func(me *mockAccountExplorer, wsExplorer *mockWorkspaceExplorer) {
				me.On("GetWorkspaceExplorer", mock.Anything, domain.Workspace{Name: "test-workspace"}).
					Return(wsExplorer, nil)
				wsExplorer.On("ListSupportedResources", mock.Anything).Return(
					[]domain.WorkspaceResource{
						{WorkspaceName: "test-workspace", ResourceName: "warehouse"},
						{WorkspaceName: "test-workspace", ResourceName: "cluster"},
					},
					nil,
				)
			},
			expectedStatus: http.StatusOK,
			expectedBody: []api.WorkspaceResource{
				{Name: "warehouse"},
				{Name: "cluster"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			accExplorer := new(mockAccountExplorer)
			wsExplorer := new(mockWorkspaceExplorer)
			tt.setupMock(accExplorer, wsExplorer)
			workflowController := new(mockWorkflowController)

			router := setupRouter(accExplorer, workflowController)
			req := httptest.NewRequest("GET", "/workspaces/"+tt.workspace+"/resources", nil)
			rec := httptest.NewRecorder()

			// Set up chi context with URL parameters
			ctx := chi.NewRouteContext()
			ctx.URLParams.Add("workspace", tt.workspace)
			req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, ctx))

			router.ListResources(rec, req)

			assert.Equal(t, tt.expectedStatus, rec.Code)

			var response []api.WorkspaceResource
			err := json.NewDecoder(rec.Body).Decode(&response)
			assert.NoError(t, err)
			assert.Equal(t, tt.expectedBody, response)

			accExplorer.AssertExpectations(t)
			wsExplorer.AssertExpectations(t)
		})
	}
}
func TestGetResourceCost(t *testing.T) {
	startTimeTest := time.Date(2025, 7, 1, 0, 0, 0, 0, time.UTC)
	endTimeTest := time.Date(2025, 7, 13, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		name           string
		workspace      string
		resource       string
		queryParams    map[string]string
		setupMock      func(*mockAccountExplorer, *mockWorkspaceCostManager)
		expectedStatus int
		expectedBody   []api.ResourceCost
	}{
		{
			name:      "successful response",
			workspace: "test-workspace",
			resource:  "warehouse",
			queryParams: map[string]string{
				"from": "01-07-2025",
				"to":   "13-07-2025",
			},
			setupMock: func(me *mockAccountExplorer, cm *mockWorkspaceCostManager) {
				me.On("GetWorkspaceCostManagerCached", mock.Anything, domain.Workspace{Name: "test-workspace"}).
					Return(cm, nil)

				cm.On("GetResourcesCost",
					mock.Anything,
					domain.WorkspaceResources{
						WorkspaceName: "test-workspace",
						Resources:     []string{"warehouse"},
					},
					startTimeTest,
					endTimeTest,
				).Return([]domain.ResourceCost{
					{
						StartTime: startTimeTest,
						EndTime:   endTimeTest,
						Resource: domain.ResourceDef{
							Platform:    "Databricks",
							Service:     "SQL",
							Name:        "warehouse",
							Description: "Databricks SQL Warehouse",
							Metadata: map[string]string{
								"warehouse_id": "test-id",
							},
						},
						Costs: []domain.CostComponent{
							{
								Type:        "compute",
								Value:       2.0,
								Unit:        "DBU",
								TotalAmount: 4.0,
								Rate:        2.0,
								Currency:    "USD",
								Description: "Compute usage",
							},
							{
								Type:        "storage",
								Value:       100.0,
								Unit:        "GB",
								TotalAmount: 10.0,
								Rate:        0.1,
								Currency:    "USD",
								Description: "Storage usage",
							},
						},
					},
				}, nil)
			},
			expectedStatus: http.StatusOK,
			expectedBody: []api.ResourceCost{
				{
					StartTime: startTimeTest,
					EndTime:   endTimeTest,
					Resource: api.ResourceDef{
						Platform:    "Databricks",
						Service:     "SQL",
						Name:        "warehouse",
						Description: "Databricks SQL Warehouse",
						Metadata: map[string]string{
							"warehouse_id": "test-id",
						},
					},
					Costs: []api.CostComponent{
						{
							Type:        "compute",
							Value:       2.0,
							Unit:        "DBU",
							TotalAmount: 4.0,
							Rate:        2.0,
							Currency:    "USD",
							Description: "Compute usage",
						},
						{
							Type:        "storage",
							Value:       100.0,
							Unit:        "GB",
							TotalAmount: 10.0,
							Rate:        0.1,
							Currency:    "USD",
							Description: "Storage usage",
						},
					},
				},
			},
		},
		{
			name:      "invalid date format",
			workspace: "test-workspace",
			resource:  "warehouse",
			queryParams: map[string]string{
				"from": "invalid-date",
			},
			setupMock: func(me *mockAccountExplorer, cm *mockWorkspaceCostManager) {
				// No mocks needed for this case
			},
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:      "cost manager error",
			workspace: "test-workspace",
			resource:  "warehouse",
			queryParams: map[string]string{
				"from": "01-07-2025",
				"to":   "13-07-2025",
			},
			setupMock: func(me *mockAccountExplorer, cm *mockWorkspaceCostManager) {
				me.On("GetWorkspaceCostManagerCached", mock.Anything, domain.Workspace{Name: "test-workspace"}).
					Return(nil, fmt.Errorf("workspace not found"))
			},
			expectedStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockExplorer := new(mockAccountExplorer)
			mockCostManager := new(mockWorkspaceCostManager)
			tt.setupMock(mockExplorer, mockCostManager)
			workflowController := new(mockWorkflowController)

			router := setupRouter(mockExplorer, workflowController)

			// Build URL with query parameters
			url := "/workspaces/" + tt.workspace + "/" + tt.resource + "/cost"
			if len(tt.queryParams) > 0 {
				first := true
				for k, v := range tt.queryParams {
					if first {
						url += "?"
						first = false
					} else {
						url += "&"
					}
					url += k + "=" + v
				}
			}

			req := httptest.NewRequest("GET", url, nil)
			rec := httptest.NewRecorder()

			// Set up chi context with URL parameters
			ctx := chi.NewRouteContext()
			ctx.URLParams.Add("workspace", tt.workspace)
			ctx.URLParams.Add("resource", tt.resource)
			req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, ctx))

			router.GetResourceCost(rec, req)

			assert.Equal(t, tt.expectedStatus, rec.Code)

			if tt.expectedStatus == http.StatusOK {
				var response []api.ResourceCost
				err := json.NewDecoder(rec.Body).Decode(&response)
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedBody, response)
			}

			mockExplorer.AssertExpectations(t)
			mockCostManager.AssertExpectations(t)
		})
	}
}

func TestParseDataParam(t *testing.T) {
	tests := []struct {
		name         string
		paramName    string
		paramValue   string
		defaultDate  time.Time
		expectedDate time.Time
		expectError  bool
	}{
		{
			name:         "valid date",
			paramName:    "from",
			paramValue:   "13-07-2025",
			defaultDate:  time.Now(),
			expectedDate: time.Date(2025, 7, 13, 0, 0, 0, 0, time.UTC),
			expectError:  false,
		},
		{
			name:         "invalid date format",
			paramName:    "from",
			paramValue:   "2025-07-13",
			defaultDate:  time.Now(),
			expectedDate: time.Time{},
			expectError:  true,
		},
		{
			name:         "empty date",
			paramName:    "from",
			paramValue:   "",
			defaultDate:  time.Date(2025, 7, 13, 0, 0, 0, 0, time.UTC),
			expectedDate: time.Date(2025, 7, 13, 0, 0, 0, 0, time.UTC),
			expectError:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", "/?"+tt.paramName+"="+tt.paramValue, nil)
			result, err := parseDateParam(req, tt.paramName, tt.defaultDate)

			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedDate, result)
			}
		})
	}
}
