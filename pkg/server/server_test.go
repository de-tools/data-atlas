package server

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/de-tools/data-atlas/pkg/services/account/workspace"

	"github.com/de-tools/data-atlas/pkg/models/api"
	"github.com/de-tools/data-atlas/pkg/models/domain"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

type mockExplorer struct {
	mock.Mock
}

func (m *mockExplorer) ListWorkspaces(ctx context.Context) ([]domain.Workspace, error) {
	args := m.Called(ctx)
	return args.Get(0).([]domain.Workspace), args.Error(1)
}

func (m *mockExplorer) GetWorkspaceExplorer(ctx context.Context, ws domain.Workspace) (workspace.Explorer, error) {
	args := m.Called(ctx, ws)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(workspace.Explorer), args.Error(1)
}

func (m *mockExplorer) GetWorkspaceCostManager(
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

func (m *mockWorkspaceCostManager) GetResourceCost(
	ctx context.Context,
	resource domain.WorkspaceResource,
	startTime, endTime time.Time,
) ([]domain.ResourceCost, error) {
	args := m.Called(ctx, resource, startTime, endTime)
	return args.Get(0).([]domain.ResourceCost), args.Error(1)
}

func TestWebAPI_Endpoints(t *testing.T) {
	logger := zerolog.New(zerolog.NewTestWriter(nil))

	mockExp := new(mockExplorer)
	mockWsExp := new(mockWorkspaceExplorer)
	mockCostMgr := new(mockWorkspaceCostManager)

	config := Config{
		Addr:            ":8080",
		ShutdownTimeout: 10 * time.Second,
		Dependencies: Dependencies{
			Account: mockExp,
			Logger:  logger,
		},
	}
	router := ConfigureRouter(config)
	testServer := httptest.NewServer(router)
	defer testServer.Close()

	expectedStartTime, _ := time.Parse("2006-01-02", "2025-06-13")
	expectedEndTime, _ := time.Parse("2006-01-02", "2025-06-20")

	tests := []struct {
		name           string
		path           string
		setupMocks     func()
		expectedStatus int
		expected       interface{}
		parseResponse  func([]byte) (interface{}, error)
	}{
		{
			name: "ListWorkspaces",
			path: "/api/v1/workspaces",
			setupMocks: func() {
				mockExp.On("ListWorkspaces", mock.Anything).
					Return([]domain.Workspace{{Name: "default"}}, nil)
			},
			expectedStatus: http.StatusOK,
			expected:       []api.Workspace{{Name: "default"}},
			parseResponse:  unmarshalResponse[[]api.Workspace](),
		},
		{
			name: "ListResources",
			path: "/api/v1/workspaces/default/resources",
			setupMocks: func() {
				mockExp.On("GetWorkspaceExplorer", mock.Anything, domain.Workspace{Name: "default"}).
					Return(mockWsExp, nil)
				mockWsExp.On("ListSupportedResources", mock.Anything).
					Return([]domain.WorkspaceResource{{ResourceName: "warehouse"}}, nil)
			},
			expectedStatus: http.StatusOK,
			expected:       []api.WorkspaceResource{{Name: "warehouse"}},
			parseResponse:  unmarshalResponse[[]api.WorkspaceResource](),
		},
		{
			name: "GetResourceCost",
			path: "/api/v1/workspaces/default/warehouse/cost?from=2025-06-13&to=2025-06-20",
			setupMocks: func() {
				mockExp.On("GetWorkspaceCostManager", mock.Anything, domain.Workspace{Name: "default"}).
					Return(mockCostMgr, nil)
				mockCostMgr.On("GetResourceCost",
					mock.Anything,
					domain.WorkspaceResource{
						WorkspaceName: "default",
						ResourceName:  "warehouse",
					},
					expectedStartTime,
					expectedEndTime,
				).Return([]domain.ResourceCost{{
					StartTime: expectedStartTime,
					EndTime:   expectedEndTime,
					Resource: domain.ResourceDef{
						Platform:    "Databricks",
						Service:     "warehouse",
						Name:        "warehouse",
						Description: "Mock resource in default",
						Metadata:    map[string]string{},
					},
					Costs: []domain.CostComponent{{
						Type:        "compute",
						Value:       2,
						Unit:        "hours",
						TotalAmount: 0.0084,
						Rate:        0.0042,
						Currency:    "USD",
						Description: "Mock cost data",
					}},
				}}, nil)
			},
			expectedStatus: http.StatusOK,
			expected: []api.ResourceCost{{
				StartTime: expectedStartTime,
				EndTime:   expectedEndTime,
				Resource: api.ResourceDef{
					Platform:    "Databricks",
					Service:     "warehouse",
					Name:        "warehouse",
					Description: "Mock resource in default",
					Metadata:    map[string]string{},
				},
				Costs: []api.CostComponent{{
					Type:        "compute",
					Value:       2,
					Unit:        "hours",
					TotalAmount: 0.0084,
					Rate:        0.0042,
					Currency:    "USD",
					Description: "Mock cost data",
				}},
			}},
			parseResponse: unmarshalResponse[[]api.ResourceCost](),
		},
		{
			name: "GetResourceCost_InvalidFromDate",
			path: "/api/v1/workspaces/default/warehouse/cost?from=invalid-date",
			setupMocks: func() {
				mockExp.On("GetWorkspaceCostManager", mock.Anything, domain.Workspace{Name: "default"}).
					Return(mockCostMgr, nil)
			},
			expectedStatus: http.StatusBadRequest,
			expected:       "invalid 'from' date format. Expected format: YYYY-MM-DD\n",
			parseResponse: func(data []byte) (interface{}, error) {
				return string(data), nil
			},
		},
		{
			name: "GetResourceCost_InvalidToDate",
			path: "/api/v1/workspaces/default/warehouse/cost?to=invalid-date",
			setupMocks: func() {
				mockExp.On("GetWorkspaceCostManager", mock.Anything, domain.Workspace{Name: "default"}).
					Return(mockCostMgr, nil)
			},
			expectedStatus: http.StatusBadRequest,
			expected:       "invalid 'to' date format. Expected format: YYYY-MM-DD\n",
			parseResponse: func(data []byte) (interface{}, error) {
				return string(data), nil
			},
		},
		{
			name: "GetResourceCost_DefaultDates",
			path: "/api/v1/workspaces/default/warehouse/cost",
			setupMocks: func() {
				mockExp.On("GetWorkspaceCostManager", mock.Anything, domain.Workspace{Name: "default"}).
					Return(mockCostMgr, nil)

				now := time.Now()
				startTime := now.AddDate(0, 0, -7)

				mockCostMgr.On("GetResourceCost",
					mock.Anything,
					domain.WorkspaceResource{
						WorkspaceName: "default",
						ResourceName:  "warehouse",
					},
					mock.MatchedBy(func(t time.Time) bool {
						// Match the start time within a day to account for test execution time
						return t.Sub(startTime).Hours() < 24
					}),
					mock.MatchedBy(func(t time.Time) bool {
						// Match the end time within a day
						return now.Sub(t).Hours() < 24
					}),
				).Return([]domain.ResourceCost{}, nil)
			},
			expectedStatus: http.StatusOK,
			expected:       []api.ResourceCost{},
			parseResponse:  unmarshalResponse[[]api.ResourceCost](),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.setupMocks()
			resp, err := http.Get(testServer.URL + tc.path)
			require.NoError(t, err, "Failed to send request")
			defer resp.Body.Close()

			assert.Equal(t, tc.expectedStatus, resp.StatusCode, "Status code mismatch")

			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err, "Failed to read response body")

			actual, err := tc.parseResponse(body)
			require.NoError(t, err, "Failed to parse response")

			assert.Equal(t, tc.expected, actual)
		})
	}
}

func unmarshalResponse[T any]() func([]byte) (interface{}, error) {
	return func(data []byte) (interface{}, error) {
		var response T
		err := json.Unmarshal(data, &response)
		return response, err
	}
}
