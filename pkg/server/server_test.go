package server

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"

	"github.com/de-tools/data-atlas/pkg/models/api"
	"github.com/de-tools/data-atlas/pkg/models/domain"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockAccountService struct {
	mock.Mock
}

func (m *mockAccountService) ListWorkspaces(ctx context.Context) ([]domain.Workspace, error) {
	args := m.Called(ctx)
	return args.Get(0).([]domain.Workspace), args.Error(1)
}

type mockWorkspaceService struct {
	mock.Mock
}

func (m *mockWorkspaceService) ListSupportedResources(
	ctx context.Context,
	ws domain.Workspace,
) ([]domain.WorkspaceResource, error) {
	args := m.Called(ctx, ws)
	return args.Get(0).([]domain.WorkspaceResource), args.Error(1)
}

func (m *mockWorkspaceService) GetResourceCost(
	ctx context.Context,
	res domain.WorkspaceResource,
	interval int,
) ([]domain.ResourceCost, error) {
	args := m.Called(ctx, res, interval)
	return args.Get(0).([]domain.ResourceCost), args.Error(1)
}

type testCase struct {
	name           string
	path           string
	setupMocks     func(*mockAccountService, *mockWorkspaceService)
	expectedStatus int
	expected       interface{}
	parseResponse  func([]byte) (interface{}, error)
}

func TestWebAPI_Endpoints(t *testing.T) {
	logger := zerolog.New(zerolog.NewTestWriter(nil))
	mockAccSvc := new(mockAccountService)
	mockWsSvc := new(mockWorkspaceService)

	config := Config{
		Addr:            ":8080",
		ShutdownTimeout: 10 * time.Second,
		Dependencies: Dependencies{
			Account:   mockAccSvc,
			Workspace: mockWsSvc,
		},
	}
	webAPI := NewWebAPI(logger, config)
	testServer := httptest.NewServer(webAPI.router)
	defer testServer.Close()

	tests := []testCase{
		{
			name: "ListWorkspaces",
			path: "/api/v1/workspaces",
			setupMocks: func(accSvc *mockAccountService, wsSvc *mockWorkspaceService) {
				accSvc.On("ListWorkspaces", mock.Anything).Return([]domain.Workspace{{Name: "default"}}, nil)
			},
			expectedStatus: http.StatusOK,
			expected:       []api.Workspace{{Name: "default"}},
			parseResponse:  unmarshalResponse[[]api.Workspace](),
		},
		{
			name: "ListResources",
			path: "/api/v1/workspaces/default/resources",
			setupMocks: func(accSvc *mockAccountService, wsSvc *mockWorkspaceService) {
				wsSvc.On("ListSupportedResources", mock.Anything, domain.Workspace{Name: "default"}).
					Return([]domain.WorkspaceResource{{ResourceName: "warehouse"}}, nil)
			},
			expectedStatus: http.StatusOK,
			expected:       []api.WorkspaceResource{{Name: "warehouse"}},
			parseResponse:  unmarshalResponse[[]api.WorkspaceResource](),
		},
		{
			name: "GetResourceCost",
			path: "/api/v1/workspaces/default/warehouse/cost",
			setupMocks: func(accSvc *mockAccountService, wsSvc *mockWorkspaceService) {
				wsSvc.On("GetResourceCost", mock.Anything, domain.WorkspaceResource{
					WorkspaceName: "default",
					ResourceName:  "warehouse",
				}, 7).Return([]domain.ResourceCost{{
					StartTime: time.Date(2025, 6, 19, 12, 0, 0, 0, time.UTC),
					EndTime:   time.Date(2025, 6, 20, 12, 0, 0, 0, time.UTC),
					Resource: domain.ResourceDef{
						Platform:    "Databricks",
						Service:     "warehouse",
						Name:        "warehouse",
						Description: "Mock resource in default",
						Tags: map[string]string{
							"environment": "default",
						},
						Metadata: map[string]string{},
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
			expected: []domain.ResourceCost{{
				StartTime: time.Date(2025, 6, 19, 12, 0, 0, 0, time.UTC),
				EndTime:   time.Date(2025, 6, 20, 12, 0, 0, 0, time.UTC),
				Resource: domain.ResourceDef{
					Platform:    "Databricks",
					Service:     "warehouse",
					Name:        "warehouse",
					Description: "Mock resource in default",
					Tags: map[string]string{
						"environment": "default",
					},
					Metadata: map[string]string{},
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
			}},
			parseResponse: unmarshalResponse[[]domain.ResourceCost](),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.setupMocks(mockAccSvc, mockWsSvc)

			resp, err := http.Get(testServer.URL + tc.path)
			require.NoError(t, err, "Failed to send request")
			defer func(Body io.ReadCloser) {
				err := Body.Close()
				if err != nil {
					require.NoError(t, err, "Failed to close response body")
				}
			}(resp.Body)

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
