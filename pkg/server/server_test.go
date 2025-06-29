package server

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/de-tools/data-atlas/pkg/models/api"
	"github.com/de-tools/data-atlas/pkg/models/domain"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testCase struct {
	name           string
	path           string
	expectedStatus int
	expected       interface{}
	parseResponse  func([]byte) (interface{}, error)
}

func TestWebAPI_Endpoints(t *testing.T) {
	logger := zerolog.New(zerolog.NewTestWriter(nil))
	config := Config{
		Addr:            ":8080",
		ShutdownTimeout: 10 * time.Second,
	}
	webAPI := NewWebAPI(logger, config)
	testServer := httptest.NewServer(webAPI.router)
	defer testServer.Close()

	tests := []testCase{
		{
			name:           "ListWorkspaces",
			path:           "/api/v1/workspaces",
			expectedStatus: http.StatusOK,
			expected:       []api.Workspace{{Name: "default"}},
			parseResponse:  unmarshalResponse[[]api.Workspace](),
		},
		{
			name:           "ListResources",
			path:           "/api/v1/workspaces/default/resources",
			expectedStatus: http.StatusOK,
			expected: api.WorkspaceResources{
				Resources: []api.Resource{{
					ID:   "1",
					Name: "warehouse",
				}},
			},
			parseResponse: unmarshalResponse[api.WorkspaceResources](),
		},
		{
			name:           "GetResourceCost",
			path:           "/api/v1/workspaces/default/warehouse/cost",
			expectedStatus: http.StatusOK,
			expected: domain.ResourceCost{
				StartTime: time.Date(2025, 6, 19, 12, 0, 0, 0, time.UTC),
				EndTime:   time.Date(2025, 6, 20, 12, 0, 0, 0, time.UTC),
				Resource: domain.Resource{
					Platform:    "Databricks",
					Service:     "warehouse",
					Name:        "warehouse",
					Description: "Mock resource in default",
					Tags: map[string]string{
						"environment": "default",
					},
					Metadata: struct {
						ID        string
						AccountID string
						UserID    string
						Region    string
					}{
						ID:        "mock-id",
						AccountID: "123456789",
						UserID:    "user-1",
						Region:    "us-east-1",
					},
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
			},
			parseResponse: unmarshalResponse[domain.ResourceCost](),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
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
