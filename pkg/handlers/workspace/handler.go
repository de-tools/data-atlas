package workspace

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/databricks/databricks-sql-go/logger"
	"github.com/de-tools/data-atlas/pkg/models/api"
	"github.com/de-tools/data-atlas/pkg/models/domain"
	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog"
)

type Handler struct{}

func NewHandler() *Handler {
	return &Handler{}
}

func (h *Handler) ListWorkspaces(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	logger := zerolog.Ctx(ctx)
	workspaces := []api.Workspace{{Name: "default"}}
	err := json.NewEncoder(w).Encode(workspaces)
	if err != nil {
		logger.Error().
			Err(err).
			Msg("failed to encode workspaces")
	}
}

func (h *Handler) ListResources(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	logger := zerolog.Ctx(ctx)
	workspace := chi.URLParam(r, "workspace")
	workspaceResources := api.WorkspaceResources{
		Resources: []api.Resource{{ID: "1", Name: "warehouse"}},
	}
	err := json.NewEncoder(w).Encode(workspaceResources)
	if err != nil {
		logger.Error().
			Err(err).
			Str("ws", workspace).
			Msg("failed to encode workspace resources")
	}
}

func (h *Handler) GetResourceCost(w http.ResponseWriter, r *http.Request) {
	workspace := chi.URLParam(r, "workspace")
	resource := chi.URLParam(r, "resource")

	fixedEndTime := time.Date(2025, 6, 20, 12, 0, 0, 0, time.UTC)
	fixedStartTime := fixedEndTime.Add(-24 * time.Hour)

	cost := domain.ResourceCost{
		StartTime: fixedStartTime,
		EndTime:   fixedEndTime,
		Resource: domain.Resource{
			Platform:    "Databricks",
			Service:     resource,
			Name:        resource,
			Description: fmt.Sprintf("Mock resource in %s", workspace),
			Tags: map[string]string{
				"environment": workspace,
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
		Costs: []domain.CostComponent{
			{
				Type:        "compute",
				Value:       2,
				Unit:        "hours",
				TotalAmount: 0.0084,
				Rate:        0.0042,
				Currency:    "USD",
				Description: "Mock cost data",
			},
		},
	}

	err := json.NewEncoder(w).Encode(cost)
	if err != nil {
		logger.Error().
			Err(err).
			Str("ws", workspace).
			Msg("failed to encode resource cost")
	}
}
