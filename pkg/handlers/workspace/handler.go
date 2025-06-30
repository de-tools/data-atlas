package workspace

import (
	"encoding/json"
	"net/http"

	"github.com/de-tools/data-atlas/pkg/services/account"

	"github.com/de-tools/data-atlas/pkg/models/api"
	"github.com/de-tools/data-atlas/pkg/models/domain"
	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog"
)

const (
	defaultInterval = 7 // 7 days ~ 1 week
)

type Handler struct {
	explorer account.Explorer
}

func NewHandler(explorer account.Explorer) *Handler {
	return &Handler{
		explorer: explorer,
	}
}

func (h *Handler) ListWorkspaces(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	logger := zerolog.Ctx(ctx)

	workspaces, err := h.explorer.ListWorkspaces(ctx)
	var response []api.Workspace
	for _, ws := range workspaces {
		response = append(response, api.Workspace{Name: ws.Name})
	}

	err = json.NewEncoder(w).Encode(workspaces)

	if err != nil {
		logger.Error().
			Err(err).
			Msg("failed to encode workspaces")
	}
}

func (h *Handler) ListResources(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	logger := zerolog.Ctx(ctx)

	ws := chi.URLParam(r, "workspace")

	wsExplorer, err := h.explorer.GetWorkspaceExplorer(ctx, domain.Workspace{Name: ws})
	if err != nil {
		logger.Error().
			Err(err).
			Str("ws", ws).
			Msg("failed to get workspace explorer")
		http.Error(w, "workspace not found", http.StatusNotFound)
		return
	}

	resources, err := wsExplorer.ListSupportedResources(ctx)
	var response []api.WorkspaceResource
	for _, r := range resources {
		response = append(response, api.WorkspaceResource{Name: r.ResourceName})
	}

	err = json.NewEncoder(w).Encode(response)
	if err != nil {
		logger.Error().
			Err(err).
			Str("ws", ws).
			Msg("failed to encode workspace resources")
	}
}

func (h *Handler) GetResourceCost(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	logger := zerolog.Ctx(ctx)
	ws := chi.URLParam(r, "workspace")
	resource := chi.URLParam(r, "resource")
	interval := r.URL.Query().Get("interval")

	var intervalNum int
	if interval == "" {
		intervalNum = defaultInterval
	}

	costManager, err := h.explorer.GetWorkspaceCostManager(ctx, domain.Workspace{Name: ws})
	if err != nil {
		logger.Error().
			Err(err).
			Str("ws", ws).
			Msg("failed to get workspace cost manager")
		http.Error(w, "workspace not found", http.StatusNotFound)
		return
	}

	wsResource := domain.WorkspaceResource{WorkspaceName: ws, ResourceName: resource}
	records, err := costManager.GetResourceCost(ctx, wsResource, intervalNum)

	// TODO: introduce API response model
	err = json.NewEncoder(w).Encode(records)
	if err != nil {
		logger.Error().
			Err(err).
			Str("ws", ws).
			Msg("failed to encode resource cost")
	}
}
