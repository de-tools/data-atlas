package workspace

import (
	"encoding/json"
	"net/http"

	"github.com/de-tools/data-atlas/pkg/models/api"
	"github.com/de-tools/data-atlas/pkg/models/domain"
	"github.com/de-tools/data-atlas/pkg/services/resources/account"
	"github.com/de-tools/data-atlas/pkg/services/resources/workspace"
	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog"
)

const (
	defaultInterval = 7 // 7 days ~ 1 week
)

type Handler struct {
	accMgmt account.ManagementService
	wsMgmt  workspace.ManagementService
}

func NewHandler(accMgmt account.ManagementService, wsMgmt workspace.ManagementService) *Handler {
	return &Handler{
		accMgmt: accMgmt,
		wsMgmt:  wsMgmt,
	}
}

func (h *Handler) ListWorkspaces(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	logger := zerolog.Ctx(ctx)

	workspaces, err := h.accMgmt.ListWorkspaces(ctx)
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

	resources, err := h.wsMgmt.ListSupportedResources(ctx, domain.Workspace{Name: ws})
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
	wsResource := domain.WorkspaceResource{WorkspaceName: ws, ResourceName: resource}
	records, err := h.wsMgmt.GetResourceCost(ctx, wsResource, intervalNum)

	// TODO: introduce API response model
	err = json.NewEncoder(w).Encode(records)
	if err != nil {
		logger.Error().
			Err(err).
			Str("ws", ws).
			Msg("failed to encode resource cost")
	}
}
