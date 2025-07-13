package workspace

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/de-tools/data-atlas/pkg/adapters"

	"github.com/de-tools/data-atlas/pkg/services/account"

	"github.com/de-tools/data-atlas/pkg/models/api"
	"github.com/de-tools/data-atlas/pkg/models/domain"
	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog"
)

const (
	defaultInterval = 7 // 7 days ~ 1 week
	dateLayout      = "02-01-2006"
)

type Router struct {
	explorer account.Explorer
}

func NewWorkspaceRouter(explorer account.Explorer) *Router {
	return &Router{
		explorer: explorer,
	}
}

func (r *Router) Routes() chi.Router {
	router := chi.NewRouter()
	router.Get("/workspaces", r.ListWorkspaces)
	router.Get("/workspaces/{workspace}/resources", r.ListResources)
	router.Get("/workspaces/{workspace}/{resource}/cost", r.GetResourceCost)
	router.Get("/workspaces/{workspace}/metrics/cost", r.GetWorkspaceMetricsCost)
	return router
}

func (r *Router) ListWorkspaces(w http.ResponseWriter, req *http.Request) {
	ctx := req.Context()
	workspaces, err := r.explorer.ListWorkspaces(ctx)
	if err != nil {
		handleError(ctx, w, http.StatusInternalServerError, err)
		return
	}

	response := make([]api.Workspace, 0, len(workspaces))
	for _, ws := range workspaces {
		response = append(response, api.Workspace{Name: ws.Name})
	}

	err = jsonResponse(w, response)
	if err != nil {
		handleError(ctx, w, http.StatusInternalServerError, err)
	}
}

func (r *Router) ListResources(w http.ResponseWriter, req *http.Request) {
	ctx := req.Context()

	ws := getWorkspaceFromPath(req)
	wsExplorer, err := r.explorer.GetWorkspaceExplorer(ctx, ws)
	if err != nil {
		handleError(ctx, w, http.StatusInternalServerError, err)
		return
	}

	resources, err := wsExplorer.ListSupportedResources(ctx)
	if err != nil {
		handleError(ctx, w, http.StatusInternalServerError, err)
		return
	}

	response := make([]api.WorkspaceResource, 0, len(resources))
	for _, r := range resources {
		response = append(response, api.WorkspaceResource{Name: r.ResourceName})
	}

	err = jsonResponse(w, response)
	if err != nil {
		handleError(ctx, w, http.StatusInternalServerError, err)
	}
}

func (r *Router) GetResourceCost(w http.ResponseWriter, req *http.Request) {
	ctx := req.Context()
	ws := getWorkspaceFromPath(req)
	resource := chi.URLParam(req, "resource")

	endTime, err := parseDateParam(req, "to", time.Now())
	if err != nil {
		handleError(ctx, w, http.StatusBadRequest, err)
		return
	}

	startTime, err := parseDateParam(req, "from", time.Now().AddDate(0, 0, -defaultInterval))
	if err != nil {
		handleError(ctx, w, http.StatusBadRequest, err)
		return
	}

	costManager, err := r.explorer.GetWorkspaceCostManager(ctx, ws)
	if err != nil {
		handleError(ctx, w, http.StatusNotFound, err)
		return
	}

	resources := domain.WorkspaceResources{WorkspaceName: ws.Name, Resources: []string{resource}}
	records, err := costManager.GetResourcesCost(ctx, resources, startTime, endTime)
	if err != nil {
		handleError(ctx, w, http.StatusInternalServerError, err)
		return
	}

	apiRecords := make([]api.ResourceCost, 0, len(records))
	for _, r := range records {
		apiRecords = append(apiRecords, adapters.MapResourceCostDomainToApi(r))
	}

	err = jsonResponse(w, apiRecords)
	if err != nil {
		handleError(ctx, w, http.StatusInternalServerError, err)
	}
}

func (r *Router) GetWorkspaceMetricsCost(w http.ResponseWriter, req *http.Request) {
	ctx := req.Context()
	ws := getWorkspaceFromPath(req)
	resourceTypes := req.URL.Query()["resource_type"]

	endTime, err := parseDateParam(req, "to", time.Now())
	if err != nil {
		handleError(ctx, w, http.StatusBadRequest, err)
		return
	}

	startTime, err := parseDateParam(req, "from", time.Now().AddDate(0, 0, -defaultInterval))
	if err != nil {
		handleError(ctx, w, http.StatusBadRequest, err)
		return
	}

	costManager, err := r.explorer.GetWorkspaceCostManager(ctx, ws)
	if err != nil {
		handleError(ctx, w, http.StatusNotFound, err)
		return
	}

	resources := domain.WorkspaceResources{WorkspaceName: ws.Name, Resources: resourceTypes}
	records, err := costManager.GetResourcesCost(ctx, resources, startTime, endTime)
	if err != nil {
		handleError(ctx, w, http.StatusInternalServerError, err)
		return
	}

	apiRecords := make([]api.ResourceCost, 0, len(records))
	for _, r := range records {
		apiRecords = append(apiRecords, adapters.MapResourceCostDomainToApi(r))
	}

	err = jsonResponse(w, apiRecords)
	if err != nil {
		handleError(ctx, w, http.StatusInternalServerError, err)
	}
}

func handleError(ctx context.Context, w http.ResponseWriter, statusCode int, err error) {
	if err == nil {
		return
	}

	l := zerolog.Ctx(ctx)
	l.Error().Err(err).Msg("handler error")
	http.Error(w, err.Error(), statusCode)
}

func jsonResponse(w http.ResponseWriter, data interface{}) error {
	w.Header().Set("Content-Type", "application/json")
	return json.NewEncoder(w).Encode(data)
}

func parseDateParam(r *http.Request, paramName string, defaultDate time.Time) (time.Time, error) {
	param := r.URL.Query().Get(paramName)

	if param == "" {
		return defaultDate, nil
	}

	parsed, err := time.Parse(dateLayout, param)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid '%s' date format. Expected format: DD-MM-YYYY", paramName)
	}
	return parsed, nil
}

func getWorkspaceFromPath(r *http.Request) domain.Workspace {
	return domain.Workspace{Name: chi.URLParam(r, "workspace")}
}
