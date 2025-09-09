package workspace

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/de-tools/data-atlas/pkg/services/account/workspace"
	"github.com/de-tools/data-atlas/pkg/services/workflow"

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
	explorer     account.Explorer
	workflowCtrl workflow.Controller
}

func NewWorkspaceRouter(explorer account.Explorer, workflowController workflow.Controller) *Router {
	return &Router{
		explorer:     explorer,
		workflowCtrl: workflowController,
	}
}

func (r *Router) Routes() chi.Router {
	router := chi.NewRouter()
	router.Get("/workspaces", r.ListWorkspaces)
	router.Get("/workspaces/{workspace}/resources", r.ListResources)
	router.Get("/workspaces/{workspace}/resources/{resource}/cost", r.GetResourceCost)
	router.Get("/workspaces/{workspace}/resources/cost", r.GetWorkspaceResourcesCost)
	router.Post("/workspaces/{workspace}/sync", r.SyncWorkspace)

	// Audit endpoints - WIP
	router.Get("/workspaces/{workspace}/resources/warehouse/audit", r.GetWarehouseAudit)
	router.Get("/workspaces/{workspace}/resources/cluster/audit", r.GetClusterAudit)
	router.Get("/workspaces/{workspace}/resources/dlt_pipeline/audit", r.GetDLTAudit)
	router.Get("/workspaces/{workspace}/resources/endpoint/audit", r.GetModelServingAudit)

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

	costManager, err := r.explorer.GetWorkspaceCostManagerCached(ctx, ws)
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

func (r *Router) GetWorkspaceResourcesCost(w http.ResponseWriter, req *http.Request) {
	ctx := req.Context()
	ws := getWorkspaceFromPath(req)
	resourceTypes := req.URL.Query()["resource"]

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

	costManager, err := r.explorer.GetWorkspaceCostManagerCached(ctx, ws)
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

func (r *Router) SyncWorkspace(w http.ResponseWriter, req *http.Request) {
	ctx := req.Context()

	ws := getWorkspaceFromPath(req)

	err := r.workflowCtrl.Start(context.WithoutCancel(ctx), ws.Name)
	if err != nil {
		handleError(
			ctx,
			w,
			http.StatusInternalServerError,
			fmt.Errorf("failed to start workflow for workspace %s: %w", ws.Name, err),
		)
		return
	}

	err = jsonResponse(w, "OK")
	if err != nil {
		handleError(ctx, w, http.StatusInternalServerError, err)
	}
}

func (r *Router) GetWarehouseAudit(writer http.ResponseWriter, request *http.Request) {
	// TODO:
	// - Runs for too long, or Idle Workloads
	// - WH is to big
	// - Budgets / Alerts are set (based on DBU consumption)
	// - Removes stale / orphaned resources
	// - Right Size for Resource -> over provision e.g. large cluster running small workloads
	panic("Implement me")
}

func (r *Router) GetClusterAudit(writer http.ResponseWriter, request *http.Request) {
	// TODO:
	// - Billing per cluster / workspace
	// - Autoscalling / auto-terminations are on / off
	// - Cluster Utilisation, low average CPU and memory usage
	// - ID underutilized / overutilized clusters TOP N
	// - Cluster runtime duration - e.g. ran for 15 hours etc.
	// - GPU enabled costs, check if we have them [???]
	panic("Implement me")
}

func (r *Router) GetDLTAudit(w http.ResponseWriter, req *http.Request) {
	ctx := req.Context()
	ws := getWorkspaceFromPath(req)

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

	costManager, err := r.explorer.GetWorkspaceCostManagerCached(ctx, ws)
	if err != nil {
		handleError(ctx, w, http.StatusNotFound, err)
		return
	}

	// For now construct audit settings here; later can be provided via DI
	settings := workspace.DLTAuditSettings{
		MaintenanceRatioThreshold:  0.3,
		MinMaintenanceEvents:       3,
		LongRunAvgSecondsThreshold: 2 * 3600,
	}
	report, err := workspace.GetDLTAudit(ctx, ws, startTime, endTime, costManager, settings)
	if err != nil {
		handleError(ctx, w, http.StatusInternalServerError, err)
		return
	}

	if err := jsonResponse(w, adapters.MapAuditReportDomainToApi(report)); err != nil {
		handleError(ctx, w, http.StatusInternalServerError, err)
	}
}

func (r *Router) GetModelServingAudit(writer http.ResponseWriter, request *http.Request) {
	// Serving endpoints
	// TODO:
	// - Autoscaling & long time to provision endpoints
	// - Not working endpoints
	// - Utilization of endpoints
	// - Active compute time
	// - Golden Signals
	panic("Implement me")
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
