package resources

import (
	"context"
	"github.com/de-tools/data-atlas/pkg/adapters"
	"github.com/de-tools/data-atlas/pkg/models/domain"
	"github.com/de-tools/data-atlas/pkg/store/api"
)

type ManagementService interface {
	ListWorkspaces(ctx context.Context) ([]domain.Workspace, error)
}

type workspaceMgmtService struct {
	client client.AccountClient
}

func NewManagementService(client client.AccountClient) ManagementService {
	return &workspaceMgmtService{
		client: client,
	}
}

func (wms *workspaceMgmtService) ListWorkspaces(ctx context.Context) ([]domain.Workspace, error) {
	workspaces, err := wms.client.ListWorkspaces(ctx)
	if err != nil {
		return nil, err
	}

	domainWorkspaces := make([]domain.Workspace, len(workspaces))
	for i, ws := range workspaces {
		domainWorkspaces[i] = adapters.MapStoreWsToDomainWs(ws)
	}

	return domainWorkspaces, nil
}
