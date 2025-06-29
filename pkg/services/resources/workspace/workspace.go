package workspace

import (
	"context"

	"github.com/de-tools/data-atlas/pkg/models/domain"
)

type ManagementService interface {
	ListSupportedResources(ctx context.Context, ws domain.Workspace) ([]domain.WorkspaceResource, error)
	GetResourceCost(ctx context.Context, res domain.WorkspaceResource, interval int) ([]domain.ResourceCost, error)
}

type workspaceMgmtService struct {
}

func NewManagementService() ManagementService {
	return &workspaceMgmtService{}
}

func (w workspaceMgmtService) ListSupportedResources(ctx context.Context, ws domain.Workspace) ([]domain.WorkspaceResource, error) {
	//TODO implement me
	panic("implement me")
}

func (w workspaceMgmtService) GetResourceCost(ctx context.Context, res domain.WorkspaceResource, interval int) ([]domain.ResourceCost, error) {
	//TODO implement me
	panic("implement me")
}
