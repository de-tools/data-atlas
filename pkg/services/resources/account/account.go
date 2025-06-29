package account

import (
	"context"

	"github.com/de-tools/data-atlas/pkg/models/domain"
)

type ManagementService interface {
	ListWorkspaces(ctx context.Context) ([]domain.Workspace, error)
}

type accountMgmtService struct {
}

func NewManagementService() ManagementService {
	return &accountMgmtService{}
}

func (a *accountMgmtService) ListWorkspaces(ctx context.Context) ([]domain.Workspace, error) {
	//TODO implement me
	panic("implement me")
}
