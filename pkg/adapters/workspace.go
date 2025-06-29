package adapters

import (
	"github.com/de-tools/data-atlas/pkg/models/domain"
	"github.com/de-tools/data-atlas/pkg/models/store"
)

func MapStoreWsToDomainWs(ws store.Workspace) domain.Workspace {
	return domain.Workspace{
		ID:   ws.WorkspaceID,
		Name: ws.WorkspaceName,
	}
}
