package client

import (
	"context"

	"github.com/de-tools/data-atlas/pkg/models/store"
)

type ResourcesClient interface {
	List(ctx context.Context) ([]store.Workspace, error)
}
