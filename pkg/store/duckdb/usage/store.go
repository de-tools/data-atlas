package usage

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/de-tools/data-atlas/pkg/models/store"
)

type Store interface {
	Add(ctx context.Context, records []store.UsageRecord) error
}

type usageStore struct {
	db *sql.DB
}

func NewStore(db *sql.DB) (Store, error) {
	if db == nil {
		return nil, fmt.Errorf("database connection is nil")
	}
	return &usageStore{
		db: db,
	}, nil
}

func (u *usageStore) Add(ctx context.Context, records []store.UsageRecord) error {
	return fmt.Errorf("not implemented yet")
}
