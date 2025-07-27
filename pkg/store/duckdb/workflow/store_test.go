package workflow

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/de-tools/data-atlas/pkg/models/store"
	"github.com/de-tools/data-atlas/pkg/store/duckdb"
	_ "github.com/marcboeker/go-duckdb/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fixture struct {
	db    *sql.DB
	store Store
}

func setupTestDB(t *testing.T) *sql.DB {
	db, err := duckdb.NewDB(duckdb.Settings{DbPath: ":memory:"})
	require.NoError(t, err)
	return db
}

func setupFixture(t *testing.T) *fixture {
	db := setupTestDB(t)
	store, err := NewStore(db)
	require.NoError(t, err)

	t.Cleanup(func() {
		db.Close()
	})

	return &fixture{
		db:    db,
		store: store,
	}
}

func TestNewStore(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		f := setupFixture(t)
		assert.NotNil(t, f.store)
	})

	t.Run("nil db", func(t *testing.T) {
		store, err := NewStore(nil)
		assert.Error(t, err)
		assert.Nil(t, store)
	})
}

func TestStore_CreateWorkflow(t *testing.T) {
	f := setupFixture(t)

	t.Run("success", func(t *testing.T) {
		ctx := context.Background()
		identity := store.WorkflowIdentity{
			Workspace: "test-workspace",
		}

		wf, err := f.store.CreateWorkflow(ctx, identity)
		require.NoError(t, err)
		assert.Equal(t, identity.Workspace, wf.Workspace)
		assert.NotNil(t, wf.CreatedAt)
		assert.Nil(t, wf.LastProcessedAt)
	})
}

func TestStore_ListWorkflows(t *testing.T) {
	f := setupFixture(t)
	ctx := context.Background()

	identity1 := store.WorkflowIdentity{
		Workspace: "workspace1",
	}
	identity2 := store.WorkflowIdentity{
		Workspace: "workspace2",
	}

	_, err := f.store.CreateWorkflow(ctx, identity1)
	require.NoError(t, err)
	_, err = f.store.CreateWorkflow(ctx, identity2)
	require.NoError(t, err)

	t.Run("list all workflows", func(t *testing.T) {
		workflows, err := f.store.ListWorkflows(ctx, nil)
		require.NoError(t, err)
		assert.Len(t, workflows, 2)
	})

	t.Run("list by workspace", func(t *testing.T) {
		workflows, err := f.store.ListWorkflows(ctx, []string{"workspace1"})
		require.NoError(t, err)
		assert.Len(t, workflows, 1)
		assert.Equal(t, "workspace1", workflows[0].Workspace)
	})
}

func TestStore_UpdateWorkflow(t *testing.T) {
	f := setupFixture(t)
	ctx := context.Background()

	t.Run("update last processed time", func(t *testing.T) {
		identity := store.WorkflowIdentity{
			Workspace: "workspace1",
		}

		_, err := f.store.CreateWorkflow(ctx, identity)
		require.NoError(t, err)

		now := time.Now()
		err = f.store.UpdateWorkflow(ctx, identity, now)
		require.NoError(t, err)

		// Verify update
		workflows, err := f.store.ListWorkflows(ctx, []string{identity.Workspace})
		require.NoError(t, err)
		assert.Len(t, workflows, 1)
		assert.Equal(t, now.Unix(), workflows[0].LastProcessedAt.Unix())
	})

	t.Run("update nonexistent workflow", func(t *testing.T) {
		nonexistent := store.WorkflowIdentity{
			Workspace: "nonexistent",
		}
		err := f.store.UpdateWorkflow(ctx, nonexistent, time.Now())
		assert.Error(t, err)
	})
}
