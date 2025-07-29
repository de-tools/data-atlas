package duckdb

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWorkflowService_ProcessWorkflow(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "duckdb-test-*")
	require.NoError(t, err)

	defer func() {
		err := os.RemoveAll(tmpDir)
		if err != nil {
			t.Errorf("failed to cleanup test directory: %v", err)
		}
	}()

	dbPath := filepath.Join(tmpDir, "test.db")
	db, err := NewDB(Settings{
		DbPath: dbPath,
	})
	require.NoError(t, err)
	require.NotNil(t, db)

	defer func() {
		err := db.Close()
		if err != nil {
			t.Errorf("failed to close database connection: %v", err)
		}
	}()

	_, err = db.Exec(
		`INSERT INTO workflow_state (id, workspace, status, error) VALUES (?, ?, ?, ?)`,
		"workflow-001", "my-workspace", "running", nil,
	)
	require.NoError(t, err)

	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM workflow_state WHERE id = ?", "workflow-001").Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, 1, count)
}
