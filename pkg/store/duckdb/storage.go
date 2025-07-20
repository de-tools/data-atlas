package duckdb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"

	"github.com/marcboeker/go-duckdb/v2"
)

const WorkflowState = `
	CREATE TABLE IF NOT EXISTS workflow_state (
		id VARCHAR PRIMARY KEY,
		workspace VARCHAR NOT NULL,
		status VARCHAR  NOT NULL,
		error TEXT,
		created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
	);
`

var bootQueries = []string{
	WorkflowState,
}

type Settings struct {
	DbPath string
}

func NewDB(settings Settings) (*sql.DB, error) {
	c, err := duckdb.NewConnector(fmt.Sprintf("%s?threads=4", settings.DbPath), func(exec driver.ExecerContext) error {
		bootQueries := append([]string{}, bootQueries...)

		for _, query := range bootQueries {
			_, err := exec.ExecContext(context.Background(), query, nil)
			if err != nil {
				return err
			}
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	db := sql.OpenDB(c)
	return db, nil
}
