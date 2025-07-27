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
		workspace VARCHAR NOT NULL,
		created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	    last_processed_record_at TIMESTAMP NULL
	);
`
const UsageTableSchema = `
	CREATE TABLE IF NOT EXISTS usage_records (
		id VARCHAR NOT NULL,
		workspace VARCHAR NOT NULL,
		resource VARCHAR,
		metadata JSON,
		quantity DOUBLE,
		unit VARCHAR,
		sku VARCHAR,
		rate DOUBLE,
		currency VARCHAR,
		start_time TIMESTAMP,
		end_time TIMESTAMP,
		PRIMARY KEY (workspace, id)
	);
`

var bootQueries = []string{
	WorkflowState,
	UsageTableSchema,
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
