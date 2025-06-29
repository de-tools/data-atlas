package main

import (
	"fmt"
	"os"

	"github.com/de-tools/data-atlas/pkg/terminal"

	"github.com/de-tools/data-atlas/pkg/services/legacy_cost"
	"github.com/de-tools/data-atlas/pkg/services/legacy_cost/aws"
	awsce "github.com/de-tools/data-atlas/pkg/services/legacy_cost/aws_ce"
	"github.com/de-tools/data-atlas/pkg/services/legacy_cost/azure"
	"github.com/de-tools/data-atlas/pkg/services/legacy_cost/databricks"
	"github.com/de-tools/data-atlas/pkg/services/legacy_cost/snowflake"
)

func main() {
	cli := terminal.NewCLI(terminal.Options{
		Registry: legacy_cost.NewRegistry(map[string]legacy_cost.ControllerFactory{
			"databricks": databricks.ControllerFactory,
			"snowflake":  snowflake.ControllerFactory,
			"aws_ce":     awsce.ControllerFactory,
			"aws":        aws.ControllerFactory,
			"azure":      azure.ControllerFactory,
		}),
		Output: os.Stdout,
	})

	if err := cli.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}
