package main

import (
	"fmt"
	"os"

	"github.com/de-tools/data-atlas/pkg/runtime/terminal"
	"github.com/de-tools/data-atlas/pkg/services/cost"
	"github.com/de-tools/data-atlas/pkg/services/cost/aws"
	awsce "github.com/de-tools/data-atlas/pkg/services/cost/aws_ce"
	"github.com/de-tools/data-atlas/pkg/services/cost/databricks"
	"github.com/de-tools/data-atlas/pkg/services/cost/snowflake"
)

func main() {
	cli := terminal.NewCLI(terminal.Options{
		Registry: cost.NewRegistry(map[string]cost.ControllerFactory{
			"databricks": databricks.ControllerFactory,
			"snowflake":  snowflake.ControllerFactory,
			"aws_ce":     awsce.ControllerFactory,
			"aws":        aws.ControllerFactory,
		}),
		Output: os.Stdout,
	})

	if err := cli.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}
