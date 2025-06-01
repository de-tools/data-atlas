package main

import (
	"fmt"
	"github.com/de-tools/data-atlas/pkg/runtime/terminal"
	"github.com/de-tools/data-atlas/pkg/services/cost"
	"github.com/de-tools/data-atlas/pkg/services/cost/controllers/snowflake"
	"os"
)

func main() {
	cli := terminal.NewCLI(terminal.Options{
		Registry: cost.NewRegistry(map[string]cost.ControllerFactory{
			"snowflake": snowflake.ControllerFactory,
		}),
		Output: os.Stdout,
	})

	if err := cli.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}
