package main

import (
	"fmt"
	"github.com/de-tools/data-atlas/pkg/runtime/terminal"
	"github.com/de-tools/data-atlas/pkg/services/cost"
	"github.com/de-tools/data-atlas/pkg/services/cost/controllers/snowflake"
	"os"
)

func main() {
	registry := cost.NewRegistry()
	err := registry.Register("snowflake", snowflake.ControllerFactory)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to register snowflake platform: %v\n", err)
		os.Exit(1)
	}

	cli := terminal.NewCLI(terminal.Options{
		Registry: registry,
		Output:   os.Stdout,
	})

	if err := cli.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}
