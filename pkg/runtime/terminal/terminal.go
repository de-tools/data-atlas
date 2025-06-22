package terminal

import (
	"io"
	"os"

	"github.com/de-tools/data-atlas/pkg/runtime/terminal/commands"
	"github.com/de-tools/data-atlas/pkg/runtime/terminal/export"

	"github.com/de-tools/data-atlas/pkg/services/cost"
	"github.com/spf13/cobra"
)

// CLI represents the command-line interface
type CLI struct {
	registry cost.Registry
	reporter *export.Reporter
	rootCmd  *cobra.Command
}

// Options contain configuration for the CLI
type Options struct {
	Registry cost.Registry
	Output   io.Writer
}

// NewCLI creates a new CLI instance
func NewCLI(opts Options) *CLI {
	if opts.Output == nil {
		opts.Output = os.Stdout
	}

	cli := &CLI{
		registry: opts.Registry,
		reporter: export.NewReporter(opts.Output),
	}

	cli.rootCmd = cli.newRootCmd()
	return cli
}

func (cli *CLI) Execute() error {
	return cli.rootCmd.Execute()
}

func (cli *CLI) newRootCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "cost",
		Short: "Cost analysis tool",
	}

	cmd.AddCommand(commands.NewAnalyzeCmd(cli.registry, cli.reporter))
	cmd.AddCommand(commands.NewResourcesCmd(cli.registry))

	return cmd
}
