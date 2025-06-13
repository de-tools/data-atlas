package terminal

import (
	"context"
	"fmt"
	"github.com/de-tools/data-atlas/pkg/services/cost"
	"github.com/spf13/cobra"
	"io"
	"os"
)

// CLI represents the command-line interface
type CLI struct {
	registry cost.Registry
	reporter *Reporter
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
		reporter: NewReporter(opts.Output),
	}

	cli.rootCmd = cli.newRootCmd()
	return cli
}

// Execute runs the CLI application
func (cli *CLI) Execute() error {
	return cli.rootCmd.Execute()
}

func (cli *CLI) newRootCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "cost",
		Short: "Cost analysis tool",
	}

	cmd.AddCommand(cli.newAnalyzeCmd())
	return cmd
}

type analyzeCmd struct {
	profilePath  string
	platform     string
	resourceType string
	duration     int
	registry     cost.Registry
	reporter     *Reporter
}

func (cli *CLI) newAnalyzeCmd() *cobra.Command {
	ac := &analyzeCmd{registry: cli.registry, reporter: cli.reporter}
	cmd := &cobra.Command{
		Use:   "analyze",
		Short: "Analyze resource costs",
		RunE:  ac.run,
	}

	// Define flags
	cmd.Flags().StringVar(&ac.profilePath, "profile", "", "Path to the configuration profile")
	cmd.Flags().StringVar(&ac.platform, "platform", "", "Platform to analyze (e.g., snowflake)")
	cmd.Flags().StringVar(&ac.resourceType, "resource_type", "", "Type of resource to analyze")
	cmd.Flags().IntVar(&ac.duration, "duration", 30, "Duration in days to analyze")

	// Mark required flags
	_ = cmd.MarkFlagRequired("profile")
	_ = cmd.MarkFlagRequired("platform")
	_ = cmd.MarkFlagRequired("resource_type")

	return cmd
}

func (ac *analyzeCmd) run(cmd *cobra.Command, args []string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	ctrl, err := ac.registry.Create(ctx, ac.platform, ac.profilePath)
	if err != nil {
		return fmt.Errorf("failed to create a ctrl for platform: %s", ac.platform)
	}

	// Validate resource type
	supported := ctrl.GetSupportedResources()
	valid := false
	for _, r := range supported {
		if r == ac.resourceType {
			valid = true
			break
		}
	}

	if !valid {
		return fmt.Errorf("unsupported resource type %q for platform %q. Supported types: %v",
			ac.resourceType, ac.platform, supported)
	}

	report, err := ctrl.EstimateResourceCost(ctx, ac.resourceType, ac.duration)
	if err != nil {
		return fmt.Errorf("failed to estimate resource cost: %w", err)
	}

	return ac.reporter.Handle(report)
}
