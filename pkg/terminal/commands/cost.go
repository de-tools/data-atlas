package commands

import (
	"context"
	"fmt"
	"time"

	"github.com/de-tools/data-atlas/pkg/services/legacy_cost"
	"github.com/de-tools/data-atlas/pkg/terminal/export"

	"github.com/spf13/cobra"
)

type AnalyzeCmd struct {
	profilePath  string
	platform     string
	resourceType string
	duration     int
	registry     legacy_cost.Registry
	reporter     *export.Reporter
}

func NewAnalyzeCmd(registry legacy_cost.Registry, reporter *export.Reporter) *cobra.Command {
	ac := &AnalyzeCmd{registry: registry, reporter: reporter}
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

func (ac *AnalyzeCmd) run(cmd *cobra.Command, args []string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	ctrl, err := ac.registry.Create(ctx, ac.platform, ac.profilePath)
	if err != nil {
		return fmt.Errorf("failed to create a ctrl for platform: %s", ac.platform)
	}

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
