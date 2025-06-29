package commands

import (
	"fmt"
	"strings"

	"github.com/de-tools/data-atlas/pkg/services/legacy_cost"
	"github.com/spf13/cobra"
)

type ResourcesCmd struct {
	profilePath string
	platform    string
	registry    legacy_cost.Registry
}

func NewResourcesCmd(registry legacy_cost.Registry) *cobra.Command {
	rc := &ResourcesCmd{registry: registry}
	cmd := &cobra.Command{
		Use:   "resources",
		Short: "List supported resource types for a platform",
		RunE:  rc.run,
	}

	cmd.Flags().StringVar(&rc.profilePath, "profile", "", "Path to the configuration profile")
	cmd.Flags().StringVar(&rc.platform, "platform", "", "Platform to list resources for (e.g., azure)")

	_ = cmd.MarkFlagRequired("profile")
	_ = cmd.MarkFlagRequired("platform")

	return cmd
}

func (rc *ResourcesCmd) run(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()

	ctrl, err := rc.registry.Create(ctx, rc.platform, rc.profilePath)
	if err != nil {
		return fmt.Errorf("failed to create controller for platform %s: %w", rc.platform, err)
	}

	resources := ctrl.GetSupportedResources()
	if len(resources) == 0 {
		fmt.Fprintf(cmd.OutOrStdout(), "No supported resources found for platform: %s\n", rc.platform)
		return nil
	}

	fmt.Fprintf(cmd.OutOrStdout(), "Supported resources for %s:\n%s\n",
		rc.platform,
		strings.Join(resources, "\n"))

	return nil
}
