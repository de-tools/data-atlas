package main

import (
	"fmt"
	"net/http"
	"os"
	"os/user"

	"github.com/de-tools/data-atlas/pkg/server"
	"github.com/de-tools/data-atlas/pkg/services/account"
	"github.com/de-tools/data-atlas/pkg/services/config"
	"github.com/rs/zerolog"
	"github.com/spf13/cobra"
)

var cfgPath string

func main() {
	var rootCmd = &cobra.Command{
		Use:   "web",
		Short: "Start the web server for Data Atlas",
		RunE:  runServer,
	}

	usr, _ := user.Current()
	defaultPath := fmt.Sprintf("%s/.databrickscfg", usr.HomeDir)

	rootCmd.Flags().StringVarP(&cfgPath, "config", "c", defaultPath,
		"Path to the .databrickscfg file (default is $HOME/.databrickscfg)")

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func runServer(cmd *cobra.Command, _ []string) error {
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()
	ctx := logger.WithContext(cmd.Context())

	registry, err := config.NewRegistry(cfgPath)

	if err != nil {
		return fmt.Errorf("failed to create config registry: %w", err)
	}

	err = registry.Init(ctx)
	if err != nil {
		return fmt.Errorf("failed to initialize config registry: %w", err)
	}

	logger.Info().Msgf("Configuration found at `%s` successfully loaded.", cfgPath)
	logger.Info().Msgf("Found the following profiles:")
	profiles, _ := registry.GetProfiles(ctx)
	for _, profile := range profiles {
		logger.Info().Msgf("Name: `%s`, Type: `%s`", profile.Name, profile.Type)
	}

	mux := server.ConfigureRouter(server.Config{
		Dependencies: server.Dependencies{
			Account: account.NewExplorer(registry),
			Logger:  logger,
		},
	})

	return http.ListenAndServe(":8080", mux)
}
