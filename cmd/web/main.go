package main

import (
	"fmt"
	"net"
	"net/http"
	"os"
	"os/user"

	"github.com/de-tools/data-atlas/pkg/services/workflow"
	"github.com/de-tools/data-atlas/pkg/store/duckdb"
	duckdbusage "github.com/de-tools/data-atlas/pkg/store/duckdb/usage"
	duckdbworkflow "github.com/de-tools/data-atlas/pkg/store/duckdb/workflow"

	"github.com/de-tools/data-atlas/pkg/server"
	"github.com/de-tools/data-atlas/pkg/services/account"
	"github.com/de-tools/data-atlas/pkg/services/config"
	"github.com/joho/godotenv"
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
	if err := godotenv.Load(); err != nil {
		fmt.Printf("Error loading .env file: %v\n", err)
	}

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

	accountExplorer := account.NewExplorer(registry)

	db, err := duckdb.NewDB(duckdb.Settings{
		DbPath: "data-atlas.db",
	})
	if err != nil {
		return fmt.Errorf("failed to create DuckDB instance: %w", err)
	}

	workflowStore, err := duckdbworkflow.NewStore(db)
	if err != nil {
		return fmt.Errorf("failed to create workflow store: %w", err)
	}
	usageStore, err := duckdbusage.NewStore(db)
	if err != nil {
		return fmt.Errorf("failed to create usage store: %w", err)
	}
	workflowCtrl := workflow.NewController(db, accountExplorer, workflowStore, usageStore)
	err = workflowCtrl.Init(ctx)
	if err != nil {
		return fmt.Errorf("failed to initialize workflow controller: %w", err)
	}

	logger.Info().Msgf("Configuration found at `%s` successfully loaded.", cfgPath)
	logger.Info().Msgf("Found the following profiles:")
	profiles, _ := registry.GetProfiles(ctx)
	for _, profile := range profiles {
		logger.Info().Msgf("Name: `%s`, Type: `%s`", profile.Name, profile.Type)
	}

	mux := server.ConfigureRouter(server.Config{
		Dependencies: server.Dependencies{
			Account:            accountExplorer,
			WorkflowController: workflowCtrl,
			Logger:             logger,
		},
	})

	host := os.Getenv("SERVER_HOST")
	port := os.Getenv("SERVER_PORT")

	if host == "" || port == "" {
		logger.Error().Msgf("Missing server configuration from .env file")
		os.Exit(1)
	}

	addr := net.JoinHostPort(host, port)
	logger.Info().Msgf("starting server on %s", addr)

	return http.ListenAndServe(addr, mux)
}
