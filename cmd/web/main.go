package main

import (
	"fmt"
	"os"
	"os/user"
	"time"

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
		"Path to the databrickscfg folder")

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func runServer(cmd *cobra.Command, args []string) error {
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	registry, err := config.NewRegistry(cfgPath)
	if err != nil {
		return fmt.Errorf("failed to create config registry: %w", err)
	}

	logger.Info().Msgf("Configuration found at %s", cfgPath)

	api := server.NewWebAPI(logger, server.Config{
		Addr:            ":8080",
		ShutdownTimeout: 10 * time.Second,
		Dependencies: server.Dependencies{
			Account: account.NewExplorer(registry),
		},
	})

	if err := api.Start(); err != nil {
		return fmt.Errorf("server failed: %w", err)
	}

	return nil
}
