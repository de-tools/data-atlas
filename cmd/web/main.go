package main

import (
	"fmt"
	"os"
	"os/user"
	"time"

	"github.com/de-tools/data-atlas/pkg/services/account"
	"github.com/de-tools/data-atlas/pkg/services/config"

	"github.com/de-tools/data-atlas/pkg/server"

	"github.com/rs/zerolog"
)

func main() {
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	usr, err := user.Current()
	if err != nil {
		logger.Fatal().Err(err).Msg("server failed to start")
	}

	registry, err := config.NewRegistry(fmt.Sprintf("%s/.databrickscfg", usr.HomeDir))
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to create config registry")
	}

	api := server.NewWebAPI(logger, server.Config{
		Addr:            ":8080",
		ShutdownTimeout: 10 * time.Second,
		Dependencies: server.Dependencies{
			Account: account.NewExplorer(registry),
		},
	})

	if err := api.Start(); err != nil {
		logger.Fatal().Err(err).Msg("server failed")
	}
}
