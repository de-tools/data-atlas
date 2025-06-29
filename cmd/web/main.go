package main

import (
	"os"
	"time"

	"github.com/de-tools/data-atlas/pkg/server"
	"github.com/rs/zerolog"
)

func main() {
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()
	api := server.NewWebAPI(logger, server.Config{
		Addr:            ":8080",
		ShutdownTimeout: 10 * time.Second,
	})
	if err := api.Start(); err != nil {
		logger.Fatal().Err(err).Msg("server failed")
	}
}
