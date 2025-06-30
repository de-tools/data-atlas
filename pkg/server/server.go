package server

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/de-tools/data-atlas/pkg/services/account"

	handlers "github.com/de-tools/data-atlas/pkg/handlers/workspace"

	dataatlasmiddleware "github.com/de-tools/data-atlas/pkg/server/middleware"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/rs/zerolog"
)

type WebAPI struct {
	router *chi.Mux
	logger *zerolog.Logger
	server *http.Server
}

type Dependencies struct {
	Account account.Explorer
}
type Config struct {
	Addr            string
	ShutdownTimeout time.Duration
	Dependencies    Dependencies
}

func NewWebAPI(logger zerolog.Logger, config Config) *WebAPI {
	wsHandler := handlers.NewHandler(config.Dependencies.Account)

	router := chi.NewRouter()

	router.Use(dataatlasmiddleware.Logger(&logger))
	router.Use(middleware.Recoverer)

	router.Route("/api/v1", func(r chi.Router) {
		r.Get("/workspaces", wsHandler.ListWorkspaces)
		r.Get("/workspaces/{workspace}/resources", wsHandler.ListResources)
		r.Get("/workspaces/{workspace}/{resource}/cost", wsHandler.GetResourceCost)
	})

	return &WebAPI{
		router: router,
		logger: &logger,
		server: &http.Server{
			Addr:    config.Addr,
			Handler: router,
		},
	}
}

func (w *WebAPI) Start() error {
	serverErrors := make(chan error, 1)
	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, os.Interrupt, syscall.SIGTERM)

	go func() {
		w.logger.Info().Str("addr", w.server.Addr).Msg("starting server")
		serverErrors <- w.server.ListenAndServe()
	}()

	select {
	case err := <-serverErrors:
		return err
	case <-shutdown:
		w.logger.Info().Msg("shutdown initiated")

		// Give outstanding requests a deadline for completion.
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		err := w.server.Shutdown(ctx)
		if err != nil {
			w.logger.Error().Err(err).Msg("graceful shutdown failed")
			err = w.server.Close()
		}

		if err != nil {
			return err
		}
	}

	return nil
}
