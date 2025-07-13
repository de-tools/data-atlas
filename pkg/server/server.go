package server

import (
	"net/http"
	"time"

	"github.com/de-tools/data-atlas/pkg/services/account"

	handlers "github.com/de-tools/data-atlas/pkg/handlers/workspace"

	dataatlasmiddleware "github.com/de-tools/data-atlas/pkg/server/middleware"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/rs/zerolog"
)

type Dependencies struct {
	Account account.Explorer
	Logger  zerolog.Logger
}
type Config struct {
	Addr            string
	ShutdownTimeout time.Duration
	Dependencies    Dependencies
}

func ConfigureRouter(config Config) *chi.Mux {
	router := chi.NewRouter()

	router.Use(dataatlasmiddleware.Logger(&config.Dependencies.Logger))
	router.Use(middleware.Recoverer)

	router.Get("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("hello world"))
	})

	workspaces := handlers.NewWorkspaceRouter(config.Dependencies.Account)
	router.Mount("/api/v1", workspaces.Routes())

	return router
}
