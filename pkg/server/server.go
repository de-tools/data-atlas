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

type WebAPI struct {
	router *chi.Mux
	logger *zerolog.Logger
	addr   string
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

	router.Get("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("hello world"))
	})

	router.Route("/api/v1", func(r chi.Router) {
		r.Get("/workspaces", wsHandler.ListWorkspaces)
		r.Get("/workspaces/{workspace}/resources", wsHandler.ListResources)
		r.Get("/workspaces/{workspace}/{resource}/cost", wsHandler.GetResourceCost)
	})

	return &WebAPI{
		router: router,
		logger: &logger,
		addr:   config.Addr,
	}
}

func (w *WebAPI) Start() error {
	return http.ListenAndServe(w.addr, w.router)
}
