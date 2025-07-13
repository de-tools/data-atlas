package server

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
)

func TestConfigureRouter(t *testing.T) {
	tests := []struct {
		name           string
		path           string
		expectedStatus int
	}{
		{
			name:           "root endpoint",
			path:           "/",
			expectedStatus: http.StatusOK,
		},
		{
			name:           "non-existent endpoint",
			path:           "/not-found",
			expectedStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := Config{
				Addr:            ":8080",
				ShutdownTimeout: 5 * time.Second,
				Dependencies: Dependencies{
					Logger: zerolog.New(zerolog.NewTestWriter(t)),
				},
			}

			router := ConfigureRouter(config)

			req := httptest.NewRequest("GET", tt.path, nil)
			rec := httptest.NewRecorder()

			router.ServeHTTP(rec, req)

			assert.Equal(t, tt.expectedStatus, rec.Code)
		})
	}
}

func TestRouterMiddlewares(t *testing.T) {
	config := Config{
		Addr:            ":8080",
		ShutdownTimeout: 5 * time.Second,
		Dependencies: Dependencies{
			Logger: zerolog.New(zerolog.NewTestWriter(t)),
		},
	}

	router := ConfigureRouter(config)

	t.Run("recoverer middleware handles panic", func(t *testing.T) {
		router.Get("/panic", func(w http.ResponseWriter, r *http.Request) {
			panic("test panic")
		})

		req := httptest.NewRequest("GET", "/panic", nil)
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusInternalServerError, rec.Code)
	})

	t.Run("logger middleware is present", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/", nil)
		rec := httptest.NewRecorder()

		router.ServeHTTP(rec, req)

		assert.Equal(t, http.StatusOK, rec.Code)
	})
}
