package middleware

import (
	"net/http"

	"github.com/rs/zerolog"
)

func Logger(logger *zerolog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			reqLogger := logger.With().
				Str("method", req.Method).
				Str("path", req.URL.Path).
				Str("remote_ip", req.RemoteAddr).
				Logger()

			ctx := reqLogger.WithContext(req.Context())
			req = req.WithContext(ctx)

			next.ServeHTTP(w, req)
		})
	}
}
