package middleware

import (
	"net/http"
)

// AuthMiddleware checks for X-Client-ID header
func AuthMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		clientID := r.Header.Get("X-Client-ID")
		if clientID == "" {
			http.Error(w, "Unauthorized: missing X-Client-ID header", http.StatusUnauthorized)
			return
		}

		// Optionally: you can validate against a DB/config if required
		// e.g. check if clientID exists in your rate-limiting rules table

		// If valid, pass request to next handler
		next.ServeHTTP(w, r)
	})
}
