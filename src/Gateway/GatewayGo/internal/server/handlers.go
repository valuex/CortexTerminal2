package server

import (
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"

	"golang.org/x/crypto/bcrypt"

	"github.com/monster-echo/CortexTerminal2/gateway/internal/auth"
	"github.com/monster-echo/CortexTerminal2/gateway/internal/data"
)

// passwordLoginRequest matches the body posted by the C# Console:
// { username, password }.
type passwordLoginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

// passwordLoginResponse mirrors the C# response shape:
// { accessToken, username }.
type passwordLoginResponse struct {
	AccessToken string `json:"accessToken"`
	Username    string `json:"username"`
}

// handlePasswordLogin validates credentials against the Users table and
// mints a 7-day user JWT on success. Mirrors C# Program.cs lines 1095-1126.
func handlePasswordLogin(db *sql.DB, signingKey string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req passwordLoginRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, `{"error":"invalid body"}`, http.StatusBadRequest)
			return
		}
		user, err := data.FindByUsername(r.Context(), db, req.Username)
		if err != nil {
			if errors.Is(err, data.ErrNotFound) {
				http.Error(w, `{"error":"invalid_credentials"}`, http.StatusUnauthorized)
				return
			}
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if !user.PasswordHash.Valid || user.PasswordHash.String == "" {
			http.Error(w, `{"error":"password_login_disabled"}`, http.StatusUnauthorized)
			return
		}
		if err := bcrypt.CompareHashAndPassword([]byte(user.PasswordHash.String), []byte(req.Password)); err != nil {
			http.Error(w, `{"error":"invalid_credentials"}`, http.StatusUnauthorized)
			return
		}
		email := ""
		if user.Email.Valid {
			email = user.Email.String
		}
		tok, err := auth.MintUserToken(signingKey, user.Username, email, user.Role)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(passwordLoginResponse{
			AccessToken: tok,
			Username:    user.Username,
		})
	}
}

// handleMeProfile returns the authenticated user's profile by looking up
// the username from the JWT context. Mirrors C# Program.cs lines 493-503.
func handleMeProfile(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		username := auth.UsernameFromContext(r.Context())
		user, err := data.FindByUsername(r.Context(), db, username)
		if err != nil {
			http.Error(w, `{"error":"not_found"}`, http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"id":          user.ID,
			"username":    user.Username,
			"email":       user.Email.String,
			"displayName": user.DisplayName.String,
			"avatarUrl":   user.AvatarURL.String,
			"role":        user.Role,
			"status":      user.Status,
		})
	}
}