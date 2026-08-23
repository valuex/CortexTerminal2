// Package auth handles JWT minting/verification, OAuth, phone auth, captcha,
// and the chi middleware that gates protected endpoints.
//
// The wire shape mirrors the C# Gateway verbatim so tokens minted by either
// implementation validate on the other:
//
//	header  typ = "at+jwt"
//	issuer  iss = "https://gateway.local/"
//	aud     mint "corterm-gateway", accept both audiences
//	claims  sub + nameid + name + (email) + (role) + oi_tkn_typ=access_token
//	lifetime 7d user, configurable worker (default 30d)
package auth

import (
	"errors"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

const (
	Issuer = "https://gateway.local/"
	// AudienceMinted is the audience placed in freshly-minted tokens. The
	// C# gateway also mints this exact value.
	AudienceMinted = "corterm-gateway"
	// AudienceAcceptedLegacy is accepted on validate for backwards compat
	// with older tokens issued by the C# Gateway's first deploy.
	AudienceAcceptedLegacy = "cortex-terminal-gateway"
	// HeaderType mirrors C# token.Header["typ"] = "at+jwt".
	HeaderType = "at+jwt"
	// TokenTypeAccess is the value of the oi_tkn_typ claim.
	TokenTypeAccess = "access_token"

	// UserTokenLifetime matches C# DateTime.UtcNow.AddDays(7).
	UserTokenLifetime = 7 * 24 * time.Hour
)

// Claims is the C# JwtSecurityToken payload, with both `sub` and `nameid`
// populated to the same value (C# explicitly adds both because inbound
// claim mapping is disabled in TokenValidationParameters).
type Claims struct {
	Username string `json:"sub"`      // JwtRegisteredClaimNames.Sub
	NameID   string `json:"nameid"`   // ClaimTypes.NameIdentifier
	Name     string `json:"name"`     // ClaimTypes.Name
	Email    string `json:"email,omitempty"`
	Role     string `json:"role,omitempty"`
	oiTokenType string `json:"oi_tkn_typ"`
	jwt.RegisteredClaims
}

// UserID resolves the canonical user identifier from the claims. C# uses
// ClaimTypes.NameIdentifier as the principal identity claim because of the
// NameClaimType setting on TokenValidationParameters. We expose NameID first,
// then fall back to Sub to match the C# User.FindFirstValue logic.
func (c *Claims) UserID() string {
	if c.NameID != "" {
		return c.NameID
	}
	return c.Username
}

// Issuer is part of jwt.Claims.
func (c Claims) GetIssuer() (string, error) { return c.Issuer, nil }

// Audience is part of jwt.Claims.
func (c *Claims) GetAudience() (jwt.ClaimStrings, error) {
	if c.Audience == nil {
		return jwt.ClaimStrings{}, nil
	}
	return c.Audience, nil
}

// MintUserToken issues a 7-day user access token with both `sub` and
// `nameid` claims populated to the same username.
func MintUserToken(signingKey, username, email, role string) (string, error) {
	now := time.Now().UTC()
	claims := Claims{
		Username:    username,
		NameID:      username,
		Name:        username,
		Email:       email,
		Role:        role,
		oiTokenType: TokenTypeAccess,
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    Issuer,
			Audience:  jwt.ClaimStrings{AudienceMinted},
			IssuedAt:  jwt.NewNumericDate(now),
			NotBefore: jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(UserTokenLifetime)),
		},
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, &claims)
	token.Header["typ"] = HeaderType
	return token.SignedString([]byte(signingKey))
}

// MintWorkerToken issues a long-lived token for a Worker daemon. Role is
// fixed to "worker". Lifetime is configurable (default 30d in C#).
func MintWorkerToken(signingKey, workerID string, lifetime time.Duration) (string, error) {
	now := time.Now().UTC()
	if lifetime == 0 {
		lifetime = 30 * 24 * time.Hour
	}
	claims := Claims{
		Username:    workerID,
		NameID:      workerID,
		Name:        workerID,
		Role:        "worker",
		oiTokenType: TokenTypeAccess,
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    Issuer,
			Audience:  jwt.ClaimStrings{AudienceMinted},
			IssuedAt:  jwt.NewNumericDate(now),
			NotBefore: jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(now.Add(lifetime)),
		},
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, &claims)
	token.Header["typ"] = HeaderType
	return token.SignedString([]byte(signingKey))
}

// ParseToken validates the token's signature, issuer, audience, and expiry.
// Both `corterm-gateway` and `cortex-terminal-gateway` are accepted as the
// audience.
func ParseToken(signingKey, raw string) (*Claims, error) {
	parsed, err := jwt.ParseWithClaims(raw, &Claims{}, func(t *jwt.Token) (any, error) {
		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method %v", t.Header["alg"])
		}
		return []byte(signingKey), nil
	},
		jwt.WithIssuer(Issuer),
		jwt.WithAudience(AudienceMinted, AudienceAcceptedLegacy),
		jwt.WithExpirationRequired(),
		jwt.WithValidMethods([]string{"HS256"}),
	)
	if err != nil {
		return nil, err
	}
	claims, ok := parsed.Claims.(*Claims)
	if !ok || !parsed.Valid {
		return nil, errors.New("auth: invalid token")
	}
	return claims, nil
}