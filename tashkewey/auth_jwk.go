/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tashkewey

import (
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rsa"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/matteobertozzi/gopher-depot/insights/tracer"
)

// JWK represents a JSON Web Key
type JWK struct {
	Kid string `json:"kid"`
	Kty string `json:"kty"`
	Use string `json:"use"`
	Crv string `json:"crv,omitempty"` // For EC/OKP keys
	N   string `json:"n,omitempty"`   // For RSA keys
	E   string `json:"e,omitempty"`   // For RSA keys
	X   string `json:"x,omitempty"`   // For EC/Ed25519 keys
	Y   string `json:"y,omitempty"`   // For EC keys (not used for Ed25519)
}

// JWKSet represents a JSON Web Key Set
type JWKSet struct {
	Keys []JWK `json:"keys"`
}

// CacheEntry holds cached key data with expiration
type CacheEntry struct {
	Key       any
	ExpiresAt time.Time
}

var (
	jwksURLs         = make(map[string]string)
	allowedAudiences = make(map[string]bool) // New: audience allowlist
	keyCache         = make(map[string]*CacheEntry)
	invalidKeyCache  = make(map[string]time.Time) // Cache for invalid (issuer, kid) pairs
	lastFetch        = make(map[string]time.Time)

	cacheMutex           sync.RWMutex
	cacheDuration        = 1 * time.Hour
	invalidCacheDuration = 5 * time.Minute
	minRefreshInterval   = 1 * time.Minute
)

func ClearJwkCache() {
	cacheMutex.Lock()
	defer cacheMutex.Unlock()

	clear(keyCache)
	clear(invalidKeyCache)
	clear(lastFetch)
}

func AddJwksUrl(issuer string, url string) {
	cacheMutex.Lock()
	defer cacheMutex.Unlock()
	jwksURLs[issuer] = url
}

func RemoveJwksUrl(issuer string) {
	cacheMutex.Lock()
	defer cacheMutex.Unlock()
	delete(jwksURLs, issuer)
}

// AddAllowedAudience adds an audience to the allowlist
func AddAllowedAudience(audience string) {
	cacheMutex.Lock()
	defer cacheMutex.Unlock()
	allowedAudiences[audience] = true
}

// RemoveAllowedAudience removes an audience from the allowlist
func RemoveAllowedAudience(audience string) {
	cacheMutex.Lock()
	defer cacheMutex.Unlock()
	delete(allowedAudiences, audience)
}

// IsAudienceAllowed checks if an audience is in the allowlist
func IsAudienceAllowed(audience string) bool {
	cacheMutex.RLock()
	defer cacheMutex.RUnlock()
	if len(allowedAudiences) == 0 {
		return true
	}
	return allowedAudiences[audience]
}

func IsAudienceRequired() bool {
	cacheMutex.RLock()
	defer cacheMutex.RUnlock()
	return len(allowedAudiences) != 0
}

func jwtErrorToHttpError(ctx context.Context, err error) error {
	traceId := tracer.GetTraceId(ctx)

	switch {
	case errors.Is(err, jwt.ErrTokenExpired):
		return &HttpErrorMessage{
			StatusCode: http.StatusUnauthorized,
			TraceId:    traceId,
			Status:     "SESSION_EXPIRED",
			Message:    "jwt token expired",
		}
	case errors.Is(err, jwt.ErrTokenNotValidYet) || errors.Is(err, jwt.ErrTokenMalformed) || errors.Is(err, jwt.ErrTokenSignatureInvalid):
		tracer.LogWarn(ctx, "invalid jwt token {error.message}", err.Error())
		return &HttpErrorMessage{
			StatusCode: http.StatusUnauthorized,
			TraceId:    traceId,
			Status:     "INVALID_TOKEN",
			Message:    err.Error(),
		}
	}

	tracer.LogWarn(ctx, "unable to parse jwt token {error.message}", err.Error())
	return &HttpErrorMessage{
		StatusCode: http.StatusUnauthorized,
		TraceId:    traceId,
		Status:     "INVALID_TOKEN",
		Message:    "unable to parse token",
	}
}

func VerifyJwtAuthToken(ctx context.Context, session *AuthSession, authToken string) error {
	if session == nil {
		return errors.New("session cannot be nil")
	}
	if authToken == "" {
		return errors.New("auth token cannot be empty")
	}

	token, err := jwt.Parse(authToken, jwtKeyFunction)
	if err != nil {
		return jwtErrorToHttpError(ctx, err)
	}

	if !token.Valid {
		return errors.New("token is not valid")
	}

	issuer, err := token.Claims.GetIssuer()
	if err != nil {
		return fmt.Errorf("failed to get issuer: %w", err)
	}
	session.Issuer = issuer

	subject, err := token.Claims.GetSubject()
	if err != nil {
		return fmt.Errorf("failed to get subject: %w", err)
	}
	session.Subject = subject

	aud, err := token.Claims.GetAudience()
	if err == nil && len(aud) > 0 {
		// Check audience allowlist
		if !IsAudienceAllowed(aud[0]) {
			return fmt.Errorf("audience not allowed: %s", aud[0])
		}
		session.Audience = aud[0]
	} else if IsAudienceRequired() {
		return errors.New("token must contain a valid audience")
	}

	return nil
}

func jwtKeyFunction(token *jwt.Token) (any, error) {
	// Validate signing method
	switch token.Method.(type) {
	case *jwt.SigningMethodRSA, *jwt.SigningMethodECDSA, *jwt.SigningMethodEd25519:
		// Supported methods
	default:
		return nil, fmt.Errorf("unexpected signing method %v", token.Header["alg"])
	}

	kid, ok := token.Header["kid"].(string)
	if !ok || kid == "" {
		return nil, fmt.Errorf("missing or invalid kid in token header")
	}

	issuer, err := token.Claims.GetIssuer()
	if err != nil || issuer == "" {
		return nil, fmt.Errorf("missing or invalid issuer in token claims: %w", err)
	}

	publicKey, err := getPublicKey(issuer, kid)
	if err != nil {
		return nil, fmt.Errorf("failed to get public key for issuer %s, kid %s: %w", issuer, kid, err)
	}

	return publicKey, nil
}

func getPublicKey(issuer, kid string) (any, error) {
	cacheKey := issuer + ":" + kid

	cacheMutex.RLock()
	// Check if this key is in invalid cache and still fresh
	if invalidTime, exists := invalidKeyCache[cacheKey]; exists {
		if time.Since(invalidTime) < invalidCacheDuration {
			cacheMutex.RUnlock()
			return nil, fmt.Errorf("key %s for issuer %s is cached as invalid", kid, issuer)
		}
	}

	// Check valid key cache
	if entry, exists := keyCache[cacheKey]; exists && time.Now().Before(entry.ExpiresAt) {
		cacheMutex.RUnlock()
		return entry.Key, nil
	}

	// Check if we need to refresh (key not found or expired, and minimum refresh interval passed)
	lastFetchTime, hasLastFetch := lastFetch[issuer]
	shouldRefresh := !hasLastFetch || time.Since(lastFetchTime) >= minRefreshInterval
	cacheMutex.RUnlock()

	if shouldRefresh {
		return fetchAndCacheKey(issuer, kid)
	}

	return nil, fmt.Errorf("key %s not found for issuer %s and refresh interval not met", kid, issuer)
}

func parseJwks(reader io.Reader, jwks *JWKSet) error {
	body, err := io.ReadAll(reader)
	if err != nil {
		return fmt.Errorf("failed to read JWKs content: %w", err)
	}

	if err := json.Unmarshal(body, &jwks); err != nil {
		return fmt.Errorf("failed to parse JWKs: %w", err)
	}
	return nil
}

func fetchHttpJwks(jwksURL string, jwks *JWKSet) error {
	tracer.LogDebug(context.Background(), "fetching JWKs from {jwks.url}", jwksURL)
	req, err := http.NewRequest("GET", jwksURL, nil)
	if err != nil {
		return fmt.Errorf("failed to create JWK request to %s: %v", jwksURL, err)
	}

	// Set Basic Auth credentials
	req.Header.Set("Authorization", "basic dXplcjpwYXp3b3Jk")

	client := http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to fetch JWKs from %s: %w", jwksURL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed to fetch JWKs: HTTP %d", resp.StatusCode)
	}

	return parseJwks(resp.Body, jwks)
}

func fetchFileJwks(jwksPath string, jwks *JWKSet) error {
	file, err := os.Open(jwksPath)
	if err != nil {
		return fmt.Errorf("failed to open JWKs file %s: %w", jwksPath, err)
	}
	defer file.Close()

	return parseJwks(file, jwks)
}

func getJwksUrlFromIssuer(issuer string) (string, error) {
	cacheMutex.RLock()
	defer cacheMutex.RUnlock()
	if jwksURL, ok := jwksURLs[issuer]; ok {
		return jwksURL, nil
	}
	return "", fmt.Errorf("unknown issuer: %s", issuer)
}

func fetchAndCacheKey(issuer, kid string) (any, error) {
	jwksURL, err := getJwksUrlFromIssuer(issuer)
	if err != nil {
		return nil, err
	}

	var jwks JWKSet
	if jwkFilePath, found := strings.CutPrefix(jwksURL, "file://"); found {
		if err := fetchFileJwks(jwkFilePath, &jwks); err != nil {
			return nil, err
		}
	} else {
		if err := fetchHttpJwks(jwksURL, &jwks); err != nil {
			return nil, err
		}
	}

	cacheMutex.Lock()
	defer cacheMutex.Unlock()

	// Remove expired invalid cache entry after acquiring lock
	cacheKey := issuer + ":" + kid
	if invalidTime, exists := invalidKeyCache[cacheKey]; exists {
		if time.Since(invalidTime) >= invalidCacheDuration {
			delete(invalidKeyCache, cacheKey)
		}
	}

	lastFetch[issuer] = time.Now()
	expiresAt := time.Now().Add(cacheDuration)

	// Cache all keys from the response
	for _, key := range jwks.Keys {
		if key.Kid == "" {
			continue
		}

		publicKey, err := convertJWKToPublicKey(&key)
		if err != nil {
			// Cache as invalid if conversion fails
			invalidKeyCache[issuer+":"+key.Kid] = time.Now()
			continue
		}

		keyCache[issuer+":"+key.Kid] = &CacheEntry{
			Key:       publicKey,
			ExpiresAt: expiresAt,
		}
	}

	// Check if our requested key was found
	if entry, exists := keyCache[cacheKey]; exists {
		return entry.Key, nil
	}

	// Key not found, cache as invalid
	invalidKeyCache[cacheKey] = time.Now()
	return nil, fmt.Errorf("key with kid %s not found for issuer %s", kid, issuer)
}

func convertJWKToPublicKey(jwk *JWK) (any, error) {
	switch jwk.Kty {
	case "RSA":
		return jwkToRSAPublicKey(jwk)
	case "EC":
		return jwkToECPublicKey(jwk)
	case "OKP":
		return jwkToEdDSAPublicKey(jwk)
	default:
		return nil, fmt.Errorf("unsupported key type: %s", jwk.Kty)
	}
}

func jwkToRSAPublicKey(jwk *JWK) (*rsa.PublicKey, error) {
	if jwk.N == "" || jwk.E == "" {
		return nil, fmt.Errorf("RSA key missing required parameters")
	}

	nBytes, err := base64.RawURLEncoding.DecodeString(jwk.N)
	if err != nil {
		return nil, fmt.Errorf("failed to decode modulus: %w", err)
	}

	eBytes, err := base64.RawURLEncoding.DecodeString(jwk.E)
	if err != nil {
		return nil, fmt.Errorf("failed to decode exponent: %w", err)
	}

	n := new(big.Int)
	n.SetBytes(nBytes)

	var e int
	switch len(eBytes) {
	case 1:
		e = int(eBytes[0])
	case 2:
		e = int(binary.BigEndian.Uint16(eBytes))
	case 3:
		e = int(eBytes[0])<<16 + int(eBytes[1])<<8 + int(eBytes[2])
	case 4:
		e = int(binary.BigEndian.Uint32(eBytes))
	default:
		return nil, fmt.Errorf("unsupported exponent length: %d", len(eBytes))
	}

	if e <= 0 {
		return nil, fmt.Errorf("invalid RSA exponent: %d", e)
	}

	return &rsa.PublicKey{
		N: n,
		E: e,
	}, nil
}

func jwkToECPublicKey(jwk *JWK) (*ecdsa.PublicKey, error) {
	if jwk.X == "" || jwk.Y == "" || jwk.Crv == "" {
		return nil, fmt.Errorf("EC key missing required parameters")
	}

	var curve elliptic.Curve
	switch jwk.Crv {
	case "P-256":
		curve = elliptic.P256()
	case "P-384":
		curve = elliptic.P384()
	case "P-521":
		curve = elliptic.P521()
	default:
		return nil, fmt.Errorf("unsupported curve: %s", jwk.Crv)
	}

	xBytes, err := base64.RawURLEncoding.DecodeString(jwk.X)
	if err != nil {
		return nil, fmt.Errorf("failed to decode x coordinate: %w", err)
	}

	yBytes, err := base64.RawURLEncoding.DecodeString(jwk.Y)
	if err != nil {
		return nil, fmt.Errorf("failed to decode y coordinate: %w", err)
	}

	x := new(big.Int)
	x.SetBytes(xBytes)

	y := new(big.Int)
	y.SetBytes(yBytes)

	pubKey := &ecdsa.PublicKey{
		Curve: curve,
		X:     x,
		Y:     y,
	}

	// Validate the point is on the curve
	if !curve.IsOnCurve(x, y) {
		return nil, fmt.Errorf("invalid EC key: point not on curve")
	}

	return pubKey, nil
}

func jwkToEdDSAPublicKey(jwk *JWK) (ed25519.PublicKey, error) {
	if jwk.Crv != "Ed25519" {
		return nil, fmt.Errorf("unsupported EdDSA curve: %s", jwk.Crv)
	}

	if jwk.X == "" {
		return nil, fmt.Errorf("Ed25519 key missing x parameter")
	}

	xBytes, err := base64.RawURLEncoding.DecodeString(jwk.X)
	if err != nil {
		return nil, fmt.Errorf("failed to decode x coordinate: %w", err)
	}

	if len(xBytes) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("invalid Ed25519 key size: expected %d, got %d", ed25519.PublicKeySize, len(xBytes))
	}

	return ed25519.PublicKey(xBytes), nil
}
