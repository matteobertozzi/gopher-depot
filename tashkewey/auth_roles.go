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
	"errors"
	"fmt"
	"net/http"
	"slices"
	"time"

	"github.com/matteobertozzi/gopher-depot/insights/tracer"
	"github.com/matteobertozzi/gopher-depot/tashkewey/internal"
)

type RequiredPermission interface {
	EnsurePermission(context context.Context, session *AuthSession) error
}

type AllowPublic struct{}

func (v AllowPublic) EnsurePermission(context context.Context, session *AuthSession) error {
	// nothing required
	return nil
}

type RequireSession struct {
	Issuers []string
}

func (v RequireSession) EnsurePermission(context context.Context, session *AuthSession) error {
	if session == nil {
		return errors.New("session not found")
	}

	if err := ensureSessionIssuer(context, session, v.Issuers); err != nil {
		return err
	}
	return nil
}

type RequiredModulePermission struct {
	Module  string
	Roles   []string
	Issuers []string
}

func (v RequiredModulePermission) EnsurePermission(context context.Context, session *AuthSession) error {
	if session == nil {
		return errors.New("session not found")
	}

	if err := ensureSessionIssuer(context, session, v.Issuers); err != nil {
		return err
	}

	if err := ensureSessionRoles(context, session, v.Module, v.Roles); err != nil {
		return err
	}

	return nil
}

func ensureSessionIssuer(context context.Context, session *AuthSession, allowedIssuers []string) error {
	if len(allowedIssuers) == 0 {
		// Allow all
		return nil
	}

	if slices.Contains(allowedIssuers, session.Issuer) {
		// Allow access to the issuer
		return nil
	}

	// Deny access to the issuer
	tracer.LogWarn(context, "invalid session issuer, expected one of {allowed.issuers} got {session.issuer}", allowedIssuers, session.Issuer)
	return errors.New("invalid session issuer")
}

func ensureSessionRoles(context context.Context, session *AuthSession, allowedModule string, allowedRoles []string) error {
	if len(allowedRoles) == 0 {
		// invalid route definition, RequiredModulePermission requires at least one role
		return errors.ErrUnsupported
	}

	sessionModuleRoles, hasModuleRoles := session.Roles[allowedModule]
	if !hasModuleRoles {
		tracer.LogWarn(context, "invalid session roles, expected roles for {module} {roles}: got {session.roles}", allowedModule, allowedRoles, session.Roles)
		return errors.New("invalid session, missing module")
	}

	// TODO: optimize
	for _, allowedRole := range allowedRoles {
		if slices.Contains(sessionModuleRoles, allowedRole) {
			return nil
		}
	}

	tracer.LogWarn(context, "invalid session roles, expected one of {allowed.roles} got {session.roles}", allowedRoles, session.Roles)
	return errors.New("invalid session roles")
}

func RequireRoleMiddleware(requiredPermission RequiredPermission) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			session := GetSession(r.Context())
			if err := requiredPermission.EnsurePermission(r.Context(), session); err != nil {
				WriteErrorResponse(w, r, err)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

type stdRoleFetcherResult struct {
	Roles  AuthSessionRoles  `json:"roles"`
	Claims map[string]string `json:"claims"`
	Data   map[string]any    `json:"data"`
}

// Size calculation function for stdRoleFetcherResult
func calculateRoleResultSize(result stdRoleFetcherResult) int64 {
	size := int64(32)

	for _, role := range result.Roles {
		size += int64(len(role))
	}

	for key, value := range result.Claims {
		size += int64(len(key)) + int64(len(value))
	}

	for key, value := range result.Data {
		size += int64(len(key))
		if str, ok := value.(string); ok {
			size += int64(len(str))
		} else {
			size += 256
		}
	}

	return size
}

// Global cache instance
var roleCache = internal.NewCache[stdRoleFetcherResult](128, time.Hour, calculateRoleResultSize)

func AuthSessionHttpRolesFetcher(url string) RoleFetcherFunc {
	return func(ctx context.Context, session *AuthSession) error {
		if cached, found := roleCache.Get(session.Token); found {
			session.Roles = cached.Roles
			session.UpdateClaims(cached.Claims)
			session.UpdateData(cached.Data)
			tracer.LogDebug(ctx, "fetch roles for {session.issuer} {session.subject}: {roles} (cached)", session.Issuer, session.Subject, session.Roles)
			return nil
		}

		tracer.LogDebug(ctx, "fetching roles for {session.issuer} {session.subject}", session.Issuer, session.Subject)
		var userInfo stdRoleFetcherResult
		if err := httpGet(url, session.Token, &userInfo); err != nil {
			tracer.LogError(ctx, err, "unable to fetch roles for {session.issuer} {session.subject}", session.Issuer, session.Subject)
			return err
		}

		roleCache.Set(session.Token, userInfo)

		session.Roles = userInfo.Roles
		session.UpdateClaims(userInfo.Claims)
		session.UpdateData(userInfo.Data)
		tracer.LogDebug(ctx, "fetched roles for {session.issuer} {session.subject}: {roles}", session.Issuer, session.Subject, session.Roles)
		return nil
	}
}

func httpGet(url string, authToken string, respBody any) error {
	// Create a request
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("error creating the request: %v", err)
	}

	req.Header.Add("accept", "application/json")
	req.Header.Add("authorization", authToken)

	// Send the request
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("error making the request: %v", err)
	}

	return internal.DecodeResponseBody(resp, &respBody)
}
