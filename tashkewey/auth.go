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
	"maps"
	"net/http"
	"strings"

	"github.com/matteobertozzi/gopher-depot/insights/tracer"
)

type AuthSessionRoles map[string][]string

type AuthSession struct {
	Issuer   string // basic, jwt iss
	Audience string // jwt aud
	Subject  string // basic username, jwt sub
	Token    string // http 'Authorization' header value
	Roles    AuthSessionRoles
	Claims   map[string]string
	Data     map[string]any
}

func (session *AuthSession) UpdateClaims(claims map[string]string) {
	if len(claims) == 0 {
		return
	}

	if session.Claims == nil {
		session.Claims = make(map[string]string)
	}
	maps.Copy(session.Claims, claims)
}

func (session *AuthSession) UpdateData(data map[string]any) {
	if len(data) == 0 {
		return
	}

	if session.Data == nil {
		session.Data = make(map[string]any)
	}
	maps.Copy(session.Data, data)
}

const (
	AuthSessionCtxKey = "session"
)

func GetSession(context context.Context) *AuthSession {
	session, _ := context.Value(AuthSessionCtxKey).(*AuthSession)
	return session
}

type RoleFetcherFunc = func(context.Context, *AuthSession) error

var roleFetchers = make(map[string]RoleFetcherFunc)

func AddRoleFetcher(issuer string, fetcher RoleFetcherFunc) {
	roleFetchers[issuer] = fetcher
}

func RemoveRoleFetcher(issuer string) {
	delete(roleFetchers, issuer)
}

func parseAuthHeader(r *http.Request) (string, string, error) {
	authHeader := strings.TrimSpace(r.Header.Get("Authorization"))
	if authHeader == "" {
		return "", "", NewUnauthorizedError(r.Context(), "HEADER_MISSING", "Missing Authorization header")
	}

	typeEofIndex := strings.IndexByte(authHeader, ' ')
	if typeEofIndex < 0 {
		return "", "", NewUnauthorizedError(r.Context(), "HEADER_MISSING", "Invalid Authorization header format")
	}

	authType := strings.ToLower(authHeader[:typeEofIndex])
	authToken := authHeader[typeEofIndex+1:]
	return authType, authToken, nil
}

func AuthMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		authType, authToken, err := parseAuthHeader(r)
		if err != nil {
			WriteErrorResponse(w, r, err)
			return
		}

		session := AuthSession{
			Token: authType + " " + authToken,
		}
		ctx := context.WithValue(r.Context(), AuthSessionCtxKey, &session)

		switch authType {
		case "basic":
			err := VerifyBasicAuthToken(r.Context(), &session, authToken)
			if err != nil {
				tracer.LogError(r.Context(), err, "Invalid Basic Authorization header")
				WriteErrorResponse(w, r, err)
				return
			}
		case "bearer":
			// assume jwt
			err := VerifyJwtAuthToken(r.Context(), &session, authToken)
			if err != nil {
				tracer.LogError(r.Context(), err, "Invalid JWT Authorization header")
				WriteErrorResponse(w, r, err)
				return
			}
		default:
			http.Error(w, "Unsupported authorization method", http.StatusUnauthorized)
			return
		}

		// fetch roles
		//tracer.LogDebug(r.Context(), "role fetchers {session.issuer}: {fetchers}", session.Issuer, roleFetchers)
		if roleFetcher, hasRoleFetcher := roleFetchers[session.Issuer]; hasRoleFetcher {
			//tracer.LogDebug(r.Context(), "HAS ROLE FETCHER {fetcher} {issuer}", roleFetcher, session.Issuer)
			if err := roleFetcher(r.Context(), &session); err != nil {
				tracer.LogError(r.Context(), err, "Unable to fetch roles for {session.issuer} {session.subject}", session.Issuer, session.Subject)
				WriteErrorResponse(w, r, err)
				return
			}
		}

		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
