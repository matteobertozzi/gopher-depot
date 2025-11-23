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
	"net/http"
	"strconv"
	"strings"
)

type CorsConfig struct {
	AllowedOrigins   []string
	AllowedMethods   []string
	AllowedHeaders   []string
	AllowCredentials bool
}

func CorsMiddleware(config *CorsConfig) func(http.Handler) http.Handler {
	var allowedMethods string
	if len(config.AllowedMethods) == 0 {
		allowedMethods = "GET, POST, PUT, DELETE, OPTIONS"
	} else {
		allowedMethods = strings.Join(config.AllowedMethods, ", ")
	}

	var allowedHeaders string
	if len(config.AllowedHeaders) == 0 {
		allowedHeaders = "Content-Type, Authorization"
	} else {
		allowedHeaders = strings.Join(config.AllowedHeaders, ", ")
	}

	allowedCredentials := strconv.FormatBool(config.AllowCredentials)

	allowedOrigins := make(map[string]struct{}, len(config.AllowedOrigins))
	for _, origin := range config.AllowedOrigins {
		allowedOrigins[origin] = struct{}{}
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			origin := r.Header.Get("Origin")
			_, allowed := allowedOrigins[origin]
			if allowed {
				h := w.Header()
				h.Set("Access-Control-Allow-Origin", origin)
				h.Set("Access-Control-Allow-Methods", allowedMethods)
				h.Set("Access-Control-Allow-Headers", allowedHeaders)
				h.Set("Access-Control-Allow-Credentials", allowedCredentials)
			}

			if r.Method != http.MethodOptions {
				next.ServeHTTP(w, r)
			} else if allowed {
				w.WriteHeader(http.StatusNoContent)
			} else {
				w.WriteHeader(http.StatusForbidden)
			}
		})
	}
}
