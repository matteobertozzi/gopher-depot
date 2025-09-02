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
	"fmt"
	"net/http"

	"github.com/matteobertozzi/gopher-depot/insights/tracer"
)

type Route struct {
	Method             string
	Uri                string
	RequiredPermission RequiredPermission
	RateLimit          RateLimitConfig
	Handler            http.HandlerFunc
}

func AddGetRoute(mux *http.ServeMux, uri string, requiredPermission RequiredPermission, handler http.HandlerFunc) {
	addRoute(mux, "GET", uri, requiredPermission, handler)
}

func AddPostRoute(mux *http.ServeMux, uri string, requiredPermission RequiredPermission, handler http.HandlerFunc) {
	addRoute(mux, "POST", uri, requiredPermission, handler)
}

func AddPutRoute(mux *http.ServeMux, uri string, requiredPermission RequiredPermission, handler http.HandlerFunc) {
	addRoute(mux, "PUT", uri, requiredPermission, handler)
}

func AddPatchRoute(mux *http.ServeMux, uri string, requiredPermission RequiredPermission, handler http.HandlerFunc) {
	addRoute(mux, "PATCH", uri, requiredPermission, handler)
}

func AddDeleteRoute(mux *http.ServeMux, uri string, requiredPermission RequiredPermission, handler http.HandlerFunc) {
	addRoute(mux, "DELETE", uri, requiredPermission, handler)
}

func AddGetRouteWithRateLimit(mux *http.ServeMux, uri string, requiredPermission RequiredPermission, rateLimit RateLimitConfig, handler http.HandlerFunc) {
	addRouteWithRateLimit(mux, "GET", uri, requiredPermission, rateLimit, handler)
}

func AddPostRouteWithRateLimit(mux *http.ServeMux, uri string, requiredPermission RequiredPermission, rateLimit RateLimitConfig, handler http.HandlerFunc) {
	addRouteWithRateLimit(mux, "POST", uri, requiredPermission, rateLimit, handler)
}

func AddPutRouteWithRateLimit(mux *http.ServeMux, uri string, requiredPermission RequiredPermission, rateLimit RateLimitConfig, handler http.HandlerFunc) {
	addRouteWithRateLimit(mux, "PUT", uri, requiredPermission, rateLimit, handler)
}

func AddPatchRouteWithRateLimit(mux *http.ServeMux, uri string, requiredPermission RequiredPermission, rateLimit RateLimitConfig, handler http.HandlerFunc) {
	addRouteWithRateLimit(mux, "PATCH", uri, requiredPermission, rateLimit, handler)
}

func AddDeleteRouteWithRateLimit(mux *http.ServeMux, uri string, requiredPermission RequiredPermission, rateLimit RateLimitConfig, handler http.HandlerFunc) {
	addRouteWithRateLimit(mux, "DELETE", uri, requiredPermission, rateLimit, handler)
}

func addRoute(mux *http.ServeMux, method string, uri string, requiredPermission RequiredPermission, handler http.HandlerFunc) {
	AddRoute(mux, Route{
		Method:             method,
		Uri:                uri,
		RequiredPermission: requiredPermission,
		RateLimit:          NoRateLimit,
		Handler:            handler,
	})
}

func addRouteWithRateLimit(mux *http.ServeMux, method string, uri string, requiredPermission RequiredPermission, rateLimit RateLimitConfig, handler http.HandlerFunc) {
	AddRoute(mux, Route{
		Method:             method,
		Uri:                uri,
		RequiredPermission: requiredPermission,
		RateLimit:          rateLimit,
		Handler:            handler,
	})
}

func AddRoute(mux *http.ServeMux, route Route) {
	if route.RequiredPermission == nil {
		panic(fmt.Sprintf("expected required-permission to be set: %v %s", route.Method, route.Uri))
	}

	var handler http.Handler
	if _, ok := route.RequiredPermission.(AllowPublic); ok {
		// allow public, no authentication header required
		handler = route.Handler
		tracer.LogDebug(context.TODO(), "ALLOW PUBLIC {http.method} {http.uri}}", route.Method, route.Uri)
	} else {
		tracer.LogDebug(context.TODO(), "REQUIRE SESSION {http.method} {http.uri}}", route.Method, route.Uri)
		// authentication header required, and role validation
		requireRoleMiddleware := RequireRoleMiddleware(route.RequiredPermission)
		handler = AuthMiddleware(requireRoleMiddleware(route.Handler))
	}

	// Apply rate limiting if configured (non-zero values)
	if route.RateLimit.IsNotEmpty() {
		rateLimitMiddleware := CreateRateLimitMiddleware(route.RateLimit)
		handler = rateLimitMiddleware(handler)
		tracer.LogDebug(context.TODO(), "RATE LIMIT ENABLED {http.method} {http.uri} - {requests}/min, burst {burst}",
			route.Method, route.Uri, route.RateLimit.RequestsPerMinute, route.RateLimit.BurstSize)
	}

	if route.Method != "" {
		mux.Handle(route.Method+" "+route.Uri, handler)
	} else {
		mux.Handle(route.Uri, handler)
	}
}
