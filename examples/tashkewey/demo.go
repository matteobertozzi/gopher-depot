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

package main

import (
	"net/http"

	"github.com/matteobertozzi/gopher-depot/tashkewey"
)

type DummyService struct{}

type DummyBody struct {
	Value string `json:"value" validate:"required"`
}

func (s *DummyService) RegisterRoutes(mux *http.ServeMux) {
	tashkewey.AddRoute(mux, s.handleDummy())
}

func (s *DummyService) handleDummy() tashkewey.Route {
	return tashkewey.Route{
		Method:             http.MethodPost,
		Uri:                "/dummy",
		RequiredPermission: tashkewey.AllowPublic{},
		Handler: tashkewey.DataInOutMiddleware(func(r *http.Request, body *DummyBody) (*DummyBody, error) {
			return body, nil
		}),
	}
}

func main() {
	// init handlers
	monitor := tashkewey.NewMonitor()
	dummy := &DummyService{}

	// setup the server
	server := tashkewey.NewTashkewey(":9000", tashkewey.TashkeweyOptions{
		CorsConfig: &tashkewey.CorsConfig{
			AllowedOrigins: []string{"http://localhost:4200"},
		},
	})

	// register routes/handlers
	monitor.RegisterRoutes(server.Mux())
	dummy.RegisterRoutes(server.Mux())

	// add a prefix alias (useful for aws alb)
	server.AddPrefix("/demo/")
	server.ListenAndServe()
}
