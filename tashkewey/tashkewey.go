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
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/matteobertozzi/gopher-depot/insights/tracer"
)

type Tashkewey struct {
	server  *http.Server
	mux     *http.ServeMux
	healthy int32
}

type TashkeweyOptions struct {
	CorsConfig *CorsConfig
}

func NewTashkewey(addr string, options TashkeweyOptions) *Tashkewey {
	mux := http.NewServeMux()

	// middleware should be in reverse order...
	var handler http.Handler = mux
	if options.CorsConfig != nil {
		handler = CorsMiddleware(options.CorsConfig)(handler)
	}
	handler = TracingMiddleware(handler)

	return &Tashkewey{
		mux: mux,
		server: &http.Server{
			Addr:         addr,
			Handler:      handler,
			ReadTimeout:  5 * time.Second,
			WriteTimeout: 10 * time.Second,
			IdleTimeout:  15 * time.Second,
		},
	}
}

func (s *Tashkewey) IsHealthy() bool {
	return atomic.LoadInt32(&(s.healthy)) == 1
}

func (s *Tashkewey) AddPrefix(prefix string) {
	s.mux.Handle(prefix, http.StripPrefix(prefix[:len(prefix)-1], s.mux))
}

func (s *Tashkewey) Mux() *http.ServeMux {
	return s.mux
}

func (s *Tashkewey) Handle(pattern string, handler http.Handler) {
	s.mux.Handle(pattern, handler)
}

func (s *Tashkewey) HandleFunc(pattern string, handler func(http.ResponseWriter, *http.Request)) {
	s.mux.HandleFunc(pattern, handler)
}

func (s *Tashkewey) ListenAndServe() {
	done := make(chan bool)
	quit := make(chan os.Signal, syscall.SIGHUP)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	atomic.StoreInt32(&(s.healthy), 1)
	go func() {
		<-quit
		tracer.LogInfo(context.Background(), "Server is shutting down")
		atomic.StoreInt32(&(s.healthy), 0)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)

		defer cancel()
		s.server.SetKeepAlivesEnabled(false)
		if err := s.server.Shutdown(ctx); err != nil {
			tracer.LogError(context.Background(), err, "Could not gracefully shutdown the server")
		}
		close(done)
	}()

	tracer.LogInfo(context.Background(), "Server Listening on {address}", s.server.Addr)
	if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		tracer.LogError(context.Background(), err, "Could not listen on {address}", s.server.Addr)
	}

	<-done
	tracer.LogInfo(context.Background(), "Server stopped")
}
