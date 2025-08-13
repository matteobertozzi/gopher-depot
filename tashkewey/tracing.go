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
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/matteobertozzi/gopher-depot/insights/metrics"
	"github.com/matteobertozzi/gopher-depot/insights/tracer"
)

var httpRequests = metrics.RegisterMetric[*metrics.TimeRangeCounter](metrics.Metric{
	Unit:      "COUNT",
	Name:      "http.requests",
	Collector: metrics.NewTimeRangeCounter(3*time.Hour, 1*time.Minute),
})

var http5xx = metrics.RegisterMetric[*metrics.TimeRangeCounter](metrics.Metric{
	Unit:      "COUNT",
	Name:      "http.response.5xx",
	Collector: metrics.NewTimeRangeCounter(3*time.Hour, 1*time.Minute),
})

var httpExecTimes = metrics.RegisterMetric[*metrics.MaxAndAvgTimeRangeGauge](metrics.Metric{
	Unit:      "TIME_NS",
	Name:      "http.exec.times",
	Collector: metrics.NewMaxAndAvgTimeRangeGauge(3*time.Hour, 1*time.Minute),
})

var httpTop = metrics.RegisterMetric[*metrics.TopKTable](metrics.Metric{
	Unit:      "TIME_NS",
	Name:      "http.top.exec.time",
	Collector: metrics.NewTopKTable(16, 5, 60*time.Minute),
})

var httpTopIps = metrics.RegisterMetric[*metrics.TopKTable](metrics.Metric{
	Unit:      "TIME_NS",
	Name:      "http.top.ip.exec.time",
	Collector: metrics.NewTopKTable(16, 5, 60*time.Minute),
})

type responseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}

func TracingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		startTime := time.Now()
		traceId := tracer.GenerateTraceId()
		httpRequests.Inc(time.Now())

		clientIp := GetClientIP(r)
		ctx := context.WithValue(r.Context(), "TraceId", traceId)
		w.Header().Set("X-Request-Id", traceId)

		ww := &responseWriter{ResponseWriter: w, statusCode: 200}
		next.ServeHTTP(ww, r.WithContext(ctx))

		endTime := time.Now()
		execNs := endTime.Sub(startTime).Nanoseconds()
		tracer.LogTrace(ctx, "{http.status} {http.method} {http.path} {ip.addr} {duration.ns}",
			ww.statusCode, r.Method, r.URL.Path, clientIp, execNs)

		httpTop.AddEvent(endTime, r.Method+" "+r.URL.Path, execNs, traceId)
		httpTopIps.AddEvent(endTime, clientIp, execNs, traceId)
		httpExecTimes.Sample(endTime, execNs)
		if ww.statusCode >= 500 && ww.statusCode < 600 {
			http5xx.Inc(endTime)
		}
	})
}

func GetClientIP(r *http.Request) string {
	ip := strings.TrimSpace(r.Header.Get("X-Forwarded-For"))
	if ip == "" {
		ip = strings.TrimSpace(r.Header.Get("X-Real-IP"))
	}
	if ip == "" {
		ip, _, _ = net.SplitHostPort(r.RemoteAddr)
	}
	return ip
}
