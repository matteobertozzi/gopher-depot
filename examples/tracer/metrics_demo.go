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
	"context"
	"time"

	"github.com/matteobertozzi/gopher-depot/insights/tracer"
)

func main() {
	ctx := context.WithValue(context.Background(), "TraceId", tracer.GenerateTraceId())

	tracer.LogInfo(ctx, "Starting metrics demo")

	// Simulate some application metrics
	tags := map[string]any{
		"service":     "demo-app",
		"version":     "1.2.3",
		"environment": "production",
		"healthy":     true,
	}

	measurements := map[string]any{
		"request.count":        150,
		"response.time.ms":     45.7,
		"memory.usage.bytes":   2048576,
		"cpu.usage.ns":         1500000000,
		"cache.hit.count":      120,
		"cache.miss.count":     30,
		"database.queries.sec": 2.5,
		"error.rate":           0.05,
	}

	tracer.EmitMetricsEvent(ctx, tags, measurements)

	// Simulate another metrics event with different data
	time.Sleep(100 * time.Millisecond)

	apiTags := map[string]any{
		"endpoint": "/api/users",
		"method":   "GET",
		"cached":   false,
	}

	apiMeasurements := map[string]any{
		"latency.ms":       23.4,
		"payload.bytes":    512,
		"status.code":      200,
		"db.query.time.us": 850,
	}

	tracer.EmitMetricsEvent(ctx, apiTags, apiMeasurements)

	tracer.LogInfo(ctx, "Metrics demo completed")
}