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
	"sync"
	"sync/atomic"
	"time"

	"github.com/matteobertozzi/gopher-depot/insights/tracer"
)

type RateLimitConfig struct {
	RequestsPerMinute int
	BurstSize         int
}

func (rlc *RateLimitConfig) IsNotEmpty() bool {
	return rlc.RequestsPerMinute > 0 && rlc.BurstSize > 0
}

var NoRateLimit = RateLimitConfig{
	RequestsPerMinute: 0,
	BurstSize:         0,
}

type rateLimiter struct {
	tokens     int
	maxTokens  int
	refillRate int
	lastRefill time.Time
	mutex      sync.Mutex
	lastUsed   atomic.Int64
}

func newRateLimiter(requestsPerMinute, burstSize int) *rateLimiter {
	rl := &rateLimiter{
		tokens:     burstSize,
		maxTokens:  burstSize,
		refillRate: requestsPerMinute,
		lastRefill: time.Now(),
	}
	rl.lastUsed.Store(time.Now().UnixNano())
	return rl
}

func (rl *rateLimiter) allow() bool {
	rl.mutex.Lock()
	defer rl.mutex.Unlock()

	now := time.Now()
	elapsed := now.Sub(rl.lastRefill)

	tokensToAdd := int(elapsed.Minutes() * float64(rl.refillRate))
	if tokensToAdd > 0 {
		rl.tokens = min(rl.maxTokens, rl.tokens+tokensToAdd)
		rl.lastRefill = now
	}

	rl.lastUsed.Store(now.UnixNano())

	if rl.tokens > 0 {
		rl.tokens--
		return true
	}

	return false
}

type RateLimitMiddleware struct {
	config          RateLimitConfig
	limiters        map[string]*rateLimiter
	mutex           sync.RWMutex
	limiterTTL      time.Duration
	cleanupInterval time.Duration
}

func NewRateLimitMiddleware(config RateLimitConfig) *RateLimitMiddleware {
	middleware := &RateLimitMiddleware{
		config:          config,
		limiters:        make(map[string]*rateLimiter),
		limiterTTL:      30 * time.Minute,
		cleanupInterval: 5 * time.Minute,
	}
	go middleware.cleanupLoop()
	return middleware
}

func (rlm *RateLimitMiddleware) getLimiter(key string) *rateLimiter {
	rlm.mutex.RLock()
	limiter, exists := rlm.limiters[key]
	rlm.mutex.RUnlock()

	if exists {
		return limiter
	}

	rlm.mutex.Lock()
	defer rlm.mutex.Unlock()

	limiter, exists = rlm.limiters[key]
	if !exists {
		limiter = newRateLimiter(rlm.config.RequestsPerMinute, rlm.config.BurstSize)
		rlm.limiters[key] = limiter
	}

	return limiter
}

func (rlm *RateLimitMiddleware) getClientKey(r *http.Request) string {
	session := GetSession(r.Context())
	if session != nil && session.Token != "" {
		return "token:" + session.Token
	}

	return "ip:" + GetClientIP(r)
}

func (rlm *RateLimitMiddleware) middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		clientKey := rlm.getClientKey(r)
		limiter := rlm.getLimiter(clientKey)

		if !limiter.allow() {
			tracer.LogWarn(r.Context(), "Rate limit exceeded for client: {key}", clientKey)
			WriteErrorResponse(w, r, NewTooManyRequestsError(r.Context(), "RATE_LIMIT_EXCEEDED", "Rate limit exceeded"))
			return
		}

		next.ServeHTTP(w, r)
	})
}

func CreateRateLimitMiddleware(config RateLimitConfig) func(http.Handler) http.Handler {
	middleware := NewRateLimitMiddleware(config)
	return middleware.middleware
}

func (rlm *RateLimitMiddleware) cleanupLoop() {
	ticker := time.NewTicker(rlm.cleanupInterval)
	defer ticker.Stop()

	for range ticker.C {
		rlm.evictStaleLimiters()
	}
}

func (rlm *RateLimitMiddleware) evictStaleLimiters() {
	cutoff := time.Now().Add(-rlm.limiterTTL)

	rlm.mutex.Lock()
	defer rlm.mutex.Unlock()

	for key, limiter := range rlm.limiters {
		lastUsed := time.Unix(0, limiter.lastUsed.Load())
		if lastUsed.Before(cutoff) {
			delete(rlm.limiters, key)
		}
	}
}
