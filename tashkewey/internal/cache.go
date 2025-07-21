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
package internal

import (
	"sync"
	"time"
)

type cacheEntry[T any] struct {
	value     T
	expiresAt time.Time
	size      int64
}

type Cache[T any] struct {
	mu          sync.RWMutex
	entries     map[string]*cacheEntry[T]
	currentSize int64
	maxSize     int64
	ttl         time.Duration
	sizeFunc    func(T) int64
}

func NewCache[T any](maxSizeMB int, ttl time.Duration, sizeFunc func(T) int64) *Cache[T] {
	return &Cache[T]{
		entries:  make(map[string]*cacheEntry[T]),
		maxSize:  int64(maxSizeMB) * 1024 * 1024,
		ttl:      ttl,
		sizeFunc: sizeFunc,
	}
}

func (c *Cache[T]) Get(key string) (T, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	entry, exists := c.entries[key]
	if !exists || time.Now().After(entry.expiresAt) {
		var zero T
		return zero, false
	}

	return entry.value, true
}

func (c *Cache[T]) Set(key string, value T) {
	c.mu.Lock()
	defer c.mu.Unlock()

	size := c.sizeFunc(value)

	c.cleanupExpired()

	for c.currentSize+size > c.maxSize && len(c.entries) > 0 {
		c.evictOldest()
	}

	if size > c.maxSize {
		return
	}

	if existing, exists := c.entries[key]; exists {
		c.currentSize -= existing.size
	}

	entry := &cacheEntry[T]{
		value:     value,
		expiresAt: time.Now().Add(c.ttl),
		size:      size,
	}

	c.entries[key] = entry
	c.currentSize += size
}

func (c *Cache[T]) cleanupExpired() {
	now := time.Now()
	for key, entry := range c.entries {
		if now.After(entry.expiresAt) {
			c.currentSize -= entry.size
			delete(c.entries, key)
		}
	}
}

func (c *Cache[T]) evictOldest() {
	var oldestKey string
	var oldestTime time.Time

	for key, entry := range c.entries {
		if oldestKey == "" || entry.expiresAt.Before(oldestTime) {
			oldestKey = key
			oldestTime = entry.expiresAt
		}
	}

	if oldestKey != "" {
		c.currentSize -= c.entries[oldestKey].size
		delete(c.entries, oldestKey)
	}
}
