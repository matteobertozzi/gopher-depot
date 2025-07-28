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

package metrics

import (
	"sort"
	"sync"
	"time"
)

type Event struct {
	Key       string
	Value     int64
	Timestamp time.Time
	TraceID   string
}

type Entry struct {
	Key          string   `json:"key"`
	MaxTimestamp int64    `json:"maxTimestamp"`
	MaxValue     int64    `json:"maxValue"`
	MinValue     int64    `json:"minValue"`
	Sum          int64    `json:"sum"`
	SumSquares   int64    `json:"sumSquares"`
	Count        int64    `json:"count"`
	TraceIDs     []string `json:"traceIds"`
}

type TraceHeap []struct {
	TraceID string
	Value   int64
}

func (h TraceHeap) Len() int           { return len(h) }
func (h TraceHeap) Less(i, j int) bool { return h[i].Value < h[j].Value }
func (h TraceHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *TraceHeap) Push(x any) {
	*h = append(*h, x.(struct {
		TraceID string
		Value   int64
	}))
}

func (h *TraceHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

type TopKTable struct {
	mu           sync.RWMutex
	entries      map[string]*Entry
	events       []Event
	k            int
	traceK       int
	windowSize   time.Duration
	lastCleanup  time.Time
	cleanupEvery time.Duration
}

func NewTopKTable(k, traceK int, windowSize time.Duration) *TopKTable {
	if k <= 0 {
		k = 10
	}
	if traceK <= 0 {
		traceK = 5
	}
	if windowSize <= 0 {
		windowSize = time.Minute
	}

	return &TopKTable{
		entries:      make(map[string]*Entry),
		events:       make([]Event, 0),
		k:            k,
		traceK:       traceK,
		windowSize:   windowSize,
		lastCleanup:  time.Now(),
		cleanupEvery: windowSize / 10, // cleanup every 10% of window
	}
}

func (t *TopKTable) AddEvent(timestamp time.Time, key string, value int64, traceID string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	event := Event{
		Key:       key,
		Value:     value,
		Timestamp: timestamp,
		TraceID:   traceID,
	}

	t.events = append(t.events, event)

	// Clean up expired events periodically
	if timestamp.Sub(t.lastCleanup) > t.cleanupEvery {
		t.cleanupExpiredEvents(timestamp)
		t.lastCleanup = timestamp
	}

	t.updateEntry(event)
}

func (t *TopKTable) cleanupExpiredEvents(now time.Time) {
	cutoff := now.Add(-t.windowSize)
	validEvents := make([]Event, 0, len(t.events)/2) // Pre-allocate with estimate

	for _, event := range t.events {
		if event.Timestamp.After(cutoff) {
			validEvents = append(validEvents, event)
		}
	}

	// Limit memory growth
	if len(validEvents) > 100000 {
		// Keep only the most recent events
		validEvents = validEvents[len(validEvents)-100000:]
	}

	t.events = validEvents
	t.rebuildEntries()
}

func (t *TopKTable) rebuildEntries() {
	t.entries = make(map[string]*Entry)

	for _, event := range t.events {
		t.updateEntry(event)
	}
}

func (t *TopKTable) updateEntry(event Event) {
	entry, exists := t.entries[event.Key]
	if !exists {
		entry = &Entry{
			Key:          event.Key,
			MaxTimestamp: event.Timestamp.Unix(),
			MaxValue:     event.Value,
			MinValue:     event.Value,
			TraceIDs:     make([]string, 0),
		}
		t.entries[event.Key] = entry
	}

	// Update aggregated values
	entry.Sum += event.Value
	entry.SumSquares += event.Value * event.Value
	entry.Count++

	if event.Value > entry.MaxValue {
		entry.MaxValue = event.Value
		entry.MaxTimestamp = event.Timestamp.UnixMilli()
	}

	if event.Value < entry.MinValue {
		entry.MinValue = event.Value
	}

	// Update top-k trace IDs
	t.updateTopKTraceIDs(entry)
}

func (t *TopKTable) updateTopKTraceIDs(entry *Entry) {
	// Build map of trace values for this key
	traceValues := make(map[string]int64)

	// Collect all trace values for this key
	for _, event := range t.events {
		if event.Key == entry.Key {
			if existing, exists := traceValues[event.TraceID]; !exists || event.Value > existing {
				traceValues[event.TraceID] = event.Value
			}
		}
	}

	// Convert to slice for sorting
	type traceItem struct {
		TraceID string
		Value   int64
	}

	traces := make([]traceItem, 0, len(traceValues))
	for tid, val := range traceValues {
		traces = append(traces, traceItem{TraceID: tid, Value: val})
	}

	// Sort by value descending
	sort.Slice(traces, func(i, j int) bool {
		return traces[i].Value > traces[j].Value
	})

	// Keep only top-k
	if len(traces) > t.traceK {
		traces = traces[:t.traceK]
	}

	// Extract trace IDs
	result := make([]string, len(traces))
	for i, trace := range traces {
		result[i] = trace.TraceID
	}

	entry.TraceIDs = result
}

// GetTopK returns the top-k entries sorted by max value
func (t *TopKTable) GetTopK() []Entry {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Clean up if needed
	now := time.Now()
	if now.Sub(t.lastCleanup) > t.cleanupEvery {
		t.cleanupExpiredEvents(now)
		t.lastCleanup = now
	}

	// Convert to slice and sort
	entries := make([]Entry, 0, len(t.entries))
	for _, entry := range t.entries {
		entries = append(entries, *entry)
	}

	// Sort by max value (descending) using efficient sort
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].MaxValue > entries[j].MaxValue
	})

	// Return top-k
	if len(entries) > t.k {
		entries = entries[:t.k]
	}

	return entries
}

// TopKTableta represents the serializable format
type TopKTableData struct {
	Entries []Entry `json:"entries"`
}

func (t *TopKTable) Type() string {
	return "TOPK"
}

// MarshalJSON implements json.Marshaler
func (t *TopKTable) Snapshot() any {
	entries := t.GetTopK()
	return TopKTableData{
		Entries: entries,
	}
}

func NewTopKTableFromData(data TopKTableData) *TopKTable {
	if len(data.Entries) == 0 {
		return nil
	}

	t := &TopKTable{
		entries:      make(map[string]*Entry, len(data.Entries)),
		k:            len(data.Entries), // Default k to number of entries
		traceK:       5,                 // Default traceK
		windowSize:   time.Hour,         // Default window
		lastCleanup:  time.Now(),
		cleanupEvery: time.Minute,
	}

	for _, entry := range data.Entries {
		entryCopy := entry // Copy to avoid pointer issues
		t.entries[entry.Key] = &entryCopy
	}

	return t
}
