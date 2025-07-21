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
	"sync"
	"time"
)

type TimeRangeCounter struct {
	mu           sync.RWMutex
	window       time.Duration
	interval     time.Duration
	counters     []int64
	lastInterval int64
	size         int
}

func NewTimeRangeCounter(window, interval time.Duration) *TimeRangeCounter {
	if window <= 0 {
		window = time.Hour
	}
	if interval <= 0 {
		interval = time.Minute
	}
	
	size := int(window / interval)
	if size <= 0 {
		size = 1
	}
	if size > 10000 { // Prevent excessive memory usage
		size = 10000
		interval = window / time.Duration(size)
	}

	now := time.Now()
	alignedTime := alignToInterval(now, interval)

	return &TimeRangeCounter{
		window:       window,
		interval:     interval,
		counters:     make([]int64, size),
		lastInterval: alignedTime.UnixMilli(),
		size:         size,
	}
}

func (trc *TimeRangeCounter) updateToCurrentTime(now time.Time) {
	alignedNow := alignToInterval(now, trc.interval)
	nowMs := alignedNow.UnixMilli()
	if nowMs <= trc.lastInterval {
		return
	}

	intervalMs := trc.interval.Milliseconds()
	intervalsPassed := (nowMs - trc.lastInterval) / intervalMs

	if intervalsPassed >= int64(trc.size) {
		// Clear all counters if we've passed the entire window
		for i := range trc.counters {
			trc.counters[i] = 0
		}
	} else {
		// Shift and clear the appropriate number of slots
		for range intervalsPassed {
			trc.counters = append(trc.counters[1:], 0)
		}
	}

	trc.lastInterval = nowMs
}

func (trc *TimeRangeCounter) getIndex(timestamp time.Time) (int, bool) {
	alignedTime := alignToInterval(timestamp, trc.interval)
	timestampMs := alignedTime.UnixMilli()

	// Check if timestamp is within the window
	oldestAllowed := trc.lastInterval - int64(trc.size-1)*trc.interval.Milliseconds()
	if timestampMs < oldestAllowed || timestampMs > trc.lastInterval {
		return 0, false
	}

	// Calculate index in ring buffer
	intervalsPassed := (trc.lastInterval - timestampMs) / trc.interval.Milliseconds()
	index := trc.size - 1 - int(intervalsPassed)

	return index, true
}

func (trc *TimeRangeCounter) Inc(timestamp time.Time) {
	trc.Add(timestamp, 1)
}

func (trc *TimeRangeCounter) Add(timestamp time.Time, amount int64) {
	trc.mu.Lock()
	defer trc.mu.Unlock()

	now := time.Now()
	trc.updateToCurrentTime(now)

	// Check if timestamp is too old
	if now.Sub(timestamp) > trc.window {
		return // Ignore old timestamps
	}

	index, valid := trc.getIndex(timestamp)
	if !valid {
		return
	}

	trc.counters[index] += amount
}

func (trc *TimeRangeCounter) Sum() int64 {
	trc.mu.Lock()
	defer trc.mu.Unlock()

	trc.updateToCurrentTime(time.Now())

	var sum int64
	for _, count := range trc.counters {
		sum += count
	}
	return sum
}

type TimeRangeCounterData struct {
	Window       int64   `json:"window"`
	LastInterval int64   `json:"lastInterval"`
	Counters     []int64 `json:"counters"`
}

func (m *TimeRangeCounter) Type() string {
	return "TIME_RANGE_COUNTER"
}

func (trc *TimeRangeCounter) Snapshot() any {
	trc.mu.Lock()
	defer trc.mu.Unlock()

	trc.updateToCurrentTime(time.Now())

	data := TimeRangeCounterData{
		Window:       trc.window.Milliseconds(),
		LastInterval: trc.lastInterval,
		Counters:     make([]int64, len(trc.counters)),
	}
	copy(data.Counters, trc.counters)

	return data
}

func NewTimeRangeCounterFromData(data TimeRangeCounterData) *TimeRangeCounter {
	if data.Window <= 0 {
		return nil
	}
	if len(data.Counters) == 0 {
		return nil
	}
	
	window := time.Duration(data.Window) * time.Millisecond
	trc := &TimeRangeCounter{
		window:       window,
		interval:     window / time.Duration(len(data.Counters)),
		lastInterval: data.LastInterval,
		counters:     make([]int64, len(data.Counters)),
		size:         len(data.Counters),
	}

	copy(trc.counters, data.Counters)

	return trc
}
