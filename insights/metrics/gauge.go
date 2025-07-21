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

type MaxAndAvgTimeRangeGauge struct {
	mu           sync.RWMutex
	window       time.Duration
	interval     time.Duration
	max          []int64
	sum          []int64
	count        []int64
	lastInterval int64
	size         int
}

func NewMaxAndAvgTimeRangeGauge(window, interval time.Duration) *MaxAndAvgTimeRangeGauge {
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

	return &MaxAndAvgTimeRangeGauge{
		window:       window,
		interval:     interval,
		max:          make([]int64, size),
		sum:          make([]int64, size),
		count:        make([]int64, size),
		lastInterval: alignedTime.UnixMilli(),
		size:         size,
	}
}

func (marc *MaxAndAvgTimeRangeGauge) updateToCurrentTime(now time.Time) {
	alignedNow := alignToInterval(now, marc.interval)
	nowMs := alignedNow.UnixMilli()
	if nowMs <= marc.lastInterval {
		return
	}

	intervalMs := marc.interval.Milliseconds()
	intervalsPassed := (nowMs - marc.lastInterval) / intervalMs

	if intervalsPassed >= int64(marc.size) {
		// Clear all data if we've passed the entire window
		for i := range marc.max {
			marc.max[i] = 0
			marc.sum[i] = 0
			marc.count[i] = 0
		}
	} else {
		// Shift and clear the appropriate number of slots
		for range intervalsPassed {
			marc.max = append(marc.max[1:], 0)
			marc.sum = append(marc.sum[1:], 0)
			marc.count = append(marc.count[1:], 0)
		}
	}

	marc.lastInterval = nowMs
}

func (marc *MaxAndAvgTimeRangeGauge) getIndex(timestamp time.Time) (int, bool) {
	alignedTime := alignToInterval(timestamp, marc.interval)
	timestampMs := alignedTime.UnixMilli()

	// Check if timestamp is within the window
	oldestAllowed := marc.lastInterval - int64(marc.size-1)*marc.interval.Milliseconds()
	if timestampMs < oldestAllowed || timestampMs > marc.lastInterval {
		return 0, false
	}

	// Calculate index in ring buffer
	intervalsPassed := (marc.lastInterval - timestampMs) / marc.interval.Milliseconds()
	index := marc.size - 1 - int(intervalsPassed)

	return index, true
}

func (marc *MaxAndAvgTimeRangeGauge) Sample(timestamp time.Time, value int64) {
	marc.mu.Lock()
	defer marc.mu.Unlock()

	now := time.Now()
	marc.updateToCurrentTime(now)

	// Check if timestamp is too old
	if now.Sub(timestamp) > marc.window {
		return // Ignore old timestamps
	}

	index, valid := marc.getIndex(timestamp)
	if !valid {
		return
	}

	// Update max
	if marc.count[index] == 0 || value > marc.max[index] {
		marc.max[index] = value
	}

	// Update sum and count
	marc.sum[index] += value
	marc.count[index]++
}

func (marc *MaxAndAvgTimeRangeGauge) GetMax() int64 {
	marc.mu.Lock()
	defer marc.mu.Unlock()

	marc.updateToCurrentTime(time.Now())

	max := int64(0)
	for i, count := range marc.count {
		if count > 0 && marc.max[i] > max {
			max = marc.max[i]
		}
	}
	return max
}

func (marc *MaxAndAvgTimeRangeGauge) GetAvg() float64 {
	marc.mu.Lock()
	defer marc.mu.Unlock()

	marc.updateToCurrentTime(time.Now())

	totalSum := int64(0)
	totalCount := int64(0)

	for i, count := range marc.count {
		if count > 0 {
			totalSum += marc.sum[i]
			totalCount += count
		}
	}

	if totalCount == 0 {
		return 0
	}

	return float64(totalSum) / float64(totalCount)
}

func (marc *MaxAndAvgTimeRangeGauge) GetTotalCount() int64 {
	marc.mu.Lock()
	defer marc.mu.Unlock()

	marc.updateToCurrentTime(time.Now())

	var total int64
	for _, count := range marc.count {
		total += count
	}
	return total
}

type MaxAndAvgTimeRangeGaugeData struct {
	Window       int64   `json:"window"`
	LastInterval int64   `json:"lastInterval"`
	Max          []int64 `json:"max"`
	Sum          []int64 `json:"sum"`
	Count        []int64 `json:"count"`
}

func (m *MaxAndAvgTimeRangeGauge) Type() string {
	return "MAX_AVG_TIME_RANGE_GAUGE"
}

func (marc *MaxAndAvgTimeRangeGauge) Snapshot() any {
	marc.mu.Lock()
	defer marc.mu.Unlock()

	marc.updateToCurrentTime(time.Now())

	data := MaxAndAvgTimeRangeGaugeData{
		Window:       marc.window.Milliseconds(),
		LastInterval: marc.lastInterval,
		Max:          make([]int64, len(marc.max)),
		Sum:          make([]int64, len(marc.sum)),
		Count:        make([]int64, len(marc.count)),
	}

	copy(data.Max, marc.max)
	copy(data.Sum, marc.sum)
	copy(data.Count, marc.count)
	return data
}

func NewMaxAndAvgTimeRangeGaugeData(data MaxAndAvgTimeRangeGaugeData) *MaxAndAvgTimeRangeGauge {
	if data.Window <= 0 {
		return nil
	}
	if len(data.Max) == 0 || len(data.Sum) == 0 || len(data.Count) == 0 {
		return nil
	}
	if len(data.Max) != len(data.Sum) || len(data.Max) != len(data.Count) {
		return nil
	}
	
	window := time.Duration(data.Window) * time.Millisecond
	marc := &MaxAndAvgTimeRangeGauge{
		window:       window,
		interval:     window / time.Duration(len(data.Max)),
		lastInterval: data.LastInterval,
		size:         len(data.Max),
		max:          make([]int64, len(data.Max)),
		sum:          make([]int64, len(data.Sum)),
		count:        make([]int64, len(data.Count)),
	}

	copy(marc.max, data.Max)
	copy(marc.sum, data.Sum)
	copy(marc.count, data.Count)

	return marc
}
