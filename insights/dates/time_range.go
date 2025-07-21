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

package dates

import (
	"iter"
	"time"
)

type TimeRange struct {
	StartTime      int64                   `json:"startTime"`
	EndTime        int64                   `json:"endTime"`
	WindowInterval TimeWindowGroupInterval `json:"windowInterval"`
}

func NewTimeRange(startDate, endDate time.Time, windowInterval *TimeWindowGroupInterval) TimeRange {
	startTime := startDate.UnixMilli()
	endTime := endDate.UnixMilli()

	var interval TimeWindowGroupInterval
	if windowInterval != nil {
		interval = *windowInterval
	} else {
		interval = TimeWindowGroupIntervalForTimeDelta(endTime - startTime)
	}

	return TimeRange{
		StartTime:      startTime,
		EndTime:        endTime,
		WindowInterval: interval,
	}
}

func DatesForTimeRange(timeRange TimeRange) iter.Seq[time.Time] {
	endTime := timeRange.EndTime
	d := time.UnixMilli(timeRange.StartTime)

	return func(yield func(time.Time) bool) {
		switch timeRange.WindowInterval {
		case SECOND, MINUTE, HOUR, DAY, WEEK:
			windowMs, _ := WindowMsForGroupInterval(timeRange.WindowInterval, false)
			interval := time.Duration(windowMs) * time.Millisecond
			for d.UnixMilli() < endTime {
				if !yield(d) {
					return
				}
				d = d.Add(interval)
			}
		case MONTH:
			d := time.Date(d.Year(), d.Month(), 1, 0, 0, 0, 0, time.UTC)
			for d.UnixMilli() < endTime {
				if !yield(d) {
					return
				}
				d = d.AddDate(0, 1, 0)
			}
		case YEAR:
			d := time.Date(d.Year(), 1, 1, 0, 0, 0, 0, time.UTC)
			for d.UnixMilli() < endTime {
				if !yield(d) {
					return
				}
				d = d.AddDate(1, 0, 0)
			}
		default:
			panic("Invalid Interval: " + string(timeRange.WindowInterval))
		}
	}
}
