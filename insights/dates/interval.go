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
	"errors"
)

type TimeWindowGroupInterval string

const (
	SECOND TimeWindowGroupInterval = "SECOND"
	MINUTE TimeWindowGroupInterval = "MINUTE"
	HOUR   TimeWindowGroupInterval = "HOUR"
	DAY    TimeWindowGroupInterval = "DAY"
	WEEK   TimeWindowGroupInterval = "WEEK"
	MONTH  TimeWindowGroupInterval = "MONTH"
	YEAR   TimeWindowGroupInterval = "YEAR"
)

func WindowMsForGroupInterval(interval TimeWindowGroupInterval, failUnsupported bool) (int64, error) {
	switch interval {
	case SECOND:
		return 1000, nil
	case MINUTE:
		return 60000, nil
	case HOUR:
		return 3600000, nil
	case DAY:
		return 86400000, nil
	case WEEK:
		return 604800000, nil
	case MONTH:
		if failUnsupported {
			return 0, errors.New("window for MONTH is not supported")
		}
		return -1, nil
	case YEAR:
		if failUnsupported {
			return 0, errors.New("window for YEAR is not supported")
		}
		return -1, nil
	default:
		return 0, errors.New("unhandled window for interval: " + string(interval))
	}
}

const (
	HOUR_MS  = 60 * 60 * 1000
	DAY_MS   = 24 * HOUR_MS
	MONTH_MS = 31 * DAY_MS
)

func TimeWindowGroupIntervalForTimeDelta(deltaTime int64) TimeWindowGroupInterval {
	if deltaTime >= (3 * MONTH_MS) {
		return MONTH // 3 months, 1month window
	} else if deltaTime >= (7 * DAY_MS) {
		return DAY // week, 1day window
	} else if deltaTime >= HOUR_MS {
		return HOUR // 1h window
	}
	return MINUTE
}
