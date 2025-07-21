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

package humans

import (
	"fmt"
	"math"
	"strings"
)

func TimeNanos(nanos float64) string {
	if nanos < 1000 {
		if nanos < 0 {
			return "unknown"
		}
		return fmt.Sprintf("%.0fns", nanos)
	}
	return TimeMicros(nanos / 1000)
}

func TimeMicros(usec float64) string {
	if usec < 1000 {
		if usec < 0 {
			return "unknown"
		}
		return fmt.Sprintf("%.0fus", usec)
	}
	return TimeMillis(usec / 1000)
}

func TimeMillis(millis float64) string {
	if millis < 1000 {
		if millis < 0 {
			return "unknown"
		}
		if millis != math.Trunc(millis) {
			return fmt.Sprintf("%.2fms", millis)
		}
		return fmt.Sprintf("%.0fms", millis)
	}
	return TimeSeconds(millis / 1000)
}

func TimeSeconds(sec float64) string {
	if sec < 60 {
		if sec < 0 {
			return "unknown"
		}
		return fmt.Sprintf("%.2fsec", sec)
	}

	hours := math.Floor(sec / (60 * 60))
	rem := math.Mod(sec, (60 * 60))
	minutes := math.Floor(rem / 60)
	seconds := math.Mod(rem, 60)

	var buf strings.Builder

	if hours > 0 {
		buf.WriteString(fmt.Sprintf("%.0fhrs, ", hours))
	}
	if minutes > 0 {
		buf.WriteString(fmt.Sprintf("%.0fmin, ", minutes))
	}

	var humanTime string
	if seconds > 0 {
		buf.WriteString(fmt.Sprintf("%.2fsec", seconds))
		humanTime = buf.String()
	} else {
		sbuf := buf.String()
		if len(sbuf) >= 2 {
			humanTime = sbuf[:len(sbuf)-2] // Remove trailing ", "
		} else {
			humanTime = sbuf
		}
	}

	if hours > 24 {
		ndays := hours / 24.0
		return fmt.Sprintf("%s (%.1f days)", humanTime, ndays)
	}
	return humanTime
}
