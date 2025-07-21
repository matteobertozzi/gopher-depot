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
)

func Bits(bits float64) string {
	if bits >= 1000_000_000_000_000 {
		return fmt.Sprintf("%.0fPbit", bits/1000_000_000_000_000)
	}
	if bits >= 1000_000_000_000 {
		return fmt.Sprintf("%.0fTbit", bits/1000_000_000_000)
	}
	if bits >= 1000_000_000 {
		return fmt.Sprintf("%.0fGbit", bits/1000_000_000)
	}
	if bits >= 1000_000 {
		return fmt.Sprintf("%.0fMbit", bits/1000_000)
	}
	if bits >= 1000 {
		return fmt.Sprintf("%.0fKbit", bits/1000)
	}
	return fmt.Sprintf("%.0fbit", bits)
}

func Bytes(bytes float64) string {
	if bytes >= 1125899906842624 {
		return fmt.Sprintf("%.2fPiB", bytes/1125899906842624)
	}
	if bytes >= 1099511627776 {
		return fmt.Sprintf("%.2fTiB", bytes/1099511627776)
	}
	if bytes >= 1073741824 {
		return fmt.Sprintf("%.2fGiB", bytes/1073741824)
	}
	if bytes >= 1048576 {
		return fmt.Sprintf("%.2fMiB", bytes/1048576)
	}
	if bytes >= 1024 {
		return fmt.Sprintf("%.2fKiB", bytes/1024)
	}
	if bytes > 1 {
		return fmt.Sprintf("%.0fbytes", math.Round(bytes))
	}
	return fmt.Sprintf("%.0fbyte", bytes)
}
