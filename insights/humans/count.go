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
)

func Count(count float64) string {
	if count >= 1000000000 {
		return fmt.Sprintf("%.2fB", count/1000000000)
	}
	if count >= 1000000 {
		return fmt.Sprintf("%.2fM", count/1000000)
	}
	if count >= 1000 {
		return fmt.Sprintf("%.2fk", count/1000)
	}
	return fmt.Sprintf("%.0f", count)
}
