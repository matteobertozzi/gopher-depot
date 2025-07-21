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

package tracer

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"time"
)

const kTraceIdCtxKey = "TraceId"

func GetTraceId(ctx context.Context) string {
	traceId, _ := ctx.Value(kTraceIdCtxKey).(string)
	return traceId
}

func GenerateTraceId() string {
	var buf [16]byte
	buffer := buf[:]
	rand.Read(buffer)
	putDateTime40(buffer, time.Now().UnixMilli())
	return hex.EncodeToString(buffer)
}

func ParseTraceId(traceId string) ([]byte, error) {
	return hex.DecodeString(traceId)
}

func GenerateSpanId() string {
	var buf [8]byte
	buffer := buf[:]
	rand.Read(buffer)
	return hex.EncodeToString(buffer)
}

func ParseSpanId(spanId string) ([]byte, error) {
	return hex.DecodeString(spanId)
}

func putDateTime40(buffer []byte, timestamp int64) {
	buffer[0] = byte((timestamp >> 40) & 0xff)
	buffer[1] = byte((timestamp >> 32) & 0xff)
	buffer[2] = byte((timestamp >> 24) & 0xff)
	buffer[3] = byte((timestamp >> 16) & 0xff)
	buffer[4] = byte((timestamp >> 8) & 0xff)
	buffer[5] = byte(timestamp & 0xff)
}
