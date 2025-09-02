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
	"encoding/json"
	"fmt"
	"log"
	"math"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"text/tabwriter"
	"time"

	"github.com/matteobertozzi/gopher-depot/insights/humans"
)

// Color constants to avoid string concatenation
const (
	colorReset   = "\x1b[0m"
	colorYellow  = "\x1b[33m"
	colorRed     = "\x1b[31m"
	colorMagenta = "\x1b[35m"
	colorCyan    = "\x1b[36m"
)

var (
	// Pool for string builders to reduce allocations
	stringBuilderPool = sync.Pool{
		New: func() interface{} {
			return &strings.Builder{}
		},
	}
)

type consoleLogger struct{}

func NewConsoleLogger() *consoleLogger {
	return &consoleLogger{}
}

// getStringBuilder gets a string builder from the pool
func getStringBuilder() *strings.Builder {
	return stringBuilderPool.Get().(*strings.Builder)
}

// putStringBuilder returns a string builder to the pool
func putStringBuilder(sb *strings.Builder) {
	sb.Reset()
	stringBuilderPool.Put(sb)
}

func (l *consoleLogger) EmitLogEvent(context context.Context, level string, errorMessage string, format string, args []any) {
	sb := getStringBuilder()
	defer putStringBuilder(sb)

	traceId := GetTraceId(context)
	message := consoleHumanLogFormat(format, args)
	now := time.Now().Format(time.RFC3339)
	file, fileLine, funcName := getCallerFuncFileLine(3)

	// Build the log message efficiently
	sb.WriteString(consoleLevelColorText(level, now))
	sb.WriteString(" [")
	sb.WriteString(consoleLevelColorText(level, traceId))
	sb.WriteString("] ")
	sb.WriteString(file)
	sb.WriteByte(':')
	sb.WriteString(strconv.Itoa(fileLine))
	sb.WriteByte(' ')
	sb.WriteString(funcName)
	sb.WriteString("() ")
	sb.WriteString(consoleLevelColorText(level, level))
	sb.WriteByte(' ')
	sb.WriteString(message)

	if errorMessage != "" {
		sb.WriteString(": ")
		sb.WriteString(errorMessage)
	}

	log.Print(sb.String())
}

func (l *consoleLogger) EmitMetricsEvent(ctx context.Context, event *MetricsEvent) {
	fmt.Printf("\n=== METRICS EVENT ===\n")
	fmt.Printf("Timestamp: %s\n", event.Timestamp.Format(time.RFC3339))
	fmt.Printf("TraceId:   %s\n", event.TraceId)
	
	if len(event.Tags) > 0 || len(event.Measurements) > 0 {
		fmt.Printf("\nMetrics Data:\n")
		
		// Create a tabwriter for aligned output
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		
		// Add tags first
		if len(event.Tags) > 0 {
			for key, value := range event.Tags {
				formatted := HumanFormatFieldValue(key, value)
				fmt.Fprintf(w, "%s\t%s\t(tag)\n", key, formatted)
			}
		}
		
		// Add measurements
		if len(event.Measurements) > 0 {
			for key, value := range event.Measurements {
				formatted := HumanFormatFieldValue(key, value)
				fmt.Fprintf(w, "%s\t%s\t(measurement)\n", key, formatted)
			}
		}
		
		w.Flush()
	}
	
	fmt.Printf("====================\n\n")
}

func consoleLevelColorText(level string, text string) string {
	switch level {
	case "WARN":
		return colorYellow + text + colorReset
	case "ERROR":
		return colorRed + text + colorReset
	}
	return text
}

func HumanMeasurementValue(name string, value float64) string {
	suffixIndex := strings.LastIndex(name, ".")
	if suffixIndex > 0 {
		suffix := name[suffixIndex+1:]
		switch suffix {
		case "ns":
			return humans.TimeNanos(value)
		case "ms":
			return humans.TimeMillis(value)
		case "us":
			return humans.TimeMicros(value)
		case "sec":
			return humans.TimeSeconds(value)
		case "bytes":
			return humans.Bytes(uint64(value))
		case "count":
			return humans.Count(value)
		case "bits":
			return humans.Bits(uint64(value))
		case "epoch":
			return time.Unix(int64(value), 0).Format("2006-01-02 15:04:05")
		}
	}

	// Check if value is effectively an integer
	if math.Trunc(value) == value {
		return strconv.FormatFloat(value, 'f', 0, 64)
	}
	return strconv.FormatFloat(value, 'f', 2, 64)
}

func HumanFormatFieldValue(name string, value any) string {
	if value == nil {
		return "null"
	}

	// Fast path for common types to avoid reflection
	switch v := value.(type) {
	case string:
		return v
	case bool:
		if v {
			return "true"
		}
		return "false"
	case int:
		return HumanMeasurementValue(name, float64(v))
	case int32:
		return HumanMeasurementValue(name, float64(v))
	case int64:
		return HumanMeasurementValue(name, float64(v))
	case uint:
		return HumanMeasurementValue(name, float64(v))
	case uint32:
		return HumanMeasurementValue(name, float64(v))
	case uint64:
		return HumanMeasurementValue(name, float64(v))
	case float32:
		return HumanMeasurementValue(name, float64(v))
	case float64:
		return HumanMeasurementValue(name, v)
	}

	// Fallback to reflection for complex types
	rv := reflect.ValueOf(value)
	switch rv.Kind() {
	case reflect.Bool:
		if rv.Bool() {
			return "true"
		}
		return "false"
	case reflect.String:
		return rv.String()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return HumanMeasurementValue(name, float64(rv.Int()))
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return HumanMeasurementValue(name, float64(rv.Uint()))
	case reflect.Float32, reflect.Float64:
		return HumanMeasurementValue(name, rv.Float())
	case reflect.Ptr:
		if rv.IsNil() {
			return "null"
		}
		return HumanFormatFieldValue(name, rv.Elem().Interface())
	default:
		// Handle undefined-like cases or complex types
		jsonBytes, err := json.Marshal(value)
		if err != nil {
			return fmt.Sprintf("%v", value)
		}
		return string(jsonBytes)
	}
}

func consoleHumanLogFormat(message string, args []any) string {
	return HumanLogFormat(message, args, func(token string, formatted *string) string {
		if formatted != nil {
			return colorMagenta + token + colorReset + ":" + colorCyan + *formatted + colorReset
		}
		return colorMagenta + token + colorReset
	})
}
