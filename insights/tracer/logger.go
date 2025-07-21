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
	"fmt"
	"regexp"
	"runtime"
)

type Logger interface {
	EmitLogEvent(context context.Context, level string, errorMessage string, format string, args []any)
}

var _logger = NewConsoleLogger()

func SetGlobalLogger(logger Logger) {
	_logger = logger
}

func LogTrace(context context.Context, message string, args ...any) {
	_logger.EmitLogEvent(context, "TRACE", "", message, args)
}

func LogDebug(context context.Context, message string, args ...any) {
	_logger.EmitLogEvent(context, "DEBUG", "", message, args)
}

func LogInfo(context context.Context, message string, args ...any) {
	_logger.EmitLogEvent(context, "INFO", "", message, args)
}

func LogWarn(context context.Context, message string, args ...any) {
	_logger.EmitLogEvent(context, "WARN", "", message, args)
}

func LogError(context context.Context, error error, message string, args ...any) {
	_logger.EmitLogEvent(context, "ERROR", error.Error(), message, args)
}

func PlainHumanLogFormat(message string, args []any) string {
	return HumanLogFormat(message, args, func(token string, formatted *string) string {
		if formatted != nil {
			return fmt.Sprintf("%s:%s", token, *formatted)
		}
		return token
	})
}

var logFormatRegex = regexp.MustCompile(`\{([^}]+)\}`)

func HumanLogFormat(message string, args []any, humanParam func(string, *string) string) string {
	index := 0

	result := logFormatRegex.ReplaceAllStringFunc(message, func(match string) string {
		// Extract token (remove braces)
		token := match[1 : len(match)-1]

		if index < len(args) {
			formatted := HumanFormatFieldValue(token, args[index])
			index++
			return humanParam(token, &formatted)
		}
		return humanParam(match, nil) // Return original if no more args
	})

	return result
}

func getCallerFuncFileLine(skip int) (string, int, string) {
	var funcName string
	pc, file, fileLine, ok := runtime.Caller(skip)
	if ok {
		details := runtime.FuncForPC(pc)
		funcName = details.Name()
	} else {
		file = "unknown"
		fileLine = 0
	}
	return file, fileLine, funcName
}
