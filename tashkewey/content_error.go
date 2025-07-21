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

package tashkewey

import (
	"context"
	"fmt"
	"net/http"
	"os"

	"github.com/matteobertozzi/gopher-depot/insights/tracer"
)

type HttpErrorMessage struct {
	StatusCode int    `json:"-"`
	TraceId    string `json:"traceId"`
	Status     string `json:"status"`
	Message    string `json:"message"`
	Data       any    `json:"data"`
}

func (e *HttpErrorMessage) Error() string {
	return fmt.Sprintf("HTTP %d: %s %s", e.StatusCode, e.Status, e.Message)
}

func NewBadRequestError(ctx context.Context, status string, message string) *HttpErrorMessage {
	return &HttpErrorMessage{
		StatusCode: 400,
		TraceId:    tracer.GetTraceId(ctx),
		Status:     status,
		Message:    message,
	}
}

func NewUnauthorizedError(ctx context.Context, status string, message string) *HttpErrorMessage {
	return &HttpErrorMessage{
		StatusCode: 401,
		TraceId:    tracer.GetTraceId(ctx),
		Status:     status,
		Message:    message,
	}
}

func NewForbiddenError(ctx context.Context, status string, message string) *HttpErrorMessage {
	return &HttpErrorMessage{
		StatusCode: 403,
		TraceId:    tracer.GetTraceId(ctx),
		Status:     status,
		Message:    message,
	}
}

func NewConflictError(ctx context.Context, status string, message string) *HttpErrorMessage {
	return &HttpErrorMessage{
		StatusCode: 409,
		TraceId:    tracer.GetTraceId(ctx),
		Status:     status,
		Message:    message,
	}
}

func NewInternalServerError(ctx context.Context, message string) *HttpErrorMessage {
	return &HttpErrorMessage{
		StatusCode: 500,
		TraceId:    tracer.GetTraceId(ctx),
		Status:     "INTERNAL_SERVER_ERROR",
		Message:    message,
	}
}

func NewServiceUnavailableError(ctx context.Context, message string) *HttpErrorMessage {
	return &HttpErrorMessage{
		StatusCode: 503,
		TraceId:    tracer.GetTraceId(ctx),
		Status:     "SERVICE_UNAVAILABLE",
		Message:    message,
	}
}

func WriteErrorResponse(w http.ResponseWriter, r *http.Request, err error) {
	traceId := tracer.GetTraceId(r.Context())

	httpErr, ok := err.(*HttpErrorMessage)
	if !ok {
		var statusCode int
		var status string
		if os.IsNotExist(err) {
			statusCode = http.StatusNotFound
			status = "NOT_FOUND"
		} else {
			statusCode = http.StatusInternalServerError
			status = "INTERNAL_SERVER_ERROR"
		}

		httpErr = &HttpErrorMessage{
			StatusCode: statusCode,
			TraceId:    traceId,
			Status:     status,
			Message:    err.Error(),
			Data:       nil,
		}
	}

	tracer.LogError(r.Context(), err, "Request {http.method} {http.path} failed with {http.status} {error.status}",
		r.Method, r.URL.Path, httpErr.StatusCode, httpErr.Status)
	contentEncoding, contentType, bodyEnc, err := encodeResponseBody(r, httpErr)
	if err != nil {
		// unable to serialize error response
		tracer.LogError(r.Context(), err, "Unable to serialize error response", httpErr.Status, httpErr.Message)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	if contentEncoding != "" {
		w.Header().Set("Content-Encoding", contentEncoding)
	}
	w.Header().Set("Content-Type", contentType)
	w.WriteHeader(httpErr.StatusCode)
	w.Write(bodyEnc)
}
