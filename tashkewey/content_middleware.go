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
	"net/http"

	"github.com/matteobertozzi/gopher-depot/tashkewey/internal"
)

type NoDataHandlerFunc func(r *http.Request) error
type DataInHandlerFunc[TReqBody any] func(r *http.Request, body *TReqBody) error
type DataOutHandlerFunc[TRespBody any] func(r *http.Request) (TRespBody, error)
type DataInOutHandlerFunc[TReqBody, TRespBody any] func(r *http.Request, body *TReqBody) (TRespBody, error)
type DataInRawOutHandlerFunc[TReqBody any] func(w http.ResponseWriter, r *http.Request, body *TReqBody) error
type DataRawOutHandlerFunc func(w http.ResponseWriter, r *http.Request) error

func NoDataMiddleware(handler NoDataHandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if err := handler(r); err != nil {
			WriteErrorResponse(w, r, err)
			return
		}

		w.WriteHeader(http.StatusNoContent)
	}
}

func DataInMiddleware[TReq any](handler DataInHandlerFunc[TReq]) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var reqBody TReq
		if err := internal.DecodeRequestBody(r, &reqBody); err != nil {
			WriteErrorResponse(w, r, err)
			return
		}

		if err := handler(r, &reqBody); err != nil {
			WriteErrorResponse(w, r, err)
			return
		}

		w.WriteHeader(http.StatusNoContent)
	}
}

func DataOutMiddleware[TResp any](handler DataOutHandlerFunc[TResp]) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Body != nil {
			r.Body.Close()
		}

		respBody, err := handler(r)
		if err != nil {
			WriteErrorResponse(w, r, err)
		} else {
			WriteResponseBody(w, r, respBody)
		}
	}
}

func DataInOutMiddleware[TReq, TResp any](handler DataInOutHandlerFunc[TReq, TResp]) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var reqBody TReq
		if err := internal.DecodeRequestBody(r, &reqBody); err != nil {
			WriteErrorResponse(w, r, err)
			return
		}

		respBody, err := handler(r, &reqBody)
		if err != nil {
			WriteErrorResponse(w, r, err)
		} else {
			WriteResponseBody(w, r, respBody)
		}
	}
}

func DataInRawOutMiddleware[TReq any](handler DataInRawOutHandlerFunc[TReq]) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var reqBody TReq
		if err := internal.DecodeRequestBody(r, &reqBody); err != nil {
			WriteErrorResponse(w, r, err)
			return
		}

		err := handler(w, r, &reqBody)
		if err != nil {
			WriteErrorResponse(w, r, err)
		}
	}
}

func DataRawOutMiddleware(handler DataRawOutHandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		err := handler(w, r)
		if err != nil {
			WriteErrorResponse(w, r, err)
		}
	}
}
