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
	"reflect"
	"time"

	"github.com/matteobertozzi/gopher-depot/tashkewey/internal"
)

func WriteResponseBody(w http.ResponseWriter, r *http.Request, resp any) {
	if resp == nil || reflect.ValueOf(resp).IsNil() {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	contentEncoding, contentType, bodyEnc, err := internal.EncodeResponseBody(r, resp)
	if err != nil {
		WriteErrorResponse(w, r, err)
		return
	}

	if contentEncoding != "" {
		w.Header().Set("Content-Encoding", contentEncoding)
	}
	w.Header().Set("Content-Type", contentType)
	w.WriteHeader(http.StatusOK)
	w.Write(bodyEnc)
}

func WriteImage(w http.ResponseWriter, r *http.Request, imageType string, timestamp time.Time, eTag string, image []byte) {
	if (eTag != "" && r.Header.Get("If-None-Match") == eTag) || r.Header.Get("If-Modified-Since") == timestamp.Format(http.TimeFormat) {
		w.WriteHeader(http.StatusNotModified)
		return
	}

	w.Header().Set("Content-Type", imageType)
	w.Header().Set("Last-Modified", timestamp.Format(http.TimeFormat))
	w.Header().Set("ETag", eTag)
	w.Header().Set("Cache-Control", "public, max-age=86400")
	w.WriteHeader(http.StatusOK)
	w.Write(image)
}
