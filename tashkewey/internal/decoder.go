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

package internal

import (
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/fxamacker/cbor"
	"github.com/klauspost/compress/zstd"
	"github.com/matteobertozzi/yajbe-data-format/golang/yajbe"
	"gopkg.in/yaml.v3"
)

func ParseContentType(fullContentType string) (string, string) {
	fullContentType = strings.TrimSpace(fullContentType)
	if fullContentType == "" {
		// NOTE: we are assuming json as default encoding
		return "application/json", ""
	}

	paramIndex := strings.IndexByte(fullContentType, ',')
	if paramIndex < 0 {
		return fullContentType, ""
	}

	contentType := strings.TrimSpace(fullContentType[:paramIndex])
	params := strings.TrimSpace(fullContentType[paramIndex+1:])
	return contentType, params
}

func DecodeBody[TBody any](contentEncoding string, contentType string, body io.ReadCloser, bodyObj *TBody) error {
	if body == nil || body == http.NoBody {
		return errors.New("expected body")
	}
	defer body.Close()

	var reader io.Reader = body

	contentEncoding = strings.ToLower(strings.TrimSpace(contentEncoding))
	if contentEncoding != "" {
		switch contentEncoding {
		case "gzip":
			gzr, err := gzip.NewReader(reader)
			if err != nil {
				return err
			}
			defer gzr.Close()
			reader = gzr
		case "zstd":
			zstdr, err := zstd.NewReader(reader)
			if err != nil {
				return err
			}
			defer zstdr.Close()
			reader = zstdr
		}
	}

	switch strings.ToLower(contentType) {
	case "application/json":
		return json.NewDecoder(reader).Decode(bodyObj)
	case "application/cbor":
		return cbor.NewDecoder(reader).Decode(bodyObj)
	case "application/yajbe":
		return yajbe.NewDecoder(reader).Decode(bodyObj)
	case "text/yaml", "application/yaml":
		return yaml.NewDecoder(reader).Decode(bodyObj)
	}
	return fmt.Errorf("unsupported content-type %s", contentType)
}

func DecodeRequestBody[TReq any](r *http.Request, body *TReq) error {
	if r.Body == nil || r.Body == http.NoBody || r.ContentLength == 0 {
		return errors.New("expected body")
	}

	contentEncoding := r.Header.Get("Content-Encoding")
	contentType, _ := ParseContentType(r.Header.Get("Content-Type"))
	return DecodeBody(contentEncoding, contentType, r.Body, body)
}

func DecodeResponseBody[TReq any](r *http.Response, body *TReq) error {
	if r.Body == nil || r.Body == http.NoBody || r.ContentLength == 0 {
		return errors.New("expected body")
	}

	contentEncoding := r.Header.Get("Content-Encoding")
	contentType, _ := ParseContentType(r.Header.Get("Content-Type"))
	return DecodeBody(contentEncoding, contentType, r.Body, body)
}

func ParseAcceptHeader(acceptHeader string) string {
	for mediaType := range strings.SplitSeq(acceptHeader, ",") {
		mt := strings.TrimSpace(mediaType)
		// Remove quality factors (;q=0.8) if present
		if idx := strings.IndexByte(mt, ';'); idx != -1 {
			mt = mt[:idx]
		}

		// Check for supported types in order of preference
		switch strings.TrimSpace(mt) {
		case "", "*/*", "application/json":
			// default for empty accept or wildcard
			return "application/json"
		case "application/cbor":
			return "application/cbor"
		case "text/yaml", "application/yaml":
			return "text/yaml"
		}
	}
	return "application/json" // default fallback
}

func ParseAcceptEncodingHeader(acceptHeader string) string {
	for mediaType := range strings.SplitSeq(acceptHeader, ",") {
		mt := strings.TrimSpace(mediaType)
		// Remove quality factors (;q=0.8) if present
		if idx := strings.IndexByte(mt, ';'); idx != -1 {
			mt = mt[:idx]
		}
		mt = strings.TrimSpace(mt)

		// Check for supported types in order of preference
		switch strings.ToLower(mt) {
		case "gzip":
			return "gzip"
		case "zstd":
			return "zstd"
		case "*/*":
			return "gzip" // default for wildcard
		}
	}
	return "" // default fallback
}
