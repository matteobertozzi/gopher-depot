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
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/fxamacker/cbor/v2"
	"github.com/klauspost/compress/zstd"
	"github.com/matteobertozzi/gopher-depot/insights/metrics"
	"github.com/matteobertozzi/gopher-depot/insights/tracer"
	"github.com/matteobertozzi/yajbe-data-format/golang/yajbe"
	"gopkg.in/yaml.v2"
)

type encoder interface {
	Encode(v any) error
}

type compressedWriter interface {
	Close() error
	Flush() error
}

var httpRespBodySize = metrics.RegisterMetric[*metrics.MaxAndAvgTimeRangeGauge](metrics.Metric{
	Unit:      "BYTES",
	Name:      "http.response.body.size",
	Collector: metrics.NewMaxAndAvgTimeRangeGauge(3*time.Hour, 1*time.Minute),
})

var httpTopRespBodySize = metrics.RegisterMetric[*metrics.TopKTable](metrics.Metric{
	Unit:      "BYTES",
	Name:      "http.top.response.body.size",
	Collector: metrics.NewTopKTable(16, 5, 60*time.Minute),
})

func EncodeResponseBody(r *http.Request, body any) (string, string, []byte, error) {
	var buf bytes.Buffer
	var writer io.Writer = &buf

	contentEncoding := ParseAcceptEncodingHeader(r.Header.Get("Accept-Encoding"))
	if contentEncoding != "" {
		switch contentEncoding {
		case "gzip":
			gzw := gzip.NewWriter(writer)
			defer gzw.Close()
			writer = gzw
		case "zstd":
			zstdw, err := zstd.NewWriter(writer)
			if err == nil {
				defer zstdw.Close()
				contentEncoding = ""
				writer = zstdw
			}
		}
	}

	var encoder encoder
	contentType := ParseAcceptHeader(r.Header.Get("accept"))
	switch contentType {
	case "application/cbor":
		encoder = cbor.NewEncoder(writer)
	case "application/yajbe":
		encoder = yajbe.NewEncoder(writer)
	case "text/yaml":
		encoder = yaml.NewEncoder(writer)
	default:
		encoder = json.NewEncoder(writer)
	}

	err := encoder.Encode(body)
	if err != nil {
		return contentEncoding, contentType, nil, err
	}

	if cw, ok := writer.(compressedWriter); ok {
		cw.Flush()
		cw.Close()
	}

	httpRespBodySize.Sample(time.Now(), int64(buf.Len()))
	httpTopRespBodySize.AddEvent(time.Now(), fmt.Sprintf("%s %s (%s %s)", r.Method, r.URL.Path, contentEncoding, contentType), int64(buf.Len()), tracer.GetTraceId(r.Context()))

	return contentEncoding, contentType, buf.Bytes(), err
}
