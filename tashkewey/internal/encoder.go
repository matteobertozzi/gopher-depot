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
	"sync"
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

// bufferPool reuses bytes.Buffer objects to reduce GC pressure
var bufferPool = sync.Pool{
	New: func() any {
		return new(bytes.Buffer)
	},
}

// gzipWriterPool reuses gzip writers
var gzipWriterPool = sync.Pool{
	New: func() any {
		return new(gzipWriter)
	},
}

type gzipWriter struct {
	*gzip.Writer
	buf *bytes.Buffer
}

func EncodeResponseBody(r *http.Request, body any) (string, string, []byte, error) {
	buf := bufferPool.Get().(*bytes.Buffer)
	defer func() {
		buf.Reset()
		bufferPool.Put(buf)
	}()

	var writer io.Writer = buf

	contentEncoding := ParseAcceptEncodingHeader(r.Header.Get("Accept-Encoding"))
	if contentEncoding != "" {
		switch contentEncoding {
		case "gzip":
			gzw := gzipWriterPool.Get().(*gzipWriter)
			gzw.buf = buf
			if gzw.Writer == nil {
				gzw.Writer = gzip.NewWriter(buf)
			} else {
				gzw.Writer.Reset(buf)
			}
			writer = gzw.Writer
			defer func() {
				gzw.Close()
				gzw.buf = nil
				gzipWriterPool.Put(gzw)
			}()
		case "zstd":
			zstdw, err := zstd.NewWriter(writer)
			if err != nil {
				contentEncoding = ""
			} else {
				defer zstdw.Close()
				writer = zstdw
			}
		}
	}

	var enc encoder
	contentType := ParseAcceptHeader(r.Header.Get("accept"))
	switch contentType {
	case "application/cbor":
		enc = cbor.NewEncoder(writer)
	case "application/yajbe":
		enc = yajbe.NewEncoder(writer)
	case "text/yaml":
		enc = yaml.NewEncoder(writer)
	default:
		enc = json.NewEncoder(writer)
	}

	err := enc.Encode(body)
	if err != nil {
		return contentEncoding, contentType, nil, err
	}

	if cw, ok := writer.(compressedWriter); ok {
		cw.Flush()
		cw.Close()
	}

	httpRespBodySize.Sample(time.Now(), int64(buf.Len()))
	httpTopRespBodySize.AddEvent(time.Now(), fmt.Sprintf("%s %s (%s %s)", r.Method, r.URL.Path, contentEncoding, contentType), int64(buf.Len()), tracer.GetTraceId(r.Context()))

	// Copy bytes before returning (buffer will be pooled)
	result := make([]byte, buf.Len())
	copy(result, buf.Bytes())
	return contentEncoding, contentType, result, err
}
