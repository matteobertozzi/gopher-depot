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

package metrics

import "sync"

type MetricCollector interface {
	Type() string
	Snapshot() any
}

type Metric struct {
	Unit      string
	Name      string
	Collector MetricCollector
}

var registry sync.Map

func RegisterMetric[T MetricCollector](metric Metric) T {
	if metric.Name == "" {
		panic("unsupported metric without a name")
	}

	collector, ok := metric.Collector.(T)
	if !ok {
		panic("invalid metric collector type cast")
	}

	registry.Store(metric.Name, metric)
	return collector
}

func UnregisterMetric(name string) {
	registry.Delete(name)
}

func ClearRegistry() {
	registry.Range(func(key, value any) bool {
		registry.Delete(key)
		return true
	})
}

type MetricData struct {
	Name  string `json:"name"`
	Type  string `json:"type"`
	Unit  string `json:"unit"`
	Label string `json:"label"`
	Data  any    `json:"data"`
}

func CollectMetricsData() []MetricData {
	var metrics []MetricData
	registry.Range(func(key, value any) bool {
		metric, ok := value.(Metric)
		if !ok {
			return true
		}
		data := metric.Collector.Snapshot()
		metrics = append(metrics, MetricData{
			Name:  metric.Name,
			Type:  metric.Collector.Type(),
			Unit:  metric.Unit,
			Label: metric.Name,
			Data:  data,
		})
		return true
	})
	return metrics
}
