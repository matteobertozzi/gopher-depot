package tashkewey

import (
	"fmt"
	"log"
	"net/http"
	"runtime"
	runtimeMetrics "runtime/metrics"
	"sort"
	"sync"
	"time"

	"github.com/matteobertozzi/gopher-depot/insights/metrics"
)

type Monitor struct {
	mu       sync.RWMutex
	samples  []runtimeMetrics.Sample
	lastCPU  float64
	lastTime time.Time
}

func NewMonitor() *Monitor {
	m := &Monitor{}

	// Initialize runtime metrics
	descs := runtimeMetrics.All()
	m.samples = make([]runtimeMetrics.Sample, len(descs))
	for i := range m.samples {
		m.samples[i].Name = descs[i].Name
	}

	// Start background collection every 15 seconds
	go m.collectData()

	return m
}

func (m *Monitor) collectDataNow() {
	// Read runtime metrics
	runtimeMetrics.Read(m.samples)

	var memory int64
	var cpuPercent int64

	currentTime := time.Now()
	for _, sample := range m.samples {
		switch sample.Name {
		case "/memory/classes/heap/objects:bytes":
			memory = int64(sample.Value.Uint64())
		case "/cpu/classes/total:cpu-seconds":
			// Calculate CPU percentage
			var currentCPU float64
			switch sample.Value.Kind() {
			case runtimeMetrics.KindUint64:
				currentCPU = float64(sample.Value.Uint64())
			case runtimeMetrics.KindFloat64:
				currentCPU = float64(sample.Value.Float64())
			}

			if !m.lastTime.IsZero() {
				cpuDelta := float64(currentCPU-m.lastCPU) / 1e9 // Convert nanoseconds to seconds
				timeDelta := currentTime.Sub(m.lastTime).Seconds()
				if timeDelta > 0 {
					cpuPercent = int64((cpuDelta / timeDelta) * 100)
				}
			}

			m.lastCPU = currentCPU
			m.lastTime = currentTime
		}
	}

	// Fallback to runtime.MemStats if metrics not available
	if memory == 0 {
		var memStats runtime.MemStats
		runtime.ReadMemStats(&memStats)
		memory = int64(memStats.Alloc)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	memUsage.Sample(currentTime, memory)
	//cpuUsage.Sample(currentTime, cpuPercent)
	goRoutines.Sample(currentTime, int64(runtime.NumGoroutine()))
	if cpuPercent > 0 {
		log.Printf("CPU usage: %d%%", cpuPercent)
	}
}

func (m *Monitor) collectData() {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	m.collectDataNow()
	for range ticker.C {
		m.collectDataNow()
	}
}

var memUsage = metrics.RegisterMetric[*metrics.MaxAndAvgTimeRangeGauge](metrics.Metric{
	Unit:      "BYTES",
	Name:      "memory.usage",
	Collector: metrics.NewMaxAndAvgTimeRangeGauge(3*time.Hour, 1*time.Minute),
})

var goRoutines = metrics.RegisterMetric[*metrics.MaxAndAvgTimeRangeGauge](metrics.Metric{
	Unit:      "COUNT",
	Name:      "go.routines",
	Collector: metrics.NewMaxAndAvgTimeRangeGauge(3*time.Hour, 1*time.Minute),
})

func (m *Monitor) RegisterRoutes(mux *http.ServeMux) {
	AddRoute(mux, m.healthHandler())
	AddRoute(mux, m.monitorHandler())
	AddRoute(mux, m.metricsHandler())
}

// Health handler for AWS ALB health checks
func (m *Monitor) healthHandler() Route {
	return Route{
		Method:             http.MethodGet,
		Uri:                "/runtime/health",
		RequiredPermission: AllowPublic{},
		Handler: func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/plain")
			w.WriteHeader(http.StatusNoContent)
		},
	}
}

// Monitor handler for runtime statistics
func (monitor *Monitor) monitorHandler() Route {
	return Route{
		Method:             http.MethodGet,
		Uri:                "/runtime/monitor",
		RequiredPermission: RequiredModulePermission{Module: "runtime", Roles: []string{"MONITOR"}},
		Handler: DataOutMiddleware(func(r *http.Request) (any, error) {
			stats := metrics.CollectMetricsData()
			return stats, nil
		}),
	}
}

func (monitor *Monitor) metricsHandler() Route {
	return Route{
		Method:             http.MethodGet,
		Uri:                "/runtime/metrics",
		RequiredPermission: RequiredModulePermission{Module: "runtime", Roles: []string{"METRICS"}},
		Handler: func(w http.ResponseWriter, r *http.Request) {
			descs := runtimeMetrics.All()
			samples := make([]runtimeMetrics.Sample, 0, len(descs))
			for _, desc := range descs {
				samples = append(samples, runtimeMetrics.Sample{Name: desc.Name})
			}

			runtimeMetrics.Read(samples)

			sort.Slice(samples, func(i, j int) bool {
				return samples[i].Name < samples[j].Name
			})

			w.Header().Set("Content-Type", "text/plain; charset=utf-8")

			fmt.Fprintln(w, "Go Runtime Metrics:")
			fmt.Fprintln(w, "===================")
			for _, sample := range samples {
				name := sample.Name
				value := sample.Value

				var descText string
				for _, d := range descs {
					if d.Name == name {
						descText = d.Description
						break
					}
				}

				fmt.Fprintf(w, "\nMetric: %s\n", name)
				if descText != "" {
					fmt.Fprintf(w, "  Description: %s\n", descText)
				}
				fmt.Fprintf(w, "  Kind: %d\n", value.Kind())
				fmt.Fprintf(w, "  Value: ")

				switch value.Kind() {
				case runtimeMetrics.KindUint64:
					fmt.Fprintf(w, "%d\n", value.Uint64())
				case runtimeMetrics.KindFloat64:
					fmt.Fprintf(w, "%f\n", value.Float64())
				case runtimeMetrics.KindFloat64Histogram:
					// For histograms, print the buckets and counts.
					hist := value.Float64Histogram()
					fmt.Fprintf(w, "Histogram (Counts: %v, Buckets: %v)\n", hist.Counts, hist.Buckets)

					if len(hist.Buckets) > 0 && len(hist.Counts) == len(hist.Buckets)+1 {
						fmt.Fprintln(w, "    (-Inf, "+fmt.Sprintf("%.2e", hist.Buckets[0])+"]: "+fmt.Sprintf("%d", hist.Counts[0]))
						for i := 0; i < len(hist.Buckets)-1; i++ {
							fmt.Fprintln(w, "    ("+fmt.Sprintf("%.2e", hist.Buckets[i])+", "+fmt.Sprintf("%.2e", hist.Buckets[i+1])+"]: "+fmt.Sprintf("%d", hist.Counts[i+1]))
						}
						fmt.Fprintln(w, "    ("+fmt.Sprintf("%.2e", hist.Buckets[len(hist.Buckets)-1])+", +Inf): "+fmt.Sprintf("%d", hist.Counts[len(hist.Counts)-1]))
					}

				case runtimeMetrics.KindBad:
					// This should not happen for metrics listed by metrics.All()
					fmt.Fprintln(w, "Bad Kind")
				default:
					// Should not happen.
					fmt.Fprintf(w, "Unknown Kind: %d\n", value.Kind())
				}
			}
			fmt.Fprintln(w, "\n===================")
			fmt.Fprintln(w, "End of Metrics")
		},
	}
}
