package tracer

import (
	"context"
	"io"
	"log"
	"testing"
)

// BenchmarkConsoleLogger benchmarks the console logger performance
func BenchmarkConsoleLogger(b *testing.B) {
	// Redirect output to discard to avoid terminal spam during benchmarks
	originalOutput := log.Writer()
	log.SetOutput(io.Discard)
	defer log.SetOutput(originalOutput)

	logger := NewConsoleLogger()
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		logger.EmitLogEvent(ctx, "INFO", "", "Simple message", nil)
	}
}

func BenchmarkConsoleLoggerWithArgs(b *testing.B) {
	originalOutput := log.Writer()
	log.SetOutput(io.Discard)
	defer log.SetOutput(originalOutput)

	logger := NewConsoleLogger()
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		logger.EmitLogEvent(ctx, "INFO", "", "Message with {count} and {data}", []any{i, "test-data"})
	}
}

func BenchmarkConsoleLoggerWithError(b *testing.B) {
	originalOutput := log.Writer()
	log.SetOutput(io.Discard)
	defer log.SetOutput(originalOutput)

	logger := NewConsoleLogger()
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		logger.EmitLogEvent(ctx, "ERROR", "test error", "Error message {id}", []any{i})
	}
}

func BenchmarkConsoleLoggerWithTrace(b *testing.B) {
	originalOutput := log.Writer()
	log.SetOutput(io.Discard)
	defer log.SetOutput(originalOutput)

	logger := NewConsoleLogger()
	traceId := GenerateTraceId()
	ctx := context.WithValue(context.Background(), "TraceId", traceId)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		logger.EmitLogEvent(ctx, "INFO", "", "Message with trace {id}", []any{i})
	}
}

// Benchmark individual components
func BenchmarkHumanFormatFieldValue(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		HumanFormatFieldValue("count", i)
	}
}

func BenchmarkHumanLogFormat(b *testing.B) {
	args := []any{42, "test-data", 3.14}
	message := "Processing {count} items with {data} at {rate.ms}"

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		HumanLogFormat(message, args, func(token string, formatted *string) string {
			if formatted != nil {
				return token + ":" + *formatted
			}
			return token
		})
	}
}

func BenchmarkConsoleHumanLogFormat(b *testing.B) {
	args := []any{42, "test-data", 3.14}
	message := "Processing {count} items with {data} at {rate.ms}"

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		consoleHumanLogFormat(message, args)
	}
}

func BenchmarkGetCallerFuncFileLine(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		getCallerFuncFileLine(3)
	}
}

func BenchmarkConsoleLevelColorText(b *testing.B) {
	text := "test message"

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		consoleLevelColorText("INFO", text)
		consoleLevelColorText("WARN", text)
		consoleLevelColorText("ERROR", text)
	}
}