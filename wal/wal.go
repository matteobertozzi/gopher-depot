package wal

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"
)

type pendingWrite[T any] struct {
	entry T
	done  chan error
}

func (v *pendingWrite[T]) Content() T {
	return v.entry
}

// Wal represents the Write-Ahead Log.
type Wal[T any] struct {
	// User-configurable fields
	flusher        WalFlusher[T]
	flushInterval  time.Duration
	flushThreshold int // in bytes

	// Internal state
	mu          sync.Mutex
	buffer      []*pendingWrite[T]
	currentSize int
	isOpen      bool
	ctx         context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup
	flushSignal chan struct{}
}

// WalConfig holds the configuration for the WAL.
type WalConfig[T any] struct {
	// Flusher is the mandatory implementation that handles persistence.
	Flusher WalFlusher[T]
	// FlushInterval is the maximum time to wait before a flush is triggered.
	// Defaults to 500ms.
	FlushInterval time.Duration
	// FlushThresholdBytes is the size in bytes that triggers a flush.
	// Defaults to 64KB.
	FlushThresholdBytes int
	// InitialBufferSize is the initial size of the pending writes buffer.
	// Defaults to 1024.
	InitialBufferSize int
}

// NewWal creates a new Write-Ahead Log for a specific entry type T.
func NewWal[T any](config WalConfig[T]) (*Wal[T], error) {
	if config.Flusher == nil {
		return nil, fmt.Errorf("Flusher cannot be nil")
	}

	// Apply default values if not provided
	if config.FlushInterval == 0 {
		config.FlushInterval = 500 * time.Millisecond
	}
	if config.FlushThresholdBytes == 0 {
		config.FlushThresholdBytes = 64 << 10
	}
	if config.InitialBufferSize == 0 {
		config.InitialBufferSize = 1024
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &Wal[T]{
		flusher:        config.Flusher,
		flushInterval:  config.FlushInterval,
		flushThreshold: config.FlushThresholdBytes,
		ctx:            ctx,
		cancel:         cancel,
		buffer:         make([]*pendingWrite[T], 0, config.InitialBufferSize),
		flushSignal:    make(chan struct{}, 1),
	}, nil
}

// Open starts the background flushing goroutine and opens the flusher.
func (w *Wal[T]) Open() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.isOpen {
		return nil
	}

	if err := w.flusher.Open(); err != nil {
		return fmt.Errorf("failed to open flusher: %w", err)
	}

	w.isOpen = true

	w.wg.Add(1)
	go w.flushLoop()
	return nil
}

// Close flushes any remaining entries, stops the goroutine, and closes the flusher.
func (w *Wal[T]) Close() error {
	w.mu.Lock()
	if !w.isOpen {
		w.mu.Unlock()
		return nil
	}
	w.isOpen = false
	w.mu.Unlock()

	// Signal the flush loop to stop and wait for it to finish.
	w.cancel()
	w.wg.Wait()

	// Close the user-provided flusher.
	if err := w.flusher.Close(); err != nil {
		return fmt.Errorf("failed to close flusher: %w", err)
	}
	return nil
}

func (w *Wal[T]) IsOpen() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.isOpen
}

// Add appends a new entry of type T to the log.
// It will block until the entry has been successfully flushed.
func (w *Wal[T]) Add(entry T) error {
	req := &pendingWrite[T]{
		entry: entry,
		done:  make(chan error, 1),
	}

	w.mu.Lock()
	if !w.isOpen {
		w.mu.Unlock()
		return fmt.Errorf("WAL is closed")
	}

	w.buffer = append(w.buffer, req)
	w.currentSize += getEntrySize(entry)
	// Check if the threshold is reached and signal for an immediate flush
	if w.currentSize >= w.flushThreshold {
		select {
		case w.flushSignal <- struct{}{}:
			// Signal sent successfully.
		default:
			// The channel is full, meaning a flush is already pending or in progress.
			// No need to send another signal.
		}
	}
	w.mu.Unlock()

	// Wait for the flush to complete
	return <-req.done
}

// flushLoop is the main loop for the background goroutine.
func (w *Wal[T]) flushLoop() {
	defer w.wg.Done()

	ticker := time.NewTicker(w.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C: // Periodic flush based on interval
			w.mu.Lock()
			// Flush if we have reached the size threshold or just if buffer is not empty on tick
			shouldFlush := w.currentSize > 0
			w.mu.Unlock()

			if shouldFlush {
				if err := w.flush(w.ctx); err != nil {
					log.Printf("WAL: error during periodic flush: %v", err)
				}
			}

		case <-w.flushSignal: // NEW: Immediate flush based on threshold signal
			// A signal was received from Add() indicating threshold might be reached.
			// Re-check conditions under lock to ensure a flush is still needed
			// (e.g., in case a concurrent flush already cleared the buffer).
			w.mu.Lock()
			shouldFlush := w.currentSize > 0
			w.mu.Unlock()

			if shouldFlush {
				if err := w.flush(w.ctx); err != nil {
					log.Printf("WAL: error during threshold flush: %v", err)
				}
			}

		case <-w.ctx.Done(): // Shutdown signal received
			// Perform a final flush.
			if err := w.flush(context.Background()); err != nil { // Use a background context for final flush
				log.Printf("WAL: error during final flush on close: %v", err)
			}
			return
		}
	}
}

// flush persists the current buffer of entries using the flusher.
func (w *Wal[T]) flush(ctx context.Context) error {
	w.mu.Lock()

	if len(w.buffer) == 0 {
		w.mu.Unlock()
		return nil
	}

	pending := w.buffer
	w.buffer = nil
	w.currentSize = 0
	w.mu.Unlock()

	entries := make([]T, len(pending))
	for i, p := range pending {
		entries[i] = p.entry
	}

	// Call the user-provided flusher with the context.
	err := w.flusher.Flush(ctx, entries)

	// Notify all waiting goroutines about the result.
	for _, p := range pending {
		p.done <- err
		close(p.done)
	}

	return err
}

// Sizer is an interface that can be implemented by entry types
// to provide a more accurate size calculation.
type Sizer interface {
	Size() int
}

func getEntrySize(entry any) int {
	if s, ok := entry.(Sizer); ok {
		return s.Size()
	}
	// Default size if T doesn't implement Sizer
	// Consider carefully what makes sense for your application
	if b, ok := any(entry).([]byte); ok {
		return len(b)
	}
	return 1
}
