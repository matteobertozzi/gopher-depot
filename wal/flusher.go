package wal

import "context"

// WalFlusher is an interface that defines the persistence strategy for the WAL.
// Users of the library must implement this interface.
type WalFlusher[T any] interface {
	// Open is called when the WAL is opened. Use it to initialize resources
	// like file handles or database connections.
	Open() error
	// Flush is called to persist a batch of entries. It receives a context
	// that will be canceled on WAL closure, allowing for graceful shutdowns.
	Flush(ctx context.Context, entries []T) error
	// Close is called when the WAL is closed. Use it to release any resources.
	Close() error
}
