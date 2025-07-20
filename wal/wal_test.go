package wal

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// MockEntry for testing
type MockEntry struct {
	ID      int
	Content string
}

// Size implements the Sizer interface for MockEntry.
func (e *MockEntry) Size() int {
	return len(e.Content) + 4 // 4 for ID int
}

// MockFlusher for testing
type MockFlusher struct {
	mu           sync.Mutex
	flushedData  []MockEntry
	openCount    int
	closeCount   int
	flushCount   int
	flushError   error
	flushDelay   time.Duration
	openError    error
	closeError   error
	flushContext CtxInfo // To capture context state during flush
}

// CtxInfo stores information about the context's state during a flush.
type CtxInfo struct {
	DoneCalled bool
	Err        error
}

// Open is the mock implementation for WalFlusher.Open.
func (m *MockFlusher) Open() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.openCount++
	return m.openError
}

// Flush is the mock implementation for WalFlusher.Flush.
// It simulates work with a delay and correctly responds to context cancellation,
// avoiding deadlocks by carefully managing the mutex.
func (m *MockFlusher) Flush(ctx context.Context, entries []MockEntry) error {
	// Acquire lock only for initial state update of the flusher's counts/data.
	m.mu.Lock()
	m.flushCount++
	m.flushedData = append(m.flushedData, entries...)
	m.mu.Unlock() // Release the lock immediately after updating shared state.

	if m.flushDelay > 0 {
		ticker := time.NewTicker(m.flushDelay / 10) // Check periodically during the delay
		if m.flushDelay < 10*time.Millisecond {     // Ensure at least one check for very short delays
			ticker = time.NewTicker(time.Millisecond)
		}
		defer ticker.Stop()

		// Channel to communicate CtxInfo back from the simulating goroutine.
		// Buffered to prevent deadlock if the main goroutine isn't immediately reading.
		resultCh := make(chan CtxInfo, 1)

		go func() {
			defer close(resultCh) // Always close the channel

			var finalCtxInfo CtxInfo
			totalSlept := time.Duration(0)

			for totalSlept < m.flushDelay {
				select {
				case <-ctx.Done():
					// Context was canceled during the flush operation.
					finalCtxInfo.DoneCalled = true
					finalCtxInfo.Err = ctx.Err()
					resultCh <- finalCtxInfo // Send result
					return                   // Exit the goroutine
				case <-ticker.C:
					totalSlept += m.flushDelay / 10
					if m.flushDelay < 10*time.Millisecond {
						totalSlept += time.Millisecond
					}
				}
			}
			// If work completed without context cancellation during the delay.
			finalCtxInfo.DoneCalled = false
			finalCtxInfo.Err = nil
			resultCh <- finalCtxInfo // Send result
		}()

		// Wait for the simulated work/cancellation to complete and receive the CtxInfo.
		// This now correctly waits on the result from the inner goroutine,
		// without holding m.mu, thus preventing the deadlock.
		m.flushContext = <-resultCh
	} else {
		// No delay, check context immediately.
		select {
		case <-ctx.Done():
			m.flushContext.DoneCalled = true
			m.flushContext.Err = ctx.Err()
			return ctx.Err() // If context is already done, return its error.
		default:
			m.flushContext.DoneCalled = false
			m.flushContext.Err = nil
		}
	}

	// Now, determine the error to return based on context cancellation or mock's configured error.
	if m.flushContext.DoneCalled && m.flushContext.Err != nil {
		return m.flushContext.Err // If context was canceled, return its error.
	}
	return m.flushError // Otherwise, return any configured simulated flush error.
}

// Close is the mock implementation for WalFlusher.Close.
func (m *MockFlusher) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closeCount++
	return m.closeError
}

// Helper to reset mock flusher state.
func (m *MockFlusher) reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.flushedData = nil
	m.openCount = 0
	m.closeCount = 0
	m.flushCount = 0
	m.flushError = nil
	m.flushDelay = 0
	m.openError = nil
	m.closeError = nil
	m.flushContext = CtxInfo{}
}

func TestNewWal(t *testing.T) {
	// Test case 1: Nil flusher
	_, err := NewWal(WalConfig[MockEntry]{Flusher: nil})
	if err == nil {
		t.Errorf("Expected error for nil flusher, got nil")
	}
	if err.Error() != "Flusher cannot be nil" {
		t.Errorf("Expected 'Flusher cannot be nil' error, got: %v", err)
	}

	// Test case 2: Default values
	flusher := &MockFlusher{}
	wal, err := NewWal(WalConfig[MockEntry]{Flusher: flusher})
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	if wal.flushInterval != 500*time.Millisecond {
		t.Errorf("Expected default FlushInterval 500ms, got: %v", wal.flushInterval)
	}
	if wal.flushThreshold != 64<<10 { // Default is 64KB as per wal.go
		t.Errorf("Expected default FlushThresholdBytes 64KB, got: %d", wal.flushThreshold)
	}
	if wal.flusher != flusher {
		t.Errorf("Flusher not set correctly")
	}

	// Test case 3: Custom values
	customFlusher := &MockFlusher{}
	wal, err = NewWal(WalConfig[MockEntry]{
		Flusher:             customFlusher,
		FlushInterval:       100 * time.Millisecond,
		FlushThresholdBytes: 1024,
	})
	if err != nil {
		t.Fatalf("Failed to create WAL with custom config: %v", err)
	}
	if wal.flushInterval != 100*time.Millisecond {
		t.Errorf("Expected custom FlushInterval 100ms, got: %v", wal.flushInterval)
	}
	if wal.flushThreshold != 1024 {
		t.Errorf("Expected custom FlushThresholdBytes 1KB, got: %d", wal.flushThreshold)
	}
}

func TestWalOpenClose(t *testing.T) {
	flusher := &MockFlusher{}
	wal, _ := NewWal(WalConfig[MockEntry]{Flusher: flusher})

	// Test Open
	err := wal.Open()
	if err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}
	if flusher.openCount != 1 {
		t.Errorf("Expected flusher.Open to be called once, got: %d", flusher.openCount)
	}
	if !wal.IsOpen() { // Use Wal.IsOpen()
		t.Errorf("WAL should be open after calling Open()")
	}

	// Test calling Open again (should be no-op)
	err = wal.Open()
	if err != nil {
		t.Fatalf("Calling Open again failed: %v", err)
	}
	if flusher.openCount != 1 { // Should still be 1
		t.Errorf("Expected flusher.Open not to be called again, got: %d", flusher.openCount)
	}

	// Test Close
	err = wal.Close()
	if err != nil {
		t.Fatalf("Failed to close WAL: %v", err)
	}
	if flusher.closeCount != 1 {
		t.Errorf("Expected flusher.Close to be called once, got: %d", flusher.closeCount)
	}
	if wal.IsOpen() { // Use Wal.IsOpen()
		t.Errorf("WAL should be closed after calling Close()")
	}
	if wal.ctx.Err() == nil {
		t.Errorf("WAL context should be canceled after Close()")
	}

	// Test calling Close again (should be no-op)
	err = wal.Close()
	if err != nil {
		t.Fatalf("Calling Close again failed: %v", err)
	}
	if flusher.closeCount != 1 { // Should still be 1
		t.Errorf("Expected flusher.Close not to be called again, got: %d", flusher.closeCount)
	}

	// Test Open error
	flusher.reset()
	flusher.openError = errors.New("open failed")
	wal, _ = NewWal(WalConfig[MockEntry]{Flusher: flusher})
	err = wal.Open()
	if err == nil {
		t.Errorf("Expected error when flusher.Open fails, got nil")
	}
	if flusher.openCount != 1 {
		t.Errorf("Expected flusher.Open to be called, got: %d", flusher.openCount)
	}
	if wal.IsOpen() { // Use Wal.IsOpen()
		t.Errorf("WAL should not be open if flusher.Open fails")
	}

	// Test Close error
	flusher.reset()
	flusher.closeError = errors.New("close failed")
	wal, _ = NewWal(WalConfig[MockEntry]{Flusher: flusher})
	_ = wal.Open() // Successfully open
	err = wal.Close()
	if err == nil {
		t.Errorf("Expected error when flusher.Close fails, got nil")
	}
	if flusher.closeCount != 1 {
		t.Errorf("Expected flusher.Close to be called, got: %d", flusher.closeCount)
	}
}

func TestWalAddAndFlushInterval(t *testing.T) {
	flusher := &MockFlusher{}
	wal, _ := NewWal(WalConfig[MockEntry]{
		Flusher:       flusher,
		FlushInterval: 100 * time.Millisecond,
		// Set threshold very high so only interval triggers flush
		FlushThresholdBytes: 100000,
	})

	err := wal.Open()
	if err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}
	defer wal.Close()

	entry1 := MockEntry{ID: 1, Content: "test entry 1"}
	entry2 := MockEntry{ID: 2, Content: "test entry 2"}

	var wg sync.WaitGroup

	// Add entries concurrently - these calls will block
	wg.Add(2)
	go func() {
		defer wg.Done()
		if err := wal.Add(entry1); err != nil {
			t.Errorf("Failed to add entry1: %v", err)
		}
	}()
	go func() {
		defer wg.Done()
		if err := wal.Add(entry2); err != nil {
			t.Errorf("Failed to add entry2: %v", err)
		}
	}()
	wg.Wait() // Wait for both Add calls to complete (i.e., flush)

	flusher.mu.Lock()
	if flusher.flushCount == 0 {
		t.Errorf("Expected flush to occur due to interval, but it didn't")
	}
	if len(flusher.flushedData) != 2 {
		t.Errorf("Expected 2 entries to be flushed, got: %d", len(flusher.flushedData))
	}
	flusher.mu.Unlock()
}

func TestWalAddAndFlushThreshold(t *testing.T) {
	flusher := &MockFlusher{}
	// Set threshold to be exactly the size of one entry
	testEntrySize := (&MockEntry{Content: "A"}).Size() // A single character + 4 bytes for ID
	wal, _ := NewWal(WalConfig[MockEntry]{
		Flusher:             flusher,
		FlushInterval:       10 * time.Second, // Very long interval to rely on threshold
		FlushThresholdBytes: testEntrySize,
	})

	err := wal.Open()
	if err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}
	defer wal.Close()

	entry1 := MockEntry{ID: 1, Content: "A"} // Size = testEntrySize
	entry2 := MockEntry{ID: 2, Content: "B"}

	// Add the first entry, this should trigger a flush and block until it's done
	if err := wal.Add(entry1); err != nil {
		t.Fatalf("Failed to add entry1: %v", err)
	}

	flusher.mu.Lock()
	if flusher.flushCount != 1 {
		t.Errorf("Expected 1 flush after adding entry1, got: %d", flusher.flushCount)
	}
	if len(flusher.flushedData) != 1 || flusher.flushedData[0] != entry1 {
		t.Errorf("Expected entry1 to be flushed, got: %+v", flusher.flushedData)
	}
	flusher.mu.Unlock()

	// Add the second entry, this should trigger another flush and block
	if err := wal.Add(entry2); err != nil {
		t.Fatalf("Failed to add entry2: %v", err)
	}

	flusher.mu.Lock()
	if flusher.flushCount != 2 {
		t.Errorf("Expected 2 flushes after adding entry2, got: %d", flusher.flushCount)
	}
	// The flushedData will accumulate, so check the latest one
	if len(flusher.flushedData) != 2 || flusher.flushedData[1] != entry2 {
		t.Errorf("Expected entry2 to be flushed, got: %+v", flusher.flushedData)
	}
	flusher.mu.Unlock()
}

func TestWalAddWhenClosed(t *testing.T) {
	flusher := &MockFlusher{}
	wal, _ := NewWal(WalConfig[MockEntry]{Flusher: flusher})

	// Do not open the WAL
	err := wal.Add(MockEntry{ID: 1})
	if err == nil {
		t.Errorf("Expected error when adding to a closed WAL, got nil")
	}
	if err.Error() != "WAL is closed" {
		t.Errorf("Expected 'WAL is closed' error, got: %v", err)
	}

	// Open and then close the WAL
	_ = wal.Open()
	_ = wal.Close()

	err = wal.Add(MockEntry{ID: 2})
	if err == nil {
		t.Errorf("Expected error when adding to a closed WAL after opening/closing, got nil")
	}
	if err.Error() != "WAL is closed" {
		t.Errorf("Expected 'WAL is closed' error, got: %v", err)
	}
}

func TestWalConcurrentAdd(t *testing.T) {
	flusher := &MockFlusher{}
	wal, _ := NewWal(WalConfig[MockEntry]{
		Flusher:             flusher,
		FlushInterval:       50 * time.Millisecond,
		FlushThresholdBytes: 100, // Small threshold to trigger frequent flushes
	})

	err := wal.Open()
	if err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}
	defer wal.Close()

	numGoroutines := 5
	entriesPerGoroutine := 10
	totalExpectedEntries := numGoroutines * entriesPerGoroutine

	var wg sync.WaitGroup
	var successfulAdds int64 // Use atomic for concurrent increment

	for i := range numGoroutines {
		wg.Add(1)
		go func(gID int) {
			defer wg.Done()
			for j := range entriesPerGoroutine {
				entry := MockEntry{ID: gID*100 + j, Content: fmt.Sprintf("goroutine-%d-entry-%d", gID, j)}
				if err := wal.Add(entry); err != nil { // This call will block
					t.Errorf("Goroutine %d: Failed to add entry: %v", gID, err)
				} else {
					atomic.AddInt64(&successfulAdds, 1)
				}
				time.Sleep(20 * time.Millisecond) // Simulate some work between adds
			}
		}(i)
	}

	wg.Wait() // Wait for all Add calls (and their flushes) to complete

	flusher.mu.Lock()
	defer flusher.mu.Unlock()

	if successfulAdds != int64(totalExpectedEntries) {
		t.Errorf("Expected %d entries to be successfully added, got %d", totalExpectedEntries, successfulAdds)
	}
	if len(flusher.flushedData) != totalExpectedEntries {
		t.Errorf("Expected %d entries to be flushed, got %d", totalExpectedEntries, len(flusher.flushedData))
	}

	// Verify all entries are present (order might not be preserved due to concurrency/batching)
	flushedMap := make(map[int]bool)
	for _, entry := range flusher.flushedData {
		flushedMap[entry.ID] = true
	}
	for i := range numGoroutines {
		for j := range entriesPerGoroutine {
			expectedID := i*100 + j
			if !flushedMap[expectedID] {
				t.Errorf("Entry with ID %d was not flushed", expectedID)
			}
		}
	}
}

func TestWalFlusherErrorHandling(t *testing.T) {
	flusher := &MockFlusher{}
	flusher.flushError = errors.New("simulated flush error") // Simulate an error during flush

	wal, _ := NewWal(WalConfig[MockEntry]{
		Flusher:             flusher,
		FlushInterval:       50 * time.Millisecond,
		FlushThresholdBytes: 1, // Flush on every entry
	})

	err := wal.Open()
	if err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}
	defer wal.Close() // Ensure close is called

	entry := MockEntry{ID: 1, Content: "error test"}
	// This Add call will block and return the error from the flusher
	addErr := wal.Add(entry)

	if addErr == nil {
		t.Errorf("Expected Add to return an error, got nil")
	}
	if addErr.Error() != "simulated flush error" {
		t.Errorf("Expected specific flush error, got: %v", addErr)
	}

	// Verify that the error was propagated from the flusher to the Add caller
	flusher.mu.Lock()
	if flusher.flushCount != 1 {
		t.Errorf("Expected flusher to be called once, got: %d", flusher.flushCount)
	}
	flusher.mu.Unlock()

	// Add another entry, it should still work (flusher error doesn't stop the WAL)
	flusher.reset() // Clear the previous error
	entry2 := MockEntry{ID: 2, Content: "another test"}
	addErr2 := wal.Add(entry2) // This call will block and succeed
	if addErr2 != nil {
		t.Errorf("Expected Add to succeed after resetting flusher error, got: %v", addErr2)
	}
	flusher.mu.Lock()
	if flusher.flushCount != 1 { // This is the first flush since reset
		t.Errorf("Expected flusher to be called again, got: %d", flusher.flushCount)
	}
	if len(flusher.flushedData) != 1 || flusher.flushedData[0] != entry2 {
		t.Errorf("Expected entry2 to be flushed, got: %+v", flusher.flushedData)
	}
	flusher.mu.Unlock()
}

func TestWalShutdownFlush(t *testing.T) {
	flusher := &MockFlusher{}
	wal, _ := NewWal(WalConfig[MockEntry]{
		Flusher:             flusher,
		FlushInterval:       10 * time.Second, // Long interval, rely on shutdown flush
		FlushThresholdBytes: 100000,           // High threshold
	})

	err := wal.Open()
	if err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}

	entry1 := MockEntry{ID: 100, Content: "shutdown entry 1"}
	entry2 := MockEntry{ID: 101, Content: "shutdown entry 2"}

	var wg sync.WaitGroup // Use a WaitGroup to wait for Add calls to complete
	wg.Add(2)
	var addErr1, addErr2 error

	go func() {
		defer wg.Done()
		addErr1 = wal.Add(entry1) // This will block until flushed by Close
	}()
	go func() {
		defer wg.Done()
		addErr2 = wal.Add(entry2) // This will block until flushed by Close
	}()

	// Give a moment for entries to be buffered, but not enough for flush interval/threshold.
	// Add calls will be blocked by the '<-req.done' in wal.Add().
	time.Sleep(50 * time.Millisecond)

	// Now close the WAL, which should trigger a final flush and unblock the Add calls.
	closeErr := wal.Close()
	if closeErr != nil {
		t.Fatalf("Failed to close WAL: %v", closeErr)
	}

	wg.Wait() // Wait for the Add goroutines to unblock and finish

	if addErr1 != nil {
		t.Errorf("Add error for entry1: %v", addErr1)
	}
	if addErr2 != nil {
		t.Errorf("Add error for entry2: %v", addErr2)
	}

	flusher.mu.Lock()
	defer flusher.mu.Unlock()

	if flusher.flushCount != 1 { // Should be exactly one flush on close
		t.Errorf("Expected 1 flush on close, got: %d", flusher.flushCount)
	}
	if len(flusher.flushedData) != 2 {
		t.Errorf("Expected 2 entries to be flushed during shutdown, got: %d", len(flusher.flushedData))
	}
	// Check that the context passed to the final flush is NOT canceled (it uses context.Background())
	if flusher.flushContext.DoneCalled {
		t.Errorf("Expected flush context to NOT be canceled for final flush, but it was.")
	}

	// Ensure the entries are indeed the ones we added
	found1 := false
	found2 := false
	for _, entry := range flusher.flushedData {
		if entry == entry1 {
			found1 = true
		}
		if entry == entry2 {
			found2 = true
		}
	}
	if !found1 || !found2 {
		t.Errorf("Not all expected entries were flushed during shutdown: %+v", flusher.flushedData)
	}
}

// NOTE: This getEntrySize function is expected to be part of the `wal` package.
// If your `wal.go` uses `return len(fmt.Sprintf("%v", entry))` for non-Sizer types,
// then the test expectations for non-Sizer types below should be adjusted accordingly.
// Based on the provided `wal.go` (not `wal-3.go`), it returns `1`.
// func getEntrySize(entry any) int {
// 	if s, ok := entry.(Sizer); ok {
// 		return s.Size()
// 	}
// 	return 1
// }

func TestGetEntrySize(t *testing.T) {
	// Test with a Sizer implementation
	entrySizer := &MockEntry{ID: 1, Content: "hello"}
	expectedSize := len(entrySizer.Content) + 4 // 5 for "hello" + 4 for int ID
	if getEntrySize(entrySizer) != expectedSize {
		t.Errorf("Expected size %d for Sizer, got %d", expectedSize, getEntrySize(entrySizer))
	}

	// Test with a non-Sizer type (string)
	// Based on wal.go, getEntrySize returns 1 for non-Sizer types.
	entryString := "plain string"
	if getEntrySize(entryString) != 1 {
		t.Errorf("Expected size 1 for string (non-Sizer in wal.go), got %d", getEntrySize(entryString))
	}

	// Test with an integer
	// Based on wal.go, getEntrySize returns 1 for non-Sizer types.
	entryInt := 12345
	if getEntrySize(entryInt) != 1 {
		t.Errorf("Expected size 1 for int (non-Sizer in wal.go), got %d", getEntrySize(entryInt))
	}

	// Test with an empty struct that doesn't implement Sizer
	// Based on wal.go, getEntrySize returns 1 for non-Sizer types.
	type EmptyStruct struct{}
	empty := EmptyStruct{}
	if getEntrySize(empty) != 1 {
		t.Errorf("Expected size 1 for empty struct (non-Sizer in wal.go), got %d", getEntrySize(empty))
	}
}

func TestWalFlushContextCancellation(t *testing.T) {
	flusher := &MockFlusher{}
	flusher.flushDelay = 500 * time.Millisecond // Increase flush delay to ensure it's in progress

	wal, _ := NewWal(WalConfig[MockEntry]{ // Use WalConfig
		Flusher:             flusher,
		FlushInterval:       10 * time.Second, // Long interval so it doesn't flush on its own
		FlushThresholdBytes: 100000,
	})

	err := wal.Open()
	if err != nil {
		t.Fatalf("Failed to open WAL: %v", err)
	}

	// Channel to signal that wal.Add has started blocking.
	addStarted := make(chan struct{})
	var addErr error // To capture error from Wal.Add

	// Call Add in a goroutine, as it will block until flushed.
	go func() {
		close(addStarted) // Signal that Add has been called and is about to block.
		addErr = wal.Add(MockEntry{ID: 1, Content: "slow flush entry"})
	}()

	<-addStarted // Wait for Add to be called and start blocking.

	// Give the flush a moment to actually start its long operation within the mock.
	// This ensures the MockFlusher.Flush goroutine is running and can receive context.
	time.Sleep(50 * time.Millisecond)

	// Now close the WAL. This will cancel the WAL's internal context,
	// but the final flush on close uses context.Background().
	closeErr := wal.Close()
	if closeErr != nil {
		t.Fatalf("Failed to close WAL: %v", closeErr)
	}

	// After wal.Close(), the Add call should have unblocked.
	// Since the final flush uses context.Background(), Add will return nil if successful.
	if addErr != nil { // Changed from context.Canceled
		t.Errorf("Expected wal.Add to return nil error, got: %v", addErr)
	}

	flusher.mu.Lock()
	defer flusher.mu.Unlock()

	if flusher.flushCount != 1 {
		t.Fatalf("Expected 1 flush call, got %d", flusher.flushCount)
	}
	// Expected to be false because the final flush uses context.Background()
	if flusher.flushContext.DoneCalled { // Changed from !flusher.flushContext.DoneCalled
		t.Errorf("Expected flush context to NOT be canceled for final flush, but it was. Flush context info: %+v", flusher.flushContext)
	}
	// Expected to be nil because the final flush uses context.Background()
	if flusher.flushContext.Err != nil { // Changed from context.Canceled
		t.Errorf("Expected flush context error to be nil, got: %v", flusher.flushContext.Err)
	}
}
