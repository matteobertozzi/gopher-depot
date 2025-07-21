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

package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/matteobertozzi/gopher-depot/wal"
)

// MyEntry is a custom data type for our application log.
type MyEntry struct {
	Timestamp time.Time
	Message   string
	Level     string
}

func (e *MyEntry) Size() int {
	return len(e.Message) + len(e.Level) + 16
}

// MyDiskFlusher is an example implementation of the WalFlusher interface.
// It simulates writing log entries to a file on disk.
type MyDiskFlusher struct {
	filePath string
	// In a real implementation, this might be a *os.File handle
}

func (f *MyDiskFlusher) Open() error {
	fmt.Printf("[Flusher] Opening resource for file: %s\n", f.filePath)
	// Here you would os.OpenFile(f.filePath, ...)
	return nil
}

func (f *MyDiskFlusher) Flush(ctx context.Context, entries []*MyEntry) error {
	fmt.Printf("--- Flushing %d entries to disk ---\n", len(entries))
	for _, myEntry := range entries {
		select {
		case <-ctx.Done():
			fmt.Println("[Flusher] Flush canceled by context.")
			return ctx.Err() // Return context error
		default:
			// a real implementation would write to a file here
			fmt.Printf("  -> Writing: Level=%s, Message=%s\n", myEntry.Level, myEntry.Message)
		}
	}
	fmt.Println("--- Flush complete ---")
	time.Sleep(50 * time.Millisecond) // Simulate I/O delay
	return nil
}

func (f *MyDiskFlusher) Close() error {
	fmt.Printf("[Flusher] Closing resource for file: %s\n", f.filePath)
	// Here you would fileHandle.Close()
	return nil
}

// Example main function to demonstrate usage.
func main() {
	// 1. Create an instance of our Flusher implementation.
	flusher := &MyDiskFlusher{filePath: "wal_demo.log"}

	// 2. Create a new WAL instance, providing the flusher.
	wal, err := wal.NewWal(wal.WalConfig[*MyEntry]{
		Flusher:             flusher,
		FlushInterval:       500 * time.Millisecond,
		FlushThresholdBytes: 256,
	})
	if err != nil {
		fmt.Printf("Failed to create WAL: %v\n", err)
		return
	}

	// 3. Open the WAL. This will also call flusher.Open().
	if err := wal.Open(); err != nil {
		fmt.Printf("Failed to open WAL: %v\n", err)
		return
	}
	defer wal.Close()

	fmt.Println("WAL is open. Adding entries from multiple goroutines...")

	var addWg sync.WaitGroup
	for i := range 5 {
		addWg.Add(1)
		go func(id int) {
			defer addWg.Done()
			for j := range 3 {
				entry := &MyEntry{
					Timestamp: time.Now(),
					Level:     "INFO",
					Message:   fmt.Sprintf("Log message %d from goroutine %d", j, id),
				}
				if err := wal.Add(entry); err != nil {
					fmt.Printf("[Goroutine %d] Error adding entry: %v\n", id, err)
				} else {
					fmt.Printf("[Goroutine %d] Successfully flushed entry %d\n", id, j)
				}
				time.Sleep(20 * time.Millisecond)
			}
		}(i)
	}

	addWg.Wait()
	fmt.Println("All entries have been added and flushed.")
}
