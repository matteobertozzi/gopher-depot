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

package sqlh

import (
	"context"
	"database/sql"
	"fmt"
	"iter"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"sync"
	"time"

	_ "github.com/marcboeker/go-duckdb/v2"
)

type DuckDbSnapshotReader struct {
	mu            sync.RWMutex
	db            *sql.DB
	dirPath       string
	filePrefix    string
	fileSuffix    string
	currentSeq    uint64
	currentPath   string
	activeQueries sync.WaitGroup
	watchCancel   context.CancelFunc
	watchCtx      context.Context
}

type ReloadOptions struct {
	DeleteOnClose bool
}

type WatchOptions struct {
	Interval      time.Duration
	DeleteOnClose bool
}

func NewDuckDbSnapshotReader(dirPath string, prefix string, suffix string) *DuckDbSnapshotReader {
	ctx, cancel := context.WithCancel(context.Background())

	return &DuckDbSnapshotReader{
		dirPath:     dirPath,
		filePrefix:  prefix,
		fileSuffix:  suffix,
		watchCancel: cancel,
		watchCtx:    ctx,
	}
}

func (r *DuckDbSnapshotReader) Close() error {
	// Cancel any running watchers
	if r.watchCancel != nil {
		r.watchCancel()
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	// Wait for active queries to complete
	r.activeQueries.Wait()

	if r.db != nil {
		return r.db.Close()
	}
	return nil
}

func (r *DuckDbSnapshotReader) Query(query string, args ...any) (SqlRowsScanner, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.db == nil {
		return nil, fmt.Errorf("no database loaded, call Reload() first")
	}

	r.activeQueries.Add(1)

	rows, err := r.db.Query(query, args...)
	if err != nil {
		r.activeQueries.Done()
		return nil, err
	}

	return &DuckDbTrackedRows{
		Rows:   rows,
		reader: r,
		closed: false,
	}, nil
}

func (r *DuckDbSnapshotReader) QueryRow(query string, args ...any) SqlScanner {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.db == nil {
		// Return a row that will error when scanned
		return &DuckDbTrackedRow{
			Row:    &sql.Row{},
			reader: r,
		}
	}

	// Track active query
	r.activeQueries.Add(1)

	row := r.db.QueryRow(query, args...)
	return &DuckDbTrackedRow{
		Row:    row,
		reader: r,
	}
}

// Reload scans for the latest snapshot and opens it
func (r *DuckDbSnapshotReader) Reload(options ReloadOptions) error {
	// Find latest snapshot
	fileCount, latestSeq, latestPath, err := r.findLatestSnapshot()
	if err != nil {
		return fmt.Errorf("failed to find latest snapshot: %w", err)
	}

	if fileCount == 0 {
		return fmt.Errorf("no snapshots found in directory: %s", r.dirPath)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	// Wait for active queries to complete before switching
	r.activeQueries.Wait()

	// Close current database if exists
	if r.db != nil {
		r.db.Close()
	}

	// Open new database in readonly mode
	connStr := fmt.Sprintf("%s?access_mode=READ_ONLY", latestPath)
	db, err := sql.Open("duckdb", connStr)
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}

	r.db = db
	r.currentSeq = latestSeq
	r.currentPath = latestPath

	// Delete old snapshots if requested
	if options.DeleteOnClose && fileCount > 0 {
		go r.deleteOldSnapshots(latestSeq)
	}

	return nil
}

func (r *DuckDbSnapshotReader) WatchForNewFiles(options WatchOptions) {
	if r.db == nil {
		r.Reload(ReloadOptions{DeleteOnClose: options.DeleteOnClose})
	}

	go func() {
		ticker := time.NewTicker(options.Interval)
		defer ticker.Stop()

		for {
			select {
			case <-r.watchCtx.Done():
				return
			case <-ticker.C:
				fileCount, latestSeq, _, err := r.findLatestSnapshot()
				if err != nil {
					log.Printf("failed to find latest snapshot: %v", err)
					continue // Skip this iteration on error
				}

				if fileCount == 0 {
					log.Printf("no snapshot files found")
					continue // Skip this iteration on error
				}

				r.mu.RLock()
				currentSeq := r.currentSeq
				r.mu.RUnlock()

				log.Printf("check %d > %d", latestSeq, currentSeq)
				if latestSeq > currentSeq {
					r.Reload(ReloadOptions{DeleteOnClose: options.DeleteOnClose})
				}
			}
		}
	}()
}

func (r *DuckDbSnapshotReader) WaitForDbAvailable(pollInterval time.Duration) {
	for {
		r.mu.RLock()
		avail := r.db != nil
		r.mu.RUnlock()

		if avail {
			return
		}

		log.Print("polling for db availability, not available")
		time.Sleep(pollInterval)
	}
}

func (r *DuckDbSnapshotReader) findLatestSnapshot() (int, uint64, string, error) {
	files, err := os.ReadDir(r.dirPath)
	if err != nil {
		return 0, 0, "", fmt.Errorf("failed to read directory: %w", err)
	}

	count, maxSeqId := findLatestSequenceId(files, r.filePrefix, r.fileSuffix)
	if count == 0 {
		return 0, 0, "", nil
	}

	path := filepath.Join(r.dirPath, computeDbFileName(r.filePrefix, r.fileSuffix, maxSeqId))
	return count, maxSeqId, path, nil
}

func (r *DuckDbSnapshotReader) deleteOldSnapshots(belowSeq uint64) {
	files, err := os.ReadDir(r.dirPath)
	if err != nil {
		return
	}

	for seqId, file := range findSequenceFiles(files, r.filePrefix, r.fileSuffix) {
		if seqId < belowSeq {
			filePath := filepath.Join(r.dirPath, file.Name())
			log.Printf("deleting old snapshot %d < %d: %s", seqId, belowSeq, filePath)
			os.Remove(filePath) // Ignore errors for cleanup
		}
	}
}

type DuckDbTrackedRows struct {
	*sql.Rows
	reader *DuckDbSnapshotReader
	closed bool
	mu     sync.Mutex
}

func (tr *DuckDbTrackedRows) Close() error {
	tr.mu.Lock()
	defer tr.mu.Unlock()

	if tr.closed {
		return nil
	}

	tr.closed = true
	tr.reader.activeQueries.Done()
	return tr.Rows.Close()
}

type DuckDbTrackedRow struct {
	*sql.Row
	reader *DuckDbSnapshotReader
}

func (tr *DuckDbTrackedRow) Scan(dest ...any) error {
	defer tr.reader.activeQueries.Done()
	return tr.Row.Scan(dest...)
}

type DuckDbWriter struct {
	db       *sql.DB
	filePath string
}

func NewDuckDbWriter(filePath string) (*DuckDbWriter, error) {
	db, err := sql.Open("duckdb", fmt.Sprintf("%s?access_mode=READ_WRITE&wal_autocheckpoint=128MB", filePath))
	if err != nil {
		return nil, fmt.Errorf("failed to open DuckDB: %w", err)
	}

	return &DuckDbWriter{
		db:       db,
		filePath: filePath,
	}, nil
}

func (w *DuckDbWriter) Close() error {
	return w.db.Close()
}

func (w *DuckDbWriter) Exec(query string, args ...any) (sql.Result, error) {
	return w.db.Exec(query, args...)
}

func (w *DuckDbWriter) Query(query string, args ...any) (*sql.Rows, error) {
	return w.db.Query(query, args...)
}

func (w *DuckDbWriter) QueryRow(query string, args ...any) *sql.Row {
	return w.db.QueryRow(query, args...)
}

func (w *DuckDbWriter) Snapshot(dirPath, prefix, suffix string, maxSnapshots int) error {
	// Find the latest sequence number
	fileCount, latestSeq, err := w.findLatestSequence(dirPath, prefix, suffix)
	if err != nil {
		return fmt.Errorf("failed to find latest sequence: %w", err)
	}

	if fileCount >= maxSnapshots {
		return fmt.Errorf("maximum number of snapshots reached: %d", fileCount)
	}

	filename := computeDbFileName(prefix, suffix, latestSeq+1)
	targetPath := filepath.Join(dirPath, filename)
	tmpPath := targetPath + ".tmp"

	// Copy database to temporary file
	if err = w.copyDatabase(tmpPath); err != nil {
		os.Remove(tmpPath) // Clean up on error
		return fmt.Errorf("failed to copy database: %w", err)
	}

	// Atomic rename
	if err = os.Rename(tmpPath, targetPath); err != nil {
		os.Remove(tmpPath) // Clean up on error
		return fmt.Errorf("failed to rename snapshot: %w", err)
	}

	log.Printf("snapshot %d", latestSeq+1)
	return nil
}

func (w *DuckDbWriter) findLatestSequence(dirPath, prefix, suffix string) (int, uint64, error) {
	files, err := os.ReadDir(dirPath)
	if err != nil {
		if os.IsNotExist(err) {
			// Directory doesn't exist, create it and return 0
			err = os.MkdirAll(dirPath, 0755)
			if err != nil {
				return 0, 0, fmt.Errorf("failed to create directory: %w", err)
			}
			return 0, 0, nil
		}
		return 0, 0, fmt.Errorf("failed to read directory: %w", err)
	}

	fileCount, maxSeqId := findLatestSequenceId(files, prefix, suffix)
	return fileCount, maxSeqId, nil
}

func (w *DuckDbWriter) currentCatalog() (string, error) {
	var catalog string
	row := w.db.QueryRow("SELECT current_catalog()")
	err := row.Scan(&catalog)
	return catalog, err
}

func (w *DuckDbWriter) copyDatabase(targetPath string) error {
	// Get current catalog name
	catalog, err := w.currentCatalog()
	if err != nil {
		return fmt.Errorf("failed to get current catalog: %w", err)
	}

	// Attach target database
	if _, err = w.db.Exec(fmt.Sprintf("ATTACH '%s' AS targetDb", targetPath)); err != nil {
		return fmt.Errorf("failed to attach target database: %w", err)
	}

	// Copy from source database to target
	if _, err = w.db.Exec(fmt.Sprintf("COPY FROM DATABASE %s TO targetDb", catalog)); err != nil {
		// Try to detach even if copy failed
		w.db.Exec("DETACH targetDb")
		return fmt.Errorf("failed to copy database: %w", err)
	}

	// Detach target database
	if _, err = w.db.Exec("DETACH targetDb"); err != nil {
		return fmt.Errorf("failed to detach target database: %w", err)
	}

	return nil
}

func computeDbFileName(prefix, suffix string, seqId uint64) string {
	return fmt.Sprintf("%s.%016d.%s", prefix, seqId, suffix)
}

func findLatestSequenceId(files []os.DirEntry, prefix, suffix string) (int, uint64) {
	count := 0
	maxSeqId := uint64(0)
	for seqId, _ := range findSequenceFiles(files, prefix, suffix) {
		maxSeqId = max(maxSeqId, seqId)
		count++
	}
	return count, maxSeqId
}

func findSequenceFiles(files []os.DirEntry, prefix, suffix string) iter.Seq2[uint64, os.DirEntry] {
	pattern := fmt.Sprintf(`^%s\.(\d{16})\.%s$`, regexp.QuoteMeta(prefix), regexp.QuoteMeta(suffix))
	re := regexp.MustCompile(pattern)

	return func(yield func(uint64, os.DirEntry) bool) {
		for _, file := range files {
			if matches := re.FindStringSubmatch(file.Name()); matches != nil {
				if seqId, err := strconv.ParseUint(matches[1], 10, 64); err == nil {
					if !yield(seqId, file) {
						return
					}
				}
			}
		}
	}
}
