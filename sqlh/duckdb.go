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
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/marcboeker/go-duckdb/v2"
)

var duckSeqId int64

// RawDuckDbStorage wraps a DuckDB connection with lifecycle management
type RawDuckDbStorage struct {
	mu            sync.RWMutex
	db            *sql.DB
	activeQueries sync.WaitGroup
	dbPath        string
	dbSeqId       int64
	closed        bool
	deleteOnClose bool
}

// NewRawDuckDbStorage creates a new RawDuckDbStorage instance
func NewRawDuckDbStorage(dbPath string, deleteOnClose bool) *RawDuckDbStorage {
	return &RawDuckDbStorage{
		dbSeqId:       atomic.AddInt64(&duckSeqId, 1),
		dbPath:        dbPath,
		activeQueries: sync.WaitGroup{},
		deleteOnClose: deleteOnClose,
	}
}

// Open opens the database connection
func (r *RawDuckDbStorage) Open(readOnly bool) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if !readOnly {
		if err := os.MkdirAll(filepath.Dir(r.dbPath), 0755); err != nil {
			return fmt.Errorf("failed to create directory: %w", err)
		}
	}

	var dsn string
	if readOnly {
		dsn = fmt.Sprintf("%s?access_mode=READ_ONLY", r.dbPath)
	} else {
		dsn = fmt.Sprintf("%s?access_mode=READ_WRITE&wal_autocheckpoint=128MB", r.dbPath)
	}

	db, err := sql.Open("duckdb", dsn)
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}

	r.db = db
	r.closed = false
	return nil
}

func (r *RawDuckDbStorage) rawClose() {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.db != nil {
		r.activeQueries.Wait()
		fmt.Printf("Closing %d %s\n", r.dbSeqId, r.dbPath)
		r.db.Close()
		r.db = nil

		if r.deleteOnClose {
			os.Remove(r.dbPath)
		}
	}
}

func (r *RawDuckDbStorage) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.closed = true
	go r.rawClose()
}

// Exec executes a SQL statement without returning results
func (r *RawDuckDbStorage) Exec(query string, args ...any) error {
	return r.withConnection(func(db *sql.DB) error {
		_, err := db.Exec(query, args...)
		return err
	})
}

// Query executes a SQL query and returns rows
func (r *RawDuckDbStorage) Query(query string, args ...any) (*sql.Rows, error) {
	var rows *sql.Rows
	err := r.withConnection(func(db *sql.DB) error {
		var err error
		rows, err = db.Query(query, args...)
		return err
	})
	return rows, err
}

// QueryRow executes a SQL query that returns a single row
func (r *RawDuckDbStorage) QueryRow(query string, args ...any) *sql.Row {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.db == nil || r.closed {
		// Return a row that will error when scanned
		return &sql.Row{}
	}

	r.activeQueries.Add(1)
	defer r.activeQueries.Done()

	return r.db.QueryRow(query, args...)
}

func (r *RawDuckDbStorage) withConnection(fn func(*sql.DB) error) error {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if r.db == nil || r.closed {
		return fmt.Errorf("database not connected: %s", r.dbPath)
	}

	r.activeQueries.Add(1)
	defer r.activeQueries.Done()

	return fn(r.db)
}

// CopyDatabase copies the current database to a new location
func (r *RawDuckDbStorage) CopyDatabase(copyDbPath string) error {
	tmpCopyDbPath := copyDbPath + ".tmp"

	catalog, err := r.CurrentCatalog()
	if err != nil {
		return err
	}

	if err := r.Exec(fmt.Sprintf("ATTACH '%s' as targetDb;", tmpCopyDbPath)); err != nil {
		return err
	}

	defer r.Exec("DETACH targetDb;")

	if err := r.Exec(fmt.Sprintf("COPY FROM DATABASE %s TO targetDb;", catalog)); err != nil {
		return err
	}

	return os.Rename(tmpCopyDbPath, copyDbPath)
}

// CurrentCatalog returns the current catalog name
func (r *RawDuckDbStorage) CurrentCatalog() (string, error) {
	var catalog string
	row := r.QueryRow("SELECT current_catalog()")
	err := row.Scan(&catalog)
	return catalog, err
}

// DbPath returns the database path
func (r *RawDuckDbStorage) DbPath() string {
	return r.dbPath
}

// RollingDuckDbStorage manages rolling database connections
type RollingDuckDbStorage struct {
	mu                sync.RWMutex
	db                *RawDuckDbStorage
	deleteFileOnClose bool
	closed            bool
}

// NewRollingDuckDbStorage creates a new RollingDuckDbStorage
func NewRollingDuckDbStorage(deleteFileOnClose bool) *RollingDuckDbStorage {
	return &RollingDuckDbStorage{
		deleteFileOnClose: deleteFileOnClose,
	}
}

// Open opens a new database, closing the previous one
func (r *RollingDuckDbStorage) Open(dbPath string, readOnly bool) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	log.Printf("CLOSING %s\n", r.dbPath())
	r.closeCurrentDatabase()

	log.Printf("OPENING %s\n", dbPath)
	db := NewRawDuckDbStorage(dbPath, r.deleteFileOnClose)
	if err := db.Open(readOnly); err != nil {
		return err
	}

	r.db = db
	return nil
}

// Close closes the rolling storage
func (r *RollingDuckDbStorage) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.closed = true
	r.closeCurrentDatabase()
	return nil
}

// IsOpen returns true if a database is currently open
func (r *RollingDuckDbStorage) IsOpen() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.db != nil
}

func (r *RollingDuckDbStorage) dbPath() string {
	if r.db != nil {
		return r.db.DbPath()
	}
	return ""
}

func (r *RollingDuckDbStorage) closeCurrentDatabase() {
	if r.db != nil {
		r.db.Close()
		r.db = nil
	}
}

// Query executes a SQL query
func (r *RollingDuckDbStorage) Query(query string, args ...any) (*sql.Rows, error) {
	db, err := r.waitForRollingOpen()
	if err != nil {
		return nil, err
	}
	return db.Query(query, args...)
}

// QueryRow executes a SQL query that returns a single row
func (r *RollingDuckDbStorage) QueryRow(query string, args ...any) (*sql.Row, error) {
	db, err := r.waitForRollingOpen()
	if err != nil {
		return nil, err
	}
	return db.QueryRow(query, args...), nil
}

// Exec executes a SQL statement
func (r *RollingDuckDbStorage) Exec(query string, args ...any) error {
	db, err := r.waitForRollingOpen()
	if err != nil {
		return err
	}
	return db.Exec(query, args...)
}

func (r *RollingDuckDbStorage) waitForRollingOpen() (*RawDuckDbStorage, error) {
	for {
		r.mu.RLock()
		if r.closed {
			r.mu.RUnlock()
			return nil, fmt.Errorf("database not open")
		}
		if r.db != nil {
			db := r.db
			r.mu.RUnlock()
			return db, nil
		}
		r.mu.RUnlock()

		time.Sleep(250 * time.Millisecond)
	}
}

// SeqFileRollingDuckDbReader manages sequential database files
type SeqFileRollingDuckDbReader struct {
	db          *RollingDuckDbStorage
	reloadTimer *time.Ticker
	dbDirPath   string
}

// NewSeqFileRollingDuckDbReader creates a new reader
func NewSeqFileRollingDuckDbReader(dbDirPath string, deleteFileOnClose bool) *SeqFileRollingDuckDbReader {
	return &SeqFileRollingDuckDbReader{
		db:        NewRollingDuckDbStorage(deleteFileOnClose),
		dbDirPath: dbDirPath,
	}
}

func (s *SeqFileRollingDuckDbReader) ReloadEach(interval time.Duration) {
	if s.reloadTimer != nil {
		s.reloadTimer.Stop()
	}

	ticker := time.NewTicker(interval)
	s.reloadTimer = ticker
	go func() {
		for range ticker.C {
			s.Reload()
		}
	}()
}

// Reload finds and opens the latest sequential database file
func (s *SeqFileRollingDuckDbReader) Reload() (bool, error) {
	latestPath, err := FindLatestSeqFilePath(s.dbDirPath, "", ".db")
	if err != nil {
		return false, err
	}
	if latestPath == "" {
		return false, nil
	}

	if !s.db.IsOpen() {
		if err := s.cleanupFiles(); err != nil {
			return false, err
		}
	}

	if s.db.dbPath() != latestPath {
		if err := s.db.Open(latestPath, true); err != nil {
			return false, err
		}
		if err := s.cleanupOlderFiles(latestPath); err != nil {
			return false, err
		}
	}

	return true, nil
}

// Close closes the reader
func (s *SeqFileRollingDuckDbReader) Close() error {
	if s.reloadTimer != nil {
		s.reloadTimer.Stop()
		s.reloadTimer = nil
	}
	return s.db.Close()
}

// IsOpen returns true if the reader is open
func (s *SeqFileRollingDuckDbReader) IsOpen() bool {
	return s.db.IsOpen()
}

// Query executes a SQL query
func (s *SeqFileRollingDuckDbReader) Query(query string, args ...any) (*sql.Rows, error) {
	return s.db.Query(query, args...)
}

// QueryRow executes a SQL query that returns a single row
func (s *SeqFileRollingDuckDbReader) QueryRow(query string, args ...any) (*sql.Row, error) {
	return s.db.QueryRow(query, args...)
}

// Exec executes a SQL statement
func (s *SeqFileRollingDuckDbReader) Exec(query string, args ...any) error {
	return s.db.Exec(query, args...)
}

func (s *SeqFileRollingDuckDbReader) cleanupFiles() error {
	if !s.db.deleteFileOnClose {
		return nil
	}

	files, err := FindSeqFiles(s.dbDirPath, "", ".db")
	if err != nil || len(files) == 0 {
		return err
	}

	// Remove all but the last file
	for _, file := range files[:len(files)-1] {
		if err := os.Remove(filepath.Join(s.dbDirPath, file)); err != nil {
			return err
		}
	}

	return nil
}

func (s *SeqFileRollingDuckDbReader) cleanupOlderFiles(latestPath string) error {
	if !s.db.deleteFileOnClose {
		return nil
	}

	files, err := FindSeqFiles(s.dbDirPath, "", ".db")
	if err != nil || len(files) == 0 {
		return err
	}

	latestName := filepath.Base(latestPath)
	for _, file := range files {
		if file == latestName {
			break
		}
		if err := os.Remove(filepath.Join(s.dbDirPath, file)); err != nil {
			return err
		}
	}

	return nil
}
