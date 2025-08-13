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
	"path"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

type SqliteStorage struct {
	db          *sql.DB
	readonly    bool
	vacuumTimer *time.Ticker
	mu          sync.Mutex    // Mutex to protect DB operations if needed, particularly for vacuum
	closeChan   chan struct{} // Channel to signal the vacuum timer to stop
}

func NewSqliteTable[T SqlRowLoader](s *SqliteStorage, name string) *SqlTable[T] {
	return NewSqlTable[T](s.db, name)
}

func SqliteStorageOpenReadOnly(dbPath string) (*SqliteStorage, error) {
	db, err := sql.Open("sqlite3", fmt.Sprintf("file:%s?mode=ro", dbPath))
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	return &SqliteStorage{
		db:        db,
		readonly:  true,
		closeChan: make(chan struct{}),
	}, nil
}

func SqliteStorageOpenReadWrite(dbPath string) (*SqliteStorage, error) {
	return sqliteStorageOpenReadWrite(dbPath, false)
}

func SqliteStorageOpenReadWriteWithInMemoryJournal(dbPath string) (*SqliteStorage, error) {
	return sqliteStorageOpenReadWrite(dbPath, true)
}

func sqliteStorageOpenReadWrite(dbPath string, inMemoryJournal bool) (*SqliteStorage, error) {
	os.MkdirAll(path.Dir(dbPath), 0750)

	// For read-write, allow normal opening, then apply pragmas
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	wrapper := &SqliteStorage{
		db:        db,
		readonly:  false,
		closeChan: make(chan struct{}),
	}

	if inMemoryJournal {
		// Use journal_mode = MEMORY
		if _, err := db.Exec("PRAGMA journal_mode=MEMORY;"); err != nil {
			wrapper.Close() // Close the DB on error
			return nil, fmt.Errorf("failed to set journal_mode=MEMORY: %w", err)
		}
		log.Println("Set PRAGMA journal_mode=MEMORY")
	} else {
		// Use WAL journal mode for better performance and durability
		if _, err := db.Exec("PRAGMA journal_mode=WAL;"); err != nil {
			wrapper.Close() // Close the DB on error
			return nil, fmt.Errorf("failed to set journal_mode=WAL: %w", err)
		}
		log.Println("Set PRAGMA journal_mode=WAL")
	}

	if _, err := db.Exec("PRAGMA auto_vacuum=2;"); err != nil {
		wrapper.Close() // Close the DB on error
		return nil, fmt.Errorf("failed to set auto_vacuum=2: %w", err)
	}
	log.Println("Set PRAGMA auto_vacuum=2")

	// Start incremental vacuum timer
	wrapper.startIncrementalVacuumTimer()

	return wrapper, nil
}

// Close closes the database connection and stops the vacuum timer.
func (s *SqliteStorage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.vacuumTimer != nil {
		s.vacuumTimer.Stop()
		close(s.closeChan) // Signal the goroutine to exit
		log.Println("Stopped incremental vacuum timer.")
	}

	if s.db != nil {
		err := s.db.Close()
		s.db = nil // Prevent double close
		log.Println("Database connection closed.")
		return err
	}

	return nil
}

func (s *SqliteStorage) startIncrementalVacuumTimer() {
	s.vacuumTimer = time.NewTicker(15 * time.Minute)
	log.Println("Started incremental vacuum timer (every 15 minutes).")

	go func() {
		for {
			select {
			case <-s.vacuumTimer.C:
				s.mu.Lock() // Protect DB access during vacuum
				if s.db != nil {
					log.Println("Calling PRAGMA incremental_vacuum...")
					// The argument to incremental_vacuum is the number of pages to vacuum.
					// A value of 0 vacuums all free pages.
					if _, err := s.db.Exec("PRAGMA incremental_vacuum(0);"); err != nil {
						log.Printf("Error during incremental vacuum: %v", err)
					} else {
						log.Println("PRAGMA incremental_vacuum completed.")
					}
				}
				s.mu.Unlock()
			case <-s.closeChan:
				return // Exit goroutine when Close() is called
			}
		}
	}()
}

func (s *SqliteStorage) Exec(sql string, args ...any) (sql.Result, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.db.Exec(sql, args...)
}

func (s *SqliteStorage) Query(sql string, args ...any) (SqlRowsScanner, error) {
	//s.mu.Lock()
	//defer s.mu.Unlock()

	return s.db.Query(sql, args...)
}

func (s *SqliteStorage) QueryRow(sql string, args ...any) SqlScanner {
	//s.mu.Lock()
	//defer s.mu.Unlock()

	return s.db.QueryRow(sql, args...)
}

func (s *SqliteStorage) Prepare(query string) (*sql.Stmt, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.db.Prepare(query)
}

func (s *SqliteStorage) Begin() (*sql.Tx, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.db.Begin()
}
