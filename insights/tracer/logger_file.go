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

package tracer

import (
	"context"
	"crypto/rand"
	"encoding/base32"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type JournalFileName struct {
	JournalId string
	Uid       string
	SeqId     uint64
	Ext       string
}

func (file *JournalFileName) String() string {
	return fmt.Sprintf("%s.%s.%016x.%s", file.JournalId, file.Uid, file.SeqId, file.Ext)
}

func ParseJournalFileName(filename string) *JournalFileName {
	// Pattern: <journalId>.<uid>.<seqId>.<ext>
	re := regexp.MustCompile(`^(.+?)\.([0-9A-V=]+)\.([0-9A-Fa-f]{16})\.(.+?)$`)
	matches := re.FindStringSubmatch(filename)
	if len(matches) != 5 {
		return nil
	}

	seqId, err := strconv.ParseUint(matches[3], 16, 64)
	if err != nil {
		return nil
	}

	return &JournalFileName{
		JournalId: matches[1],
		Uid:       matches[2],
		SeqId:     seqId,
		Ext:       matches[4],
	}
}

type LogEntry struct {
	Timestamp    time.Time
	Level        string
	TraceId      string
	File         string
	FileLine     int
	FuncName     string
	ErrorMessage string
	Message      string
}

func (e *LogEntry) String() string {
	date := time.Now().Format(time.RFC3339)

	if e.ErrorMessage == "" {
		return fmt.Sprintf("%s [%s] %s:%d %s() %s %s\n", date, e.TraceId, e.File, e.FileLine, e.FuncName, e.Level, e.Message)
	}
	return fmt.Sprintf("%s [%s] %s:%d %s() %s %s: %s\n", date, e.TraceId, e.File, e.FileLine, e.FuncName, e.Level, e.Message, e.ErrorMessage)
}

// Reset clears the LogEntry fields for reuse
func (e *LogEntry) Reset() {
	e.Timestamp = time.Time{}
	e.Level = ""
	e.TraceId = ""
	e.File = ""
	e.FileLine = 0
	e.FuncName = ""
	e.ErrorMessage = ""
	e.Message = ""
}

type FileLogger struct {
	logDir      string
	journalId   string
	maxFileSize int64
	maxBackups  int
	maxAge      time.Duration

	mu            sync.Mutex
	currentFile   *os.File
	currentSize   int64
	currentUid    string
	currentSeqId  uint64
	buffer        []byte
	bufferSize    int
	maxBufferSize int
	flushInterval time.Duration
	lastFlush     time.Time
	done          chan struct{}
	wg            sync.WaitGroup
	entryPool     sync.Pool
}

func NewFileLogger(logDir string, logName string, maxSize int, maxBackups int, maxAge time.Duration) (*FileLogger, error) {
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create log directory: %w", err)
	}

	logger := &FileLogger{
		logDir:        logDir,
		journalId:     logName,
		maxFileSize:   int64(maxSize),
		maxBackups:    maxBackups,
		maxAge:        maxAge,
		buffer:        make([]byte, 0, 1024*1024), // 1MB initial capacity
		maxBufferSize: 1024 * 1024,                // 1MB buffer threshold
		flushInterval: 250 * time.Millisecond,     // 250ms flush interval
		lastFlush:     time.Now(),
		done:          make(chan struct{}),
		entryPool: sync.Pool{
			New: func() any {
				return &LogEntry{}
			},
		},
	}

	if err := logger.createNewFile(); err != nil {
		return nil, fmt.Errorf("failed to create initial log file: %w", err)
	}

	// Set this logger as the output for standard Go log
	log.SetFlags(0)
	log.SetOutput(logger)

	// Start the flush goroutine
	logger.wg.Add(1)
	go logger.flushLoop()

	return logger, nil
}

func (f *FileLogger) getLogEntry() *LogEntry {
	return f.entryPool.Get().(*LogEntry)
}

func (f *FileLogger) putLogEntry(entry *LogEntry) {
	entry.Reset()
	f.entryPool.Put(entry)
}

func (f *FileLogger) createNewFile() error {
	f.currentUid = generateUniqueId()
	f.currentSeqId = 0

	filename := f.generateFileName()
	filepath := filepath.Join(f.logDir, filename)

	file, err := os.OpenFile(filepath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}

	if f.currentFile != nil {
		f.currentFile.Close()
	}

	f.currentFile = file
	f.currentSize = 0

	return nil
}

func (f *FileLogger) generateFileName() string {
	return fmt.Sprintf("%s.%s.%016x.log", f.journalId, f.currentUid, f.currentSeqId)
}

func (f *FileLogger) rotateFile() error {
	if f.currentFile != nil {
		f.currentFile.Close()
		f.currentFile = nil
	}

	f.currentSeqId++
	filename := f.generateFileName()
	filepath := filepath.Join(f.logDir, filename)

	file, err := os.OpenFile(filepath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}

	f.currentFile = file
	f.currentSize = 0

	go f.cleanupOldFiles()

	return nil
}

func (f *FileLogger) cleanupOldFiles() {
	entries, err := os.ReadDir(f.logDir)
	if err != nil {
		return
	}

	var journalFiles []JournalFileName
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		if parsed := ParseJournalFileName(entry.Name()); parsed != nil && parsed.JournalId == f.journalId {
			journalFiles = append(journalFiles, *parsed)
		}
	}

	// Remove files older than maxAge using embedded timestamps
	if f.maxAge > 0 {
		cutoff := time.Now().Add(-f.maxAge)
		var filesToRemove []JournalFileName

		for _, file := range journalFiles {
			// Extract timestamp from UID instead of using filesystem times
			if timestamp, err := extractTimestampFromUniqueId(file.Uid); err == nil {
				if timestamp.Before(cutoff) {
					filesToRemove = append(filesToRemove, file)
				}
			}
		}

		// Remove old files
		for _, file := range filesToRemove {
			filename := file.String()
			filePath := filepath.Join(f.logDir, filename)
			os.Remove(filePath)
		}

		// Update journalFiles list after removal
		var remainingFiles []JournalFileName
		for _, file := range journalFiles {
			found := false
			for _, removed := range filesToRemove {
				if file.Uid == removed.Uid && file.SeqId == removed.SeqId {
					found = true
					break
				}
			}
			if !found {
				remainingFiles = append(remainingFiles, file)
			}
		}
		journalFiles = remainingFiles
	}

	// Keep only maxBackups files, sorted by timestamp and seqId
	if f.maxBackups > 0 && len(journalFiles) > f.maxBackups {
		// Sort files by timestamp (from UID) and then by seqId
		sort.Slice(journalFiles, func(i, j int) bool {
			// Extract timestamps for comparison
			timeI, errI := extractTimestampFromUniqueId(journalFiles[i].Uid)
			timeJ, errJ := extractTimestampFromUniqueId(journalFiles[j].Uid)

			if errI != nil || errJ != nil {
				// Fallback to seqId comparison if timestamp extraction fails
				return journalFiles[i].SeqId < journalFiles[j].SeqId
			}

			// Compare by timestamp first
			if !timeI.Equal(timeJ) {
				return timeI.Before(timeJ)
			}

			// If timestamps are equal, compare by seqId
			return journalFiles[i].SeqId < journalFiles[j].SeqId
		})

		// Remove oldest files (keep the newest maxBackups files)
		for i := 0; i < len(journalFiles)-f.maxBackups; i++ {
			filename := journalFiles[i].String()
			filePath := filepath.Join(f.logDir, filename)
			os.Remove(filePath)
		}
	}
}

func (f *FileLogger) writeEntry(entry *LogEntry) {
	f.mu.Lock()

	entryStr := entry.String()
	entryBytes := []byte(entryStr)
	entrySize := int64(len(entryBytes))

	// Return entry to pool immediately after converting to string
	f.putLogEntry(entry)

	// Check if we need to flush due to buffer size
	shouldFlush := f.bufferSize+len(entryBytes) >= f.maxBufferSize

	// Check if we need to rotate the file
	if f.currentSize+entrySize > f.maxFileSize {
		// Flush current buffer first
		f.flushBuffer()
		// Then rotate
		if err := f.rotateFile(); err != nil {
			f.mu.Unlock()
			fmt.Fprintf(os.Stderr, "Failed to rotate log file: %v\n", err)
			return
		}
	}

	// Add to buffer
	f.buffer = append(f.buffer, entryBytes...)
	f.bufferSize += len(entryBytes)

	// Flush if buffer is full
	if shouldFlush {
		f.flushBuffer()
	}

	f.mu.Unlock()
}

func (f *FileLogger) flushBuffer() {
	if f.bufferSize == 0 || f.currentFile == nil {
		return
	}

	if _, err := f.currentFile.Write(f.buffer[:f.bufferSize]); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to write to log file: %v\n", err)
		return
	}

	f.currentSize += int64(f.bufferSize)
	f.buffer = f.buffer[:0] // Reset buffer
	f.bufferSize = 0
	f.lastFlush = time.Now()

	// Sync to disk
	f.currentFile.Sync()
}

func (f *FileLogger) flushLoop() {
	defer f.wg.Done()
	ticker := time.NewTicker(f.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			f.mu.Lock()
			if time.Since(f.lastFlush) >= f.flushInterval && f.bufferSize > 0 {
				f.flushBuffer()
			}
			f.mu.Unlock()
		case <-f.done:
			f.mu.Lock()
			f.flushBuffer()
			f.mu.Unlock()
			return
		}
	}
}

func (f *FileLogger) Write(p []byte) (n int, err error) {
	// This handles standard Go log output
	message := strings.TrimSpace(string(p))
	if message == "" {
		return len(p), nil
	}

	entry := f.getLogEntry()
	entry.Timestamp = time.Now()
	entry.Level = "INFO"
	entry.File = ""
	entry.FileLine = 0
	entry.FuncName = ""
	entry.Message = message

	f.writeEntry(entry)
	return len(p), nil
}

func (f *FileLogger) EmitLogEvent(ctx context.Context, level string, errorMessage string, format string, args []any) {
	message := PlainHumanLogFormat(format, args)
	traceId := GetTraceId(ctx)
	file, fileLine, funcName := getCallerFuncFileLine(3)

	entry := f.getLogEntry()
	entry.Timestamp = time.Now()
	entry.Level = level
	entry.TraceId = traceId
	entry.File = file
	entry.FileLine = fileLine
	entry.FuncName = funcName
	entry.ErrorMessage = errorMessage
	entry.Message = message

	f.writeEntry(entry)
}

func (f *FileLogger) Close() error {
	close(f.done)
	f.wg.Wait()

	f.mu.Lock()
	defer f.mu.Unlock()

	// Final flush
	f.flushBuffer()

	if f.currentFile != nil {
		err := f.currentFile.Close()
		f.currentFile = nil
		return err
	}
	return nil
}

func extractTimestampFromUniqueId(uid string) (time.Time, error) {
	// Decode the base32 hex encoded UID
	decoded, err := base32.HexEncoding.DecodeString(uid)
	if err != nil {
		return time.Time{}, err
	}

	if len(decoded) < 4 {
		return time.Time{}, fmt.Errorf("UID too short to contain timestamp")
	}

	// Extract the epoch timestamp from the first 4 bytes
	epochSec := uint32(decoded[0])<<24 | uint32(decoded[1])<<16 | uint32(decoded[2])<<8 | uint32(decoded[3])
	return time.Unix(int64(epochSec), 0), nil
}

func generateUniqueId() string {
	epochSec := uint32(time.Now().Unix())

	var buf [10]byte
	buffer := buf[:]
	if _, err := rand.Read(buffer); err != nil {
		panic(err)
	}

	// Embed epoch seconds into the first 4 bytes
	buf[0] = byte((epochSec >> 24) & 0xff)
	buf[1] = byte((epochSec >> 16) & 0xff)
	buf[2] = byte((epochSec >> 8) & 0xff)
	buf[3] = byte(epochSec & 0xff)
	return base32.HexEncoding.EncodeToString(buffer)
}
