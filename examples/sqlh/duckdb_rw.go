package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/matteobertozzi/gopher-depot/sqlh"
)

func demoWriter(dbPath string, snapshotDir string, snapshotInterval time.Duration) {
	writer, err := sqlh.NewDuckDbWriter(dbPath)
	if err != nil {
		log.Fatal(err)
	}

	defer writer.Close()

	_, err = writer.Exec("CREATE TABLE IF NOT EXISTS users (id INTEGER, name VARCHAR)")
	if err != nil {
		log.Fatal(err)
	}

	lastInterval := time.UnixMilli(0)
	for i := range 100 {
		_, err = writer.Exec("INSERT INTO users VALUES (?, ?)", i, fmt.Sprintf("User %d", i))
		if err != nil {
			log.Fatal(err)
		}

		if time.Since(lastInterval) > snapshotInterval {
			if err = writer.Snapshot(snapshotDir, "foo", "db", 3); err != nil {
				log.Print(err.Error())
			}
			lastInterval = time.Now()
		}

		time.Sleep(1 * time.Second)
	}
}

func demoReader(snapshotDir string) {
	// Consumer usage
	reader := sqlh.NewDuckDbSnapshotReader(snapshotDir, "foo", "db")
	defer reader.Close()

	// Start watching for new files
	reader.WatchForNewFiles(sqlh.WatchOptions{
		Interval:      3 * time.Second,
		DeleteOnClose: true,
	})

	for {
		reader.WaitForDbAvailable(1 * time.Second)

		// Query data
		rows, err := reader.Query("SELECT COUNT(*) FROM users")
		if err != nil {
			log.Fatal(err)
		}

		var count int
		if rows.Next() {
			rows.Scan(&count)
		}

		log.Printf("Found %d users", count)
		time.Sleep(1 * time.Second)

		rows.Close()
	}
}

func main() {
	// Setup
	tempDir := "/tmp/ducky_reader_test"
	dbPath := filepath.Join(tempDir, "test.db")
	snapshotDir := filepath.Join(tempDir, "snapshots")

	os.MkdirAll(tempDir, 0755)
	os.MkdirAll(snapshotDir, 0755)
	defer os.RemoveAll(tempDir)

	go demoReader(snapshotDir)
	go demoWriter(dbPath, snapshotDir, 10*time.Second)

	time.Sleep(100 * time.Second)
}
