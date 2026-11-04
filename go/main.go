// Reference copy of the original Go driodcopy.
//
// The Rust port keeps this behaviour and fixes three problems in this file:
//   - os.MkdirAll errors are discarded
//   - logToDB ignores insert errors
//   - every worker writes SQLite itself (SQLite is not safe for concurrent writers)
//
// This tree is not built by Cargo. It is here so the Rust version can be
// compared with the program it replaces.
package main

import (
	"database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

const (
	MaxWorkers = 10
	DBPath     = "./copy_logs.db"
)

// Task is one file copy from Source to Destination.
type Task struct {
	Source      string
	Destination string
}

func main() {
	if len(os.Args) != 3 {
		fmt.Println("Usage: driodcopy <source_dir> <dest_dir>")
		os.Exit(1)
	}
	srcDir := os.Args[1]
	dstDir := os.Args[2]

	db, err := sql.Open("sqlite3", DBPath)
	if err != nil {
		fmt.Println("Error opening database:", err)
		os.Exit(1)
	}
	defer db.Close()

	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS logs (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		source TEXT,
		destination TEXT,
		status TEXT,
		error_msg TEXT,
		timestamp DATETIME
	)`)
	if err != nil {
		fmt.Println("Error creating table:", err)
		os.Exit(1)
	}

	taskChan := make(chan Task, 100)
	var wg sync.WaitGroup

	for i := 0; i < MaxWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for task := range taskChan {
				err := copyFile(task.Source, task.Destination)
				if err != nil {
					fmt.Printf("[Worker %d] Error copying %s to %s: %v\n", workerID, task.Source, task.Destination, err)
					logToDB(db, task.Source, task.Destination, "error", err.Error())
				} else {
					fmt.Printf("[Worker %d] Successfully copied %s to %s\n", workerID, task.Source, task.Destination)
					logToDB(db, task.Source, task.Destination, "success", "")
				}
			}
		}(i)
	}

	filepath.Walk(srcDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if path == srcDir {
			return nil
		}
		rel, err := filepath.Rel(srcDir, path)
		if err != nil {
			return err
		}
		dstPath := filepath.Join(dstDir, rel)
		if info.IsDir() {
			// Bug: a failed MkdirAll is ignored, so later copies fail without a directory log.
			os.MkdirAll(dstPath, 0755)
			return nil
		}
		taskChan <- Task{Source: path, Destination: dstPath}
		return nil
	})

	close(taskChan)
	wg.Wait()
	fmt.Println("Process complete.")
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	// Bug: parent directory creation errors are ignored.
	os.MkdirAll(filepath.Dir(dst), 0755)

	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = io.Copy(out, in)
	if err != nil {
		return err
	}
	return out.Close()
}

func logToDB(db *sql.DB, source, destination, status, errMsg string) {
	_, err := db.Exec(
		`INSERT INTO logs (source, destination, status, error_msg, timestamp) VALUES (?, ?, ?, ?, ?)`,
		source, destination, status, errMsg, time.Now(),
	)
	// Bug: insert failures are discarded.
	_ = err
}
