package batchqlite_test

import (
	"context"
	"path/filepath"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/michaelchisari/batchqlite"
)

func TestBatchqliteSimpleExecAndQuery(t *testing.T) {
	dir := t.TempDir()

	dbPath := filepath.Join(dir, "test.db")

	b, err := batchqlite.NewBatchqlite()
	if err != nil {
		t.Fatalf("couldn't create batchqlite: %v", err)
	}

	err = b.Open("sqlite3", dbPath)
	if err != nil {
		t.Fatalf("failed to open database: %v", err)
	}

	ctx := context.Background()

	err = b.ExecNow(ctx, "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)")
	if err != nil {
		t.Fatalf("failed to exec now: %v", err)
	}

	pending, err := b.ExecAndWait(ctx, "INSERT INTO items (name) VALUES (?)", "widget")
	if err != nil {
		t.Fatalf("failed to exec and wait: %v", err)
	}

	result, err := pending.Wait(ctx)

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		t.Fatalf("could not get rows affected: %v", err)
	}
	if rowsAffected != 1 {
		t.Fatalf("insert failed: %v", err)
	}

	rows, err := b.Query(ctx, "SELECT name FROM items WHERE id = ?", 1)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var name string
		err := rows.Scan(&name)
		if err != nil {
			t.Fatalf("could not scan rows: %v", err)
		}

		if name != "widget" {
			t.Fatalf("name doesn't match")
		}
	}

	err = b.Close()
	if err != nil {
		t.Fatalf("could not close: %v", err)
	}
}
