# batchqlite
Small, opinionated SQLite Batch Writer Library for Go

## About

Batchqlite is a single-writer batching layer over `database/sql` that provides safe, high-performance writes.

Not a database driver. Not an ORM. It is highly opinionated with a small, focused api. Four write methods and two read methods. That's it.

It works by batching writes into shared transactions while retaining per-query errors and responses.

Usable with any `database/sql` compatible driver.

## Why it exists

SQLite runs the world. And yet some people think it can't run their app. "A single writer means writes are too slow."

It's a lot faster than it gets credit for, but the single writer pattern is a bottleneck for developers who are used to firing up a database connection and throwing in queries.

All Batchqlite does is take a common pattern (The Batch Writer) and package it in a simple, easy to use library.

Queue up writes, write as a batch, queue up some more. One fsync for every batch instead of every query.

Combine that with sensible default settings like WAL mode and it's off to the races.

Early numbers are promising. A naive sqlite loop gets 100 to 1000 writes per second. Batchqlite gets 180k writes on a $6 commodity vps. 530k writes on a Macbook Pro.

Repeatable, robust benchmarking is coming. 

## Install instructions

`go get github.com/michaelchisari/batchqlite`

Pick a sqlite database driver. Not sure which one? Do this:

```
    import (
      _ "github.com/mattn/go-sqlite3"
    )
```

And then:

```
    b, err := batchqlite.NewBatchqlite()
    b.Open("sqlite3", "/tmp/data.db")
```

Having trouble with `cgo`? Try this:

```
      _ "modernc.org/sqlite"
   
```

And then:

```
    b, err := batchqlite.NewBatchqlite()
    b.Open("sqlite", "/tmp/data.db")
```

## Quick start example

```go
package main

import (
    "context"
    "log"

    "github.com/michaelchisari/batchqlite"
    _ "github.com/mattn/go-sqlite3"
)

func main() {
    b, err := batchqlite.NewBatchqlite()
    if err != nil {
        log.Fatal(err)
    }

    if err = b.Open("sqlite3", "/tmp/data.db"); err != nil {
        log.Fatal(err)
    }
    defer b.Close()

    ctx := context.Background()

    if err := b.ExecNow(ctx, `CREATE TABLE IF NOT EXISTS events (
        id INTEGER PRIMARY KEY,
        message TEXT NOT NULL
    )`); err != nil {
        log.Fatal(err)
    }

    // Fire-and-forget, logged on failure.
    if err := b.Exec(ctx, "INSERT INTO events (message) VALUES (?)", "hello"); err != nil {
        log.Fatal(err)
    }

    // Wait for confirmation and get the result back.
    pending, err := b.ExecAndWait(ctx, "INSERT INTO events (message) VALUES (?)", "world")
    if err != nil {
        log.Fatal(err)
    }

    result, err := pending.Wait(ctx)
    if err != nil {
        log.Fatal(err)

    }

    id, _ := result.LastInsertId()
    log.Printf("inserted row %d", id)
}
```

## Write methods

Four write methods.

Important note on the `error` returned by these methods. These errors tell whether or not the query was *successfully put in the queue*, not whether the query was successful or not.

Each method deals with success or failure differently.

`Exec` adds a query to the batch writer. Failure is logged. Queries are executed in the order they are received. First-in-first-out (FIFO).

`ExecAndWait` adds a query to the batch writer and sends the response `Pending.Wait` when it's ready. In order / FIFO.

`ExecQuiet` adds a query to the batch writer. FIFO. No response, no logging. Any failure is silent.

`ExecNow` *skips the queue* and writes right away. That means it doesn't follow the FIFO pipeline. Use only when necessary. Setting up tables on startup or rare one-off writes. Not sure whether to use it? Don't.

Not all queries are allowed. There's always a catch. The batch writer rejects queries with certain incompatible keywords with error `ErrIllegalKeywordInQuery`.

## README TODOs

- FIFO ordering guarantee

- Read methods

- Config reference

- Known limitations

- Safety notes

- Policies and behavior (OnFull, AbandonQueueOnClose, CloseTimeout, etc)

- PRAGMAs

- Checkpointing

- Context semantics

- Sentinel errors list

- Driver compatibility

- Running tests

- Performance

- Workload management

- Versioning
