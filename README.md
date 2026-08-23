# batchqlite
Small, opinionated SQLite Batch Writer Library for Go

## About

Batchqlite is a single-writer batching layer over `database/sql` that provides safe, high-performance writes.

It is not a database driver or an ORM. It is highly opinionated with a small, focused api. Four write methods and two read methods. That's it.

It works by batching writes into shared transactions while retaining per-query errors and responses.

Usable with any `database/sql` compatible driver.

## Why it exists

SQLite runs the world. And yet some people think it can't run their app. "A single writer means writes are too slow."

It's a lot faster than it gets credit for, but the single writer pattern is a bottleneck for developers who are used to firing up a database connection and throwing in queries.

All Batchqlite does is take a common pattern (The Batch Writer) and package it in a simple, easy to use library.

Queue up writes, write as a batch, queue up some more. One fsync for every batch instead of every query.

Combine that with sensible default settings like WAL mode and it's off to the races.

Early numbers are promising. A naive sqlite loop gets 100 to 1000 writes per second. Batchql gets 180k writes on a $6 commodity vps. 530k writes on a Macbook Pro.

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
    b := batchqlite.NewBatchqlite()
    b.Open("sqlite3", "/tmp/data.db");
```

Having trouble with `cgo`? Try this:

```
      _ "modernc.org/sqlite"
   
```

And then:

```
    b := batchqlite.NewBatchqlite()
    b.Open("sqlite", "/tmp/data.db");
```


## README TODOs

- Quick start example

- Write methods

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
