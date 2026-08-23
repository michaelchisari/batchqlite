# batchqlite
Small, opinionated SQLite Batch Writer Library for Go

## About

Batchqlite is a single-writer batching layer over `database/sql` that provides safe, high-performance writes.

It is not a database driver or an ORM. It is highly opinionated with a small, focused api. Four write methods and two read methods. That's it.

It works by batching writes into shared transactions while retaining per-query errors and responses.

Usable with any `database/sql` compatible driver.

## README TODOs

- Why it exists (fsync-per-commit, batching as the fix)

- Install instructions

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
