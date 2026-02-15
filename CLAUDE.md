# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Is

A Go CLI tool for migrating all PostgreSQL databases from a source instance to a destination instance. It dumps databases in parallel using `pg_dump`, restores with `pg_restore`, and verifies migration by comparing database counts and spot-checking row counts.

## Build & Run

```bash
go build -o dump-tool .
./dump-tool migrate          # interactive mode
./dump-tool migrate --non-interactive  # scripting mode (requires env vars)
```

## Architecture

Single-command CLI built with Cobra. All meaningful logic is in `cmd/migrate.go`:

- **Config resolution**: flags → env vars → interactive prompts (in that priority order)
- **Migration pipeline**: list DBs → maintenance pause → parallel dump → parallel restore → verify counts
- **Parallelism**: semaphore channel pattern (`maxParallel` goroutines) for both dump and restore phases
- **External tools**: shells out to `pg_dump`, `pg_restore`, `createdb` via `os/exec`; passes `PGPASSWORD` via env
- **DB connections**: uses `pgx/v5` for listing databases and verification queries; connects via `postgres://` URI with `sslmode=require`

`cmd/root.go` is just the Cobra root command. `main.go` calls `cmd.Execute()`. `script.sh` is the original bash prototype that the Go tool replaces.

## Key Design Decisions

- Passwords are never accepted as CLI flags — only via env vars (`SOURCE_PGPASSWORD`, `DEST_PGPASSWORD`, `PGPASSWORD`) or interactive prompt
- System databases (`template0`, `template1`, `postgres`) are always excluded
- Dumps use custom format (`-Fc`) with compression (`-Z3`); restores use `-j 4` for parallel table restore within each database
- `createdb` errors are intentionally ignored (database may already exist)
