# dump-tool

A Go CLI tool for migrating PostgreSQL databases from a source instance to a destination instance. It dumps all databases in parallel, restores them to the destination, and verifies the migration. Works with any PostgreSQL-compatible hosts (Neon, AWS RDS/Aurora, self-hosted, etc.).

## Prerequisites

- **PostgreSQL client tools**: `psql`, `pg_dump`, `pg_restore`, `createdb` must be installed and on your PATH.
- **Go 1.21+** (for building from source).

## Installation

```bash
# Build from source
go build -o dump-tool .
```

Or install globally:

```bash
go install .
```

## Usage

```bash
dump-tool migrate [flags]
```

### Interactive Mode

By default, the tool prompts for any missing connection details. For each database (source and destination) it asks for:

- **Host** — PostgreSQL server hostname or IP
- **Port** — Defaults to 5432; press Enter to accept or type a different port
- **Username** — Database user
- **Password** — Masked input (or set via env for scripting)

Run `dump-tool migrate` with no flags to use interactive mode.

### Configuration (Flags / Env)

For scripting, set via flags or environment variables:

| Flag | Env Var | Description |
|------|---------|-------------|
| `--source-host`, `-s` | `SOURCE_HOST` | Source PostgreSQL host |
| `--source-port` | `SOURCE_PORT` | Source port (default 5432) |
| `--source-user` | `SOURCE_USER` | Source username |
| `--dest-host`, `-d` | `DEST_HOST` | Destination PostgreSQL host |
| `--dest-port` | `DEST_PORT` | Destination port (default 5432) |
| `--dest-user` | `DEST_USER` | Destination username |

### Authentication

- `PGPASSWORD` — Used for both source and destination when the same
- `SOURCE_PGPASSWORD` — Source password only
- `DEST_PGPASSWORD` — Destination password only

Passwords are never read from flags (to avoid leaking in process lists). Use env vars or interactive prompts.

### Optional Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--dump-dir`, `-o` | `/tmp/pg_migration` | Directory for dump files |
| `--parallel`, `-p` | `10` | Max parallel dump/restore jobs |
| `--no-pause` | `false` | Skip maintenance mode pause (for scripting) |
| `--non-interactive` | `false` | Never prompt; fail if any required value is missing |

### Examples

**Interactive** (prompts for host, port, user, password for each database):

```bash
dump-tool migrate
```

**Scripting** (env vars, port defaults to 5432):

```bash
export SOURCE_HOST="source-host.example.com"
export SOURCE_PORT="5432"
export SOURCE_USER="db_user"
export SOURCE_PGPASSWORD="source-pass"
export DEST_HOST="dest-host.example.com"
export DEST_USER="db_user"
export DEST_PGPASSWORD="dest-pass"

dump-tool migrate --non-interactive
```

**Flags** (passwords still from env):

```bash
dump-tool migrate \
  --source-host source-host.example.com \
  --source-port 5432 \
  --source-user db_user \
  --dest-host dest-host.example.com \
  --dest-user db_user \
  --non-interactive
```

### Workflow

1. **Fetch database list** — Connects to the source and lists all databases (excluding `template0`, `template1`, `postgres`).
2. **Maintenance mode** — Prompts you to put your app in maintenance mode before proceeding (unless `--no-pause`).
3. **Dump** — Runs `pg_dump` for each database in parallel.
4. **Restore** — Creates each database on the destination and runs `pg_restore` in parallel.
5. **Verify** — Compares database counts and spot-checks row counts for the first 5 databases.
6. **Complete** — Prints instructions to update connection strings and bring the app out of maintenance.

## License

MIT
