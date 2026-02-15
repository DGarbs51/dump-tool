#!/bin/bash
set -euo pipefail

NEON_HOST="your-neon-host"
NEON_USER="neon_user"
AWS_HOST="your-aws-host"
AWS_USER="new_admin"
DUMP_DIR="/tmp/pg_migration"
MAX_PARALLEL=10
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

mkdir -p "$DUMP_DIR"

# Get database list
echo "[$(date)] Fetching database list..."
DBS=$(psql -h "$NEON_HOST" -U "$NEON_USER" -d postgres -Atc \
  "SELECT datname FROM pg_database
   WHERE datname NOT IN ('template0','template1','postgres')
   ORDER BY datname")

DB_COUNT=$(echo "$DBS" | wc -l)
echo "[$(date)] Found $DB_COUNT databases to migrate"

# ─── STEP 1: PUT APP IN MAINTENANCE MODE ───
echo ""
echo "============================================"
echo " START DOWNTIME NOW"
echo " Put your app in maintenance mode, then press Enter"
echo "============================================"
read -r

# ─── STEP 2: Dump all databases in parallel ───
echo "[$(date)] Dumping all databases..."
COMPLETED=0

for db in $DBS; do
  (
    pg_dump -h "$NEON_HOST" -U "$NEON_USER" -d "$db" \
      --no-owner --no-privileges --no-acl \
      -Fc -Z3 -f "${DUMP_DIR}/${db}.dump" 2>/dev/null
    echo "[$(date)] Dumped: $db"
  ) &
  [ $(jobs -r | wc -l) -ge $MAX_PARALLEL ] && wait -n
done
wait
echo "[$(date)] All dumps complete"

# ─── STEP 3: Create databases and restore in parallel ───
echo "[$(date)] Restoring all databases..."

for db in $DBS; do
  (
    createdb -h "$AWS_HOST" -U "$AWS_USER" "$db" 2>/dev/null || true
    pg_restore -h "$AWS_HOST" -U "$AWS_USER" -d "$db" \
      --no-owner --no-privileges --no-acl \
      -j 4 "${DUMP_DIR}/${db}.dump" 2>/dev/null
    echo "[$(date)] Restored: $db"
  ) &
  [ $(jobs -r | wc -l) -ge $MAX_PARALLEL ] && wait -n
done
wait
echo "[$(date)] All restores complete"

# ─── STEP 4: Verify ───
echo "[$(date)] Verifying database counts..."
NEON_COUNT=$(psql -h "$NEON_HOST" -U "$NEON_USER" -d postgres -Atc \
  "SELECT count(*) FROM pg_database WHERE datname NOT IN ('template0','template1','postgres')")
AWS_COUNT=$(psql -h "$AWS_HOST" -U "$AWS_USER" -d postgres -Atc \
  "SELECT count(*) FROM pg_database WHERE datname NOT IN ('template0','template1','postgres')")

echo "Neon databases: $NEON_COUNT"
echo "AWS databases:  $AWS_COUNT"

if [ "$NEON_COUNT" -eq "$AWS_COUNT" ]; then
  echo "✓ Database count matches"
else
  echo "✗ MISMATCH — investigate before proceeding"
  exit 1
fi

# Spot-check row counts on a few databases
echo ""
echo "Spot-checking row counts on first 5 databases..."
for db in $(echo "$DBS" | head -5); do
  NEON_ROWS=$(psql -h "$NEON_HOST" -U "$NEON_USER" -d "$db" -Atc \
    "SELECT sum(n_live_tup) FROM pg_stat_user_tables" 2>/dev/null || echo "0")
  AWS_ROWS=$(psql -h "$AWS_HOST" -U "$AWS_USER" -d "$db" -Atc \
    "SELECT sum(n_live_tup) FROM pg_stat_user_tables" 2>/dev/null || echo "0")
  STATUS=$( [ "$NEON_ROWS" = "$AWS_ROWS" ] && echo "✓" || echo "✗" )
  echo "  $STATUS $db — Neon: $NEON_ROWS rows, AWS: $AWS_ROWS rows"
done

# ─── STEP 5: Switch connection strings ───
echo ""
echo "============================================"
echo " UPDATE YOUR CONNECTION STRINGS NOW"
echo " Point everything to: $AWS_HOST"
echo " Then bring the app out of maintenance mode"
echo "============================================"
echo ""
echo "[$(date)] Migration complete"
echo "Total dump size: $(du -sh $DUMP_DIR | cut -f1)"

# Cleanup
# rm -rf "$DUMP_DIR"
