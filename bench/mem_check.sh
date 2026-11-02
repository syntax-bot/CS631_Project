#!/bin/bash
# Does qcache.max_size match the memory really used?
# Caches N small results in ONE connection, then compares qcache's own count
# with the memory its contexts really hold.
# Usage: bench/mem_check.sh [database] [N]
DB=${1:-postgres}
N=${2:-20000}
SQL=$(mktemp)
psql -qd "$DB" -c "DROP TABLE IF EXISTS cc;
  CREATE TABLE cc (id int PRIMARY KEY, v int) WITH (autovacuum_enabled = off);
  INSERT INTO cc SELECT g, g FROM generate_series(1, $N) g;"
{
  echo "SELECT qcache_reset();"
  for i in $(seq 1 "$N"); do echo "SELECT v FROM cc WHERE id = $i;"; done
  echo "\\echo RESULT"
  echo "SELECT 'entries=' || entries || ' counted=' || pg_size_pretty(total_bytes) FROM qcache_stats();"
  echo "SELECT 'really_used=' || pg_size_pretty(sum(total_bytes)) FROM pg_backend_memory_contexts WHERE name LIKE 'qcache%';"
} > "$SQL"
psql -qAtd "$DB" -f "$SQL" | sed -n '/RESULT/,$p' | tail -n +2 | paste -sd' '
rm -f "$SQL"
