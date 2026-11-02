#!/bin/bash
# How much does a COMMIT cost when the cache is full?  (experiment E6)
# Fills ONE connection's cache with N results, then times 200 single-row
# UPDATE commits on a table that none of the cached results read.
# Usage: bench/commit_cost.sh [database] [N]
# Use a release build for real numbers: an --enable-cassert build checks all
# memory after every statement, which adds time that grows with N.
DB=${1:-postgres}
N=${2:-20000}
SQL=$(mktemp)
psql -qd "$DB" -c "DROP TABLE IF EXISTS cc, other;
  CREATE TABLE cc (id int PRIMARY KEY, v int) WITH (autovacuum_enabled = off);
  INSERT INTO cc SELECT g, g FROM generate_series(1, $N) g;
  CREATE TABLE other (id int) WITH (autovacuum_enabled = off);
  INSERT INTO other VALUES (1);"
{
  echo "SELECT qcache_reset();"
  for i in $(seq 1 "$N"); do echo "SELECT v FROM cc WHERE id = $i;"; done
  echo "SELECT 'entries=' || entries FROM qcache_stats();"
  echo "\\timing on"
  for i in $(seq 1 200); do echo "UPDATE other SET id = id + 1;"; done
} > "$SQL"
psql -qAtd "$DB" -f "$SQL" | grep -E "^entries=|^Time" > "$SQL.out"
grep entries= "$SQL.out" | tr '\n' ' '
grep Time "$SQL.out" | awk '{print $2}' | sort -n |
  awk '{a[NR] = $1} END {printf "median commit = %.3f ms\n", a[int(NR / 2)]}'
rm -f "$SQL" "$SQL.out"
