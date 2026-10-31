#!/bin/bash
# Compare throughput with the cache off and on.
# Run demo.sql first so the orders table exists.
# Usage: bench/run_bench.sh [database] [seconds] [clients]
set -e
DB=${1:-postgres}
SECS=${2:-20}
CLIENTS=${3:-1}
HERE=$(cd "$(dirname "$0")" && pwd)

for mode in off on; do
    echo "=== qcache.enabled = $mode ==="
    # -n: no vacuum, -M simple: the cache works with the simple protocol
    PGOPTIONS="-c qcache.enabled=$mode" \
        pgbench -n -M simple -f "$HERE/read_query.sql" \
                -T "$SECS" -c "$CLIENTS" -j "$CLIENTS" "$DB" |
        grep -E "^(tps|latency average)"
done
