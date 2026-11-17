#!/bin/bash
# bench.sh: everything for measuring qcache, in one file.
#
#   bench/bench.sh setup [rows]   create the test data: 8 tables orders_1..8 (default 1M rows each)
#   bench/bench.sh check          correctness under load: stale reads, transfer totals, full cache
#   bench/bench.sh run [E1 E2 ..] run experiments (default: all five), append to bench/results.csv
#   python3 bench/analyze.py      tables and figures from bench/results.csv
#
# The experiments:
#   E1  benefit of a hit       1 client, read only: heavy, cheap and never-repeated queries
#   E2  writes to read tables  0-20% writes to the tables the queries read: how fast
#                              table-level invalidation empties any result cache
#   E3  writes elsewhere       0-50% writes to orders_8; queries read orders_1..7, so
#                              results stay valid: the cost of the invalidation
#                              mechanism itself (delete-on-write vs version numbers)
#   E4  clients                1 to $MAXC clients, 5% writes elsewhere: the locks
#   E5  cost of one commit     with 1k, 10k, 50k cached results
#
# Run "source env.sh" first (pg_ctl, psql, pgbench and PGDATA must point at your
# server; use a release build, not --enable-cassert).  Settings, as environment
# variables:  DB=postgres  SECS=20 (per run)  WARM=10 (warm-up)  REPS=3
#             CLIENTS=16  MAXC=2 x cores  KEYS=3000  SHARED_BUFFERS=1GB
# Quick smoke test:  SECS=3 WARM=1 REPS=1 MAXC=4 CLIENTS=4 bench/bench.sh run
set -u
HERE=$(cd "$(dirname "$0")" && pwd)
DB=${DB:-postgres}
SECS=${SECS:-20}
WARM=${WARM:-10}
REPS=${REPS:-3}
CLIENTS=${CLIENTS:-16}
MAXC=${MAXC:-$(( $(nproc) * 2 ))}
KEYS=${KEYS:-3000}
SHARED_BUFFERS=${SHARED_BUFFERS:-1GB}
OUT=${OUT:-$HERE/results.csv}
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

sql() { PGOPTIONS="${PGOPTIONS:-} -c client_min_messages=warning" psql -qAtd "$DB" -c "$1"; }


# ---------------------------------------------------------------------------
# The configurations compared.  The server is restarted for each, because
# qcache.partitions and qcache.invalidation can only change at start.
#   mysql            one lock for everything + delete on write: the baseline
#   partitions_only  32 locks, but still delete on write
#   versions_only    one lock, but version numbers instead of deleting
#   qcache           32 locks + version numbers: our design
# ---------------------------------------------------------------------------
set_config() {
  {
    echo "shared_buffers = $SHARED_BUFFERS"
    echo "synchronous_commit = off"
    echo "qcache.max_size = 256MB"
    case $1 in
      nocache)         echo "qcache.enabled = off" ;;
      mysql)           echo "qcache.partitions = 1";  echo "qcache.invalidation = eager" ;;
      partitions_only) echo "qcache.partitions = 32"; echo "qcache.invalidation = eager" ;;
      versions_only)   echo "qcache.partitions = 1";  echo "qcache.invalidation = lazy" ;;
      qcache)          echo "qcache.partitions = 32"; echo "qcache.invalidation = lazy" ;;
      qcache_admit)    echo "qcache.partitions = 32"; echo "qcache.min_exec_time = 0.2" ;;
    esac
  } > "$PGDATA/qcache_bench.conf"
  grep -q "qcache_bench.conf" "$PGDATA/postgresql.conf" ||
    echo "include_if_exists = 'qcache_bench.conf'" >> "$PGDATA/postgresql.conf"
  pg_ctl -D "$PGDATA" -l "$HERE/server.log" restart -w > /dev/null ||
    { echo "server did not restart, see $HERE/server.log"; exit 1; }
  sql "CREATE EXTENSION IF NOT EXISTS qcache" > /dev/null 2>&1
}


# ---------------------------------------------------------------------------
# The queries pgbench runs (pgbench replaces :t, :r, :id with random values;
# -M simple sends each as plain text, which is what qcache caches).
# ---------------------------------------------------------------------------
cat > "$TMP/heavy.sql"  <<'EOF'
\set t random(1, 8)
\set r random(0, 49)
SELECT category, count(*), avg(amount) FROM orders_:t WHERE region = :r GROUP BY category ORDER BY category;
EOF
cat > "$TMP/cheap.sql"  <<'EOF'
\set t random(1, 8)
\set id random(1, 1000)
SELECT * FROM orders_:t WHERE id = :id;
EOF
cat > "$TMP/unique.sql" <<'EOF'
\set t random(1, 8)
\set id random(1, 1000000)
SELECT * FROM orders_:t WHERE id = :id;
EOF
cat > "$TMP/write.sql"  <<'EOF'
\set t random(1, 8)
\set id random(1, 1000000)
UPDATE orders_:t SET amount = amount + 1 WHERE id = :id;
EOF
cat > "$TMP/lookup.sql" <<EOF
\\set t random(1, 7)
\\set id random(1, $KEYS)
SELECT * FROM orders_:t WHERE id = :id;
EOF
cat > "$TMP/write8.sql" <<'EOF'
\set id random(1, 1000000)
UPDATE orders_8 SET amount = amount + 1 WHERE id = :id;
EOF


# ---------------------------------------------------------------------------
# setup
# ---------------------------------------------------------------------------
do_setup() {
  local rows=${1:-1000000}
  sql "CREATE EXTENSION IF NOT EXISTS qcache" > /dev/null
  for t in 1 2 3 4 5 6 7 8; do
    echo "creating orders_$t ($rows rows)"
    sql "DROP TABLE IF EXISTS orders_$t;
         CREATE TABLE orders_$t (id int PRIMARY KEY, region int, category text, amount numeric);
         INSERT INTO orders_$t SELECT g, g % 50, 'cat_' || (g % 20), (g % 997) * 1.5
           FROM generate_series(1, $rows) g;
         CREATE INDEX ON orders_$t (region);"
  done
  sql "VACUUM ANALYZE"
}


# ---------------------------------------------------------------------------
# check: three correctness tests under load, each prints PASS or FAIL.
# With the PRE_COMMIT bump switched off, test 1 sometimes catches a stale read
# (the window is a few microseconds, so not in every run; the race test in
# "make installcheck" shows it every time):
#   PGOPTIONS='-c qcache.test_skip_precommit=on' SECS=60 bench/bench.sh check
# ---------------------------------------------------------------------------
do_check() {
  local secs=${SECS} log=$TMP/bad.log

  # 1. Stale reads.  Counters only go up.  A reader first reads a counter
  #    with a query that is never cached (random() makes it uncacheable):
  #    a value already committed.  A cached value below it is stale.
  sql "CREATE EXTENSION IF NOT EXISTS qcache; DROP TABLE IF EXISTS ctr;
       CREATE TABLE ctr (id int PRIMARY KEY, v bigint);
       INSERT INTO ctr SELECT g, 0 FROM generate_series(1, 8) g;
       SELECT qcache_reset();" > /dev/null
  printf '%s\n' '\set id random(1, 8)' 'UPDATE ctr SET v = v + 1 WHERE id = :id;' > "$TMP/inc.sql"
  printf '%s\n' '\set id random(1, 8)' \
    'SELECT v AS lo FROM ctr WHERE id = :id AND random() >= 0 \gset' \
    'SELECT v AS c FROM ctr WHERE id = :id \gset' \
    '\if :c < :lo' \
    '  \shell echo STALE counter :id cached :c but :lo was already committed >> :log' \
    '\endif' > "$TMP/read.sql"
  : > "$log"
  pgbench -n -M simple -c 16 -j 16 -T "$secs" -D log="$log" \
    -f "$TMP/read.sql@90" -f "$TMP/inc.sql@10" "$DB" > /dev/null 2>&1
  report "stale reads   " "$log" "$(sql "SELECT hits FROM qcache_stats()") cache hits"

  # 2. The proposal's transfer test: transfers keep the total at 100000, so
  #    a cached SUM must always be 100000.  (Catches results that mix data from
  #    different moments; an old total would still be right, hence test 1.)
  sql "DROP TABLE IF EXISTS accounts;
       CREATE TABLE accounts (id int PRIMARY KEY, balance bigint);
       INSERT INTO accounts SELECT g, 1000 FROM generate_series(1, 100) g;
       SELECT qcache_reset();" > /dev/null
  printf '%s\n' '\set a random(1, 100)' '\set b random(1, 100)' '\set x random(1, 50)' 'BEGIN;' \
    'UPDATE accounts SET balance = balance - :x WHERE id = :a;' \
    'UPDATE accounts SET balance = balance + :x WHERE id = :b;' 'COMMIT;' > "$TMP/transfer.sql"
  printf '%s\n' 'SELECT sum(balance) AS total FROM accounts \gset' \
    '\if :total != 100000' '  \shell echo BAD total :total >> :log' '\endif' > "$TMP/total.sql"
  : > "$log"
  pgbench -n -M simple -c 16 -j 16 -T "$secs" -D log="$log" \
    -f "$TMP/total.sql@80" -f "$TMP/transfer.sql@20" "$DB" > /dev/null 2>&1
  report "transfer total" "$log" "$(sql "SELECT hits FROM qcache_stats()") cache hits"

  # 3. A full cache keeps working: save 80,000 results (more than the
  #    65,536-entry limit), then a new query must still be saved and hit.
  sql "DROP TABLE IF EXISTS ft; CREATE TABLE ft (id int PRIMARY KEY, v int);
       INSERT INTO ft SELECT g, g FROM generate_series(1, 80000) g;" > /dev/null
  { echo "SELECT qcache_reset();"
    for i in $(seq 1 80000); do echo "SELECT v FROM ft WHERE id = $i;"; done
    echo "SELECT count(*) FROM ft WHERE v > 0;"; echo "SELECT count(*) FROM ft WHERE v > 0;"
  } > "$TMP/fill.sql"
  psql -qd "$DB" -f "$TMP/fill.sql" > /dev/null
  : > "$log"
  [ "$(sql "SELECT hits FROM qcache_stats()")" = 1 ] || echo "new query not cached" > "$log"
  report "full cache    " "$log" "$(sql "SELECT evictions FROM qcache_stats()") evictions"
}

report() {   # name, log file, extra info
  if [ -s "$2" ]; then echo "$1  FAIL  ($3)"; head -3 "$2"; else echo "$1  PASS  ($3)"; fi
}


# ---------------------------------------------------------------------------
# run: the experiments
# ---------------------------------------------------------------------------
[ -f "$OUT" ] ||
  echo "config,experiment,query,clients,write_pct,entries,rep,tps,latency_ms,hits,misses,commit_ms" > "$OUT"

# One pgbench measurement: CONFIG EXPERIMENT QUERY WRITE_SCRIPT CLIENTS WRITE_PCT
measure() {
  local cfg=$1 exp=$2 query=$3 write=$4 clients=$5 wpct=$6 jobs=$5 rep out tps lat h0 m0 h1 m1
  [ "$jobs" -gt "$(nproc)" ] && jobs=$(nproc)
  local args="-n -M simple -c $clients -j $jobs -f $TMP/$query.sql@$((100 - wpct)) -f $TMP/$write.sql@$wpct"
  for rep in $(seq 1 "$REPS"); do
    sql "SELECT qcache_reset()" > /dev/null
    # Warm-up: reads only, so every configuration starts with a full cache
    pgbench -n -M simple -c "$clients" -j "$jobs" -f "$TMP/$query.sql" -T "$WARM" "$DB" > /dev/null 2>&1
    read -r h0 m0 < <(sql "SELECT hits, misses FROM qcache_stats()" | tr '|' ' ')
    out=$(pgbench $args -T "$SECS" "$DB" 2>&1)
    read -r h1 m1 < <(sql "SELECT hits, misses FROM qcache_stats()" | tr '|' ' ')
    tps=$(grep -oP '^tps = \K[0-9.]+' <<< "$out")
    lat=$(grep -oP '^latency average = \K[0-9.]+' <<< "$out")
    if [ -z "$tps" ]; then
      echo "pgbench failed:"; grep -m 2 -i error <<< "$out"; echo "(did you run: bench/bench.sh setup?)"; exit 1
    fi
    echo "$cfg,$exp,$query,$clients,$wpct,,$rep,$tps,$lat,$((h1 - h0)),$((m1 - m0))," >> "$OUT"
    printf "  %-16s %s %-6s clients=%-3s writes=%2s%%  %10.0f tps  %8s ms  hits %s%%\n" \
      "$cfg" "$exp" "$query" "$clients" "$wpct" "$tps" "$lat" \
      "$(( (h1 - h0) * 100 / ((h1 - h0) + (m1 - m0) + 1) ))"
  done
}

# Commit cost: fill the cache with N results, then time 200 one-row UPDATEs
# of a table none of them read; the median of the 200 is one repetition.
commit_cost() {
  local cfg=$1 n=$2 rep ms
  sql "DROP TABLE IF EXISTS cc, other;
       CREATE TABLE cc (id int PRIMARY KEY, v int);
       INSERT INTO cc SELECT g, g FROM generate_series(1, $n) g;
       CREATE TABLE other (id int); INSERT INTO other VALUES (1);" > /dev/null
  { echo "SELECT qcache_reset();"
    for i in $(seq 1 "$n"); do echo "SELECT v FROM cc WHERE id = $i;"; done
    echo '\timing on'
    for i in $(seq 1 200); do echo "UPDATE other SET id = id + 1;"; done
  } > "$TMP/commit.sql"
  for rep in $(seq 1 "$REPS"); do
    ms=$(psql -qAtd "$DB" -f "$TMP/commit.sql" | grep -oP '^Time: \K[0-9.]+' | sort -n | sed -n 100p)
    echo "$cfg,E5,,1,,$n,$rep,,,,,$ms" >> "$OUT"
    printf "  %-16s E5 %6s cached results: commit %s ms\n" "$cfg" "$n" "$ms"
  done
}
# Which configurations each experiment compares.
E1_CONFIGS="nocache qcache qcache_admit"
E2_CONFIGS="nocache mysql qcache"
E3_CONFIGS="nocache mysql partitions_only versions_only qcache"
E4_CONFIGS="nocache mysql partitions_only versions_only qcache"
E5_CONFIGS="mysql partitions_only qcache"

do_run() {
  local exps=" ${*:-E1 E2 E3 E4 E5} " cfg e list
   [ "$CLIENTS" -gt "$MAXC" ] && CLIENTS=$MAXC
  if pg_config --configure | grep -q enable-cassert; then
    echo "WARNING: this PostgreSQL is a debug build (--enable-cassert): numbers will be"
    echo "         slow and noisy.  Use a release build for the report."
  fi
  for cfg in nocache mysql partitions_only versions_only qcache qcache_admit; do
    local todo=""
    for e in E1 E2 E3 E4 E5; do
      list=${e}_CONFIGS
      [[ $exps == *" $e "* && " ${!list} " == *" $cfg "* ]] && todo="$todo $e"
    done
    [ -n "$todo" ] || continue

    echo "== $cfg:$todo"
    set_config "$cfg"
    for e in $todo; do
      case $e in
        E1) for q in heavy cheap unique; do measure "$cfg" E1 "$q" write 1 0; done ;;
        E2) for w in 0 1 5 20; do measure "$cfg" E2 heavy write "$CLIENTS" "$w"; done ;;
        E3) for w in 0 1 5 10 20 50; do measure "$cfg" E3 lookup write8 "$CLIENTS" "$w"; done ;;
        E4) for c in 1 2 4 8 16 32 64 128; do
              [ "$c" -le "$MAXC" ] && measure "$cfg" E4 lookup write8 "$c" 5
            done ;;
        E5) for n in 1000 10000 50000; do commit_cost "$cfg" "$n"; done ;;
      esac
    done
  done
  rm -f "$PGDATA/qcache_bench.conf"
  pg_ctl -D "$PGDATA" -l "$HERE/server.log" restart -w > /dev/null
  echo "results in $OUT; now run: python3 $HERE/analyze.py"
}


: "${PGDATA:?PGDATA is not set: run 'source env.sh' first}"
case ${1:-} in
  setup) shift; do_setup "$@" ;;
  check) do_check ;;
  run)   shift; do_run "$@" ;;
  *)     sed -n '2,26p' "$0" | sed 's/^# \{0,1\}//'; exit 1 ;;
esac
