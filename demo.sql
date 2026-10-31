-- Checkpoint demo: run with   psql -f demo.sql
-- Needs qcache in shared_preload_libraries and CREATE EXTENSION qcache.
\timing on
\pset pager off

DROP TABLE IF EXISTS orders;
CREATE TABLE orders (id int, customer int, category text, amount numeric);
INSERT INTO orders
SELECT g, g % 5000, 'cat_' || (g % 20), (g % 997) * 1.5
FROM generate_series(1, 2000000) g;
ANALYZE orders;

SELECT qcache_reset();

\echo '--- first run: executed normally and saved'
SELECT category, count(*), round(avg(amount), 2) AS avg_amount
FROM orders GROUP BY category ORDER BY category LIMIT 5;

\echo '--- second run: served from the cache'
SELECT category, count(*), round(avg(amount), 2) AS avg_amount
FROM orders GROUP BY category ORDER BY category LIMIT 5;

SELECT * FROM qcache_stats();

\echo '--- a write to orders invalidates the saved result'
UPDATE orders SET amount = amount + 1000 WHERE category = 'cat_0';

\echo '--- executed again, with the new data'
SELECT category, count(*), round(avg(amount), 2) AS avg_amount
FROM orders GROUP BY category ORDER BY category LIMIT 5;

SELECT * FROM qcache_stats();
SELECT * FROM qcache_entries();
