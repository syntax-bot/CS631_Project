SET qcache.debug = on;
CREATE TABLE books (id int, title text, price numeric);
INSERT INTO books VALUES (1, 'Database Concepts', 900), (2, 'Postgres Internals', 650);
SELECT qcache_reset();

-- 1. first run: a miss
SELECT count(*), sum(price) FROM books;
-- 2. same text again: a hit
SELECT count(*), sum(price) FROM books;
-- 3. a write, committed on its own
UPDATE books SET price = 700 WHERE id = 2;
-- 4. the saved result is gone: a miss again, with the new sum
SELECT count(*), sum(price) FROM books;
-- 5. queries that are never cached, and why
SELECT count(*) FROM books WHERE random() < 2;
BEGIN;
INSERT INTO books VALUES (3, 'SQL Basics', 300);
SELECT count(*), sum(price) FROM books;
ROLLBACK;
ALTER TABLE books ADD COLUMN pages int;
SELECT * FROM qcache_stats();
DROP TABLE books;
