# The commit window: why qcache bumps versions at PRE_COMMIT.
#
# PostgreSQL makes a commit visible to other connections BEFORE it calls
# qcache's transaction callback with XACT_EVENT_COMMIT.  This test freezes
# the writer w exactly there, using an injection point (a named spot in the
# code where a test can make a connection wait), and lets the reader r run
# its cached query inside that window.
#
#   protocol on  (default):  the PRE_COMMIT bump already marked the table
#                            "in flight", so r skips the cache and sees 101.
#   protocol off (qcache.test_skip_precommit = on, the step-1 behaviour):
#                            the old result still matches, so r gets the
#                            stale value 100 although 101 is committed.
#
# Needs a server built with --enable-injection-points and the injection_points
# test module installed (make -C postgres/src/test/modules/injection_points install).

setup
{
    CREATE EXTENSION IF NOT EXISTS injection_points;
    CREATE TABLE acc (id int PRIMARY KEY, v int);
    INSERT INTO acc VALUES (1, 100);
}

teardown
{
    DROP TABLE acc;
    DROP EXTENSION injection_points;
}

session r
setup        { SELECT qcache_reset(); }
step r_read  { SELECT v FROM acc WHERE id = 1; }

session w
setup
{
    SELECT injection_points_set_local();
    SELECT injection_points_attach('qcache-before-commit-bump', 'wait');
}
step w_protocol_off { SET qcache.test_skip_precommit = on; }
step w_update       { UPDATE acc SET v = v + 1 WHERE id = 1; }

session c
step c_wakeup { SELECT injection_points_wakeup('qcache-before-commit-bump'); }
step c_detach { SELECT injection_points_detach('qcache-before-commit-bump'); }

# 1. Protocol on.  r: 100 (miss), 100 (hit); w commits and waits in the
#    window; r reads 101 there (cache skipped); after the wakeup r reads 101.
permutation r_read r_read w_update r_read c_wakeup r_read c_detach

# 2. Protocol off.  Same steps, but r gets the STALE 100 inside the window.
permutation w_protocol_off r_read r_read w_update r_read c_wakeup r_read c_detach
