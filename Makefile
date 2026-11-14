MODULE_big = qcache
OBJS = qcache.o
PGFILEDESC = "qcache - basic query result cache"

EXTENSION = qcache
DATA = qcache--1.0.sql

REGRESS = qcache qcache_security
ISOLATION = qcache_cross
ISOLATION_OPTS = --load-extension=qcache

PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
ifeq ($(enable_injection_points),yes)
ISOLATION += qcache_race
endif
