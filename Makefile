MODULE_big = qcache
OBJS = qcache.o
PGFILEDESC = "qcache - basic query result cache"

EXTENSION = qcache
DATA = qcache--1.0.sql

REGRESS = qcache

PG_CONFIG ?= pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
