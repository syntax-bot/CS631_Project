#include "postgres.h"

#include "access/htup_details.h"
#include "access/xact.h"
#include "common/hashfn.h"
#include "executor/executor.h"
#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "optimizer/optimizer.h"
#include "optimizer/planner.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/hsearch.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/tuplestore.h"

#if PG_VERSION_NUM < 180000 || PG_VERSION_NUM >= 190000
#error "This version of qcache is written for PostgreSQL 18"
#endif

PG_MODULE_MAGIC;

static bool setting_enabled = true;		
static bool setting_debug = false;		

#define DEBUG_MSG(...) \
	do { \
		if (setting_debug) \
			ereport(NOTICE, (errmsg(__VA_ARGS__))); \
	} while (0)

typedef struct CachedResult
{
	uint64		key_hash;		

	char	   *key;			
	int			key_len;
	char	   *query_text;		

	MinimalTuple *rows;			
	uint64		num_rows;
	Size		size_bytes;

	uint64		hits;			

	MemoryContext memory;		
} CachedResult;

typedef struct CurrentQuery
{
	QueryDesc  *query_desc;		
	MemoryContext memory;		

	uint64		key_hash;
	char	   *key;
	int			key_len;
	char	   *query_text;

	bool		already_ran;	
} CurrentQuery;

typedef struct RowCollector
{
	DestReceiver base;			
	DestReceiver *real_receiver;	

	MemoryContext memory;		
	MinimalTuple *rows;
	uint64		num_rows;
	uint64		capacity;		
	Size		size_bytes;
} RowCollector;

static MemoryContext cache_memory = NULL;	
static HTAB *cache_table = NULL;	
static Size cache_size_bytes = 0;	

static CurrentQuery current;	

static int	nesting_level = 0;

static PlannedStmt *plan_just_made = NULL;
static const char *plan_just_made_problem = NULL;	

static bool transaction_wrote = false;

static uint64 count_hits = 0;
static uint64 count_misses = 0;
static uint64 count_stores = 0;
static uint64 count_invalidations = 0;
static uint64 count_evictions = 0;	

static planner_hook_type prev_planner_hook = NULL;
static ExecutorStart_hook_type prev_ExecutorStart_hook = NULL;
static ExecutorRun_hook_type prev_ExecutorRun_hook = NULL;
static ExecutorEnd_hook_type prev_ExecutorEnd_hook = NULL;

void		_PG_init(void);
PG_FUNCTION_INFO_V1(qcache_stats);
PG_FUNCTION_INFO_V1(qcache_entries);
PG_FUNCTION_INFO_V1(qcache_reset);

static CachedResult *
find_cached_result(uint64 key_hash, const char *key, int key_len)
{
	CachedResult *entry;

	entry = (CachedResult *) hash_search(cache_table, &key_hash, HASH_FIND, NULL);
	if (entry == NULL)
		return NULL;

	/* Two different keys can have the same hash; compare the full keys. */
	if (entry->key_len != key_len || memcmp(entry->key, key, key_len) != 0)
		return NULL;

	return entry;
}

/* Delete one saved result and free its memory. */
static void
remove_cached_result(CachedResult *entry)
{
	MemoryContext entry_memory = entry->memory;
	uint64		key_hash = entry->key_hash;

	cache_size_bytes -= entry->size_bytes;
	hash_search(cache_table, &key_hash, HASH_REMOVE, NULL);

	MemoryContextDelete(entry_memory);
}

static int
remove_all_cached_results(void)
{
	HASH_SEQ_STATUS scan;
	CachedResult *entry;
	int			removed = 0;

	hash_seq_init(&scan, cache_table);
	while ((entry = (CachedResult *) hash_seq_search(&scan)) != NULL)
	{
		
		remove_cached_result(entry);
		removed++;
	}
	return removed;
}

/* Forget the current query and free its memory. */
static void
forget_current_query(void)
{
	if (current.memory != NULL)
		MemoryContextDelete(current.memory);
	memset(&current, 0, sizeof(current));
}

/* Forget what the current transaction changed. */
static void
forget_transaction_changes(void)
{
	transaction_wrote = false;
}

static const char *
check_query_tree(Query *query)
{
	if (query->commandType != CMD_SELECT || query->utilityStmt != NULL)
		return "not a SELECT";
	if (query->hasModifyingCTE)
		return "contains a WITH clause that changes data";
	if (query->rowMarks != NIL)
		return "uses FOR UPDATE or FOR SHARE";
	if (query->hasRowSecurity)
		return "reads a table with row-level security";

	if (contain_mutable_functions((Node *) query))
		return "calls a function that is not IMMUTABLE (like now(), random() or qcache_stats())";

	return NULL;
}

static PlannedStmt *
qcache_planner_hook(Query *query, const char *query_string, int cursor_options,
					ParamListInfo bound_params)
{
	const char *problem;
	PlannedStmt *plan;

	/* Check the tree before planning: the planner may change it. */
	problem = check_query_tree(query);

	/* Let PostgreSQL (or a hook installed before ours) make the plan. */
	if (prev_planner_hook != NULL)
		plan = prev_planner_hook(query, query_string, cursor_options, bound_params);
	else
		plan = standard_planner(query, query_string, cursor_options, bound_params);

	plan_just_made = plan;
	plan_just_made_problem = problem;

	return plan;
}

static const char *
check_plan(QueryDesc *query_desc, int eflags)
{
	PlannedStmt *plan = query_desc->plannedstmt;

	if (nesting_level > 0)
		return "runs inside another command (a function, EXPLAIN, a cursor, ...)";

	if (plan != plan_just_made)
		return "uses a saved plan (prepared statement)";
	if (plan_just_made_problem != NULL)
		return plan_just_made_problem;

	if (query_desc->operation != CMD_SELECT || plan->hasModifyingCTE ||
		plan->rowMarks != NIL || plan->dependsOnRole)
		return "not a plain SELECT";
	if ((eflags & EXEC_FLAG_EXPLAIN_ONLY) || query_desc->instrument_options != 0)
		return "EXPLAIN";
	if (query_desc->params != NULL && query_desc->params->numParams > 0)
		return "has parameters";

	/* We cannot see this transaction's own uncommitted changes. */
	if (transaction_wrote)
		return "this transaction already changed data";

	return NULL;
}

static void
build_cache_key(QueryDesc *query_desc)
{
	PlannedStmt *plan = query_desc->plannedstmt;
	const char *text = query_desc->sourceText;
	int			text_len;
	Oid			user = GetUserId();
	int			start = plan->stmt_location;
	int			length = plan->stmt_len;
	StringInfoData key;

	if (text == NULL)
		text = "";
	text_len = strlen(text);

	initStringInfo(&key);
	appendBinaryStringInfo(&key, &user, sizeof(user));
	appendBinaryStringInfo(&key, text, text_len);

	current.key = key.data;
	current.key_len = key.len;
	current.key_hash = hash_bytes_extended((const unsigned char *) key.data, key.len, 0);

	if (start < 0 || start > text_len)
		start = 0;
	if (length <= 0 || start + length > text_len)
		length = text_len - start;
	current.query_text = pnstrdup(text + start, length);
}

static void
qcache_ExecutorStart_hook(QueryDesc *query_desc, int eflags)
{
	const char *problem;
	MemoryContext old_memory;

	if (prev_ExecutorStart_hook != NULL)
		prev_ExecutorStart_hook(query_desc, eflags);
	else
		standard_ExecutorStart(query_desc, eflags);

	if (query_desc->operation != CMD_SELECT || query_desc->plannedstmt->hasModifyingCTE)
	{
		if (!transaction_wrote)
			DEBUG_MSG("qcache: this transaction changes data; the cache will be "
					  "cleared at COMMIT");
		transaction_wrote = true;
		return;
	}

	if (!setting_enabled)
		return;

	problem = check_plan(query_desc, eflags);
	if (problem != NULL)
	{
		if (nesting_level == 0)
			DEBUG_MSG("qcache: not cached: %s", problem);
		return;
	}

	forget_current_query();
	current.query_desc = query_desc;
	current.memory = AllocSetContextCreate(cache_memory, "qcache current query",
										   ALLOCSET_DEFAULT_SIZES);
	old_memory = MemoryContextSwitchTo(current.memory);
	build_cache_key(query_desc);
	MemoryContextSwitchTo(old_memory);
}

/* ---- 3a. The row collector (used on a miss) ---- */

static bool
collector_receive_row(TupleTableSlot *slot, DestReceiver *self)
{
	RowCollector *collector = (RowCollector *) self;

	MemoryContext old_memory = MemoryContextSwitchTo(collector->memory);
	MinimalTuple row = ExecCopySlotMinimalTuple(slot);	

	collector->size_bytes += row->t_len;

	/* Grow the array when it is full (64, 128, 256, ...). */
	if (collector->num_rows == collector->capacity)
	{
		if (collector->capacity == 0)
		{
			collector->capacity = 64;
			collector->rows = palloc(sizeof(MinimalTuple) * collector->capacity);
		}
		else
		{
			collector->capacity = collector->capacity * 2;
			collector->rows = repalloc(collector->rows,
									   sizeof(MinimalTuple) * collector->capacity);
		}
	}
	collector->rows[collector->num_rows] = row;
	collector->num_rows++;
	MemoryContextSwitchTo(old_memory);

	/* Hand the row on to the client, unchanged. */
	return collector->real_receiver->receiveSlot(slot, collector->real_receiver);
}

static void
collector_startup(DestReceiver *self, int operation, TupleDesc columns)
{
	RowCollector *collector = (RowCollector *) self;

	collector->real_receiver->rStartup(collector->real_receiver, operation, columns);
}

static void
collector_shutdown(DestReceiver *self)
{
	RowCollector *collector = (RowCollector *) self;

	collector->real_receiver->rShutdown(collector->real_receiver);
}

static void
collector_destroy(DestReceiver *self)
{
	
}

static RowCollector *
create_row_collector(DestReceiver *real_receiver)
{
	RowCollector *collector;

	collector = MemoryContextAllocZero(current.memory, sizeof(RowCollector));
	collector->base.receiveSlot = collector_receive_row;
	collector->base.rStartup = collector_startup;
	collector->base.rShutdown = collector_shutdown;
	collector->base.rDestroy = collector_destroy;
	collector->base.mydest = DestNone;
	collector->real_receiver = real_receiver;
	collector->memory = AllocSetContextCreate(current.memory, "qcache rows",
											  ALLOCSET_DEFAULT_SIZES);
	return collector;
}

/* ---- 3b. Saving a result (after a miss) ---- */

static void
save_result(RowCollector *collector)
{
	Size		size = collector->size_bytes + current.key_len + strlen(current.query_text);
	CachedResult *entry;
	bool		found;
	MemoryContext old_memory;

	entry = (CachedResult *) hash_search(cache_table, &current.key_hash, HASH_ENTER, &found);
	if (found)
	{
		
		cache_size_bytes -= entry->size_bytes;
		MemoryContextDelete(entry->memory);
	}

	MemoryContextSetParent(collector->memory, cache_memory);
	entry->memory = collector->memory;
	collector->memory = NULL;

	old_memory = MemoryContextSwitchTo(entry->memory);

	entry->key = palloc(current.key_len);
	memcpy(entry->key, current.key, current.key_len);
	entry->key_len = current.key_len;
	entry->query_text = pstrdup(current.query_text);

	MemoryContextSwitchTo(old_memory);

	entry->rows = collector->rows;
	entry->num_rows = collector->num_rows;
	entry->size_bytes = size;
	entry->hits = 0;

	cache_size_bytes += size;
	count_stores++;

	DEBUG_MSG("qcache: saved %llu row(s), %zu bytes",
			  (unsigned long long) entry->num_rows, size);
}

/* ---- 3c. Sending saved rows (on a hit) ---- */

static void
send_saved_rows(QueryDesc *query_desc, CachedResult *entry)
{
	EState	   *estate = query_desc->estate;
	DestReceiver *receiver = query_desc->dest;
	MemoryContext old_memory = MemoryContextSwitchTo(estate->es_query_cxt);
	TupleTableSlot *slot;

	estate->es_processed = 0;
	receiver->rStartup(receiver, CMD_SELECT, query_desc->tupDesc);

	slot = MakeSingleTupleTableSlot(query_desc->tupDesc, &TTSOpsMinimalTuple);
	for (uint64 i = 0; i < entry->num_rows; i++)
	{
		ExecStoreMinimalTuple(entry->rows[i], slot, false);
		if (!receiver->receiveSlot(slot, receiver))
			break;
		estate->es_processed++;		
	}
	ExecDropSingleTupleTableSlot(slot);

	estate->es_total_processed += estate->es_processed;
	receiver->rShutdown(receiver);
	MemoryContextSwitchTo(old_memory);
}

/* ---- 3d. The hook itself ---- */

static void
qcache_ExecutorRun_hook(QueryDesc *query_desc, ScanDirection direction, uint64 count)
{
	bool		use_cache = false;
	RowCollector *collector = NULL;

	if (current.query_desc == query_desc && !current.already_ran &&
		count == 0 && ScanDirectionIsForward(direction))
		use_cache = true;

	if (current.query_desc == query_desc)
		current.already_ran = true;

	if (use_cache)
	{
		CachedResult *entry = find_cached_result(current.key_hash, current.key,
												 current.key_len);

		if (entry != NULL)
		{
			/* HIT: send the saved rows and skip running the plan. */
			DEBUG_MSG("qcache: HIT, sending %llu saved row(s)",
					  (unsigned long long) entry->num_rows);
			send_saved_rows(query_desc, entry);
			entry->hits++;
			count_hits++;
			forget_current_query();
			return;
		}

		/* MISS: put the row collector in front of the real receiver. */
		DEBUG_MSG("qcache: MISS, running the query and saving its rows");
		count_misses++;
		collector = create_row_collector(query_desc->dest);
		query_desc->dest = &collector->base;
	}

	nesting_level++;
	PG_TRY();
	{
		if (prev_ExecutorRun_hook != NULL)
			prev_ExecutorRun_hook(query_desc, direction, count);
		else
			standard_ExecutorRun(query_desc, direction, count);
	}
	PG_FINALLY();
	{
		nesting_level--;
		if (collector != NULL)
			query_desc->dest = collector->real_receiver;
	}
	PG_END_TRY();

	/* After a miss, the collector holds every row: save them. */
	if (collector != NULL)
	{
		save_result(collector);
		forget_current_query();
	}
}

static void
qcache_ExecutorEnd_hook(QueryDesc *query_desc)
{
	
	if (current.query_desc == query_desc)
		forget_current_query();

	if (prev_ExecutorEnd_hook != NULL)
		prev_ExecutorEnd_hook(query_desc);
	else
		standard_ExecutorEnd(query_desc);
}

static void
qcache_transaction_callback(XactEvent event, void *arg)
{
	int			removed;

	switch (event)
	{
		case XACT_EVENT_COMMIT:
		case XACT_EVENT_PREPARE:
			/* The changes are now permanent: drop every saved result. */
			if (transaction_wrote)
			{
				removed = remove_all_cached_results();
				count_invalidations += removed;
				if (removed > 0)
					DEBUG_MSG("qcache: COMMIT cleared the whole cache (%d results)", removed);
			}
			forget_transaction_changes();
			forget_current_query();
			plan_just_made = NULL;
			break;

		case XACT_EVENT_ABORT:
			/* ROLLBACK or an error: the data did not change, keep the cache. */
			forget_transaction_changes();
			forget_current_query();
			plan_just_made = NULL;
			break;

		default:
			break;
	}
}

Datum
qcache_stats(PG_FUNCTION_ARGS)
{
	TupleDesc	result_columns;
	Datum		values[7];
	bool		nulls[7] = {false, false, false, false, false, false, false};

	if (get_call_result_type(fcinfo, NULL, &result_columns) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	values[0] = Int64GetDatum(hash_get_num_entries(cache_table));
	values[1] = Int64GetDatum((int64) cache_size_bytes);
	values[2] = Int64GetDatum((int64) count_hits);
	values[3] = Int64GetDatum((int64) count_misses);
	values[4] = Int64GetDatum((int64) count_stores);
	values[5] = Int64GetDatum((int64) count_invalidations);
	values[6] = Int64GetDatum((int64) count_evictions);

	PG_RETURN_DATUM(HeapTupleGetDatum(heap_form_tuple(result_columns, values, nulls)));
}

Datum
qcache_entries(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *result = (ReturnSetInfo *) fcinfo->resultinfo;
	HASH_SEQ_STATUS scan;
	CachedResult *entry;

	/* Prepare to return many rows at once. */
	InitMaterializedSRF(fcinfo, 0);

	/* One row per saved result. */
	hash_seq_init(&scan, cache_table);
	while ((entry = (CachedResult *) hash_seq_search(&scan)) != NULL)
	{
		Datum		values[4];
		bool		nulls[4] = {false, false, false, false};

		values[0] = CStringGetTextDatum(entry->query_text);
		values[1] = Int64GetDatum((int64) entry->num_rows);
		values[2] = Int64GetDatum((int64) entry->size_bytes);
		values[3] = Int64GetDatum((int64) entry->hits);
		tuplestore_putvalues(result->setResult, result->setDesc, values, nulls);
	}
	return (Datum) 0;
}

Datum
qcache_reset(PG_FUNCTION_ARGS)
{
	remove_all_cached_results();
	count_hits = 0;
	count_misses = 0;
	count_stores = 0;
	count_invalidations = 0;
	count_evictions = 0;
	PG_RETURN_VOID();
}

void
_PG_init(void)
{
	HASHCTL		table_options;

	DefineCustomBoolVariable("qcache.enabled",
							 "Use and fill the query result cache.",
							 NULL, &setting_enabled, true,
							 PGC_USERSET, 0, NULL, NULL, NULL);
	DefineCustomBoolVariable("qcache.debug",
							 "Print a message at every step qcache takes.",
							 NULL, &setting_debug, false,
							 PGC_USERSET, 0, NULL, NULL, NULL);
	MarkGUCPrefixReserved("qcache");

	cache_memory = AllocSetContextCreate(TopMemoryContext, "qcache",
										 ALLOCSET_DEFAULT_SIZES);

	memset(&table_options, 0, sizeof(table_options));
	table_options.keysize = sizeof(uint64);
	table_options.entrysize = sizeof(CachedResult);
	table_options.hcxt = cache_memory;
	cache_table = hash_create("qcache results", 1024, &table_options,
							  HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);

	prev_planner_hook = planner_hook;
	planner_hook = qcache_planner_hook;

	prev_ExecutorStart_hook = ExecutorStart_hook;
	ExecutorStart_hook = qcache_ExecutorStart_hook;

	prev_ExecutorRun_hook = ExecutorRun_hook;
	ExecutorRun_hook = qcache_ExecutorRun_hook;

	prev_ExecutorEnd_hook = ExecutorEnd_hook;
	ExecutorEnd_hook = qcache_ExecutorEnd_hook;

	RegisterXactCallback(qcache_transaction_callback, NULL);
}

