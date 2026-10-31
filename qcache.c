#include "postgres.h"

#include <limits.h>

#include "access/detoast.h"
#include "access/htup_details.h"
#include "access/transam.h"
#include "access/xact.h"
#include "catalog/pg_class.h"
#include "catalog/pg_inherits.h"
#include "common/hashfn.h"
#include "executor/executor.h"
#include "fmgr.h"
#include "funcapi.h"
#include "lib/ilist.h"
#include "miscadmin.h"
#include "optimizer/optimizer.h"
#include "optimizer/planner.h"
#include "tcop/utility.h"
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

/* Cache settings. */
static bool setting_enabled = true;		
static bool setting_debug = false;		
static int	setting_max_size_kb = 64 * 1024;	
static int	setting_max_result_kb = 1024;	

#define DEBUG_MSG(...) \
	do { \
		if (setting_debug) \
			ereport(NOTICE, (errmsg(__VA_ARGS__))); \
	} while (0)

/* A cached query result. */
typedef struct CachedResult
{
	uint64		key_hash;		

	char	   *key;			
	int			key_len;
	char	   *query_text;		

	Oid		   *table_oids;		
	int			num_tables;

	int			num_columns;	
	Oid		   *column_types;	

	MinimalTuple *rows;			
	uint64		num_rows;
	Size		size_bytes;

	uint64		hits;			

	MemoryContext memory;		
	dlist_node	lru_link;		
} CachedResult;

/* The query currently being prepared for caching. */
typedef struct CurrentQuery
{
	QueryDesc  *query_desc;		
	MemoryContext memory;		

	uint64		key_hash;
	char	   *key;
	int			key_len;
	char	   *query_text;
	Oid		   *table_oids;
	int			num_tables;

	bool		already_ran;	
} CurrentQuery;

/* Collect rows while sending them to the client. */
typedef struct RowCollector
{
	DestReceiver base;			
	DestReceiver *real_receiver;	

	MemoryContext memory;		
	MinimalTuple *rows;
	uint64		num_rows;
	uint64		capacity;		
	Size		size_bytes;
	bool		too_big;		
} RowCollector;

/* Cache data and counters for this connection. */
static MemoryContext cache_memory = NULL;	
static HTAB *cache_table = NULL;	
static dlist_head lru_list = DLIST_STATIC_INIT(lru_list);	
static Size cache_size_bytes = 0;	

static CurrentQuery current;	

static int	nesting_level = 0;

static PlannedStmt *plan_just_made = NULL;
static const char *plan_just_made_problem = NULL;	

static List *tables_written = NIL;	
static bool structure_changed = false;	

static uint64 count_hits = 0;
static uint64 count_misses = 0;
static uint64 count_stores = 0;
static uint64 count_invalidations = 0;
static uint64 count_evictions = 0;

static planner_hook_type prev_planner_hook = NULL;
static ExecutorStart_hook_type prev_ExecutorStart_hook = NULL;
static ExecutorRun_hook_type prev_ExecutorRun_hook = NULL;
static ExecutorFinish_hook_type prev_ExecutorFinish_hook = NULL;
static ExecutorEnd_hook_type prev_ExecutorEnd_hook = NULL;
static ProcessUtility_hook_type prev_ProcessUtility_hook = NULL;

void		_PG_init(void);
PG_FUNCTION_INFO_V1(qcache_stats);
PG_FUNCTION_INFO_V1(qcache_entries);
PG_FUNCTION_INFO_V1(qcache_reset);

/* Look up a result using its hash and full key. */
static CachedResult *
find_cached_result(uint64 key_hash, const char *key, int key_len)
{
	CachedResult *entry;

	entry = (CachedResult *) hash_search(cache_table, &key_hash, HASH_FIND, NULL);
	if (entry == NULL)
		return NULL;

	if (entry->key_len != key_len || memcmp(entry->key, key, key_len) != 0)
		return NULL;

	return entry;
}

/* Remove one result from the cache. */
static void
remove_cached_result(CachedResult *entry)
{
	MemoryContext entry_memory = entry->memory;
	uint64		key_hash = entry->key_hash;

	dlist_delete(&entry->lru_link);
	cache_size_bytes -= entry->size_bytes;
	hash_search(cache_table, &key_hash, HASH_REMOVE, NULL);

	MemoryContextDelete(entry_memory);
}

static bool
result_reads_any_table(CachedResult *entry, List *table_oids)
{
	for (int i = 0; i < entry->num_tables; i++)
	{
		if (list_member_oid(table_oids, entry->table_oids[i]))
			return true;
	}
	return false;
}

/* Remove saved results. */
static int
remove_cached_results(List *table_oids)
{
	HASH_SEQ_STATUS scan;
	CachedResult *entry;
	int			removed = 0;

	hash_seq_init(&scan, cache_table);
	while ((entry = (CachedResult *) hash_seq_search(&scan)) != NULL)
	{
		if (table_oids == NIL || result_reads_any_table(entry, table_oids))
		{
			
			remove_cached_result(entry);
			removed++;
		}
	}
	return removed;
}

/* Clear the current query data. */
static void
forget_current_query(void)
{
	if (current.memory != NULL)
		MemoryContextDelete(current.memory);
	memset(&current, 0, sizeof(current));
}

/* Reset the transaction changes. */
static void
forget_transaction_changes(void)
{
	list_free(tables_written);
	tables_written = NIL;
	structure_changed = false;
}

/* Check if the query can be cached. */
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

/* Let PostgreSQL plan the query and remember the cache check. */
static PlannedStmt *
qcache_planner_hook(Query *query, const char *query_string, int cursor_options,
					ParamListInfo bound_params)
{
	const char *problem;
	PlannedStmt *plan;

	problem = check_query_tree(query);

	if (prev_planner_hook != NULL)
		plan = prev_planner_hook(query, query_string, cursor_options, bound_params);
	else
		plan = standard_planner(query, query_string, cursor_options, bound_params);

	plan_just_made = plan;
	plan_just_made_problem = problem;

	return plan;
}

static void
remember_written_tables(PlannedStmt *plan)
{
	ListCell   *cell;
	MemoryContext old_memory;

	old_memory = MemoryContextSwitchTo(cache_memory);

	foreach(cell, plan->rtable)
	{
		RangeTblEntry *table = (RangeTblEntry *) lfirst(cell);

		if (table->rtekind != RTE_RELATION)
			continue;			

		if (!list_member_oid(tables_written, table->relid))
		{
			tables_written = lappend_oid(tables_written, table->relid);
			DEBUG_MSG("qcache: this transaction changes table \"%s\"; results that "
					  "read it will be removed at COMMIT", get_rel_name(table->relid));
		}
	}

	MemoryContextSwitchTo(old_memory);
}

/* Check the plan before caching it. */
static const char *
check_plan(QueryDesc *query_desc, int eflags, List **table_oids)
{
	PlannedStmt *plan = query_desc->plannedstmt;
	ListCell   *cell;

	*table_oids = NIL;

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
	if (IsolationUsesXactSnapshot())
		return "transaction is REPEATABLE READ or SERIALIZABLE";
	if (structure_changed)
		return "this transaction ran ALTER, TRUNCATE or a similar command";

	foreach(cell, plan->rtable)
	{
		RangeTblEntry *table = (RangeTblEntry *) lfirst(cell);
		Oid			oid = table->relid;
		char		kind;

		if (table->rtekind != RTE_RELATION)
			continue;

		if (table->tablesample != NULL)
			return "uses TABLESAMPLE, which returns random rows";

		if (oid < FirstNormalObjectId)
			return "reads a system catalog";

		kind = get_rel_relkind(oid);
		if (kind != RELKIND_RELATION && kind != RELKIND_VIEW && kind != RELKIND_MATVIEW)
			return "reads a partitioned or foreign table";

		if (get_rel_persistence(oid) == RELPERSISTENCE_TEMP)
			return "reads a temporary table";

		if (get_rel_relispartition(oid) || has_subclass(oid))
			return "reads a partition or an inheritance parent";

		if (list_member_oid(tables_written, oid))
			return "this transaction already changed a table it reads";

		*table_oids = list_append_unique_oid(*table_oids, oid);
	}

	return NULL;
}

/* Build a key using the user, tables, and query text. */
static void
build_cache_key(QueryDesc *query_desc, List *table_oids)
{
	PlannedStmt *plan = query_desc->plannedstmt;
	const char *text = query_desc->sourceText;
	int			text_len;
	Oid			user = GetUserId();
	int			start = plan->stmt_location;
	int			length = plan->stmt_len;
	StringInfoData key;
	ListCell   *cell;
	int			i;

	if (text == NULL)
		text = "";
	text_len = strlen(text);

	initStringInfo(&key);
	appendBinaryStringInfo(&key, &user, sizeof(user));
	appendBinaryStringInfo(&key, &start, sizeof(start));
	appendBinaryStringInfo(&key, &length, sizeof(length));
	foreach(cell, table_oids)
	{
		Oid			oid = lfirst_oid(cell);

		appendBinaryStringInfo(&key, &oid, sizeof(oid));
	}
	appendBinaryStringInfo(&key, text, text_len);

	current.key = key.data;
	current.key_len = key.len;
	current.key_hash = hash_bytes_extended((const unsigned char *) key.data, key.len, 0);

	if (start < 0 || start > text_len)
		start = 0;
	if (length <= 0 || start + length > text_len)
		length = text_len - start;
	current.query_text = pnstrdup(text + start, length);

	current.num_tables = list_length(table_oids);
	current.table_oids = palloc(sizeof(Oid) * Max(current.num_tables, 1));
	i = 0;
	foreach(cell, table_oids)
		current.table_oids[i++] = lfirst_oid(cell);
}

/* Check the query and prepare it for caching. */
static void
qcache_ExecutorStart_hook(QueryDesc *query_desc, int eflags)
{
	const char *problem;
	List	   *table_oids;
	MemoryContext old_memory;

	if (prev_ExecutorStart_hook != NULL)
		prev_ExecutorStart_hook(query_desc, eflags);
	else
		standard_ExecutorStart(query_desc, eflags);

	if (query_desc->operation != CMD_SELECT || query_desc->plannedstmt->hasModifyingCTE)
	{
		remember_written_tables(query_desc->plannedstmt);
		return;
	}

	if (!setting_enabled)
		return;

	problem = check_plan(query_desc, eflags, &table_oids);
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
	build_cache_key(query_desc, table_oids);
	MemoryContextSwitchTo(old_memory);

	list_free(table_oids);
}

/* Copy a result row. */
static MinimalTuple
copy_row(TupleTableSlot *slot)
{
	TupleDesc	columns = slot->tts_tupleDescriptor;
	int			num_columns = columns->natts;
	Datum	   *values = palloc(sizeof(Datum) * Max(num_columns, 1));
	bool	   *is_null = palloc(sizeof(bool) * Max(num_columns, 1));
	bool	   *was_copied = palloc0(sizeof(bool) * Max(num_columns, 1));
	MinimalTuple row;

	slot_getallattrs(slot);

	for (int i = 0; i < num_columns; i++)
	{
		bool		variable_length = (TupleDescAttr(columns, i)->attlen == -1);

		values[i] = slot->tts_values[i];
		is_null[i] = slot->tts_isnull[i];

		if (!is_null[i] && variable_length &&
			VARATT_IS_EXTERNAL(DatumGetPointer(values[i])))
		{
			struct varlena *stored = (struct varlena *) DatumGetPointer(values[i]);

			values[i] = PointerGetDatum(detoast_external_attr(stored));
			was_copied[i] = true;
		}
	}

	row = heap_form_minimal_tuple(columns, values, is_null, 0);

	for (int i = 0; i < num_columns; i++)
	{
		if (was_copied[i])
			pfree(DatumGetPointer(values[i]));
	}
	pfree(values);
	pfree(is_null);
	pfree(was_copied);

	return row;
}

/* Collect rows on a cache miss. */
static bool
collector_receive_row(TupleTableSlot *slot, DestReceiver *self)
{
	RowCollector *collector = (RowCollector *) self;

	if (!collector->too_big)
	{
		MemoryContext old_memory = MemoryContextSwitchTo(collector->memory);
		MinimalTuple row = copy_row(slot);

		collector->size_bytes += row->t_len;

		if (collector->size_bytes > (Size) setting_max_result_kb * 1024)
		{
			
			MemoryContextSwitchTo(old_memory);
			collector->too_big = true;
			MemoryContextReset(collector->memory);
			collector->rows = NULL;
			collector->num_rows = 0;
		}
		else
		{
			
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
		}
	}

	return collector->real_receiver->receiveSlot(slot, collector->real_receiver);
}

/* Pass startup to the real receiver. */
static void
collector_startup(DestReceiver *self, int operation, TupleDesc columns)
{
	RowCollector *collector = (RowCollector *) self;

	collector->real_receiver->rStartup(collector->real_receiver, operation, columns);
}

/* Pass shutdown to the real receiver. */
static void
collector_shutdown(DestReceiver *self)
{
	RowCollector *collector = (RowCollector *) self;

	collector->real_receiver->rShutdown(collector->real_receiver);
}

/* Nothing to clean up here. */
static void
collector_destroy(DestReceiver *self)
{
	
}

/* Create a receiver that saves rows and sends them to the client. */
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

/* Save the collected rows in the cache. */
static void
save_result(RowCollector *collector, TupleDesc columns)
{
	Size		size = collector->size_bytes + current.key_len + strlen(current.query_text);
	Size		max_size = (Size) setting_max_size_kb * 1024;
	CachedResult *entry;
	bool		found;
	MemoryContext old_memory;

	if (collector->too_big || size > max_size)
	{
		DEBUG_MSG("qcache: result is larger than qcache.max_result_size; not saved");
		return;
	}

	while (cache_size_bytes + size > max_size && !dlist_is_empty(&lru_list))
	{
		CachedResult *oldest = dlist_tail_element(CachedResult, lru_link, &lru_list);

		remove_cached_result(oldest);
		count_evictions++;
	}

	entry = (CachedResult *) hash_search(cache_table, &current.key_hash, HASH_ENTER, &found);
	if (found)
	{
		
		dlist_delete(&entry->lru_link);
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

	entry->num_tables = current.num_tables;
	entry->table_oids = palloc(sizeof(Oid) * Max(entry->num_tables, 1));
	memcpy(entry->table_oids, current.table_oids, sizeof(Oid) * entry->num_tables);

	entry->num_columns = columns->natts;
	entry->column_types = palloc(sizeof(Oid) * Max(entry->num_columns, 1));
	for (int i = 0; i < entry->num_columns; i++)
		entry->column_types[i] = TupleDescAttr(columns, i)->atttypid;

	MemoryContextSwitchTo(old_memory);

	entry->rows = collector->rows;
	entry->num_rows = collector->num_rows;
	entry->size_bytes = size;
	entry->hits = 0;

	dlist_push_head(&lru_list, &entry->lru_link);
	cache_size_bytes += size;
	count_stores++;

	DEBUG_MSG("qcache: saved %llu row(s), %zu bytes",
			  (unsigned long long) entry->num_rows, size);
}

/* Check that the cached result has the expected columns. */
static bool
same_columns(CachedResult *entry, TupleDesc columns)
{
	if (entry->num_columns != columns->natts)
		return false;
	for (int i = 0; i < entry->num_columns; i++)
	{
		if (entry->column_types[i] != TupleDescAttr(columns, i)->atttypid)
			return false;
	}
	return true;
}

/* Send cached rows to the client. */
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

/* Check for a cache hit; otherwise run and save the query. */
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

		if (entry != NULL && same_columns(entry, query_desc->tupDesc))
		{
			
			DEBUG_MSG("qcache: HIT, sending %llu saved row(s)",
					  (unsigned long long) entry->num_rows);
			send_saved_rows(query_desc, entry);
			entry->hits++;
			count_hits++;
			dlist_move_head(&lru_list, &entry->lru_link);	
			forget_current_query();
			return;
		}

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

	if (collector != NULL)
	{
		save_result(collector, query_desc->tupDesc);
		forget_current_query();
	}
}

/* Handle query finish bookkeeping. */
static void
qcache_ExecutorFinish_hook(QueryDesc *query_desc)
{
	
	nesting_level++;
	PG_TRY();
	{
		if (prev_ExecutorFinish_hook != NULL)
			prev_ExecutorFinish_hook(query_desc);
		else
			standard_ExecutorFinish(query_desc);
	}
	PG_FINALLY();
	{
		nesting_level--;
	}
	PG_END_TRY();
}

/* Clear query state when execution ends. */
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

/* Check whether a utility command can change data. */
static bool
is_harmless_command(Node *command)
{
	switch (nodeTag(command))
	{
		case T_TransactionStmt:		
		case T_VariableSetStmt:		
		case T_VariableShowStmt:	
		case T_ExplainStmt:
		case T_PrepareStmt:
		case T_ExecuteStmt:
		case T_DeallocateStmt:
		case T_DeclareCursorStmt:
		case T_FetchStmt:
		case T_ClosePortalStmt:
		case T_CallStmt:		
		case T_DoStmt:			
		case T_ListenStmt:
		case T_UnlistenStmt:
		case T_NotifyStmt:
		case T_CheckPointStmt:
		case T_VacuumStmt:
			return true;
		default:
			return false;
	}
}

/* Track utility commands that may change tables. */
static void
qcache_ProcessUtility_hook(PlannedStmt *plan, const char *query_string,
						   bool read_only_tree, ProcessUtilityContext context,
						   ParamListInfo params, QueryEnvironment *query_env,
						   DestReceiver *dest, QueryCompletion *completion)
{
	
	if (!is_harmless_command(plan->utilityStmt))
	{
		if (!structure_changed)
			DEBUG_MSG("qcache: this command may change tables; the whole cache "
					  "will be cleared at COMMIT");
		structure_changed = true;
	}

	nesting_level++;
	PG_TRY();
	{
		if (prev_ProcessUtility_hook != NULL)
			prev_ProcessUtility_hook(plan, query_string, read_only_tree, context,
									 params, query_env, dest, completion);
		else
			standard_ProcessUtility(plan, query_string, read_only_tree, context,
									params, query_env, dest, completion);
	}
	PG_FINALLY();
	{
		nesting_level--;
	}
	PG_END_TRY();
}

/* Clear cached results after a transaction changes data. */
static void
qcache_transaction_callback(XactEvent event, void *arg)
{
	int			removed;

	switch (event)
	{
		case XACT_EVENT_COMMIT:
		case XACT_EVENT_PREPARE:
			
			if (structure_changed)
			{
				removed = remove_cached_results(NIL);
				count_invalidations += removed;
				if (removed > 0)
					DEBUG_MSG("qcache: COMMIT cleared the whole cache (%d results)", removed);
			}
			else if (tables_written != NIL)
			{
				removed = remove_cached_results(tables_written);
				count_invalidations += removed;
				if (removed > 0)
					DEBUG_MSG("qcache: COMMIT removed %d cached results", removed);
			}
			forget_transaction_changes();
			forget_current_query();
			plan_just_made = NULL;
			break;

		case XACT_EVENT_ABORT:
			
			forget_transaction_changes();
			forget_current_query();
			plan_just_made = NULL;
			break;

		default:
			break;
	}
}

/* Return cache statistics. */
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

/* Return the saved query entries. */
Datum
qcache_entries(PG_FUNCTION_ARGS)
{
	ReturnSetInfo *result = (ReturnSetInfo *) fcinfo->resultinfo;
	dlist_iter	iter;

	InitMaterializedSRF(fcinfo, 0);

	dlist_foreach(iter, &lru_list)
	{
		CachedResult *entry = dlist_container(CachedResult, lru_link, iter.cur);
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

/* Clear the cache and reset the counters. */
Datum
qcache_reset(PG_FUNCTION_ARGS)
{
	remove_cached_results(NIL);
	count_hits = 0;
	count_misses = 0;
	count_stores = 0;
	count_invalidations = 0;
	count_evictions = 0;
	PG_RETURN_VOID();
}

/* Set up the cache and PostgreSQL hooks. */
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
	DefineCustomIntVariable("qcache.max_size",
							"Memory for cached results, per connection.",
							NULL, &setting_max_size_kb, 64 * 1024,
							0, INT_MAX / 1024, PGC_USERSET, GUC_UNIT_KB,
							NULL, NULL, NULL);
	DefineCustomIntVariable("qcache.max_result_size",
							"Largest single result that is cached.",
							NULL, &setting_max_result_kb, 1024,
							0, INT_MAX / 1024, PGC_USERSET, GUC_UNIT_KB,
							NULL, NULL, NULL);
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

	prev_ExecutorFinish_hook = ExecutorFinish_hook;
	ExecutorFinish_hook = qcache_ExecutorFinish_hook;

	prev_ExecutorEnd_hook = ExecutorEnd_hook;
	ExecutorEnd_hook = qcache_ExecutorEnd_hook;

	prev_ProcessUtility_hook = ProcessUtility_hook;
	ProcessUtility_hook = qcache_ProcessUtility_hook;

	RegisterXactCallback(qcache_transaction_callback, NULL);
}
