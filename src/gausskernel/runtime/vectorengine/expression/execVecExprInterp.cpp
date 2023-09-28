#include "postgres.h"
#include "catalog/pg_type.h"
#include "funcapi.h"
#include "utils/memutils.h"
#include "miscadmin.h"
#include "nodes/nodeFuncs.h"
#include "pgstat.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/datum.h"
#include "utils/elog.h"
#include "vecexecutor/execVecExpr.h"
#include "vecexecutor/vecnodes.h"
#include "executor/node/nodeAgg.h"

/*
 * Use computed-goto-based opcode dispatch when computed gotos are available.
 * But use a separate symbol so that it's easy to adjust locally in this file
 * for development and testing.
 */
#ifdef HAVE_COMPUTED_GOTO
#define EEO_USE_COMPUTED_GOTO
#endif

/*
 * Macros for opcode dispatch.
 *
 * EEO_SWITCH - just hides the switch if not in use.
 * EEO_CASE - labels the implementation of named expression step type.
 * EEO_DISPATCH - jump to the implementation of the step type for 'op'.
 * EEO_OPCODE - compute opcode required by used expression evaluation method.
 * EEO_NEXT - increment 'op' and jump to correct next step type.
 * EEO_JUMP - jump to the specified step number within the current expression.
 */
#if defined(EEO_USE_COMPUTED_GOTO)
typedef struct VecExprEvalOpLookup {
	const void *opcode;
	VecExprEvalOp	op;
} ExprEvalOpLookup;

/* to make dispatch_table accessible outside ExecInterpVecExpr() */
static const void **dispatch_table = NULL;

/* jump target -> opcode lookup table */
static VecExprEvalOpLookup reverse_dispatch_table[EEOP_VEC_LAST];

#define EEO_SWITCH()
#define EEO_CASE(name)		CASE_##name:
#define EEO_DISPATCH()		goto *((void *) op->opcode)
#define EEO_OPCODE(opcode)	((intptr_t) dispatch_table[opcode])

#else

#define EEO_SWITCH()		starteval: switch ((ExprEvalOp) op->opcode)
#define EEO_CASE(name)		case name:
#define EEO_DISPATCH()		goto starteval
#define EEO_OPCODE(opcode)	(opcode)

#endif

#define EEO_NEXT() \
	do { \
		op++; \
		EEO_DISPATCH(); \
	} while (0)

#define EEO_JUMP(stepno) \
	do { \
		op = &state->steps[stepno]; \
		EEO_DISPATCH(); \
	} while (0)

static CVector* ExecInterpVecExpr(VecExprState *state, VecExprContext *econtext);
static void ExecInitVecInterpreter(void);

void ExecReadyInterpretedVecExpr(VecExprState *state)
{
	ExecInitVecInterpreter();

	Assert(state->steps_len >= 1);
	Assert(state->steps[state->steps_len - 1].opcode == EEOP_VEC_DONE);

#if defined(EEO_USE_COMPUTED_GOTO)
	for (uint32 off = 0; off < state->steps_len; off++) {
		VecExprEvalStep *op = &state->steps[off];
		op->opcode = EEO_OPCODE(op->opcode);
	}
#endif

	state->evalfunc = ExecInterpVecExpr;
}

BatchVector* VecExecEvalExprSwitchContext(VecExprState *state, VecExprContext *econtext)
{
	MemoryContext oldContext;

	oldContext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);
	state->evalfunc(state, econtext);
	MemoryContextSwitchTo(oldContext);

	return state->resultbatch;
}

static void ExecVecAggPlainTransByVal(VecExprContext *econtext, VecAggState *aggstate, VecAggStatePerTrans pertrans,
    AggStatePerGroup pergroup, MemoryContext aggcontext, int setno)
{
    VecFunctionCallInfo fcinfo = &pertrans->vec_transfn_fcinfo;
    MemoryContext oldContext;

    oldContext = MemoryContextSwitchTo(aggstate->vectmpcontext->ecxt_per_tuple_memory);

    fcinfo->rows = econtext->align_rows;
	if (fcinfo->vec[0])
		fcinfo->args[0] = (void *)fcinfo->vec[0]->Data();

    pertrans->transfn.vector_fn_addr(fcinfo);

    MemoryContextSwitchTo(oldContext);
}

static void ExecVecAggCollectPlainTransByVal(VecExprContext *econtext, VecAggState *aggstate, VecAggStatePerTrans pertrans,
    AggStatePerGroup pergroup, MemoryContext aggcontext, int setno)
{
    VecFunctionCallInfo fcinfo = &pertrans->vec_collectfn_fcinfo;
    MemoryContext oldContext;

    oldContext = MemoryContextSwitchTo(aggstate->vectmpcontext->ecxt_per_tuple_memory);

    fcinfo->rows = econtext->align_rows;
	if (fcinfo->vec[0])
		fcinfo->args[0] = (void *)fcinfo->vec[0]->Data();

    pertrans->collectfn.vector_fn_addr(fcinfo);

    MemoryContextSwitchTo(oldContext);
}

void ExecVecAggPlainTransByRef(VecExprContext *econtext, VecAggState *aggstate, VecAggStatePerTrans pertrans,
    AggStatePerGroup pergroup, MemoryContext aggcontext, int setno)
{
    VecFunctionCallInfo fcinfo = &pertrans->vec_transfn_fcinfo;
    MemoryContext oldContext;

    oldContext = MemoryContextSwitchTo(aggstate->vectmpcontext->ecxt_per_tuple_memory);

	fcinfo->rows = econtext->align_rows;
	fcinfo->args[0] = (void *)fcinfo->vec[0]->Data();

    pertrans->transfn.vector_fn_addr(fcinfo);

    MemoryContextSwitchTo(oldContext);
}

void ExecVecAggCollectPlainTransByRef(VecExprContext *econtext, VecAggState *aggstate, VecAggStatePerTrans pertrans,
    AggStatePerGroup pergroup, MemoryContext aggcontext, int setno)
{
    VecFunctionCallInfo fcinfo = &pertrans->vec_collectfn_fcinfo;
    MemoryContext oldContext;

    oldContext = MemoryContextSwitchTo(aggstate->vectmpcontext->ecxt_per_tuple_memory);

	fcinfo->rows = econtext->align_rows;
	fcinfo->args[0] = (void *)fcinfo->vec[0]->Data();

    pertrans->collectfn.vector_fn_addr(fcinfo);

    MemoryContextSwitchTo(oldContext);
}

static CVector* ExecInterpVecExpr(VecExprState *state, VecExprContext *econtext)
{
	VecExprEvalStep *op;
	BatchVector *resultbatch;
	BatchVector *innerbatch;
	BatchVector *outerbatch;
	BatchVector *scanbatch;

#if defined(EEO_USE_COMPUTED_GOTO)
	static const void *const dispatch_table[] = {
		&&CASE_EEOP_VEC_DONE,

		&&CASE_EEOP_VEC_INNER_VECTOR,
		&&CASE_EEOP_VEC_OUTER_VECTOR,
		&&CASE_EEOP_VEC_SCAN_VECTOR,

		&&CASE_EEOP_VEC_PROJ_INNER_VECTOR,
		&&CASE_EEOP_VEC_PROJ_OUTER_VECTOR,
		&&CASE_EEOP_VEC_PROJ_SCAN_VECTOR,
        &&CASE_EEOP_VEC_PROJ_VECTOR_TMP,

		&&CASE_EEOP_VEC_CONST,

		&&CASE_EEOP_VEC_FUNCEXPR,

		&&CASE_EEOP_VEC_QUAL,
		
		&&CASE_EEOP_VEC_AGGREF,
        &&CASE_EEOP_VEC_AGG_PLAIN_TRANS_BYVAL,
		&&CASE_EEOP_VEC_AGG_COLLECT_PLAIN_TRANS_BYVAL,
		&&CASE_EEOP_VEC_AGG_PLAIN_TRANS_BYREF,
		&&CASE_EEOP_VEC_AGG_COLLECT_PLAIN_TRANS_BYREF,
		&&CASE_EEOP_VEC_LAST
	};

	StaticAssertStmt(EEOP_VEC_LAST + 1 == lengthof(dispatch_table),
					 "dispatch_table out of whack with ExprEvalOp");

	if (unlikely(state == NULL))
		return (CVector*)(dispatch_table);
#else
	Assert(state != NULL);
#endif

	op = state->steps;
	resultbatch = state->resultbatch;
    innerbatch = econtext->ecxt_innerbatch;
	outerbatch = econtext->ecxt_outerbatch;
	scanbatch = econtext->ecxt_scanbatch;
	state->filter_rows = econtext->align_rows;
	
#if defined(EEO_USE_COMPUTED_GOTO)
	EEO_DISPATCH();
#endif

	EEO_SWITCH()
	{
		EEO_CASE(EEOP_VEC_DONE)
		{
			goto out;
		}

		EEO_CASE(EEOP_VEC_INNER_VECTOR)
		{
			(*op->resvector)->ShallowCopy(innerbatch->GetColumn(op->d.var_vector.attnum));
			EEO_NEXT();
		}

		EEO_CASE(EEOP_VEC_OUTER_VECTOR)
		{
			(*op->resvector)->ShallowCopy(outerbatch->GetColumn(op->d.var_vector.attnum));
			EEO_NEXT();
		}
		
		EEO_CASE(EEOP_VEC_SCAN_VECTOR)
		{
			(*op->resvector)->ShallowCopy(scanbatch->GetColumn(op->d.var_vector.attnum));
			EEO_NEXT();
		}

		EEO_CASE(EEOP_VEC_PROJ_INNER_VECTOR)
		{
			resultbatch->GetColumn(op->d.proj_vector.resultnum)->ShallowCopy(innerbatch->GetColumn(op->d.proj_vector.attnum));
            EEO_NEXT();
        }

		EEO_CASE(EEOP_VEC_PROJ_OUTER_VECTOR)
		{
			resultbatch->GetColumn(op->d.proj_vector.resultnum)->ShallowCopy(outerbatch->GetColumn(op->d.proj_vector.attnum));
            EEO_NEXT();
        }

		EEO_CASE(EEOP_VEC_PROJ_SCAN_VECTOR)
		{
			resultbatch->GetColumn(op->d.proj_vector.resultnum)->ShallowCopy(scanbatch->GetColumn(op->d.proj_vector.attnum));
            EEO_NEXT();
        }

		EEO_CASE(EEOP_VEC_PROJ_VECTOR_TMP)
		{
			resultbatch->GetColumn(op->d.proj_vector_tmp.resultnum)->ShallowCopy(*op->resvector);
            EEO_NEXT();
        }

		EEO_CASE(EEOP_VEC_CONST)
		{
			EEO_NEXT();
		}

		EEO_CASE(EEOP_VEC_FUNCEXPR)
		{
            VecFunctionCallInfo fcinfo = op->d.func.fcinfo;
    		fcinfo->rows = econtext->align_rows;
    		fcinfo->filter = econtext->use_filter ? state->filter : NULL;
			op->d.func.vec_fn_addr(fcinfo);
			fcinfo->vec[op->d.func.nargs - 1]->Resize(fcinfo->rows);
            EEO_NEXT();
        }

		EEO_CASE(EEOP_VEC_QUAL)
		{
			ColumnVectorUint8* opresult;
			ColumnVectorUint8* filter = state->filter;

			switch (op->d.qual.type) {
				case VEC_QUAL_DIRECT_END: {
					state->resultvector->ShallowCopy(*op->resvector);
					break;
				}
				case VEC_QUAL_CHECK: {
					opresult = (ColumnVectorUint8*)(*op->resvector);

					state->filter_rows = opresult->GetFilterRows();
					if (state->filter_rows) {
						econtext->use_filter = ((state->filter_rows * 100) / econtext->align_rows) < 10; //10%
						filter->ShallowCopy(opresult);
					}
					else {
						state->resultvector->Reset();
						EEO_JUMP(op->d.qual.jumpdone);
					}

					break;
				}
				case VEC_QUAL_CHECK_MERGE: {
					opresult = (ColumnVectorUint8*)(*op->resvector);
					
					if (econtext->use_filter) {
						state->filter_rows = opresult->GetFilterRows();
						if (!state->filter_rows) {
							econtext->use_filter = ((state->filter_rows * 100) / econtext->align_rows) < 10; //10%
							state->resultvector->Reset();
							EEO_JUMP(op->d.qual.jumpdone);
						}
					}
					else {
						uint32 filter_rows = opresult->GetFilterRows();
						if (filter_rows) {
							ColumnVectorUint8And(opresult, filter, filter);
							state->filter_rows = filter->GetFilterRows();
							if (!state->filter_rows) {
								econtext->use_filter = ((state->filter_rows * 100) / econtext->align_rows) < 10; //10%
								state->resultvector->Reset();
								EEO_JUMP(op->d.qual.jumpdone);
							}
						}
						else {
							state->resultvector->Reset();
							EEO_JUMP(op->d.qual.jumpdone);
						}
					}

					break;
				}
				case VEC_QUAL_CHECK_MERGE_END: {
					opresult = (ColumnVectorUint8*)(*op->resvector);

					if (econtext->use_filter) {
						state->filter_rows = opresult->GetFilterRows();
						if (state->filter_rows) {
							state->resultvector->ShallowCopy(opresult);
						}
						else {
							state->resultvector->Reset();
							EEO_JUMP(op->d.qual.jumpdone);
						}
					}
					else {
						uint32 filter_rows = opresult->GetFilterRows();
						if (filter_rows) {
							ColumnVectorUint8And(opresult, filter, filter);
							state->filter_rows = filter->GetFilterRows();
							if (state->filter_rows) {
								state->resultvector->ShallowCopy(filter);
							}
							else {
								state->resultvector->Reset();
								EEO_JUMP(op->d.qual.jumpdone);
							}
						}
						else {
							state->resultvector->Reset();
							EEO_JUMP(op->d.qual.jumpdone);
						}
					}

					break;
				}
				default:
					elog(ERROR, "unrecognized type:%d", op->d.qual.type);				
			}

			EEO_NEXT();
		}

		EEO_CASE(EEOP_VEC_AGGREF)
		{
			AggrefExprState *aggref = op->d.aggref.astate;
			Assert(aggref->m_htbOffset < econtext->ecxt_aggbatch->Cols());
    		*op->resvector = econtext->ecxt_aggbatch->GetColumn(aggref->m_htbOffset);
            EEO_NEXT();
		}
		
        EEO_CASE(EEOP_VEC_AGG_PLAIN_TRANS_BYVAL)
        {
            VecAggState *aggstate = castNode(VecAggState, state->parent);
            VecAggStatePerTrans pertrans = op->d.agg_trans.pertrans;
            AggStatePerGroup pergroup = &aggstate->all_pergroups
                [op->d.agg_trans.setoff * aggstate->numtrans + op->d.agg_trans.transno];

            Assert(pertrans->transtypeByVal);

			if (pertrans->vec_transfn_fcinfo.vec[0])
				pertrans->vec_transfn_fcinfo.vec[0]->PrefetchVectorData();

            ExecVecAggPlainTransByVal(econtext, aggstate, pertrans, pergroup,
                                   op->d.agg_trans.aggcontext,
                                   op->d.agg_trans.setno);

			EEO_NEXT();
        }

		EEO_CASE(EEOP_VEC_AGG_COLLECT_PLAIN_TRANS_BYVAL)
        {
			VecAggState *aggstate = castNode(VecAggState, state->parent);
            VecAggStatePerTrans pertrans = op->d.agg_trans.pertrans;
            AggStatePerGroup pergroup = &aggstate->all_pergroups
                [op->d.agg_trans.setoff * aggstate->numtrans + op->d.agg_trans.transno];

            Assert(pertrans->transtypeByVal);

			ExecVecAggCollectPlainTransByVal(econtext, aggstate, pertrans, pergroup, 
									op->d.agg_trans.aggcontext, 
									op->d.agg_trans.setno);

			EEO_NEXT();
		}

		EEO_CASE(EEOP_VEC_AGG_PLAIN_TRANS_BYREF)
        {
            VecAggState   *aggstate = castNode(VecAggState, state->parent);
            VecAggStatePerTrans pertrans = op->d.agg_trans.pertrans;
            AggStatePerGroup pergroup = &aggstate->all_pergroups
                [op->d.agg_trans.setoff * aggstate->numtrans
                + op->d.agg_trans.transno];

            Assert(!pertrans->transtypeByVal);

            ExecVecAggPlainTransByRef(econtext, aggstate, pertrans, pergroup,
                                   op->d.agg_trans.aggcontext,
                                   op->d.agg_trans.setno);

            EEO_NEXT();
        }

		EEO_CASE(EEOP_VEC_AGG_COLLECT_PLAIN_TRANS_BYREF)
        {
            VecAggState   *aggstate = castNode(VecAggState, state->parent);
            VecAggStatePerTrans pertrans = op->d.agg_trans.pertrans;
            AggStatePerGroup pergroup = &aggstate->all_pergroups
                [op->d.agg_trans.setoff * aggstate->numtrans
                + op->d.agg_trans.transno];

            Assert(!pertrans->transtypeByVal);

            ExecVecAggCollectPlainTransByRef(econtext, aggstate, pertrans, pergroup,
                                   op->d.agg_trans.aggcontext,
                                   op->d.agg_trans.setno);

            EEO_NEXT();
        }

		EEO_CASE(EEOP_VEC_LAST)
		{
			/* unreachable */
			Assert(false);
			goto out;
		}
	}

out:
	return state->resultvector;
}

/*
 * Do one-time initialization of interpretation machinery.
 */
static void ExecInitVecInterpreter(void)
{
#if defined(EEO_USE_COMPUTED_GOTO)
	/* Set up externally-visible pointer to dispatch table */
	if (dispatch_table == NULL) {
		dispatch_table = (const void **)(ExecInterpVecExpr(NULL, NULL));

		/* build reverse lookup table */
		for (int i = 0; i < EEOP_VEC_LAST; i++) {
			reverse_dispatch_table[i].opcode = dispatch_table[i];
			reverse_dispatch_table[i].op = (VecExprEvalOp) i;
		}
	}
#endif
}

BatchVector* ExecVectorProject(VecExprState *vecstate, VecExprContext* econtext)
{
    MemoryContext oldContext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);
    vecstate->evalfunc(vecstate, econtext);
	vecstate->resultbatch->SetRows(econtext->align_rows);
    MemoryContextSwitchTo(oldContext);

    return vecstate->resultbatch;
}

ColumnVectorUint8* ExecVectorQual(VecExprState *vecstate, VecExprContext* econtext)
{
    MemoryContext oldContext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);
    ColumnVectorUint8* vector = (ColumnVectorUint8*)vecstate->evalfunc(vecstate, econtext);
    MemoryContextSwitchTo(oldContext);

    return vector;
}

void ExecVectorPack(BatchVector* packbatch, BatchVector* batch, List* packlist, VecQualResult* QualResult, int32* batchpos)
{
	uint32 idx, rows;
	int32 pos;
	CVector *src, *dst;
	ListCell *lc;

	Assert(packbatch->Cols() == batch->Cols());

	rows = batch->Rows();
	foreach (lc, packlist) {
		idx = lfirst_int(lc) - 1;
		Assert(idx < packbatch->Cols());
		
		dst = packbatch->GetColumn(idx);
		src = batch->GetColumn(idx);
		Assert(dst);
		Assert(src);

		pos = *batchpos;
		src->PrefetchVectorData();
		dst->VectorPack(src, QualResult, &pos);
	}
	Assert(pos <= ColumnVectorSize);
	Assert(rows + *batchpos == src->Size());
	Assert(*batchpos <= pos);
	packbatch->SetRows(dst->Size());
	batch->SetRows(rows - (pos - *batchpos));
	*batchpos = pos;

	return;
}