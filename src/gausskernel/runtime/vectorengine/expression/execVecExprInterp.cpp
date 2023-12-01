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
#include "executor/node/nodeSubplan.h"
#include "windowapi.h"

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
extern void ExecVecWinAggPlainTransByVal(VecExprContext *econtext, VecWindowAggState *aggstate,
    VecFunctionCallInfo trans_fcinfo, MemoryContext aggcontext);

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
    MemoryContext aggcontext)
{
    VecFunctionCallInfo fcinfo = &pertrans->vec_transfn_fcinfo;
    MemoryContext oldContext;

    oldContext = MemoryContextSwitchTo(aggstate->vectmpcontext->ecxt_per_tuple_memory);

    fcinfo->rows = econtext->align_rows;
	if (fcinfo->vec[0]) {
		fcinfo->args[0] = (void *)fcinfo->vec[0]->Data();
		fcinfo->offset[0] = (void *)fcinfo->vec[0]->Offset();
		fcinfo->null[0] = (void *)fcinfo->vec[0]->Bitmap();
	}

    pertrans->transfn.vector_fn_addr(fcinfo);

    MemoryContextSwitchTo(oldContext);
}

static void ExecVecAggCollectPlainTransByVal(VecExprContext *econtext, VecAggState *aggstate, VecAggStatePerTrans pertrans, 
	MemoryContext aggcontext)
{
    VecFunctionCallInfo fcinfo = &pertrans->vec_collectfn_fcinfo;
    MemoryContext oldContext;

    oldContext = MemoryContextSwitchTo(aggstate->vectmpcontext->ecxt_per_tuple_memory);

    fcinfo->rows = econtext->align_rows;
	if (fcinfo->vec[0]) {
		fcinfo->args[0] = (void *)fcinfo->vec[0]->Data();
		fcinfo->offset[0] = (void *)fcinfo->vec[0]->Offset();
		fcinfo->null[0] = (void *)fcinfo->vec[0]->Bitmap();
	}

    pertrans->collectfn.vector_fn_addr(fcinfo);

    MemoryContextSwitchTo(oldContext);
}

void ExecVecAggPlainTransByRef(VecExprContext *econtext, VecAggState *aggstate, VecAggStatePerTrans pertrans,
    MemoryContext aggcontext)
{
    VecFunctionCallInfo fcinfo = &pertrans->vec_transfn_fcinfo;
    MemoryContext oldContext;

    oldContext = MemoryContextSwitchTo(aggstate->vectmpcontext->ecxt_per_tuple_memory);

	fcinfo->rows = econtext->align_rows;
	fcinfo->args[0] = (void *)fcinfo->vec[0]->Data();
	fcinfo->offset[0] = (void *)fcinfo->vec[0]->Offset();
	fcinfo->null[0] = (void *)fcinfo->vec[0]->Bitmap();

    pertrans->transfn.vector_fn_addr(fcinfo);

    MemoryContextSwitchTo(oldContext);
}

void ExecVecAggCollectPlainTransByRef(VecExprContext *econtext, VecAggState *aggstate, VecAggStatePerTrans pertrans,
    MemoryContext aggcontext)
{
    VecFunctionCallInfo fcinfo = &pertrans->vec_collectfn_fcinfo;
    MemoryContext oldContext;

    oldContext = MemoryContextSwitchTo(aggstate->vectmpcontext->ecxt_per_tuple_memory);

	fcinfo->rows = econtext->align_rows;
	fcinfo->args[0] = (void *)fcinfo->vec[0]->Data();
	fcinfo->offset[0] = (void *)fcinfo->vec[0]->Offset();
	fcinfo->null[0] = (void *)fcinfo->vec[0]->Bitmap();

    pertrans->collectfn.vector_fn_addr(fcinfo);

    MemoryContextSwitchTo(oldContext);
}


/*
 * Evaluate a PARAM_EXEC parameter.
 *
 * PARAM_EXEC params (internal executor parameters) are stored in the
 * ecxt_param_exec_vals array, and can be accessed by array index.
 */
void VecExecEvalParamExec(VecExprState* exprstate, VecExprEvalStep *op, VecExprContext* econtext)
{
    Param* expression = (Param*)exprstate->expr;
    int thisParamId = expression->paramid;
    ParamExecData* prm = NULL;
    ScalarValue val;
    MemoryContext oldContext = NULL;
    CVector* paramVec = NULL;
    Assert(econtext->align_rows != 0);

    // PARAM_EXEC params (internal executor parameters) are stored in the
    // ecxt_param_exec_vals array, and can be accessed by array index.
    prm = &(econtext->ecxt_param_exec_vals[thisParamId]);
    prm->valueType = expression->paramtype;

	Assert(prm->paramVector == NULL); // TODO:// don't know when its not null

    // if the parameter is changed
    // or parameter is const but the vector is not initialized
    // or init plan has not been evaluated
    if (prm->isChanged || (prm->isConst && prm->paramVector == NULL) || prm->execPlan != NULL ||
        (prm->isConst == false && prm->execPlan == NULL)) {
        paramVec = (CVector*)prm->paramVector;

        if (prm->execPlan != NULL) {
			ExprContext row_context;
			row_context.ecxt_param_exec_vals = econtext->ecxt_param_exec_vals;
			row_context.ecxt_per_query_memory = econtext->ecxt_per_query_memory;
            // Invoke the row interface to get a parameter evaluated
            ExecSetParamPlan((SubPlanState*)prm->execPlan, &row_context);

            // ExecSetParamPlan should have processed this param
            prm->isConst = true;
            prm->valueType = expression->paramtype;
            Assert(prm->execPlan == NULL);
        }

        /*
         * Extend the result to a const vector and return.
         * We are probably in a short-lived expression-evaluation context. Switch
         * to the per-query context for manipulating the const value is valid.
         */
		if (!prm->isnull) {
			Const const_val;
			const_val.constvalue = prm->value;
			oldContext = MemoryContextSwitchTo(econtext->ecxt_estate->es_query_cxt);
			paramVec = CreateColumnVectorConst(CurrentMemoryContext, &const_val);
			prm->paramVector = paramVec;
         	(void)MemoryContextSwitchTo(oldContext);
		}

        prm->isChanged = false;
        prm->isConst = true;
    }
	
	op->resvector = (CVector**)&prm->paramVector;
}

/*
 * Evaluate a PARAM_EXTERN parameter.
 *
 * PARAM_EXTERN parameters must be sought in ecxt_param_list_info.
 */
void VecExecEvalParamExtern(VecExprState* exprstate, VecExprEvalStep *op, VecExprContext* econtext)
{
    Param* expression = (Param*)exprstate->expr;
    int thisParamId = expression->paramid;
    ParamListInfo paramInfo = econtext->ecxt_param_list_info;
    ParamExternData* prm = NULL;
    ScalarValue val;
	CVector* paramVec = NULL;

    Assert(econtext->align_rows != 0);
    /*
     * PARAM_EXTERN parameters must be sought in ecxt_param_list_info.
     */
    if (paramInfo && thisParamId > 0 && thisParamId <= paramInfo->numParams) {
        prm = &paramInfo->params[thisParamId - 1];

        /* give hook a chance in case parameter is dynamic */
        if (!OidIsValid(prm->ptype) && paramInfo->paramFetch != NULL)
            (*paramInfo->paramFetch)(paramInfo, thisParamId);

        if (OidIsValid(prm->ptype)) {
            /* safety check in case hook did something unexpected */

            if (prm->ptype != expression->paramtype)
                ereport(ERROR,
                    (errcode(ERRCODE_DATATYPE_MISMATCH),
                        errmsg("Type of parameter %d (%s) does not match that when preparing the plan (%s)",
                            thisParamId,
                            format_type_be(prm->ptype),
                            format_type_be(expression->paramtype))));
        }

		if (!prm->isnull) {
			Const const_val;
			const_val.constvalue = prm->value;
			MemoryContext oldContext = MemoryContextSwitchTo(econtext->ecxt_estate->es_query_cxt);
			paramVec = CreateColumnVectorConst(CurrentMemoryContext, &const_val);
			op->resvector = &paramVec;
         	(void)MemoryContextSwitchTo(oldContext);
		}
    }

    ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("No value found for parameter %d", thisParamId)));
}

void ExecVecFixlenScalarArrayOp(VecExprEvalStep *op)
{
	ArrayType* arr = op->d.scalararrayop.arr;
	int nitems;
	int16		typlen;
	bool		typbyval;
	char		typalign;
	Oid element_type = ARR_ELEMTYPE(arr);
	char* arr_const;
	Datum element;
	VecFunctionCallInfo fcinfo = op->d.scalararrayop.fcinfo;

	get_typlenbyvalalign(element_type, &typlen, &typbyval, &typalign);
	nitems = ArrayGetNItems(ARR_NDIM(arr), ARR_DIMS(arr));
	arr_const = (char*)ARR_DATA_PTR(arr);
	fcinfo->vec[op->d.scalararrayop.nargs - 1]->Clear();
	
	for(uint32 i = 0; i < nitems; ++i) {
		element = fetch_att(arr_const, typbyval, typlen);
		fcinfo->args[1] = &element;
		op->d.scalararrayop.vec_fn_addr(fcinfo);
		arr_const = att_addlength_pointer(arr_const, typlen, arr_const);
		arr_const = (char*)att_align_nominal(arr_const, typalign);
	}
}

void ExecVecStrScalarArrayOp(VecExprEvalStep *op)
{
	ArrayType* arr = op->d.scalararrayop.arr;
	int nitems;
	int16		typlen;
	bool		typbyval;
	char		typalign;
	Oid element_type = ARR_ELEMTYPE(arr);
	char* arr_const;
	Datum element;
	uint8* data;
	uint32 len;
	VecFunctionCallInfo fcinfo = op->d.scalararrayop.fcinfo;

	get_typlenbyvalalign(element_type, &typlen, &typbyval, &typalign);
	nitems = ArrayGetNItems(ARR_NDIM(arr), ARR_DIMS(arr));
	arr_const = (char*)ARR_DATA_PTR(arr);
	fcinfo->vec[op->d.scalararrayop.nargs - 1]->Clear();

	for(uint32 i = 0; i < nitems; ++i) {
		element = fetch_att(arr_const, typbyval, typlen);
		data = (uint8*)VARDATA_ANY(element);
		len = VARSIZE_ANY_EXHDR(element);
		fcinfo->args[1] = &data;
		fcinfo->offset[1] = &len;
		op->d.scalararrayop.vec_fn_addr(fcinfo);
		arr_const = att_addlength_pointer(arr_const, typlen, arr_const);
		arr_const = (char*)att_align_nominal(arr_const, typalign);
	}
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

		&&CASE_EEOP_VEC_PARAM_EXEC,
		&&CASE_EEOP_VEC_PARAM_EXTERN,
		
		&&CASE_EEOP_VEC_AGGREF,
		&&CASE_EEOP_VEC_WINDOW_FUNC,
        &&CASE_EEOP_VEC_AGG_PLAIN_TRANS_BYVAL,
		&&CASE_EEOP_VEC_AGG_COLLECT_PLAIN_TRANS_BYVAL,
		&&CASE_EEOP_VEC_AGG_PLAIN_TRANS_BYREF,
		&&CASE_EEOP_VEC_AGG_COLLECT_PLAIN_TRANS_BYREF,
    	&&CASE_EEOP_VEC_AGG_ORDERED_TRANS_DATUM,
    	&&CASE_EEOP_VEC_AGG_ORDERED_TRANS_TUPLE,

		&&CASE_EEOP_VEC_BOOL_AND_STEP_FIRST,
		&&CASE_EEOP_VEC_BOOL_AND_STEP,
		&&CASE_EEOP_VEC_BOOL_AND_STEP_LAST,
		&&CASE_EEOP_VEC_BOOL_OR_STEP_FIRST,
		&&CASE_EEOP_VEC_BOOL_OR_STEP,
		&&CASE_EEOP_VEC_BOOL_OR_STEP_LAST,

		&&CASE_EEOP_VEC_CASEWHEN_WHEN_FIRST,
		&&CASE_EEOP_VEC_CASEWHEN_WHEN,
		&&CASE_EEOP_VEC_CASEWHEN_THEN_STR,
		&&CASE_EEOP_VEC_CASEWHEN_THEN,
		&&CASE_EEOP_VEC_CASEWHEN_DEFAULT_STR,
		&&CASE_EEOP_VEC_CASEWHEN_DEFAULT,

		&&CASE_EEOP_VEC_SCALARARRAYOP,

		&&CASE_EEOP_VEC_WINAGG_PLAIN_TRANS_BYVAL,
		&&CASE_EEOP_VEC_WINAGG_PLAIN_TRANS_BYREF,

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

					uint32 filter_rows = opresult->GetFilterRows();
					if (filter_rows) {
						econtext->use_filter = 
							((filter_rows * 100) / econtext->align_rows) < EXPR_FILTER_SELECTION_RATIO;
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
						uint32 filter_rows = opresult->GetFilterRows();
						if (!filter_rows) {
							econtext->use_filter = 
								((filter_rows * 100) / econtext->align_rows) < EXPR_FILTER_SELECTION_RATIO;
							state->resultvector->Reset();
							EEO_JUMP(op->d.qual.jumpdone);
						}
					}
					else {
						uint32 filter_rows = opresult->GetFilterRows();
						if (filter_rows) {
							ColumnVectorUint8And(opresult, filter, filter);
							uint32 filter_rows = filter->GetFilterRows();
							if (!filter_rows) {
								econtext->use_filter = 
									((filter_rows * 100) / econtext->align_rows) < EXPR_FILTER_SELECTION_RATIO;
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
						uint32 filter_rows = opresult->GetFilterRows();
						if (filter_rows) {
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
							uint32 filter_rows = filter->GetFilterRows();
							if (filter_rows) {
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

		EEO_CASE(EEOP_VEC_PARAM_EXEC)
		{
			/* out of line implementation: too large */
			VecExecEvalParamExec(state, op, econtext);

			EEO_NEXT();
		}

		EEO_CASE(EEOP_VEC_PARAM_EXTERN)
		{
			/* out of line implementation: too large */
			VecExecEvalParamExtern(state, op, econtext);
			EEO_NEXT();
		}

		EEO_CASE(EEOP_VEC_AGGREF)
		{
			AggrefExprState *aggref = op->d.aggref.astate;
			Assert(aggref->m_htbOffset < econtext->ecxt_aggbatch->Cols());
			(*op->resvector)->ShallowCopy(econtext->ecxt_aggbatch->GetColumn(aggref->m_htbOffset));
            EEO_NEXT();
		}

		EEO_CASE(EEOP_VEC_WINDOW_FUNC)
		{
			WindowFuncExprState *wfunc = op->d.window_func.wfstate;
			(*op->resvector)->ShallowCopy(wfunc->m_result_vec);
            EEO_NEXT();
		}
		
        EEO_CASE(EEOP_VEC_AGG_PLAIN_TRANS_BYVAL)
        {
            VecAggState *aggstate = castNode(VecAggState, state->parent);
            VecAggStatePerTrans pertrans = op->d.agg_trans.pertrans;
            Assert(pertrans->transtypeByVal);

			if (pertrans->vec_transfn_fcinfo.vec[0])
				pertrans->vec_transfn_fcinfo.vec[0]->PrefetchVectorData();

            ExecVecAggPlainTransByVal(econtext, aggstate, pertrans,
                                   op->d.agg_trans.aggcontext);

			EEO_NEXT();
        }

		EEO_CASE(EEOP_VEC_AGG_COLLECT_PLAIN_TRANS_BYVAL)
        {
			VecAggState *aggstate = castNode(VecAggState, state->parent);
            VecAggStatePerTrans pertrans = op->d.agg_trans.pertrans;
            Assert(pertrans->transtypeByVal);

			ExecVecAggCollectPlainTransByVal(econtext, aggstate, pertrans, 
									op->d.agg_trans.aggcontext);

			EEO_NEXT();
		}

		EEO_CASE(EEOP_VEC_AGG_PLAIN_TRANS_BYREF)
        {
            VecAggState   *aggstate = castNode(VecAggState, state->parent);
            VecAggStatePerTrans pertrans = op->d.agg_trans.pertrans;
            Assert(!pertrans->transtypeByVal);

            ExecVecAggPlainTransByRef(econtext, aggstate, pertrans,
                                   op->d.agg_trans.aggcontext);

            EEO_NEXT();
        }

		EEO_CASE(EEOP_VEC_AGG_COLLECT_PLAIN_TRANS_BYREF)
        {
            VecAggState   *aggstate = castNode(VecAggState, state->parent);
            VecAggStatePerTrans pertrans = op->d.agg_trans.pertrans;
            Assert(!pertrans->transtypeByVal);

            ExecVecAggCollectPlainTransByRef(econtext, aggstate, pertrans,
                                   op->d.agg_trans.aggcontext);

            EEO_NEXT();
        }

		EEO_CASE(EEOP_VEC_AGG_ORDERED_TRANS_DATUM)
		{
			VecAggState *aggstate = castNode(VecAggState, state->parent);
            VecAggStatePerTrans pertrans = op->d.agg_trans.pertrans;
            Assert(pertrans->transtypeByVal);

			if (pertrans->vec_transfn_fcinfo.vec[0])
				pertrans->vec_transfn_fcinfo.vec[0]->PrefetchVectorData();

            ExecVecAggPlainTransByVal(econtext, aggstate, pertrans,
                                   op->d.agg_trans.aggcontext);

			EEO_NEXT();
		}

		EEO_CASE(EEOP_VEC_AGG_ORDERED_TRANS_TUPLE)
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmodule(MOD_VEC_EXECUTOR), errmsg("need supported!")));
			
			EEO_NEXT();
		}

		EEO_CASE(EEOP_VEC_CASEWHEN_WHEN_FIRST)
		{	
			ColumnVectorUint8* whenrvector = (ColumnVectorUint8*)(op->d.casewhen.when_vector);
			uint32 filter_rows = whenrvector->GetFilterRowsSaveResult(op->d.casewhen.qual_result);

			op->d.casewhen.final_result->ShallowCopy(whenrvector);
			if (!filter_rows) {
				EEO_JUMP(op->d.casewhen.jumpnext);
			}

			EEO_NEXT();
		}

		EEO_CASE(EEOP_VEC_CASEWHEN_WHEN)
		{	
			ColumnVectorUint8* whenrvector = (ColumnVectorUint8*)(op->d.casewhen.when_vector);
			uint32 filter_rows;

			op->d.casewhen.case_result->Resize(op->d.casewhen.final_result->Size());
			ColumnVectorUint8Not(op->d.casewhen.final_result, op->d.casewhen.case_result);
			ColumnVectorUint8And(whenrvector, op->d.casewhen.case_result, op->d.casewhen.case_result);
			ColumnVectorUint8Or(op->d.casewhen.final_result, op->d.casewhen.case_result, op->d.casewhen.final_result);
			
			filter_rows = op->d.casewhen.case_result->GetFilterRowsSaveResult(op->d.casewhen.qual_result);
			if (!filter_rows) {
				EEO_JUMP(op->d.casewhen.jumpnext);
			}
			
			EEO_NEXT();
		}

		EEO_CASE(EEOP_VEC_CASEWHEN_THEN_STR)
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmodule(MOD_VEC_EXECUTOR), errmsg("need supported!")));
			
			EEO_NEXT();
		}

		EEO_CASE(EEOP_VEC_CASEWHEN_THEN)
		{
			op->d.casewhen.final_vector->FilterCopy(op->d.casewhen.case_vector.then_vector, op->d.casewhen.qual_result);
			*(op->d.casewhen.count) += op->d.casewhen.qual_result->rows;
			if (*(op->d.casewhen.count) == econtext->align_rows) {
				/* for the next expression evaluation */
				*(op->d.casewhen.count) = 0;
				EEO_JUMP(op->d.casewhen.jumpdone);
			}

			EEO_NEXT();
		}

		EEO_CASE(EEOP_VEC_CASEWHEN_DEFAULT_STR)
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmodule(MOD_VEC_EXECUTOR), errmsg("need supported!")));

			EEO_NEXT();
		}

		EEO_CASE(EEOP_VEC_CASEWHEN_DEFAULT)
		{
			uint32 filter_rows;

			ColumnVectorUint8Not(op->d.casewhen.final_result, op->d.casewhen.final_result);
			filter_rows = op->d.casewhen.final_result->GetFilterRowsSaveResult(op->d.casewhen.qual_result);

			if (filter_rows) {
				op->d.casewhen.final_vector->FilterCopy(op->d.casewhen.case_vector.default_vector, op->d.casewhen.qual_result);
				*(op->d.casewhen.count) += op->d.casewhen.qual_result->rows;
			}

			Assert(*(op->d.casewhen.count) == econtext->align_rows);
			/* for the next expression evaluation */
			*(op->d.casewhen.count) = 0;

			op->d.casewhen.final_vector->Resize(econtext->align_rows);
			(*op->resvector)->ShallowCopy(op->d.casewhen.final_vector);

			EEO_NEXT();
		}
		
		EEO_CASE(EEOP_VEC_BOOL_AND_STEP_FIRST)
		{
			ColumnVectorUint8* opresult = (ColumnVectorUint8*)(op->d.boolqual.expr_result);
			uint32 filter_rows = opresult->GetFilterRows();
			Assert(op->d.boolqual.bool_result == *op->resvector);

			op->d.boolqual.bool_result->ShallowCopy(opresult);
			if (!filter_rows) {
				EEO_JUMP(op->d.boolqual.jumpdone);
			}

			EEO_NEXT();
		}

		EEO_CASE(EEOP_VEC_BOOL_AND_STEP)
		{
			ColumnVectorUint8* opresult = (ColumnVectorUint8*)(op->d.boolqual.expr_result);
			uint32 filter_rows = opresult->GetFilterRows();
			Assert(op->d.boolqual.bool_result == *op->resvector);

			if (filter_rows) {
				ColumnVectorUint8And(opresult, op->d.boolqual.bool_result, op->d.boolqual.bool_result);
			}
			else {
				op->d.boolqual.bool_result->ShallowCopy(opresult);
				EEO_JUMP(op->d.boolqual.jumpdone);
			}

			EEO_NEXT();
		}

		EEO_CASE(EEOP_VEC_BOOL_AND_STEP_LAST)
		{
			ColumnVectorUint8* opresult = (ColumnVectorUint8*)(op->d.boolqual.expr_result);
			uint32 filter_rows = opresult->GetFilterRows();
			Assert(op->d.boolqual.bool_result == *op->resvector);

			if (filter_rows) {
				ColumnVectorUint8And(opresult, op->d.boolqual.bool_result, (ColumnVectorUint8*)(*op->resvector));
			}
			else {
				(*op->resvector)->ShallowCopy(opresult);
			}

			EEO_NEXT();
		}

		EEO_CASE(EEOP_VEC_BOOL_OR_STEP_FIRST)
		{
			ColumnVectorUint8* opresult = (ColumnVectorUint8*)(op->d.boolqual.expr_result);
			uint32 filter_rows = opresult->GetFilterRows();
			Assert(op->d.boolqual.bool_result == *op->resvector);

			op->d.boolqual.bool_result->ShallowCopy(opresult);
			if (filter_rows == econtext->align_rows) {
				EEO_JUMP(op->d.boolqual.jumpdone);
			}
			else if (!filter_rows) {
				/* for Or fast pass  */
				op->d.boolqual.bool_result->Reset();
			}

			EEO_NEXT();
		}

		EEO_CASE(EEOP_VEC_BOOL_OR_STEP)
		{
			ColumnVectorUint8* opresult = (ColumnVectorUint8*)(op->d.boolqual.expr_result);
			uint32 filter_rows = opresult->GetFilterRows();
			Assert(op->d.boolqual.bool_result == *op->resvector);

			if (filter_rows == econtext->align_rows) {
				op->d.boolqual.bool_result->ShallowCopy(opresult);
				EEO_JUMP(op->d.boolqual.jumpdone);
			}
			else if (filter_rows) {
				if (op->d.boolqual.bool_result->Size()) {
					ColumnVectorUint8Or(opresult, op->d.boolqual.bool_result, op->d.boolqual.bool_result);
				}
				else {
					op->d.boolqual.bool_result->ShallowCopy(opresult);
				}	
			}
			else {
				/* both are zero, continue pass */
				if (!op->d.boolqual.bool_result->Size()) {
					op->d.boolqual.bool_result->Reset();
				}
			}
			
			EEO_NEXT();
		}

		EEO_CASE(EEOP_VEC_BOOL_OR_STEP_LAST)
		{
			ColumnVectorUint8* opresult = (ColumnVectorUint8*)(op->d.boolqual.expr_result);
			uint32 filter_rows = opresult->GetFilterRows();
			Assert(op->d.boolqual.bool_result == *op->resvector);
			
			if (filter_rows == econtext->align_rows) {
				(*op->resvector)->ShallowCopy(opresult);
			}
			else if (filter_rows) {
				if (op->d.boolqual.bool_result->Size()) {
					ColumnVectorUint8Or(opresult, op->d.boolqual.bool_result, (ColumnVectorUint8*)(*op->resvector));
				}
				else {
					(*op->resvector)->ShallowCopy(opresult);
				}
			}
			else {
				if (op->d.boolqual.bool_result->Size()) {
					(*op->resvector)->ShallowCopy(op->d.boolqual.bool_result);
				}
				else {
					(*op->resvector)->ShallowCopy(opresult);
				}
			}
			
			EEO_NEXT();
		}

		EEO_CASE(EEOP_VEC_SCALARARRAYOP)
		{
			VecFunctionCallInfo fcinfo = op->d.scalararrayop.fcinfo;
			fcinfo->rows = econtext->align_rows;
			op->d.scalararrayop.exec_scalararray_op(op);
			fcinfo->vec[op->d.scalararrayop.nargs - 1]->Resize(fcinfo->rows);

			EEO_NEXT();
		}

		EEO_CASE(EEOP_VEC_WINAGG_PLAIN_TRANS_BYVAL)
        {
            VecWindowAggState *winagg_state = castNode(VecWindowAggState, state->parent);
            WindowStatePerFunc perfunc = op->d.winagg_trans.perfunc;
			VecFunctionCallInfo trans_fcinfo = op->d.winagg_trans.trans_fcinfo;

            Assert(perfunc->resulttypeByVal);

			if (trans_fcinfo->vec[0])
				trans_fcinfo->vec[0]->PrefetchVectorData();

            ExecVecWinAggPlainTransByVal(econtext, winagg_state, trans_fcinfo, op->d.agg_trans.aggcontext);

			EEO_NEXT();
        }

		EEO_CASE(EEOP_VEC_WINAGG_PLAIN_TRANS_BYREF)
        {
            VecWindowAggState *winagg_state = castNode(VecWindowAggState, state->parent);
            WindowStatePerFunc perfunc = op->d.winagg_trans.perfunc;
            VecFunctionCallInfo trans_fcinfo = op->d.winagg_trans.trans_fcinfo;

            Assert(!perfunc->resulttypeByVal);

            if (trans_fcinfo->vec[0])
                trans_fcinfo->vec[0]->PrefetchVectorData();

            ExecVecWinAggPlainTransByVal(econtext, winagg_state, trans_fcinfo, op->d.agg_trans.aggcontext);

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
	uint32 idx = 0;
	int32 pos = 0;
	int32 rows = batch->Rows();
	int32 cols = batch->Cols();
	CVector *src = NULL;
	CVector *dst = NULL;
	ListCell *lc;

	Assert(packbatch->Cols() == cols);

	if (packlist) {
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
	}
	else {
		for (uint32 i = 0; i < packbatch->Cols(); ++i) {
			dst = packbatch->GetColumn(i);
			src = batch->GetColumn(i);
			Assert(dst);
			Assert(src);

			pos = *batchpos;
			src->PrefetchVectorData();
			dst->VectorPack(src, QualResult, &pos);
		}
	}

	Assert(pos <= ColumnVectorSize);
	Assert(rows + *batchpos == src->Size());
	Assert(*batchpos <= pos);
	packbatch->SetRows(dst->Size());
	batch->SetRows(rows - (pos - *batchpos));
	*batchpos = pos;

	return;
}

void ExecVecWinAggPlainTransByVal(VecExprContext *econtext, VecWindowAggState *aggstate,
    VecFunctionCallInfo trans_fcinfo, MemoryContext aggcontext)
{
    MemoryContext oldContext = MemoryContextSwitchTo(aggstate->vectmpcontext->ecxt_per_tuple_memory);

    trans_fcinfo->rows = econtext->align_rows;
	if (trans_fcinfo->vec[0])
		trans_fcinfo->args[0] = (void *)trans_fcinfo->vec[0]->Data();

    trans_fcinfo->finfo->vector_fn_addr(trans_fcinfo);

    MemoryContextSwitchTo(oldContext);
}