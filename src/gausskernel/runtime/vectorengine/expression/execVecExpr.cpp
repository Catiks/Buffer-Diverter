#include "postgres.h"
#include "nodes/execnodes.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_type.h"
#include "catalog/pg_proc_fn.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "pgstat.h"
#include "utils/builtins.h"
#include "utils/datum.h"
#include "utils/acl.h"
#include "utils/fmgrtab.h"
#include "fmgr.h"
#include "vecexecutor/execVecExpr.h"
#include "vecexecutor/vecexecutor.h"
#include "vecexecutor/columnvector.h"
#include "vecexecutor/vecfunc.h"
#include "vecexecutor/vecnodes.h"
#include "executor/node/nodeAgg.h"
#include "windowapi.h"

static void ExecReadyVecExpr(VecExprState *state);
static void ExecInitVecExprRec(Expr *node, VecExprState *state, CVector **resv, Expr *parent = NULL);
static void VecExprEvalPushStep(VecExprState *es, const VecExprEvalStep *s);
static void ExecInitVecFunc(VecExprEvalStep *scratch, Expr *node, List *args,
			 Oid funcid, Oid inputcollid, VecExprState *state);
static void ExecInitScalarArrayOp(VecExprEvalStep *scratch, Expr *node, List *args,
			 Oid funcid, Oid inputcollid, VecExprState *state);

VecExprState* ExecInitVectorExpr(Expr *node, PlanState *parent)
{
	VecExprState  *state;
	VecExprEvalStep scratch;

	if (node == NULL)
		return NULL;

	state = makeNode(VecExprState);
	state->expr = node;
	state->parent = parent;
	state->resultvector = NULL;

	ExecInitVecExprRec(node, state, &state->resultvector);

	scratch.opcode = EEOP_VEC_DONE;
	VecExprEvalPushStep(state, &scratch);

	ExecReadyVecExpr(state);

	return state;
}

List* ExecInitVectorExprList(List *nodes, PlanState *parent)
{
   List	   *result = NIL;
   ListCell   *lc;

   foreach(lc, nodes)
   {
       Expr	   *e = (Expr*)lfirst(lc);

       result = lappend(result, ExecInitVectorExpr(e, parent));
   }

   return result;
}

static void ExecReadyVecExpr(VecExprState *state)
{
	ExecReadyInterpretedVecExpr(state);
}

static void ExecInitVecExprRec(Expr *node, VecExprState *state, CVector **resv, Expr *parent)
{
    VecExprEvalStep scratch;

	/* Guard against stack overflow due to overly complex expressions */
	check_stack_depth();

	scratch.resvector = resv;

	/* cases should be ordered as they are in enum NodeTag */
	switch (nodeTag(node))
	{
        case T_Var:
			{
				Var *variable = (Var *) node;
				switch (variable->varno) {
					case INNER_VAR:
						scratch.opcode = EEOP_VEC_INNER_VECTOR;
						break;
					case OUTER_VAR:
						scratch.opcode = EEOP_VEC_OUTER_VECTOR;
						break;
					default:
						scratch.opcode = EEOP_VEC_SCAN_VECTOR;
						break;
				}		

				/* must be reference scan/inner/outer batch, direct project have EEOP_VEC_PROJ_*_VECTOR */
				Assert(*resv == NULL);

				CVector* resultvector = AllocColumnVectorByType(CurrentMemoryContext, variable->vartype);
				*resv = resultvector;
				scratch.d.var_vector.attnum = variable->varattno - 1;
				VecExprEvalPushStep(state, &scratch);
				break;
			}
        case T_Const:
			{
				Const *con = (Const *) node;
				
				/* must be create const vector */
				Assert(*resv == NULL);
				
				CVector* resultvector = AllocColumnVectorByType(CurrentMemoryContext, con->consttype);
				resultvector->Init();
				
				if (con->constbyval) {
					resultvector->InitByValue(&con->constvalue, ColumnVectorSize);
				}
				else {
					resultvector->InitByValue(VARDATA_ANY(con->constvalue), 
						ColumnVectorSize, VARSIZE_ANY(con->constvalue) - VARHDRSZ);
				}
                *resv = resultvector;
				break;				
			}
        case T_FuncExpr:
			{
				FuncExpr *func = (FuncExpr *) node;

				/* result must be create by me */
				Assert(*resv == NULL);

				ExecInitVecFunc(
					&scratch, node, func->args, func->funcid, func->inputcollid, state);

				scratch.opcode = EEOP_VEC_FUNCEXPR;
				VecExprEvalPushStep(state, &scratch);
				break;
			}
		case T_OpExpr:
			{
				OpExpr *func = (OpExpr *) node;

				/* result must be create by me, should uint8 filter */
				Assert(*resv == NULL);
			
				ExecInitVecFunc(
					&scratch, node, func->args, func->opfuncid, func->inputcollid, state);

				scratch.opcode = EEOP_VEC_FUNCEXPR;
				VecExprEvalPushStep(state, &scratch);
				break;
			}
		case T_Param:
			{
				Param *param = (Param *) node;			

				switch (param->paramkind)
				{
					case PARAM_EXEC:
						scratch.opcode = EEOP_VEC_PARAM_EXEC;
						scratch.d.param.paramid = param->paramid;
						scratch.d.param.paramtype = param->paramtype;
						VecExprEvalPushStep(state, &scratch);
						break;
					case PARAM_EXTERN:
						scratch.opcode = EEOP_VEC_PARAM_EXTERN;
						scratch.d.param.paramid = param->paramid;
						scratch.d.param.paramtype = param->paramtype;

						if (parent && IsA(parent, FuncExpr) && 
							expr_func_has_refcursor_args(((FuncExpr*)parent)->funcid))
							scratch.d.param.is_cursor = true;
						else
							scratch.d.param.is_cursor = false;

						VecExprEvalPushStep(state, &scratch);
						break;
					default:
						elog(ERROR, "unrecognized paramkind: %d", (int) param->paramkind);
						break;
				}
				break;
			}
		case T_Aggref:
			{
				Aggref* aggref = (Aggref*)node;
            	AggrefExprState* astate = makeNode(AggrefExprState);
				astate->aggref = aggref;
				astate->xprstate.expr = node;

				if (state->parent && (IsA(state->parent, AggState) || IsA(state->parent, VecAggState))) {
					AggState* aggstate = (AggState*)state->parent;
					int naggs;

					aggstate->aggs = lappend(aggstate->aggs, astate);
					naggs = ++aggstate->numaggs;

					/*
					* Complain if the aggregate's arguments contain any
					* aggregates; nested agg functions are semantically
					* nonsensical.  (This should have been caught earlier,
					* but we defend against it here anyway.)
					*/
					if (naggs != aggstate->numaggs)
                    	ereport(ERROR,
                    	    (errcode(ERRCODE_GROUPING_ERROR),
                    	        errmodule(MOD_VEC_EXECUTOR),
                    	        errmsg("aggregate function calls cannot be nested")));
				} 
				else {
					/* planner messed up */
					ereport(ERROR, (errcode(ERRCODE_UNEXPECTED_NODE_STATE), errmsg("Aggref found in non-Agg plan node")));
				}

				CVector* resultvector = AllocColumnVectorByType(CurrentMemoryContext, aggref->aggtype);
				*resv = resultvector;
				scratch.d.aggref.astate = astate;
				scratch.opcode = EEOP_VEC_AGGREF;
				VecExprEvalPushStep(state, &scratch);
				break;
			} 

		case T_WindowFunc:
			{
				WindowFunc* wfunc = (WindowFunc*)node;
				WindowFuncExprState* wfstate = makeNode(WindowFuncExprState);
				wfstate->wfunc = wfunc;
				wfstate->xprstate.expr = node;
				
				if (state->parent && (IsA(state->parent, WindowAggState) || IsA(state->parent, VecWindowAggState))) {
					VecWindowAggState* winstate = (VecWindowAggState*)state->parent;
					int nfuncs;

					winstate->funcs = lappend(winstate->funcs, wfstate);
					nfuncs = ++winstate->numfuncs;
					if (wfunc->winagg)
						winstate->numaggs++;

					wfstate->args = ExecInitVectorExprList(wfunc->args, state->parent);

					/*
					* Complain if the windowfunc's arguments contain any
					* windowfuncs; nested window functions are semantically
					* nonsensical.  (This should have been caught earlier,
					* but we defend against it here anyway.)
					*/
					if (nfuncs != winstate->numfuncs)
						ereport(
							ERROR, (errcode(ERRCODE_WINDOWING_ERROR), errmsg("window function calls cannot be nested")));
				} else {
					/* planner messed up */
					ereport(
						ERROR, (errcode(ERRCODE_WINDOWING_ERROR), errmsg("WindowFunc found in non-WindowAgg plan node")));
				}

				CVector* resultvector = AllocColumnVectorByType(CurrentMemoryContext, wfunc->wintype);
				*resv = resultvector;

				scratch.opcode = EEOP_VEC_WINDOW_FUNC;
				scratch.d.window_func.wfstate = wfstate;
				VecExprEvalPushStep(state, &scratch);
				break;
			}
		case T_RelabelType: 
			{
				RelabelType* relabel = (RelabelType*) node;
				ExecInitVecExprRec(relabel->arg, state, resv, node);
				break;
			}
		case T_BoolExpr:
			{
				BoolExpr* boolexpr = (BoolExpr*)node;
				int nargs = list_length(boolexpr->args);
				List *adjust_jumps = NIL;
				int off;
				ListCell *lc;
				ColumnVectorUint8* orresult;

				orresult = New(CurrentMemoryContext) ColumnVectorUint8(CurrentMemoryContext);		
				off = 0;
				foreach(lc, boolexpr->args) {
					Expr *node = (Expr *) lfirst(lc);
					CVector** resultvector = (CVector**)palloc(sizeof(CVector**));
					*resultvector = NULL;

					ExecInitVecExprRec(node, state, resultvector);

					switch (boolexpr->boolop)
					{
						case AND_EXPR:
							Assert(nargs >= 2);

							if (off == 0)
								scratch.opcode = EEOP_VEC_BOOL_AND_STEP_FIRST;
							else if (off + 1 == nargs)
								scratch.opcode = EEOP_VEC_BOOL_AND_STEP_LAST;
							else
								scratch.opcode = EEOP_VEC_BOOL_AND_STEP;
							break;
						case OR_EXPR:
							Assert(nargs >= 2);

							if (off == 0)
								scratch.opcode = EEOP_VEC_BOOL_OR_STEP_FIRST;
							else if (off + 1 == nargs)
								scratch.opcode = EEOP_VEC_BOOL_OR_STEP_LAST;
							else
								scratch.opcode = EEOP_VEC_BOOL_OR_STEP;
							break;
						default:
							elog(ERROR, "unrecognized boolop: %d", (int) boolexpr->boolop);
							break;
					}

					scratch.d.boolqual.jumpdone = -1;
					scratch.d.boolqual.bool_result = orresult;
					scratch.d.boolqual.expr_result = *resultvector;
					VecExprEvalPushStep(state, &scratch);
					adjust_jumps = lappend_int(adjust_jumps, state->steps_len - 1);
					off++;
				}

				*scratch.resvector = orresult;
				foreach(lc, adjust_jumps) {
					VecExprEvalStep *as = &state->steps[lfirst_int(lc)];
					Assert(as->d.boolqual.jumpdone == -1);
					as->d.boolqual.jumpdone = state->steps_len;
				}
				break;
			}
		case T_CaseExpr:
			{
				CaseExpr *caseExpr = (CaseExpr*)node;
				List *adjust_jumps = NIL;
				ListCell *lc;
				CVector* finalvector;
				ColumnVectorUint8* finalresult;
				bool is_first = true;
				bool result_is_str;
				uint32* count;

				Assert(*resv == NULL);
				result_is_str = !ColumnVectorIsFixLen(caseExpr->casetype);
				finalvector = AllocColumnVectorByType(CurrentMemoryContext, caseExpr->casetype);
				finalvector->Init();
				*resv = finalvector;
				
				finalresult = New(CurrentMemoryContext) ColumnVectorUint8(CurrentMemoryContext);
				count = (uint32*)palloc(sizeof(uint32));
				*count = 0;

				foreach(lc, caseExpr->args) {
					CaseWhen *when = (CaseWhen*)lfirst(lc);
					CVector** whenvector = (CVector**)palloc(sizeof(CVector*));
					*whenvector = NULL;
					CVector** thenvector = (CVector**)palloc(sizeof(CVector*));
					*thenvector = NULL;
					int	whenstep;
					VecQualResult* qualresult = CreateQualResult();
					ColumnVectorUint8* caseresult = New(CurrentMemoryContext) ColumnVectorUint8(CurrentMemoryContext);
					caseresult->Init();

					ExecInitVecExprRec(when->expr, state, whenvector);

					if (is_first)
						scratch.opcode = EEOP_VEC_CASEWHEN_WHEN_FIRST;
					else
						scratch.opcode = EEOP_VEC_CASEWHEN_WHEN;
					scratch.d.casewhen.jumpnext = -1;
					scratch.d.casewhen.jumpdone = -1;
					scratch.d.casewhen.when_vector = *whenvector;
					scratch.d.casewhen.qual_result = qualresult;
					scratch.d.casewhen.case_result = caseresult;
					scratch.d.casewhen.final_result = finalresult;
					scratch.d.casewhen.count = count;
					VecExprEvalPushStep(state, &scratch);
					
					whenstep = state->steps_len - 1;
					is_first = false;

					ExecInitVecExprRec(when->result, state, thenvector);

					if (result_is_str)
						scratch.opcode = EEOP_VEC_CASEWHEN_THEN_STR;
					else
						scratch.opcode = EEOP_VEC_CASEWHEN_THEN;
					scratch.d.casewhen.jumpnext = -1;
					scratch.d.casewhen.jumpdone = -1;
					scratch.d.casewhen.qual_result = qualresult;
					scratch.d.casewhen.case_vector.then_vector = *thenvector;
					scratch.d.casewhen.final_vector = finalvector;
					VecExprEvalPushStep(state, &scratch);

					adjust_jumps = lappend_int(adjust_jumps, state->steps_len - 1);
					state->steps[whenstep].d.casewhen.jumpnext = state->steps_len;
				}

				if (caseExpr->defresult) {
					CVector** defaultvector = (CVector**)palloc(sizeof(CVector**));
					*defaultvector = NULL;
					VecQualResult* qualresult = CreateQualResult();

					ExecInitVecExprRec(caseExpr->defresult, state, defaultvector);

					if (result_is_str)
						scratch.opcode = EEOP_VEC_CASEWHEN_DEFAULT_STR;
					else
						scratch.opcode = EEOP_VEC_CASEWHEN_DEFAULT;
					scratch.d.casewhen.qual_result = qualresult;
					scratch.d.casewhen.case_vector.default_vector = *defaultvector;
					scratch.d.casewhen.final_result = finalresult;
					scratch.d.casewhen.final_vector = finalvector;
					scratch.d.casewhen.count = count;
					VecExprEvalPushStep(state, &scratch);
				}
				else {
					ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmodule(MOD_VEC_EXECUTOR), errmsg("null need supported!")));
				}

				foreach(lc, adjust_jumps) {
					VecExprEvalStep *as = &state->steps[lfirst_int(lc)];
					Assert(as->opcode == EEOP_VEC_CASEWHEN_THEN_STR || 
						   as->opcode == EEOP_VEC_CASEWHEN_THEN);
					Assert(as->d.casewhen.jumpdone == -1);
					as->d.casewhen.jumpdone = state->steps_len;
				}
				break;
			}
		case T_ScalarArrayOpExpr:
		{
			ScalarArrayOpExpr* opexpr = (ScalarArrayOpExpr*)node;

			/* result must be create by me, should uint8 filter */
			Assert(*resv == NULL);
			
			ExecInitScalarArrayOp(
				&scratch, node, opexpr->args, opexpr->opfuncid,  opexpr->inputcollid, state);
					
			scratch.opcode = EEOP_VEC_SCALARARRAYOP;
			VecExprEvalPushStep(state, &scratch);
			break;
		}

		default:
			elog(ERROR, "unrecognized node type: %d, line=%d, func:%s",
				 (int) nodeTag(node), __LINE__, __func__);	
			break;
	}
}

void VecExprEvalPushStep(VecExprState *es, const VecExprEvalStep *s)
{
    if (es->steps_alloc == 0)
	{
		es->steps_alloc = 16;
		es->steps = (VecExprEvalStep*)palloc(sizeof(VecExprEvalStep) * es->steps_alloc);
	}
	else if (es->steps_alloc == es->steps_len)
	{
		es->steps_alloc *= 2;
		es->steps = (VecExprEvalStep*)repalloc(es->steps, sizeof(VecExprEvalStep) * es->steps_alloc);
	}

	memcpy(&es->steps[es->steps_len++], s, sizeof(VecExprEvalStep));
}

Oid search_typeid_from_funid(Oid funcid) {
	Oid TypeId = InvalidOid;
	switch (funcid) {
		case 65:
		case 66:
		case 144:
		case 147:
		case 149:
		case 150:
		case 351:
			TypeId = INT4OID;
			break;
		case 467:
		case 468:
		case 469:
		case 470:
		case 471:
		case 472:
		case 474:
		case 475:
		case 476:
		case 477:
		case 478:
		case 479:
		case 842:
		case 852:
		case 853:
		case 854:
		case 855:
		case 856:
		case 857:
		case 2052:
		case 2053:
		case 2054:
		case 2055:
		case 2056:
		case 2057:
			TypeId = INT8OID;
			break;
		case 202:
		case 203:
		case 204:
		case 205:
			TypeId = FLOAT4OID;
			break;
		case 216:
		case 217:
		case 218:
		case 219:
		case 355:
		case 293:
		case 294:
		case 295:
		case 296:
		case 297:
		case 298:
			TypeId = FLOAT8OID;
			break;
		case 67:
		case 850:
			TypeId = TEXTOID;
		case 360:
			TypeId = VARCHAROID;
			break;
		case 1048:
		case 1078:
			TypeId = BPCHAROID;
			break;
		case 2045:
			TypeId = TIMESTAMPOID;
			break;
		default:
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmodule(MOD_VEC_EXECUTOR), 
					errmsg("UnSupported vector function id:%d, line=%d, func:%s", funcid, __LINE__, __func__)));
			TypeId = InvalidOid;
			break;
	}

	return TypeId;
}

void FuncArgsDispatch(int FArgsId, int LArgsId, int RArgsId, int* idx) {
	if (FArgsId == LArgsId) {
		switch (RArgsId) {
			case INT1OID:
				*idx = 2;
				return;
			case INT2OID:
				*idx = 3;
				return;
			case INT4OID:
			case DATEOID:
			case FLOAT4OID:
				*idx = 4;
				return;
			case INT8OID:
			case TIMESTAMPOID:
			case TIDOID:
			case FLOAT8OID:
			case TEXTOID: 
			case VARCHAROID:
			case BPCHAROID:
				*idx = 5;
				return;
			default:
				*idx = -1;
				return;
		}
	}
	else {
		switch (LArgsId) {
			case INT1OID:
				*idx = 6;
				return;
			case INT2OID:
				*idx = 7;
				return;
			case INT4OID:
			case DATEOID:
			case FLOAT4OID:
				*idx = 8;
				return;
			case INT8OID:
			case TIMESTAMPOID:
			case TIDOID:
			case FLOAT8OID:
			case TEXTOID: 
			case VARCHAROID:
			case BPCHAROID:
				*idx = 9;
				return;
			default:
				*idx = -1;
				return;
		}
	}
}

static void ExecInitScalarArrayOp(VecExprEvalStep *scratch, Expr *node, List *args,
			 Oid funcid, Oid inputcollid, VecExprState *state)
{
	AclResult	aclresult;
	FmgrInfo   *finfo;
	VecFunctionCallInfo fcinfo;
	VectorFuncCacheEntry* entry = NULL;
    bool found = false;
	CVector* resvector;
    Oid arg_type = InvalidOid;
	int nargs;
	int idx = 1; //const always on the right
	Expr *scalararg;
	Expr *arrayarg;

	/* Check permission to call function */
	aclresult = pg_proc_aclcheck(funcid, GetUserId(), ACL_EXECUTE);
	if (aclresult != ACLCHECK_OK)
		aclcheck_error(aclresult, ACL_KIND_PROC, get_func_name(funcid));

	finfo = (FmgrInfo*)palloc0(sizeof(FmgrInfo));
	fcinfo =(VecFunctionCallInfo) palloc0(sizeof(VecFunctionCallInfoData));

	fmgr_info(funcid, finfo);
	fmgr_info_set_expr((Node*)node, finfo);
	InitVecFunctionCallInfoData(fcinfo, finfo, inputcollid);

	nargs = list_length(args);
	Assert(nargs == 2);

	scalararg = (Expr*)linitial(args);
	arrayarg = (Expr*)lsecond(args);

	ExecInitVecExprRec(scalararg, state, &fcinfo->vec[0], node);
	fcinfo->args[0] = fcinfo->vec[0]->DataAddr();
	fcinfo->offset[0] = fcinfo->vec[0]->OffsetAddr();
	fcinfo->null[0] = fcinfo->vec[0]->BitmapAddr();
	arg_type = exprType((Node*)scalararg);

	Oid funcrettype;
	TupleDesc tupdesc;
	get_expr_result_type((Node*)node, &funcrettype, &tupdesc);
	resvector = AllocColumnVectorByType(CurrentMemoryContext, funcrettype);
	resvector->Init(true);
	fcinfo->args[2] = resvector->DataAddr();
	fcinfo->vec[2] = resvector;

    entry = (VectorFuncCacheEntry*)hash_search(g_instance.vector_func_hash, &funcid, HASH_FIND, &found);
    if (found && entry && idx != -1 && entry->vec_fn_cache_append[idx]) {
        scratch->d.scalararrayop.vec_fn_addr = entry->vec_fn_cache_append[idx];
    }
    else {
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmodule(MOD_VEC_EXECUTOR), 
			errmsg("UnSupported vector function id:%d, line=%d, func:%s", funcid, __LINE__, __func__)));
    }

	*scratch->resvector = resvector;
	scratch->d.scalararrayop.fcinfo = fcinfo;
	scratch->d.scalararrayop.finfo = finfo;
	scratch->d.scalararrayop.nargs = nargs + 1;
	if (ColumnVectorIsFixLen(arg_type))
		scratch->d.scalararrayop.exec_scalararray_op = ExecVecFixlenScalarArrayOp;
	else
		scratch->d.scalararrayop.exec_scalararray_op = ExecVecStrScalarArrayOp;

	scratch->d.scalararrayop.arr = (ArrayType*)(((Const*)arrayarg)->constvalue);
}

static void ExecInitVecFunc(VecExprEvalStep *scratch, Expr *node, List *args, Oid funcid,
			 Oid inputcollid, VecExprState *state)
{
	AclResult	aclresult;
	FmgrInfo   *finfo;
	VecFunctionCallInfo fcinfo;
	int			argno;
	ListCell   *lc;
	VectorFuncCacheEntry* entry = NULL;
    bool found = false;
	CVector* resvector;
	uint32 const_argno[VECFUNC_ARGS];
    Oid arg_type[VECFUNC_ARGS];
    Const* const_val[VECFUNC_ARGS];
	int const_num;
	int nargs;
	int idx = -1;

	/* Check permission to call function */
	aclresult = pg_proc_aclcheck(funcid, GetUserId(), ACL_EXECUTE);
	if (aclresult != ACLCHECK_OK)
		aclcheck_error(aclresult, ACL_KIND_PROC, get_func_name(funcid));

	finfo = (FmgrInfo*)palloc0(sizeof(FmgrInfo));
	fcinfo =(VecFunctionCallInfo) palloc0(sizeof(VecFunctionCallInfoData));

	fmgr_info(funcid, finfo);
	fmgr_info_set_expr((Node*)node, finfo);
	InitVecFunctionCallInfoData(fcinfo, finfo, inputcollid);
	
	memset(const_argno, 0, sizeof(uint32) * VECFUNC_ARGS);
	memset(arg_type, 0, sizeof(Oid) * VECFUNC_ARGS);
	memset(const_val, 0, sizeof(Const*) * VECFUNC_ARGS);

	argno = 0;
	const_num = 0;
	nargs = list_length(args);
	if (nargs > 1) {
		foreach(lc, args) {
			Expr *arg = (Expr *) lfirst(lc);

			if (IsA(arg, Const)) {
				Const *con = (Const*)arg;
    	        const_argno[const_num] = argno;
    	        const_val[const_num] = con;
				const_num++;
			}
			else {
				arg_type[argno] = exprType((Node *)arg);
    	    	ExecInitVecExprRec(arg, state, &fcinfo->vec[argno], node);
				fcinfo->args[argno] = fcinfo->vec[argno]->DataAddr();
				fcinfo->offset[argno] = fcinfo->vec[argno]->OffsetAddr();
				fcinfo->null[argno] = fcinfo->vec[argno]->BitmapAddr();
			}
			argno++;
		}

		/* for include const OP function */
		if (nargs == 2 && const_num == 1) {
			// TODO:// when the type of const value is not a num type, we  dont trans it.
			if (const_val[0]->consttype != TEXTOID) {
				const_val[0]->consttype = arg_type[(int)(!const_argno[0])];
			}
			fcinfo->vec[const_argno[0]] = CreateColumnVectorConst(CurrentMemoryContext, const_val[0]);
			fcinfo->args[const_argno[0]] = fcinfo->vec[const_argno[0]]->DataAddr();
			fcinfo->offset[const_argno[0]] = fcinfo->vec[const_argno[0]]->OffsetAddr();
			fcinfo->null[const_argno[0]] = fcinfo->vec[const_argno[0]]->BitmapAddr();
			idx = const_argno[0];
		}
		/* for two var function */
		else if (nargs == 2 && const_num == 0) {		
			Oid TypeId = search_typeid_from_funid(funcid);
			FuncArgsDispatch(TypeId, arg_type[0], arg_type[1], &idx);
		}
		/* for include const and var function */
		else if (nargs != const_num && const_num) {
			for (int i = 0; i < const_num; ++i) {
				fcinfo->vec[const_argno[i]] = CreateColumnVectorConst(CurrentMemoryContext, const_val[i]);
				fcinfo->args[const_argno[i]] = fcinfo->vec[const_argno[i]]->DataAddr();
				fcinfo->offset[const_argno[i]] = fcinfo->vec[const_argno[i]]->OffsetAddr();
				fcinfo->null[const_argno[i]] = fcinfo->vec[const_argno[i]]->BitmapAddr();
			}
			idx = 0;
		}
		else {
			/* other number of const */
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmodule(MOD_VEC_EXECUTOR), errmsg("need supported!")));
		}
	}
	else if (nargs == 1) {
		Expr *arg = (Expr *)lfirst(list_head(args));

		if (IsA(arg, Const))
			fcinfo->vec[argno] = CreateColumnVectorConst(CurrentMemoryContext, (Const*)arg);
		else
    		ExecInitVecExprRec(arg, state, &fcinfo->vec[argno], node);

		fcinfo->args[argno] = fcinfo->vec[argno]->DataAddr();
		fcinfo->offset[argno] = fcinfo->vec[argno]->OffsetAddr();
		fcinfo->null[argno] = fcinfo->vec[argno]->OffsetAddr();
		argno++;
		idx = 0;
	}
	else {
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmodule(MOD_VEC_EXECUTOR), errmsg("need supported!")));
	}

	Oid funcrettype;
	TupleDesc tupdesc;
	get_expr_result_type((Node*)node, &funcrettype, &tupdesc);
	resvector = AllocColumnVectorByType(CurrentMemoryContext, funcrettype);
	resvector->Init();
	fcinfo->args[argno] = resvector->DataAddr();
	fcinfo->vec[argno] = resvector;
	argno++;

	Assert(idx >= 0);
    entry = (VectorFuncCacheEntry*)hash_search(g_instance.vector_func_hash, &funcid, HASH_FIND, &found);
    if (found && entry && idx != -1 && entry->vec_fn_cache[idx]) {
        scratch->d.func.vec_fn_addr = entry->vec_fn_cache[idx];
    }
    else {
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmodule(MOD_VEC_EXECUTOR), 
			errmsg("UnSupported vector function id:%d, line=%d, func:%s", funcid, __LINE__, __func__)));
    }

	*scratch->resvector = resvector;
	scratch->d.func.fcinfo = fcinfo;
	scratch->d.func.finfo = finfo;
	scratch->d.func.nargs = argno;
}

VecExprState* ExecInitQualVectorExpr(List *qual, PlanState *parent)
{
	VecExprState  *state;
	VecExprEvalStep scratch;
	List	   *adjust_jumps = NIL;
	ListCell   *lc;
	int32 nquals, firstqual, lastqual;

	if (qual == NIL)
		return NULL;

	state = makeNode(VecExprState);
	state->expr = (Expr*)qual;
	state->parent = parent;
	state->resultvector = New(CurrentMemoryContext) ColumnVectorUint8(CurrentMemoryContext);

	nquals = 0;
	scratch.opcode = EEOP_VEC_QUAL;
	foreach(lc, qual) {
		Expr *node = (Expr*)lfirst(lc);
		CVector** resultvector = (CVector**)palloc(sizeof(CVector**));
		*resultvector = NULL;

		/* no create vector, use opresult or filter as resultvector */
		ExecInitVecExprRec(node, state, resultvector);

		scratch.resvector = resultvector;
		scratch.d.qual.jumpdone = -1;
		VecExprEvalPushStep(state, &scratch);

		adjust_jumps = lappend_int(adjust_jumps, state->steps_len - 1);
		nquals++;
	}

	firstqual = linitial_int(adjust_jumps);
	lastqual = llast_int(adjust_jumps);

	if (nquals > 1) {
		state->filter = New(CurrentMemoryContext) ColumnVectorUint8(CurrentMemoryContext);

		foreach(lc, adjust_jumps) {
			VecExprEvalStep *as = &state->steps[lfirst_int(lc)];
			Assert(as->opcode == EEOP_VEC_QUAL);
			Assert(as->d.qual.jumpdone == -1);
			as->d.qual.jumpdone = state->steps_len;

			if (firstqual == lfirst_int(lc))
				as->d.qual.type = VEC_QUAL_CHECK;
			else {
				if (lastqual == lfirst_int(lc))
					as->d.qual.type = VEC_QUAL_CHECK_MERGE_END;
				else
					as->d.qual.type = VEC_QUAL_CHECK_MERGE;
			}
		}
	}
	else {
		/* only one */
		VecExprEvalStep *as = &state->steps[firstqual];
		Assert(as->opcode == EEOP_VEC_QUAL);
		Assert(as->d.qual.jumpdone == -1);
		as->d.qual.jumpdone = state->steps_len;
		as->d.qual.type = VEC_QUAL_DIRECT_END;
	}

	scratch.opcode = EEOP_VEC_DONE;
	VecExprEvalPushStep(state, &scratch);

	ExecReadyVecExpr(state);

	return state;
}

VecQualResult* CreateQualResult() {
	VecQualResult* result = (VecQualResult*)palloc(sizeof(VecQualResult) + sizeof(uint8) * (ColumnVectorSize / 8));
	result->masks = (uint64*)((uint8*)result + sizeof(VecQualResult));
	return result;
}

VecExprState* ExecInitProjVectorExpr(List *targetList, TupleTableSlot* slot, PlanState *parent, TupleDesc inputDesc) {
	VecExprState  *state;
	VecExprEvalStep scratch;
	ListCell   *lc;

	if (targetList == NIL)
		return NULL;

	state = makeNode(VecExprState);
	state->expr = (Expr*)targetList;
	state->parent = parent;
    state->resultbatch = New(CurrentMemoryContext) BatchVector(CurrentMemoryContext, slot->tts_tupleDescriptor);

    foreach (lc, targetList) {
		TargetEntry *tle = lfirst_node(TargetEntry, lc);
		Var* variable = (Var*)tle->expr;
		bool isSimpleVar = false;

		if (variable && IsA(variable, Var) && variable->varattno > 0) {
            if (!inputDesc)
                isSimpleVar = true; /* can't check type, assume OK */
            else if (variable->varattno <= inputDesc->natts) {
                Form_pg_attribute attr;
                attr = &inputDesc->attrs[variable->varattno - 1];
                if (!attr->attisdropped && variable->vartype == attr->atttypid)
                    isSimpleVar = true;
            }
		}

        if (isSimpleVar) {		
			switch (variable->varno) {
				case INNER_VAR:
					scratch.opcode = EEOP_VEC_PROJ_INNER_VECTOR;
					break;
				case OUTER_VAR:
					scratch.opcode = EEOP_VEC_PROJ_OUTER_VECTOR;
					break;
				default:
					scratch.opcode = EEOP_VEC_PROJ_SCAN_VECTOR;
					break;
			}
			
			scratch.d.proj_vector.attnum = variable->varattno - 1;
            scratch.d.proj_vector.resultnum = tle->resno - 1;
			VecExprEvalPushStep(state, &scratch);
		}
        else {
			CVector** resultvector = (CVector**)palloc(sizeof(CVector**));
			*resultvector = NULL;

            ExecInitVecExprRec(tle->expr, state, resultvector);

			scratch.resvector = resultvector;
			scratch.opcode = EEOP_VEC_PROJ_VECTOR_TMP;
			scratch.d.proj_vector_tmp.resultnum = tle->resno - 1;
			VecExprEvalPushStep(state, &scratch);
        }
    }

	scratch.opcode = EEOP_VEC_DONE;
	VecExprEvalPushStep(state, &scratch);

	ExecReadyVecExpr(state);

	return state;
}

ProjectionInfo* ExecBuildVectorProjectionInfo(
    List* targetList, List* qual, VecExprContext* econtext, TupleTableSlot* slot, PlanState *parent, TupleDesc inputDesc)
{
	ProjectionInfo* projInfo;

	if (targetList == NIL)
		return NULL;

	projInfo = makeNode(ProjectionInfo);
    projInfo->pi_VecExprContext = econtext;

	if (list_length(qual))
		projInfo->QualResult = CreateQualResult();

    GetAccessedVarNumbers(projInfo, targetList, qual);

	projInfo->VecState = ExecInitProjVectorExpr(targetList, slot, parent, inputDesc);
	
    return projInfo;
}

ProjectionInfo* ExecBuildStreamVectorProjectionInfo(
    List* targetList, List* qual, VecExprContext* econtext, TupleTableSlot* slot, PlanState *parent, TupleDesc inputDesc)
{
	ProjectionInfo* projInfo;

	if (targetList == NIL)
		return NULL;

	projInfo = makeNode(ProjectionInfo);
    projInfo->pi_VecExprContext = econtext;

	if (list_length(qual))
		projInfo->QualResult = CreateQualResult();

    GetAccessedVarNumbers(projInfo, targetList, qual);

    return projInfo;
}

Bitmapset* CreateAssignColumnBitmap(List* AccessedVar, TupleDesc desc) 
{
	ListCell *lc;
	Bitmapset* acb = NULL;
	int nattr = desc->natts;
	int idx;

	foreach (lc, AccessedVar) {
		idx = lfirst_int(lc) - 1;
		if (idx < nattr)
			acb = bms_add_member(acb, idx);
	}

	return acb;
}

static void VecExecBuildAggTransCall(VecExprState *state, VecAggState *aggstate, VecExprEvalStep *scratch,
    VecFunctionCallInfo fcinfo, VecAggStatePerTrans pertrans, int transno, int setno, int setoff,
    bool ishash, bool iscollect)
{
    if (pertrans->numSortCols == 0) {
		if (pertrans->transtypeByVal) {
			if (iscollect)
				scratch->opcode = EEOP_VEC_AGG_COLLECT_PLAIN_TRANS_BYVAL;
			else
				scratch->opcode = EEOP_VEC_AGG_PLAIN_TRANS_BYVAL;
		} else {
			if (iscollect)
				scratch->opcode = EEOP_VEC_AGG_COLLECT_PLAIN_TRANS_BYREF;
			else
				scratch->opcode = EEOP_VEC_AGG_PLAIN_TRANS_BYREF;
		}
    } 
	else if (pertrans->numInputs == 1) {
		scratch->opcode = EEOP_VEC_AGG_ORDERED_TRANS_DATUM;
    } 
	else {
        scratch->opcode = EEOP_VEC_AGG_ORDERED_TRANS_TUPLE;
	}

    scratch->d.agg_trans.pertrans = pertrans;
    scratch->d.agg_trans.setno = setno;
    scratch->d.agg_trans.setoff = setoff;
    scratch->d.agg_trans.transno = transno;
    scratch->d.agg_trans.aggcontext = NULL;
    VecExprEvalPushStep(state, scratch);
}

VecExprState* ExecBuildVecAggTrans(VecAggState* aggstate, AggStatePerPhase phase, bool doSort, bool doHash)
{
	VecExprState  *state;
	VecExprEvalStep scratch;
    int transno = 0;
    int setoff = 0;

	state = makeNode(VecExprState);
    state->expr = (Expr*)aggstate;
	state->parent = &aggstate->ss.ps;
	state->resultvector = NULL;
    aggstate->vecaggstate = state;

    for (transno = 0; transno < aggstate->numtrans; transno++) {
        VecAggStatePerTrans pertrans = &aggstate->pervectrans[transno];
        int argno;
        int setno;
        bool isCollect = ((pertrans->aggref->aggstage > 0 || aggstate->is_final) &&
                          need_adjust_agg_inner_func_type(pertrans->aggref) && pertrans->numSortCols == 0);
        VecFunctionCallInfo trans_fcinfo = &pertrans->vec_transfn_fcinfo;
        VecFunctionCallInfo collect_fcinfo = &pertrans->vec_collectfn_fcinfo;
        ListCell *arg;

        /*
         * Evaluate arguments to aggregate/combine function.
         */
        argno = 0;

        if (isCollect) {
            /*
             * like Normal transition function below
             */
            foreach (arg, pertrans->aggref->args) {
                TargetEntry *source_tle = (TargetEntry *)lfirst(arg);

                /*
                 * Start from 1, since the 0th arg will be the transition
                 * value
                 */
                ExecInitVecExprRec(source_tle->expr, state, &collect_fcinfo->vec[0]);
                argno++;
            }
        } else if (pertrans->numSortCols == 0) {
            /*
             * Normal transition function without ORDER BY / DISTINCT.
             */
            foreach (arg, pertrans->aggref->args) {
                TargetEntry *source_tle = (TargetEntry *)lfirst(arg);

                /*
                 * Start from 1, since the 0th arg will be the transition
                 * value
                 */
                ExecInitVecExprRec(source_tle->expr, state, &trans_fcinfo->vec[0]);
                argno++;
            }
        } else if (pertrans->numInputs == 1) {
            /*
             * DISTINCT and/or ORDER BY case, with a single column sorted on.
             */

			TargetEntry *source_tle = (TargetEntry *)linitial(pertrans->aggref->args);

            Assert(list_length(pertrans->aggref->args) == 1);

            ExecInitVecExprRec(source_tle->expr, state, &trans_fcinfo->vec[0]);
        } else {
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED), errmodule(MOD_VEC_EXECUTOR), errmsg("need supported!")));
        }

        /*
         * Call transition function (once for each concurrently evaluated
         * grouping set). Do so for both sort and hash based computations, as
         * applicable.
         */
        setoff = 0;
        if (doSort) {
            int processGroupingSets = Max(phase->numsets, 1);

			if (pertrans->numDistinctCols)
				continue;
				
            for (setno = 0; setno < processGroupingSets; setno++) {
                VecExecBuildAggTransCall(state, aggstate, &scratch, isCollect ? collect_fcinfo : trans_fcinfo, pertrans,
                                      transno, setno, setoff, false, isCollect);
                setoff++;
            }
        }

        if (doHash) {
            int numHashes = aggstate->num_hashes;

            /* in MIXED mode, there'll be preceding transition values */
            if (aggstate->aggstrategy != AGG_HASHED)
                setoff = aggstate->maxsets;
            else
                setoff = 0;

            for (setno = 0; setno < numHashes; setno++) {
                VecExecBuildAggTransCall(state, aggstate, &scratch, isCollect ? collect_fcinfo : trans_fcinfo, pertrans,
                                      transno, setno, setoff, true, isCollect);
                setoff++;
            }
        }
    }

    scratch.opcode = EEOP_VEC_DONE;
    VecExprEvalPushStep(state, &scratch);
	
    ExecReadyVecExpr(state);

    return state;
}

VecExprState* ExecBuildVecWinAgg(VecWindowAggState* winstate)
{
	VecExprState  *state;
	VecExprEvalStep scratch;
    int transno = 0;
    WindowStatePerFunc per_func_state;
	ListCell *l;
	VecFunctionCallInfo trans_fcinfo;

	state = makeNode(VecExprState);
    state->expr = (Expr*)winstate;
	state->parent = &winstate->ss.ps;
	state->resultvector = NULL;
    winstate->vec_evaltrans = state;

    for (transno = 0; transno < winstate->numaggs; transno++) {
        per_func_state = &(winstate->perfunc[transno]);
        int argno;
        trans_fcinfo = &(winstate->windowAggInfo[transno].vec_agg_function);

        argno = 0;

        foreach (l, per_func_state->wfunc->args) {
            ExecInitVecExprRec((Expr *)lfirst(l), state, &trans_fcinfo->vec[0]);
            argno++;
        }

        if (per_func_state->resulttypeByVal) {
            scratch.opcode = EEOP_VEC_WINAGG_PLAIN_TRANS_BYVAL;
        } else {
            scratch.opcode = EEOP_VEC_WINAGG_PLAIN_TRANS_BYREF;
        }

        scratch.d.winagg_trans.perfunc = per_func_state;
        scratch.d.winagg_trans.transno = transno;
        scratch.d.winagg_trans.aggcontext = NULL;
        scratch.d.winagg_trans.trans_fcinfo = trans_fcinfo;
        VecExprEvalPushStep(state, &scratch);
    }

    scratch.opcode = EEOP_VEC_DONE;
    VecExprEvalPushStep(state, &scratch);
    ExecReadyVecExpr(state);

    return state;
}