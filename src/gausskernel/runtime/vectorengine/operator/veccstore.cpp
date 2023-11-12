/* -------------------------------------------------------------------------
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2016, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * veccstore.cpp
 *    Support routines for sequential scans of column stores.
 *
 * IDENTIFICATION
 *        Code/src/gausskernel/runtime/vecexecutor/vecnode/veccstore.cpp
 *
 * -------------------------------------------------------------------------
 */

/*
 * INTERFACE ROUTINES
 *      ExecCStoreScan              sequentially scans a column store.
 *      ExecCStoreNext              retrieve next tuple in sequential order.
 *      ExecInitCStoreScan          creates and initializes a cstorescan node.
 *      ExecEndCStoreScan           releases any storage allocated.
 *      ExecReScanCStoreScan        rescans the column store
 */
#include "codegen/gscodegen.h"
#include "codegen/vecexprcodegen.h"
#include "postgres.h"
#include "knl/knl_variable.h"

#include "access/relscan.h"
#include "access/tableam.h"
#include "executor/exec/execdebug.h"
#include "vecexecutor/vecnodecstorescan.h"
#include "vecexecutor/vecnoderowtovector.h"
#include "vecexecutor/columnvector.h"
#include "vecexecutor/vecexecutor.h"
#include "vecexecutor/execVecExpr.h"
#include "executor/node/nodeModifyTable.h"
#include "executor/node/nodeSeqscan.h"
#include "storage/cstore/cstore_compress.h"
#include "access/cstore_am.h"
#include "optimizer/clauses.h"
#include "nodes/params.h"
#include "utils/lsyscache.h"
#include "utils/datum.h"
#include "utils/rel.h"
#include "utils/rel_gs.h"
#include "utils/syscache.h"
#include "executor/node/nodeSamplescan.h"
#include "executor/node/nodeSeqscan.h"
#include "access/cstoreskey.h"
#include "catalog/pg_cast.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_proc.h"
#include "access/genam.h"
#include "access/heapam.h"
#include "access/sysattr.h"
#include "catalog/indexing.h"
#include "access/heapam.h"
#include "utils/fmgroids.h"
#include "utils/snapmgr.h"
#include "catalog/pg_partition_fn.h"
#include "pgxc/redistrib.h"
#include "optimizer/pruning.h"

extern bool CodeGenThreadObjectReady();
extern bool CodeGenPassThreshold(double rows, int dn_num, int dop);

static CStoreStrategyNumber GetCStoreScanStrategyNumber(Oid opno);
static Datum GetParamExternConstValue(Oid left_type, Expr* expr, PlanState* ps, uint16* flag);
static void ExecInitNextPartitionForCStoreScan(CStoreScanState* node);
static void ExecCStoreBuildScanKeys(CStoreScanState* scan_stat, List* quals, CStoreScanKey* scan_keys, int* num_scan_keys,
    CStoreScanRunTimeKeyInfo** runtime_key_info, int* runtime_keys_num);
static void ExecCStoreScanEvalRuntimeKeys(
    ExprContext* expr_ctx, CStoreScanRunTimeKeyInfo* runtime_keys, int num_runtime_keys);

/* the same to CStore::SetTiming() */
#define TIMING_VECCSTORE_SCAN(_node) (NULL != (_node)->ps.instrument && (_node)->ps.instrument->need_timer)

#define VECCSTORE_SCAN_TRACE_START(_node, _desc_id)                  \
    do {                                                             \
        if (unlikely(TIMING_VECCSTORE_SCAN(_node))) {                \
            TRACK_START((_node)->ps.plan->plan_node_id, (_desc_id)); \
        }                                                            \
    } while (0)

#define VECCSTORE_SCAN_TRACE_END(_node, _desc_id)                  \
    do {                                                           \
        if (unlikely(TIMING_VECCSTORE_SCAN(_node))) {              \
            TRACK_END((_node)->ps.plan->plan_node_id, (_desc_id)); \
        }                                                          \
    } while (0)

/*
 * type conversion for cu min/max filter
 * see also: cstore_roughcheck_func.cpp
 */
static FORCE_INLINE Datum convert_scan_key_int64_if_need(Oid left_type, Oid right_type, Datum right_value)
{
    Datum v = right_value;
    switch (right_type) {
        /* int family to INT64 */
        case INT4OID: {
            v = Int64GetDatum((int64)DatumGetInt32(right_value));
            break;
        }
        case INT2OID: {
            v = Int64GetDatum((int64)DatumGetInt16(right_value));
            break;
        }
        case INT1OID: {
            v = Int64GetDatum((int64)DatumGetChar(right_value));
            break;
        }
        case DATEOID:
        case TIMEOID:
        case TIMESTAMPOID:
        case TIMESTAMPTZOID: {
            /* date, time etc. make right type same as left */
            if (right_type != left_type) {
                /* get type conversion function */
                HeapTuple cast_tuple =
                    SearchSysCache2(CASTSOURCETARGET, ObjectIdGetDatum(right_type), ObjectIdGetDatum(left_type));

                if (!HeapTupleIsValid(cast_tuple))
                    ereport(ERROR,
                        (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                            errmsg("can not cast from type %s to type %s ",
                                format_type_be(right_type),
                                format_type_be(left_type))));

                Form_pg_cast cast_form = (Form_pg_cast)GETSTRUCT(cast_tuple);
                Oid func_id = cast_form->castfunc;
                char cast_method = cast_form->castmethod;

                ReleaseSysCache(cast_tuple);

                if (!OidIsValid(func_id) || cast_method != COERCION_METHOD_FUNCTION)
                    break;

                /* check function args */
                HeapTuple proc_tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(func_id));

                if (!HeapTupleIsValid(proc_tuple))
                    ereport(ERROR,
                        (errmodule(MOD_VEC_EXECUTOR),
                            errcode(ERRCODE_CACHE_LOOKUP_FAILED),
                            errmsg("cache lookup failed for function %u", func_id)));

                Form_pg_proc proc_form = (Form_pg_proc)GETSTRUCT(proc_tuple);
                int proc_nargs = proc_form->pronargs;

                ReleaseSysCache(proc_tuple);

                /* execute type conversion */
                if (proc_nargs == 1)
                    v = OidFunctionCall1(func_id, right_value);
            }
            break;
        }
        default:
            break;
    }
    return v;
}

void OptimizeProjectionAndFilter(CStoreScanState* node)
{
    ProjectionInfo* proj = NULL;
    bool simple_map = false;

    proj = node->ps.ps_ProjInfo;

    // Check if it is simple without need to invoke projection code
    //
    simple_map = proj->pi_directMap && (node->m_pCurrentBatch->m_cols == proj->pi_numSimpleVars);

    node->m_fSimpleMap = simple_map;
}

void ApplyProjectionAndFilter(CStoreScanState* node)
{

    
    return;
}

TupleDesc BuildTupleDescByTargetList(List* tlist)
{
    ListCell* lc = NULL;
    TupleDesc desc = CreateTemplateTupleDesc(list_length(tlist), false);
    int att_num = 1;
    foreach (lc, tlist) {
        TargetEntry* tle = (TargetEntry*)lfirst(lc);
        Var* var = (Var*)tle->expr;
        TupleDescInitEntry(desc, (AttrNumber)att_num, tle->resname, var->vartype, var->vartypmod, 0);
        att_num++;
    }

    return desc;
}

/* ----------------------------------------------------------------
 *      ExecCStoreScan(node)
 *
 *      Scans the relation sequentially and returns the next qualifying
 *      tuple.
 *      We call the ExecScan() routine and pass it the appropriate
 *      access method functions.
 * ----------------------------------------------------------------
 */
VectorBatch* ExecCStoreScan(CStoreScanState* node)
{
    ProjectionInfo* proj = node->ps.ps_ProjInfo;
    BatchVector* outputbatch;
    BatchVector* packbatch;
    BatchVector* scanbatch = node->m_scanBatch;
    VecExprContext* econtext = node->ps.ps_VecExprContext;
    ColumnVectorUint8* filter;
    uint32 af_rows;
    uint8 pos;
    bool nodop = node->ps.plan->dop == 1;
    uint32 scanstatus_project;

    if (nodop) {
        packbatch = node->m_packBatch;
        scanstatus_project =  SCANSTATUS_PROJECT;
    }
    else {
        packbatch = node->m_packBatchManager->GetCurrentBatch();
        scanstatus_project =  SCANSTATUS_STREAM_PROJECT;
    }

    while (true) {
        switch (node->ScanStatus) {
            case SCANSTATUS_FETCH: {
                Assert(node->ScanBatchStatus == SCANBATCH_STATUS_EMPTY);
                scanbatch->Reset();

                /* RefreshCursor is in CStoreScan, unpin's last data */
                node->m_CStore->CStoreScan(node, scanbatch);
                if (unlikely(scanbatch->Empty())) {
                    if (!packbatch->Empty()) {
                        ResetExprContext(econtext);
                        econtext->align_rows = packbatch->Rows();
                        econtext->ecxt_scanbatch = packbatch;
                        node->ScanStatus = scanstatus_project;
                        break;
                    }
                    else {
                        node->m_CStore->RefreshCursor();
                        node->ScanStatus = SCANSTATUS_END;
                        return NULL;
                    }
                }
                node->ScanBatchStatus = SCANBATCH_STATUS_FULL;
                node->ScanStatus = SCANSTATUS_QUAL;
            }

            case SCANSTATUS_QUAL: {
                ResetExprContext(econtext);
                econtext->align_rows = scanbatch->Rows();
                initEcontextBatch(scanbatch, NULL, NULL, NULL);
                Assert(econtext->align_rows);

                if (node->ps.qual) {
                    filter = ExecVectorQual((VecExprState*)node->ps.qual, econtext);
                    af_rows = filter->GetFilterRowsSaveResult(proj->QualResult);
                    if (af_rows) {
                        /* fill late read data */
                        node->m_CStore->FillVecBatch<true>(scanbatch);

                        /* selection ratio is 100% */
                        if (af_rows == scanbatch->Rows()) {
                            /* goto ExecVectorProject, no need pack
                               we can change the order, because we 
                               don't have a sort of lower level */
                            ResetExprContext(econtext);
                            node->ScanStatus = scanstatus_project;
                        }
                        else {
                            /* pack from the head of scan */
                            node->ScanBatchPos = 0;
                            node->ScanStatus = SCANSTATUS_PACK;
                        }
                    }
                    else {
                        /* selection ratio is 0%, fetch new data */
                        node->ScanBatchStatus = SCANBATCH_STATUS_EMPTY;
                        node->ScanStatus = SCANSTATUS_FETCH;
                    }
                }
                else {
                    /* goto ExecVectorProject, No need Pack */
                    node->ScanStatus = scanstatus_project;
                }

                break;
            }

            case SCANSTATUS_PACK: {
                Assert(scanbatch->Rows());
                ExecVectorPack(packbatch, scanbatch, proj->pi_scanTargetlistVar, proj->QualResult, &node->ScanBatchPos);
                
                if (!scanbatch->Empty()) {
                    Assert(packbatch->Rows() == ColumnVectorSize);
                    Assert(!scanbatch->Empty());

                    ResetExprContext(econtext);
                    econtext->align_rows = ColumnVectorSize;
                    econtext->ecxt_scanbatch = packbatch;
                    node->ScanBatchStatus = SCANBATCH_STATUS_LEFT;
                    node->ScanStatus = scanstatus_project;
                }
                else {
                    /* scanbatch all of data put packbatch, continue scan */
                    node->ScanBatchStatus = SCANBATCH_STATUS_EMPTY;
                    node->ScanStatus = SCANSTATUS_FETCH;
                }

                break;
            }

            case SCANSTATUS_PROJECT: {
                outputbatch = ExecVectorProject(proj->VecState, econtext);
                if (likely(!outputbatch->Empty())) {
                    if (econtext->ecxt_scanbatch == packbatch)
                        packbatch->Reset();
                    else
                        node->ScanBatchStatus = SCANBATCH_STATUS_EMPTY;

                    if (node->ScanBatchStatus == SCANBATCH_STATUS_LEFT) {
                        /* scanbatch have left data, continue pack */
                        node->ScanStatus = SCANSTATUS_PACK;
                    }
                    else {
                        /* SCANBATCH_STATUS_FULL is ok */
                        node->ScanStatus = SCANSTATUS_FETCH;
                    }
                    return (VectorBatch*)outputbatch;
                }
                Assert(false);

                break;
            }

            case SCANSTATUS_STREAM_PROJECT: {
                pos = node->m_packBatchManager->GetPointor();
                outputbatch = ExecVectorProject(proj->VecStateQueue[pos], econtext);
                if (likely(!outputbatch->Empty())) {
                    if (econtext->ecxt_scanbatch == packbatch)
                        packbatch->Reset();
                    else
                        node->ScanBatchStatus = SCANBATCH_STATUS_EMPTY;

                    if (node->ScanBatchStatus == SCANBATCH_STATUS_LEFT) {
                        /* scanbatch have left data, continue pack */
                        node->ScanStatus = SCANSTATUS_PACK;
                    }
                    else {
                        /* SCANBATCH_STATUS_FULL is ok */
                        node->ScanStatus = SCANSTATUS_FETCH;
                    }
                    node->m_packBatchManager->SwitchBatch();
                    return (VectorBatch*)outputbatch;
                }
                Assert(false);

                break;
            }

            default:
                return NULL;
        }
    }

    Assert(false);
    return (VectorBatch*)outputbatch;
}

void ReScanDeltaRelation(CStoreScanState* node)
{
    if (node->ss_currentDeltaScanDesc) {
        tableam_scan_rescan((TableScanDesc)(node->ss_currentDeltaScanDesc), NULL);
    }
    node->ss_deltaScan = false;
    node->ss_deltaScanEnd = false;
}

void InitCStoreRelation(CStoreScanState* node, EState* estate, bool idx_flag, Relation parent_rel)
{
    Relation curr_rel;
    CStoreScan* plan = (CStoreScan*)node->ps.plan;
    curr_rel = ExecOpenScanRelation(estate, plan->scanrelid);
    node->ss_currentRelation = curr_rel;
    ExecAssignScanType(node, RelationGetDescr(curr_rel));
}

void InitScanDeltaRelation(CStoreScanState* node, EState* estate)
{
    Relation delta_rel;
    TableScanDesc delta_scan_desc;
    Relation cstore_rel = node->ss_currentRelation;

    if (node->ss_currentRelation == NULL)
        return;

    delta_rel = heap_open(cstore_rel->rd_rel->reldeltarelid, AccessShareLock);
    delta_scan_desc = tableam_scan_begin(delta_rel, estate->es_snapshot, 0, NULL);

    node->ss_currentDeltaRelation = delta_rel;
    node->ss_currentDeltaScanDesc = delta_scan_desc;
    node->ss_deltaScanEnd = false;
}

/* ----------------------------------------------------------------
 *      ExecInitCStoreScan
 * ----------------------------------------------------------------
 */
CStoreScanState* ExecInitCStoreScan(
    CStoreScan* node, Relation parent_heap_rel, EState* estate, int eflags, bool idx_flag, bool codegen_in_up_level)
{
    CStoreScanState* scan_stat = NULL;
    PlanState* plan_stat = NULL;
    Bitmapset* scan_acb;
    Bitmapset* pack_acb;

    scan_stat = makeNode(CStoreScanState);
    scan_stat->ps.plan = (Plan*)node;
    scan_stat->ps.state = estate;
    scan_stat->ps.vectorized = true;
    plan_stat = &scan_stat->ps;

    ExecAssignVecExprContext(estate, plan_stat);

    ExecInitResultTupleSlot(estate, plan_stat);
    ExecInitScanTupleSlot(estate, (ScanState*)scan_stat);

    scan_stat->ps.qual = (List*)ExecInitQualVectorExpr(scan_stat->ps.plan->qual, &scan_stat->ps);

    InitCStoreRelation(scan_stat, estate, idx_flag, parent_heap_rel);

    ExecAssignResultTypeFromTL(plan_stat, scan_stat->ss_ScanTupleSlot->tts_tupleDescriptor->td_tam_ops);

    if (node->plan.dop == 1) {
        /* for exec GetAccessedVarNumbers */
        plan_stat->ps_ProjInfo = ExecBuildVectorProjectionInfo(node->plan.targetlist,
            node->plan.qual,
            plan_stat->ps_VecExprContext,
            plan_stat->ps_ResultTupleSlot,
            plan_stat,
            scan_stat->ss_ScanTupleSlot->tts_tupleDescriptor);
    }
    else {
        /* for exec GetAccessedVarNumbers */
        plan_stat->ps_ProjInfo = ExecBuildStreamVectorProjectionInfo(node->plan.targetlist,
            node->plan.qual,
            plan_stat->ps_VecExprContext,
            plan_stat->ps_ResultTupleSlot,
            plan_stat,
            scan_stat->ss_ScanTupleSlot->tts_tupleDescriptor);
    }

    scan_acb = CreateAssignColumnBitmap(plan_stat->ps_ProjInfo->pi_accessedVar, scan_stat->ss_currentRelation->rd_att);
    pack_acb = CreateAssignColumnBitmap(plan_stat->ps_ProjInfo->pi_scanTargetlistVar, scan_stat->ss_currentRelation->rd_att);
    Assert(scan_acb);
    Assert(pack_acb);

    scan_stat->m_scanBatch = New(CurrentMemoryContext) BatchVector(CurrentMemoryContext, scan_stat->ss_currentRelation->rd_att, scan_acb);

    if (node->plan.dop == 1) {
        scan_stat->m_packBatch = New(CurrentMemoryContext) BatchVector(CurrentMemoryContext, scan_stat->ss_currentRelation->rd_att, pack_acb);
        scan_stat->m_packBatch->Init();
    }
    else {
        scan_stat->m_packBatchManager = BatchManager::CreateBatchManager(scan_stat->ss_currentRelation->rd_att, pack_acb);
        plan_stat->ps_ProjInfo->VecStateQueue = scan_stat->m_packBatchManager->CreateProjVectorExpr(node->plan.targetlist, 
            plan_stat->ps_ResultTupleSlot, 
            plan_stat,
            scan_stat->ss_ScanTupleSlot->tts_tupleDescriptor);        
    }

    scan_stat->ScanBatchPos = 0;
    scan_stat->ScanBatchStatus = SCANBATCH_STATUS_EMPTY;
    scan_stat->ScanStatus = SCANSTATUS_FETCH;
    scan_stat->m_CStore = New(CurrentMemoryContext) CStore();
    scan_stat->m_CStore->InitScan(scan_stat, GetActiveSnapshot());

    bms_free(scan_acb);
    bms_free(pack_acb);
    return scan_stat;
}

/* ----------------------------------------------------------------
 *      ExecEndCStoreScan
 *
 *      frees any storage allocated through C routines.
 * ----------------------------------------------------------------
 */
void ExecEndCStoreScan(CStoreScanState* node, bool idx_flag)
{
    Relation relation;
    relation = node->ss_currentRelation;
    /*
     * Free the exprcontext
     */
    ExecFreeVecExprContext(&node->ps);

    /*
     * clean out the tuple table
     */
    (void)ExecClearTuple(node->ps.ps_ResultTupleSlot);
    (void)ExecClearTuple(node->ss_ScanTupleSlot);

    /*
     * release CStoreScan
     */
    if (node->m_CStore) {
        DELETE_EX(node->m_CStore);
    }

    /*
     * close the heap relation.
     */
    ExecCloseScanRelation(relation);
}

/* Build the cstore scan keys from the qual. */
static void ExecCStoreBuildScanKeys(CStoreScanState* scan_stat, List* quals, CStoreScanKey* scan_keys, int* num_scan_keys,
    CStoreScanRunTimeKeyInfo** runtime_key_info, int* runtime_keys_num)
{
    ListCell* lc = NULL;
    CStoreScanKey tmp_scan_keys;
    int n_scan_keys;
    int j;
    List* accessed_varnos = NIL;
    CStoreScanRunTimeKeyInfo* runtime_info = NULL;
    int runtime_keys;
    int max_runtime_keys;

    n_scan_keys = list_length(quals);
    // only if the indexFlag is flase and n_scan_keys is zero, we should reinit scan_keys and num_scan_keys
    //
    if (n_scan_keys == 0) {
        return;
    }

    Assert(*scan_keys == NULL);
    Assert(*num_scan_keys == 0);
    tmp_scan_keys = (CStoreScanKey)palloc0(n_scan_keys * sizeof(CStoreScanKeyData));

    accessed_varnos = scan_stat->ps.ps_ProjInfo->pi_accessedVar;

    runtime_info = *runtime_key_info;
    runtime_keys = *runtime_keys_num;
    max_runtime_keys = runtime_keys;
    j = 0;
    foreach (lc, quals) {
        Expr* clause = (Expr*)lfirst(lc);
        CStoreScanKey this_scan_key = &tmp_scan_keys[j++];
        Oid opno;
        RegProcedure opfunc_id;

        Expr* leftop = NULL;
        Expr* rightop = NULL;
        AttrNumber varattno;

        if (IsA(clause, OpExpr)) {
            uint16 flags = 0;  // no use,default 0
            Datum scan_val = 0;
            CStoreStrategyNumber strategy = InvalidCStoreStrategy;
            ListCell* lcell = NULL;
            AttrNumber count_no = 0;
            Oid left_type = InvalidOid;

            opno = ((OpExpr*)clause)->opno;
            opfunc_id = ((OpExpr*)clause)->opfuncid;

            /* Leftop should be var. Has been checked */
            leftop = (Expr*)get_leftop(clause);
            if (leftop && IsA(leftop, RelabelType))
                leftop = ((RelabelType*)leftop)->arg;
            if (leftop == NULL)
                ereport(ERROR,
                    (errmodule(MOD_VEC_EXECUTOR),
                        errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                        errmsg("The left value of the expression should not be NULL")));

            /* The attribute numbers of column in cstore scan is a sequence.begin with 0. */
            varattno = ((Var*)leftop)->varattno;
            foreach (lcell, accessed_varnos) {
                if ((int)varattno == lfirst_int(lcell)) {
                    varattno = count_no;
                    break;
                }
                count_no++;
            }

            left_type = ((Var*)leftop)->vartype;

            /* Rightop should be const.Has been checked */
            rightop = (Expr*)get_rightop(clause);
            if (rightop == NULL)
                ereport(ERROR,
                    (errmodule(MOD_VEC_EXECUTOR),
                        errcode(ERRCODE_UNEXPECTED_NULL_VALUE),
                        errmsg("Fail to get the right value of the expression")));

            if (IsA(rightop, Const)) {
                scan_val =
                    convert_scan_key_int64_if_need(left_type, ((Const*)rightop)->consttype, ((Const*)rightop)->constvalue);
                flags = ((Const*)rightop)->constisnull;
            } else if (IsA(rightop, RelabelType)) {
                rightop = ((RelabelType*)rightop)->arg;
                Assert(rightop != NULL);
                scan_val =
                    convert_scan_key_int64_if_need(left_type, ((Const*)rightop)->consttype, ((Const*)rightop)->constvalue);
                flags = ((Const*)rightop)->constisnull;
            } else if (nodeTag(rightop) == T_Param && ((Param*)rightop)->paramkind == PARAM_EXTERN) {
                scan_val = GetParamExternConstValue(left_type, rightop, &(scan_stat->ps), &flags);
            } else if (nodeTag(rightop) == T_Param) {
                // when the rightop is T_Param, the scan_val will be filled until rescan happens.
                //
                if (runtime_keys >= max_runtime_keys) {
                    if (max_runtime_keys == 0) {
                        max_runtime_keys = 8;
                        runtime_info =
                            (CStoreScanRunTimeKeyInfo*)palloc(max_runtime_keys * sizeof(CStoreScanRunTimeKeyInfo));
                    } else {
                        max_runtime_keys *= 2;
                        runtime_info = (CStoreScanRunTimeKeyInfo*)repalloc(
                            runtime_info, max_runtime_keys * sizeof(CStoreScanRunTimeKeyInfo));
                    }
                }
                runtime_info[runtime_keys].scan_key = this_scan_key;
                runtime_info[runtime_keys].key_expr = ExecInitExpr(rightop, NULL);
                runtime_keys++;
                scan_val = (Datum)0;
            } else
                ereport(ERROR,
                    (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
                        errmsg("Not support pushing  predicate with none-const external param")));
            /* Get strategy number. */
            strategy = GetCStoreScanStrategyNumber(opno);
            /* initialize the scan key's fields appropriately */
            CStoreScanKeyInit(this_scan_key,
                flags,
                varattno,
                strategy,
                ((OpExpr*)clause)->inputcollid,
                opfunc_id,
                scan_val,
                left_type);
        } else {
            pfree_ext(tmp_scan_keys);
            tmp_scan_keys = NULL;
            n_scan_keys = 0;

            break;
        }
    }

    *scan_keys = tmp_scan_keys;
    *num_scan_keys = n_scan_keys;
    *runtime_key_info = runtime_info;
    *runtime_keys_num = runtime_keys;
}

/* No metadata for the operator strategy. The followings are temporary codes.
 */
static CStoreStrategyNumber GetCStoreScanStrategyNumber(Oid opno)
{
    CStoreStrategyNumber strategy_number = InvalidCStoreStrategy;
    Relation hdesc;
    ScanKeyData skey[1];
    SysScanDesc sysscan;
    HeapTuple tuple;
    Form_pg_operator fpo;

    ScanKeyInit(&skey[0], ObjectIdAttributeNumber, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(opno));
    hdesc = heap_open(OperatorRelationId, AccessShareLock);
    sysscan = systable_beginscan(hdesc, OperatorOidIndexId, true, NULL, 1, skey);

    tuple = systable_getnext(sysscan);
    if (!HeapTupleIsValid(tuple))
        ereport(ERROR,
            (errmodule(MOD_VEC_EXECUTOR),
                errcode(ERRCODE_DATA_EXCEPTION),
                errmsg("could not find tuple for operator %u", opno)));

    fpo = (Form_pg_operator)GETSTRUCT(tuple);

    if (strncmp(NameStr(fpo->oprname), "<", NAMEDATALEN) == 0)
        strategy_number = CStoreLessStrategyNumber;
    else if (strncmp(NameStr(fpo->oprname), ">", NAMEDATALEN) == 0)
        strategy_number = CStoreGreaterStrategyNumber;
    else if (strncmp(NameStr(fpo->oprname), "=", NAMEDATALEN) == 0)
        strategy_number = CStoreEqualStrategyNumber;
    else if (strncmp(NameStr(fpo->oprname), ">=", NAMEDATALEN) == 0)
        strategy_number = CStoreGreaterEqualStrategyNumber;
    else if (strncmp(NameStr(fpo->oprname), "<=", NAMEDATALEN) == 0)
        strategy_number = CStoreLessEqualStrategyNumber;
    else
        strategy_number = InvalidCStoreStrategy;

    systable_endscan(sysscan);
    heap_close(hdesc, AccessShareLock);

    return strategy_number;
}

static Datum GetParamExternConstValue(Oid left_type, Expr* expr, PlanState* ps, uint16* flag)
{
    Param* expression = (Param*)expr;
    int param_id = expression->paramid;
    ParamListInfo param_info = ps->state->es_param_list_info;

    /*
     * PARAM_EXTERN parameters must be sought in ecxt_param_list_info.
     */
    if (param_info && param_id > 0 && param_id <= param_info->numParams) {
        ParamExternData* prm = &param_info->params[param_id - 1];
        *flag = prm->isnull;
        if (OidIsValid(prm->ptype)) {
            return convert_scan_key_int64_if_need(left_type, prm->ptype, prm->value);
        }

    } else {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_OBJECT), errmsg("no value found for parameter %d", param_id)));
    }

    return 0;
}

void ExecReCStoreSeqScan(CStoreScanState* node)
{
    node->m_CStore->InitReScan();
}

void ExecCStoreScanEvalRuntimeKeys(ExprContext* expr_ctx, CStoreScanRunTimeKeyInfo* runtime_keys, int num_runtime_keys)
{
    int j;
    MemoryContext old_ctx;

    /* We want to keep the key values in per-tuple memory */
    old_ctx = MemoryContextSwitchTo(expr_ctx->ecxt_per_tuple_memory);

    for (j = 0; j < num_runtime_keys; j++) {
        CStoreScanKey scan_key = runtime_keys[j].scan_key;
        ExprState* key_expr = runtime_keys[j].key_expr;
        Datum scan_val;
        bool is_null = false;

        /*
         * For each run-time key, extract the run-time expression and evaluate
         * it with respect to the current context.	We then stick the result
         * into the proper scan key.
         *
         * Note: the result of the eval could be a pass-by-ref value that's
         * stored in some outer scan's tuple, not in
         * expr_ctx->ecxt_per_tuple_memory.  We assume that the outer tuple
         * will stay put throughout our scan.  If this is wrong, we could copy
         * the result into our context explicitly, but I think that's not
         * necessary.
         *
         * It's also entirely possible that the result of the eval is a
         * toasted value.  In this case we should forcibly detoast it, to
         * avoid repeat detoastings each time the value is examined by an
         * index support function.
         */
        scan_val = ExecEvalExpr(key_expr, expr_ctx, &is_null, NULL);
        if (is_null)
            scan_key->cs_flags |= SK_ISNULL;
        else
            scan_key->cs_flags &= ~SK_ISNULL;

        scan_key->cs_argument =
            convert_scan_key_int64_if_need(scan_key->cs_left_type, ((Param*)key_expr->expr)->paramtype, scan_val);
    }

    (void)MemoryContextSwitchTo(old_ctx);
}

void ExecReSetRuntimeKeys(CStoreScanState* node)
{
    ExprContext* expr_ctx = node->ps.ps_ExprContext;
    ExecCStoreScanEvalRuntimeKeys(expr_ctx, node->m_pScanRunTimeKeys, node->m_ScanRunTimeKeysNum);
}

void ExecReScanCStoreScan(CStoreScanState* node)
{
    TableScanDesc scan;

    if (node->isSampleScan) {
        /* Remember we need to do BeginSampleScan again (if we did it at all) */
        (((ColumnTableSample*)node->sampleScanInfo.tsm_state)->resetVecSampleScan)();
    }

    if (node->m_ScanRunTimeKeysNum != 0) {
        ExecReSetRuntimeKeys(node);
    }
    node->m_ScanRunTimeKeysReady = true;

    scan = (TableScanDesc)(node->ss_currentScanDesc);
    if (node->isPartTbl && !(((Scan *)node->ps.plan)->partition_iterator_elimination)) {
        if (PointerIsValid(node->partitions)) {
            /* end scan the prev partition first, */
            tableam_scan_end(scan);
            EndScanDeltaRelation(node);

            /* finally init Scan for the next partition */
            ExecInitNextPartitionForCStoreScan(node);
        }
    }

    node->ScanBatchPos = 0;
    node->ScanBatchStatus = SCANBATCH_STATUS_EMPTY;
    node->ScanStatus = SCANSTATUS_FETCH;
    
    ExecReCStoreSeqScan(node);
    ReScanDeltaRelation(node);
}

static void ExecInitNextPartitionForCStoreScan(CStoreScanState* node)
{
    Partition curr_part = NULL;
    Relation curr_part_rel = NULL;
    TableScanDesc curr_scan_desc = NULL;
    int param_no = -1;
    ParamExecData* param = NULL;
    CStoreScan* plan = NULL;

    plan = (CStoreScan*)node->ps.plan;

    /* get partition sequnce */
    param_no = plan->plan.paramno;
    param = &(node->ps.state->es_param_exec_vals[param_no]);
    node->currentSlot = (int)param->value;

    /* construct HeapScanDesc for new partition */
    curr_part = (Partition)list_nth(node->partitions, node->currentSlot);
    curr_part_rel = partitionGetRelation(node->ss_partition_parent, curr_part);

    releaseDummyRelation(&(node->ss_currentPartition));
    node->ss_currentPartition = curr_part_rel;
    node->ss_currentRelation = curr_part_rel;

    /* add qual for redis */
    if (u_sess->attr.attr_sql.enable_cluster_resize && RelationInRedistribute(curr_part_rel)) {
        List* new_qual = NIL;

        new_qual = eval_ctid_funcs(curr_part_rel, node->ps.plan->qual, &node->rangeScanInRedis);
        node->ps.qual = (List*)ExecInitVecExpr((Expr*)new_qual, (PlanState*)&node->ps);
    }

    if (!node->isSampleScan) {
        curr_scan_desc = tableam_scan_begin(curr_part_rel, node->ps.state->es_snapshot, 0, NULL);
    } else {
        curr_scan_desc = InitSampleScanDesc((ScanState*)node, curr_part_rel);
    }

    /* update partition scan-related fileds in SeqScanState  */
    node->ss_currentScanDesc = curr_scan_desc;
    node->m_CStore->InitPartReScan(node->ss_currentRelation);

    /* reinit delta scan */
    InitScanDeltaRelation(node, node->ps.state->es_snapshot);
}
