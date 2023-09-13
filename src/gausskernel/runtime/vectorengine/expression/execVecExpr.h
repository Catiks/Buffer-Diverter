#ifndef EXEC_VEC_EXPR_H
#define EXEC_VEC_EXPR_H

#include "vecexecutor/columnvector.h"
#include "vecexecutor/vecnodes.h"
#include "nodes/execnodes.h"

#define VEC_QUAL_DIRECT_END			1
#define VEC_QUAL_CHECK				2
#define VEC_QUAL_CHECK_MERGE		3
#define VEC_QUAL_CHECK_MERGE_END	4

typedef enum VecExprEvalOp 
{
	/* entire expression has been evaluated completely, return */
	EEOP_VEC_DONE,

	EEOP_VEC_INNER_VECTOR,
	EEOP_VEC_OUTER_VECTOR,
	EEOP_VEC_SCAN_VECTOR,

	EEOP_VEC_PROJ_INNER_VECTOR,
	EEOP_VEC_PROJ_OUTER_VECTOR,
	EEOP_VEC_PROJ_SCAN_VECTOR,
	EEOP_VEC_PROJ_VECTOR_TMP,

	EEOP_VEC_CONST,

	EEOP_VEC_FUNCEXPR,

	EEOP_VEC_QUAL,

	EEOP_VEC_AGGREF,
    EEOP_VEC_AGG_PLAIN_TRANS_BYVAL,
	EEOP_VEC_AGG_COLLECT_PLAIN_TRANS_BYVAL,
	EEOP_VEC_AGG_PLAIN_TRANS_BYREF,
	EEOP_VEC_AGG_COLLECT_PLAIN_TRANS_BYREF,

	/* non-existent operation, used e.g. to check array lengths */
	EEOP_VEC_LAST
} VecExprEvalOp;

typedef struct VecExprEvalStep
{
	intptr_t opcode;

	CVector **resvector;

	union
	{
		struct {
			int attnum;
		} var_vector;

		struct {
			int resultnum;
			int attnum;
		} proj_vector;

		struct {
			int resultnum;
		} proj_vector_tmp;

		struct {
			uint32				nargs;
			FmgrInfo			*finfo;
			VecFunctionCallInfo fcinfo;
			VecFunction			vec_fn_addr;
		} func;

		struct {
			short type;
			short jumpdone;
		} qual;

		struct {
			short jumpdone;
		} jump;

		struct {
			AggrefExprState *astate;
		} aggref;
		
        struct {
            VecAggStatePerTrans pertrans;
            MemoryContext aggcontext;
            int setno;
            int transno;
            int setoff;
            VecFunction vec_fn_addr;
        } agg_trans;
	} d;
	
} VecExprEvalStep;

/* functions in execExprInterp.c */
extern void ExecReadyInterpretedVecExpr(VecExprState *state);
extern VecExprEvalOp ExecEvalStepOp(VecExprState *state, VecExprEvalStep *op);
extern BatchVector* ExecVectorProject(ProjectionInfo* projInfo, bool selReSet);
extern void GetAccessedVarNumbers(ProjectionInfo* projInfo, List* targetlist, List* qual);
extern Bitmapset* CreateAssignColumnBitmap(List* AccessedVar, TupleDesc tupledesc);
extern VecExprState* ExecBuildVecAggTrans(VecAggState* aggstate, AggStatePerPhase phase, bool doSort, bool doHash);
extern VecQualResult* CreateQualResult();

#endif /* EXEC_VEC_EXPR_H */
