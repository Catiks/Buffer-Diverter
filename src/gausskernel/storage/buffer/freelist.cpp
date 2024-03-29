/* -------------------------------------------------------------------------
 *
 * freelist.cpp
 *	  routines for managing the buffer pool's replacement strategy.
 *
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/storage/buffer/freelist.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"
#include "knl/knl_variable.h"
#include "utils/atomic.h"
#include "access/xlog.h"
#include "storage/buf/buf_internals.h"
#include "storage/buf/bufmgr.h"
#include "storage/proc.h"
#include "postmaster/aiocompleter.h" /* this is for the function AioCompltrIsReady() */
#include "postmaster/bgwriter.h"
#include "postmaster/pagewriter.h"
#include "postmaster/postmaster.h"
#include "access/double_write.h"
#include "gstrace/gstrace_infra.h"
#include "gstrace/storage_gstrace.h"
#include "pgstat.h"

#define INT_ACCESS_ONCE(var) ((int)(*((volatile int *)&(var))))
#ifdef UBRL

#define MAX(A, B) (((B) > (A)) ? (B) : (A))
#define MIN(A, B) (((B) < (A)) ? (B) : (A))


// #define GetStrategyHistoryDesc(strategy, user, id) (&t_thrd.storage_cxt.UserStrategyControl[user].victimHistory[(strategy)][(id)] )
// #define STRATEGY_HIS_HEAD(strategy, user) (t_thrd.storage_cxt.UserStrategyControl[user].BufferInfo[(strategy)].history_head)
// #define STRATEGY_HIS_TAIL(strategy, user) (t_thrd.storage_cxt.UserStrategyControl[user].BufferInfo[(strategy)].history_tail)
// #define BUF_STRATEGY_HIS_NEXT(id, strategy, user) (t_thrd.storage_cxt.UserStrategyControl[user].victimHistory[(strategy)][(id)].next)
// #define BUF_STRATEGY_HIS_PREV(id, strategy, user) (t_thrd.storage_cxt.UserStrategyControl[user].victimHistory[(strategy)][(id)].prev)
// #define BUF_STRATEGY_HIS_TAG(id, strategy, user) (t_thrd.storage_cxt.UserStrategyControl[user].victimHistory[(strategy)][(id)].buf_tag)
// #define BUF_STRATEGY_HIS_HASH(id, strategy, user) (t_thrd.storage_cxt.UserStrategyControl[user].victimHistory[(strategy)][(id)].buf_hash)

#define GetStrategyHistoryDesc(id) (&t_thrd.storage_cxt.BufVictimHistory[(id)])
#define STRATEGY_HIS_HEAD(user) (t_thrd.storage_cxt.UserStrategyControl[user].history_head)
#define STRATEGY_HIS_TAIL(user) (t_thrd.storage_cxt.UserStrategyControl[user].history_tail)
// #define BUF_STRATEGY_HIS_NEXT(id, user) (t_thrd.storage_cxt.BufVictimHistory[(user)][(id)].next)
// #define BUF_STRATEGY_HIS_PREV(id, user) (t_thrd.storage_cxt.BufVictimHistory[(user)][(id)].prev)
// #define BUF_STRATEGY_HIS_TAG(id, user) (t_thrd.storage_cxt.BufVictimHistory[(user)][(id)].buf_tag)
// #define BUF_STRATEGY_HIS_HASH(id, user) (t_thrd.storage_cxt.BufVictimHistory[(user)][(id)].buf_hash)


// #define STRAT_HIS_LIST_REMOVE(node, strategy, user) \
// do { \
// 	if (BUF_STRATEGY_HIS_PREV((node), strategy, (user)) < 0)										\
// 		(STRATEGY_HIS_HEAD(strategy, (user))) = BUF_STRATEGY_HIS_NEXT((node), strategy, (user));	                                \
// 	else														\
// 		BUF_STRATEGY_HIS_NEXT(BUF_STRATEGY_HIS_PREV((node), (strategy), (user)), (strategy), (user)) = BUF_STRATEGY_HIS_NEXT((node), (strategy), (user));  \
// 	if (BUF_STRATEGY_HIS_NEXT((node), strategy, (user)) < 0)										\
// 		(STRATEGY_HIS_TAIL(strategy, (user))) = BUF_STRATEGY_HIS_PREV((node), (strategy), (user));	\
// 	else														\
// 		BUF_STRATEGY_HIS_PREV((BUF_STRATEGY_HIS_NEXT((node), (strategy), (user))), (strategy), (user)) = BUF_STRATEGY_HIS_PREV((node), (strategy), (user));  \
// } while(0)

// #define STRAT_HIS_LIST_PUSH(buf_id, strategy, user) \
// do { \
//     if (STRATEGY_HIS_TAIL(strategy, (user)); < 0) { \
//         STRATEGY_HIS_HEAD(strategy, (user)) = STRATEGY_HIS_TAIL(strategy, (user)) = buf_id; \
//         BUF_STRATEGY_HIS_NEXT(buf_id, (strategy), (user)) = BUF_STRATEGY_HIS_PREV(buf_id, (strategy), (user)) = -1; \
//     } else { \
//         BUF_STRATEGY_HIS_NEXT(buf_id, strategy, (user)) = -1; \
//         BUF_STRATEGY_HIS_PREV(buf_id, strategy, (user)) = STRATEGY_HIS_TAIL(strategy, (user)); \
//         BUF_STRATEGY_HIS_NEXT((STRATEGY_HIS_TAIL(strategy, (user))), (strategy), (user)) = \
//             (buf_id); \
//         (STRATEGY_HIS_TAIL(strategy, (user))) = buf_id; \
//     } \
// } while(0)

/*
 * Macro to remove a CDB from whichever list it currently is on
 */
#define STRATEGY_HEAD(strategy, user) (t_thrd.storage_cxt.UserStrategyControl[(user)].BufferInfo[(strategy)].head)
#define STRATEGY_TAIL(strategy, user) (t_thrd.storage_cxt.UserStrategyControl[(user)].BufferInfo[(strategy)].tail)
#define BUF_STRATEGY_NEXT(buf, strategy) (buf->links[(strategy)].next)
#define BUF_STRATEGY_PREV(buf, strategy) (buf->links[(strategy)].prev)


#define STRAT_LIST_REMOVE(node, strategy, user) \
do { \
	if (BUF_STRATEGY_PREV((node), (strategy)) < 0)										\
		(STRATEGY_HEAD((strategy), (user))) = BUF_STRATEGY_NEXT((node), (strategy));	                                \
	else														\
		BUF_STRATEGY_NEXT(GetUserBufferDescriptor(BUF_STRATEGY_PREV((node), (strategy))), (strategy)) = BUF_STRATEGY_NEXT((node), (strategy));  \
	if (BUF_STRATEGY_NEXT((node), strategy) < 0)										\
		(STRATEGY_TAIL(strategy, (user))) = BUF_STRATEGY_PREV((node), (strategy));	\
	else														\
		BUF_STRATEGY_PREV(GetUserBufferDescriptor(BUF_STRATEGY_NEXT((node), (strategy))), (strategy)) = BUF_STRATEGY_PREV((node), (strategy));  \
    BUF_STRATEGY_NEXT((node), strategy) = -1; \
    BUF_STRATEGY_PREV((node), strategy) = -1; \
} while(0)

/*
 * Macro to add a buf to the tail of a list (MRU position)
 */
#define STRAT_MRU_INSERT(node, strategy, user) \
do { \
	if (STRATEGY_TAIL(strategy, (user)) < 0)						    \
	{															\
		BUF_STRATEGY_PREV(node, strategy) = BUF_STRATEGY_NEXT(node, (strategy)) = -1;							\
		(STRATEGY_HEAD(strategy, (user))) =						\
			STRATEGY_TAIL(strategy, (user)) =					\
			(node)->buf_id;								\
	}															\
	else														\
	{															\
		BUF_STRATEGY_NEXT(node, strategy) = -1;										\
		BUF_STRATEGY_PREV(node, strategy) = (STRATEGY_TAIL(strategy, (user)));			\
		BUF_STRATEGY_NEXT(GetUserBufferDescriptor(STRATEGY_TAIL(strategy, (user))), (strategy)) =		\
			((node)->buf_id);								\
		(STRATEGY_TAIL(strategy,(user))) =						\
			(node)->buf_id;								\
	}															\
} while(0)

/*
 * Macro to add a buf to the head of a list (LRU position)
 */
#define STRAT_LRU_INSERT(node, strategy, user) \
do { \
	if (STRATEGY_HEAD(strategy, (user)) < 0)						    \
	{															        \
		BUF_STRATEGY_PREV((node), (strategy)) = BUF_STRATEGY_NEXT(node, (strategy)) = -1;							\
		(STRATEGY_HEAD((strategy), (user))) =						\
			STRATEGY_TAIL((strategy), (user)) =					\
			(node)->buf_id;								        \
	}															\
	else														\
	{															\
		BUF_STRATEGY_PREV(node, strategy) = -1;									                    	\
		BUF_STRATEGY_NEXT(node, strategy) = (STRATEGY_HEAD(strategy, (user)));		                 	\
		BUF_STRATEGY_PREV(GetUserBufferDescriptor(STRATEGY_HEAD(strategy, (user))), (strategy)) =		\
			((node)->buf_id);								    \
		(STRATEGY_HEAD(strategy, (user))) =						\
			(node)->buf_id;								        \
	}															\
} while(0)

#endif

/*
 * The shared freelist control information.
 */
typedef struct BufferStrategyControl {
    /* Spinlock: protects the values below */
    slock_t buffer_strategy_lock;

    /*
     * Clock sweep hand: index of next buffer to consider grabbing. Note that
     * this isn't a concrete buffer - we only ever increase the value. So, to
     * get an actual buffer, it needs to be used modulo NBuffers.
     */
    pg_atomic_uint32 nextVictimBuffer;

    /*
     * Statistics.	These counters should be wide enough that they can't
     * overflow during a single bgwriter cycle.
     */
    uint32 completePasses;            /* Complete cycles of the clock sweep */
    pg_atomic_uint32 numBufferAllocs; /* Buffers allocated since last reset */

    /*
     * Bgworker process to be notified upon activity or -1 if none. See
     * StrategyNotifyBgWriter.
     */
    int bgwprocno;
} BufferStrategyControl;

typedef struct {
    int64 retry_times;
    int cur_delay_time;
} StrategyDelayStatus;

const int MIN_DELAY_RETRY = 100;
const int MAX_DELAY_RETRY = 1000;
const int MAX_RETRY_TIMES = 1000;
const float NEED_DELAY_RETRY_GET_BUF = 0.8;

/* Prototypes for internal functions */
static BufferDesc* GetBufferFromRing(BufferAccessStrategy strategy, uint64* buf_state);
static void AddBufferToRing(BufferAccessStrategy strategy, volatile BufferDesc* buf);
void PageListBackWrite(uint32* bufList, int32 n,
    /* buffer list, bufs to scan, */
    uint32 flags = 0,                 /* opt flags */
    SMgrRelation use_smgrReln = NULL, /* opt relation */
    int32* bufs_written = NULL,       /* opt written count returned */
    int32* bufs_reusable = NULL);     /* opt reusable count returned */
static BufferDesc* get_buf_from_candidate_list(BufferAccessStrategy strategy, uint64* buf_state);
#ifdef UBRL
static BufferDesc* get_buf_from_user_candidate_list(BufferAccessStrategy strategy, int user_slot, uint64* buf_state);
#endif
static void perform_delay(StrategyDelayStatus *status)
{
    if (++(status->retry_times) > MAX_RETRY_TIMES &&
        get_dirty_page_num() > g_instance.attr.attr_storage.NBuffers * NEED_DELAY_RETRY_GET_BUF) {
        if (status->cur_delay_time == 0) {
            status->cur_delay_time = MIN_DELAY_RETRY;
        }
        pg_usleep(status->cur_delay_time);

        /* increase delay by a random fraction between 1X and 2X */
        status->cur_delay_time += (int)(status->cur_delay_time * ((double)random() / (double)MAX_RANDOM_VALUE) + 0.5);
        if (status->cur_delay_time > MAX_DELAY_RETRY) {
            status->cur_delay_time = MIN_DELAY_RETRY;
        }
    }
    return;
}

#ifdef UBRL
static inline void update_all_topk_user() {
    for (int i = 0; i < USER_SLOT_NUM; i++) {
        if (g_instance.ckpt_cxt_ctl->user_buffer_info->user_info[i].slot == -1) {
            continue;
        }
        update_topk_user(i);
    }
}

/*
 * The shared user freelist control information.
 * update the top K user info when a user get a buffer
 * input is the user slot id in the user info that 
 * to be updated.
 * Find the min score in the topk list, if the user's score
 * is bigger than the min score, then update the topk list
 */

void update_topk_user(int slot_id) {
    UserInfo * user_info = NULL; 
    int min_index = -1;
    int slot_id_in_topk = -1;
    double score = 0;
    bool find = false;
    double min_score = INT_MAX;
    int sensitive = 10;
    /*first update it's score
     * no need acqure lock, this will not affect much.
     */
    UserBufferInfo * user_buffer_info = g_instance.ckpt_cxt_ctl->user_buffer_info;
    user_info = &user_buffer_info->user_info[slot_id];
    if (user_info->user_buffer_bought == 0) {
        return ;
    }
    pg_memory_barrier();
    int numBufferAllocs = (int)pg_atomic_read_u32(&user_info->numBufferAllocs);
    pg_memory_barrier();
    int numBufferAccess = (int)pg_atomic_read_u32(&user_info->numBufferAccessed);
    // double occu_percent = (double)numBufferAllocs/((double)user_info->user_buffer_bought);
    double occu_percent = 
            ((double)MIN(numBufferAllocs, user_info->user_buffer_bought)) / ((double)user_info->user_buffer_bought) + \
            ((double)MAX(0,  numBufferAllocs - (user_info->user_buffer_bought))) / ((double)TOTAL_BUFFER_NUM) ;

    // double occu_percent = ((double)numBufferAllocs * 2.0) /((double)TOTAL_BUFFER_NUM + ((double)numBufferAllocs));

    double use_percent = (double)numBufferAccess/(double)UB_STATISTICS_NUM;
    double new_score = occu_percent + (use_percent > 1e-5 ? -1:-0.01);
    if (numBufferAccess == 0) {
        user_info->in_use = false;
    }
    /*
        several situation:
        1. the one is not using the buffer, the score is big than 0;
        2. two user using the buffer, than the score will be small than 0, until the occu_percent is big than 1;
        3. At step 2, if a new user is comming in, it's use percent is growing and get buufer from the existing user until the occu_percent is big than 1; 
    */
    /*the score is associate with the user*/
    // user_info->buffer_occupation_score = (1 / (1 + exp(-1 * sensitive * percent))) - use_percent * 0.2;
    user_info->buffer_occupation_score = new_score;
    for (int i = 0; i < USER_BUFFER_TOPK; i++) {
        slot_id_in_topk = user_buffer_info->topk_user[i];

        score = user_buffer_info->user_info[slot_id_in_topk].buffer_occupation_score;

        if (slot_id_in_topk == slot_id) {
            // already in the topk list, no need to update;
            find = true;
            break;
        }

        if (slot_id_in_topk == -1) {
            // find a empty slot, stop searching
            // notice that this must be after the slot_id_in_topk == slot_id, 
            // make sure that the slot_id is not in the topk list
            find = true;
            user_buffer_info->topk_user[i] = slot_id;
            break;
        }
        // only score bigger than 0 is valid
        if (score < min_score && score > 0) {
            min_score = score;
            min_index = i;
        }
    }
    /*replace it with current user slot_id*/
    if (!find && min_index > -1 && user_info->buffer_occupation_score > min_score) {
        user_buffer_info->topk_user[min_index] = slot_id;
    }
}

int choose_top_user(int current_user_slot) {

    u_sess->attr.attr_storage.shared_buffers_fraction = 1;
    Assert(current_user_slot >= 0);
    UserInfo * current_user_info = &g_instance.ckpt_cxt_ctl->user_buffer_info->user_info[current_user_slot];
    if (g_instance.ckpt_cxt_ctl->user_buffer_info->strict_mode && \
        current_user_info->numBufferAllocs >= current_user_info->user_buffer_bought)  {
        /* use the current user if is strict mode and
         current user buf number is bigger than the bought number */
        return current_user_slot;
    }
    /*super user will not occupy free buffer if its buffer is equal to the bought number*/
    if (current_user_slot == 0 && current_user_info->numBufferAllocs >= current_user_info->user_buffer_bought) {
        return current_user_slot;
    }
    int slot_id_in_topk = -1;
    double score = 0;
    double max_score = DBL_MIN;
    int max_slot_index = current_user_slot; // t_thrd.storage_cxt.current_user_index;;
    double current_score = g_instance.ckpt_cxt_ctl->user_buffer_info->user_info[current_user_slot].buffer_occupation_score;
    for (int i = 0; i < USER_BUFFER_TOPK; i++) {
        slot_id_in_topk = g_instance.ckpt_cxt_ctl->user_buffer_info->topk_user[i];
        if (slot_id_in_topk == -1) {
            // find a empty slot
            continue;
        }
        if (i == 0 && g_instance.ckpt_cxt_ctl->user_buffer_info->user_info[0].numBufferAllocs <= g_instance.ckpt_cxt_ctl->user_buffer_info->user_info[0].user_buffer_bought)
            continue;
        score = g_instance.ckpt_cxt_ctl->user_buffer_info->user_info[slot_id_in_topk].buffer_occupation_score;
        if (score > max_score) {
            // to avoid frequent change, we will not change the user frequently, and two near score will not change
            max_slot_index = slot_id_in_topk;
            max_score = score;
        }
    }
    if (max_score < 0 || abs(current_score - max_score) < 1e-3) {
        max_slot_index = u_sess->user_slot ;
    }

    // if -1, the use current_user
    return max_slot_index;
}

bool search_remove_user_buffer_slot(uint64 user_id) {
    bool found = false;

    for (int i = 0; i < USER_SLOT_NUM; i++) {
        if (g_instance.ckpt_cxt_ctl->user_buffer_info->user_info[i].user_id == user_id) {
            pg_atomic_write_u32(&g_instance.ckpt_cxt_ctl->user_buffer_info->user_info[i].numBufferAllocs, 0);
            pg_atomic_write_u32(&g_instance.ckpt_cxt_ctl->user_buffer_info->user_info[i].nextVictimBuffer, UINT32_MAX);
            S_LOCK(&g_instance.ckpt_cxt_ctl->user_buffer_info->user_info[i].user_lock);
            g_instance.ckpt_cxt_ctl->user_buffer_info->user_info[i].user_id = ((Oid)0);
            S_UNLOCK(&g_instance.ckpt_cxt_ctl->user_buffer_info->user_info[i].user_lock);
            found = true;
            break;
        }
    }
    return found;
}

/*
 * @Description: get the next victim buffer for a user
    * @in user_id - the user id
    * @return - the next victim buffer index
    * @see also:
    * @note: this function will lock the user_info->user_lock and user_buffer_lock
    *       so the caller should not hold the lock before call this function
    *      and should not hold the lock after call this function
    *     this function will not change the user_info->nextVictimBuffer
    *    so the caller should change the user_info->nextVictimBuffer after call this function
    *  this function will not change the user_info->numBufferAllocs
    * so the caller should change the user_info->numBufferAllocs after call this function
    * this function will not change the StrategyControl->nextVictimBuffer
*/


static inline uint32 GetUserNextVictimBuffer(int victim_user_slot)
{
    UserInfo* victim_user_info = NULL;
    // if (user_slot < 0) {
    //    victim_user_info = search_insert_user_buffer_slot(u_sess->misc_cxt.CurrentUserId, true);
    // } else {
    victim_user_info = &g_instance.ckpt_cxt_ctl->user_buffer_info->user_info[victim_user_slot];
    // } 
    uint32 next_victim = 0;
    UserBufferDesc* buf_desc = NULL;
    // uint32 current_victim = 0;

    Assert(victim_user_info != NULL);
    /* choose other's buffer*/

    /* if the buffer is the last buffer page */  
    S_LOCK(&victim_user_info->buffer_link_lock);

    /* -- not removed hear  */
    // next_victim = pg_atomic_exchange_u32()
    int retry_times = 0;
    int b_id = 0;
    if (victim_user_info->numBufferAllocs > 0) {
        next_victim = pg_atomic_read_u32(&victim_user_info->nextVictimBuffer);
        // while(true) {
            /* find a clean buffer as victim*/
        buf_desc = GetUserBufferDescriptor(next_victim);
        Assert(buf_desc->user_slot == victim_user_slot);
        pg_atomic_write_u32(&victim_user_info->nextVictimBuffer, buf_desc->links->next);        
    } else {
        next_victim = INVALID_USER_VICTIM;
    }
    // if (retry_times >= victim_user_info->numBufferAllocs) {
    //     next_victim = INVALID_USER_VICTIM;
    // }
    S_UNLOCK(&victim_user_info->buffer_link_lock);

    return next_victim;
}




// lock should be held by the caller
static inline void strategy_list_remove(UserBufferDesc* user_buffer, int strategy, int user_slot) {
    // S_LOCK(&g_instance.ckpt_cxt_ctl->user_buffer_info->user_info[user_slot].buffer_link_lock);
    int head = t_thrd.storage_cxt.UserStrategyControl[(user_slot)].BufferInfo[(strategy)].head;
    int tail = t_thrd.storage_cxt.UserStrategyControl[(user_slot)].BufferInfo[(strategy)].tail;
    Assert(head != -1 && tail != -1);
    int prev = user_buffer->links[strategy].prev;
    int next = user_buffer->links[strategy].next;
    if (prev == -1) {
        // this buf is the head, change the head
        t_thrd.storage_cxt.UserStrategyControl[(user_slot)].BufferInfo[(strategy)].head = user_buffer->links[strategy].next;
    } else {
        GetUserBufferDescriptor(prev)->links[strategy].next = user_buffer->links[strategy].next;
    }

    if (next == -1) {
        t_thrd.storage_cxt.UserStrategyControl[(user_slot)].BufferInfo[(strategy)].tail = user_buffer->links[strategy].prev;

    } else {
        GetUserBufferDescriptor(next)->links[strategy].prev = user_buffer->links[strategy].prev;
    }
    user_buffer->links[strategy].prev = -1;
    user_buffer->links[strategy].next = -1;
    user_buffer->removed = true;
    // S_UNLOCK(&g_instance.ckpt_cxt_ctl->user_buffer_info->user_info[user_slot].buffer_link_lock);
}

static inline void strategy_insert_head(UserBufferDesc* user_buffer, int strategy, int user_slot) {

    // S_LOCK(&g_instance.ckpt_cxt_ctl->user_buffer_info->user_info[user_slot].buffer_link_lock);

    int head = t_thrd.storage_cxt.UserStrategyControl[(user_slot)].BufferInfo[(strategy)].head;
    int tail = t_thrd.storage_cxt.UserStrategyControl[(user_slot)].BufferInfo[(strategy)].tail;
    if (head < 0) {
        ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("head is -1")));
        t_thrd.storage_cxt.UserStrategyControl[(user_slot)].BufferInfo[(strategy)].head = user_buffer->buf_id;
        t_thrd.storage_cxt.UserStrategyControl[(user_slot)].BufferInfo[(strategy)].tail = user_buffer->buf_id;
        user_buffer->links[(strategy)].prev = -1;
        user_buffer->links[(strategy)].next = -1;
    } else {
        Assert(head >= 0 && tail >= 0);

        user_buffer->links[(strategy)].prev = -1;
        user_buffer->links[(strategy)].next = head;
        GetUserBufferDescriptor(head)->links[(strategy)].prev = user_buffer->buf_id;
        t_thrd.storage_cxt.UserStrategyControl[(user_slot)].BufferInfo[(strategy)].head = user_buffer->buf_id;
    }
    user_buffer->removed = true;

    // S_UNLOCK(&g_instance.ckpt_cxt_ctl->user_buffer_info->user_info[user_slot].buffer_link_lock);
}

static inline void strategy_insert_tail(UserBufferDesc* user_buffer, int strategy, int user_slot) {
    // S_LOCK(&g_instance.ckpt_cxt_ctl->user_buffer_info->user_info[user_slot].buffer_link_lock);

    int head = t_thrd.storage_cxt.UserStrategyControl[(user_slot)].BufferInfo[(strategy)].head;
    int tail = t_thrd.storage_cxt.UserStrategyControl[(user_slot)].BufferInfo[(strategy)].tail;
    if (head < 0) {
        ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR), errmsg("head is -1")));

        Assert(tail < 0);
        t_thrd.storage_cxt.UserStrategyControl[(user_slot)].BufferInfo[(strategy)].head = user_buffer->buf_id;
        t_thrd.storage_cxt.UserStrategyControl[(user_slot)].BufferInfo[(strategy)].tail = user_buffer->buf_id;
        user_buffer->links[(strategy)].prev = -1;
        user_buffer->links[(strategy)].next = -1;
    } else {
        Assert(head >= 0 && tail >= 0);
        user_buffer->links[(strategy)].prev = tail;
        user_buffer->links[(strategy)].next = -1;
        GetUserBufferDescriptor(tail)->links[(strategy)].next = user_buffer->buf_id;
        t_thrd.storage_cxt.UserStrategyControl[(user_slot)].BufferInfo[(strategy)].tail = user_buffer->buf_id;
    }

    // S_UNLOCK(&g_instance.ckpt_cxt_ctl->user_buffer_info->user_info[user_slot].buffer_link_lock);
}
/*
 * StrategyUpdateBuffer: if the buffer is found or need to update(when a new buffer is allocated),
 * Only deal with buffer in current user
 */
BufferDesc * StrategyUpdateBuffer(BufferDesc *buf, int user_slot) {

    UserInfo* current_user_info = &g_instance.ckpt_cxt_ctl->user_buffer_info->user_info[u_sess->user_slot];
    UserBufferDesc *user_buffer = GetUserBufferDescriptor(buf->buf_id);
    S_LOCK(&current_user_info->buffer_link_lock);
    for (int i = 0; i < NUM_STRATEGY; i++) {
        strategy_list_remove(user_buffer, i, u_sess->user_slot);
        // STRAT_LIST_REMOVE(user_buffer, i, u_sess->user_slot);
        if (i == LRU) {
            // put it to the tail of the list for LRU
            strategy_insert_tail(user_buffer, i, u_sess->user_slot);
            // STRAT_MRU_INSERT(user_buffer, i, u_sess->user_slot);
        } else if (i == MRU) {
            // put it to the head of the list for MRU
            // STRAT_LRU_INSERT(user_buffer, i, u_sess->user_slot);
            strategy_insert_head(user_buffer, i, u_sess->user_slot);
        }

    }
    S_UNLOCK(&current_user_info->buffer_link_lock);

	return NULL;
}
/*
 * StrategyUpdateBuffer: if the buffer is found or need to update(when a new buffer is allocated),
 */
bool StrategyBackBuffer(BufferDesc *buf, int user_slot, bool head) {
    UserBufferDesc * user_buf = GetUserBufferDescriptor(buf->buf_id);
    UserInfo * user_info = &g_instance.ckpt_cxt_ctl->user_buffer_info->user_info[user_slot];
    S_LOCK(&user_info->buffer_link_lock);
    // STRAT_LRU_INSERT(user_buf, LRU, user_slot);
    //STRAT_LRU_INSERT(user_buf, MRU, user_slot);
    if (head) {
        strategy_insert_head(user_buf, LRU, user_slot);
        strategy_insert_head(user_buf, MRU, user_slot);
    } else {
        strategy_insert_tail(user_buf, LRU, user_slot);
        strategy_insert_tail(user_buf, MRU, user_slot);
    }
    S_UNLOCK(&user_info->buffer_link_lock);
	/* not reached */
	return true;
}


/*这个函数是在持有分区锁的情况下执行的*/
void StrategyUpdateHistory(int next_item_id, BufferTag *oldTag, uint32 old_hash, int strategy) {
    // StrategyUpdateBuffer(buf, victim_user_slot);

    bool is_valid = false;
    BufferTag head_tag;
    uint32 head_hash;
    UserStrategyVictimHistory *next_item = NULL;
    LWLock * next_partition_lock = NULL; /* buffer partition lock for it */
    LWLock *old_partition_lock = NULL; /* buffer partition lock for it */

    // free one on the head of history list, and add it to the head of history list
    // S_LOCK(&victim_user_info->victim_history_lock);
    // next_history = pg_atomic_read_u32(&t_thrd.storage_cxt.UserStrategyControl[victim_user_slot].next_history);
    next_item = GetStrategyHistoryDesc(next_item_id);
    // pg_atomic_write_u32(&t_thrd.storage_cxt.UserStrategyControl[victim_user_slot].next_history, next_item->next);
    // S_UNLOCK(&victim_user_info->victim_history_lock);

    S_LOCK(&next_item->lock);
    // set the tag and hash to old
    next_partition_lock = BufHisMappingPartitionLock(next_item->buf_hash);
    (void)LWLockAcquire(next_partition_lock, LW_EXCLUSIVE);
        // remove this one in history list
    if (next_item->valid) {
        UserBufVictimHisTableDelete(&next_item->buf_tag, next_item->buf_hash);
    }
    // 替换成刚被淘汰的记录，该记录已经被淘汰了，但是保留在hash中

    LWLockRelease(next_partition_lock);

    old_partition_lock = BufHisMappingPartitionLock(old_hash);
    (void)LWLockAcquire(old_partition_lock, LW_EXCLUSIVE);
    UserBufVictimHisTableInsert(oldTag, old_hash, next_item_id);
    next_item->valid = true;
    BUFFERTAGS_PTR_SET(&next_item->buf_tag, oldTag);
    next_item->buf_hash = old_hash;
    next_item->strategy = strategy;
    next_item->timestemp = (int64)(get_time_ms()/1000);
    LWLockRelease(old_partition_lock);
    S_UNLOCK(&next_item->lock);


}

/*
 * StrategyReplaceBuffer
 * insert the old buffer to the victim history,  if it's full, remove the old one;
 */
void StrategyReplaceBuffer(BufferDesc *buf, BufferTag *oldTag, uint32 old_hash, bool tag_valid, int strategy, int victim_user_slot) {
    // StrategyUpdateBuffer(buf, victim_user_slot);

    UserInfo * victim_user_info = &g_instance.ckpt_cxt_ctl->user_buffer_info->user_info[victim_user_slot];
    UserInfo * current_user_info = &g_instance.ckpt_cxt_ctl->user_buffer_info->user_info[u_sess->user_slot];
    bool is_valid = false;
    BufferTag head_tag;
    uint32 head_hash;
    UserStrategyVictimHistory *next_item = NULL;
    LWLock * next_partition_lock = NULL; /* buffer partition lock for it */

    // free one on the head of history list, and add it to the head of history list
    int next_history = -1;
    S_LOCK(&victim_user_info->victim_history_lock);
    next_history = pg_atomic_read_u32(&t_thrd.storage_cxt.UserStrategyControl[victim_user_slot].next_history);
    next_item = GetStrategyHistoryDesc(next_history);
    pg_atomic_write_u32(&t_thrd.storage_cxt.UserStrategyControl[victim_user_slot].next_history, next_item->next);
    S_UNLOCK(&victim_user_info->victim_history_lock);

    S_LOCK(&next_item->lock);
    if (next_item->valid) {
        // this item contain a valid hash, so delete it from the hash table
        next_partition_lock = BufMappingPartitionLock(next_item->buf_hash);
        (void)LWLockAcquire(next_partition_lock, LW_EXCLUSIVE);
        // remove this one in history list
        // S_LOCK(&head_item->lock);
        // S_UNLOCK(&head_item->lock);
        UserBufVictimHisTableDelete(&next_item->buf_tag, next_item->buf_hash);
        LWLockRelease(next_partition_lock);
    }
    next_item->valid = false;
    if (tag_valid) {
        // set the tag and hash to old
        LWLock *old_partition_lock = NULL; /* buffer partition lock for it */
        old_partition_lock = BufMappingPartitionLock(old_hash);
        (void)LWLockAcquire(old_partition_lock, LW_EXCLUSIVE);
        // remove this one in history list
        UserBufVictimHisTableInsert(oldTag, old_hash, next_item->id);
        next_item->valid = true;
        BUFFERTAGS_PTR_SET(&next_item->buf_tag, oldTag);
        next_item->buf_hash = old_hash;
        next_item->strategy = strategy;
        next_item->timestemp = (int64)(get_time_ms()/1000);
        LWLockRelease(old_partition_lock);
    }
    S_UNLOCK(&next_item->lock);

}

int get_user_next_victim_history(int victim_user_slot) {
    UserInfo * victim_user_info = &g_instance.ckpt_cxt_ctl->user_buffer_info->user_info[victim_user_slot];
    S_LOCK(&victim_user_info->victim_history_lock);
    int next_history = pg_atomic_read_u32(&t_thrd.storage_cxt.UserStrategyControl[victim_user_slot].next_history);
    UserStrategyVictimHistory * next_item = GetStrategyHistoryDesc(next_history);
    pg_atomic_write_u32(&t_thrd.storage_cxt.UserStrategyControl[victim_user_slot].next_history, next_item->next);
    S_UNLOCK(&victim_user_info->victim_history_lock);
    return next_history;
}
/*
 * check is the new buffer is in the history list,
 * if it is, remove it from the history list
 * then choose the strategy
 */
// int UpdateRLStrategy(BufferTag new_tag, uint32 new_hash, int user_slot) {
int UpdateRLStrategy(int victim_buffer_id, BufferTag* tag, uint32 hash, int user_slot) {
    
    int record_id = -1;
    int new_buf_strategy = -1;
    UserStrategyVictimHistory * history = NULL;
    LWLock *partition_lock = NULL; /* buffer partition lock for it */
    struct timeval tv;
    uint64 time_s;
    // (void)gettimeofday(&tv, NULL);
    time_s = (int64)(get_time_ms()/1000);
    uint64 time_passed = 0;
    double discount_rate = 0.9999;
    bool find = false;
    // (void)LWLockAcquire(partition_lock, LW_EXCLUSIVE);
    // record_id = UserBufVictimHisTableLookup(&new_tag, new_hash, user_slot);
    // LWLockRelease(partition_lock);

    if (victim_buffer_id >= 0) {

        UserStrategyVictimHistory* history_item = GetStrategyHistoryDesc(victim_buffer_id);
        S_LOCK(&history_item->lock);

        partition_lock =  BufHisMappingPartitionLock(history_item->buf_hash);
        (void)LWLockAcquire(partition_lock, LW_EXCLUSIVE);

        if (history_item->valid && history_item->buf_hash == hash
            && BUFFERTAGS_EQUAL(history_item->buf_tag , *tag)) { 
            // 证明该页面保存的是找到的节点，否则，该节点已经被删除了。
            UserBufVictimHisTableDelete(&history_item->buf_tag, history_item->buf_hash);
            find = true;
            history_item->valid = false;
            // 在其他位置删除的时候，必定会先加分区锁
        }
        time_passed = time_s - history_item->timestemp;
        new_buf_strategy = history_item->strategy;
        LWLockRelease(partition_lock);
        S_UNLOCK(&history_item->lock);

    }

    if (find && new_buf_strategy > -1) {
        discount_rate = powf64(discount_rate, time_passed);

        // update the weight of the new buffer if the buf in any history list
            // find new buff in history list, update list
        t_thrd.storage_cxt.UserStrategyControl[user_slot].Strategy.weight[new_buf_strategy] *= \
                        exp(-t_thrd.storage_cxt.UserStrategyControl[user_slot].Strategy.lr[0] * discount_rate);
        if (new_buf_strategy == 0) {
            t_thrd.storage_cxt.UserStrategyControl[user_slot].Strategy.weight[1] = \
                        1 - t_thrd.storage_cxt.UserStrategyControl[user_slot].Strategy.weight[0];
        } else {
            t_thrd.storage_cxt.UserStrategyControl[user_slot].Strategy.weight[0] = \
                        1 - t_thrd.storage_cxt.UserStrategyControl[user_slot].Strategy.weight[1];
        }
    }
    return 0;
}

int chooseRLStrategy(bool is_current_user) {

    if (!is_current_user) {
        // 如果我们想要淘汰其他用户的页面，则调用最近最少使用的页面（LRU）
        return LRU;
    }
    int strategy = 0;
    int user_slot = u_sess->user_slot;
    for (int i = 0; i < NUM_STRATEGY; i++) {
        if (t_thrd.storage_cxt.UserStrategyControl[user_slot].Strategy.weight[i] > \
                    t_thrd.storage_cxt.UserStrategyControl[user_slot].Strategy.weight[strategy]) {
            strategy = i;
        }
    }
    return strategy;
}

void updateRLLearnRate(int user_slot) {
    // int64 time = get_time_ms();
    double lr = 0.0;
    double d_hr = t_thrd.storage_cxt.UserStrategyControl[user_slot].Strategy.hit_rate[0] - \
                    t_thrd.storage_cxt.UserStrategyControl[user_slot].Strategy.hit_rate[1];
    double d_lr = t_thrd.storage_cxt.UserStrategyControl[user_slot].Strategy.lr[0] - \
                    t_thrd.storage_cxt.UserStrategyControl[user_slot].Strategy.lr[1];
    double new_lr = 0;
    if (d_lr != 0) {
        new_lr = (d_hr*d_lr > 0 ? 1:-1)* t_thrd.storage_cxt.UserStrategyControl[user_slot].Strategy.lr[0] * Abs(d_lr) + 
                                        t_thrd.storage_cxt.UserStrategyControl[user_slot].Strategy.lr[0];
        
        new_lr = MAX(1e-3, new_lr );
        t_thrd.storage_cxt.UserStrategyControl[user_slot].Strategy.unlearnCount = 0;
    } else {
        if (t_thrd.storage_cxt.UserStrategyControl[user_slot].Strategy.hit_rate[0] == 0 || d_hr < 0) {
            t_thrd.storage_cxt.UserStrategyControl[user_slot].Strategy.unlearnCount++;
        }
        if (t_thrd.storage_cxt.UserStrategyControl[user_slot].Strategy.unlearnCount > 10) {
            do {
                new_lr = random() / (double)RAND_MAX;
                t_thrd.storage_cxt.UserStrategyControl[user_slot].Strategy.unlearnCount = 0;
            } while (new_lr < 1e-3);
        }
    }
    t_thrd.storage_cxt.UserStrategyControl[user_slot].Strategy.lr[2] = \
            t_thrd.storage_cxt.UserStrategyControl[user_slot].Strategy.lr[1];
    t_thrd.storage_cxt.UserStrategyControl[user_slot].Strategy.lr[1] = \
            t_thrd.storage_cxt.UserStrategyControl[user_slot].Strategy.lr[0];
    t_thrd.storage_cxt.UserStrategyControl[user_slot].Strategy.lr[0] = new_lr;
}

#endif

/*
 * ClockSweepTick - Helper routine for StrategyGetBuffer()
 *
 * Move the clock hand one buffer ahead of its current position and return the
 * id of the buffer now under the hand.
 */
static inline uint32 ClockSweepTick(int max_nbuffer_can_use)
{
    uint32 victim;

    /*
     * Atomically move hand ahead one buffer - if there's several processes
     * doing this, this can lead to buffers being returned slightly out of
     * apparent order.
     */
    victim = pg_atomic_fetch_add_u32(&t_thrd.storage_cxt.StrategyControl->nextVictimBuffer, 1);
    if (victim >= (uint32)max_nbuffer_can_use) {
        uint32 original_victim = victim;

        /* always wrap what we look up in BufferDescriptors */
        victim = victim % max_nbuffer_can_use;

        /*
         * If we're the one that just caused a wraparound, force
         * completePasses to be incremented while holding the spinlock. We
         * need the spinlock so StrategySyncStart() can return a consistent
         * value consisting of nextVictimBuffer and completePasses.
         */
        if (victim == 0) {
            uint32 expected;
            uint32 wrapped;
            bool success = false;

            expected = original_victim + 1;

            while (!success) {
                /*
                 * Acquire the spinlock while increasing completePasses. That
                 * allows other readers to read nextVictimBuffer and
                 * completePasses in a consistent manner which is required for
                 * StrategySyncStart().  In theory delaying the increment
                 * could lead to a overflow of nextVictimBuffers, but that's
                 * highly unlikely and wouldn't be particularly harmful.
                 */
                SpinLockAcquire(&t_thrd.storage_cxt.StrategyControl->buffer_strategy_lock);

                wrapped = expected % max_nbuffer_can_use;

                success = pg_atomic_compare_exchange_u32(&t_thrd.storage_cxt.StrategyControl->nextVictimBuffer,
                                                         &expected, wrapped);
                if (success)
                    t_thrd.storage_cxt.StrategyControl->completePasses++;
                SpinLockRelease(&t_thrd.storage_cxt.StrategyControl->buffer_strategy_lock);
            }
        }
    }
    return victim;
}

#ifdef UBRL
static inline uint32 getBufferByStrategy(int victim_user_slot, int strategy_id) {
    UserBufferDesc * buf = NULL;
    int buf_id = -1;
    UserInfo* victim_user_info = NULL;

    victim_user_info = &g_instance.ckpt_cxt_ctl->user_buffer_info->user_info[victim_user_slot];
    // } 
    uint32 next_victim = 0;
    BufferDesc* buf_desc = NULL;
    UserBufferDesc *user_buffer = NULL;
    // uint32 current_victim = 0;

    Assert(victim_user_info != NULL);
    /* choose other's buffer*/

    /* if the buffer is the last buffer page */  
    S_LOCK(&victim_user_info->buffer_link_lock);

    /* -- not removed hear  */
    // next_victim = pg_atomic_exchange_u32()
    int retry_times = 0;
    int b_id = 0;
    if (victim_user_info->numBufferAllocs > 0) {
        next_victim = pg_atomic_read_u32(&t_thrd.storage_cxt.UserStrategyControl[victim_user_slot].BufferInfo[strategy_id].nextVictimBuffer);
        // while(true) {
            /* find a clean buffer as victim*/
        user_buffer = GetUserBufferDescriptor(next_victim);
        Assert(user_buffer->user_slot == victim_user_slot);
        pg_atomic_write_u32(&t_thrd.storage_cxt.UserStrategyControl[victim_user_slot].BufferInfo[strategy_id].nextVictimBuffer,\
                                 user_buffer->links[strategy_id].next);   
        // }     
    } else {
        next_victim = INVALID_USER_VICTIM;
    }

    S_UNLOCK(&victim_user_info->buffer_link_lock);

    return next_victim;
}

/*
 * strategy_id is the strategy id of rl, user_slot is the user slot of the victim user,
 * last_buf is the last buffer selected by this function, and it was rejected by the caller,
 * so we need to put it back to the strategy list.
 */
BufferDesc *StrategyGetBufferRL(BufferAccessStrategy strategy, uint64 *buf_state, int strategy_id, int user_slot, BufferDesc *last_buf) {
    BufferDesc *buf = NULL;
    int bgwproc_no;
    int try_counter;
    uint64 local_buf_state = 0; /* to avoid repeated (de-)referencing */
    int max_buffer_can_use;
    bool am_standby = RecoveryInProgress();
    StrategyDelayStatus retry_lock_status = { 0, 0 };
    StrategyDelayStatus retry_buf_status = { 0, 0 };
    UserInfo* victim_user_info = &g_instance.ckpt_cxt_ctl->user_buffer_info->user_info[user_slot];
    int buf_id = -1;
    UserBufferDesc * user_buf = NULL;

    if (last_buf != NULL) {
        UserBufferDesc *last_user_buf = GetUserBufferDescriptor(last_buf->buf_id);
        // push back the last buffer poped from the pool
        // STRAT_MRU_INSERT(last_user_buf, LRU, user_slot);
        // STRAT_MRU_INSERT(last_user_buf, MRU, user_slot);
        S_LOCK(&victim_user_info->buffer_link_lock);
        strategy_insert_tail(last_user_buf, LRU, user_slot);
        strategy_insert_tail(last_user_buf, MRU, user_slot);
        S_UNLOCK(&victim_user_info->buffer_link_lock);
    }
    /*
     * If given a strategy object, see whether it can select a buffer. We
     * assume strategy objects don't need buffer_strategy_lock.
     */
    if (strategy != NULL) {
        buf = GetBufferFromRing(strategy, buf_state);
        if (buf != NULL) {
            buf_id = buf->buf_id;
            user_buf = GetUserBufferDescriptor(buf_id);
            S_LOCK(&victim_user_info->buffer_link_lock);
            strategy_list_remove(user_buf, LRU, user_slot);
            strategy_list_remove(user_buf, MRU, user_slot);

            // // remove it from all list
            // STRAT_LIST_REMOVE(user_buf, LRU, user_slot);
            // STRAT_LIST_REMOVE(user_buf, MRU, user_slot);

            S_UNLOCK(&victim_user_info->buffer_link_lock);

            return buf;
        }
    }

    /*
     * If asked, we need to waken the bgwriter. Since we don't want to rely on
     * a spinlock for this we force a read from shared memory once, and then
     * set the latch based on that value. We need to go through that length
     * because otherwise bgprocno might be reset while/after we check because
     * the compiler might just reread from memory.
     *
     * This can possibly set the latch of the wrong process if the bgwriter
     * dies in the wrong moment. But since PGPROC->procLatch is never
     * deallocated the worst consequence of that is that we set the latch of
     * some arbitrary process.
     */
    bgwproc_no = INT_ACCESS_ONCE(t_thrd.storage_cxt.StrategyControl->bgwprocno);
    if (bgwproc_no != -1) {
        /* reset bgwprocno first, before setting the latch */
        t_thrd.storage_cxt.StrategyControl->bgwprocno = -1;

        /*
         * Not acquiring ProcArrayLock here which is slightly icky. It's
         * actually fine because procLatch isn't ever freed, so we just can
         * potentially set the wrong process' (or no process') latch.
         */
        SetLatch(&g_instance.proc_base_all_procs[bgwproc_no]->procLatch);
    }

    /*
     * We count buffer allocation requests so that the bgwriter can estimate
     * the rate of buffer consumption.	Note that buffers recycled by a
     * strategy object are intentionally not counted here.
     */
    (void)pg_atomic_fetch_add_u32(&t_thrd.storage_cxt.StrategyControl->numBufferAllocs, 1);
    if (t_thrd.storage_cxt.StrategyControl->numBufferAllocs % 5000 == 0) {
        update_all_topk_user();
    }
    int victim_user_slot = user_slot;
    int real_victim = 0;

    Assert(victim_user_slot != -1);

    /* Check the Candidate list */
    if (ENABLE_INCRE_CKPT && pg_atomic_read_u32(&g_instance.ckpt_cxt_ctl->current_page_writer_count) > 1) {
        if (NEED_CONSIDER_USECOUNT) {
            const uint32 MAX_RETRY_SCAN_CANDIDATE_LISTS = 5;
            const int MILLISECOND_TO_MICROSECOND = 1000;
            uint64 maxSleep = u_sess->attr.attr_storage.BgWriterDelay * MILLISECOND_TO_MICROSECOND;
            uint64 sleepTime = 1000L;
            uint32 retry_times = 0;
            while (retry_times < MAX_RETRY_SCAN_CANDIDATE_LISTS) {
                buf = get_buf_from_user_candidate_list(strategy, victim_user_slot, buf_state);
                if (buf != NULL) {
                    (void)pg_atomic_fetch_add_u64(&g_instance.ckpt_cxt_ctl->get_buf_num_candidate_list, 1);
                    return buf;
                }
                pg_usleep(sleepTime);
                sleepTime = Min(sleepTime * 2, maxSleep);
                retry_times++;
            }
        } else {
            buf = get_buf_from_user_candidate_list(strategy, victim_user_slot, buf_state);
            if (buf != NULL) {
                (void)pg_atomic_fetch_add_u64(&g_instance.ckpt_cxt_ctl->get_buf_num_candidate_list, 1);
                return buf;
            }
        }
    }

    if (strategy_id != LRU && strategy_id != MRU) {
        ereport(ERROR,
                (errmsg("Unknow strategy id %d when StrategyGetBufferRL", strategy_id)));
    }

retry:
    /* Nothing on the freelist, so run the "clock sweep" algorithm */
    if (am_standby)
        max_buffer_can_use = int(victim_user_info->user_buffer_bought * u_sess->attr.attr_storage.shared_buffers_fraction);
    else
        max_buffer_can_use = victim_user_info->user_buffer_bought;
    try_counter = max_buffer_can_use;
    int try_get_loc_times = max_buffer_can_use;

    for (;;) {

        S_LOCK(&victim_user_info->buffer_link_lock);

        buf_id = t_thrd.storage_cxt.UserStrategyControl[user_slot].BufferInfo[strategy_id].head;
        user_buf = GetUserBufferDescriptor(buf_id);
        // remove it from all list
        // STRAT_LIST_REMOVE(user_buf, LRU, user_slot);
        // STRAT_LIST_REMOVE(user_buf, MRU, user_slot);
        strategy_list_remove(user_buf, LRU, user_slot);
        strategy_list_remove(user_buf, MRU, user_slot);
    
        S_UNLOCK(&victim_user_info->buffer_link_lock);
        buf = GetBufferDescriptor(buf_id);
        Assert(buf_id >= 0);

        /*
         * If the buffer is pinned, we cannot use it.
         */
        if (!retryLockBufHdr(buf, &local_buf_state)) {
            if (--try_get_loc_times == 0) {
                ereport(WARNING,
                        (errmsg("try get buf headr lock times equal to maxNBufferCanUse when StrategyGetBuffer")));
                try_get_loc_times = max_buffer_can_use;
            }
            perform_delay(&retry_lock_status);
            S_LOCK(&victim_user_info->buffer_link_lock);
            // this buffer can not be used, so put it back to the tail of the list
            // STRAT_MRU_INSERT(user_buf, LRU, user_slot);
            // STRAT_MRU_INSERT(user_buf, MRU, user_slot);
            strategy_insert_tail(user_buf, LRU, user_slot);
            strategy_insert_tail(user_buf, MRU, user_slot);
            S_UNLOCK(&victim_user_info->buffer_link_lock);

            continue;
        }

        retry_lock_status.retry_times = 0;
        if (BUF_STATE_GET_REFCOUNT(local_buf_state) == 0 && !(local_buf_state & BM_IS_META) &&
            (backend_can_flush_dirty_page() || !(local_buf_state & BM_DIRTY))) {
            /* Found a usable buffer */
            if (strategy != NULL)
                AddBufferToRing(strategy, buf);
            *buf_state = local_buf_state;
            (void)pg_atomic_fetch_add_u64(&g_instance.ckpt_cxt_ctl->get_buf_num_clock_sweep, 1);
            // S_UNLOCK(&victim_user_info->buffer_link_lock);

            return buf;
        } else if (--try_counter == 0) {
            /*
             * We've scanned all the buffers without making any state changes,
             * so all the buffers are pinned (or were when we looked at them).
             * We could hope that someone will free one eventually, but it's
             * probably better to fail than to risk getting stuck in an
             * infinite loop.
             */
            UnlockBufHdr(buf, local_buf_state);
            S_LOCK(&victim_user_info->buffer_link_lock);
            // this buffer can not be used, so put it back to the tail of the list
            // STRAT_MRU_INSERT(user_buf, LRU, user_slot);
            // STRAT_MRU_INSERT(user_buf, MRU, user_slot);
            strategy_insert_tail(user_buf, LRU, user_slot);
            strategy_insert_tail(user_buf, MRU, user_slot);

            S_UNLOCK(&victim_user_info->buffer_link_lock);
            if (am_standby && u_sess->attr.attr_storage.shared_buffers_fraction < 1.0) {
                ereport(WARNING, (errmsg("no unpinned buffers available")));
                u_sess->attr.attr_storage.shared_buffers_fraction =
                    Min(u_sess->attr.attr_storage.shared_buffers_fraction + 0.1, 1.0);
                goto retry;
            } else if (dw_page_writer_running()) {
                ereport(LOG, (errmsg("double writer is on, no buffer available, this buffer dirty is %lu, "
                                     "this buffer refcount is %lu, now dirty page num is %ld",
                                     (local_buf_state & BM_DIRTY), BUF_STATE_GET_REFCOUNT(local_buf_state),
                                     get_dirty_page_num())));
                perform_delay(&retry_buf_status);
                goto retry;
            } else if (t_thrd.storage_cxt.is_btree_split) {
                ereport(WARNING, (errmsg("no unpinned buffers available when btree insert parent")));
                goto retry;
            } else
                ereport(ERROR, (errcode(ERRCODE_INVALID_BUFFER), (errmsg("no unpinned buffers available"))));
        } else {
            S_LOCK(&victim_user_info->buffer_link_lock);
            // this buffer can not be used, so put it back to the tail of the list
            // STRAT_MRU_INSERT(user_buf, LRU, user_slot);
            // STRAT_MRU_INSERT(user_buf, MRU, user_slot);
            strategy_insert_tail(user_buf, LRU, user_slot);
            strategy_insert_tail(user_buf, MRU, user_slot);

            S_UNLOCK(&victim_user_info->buffer_link_lock);
        }
        UnlockBufHdr(buf, local_buf_state);

        perform_delay(&retry_buf_status);
    }
    /* not reached */
    return NULL;
}

#endif


/*
 * StrategyGetBuffer
 *
 *	Called by the bufmgr to get the next candidate buffer to use in
 *	BufferAlloc(). The only hard requirement BufferAlloc() has is that
 *	the selected buffer must not currently be pinned by anyone.
 *
 *	strategy is a BufferAccessStrategy object, or NULL for default strategy.
 *
 *	To ensure that no one else can pin the buffer before we do, we must
 *	return the buffer with the buffer header spinlock still held.
 *
 *  If Standby, we restrict its memory usage to shared_buffers_fraction of
 *  NBuffers, Standby will not get buffer from freelist to avoid touching all
 *  buffers and always run the "clock sweep" in shared_buffers_fraction * NBuffers.
 *  If the fraction is too small, we will increase dynamiclly to avoid elog(ERROR)
 *  in `Startup' process because of ERROR will promote to FATAL.
 */
#ifdef UBRL
BufferDesc* StrategyGetBuffer(BufferAccessStrategy strategy, uint64* buf_state, bool use_own) //, int strategy_id, int user_slot, BufferDesc *last_buf)
#else 
BufferDesc* StrategyGetBuffer(BufferAccessStrategy strategy, uint64* buf_state)
#endif
{
    BufferDesc *buf = NULL;
    int bgwproc_no;
    int try_counter;
    uint64 local_buf_state = 0; /* to avoid repeated (de-)referencing */
    int max_buffer_can_use;
    bool am_standby = RecoveryInProgress();
    StrategyDelayStatus retry_lock_status = { 0, 0 };
    StrategyDelayStatus retry_buf_status = { 0, 0 };

#ifdef UBRL
    int user_slot = -1;
    if (use_own) {
        user_slot = u_sess->user_slot;
    } else {
        user_slot = choose_top_user(u_sess->user_slot);
    }
    Assert(u_sess->user_slot != -1);
    
    BufferDesc *last_buf = NULL;
    // not found, choose a strategy, check the strategy history
    int strategy_id = chooseRLStrategy(u_sess->user_slot == user_slot);
    BufferDesc *old_buf = NULL;

    UserInfo* victim_user_info = &g_instance.ckpt_cxt_ctl->user_buffer_info->user_info[user_slot];
    int buf_id = -1;
    UserBufferDesc *last_user_buf = NULL;
    UserBufferDesc * user_buf = NULL;

#endif
    /*
     * If given a strategy object, see whether it can select a buffer. We
     * assume strategy objects don't need buffer_strategy_lock.
     */
    if (strategy != NULL) {
        buf = GetBufferFromRing(strategy, buf_state);
        if (buf != NULL) {
            return buf;
        }
    }

    /*
     * If asked, we need to waken the bgwriter. Since we don't want to rely on
     * a spinlock for this we force a read from shared memory once, and then
     * set the latch based on that value. We need to go through that length
     * because otherwise bgprocno might be reset while/after we check because
     * the compiler might just reread from memory.
     *
     * This can possibly set the latch of the wrong process if the bgwriter
     * dies in the wrong moment. But since PGPROC->procLatch is never
     * deallocated the worst consequence of that is that we set the latch of
     * some arbitrary process.
     */
    bgwproc_no = INT_ACCESS_ONCE(t_thrd.storage_cxt.StrategyControl->bgwprocno);
    if (bgwproc_no != -1) {
        /* reset bgwprocno first, before setting the latch */
        t_thrd.storage_cxt.StrategyControl->bgwprocno = -1;

        /*
         * Not acquiring ProcArrayLock here which is slightly icky. It's
         * actually fine because procLatch isn't ever freed, so we just can
         * potentially set the wrong process' (or no process') latch.
         */
        SetLatch(&g_instance.proc_base_all_procs[bgwproc_no]->procLatch);
    }

#ifdef UBRL
    if (t_thrd.storage_cxt.StrategyControl->numBufferAllocs % 1000 == 0) {
        update_all_topk_user();
    }
    int victim_user_slot = user_slot;
    int real_victim = 0;

    Assert(victim_user_slot != -1);

#endif
    /*
     * We count buffer allocation requests so that the bgwriter can estimate
     * the rate of buffer consumption.	Note that buffers recycled by a
     * strategy object are intentionally not counted here.
     */
    (void)pg_atomic_fetch_add_u32(&t_thrd.storage_cxt.StrategyControl->numBufferAllocs, 1);

    /* Check the Candidate list */
    if (ENABLE_INCRE_CKPT && pg_atomic_read_u32(&g_instance.ckpt_cxt_ctl->current_page_writer_count) > 1) {
        if (NEED_CONSIDER_USECOUNT) {
            const uint32 MAX_RETRY_SCAN_CANDIDATE_LISTS = 5;
            const int MILLISECOND_TO_MICROSECOND = 1000;
            uint64 maxSleep = u_sess->attr.attr_storage.BgWriterDelay * MILLISECOND_TO_MICROSECOND;
            uint64 sleepTime = 1000L;
            uint32 retry_times = 0;
            while (retry_times < MAX_RETRY_SCAN_CANDIDATE_LISTS) {
#ifdef UBRL
                buf = get_buf_from_user_candidate_list(strategy, victim_user_slot, buf_state);
#else
                buf = get_buf_from_candidate_list(strategy, buf_state);
#endif
                if (buf != NULL) {
                    (void)pg_atomic_fetch_add_u64(&g_instance.ckpt_cxt_ctl->get_buf_num_candidate_list, 1);
                    return buf;
                }
                pg_usleep(sleepTime);
                sleepTime = Min(sleepTime * 2, maxSleep);
                retry_times++;
            }
        } else {
#ifdef UBRL
            buf = get_buf_from_user_candidate_list(strategy, victim_user_slot, buf_state);
#else
            buf = get_buf_from_candidate_list(strategy, buf_state);
#endif            
            if (buf != NULL) {
                (void)pg_atomic_fetch_add_u64(&g_instance.ckpt_cxt_ctl->get_buf_num_candidate_list, 1);
                return buf;
            }
        }
    }

retry:
    /* Nothing on the freelist, so run the "clock sweep" algorithm */
    if (am_standby)
        max_buffer_can_use = int(NORMAL_SHARED_BUFFER_NUM * u_sess->attr.attr_storage.shared_buffers_fraction);
    else
        max_buffer_can_use = NORMAL_SHARED_BUFFER_NUM;
    try_counter = max_buffer_can_use;
    int try_get_loc_times = max_buffer_can_use;
    for (;;) {
#ifdef UBRL
        // S_LOCK(&victim_user_info->buffer_link_lock);
        // if (last_buf != NULL) {
        //     last_user_buf = GetUserBufferDescriptor(last_buf->buf_id);
        //     strategy_insert_tail(last_user_buf, LRU, user_slot);
        //     strategy_insert_tail(last_user_buf, MRU, user_slot);
        // }
        // buf_id = t_thrd.storage_cxt.UserStrategyControl[user_slot].BufferInfo[strategy_id].head;
        // user_buf = GetUserBufferDescriptor(buf_id);
        // // remove it from all list
        // // STRAT_LIST_REMOVE(user_buf, LRU, user_slot);
        // // STRAT_LIST_REMOVE(user_buf, MRU, user_slot);
        // strategy_list_remove(user_buf, LRU, user_slot);
        // strategy_list_remove(user_buf, MRU, user_slot);
    
        // S_UNLOCK(&victim_user_info->buffer_link_lock);
        
        buf = GetBufferDescriptor(getBufferByStrategy(user_slot, strategy_id ));
        // last_buf = buf;
#else
        buf = GetBufferDescriptor(ClockSweepTick(max_buffer_can_use));
#endif
        /*
         * If the buffer is pinned, we cannot use it.
         */
        if (!retryLockBufHdr(buf, &local_buf_state)) {
            if (--try_get_loc_times == 0) {
                ereport(WARNING,
                        (errmsg("try get buf headr lock times equal to maxNBufferCanUse when StrategyGetBuffer")));
                try_get_loc_times = max_buffer_can_use;
            }
            perform_delay(&retry_lock_status);
            continue;
        }

        retry_lock_status.retry_times = 0;
        if (BUF_STATE_GET_REFCOUNT(local_buf_state) == 0 && !(local_buf_state & BM_IS_META) &&
            (backend_can_flush_dirty_page() || !(local_buf_state & BM_DIRTY))) {
            /* Found a usable buffer */
            if (strategy != NULL)
                AddBufferToRing(strategy, buf);
            *buf_state = local_buf_state;
            (void)pg_atomic_fetch_add_u64(&g_instance.ckpt_cxt_ctl->get_buf_num_clock_sweep, 1);
#ifdef UBRL
            user_buf = GetUserBufferDescriptor(buf->buf_id);
            user_buf->strategy_id = strategy_id;
            user_buf->removed = true;
#endif
            return buf;
        } else if (--try_counter == 0) {
            /*
             * We've scanned all the buffers without making any state changes,
             * so all the buffers are pinned (or were when we looked at them).
             * We could hope that someone will free one eventually, but it's
             * probably better to fail than to risk getting stuck in an
             * infinite loop.
             */
            UnlockBufHdr(buf, local_buf_state);

            if (am_standby && u_sess->attr.attr_storage.shared_buffers_fraction < 1.0) {
                ereport(WARNING, (errmsg("no unpinned buffers available")));
                u_sess->attr.attr_storage.shared_buffers_fraction =
                    Min(u_sess->attr.attr_storage.shared_buffers_fraction + 0.1, 1.0);
                goto retry;
            } else if (dw_page_writer_running()) {
                ereport(LOG, (errmsg("double writer is on, no buffer available, this buffer dirty is %lu, "
                                     "this buffer refcount is %lu, now dirty page num is %ld",
                                     (local_buf_state & BM_DIRTY), BUF_STATE_GET_REFCOUNT(local_buf_state),
                                     get_dirty_page_num())));
                perform_delay(&retry_buf_status);
                goto retry;
            } else if (t_thrd.storage_cxt.is_btree_split) {
                ereport(WARNING, (errmsg("no unpinned buffers available when btree insert parent")));
                goto retry;
            } else
                ereport(ERROR, (errcode(ERRCODE_INVALID_BUFFER), (errmsg("no unpinned buffers available"))));
        }
        UnlockBufHdr(buf, local_buf_state);
        perform_delay(&retry_buf_status);
    }

    /* not reached */
    return NULL;
}

/*
 * StrategySyncStart -- tell BufferSync where to start syncing
 *
 * The result is the buffer index of the best buffer to sync first.
 * BufferSync() will proceed circularly around the buffer array from there.
 *
 * In addition, we return the completed-pass count (which is effectively
 * the higher-order bits of nextVictimBuffer) and the count of recent buffer
 * allocs if non-NULL pointers are passed.	The alloc count is reset after
 * being read.
 */
int StrategySyncStart(uint32 *complete_passes, uint32 *num_buf_alloc)
{
    uint32 next_victim_buffer;
    int result;

    SpinLockAcquire(&t_thrd.storage_cxt.StrategyControl->buffer_strategy_lock);
    next_victim_buffer = pg_atomic_read_u32(&t_thrd.storage_cxt.StrategyControl->nextVictimBuffer);
    result = ((int) next_victim_buffer) % NORMAL_SHARED_BUFFER_NUM;

    if (complete_passes != NULL) {
        *complete_passes = t_thrd.storage_cxt.StrategyControl->completePasses;
        /*
         * Additionally add the number of wraparounds that happened before
         * completePasses could be incremented. C.f. ClockSweepTick().
         */
        *complete_passes += next_victim_buffer / (unsigned int) NORMAL_SHARED_BUFFER_NUM;
    }

    if (num_buf_alloc != NULL) {
        *num_buf_alloc = pg_atomic_exchange_u32(&t_thrd.storage_cxt.StrategyControl->numBufferAllocs, 0);
    }
    SpinLockRelease(&t_thrd.storage_cxt.StrategyControl->buffer_strategy_lock);
    return result;
}

/*
 * StrategyNotifyBgWriter -- set or clear allocation notification latch
 *
 * If bgwriterLatch isn't NULL, the next invocation of StrategyGetBuffer will
 * set that latch.	Pass NULL to clear the pending notification before it
 * happens.  This feature is used by the bgwriter process to wake itself up
 * from hibernation, and is not meant for anybody else to use.
 */
void StrategyNotifyBgWriter(int bgwproc_no)
{
    /*
     * We acquire the BufFreelistLock just to ensure that the store appears
     * atomic to StrategyGetBuffer.  The bgwriter should call this rather
     * infrequently, so there's no performance penalty from being safe.
     */
    SpinLockAcquire(&t_thrd.storage_cxt.StrategyControl->buffer_strategy_lock);
    t_thrd.storage_cxt.StrategyControl->bgwprocno = bgwproc_no;
    SpinLockRelease(&t_thrd.storage_cxt.StrategyControl->buffer_strategy_lock);
}

/*
 * StrategyShmemSize
 *
 * estimate the size of shared memory used by the freelist-related structures.
 *
 * Note: for somewhat historical reasons, the buffer lookup hashtable size
 * is also determined here.
 */
Size StrategyShmemSize(void)
{
    Size size = 0;

    /* size of lookup hash table ... see comment in StrategyInitialize */
#ifdef UBRL
    size = add_size(size, BufTableShmemSize(TOTAL_BUFFER_NUM + NUM_BUFFER_PARTITIONS + NORMAL_SHARED_BUFFER_NUM * NUM_STRATEGY));
#else
    size = add_size(size, BufTableShmemSize(TOTAL_BUFFER_NUM + NUM_BUFFER_PARTITIONS));
#endif

    /* size of the shared replacement strategy control block */
    size = add_size(size, MAXALIGN(sizeof(BufferStrategyControl)));

    return size;
}

/*
 * StrategyInitialize -- initialize the buffer cache replacement
 *		strategy.
 *
 * Assumes: All of the buffers are already built into a linked list.
 *		Only called by postmaster and only during initialization.
 */
void StrategyInitialize(bool init)
{
    bool found = false;
#ifdef UBRL
    bool found_user_strategy = false;

    UserInfo* user_info;
    uint64 buf_num = 0;
    UserBufferDesc* user_buffer = NULL; 

#endif
    ereport(WARNING, (errmsg_internal("StrategyInitialize")));

    /*
     * Initialize the shared buffer lookup hashtable.
     *
     * Since we can't tolerate running out of lookup table entries, we must be
     * sure to specify an adequate table size here.  The maximum steady-state
     * usage is of course NBuffers entries, but BufferAlloc() tries to insert
     * a new entry before deleting the old.  In principle this could be
     * happening in each partition concurrently, so we could need as many as
     * NBuffers + NUM_BUFFER_PARTITIONS entries.
     */
#ifdef UNUSED
    /* victim history is in this buf table*/
    InitBufTable(TOTAL_BUFFER_NUM + NORMAL_SHARED_BUFFER_NUM + NUM_BUFFER_PARTITIONS);
#else
    InitBufTable(TOTAL_BUFFER_NUM + NUM_BUFFER_PARTITIONS);
#endif
    /*
     * Get or create the shared strategy control block
     */
    t_thrd.storage_cxt.StrategyControl =
        (BufferStrategyControl *)ShmemInitStruct("Buffer Strategy Status", sizeof(BufferStrategyControl), &found);
#ifdef UBRL
    t_thrd.storage_cxt.UserStrategyControl =
        (UserStrategy *)ShmemInitStruct("User Buffer Strategy Status", sizeof(UserStrategy)*USER_SLOT_NUM, &found_user_strategy);

#endif

#ifdef UBRL
    /* Initialize the user's victim history hash table */
    // for (int i = 0; i < USER_SLOT_NUM; i++) {
    //     user_info = &g_instance.ckpt_cxt_ctl->user_buffer_info->user_info[i];
    //     S_LOCK(&user_info->user_lock);
    //     if (user_info->user_buffer_bought > 0) {
        InitUserVictimHisTable(NORMAL_SHARED_BUFFER_NUM + NUM_BUFFER_PARTITIONS);
        // }
        // S_UNLOCK(&user_info->user_lock);
    // }
#endif

    if (!found) {
        /*
         * Only done once, usually in postmaster
         */
        Assert(init);
        SpinLockInit(&t_thrd.storage_cxt.StrategyControl->buffer_strategy_lock);

        /* Initialize the clock sweep pointer */
        pg_atomic_init_u32(&t_thrd.storage_cxt.StrategyControl->nextVictimBuffer, 0);

        /* Clear statistics */
        t_thrd.storage_cxt.StrategyControl->completePasses = 0;
        pg_atomic_init_u32(&t_thrd.storage_cxt.StrategyControl->numBufferAllocs, 0);

        /* No pending notification */
        t_thrd.storage_cxt.StrategyControl->bgwprocno = -1;
    } else {
        Assert(!init);
    }
#ifdef UBRL

    if (!found_user_strategy) {
        ereport(WARNING, (errmsg_internal("Init user buffer strategy information")));
        for (int i = 0; i < USER_SLOT_NUM; i++) {
            user_info = &g_instance.ckpt_cxt_ctl->user_buffer_info->user_info[i];
            pg_atomic_write_u32(&user_info->numBufferAllocs, user_info->user_buffer_bought);
            pg_atomic_init_u32(&t_thrd.storage_cxt.UserStrategyControl[i].next_history, buf_num);

            /*Initialize the reinforcement learning information for each use i*/
            for (int j = 0; j < NUM_STRATEGY; j++)
                t_thrd.storage_cxt.UserStrategyControl[i].Strategy.weight[j] = (1.0 / (double) NUM_STRATEGY);

            for (int k = 0; k < 3; k++) {
                t_thrd.storage_cxt.UserStrategyControl[i].Strategy.lr[k] = 0.0;
                t_thrd.storage_cxt.UserStrategyControl[i].Strategy.hit_rate[k] = 0.0;
            }

            t_thrd.storage_cxt.UserStrategyControl[i].Strategy.unlearnCount = 0;

            /*alloc buffer for each user */
            /*Initialize the RL buffer information*/

            for (int j = 0; j < NUM_STRATEGY; j++) {
                // so the total num of history list for each user is NUM_STRATEGY * buffer_bought
                t_thrd.storage_cxt.UserStrategyControl[i].BufferInfo[j].head = buf_num;
                t_thrd.storage_cxt.UserStrategyControl[i].BufferInfo[j].tail = buf_num + user_info->user_buffer_bought - 1;
                pg_atomic_init_u32(&t_thrd.storage_cxt.UserStrategyControl[i].BufferInfo[j].nextVictimBuffer, buf_num);
                for (int k = 0; k < user_info->user_buffer_bought; k++) {
                    user_buffer = GetUserBufferDescriptor(k + buf_num);
                    user_buffer->links[j].next = k + buf_num + 1;
                    user_buffer->links[j].prev = k + buf_num - 1;
                }
                // this is a circle
                GetUserBufferDescriptor(buf_num)->links[j].prev = buf_num + user_info->user_buffer_bought - 1;
                GetUserBufferDescriptor(buf_num + user_info->user_buffer_bought - 1)->links[j].next = buf_num;
            }
            for (int j = 0; j < user_info->user_buffer_bought; j++) {
                user_buffer = GetUserBufferDescriptor(j + buf_num);
                user_buffer->buf_id = j + buf_num;
                user_buffer->user_slot = i;
                user_buffer->removed = false;
                user_buffer->strategy_id = 0; // default is LRU
                SpinLockInit(&user_buffer->user_buffer_lock);

            }

            buf_num += user_info->user_buffer_bought;

            t_thrd.storage_cxt.UserStrategyControl[i].listFreeBuffers = 0;
        }
    }else {
        ereport(WARNING, (errmsg_internal("Found user buffer strategy information")));
        Assert(!init);
    }
    bool rl_his_found = false;
    char shmem_name[64];
    int current_number = 0;
    sprintf(shmem_name, "Buffer Strategy History");

    t_thrd.storage_cxt.BufVictimHistory = 
        (UserStrategyVictimHistory *)ShmemInitStruct(shmem_name, 
            sizeof(UserStrategyVictimHistory) * (NORMAL_SHARED_BUFFER_NUM), &rl_his_found);
    if (!rl_his_found) {

        for (int i = 0; i < USER_SLOT_NUM; i++) {
            /*initialize the history list for each strategy j of each user i  */

            user_info = &g_instance.ckpt_cxt_ctl->user_buffer_info->user_info[i];
            if (user_info->user_buffer_bought == 0)
                continue;
            // for (int j = 0; j < NUM_STRATEGY; j++) {
            // sprintf(shmem_name, "Buffer Strategy History %d", i);
            // each user has a history list for each strategy of it bought buffer number
            // t_thrd.storage_cxt.BufVictimHistory[i] = 
            //     (UserStrategyVictimHistory *)ShmemInitStruct(shmem_name, 
            //         sizeof(UserStrategyVictimHistory) * (user_info->user_buffer_bought), &rl_his_found);
            // t_thrd.storage_cxt.UserStrategyControl[i].history_head = 0;
            // t_thrd.storage_cxt.UserStrategyControl[i].history_tail = user_info->user_buffer_bought - 1;
            for (int k = 0; k < user_info->user_buffer_bought; k++) {
                int index = current_number + k;
                t_thrd.storage_cxt.BufVictimHistory[index].next = index + 1;
                t_thrd.storage_cxt.BufVictimHistory[index].prev = index - 1;
                t_thrd.storage_cxt.BufVictimHistory[index].strategy = -1;
                CLEAR_BUFFERTAG(t_thrd.storage_cxt.BufVictimHistory[index].buf_tag);
                t_thrd.storage_cxt.BufVictimHistory[index].buf_hash = 0;
                t_thrd.storage_cxt.BufVictimHistory[index].id = index;
                t_thrd.storage_cxt.BufVictimHistory[index].valid = false;
                SpinLockInit(&t_thrd.storage_cxt.BufVictimHistory[index].lock);
            }
            t_thrd.storage_cxt.BufVictimHistory[current_number].prev = user_info->user_buffer_bought - 1 + current_number;
            t_thrd.storage_cxt.BufVictimHistory[current_number + user_info->user_buffer_bought - 1].next = current_number;
            current_number += user_info->user_buffer_bought;
        
        }

    }
#endif

}

const int MIN_REPAIR_FILE_SLOT_NUM = 32;
/* ----------------------------------------------------------------
 *				Backend-private buffer ring management
 * ----------------------------------------------------------------
 */
/*
 * GetAccessStrategy -- create a BufferAccessStrategy object
 *
 * The object is allocated in the current memory context.
 */
BufferAccessStrategy GetAccessStrategy(BufferAccessStrategyType btype)
{
    BufferAccessStrategy strategy;
    int ring_size;

    /*
     * Select ring size to use.  See buffer/README for rationales.
     *
     * Note: if you change the ring size for BAS_BULKREAD, see also
     * SYNC_SCAN_REPORT_INTERVAL in access/heap/syncscan.c.
     */
    switch (btype) {
        case BAS_NORMAL:
            /* if someone asks for NORMAL, just give 'em a "default" object */
            return NULL;

        case BAS_BULKREAD:
            ring_size = int(int64(u_sess->attr.attr_storage.bulk_read_ring_size) * 1024 / BLCKSZ);
            break;
        case BAS_BULKWRITE:
            ring_size = (u_sess->attr.attr_storage.bulk_write_ring_size / BLCKSZ) * 1024;
            break;
        case BAS_VACUUM:
            ring_size = g_instance.attr.attr_storage.NBuffers / 32 /
                Max(g_instance.attr.attr_storage.autovacuum_max_workers, 1);
            break;
        case BAS_REPAIR:
            ring_size = Min(g_instance.attr.attr_storage.NBuffers, MIN_REPAIR_FILE_SLOT_NUM);
            break;
        default:
            ereport(ERROR, (errcode(ERRCODE_INVALID_OPERATION),
                            (errmsg("unrecognized buffer access strategy: %d", (int)btype))));
            return NULL; /* keep compiler quiet */
    }

    /* If the shared buffers is too small, make sure ring size not equal zero. */
    ring_size = Max(ring_size, 4);

    /* Make sure ring isn't an undue fraction of shared buffers */
    if (btype != BAS_BULKWRITE && btype != BAS_BULKREAD)
        ring_size = Min(g_instance.attr.attr_storage.NBuffers / 8, ring_size);
    else
        ring_size = Min(g_instance.attr.attr_storage.NBuffers / 4, ring_size);

    /* Allocate the object and initialize all elements to zeroes */
    strategy = (BufferAccessStrategy)palloc0(offsetof(BufferAccessStrategyData, buffers) + ring_size * sizeof(Buffer));

    /* Set fields that don't start out zero */
    strategy->btype = btype;
    strategy->ring_size = ring_size;
    strategy->flush_rate = Min(u_sess->attr.attr_storage.backwrite_quantity, ring_size);

    return strategy;
}

/*
 * FreeAccessStrategy -- release a BufferAccessStrategy object
 *
 * A simple pfree would do at the moment, but we would prefer that callers
 * don't assume that much about the representation of BufferAccessStrategy.
 */
void FreeAccessStrategy(BufferAccessStrategy strategy)
{
    /* don't crash if called on a "default" strategy */
    if (strategy != NULL) {
        pfree(strategy);
        strategy = NULL;
    }
}

const int MAX_RETRY_RING_TIMES = 100;
const float MAX_RETRY_RING_PCT = 0.1;
/*
 * GetBufferFromRing -- returns a buffer from the ring, or NULL if the
 *		ring is empty.
 *
 * The bufhdr spin lock is held on the returned buffer.
 */
static BufferDesc *GetBufferFromRing(BufferAccessStrategy strategy, uint64 *buf_state)
{
    BufferDesc *buf = NULL;
    Buffer buf_num;
    uint64 local_buf_state; /* to avoid repeated (de-)referencing */
    uint16 retry_times = 0;

RETRY:
    /* Advance to next ring slot */
    if (++strategy->current >= strategy->ring_size)
        strategy->current = 0;
    retry_times++;

    ADIO_RUN()
    {
        /*
         * Flush out buffers asynchronously from behind the current slot.
         * This is a kludge because the PageListBackWrite() is not strictly
         * asynchronous and this function really shouldn't be doing the actual I/O.
         */
        if (AioCompltrIsReady() &&
            ((strategy->btype == BAS_BULKWRITE) && (strategy->current % strategy->flush_rate == 0))) {
            if (strategy->current == 0) {
                if (strategy->buffers[strategy->ring_size - strategy->flush_rate] != InvalidBuffer) {
                    PageListBackWrite((uint32 *)&strategy->buffers[strategy->ring_size - strategy->flush_rate],
                                      strategy->flush_rate, STRATEGY_BACKWRITE, NULL, NULL, NULL);
                    ereport(DEBUG1,
                            (errmodule(MOD_ADIO), errmsg("BufferRingBackWrite, start(%d) count(%d)",
                                                         strategy->buffers[strategy->ring_size - strategy->flush_rate],
                                                         strategy->flush_rate)));
                }
            } else {
                PageListBackWrite((uint32 *)&strategy->buffers[strategy->current - strategy->flush_rate],
                                  strategy->flush_rate, STRATEGY_BACKWRITE, NULL, NULL, NULL);
                ereport(DEBUG1,
                        (errmodule(MOD_ADIO),
                         errmsg("BufferRingBackWrite, start(%d) count(%d)",
                                strategy->buffers[strategy->current - strategy->flush_rate], strategy->flush_rate)));
            }
        }
    }
    ADIO_END();

    /*
     * If the slot hasn't been filled yet, tell the caller to allocate a new
     * buffer with the normal allocation strategy.	He will then fill this
     * slot by calling AddBufferToRing with the new buffer.
     */
    buf_num = strategy->buffers[strategy->current];
    if (buf_num == InvalidBuffer) {
        strategy->current_was_in_ring = false;
        return NULL;
    }

    /*
     * If the buffer is pinned we cannot use it under any circumstances.
     *
     * If usage_count is 0 or 1 then the buffer is fair game (we expect 1,
     * since our own previous usage of the ring element would have left it
     * there, but it might've been decremented by clock sweep since then). A
     * higher usage_count indicates someone else has touched the buffer, so we
     * shouldn't re-use it.
     */
    buf = GetBufferDescriptor(buf_num - 1);
    if (pg_atomic_read_u64(&buf->state) & (BM_DIRTY | BM_IS_META)) {
        if (retry_times < Min(MAX_RETRY_RING_TIMES, strategy->ring_size * MAX_RETRY_RING_PCT)) {
            goto RETRY;
        } else if (get_curr_candidate_nums(CAND_LIST_NORMAL) >= (uint32)g_instance.attr.attr_storage.NBuffers *
            u_sess->attr.attr_storage.candidate_buf_percent_target){
            strategy->current_was_in_ring = false;
            return NULL;
        }
    }

    local_buf_state = LockBufHdr(buf);
    if (BUF_STATE_GET_REFCOUNT(local_buf_state) == 0 && BUF_STATE_GET_USAGECOUNT(local_buf_state) <= 1 &&
        (backend_can_flush_dirty_page() || !(local_buf_state & BM_DIRTY)) &&
        !(local_buf_state & BM_IS_META)) {
        strategy->current_was_in_ring = true;
        *buf_state = local_buf_state;
        return buf;
    }

    UnlockBufHdr(buf, local_buf_state);
    /*
     * Tell caller to allocate a new buffer with the normal allocation
     * strategy.  He'll then replace this ring element via AddBufferToRing.
     */
    strategy->current_was_in_ring = false;
    return NULL;
}

/*
 * AddBufferToRing -- add a buffer to the buffer ring
 *
 * Caller must hold the buffer header spinlock on the buffer.  Since this
 * is called with the spinlock held, it had better be quite cheap.
 */
static void AddBufferToRing(BufferAccessStrategy strategy, volatile BufferDesc *buf)
{
    strategy->buffers[strategy->current] = BufferDescriptorGetBuffer(buf);
}

/*
 * StrategyRejectBuffer -- consider rejecting a dirty buffer
 *
 * When a nondefault strategy is used, the buffer manager calls this function
 * when it turns out that the buffer selected by StrategyGetBuffer needs to
 * be written out and doing so would require flushing WAL too.	This gives us
 * a chance to choose a different victim.
 *
 * Returns true if buffer manager should ask for a new victim, and false
 * if this buffer should be written and re-used.
 */
bool StrategyRejectBuffer(BufferAccessStrategy strategy, BufferDesc *buf)
{
    /* We only do this in bulkread mode */
    if (strategy->btype != BAS_BULKREAD)
        return false;

    /* Don't muck with behavior of normal buffer-replacement strategy */
    if (!strategy->current_was_in_ring || strategy->buffers[strategy->current] != BufferDescriptorGetBuffer(buf))
        return false;

    /*
     * Remove the dirty buffer from the ring; necessary to prevent infinite
     * loop if all ring members are dirty.
     */
    strategy->buffers[strategy->current] = InvalidBuffer;

    return true;
}

void StrategyGetRingPrefetchQuantityAndTrigger(BufferAccessStrategy strategy, int *quantity, int *trigger)
{
    int threshold;
    int prefetch_trigger = u_sess->attr.attr_storage.prefetch_quantity;

    if (strategy == NULL || strategy->btype != BAS_BULKREAD) {
        return;
    }
    threshold = strategy->ring_size / 4;
    if (quantity != NULL) {
        *quantity = (threshold > u_sess->attr.attr_storage.prefetch_quantity)
                        ? u_sess->attr.attr_storage.prefetch_quantity
                        : threshold;
    }
    if (trigger != NULL) {
        *trigger = (threshold > prefetch_trigger) ? prefetch_trigger : threshold;
    }
}

void wakeup_pagewriter_thread()
{
    PageWriterProc *pgwr = &g_instance.ckpt_cxt_ctl->pgwr_procs.writer_proc[0];
    /* The current candidate list is empty, wake up the buffer writer. */
    if (pgwr->proc != NULL) {
        SetLatch(&pgwr->proc->procLatch);
    }
    return;
}

const int CANDIDATE_DIRTY_LIST_LEN = 100;
const float HIGH_WATER = 0.75;
static BufferDesc* get_buf_from_candidate_list(BufferAccessStrategy strategy, uint64* buf_state)
{
    BufferDesc* buf = NULL;
    uint64 local_buf_state;
    int buf_id = 0;
    int list_num = g_instance.ckpt_cxt_ctl->pgwr_procs.sub_num;
    int list_id = 0;
    volatile PgBackendStatus* beentry = t_thrd.shemem_ptr_cxt.MyBEEntry;
    Buffer *candidate_dirty_list = NULL;
    int dirty_list_num = 0;
    bool enable_available = false;
    bool need_push_dirst_list = false;
    bool need_scan_dirty =
        (g_instance.ckpt_cxt_ctl->actual_dirty_page_num / (float)(g_instance.attr.attr_storage.NBuffers) > HIGH_WATER)
        && backend_can_flush_dirty_page();
    if (need_scan_dirty) {
        /*Not return the dirty page when there are few dirty pages */
        candidate_dirty_list = (Buffer*)palloc0(sizeof(Buffer) * CANDIDATE_DIRTY_LIST_LEN);
    }

    list_id = beentry->st_tid > 0 ? (beentry->st_tid % list_num) : (beentry->st_sessionid % list_num);

    for (int i = 0; i < list_num; i++) {
        /* the pagewriter sub thread store normal buffer pool, sub thread starts from 1 */
        int thread_id = (list_id + i) % list_num + 1;
        Assert(thread_id > 0 && thread_id <= list_num);
        while (candidate_buf_pop(&g_instance.ckpt_cxt_ctl->pgwr_procs.writer_proc[thread_id].normal_list, &buf_id)) {
            Assert(buf_id < SegmentBufferStartID);
            buf = GetBufferDescriptor(buf_id);
            local_buf_state = LockBufHdr(buf);

            if (g_instance.ckpt_cxt_ctl->candidate_free_map[buf_id]) {
                g_instance.ckpt_cxt_ctl->candidate_free_map[buf_id] = false;
                enable_available = BUF_STATE_GET_REFCOUNT(local_buf_state) == 0 && !(local_buf_state & BM_IS_META);
                need_push_dirst_list = need_scan_dirty && dirty_list_num < CANDIDATE_DIRTY_LIST_LEN &&
                        free_space_enough(buf_id);
                if (enable_available) {
                    if (NEED_CONSIDER_USECOUNT && BUF_STATE_GET_USAGECOUNT(local_buf_state) != 0) {
                        local_buf_state -= BUF_USAGECOUNT_ONE;
                    } else if (!(local_buf_state & BM_DIRTY)) {
                        if (strategy != NULL) {
                            AddBufferToRing(strategy, buf);
                        }
                        *buf_state = local_buf_state;
                        if (candidate_dirty_list != NULL) {
                            pfree(candidate_dirty_list);
                        }
                        return buf;
                    } else if (need_push_dirst_list) {
                        candidate_dirty_list[dirty_list_num++] = buf_id;
                    }
                }
            }
            UnlockBufHdr(buf, local_buf_state);
        }
    }

    wakeup_pagewriter_thread();

    if (need_scan_dirty) {
        for (int i = 0; i < dirty_list_num; i++) {
            buf_id = candidate_dirty_list[i];
            buf = GetBufferDescriptor(buf_id);
            local_buf_state = LockBufHdr(buf);
            enable_available = (BUF_STATE_GET_REFCOUNT(local_buf_state) == 0) && !(local_buf_state & BM_IS_META)
                && free_space_enough(buf_id);
            if (enable_available) {
                if (strategy != NULL) {
                    AddBufferToRing(strategy, buf);
                }
                *buf_state = local_buf_state;
                pfree(candidate_dirty_list);
                return buf;
            }
            UnlockBufHdr(buf, local_buf_state);
        }
    }

    if (candidate_dirty_list != NULL) {
        pfree(candidate_dirty_list);
    }
    return NULL;
}

BufferDesc *SSTryGetBuffer(uint64 times, uint64 *buf_state)
{
    int max_buffer_can_use = NORMAL_SHARED_BUFFER_NUM;
    int try_times = times;
    uint64 local_buf_state;
    BufferDesc *buf = NULL;
    for (int i = 0; i < try_times; i++) {
        buf = GetBufferDescriptor(ClockSweepTick(max_buffer_can_use));

        if (!retryLockBufHdr(buf, &local_buf_state)) {
            return NULL;
        }

        if (BUF_STATE_GET_REFCOUNT(local_buf_state) == 0 && !(local_buf_state & BM_IS_META) &&
            (backend_can_flush_dirty_page() || !(local_buf_state & BM_DIRTY))) {
            *buf_state = local_buf_state;
            (void)pg_atomic_fetch_add_u64(&g_instance.ckpt_cxt_ctl->get_buf_num_clock_sweep, 1);
            return buf;
        }

        UnlockBufHdr(buf, local_buf_state);
    }

    return NULL;
}

#ifdef UBRL
static BufferDesc* get_buf_from_user_candidate_list(BufferAccessStrategy strategy, int user_slot, uint64* buf_state)
{
    BufferDesc* buf = NULL;
    UserBufferDesc * user_buf = NULL;
    uint64 local_buf_state;
    int buf_id = 0;
    int list_id = 0;
    UserInfo* user_info = &g_instance.ckpt_cxt_ctl->user_buffer_info->user_info[user_slot];
    int list_num = g_instance.ckpt_cxt_ctl->pgwr_procs.sub_num;
    volatile PgBackendStatus* beentry = t_thrd.shemem_ptr_cxt.MyBEEntry;
    list_id = beentry->st_tid > 0 ? (beentry->st_tid % list_num) : (beentry->st_sessionid % list_num);

    Buffer *candidate_dirty_list = NULL;
    int dirty_list_num = 0;
    bool enable_available = false;
    bool need_push_dirst_list = false;
    bool need_scan_dirty =
        (g_instance.ckpt_cxt_ctl->actual_dirty_page_num / (float)(g_instance.attr.attr_storage.NBuffers) > HIGH_WATER)
        && backend_can_flush_dirty_page();
    if (need_scan_dirty) {
        /*Not return the dirty page when there are few dirty pages */
        candidate_dirty_list = (Buffer*)palloc0(sizeof(Buffer) * CANDIDATE_DIRTY_LIST_LEN);
    }

    // if (user_slot == -1) {
    //     user_slot = t_thrd.storage_cxt.current_user_index;
    // }
    Assert(user_slot >= 0);
    for (int i = 0; i < list_num; i++) {
        /* the pagewriter sub thread store normal buffer pool, sub thread starts from 1 */
        int thread_id = i + 1;
        Assert(thread_id > 0 && thread_id <= list_num);
        while (candidate_buf_pop(&g_instance.ckpt_cxt_ctl->pgwr_procs.writer_proc[thread_id].user_list[user_slot], &buf_id)) {
        // while (candidate_buf_pop_user(user_slot, &buf_id)) {
            Assert(buf_id < SegmentBufferStartID);
            buf = GetBufferDescriptor(buf_id);
            local_buf_state = LockBufHdr(buf);
            user_buf = GetUserBufferDescriptor(buf_id);
            if (g_instance.ckpt_cxt_ctl->candidate_free_map[buf_id]) {
                g_instance.ckpt_cxt_ctl->candidate_free_map[buf_id] = false;

                enable_available = BUF_STATE_GET_REFCOUNT(local_buf_state) == 0 && !(local_buf_state & BM_IS_META);
                need_push_dirst_list = need_scan_dirty && dirty_list_num < CANDIDATE_DIRTY_LIST_LEN &&
                        free_space_enough(buf_id);
                if (enable_available && user_buf->user_slot == user_slot) {
                    if (NEED_CONSIDER_USECOUNT && BUF_STATE_GET_USAGECOUNT(local_buf_state) != 0) {
                        local_buf_state -= BUF_USAGECOUNT_ONE;
                    } else if (!(local_buf_state & BM_DIRTY)) {
                        if (strategy != NULL) {
                            AddBufferToRing(strategy, buf);
                        }
                        *buf_state = local_buf_state;
                        if (candidate_dirty_list != NULL) {
                            pfree(candidate_dirty_list);
                        }
                        // S_LOCK(&user_info->buffer_link_lock);
                        // // remove it from all list
                        // // STRAT_LIST_REMOVE(user_buf, LRU, user_slot);
                        // // STRAT_LIST_REMOVE(user_buf, MRU, user_slot);
                        // strategy_list_remove(user_buf, LRU, user_slot);
                        // strategy_list_remove(user_buf, MRU, user_slot);

                        // S_UNLOCK(&user_info->buffer_link_lock);
                        user_buf->strategy_id = LRU; //default as LRU
                        // if (*buf_state&BM_TAG_VALID) {
                        //     ereport(WARNING, ((errmsg("Get dirty buffer %u for user %d, tag is valid", buf_id, user_slot))));

                        // } else {
                        //     ereport(WARNING, ((errmsg("Get dirty buffer %u for user %d, tag is invalid", buf_id, user_slot))));
                        // }
                        return buf;
                    } else if (need_push_dirst_list) {
                        candidate_dirty_list[dirty_list_num++] = buf_id;
                    }
                }
            }
            UnlockBufHdr(buf, local_buf_state);
            // CleanUserBuffer(user_info->buffer_link_lock, buf);
        }
    }
    wakeup_pagewriter_thread();

    if (need_scan_dirty) {
        for (int i = 0; i < dirty_list_num; i++) {
            buf_id = candidate_dirty_list[i];
            buf = GetBufferDescriptor(buf_id);
            user_buf = GetUserBufferDescriptor(buf_id);
            local_buf_state = LockBufHdr(buf);
            enable_available = (BUF_STATE_GET_REFCOUNT(local_buf_state) == 0) && !(local_buf_state & BM_IS_META)
                && free_space_enough(buf_id);
            if (enable_available && user_buf->user_slot == user_slot) {
                if (strategy != NULL) {
                    AddBufferToRing(strategy, buf);
                }
                *buf_state = local_buf_state;
                pfree(candidate_dirty_list);
                // S_LOCK(&user_info->buffer_link_lock);
                // // remove it from all list
                // // STRAT_LIST_REMOVE(user_buf, LRU, user_slot);
                // // STRAT_LIST_REMOVE(user_buf, MRU, user_slot);

                // strategy_list_remove(user_buf, LRU, user_slot);
                // strategy_list_remove(user_buf, MRU, user_slot);
                // S_UNLOCK(&user_info->buffer_link_lock);
                user_buf->strategy_id = LRU; //default as LRU
                // if (*buf_state&BM_TAG_VALID) {
                //     ereport(WARNING, ((errmsg("Get dirty buffer %u for user %d, tag is valid", buf_id, user_slot))));

                // } else {
                //     ereport(WARNING, ((errmsg("Get dirty buffer %u for user %d, tag is invalid", buf_id, user_slot))));
                // }
                return buf;
            }
            UnlockBufHdr(buf, local_buf_state);
        }
    }

    if (candidate_dirty_list != NULL) {
        pfree(candidate_dirty_list);
    }
    return NULL;
}
#endif