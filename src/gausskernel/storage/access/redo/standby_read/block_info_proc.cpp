/*
 * Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *          http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * -------------------------------------------------------------------------
 *
 * block_info_proc.cpp
 *
 * IDENTIFICATION
 *    src/gausskernel/storage/recovery/parallel/blocklevel/standby_read/block_info_proc.cpp
 *
 * -------------------------------------------------------------------------
 */

#include <cassert>
#include "access/extreme_rto/standby_read/block_info_meta.h"
#include "access/extreme_rto/standby_read/lsn_info_meta.h"
#include "storage/smgr/relfilenode.h"

namespace extreme_rto_standby_read {

void block_info_page_init(Page page)
{
    static_assert(sizeof(BlockInfoPageHeader) == BLOCK_INFO_HEAD_SIZE, "BlockInfoPageHeader size is not 64 bytes");
    static_assert(sizeof(BlockMetaInfo) == BLOCK_INFO_SIZE, "BlockMetaInfo size is not 64 bytes");

    BlockInfoPageHeader* page_header = (BlockInfoPageHeader*)page;
    errno_t ret = memset_s(page_header, BLCKSZ, 0, BLCKSZ);
    securec_check(ret, "", "");
    page_header->flags |= BLOCK_INFO_PAGE_VALID_FLAG;
    page_header->version = BLOCK_INFO_PAGE_VERSION;
}

inline BlockNumber data_block_number_to_meta_page_number(BlockNumber block_num)
{
    return block_num / BLOCK_INFO_NUM_PER_PAGE;
}

inline uint32 block_info_meta_page_offset(BlockNumber block_num)
{
    return (block_num % BLOCK_INFO_NUM_PER_PAGE) * BLOCK_INFO_SIZE + BLOCK_INFO_HEAD_SIZE;
}

// get page, just have pin, no lock
BlockMetaInfo* get_block_meta_info_by_relfilenode(
    const BufferTag& buf_tag, BufferAccessStrategy strategy, ReadBufferMode mode, Buffer* buffer)
{
    RelFileNode standby_read_rnode = buf_tag.rnode;
    standby_read_rnode.spcNode = EXRTO_BLOCK_INFO_SPACE_OID;
    SMgrRelation smgr = smgropen(standby_read_rnode, InvalidBackendId);
    bool hit = false;

    BlockNumber meta_block_num = data_block_number_to_meta_page_number(buf_tag.blockNum);
    *buffer = ReadBuffer_common(smgr, 0, buf_tag.forkNum, meta_block_num, mode, strategy, &hit, NULL);

    if (*buffer == InvalidBuffer) {
        return NULL;
    }

    Page page = BufferGetPage(*buffer);
    if (!is_block_info_page_valid((BlockInfoPageHeader*)page)) {
        if (mode == RBM_NORMAL) {
            ReleaseBuffer(*buffer);
            return NULL;
        }
    }

    uint32 offset = block_info_meta_page_offset(buf_tag.blockNum);
    BlockMetaInfo *block_info = ((BlockMetaInfo*)(page + offset));
    if (!is_block_meta_info_valid(block_info) && mode == RBM_NORMAL) {
        ReleaseBuffer(*buffer);

        return NULL;
    }

    return block_info;
}

void init_block_info(BlockMetaInfo* block_info, XLogRecPtr max_lsn)
{
    errno_t ret = memset_s(block_info, BLOCK_INFO_SIZE, 0, BLOCK_INFO_SIZE);
    securec_check(ret, "", "");
    block_info->timeline = t_thrd.shemem_ptr_cxt.ControlFile->timeline;
    block_info->flags |= BLOCK_INFO_NODE_VALID_FLAG;
    lsn_info_list_init(&block_info->lsn_info_list);
    lsn_info_list_init(&block_info->base_page_info_list);
    block_info->max_lsn = max_lsn;  // just for update first base page info' lsn
    block_info->min_lsn = max_lsn;
}

void insert_lsn_to_block_info(
    StandbyReadMetaInfo* meta_info, const BufferTag& buf_tag, const Page base_page, XLogRecPtr next_lsn)
{
    Buffer block_info_buf = InvalidBuffer;
    BlockMetaInfo* block_info = get_block_meta_info_by_relfilenode(buf_tag, NULL, RBM_ZERO_ON_ERROR, &block_info_buf);
    if (unlikely(block_info == NULL || block_info_buf == InvalidBuffer)) {
        ereport(PANIC, (errmsg("insert lsn failed,block invalid %u/%u/%u %d %u", buf_tag.rnode.spcNode,
                               buf_tag.rnode.dbNode, buf_tag.rnode.relNode, buf_tag.forkNum, buf_tag.blockNum)));
    }
    LockBuffer(block_info_buf, BUFFER_LOCK_EXCLUSIVE);
    Page page = BufferGetPage(block_info_buf);
    XLogRecPtr current_page_lsn = PageGetLSN(base_page);
    if (!is_block_meta_info_valid(block_info)) {
        if (!is_block_info_page_valid((BlockInfoPageHeader*)page)) {
            block_info_page_init(page);
        }

        init_block_info(block_info, current_page_lsn);
    }

    if (block_info->record_num == 0 ||
        (block_info->record_num % (uint32)g_instance.attr.attr_storage.base_page_saved_interval) == 0) {
        insert_base_page_to_lsn_info(meta_info, &block_info->lsn_info_list, &block_info->base_page_info_list, buf_tag,
            base_page, current_page_lsn, next_lsn);
    } else {
        insert_lsn_to_lsn_info(meta_info, &block_info->lsn_info_list, next_lsn);
    }

    Assert(block_info->max_lsn <= next_lsn);
    block_info->max_lsn = next_lsn;

    ++(block_info->record_num);

    standby_read_meta_page_set_lsn(page, next_lsn);
    MarkBufferDirty(block_info_buf);
    UnlockReleaseBuffer(block_info_buf);
}

StandbyReadRecyleState recyle_block_info(
    const BufferTag& buf_tag, LsnInfoPosition base_page_info_pos, XLogRecPtr next_base_page_lsn, XLogRecPtr recyle_lsn)
{
    Buffer buffer = InvalidBuffer;
    BlockMetaInfo* block_meta_info = get_block_meta_info_by_relfilenode(buf_tag, NULL, RBM_NORMAL, &buffer);
    if ((block_meta_info == NULL) || (buffer == InvalidBuffer)) {
        // no block info, should not at this branch
        ereport(WARNING, (errmsg("block meta is invalid %u/%u/%u %d %u", buf_tag.rnode.spcNode, buf_tag.rnode.dbNode,
                                  buf_tag.rnode.relNode, buf_tag.forkNum, buf_tag.blockNum)));
        return STANDBY_READ_RECLYE_ALL;
    }
    StandbyReadRecyleState stat = STANDBY_READ_RECLYE_NONE;
    LockBuffer(buffer, BUFFER_LOCK_EXCLUSIVE);
    Assert(((block_meta_info->flags & BLOCK_INFO_NODE_VALID_FLAG) == BLOCK_INFO_NODE_VALID_FLAG));
    if (XLByteLT(block_meta_info->max_lsn, recyle_lsn)) {
        block_meta_info->flags &= ~BLOCK_INFO_NODE_VALID_FLAG;
        stat = STANDBY_READ_RECLYE_ALL;
        MarkBufferDirty(buffer);
    } else if (XLogRecPtrIsValid(next_base_page_lsn)) {
        LsnInfoPosition min_page_info_pos = LSN_INFO_LIST_HEAD;
        XLogRecPtr min_lsn = InvalidXLogRecPtr;
        recycle_one_lsn_info_list(buf_tag, base_page_info_pos, recyle_lsn, &min_page_info_pos, &min_lsn);

        Assert(INFO_POSITION_IS_VALID(min_page_info_pos));
        if (block_meta_info->base_page_info_list.next != min_page_info_pos) {
            block_meta_info->min_lsn = min_lsn;
            block_meta_info->lsn_info_list.next = min_page_info_pos;
            block_meta_info->base_page_info_list.next = min_page_info_pos;
            stat = STANDBY_READ_RECLYE_UPDATE;
            MarkBufferDirty(buffer);
        }
    }
    UnlockReleaseBuffer(buffer);
    return stat;
}

static void reset_tmp_lsn_info_array(StandbyReadLsnInfoArray* lsn_info)
{
    lsn_info->lsn_num = 0;
    lsn_info->base_page_lsn = InvalidXLogRecPtr;
    if (lsn_info->lsn_array == NULL) {
        uint32 max_save_nums = (uint32)g_instance.attr.attr_storage.base_page_saved_interval;
        lsn_info->lsn_array = (XLogRecPtr*)MemoryContextAlloc(
            THREAD_GET_MEM_CXT_GROUP(MEMORY_CONTEXT_STORAGE), sizeof(XLogRecPtr) * max_save_nums);
    }
}

bool get_page_lsn_info(const BufferTag& buf_tag, BufferAccessStrategy strategy, XLogRecPtr read_lsn,
    StandbyReadLsnInfoArray* lsn_info)
{
    Buffer buf;
    BlockMetaInfo* block_meta_info = get_block_meta_info_by_relfilenode(buf_tag, strategy, RBM_NORMAL, &buf);
    if (block_meta_info == NULL) {
        return false;
    }

    LockBuffer(buf, BUFFER_LOCK_SHARE);

    if (XLByteLT(read_lsn, block_meta_info->min_lsn)) {
        UnlockReleaseBuffer(buf);
        ereport(ERROR, (errcode(ERRCODE_INTERNAL_ERROR),
                        (errmsg("block old version can not found %u/%u/%u %d %u read lsn %lu, min lsn %lu",
                                buf_tag.rnode.spcNode, buf_tag.rnode.dbNode, buf_tag.rnode.relNode, buf_tag.forkNum,
                                buf_tag.blockNum, read_lsn, block_meta_info->min_lsn))));
        return false;
    }

    Assert(block_meta_info->base_page_info_list.prev != LSN_INFO_LIST_HEAD);
    reset_tmp_lsn_info_array(lsn_info);
    get_lsn_info_for_read(buf_tag, block_meta_info->base_page_info_list.prev, lsn_info, read_lsn);
    UnlockReleaseBuffer(buf);
    return true;
}

/*
 * recycle one block info file
 * rnode: database oid.
 */
void remove_one_block_info_file(const RelFileNode rnode)
{
    DropRelFileNodeShareBuffers(rnode, MAIN_FORKNUM, 0);
    DropRelFileNodeShareBuffers(rnode, FSM_FORKNUM, 0);
    DropRelFileNodeShareBuffers(rnode, VISIBILITYMAP_FORKNUM, 0);

    SMgrRelation srel = smgropen(rnode, InvalidBackendId);
    smgrdounlink(srel, true);
    smgrclose(srel);
}
/*
 * recycle all relation files when drop db occurs.
 * db_id: database oid.
 */
void remove_block_meta_info_files_of_db(Oid db_oid, Oid rel_oid)
{
    char pathbuf[EXRTO_FILE_PATH_LEN];
    char **filenames;
    char **filename;
    struct stat statbuf;
    /* get block info file directory */
    char exrto_block_info_dir[EXRTO_FILE_PATH_LEN] = {0};
    int rc = snprintf_s(exrto_block_info_dir, EXRTO_FILE_PATH_LEN, EXRTO_FILE_PATH_LEN - 1, "%s/%s", EXRTO_FILE_DIR,
                        EXRTO_FILE_SUB_DIR[BLOCK_INFO_META]);
    securec_check_ss(rc, "", "");
    /* get all files' name from block meta file directory */
    filenames = pgfnames(exrto_block_info_dir);
    if (filenames == NULL) {
        return;
    }
    char target_prefix[EXRTO_FILE_PATH_LEN] = {0};
    if (rel_oid != InvalidOid) {
        rc = sprintf_s(target_prefix, EXRTO_FILE_PATH_LEN, "%u_%u_", db_oid, rel_oid);
    } else {
        rc = sprintf_s(target_prefix, EXRTO_FILE_PATH_LEN, "%u_", db_oid);
    }
    securec_check_ss(rc, "", "");
    /* use the prefix name to match up files we want to delete */
    size_t prefix_len = strlen(target_prefix);
    for (filename = filenames; *filename != NULL; filename++) {
        char *fname = *filename;
        size_t fname_len = strlen(fname);
        /*
         * the length of prefix is less than the length of file name and must be the same under the same prefix_len
         */
        if (prefix_len >= fname_len || strncmp(target_prefix, fname, prefix_len) != 0) {
            continue;
        }
        rc =
            snprintf_s(pathbuf, EXRTO_FILE_PATH_LEN, EXRTO_FILE_PATH_LEN - 1, "%s/%s", exrto_block_info_dir, *filename);
        securec_check_ss(rc, "", "");
        /* may be can be some error */
        if (lstat(pathbuf, &statbuf) != 0) {
            if (errno != ENOENT) {
#ifndef FRONTEND
                ereport(WARNING, (errmsg("could not stat file or directory \"%s\" \n", pathbuf)));
#else
                fprintf(stderr, _("could not stat file or directory \"%s\": %s\n"), pathbuf, gs_strerror(errno));
#endif
            }
            continue;
        }
        /* if the file is a directory, don't touch it */
        if (S_ISDIR(statbuf.st_mode)) {
            /* skip dir */
            continue;
        }
        /* delete this file we found */
        if (unlink(pathbuf) != 0) {
            if (errno != ENOENT) {
#ifndef FRONTEND
                ereport(WARNING, (errmsg("could not remove file or directory \"%s\" ", pathbuf)));
#else
                fprintf(stderr, _("could not remove file or directory \"%s\": %s\n"), pathbuf, gs_strerror(errno));
#endif
            }
        }
    }
    pgfnames_cleanup(filenames);
    return;
}

}  // namespace extreme_rto_standby_read
