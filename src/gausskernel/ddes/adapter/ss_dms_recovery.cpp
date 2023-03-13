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
 * ---------------------------------------------------------------------------------------
 *
 * ss_dms_recovery.cpp
 *        Provide common interface for recovery within DMS reform process.
 *
 * IDENTIFICATION
 *        src/gausskernel/ddes/adapter/ss_dms_recovery.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "postgres.h"
#include "access/xlog.h"
#include "access/xact.h"
#include "access/multi_redo_api.h"
#include "storage/standby.h"
#include "storage/pmsignal.h"
#include "storage/buf/bufmgr.h"
#include "storage/dss/fio_dss.h"
#include "storage/smgr/fd.h"
#include "storage/smgr/segment.h"
#include "postmaster/postmaster.h"
#include "storage/file/fio_device.h"
#include "ddes/dms/ss_dms_bufmgr.h"
#include "ddes/dms/ss_dms_recovery.h"
#include "ddes/dms/ss_reform_common.h"
#include "access/double_write.h"
#include <sys/types.h>
#include <dirent.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>

int SSGetPrimaryInstId()
{
    return g_instance.dms_cxt.SSReformerControl.primaryInstId;
}

void SSSavePrimaryInstId(int id)
{
    g_instance.dms_cxt.SSReformerControl.primaryInstId = id;
    SSSaveReformerCtrl();
}

bool SSRecoveryNodes()
{
    bool result = false;
    while (true) {
        if (dms_reform_failed()) {
            result = false;
            break;
        }
        /** why use lock:
         * time1 startup thread: update IsRecoveryDone, not finish UpdateControlFile
         * time2 reform_proc: finish reform, think ControlFile is ok
         * time3 DB crash
         * time4 read the checkpoint which created before failover. oops, it is wrong
         */
        LWLockAcquire(ControlFileLock, LW_SHARED);
        if (t_thrd.shemem_ptr_cxt.XLogCtl->IsRecoveryDone &&
            t_thrd.shemem_ptr_cxt.ControlFile->state == DB_IN_PRODUCTION) {
            LWLockRelease(ControlFileLock);
            result = true;
            break;
        }
        LWLockRelease(ControlFileLock);
        pg_usleep(REFORM_WAIT_TIME);
    }
    return result;
}

bool SSRecoveryApplyDelay()
{
    if (!ENABLE_REFORM) {
        return false;
    }

    while (g_instance.dms_cxt.SSRecoveryInfo.recovery_pause_flag) {
        /* might change the trigger file's location */
        RedoInterruptCallBack();

        pg_usleep(REFORM_WAIT_TIME);
    }

    return true;
}

void SSReadControlFile(int id, bool updateDmsCtx)
{
    pg_crc32c crc;
    errno_t rc = EOK;
    int fd = -1;
    char *fname = NULL;
    bool retry = false;
    int read_size = 0;
    int len = 0;
    fname = XLOG_CONTROL_FILE;

loop:
    fd = BasicOpenFile(fname, O_RDWR | PG_BINARY, S_IRUSR | S_IWUSR);
    if (fd < 0) {
        ereport(FATAL, (errcode_for_file_access(), errmsg("could not open control file \"%s\": %m", fname)));
    }

    off_t seekpos = (off_t)BLCKSZ * id;

    if (id == REFORM_CTRL_PAGE) {
        len = sizeof(ss_reformer_ctrl_t);
    } else {
        len = sizeof(ControlFileData);
    }

    read_size = (int)BUFFERALIGN(len);
    char buffer[read_size] __attribute__((__aligned__(ALIGNOF_BUFFER)));
    if (pread(fd, buffer, read_size, seekpos) != read_size) {
        ereport(PANIC, (errcode_for_file_access(), errmsg("could not read from control file: %m")));
    }

    if (id == REFORM_CTRL_PAGE) {
        rc = memcpy_s(&g_instance.dms_cxt.SSReformerControl, len, buffer, len);
        securec_check(rc, "", "");
        if (close(fd) < 0) {
            ereport(PANIC, (errcode_for_file_access(), errmsg("could not close control file: %m")));
        }

        /* Now check the CRC. */
        INIT_CRC32C(crc);
        COMP_CRC32C(crc, (char *)&g_instance.dms_cxt.SSReformerControl, offsetof(ss_reformer_ctrl_t, crc));
        FIN_CRC32C(crc);

        if (!EQ_CRC32C(crc, g_instance.dms_cxt.SSReformerControl.crc)) {
            if (retry == false) {
                ereport(WARNING, (errmsg("control file \"%s\" contains incorrect checksum, try backup file", fname)));
                fname = XLOG_CONTROL_FILE_BAK;
                retry = true;
                goto loop;
            } else {
                ereport(FATAL, (errmsg("incorrect checksum in control file")));
            }
        }
    } else {
        ControlFileData* controlFile = NULL;
        ControlFileData tempControlFile;
        if (updateDmsCtx) {
            controlFile = &tempControlFile;
        } else {
            controlFile = t_thrd.shemem_ptr_cxt.ControlFile;
        }

        rc = memcpy_s(controlFile, (size_t)len, buffer, (size_t)len);
        securec_check(rc, "", "");
        if (close(fd) < 0) {
            ereport(PANIC, (errcode_for_file_access(), errmsg("could not close control file: %m")));
        }

        /* Now check the CRC. */
        INIT_CRC32C(crc);
        COMP_CRC32C(crc, (char *)controlFile, offsetof(ControlFileData, crc));
        FIN_CRC32C(crc);

        if (!EQ_CRC32C(crc, controlFile->crc)) {
            if (retry == false) {
                ereport(WARNING, (errmsg("control file \"%s\" contains incorrect checksum, try backup file", fname)));
                fname = XLOG_CONTROL_FILE_BAK;
                retry = true;
                goto loop;
            } else {
                ereport(FATAL, (errmsg("incorrect checksum in control file")));
            }
        }

        if (XLByteLE(g_instance.dms_cxt.ckptRedo, controlFile->checkPointCopy.redo)) {
            g_instance.dms_cxt.ckptRedo = controlFile->checkPointCopy.redo;
        }
    }
}

/* initialize reformer ctrl parameter when initdb */
void SSWriteReformerControlPages(void)
{
    /*
     * If already exists control file, reformer page must have been initialized
     */
    if (dss_exist_file(XLOG_CONTROL_FILE)) {
        SSReadControlFile(REFORM_CTRL_PAGE);
        if (g_instance.dms_cxt.SSReformerControl.list_stable != 0 ||
            g_instance.dms_cxt.SSReformerControl.primaryInstId == SS_MY_INST_ID) {
            (void)printf("[SS] ERROR: files from last install must be cleared.\n");
            ereport(PANIC, (errmsg("Files from last initdb not cleared")));
        }
        (void)printf("[SS] Current node:%d acknowledges cluster PRIMARY node:%d.\n",
            SS_MY_INST_ID, g_instance.dms_cxt.SSReformerControl.primaryInstId);
        return;
    }

    int fd = -1;
    char buffer[PG_CONTROL_SIZE] __attribute__((__aligned__(ALIGNOF_BUFFER))); /* need to be aligned */
    errno_t errorno = EOK;

    /*
     * Initialize list_stable and primaryInstId
     * First node to initdb is chosen as primary for now, and for first-time cluster startup.
     */
    Assert(!dss_exist_file(XLOG_CONTROL_FILE));
    g_instance.dms_cxt.SSReformerControl.list_stable = 0;
    g_instance.dms_cxt.SSReformerControl.primaryInstId = SS_MY_INST_ID;
    (void)printf("[SS] Current node:%d initdb first, will become PRIMARY for first-time SS cluster startup.\n",
        SS_MY_INST_ID);

    /* Contents are protected with a CRC */
    INIT_CRC32C(g_instance.dms_cxt.SSReformerControl.crc);
    COMP_CRC32C(g_instance.dms_cxt.SSReformerControl.crc, (char *)&g_instance.dms_cxt.SSReformerControl,
                offsetof(ss_reformer_ctrl_t, crc));
    FIN_CRC32C(g_instance.dms_cxt.SSReformerControl.crc);

    if (sizeof(ss_reformer_ctrl_t) > PG_CONTROL_SIZE) {
        ereport(PANIC, (errmsg("sizeof(ControlFileData) is larger than PG_CONTROL_SIZE; fix either one")));
    }

    errorno = memset_s(buffer, PG_CONTROL_SIZE, 0, PG_CONTROL_SIZE);
    securec_check(errorno, "", "");

    errorno = memcpy_s(buffer, PG_CONTROL_SIZE, &g_instance.dms_cxt.SSReformerControl, sizeof(ss_reformer_ctrl_t));
    securec_check(errorno, "", "");

    fd = BasicOpenFile(XLOG_CONTROL_FILE, O_RDWR | O_CREAT | O_EXCL | PG_BINARY, S_IRUSR | S_IWUSR);
    if (fd < 0) {
        ereport(PANIC,
                (errcode_for_file_access(), errmsg("could not create control file \"%s\": %m", XLOG_CONTROL_FILE)));
    }

    SSWriteInstanceControlFile(fd, buffer, REFORM_CTRL_PAGE, PG_CONTROL_SIZE);
    if (close(fd)) {
        ereport(PANIC, (errcode_for_file_access(), errmsg("could not close control file: %m")));
    }
}

void SSTriggerFailover()
{
    if (g_instance.dms_cxt.SSRecoveryInfo.startup_reform) {
        g_instance.dms_cxt.SSRecoveryInfo.restart_failover_flag = true;
        ereport(LOG, (errmodule(MOD_DMS), errmsg("[SS failover] do failover when DB restart.")));
    } else {
        SendPostmasterSignal(PMSIGNAL_DMS_TRIGGERFAILOVER);
        ereport(LOG, (errmodule(MOD_DMS), errmsg("[SS failover] do failover when DB alive")));
    }
}

void SShandle_promote_signal()
{
    if (pmState == PM_WAIT_BACKENDS) {
        g_instance.pid_cxt.StartupPID = initialize_util_thread(STARTUP);
        Assert(g_instance.pid_cxt.StartupPID != 0);
        pmState = PM_STARTUP;
    }

    ereport(LOG, (errmodule(MOD_DMS), errmsg("[SS failover] begin startup.")));
}


void ss_failover_dw_init_internal()
{
    /*
     * step 1: remove self dw file dw_exit close self dw
     * step 2: load old primary dw ,and finish dw recovery, exit
     * step 3: rebuild dw file and init self dw
     */

    char *dssdir = g_instance.attr.attr_storage.dss_attr.ss_dss_vg_name;
    int old_primary_id = g_instance.dms_cxt.SSReformerControl.primaryInstId;
    int self_id = g_instance.attr.attr_storage.dms_attr.instance_id;
    if (!g_instance.dms_cxt.SSRecoveryInfo.startup_reform) {
        dw_exit(true);
        dw_exit(false);
    }

    dw_exit(true);
    dw_exit(false);
    ss_initdwsubdir(dssdir, old_primary_id);
    dw_ext_init();
    dw_init();
    g_instance.dms_cxt.finishedRecoverOldPrimaryDWFile = true;
    dw_exit(true);
    dw_exit(false);
    ss_initdwsubdir(dssdir, self_id);
    dw_ext_init();
    dw_init();
    g_instance.dms_cxt.finishedRecoverOldPrimaryDWFile = false;
    ereport(LOG, (errmodule(MOD_DMS), errmsg("[SS failover] dw init finish")));
}

void ss_failover_dw_init()
{
    for (int i = 0; i < g_instance.ckpt_cxt_ctl->pgwr_procs.num; i++) {
        if (g_instance.pid_cxt.PageWriterPID[i] != 0) {
            signal_child(g_instance.pid_cxt.PageWriterPID[i], SIGTERM, -1);
        }
    }
    ckpt_shutdown_pagewriter();
    ss_failover_dw_init_internal();
    g_instance.dms_cxt.dw_init = true;
}

/* In serial switchover scenario, prevent this promoting node from reinitializing dw. */
void ss_switchover_promoting_dw_init()
{
    for (int i = 0; i < g_instance.ckpt_cxt_ctl->pgwr_procs.num; i++) {
        if (g_instance.pid_cxt.PageWriterPID[i] != 0) {
            signal_child(g_instance.pid_cxt.PageWriterPID[i], SIGTERM, -1);
        }
    }
    ckpt_shutdown_pagewriter();
    
    dw_exit(true);
    dw_exit(false);
    dw_ext_init();
    dw_init();
    g_instance.dms_cxt.dw_init = true;
    ereport(LOG, (errmodule(MOD_DMS), errmsg("[SS switchover] dw init finished")));
}