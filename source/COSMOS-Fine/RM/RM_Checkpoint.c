/******************************************************************************/
/*                                                                            */
/*    Copyright (c) 1990-2016, KAIST                                          */
/*    All rights reserved.                                                    */
/*                                                                            */
/*    Redistribution and use in source and binary forms, with or without      */
/*    modification, are permitted provided that the following conditions      */
/*    are met:                                                                */
/*                                                                            */
/*    1. Redistributions of source code must retain the above copyright       */
/*       notice, this list of conditions and the following disclaimer.        */
/*                                                                            */
/*    2. Redistributions in binary form must reproduce the above copyright    */
/*       notice, this list of conditions and the following disclaimer in      */
/*       the documentation and/or other materials provided with the           */
/*       distribution.                                                        */
/*                                                                            */
/*    3. Neither the name of the copyright holder nor the names of its        */
/*       contributors may be used to endorse or promote products derived      */
/*       from this software without specific prior written permission.        */
/*                                                                            */
/*    THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS     */
/*    "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT       */
/*    LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS       */
/*    FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE          */
/*    COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,    */
/*    INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,    */
/*    BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;        */
/*    LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER        */
/*    CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT      */
/*    LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN       */
/*    ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE         */
/*    POSSIBILITY OF SUCH DAMAGE.                                             */
/*                                                                            */
/******************************************************************************/
/******************************************************************************/
/*                                                                            */
/*    ODYSSEUS/COSMOS General-Purpose Large-Scale Object Storage System --    */
/*    Fine-Granule Locking Version                                            */
/*    Version 3.0                                                             */
/*                                                                            */
/*    Developed by Professor Kyu-Young Whang et al.                           */
/*                                                                            */
/*    Advanced Information Technology Research Center (AITrc)                 */
/*    Korea Advanced Institute of Science and Technology (KAIST)              */
/*                                                                            */
/*    e-mail: odysseus.oosql@gmail.com                                        */
/*                                                                            */
/*    Bibliography:                                                           */
/*    [1] Whang, K., Lee, J., Lee, M., Han, W., Kim, M., and Kim, J., "DB-IR  */
/*        Integration Using Tight-Coupling in the Odysseus DBMS," World Wide  */
/*        Web, Vol. 18, No. 3, pp. 491-520, May 2015.                         */
/*    [2] Whang, K., Lee, M., Lee, J., Kim, M., and Han, W., "Odysseus: a     */
/*        High-Performance ORDBMS Tightly-Coupled with IR Features," In Proc. */
/*        IEEE 21st Int'l Conf. on Data Engineering (ICDE), pp. 1104-1105     */
/*        (demo), Tokyo, Japan, April 5-8, 2005. This paper received the Best */
/*        Demonstration Award.                                                */
/*    [3] Whang, K., Park, B., Han, W., and Lee, Y., "An Inverted Index       */
/*        Storage Structure Using Subindexes and Large Objects for Tight      */
/*        Coupling of Information Retrieval with Database Management          */
/*        Systems," U.S. Patent No.6,349,308 (2002) (Appl. No. 09/250,487     */
/*        (1999)).                                                            */
/*    [4] Whang, K., Lee, J., Kim, M., Lee, M., Lee, K., Han, W., and Kim,    */
/*        J., "Tightly-Coupled Spatial Database Features in the               */
/*        Odysseus/OpenGIS DBMS for High-Performance," GeoInformatica,        */
/*        Vol. 14, No. 4, pp. 425-446, Oct. 2010.                             */
/*    [5] Whang, K., Lee, J., Kim, M., Lee, M., and Lee, K., "Odysseus: a     */
/*        High-Performance ORDBMS Tightly-Coupled with Spatial Database       */
/*        Features," In Proc. 23rd IEEE Int'l Conf. on Data Engineering       */
/*        (ICDE), pp. 1493-1494 (demo), Istanbul, Turkey, Apr. 16-20, 2007.   */
/*                                                                            */
/******************************************************************************/
/*
 * Module: RM_Checkpoint.c
 *
 * Description:
 *  perform checkpointing
 *
 * Exports:
 *  Four RM_Checkpoint(Four)
 */


#include "common.h"
#include "error.h"
#include "trace.h"
#include "Util.h"
#include "xactTable.h"
#include "dirtyPageTable.h"
#include "TM.h"
#include "RDsM.h"
#include "LOG.h"
#include "RM.h"
#include "perProcessDS.h"
#include "perThreadDS.h"



/*
 * Function: Four RM_Checkpoint(Four)
 *
 * Description:
 *  perform checkpointing
 *
 * Returns:
 *  error code
 */
Four RM_Checkpoint(
    Four		handle)
{
    Four 		e;			/* error code */
    Lsn_T 		chkptLsn;             	/* lsn for the begin checkpoint log record */
    Lsn_T 		lsn;                  	/* LSN of the newly written log record */
    Four 		logRecLen;             	/* log record length */
    LOG_LogRecInfo_T 	logRecInfo; 		/* log record information */
    Four                i, j;          		/* loop index */
    

    /* pointer for COMMON Data Structure of perThreadTable */
    COMMON_PerThreadDS_T *common_perThreadDSptr = COMMON_PER_THREAD_DS_PTR(handle);

    TR_PRINT(handle, TR_RM, TR1, ("RM_Checkpoint()"));


#ifdef USE_PDL 
    /*
     * Flushes the data written into the cache of a storage device immediately out to the devices
     */

    /* For the devices of the data volumes, call fsync() */
    for (i = 0; i < MAXNUMOFVOLS; i++) {
        if (RDSM_USERVOLTABLE(handle)[i].volNo != NOVOL && RDSM_VOLTABLE[i].volInfo.type == VOLUME_TYPE_DATA) {

            assert(RDSM_USERVOLTABLE(handle)[i].volNo == RDSM_VOLTABLE[i].volInfo.volNo);

            for (j = 0; j < RDSM_USERVOLTABLE(handle)[i].numDevices; j++) {
#ifndef WIN32
                if (Util_fsync(OPENFILEDESC_ARRAY(RDSM_USERVOLTABLE(handle)[i].openFileDesc)[j]) == -1) ERR(handle, eDEVICECLOSEFAIL_RDSM);
#else
                if (FlushFileBuffer(OPENFILEDESC_ARRAY(RDSM_USERVOLTABLE(handle)[i].openFileDesc)[j]) == 0) ERR(handle, eDEVICECLOSEFAIL_RDSM);
#endif /* WIN32 */
            }
        }
    }

    /* For the devices of the log volumes, call fsync() */
    for (i = 0; i < MAXNUMOFVOLS; i++) {
        if (RDSM_USERVOLTABLE(handle)[i].volNo != NOVOL && RDSM_VOLTABLE[i].volInfo.type == VOLUME_TYPE_RAW) {

            assert(RDSM_USERVOLTABLE(handle)[i].volNo == RDSM_VOLTABLE[i].volInfo.volNo);

            for (j = 0; j < RDSM_USERVOLTABLE(handle)[i].numDevices; j++) {
#ifndef WIN32
                if (fsync(OPENFILEDESC_ARRAY(RDSM_USERVOLTABLE(handle)[i].openFileDesc)[j]) == -1) ERR(handle, eDEVICECLOSEFAIL_RDSM);
#else
                if (FlushFileBuffer(OPENFILEDESC_ARRAY(RDSM_USERVOLTABLE(handle)[i].openFileDesc)[j]) == 0) ERR(handle, eDEVICECLOSEFAIL_RDSM);
#endif /* WIN32 */
            }
        }
    }
#endif 

    /*
     * Write a checkpoint begin log record.
     */
    LOG_FILL_LOGRECINFO_0(logRecInfo, common_perThreadDSptr->nilXactId, LOG_TYPE_CHECKPOINT,
                          LOG_ACTION_CHKPT_BEGIN, LOG_REDO_ONLY,
                          common_perThreadDSptr->nilPid, common_perThreadDSptr->nilLsn, common_perThreadDSptr->nilLsn);

    e = LOG_WriteLogRecord(handle, NULL, &logRecInfo, &chkptLsn, &logRecLen);
    if (e < eNOERROR) ERR(handle, e);


    /*
     * Write the log record containing the list of dirty pages
     */
    e = BfM_LogDirtyPageTableEntries(handle);
    if (e < eNOERROR) ERR(handle, e);


    /*
     * Write the log record containing the list of active transactions
     */
    e = TM_XT_LogActiveXacts(handle);
    if (e < eNOERROR) ERR(handle, e);


    /*
     * Write log record containing the list of mounted volumes
     */
    e = RDsM_LogMountedVols(handle);
    if (e < eNOERROR) ERR(handle, e);


    /*
     * Write the end of checkpoint log record
     */
    LOG_FILL_LOGRECINFO_0(logRecInfo, common_perThreadDSptr->nilXactId, LOG_TYPE_CHECKPOINT,
                          LOG_ACTION_CHKPT_END, LOG_REDO_ONLY,
                          common_perThreadDSptr->nilPid, common_perThreadDSptr->nilLsn, common_perThreadDSptr->nilLsn);

    e = LOG_WriteLogRecord(handle, NULL, &logRecInfo, &lsn, &logRecLen);
    if (e < eNOERROR) ERR(handle, e);


    /*
     * force all the log records to the end of log
     */
    e = LOG_FlushLogRecords(handle, &lsn, logRecLen);
    if (e < eNOERROR) ERR(handle, e);


    /*
     * set the lsn of the new valid checkpoint log record and
     * write the checkpoint lsn into the disk
     */
    e = LOG_SetCheckpointLsn(handle, &chkptLsn);
    if (e < eNOERROR) ERR(handle, e);

    return(eNOERROR);

} /* RM_Checkpoint() */
