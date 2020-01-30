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
 * Module: LOG_SetNextLogRecordLsn.c
 *
 * Description:
 *  Set the next log record lsn. After the restart analysis the next log
 * record lsn is set using the function LOG_SetNextLogRecordLsn( ).
 *
 * Exports:
 *  Four LOG_SetNextLogRecordLsn(Four, LOG_Lsn_T*)
 */


#include <assert.h>
#include "common.h"
#include "error.h"
#include "trace.h"
#include "LOG.h"
#include "RDsM.h"
#include "perProcessDS.h"
#include "perThreadDS.h"



/*
 * Function: Four LOG_SetNextLogRecordLsn(Four, Lsn_T*)
 *
 * Description:
 *  Set the next log record lsn.
 *
 * Returns:
 *  error code
 */
Four LOG_SetNextLogRecordLsn(
    Four 		handle,
    Lsn_T 		*lsn)           /* IN the next log record lsn */
{
    Four 		e;		/* error code */
    PageID 		pid;		/* temporary variable */
    log_LogPage_T 	*logPage;	/* pointer to a log page */


    TR_PRINT(handle, TR_LOG, TR1, ("LOG_SetNextLogRecordLsn(lsn=%P)", lsn));


    /* This function assumes that concurrency control is provided by the caller. */

    /* Set the next log record lsn. */
    LOG_LOGMASTER.nextLsn = *lsn;

    /* Set the number of remained bytes in the current log file. */
    LOG_LOGMASTER.numBytesRemained = LOG_LOGMASTER.numBytes - lsn->offset;

    /*
     * Initialize the log write buffer.
     */

    /* Get the page containing the bytes to be written next time. */
    LOG_LOGMASTER.headWrapCount = lsn->wrapCount;
    LOG_LOGMASTER.headPageNo = LOG_GET_PAGE_NO_FROM_LSN_OFFSET(lsn->offset);

    LOG_LBT_PAGENO(LOG_LBI_HEAD) = LOG_LOGMASTER.headPageNo;
    LOG_LBT_WRAPCOUNT(LOG_LBI_HEAD) = LOG_LOGMASTER.headWrapCount;


    /*
     *  read in the log page
     */
    pid.volNo = LOG_LOGMASTER.volNo;
    pid.pageNo = LOG_GET_PHYSICAL_PAGENO(LOG_LOGMASTER, lsn->wrapCount, LOG_LBT_PAGENO(LOG_LBI_HEAD));
    e = RDsM_ReadTrain(handle, &pid, (char*)&LOG_LOGBUFFERPAGE[LOG_LBI_HEAD], PAGESIZE2);
    if (e < eNOERROR) ERR(handle, e);

    logPage = &LOG_LOGBUFFERPAGE[LOG_LBI_HEAD];
    if (logPage->hdr.lsn.wrapCount < lsn->wrapCount) { 
        /* Initialize the log buffer page */
        LOG_INIT_LOG_PAGE(logPage, pid, LOG_GET_LSN_OFFSET_FROM_PAGE_NO(LOG_LBT_PAGENO(LOG_LBI_HEAD)), lsn->wrapCount); 
    } else {
        logPage->hdr.lsn.offset = lsn->offset;
    }

    return(eNOERROR);

} /* LOG_SetNextLogRecordLsn() */



