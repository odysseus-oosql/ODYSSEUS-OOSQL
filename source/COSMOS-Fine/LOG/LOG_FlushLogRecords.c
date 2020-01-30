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
 * Module: LOG_FlushLogRecords.c
 *
 * Description:
 *  Flush log buffer pages so that the given log record should be permanant.
 *
 * Exports:
 *  Four LOG_FlushLogRecords(Four, Lsn_T*, Four)
 */


#include <assert.h>
#include "common.h"
#include "error.h"
#include "trace.h"
#include "LOG.h"
#include "perProcessDS.h"
#include "perThreadDS.h"



/*
 * Function: Four LOG_FlushLogRecords(Four, Lsn_T*, Four)
 *
 * Description:
 *  Flush log buffer pages so that the given log record should be permanant.
 *  The log buffers from LOG_LBI_TAIL to the buffer holding the given log record
 *  will be written into the disk.
 *
 * Returns:
 *  error code
 */
Four LOG_FlushLogRecords(
    Four 	handle,
    Lsn_T 	*logRecLsn,		/* IN log record which should be written into disk */
    Four 	logRecLength)		/* IN the length of the log record */
{
    Four 	e;			/* error code */
    Four 	pageNo;			/* page no of the log page containing the last byte of the log record */
    Four 	pageDistance;		/* distance between two log pages */
    Four 	nPages;			/* # of log pages buffered in log buffer pool */
    Lsn_T 	lsn;                  	/* temporary variable */
    Four 	logBufIdx;            	/* log buffer index */
    Boolean 	flushAllBuffersFlag; 	/* TRUE if all the buffers are to be flushed */


    TR_PRINT(handle, TR_LOG, TR1, ("LOG_FlushLogRecords(logRecLsn=%P, logRecLength=%lD)",
			   logRecLsn, logRecLength));


    /*
     *	check input parameter
     */
    if (logRecLsn == NULL) ERR(handle, eBADPARAMETER);

    /*
     * If there is no log volume, we have completed the flush.
     */
    if (LOG_LOGMASTER.volNo == NIL) return(eNOERROR);

    /* if log record length is 0, there is no need to flush. */
    /* The log record length can be 0.; no update but buffer dirty flag was set. */
    /* If the length is 0, we *SHOULD* return here becuase the follows cannot handle this case.(LOG_INCREASE_LSN(lsn, logRecLength-1) */
    assert(logRecLength >= 0);
    if (logRecLength == 0) return(eNOERROR);

    /* Get the page no containing the last byte of the log record. */
    lsn = *logRecLsn;
    LOG_INCREASE_LSN(lsn, logRecLength-1);
    pageNo = LOG_GET_PAGE_NO_FROM_LSN_OFFSET(lsn.offset);


    /*
     * Save the current LOG_LBI_HEAD value.
     * To avoid deadlock we should request latches in the order
     * LOG_LATCH4HEAD ==> LOG_LATCH4TAIL
     */
    /* Get the latch LOG_LATCH4HEAD to read the head no of the circular log buffer pool. */
    e = SHM_getLatch(handle, &LOG_LATCH4HEAD, procIndex, M_SHARED, M_UNCONDITIONAL, NULL);
    if (e < eNOERROR) ERR(handle, e);

    /*
     * We should get the latch LOG_LATCH4TAIL before releasing the latch
     * LOG_LATCH4HEAD.
     */
    e = SHM_getLatch(handle, &LOG_LATCH4TAIL, procIndex, M_EXCLUSIVE, M_UNCONDITIONAL, NULL);
    if (e < eNOERROR) ERRL1(handle, e, &LOG_LATCH4HEAD);


    /*
     * Get the distance between the log page containg log records and the log
     * page pointed by LOG_LBI_TAIL.
     */
    pageDistance = LOG_GET_DISTANCE_BTW_PAGES(lsn.wrapCount, pageNo,
					      LOG_LBT_WRAPCOUNT(LOG_LBI_TAIL),
					      LOG_LBT_PAGENO(LOG_LBI_TAIL));

    /* The log records were already flushed out. */
    if (pageDistance < 0) {
	e = SHM_releaseLatch(handle, &LOG_LATCH4TAIL, procIndex);
	if (e < eNOERROR) ERRL1(handle, e, &LOG_LATCH4HEAD);

        e = SHM_releaseLatch(handle, &LOG_LATCH4HEAD, procIndex);
        if (e < eNOERROR) ERR(handle, e);

	return(eNOERROR);
    }

#ifndef NDEBUG
    /* Get the number of pages in the log buffer pool. */
    nPages = (LOG_LBI_HEAD >= LOG_LBI_TAIL) ?
	(LOG_LBI_HEAD - LOG_LBI_TAIL + 1) : (NUM_WRITE_LOG_BUFS + LOG_LBI_HEAD - LOG_LBI_TAIL + 1);

    /* The log records are not written yet. */
    if ((pageDistance+1) > nPages) {
	e = SHM_releaseLatch(handle, &LOG_LATCH4TAIL, procIndex);
	if (e < eNOERROR) ERRL1(handle, e, &LOG_LATCH4HEAD);

        e = SHM_releaseLatch(handle, &LOG_LATCH4HEAD, procIndex);
        if (e < eNOERROR) ERR(handle, e);

	ERR(handle, eBADPARAMETER);
    }
#endif /* NDEBUG */

    logBufIdx = (LOG_LBI_TAIL + pageDistance) % NUM_WRITE_LOG_BUFS;

    if (LOG_LBI_HEAD == logBufIdx) {
        flushAllBuffersFlag = TRUE;

    } else {
        flushAllBuffersFlag = FALSE;

        /* Release the latch LOG_LATCH4HEAD to allow the other things to go on. */
        e = SHM_releaseLatch(handle, &LOG_LATCH4HEAD, procIndex);
        if (e < eNOERROR) ERRL1(handle, e, &LOG_LATCH4TAIL);

    }

    /* lastKeepBufFlag parameter has TRUE value when flushAllBuffersFlag is TRUE. */
    e = log_FlushLogBuffers(handle, logBufIdx, flushAllBuffersFlag);
    if (e < eNOERROR) {
        if (flushAllBuffersFlag)
            (Four) SHM_releaseLatch(handle, &LOG_LATCH4HEAD, procIndex);
        ERRL1(handle, e, &LOG_LATCH4TAIL);
    }

    if (flushAllBuffersFlag) {
        /* Release the latch LOG_LATCH4HEAD. */
        e = SHM_releaseLatch(handle, &LOG_LATCH4HEAD, procIndex);
        if (e < eNOERROR) ERRL1(handle, e, &LOG_LATCH4TAIL);
    }

    /* release latch */
    e = SHM_releaseLatch(handle, &LOG_LATCH4TAIL, procIndex);
    if (e < eNOERROR) ERR(handle, e);


    return(eNOERROR);

} /* LOG_Flush() */
