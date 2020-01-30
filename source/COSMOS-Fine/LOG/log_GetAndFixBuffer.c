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
 * Module: log_GetAndFixBuffer.c
 *
 * Description:
 *  This module includes log_GetAndFixBuffer( ) and log_UnfixBuffer( ) routines.
 *  This module allows a log page to be accessed via a buffer, which is in
 *  a log buffer pool or a buffer pool managed by the buffer manager. Only
 *  one log page can be accessed by this module; log_UnfixBuffer() should be
 *  called before another log_GetAndFixBuffer() is called.
 *
 * Exports:
 *  Four log_GetAndFixBuffer(Four, Four, Four, log_LogPage_T**)
 *  Four log_UnfixBuffer(Four)
 */


#include <assert.h>
#include "common.h"
#include "error.h"
#include "trace.h"
#include "LOG.h"
#include "BfM.h"
#include "perProcessDS.h"
#include "perThreadDS.h"





/*
 * Function: Four log_GetAndFixBuffer(Four, Four, Four, log_LogPage_T**)
 *
 * Description:
 *  Return an address pointing the log buffer holding the requested log page.
 *
 * Returns:
 *  error code
 */
Four log_GetAndFixBuffer(
    Four 		handle,
    Four	  	wrapCount,	/* IN wrap count of the page */
    Four	  	pageNo,	  	/* IN page number */
    log_LogPage_T 	**logPage)	/* OUT pointer to a buffer page */
{
    Four 		e;		/* error code */
    Four 		logBufHeadNo;	/* head no of the circular log buffer pool */
    Four 		pageDistance;	/* distance between two log pages */
    Four 		nPages;		/* # of log pages buffered in log buffer pool */
    PageID 		pid;

    /* pointer for LOG Data Structure of perThreadTable */
    LOG_PerThreadDS_T *log_perThreadDSptr = LOG_PER_THREAD_DS_PTR(handle);

    /* pointer for COMMON Data Structure of perThreadTable */
    COMMON_PerThreadDS_T *common_perThreadDSptr = COMMON_PER_THREAD_DS_PTR(handle);

    /*
     *	check input parameters
     */
    if (logPage == NULL) ERR(handle, eBADPARAMETER);


    /*
     * It is assumed that the offline log record(or log page) should not be
     * requested. We do not check if the requested log page is offline.
     */


    /*
     * Get the head no of the circular log buffer pool.
     */
    /* get a latch for reading head no */
    e = SHM_getLatch(handle, &LOG_LATCH4HEAD, procIndex, M_SHARED, M_UNCONDITIONAL, NULL);
    if (e < eNOERROR) ERR(handle, e);


    /* remember the head no. */
    logBufHeadNo = LOG_LBI_HEAD;

    /*
     * Release the latch LOG_LATCH4HEAD to allow the other things to go on.
     */
    e = SHM_releaseLatch(handle, &LOG_LATCH4HEAD, procIndex);
    if (e < eNOERROR) ERR(handle, e);

    /*
     *
     *    if request page is in Log Buffer,
     *    return without releasing latch for LOG_LATCH4TAIL.
     */

    /* Acquire the LOG_LATCH4TAIL not to flush the accessed log buffer page */

    e = SHM_getLatch(handle, &LOG_LATCH4TAIL, procIndex, M_SHARED, M_UNCONDITIONAL, NULL);
    if (e < 0)	ERR(handle, e);

    if (LOG_LBT_WRAPCOUNT(LOG_LBI_TAIL) == NIL) /* in the case that log buffer pool is empty */
	pageDistance = -1;	/* set to any negative number */
    else
	pageDistance = LOG_GET_DISTANCE_BTW_PAGES(wrapCount, pageNo,
				   	          LOG_LBT_WRAPCOUNT(LOG_LBI_TAIL),
					          LOG_LBT_PAGENO(LOG_LBI_TAIL));

    if (pageDistance >= 0) {	/* log page is in the log buffer */
	/* Get the number of pages in the log buffer pool. */
	nPages = (logBufHeadNo >= LOG_LBI_TAIL) ?
	    (logBufHeadNo - LOG_LBI_TAIL + 1) : (NUM_WRITE_LOG_BUFS + logBufHeadNo - LOG_LBI_TAIL + 1);

	/* The requested log page is the valid log page? */
	if (pageDistance >= nPages) {
	    e = SHM_releaseLatch(handle, &LOG_LATCH4TAIL, procIndex);
	    if (e < eNOERROR) ERR(handle, e);

	    return eENDOFLOG_LOG;
	}

	*logPage = &LOG_LOGBUFFERPAGE[LOG_LBI_TAIL + pageDistance];

    } else {			/* log page is in buffer pool managed by buffer manager */

	/* release the latch for tail access */
	e = SHM_releaseLatch(handle, &LOG_LATCH4TAIL, procIndex);
	if (e < eNOERROR) ERR(handle, e);

	pid.volNo = LOG_LOGMASTER.volNo;
	pid.pageNo = LOG_GET_PHYSICAL_PAGENO(LOG_LOGMASTER, wrapCount, pageNo);

	e = BfM_getAndFixBuffer(handle, &pid, M_FREE, &(log_perThreadDSptr->logPage_BCBP), PAGE_BUF);
	if (e < eNOERROR) {
	    log_perThreadDSptr->logPage_BCBP = NULL; /* clean up logPage_BCBP */
	    ERR(handle, e);
	}

	*logPage = (log_LogPage_T*)((log_perThreadDSptr->logPage_BCBP)->bufPagePtr);

	/*
	 * At the above, when the log buffer pool is empty we cannot check
	 * the boundary of the end of the log.
	 */
        assert(common_perThreadDSptr->nilLsn.wrapCount < LSN_STARTING_WRAP_COUNT);
        assert(wrapCount >= (*logPage)->hdr.lsn.wrapCount); 
	if (wrapCount > (*logPage)->hdr.lsn.wrapCount) {
	    e = BfM_unfixBuffer(handle, log_perThreadDSptr->logPage_BCBP, PAGE_BUF);
	    if (e < eNOERROR) ERR(handle, e);

            log_perThreadDSptr->logPage_BCBP = NULL;

	    return eENDOFLOG_LOG;
	}
    }

    return(eNOERROR);

} /* log_GetAndFixBuffer() */



/*
 * Function: Four log_UnfixBuffer(Four)
 *
 * Description:
 *  Unfix the currently used buffer which used for buffering a log page.
 *
 * Returns:
 *  error code
 */
Four log_UnfixBuffer(
    Four	handle)
{
    Four 	e;			/* error code */

    /* pointer for LOG Data Structure of perThreadTable */
    LOG_PerThreadDS_T *log_perThreadDSptr = LOG_PER_THREAD_DS_PTR(handle);

    TR_PRINT(handle, TR_LOG, TR1, ("log_UnfixBuffer()"));


    if (log_perThreadDSptr->logPage_BCBP == NULL) {	/* buffer is in the log buffer pool */
	/* release latch for the tail */
	e = SHM_releaseLatch(handle, &LOG_LATCH4TAIL, procIndex);

    } else { /* buffer is in the buffer pool managed by buffer manager */
	e = BfM_unfixBuffer(handle, log_perThreadDSptr->logPage_BCBP, PAGE_BUF);

	log_perThreadDSptr->logPage_BCBP = NULL;	/* clean up logPage_BCBP */
    }

    if (e < eNOERROR) ERR(handle, e);

    return(eNOERROR);

} /* log_UnfixBuffer() */






