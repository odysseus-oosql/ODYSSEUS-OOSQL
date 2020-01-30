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
 * Module: log_AllocLogBuffer.c
 *
 * Description:
 *  Allocate a log buffer to the given log page.
 *
 * Exports:
 *  Four log_AllocLogBuffer(Four, Four, Four)
 */


#include "common.h"
#include "error.h"
#include "trace.h"
#include "LOG.h"
#include "perProcessDS.h"
#include "perThreadDS.h"



/*
 * Function: Four log_AllocLogBuffer(Four, Four, Four)
 *
 * Description:
 *  Allocate a log buffer to the given log page.
 *
 * Returns:
 *  error code
 *
 * Assumption:
 *  The caller is holding the latch LOG_LATCH4HEAD.
 */
Four log_AllocLogBuffer(
    Four handle,
    Four pageNo,		/* IN page no of the log page */
    Four wrapCount)		/* IN wrap count of the log page */
{
    Four e;			/* error code */


    TR_PRINT(handle, TR_LOG, TR1, ("log_AllocLogBuffer(pageNo=%lD, wrapCount=%lD)", pageNo, wrapCount));


    /* LOG_LBI_TAIL information may be changed */
    e = SHM_getLatch(handle, &LOG_LATCH4TAIL, procIndex, M_EXCLUSIVE, M_UNCONDITIONAL, NULL);
    if (e < eNOERROR) ERR(handle, e);


    /*
     *	check if all the buffer pages are used now
     */
    if (((LOG_LBI_HEAD+1)%NUM_WRITE_LOG_BUFS) == LOG_LBI_TAIL) {

	/*
	 * flush the log pages from the page pointed by LOG_LBI_TAIL to the page including the headLsn
	 */
	e = log_FlushLogBuffers(handle, LOG_LBI_HEAD, FALSE);
        if (e < eNOERROR) ERRL1(handle, e, &LOG_LATCH4TAIL);

	LOG_LBI_HEAD = 0;
	LOG_LBI_TAIL = 0;

    } else {

	/*
	 *	proceed the head index
	 */
	LOG_LBI_HEAD = (LOG_LBI_HEAD+1) % NUM_WRITE_LOG_BUFS;

    }

    e = SHM_releaseLatch(handle, &LOG_LATCH4TAIL, procIndex);
    if (e < eNOERROR) ERR(handle, e);

    /*
     *	fill out the entry in the logBufferTable
     */
    LOG_LBT_WRAPCOUNT(LOG_LBI_HEAD) = wrapCount;
    LOG_LBT_PAGENO(LOG_LBI_HEAD) = pageNo;

    return(eNOERROR);

} /* log_AllocLogBuffer() */
