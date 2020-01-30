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
 * Module: log_WriteLogRecord.c
 *
 * Description:
 *  Write a log record into the log file.
 *
 * Exports:
 *  Four log_WriteLogRecord(Four, LOG_LogRecInfo_T*)
 */


#include <assert.h>
#include <string.h>
#include "common.h"
#include "error.h"
#include "trace.h"
#include "LOG.h"
#include "perProcessDS.h"
#include "perThreadDS.h"



/*
 * Function: Four log_WriteLogRecord(Four, LOG_LogRecHdr_T*)
 *
 * Description:
 *  Write a log record into the log file. The log record consists of a few
 *  images. The first image should be a log record header.
 *
 * Returns:
 *  error code
 *    eNOERROR
 *
 * Assumption:
 *  The caller is holding the latch LOG_LATCH4HEAD.
 */
Four log_WriteLogRecord(
    Four 		handle,
    LOG_LogRecInfo_T 	*logRecInfo)    /* IN log record infomation */
{
    Four 		e;              /* returned error code */
    log_LogPage_T 	*logPage;	/* pointer to a log page */
    Four 		nBytes;         /* size of an image in bytes */
    Four 		subWritten;     /* sub-written size of a part in a log record */
    Four 		offsetInPage;   /* offset of a log record in a log page */
    Four 		spaceLeft;      /* left space in a log page */
    char 		*basePtrFrom;   /* base pointer from which something is copied */
    char 		*basePtrTo;     /* base pointer to which something is copied */
    Four 		wrapCount;      /* wrap count of the newly allocated log page */
    Four 		pageNo;		/* page no of the newly allocated log page */
    PageID 		pid;
    Four 		i;              /* loop variable */


    TR_PRINT(handle, TR_LOG, TR1, ("log_WriteLogRecords(logRecInfo=%P)", logRecInfo));


    /* calculate the position of this log record in the log page */
    offsetInPage = LOG_GET_PAGE_OFFSET_IN_PAGE_FROM_LSN_OFFSET(LOG_LOGMASTER.nextLsn.offset);

    /* calculate the space left in the log page */
    spaceLeft = LOG_GET_SPACE_SIZE_FROM_OFFSET_IN_PAGE(offsetInPage);

    /* Get a pointer to destination. */
    logPage = &LOG_LOGBUFFERPAGE[LOG_LBI_HEAD];
    basePtrTo = &(logPage->data[offsetInPage]);


    assert(logRecInfo->nImages <= LOG_MAX_NUM_IMAGES);


    /*
     * Write log record.
     */
    for (i = 0; i < logRecInfo->nImages + 1; i++) {
	/*
	 * get a starting address and the number of bytes of the data to
	 * be written in this loop
	 */
	if (i == 0) {
	    basePtrFrom = (char*)logRecInfo;
	    nBytes = OFFSET_OF(LOG_LogRecInfo_T, imageData[0]);
	} else {
            assert(logRecInfo->imageSize[i-1] <= LOG_MAX_IMAGE_SIZE);

	    basePtrFrom = (char*)logRecInfo->imageData[i-1];
	    nBytes = logRecInfo->imageSize[i-1];
	}

	for ( ; nBytes > 0; ) {

	    /* calculate the amount of bytes that can be stored in this log page */
	    subWritten = MIN(nBytes, spaceLeft);
	    memcpy(basePtrTo, basePtrFrom, subWritten);

	    /* adjust variables */
	    nBytes -= subWritten;
	    basePtrFrom += subWritten;
	    basePtrTo += subWritten;
	    spaceLeft -= subWritten;

            logPage->hdr.lsn.offset += subWritten;

	    /*
	     *	if the current log page becomes full,
	     *	a new log page should be allocated for the next logging
	     */
	    if (spaceLeft == 0) {

		/* get the next log buffer page */
		e = log_AllocPage(handle, &pageNo, &wrapCount);
		if (e < eNOERROR) ERR(handle, e);

		e = log_AllocLogBuffer(handle, pageNo, wrapCount);
		if (e < eNOERROR) ERR(handle, e);

		logPage = &LOG_LOGBUFFERPAGE[LOG_LBI_HEAD];

		/* Initialize the log buffer page */
                pid.volNo = LOG_LOGMASTER.volNo;
                pid.pageNo = LOG_GET_PHYSICAL_PAGENO(LOG_LOGMASTER, wrapCount, pageNo); 
		LOG_INIT_LOG_PAGE(logPage, pid, LOG_GET_LSN_OFFSET_FROM_PAGE_NO(pageNo), wrapCount);

		/* adjust variables */
		basePtrTo = logPage->data;
		spaceLeft = LOG_GET_SPACE_SIZE_FROM_OFFSET_IN_PAGE(0);

	    }	/* end if */
	}
    }

    return(eNOERROR);

} /* log_WriteLogRecord() */
