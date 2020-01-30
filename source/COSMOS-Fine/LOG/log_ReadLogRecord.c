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
 * Module: log_ReadLogRecord.c
 *
 * Description:
 *  Read a given log record from the log file.
 *
 * Exports:
 *  Four Four log_ReadLogRecord(Four, Lsn_T*, LOG_LogRecInfo_T*)
 */


#include <string.h>
#include "common.h"
#include "error.h"
#include "trace.h"
#include "LOG.h"
#include "perProcessDS.h"
#include "perThreadDS.h"



/*
 * Function: Four log_ReadLogRecord(Four, Lsn_T*, LOG_LogRecInfo_T*)
 *
 * Description:
 *  Read a given log record from the log file. The log record
 *  consists of a log record header and a set of images.
 *
 * Returns:
 *  error code
 */
Four log_ReadLogRecord(
    Four 		handle,
    Lsn_T 		*lsn,                 	/* IN log record to be read */
    LOG_LogRecInfo_T 	*logRecInfo) 		/* OUT log record info */
{
    Four 		e;                     	/* returned error code */
    log_LogPage_T 	*logPage;     		/* pointer to a log page */
    Four 		subRead;               	/* sub-read size of a part in a log record */
    Four 		spaceLeft;            	/* left space in a log page */
    Four 		wrapCount;             	/* wrapcount of the log page */
    Four 		offset;			/* offset of a log record in a log page */
    PageNo 		pageNo;              	/* page no of the log page */
    char		*basePtrTo;             /* base pointer to which something is copied */
    Four 		nBytes;                	/* log image size */
    Four 		i;


    TR_PRINT(handle, TR_LOG, TR1, ("log_ReadLogRecord(lsn=%P, logRecInfo=%P)",
			   lsn, logRecInfo));


    /*
     *  check input parameters
     */
    if (lsn == NULL || logRecInfo == NULL) ERR(handle, eBADPARAMETER);


    /*
     * calculate the wrap counter, the page number and the position of
     * this log record in the log page
     */
    wrapCount = lsn->wrapCount;
    pageNo = LOG_GET_PAGE_NO_FROM_LSN_OFFSET(lsn->offset);
    offset = LOG_GET_PAGE_OFFSET_IN_PAGE_FROM_LSN_OFFSET(lsn->offset);

    /* calculate the space left in the log page */
    spaceLeft = LOG_GET_SPACE_SIZE_FROM_OFFSET_IN_PAGE(offset);


    /* get a log page from the buffer */
    e = log_GetAndFixBuffer(handle, wrapCount, pageNo, &logPage);
    if(e == eENDOFLOG_LOG) return eENDOFLOG_LOG; if (e < eNOERROR) ERR(handle, e);


    /*
     * A trick: Initially set a logRecHdr->nImages to 1.
     *          The value will be updated from the log when reading the log
     *          record header.
     */
    logRecInfo->nImages = 0;
    for (i = 0; i < logRecInfo->nImages+1; i++) {

	/*
	 * get a starting address of the destination and the number of bytes
	 * to be read
	 */
	if (i == 0) {		/* read the log record header */
	    basePtrTo = (char*)logRecInfo;
	    nBytes = OFFSET_OF(LOG_LogRecInfo_T, imageData[0]);
	} else {		/* read the images */
	    basePtrTo = (char*)logRecInfo->imageData[i-1];
	    nBytes = logRecInfo->imageSize[i-1];
	}


	for ( ; nBytes > 0; ) {

	    /* calculate the amount of bytes that can be read in this log page */
	    subRead = MIN(nBytes, spaceLeft);

            /*
             * check whether this is the part of a valid log record
             */
            if (LOG_GET_PAGE_NO_FROM_LSN_OFFSET(logPage->hdr.lsn.offset) == pageNo &&
                offset + subRead > LOG_GET_PAGE_OFFSET_IN_PAGE_FROM_LSN_OFFSET(logPage->hdr.lsn.offset)) {
                e = log_UnfixBuffer(handle);
                if (e < eNOERROR) ERR(handle, e);

                return eENDOFLOG_LOG;
            }

	    memcpy(basePtrTo, &logPage->data[offset], subRead);

	    /* adjust variable nBytes and basePtrTo */
	    nBytes -= subRead;
	    basePtrTo += subRead;
	    spaceLeft -= subRead;
            offset += subRead;

	    /*
	     * when the nBytes wanted-to-read spans more than a log page,
	     * an additional log page should be gotten
	     */
	    if (nBytes > 0) {

		/* free the current log buffer page */
		e = log_UnfixBuffer(handle);
		if (e < eNOERROR) ERR(handle, e);

		/* get the next log page id */
		if ((pageNo + 1) == LOG_LOGMASTER.numPages) {
		    wrapCount++;
		    pageNo = 0;

		} else {
		    pageNo++;
		}

		/* get the next log page from the buffer */
		e = log_GetAndFixBuffer(handle, wrapCount, pageNo, &logPage);
		if (e < eNOERROR) ERR(handle, e);

		/* adjust variables */
                offset = 0;
		spaceLeft = LOG_GET_SPACE_SIZE_FROM_OFFSET_IN_PAGE(0);

	    }	/* end if */
	}
    } /* end while */


    /* free the current log buffer page */
    e = log_UnfixBuffer(handle);
    if (e < eNOERROR) ERR(handle, e);


    return(eNOERROR);

} /* log_ReadLogRecord() */

