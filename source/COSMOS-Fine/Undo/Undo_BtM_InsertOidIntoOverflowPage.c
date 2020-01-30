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
 * Module: Undo_BtM_InsertOidIntoOverflowPage.c
 *
 * Description:
 *  Undo inserting an oid into an overflow page
 *
 * Exports:
 *  Four Undo_BtM_InsertOidIntoOverflowPage(Four, LOG_LogRecInfo_T*)
 */


#include <string.h>
#include "common.h"
#include "error.h"
#include "trace.h"
#include "TM.h"
#include "LOG.h"
#include "BfM.h"
#include "BtM.h"
#include "Undo.h"
#include "perProcessDS.h"
#include "perThreadDS.h"


Four Undo_BtM_InsertOidIntoOverflowPage(
    Four 				handle,
    XactTableEntry_T 			*xactEntry, 		/* IN transaction table entry */
    Buffer_ACC_CB 			*aPage_BCBP,  		/* INOUT buffer access control block holding data */
    Lsn_T 				*logRecLsn,           	/* IN log record to undo */
    LOG_LogRecInfo_T 			*logRecInfo) 		/* IN operation information for writing a small object */
{
    Four 				e;			/* error code */
    BtreeOverflow 			*aPage;       		/* pointer to a slotted buffer page */
    Lsn_T 				lsn;                  	/* lsn of the newly written log record */
    Four 				logRecLen;             	/* log record length */
    LOG_LogRecInfo_T 			localLogRecInfo; 	/* log record information */
    LOG_Image_BtM_OidInOverflow_T 	*insertOidInfo;
    LogParameter_T 			logParam;
    ObjectID 				dummyCatObjForFile; 	/* dummy variable */
    LOG_Image_BtM_IndexInfo_T		*iinfoPtr;
    BtreeIndexInfo 			iinfo;
    KeyValue 				kval;          		/* key value */


    TR_PRINT(handle, TR_UNDO, TR1, ("Undo_BtM_InsertOidIntoOverflowPage(aPage_BCBP=%P, logRecInfo=%P)", aPage_BCBP, logRecInfo));


    /*
     *	check input parameter
     */
    if (aPage_BCBP == NULL || logRecInfo == NULL) ERR(handle, eBADPARAMETER);


    aPage = (BtreeOverflow*)aPage_BCBP->bufPagePtr;

    /*
     *	gent images
     */
    insertOidInfo = (LOG_Image_BtM_OidInOverflow_T*)logRecInfo->imageData[0];

    if (LSN_CMP_EQ(aPage->hdr.lsn, xactEntry->undoNextLsn)) {

        BTM_DELETE_OIDS_SPACE_FROM_OID_ARRAY(aPage->oid, aPage->hdr.nObjects, insertOidInfo->oidArrayElemNo, 1);
        aPage->hdr.nObjects --;


        /*
         *  make the compensation log record
         */
        LOG_FILL_LOGRECINFO_1(localLogRecInfo, logRecInfo->xactId, LOG_TYPE_COMPENSATION,
                              LOG_ACTION_BTM_DELETE_OID_FROM_OVERFLOW_PAGE, LOG_REDO_ONLY,
                              logRecInfo->pid, xactEntry->lastLsn, logRecInfo->prevLsn,
                              logRecInfo->imageSize[0], logRecInfo->imageData[0]);

        e = LOG_WriteLogRecord(handle, xactEntry, &localLogRecInfo, &lsn, &logRecLen);
        if (e < eNOERROR) ERR(handle, e);

        /* mark the lsn in the page */
        aPage->hdr.lsn = lsn;
        aPage->hdr.logRecLen = logRecLen;

        /*
         *	set dirty flag for buffering
         */
        aPage_BCBP->dirtyFlag = 1;

    } else {

#ifdef CCRL
	/*
         * We release the buffer page's latch in order to avid a deadlock situation of latches.
	 */
	e = SHM_releaseLatch(handle, aPage_BCBP->latchPtr, procIndex);
	if (e < eNOERROR) ERR(handle, e);

	aPage_BCBP->latchPtr = NULL;
#endif /* CCRL */

        kval.len = logRecInfo->imageSize[3];
        memcpy(kval.val, logRecInfo->imageData[3], kval.len);

        SET_LOG_PARAMETER(logParam, TRUE, FALSE);
        logParam.logFlag |= LOG_FLAG_UNDO;
        logParam.undo.undoLsn = *logRecLsn;
        logParam.undo.undoNextLsn = logRecInfo->prevLsn;

        /* set iinfoPtr */
        iinfoPtr = (LOG_Image_BtM_IndexInfo_T *)logRecInfo->imageData[1];

        /* get iinfo */
        iinfo.iid = iinfoPtr->iid;
        iinfo.tmpIndexFlag = FALSE;
        iinfo.catalog.oid = iinfoPtr->catEntry;

        e = BtM_DeleteObject(handle, xactEntry, &iinfo,
			     NULL, 
                             (KeyDesc*)logRecInfo->imageData[2],
                             &kval, &insertOidInfo->oid, NULL, &logParam);
        if (e < eNOERROR) ERR(handle, e);

    }

    return(eNOERROR);

} /* Undo_BtM_InsertOidIntoOverflowPage( ) */
