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
 * Module: Undo_MLGF_InsertObjectsIntoOverflowPage.c
 *
 * Description:
 *  Undo deleting a sequence of objects from an overflow page
 *
 * Exports:
 *  Four Undo_MLGF_InsertObjectsIntoOverflowPage(Four, LOG_LogRecInfo_T*)
 */


#include <string.h>
#include "common.h"
#include "error.h"
#include "trace.h"
#include "TM.h"
#include "LOG.h"
#include "BfM.h"
#include "MLGF.h"
#include "perProcessDS.h"
#include "perThreadDS.h"


Four Undo_MLGF_InsertObjectsIntoOverflowPage(
    Four handle,
    XactTableEntry_T *xactEntry, /* IN transaction table entry */
    Buffer_ACC_CB *aPage_BCBP,  /* INOUT buffer access control block holding data */
    Lsn_T *logRecLsn,           /* IN log record to undo */
    LOG_LogRecInfo_T *logRecInfo) /* IN operation information for writing a small object */
{
    Four e;			/* error code */
    mlgf_OverflowPage *aPage;   /* pointer to a slotted buffer page */
    Four objectItemLen;
    Lsn_T lsn;                  /* lsn of the newly written log record */
    Four logRecLen;             /* log record length */
    LOG_LogRecInfo_T localLogRecInfo; /* log record information */
    LOG_Image_MLGF_ObjectsInOverflow_T *objsInfo;


    TR_PRINT(handle, TR_UNDO, TR1, ("Undo_MLGF_InsertObjectsIntoOverflowPage()"));


    /*
     *	check input parameter
     */
    if (aPage_BCBP == NULL || logRecInfo == NULL) ERR(handle, eBADPARAMETER);


    /* Get the length of an object item. */
    objectItemLen = MLGF_LEAFENTRY_OBJECTITEM_LEN(aPage->hdr.extraDataLen);


    /*
     *	get images
     */
    aPage = (mlgf_OverflowPage*)aPage_BCBP->bufPagePtr;
    objsInfo = (LOG_Image_MLGF_ObjectsInOverflow_T*)logRecInfo->imageData[0];


    /* Delete the objects */
    MLGF_DELETE_OBJECTS_SPACE_FROM_OBJECT_ARRAY(
        MLGF_OVERFLOW_ITH_OBJECTITEM(objectItemLen, aPage, 0),
        aPage->hdr.nObjects, objsInfo->startObjArrayElemNo,
        objsInfo->nObjects, objectItemLen);

    /* decrease the # of objects */
    aPage->hdr.nObjects -= objsInfo->nObjects;


    /*
     *  make the compensation log record
     */
    LOG_FILL_LOGRECINFO_1(localLogRecInfo, logRecInfo->xactId, LOG_TYPE_COMPENSATION,
                          LOG_ACTION_MLGF_DELETE_OBJECTS_FROM_OVERFLOW_PAGE, LOG_REDO_ONLY,
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

    return(eNOERROR);

} /* Undo_MLGF_InsertObjectsIntoOverflowPage( ) */
