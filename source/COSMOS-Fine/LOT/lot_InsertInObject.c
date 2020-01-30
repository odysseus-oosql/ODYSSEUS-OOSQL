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
 * Module :	lot_InsertInObject.c
 *
 * Description :
 *  Insert data to the large object.
 *
 * Exports :
 *  Four lot_InsertInObject(Four, DataFileInfo*, PageID*, Four, Four, Four,
 *                          char*, L_O_T_ItemList*, Boolean*)
 */


#include "common.h"
#include "error.h"
#include "trace.h"
#include "latch.h"
#include "LOG.h"
#include "BfM.h"
#include "LOT.h"
#include "perProcessDS.h"
#include "perThreadDS.h"



/*@================================
 * lot_InsertInObject( )
 *================================*/
/*
 * Function: Four lot_InsertInObject(Four, DataFileInfo*, PageID*, Four, Fou, Four,
 *                          char*, L_O_T_ItemList*, Boolean*)
 *
 * Description :
 *  Insert data to the large object.
 *
 * Returns:
 *  Error codes
 *    eMEMORYALLOCERR_LOT - memory allocation error
 *    some errors caused by function calls
 *
 * Note:
 *  Caller should free the space allocated for the entries; itemList->entry
 *  if the overflow flag is set.
 */
Four lot_InsertInObject(
    Four handle,
    XactTableEntry_T *xactEntry, /* IN transaction table entry */
    DataFileInfo *finfo,	/* IN file information */
    PageID *nearPidForRoot,     /* IN near page for root */ 
    PageID   *root,		/* IN root page's PageID */
    L_O_T_INode *anode,         /* INOUT root node */
    Four     start,		/* IN starting offset to insert */
    Four     length,		/* IN amount of data to append */
    char     *data,		/* IN user buffer holding the data */
    L_O_T_ItemList *itemList,	/* OUT items to be inserted in the parent */
    Boolean  *overflow,		/* OUT overflow flag */
    Four *height,               /* OUT root node's height */
    LogParameter_T *logParam)   /* IN log parameter */
{
    Four e;			/* error number */
    Four	newStart;	/* start value in the subtree */
    L_O_T_INodePage *apage;     /* pointer to buffer holding the slotted page */
    Buffer_ACC_CB *apage_BCBP;
    Four	childIdx;	/* index of the subtree entry */
    PageID	childPid;	/* PageID of the root of subtree */
    Boolean	childOf;	/* overflow flag indicating child overflow */
    L_O_T_ItemList childList;	/* list to give the subtree for the overflowed items */
    Four i;
    Lsn_T lsn;                  /* lsn of the newly written log record */
    Four logRecLen;             /* log record length */
    LOG_LogRecInfo_T logRecInfo; /* log record information */
    LOG_Image_LOT_UpdateCountFields_T updateCountFieldsInfo; /* count fields update info */

    /* pointer for COMMON Data Structure of perThreadTable */
    COMMON_PerThreadDS_T *common_perThreadDSptr = COMMON_PER_THREAD_DS_PTR(handle);

    TR_PRINT(handle, TR_LOT, TR1, ("lot_InsertInObject()"));


    apage = NULL;               /* initialize for error handling */

    if (root != NULL) {
        /*@ read the page containing the root node into the buffer */
        e = BfM_getAndFixBuffer(handle, root, M_FREE, &apage_BCBP, PAGE_BUF);
        if (e < eNOERROR) ERR (handle, e);

        apage = (L_O_T_INodePage *)apage_BCBP->bufPagePtr;
        anode = &apage->node;
    }

    *overflow = FALSE;

    /* find the child pointer */
    childIdx = lot_SearchInNode(handle, anode, start);

    MAKE_PAGEID(childPid, finfo->fid.volNo, anode->entry[childIdx].spid);

    /* calculate the relative offset in the child node */
    newStart = start - ((childIdx == 0) ? 0:anode->entry[childIdx-1].count);

    if (anode->header.height > 1) {     /* internal node except the lowest */

	/*@ recursively calls the insert routine */
	e = lot_InsertInObject(handle, xactEntry, finfo, nearPidForRoot, &childPid, NULL, newStart,
			       length, data, &childList, &childOf, height, logParam); 
	if (e < 0) { PRTERR(handle, e); goto LABEL_Error; }

    } else {	/* in the case of the lowest internal node */
	/* prepare the localItemList */
	childList.nEntries = 2 + (length/LOT_LNODE_MAXFREE);
        childList.entryArrayPtr = LOT_GET_ENTRY_ARRAY(handle);
        e = Util_reallocVarArray(handle, childList.entryArrayPtr, sizeof(L_O_T_INodeEntry), childList.nEntries);
        if (e < 0) { PRTERR(handle, e); goto LABEL_Error; }

	childList.entry = (L_O_T_INodeEntry *)childList.entryArrayPtr->ptr;

	childList.entry[0].spid = anode->entry[childIdx].spid;
	childList.entry[0].count = lot_GetCount(handle, anode, childIdx);
	childList.nEntries = 1;
	childList.nReplaces = 1;

	/* insert the data into the data page */
	e = lot_InsertInDataPage(handle, xactEntry, finfo, newStart, length,
				 data, &childList, &childOf, logParam);
	if (e < 0) { PRTERR(handle, e); goto LABEL_Error; }
    }

    if (childOf) {

	/* Insert the overflowed items into the current node */
	e = lot_InsertInternal(handle, xactEntry, finfo, nearPidForRoot, apage, anode, childIdx,
			       &childList, itemList, overflow, logParam); 
	if (e < 0) { PRTERR(handle, e); goto LABEL_Error; }

    } else {

        /*
         * Write log record.
         */
        if (logParam->logFlag & LOG_FLAG_DATA_LOGGING && root != NULL) {

            updateCountFieldsInfo.start = childIdx;
            updateCountFieldsInfo.delta = length;

            LOG_FILL_LOGRECINFO_1(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                                  LOG_ACTION_LOT_UPDATE_COUNT_FIELDS, LOG_REDO_UNDO,
                                  *root, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                                  sizeof(LOG_Image_LOT_UpdateCountFields_T), &updateCountFieldsInfo);

            e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
            if (e < eNOERROR) ERRB1(handle, e, apage_BCBP, PAGE_BUF);

            /* mark the lsn in the page */
            apage->header.lsn = lsn;
            apage->header.logRecLen = logRecLen;
        }

	/*@ update the entry counts */
	for (i = childIdx; i < anode->header.nEntries; i++)
	    anode->entry[i].count += length;

    }

    *height = anode->header.height;    /* return height */

    if (root != NULL) {
        apage_BCBP->dirtyFlag = 1;

        e = BfM_unfixBuffer(handle, apage_BCBP, PAGE_BUF);
        if (e < eNOERROR) ERR(handle, e);
    }

    return (eNOERROR);

  LABEL_Error:
    if (apage != NULL) {
        (Four)BfM_unfixBuffer(handle, apage_BCBP, PAGE_BUF);
    }
    return(e);

} /* lot_InsertInObject() */
