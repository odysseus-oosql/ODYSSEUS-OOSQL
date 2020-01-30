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
 * Module: btm_Overflow.c
 *
 * Description :
 *  This file has five functions which are concerned with maintaining overflow
 *  pages. A new overflow page is created when the size of a leaf item becomes
 *  greater than a third of a page size.  If the member of ObjectIDs having the
 *  same key value grows more than one page limit, the page should be splitted
 *  by two pages and they are connected by doubly linked list. The deleting a
 *  ObjectID may occur an underflow of an overflow page. If it occurs, overflow
 *  pages may be merged or redistributed.
 *
 * Exports:
 *  Four btm_CreateOverflow(Four, Buffer_ACC_CB*, Four, ObjectID*, KeyDesc*, KeyValue*)
 *  Four btm_InsertOverflow(Four, PageID*, ObjectID*, KeyDesc*, KeyValue*)
 *  Four btm_DeleteOverflow(Four, PageID*, ObjectID*, Four*, LocalPool*, DeallocListElem*, KeyDesc*, KeyValue*)
 */


#include <string.h>
#include "common.h"
#include "error.h"
#include "trace.h"
#include "Util.h"
#include "latch.h"
#include "TM.h"
#include "LOG.h"
#include "RDsM.h"
#include "BfM.h"
#include "BtM.h"
#include "perProcessDS.h"
#include "perThreadDS.h"


/* Internal Function Prototypes */
Four btm_OverflowDistribute(Four, XactTableEntry_T*, BtreeOverflow*, BtreeOverflow*, LogParameter_T*);
Four btm_OverflowMerge(Four, XactTableEntry_T*, BtreeIndexInfo*, BtreeOverflow*, BtreeOverflow*, LogParameter_T*); 
Four btm_SplitOverflow(Four, XactTableEntry_T*, BtreeIndexInfo*, BtreeOverflow*, LogParameter_T*);



/*
 * Function: Four btm_CreateOverflow(Four, ObjectID*, BtreeLeaf*, Four, ObjectID*, KeyDesc*, KeyValue*)
 *
 * Description:
 *  This function created a new overflow page. Ar first, that is, ObjectIDs
 *  having the same key value are moved from the leaf page to the newly
 *  allocated overflow page.
 *  After the new page was made, the given ObjectID is inserted using the insert
 *  routine.
 *  Sinve it is necessary to mark that over flow pages are used, the item in
 *  the leaf should be updated.
 *  ( The ordinary leaf item :
 *       <key length, key value, # of ObjectIDs, ObjectID list>
 *    The leaf item using overflow page :
 *       <key length, key value, ShortPageID of Overflow page> )
 *
 * Returns:
 *  Error code
 *    some errors caused by function calls
 */
Four btm_CreateOverflow(
    Four handle,
    XactTableEntry_T *xactEntry, /* IN transaction table entry */
    BtreeIndexInfo *iinfo,       /* IN Btree Index Info */
    Buffer_ACC_CB *leaf_BCB,	/* IN BCB for leaf page including the leaf item */
    Four      slotNo,		/* IN slot to be converted into overflow page */
    LogParameter_T *logParam)   /* IN log parameter */
{
    Four e;			/* error number */
    Four offset;		/* starting offset of ObjectID array or PageID of overflow page */
    PageID newPid;		/* a New PageID for a new overflow page */
    ObjectID *oidArray;		/* array of ObjectIDs */
    BtreeLeaf *apage;		/* pointer to buffer page for leaf */
    BtreeOverflow *opage;	/* pointer to buffer page for overflow */
    btm_LeafEntry *lEntry;	/* a leaf entry */
    Buffer_ACC_CB *opage_BCBP;	/* buffer control block for overflow page */
    Lsn_T lsn;                  /* lsn of the newly written log record */
    Four logRecLen;             /* log record length */
    LOG_LogRecInfo_T logRecInfo; /* log record information */
    LOG_Image_BtM_InitOverflowPage_T initOverflowPageInfo;
    LOG_Image_BtM_OidsInOverflow_T oidsInfo;
    LOG_Image_BtM_ChangeLeafEntry_T changeLeafEntryInfo;
    SegmentID_T pageSegmentID;  /* page segment ID */

    /* pointer for COMMON Data Structure of perThreadTable */
    COMMON_PerThreadDS_T *common_perThreadDSptr = COMMON_PER_THREAD_DS_PTR(handle);

    TR_PRINT(handle, TR_BTM, TR1, ("btm_CreateOverflow()"));

    /*
     * Begin new nested top action
     */
    e = TM_XT_BeginNestedTopAction(handle, xactEntry, &xactEntry->lastLsn);
    if (e < eNOERROR) ERR(handle, e);

    apage = (BtreeLeaf*)leaf_BCB->bufPagePtr;

    /* Allocat a new page and initialize it as an overflow page. */
    e = btm_GetSegmentIDFromIndexInfo(handle, xactEntry, iinfo, &pageSegmentID, PAGESIZE2);
    if (e < eNOERROR) ERR(handle, e);

    e = RDsM_AllocTrains(handle, xactEntry, apage->hdr.pid.volNo, &pageSegmentID, &apage->hdr.pid, 1, PAGESIZE2, FALSE, &newPid, logParam);
    if (e < eNOERROR) ERR(handle, e);

    /* The new page is fixed in the buffer pool and is X latched. */
    e = BfM_fixNewBuffer(handle, &newPid, M_EXCLUSIVE, &opage_BCBP, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);

    opage = (BtreeOverflow*)opage_BCBP->bufPagePtr;

    BTM_INIT_OVERFLOW_PAGE(opage, apage->hdr.iid, newPid, NIL, NIL);

    lEntry = (btm_LeafEntry*)&(apage->data[apage->slot[-slotNo]]);
    offset = ALIGNED_LENGTH(BTM_LEAFENTRY_FIXED + lEntry->klen) - BTM_LEAFENTRY_FIXED;

    /* Copy ObjectIDs from the leaf to the new overflow page */
    oidArray = (ObjectID*)&(lEntry->kval[offset]);
    memcpy(&(opage->oid[0]), &(oidArray[0]), lEntry->nObjects*OBJECTID_SIZE);
    opage->hdr.nObjects = lEntry->nObjects;

    /*
     * Write log record.
     */
    if (logParam->logFlag & LOG_FLAG_DATA_LOGGING) {

        initOverflowPageInfo.iid = opage->hdr.iid;
        initOverflowPageInfo.prevPage = NIL;
        initOverflowPageInfo.nextPage = NIL;

        LOG_FILL_LOGRECINFO_1(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                              LOG_ACTION_BTM_INIT_OVERFLOW_PAGE, LOG_REDO_ONLY,
                              opage->hdr.pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                              sizeof(LOG_Image_BtM_InitOverflowPage_T), &initOverflowPageInfo);

        e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
        if (e < eNOERROR) ERR(handle, e); 


        oidsInfo.startOidArrayElemNo = 0;
        oidsInfo.nObjects = lEntry->nObjects;

        LOG_FILL_LOGRECINFO_2(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                              LOG_ACTION_BTM_INSERT_OIDS_INTO_OVERFLOW_PAGE, LOG_REDO_ONLY,
                              opage->hdr.pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                              sizeof(LOG_Image_BtM_OidsInOverflow_T), &oidsInfo,
                              lEntry->nObjects*OBJECTID_SIZE, &opage->oid[0]);

        e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
        if (e < eNOERROR) ERR(handle, e); 

        opage->hdr.lsn = lsn;
        opage->hdr.logRecLen = logRecLen;
    } else {
        INCREASE_LSN_BY_ONE(opage->hdr.lsn); 
    }

    /* Set the dirty flag of the overflow page. */
    opage_BCBP->dirtyFlag = 1;

    /* Unlatch and unfix the buffer for the overflow. */
    e = SHM_releaseLatch(handle, opage_BCBP->latchPtr, procIndex);
    if (e < eNOERROR) ERR(handle, e); 

    e = BfM_unfixBuffer(handle, opage_BCBP, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);


    /*
     * Write log record.
     */
    if (logParam->logFlag & LOG_FLAG_DATA_LOGGING) {

        changeLeafEntryInfo.slotNo = slotNo;
        changeLeafEntryInfo.deltaOfObjectArrayAreaSize = sizeof(PageNo) - lEntry->nObjects * OBJECTID_SIZE;
        changeLeafEntryInfo.deltaInNumOfObjects = NIL - lEntry->nObjects;

        LOG_FILL_LOGRECINFO_3(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                              LOG_ACTION_BTM_CHANGE_LEAF_ENTRY, LOG_REDO_UNDO,
                              apage->hdr.pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                              sizeof(LOG_Image_BtM_ChangeLeafEntry_T), &changeLeafEntryInfo,
                              sizeof(PageNo), &newPid.pageNo,
                              lEntry->nObjects*OBJECTID_SIZE, oidArray);

        e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
        if (e < eNOERROR) ERR(handle, e);

        apage->hdr.lsn = lsn;
        apage->hdr.logRecLen = logRecLen;
    } else {
        INCREASE_LSN_BY_ONE(apage->hdr.lsn); 
    }

    /*@ change the 'unused' field of rpage */
    apage->hdr.unused += lEntry->nObjects*OBJECTID_SIZE - sizeof(ShortPageID);

    /* The leaf page has an overflow PageID instead of ObjectID list */
    lEntry->nObjects = NIL;
    *((ShortPageID*)&(lEntry->kval[offset])) = newPid.pageNo;

    /*
     * End the nested top action.
     */
    e = TM_XT_EndNestedTopAction(handle, xactEntry, logParam);
    if (e < eNOERROR) ERR(handle, e);

    /* Set the dirty flag of the leaf page. */
    leaf_BCB->dirtyFlag = 1;

    return(eNOERROR);

} /* btm_CreateOverflow() */



/*
 * Function: Four btm_InsertOverflow(Four, PageID*, ObjectID*, KeyDesc*, KeyValue*)
 *
 * Description:
 *  Insert the new ObjectID into the given overflow page.
 *  At first, it find out the correct position to be inserted using the binary
 *  search routine, and then insert it if there is enough space. If there is
 *  not enough space in the given page, split it and insert the ObjectID
 *  by using btm_SplitOverflow().
 *
 * Returns:
 *  Error code
 *    eDUPLICATEDOBJECTID_BTM
 *    some errors caused by function calls
 */
Four btm_InsertOverflow(
    Four handle,
    XactTableEntry_T *xactEntry, /* IN transaction table entry */
    BtreeIndexInfo* iinfo,	/* IN index information */ 
    PageID   *ovPid,		/* IN where the ObjectID to be inserted */
    KeyDesc   *kdesc,		/* IN Btree key descriptor */
    KeyValue  *kval,		/* IN key value */
    ObjectID *oid,		/* IN ObjectID to be inserted */
    LogParameter_T *logParam)   /* IN log parameter */
{
    Four e;			/* error number */
    Four idx;			/* the position to be inserted */
    ObjectID curOid;		/* The current ObjectID which is examined */
    PageID nextPid;		/* the Next Overflow Page */
    BtreeOverflow *apage;	/* Page Pointer to the overflow page */
    Boolean found;		/* search result */
    Buffer_ACC_CB *opage_BCBP;	/* buffer control block for overflow page */
    Lsn_T lsn;                  /* lsn of the newly written log record */
    Four logRecLen;             /* log record length */
    LOG_LogRecInfo_T logRecInfo; /* log record information */
    LOG_Image_BtM_OidInOverflow_T insertOidInfo;

    /* pointer for COMMON Data Structure of perThreadTable */
    COMMON_PerThreadDS_T *common_perThreadDSptr = COMMON_PER_THREAD_DS_PTR(handle);


    TR_PRINT(handle, TR_BTM, TR1,
             ("btm_InsertOverflow(ovPid=%P, oid=%P)", ovPid, oid));


    e = BfM_getAndFixBuffer(handle, ovPid, M_EXCLUSIVE, &opage_BCBP, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);

    apage = (BtreeOverflow*)opage_BCBP->bufPagePtr;

    /* The last ObjectID in the given overflow page */
    curOid = apage->oid[apage->hdr.nObjects-1];

    if (btm_ObjectIdComp(handle, oid, &curOid) == GREAT && apage->hdr.nextPage != NIL) {
        /* insert it into the next page */
	MAKE_PAGEID(nextPid, ovPid->volNo, apage->hdr.nextPage);

        e = SHM_releaseLatch(handle, opage_BCBP->latchPtr, procIndex);
        if (e < eNOERROR) ERR(handle, e);

        e = BfM_unfixBuffer(handle, opage_BCBP, PAGE_BUF);
        if (e < eNOERROR) ERR(handle, e);

        e = btm_InsertOverflow(handle, xactEntry, iinfo, &nextPid, kdesc, kval, oid, logParam); 
        if (e < eNOERROR) ERR(handle, e);

        return(eNOERROR);
    }

    /* Search the correct postion to be inserted using the binary search */
    found = btm_BinarySearchOidArray(handle, &(apage->oid[0]), oid, apage->hdr.nObjects, &idx);
    if (found) ERRBL1(handle, eDUPLICATEDOBJECTID_BTM, opage_BCBP, PAGE_BUF);

    if (apage->hdr.nObjects < BO_MAXOBJECTIDS) {

        /*
         * Write log record.
         */
        if (logParam->logFlag & LOG_FLAG_DATA_LOGGING) {

            insertOidInfo.oidArrayElemNo = idx + 1;
            insertOidInfo.oid = *oid;

            if (logParam->logFlag & LOG_FLAG_UNDO) {
                LOG_FILL_LOGRECINFO_1(logRecInfo, xactEntry->xactId, LOG_TYPE_COMPENSATION,
                                      LOG_ACTION_BTM_INSERT_OID_INTO_OVERFLOW_PAGE, LOG_REDO_ONLY,
                                      apage->hdr.pid, xactEntry->lastLsn, logParam->undo.undoNextLsn,
                                      sizeof(LOG_Image_BtM_OidInOverflow_T), &insertOidInfo);
            } else {
                LOG_Image_BtM_IndexInfo_T logImage;

                /* set logImage */
                logImage.iid = iinfo->iid;
                logImage.catEntry = iinfo->catalog.oid;

                LOG_FILL_LOGRECINFO_4(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                                      LOG_ACTION_BTM_INSERT_OID_INTO_OVERFLOW_PAGE, LOG_REDO_UNDO,
                                      apage->hdr.pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                                      sizeof(LOG_Image_BtM_OidInOverflow_T), &insertOidInfo,
                                      sizeof(LOG_Image_BtM_IndexInfo_T), &logImage,
                                      KEYDESC_USED_SIZE(kdesc), kdesc,
                                      kval->len, kval->val);
            }

            e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
            if (e < eNOERROR) ERR(handle, e); 

            apage->hdr.lsn = lsn;
            apage->hdr.logRecLen = logRecLen;
        } else {
            INCREASE_LSN_BY_ONE(apage->hdr.lsn); 
        }

	/* Make room for the ObjectID */
        BTM_INSERT_OIDS_SPACE_INTO_OID_ARRAY(apage->oid, apage->hdr.nObjects, idx+1, 1);

	/* Save the ObjectID and decrease free area */
	apage->oid[idx+1] = *oid;
	(apage->hdr.nObjects)++;

    } else {	/* not enough */

	/* Insert it after split */
	e = btm_SplitOverflow(handle, xactEntry, iinfo, apage, logParam); 
        if (e < eNOERROR) ERR(handle, e);

        e = btm_InsertOverflow(handle, xactEntry, iinfo, ovPid, kdesc, kval, oid, logParam); 
        if (e < eNOERROR) ERR(handle, e); 
    }

    opage_BCBP->dirtyFlag = 1;

    e = SHM_releaseLatch(handle, opage_BCBP->latchPtr, procIndex);
    if (e < eNOERROR) ERR(handle, e);

    e = BfM_unfixBuffer(handle, opage_BCBP, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);

    return(eNOERROR);

} /* btm_InsertOverflow() */




/*
 * Function: Four btm_DeleteOverflow(Four, PageID*, ObjectID*, Four*, LocalPool*, DeallocListElem*, KeyDesc*, KeyValue*)
 *
 * Description:
 *  This function deletes the given ObjectID from the given overflow page.
 *  If the page after deleting is not half full, the page and the neighboring
 *  page are merged or redistributed. After deleting, if there is only one
 *  overflow page and its size is less than a fourth of a page, then 'f' is
 *  assigned to the # of ObjectIDs, otherwise it becomes FALSE.
 *
 * Returns:
 *  Error code
 *    eNOTFOUND
 *    some errors caused by function calls
 */
Four btm_DeleteOverflow(
    Four handle,
    XactTableEntry_T *xactEntry, /* IN transaction table entry */
    BtreeIndexInfo* iinfo,	/* IN index information */ 
    PageID   *ovPid,		/* IN The given ObjectID will be deleted from this page */
    KeyDesc *kdesc,           /* IN key description for logging */
    KeyValue *kval,         /* IN key value for logging */
    ObjectID *oid,		/* IN ObjectID to be deleted */
    Four     *f,		/* OUT # of ObjectIDs if underflow occur
                                       otherwise, max number of object ids */
    LogParameter_T *logParam)   /* IN log parameter */
{
    Four e;			/* error number */
    Four idx;			/* nth ObjectID in the overflow page */
    PageID prevPid;		/* The previous Page of the Overflow Page */
    PageID nextPid;		/* The nextPage of the given overflow page */
    ObjectID curOid;		/* The current ObjectID which is examined */
    BtreeOverflow *mpage;	/* Page Pointer to the neighbor page */
    PageID *mpid;               /* page id of the neighbor page */
    Buffer_ACC_CB *mpage_BCBP;	/* buffer access control block containing data */
    BtreeOverflow *opage;	/* Page Pointer to the given overflow page */
    Buffer_ACC_CB *opage_BCBP;	/* buffer access control block containing data */
    Boolean found;		/* search result */
    BtreeOverflow *leftPage;    /* left page of between two overflow pages */
    BtreeOverflow *rightPage;   /* right page of between two overflow pages */
    PageID *rightPid;           /* page id of the right page */
    Lsn_T lsn;                  /* lsn of the newly written log record */
    Four logRecLen;             /* log record length */
    LOG_LogRecInfo_T logRecInfo; /* log record information */
    LOG_Image_BtM_OidInOverflow_T deleteOidInfo;

    /* pointer for COMMON Data Structure of perThreadTable */
    COMMON_PerThreadDS_T *common_perThreadDSptr = COMMON_PER_THREAD_DS_PTR(handle);


    TR_PRINT(handle, TR_BTM, TR1, ("btm_DeleteOverflow()"));


    /* Get and fix buffer for the overflow page. */
    e = BfM_getAndFixBuffer(handle, ovPid, M_EXCLUSIVE, &opage_BCBP, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);

    opage = (BtreeOverflow *)opage_BCBP->bufPagePtr;

    /* curOid is the last ObjectID of the given overflow page */
    curOid = opage->oid[opage->hdr.nObjects - 1];

    MAKE_PAGEID(nextPid, ovPid->volNo, opage->hdr.nextPage);

    if(btm_ObjectIdComp(handle, oid, &curOid) == GREAT) { /* Not exist in this page */

        e = SHM_releaseLatch(handle, opage_BCBP->latchPtr, procIndex);
        if (e < eNOERROR) ERR(handle, e); 

        e = BfM_unfixBuffer(handle, opage_BCBP, PAGE_BUF);
        if (e < eNOERROR) ERR(handle, e);

        /* Recursively delete the ObjectID using the next page */
        e = btm_DeleteOverflow(handle, xactEntry, iinfo, &nextPid, kdesc, kval, oid, f, logParam); 
        if( e < eNOERROR ) ERR(handle, e);

        *f = BO_MAXOBJECTIDS;	/* There is one more page */

        return(eNOERROR);
    }

    /* The deleted object is in this page, if exist. */

    /*@ Search the ObjectID */
    found = btm_BinarySearchOidArray(handle, &(opage->oid[0]), oid, opage->hdr.nObjects, &idx);

    if (!found) ERRBL1(handle, eNOTFOUND, opage_BCBP, PAGE_BUF);


    /*
     * Write log record.
     */
    if (logParam->logFlag & LOG_FLAG_DATA_LOGGING) {

        deleteOidInfo.oidArrayElemNo = idx;
        deleteOidInfo.oid = *oid;

        if (logParam->logFlag & LOG_FLAG_UNDO) {
            LOG_FILL_LOGRECINFO_1(logRecInfo, xactEntry->xactId, LOG_TYPE_COMPENSATION,
                                  LOG_ACTION_BTM_DELETE_OID_FROM_OVERFLOW_PAGE, LOG_REDO_ONLY,
                                  opage->hdr.pid, xactEntry->lastLsn, logParam->undo.undoNextLsn,
                                  sizeof(LOG_Image_BtM_OidInOverflow_T), &deleteOidInfo);
        } else {
            LOG_Image_BtM_IndexInfo_T logImage;

            /* set logImage */
            logImage.iid = iinfo->iid;
            logImage.catEntry = iinfo->catalog.oid;

            LOG_FILL_LOGRECINFO_4(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                                  LOG_ACTION_BTM_DELETE_OID_FROM_OVERFLOW_PAGE, LOG_REDO_UNDO,
                                  opage->hdr.pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                                  sizeof(LOG_Image_BtM_OidInOverflow_T), &deleteOidInfo,
                                  sizeof(LOG_Image_BtM_IndexInfo_T), &logImage,
                                  KEYDESC_USED_SIZE(kdesc), kdesc,
                                  kval->len, kval->val);
        }

        e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
        if (e < eNOERROR) ERR(handle, e);

        opage->hdr.lsn = lsn;
        opage->hdr.logRecLen = logRecLen;
    } else {
        INCREASE_LSN_BY_ONE(opage->hdr.lsn);
    }

    /* Get the previous page id */
    MAKE_PAGEID(prevPid, ovPid->volNo, opage->hdr.prevPage);

    /* Delete the ObjectID */
    BTM_DELETE_OIDS_SPACE_FROM_OID_ARRAY(opage->oid, opage->hdr.nObjects, idx, 1);

    /*@ update the variables of the page */
    (opage->hdr.nObjects)--;

    /* Initialize 'f' to the max # of objects in the overflow page. */
    *f = BO_MAXOBJECTIDS;

    if (opage->hdr.nObjects < HALF_OF_OBJECTS) {	/* Underflow */

        /*
         * Get the neighbor(= mpid) to merge or redistribute.
         */
        if (nextPid.pageNo == NIL) {
            if (prevPid.pageNo == NIL) /* There is no neighbor. */
                mpid = (PageID*)NULL;
            else /* The previous page is used as the neighbor */
                mpid = &prevPid;
        } else {
            /* The next page is used as the neighbor. */
            mpid = &nextPid;
        }

        /*
         * Merge/Redistriubte the underflowed page with the neighbor.
         */
        if (mpid == NULL) {
            /*
             * If the overflow page is underflow (and there is no
             * neighbor), set 'f' to the number of objects in 'opage'.
             */
            *f = opage->hdr.nObjects;
        } else {

            /* The previous page is used by the neighboring page */
            e = BfM_getAndFixBuffer(handle, mpid, M_EXCLUSIVE, &mpage_BCBP, PAGE_BUF);
            if (e < eNOERROR) ERR(handle, e); 

            mpage = (BtreeOverflow *)mpage_BCBP->bufPagePtr;

            if (mpid->pageNo == prevPid.pageNo) {
                leftPage = mpage;
                rightPage = opage;
                rightPid = ovPid;
            } else {
                leftPage = opage;
                rightPage = mpage;
                rightPid = &nextPid;
            }

            if((leftPage->hdr.nObjects + rightPage->hdr.nObjects) <= BO_MAXOBJECTIDS) {
                /* The sum of the two pages is less than a page size */
                e = btm_OverflowMerge(handle, xactEntry, iinfo, leftPage, 
                                      rightPage, logParam);
                if (e < eNOERROR) ERR(handle, e); 
                } else { /* more than one page size */
                    e = btm_OverflowDistribute(handle, xactEntry, leftPage, rightPage, logParam);
                    if (e < eNOERROR) ERR(handle, e); 
                }

            mpage_BCBP->dirtyFlag = 1;

            e = SHM_releaseLatch(handle, mpage_BCBP->latchPtr, procIndex);
            if (e < eNOERROR) ERR(handle, e); 

            e = BfM_unfixBuffer(handle, mpage_BCBP, PAGE_BUF);
            if (e < eNOERROR) ERR(handle, e); 
        }
    }

    opage_BCBP->dirtyFlag = 1;

    e = SHM_releaseLatch(handle, opage_BCBP->latchPtr, procIndex);
    if (e < eNOERROR) ERR(handle, e); 

    /*@ unfix the buffer */
    e = BfM_unfixBuffer(handle, opage_BCBP, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);

    return(eNOERROR);

} /* btm_DeleteOverflow() */



/*
 * Function: Four btm_OverflowMerge(Four, Buffer_ACC_CB*, Buffer_ACC_CB*, LocalPool*,
 *                                  DeallocListElem*)
 *
 * Description:
 *  All ObjectIDs in the second page(page2) are copied to the first page(page1).
 *  After the copy is completed, 'nextPage' and 'prevPage' are updated to
 *  maintain doubly linked list. Finally, deallocate the the second page.
 *
 * Returns:
 *  Error number
 *    some errors caused by function calls
 */
Four btm_OverflowMerge(
    Four	handle,
    XactTableEntry_T *xactEntry, /* IN transaction table entry */
    BtreeIndexInfo* iinfo,	/* IN index information */ 
    BtreeOverflow *page1,	  /* INOUT The page to be copied to */
    BtreeOverflow *page2,	  /* INOUT The page to be copied from */
    LogParameter_T *logParam)   /* IN log parameter */
{
    Four e;			/* error number */
    PageID  nextPid;		/* The next page of page2 */
    BtreeOverflow *npage;	/* Page Pointer to the overflow page */
    Buffer_ACC_CB *npage_BCBP;	/* buffer access control block containing data */
    Lsn_T lsn;                  /* lsn of the newly written log record */
    Four logRecLen;             /* log record length */
    LOG_LogRecInfo_T logRecInfo; /* log record information */
    LOG_Image_BtM_OidsInOverflow_T oidsInfo;

    /* pointer for COMMON Data Structure of perThreadTable */
    COMMON_PerThreadDS_T *common_perThreadDSptr = COMMON_PER_THREAD_DS_PTR(handle);


    TR_PRINT(handle, TR_BTM, TR1, ("btm_OverflowMerge()"));


    e = TM_XT_BeginNestedTopAction(handle, xactEntry, &xactEntry->lastLsn);
    if (e < eNOERROR) ERR(handle, e);

    /*
     * Write log record.
     */
    if (logParam->logFlag & LOG_FLAG_DATA_LOGGING) {

        LOG_Image_BtM_IndexInfo_T logImage; 

        /*
         * logging for page1
         */
        oidsInfo.startOidArrayElemNo = page1->hdr.nObjects;
        oidsInfo.nObjects = page2->hdr.nObjects;

        LOG_FILL_LOGRECINFO_2(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                              LOG_ACTION_BTM_INSERT_OIDS_INTO_OVERFLOW_PAGE, LOG_REDO_UNDO,
                              page1->hdr.pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                              sizeof(LOG_Image_BtM_OidsInOverflow_T), &oidsInfo,
                              page2->hdr.nObjects*OBJECTID_SIZE, &page2->oid[0]);

        e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
        if (e < eNOERROR) ERR(handle, e);


        LOG_FILL_LOGRECINFO_2(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                              LOG_ACTION_BTM_MODIFY_OVERFLOW_CHAIN_NEXT_LINK, LOG_REDO_UNDO,
                              page1->hdr.pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                              sizeof(ShortPageID), &page2->hdr.nextPage,
                              sizeof(ShortPageID), &page1->hdr.nextPage);

        e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
        if (e < eNOERROR) ERR(handle, e);

        page1->hdr.lsn = lsn;
        page1->hdr.logRecLen = logRecLen;


        /*
         * logging for page2
         */
        /* set logImage */
        logImage.iid = iinfo->iid;
        logImage.catEntry = iinfo->catalog.oid;

        LOG_FILL_LOGRECINFO_2(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
			      LOG_ACTION_BTM_MODIFY_INDEXID_OF_BTREE_PAGE, LOG_REDO_UNDO, page2->hdr.pid, xactEntry->lastLsn,
			      common_perThreadDSptr->nilLsn,
                              sizeof(IndexID), &(common_perThreadDSptr->nilIid),
			      sizeof(LOG_Image_BtM_IndexInfo_T), &logImage);

        e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
        if (e < eNOERROR) ERR(handle, e);

        page2->hdr.lsn = lsn;
        page2->hdr.logRecLen = logRecLen;
    } else {
        INCREASE_LSN_BY_ONE(page1->hdr.lsn); 
        INCREASE_LSN_BY_ONE(page2->hdr.lsn); 
    }


    /* Copy the contents of the page2 to the page1 */
    memcpy(&(page1->oid[page1->hdr.nObjects]), &(page2->oid[0]),
	   page2->hdr.nObjects*OBJECTID_SIZE);

    page1->hdr.nObjects += page2->hdr.nObjects;

    /* Update links */
    MAKE_PAGEID(nextPid, page2->hdr.pid.volNo, page2->hdr.nextPage);
    page1->hdr.nextPage = nextPid.pageNo;
    if (nextPid.pageNo != NIL) {
	e = BfM_getAndFixBuffer(handle, &nextPid, M_FREE, &npage_BCBP, PAGE_BUF);
	if (e < eNOERROR) ERR(handle, e);

	npage = (BtreeOverflow *)npage_BCBP->bufPagePtr;

        /*
         * Write log record.
         */
        if (logParam->logFlag & LOG_FLAG_DATA_LOGGING) {

            LOG_FILL_LOGRECINFO_2(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                                  LOG_ACTION_BTM_MODIFY_OVERFLOW_CHAIN_PREV_LINK, LOG_REDO_UNDO,
                                  npage->hdr.pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                                  sizeof(ShortPageID), &page2->hdr.prevPage,
                                  sizeof(ShortPageID), &npage->hdr.prevPage);

            e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
            if (e < eNOERROR) ERR(handle, e); 

            npage->hdr.lsn = lsn;
            npage->hdr.logRecLen = logRecLen;
        } else {
            INCREASE_LSN_BY_ONE(npage->hdr.lsn); 
        }

	npage->hdr.prevPage = page2->hdr.prevPage;

	npage_BCBP->dirtyFlag = 1;

	e = BfM_unfixBuffer(handle, npage_BCBP, PAGE_BUF);
	if (e < eNOERROR) ERR(handle, e);
    }

    SET_NILINDEXID(page2->hdr.iid);

    e = RDsM_FreeTrain(handle, xactEntry, &page2->hdr.pid, PAGESIZE2, FALSE, logParam);
    if (e < eNOERROR) ERR(handle, e);

    e = TM_XT_EndNestedTopAction(handle, xactEntry, logParam);
    if (e < eNOERROR) ERR(handle, e);

    return(eNOERROR);

} /* btm_OverflowMerge() */



/*
 * Function: Four btm_OverflowDistribute(Four, Buffer_ACC_CB*, Buffer_ACC_CB*)
 *
 * Description:
 *  If the size of the first page is larger than the second page, ObjectIDs are
 *  moved from the first page to the second page, otherwise from the second
 *  page to the first page.
 *
 * Returns:
 *  None
 */
Four btm_OverflowDistribute(
    Four	handle,
    XactTableEntry_T *xactEntry, /* IN transaction table entry */
    BtreeOverflow *page1,	/* INOUT The first page to be redistributed */
    BtreeOverflow *page2,	/* INOUT The second page to be redistributed */
    LogParameter_T *logParam)   /* IN log parameter */
{
    Four e;                     /* error code */
    Four nMovedObjects;		/* # of objects to move */
    BtreeOverflow *srcPage;     /* pointer to the source page */
    BtreeOverflow *dstPage;     /* pointer to the destination page */
    Four srcOidArrayElemNo;     /* starting position of delete */
    Four dstOidArrayElemNo;     /* starting position of insert */
    Lsn_T lsn;                  /* lsn of the newly written log record */
    Four logRecLen;             /* log record length */
    LOG_LogRecInfo_T logRecInfo; /* log record information */
    LOG_Image_BtM_OidsInOverflow_T oidsInfo;

    /* pointer for COMMON Data Structure of perThreadTable */
    COMMON_PerThreadDS_T *common_perThreadDSptr = COMMON_PER_THREAD_DS_PTR(handle);

    TR_PRINT(handle, TR_BTM, TR1, ("btm_OverflowDistribute()"));


    if( page1->hdr.nObjects > page2->hdr.nObjects ) {	/* page1 -> page2 */
	/*@ number of objects to move */
	nMovedObjects = (page1->hdr.nObjects - page2->hdr.nObjects)/2;


        /* for writing log */
        srcPage = page1;
        dstPage = page2;

        srcOidArrayElemNo = srcPage->hdr.nObjects - nMovedObjects;
        dstOidArrayElemNo = 0;


	/* reserve space in page2 */
        BTM_INSERT_OIDS_SPACE_INTO_OID_ARRAY(page2->oid, page2->hdr.nObjects, 0, nMovedObjects);

	/* move the objects from page1 to page2 */
        BTM_WRITE_OIDS_IN_OID_ARRAY(page2->oid, 0, nMovedObjects, &page1->oid[page1->hdr.nObjects-nMovedObjects]);

	/* update the number of objects in each page */
	page1->hdr.nObjects -= nMovedObjects;
	page2->hdr.nObjects += nMovedObjects;

    } else {	/* page2 => page1 */
	/*@ number of objects to move */
	nMovedObjects = (page2->hdr.nObjects - page1->hdr.nObjects)/2;


        /* for logging */
        srcPage = page2;
        dstPage = page1;

        srcOidArrayElemNo = 0;
        dstOidArrayElemNo = dstPage->hdr.nObjects;


	/* move the objects from page2 to page1 */
        BTM_WRITE_OIDS_IN_OID_ARRAY(page1->oid, page1->hdr.nObjects, nMovedObjects, &page2->oid[0]);

	/* fill the moved space in page1 */
        BTM_DELETE_OIDS_SPACE_FROM_OID_ARRAY(page2->oid, page2->hdr.nObjects, 0, nMovedObjects);

	/* update the number of objects in each page */
	page1->hdr.nObjects += nMovedObjects;
	page2->hdr.nObjects -= nMovedObjects;
    }

    e = TM_XT_BeginNestedTopAction(handle, xactEntry, &xactEntry->lastLsn);
    if (e < eNOERROR) ERR(handle, e);

    /*
     * Write log record.
     */
    if (logParam->logFlag & LOG_FLAG_DATA_LOGGING) {

        /*
         * logging for the page from which objects were deleted
         */
        oidsInfo.startOidArrayElemNo = srcOidArrayElemNo;
        oidsInfo.nObjects = nMovedObjects;

        LOG_FILL_LOGRECINFO_2(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                              LOG_ACTION_BTM_DELETE_OIDS_FROM_OVERFLOW_PAGE, LOG_REDO_UNDO,
                              srcPage->hdr.pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                              sizeof(LOG_Image_BtM_OidsInOverflow_T), &oidsInfo,
                              nMovedObjects*OBJECTID_SIZE, &dstPage->oid[dstOidArrayElemNo]);

        e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
        if (e < eNOERROR) ERR(handle, e);

        srcPage->hdr.lsn = lsn;
        srcPage->hdr.logRecLen = logRecLen;


        /*
         * logging for the page into which objects were inserted
         */
        oidsInfo.startOidArrayElemNo = dstOidArrayElemNo;
        oidsInfo.nObjects = nMovedObjects;

        LOG_FILL_LOGRECINFO_2(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                              LOG_ACTION_BTM_INSERT_OIDS_INTO_OVERFLOW_PAGE, LOG_REDO_UNDO,
                              dstPage->hdr.pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                              sizeof(LOG_Image_BtM_OidsInOverflow_T), &oidsInfo,
                              nMovedObjects*OBJECTID_SIZE, &dstPage->oid[dstOidArrayElemNo]);

        e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
        if (e < eNOERROR) ERR(handle, e);

        dstPage->hdr.lsn = lsn;
        dstPage->hdr.logRecLen = logRecLen;
    } else {
        INCREASE_LSN_BY_ONE(srcPage->hdr.lsn);
        INCREASE_LSN_BY_ONE(dstPage->hdr.lsn);
    }

    /*
     * write a dummy CLR for the nested top action.
     */
    e = TM_XT_EndNestedTopAction(handle, xactEntry, logParam);
    if (e < eNOERROR) ERR(handle, e);

    return(eNOERROR);

} /* btm_OverflowDistribute() */


/*
 * Function: Four btm_SplitOverflow(Four, Buffer_ACC_CB*, Four, ObjectID*, KeyDesc*, KeyValue*)
 *
 * Description:
 *  This functions simply splits the given overflow page. At first, it allocates
 *  a new overflow page, and then half of the ObjectIDs are moved to the new
 *  page.
 *  Secondly, it should update the links for maintaining doubly linked list.
 *
 * Returns:
 *  Error code
 *    some errors caused by function calls
 *
 * Note:
 *  The caller should call BfM_SetDirty() for 'fpage'.
*/
Four btm_SplitOverflow(
    Four	handle,
    XactTableEntry_T *xactEntry, /* IN transaction table entry */
    BtreeIndexInfo *iinfo,       /* IN Btree Index Info */
    BtreeOverflow *fpage,	/* INOUT the page which will be splitted */
    LogParameter_T *logParam)   /* IN log parameter */
{
    Four e;			/* error number */
    Four how;			/* how may bytes are moved to the new page */
    Four from;			/* the starting byte to be copied */
    PageID newPid;		/* a new allocated page */
    PageID nextPid;		/* for maintaining doubly linked list */
    BtreeOverflow *npage;	/* a page pointer to the new page */
    Buffer_ACC_CB *npage_BCBP;
    Lsn_T lsn;                  /* lsn of the newly written log record */
    Four logRecLen;             /* log record length */
    LOG_LogRecInfo_T logRecInfo; /* log record information */
    LOG_Image_BtM_InitOverflowPage_T initOverflowPageInfo;
    LOG_Image_BtM_OidsInOverflow_T oidsInfo;
    SegmentID_T pageSegmentID;    /* page segment ID */

    /* pointer for COMMON Data Structure of perThreadTable */
    COMMON_PerThreadDS_T *common_perThreadDSptr = COMMON_PER_THREAD_DS_PTR(handle);


    TR_PRINT(handle, TR_BTM, TR1, ("btm_SplitOverflow()"));


    /*
     * Begin new nested top action
     */
    e = TM_XT_BeginNestedTopAction(handle, xactEntry, &xactEntry->lastLsn);
    if (e < eNOERROR) ERR(handle, e);

    /*@ Allocate a new page and initialize it as an overflow page. */
    e = btm_GetSegmentIDFromIndexInfo(handle, xactEntry, iinfo, &pageSegmentID, PAGESIZE2);
    if (e < eNOERROR) ERR(handle, e);

    e = RDsM_AllocTrains(handle, xactEntry, fpage->hdr.pid.volNo, &pageSegmentID, &fpage->hdr.pid, 1, PAGESIZE2, FALSE, &newPid, logParam);
    if (e < eNOERROR)  ERR(handle, e);

    e = BfM_fixNewBuffer(handle, &newPid, M_FREE, &npage_BCBP, PAGE_BUF);
    if (e < eNOERROR)  ERR(handle, e);

    npage = (BtreeOverflow *)npage_BCBP->bufPagePtr;

    /* Initialize the new page to the overflow page. */
    BTM_INIT_OVERFLOW_PAGE(npage, fpage->hdr.iid, newPid, fpage->hdr.pid.pageNo, fpage->hdr.nextPage);


    /* Half of the ObjectIDs are remained in the original page and the rest of
     * the ObjectIDs are moved to the new allocated overflow page.
     */
    from = HALF_OF_OBJECTS;
    how = NO_OF_OBJECTS - HALF_OF_OBJECTS;

    /* Move half of the ObjectIDs and update the variables of two pages */
    memcpy(&(npage->oid[0]), &(fpage->oid[from]), how*OBJECTID_SIZE);
    fpage->hdr.nObjects = from;
    npage->hdr.nObjects = how;

    /*@ Page ID of the next page */
    MAKE_PAGEID(nextPid, fpage->hdr.pid.volNo, fpage->hdr.nextPage);

    /* Update links to maintain doubly linked list */
    fpage->hdr.nextPage = newPid.pageNo;

    /*
     * Write log record.
     */
    if (logParam->logFlag & LOG_FLAG_DATA_LOGGING) {

        /*
         * original page
         */
        LOG_FILL_LOGRECINFO_3(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                              LOG_ACTION_BTM_SPLIT_OVERFLOW_PAGE, LOG_REDO_UNDO,
                              fpage->hdr.pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                              sizeof(Two), &npage->hdr.nObjects,
                              sizeof(ShortPageID), &newPid.pageNo,
                              sizeof(ShortPageID), &npage->hdr.nextPage);

        e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
        if (e < eNOERROR) ERR(handle, e);

        fpage->hdr.lsn = lsn;
        fpage->hdr.logRecLen = logRecLen;


        /*
         * new page
         */
        initOverflowPageInfo.iid = npage->hdr.iid;
        initOverflowPageInfo.prevPage = npage->hdr.prevPage;
        initOverflowPageInfo.nextPage = npage->hdr.nextPage;;

        LOG_FILL_LOGRECINFO_1(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                              LOG_ACTION_BTM_INIT_OVERFLOW_PAGE, LOG_REDO_ONLY,
                              npage->hdr.pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                              sizeof(LOG_Image_BtM_InitOverflowPage_T), &initOverflowPageInfo);

        e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
        if (e < eNOERROR) ERR(handle, e);

        oidsInfo.startOidArrayElemNo = 0;
        oidsInfo.nObjects = how;

        LOG_FILL_LOGRECINFO_2(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                              LOG_ACTION_BTM_INSERT_OIDS_INTO_OVERFLOW_PAGE, LOG_REDO_ONLY,
                              npage->hdr.pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                              sizeof(LOG_Image_BtM_OidsInOverflow_T), &oidsInfo,
                              how*OBJECTID_SIZE, npage->oid);

        e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
        if (e < eNOERROR) ERR(handle, e);


        npage->hdr.lsn = lsn;
        npage->hdr.logRecLen = logRecLen;
    } else {
        INCREASE_LSN_BY_ONE(fpage->hdr.lsn);
        INCREASE_LSN_BY_ONE(npage->hdr.lsn);
    }

    npage_BCBP->dirtyFlag = 1;

    e = BfM_unfixBuffer(handle, npage_BCBP, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);

    if (nextPid.pageNo != NIL) {
	e = BfM_getAndFixBuffer(handle, &nextPid, M_FREE, &npage_BCBP, PAGE_BUF);
	if(e < eNOERROR) ERR(handle, e);

	npage = (BtreeOverflow *)npage_BCBP->bufPagePtr;


        /*
         * Write log record.
         */
        if (logParam->logFlag & LOG_FLAG_DATA_LOGGING) {

            LOG_FILL_LOGRECINFO_2(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                                  LOG_ACTION_BTM_MODIFY_OVERFLOW_CHAIN_PREV_LINK, LOG_REDO_UNDO,
                                  npage->hdr.pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                                  sizeof(ShortPageID), &newPid.pageNo,
                                  sizeof(ShortPageID), &npage->hdr.prevPage);

            e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
            if (e < eNOERROR) ERR(handle, e);

            npage->hdr.lsn = lsn;
            npage->hdr.logRecLen = logRecLen;
        } else {
            INCREASE_LSN_BY_ONE(npage->hdr.lsn);
        }

	npage->hdr.prevPage = newPid.pageNo;

	npage_BCBP->dirtyFlag = 1;

	e = BfM_unfixBuffer(handle, npage_BCBP, PAGE_BUF);
	if (e < eNOERROR) ERR(handle, e);
    }

    /*
     * End the nested top action.
     */
    e = TM_XT_EndNestedTopAction(handle, xactEntry, logParam);
    if (e < eNOERROR) ERR(handle, e);

    return(eNOERROR);

} /* btm_SplitOverflow() */
