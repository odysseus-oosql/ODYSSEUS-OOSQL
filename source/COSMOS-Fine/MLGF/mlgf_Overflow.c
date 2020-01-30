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
/******************************************************************************/
/*                                                                            */
/*    This module has been implemented based on "The Multilevel Grid File     */
/*    (MLGF) Version 4.0," which can be downloaded at                         */
/*    "http://dblab.kaist.ac.kr/Open-Software/MLGF/main.html".                */
/*                                                                            */
/******************************************************************************/

/*
 * Module: mlgf_Overflow.c
 *
 * Description :
 *  This file has five functions which are concerned with maintaining overflow
 *  pages. A new overflow page is created when the size of a leaf entry becomes
 *  greater than a third of a page size.  If the member of objects having the
 *  same key value grows more than one page limit, the page should be splitted
 *  by two pages and they are connected by doubly linked list. The deletion
 *  of an object may cause an underflow of an overflow page. If it occurs,
 *  overflow pages may be merged or redistributed.
 *
 * Exports:
 *  Four mlgf_CreateOverflow(Four, Buffer_ACC_CB*, MLGF_KeyDesc*, Four, ObjectID*, char*)
 * Four mlgf_InsertOverflow(Four, PageID*, MLGF_KeyDesc*, ObjectID*, char*)
 * Four mlgf_DeleteOverflow(Four, PageID*, MLGF_KeyDesc*, ObjectID*, Four*,
 *                          LocalPool*, DeallocListElem*)
 */


#include <string.h>
#include "common.h"
#include "error.h"
#include "trace.h"
#include "Util.h"
#include "RDsM.h"
#include "BfM.h"
#include "MLGF.h"
#include "LOG.h"
#include "perProcessDS.h"
#include "perThreadDS.h"


/* Internal Function Prototypes */
Four mlgf_SplitOverflow(Four, XactTableEntry_T*, MLGFIndexInfo*, PageID*, mlgf_OverflowPage*, MLGF_KeyDesc*, LogParameter_T*); 
Four mlgf_OverflowMerge(Four, XactTableEntry_T*, mlgf_OverflowPage*, mlgf_OverflowPage*, Four, LogParameter_T*);
Four mlgf_OverflowDistribute(Four, XactTableEntry_T*, mlgf_OverflowPage*, mlgf_OverflowPage*, Four, LogParameter_T*);


/*
 * Function: Four mlgf_CreateOverflow(Four, Buffer_ACC_CB*, MLGF_KeyDesc*, Four, ObjectID*, char*)
 *
 * Description:
 *  This function creates an overflow chain from the given leaf entry.
 *  At first, it allocates a new page and intializes it as an overflow page.
 *  And then, it moves the objects from the leaf entry into the newlly created
 *  overflow page. At the same time, it inserts the given new object into
 *  the overflow page.
 *
 * Returns:
 *  Error code
 *    some errors caused by function calls
 */
Four mlgf_CreateOverflow(
    Four 				handle,
    XactTableEntry_T 			*xactEntry, 		/* IN transaction table entry */
    MLGFIndexInfo 			*iinfo,        		/* IN MLGF Index Info */
    PageID				*root,			/* IN root page ID */ 
    mlgf_LeafPage 			*apage,       		/* INOUT leaf page including the leaf entry */
    MLGF_KeyDesc 			*kdesc,			/* IN key descriptor of MLGF index */
    Four      				entryNo,		/* IN entry to be converted into overflow page */
    LogParameter_T 			*logParam) 		/* IN log parameter */
{
    Four 				e;			/* error number */
    Four 				objectItemLen;		/* length of an object item of object array*/
    char 				*firstObjectItem;	/* points to the first object item in object array */
    PageID 				newPid;			/* PageID of the newlly created overflow page */
    mlgf_LeafEntry 			*entry;			/* pointer to the entryNo-th leaf entry */
    mlgf_OverflowPage 			*opage;			/* pointer to the buffer for the overflow page */
    Buffer_ACC_CB 			*opage_BCBP;  		/* buffer control block for the overflow page */
    Lsn_T 				lsn;                  	/* lsn of the newly written log record */
    Four 				logRecLen;             	/* log record length */
    LOG_LogRecInfo_T 			logRecInfo; 		/* log record information */
    LOG_Image_MLGF_ObjectsInOverflow_T 	objectsInfo;
    LOG_Image_MLGF_InitOverflowPage_T	initOverflowPageInfo;
    LOG_Image_MLGF_ChangeLeafEntry_T 	changeLeafEntryInfo;
    SegmentID_T 			pageSegmentID;  	/* page segment ID */

    /* pointer for COMMON Data Structure of perThreadTable */
    COMMON_PerThreadDS_T *common_perThreadDSptr = COMMON_PER_THREAD_DS_PTR(handle);

    TR_PRINT(handle, TR_MLGF, TR1, ("mlgf_CreateOverflow()"));


    /*
     * Begin new nested top action
     */
    e = TM_XT_BeginNestedTopAction(handle, xactEntry, &xactEntry->lastLsn);
    if (e < eNOERROR) ERR(handle, e);

    e = mlgf_GetSegmentIDFromIndexInfo(handle, xactEntry, iinfo, &pageSegmentID, PAGESIZE2);
    if (e < eNOERROR) ERR(handle, e);

    e = RDsM_AllocTrains(handle, xactEntry, apage->hdr.pid.volNo, &pageSegmentID, &apage->hdr.pid, 1, PAGESIZE2, FALSE, &newPid, logParam);
    if (e < eNOERROR) ERR(handle, e);

    /* Fix the page to the buffer. */
    e = BfM_fixNewBuffer(handle, &newPid, M_FREE, &opage_BCBP, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);

    /* opage points to the buffer for the overflow page. */
    opage = (mlgf_OverflowPage*)opage_BCBP->bufPagePtr;

    /* Initialize the new page as an overflow page. */
    MLGF_INIT_OVERFLOW_PAGE(opage, apage->hdr.iid, newPid, NIL, NIL, kdesc->extraDataLen);

    /* entry points to the leaf entry which will be converted to the overflow chain. */
    entry = MLGF_ITH_LEAFENTRY(apage, entryNo);

    /* firstObjectItem points to the first object item in the leaf entry. */
    firstObjectItem = MLGF_LEAFENTRY_FIRST_OBJECT(kdesc->nKeys, entry);
    objectItemLen = MLGF_LEAFENTRY_OBJECTITEM_LEN(kdesc->extraDataLen);

    /* Move the objects from the leaf entry to the overflow page. */
    memcpy(MLGF_OVERFLOW_ITH_OBJECTITEM(objectItemLen, opage, 0),
	   firstObjectItem, entry->nObjects*objectItemLen);

    opage->hdr.nObjects = entry->nObjects; 

    /*
     * Write log record.
     */
    if (logParam->logFlag & LOG_FLAG_DATA_LOGGING) {
        initOverflowPageInfo.iid = apage->hdr.iid;
        initOverflowPageInfo.prevPage = NIL;
        initOverflowPageInfo.nextPage = NIL;
        initOverflowPageInfo.extraDataLen = kdesc->extraDataLen;

        LOG_FILL_LOGRECINFO_1(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                              LOG_ACTION_MLGF_INIT_OVERFLOW_PAGE, LOG_REDO_ONLY,
                              newPid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                              sizeof(LOG_Image_MLGF_InitOverflowPage_T), &initOverflowPageInfo);

        e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
        if (e < eNOERROR) ERRB1(handle, e, opage_BCBP, PAGE_BUF);

        objectsInfo.startObjArrayElemNo = 0;
        objectsInfo.nObjects = entry->nObjects;

        LOG_FILL_LOGRECINFO_2(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                              LOG_ACTION_MLGF_INSERT_OBJECTS_INTO_OVERFLOW_PAGE, LOG_REDO_ONLY,
                              opage->hdr.pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                              sizeof(LOG_Image_MLGF_ObjectsInOverflow_T), &objectsInfo,
                              entry->nObjects*objectItemLen, firstObjectItem);

        e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
        if (e < eNOERROR) ERRB1(handle, e, opage_BCBP, PAGE_BUF);

        opage->hdr.lsn = lsn;
        opage->hdr.logRecLen = logRecLen;
    }

    /* Set dirtyFlag to 1 in order to mark that the overflow page was updated. */
    opage_BCBP->dirtyFlag = 1;

    /* Unfix the buffer. */
    e = BfM_unfixBuffer(handle, opage_BCBP, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);

    /*
     * Write log record.
     */
    if (logParam->logFlag & LOG_FLAG_DATA_LOGGING) {

        changeLeafEntryInfo.entryNo = entryNo;
        changeLeafEntryInfo.deltaOfObjectArrayAreaSize = sizeof(ShortPageID) - entry->nObjects*objectItemLen;
        changeLeafEntryInfo.deltaInNumOfObjects = NIL - entry->nObjects;

        LOG_FILL_LOGRECINFO_3(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                              LOG_ACTION_MLGF_CHANGE_LEAF_ENTRY, LOG_REDO_UNDO,
                              apage->hdr.pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                              sizeof(LOG_Image_MLGF_ChangeLeafEntry_T), &changeLeafEntryInfo,
                              sizeof(ShortPageID), &newPid.pageNo,
                              entry->nObjects*objectItemLen, firstObjectItem);

        e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
        if (e < eNOERROR) ERR(handle, e);

        apage->hdr.lsn = lsn;
        apage->hdr.logRecLen = logRecLen;
    }

    /* Release space used for the objects. */
    apage->hdr.unused += entry->nObjects*objectItemLen - sizeof(ShortPageID);

    /* The leaf entry has an overflow PageID instead of Object list */
    entry->nObjects = NIL;
    *((ShortPageID*)firstObjectItem) = newPid.pageNo;

    return(eNOERROR);

} /* mlgf_CreateOverflow() */



/*
 * Function: Four mlgf_InsertOverflow(Four, PageID*, MLGF_KeyDesc*, ObjectID*, char*)
 *
 * Description:
 *  If the ObjectID is greater than the last ObjectID in the given page, get
 *  the nextPage and recursively call itself using the nextPage as long as the
 *  nextPage is not NIL.
 *  If the next page is NIL, append it to the end of the given page.
 *  If the ObjectID is less than the last ObjectID, the object should be
 *  inserted into this page. At first, it find out the correct position to be
 *  inserted using the binary search routine, and then insert it if there is
 *  enough space. If there is not enough space in the given page, split it and
 *  then recursively call itself.
 *
 * Returns:
 *  Error code
 *    eDUPLICATEDOBJECTID_MLGF
 *    some errors caused by function calls
 */
Four mlgf_InsertOverflow(
    Four 			handle,
    XactTableEntry_T 		*xactEntry, 		/* IN transaction table entry */
    MLGFIndexInfo 		*iinfo,        		/* IN MLGF Index Info */
    PageID			*root,			/* IN root page ID */ 
    PageID   			*overPid,		/* IN where the object to be inserted */
    MLGF_KeyDesc 		*kdesc,			/* IN key descriptor of this index */
    MLGF_HashValue 		keys[],			/* IN hash values of key values */
    ObjectID 			*oid,			/* IN ObjectID to be inserted */
    char 			*data,			/* IN extra data to be inseted */
    LogParameter_T 		*logParam) 		/* IN log parameter */
{
    Four 			e;			/* error number */
    Four 			idx;			/* the position to be inserted */
    PageID 			nextPage;		/* the Next Overflow Page */
    mlgf_OverflowPage 		*opage;			/* Page Pointer to the overflow page */
    Buffer_ACC_CB 		*opage_BCBP;		/* buffer access control block containing data */
    Boolean 			found;			/* search result */
    Four 			objectItemLen;		/* length of an object item in object array */
    char 			*lastObjectItem;	/* points to last object in the overflow page */
    char 			*objectItemPtr;		/* points to an object item in overflow page */
    Lsn_T 			lsn;                  	/* lsn of the newly written log record */
    Four 			logRecLen;             	/* log record length */
    LOG_LogRecInfo_T 		logRecInfo; 		/* log record information */
    LOG_Image_MLGF_LogicalUndoInfo_T undoInfo;

    /* pointer for COMMON Data Structure of perThreadTable */
    COMMON_PerThreadDS_T *common_perThreadDSptr = COMMON_PER_THREAD_DS_PTR(handle);

    TR_PRINT(handle, TR_MLGF, TR1, ("mlgf_InsertOverflow()"));


    e = BfM_getAndFixBuffer(handle, overPid, M_FREE, &opage_BCBP, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);

    opage = (mlgf_OverflowPage *)opage_BCBP->bufPagePtr;

    /* Get the length of an object item. */
    objectItemLen = MLGF_LEAFENTRY_OBJECTITEM_LEN(kdesc->extraDataLen);

    /* Get the last object in the given overflow page. */
    lastObjectItem = MLGF_OVERFLOW_ITH_OBJECTITEM(objectItemLen, opage, opage->hdr.nObjects-1);

    if (mlgf_ObjectIdComp(handle, oid, (ObjectID*)lastObjectItem) == GREAT && opage->hdr.nextPage != NIL) {
        /* insert it into the next page */
	MAKE_PAGEID(nextPage, overPid->volNo, opage->hdr.nextPage);

        e = BfM_unfixBuffer(handle, opage_BCBP, PAGE_BUF);
        if (e < eNOERROR) ERR(handle, e);

        e = mlgf_InsertOverflow(handle, xactEntry, iinfo, root, &nextPage, kdesc, keys, oid, data, logParam); 
        if (e < eNOERROR) ERR(handle, e);

        return(eNOERROR);
    }


    /* The ObjectID should be inserted in this given page */

    if (opage->hdr.nObjects < MLGF_OVERFLOW_MAXNUM_OBJECTS(objectItemLen)) {
        /* Search the correct postion to be inserted using the binary search */
        found = mlgf_BinarySearchObjectArray(handle,
                                             MLGF_OVERFLOW_ITH_OBJECTITEM(objectItemLen, opage, 0),
                                             oid, opage->hdr.nObjects, objectItemLen, &idx);
        if (found) ERRB1(handle, eDUPLICATEDOBJECTID_BTM, opage_BCBP, PAGE_BUF);

        /* objectItemPtr points to the object item where the new object */
        /* will be inserted. */
        objectItemPtr = MLGF_OVERFLOW_ITH_OBJECTITEM(objectItemLen, opage, idx);

        /* Make room for the object */
        MLGF_INSERT_OBJECTS_SPACE_INTO_OBJECT_ARRAY(MLGF_OVERFLOW_ITH_OBJECTITEM(objectItemLen, opage, 0),
                                                    opage->hdr.nObjects, idx, 1, objectItemLen);

        /* Insert the new object into the overflow page. */
        *((ObjectID*)objectItemPtr) = *oid;
        if (kdesc->extraDataLen != 0)
            memcpy(objectItemPtr + sizeof(ObjectID), data, kdesc->extraDataLen);
        (opage->hdr.nObjects)++;

        /*
         * Write log record.
         */
        if (logParam->logFlag & LOG_FLAG_DATA_LOGGING) {

            Two tmpObjArrayElemNo = idx;

            undoInfo.iid = iinfo->iid; 
	    undoInfo.catEntry = iinfo->catalog.oid; 
            undoInfo.kdesc = *kdesc;

            if (logParam->logFlag & LOG_FLAG_UNDO) {
                LOG_FILL_LOGRECINFO_2(logRecInfo, xactEntry->xactId, LOG_TYPE_COMPENSATION,
                                      LOG_ACTION_MLGF_INSERT_OBJECT_INTO_OVERFLOW_PAGE, LOG_REDO_ONLY,
                                      opage->hdr.pid, xactEntry->lastLsn, logParam->undo.undoNextLsn,
                                      sizeof(Two), &tmpObjArrayElemNo,
                                      objectItemLen, objectItemPtr);
            } else {
                LOG_FILL_LOGRECINFO_4(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                                      LOG_ACTION_MLGF_INSERT_OBJECT_INTO_OVERFLOW_PAGE, LOG_REDO_UNDO,
                                      opage->hdr.pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                                      sizeof(Two), &tmpObjArrayElemNo,
                                      objectItemLen, objectItemPtr,
                                      sizeof(LOG_Image_MLGF_LogicalUndoInfo_T), &undoInfo,
                                      kdesc->nKeys*sizeof(MLGF_HashValue), keys);
            }

            e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
            if (e < eNOERROR) ERRB1(handle, e, opage_BCBP, PAGE_BUF);

            opage->hdr.lsn = lsn;
            opage->hdr.logRecLen = logRecLen;
        }

    } else {	/* not enough */
        /* Insert it after split */
        e = mlgf_SplitOverflow(handle, xactEntry, iinfo, root, opage, kdesc, logParam); 
        if (e < eNOERROR) ERRB1(handle, e, opage_BCBP, PAGE_BUF);

        e = mlgf_InsertOverflow(handle, xactEntry, iinfo, root, overPid, kdesc, keys, oid, data, logParam); 
        if (e < eNOERROR) ERRB1(handle, e, opage_BCBP, PAGE_BUF);
    }

    opage_BCBP->dirtyFlag = 1;

    e = BfM_unfixBuffer(handle, opage_BCBP, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);

    return(eNOERROR);

} /* mlgf_InsertOverflow() */



/*
 * Function: Four mlgf_DeleteOverflow(Four, PageID*, MLGF_KeyDesc*, ObjectID*, char*,
 *                                    Four*, LocalPool*, DeallocListElem*)
 *
 * Description:
 *  This function deletes the given ObjectID from the overflow pages. If the
 *  ObjectID is in the given overflow page, simply delete it. If not, get the
 *  next page and recursively call itself using the next page. If the page after
 *  deleting is not half full, the page and the neighboring page are merged or
 *  redistributed. After deleting, if there is only one overflow page and its
 *  size is less than a fourth of a page, then 'f' is assigned to the # of
 *  ObjectIDs, otherwise it becomes FALSE.
 *
 * Returns:
 *  Error code
 *    eNOTFOUND_BTM
 *    some errors caused by function calls
 */
Four mlgf_DeleteOverflow(
    Four 			handle,
    XactTableEntry_T 		*xactEntry, 		/* IN transaction table entry */
    MLGFIndexInfo 		*iinfo,        		/* IN MLGF Index Info */
    PageID			*root,			/* IN root page ID */ 
    PageID   			*overPid,		/* IN where the given object will be deleted? */
    MLGF_KeyDesc 		*kdesc,			/* IN key descriptor of used index */
    MLGF_HashValue 		keys[],			/* IN hash values of key values */
    ObjectID 			*oid,			/* IN ObjectID to be deleted */
    char 			*data,			/* OUT extra data of the deleted object */
    Four     			*f,			/* OUT # of ObjectIDs if underflow occur otherwise,
							       max number of object ids */
    LogParameter_T 		*logParam)   		/* IN log parameter */
{
    Four 			e;			/* error number */
    Four 			idx;			/* nth ObjectID in the overflow page */
    PageID 			prevPid;		/* The previous Page of the Overflow Page */
    PageID 			nextPid;		/* The nextPage of the given overflow page */
    ObjectID 			curOid;			/* The current ObjectID which is examined */
    mlgf_OverflowPage 		*mpage;			/* Page Pointer to the neighbor page */
    PageID 			*mpid;               	/* page id of the neighbor page */
    Buffer_ACC_CB 		*mpage_BCBP;		/* buffer access control block containing data */
    mlgf_OverflowPage 		*opage;			/* Page Pointer to the given overflow page */
    Buffer_ACC_CB 		*opage_BCBP;		/* buffer access control block containing data */
    Boolean 			found;			/* search result */
    mlgf_OverflowPage 		*leftPage;    		/* left page of between two overflow pages */
    mlgf_OverflowPage 		*rightPage;   		/* right page of between two overflow pages */
    PageID 			*rightPid;           	/* page id of the right page */
    Four 			objectItemLen;		/* length of an object item in object array */
    char 			*lastObjectItem;	/* points to last object in the overflow page */
    char 			*objectItemPtr;		/* points to an object item in overflow page */
    Lsn_T 			lsn;                  	/* lsn of the newly written log record */
    Four 			logRecLen;             	/* log record length */
    LOG_LogRecInfo_T 		logRecInfo; 		/* log record information */
    LOG_Image_MLGF_LogicalUndoInfo_T undoInfo;

    /* pointer for COMMON Data Structure of perThreadTable */
    COMMON_PerThreadDS_T *common_perThreadDSptr = COMMON_PER_THREAD_DS_PTR(handle);

    TR_PRINT(handle, TR_MLGF, TR1, ("mlgf_DeleteOverflow()"));


    e = BfM_getAndFixBuffer(handle, overPid, M_FREE, &opage_BCBP, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);

    opage = (mlgf_OverflowPage *)opage_BCBP->bufPagePtr;

    /* Get the length of an object item. */
    objectItemLen = MLGF_LEAFENTRY_OBJECTITEM_LEN(kdesc->extraDataLen);

    /* Get the last object in the given overflow page. */
    lastObjectItem = MLGF_OVERFLOW_ITH_OBJECTITEM(objectItemLen, opage, opage->hdr.nObjects-1);

    MAKE_PAGEID(nextPid, overPid->volNo, opage->hdr.nextPage);

    if(mlgf_ObjectIdComp(handle, oid, (ObjectID*)lastObjectItem) == GREAT) { /* Not exist in this page */

        e = BfM_unfixBuffer(handle, opage_BCBP, PAGE_BUF);
        if (e < eNOERROR) ERR(handle, e);

        if (IS_NILPAGEID(nextPid)) ERR(handle, eNOTFOUND); 

        /* Recursively delete the object using the next page */
        e = mlgf_DeleteOverflow(handle, xactEntry, iinfo, root, &nextPid, kdesc, keys, 
                                oid, data, f, logParam);
        if( e < eNOERROR ) ERR(handle, e);

        return(eNOERROR);
    }

    /* The deleted object is in this page, if exist. */

    /*@ Search the ObjectID */
    found = mlgf_BinarySearchObjectArray(handle,
	                                 MLGF_OVERFLOW_ITH_OBJECTITEM(objectItemLen, opage, 0),
	                                 oid, opage->hdr.nObjects, objectItemLen, &idx);

    if (!found) ERRB1(handle, eNOTFOUND, opage_BCBP, PAGE_BUF);


    objectItemPtr = MLGF_OVERFLOW_ITH_OBJECTITEM(objectItemLen, opage, idx);
    if (data) memcpy(data, objectItemPtr + sizeof(ObjectID), kdesc->extraDataLen);

    /*
     * Write log record.
     */
    if (logParam->logFlag & LOG_FLAG_DATA_LOGGING) {

        Two tmpObjArrayElemNo = idx;

	undoInfo.iid = iinfo->iid; 
	undoInfo.catEntry = iinfo->catalog.oid; 
        undoInfo.kdesc = *kdesc;

        if (logParam->logFlag & LOG_FLAG_UNDO) {
            LOG_FILL_LOGRECINFO_1(logRecInfo, xactEntry->xactId, LOG_TYPE_COMPENSATION,
				  LOG_ACTION_MLGF_DELETE_OBJECT_FROM_OVERFLOW_PAGE, LOG_REDO_ONLY,
                                  opage->hdr.pid, xactEntry->lastLsn, logParam->undo.undoNextLsn,
                                  sizeof(Two), &tmpObjArrayElemNo);
        } else {
            LOG_FILL_LOGRECINFO_4(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
				  LOG_ACTION_MLGF_DELETE_OBJECT_FROM_OVERFLOW_PAGE, LOG_REDO_UNDO,
                                  opage->hdr.pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                                  sizeof(Two), &tmpObjArrayElemNo,
                                  objectItemLen, objectItemPtr,
				  sizeof(LOG_Image_MLGF_LogicalUndoInfo_T), &undoInfo,
                                  kdesc->nKeys*sizeof(MLGF_HashValue), keys);
        }

        e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
        if (e < eNOERROR) ERRB1(handle, e, opage_BCBP, PAGE_BUF);

        opage->hdr.lsn = lsn;
        opage->hdr.logRecLen = logRecLen;
    }

    /* Get the previous page id */
    MAKE_PAGEID(prevPid, overPid->volNo, opage->hdr.prevPage);

    /* Delete the ObjectID */
    MLGF_DELETE_OBJECTS_SPACE_FROM_OBJECT_ARRAY(opage->data, opage->hdr.nObjects, idx, 1, objectItemLen);

    /*@ update the variables of the page */
    (opage->hdr.nObjects)--;

    /* Initialize 'f' to the max # of objects in the overflow page. */
    *f = MLGF_OVERFLOW_MAXNUM_OBJECTS(objectItemLen);

    if (opage->hdr.nObjects < MLGF_OVERFLOW_HALF_OF_OBJECTS(objectItemLen)) {	/* Underflow */

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
            e = BfM_getAndFixBuffer(handle, mpid, M_FREE, &mpage_BCBP, PAGE_BUF);
            if (e < eNOERROR) ERRB1(handle, e, opage_BCBP, PAGE_BUF);

            mpage = (mlgf_OverflowPage *)mpage_BCBP->bufPagePtr;

            if (mpid->pageNo == prevPid.pageNo) {
                leftPage = mpage;
                rightPage = opage;
                rightPid = overPid;
            } else {
                leftPage = opage;
                rightPage = mpage;
                rightPid = &nextPid;
            }

            if((leftPage->hdr.nObjects + rightPage->hdr.nObjects) <= MLGF_OVERFLOW_MAXNUM_OBJECTS(objectItemLen)) {
                /* The sum of the two pages is less than a page size */
                e = mlgf_OverflowMerge(handle, xactEntry, leftPage, rightPage, objectItemLen, logParam);
                if (e < eNOERROR) ERRB2(handle, e, opage_BCBP, PAGE_BUF, mpage_BCBP, PAGE_BUF);
            } else { /* more than one page size */
                e = mlgf_OverflowDistribute(handle, xactEntry, leftPage, rightPage, objectItemLen, logParam);
                if (e < eNOERROR) ERRB2(handle, e, opage_BCBP, PAGE_BUF, mpage_BCBP, PAGE_BUF);
            }

            mpage_BCBP->dirtyFlag = 1;

            e = BfM_unfixBuffer(handle, mpage_BCBP, PAGE_BUF);
            if (e < eNOERROR) ERRB1(handle, e, opage_BCBP, PAGE_BUF);
        }
    }

    opage_BCBP->dirtyFlag = 1;

    /*@ unfix the buffer */
    e = BfM_unfixBuffer(handle, opage_BCBP, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);

    return(eNOERROR);

} /* mlgf_DeleteOverflow() */



/*
 * Function: Four mlgf_SplitOverflow(Buffer_ACC_CB*, MLGF_KeyDesc*)
 *
 * Description:
 *  This function splits the given overflow page and inserts the given object
 *  into the appropriate overflow page.
 *
 * Returns:
 *  Error code
 *    some errors caused by function calls
 *
 * Note:
 *  The caller should call BfM_SetDirty() for 'fpage'.
 */
Four mlgf_SplitOverflow(
    Four			handle,
    XactTableEntry_T 		*xactEntry, 		/* IN transaction table entry */
    MLGFIndexInfo 		*iinfo,        		/* IN MLGF Index Info */
    PageID			*root,			/* IN root page ID */ 
    mlgf_OverflowPage 		*fpage,       		/* INOUT the page which will be splitted */
    MLGF_KeyDesc 		*kdesc,			/* IN key descriptor of used index */
    LogParameter_T 		*logParam)   		/* IN log parameter */
{
    Four 			e;			/* error number */
    Four 			how;			/* how may bytes are moved to the new page */
    Four 			from;			/* the starting byte to be copied */
    PageID 			newPid;			/* a new allocated page */
    PageID 			nextPid;		/* for maintaining doubly linked list */
    mlgf_OverflowPage 		*npage;			/* a page pointer to the new/next page */
    Buffer_ACC_CB 		*npage_BCBP;
    Four 			objectItemLen;		/* length of an object item of object array*/
    Lsn_T 			lsn;                  	/* lsn of the newly written log record */
    Four 			logRecLen;             	/* log record length */
    LOG_LogRecInfo_T 		logRecInfo; 		/* log record information */
    SegmentID_T 		pageSegmentID;  	/* page segment ID */
    LOG_Image_MLGF_InitOverflowPage_T initOverflowPageInfo;
    LOG_Image_MLGF_ObjectsInOverflow_T objsInfo;

    /* pointer for COMMON Data Structure of perThreadTable */
    COMMON_PerThreadDS_T *common_perThreadDSptr = COMMON_PER_THREAD_DS_PTR(handle);

    TR_PRINT(handle, TR_MLGF, TR1, ("mlgf_SplitOverflow()"));


    objectItemLen = MLGF_LEAFENTRY_OBJECTITEM_LEN(kdesc->extraDataLen);

    /*
     * Begin new nested top action
     */
    e = TM_XT_BeginNestedTopAction(handle, xactEntry, &xactEntry->lastLsn);
    if (e < eNOERROR) ERR(handle, e);

    e = mlgf_GetSegmentIDFromIndexInfo(handle, xactEntry, iinfo, &pageSegmentID, PAGESIZE2);
    if (e < eNOERROR) ERR(handle, e);

    e = RDsM_AllocTrains(handle, xactEntry, fpage->hdr.pid.volNo, &pageSegmentID, &fpage->hdr.pid, 1, PAGESIZE2, FALSE, &newPid, logParam);
    if (e < eNOERROR)  ERR(handle, e);

    e = BfM_fixNewBuffer(handle, &newPid, M_FREE, &npage_BCBP, PAGE_BUF);
    if (e < eNOERROR)  ERR(handle, e);

    npage = (mlgf_OverflowPage *)npage_BCBP->bufPagePtr;

    /* Initialize the new page to the overflow page. */
    MLGF_INIT_OVERFLOW_PAGE(npage, fpage->hdr.iid, newPid, fpage->hdr.pid.pageNo,
                            fpage->hdr.nextPage, fpage->hdr.extraDataLen);


    /* Half of the objects are remained in the original page and the rest of
     * the objects are moved to the new allocated overflow page.
     */
    from = (fpage->hdr.nObjects+1) / 2;
    how = fpage->hdr.nObjects - from;

    /* Move half of the objects and update the variables of two pages */
    memcpy(MLGF_OVERFLOW_ITH_OBJECTITEM(objectItemLen, npage, 0),
           MLGF_OVERFLOW_ITH_OBJECTITEM(objectItemLen, fpage, from),
           how*objectItemLen);
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
        objsInfo.startObjArrayElemNo = from;
        objsInfo.nObjects = how;

        LOG_FILL_LOGRECINFO_2(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                              LOG_ACTION_MLGF_DELETE_OBJECTS_FROM_OVERFLOW_PAGE, LOG_REDO_UNDO,
                              fpage->hdr.pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                              sizeof(LOG_Image_MLGF_ObjectsInOverflow_T), &objsInfo,
                              how*objectItemLen, MLGF_OVERFLOW_ITH_OBJECTITEM(objectItemLen, npage, 0));

        e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
        if (e < eNOERROR) ERRB1(handle, e, npage_BCBP, PAGE_BUF);

        LOG_FILL_LOGRECINFO_2(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                              LOG_ACTION_MLGF_MODIFY_OVERFLOW_CHAIN_NEXT_LINK, LOG_REDO_UNDO,
                              fpage->hdr.pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                              sizeof(ShortPageID), &fpage->hdr.nextPage,
                              sizeof(ShortPageID), &nextPid.pageNo);

        e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
        if (e < eNOERROR) ERRB1(handle, e, npage_BCBP, PAGE_BUF);

        fpage->hdr.lsn = lsn;
        fpage->hdr.logRecLen = logRecLen;


        /*
         * new page
         */
        initOverflowPageInfo.iid = npage->hdr.iid;
        initOverflowPageInfo.prevPage = npage->hdr.prevPage;
        initOverflowPageInfo.nextPage = npage->hdr.nextPage;
        initOverflowPageInfo.extraDataLen = npage->hdr.extraDataLen;

        LOG_FILL_LOGRECINFO_1(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                              LOG_ACTION_MLGF_INIT_OVERFLOW_PAGE, LOG_REDO_ONLY,
                              npage->hdr.pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                              sizeof(LOG_Image_MLGF_InitOverflowPage_T), &initOverflowPageInfo);

        e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
        if (e < eNOERROR) ERRB1(handle, e, npage_BCBP, PAGE_BUF);

        objsInfo.startObjArrayElemNo = 0;
        objsInfo.nObjects = how;

        LOG_FILL_LOGRECINFO_2(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                              LOG_ACTION_MLGF_INSERT_OBJECTS_INTO_OVERFLOW_PAGE, LOG_REDO_ONLY,
                              npage->hdr.pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                              sizeof(LOG_Image_MLGF_ObjectsInOverflow_T), &objsInfo,
                              how*objectItemLen, MLGF_OVERFLOW_ITH_OBJECTITEM(objectItemLen, npage, 0));

        e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
        if (e < eNOERROR) ERRB1(handle, e, npage_BCBP, PAGE_BUF);

        npage->hdr.lsn = lsn;
        npage->hdr.logRecLen = logRecLen;
    }

    npage_BCBP->dirtyFlag = 1;

    e = BfM_unfixBuffer(handle, npage_BCBP, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);

    if (nextPid.pageNo != NIL) {
	e = BfM_getAndFixBuffer(handle, &nextPid, M_FREE, &npage_BCBP, PAGE_BUF);
	if(e < eNOERROR) ERR(handle, e);

	npage = (mlgf_OverflowPage *)npage_BCBP->bufPagePtr;


        /*
         * Write log record.
         */
        if (logParam->logFlag & LOG_FLAG_DATA_LOGGING) {

            LOG_FILL_LOGRECINFO_2(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                                  LOG_ACTION_MLGF_MODIFY_OVERFLOW_CHAIN_PREV_LINK, LOG_REDO_UNDO,
                                  npage->hdr.pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                                  sizeof(ShortPageID), &newPid.pageNo,
                                  sizeof(ShortPageID), &npage->hdr.prevPage);

            e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
            if (e < eNOERROR) ERRB1(handle, e, npage_BCBP, PAGE_BUF);

            npage->hdr.lsn = lsn;
            npage->hdr.logRecLen = logRecLen;
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

} /* mlgf_SplitOverflow() */



/*
 * Function: Four mlgf_OverflowMerge(Four, Buffer_ACC_CB*, Buffer_ACC_CB*, Four
 *                                   LocalPool*, DeallocListElem*)
 *
 * Description:
 *  All objects in the right page are copied to the right page.
 *  After the copy is completed, 'nextPage' and 'prevPage' are updated to
 *  maintain doubly linked list. Finally, deallocate the the right page.
 *
 * Returns:
 *  Error number
 *    some errors caused by function calls
 */
Four mlgf_OverflowMerge(
    Four			handle,
    XactTableEntry_T 		*xactEntry, 		/* IN transaction table entry */
    mlgf_OverflowPage 		*page1,   		/* INOUT page to be copied to */
    mlgf_OverflowPage 		*page2,   		/* INOUT page to be copied from */
    Four 			objectItemLen,		/* IN length of an object item in object array */
    LogParameter_T 		*logParam)   		/* IN log parameter */
{
    FileID 			fid;
    Four 			e;			/* error number */
    PageID  			nextPid;		/* The next page of page2 */
    mlgf_OverflowPage 		*npage;			/* Page Pointer to the overflow page */
    Buffer_ACC_CB 		*npage_BCBP;		/* buffer access control block containing data */
    Lsn_T 			lsn;                  	/* lsn of the newly written log record */
    Four 			logRecLen;             	/* log record length */
    LOG_LogRecInfo_T 		logRecInfo; 		/* log record information */
    LOG_Image_MLGF_ObjectsInOverflow_T objsInfo;

    /* pointer for COMMON Data Structure of perThreadTable */
    COMMON_PerThreadDS_T *common_perThreadDSptr = COMMON_PER_THREAD_DS_PTR(handle);

    TR_PRINT(handle, TR_MLGF, TR1, ("mlgf_OverflowMerge()"));


    e = TM_XT_BeginNestedTopAction(handle, xactEntry, &xactEntry->lastLsn);
    if (e < eNOERROR) ERR(handle, e);

    /*
     * Write log record.
     */
    if (logParam->logFlag & LOG_FLAG_DATA_LOGGING) {

        /*
         * logging for page1
         */
        objsInfo.startObjArrayElemNo = page1->hdr.nObjects;
        objsInfo.nObjects = page2->hdr.nObjects;

        LOG_FILL_LOGRECINFO_2(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                              LOG_ACTION_MLGF_INSERT_OBJECTS_INTO_OVERFLOW_PAGE, LOG_REDO_UNDO,
                              page1->hdr.pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                              sizeof(LOG_Image_MLGF_ObjectsInOverflow_T), &objsInfo,
                              page2->hdr.nObjects*objectItemLen, MLGF_OVERFLOW_ITH_OBJECTITEM(objectItemLen, page2, 0));

        e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
        if (e < eNOERROR) ERR(handle, e);


        LOG_FILL_LOGRECINFO_2(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                              LOG_ACTION_MLGF_MODIFY_OVERFLOW_CHAIN_NEXT_LINK, LOG_REDO_UNDO,
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
        LOG_FILL_LOGRECINFO_2(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                              LOG_ACTION_MLGF_MODIFY_INDEXID_OF_MLGF_PAGE, LOG_REDO_UNDO,
                              page2->hdr.pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                              sizeof(IndexID), &(common_perThreadDSptr->nilIid),
                              sizeof(IndexID), &page2->hdr.iid);

        e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
        if (e < eNOERROR) ERR(handle, e);

        page2->hdr.lsn = lsn;
        page2->hdr.logRecLen = logRecLen;
    }


    /* Copy the contents of the page2 to the page1 */
    memcpy(MLGF_OVERFLOW_ITH_OBJECTITEM(objectItemLen, page1, page1->hdr.nObjects),
           MLGF_OVERFLOW_ITH_OBJECTITEM(objectItemLen, page2, 0),
	   page2->hdr.nObjects*objectItemLen);

    page1->hdr.nObjects += page2->hdr.nObjects;

    /* Update links */
    MAKE_PAGEID(nextPid, page2->hdr.pid.volNo, page2->hdr.nextPage);
    page1->hdr.nextPage = nextPid.pageNo;
    if (nextPid.pageNo != NIL) {
	e = BfM_getAndFixBuffer(handle, &nextPid, M_FREE, &npage_BCBP, PAGE_BUF);
	if (e < eNOERROR) ERR(handle, e);

	npage = (mlgf_OverflowPage *)npage_BCBP->bufPagePtr;

        /*
         * Write log record.
         */
        if (logParam->logFlag & LOG_FLAG_DATA_LOGGING) {

            LOG_FILL_LOGRECINFO_2(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                                  LOG_ACTION_MLGF_MODIFY_OVERFLOW_CHAIN_PREV_LINK, LOG_REDO_UNDO,
                                  npage->hdr.pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                                  sizeof(ShortPageID), &page2->hdr.prevPage,
                                  sizeof(ShortPageID), &npage->hdr.prevPage);

            e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
            if (e < eNOERROR) ERRB1(handle, e, npage_BCBP, PAGE_BUF);

            npage->hdr.lsn = lsn;
            npage->hdr.logRecLen = logRecLen;
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

} /* mlgf_OverflowMerge() */



/*
 * Function: void mlgf_OverflowDistribute(Four, Buffer_ACC_CB*, Buffer_ACC_CB*, Four)
 *
 * Description:
 *  If the size of the left page is larger than the right page, ObjectIDs are
 *  moved from the left page to the right page, otherwise from the right
 *  page to the left page.
 *
 * Returns:
 *  None
 */
Four mlgf_OverflowDistribute(
    Four			handle,
    XactTableEntry_T 		*xactEntry, 		/* IN transaction table entry */
    mlgf_OverflowPage 		*page1,   		/* INOUT The left page to be redistributed */
    mlgf_OverflowPage 		*page2,   		/* INOUT The right page to be redistributed */
    Four 			objectItemLen,         	/* IN length of an object item in object array */
    LogParameter_T 		*logParam)   		/* IN log parameter */
{
    Four 			e;                     	/* error code */
    Four 			nMovedObjects;		/* # of objects to move */
    mlgf_OverflowPage 		*srcPage; 		/* pointer to the source page */
    mlgf_OverflowPage 		*dstPage; 		/* pointer to the destination page */
    Four 			srcOidArrayElemNo;     	/* starting position of delete */
    Four 			dstOidArrayElemNo;     	/* starting position of insert */
    Lsn_T 			lsn;                 	/* lsn of the newly written log record */
    Four 			logRecLen;             	/* log record length */
    LOG_LogRecInfo_T 		logRecInfo; 		/* log record information */
    LOG_Image_MLGF_ObjectsInOverflow_T objsInfo;


    /* pointer for COMMON Data Structure of perThreadTable */
    COMMON_PerThreadDS_T *common_perThreadDSptr = COMMON_PER_THREAD_DS_PTR(handle);

    TR_PRINT(handle, TR_MLGF, TR1, ("mlgf_OverflowDistribute()"));


    if( page1->hdr.nObjects > page2->hdr.nObjects ) {	/* page1 -> page2 */
	/*@ number of objects to move */
	nMovedObjects = (page1->hdr.nObjects - page2->hdr.nObjects)/2;


        /* for writing log */
        srcPage = page1;
        dstPage = page2;

        srcOidArrayElemNo = srcPage->hdr.nObjects - nMovedObjects;
        dstOidArrayElemNo = 0;


	/* reserve space in page2 */
        MLGF_INSERT_OBJECTS_SPACE_INTO_OBJECT_ARRAY(
            page2->data, page2->hdr.nObjects, 0, nMovedObjects, objectItemLen);

	/* move the objects from page1 to page2 */
        MLGF_WRITE_OBJECTS_IN_OBJECT_ARRAY(
            MLGF_OVERFLOW_ITH_OBJECTITEM(objectItemLen, page2, 0),
            0, nMovedObjects,
            MLGF_OVERFLOW_ITH_OBJECTITEM(objectItemLen, page1, page1->hdr.nObjects-nMovedObjects), objectItemLen);

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
        MLGF_WRITE_OBJECTS_IN_OBJECT_ARRAY(
            MLGF_OVERFLOW_ITH_OBJECTITEM(objectItemLen, page1, 0),
            page1->hdr.nObjects, nMovedObjects,
            MLGF_OVERFLOW_ITH_OBJECTITEM(objectItemLen, page2, 0), objectItemLen);

	/* fill the moved space in page1 */
        MLGF_DELETE_OBJECTS_SPACE_FROM_OBJECT_ARRAY(
            MLGF_OVERFLOW_ITH_OBJECTITEM(objectItemLen, page2, 0),
            page2->hdr.nObjects, 0, nMovedObjects, objectItemLen);

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
        objsInfo.startObjArrayElemNo = srcOidArrayElemNo;
        objsInfo.nObjects = nMovedObjects;

        LOG_FILL_LOGRECINFO_2(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                              LOG_ACTION_MLGF_DELETE_OBJECTS_FROM_OVERFLOW_PAGE, LOG_REDO_UNDO,
                              srcPage->hdr.pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                              sizeof(LOG_Image_MLGF_ObjectsInOverflow_T), &objsInfo,
                              nMovedObjects*objectItemLen, MLGF_OVERFLOW_ITH_OBJECTITEM(objectItemLen, dstPage, dstOidArrayElemNo));

        e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
        if (e < eNOERROR) ERR(handle, e);

        srcPage->hdr.lsn = lsn;
        srcPage->hdr.logRecLen = logRecLen;


        /*
         * logging for the page into which objects were inserted
         */
        objsInfo.startObjArrayElemNo = dstOidArrayElemNo;
        objsInfo.nObjects = nMovedObjects;

        LOG_FILL_LOGRECINFO_2(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                              LOG_ACTION_MLGF_INSERT_OBJECTS_INTO_OVERFLOW_PAGE, LOG_REDO_UNDO,
                              dstPage->hdr.pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                              sizeof(LOG_Image_MLGF_ObjectsInOverflow_T), &objsInfo,
                              nMovedObjects*objectItemLen, MLGF_OVERFLOW_ITH_OBJECTITEM(objectItemLen, dstPage, dstOidArrayElemNo));

        e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
        if (e < eNOERROR) ERR(handle, e);

        dstPage->hdr.lsn = lsn;
        dstPage->hdr.logRecLen = logRecLen;
    }

    /*
     * write a dummy CLR for the nested top action.
     */
    e = TM_XT_EndNestedTopAction(handle, xactEntry, logParam);
    if (e < eNOERROR) ERR(handle, e);

    return(eNOERROR);

} /* mlgf_OverflowDistribute() */
