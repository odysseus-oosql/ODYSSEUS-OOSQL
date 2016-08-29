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
/*    Coarse-Granule Locking (Volume Lock) Version                            */
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
 */


#include <string.h>
#include "common.h"
#include "error.h"
#include "trace.h"
#include "Util.h"
#include "RDsM_Internal.h"
#include "BfM.h"
#include "MLGF_Internal.h"
#include "perThreadDS.h"
#include "perProcessDS.h"


/* Internal Function Prototypes */
Four mlgf_SplitOverflow(Four, ObjectID*, PageID*, mlgf_OverflowPage*, MLGF_KeyDesc*); 
Four mlgf_OverflowMerge(Four, mlgf_OverflowPage*, mlgf_OverflowPage*, Two, Pool*, DeallocListElem*);
Four mlgf_OverflowDistribute(Four, mlgf_OverflowPage*, mlgf_OverflowPage*, Two);


/*
 * Function: Four mlgf_CreateOverflow(PageID*, mlgf_LeafPage*, MLGF_KeyDesc*, Two)
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
    Four handle,
    ObjectID            *catObjForFile,         /* IN catalog object of index file */
    PageID              *root,                  /* IN root page ID */ 
    mlgf_LeafPage       *apage,                 /* INOUT leaf page including the leaf entry */
    MLGF_KeyDesc        *kdesc,                 /* IN key descriptor of MLGF index */
    Two                 entryNo)                /* IN entry to be converted into overflow page */
{
    Four                e;                      /* error number */
    Boolean             isTmp;                  
    Two                 objectItemLen;          /* length of an object item of object array*/
    char                *firstObjectItem;       /* points to the first object item in object array */
    PageID              newPid;                 /* PageID of the newlly created overflow page */
    mlgf_LeafEntry      *entry;                 /* pointer to the entryNo-th leaf entry */
    mlgf_OverflowPage   *opage;                 /* pointer to the buffer for the overflow page */


    TR_PRINT(TR_MLGF, TR1, ("mlgf_CreateOverflow(handle)"));


    /* Allocat a new page and initialize it as an overflow page. */
    e = mlgf_AllocPage(handle, catObjForFile, &apage->hdr.pid, &newPid);
    if (e < eNOERROR) ERR(handle, e);
    
    /* check this MLGF is temporary */
    e = mlgf_IsTemporary(handle, catObjForFile, &isTmp);
    if (e < 0)  ERR(handle, e);

    /* Read the page into the buffer. */
    e = BfM_GetNewTrain(handle, &newPid, (char**)&opage, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);

    /* Initialize the new page as an overflow page. */
    MLGF_INIT_OVERFLOW_PAGE(opage, isTmp, newPid, NIL, NIL, kdesc->extraDataLen);

    /* entry points to the leaf entry which will be converted to the overflow chain. */
    entry = MLGF_ITH_LEAFENTRY(apage, entryNo);

    /* firstObjectItem points to the first object item in the leaf entry. */
    firstObjectItem = MLGF_LEAFENTRY_FIRST_OBJECT(kdesc->nKeys, entry);
    objectItemLen = MLGF_LEAFENTRY_OBJECTITEM_LEN(kdesc->extraDataLen);

    /* Move the objects from the leaf entry to the overflow page. */
    memcpy(MLGF_OVERFLOW_ITH_OBJECTITEM(objectItemLen, opage, 0),
	   firstObjectItem, entry->nObjects*objectItemLen);

    opage->hdr.nObjects = entry->nObjects; 

    /* Set dirtyFlag to 1 in order to mark that the overflow page was updated. */
    e = BfM_SetDirty(handle, &newPid, PAGE_BUF);
    if (e < eNOERROR) ERRB1(handle, e, &newPid, PAGE_BUF);

    e = BfM_FreeTrain(handle, &newPid, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);

    /* Release space used for the objects. */
    apage->hdr.unused += entry->nObjects*objectItemLen - sizeof(ShortPageID);

    /* The leaf entry has an overflow PageID instead of Object list */
    entry->nObjects = NIL;
    *((ShortPageID*)firstObjectItem) = newPid.pageNo;

    return(eNOERROR);

} /* mlgf_CreateOverflow() */



/*
 * Function: Four mlgf_InsertOverflow(PageID*, MLGF_KeyDesc*, ObjectID*, char*)
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
    Four handle,
    ObjectID                    *catObjForFile,         /* IN catalog object of index file */
    PageID                      *root,                  /* IN root page ID */ 
    PageID                      *overPid,               /* IN where the object to be inserted */
    MLGF_KeyDesc                *kdesc,                 /* IN key descriptor of this index */
    MLGF_HashValue              keys[],                 /* IN hash values of key values */
    ObjectID                    *oid,                   /* IN ObjectID to be inserted */
    char                        *data)                  /* IN extra data to be inseted */
{
    Four                        e;                      /* error number */
    Two                         idx;                    /* the position to be inserted */
    PageID                      nextPage;               /* the Next Overflow Page */
    mlgf_OverflowPage           *opage;                 /* Page Pointer to the overflow page */
    Boolean                     found;                  /* search result */
    Two                         objectItemLen;          /* length of an object item in object array */
    char                        *lastObjectItem;        /* points to last object in the overflow page */
    char                        *objectItemPtr;         /* points to an object item in overflow page */


    TR_PRINT(TR_MLGF, TR1, ("mlgf_InsertOverflow(handle)"));


    e = BfM_GetTrain(handle, overPid, (char**)&opage, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);

    /* Get the length of an object item. */
    objectItemLen = MLGF_LEAFENTRY_OBJECTITEM_LEN(kdesc->extraDataLen);

    /* Get the last object in the given overflow page. */
    lastObjectItem = MLGF_OVERFLOW_ITH_OBJECTITEM(objectItemLen, opage, opage->hdr.nObjects-1);

    if (mlgf_ObjectIdComp(handle, oid, (ObjectID*)lastObjectItem) == GREAT && opage->hdr.nextPage != NIL) {
        /* insert it into the next page */
	MAKE_PAGEID(nextPage, overPid->volNo, opage->hdr.nextPage);

        e = BfM_FreeTrain(handle, overPid, PAGE_BUF);
        if (e < eNOERROR) ERR(handle, e);

        e = mlgf_InsertOverflow(handle, catObjForFile, root, &nextPage, kdesc, keys, oid, data); 
        if (e < eNOERROR) ERR(handle, e);

        return(eNOERROR);
    }


    /* The ObjectID should be inserted in this given page */

    if (opage->hdr.nObjects < MLGF_OVERFLOW_MAXNUM_OBJECTS(objectItemLen)) {
        /* Search the correct postion to be inserted using the binary search */
        found = mlgf_BinarySearchObjectArray(
            handle, MLGF_OVERFLOW_ITH_OBJECTITEM(objectItemLen, opage, 0),
            oid, opage->hdr.nObjects, objectItemLen, &idx);
        if (found) ERRB1(handle, eDUPLICATEDOBJECTID_BTM, overPid, PAGE_BUF);

        /* objectItemPtr points to the object item where the new object */
        /* will be inserted. */
        objectItemPtr = MLGF_OVERFLOW_ITH_OBJECTITEM(objectItemLen, opage, idx);

        /* Make room for the object */
        MLGF_INSERT_OBJECTS_SPACE_INTO_OBJECT_ARRAY(
            MLGF_OVERFLOW_ITH_OBJECTITEM(objectItemLen, opage, 0),
            opage->hdr.nObjects, idx, 1, objectItemLen);

        /* Insert the new object into the overflow page. */
        *((ObjectID*)objectItemPtr) = *oid;
        if (kdesc->extraDataLen != 0)
            memcpy(objectItemPtr + sizeof(ObjectID), data, kdesc->extraDataLen);
        (opage->hdr.nObjects)++;

    } else {	/* not enough */
        /* Insert it after split */
        e = mlgf_SplitOverflow(handle, catObjForFile, root, opage, kdesc); 
        if (e < eNOERROR) ERRB1(handle, e, overPid, PAGE_BUF);

        e = mlgf_InsertOverflow(handle, catObjForFile, root, overPid, kdesc, keys, oid, data); 
        if (e < eNOERROR) ERRB1(handle, e, overPid, PAGE_BUF);
    }

    e = BfM_SetDirty(handle, overPid, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);

    e = BfM_FreeTrain(handle, overPid, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);

    return(eNOERROR);

} /* mlgf_InsertOverflow() */



/*
 * Function: Four mlgf_DeleteOverflow(PageID*, MLGF_KeyDesc*, ObjectID*, char*,
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
    Four handle,
    PageID                      *root,                  /* IN root page ID */ 
    PageID                      *overPid,               /* IN where the given object will be deleted? */
    MLGF_KeyDesc                *kdesc,                 /* IN key descriptor of used index */
    MLGF_HashValue              keys[],                 /* IN hash values of key values */
    ObjectID                    *oid,                   /* IN ObjectID to be deleted */
    char                        *data,                  /* OUT extra data of the deleted object */
    Four                        *f,                     /* OUT # of ObjectIDs if underflow occur otherwise, max number of object ids */
    Pool                        *dlPool,                /* INOUT pool of dealloc list elements */
    DeallocListElem             *dlHead)                /* INOUT head of the dealloc list */
{
    Four                        e;                      /* error number */
    Two                         idx;                    /* nth ObjectID in the overflow page */
    PageID                      prevPid;                /* The previous Page of the Overflow Page */
    PageID                      nextPid;                /* The nextPage of the given overflow page */
    ObjectID                    curOid;                 /* The current ObjectID which is examined */
    mlgf_OverflowPage           *mpage;                 /* Page Pointer to the neighbor page */
    PageID                      *mpid;                  /* page id of the neighbor page */
    mlgf_OverflowPage           *opage;                 /* Page Pointer to the given overflow page */
    Boolean                     found;                  /* search result */
    mlgf_OverflowPage           *leftPage;              /* left page of between two overflow pages */
    mlgf_OverflowPage           *rightPage;             /* right page of between two overflow pages */
    PageID                      *rightPid;              /* page id of the right page */
    Two                         objectItemLen;          /* length of an object item in object array */
    char                        *lastObjectItem;        /* points to last object in the overflow page */
    char                        *objectItemPtr;         /* points to an object item in overflow page */


    TR_PRINT(TR_MLGF, TR1, ("mlgf_DeleteOverflow(handle)"));


    e = BfM_GetTrain(handle, overPid, (char**)&opage, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);

    /* Get the length of an object item. */
    objectItemLen = MLGF_LEAFENTRY_OBJECTITEM_LEN(kdesc->extraDataLen);

    /* Get the last object in the given overflow page. */
    lastObjectItem = MLGF_OVERFLOW_ITH_OBJECTITEM(objectItemLen, opage, opage->hdr.nObjects-1);

    MAKE_PAGEID(nextPid, overPid->volNo, opage->hdr.nextPage);

    if(mlgf_ObjectIdComp(handle, oid, (ObjectID*)lastObjectItem) == GREAT) { /* Not exist in this page */

        e = BfM_FreeTrain(handle, overPid, PAGE_BUF);
        if (e < eNOERROR) ERR(handle, e);

        if (IS_NILPAGEID(nextPid)) ERR(handle, eNOTFOUND); 

        /* Recursively delete the object using the next page */
        e = mlgf_DeleteOverflow(handle, root, &nextPid, kdesc, keys, 
                                oid, data, f, dlPool, dlHead);
        if( e < eNOERROR ) ERR(handle, e);

        return(eNOERROR);
    }

    /* The deleted object is in this page, if exist. */

    /*@ Search the ObjectID */
    found = mlgf_BinarySearchObjectArray(
	handle, MLGF_OVERFLOW_ITH_OBJECTITEM(objectItemLen, opage, 0),
	oid, opage->hdr.nObjects, objectItemLen, &idx);

    if (!found) ERRB1(handle, eNOTFOUND, overPid, PAGE_BUF);


    objectItemPtr = MLGF_OVERFLOW_ITH_OBJECTITEM(objectItemLen, opage, idx);
    if (data) memcpy(data, objectItemPtr + sizeof(ObjectID), kdesc->extraDataLen);

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
            e = BfM_GetTrain(handle, mpid, (char**)&mpage, PAGE_BUF);
            if (e < eNOERROR) ERRB1(handle, e, overPid, PAGE_BUF);

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
                e = mlgf_OverflowMerge(handle, leftPage, rightPage, objectItemLen, dlPool, dlHead);
                if (e < eNOERROR) ERRB2(handle, e, overPid, PAGE_BUF, mpid, PAGE_BUF);
            } else { /* more than one page size */
                e = mlgf_OverflowDistribute(handle, leftPage, rightPage, objectItemLen);
                if (e < eNOERROR) ERRB2(handle, e, overPid, PAGE_BUF, mpid, PAGE_BUF);
            }

            e = BfM_SetDirty(handle, mpid, PAGE_BUF);
            if (e < eNOERROR) ERRB1(handle, e, overPid, PAGE_BUF);

            e = BfM_FreeTrain(handle, mpid, PAGE_BUF);
            if (e < eNOERROR) ERRB1(handle, e, overPid, PAGE_BUF);
        }
    }

    e = BfM_SetDirty(handle, overPid, PAGE_BUF);
    if (e < eNOERROR) ERRB1(handle, e, overPid, PAGE_BUF);

    e = BfM_FreeTrain(handle, overPid, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);

    return(eNOERROR);

} /* mlgf_DeleteOverflow() */



/*
 * Function: Four mlgf_SplitOverflow(PageID*, mlgf_OverflowPage*, MLGF_KeyDesc*)
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
    Four handle,
    ObjectID            *catObjForFile,         /* IN catalog object of index file */
    PageID              *root,                  /* IN root page ID */ 
    mlgf_OverflowPage   *fpage,                 /* INOUT the page which will be splitted */
    MLGF_KeyDesc        *kdesc)                 /* IN key descriptor of used index */
{
    Four                e;                      /* error number */
    Boolean             isTmp;                  
    Four                how;                    /* how may bytes are moved to the new page */
    Four                from;                   /* the starting byte to be copied */
    PageID              newPid;                 /* a new allocated page */
    PageID              nextPid;                /* for maintaining doubly linked list */
    mlgf_OverflowPage   *npage;                 /* a page pointer to the new/next page */
    Two                 objectItemLen;          /* length of an object item of object array*/


    TR_PRINT(TR_MLGF, TR1, ("mlgf_SplitOverflow(handle)"));


    objectItemLen = MLGF_LEAFENTRY_OBJECTITEM_LEN(kdesc->extraDataLen);

    /*@ Allocate a new page and initialize it as an overflow page. */
    e = mlgf_AllocPage(handle, catObjForFile, &fpage->hdr.pid, &newPid);
    if (e < eNOERROR)  ERR(handle, e);
    
    /* check this MLGF is temporary */
    e = mlgf_IsTemporary(handle, catObjForFile, &isTmp);
    if (e < 0)  ERR(handle, e);

    e = BfM_GetNewTrain(handle, &newPid, (char**)&npage, PAGE_BUF);
    if (e < eNOERROR)  ERR(handle, e);

    /* Initialize the new page to the overflow page. */
    MLGF_INIT_OVERFLOW_PAGE(npage, isTmp, newPid, fpage->hdr.pid.pageNo,
                            fpage->hdr.nextPage, fpage->hdr.extraDataLen);


    /* Half of the objects are remained in the original page and the rest of
     * the objects are moved to the new allocated overflow page.
     */
    from = (fpage->hdr.nObjects+1) / 2;
    how = fpage->hdr.nObjects - from;

    /* Move half of the objects and update the variables of two pages */
    memcpy(
        MLGF_OVERFLOW_ITH_OBJECTITEM(objectItemLen, npage, 0),
        MLGF_OVERFLOW_ITH_OBJECTITEM(objectItemLen, fpage, from),
        how*objectItemLen);
    fpage->hdr.nObjects = from;
    npage->hdr.nObjects = how;

    /*@ Page ID of the next page */
    MAKE_PAGEID(nextPid, fpage->hdr.pid.volNo, fpage->hdr.nextPage);

    /* Update links to maintain doubly linked list */
    fpage->hdr.nextPage = newPid.pageNo;

    e = BfM_SetDirty(handle, &newPid, PAGE_BUF);
    if (e < eNOERROR) ERRB1(handle, e, &newPid, PAGE_BUF);

    e = BfM_FreeTrain(handle, &newPid, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);

    if (nextPid.pageNo != NIL) {
	e = BfM_GetTrain(handle, &nextPid, (char**)&npage, PAGE_BUF);
	if(e < eNOERROR) ERR(handle, e);
        
	npage->hdr.prevPage = newPid.pageNo;

	e = BfM_SetDirty(handle, &nextPid, PAGE_BUF);
        if (e < eNOERROR) ERRB1(handle, e, &nextPid, PAGE_BUF);

	e = BfM_FreeTrain(handle, &nextPid, PAGE_BUF);
	if (e < eNOERROR) ERR(handle, e);
    }

    return(eNOERROR);

} /* mlgf_SplitOverflow() */



/*
 * Function: Four mlgf_OverflowMerge(mlgf_OverflowPage*, mlgf_OverflowPage*, Two)
 *
 * Description:
 *  All objects in the right page are copied to the right page.
 *  After the copy is completed, 'nextPage' and 'prevPage' are updated to
 *  maintain doubly linked list. Finally, deallocate the the right page.
 */
Four mlgf_OverflowMerge(
    Four handle,
    mlgf_OverflowPage   *page1,         /* INOUT page to be copied to */
    mlgf_OverflowPage   *page2,         /* INOUT page to be copied from */
    Two                 objectItemLen,  /* IN length of an object item in object array */
    Pool                *dlPool,        /* INOUT pool of dealloc list elements */
    DeallocListElem     *dlHead)        /* INOUT head of the dealloc list */
{
    FileID              fid;
    Four                e;              /* error number */
    PageID              nextPid;        /* The next page of page2 */
    mlgf_OverflowPage   *npage;         /* Page Pointer to the overflow page */
    DeallocListElem     *dlElem;        /* an element of dealloc list */


    TR_PRINT(TR_MLGF, TR1, ("mlgf_OverflowMerge(handle)"));


    /* Copy the contents of the page2 to the page1 */
    memcpy(MLGF_OVERFLOW_ITH_OBJECTITEM(objectItemLen, page1, page1->hdr.nObjects),
           MLGF_OVERFLOW_ITH_OBJECTITEM(objectItemLen, page2, 0),
	   page2->hdr.nObjects*objectItemLen);

    page1->hdr.nObjects += page2->hdr.nObjects;

    /* Update links */
    MAKE_PAGEID(nextPid, page2->hdr.pid.volNo, page2->hdr.nextPage);
    page1->hdr.nextPage = nextPid.pageNo;
    if (nextPid.pageNo != NIL) {
	e = BfM_GetTrain(handle, &nextPid, (char**)&npage, PAGE_BUF);
	if (e < eNOERROR) ERR(handle, e);

	npage->hdr.prevPage = page2->hdr.prevPage;

	e = BfM_SetDirty(handle, &nextPid, PAGE_BUF);
        if (e < eNOERROR) ERRB1(handle, e, &nextPid, PAGE_BUF);

	e = BfM_FreeTrain(handle, &nextPid, PAGE_BUF);
	if (e < eNOERROR) ERR(handle, e);
    }

    page2->hdr.type = MLGF_FREEPAGE;

    /*
    ** Insert a new node for the dropped file.
    ** Original code:
    ** e = RDsM_FreeTrain(&page2->hdr.pid, PAGESIZE2);
    ** if (e < eNOERROR) ERR(handle, e);
    */    
    e = Util_getElementFromPool(handle, dlPool, &dlElem);
    if (e < 0) ERR(handle, e);

    dlElem->type = DL_PAGE;
    dlElem->elem.pid = page2->hdr.pid; /* save the deallcoated PageID. */
    dlElem->next = dlHead->next; /* insert to the list */
    dlHead->next = dlElem;       /* new first element of the list */

    return(eNOERROR);

} /* mlgf_OverflowMerge() */



/*
 * Function: void mlgf_OverflowDistribute(mlgf_OverflowPage*, mlgf_OverflowPage*, Two)
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
    Four handle,
    mlgf_OverflowPage           *page1,         /* INOUT The left page to be redistributed */
    mlgf_OverflowPage           *page2,         /* INOUT The right page to be redistributed */
    Two                         objectItemLen)  /* IN length of an object item in object array */
{
    Four                        e;              /* error code */
    Four                        nMovedObjects;  /* # of objects to move */


    TR_PRINT(TR_MLGF, TR1, ("mlgf_OverflowDistribute(handle)"));


    if( page1->hdr.nObjects > page2->hdr.nObjects ) {	/* page1 -> page2 */
	/*@ number of objects to move */
	nMovedObjects = (page1->hdr.nObjects - page2->hdr.nObjects)/2;

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

    return(eNOERROR);

} /* mlgf_OverflowDistribute() */
