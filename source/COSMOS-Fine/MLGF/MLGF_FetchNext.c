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
 * Module: MLGF_FetchNext.c
 *
 * Description:
 *  Fetch the next object of cursor in the given range. If found, cursor
 *  points to the next object and cursor's flag is set to CURSOR_ON.
 *  Otherwise, cursor's flag is set to CURSOR_EOS;
 *
 * Exports:
 *  Four MLGF_FetchNext(Four, IndexID*, MLGF_KeyDesc*, MLGF_HashValue*,
 *                      MLGF_HashValue[], MLGF_Cursor*, char*, LockParameter*)
 */


#include <string.h>
#include "common.h"
#include "error.h"
#include "trace.h"
#include "Util.h"
#include "TM.h"
#include "MLGF.h"
#include "LM.h"
#include "perProcessDS.h"
#include "perThreadDS.h"

/* Internal Function Prototypes */
Four mlgf_FetchNextFromPathTop(Four, XactTableEntry_T*, MLGF_KeyDesc*, MLGF_HashValue*, MLGF_HashValue*,
                               MLGF_Cursor*, mlgf_MortonValue*, char *data,
                               Boolean, PageID*, LockParameter*);
Four mlgf_FetchNextFromLeaf(Four, XactTableEntry_T*, MLGF_KeyDesc*, MLGF_HashValue*, MLGF_HashValue*, MLGF_Cursor*,
			    mlgf_MortonValue*, char*, Boolean, Boolean, LockParameter*);
Four mlgf_FetchNextInOverflow(Four, MLGF_KeyDesc*, MLGF_Cursor*, char*);


/*
 * Function: MLGF_FetchNext(handle, IndexID*, MLGF_KeyDesc*, MLGF_HashValue[],
 *                          MLGF_HashValue[], MLGF_Cursor*, char*, LockParameter*)
 *
 * Description:
 *  Fetch the next object of the current object pointed by the cursor.
 *  If the current object was fetched in the overflow page, then first look
 *  at an overflow chaing connected to the overflow page. If we find the
 *  next object, then return it.
 *  And then, we search for the next object from the leaf page. If we find it,
 *  then return. At last, we search from the directory page cached in the path
 *  of the cursor.
 *
 * Returns:
 *  Error code
 *    eBADPARAMETER
 *    some erros caused by function calls
 */
Four MLGF_FetchNext(
    Four                        handle,                 /* IN handle */
    XactTableEntry_T 		*xactEntry, 		/* IN transaction table entry */
    MLGFIndexInfo               *iinfo,                 /* IN MLGF Index Info */
    MLGF_KeyDesc 		*kdesc,			/* IN key descriptor of this index */
    MLGF_HashValue 		*lowerBound,		/* IN lower bound of region to fetch */
    MLGF_HashValue 		*upperBound,		/* IN upper bound of region to fetch */
    MLGF_Cursor 		*cursor,		/* INOUT return the position of fetched object */
    char 			*data,			/* OUT return the extra data */
    LockParameter 		*lockup)      		/* IN request lock or not */
{
    Four 			e;			/* error code */
    Four 			i;			/* index variable */
    Four 			status;			/* program control state */
    Four 			cmp;			/* result of comparison */
    ObjectID 			*firstOid;		/* first object in overflow page */
    mlgf_OverflowPage 		*opage;			/* an overflow page */
    mlgf_LeafPage 		*apage;			/* a leaf page */
    Buffer_ACC_CB 		*ov_BCB;		/* buffer control block for overflow page */
    Buffer_ACC_CB 		*leaf_BCB;		/* buffer control block for leaf page */
    mlgf_MortonValue 		keyMortonVal; 		/* mroton value for the key */
    One 			nValidBits[MLGF_MAXNUM_KEYS]; /* used for getting morton value */
    LockReply 			lockReply; 
    LockMode 			oldMode;
    PageID 			rootPid;		/* PageID of the root page */


    /* pointer for COMMON Data Structure of perThreadTable */
    COMMON_PerThreadDS_T *common_perThreadDSptr = COMMON_PER_THREAD_DS_PTR(handle);


    TR_PRINT(handle, TR_MLGF, TR1,
	     ("MLGF_FetchNext(xactEntry=%P, iinfo=%P, kdesc=%P, lowerBound=%P, upperBound=%P, cursor=%P, data=%P, lockup=%P",
	     xactEntry, iinfo, kdesc, lowerBound, upperBound, cursor, data, lockup));


    if (iinfo == NULL || kdesc == NULL || lowerBound == NULL || upperBound == NULL || 
	cursor == NULL)
	ERR(handle, eBADPARAMETER);

    /* Check cursor. */
    if (cursor->flag != CURSOR_ON) ERR(handle, eBADCURSOR);

    /* Get 'rootPid' from MLGF index info */
    e = mlgf_GetRootPid(handle, xactEntry, iinfo, &rootPid, lockup);
    if (e < eNOERROR) ERR(handle, e);

    /* get lock on the root page */
    if(lockup) {

	e = LM_getFlatPageLock(handle, &xactEntry->xactId, &(cursor->leaf), L_S, L_MANUAL,
			       L_UNCONDITIONAL, &lockReply, &oldMode);
	if (e < eNOERROR) ERR(handle, e);

	if(lockReply == LR_DEADLOCK){
	    ERR(handle, eDEADLOCK);
	}

    }

    if (IS_NILPAGEID(cursor->overflow)) { /* normal entry */
	/* Set status to MLGF_STATUS_INVALIDPAGE in order to search from leaf. */
	status = MLGF_STATUS_INVALIDPAGE;

    } else {			/* overflow chain */
	/* Read the overflow page into the buffer. */
	e = BfM_getAndFixBuffer(handle, &cursor->overflow, M_FREE, &ov_BCB, PAGE_BUF);
	if (e < eNOERROR) ERR(handle, e);

	opage = (mlgf_OverflowPage*)ov_BCB->bufPagePtr;

	if (IS_NILINDEXID(opage->hdr.iid)) /* The page has been deallocated. */
	    status = MLGF_STATUS_INVALIDPAGE;
	else {

	    /* Compare the current ObjectID with the first ObjectID in this overflow page. */
	    firstOid = (ObjectID*)MLGF_OVERFLOW_ITH_OBJECTITEM(
		MLGF_LEAFENTRY_OBJECTITEM_LEN(kdesc->extraDataLen), opage, 0);

	    cmp = mlgf_ObjectIdComp(handle, &cursor->oid, firstOid);

	    if (cmp == LESS) status = MLGF_STATUS_INVALIDPAGE;
	    else {
		status = mlgf_FetchNextInOverflow(handle, kdesc, cursor, data);
		if (status < 0) {
		    ERRB1(handle, status, ov_BCB, PAGE_BUF);
		}
	    }
	}

	/* unfix buffer. */
	e = BfM_unfixBuffer(handle, ov_BCB, PAGE_BUF);
	if (e < eNOERROR) ERR(handle, e);

	if (status == MLGF_STATUS_FOUND) return(eNOERROR);
    }


    /* Get the morton value of the search_for_hashValue. */
    for (i = 0; i < kdesc->nKeys; i++) 
	nValidBits[i] = MLGF_MAXNUM_VALIDBITS;

    mlgf_GetMortonValue(handle, cursor->keys, nValidBits, &keyMortonVal, kdesc->nKeys);


    /* Read the leaf page into the buffer. */
    e = BfM_getAndFixBuffer(handle, &cursor->leaf, M_FREE, &leaf_BCB, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);

    apage = (mlgf_LeafPage*)leaf_BCB->bufPagePtr;

    if (IS_NILINDEXID(apage->hdr.iid)) /* The page has been deallocated. */
	status = MLGF_STATUS_INVALIDPAGE;
    else {
	/*
	 * It is not necessary to check the boundary condition.
	 * If the entry was moved into the other page, then the original page
	 * must have been deallocated. (There is no redistribution in MLGF and
	 * the page deallocation is deffered until the transaction ends).
	 * Assumption: When a page splits, the original page has the entries
	 *    numbered with the lower number entry no.
	 */
	status = mlgf_FetchNextFromLeaf(handle,
	    xactEntry, kdesc, lowerBound, upperBound, cursor, &keyMortonVal, data, TRUE,
	    (status == MLGF_STATUS_NOTFOUND) ? TRUE:FALSE, lockup);
	if (status < 0) {
	    ERRB1(handle, status, leaf_BCB, PAGE_BUF);
	}
    }

    /* unfix buffer. */
    if (e < eNOERROR) ERRB1(handle, e, leaf_BCB, PAGE_BUF);

    e = BfM_unfixBuffer(handle, leaf_BCB, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);

    if(lockup){
	e = LM_releaseFlatPageLock(handle, &xactEntry->xactId, &(cursor->leaf), L_MANUAL);
	if (e < eNOERROR) ERR(handle, e);

    }

    if (status == MLGF_STATUS_FOUND) return(eNOERROR); 

    /* Retraverse the index using the path information in the cursor. */
    e = mlgf_FetchNextFromPathTop(handle,
	xactEntry, kdesc, lowerBound, upperBound, cursor, &keyMortonVal, data,
	(status == MLGF_STATUS_NOTFOUND) ? TRUE:FALSE, &(common_perThreadDSptr->nilPid), lockup); 
    if (e < eNOERROR) ERR(handle, e);

    if (e == MLGF_STATUS_NOTFOUND) cursor->flag = CURSOR_EOS;

    return(eNOERROR);

} /* MLGF_FetchNext( ) */



/*
 * Function: Four mlgf_FetchNextFromPathTop(Four, MLGF_KeyDesc*, MLGF_HashValue*,
 *                         MLGF_HashValue*, MLGF_Cursor*, char *data, Boolean,
 *                         PageID*, LockParameter*)
 *
 * Description:
 *  In order to search for the next object, we retraverse from the index.
 *  We use the path information cached in the path stack of the cursor.
 *
 * Returns:
 *  MLGF_STATUS_FOUND
 *  MLGF_STATUS_NOTFOUND
 *  Error code
 *    some erros caused by function calls
 */
Four mlgf_FetchNextFromPathTop(
    Four			handle,
    XactTableEntry_T 		*xactEntry, 		/* IN transaction table entry */
    MLGF_KeyDesc 		*kdesc,			/* IN key descriptor of this index */
    MLGF_HashValue 		*lowerBound, 		/* IN lower bound of region to fetch */
    MLGF_HashValue 		*upperBound, 		/* IN upper bound of region to fetch */
    MLGF_Cursor 		*cursor,		/* INOUT return the position of fetched object */
    mlgf_MortonValue 		*keyMortonVal, 		/* IN morton value for the key */
    char 			*data,			/* OUT return the extra data */
    Boolean 			skipCurrentFlag,	/* IN skip the current entry if TRUE */
    PageID 			*parent,             	/* IN pageID of parent */
    LockParameter 		*lockup)      		/* IN request lock or not */
{
    Four 			e;			/* error code */
    Four 			k;			/* index variable */
    Four 			status;			/* program control state */
    Four 			entryNo;		/* index to entry */
    Four 			entryLen;		/* length of a directory entry */
    PageID 			curPid;			/* current page's ID */
    PageID 			child;			/* PageID of child page */
    Boolean 			found;			/* TRUE when we find something to want */
    MLGF_HashValue 		hashVal;		/* a hash value */
    MLGF_HashValue 		min, max;		/* range of hash values represented by an entry */
    MLGF_HashValue 		*hashVector;		/* starting offset of vector of hash values */
    Buffer_ACC_CB 		*curPage_BCB;		/* buffer control block for current page */
    mlgf_DirectoryPage 		*dirPage; 		/* directory page */
    mlgf_DirectoryEntry 	*dirEntry; 		/* directory entry */
    LockReply 			lockReply;
    LockMode 			oldMode;
    LockParameter 		kLockup; 

    TR_PRINT(handle, TR_MLGF, TR1,
	     ("mlgf_FetchNextFromPathTop(kdesc=%P, lowerBound=%P, upperBound=%P, cursor=%P, keyMortonVal=%P, data=%P, skipCurrentFlag=%ld, parent=%P, lockup=%P)",
	      kdesc, lowerBound, upperBound, cursor, keyMortonVal, data, skipCurrentFlag, parent, lockup));


    /* Get the length of a directory entry. */
    entryLen = MLGF_DIRENTRY_LENGTH(kdesc->nKeys);

    status = MLGF_STATUS_NOTFOUND;

    while (status == MLGF_STATUS_NOTFOUND && !MLGF_CURSOR_PATH_IS_EMPTY(cursor)) {

	/* Get the current page ID. */
	MLGF_CURSOR_PATH_READ_TOP(cursor, curPid);

        if (EQUAL_PAGEID(curPid, *parent)) break; 

	/* get lock on the root page */
	if(lockup) {
	    /* get key range lock first */
	    kLockup.mode = L_IS;
	    kLockup.duration = L_MANUAL;

	    e = LM_getKeyRangeLock(handle, &xactEntry->xactId, &curPid, kLockup.mode,
				  kLockup.duration, L_UNCONDITIONAL, &lockReply);
	    if(e < eNOERROR) ERR(handle, e);

	    if(lockReply == LR_DEADLOCK) ERR(handle, eDEADLOCK);

	    e = LM_getFlatPageLock(handle, &xactEntry->xactId, &curPid, lockup->mode, lockup->duration,
				   L_UNCONDITIONAL, &lockReply, &oldMode); 
	    if (e < eNOERROR) ERR(handle, e);

	    if(lockReply == LR_DEADLOCK){
		ERR(handle, eDEADLOCK);
	    }

	}

	/* Read the current page into the buffer. */
	e = BfM_getAndFixBuffer(handle, &curPid, M_FREE, &curPage_BCB, PAGE_BUF);
	if (e < eNOERROR) ERR(handle, e);

	dirPage = (mlgf_DirectoryPage*)curPage_BCB->bufPagePtr;

	if (IS_NILINDEXID(dirPage->hdr.iid)) { /* This page has been deallocated. */
	    /* Pop the current page. */
	    skipCurrentFlag = FALSE;

	} else {

	    /* Search the direcory page for the entry containing the key. */
	    found = mlgf_SearchDirPageInMortonOrder(handle, dirPage, kdesc, keyMortonVal, FALSE, &entryNo); 

	    if (found && skipCurrentFlag) entryNo++; /* skip current entry */

	    /*
	     * It is not necessary to check the boundary condition(!found && entryNo == 0).
	     * If the entry was moved into the other page, then the original page
	     * must have been deallocated. (There is no redistribution in MLGF and
	     * the page deallocation is deffered until the transaction ends).
	     * Assumption: When a page splits, the original page has the entries
	     *    numbered with the lower number entry no.
	     */
	    for ( ; status == MLGF_STATUS_NOTFOUND && entryNo < dirPage->hdr.nEntries; entryNo++) {

		dirEntry = MLGF_ITH_DIRENTRY(dirPage, entryNo, entryLen);

		hashVector = MLGF_DIRENTRY_HASHVALUEPTR(dirEntry, kdesc->nKeys);

		for (k = 0; k < kdesc->nKeys; k++) {
		    if (MLGF_KEYDESC_IS_MINTYPE(*kdesc,k)) {
			min = hashVector[k];
			max = MLGF_HASHVALUE_SET_EXCEPT_UPPER_N_BITS(dirEntry->nValidBits[k]) | hashVector[k];
		    } else {	/* max type attribute */
			min = MLGF_HASHVALUE_MASK_UPPER_N_BITS(hashVector[k], dirEntry->nValidBits[k]);
			max = hashVector[k];
		    }
		    if (min > upperBound[k] || max < lowerBound[k]) break;
		}

		if (k == kdesc->nKeys) { /* found!!! */

		    MAKE_PAGEID(child, curPid.volNo, dirEntry->spid);

		    if (dirPage->hdr.height == 1) { /* child is a leaf page */
			/* set the leaf page id */
			cursor->leaf = child;

			status = mlgf_FetchNextFromLeaf(handle,
			    xactEntry, kdesc, lowerBound, upperBound, cursor, keyMortonVal,
			    data, FALSE, FALSE, lockup);
			if (status < 0) {
			    ERRB1(handle, status, curPage_BCB, PAGE_BUF);
			}

		    } else {
			/* Push the child page. */
			MLGF_CURSOR_PATH_PUSH(handle, cursor, child);

			status = mlgf_FetchNextFromPathTop(handle,
			    xactEntry, kdesc, lowerBound, upperBound, cursor,
			    keyMortonVal, data, FALSE, &curPid, lockup);
			if (status < 0) {
			    ERRB1(handle, status, curPage_BCB, PAGE_BUF);
			}

		    }
		}
	    }

	    skipCurrentFlag = TRUE;
	}

	/* Retraverse from the parent if the next object was not found. */
	if (status == MLGF_STATUS_NOTFOUND) MLGF_CURSOR_PATH_POP(cursor);

	/* unfix buffer. */
	if (e < eNOERROR) ERRB1(handle, e, curPage_BCB, PAGE_BUF);

	e = BfM_unfixBuffer(handle, curPage_BCB, PAGE_BUF);
	if (e < eNOERROR) ERR(handle, e);

    }

    return(status);

} /* mlgf_FetchNextFromPath() */



/*
 * Function: Four mlgf_FetchNextFromLeaf(Four, MLGF_KeyDesc*, MLGF_HashValue*, MLGF_HashValue*,
 *                    MLGF_Cursor*, mlgf_MortonValue*, char*, Boolean, Boolean, LockParameter*)
 *
 * Description:
 *  Search for the next object from the leaf page given in the cursor's leaf
 *  field.
 *
 * Returns:
 *  MLGF_STATUS_FOUND
 *  MLGF_STATUS_NOTFOUND
 *  Error code
 *    some erros caused by function calls
 */
Four mlgf_FetchNextFromLeaf(
    Four			handle,
    XactTableEntry_T 		*xactEntry, 		/* IN transaction table entry */
    MLGF_KeyDesc 		*kdesc,			/* IN key descriptor of this index */
    MLGF_HashValue 		*lowerBound, 		/* IN lower bound of region to fetch */
    MLGF_HashValue 		*upperBound, 		/* IN upper bound of region to fetch */
    MLGF_Cursor 		*cursor,		/* INOUT return the position of fetched object */
    mlgf_MortonValue 		*keyMortonVal, 		/* IN morton value for the key */
    char 			*data,			/* OUT return the extra data */
    Boolean 			useCursorFlag,		/* IN use cursor information if TRUE */
    Boolean 			skipCurrentFlag,	/* IN skip the current entry if TRUE */
    LockParameter 		*lockup)      		/* IN request lock or not */
{
    Four 			e;			/* error code */
    Four 			k;			/* index variable */
    Four 			objectItemLen;		/* length of an element in object array */
    Four 			entryNo;		/* index to entry */
    Four 			status;			/* program control status */
    Four 			oidArrayElemNo;		/* index to an element in object array */
    char 			*objectItem;		/* points to an element in object array  */
    Boolean 			found;			/* TRUE when we find something to want */
    Boolean 			oidFound;		/* result of searching object array */
    mlgf_LeafPage 		*apage;			/* a leaf page */
    mlgf_LeafEntry 		*entry;			/* a leaf entry */
    Buffer_ACC_CB 		*leaf_BCB;		/* buffer control block for leaf page */
    LockReply 			lockReply;
    LockMode 			oldMode;


    TR_PRINT(handle, TR_MLGF, TR1,
	     ("mlgf_FetchNextFromLeaf(kdesc=%P, lowerBound=%P, upperBound=%P, cursor=%P, data=%P, keyMortonVal=%P, useCursorFlag=%ld, skipCurrentFlag=%ld, lockup=%P)",
	      kdesc, lowerBound, upperBound, cursor, data, keyMortonVal, useCursorFlag, skipCurrentFlag, lockup));

    /* Read the leaf page into the buffer. */
    e = BfM_getAndFixBuffer(handle, &cursor->leaf, M_FREE, &leaf_BCB, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);

    apage = (mlgf_LeafPage*)(leaf_BCB->bufPagePtr);

    if (useCursorFlag && cursor->entryNo < apage->hdr.nEntries &&
	mlgf_EqualKeys(handle, kdesc, (MLGF_ITH_LEAFENTRY(apage, cursor->entryNo))->keys, cursor->keys)) {

	found = TRUE;		/* The current entry was found. */
	entryNo = cursor->entryNo;
	if (skipCurrentFlag) entryNo++; /* skip current entry */

    } else {
	/* Search the leaf page for the entry whose keys are equal to the given keys. */
	found = mlgf_SearchLeafPageInMortonOrder(handle, apage, kdesc, keyMortonVal, &entryNo);
	if (found && skipCurrentFlag) entryNo ++; /* skip current entry */
    }

    /* Get the length of object item in object array. */
    objectItemLen = MLGF_LEAFENTRY_OBJECTITEM_LEN(kdesc->extraDataLen);

    status = MLGF_STATUS_NOTFOUND;

    if (found && !skipCurrentFlag) { /* search the current entry */
	entry = MLGF_ITH_LEAFENTRY(apage, entryNo);

	cursor->entryNo = entryNo;

	if (entry->nObjects >= 0) {	/* normal entry */

	    objectItem = MLGF_LEAFENTRY_ITH_OBJECTITEM(kdesc->nKeys, kdesc->extraDataLen, entry, 0);

	    oidFound = mlgf_BinarySearchObjectArray(handle, objectItem, &cursor->oid,
						    entry->nObjects, objectItemLen,
						    &oidArrayElemNo);
	    if (oidFound)
		oidArrayElemNo++; /* go ahead to get the next object */

	    if (oidArrayElemNo < entry->nObjects) { /* found the next object !!! */
		memcpy((char*)cursor->keys, (char*)entry->keys, sizeof(cursor->keys[0])*kdesc->nKeys);
		objectItem = MLGF_LEAFENTRY_ITH_OBJECTITEM(kdesc->nKeys, kdesc->extraDataLen, entry, oidArrayElemNo);

		SET_NILPAGEID(cursor->overflow);
		cursor->oidArrayElemNo = oidArrayElemNo;
		cursor->oid = *((ObjectID*)objectItem);
		cursor->flag = CURSOR_ON;
		if (data) memcpy(data, objectItem+sizeof(ObjectID), kdesc->extraDataLen); 
		status = MLGF_STATUS_FOUND;
	    }

	} else {	/* overflow entry */

	    MAKE_PAGEID(cursor->overflow, cursor->leaf.volNo,
			MLGF_LEAFENTRY_FIRST_OVERFLOW(kdesc->nKeys, entry));

	    status = mlgf_FetchNextInOverflow(handle, kdesc, cursor, data);
	    if (status < 0) {
		ERRB1(handle, status, leaf_BCB, PAGE_BUF);

	    } else if (status == MLGF_STATUS_FOUND) { 
		memcpy((char*)cursor->keys, (char*)entry->keys, sizeof(cursor->keys[0])*kdesc->nKeys); 
	    }
	}

	entryNo++;
    }

    for ( ; status == MLGF_STATUS_NOTFOUND && entryNo < apage->hdr.nEntries; entryNo++) {

	entry = MLGF_ITH_LEAFENTRY(apage, entryNo);

	/* Are the keys included in the given range? */
	for (k = 0; k < kdesc->nKeys; k++)
	    if (entry->keys[k] < lowerBound[k] || entry->keys[k] > upperBound[k])
		break;

	if (k == kdesc->nKeys) { /* found!!! */

	    cursor->entryNo = entryNo;
	    memcpy((char*)cursor->keys, (char*)entry->keys, sizeof(cursor->keys[0])*kdesc->nKeys);
	    cursor->oidArrayElemNo = 0;
	    cursor->flag = CURSOR_ON;

	    if (entry->nObjects >= 0) {	/* normal entry */

		objectItem = MLGF_LEAFENTRY_ITH_OBJECTITEM(kdesc->nKeys, kdesc->extraDataLen, entry, 0);

		SET_NILPAGEID(cursor->overflow);
		cursor->oid = *((ObjectID*)objectItem);
		if (data) memcpy(data, objectItem+sizeof(ObjectID), kdesc->extraDataLen); 

	    } else {	/* overflow entry */

		MAKE_PAGEID(cursor->overflow, cursor->leaf.volNo,
			    MLGF_LEAFENTRY_FIRST_OVERFLOW(kdesc->nKeys, entry));

		/* Get the ObjectID and its extra data. */
		e = mlgf_GetObjectFromOverflow(handle, &cursor->overflow, kdesc,
					       &cursor->oid, data);
		if (e < 0) {
		    ERRB1(handle, e, leaf_BCB, PAGE_BUF);
		}
	    }

	    status = MLGF_STATUS_FOUND;
	}
    }

    e = BfM_unfixBuffer(handle, leaf_BCB, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);

    return(status);

} /* mlgf_FetchNextFromLeaf() */



/*
 * Module: Four mlgf_FetchNextInOverflow(Four, MLGF_KeyDesc*, MLGF_Cursor*, char*)
 *
 * Description:
 *  Search for the next object the overflow chain from the given overflow page
 *  in ther cursor's overflow field.
 *
 * Returns:
 *  MLGF_STATUS_FOUND if we find the next object
 *  MLGF_STATUS_NOTFOUND otherwise
 *  Error code
 *    some erros caused by function calls
 */
Four mlgf_FetchNextInOverflow(
    Four		handle,
    MLGF_KeyDesc 	*kdesc,			/* IN key descriptor of this index */
    MLGF_Cursor  	*cursor,		/* INOUT return the position of fected object */
    char         	*data)			/* OUT return the extra data */
{
    Four 		e;			/* error code */
    Four 		cmp;			/* result of comparison */
    Four 		objectItemLen;		/* length of an element in object array */
    Four 		oidArrayElemNo;		/* index to an element in object array */
    char 		*objectItem;		/* points to an element in object array */
    Boolean 		found;			/* TRUE when we find something to want */
    Boolean 		oidFound;		/* result of searching object array */
    PageID 		ovPid;			/* PageID of an overflow page */
    Buffer_ACC_CB 	*ov_BCB;		/* buffer control block for overflow page */
    mlgf_OverflowPage 	*opage;			/* an overflow page */


    TR_PRINT(handle, TR_MLGF, TR1,
	     ("mlgf_FetchNextInOverflow(kdesc=%P, cursor=%P, data=%P)",
	      kdesc, cursor, data));


    ovPid = cursor->overflow;

    /* Get the length of one element. */
    objectItemLen = MLGF_LEAFENTRY_OBJECTITEM_LEN(kdesc->extraDataLen);

    found = FALSE;

    do {
	/* Read the current overflow page into the buffer. */
	e = BfM_getAndFixBuffer(handle, &ovPid, M_FREE, &ov_BCB, PAGE_BUF);
	if (e < eNOERROR) ERR(handle, e);

	opage = (mlgf_OverflowPage*)ov_BCB->bufPagePtr;

	cmp = mlgf_ObjectIdComp(handle, &cursor->oid,
				(ObjectID*)MLGF_OVERFLOW_ITH_OBJECTITEM(objectItemLen, opage, opage->hdr.nObjects-1));

	if (cmp == LESS) { /* The next object is in the current page. */
	    oidFound = mlgf_BinarySearchObjectArray(handle, &opage->data[0], &cursor->oid,
						    opage->hdr.nObjects, objectItemLen,
						    &oidArrayElemNo);
	    if (oidFound) oidArrayElemNo++; /* go ahead to get the next object */

	    objectItem = MLGF_OVERFLOW_ITH_OBJECTITEM(objectItemLen, opage, oidArrayElemNo);

	    cursor->overflow = ovPid;
	    cursor->oid = *((ObjectID*)objectItem);
	    cursor->oidArrayElemNo = oidArrayElemNo;
	    cursor->flag = CURSOR_ON;
	    if (data) memcpy(data, objectItem+sizeof(ObjectID), kdesc->extraDataLen); 
	    found = TRUE;

	} else {		/* go to the next overflow page */
	    ovPid.pageNo = opage->hdr.nextPage;
	}

	/* Unfix buffer. */
	e = BfM_unfixBuffer(handle, ov_BCB, PAGE_BUF);
	if (e < eNOERROR) ERR(handle, e);

    } while (!found && ovPid.pageNo != NIL);

    return((found) ? MLGF_STATUS_FOUND:MLGF_STATUS_NOTFOUND);

} /* mlgf_FetchNextInOverflow() */
