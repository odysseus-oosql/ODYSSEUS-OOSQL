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
 * Module: MLGF_Fetch.c
 *
 * Description:
 *  Find an object in the given range and returns its ObjectID and extra
 *  data, if any. If found, the cursor points to the found object and
 *  cursor's flag is set to CURSOR_ON.
 *  If there is no object in the given range, then the cursor's flag is set
 *  to CURSOR_EOS;
 *
 * Exports:
 *  Four MLGF_Fetch(Four, IndexID*, MLGF_KeyDesc*, MLGF_HashValue[],
 *                  MLGF_HashValue[], MLGF_Cursor*, char*, LockParameter*)
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
Four mlgf_Fetch(Four, XactTableEntry_T*, PageID*, MLGF_KeyDesc*, MLGF_HashValue[],
                MLGF_HashValue[], MLGF_Cursor*, char*, PageID*, LockParameter*, LockParameter*);


/*
 * Function: MLGF_Fetch(handle, IndexID*, MLGF_KeyDesc*, MLGF_HashValue[],
 *                      MLGF_HashValue[], MLGF_Cursor*, char*, LockParameter*)
 *
 * Description:
 *  Call mlgf_Fetch() to search an object in the given range.
 *
 * Returns:
 *  Error code
 *    eBADPARAMETER
 *    some errors caused by function calls
 */
Four MLGF_Fetch(
    Four                        handle,                 /* IN handle */
    XactTableEntry_T 		*xactEntry, 		/* IN transaction table entry */
    MLGFIndexInfo		*iinfo,			/* IN MLGF Index Info */
    MLGF_KeyDesc 		*kdesc,			/* IN key descriptor of this index */
    MLGF_HashValue 		*lowerBound,		/* IN lower bound of region to fetch */
    MLGF_HashValue 		*upperBound,		/* IN upper bound of region to fetch */
    MLGF_Cursor 		*cursor,		/* OUT return the position of fetched object */
    char 			*data,			/* OUT return the extra data */
    LockParameter 		*lockup)      		/* IN request lock or not */
{
    Four 			e, i;			/* error code */
    LockReply 			lockReply;
    LockParameter 		keyRangeLockup; 
    LockMode 			oldMode, kLockMode; 
    PageID 			rootPid;		/* PageID of the root page */


    TR_PRINT(handle, TR_MLGF, TR1, ("MLGF_Fetch(xactEntry=%P, iinfo=%P, kdesc=%P, lowerBound=%P, upperBound=%P, cursor=%P, data=%P, lockup=%P)", xactEntry, iinfo, kdesc, lowerBound, upperBound, cursor, data, lockup));


    if (iinfo == NULL || kdesc == NULL || lowerBound == NULL || upperBound == NULL || 
	cursor == NULL)
	ERR(handle, eBADPARAMETER);

    /* Get 'rootPid' from MLGF index info */
    e = mlgf_GetRootPid(handle, xactEntry, iinfo, &rootPid, lockup);
    if (e < eNOERROR) ERR(handle, e);

    if(lockup){
    	keyRangeLockup.mode = L_IS;

    	for(i = 0; i< kdesc->nKeys; i++){
	    if(lowerBound[i] != 0 || upperBound[i] != MLGF_HASHVALUE_ALL_BITS_SET){
		keyRangeLockup.mode = L_S;
		break;
	    }
    	}
        keyRangeLockup.duration = L_MANUAL;
    }

    MLGF_CURSOR_PATH_INIT(cursor);	/* initialize the path */
    e = mlgf_Fetch(handle, xactEntry, &rootPid, kdesc, lowerBound, upperBound, cursor, data, NULL, lockup, &keyRangeLockup);
    if (e < eNOERROR) ERR(handle, e);

    if (e == MLGF_STATUS_NOTFOUND) cursor->flag = CURSOR_EOS;

    return(eNOERROR);

} /* MLGF_Fetch( ) */


/*
 * Function: mlgf_Fetch(PageID*, MLGF_KeyDesc*, MLGF_HashValue[],
 *                      MLGF_HashValue[], MLGF_Cursor*, char*, PageID*)
 *
 * Description;
 *  Recursive function that traverse the MLGF tree to search an object
 *  in the given range.
 *
 * Returns:
 *  MLGF_STATUS_NOTFOUND
 *  Error codes
 *    some errors caused by fucntion calls
 */
Four mlgf_Fetch(
    Four	     		handle,
    XactTableEntry_T 		*xactEntry, 		/* IN transaction table entry */
    PageID 			*root,			/* IN PageID of the root page */
    MLGF_KeyDesc 		*kdesc,			/* IN key descriptor of this index */
    MLGF_HashValue 		*lowerBound,		/* IN lower bound of region to fetch */
    MLGF_HashValue 		*upperBound,		/* IN upper bound of region to fetch */
    MLGF_Cursor 		*cursor,		/* INOUT return the position of fetched object */
    char 			*data,			/* OUT return the extra data */
    PageID 			*parent,             	/* IN pageID of parent */
    LockParameter 		*lockup,      		/* IN request lock or not */
    LockParameter 		*kLockup)     		/* IN request keyRangeLock or not */
{
    Four 			e;			/* error code */
    Four 			i, k;			/* index variable */
    Four 			entryLen;		/* length of a directory entry */
    char 			*objectItem;		/* starting point of object item of object list */
    PageID 			child;			/* PageID of the child page */
    Boolean 			found;			/* TRUE when we find something to want */
    mlgf_Page 			*apage;			/* an MLGF page */
    mlgf_LeafEntry 		*leafEntry;		/* a leaf entry */
    MLGF_HashValue 		hashVal;		/* hash value */
    MLGF_HashValue 		min, max;		/* range of hash values represented by an entry */
    MLGF_HashValue 		*hashVector;		/* vector of hash values */
    Buffer_ACC_CB 		*root_BCB;		/* buffer control block for root (of subtree) */
    mlgf_DirectoryEntry 	*dirEntry; 		/* a directory entry */
    LockReply 			lockReply;
    LockMode 			oldMode;
    Boolean 			isLeafNode = FALSE; 

    TR_PRINT(handle, TR_MLGF, TR1,
	     ("mlgf_Fetch(root=%P, kdesc=%P, lowerBound=%P, upperBound=%P, cursor=%P, data=%P, parent=%P)",
	      root, kdesc, lowerBound, upperBound, cursor, data, parent));

    /* get lock on the root page */
    if(lockup) {

	/* get key range lock first */
	e = LM_getKeyRangeLock(handle, &xactEntry->xactId, root, kLockup->mode, kLockup->duration,
			      L_UNCONDITIONAL, &lockReply);
	if (e < eNOERROR) ERR(handle, e);

	if(lockReply == LR_DEADLOCK) ERR(handle, eDEADLOCK);

	e = LM_getFlatPageLock(handle, &xactEntry->xactId, root, lockup->mode, L_MANUAL,
			       L_UNCONDITIONAL, &lockReply, &oldMode); 
	if (e < eNOERROR) ERR(handle, e);

	if(lockReply == LR_DEADLOCK){
	    ERR(handle, eDEADLOCK);
	}
    }

    /* Read the root page into the buffer. */
    e = BfM_getAndFixBuffer(handle, root, M_FREE, &root_BCB, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);

    apage = (mlgf_Page*)root_BCB->bufPagePtr;

    if (apage->any.hdr.type & MLGF_LEAFPAGE) { /* leaf page */

	/* upgrade lock mode */
        if(lockup && kLockup->mode == L_IS) {
	    e = LM_getKeyRangeLock(handle, &xactEntry->xactId, root, L_S, L_MANUAL,
	    		           L_UNCONDITIONAL, &lockReply);
	    if (e < eNOERROR) ERRB1(handle, e, root_BCB, PAGE_BUF);

	    if(lockReply == LR_DEADLOCK){
	        ERRB1(handle, eDEADLOCK, root_BCB, PAGE_BUF);
	    }
	}

	isLeafNode = TRUE; 

	/* find the first object in the given range. */
	for (i = 0, found = FALSE; i < apage->leaf.hdr.nEntries && !found; i++) {
	    leafEntry = MLGF_ITH_LEAFENTRY(&apage->leaf, i);

	    for (k = 0; k < kdesc->nKeys; k++)
		if (leafEntry->keys[k] < lowerBound[k] || leafEntry->keys[k] > upperBound[k])
		    break;

	    if (k == kdesc->nKeys) { /* found!!! */
		cursor->leaf = *root;
		cursor->entryNo = i;
		memcpy((char*)cursor->keys, (char*)leafEntry->keys, sizeof(cursor->keys[0])*kdesc->nKeys); 
		cursor->oidArrayElemNo = 0;
		cursor->flag = CURSOR_ON;

		if (leafEntry->nObjects >= 0) {	/* normal entry */

		    objectItem = MLGF_LEAFENTRY_ITH_OBJECTITEM(kdesc->nKeys, kdesc->extraDataLen, leafEntry, 0);

		    SET_NILPAGEID(cursor->overflow);
		    cursor->oid = *((ObjectID*)objectItem);
		    if (data) memcpy(data, objectItem+sizeof(ObjectID), kdesc->extraDataLen); 

		} else {	/* overflow entry */

		    MAKE_PAGEID(cursor->overflow, root->volNo,
				MLGF_LEAFENTRY_FIRST_OVERFLOW(kdesc->nKeys, leafEntry));

		    /* Get the ObjectID and its extra data. */
		    e = mlgf_GetObjectFromOverflow(handle, &cursor->overflow, kdesc,
						   &cursor->oid, data);
		    if (e < 0) {
			ERRB1(handle, e, root_BCB, PAGE_BUF);
		    }
		}

		found = TRUE;
	    }
	}

    } else {			/* direcotry page */

	/* Get the length of a directory entry. */
	entryLen = MLGF_DIRENTRY_LENGTH(kdesc->nKeys);

	/* find the first entry in the given range. */
	dirEntry = MLGF_ITH_DIRENTRY(&apage->directory, 0, entryLen);
	for (i = 0, found = FALSE; i < apage->directory.hdr.nEntries && !found; i++) {
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

		/* Push the current node into the path stack. */
		MLGF_CURSOR_PATH_PUSH(handle, cursor, *root);

		/* Traverse the tree to leaf node recursively. */
		MAKE_PAGEID(child, root->volNo, dirEntry->spid);

		/* get lock mode of the child node */
		if(lockup){
		    if(mlgf_IsContained(handle, kdesc, lowerBound, upperBound, dirEntry))
			kLockup->mode = L_S;
		    else kLockup->mode = L_IS;
		}

		e = mlgf_Fetch(handle, xactEntry, &child, kdesc, lowerBound, upperBound, cursor, data, root, lockup, kLockup); 
		if (e < 0) {
		    ERRB1(handle, e, root_BCB, PAGE_BUF);
		}

		if (e == MLGF_STATUS_NOTFOUND) /* pop the current node */
		    MLGF_CURSOR_PATH_POP(cursor);
		else /* eNOERROR */
		    found = TRUE;
	    }

	    dirEntry = MLGF_NEXT_DIRENTRY(dirEntry, entryLen);
	}
    }


    /* unfix buffer. */
    e = BfM_unfixBuffer(handle, root_BCB, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);

    if(lockup && isLeafNode){ 
	e = LM_releaseFlatPageLock(handle, &xactEntry->xactId, root, L_MANUAL);
	if (e < eNOERROR) ERR(handle, e);

    }

    return((found) ? eNOERROR:MLGF_STATUS_NOTFOUND);

} /* mlgf_Fetch() */
