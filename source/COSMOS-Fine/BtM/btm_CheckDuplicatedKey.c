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
 * Module: btm_CheckDuplicatedKey.c
 *
 * Description:
 *  Check if there is an object with same key as the inserted object.
 *
 *  CAUTION :: Calling routine must acquire the latch of child_BCB.
 *             And this routine keeps the latch of child_BCB.
 *
 * Exports:
 *  Four btm_CheckDuplicatedKey( )
 *
 * Returns:
 *  eNOERRROR
 *  BTM_RETRAVERSE
 *  Error code
 *    eDUPLICATEDKEY_BTM
 *    some errors caused by function calls
 */


#include "common.h"
#include "error.h"
#include "trace.h"
#include "LM.h"
#include "OM.h"
#include "BfM.h"
#include "BtM.h"
#include "perProcessDS.h"
#include "perThreadDS.h"


Four btm_CheckDuplicatedKey(
    Four 		handle,
    XactTableEntry_T 	*xactEntry, 		/* IN transaction table entry */
    FileID 		*fid,			/* FileID of the ObjectID */ 
    Buffer_ACC_CB 	*leaf_BCB,		/* IN buffer control block for leaf */
    KeyDesc 		*kdesc,			/* IN key descriptor */
    KeyValue 		*kval,			/* IN key value */
    LockParameter 	*lockup       		/* IN lock parameter */
)
{
    Four 		e;			/* error code */
    Four 		slotNo;			/* slot no */
    IndexID 		iid;			/* IndexID of the leaf page */
    Boolean 		found;			/* TRUE if the given key exist */
    Boolean 		latchRelease;		/* TRUE if latch on leaf is released */
    LockReply 		lockResult;		/* result of lock request */
    BtreeLeaf 		*apage;			/* IN a Btree leaf page */
    ObjectID 		dupOid;			/* variable for testing ObjectID duplication */
    ObjectID 		dupOid2;		/* variable for testing ObjectID duplication */
    Lsn_T 		leaf_LSN;		/* remember the LSN of the leaf */
    LockMode 		oldMode;


    TR_PRINT(handle, TR_BTM, TR1,
	     ("btm_CheckDuplicatedKey(handle, leaf_BCB=%P, kdesc=%P, kval=%P)",
	      leaf_BCB, kdesc, kval));


    /* Initialize the variable latchRelease. */
    latchRelease = FALSE;

    apage = (BtreeLeaf *)leaf_BCB->bufPagePtr;

    /* Search the leaf to find the insert position. */
    found = btm_BinarySearchLeaf(handle, apage, kdesc, kval, &slotNo);

    if (!found) return(eNOERROR);

    /* found == TRUE */
    if (!lockup) ERRL1(handle, eDUPLICATEDKEY_BTM, leaf_BCB->latchPtr);


    /* Duplicate Key Error */
    /* Get the found key's ObjectID; the ObjectID is only one because */
    /* this index has the unique key. */
    e = btm_GetObjectId(handle, apage, slotNo, &dupOid, NULL);
    if (e < eNOERROR) ERRL1(handle, e, leaf_BCB->latchPtr);

    for ( ; ; ) {

	/* Request a conditional S lock on the found key's ObjectID */
	/* to make sure that the key value is in the committed state. */
	e = LM_getObjectLock(handle, &xactEntry->xactId, &dupOid, fid, L_S, L_COMMIT, L_CONDITIONAL, &lockResult, &oldMode); 
        if (e < eNOERROR) ERRL1(handle, e, leaf_BCB->latchPtr);

	if (lockResult != LR_NOTOK) {
	    /* It is a committed insert or */
	    /* an uncommitted insert of the same transaction. */

            ERRL1(handle, eDUPLICATEDKEY_BTM, leaf_BCB->latchPtr);
	}

	/* lockResult == LR_NOTOK */

	/* Remember the leaf page's LSN. */
	leaf_LSN = apage->hdr.lsn;
	iid = apage->hdr.iid;

	/* Unlatch the pages. */
	e = SHM_releaseLatch(handle, leaf_BCB->latchPtr, procIndex);
        if (e < eNOERROR) ERR(handle, e);

	/* Latch was released. */
	latchRelease = TRUE;

	/* Request an unconditional, S lock on the found key's ObjectID. */
	e = LM_getObjectLock(handle, &xactEntry->xactId, &dupOid, fid, L_S, L_MANUAL, L_UNCONDITIONAL, &lockResult, &oldMode); 
        if (e < eNOERROR) ERR(handle, e);

	if ( lockResult == LR_DEADLOCK )
            ERR(handle, eDEADLOCK);

	/* Relatch the leaf page. */
	e = SHM_getLatch(handle, leaf_BCB->latchPtr, procIndex, M_EXCLUSIVE, M_UNCONDITIONAL, NULL);
        if (e < eNOERROR) ERR(handle, e);


	if (!btm_IsCorrectLeaf(handle, apage, kdesc, kval, &iid, &leaf_LSN)) {
	    e = LM_releaseObjectLock(handle, &xactEntry->xactId, &dupOid, L_MANUAL);
            if (e < eNOERROR) ERRL1(handle, e, leaf_BCB->latchPtr);

            e = SHM_releaseLatch(handle, leaf_BCB->latchPtr, procIndex);
            if (e < eNOERROR) ERR(handle, e);

	    return(BTM_RETRAVERSE);
	}

	/* Search the leaf again to find the previously found key. */
	found = btm_BinarySearchLeaf(handle, apage, kdesc, kval, &slotNo);

	if (!found) {
	    e = LM_releaseObjectLock(handle, &xactEntry->xactId, &dupOid, L_MANUAL);
            if (e < eNOERROR) ERRL1(handle, e, leaf_BCB->latchPtr);
	    break;
	}

	/* Fetch the ObjectID whose key value is duplicated. */
	e = btm_GetObjectId(handle, apage, slotNo, &dupOid2, NULL);
        if (e < eNOERROR) ERRL1(handle, e, leaf_BCB->latchPtr);

	if (btm_ObjectIdComp(handle, &dupOid, &dupOid2) == EQUAL) {
	    /* Request an unconditional, S lock on the found key's ObjectID. */
	    e = LM_getObjectLock(handle, &xactEntry->xactId, &dupOid, fid, 
				 L_S, L_COMMIT, L_UNCONDITIONAL, &lockResult, &oldMode);
            if (e < eNOERROR) ERRL1(handle, e, leaf_BCB->latchPtr);

            ERRL1(handle, eDUPLICATEDKEY_BTM, leaf_BCB->latchPtr);
	}

	e = LM_releaseObjectLock(handle, &xactEntry->xactId, &dupOid, L_MANUAL);
	if (e < eNOERROR) ERRL1(handle, e, leaf_BCB->latchPtr);

	dupOid = dupOid2;
    }

    return((latchRelease == TRUE) ? BTM_RELATCHED : eNOERROR);

} /* btm_CheckDuplicatedKey() */







