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
 * Module: BtM_Fetch.c
 *
 * Description :
 *  Find an object identifier satisfying the given condition.
 *  If there is no such object, then return with 'flag' field of cursor set
 *  to CURSOR_EOS. If there is an object satisfying the condition, then cursor
 *  points to the object position in the B+ tree and the object identifier
 *  is returned via 'oid' parameter.
 *  The condition is given with a key value and a comparison operator;
 *  the comparison operator is one among SM_EQ, SM_LT, SM_LE, SM_GT, SM_GE.
 *
 * Exports:
 *  Four BtM_Fetch(Four, PageID*, KeyDesc*, KeyValue*, Four, BtreeCursor*, LockParameter*)
 *
 * Returns:
 *  Error code
 *    eBADCOMPOP_BTM
 *    eBADPARAMETER_BTM
 *    some errors caused by function calls
 *
 * Side effects:
 *  cursor  : The found ObjectID and its position in the Btree Leaf
 *            (it may indicate a ObjectID in an  overflow page).
 */


#include "common.h"
#include "error.h"
#include "trace.h"
#include "latch.h"
#include "BfM.h"
#include "BtM.h"
#include "perProcessDS.h"
#include "perThreadDS.h"


Four BtM_Fetch(
    Four handle,
    XactTableEntry_T *xactEntry, /* IN transaction table entry */
    BtreeIndexInfo *iinfo,	/* IN B tree index information */ 
    FileID     *fid,            /* IN FileID */ 
    KeyDesc  *kdesc,		/* IN Btree key descriptor */
    KeyValue *startKval,	/* IN start key value */
    Four     startCompOp,	/* IN comparison operator of start condition */
    KeyValue *stopKval,		/* IN stop key value */
    Four     stopCompOp,	/* IN comparison operator of stop condition */
    BtreeCursor *cursor,	/* OUT Btree Cursor */
    PageID *parentPid,          /* IN parent page */
    LockParameter *lockup)	/* IN request lock or nolock */
{
    Four e;		   /* error number */
    Four cmp;		   /* result of key comparison */
    btm_TraversePath path;     /* a Btree Traverse Path Stack */
    LATCH_TYPE *treeLatchPtr;
    PageID root; 


    TR_PRINT(handle, TR_BTM, TR1,
	     ("BtM_Fetch(handle, iinfo=%P, kdesc=%P, startKval=%P, startCompOp=%ld, stopKval=%P, stopCompOp=%P, lockup=%P, cursor=%P)",
	      iinfo, kdesc, startKval, startCompOp, stopKval, stopCompOp, lockup, cursor));


    /* Check the parameters. */
    if (iinfo == NULL || kdesc == NULL || startKval == NULL || stopKval == NULL || cursor == NULL ||
	(startCompOp != SM_EQ && startCompOp != SM_LT && startCompOp != SM_LE &&
	 startCompOp != SM_GT && startCompOp != SM_GE && startCompOp != SM_BOF && startCompOp != SM_EOF) ||
	(stopCompOp != SM_EQ && stopCompOp != SM_LT && stopCompOp != SM_LE &&
	 stopCompOp != SM_GT && stopCompOp != SM_GE && stopCompOp != SM_BOF && stopCompOp != SM_EOF))
	ERR(handle, eBADPARAMETER);


    /* Get root's page ID of the current index */
    e = btm_GetRootPid(handle, xactEntry, iinfo, &root, lockup);
    if (e < eNOERROR) ERR(handle, e);

    /* Get TreeLatchPtr of the current index */
    e = BtM_GetTreeLatchPtrFromIndexId(handle, &iinfo->iid, &treeLatchPtr); 
    if (e < eNOERROR) ERR(handle, e);

    /* Initialize the traverse path stack. */
    btm_InitPath(handle, &path, treeLatchPtr);

    if (startCompOp == SM_BOF) {
	/* Find the first object which has the smallest key value. */

	e = btm_FirstObject(handle, xactEntry, &iinfo->iid, fid, &root, &path, cursor, lockup); 
	if (e < eNOERROR) ERRPATH(handle, e, &path);

    } else if (startCompOp == SM_EOF) {
	/* Find the last object which has the largest key value. */

	e = btm_LastObject(handle, xactEntry, &iinfo->iid, fid, &root, &path, cursor, lockup); 
	if (e < eNOERROR) ERRPATH(handle, e, &path);

    } else {
	for (;;) {

	    /* Get the traverse path. */
	    e = btm_Search(handle, &iinfo->iid, &root, kdesc, startKval, BTM_FETCH, 0, &path); 
	    if (e < eNOERROR) ERRPATH(handle, e, &path);

	    if (startCompOp == SM_EQ || startCompOp == SM_GT || startCompOp == SM_GE)
		e = btm_FetchForward(handle, xactEntry, fid, &path, kdesc, startKval, startCompOp, cursor, lockup);
	    else	/* startCompOp == SM_LT or startCompOp == SM_LE */
		e = btm_FetchBackward(handle, xactEntry, fid, &path, kdesc, startKval, startCompOp, cursor, lockup);

	    if (e < eNOERROR) ERRPATH(handle, e, &path);

	    if (e == eNOERROR) break;

	    /* Retraverse the tree. */
	}
    }

    /* Finalize the traverse path stack. */
    e = btm_FinalPath(handle, &path);
    if (e < eNOERROR) ERR(handle, e);

    /* Check the stop condition. */
    if (cursor->flag == CURSOR_ON && stopCompOp != SM_EOF && stopCompOp != SM_BOF) {
	cmp = btm_KeyCompare(handle, kdesc, &cursor->key, stopKval);

	switch(stopCompOp) {
	  case SM_EQ:
	    if (cmp != EQUAL) cursor->flag = CURSOR_EOS;
	    break;

	  case SM_LT:
	    if (cmp != LESS) cursor->flag = CURSOR_EOS;
	    break;

	  case SM_LE:
	    if (cmp == GREAT) cursor->flag = CURSOR_EOS;
	    break;

	  case SM_GT:
	    if (cmp != GREAT) cursor->flag = CURSOR_EOS;
	    break;

	  case SM_GE:
	    if (cmp == LESS) cursor->flag = CURSOR_EOS;
	    break;
	}
    }

    return(eNOERROR);

} /* BtM_Fetch() */





