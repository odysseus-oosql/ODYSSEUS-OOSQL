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
 * Module: btm_firstObject.c
 *
 * Description :
 *  Find the first ObjectID of the given Btree. The 'cursor' will indicate
 *  the first ObjectID in the Btree, and it will be used as successive access
 *  by using the Btree.
 *
 * Exports:
 *  Four btm_firstObject(PageID*, btm_TraversePath*, BtreeCursor*)
 *
 * Returns:
 *  Error code
 *    eBADPAGE_BTM
 *    eBADBTREEPAGE_BTM
 *    some errors caused by function calls
 *
 * Side effects:
 *  cursor : A position in the Btree which indicates the first ObjectID.
 *             The first object's object identifier is also returned via this.
 */


#include "common.h"
#include "error.h"
#include "trace.h"
#include "latch.h"
#include "LM.h"
#include "BfM.h"
#include "BtM.h"
#include "OM.h"
#include "perProcessDS.h"
#include "perThreadDS.h"


Four btm_FirstObject(
    Four handle,
    XactTableEntry_T *xactEntry, /* IN transaction table entry */
    IndexID *iid,		/* IN index ID */
    FileID *fid,		/* FileID of file containing first object */
    PageID  *root,		/* IN The root of Btree(or subtree) */
    btm_TraversePath *path,	/* INOUT traverse path stack */
    BtreeCursor *cursor,	/* OUT The first ObjectID in the Btree */
    LockParameter *lockup)	/* IN request lock or not */
{
    Four e;			/* error code */
    Four cmp;			/* result of comparison */
    ObjectID oldOid;		/* save the old ObjectID */
    Lsn_T LeafLsn;		/* LSN of the child */
    Boolean eofFlag;		/* TRUE if we reach the end of file of btree */
    Buffer_ACC_CB *leaf_BCB;	/* pointer to buffer control block for child */
    BtreeLeaf *apage;           /* pointer to buffer holding child page */
    LockReply lockResult;	/* result of lock request */
    LockMode oldMode;


    TR_PRINT(handle, TR_BTM, TR1,
	     ("btm_FirstObject(handle, root=%P, path=%P, cursor=%P)", root, path, cursor));


    while (1) {
        /*
         * Search the first leaf page
         * the latch of the top entry(leaf_BCB) is holding
         */
        e = btm_SearchFirstOrLastPage(handle, iid, root, 0, path);
        if (e < eNOERROR) ERR(handle, e);

        /* Get the buffer control block for the leaf. */
        e = btm_PopElemFromPath(handle, path, &leaf_BCB, &LeafLsn);
        if (e < eNOERROR) ERR(handle, e);

        apage = (BtreeLeaf*)leaf_BCB->bufPagePtr;

        if (apage->hdr.nSlots == 0 && apage->hdr.nextPage == NIL) {

            eofFlag = TRUE;

            /* Construct the special ObjectID. */
            cursor->oid.volNo = apage->hdr.iid.volNo;
            cursor->oid.pageNo = apage->hdr.iid.serial;
            cursor->oid.slotNo = -1;
            cursor->oid.unique = 0;

            cursor->flag = CURSOR_EOS;
            cursor->leafLsn = apage->hdr.lsn;

            if (lockup) {
                e = LM_getFlatObjectLock(handle, &xactEntry->xactId, &cursor->oid,
                                         L_S, L_COMMIT, L_CONDITIONAL, &lockResult, &oldMode);
                if (e < eNOERROR) ERRBL1(handle, e, leaf_BCB, PAGE_BUF);
            }

        } else {

            eofFlag = FALSE;

            /* Construct Btree cursor.*/
            e = btm_GetCursorForObjectInSlot(handle, apage, 0, FALSE, cursor);
            if (e < eNOERROR) ERRBL1(handle, e, leaf_BCB, PAGE_BUF);

            if ( lockup ) {
                /* While holding the page latches, request conditional S lock on the found ObjectID. */
                e = LM_getObjectLock(handle, &xactEntry->xactId, &cursor->oid, fid,
                                     L_S, L_COMMIT, L_CONDITIONAL, &lockResult, &oldMode);
                if (e < eNOERROR) ERRBL1(handle, e, leaf_BCB, PAGE_BUF);

            }
        }

        if (!lockup || lockResult != LR_NOTOK) break; /* goto Found */

        while (lockResult == LR_NOTOK) { /* lock wasn't granted. */

            /* The found key and the current page's LSN are remembered in 'cursor'. */

            /* Unlatch holding latches. */
            e = SHM_releaseLatch(handle, leaf_BCB->latchPtr, procIndex);
            if (e < eNOERROR) ERRB1(handle, e, leaf_BCB, PAGE_BUF);

            /* Request unconditional manual duration S lock on the found ObjectID. */
            if (eofFlag)
                e = LM_getFlatObjectLock(handle, &xactEntry->xactId, &cursor->oid,
                                         L_S, L_MANUAL, L_UNCONDITIONAL, &lockResult, &oldMode);
            else
                e = LM_getObjectLock(handle, &xactEntry->xactId, &cursor->oid, fid,
                                     L_S, L_MANUAL, L_UNCONDITIONAL, &lockResult, &oldMode);
            if (e < eNOERROR) ERRB1(handle, e, leaf_BCB, PAGE_BUF);

            if ( lockResult == LR_DEADLOCK )
                ERRB1(handle, eDEADLOCK, leaf_BCB, PAGE_BUF);


            /* Relatch the leaf page which contained the key. */
            e = SHM_getLatch(handle, leaf_BCB->latchPtr, procIndex, M_SHARED, M_UNCONDITIONAL, NULL);
            if (e < eNOERROR) ERRB1(handle, e, leaf_BCB, PAGE_BUF);

            if (!btm_IsCorrectLeaf(handle, apage, NULL, NULL, iid, &LeafLsn) ||
                apage->hdr.prevPage != NIL ||
                (apage->hdr.nSlots == 0 && apage->hdr.nextPage != NIL)) {

                /* Release the holding lock and retraverse from the parent. */
                if (eofFlag)
                    e = LM_releaseFlatObjectLock(handle, &xactEntry->xactId, &cursor->oid, L_MANUAL);
                else
                    e = LM_releaseObjectLock(handle, &xactEntry->xactId, &cursor->oid, L_MANUAL);
                if (e < eNOERROR) ERRBL1(handle, e, leaf_BCB, PAGE_BUF);

                /* Retraverse from the parent(or its ancedents). */
                break;          /* goto WaitSMOandRetraverseFromParent; */


            }

            if (LSN_CMP_EQ(cursor->leafLsn, apage->hdr.lsn)
                && cursor->overflow.pageNo == NIL) {

                if (eofFlag) {
                    /* extend the lock duration */
                    e = LM_getFlatObjectLock(handle, &xactEntry->xactId, &cursor->oid,
                                             L_S, L_COMMIT, L_UNCONDITIONAL, &lockResult, &oldMode);
                    if (e < eNOERROR) ERRBL1(handle, e, leaf_BCB, PAGE_BUF);
                }
                else {
                    /* Requests the unconditional commit duration S lock. */
                    e = LM_getObjectLock(handle, &xactEntry->xactId, &cursor->oid, fid, 
                                         L_S, L_COMMIT, L_UNCONDITIONAL, &lockResult, &oldMode);
                    if (e < eNOERROR) ERRBL1(handle, e, leaf_BCB, PAGE_BUF);

                }
                goto Found;
            }

            if ( apage->hdr.nSlots == 0 && apage->hdr.nextPage == NIL ) {
                if (eofFlag) {
                    /* extend the lock duration */
                    e = LM_getFlatObjectLock(handle, &xactEntry->xactId, &cursor->oid,
                                             L_S, L_COMMIT, L_UNCONDITIONAL, &lockResult, &oldMode);
                    if (e < eNOERROR) ERRBL1(handle, e, leaf_BCB, PAGE_BUF);

                    goto Found;
                }
                else {

                    /* during the lock request, all objects are deleted */
                    e = LM_releaseObjectLock(handle, &xactEntry->xactId, &cursor->oid, L_MANUAL);
                    if (e < eNOERROR) ERRBL1(handle, e, leaf_BCB, PAGE_BUF);

                    eofFlag = TRUE;

                    cursor->oid.volNo = apage->hdr.iid.volNo;
                    cursor->oid.pageNo = apage->hdr.iid.serial;
                    cursor->oid.slotNo = -1;
                    cursor->oid.unique = 0;

                    cursor->flag = CURSOR_EOS;

                    e = LM_getFlatObjectLock(handle, &xactEntry->xactId, &cursor->oid,
                                             L_S, L_COMMIT, L_CONDITIONAL, &lockResult, &oldMode);
                    if (e < eNOERROR) ERRBL1(handle, e, leaf_BCB, PAGE_BUF);

                    continue;
                }

            }


            /* Construct Btree cursor.*/
            e = btm_GetCursorForObjectInSlot(handle, apage, 0, FALSE, cursor);
            if (e < eNOERROR) ERRBL1(handle, e, leaf_BCB, PAGE_BUF);


            cmp = btm_ObjectIdComp(handle, &oldOid, &cursor->oid);
            if (cmp == EQUAL) {
                /* Requests the unconditional commit duration S lock. */
                e = LM_getObjectLock(handle, &xactEntry->xactId, &cursor->oid, fid,
                                     L_S, L_COMMIT, L_UNCONDITIONAL, &lockResult, &oldMode);
                if (e < eNOERROR) ERRBL1(handle, e, leaf_BCB, PAGE_BUF);
                /* We locked the first object. */
                goto Found;
            }

            if (eofFlag) {
                eofFlag = FALSE; 
                e = LM_releaseFlatObjectLock(handle, &xactEntry->xactId, &cursor->oid, L_MANUAL);
            }
            else
                e = LM_releaseObjectLock(handle, &xactEntry->xactId, &cursor->oid, L_MANUAL);
            if (e < eNOERROR) ERRBL1(handle, e, leaf_BCB, PAGE_BUF);

            /* Request the conditional commit duration lock on the new ObjectID. */
            e = LM_getObjectLock(handle, &xactEntry->xactId, &cursor->oid, fid,
                                 L_S, L_MANUAL, L_CONDITIONAL, &lockResult, &oldMode);
            if (e < eNOERROR) ERRBL1(handle, e, leaf_BCB, PAGE_BUF);

        } /* while lockResult == LR_NOTOK */


        /* WaitSMOandRetraversePath:
         * RETRAVERSE the tree from the ancedents.
         * Unlatch the child.
         */
        e = SHM_releaseLatch(handle, leaf_BCB->latchPtr, procIndex);
        if (e < eNOERROR) ERRB1(handle, e, leaf_BCB, PAGE_BUF);

        /* Unfix the buffer for the child. */
        e = BfM_unfixBuffer(handle, leaf_BCB, PAGE_BUF);
        if (e < eNOERROR) ERR(handle, e);

        /* Request S latch on the tree to progress the page deletion . */
        e = btm_GetTreeLatchInPath(handle, path, M_SHARED, M_UNCONDITIONAL|M_INSTANT);
        if (e < eNOERROR) ERR(handle, e);

    } /* while (1) */


  Found:
    e = SHM_releaseLatch(handle, leaf_BCB->latchPtr, procIndex);
    if (e < eNOERROR) ERRB1(handle, e, leaf_BCB, PAGE_BUF);

    /* Unfix the buffer for the child. */
    e = BfM_unfixBuffer(handle, leaf_BCB, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);

    return(eNOERROR);


} /* btm_FirstObject() */





Four btm_SearchFirstOrLastPage(
    Four handle,
    IndexID  *iid,		/* IN index ID */
    PageID   *root,		/* IN PageID of root page of the Btree */
    Boolean  isLast,            /* IN is Last?  */
    btm_TraversePath *path)	/* INOUT traverse path stack */

{
    Four e;			/* error code */
    PageID parent;		/* PageID of the parent */
    PageID child;		/* PageID of the child page */
    Lsn_T parentLsn;		/* LSN of the parent */
    Buffer_ACC_CB *child_BCB;	/* pointer to buffer control block for child */
    Buffer_ACC_CB *parent_BCB;	/* pointer to buffer control block for parent */
    BtreePage *apage;		/* pointer to buffer holding child page */
    btm_InternalEntry *iEntry;	/* pointer to an internal entry */

    TR_PRINT(handle, TR_BTM, TR1,("btm_SearchFirst(root=%P, path=%P)", root, path));


    while (1) {

        /* RetraveralFromParent */
        /* If the path is empty then traverse from the root. Otherwise traverse */
        /* from the top page of the traverse path stack. */
        for (; !btm_IsEmptyPath(handle, path); ) {

            e = btm_ReadTopElemFromPath(handle, path, &parent_BCB, &parentLsn);
            if (e < eNOERROR) ERR(handle, e);

            e = SHM_getLatch(handle, parent_BCB->latchPtr, procIndex, M_SHARED, M_UNCONDITIONAL, NULL);
            if (e < eNOERROR) ERR(handle, e);

            apage = (BtreePage*)parent_BCB->bufPagePtr;

            /* The page isn't touched since we got the parent. */
            if (btm_IsCorrectInternal(handle, &apage->bi, NULL, NULL, iid, &parentLsn)) { 

                parent = parent_BCB->key;
                goto Decend;
            }

            e = SHM_releaseLatch(handle, parent_BCB->latchPtr, procIndex);
            if (e < eNOERROR) ERR(handle, e);

            e = BfM_unfixBuffer(handle, parent_BCB, PAGE_BUF);
            if (e < eNOERROR) ERR(handle, e);

            e = btm_PopElemFromPath(handle, path, &parent_BCB, &parentLsn);
            if (e < eNOERROR) ERR(handle, e);

        }

        child = *root;
        MAKE_PAGEID(parent, root->volNo, NIL);
        parent_BCB = NULL;

        for ( ; ; ) {

            /* Get a buffer and latch the child in S mode. */
            e = BfM_getAndFixBuffer(handle, &child, M_SHARED, &child_BCB, PAGE_BUF);
            if (e < eNOERROR) ERRGOTO(handle, e, ErrorHandle);

            if (parent_BCB != NULL) { /* Unlatch parent */
                e = SHM_releaseLatch(handle, parent_BCB->latchPtr, procIndex);
                if (e < eNOERROR) ERRBL1(handle, e, child_BCB, PAGE_BUF);
            }

            apage = (BtreePage*)child_BCB->bufPagePtr;

            if (apage->any.hdr.type & INTERNAL) {
                /* 'child' is a nonleaf node. */

                /* check correctness of the page */
                if (apage->bi.hdr.nSlots == 0 && (apage->bi.hdr.statusBits & BTM_SM_BIT)) {

                    goto WaitSMOandRetraversal;
                }

                /* Push the current child into the traverse path stack. */
                e = btm_PushElemIntoPath(handle, path, child_BCB, &apage->any.hdr.lsn);
                if (e < eNOERROR) ERRBL1(handle, e, child_BCB, PAGE_BUF);

                parent = child;
                parent_BCB = child_BCB;

              Decend:
                if (isLast) {
                    /* traverse to find the last leaf page */
                    iEntry = (btm_InternalEntry*)&apage->bi.data[apage->bi.slot[-(apage->bi.hdr.nSlots-1)]];
                    MAKE_PAGEID(child, parent.volNo, iEntry->spid);
                }
                else
                    /* traverse to find the first leaf page */
                    MAKE_PAGEID(child, parent.volNo, apage->bi.hdr.p0);
                continue;

            }
            else {		/* The child page is the leaf page. */

                if ((apage->bl.hdr.nSlots == 0 && apage->bl.hdr.statusBits & BTM_SM_BIT) ||
                    (isLast && apage->bl.hdr.nextPage != NIL) ||
                    (!isLast && apage->bl.hdr.prevPage != NIL))
                    goto WaitSMOandRetraversal;

                /* Push the current child into the traverse path stack. */
                e = btm_PushElemIntoPath(handle, path, child_BCB, &apage->any.hdr.lsn);
                if (e < eNOERROR) ERRBL1(handle, e, child_BCB, PAGE_BUF);

                return(eNOERROR);


            }

        }	/* for loop */

      WaitSMOandRetraversal:
        /* Unlatch the child. */
        e = SHM_releaseLatch(handle, child_BCB->latchPtr, procIndex);
        if (e < eNOERROR) ERRB1(handle, e, child_BCB, PAGE_BUF);

        /* Unfix the buffer for the child. */
        e = BfM_unfixBuffer(handle, child_BCB, PAGE_BUF);
        if (e < eNOERROR) ERR(handle, e);

        /* Request S latch on the tree to progress the page deletion . */
        e = btm_GetTreeLatchInPath(handle, path, M_SHARED, M_UNCONDITIONAL|M_INSTANT);
        if (e < eNOERROR) ERR(handle, e);

    }

  ErrorHandle:
    if (parent_BCB)
        ERRBL1(handle, e, parent_BCB, PAGE_BUF);
    else
        ERR(handle, e);

} /* btm_SearchFirst() */



