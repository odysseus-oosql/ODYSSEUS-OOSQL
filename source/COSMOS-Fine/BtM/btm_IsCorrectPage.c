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
 * Module: btm_IsCorrectPage.c
 *
 * Description:
 *  Check if the given page is the correct btree page. The given page is the
 *  relatched page after releasing its latch for avoiding deadlocks.
 *
 * Exports:
 *  Boolean_btm_IsCorrectInternal(handle, BtreeInternal*, KeyDesc*, KeyValue*, IndexID*, LSN*)
 *  Boolean_btm_IsCorrectLeaf(handle, BtreeLeaf*, KeyDesc*, KeyValue*, IndexID*, LSN*)
 *
 */


#include "common.h"
#include "error.h"
#include "trace.h"
#include "latch.h"
#include "BtM.h"
#include "perProcessDS.h"
#include "perThreadDS.h"

/*
 * Function: Boolean btm_IsCorrectInternal(Four, BtreeInternal*, KeyDesc*, KeyValue*,
 *                                         IndexID*, LSN*)
 *
 * Description:
 *  Check if the given page is the correct internal page.
 *  The given page was a Btree internal page releasing its latch and
 *  now it is relatched.
 *
 * Returns:
 *  TRUE: if the page is a correct internal page.
 *  FALSE: otherwise
 */
Boolean btm_IsCorrectInternal(
    Four handle,
    BtreeInternal *apage,	/* IN candidate internal page */
    KeyDesc *kdesc,		/* IN key descriptor */
    KeyValue *kval,		/* IN key value */
    IndexID *iid,		/* IN old IndexID value */
    Lsn_T *oldLsn)		/* IN old LSN value */
{
    Four cmp1;			/* result of key comparison */
    Four cmp2;			/* result of key comparison */
    btm_InternalEntry *entry;	/* a leaf entry of Btree */


    TR_PRINT(handle, TR_BTM, TR1,
	     ("btm_IsCorrectInternal(handle, apage=%P, kdesc=%P, kval=%P, iid=%P, oldLsn=%P)",
	      apage, kdesc, kval, iid, oldLsn));


    /* if LSN is unchanged then this page is correct */
    if (oldLsn != NULL && LSN_CMP_EQ(apage->hdr.lsn, *oldLsn)) return(TRUE);

    /* Check if this page remains a leaf page of the given index. */
    if (apage->hdr.iid.serial != iid->serial || !(apage->hdr.type & INTERNAL)) return(FALSE);

    /* If kdesc is NULL,
     * There is no way to prove the correctness of this internal page
     */
    if (kdesc == NULL) return(FALSE);


    /* comparison is possible when # of slot is greater than 0 */
    if (apage->hdr.nSlots == 0) return(FALSE);

    /* Compare the inserted key with the first key on the leaf. */
    entry = (btm_InternalEntry*)&apage->data[apage->slot[0]];
    cmp1 = btm_KeyCompare(handle, kdesc, (KeyValue*)&entry->klen, kval);

    /* Compare the inserted key with the last key on the leaf. */
    entry = (btm_InternalEntry*)&apage->data[apage->slot[-(apage->hdr.nSlots-1)]];
    cmp2 = btm_KeyCompare(handle, kdesc, (KeyValue*)&entry->klen, kval);

    return( (cmp1 == GREAT || cmp2 == LESS) ? FALSE:TRUE );

} /* btm_IsCorrectInternal() */



/*
 * Function: Boolean btm_IsCorrectLeaf(Four, BtreeLeaf*, KeyDesc*, KeyValue*,
 *                                     IndexID*, LSN*)
 *
 * Description:
 *  Check if the given page is the correct leaf page.
 *  The given page was a btree leaf page releasing its latch and
 *  now it is relatched.
 *
 * Returns:
 *  TRUE: if the page is a correct leaf page.
 *  FALSE: otherwise
 */
Boolean btm_IsCorrectLeaf(
    Four handle,
    BtreeLeaf *apage,		/* IN candidate leaf page */
    KeyDesc *kdesc,		/* IN key descriptor */
    KeyValue *kval,		/* IN key value */
    IndexID *iid,		/* IN old IndexID value */
    Lsn_T *oldLsn)		/* IN old LSN value */
{
    Four cmp1;			/* result of key comparison */
    Four cmp2;			/* result of key comparison */
    btm_LeafEntry *entry;	/* a leaf entry of Btree */


    TR_PRINT(handle, TR_BTM, TR1,
	     ("btm_IsCorrectLeaf(handle, apage=%P, kdesc=%P, kval=%P, iid=%P, oldLsn=%P)",
	      apage, kdesc, kval, iid, oldLsn));

    /* if LSN is unchanged then this page is correct */
    if (oldLsn != NULL && LSN_CMP_EQ(apage->hdr.lsn, *oldLsn)) return(TRUE); 

    /* Check if this page remains a leaf page of the given index. */
    if (apage->hdr.iid.serial != iid->serial || !(apage->hdr.type & LEAF)) return(FALSE);

    /* If kdesc is NULL, then skip the key comparison part.
    *  In this case the correctness will be proved by the calling function
    */
    if (kdesc == NULL) return(TRUE);

    if (apage->hdr.nSlots == 0) return(FALSE);

    /* Compare the inserted key with the first key on the leaf. */
    entry = (btm_LeafEntry*)&apage->data[apage->slot[0]];
    cmp1 = btm_KeyCompare(handle, kdesc, (KeyValue*)&entry->klen, kval);

    /* Compare the inserted key with the last key on the leaf. */
    entry = (btm_LeafEntry*)&apage->data[apage->slot[-(apage->hdr.nSlots-1)]];
    cmp2 = btm_KeyCompare(handle, kdesc, (KeyValue*)&entry->klen, kval);

    return( (cmp1 == GREAT || cmp2 == LESS) ? FALSE:TRUE );

} /* btm_isCorrectLeaf() */






