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
 * Module: btm_ResetStatusBit.c
 *
 * Description:
 *  Reset the status bits of the given page.
 *
 *  CAUTION :: Calling routine must acquire the latch(es) of child_BCB
 *            (and parentLatchPtr, if exists). This routine keeps the
 *            latch of child_BCB and if parentLatchPtr is not NULL,
 *            release this latch.
 *
 * Exports:
 *  Four btm_ResetStatusBitsLeaf(Four, btm_TraversePath*, KeyDesc*,
 *                               KeyValue*, Buffer_ACC_CB*, LATCH_TYPE*)
 *
 * Returns:
 *  BTM_RELATCHED
 *  BTM_RETRAVERSE
 *  Error code
 *    some errors caused by function calls
 */


#include "common.h"
#include "error.h"
#include "trace.h"
#include "latch.h"
#include "BfM.h"
#include "BtM.h"
#include "perProcessDS.h"
#include "perThreadDS.h"


Four btm_ResetStatusBitsLeaf(
    Four handle,
    btm_TraversePath *path,	/* IN Btree traverse path stack */
    KeyDesc *kdesc,		/* IN key descriptor */
    KeyValue *kval,		/* IN key value */
    Buffer_ACC_CB *leaf_BCB,	/* INOUT buffer control block for leaf page */
    LATCH_TYPE *parentLatchPtr)	/* IN pointer to parent latch */
{
    Four e;			/* error code */
    BtreeLeaf *apage;		/* pointer to buffer holding a leaf page */
    Lsn_T oldLsn;               /* remember the lsn of the leaf */
    IndexID iid;		/* IndexID of the leaf */


    TR_PRINT(handle, TR_BTM, TR1,
	     ("btm_ResetStatusBitsLeaf(handle, path=%P, kdesc=%P, kval=%P, leaf_BCB=%P, parentLatchPtr=%P)",
	      path, kdesc, kval, leaf_BCB, parentLatchPtr));


    apage = (BtreeLeaf*)leaf_BCB->bufPagePtr;

    /* Request a conditional, instant duration S latch on the tree. */
    e = btm_GetTreeLatchInPath(handle, path, M_SHARED, M_CONDITIONAL|M_INSTANT);
    if (e < 0) {
        if (parentLatchPtr != NULL)
            ERRL2(handle, e, leaf_BCB->latchPtr, parentLatchPtr);
        else
            ERRL1(handle, e, leaf_BCB->latchPtr);
    }

    if (e == eNOERROR) {   /* if e != SHM_BUSYLATCH */

        /* release the parent latch. */
        if (parentLatchPtr != NULL) {
            e = SHM_releaseLatch(handle, parentLatchPtr, procIndex);
            if (e < eNOERROR) ERRL1(handle, e, leaf_BCB->latchPtr);
        }

	/* Reset the smBit and deleteBit to 0 on the leaf. */
	apage->hdr.statusBits = 0;

	leaf_BCB->dirtyFlag = 1;

	return(eNOERROR);

    }

    /* Release latches and acqiure tree latch. */
    if (parentLatchPtr != NULL) {
        e = SHM_releaseLatch(handle, parentLatchPtr, procIndex);
        if (e < eNOERROR) ERRL1(handle, e, leaf_BCB->latchPtr);
    }

    /* Remember the LSN of the leaf. */
    oldLsn = apage->hdr.lsn;
    iid = apage->hdr.iid;

    e = SHM_releaseLatch(handle, leaf_BCB->latchPtr, procIndex);
    if (e < eNOERROR) ERR(handle, e);


    /* Request an unconditional, manual duration S latch on the tree. */
    e = btm_GetTreeLatchInPath(handle, path, M_SHARED, M_UNCONDITIONAL);
    if (e < eNOERROR) ERR(handle, e);

    /* Relatch the leaf with X mode. */
    e = SHM_getLatch(handle, leaf_BCB->latchPtr, procIndex, M_EXCLUSIVE, M_UNCONDITIONAL, NULL);
    if (e < eNOERROR) ERRTL(handle, e, path);

    /* Release the tree latch. */
    e = btm_ReleaseTreeLatchInPath(handle, path);
    if (e < eNOERROR) ERRL1(handle, e, leaf_BCB->latchPtr);

    if (!LSN_CMP_EQ(apage->hdr.lsn, oldLsn) &&
	(apage->hdr.iid.serial != iid.serial || !(apage->hdr.type & LEAF))) { 

	/* Possible starvation: We may hold the tree latch which */
	/* is released above after acquiring a latch for the correct leaf. */
        e = SHM_releaseLatch(handle, leaf_BCB->latchPtr, procIndex);
        if (e < eNOERROR) ERR(handle, e);

	return(BTM_RETRAVERSE);
    }

    /* Reset the smBit and deleteBit to 0 on the leaf. */
    apage->hdr.statusBits = 0;

    leaf_BCB->dirtyFlag = 1;

    /* Check the boundary condition. */
    if (!btm_IsCorrectLeaf(handle, apage, kdesc, kval, &iid, NULL)) { 

	/* Possible starvation: We may hold the tree latch which */
	/* is released above after acquiring a latch for the correct leaf. */
	e = SHM_releaseLatch(handle, leaf_BCB->latchPtr, procIndex);
        if (e < eNOERROR) ERR(handle, e);

	return(BTM_RETRAVERSE);
    }


    return(BTM_RELATCHED);



} /* btm_ResetStatusBitsLeaf() */
