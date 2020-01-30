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
 * Module: BfM_init.c
 *
 * Description :
 *  Initialize the data structure used in buffer manager.
 *
 * Exports:
 *  Four BfM_initSharedDS(Four)
 *   (Internally BfM_initSharedDS( ) use bfm_initBufferInfo( ) function.)
 */


#include "common.h"
#include "error.h"
#include "trace.h"
#include "latch.h"
#include "SHM.h"
#include "BfM.h"
#include "Util.h"
#include "perProcessDS.h"
#include "perThreadDS.h"



/*@================================
 * BfM_InitSharedDS()
 *================================*/
/*
 * Function: Four BfM_InitSharedDS(void)
 *
 * Description:
 *  Initialize the data structure used in buffer manager.
 *  The used data structure is BufferInfo, one per each buffer pool.
 *  For each buffer pool, the required information is buffer size and
 *  the number of buffers in it. With these information, the needed
 *  memory for buffer pool, buffer table, and hash table is allocated.
 *  At last, all data structure is initiated with the intial value.
 *
 * Returns:
 *  error code
 *    some errors cased by function calls
 */
Four BfM_initSharedDS(
    Four    handle
)
{
    Four e;			/* error number */

    TR_PRINT(handle, TR_BFM, TR1, ("BfM_initSharedDS()"));

    e = bfm_initBufferInfo(handle, PAGE_BUF, 1, NUM_PAGE_BUFS);
    if (e < 0) return(e);

    e = bfm_initBufferInfo(handle, TRAIN_BUF, TRAINSIZE2, NUM_LOT_LEAF_BUFS );
    if (e < 0) return(e);


    return (eNOERROR);

}  /* BfM_initSharedDS() */





/*@================================
 * BfM_initLocalDS( )
 *================================*/
Four BfM_initLocalDS(
    Four    handle
)
{
    Four e;			/* error number */
    Lock_ctrlBlock *lock_cb1, *lock_cb2;

    /* pointer for BfM Data Structure of perThreadTable */
    BfM_PerThreadDS_T *bfm_perThreadDSptr = BfM_PER_THREAD_DS_PTR(handle);

    TR_PRINT(handle, TR_BFM, TR1, ("BfM_initLocalDS()"));

    e = Util_initLocalPool(handle, &(bfm_perThreadDSptr->BACB_pool), sizeof(Buffer_ACC_CB), BFM_BACB_POOLSIZE);
    if ( e < eNOERROR ) return(e);

    /*@ initialization */
    /* initialize circular doubly linked list */
    bfm_perThreadDSptr->MyFixed_BACB.prev = &(bfm_perThreadDSptr->MyFixed_BACB);
    bfm_perThreadDSptr->MyFixed_BACB.next = &(bfm_perThreadDSptr->MyFixed_BACB);

    e = Util_getElementFromPool(handle, &BI_LOCK_CB_POOL(PAGE_BUF), &lock_cb1);
    if (e < eNOERROR) ERR(handle, e);

    SHM_initLatch(handle, &lock_cb1->latch);

    bfm_perThreadDSptr->myLCB = lock_cb1;

    e = Util_getElementFromPool(handle, &BI_LOCK_CB_POOL(PAGE_BUF), &lock_cb2);
    if (e < eNOERROR) ERR(handle, e);

    SHM_initLatch(handle, &lock_cb2->latch);

    lock_cb2->nextHashChain = NULL;
    lock_cb1->nextHashChain = lock_cb2;


    return (eNOERROR);

}  /* BfM_initLocalDS */



/*@================================
 * bfm_initBufferInfo( )
 *================================*/
/*
 * Function: Four bfm_initBufferInfo(Four, Four, Four, Four)
 *
 * Description:
 *  Initialize the BufferInfo data structure.
 *
 * Returns:
 *  error code
 *    some errors caused by function calls
 */
Four bfm_initBufferInfo(
    Four handle,
    Four type,			/* IN buffer type */
    Four bufSize,		/* IN buffer size */
    Four nBufs)			/* IN number of buffers */
{
    Four e;			/* error code */
    Four i;			/* loop index */
    BufTBLEntry *anEntry;	/* a Buffer Table Entry */
    void *physical_ptr; 


    TR_PRINT(handle, TR_BFM, TR1, ("bfm_initBufferInfo(type=%lD, bufSize=%lD, nBufs=%lD)",
			   type, bufSize, nBufs));


    /*@ set the control values */
    BI_BUFSIZE(type) = bufSize;
    BI_NBUFS(type) = nBufs;

    /*@
     * allocate the needed memory
     */
    /* allocate memory for buffer table */
    e = SHM_alloc(handle, sizeof(BufTBLEntry)*nBufs, -1, (char **)&physical_ptr);
    BI_BUFTABLE(type) = LOGICAL_PTR(physical_ptr);
    if ( e < eNOERROR )	ERR(handle, e);

    /* allocate memory for buffer pool */
    e = SHM_alloc(handle, nBufs*bufSize*PAGESIZE, -1, (char **)&physical_ptr);
    BI_BUFFERPOOL(type) = LOGICAL_PTR(physical_ptr);
    if ( e < eNOERROR )	ERR(handle, e);

    /*@
     * initialize the allocated memory
     */
    /* initialize the buffer table */
    for (i = 0, anEntry = &BI_BTENTRY(type, 0); i < nBufs; i++, anEntry++) {

	/* reset dirty/invalid Flag */
	anEntry->dirtyFlag = FALSE;
	anEntry->invalidFlag = TRUE;

	/* fixed counter is reset to 0. */
	anEntry->fixed = 0;

	/* hash key is initialized to NIL value. */
	SET_NILBFMHASHKEY(anEntry->key);

        /* initialize the latch of buffer hash entries */
        e = SHM_initLatch(handle, &anEntry->latch);
	if (e < eNOERROR) ERR(handle, e);
    }

    /* initialize pointer of next victim */
    BI_NEXTVICTIM(type) = 0;

    /* Initialize the hash table */
    e = bfm_initBufferHashTable(handle, type);
    if (e < eNOERROR) {
	SHM_free(handle, (char *)PHYSICAL_PTR(BI_BUFTABLE(type)), -1);
	SHM_free(handle, (char *)PHYSICAL_PTR(BI_BUFFERPOOL(type)), -1);
	ERR(handle, e);
    }

    /* Initialize the hash table */
    e = bfm_initLockHashTable(handle, type);
    if (e < eNOERROR) {
	SHM_free(handle, (char *)PHYSICAL_PTR(BI_HASHTABLE(type)), -1);
	SHM_free(handle, (char *)PHYSICAL_PTR(BI_BUFTABLE(type)), -1);
	SHM_free(handle, (char *)PHYSICAL_PTR(BI_BUFFERPOOL(type)), -1);
	ERR(handle, e);
    }

    return(eNOERROR);

} /* bfm_initBufferInfo() */
