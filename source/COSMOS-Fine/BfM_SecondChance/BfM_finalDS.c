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
 * Module: BfM_finalDS.c
 *
 * Description :
 *  Finalize the buffer manager. The finalization is divided into two.
 *
 * Exports:
 *  Four BfM_finalSharedDS( )
 */


#include <stdlib.h>
#include "common.h"
#include "error.h"
#include "trace.h"
#include "BfM.h"
#include "SHM.h"
#include "Util.h" 
#include "perProcessDS.h"
#include "perThreadDS.h"



/*@================================
 * BfM_finalSharedDS( )
 *================================*/
/*
 * Function: Four BfM_finalSharedDS(Four)
 *
 * Description :
 *  Finalize the buffer manager. The finalization is divided into two.
 *  1) For each buffer pool, flush all dirty pages in the buffer pool.
 *  2) For each buffer pool, finalize the BufferInfo structure.
 *
 * Returns:
 *  error Code
 *
 * Side effects:
 *  1) Dirty buffers are flushed.
 *  2) Memory is freed.
 */
Four BfM_finalSharedDS(
    Four    handle
)
{
    Four e;			/* error */
    BufTBLEntry *anEntry;	/* a Buffer Table Entry */
    Four type;			/* buffer type */
    Four i;			/* loop index */
    BfM_PerThreadDS_T *bfm_perThreadDSptr = BfM_PER_THREAD_DS_PTR(handle); 


    TR_PRINT(handle, TR_BFM, TR1, ("BfM_finalSharedDS()"));


    /* For each buffer pool */
    for (type = 0; type < NUM_BUF_TYPES; type++) {

	/*@ flush the dirty buffer */
	/* Flush the dirty buffer in the current buffer pool. */
	for( anEntry = PHYSICAL_PTR(BI_BUFTABLE(type)), i = 0;
	      i < BI_NBUFS(type); anEntry++, i++ )  { 

	    if( anEntry->dirtyFlag ) { /* This buffer is the dirty one. */
		e = bfm_flushBuffer(handle,  anEntry, type );
		if (e < eNOERROR) ERR(handle,  e );
	    }
	}

	/* finalize the data structure 'BufferInfo' */
	e = bfm_finalBufferInfo(handle, type);
	if (e < eNOERROR) ERR(handle, e);
    }

    e = Util_freeElementToPool(handle, &BI_LOCK_CB_POOL(PAGE_BUF), bfm_perThreadDSptr->myLCB->nextHashChain);
    if ( e < eNOERROR ) ERR(handle, e);

    e = Util_freeElementToPool(handle, &BI_LOCK_CB_POOL(PAGE_BUF), bfm_perThreadDSptr->myLCB);
    if ( e < eNOERROR ) ERR(handle, e);


    return( eNOERROR );

}  /* BfM_finalSharedDS */



/*@================================
 * bfm_finalLocalDS()
 *================================*/
/*
 * Function: Four bfm_finalLocalDS(void)
 *
 * Description:
 *  Finalize the local(not shared) data structure.
 *
 * Returns:
 *  error code
 */
Four BfM_finalLocalDS(
    Four    handle
)
{
    Four e;

    /* pointer for BfM Data Structure of perThreadTable */
    BfM_PerThreadDS_T *bfm_perThreadDSptr = BfM_PER_THREAD_DS_PTR(handle);

    TR_PRINT(handle, TR_BFM, TR1, ("BfM_finalLocalDS()"));

    e = Util_finalLocalPool(handle, &(bfm_perThreadDSptr->BACB_pool));
    if ( e < eNOERROR ) ERR(handle, e);

    return(eNOERROR);

} /* bfm_finalLocalDS() */



/*@================================
 * bfm_finalBufferInfo( )
 *================================*/
/*
 * Function: void bfm_finalBufferInfo(handle, Four)
 *
 * Description:
 *  Finalize the BufferInfo data structure.
 *
 * Returns:
 *  error code
 */
Four bfm_finalBufferInfo(
    Four 	handle,
    Four 	type)			/* IN buffer type */
{

    /* free all the allocated memory */

    ERROR_PASS(handle, SHM_free(handle, (char *)PHYSICAL_PTR(BI_BUFTABLE(type)), procIndex));
    ERROR_PASS(handle, SHM_free(handle, (char *)PHYSICAL_PTR(BI_BUFFERPOOL(type)), procIndex));
    ERROR_PASS(handle, SHM_free(handle, (char *)PHYSICAL_PTR(BI_HASHTABLE(type)), procIndex));
    ERROR_PASS(handle, SHM_free(handle, (char *)PHYSICAL_PTR(BI_LOCKHASHTABLE(type)), procIndex));

    return(eNOERROR);

} /* bfm_finalBufferInfo() */

