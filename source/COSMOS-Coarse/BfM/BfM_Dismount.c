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
/*
 * Module: BfM_Dismount.c
 *
 * Description :
 *  Flush dirty buffers holding trains in the dismounted volume.
 *
 * Exports:
 *  Four BfM_Dismount(Four)
 */


#include "common.h"
#include "trace.h"
#include "BfM_Internal.h"
#include "perThreadDS_Internal.h"
#include "perProcessDS.h"



/*@================================
 * BfM_Dismount()
 *================================*/
/*
 * Function: Four BfM_Dismount(Four)
 *
 * Description :
 *  Flush dirty buffers holding trains in the dismounted volume.
 *  The dismounted volume is specified by the volume number 'volNo'.
 *  A dirty buffer is one with the dirty bit set.
 *
 * Returns:
 *  error code
 *    eFLUSHFIXEDBUF_BFM
 *    some errors caused by function calls
 */
Four BfM_Dismount(
    Four handle,
    Four 	volNo )			/* IN volume to dismount */
{
    Four 		e;					/* error */
    Four  		i;					/* index */
    Four 		type;				/* buffer type */
#ifdef USE_SHARED_MEMORY_BUFFER		
    LockMode	volumeLockMode;		/* volume lock mode */
    BfMHashKey	localKey;
#endif
    
    TR_PRINT(TR_BFM, TR1, ("BfM_Dismount(handle,  volNo = %ld )", volNo));

#if defined(USE_SHARED_MEMORY_BUFFER) && defined(DBLOCK)
    /* 
     * If use shared memory buffer, have to flush and delete buffer only in volume which locked by exclusive lock
     * because when the given volume is locked by read lock and one process dismount the volume, the other process
     * losts shared buffer page and can't read it.
     */
    volumeLockMode = RDsM_GetVolumeLockMode(handle, volNo);
    if (volumeLockMode != L_X && volumeLockMode != L_IX && volumeLockMode != L_SIX) return eNOERROR;
#endif

#ifdef USE_SHARED_MEMORY_BUFFER	
    /* Block signals. */
    SHARED_MEMORY_BUFFER_BLOCK_SIGNAL(handle);
#endif

    /*@ For each buffer pool */
    for (type = 0; type < NUM_BUF_TYPES; type++) {
	
	/* Flush out the dirty buffer with same volume number
	 * in the current buffer pool. */
	for(i = 0; i < BI_NBUFS(type); i++ )  {

#ifdef USE_SHARED_MEMORY_BUFFER	
	    localKey = BI_KEY(type, i);
	    if (!IS_NILBFMHASHKEY(localKey) && localKey.volNo == volNo) { 
		/* This buffer hold a train in the dismounted volume */
		ERROR_PASS(handle, bfm_Lock(handle, (TrainID* )&localKey, type));

		if (!EQUALKEY(&BI_KEY(type, i), &localKey)) {	
		    ERROR_PASS(handle, bfm_Unlock(handle, (TrainID* )&localKey, type));
		    continue;
		}
#else
	    if (!IS_NILBFMHASHKEY(BI_KEY(type,i)) && BI_KEY(type, i).volNo == volNo) { 
		/* This buffer hold a train in the dismounted volume */
#endif
		
		/* This buffer is an unfixed buffer. */
		if (BI_NFIXED(type, i) != 0) {
		    ERR_BfM(handle, eFLUSHFIXEDBUF_BFM, (TrainID* )&localKey, type);	
		}

		/* flush out this buffer */
		if (BI_BITS(type, i) & DIRTY) {
		    e = bfm_FlushTrain(handle,  (TrainID *)&BI_KEY(type, i), type );
		    if( e < eNOERROR ) {
			ERR_BfM(handle, e, (TrainID* )&localKey, type);		
		    }
		}
		
		/* Delete the victim from the hash table */
		e = bfm_Delete(handle,  &BI_KEY(type, i), type );
		if (e < eNOERROR ) {
		    ERR_BfM(handle, e, (TrainID* )&localKey, type);	
		}
		
		/*
		 * Clear the REFER bit : 
		 * A train in the dismounted volume will not be used
		 * near future. 
		 */
#ifdef USE_SHARED_MEMORY_BUFFER 	
		/* 
		 * Set BI_NFIXED(type, i) by 1 not to select as victim
		 * during the initializing BI_KEY(type, i) to NIL.
		 */
		BI_NFIXED(type, i) = 1;
		BI_BITS(type, i) = ALL_0;
		SET_NILBFMHASHKEY(BI_KEY(type, i));
		BI_NFIXED(type, i) = 0;
		
		/* Release lock of a buffer table entry. */
		ERROR_PASS(handle, bfm_Unlock(handle, (TrainID* )&localKey, type));
#else
		BI_BITS(type, i) = ALL_0;
		SET_NILBFMHASHKEY(BI_KEY(type, i));
#endif
	    }
	}
    }
    
#ifdef USE_SHARED_MEMORY_BUFFER		
    /* Unblock signals. */
    SHARED_MEMORY_BUFFER_UNBLOCK_SIGNAL(handle);

    /* Check received signal and handle it. */
    SHARED_MEMORY_BUFFER_CHECK_SIGNAL(handle);
#endif

    return( eNOERROR );
    
}  /* BfM_Dismount */

