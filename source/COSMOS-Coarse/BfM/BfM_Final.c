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
 * Module: BfM_Final.c
 *
 * Description :
 *  Finalize the buffer manager.
 *
 * Exports:
 *  Four BfM_Final(void)
 */

#ifdef USE_SHARED_MEMORY_BUFFER
#include <sys/shm.h>
#undef PAGESIZE
#endif

#include <stdlib.h>
#include "common.h"
#include "trace.h"
#include "BfM_Internal.h"
#include "perThreadDS_Internal.h"
#include "perProcessDS.h"


/*@ internal function prototypes */
Four bfm_FinalBufferInfo(Four);	



/*@================================
 * BfM_FinalPerThreadDS()
 *================================*/
/* 
 * Function: Four BfM_Final(void)
 *
 * Description :
 *  Finalize the buffer manager. The finalization is divided into two.
 *  1) For each buffer pool, flush all dirty pages in the buffer pool.
 *  2) For each buffer pool, finalize the BufferInfo structure.
 *
 * Note:
 *  In the case using shared memory buffer, the finalization is divided 
 *  into three.  
 *  1) For each buffer pool, flush all dirty pages in the buffer pool.
 *
 * Returns:
 *  error Code
 *
 * Side effects:
 *  1) Dirty buffers are flushed.
 */
Four BfM_FinalPerThreadDS(Four handle)
{
    Four  	    e;			/* error */
    Four 	    type;		/* buffer type */
    
    TR_PRINT(TR_BFM, TR1, ("BfM_Final(handle)"));

#ifdef USE_SHARED_MEMORY_BUFFER		
    /* Block signals. */
    SHARED_MEMORY_BUFFER_BLOCK_SIGNAL(handle);
    
    /* Flush all dirty page */
    e = bfm_FinalBufferInfo(handle);
    if (e < eNOERROR) ERR(handle, e);

#ifndef USE_MUTEX
    e = bfm_FreeThreadCtrlBlock(handle);
    if (e < eNOERROR) ERR(handle, e);
#endif

    /* Free lock control blocks of this process. */
    e = bfm_FreeMyLockCBs(handle, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);
    
   /* Unblock signals. */
    SHARED_MEMORY_BUFFER_UNBLOCK_SIGNAL(handle);

    /* Check signal and handle it. */
    SHARED_MEMORY_BUFFER_CHECK_SIGNAL(handle);

    /*@
     * Final fixed buffer table. 
     *
     * Must call this function after SHARED_MEMORY_BUFFER_CHECK_SIGNAL()
     * because bfm_SignalHandler which is called by SHARED_MEMORY_BUFFER_CHECK_SIGNAL 
     * uses fixed buffer table.
     */
    e = bfm_FinalFixedBufferInfo(handle);
    if (e < eNOERROR) ERR(handle, e);
#else
    /* Flush all dirty page */
    e = bfm_FinalBufferInfo(handle);
    if (e < eNOERROR) ERR(handle, e);
#endif

    return(eNOERROR);

} /* BfM_FinalPerThreadDS() */

/*@================================
 * bfm_FinalBufferInfo()
 *================================*/
/*
 * Function: Four bfm_FinalBufferInfo()
 *
 * Description:
 *  Finalize the BufferInfo data structure.
 *
 * Note:
 *  Remove the type parameter from bfm_FinalBufferInfo(Four type)
 *
 * Returns:
 *  error code
 */
Four bfm_FinalBufferInfo(Four handle)
{    												
    Four 		type;
    Four 		i;
    Four 		e;
#ifdef USE_SHARED_MEMORY_BUFFER		
    LockMode	volumeLockMode;		
    BfMHashKey	localKey;
#endif
    
    TR_PRINT(TR_BFM, TR1, ("bfm_FinalBufferInfo(handle)"));

    /*@ For each buffer pool */
    for (type = 0; type < NUM_BUF_TYPES; type++) {
	/* Flush the dirty buffer in the current buffer pool. */
	for(i = 0; i < BI_NBUFS(type); i++ )  {

	    /* Some buffer pages could be updated by other processes 
	     * between transaction commit and buffer finalization.
	     * So, comment out next line.
	     */ 
            /* if (BI_BITS(type, i) & DIRTY) ERR(handle, eINTERNAL); */ 

#ifdef USE_SHARED_MEMORY_BUFFER		
	    localKey = BI_KEY(type, i);
	    
#ifdef DBLOCK
	    volumeLockMode = RDsM_GetVolumeLockMode(handle, localKey.volNo);
	    if( BI_BITS(type, i) & DIRTY 
		&& bfm_CheckBufPageInMountedVolume(handle, BFM_PER_THREAD_DS(handle).mountedVolNos, BFM_PER_THREAD_DS(handle).nMountedVols,&localKey) 
		&& (volumeLockMode == L_X || volumeLockMode == L_IX || volumeLockMode == L_SIX)) {
#else
	    if( BI_BITS(type, i) & DIRTY && bfm_CheckBufPageInMountedVolume(handle, BFM_PER_THREAD_DS(handle).mountedVolNos, 
		BFM_PER_THREAD_DS(handle).nMountedVols, &localkey)) {
#endif

		/* Acquire lock of the buffer table entry. */
		ERROR_PASS(handle, bfm_Lock(handle, (TrainID* )&localKey, type));

		if (!EQUALKEY(&BI_KEY(type, i), &localKey)) {
		    ERROR_PASS(handle, bfm_Unlock(handle, (TrainID* )&localKey, type));
		    continue;
		}

		/* This buffer is the dirty one. */
		e = bfm_FlushTrain(handle, (TrainID* )&BI_KEY(type, i), type);
		if (e < eNOERROR) {
		    ERR_BfM(handle, e, (TrainID* )&localKey, type);
		}

		/* Release lock of the buffer table entry. */
		ERROR_PASS(handle, bfm_Unlock(handle, (TrainID* )&localKey, type));
	    }
#else
	    if( BI_BITS(type, i) & DIRTY ) {

		/* This buffer is the dirty one. */
		e = bfm_FlushTrain(handle, (TrainID* )&BI_KEY(type, i), type);
		if (e < 0) ERR(handle, e);
	    }
#endif
	}
    }
    return (eNOERROR);
} /* bfm_FinalBufferInfo() */


/*@================================
 * BfM_FinalPerProcessDS()
 *================================*/
/* 
 * Function: Four BfM_FinalPerProcessDS(void)
 *
 * Description :
 *  Finalize the buffer manager. The finalization is divided into two.
 *  1) For each buffer pool, flush all dirty pages in the buffer pool.
 *  2) For each buffer pool, finalize the BufferInfo structure.
 *
 * Note:
 *  In the case using shared memory buffer, the finalization is divided 
 *  into three.  
 *  1) Detach the shared memory buffer.
 *  2) If this process is last process, destroy the shared memory buffer in kernel.
 *  
 *
 * Returns:
 *  error Code
 *
 * Side effects:
 *  1) Memory is freed.
 */
Four BfM_FinalPerProcessDS(Four handle)
{
    Four e;

#ifdef USE_SHARED_MEMORY_BUFFER
    /* Only one process can finalize destroy buffer  */
    /* Acquire the file lock of shared memory buffer */
    //SHARED_MEMORY_BUFFER_CRITICAL_SECTION_START();
   
    /* Free process table entry of this process. */
    e = bfm_FreeProcessTableEntry(handle);
    if (e < eNOERROR) ERR(handle, e);

   /* Destroy shared memory buffer */ 
    e = bfm_DestroySharedMemoryBuffer(handle);
    if (e < eNOERROR) ERR(handle, e);

    /* Release the file lock of shared memory buffer */
    //SHARED_MEMORY_BUFFER_CRITICAL_SECTION_END();
#else
    /* Destroy local memory buffer */
    e = bfm_DestroyLocalMemoryBuffer(handle);
    if (e < eNOERROR) ERR(handle, e);
#endif
} /* BfM_FinalPerProcessDS */

#ifdef USE_SHARED_MEMORY_BUFFER
/*@================================
 * bfm_DestroySharedMemoryBuffer
 *================================*/
/*
 * Function: Four bfm_DestroySharedMemoryBuffer()
 *
 * Description:
 *  Detach shared memory buffer in this process.
 *  If this process is last one, destroy shared memory buffer.
 *
 * Returns:
 *  error code
 */
Four bfm_DestroySharedMemoryBuffer(Four handle)
{
    Four e;
    Four i;

    TR_PRINT(TR_BFM, TR1, ("bfm_DestroySharedMemoryBuffer(handle)"));

    /* If this process is last process, flush the dirty buffer and destroy shared memory */
    /* Judge this process is last one by getting the number of attached shared memory buffer. */
    if ((e = bfm_GetNumAttachedSHMBuffer(handle)) == 1) {

#ifdef COSMOS_MULTITHREAD
    	for (i = 0; i < MAXNUMOFVOLS; i++) {
            RWLOCK_FINAL(&(volLockTable[i].rwlock));
    	}
#endif

	/* Detache shared memory */
	e = shmdt((char* )bufInfo);
	if (e < 0) ERR(handle, eSHMDTFAILED_BFM);
	
	bufInfo = NULL;

	/* Destroy shared memory */
	e = shmctl(BFM_PER_PROCESS_DS.shmBufferId, IPC_RMID, NULL); 
	if (e < 0) ERR(handle, eSHMCTLFAILED_BFM);

    }
    /* Other processes just detach shared memory */
    else if (e > 1) {
	e = shmdt((char* )bufInfo);
	if (e < 0) ERR(handle, eSHMDTFAILED_BFM);
	bufInfo = NULL;
    }
    /* the case, e < eNOERROR */
    else ERR(handle, e);


    return eNOERROR;
} /* bfm_DestroySharedMemoryBuffer() */
#else

/*@================================
 * bfm_DestroyLocalMemoryBuffer
 *================================*/
/*
 * Function: Four bfm_DestroyLocalMemoryBuffer()
 *
 * Description:
 *  Destroy local memory buffer.
 *
 * Returns:
 *  error code
 */
Four bfm_DestroyLocalMemoryBuffer(Four handle)
{
    Four 	type;

    TR_PRINT(TR_BFM, TR1, ("bfm_DestroyLocalMemoryBuffer(handle)"));

    for (type = 0; type < NUM_BUF_TYPES; type++) {
    	free(BI_BUFTABLE(type));    
    	free(BI_BUFFERPOOL(type));
    	free(BI_HASHTABLE(type));
    }
    
    return (eNOERROR);
} /* bfm_DestroyLocalMemoryBuffer() */
#endif
