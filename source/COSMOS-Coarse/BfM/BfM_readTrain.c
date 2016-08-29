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
 * Module: BfM_readTrain.c
 *
 * Description : 
 *  read a train from the buffer if it exits in the buffer.
 *  otherwise read a train from the disk page.
 *
 * Exports:
 *  Four BfM_readTrain(TrainID *, void *, Four)
 */


#include <string.h>
#include "common.h"
#include "trace.h"
#include "BfM_Internal.h"
#include "perThreadDS_Internal.h"
#include "perProcessDS.h"



/*@================================
 * BfM_readTrain()
 *================================*/
/*
 * Function: Four BfM_readTrain(TrainID *, char *, Four)
 *
 * Description : 
 *  read a train from the buffer if it exits in the buffer.
 *  otherwise read a train from the disk page.
 *
 * Returns :
 *  error code
 *    eBADBUFFER_BFM - Invalid Buffer
 *    eNOTFOUND_BFM  - The key isn't in the hash table
 *    some errors caused by function calls
 *
 * Side effects:
 *  1) parameter acc_cb
 *     pointer to buffer access control block holding the disk train indicated by `trainId'
 */
Four BfM_readTrain(
    Four handle,
    TrainID             *trainId,               /* IN train to read */
    char                *aTrain,                /* OUT pointer to the train buffer */
    Four                type )                  /* IN buffer type */
{
    Four                index;                  /* index of entry in buffer table */
    Four                e;                      /* for resource release error */

    
    TR_PRINT(TR_BFM, TR1,("BfM_readTrain(handle)"));

#ifdef USE_SHARED_MEMORY_BUFFER	
    /* Block signal. */
    SHARED_MEMORY_BUFFER_BLOCK_SIGNAL(handle);
#endif

    /*@ Check the validity of given parameters */
    
    /* Is the buffer type valid? */
    if (IS_BAD_BUFFERTYPE(type))
	ERR(handle, eBADBUFFERTYPE_BFM);

#ifdef USE_SHARED_MEMORY_BUFFER		
    /*
     * Acquire lock of a buffer table entry.
     * Only one process can access a certain buffer table entry at a time.
     */
    ERROR_PASS(handle, bfm_Lock(handle, trainId, type));
#endif

    /* Check whether the page exist in the buffer pool */
    index = bfm_LookUp(handle, (BfMHashKey *)trainId, type);
    if(index == NOTFOUND_IN_HTABLE) {	/* Not Exist in the pool */
	/*@ read a train */
        e = bfm_ReadTrain(handle, trainId, aTrain, type);
        if( e < eNOERROR ) {
	    ERR_BfM(handle, e, trainId, type);	
	}
    } 
    else if (index >= eNOERROR) {

        memcpy(aTrain, BI_BUFFER(type, index), BI_BUFSIZE(type)*PAGESIZE);
        
        /*@ if the dirty bit is set, force out to the disk */
        if(BI_BITS(type, index) & DIRTY)  {
            e = bfm_FlushTrain(handle, trainId, type);
            if( e < eNOERROR ) {
	    	ERR_BfM(handle, e, trainId, type);	
            }
        }

        /*@ Delete the train from the hash table */
        e = bfm_Delete(handle, (BfMHashKey *)trainId, type);
        if( e < eNOERROR ) {
	    ERR_BfM(handle, e, trainId, type);	
        }

        /*@ Delete the train from buffer pool */
#ifdef USE_SHARED_MEMORY_BUFFER	
	/* 
	 * Set BI_NFIXED(type, index) by 1 not to select as victim
	 * during the initializing BI_KEY(type, index) to NIL
	 */
	BI_NFIXED(type, index) = 1;
#endif	
        /* reset dirty/refer bit */
        BI_BITS(type, index) = ALL_0;

        /* hash key is initialized to NIL value. */
        SET_NILBFMHASHKEY(BI_KEY(type, index));
        
	/* fixed counter is reset to 0. */
        BI_NFIXED(type, index) = 0;
    } 
    else {
	ERR_BfM(handle, index, trainId, type);	
    }
    
#ifdef USE_SHARED_MEMORY_BUFFER		
    /* Release lock of a buffer table entry. */
    ERROR_PASS(handle, bfm_Unlock(handle, trainId, type));

    /* Unblock signal. */
    SHARED_MEMORY_BUFFER_UNBLOCK_SIGNAL(handle);

    /* Check received signal and handle it. */
    SHARED_MEMORY_BUFFER_CHECK_SIGNAL(handle);
#endif

    return( eNOERROR );   /* No error */

}  /* BfM_readTrain */
