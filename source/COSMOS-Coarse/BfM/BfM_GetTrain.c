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
 * Module: BfM_GetTrain.c
 *
 * Description : 
 *  Return a buffer which has the disk content indicated by `trainId'.
 *
 * Exports:
 *  Four BfM_GetTrain(TrainID *, char **, Four)
 */

#include "common.h"
#include "trace.h"
#include "BfM_Internal.h"
#include "perThreadDS_Internal.h"
#include "perProcessDS.h"



/*@================================
 * BfM_GetTrain()
 *================================*/
/*
 * Function: BfM_GetTrain(TrainID*, char**, Four)
 *
 * Description : 
 *  Return a buffer which has the disk content indicated by `trainId'.
 *  Before the allocation of a buffer, look up the train in the buffer
 *  pool using hashing mechanism.   If the train already  exist in the pool
 *  then simply return it and set the reference bit of the correponding
 *  buffer table entry.   Otherwise, i.e. the train does not exist  in the
 *  pool,  allocate a buffer (a buffer selected as victim may be forced out
 *  by the buffer replacement algorithm), read a disk train into the 
 *  selected buffer train, and return it.
 *
 * Returns:
 *  error code
 *    eBADBUFFER_BFM - Invalid Buffer
 *    eNOTFOUND_BFM  - The key isn't in the hash table
 *    some errors caused by function calls
 *
 * Side effects:
 *  1) parameter retBuf
 *     pointer to buffer holding the disk train indicated by `trainId'
 */
Four BfM_GetTrain(
    Four handle,
    TrainID             *trainId,               /* IN train to be used */
    char                **retBuf,               /* OUT pointer to the returned buffer */
    Four                type )                  /* IN buffer type */
{
    Four                e;                      /* for error */
    Four                index;                  /* index of the buffer pool */


    TR_PRINT(TR_BFM, TR1, ("BfM_GetTrain(handle, trainId=%P, retBuf=%P, type=%ld)",
			   trainId, retBuf, type));

#ifdef USE_SHARED_MEMORY_BUFFER		
    /* Block signals. */
    SHARED_MEMORY_BUFFER_BLOCK_SIGNAL(handle);
#endif
    
    /*@ Check the validity of given parameters */
    /* Some restrictions may be added         */
    if( retBuf == NULL ) ERR(handle, eBADBUFFER_BFM);

    /* Is the buffer type valid? */
    if (IS_BAD_BUFFERTYPE(type)) ERR(handle, eBADBUFFERTYPE_BFM);	

#ifdef USE_SHARED_MEMORY_BUFFER		
    /*
     * Acquire lock of buffer table entry.
     * Only one process can access a certain buffer table entry.
     */
    ERROR_PASS(handle, bfm_Lock(handle, trainId, type));
#endif

    /*@ Check whether the page exist in the buffer pool */
    if((index = bfm_LookUp(handle, (BfMHashKey *)trainId,type)) == NOTFOUND_IN_HTABLE) {
	/* Not Exist in the pool */
	
        /* Allocate a buffer from the buffer pool of type 'type' */
        index = bfm_AllocTrain(handle, type);
        if( index < eNOERROR ) {
	    ERR_BfM(handle, index, trainId, type); 
	}

        /* Read a train from the disk */
        e = bfm_ReadTrain(handle,  trainId, BI_BUFFER(type, index), type );
        if( e < eNOERROR ) {
	    ERR_BfM(handle, e, trainId, type); 
	}
	
        /* fill the buffer table using the given parameters */
        BI_KEY(type, index) = *((BfMHashKey *)trainId);
	
        /* set the reference bit */
        BI_BITS(type, index) |= REFER;
#ifdef USE_SHARED_MEMORY_BUFFER		
	BI_BITS(type, index) |= VALID;
#endif
	/* Insert the key into the hash table */
        e = bfm_Insert(handle,  (BfMHashKey *)trainId, index, type );
        if( e < eNOERROR ) {
	    ERR_BfM(handle, e, trainId, type);	
	}

	BI_NFIXED(type, index) = 1;
	
    } 
    else if( index >= 0 ) { /* exist in the pool */
	
	BI_NFIXED(type, index)++;

        /* Set the reference bit of the table indicated by `index' */
        BI_BITS(type, index) |= REFER;
    } 
    else {
	ERR_BfM(handle, index, trainId, type); 	
    }

#ifdef USE_SHARED_MEMORY_BUFFER	
    /* Add fixed buffer to fixed buffer table. */
    e = bfm_InsertFixedBuffer(handle, (BfMHashKey* )trainId, type);
    if (e < eNOERROR) {
        ERR_BfM(handle, e, trainId, type);
    }
#endif

    /*@ return the page which corresponds to `index' in the buffer pool */
    *retBuf = BI_BUFFER(type, index);
     
#ifdef USE_SHARED_MEMORY_BUFFER	
    /* Release lock of buffer table entry. */
    ERROR_PASS(handle, bfm_Unlock(handle, trainId, type));
#endif

    return( eNOERROR );   /* No error */
}  /* BfM_GetTrain() */
