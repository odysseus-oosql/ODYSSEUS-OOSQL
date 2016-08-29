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
 * Module: bfm_Hash.c
 *
 * Description:
 *  Some functions are provided to support buffer manager.
 *  Each BfMHashKey is mapping to one table entry in a hash table(hTable),
 *  and each entry has an index which indicates a buffer in a buffer pool.
 *  An ordinary hashing method is used and linear probing strategy is
 *  used if collision has occurred.
 *
 * Exports:
 *  Four bfm_LookUp(BfMHashKey *, Four)
 *  Four bfm_Insert(BfMHaskKey *, Two, Four)
 *  Four bfm_Delete(BfMHashKey *, Four)
 *  Four bfm_DeleteAll(void)
 */


#include <stdlib.h> /* for malloc & free */
#include "common.h"
#include "trace.h"
#include "BfM_Internal.h"
#include "perThreadDS_Internal.h"
#include "perProcessDS.h"


/*@
 * macro definitions
 */  
#define R_MOD(type)	(HASHTABLESIZE(type)*3)

#define BFM_HASH(k,type)	(((k)->volNo + (k)->pageNo) % HASHTABLESIZE(type)) 

/* Circular hash table is supported by the following macros */

#define BFM_NEXT(i,type)  ( ((i) < (HASHTABLESIZE(type) - 1)) ? (i)+1 : 0 ) 

#define RELOCATABLE(h,l,u) \
	((l < u) ? ((h <= l) || (h > u)) : ((h <= l) && (h > u)))



/*@================================
 * bfm_Insert()
 *================================*/
/*
 * Function: Four bfm_Insert(BfMHashKey *, Two, Four)
 *
 * Description:
 *  Insert a new entry into the hash table.
 *  If collision occurs, then use the linear probing method.
 *
 * Returns:
 *  error code
 *    eBABBUFINDEX_BFM - bad index value for buffer table
 */
Four bfm_Insert(
    Four handle,
    BfMHashKey 		*key,			/* IN a hash key in Buffer Manager */
    Four			index,			/* IN an index used in the buffer pool */
    Four 			type)			/* IN buffer type */
{
    Four 			i;				
    Four  			hashValue;             	
#ifdef USE_SHARED_MEMORY_BUFFER			
    Four			e;			/* error code */
    HashTable		*hashEntryPtr;
#endif

    TR_PRINT(TR_BFM, TR1,
             ("bfm_Insert(handle,  key=%P, index=%ld, type=%ld )", key, index, type));
    
    CHECKKEY(key);    /*@ check validity of key */

    if( (index < 0) || (index > BI_NBUFS(type)) )
        ERR(handle,  eBADBUFINDEX_BFM );

    hashValue = BFM_HASH(key,type);

#ifdef USE_SHARED_MEMORY_BUFFER		
    /* Get latch to protect from updating the hash chain. */
	hashEntryPtr = &(((HashTable*)PHYSICAL_PTR(BI_HASHTABLE(type)))[hashValue]); 
#ifdef USE_MUTEX
    ERROR_PASS(handle, MUTEX_LOCK(&hashEntryPtr->mutex));
#else
    ERROR_PASS(handle, bfm_GetLatch(handle, &hashEntryPtr->latch, procIndex, M_EXCLUSIVE, M_UNCONDITIONAL, NULL));
#endif
#endif
    BI_NEXTHASHENTRY(type,index) = BI_HASHTABLEENTRY(type,hashValue);
    BI_HASHTABLEENTRY(type,hashValue) = index;
   
#ifdef USE_SHARED_MEMORY_BUFFER		
#ifdef USE_MUTEX
    /* Release latch. */
    ERROR_PASS(handle, MUTEX_UNLOCK(&hashEntryPtr->mutex));
#else
    /* Release latch. */
    ERROR_PASS(handle, bfm_ReleaseLatch(handle, &hashEntryPtr->latch, procIndex));
#endif
#endif

    return( eNOERROR );

}  /* bfm_Insert */



/*@================================
 * bfm_Delete()
 *================================*/
/*
 * Function: Four bfm_Delete(BfMHashKey *, Four)
 *
 * Description:
 *  Look up the entry which corresponds to `key' and
 *  Delete the entry from the hash table.
 *
 * Returns:
 *  error code
 *    eNOTFOUND_BFM - The key isn't in the hash table.
 */
Four bfm_Delete(
    Four handle,
    BfMHashKey          *key,                   /* IN a hash key in buffer manager */
    Four                type )                  /* IN buffer type */
{
    Four                i, prev;                
    Four                hashValue;              
#ifdef USE_SHARED_MEMORY_BUFFER	
    Four				e;			/* error code */
    HashTable			*hashEntryPtr;
#endif

    TR_PRINT(TR_BFM, TR1, ("bfm_Delete(handle,  key=%P, type=%ld )", key, type));


    CHECKKEY(key);    /*@ check validity of key */

    hashValue = BFM_HASH(key,type);
#ifdef USE_SHARED_MEMORY_BUFFER		
    /* Get latch to prevent updating the hash chain. */
	hashEntryPtr = &(((HashTable*)PHYSICAL_PTR(BI_HASHTABLE(type)))[hashValue]); 
#ifdef USE_MUTEX
    ERROR_PASS(handle, MUTEX_LOCK(&hashEntryPtr->mutex));
#else
    ERROR_PASS(handle, bfm_GetLatch(handle, &hashEntryPtr->latch, procIndex, M_EXCLUSIVE, M_UNCONDITIONAL, NULL));
#endif
#endif
    /* Look up the key in the hash chain */
    for( i = BI_HASHTABLEENTRY(type,hashValue), prev = NIL; i != NIL ; prev = i, i = BI_NEXTHASHENTRY(type,i) ) {
        if(EQUALKEY( &(BI_KEY(type, i)), key )) {
            if (prev == NIL)
                BI_HASHTABLEENTRY(type,hashValue) = BI_NEXTHASHENTRY(type, i);
            else
                BI_NEXTHASHENTRY(type,prev) = BI_NEXTHASHENTRY(type, i);

#ifdef USE_SHARED_MEMORY_BUFFER		
#ifdef USE_MUTEX
            /* Release the latch. */
            ERROR_PASS(handle, MUTEX_UNLOCK(&hashEntryPtr->mutex));
#else
    	    /* Release the latch. */
		    ERROR_PASS(handle, bfm_ReleaseLatch(handle, &hashEntryPtr->latch, procIndex));
#endif
#endif

            return(eNOERROR);
        }
    }

#ifdef USE_SHARED_MEMORY_BUFFER	
#ifdef USE_MUTEX
    /* Release the latch. */
    ERROR_PASS(handle, MUTEX_UNLOCK(&hashEntryPtr->mutex));
#else
    /* Release the latch. */
    ERROR_PASS(handle, bfm_ReleaseLatch(handle, &hashEntryPtr->latch, procIndex));
#endif
#endif

    ERR(handle,  eNOTFOUND_BFM );
}  /* bfm_Delete */


/*@================================
 * bfm_LookUp()
 *================================*/
/*
 * Function: Four bfm_LookUp(BfMHashKey *, Four)
 *
 * Description:
 *  Look up the given key in the hash table and return its
 *  corressponding index to the buffer table.
 *
 * Retruns:
 *  index on buffer table entry holding the train specified by 'key'
 *  (NOTFOUND_IN_HTABLE - The key don't exist in the hash table.)
 */
Four bfm_LookUp(
    Four handle,
    BfMHashKey          *key,                   /* IN a hash key in Buffer Manager */
    Four                type)                   /* IN buffer type */
{
    Four                i, j;                   /* indices */
    Four                hashValue;              
#ifdef USE_SHARED_MEMORY_BUFFER 
    Four				e;			/* error code */
    HashTable			*hashEntryPtr;
#endif

    TR_PRINT(TR_BFM, TR1, ("bfm_LookUp(handle,  key=%P, type=%ld )", key, type));
    
    CHECKKEY(key);    /*@ check validity of key */

    hashValue = BFM_HASH(key,type);
#ifdef USE_SHARED_MEMORY_BUFFER		
    /* Get latch to prevent updating the hash chain. */
	hashEntryPtr = &(((HashTable*)PHYSICAL_PTR(BI_HASHTABLE(type)))[hashValue]);
#ifdef USE_MUTEX
    ERROR_PASS(handle, MUTEX_LOCK(&hashEntryPtr->mutex));
#else
    ERROR_PASS(handle, bfm_GetLatch(handle, &hashEntryPtr->latch, procIndex, M_SHARED, M_UNCONDITIONAL, NULL));
#endif
#endif

    /* Look up the key in the hash chain */
    for( i = BI_HASHTABLEENTRY(type,hashValue); i != NIL ; i = BI_NEXTHASHENTRY(type,i) ) {
        if(EQUALKEY( &(BI_KEY(type, i)), key )) {
#ifdef USE_SHARED_MEMORY_BUFFER		
#ifdef USE_MUTEX
            /* Release the latch. */
            ERROR_PASS(handle, MUTEX_UNLOCK(&hashEntryPtr->mutex));
#else
		    /* Release the latch. */
		    ERROR_PASS(handle, bfm_ReleaseLatch(handle, &hashEntryPtr->latch, procIndex));
#endif
#endif
	    return(i);
	}
    }

#ifdef USE_SHARED_MEMORY_BUFFER		
#ifdef USE_MUTEX
    /* Release the latch. */
    ERROR_PASS(handle, MUTEX_UNLOCK(&hashEntryPtr->mutex));
#else
    /* Release the latch. */
    ERROR_PASS(handle, bfm_ReleaseLatch(handle, &hashEntryPtr->latch, procIndex));
#endif
#endif

    return( NOTFOUND_IN_HTABLE );
    
}  /* bfm_LookUp */



/*@================================
 * bfm_InitHashTable()
 *================================*/
/*
 * Function: Four bfm_InitHashTable(Four)
 *
 * Description:
 *  Initialize the hash table as each entry has NIL value.
 *  At first, we allocate memory for the hash table.
 *
 * Returns:
 *  error codes
 *    eNOMEM_BFM - memory allocation failed
 */
Four bfm_InitHashTable(
    Four 		handle,
    Four 		type)			/* IN buffer type */
{
    Four 		i;			/* index variable */
    Four 		e;			/* error code */ 

    TR_PRINT(TR_BFM, TR1, ("bfm_InitHashTable(handle,  type=%ld )", type));

    for(i=0; i < HASHTABLESIZE(type); i++) {
	BI_HASHTABLEENTRY(type, i) = NIL; 		    
#ifdef USE_SHARED_MEMORY_BUFFER	
#ifdef USE_MUTEX
        /* Init mutex of hash table entry */
        MUTEX_INIT(&(((HashTable*)PHYSICAL_PTR(BI_HASHTABLE(type))))[i].mutex, &cosmos_thread_mutex_attr, PTHREAD_PROCESS_SHARED); 
#else
	e = bfm_InitLatch(handle, &((HashTable*)PHYSICAL_PTR(BI_HASHTABLE(type)))[i].latch); 
	if (e < eNOERROR) ERR(handle, e); 
#endif
#endif
    }

    return(eNOERROR);
    
}  /* bfm_InitHashTable */


/*@================================
 * bfm_DeleteAll()
 *================================*/
/*
 * Function: Four bfm_DeleteAll(void)
 *
 * Description:
 *  Delete all hash entries.
 *
 * Returns:
 *  error code
 */
Four bfm_DeleteAll(Four handle)
{
    Four	i;
#ifdef USE_SHARED_MEMORY_BUFFER		
    Four 	e;						/* error code */
    Four 	bufIdx;					/* index of buffer table */
    Four 	prevIdx;				/* index of buffer table */
    LockMode 	volumeLockMode;		/* volume lock mode */
    HashTable*	hashEntryPtr;
#endif
    Four        tableSize;
    
    TR_PRINT(TR_BFM, TR1, ("bfm_DeleteAll(handle)"));

    tableSize = HASHTABLESIZE(PAGE_BUF);
    for(i = 0; i < tableSize; i++) {

#ifdef USE_SHARED_MEMORY_BUFFER		
	/* 
	 * If use shared memory buffer, must delete hashtable entry which has buffer index of buffer page in mounted volumes
	 * because pages in not-mounted volume may be in shared memory buffer.
	 */
	
	/* Get latch to prevent updating the hash chain. */
	hashEntryPtr = &(((HashTable*)PHYSICAL_PTR(BI_HASHTABLE(PAGE_BUF)))[i]); 
#ifdef USE_MUTEX	
        ERROR_PASS(handle, MUTEX_LOCK(&hashEntryPtr->mutex));
#else
        ERROR_PASS(handle, bfm_GetLatch(handle, &hashEntryPtr->latch, procIndex, M_EXCLUSIVE, M_UNCONDITIONAL, NULL));
#endif

	prevIdx = NIL;
	bufIdx  = BI_HASHTABLEENTRY(PAGE_BUF, i);
	BI_HASHTABLEENTRY(PAGE_BUF, i) = NIL; 
	while (bufIdx != NIL) {
#ifdef DBLOCK
	    volumeLockMode = RDsM_GetVolumeLockMode(handle, BI_KEY(PAGE_BUF, bufIdx).volNo); 
	    if (!bfm_CheckBufPageInMountedVolume(handle, BFM_PER_THREAD_DS(handle).mountedVolNos, BFM_PER_THREAD_DS(handle).nMountedVols, 
		&BI_KEY(PAGE_BUF, bufIdx)) || !(volumeLockMode == L_X || volumeLockMode == L_IX || volumeLockMode == L_SIX)) {
#else
	    if (!bfm_CheckBufPageInMountedVolume(handle, BFM_PER_THREAD_DS(handle).mountedVolNos, BFM_PER_THREAD_DS(handle).nMountedVols, 
		&BI_KEY(PAGE_BUF, bufIdx))) {
#endif
	        if (prevIdx == NIL) 
		    BI_HASHTABLEENTRY(PAGE_BUF, i) = bufIdx;
	    	else
		    BI_NEXTHASHENTRY(PAGE_BUF, prevIdx) = bufIdx;
		prevIdx = bufIdx;
	    }
	    bufIdx = BI_NEXTHASHENTRY(PAGE_BUF, bufIdx);
	}
#ifdef USE_MUTEX
        /* Release the latch. */
        ERROR_PASS(handle, MUTEX_UNLOCK(&hashEntryPtr->mutex));
#else
		/* Release the latch. */
        ERROR_PASS(handle, bfm_ReleaseLatch(handle, &hashEntryPtr->latch, procIndex));
#endif
#else
	BI_HASHTABLEENTRY(PAGE_BUF, i) = NIL; 
#endif
    }

    tableSize = HASHTABLESIZE(LOT_LEAF_BUF);
    for(i = 0; i < tableSize; i++) {

#ifdef USE_SHARED_MEMORY_BUFFER	
	/* 
	 * If use shared memory buffer, must delete hashtable entry which has buffer index of buffer page in mounted volumes
	 * because pages in not-mounted volume may be in shared memory buffer.
	 */
	
	/* Get latch to prevent updating the hash chain. */
	hashEntryPtr = &(((HashTable*)PHYSICAL_PTR(BI_HASHTABLE(LOT_LEAF_BUF)))[i]); 
#ifdef USE_MUTEX
        ERROR_PASS(handle, MUTEX_LOCK(&hashEntryPtr->mutex));
#else
        ERROR_PASS(handle, bfm_GetLatch(handle, &hashEntryPtr->latch, procIndex, M_EXCLUSIVE, M_UNCONDITIONAL, NULL));
#endif

	prevIdx = NIL;
	bufIdx  = BI_HASHTABLEENTRY(LOT_LEAF_BUF, i);
	BI_HASHTABLEENTRY(LOT_LEAF_BUF, i) = NIL;
	while (bufIdx != NIL) {
#ifdef DBLOCK
	    volumeLockMode = RDsM_GetVolumeLockMode(handle, BI_KEY(LOT_LEAF_BUF, bufIdx).volNo); 
	    if (!bfm_CheckBufPageInMountedVolume(handle, BFM_PER_THREAD_DS(handle).mountedVolNos, BFM_PER_THREAD_DS(handle).nMountedVols, 
		&BI_KEY(LOT_LEAF_BUF, bufIdx)) || !(volumeLockMode == L_X || volumeLockMode == L_IX || volumeLockMode == L_SIX)) {
#else
	    if (!bfm_CheckBufPageInMountedVolume(handle, BFM_PER_THREAD_DS(handle).mountedVolNos, BFM_PER_THREAD_DS(handle).nMountedVols, 
		&BI_KEY(LOT_LEAF_BUF, bufIdx))) {
#endif
	        if (prevIdx == NIL) 
		    BI_HASHTABLEENTRY(LOT_LEAF_BUF, i) = bufIdx;
	    	else
		    BI_NEXTHASHENTRY(LOT_LEAF_BUF, prevIdx) = bufIdx;
		prevIdx = bufIdx;
	    }
	    bufIdx = BI_NEXTHASHENTRY(LOT_LEAF_BUF, bufIdx);
	}
#ifdef USE_MUTEX
        /* Release the latch. */
        ERROR_PASS(handle, MUTEX_UNLOCK(&hashEntryPtr->mutex));
#else
	/* Release the latch. */
        ERROR_PASS(handle, bfm_ReleaseLatch(handle, &hashEntryPtr->latch, procIndex));
#endif
#else
	BI_HASHTABLEENTRY(LOT_LEAF_BUF, i) = NIL; 
#endif
    }


    return(eNOERROR);
} /* bfm_DeleteAll() */ 
