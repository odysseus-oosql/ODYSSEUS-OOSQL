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
 * Module: bfm_Lock.c
 *
 * Description: simple lock implementation.
 *   bfm_Lock : request manual duration lock for a given TrainID
 *              in exclusive mode
 *   bfm_Unlock : release lock for given TrainID
 *   bfm_InitLockHashTable : initialize lock structure
 *   bfm_InitLockCBTable : initialize lock control block table
 *   bfm_AllocMyLockCBs : allocate lock control blocks for this process
 *   bfm_FreeMyLockCBs : free lock control blocks for this process
 *
 * Note: This bfm_Lock.c is taken from COSMOS-CC. 
 *
 * Exports:
 *  Four bfm_Lock(TrainID *, Four)
 *  Four bfm_Unlock(TrainID *, Four)
 *  Four bfm_InitLockHashTable(Four)
 *  Four bfm_InitLockCBTable(Four)
 *  Four bfm_AllocMyLockCBs(Four)
 *  Four bfm_FreeMyLockCBs(Four)
 */

#ifdef USE_SHARED_MEMORY_BUFFER

#include <stdio.h>
#include <stdlib.h>
#include "common.h"
#include "error.h"
#include "BfM_Internal.h"
#include "perThreadDS_Internal.h"
#include "perProcessDS.h"

#define BFM_LOCKHASH(k, t) (((k)->volNo + (k)->pageNo) % LOCKHASHTABLESIZE(t))

/*@================================
 * bfm_Lock( )
 *================================*/
/*
 * Function: Four bfm_Lock(TrainID *, Four)
 *
 * Description:
 *   request manual duration lock for a given trainID in exclusive mode
 *
 * Return values:
 *  Error codes
 *      function call errors
 *
 */
Four bfm_Lock(
    Four handle,
    TrainID 	    *key,		/* IN a train identifier */
    Four 	   		type		/* IN buffer type */
)
{

    Lock_hashEntry 		*hashEntryPtr; 	/* hash table entry holding lock_cb */
    Lock_ctrlBlock 		*lock_cb;	 	/* an Lock Control Block */
    Lock_ctrlBlock 		*temp_lock_cb;	/* an Lock Control Block */ 
    Four 	   			e;				/* for returned error value */

    /*@ check input parameters */
    CHECKKEY(key);    /* check validity of key */

    hashEntryPtr = &BI_LOCKHASHENTRY(type, BFM_LOCKHASH(key, type));

    /*@ get latch */
#ifdef USE_MUTEX
    /* MUTEX begin : protect from updating the chain */
    ERROR_PASS(handle, MUTEX_LOCK(&hashEntryPtr->mutex));
#else
    /* MUTEX begin : protect from updating the chain */
    e = bfm_GetLatch(handle, &hashEntryPtr->latch, procIndex, M_EXCLUSIVE, M_UNCONDITIONAL, NULL);
    if (e < eNOERROR) ERR(handle, e);
#endif

    /*@ find the existing lock of given key */
    lock_cb = PHYSICAL_PTR(hashEntryPtr->blockPtr); 
    while ( lock_cb ) {

	if ( EQUALKEY(&lock_cb->key, key) ) {

	    /* lockCounter keeps this structure from deleting
	    ** although release hashEntryPtr->latch
	    */
	    lock_cb->lockCounter++;

	    temp_lock_cb = BFM_PER_THREAD_DS(handle).myLCB;
	    BFM_PER_THREAD_DS(handle).myLCB = PHYSICAL_PTR(temp_lock_cb->nextHashChain);
	    temp_lock_cb->nextHashChain = hashEntryPtr->freeBlockPtr;
	    hashEntryPtr->freeBlockPtr = LOGICAL_PTR(temp_lock_cb);

	    /*@ release latch */
	    /* MUTEX end
	    ** release latch to avoid deadlock situation
	    */
#ifdef USE_MUTEX
        ERROR_PASS(handle, MUTEX_UNLOCK(&hashEntryPtr->mutex));
#else
	    e = bfm_ReleaseLatch(handle, &hashEntryPtr->latch, procIndex);
	    if (e < eNOERROR) ERR(handle, e);
#endif
	    /*@ get latch */
	    /* NOTE :: return without releasing this latch */
#ifdef USE_MUTEX
            ERROR_PASS(handle, MUTEX_LOCK(&lock_cb->mutex));
#else
	    e = bfm_GetLatch(handle, &lock_cb->latch, procIndex, M_EXCLUSIVE, M_UNCONDITIONAL, NULL); /* THINK */
	    if (e < eNOERROR) ERR(handle, e);
#endif
	    return(eNOERROR);
	}

	lock_cb = PHYSICAL_PTR(lock_cb->nextHashChain); 
    }

    /* if no lock exists, allocate new */
    lock_cb = BFM_PER_THREAD_DS(handle).myLCB;
    BFM_PER_THREAD_DS(handle).myLCB = PHYSICAL_PTR(BFM_PER_THREAD_DS(handle).myLCB->nextHashChain);

    /*@ fill and initialize the information */
    lock_cb->lockCounter = 1;
    lock_cb->key = *key;

    /* insert into hash chain */
    lock_cb->nextHashChain = hashEntryPtr->blockPtr;
    hashEntryPtr->blockPtr = LOGICAL_PTR(lock_cb); 

    /*@ get latch */
    /* NOTE :: return without releasing this latch */
#ifdef USE_MUTEX
    ERROR_PASS(handle, MUTEX_LOCK(&lock_cb->mutex));
#else
    e = bfm_GetLatch(handle, &lock_cb->latch, procIndex, M_EXCLUSIVE, M_UNCONDITIONAL, NULL);
    if (e < eNOERROR) ERR(handle, e);
#endif


    /*@ release latch */
    /* MUTEX end */
#ifdef USE_MUTEX
    ERROR_PASS(handle, MUTEX_UNLOCK(&hashEntryPtr->mutex));
#else
    e = bfm_ReleaseLatch(handle, &hashEntryPtr->latch, procIndex);
    if (e < eNOERROR) ERR(handle, e);
#endif

    return( eNOERROR );

}  /* bfm_Lock */


/*@================================
 * bfm_Unlock( )
 *================================*/
/*
 * Function: Four bfm_Unlock(TrainID *, Four)
 *
 * Description:
 *   release lock for given TrainID
 *
 * Return values:
 *  Error codes
 */
Four bfm_Unlock(
    Four handle,
    TrainID 	    *key,			/* IN a train identifier */
    Four 	   		type			/* IN buffer type */
)
{

    Lock_hashEntry 	*hashEntryPtr; 			/* hash table entry holding lock_cb */
    Lock_ctrlBlock 	*lock_cb, *prev_cb; 	/* an Lock Control Block */
    Lock_ctrlBlock 	*temp_lock_cb; 			/* an Lock Control Block */ 
    Four 	   		e;						/* for returned error value */

    hashEntryPtr = &BI_LOCKHASHENTRY(type, BFM_LOCKHASH(key, type));

    /*@ get latch */
    /* MUTEX begin : protect from updating the chain */
#ifdef USE_MUTEX
    ERROR_PASS(handle, MUTEX_LOCK(&hashEntryPtr->mutex));
#else
    e = bfm_GetLatch(handle, &hashEntryPtr->latch, procIndex, M_EXCLUSIVE, M_UNCONDITIONAL, NULL);
    if ( e < eNOERROR ) ERR(handle, e);
#endif

    /*@ find the existing lock of given key */
    prev_cb = NULL;
    lock_cb = PHYSICAL_PTR(hashEntryPtr->blockPtr); 
    while ( lock_cb ) {

	if ( EQUALKEY(&lock_cb->key, key) ) {

	    lock_cb->lockCounter--;

	    /*@ release latch */
#ifdef USE_MUTEX
            ERROR_PASS(handle, MUTEX_UNLOCK(&lock_cb->mutex));
#else
	    e = bfm_ReleaseLatch(handle, &lock_cb->latch, procIndex);
	    if ( e < eNOERROR ) ERR(handle, e);
#endif

	    /* if no lock on this pageid exists, deallocate lock_cb */
	    if (lock_cb->lockCounter == 0) {

		/* update hash chin before freeing the lock_cb */
		if (prev_cb)
		    prev_cb->nextHashChain = lock_cb->nextHashChain;
		else
		    hashEntryPtr->blockPtr = lock_cb->nextHashChain;

		lock_cb->nextHashChain = LOGICAL_PTR(BFM_PER_THREAD_DS(handle).myLCB);
    		BFM_PER_THREAD_DS(handle).myLCB = lock_cb;

	    }else {
		temp_lock_cb = PHYSICAL_PTR(hashEntryPtr->freeBlockPtr);
		hashEntryPtr->freeBlockPtr = temp_lock_cb->nextHashChain;
		temp_lock_cb->nextHashChain = LOGICAL_PTR(BFM_PER_THREAD_DS(handle).myLCB);
		BFM_PER_THREAD_DS(handle).myLCB = temp_lock_cb;
	    }
	    /*@ release latch */
	    /* MUTEX end */
#ifdef USE_MUTEX
            ERROR_PASS(handle, MUTEX_UNLOCK(&hashEntryPtr->mutex));
#else
	    e = bfm_ReleaseLatch(handle, &hashEntryPtr->latch, procIndex);
	    if ( e < eNOERROR ) ERR(handle, e);
#endif
	    return(eNOERROR);
	}

	prev_cb = lock_cb;
	lock_cb = PHYSICAL_PTR(lock_cb->nextHashChain); 
    }

    /*@ release latch */
    /* MUTEX end */
#ifdef USE_MUTEX
    ERROR_PASS(handle, MUTEX_UNLOCK(&hashEntryPtr->mutex));
#else
    e = bfm_ReleaseLatch(handle, &hashEntryPtr->latch, procIndex);
    if ( e < eNOERROR ) ERR(handle, e);
#endif

    return( eNOSUCHLOCKEXIST_BFM );


}  /* bfm_Unlock */


/*@================================
 * bfm_InitLockHashTable( )
 *================================*/
/*
 * Function: Four bfm_InitLockHashTable(Four)
 *
 * Description:
 *  We allocate memory for the hash table and lock control block pool.
 *  Initialize the Lock hash table as each entry has NIL value.
 *
 * Return values:
 *  Error codes
 *    function call errors
 */
Four bfm_InitLockHashTable(
    Four 		handle,
    Four		type)			/* IN buffer type */
{
    Four 			e;				/* returned error */
    Four 			i;				/* index variable */
    Lock_hashEntry 	*hashEntryPtr; 	/* hash table entry */

    e = bfm_InitLockCBTable(handle, type);
    if (e < eNOERROR) ERR(handle, e);

    /*@ initialize the talbe */
    /* initialize BfM Lock Hash Table */
	hashEntryPtr =((Lock_hashEntry*)PHYSICAL_PTR( BI_LOCKHASHTABLE(type))); 
    for(i = 0; i < LOCKHASHTABLESIZE(type); hashEntryPtr++, i++ ) { 
	hashEntryPtr->blockPtr = LOGICAL_PTR(NULL); 
	hashEntryPtr->freeBlockPtr = LOGICAL_PTR(NULL); 
#ifdef USE_MUTEX
        MUTEX_INIT(&hashEntryPtr->mutex, &cosmos_thread_mutex_attr, PTHREAD_PROCESS_SHARED);
#else
	bfm_InitLatch(handle, &hashEntryPtr->latch);
#endif
    }

    return(eNOERROR);
}  /* bfm_InitLockHashTable */

/*@================================
 * bfm_InitLockCBTable( )
 *================================*/
/*
 * Function: Four bfm_InitLockCBTable(Four)
 *
 * Description:
 *  Initialize lock control block table.
 *
 * Return values:
 *  Error codes
 *    function call errors
 */
Four bfm_InitLockCBTable(Four handle, Four type)
{
    Four i;	/* loop index */

    /*@
     * Initialize lock hash table.
     */
		
    for (i = 0; i < LOCK_CB_TABLESIZE(type) - 1; i++) {
	BI_LOCK_CB_ENTRY(type, i).nextHashChain = LOGICAL_PTR(&BI_LOCK_CB_ENTRY(type, i + 1));
    }
    BI_LOCK_CB_ENTRY(type, i).nextHashChain = LOGICAL_PTR(NULL);

    /*@
     * Initialize lock control block header.
     */
    BI_FREE_LOCKCBHEADER(type) = LOGICAL_PTR(&BI_LOCK_CB_ENTRY(type, 0));

    return (eNOERROR);
}

/*@================================
 * bfm_AllocMyLockCBs( )
 *================================*/
/*
 * Function: Four bfm_AllocMyLockCBs(Four)
 *
 * Description:
 *  Allocate lock control blocks for this process.
 *
 * Return values:
 *  Error codes
 *    function call errors
 */
Four bfm_AllocMyLockCBs(Four handle, Four type)
{
    Lock_ctrlBlock *lockCB1, *lockCB2;	/* lock control blocks for this process */

    /*@ 
     * Allocate first lock control block.
     */
    lockCB1 = PHYSICAL_PTR(BI_FREE_LOCKCBHEADER(type));
    if (lockCB1 == NULL) ERR(handle, eNOMORELOCKCONTROLBLOCKS_BFM);
    BI_FREE_LOCKCBHEADER(type) = lockCB1->nextHashChain;
#ifdef USE_MUTEX
    MUTEX_INIT(&lockCB1->mutex, &cosmos_thread_mutex_attr, PTHREAD_PROCESS_SHARED);
#else
    bfm_InitLatch(handle, &lockCB1->latch);
#endif
   
    /*@
     * Allocate second lock control block.
     */
    lockCB2 = PHYSICAL_PTR(BI_FREE_LOCKCBHEADER(type));
    if (lockCB2 == NULL) ERR(handle, eNOMORELOCKCONTROLBLOCKS_BFM);
    BI_FREE_LOCKCBHEADER(type) = lockCB2->nextHashChain;
#ifdef USE_MUTEX
    MUTEX_INIT(&lockCB2->mutex, &cosmos_thread_mutex_attr, PTHREAD_PROCESS_SHARED);
#else
    bfm_InitLatch(handle, &lockCB2->latch);
#endif
   
    /*@
     * Link first lock control block and second lock control block.
     */
    lockCB2->nextHashChain = LOGICAL_PTR(NULL);
    lockCB1->nextHashChain = LOGICAL_PTR(lockCB2);
    BFM_PER_THREAD_DS(handle).myLCB = lockCB1;

    /*@
     * Initialize two lock control blocks.
     */
    SET_NILPAGEID(lockCB1->key);
    lockCB1->lockCounter = 0;

    SET_NILPAGEID(lockCB2->key);
    lockCB2->lockCounter = 0;

    return (eNOERROR);
}

/*@================================
 * bfm_FreeMyLockCBs( )
 *================================*/
/*
 * Function: Four bfm_FreeMyLockCBs(Four)
 *
 * Description:
 *  Free lock control blocks for this process.
 *
 * Return values:
 *  Error codes
 *    function call errors
 */
Four bfm_FreeMyLockCBs(Four handle, Four type)
{
    Lock_ctrlBlock *lockCB1, *lockCB2;

    /*@
     * Point lock control blocks.
     */
    lockCB1 = BFM_PER_THREAD_DS(handle).myLCB;
    lockCB2 = PHYSICAL_PTR(BFM_PER_THREAD_DS(handle).myLCB->nextHashChain);

    /*@
     * Free lock control blocks.
     */
    lockCB2->nextHashChain = BI_FREE_LOCKCBHEADER(type);
    BI_FREE_LOCKCBHEADER(type) = LOGICAL_PTR(lockCB2);
    
    lockCB1->nextHashChain = BI_FREE_LOCKCBHEADER(type);
    BI_FREE_LOCKCBHEADER(type) = LOGICAL_PTR(lockCB1);
    
    /*@
     * Unlink pointer of lock control blocks
     */
    BFM_PER_THREAD_DS(handle).myLCB = NULL;

    return (eNOERROR);
}

#endif /* USE_SHARED_MEMORY_BUFFER */

