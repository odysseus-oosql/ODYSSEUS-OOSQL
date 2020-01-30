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
 * Module: bfm_lock.c
 *
 * Description: simple lock implementation.
 *   bfm_lock : request manual duration lock for a given TrainID
 *              in exclusive mode
 *   bfm_unlock : release lock for given TrainID
 *   bfm_initLockHashTable : initialize lock structure
 *
 * Exports:
 *  Four bfm_lock(Four, TrainID *, Four)
 *  Four bfm_unlock(Four, TrainID *, Four)
 *  Four bfm_initLockHashTable(Four, Four)
 */


#include <stdlib.h>
#include "common.h"
#include "error.h"
#include "trace.h"
#include "BfM.h"
#include "SHM.h"
#include "perProcessDS.h"
#include "perThreadDS.h"


#define BFM_LOCKHASH(k, t) (((k)->volNo + (k)->pageNo)%BFM_LOCKHASHTABLESIZE(t))


#ifndef SINGLE_USER

/*@================================
 * bfm_lock( )
 *================================*/
/*
 * Function: Four bfm_lock(Four, TrainID *, Four)
 *
 * Description:
 *   request manual duration lock for a given trainID in exclusive mode
 *
 * Return values:
 *  Error codes
 *      function call errors
 *
 */
Four bfm_lock(
    Four 		handle,
    TrainID 		*key,			/* IN a train identifier */
    Four 		type)			/* IN buffer type */
{
    Lock_hashEntry 	*hashEntryPtr; 		/* hash table entry holding lock_cb */
    Lock_ctrlBlock 	*lock_cb;	 	/* an Lock Control Block */
    Four 		e;			/* for returned error value */


    TR_PRINT(handle, TR_BFM, TR1,("bfm_lock( key=%P, type=%ld )", key, type));

    /*@ check input parameters */
    CHECKKEY(key);    /* check validity of key */

    hashEntryPtr = &BI_LOCKHASHENTRY(type, BFM_LOCKHASH(key, type));

    /*@ get latch */
    /* MUTEX begin : protect from updating the chain */
    e = SHM_getLatch(handle,  &hashEntryPtr->latch, procIndex, M_EXCLUSIVE, M_UNCONDITIONAL, NULL);
    if (e < eNOERROR) ERR(handle, e);

    /*@ find the existing lock of given key */
    lock_cb = PHYSICAL_PTR(hashEntryPtr->blockPtr);
    while ( lock_cb ) {

	if ( EQUALKEY(&lock_cb->key, key) ) {

	    /* lockCounter keeps this structure from deleting
	    ** although release hashEntryPtr->latch
	    */
	    lock_cb->lockCounter++;

	    /*@ release latch */
	    /* MUTEX end
	    ** release latch to avoid deadlock situation
	    */
	    e = SHM_releaseLatch(handle,  &hashEntryPtr->latch, procIndex);
	    if (e < eNOERROR) ERR(handle, e);

	    /*@ get latch */
	    /* NOTE :: return without releasing this latch */
	    e = SHM_getLatch(handle,  &lock_cb->latch, procIndex, M_EXCLUSIVE, M_UNCONDITIONAL, NULL); /* THINK */
	    if (e < eNOERROR) ERR(handle, e);

	    return(eNOERROR);
	}

	lock_cb = PHYSICAL_PTR(lock_cb->nextHashChain);
    }

    /* if no lock exists, allocate new */
    e = Util_getElementFromPool(handle, &BI_LOCK_CB_POOL(type), &lock_cb);
    if (e < eNOERROR) ERR(handle, e);

    /*@ fill and initialize the information */
    lock_cb->lockCounter = 1;
    lock_cb->key = *key;
    SHM_initLatch(handle, &lock_cb->latch);

    /* insert into hash chain */
    lock_cb->nextHashChain = hashEntryPtr->blockPtr;
    hashEntryPtr->blockPtr = LOGICAL_PTR(lock_cb);

    /*@ get latch */
    /* NOTE :: return without releasing this latch */
    e = SHM_getLatch(handle,  &lock_cb->latch, procIndex, M_EXCLUSIVE, M_UNCONDITIONAL, NULL);
    if (e < eNOERROR) ERR(handle, e);


    /*@ release latch */
    /* MUTEX end */
    e = SHM_releaseLatch(handle,  &hashEntryPtr->latch, procIndex);
    if (e < eNOERROR) ERR(handle, e);

    return( eNOERROR );

}  /* bfm_lock */



/*@================================
 * bfm_unlock( )
 *================================*/
/*
 * Function: Four bfm_unlock(Four, TrainID *, Four)
 *
 * Description:
 *   release lock for given TrainID
 *
 * Return values:
 *  Error codes
 */
Four bfm_unlock(
    Four 		handle,
    TrainID 		*key,			/* IN a train identifier */
    Four 		type)			/* IN buffer type */
{
    Lock_hashEntry 	*hashEntryPtr; 		/* hash table entry holding lock_cb */
    Lock_ctrlBlock 	*lock_cb, *prev_cb; 	/* an Lock Control Block */
    Four 		e;		        /* for returned error value */


    TR_PRINT(handle, TR_BFM, TR1,("bfm_unlock( key=%P, type=%ld )", key, type));


    hashEntryPtr = &BI_LOCKHASHENTRY(type, BFM_LOCKHASH(key, type));

    /*@ get latch */
    /* MUTEX begin : protect from updating the chain */
    e = SHM_getLatch(handle,  &hashEntryPtr->latch, procIndex, M_EXCLUSIVE, M_UNCONDITIONAL, NULL);
    if ( e < eNOERROR ) ERR(handle, e);

    /*@ find the existing lock of given key */
    prev_cb = NULL;
    lock_cb = PHYSICAL_PTR(hashEntryPtr->blockPtr);
    while ( lock_cb ) {

	if ( EQUALKEY(&lock_cb->key, key) ) {

	    lock_cb->lockCounter--;

	    /*@ release latch */
	    e = SHM_releaseLatch(handle,  &lock_cb->latch, procIndex);
	    if ( e < eNOERROR ) ERR(handle, e);

	    /* if no lock on this pageid exists, deallocate lock_cb */
	    if (lock_cb->lockCounter == 0) {

		/* update hash chin before freeing the lock_cb */
		if (prev_cb)
		    prev_cb->nextHashChain = lock_cb->nextHashChain;
		else
		    hashEntryPtr->blockPtr = lock_cb->nextHashChain;

		e = Util_freeElementToPool(handle, &BI_LOCK_CB_POOL(type), lock_cb);
		if ( e < eNOERROR ) ERR(handle, e);

	    }

	    /*@ release latch */
	    /* MUTEX end */
	    e = SHM_releaseLatch(handle,  &hashEntryPtr->latch, procIndex);
	    if ( e < eNOERROR ) ERR(handle, e);

	    return(eNOERROR);
	}

	prev_cb = lock_cb;
	lock_cb = PHYSICAL_PTR(lock_cb->nextHashChain);
    }

    /*@ release latch */
    /* MUTEX end */
    e = SHM_releaseLatch(handle,  &hashEntryPtr->latch, procIndex);
    if ( e < eNOERROR ) ERR(handle, e);

    return( eNOSUCHLOCKEXIST_BFM );

}  /* bfm_unlock */

#endif /* SINGLE_USER */



/*@================================
 * bfm_initLockHashTable( )
 *================================*/
/*
 * Function: Four bfm_initLockHashTable(Four, Four)
 *
 * Description:
 *  We allocate memory for the hash table and lock control block pool.
 *  Initialize the Lock hash table as each entry has NIL value.
 *
 * Return values:
 *  Error codes
 *    function call errors
 */

Four bfm_initLockHashTable(
    Four 		handle,
    Four 		type)			/* IN buffer type */
{
    Four 		e;			/* returned error */
    Four 		i;			/* index variable */
    Lock_hashEntry 	*hashEntryPtr; 		/* hash table entry */
    void 		*physical_ptr;


    TR_PRINT(handle, TR_BFM, TR1, ("bfm_initLockHashTable( type=%ld )", type));

    /* allocate BfM Lock Hash Table */
    e = SHM_alloc(handle, sizeof(Lock_hashEntry)*BFM_LOCKHASHTABLESIZE(type),
		  -1, (char **)&physical_ptr);
    BI_LOCKHASHTABLE(type) = LOGICAL_PTR(physical_ptr);
    if ( e < eNOERROR )	ERR(handle, e);

    /*@ initialize the pool */
    /* allocate and initialize pool of BfM lock control block */
    e = Util_initPool(handle, &BI_LOCK_CB_POOL(type), sizeof(Lock_ctrlBlock),
		      BFM_LOCK_CB_POOLSIZE(type));
    if ( e < eNOERROR ) ERR(handle, e);

    /*@ initialize the talbe */
    /* initialize BfM Lock Hash Table */
    for( hashEntryPtr = PHYSICAL_PTR(BI_LOCKHASHTABLE(type)), i = 0;
	i < BFM_LOCKHASHTABLESIZE(type); hashEntryPtr++, i++ ) {

	hashEntryPtr->blockPtr = LOGICAL_PTR(NULL);
	SHM_initLatch(handle, &hashEntryPtr->latch);
    }

    return(eNOERROR);

}  /* bfm_initLockHashTable */
