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
 * Module: bfm_hash.c
 *
 * Description:
 *  Simple hashing functions are provided.
 *  Each BfMHashKey is mapping to one table entry in a hash table(hTable),
 *  and each entry has a chain of buffer table entry
 *  which indicates a buffer in a buffer pool.
 *  An ordinary hashing method is used and
 *  hash chain strategy is used if collision has occurred.
 *
 * CAUTION :: For Concurrency Control, these functions are called
 *             after bfm_lock(handle, &bufEntry->key, type) is called.
 *
 * Exports:
 *  Four bfm_insert(Four, BfMHashKey *, BufTBLEntry *, Four)
 *  Four bfm_delete(Four, BufTBLEntry *, Four)
 *  Four bfm_lookUP(BfMHashKey *, Four, BufTBLEntry**)
 */


#include <stdlib.h>
#include "common.h"
#include "error.h"
#include "trace.h"
#include "BfM.h"
#include "SHM.h"
#include "perProcessDS.h"
#include "perThreadDS.h"



/*@================================
 * bfm_insert( )
 *================================*/
/*
 * Function: Four bfm_insert(Four, BfMHashKey *, BufTBLEntry *, Four)
 *
 * Description:
 *  Insert a new entry into the hash table.
 *  If collision occurs, then use the hash chain.
 *
 * Return values:
 *  Error codes
 *    eBABBUFINDEX_BFM - bad index value for buffer table
 */
Four bfm_insert(
    Four handle,
    BfMHashKey *key,		/* IN a hash key in Buffer Manager */
    BufTBLEntry *anEntry,	/* IN an inserted entry */
    Four type )			/* IN buffer type */
{

    bfmHashEntry *hashEntryPtr; /* hash table entry holding anEntry */
    Four e;			/* error returned */
    unsigned int  tempSeed;     /* temp seed */


    TR_PRINT(handle, TR_BFM, TR1, ("bfm_insert( key=%P, anEntry=%P, type=%ld )",
			   key, anEntry, type));

    /*@ check input parameters */
    CHECKKEY(key);    /* check validity of key */

    if ( !anEntry ) ERR(handle,  eBADBUFTBLENTRY_BFM );

    /* find corrsponding hash entry */
    hashEntryPtr = &BI_HASHENTRY(type, BFM_HASH(key, tempSeed, type));

    /*@ get latch */
    /* MUTEX begin : protect from updating the hash chain */
    e = SHM_getLatch(handle,  &hashEntryPtr->latch, procIndex,  M_EXCLUSIVE, M_UNCONDITIONAL, NULL);
    if ( e < eNOERROR ) ERR(handle, e);

    /* insert anEntry into hash chain */
    anEntry->nextHashChain = hashEntryPtr->entryPtr;
    hashEntryPtr->entryPtr = LOGICAL_PTR(anEntry); 

    /*@ release latch */
    /* MUTEX end */
    e = SHM_releaseLatch(handle,  &hashEntryPtr->latch, procIndex);
    if ( e < eNOERROR ) ERR(handle, e);

    return( eNOERROR );

}  /* bfm_insert */



/*@================================
 * bfm_delete( )
 *================================*/
/*
 * Function: Four bfm_delete(Four, BfMHashKey *, Four)
 *
 * Description:
 *  Look up the entry which corresponds to `key' and
 *  Delete the entry from the hash table.
 *
 * Return values:
 *  Error codes
 *    eNOTFOUND_BFM - The key isn't in the hash table.
 */
Four bfm_delete(
    Four handle,
    BufTBLEntry *anEntry,	/* IN a BufTBLEntry to be deleted from hash chain */
    Four type )			/* IN buffer type */
{
    bfmHashEntry *hashEntryPtr; /* hash table entry holding anEntry */
    BufTBLEntry *prevEntry;	/* temporary pointer */
    Four e;			/* error returned */
    unsigned int  tempSeed;     /* temp seed */


    TR_PRINT(handle, TR_BFM, TR1, ("bfm_delete( anEntry=%P, type=%ld )", anEntry, type));

    /*@ check input parameters */
    if ( !anEntry )
	ERR(handle, eBADBUFTBLENTRY_BFM);

    CHECKKEY(&anEntry->key);    /* check validity of key */

    /* @ find corresponding hash entry */
    hashEntryPtr = &BI_HASHENTRY(type, BFM_HASH(&anEntry->key, tempSeed, type));

    /*@ get latch */
    /* MUTEX begin : protect from updating the hash chain */
    e = SHM_getLatch(handle,  &hashEntryPtr->latch, procIndex, M_EXCLUSIVE, M_UNCONDITIONAL, NULL);

    if ( e < eNOERROR ) ERR(handle, e);

    /*
     * search corresponding entry and delete from chain
     */
    prevEntry = PHYSICAL_PTR(hashEntryPtr->entryPtr);

    if ( prevEntry == anEntry )
	hashEntryPtr->entryPtr = anEntry->nextHashChain;

    else {

	while ( prevEntry && PHYSICAL_PTR(prevEntry->nextHashChain) &&
		PHYSICAL_PTR(prevEntry->nextHashChain) != anEntry) 
	    prevEntry = PHYSICAL_PTR(prevEntry->nextHashChain);

	/* if not found, release latch and return error */
	if ( !prevEntry || !PHYSICAL_PTR(prevEntry->nextHashChain) ) {

	    /*@ release latch */
	    e = SHM_releaseLatch(handle,  &hashEntryPtr->latch, procIndex);
	    if ( e < eNOERROR ) ERR(handle, e);

	    ERR(handle, eNOTFOUND_BFM);
	}

	/* delete from hash chain */
	prevEntry->nextHashChain = anEntry->nextHashChain;
    }

    /*@ release latch */
    /* MUTEX end */
    e = SHM_releaseLatch(handle,  &hashEntryPtr->latch, procIndex);
    if ( e < eNOERROR ) ERR(handle, e);

    return(eNOERROR);

}  /* bfm_delete */



/*@================================
 * bfm_lookUp( )
 *================================*/
/*
 * Function: Four bfm_lookUp(Four, BfMHashKey *, Four)
 *
 * Description:
 *  Look up the given key in the hash table and return its
 *  corressponding entry in the buffer table.
 *
 * Retrun values:
 *  index on buffer table entry holding the train specified by 'key'
 *  (NOTFOUND_IN_HTABLE - The key don't exist in the hash table.)
 */
Four bfm_lookUp(
    Four handle,
    BfMHashKey *lookupedKey,	/* IN a hash key to be lookuped in Buffer Manager */
    Four type,			/* IN buffer type */
    BufTBLEntry **anEntry)	/* OUT searched anEntry */
{
    bfmHashEntry *hashEntryPtr; /* hash table entry holding anEntry */
    Four  e;			/* error returned */
    unsigned int  tempSeed;     /* temp seed */

    TR_PRINT(handle, TR_BFM, TR1, ("bfm_LookUp( key=%P, type=%ld )", lookupedKey, type));

    /*@ check input parameters */

    CHECKKEY(lookupedKey);    /* check validity of key */

    /* @ find corresponding hash entry */
    hashEntryPtr = &BI_HASHENTRY(type, BFM_HASH(lookupedKey, tempSeed, type));

    /* get latch */
    /* MUTEX begin : protect from updating the chain */
    e = SHM_getLatch(handle,  &hashEntryPtr->latch, procIndex, M_SHARED, M_UNCONDITIONAL, NULL);
    if ( e < eNOERROR ) ERR(handle, e);

    /* search corresponding buffer table entry in hash chain */
    *anEntry = PHYSICAL_PTR(hashEntryPtr->entryPtr); 
    while ( *anEntry && !EQUALKEY(&((*anEntry)->key), lookupedKey) )
	*anEntry = PHYSICAL_PTR((*anEntry)->nextHashChain); 

    /*@ relese latch */
    /* MUTEX end : proctect from updating the chain */
    e = SHM_releaseLatch(handle,  &hashEntryPtr->latch, procIndex);
    if ( e < eNOERROR ) ERR(handle, e);

    /* the key does not exist in the table */
    if ( !(*anEntry) )  {
#ifdef TRACE
	Four i;

	for (i = 0, (*anEntry) = &BI_BTENTRY(type, 0); i < BI_NBUFS(type); i++, (*anEntry)++)
	    if (EQUALKEY(&(*anEntry)->key, lookupedKey)) {
		e = bfm_lock(handle, (TrainID*)&(*anEntry)->key, type);
		if (e < eNOERROR) ERR(handle, e);

		if (!EQUALKEY(&(*anEntry)->key, lookupedKey)) {
		    e = bfm_unlock(handle, (TrainID*)&(*anEntry)->key, type);
		    if (e < eNOERROR) ERR(handle, e);
		    break;
		}
		printf("WAITING ERROR compared pageNo %ld\n", (*anEntry)->key.pageNo);

	    }

#endif
	return(NOTFOUND_IN_HTABLE);
    }

    return (eNOERROR);

}  /* bfm_lookUp */



/*@================================
 * bfm_initBufferHashTable( )
 *================================*/
/*
 * Function: Four bfm_initBufferHashTable(Four, Four)
 *
 * Description:
 *  Initialize the buffer hash table as each entry has NIL value.
 *  At first, we allocate memory for the hash table.
 *
 * Return values:
 *  Error codes
 *     function call erros.
 */
Four bfm_initBufferHashTable(
    Four handle,
    Four type)			/* IN buffer type */
{

    Four e;			/* returned error */
    Four i;			/* index variable */
    bfmHashEntry *hashEntryPtr; /* hash table entry holding anEntry */
    void *physical_ptr; 

    TR_PRINT(handle, TR_BFM, TR1, ("bfm_initBufferHashTable( type=%ld )", type));

    e = SHM_alloc(handle, sizeof(bfmHashEntry)*HASHTABLESIZE(type),
		  -1, (char **)&physical_ptr);
    BI_HASHTABLE(type) = LOGICAL_PTR(physical_ptr);
    if ( e < eNOERROR ) ERR(handle, e);

    for( hashEntryPtr = PHYSICAL_PTR(BI_HASHTABLE(type)), i = 0;
        i < HASHTABLESIZE(type); hashEntryPtr++, i++ ) { 

	hashEntryPtr->entryPtr = LOGICAL_PTR(NULL);
	SHM_initLatch(handle, &hashEntryPtr->latch);
    }
    return(eNOERROR);

}  /* bfm_initBufferHashTable */


