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
 * Module: bfm_allocBuffer.c
 *
 * Description :
 *  Allocate a new buffer from the buffer pool.
 *
 * Exports:
 *  Four bfm_allocTrain(Four)
 */


#include "common.h"
#include "error.h"
#include "trace.h"
#include "latch.h"
#include "SHM.h"
#include "BfM.h"
#include "perProcessDS.h"
#include "perThreadDS.h"


/*@ macros */
#define DELETE_FROM_LRU_CHAIN(victim, type) \
        if ( (victim) == PHYSICAL_PTR(BI_LRU_ENTRY(type)) )\
	    BI_LRU_ENTRY(type) = (victim)->next_in_LRU;\
	if ( (victim) == PHYSICAL_PTR(BI_MRU_ENTRY(type)) )\
	    BI_MRU_ENTRY(type) = (victim)->prev_in_LRU;\
	if( PHYSICAL_PTR((victim)->prev_in_LRU) )\
	    ((BufTBLEntry*)PHYSICAL_PTR((victim)->prev_in_LRU))->next_in_LRU = (victim)->next_in_LRU;\
	if ( PHYSICAL_PTR((victim)->next_in_LRU) )\
	    ((BufTBLEntry*)PHYSICAL_PTR((victim)->next_in_LRU))->prev_in_LRU = (victim)->prev_in_LRU;\
	(victim)->prev_in_LRU = (victim)->next_in_LRU = LOGICAL_PTR(NULL);



/*@================================
 * bfm_allocBuffer( )
 *================================*/
/*
 * Function: bfm_allocBuffer(handle, Four, BufTBLEntry**)
 *
 * Description :
 *  Allocate a new buffer from the buffer pool.
 *  The used buffer pool is specified by the parameter 'type'.
 *  This routine construct Least Recently Used  Chain
 *  to select a victim.  From the first lru_entry check if its fixed vlaue
 *  is zero. If its value is zero then get the latch for this entry
 *  and delete it from LRU chain.
 *  Before return the buffer, if the dirty bit of the victim is set, it
 *  must be force out to the disk.
 *
 * Returns :
 *  1) An entry of a new buffer from the buffer pool
 *  2) Error codes: Negative value means error code.
 *     eNOUNFIXEDBUF_BFM - There is no unfixed buffer.
 *     some errors caused by fuction calls
 */
Four bfm_allocBuffer(
    Four 		handle,
    Four 		type,			/* IN type of buffer (PAGE or TRAIN) */
    BufTBLEntry 	**victimEntry)		/* OUT a Buffer Table Entry to be victim  */
{
    Four 		e;			/* for error */
    BfMHashKey 		localKey;		/* local key to handle replaced entry */


    TR_PRINT(handle, TR_BFM, TR1, ("bfm_allocBuffer(type=%ld, victimEntry=%p)", type, victimEntry));


    /*@ get latch */
    /* Mutex I Begin :: for updating LRU chain */
    ERROR_PASS(handle, SHM_getLatch(handle,  &BI_LRU_LATCH(type), procIndex, M_EXCLUSIVE, M_UNCONDITIONAL, NULL));

    *victimEntry = PHYSICAL_PTR(BI_LRU_ENTRY(type));


    /*@ repeat while victimEntry is not NULL */
    while (*victimEntry) {


	if ( (*victimEntry)->fixed < 1 ) {

	    /* at first, search the unused entry */
	    if(IS_NILBFMHASHKEY((*victimEntry)->key)) {

		/* just only delete victim from LRU chain */
		DELETE_FROM_LRU_CHAIN(*victimEntry, type);

		/*@ release latch */
		/* Mutex I End :: for updating LRU chain */
		ERROR_PASS(handle, SHM_releaseLatch(handle, &BI_LRU_LATCH(type), procIndex));

		return(eNOERROR);

	    }

	    localKey = (*victimEntry)->key;
	    ERROR_PASS(handle, bfm_lock(handle, (TrainID *)&localKey, type));

	    if ((*victimEntry)->fixed > 0) {
		ERROR_PASS(handle, bfm_unlock(handle, (TrainID *)&localKey, type));
	    }
	    else {

		if(IS_NILBFMHASHKEY((*victimEntry)->key)) {

		    ERROR_PASS(handle, bfm_unlock(handle, (TrainID *)&localKey, type));

		    /* just only delete victim from LRU chain */
		    DELETE_FROM_LRU_CHAIN(*victimEntry, type);

		    /*@ release latch */
		    /* Mutex I End :: for updating LRU chain */
		    ERROR_PASS(handle, SHM_releaseLatch(handle, &BI_LRU_LATCH(type), procIndex));

		    return(eNOERROR);

		}

		/* as soon as possible try to release LRU_LATCH */
		/* delete from LRU chain */
		DELETE_FROM_LRU_CHAIN(*victimEntry, type);

		/*@ release latch */
		/* Mutex I End :: for updating LRU chain */
		ERROR_PASS(handle, SHM_releaseLatch(handle, &BI_LRU_LATCH(type), procIndex));
		/* escape from the loop - because it finds the victim */
		break;
	    }

	}   /* fixed <= 0 */

	*victimEntry = PHYSICAL_PTR((*victimEntry)->next_in_LRU);

    }	/*  while loop */

    if (*victimEntry == NULL)
	ERRL1(handle, eNOUNFIXEDBUF_BFM, &BI_LRU_LATCH(type));

    /* Delete the victim from the hash table */
    e = bfm_delete(handle,  *victimEntry, type );
    if( e < 0 ) {
	ERROR_PASS(handle, bfm_unlock(handle, (TrainID *)&localKey, type));
	ERR(handle,  e );
    }

    /*@ if the dirty bit is set, force out to the disk */
    if( (*victimEntry)->dirtyFlag )  {
	e = bfm_flushBuffer(handle, *victimEntry, type);
	if( e < 0 ) {
	    ERROR_PASS(handle, bfm_unlock(handle, (TrainID *)&localKey, type));
	    ERR(handle,  e );
	}
    }

    /*@ hash key is initialized to NIL value. */
    SET_NILBFMHASHKEY((*victimEntry)->key);

    ERROR_PASS(handle, bfm_unlock(handle, (TrainID *)&localKey, type));

    /* by victimEntry parameter, return victim */
    return(eNOERROR);

}  /* bfm_allocBuffer */

