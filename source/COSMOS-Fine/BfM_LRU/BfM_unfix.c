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
 * Module: BfM_unfixBuffer.c
 *
 * Description :
 *  1. This function frees a buffer by decrementing
 *     the fix count by 1.
 *  2. If the dirtyFlag is on, set the dirty bit of
 *     an entry in the buffer table.
 *
 * Exports:
 *  Four BfM_unfixBuffer(Four, TrainID *, Four, Four)
 */


#include "common.h"
#include "error.h"
#include "trace.h"
#include "latch.h"
#include "SHM.h"
#include "BfM.h"
#include "perProcessDS.h"
#include "perThreadDS.h"



/*@================================
 * BfM_unfixBuffer( )
 *================================*/
/*
 * Function: BfM_unfixBuffer(handle, Buffer_ACC_CB*, Four)
 *
 * Description :
 *  1. This function frees a buffer by decrementing the fix count by 1.
 *  2. If the dirtyFlag is on, set the dirty bit of an entry in the buffer table.
 *
 * Returns :
 *  error codes
 *    eBADBUFFERTYPE_BFM - bad buffer type
 *    some errors caused by fuction calls
 */
Four BfM_unfixBuffer(
    Four 		handle,
    Buffer_ACC_CB  	*acc_cb,	/* IN buffer control block to be accessed */
    Four     		type)		/* IN buffer type */
{
    Four 		e;		/* error returned */
    BufTBLEntry 	*anEntry;	/* Buffer Table Entry to be freed */

    /* pointer for BfM Data Structure of perThreadTable */
    BfM_PerThreadDS_T *bfm_perThreadDSptr = BfM_PER_THREAD_DS_PTR(handle);

    TR_PRINT(handle, TR_BFM, TR1,
	     ("BfM_unfixBuffer(handle,  acc_cb->key=%P, acc_cb->dirtyFlag=%ld type=%ld )", acc_cb->key, acc_cb->dirtyFlag, type));


    /*@ check if the parameter is valid. */
    if (!acc_cb)
	ERR(handle, eNULLBUFACCESSCB_BFM);

    if (IS_BAD_BUFFERTYPE(type))
	ERR(handle, eBADBUFFERTYPE_BFM);


    /*@ get latch */
    /* Mutex Begin : mutual exclusively fix the trainID page */
    ERROR_PASS(handle, bfm_lock(handle, (TrainID *)&acc_cb->key, type));


    /* Check whether the page exist in the buffer pool */
    e = bfm_lookUp(handle,  (BfMHashKey *)&acc_cb->key, type, &anEntry);
    if( e == NOTFOUND_IN_HTABLE )  {
	ERROR_PASS(handle, bfm_unlock(handle, (TrainID *)&acc_cb->key, type));
	ERR(handle,  eNOTFOUND_BFM );
    }
    else if ( e < eNOERROR ) {
	ERROR_PASS(handle, bfm_unlock(handle, (TrainID *)&acc_cb->key, type));
	ERR(handle, e);
    }

    /* update Buffer Table Entry information upon acc_cb */
    if ( acc_cb->dirtyFlag )
	anEntry->dirtyFlag = TRUE;

    if ( acc_cb->invalidFlag )
	anEntry->invalidFlag = TRUE;

    if( anEntry->fixed <= 0 ) {
#ifdef TRACE
	printf("fixed counter is less than 0!!!\n");
	PRINT_TRAINID("trainId", &acc_cb->key);
#endif /* TRACE */
	anEntry->fixed = 0;
    }

    if ( anEntry->fixed == 1 ) {

	/*@ get latch */
	/* MUTEX  begin : to update LRU chain */
	e = SHM_getLatch(handle, &BI_LRU_LATCH(type), procIndex,
			 M_EXCLUSIVE, M_CONDITIONAL, NULL);
	if ( e < eNOERROR ) {
	    ERROR_PASS(handle, bfm_unlock(handle, (TrainID *)&acc_cb->key, type));
	    ERR(handle, e);
	}

	if ( e == SHM_BUSYLATCH ) {
	    ERROR_PASS(handle, bfm_unlock(handle, (TrainID *)&acc_cb->key, type));

	    /*@ get latch */
	    e = SHM_getLatch(handle, &BI_LRU_LATCH(type), procIndex,
			     M_EXCLUSIVE, M_UNCONDITIONAL, NULL);

	    if ( e < eNOERROR ) ERR(handle, e);

	    ERROR_PASS(handle, bfm_lock(handle, (TrainID *)&acc_cb->key, type));
	}

	/*@ decrease the counter */
        /* really unfix operation... decrease the fix counter */
	anEntry->fixed--;

        /* Check the fix counter
         * since other transaction might fix this entry while this transaction
         * requested the LRU_LATCH unconditionally
         */
	if ( anEntry->fixed == 0 &&  anEntry != PHYSICAL_PTR(BI_MRU_ENTRY(type)) ) {
	    /* if anEntry exists in LRU chain, delete from LRU chain */
	    if ( anEntry == PHYSICAL_PTR(BI_LRU_ENTRY(type)) )
		BI_LRU_ENTRY(type) = anEntry->next_in_LRU;
	    if (PHYSICAL_PTR(anEntry->prev_in_LRU))
		((BufTBLEntry*)PHYSICAL_PTR(anEntry->prev_in_LRU))->next_in_LRU = anEntry->next_in_LRU;
	    if (PHYSICAL_PTR(anEntry->next_in_LRU))
		((BufTBLEntry*)PHYSICAL_PTR(anEntry->next_in_LRU))->prev_in_LRU = anEntry->prev_in_LRU;

	    /*@ insert entry */
	    /* insert anEntry as MRU_ENTRY */
	    anEntry->next_in_LRU = LOGICAL_PTR(NULL);
	    anEntry->prev_in_LRU = BI_MRU_ENTRY(type);
	    ((BufTBLEntry*)PHYSICAL_PTR(BI_MRU_ENTRY(type)))->next_in_LRU = LOGICAL_PTR(anEntry);
	    BI_MRU_ENTRY(type) = LOGICAL_PTR(anEntry);

	    if ( PHYSICAL_PTR(BI_LRU_ENTRY(type)) == NULL )
		BI_LRU_ENTRY(type) = LOGICAL_PTR(anEntry);
	}

	/*@ release latch */
	/* MUTEX  end : to update LRU chain */
	e = SHM_releaseLatch(handle, &BI_LRU_LATCH(type), procIndex);
	if ( e < eNOERROR ) {
	    ERROR_PASS(handle, bfm_unlock(handle, (TrainID *)&acc_cb->key, type));
	    ERR(handle, e);
	}

    }
    else     anEntry->fixed--;    /* really unfix operation... decrease the fix counter */ /*@ decrease the counter */

    ERROR_PASS(handle, bfm_unlock(handle, (TrainID *)&acc_cb->key, type));

    DELETE_BACB_FROM_MYFIXEDBACB(acc_cb);

    e = Util_freeElementToLocalPool(handle, &(bfm_perThreadDSptr->BACB_pool), acc_cb);
    if ( e < eNOERROR )
	ERR(handle, e);

    return( eNOERROR );

} /* Bfm_unfixBuffer */
