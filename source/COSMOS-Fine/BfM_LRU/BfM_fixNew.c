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
 * Module: BfM_fixNewBuffer .c
 *
 * Description :
 *  Return a buffer which has the disk content indicated by `trainId'.
 *
 * Exports:
 *  Four BfM_fixNewBuffer(Four, TrainID *, Four, Buffer_ACC_CB *, Four)
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
 * bfm_readBuffer( )
 *================================*/
/*
 * Function: Four BfM_fixNewBuffer(Four, TrainID *, Four, Buffer_ACC_CB *, Four)
 *
 * Description :
 *  Return a buffer which has the disk content indicated by `trainId'.
 *  Before the allocation of a buffer, look up the train in the buffer
 *  pool using hashing mechanism.   If the train already  exist in the pool
 *  then simply return it and increase the fixed counter of the correponding
 *  buffer table entry.   Otherwise, i.e. the train does not exist  in the
 *  pool,  allocate a buffer (a buffer selected as victim may be forced out
 *  by the buffer replacement algorithm) and return it.
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
Four BfM_fixNewBuffer(
    Four handle,
    TrainID *trainId,		/* IN train to be used */
    Four mode,			/* IN M_FREE, M_SHARED or M_EXCLUSIVE */
    Buffer_ACC_CB **acc_cb,	/* OUT pointer to the access control block */
    Four    type )		/* IN buffer type */
{
    BufTBLEntry *anEntry;	/* a buffer Table Entry */
    Four status;		/* for returned message */
    Four e;			/* for resource release error */


    /* pointer for BfM Data Structure of perThreadTable */
    BfM_PerThreadDS_T *bfm_perThreadDSptr = BfM_PER_THREAD_DS_PTR(handle);

    /* pointer for COMMON Data Structure of perThreadTable */
    COMMON_PerThreadDS_T *common_perThreadDSptr = COMMON_PER_THREAD_DS_PTR(handle);


    TR_PRINT(handle, TR_BFM, TR1,("BfM_fixNewBuffer(trainId=%P, acc_cb=%P, mode+%ld type=%ld)", trainId, acc_cb, mode, type));


    /*@ Check the validity of given parameters */

    /* Is the latch mode valid? */
    if (IS_BAD_LATCHMODE(mode))
	ERR(handle, eBADLATCHMODE_BFM);

    /* Is the buffer type valid? */
    if (IS_BAD_BUFFERTYPE(type))
	ERR(handle, eBADBUFFERTYPE_BFM);


    /*@ get latch */
    /* Mutex Begin : mutual exclusively fix the trainID page */
    ERROR_PASS(handle, bfm_lock(handle, trainId, type));


    /* Check whether the page exist in the buffer pool */
    status = bfm_lookUp(handle, (BfMHashKey *)trainId,type, &anEntry);

    if( status == NOTFOUND_IN_HTABLE ) {	/* Not Exist in the pool */

	/*@ allocate a buffer */
        /* Allocate a buffer from the buffer pool of type 'type' */
        e = bfm_allocBuffer(handle, type, &anEntry);

        if( e < 0 ) {  /* Buffer Allocation Error */
	    ERROR_PASS(handle, bfm_unlock(handle, trainId, type));
            ERR(handle,  e );
	}

	/* initialize before next use */
	anEntry->dirtyFlag = FALSE;
	anEntry->invalidFlag = FALSE;
	anEntry->fixed = 1;

        /* fill the buffer table using the given parameters */
 	anEntry->key = *((BfMHashKey *)trainId);

        /* Initialize the recovery LSN of the new buffer. */
        e = LOG_GetNextLogRecordLsn(handle, &anEntry->recLsn);
        if (e < 0) {
	    ERROR_PASS(handle, bfm_unlock(handle, trainId, type));
	    ERR(handle,  e );
        }

	/*@ insert the key */
	/* Insert the key into the hash table */
        e = bfm_insert(handle,  (BfMHashKey *)trainId, anEntry, type );
	if( e < 0 ) {
	    ERROR_PASS(handle, bfm_unlock(handle, trainId, type));
	    ERR(handle,  e );
	}

	/* Now, requested page is safely in the buffer pool */

    }
    else if (status  < eNOERROR) {
	ERROR_PASS(handle, bfm_unlock(handle, trainId, type));
	ERR(handle, status);
    }
    else
        anEntry->fixed++;       /* increase counter */


    /* Now, buffer page fixed in the pool */
    /* Mutex End : mutual exclusively fix the trainID page */
    ERROR_PASS(handle, bfm_unlock(handle, trainId, type));


    /* request Latch as input parameter */
    switch ( mode ) {

      case M_SHARED: /* will be updated to SHARED mode latch call */
		    ERROR_PASS(handle, SHM_getLatch(handle, &anEntry->latch, procIndex, M_SHARED, M_UNCONDITIONAL, NULL));
		    break;

      case M_EXCLUSIVE : /* acquire latch in EXCLUSIVE MODE */
		    ERROR_PASS(handle, SHM_getLatch(handle, &anEntry->latch, procIndex, M_EXCLUSIVE, M_UNCONDITIONAL, NULL));
		    break;
    }


    /* get and fill the return parameters */
    e = Util_getElementFromLocalPool(handle, &(bfm_perThreadDSptr->BACB_pool), acc_cb);
    if ( e < eNOERROR )
	ERR(handle, e);

    (*acc_cb)->key = *trainId;
    /* return the page which corresponds to "anEntry" in the buffer pool */
    (*acc_cb)->bufPagePtr = BI_BUFFER(type, anEntry);
    (*acc_cb)->latchPtr = &anEntry->latch;
    (*acc_cb)->dirtyFlag = FALSE;
    (*acc_cb)->invalidFlag = FALSE;
    (*acc_cb)->type = type;

    ADD_BACB_INTO_MYFIXEDBACB(handle, (*acc_cb));


    /* Initialize the LSN field. */
    /*
     * During formatting a volume, we do not logging because the recovery is
     * considered for the formatted volume.
     * Thus, we initialize the LSN field of the new page here.
     * This is a common place to initialize the field.
     */
    ((PageHdr_T*)(*acc_cb)->bufPagePtr)->lsn = common_perThreadDSptr->minLsn;


    return( eNOERROR );   /* No error */

}  /* BfM_fixNewBuffer */

