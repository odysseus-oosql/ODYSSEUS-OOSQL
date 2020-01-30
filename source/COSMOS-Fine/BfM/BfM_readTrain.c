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
 * Module: BfM_getAndFixBuffer.c
 *
 * Description :
 *  Return a buffer which has the disk content indicated by `trainId'.
 *
 * Exports:
 *  Four BfM_getAndFixBuffer(Four, TrainID *, Four, Buffer_ACC_CB *, Four)
 */


#include <string.h>  /* for memcpy */
#include "common.h"
#include "error.h"
#include "trace.h"
#include "latch.h"
#include "SHM.h"
#include "BfM.h"
#include "perProcessDS.h"
#include "perThreadDS.h"



/*@================================
 * BfM_readTrain( )
 *================================*/
/*
 * Function: Four BfM_readTrain(Four, TrainID *, char *, Four)
 *
 * Description :
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
    TrainID     *trainId,	/* IN  train to be used */
    char        *aTrain,	/* OUT pointer to the train buffer */
    Four        type )		/* IN  buffer type */
{
    Four        status;		/* for returned message */
    Four        e;		/* for resource release error */
    BufTBLEntry *anEntry;	/* a buffer Table Entry */


    TR_PRINT(handle, TR_BFM, TR1,("BfM_readTrain(trainId=%P, aTrain=%P, type=%ld)", trainId, aTrain, type));


    /*@ Check the validity of given parameters */

    /* Is the buffer type valid? */
    if (IS_BAD_BUFFERTYPE(type)) ERR(handle, eBADBUFFERTYPE_BFM);


    /*@ get latch */
    /* Mutex Begin : mutual exclusively fix the trainID page */
    ERROR_PASS(handle, bfm_lock(handle, trainId, type));


    /* Check whether the page exist in the buffer pool */
    status = bfm_lookUp(handle, (BfMHashKey *)trainId, type, &anEntry);
    if (status != NOTFOUND_IN_HTABLE && status  < eNOERROR) {
	ERROR_PASS(handle, bfm_unlock(handle, trainId, type));
	ERR(handle, status);
    }

    /* Not Exist in the pool */
    if( status == NOTFOUND_IN_HTABLE ) {

	/*@ read a train */
        /* Read a Train from the disk */
        e = bfm_readBuffer(handle, trainId, aTrain, type);
        if( e < 0 ) {		/* Buffer Read Error :: THINK */
	    ERROR_PASS(handle, bfm_unlock(handle, trainId, type));
            ERR(handle,  e );
	}

        ERROR_PASS(handle, bfm_unlock(handle, trainId, type));

    }
    /* Exist in the pool */
    else {

        /* error check */
        /* 여기서는 fixed count가 반드시 0이어야 한다!! */
        /* 왜냐하면 buffer에서 없애버리기 때문이다!! */
	/* Because a train is removed from buffer here, the fixed count of the train must be 0. */ 
        if(anEntry->fixed >= 1) {
	    ERROR_PASS(handle, bfm_unlock(handle, trainId, type));
            ERR(handle, eINTERNAL);
        }

        /*@ read a train */
        memcpy(aTrain, BI_BUFFER(type, anEntry), BI_BUFSIZE(type)*PAGESIZE);

        /* Delete the victim from the hash table */
        e = bfm_delete(handle, anEntry, type);
        if( e < 0 ) {
            ERROR_PASS(handle, bfm_unlock(handle, trainId, type));
            ERR(handle,  e );
        }

        /*@ if the dirty bit is set, force out to the disk */
        if( anEntry->dirtyFlag )  {
            e = bfm_flushBuffer(handle, anEntry, type);
            if( e < 0 ) {
                ERROR_PASS(handle, bfm_unlock(handle, trainId, type));
                ERR(handle,  e );
            }
        }

        /* reset invalid Flag */
        anEntry->dirtyFlag = FALSE;

         /* Set anEntry->fixed by 1 not to select as victim
         during the initializing anEntry->key to NIL*/
        anEntry->fixed = 1;
        anEntry->invalidFlag = TRUE;
        SET_NILBFMHASHKEY(anEntry->key);

        /* fixed counter is reset to 0. */
        anEntry->fixed = 0;

        /*@ release latch */
        /* Mutex End : mutual exclusively fix the trainID page */
        ERROR_PASS(handle, bfm_unlock(handle, trainId, type));

    }


    return( eNOERROR );   /* No error */

}  /* BfM_readTrain */
