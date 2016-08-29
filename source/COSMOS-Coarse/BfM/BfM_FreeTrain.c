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
 * Module: BfM_FreeTrain.c
 *
 * Description :
 *  Free(or unfix) a buffer.
 *
 * Exports:
 *  Four BfM_FreeTrain(TrainID *, Four)
 */


#include "common.h"
#include "trace.h"
#include "BfM_Internal.h"
#include "perThreadDS_Internal.h"
#include "perProcessDS.h"



/*@================================
 * BfM_FreeTrain()
 *================================*/
/*
 * Function: Four BfM_FreeTrain(TrainID*, Four)
 *
 * Description :
 *  Free(or unfix) a buffer.
 *  This function simply frees a buffer by decrementing the fix count by 1.
 *
 * Returns :
 *  error code
 *    eBADBUFFERTYPE_BFM - bad buffer type
 *    some errors caused by fuction calls
 */
Four BfM_FreeTrain( 
    Four handle,
    TrainID             *trainId,       /* IN train to be freed */
    Four                type)           /* IN buffer type */
{
    Four                index;          /* index on buffer holding the train */
    Four 				e;				/* error code */

    TR_PRINT(TR_BFM, TR1,
             ("BfM_FreeTrain(handle,  trainIid=%P, type=%ld )", trainId, type));

    /*@ check if the parameter is valid. */
    if (IS_BAD_BUFFERTYPE(type)) ERR(handle, eBADBUFFERTYPE_BFM);	

#ifdef USE_SHARED_MEMORY_BUFFER	
    /* 
     * Acquire lock of a buffer table entry.
     * Only one process can access a certain buffer table entry.
     */
    ERROR_PASS(handle, bfm_Lock(handle, trainId, type));
#endif

    index = bfm_LookUp(handle,  (BfMHashKey *)trainId, type );
    if( index == NOTFOUND_IN_HTABLE ) {
	ERR_BfM(handle, eNOTFOUND_BFM, trainId, type);	
    }

    BI_NFIXED(type, index)--;
    
#ifdef USE_SHARED_MEMORY_BUFFER	
    /* Delete fixed buffer from fixed buffer table. */
    e = bfm_DeleteFixedBuffer(handle, (BfMHashKey* )trainId, type);
    if (e < eNOERROR) {
	ERR(handle, e);
    }
#endif
    
    if( BI_NFIXED(type, index) < 0 ) {
    	printf("### [pID=%d, tID=%d] Fixed counter is less than 0!!! Train ID: [%d, %d]\n", procIndex, handle, trainId->volNo, trainId->pageNo);
	BI_NFIXED(type, index) = 0;
    }
    
#ifdef USE_SHARED_MEMORY_BUFFER
    /* Release lock of a buffer table entry. */
    ERROR_PASS(handle, bfm_Unlock(handle, trainId, type));

    /* Unblock signals. */
    SHARED_MEMORY_BUFFER_UNBLOCK_SIGNAL(handle);
    
    /* Check received signal and handle it. */
    SHARED_MEMORY_BUFFER_CHECK_SIGNAL(handle);
    
    /* 
     * Check the number of fixed buffer is same with the number of unfixed buffer. 
     * If both of them are same, initialize fixed buffer counter and unfixed buffer counter to 0.
     */
    SHARED_MEMORY_BUFFER_CHECK_FIXED_BUFFERS(handle);
#endif
  
    return( eNOERROR );
    
} /* BfM_FreeTrain() */
