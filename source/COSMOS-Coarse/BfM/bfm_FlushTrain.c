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
 * Module: bfm_FlushTrain.c
 *
 * Description : 
 *  Write a train specified by 'trainId' into the disk.
 *
 * Exports:
 *  Four bfm_FlushTrain(TrainID *, Four)
 */


#include "common.h"
#include "trace.h"
#include "RDsM_Internal.h"	
#include "RM.h"                 
#include "BfM_Internal.h"
#include "perThreadDS_Internal.h"
#include "perProcessDS.h"


/*@================================
 * bfm_FlushTrain()
 *================================*/
/*
 * Function: Four bfm_FlushTrain(TrainID*, Four)
 *
 * Description : 
 *  Write a train specified by 'trainId' into the disk.
 *  Construct a hash key using the TrainID 'trainId'(actually same)
 *  in order to look up the buffer in the buffer pool. If it is successfully
 *  found, then force it out to the disk using RDsM, especially
 *  RDsM_WriteTrain().
 *
 * Returns:
 *  error code
 *    some errors caused by function calls
 */
Four bfm_FlushTrain(
    Four handle,
    TrainID 			*trainId,		/* IN train to be flushed */
    Four   				type)			/* IN buffer type */
{
    Four 				e;			/* for errors */
    Four 				index;		/* for an index */
#if defined(USE_COHERENCY_VOLUME) || defined(USE_LOG_COHERENCY_VOLUME)
    bfm_CoherencyPageInfo_t	pageInfo;
#ifdef USE_SHARED_MEMORY_BUFFER
    Four 			hostId;
#endif
#endif

    TR_PRINT(TR_BFM, TR1,
             ("bfm_FlushTrain(handle,  trainId=%P, type=%ld )", trainId, type));

    /* Look up the key in the hash table and get a corresponding index */
    index = bfm_LookUp(handle,  (BfMHashKey *)trainId, type );

    if( index == NOTFOUND_IN_HTABLE )   /* errors in bfm_LookUp() */
        ERR(handle,  eNOTFOUND_BFM );

    /* If the buffer is not dirty, return successfully */
    if (!(BI_BITS(type, index) & DIRTY))
	return(eNOERROR);

    if (!IS_TEMP_VOLUME(trainId->volNo) && RM_IS_ROLLBACK_REQUIRED(handle) && !IS_TEMP_PAGE(BI_BUFFER(type, index)) && !(BI_BITS(type, index) & NEW)) {  

        /* Rollback is required. */
        e = RM_SaveTrain(handle, trainId, BI_BUFFER(type, index), BI_BUFSIZE(type));
        if (e < 0) ERR(handle, e);

        
    } else {
        /*@ write a train into the disk */
        e = RDsM_WriteTrain(handle,  BI_BUFFER(type, index), trainId, BI_BUFSIZE(type) );
        if( e < 0 )   /* errors in RDsM_WriteTrain() */
            ERR(handle,  e );

    }

#if defined(USE_COHERENCY_VOLUME) || defined(USE_LOG_COHERENCY_VOLUME)
    pageInfo.pageId      = *trainId;
    pageInfo.timestamp   = NIL;
#ifdef USE_SHARED_MEMORY_BUFFER
    pageInfo.hostId      = gethostid();
    pageInfo.shmBufferId = BFM_PER_PROCESS_DS.shmBufferId; 
#endif
    e = bfm_AppendCoherencyPageInfos(handle, &pageInfo);
    if (e < 0) ERR(handle, e);
#endif

    /* clear the dirty bit */
    BI_BITS(type, index) ^= DIRTY;

    /* clear the new bit if set */
    if(BI_BITS(type, index) & NEW) BI_BITS(type, index) ^= NEW; 
	
    return( eNOERROR );

}  /* bfm_FlushTrain */
