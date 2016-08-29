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
 * Module: BfM_GetNewTrain.c
 *
 * Description : 
 *  Return a buffer which has the disk content indicated by `trainId'.
 *
 * Exports:
 *  Four BfM_GetNewTrain(TrainID *, char **, Four)
 */

#include "common.h"
#include "trace.h"
#include "RM_Internal.h"
#include "RM.h"
#include "BfM_Internal.h"
#include "perThreadDS_Internal.h"
#include "perProcessDS.h"



/*@================================
 * BfM_GetNewTrain()
 *================================*/
/*
 * Function: BfM_GetNewTrain(TrainID*, char**, Four)
 *
 * Description : 
 *  Return a buffer which has the disk content indicated by `trainId'.
 *  Before the allocation of a buffer, look up the train in the buffer
 *  pool using hashing mechanism.   If the train already  exist in the pool
 *  then simply return it and set the reference bit of the correponding
 *  buffer table entry.   Otherwise, i.e. the train does not exist  in the
 *  pool,  allocate a buffer (a buffer selected as victim may be forced out
 *  by the buffer replacement algorithm), read a disk train into the 
 *  selected buffer train, and return it.
 *
 *  이 함수는 기본적으로 BfM_GetTrain 과 같은 기능을 수행하지만, 가지고온 page를 buffer에 기록할때,
 *  NEW flag를 설정하는 점에서 다르다. NEW flag는 bfM_FlushTrain에서 사용되는 값으로, 이 값이 설정되어
 *  있을 경우, LOG volume이 있음에도 불구하고, 무조건 데이타페이지에 주어진 page를 flush하게된다.
 *  이 함수는 새로 할당된 페이지와 같이 logging이 필요없는(recovery가 필요없는) 페이지에 대해 logging 
 *  overhead를 줄이기 위한 목적으로 사용된다. 
 *  이 함수를 사용할때 주의점은, 이 함수를 통해 읽혀지는 페이지는 
 *	1. 반드시 새로 할당된 페이지 여야 하며,
 *	2. log volume에 그 내용이 써진 적이 없어야 한다.
 *  만약 이 두 조건을 만족하지 않거나, 만족하는지 여부가 불분명한 경우에, 이 함수를 절대로 사용해서는 
 *  안된다. 이 경우, 데이타베이스가 망가지는 문제가 발생할 수 있기 때문이다.
 *  위 두 조건을 만족하지 않는 경우네는 BfM_GetTrain을 써야 한다.
 *
 *  혹시 있을 잘못된 사용을 방지하기 위해, 본 함수는 위 두가지 조건을 최대한으로 검사하며, 만약 검사 조건을
 *  만적하지 못할 시에는 BfM_GetTrain과 동일하게 동작한다.
 * 
 *  This function is basically doing same operation with BfM_GetTrain(). 
 *  The difference from BfM_GetTrain() is that this function sets 'NEW flag' when a page 
 *  is loaded up to buffer. NEW flag stands for whether a page have to be logged 
 *  or not. The flag is used in bfm_FlushTrain(), and if the flag is set, flush out
 *  a page to data page even though there is log volume. The purpose of this flag
 *  is to remove logging overhead for a page which doesn't have to be logged(or recovered)
 *  such as a newly allocated page.
 *
 *  Note:
 *  The page that is read by this function
 *  	1. should be an newly allocated page.  
 *  	2. has never been written to log volume. 
 *
 *  If these condition is not satisfied nor clear, must not use this function because 
 *  a database will lose its consistency. In this case, use BfM_GetTrain() instead of 
 *  BfM_GetNewTrain(). To prevent misuse of BfM_GetNewTrain(), BfM_GetNewTrain() checks 
 *  above two conditions as fully as possible and executes BfM_GetTrain() when the 
 *  condition is not satisfied.
 *
 * Returns:
 *  error code
 *    eBADBUFFER_BFM - Invalid Buffer
 *    eNOTFOUND_BFM  - The key isn't in the hash table
 *    some errors caused by function calls
 *
 * Side effects:
 *  1) parameter retBuf
 *     pointer to buffer holding the disk train indicated by `trainId'
 */
Four BfM_GetNewTrain(
    Four handle,
    TrainID 	*trainId,		/* IN train to be used */
    char    	**retBuf,		/* OUT pointer to the returned buffer */
    Four    	type )			/* IN buffer type */
{
    Four 	e;				/* for error */
    Four 	index;			/* index of the buffer pool */
    Four 	logPageNo;		/* log page number */

    TR_PRINT(TR_BFM, TR1, ("BfM_GetNewTrain(handle, trainId=%P, retBuf=%P, type=%ld)",
			   trainId, retBuf, type));

#ifdef USE_SHARED_MEMORY_BUFFER		
    /* Block signals. */
    SHARED_MEMORY_BUFFER_BLOCK_SIGNAL(handle);
#endif

    /*@ Check the validity of given parameters */
    /* Some restrictions may be added         */
    if( retBuf == NULL ) ERR(handle, eBADBUFFER_BFM);

    /* Is the buffer type valid? */
    if (IS_BAD_BUFFERTYPE(type)) ERR(handle, eBADBUFFERTYPE_BFM);	
   
#ifdef USE_SHARED_MEMORY_BUFFER		
    /* 
     * Acquire lock of buffer table entry. 
     * Only one procecc access a certain buffer table entry.
     */
    ERROR_PASS(handle, bfm_Lock(handle, trainId, type));
#endif

    /*@ Check whether the page exist in the buffer pool */
    if( (index = bfm_LookUp(handle, (BfMHashKey *)trainId,type))==NOTFOUND_IN_HTABLE ) {
	if(RM_IS_ROLLBACK_REQUIRED(handle) && rm_LookUpInLogTable(handle, trainId, &logPageNo))	
	{
#ifdef USE_SHARED_MEMORY_BUFFER		
	    /* Release the lock of buffer table entry. */
	    ERROR_PASS(handle, bfm_Unlock(handle, trainId, type));
#endif
	    /* 만약 log page에 이미 페이지가 존재한다면, logging을 하도록 BfM_GetTrain을 호출한다. */
            /* If the page is already in log page, call BfM_GetTrain for logging it. */ 
	    return BfM_GetTrain(handle, trainId, retBuf, type);
	}

	/* Not Exist in the pool */
	
        /* Allocate a buffer from the buffer pool of type 'type' */
        index = bfm_AllocTrain(handle, type);
        if( index < eNOERROR ) {
	    ERR_BfM(handle, index, trainId, type);	
	}
	
        /* fill the buffer table using the given parameters */
        BI_KEY(type, index) = *((BfMHashKey *)trainId);
	
        /* set the reference bit */
        BI_BITS(type, index) |= REFER;
#ifdef USE_SHARED_MEMORY_BUFFER	
	BI_BITS(type, index) |= VALID;
#endif
	
	/* Insert the key into the hash table */
        e = bfm_Insert(handle,  (BfMHashKey *)trainId, index, type );
	if( e < eNOERROR ) {
	    ERR_BfM(handle, e, trainId, type); 	
	}
	
	BI_NFIXED(type, index) = 1;
	/* set the new bit */
	BI_BITS(type, index) |= NEW;	

    } else if( index >= 0 ) { /* exist in the pool */
	BI_NFIXED(type, index)++;

        /* Set the reference bit of the table indicated by `index' */
        BI_BITS(type, index) |= REFER;
    } else {
	ERR_BfM(handle, index, trainId, type);	
    }

#ifdef USE_SHARED_MEMORY_BUFFER
    /* Add fixed buffer to fixed buffer table. */
    e = bfm_InsertFixedBuffer(handle, (BfMHashKey* )trainId, type);
    if (e < eNOERROR) {
        ERR_BfM(handle, e, trainId, type);
    }
#endif

    /*@ return the page which corresponds to `index' in the buffer pool */
    *retBuf = BI_BUFFER(type, index);


#ifdef USE_SHARED_MEMORY_BUFFER		
    /* Release lock of buffer table entry. */
    ERROR_PASS(handle, bfm_Unlock(handle, trainId, type));
#endif

    return( eNOERROR );   /* No error */

}  /* BfM_GetNewTrain() */
