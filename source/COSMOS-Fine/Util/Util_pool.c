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
 * Module: Util_pool.c
 *
 * Description:
 *  Those routines in this module implements a pool of elements with some type.
 *  If we manage a pool then we reduce the number of calls for SHM_alloc( ).
 *  Each pool should have elements of same type.
 *
 * Exports:
 *  Four Util_initPool(Pool*, Four, Four)
 *  Four Util_getElementFromPool(Pool*, void*)
 *  Four Util_freeElementToPool(Pool*, void*)
 *  Four Util_finalPool(Pool*)
 *
 * Note:
 *  The routine to deallocate an element has not been implemented.
 *  (At this time the routine is not necessary.)
 */


#include "common.h"
#include "error.h"
#include "latch.h"
#include "Util_pool.h"
#include "Util.h"
#include "trace.h"
#include "SHM.h"
#include "perProcessDS.h"
#include "perThreadDS.h"



/*@================================
 * Util_initPool()
 *================================*/
/*
 * Function: Four Util_initPool(Pool *, Four, Four)
 *
 * Description:
 *  Initialize a pool. To initialize a pool, some information is required:
 *  element size in the pool and maximum number of elements in a subpool.
 *  At first time, we allocate a subpool.
 *
 * Returns:
 *  Error code
 *     eBADPARAMETER_UTIL
 *     eMEMORYALLOCERR_UTIL
 */
Four Util_initPool(
    Four handle,
    Pool *aPool,		/* INOUT pool to be initialized */
    Four elemSize,		/* IN element size */
    Four subpoolSize)		/* IN max elements in a subpool */
{
    SubpoolHdr *subpool;	/* header of a subpool */
    char       *elemPtr;	/* pointer to an element */
    Four       i;		/* a index variable */
    Four       e;		/* returned error */


    TR_PRINT(handle, TR_UTIL, TR1,
	     ("Util_initPool(aPool=%P, elemSize=%ld, subpoolSize=%ld)",
	      aPool, elemSize, subpoolSize));

    /*@ check parameter */
    if (aPool == NULL) ERR(handle, eBADPARAMETER);

    /*@ Initialize the given pool. */
    /* Note: element size must be a multiple of the size of a pointer type (4/8byte) */
    SHM_initLatch(handle, &aPool->latch);
    aPool->elemSize = (elemSize%sizeof(MEMORY_ALIGN_TYPE) == 0) ? elemSize : elemSize - (elemSize%sizeof(MEMORY_ALIGN_TYPE)) +
                                                                             sizeof(MEMORY_ALIGN_TYPE);
    aPool->maxElemInSubpool = subpoolSize;
    aPool->usedElemInPool = 0; 
    aPool->subpoolPtr = LOGICAL_PTR(NULL); 

    return(eNOERROR);

} /* Util_initPool() */



/*@================================
 * Util_getElementFromPool()
 *================================*/
/*
 * Function: Four Util_getElementFromPool(Pool *, void *)
 *
 * Description:
 *  Returns pointer to an allocated element.
 *  If there is no space in a pool, then a subpool is allocated to the pool.
 *
 * Returns:
 *  error codes
 *     eBADPARAMETER_UTIL
 *     eMEMORYALLOCERR_UTIL
 */
Four Util_getElementFromPool(
    Four handle,
    Pool *aPool,		/* IN pool to be used */
    void *elem)			/* OUT allocated element */
{
    SubpoolHdr *prevSubpool;	/* pointer to the previous subpool */
    SubpoolHdr *subpool;	/* pointer to the current subpool */
    char *allocatedElem;	/* pointer to the newly allocated one */
    char       *elemPtr;	/* pointer to an element */
    Four	e;		/* returned error */
    Four	i;		/* index */


    TR_PRINT(handle, TR_UTIL, TR1,
	     ("Util_getElementFromPool(aPool=%P, elem=%P)", aPool, elem));


    /*@ check parameters */
    if (aPool == NULL) ERR(handle, eBADPARAMETER);

    /*@ get latch */
    /*
     * first of all, acquire the latch
     */
    e = SHM_getLatch(handle, &aPool->latch, procIndex, M_EXCLUSIVE, M_UNCONDITIONAL, NULL);
    if ( e < eNOERROR ) ERR(handle, e);


    /*@
     * find a subpool having some freed elements.
     */
    prevSubpool = NULL;
    subpool = PHYSICAL_PTR(aPool->subpoolPtr); 

    while (subpool != NULL) {

	/* found subpool having some freed elements */
	if (subpool->count > 0) break;

	/* go to the next subpool */
	prevSubpool = subpool;
	subpool = PHYSICAL_PTR(subpool->nextSubpool); 
    }

    /*@
     * If 'subpool' is NULL then there is no freed element. Allocate a subpool.
     */
    if (subpool == NULL) {
	e = SHM_alloc(handle, aPool->elemSize*aPool->maxElemInSubpool
		      + sizeof(SubpoolHdr), procIndex, (char **)&subpool);

	if (e < eNOERROR) ERRL1(handle, e, &aPool->latch); 

	/* Initialize the subpool header. */
	subpool->count = aPool->maxElemInSubpool;
	subpool->firstElem = LOGICAL_PTR((char *)subpool + sizeof(SubpoolHdr)); 
	subpool->nextSubpool = LOGICAL_PTR(NULL); 

	/* link the newly allocated subpool to the subpool list. */
        if (prevSubpool == NULL)
            aPool->subpoolPtr = LOGICAL_PTR(subpool); 
        else
            prevSubpool->nextSubpool = LOGICAL_PTR(subpool); 

	/*
	 * Make links among the elements in a subpool.
	 */
	elemPtr = PHYSICAL_PTR(subpool->firstElem); 

	for (i = 0; i < aPool->maxElemInSubpool-1; i++) {
	    /* Make the current element point to the next element. */
	    *((char **)elemPtr) = LOGICAL_PTR(elemPtr + aPool->elemSize); /* elements have logical addresses  */

	    /* The next element becomes the current element next time. */
	    elemPtr += aPool->elemSize;
	}

	*((char **)elemPtr) = LOGICAL_PTR(NULL); /* the last element in the subpool */
    }

    /*@
     * Allocate an element from the subpool.
     */
    allocatedElem = PHYSICAL_PTR(subpool->firstElem); /* allocate an element */

    /* 'firstElem' points to the next free element. */
    subpool->firstElem = *((char **)allocatedElem); /* elements have logical addresses  */

    subpool->count--;		/* decrement the number of elements in subpool */
    aPool->usedElemInPool++; 

    *((char **)elem) = allocatedElem;

    /*@ release latch */
    /*
     * finally, release the latch
     */
    e = SHM_releaseLatch(handle, &aPool->latch, procIndex);
    if (e < eNOERROR) ERR(handle, e);


    return(eNOERROR);

} /* Util_getElementFromPool() */



/*@================================
 * Util_freeElementToPool()
 *================================*/
/*
 * Function: Util_freeElementToPool(Pool*, void*)
 *
 * Description:
 *  Release a previously allocated element to the pool.
 *  If a subpool is fully filled after releasing the elemnet, then the subpool
 *  is deallocated.
 *
 * Returns:
 *  Error code
 *    eBADPARAMETER_UTIL
 *    eMEMORYFREEERR_UTIL
 */
Four Util_freeElementToPool(
    Four handle,
    Pool *aPool,		/* IN a pool where the element is released */
    void *elem)			/* IN an element to free */
{
    Four e;			/* error code */
    SubpoolHdr *prevSubpool;	/* pointer to the previous subpool */
    SubpoolHdr *subpool;	/* pointer to the current subpool */


    TR_PRINT(handle, TR_UTIL, TR1,
	     ("Util_freeElementToPool(aPool=%P, elem=%P)", aPool, elem));


    /*@ check parameter. */
    if (aPool == NULL) ERR(handle, eBADPARAMETER);

    if (elem == NULL) ERR(handle, eBADPARAMETER);

    /*@ get latch */
    /*
     * first of all, acquire the latch
     */
    e = SHM_getLatch(handle, &aPool->latch, procIndex, M_EXCLUSIVE, M_UNCONDITIONAL, NULL);
    if (e < eNOERROR) ERR(handle, e);

    /*@
     * Determine which subpool contains the freed element.
     */
    prevSubpool = NULL;
    subpool = PHYSICAL_PTR(aPool->subpoolPtr); 

    while (subpool != NULL) {
	if ((size_t)elem >= (size_t)(subpool+1) &&
	    (size_t)elem < (size_t)(subpool+1) + aPool->elemSize*aPool->maxElemInSubpool) 
	    break;		/* found the container subpool */

	/* go to the next subpool */
	prevSubpool = subpool;
	subpool = PHYSICAL_PTR(subpool->nextSubpool); 
    }

    /* There is no subpool which contains the freed element. */
    if (subpool == NULL) ERRL1(handle, eBADFREEDELEMENT_UTIL, &aPool->latch); 

    /*@
     * free the element to the subpool.
     */
    *((char **)elem) = subpool->firstElem; /* elements have logical addresses  */
    subpool->firstElem = LOGICAL_PTR(elem); 
    subpool->count++;
    --aPool->usedElemInPool; 

    /*
     * If 'subpool' is fully filled with the elements and 'subpool' is not
     * the first subpool, then release the subpool.
     */

    if (subpool->count == aPool->maxElemInSubpool &&
	prevSubpool != NULL) {

	prevSubpool->nextSubpool = subpool->nextSubpool;

        /* free the subpool. */
	e = SHM_free(handle, (char *)subpool, procIndex);
	if (e < eNOERROR) ERRL1(handle, e, &aPool->latch); 
    }


    /*@ release latch */
    /*
     * finally, release the latch
     */
    e = SHM_releaseLatch(handle, &aPool->latch, procIndex);
    if (e < eNOERROR) ERR(handle, e);

    return(eNOERROR);

} /* Util_freeElementToPool() */



/*@================================
 * Util_finalPool()
 *================================*/
/*
 * Function: Four Util_finalPool(Pool *)
 *
 * Description:
 *  Finalize a pool; deallocate a subpool in the pool.
 *
 * Returns:
 *  error code
 *    eBADPARAMETER_UTIL
 *    eMEMORYFREEERR_UTIL
 */
Four Util_finalPool(
    Four handle,
    Pool *aPool)		/* IN a pool to finalize */
{
    Four       e;		/* error code */
    SubpoolHdr *subpool;	/* current subpool */
    SubpoolHdr *nextSubpool;	/* next subpool */


    TR_PRINT(handle, TR_UTIL, TR1, ("Util_finalPool(aPool=%P)", aPool));

    /*@ check parameter. */
    if (aPool == NULL) ERR(handle, eBADPARAMETER);

    /* 'subpool' points to the first subpool in the given pool. */
    subpool = PHYSICAL_PTR(aPool->subpoolPtr); 

    /*@ deallocate the subpools */
    while (subpool != NULL) {
	/* 'nextSubpool' points to the next subpool. */
	nextSubpool = PHYSICAL_PTR(subpool->nextSubpool); 

	/* deallocate the current subpool. */
	e = SHM_free(handle, (char *)subpool, procIndex);
	if (e < eNOERROR) ERR(handle, e);

	/* 'nextSubpool' becomes the current pool next time. */
	subpool = nextSubpool;
    }

    return(eNOERROR);

} /* Util_finalPool() */


/*@================================
 * Util_getUsedElemInPool()
 *================================*/
/*
 * Function: Four Util_getUsedElemInPool(Pool *, Four *)
 *
 * Description:
 *   get the value of usedElemInPool
 *
 * Returns:
 */
Four Util_getElemInPool(
    Four handle,
    Pool *aPool,                /* IN the given  pool  */
    Four *usedElem)             /* OUT usedElemInPool */
{
    Four       e;               /* error code */

    e = SHM_getLatch(handle, &aPool->latch, procIndex, M_SHARED, M_UNCONDITIONAL, NULL);
    if ( e < eNOERROR ) ERR(handle, e);

    *usedElem = aPool->usedElemInPool;

    e = SHM_releaseLatch(handle, &aPool->latch, procIndex);
    if ( e < eNOERROR ) ERR(handle, e);

    return(eNOERROR);
}

