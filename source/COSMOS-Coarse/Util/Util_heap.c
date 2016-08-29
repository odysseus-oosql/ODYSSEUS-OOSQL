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
 * Module: Util_heap.c
 *
 * Description:
 *  Implements a heap which has elements of the same type. Each heap should
 *  cosists of the same elements but different heaps may have different types
 *  of elements. The user can request a variable size of array composed of this
 *  elements; this is the reason we implement a heap aside from the 'Pool'.
 *
 * Exports:
 *  Four Util_initHeap(Heap*, Four, Four)
 *  Four Util_getArrayFromHeap(Heap*, Four, void*)
 *  Four Util_freeArrayToHeap(Heap*, void*)
 *  Four Util_finalHeap(Heap*)
 */


#include <stdlib.h> /* for malloc & free */
#include "common.h"
#include "error.h"
#include "trace.h"
#include "Util_Internal.h"
#include "perThreadDS.h"
#include "perProcessDS.h"


/*@ Some macros */
/* Note: We have casted the (x) with 'Pointer' which is the alignment type. */
#define USED           CONSTANT_ONE
#define SET_USED(x)    (HeapWord*)((MEMORY_ALIGN_TYPE)(x) | USED)
#define RESET_USED(x)  (HeapWord*)((MEMORY_ALIGN_TYPE)(x) & ~USED)
#define IS_USED(x)     ((MEMORY_ALIGN_TYPE)(x) & USED)



/*@================================
 * Util_initHeap()
 *================================*/
/*
 * Function: Four Util_initHeap(Heap*, Four, Four)
 *
 * Description:
 *  Initialize a heap. Save the element size and the maximum number of elements
 *  in a subheap. Then allocate and initialize a subheap.
 *
 * Returns:
 *  Error code
 *    eBADPARAMETER
 *    eMEMORYALLOCERR
 */
Four Util_initHeap(
    Four handle,
    Heap *aHeap,		/* INOUT heap to initialize */
    Four elemSize,		/* IN element size */
    Four subheapSize)		/* IN max elements in a subheap */
{
    Four maxWordsInSubheap;	/* max # of heap words in a subheap */
    SubheapHdr *subheap;	/* header of a subheap */
    HeapWord *heapArray;	/* start of array of heap words */


    TR_PRINT(TR_UTIL, TR1,
             ("Util_initHeap(handle, aHeap=%P, elemSize=%ld, subheapSize=%ld)",
	      aHeap, elemSize, subheapSize));


    /*@ check parameter */
    if (aHeap == NULL) ERR(handle, eBADPARAMETER);

    /* Guarantees the allocation of an array with subheapSize elements. */
    maxWordsInSubheap = 2 +
	(elemSize*subheapSize + sizeof(HeapWord) - 1)/sizeof(HeapWord);

    /*@ Initialize the given heap. */
    aHeap->elemSize = elemSize;
    aHeap->maxWordsInSubheap = maxWordsInSubheap;

    aHeap->subheapPtr = (SubheapHdr*)malloc(aHeap->maxWordsInSubheap*sizeof(HeapWord) + sizeof(SubheapHdr));

    if (aHeap->subheapPtr == NULL) ERR(handle, eMEMORYALLOCERR);

    /* 'heapArray' points to the start position of the chunk of elements. */
    heapArray = (HeapWord*)((char *)aHeap->subheapPtr + sizeof(SubheapHdr));

    /* Initialize the subheap header. */
    subheap = aHeap->subheapPtr;
    subheap->count = maxWordsInSubheap - 1; /* don't count the last heap word for boundary tag */
    subheap->searchPtr = &(heapArray[0]);
    subheap->nextSubheap = NULL;

    /* Set up the boundary tag. */
    heapArray[0].ptr = RESET_USED(&heapArray[aHeap->maxWordsInSubheap-1]);
    heapArray[aHeap->maxWordsInSubheap-1].ptr = SET_USED(&heapArray[0]);

    return(eNOERROR);

} /* Util_initHeap() */



/*@================================
 * Util_getArrayFromHeap()
 *================================*/
/*
 * Function: Four Util_getArrayFromHeap(Heap*, Four, void*)
 *
 * Description:
 *  Allocate an array composed of the given number of elements.
 *
 * Returns:
 *  eBADPARAMETER
 *  eMEMORYALLOCERR
 */
Four Util_getArrayFromHeap(
    Four handle,
    Heap *aHeap,		/* INOUT heap to use */
    Four nElems,		/* IN number of elements to allocate */
    void *array)		/* OUT allocated array */
{
    Four nWords;		/* # of heap words needed */
    Four nLoops;		/* # of loops */
    SubheapHdr *subheap;	/* points to a subheap */
    SubheapHdr *prevSubheap;	/* points to a previous subheap */
    HeapWord *heapArray;	/* start of array of heap words */
    HeapWord *current;		/* points to control heap word of current block */
    HeapWord *next;		/* points to control heap word of next block */
    Boolean  notFound;		/* We find the enough space? */


    TR_PRINT(TR_UTIL, TR1,
             ("Util_getArrayFromHeap(handle, aHeap=%P, nElems=%ld, array=%P)",
	      aHeap, nElems, array));

    /*@ check parameters */
    if (aHeap == NULL) ERR(handle, eBADPARAMETER);

    if (nElems < 0) ERR(handle, eBADPARAMETER);


    /*@ Calculate the number of heap words needed. */
    nWords = 1 + (nElems*aHeap->elemSize + sizeof(HeapWord) - 1)/sizeof(HeapWord);

    /* check parameter - nWords must be smaller than maxWordsInSubheap-1 */
    if (nWords > aHeap->maxWordsInSubheap-1) ERR(handle, eBADPARAMETER); 

    /*@
    ** Find a subheap having the enough elements.
    */
    prevSubheap = NULL;
    subheap = aHeap->subheapPtr;

    while (subheap != NULL) {

	/* Check if there is a possibility that the subheap is enough space. */
	if (subheap->count >= nWords) {

	    notFound = TRUE;
	    current = subheap->searchPtr;
	    for (nLoops = 0; notFound; ) {
		if (!IS_USED(current->ptr)) {
		    /* Make the fragmented block into the contiguous block */
		    while (!IS_USED((next = current->ptr)->ptr))
			current->ptr = next->ptr;

                    if ((size_t)current < (size_t)subheap->searchPtr &&
                                          (size_t)subheap->searchPtr < (size_t)current->ptr)
                        subheap->searchPtr = current;
		} else
		    next = current;

		if (next >= current+nWords) {
		    notFound = FALSE; /* found */
		} else {
		    if (next > next->ptr) {	/* check rounding up */
			if (++nLoops > 1) break;
		    }

		    /* Search from the next block. */
		    current = RESET_USED(next->ptr);
		}
	    }

	    if (!notFound) break; /* found */
	}

	/* Go to the next subheap because there is no enough space. */
	prevSubheap = subheap;
	subheap = subheap->nextSubheap;
    }

    /*@
    ** If 'subheap' is NULL then there is no enuough space. Allocate a subheap.
    */
    if (subheap == NULL) {
	subheap = (SubheapHdr*)malloc(aHeap->maxWordsInSubheap*sizeof(HeapWord) + sizeof(SubheapHdr));

	if (subheap == NULL) ERR(handle, eMEMORYALLOCERR);

	/* 'heapArray' points to the start position of the chunk of elements. */
        heapArray = (HeapWord*)((char *)subheap + sizeof(SubheapHdr));

	/* Initialize the subheap header. */
	subheap->count = aHeap->maxWordsInSubheap - 1; /* ignore the last word for the boundary tag */
	subheap->searchPtr = &(heapArray[0]);
	subheap->nextSubheap = NULL;

	/* Set up the boundary tag. */
	heapArray[0].ptr = RESET_USED(&heapArray[aHeap->maxWordsInSubheap-1]);
	heapArray[aHeap->maxWordsInSubheap-1].ptr = SET_USED(&heapArray[0]);

	/* link the subheap to heap */
	prevSubheap->nextSubheap = subheap;

	/* Set the variables for array allocation. */
	current = &(heapArray[0]);
	next = heapArray[0].ptr;
    }

    /*@
     * Allocate an array from the subheap.
     */
    /*
     * If the current free block is greater than the requested size, then
     * break it into two blocks.
     */
    subheap->searchPtr = current + nWords;
    if (next > subheap->searchPtr) /* The current block is divided into two. */
	subheap->searchPtr->ptr = current->ptr;
    current->ptr = SET_USED(subheap->searchPtr);
    subheap->count -= nWords;

    *((HeapWord **)array) = current + 1;

    return(eNOERROR);

} /* Util_getArrayFromHeap() */



/*@================================
 * Util_freeArrayToHeap()
 *================================*/
/*
 * Function: Four Util_freeArrayToHeap(Heap*, void*)
 *
 * Description:
 *  Release a previously allocated array to the heap.
 *  This function just marks the array free and the coalescing of the free
 *  blocks is done by Util_getArrayFromHeap() function.
 *
 * Returns:
 *  Error code
 *    eBADPARAMETER
 *    eMEMORYFREEERR
 *    eBADFREEDARRAY_UTIL
 */
Four Util_freeArrayToHeap(
    Four handle,
    Heap *aHeap,		/* IN a heap where the array is released */
    void *array)		/* IN an array to free */
{
    Four e;			/* error number */
    Four nWords;		/* # of heap words deallocated */
    SubheapHdr *subheap;	/* pointer to a subheap */
    SubheapHdr *prevSubheap;	/* pointer to the previous subheap */


    TR_PRINT(TR_UTIL, TR1, ("Util_freeArrayToHeap(handle, aHeap=%P, array=%P)", aHeap, array));


    /*@ check parameter. */
    if (aHeap == NULL) ERR(handle, eBADPARAMETER);

    if (array == NULL) ERR(handle, eBADPARAMETER);


    /*@
    ** Determine which subheap contains the freed array.
    */
    prevSubheap = NULL;
    subheap = aHeap->subheapPtr;

    while (subheap != NULL) {
	if ((size_t)array >= (size_t)(subheap+1) &&
	    (size_t)array < (size_t)(subheap+1)+aHeap->maxWordsInSubheap*sizeof(HeapWord)) 
	    break;		/* found the container subheap */

	/* Go to the next subheap. */
	prevSubheap = subheap;
	subheap = subheap->nextSubheap;
    }

    /* There doesn't exist a subheap which contains the freed array. */
    if (subheap == NULL) ERR(handle, eBADFREEDARRAY_UTIL);

    /*@
    ** Free the element to the subheap.
    */
    subheap->searchPtr = (HeapWord*)array - 1;
    subheap->searchPtr->ptr = RESET_USED(subheap->searchPtr->ptr);

    /*
    ** Increment the 'count' of subheap.
    */
    nWords = 1 + ((size_t)subheap->searchPtr->ptr - (size_t)array) / sizeof(HeapWord);
    subheap->count += nWords;

    /*
    ** If a subheap is fully filled after releasing the element, then
    ** the subheap is deallocated.
    */
    if (prevSubheap != NULL && subheap->count == aHeap->maxWordsInSubheap - 1) {
	prevSubheap->nextSubheap = subheap->nextSubheap;

	(void) free(subheap);
    }

    return(eNOERROR);

} /* Util_freeArrayToHeap() */



/*@================================
 * Util_finalHeap()
 *================================*/
/*
 * Function: Four Util_finalHeap(Heap *)
 *
 * Description:
 *  Finalize a heap; deallocate subheaps in the heap.
 *
 * Returns:
 *  eBADPARAMETER
 *  eMEMORYFREEERR
 */
Four Util_finalHeap(
    Four handle,
    Heap *aHeap)		/* IN a heap to finalize */
{
    Four e;			/* error number */
    SubheapHdr *subheap;	/* current subheap */
    SubheapHdr *nextSubheap;	/* next subheap */


    TR_PRINT(TR_UTIL, TR1, ("Util_fianlHeap(aHeap=%P)", aHeap));


    /*@ check parameters. */
    if (aHeap == NULL) ERR(handle, eBADPARAMETER);

    /* 'subheap' points to the first subheap of the given heap. */
    subheap = aHeap->subheapPtr;

    /*@ Deallocate the subheaps. */
    while (subheap != NULL) {
	/* 'nextSubheap' points to the next subheap. */
	nextSubheap = subheap->nextSubheap;

	/* Deallocate the current subheap. */
	(void) free(subheap);

	/* 'nextSubheap' becomes the current subheap next time. */
	subheap = nextSubheap;
    }

    return(eNOERROR);

} /* Util_finalHeap() */




