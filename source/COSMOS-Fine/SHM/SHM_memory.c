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
 * Module: SHM_alloc_free.c
 *
 * Description:
 *  support Mutual Exclusion for a critical section
 *
 * Exports:
 *	SHM_alloc()
 *	SHM_free()
 */


#include "common.h"
#include "error.h"
#include "trace.h"
#include "SHM.h"
#include "perProcessDS.h"
#include "perThreadDS.h"


/* Some macros */
/* Note: We have casted the (x) with 'Four' which is the alignment type. */
#define USED           CONSTANT_ONE
#define SET_USED(x)    (HeapWord*)((MEMORY_ALIGN_TYPE)(x) | USED)
#define RESET_USED(x)  (HeapWord*)((MEMORY_ALIGN_TYPE)(x) & ~USED)
#define IS_USED(x)     ((MEMORY_ALIGN_TYPE)(x) & USED)



/*@================================
 * shm_initSharedHeap( )
 *================================*/
/*
** shm_initShardHeap :: initialize Shared Heap Area.
**                      called only when initialize time.
*/
Four shm_initSharedHeap(
    Four 	handle,
    Four 	initSize
)
{
    HeapWord 	*heapArray;	/* start of array of heap words */
    Four 	maxElement;	/* maximum number of elements */


    TR_PRINT(handle, TR_SHM, TR1, ("SHM_initHeap(initSize=%ld)", initSize));

    /*@ initialize the data structures */

    /* Initialize the given heap. */
    SHM_initLatch(handle, &LATCH_SHAREDHEAP);
    shmPtr->sharedHeap = LOGICAL_PTR(&(shmPtr->freeSpaceBase[0])); 

    /* Set up the boundary tag. */
    heapArray = (HeapWord*)PHYSICAL_PTR(shmPtr->sharedHeap); 
    maxElement = initSize/sizeof(HeapWord);

    heapArray[0].ptr = LOGICAL_PTR(RESET_USED(&heapArray[maxElement-1])); 
    heapArray[maxElement-1].ptr = LOGICAL_PTR(SET_USED(&heapArray[0])); 

    return(eNOERROR);

}



/*@================================
 * SHM_alloc( )
 *================================*/
/* SHM_alloc :: memory allocation routine called at normal operation time
   		To serialize concurrent allocation/free, acquire the latch */
Four SHM_alloc(
    Four    	handle,
    Four 	size,
    Four 	procIndex,
    char 	**allocated
)
{
    Four 	nWords;		/* # of heap words needed */
    Four 	nLoops;		/* # of loops */
    HeapWord 	*current;	/* points to control heap word of current block */
    HeapWord 	*next;		/* points to control heap word of next block */
    Boolean  	notFound;	/* We find the enough space? */
    Four 	e;		/* returned error */

    TR_PRINT(handle, TR_SHM, TR1,("SHM_alloc(size=%ld, procIndex=%P)",
			  size, procIndex));

    /*@ check parameters */
    if (size < 0) ERR(handle, eBADPARAMETER);


    /* Calculate the number of heap words needed. */
    nWords = 1 + (size + sizeof(HeapWord) - 1)/sizeof(HeapWord);

    TR_PRINT(handle, TR_SHM, TR2, (" %ld(%ld) bytes are needed(allocated) from Shared Memeory\n",
			   size, nWords*sizeof(HeapWord)));

    /*@ get latch */
    /* Mutex Begin ---------------------------------------- */
    if (procIndex != -1) {
        e = SHM_getLatch(handle, &LATCH_SHAREDHEAP, procIndex, M_EXCLUSIVE, M_UNCONDITIONAL, NULL);
        if ( e < eNOERROR ) ERR(handle, e);
    }

    /*@ find unused one */
    current = PHYSICAL_PTR(shmPtr->sharedHeap); 

    notFound = TRUE;
    for (nLoops = 0; notFound; ) {
	if (!IS_USED(PHYSICAL_PTR(current->ptr))) {
	    /* Make the fragmented block into the contiguous block */
            for (next = PHYSICAL_PTR(current->ptr); !IS_USED(PHYSICAL_PTR(next->ptr)); next = PHYSICAL_PTR(current->ptr))
		current->ptr = next->ptr;

            if ((size_t)current < (size_t)PHYSICAL_PTR(shmPtr->sharedHeap) &&
                                  (size_t)PHYSICAL_PTR(shmPtr->sharedHeap) < (size_t)PHYSICAL_PTR(current->ptr))
                shmPtr->sharedHeap = LOGICAL_PTR(current);
	} else
	    next = current;

	if (next >= current+nWords) {
	    notFound = FALSE;	/* found */
	} else {
	    if (next > (HeapWord*)PHYSICAL_PTR(next->ptr)) { /* check rounding up */ 
		if (++nLoops > 1) break;
	    }

	    /* Search from the next block. */
	    current = RESET_USED(PHYSICAL_PTR(next->ptr));
	}
    }

    if (notFound) ERRL1(handle, eNOFREESPACEINGHEAP_SHM, &LATCH_SHAREDHEAP); 

    /*
    ** Allocate an array from the subheap.
    ** If the current free block is greater than the requested size, then
    *  break it into two blocks.
    */
    shmPtr->sharedHeap  = LOGICAL_PTR((HeapWord*)current + nWords); 
    if ( next > (HeapWord*)PHYSICAL_PTR(shmPtr->sharedHeap) ) /* The current block is divided into two. */
	((HeapWord*)PHYSICAL_PTR(shmPtr->sharedHeap))->ptr = current->ptr; 
    current->ptr = LOGICAL_PTR( SET_USED( PHYSICAL_PTR(shmPtr->sharedHeap)));

    *((HeapWord **)allocated) = current + 1;

    /*@ release latch */
    if (procIndex != -1) {
        e = SHM_releaseLatch(handle, &LATCH_SHAREDHEAP, procIndex);
        if (e < eNOERROR) ERR(handle, e);
    }
    /* MUTEX end --------------------------------------------------------*/

    return(eNOERROR);

}



/*@================================
 * SHM_free( )
 *================================*/
/* SHM_free :: memory free routine
   	       To serialize concurrent allocation/free, acquire the latch */
Four SHM_free(
    Four 	handle,
    char 	*memptr,
    Four 	procIndex
)
{
    Four 	e;			/* error number */
    HeapWord 	*sharedHeapPtr; 


    TR_PRINT(handle, TR_SHM, TR1, ("SHM_free(memptr=%P, procIndex=%ld)", memptr, procIndex));

    /*@ parameter check */
    if (memptr == NULL) ERR(handle, eBADPARAMETER);

    /*@ get latch */
    /* Mutex Begin ---------------------------------------------------- */
    if (procIndex != -1) {
        e = SHM_getLatch(handle, &LATCH_SHAREDHEAP, procIndex, M_EXCLUSIVE, M_UNCONDITIONAL, NULL);
        if ( e < eNOERROR ) ERR(handle, e);
    }

    /*
    ** Free the element to the subpool.
    */
    sharedHeapPtr = (HeapWord*)memptr - 1;
    shmPtr->sharedHeap = LOGICAL_PTR(sharedHeapPtr);
    sharedHeapPtr->ptr = LOGICAL_PTR( RESET_USED( PHYSICAL_PTR(sharedHeapPtr->ptr)));

    /*@ release latch */
    if (procIndex != -1) {
        e = SHM_releaseLatch(handle, &LATCH_SHAREDHEAP, procIndex);
        if (e < eNOERROR) ERR(handle, e);
    }
    /* MUTEX end --------------------------------------------------------*/

    return(eNOERROR);

}
