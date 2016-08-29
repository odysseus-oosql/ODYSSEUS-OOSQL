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
#ifdef COSMOS_S		

#include "common.h"
#include "Util_heap.h"
#include "Util_pool.h"
#include "LRDS.h"
#include "perThreadDS.h"
#include "perProcessDS.h"


Four Util_initLocalHeap(
    Four      handle,
    LocalHeap *aHeap,           /* INOUT heap to initialize */
    Four elemSize,              /* IN element size */
    Four subheapSize)           /* IN max elements in a subheap */
{
    return(Util_initHeap(handle, aHeap, elemSize, subheapSize));
}

Four Util_getArrayFromLocalHeap(
    Four      handle, 
    LocalHeap *aHeap,           /* INOUT heap to use */
    Four nElems,                /* IN number of elements to allocate */
    void *array)                /* OUT allocated array */
{
    return(Util_getArrayFromHeap(handle, aHeap, nElems, array));
}

Four Util_freeArrayToLocalHeap(
    Four      handle,
    LocalHeap *aHeap,           /* IN a heap where the array is released */
    void *array)                /* IN an array to free */
{
    return(Util_freeArrayToHeap(handle, aHeap, array));
}

Four Util_finalLocalHeap(
    Four      handle,
    LocalHeap *aHeap)           /* IN a heap to finalize */
{
    return(Util_finalHeap(handle, aHeap));
}


Four Util_initLocalPool(
    Four      handle,
    LocalPool *aPool,           /* INOUT pool to be initialized */
    Four elemSize,              /* IN element size */
    Four subpoolSize)           /* IN max elements in a subpool */
{
    return(Util_initPool(handle, aPool, elemSize, subpoolSize));
}

Four Util_getElementFromLocalPool(
    Four      handle,
    LocalPool *aPool,                /* IN pool to be used */
    void *elem)                 /* OUT allocated element */
{
    return(Util_getElementFromPool(handle, aPool, elem));
}

Four Util_freeElementToLocalPool(
    Four      handle,
    LocalPool *aPool,           /* IN a pool where the element is released */
    void *elem)                 /* IN an element to free */
{
    return(Util_freeElementToPool(handle, aPool, elem));
}

Four Util_finalLocalPool(
    Four      handle,
    LocalPool *aPool)           /* IN a pool to finalize */
{
    return(Util_finalPool(handle, aPool));
}

Four SHM_initLatch(
    Four       handle, 
    LATCH_TYPE *latchPtr
)
{
    return(eNOERROR);
}

Four SHM_getLatch(Four handle, LATCH_TYPE *latchPtr, Four procIndex, LatchMode reqMode,
                  LatchConditional reqCondition, LATCH_TYPE *releasedLatchPtr)
{
    return(eNOERROR);
}

Four SHM_releaseLatch(Four handle, LATCH_TYPE *latchPtr, Four procIndex)
{
    return(eNOERROR);
}


Four LM_getFlatPageLock(Four handle, XactID *xactID, PageID *pid, LockMode mode,
                        LockDuration duration, LockConditional conditional, LockReply *lockReply, LockMode *oldMode)
{
    *lockReply = (LockReply) mode;
    return(eNOERROR);
}

Four LM_releaseFlatPageLock(Four handle, XactID *xactID, PageID *pid, LockDuration duration)
{
    return(eNOERROR);
}

Four LM_getFileLock(Four handle, XactID *xactID, FileID *fileID, LockMode mode,
                    LockDuration duration, LockConditional conditional, LockReply *lockReply, LockMode *oldMode)
{
    *lockReply = (LockReply) mode;
    return(eNOERROR);
}

Four LM_releaseFileLock(Four handle, XactID *xactID, FileID *fid, LockDuration duration)
{
    return(eNOERROR);
}

#endif /* COSMOS_S */
