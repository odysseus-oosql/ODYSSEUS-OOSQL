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
/*    ODYSSEUS/OOSQL DB-IR-Spatial Tightly-Integrated DBMS                    */
/*    Version 5.0                                                             */
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
 * Module: QuickFitMM_Init.c
 *
 * Description:
 *      Initialize memory manager
 *
 *
 * Imports:
 * Four getIndexOfFreeListTbl(mh->size);
 * void _quickFitMM_insertFreeBlkToFreeListTbl();
 *
 * Exports:
 * Four QuickFitMM_Init()
 *
*/

#include "QuickFitMM_Internal.h"
#include "QuickFitMM.hxx"
#include <stdlib.h>

Four _quickFitMM_checkBlock(
    QuickFitMM_Handle *mm_handle,
    struct _quickFitMM_freeblockinfo *freeBlock
)
{
    struct _quickFitMM_freeblockinfo *currentFreeBlk;
    struct _quickFitMM_freeblockinfo *nextFreeBlk;
    Four bufferSize;

    bufferSize = (Four)(((mm_handle->maxBufferSize + QuickFitMM_ALLOCUNIT - 1)/QuickFitMM_ALLOCUNIT)*QuickFitMM_ALLOCUNIT);

    currentFreeBlk = freeBlock;
    nextFreeBlk = currentFreeBlk->nextFreeBlock;
    while(nextFreeBlk != NULL) {
        if(currentFreeBlk->nextFreeBlock->prevFreeBlock != currentFreeBlk)
            QUICKFITMM_ERR(eINTERNAL_QuickFitMM);
        if(!( ((unsigned long)((mm_handle)->mm_bufferPool) <= (unsigned long)(freeBlock)) &&
        ((unsigned long)((mm_handle)->mm_bufferPool + sizeof(QuickFitMM_BufferPool) + (bufferSize - 1) + sizeof(QuickFitMM_Header)) > (unsigned long)(freeBlock)))) QUICKFITMM_ERR(eINTERNAL_QuickFitMM);
        currentFreeBlk = nextFreeBlk;
        nextFreeBlk = currentFreeBlk->nextFreeBlock;
    }

    return eNOERROR;
}


Four QuickFitMM_Check(QuickFitMM_Handle *mm_handle)
{    

    Four i; /* counter variable */
    QuickFitMM_Header *mh;
    Four tmpSize;

#ifdef  QuickFitMM_TRACE
    printf("\nQuickFitMM_Check(mm_handle=%X)", mm_handle);
#endif

    /* make next free buffer pool NULL */
    struct _quickFitMM_freeblockinfo *tmpFreeBlock;

    for(i = 0; i < mm_handle->freeListTblSize ; i++) {
        tmpFreeBlock = mm_handle->mm_freeListTbl[i].nextFreeBlock;
        if(tmpFreeBlock != NULL)
            if(_quickFitMM_checkBlock(mm_handle, tmpFreeBlock) < 0) QUICKFITMM_ERR(eINTERNAL_QuickFitMM);
    }


    return eNOERROR;
}

Four QuickFitMM_Init(QuickFitMM_Handle *mm_handle, Four maxBufferSize)
{    

    Four i; /* counter variable */
    QuickFitMM_Header *mh;
    Four tmpSize;
    Four freeListTblSize;
    Four bufferSize;

    /* make next free buffer pool NULL */

    bufferSize = (Four)(((maxBufferSize + QuickFitMM_ALLOCUNIT - 1)/QuickFitMM_ALLOCUNIT)*QuickFitMM_ALLOCUNIT);
    freeListTblSize = getIndexOfFreeListTbl(bufferSize) +  1;

    mm_handle->mm_freeListTbl = (QuickFitMM_FreeListTbl *)malloc(sizeof(QuickFitMM_FreeListTbl)*freeListTblSize);
    for(i = 0; i < freeListTblSize ; i++)
        mm_handle->mm_freeListTbl[i].nextFreeBlock = (QuickFitMM_FreeBlockInfo *)NULL;

    /* allocate buffer pool */
    mm_handle->mm_bufferPool = (QuickFitMM_BufferPool *)malloc(sizeof(QuickFitMM_BufferPool) + (bufferSize - sizeof(Four)) + sizeof(QuickFitMM_Header));
    if(mm_handle->mm_bufferPool == (QuickFitMM_BufferPool *)NULL) return eCANNOTINIT_QuickFitMM;

    /* insert one large free block */
    mm_handle->mm_bufferPool->nextFreeBufferPool = (QuickFitMM_BufferPool *)NULL;
    mh = (QuickFitMM_Header *)&(mm_handle->mm_bufferPool->data[0]);
    mh->isLast = 1; /* i.e. last block */
    mh->isFree = 1;
    mh->size = bufferSize;
    mh->prevBlock = (QuickFitMM_Header *)NULL;
    mm_handle->maxBufferSize = maxBufferSize;
    mm_handle->freeListTblSize = freeListTblSize;

    tmpSize = getIndexOfFreeListTbl(mh->size);
    _quickFitMM_insertFreeBlkToFreeListTbl(mm_handle, mh, tmpSize);

    return eNOERROR;
}
