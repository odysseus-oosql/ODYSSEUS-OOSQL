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
 * Module: QuickFitMM_Alloc.c
 *
 * Description:
 *      allocate memory from free list table using quick fit algorithm.
 *
 *
 * Imports:
 *
 * Exports:
 * Four QuickFitMM_Alloc()
 *
*/

#include "QuickFitMM_Internal.h"
#include "QuickFitMM.hxx"
#include <stdlib.h>

void *QuickFitMM_Alloc(
    QuickFitMM_Handle *mm_handle,   /* handle for memory manager */
    Four size) 
{    
    Four freeListTblIndex ;
    Four i;
    Four e;
    QuickFitMM_Header *mh;
    QuickFitMM_Header *nextMH;
    QuickFitMM_FreeBlockInfo *freeBlkInf;
    QuickFitMM_FreeBlockInfo *tfreeBlkInf;
    QuickFitMM_FreeBlockInfo *insertedBlkInf;
    QuickFitMM_FreeBlockInfo *tmpBlkInfo;
    Four tmpSize;
    Four HashVal;
    Four tSize;

    
    /* check parameter */
    if(size < 0) 
    {
        QUICKFITMM_PRTERR(eBADPARAMETER_QuickFitMM);
        return NULL;
    }
    else if(size >= mm_handle->maxBufferSize)
    {
        QuickFitMM_Header* p;

        p = (QuickFitMM_Header*)malloc(size + sizeof(QuickFitMM_Header));
        if(p == NULL)
        {
            QUICKFITMM_PRTERR(eSHORTOFMEMORY_QuickFitMM);
            return NULL;
        }
        p->isFree    = -1;
        p->isLast    = -1;
        p->prevBlock = NULL;
        p->size      = size;
        return (char*)p + sizeof(QuickFitMM_Header);
    }

    if(size == 0)
        size = QuickFitMM_ALLOCUNIT;
    else
        size = ((Four)((size + QuickFitMM_ALLOCUNIT - 1)/QuickFitMM_ALLOCUNIT))*QuickFitMM_ALLOCUNIT;
    
    /* quick fit allocation */
    freeListTblIndex = getIndexOfFreeListTbl(size);

    if(freeListTblIndex!=mm_handle->freeListTblSize-1) freeListTblIndex++;

    for(i = freeListTblIndex ; i < mm_handle->freeListTblSize ; i++) 
        if(mm_handle->mm_freeListTbl[i].nextFreeBlock != (QuickFitMM_FreeBlockInfo *)NULL)  break;

    if(i == mm_handle->freeListTblSize) {

        /* no memory for requested size */
        /* memory doubling */
        /* one large free block */
        e = QuickFitMM_DoubleBufferPool(mm_handle);
        if( e < 0) {
            QUICKFITMM_PRTERR(e);
            return NULL;
        }
        i = mm_handle->freeListTblSize - 1; 
    }
    
    freeBlkInf = mm_handle->mm_freeListTbl[i].nextFreeBlock;
    mh = &(((QuickFitMM_Header *)freeBlkInf)[-1]);

    /* remove free block from free list table */
    _quickFitMM_removeFreeBlkFromFreeListTbl(mm_handle, mh, i);

#ifdef QuickFitMM_TRACE
    if(!mh->isFree) {
        QUICKFITMM_PRTERR(eINTERNAL_QuickFitMM);
        return NULL;
    }
#endif

    /* this block is no more free */
    mh->isFree = 0;

    /* if this block is used for other size */
    if(mh->size >= (size + sizeof(QuickFitMM_Header) +  QuickFitMM_ALLOCUNIT*2)) {
        /* next free block information */
        /* split this block two blocks */
        nextMH = (QuickFitMM_Header *)((char *)&mh[1] + size);
        nextMH->isFree = 1;
        nextMH->size = mh->size - size - sizeof(QuickFitMM_Header);
        nextMH->prevBlock = mh;
        nextMH->isLast = mh->isLast;

        /* find size */
        tmpSize = getIndexOfFreeListTbl(nextMH->size);

        /* insert next block into free list table */
        _quickFitMM_insertFreeBlkToFreeListTbl(mm_handle, nextMH, tmpSize);

        /* allocated data is not last entry */
        mh->isLast = 0;

        /* allocated size */
        mh->size = size;

        /* the next block of the next block of mh is changed...*/
        if(!nextMH->isLast) {
            QuickFitMM_Header *tmpHeader;
            tmpHeader = (QuickFitMM_Header *)((char *)&nextMH[1] + nextMH->size);
            tmpHeader->prevBlock = nextMH;
            tmpHeader->prevBlock = nextMH;
        }
    }
    
#ifdef QuickFitMM_TRACE
    if(mh->prevBlock!=NULL) {
        if((((char*)&(mh->prevBlock)[1])+mh->prevBlock->size)!=(char *)mh) {
            QUICKFITMM_PRTERR(eINTERNAL_QuickFitMM);
            return NULL;
        }
    }
    if(!mh->isLast) {
        QuickFitMM_Header *tmpHeader1;
        QuickFitMM_Header *tmpHeader2;
        tmpHeader1 = (QuickFitMM_Header *)((char *)&mh[1] + mh->size);
        if(tmpHeader1->prevBlock != mh) {
            QUICKFITMM_PRTERR(eINTERNAL_QuickFitMM);
            return NULL;
        }
        if(!tmpHeader1->isLast) {
            tmpHeader2 = (QuickFitMM_Header *)((char *)&tmpHeader1[1] + tmpHeader1->size);
            if(tmpHeader2->prevBlock != tmpHeader1) {
                QUICKFITMM_PRTERR(eINTERNAL_QuickFitMM);
                return NULL;
            }
        }
    }
#endif

    /* allocate this free block for request */
    return (void *)freeBlkInf;
}

