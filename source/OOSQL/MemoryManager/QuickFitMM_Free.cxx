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
 * Module: QuickFitMM_Free.c
 *
 * Description:
 *      Free p and return this freed block to free list table 
 *      for next allocation.
 *
 *
 * Imports:
 * Four getIndexOfFreeListTbl();
 * void  _quickFitMM_insertFreeBlkToFreeListTbl();
 * Exports:
 * Four QuickFitMM_Free()
 *
*/
#include <stdlib.h>
#include "QuickFitMM_Internal.h"
#include "QuickFitMM.hxx"


#undef SPEED


Four QuickFitMM_Free_Void_Pointer(
		    QuickFitMM_Handle *mm_handle,
			    void *p)
{
	    return QuickFitMM_Free(mm_handle, (char*)p);
}

Four QuickFitMM_Free(
    QuickFitMM_Handle *mm_handle,
    char *p)
{    
    Four freeListTblIndex ;
    Four i;
    Four tmpSize;
    Four tSize;
    Four HashVal;
    QuickFitMM_Header *mh;
    QuickFitMM_Header *prevMH;
    QuickFitMM_Header *nextMH;
    QuickFitMM_Header *tmpMH;
    QuickFitMM_FreeBlockInfo *freeBlkInf;
    QuickFitMM_FreeBlockInfo *tfreeBlkInf;
    QuickFitMM_FreeBlockInfo *removedBlkInf;
    QuickFitMM_FreeBlockInfo *insertedBlkInf;


    tmpMH = (QuickFitMM_Header *)p;
    mh = &tmpMH[-1];

    /* check parameter */
    if(mh->size < 0) 
    {
        QUICKFITMM_ERR(eBADPARAMETER_QuickFitMM);
    }
    else if(mh->size >= (unsigned)mm_handle->maxBufferSize)
    {
        free(mh);
        return eNOERROR;
    }
    
    prevMH = mh->prevBlock;
    if(!mh->isLast) nextMH = (QuickFitMM_Header *)(p + mh->size);
    else nextMH = (QuickFitMM_Header *)NULL;

    /* 
     *  If this block can be merged the previous block or 
     *  the next block of this block only if they are already free 
     */

    /* merge the previous free block */
    if(prevMH != (QuickFitMM_Header *)NULL) {
#ifdef QuickFitMM_TRACE
        if(((char*)&prevMH[1] + prevMH->size )!=(char*)mh) {
            printf("mh = 0x%x, mh->prevBlock = 0x%x\n",mh, mh->prevBlock);
            QUICKFITMM_ERR(eINTERNAL_QuickFitMM);
        }
#endif
        if(prevMH->isFree) {
            tmpSize = getIndexOfFreeListTbl(prevMH->size);

            _quickFitMM_removeFreeBlkFromFreeListTbl(mm_handle, prevMH, tmpSize);

            prevMH->size = prevMH->size + mh->size + sizeof(QuickFitMM_Header);
            prevMH->isLast = mh->isLast;
#ifdef QuickFitMM_TRACE
            prevMH->isFree = 0;
#endif

            /* previous block of next block is changed */
            if(nextMH != (QuickFitMM_Header *)NULL) nextMH->prevBlock = prevMH;
            mh = prevMH;
        }
    }

    /* merge the next free block */
    if(nextMH != (QuickFitMM_Header *)NULL) {
#ifdef QuickFitMM_TRACE
        if(nextMH->prevBlock!=mh) QUICKFITMM_ERR(eINTERNAL_QuickFitMM);
#endif
        if(nextMH->isFree) {
            tmpSize = getIndexOfFreeListTbl(nextMH->size);

            _quickFitMM_removeFreeBlkFromFreeListTbl(mm_handle, nextMH, tmpSize);

            mh->size = mh->size + nextMH->size + sizeof(QuickFitMM_Header);
            mh->isLast = nextMH->isLast;

            /* previous block of next block of next block */
            if(!nextMH->isLast) {
                tmpMH = (QuickFitMM_Header *)((char *)&nextMH[1] + nextMH->size);
#ifdef QuickFitMM_TRACE
                if(tmpMH->prevBlock!=nextMH) QUICKFITMM_ERR(eINTERNAL_QuickFitMM);
#endif
                tmpMH->prevBlock = mh;
            }
        }
#ifdef QuickFitMM_TRACE
        nextMH->isFree = 0;
#endif
    }

    /* set this block is free */
    mh->isFree = 1;
    tmpSize = getIndexOfFreeListTbl(mh->size);
    /* return this block to free list table */
    _quickFitMM_insertFreeBlkToFreeListTbl(mm_handle, mh, tmpSize);

    /* 
     *  For CMPARK , We here return the size of 
     *  freed object rather eNOERROR 
     */
    return mh->size;
}

