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
 * Module : util_QuickSort.c
 *
 * Description :
 *  sort given tuples by 'Quick Sort' method
 *
 * Exports :
 *  Four util_QuickSort(void**,Four)
 */


#include "common.h"
#include "trace.h"
#include "error.h"
#include "Util_Sort.h"
#include "perProcessDS.h"
#include "perThreadDS.h"


/*=========================================
 * util_QuickSort()
 *========================================*/
/*
 * Function : util_QuickSort(SortTupleDesc*, void**, Four)
 *
 * Description :
 *  sort given tuples by 'Quick Sort' method
 *
 * Return Values :
 *  Error codes
 *
 * Side effects :
 *  contents of tuples will be sorted
 */
Four util_QuickSort(
    Four           	handle,
    SortTupleDesc* 	sortTupleDesc,            /* IN */
    void**       	tuples,                   /* INOUT pointer array which points each object containg tuple */
    Four         	numTuples)                /* IN number of tuples */
{
    Partition   	stack[MAXSTACKDEPTH];     /* stack for emulating recursive call */
    Four        	stackPtr = 0;             /* index of 'stack' */
    Four        	curStart, curEnd;         /* variables which indicate current sorting partition */
    void*       	tmp;                      /* temporary variable for swapping */
    Four        	i, j;                     /* index variable */
    void*       	pivot;                    /* pivot which indicates split point */
    static Seed         rand=0;                   /* varible for generating random variable */


    /* initialize 'curStart' & 'curEnd' */
    curStart = 0;
    curEnd = numTuples - 1;

    while (1) {

        /* if partition size is smaller than limit value, other method will be used */
        if (curEnd - curStart < QUICKSORTLIMIT) {

            /* insertion sort!! */
            for (i = curEnd; i > curStart; i-- ) {
                pivot = tuples[i];
                for (j = curStart; j < i; j++ ) {
                    if (util_SortKeyCompare(handle, sortTupleDesc, pivot, tuples[j]) < 0) {
                        tmp = tuples[j];
                        tuples[j] = pivot;
                        pivot = tmp;
                    }
                 }
                 tuples[i] = pivot;
            }

            /* stack empty */
            if (--stackPtr < 0) break;

            /* pop!! */
            curStart = stack[stackPtr].start;
            curEnd = stack[stackPtr].end;
            continue;
        }

        /* by random number, determine pivot!! */
        rand = (rand*1103515245 +12345) & 0x7fffffff;
        pivot = tuples[curStart+rand%(curEnd-curStart)];

        /* split!! */
        i = curStart; j = curEnd;
        while (i <= j) {
            while (util_SortKeyCompare(handle, sortTupleDesc, tuples[i], pivot) < 0) i++;
            while (util_SortKeyCompare(handle, sortTupleDesc, pivot, tuples[j]) < 0) j--;
            if (i < j) {
                tmp = tuples[i]; tuples[i] = tuples[j]; tuples[j] = tmp;
            }
            if (i <= j) {
                i++; j--;
            }
        }

        /* push the 'larger' partition on stack */
        if (j-curStart < curEnd-i) {
            if (i < curEnd) {
                stack[stackPtr].start = i;
                stack[stackPtr++].end = curEnd;

                /* error check */
                if (stackPtr >= MAXSTACKDEPTH) ERR(handle, eOVERFLOWQUICKSORTSTACK_UTIL);
            }
            curEnd = j;
        }
        else {
            if (curStart < j) {
                stack[stackPtr].start = curStart;
                stack[stackPtr++].end = j;
            }
            curStart = i;
        }
    }


    return(eNOERROR);

} /* util_QuickSort() */
