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
 * Module : util_LoserTree.c
 *
 * Description :
 *  Manipulate 'Tree of Loser'.
 *
 * Exports :
 *  Four util_CreateLoserTree(void**, Four*, Four)
 *  Four util_FixLoserTree(void **, Four*, Four, Four)
 */


#include "common.h"
#include "Util_Sort.h"
#include "perProcessDS.h"
#include "perThreadDS.h"


/*================================================
 * util_CreateLoserTree()
 *===============================================*/
/*
 * Function : util_CreateLoserTree(void**, Four*, Four)
 *
 * Description :
 *  Create 'Tree of Loser'
 *
 * Return Values :
 *  Error Codes
 *
 * Side effects :
 *  array 'loserTree' will contains internal node of 'Tree of Loser'
 */
Four util_CreateLoserTree(
    Four    handle,
    SortTupleDesc* sortTupleDesc,
    void**  tuples,         /* IN array which contains pointers to tuples and these are leaf nodes of 'Tree of Loser' */
    Four*   loserTree,      /* INOUT internal nodes of 'Tree of Loser' */
    Four    treeSize)       /* IN number of 'Tree of Loser''s leaf nodes */
{
    Four    i, j, k;        /* variables for indexing */
    Four    winner;         /* variable which contains index of winner, i.e. smaller one */
    Four    tmp;            /* temporary variable for swapping */


    /* initialize 'Tree of Loser' */
    for (i = 0; i < treeSize; i++ ) loserTree[i] = -1;

    /* i -> index of 'tuples', j-> index of 'loserTree' */

    /* initialize index */
    i = 0;
    j = treeSize/2;

    /* determine each internal node's value */
    if (treeSize&CONSTANT_ONE == 1) {
        loserTree[j] = i;
        i+=1;
        j+=1;
    }
    else {
        if (util_SortKeyCompare(handle, sortTupleDesc, tuples[i], tuples[i+1]) > 0) {
            loserTree[j] = i;
            loserTree[j/2] = i+1;
        }
        else {
            loserTree[j] = i+1;
            loserTree[j/2] = i;
        }

        i+=2;
        j+=1;
    }
    while (j < treeSize) {

        /* determine winner */
        if (util_SortKeyCompare(handle, sortTupleDesc, tuples[i], tuples[i+1]) > 0) {
            loserTree[j] = i;
            winner = i+1;
        }
        else {
            loserTree[j] = i+1;
            winner = i;
        }

        /* propagate the winner upwards */
        k = j/2;
        while (1) {
            if (loserTree[k] == -1) {
                loserTree[k] = winner;
                break;
            }
            if (util_SortKeyCompare(handle, sortTupleDesc, tuples[loserTree[k]],tuples[winner]) < 0) {
                tmp = loserTree[k];
                loserTree[k] = winner;
                winner = tmp;
            }
            k /= 2;
        }

        i+=2;
        j+=1;
    }

    return(eNOERROR);
}


/*================================================
 * util_FixLoserTree()
 *===============================================*/
/*
 * Function : util_FixLoserTree(void**, Four*, Four, Four)
 *
 * Description :
 *  Insert new tuple in place of old winner and Update 'Tree of Loser'
 *
 * Return Values :
 *  Index of new winner
 *
 * Side effects :
 *  array 'loserTree' will contains internal node of updated 'Tree of Loser'
 */
Four util_FixLoserTree(
    Four    handle,
    SortTupleDesc* sortTupleDesc,
    void**  tuples,         /* IN    array which contains pointers to tuples and these are leaf nodes of 'Tree of Loser' */
    Four*   loserTree,      /* INOUT internal nodes of 'Tree of Loser' */
    Four    treeSize,       /* IN    number of 'Tree of Loser''s leaf node */
    Four    oldWinner)      /* IN    index of old winner in 'loserTree' */
{
    Four    i;              /* index variable */
    Four    tmp;            /* temporary variable for swapping */
    Four    winner;         /* variable which contains index of winner */


    /* initialize winner */
    winner = oldWinner;

    /* propagate to root */
    for (i = (treeSize+winner)/2; i > 0; i /= 2 ) {
        if (util_SortKeyCompare(handle, sortTupleDesc, tuples[loserTree[i]], tuples[winner]) < 0) {
            tmp = loserTree[i];
            loserTree[i] = winner;
            winner = tmp;
        }
    }

    /* set winner of 'Tree of Loser' - loserTree[0] must always contain winner */
    loserTree[0] = winner;


    return(winner);
}
