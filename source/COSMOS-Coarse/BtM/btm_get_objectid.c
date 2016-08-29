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
 * Module: btm_get_objectid.c
 *
 * Description:
 *  Get an ObjectID from the given leaf page or overflow page.
 *
 * Exports:
 *  Four btm_get_objectid_from_leaf(BtreeCursor*)
 *  Four btm_get_objectid_from_overflow(BtreeCursor*)
 */


#include "common.h"
#include "trace.h"
#include "BfM.h"
#include "BtM_Internal.h"
#include "perThreadDS.h"
#include "perProcessDS.h"



/*@================================
 * btm_get_objectid_from_leaf()
 *================================*/
/*
 * Function: Four btm_get_objectid_from_leaf(BtreeCursor*)
 *
 * Description:
 *  Get the ObjectID which is indicated by the given 'cursor' in the leaf page.
 *  The found ObjectID is returned via the given BtreeCursor.
 *
 * Returns:
 *  Error code
 *    some errors caused by function calls
 */
Four btm_get_objectid_from_leaf(
    Four handle,
    BtreeCursor         *cursor)                /* INOUT a position on the Btree */
{
    Four                e;                      /* error number */
    Two                 entryOffset;            /* starting offset of an entry */
    BtreeLeaf           *apage;                 /* a leaf page */
    btm_LeafEntry       *entry;                 /* a leaf entry */
    ObjectID            *oidArray;              /* ObjectID array */


    TR_PRINT(TR_BTM, TR1, ("btm_get_objectid_from_leaf(handle, cursor=%P)", cursor));

    e = BfM_GetTrain(handle, &(cursor->leaf), (char **)&apage, PAGE_BUF);
    if (e < 0) ERR(handle, e);
    
    entryOffset = apage->slot[-(cursor->slotNo)];
    entry = (btm_LeafEntry*)&(apage->data[entryOffset]);

    /* oidArray starts at an alignment boundary after the key value. */
    oidArray = (ObjectID*)&(entry->kval[ALIGNED_LENGTH(entry->klen)]);
    cursor->oid = oidArray[cursor->oidArrayElemNo];

    e = BfM_FreeTrain(handle, &(cursor->leaf), PAGE_BUF);
    if (e < 0) ERR(handle, e);

    return(eNOERROR);
    
} /* btm_get_objectid_from_leaf() */



/*@================================
 * btm_get_objectid_from_overflow()
 *================================*/
/*
 * Function: Four btm_get_objectid_from_overflow(BtreeCursor*)
 *
 * Description:
 *  Get the ObjectID indicated by the given 'cursor' in the overflow page.
 *  The found ObjectID is returned via the given BtreeCursor. 
 *
 * Returns:
 *  error code
 *    some errors caused by function calls
 */
Four btm_get_objectid_from_overflow(
    Four handle,
    BtreeCursor *cursor)		/* INOUT a position on the Btree(overflow page) */
{
    Four e;			/* error number */
    BtreeOverflow *opage;	/* a page pointer to the overflow page */

    
    TR_PRINT(TR_BTM, TR1,
             ("btm_get_objectid_from_overflow(handle, cursor=%P)", cursor));
    
    e = BfM_GetTrain(handle, &(cursor->overflow), (char **)&opage, PAGE_BUF);
    if (e < 0) ERR(handle, e);
    
    /* Constrcut the ObjectID whose offset is given by 'cursor'. */
    cursor->oid = opage->oid[cursor->oidArrayElemNo];
    
    e = BfM_FreeTrain(handle, &(cursor->overflow), PAGE_BUF);
    if (e < 0) ERR(handle, e);

    return(eNOERROR);
    
} /* btm_get_objectid_from_overflow() */
