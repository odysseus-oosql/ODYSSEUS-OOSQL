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
 * Module: BtM_NextSortedAppendBulkLoad.c
 *
 * Description:
 *  Next phase of B+ tree index append bulkload.
 *
 * Exports:
 *  Four BtM_NextSortedAppendBulkLoad(KeyValue*, ObjectID*)
 *
 */


#include "common.h"
#include "error.h"
#include "trace.h"
#include "Util_Sort.h"
#include "BtM.h"
#include "BL_BtM.h"
#include "perThreadDS.h"
#include "perProcessDS.h"





/*@===============================
 * BtM_NextSortedAppendBulkLoad()
 *===============================*/
/*
 * Function: Four BtM_NextSortedAppendBulkLoad(KeyValue*, ObjectID*)
 *
 * Description:
 *  Put given <key, oid> into B+ tree index.
 *
 * Returns:
 *  error code
 *    eBADPARAMETER
 *    some errors caused by function calls
 *
 * Side Effects:
 *
 */
Four BtM_NextSortedAppendBulkLoad (
    Four		    handle,
    XactTableEntry_T        *xactEntry,             /* IN transaction table entry */
    Four                    btmBlkLdId,             /* IN BtM bulkload ID */
    FileID                  *fid,                   /* IN FileID */ 
    KeyValue                *key,                   /* IN key value of the inseted ObjectID */
    ObjectID                *oid,                   /* IN ObjectID to insert */
    LogParameter_T          *logParam)              /* IN log parameter */
{
    Four                    e;                      /* error number */
    Four                    compResult;             /* compare result of key values */
    Boolean                 againFlag = FALSE;      /* flag to determin looping or not */
    BtM_BlkLdTableEntry*    blkLdEntry;             /* entry in which information about bulkload is saved */



    TR_PRINT(handle, TR_BTM, TR1,
             ("BtM_NextSortedAppendBulkLoad(xactEntry=%P, btmBlkLdId=%ld, key=%P, oid=%P, logParam=%P)",
             xactEntry, btmBlkLdId, key, oid, logParam));


    /*
    ** O. set entry for fast access
    */

    blkLdEntry = &BTM_BLKLD_TABLE(handle)[btmBlkLdId];



    /*
    ** I. Check parameters
    */

    if (xactEntry == NULL)              ERR(handle, eBADPARAMETER);

    if (key == NULL)                    ERR(handle, eBADPARAMETER);

    if (oid == NULL)                    ERR(handle, eBADPARAMETER);

    if (logParam == NULL)               ERR(handle, eBADPARAMETER);



    /*
    ** II. Get sorted <key,OID> list from the given SortedStream and the B+ tree index leaf,
    **     Insert <key,OID> list into B+ tree
    */

    /* 1. compare B+ tree's <key,oid> with sortstream's <key,oid> and insert smaller one */
    do {
        /* reset again flag */
        againFlag = FALSE;

        /* IF : B+ tree index leaf's scan isn't Finished */
        if (blkLdEntry->btmBlkLdscanInfo.currCursor.flag != CURSOR_EOS) {

            /* compare B+ tree's key with sortstream's key */
            compResult = btm_KeyCompare(handle, &blkLdEntry->btmBlkLdblkldInfo.kdesc, &blkLdEntry->btmBlkLdscanInfo.currCursor.key, key);

            /* if B+ tree's key == sortstream's key */
            if (compResult == EQUAL) {
                /* compare B+ tree's oid with sortstream's oid */
                compResult = btm_ObjectIdComp(handle, &blkLdEntry->btmBlkLdscanInfo.currCursor.oid, oid);
            }

            /* CASE1 : B+ tree's key > sortstream's key */
            if (compResult == GREAT) {
                /* insert sortstream's <key,oid> into leaf node of index */
                e = btm_BlkLdInsertLeaf(handle, xactEntry, btmBlkLdId, key, oid, logParam);
                if (e < 0)  ERR(handle, e);

                /* set again flag with FALSE */
                againFlag = FALSE;
            }

            /* CASE2 : B+ tree's key < sortstream's key */
            else {
                /* insert B+ tree's <key,oid> into leaf node of index */
                e = btm_BlkLdInsertLeaf(handle, xactEntry, btmBlkLdId, &blkLdEntry->btmBlkLdscanInfo.currCursor.key,
                                        &blkLdEntry->btmBlkLdscanInfo.currCursor.oid, logParam);
                if (e < 0)  ERR(handle, e);

                /* find next cursor in B+ tree index */
                e = BtM_FetchNext(handle, xactEntry, &blkLdEntry->btmBlkLdblkldInfo.iinfo,
                                  fid, 
                                  &blkLdEntry->btmBlkLdblkldInfo.kdesc,
                                  &blkLdEntry->btmBlkLdscanInfo.stopKval, blkLdEntry->btmBlkLdscanInfo.stopCompOp,
                                  &blkLdEntry->btmBlkLdscanInfo.currCursor, &blkLdEntry->btmBlkLdscanInfo.nextCursor,
                                  NULL);
                if (e < 0)  ERR(handle, e);
                blkLdEntry->btmBlkLdscanInfo.currCursor = blkLdEntry->btmBlkLdscanInfo.nextCursor;

                /* set again flag with TRUE */
                againFlag = TRUE;
            }
        }

        /* IF : B+ tree index leaf's scan is Finished */
        else {
            /* insert sortstream's <key,oid> into leaf node of index */
            e = btm_BlkLdInsertLeaf(handle, xactEntry, btmBlkLdId, key, oid, logParam);
            if (e < 0)  ERR(handle, e);
        }

    } while (againFlag == TRUE);



    return eNOERROR;

}   /* BtM_NextSortedAppendBulkLoad() */
