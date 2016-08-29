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
 * Module: SM_NextIndexBulkLoad.c
 *
 * Description :
 *  Next phase of B+ tree index bulkload.
 *
 * Exports:
 *  Four SM_NextIndexBulkLoad(Four, KeyValue*, ObjectID*)
 */


#include <string.h>
#include "common.h"
#include "trace.h"
#include "Util_Sort.h"
#include "OM_Internal.h"
#include "BtM.h"
#include "SM_Internal.h"
#include "BL_SM_Internal.h"
#include "perThreadDS.h"
#include "perProcessDS.h"




/*@===========================
 * SM_NextIndexBulkLoad()
 *===========================*/
/*
 * Function: Four SM_NextIndexBulkLoad(Four, KeyValue*, ObjectID*)
 *
 * Description: 
 *  Put given <key, oid> into sort stream for B+ tree index bulkload. 
 *  
 * Returns:
 *  error code
 *    eBADPARAMETER_SM
 *    some errors caused by function calss
 *
 * Side Effects:
 *
 */
Four _SM_NextIndexBulkLoad (
    Four handle,
    Four                   indexBlkLdId,           /* IN index bulkload id */
    KeyValue               *key,                   /* IN key value of the inseted ObjectID */
    ObjectID               *oid)                   /* IN ObjectID to insert */
{
    Four                   e;                      /* error number */
    SortStreamTuple        sortTuples;             /* tuple for sort stream */
    char                   tuples[2*MAXKEYLEN];    /* buffer for temporary object */
    SM_IdxBlkLdTableEntry* blkLdEntry;             /* entry in which information about bulkload is saved */


    TR_PRINT(TR_SM, TR1, 
            ("SM_NextIndexBulkLoad(handle, indexBlkLdId=%ld, key=%P, oid=%P)",
            indexBlkLdId, key, oid));


    /*
    **  O. Check parameters 
    */

    if (indexBlkLdId < 0)   ERR(handle, eBADPARAMETER_SM); 

    if (key == NULL)        ERR(handle, eBADPARAMETER_SM);

    if (key->len > MAXKEYLEN) ERR(handle, eEXCEEDMAXKEYLENGTH_SM); 

    if (oid == NULL)        ERR(handle, eBADPARAMETER_SM);


    /*
    **  I. set entry for fast access
    */
    blkLdEntry = &SM_IDXBLKLD_TABLE(handle)[indexBlkLdId];


    /*
    **  II. Make sortTuples which will be inserted into SortStream 
    */

    sortTuples.len  = key->len + sizeof(ObjectID);
    sortTuples.data = &tuples[0];
    memcpy(&sortTuples.data[0], &key->val, key->len);
    memcpy(&sortTuples.data[key->len], oid, sizeof(ObjectID));


    /* 
    **  III. Put sortTuples into sort stream
    */

    e = Util_PutTuplesIntoSortStream(handle, blkLdEntry->streamId, 1, &sortTuples);
    if (e < 0) ERR(handle, e);


    return eNOERROR;

}   /* SM_NextIndexBulkLoad() */
