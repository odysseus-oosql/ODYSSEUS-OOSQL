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
 * Module: SM_IndexBulkInsert.c
 *
 * Description :
 *  Initialize B+ tree index bulkload.
 *
 * Exports:
 *  Four SM_InitIndexBulkInsert(VolID, KeyDesc *)
 *  Four SM_NextIndexBulkInsert(Four, KeyValue*, ObjectID*)
 *  Four SM_FinalIndexBulkInsert(Four, IndexID*, KeyDesc*)
 */


#include <assert.h>
#include <string.h>
#include "common.h"
#include "error.h"
#include "trace.h"
#include "Util_Sort.h"
#include "BtM.h"
#include "SM.h"
#include "BL_BtM.h"
#include "BL_SM.h"
#include "perThreadDS.h"
#include "perProcessDS.h"


/*@===========================
 * SM_InitIndexBulkInsert()
 *===========================*/
/*
 * Function: Four SM_InitIndexBulkInsert(VolID, KeyDesc *)
 *
 * Description:
 *  Initialize B+ tree index bulkload.
 *
 * Returns:
 *  error code
 *    eBADPARAMETER_SM
 *    some errors caused by function calss
 *
 * Side Effects:
 *  parameter indexBlkLdId is filled with index bulkload ID
 */
Four SM_InitIndexBulkInsert (
    Four			handle,
    VolID                  	volId,                  /* IN volume ID in which temporary files are allocated */
    KeyDesc                	*kdesc)                 /* IN key descriptor of the given B+ tree */
{
    TR_PRINT(handle, TR_SM, TR1,
            ("SM_InitIndexBulkInsert(volId=%ld, kdesc=%P)", volId, kdesc));


    return SM_InitIndexBulkLoad(handle, volId, kdesc);

}   /* SM_InitIndexBulkInsert() */


/*@===========================
 * SM_NextIndexBulkInsert()
 *===========================*/
/*
 * Function: Four SM_NextIndexBulkInsert(Four, KeyValue*, ObjectID*)
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
Four SM_NextIndexBulkInsert (
    Four		   handle,
    Four                   indexBlkLdId,           /* IN index bulkload id */
    KeyValue               *key,                   /* IN key value of the inseted object */
    ObjectID               *oid)                   /* IN ObjectID to insert */
{
    TR_PRINT(handle, TR_SM, TR1,
            ("SM_NextIndexBulkInsert(indexBlkLdId=%ld, key=%P, oid=%P)", indexBlkLdId, key, oid));

    return SM_NextIndexBulkLoad(handle, indexBlkLdId, key, oid);

}   /* SM_NextIndexBulkInsert() */


/*@===========================
 * SM_FinalIndexBulkInsert()
 *===========================*/
/*
 * Function: Four SM_FinalIndexBulkInsert(Four, IndexID*, KeyDesc*)
 *
 * Description:
 *  Sort given sort stream, and then build B+ tree index using sorted <key, oid> list
 *
 * Returns:
 *  error code
 *    eBADPARAMETER_SM
 *    some errors caused by function calss
 *
 * Side Effects:
 *
 */
Four SM_FinalIndexBulkInsert (
    Four		handle,
    Four                indexBlkLdId,           /* IN index bulkload id */
    IndexID             *iid,                   /* IN B+ tree where the given ObjectID is inserted */
    KeyDesc             *kdesc,                 /* IN key descriptor of the given B+ tree */
    LockParameter 	*lockup)  		/* IN request lock or not */
{
    Four                e;                      /* error number */
    Four                numSortTuples;          /* # of tuples from sortStream */
    SortStreamTuple     sortTuple;              /* tuple for sort stream */
    char                tuple[2*MAXKEYLEN];     /* buffer for temporary object */
    KeyValue            kval;                   /* a key value */
    ObjectID            oid;                    /* a Object ID */
    Boolean             done = FALSE;           /* flag which indicates sort stream is empty or not */
    SM_IdxBlkLdTableEntry* blkLdEntry;          /* entry in which information about bulkload is saved */
    LogParameter_T      logParamForSortStream;


    TR_PRINT(handle, TR_SM, TR1,
            ("SM_FinalIndexBulkInsert(indexBlkLdId=%ld, iid=%P)", indexBlkLdId, iid));


    /*
    **  O. Check parameters
    */

    if (indexBlkLdId < 0)               ERR(handle, eBADPARAMETER);
    if (iid == NULL)                    ERR(handle, eBADPARAMETER);


    /*
    **  I. set entry for fast access
    */
    blkLdEntry = &SM_IDXBLKLD_TABLE(handle)[indexBlkLdId];


    /*
    **  II. Sort given stream - <key, oid> list
    */
    SET_LOG_PARAMETER(logParamForSortStream, common_shmPtr->recoveryFlag, FALSE);

    e = Util_SortingSortStream(handle, MY_XACT_TABLE_ENTRY(handle), blkLdEntry->streamId, &logParamForSortStream);
    if (e < eNOERROR) ERR(handle, e);


    /*
    **  III. insert sorted <key, oid> list into index
    */

    while (1) {

        /* initialize numSortTuples */
        numSortTuples = 1;

        /* initialize sortTuple */
        sortTuple.len = 2*MAXKEYLEN;
        sortTuple.data = &tuple[0];

        /* get tuples from sort stream */
        e = Util_GetTuplesFromSortStream(handle, blkLdEntry->streamId, &numSortTuples, &sortTuple, &done);
        if (e < 0)  ERR(handle, e);

        /* end of sort stream check */
        if (done) break;

        /* assertion check */
        assert(numSortTuples == 1);

        /* get kval & oid */
        kval.len = sortTuple.len - sizeof(ObjectID);
        memcpy(kval.val, &sortTuple.data[0], kval.len);
        memcpy(&oid, &sortTuple.data[kval.len], sizeof(ObjectID));

        /* insert into index */
        e = SM_InsertIndexEntry(handle, iid, kdesc, &kval, &oid, lockup);
        if (e < eNOERROR) ERR(handle, e);
    }


    /*
    **  IV. Close the given sort stream
    */

    e = Util_CloseSortStream(handle, MY_XACT_TABLE_ENTRY(handle), blkLdEntry->streamId, &logParamForSortStream);
    if (e < eNOERROR) ERR(handle, e);


    /*
    **  V. empty entry of SM bulkload table
    */

    blkLdEntry->isUsed = FALSE;


    return eNOERROR;

}   /* SM_FinalIndexBulkInsert() */
