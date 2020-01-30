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
 * Module: SM_FinalIndexBulkLoad.c
 *
 * Description :
 *  Finalize B+ tree index bulkload
 *
 * Exports:
 *  Four SM_FinalIndexBulkLoad(Four, IndexID*, Two, Two)
 */


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
 * SM_FinalIndexBulkLoad()
 *===========================*/
/*
 * Function: Four SM_FinalIndexBulkLoad(Four, IndexID*, Two, Two)
 *
 * Description:
 *  Sort given sort stream, and then build B+ tree index using sorted <key, oid> list
 *
 * Returns:
 *  error code
 *    eBADPARAMETER
 *    some errors caused by function calss
 *
 * Side Effects:
 *
 */
Four SM_FinalIndexBulkLoad (
    Four		    handle,
    Four                    indexBlkLdId,           /* IN index bulkload id */
    IndexID                 *iid,                   /* IN B+ tree where the given ObjectID is inserted */
    Two                     eff,                    /* IN Extent fill factor */
    Two                     pff,                    /* IN Page fill factor */
    LockParameter      	    *lockup)                /* IN lockup parameter for data volume */
{
    Four                    e;                      /* error number */
    Four                    v;                      /* index for the used volume on the mount table */
    SM_IdxBlkLdTableEntry*  blkLdEntry;             /* entry in which information about bulkload is saved */
    BtreeIndexInfo          iinfo;                  /* index information */
    PhysicalIndexID         pIid;
    ObjectID                catObjForIdx;           /* catalog object of B+ tree file */
    LogParameter_T          logParamForSortStream;
    LogParameter_T          logParamForBulkload;


    TR_PRINT(handle, TR_SM, TR1,
            ("SM_FinalIndexBulkLoad(indexBlkLdId=%ld, iid=%P, eff=%ld, pff=%ld)",
            indexBlkLdId, iid, eff, pff));


    /*
    **  O. Check parameters
    */

    if (indexBlkLdId < 0)               ERR(handle, eBADPARAMETER);

    if (iid == NULL)                    ERR(handle, eBADPARAMETER);

    if (eff < 0 || eff > 100)           ERR(handle, eBADPARAMETER);

    if (pff < MINPFF || pff > MAXPFF)   ERR(handle, eBADPARAMETER);



    /*
    **  I. set entry for fast access
    */
    blkLdEntry = &SM_IDXBLKLD_TABLE(handle)[indexBlkLdId];


    /*
    **  II. Get catalog entry for the given B+ tree file
    */

    /* 1. find the given volume in the scan manager mount table */
    for (v = 0; v < MAXNUMOFVOLS; v++)
        if (SM_MOUNTTABLE[v].volId == iid->volNo) break; /* found */

    if (v == MAXNUMOFVOLS) ERR(handle, eNOTMOUNTEDVOLUME_SM);



    /* 2. get the catalog object for the given B+ tree index. */
    e = sm_GetCatalogEntryFromIndexId(handle, v, iid, &catObjForIdx, &pIid);
    if (e < 0)  ERR(handle, e);


    /* 3. get the index-info for the given B+ tree index. */
    e = sm_GetIndexInfoFromIndexId(handle, v, iid, &iinfo, &blkLdEntry->fid); 
    if (e < 0)  ERR(handle, e);



    /*
    **  III. Sort given stream - <key, oid> list
    */
    SET_LOG_PARAMETER(logParamForSortStream, common_shmPtr->recoveryFlag, FALSE);

    e = Util_SortingSortStream(handle, MY_XACT_TABLE_ENTRY(handle), blkLdEntry->streamId, &logParamForSortStream);
    if (e < eNOERROR) ERR(handle, e);



    /*
    **  IV. Find which to do append bulkload or normal bulkload
    */

    e = BtM_IsAppendBulkLoad(handle, (PageID*)&pIid, &blkLdEntry->smBlkLdisAppend);
    if (e < eNOERROR) ERR(handle, e);



    /*
    **  V. Bulkload B+ tree index given sort <key, oid> list
    */
    SET_LOG_PARAMETER(logParamForBulkload, common_shmPtr->recoveryFlag, iinfo.tmpIndexFlag);

    if (blkLdEntry->smBlkLdisAppend == TRUE) {
        e = BtM_AppendBulkLoad(handle, MY_XACT_TABLE_ENTRY(handle), &iinfo, &blkLdEntry->fid, (PageID*)&pIid, blkLdEntry->streamId, eff, pff, &logParamForBulkload); 
        if (e < eNOERROR) ERR(handle, e);
    }
    else {
        e = BtM_BulkLoad(handle, MY_XACT_TABLE_ENTRY(handle), &iinfo, (PageID*)&pIid, blkLdEntry->streamId, eff, pff, &logParamForBulkload);
        if (e < eNOERROR) ERR(handle, e);
    }



    /*
    **  VI. Close the given sort stream
    */

    e = Util_CloseSortStream(handle, MY_XACT_TABLE_ENTRY(handle), blkLdEntry->streamId, &logParamForSortStream);
    if (e < eNOERROR) ERR(handle, e);


    /*
    **  VII. empty entry of SM bulkload table
    */

    blkLdEntry->isUsed = FALSE;


    return eNOERROR;

}   /* SM_FinalIndexBulkLoad() */

