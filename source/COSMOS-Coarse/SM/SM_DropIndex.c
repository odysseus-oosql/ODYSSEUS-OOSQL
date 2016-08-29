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
 * Module: SM_DropIndex.c
 *
 * Description:
 *  Drop the given index.
 *
 * Exports:
 *  Four SM_DropIndex(IndexID*)
 */


#include <string.h>
#include "common.h"
#include "trace.h"
#include "OM_Internal.h"	
#include "BtM.h"
#include "SM_Internal.h"
#include "perThreadDS.h"
#include "perProcessDS.h"



/*@================================
 * SM_DropIndex()
 *================================*/
/*
 * Function: Four SM_DropIndex(IndexID*)
 *
 * Description:
 *  Drop the given index.
 *
 * Returns:
 *  error code
 *    eBADPARAMETER_SM
 *    eOPENINDEX_SM
 *    some errors caused by function calls
 */
Four _SM_DropIndex(
    Four handle,
    IndexID *index)          /* IN index to drop */
{
    Four     e;              /* error number */
    Four     v;              /* index for the used volume on the mount table */
    Four     i;              /* temporary variable */
    KeyValue kval;           /* key value of a B+ tree */
    PhysicalFileID  pFid;    /* physical B-tree file ID */
    PhysicalIndexID pIid;    /* physical B-tree index ID */
    ObjectID catObjForFile;  /* object identifier of the SYSTABLES catalog object */
    ObjectID catObjForIndex; /* object identifier of SYSINDEXES catalog object */
    sm_CatOverlayForBtree catalogOverlay; /* B-tree entry of SM_SYSTABLES */

    TR_PRINT(TR_SM, TR1, ("SM_DropIndex(handle, index=%P)", index));

    
    /*  
     * Check parameters 
     */

    if (index == NULL) ERR(handle, eBADPARAMETER_SM);
    

    /* 
     * Find the given volume in the scan manager mount table 
     */

    for (v = 0; v < MAXNUMOFVOLS; v++)
        if (SM_PER_THREAD_DS(handle).smMountTable[v].volId == index->volNo) break; /* found */
    
    if (v == MAXNUMOFVOLS) ERR(handle, eNOTMOUNTEDVOLUME_SM);
    

    /* 
     *  Is there an active transaction? 
     */

    if (!SM_PER_THREAD_DS(handle).xactRunningFlag) ERR(handle, eNOACTIVETRANSACTION_SM);


    /*
     *  Get needed information
     */

    /* get pIid & catObjForIndex */
    e = sm_GetCatalogEntryFromIndexId(handle, v, index, &catObjForFile, &pIid, &catObjForIndex);
    if (e < 0) ERR(handle, e);

    /* get catalogOverlay */
    e = OM_ReadObject(handle, &catObjForFile, SM_SYSTABLES_BTREE_START, sizeof(sm_CatOverlayForBtree), &catalogOverlay);
    if (e < 0) ERR(handle, e);

    /* set pFid */
    MAKE_PHYSICALFILEID(pFid, catalogOverlay.fid.volNo, catalogOverlay.firstPage);


    /*
    ** Check if a scan is opened on the dropped index.
    */

    for (i = 0; i < SM_PER_THREAD_DS(handle).smScanTable.nEntries; i++)
        if (SM_SCANTABLE(handle)[i].scanType == BTREEINDEX &&
            EQUAL_PHYSICALINDEXID(SM_SCANTABLE(handle)[i].scanInfo.btree.pIid, pIid)) 
            ERR(handle, eOPENINDEX_SM);
    

    /*
    ** Delete entry from index on indexID field of SM_SYSINDEXES.
    */

    /* Construct a key for the B+ tree index on IndexID field of SM_SYSINDEXES. */
    kval.len = sizeof(VolNo) + sizeof(Serial);
    memcpy(&(kval.val[0]), (char*)&(index->volNo), sizeof(VolNo));
    memcpy(&(kval.val[sizeof(VolNo)]), (char*)&(index->serial), sizeof(Serial));

    /* Delete the catalog entry from the B+ tree on SM_SYSINDEXES . */
    e = BtM_DeleteObject(handle, &(SM_PER_THREAD_DS(handle).smMountTable[v].sysTablesSysIndexesEntry),
                         &(SM_PER_THREAD_DS(handle).smMountTable[v].sysIndexesIndexIdIndex),
                         &SM_PER_THREAD_DS(handle).smSysIndexesIndexIdIndexKdesc,
                         &kval, &catObjForIndex, &SM_PER_THREAD_DS(handle).dlPool, &(SM_PER_THREAD_DS(handle).smMountTable[v].dlHead));
    if (e < 0) ERR(handle, e);


    /*
    ** Delete entry from index on BtreeFileId field of SM_SYSINDEXES.
    */

    /*@ construct kval2 */
    kval.len = sizeof(VolNo) + sizeof(Serial);
    memcpy(&(kval.val[0]), (char*)&(catalogOverlay.fid.volNo), sizeof(VolNo));
    memcpy(&(kval.val[sizeof(VolNo)]), (char*)&(catalogOverlay.fid.serial), sizeof(Serial));

    /* Delete the catalog entry from the B+ tree on SM_SYSINDEXES . */
    e = BtM_DeleteObject(handle, &(SM_PER_THREAD_DS(handle).smMountTable[v].sysTablesSysIndexesEntry),
                         &(SM_PER_THREAD_DS(handle).smMountTable[v].sysIndexesBtreeFileIdIndex),
                         &SM_PER_THREAD_DS(handle).smSysIndexesIndexFileIdIndexKdesc,
                         &kval, &catObjForIndex, &SM_PER_THREAD_DS(handle).dlPool, &(SM_PER_THREAD_DS(handle).smMountTable[v].dlHead)); 
    if (e < 0) ERR(handle, e);

    
    /* 
     *  Delete the catalog entry from the data file SM_SYSINDEXES. 
     */

    e = OM_DestroyObject(handle, &(SM_PER_THREAD_DS(handle).smMountTable[v].sysTablesSysIndexesEntry),
                         &catObjForIndex, &SM_PER_THREAD_DS(handle).dlPool, &(SM_PER_THREAD_DS(handle).smMountTable[v].dlHead));
    if (e < 0) ERR(handle, e);

    
    /* 
     *  Drop the index. 
     */

    e = BtM_DropIndex(handle, &pFid, &pIid, &SM_PER_THREAD_DS(handle).dlPool, &(SM_PER_THREAD_DS(handle).smMountTable[v].dlHead));
    if (e < 0) ERR(handle, e);

    
    return(eNOERROR);
    
} /* SM_DropIndex() */

