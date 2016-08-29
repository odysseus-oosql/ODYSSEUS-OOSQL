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
 * Module: SM_GetStatistics.c
 *
 * Description:
 *  Get Statictics Information
 */


#include "common.h"
#include "trace.h"
#include "RDsM_Internal.h"	
#include "BfM.h"
#include "BtM.h"
#include "OM_Internal.h"
#include "SM_Internal.h"
#include "perThreadDS.h"
#include "perProcessDS.h"

Four getFileID(Four, ObjectID *, FileID *);

Four SM_GetStatistics_numDataFileAndIndex(
    Four handle,
    Four            volId,                 /* IN  */
    Four*           numDataFiles,          /* OUT */
    sm_NumIndexes*  numIndexes)            /* OUT */
{
    Four            e;
    Four            v;
    Four            scanId;
    Two             count1, count2;
    Four            count;
    FileID          fid;
    sm_CatOverlayForSysIndexes catOverlayForSysIndexes;

    TR_PRINT(TR_SM, TR1,
             ("SM_GetStatistics_numDataFileAndIndex(handle, volId=%ld, numDataFiles=%P, numIndexes=%P)",
              volId, numDataFiles, numIndexes));
    
    /*@
    ** check parameters
    */
    /* find the given volume in the scan manager mount table */
    for (v = 0; v < MAXNUMOFVOLS; v++)
        if (SM_PER_THREAD_DS(handle).smMountTable[v].volId == volId) break; /* found */
    if (v == MAXNUMOFVOLS) ERR(handle, eNOTMOUNTEDVOLUME_SM);
    
    if (numDataFiles == NULL) ERR(handle, eBADPARAMETER_SM);
    if (numIndexes == NULL)   ERR(handle, eBADPARAMETER_SM);

     
    /*@
    ** count # of data files
    */

    /* Get the file's ID of SM_SYSTABLES */
    e = getFileID(handle, &(SM_PER_THREAD_DS(handle).smMountTable[v].sysTablesSysTablesEntry), &fid);
    if (e < 0) ERR(handle, e);

    /* open sequential scan of SM_SYSTABLES */
    scanId = _SM_OpenSeqScan(handle, &fid, FORWARD);
    if (scanId < 0) ERR(handle, scanId);

    /* count catalog entries in SM_SYSTABLES */
    for ( count=0; ; count++) {

        /* move cursor to next catalog entry */
        e = _SM_NextObject(handle, scanId, NULL, NULL, NULL, NULL); 
        if (e < 0) ERR(handle, e);

        /* boundary check */
        if (e == EOS) break;
    }

    /* set 'numDataFiles' */
    *numDataFiles = count;

    /* close scan */
    e = _SM_CloseScan(handle, scanId);
    if (e < 0) ERR(handle, e);


    /*@
    ** count # of indexes
    */

    /* Get the file's ID of SM_SYSINDEXES */
    e = getFileID(handle, &(SM_PER_THREAD_DS(handle).smMountTable[v].sysTablesSysIndexesEntry), &fid);
    if (e < 0) ERR(handle, e);

    /* open sequential scan of SM_SYSINDEXES */
    scanId = _SM_OpenSeqScan(handle, &fid, FORWARD);
    if (scanId < 0) ERR(handle, scanId);

    /* count catalog entries in SM_SYSINDEXES */
    for ( count1=0, count2=0; ; ) {

        /* move cursor to next catalog entry */
        e = _SM_NextObject(handle, scanId, NULL, NULL, NULL, NULL); 
        if (e < 0) ERR(handle, e);

        /* boundary check */
        if (e == EOS) break;

        /* read a catalog entry of the sm_CatOverlayForSysIndexes. */
        e = _SM_FetchObject(handle, scanId, NULL, 0, sizeof(sm_CatOverlayForSysIndexes),
                           (char*)&catOverlayForSysIndexes);
        if (e < 0) ERR(handle, e);

        /* count # of indexes for each type */
        switch( catOverlayForSysIndexes.indexType ) { 
            case SM_INDEXTYPE_BTREE : count1++; break;
            case SM_INDEXTYPE_MLGF  : count2++; break;
            default : ERR(handle, eINTERNAL_SM);
        }
    }

    /* set 'numIndexes' */
    numIndexes->numBtrees = count1;
    numIndexes->numMLGFs = count2;

    /* close scan */
    e = _SM_CloseScan(handle, scanId);
    if (e < 0) ERR(handle, e);


    return eNOERROR;
}

Four _SM_GetStatistics_BtreePageInfo(
    Four handle,
    IndexID       *iid,              /* IN the root of a Btree */
    Four          numPinfoArray,     /* IN */
    Four          *numAnswer,        /* OUT */
    sm_PageInfo   *pinfoArray)       /* OUT */
{
    Four          e;
    Four          v;
    ObjectID      dummy;
    PhysicalIndexID pIid;

    TR_PRINT(TR_SM, TR1,
             ("SM_GetStatistics_BtreePageInfo(pIid=%P, numPinfoArray=%ld, numAnswer=%P, pinfoArray=%P)",
              pIid, numPinfoArray, numAnswer, pinfoArray));

    /* find the given volume in the scan manager mount table */
    for (v = 0; v < MAXNUMOFVOLS; v++)
        if (SM_PER_THREAD_DS(handle).smMountTable[v].volId == iid->volNo) break; /* found */
    if (v == MAXNUMOFVOLS) ERR(handle, eNOTMOUNTEDVOLUME_SM);

    /* get physical index ID */
    e = sm_GetCatalogEntryFromIndexId(handle, v, iid, &dummy, &pIid, NULL);
    if (e < 0) ERR(handle, e);

    /* initialize 'numAnswer' */
    *numAnswer = 0;

    e = BtM_GetStatistics_BtreePageInfo(handle, &pIid, numPinfoArray, numAnswer, pinfoArray);
    if (e < 0) ERR(handle, e);

    return(eNOERROR);
}


Four _SM_GetStatistics_numExtents(
    Four handle,
    Four         volId,
    Two*         extentSize,               /* OUT extent size */
    Four*        nTotalExtents,            /* OUT # of total extents */
    Four*        nUsedExtents)             /* OUT # of used extents */
{
    Four         e;

    TR_PRINT(TR_SM, TR1,
             ("SM_GetStatistics_numExtents(volId=%ld, extentSize=%P, nTotalExtents=%P, nUsedExtents=%P)",
              volId, extentSize, nTotalExtents, nUsedExtents));
#ifdef DBLOCK
	e = SM_GetVolumeLock(handle, volId, L_S);
	if (e < 0) ERR(handle, e);
#endif

    e = RDsM_GetStatistics_numExtents(handle, volId, extentSize, nTotalExtents, nUsedExtents);
    if (e < 0) ERR(handle, e);

#ifdef DBLOCK
	e = SM_ReleaseVolumeLock(handle, volId);
	if (e < 0) ERR(handle, e);
#endif

    return(eNOERROR);
}


Four _SM_GetStatistics_numPages(
    Four handle,
    Four         volId,                 /* IN  volume id */
    sm_NumPages* numPages,              /* OUT # of pages */
    Boolean      getKindFlag,           /* IN  get kind of page if TRUE */ 
    Boolean      bitmapPrintFlag)       /* IN  print bitmap if TRUE */
{
    Four         e;

    TR_PRINT(TR_SM, TR1,
             ("SM_GetStatistics_numPages(volId=%ld, numPages=%P, bitmapPrintFlag=%ld)",
             volId, numPages, bitmapPrintFlag));

#ifdef DBLOCK
	e = SM_GetVolumeLock(handle, volId, L_S);
	if (e < 0) ERR(handle, e);
#endif

    e = RDsM_GetStatistics_numPages(handle, volId, numPages, getKindFlag, bitmapPrintFlag); 
    if (e < 0) ERR(handle, e);

#ifdef DBLOCK
	e = SM_ReleaseVolumeLock(handle, volId);
	if (e < 0) ERR(handle, e);
#endif

    return(eNOERROR);
}


Four getFileID(
    Four handle,
    ObjectID*    catObj,                /* IN */
    FileID*      fid)                   /* OUT */
{
    Four         e;
    SlottedPage* catPage;               /* pointer to buffer containing the catalog */
    sm_CatOverlayForData* catEntry;     /* pointer to data file catalog information */

    e = BfM_GetTrain(handle, (TrainID*)catObj, (char**)&catPage, PAGE_BUF);
    if (e < 0) ERR(handle, e);

    GET_PTR_TO_CATENTRY_FOR_DATA(catObj, catPage, catEntry);

    *fid = catEntry->fid;

    e = BfM_FreeTrain(handle, (TrainID*)catObj, PAGE_BUF);
    if (e < 0) ERR(handle, e);

    return eNOERROR;
}
