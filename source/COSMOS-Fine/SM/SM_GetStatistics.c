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
 * Module: SM_GetStatistics.c
 *
 * Description:
 *  Get Statictics Information
 */


#include "common.h"
#include "error.h"
#include "trace.h"
#include "latch.h"
#include "TM.h"
#include "LM.h"
#include "RDsM.h"
#include "BtM.h"
#include "OM.h"
#include "SM.h"
#include "perProcessDS.h"
#include "perThreadDS.h"


Four SM_GetStatistics_numDataFileAndIndex(
    Four            handle,
    Four            volId,                 /* IN  */
    Four*           numDataFiles,          /* OUT */
    sm_NumIndexes*  numIndexes,            /* OUT */
    LockParameter*  lockup)                /* IN request lock or not */
{
    Four            e;
    Four            v;
    Four            scanId;
    Four            count1, count2;
    FileID          fid;
    sm_CatOverlayForSysIndexes catOverlayForSysIndexes;

    TR_PRINT(handle, TR_SM, TR1,
             ("SM_GetStatistics_numDataFileAndIndex(handle, volId=%ld, numDataFiles=%P, numIndexes=%P, lockup=%P)",
              volId, numDataFiles, numIndexes, lockup));

    /*@
    ** check parameters
    */
    /* find the given volume in the scan manager mount table */
    for (v = 0; v < MAXNUMOFVOLS; v++)
        if (SM_MOUNTTABLE[v].volId == volId) break; /* found */
    if (v == MAXNUMOFVOLS) ERR(handle, eNOTMOUNTEDVOLUME_SM);

    if (numDataFiles == NULL) ERR(handle, eBADPARAMETER);
    if (numIndexes == NULL)   ERR(handle, eBADPARAMETER);


    /*@
    ** count # of data files
    */

    /* Get the file's ID of SM_SYSTABLES */
    fid = SM_MOUNTTABLE[v].sysTablesInfo.fid;

    /* open sequential scan of SM_SYSTABLES */
    scanId = SM_OpenSeqScan(handle, &fid, FORWARD, lockup);
    if (scanId < 0) ERR(handle, scanId);

    /* count catalog entries in SM_SYSTABLES */
    for (count1 = 0; ; count1++) {

        /* move cursor to next catalog entry */
        e = SM_NextObject(handle, scanId, NULL, NULL, NULL, NULL, lockup);
        if (e < eNOERROR) ERR(handle, e);

        /* boundary check */
        if (e == EOS) break;
    }

    /* set 'numDataFiles' */
    *numDataFiles = count1;

    /* close scan */
    e = SM_CloseScan(handle, scanId);
    if (e < eNOERROR) ERR(handle, e);


    /*@
    ** count # of indexes
    */

    /* Get the file's ID of SM_SYSINDEXES */
    fid = SM_MOUNTTABLE[v].sysIndexesInfo.fid;

    /* open sequential scan of SM_SYSINDEXES */
    scanId = SM_OpenSeqScan(handle, &fid, FORWARD, lockup);
    if (scanId < 0) ERR(handle, scanId);

    /* count catalog entries in SM_SYSINDEXES */
    for (count1 = 0, count2 = 0; ; ) {

        /* move cursor to next catalog entry */
        e = SM_NextObject(handle, scanId, NULL, NULL, NULL, NULL, lockup);
        if (e < eNOERROR) ERR(handle, e);

        /* boundary check */
        if (e == EOS) break;

        /* read a catalog entry of the sm_CatOverlayForSysIndexes. */
        e = SM_FetchObject(handle, scanId, NULL, 0, sizeof(sm_CatOverlayForSysIndexes),
                           (char*)&catOverlayForSysIndexes, lockup);
        if (e < eNOERROR) ERR(handle, e);

        /* count # of indexes for each type */
        switch( catOverlayForSysIndexes.indexType ) {
            case SM_INDEXTYPE_BTREE : count1++; break;
            case SM_INDEXTYPE_MLGF  : count2++; break;
            default : ERR(handle, eINTERNAL);
        }
    }

    /* set 'numIndexes' */
    numIndexes->numBtrees = count1;
    numIndexes->numMLGFs = count2;

    /* close scan */
    e = SM_CloseScan(handle, scanId);
    if (e < eNOERROR) ERR(handle, e);


    return eNOERROR;
}


Four SM_GetStatistics_DataFilePageInfo(
    Four            handle,
    FileID*         fid,                   /* IN  */
    PageID*         startPid,              /* IN  */
    Four*           numPinfoArray,         /* INOUT */
    sm_PageInfo*    pinfoArray,            /* OUT */
    LockParameter*  lockup)                /* IN request lock or not */
{
    Four            e;
    Four            i;
    Four            v;                     /* index for the used volume on the mount table */
    DataFileInfo    finfo;                 /* data file info */
    LockReply       lockReply;
    LockMode        oldMode;

    TR_PRINT(handle, TR_SM, TR1,
             ("SM_GetStatistics_DataFilePageInfo(handle, fid=%P, startPid=%P, numPinfoArray=%P, pinfoArray=%P, lockup=%P)",
              fid, startPid, numPinfoArray, pinfoArray, lockup));

    /* find the given volume in the scan manager mount table */
    for (v = 0; v < MAXNUMOFVOLS; v++)
        if (SM_MOUNTTABLE[v].volId == fid->volNo) break; /* found */

    if (v == MAXNUMOFVOLS) ERR(handle, eNOTMOUNTEDVOLUME_SM);

    /* Check if the file is a temporary file. */
    finfo.fid = *fid;
    finfo.tmpFileFlag = FALSE; /* initialize */
    for (i = 0; i < SM_NUM_OF_ENTRIES_OF_ST_FOR_TMP_FILES(handle); i++) 
        if (!SM_IS_UNUSED_ENTRY_OF_ST_FOR_TMP_FILES(SM_ST_FOR_TMP_FILES(handle)[i]) &&
            EQUAL_FILEID(*fid, SM_ST_FOR_TMP_FILES(handle)[i].data.fid)) {

            finfo.tmpFileFlag = TRUE;
            finfo.catalog.entry = &(SM_ST_FOR_TMP_FILES(handle)[i]);
            break;
        }
    if (finfo.tmpFileFlag == FALSE) {
        if (lockup) {
            if(lockup->duration != L_COMMIT) ERR(handle, eCOMMITDURATIONLOCKREQUIRED_SM);

            /* lock on the data file */
            e = LM_getFileLock(handle,  &MY_XACTID(handle), fid, lockup->mode, lockup->duration,
                                L_UNCONDITIONAL, &lockReply, &oldMode);
            if ( e < eNOERROR ) ERR(handle, e);

            if (lockReply == LR_DEADLOCK) ERR(handle, eDEADLOCK);
        }

        /* Get catalog entry of */
        e = sm_GetCatalogEntryFromDataFileId(handle, v, fid, &(finfo.catalog.oid));
        if (e < eNOERROR) ERR(handle, e);
    }

    e = OM_GetStatistics_DataFilePageInfo(handle, MY_XACT_TABLE_ENTRY(handle), &finfo, startPid, numPinfoArray, pinfoArray, lockup);
    if (e < eNOERROR) ERR(handle, e);


    return(eNOERROR);
}

Four _SM_GetStatistics_numExtents(
    Four         handle,
    Four         volId,
    Four*        extentSize,               /* OUT extent size */
    Four*        nTotalExtents,            /* OUT # of total extents */
    Four*        nUsedExtents)             /* OUT # of used extents */
{
    Four         e;

    TR_PRINT(handle, TR_SM, TR1,
             ("SM_GetStatistics_numExtents(volId=%ld, extentSize=%P, nTotalExtents=%P, nUsedExtents=%P)",
              volId, extentSize, nTotalExtents, nUsedExtents));

    e = RDsM_GetStatistics_numExtents(handle, volId, extentSize, nTotalExtents, nUsedExtents);
    if (e < eNOERROR) ERR(handle, e);

    return(eNOERROR);
}

Four _SM_GetStatistics_numPages(
    Four         handle,
    Four         volId,                 /* IN  volume id */
    sm_NumPages* numPages,              /* OUT # of pages */
    Boolean      getKindFlag,           /* IN  get kind of page if TRUE */ 
    Boolean      bitmapPrintFlag)       /* IN  print bitmap if TRUE */
{
    Four         e;

    TR_PRINT(handle, TR_SM, TR1,
             ("SM_GetStatistics_numPages(volId=%ld, numPages=%P, bitmapPrintFlag=%ld)",
             volId, numPages, bitmapPrintFlag));

    e = RDsM_GetStatistics_numPages(handle, volId, numPages, getKindFlag, bitmapPrintFlag); 
    if (e < eNOERROR) ERR(handle, e);

    return(eNOERROR);
}
