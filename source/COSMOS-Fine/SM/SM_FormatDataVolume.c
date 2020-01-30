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
 * Module: SM_FormatDataVolume.c
 *
 * Description:
 *  Format a file or a raw device so that it can be used as the volume of our
 *  storage system. This format is for Scan Manager level.
 *
 * Exports:
 *  Four SM_FormatDataVolume(Four, Four, char**, char*, Four, Four, Four, Four)
 */


#include <assert.h>
#include <stdio.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>

#include "common.h"
#include "error.h"
#include "trace.h"
#include "latch.h"
#include "TM.h"			/* for transaction begin/commit */
#include "LM.h"
#include "RDsM.h"
#include "BfM.h"
#include "BtM.h"
#include "OM.h"                 /* for createFirstObjectInSysTables() */
#include "SM.h"
#include "SHM.h"
#include "perProcessDS.h"
#include "perThreadDS.h"



#define EFF 100

/* Internal Function Prototype */
Four sm_FormatDataVolume(Four, XactTableEntry_T*, Four);
Four createFirstObjectInSysTables(Four, FileID*, ObjectHdr*, Four, char*, ObjectID*);


/*
 * Function: Four SM_FormatDataVolume(Four, Four, char**, char*, Four, Four, Four, Four, Boolean)
 *
 * Description:
 *  Format a data volume
 *
 * Returns:
 *  error code
 */
Four SM_FormatDataVolume(
    Four	handle, 		 /* IN handle */
    Four   	numDevices,              /* IN number of devices in formated volume */
    char    	**devNames,              /* IN array of device name */
    char    	*title,                  /* IN volume title */
    Four   	volId,                   /* IN volume number */
    Four   	extSize,                 /* IN number of pages in an extent */
    Four   	*numPagesInDevice,       /* IN array of extents' number */
    Four   	segmentSize              /* IN # of pages in an segment */
)
{
    Four   	e;                       /* error code */
    Four   	dummyVolId;              /* volume identifier */
    double 	deviceSize;          	 /* device size in bytes */
    Four   	i;
    XactID 	xactId;


    TR_PRINT(handle, TR_SM, TR1, ("sm_FormatDataOrTempDataVolume(numDevices=%lD, devNames=%P, title=%P, volId=%ld, extSize=%ld, numPagesInDevice=%P, segmentSize=%lD)",
                          numDevices, devNames, title, volId, extSize, numPagesInDevice, segmentSize));


    /*
     * Check parameters
     */
    if (volId < 0 || volId > MAX_VOLUME_NUMBER || extSize < 1) ERR(handle, eBADPARAMETER);


    for (i = 0; i < numDevices; i++) {
        deviceSize = (double)numPagesInDevice[i] * PAGESIZE;
        if (deviceSize > MAX_RAW_DEVICE_OFFSET) ERR(handle, eBADPARAMETER);
    }

    /*
     * Format the device in the RDsM level.
     */
    e = RDsM_Format(handle, numDevices, devNames, title, volId, extSize, numPagesInDevice, VOLUME_TYPE_DATA, EFF); 
    if (e < eNOERROR) ERR(handle, e);


    /*
     * Mount the formated volume.
     */
    e = RDsM_Mount(handle, numDevices, devNames, &dummyVolId, FALSE);
    if (e < eNOERROR) ERR(handle, e);

    assert(volId == dummyVolId);


    e = SM_BeginTransaction(handle, &xactId, X_RR_RR);
    if (e < eNOERROR) ERR(handle, e);

    e = sm_FormatDataVolume(handle, MY_XACT_TABLE_ENTRY(handle), volId);
    if (e < eNOERROR) ERR(handle, e);

    e = SM_CommitTransaction(handle, &xactId);
    if (e < eNOERROR) ERR(handle, e);

    /* Dismount the volume in BfM level. */
    e = BfM_dismount(handle, volId);
    if (e < eNOERROR) ERR(handle, e);

    /* Dismount the volume in RDsM level. */
    e = RDsM_Dismount(handle, volId, FALSE);
    if (e < eNOERROR) ERR(handle, e);

    return(eNOERROR);

} /* SM_FormatDataVolume() */


/*
 * Function: sm_FormatDataVolume()
 *
 * Description:
 *  Format the volume in the SM level.
 *
 * Returns:
 *  error code
 */
Four sm_FormatDataVolume(
    Four handle,
    XactTableEntry_T *xactEntry,
    Four volId)                 /* IN volume id */
{
    Four e;                     /* error code */
    KeyValue kval;		/* key value for B+ tree */
    ObjectID sysTablesSysTablesEntry; /* ObjectID of entry for SM_SYSTABLES in SM_SYSTABLES */
    ObjectID sysTablesSysIndexesEntry; /* ObjectID of entry for SM_SYSINDEXES in SM_SYSTABLES */
    sm_CatOverlayForSysTables  stForSt;	/* entry for SM_SYSTABLES in SM_SYSTABLES*/
    sm_CatOverlayForSysTables  stForSi;	/* entry for SM_SYSINDEXES in SM_SYSTABLES */
    sm_CatOverlayForSysIndexes siForSt;	/* entry for SM_SYSTABLES in SM_SYSINDEXES */
    sm_CatOverlayForSysIndexes siForSi;	/* entry for SM_SYSINDEXS in SM_SYSINDEXES */
    DataFileInfo sysTablesInfo; /* data file info for SM_SYSTABLES */
    DataFileInfo sysIndexesInfo; /* data file info for SM_SYSINDEXES */
    LogParameter_T logParam;
    FileID fid;					/* allocated file ID */
    PhysicalFileID pFid;			/* allocated file ID */
    Serial  serialForFile = 0;			/* serial number for FileID */
    Serial  serialForIndex = 0;			/* serial number for IndexID */

    ObjectID sysTablesDataFileIdIndexEntry;	/* ObjectID of entry for index on SM_SYSTABLES in SM_SYSINDEXES */
    ObjectID sysIndexesDataFileIdIndexEntry;	/* ObjectID of entry for index on SM_SYSINDEXES in SM_SYSINDEXES */
    ObjectID sysIndexesIndexIdIndexEntry;	/* ObjectID of entry for index on SM_SYSINDEXES in SM_SYSINDEXES */
    BtreeIndexInfo sysTablesDataFileIdIndexInfo;/* B+ tree on DataFileId of SM_SYSTABLES */
    BtreeIndexInfo sysIndexesIndexIdIndexInfo;	/* B+ tree on IndexId of SM_SYSINDEXES */
    BtreeIndexInfo sysIndexesDataFileIdIndexInfo;/* B+ tree on DataFileId of SM_SYSINDEXES */
    IndexID sysTablesDataFileIdIndexId;		/* B+ tree on DataFileId of SM_SYSTABLES */
    IndexID sysIndexesIndexIdIndexId;		/* B+ tree on IndexId of SM_SYSINDEXES */
    IndexID sysIndexesDataFileIdIndexId;	/* B+ tree on DataFileId of SM_SYSINDEXES */
    PhysicalIndexID sysTablesDataFileIdIndex;	/* B+ tree on DataFileId of SM_SYSTABLES */
    PhysicalIndexID sysIndexesIndexIdIndex;	/* B+ tree on IndexId of SM_SYSINDEXES */
    PhysicalIndexID sysIndexesDataFileIdIndex;	/* B+ tree on DataFileId of SM_SYSINDEXES */

    ObjectID oid;				/* ObjectID of the objects in SM_SYSCOUNTERS */
    sm_CatOverlayForSysTables  stForSc;		/* entry for SM_SYSCOUNTERS in SM_SYSTABLES */
    sm_CatOverlayForSysIndexes siForSc;		/* entry for SM_SYSCOUNTERS in SM_SYSINDEXES */
    sm_SysCountersOverlay_T sysSerialForFileCounter; /* system counter for serial in File ID */
    sm_SysCountersOverlay_T sysSerialForIndexCounter; /* system counter for seriial in Index ID */
    DataFileInfo sysCountersInfo;		/* data file info for SM_SYSCOUNTERS */
    ObjectID sysTablesSysCountersEntry; 	/* ObjectID of entry for SM_COUUNTERS in SM_SYSTABLES */
    ObjectID sysCountersCounterNameIndexEntry; 	/* ObjectID of entry for index on SM_SYSCOUNTERS in SM_SYSINDEXES */
    BtreeIndexInfo sysCountersCounterNameIndexInfo;/* B+ tree on counterName of SM_SYSCOUNTERS */
    IndexID sysCountersCounterNameIndexId;	/* B+ tree on counterName of SM_SYSCOUNTERS */
    PhysicalIndexID sysCountersCounterNameIndex;/* B+ tree on counterName of SM_SYSCOUNTERS */
    SegmentID_T pageSegmentIDOfSysTablesDataFileIdIndex; /* page segment ID */
    SegmentID_T pageSegmentIDOfSysIndexesIndexIdIndex;   /* page segment ID */
    SegmentID_T pageSegmentIDOfSysIndexesDataFileIdIndex;/* page segment ID */
    SegmentID_T pageSegmentIDOfSysCountersCounterNameIndex;/* page segment ID */

    logParam.logFlag = LOG_FLAG_ALL_CLEAR;


    /*
    ** I. Create data file and Btree file of SM_SYSTABLES & SM_SYSINDEXES
    */

    /* I-1. Create SM_SYSTABLES data file in the given volume. */
    MAKE_FILEID(fid, volId, serialForFile++);
    e = OM_CreateFile(handle, xactEntry, &fid, &stForSt.data, &logParam);
    if (e < eNOERROR) ERR(handle, e);

    /* I-2. Create SM_SYSINDEXES data file in the given volume. */
    MAKE_FILEID(fid, volId, serialForFile++);
    e = OM_CreateFile(handle, xactEntry, &fid, &stForSi.data, &logParam);
    if (e < eNOERROR) ERR(handle, e);


    /*
    ** II. Insert the objects in SM_SYSTABLES.
    */

    /* II-1. Register SM_SYSTABLES in the catalog table SM_SYSTABLES itself. */
    /* At this time, we cannot use OM_CreateObject() function because */
    /* the catalog entry for the SM_SYSTABLES does not yet inserted into */
    /* the SM_SYSTABLES. So we use the special function createCatalogObject(). */
    MAKE_PHYSICALFILEID(pFid, volId, stForSt.data.firstPage);
    e = OM_CreateFirstObjectInSysTables(handle, xactEntry, &pFid, (ObjectHdr*)NULL,
                                        sizeof(sm_CatOverlayForSysTables),
                                        (char*)&stForSt, &sysTablesSysTablesEntry, NULL, &logParam);
    if (e < eNOERROR) ERR(handle, e);

    /* II-2. Register SM_SYSINDEXES in the catalog table SM_SYSTABLES. */
    sysTablesInfo.fid = stForSt.data.fid;
    sysTablesInfo.tmpFileFlag = FALSE;
    sysTablesInfo.catalog.oid = sysTablesSysTablesEntry;
    e = OM_CreateObject(handle, xactEntry, &sysTablesInfo, (ObjectID*)NULL,
			(ObjectHdr*)NULL, sizeof(sm_CatOverlayForSysTables),
			(char*)&stForSi, &sysTablesSysIndexesEntry, NULL, &logParam);
    if (e < eNOERROR) ERR(handle, e);


    /*
    ** III. Create B+ tree indexes for SM_SYSTABLES & SM_SYSINDEXES
    */

    /* III-1. Create a B+ tree index on the Data FileID field of SM_SYSTABLES. */
    MAKE_INDEXID(sysTablesDataFileIdIndexId, volId, serialForIndex++);
    e = BtM_CreateIndex(handle, xactEntry, volId, &sysTablesDataFileIdIndex, &pageSegmentIDOfSysTablesDataFileIdIndex, &logParam); 
    if (e < eNOERROR) ERR(handle, e);

    /* III-2. Create a B+ tree index on the IndexID field of SM_SYSINDEXES. */
    MAKE_INDEXID(sysIndexesIndexIdIndexId, volId, serialForIndex++);
    e = BtM_CreateIndex(handle, xactEntry, volId, &sysIndexesIndexIdIndex, &pageSegmentIDOfSysIndexesIndexIdIndex, &logParam); 
    if (e < eNOERROR) ERR(handle, e);

    /* III-3. Create a B+ tree index on the Data FileID field of SM_SYSINDEXES. */
    MAKE_INDEXID(sysIndexesDataFileIdIndexId, volId, serialForIndex++);
    e = BtM_CreateIndex(handle, xactEntry, volId, &sysIndexesDataFileIdIndex, &pageSegmentIDOfSysIndexesDataFileIdIndex, &logParam); 
    if (e < eNOERROR) ERR(handle, e);


    /*
    ** IV. Insert the objects in SM_SYSINDEXES.
    */

    /* IV-0. prepare sysIndexesInfo */
    sysIndexesInfo.fid = stForSi.data.fid;
    sysIndexesInfo.tmpFileFlag = FALSE;
    sysIndexesInfo.catalog.oid = sysTablesSysIndexesEntry;

    /* IV-1. Register Btree index on DataFileId of SM_SYSTABLES in the table SM_SYSINDEXES. */
    siForSt.dataFid = stForSt.data.fid;
    siForSt.iid = sysTablesDataFileIdIndexId;
    siForSt.rootPage = sysTablesDataFileIdIndex.pageNo;
    siForSt.pageSegmentID = pageSegmentIDOfSysTablesDataFileIdIndex; 
    siForSt.indexType = SM_INDEXTYPE_BTREE;
    siForSt.kdesc.btree = SM_SYSTBL_DFILEIDIDX_KEYDESC;
    e = OM_CreateObject(handle, xactEntry, &sysIndexesInfo, (ObjectID*)NULL,
			(ObjectHdr*)NULL, sizeof(sm_CatOverlayForSysIndexes),
			(char*)&siForSt, &sysTablesDataFileIdIndexEntry, NULL, &logParam);
    if (e < eNOERROR) ERR(handle, e);

    /* IV-2. Register btree index on IndexID of SM_SYSINDEXES in the table SM_SYSINDEXES */
    siForSi.dataFid = stForSi.data.fid;
    siForSi.iid = sysIndexesIndexIdIndexId;
    siForSi.rootPage = sysIndexesIndexIdIndex.pageNo;
    siForSi.pageSegmentID = pageSegmentIDOfSysIndexesIndexIdIndex; 
    siForSi.indexType = SM_INDEXTYPE_BTREE;
    siForSi.kdesc.btree = SM_SYSIDX_INDEXID_KEYDESC;
    e = OM_CreateObject(handle, xactEntry, &sysIndexesInfo, (ObjectID*)NULL,
			(ObjectHdr*)NULL, sizeof(sm_CatOverlayForSysIndexes),
			(char*)&siForSi, &sysIndexesIndexIdIndexEntry, NULL, &logParam);
    if (e < eNOERROR) ERR(handle, e);

    /* IV-3. Register btree index on IndexID of SM_SYSINDEXES in the table SM_SYSINDEXES. */
    siForSi.dataFid = stForSi.data.fid;
    siForSi.iid = sysIndexesDataFileIdIndexId;
    siForSi.rootPage = sysIndexesDataFileIdIndex.pageNo;
    siForSi.pageSegmentID = pageSegmentIDOfSysIndexesDataFileIdIndex; 
    siForSi.indexType = SM_INDEXTYPE_BTREE;
    siForSi.kdesc.btree = SM_SYSIDX_DATAFILEID_KEYDESC;
    e = OM_CreateObject(handle, xactEntry, &sysIndexesInfo, (ObjectID*)NULL,
			(ObjectHdr*)NULL, sizeof(sm_CatOverlayForSysIndexes),
			(char*)&siForSi, &sysIndexesDataFileIdIndexEntry, NULL, &logParam);
    if (e < eNOERROR) ERR(handle, e);


    /*
    ** V. Construct B+ tree indexes of SM_SYSTABLES.
    */

    /*
     *  V-1. Insert B+ tree on data FileID of SM_SYSTABLES
     */

    /* prepare sysTablesDataFileIdIndexInfo */
    sysTablesDataFileIdIndexInfo.iid = sysTablesDataFileIdIndexId;
    sysTablesDataFileIdIndexInfo.tmpIndexFlag = FALSE;
    sysTablesDataFileIdIndexInfo.catalog.oid = sysTablesDataFileIdIndexEntry;

    /* Insert sysTablesSysTablesEntry into B+ tree on data FileID of SM_SYSTABLES. */
    kval.len = sizeof(Two) + sizeof(Four);
    memcpy(&(kval.val[0]), (char*)&(stForSt.data.fid.volNo), sizeof(Two));
    memcpy(&(kval.val[sizeof(Two)]), (char*)&(stForSt.data.fid.serial), sizeof(Four));
    e = BtM_InsertObject(handle, xactEntry, &sysTablesDataFileIdIndexInfo,
                         &sysTablesInfo.fid, 
			 &SM_SYSTBL_DFILEIDIDX_KEYDESC, &kval, &sysTablesSysTablesEntry, NULL, &logParam);
    if (e < eNOERROR) ERR(handle, e);

    /* Insert sysTablesSysIndexesEntry into B+ tree on data FileID of SM_SYSTABLES. */
    kval.len = sizeof(Two) + sizeof(Four);
    memcpy(&(kval.val[0]), (char*)&(stForSi.data.fid.volNo), sizeof(Two));
    memcpy(&(kval.val[sizeof(Two)]), (char*)&(stForSi.data.fid.serial), sizeof(Four));
    e = BtM_InsertObject(handle, xactEntry, &sysTablesDataFileIdIndexInfo,
                         &sysTablesInfo.fid, 
			 &SM_SYSTBL_DFILEIDIDX_KEYDESC, &kval, &sysTablesSysIndexesEntry, NULL, &logParam);
    if (e < eNOERROR) ERR(handle, e);


    /*
     *  V-2. Insert B+ tree on IndexID of SM_SYSINDEXES
     */

    /* prepare sysTablesDataFileIdIndexInfo */
    sysIndexesIndexIdIndexInfo.iid = sysIndexesIndexIdIndexId;
    sysIndexesIndexIdIndexInfo.tmpIndexFlag = FALSE;
    sysIndexesIndexIdIndexInfo.catalog.oid = sysIndexesIndexIdIndexEntry;

    /* Insert sysTablesDataFileIdIndexEntry into B+ tree on IndexID of SM_SYSINDEXES. */
    kval.len = sizeof(Two) + sizeof(Four);
    memcpy(&(kval.val[0]), (char*)&(sysTablesDataFileIdIndexId.volNo), sizeof(Two));
    memcpy(&(kval.val[sizeof(Two)]), (char*)&(sysTablesDataFileIdIndexId.serial), sizeof(Four));
    e = BtM_InsertObject(handle, xactEntry, &sysIndexesIndexIdIndexInfo,
                         &sysIndexesInfo.fid, 
			 &SM_SYSIDX_INDEXID_KEYDESC, &kval, &sysTablesDataFileIdIndexEntry, NULL, &logParam);
    if (e < eNOERROR) ERR(handle, e);

    /* Insert sysIndexesIndexIdIndexEntry into B+ tree on IndexID of SM_SYSINDEXES. */
    kval.len = sizeof(Two) + sizeof(Four);
    memcpy(&(kval.val[0]), (char*)&(sysIndexesIndexIdIndexId.volNo), sizeof(Two));
    memcpy(&(kval.val[sizeof(Two)]), (char*)&(sysIndexesIndexIdIndexId.serial), sizeof(Four));
    e = BtM_InsertObject(handle, xactEntry, &sysIndexesIndexIdIndexInfo,
                         &sysIndexesInfo.fid, 
			 &SM_SYSIDX_INDEXID_KEYDESC, &kval, &sysIndexesIndexIdIndexEntry, NULL, &logParam);
    if (e < eNOERROR) ERR(handle, e);

    /* Insert sysIndexesDataFileIdIndexEntry into B+ tree on IndexID of SM_SYSINDEXES. */
    kval.len = sizeof(Two) + sizeof(Four);
    memcpy(&(kval.val[0]), (char*)&(sysIndexesDataFileIdIndexId.volNo), sizeof(Two));
    memcpy(&(kval.val[sizeof(Two)]), (char*)&(sysIndexesDataFileIdIndexId.serial), sizeof(Four));
    e = BtM_InsertObject(handle, xactEntry, &sysIndexesIndexIdIndexInfo,
                         &sysIndexesInfo.fid, 
			 &SM_SYSIDX_INDEXID_KEYDESC, &kval, &sysIndexesDataFileIdIndexEntry, NULL, &logParam);
    if (e < eNOERROR) ERR(handle, e);


    /*
     *  V-3. Insert B+ tree on data FileID of SM_SYSINDEXES
     */

    /* prepare sysTablesDataFileIdIndexInfo */
    sysIndexesDataFileIdIndexInfo.iid = sysIndexesDataFileIdIndexId;
    sysIndexesDataFileIdIndexInfo.tmpIndexFlag = FALSE;
    sysIndexesDataFileIdIndexInfo.catalog.oid = sysIndexesDataFileIdIndexEntry;

    /* Insert sysTablesDataFileIdIndexEntry into B+ tree on data FileID of SM_SYSINDEXES. */
    kval.len = sizeof(Two) + sizeof(Four);
    memcpy(&(kval.val[0]), (char*)&(siForSt.dataFid.volNo), sizeof(Two));
    memcpy(&(kval.val[sizeof(Two)]), (char*)&(siForSt.dataFid.serial), sizeof(Four));
    e = BtM_InsertObject(handle, xactEntry, &sysIndexesDataFileIdIndexInfo,
                         &sysIndexesInfo.fid, 
			 &SM_SYSIDX_DATAFILEID_KEYDESC, &kval, &sysTablesDataFileIdIndexEntry, NULL, &logParam);
    if (e < eNOERROR) ERR(handle, e);

    /* Insert sysIndexesIndexIdIndexEntry into B+ tree on data FileID of SM_SYSINDEXES. */
    kval.len = sizeof(Two) + sizeof(Four);
    memcpy(&(kval.val[0]), (char*)&(siForSi.dataFid.volNo), sizeof(Two));
    memcpy(&(kval.val[sizeof(Two)]), (char*)&(siForSi.dataFid.serial), sizeof(Four));
    e = BtM_InsertObject(handle, xactEntry, &sysIndexesDataFileIdIndexInfo,
                         &sysIndexesInfo.fid, 
			 &SM_SYSIDX_DATAFILEID_KEYDESC, &kval, &sysIndexesIndexIdIndexEntry, NULL, &logParam);
    if (e < eNOERROR) ERR(handle, e);

    /* Insert the above object into B+ tree on data FileID of SM_SYSINDEXES. */
    kval.len = sizeof(Two) + sizeof(Four);
    memcpy(&(kval.val[0]), (char*)&(siForSi.dataFid.volNo), sizeof(Two));
    memcpy(&(kval.val[sizeof(Two)]), (char*)&(siForSi.dataFid.serial), sizeof(Four));
    e = BtM_InsertObject(handle, xactEntry, &sysIndexesDataFileIdIndexInfo,
                         &sysIndexesInfo.fid, 
			 &SM_SYSIDX_DATAFILEID_KEYDESC, &kval, &sysIndexesDataFileIdIndexEntry, NULL, &logParam);
    if (e < eNOERROR) ERR(handle, e);

    /*
    ** Set the fixed information for the catalog table SM_SYSTABLES.
    */
    e = RDsM_InsertMetaDictEntry(handle, xactEntry, volId, "smSysTablesDataFileId",
                                 (char *)&stForSt.data.fid, sizeof(FileID), &logParam);
    if (e < eNOERROR) ERR(handle, e);

    e = RDsM_InsertMetaDictEntry(handle, xactEntry, volId, "smSysTablesDataFileIdIndexId",
                                 (char *)&sysTablesDataFileIdIndexId, sizeof(IndexID), &logParam);
    if (e < eNOERROR) ERR(handle, e);

    e = RDsM_InsertMetaDictEntry(handle, xactEntry, volId, "smSysTablesDataFileIdIndexEntry",
                                 (char *)&sysTablesDataFileIdIndexEntry, sizeof(ObjectID), &logParam);
    if (e < eNOERROR) ERR(handle, e);


    /*
    ** Set the fixed information for the catalog table SM_SYSINDEXES.
    */
    e = RDsM_InsertMetaDictEntry(handle, xactEntry, volId, "smSysIndexesDataFileId",
                                 (char *)&stForSi.data.fid, sizeof(FileID), &logParam);
    if (e < eNOERROR) ERR(handle, e);

    e = RDsM_InsertMetaDictEntry(handle, xactEntry, volId, "smSysIndexesIndexIdIndexId",
                                 (char *)&sysIndexesIndexIdIndexId, sizeof(IndexID), &logParam);
    if (e < eNOERROR) ERR(handle, e);

    e = RDsM_InsertMetaDictEntry(handle, xactEntry, volId, "smSysIndexesIndexIdIndexEntry",
                                 (char *)&sysIndexesIndexIdIndexEntry, sizeof(ObjectID), &logParam);
    if (e < eNOERROR) ERR(handle, e);


    /* smSysIndexesBtreeFileIdIndex => smSysIndexesDataFileIdIndex */
    e = RDsM_InsertMetaDictEntry(handle, xactEntry, volId, "smSysIndexesDataFileIdIndexId",
                                 (char *)&sysIndexesDataFileIdIndexId, sizeof(IndexID), &logParam);
    if (e < eNOERROR) ERR(handle, e);

    e = RDsM_InsertMetaDictEntry(handle, xactEntry, volId, "smSysIndexesDataFileIdIndexEntry",
                                 (char *)&sysIndexesDataFileIdIndexEntry, sizeof(ObjectID), &logParam);
    if (e < eNOERROR) ERR(handle, e);


    /*
     * Create catalog table SM_SYSCOUNTERS.
     */
    MAKE_FILEID(fid, volId, serialForFile++);
    e = OM_CreateFile(handle, xactEntry, &fid, &stForSc.data, &logParam);
    if (e < eNOERROR) ERR(handle, e);


    /*
     *  Register SM_SYSCOUNTERS in the catalog table SM_SYSTABLES.
     */
    e = OM_CreateObject(handle, xactEntry, &sysTablesInfo, (ObjectID*)NULL,
                        (ObjectHdr*)NULL, sizeof(sm_CatOverlayForSysTables),
                        (char*)&stForSc, &sysTablesSysCountersEntry, NULL, &logParam); /* lockup */
    if (e < eNOERROR) ERR(handle, e);


    /*
     * Insert the index entry of SM_SYSCOUNTERS into B+ tree indexes on SM_SYSTABLES.
     */

    /*@ construct kval */
    kval.len = sizeof(Two) + sizeof(Four);
    memcpy(&(kval.val[0]), (char*)&(stForSc.data.fid.volNo), sizeof(Two));
    memcpy(&(kval.val[sizeof(Two)]), (char*)&(stForSc.data.fid.serial), sizeof(Four));

    /* insert sysTablesSysCountersEntry into B+ tree on data FileID of SM_SYSTABLES. */
    e = BtM_InsertObject(handle, xactEntry, &sysTablesDataFileIdIndexInfo,
                         &sysTablesInfo.fid, 
                         &SM_SYSTBL_DFILEIDIDX_KEYDESC, &kval, &sysTablesSysCountersEntry, NULL, &logParam);


    /*
     *  Create a B+ tree index on the counterName field of SM_SYSCOUNTERS.
     */

    /* create index */
    MAKE_INDEXID(sysCountersCounterNameIndexId, volId, serialForIndex++); 
    e = BtM_CreateIndex(handle, xactEntry, volId, &sysCountersCounterNameIndex, &pageSegmentIDOfSysCountersCounterNameIndex, &logParam); 
    if (e < eNOERROR) ERR(handle, e);


    /*
     * Register Btree index on counterName filed of SM_SYSCOUNTERS into the table SM_SYSINDEXES.
     */

    /* set siForSc */
    siForSc.dataFid = stForSc.data.fid;
    siForSc.iid = sysCountersCounterNameIndexId;
    siForSc.rootPage = sysCountersCounterNameIndex.pageNo;
    siForSc.indexType = SM_INDEXTYPE_BTREE;
    siForSc.kdesc.btree = SM_SYSCNTR_CNTRNAME_KEYDESC;

    /* insert into SM_SYSINDEXES */
    e = OM_CreateObject(handle, xactEntry, &sysIndexesInfo, (ObjectID*)NULL,
                        (ObjectHdr*)NULL, sizeof(sm_CatOverlayForSysIndexes),
                        (char*)&siForSc, &sysCountersCounterNameIndexEntry, NULL, &logParam);
    if (e < eNOERROR) ERR(handle, e);


    /*
     * Insert the index entry of index on SM_SYSCOUNTERS into B+ tree indexes on SM_SYINDEXES.
     */

    /*@ construct kval */
    /* Insert the above object into B+ tree on IndexID of SM_SYSINDEXES. */
    kval.len = sizeof(Two) + sizeof(Four);
    memcpy(&(kval.val[0]), (char*)&(siForSc.iid.volNo), sizeof(Two));
    memcpy(&(kval.val[sizeof(Two)]), (char*)&(siForSc.iid.serial), sizeof(Four));

    /* The dealloc list will not be used in this call. */
    e = BtM_InsertObject(handle, xactEntry, &sysIndexesIndexIdIndexInfo,
                         &sysIndexesInfo.fid, 
                         &SM_SYSIDX_INDEXID_KEYDESC, &kval, &sysCountersCounterNameIndexEntry, NULL, &logParam);
    if (e < eNOERROR) ERR(handle, e);

    /*@ construct kval */
    /* Insert the above object into B+ tree on Data FileID of SM_SYSINDEXES. */
    kval.len = sizeof(Two) + sizeof(Four);
    memcpy(&(kval.val[0]), (char*)&(siForSc.dataFid.volNo), sizeof(Two));
    memcpy(&(kval.val[sizeof(Two)]), (char*)&(siForSc.dataFid.serial), sizeof(Four));

    /* The dealloc list will not be used in this call. */
    e = BtM_InsertObject(handle, xactEntry, &sysIndexesDataFileIdIndexInfo,
                         &sysIndexesInfo.fid, 
                         &SM_SYSIDX_DATAFILEID_KEYDESC, &kval, &sysCountersCounterNameIndexEntry, NULL, &logParam);
    if (e < eNOERROR) ERR(handle, e);


    /*
    ** Set the fixed information for the catalog table SM_COUNTERS.
    */

    /* register 'sysTablesSysCountersEntry' */
    e = RDsM_InsertMetaDictEntry(handle, xactEntry, volId, "smSysCountersDataFileId",
                                 (char *)&stForSc.data.fid, sizeof(FileID), &logParam);
    if (e < eNOERROR) ERR(handle, e);

    /* register 'sysCountersCounterNameIndex' */
    e = RDsM_InsertMetaDictEntry(handle, xactEntry, volId, "smSysCountersCounterNameIndexId",
                                 (char *)&sysCountersCounterNameIndexId, sizeof(IndexID), &logParam); 
    if (e < eNOERROR) ERR(handle, e);


    /*
     *  Create entry for sysSerialForFileCounter into SM_SYSCOUNTERS
     */

    /* set 'sysSerialForFileCounter' */
    strcpy(sysSerialForFileCounter.counterName, "smSysSerialForFileCounter");
    sysSerialForFileCounter.counterValue = serialForFile;

    /* register smSysSerialCounter in the catalog table SM_COUNTERS. */
    sysCountersInfo.fid = stForSc.data.fid;
    sysCountersInfo.tmpFileFlag = FALSE;
    sysCountersInfo.catalog.oid = sysTablesSysCountersEntry;
    e = OM_CreateObject(handle, xactEntry, &sysCountersInfo, (ObjectID*)NULL,
                        (ObjectHdr*)NULL, sizeof(sm_SysCountersOverlay_T),
                        (char*)&sysSerialForFileCounter, &oid, NULL, &logParam);
    if (e < eNOERROR) ERR(handle, e);

    /*@ construct kval */
    kval.len = strlen(sysSerialForFileCounter.counterName);
    memcpy(&kval.val[0], &kval.len, sizeof(Two));
    memcpy(&(kval.val[sizeof(Two)]), sysSerialForFileCounter.counterName, kval.len);
    kval.len += sizeof(Two);

    /* Insert the above object into B+ tree on counterName on SM_SYSCOUNTERS. */
    sysCountersCounterNameIndexInfo.iid = sysCountersCounterNameIndexId;
    sysCountersCounterNameIndexInfo.tmpIndexFlag = FALSE;
    sysCountersCounterNameIndexInfo.catalog.oid = sysCountersCounterNameIndexEntry;
    e = BtM_InsertObject(handle, xactEntry, &sysCountersCounterNameIndexInfo,
                         &sysCountersInfo.fid, 
                         &SM_SYSCNTR_CNTRNAME_KEYDESC, &kval, &oid, NULL, &logParam);
    if (e < eNOERROR) ERR(handle, e);


    /*
     *  Create entry for sysSerialForIndexCounter into SM_SYSCOUNTERS
     */

    /* set 'sysSerialForIndexCounter' */
    strcpy(sysSerialForIndexCounter.counterName, "smSysSerialForIndexCounter");
    sysSerialForIndexCounter.counterValue = serialForIndex;

    /* register smSysSerialCounter in the catalog table SM_COUNTERS. */
    e = OM_CreateObject(handle, xactEntry, &sysCountersInfo, (ObjectID*)NULL,
                        (ObjectHdr*)NULL, sizeof(sm_SysCountersOverlay_T),
                        (char*)&sysSerialForIndexCounter, &oid, NULL, &logParam);
    if (e < eNOERROR) ERR(handle, e);

    /*@ construct kval */
    kval.len = strlen(sysSerialForIndexCounter.counterName);
    memcpy(&kval.val[0], &kval.len, sizeof(Two));
    memcpy(&(kval.val[sizeof(Two)]), sysSerialForIndexCounter.counterName, kval.len);
    kval.len += sizeof(Two);

    /* Insert the above object into B+ tree on counterName on SM_SYSCOUNTERS. */
    e = BtM_InsertObject(handle, xactEntry, &sysCountersCounterNameIndexInfo,
                         &sysCountersInfo.fid, 
                         &SM_SYSCNTR_CNTRNAME_KEYDESC, &kval, &oid, NULL, &logParam);
    if (e < eNOERROR) ERR(handle, e);


    return(eNOERROR);

} /* sm_FormatDataVolume() */
