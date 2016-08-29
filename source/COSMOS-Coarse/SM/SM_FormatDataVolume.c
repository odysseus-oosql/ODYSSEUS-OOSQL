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
 * Module: SM_FormatDataVolume.c
 *
 * Description:
 *  Format a file or a raw device so that it can be used as the volume of our
 *  storage system. This format is for Scan Manager level.
 *
 * Exports:
 *  Four SM_FormatDataVolume()
 */


#include <stdio.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>
#include "common.h"
#include "trace.h"
#include "RDsM_Internal.h"	
#include "BfM.h"
#include "BtM.h"
#include "SM_Internal.h"	
#include "OM_Internal.h"	/* for createFirstObjectInSysTables() */
#include "perThreadDS.h"
#include "perProcessDS.h"


/*@ Internal Function Prototype */
Four createFirstObjectInSysTables(Four, PhysicalFileID*, ObjectHdr*, Four, char*, ObjectID*); 



/*@================================
 * SM_FormatDataVolume()
 *================================*/
/*
 * Function: Four SM_FormatDataVolume(Four, char**, char*, Four, Two, Four)
 *
 * Description:
 *  Format a file or a raw device so that it can be used as the volume of our
 *  storage system. This format is for Scan Manager level.
 *
 * Returns:
 *  exit status
 */
Four _SM_FormatDataVolume(
    Four handle,
    Four     numDevices,                         /* # of devices in formated volume */
    char     **devNames,                         /* IN array of device names */
    char     *title,                             /* IN volume title */
    Four     volId,                              /* IN volume identifier */
    Two      sizeOfExt,                          /* IN size of an extent */
    Four     *numPagesInDevice)                  /* IN array of pages' number in each given device */
{
    Four     e;                                  /* error code */
    Four     i;                                  /* index variable */
    Boolean  err;                                /* TRUE if there is any error. */
    KeyValue kval;                               /* key value for B+ tree  */
    ObjectID oid;                                /* ObjectID of the objects in SM_SYSINDEXES */
    ObjectID sysTablesSysTablesEntry;            /* ObjectID of entry for SM_SYSTABLES in SM_SYSTABLES */
    ObjectID sysTablesSysIndexesEntry;           /* ObjectID of entry for SM_SYSINDEXES in SM_SYSTABLES */
    sm_CatOverlayForSysTables  stForSt;          /* entry for SM_SYSTABLES in SM_SYSTABLES*/
    sm_CatOverlayForSysTables  stForSi;          /* entry for SM_SYSINDEXES in SM_SYSTABLES */
    sm_CatOverlayForSysIndexes siForSt;          /* entry for SM_SYSTABLES in SM_SYSINDEXES */
    sm_CatOverlayForSysIndexes siForSi;          /* entry for SM_SYSINDEXS in SM_SYSINDEXES */
    KeyDesc sysTablesDataFileIdIndexKdesc;       /* key descriptor for B+ tree on DataFileId of SM_SYSTABLES */
    KeyDesc sysTablesBtreeFileIdIndexKdesc;      /* key descriptor for B+ tree on BtreeFileId of SM_SYSTABLES */
    KeyDesc sysIndexesIndexIdIndexKdesc;         /* key descriptor for B+ tree on IndexID of SM_SYSINDEXES */
    KeyDesc sysIndexesBtreeFileIdIndexKdesc;     /* key descriptor for B+ tree on BtreeFileId of SM_SYSINDEXES */
    double  volumeSize;                          /* volume size in # of pages */
    double  deviceSize;                          /* device size in bytes */
    FileID  fid;                                 /* file ID for temporary use */
    Serial  serialForFile = 0;                   /* serial number for FileID */
    Serial  serialForIndex = 0;                  /* serial number for IndexID */
    PhysicalFileID pFid;                         /* physical file ID of SM_SYSTABLES */
    sm_CatOverlayForSysTables  stForSc;          /* entry for SM_SYSCOUNTERS in SM_SYSTABLES*/
    sm_CatOverlayForSysIndexes siForSc;          /* entry for SM_SYSCOUNTERS in SM_SYSINDEXES */
    PhysicalIndexID sysTablesDataFileIdIndex;    /* B+ tree on DataFileId of SM_SYSTABLES */
    PhysicalIndexID sysTablesBtreeFileIdIndex;   /* B+ tree on BtreeFileId of SM_SYSTABLES */
    PhysicalIndexID sysIndexesIndexIdIndex;      /* B+ tree on IndexId of SM_SYSINDEXES */
    PhysicalIndexID sysIndexesBtreeFileIdIndex;	 /* B+ tree on BtreeFileId of SM_SYSINDEXES */
    ObjectID        sysTablesSysCountersEntry;   /* ObjectID of entry for SM_COUNTERS in SM_SYSTABLES */
    PhysicalIndexID sysCountersCounterNameIndex; /* B+ tree on counterName of SM_SYSCOUNTERS */
    KeyDesc sysCountersCounterNameIndexKdesc;    /* key descriptor for B+ tree on BtreeFileId of SM_SYSINDEXES */
    sm_SysCountersOverlay_T sysSerialForFileCounter; /* system counter for serial in File ID */
    sm_SysCountersOverlay_T sysSerialForIndexCounter; /* system counter for serial in File ID */

    
    /* parameter check */
    if (volId < 0 || volId > MAX_VOLUME_NUMBER) ERR(handle, eBADPARAMETER);
    if (sizeOfExt < 1) ERR(handle, eBADPARAMETER);
    if (numDevices <= 0) ERR(handle, eBADPARAMETER);
    
    for (i = 0, volumeSize = 0; i < numDevices; i++) {
        deviceSize = numPagesInDevice[i]/sizeOfExt*sizeOfExt * PAGESIZE;
        if (deviceSize > MAX_RAW_DEVICE_OFFSET) ERR(handle, eBADPARAMETER); 
        
        volumeSize += numPagesInDevice[i]/sizeOfExt*sizeOfExt; 
    }
    if (volumeSize > MAX_PAGES_IN_VOLUME) ERR(handle, eBADPARAMETER); 

    /* Format the device in the RDsM level. */
    e = RDsM_Format(handle, numDevices, devNames, title, volId, sizeOfExt, numPagesInDevice);
    if (e < 0) ERR(handle, e);

    /* Mount the formated volume. */
    e = RDsM_Mount(handle, numDevices, devNames, &volId);
    if (e < 0) ERR(handle, e);
    

    /*
    ** Format the volume in the SM level.
    */

#ifdef DBLOCK
    /* 
     *  Acquire lock for data volume 
     */
    e = RDsM_GetVolumeLock(handle, volId, L_X, FALSE);
    if (e < eNOERROR) ERR(handle, e);
#endif

    /*
    ** Construct key descriptors of the indexes on the catalog tables.
    */
    sysTablesDataFileIdIndexKdesc.flag = 0;
    sysTablesDataFileIdIndexKdesc.nparts = 2;
    sysTablesDataFileIdIndexKdesc.kpart[0].type = SM_VOLNO;
    sysTablesDataFileIdIndexKdesc.kpart[0].offset = 0;
    sysTablesDataFileIdIndexKdesc.kpart[0].length = SM_VOLNO_SIZE;
    sysTablesDataFileIdIndexKdesc.kpart[1].type = SM_SERIAL;
    sysTablesDataFileIdIndexKdesc.kpart[1].offset = SM_VOLNO_SIZE;
    sysTablesDataFileIdIndexKdesc.kpart[1].length = SM_SERIAL_SIZE;
    
    sysTablesBtreeFileIdIndexKdesc = sysTablesDataFileIdIndexKdesc;
    sysIndexesBtreeFileIdIndexKdesc = sysTablesDataFileIdIndexKdesc;
    sysIndexesIndexIdIndexKdesc = sysTablesDataFileIdIndexKdesc;
    
    /* construct kdesc for index on SM_SYSCOUNTERS */
    sysCountersCounterNameIndexKdesc.flag = 0;
    sysCountersCounterNameIndexKdesc.nparts = 1;
    sysCountersCounterNameIndexKdesc.kpart[0].type = SM_VARSTRING;
    sysCountersCounterNameIndexKdesc.kpart[0].offset = 0;
    sysCountersCounterNameIndexKdesc.kpart[0].length = SM_COUNTER_NAME_MAX_LEN;


    /*
    ** Create data file and Btree file of SM_SYSTABLES.
    */

    /* Create SM_SYSTABLES data file in the given volume. */
    MAKE_FILEID(fid, volId, serialForFile++);
    e = OM_CreateFile(handle, &fid, 100, &stForSt.data, FALSE);   
    if (e < 0) ERR(handle, e);

    /* Create a B+-tree file for SM_SYSTABLES in the given volume. */
    MAKE_FILEID(fid, volId, serialForFile++);
    e = BtM_CreateFile(handle, &fid, 100, &stForSt.btree);        
    if (e < 0) ERR(handle, e);


    /*
    ** Create data file and Btree file of SM_SYSINDEXES.
    */

    /* Create SM_SYSINDEXES data file in the given volume. */
    MAKE_FILEID(fid, volId, serialForFile++);
    e = OM_CreateFile(handle, &fid, 100, &stForSi.data, FALSE);   
    if (e < 0) ERR(handle, e);

    /* Create a B+-tree file for SM_SYSINDEXES in the given volume. */
    MAKE_FILEID(fid, volId, serialForFile++);
    e = BtM_CreateFile(handle, &fid, 100, &stForSi.btree);      
    if (e < 0) ERR(handle, e);


    /*
    ** Insert the catalog entries of SM_SYSTABLES & SM_SYSINDEXES into SM_SYSTABLES.
    */

    /* Register SM_SYSTABLES in the catalog table SM_SYSTABLES itself. */
    /* At this time, we cannot use OM_CreateObject() function because */
    /* the catalog entry for the SM_SYSTABLES does not yet inserted into */
    /* the SM_SYSTABLES. So we use the special function createCatalogObject(). */
    MAKE_PHYSICALFILEID(pFid, stForSt.data.fid.volNo, stForSt.data.firstPage);
    e = createFirstObjectInSysTables(handle, &pFid, (ObjectHdr*)NULL,  
				     sizeof(sm_CatOverlayForSysTables),
				     (char*)&stForSt, &sysTablesSysTablesEntry);
    if (e < 0) ERR(handle, e);
    
    /* Register SM_SYSINDEXES in the catalog table SM_SYSTABLES. */
    e = OM_CreateObject(handle, (ObjectID*)&sysTablesSysTablesEntry, (ObjectID*)NULL,
			(ObjectHdr*)NULL, sizeof(sm_CatOverlayForSysTables),
			(char*)&stForSi, &sysTablesSysIndexesEntry);
    if (e < 0) ERR(handle, e);


    /*
    ** Create B+ tree indexes on SM_SYSTABLES.
    */

    /* Create a B+ tree index on the Data FileID field of SM_SYSTABLES. */
    e = BtM_CreateIndex(handle, &sysTablesSysTablesEntry, &sysTablesDataFileIdIndex);
    if (e < 0) ERR(handle, e);

    /* Create a B+ tree index on the Btree FileID field of SM_SYSTABLES. */
    e = BtM_CreateIndex(handle, &sysTablesSysTablesEntry, &sysTablesBtreeFileIdIndex);
    if (e < 0) ERR(handle, e);


    /*
    ** Insert the index entry of SM_SYSTABLES & SM_SYSINDEXES into B+ tree indexes on SM_SYSTABLES.
    */

    /*@ construct kval */
    /* Insert sysTablesSysTablesEntry into B+ tree on data FileID of SM_SYSTABLES. */
    kval.len = sizeof(VolNo) + sizeof(Serial);
    memcpy(&(kval.val[0]), (char*)&(stForSt.data.fid.volNo), sizeof(VolNo));
    memcpy(&(kval.val[sizeof(VolNo)]), (char*)&(stForSt.data.fid.serial), sizeof(Serial)); 

    /* The dealloc list will not be used in this call. */
    e = BtM_InsertObject(handle, &sysTablesSysTablesEntry, &sysTablesDataFileIdIndex,
			 &sysTablesDataFileIdIndexKdesc, &kval,
			 &sysTablesSysTablesEntry, NULL, NULL);
    if (e < 0) ERR(handle, e);


    /*@ construct kval */
    /* Insert sysTablesSysTablesEntry into B+ tree on Btree FileID of SM_SYSTABLES. */
    kval.len = sizeof(VolNo) + sizeof(Serial);
    memcpy(&(kval.val[0]), (char*)&(stForSt.btree.fid.volNo), sizeof(VolNo));
    memcpy(&(kval.val[sizeof(VolNo)]), (char*)&(stForSt.btree.fid.serial), sizeof(Serial)); 

    /* The dealloc list will not be used in this call. */
    e = BtM_InsertObject(handle, &sysTablesSysTablesEntry, &sysTablesBtreeFileIdIndex,
			 &sysTablesBtreeFileIdIndexKdesc, &kval,
			 &sysTablesSysTablesEntry, NULL, NULL);
    if (e < 0) ERR(handle, e);


    /*@ construct kval */
    /* Insert sysTablesSysIndexesEntry into B+ tree on data FileID of SM_SYSTABLES. */
    kval.len = sizeof(VolNo) + sizeof(Serial);
    memcpy(&(kval.val[0]), (char*)&(stForSi.data.fid.volNo), sizeof(VolNo));
    memcpy(&(kval.val[sizeof(VolNo)]), (char*)&(stForSi.data.fid.serial), sizeof(Serial)); 

    /* The dealloc list will not be used in this call. */
    e = BtM_InsertObject(handle, &sysTablesSysTablesEntry, &sysTablesDataFileIdIndex,
			 &sysTablesDataFileIdIndexKdesc, &kval,
			 &sysTablesSysIndexesEntry, NULL, NULL);
    if (e < 0) ERR(handle, e);


    /*@ construct kval */
    /* Insert sysTablesSysIndexesEntry into B+ tree on Btree FileID of SM_SYSTABLES. */
    kval.len = sizeof(VolNo) + sizeof(Serial);
    memcpy(&(kval.val[0]), (char*)&(stForSi.btree.fid.volNo), sizeof(VolNo));
    memcpy(&(kval.val[sizeof(VolNo)]), (char*)&(stForSi.btree.fid.serial), sizeof(Serial)); 

    /* The dealloc list will not be used in this call. */
    e = BtM_InsertObject(handle, &sysTablesSysTablesEntry, &sysTablesBtreeFileIdIndex,
			 &sysTablesBtreeFileIdIndexKdesc, &kval,
			 &sysTablesSysIndexesEntry, NULL, NULL);
    if (e < 0) ERR(handle, e);


    /*
    ** Create B+ tree indexes on SM_SYSINDEXES.
    */

    /* Create a B+ tree index on the IndexID field of SM_SYSINDEXES. */
    e = BtM_CreateIndex(handle, &sysTablesSysIndexesEntry, &sysIndexesIndexIdIndex);
    if (e < 0) ERR(handle, e);

    /* Create a B+ tree index on the Btree FileID field of SM_SYSINDEXES. */
    e = BtM_CreateIndex(handle, &sysTablesSysIndexesEntry, &sysIndexesBtreeFileIdIndex);
    if (e < 0) ERR(handle, e);

    
    /*
    ** Insert the objects in SM_SYSINDEXES.
    ** And construct B+ tree indexes of SM_SYSINDEXES.
    */

    /* Register Btree index on DataFileId of SM_SYSTABLES in the table SM_SYSINDEXES. */
    siForSt.indexFid = stForSt.btree.fid; 
    MAKE_INDEXID(siForSt.iid, volId, serialForIndex++);
    siForSt.rootPage = sysTablesDataFileIdIndex.pageNo;
    e = OM_CreateObject(handle, &sysTablesSysIndexesEntry, (ObjectID*)NULL,
			(ObjectHdr*)NULL, sizeof(sm_CatOverlayForSysIndexes),
			(char*)&siForSt, &oid);
    if (e < 0) ERR(handle, e);

    /*@ construct kval */
    /* Insert the above object into B+ tree on IndexID of SM_SYSINDEXES. */
    kval.len = sizeof(VolNo) + sizeof(Serial);
    memcpy(&(kval.val[0]), (char*)&(siForSt.iid.volNo), sizeof(VolNo));
    memcpy(&(kval.val[sizeof(VolNo)]), (char*)&(siForSt.iid.serial), sizeof(Serial)); 

    /* The dealloc list will not be used in this call. */
    e = BtM_InsertObject(handle, &sysTablesSysIndexesEntry, &sysIndexesIndexIdIndex,
			 &sysIndexesIndexIdIndexKdesc, &kval,
			 &oid, NULL, NULL);
    if (e < 0) ERR(handle, e);

    /*@ construct kval */
    /* Insert the above object into B+ tree on Btree FileID of SM_SYSINDEXES. */
    kval.len = sizeof(VolNo) + sizeof(Serial);
    memcpy(&(kval.val[0]), (char*)&(siForSt.indexFid.volNo), sizeof(VolNo)); 
    memcpy(&(kval.val[sizeof(VolNo)]), (char*)&(siForSt.indexFid.serial), sizeof(Serial)); 

    /* The dealloc list will not be used in this call. */
    e = BtM_InsertObject(handle, &sysTablesSysIndexesEntry, &sysIndexesBtreeFileIdIndex,
			 &sysIndexesBtreeFileIdIndexKdesc, &kval,
			 &oid, NULL, NULL);
    if (e < 0) ERR(handle, e);

    
    /* Register Btree index on BtreeFileId of SM_SYSTABLES in the table SM_SYSINDEXES. */
    siForSt.indexFid = stForSt.btree.fid; 
    MAKE_INDEXID(siForSt.iid, volId, serialForIndex++);
    siForSt.rootPage = sysTablesBtreeFileIdIndex.pageNo;
    e = OM_CreateObject(handle, &sysTablesSysIndexesEntry, (ObjectID*)NULL,
			(ObjectHdr*)NULL, sizeof(sm_CatOverlayForSysIndexes),
			(char*)&siForSt, &oid);
    if (e < 0) ERR(handle, e);

    /*@ construct kval */
    /* Insert the above object into B+ tree on IndexID of SM_SYSINDEXES. */
    kval.len = sizeof(VolNo) + sizeof(Serial);
    memcpy(&(kval.val[0]), (char*)&(siForSt.iid.volNo), sizeof(VolNo));
    memcpy(&(kval.val[sizeof(VolNo)]), (char*)&(siForSt.iid.serial), sizeof(Serial)); 

    /* The dealloc list will not be used in this call. */
    e = BtM_InsertObject(handle, &sysTablesSysIndexesEntry, &sysIndexesIndexIdIndex,
			 &sysIndexesIndexIdIndexKdesc, &kval,
			 &oid, NULL, NULL);
    if (e < 0) ERR(handle, e);

    /*@ construct kval */
    /* Insert the above object into B+ tree on Btree FileID of SM_SYSINDEXES. */
    kval.len = sizeof(VolNo) + sizeof(Serial);
    memcpy(&(kval.val[0]), (char*)&(siForSt.indexFid.volNo), sizeof(VolNo)); 
    memcpy(&(kval.val[sizeof(VolNo)]), (char*)&(siForSt.indexFid.serial), sizeof(Serial)); 

    /* The dealloc list will not be used in this call. */
    e = BtM_InsertObject(handle, &sysTablesSysIndexesEntry, &sysIndexesBtreeFileIdIndex,
			 &sysIndexesBtreeFileIdIndexKdesc, &kval,
			 &oid, NULL, NULL);
    if (e < 0) ERR(handle, e);

    
    /* Register btree index on IndexID of SM_SYSINDEXES in the table SM_SYSINDEXES */
    siForSi.indexFid = stForSi.btree.fid; 
    MAKE_INDEXID(siForSi.iid, volId, serialForIndex++);
    siForSi.rootPage = sysIndexesIndexIdIndex.pageNo;
    e = OM_CreateObject(handle, &sysTablesSysIndexesEntry, (ObjectID*)NULL,
			(ObjectHdr*)NULL, sizeof(sm_CatOverlayForSysIndexes),
			(char*)&siForSi, &oid);
    if (e < 0) ERR(handle, e);

    /*@ construct kval */
    /* Insert the above object into B+ tree on IndexID of SM_SYSINDEXES. */
    kval.len = sizeof(VolNo) + sizeof(Serial);
    memcpy(&(kval.val[0]), (char*)&(siForSi.iid.volNo), sizeof(VolNo));
    memcpy(&(kval.val[sizeof(VolNo)]), (char*)&(siForSi.iid.serial), sizeof(Serial)); 

    /* The dealloc list will not be used in this call. */
    e = BtM_InsertObject(handle, &sysTablesSysIndexesEntry, &sysIndexesIndexIdIndex,
			 &sysIndexesIndexIdIndexKdesc, &kval,
			 &oid, NULL, NULL);
    if (e < 0) ERR(handle, e);

    /*@ construct kval */
    /* Insert the above object into B+ tree on Btree FileID of SM_SYSINDEXES. */
    kval.len = sizeof(VolNo) + sizeof(Serial);
    memcpy(&(kval.val[0]), (char*)&(siForSt.indexFid.volNo), sizeof(VolNo)); 
    memcpy(&(kval.val[sizeof(VolNo)]), (char*)&(siForSt.indexFid.serial), sizeof(Serial)); 

    /* The dealloc list will not be used in this call. */
    e = BtM_InsertObject(handle, &sysTablesSysIndexesEntry, &sysIndexesBtreeFileIdIndex,
			 &sysIndexesBtreeFileIdIndexKdesc, &kval,
			 &oid, NULL, NULL);
    if (e < 0) ERR(handle, e);

    
    /* Register Btree index on BtreeFileId of SM_SYSINDEXES in the table SM_SYSINDEXES. */
    siForSi.indexFid = stForSi.btree.fid;
    MAKE_INDEXID(siForSi.iid, volId, serialForIndex++);
    siForSi.rootPage = sysIndexesBtreeFileIdIndex.pageNo;
    e = OM_CreateObject(handle, &sysTablesSysIndexesEntry, (ObjectID*)NULL,
			(ObjectHdr*)NULL, sizeof(sm_CatOverlayForSysIndexes),
			(char*)&siForSi, &oid);
    if (e < 0) ERR(handle, e);

    /*@ construct kval */
    /* Insert the above object into B+ tree on IndexID of SM_SYSINDEXES. */
    kval.len = sizeof(VolNo) + sizeof(Serial);
    memcpy(&(kval.val[0]), (char*)&(siForSi.iid.volNo), sizeof(VolNo));
    memcpy(&(kval.val[sizeof(VolNo)]), (char*)&(siForSi.iid.serial), sizeof(Serial)); 

    /* The dealloc list will not be used in this call. */
    e = BtM_InsertObject(handle, &sysTablesSysIndexesEntry, &sysIndexesIndexIdIndex,
			 &sysIndexesIndexIdIndexKdesc, &kval,
			 &oid, NULL, NULL);
    if (e < 0) ERR(handle, e);

    /*@ construct kval */
    /* Insert the above object into B+ tree on Btree FileID of SM_SYSINDEXES. */
    kval.len = sizeof(VolNo) + sizeof(Serial);
    memcpy(&(kval.val[0]), (char*)&(siForSt.indexFid.volNo), sizeof(VolNo)); 
    memcpy(&(kval.val[sizeof(VolNo)]), (char*)&(siForSt.indexFid.serial), sizeof(Serial)); 

    /* The dealloc list will not be used in this call. */
    e = BtM_InsertObject(handle, &sysTablesSysIndexesEntry, &sysIndexesBtreeFileIdIndex,
			 &sysIndexesBtreeFileIdIndexKdesc, &kval,
			 &oid, NULL, NULL);
    if (e < 0) ERR(handle, e);


    /*
    ** Set the fixed information for the catalog table SM_SYSTABLES.
    */
    e = RDsM_InsertMetaEntry(handle, volId, "smSysTablesSysTablesEntry",
                             (char*)&sysTablesSysTablesEntry, sizeof(ObjectID));
    if (e < 0) ERR(handle, e);

    e = RDsM_InsertMetaEntry(handle, volId, "smSysTablesDataFileIdIndex",
                             (char*)&sysTablesDataFileIdIndex, sizeof(PhysicalIndexID));
    if (e < 0) ERR(handle, e);

    e = RDsM_InsertMetaEntry(handle, volId, "smSysTablesBtreeFileIdIndex",
                             (char*)&sysTablesBtreeFileIdIndex, sizeof(PhysicalIndexID)); 
    if (e < 0) ERR(handle, e);

    
    /*
    ** Set the fixed information for the catalog table SM_SYSINDEXES.
    */
    e = RDsM_InsertMetaEntry(handle, volId, "smSysTablesSysIndexesEntry",
                             (char*)&sysTablesSysIndexesEntry, sizeof(ObjectID));
    if (e < 0) ERR(handle, e);

    e = RDsM_InsertMetaEntry(handle, volId, "smSysIndexesIndexIdIndex",
                             (char*)&sysIndexesIndexIdIndex, sizeof(PhysicalIndexID)); 
    if (e < 0) ERR(handle, e);

    e = RDsM_InsertMetaEntry(handle, volId, "smSysIndexesBtreeFileIdIndex",
                             (char*)&sysIndexesBtreeFileIdIndex, sizeof(PhysicalIndexID)); 
    if (e < 0) ERR(handle, e);
    


    /*
     * Create catalog table SM_SYSCOUNTERS.
     */

    /* Create data file for SM_SYSCOUNTERS in the given volume. */
    MAKE_FILEID(fid, volId, serialForFile++);
    e = OM_CreateFile(handle, &fid, 100, &stForSc.data, FALSE);     
    if (e < 0) ERR(handle, e);

    /* Create a B+-tree file for SM_SYSCOUNTERS in the given volume. */
    MAKE_FILEID(fid, volId, serialForFile++);
    e = BtM_CreateFile(handle, &fid, 100, &stForSc.btree);          
    if (e < 0) ERR(handle, e);


    /* 
     *  Register SM_SYSCOUNTERS in the catalog table SM_SYSTABLES. 
     */
    e = OM_CreateObject(handle, (ObjectID*)&sysTablesSysTablesEntry, (ObjectID*)NULL,
			(ObjectHdr*)NULL, sizeof(sm_CatOverlayForSysTables), (char*)&stForSc, &sysTablesSysCountersEntry);
    if (e < 0) ERR(handle, e);


    /*
     * Insert the index entry of SM_SYSCOUNTERS into B+ tree indexes on SM_SYSTABLES.
     */

    /*@ construct kval */
    kval.len = sizeof(VolNo) + sizeof(Serial);
    memcpy(&(kval.val[0]), (char*)&(stForSc.data.fid.volNo), sizeof(VolNo));
    memcpy(&(kval.val[sizeof(VolNo)]), (char*)&(stForSc.data.fid.serial), sizeof(Serial));

    /* insert sysTablesSysCountersEntry into B+ tree on data FileID of SM_SYSTABLES. */
    e = BtM_InsertObject(handle, &sysTablesSysTablesEntry, &sysTablesDataFileIdIndex,
                         &sysTablesDataFileIdIndexKdesc, &kval, &sysTablesSysCountersEntry, NULL, NULL);
    if (e < 0) ERR(handle, e);


    /*@ construct kval */
    kval.len = sizeof(VolNo) + sizeof(Serial);
    memcpy(&(kval.val[0]), (char*)&(stForSc.btree.fid.volNo), sizeof(VolNo));
    memcpy(&(kval.val[sizeof(VolNo)]), (char*)&(stForSc.btree.fid.serial), sizeof(Serial));

    /* insert sysTablesSysCountersEntry into B+ tree on Btree FileID of SM_SYSTABLES. */
    e = BtM_InsertObject(handle, &sysTablesSysTablesEntry, &sysTablesBtreeFileIdIndex,
                         &sysTablesBtreeFileIdIndexKdesc, &kval, &sysTablesSysCountersEntry, NULL, NULL);
    if (e < 0) ERR(handle, e);


    /* 
     *  Create a B+ tree index on the counterName field of SM_SYSCOUNTERS. 
     */

    /* create index */
    e = BtM_CreateIndex(handle, &sysTablesSysCountersEntry, &sysCountersCounterNameIndex);
    if (e < 0) ERR(handle, e);


    /* 
     * Register Btree index on counterName filed of SM_SYSCOUNTERS into the table SM_SYSINDEXES. 
     */

    /* set siForSc */
    siForSc.indexFid = stForSc.btree.fid; 
    MAKE_INDEXID(siForSc.iid, volId, serialForIndex++);
    siForSc.rootPage = sysCountersCounterNameIndex.pageNo;

    /* insert into SM_SYSINDEXES */
    e = OM_CreateObject(handle, &sysTablesSysIndexesEntry, (ObjectID*)NULL,
                        (ObjectHdr*)NULL, sizeof(sm_CatOverlayForSysIndexes), (char*)&siForSc, &oid);
    if (e < 0) ERR(handle, e);


    /*
     * Insert the index entry of index on SM_SYSCOUNTERS into B+ tree indexes on SM_SYINDEXES.
     */

    /*@ construct kval */
    /* Insert the above object into B+ tree on IndexID of SM_SYSINDEXES. */
    kval.len = sizeof(VolNo) + sizeof(Serial);
    memcpy(&(kval.val[0]), (char*)&(siForSc.iid.volNo), sizeof(VolNo));
    memcpy(&(kval.val[sizeof(VolNo)]), (char*)&(siForSc.iid.serial), sizeof(Serial));

    /* The dealloc list will not be used in this call. */
    e = BtM_InsertObject(handle, &sysTablesSysIndexesEntry, &sysIndexesIndexIdIndex,
                         &sysIndexesIndexIdIndexKdesc, &kval, &oid, NULL, NULL);
    if (e < 0) ERR(handle, e);

    /*@ construct kval */
    /* Insert the above object into B+ tree on Btree FileID of SM_SYSINDEXES. */
    kval.len = sizeof(VolNo) + sizeof(Serial);
    memcpy(&(kval.val[0]), (char*)&(siForSc.indexFid.volNo), sizeof(VolNo)); 
    memcpy(&(kval.val[sizeof(VolNo)]), (char*)&(siForSc.indexFid.serial), sizeof(Serial)); 

    /* The dealloc list will not be used in this call. */
    e = BtM_InsertObject(handle, &sysTablesSysIndexesEntry, &sysIndexesBtreeFileIdIndex,
                         &sysIndexesBtreeFileIdIndexKdesc, &kval, &oid, NULL, NULL);
    if (e < 0) ERR(handle, e);


    /*
    ** Set the fixed information for the catalog table SM_COUNTERS.
    */

    /* register 'sysTablesSysCountersEntry' */
    e = RDsM_InsertMetaEntry(handle, volId, "smSysTablesSysCountersEntry",
                             (char*)&sysTablesSysCountersEntry, sizeof(ObjectID));
    if (e < 0) ERR(handle, e);

    /* register 'sysCountersCounterNameIndex' */
    e = RDsM_InsertMetaEntry(handle, volId, "smSysCountersCounterNameIndex",
                             (char*)&sysCountersCounterNameIndex, sizeof(PhysicalIndexID));
    if (e < 0) ERR(handle, e);


    /*
     *  Create entry for sysSerialForFileCounter into SM_SYSCOUNTERS
     */

    /* set 'sysSerialForFileCounter' */
    strcpy(sysSerialForFileCounter.counterName, "smSysSerialForFileCounter");
    sysSerialForFileCounter.counterValue = serialForFile;

    /* register smSysSerialCounter in the catalog table SM_COUNTERS. */
    e = OM_CreateObject(handle, &sysTablesSysCountersEntry, NULL, NULL, 
                        sizeof(sm_SysCountersOverlay_T), (char*)&sysSerialForFileCounter, &oid);
    if (e < 0) ERR(handle, e);
    
    /*@ construct kval */
    kval.len = strlen(sysSerialForFileCounter.counterName);
    memcpy(&kval.val[0], &kval.len, sizeof(Two));
    memcpy(&(kval.val[sizeof(Two)]), sysSerialForFileCounter.counterName, kval.len);
    kval.len += sizeof(Two);

    /* Insert the above object into B+ tree on counterName on SM_SYSCOUNTERS. */
    e = BtM_InsertObject(handle, &sysTablesSysCountersEntry, &sysCountersCounterNameIndex,
                         &sysCountersCounterNameIndexKdesc, &kval, &oid, NULL, NULL);
    if (e < 0) ERR(handle, e);


    /*
     *  Create entry for sysSerialForIndexCounter into SM_SYSCOUNTERS
     */

    /* set 'sysSerialForIndexCounter' */
    strcpy(sysSerialForIndexCounter.counterName, "smSysSerialForIndexCounter");
    sysSerialForIndexCounter.counterValue = serialForIndex;

    /* register smSysSerialCounter in the catalog table SM_COUNTERS. */
    e = OM_CreateObject(handle, &sysTablesSysCountersEntry, NULL, NULL, 
                        sizeof(sm_SysCountersOverlay_T), (char*)&sysSerialForIndexCounter, &oid);
    if (e < 0) ERR(handle, e);
    
    /*@ construct kval */
    kval.len = strlen(sysSerialForIndexCounter.counterName);
    memcpy(&kval.val[0], &kval.len, sizeof(Two));
    memcpy(&(kval.val[sizeof(Two)]), sysSerialForIndexCounter.counterName, kval.len);
    kval.len += sizeof(Two);

    /* Insert the above object into B+ tree on counterName on SM_SYSCOUNTERS. */
    e = BtM_InsertObject(handle, &sysTablesSysCountersEntry, &sysCountersCounterNameIndex,
                         &sysCountersCounterNameIndexKdesc, &kval, &oid, NULL, NULL);
    if (e < 0) ERR(handle, e);


    /*@
     * finalization
     */
    /* Dismount the volume in BfM level. */
    e = BfM_Dismount(handle, volId);
    if (e < 0) ERR(handle, e);

#ifdef DBLOCK
    /* 
     * Release lock for data volume
     */
    e = RDsM_ReleaseVolumeLock(handle, volId);
    if (e < eNOERROR) ERR(handle, e);
#endif

    /* Dismount the volume in RDsM level. */
    e = RDsM_Dismount(handle, volId);
    if (e < 0) ERR(handle, e);
    
    return(eNOERROR);

} /* SM_FormatDataVolume() */



/*@================================
 * createFirstObjectInSysTables()
 *================================*/
/*
 * Function: createFirstObjectInSysTables(PhysicalFileID*, ObjectHdr*, Four, char*, ObjectID*)
 * 
 * Description :
 *  Create the first catalog entry in the catalog table SM_SYSTABLES. The
 *  first catalog entry is for the SM_SYSTABLES itself. The OM_CreateObject()
 *  function in the object manager(OM) request that the catalog entry for
 *  the file where the new object will be put should be in the SM_SYSTABLES.
 *  So we cannot use the OM_CreateObject() function for the first catalog entry
 *  of SM_SYSTABLES becase the catalog entry for SM_SYSTABLES does not exist
 *  in SM_SYSTABLES.
 *
 * Return Values :
 *  error Code
 *    some errors caused by fuction calls
 *
 * Notice:
 *  We assume that the length of the data is less than the half of the page
 *  size. So we don't modify the links of the available space list.
 */
Four createFirstObjectInSysTables(
    Four handle,
    PhysicalFileID* pFid,	/* IN physical file ID */ 
    ObjectHdr   *objHdr,	/* IN from which tag & properties are set */
    Four	length,		/* IN amount of data */
    char	*data,		/* IN the initial data for the object */
    ObjectID	*oid)		/* OUT the object's ObjectID */
{
    Four        e;		/* error number */
    Four	neededSpace;	/* space needed to put new object [+ header] */
    SlottedPage *apage;		/* pointer to the slotted page buffer */
    Four        alignedLen;	/* aligned length of initial data */
    Boolean     needToAllocPage;/* Is there a need to alloc a new page? */
    PageID      pid;            /* PageID in which new object to be inserted */
    Four        firstExt;	/* first Extent No of the file */
    Object      *obj;		/* point to the newly created object */
    Two         i;		/* index variable */
    sm_CatOverlayForData *catEntry; /* pointer to data file catalog information */
    SlottedPage *catPage;	/* pointer to buffer containing the catalog */
    

    /* FileID is the PageID of the first page of the file. */
    pid = *pFid; 

    /*@ get page */
    e = BfM_GetTrain(handle, &pid, (char**)&apage, PAGE_BUF);
    if (e < 0) ERR(handle, e);

    alignedLen = MAX(sizeof(ShortPageID), ALIGNED_LENGTH(length));
    
    /*
     * At this point
     * pid : PageID of the page into which the new object will be placed
     * apage : pointer to the slotted page buffer
     * alignedLen : space for data of the new object
     */
    /* where to put the object[header]? */
    obj = (Object *)&(apage->data[apage->header.free]);
    
    /*@ initialize ObjectHdr */
    obj->header.properties = P_CLEAR;
    obj->header.tag = (objHdr != NULL) ? objHdr->tag:0;
    obj->header.length = alignedLen;
    
    /* copy the data into the object */
    memcpy(obj->data, data, length);
    
    /* find the slot index and update the slot contents */
    for(i = 0; i < apage->header.nSlots; i++)
	if (apage->slot[-i].offset == EMPTYSLOT) break;
    
    /* if no empty slot, then create the new slot */
    if (i == apage->header.nSlots)
	apage->header.nSlots++;	/* increment # of slots */
    
    apage->slot[-i].offset = apage->header.free;
    e = om_GetUnique(handle, &pid, &(apage->slot[-i].unique));
    if (e < 0) ERRB1(handle, e, &pid, PAGE_BUF);

    /* update the pointer to the start of contiguous free space */
    apage->header.free += sizeof(ObjectHdr) + alignedLen;

    /* Construct the ObjectID to be returned */
    if(oid != NULL)
	MAKE_OBJECTID(*oid, pid.volNo, pid.pageNo, i, apage->slot[-i].unique);

    /*@ set dirty */
    e = BfM_SetDirty(handle, &pid, PAGE_BUF);
    if (e < 0) ERRB1(handle, e, &pid, PAGE_BUF);

    /*@ free the buffer page */
    e = BfM_FreeTrain(handle, &pid, PAGE_BUF);
    if (e < 0) ERR(handle, e);

    return(eNOERROR);
    
} /* om_CreateObject() */
