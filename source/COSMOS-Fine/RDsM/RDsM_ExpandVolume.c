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
 * Module: RDsM_ExpandVolume.c
 *
 * Description:
 *  Format the given device as a new volume.
 *
 * Exports:
 *  Four RDsM_ExpandVolume(Four, Four, char**, Four*, Four)
 */


#include <string.h>
#ifndef WIN32
#include <unistd.h>
#else
#include <windows.h>
#endif /* WIN32 */
#include <stdlib.h> /* for malloc & free */
#include <limits.h>
#include <assert.h>
#include "common.h"
#include "trace.h"
#include "error.h"
#include "Util.h"
#include "latch.h"
#include "SHM.h"
#include "RDsM.h"
#include "Util_heap.h"
#include "Util_varArray.h"
#include "perProcessDS.h"
#include "perThreadDS.h"



/*
 * internal function prototypes
 */
Four rdsm_ExpandDataVolume(Four, FileDesc*, RDsM_VolumeInfo_T*, Four);


/*
 * Function: Four RDsM_ExpandVolume(Four, Four, char**, Four*, Four)
 *
 * Description:
 *   Expand a size of exist volume
 *
 * Returns:
 *  Error code
 */
Four RDsM_ExpandVolume(
    Four                 handle,                  /* IN handle */
    Four                 volNo,                   /* IN volume number which will be expanded */
    Four                 numAddDevices,           /* IN number of added devices */
    char                 **addDevNames,           /* IN array of device name */
    Four                 *numPagesInAddDevice)    /* IN # of pages in each added device */
{
    Four                 e;                       /* error code */
    Four                 i;                       /* loop index */
    FileDesc             fd;                      /* file descriptor for added devce */
    Four                 entryNo;                 /* entry number in volTable */
    Four                 oldNumDevices;           /* # of devices before expansion */
    Four                 numAddExtsForTrain;      /* # of extents for train in added devices */
    Four                 numExtsForBitmap;        /* # of extents for bitmap train (unit = # of extents) */
    RDsM_Page_T          aPage;                   /* a RDsM page */
    RDsM_VolumeInfo_T    *volInfo;                /* device information */
    rdsm_VolTableEntry_T *entry;                  /* volume table entry of the given volume */
    RDsM_DevInfo         *oldDevInfo;
    RDsM_DevInfoForDataVol *oldDevInfoForDataVol;
    RDsM_DevInfo         *newDevInfo; 
    RDsM_DevInfoForDataVol *newDevInfoForDataVol; 
    RDsM_SegmentInfo *pageSegInfo, *trainSegInfo; 
    Four		 firstExtentNo = 0; 	  /* first extent no */

    /* pointer for COMMON Data Structure of perThreadTable */
    COMMON_PerThreadDS_T *common_perThreadDSptr = COMMON_PER_THREAD_DS_PTR(handle);


    TR_PRINT(handle, TR_RDSM, TR1, ("RDsM_ExpandVolume(volNo=%lD, numAddDevices=%lD, addDevNames=%P, numPagesInAddDevice=%P)",
                             volNo, numAddDevices, addDevNames, numPagesInAddDevice));

    /*
     * Parameter check
     */
    if (numAddDevices <= 0) ERR(handle, eBADPARAMETER);
    if (addDevNames == NULL || numPagesInAddDevice == NULL) ERR(handle, eBADPARAMETER);


    /*
     * get the corresponding volume table entry via searching the user volTable
     */
    e = rdsm_GetVolTableEntryNoByVolNo(handle, volNo, &entryNo);
    if (e < eNOERROR) ERR(handle, e);


    /*
     *  set a pointer to the corresponding entry & volInfo
     */
    entry = &(RDSM_VOLTABLE[entryNo]);
    volInfo = &entry->volInfo;


    /*
     *  Mutex Begin :: Volume Table Entry
     *  Note!! By holding the entry latch, we keep other process from mounting this volume.
     */
    e = SHM_getLatch(handle, &entry->latch, procIndex, M_EXCLUSIVE, M_UNCONDITIONAL, NULL);
    if (e < eNOERROR) ERR(handle, e);


    /*
     *  check whether other processes mount this volume
     *  Note!! while expading volume, no process access that volume.
     */
    if (entry->nMounts != 1) ERRL1(handle, eINTERNAL, &entry->latch);


    /*
     *  Save value of numDevices in 'oldNumDevices'
     */
    oldNumDevices = volInfo->numDevices;


    /*
     *  Update 'numDevices' in shared volume table & user volume table
     */
    volInfo->numDevices += numAddDevices;
    RDSM_USERVOLTABLE(handle)[entryNo].numDevices += numAddDevices;


    /*
     * Increase devInfo array
     */
    oldDevInfo = PHYSICAL_PTR(volInfo->devInfo);

    e = Util_getArrayFromHeap(handle, &RDSM_DEVINFOTABLEHEAP, volInfo->numDevices, &newDevInfo);
    if (e < eNOERROR) ERRL1(handle, e, &entry->latch);

    memcpy(newDevInfo, oldDevInfo, oldNumDevices*sizeof(RDsM_DevInfo));

    volInfo->devInfo = LOGICAL_PTR(newDevInfo);

    e = Util_freeArrayToHeap(handle, &RDSM_DEVINFOTABLEHEAP, oldDevInfo);
    if (e < eNOERROR) ERRL1(handle, e, &entry->latch);


    /*
     *  Increase devInfoForDataVol array if needed
     */
    if (volInfo->type == VOLUME_TYPE_DATA) {

        oldDevInfoForDataVol = PHYSICAL_PTR(volInfo->dataVol.devInfo);

        e = Util_getArrayFromHeap(handle, &RDSM_DEVINFOFORDATAVOLTABLEHEAP, volInfo->numDevices, &newDevInfoForDataVol);
        if (e < eNOERROR) ERRL1(handle, e, &entry->latch);

        memcpy(newDevInfoForDataVol, oldDevInfoForDataVol, oldNumDevices*sizeof(RDsM_DevInfoForDataVol));

        volInfo->dataVol.devInfo = LOGICAL_PTR(newDevInfoForDataVol);

        e = Util_freeArrayToHeap(handle, &RDSM_DEVINFOFORDATAVOLTABLEHEAP, oldDevInfoForDataVol);
        if (e < eNOERROR) ERRL1(handle, e, &entry->latch);
    }


    /*
     *  For each added device, insert information about that device into entry of volume table
     */
    for (i = oldNumDevices; i < volInfo->numDevices; i++) {

        /*
         *  doubling openFileDesc array if needed
         */
        if(i >= RDSM_USERVOLTABLE(handle)[entryNo].openFileDesc.nEntries) {
            e = Util_doublesizeVarArray(handle, &RDSM_USERVOLTABLE(handle)[entryNo].openFileDesc, sizeof(FileDesc));
            if (e < eNOERROR) ERRL1(handle, e, &entry->latch);
        }

        /*
         *  insert information of added devices into devInfo in the entry of shared volume table
         */
        strcpy(newDevInfo[i].devName, addDevNames[i-oldNumDevices]); 
        newDevInfo[i].numExtsInDevice = numPagesInAddDevice[i-oldNumDevices] / volInfo->extSize; 

        /*
         *  Open added device
         */
#ifndef WIN32

#ifndef _LARGEFILE64_SOURCE 
	if ((fd = open(newDevInfo[i].devName, O_RDWR | O_SYNC | O_CREAT, CREATED_VOLUME_PERM)) == -1) 
#else
	if ((fd = open64(newDevInfo[i].devName, O_RDWR | O_SYNC | O_CREAT, CREATED_VOLUME_PERM)) == -1) 
#endif

#else
	if ((fd = CreateFile(newDevInfo[i].devName, GENERIC_WRITE | GENERIC_READ, FILE_SHARE_READ | FILE_SHARE_WRITE, NULL,
		             OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL | FILE_FLAG_NO_BUFFERING, NULL)) == INVALID_HANDLE_VALUE) 
#endif /* WIN32 */
            ERRL1(handle, eDEVICEOPENFAIL_RDSM, &entry->latch);


        /*
         *  Write dummy contents into the last page for added device :
         *  meaningful only in the case of using a unix file as a volume
         */
        e = rdsm_WriteTrain(handle, fd, newDevInfo[i].numExtsInDevice*volInfo->extSize-1, &aPage, PAGESIZE2); 
        if (e < eNOERROR) ERRL1(handle, e, &entry->latch);

        /*
         *  Set openFileDesc of user volume table
         */
        OPENFILEDESC_ARRAY(RDSM_USERVOLTABLE(handle)[entryNo].openFileDesc)[i] = fd;

        /*
         *  Update 'numExts' in shared volume table
         */
        volInfo->numExts += newDevInfo[i].numExtsInDevice; 
    }


    /*
     *  Update information about data volume if needed
     */
    if (volInfo->type == VOLUME_TYPE_DATA) {

        /* set for data volume & get relatted masterPage information */
        e = rdsm_ExpandDataVolume(handle, OPENFILEDESC_ARRAY(RDSM_USERVOLTABLE(handle)[entryNo].openFileDesc), volInfo, oldNumDevices);
        if (e < eNOERROR) ERRL1(handle, e, &entry->latch);
    }


    /*
     *  Write volume information page because 'numDevices' is updated
     */

    aPage.vi.hdr.pid = volInfo->volInfoPageId;
    SET_PAGE_TYPE(&aPage, VOL_INFO_PAGE_TYPE);
    aPage.vi.hdr.lsn = common_perThreadDSptr->minLsn;
    aPage.vi.hdr.logRecLen = 0;

    strcpy(aPage.vi.title, volInfo->title);
    aPage.vi.volNo = volInfo->volNo;
    aPage.vi.type = volInfo->type;
    aPage.vi.numDevices = volInfo->numDevices;
    aPage.vi.extSize = volInfo->extSize;

    if (volInfo->type == VOLUME_TYPE_DATA) {

        aPage.vi.dataVol.eff = volInfo->dataVol.eff;
        aPage.vi.dataVol.metaDictPid = volInfo->dataVol.metaDictPid;
        aPage.vi.dataVol.metaDictSize = volInfo->dataVol.metaDictSize;
        aPage.vi.dataVol.freeExtent = volInfo->dataVol.freeExtent;
        aPage.vi.dataVol.numOfFreeExtent = volInfo->dataVol.numOfFreeExtent;
        aPage.vi.dataVol.freePageExtent = volInfo->dataVol.freePageExtent;
        aPage.vi.dataVol.numOfFreePageExtent = volInfo->dataVol.numOfFreePageExtent;
        aPage.vi.dataVol.freeTrainExtent = volInfo->dataVol.freeTrainExtent;
        aPage.vi.dataVol.numOfFreeTrainExtent = volInfo->dataVol.numOfFreeTrainExtent;
    }

    /* write volume information page */
    /* Note!! volume information page always locates in first device */
    e = rdsm_WriteTrain(handle, OPENFILEDESC_ARRAY(RDSM_USERVOLTABLE(handle)[entryNo].openFileDesc)[0], volInfo->volInfoPageId.pageNo, &aPage.vi, PAGESIZE2);
    if (e < eNOERROR) ERRL1(handle, e, &entry->latch);


    /*
     *  Write Master page into each added device & Update segment table in 'volInfo'
     */

    /* set header of master page */
    SET_PAGE_TYPE(&aPage, MASTER_PAGE_TYPE);
    aPage.ms.hdr.lsn = common_perThreadDSptr->minLsn;
    aPage.ms.hdr.logRecLen = 0;

    /* get 'firstExtentNo' that is a first extent no */
    oldDevInfo = PHYSICAL_PTR(volInfo->devInfo);
    for (i=0; i<oldNumDevices; i++) firstExtentNo += oldDevInfo[i].numExtsInDevice;

    /* write for each devices */
    for (i = oldNumDevices; i < volInfo->numDevices; i++) {

        /* set 'pid' of header */
        /* Note!! master page is first page of each added page segments */
        MAKE_PAGEID(aPage.ms.hdr.pid, volNo, firstExtentNo*volInfo->extSize);

        /* set contents of master page */
        strcpy(aPage.ms.tag, DEVICE_TAG);
        aPage.ms.volNo = volInfo->volNo;
        aPage.ms.volInfoPageId = volInfo->volInfoPageId;
        aPage.ms.devNo = i;
        aPage.ms.numExtsInDevice = newDevInfo[i].numExtsInDevice; 

        if (volInfo->type == VOLUME_TYPE_DATA) {

            aPage.ms.dataVol.bitmapTrainId = newDevInfoForDataVol[i].bitmapTrainId;
            aPage.ms.dataVol.bitmapSize = newDevInfoForDataVol[i].bitmapSize;
            aPage.ms.dataVol.uniqNumPid = newDevInfoForDataVol[i].uniqNumPid;
            aPage.ms.dataVol.uniqPartitionSize = newDevInfoForDataVol[i].uniqPartitionSize;
            aPage.ms.dataVol.numUniqNumEntries = newDevInfoForDataVol[i].numUniqNumEntries;

            aPage.ms.dataVol.extentMapPageId = newDevInfoForDataVol[i].extentMapPageId;
            aPage.ms.dataVol.extentMapPageSize = newDevInfoForDataVol[i].extentMapPageSize;
            aPage.ms.dataVol.firstExtentInDevice = newDevInfoForDataVol[i].firstExtentInDevice;

            firstExtentNo += aPage.ms.numExtsInDevice;
        }

        /* write master page into first of the device */
        e = rdsm_WriteTrain(handle, OPENFILEDESC_ARRAY(RDSM_USERVOLTABLE(handle)[entryNo].openFileDesc)[i], 0, &aPage.ms, PAGESIZE2);
        if (e < eNOERROR) ERRL1(handle, e, &entry->latch);
    }


    /*
     *  Mutex End :: Volume Table Entry
     */
    e = SHM_releaseLatch(handle, &entry->latch, procIndex);
    if ( e < eNOERROR ) ERR(handle, e);

    return(eNOERROR);

} /* RDsM_ExpandVolume() */



/*
 * Function: Four rdsm_ExpandDataVolume(int*, RDsM_VolumeInfo_T*, Four, Four, Four)
 *
 * Description:
 *  Expand the data volume.
 *
 * Returns:
 *  error code
 */
Four rdsm_ExpandDataVolume(
    Four              handle,                 /* IN handle */
    FileDesc          *fd,                    /* IN array of file descriptor */
    RDsM_VolumeInfo_T *volInfo,               /* In volume information */
    Four              oldNumDevices)          /* IN # of devices before expansion */
{
    Four              e;                      /* error code */
    Four              extSize;                /* size of an extent (unit = # of pages) */
    Four              i, j, k;                /* index variable */
    Four              numExtsForBitmap;       /* # of extents for bitmap train (unit = # of extents) */
    Four              accumPnoForPages;       /* page number for page segments */
    Four              accumPnoForTrains;      /* page number for train segments */
    MetaDictPage_T    *mdPage;                /* meta dictionary page */
    BitmapTrain_T     *bmTrain;               /* bitmap train */
    UniqNumPage_T     *unPage;                /* unique number page */
    Page_T            *aPage;                 /* a page with all common attributes */
    char              aTrainBuf[TRAINSIZE];   /* buffer for page/train */
    PageID            pid;                    /* pointer to a page identifier */
    RDsM_DevInfo      *devInfo; 
    RDsM_DevInfoForDataVol *devInfoForDataVol; 
    Four	      firstExtentNo = 0;      /* first extent no */
    Four              *numSystemExtentInDevice; /* number of system extent in the device */
    Four	      nPagesInBuffer;
    Page_T            *pagesBufPtr;
    ExtentMapPage_T   *extentMap;
    ExtentMapEntry_T  *extentMapEntry;
    Four	      curExtentNoInDevice;
    Four              nSysPages;              /* # of system pages to clear in a Bitmap Page*/
    Four              nRemainSysPages;        /* # of system pages to clear in BitMap */


    /* pointer for COMMON Data Structure of perThreadTable */
    COMMON_PerThreadDS_T *common_perThreadDSptr = COMMON_PER_THREAD_DS_PTR(handle);


    TR_PRINT(handle, TR_RDSM, TR1, ("rdsm_FormatDataVolume(fd=%P, volInfo=%P, oldNumDevices=%ld)", fd, volInfo, oldNumDevices));


    /*
     *  O. Set 'extSize' for fast access
     */
    extSize = volInfo->extSize;


    /*
     *  I. Set variable of dataVol
     */
    devInfo = PHYSICAL_PTR(volInfo->devInfo); 
    devInfoForDataVol = PHYSICAL_PTR(volInfo->dataVol.devInfo); 

    /* get 'firstExtentNo' that is a first extent no */
    for (i=0; i<oldNumDevices; i++) firstExtentNo += devInfo[i].numExtsInDevice;

    /* for each added device */
    for (i = oldNumDevices; i < volInfo->numDevices; i++) {

        /* set variable about unique number page */
        devInfoForDataVol[i].uniqNumPid.volNo = volInfo->volNo;
        devInfoForDataVol[i].uniqNumPid.pageNo = (i > 0) ? firstExtentNo * extSize + NUM_DEVICE_MASTER_PAGES :
					                   firstExtentNo * extSize + NUM_DEVICE_MASTER_PAGES +
							   NUM_VOLUME_INFO_PAGES + NUM_VOLUME_METADICT_PAGES;

		devInfoForDataVol[i].numUniqNumEntries = MIN(NUMUNIQUEENTRIESPERPAGE, devInfo[i].numExtsInDevice*extSize);
        devInfoForDataVol[i].uniqPartitionSize = (devInfo[i].numExtsInDevice*extSize-1)/devInfoForDataVol[i].numUniqNumEntries + 1;

        /* set variable about bitmap train */
        devInfoForDataVol[i].bitmapTrainId.volNo = volInfo->volNo;
        devInfoForDataVol[i].bitmapTrainId.pageNo = devInfoForDataVol[i].uniqNumPid.pageNo +
                                                    NUM_VOLUME_UNIQ_PAGES; 
        devInfoForDataVol[i].bitmapSize = ((devInfo[i].numExtsInDevice-1) / volInfo->dataVol.numExtMapsInTrain + 1) * TRAINSIZE2;


        /* set information about extent map array page */
        devInfoForDataVol[i].extentMapPageSize = ((devInfo[i].numExtsInDevice-1) / EXTENTMAPENTRIESPERPAGE + 1) * PAGESIZE2;
        devInfoForDataVol[i].extentMapPageId.volNo = volInfo->volNo;
        devInfoForDataVol[i].extentMapPageId.pageNo = devInfoForDataVol[i].bitmapTrainId.pageNo +
                                                      devInfoForDataVol[i].bitmapSize;
        devInfoForDataVol[i].firstExtentInDevice = firstExtentNo;

        firstExtentNo += devInfo[i].numExtsInDevice;

    }


    /*
     *  Note!! meta dictionary pages always located  in first device. So we need not to update variables about them
     */


    /*
     *  II. initialize unique number pages in each added device
     */

    unPage = (UniqNumPage_T*)&aTrainBuf;

    /* initialize header of meta dictionary page */
    SET_PAGE_TYPE(unPage, UNIQUE_NUM_PAGE_TYPE);
    unPage->hdr.lsn = common_perThreadDSptr->minLsn;
    unPage->hdr.logRecLen = 0;

    /* intialize each entries in unique number page */
    for (i = 0; i < NUMUNIQUEENTRIESPERPAGE; i++) unPage->uniques[i] = 0;

    /* for each added devices in volume */
    for (i = oldNumDevices; i < volInfo->numDevices; i++) {

        /* set PID of unique number page */
        unPage->hdr.pid = devInfoForDataVol[i].uniqNumPid; 

        /* Write this page into the disk */
        e = rdsm_WriteTrain(handle, fd[i], NUM_DEVICE_MASTER_PAGES, unPage, PAGESIZE2);
        if (e < eNOERROR) ERR(handle, e);
    }


    /*
     *  Initialize 'numSystemExtentInDevice' which is used to initialize bitmap & extent map
     */
    numSystemExtentInDevice = (Four*) malloc (sizeof(Four) * volInfo->numDevices);
    if (numSystemExtentInDevice == NULL) ERR(handle, eMEMORYALLOCERR);

    for (i = 0; i < volInfo->numDevices; i++) {

        numSystemExtentInDevice[i] = (i == 0) ? ((NUM_SYSTEM_PAGES_IN_FIRST_DEVICE +
                                                 devInfoForDataVol[i].bitmapSize +
                                                 devInfoForDataVol[i].extentMapPageSize - 1 ) / extSize + 1)
                                             : ((NUM_SYSTEM_PAGES_IN_NOT_FIRST_DEVICE +
                                                 devInfoForDataVol[i].bitmapSize +
                                                 devInfoForDataVol[i].extentMapPageSize - 1 ) / extSize + 1);
    }


    /*
     *	III. initialize all bitmap trains in each added device
     */

    bmTrain = (BitmapTrain_T*) &aTrainBuf;

    /* initialize header of bitmap train */
    SET_PAGE_TYPE(bmTrain, BITMAP_TRAIN_TYPE);
    bmTrain->hdr.lsn = common_perThreadDSptr->minLsn;
    bmTrain->hdr.logRecLen = 0;

    /* for each added devices in volume */
    for (i = oldNumDevices; i < volInfo->numDevices; i++) {

        /* initialize bitmap train */
        Util_SetBits(handle, bmTrain->bytes, 0, USABLE_BYTES_PER_BITMAP_TRAIN*CHAR_BIT);

        /*
         * initialize the bitmap train except for first bitmap train
         */

        for (j = TRAINSIZE2; j < devInfoForDataVol[i].bitmapSize; j += TRAINSIZE2) { 

            /* set ID of each bitmap train */
            bmTrain->hdr.pid.volNo = volInfo->volNo;
            bmTrain->hdr.pid.pageNo = devInfoForDataVol[i].bitmapTrainId.pageNo + j; 

            /* Write this page into the disk */
            e = rdsm_WriteTrain(handle, fd[i], bmTrain->hdr.pid.pageNo - devInfoForDataVol[i].firstExtentInDevice * extSize,
				bmTrain, TRAINSIZE2);
            if (e < eNOERROR) ERR(handle, e);
        }


        /*
         * Because previous code cannot support the large database (>8T), we modify the previous code
         */
        for (j = 0, nRemainSysPages = numSystemExtentInDevice[i]*extSize; nRemainSysPages > 0; j += TRAINSIZE2) {

            /* calculate # of pages for the system information to clear in a Bitmap Page */
            nSysPages = MIN(USABLE_BYTES_PER_BITMAP_TRAIN*CHAR_BIT, nRemainSysPages);

            /* initialize bitmap train */
            Util_SetBits(handle, bmTrain->bytes, 0, USABLE_BYTES_PER_BITMAP_TRAIN*CHAR_BIT);

            /* allocate pages for the system information */
            /* Note!! whole parts of extents for meta data pages & bitmap trains are reserved */
            Util_ClearBits(handle, bmTrain->bytes, 0, nSysPages);

            /* set ID of first bitmap page */
            bmTrain->hdr.pid.pageNo = devInfoForDataVol[i].bitmapTrainId.pageNo + j; 
            bmTrain->hdr.pid.volNo  = devInfoForDataVol[i].bitmapTrainId.volNo; 

            /* Write first train */
            e = rdsm_WriteTrain(handle, fd[i], bmTrain->hdr.pid.pageNo - devInfoForDataVol[i].firstExtentInDevice * extSize,
			        bmTrain, TRAINSIZE2);
            if (e < eNOERROR) ERR(handle, e);

            /* calculate # of remain pages for the system information to clear in BitMap */
            nRemainSysPages -= nSysPages;
        }

    } /* end for */


    /* Set some variables of volInfoPage */
    volInfo->dataVol.freeExtent = devInfoForDataVol[oldNumDevices].firstExtentInDevice +
                                      numSystemExtentInDevice[oldNumDevices];
    for (i = oldNumDevices; i < volInfo->numDevices; i++) {
        volInfo->dataVol.numOfFreeExtent += (devInfo[i].numExtsInDevice - numSystemExtentInDevice[i]);
    }


    /*
     *  IV. initialize all extent map pages in each device
     */
    extentMap = (ExtentMapPage_T*) aTrainBuf;

    /* initialize header of extent map page */
    SET_PAGE_TYPE(extentMap, EXT_ENTRY_PAGE_TYPE);
    extentMap->hdr.lsn = common_perThreadDSptr->minLsn;
    extentMap->hdr.logRecLen = 0;

    /* for each device, write extent map array entries */
    for (i = oldNumDevices; i < volInfo->numDevices; i++) {

        for (j = 0; j < devInfoForDataVol[i].extentMapPageSize; j += PAGESIZE2) {

            /* set ID of each extent map page */
            extentMap->hdr.pid.volNo  = volInfo->volNo;
            extentMap->hdr.pid.pageNo = devInfoForDataVol[i].extentMapPageId.pageNo + j;

            /* initialize 'curExtentNoInDevice' in extent map array */
            curExtentNoInDevice = j * EXTENTMAPENTRIESPERPAGE;

            for (k = 0, extentMapEntry = extentMap->entry; k < EXTENTMAPENTRIESPERPAGE; k++, extentMapEntry++) {

                /* set extent map entry */
                /* 1. prevExt */
                extentMapEntry->prevExt = NIL;

                /* 2-1. if extent is system extents, exclude from free extent list */
                if (curExtentNoInDevice < numSystemExtentInDevice[i]) {

                    extentMapEntry->nextExt = NIL;
                }

                /* 2-2. if extent is not system extents, insert into free extent list */
                else {

                    if (curExtentNoInDevice < devInfo[i].numExtsInDevice-1) {

                        extentMapEntry->nextExt = devInfoForDataVol[i].firstExtentInDevice + curExtentNoInDevice + 1;
                    }
                    else {

                        extentMapEntry->nextExt = (i<volInfo->numDevices-1) ? devInfoForDataVol[i+1].firstExtentInDevice +
                                                                              numSystemExtentInDevice[i+1]
                                                                             : volInfo->dataVol.freeExtent;
                        break;
                    }
                }

                /* update 'curExtentNoInDevice' */
                curExtentNoInDevice++;
            }

            /* write this page into the disk */
            e = rdsm_WriteTrain(handle, fd[i], extentMap->hdr.pid.pageNo - devInfoForDataVol[i].firstExtentInDevice * extSize,
				extentMap, PAGESIZE2);
            if (e < eNOERROR) ERR(handle, e);
        }
    }


    /*
     * V. write the lsn field of each page for the recovery purpose
     *
     * Consider the following senario:
     *
     * 1) initially, page A has never updated before, thus lsn field of page A has an arbitrary value
     * 2) action B updates page A and fills in the lsn field
     * 3) log record C for action B is written, however, page A is not forced in the disk
     * 4) the system crashes, and restart processing starts
     * 5) in the redo pass, page A is checked, however, its lsn field is greater than that
     *    of log record C accidentally since the initial lsn field has arbitrary value.
     * 6) action B is regarded as the reflected one (ERROR !!)
     *
     * thus, in this implementation, we decied to initialize each of the field.
     */

    nPagesInBuffer = extSize*VOLUME_FORMAT_WRITEBUFFERSIZE_IN_EXTENTS;
    pagesBufPtr = malloc(sizeof(Page_T)*nPagesInBuffer);
    if (pagesBufPtr == NULL) ERR(handle, eMEMORYALLOCERR);

    /* intialize header of each page */
    for (i = 0; i < nPagesInBuffer; i++) {

        aPage = &pagesBufPtr[i];

        aPage->header.pid.volNo = volInfo->volNo;
        aPage->header.pid.pageNo = NIL;
        SET_PAGE_TYPE(aPage, FREE_PAGE_TYPE);
        aPage->header.lsn = common_perThreadDSptr->minLsn;
        aPage->header.logRecLen = 0;
    }

    /* for each devices, initialize all pages */
    for (i = oldNumDevices; i < volInfo->numDevices; i++) {

        for (j = numSystemExtentInDevice[i]; 
             j < devInfo[i].numExtsInDevice;
             j += VOLUME_FORMAT_WRITEBUFFERSIZE_IN_EXTENTS) {

            if (devInfo[i].numExtsInDevice - j < VOLUME_FORMAT_WRITEBUFFERSIZE_IN_EXTENTS)
                nPagesInBuffer = (devInfo[i].numExtsInDevice - j) * extSize;

            e = rdsm_WriteTrain(handle, fd[i], j*extSize, pagesBufPtr, nPagesInBuffer);
            if (e < eNOERROR) {
                free(pagesBufPtr);
                ERR(handle, e);
            }
        }
    }

    free(pagesBufPtr);
    free(numSystemExtentInDevice); 


    return(eNOERROR);

} /* rdsm_ExpandDataVolume() */
