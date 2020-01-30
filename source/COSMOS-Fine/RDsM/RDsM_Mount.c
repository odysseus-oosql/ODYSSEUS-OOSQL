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
 * Module: RDsM_Mount.c
 *
 * Description:
 *  Mount a named device
 *
 * Exports:
 *  Four RDsM_Mount(Four, Four, char**, Four*, Boolean)
 */


#include <stdlib.h>
#include <assert.h>
#include <string.h>
#ifndef WIN32
#include <unistd.h>
#else
#include <windows.h>
#endif /* WIN32 */
#include "common.h"
#include "trace.h"
#include "error.h"
#include "latch.h"
#include "SHM.h"
#include "RDsM.h"
#include "TM.h"
#include "LOG.h"
#include "Util_varArray.h"
#include "Util_heap.h"
#include "perProcessDS.h"
#include "perThreadDS.h"




/*
 * Function: Four RDsM_Mount(Four, Four, char**, Four*, Boolean)
 *
 * Description:
 *   Mount a named device.
 *
 * Returns:
 *  Error code
 */
Four RDsM_Mount(
    Four                 handle,                  /* IN    handle */
    Four                 numDevices,              /* IN  number of devices */
    char                 **devNames,              /* IN  array of the device's name to be mount */
    Four                 *volId,                  /* OUT volume number */
    Boolean              logFlag)                 /* IN  indicates whether logging is performed */
{
    Four                 e;                       /* error code */
    Four                 i, j;                    /* loop variable */
    Four                 idx;
    Four                 entryNo;                 /* entry no of volume table entry corresponding to the given volume */
    RDsM_Page_T          aPage;                   /* a RDsM Page */
    PageID               volInfoPid;              /* the ID of volume information page */
    VolInfoPage_T        *volInfoPage;            /* volume information page */
    MasterPage_T         *masterPage;             /* device master page */
    FileDesc             *fd;                     /* open file descriptor for the log volume */
    Four                 accumNumExts;            /* accumulated # of extents */
    Four                 *numExtsInPageSegment;
    Four                 *numExtsInTrainSegment;
    Lsn_T                lsn;                     /* LSN of the newly written log record */
    Four                 logRecLen;               /* log record length */
    LOG_LogRecInfo_T     logRecInfo;              /* log record information */
    RDsM_VolumeInfo_T    *volInfo;                /* volume information in volume table entry */
    rdsm_VolTableEntry_T *entry;                  /* volume table entry corresponding to the given volume */
    LOG_Image_RDsM_MountedVol_T mountedVol;       /* mounted volume log image */
    RDsM_DevInfo         *devInfo; 
    RDsM_DevInfoForDataVol *devInfoForDataVol; 
    RDsM_SegmentInfo     *pageSegInfo, *trainSegInfo; 
    Four     		 devNo;			  /* device number in the volume */
    Four                 extNo;                   /* extent number */
    Four                 extOffset;               /* extent offset in the device */
    Four     		 trainOffset;	          /* offset of train in the device (unit = # of page) */
    Four                 pos;                     /* 1st train position */
    Four                 count;                   /* # of free page in a extent */
    BitmapTrain_T        *bmTrain;                /* pointer to a train buffer */
    PageID               bmTrainId;               /* bitmap train identifier */
    Buffer_ACC_CB     	 *bmTrain_BCBP;           /* buffer page access control block */
    Four     		 lastPageOffsetInSegment; /* last page offset in segment */
    Buffer_ACC_CB        *volInfoPage_BCBP;       /* BCBP of volume info page */

    /* pointer for COMMON Data Structure of perThreadTable */
    COMMON_PerThreadDS_T *common_perThreadDSptr = COMMON_PER_THREAD_DS_PTR(handle);


    TR_PRINT(handle, TR_RDSM, TR1, ("RDsM_Mount(numDevices=%ld, devNames=%P, volId=%P, logFlag=%ld)",
                            numDevices, devNames, volId, logFlag));


    /*
     * Parameter check
     */
    if (numDevices <= 0 || devNames == NULL || volId == NULL) ERR(handle, eBADPARAMETER);


    /*
     * Allocate memory for local variable
     */
    fd = (FileDesc *) malloc(sizeof(FileDesc)*numDevices);
    numExtsInPageSegment = (Four *) malloc(sizeof(Four)*numDevices*MAX_NUM_SEGMENT_IN_DEVICE);
    numExtsInTrainSegment = (Four *) malloc(sizeof(Four)*numDevices*MAX_NUM_SEGMENT_IN_DEVICE);


    /*
     * Mutex Begin : for controlling mount operation with other mount operations
     */
    e = SHM_getLatch(handle, &RDSM_LATCH_VOLTABLE, procIndex, M_EXCLUSIVE, M_UNCONDITIONAL, NULL);
    if ( e < eNOERROR ) ERR(handle, e);


    /*
     * first check given devices is proper set
     */
    e = rdsm_CheckDevices(handle, numDevices, devNames, &volInfoPid);
    if ( e < eNOERROR ) ERRL1(handle, e, &RDSM_LATCH_VOLTABLE);


    /*
     *	check whether the volume is already mounted
     *  Note!! In this operation, we check only first device of volume
     *         Because we guarantee devNames[0] has first device's name by above checking operation
     */

    for (entryNo = 0, entry = &RDSM_VOLTABLE[0]; entryNo < MAXNUMOFVOLS; entryNo++, entry++) {

        /* points to the volume information for the fast access */
        volInfo = &entry->volInfo;
        devInfo = PHYSICAL_PTR(volInfo->devInfo); 

        if (devInfo != NULL && strcmp(devInfo[0].devName, devNames[0]) == 0) { /* found */ 

            /* Mutex Begin :: Volume Table Entry  */
            e = SHM_getLatch(handle, &entry->latch, procIndex, M_EXCLUSIVE, M_UNCONDITIONAL, NULL);
            if (e < eNOERROR) ERRL1(handle, e, &RDSM_LATCH_VOLTABLE);

            /* If volume is dismounted, we must mount it again!! */
            devInfo = PHYSICAL_PTR(volInfo->devInfo); 
            if (devInfo != NULL && strcmp(devInfo[0].devName, devNames[0]) != 0) { 

                /* Mutex End : This volume was dismounted now. So retry mount operation */
                e = SHM_releaseLatch(handle, &entry->latch, procIndex);
                if (e < eNOERROR) ERRL1(handle, e, &RDSM_LATCH_VOLTABLE);

                break;
            }

            /*
             * The volume has been mounted on the system.
             * Check whether this process has mounted it.
             */
            if (RDSM_USERVOLTABLE(handle)[entryNo].volNo != NOVOL) {

                assert(RDSM_USERVOLTABLE(handle)[entryNo].volNo == volInfo->volNo);

                // Mutex End : for controlling mount operation with other mount operations 
                e = SHM_releaseLatch(handle, &entry->latch, procIndex);
                if (e < eNOERROR) ERRL1(handle, e, &RDSM_LATCH_VOLTABLE);

                // Mutex End : for controlling mount operation with other mount operations 
                e = SHM_releaseLatch(handle, &RDSM_LATCH_VOLTABLE, procIndex);
                if (e < eNOERROR) ERR(handle, e);

                *volId = volInfo->volNo;

                // return with error code 
                ERR(handle, eVOLALREADYMOUNTED_RDSM);
            }

            /*
             * Mount the volume on this process.
             * This volume is already mounted by another process.
             * However, the device should be opened for accesses by this process.
             */

            /* set 'volNo' of user volume table */
            RDSM_USERVOLTABLE(handle)[entryNo].volNo = volInfo->volNo;

            /* set 'numDevices' of user volume table */
            RDSM_USERVOLTABLE(handle)[entryNo].numDevices = numDevices;

            /* set 'openFileDesc' of user volume table */
            for (i = 0; i < numDevices; i++) {        /* for each device of volume */
#ifndef WIN32

#ifndef _LARGEFILE64_SOURCE 
		if ((fd[i] = open(devNames[i], O_RDWR | O_SYNC)) == -1) { 
#else
		if ((fd[i] = open64(devNames[i], O_RDWR | O_SYNC)) == -1) { 
#endif

#else
	        if ((fd[i] = CreateFile(devNames[i], GENERIC_WRITE | GENERIC_READ, FILE_SHARE_READ | FILE_SHARE_WRITE, NULL,
			                OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL | FILE_FLAG_NO_BUFFERING, NULL)) == INVALID_HANDLE_VALUE) {
#endif /* WIN32 */
                    /* Mutex End : for controlling mount operation with other mount operations */
                    e = SHM_releaseLatch(handle, &entry->latch, procIndex);
                    if (e < eNOERROR) ERRL1(handle, e, &RDSM_LATCH_VOLTABLE);

                    /* Mutex End : for controlling mount operation with other mount operations */
                    e = SHM_releaseLatch(handle, &RDSM_LATCH_VOLTABLE, procIndex);
                    if (e < eNOERROR) ERR(handle, e);

                    ERR(handle, eDEVICEOPENFAIL_RDSM);
                }

                OPENFILEDESC_ARRAY(RDSM_USERVOLTABLE(handle)[entryNo].openFileDesc)[i] = fd[i];
            }

            /* Increase the mount count. */
            entry->nMounts ++;

            /* set return parameter */
            *volId = volInfo->volNo;

            /* Mutex End : for controlling mount operation with other mount operations */
            e = SHM_releaseLatch(handle, &entry->latch, procIndex);
            if (e < eNOERROR) ERRL1(handle, e, &RDSM_LATCH_VOLTABLE);

            /* Mutex End : for controlling mount operation with other mount operations */
            e = SHM_releaseLatch(handle, &RDSM_LATCH_VOLTABLE, procIndex);
            if (e < eNOERROR) ERR(handle, e);

            return(eNOERROR);
        }
    }


    /*
     * The volume isn't mounted. So we should mount it.
     */


    /*
     * Find an empty entry, whose nMounts value is less than or equal to 0.
     */

    for (entryNo = 0, entry = &RDSM_VOLTABLE[0]; entryNo < MAXNUMOFVOLS; entryNo++, entry++)
        if (entry->volInfo.volNo == NOVOL) break;

    /* too many volumes exist in volume table */
    if (entryNo >= MAXNUMOFVOLS) ERRL1(handle, eTOOMANYVOLUMES, &RDSM_LATCH_VOLTABLE);


    /*
     *	open the volume device
     */
    for (i = 0; i < numDevices; i++) {        /* for each device of volume */

#ifndef WIN32

#ifndef _LARGEFILE64_SOURCE 
	if ((fd[i] = open(devNames[i], O_RDWR | O_SYNC)) == -1) ERRL1(handle, eDEVICEOPENFAIL_RDSM, &RDSM_LATCH_VOLTABLE); 
#else
	if ((fd[i] = open64(devNames[i], O_RDWR | O_SYNC)) == -1) ERRL1(handle, eDEVICEOPENFAIL_RDSM, &RDSM_LATCH_VOLTABLE); 
#endif

#else
	if ((fd[i] = CreateFile(devNames[i], GENERIC_WRITE | GENERIC_READ, FILE_SHARE_READ | FILE_SHARE_WRITE, NULL,
			        OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL | FILE_FLAG_NO_BUFFERING, NULL)) == INVALID_HANDLE_VALUE) ERRL1(handle, eDEVICEOPENFAIL_RDSM, &RDSM_LATCH_VOLTABLE);
#endif /* WIN32 */
    }


    /*
     *  Read the volume information page
     */
    /* initialize 'volInfoPage' */
    volInfoPage = &aPage.vi;

    /* Note!! volume information page always located in the first device */
    e = rdsm_ReadTrain(handle, fd[0], volInfoPid.pageNo, volInfoPage, PAGESIZE2);
    if (e < eNOERROR) ERRL1(handle, e, &RDSM_LATCH_VOLTABLE);


    /*
     *  Set part of variables in the volInfo from 'volInfoPage'
     */
    strcpy(entry->volInfo.title, volInfoPage->title);
    entry->volInfo.volNo = volInfoPage->volNo;
    entry->volInfo.type = volInfoPage->type;
    entry->volInfo.volInfoPageId = volInfoPid;
    entry->volInfo.extSize = volInfoPage->extSize;
    entry->volInfo.numDevices = volInfoPage->numDevices;

    if (entry->volInfo.type == VOLUME_TYPE_DATA) {
        entry->volInfo.dataVol.eff = volInfoPage->dataVol.eff;

        entry->volInfo.dataVol.numExtMapsInTrain = USABLE_BYTES_PER_BITMAP_TRAIN*CHAR_BIT / volInfoPage->extSize;

        entry->volInfo.dataVol.metaDictPid = volInfoPage->dataVol.metaDictPid;
        entry->volInfo.dataVol.metaDictSize = volInfoPage->dataVol.metaDictSize;

        /* set the variables about extent map */
        entry->volInfo.dataVol.numExtentMapEntryInPage = EXTENTMAPENTRIESPERPAGE;
        entry->volInfo.dataVol.freeExtent = volInfoPage->dataVol.freeExtent;
        entry->volInfo.dataVol.numOfFreeExtent = volInfoPage->dataVol.numOfFreeExtent;
        entry->volInfo.dataVol.freePageExtent = volInfoPage->dataVol.freePageExtent;
        entry->volInfo.dataVol.numOfFreePageExtent = volInfoPage->dataVol.numOfFreePageExtent;
        entry->volInfo.dataVol.freeTrainExtent = volInfoPage->dataVol.freeTrainExtent;
        entry->volInfo.dataVol.numOfFreeTrainExtent = volInfoPage->dataVol.numOfFreeTrainExtent;

    }

    /*
     *  Allocate memory for devInfo array from heap
     */

    /* assertion check */
    assert(PHYSICAL_PTR(entry->volInfo.devInfo) == NULL); 
    assert(PHYSICAL_PTR(entry->volInfo.dataVol.devInfo) == NULL); 

    /* allocate memory for 'entry->volInfo.devInfo' array */
    e = Util_getArrayFromHeap(handle, &RDSM_DEVINFOTABLEHEAP, numDevices, &devInfo); 
    if (e < eNOERROR) ERRL1(handle, e, &RDSM_LATCH_VOLTABLE);
    entry->volInfo.devInfo = LOGICAL_PTR(devInfo); 

    /* allocate memory for 'entry->volInfo.dataVol.devInfo' array if needed */
    if (entry->volInfo.type == VOLUME_TYPE_DATA) {
        e = Util_getArrayFromHeap(handle, &RDSM_DEVINFOFORDATAVOLTABLEHEAP, numDevices, &devInfoForDataVol); 
        if (e < eNOERROR) ERRL1(handle, e, &RDSM_LATCH_VOLTABLE);
        entry->volInfo.dataVol.devInfo = LOGICAL_PTR(devInfoForDataVol); 
    }


    /*
     *  Set remain part of variables in the volInfo from 'masterPage' of each device
     */

    /* initialize variables if needed */
    entry->volInfo.numExts = 0;

    /* it needs because master page size is not equal to PAGESIZE */
    masterPage = &aPage.ms;

    /* for each devices in mounted volume */
    for (i = 0; i < numDevices; i++) {

        /* read master page of the device */
        e = rdsm_ReadTrain(handle, fd[i], 0, masterPage, PAGESIZE2);
        if (e < eNOERROR) ERRL1(handle, e, &RDSM_LATCH_VOLTABLE);

        /* update 'numExts' */
        entry->volInfo.numExts += masterPage->numExtsInDevice;

        /* set 'devInfo' */
        strcpy(devInfo[i].devName, devNames[i]); 
        devInfo[i].numExtsInDevice = masterPage->numExtsInDevice; 

        if (entry->volInfo.type == VOLUME_TYPE_DATA) {

            /* set information about bitmap page */
            devInfoForDataVol[i].bitmapTrainId = masterPage->dataVol.bitmapTrainId;
            devInfoForDataVol[i].bitmapSize = masterPage->dataVol.bitmapSize;

            /* set information about unique number page */
            devInfoForDataVol[i].uniqNumPid = masterPage->dataVol.uniqNumPid;
            devInfoForDataVol[i].uniqPartitionSize = masterPage->dataVol.uniqPartitionSize;
            devInfoForDataVol[i].numUniqNumEntries = masterPage->dataVol.numUniqNumEntries;

            /* set information about extentmap page */
            devInfoForDataVol[i].extentMapPageId = masterPage->dataVol.extentMapPageId;
            devInfoForDataVol[i].extentMapPageSize = masterPage->dataVol.extentMapPageSize;
            devInfoForDataVol[i].firstExtentInDevice = masterPage->dataVol.firstExtentInDevice;

        } /* end if */

    } /* end for i */


    /*
     * Set the mount count to 1.
     */
    entry->nMounts = 1;


    /*
     * fill the user volume table entry.
     */
    RDSM_USERVOLTABLE(handle)[entryNo].volNo = entry->volInfo.volNo;
    RDSM_USERVOLTABLE(handle)[entryNo].numDevices = entry->volInfo.numDevices;
    for (i = 0; i < numDevices; i++) {

        /* doubling openFileDesc array if needed */
        if(i >= RDSM_USERVOLTABLE(handle)[entryNo].openFileDesc.nEntries) {
            e = Util_doublesizeVarArray(handle, &RDSM_USERVOLTABLE(handle)[entryNo].openFileDesc, sizeof(FileDesc));
            if (e < eNOERROR) ERRL1(handle, e, &RDSM_LATCH_VOLTABLE);
        }

        /* insert file descripter into openFileDesc array */
        OPENFILEDESC_ARRAY(RDSM_USERVOLTABLE(handle)[entryNo].openFileDesc)[i] = fd[i];
    }


    /*
     *	returned volume number
     */
    *volId = entry->volInfo.volNo;


    /*
     * Write log record.
     */
    if (logFlag) {
        char buf[LOG_MAX_IMAGE_SIZE];
        Four devNameLen, len;

        mountedVol.volNo = *volId; /* currently no meaning */
        mountedVol.nDevices = numDevices;

        for (i = 0, len = 0; i < numDevices; i++) {
            devNameLen = strlen(devNames[i])+1;

            if (len+devNameLen > sizeof(buf)) ERRL1(handle, eBADPARAMETER, &RDSM_LATCH_VOLTABLE);

            sprintf(&buf[len], "%s", devNames[i]);
            len += devNameLen;
            buf[len-1] = ';'; 
        }
        buf[len-1] = '\0';

        LOG_FILL_LOGRECINFO_2(logRecInfo, common_perThreadDSptr->nilXactId, LOG_TYPE_VOLUME,
                              LOG_ACTION_VOL_MOUNT_VOLUME, LOG_REDO_ONLY,
                              common_perThreadDSptr->nilPid, common_perThreadDSptr->nilLsn, common_perThreadDSptr->nilLsn,
                              sizeof(LOG_Image_RDsM_MountedVol_T), &mountedVol,
                              len, buf);

        e = LOG_WriteLogRecord(handle, NULL, &logRecInfo, &lsn, &logRecLen);
        if (e < eNOERROR) ERRL1(handle, e, &RDSM_LATCH_VOLTABLE);
    }


    /*
     * Mutex End: for controlling mount operation with other mount operations
     */
    e = SHM_releaseLatch(handle, &RDSM_LATCH_VOLTABLE, procIndex);
    if (e < eNOERROR) ERR(handle, e);


    /*
     * Free memory for local variable
     */
    free(fd);
    free(numExtsInPageSegment);
    free(numExtsInTrainSegment);


    return(eNOERROR);

} /* RDsM_Mount() */



Four rdsm_CheckDevices(
    Four                        handle,                 /* IN    handle */
    Four         		numDevices,            	/* IN  number of devices */
    char         		**devNames,            	/* IN  array of the device's name to be mount */
    PageID       		*volInfoPid)           	/* OUT ID of volume information page */
{
    Four         		e;                     	/* error code */
    Four         		i;                     	/* loop variable */
    FileDesc     		fd;                    	/* open file descriptor for each device */
    RDsM_Page_T  		aPage;                 	/* a RDsM Page */
    VolInfoPage_T 		*volInfoPagePtr;      	/* pointer of volume information page */
    MasterPage_T 		*masterPagePtr;       	/* pointer of device master page */
    MasterPage_T 		masterPage;            	/* device master page */
    Boolean	 		flag;                  	/* flag where volume number is already used */
    Four         		entryNo;               	/* entry no of volume table entry corresponding to the given volume */
    rdsm_VolTableEntry_T 	*entry;        		/* volume table entry corresponding to the given volume */
    RDsM_VolumeInfo_T    	*volInfo;      		/* volume information in volume table entry */
    RDsM_DevInfo         	*devInfo;      		/* device information in volume information */
    Buffer_ACC_CB               *volInfoPage_BCBP;      /* BCBP of volume info page */



    /*
     * check first device & numDevices
     */

    /* open first device */
#ifndef WIN32

#ifndef _LARGEFILE64_SOURCE 
    if ((fd = open(devNames[0], O_RDWR | O_SYNC)) == -1) ERR(handle, eDEVICEOPENFAIL_RDSM); 
#else
    if ((fd = open64(devNames[0], O_RDWR | O_SYNC)) == -1) ERR(handle, eDEVICEOPENFAIL_RDSM); 
#endif

#else
    if ((fd = CreateFile(devNames[0], GENERIC_WRITE | GENERIC_READ, FILE_SHARE_READ | FILE_SHARE_WRITE, NULL,
			 OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL | FILE_FLAG_NO_BUFFERING, NULL)) == INVALID_HANDLE_VALUE) ERR(handle, eDEVICEOPENFAIL_RDSM);
#endif /* WIN32 */

    /* initialize the pointer */
    /* Note!! it needs because master page size is not equal to PAGESIZE */
    masterPagePtr = &aPage.ms;

    /* get master page of first device */
    e = rdsm_ReadTrain(handle, fd, 0, masterPagePtr, PAGESIZE2);
    if (e < eNOERROR) ERR(handle, eREADFAIL_RDSM);

    /* check first device is valid */
    if(strcmp(masterPagePtr->tag, DEVICE_TAG) != 0 || masterPagePtr->devNo != 0) ERR(handle, eBADPARAMETER);

    /* Get 'masterPage' */
    masterPage = *masterPagePtr;

    /* initialize the pointer */
    /* Note!! it needs because master page size is not equal to PAGESIZE */
    volInfoPagePtr = &aPage.vi;

    /* get volume information page */
    /* Note!! volume information page always located in first device */
    e = rdsm_ReadTrain(handle, fd, masterPage.volInfoPageId.pageNo, volInfoPagePtr, PAGESIZE2);
    if (e < eNOERROR) ERR(handle, eREADFAIL_RDSM);

    /* check numDevices is valid */
    if(volInfoPagePtr->numDevices != numDevices || volInfoPagePtr->volNo != masterPage.volNo) ERR(handle, eBADPARAMETER);


    /* close first device */
#ifndef WIN32
    if (close(fd) == -1) ERR(handle, eDEVICECLOSEFAIL_RDSM);
#else
    if (CloseHandle(fd) == 0) ERR(handle, eDEVICECLOSEFAIL_RDSM);
#endif /* WIN32 */

    /*
     * check devices except for first device
     */

    /* for each device of volume */
    for (i = 1; i < numDevices; i++) {

        /* open the device */
#ifndef WIN32

#ifndef _LARGEFILE64_SOURCE 
	if ((fd = open(devNames[i], O_RDWR | O_SYNC)) == -1) ERR(handle, eDEVICEOPENFAIL_RDSM); 
#else
	if ((fd = open64(devNames[i], O_RDWR | O_SYNC)) == -1) ERR(handle, eDEVICEOPENFAIL_RDSM); 
#endif

#else
	if ((fd = CreateFile(devNames[i], GENERIC_WRITE | GENERIC_READ, FILE_SHARE_READ | FILE_SHARE_WRITE, NULL,
			     OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL | FILE_FLAG_NO_BUFFERING, NULL)) == INVALID_HANDLE_VALUE) ERR(handle, eDEVICEOPENFAIL_RDSM);
#endif /* WIN32 */

        /* get master page of the device */
        e = rdsm_ReadTrain(handle, fd, 0, masterPagePtr, PAGESIZE2);
        if (e < eNOERROR) ERR(handle, eREADFAIL_RDSM);

        /* close the device */
#ifndef WIN32
        if (close(fd) == -1) ERR(handle, eDEVICECLOSEFAIL_RDSM);
#else
        if (CloseHandle(fd) == 0) ERR(handle, eDEVICECLOSEFAIL_RDSM);
#endif /* WIN32 */

        /* check i'th device is valid */
        if(strcmp(masterPagePtr->tag, DEVICE_TAG) != 0 ||
           masterPagePtr->volNo != masterPage.volNo ||
           !EQUAL_PAGEID(masterPagePtr->volInfoPageId, masterPage.volInfoPageId) ||
           masterPagePtr->devNo != i)   ERR(handle, eBADPARAMETER);
    }

    /* Check whether the volume number is already used */
    flag = FALSE;

    for (entryNo = 0, entry = &RDSM_VOLTABLE[0]; entryNo < MAXNUMOFVOLS; entryNo++, entry++) {

        /* Mutex Begin : Volume Table Entry  */
        e = SHM_getLatch(handle, &entry->latch, procIndex, M_EXCLUSIVE, M_UNCONDITIONAL, NULL);
        if (e < eNOERROR) ERR(handle, e);

        /* points to the volume information for the fast access */
        volInfo = &entry->volInfo;
        devInfo = PHYSICAL_PTR(volInfo->devInfo);

        if (devInfo != NULL && entry->volInfo.volNo != NOVOL &&
            strcmp(devInfo[0].devName, devNames[0]) != 0 && volInfoPagePtr->volNo == entry->volInfo.volNo)
            flag = TRUE;

        /* Mutex End : Volume Table Entry */
        e = SHM_releaseLatch(handle, &entry->latch, procIndex);
        if (e < eNOERROR) ERR(handle, e);

        if (flag == TRUE) ERR(handle, eBADPARAMETER);
    }


    /*
     * Get return variable
     */
    *volInfoPid = masterPage.volInfoPageId;


    return eNOERROR;
}


Four RDsM_MountWithDeviceListString(
    Four handle,                /* IN    handle */
    char *devList,              /* IN device list string */
    Four *volId,                /* OUT volume number */
    Boolean logFlag)            /* IN  indicates whether logging is performed */
{
    Four nDevices;
    Four len;
    char *ptr, *ptr_tmp, *ptr_tmp1, *ptr_tmp2; 
    char **arrayOfPtrToDevice;
    Four i;

    len = strlen(devList) + 1;

    if ((ptr = malloc(len)) == NULL) ERR(handle, eMEMORYALLOCERR);

    strcpy(ptr, devList);

    /* delete the trailing colons */
    for (i = len-2; i >= 0; i--)
#ifndef WIN32
	if (ptr[i] != ';' && ptr[i] != ':') break; 
#else
        if (ptr[i] != ';') break; 
#endif
    ptr[i+1] = '\0';

    if (*ptr == '\0') ERR(handle, eBADPARAMETER);

    /* get the # of devices */
    for (nDevices = 1, i = 0; ptr[i]; i++)
#ifndef WIN32
        if (ptr[i] == ';' || ptr[i] == ':') nDevices++;	
#else
        if (ptr[i] == ';') nDevices++; 
#endif

    if ((ptr_tmp = realloc(ptr, len + nDevices*sizeof(char*))) == NULL) {
        free(ptr);
        ERR(handle, eMEMORYALLOCERR);
    }

    arrayOfPtrToDevice = (char**)ptr_tmp;
    ptr = ptr_tmp + nDevices*sizeof(char*);
    memmove(ptr, ptr_tmp, len);

    /* set the device list */
    for (i = 0; i < nDevices; i++) {
        arrayOfPtrToDevice[i] = ptr;
#ifndef WIN32
        ptr_tmp1 = strchr(ptr, ';');
        ptr_tmp2 = strchr(ptr, ':');
	if (ptr_tmp1 != NULL && ptr_tmp2 != NULL) ptr_tmp = MIN(ptr_tmp1, ptr_tmp2);
	    else ptr_tmp = MAX(ptr_tmp1, ptr_tmp2);
#else
        ptr_tmp = strchr(ptr, ';');	
#endif
        if (ptr_tmp != NULL) {
            *ptr_tmp = '\0';
            ptr = ptr_tmp + 1;
        }
    }

    return(RDsM_Mount(handle, nDevices, arrayOfPtrToDevice, volId, logFlag));

} /* RDsM_MountWithDeviceListString() */
