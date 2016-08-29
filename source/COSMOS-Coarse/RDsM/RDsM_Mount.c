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
 * Module: RDsM_Mount.c
 *
 * Description:
 *
 *
 * Exports:
 *  Four RDsM_Mount(Four, char**, Four*)
 */


#ifndef WIN32
#include <unistd.h>
#else
#include <windows.h>
#endif

#include <string.h>
#include "common.h"
#include "error.h"
#include "trace.h"
#include "Util.h"
#include "RDsM_Internal.h"
#include "perThreadDS.h"
#include "perProcessDS.h"


extern CfgParams_T sm_cfgParams;


Four rdsm_CheckDevices(Four, Four, char**, PageID*, Two*);


/*@================================
 * RDsM_Mount()
 *================================*/
/*
 * Function: Four RDsM_Mount(Four, char**, Four*)
 *
 * Description:
 *  Mount a named device
 *
 * Returns:
 *  error code
 *    eTOOMANYVOLUMES_RDSM - too many volumes mounted
 *    eDEVICEOPENFAIL_RDSM - device open operation failed
 *    eLSEEKFAIL_RDSM - lseek operation failed
 *    eREADFAIL_RDSM - read operation failed
 *
 * Side Effects:
 *  Bring the device on line and Cache device information in the VolInfo[] table.
 */
Four RDsM_Mount(
    Four 	      handle,
    Four          numDevices,           /* IN  number of devices */
    char          **devNames,           /* IN  array of the device's name to be mount */
    Four          *volNo)               /* OUT volume number */
{
    Four          e;                    /* returned error code */
    Two           idx;                  /* loop index */
    Four          i;                    /* loop index */
    Two           volInfoSize;          /* # of pages for volume information */
    Four          firstExtNo;           /* first extent number of each device */
    Four          totalNumExtsInVolume; /* total # of extents in mounted volume */
    VolumeTable	  *v;                   /* pointer to a volume table entry */
    PageType      aPage;                /* page buffer */
    PageID        volInfoPid;           /* ID of volume information page */
    VolInfoPage   *volInfoPage;         /* pointer to volume information page */
    DevMasterPage *masterPage;          /* pointer to device master page */


    TR_PRINT(TR_RDSM, TR1, ("RDsM_Mount(handle, numDevices=%lD, devNames=%P, volNo=%P)", numDevices, devNames, volNo));


    /*
     * Parameter check
     */
    if (numDevices <= 0 || devNames == NULL || volNo == NULL) ERR(handle, eBADPARAMETER);


    /*
     * first check given devices is proper set
     */
    e = rdsm_CheckDevices(handle, numDevices, devNames, &volInfoPid, &volInfoSize);
    if ( e < eNOERROR ) ERR(handle, e);


    /*
     *  Check whether the volume is already mounted
     *  Note!! In this operation, we check only first device of volume
     *         Because we guarantee devNames[0] has first device's name by above checking operation
     */
    for (idx = 0; idx < MAXNUMOFVOLS; idx++) {
	/* modify the way of accessing volTable for multi threading */
        if (RDSM_PER_THREAD_DS(handle).volTable[idx].volNo != NOVOL && 
	    !strcmp(DEVINFO_ARRAY(RDSM_PER_THREAD_DS(handle).volTable[idx].devInfo)[0].devName, devNames[0])) {
	    /* this volume already mounted */
            *volNo = RDSM_PER_THREAD_DS(handle).volTable[idx].volNo;
	    return(eNOERROR);
	} /* end if */
    }


    /*
     *  Look for a volume table entry from the volTable
     */
    for (idx = 0; idx < MAXNUMOFVOLS; idx++) {
        if (RDSM_PER_THREAD_DS(handle).volTable[idx].volNo == NOVOL) break; 
    }
    if (idx >= MAXNUMOFVOLS) ERR(handle, eTOOMANYVOLUMES_RDSM);


    v = &(RDSM_PER_THREAD_DS(handle).volTable[idx]); 

    /*
     *  Set each entry of devInfo array in volume table entry 'v'
     */

    /* initialize 'masterPage' */
    masterPage = &aPage.ms;

    /* for each devices */
    for (i = 0, firstExtNo = 0; i < numDevices; i++ ) {

        /*
         *  Check devInfo array need to be doubled
         */
        if(i >= v->devInfo.nEntries) {
            e = Util_doublesizeVarArray(handle, &v->devInfo, sizeof(DevInfo));
            if (e < 0) ERR(handle, e);
        }

        /*
         *  Open the volume device
         */
#ifndef WIN32

#ifndef _LARGEFILE64_SOURCE 
        if ((DEVINFO_ARRAY(v->devInfo)[i].devAddr = open(devNames[i], O_RDWR | O_SYNC, PERM)) < eNOERROR) ERR(handle, eDEVICEOPENFAIL_RDSM);
#else
        if ((DEVINFO_ARRAY(v->devInfo)[i].devAddr = open64(devNames[i], O_RDWR | O_SYNC, PERM)) < eNOERROR) ERR(handle, eDEVICEOPENFAIL_RDSM);
#endif

#else
        if (sm_cfgParams.coherencyVolumeDevice != NULL && strcmp(sm_cfgParams.coherencyVolumeDevice, devNames[i]) == 0) {
            if ((DEVINFO_ARRAY(v->devInfo)[i].devAddr = CreateFile(devNames[i], GENERIC_WRITE | GENERIC_READ, 
	 						           FILE_SHARE_READ | FILE_SHARE_WRITE, NULL, OPEN_ALWAYS, 
							           FILE_ATTRIBUTE_NORMAL, NULL)) == INVALID_HANDLE_VALUE)
                ERR(handle, eDEVICEOPENFAIL_RDSM);
        }
	else { /* it is not a coherency volume */
            if ((DEVINFO_ARRAY(v->devInfo)[i].devAddr = CreateFile(devNames[i], GENERIC_WRITE | GENERIC_READ, 
	 						           FILE_SHARE_READ | FILE_SHARE_WRITE, NULL, OPEN_ALWAYS, 
							           FILE_ATTRIBUTE_NORMAL | FILE_FLAG_NO_BUFFERING, NULL)) == INVALID_HANDLE_VALUE)
                ERR(handle, eDEVICEOPENFAIL_RDSM);
	}
#endif

        /*
         *  Read master page of i'th device
         */
        e = rdsm_ReadTrain(handle, DEVINFO_ARRAY(v->devInfo)[i].devAddr, 0, masterPage, PAGESIZE2);
        if (e < eNOERROR) ERR(handle, e);

        /*
         *  Set entry of devInfo array in volume table entry
         */
        strcpy(DEVINFO_ARRAY(v->devInfo)[i].devName, devNames[i]);
        DEVINFO_ARRAY(v->devInfo)[i].firstExtNo = firstExtNo;
        DEVINFO_ARRAY(v->devInfo)[i].uniqNumSize = masterPage->uniqNumSize;
        DEVINFO_ARRAY(v->devInfo)[i].bitMapSize = masterPage->bitMapSize;
        DEVINFO_ARRAY(v->devInfo)[i].extEntryArraySize = masterPage->extEntryArraySize;
        DEVINFO_ARRAY(v->devInfo)[i].uniqNumPageId = masterPage->uniqNumPageId;
        DEVINFO_ARRAY(v->devInfo)[i].bitMapPageId = masterPage->bitMapPageId;
        DEVINFO_ARRAY(v->devInfo)[i].extEntryArrayPageId = masterPage->extEntryArrayPageId;

        /*
         * Update 'firstExtNo'
         */
        firstExtNo += masterPage->numOfExtsInDevice;

    } /* end for i */

    /* set 'totalNumExtsInVolume' */
    totalNumExtsInVolume = firstExtNo;


    /*
     *  Read the volume information page
     */

    /* initialize 'volInfoPage' */
    volInfoPage = &aPage.vi;

    /* Note!! volume information page always located in the first device */
    e = rdsm_ReadTrain(handle, DEVINFO_ARRAY(v->devInfo)[0].devAddr, volInfoPid.pageNo, volInfoPage, PAGESIZE2);
    if (e < eNOERROR) ERR(handle, e);


    /*
     *  Set remaining part of variables in the volume table entry from 'volInfoPage'
     */
    strcpy(v->title, volInfoPage->title);
    v->volNo = volInfoPage->volNo;
    v->sizeOfExt = volInfoPage->sizeOfExt;
    v->numOfExts = totalNumExtsInVolume;
    v->numOfFreeExts = NOT_ASSIGNED;
    v->firstFreeExt = NOT_ASSIGNED;
/*
    Note!! numOfFreeExts & firstFreeExt can be updated, so these variables must be read after volume is locked
    v->numOfFreeExts = volInfoPage->numOfFreeExts;
    v->firstFreeExt = volInfoPage->firstFreeExt;
*/
    v->volInfoSize = volInfoSize;
    v->metaDicSize = volInfoPage->metaDicSize;
    v->volInfoPageId = volInfoPid;
    v->metaDicPageId = volInfoPage->metaDicPageId;
    v->numDevices = numDevices;
#ifdef DBLOCK
    v->lockMode = L_NL;           
#endif


    /*
     *  returned volume number
     */
    *volNo = v->volNo;


    return(eNOERROR);

} /* RDsM_Mount() */



Four rdsm_CheckDevices(
    Four handle,
    Four          numDevices,            /* IN  number of devices */
    char          **devNames,            /* IN  array of the device's name to be mount */
    PageID        *volInfoPid,           /* OUT ID of volume information page */
    Two           *volInfoSize)          /* OUT # of pages for volume information */
{
    Four          e;                     /* error code */
    Four          i;                     /* loop variable */
    Two           idx;                   /* loop variable */
    PageType      aPage;                 /* a RDsM Page */
    VolInfoPage   *volInfoPagePtr;       /* pointer of volume information page */
    DevMasterPage *masterPagePtr;        /* pointer of device master page */
    DevMasterPage masterPage;            /* device master page */
    FileDesc      fd;                    /* open file descriptor for each device */


    /*
     * check first device & numDevices
     */

    /* open first device */
#ifndef WIN32

#ifndef _LARGEFILE64_SOURCE 
    if ((fd = open(devNames[0], O_RDWR | O_SYNC, PERM)) == -1) ERR(handle, eDEVICEOPENFAIL_RDSM);
#else
    if ((fd = open64(devNames[0], O_RDWR | O_SYNC, PERM)) == -1) ERR(handle, eDEVICEOPENFAIL_RDSM);
#endif

#else
    if (sm_cfgParams.coherencyVolumeDevice != NULL && strcmp(sm_cfgParams.coherencyVolumeDevice, devNames[0]) == 0) {
        if ((fd = CreateFile(devNames[0], GENERIC_WRITE | GENERIC_READ, FILE_SHARE_READ | FILE_SHARE_WRITE, NULL, OPEN_ALWAYS, 
			     FILE_ATTRIBUTE_NORMAL, NULL)) == INVALID_HANDLE_VALUE) ERR(handle, eDEVICEOPENFAIL_RDSM);
    }
    else { /* it is not a coherency volume */
        if ((fd = CreateFile(devNames[0], GENERIC_WRITE | GENERIC_READ, FILE_SHARE_READ | FILE_SHARE_WRITE, NULL, OPEN_ALWAYS, 
			     FILE_ATTRIBUTE_NORMAL | FILE_FLAG_NO_BUFFERING, NULL)) == INVALID_HANDLE_VALUE) ERR(handle, eDEVICEOPENFAIL_RDSM);
    }
#endif

    /* initialize the pointer */
    masterPagePtr = &aPage.ms;

    /* get master page of first device */
    e = rdsm_ReadTrain(handle, fd, 0, masterPagePtr, PAGESIZE2);
    if (e < eNOERROR) ERR(handle, eREADFAIL_RDSM);

    /* check first device is valid */
    if(strcmp(masterPagePtr->tag, DEVICE_TAG) != 0 || masterPagePtr->devNo != 0) ERR(handle, eBADPARAMETER);

    /* Get 'masterPage' */
    masterPage = *masterPagePtr;


    /* initialize 'volInfoPagePtr' */
    volInfoPagePtr = &aPage.vi;

    /* get volume information page */
    /* Note!! volume information page always located in the first device */
    e = rdsm_ReadTrain(handle, fd, masterPage.volInfoPageId.pageNo, volInfoPagePtr, PAGESIZE2);
    if (e < eNOERROR) ERR(handle, e);

    /* check numDevices is valid */
    if(volInfoPagePtr->numDevices != numDevices || volInfoPagePtr->volNo != masterPage.volNo) ERR(handle, eBADPARAMETER);


    /* close first device */
#ifndef WIN32
    if (close(fd) == -1) ERR(handle, eDEVICECLOSEFAIL_RDSM);
#else
    if (CloseHandle(fd) == 0) ERR(handle, eDEVICECLOSEFAIL_RDSM);
#endif

    /*
     * check devices except for first device
     */

    /* for each device of volume */
    for (i = 1; i < numDevices; i++) {

        /* open the device */
#ifndef WIN32

#ifndef _LARGEFILE64_SOURCE
        if ((fd = open(devNames[i], O_RDWR | O_SYNC, PERM)) == -1) ERR(handle, eDEVICEOPENFAIL_RDSM);
#else
        if ((fd = open64(devNames[i], O_RDWR | O_SYNC, PERM)) == -1) ERR(handle, eDEVICEOPENFAIL_RDSM);
#endif

#else
        if (sm_cfgParams.coherencyVolumeDevice != NULL && strcmp(sm_cfgParams.coherencyVolumeDevice, devNames[i]) == 0) {
            if ((fd = CreateFile(devNames[i], GENERIC_WRITE | GENERIC_READ, FILE_SHARE_READ | FILE_SHARE_WRITE, NULL, OPEN_ALWAYS, 
				 FILE_ATTRIBUTE_NORMAL, NULL)) == INVALID_HANDLE_VALUE) 
	        ERR(handle, eDEVICEOPENFAIL_RDSM);
	}
	else { /* it is not a coherency volume */
            if ((fd = CreateFile(devNames[i], GENERIC_WRITE | GENERIC_READ, FILE_SHARE_READ | FILE_SHARE_WRITE, NULL, OPEN_ALWAYS, 
				 FILE_ATTRIBUTE_NORMAL | FILE_FLAG_NO_BUFFERING, NULL)) == INVALID_HANDLE_VALUE) 
	        ERR(handle, eDEVICEOPENFAIL_RDSM);
	}
#endif

        /* get master page of the device */
        e = rdsm_ReadTrain(handle, fd, 0, masterPagePtr, PAGESIZE2);
        if (e < eNOERROR) ERR(handle, eREADFAIL_RDSM);

        /* close the device */
#ifndef WIN32
        if (close(fd) == -1) ERR(handle, eDEVICECLOSEFAIL_RDSM);
#else
        if (CloseHandle(fd) == 0) ERR(handle, eDEVICECLOSEFAIL_RDSM);
#endif

        /* check i'th device is valid */
        if(strcmp(masterPagePtr->tag, DEVICE_TAG) != 0 ||
           masterPagePtr->volNo != masterPage.volNo ||
           !EQUAL_PAGEID(masterPagePtr->volInfoPageId, masterPage.volInfoPageId) ||
           masterPagePtr->devNo != i)   ERR(handle, eBADPARAMETER);
    }

    /* Check whether the volume number is already used */
    for (idx = 0; idx < MAXNUMOFVOLS; idx++) {
        if (RDSM_PER_THREAD_DS(handle).volTable[idx].volNo != NOVOL &&
            strcmp(DEVINFO_ARRAY(RDSM_PER_THREAD_DS(handle).volTable[idx].devInfo)[0].devName, devNames[0]) != 0 &&
            volInfoPagePtr->volNo == RDSM_PER_THREAD_DS(handle).volTable[idx].volNo)
	    ERR(handle, eBADPARAMETER); /* Another volume already uses this volume number */
    }

    /*
     * Get return variable
     */
    *volInfoPid = masterPage.volInfoPageId;
    *volInfoSize = masterPage.volInfoSize;


    return eNOERROR;
}
