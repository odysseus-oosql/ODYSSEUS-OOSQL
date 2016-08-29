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
 * Module: RDsM_ExpandVolume.c
 *
 * Description:
 *  Format the given device as a new volume.
 *
 * Exports:
 *  Four RDsM_ExpandVolume(Four, Four, char**, Four*)
 */


#ifndef WIN32
#include <unistd.h>
#else
#include <windows.h>
#endif

#include <stdlib.h>   /* for malloc & free */
#include <string.h>
#include <assert.h>
#include "common.h"
#include "error.h"
#include "trace.h"
#include "Util.h"
#include "BfM.h"
#include "RDsM_Internal.h"
#include "perThreadDS.h"
#include "perProcessDS.h"


extern CfgParams_T sm_cfgParams;


/*
 * Function: Four RDsM_ExpandVolume(Four, Four, char**, Four*)
 *
 * Description:
 *   Expand a size of exist volume
 *
 * Returns:
 *  Error code
 */
Four RDsM_ExpandVolume(
    Four handle,
    Four             volNo,                   /* IN volume number which will be expanded */
    Four             numAddDevices,           /* IN number of added devices */
    char             **addDevNames,           /* IN array of device name */
    Four             *numPagesInAddDevice)    /* IN # of pages in each added device */
{
    Four             e;                       /* error code */
    Four             i;                       /* loop index */
    Four             j;                       /* loop index */
    Four             oldNumDevices;           /* # of devices before expansion */
    Four             oldFirstFreeExt;         /* first extent of free extents list before expansion */
    Four             firstExtNo;              /* first extent number of each device */
    Four             firstPageNo;             /* first page number of each device */
    Four             numOfExtsInDevice;       /* # of extents in each device */
    Four             curExtNoInDevice;        /* extent number in each device */
    ExtEntry         *extEntry;               /* pointer to extent entry */
    PageType         aPage;                   /* a RDsM page */
    VolInfoPage      *volInfoPage;            /* pointer to volume information page */
    DevMasterPage    *masterPage;             /* pointer to device master page */
    DevInfo          *devInfoEntry;           /* entry of devInfo array in volume table entry */
    VolumeTable      *v;                      /* pointer to a volume table entry */
    Four             totalAddedNumSysExts;    /* # of extents which is used for system information of added devices */
    Four             *numAddedSysPages;
    Four             *numAddedSysExts;
    Four             *firstAddedFreeExtNo;
    Two              vIndex;
    Four             dIndex;
    Four             index;
    Two              eIndex;
    Two              uIndex;
    Four             nSysPages;                /* # of system pages to clear in a Bitmap Page*/
    Four             nRemainSysPages;          /* # of system pages to clear in BitMap */


    TR_PRINT(TR_RDSM, TR1, ("RDsM_ExpandVolume(handle, volNo=%lD, numAddDevices=%lD, addDevNames=%P, numPagesInAddDevice=%P)",
                             volNo, numAddDevices, addDevNames, numPagesInAddDevice));

    /*
     *  O. Parameter check
     */
    if (numAddDevices <= 0) ERR(handle, eBADPARAMETER);
    if (addDevNames == NULL || numPagesInAddDevice == NULL) ERR(handle, eBADPARAMETER);

    /*
     *  I. Allocate memory for local variables
     */
    numAddedSysPages = (Four *) malloc(sizeof(Four)*numAddDevices);
    numAddedSysExts = (Four *) malloc(sizeof(Four)*numAddDevices);
    firstAddedFreeExtNo = (Four *) malloc(sizeof(Four)*numAddDevices);

    /*
     *  II. Check if given devices are already used
     */
    for (vIndex = 0; vIndex < MAXNUMOFVOLS; vIndex++)                  /* for each mounted volume */
        for (dIndex = 0; dIndex < RDSM_PER_THREAD_DS(handle).volTable[vIndex].numDevices; dIndex++)    /* for each device in the i'th volume */ 
            for (index = 0; index < numAddDevices; index++)         /* for each given devices */
                if (!strcmp(DEVINFO_ARRAY(RDSM_PER_THREAD_DS(handle).volTable[vIndex].devInfo)[dIndex].devName, addDevNames[index]))
                    ERR(handle, eUSEDDEVICE_RDSM);

    /*
     *  III. Get volume table entry in which expanded volume's information is located
     */

    /* check whether the volume is already mounted */
    for (vIndex = 0; vIndex < MAXNUMOFVOLS; vIndex++)
        if (RDSM_PER_THREAD_DS(handle).volTable[vIndex].volNo == volNo) break; 

    /* error check */
    if (vIndex >= MAXNUMOFVOLS) ERR(handle, eVOLNOTMOUNTED_RDSM);

    /* for fast access */
    v = &(RDSM_PER_THREAD_DS(handle).volTable[vIndex]); 


    /*
     *  IV. Save old value of 'numDevices' & 'firstFreeExt'
     */

    /* if value is invalid, read from disk */
    if (v->numOfFreeExts == NOT_ASSIGNED || v->firstFreeExt == NOT_ASSIGNED) {

        /* read volume information page */
        e = BfM_GetTrain(handle, &v->volInfoPageId, (char **)&volInfoPage, PAGE_BUF);
        if (e < eNOERROR) ERR(handle, e);

        /* set numOfFreeExts & firstFreeExt */
        v->numOfFreeExts = volInfoPage->numOfFreeExts;
        v->firstFreeExt = volInfoPage->firstFreeExt;

	/* free from buffer */
	e = BfM_FreeTrain(handle, &v->volInfoPageId, PAGE_BUF);
	if (e < eNOERROR) ERR(handle, e);
    }

    /* save old values */
    oldNumDevices = v->numDevices;
    oldFirstFreeExt = v->firstFreeExt;


    /*
     *  V. Update 'numDevices' in volume table
     */
    v->numDevices += numAddDevices;


    /*
     *  VI. For each added device, insert information about that device into entry of volume table
     */
    for (i = oldNumDevices, firstExtNo = v->numOfExts, totalAddedNumSysExts = 0; i < v->numDevices; i++) {


        /*
         *  Check devInfo array need to be doubled
         */
        if(i >= v->devInfo.nEntries) {
            e = Util_doublesizeVarArray(handle, &v->devInfo, sizeof(DevInfo));
            if (e < 0) ERR(handle, e);
        }


        /*
         *  Set devInfoEntry for fast access
         */
        devInfoEntry = &DEVINFO_ARRAY(v->devInfo)[i];


        /*
         *  Calculate 'numExtsInDevice'
         */
        numOfExtsInDevice = numPagesInAddDevice[i-oldNumDevices] / v->sizeOfExt;


        /*
         *  Set each variable of devInfo array's entry
         */

        /* set 'devName' */
        strcpy(devInfoEntry->devName, addDevNames[i-oldNumDevices]);

        /* set 'firstExtNo' */
        devInfoEntry->firstExtNo = firstExtNo;

        /* initialize information about bitmap */
        devInfoEntry->bitMapPageId.volNo = volNo;
        devInfoEntry->bitMapPageId.pageNo = firstExtNo*v->sizeOfExt + MASTERPAGESIZE;
        devInfoEntry->bitMapSize = (numOfExtsInDevice-1)/((BITMAP_USABLEBYTESPERPAGE*BITSPERBYTE)/v->sizeOfExt) + 1;

        /* initialize information about extent array */
        devInfoEntry->extEntryArrayPageId.volNo = volNo;
        devInfoEntry->extEntryArrayPageId.pageNo = devInfoEntry->bitMapPageId.pageNo + devInfoEntry->bitMapSize;
        devInfoEntry->extEntryArraySize = (numOfExtsInDevice-1)/EXTENTRYPERPAGE + 1;

        /* initialize information about unique number page */
        devInfoEntry->uniqNumPageId.volNo = volNo;
        devInfoEntry->uniqNumPageId.pageNo = devInfoEntry->extEntryArrayPageId.pageNo + devInfoEntry->extEntryArraySize;
        devInfoEntry->uniqNumSize = ((numOfExtsInDevice*v->sizeOfExt-1)/UNIQUEPARTITIONSIZE)/NUMUNIQUESPERPAGE + 1;


        /*
         *  Open added device & Write dummy contents into the last page in added device
         */

        /* open added device & set 'devAddr' */
#ifndef WIN32

#ifndef _LARGEFILE64_SOURCE 
        if ((devInfoEntry->devAddr = open(devInfoEntry->devName, O_RDWR | O_SYNC | O_CREAT, PERM)) == -1) ERR(handle, eDEVICEOPENFAIL_RDSM);
#else
        if ((devInfoEntry->devAddr = open64(devInfoEntry->devName, O_RDWR | O_SYNC | O_CREAT, PERM)) == -1) ERR(handle, eDEVICEOPENFAIL_RDSM);
#endif

#else
        if (sm_cfgParams.coherencyVolumeDevice != NULL && strcmp(sm_cfgParams.coherencyVolumeDevice, devInfoEntry->devName) == 0) {
	    if ((devInfoEntry->devAddr = CreateFile(devInfoEntry->devName, GENERIC_WRITE | GENERIC_READ, 
						    FILE_SHARE_READ | FILE_SHARE_WRITE, NULL, OPEN_ALWAYS, 
						    FILE_ATTRIBUTE_NORMAL, NULL)) == INVALID_HANDLE_VALUE)
                ERR(handle, eDEVICEOPENFAIL_RDSM);
        }
	else { /* it is not a coherency volume */
	    if ((devInfoEntry->devAddr = CreateFile(devInfoEntry->devName, GENERIC_WRITE | GENERIC_READ, 
						    FILE_SHARE_READ | FILE_SHARE_WRITE, NULL, OPEN_ALWAYS, 
						    FILE_ATTRIBUTE_NORMAL | FILE_FLAG_NO_BUFFERING, NULL)) == INVALID_HANDLE_VALUE)
                ERR(handle, eDEVICEOPENFAIL_RDSM);
	}
#endif

        /*  this operation meaningful only in the case of using a unix file as a volume */
        e = rdsm_WriteTrain(handle, devInfoEntry->devAddr, numOfExtsInDevice*v->sizeOfExt-1, &aPage, PAGESIZE2);
        if (e < eNOERROR) ERR(handle, e);


        /*
         *  Set 'numAddedSysExts[]' & 'firstAddedFreeExtNo[]' and calculate 'totalNumSysEx
         */

        /* calculate 'numAddedSysPages' */
        numAddedSysPages[i-oldNumDevices] = devInfoEntry->uniqNumPageId.pageNo + devInfoEntry->uniqNumSize - firstExtNo*v->sizeOfExt;

        /* set 'numAddedSysExtsInDevice[]' */
        numAddedSysExts[i-oldNumDevices] = (numAddedSysPages[i-oldNumDevices] - 1)/v->sizeOfExt + 1;

        /* set 'firstAddedFreeExtNo[]' */
        firstAddedFreeExtNo[i-oldNumDevices] = firstExtNo + numAddedSysExts[i-oldNumDevices];

        /* update 'totalAddedNumSysExts' */
        totalAddedNumSysExts += numAddedSysExts[i-oldNumDevices];

        /*
         *  Update 'firstExtNo' in volume table
         */
        firstExtNo += numOfExtsInDevice;
    }


    /*
     *  VII. Update part of variables in volume table
     */
    v->numOfFreeExts += (firstExtNo - v->numOfExts) - totalAddedNumSysExts;
    v->firstFreeExt = firstAddedFreeExtNo[0];
    v->numOfExts = firstExtNo;


    /*
     *  VIII. Initialize bitmap pages, extEntryArray, and unique number pages in the each device
     */

    /* for each device */
    for (i = oldNumDevices; i < v->numDevices; i++) {

        /*
         *  Set 'numOfExtsInDevice', 'devInfoEntry' and 'firstPageNo' for fast access
         */

        /* calculate # of extents in i'th device */
        numOfExtsInDevice = numPagesInAddDevice[i-oldNumDevices] / v->sizeOfExt;

        /* set 'devInfoEntry' which points information about i'th device */
        devInfoEntry = &DEVINFO_ARRAY(v->devInfo)[i];

        /* set first page's number of i'th device */
        firstPageNo = devInfoEntry->firstExtNo * v->sizeOfExt;


        /*
         *  Initialize bitmap pages
         */

        /* initialize header except for pid */
        SET_PAGE_TYPE(&aPage.ch, BITMAP_PAGE_TYPE);
        RESET_TEMP_PAGE_FLAG(&aPage.ch);

        /* initialize bitmap */
        RDsM_set_bits(handle, &aPage, 0, BITMAP_USABLEBYTESPERPAGE*BITSPERBYTE);

        /*  initialize the bitmap page except for first bitmap page */
        for (j = PAGESIZE2; j < devInfoEntry->bitMapSize; j += PAGESIZE2) {

            /* set ID of each bitmap page's header */
            aPage.ch.hdr.pid.volNo = volNo;
            aPage.ch.hdr.pid.pageNo = devInfoEntry->bitMapPageId.pageNo + j;

            /* Write this page into the disk */
            e = rdsm_WriteTrain(handle, devInfoEntry->devAddr, aPage.ch.hdr.pid.pageNo - firstPageNo, &aPage.ch, PAGESIZE2);
            if (e < eNOERROR) ERR(handle, e);
        }

        /*
         * Because previous code cannot support the large database (>8T), we modify the previous code
         */
        for (j = 0, nRemainSysPages = numAddedSysPages[i-oldNumDevices]; nRemainSysPages > 0; j += PAGESIZE2) {

            /* calculate # of pages for the system information to clear in a Bitmap Page */
            nSysPages = MIN(BITMAP_USABLEBYTESPERPAGE*BITSPERBYTE, nRemainSysPages);

            /* initialize bitmap */
            RDsM_set_bits(handle, &aPage, 0, BITMAP_USABLEBYTESPERPAGE*BITSPERBYTE);

            /* allocate pages for the system information */
            RDsM_clear_bits(handle, &aPage, 0, nSysPages);

            /* set ID of first bitmap page */
            aPage.ch.hdr.pid.pageNo = devInfoEntry->bitMapPageId.pageNo + j;
            aPage.ch.hdr.pid.volNo  = devInfoEntry->bitMapPageId.volNo;

            /* Write first bitmap page */
            e = rdsm_WriteTrain(handle, devInfoEntry->devAddr, aPage.ch.hdr.pid.pageNo - firstPageNo, &aPage.ch, PAGESIZE2);
            if (e < eNOERROR) ERR(handle, e);

            /* calculate # of remain pages for the system information to clear in BitMap */
            nRemainSysPages -= nSysPages;
        }


        /*
         *  Initialize extent entry array
         */

        /* initialize header except for pid */
        SET_PAGE_TYPE(&aPage.el, EXT_ENTRY_PAGE_TYPE);
        RESET_TEMP_PAGE_FLAG(&aPage.el);

        /* for each extent entry array page */
        for (j = 0; j < devInfoEntry->extEntryArraySize; j += PAGESIZE2) {

            /* set ID of each bitmap page's header */
            aPage.el.hdr.pid.volNo = volNo;
            aPage.el.hdr.pid.pageNo = devInfoEntry->extEntryArrayPageId.pageNo + j;

            /* initialize 'curExtNoInDevice' in extent array */
            curExtNoInDevice = j * EXTENTRYPERPAGE;

            /* for each extent entry */
            for (eIndex = 0, extEntry = aPage.el.el; eIndex < EXTENTRYPERPAGE; eIndex++, extEntry++) {

                /* set extent fill factor and prevExt to NIL */
                extEntry->eff = NIL;
                extEntry->prevExt = NIL;

                /* if extent is system extents, exclude from free extent list */
                if (curExtNoInDevice < numAddedSysExts[i-oldNumDevices]) {
                    extEntry->nextExt = NIL;
                }
                /* if extent is normal extents, insert into first part of free extent list */
                else {
                    if (curExtNoInDevice < numOfExtsInDevice - 1) {
                        extEntry->nextExt = devInfoEntry->firstExtNo + curExtNoInDevice + 1;
                    }
                    else {
                        /* Note!! free extents in added device are insert into first free extent list */
                        extEntry->nextExt = (i < v->numDevices-1) ? firstAddedFreeExtNo[i-oldNumDevices+1] : oldFirstFreeExt;
                        assert (j == devInfoEntry->extEntryArraySize-1);
                        break;
                    }
                }

                /* update 'curExtNoInDevice' */
                curExtNoInDevice++;

            } /* end for eIndex */

            /* Write this page into the disk */
            e = rdsm_WriteTrain(handle, devInfoEntry->devAddr, aPage.el.hdr.pid.pageNo - firstPageNo, &aPage.el, PAGESIZE2);
            if (e < eNOERROR) ERR(handle, e);

        } /* end for j */


        /*
         *  Initialize unique number pages
         */

        /* initialize header except for pid */
        SET_PAGE_TYPE(&aPage.un, UNIQUE_NUM_PAGE_TYPE);
        RESET_TEMP_PAGE_FLAG(&aPage.un);

        /* initialize unique number in unique number page */
        for (uIndex = 0; uIndex < NUMUNIQUESPERPAGE; uIndex++) aPage.un.unique[uIndex] = 0;

        /* for each unique number pages */
        for (j = 0; j < devInfoEntry->uniqNumSize; j += PAGESIZE2) {

            /* set ID of each bitmap page's header */
            aPage.un.hdr.pid.volNo = volNo;
            aPage.un.hdr.pid.pageNo = devInfoEntry->uniqNumPageId.pageNo + j;

            /* Write this page into the disk */
            e = rdsm_WriteTrain(handle, devInfoEntry->devAddr, aPage.un.hdr.pid.pageNo - firstPageNo, &aPage.un, PAGESIZE2);
            if (e < eNOERROR) ERR(handle, e);

        } /* end for j */

    } /* end for i */


    /*
     *  VI. Write Master page into each added device
     */

    masterPage = &aPage.ms;

    /* set header of master page except for pid */
    SET_PAGE_TYPE(masterPage, MASTER_PAGE_TYPE);
    RESET_TEMP_PAGE_FLAG(masterPage);

    /* write for each devices */
    for (i = oldNumDevices; i < v->numDevices; i++) {

        /* set 'devInfoEntry' which points information about i'th device */
        devInfoEntry = &DEVINFO_ARRAY(v->devInfo)[i];

        /* set 'pid' of header */
        /* Note!! master page is first page of each added page segments */
        MAKE_PAGEID(masterPage->hdr.pid, volNo, devInfoEntry->firstExtNo*v->sizeOfExt);

        /* set contents of master page */
        strcpy(masterPage->tag, DEVICE_TAG);
        masterPage->volNo = volNo;
        masterPage->devNo = i;
        masterPage->numOfExtsInDevice = numPagesInAddDevice[i-oldNumDevices] / v->sizeOfExt;
        masterPage->volInfoSize = VOLINFOSIZE;
        masterPage->volInfoPageId = v->volInfoPageId;
        masterPage->bitMapSize = devInfoEntry->bitMapSize;
        masterPage->bitMapPageId = devInfoEntry->bitMapPageId;
        masterPage->extEntryArraySize = devInfoEntry->extEntryArraySize;
        masterPage->extEntryArrayPageId = devInfoEntry->extEntryArrayPageId;
        masterPage->uniqNumSize = devInfoEntry->uniqNumSize;
        masterPage->uniqNumPageId = devInfoEntry->uniqNumPageId;

        /* write master page into first of the device */
        e = rdsm_WriteTrain(handle, devInfoEntry->devAddr, 0, masterPage, PAGESIZE2);
        if (e < eNOERROR) ERR(handle, e);
    }


    /*
     *  VII. Update volume information page because 'numDevices', 'numOfFreeExts' & 'firstFreeExt' are altered
     *       Note!! This operation must be done at final step in expansion function because common part is altered
     */

    /* read exist volume information page */
    e = BfM_GetTrain(handle, &v->volInfoPageId, (char **)&volInfoPage, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);

    /* update contents */
    volInfoPage->numDevices = v->numDevices;
    volInfoPage->numOfFreeExts = v->numOfFreeExts;
    volInfoPage->firstFreeExt = v->firstFreeExt;

    /* set dirty bit */
    e = BfM_SetDirty(handle, &v->volInfoPageId, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);

    /* free from buffer */
    e = BfM_FreeTrain(handle, &v->volInfoPageId, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);


    /*
     *  VIII. Free memory for local variables
     */
    free(numAddedSysPages);
    free(numAddedSysExts);
    free(firstAddedFreeExtNo);


    return(eNOERROR);

} /* RDsM_ExpandVolume() */
