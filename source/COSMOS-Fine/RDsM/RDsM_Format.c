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
 * Module: RDsM_Format.c
 *
 * Description:
 *  Format the given device as a new volume.
 *
 * Exports:
 *  Four RDsM_Format(Four, Four, char*, char*, Four, Four, Four, Four, Four)
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
#include "perProcessDS.h"
#include "perThreadDS.h"



/*
 * internal function prototypes
 */
Four rdsm_FormatDataVolume(Four, FileDesc*, VolInfoPage_T*, MasterPage_T*, Four);


/*
 * Function: Four RDsM_Format(Four, Four, char*, char*, Four, Four, Four, Four, Four)
 *
 * Description:
 *   Format a new volume on the named device.
 *
 * Returns:
 *  Error code
 */
Four RDsM_Format(
    Four          handle,                  /* IN handle */
    Four          numDevices,              /* IN number of devices in formated volume */
    char          **devNames,              /* IN array of device name */
    char          *title,                  /* IN volume title */
    Four          volNo,                   /* IN volume number */
    Four          extSize,                 /* IN number of pages in an extent */
    Four          *numPagesInDevice,       /* IN array of pages' number in each device */
    Four          type,                    /* IN volume type */
    Four          eff                      /* IN extent fill factor: used in data volume */
)
{
    Four          e;                       /* error code */
    Four          i, j, k;                 /* loop index */
    FileDesc*     fd;                      /* file descriptor for new volume */
    RDsM_Page_T   aPage;                   /* a RDsM page */
    VolInfoPage_T volInfoPage;             /* volume information page */
    MasterPage_T* masterPages;             /* master page for each device */
    Four          firstExtentNo;           /* first extent No. in each device */

    /* pointer for COMMON Data Structure of perThreadTable */
    COMMON_PerThreadDS_T *common_perThreadDSptr = COMMON_PER_THREAD_DS_PTR(handle);


    TR_PRINT(handle, TR_RDSM, TR1, ("RDsM_Format(numDevices=%lD, devNames=%P, title=%P, volNo=%lD, \
                             extSize=%lD, numPagesInDevice=%P, type=%lD, eff=%lD)",
                             numDevices, devNames, title, volNo, extSize, numPagesInDevice, type, eff));


    /*
     *  O. Parameter check
     */
    if (extSize%PAGESIZE2 != 0 || extSize%TRAINSIZE2 != 0) ERR(handle, eBADPARAMETER);
    if (numDevices <= 0) ERR(handle, eBADPARAMETER);
    if (devNames == NULL || title == NULL || numPagesInDevice == NULL) ERR(handle, eBADPARAMETER);


    /*
     *  I. Allocate memory for 'fd' & 'masterPages'
     */
    fd = (FileDesc *) malloc(sizeof(FileDesc)*numDevices);
    masterPages = (MasterPage_T *) malloc(sizeof(MasterPage_T)*numDevices);


    /*
     *  II. Mutex Begin : for mount/mount operation
     */
    e = SHM_getLatch(handle, &RDSM_LATCH_VOLTABLE, procIndex, M_SHARED, M_UNCONDITIONAL, NULL);
    if ( e < eNOERROR ) ERR(handle, e);


    /*
     * By holding the RDSM_LATCH_VOLUME latch, we keep other process from
     * mounting the volume to be formatted.
     */


    /*
     *	III. check if given devices are already mounted
     */
    for (i = 0; i < MAXNUMOFVOLS; i++) {                            /* for each entry of RDsM_VOLTABLE */
        for (j = 0; j < RDSM_VOLTABLE[i].volInfo.numDevices; j++) { /* for each device of the volume */
            for (k = 0; k < numDevices; k++) {                      /* for each devce of given diveces */
	        if (!strcmp(((RDsM_DevInfo*)PHYSICAL_PTR(RDSM_VOLTABLE[i].volInfo.devInfo))[j].devName, devNames[k])) { 

	            e = SHM_releaseLatch(handle, &RDSM_LATCH_VOLTABLE, procIndex);
	            if (e < eNOERROR) ERR(handle, e);

	            ERR(handle, eUSEDDEVICE_RDSM);
	        } /* end if */
	    }
        }
    }


    /*
     *  IV. Set variables of volInfoPage
     *      Note!! volInfoPage is located after masterPage in the first device
     */

    MAKE_PAGEID(volInfoPage.hdr.pid, volNo, NUM_DEVICE_MASTER_PAGES);
    SET_PAGE_TYPE(&volInfoPage, VOL_INFO_PAGE_TYPE);
    volInfoPage.hdr.lsn = common_perThreadDSptr->minLsn;
    volInfoPage.hdr.logRecLen = 0;

    strcpy(volInfoPage.title, title);
    volInfoPage.volNo = volNo;
    volInfoPage.type = type;
    volInfoPage.numDevices = numDevices;
    volInfoPage.extSize = extSize;


    /*
     *  V. Set variables of masterPage and create volume
     */

    /* for each device */
    for (i = 0, firstExtentNo = 0; i < numDevices; i++) {

        /*
         *  Set variables of masterPage
         *  Note!! master page is first page of each device's page segment
         *         each device has "NUM_EXTS_FOR_SYS_PAGES" extents as page segment for meta data
         */

        MAKE_PAGEID(masterPages[i].hdr.pid, volNo, (extSize*firstExtentNo));
        SET_PAGE_TYPE(&masterPages[i], MASTER_PAGE_TYPE);
        masterPages[i].hdr.lsn = common_perThreadDSptr->minLsn;
        masterPages[i].hdr.logRecLen = 0;

        strcpy(masterPages[i].tag, DEVICE_TAG);
        masterPages[i].volNo = volNo;
        masterPages[i].volInfoPageId = volInfoPage.hdr.pid;
        masterPages[i].devNo = i;
        masterPages[i].numExtsInDevice = numPagesInDevice[i] / extSize;

	firstExtentNo += masterPages[i].numExtsInDevice;

        /*
         *  Open device
         */
#ifndef WIN32

#ifndef _LARGEFILE64_SOURCE 
	if ((fd[i] = open(devNames[i], O_RDWR | O_SYNC | O_CREAT, CREATED_VOLUME_PERM)) == -1) 
#else
	if ((fd[i] = open64(devNames[i], O_RDWR | O_SYNC | O_CREAT, CREATED_VOLUME_PERM)) == -1) 
#endif

#else
	if ((fd[i] = CreateFile(devNames[i], GENERIC_WRITE | GENERIC_READ, FILE_SHARE_READ | FILE_SHARE_WRITE, NULL,
		                OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL | FILE_FLAG_NO_BUFFERING, NULL)) == INVALID_HANDLE_VALUE)
#endif /* WIN32 */
 	    ERRL1(handle, eDEVICEOPENFAIL_RDSM, &RDSM_LATCH_VOLTABLE);




        /*
         *  Write dummy contents into the last page:
         *  meaningful only in the case of using a unix file as a volume
         */
        e = rdsm_WriteTrain(handle, fd[i], masterPages[i].numExtsInDevice*extSize-1, &aPage, PAGESIZE2);
        if (e < eNOERROR) ERRL1(handle, e, &RDSM_LATCH_VOLTABLE);
    }


    /*
     * Check size of VolInfoPage & MasterPages
     */
    if (sizeof(MasterPage_T) > PAGESIZE || sizeof(VolInfoPage_T) > PAGESIZE) ERR(handle, eINTERNAL);


    /*
     *  VI. Format data volume.
     */
    if (type == VOLUME_TYPE_DATA) {

        /* set for data volume & get relatted masterPage information */
        e = rdsm_FormatDataVolume(handle, fd, &volInfoPage, masterPages, eff);
        if (e < eNOERROR) ERRL1(handle, e, &RDSM_LATCH_VOLTABLE);
    }


    /*
     *  VII. Write volume information page into the volume
     */

    /* copy contents of volume information page */
    /* Note!! because volume information page size isn't equal to PAGESIZE, this operation is needed */
    aPage.vi = volInfoPage;

    /* write volume information page */
    /* Note!! volume information page always locates in first device */
    e = rdsm_WriteTrain(handle, fd[0], volInfoPage.hdr.pid.pageNo, &aPage.vi, PAGESIZE2);
    if (e < eNOERROR) ERRL1(handle, e, &RDSM_LATCH_VOLTABLE);


    /*
     *  VIII. Write master page into each device
     */

    /* write for each device */
    for (i = 0; i < numDevices; i++) {

        /* copy contents of master page */
        /* Note!! because master page size isn't equal to PAGESIZE, this operation is needed */
        aPage.ms = masterPages[i];

        /* write master page */
        e = rdsm_WriteTrain(handle, fd[i], 0, &aPage.ms, PAGESIZE2);
        if (e < eNOERROR) ERRL1(handle, e, &RDSM_LATCH_VOLTABLE);
    }


    /*
     *  IX. close each device
     */
    for (i = 0; i < numDevices; i++) {
#ifndef WIN32
        if (close(fd[i]) == -1) ERRL1(handle, eDEVICECLOSEFAIL_RDSM, &RDSM_LATCH_VOLTABLE);
#else
	if (CloseHandle(fd[i]) == 0) ERRL1(handle, eDEVICECLOSEFAIL_RDSM, &RDSM_LATCH_VOLTABLE);
#endif /* WIN32 */
    }


    /*
     *  X. release latch
     */
    e = SHM_releaseLatch(handle, &RDSM_LATCH_VOLTABLE, procIndex);
    if (e < eNOERROR) ERR(handle, e);


    /*
     *  XI. Free memory for 'fd' & 'masterPages'
     */
    free(fd);
    free(masterPages);


    return(eNOERROR);

} /* RDsM_Format() */



/*
 * Function: Four rdsm_FormatDataVolume(int*, MasterPage_T*, Four, Four)
 *
 * Description:
 *  Format the given volume as a data volume.
 *
 * Returns:
 *  error code
 */
Four rdsm_FormatDataVolume(
    Four              handle,                /* IN handle */
    FileDesc          *fd,                   /* IN array of file descriptor */
    VolInfoPage_T     *volInfoPage,          /* INOUT volume information page */
    MasterPage_T      *masterPages,          /* INOUT array of master page */
    Four              eff)                   /* IN extent fill factor */
{
    Four              e;                     /* error code */
    Four              i, j, k;               /* index variable */
    Four              numDevices;            /* # of devices in the volume */
    Four              extSize;               /* size of extent (unit = # of page) */
    Four              numExtMapsInTrain;     /* # of extents in bitmap train */
    char              aTrainBuf[TRAINSIZE];  /* buffer for page/train */
    MetaDictPage_T    *mdPage;               /* meta dictionary page */
    BitmapTrain_T     *bmTrain;              /* bitmap train */
    UniqNumPage_T     *unPage;               /* unique number page */
    Page_T            *aPage;                /* a page with all common attributes */
    Four              numExtsForBitmap;      /* # of extents for bitmap (unit = # of extents) */
    Four              accumNumExtsForBitmap; /* accumulated # of extents for bitmap (unit = # of extents) */
    Seed              seed;                  /* seed for random number generation */
    Page_T	      *pagesBufPtr;          /* pointer to buffer of pages */ 
    Four	      nPagesInBuffer; 
    Four	      *numSystemExtentInDevice;
    ExtentMapPage_T   *extentMap;
    ExtentMapEntry_T  *extentMapEntry;
    Four	      curExtentNoInDevice;
    Four              nSysPages;             /* # of system pages to clear in a Bitmap Page*/
    Four              nRemainSysPages;       /* # of system pages to clear in BitMap */


    /* pointer for COMMON Data Structure of perThreadTable */
    COMMON_PerThreadDS_T *common_perThreadDSptr = COMMON_PER_THREAD_DS_PTR(handle);


    TR_PRINT(handle, TR_RDSM, TR1, ("rdsm_FormatDataVolume(fd=%P, volInfoPage=%P, masterPages=%P, eff=%ld)",
                            fd, volInfoPage, masterPages, eff));


    /*
     *  Get variables for fast access
     */
    numDevices = volInfoPage->numDevices;
    extSize = volInfoPage->extSize;


    /*
     *  I. Set some variables of volInfoPage
     */

    /* set 'eff' */
    volInfoPage->dataVol.eff = (eff * extSize) / 100 ;

    /* set variable about meta dictionary page */
    /* Note!! meta dictionary pages are always located after volume information page in first device */
    volInfoPage->dataVol.metaDictPid.volNo = volInfoPage->volNo;
    volInfoPage->dataVol.metaDictPid.pageNo = volInfoPage->hdr.pid.pageNo + NUM_VOLUME_INFO_PAGES;
    volInfoPage->dataVol.metaDictSize = NUM_VOLUME_METADICT_PAGES;


    /*
     *  II. Set variables of masterPage
     */

    /* Calculate 'numExtMapsInTrain */
    numExtMapsInTrain = USABLE_BYTES_PER_BITMAP_TRAIN*CHAR_BIT / extSize;

    /* for each devices, set variables of master page */
    for (i = 0; i < numDevices; i++) {

        /* set information about unique number page */
        masterPages[i].dataVol.numUniqNumEntries = MIN(NUMUNIQUEENTRIESPERPAGE, masterPages[i].numExtsInDevice*extSize);
        masterPages[i].dataVol.uniqPartitionSize = (masterPages[i].numExtsInDevice*extSize-1)/masterPages[i].dataVol.numUniqNumEntries + 1;
        masterPages[i].dataVol.uniqNumPid.volNo = volInfoPage->volNo;
        masterPages[i].dataVol.uniqNumPid.pageNo = (i > 0) ?
                                                   masterPages[i].hdr.pid.pageNo + NUM_DEVICE_MASTER_PAGES :
                                                   masterPages[i].hdr.pid.pageNo + NUM_DEVICE_MASTER_PAGES + NUM_VOLUME_INFO_PAGES + NUM_VOLUME_METADICT_PAGES;

        /* set information about bitmap train */
        masterPages[i].dataVol.bitmapSize = ((masterPages[i].numExtsInDevice-1) / numExtMapsInTrain + 1) * TRAINSIZE2;
        masterPages[i].dataVol.bitmapTrainId.volNo = volInfoPage->volNo;
        masterPages[i].dataVol.bitmapTrainId.pageNo = masterPages[i].dataVol.uniqNumPid.pageNo +
						      NUM_VOLUME_UNIQ_PAGES; 

	/* set information about extent map array page */
	masterPages[i].dataVol.extentMapPageSize = ((masterPages[i].numExtsInDevice-1) / EXTENTMAPENTRIESPERPAGE + 1) * PAGESIZE2;
	masterPages[i].dataVol.extentMapPageId.volNo = volInfoPage->volNo;
	masterPages[i].dataVol.extentMapPageId.pageNo = masterPages[i].dataVol.bitmapTrainId.pageNo +
							masterPages[i].dataVol.bitmapSize;
	masterPages[i].dataVol.firstExtentInDevice = masterPages[i].hdr.pid.pageNo / extSize;
    }


    /*
     *  III. initialize the meta dictionary page
     */

    mdPage = (MetaDictPage_T*) aTrainBuf;

    /* initialize header of meta dictionary page */
    mdPage->hdr.pid = volInfoPage->dataVol.metaDictPid;
    SET_PAGE_TYPE(mdPage, META_DIC_PAGE_TYPE);
    mdPage->hdr.lsn = common_perThreadDSptr->minLsn;
    mdPage->hdr.logRecLen = 0;

    /* intialize each entries except for first meta entry */
    for (i = 1; i < NUMMETADICTENTRIESPERPAGE; i++)
        mdPage->entries[i].name[0] = '\0'; /* null string */

    /* insert the first meta entry for RDSM_FILEID_SEED_NAME */
    seed = FILEID_SEED_INIT_VALUE;
    (void) strcpy(mdPage->entries[0].name, RDSM_FIRST_TRAIN_SEED_NAME);
    (void) memcpy(mdPage->entries[0].data, &seed, sizeof(Seed));

    /* Write this page into the disk */
    /* Note!! meta dictionary pages are always located in first device */
    e = rdsm_WriteTrain(handle, fd[0], mdPage->hdr.pid.pageNo, mdPage, PAGESIZE2);
    if (e < eNOERROR) ERR(handle, e);


    /*
     *  IV. initialize unique number pages in each device
     */

    unPage = (UniqNumPage_T*) aTrainBuf;

    /* initialize header of meta dictionary page */
    SET_PAGE_TYPE(unPage, UNIQUE_NUM_PAGE_TYPE);
    unPage->hdr.lsn = common_perThreadDSptr->minLsn;
    unPage->hdr.logRecLen = 0;

    /* intialize each entries in unique number page */
    for (i = 0; i < NUMUNIQUEENTRIESPERPAGE; i++) unPage->uniques[i] = 0;

    for (i = 0; i < numDevices; i++) {

        /* set PID of unique number page */
        unPage->hdr.pid = masterPages[i].dataVol.uniqNumPid;

        /* Write this page into the disk */
        e = rdsm_WriteTrain(handle, fd[i], unPage->hdr.pid.pageNo - masterPages[i].hdr.pid.pageNo, unPage, PAGESIZE2);
        if (e < eNOERROR) ERR(handle, e);
    }


    /*
     *	Initialize 'numSystemExtentInDevice' which is used to initialize bitmap & extent map
     */
    numSystemExtentInDevice = (Four*) malloc (sizeof(Four) * numDevices);
    if (numSystemExtentInDevice == NULL) ERR(handle, eMEMORYALLOCERR);

    for (i = 0; i < numDevices; i++) {

        numSystemExtentInDevice[i] = (i == 0) ? ((NUM_SYSTEM_PAGES_IN_FIRST_DEVICE +
                                                 masterPages[i].dataVol.bitmapSize +
                                                 masterPages[i].dataVol.extentMapPageSize - 1 ) / extSize + 1)
                                             : ((NUM_SYSTEM_PAGES_IN_NOT_FIRST_DEVICE +
                                                 masterPages[i].dataVol.bitmapSize +
                                                 masterPages[i].dataVol.extentMapPageSize - 1 ) / extSize + 1);
    }


    /*
     *	V. initialize all bitmap trains in each device
     */

    bmTrain = (BitmapTrain_T*) aTrainBuf;

    /* initialize header of bitmap train */
    SET_PAGE_TYPE(bmTrain, BITMAP_TRAIN_TYPE);
    bmTrain->hdr.lsn = common_perThreadDSptr->minLsn;
    bmTrain->hdr.logRecLen = 0;

    for (i = 0; i < numDevices; i++) {

        /*
	 * initialize bitmap train
         */
	Util_SetBits(handle, bmTrain->bytes, 0, USABLE_BYTES_PER_BITMAP_TRAIN*CHAR_BIT);


        /*
         *  V-1. initialize the bitmap train except for first bitmap train
         */

        for (j = TRAINSIZE2; j < masterPages[i].dataVol.bitmapSize; j += TRAINSIZE2) {

            /* set ID of each bitmap train */
 	    bmTrain->hdr.pid.volNo = volInfoPage->volNo;
 	    bmTrain->hdr.pid.pageNo = masterPages[i].dataVol.bitmapTrainId.pageNo + j;

	    /* Write this page into the disk */
            e = rdsm_WriteTrain(handle, fd[i], bmTrain->hdr.pid.pageNo - masterPages[i].hdr.pid.pageNo, bmTrain, TRAINSIZE2);
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
            bmTrain->hdr.pid.pageNo = masterPages[i].dataVol.bitmapTrainId.pageNo + j;
            bmTrain->hdr.pid.volNo  = masterPages[i].dataVol.bitmapTrainId.volNo;

            /* Write first train */
	    /* Note!! bitmaps are located from NUM_EXTS_FOR_SYS_PAGES'th extent */
            e = rdsm_WriteTrain(handle, fd[i], bmTrain->hdr.pid.pageNo - masterPages[i].hdr.pid.pageNo, bmTrain, TRAINSIZE2);
            if (e < eNOERROR) ERR(handle, e);

            /* calculate # of remain pages for the system information to clear in BitMap */
            nRemainSysPages -= nSysPages;
        }

    } /* end for each devices in volume */


    /*
     *	VI. initialize all extent map pages in each device
     */
    extentMap = (ExtentMapPage_T*) aTrainBuf;

    /* initialize header of extent map page */
    SET_PAGE_TYPE(extentMap, EXT_ENTRY_PAGE_TYPE);
    extentMap->hdr.lsn = common_perThreadDSptr->minLsn;
    extentMap->hdr.logRecLen = 0;

    /* for each device, write extent map array entries */
    for (i = 0; i < numDevices; i++) {

        for (j = 0; j < masterPages[i].dataVol.extentMapPageSize; j += PAGESIZE2) {

            /* set ID of each extent map page */
            extentMap->hdr.pid.volNo  = volInfoPage->volNo;
            extentMap->hdr.pid.pageNo = masterPages[i].dataVol.extentMapPageId.pageNo + j;

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

                    if (curExtentNoInDevice < masterPages[i].numExtsInDevice-1) {

                        extentMapEntry->nextExt = masterPages[i].dataVol.firstExtentInDevice + curExtentNoInDevice + 1;
                    }
                    else {

                        extentMapEntry->nextExt = (i < numDevices-1) ? masterPages[i+1].dataVol.firstExtentInDevice +
                                                                       numSystemExtentInDevice[i+1]
                                                                       : NIL;
                        break;
                    }
                }

                /* update 'curExtentNoInDevice' */
                curExtentNoInDevice++;
            }

            /* write this page into the disk */
            e = rdsm_WriteTrain(handle, fd[i], extentMap->hdr.pid.pageNo - masterPages[i].hdr.pid.pageNo, extentMap, PAGESIZE2);
            if (e < eNOERROR) ERR(handle, e);
        }
    }


    /*
     *  VII. Set some variables of volInfoPage
     */

    /* set variable about extent map page */
    volInfoPage->dataVol.freeExtent = numSystemExtentInDevice[0];
    volInfoPage->dataVol.numOfFreeExtent = 0;
    for (i = 0; i < numDevices; i++) {
    	volInfoPage->dataVol.numOfFreeExtent += (masterPages[i].numExtsInDevice - numSystemExtentInDevice[i]);
    }
    volInfoPage->dataVol.freePageExtent = NIL;
    volInfoPage->dataVol.numOfFreePageExtent = 0;
    volInfoPage->dataVol.freeTrainExtent = NIL;
    volInfoPage->dataVol.numOfFreeTrainExtent = 0;


    /*
     * VIII. write the lsn field of each page for the recovery purpose
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

        aPage->header.pid.volNo = volInfoPage->volNo;
        aPage->header.pid.pageNo = NIL;
        SET_PAGE_TYPE(aPage, FREE_PAGE_TYPE);
        aPage->header.lsn = common_perThreadDSptr->minLsn;
        aPage->header.logRecLen = 0;
    }

    /* for each devices, initialize all pages */
    for (i = 0; i < numDevices; i++) {

        for (j = numSystemExtentInDevice[i]; 
	     j < masterPages[i].numExtsInDevice;
	     j += VOLUME_FORMAT_WRITEBUFFERSIZE_IN_EXTENTS) {

            if (masterPages[i].numExtsInDevice - j < VOLUME_FORMAT_WRITEBUFFERSIZE_IN_EXTENTS)
                nPagesInBuffer = (masterPages[i].numExtsInDevice - j) * extSize;

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

} /* rdsm_FormatDataVolume() */
