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
 * Module: RDsM_Format.c
 *
 * Description:
 *
 *
 * Exports:
 *  Four RDsM_Format(Four, char**, char*, Four, Two, Four*)
 */


#ifndef WIN32
#include <unistd.h>
#else
#include <windows.h>
#endif

#include <stdlib.h>  /* for malloc & free */
#include <string.h>
#include <assert.h>
#include "common.h"
#include "error.h"
#include "trace.h"
#include "RDsM_Internal.h"
#include "perThreadDS.h"
#include "perProcessDS.h"


/* move this variable to the perProcessDS.h/perThreadDS.h */
/*@ global variable */
/*VolumeTable volTable[MAXNUMOFVOLS]; */



/*@================================
 * RDsM_Format()
 *================================*/
/*
 * Function: Four RDsM_Format(Four, char**, char*, Four, Two, Four*)
 *
 * Description:
 *  Format a new volume on the named device
 *
 * Returns:
 *  error code
 *    eUSEDVOLUMERDsM - this volume already used
 *
 * Side Effects:
 *  The named device is initialized and mounted
 */
Four RDsM_Format(
    Four handle,
    Four          numDevices,                        /* IN # of devices in formated volume */
    char          **devNames,                        /* IN array of device names */
    char          *title,                            /* IN volume title */
    Four          volNo,                             /* IN volume ID */
    Two           sizeOfExt,                         /* IN # of pages in an extent */
    Four          *numPagesInDevice)                 /* IN array of pages' number in each device */
{
    Four          e;                                 /* returned error code */
    Four          i;                                 /* loop index */
    Four          j;                                 /* loop index */
    Four          firstExtNo;                        /* first extent number of each device */
    ExtEntry      *extEntry;                         /* pointer to extent entry */
    unsigned int  seed;                              /* seed for random number generation */
    PageType      aPage;                             /* pointer to a page buffer */
    VolInfoPage   volInfoPage;                       /* volume information page which contains formatted volume */
    Four          totalNumExts;                      /* total # of extents in the formated volume */
    Four          totalNumSysExts;                   /* # of extents which is used for system information in the volume */
    Four          curExtNoInDevice;                  /* extent number in i'th device */
    Four*         numSysPages;                       /* # of system pages in the each device */
    Four*         numSysExts;                        /* # of extents for system pages in the each device */
    Four*         firstFreeExtNo;                    /* first free extent number of the each device */
    DevMasterPage* masterPages;                      /* master page of each device */
    FileDesc*     fd;                                /* open file descriptor */
    Two           vIndex;
    Four          dIndex;
    Four          index;
    Two           k;                                 /* loop index */
    Two           uIndex;                            /* loop index */
    Two           mIndex;                            /* loop index */
    Four          nSysPages;                         /* # of system pages to clear in a Bitmap Page*/
    Four          nRemainSysPages;                   /* # of system pages to clear in BitMap */


    TR_PRINT(TR_RDSM, TR1, ("RDsM_Format(handle, numDevices=%lD, devNames=%P, title=%P, volNo=%lD, sizeOfExt=%lD, numPagesInDevice=%P)",
                             numDevices, devNames, title, volNo, sizeOfExt, numPagesInDevice));


    /*
     *  O. Parameter check
     */
    if (sizeOfExt%PAGESIZE2 != 0 || sizeOfExt%TRAINSIZE2 != 0) ERR(handle, eBADPARAMETER);
    if (numDevices <= 0) ERR(handle, eBADPARAMETER);
    if (devNames == NULL || title == NULL || numPagesInDevice == NULL) ERR(handle, eBADPARAMETER);


    /*
     *  I. Allocate memory for local variables
     */
     fd = (FileDesc *) malloc(sizeof(FileDesc)*numDevices); 
     numSysPages = (Four *) malloc(sizeof(Four)*numDevices);
     numSysExts = (Four *) malloc(sizeof(Four)*numDevices);
     firstFreeExtNo = (Four *) malloc(sizeof(Four)*numDevices);
     masterPages = (DevMasterPage *) malloc(sizeof(DevMasterPage)*numDevices);


    /*
     *  II. Check if given devices are already mounted
     */
    /* modify the way of accessing the volTable for multi threading */
    for (vIndex = 0; vIndex < MAXNUMOFVOLS; vIndex++)               /* for each mounted volume */
        for (dIndex = 0; dIndex < RDSM_PER_THREAD_DS(handle).volTable[vIndex].numDevices; dIndex++)    /* for each device in the i'th volume */
            for (index = 0; index < numDevices; index++)            /* for each given device */
                if (!strcmp(DEVINFO_ARRAY(RDSM_PER_THREAD_DS(handle).volTable[vIndex].devInfo)[dIndex].devName, devNames[index]))
                    ERR(handle, eUSEDDEVICE_RDSM);


    /*
     *  III. Set part of volInfoPage
     *       Note!! volume information page is located after master page of the first device
     */

    MAKE_PAGEID(volInfoPage.hdr.pid, volNo, MASTERPAGESIZE);
    SET_PAGE_TYPE(&volInfoPage, VOL_INFO_PAGE_TYPE);
    RESET_TEMP_PAGE_FLAG(&volInfoPage);

    strcpy(volInfoPage.title, title);
    volInfoPage.volNo = volNo;
    volInfoPage.numDevices = numDevices;
    volInfoPage.sizeOfExt = sizeOfExt;


    /*
     *  IV. Set variables of masterPage and create volume
     */

    /* for each device */
    for (i = 0, firstExtNo = 0, totalNumSysExts = 0; i < numDevices; i++) {

        /*
         * set header of master page
         */
        MAKE_PAGEID(masterPages[i].hdr.pid, volNo, firstExtNo*sizeOfExt);
        SET_PAGE_TYPE(&masterPages[i], MASTER_PAGE_TYPE);
        RESET_TEMP_PAGE_FLAG(&masterPages[i]);

        /*
         * set part of master page
         */
        strcpy(masterPages[i].tag, DEVICE_TAG);
        masterPages[i].volNo = volNo;
        masterPages[i].devNo = i;
        masterPages[i].numOfExtsInDevice = numPagesInDevice[i] / sizeOfExt;
        masterPages[i].volInfoSize = VOLINFOSIZE;
        masterPages[i].volInfoPageId = volInfoPage.hdr.pid;

        /*
         * Initialize information about bitmap
         */
        masterPages[i].bitMapPageId.volNo = volNo;
        masterPages[i].bitMapPageId.pageNo = (i > 0) ?
                                             firstExtNo*sizeOfExt + MASTERPAGESIZE :
                                             firstExtNo*sizeOfExt + MASTERPAGESIZE + VOLINFOSIZE;
        masterPages[i].bitMapSize = (masterPages[i].numOfExtsInDevice-1)/((BITMAP_USABLEBYTESPERPAGE*BITSPERBYTE)/sizeOfExt) + 1;

        /*
         * Initialize information about extent array
         */
        masterPages[i].extEntryArrayPageId.volNo = volNo;
        masterPages[i].extEntryArrayPageId.pageNo = masterPages[i].bitMapPageId.pageNo + masterPages[i].bitMapSize;
        masterPages[i].extEntryArraySize = (masterPages[i].numOfExtsInDevice-1)/EXTENTRYPERPAGE + 1;

        /*
         * Initialize information about unique number page
         */
        masterPages[i].uniqNumPageId.volNo = volNo;
        masterPages[i].uniqNumPageId.pageNo = masterPages[i].extEntryArrayPageId.pageNo + masterPages[i].extEntryArraySize;
        masterPages[i].uniqNumSize = ((masterPages[i].numOfExtsInDevice*sizeOfExt-1)/UNIQUEPARTITIONSIZE)/NUMUNIQUESPERPAGE + 1;

        /*
         *  Open device
         */
#ifndef WIN32

#ifndef _LARGEFILE64_SOURCE 
        if ((fd[i] = open(devNames[i], O_RDWR | O_CREAT, PERM)) == -1)
#else
        if ((fd[i] = open64(devNames[i], O_RDWR | O_CREAT, PERM)) == -1)
#endif

#else
	     /* OS disk cache를 bypass하기 위해서는 FILE_FLAG_NO_BUFFERING을 설정하면 된다. 하지만 Windows의 경우,
	     FILE_FLAG_NO_BUFFERING을 설정할 경우, 빈번한 disk access로 인해, 성능이 떨어지는데, 특히, multiprocess
	     환경에서는 이런 문제가 심각하다. 그러므로, 일단 설정을 하지 않는다 */
             /* To bypass the OS disk cache, set FILE_FLAG_NO_BUFFERING flag. 
		But, if set FILE_FLAG_NO_BUFFERING flag in windows, too many disk access is occured. 
                So the performance is decreased. Especially in multiprocess environment, This problem is mush serious.
                Therefore, we do not set the flag in windows. */
        if ((fd[i] = CreateFile(devNames[i], GENERIC_WRITE | GENERIC_READ, FILE_SHARE_READ | FILE_SHARE_WRITE, NULL,
                                OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL /*| FILE_FLAG_NO_BUFFERING*/, NULL)) == INVALID_HANDLE_VALUE)
#endif
            ERR(handle, eDEVICEOPENFAIL_RDSM);

        /*
         *  Write dummy contents into the last page
         * : meaningful only in the case of using a unix file as a volume
         */
        e = rdsm_WriteTrain(handle, fd[i], numPagesInDevice[i]-1, &aPage, PAGESIZE2);
        if (e < eNOERROR) ERR(handle, e);


        /*
         *  Set 'numSysExts[]' & 'firstFreeExtNo[]' and calculate 'totalNumSysExts'
         */

        /* calculate 'numSysPages' */
        /* Note!! only first device has meta dictionary pages */
        numSysPages[i] = (i > 0) ?
                         masterPages[i].uniqNumPageId.pageNo + masterPages[i].uniqNumSize - firstExtNo*sizeOfExt:
                         masterPages[i].uniqNumPageId.pageNo + masterPages[i].uniqNumSize - firstExtNo*sizeOfExt + METADICTSIZE;

        /* set 'numSysExtsInDevice[]' */
        numSysExts[i] = (numSysPages[i] - 1)/sizeOfExt + 1;

        /* set 'firstFreeExtNo[]' */
        firstFreeExtNo[i] = firstExtNo + numSysExts[i];

        /* update 'numSysPages' */
        totalNumSysExts += numSysExts[i];


        /*
         *  Update 'firstExtNo'
         */
        firstExtNo += masterPages[i].numOfExtsInDevice;
    }

    /* set totalNumExts */
    totalNumExts = firstExtNo;


    /*
     *  V. Set remaining part of volInfoPage
     */

    /* set variable about meta dictionary page */
    /* Note!! meta dictionary page is located after unique number page of first device */
    volInfoPage.metaDicPageId.volNo = volNo;
    volInfoPage.metaDicPageId.pageNo = masterPages[0].uniqNumPageId.pageNo + masterPages[0].uniqNumSize;
    volInfoPage.metaDicSize = METADICTSIZE;

    /* set 'numOfFreeExts' & 'firstFreeExts' of volInfoPage */
    volInfoPage.numOfFreeExts = totalNumExts - totalNumSysExts;
    volInfoPage.firstFreeExt = firstFreeExtNo[0];


    /*
     *  V. Initialize bitmap pages, extEntryArray, and unique number pages in the each device
     */

    /* for each device */
    for (i = 0, firstExtNo = 0; i < numDevices; i++) {

        /*
         *  V-1. Initialize bitmap pages
         */

        /* initialize header except for pid */
        SET_PAGE_TYPE(&aPage.ch, BITMAP_PAGE_TYPE);
        RESET_TEMP_PAGE_FLAG(&aPage.ch);

        /* initialize bitmap */
        RDsM_set_bits(handle, &aPage, 0, BITMAP_USABLEBYTESPERPAGE*BITSPERBYTE);

        /*  initialize the bitmap page except for first bitmap page */
        for (j = PAGESIZE2; j < masterPages[i].bitMapSize; j += PAGESIZE2) {

            /* set ID of each bitmap page's header */
            aPage.ch.hdr.pid.volNo = volNo;
            aPage.ch.hdr.pid.pageNo = masterPages[i].bitMapPageId.pageNo + j;

            /* Write this page into the disk */
            e = rdsm_WriteTrain(handle, fd[i], aPage.ch.hdr.pid.pageNo - firstExtNo*sizeOfExt, &aPage.ch, PAGESIZE2);
            if (e < eNOERROR) ERR(handle, e);
        }

        /*
         * Because previous code cannot support the large database (>8T), we modify the previous code
         */
        for (j = 0, nRemainSysPages = numSysPages[i]; nRemainSysPages > 0; j += PAGESIZE2) {

            /* calculate # of pages for the system information to clear in a Bitmap Page */
            nSysPages = MIN(BITMAP_USABLEBYTESPERPAGE*BITSPERBYTE, nRemainSysPages);

            /* initialize bitmap */
            RDsM_set_bits(handle, &aPage, 0, BITMAP_USABLEBYTESPERPAGE*BITSPERBYTE);

            /* allocate pages for the system information */
            RDsM_clear_bits(handle, &aPage, 0, nSysPages);

            /* set ID of first bitmap page */
            aPage.ch.hdr.pid.pageNo = masterPages[i].bitMapPageId.pageNo + j;
            aPage.ch.hdr.pid.volNo  = masterPages[i].bitMapPageId.volNo;

            /* Write first bitmap page */
            e = rdsm_WriteTrain(handle, fd[i], aPage.ch.hdr.pid.pageNo - firstExtNo*sizeOfExt, &aPage.ch, PAGESIZE2);
            if (e < eNOERROR) ERR(handle, e);

            /* calculate # of remain pages for the system information to clear in BitMap */
            nRemainSysPages -= nSysPages;
        }


        /*
         *  V-2. Initialize extent entry array
         */

        /* initialize header except for pid */
        SET_PAGE_TYPE(&aPage.el, EXT_ENTRY_PAGE_TYPE);
        RESET_TEMP_PAGE_FLAG(&aPage.el);

        /* for each extent entry array page */
        for (j = 0; j < masterPages[i].extEntryArraySize; j += PAGESIZE2) {

            /* set ID of each bitmap page's header */
            aPage.el.hdr.pid.volNo = volNo;
            aPage.el.hdr.pid.pageNo = masterPages[i].extEntryArrayPageId.pageNo + j;

           /* initialize 'curExtNoInDevice' in extent array */
           curExtNoInDevice = j * EXTENTRYPERPAGE;

            /* for each extent entry */
            for (k = 0, extEntry = aPage.el.el; k < EXTENTRYPERPAGE; k++, extEntry++) {

                /* set extent fill factor and prevExt to NIL */
                extEntry->eff = NIL;
                extEntry->prevExt = NIL;

                /* if extent is system extents, exclude from free extent list */
                if (curExtNoInDevice < numSysExts[i]) {
                    extEntry->nextExt = NIL;
                }
                /* if extent is normal extents, insert into free extent list */
                else {
                    if (curExtNoInDevice < masterPages[i].numOfExtsInDevice - 1) {
                        extEntry->nextExt = firstExtNo + curExtNoInDevice + 1;
                    }
                    else {
                        extEntry->nextExt = (i < numDevices-1) ? firstFreeExtNo[i+1] : NIL;
                        assert (j == masterPages[i].extEntryArraySize-1);
                        break;
                    }
                }

                /* update 'curExtNoInDevice' */
                curExtNoInDevice++;

            } /* end for k */

            /* Write this page into the disk */
            e = rdsm_WriteTrain(handle, fd[i], aPage.el.hdr.pid.pageNo - firstExtNo*sizeOfExt, &aPage.el, PAGESIZE2);
            if (e < eNOERROR) ERR(handle, e);

        } /* end for j */


        /*
         *  V-3. Initialize unique number pages
         */

        /* initialize header except for pid */
        SET_PAGE_TYPE(&aPage.un, UNIQUE_NUM_PAGE_TYPE);
        RESET_TEMP_PAGE_FLAG(&aPage.un);

        /* initialize unique number in unique number page */
        for (uIndex = 0; uIndex < NUMUNIQUESPERPAGE; uIndex++) aPage.un.unique[uIndex] = 0;

        /* for each unique number pages */
        for (j = 0; j < masterPages[i].uniqNumSize; j += PAGESIZE2) {

            /* set ID of each bitmap page's header */
            aPage.un.hdr.pid.volNo = volNo;
            aPage.un.hdr.pid.pageNo = masterPages[i].uniqNumPageId.pageNo + j;

            /* Write this page into the disk */
            e = rdsm_WriteTrain(handle, fd[i], aPage.un.hdr.pid.pageNo - firstExtNo*sizeOfExt, &aPage.un, PAGESIZE2);
            if (e < eNOERROR) ERR(handle, e);

        } /* end for j */


        /*
         *  V-4. Update 'firstExtNo'
         */
        firstExtNo += masterPages[i].numOfExtsInDevice;

    } /* end for each device */



    /*
     *  V. Initialize meta dictionary page
     */

    /* initialize the meta dictionary page */
    aPage.md.hdr.pid = volInfoPage.metaDicPageId;
    SET_PAGE_TYPE(&aPage.md, META_DIC_PAGE_TYPE);
    RESET_TEMP_PAGE_FLAG(&aPage.md);

    /* set first meta dictonary entry as 'SEED' */
    seed = 0;
    strcpy(aPage.md.metaEntry[0].name, "SEED");
    memcpy(aPage.md.metaEntry[0].data, &seed, sizeof(unsigned int));

    /* initialize meta dictionary entry except for first entry */
    for (mIndex = 1; mIndex < NUMMETAENTRIESPERPAGE; mIndex++)
        aPage.md.metaEntry[mIndex].name[0] = '\0';

    /* Write this page into the disk */
    /* Note!! meta dictionary page is always located at first device */
    e = rdsm_WriteTrain(handle, fd[0], aPage.md.hdr.pid.pageNo, &aPage.md, PAGESIZE2);
    if (e < eNOERROR) ERR(handle, e);


    /*
     *  VI. Write device master page
     */

    /* write for each device */
    /* Note!! device master page is always located at start of the each device */
    for (i = 0; i < numDevices; i++) {
        e = rdsm_WriteTrain(handle, fd[i], 0, &masterPages[i], PAGESIZE2);
        if (e < eNOERROR) ERR(handle, e);
    }


    /*
     *  VII. Write volume information page
     */

    /* write volume information page */
    /* Note!! volume information page is always located at first device */
    e = rdsm_WriteTrain(handle, fd[0], volInfoPage.hdr.pid.pageNo, &volInfoPage, PAGESIZE2);
    if (e < eNOERROR) ERR(handle, e);


    /*
     *  VIII. Close each device
     */

    for (i = 0; i < numDevices; i++) {
#ifndef WIN32
        if (close(fd[i]) == -1) ERR(handle, eDEVICECLOSEFAIL_RDSM);
#else
        if (CloseHandle(fd[i]) == 0) ERR(handle, eDEVICECLOSEFAIL_RDSM);
#endif
    }


    /*
     *  IX. Free memory for local variables
     */
     free(fd);
     free(numSysPages);
     free(numSysExts);
     free(firstFreeExtNo);
     free(masterPages);


    return(eNOERROR);

} /* RDsM_Format() */
