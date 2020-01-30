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
 * Module: LOG_InitLogVolume.c
 *
 * Description:
 *  Initialize a log volume.
 *
 * Exports:
 *  Four LOG_InitLogVolume(Four, Four)
 */


#include <stdlib.h>
#include "common.h"
#include "error.h"
#include "trace.h"
#include "LOG.h"
#include "RDsM.h"
#include "perProcessDS.h"
#include "perThreadDS.h"


/*
 * Function: Four LOG_InitLogVolume(Four, Four)
 *
 * Description:
 *  initialize the given volume as a log volume.
 *
 * Returns:
 *  error code
 */
Four LOG_InitLogVolume(
    Four 		handle,
    Four 		volNo)		/* IN volume to format */
{
    Four 		e;		/* error code */
    log_LogMasterPage_T masterPage; 	/* a log master page */
    LOG_LogMaster_T 	*logMaster;
    RDsM_VolumeInfo_T 	volumeInfo;   	/* volume information */
    PageID 		pid;            /* page id */
    Four 		i, j;
    log_LogPage_T 	*pagesBufPtr; 	/* pointer to buffer of pages */ 
    Four 		nRemainedPages; 
    Four		nPagesInBuffer; 

    /* pointer for COMMON Data Structure of perThreadTable */
    COMMON_PerThreadDS_T *common_perThreadDSptr = COMMON_PER_THREAD_DS_PTR(handle);

    TR_PRINT(handle, TR_LOG, TR1, ("LOG_InitLogVolume(volNo=%lD)"));


    /*
     * Get the volume information.
     */
    e = RDsM_GetVolumeInfo(handle, volNo, &volumeInfo);
    if (e < eNOERROR) ERR(handle, e);


    /*
     * Initialize the master page.
     */
    pid.volNo = volNo;
    pid.pageNo = LOG_MASTER_PAGE_NO;

    LOG_INIT_LOG_MASTER_PAGE(&masterPage, pid);

    logMaster = &masterPage.master;
    logMaster->volNo = volNo;
    logMaster->logMasterPageNo = LOG_MASTER_PAGE_NO;
    logMaster->firstPageNoOfFirstLogFile = logMaster->logMasterPageNo + 1;
    logMaster->headWrapCount = LSN_STARTING_WRAP_COUNT;
    logMaster->headPageNo = 0;
    logMaster->tailWrapCount = LSN_STARTING_WRAP_COUNT;
    logMaster->tailPageNo = 0;
    logMaster->nLogFiles = NUM_LOG_FILES_IN_LOG_VOLUME;
    logMaster->numPages = ((volumeInfo.numExts - (volumeInfo.numDevices*NUM_EXTS_OF_SYS_PAGES_FOR_LOG_VOLUME)) * volumeInfo.extSize - 1 - logMaster->firstPageNoOfFirstLogFile + 1) / logMaster->nLogFiles;
    logMaster->numBytes = logMaster->numPages *	LOG_GET_SPACE_SIZE_FROM_OFFSET_IN_PAGE(0);
    logMaster->numBytesRemained = logMaster->numBytes;
    logMaster->nextLsn.wrapCount = LSN_STARTING_WRAP_COUNT;
    logMaster->nextLsn.offset = 0;
    logMaster->checkpointLsn = common_perThreadDSptr->nilLsn;
    logMaster->logRecordCount = 0;


    /*
     * Write the log master page into the volume.
     */
    e = RDsM_WriteTrain(handle, (char*)&masterPage, &pid, PAGESIZE2);
    if (e < eNOERROR) ERR(handle, e);


    /*
     * for all the log pages, set their wrapCount fields to LSN_STARTING_WRAP_COUNT
     */
    nPagesInBuffer = volumeInfo.extSize*VOLUME_FORMAT_WRITEBUFFERSIZE_IN_EXTENTS;
    pagesBufPtr = (log_LogPage_T*)malloc(sizeof(log_LogPage_T)*nPagesInBuffer);
    if (pagesBufPtr == NULL) ERR(handle, eMEMORYALLOCERR);

    pid.volNo = volNo;
    pid.pageNo = logMaster->firstPageNoOfFirstLogFile;

    for (i = 0; i < nPagesInBuffer; i++) {
        /* initialize a log page */
        LOG_INIT_LOG_PAGE(&pagesBufPtr[i], pid, common_perThreadDSptr->nilLsn.offset, common_perThreadDSptr->nilLsn.wrapCount);

        pid.pageNo++;
    }

    for (nRemainedPages = logMaster->nLogFiles * logMaster->numPages;
         nRemainedPages > 0;
         nRemainedPages -= nPagesInBuffer) {

        e = RDsM_WriteTrains(handle, (char*)pagesBufPtr, &(pagesBufPtr[0].hdr.pid),
                             ((nRemainedPages >= nPagesInBuffer) ? nPagesInBuffer:nRemainedPages),
                             PAGESIZE2);
        if (e < eNOERROR) {
            free(pagesBufPtr);
            ERR(handle, e);
        }

        for (i = 0; i < nPagesInBuffer; i++)
            pagesBufPtr[i].hdr.pid.pageNo += nPagesInBuffer;
    }

    free(pagesBufPtr);


    return(eNOERROR);

} /* LOG_InitLogVolume() */
