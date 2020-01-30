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
 * Module: RDsM_Dismount.c
 *
 * Description:
 *  Dismount the named device
 *
 * Exports:
 *  Four RDsM_Dismount(Four, Boolean)
 */


#ifndef WIN32
#include <unistd.h>
#else
#include <windows.h>
#endif /* WIN32 */
#include <string.h>
#include "common.h"
#include "trace.h"
#include "error.h"
#include "latch.h"
#include "SHM.h"
#include "RDsM.h"
#include "TM.h"
#include "LOG.h"
#include "perProcessDS.h"
#include "perThreadDS.h"




/*
 * Function: Four RDsM_Dismount(Four)
 *
 * Description:
 *   Dismount the named device
 *
 * Returns:
 *  Error code
 */
Four RDsM_Dismount(
    Four                        handle,                 /* IN    handle */
    Four    			volNo,			/* IN volID of the physical volume device */
    Boolean 			logFlag)            	/* IN indicates whether logging is performed */
{
    Four 			e;                     	/* error returned */
    Four 			i;                     	/* loop variable */
    Four 			entryNo;               	/* entry no of volume table entry corresponding to the given volume */
    rdsm_VolTableEntry_T 	*entry;			/* volume table entry (in shared memory) corresponding to the given volume */
    Lsn_T 			lsn;                  	/* LSN of the newly written log record */
    Four 			logRecLen;            	/* log record length */
    LOG_LogRecInfo_T 		logRecInfo; 		/* log record information */
    RDsM_VolumeInfo_T 		*volInfo;  		/* volume info */

    /* pointer for COMMON Data Structure of perThreadTable */
    COMMON_PerThreadDS_T *common_perThreadDSptr = COMMON_PER_THREAD_DS_PTR(handle);


    TR_PRINT(handle, TR_RDSM, TR1, ("RDsM_Dismount(volNo=%ld, logFlag=%ld)", volNo, logFlag));


    /*
     * get the corresponding volume table entry via searching the volTable
     */
    e = rdsm_GetVolTableEntryNoByVolNo(handle, volNo, &entryNo);
    if (e < eNOERROR) ERR(handle, e);

    /*
     * At this point, we are sure that this process has been mounted the given volume.
     */

    /*
     *	set a pointer to the corresponding entry
     */
    entry = &(RDSM_VOLTABLE[entryNo]);

    /*
     * points to the volume information
     */
    volInfo = &entry->volInfo;

    /*
     * Flush the volume info page.
     * Note: Volume info page is read from disk, when volume is mounted.
     *       To synchronize the volume info page, we flush the volume info page here.
     */
    e = BfM_FlushTrain(handle, &volInfo->volInfoPageId, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);

    /*
     *	close the volume device
     */
    for (i = 0; i < RDSM_USERVOLTABLE(handle)[entryNo].numDevices; i++) {
#ifndef WIN32
        if (close(OPENFILEDESC_ARRAY(RDSM_USERVOLTABLE(handle)[entryNo].openFileDesc)[i]) == -1) ERR(handle, eDEVICECLOSEFAIL_RDSM);
#else
	if (CloseHandle(OPENFILEDESC_ARRAY(RDSM_USERVOLTABLE(handle)[entryNo].openFileDesc)[i]) == 0) ERR(handle, eDEVICECLOSEFAIL_RDSM);
#endif /* WIN32 */
    }

    /*
     * Mark the entry as an empty entry.
     */
    RDSM_USERVOLTABLE(handle)[entryNo].volNo = NOVOL;

    /*
     * Dismount the volume from the shared volume table.
     */
    /* Mutex Begin :: Volume Table Entry  */
    e = SHM_getLatch(handle, &entry->latch, procIndex, M_EXCLUSIVE, M_UNCONDITIONAL, NULL);
    if (e < eNOERROR) ERR(handle, e);

    /*
     * Decrement the mount count.
     * If the nMounts is 0, then the entry will be cleared.
     */
    entry->nMounts --;

    if (entry->nMounts == 0) {

        /* free allocate memory for 'devInfo' array */
        e = Util_freeArrayToHeap(handle, &RDSM_DEVINFOTABLEHEAP, PHYSICAL_PTR(entry->volInfo.devInfo)); 
        if (e < eNOERROR) ERRL1(handle, e, &entry->latch);

        /* free allocate memory for 'devInfoForDataVol' array */
        if (PHYSICAL_PTR(entry->volInfo.dataVol.devInfo) != NULL) { 
            e = Util_freeArrayToHeap(handle, &RDSM_DEVINFOFORDATAVOLTABLEHEAP, PHYSICAL_PTR(entry->volInfo.dataVol.devInfo)); 
            if (e < eNOERROR) ERRL1(handle, e, &entry->latch);
        }

        /* empty corresponding entry */
        entry->volInfo.volNo = NOVOL;
        entry->volInfo.title[0] = '\0'; /* null string */
        entry->volInfo.numDevices = 0;
        entry->volInfo.devInfo = LOGICAL_PTR(NULL); 
        entry->volInfo.dataVol.devInfo = LOGICAL_PTR(NULL); 

        /* Write Log Record */
        if (logFlag) {
            VolNo tmp_volNo = volNo;

            LOG_FILL_LOGRECINFO_1(logRecInfo, common_perThreadDSptr->nilXactId, LOG_TYPE_VOLUME,
                                  LOG_ACTION_VOL_DISMOUNT_VOLUME, LOG_REDO_ONLY,
                                  common_perThreadDSptr->nilPid, common_perThreadDSptr->nilLsn, common_perThreadDSptr->nilLsn,
                                  sizeof(VolNo), &tmp_volNo);

            e = LOG_WriteLogRecord(handle, NULL, &logRecInfo, &lsn, &logRecLen);
            if (e < eNOERROR) ERRL1(handle, e, &entry->latch);
        }
    }

    /* Mutex End :: Volume Table Entry  */
    e = SHM_releaseLatch(handle, &entry->latch, procIndex);
    if ( e < eNOERROR ) ERR(handle, e);


    return(eNOERROR);

} /* RDsM_Dismount() */
