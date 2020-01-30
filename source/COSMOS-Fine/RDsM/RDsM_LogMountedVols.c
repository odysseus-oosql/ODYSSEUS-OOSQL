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
 * Module: RDsM_LogMountedVols.c
 *
 * Description:
 *  Write a log record containing the mounted volumes.
 *
 * Exports:
 *  Four RDsM_LogMountedVols(void)
 *
 * Returns:
 *  error code
 */


#include <string.h>
#include "common.h"
#include "error.h"
#include "trace.h"
#include "SHM.h"
#include "LOG.h"
#include "RDsM.h"
#include "perProcessDS.h"
#include "perThreadDS.h"



Four RDsM_LogMountedVols(
    Four		handle 			/* handle */
)
{
    Four 		 e;                     /* error code */
    rdsm_VolTableEntry_T *entry; 		/* volume table entry corresponding to the given volume */
    RDsM_VolumeInfo_T 	 *volInfo; 		/* volume information in volume table entry */
    Four 		 i;
    Four 		 entryNo;               /* entry no of volume table */
    Four   		 nVolumes_in_chunk;
    Four 		 stringLen_in_chunk;
    Four 		 len;
    char 		 buf[LOG_MAX_IMAGE_SIZE];
    Lsn_T                lsn;                  	/* LSN of the newly written log record */
    Four                 logRecLen;             /* log record length */
    LOG_LogRecInfo_T     logRecInfo;            /* log record information */
    RDsM_DevInfo 	 *devInfo; 
    RDsM_DevInfoForDataVol *devInfoForDataVol; 

    /* pointer for COMMON Data Structure of perThreadTable */
    COMMON_PerThreadDS_T *common_perThreadDSptr = COMMON_PER_THREAD_DS_PTR(handle);


    TR_PRINT(handle, TR_RDSM, TR1, ("RDsM_LogMountedVols()"));


    /*
     * Mutex Begin : for controlling table lookup with mount/dismount operation
     */
    e = SHM_getLatch(handle, &RDSM_LATCH_VOLTABLE, procIndex, M_SHARED, M_UNCONDITIONAL, NULL);
    if ( e < eNOERROR ) ERR(handle, e);

    nVolumes_in_chunk = 0;
    stringLen_in_chunk = 0;

    for (entryNo = 0, entry = &RDSM_VOLTABLE[0]; entryNo < MAXNUMOFVOLS; entryNo++, entry++) {
        /* points to the volume information for the fast access */
        volInfo = &entry->volInfo;

        if (PHYSICAL_PTR(volInfo->devInfo) == NULL || volInfo->type != VOLUME_TYPE_DATA) continue; /* not used or no data volume */ 

        devInfo = PHYSICAL_PTR(volInfo->devInfo); 

        len = 0;
        for (i = 0; i < volInfo->numDevices; i++)
            len += strlen(devInfo[i].devName) + 1; 

        if (len > sizeof(buf)) ERRL1(handle, eINTERNAL, &RDSM_LATCH_VOLTABLE);

        if (len + stringLen_in_chunk > sizeof(buf)) {
            LOG_FILL_LOGRECINFO_2(logRecInfo, common_perThreadDSptr->nilXactId, LOG_TYPE_CHECKPOINT,
                                  LOG_ACTION_CHKPT_MOUNTED_VOLS, LOG_REDO_ONLY,
                                  common_perThreadDSptr->nilPid, common_perThreadDSptr->nilLsn, common_perThreadDSptr->nilLsn,
                                  sizeof(Four), &nVolumes_in_chunk,
                                  stringLen_in_chunk, buf);

            e = LOG_WriteLogRecord(handle, NULL, &logRecInfo, &lsn, &logRecLen);
            if (e < eNOERROR) ERRL1(handle, e, &RDSM_LATCH_VOLTABLE);

            nVolumes_in_chunk = 0;
            stringLen_in_chunk = 0;
        }

        for (i = 0, len = 0; i < volInfo->numDevices; i++) {
            len = strlen(devInfo[i].devName)+1; 
            sprintf(&buf[stringLen_in_chunk], "%s", devInfo[i].devName); 
            stringLen_in_chunk += len;
            buf[stringLen_in_chunk-1] = ';'; 
        }
        buf[stringLen_in_chunk-1] = '\0';
        nVolumes_in_chunk ++;
    }

    if (nVolumes_in_chunk != 0) {
        LOG_FILL_LOGRECINFO_2(logRecInfo, common_perThreadDSptr->nilXactId, LOG_TYPE_CHECKPOINT,
                              LOG_ACTION_CHKPT_MOUNTED_VOLS, LOG_REDO_ONLY,
                              common_perThreadDSptr->nilPid, common_perThreadDSptr->nilLsn, common_perThreadDSptr->nilLsn,
                              sizeof(Four), &nVolumes_in_chunk,
                              stringLen_in_chunk, buf);

        e = LOG_WriteLogRecord(handle, NULL, &logRecInfo, &lsn, &logRecLen);
        if (e < eNOERROR) ERRL1(handle, e, &RDSM_LATCH_VOLTABLE);
    }

    /*
     * Mutex End: for controlling table lookup with mount/dismount operation
     */
    e = SHM_releaseLatch(handle, &RDSM_LATCH_VOLTABLE, procIndex);
    if (e < eNOERROR) ERR(handle, e);

    return(eNOERROR);

} /* RDsM_LogMountedVols() */


