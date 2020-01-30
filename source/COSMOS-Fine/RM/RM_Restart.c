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
 * Module: RM_Restart.c
 *
 * Description:
 *  after a system failure, restart the system for recovery.
 *
 * Exports:
 *  Four RM_Restart(Four, char*)
 */


#include <string.h>
#include "common.h"
#include "error.h"
#include "trace.h"
#include "Util.h"
#include "dirtyPageTable.h"
#include "xactTable.h"
#include "LOG.h"
#include "RDsM.h"
#include "BfM.h"
#include "TM.h"
#include "RM.h"
#include "LRDS.h"
#include "perProcessDS.h"
#include "perThreadDS.h"




/*
 * Function: Four RM_Restart(Four, char*)
 *
 * Description:
 *  after a system failure, restart the system for recovery.
 *
 * Returns:
 *  error code
 */
Four RM_Restart(
    Four 		handle,
    char 		*logVolumeDeviceList)  			/* IN device list for the log volume */
{
    Four 		e;					/* returned error code */
    Lsn_T 		redoLsn;				/* starting point for the redo pass */
    Four 		volNo;					/* identifier of the log volume */
    Four 		numDevices;
    char 		*ptr;
    char 		buf[MAX_DEVICE_NAME_SIZE*MAX_DEVICES_IN_LOG_VOLUME];
    char 		*ptrToDevices[MAX_DEVICES_IN_LOG_VOLUME];

    /* pointer for RM Data Structure of perThreadTable */
    RM_PerThreadDS_T *rm_perThreadDSptr = RM_PER_THREAD_DS_PTR(handle);


    TR_PRINT(handle, TR_RM, TR1, ("RM_Restart(logVolumeDeviceList=%P)", logVolumeDeviceList));


    strcpy(buf, logVolumeDeviceList);

    for (numDevices = 0, ptr = buf; *ptr; numDevices++) {
        ptrToDevices[numDevices] = ptr;

#ifndef WIN32
        while (*ptr && *ptr != ':' && *ptr != ';') ptr++;
#else
        while (*ptr && *ptr != ';') ptr++;
#endif
        if (*ptr) *ptr++ = '\0'; 
    }

    /* Mount the log volume. */
    e = RDsM_Mount(handle, numDevices, ptrToDevices, &volNo, FALSE);
    if (e < eNOERROR) ERR(handle, e);


    /* Open the log volume. */
    e = LOG_OpenVolume(handle, volNo);
    if (e < eNOERROR) ERR(handle, e);


    /*
     * perform the analysis pass
     */
    e = RM_DPT_InitTable(handle, &(rm_perThreadDSptr->rm_dirtyPageTable), DPT_HASH_TABLE_SIZE, DPT_INIT_NUM_HASH_TABLE_ENTRIES);
    if (e < eNOERROR) ERR(handle, e);

    e = rm_Analysis(handle, &(rm_perThreadDSptr->rm_dirtyPageTable), &redoLsn);
    if (e < eNOERROR) ERR(handle, e);


    /*
     * perform the redo pass
     */
    e = rm_Redo(handle, &redoLsn, &(rm_perThreadDSptr->rm_dirtyPageTable));
    if (e < eNOERROR) ERR(handle, e);


    /*
     * Update the buffer manager dirty page table
     */
    e = BfM_UpdateDirtyPageTableEntries(handle); 
    if (e < eNOERROR) ERR(handle, e);


    /*
     * destroy the restart dirty page table
     */
    e = RM_DPT_FinalTable(handle, &(rm_perThreadDSptr->rm_dirtyPageTable));
    if (e < eNOERROR) ERR(handle, e);


    /*
     * perform the undo pass
     */
    e = rm_Undo(handle);
    if (e < eNOERROR) ERR(handle, e);


    /*
     * Dismount the currently mounted volumes.
     */
    e = BfM_dismount(handle, -1);       /* volume no -1 means all volumes */
    if (e < eNOERROR) ERR(handle, e);

    e = RDsM_DismountDataVolumes(handle);
    if (e < eNOERROR) ERR(handle, e);


    /*
     * perform the checkpointing for keeping a stable status
     */
    e = RM_Checkpoint(handle);
    if (e < eNOERROR) ERR(handle, e);


    return(eNOERROR);

} /* RM_Restart( ) */
