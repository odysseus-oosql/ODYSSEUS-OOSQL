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
 * Module: RDsM_GetUnique.c
 *
 * Description:
 *  get a group of unique numbers
 *
 * Exports:
 *  Four RDsM_GetUnique(PageID*, Unique*, Four*)
 */


#include <assert.h> 
#include "common.h"
#include "trace.h"
#include "error.h"
#include "latch.h"
#include "SHM.h"
#include "RDsM.h"
#include "BfM.h"
#include "TM.h"
#include "LOG.h"
#include "perProcessDS.h"
#include "perThreadDS.h"




/*
 * Function: Four RDsM_GetUnique(PageID*, Unique*, Four*)
 *
 * Description:
 *   get a group of unique numbers
 *
 * Returns:
 *  Error code
 */
Four RDsM_GetUnique(
    Four                 handle,             /* IN    handle */
    XactTableEntry_T     *xactEntry,         /* IN transaction table entry */
    PageID               *pid,               /* IN page identifier */
    Unique               *unique,            /* OUT starting unique number */
    Four                 *count,             /* OUT number of unique numbers allocated in this procedure */
    LogParameter_T       *logParam)          /* IN log parameter */
{
    Four                 e;                  /* returned error code */
    Four                 devNo;              /* device number in the volume */
    Four                 pageOffset;         /* offset of page in the device (unit = # of page) */
    Four                 entryNo;            /* entry no of volume table entry corresponding to the given volume */
    rdsm_VolTableEntry_T *entry;             /* volume table entry corresponding to the given volume */
    Buffer_ACC_CB        *aPage_BCBP;        /* Buffer Access Control Block for Unique Number */
    UniqNumPage_T        *u_page;            /* unique number page */
    Four                 parNo;              /* partition number for maintaining unique numbers */
    RDsM_VolumeInfo_T    *volInfo;           /* volume information in volume table entry */
    Lsn_T                lsn;                /* LSN of the newly written log record */
    Four                 logRecLen;          /* log record length */
    LOG_LogRecInfo_T     logRecInfo;         /* log record information */
    LOG_Image_RDsM_GetUniqueInfo_T getUniqueInfo; /* information of the gotten unique number */
    RDsM_DevInfoForDataVol *devInfoForDataVol; 


    /* pointer for COMMON Data Structure of perThreadTable */
    COMMON_PerThreadDS_T *common_perThreadDSptr = COMMON_PER_THREAD_DS_PTR(handle);

    TR_PRINT(handle, TR_RDSM, TR1, ("RDsM_GetUnique(pid=%P, unique=%P, count=%P)", pid, unique, count));


    /*
     *	check input parameters
     */
    if (pid == NULL || unique == NULL || count == NULL)	ERR(handle, eBADPARAMETER);

    /*
     *	get the corresponding volume table entry via searching the volTable
     */
    e = rdsm_GetVolTableEntryNoByVolNo(handle, pid->volNo, &entryNo);
    if (e < eNOERROR) ERR(handle, e);

    /*
     *	set entry to point to the corresponding entry
     */
    volInfo = &RDSM_VOLTABLE[entryNo].volInfo;
    assert(volInfo->type == VOLUME_TYPE_DATA); 

    /*
     *  find device which contains given page and get physical offset
     */
    e = rdsm_GetPhysicalInfo(handle, volInfo, pid->pageNo, &devNo, &pageOffset);
    if (e < eNOERROR) ERR(handle, e);

    devInfoForDataVol = PHYSICAL_PTR(volInfo->dataVol.devInfo); 

    /*
     *	calculate the corresponding unique number partition that the page belongs to
     */
    parNo = (pageOffset/devInfoForDataVol[devNo].uniqPartitionSize); 

    /*
     *	get a page buffer for a unique number page
     */
    e = BfM_getAndFixBuffer(handle, &devInfoForDataVol[devNo].uniqNumPid, M_EXCLUSIVE, &aPage_BCBP, PAGE_BUF); 
    if (e < eNOERROR) ERR(handle, e);

    u_page = (UniqNumPage_T*)aPage_BCBP->bufPagePtr;

    /*
     *	get unique numbers and increment the respective element
     */
    *unique = u_page->uniques[parNo];
    *count = NUMALLOCATEDUNIQUES;
    u_page->uniques[parNo] += NUMALLOCATEDUNIQUES;

    /*
     * Write log record.
     */
    if (logParam->logFlag & LOG_FLAG_DATA_LOGGING) {

	getUniqueInfo.partitionNo = parNo;
	getUniqueInfo.uniqueNum = u_page->uniques[parNo];

	LOG_FILL_LOGRECINFO_1(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                              LOG_ACTION_RDSM_GET_UNIQUES, LOG_REDO_ONLY,
                              devInfoForDataVol[devNo].uniqNumPid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                              sizeof(getUniqueInfo), &getUniqueInfo); 
	e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
	if (e < eNOERROR) ERR(handle, e);

        u_page->hdr.lsn = lsn;
        u_page->hdr.logRecLen = logRecLen;
    }

    /*
     *	set this unique number page dirty
     */
    aPage_BCBP->dirtyFlag = 1;

    /*
     *	free this unique number page
     */
    BFM_FREEBUFFER(handle, aPage_BCBP, PAGE_BUF, e);
    if (e < eNOERROR) ERR(handle, e);


    return(eNOERROR);

} /* RDsM_GetUnique() */
