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
/******************************************************************************/
/*                                                                            */
/*    This module has been implemented based on "The Multilevel Grid File     */
/*    (MLGF) Version 4.0," which can be downloaded at                         */
/*    "http://dblab.kaist.ac.kr/Open-Software/MLGF/main.html".                */
/*                                                                            */
/******************************************************************************/

/*
 * Module: MLGF_CreateIndex.c
 *
 * Description:
 *  Create an MLGF index.
 *
 * Exports:
 *  Four MLGF_CreateIndex(Four, Four, MLGF_KeyDesc*, IndexID*)
 *
 * Returns:
 *  Error code
 *    eBADPARAMETER
 *    some errors caused by function calls
 */


#include "common.h"
#include "error.h"
#include "trace.h"
#include "Util.h"
#include "TM.h"
#include "LOG.h"
#include "RDsM.h"
#include "MLGF.h"
#include "perProcessDS.h"
#include "perThreadDS.h"


Four MLGF_CreateIndex(
    Four handle,
    XactTableEntry_T 			*xactEntry, 		/* IN transaction table entry */
    IndexID 				*iid,			/* IN allocated index ID of new MLGF */ 
    MLGF_KeyDesc 			*kdesc,			/* IN key descriptor of the new MLGF index */
    PageID 				*rootPid,		/* OUT PageID of root of newly created index */ 
    SegmentID_T 			*pageSegmentID, 	/* OUT page segment id of new MLGF */
    LogParameter_T 			*logParam)   		/* IN log parameter */
{
    Four 				e;			/* error code */
    Buffer_ACC_CB 			*root_BCB;		/* buffer access control block for root */
    mlgf_DirectoryPage 			*apage;  		/* root page of the created index */
    Lsn_T 				lsn;                  	/* lsn of the newly written log record */
    Four 				logRecLen;             	/* log record length */
    LOG_LogRecInfo_T 			logRecInfo; 		/* log record information */
    LOG_Image_MLGF_InitDirectoryPage_T 	initDirPageInfo; 	/* directory page information */


    /* pointer for COMMON Data Structure of perThreadTable */
    COMMON_PerThreadDS_T *common_perThreadDSptr = COMMON_PER_THREAD_DS_PTR(handle);

    TR_PRINT(handle, TR_MLGF, TR1, ("MLGF_CreateIndex()"));


    /* check parameters */
    if (iid == NULL || kdesc == NULL || rootPid == NULL) ERR(handle, eBADPARAMETER);


    /* Allocate a page for root of the newly created MLGF index. */
    e = RDsM_CreateSegment(handle, xactEntry, iid->volNo, pageSegmentID, PAGESIZE2, logParam);
    if (e < eNOERROR) ERR(handle, e);

    e = RDsM_AllocTrains(handle, xactEntry, iid->volNo, pageSegmentID, NULL, 1, PAGESIZE2, TRUE, rootPid, logParam);
    if (e < eNOERROR) ERR(handle, e);

    /*
    ** Construct a root page.
    */

    /* Read the root page into the buffer. */
    e = BfM_fixNewBuffer(handle, rootPid, M_EXCLUSIVE, &root_BCB, PAGE_BUF); 
    if (e < eNOERROR) ERR(handle, e);

    apage = (mlgf_DirectoryPage*)root_BCB->bufPagePtr;

    MLGF_INIT_DIRECTORY_PAGE(apage, *iid, *rootPid, 1, TRUE, kdesc->nKeys); 

    /*
     * Write log record.
     */
    if (logParam->logFlag & LOG_FLAG_DATA_LOGGING) {

        initDirPageInfo.iid = *iid;
        initDirPageInfo.height = 1;
        initDirPageInfo.rootFlag = TRUE;
        initDirPageInfo.nKeys = kdesc->nKeys;

        LOG_FILL_LOGRECINFO_1(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                              LOG_ACTION_MLGF_INIT_DIRECTORY_PAGE, LOG_REDO_ONLY,
                              apage->hdr.pid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                              sizeof(LOG_Image_MLGF_InitDirectoryPage_T), &initDirPageInfo);

        e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
        if (e < eNOERROR) ERRBL1(handle, e, root_BCB, PAGE_BUF);

        apage->hdr.lsn = lsn;
        apage->hdr.logRecLen = logRecLen;
    }

    root_BCB->dirtyFlag = 1;

    /* Unfix buffer for root page. */
    e = SHM_releaseLatch(handle, root_BCB->latchPtr, procIndex);
    if (e < eNOERROR) ERR(handle, e);

    e = BfM_unfixBuffer(handle, root_BCB, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);


    return(eNOERROR);

} /* MLGF_CreateIndex() */
