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
 * Module :	BtM_CreateIndex.c
 *
 * Description :
 *  Create the new B+ tree Index.
 *  We allocate the root page and initialize it.
 *
 * Exports:
 *  Four BtM_CreateIndex(Four, Four, PageID*)
 */


#include "common.h"
#include "error.h"
#include "trace.h"
#include "TM.h"
#include "RDsM.h"
#include "BfM.h"
#include "BtM.h"
#include "LOG.h"
#include "perProcessDS.h"
#include "perThreadDS.h"



/*@================================
 * BtM_CreateIndex( )
 *================================*/
/*
 * Function: Four BtM_CreateIndex(Four, Four, PageID*)
 *
 * Description :
 *  Create the new B+ tree Index.
 *  We allocate the root page and initialize it.
 *
 * Returns :
 *  Error code
 *    some errors caused by function calls
 *
 * Side effects:
 *  The parameter rootPid is filled with the new root page's PageID.
 */
Four BtM_CreateIndex(
    Four handle,
    XactTableEntry_T *xactEntry, /* IN transaction table entry */
    Four volNo,			/* IN volume where the new B+tree is placed */
    PageID *rootPid,		/* OUT root page of the newly created B+tree */
    SegmentID_T *pageSegmentID, /* OUT page segment ID */
    LogParameter_T *logParam) /* IN log parameter */
{
    Four e;			/* error number */
    BtreeLeaf *rpage;           /* Page Pointer to the root page */
    Buffer_ACC_CB *rpage_BCBP;	/* buffer access control block for root page */
    Lsn_T lsn;                  /* lsn of the newly written log record */
    Four logRecLen;             /* log record length */
    LOG_LogRecInfo_T logRecInfo; /* log record information */
    LOG_Image_BtM_InitLeafPage_T initLeafPageInfo;

    /* pointer for COMMON Data Structure of perThreadTable */
    COMMON_PerThreadDS_T *common_perThreadDSptr = COMMON_PER_THREAD_DS_PTR(handle);


    TR_PRINT(handle, TR_BTM, TR1, ("BtM_CreateIndex(volNo=%P, rootPid=%P)", volNo, rootPid));


    /*@ check parameters */
    if (volNo < 0) ERR(handle, eBADPARAMETER);

    if (rootPid == NULL) ERR(handle, eBADPARAMETER);


    /* Request the first page of the file */
    e = RDsM_CreateSegment(handle, xactEntry, volNo, pageSegmentID, PAGESIZE2, logParam);
    if (e < eNOERROR) ERR(handle, e);

    e = RDsM_AllocTrains(handle, xactEntry, volNo, pageSegmentID, (PageID *)NULL, 1, PAGESIZE2, TRUE, rootPid, logParam);
    if (e < eNOERROR) ERR(handle, e);

    e = BfM_fixNewBuffer(handle, rootPid, M_EXCLUSIVE, &rpage_BCBP, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);

    rpage = (BtreeLeaf *)rpage_BCBP->bufPagePtr;

    /* Initialize the page as a leaf and root(root : 6th parameter == TRUE) */
    BTM_INIT_LEAF_PAGE(rpage, *((IndexID*)rootPid), *rootPid, NIL, NIL, TRUE);

    /*
     * Write log record.
     */
    if (logParam->logFlag & LOG_FLAG_DATA_LOGGING) {

        initLeafPageInfo.iid = rpage->hdr.iid;
        initLeafPageInfo.rootFlag = TRUE;
        initLeafPageInfo.prevPage = rpage->hdr.prevPage;
        initLeafPageInfo.nextPage = rpage->hdr.nextPage;

        LOG_FILL_LOGRECINFO_1(logRecInfo, xactEntry->xactId, LOG_TYPE_UPDATE,
                              LOG_ACTION_BTM_INIT_LEAF_PAGE, LOG_REDO_ONLY,
                              *rootPid, xactEntry->lastLsn, common_perThreadDSptr->nilLsn,
                              sizeof(LOG_Image_BtM_InitLeafPage_T), &initLeafPageInfo);

        e = LOG_WriteLogRecord(handle, xactEntry, &logRecInfo, &lsn, &logRecLen);
        if (e < eNOERROR) ERR(handle, e); 

        rpage->hdr.lsn = lsn;
        rpage->hdr.logRecLen = logRecLen;
    } else {
        INCREASE_LSN_BY_ONE(rpage->hdr.lsn);
    }


    rpage_BCBP->dirtyFlag = 1;

    e = SHM_releaseLatch(handle, rpage_BCBP->latchPtr, procIndex);
    if (e < eNOERROR) ERR(handle, e); 

    e = BfM_unfixBuffer(handle, rpage_BCBP, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);

    return(eNOERROR);

} /* BtM_CreateIndex() */
