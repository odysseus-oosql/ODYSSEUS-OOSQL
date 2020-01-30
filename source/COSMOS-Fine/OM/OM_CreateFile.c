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
 * Module:	OM_CreateFile.c
 *
 * Description :
 *  Create a new data file. The FileID - file identifier - is the same as
 *  the first page's PageID of the new segment.
 *  We reserve the first page. As doing so, we do not have to change
 *  the first page's PageID which is needed for the sequential access.
 *
 * Assume :
 *  File creating need x-lock on the new file.
 *  So, we don't need to lock the page.
 *
 * Exports:
 *  Four OM_CreateFile(Four, Four, Four, sm_CatOverlayForData*)
 *
 * Return Values:
 *  Error codes
 *    some errors caused by function calls
 *
 * Side effects:
 *  0) A new data file is created.
 *  1) parameter catEntry
 *      catEntry is set to the information for the newly create file.
 */


#include "common.h"
#include "error.h"
#include "trace.h"
#include "latch.h"
#include "LOG.h"
#include "RDsM.h"
#include "BfM.h"
#include "OM.h"
#include "perProcessDS.h"
#include "perThreadDS.h"


Four OM_CreateFile(
    Four handle,
    XactTableEntry_T *xactEntry, /* IN transaction table entry */
    FileID* fid,                 /* IN allocated file ID */ 
    sm_CatOverlayForData *catEntry, /* OUT File Information for the newly created file */
    LogParameter_T *logParam) /* IN log parameter */
{
    Four        e;		/* for the error number */
    PageID      pid;		/* first PageID of the new segment */
    SlottedPage *apage;		/* point to the data page */
    Buffer_ACC_CB *aPage_BCBP;  /* buffer access control block holding data */
    LockReply  lockReply;       /* lock reply */
    LockMode oldMode;
    SegmentID_T pageSegmentID;  /* page segment ID */
    SegmentID_T trainSegmentID;  /* train segment ID */


    TR_PRINT(handle, TR_OM, TR1,
	     ("OM_CreateFile(handle, fid=%ld, catEntry=%P)", fid, catEntry));

    /* allocate a page : first page should be reserved.
       As doing so, we do not have to update the first page's PageID
       which is needed for the sequential access.
       From this decision, we also use the first PageID as the FileID. */

    /* initialize segment ID */
    INIT_SEGMENT_ID(&pageSegmentID);
    INIT_SEGMENT_ID(&trainSegmentID);

    e = RDsM_CreateSegment(handle, xactEntry, fid->volNo, &pageSegmentID, PAGESIZE2, logParam);
    if (e < eNOERROR) ERR(handle, e);

    e = om_SetSegmentIDToCatOverlayForDataUsingCatOverlayForData(handle, catEntry, &pageSegmentID, PAGESIZE2);
    if (e < eNOERROR) ERR(handle, e);

    e = om_SetSegmentIDToCatOverlayForDataUsingCatOverlayForData(handle, catEntry, &trainSegmentID, TRAINSIZE2);
    if (e < eNOERROR) ERR(handle, e);

    e = RDsM_AllocTrains(handle, xactEntry, fid->volNo, &pageSegmentID, (PageID *)NULL, 1, PAGESIZE2, TRUE, &pid, logParam);
    if (e < eNOERROR) ERR(handle, e);

    /*
     * NOTICE: We should acquire the X lock on the new page; the page
     *         becomes the new last page of this file.
     *         The caller request the X lock on this page.
     */

    /* construct the catalog data structure */
    catEntry->fid = *fid;
    catEntry->firstPage = pid.pageNo;
    catEntry->lastPage = pid.pageNo;

#ifdef CCPL
    e = BfM_fixNewBuffer(handle, &pid, M_FREE, &aPage_BCBP, PAGE_BUF); 
    if (e < eNOERROR) ERR(handle, e);
#endif /* CCPL */

#ifdef CCRL
    e = BfM_fixNewBuffer(handle, &pid, M_EXCLUSIVE, &aPage_BCBP, PAGE_BUF); 
    if (e < eNOERROR) ERR(handle, e);
#endif /* CCRL */

    apage = (SlottedPage *)aPage_BCBP->bufPagePtr;

    e = om_InitSlottedPage(handle, xactEntry, apage, &(catEntry->fid), &pid, logParam);
    if (e < eNOERROR) {
#ifdef CCPL
        ERRB1(handle, e, aPage_BCBP, PAGE_BUF);
#endif /* CCPL */

#ifdef CCRL
        ERRBL1(handle, e, aPage_BCBP, PAGE_BUF);
#endif /* CCRL */
    }

    /* set dirty */
    aPage_BCBP->dirtyFlag = 1;

#ifdef CCRL
    e = SHM_releaseLatch(handle, aPage_BCBP->latchPtr, procIndex);
    if (e < eNOERROR) ERR(handle, e);
#endif /* CCRL */

    e = BfM_unfixBuffer(handle, aPage_BCBP, PAGE_BUF); 
    if (e < eNOERROR) ERR(handle, e);

    return(eNOERROR);

} /* OM_CreateFile() */
