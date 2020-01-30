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
 * Module : om_GetPhysicalFileID.c
 *
 * Description :
 *  Get physical file ID of given file
 *
 * Exports :
 *  Four om_GetPhysicalFileID(Four, XactTableEntry_T*, DataFileInfo*, PhysicalFileID*, LockParameter*, LogParameter_T*)
 */

#include "common.h"
#include "error.h"
#include "trace.h"              /* for tracing : TR_PRINT(handle, ) macro */
#include "latch.h"
#include "LOG.h"
#include "BfM.h"                /* for the buffer manager call */
#include "OM.h"
#include "LM.h"
#include "TM.h"
#include "perProcessDS.h"
#include "perThreadDS.h"


/* ========================================
 *  om_GetPhysicalFileID( )
 * =======================================*/

/*
 * Function om_GetPhysicalFileID(XactTableEntry_T*, DataFileInfo*, PhysicalFileID*, LockParameter*, LogParameter_T*)
 *
 * Description :
 *  Sort a data file.
 *
 * Return Values :
 *  Error Code.
 *
 * Side Effects :
 *  None
 */
Four om_GetPhysicalFileID(
    Four 	       handle,
    XactTableEntry_T*  xactEntry,       /* IN  transaction table entry */
    DataFileInfo*      finfo,           /* IN  information of file */
    PhysicalFileID*    pFid,            /* OUT physical file ID */
    LockParameter*     lockup)          /* IN  request lock or not  */
{
    Four               e;               /* error code */
    Boolean            checkFlag;       /* TRUE when we should do the check */
    LockReply          lockReply;       /* lock reply */
    LockMode           oldMode;
    PhysicalFileID     tmp_pFid;
    Buffer_ACC_CB*     catPage_BCBP;    /* buffer access control block holding catalog data */
    SlottedPage*       catPage;         /* pointer to buffer containing the catalog */
    sm_CatOverlayForData* catEntry;     /* pointer to data file catalog information */


    /* get physical file ID */
    if (finfo->tmpFileFlag) {
        /* temporary file */
        MAKE_PHYSICALFILEID(*pFid, finfo->fid.volNo, finfo->catalog.entry->data.firstPage);
    } else {

        /* ordinary file; get the first page from the catalog table */
#ifdef CCPL
        for (checkFlag = FALSE; ; checkFlag = TRUE) {

            /* Request X lock on the page where the catalog entry resides. */
            if (lockup != NULL) {
                e = LM_getFlatPageLock(handle, &xactEntry->xactId, (PageID*)&finfo->catalog.oid,
                                       L_S, L_MANUAL, L_UNCONDITIONAL, &lockReply, &oldMode);
                if (e < eNOERROR) ERR(handle, e);

                if (lockReply == LR_DEADLOCK) {
                    ERR(handle, eDEADLOCK); /* deadlock */
                }
            }

            /* get the first page in catalog */
            e = BfM_getAndFixBuffer(handle, (TrainID*)&finfo->catalog.oid, M_FREE, &catPage_BCBP, PAGE_BUF);
            if (e < eNOERROR) ERR(handle, e);

            catPage = (SlottedPage *)catPage_BCBP->bufPagePtr;
            GET_PTR_TO_CATENTRY_FOR_DATA(finfo->catalog.oid.slotNo, catPage, catEntry);

            /* get the physical file ID */
            MAKE_PHYSICALFILEID(*pFid, finfo->fid.volNo, catEntry->firstPage);

            e = BfM_unfixBuffer(handle, catPage_BCBP, PAGE_BUF);
            if(e < eNOERROR) ERR(handle, e);

            /* Release the lock on the catalog page. */
            if (lockup != NULL) {
                e = LM_releaseFlatPageLock(handle, &xactEntry->xactId, (PageID*)&finfo->catalog.oid, L_MANUAL);
                if (e < eNOERROR) ERR(handle, e);
            }

            if (checkFlag) {
                /* Be sure we get the correct first page. */
                /* The confirmation is required because we released the latch. */
                if(EQUAL_PHYSICALFILEID(*pFid, tmp_pFid)) break;

                if(lockup){
                    e = LM_releasePageLock(handle, &xactEntry->xactId, &tmp_pFid, L_MANUAL);
                    if (e < eNOERROR) ERR(handle, e);
                }
            }

            tmp_pFid = *pFid;

            if(lockup){
                e = LM_getPageLock(handle, &xactEntry->xactId, &tmp_pFid, &finfo->fid,
                                   L_S, L_MANUAL, L_UNCONDITIONAL, &lockReply, &oldMode);
                if (e < eNOERROR) ERR(handle, e);

                if(lockReply == LR_DEADLOCK){
                    ERR(handle, eDEADLOCK); /* deadlock */
                }
            }
        } /* end of for loop */
#endif /* CCPL */

#ifdef CCRL
        /* get the last page in catalog */
        e = BfM_getAndFixBuffer(handle, (TrainID*)&finfo->catalog.oid, M_SHARED, &catPage_BCBP, PAGE_BUF);
        if (e < eNOERROR) ERR(handle, e);

        catPage = (SlottedPage *)catPage_BCBP->bufPagePtr;
        GET_PTR_TO_CATENTRY_FOR_DATA(finfo->catalog.oid.slotNo, catPage, catEntry);

        /* get the physical file ID */
        MAKE_PHYSICALFILEID(*pFid, finfo->fid.volNo, catEntry->firstPage);

        e = SHM_releaseLatch(handle, catPage_BCBP->latchPtr, procIndex);
        if (e < eNOERROR) ERRB1(handle, e, catPage_BCBP, PAGE_BUF);

        e = BfM_unfixBuffer(handle, catPage_BCBP, PAGE_BUF);
        if(e < eNOERROR) ERR(handle, e);
#endif /* CCRL */

        /* Now, we got the physical file ID */
    }


    return(eNOERROR);
}
