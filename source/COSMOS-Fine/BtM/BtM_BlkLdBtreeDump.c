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
 * Module: BtM_BlkLdBtreeDump.c
 *
 * Description :
 *
 * Exports:
 */


#include "common.h"
#include "error.h"
#include "trace.h"
#include "BfM.h"
#include "BtM.h"
#include "BL_BtM.h"
#include "perThreadDS.h"
#include "perProcessDS.h"


Four btm_BlkLdBtreeDump(Four, BtreePage*, Four*, Four*, Four*, Boolean*, Boolean);


/*@================================
 * BtM_BlkLdBtreeDump()
 *================================*/
/*
 * Function: Four BtM_BlkLdBtreeDump(PageID*)
 *
 * Description:
 *
 * Returns:
 *  Error code
 *    eBADBTREEPAGE_BTM
 *    some errors caused by function calls
 */
Four BtM_BlkLdBtreeDump (
    Four	    handle,
    PageID          *iid,           /* IN the root of a Btree */
    Boolean         detail)         /* IN detail display flag */
{
    Four            e;              /* error number */
    BtreePage       *apage;         /* a pointer to the root page */
    Buffer_ACC_CB   *catPage_BCBP;  /* buffer access control block holding catalog data */
    Four            depth;
    Four            numLeafPage;
    Four            numOID;
    Boolean         bottom;


    /* initialize parameter */
    depth = 0;
    numLeafPage = 0;
    numOID = 0;
    bottom = FALSE;

    /* get page into the buffer */
    e = BfM_getAndFixBuffer(handle, iid, M_FREE, &catPage_BCBP, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);

    apage = (BtreePage*)catPage_BCBP->bufPagePtr;


    e = btm_BlkLdBtreeDump(handle, apage, &depth, &numLeafPage, &numOID, &bottom, detail);
    if (e < eNOERROR) ERR(handle, e);


    /* free buffer */
    e = BfM_unfixBuffer(handle, catPage_BCBP, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);

    printf("## depth        = %ld\n", depth);
    printf("## numLeafPage  = %ld\n", numLeafPage);

    return eNOERROR;
}


Four btm_BlkLdBtreeDump(
    Four		handle,
    BtreePage           *apage,             /* IN the root of a Btree */
    Four                *depth,             /* OUT */
    Four                *numLeafPage,       /* OUT */
    Four                *numOID,            /* OUT */
    Boolean             *bottom,            /* INOUT */
    Boolean             detail)             /* IN */
{
    Four                e;
    Four                idx;                /* index for the given key value */

    PageID              childPid;           /* a child PageID */
    PageID              overPid;            /* a overflow PageID */
    ShortPageID         overSpid;           /* a overflow ShortPageID */

    BtreePage           *childPage;         /* child page */
    BtreePage           *overPage;          /* overflow page */
    Buffer_ACC_CB       *catPage_BCBP;      /* buffer access control block holding catalog data */

    Four                alignedKlen;
    Four                iEntryOffset;       /* starting offset of an internal entry */
    Four                lEntryOffset;       /* starting offset of an internal entry */
    btm_InternalEntry   *iEntry;            /* an internal entry */
    btm_LeafEntry       *lEntry;            /* an internal entry */



    if (apage->any.hdr.type & INTERNAL) {   /* Internal */

        /* dump current internal node */
        if (detail) {
            e = btm_dumpBtreePage(handle, &apage->any.hdr.pid);
            if (e < eNOERROR) ERR(handle, e);
        }

        /* increase B+ tree depth */
        if (*bottom == FALSE)   (*depth)++;

        /* for each child page */
        MAKE_PAGEID(childPid, apage->bi.hdr.pid.volNo,  apage->bi.hdr.p0);

        /* get page into the buffer */
        e = BfM_getAndFixBuffer(handle, &childPid, M_FREE, &catPage_BCBP, PAGE_BUF);
        if (e < eNOERROR) ERR(handle, e);

        childPage = (BtreePage*)catPage_BCBP->bufPagePtr;

        /* recursive call */
        e = btm_BlkLdBtreeDump(handle, childPage, depth, numLeafPage, numOID, bottom, detail);
        if (e < eNOERROR) ERR(handle, e);

        /* free buffer */
        e = BfM_unfixBuffer(handle, catPage_BCBP, PAGE_BUF);
        if (e < eNOERROR) ERR(handle, e);

        for( idx=0; idx < apage->bi.hdr.nSlots; idx++ ) {

            iEntryOffset = apage->bi.slot[-idx];
            iEntry = (btm_InternalEntry*)&(apage->bi.data[iEntryOffset]);
            MAKE_PAGEID(childPid, apage->bi.hdr.pid.volNo, iEntry->spid);

            /* get page into the buffer */
            e = BfM_getAndFixBuffer(handle, &childPid, M_FREE, &catPage_BCBP, PAGE_BUF);
            if (e < eNOERROR) ERR(handle, e);

            childPage = (BtreePage*)catPage_BCBP->bufPagePtr;

            /* recursive call */
            e = btm_BlkLdBtreeDump(handle, childPage, depth, numLeafPage, numOID, bottom, detail);
            if (e < eNOERROR) ERR(handle, e);

            /* free buffer */
            e = BfM_unfixBuffer(handle, catPage_BCBP, PAGE_BUF);
            if (e < eNOERROR) ERR(handle, e);
        }

    }
    else if ( apage->any.hdr.type & LEAF ) {

        /* update 'numLeafPage' */
        (*numLeafPage)++;

        if (detail) {
            e = btm_dumpBtreePage(handle, &apage->any.hdr.pid);
            if (e < eNOERROR) ERR(handle, e);
        }

        /* increase B+ tree depth */
        if (*bottom == FALSE) {
            (*depth)++;
            *bottom = TRUE;
        }

        /* update 'numOID' */
        for( idx=0; idx < apage->bl.hdr.nSlots; idx++ ) {

            lEntryOffset = apage->bl.slot[-idx];
            lEntry = (btm_LeafEntry*)&(apage->bl.data[lEntryOffset]);
            alignedKlen = ALIGNED_LENGTH(lEntry->klen);
            overSpid = *((ShortPageID*)&(lEntry->kval[alignedKlen]));

            if (lEntry->nObjects < 0) {      /* overflow page */

                MAKE_PAGEID(overPid, apage->bl.hdr.pid.volNo, overSpid);

                /* get page into the buffer */
                e = BfM_getAndFixBuffer(handle, &overPid, M_FREE, &catPage_BCBP, PAGE_BUF);
                if (e < eNOERROR) ERR(handle, e);

                overPage = (BtreePage*)catPage_BCBP->bufPagePtr;


                /* recursive call */
                e = btm_BlkLdBtreeDump(handle, overPage, depth, numLeafPage, numOID, bottom, detail);
                if (e < eNOERROR) ERR(handle, e);

                /* free buffer */
                e = BfM_unfixBuffer(handle, catPage_BCBP, PAGE_BUF);
                if (e < eNOERROR) ERR(handle, e);

            }
        }
    }
    else if (apage->any.hdr.type & OVERFLOW) {

        if (detail) {
            e = btm_dumpBtreePage(handle, &apage->any.hdr.pid);
            if (e < eNOERROR) ERR(handle, e);
        }

        if (apage->bo.hdr.nextPage != NIL) {
            MAKE_PAGEID(overPid, apage->bo.hdr.pid.volNo, apage->bo.hdr.nextPage);

            /* get page into the buffer */
            e = BfM_getAndFixBuffer(handle, &overPid, M_FREE, &catPage_BCBP, PAGE_BUF);
            if (e < eNOERROR) ERR(handle, e);

            overPage = (BtreePage*)catPage_BCBP->bufPagePtr;


            /* recursive call */
            e = btm_BlkLdBtreeDump(handle, overPage, depth, numLeafPage, numOID, bottom, detail);
            if (e < eNOERROR) ERR(handle, e);

            /* free buffer */
            e = BfM_unfixBuffer(handle, catPage_BCBP, PAGE_BUF);
            if (e < eNOERROR) ERR(handle, e);
        }
    }


    return eNOERROR;

}   /* btm_BlkLdBtreeDump() */
