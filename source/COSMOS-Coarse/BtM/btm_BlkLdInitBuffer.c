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
/*    Coarse-Granule Locking (Volume Lock) Version                            */
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
 * Module: btm_BlkLdInitBuffer.c
 *
 * Description :
 *  Initialize various kind of buffer and buffer page. 
 *  
 *
 * Exports:
 *  Four btm_BlkLdInitInternalBuffer()
 *  Four btm_BlkLdInitLeafBuffer()
 *  Four btm_BlkLdInitOverflowBuffer()
 *  Four btm_BlkLdInitWriteBuffer()
 *  Four btm_BlkLdDoubleSizeInternal()
 *  Four btm_BlkLdInitInternal(BtreeInternal*, Boolean)
 *  Four btm_BlkLdInitLeaf(BtreeLeaf*, Boolean)
 *  Four btm_BlkLdInitOverflow(BtreeOverflow*, Boolean)
 */


#include <stdlib.h> /* for malloc & free */
#include "common.h"
#include "trace.h"
#include "BfM.h"
#include "BtM_Internal.h"
#include "OM_Internal.h"
#include "BL_BtM_Internal.h"
#include "Util.h"
#include "Util_Sort.h"
#include "perThreadDS.h"
#include "perProcessDS.h"




/*@================================
 * btm_BlkLdInitInternalBuffer()
 *================================*/
/*
 * Function: Four btm_BlkLdInitInternalBuffer()
 *
 * Description:
 *  Initialize B+ tree internal buffer which will be used in index bulkloading.
 *
 * Returns:
 *  Error code
 *    some errors caused by function calls
 *
 * Side Effects:
 *
 */
Four btm_BlkLdInitInternalBuffer (
    Four handle,
    Four                    btmBlkLdId)             /* IN    BtM bulkload ID */ 
{                       
    Four                    e;                      /* error number */
    Four                    i;                      /* a loop index */
    Four                    j;                      /* a loop index */
    BtM_BlkLdTableEntry*    blkLdEntry;             /* entry in which information about BtM bulkload is saved */


    TR_PRINT(TR_BTM, TR1, ("btm_BlkLdInitInternalBuffer(handle)"));


    /* 0. set entry for fast access */
    blkLdEntry = &BTM_BLKLD_TABLE(handle)[btmBlkLdId]; /* insert a handle into BTM_BLKLD_TABLE */


    /* 1. alloc internal window buffer */
    e = Util_initVarArray(handle, &blkLdEntry->btmBlkLdiBuffers.iArray, sizeof(BlkLdBtreeInternal), INITNUMOFLEVEL);
    if (e < 0)  ERR(handle, e);

    blkLdEntry->btmBlkLdiBuffers.nLevel = INITNUMOFLEVEL;


    /* 2. initialize internal window buffer */
    for (i = 0; i < INITNUMOFLEVEL; i++) {
        for (j = 0; j < NUMOFWINDOWPAGE; j++) {
            e = btm_BlkLdInitInternal(handle, btmBlkLdId, &IARRAY(blkLdEntry->btmBlkLdiBuffers)[i].iPage[j], blkLdEntry->btmBlkLdblkldInfo.isTmp);
            if (e < 0)  ERR(handle, e);
        }
        IARRAY(blkLdEntry->btmBlkLdiBuffers)[i].iCount = 0;
    }

    return eNOERROR;

}  /* btm_BlkLdInitInternalBuffer() */



/*@================================
 * btm_BlkLdInitLeafBuffer()
 *================================*/
/*
 * Function: Four btm_BlkLdInitLeafBuffer()
 *
 * Description:
 *  Initialize B+ tree leaf buffer which will be used in index bulkloading.
 *
 * Returns:
 *  Error code
 *    some errors caused by function calls
 *
 * Side Effects:
 *
 */
Four btm_BlkLdInitLeafBuffer (
    Four handle,
    Four                    btmBlkLdId)             /* IN    BtM bulkload ID */ 
{
    Four                    e;                      /* error number */
    Four                    i;                      /* a loop index */
    BtM_BlkLdTableEntry*    blkLdEntry;             /* entry in which information about BtM bulkload is saved */


    TR_PRINT(TR_BTM, TR1, ("btm_BlkLdInitLeafBuffer(handle)"));


    /* set entry for fast access */
    blkLdEntry = &BTM_BLKLD_TABLE(handle)[btmBlkLdId]; /* insert a handle into BTM_BLKLD_TABLE */

    /* initialize leaf window buffer */
    for (i = 0; i < NUMOFWINDOWPAGE; i++) {
        e = btm_BlkLdInitLeaf(handle, btmBlkLdId, &blkLdEntry->btmBlkLdlBuffer.lPage[i], blkLdEntry->btmBlkLdblkldInfo.isTmp);
        if (e < 0)  ERR(handle, e);
    }
    blkLdEntry->btmBlkLdlBuffer.lCount = 0;

    return eNOERROR;

}  /* btm_BlkLdInitLeafBuffer() */



/*@================================
 * btm_BlkLdInitOverflowBuffer()
 *================================*/
/*
 * Function: Four btm_BlkLdInitOverflowBuffer()
 *
 * Description:
 *  Initialize B+ tree overflow buffer which will be used in index bulkloading.
 *
 * Returns:
 *  Error code
 *    some errors caused by function calls
 *
 * Side Effects:
 *
 */
Four btm_BlkLdInitOverflowBuffer (
    Four handle,
    Four                    btmBlkLdId)             /* IN    BtM bulkload ID */ 
{
    Four                    e;                      /* error number */
    Four                    i;                      /* a loop index */
    BtM_BlkLdTableEntry*    blkLdEntry;             /* entry in which information about BtM bulkload is saved */


    TR_PRINT(TR_BTM, TR1, ("btm_BlkLdInitOverflowBuffer(handle)"));


    /* set entry for fast access */
    blkLdEntry = &BTM_BLKLD_TABLE(handle)[btmBlkLdId]; /* insert a handle into BTM_BLKLD_TABLE */

    /* initialize overflow window buffer */
    for (i = 0; i < NUMOFWINDOWPAGE; i++) {
        e = btm_BlkLdInitOverflow(handle, btmBlkLdId, &blkLdEntry->btmBlkLdoBuffer.oPage[i], blkLdEntry->btmBlkLdblkldInfo.isTmp);
        if (e < 0)  ERR(handle, e);
    }
    blkLdEntry->btmBlkLdoBuffer.oCount = 0;


    return eNOERROR;

}  /* btm_BlkLdInitOverflowBuffer() */




/*@================================
 * btm_BlkLdInitWriteBuffer()
 *================================*/
/*
 * Function: Four btm_BlkLdInitWriteBuffer()
 *
 * Description:
 *  Initialize bulkload write buffer which will be used in index bulkloading.
 *
 * Returns:
 *  Error code
 *    some errors caused by function calls
 *
 * Side Effects:
 *
 */
Four btm_BlkLdInitWriteBuffer (
    Four handle,
    Four                    btmBlkLdId)             /* IN    BtM bulkload ID */ 
{
    Four                    e;                      /* error number */
    Four                    i;                      /* a loop index */
    Four                    j;                      /* a loop index */
    BtM_BlkLdTableEntry*    blkLdEntry;             /* entry in which information about BtM bulkload is saved */


    TR_PRINT(TR_BTM, TR1, ("btm_BlkLdInitWriteBuffer(handle)"));


    /* set entry for fast access */
    blkLdEntry = &BTM_BLKLD_TABLE(handle)[btmBlkLdId]; /* insert a handle into BTM_BLKLD_TABLE */


    /* initialize bulkload write buffer */
    blkLdEntry->btmBlkLdwBuffer.startPageIdx       = 0;
    blkLdEntry->btmBlkLdwBuffer.bufSize            = (blkLdEntry->btmBlkLdblkldInfo.sizeOfExt * blkLdEntry->btmBlkLdblkldInfo.eff) / 100;
    blkLdEntry->btmBlkLdwBuffer.allocedBufCount    = 0;
    blkLdEntry->btmBlkLdwBuffer.flushedBufCount    = 0;

    blkLdEntry->btmBlkLdwBuffer.allocCount         = 0;
    blkLdEntry->btmBlkLdwBuffer.allocPageIdArray   = (PageID *) malloc(sizeof(PageID) * blkLdEntry->btmBlkLdwBuffer.bufSize);


    for (i = 0; i < NUMOFWRITEBUFFER; i++) { 
        blkLdEntry->btmBlkLdwBuffer.flushCount[i]          = 0;

        blkLdEntry->btmBlkLdwBuffer.flushPageIdArray[i]    = (PageID *) malloc(sizeof(PageID) * blkLdEntry->btmBlkLdwBuffer.bufSize);
        for (j = 0; j < blkLdEntry->btmBlkLdwBuffer.bufSize; j++) {
            blkLdEntry->btmBlkLdwBuffer.flushPageIdArray[i][j].volNo   = NIL;
            blkLdEntry->btmBlkLdwBuffer.flushPageIdArray[i][j].pageNo  = NIL;
        }

        blkLdEntry->btmBlkLdwBuffer.bufArray[i]            = (BtreePage *) malloc(PAGESIZE * blkLdEntry->btmBlkLdwBuffer.bufSize);
    }


    return eNOERROR;

}  /* btm_BlkLdInitWriteBuffer() */




/*@================================
 * btm_BlkLdDoubleSizeInternal()
 *================================*/
/*
 * Function: Four btm_BlkLdDoubleSizeInternal()
 *
 * Description:
 *  Doubling internal window buffer size.
 *
 * Returns:
 *  Error code
 *    some errors caused by function calls
 *
 * Side Effects:
 *
 */
Four btm_BlkLdDoubleSizeInternal (
    Four handle,
    Four                    btmBlkLdId)             /* IN    BtM bulkload ID */ 
{
    Four                    e;                  /* error number */
    Four                    i;                  /* a loop index */
    Four                    j;                  /* a loop index */
    BtM_BlkLdTableEntry*    blkLdEntry;         /* entry in which information about BtM bulkload is saved */


    TR_PRINT(TR_BTM, TR1, ("btm_BlkLdDoubleSizeInternal(handle)"));


    /* 0. set entry for fast access */
    blkLdEntry = &BTM_BLKLD_TABLE(handle)[btmBlkLdId]; /* insert a handle into BTM_BLKLD_TABLE */


    /* 1. doubling internal window buffer */
    e = Util_doublesizeVarArray(handle, &blkLdEntry->btmBlkLdiBuffers.iArray, sizeof(BlkLdBtreeInternal));
    if (e < 0)  ERR(handle, e);


    /* 2. initialize internal window buffer */
    for (i = blkLdEntry->btmBlkLdiBuffers.nLevel; i < blkLdEntry->btmBlkLdiBuffers.nLevel*2; i++) {
        for (j = 0; j < NUMOFWINDOWPAGE; j++) {
            e = btm_BlkLdInitInternal(handle, btmBlkLdId, &IARRAY(blkLdEntry->btmBlkLdiBuffers)[i].iPage[j], blkLdEntry->btmBlkLdblkldInfo.isTmp);
            if (e < 0)  ERR(handle, e);
        }
        IARRAY(blkLdEntry->btmBlkLdiBuffers)[i].iCount = 0;
    }

    blkLdEntry->btmBlkLdiBuffers.nLevel += blkLdEntry->btmBlkLdiBuffers.nLevel;


    return eNOERROR;

}  /* btm_BlkLdDoubleSizeInternal() */




/*@================================
 * btm_BlkLdInitInternal()
 *================================*/
/*
 * Function: Four btm_BlkLdInitInternal(BtreeInternal*, Boolean)
 *
 * Description:
 *  Initialize buffer page as a internal page.
 *
 * Returns:
 *  Error code
 *    some errors caused by function calls
 *
 * Side Effects:
 *
 */
Four btm_BlkLdInitInternal (
    Four handle,
    Four                    btmBlkLdId,     /* IN    BtM bulkload ID */ 
    BtreeInternal           *page,          /* OUT */
    Boolean                 isTmp)          /* IN flag which indicates B+ tree is temporary or not */
{
    Four                    e;              /* error number */


    TR_PRINT(TR_BTM, TR1, 
            ("btm_BlkLdInitInternal(handle, page=%P, isTmp=%ld)", page, isTmp));


    /* Set page type */
    SET_PAGE_TYPE(page, BTREE_PAGE_TYPE);

    /* Assume bulkloaded B+ tree index is not temporary */
    RESET_TEMP_PAGE_FLAG(page);

    /* set temporary flag */
    if( isTmp ) SET_TEMP_PAGE_FLAG(page);
    else        RESET_TEMP_PAGE_FLAG(page);

    /* Set the Btree page type. */
    page->hdr.type = INTERNAL;

    /* Initialize variables */
    page->hdr.p0        = (ShortPageID)NIL;
    page->hdr.nSlots    = 0;
    page->hdr.free      = 0;
    page->hdr.unused    = 0;


    return eNOERROR;

}  /* btm_BlkLdInitInternal() */





/*@================================
 * btm_BlkLdInitLeaf()
 *================================*/
/*
 * Function: Four btm_BlkLdInitLeaf(BtreeLeaf*, Boolean)
 *
 * Description:
 *  Initialize buffer page as a leaf page.
 *
 * Returns:
 *  Error code
 *    some errors caused by function calls
 *
 * Side Effects:
 *
 */
Four btm_BlkLdInitLeaf (
    Four handle,
    Four                    btmBlkLdId,     /* IN    BtM bulkload ID */
    BtreeLeaf               *page,          /* OUT */
    Boolean                 isTmp)          /* IN flag which indicates B+ tree is temporary or not */
{
    Four                    e;              /* error number */


    TR_PRINT(TR_BTM, TR1, 
            ("btm_BlkLdInitLeaf(handle, page=%P, isTmp=%ld)", page, isTmp));


    /* Set page type */
    SET_PAGE_TYPE(page, BTREE_PAGE_TYPE);

    /* Assume bulkloaded B+ tree index is not temporary */
    RESET_TEMP_PAGE_FLAG(page);

    /* set temporary flag */
    if( isTmp ) SET_TEMP_PAGE_FLAG(page);
    else        RESET_TEMP_PAGE_FLAG(page);

    /* Set the Btree page type. */
    page->hdr.type = LEAF;

    /* Initialize variables */
    page->hdr.nextPage  = NIL;
    page->hdr.prevPage  = NIL;
    page->hdr.nSlots    = 0;
    page->hdr.free      = 0;
    page->hdr.unused    = 0;


    return eNOERROR;

}  /* btm_BlkLdInitLeaf() */




/*@================================
 * btm_BlkLdInitOverflow()
 *================================*/
/*
 * Function: Four btm_BlkLdInitOverflow(BtreeOverflow*, Boolean)
 *
 * Description:
 *  Initialize buffer page as a overflow page.
 *
 * Returns:
 *  Error code
 *    some errors caused by function calls
 *
 * Side Effects:
 *
 */
Four btm_BlkLdInitOverflow (
    Four handle,
    Four                    btmBlkLdId,     /* IN    BtM bulkload ID */ 
    BtreeOverflow           *page,          /* OUT */
    Boolean                 isTmp)          /* IN flag which indicates B+ tree is temporary or not */
{
    Four                    e;              /* error number */


    TR_PRINT(TR_BTM, TR1, 
            ("btm_BlkLdInitOverflow(handle, page=%P, isTmp=%ld)", page, isTmp));


    /* Set page type */
    SET_PAGE_TYPE(page, BTREE_PAGE_TYPE);

    /* Assume bulkloaded B+ tree index is not temporary */
    RESET_TEMP_PAGE_FLAG(page);

    /* set temporary flag */
    if( isTmp ) SET_TEMP_PAGE_FLAG(page);
    else        RESET_TEMP_PAGE_FLAG(page);

    /* Set the Btree page type. */
    page->hdr.type = OVERFLOW;

    /* Initialize variables */
    page->hdr.nObjects = 0;
    page->hdr.nextPage = (ShortPageID)NIL;
    page->hdr.prevPage = (ShortPageID)NIL;


    return eNOERROR;

}  /* btm_BlkLdInitOverflow() */


