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
 * Module: BtM_InitSortedBulkLoad.c
 *
 * Description:
 *  Initialize B+ tree index bulkloading
 *
 * Exports:
 *  Four BtM_InitSortedBulkLoad(ObjectID*, PageID*, KeyDesc*, Two, Two)
 *
 */


#include "common.h"
#include "error.h"
#include "trace.h"
#include "Util_Sort.h"
#include "BtM.h"
#include "BL_BtM.h"
#include "perThreadDS.h"
#include "perProcessDS.h"





/*@===========================
 * BtM_InitSortedBulkLoad()
 *===========================*/
/*
 * Function: Four BtM_InitSortedBulkLoad(ObjectID*, PageID*, KeyDesc*, Two, Two)
 *
 * Description:
 *  Initialize B+ tree index bulkloading
 *
 * Returns:
 *  BtM bulkload ID
 *  error code
 *    eBADPARAMETER
 *    some errors caused by function calls
 *
 * Side Effects:
 *
 */
Four BtM_InitSortedBulkLoad (
    Four		    handle,
    XactTableEntry_T        *xactEntry,                             /* IN transaction table entry */
    BtreeIndexInfo          *iinfo,                                 /* IN B tree information */
    PageID                  *root,                                  /* IN root PageID of index to be created */
    KeyDesc                 *kdesc,                                 /* IN key descriptor of the given B+ tree */
    Two                     eff,                                    /* IN extent fill factor */
    Two                     pff,                                    /* IN page fill factor */
    LogParameter_T          *logParam)                              /* IN log parameter */
{
    Four                    e;                                      /* error number */
    Four                    btmBlkLdId;                             /* BtM bulkload ID */
    BtM_BlkLdTableEntry*    blkLdEntry;                             /* entry in which information about bulkload is saved */


    TR_PRINT(handle, TR_BTM, TR1,
             ("BtM_InitSortedBulkLoad(xactEntry=%P, iinfo=%P, root=%P, kdesc=%P, eff=%ld, pff=%ld, logParam=%P)",
              xactEntry, iinfo, root, kdesc, eff, pff, logParam));


    /*
    ** O. Check parameters
    */

    if (xactEntry == NULL)              ERR(handle, eBADPARAMETER);

    if (iinfo== NULL)                   ERR(handle, eBADPARAMETER);

    if (root == NULL)                   ERR(handle, eBADPARAMETER);

    if (kdesc == NULL)                  ERR(handle, eBADPARAMETER);

    if (eff < 0 || eff > 100)           ERR(handle, eBADPARAMETER);

    if (pff < MINPFF || pff > MAXPFF)   ERR(handle, eBADPARAMETER);

    if (logParam == NULL)               ERR(handle, eBADPARAMETER);



    /*
    ** I. Find empty entry from BtM bulkload table
    */

    for (btmBlkLdId = 0; btmBlkLdId < BTM_BLKLD_TABLE_SIZE; btmBlkLdId++ ) {
	if (BTM_BLKLD_TABLE(handle)[btmBlkLdId].isUsed == FALSE) break;
    }
    if (btmBlkLdId == BTM_BLKLD_TABLE_SIZE) ERR(handle, eBLKLDTABLEFULL);

    /* set entry for fast access */
    blkLdEntry = &BTM_BLKLD_TABLE(handle)[btmBlkLdId];

    /* set isUsed flag */
    blkLdEntry->isUsed = TRUE;



    /*
    ** II. Get index information
    */

    /* 1. get information about B+ tree index to be bulkloaded */
    e = btm_BlkLdGetInfo(handle, btmBlkLdId, iinfo, root, kdesc, eff, pff);
    if (e < 0)  ERR(handle, e);



    /*
    ** III. Initialize buffer
    */

    /* 1. initialize internal node buffer which will be used in index bulkload */
    e = btm_BlkLdInitInternalBuffer(handle, btmBlkLdId);
    if (e < 0)  ERR(handle, e);

    /* 2. initialize leaf node buffer which will be used in index bulkload */
    e = btm_BlkLdInitLeafBuffer(handle, btmBlkLdId);
    if (e < 0)  ERR(handle, e);

    /* 3. initialize overflow node buffer which will be used in index bulkload */
    e = btm_BlkLdInitOverflowBuffer(handle, btmBlkLdId);
    if (e < 0)  ERR(handle, e);

    /* 4. initialize write buffer */
    e = btm_BlkLdInitWriteBuffer(handle, btmBlkLdId);
    if (e < 0)  ERR(handle, e);

    /* 5. initialize oid array buffer for duplicate key */
    e = btm_BlkLdInitLeaf(handle, btmBlkLdId, &blkLdEntry->btmBlkLdoidBuffer, blkLdEntry->btmBlkLdblkldInfo.isTmp);
    if (e < 0)  ERR(handle, e);

    /* 6. initialize overflow flag */
    blkLdEntry->btmBlkLdoverflow = FALSE;

    /* 7. initialize append flag */
    blkLdEntry->btmBlkLdisAppend = FALSE;



    return btmBlkLdId;

}   /* BtM_InitSortedBulkLoad() */
