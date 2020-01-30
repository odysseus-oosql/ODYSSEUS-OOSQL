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
 * Module: BtM_FinalSortedBulkLoad.c
 *
 * Description:
 *  Finalize B+ tree index bulkload
 *
 * Exports:
 *  Four BtM_FinalSortedBulkLoad()
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
 * BtM_FinalSortedBulkLoad()
 *===========================*/
/*
 * Function: Four BtM_FinalSortedBulkLoad()
 *
 * Description:
 *  Finalize B+ tree index bulkload
 *
 * Returns:
 *  error code
 *    eBADPARAMETER
 *    some errors caused by function calls
 *
 * Side Effects:
 *
 */
Four BtM_FinalSortedBulkLoad (
    Four		    handle,
    XactTableEntry_T        *xactEntry,                             /* IN transaction table entry */
    Four                    btmBlkLdId,                             /* IN BtM bulkload ID */
    LogParameter_T          *logParam)                              /* IN log parameter */
{
    Four                    e;                                      /* error number */
    BtM_BlkLdTableEntry*    blkLdEntry;                             /* entry in which information about bulkload is saved */


    TR_PRINT(handle, TR_BTM, TR1,
            ("BtM_FinalSortedBulkLoad(xactEntry=%P, btmBlkLdId=%ld, logParam=%P)",
             xactEntry, btmBlkLdId, logParam));



    /*
    ** O. Check parameters
    */

    if (xactEntry == NULL)              ERR(handle, eBADPARAMETER);

    if (logParam == NULL)               ERR(handle, eBADPARAMETER);


    /*
    ** O. set entry for fast access
    */

    blkLdEntry = &BTM_BLKLD_TABLE(handle)[btmBlkLdId];


    /*
    ** I. End of bulkload
    */

    /* 1. case : end of duplicate key occurrence */
    if (blkLdEntry->btmBlkLdoidBuffer.hdr.nSlots != 0) {
        e = btm_BlkLdEndOidBuffer(handle, xactEntry, btmBlkLdId, logParam);
        if (e < 0)  ERR(handle, e);
    }

    /* 2. case : end of bulkload in overflow page */
    if (blkLdEntry->btmBlkLdoverflow == TRUE) {
        e = btm_BlkLdEndOverflow(handle, xactEntry, btmBlkLdId, logParam);
        if (e < 0)  ERR(handle, e);
    }

    /* 3. end of bulkload in leaf page */
    e = btm_BlkLdEndLeaf(handle, xactEntry, btmBlkLdId, logParam);
    if (e < 0)  ERR(handle, e);

    /* 4. end of bulkload in internal page */
    e = btm_BlkLdEndInternal(handle, xactEntry, btmBlkLdId, logParam);
    if (e < 0)  ERR(handle, e);

    /* 5. end of bulkload in write buffer */
    e = btm_BlkLdEndWriteBuffer(handle, xactEntry, btmBlkLdId, logParam);
    if (e < 0)  ERR(handle, e);



    /*
    ** II. Finalize buffer
    */

    /* 1. finalize internal node buffer */
    e = btm_BlkLdFinalInternalBuffer(handle, btmBlkLdId);
    if (e < 0)  ERR(handle, e);

    /* 2. finalize write buffer */
    e = btm_BlkLdFinalWriteBuffer(handle, btmBlkLdId);
    if (e < 0)  ERR(handle, e);



    /*
    ** III. Empty entry of SORT_STREAM_TABLE(handle)
    */

    blkLdEntry->isUsed = FALSE;



    return eNOERROR;

}   /* BtM_FinalSortedBulkLoad() */
