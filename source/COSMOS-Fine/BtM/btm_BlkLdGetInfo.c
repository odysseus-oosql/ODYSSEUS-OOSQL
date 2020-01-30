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
 * Module: btm_BlkLdGetInfo.c
 *
 * Description :
 *  This function sets information variables to be needed for bulkloading.
 *
 * Exports:
 *  Four btm_BlkLdGetInfo(ObjectID*, PageID*, KeyDesc*, Two, Two)
 */


#include "common.h"
#include "error.h"
#include "trace.h"
#include "Util_Sort.h"
#include "RDsM.h"
#include "BfM.h"
#include "BtM.h"
#include "BL_BtM.h"
#include "perThreadDS.h"
#include "perProcessDS.h"



/*@=====================
 * btm_BlkLdGetInfo()
 *=====================*/
/*
 * Function: Four btm_BlkLdGetInfo(ObjectID*, PageID*, KeyDesc*, Two, Two)
 *
 * Description:
 *  This function sets information variables to be needed for bulkloading.
 *
 * Returns:
 *  Error code
 *    some errors caused by function calls
 *
 * Side Effects:
 *  parameter blkldInfo is filled with given input parameter and other information
 */
Four btm_BlkLdGetInfo (
    Four		    handle,
    Four                    btmBlkLdId,         /* IN BtM bulkload ID */
    BtreeIndexInfo          *iinfo,             /* IN B tree information */
    PageID                  *root,              /* IN Index ID of index to be created */
    KeyDesc                 *kdesc,             /* IN key descriptor of B+ tree index */
    Two                     eff,                /* IN extent fill factor */
    Two                     pff)                /* IN page fill factor */
{
    Four                    e;                  /* error number */
    Four                    sizeOfExt;          /* size of extent for given volume */
    BtM_BlkLdTableEntry*    blkLdEntry;         /* entry in which information about BtM bulkload is saved */


    TR_PRINT(handle, TR_BTM, TR1,
            ("btm_BlkLdGetInfo(btmBlkLdId=%ld, iinfo=%P, root=%P, kdesc=%P, eff=%ld, pff=%ld)",
            btmBlkLdId, iinfo, root, kdesc, eff, pff));


    /* 0. set entry for fast access */
    blkLdEntry = &BTM_BLKLD_TABLE(handle)[btmBlkLdId];


    /* 2. get the extent size of this volume to allocate buffer */
    e = RDsM_GetSizeOfExt(handle, iinfo->iid.volNo, &sizeOfExt);
    if (e < eNOERROR) ERR(handle, e);


    /* 4. Set bulkload information variable */
    blkLdEntry->btmBlkLdblkldInfo.iinfo        = *iinfo;
    blkLdEntry->btmBlkLdblkldInfo.root         = *root;
    blkLdEntry->btmBlkLdblkldInfo.sizeOfExt    = sizeOfExt;
    blkLdEntry->btmBlkLdblkldInfo.eff          = eff;
    blkLdEntry->btmBlkLdblkldInfo.pff          = pff;
    blkLdEntry->btmBlkLdblkldInfo.isTmp        = iinfo->tmpIndexFlag;
    blkLdEntry->btmBlkLdblkldInfo.kdesc        = *kdesc;


    return eNOERROR;

}   /* btm_BlkLdGetInfo() */
