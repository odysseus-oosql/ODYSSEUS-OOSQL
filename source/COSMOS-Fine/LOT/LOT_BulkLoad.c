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
 * Module: LOT_BulkLoad.c
 *
 * Description:
 *  APIs for direct access of large object such as bulk load.
 *
 * Exports:
 */

#include <assert.h>
#include "common.h"
#include "error.h"
#include "trace.h"
#include "LOT.h"
#include "perProcessDS.h"
#include "perThreadDS.h"
#include <string.h>	


/*@================================
 * LOT_BlkLd_CreateLargeObject( )
 *================================*/
/*
 * Function: Four LOT_BlkLd_CreateLargeObject( )
 *
 * Description:
 *
 * Returns:
 *  Error codes
 */
Four LOT_BlkLd_CreateLargeObject(
    Four             handle,
    XactTableEntry_T *xactEntry,       /* IN transaction table entry */
    DataFileInfo     *finfo,           /* IN file information */
    PageID           *nearPidForRoot,  /* IN where the root page is located */ 
    PageID           *root,            /* OUT root page ID */
    LogParameter_T   *logParam)        /* IN log parameter */
{
    Four             e;                /* error code */
    L_O_T_INode      dummyNode;        /* dummy node to get root page's number */
    Boolean          dummyFlag;

    /* create large object */
    e = LOT_CreateObject(handle, xactEntry, finfo, nearPidForRoot, (char*) &dummyNode, &dummyFlag, 0, 0, NULL, logParam);
    if (e < eNOERROR) ERR(handle, e);

    /* assertion check */
    assert (dummyFlag == FALSE);

    /* set output value */
    root->volNo = finfo->fid.volNo;
    root->pageNo = *((ShortPageID *)&dummyNode);

    return(eNOERROR);

} /* LOT_BlkLd_CreateLargeObject() */


/*@================================
 * LOT_BlkLd_AppendToObject( )
 *================================*/
/*
 * Function: Four LOT_BlkLd_AppendToObject( )
 *
 * Description:
 *
 * Returns:
 *  Error codes
 */
Four LOT_BlkLd_AppendToObject(
    Four             handle,
    XactTableEntry_T *xactEntry,        /* IN transaction table entry */
    DataFileInfo     *finfo,            /* IN file information */
    PageID           *root,             /* INOUT root page ID */
    Four             length,            /* IN amount of data to append */
    char             *data,             /* IN user buffer holding the data */
    LogParameter_T   *logParam)         /* IN log parameter */
{
    Four             e;                 /* error code */
    L_O_T_INode      anode; 
    Boolean          rootWithHdr_Flag;

    /* append data to large object */
    memcpy(&anode, &root->pageNo, sizeof(ShortPageID));
    rootWithHdr_Flag = FALSE;
    e = LOT_AppendToObject(handle, xactEntry, finfo, root, (char*)&anode, &rootWithHdr_Flag, 0, length, data, logParam);
    if (e < eNOERROR) ERR(handle, e);

    /* There is a possibiliy that root page was changed. */
    root->pageNo = *((ShortPageID *)&anode);

    return(eNOERROR);
}


/*@================================
 * LOT_BlkLd_WriteObject( )
 *================================*/
/*
 * Function: Four LOT_BlkLd_WriteObject( )
 *
 * Description:
 *
 * Returns:
 *  Error codes
 */
Four LOT_BlkLd_WriteObject(
    Four             handle,
    XactTableEntry_T *xactEntry,        /* IN transaction table entry */
    DataFileInfo     *finfo,            /* IN file information */
    PageID           *root,             /* IN root page ID */
    Four             start,             /* IN starting offset of write */
    Four             length,            /* IN amount of data to append */
    char             *data,             /* IN user buffer holding the data */
    LogParameter_T   *logParam)         /* IN log parameter */
{
    Four             e;                 /* error code */

    /* append data to large object */
    e = LOT_WriteObject(handle, xactEntry, finfo, (char*)&root->pageNo, FALSE, start, length, data, logParam);
    if (e < eNOERROR) ERR(handle, e);

    return(eNOERROR);
}

