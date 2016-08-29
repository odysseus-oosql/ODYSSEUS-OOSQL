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
 * Module: BtM_GetStatistics_BtreePageInfo.c
 *
 * Description : 
 *
 * Exports:
 */


#include <string.h>
#include "common.h"
#include "trace.h"
#include "BfM.h"
#include "OM_Internal.h"	/* for SlottedPage containing catalog object */
#include "BtM_Internal.h"
#include "perThreadDS.h"
#include "perProcessDS.h"


Four btm_GetBtreeInfo(Four, BtreePage *, Four *, Four *, Four *);


/*@================================
 * BtM_GetBtreeInfo()
 *================================*/
/*
 * Function: Four      BtM_GetBtreeInfo(PageID*, Four, Four*, Four*)
 *
 * Description:
 *
 * Returns:
 *  Error code
 *    eBADBTREEPAGE_BTM
 *    some errors caused by function calls
 */
Four BtM_GetBtreeInfo(
    Four handle,
    PageID	 *iid,	   	    /* IN the root of a Btree */
    Four         *depth,            /* OUT */
    Four         *numLeafPage,      /* OUT */
    Four         *numOID)           /* OUT */
{
    Four e;			    /* error number */
    BtreePage *apage;		    /* a pointer to the root page */
    

    /* initialize parameter */
    *depth = 0;
    *numLeafPage = 0;
    *numOID = 0;

    /* get page into the buffer */
    e = BfM_GetTrain(handle, iid, (char **)&apage, PAGE_BUF);
    if (e < 0) ERR(handle, e);

    e = btm_GetBtreeInfo(handle, apage, depth, numLeafPage, numOID);
    if (e < 0) ERR(handle, e);

    /* free buffer */
    e = BfM_FreeTrain(handle, iid, PAGE_BUF);
    if (e < 0) ERR(handle, e);
    
    return eNOERROR;
}

Four btm_GetBtreeInfo(
    Four handle,
    BtreePage           *apage,            /* IN the root of a Btree */
    Four                *depth,            /* OUT */
    Four                *numLeafPage,      /* OUT */
    Four                *numOID)           /* OUT */
{
    Four                e;
    Two                 idx;               /* index for the given key value */
    PageID              childPid;          /* a overflow PageID */
    btm_InternalEntry   *iEntry;           /* an internal entry */
    Two                 iEntryOffset;      /* starting offset of an internal entry */
    btm_LeafEntry       *lEntry;           /* an internal entry */
    Two                 lEntryOffset;      /* starting offset of an internal entry */
    Two                 alignedKlen;
    BtreePage           *childPage;        /* child page */

    /* error check */
    if (apage->any.hdr.type & INTERNAL || apage->any.hdr.type & ROOT) {	/* Internal */
	
	/* for each child page */
	for( idx=0; idx < apage->bi.hdr.nSlots; idx++ ) {

            iEntryOffset = apage->bi.slot[-idx];
  	    iEntry = (btm_InternalEntry*)&(apage->bi.data[iEntryOffset]);
	    MAKE_PAGEID(childPid, apage->bi.hdr.pid.volNo, iEntry->spid);
	
            /* get page into the buffer */
            e = BfM_GetTrain(handle, &childPid, (char **)&childPage, PAGE_BUF);
            if (e < 0) ERR(handle, e);

            /* recursive call */
            e = btm_GetBtreeInfo(handle, childPage, depth, numLeafPage, numOID);
            if (e < 0) ERR(handle, e);

            /* free buffer */
            e = BfM_FreeTrain(handle, &childPid, PAGE_BUF);
            if (e < 0) ERR(handle, e);
        }
	
	(*depth)++;

    } else if( apage->any.hdr.type & LEAF ) {
	
	/* update 'numLeafPage' */
	(*numLeafPage)++;

	/* update 'numOID' */
	for( idx=0; idx < apage->bl.hdr.nSlots; idx++ ) {

            lEntryOffset = apage->bl.slot[-idx];
  	    lEntry = (btm_LeafEntry*)&(apage->bl.data[lEntryOffset]);
            alignedKlen = ALIGNED_LENGTH(lEntry->klen);
	
            if (lEntry->nObjects < 0) {      /* overflow page */
                printf("Fatal!!\n");
            }
            else {
                (*numOID) += lEntry->nObjects;
            }
        }

    } else
	ERR(handle, eBADBTREEPAGE_BTM);
    

    return(eNOERROR);
    
}   /* BtM_GetStatistics_BtreePageInfo() */
