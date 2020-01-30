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
 * Module: LOT_Test.c
 *
 * Description:
 *
 * Exports:
 *  Four LOT_LengthCheck(Four, PageID*)
 *  Four LOT_StructCheck(Four, PageID*, Four)
 */


#include <stdio.h>
#include "common.h"
#include "error.h"
#include "trace.h"
#include "latch.h"
#include "TM.h"
#include "BfM.h"
#include "LOT.h"
#include "perProcessDS.h"
#include "perThreadDS.h"



/*@================================
 * LOT_LengthCheck( )
 *================================*/
/*
 * Function: Four LOT_LengthCheck(Four, PageID*)
 *
 * Description:
 *
 * Returns:
 *
 */
Four LOT_LengthCheck(
    Four 		handle,
    PageID 		*root)		/* IN root of a large object tree */
{
    Four 		e;
    L_O_T_INode 	*anode;
    Buffer_ACC_CB 	*anode_BCBP;
    Four 		i;
    PageID 		pid;
    Four 		count, len;


    e = BfM_getAndFixBuffer(handle, root, M_FREE, &anode_BCBP, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);

    anode = (L_O_T_INode *)anode_BCBP->bufPagePtr;

    if (anode->header.height > 1) {

	for (i = 0; i < anode->header.nEntries; i++) {

	    MAKE_PAGEID(pid, root->volNo, anode->entry[i].spid);

	    if ((len = LOT_LengthCheck(handle, &pid)) == -1) {
		LOT_DumpInternal(handle, root);
		ERRB1(handle, -1, anode_BCBP, PAGE_BUF);
	    } else if (len != lot_GetCount(handle, anode, i)) {
		printf("The %ldth-subtree length is wrong!!!\n", i);
		LOT_DumpTree(handle, root);
		ERRB1(handle, -1, anode_BCBP, PAGE_BUF);
	    }
	}
    }

    count = anode->entry[anode->header.nEntries-1].count;

    e = BfM_unfixBuffer(handle, anode_BCBP, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);

    return(count);

} /* LOT_LengthCheck() */



/*@================================
 * LOT_StructCheck( )
 *================================*/
/*
 * Function: Four LOT_StructCheck(Four, PageID*, Four)
 *
 * Description:
 *  Check if there is an underflowed node
 *
 * Returns:
 *
 * Side effects:
 */
Four LOT_StructCheck(
    Four handle,
    PageID *root,		/* IN root PageID */
    Four isroot)		/* IN  */
{
    Four e;
    L_O_T_INode *anode;
    Buffer_ACC_CB *anode_BCBP;
    Four i;
    Four count;
    PageID pid;


    e = BfM_getAndFixBuffer(handle, root, M_FREE, &anode_BCBP, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);

    anode = (L_O_T_INode *)anode_BCBP->bufPagePtr;

    if (anode->header.nEntries < LOT_HALFENTRIES) {
	if (!(isroot && anode->header.nEntries >= 2)) {
	    printf("There is an underflowed anode\n");
	    e = LOT_DumpTree(handle, root);
	    if (e < eNOERROR) ERRB1(handle, e, anode_BCBP, PAGE_BUF);
	}
    }

    for(i = 0; i < anode->header.nEntries; i++) {
	if (anode->header.height == 1) {
	    count = lot_GetCount(handle, anode, i);
	    if (count < LOT_LNODE_HALFFREE) {
		printf("There is an underflowed node\n");
		e = LOT_DumpTree(handle, root);
		if (e < eNOERROR) ERRB1(handle, e, anode_BCBP, PAGE_BUF);
	    }
	} else {
	    MAKE_PAGEID(pid, root->volNo, anode->entry[i].spid);
	    e = LOT_StructCheck(handle, &pid, 0);
	    if (e < eNOERROR) ERRB1(handle, e, anode_BCBP, PAGE_BUF);
	}
    }

    e = BfM_unfixBuffer(handle, anode_BCBP, PAGE_BUF);
    if (e < eNOERROR) ERR(handle, e);

} /* LOT_StructCheck( ) */
