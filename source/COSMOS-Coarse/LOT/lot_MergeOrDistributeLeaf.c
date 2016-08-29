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
 * Module: lot_MergeOrDistributeLeaf.c
 *
 * Description:
 *  Merge or distributes bytes in two nodes. 
 *
 * Export:
 *  Four lot_MergeOrDistributeLeaf(ObjectID*, L_O_T_ItemList*, Boolean*, Boolean*)
 */


#include <string.h>
#include "common.h"
#include "RDsM_Internal.h"	
#include "trace.h"
#include "Util.h"
#include "BfM.h"
#include "LOT_Internal.h"
#include "perThreadDS.h"
#include "perProcessDS.h"



/*@================================
 * lot_MergeOrDistributeLeaf()
 *================================*/
/*
 * Function: Four lot_MergeOrDistributeLeaf(ObjectID*, L_O_T_ItemList*, Boolean*, Boolean*)
 *
 * Description:
 *  Merge or distributes bytes in two nodes. If the bytes can be entered
 * into one leaf node then merge occur. Otherwise the redistribution will
 * be occurred.
 *
 * Returns:
 *  error code
 *    some errors caused by function calls
 *
 * Note:
 *  The parameter 'catObjForFile' is required for passing the volume number.
 *  The volume number is necessary because the large object tree uses ShortPageID.
 */
Four lot_MergeOrDistributeLeaf(
    Four handle,
    ObjectID                    *catObjForFile, /* IN information for the file */
    L_O_T_ItemList              *list,          /* INOUT information of two leaf nodes */
    Boolean                     *uf,            /* OUT underflow flag */
    Boolean                     *mf,            /* OUT merge flag */
    Pool                        *dlPool,        /* INOUT pool of dealloc list elements */
    DeallocListElem             *dlHead)        /* INOUT head of the dealloc list */
{
    Four                        e;              /* error number */
    PageID                      l_pid;          /* PageID of the left node */
    PageID                      r_pid;          /* PageID of the right node */
    L_O_T_LNode                 *l_node;        /* pointer to the left node */
    L_O_T_LNode                 *r_node;        /* pointer to the right node */
    Four                        totalBytes;     /* sum of bytes in two node */
    Four                        l_count;        /* # of original bytes in left node */
    Four                        r_count;        /* # of original bytes in right node */
    Four                        movedBytes;     /* # of bytes moved from left to right */
    Four                        volNo;          /* volume in which the object is placed */
    SlottedPage                 *catPage;       /* buffer page containing the catalog object */
    sm_CatOverlayForData        *catEntry;      /* overay structure for catalog object access */
    DeallocListElem             *dlElem;        /* an element of the dealloc list */


    TR_PRINT(TR_LOT, TR1,
             ("lot_MergeOrDistributeLeaf(handle, catObjForFile=%P, list=%P, uf=%P, mf=%P)",
	      catObjForFile, list, uf, mf));

    /* Get the volume number from the catalog object. */
    e = BfM_GetTrain(handle, (TrainID*)catObjForFile, (char**)&catPage, PAGE_BUF);
    if (e < 0) ERR(handle, e);

    GET_PTR_TO_CATENTRY_FOR_DATA(catObjForFile, catPage, catEntry);

    volNo = catEntry->fid.volNo;
    
    e = BfM_FreeTrain(handle, (TrainID*)catObjForFile, PAGE_BUF);
    if (e < 0) ERR(handle, e);
    
    MAKE_PAGEID(l_pid, volNo, list->entry[0].spid);
    e = BfM_GetTrain(handle, &l_pid, (char **)&l_node, LOT_LEAF_BUF);
    if (e < 0) ERR(handle, e);

    MAKE_PAGEID(r_pid, volNo, list->entry[1].spid);
    e = BfM_GetTrain(handle, &r_pid, (char **)&r_node, LOT_LEAF_BUF);
    if (e < 0) ERRB1(handle, e, &l_pid, LOT_LEAF_BUF);

    l_count = list->entry[0].count;
    r_count = list->entry[1].count;

    /*@ merge or distribute */
    if (l_count + r_count<= LOT_LNODE_MAXFREE) {
	/* merge two nodes into the left node */

	memcpy(&l_node->data[l_count], &r_node->data[0], r_count);
	
	list->entry[0].count += r_count;
	list->entry[1].count = 0;

	*mf = TRUE;
	*uf = (list->entry[0].count < LOT_LNODE_HALFFREE) ? TRUE:FALSE;

    } else {
	/* distribute the bytes evenly */

	totalBytes = l_count + r_count;

	/* movedBytes is the # of bytes moved from the left node
	   to the right node */
	movedBytes = l_count - (totalBytes/2 + totalBytes%2);

	if (movedBytes > 0) {
	    /* move the bytes from the left node to the right node */
	    
	    memmove(&r_node->data[movedBytes], &r_node->data[0], r_count);
	    memcpy(&r_node->data[0], &l_node->data[l_count-movedBytes], movedBytes);

	} else {
	    /* move the bytes form the right node to the left node */

	    memcpy(&l_node->data[l_count], &r_node->data[0], -movedBytes);
	    memmove(&r_node->data[0], &r_node->data[-movedBytes], r_count+movedBytes);	    
	}
	
	list->entry[0].count -= movedBytes;
	list->entry[1].count += movedBytes;

	*mf = FALSE;
	*uf = FALSE;
    }

    e = BfM_SetDirty(handle, &l_pid, LOT_LEAF_BUF);
    if (e < 0) ERRB2(handle, e, &l_pid, LOT_LEAF_BUF, &r_pid, LOT_LEAF_BUF);
    
    e = BfM_FreeTrain(handle, &l_pid, LOT_LEAF_BUF);
    if (e < 0) ERRB1(handle, e, &r_pid, LOT_LEAF_BUF);

    if (*mf) {
	e = BfM_FreeTrain(handle, &r_pid, LOT_LEAF_BUF);
	if (e < 0) ERR(handle, e);

        /* remove page from buffer pool */
        e = BfM_RemoveTrain(handle, &r_pid, LOT_LEAF_BUF, FALSE);
        if (e < 0) ERR(handle, e);

        e = Util_getElementFromPool(handle, dlPool, &dlElem);
        if (e < 0) ERR(handle, e);

        dlElem->type = DL_TRAIN;
        dlElem->elem.pid = r_pid; /* save the PageID. */
        dlElem->next = dlHead->next; /* insert to the list */
        dlHead->next = dlElem;       /* new first element of the list */
	
    } else {
	e = BfM_SetDirty(handle, &r_pid, LOT_LEAF_BUF);
	if (e < 0) ERRB1(handle, e, &r_pid, LOT_LEAF_BUF);
	
	e = BfM_FreeTrain(handle, &r_pid, LOT_LEAF_BUF);
	if (e < 0) ERR(handle, e);
    }
    
    return(eNOERROR);

} /* lot_MergeOrDistributeLeaf() */
