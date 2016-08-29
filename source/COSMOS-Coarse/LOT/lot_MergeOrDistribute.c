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
 * Module: lot_MergeOrDistribute.c
 *
 * Description:
 *  Merge or distribute the entries in the two internal nodes.
 *
 * Exports:
 *  Four lot_MergeOrDistribute(ObjectID*, L_O_T_ItemList*, Four*, Boolean*, Boolean*)
 */


#include <string.h>
#include "common.h"
#include "trace.h"
#include "Util.h"
#include "RDsM_Internal.h"	
#include "BfM.h"
#include "LOT_Internal.h"
#include "perThreadDS.h"
#include "perProcessDS.h"



/*@================================
 * lot_MergeOrDistribute()
 *================================*/
/*
 * Function: Four lot_MergeOrDistribute(ObjectID*, L_O_T_ItemList*, Four*, Boolean*, Boolean*)
 *
 * Description:
 *  Merge or distribute the entries in the two internal nodes. If the entries
 * can be merged into one node, the entries are merged in the left node.
 * Otherwise the entries are evenly distributed into two nodes. If the number
 * of entries are odd, the left node has one more entry.
 *
 * Returns:
 *  error code
 *    some errors caused by function calls
 *
 * Note:
 *  The parameter 'catObjForFile' is required for passing the volume number.
 *  The volume number is necessary because the large object tree uses ShortPageID.
 */
Four lot_MergeOrDistribute(
    Four handle,
    ObjectID                    *catObjForFile, /* IN information for the file */
    L_O_T_ItemList              *list,          /* INOUT information two nodes */
    Four                        *movedEntries,  /* OUT # of entries moved from left to right */
    Boolean                     *uf,            /* OUT underflow flag */
    Boolean                     *mf,            /* OUT merge flag */
    Pool                        *dlPool,        /* INOUT pool of dealloc list elements */
    DeallocListElem             *dlHead)        /* INOUT head of the dealloc list */
{
    Four                        e;              /* error number */
    PageID                      l_pid;          /* PageID of the left node */
    PageID                      r_pid;          /* PageID of the right node */
    L_O_T_INode                 *l_node;        /* pointer to the left node */
    L_O_T_INode                 *r_node;        /* pointer to the right node */
    Four                        totalEntries;   /* sum of # of entries in two node */
    Four                        l_nEntries;     /* # of original entries in left node */
    Four                        r_nEntries;     /* # of original entries in right node */
    Four                        volNo;          /* volume in which the object is placed */
    SlottedPage                 *catPage;       /* buffer page containing the catalog object */
    sm_CatOverlayForData        *catEntry;      /* overay structure for catalog object access */
    Four                        i;
    DeallocListElem             *dlElem;        /* an element of the dealloc list */

    
    TR_PRINT(TR_LOT, TR1,
             ("lot_MergeOrDistribute(handle, catObjForFile=%P, list=%P, uf=%P, mf=%P)",
	      catObjForFile, list, uf, mf));

    /* Get the volume number from the catalog object. */
    e = BfM_GetTrain(handle, (TrainID*)catObjForFile, (char**)&catPage, PAGE_BUF);
    if (e < 0) ERR(handle, e);

    GET_PTR_TO_CATENTRY_FOR_DATA(catObjForFile, catPage, catEntry);

    volNo = catEntry->fid.volNo;
    
    e = BfM_FreeTrain(handle, (TrainID*)catObjForFile, PAGE_BUF);
    if (e < 0) ERR(handle, e);

    MAKE_PAGEID(l_pid, volNo, list->entry[0].spid);
    e = BfM_GetTrain(handle, &l_pid, (char **)&l_node, PAGE_BUF);
    if (e < 0) ERR(handle, e);

    MAKE_PAGEID(r_pid, volNo, list->entry[1].spid);
    e = BfM_GetTrain(handle, &r_pid, (char **)&r_node, PAGE_BUF);
    if (e < 0) ERR(handle, e);

    l_nEntries = l_node->header.nEntries;
    r_nEntries = r_node->header.nEntries;

    /*@ merge or distribute */
    if (l_nEntries + r_nEntries <= LOT_MAXENTRIES) {
	/* merge two nodes into the left node */

	memcpy((char*)&l_node->entry[l_nEntries], (char*)&r_node->entry[0], r_nEntries*sizeof(L_O_T_INodeEntry));
	l_node->header.nEntries += r_nEntries;

	/* adjust the count fields */
        for (i = l_nEntries; i < l_nEntries+r_nEntries; i++) {
	    l_node->entry[i].count += l_node->entry[l_nEntries-1].count;
        }

	list->entry[0].count += list->entry[1].count;
	list->entry[1].count = 0;

	*mf = TRUE;
	*uf = (l_node->header.nEntries < LOT_HALFENTRIES) ? TRUE:FALSE;
	*movedEntries = -r_nEntries;

    } else {
	/* distribute the entries evenly */

	totalEntries = l_nEntries + r_nEntries;
	/* movedEntries is the # of entries moved from the left node
	 * to the right node */
	*movedEntries = l_nEntries - (totalEntries/2 + totalEntries%2);

	if (*movedEntries > 0) {
	    /* move the entries form the left node to the right node */

	    memmove((char*)&r_node->entry[*movedEntries], (char*)&r_node->entry[0],
		        r_nEntries*sizeof(L_O_T_INodeEntry));
	    memcpy((char*)&r_node->entry[0], (char*)&l_node->entry[l_nEntries-(*movedEntries)],
		       (*movedEntries)*sizeof(L_O_T_INodeEntry));

	    /*@ adjust the count fields */
            for (i = 0; i < *movedEntries; i++) {
		r_node->entry[i].count -= l_node->entry[l_nEntries-(*movedEntries)-1].count;
            }

            for (i = *movedEntries; i < r_nEntries+(*movedEntries); i++) {
		r_node->entry[i].count += r_node->entry[(*movedEntries)-1].count;
            }

	} else {
	    /* move the entries form the right node to the left node */

	    memcpy((char*)&l_node->entry[l_nEntries], (char*)&r_node->entry[0],
		       -(*movedEntries)*sizeof(L_O_T_INodeEntry));
	    memmove((char*)&r_node->entry[0], (char*)&r_node->entry[-(*movedEntries)], 
		        (r_nEntries+(*movedEntries))*sizeof(L_O_T_INodeEntry));

	    /*@ adjust the count fields */
            for (i = 0; i < r_nEntries+(*movedEntries); i++) {
		r_node->entry[i].count -= l_node->entry[l_nEntries-(*movedEntries)-1].count;
            }

            for (i = l_nEntries; i < l_nEntries-(*movedEntries); i++) {
		l_node->entry[i].count += l_node->entry[l_nEntries-1].count;
            }
	}
	
	list->entry[0].count = l_node->entry[l_nEntries-(*movedEntries)-1].count;
	list->entry[1].count = r_node->entry[r_nEntries+(*movedEntries)-1].count;
	l_node->header.nEntries -= *movedEntries;
	r_node->header.nEntries += *movedEntries;

	*mf = FALSE;
	*uf = FALSE;
    }

    e = BfM_SetDirty(handle, &l_pid, PAGE_BUF);
    if (e < 0) ERRB2(handle, e, &l_pid, PAGE_BUF, &r_pid, PAGE_BUF);
    
    e = BfM_FreeTrain(handle, &l_pid, PAGE_BUF);
    if (e < 0) ERRB1(handle, e, &r_pid, PAGE_BUF);

    if (*mf) {
	e = BfM_FreeTrain(handle, &r_pid, PAGE_BUF);
	if (e < 0) ERR(handle, e);

        /* remove page from buffer pool */
        e = BfM_RemoveTrain(handle, &r_pid, PAGE_BUF, FALSE);
        if (e < 0) ERR(handle, e);

        e = Util_getElementFromPool(handle, dlPool, &dlElem);
        if (e < 0) ERR(handle, e);

        dlElem->type = DL_PAGE;
        dlElem->elem.pid = r_pid; /* save the PageID. */
        dlElem->next = dlHead->next; /* insert to the list */
        dlHead->next = dlElem;       /* new first element of the list */
	
    } else {
	e = BfM_SetDirty(handle, &r_pid, PAGE_BUF);
	if (e < 0) ERRB1(handle, e, &r_pid, PAGE_BUF);
	
	e = BfM_FreeTrain(handle, &r_pid, PAGE_BUF);
	if (e < 0) ERR(handle, e);
    }
    
    return(eNOERROR);

} /* lot_MergeOrDistribute() */
