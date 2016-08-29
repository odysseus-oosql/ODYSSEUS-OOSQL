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
 * Module: lot_RebalanceTree.c
 *
 * Description:
 *  Rebalnce the large object tree. The tree may not be unbalanced.
 *
 * Exports:
 *  Four lot_RebalanceTree(ObjectID*, L_O_T_Path*, PageID*, Boolean*, Boolean*)
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
 * lot_RebalanceTree()
 *================================*/
/*
 * Function: Four lot_RebalanceTree(ObjectID*, L_O_T_Path*, PageID*, Boolean*, Boolean*)
 *
 * Description:
 *  Rebalnce the large object tree. The tree may not be unbalanced.
 *
 * Returns:
 *  error code
 *    some errors caused by function calls
 */
Four lot_RebalanceTree(
    Four handle,
    ObjectID            *catObjForFile, /* IN information for the file */
    L_O_T_Path          *path,          /* IN information for the cut path */
    PageID              *root,          /* INOUT root PageID */
    Boolean             *uf,            /* OUT indicate the underflow of child node */
    Boolean             *f,             /* OUT indicate the change of root node */
    Pool                *dlPool,        /* INOUT pool of dealloc list elements */
    DeallocListElem     *dlHead)        /* INOUT head of the dealloc list */
{
    Four                e;              /* error number */
    SlottedPage         *apage;         /* pointer to buffer holding slotted page */
    PageID              p_pid;          /* PageID of the current node */
    Two                 rootSlotNo;     /* slot no of root if root is with header */
    L_O_T_INode         *p_node;        /* pointer to the current node */
    Two                 p_c_idx;        /* entry index of the child in cut path */
    Boolean             p_c_uf;         /* child underflow flag from the path */
    Boolean             c_uf;           /* child underflow flag from the recursive call */
    Boolean             c_mf;           /* merge flag of merge/distribution */
    Two                 c_l_idx;        /* left entry index of merge/distribution */
    L_O_T_ItemList      c_list;         /* for merge/distribution of two children */
    Boolean             rootChange;     /* root change flag */
    Four                movedEntries;   /* # of entries moved from left to right */
    Four                oldEntries;     /* # of entries when root is with header */
    Four                dummy;          /* dummy parameter */
    PageID              pid;
    Four                n;
    DeallocListElem     *dlElem;        /* an element of the dealloc list */
    L_O_T_INodeEntry    tmpEntries[2];  /* temporary entries */ 

    
    TR_PRINT(TR_LOT, TR1,
             ("lot_RevalanceTree(catObjForFile=%P, path=%P, root=%P, f=%P)",
	      catObjForFile, path, root, f));

    if (lot_EmptyPath(handle, path)) {
	/* one basis of recursive call */
	/* In deleteion phase we didn't reach the node at height 1 */
	*uf = FALSE;
	*f = FALSE;
	
	return(eNOERROR);
    }

    /*@ Pop from the path the information for the current node */
    lot_PopPath(handle, path, &p_pid, &rootSlotNo, (L_O_T_INode **)&apage, &p_c_idx, &p_c_uf);

    lot_GetNodePointer(handle, (Page *)apage, rootSlotNo, &p_node);

    if (rootSlotNo != NIL)
	oldEntries = p_node->header.nEntries;
    
    rootChange = FALSE;
	    
    if (p_c_uf && p_node->header.nEntries > 1) {
	/* merge or redistributes with the sibling node */

	c_list.nEntries = 2;
	c_list.entry = tmpEntries; 

	/*@ find the node to merge/distribute */
	/* we prefer the right sibling */
	if (p_c_idx == p_node->header.nEntries-1)	    
	    /* merge with the left node */
	    c_l_idx = p_c_idx-1;
	else
	    /* merge with the right node */
	    c_l_idx = p_c_idx;
	
	c_list.entry[0].spid = p_node->entry[c_l_idx].spid;
	c_list.entry[0].count = lot_GetCount(handle, p_node, c_l_idx);
	c_list.entry[1].spid = p_node->entry[c_l_idx+1].spid;
	c_list.entry[1].count = lot_GetCount(handle, p_node, c_l_idx+1);

	if (p_node->header.height == 1) {
	    /* Children are leaf nodes */

	    e = lot_MergeOrDistributeLeaf(handle, catObjForFile, &c_list, &c_uf, &c_mf, dlPool, dlHead);
	    if (e < 0)		/* leaf nodes merge/distribute failed */
		ERRB1(handle, e, &p_pid, PAGE_BUF);

	} else {
	    /* Children are internal nodes */

	    e = lot_MergeOrDistribute(handle, catObjForFile, &c_list, &movedEntries, &c_uf, &c_mf, dlPool, dlHead);
	    if (e < 0)		/* internal nodes merge/distribute failed */
		ERRB1(handle, e, &p_pid, PAGE_BUF);
	}

	/* At this point, assert that c_uf is FALSE. */
	/* adjust the count fields */
	if (c_mf) {
	    /* merge has been occured. */
	    
	    /*@ adjust the count field */
	    p_node->entry[c_l_idx].count = p_node->entry[c_l_idx+1].count;

	    /* fill the erased part by moving the entries after it */
	    n = p_node->header.nEntries - (c_l_idx+2);
	    memmove(&p_node->entry[c_l_idx+1], &p_node->entry[c_l_idx+2], 
		    n*sizeof(L_O_T_INodeEntry));
	    p_node->header.nEntries --;
	    
	    if (p_c_idx != c_l_idx) {
		/* The node was the rightmost node */
		p_c_idx --;

		if (p_node->header.height > 1) {
		    
		    MAKE_PAGEID(pid, p_pid.volNo, p_node->entry[c_l_idx].spid);
		    e = lot_ReplaceTop(handle, path, &pid, movedEntries);
		    if (e < 0) ERRB1(handle, e, &p_pid, PAGE_BUF);
		}
	    }

	} else {
	    /* distribution has been occurred */
	    
	    /* adjust the count field */
	    p_node->entry[c_l_idx].count = p_node->entry[c_l_idx+1].count - c_list.entry[1].count;

	    if (p_c_idx != c_l_idx && p_node->header.height > 1) {
		/* the node was the rightmost node */
		e = lot_ReplaceTop(handle, path, (PageID *)NULL, movedEntries);
		if (e < 0) ERRB1(handle, e, &p_pid, PAGE_BUF);
	    }
	}

    }
    
    if (p_node->header.height == 1) {
	/* recursive basis: children are leaf nodes */

	/* do nothing */
	*f = FALSE;

    } else {
	/* children are internal nodes */

	if (root != NULL && p_node->header.nEntries == 1) {
	    /* Make the child the new root node */

	    MAKE_PAGEID(*root, root->volNo, p_node->entry[0].spid);
	    rootChange = TRUE;

	    e = lot_RebalanceTree(handle, catObjForFile, path, root, &c_uf, f, dlPool, dlHead);
	    if (e < 0)		/* recursive call failed */
		ERRB1(handle, e, &p_pid, PAGE_BUF);
	    
	} else {

	    /*@ recursive call with root NULL */
	    e = lot_RebalanceTree(handle, catObjForFile, path, (PageID *)NULL, &c_uf, f, dlPool, dlHead);
	    if (e < 0)		/* recursive call failed */
		ERRB1(handle, e, &p_pid, PAGE_BUF);

	    if (c_uf) {
		/* Underflow has occurred in the child node */
		
		c_list.nEntries = 2;
		c_list.entry = tmpEntries; 
		
		/*@ find the node to merge/distribute */
		/* we prefer the right sibling */
		if (p_c_idx == p_node->header.nEntries-1)	    
		    /* merge with the left node */
		    c_l_idx = p_c_idx-1;
		else
		    /* merge with the right node */
		    c_l_idx = p_c_idx;
		
		c_list.entry[0].spid = p_node->entry[c_l_idx].spid;
		c_list.entry[0].count = lot_GetCount(handle, p_node, c_l_idx);
		c_list.entry[1].spid = p_node->entry[c_l_idx+1].spid;
		c_list.entry[1].count = lot_GetCount(handle, p_node, c_l_idx+1);
		
		/* Children are internal nodes */
		e = lot_MergeOrDistribute(handle, catObjForFile, &c_list, &dummy, &c_uf, &c_mf, dlPool, dlHead);
		if (e < 0)	/* internal nodes merge/distribute failed */
		    ERRB1(handle, e, &p_pid, PAGE_BUF);
		
		/* At this point, assert that c_uf is FALSE. */
		if (c_mf) {
		    /* merge has been occured. */
		    
		    /*@ adjust the count field */
		    p_node->entry[c_l_idx].count = p_node->entry[c_l_idx+1].count;
		    
		    /* fill the erased part by moving the entries after it */
		    n = p_node->header.nEntries - (c_l_idx+2);
		    memmove(&p_node->entry[c_l_idx+1], &p_node->entry[c_l_idx+2], 
			    n*sizeof(L_O_T_INodeEntry));
		    p_node->header.nEntries --;
		    
		} else {
		    /* distribution has been occurred */
		    
		    /*@ adjust the count field */
		    p_node->entry[c_l_idx].count = p_node->entry[c_l_idx+1].count - c_list.entry[1].count;
		}
		
	    }

	    if (root != NULL && p_node->header.nEntries == 1) {
		/* Make the child the new root */

		MAKE_PAGEID(*root, root->volNo, p_node->entry[0].spid);
		rootChange = TRUE;
	    }
	}
    }

    /* set the underflow flag */
    if (rootChange) {
	/* use child underflow flag value */
	*f = TRUE;

	if (rootSlotNo == NIL) {
	    /* free the old root */
	    e = BfM_FreeTrain(handle, &p_pid, PAGE_BUF);
	    if (e < 0) ERR(handle, e);
	
            /* remove page from buffer pool */
            e = BfM_RemoveTrain(handle, &p_pid, PAGE_BUF, FALSE);
            if (e < 0) ERR(handle, e);

	    e = Util_getElementFromPool(handle, dlPool, &dlElem);
	    if (e < 0) ERR(handle, e);

            dlElem->type = DL_PAGE;
	    dlElem->elem.pid = p_pid; /* save the PageID. */
	    dlElem->next = dlHead->next; /* insert to the list */
	    dlHead->next = dlElem;       /* new first element of the list */
	    
	} else {
	    apage->header.unused += sizeof(L_O_T_INodeHdr) +
		sizeof(L_O_T_INodeEntry)*oldEntries - sizeof(ShortPageID);
	    
	    /* free the buffer */
	    e = BfM_SetDirty(handle, &p_pid, PAGE_BUF);
	    if (e < 0) ERRB1(handle, e, &p_pid, PAGE_BUF);
	    
	    e = BfM_FreeTrain(handle, &p_pid, PAGE_BUF);
	    if (e < 0) ERR(handle, e);
	}

    } else {
	if (rootSlotNo != NIL)
	    apage->header.unused +=
		sizeof(L_O_T_INodeEntry) * (oldEntries - p_node->header.nEntries);
	
	if (p_node->header.nEntries < LOT_HALFENTRIES)
	    *uf = TRUE;
	else
	    *uf = FALSE;
	
	/* free the buffer */
	e = BfM_SetDirty(handle, &p_pid, PAGE_BUF);
	if (e < 0) ERRB1(handle, e, &p_pid, PAGE_BUF);
	
	e = BfM_FreeTrain(handle, &p_pid, PAGE_BUF);
	if (e < 0) ERR(handle, e);
    }
    
    return(eNOERROR);

} /* lot_RebalanceTree() */
