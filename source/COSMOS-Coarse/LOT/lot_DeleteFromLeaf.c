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
 * Module: lot_DeleteFromLeaf.c
 *
 * Description:
 *
 * Exports:
 *  Four lot_DeleteFromLeaf(ObjectID*, L_O_T_ItemList*, Four, Four, Boolean*, Boolean*)
 *
 * Note:
 *  The parameters from and to are offsets in the leaf node.
 *  They are different from the length. So that to is 0 means 1 byte is
 *  to be deleted.
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
 * lot_DeleteFromLeaf()
 *================================*/
/*
 * Function: Four lot_DeleteFromLeaf(ObjectID*, L_O_T_ItemList*, Four, Four, Boolean*, Boolean*)
 *
 * Description:
 *
 * Returns:
 *  error code
 *    some errors caused by function calls
 *
 * Note:
 *  The parameter 'catObjForFile' is required for passing the volume number.
 *  The volume number is necessary because the large object tree uses ShortPageID.
 */
Four lot_DeleteFromLeaf(
    Four handle,
    ObjectID                    *catObjForFile, /* IN information for the file */
    L_O_T_ItemList              *list,          /* INOUT list of two leaf nodes */
    Four                        from,           /* IN starting offset of deletion */
    Four                        to,             /* IN ending offset of deletion */
    Boolean                     *uf,            /* OUT underflow flag */
    Boolean                     *mf,            /* OUT TRUE if merge occur */
    Pool                        *dlPool,        /* INOUT pool of dealloc list elements */
    DeallocListElem             *dlHead)        /* INOUT head of the dealloc list */
{
    Four                        e;              /* error number */
    PageID                      l_root;         /* root of left cut-path subtree */
    PageID                      r_root;         /* root of right cut-path subtree */
    L_O_T_LNode                 *l_node;        /* pointer to the left leaf node */
    L_O_T_LNode                 *r_node;        /* pointer to the right leaf node */
    Four                        l_count;        /* # of bytes in left leaf node */
    Four                        r_count;        /* # of bytes in right leaf node */
    Boolean                     l_f;            /* underflow flag of left leaf node */
    Boolean                     r_f;            /* underflow flag of right leaf node */
    Four                        volNo;          /* volume where the LOT is placed */
    SlottedPage                 *catPage;       /* buffer page containing the catalog object */
    sm_CatOverlayForData        *catEntry;      /* overay structure for catalog object access */
    Four                        n;
    DeallocListElem             *dlElem;        /* an element of the dealloc list */

    
    TR_PRINT(TR_LOT, TR1,
	     ("lot_DeleteFromLeaf(handle, catObjForFile, list, from, to, uf, mf)",
	      catObjForFile, list, from, to, uf, mf));

    /* Get the volume number from the catalog object. */
    e = BfM_GetTrain(handle, (TrainID*)catObjForFile, (char**)&catPage, PAGE_BUF);
    if (e < 0) ERR(handle, e);

    GET_PTR_TO_CATENTRY_FOR_DATA(catObjForFile, catPage, catEntry);

    volNo = catEntry->fid.volNo;
    
    e = BfM_FreeTrain(handle, (TrainID*)catObjForFile, PAGE_BUF);
    if (e < 0) ERR(handle, e);

    l_f = r_f = FALSE;
    
    /*@ Cache the left leaf node information in local variables */
    MAKE_PAGEID(l_root, volNo, list->entry[0].spid);
    
    /*@ Cache the right leaf node information in local variables */
    MAKE_PAGEID(r_root, volNo, list->entry[1].spid);

    /* read the left leaf node */
    if (!IS_NILPAGEID(l_root)) {
	l_count = list->entry[0].count;
	e = BfM_GetTrain(handle, &l_root, (char **)&l_node, LOT_LEAF_BUF);
	if (e < 0) ERR(handle, e);
    }

    /* read the right leaf node */
    if (!IS_NILPAGEID(r_root)) {
	r_count = list->entry[1].count;
	e = BfM_GetTrain(handle, &r_root, (char **)&r_node, LOT_LEAF_BUF);
	if (e < 0) {
	    if (IS_NILPAGEID(l_root)) ERR(handle, e);
	    else ERRB1(handle, e, &l_root, LOT_LEAF_BUF);
	}
    }

    /*@ delete data from the left leaf node */
    if (!IS_NILPAGEID(l_root)) {
	
	if (IS_NILPAGEID(r_root) && to != TO_END) {
	    /* In this case, 'to' indicates the ending offset of deletion
	       in left leaf node */
	    
	    /* fill the erased part by moving the upper part forward */
	    n = l_count - (to+1);
	    memmove(&l_node->data[from], &l_node->data[to+1], n);

	    l_count -= (to - from + 1);
	    
	} else {
	    /* We don't have to read the leaf node */
	    /* So we need to refine not to read in this case */
	    /* Notice, if merge occur then we should read this node. */
	    l_count = from;
	}

	/* Set the underflow flag of left leaf node */
	if (l_count < LOT_LNODE_HALFFREE) l_f = TRUE;
    }
	    
    /*@ delete data from the right leaf node */
    if (!IS_NILPAGEID(r_root)) {
	/* fill the erased part moving the others forward */
	n = r_count - (to + 1);
	memmove(&r_node->data[0], &r_node->data[to+1], n);

	/* Decrement the # of bytes in the right leaf node */
	r_count -= (to + 1);

	/* set the underflow flag of right leaf node */
	if (r_count < LOT_LNODE_HALFFREE) r_f = TRUE;
    }

    if (IS_NILPAGEID(l_root)) {
	*uf = r_f;		/* underflow flag */
	*mf = FALSE;		/* no merge */
    } else if (IS_NILPAGEID(r_root)) {
	*uf = l_f;		/* underflow flag */
	*mf = FALSE;		/* no merge */
    } else if (!l_f && !r_f) {
	*uf = FALSE;		/* no underflow */
	*mf = FALSE;		/* no merge */
    } else {
	/* Either left or right leaf node is underflow */

	if (l_count + r_count <= LOT_LNODE_MAXFREE) {
	    /* merge two leaf nodes into left leaf node */

	    /* copy the data in right node into the leaf node */
	    memcpy(&l_node->data[l_count], &r_node->data[0], r_count);
	    
	    l_count += r_count;
	    r_count = 0;

	    /* set the flags indicating the status */
	    *mf = TRUE;		/* two nodes were merged */
	    if (l_count < LOT_LNODE_HALFFREE)
		*uf = TRUE;	/* underflow occurs */
	    else
		*uf = FALSE;

	} else {
	    /* distribute the original data evenly into two nodes */

	    e = lot_DistributeLeaf(handle, &l_root, &l_count, &r_root, &r_count);
	    if (e < 0) ERRB2(handle, e, &l_root, LOT_LEAF_BUF, &r_root, LOT_LEAF_BUF);
	    
	    /* set the flags indicating the status */
	    *mf = FALSE;
	    *uf = FALSE;
	}

    }

    /*@ write the local variables to the return parameters */
    list->entry[0].count = l_count;
    list->entry[1].count = r_count;

    if (!IS_NILPAGEID(l_root)) {
	e = BfM_SetDirty(handle, &l_root, LOT_LEAF_BUF);
	if (e < 0) {
	    if (IS_NILPAGEID(r_root)) ERRB1(handle, e, &l_root, LOT_LEAF_BUF);
	    else ERRB2(handle, e, &l_root, LOT_LEAF_BUF, &r_root, LOT_LEAF_BUF);
	}

	e = BfM_FreeTrain(handle, &l_root, LOT_LEAF_BUF);
	if (e < 0) {
	    if (IS_NILPAGEID(r_root)) ERR(handle, e);
	    else ERRB1(handle, e, &r_root, LOT_LEAF_BUF);
	}
    }

    if (!IS_NILPAGEID(r_root)) {
	if (*mf) {
 	    e = BfM_FreeTrain(handle, &r_root, LOT_LEAF_BUF);
	    if (e < 0) ERR(handle, e);

            /* remove leaf data page from buffer pool */
            e = BfM_RemoveTrain(handle, &r_root, LOT_LEAF_BUF, FALSE);
            if (e < 0) ERR(handle, e);

	    /* free the right node to disk manager */
            /*
            ** original code:
            ** e = RDsM_FreeTrain(&r_root, TRAINSIZE2);
	    ** if (e < 0) ERR(handle, e);
            */                
	    e = Util_getElementFromPool(handle, dlPool, &dlElem);
	    if (e < 0) ERR(handle, e);

            dlElem->type = DL_PAGE;
	    dlElem->elem.pid = r_root; /* save the PageID. */
	    dlElem->next = dlHead->next; /* insert to the list */
	    dlHead->next = dlElem;       /* new first element of the list */
	    
	} else {
	    e = BfM_SetDirty(handle, &r_root, LOT_LEAF_BUF);
	    if (e < 0) ERRB1(handle, e, &r_root, LOT_LEAF_BUF);

	    e = BfM_FreeTrain(handle, &r_root, LOT_LEAF_BUF);
	    if (e < 0) ERR(handle, e);
	}
    }

    return(eNOERROR);

} /* lot_DeleteFromLeaf() */


/*@================================
 * lot_DistributeLeaf()
 *================================*/
/*
 * Function: Four lot_DistributeLeaf(PageID*, Datalength*, PageID*, Datalength*)
 *
 * Description:
 *  Distributes the data in two leaf nodes into the original nodes.
 *  We assume that the parameters are correct.
 *
 * Returns:
 *  Error codes
 *    some errors caused by function calls
 *
 * Side effects:
 */
Four lot_DistributeLeaf(
    Four handle,
    PageID      *l_root,                /* IN PageID of left leaf node */
    Four        *l_count,               /* INOUT # of bytes in left leaf node */
    PageID      *r_root,                /* IN PageID of right leaf node */
    Four        *r_count)               /* INOUT # of bytes in right leaf node */
{
    Four        e;                      /* error number */
    L_O_T_LNode *l_node;                /* pointer to the left leaf node */
    L_O_T_LNode *r_node;                /* pointer to the rignt leaf node */
    Four        totalBytes;             /* sum of bytes in two leaf nodes */
    Four        bytesPerNode;           /* # of bytes distributed evenly */
    Four        remainedBytes;          /* # of bytes after balancing */
    Four        movedBytes;             /* # of bytes moved from left to right */

    
    TR_PRINT(TR_LOT, TR1,
             ("lot_DistributeLeaf(handle, l_root=%P, l_count=%P, r_root=%P, r_count=%P)",
	      l_root, l_count, r_root, r_count));

    totalBytes = *l_count + *r_count;
    bytesPerNode = totalBytes/2;
    remainedBytes = totalBytes % 2;
    /* movedBytes is the # of bytes to be moved from the left to the right */
    /* If movedBytes is less than 0, it means that move is from right to left */
    movedBytes = *l_count - (bytesPerNode + ((remainedBytes > 0) ? 1:0));

    e = BfM_GetTrain(handle, l_root, (char **)&l_node, LOT_LEAF_BUF);
    if (e < 0) ERR(handle, e);

    e = BfM_GetTrain(handle, r_root, (char **)&r_node, LOT_LEAF_BUF);
    if (e < 0) ERRB1(handle, e, l_root, LOT_LEAF_BUF);
    
    if (movedBytes > 0) {
	/* go from the left to the right */

	/* prepare space in the right node */
	memmove(&r_node->data[movedBytes], &r_node->data[0], *r_count);
	/* copy data from the left to the right */
	memcpy(&r_node->data[0], &l_node->data[*l_count - movedBytes], movedBytes);
    } else {
	/* go from the right to the left */

	/* copy data from the right to the left */
	memcpy(&l_node->data[*l_count], &r_node->data[0], -movedBytes);
	/* zap the right node by moving the uncopied part */
	memmove(&r_node->data[0], &r_node->data[-movedBytes], *r_count+movedBytes);
    }

    *l_count -= movedBytes;
    *r_count += movedBytes;
    
    e = BfM_SetDirty(handle, l_root, LOT_LEAF_BUF);
    if (e < 0) ERRB2(handle, e, l_root, LOT_LEAF_BUF, r_root, LOT_LEAF_BUF);
    
    e = BfM_FreeTrain(handle, l_root, LOT_LEAF_BUF);
    if (e < 0) ERRB1(handle, e, r_root, LOT_LEAF_BUF);

    e = BfM_SetDirty(handle, r_root, LOT_LEAF_BUF);
    if (e < 0) ERRB1(handle, e, r_root, LOT_LEAF_BUF);
    
    e = BfM_FreeTrain(handle, r_root, LOT_LEAF_BUF);
    if (e < 0) ERR(handle, e);

    return(eNOERROR);

} /* lot_DeleteFromLeaf() */
