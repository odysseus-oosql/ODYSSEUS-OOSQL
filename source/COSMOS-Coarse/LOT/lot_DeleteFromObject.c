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
 * Module: lot_DeleteFromObject.c
 *
 * Description:
 *
 * Exports:
 *  Four lot_DeleteFromObject(ObjectID*, L_O_T_ItemList*, Two, Four, Four, Two,
 *                            Boolean*, Boolean*, L_O_T_Path*, Pool*, DeallocListElem*)
 */


#include <string.h>
#include <stdlib.h>
#include "common.h"
#include "trace.h"
#include "Util.h"
#include "RDsM_Internal.h"	
#include "BfM.h"
#include "LOT_Internal.h"
#include "perThreadDS.h"
#include "perProcessDS.h"



/*@================================
 * lot_DeleteFromObject()
 *================================*/
/*
 *  Four lot_DeleteFromObject(ObjectID*, L_O_T_ItemList*, Two, Four, Four, Two,
 *                                 Boolean*, Boolean*, L_O_T_Path*, Pool*, DeallocListElem*)
 *
 * Description:
 *
 *
 * Returns:
 *  error code
 *    some errors caused by functions calls
 */
Four lot_DeleteFromObject(
    Four handle,
    ObjectID                    *catObjForFile, /* IN information for the file */
    L_O_T_ItemList              *list,          /* INOUT information for two nodes */
    Two                         rootSlotNo,     /* IN slot no of object when root is with header */
    Four                        from,           /* IN starting offset of deletion */
    Four                        to,             /* IN ending offset of deletion */
    Two                         *which,         /* OUT which node have the underflowed child? */
    Boolean                     *uf,            /* OUT underflow flag */
    Boolean                     *mf,            /* OUT merge flag */
    L_O_T_Path                  *path,          /* OUT cut-path registration */
    Pool                        *dlPool,        /* INOUT pool of dealloc list elements */
    DeallocListElem             *dlHead)        /* INOUT head of the dealloc list */
{
     Four                        e;              /* Error Number */
    SlottedPage                 *apage;         /* pointer to buffer holding slotted page */
    PageID                      l_root;         /* root of left cut-path subtree */
    PageID                      r_root;         /* root of right cut-path subtree */
    L_O_T_INode                 *l_node;        /* pointer to the left node */
    L_O_T_INode                 *r_node;        /* pointer to the right node */
    PageID                      c_l_root;       /* PageID of the root of left subtree */
    PageID                      c_r_root;       /* PageID of the root of right subtree */
    PageID                      *c_l_root_ptr;  /* pointer to PageID of left root */
    PageID                      *c_r_root_ptr;  /* Pointer to PageID of right root */
    Four                        c_l_idx;        /* entry index of left cut path */
    Four                        c_r_idx;        /* entry index of right cut path */
    Four                        c_l_count;      /* # of original bytes in left subtree */
    Four                        c_r_count;      /* # of original bytes in right subtree */
    L_O_T_ItemList              c_list;         /* ItemList for the subtree */
    Four                        c_from;         /* starting offset of delete in subtree */
    Four                        c_to;           /* ending offset of delete in subtree */
    Four                        height;         /* height of current node */
    Two                         c_which;        /* which value of child node */
    Two                         p_c_idx;        /* entry index of possibly underflowed child node */
    Boolean                     c_uf;           /* underflow flag of subtree */
    Boolean                     c_mf;           /* merge flag of subtree */
    Boolean                     md_uf;          /* underflow flag in lot_MergeOrDistribute() */
    Boolean                     md_mf;          /* merge flag in lot_MergeOrDistribute() */
    Boolean                     l_uf;           /* underflow flag of left node */
    Boolean                     r_uf;           /* underflow flag of right node */
    Four                        l_begin;        /* begin index of subtrees to drop in left node */
    Four                        l_end;          /* end index of subtrees to drop in left node */
    Four                        r_end;          /* end index of subtrees to drop in right node */
    Four                        oldEntries;     /* # of entries in root when root is with header */
    Four                        movedEntries;   /* # of entries moved from left to right */
    Four                        volNo;          /* volume where the LOT is placed */
    SlottedPage                 *catPage;       /* buffer page containing the catalog object */
    sm_CatOverlayForData        *catEntry;      /* overay structure for catalog object access */
    PageID                      pid;
    Four                        deletedBytes;
    Four                        movedBytes;
    Four                        n;
    Four                        i;
    DeallocListElem             *dlElem;        /* an element of the dealloc list */
    L_O_T_INodeEntry            tmpEntries[2];  /* temporary entries */ 

    
    TR_PRINT(TR_LOT, TR1,
	     ("lot_DeleteFromObject(handle, catObjForFile, list, from, to, uf, mf)",
	      catObjForFile, list, from, to, uf, mf));
    
    /* Get the volume number from the catalog object. */
    e = BfM_GetTrain(handle, (TrainID*)catObjForFile, (char**)&catPage, PAGE_BUF);
    if (e < 0) ERR(handle, e);

    GET_PTR_TO_CATENTRY_FOR_DATA(catObjForFile, catPage, catEntry);

    volNo = catEntry->fid.volNo;
    
    e = BfM_FreeTrain(handle, (TrainID*)catObjForFile, PAGE_BUF);
    if (e < 0) ERR(handle, e);

    /*@ Cache the left node information in local variables */
    MAKE_PAGEID(l_root, volNo, list->entry[0].spid);
    if (!IS_NILPAGEID(l_root)) {
	e = BfM_GetTrain(handle, &l_root, (char **)&apage, PAGE_BUF);
	if (e < 0) ERR(handle, e);

	lot_GetNodePointer(handle, (Page *)apage, rootSlotNo, &l_node);

	/* save the # of entries if root is with header */
	if (rootSlotNo != NIL) oldEntries = l_node->header.nEntries;
    }
    
    /*@ Cache the right node information in local variables */
    MAKE_PAGEID(r_root, volNo, list->entry[1].spid);
    if (!IS_NILPAGEID(r_root)) {
	e = BfM_GetTrain(handle, &r_root, (char **)&r_node, PAGE_BUF);
	if (e < 0) {
	    if (!IS_NILPAGEID(l_root)) BfM_FreeTrain(handle, &l_root, PAGE_BUF);
	    ERR(handle, e);
	}
    }
    
    /*
     * When the root of the left subtree is not NIL.
     */
    if (!IS_NILPAGEID(l_root)) {
	
	/*@ height of current node */
	height = l_node->header.height;
	
	/* get the index of the child node containing 'from' byte */
	if ((c_l_idx = lot_SearchInNode(handle, l_node, from)) < 0) {
	    if (!IS_NILPAGEID(r_root)) BfM_FreeTrain(handle, &r_root, PAGE_BUF);
	    ERRB1(handle, c_l_idx, &l_root, PAGE_BUF);
	}

	c_l_count = lot_GetCount(handle, l_node, c_l_idx);
	c_from = from - ((c_l_idx == 0) ? 0:l_node->entry[c_l_idx-1].count);
	
	if (c_from == 0) {
	    /* the whole subtree is to be deleted */
	    c_l_root_ptr = (PageID *)NULL;
	} else {
	    MAKE_PAGEID(c_l_root, volNo,l_node->entry[c_l_idx].spid);
	    c_l_root_ptr = &c_l_root;
	}
	
	if (IS_NILPAGEID(r_root)) {
	    
	    /* The right subtree root is NIL */
	    
	    if (to == TO_END) {
		/* The whole right subtree was deleted at the ancestor. */
		/* So the right subtree root is NIL */
		c_to = TO_END;
		c_r_root_ptr = (PageID *)NULL;
		
	    } else {
		/* We don't yet pass the least common ancstor */
		
		/* get the index of the child node containing 'to' byte */
		if ((c_r_idx = lot_SearchInNode(handle, l_node, to)) < 0)
		    ERRB1(handle, c_r_idx, &l_root, PAGE_BUF);

		c_r_count = lot_GetCount(handle, l_node, c_r_idx);
		c_to = to - ((c_r_idx == 0) ? 0:l_node->entry[c_r_idx-1].count);
		
		if (c_to == lot_GetCount(handle, l_node, c_r_idx) - 1) {
		
		    c_to = TO_END;
		    c_r_root_ptr = (PageID*)NULL;
		    
		} else {
		    if (c_l_idx == c_r_idx && c_l_root_ptr != NULL) {
			c_r_root_ptr = (PageID*)NULL; /* c_from != 0 */
			
		    } else {
			MAKE_PAGEID(c_r_root, volNo, l_node->entry[c_r_idx].spid);
			c_r_root_ptr = &c_r_root;
		    }
		}
	    }
	}
    } else
	c_l_root_ptr = (PageID *)NULL;
	
    /*@
     * When the root of the right subtree is not NIL.
     */
    if (!IS_NILPAGEID(r_root)) {
	
	/* height of current node: replicated line for l_root is NIL */
	height = r_node->header.height;
	
	if ((c_r_idx = lot_SearchInNode(handle, r_node, to)) < 0) {
	    if (!IS_NILPAGEID(l_root)) BfM_FreeTrain(handle, &l_root, PAGE_BUF);
	    ERRB1(handle, c_r_idx, &r_root, PAGE_BUF);
	}

	c_r_count = lot_GetCount(handle, r_node, c_r_idx);
	c_to = to - ((c_r_idx == 0) ? 0:r_node->entry[c_r_idx-1].count);
	
	if (c_to == lot_GetCount(handle, r_node, c_r_idx) - 1) {
	    /* the whole subtree is to be deleted */
	    c_r_root_ptr = (PageID *)NULL;
	    c_to = TO_END;
	    
	} else {
	    MAKE_PAGEID(c_r_root, volNo, r_node->entry[c_r_idx].spid);
	    c_r_root_ptr = &c_r_root;
	    
	}
    }
    
    /*@
     * Here is the delete operation.
     */
    /* drop all subtrees whose all bytes to be deleted */
    if (!IS_NILPAGEID(l_root)) {
	l_begin = (c_l_root_ptr == NULL) ? c_l_idx:(c_l_idx+1);
	
	if (!IS_NILPAGEID(r_root) || to == TO_END)
	    l_end = l_node->header.nEntries - 1;
	else
	    l_end = (c_to == TO_END) ? c_r_idx:c_r_idx-1;
	
	for (i =l_begin; i <= l_end; i++){
	    if (i >= l_node->header.nEntries)
		TR_PRINT(TR_LOT, TR3, ("i is greather than nEntries\n"));
	    
	    MAKE_PAGEID(pid, volNo, l_node->entry[i].spid);
	    
	    if (height > 1) {
		e = lot_DropTree(handle, &pid, dlPool, dlHead);
                if (e < 0) {
                    if (!IS_NILPAGEID(l_root)) BfM_FreeTrain(handle, &l_root, PAGE_BUF);
                    if (!IS_NILPAGEID(r_root)) BfM_FreeTrain(handle, &r_root, PAGE_BUF);
                    ERR(handle, e);
                }
            }
	    else {
                /* remove page from buffer pool */
                e = BfM_RemoveTrain(handle, &pid, LOT_LEAF_BUF, FALSE);
                if (e < 0) {
                    if (!IS_NILPAGEID(l_root)) BfM_FreeTrain(handle, &l_root, PAGE_BUF);
                    if (!IS_NILPAGEID(r_root)) BfM_FreeTrain(handle, &r_root, PAGE_BUF);
                    ERR(handle, e);
                }

                e = Util_getElementFromPool(handle, dlPool, &dlElem);
                if (e < 0) {
                    if (!IS_NILPAGEID(l_root)) BfM_FreeTrain(handle, &l_root, PAGE_BUF);
                    if (!IS_NILPAGEID(r_root)) BfM_FreeTrain(handle, &r_root, PAGE_BUF);
                    ERR(handle, e);
                }

                dlElem->type = DL_TRAIN;
                dlElem->elem.pid = pid; /* save the PageID. */
                dlElem->next = dlHead->next; /* insert to the list */
                dlHead->next = dlElem;       /* new first element of the list */                
            }
	}	    
    }

    if (!IS_NILPAGEID(r_root)) {

	/* if c_to == TO_END, the [c_r_idx]-th subtree is also droped */
	r_end = (c_to == TO_END) ? c_r_idx:c_r_idx-1;
	for (i = 0; i <= r_end; i++) {
	    MAKE_PAGEID(pid, volNo, r_node->entry[i].spid);
	    if (height > 1) { 
		e = lot_DropTree(handle, &pid, dlPool, dlHead);
                if (e < 0) {
                    if (!IS_NILPAGEID(l_root)) BfM_FreeTrain(handle, &l_root, PAGE_BUF);
                    if (!IS_NILPAGEID(r_root)) BfM_FreeTrain(handle, &r_root, PAGE_BUF);
                    ERR(handle, e);
                }
            }
	    else {
                /* remove page from buffer pool */
                e = BfM_RemoveTrain(handle, &pid, LOT_LEAF_BUF, FALSE);
                if (e < 0) {
                    if (!IS_NILPAGEID(l_root)) BfM_FreeTrain(handle, &l_root, PAGE_BUF);
                    if (!IS_NILPAGEID(r_root)) BfM_FreeTrain(handle, &r_root, PAGE_BUF);
                    ERR(handle, e);
                }

                e = Util_getElementFromPool(handle, dlPool, &dlElem);
                if (e < 0) {
                    if (!IS_NILPAGEID(l_root)) BfM_FreeTrain(handle, &l_root, PAGE_BUF);
                    if (!IS_NILPAGEID(r_root)) BfM_FreeTrain(handle, &r_root, PAGE_BUF);
                    ERR(handle, e);
                }
                
                dlElem->type = DL_TRAIN;
                dlElem->elem.pid = pid; /* save the PageID. */
                dlElem->next = dlHead->next; /* insert to the list */
                dlHead->next = dlElem;       /* new first element of the list */
            }
	}
    }

    /* set the initial which value:
       If there isn't a underflowed node, we may choose LEFT node */
    if (IS_NILPAGEID(l_root))
	*which = RIGHT;
    else
	*which = LEFT;
    
    /* delete the entries in the nodes at the current level. */
    /* If the subtree's part is to be deleted, delete it first */
    if (c_l_root_ptr == NULL && c_r_root_ptr == NULL) {
	/* Whole subtrees are deleted in both left and right cut paths */
	
	if(!IS_NILPAGEID(l_root)) {
	    if (!IS_NILPAGEID(r_root) || to == TO_END) {
		/* all entries in [c_l_idx, ~) are deleted. */
		l_node->header.nEntries = c_l_idx;
		
	    } else {
		/* delete the entries in [c_l_idx, c_r_idx] */
		
		/* adjust the count fields */
		deletedBytes = to - from + 1;
                for (i = c_r_idx+1; i < l_node->header.nEntries; i++) {
		    l_node->entry[i].count -= deletedBytes;
                }
		
		n = l_node->header.nEntries - (c_r_idx+1);
		memmove(&l_node->entry[c_l_idx], &l_node->entry[c_r_idx+1], 
			n*sizeof(L_O_T_INodeEntry));
		l_node->header.nEntries -= (c_r_idx - c_l_idx + 1);
	    }
	}
	
	if (!IS_NILPAGEID(r_root)) {
	    /* delete the entries in [0, c_r_idx] */
	    n = r_node->header.nEntries - (c_r_idx+1);
	    memmove(&r_node->entry[0], &r_node->entry[c_r_idx+1], n*sizeof(L_O_T_INodeEntry));
	    r_node->header.nEntries -= (c_r_idx+1);
	    
	    /* adjust the count fields */
            for (i = 0; i < r_node->header.nEntries; i++) {
		r_node->entry[i].count -= (to + 1);
	    } 
        }

	c_uf = FALSE;
	c_mf = FALSE;
	
    } else {
	
	/* construt L_O_T_ItemList */
	c_list.nEntries = 2;
	c_list.entry = tmpEntries; 
	c_list.entry[0].spid = (c_l_root_ptr == NULL) ? NIL:c_l_root_ptr->pageNo;
	c_list.entry[0].count = c_l_count;
	c_list.entry[1].spid = (c_r_root_ptr == NULL) ? NIL:c_r_root_ptr->pageNo;
	c_list.entry[1].count = c_r_count;
	
	if (height > 1) {
	    /* recursive call */
	    
	    e = lot_DeleteFromObject(handle, catObjForFile, &c_list, NIL, c_from,
				     c_to, &c_which, &c_uf, &c_mf, path, dlPool, dlHead);
	    
	} else {
	    /* The children are leaf nodes */

	    c_which = (c_l_root_ptr == NULL) ? RIGHT:LEFT;	    
	    e = lot_DeleteFromLeaf(handle, catObjForFile, &c_list,
				   c_from, c_to, &c_uf, &c_mf, dlPool, dlHead);
	}
	
	if (e < 0) {
	    if (!IS_NILPAGEID(l_root)) BfM_FreeTrain(handle, &l_root, PAGE_BUF);
	    if (!IS_NILPAGEID(r_root)) BfM_FreeTrain(handle, &r_root, PAGE_BUF);
	    ERR(handle, e);
	}
	
	if (c_mf) {
	    /*@ two child nodes are merged */
	    /* Assert that l_root is not NIL PageID */
	    
	    if (!IS_NILPAGEID(r_root)) {
		l_node->header.nEntries = c_l_idx + 1;
		l_node->entry[c_l_idx].count = c_list.entry[0].count + ((c_l_idx == 0) ? 0:l_node->entry[c_l_idx-1].count);
		
		/* adjust the count fields in r_node */
                for (i = c_r_idx+1; i < r_node->header.nEntries; i++) {
		    r_node->entry[i].count -= r_node->entry[c_r_idx].count;
                }
		
		/* delete the entries [0,c_r_idx] in r_node */
		r_node->header.nEntries -= (c_r_idx+1);
		n = r_node->header.nEntries;
		memmove(&r_node->entry[0], &r_node->entry[c_r_idx+1], n*sizeof(L_O_T_INodeEntry));
		
	    } else {
		/* Both root nodes of subtrees are in the same node, l_node */

		/* preparation for adjusting the count field */
		l_node->entry[c_l_idx].count = l_node->entry[c_r_idx].count;
		
		/* delete the entries [c_l_idx+1, c_r_idx] in l_node */
		n = l_node->header.nEntries - (c_r_idx+1);
		memmove(&l_node->entry[c_l_idx+1], &l_node->entry[c_r_idx+1],
			n*sizeof(L_O_T_INodeEntry));
		l_node->header.nEntries -= (c_r_idx - c_l_idx);
		
		/* adjust the count fields in l_node */
                for (i = c_l_idx; i < l_node->header.nEntries; i++) {
		    l_node->entry[i].count -= (to - from + 1);
	        }
            }
	    
	} else {
	    if (!IS_NILPAGEID(l_root)) {
		if (!IS_NILPAGEID(r_root) || to == TO_END) {
		    /* all entries [l_begin, ~) are deleted in l_node */
		    l_node->header.nEntries = l_begin;
		    
		    /* adjust the count fields */
		    /* if l_begin is 0, c_l_root_ptr has NULL. And
		     * c_r_root_ptr is NULL from above condition.
		     * It cannot be occurred
		     */
                    if (l_begin != c_l_idx) {
			l_node->entry[c_l_idx].count = c_list.entry[0].count + ((c_l_idx == 0) ? 0:l_node->entry[c_l_idx-1].count);
                    }

		} else {
		    if (l_begin <= l_end) {
			/* delete the entries [l_begin, l_end] in l_node */
			n = l_node->header.nEntries - (l_end+1);
			memmove(&l_node->entry[l_begin], &l_node->entry[l_end+1], 
				n*sizeof(L_O_T_INodeEntry));
			l_node->header.nEntries -= (l_end - l_begin +1);
		    }

		    if (c_l_idx != c_r_idx) {
			/* adjust the count fields */
			if (c_l_idx != l_begin) {
			    /* that is, c_l_root_ptr != NULL */
			    l_node->entry[c_l_idx].count = c_list.entry[0].count + ((c_l_idx == 0) ? 0:l_node->entry[c_l_idx-1].count);
			}
                        for (i = l_begin; i < l_node->header.nEntries; i++) {
			    l_node->entry[i].count -= (to - from + 1);
                        }
			
		    } else {
			/* common cut-path on the child node */
                        for (i = c_l_idx; i < l_node->header.nEntries; i++) {
			    l_node->entry[i].count -= (to - from + 1);
		        }
	    	    }
	        }
            }
	    
	    if (!IS_NILPAGEID(r_root)) {
		/* delete the entries [0,c_r_idx-1] */

		r_node->header.nEntries -= c_r_idx;
		memmove(&r_node->entry[0], &r_node->entry[c_r_idx], 
			(r_node->header.nEntries)*sizeof(L_O_T_INodeEntry));
		
		/* adjust the count fields */
		deletedBytes = r_node->entry[0].count - c_list.entry[1].count;
                for (i = 0; i < r_node->header.nEntries; i++) {
		    r_node->entry[i].count -= deletedBytes;
                }

		/* change the which value */
		if (c_l_root_ptr == NULL)
		    *which = RIGHT;
		else
		    *which = c_which;
	    }
	}
	
    }

    /*@ Adjust the unused if root is with header */
    if (rootSlotNo != NIL)
	apage->header.unused +=
	    sizeof(L_O_T_INodeEntry) * (oldEntries - l_node->header.nEntries);
    
    /* Set the child node index:
       Here must come the setting. The value may be changed after this. */
    if (*which == LEFT)
	p_c_idx = c_l_idx;
    else
	p_c_idx = 0;
       
    l_uf = r_uf = FALSE;
    
    if (!IS_NILPAGEID(l_root)) {
	list->entry[0].count = (l_node->header.nEntries == 0) ?
	    0:l_node->entry[l_node->header.nEntries-1].count;
	l_uf = (l_node->header.nEntries < LOT_HALFENTRIES) ? TRUE:FALSE;

    }

    if (!IS_NILPAGEID(r_root)) {
	list->entry[1].count = (r_node->header.nEntries == 0) ?
	    0:r_node->entry[r_node->header.nEntries-1].count;
	r_uf = (r_node->header.nEntries < LOT_HALFENTRIES) ? TRUE:FALSE;
    }

    if (l_uf || r_uf) {
	if (!IS_NILPAGEID(l_root) && !IS_NILPAGEID(r_root)) {
	    e = lot_MergeOrDistribute(handle, catObjForFile, list, &movedEntries, uf, mf, dlPool, dlHead);
	    if (e < 0) {	/* call lot_MergeOrDistribute */
		if (!IS_NILPAGEID(l_root)) BfM_FreeTrain(handle, &l_root, PAGE_BUF);
		if (!IS_NILPAGEID(r_root)) BfM_FreeTrain(handle, &r_root, PAGE_BUF);
		ERR(handle, e);
	    }
	    
	    if(*mf) {
		/* merge was occured. */
		
		if(*which == RIGHT) {
		    *which = LEFT;
		    p_c_idx++;		    
		}
		
	    } else {
		/* distribution was occured. */
		
		if((*which == RIGHT) && l_uf) {
		    *which = LEFT;
		    p_c_idx++;
		    
		} else if (*which == LEFT && r_uf) {
		    *which = RIGHT;
		    p_c_idx = movedEntries - 1;
		}
	    }
		
	} else {
	    *uf = TRUE;
	    *mf = FALSE;
	}
	
    } else {
	*uf = FALSE;
	*mf = FALSE;
    }	    

    /*
     * To free the buffer is responsibility of the caller.
     * Actually the buffer will be freed in lot_RebalanceTree().
     */
    if (!IS_NILPAGEID(l_root) && *which != LEFT) {
	e = BfM_SetDirty(handle, &l_root, PAGE_BUF);
	if (e < 0) {
	    if (IS_NILPAGEID(r_root)) ERRB1(handle, e, &l_root, PAGE_BUF);
	    else ERRB2(handle, e, &l_root, PAGE_BUF, &r_root, PAGE_BUF);
	}
	
	e = BfM_FreeTrain(handle, &l_root, PAGE_BUF);
	if (e < 0) {
	    if (IS_NILPAGEID(r_root)) ERR(handle, e);
	    else ERRB1(handle, e, &r_root, PAGE_BUF);
	}
    }
    
    if (!IS_NILPAGEID(r_root)) {
	if (*mf) {
	    e = BfM_FreeTrain(handle, &r_root, PAGE_BUF);
	    if (e < 0) ERR(handle, e);
	    /*
	     * r_root was freed at lot_MergeOrDistribute
	     */
	    
	} else if (*which != RIGHT) {
	    e = BfM_SetDirty(handle, &r_root, PAGE_BUF);
	    if (e < 0) ERRB1(handle, e, &r_root, PAGE_BUF);
	    
	    e = BfM_FreeTrain(handle, &r_root, PAGE_BUF);
	    if (e < 0) ERR(handle, e);
	}
    }

    /* register the current node in path */
    if (*which == LEFT) {
	e = lot_PushPath(handle, path, &l_root, rootSlotNo, (L_O_T_INode *)apage, p_c_idx, c_uf);
	if (e < 0) ERRB1(handle, e, &l_root, PAGE_BUF);
	
    } else {	/* r_root is not NIL PageID */
	e = lot_PushPath(handle, path, &r_root, rootSlotNo, r_node, p_c_idx, c_uf);

	if (e < 0) ERRB1(handle, e, &r_root, PAGE_BUF);
    }

    return(eNOERROR);

} /* lot_DeleteFromObject() */
