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
 * Module : lot_InsertInternal.c
 *
 * Description:
 *  Insert the L_O_T_ItemList 'itemList' into the internal node.
 *
 * Export:
 *  Four lot_InsertInternal(ObjectID*, PageID*, Two, Two, L_O_T_ItemList*, L_O_T_ItemList*, Boolean*)
 *
 * Returns:
 *  error code
 *    eMEMALLOCERR_LOT
 *    some errors caused by function calls
 */


#include <string.h>
#include "common.h"
#include "trace.h"
#include "RDsM_Internal.h"	
#include "BfM.h"
#include "Util.h"
#include "LOT_Internal.h"
#include "perThreadDS.h"
#include "perProcessDS.h"



/*@================================
 * lot_InsertInternal()
 *================================*/
/*
 * Function: Four lot_InsertInternal(ObjectID*, PageID*, Two, Two, L_O_T_ItemList*, L_O_T_ItemList*, Boolean*)
 *
 * Description:
 *  Insert the L_O_T_ItemList 'itemList' into the internal node.
 *
 * Returns:
 *  error code
 *    some errors caused by function calls
 */
Four lot_InsertInternal(
    Four 			handle,		/* IN handle for multi threading */
    ObjectID                    *catObjForFile, /* IN file Information */
    PageID                      *pid,           /* IN into which new item(s) is inserted */
    Two                         rootSlotNo,     /* IN slot no of object if root is with header */
    Two                         idx,            /* IN start position of insert */
    L_O_T_ItemList              *list,          /* IN item list to be inserted */
    L_O_T_ItemList              *newList,       /* OUT item list to be inserted into the parent node */
    Boolean                     *overflow)      /* OUT overflow flag */
{
    Four                        e;              /* error number */
    FileID                      fid;            /* ID of file where the large object is placed */
    Two                         eff;            /* data file's extent fill factor */
    Four                        firstExt;       /* first Extent Number of the file */
    Four                        oldEntries;     /* # of entries in original node */
    Four                        movedEntries;   /* moved entries for current node at that time */
    Four                        totalEntries;   /* # of entries considered */
    Four                        nEntries;       /* # of entries for current node to hold */
    Four                        listEntries;    /* remained entries in the 'list' */
    Four                        nNodes;         /* # of nodes to hold the total entries */
    Four                        entriesPerNode; /* # of entries for 1 node to hold */
    Four                        remains;        /* # of entries remaind after balancing */
    SlottedPage                 *apage;         /* pointer to buffer holding page having root */
    Two                         offset;         /* starting offset of object header */
    Object                      *obj;           /* pointer to object header in slotted page */
    Four                        len;            /* size of root node when root is in slotted page */
    PageID                      root;           /* root page separated from the slotted page */
    PageID                      *newPids;       /* array of PageIDs to be newly allocated */
    L_O_T_INode                 *anode;         /* pointer to the node */
    L_O_T_INode                 tmpNode;        /* save the original node */
    L_O_T_INodeEntry            *entryPtr;      /* pointer to entry array of temporary node */
    Four                        src;            /* where the entries comes for current node? */
    Four                        toIdx;          /* yet unused entries before idx location */
    SlottedPage                 *catPage;       /* buffer page containing the catalog object */
    sm_CatOverlayForData        *catEntry;      /* overay structure for catalog object access */
    Four                        i, j, n;
    Boolean                     isTmp;          
    
    TR_PRINT(TR_LOT, TR1,
             ("lot_InsertInternal(handle, catObjForFile=%P, pid=%P, idx=%ld, list=%P, newList=%P, overflow=%P)",
	      catObjForFile, pid, idx, list, newList, overflow));
    
    /* Get the file ID & extent fill factor from the catalog object. */
    e = BfM_GetTrain(handle, (TrainID*)catObjForFile, (char**)&catPage, PAGE_BUF);
    if (e < 0) ERR(handle, e);

    GET_PTR_TO_CATENTRY_FOR_DATA(catObjForFile, catPage, catEntry);

    fid = catEntry->fid;
    eff = catEntry->eff;
    
    e = BfM_FreeTrain(handle, (TrainID*)catObjForFile, PAGE_BUF);
    if (e < 0) ERR(handle, e);

    /*@ read the internal page in */
    e = BfM_GetTrain(handle, pid, (char **)&apage, PAGE_BUF);
    if (e < 0) ERR(handle, e);

    /* Get the point to the node */
    lot_GetNodePointer(handle, (Page *)apage, rootSlotNo, &anode);
    
    *overflow = FALSE;

    oldEntries = anode->header.nEntries;
    totalEntries = anode->header.nEntries + list->nEntries - list->nReplaces;
    
    /* handle the special case; the root node is in the slotted page */
    if (rootSlotNo != NIL) {
	
	if (totalEntries <= LOT_MAXENTRIES_ROOTWITHHDR &&
	    (totalEntries-oldEntries) * sizeof(L_O_T_INodeEntry) <= SP_FREE(apage)) {
	    /* insert the entries into the root in slotted page */

	    e = lot_InsertInRootWithHdr(handle, apage, rootSlotNo, idx, list);
	    if (e < 0) ERRB1(handle, e, pid, PAGE_BUF);

	    e = BfM_SetDirty(handle, pid, PAGE_BUF);
	    if (e < 0) ERRB1(handle, e, pid, PAGE_BUF);
	    
	    e = BfM_FreeTrain(handle, pid, PAGE_BUF);
	    if (e < 0) ERR(handle, e);
	    
	    return (eNOERROR);
	    
	}
	
	e = lot_SeparateRootNode(handle, catObjForFile, pid, anode, &root); 
	if (e < 0) ERRB1(handle, e, pid, PAGE_BUF);
	
	offset = apage->slot[-rootSlotNo].offset;
	obj = (Object *)&(apage->data[offset]);
	len = sizeof(L_O_T_INodeHdr) + oldEntries*sizeof(L_O_T_INodeEntry);
	
	/* clear the property */
	obj->header.properties ^= P_LRGOBJ_ROOTWITHHDR;
	
	/* store the root PageID */
	*((ShortPageID *)obj->data) = root.pageNo;
	
	if (apage->header.free == offset + sizeof(ObjectHdr) + len)
	    /* this object is at the end of the slotted page  */
	    apage->header.free -= len - sizeof(ShortPageID);
	else
	    apage->header.unused += len - sizeof(ShortPageID);
	
	e = BfM_SetDirty(handle, pid, PAGE_BUF);
	if (e < 0) ERRB1(handle, e, pid, PAGE_BUF);
	
	e = BfM_FreeTrain(handle, pid, PAGE_BUF);
	if (e < 0) ERR(handle, e);
	
	/* now pid the new root node */
	pid = &root;
	e = BfM_GetTrain(handle, pid, (char **)&anode, PAGE_BUF);
	if (e < 0) ERR(handle, e);
    }

    /* change the count to subtree count instead of accumulated count */
    for (j = anode->header.nEntries - 1; j > 0; j--) {
	anode->entry[j].count -= anode->entry[j-1].count;
    }

    /* removing the entries to be replaced */
    movedEntries = anode->header.nEntries - idx - list->nReplaces;
    memmove(&anode->entry[idx], &anode->entry[idx+list->nReplaces], 
	    movedEntries*sizeof(L_O_T_INodeEntry));
    anode->header.nEntries -= list->nReplaces;
        
    if (list->nEntries + anode->header.nEntries <= LOT_MAXENTRIES) {
	/* There is enough space to accomadate the new item(s) */
	
	/* prepare the space moving the entries */
	movedEntries = anode->header.nEntries - idx;
	memmove(&anode->entry[idx+list->nEntries], &anode->entry[idx], 
		movedEntries*sizeof(L_O_T_INodeEntry));
	
	/*@ copy new entries into the system buffer */
	memcpy((char*)&anode->entry[idx], (char*)list->entry, list->nEntries*sizeof(L_O_T_INodeEntry));
	      
	anode->header.nEntries += list->nEntries;
	
	/*@ adjust the count fields */
        for (j = 1; j < anode->header.nEntries; j++) {
	    anode->entry[j].count += anode->entry[j-1].count;
        }
	
	e = BfM_SetDirty(handle, pid, PAGE_BUF);
	if (e < 0) ERRB1(handle, e, pid, PAGE_BUF);
    } else {
	/* save the original node */
	tmpNode = *anode;
	
	totalEntries = anode->header.nEntries + list->nEntries;
	nNodes = totalEntries/LOT_MAXENTRIES + ((totalEntries % LOT_MAXENTRIES > 0) ? 1:0);
	
	entriesPerNode = totalEntries / nNodes;
	
	/* The remain entries 'remains' are distributed evenly from the first node */
	remains = totalEntries % nNodes;
	
	/* get memory for saving the PageIDs of nodes (both old and new) */
        e = Util_reallocVarArray(handle, &LOT_PER_THREAD_DS(handle).lot_pageidArray, sizeof(PageID), nNodes); 
        if (e < 0) ERRB1(handle, e, pid, PAGE_BUF);
	newPids = (PageID *)LOT_PER_THREAD_DS(handle).lot_pageidArray.ptr; 
	
	newPids[0] = *pid;
	/* allocat the needed nodes (# of Nodes = nNodes - 1) */
	e = RDsM_PageIdToExtNo(handle, (PageID *)&fid, &firstExt);
	if (e < 0) ERRB1(handle, e, pid, PAGE_BUF);
	
	e = RDsM_AllocTrains(handle, fid.volNo, firstExt, pid,
			     eff, nNodes - 1, PAGESIZE2, newPids+1); 
	if (e < 0) ERRB1(handle, e, pid, PAGE_BUF);
		
        e = lot_IsTemporary(handle, &fid, &isTmp);
	if (e < 0) ERRB1(handle, e, pid, PAGE_BUF);

    /* Initialize the internal nodes */
	e = lot_InitInternal(handle, newPids, nNodes, tmpNode.header.height, isTmp);
	if (e < 0) ERRB1(handle, e, pid, PAGE_BUF);
	
	/*@ make the newList */
	newList->nEntries = nNodes;
	newList->nReplaces = 1;
        newList->entryArrayPtr = LOT_GET_ENTRY_ARRAY(handle); /* insert a handle into LOT_GET_ENTRY_ARRAY */
        e = Util_reallocVarArray(handle, newList->entryArrayPtr, sizeof(L_O_T_INodeEntry), nNodes);
        if (e < 0) ERRB1(handle, e, pid, PAGE_BUF);
        
	newList->entry = (L_O_T_INodeEntry *)newList->entryArrayPtr->ptr;
    
	/* At first, take the entries from the original entries */
	/* If reach idx postition, use the new entries. */
	/* After the new entreis are used, use the entries after idx pos. */
	src = 0;		
	entryPtr = tmpNode.entry;
	toIdx = idx;		/* # of unused entries before idx position */
	for (i = 0; i < nNodes; i++) {
	    /* if currentPage is original page, use BfM_GetTrain() */
	    if (i < 1) {
		e = BfM_GetTrain(handle, &newPids[i], (char **)&anode, PAGE_BUF);
		if (e < 0) ERRB1(handle, e, pid, PAGE_BUF);
	    }
	    /* if currentPage is allocated new page, use BfM_GetNewTrain() */
	    else {
		e = BfM_GetNewTrain(handle, &newPids[i], (char **)&anode, PAGE_BUF);
		if (e < 0) ERRB1(handle, e, pid, PAGE_BUF);
	    }
	    
	    /* # of entries for this node to hold */
	    nEntries = entriesPerNode + ((remains > i) ? 1:0);

	    movedEntries = 0;
	    while (movedEntries < nEntries) {
		
		switch(src) {
		  case 0:	/* take the entries from the original entries */
		    n = MIN(nEntries - movedEntries, toIdx);
		    memcpy((char*)&anode->entry[movedEntries], (char*)entryPtr, n*sizeof(L_O_T_INodeEntry));
		    entryPtr += n;
		    toIdx -= n;
		    movedEntries += n;
		    
		    if (toIdx == 0) {
			src = 1; /* From next, use the new entries */
			listEntries = list->nEntries;
			entryPtr = list->entry;
		    }
		    
		    break;
		    
		  case 1:	/* take the entries from the new entries */
		    n = MIN(nEntries - movedEntries, listEntries);
		    memcpy((char*)&anode->entry[movedEntries], (char*)entryPtr, n*sizeof(L_O_T_INodeEntry));
		    entryPtr += n;
		    listEntries -= n;
		    movedEntries += n;
		    
		    if (listEntries == 0) {
			src = 2;	/* From now, use the original entries again */
			entryPtr = &tmpNode.entry[idx];
		    }
		    
		    break;
		    
		  case 2:	/* take the entries from the original entries */
		    n = nEntries - movedEntries;
		    memcpy((char*)&anode->entry[movedEntries], (char*)entryPtr, n*sizeof(L_O_T_INodeEntry));
		    entryPtr += n;
		    movedEntries += n;
		    break;
		    
		} /* end of switch */
	    }
	    
	    /*@ adjust the count fields */
            for (j = 1; j < nEntries; j++) {
		anode->entry[j].count += anode->entry[j-1].count;
            }
	    
	    anode->header.nEntries = nEntries;

	    /*@ make the newList */
	    newList->entry[i].spid = newPids[i].pageNo;
	    newList->entry[i].count = anode->entry[nEntries-1].count;
	    
	    e = BfM_SetDirty(handle, &newPids[i], PAGE_BUF);
	    if (e < 0) ERRB2(handle, e, &newPids[i], PAGE_BUF, pid, PAGE_BUF);
	    
	    e = BfM_FreeTrain(handle, &newPids[i], PAGE_BUF);
	    if (e < 0) ERRB1(handle, e, pid, PAGE_BUF);
	    
	} /* end of for */


	*overflow = TRUE;
    }
    
    e = BfM_FreeTrain(handle, pid, PAGE_BUF);
    if (e < 0) ERR(handle, e);
    
    
    return(eNOERROR);

} /* lot_InsertInternal() */
