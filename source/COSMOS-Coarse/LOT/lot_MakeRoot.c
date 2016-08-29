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
 * Module: lot_MakeRoot.c
 *
 * Description:
 *  make the root from the items
 *
 * Exports:
 *  Four lot_MakeRoot(ObjectID*, PageID*, Two, Four, L_O_T_ItemList*)
 */


#include <string.h>
#include "common.h"
#include "trace.h"
#include "RDsM_Internal.h"	
#include "BfM.h"
#include "Util.h"
#include "LOT_Internal.h"
#include "OM_Internal.h"
#include "perThreadDS.h"
#include "perProcessDS.h"



/*@================================
 * lot_MakeRoot()
 *================================*/
/*
 * Function: Four lot_MakeRoot(ObjectID*, PageID*, Two, Four, L_O_T_ItemList*)
 *
 * Description:
 *  make the root from the items
 *
 * Returns:
 *  error code
 *    some errors caused by function calls
 */
Four lot_MakeRoot(
    Four 			handle,		/* IN handle for multi threading */
    ObjectID                    *catObjForFile, /* IN information for the file */
    PageID                      *pid,           /* IN slotted page containing the object */
    Two                         rootSlotNo,     /* IN slot no of the object in slotted page */
    Four                        height,         /* IN height of the original tree */
    L_O_T_ItemList              *itemList)      /* IN entries from which internal tree is constructed */
{
    Four                        e;              /* error number */
    FileID                      fid;            /* ID of file where the object tree is placed */
    Two                         eff;            /* data file's extent fill factor */
    Four                        firstExt;       /* first Extent No of the file */
    Four                        totalEntries;   /* total # of entries considered */
    Four                        nEntries;       /* # of entries fot the current node to hold */
    Four                        nNodes;         /* # of nodes needed to hold the items */
    Four                        entriesPerNode; /* # of entries for one page to hold */
    Four                        remains;        /* small fraction after balancing */
    PageID                      *newPids;       /* array of PageIDs to be newly allocated */
    PageID                      *currentPid;    /* PageID being currently processed */
    SlottedPage                 *apage;         /* pointer to buffer holding slotted page */
    Two                         offset;         /* starting offset of object header in slotted page */
    Object                      *obj;           /* pointer to object header in slotted page */
    Four                        len;            /* length of node when root is in slotted page */
    L_O_T_INode                 *nodePtr;       /* pointer to the node */
    L_O_T_INodeEntry            *entryPtr;      /* start of next copy */
    L_O_T_ItemList              localList;      /* list construected at this stage */
    PageID                      root;           /* the newly created root page's PageID */
    SlottedPage                 *catPage;       /* buffer page containing the catalog object */
    sm_CatOverlayForData        *catEntry;      /* overay structure for catalog object access */
    Four                        i, j;
    Boolean                     isTmp;          
    
    
    TR_PRINT(TR_LOT, TR1,
             ("lot_MakeRoot(handle, catObjForFile=%P, height=%ld, itemList=%P, root=%P)",
	      catObjForFile, height, itemList, root));

    /* Get the file ID & extent fill factor from the catalog object. */
    e = BfM_GetTrain(handle, (TrainID*)catObjForFile, (char**)&catPage, PAGE_BUF);
    if (e < 0) ERR(handle, e);

    GET_PTR_TO_CATENTRY_FOR_DATA(catObjForFile, catPage, catEntry);

    fid = catEntry->fid;
    eff = catEntry->eff;
    
    e = BfM_FreeTrain(handle, (TrainID*)catObjForFile, PAGE_BUF);
    if (e < 0) ERR(handle, e);
    
    e = lot_IsTemporary(handle, &fid, &isTmp);
    if (e < 0) ERR(handle, e);

    if (itemList->nEntries <= LOT_MAXENTRIES) {
	
	/* the slots can be put in one node */

	len = sizeof(L_O_T_INodeHdr) + itemList->nEntries*sizeof(L_O_T_INodeEntry);

	/*@ read the slotted page into the buffer */
	e = BfM_GetTrain(handle, pid, (char **)&apage, PAGE_BUF);
	if (e < 0) ERR(handle, e);
	
	offset = apage->slot[-rootSlotNo].offset;
	obj = (Object *)&(apage->data[offset]);
		
	if (itemList->nEntries <= LOT_MAXENTRIES_ROOTWITHHDR &&
	    len <= SP_FREE(apage) + sizeof(ShortPageID)) {
	    /* make the root node in the slotted page */
	    
	    if (apage->header.free == offset+sizeof(ObjectHdr)+sizeof(ShortPageID) &&
		len <= SP_CFREE(apage) + sizeof(ShortPageID)) {
		/* do nothing */
		
	    } else if (len + sizeof(ObjectHdr) <= SP_CFREE(apage)) {
		offset = apage->header.free;
		*((ObjectHdr *)&(apage->data[offset])) = obj->header;
		apage->slot[-rootSlotNo].offset = offset;
		
		/* Now, obj points to the moved object */
		obj = (Object *)&(apage->data[offset]);

		apage->header.free += sizeof(ObjectHdr) + sizeof(ShortPageID);
		
	    } else {
		OM_CompactPage(handle, apage, rootSlotNo);

		offset = apage->slot[-rootSlotNo].offset;
		obj = (Object *)&(apage->data[offset]);
	    }
	    
	    nodePtr = (L_O_T_INode *)obj->data;

	    nodePtr->header.height = height;
	    memcpy((char*)nodePtr->entry, (char*)itemList->entry, 
		       itemList->nEntries*sizeof(L_O_T_INodeEntry));
	    nodePtr->header.nEntries = itemList->nEntries;
	    
	    /*@ adjust the count fields */
            for (j = 1; j < nodePtr->header.nEntries; j++) {
		nodePtr->entry[j].count += nodePtr->entry[j-1].count;
            }

	    /*@ set the property */
	    obj->header.properties |= P_LRGOBJ_ROOTWITHHDR;

	    apage->header.free += len - sizeof(ShortPageID);
	    
	    e = BfM_SetDirty(handle, pid, PAGE_BUF);
	    if (e < 0) ERRB1(handle, e, pid, PAGE_BUF);
	    
	    e = BfM_FreeTrain(handle, pid, PAGE_BUF);
	    if (e < 0) ERR(handle, e);
	    
	    /* set root to NIL */
	    SET_NILPAGEID(root);

	} else {
	    
	    /* get the first extent number */
	    e = RDsM_PageIdToExtNo(handle, (PageID *)&fid, &firstExt);
	    if (e < 0) ERRB1(handle, e, pid, PAGE_BUF);
	    
	    /* Allocate one page for the root node */	
	    e = RDsM_AllocTrains(handle, fid.volNo, firstExt, pid,
				 eff, 1, PAGESIZE2, &root); 
	    if (e < 0) ERRB1(handle, e, pid, PAGE_BUF);

	    /* store the root PageNo */
	    *((ShortPageID *)obj->data) = root.pageNo;
	    
	    /*@ free the buffer */
	    e = BfM_FreeTrain(handle, pid, PAGE_BUF);
	    if (e < 0) ERR(handle, e);
	    
	    e = lot_InitInternal(handle, &root, 1, height, isTmp);
	    if (e < 0) ERR(handle, e);

	    e = BfM_GetNewTrain(handle, &root, (char **)&nodePtr, PAGE_BUF);
	    if (e < 0) ERR(handle, e);
	    
	    memcpy((char*)nodePtr->entry, (char*)itemList->entry, 
		       itemList->nEntries*sizeof(L_O_T_INodeEntry));
	    nodePtr->header.nEntries = itemList->nEntries;
	    
	    /*@ adjust the count fields */
            for (j = 1; j < nodePtr->header.nEntries; j++) {
		nodePtr->entry[j].count += nodePtr->entry[j-1].count;
            }
	    
	    e = BfM_SetDirty(handle, &root, PAGE_BUF);
	    if (e < 0) ERRB1(handle, e, &root, PAGE_BUF);
	    
	    e = BfM_FreeTrain(handle, &root, PAGE_BUF);
	    if (e < 0) ERR(handle, e);
	}
	
    } else {
	
	/* The slots cannot be put in one node */
	/* We recursively call the Lg_MakeL_O_T_Root() */
	
	totalEntries = itemList->nEntries;
	nNodes = totalEntries/LOT_MAXENTRIES + (((totalEntries % LOT_MAXENTRIES) > 0 ? 1:0));
	
	entriesPerNode = totalEntries / nNodes;
	
	/* The remain slots 'remains' are distributed evenly from the first node */
	remains = totalEntries % nNodes;
	
    e = Util_reallocVarArray(handle, &LOT_PER_THREAD_DS(handle).lot_pageidArray, sizeof(PageID), nNodes);
    if (e < 0) ERR(handle, e);
        
	newPids = (PageID *)LOT_PER_THREAD_DS(handle).lot_pageidArray.ptr;
        
	/* get the first extent number */
	e = RDsM_PageIdToExtNo(handle, (PageID *)&fid, &firstExt);
	if (e < 0) ERR(handle, e);

	/* allocate the needed nodes */
	e = RDsM_AllocTrains(handle, fid.volNo, firstExt, pid,
			     eff, nNodes, PAGESIZE2, newPids); 
	if (e < 0) ERR(handle, e);

	e = lot_InitInternal(handle, newPids, nNodes, height, isTmp);
	if (e < 0) ERR(handle, e);

	localList.nEntries = nNodes;
        localList.entryArrayPtr = LOT_GET_ENTRY_ARRAY(handle); /* insert a handle into LOT_GET_ENTRY_ARRAY */
        e = Util_reallocVarArray(handle, localList.entryArrayPtr, sizeof(L_O_T_INodeEntry), localList.nEntries);
        if (e < 0) ERR(handle, e);

        localList.entry = (L_O_T_INodeEntry *)localList.entryArrayPtr->ptr;
	
	/* Evenly distribute the items in itemList */
	entryPtr = itemList->entry;
	for (i = 0; i < nNodes; i++) {
	    currentPid = &newPids[i];
	    
	    e = BfM_GetNewTrain(handle, currentPid, (char **)&nodePtr, PAGE_BUF);
	    if (e < 0) ERR(handle, e);
	    
	    nEntries = entriesPerNode + ((remains > i) ? 1:0);
	    
	    memcpy((char*)nodePtr->entry, (char*)entryPtr, nEntries*sizeof(L_O_T_INodeEntry));
	    entryPtr += nEntries;

	    nodePtr->header.nEntries = nEntries;
	    
	    /* adjust the count fields */
            for(j = 1; j < nEntries; j++) {
		nodePtr->entry[j].count += nodePtr->entry[j-1].count;
            }

	    /* Construct the L_O_T_ItemList */
	    localList.entry[i].spid = currentPid->pageNo;
	    localList.entry[i].count = nodePtr->entry[nEntries-1].count;
	    
	    e = BfM_SetDirty(handle, currentPid, PAGE_BUF);
	    if (e < 0) ERRB1(handle, e, currentPid, PAGE_BUF);
	    
	    e = BfM_FreeTrain(handle, currentPid, PAGE_BUF);
	    if (e < 0) ERR(handle, e);
	}
	
	e = lot_MakeRoot(handle, catObjForFile, pid, rootSlotNo, height+1, &localList);
	if (e < 0) ERR(handle, e);
    }

    return (eNOERROR);

} /* lot_MakeRoot() */
