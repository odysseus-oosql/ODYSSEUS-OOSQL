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
 * Module: lot_Util.c
 *
 * Description:
 *  includes the small functions needed in this level.
 *
 * Exporrts:
 *  Four lot_GetCount(L_O_T_INode*, Two)
 *  Four lot_InitInternal(PageID*, Four, Four)
 *  Four lot_GetNodePointer(Page*, Two, L_O_T_INode**)
 *  Four lot_IsTemporary(FileID*, Boolean*)
 */


#include "common.h"
#include "trace.h"
#include "BfM.h"
#include "LOT_Internal.h"
#include "SM_Internal.h"
#include "perThreadDS.h"
#include "perProcessDS.h"



/*@================================
 * lot_GetCount()
 *================================*/
/*
 * Function: Four lot_GetCount(L_O_T_INode*, Two)
 *
 * Description:
 *  Get the count of the subtree
 *
 * Returns:
 *  count of the subtree
 */
Four lot_GetCount(
    Four handle,
    L_O_T_INode 	*nodePtr,	/* IN pointer to the internal node */
    Two 		idx)		/* IN index of the subtree in parent */
{
    TR_PRINT(TR_LOT, TR1, ("lot_GetCount(handle, nodePtr=%P, idx=%ld)", nodePtr, idx));
    
    return (nodePtr->entry[idx].count - ((idx == 0) ? 0:nodePtr->entry[idx-1].count));
    
} /* lot_GetCount() */



/*@================================
 * lot_InitInternal()
 *================================*/
/*
 * Function: Four lot_InitInternal(PageID*, Four, Four)
 *
 * Description:
 *  Initialize the internal nodes
 *
 * Returns:
 *  error code
 */  
Four lot_InitInternal(
    Four handle,
    PageID *pids,		/* array of PageIDs */
    Four   nPids,		/* size of array in PageID */
    Four   height, 		/* height value of the node */
    Boolean isTmp)             
{
    Four e;			/* error number */
    Four i;
    L_O_T_INode *nodePtr;
    
    TR_PRINT(TR_LOT, TR1, ("lot_Internal(pids=%P, nPids=%ld, height=%ld)",
			 pids, nPids, height));

    for (i = 0; i < nPids; i++) {
  	e = BfM_GetNewTrain(handle, &pids[i], (char **)&nodePtr, PAGE_BUF);
	if (e < 0) ERR(handle, e);

	/*@ initialize the internal node */
        nodePtr->header.pid = pids[i];
	nodePtr->header.height = height;
	nodePtr->header.nEntries = 0;
		
        /* set page type */
        SET_PAGE_TYPE(nodePtr, LOT_I_NODE_TYPE);

        /* set temporary flag */
        if( isTmp ) SET_TEMP_PAGE_FLAG(nodePtr);
        else        RESET_TEMP_PAGE_FLAG(nodePtr);

	e = BfM_SetDirty(handle, &pids[i], PAGE_BUF);
	if (e < 0) ERRB1(handle, e, &pids[i], PAGE_BUF);

	e = BfM_FreeTrain(handle, &pids[i], PAGE_BUF);
	if (e < 0) ERR(handle, e);
    }

    return (eNOERROR);
    
} /* lot_InitInternal() */



/*@================================
 * lot_GetNodePointer()
 *================================*/
/*
 * Function: void lot_GetNodePointer(Page*, Two, L_O_T_INode**)
 *
 * Description:
 *  get a pointer to the internal node
 *
 * Returns:
 *  None
 */
void lot_GetNodePointer(
    Four		handle,
    Page                *apage,         /* IN page containing the root node */
    Two                 rootSlotNo,     /* IN slotNo if root is on the slotted page, NIL otherwise */
    L_O_T_INode         **nodePtr)      /* OUT root node to get */
{
    Two                 offset;         /* starting offset of object in slotted page */
    Object              *obj;           /* pointer to object in slotted page */
    
    TR_PRINT(TR_LOT, TR1,
             ("lot_GetNodePointer(apage=%P rootSlotNo=%ld nodePtr=%P",
	      apage, rootSlotNo, nodePtr)); 
    
    /*@ get the pointer to the node */
    if (rootSlotNo != NIL) {
	
	/* Get a pointer to the page, slot and object */
	offset = ((SlottedPage *)apage)->slot[-rootSlotNo].offset;
	obj = (Object *)&(((SlottedPage *)apage)->data[offset]);
	*nodePtr = (L_O_T_INode *)obj->data;
	
    } else {
		 
	/* the node pointer is just the page pointer */
	*nodePtr = (L_O_T_INode *)apage;
    }
    
} /* lot_GetNodePtr() */
    


/*@================================
 * lot_IsTemporary()
 *================================*/
/* 
 * Function: Four  lot_IsTemporary(FileID*, Boolean*)
 *
 * Description : 
 *  Check large object is temporary or not
 *
 * Returns :
 *  error code
 *    some errors caused by function calls
 *
 * Side effects:
 *  isTmp is filled with test result
 */

Four lot_IsTemporary(
    Four handle,
    FileID *fid,                /* IN file ID in which large object is located */
    Boolean *isTmp)             /* OUT flag which indicates B+ tree is temporary or not */
{
    Four e;
    Four i;


    TR_PRINT(TR_BTM, TR1,
             ("lot_IsTemporary(handle, fid=%P, isTmp=%P)", fid, isTmp));
    

    /*@ parameter checking */
    if (fid == NULL)     ERR(handle, eBADPARAMETER_LOT);
    if (isTmp == NULL)   ERR(handle, eBADPARAMETER_LOT);


    /*@ for each entry */
    for (i = 0; i < SM_PER_THREAD_DS(handle).smTmpFileIdTable.nEntries; i++)
        if (EQUAL_FILEID(*fid, SM_TMPFILEIDTABLE(handle)[i])) break;

    /*@ set isTmp */
    if (i < SM_PER_THREAD_DS(handle).smTmpFileIdTable.nEntries) 
	*isTmp = TRUE;
    else                              
	*isTmp = FALSE;


    return eNOERROR;
}

