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
 * Module :	lot_AppendToObject.c
 *
 * Description :
 *  Append data to the large object. 
 *
 * Exports :
 *  Four lot_AppendToObject(ObjectID*, PageID*, Two, Four, char*, L_O_T_ItemList*, Boolean*)
 */


#include "common.h"
#include "trace.h"
#include "Util.h"
#include "BfM.h"
#include "LOT_Internal.h"
#include "perThreadDS.h"
#include "perProcessDS.h"



/*@================================
 * lot_AppendToObject()
 *================================*/
/*
 * Function: Four lot_AppendToObject(ObjectID*, PageID*, Two, Four, char*, L_O_T_ItemList*, Boolean*)
 *
 * Description :
 *  Append data to the large object. 
 *  It is sufficient to handle the large object with lot_InsertToObject().
 * But this append function is designed specially for efficient storage
 * utilization.	User can make very large object with high store utilization
 * by successive call of append function.
 *
 * Returns:
 *  error code
 *    eMEMORYALLOCERR:	memory allocation error
 *    some errors caused by function calls
 *
 * NOTES:
 *  Caller should free the space allocated for the entries; itemList->entry
 *  if the overflow falg is set to TRUE.
 */
Four lot_AppendToObject(
    Four 		handle,		/* IN handle for multi threading */
    ObjectID            *catObjForFile, /* IN file information */
    PageID              *root,          /* IN root page's PageID */
    Two                 rootSlotNo,     /* IN slotNo if root is on the slotted page */
    Four                length,         /* IN amount of data to append */
    char                *data,          /* IN user buffer holding the data */
    L_O_T_ItemList      *itemList,      /* OUT items to be inserted in the parent */
    Boolean             *overflow)      /* OUT overflow flag */
{
    Four                e;              /* error number */
    Page                *apage;         /* pointer to buffer holding page having root */
    L_O_T_INode         *anode;         /* subtree's root node */
    Two                 childIdx;       /* index of the subtree entry */
    PageID              childPid;       /* PageID of the root of subtree */
    Boolean             childOf;        /* overflow flag indicating child overflow */
    L_O_T_ItemList      childList;      /* list to give the subtree for the overflowed items */
    
    TR_PRINT(TR_LOT, TR1,
            ("lot_AppendToObject(handle, catObjForFile=%P, root=%P, length=%ld, data=%P, itemList=%P, overflow=%P)",
	   catObjForFile, root, length, data, itemList, overflow));
    
    /*@ read the page containing the root node into the buffer */
    e = BfM_GetTrain(handle, root, (char **)&apage, PAGE_BUF);
    if (e < 0) ERR(handle, e);

    /*@ get the node pointer */
    lot_GetNodePointer(handle, apage, rootSlotNo, &anode);
    
    *overflow = FALSE;
    
    /* the child is the last slot in the current node */
    childIdx = anode->header.nEntries - 1;
    MAKE_PAGEID(childPid, root->volNo, anode->entry[childIdx].spid);
    
    if (anode->header.height > 1) {     /* internal node except the lowest */
	
	/*@ recursively calls the insert routine */
	e = lot_AppendToObject(handle, catObjForFile, &childPid, NIL, length, data, &childList, &childOf);
	if (e < 0) ERRB1(handle, e, root, PAGE_BUF);
	
    } else {	/* in the case of the lowest internal node */
	/*@ prepare the localItemList */
	childList.nEntries = 3 + (length/LOT_LNODE_MAXFREE);

        childList.entryArrayPtr = LOT_GET_ENTRY_ARRAY(handle); /* insert a handle into LOT_GET_ENTRY_ARRAY */
        e = Util_reallocVarArray(handle, childList.entryArrayPtr, sizeof(L_O_T_INodeEntry), childList.nEntries);
        if (e < 0) ERRB1(handle, e, root, PAGE_BUF);
        
	childList.entry = (L_O_T_INodeEntry *)childList.entryArrayPtr->ptr;
    
	if (childIdx == 0 || lot_GetCount(handle, anode, childIdx-1) == LOT_LNODE_MAXFREE) {
	    childList.entry[0].spid = anode->entry[childIdx].spid;
	    childList.entry[0].count = lot_GetCount(handle, anode, childIdx);
	    childList.nEntries = 1;
	    childList.nReplaces = 1;
	} else {
	    childList.entry[0].spid = anode->entry[childIdx-1].spid;
	    childList.entry[0].count = lot_GetCount(handle, anode, childIdx-1);
	    childList.entry[1].spid = anode->entry[childIdx].spid;
	    childList.entry[1].count = lot_GetCount(handle, anode, childIdx);
	    childList.nEntries = 2;
	    childList.nReplaces = 2;
	}

	/*@ append the data into the data page */
	e = lot_AppendToDataPage(handle, catObjForFile, length, data, &childList, &childOf);
	if (e < 0) ERRB1(handle, e, root, PAGE_BUF);

	/* When two last pages are concerned, insertion starts at the 2nd
	   last page position */
	if (childOf && childList.nReplaces == 2)
	    childIdx --;
    }
    
    if (childOf) {
	/* Insert the new overflowed items */
	e = lot_InsertInternal(handle, catObjForFile, root, rootSlotNo, childIdx,
			       &childList, itemList, overflow);
	if (e < 0) ERRB1(handle, e, root, PAGE_BUF);
	
    } else {

	/*@ update the entry counts */
	anode->entry[childIdx].count += length;

    }

    e = BfM_SetDirty(handle, root, PAGE_BUF);
    if (e < 0) ERRB1(handle, e, root, PAGE_BUF);
    
    e = BfM_FreeTrain(handle, root, PAGE_BUF);
    if (e < 0) ERR(handle, e);
    
    return (eNOERROR);

} /* lot_AppendToObject() */
