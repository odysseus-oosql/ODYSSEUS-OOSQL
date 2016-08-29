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
 * Module: LOT_DropObject.c
 *
 * Description:
 *  Drop the specified large object tree.
 *
 * Exports:
 *  Four LOT_DestroyObject(PageID*, Two, Pool*, DeallocListElem*)
 */


#include "common.h"
#include "trace.h"
#include "Util.h"
#include "RDsM_Internal.h"	
#include "BfM.h"
#include "LOT_Internal.h"
#include "OM_Internal.h"
#include "perThreadDS.h"
#include "perProcessDS.h"



/*@================================
 * LOT_DropObject()
 *================================*/
/*
 * Function: Four LOT_DestroyObject(PageID*, Two, Pool*, DeallocListElem*)
 *
 * Description:
 *  Drop the specified large object tree.
 *
 * Returns:
 *  error code
 *    eBADPAGEID_LOT
 *    some errors caused by function calls
 *
 * Side Effects:
 *  The large object tree is dropped.
 */
Four LOT_DestroyObject(
    Four handle,
    PageID              *pid,           /* IN page containing the object */
    Two                 slotNo,         /* IN slot no of object */
    Pool                *dlPool,        /* INOUT pool of dealloc list elements */
    DeallocListElem     *dlHead)        /* INOUT head of the dealloc list */

{
    Four                e;              /* error nubmer */
    Two                 i;              /* index variable */
    SlottedPage         *apage;         /* pointer to buffer holding the slotted page */
    Two                 offset;         /* starting offset of object in slotted page */
    Object              *obj;           /* pointer to object in slotted page */
    L_O_T_INode         *anode;         /* pointer to the root node */
    Two                 len;            /* length of internal node in slotted page */
    PageID              root;           /* the root of the large object tree */
    PageID              child;          /* child PageID */
    DeallocListElem     *dlElem;        /* an element of the dealloc list */
     
    TR_PRINT(TR_LOT, TR1, ("LOT_DestroyObject(handle, pid=%P, slotNo=%ld)", pid, slotNo));

    /*@ check parameters */
    if (pid == NULL || IS_NILPAGEID(*pid))	/* pid is NULL */
	ERR(handle, eBADPAGEID_LOT);

    /*@ read the slotted page into the buffer */
    e = BfM_GetTrain(handle, pid, (char **)&apage, PAGE_BUF);
    if (e < 0) ERR(handle, e);

    offset = apage->slot[-slotNo].offset;
    obj = (Object *)&(apage->data[offset]);

    if (obj->header.properties & P_LRGOBJ_ROOTWITHHDR) {
	anode = (L_O_T_INode *)obj->data;

	for (i = 0; i < anode->header.nEntries; i++) {
	    
	    /*@ construct the child's PageID */
	    MAKE_PAGEID(child, pid->volNo, anode->entry[i].spid);
	    
	    if (anode->header.height == 1) {	/* the deepest internal node */

                /* remove leaf data page from buffer pool */
                e = BfM_RemoveTrain(handle, &child, LOT_LEAF_BUF, FALSE);
                if (e < 0) ERRB1(handle, e, pid, PAGE_BUF);
		
		/* deallocate the leaf data page */
                e = Util_getElementFromPool(handle, dlPool, &dlElem);
                if (e < 0) ERRB1(handle, e, pid, PAGE_BUF);

                dlElem->type = DL_PAGE;
                dlElem->elem.pid = child; /* save the PageID. */
                dlElem->next = dlHead->next; /* insert to the list */
                dlHead->next = dlElem;       /* new first element of the list */
		
	    } else {		/* the internal node except the deepest internal node */
		
		/*@ recursive call to drop the subtree */ 
		e = lot_DropTree(handle, &child, dlPool, dlHead);
		if (e < 0) ERRB1(handle, e, pid, PAGE_BUF);
	    }
	}

	len = sizeof(L_O_T_INodeHdr) + sizeof(L_O_T_INodeEntry)*anode->header.nEntries;
	if (apage->header.free == offset + sizeof(ObjectHdr) + len)
	    apage->header.free -= sizeof(ObjectHdr) + len;
	else
	    apage->header.unused += sizeof(ObjectHdr) + len;
		
	    
    } else {

	/* make root PageID */
	MAKE_PAGEID(root, pid->volNo, *((ShortPageID *)obj->data));
	
	/* delete the tree */
	e = lot_DropTree(handle, &root, dlPool, dlHead);
	if (e < 0) ERRB1(handle, e, pid, PAGE_BUF);

	if (apage->header.free == offset + sizeof(ObjectHdr) + sizeof(ShortPageID))
	    apage->header.free -= sizeof(ObjectHdr) + sizeof(ShortPageID);
	else
	    apage->header.unused += sizeof(ObjectHdr) + sizeof(ShortPageID);
    }

    e = BfM_SetDirty(handle, pid, PAGE_BUF);
    if (e < 0) ERRB1(handle, e, pid, PAGE_BUF);
    
    e = BfM_FreeTrain(handle, pid, PAGE_BUF);
    if (e < 0) ERR(handle, e);
    
    return(eNOERROR);
    
} /* LOT_DestroyObject() */
	    
	
