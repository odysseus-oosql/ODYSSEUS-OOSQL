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
 * Module: lot_MakeRootWithHdr.c
 *
 * Description:
 *  If the given root node can be accomadated in slotted page, the root node
 * is stored in slotted page with object header.
 *
 * Exports:
 *  Four lot_MakeRootWithHdr(PageID*, SlottedPage*, Two, PageID*)
 */


#include <string.h>
#include "common.h"
#include "RDsM_Internal.h"	
#include "trace.h"
#include "BfM.h"
#include "Util.h"
#include "LOT_Internal.h"
#include "perThreadDS.h"
#include "perProcessDS.h"



/*@================================
 * lot_MakeRootWithHdr()
 *================================*/
/*
 * Function: Four lot_MakeRootWithHdr(PageID*, SlottedPage*, Two, PageID*)
 *
 * Description:
 *  If the given root node can be accomadated in slotted page, the root node
 * is stored in slotted page with object header.
 *
 * Returns:
 *  error code
 *    some errors caused by function calls
 */
Four lot_MakeRootWithHdr(
    Four handle,
    PageID              *pid,           /* IN slotted page containing the object */
    SlottedPage         *apage,         /* INOUT pointer to buffer holding slotted page */
    Two                 slotNo,         /* IN slot No of object */
    PageID              *root,          /* IN PageID of root node */
    Pool                *dlPool,        /* INOUT pool of dealloc list elements */
    DeallocListElem     *dlHead)        /* INOUT head of the dealloc list */
{
    Four                e;              /* error number */
    Two                 offset;         /* starting offset of object */
    Object              *obj;           /* points to the object header */
    L_O_T_INode         *r_node;        /* pointer to root node */
    L_O_T_INode         *anode;         /* pointer to new root node in slotted page */
    Four                neededSpace;    /* spaced needed for storing the root node */
    DeallocListElem     *dlElem;        /* an element of the dealloc list */

    TR_PRINT(TR_LOT, TR1,
             ("lot_MakeRootWithHdr(handle, apage=%P, slotNo=%ld, root=%P)",
	      apage, slotNo, root));

    e = BfM_GetTrain(handle, root, (char **)&r_node, PAGE_BUF);
    if (e < 0) ERR(handle, e);

    neededSpace = sizeof(L_O_T_INodeHdr) +
	sizeof(L_O_T_INodeEntry)*r_node->header.nEntries;
    
    if(r_node->header.nEntries > LOT_MAXENTRIES_ROOTWITHHDR ||
       neededSpace > (SP_FREE(apage) + sizeof(ShortPageID))) {

	e = BfM_FreeTrain(handle, root, PAGE_BUF);
	if (e < 0) ERR(handle, e);

	return(eNOERROR);
    }

    
    if (SP_CFREE(apage) >= sizeof(ObjectHdr) + neededSpace) {

	/*@ obj points to the old object */
	offset = apage->slot[-slotNo].offset;
	obj = (Object *)&(apage->data[offset]);
	
	/* move the old object to the end of the data area */
	offset = apage->header.free;

	/*@ copy the object header */
	memcpy(&(apage->data[offset]), (char*)&(obj->header), sizeof(ObjectHdr));

	/* now, obj points to the moved object */
	obj = (Object *)&(apage->data[offset]);
	anode = (L_O_T_INode *)obj->data;

	/* change the offset of slot */
	apage->slot[-slotNo].offset = offset;
	
	apage->header.unused += sizeof(ObjectHdr) + sizeof(ShortPageID);
	apage->header.free += sizeof(ObjectHdr) + neededSpace;
	
    } else {
	OM_CompactPage(handle, apage, slotNo);

	offset = apage->slot[-slotNo].offset;
	obj = (Object *)&(apage->data[offset]);
	anode = (L_O_T_INode *)obj->data;

	apage->header.free -= sizeof(ShortPageID) - neededSpace;
    }
    
    /* mark the root node is with the header. */
    obj->header.properties |= P_LRGOBJ_ROOTWITHHDR;

    /* copy the node */
    memcpy((char*)anode, (char*)r_node,
	       sizeof(L_O_T_INodeHdr) + sizeof(L_O_T_INodeEntry)*r_node->header.nEntries);

    /*@ free the root node */
    e = BfM_FreeTrain(handle, root, PAGE_BUF);
    if(e < 0) ERR(handle, e);

    /* remove page from buffer pool */
    e = BfM_RemoveTrain(handle, root, PAGE_BUF, FALSE);
    if (e < 0) ERR(handle, e);

    e = Util_getElementFromPool(handle, dlPool, &dlElem);
    if (e < 0) ERR(handle, e);

    dlElem->type = DL_PAGE;
    dlElem->elem.pid = *root; /* save the PageID. */
    dlElem->next = dlHead->next; /* insert to the list */
    dlHead->next = dlElem;       /* new first element of the list */
    
    e = BfM_SetDirty(handle, pid, PAGE_BUF);
    if(e < 0) ERR(handle, e);

    return(eNOERROR);

} /* lot_MakeRootWithHdr() */
