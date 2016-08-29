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
 * Module: lot_InsertInRootWithHdr.c
 *
 * Description:
 *
 * Exports:
 *  Four lot_InsertInRootWithHdr(SlottedPage*, Two, Two, L_O_T_ItemList*)
 */


#include <string.h>
#include "common.h"
#include "trace.h"
#include "LOT_Internal.h"
#include "OM_Internal.h"
#include "perThreadDS.h"
#include "perProcessDS.h"



/*@================================
 * lot_InsertInRootWithHdr()
 *================================*/
/*
 * Function: Four lot_InsertInRootWithHdr(SlottedPage*, Two, Two, L_O_T_ItemList*)
 *
 * Description:
 *
 * Retruns:
 *  error code
 *    some errors caused by function calls
 *
 * Note:
 *  Assume that there is enough room to store the entries
 */
Four lot_InsertInRootWithHdr(
    Four handle,
    SlottedPage         *apage,         /* INOUT pointer to buffer holding the page */
    Two                 rootSlotNo,     /* IN slot no of the object */
    Two                 idx,            /* IN start position of insert */
    L_O_T_ItemList      *list)          /* IN list to insert */
{
    Four                e;              /* error number */
    Object              *obj;           /* point to the object in page */
    Two                 offset;         /* offset in data area of the object */
    Four                len;            /* length of internal node */
    Four                neededSpace;    /* length of internal node after insert */
    L_O_T_INode         *anode;         /* pointer to the internal node */
    Boolean             last;           /* Is the object at the end of data area? */
    Two                 i;

    TR_PRINT(TR_LOT, TR1,
             ("lot_InsertInRootWithHdr(apage=%P, rootSlotNo=%ld, idx=%ld, list=%P",
	      apage, rootSlotNo, idx, list));

    offset = apage->slot[-rootSlotNo].offset;
    obj = (Object *)&(apage->data[offset]);
    anode = (L_O_T_INode *)obj->data;
    len = sizeof(L_O_T_INodeHdr) + sizeof(L_O_T_INodeEntry)*anode->header.nEntries;
    
    /* At first, change count field values from the accmulated values to
       individual count */
    for (i = anode->header.nEntries-1; i >= idx && i > 0; i--) {
	anode->entry[i].count -= anode->entry[i-1].count;
    }

    /* delete the replaced entries */
    memmove(&(anode->entry[idx]), &(anode->entry[idx+list->nReplaces]),
	    sizeof(L_O_T_INodeEntry)*(anode->header.nEntries-idx-list->nReplaces));
    anode->header.nEntries -= list->nReplaces;

    /*@ adjust the unused or free */
    if (apage->header.free == offset + sizeof(ObjectHdr) + len) {
	last = TRUE;
	apage->header.free -= sizeof(L_O_T_INodeEntry)*list->nReplaces;
    } else {
	last = FALSE;
	apage->header.unused += sizeof(L_O_T_INodeEntry)*list->nReplaces;
    }
    
    neededSpace = sizeof(L_O_T_INodeHdr) +
	sizeof(L_O_T_INodeEntry)*(anode->header.nEntries+list->nEntries);
    
    if (last &&
	sizeof(L_O_T_INodeEntry)*list->nEntries <= SP_CFREE(apage)) {
	/* This object is the last object in the page. */
	/* move the entries after the insertion point */
	
	memmove(&(anode->entry[idx+list->nEntries]), &(anode->entry[idx]), 
		sizeof(L_O_T_INodeEntry)*(anode->header.nEntries-idx));
	
	/*@ insert the data */
	memcpy((char*)&(anode->entry[idx]), (char*)list->entry, sizeof(L_O_T_INodeEntry)*list->nEntries);
	
	apage->header.free += sizeof(L_O_T_INodeEntry)*list->nEntries;
	
    } else if (sizeof(ObjectHdr) + neededSpace <= SP_CFREE(apage)) {
	/* The contiguous free space can accomadate full object */
	
	/* copy the original object to the last data space */
	offset = apage->header.free;
	memcpy(&(apage->data[offset]), (char*)obj,
	       sizeof(ObjectHdr) + sizeof(L_O_T_INodeHdr) + sizeof(L_O_T_INodeEntry)*idx);
	memcpy((char*)&(apage->data[offset+sizeof(ObjectHdr)+sizeof(L_O_T_INodeHdr)+sizeof(L_O_T_INodeEntry)*(idx+list->nEntries)]),
	       (char*)&(anode->entry[idx]),
	       sizeof(L_O_T_INodeEntry)*(anode->header.nEntries-idx));
	      
	/* Now obj points to the new object */
	obj = (Object *)(&(apage->data[offset]));
	anode = (L_O_T_INode *)obj->data;
	
	/*@ insert the new data */
	memcpy((char*)&(anode->entry[idx]), (char*)list->entry, sizeof(L_O_T_INodeEntry)*list->nEntries);
	
	apage->slot[-rootSlotNo].offset = offset;
	apage->header.free += sizeof(ObjectHdr) + neededSpace;
	apage->header.unused += sizeof(ObjectHdr) + sizeof(L_O_T_INodeHdr) +
	    sizeof(L_O_T_INodeEntry)*anode->header.nEntries;
	
    } else {
	/* Complex Case: Compact the data page and insert it */
	e = OM_CompactPage(handle, apage, rootSlotNo);
	if (e < 0) ERR(handle, e);
	
	/* Now obj points to the new object */
	offset = apage->slot[-rootSlotNo].offset;
	obj = (Object *)(&(apage->data[offset]));
	anode = (L_O_T_INode *)obj->data;
	
	/* move the bytes after the insertion point */
	memmove(&(anode->entry[idx+list->nEntries]), &(anode->entry[idx]), 
		sizeof(L_O_T_INodeEntry)*(anode->header.nEntries-idx));
	/* insert the new data */
	memcpy((char*)&(anode->entry[idx]), (char*)list->entry, sizeof(L_O_T_INodeEntry)*list->nEntries);
	
	apage->header.free += sizeof(L_O_T_INodeEntry)*list->nEntries;
    }

    anode->header.nEntries += list->nEntries;
    
    /* change count field values from the accmulated values to individual count */
    for (i = MAX(1, idx); i < anode->header.nEntries; i++) {
	anode->entry[i].count += anode->entry[i-1].count;
    }
    
    return(eNOERROR);

} /* lot_InsertInRootWithHdr() */
