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
 * Module : OM_CompactPage.c
 * 
 * Description : 
 *  OM_CompactPage() reorganizes the page to make sure the unused bytes
 *  in the page are located contiguously "in the middle", between the tuples
 *  and the slot array. 
 *
 * Exports:
 *  Four OM_CompactPage(SlottedPage*, Two)
 */


#include <string.h>
#include "common.h"
#include "trace.h"		/* for tracing : TR_PRINT() macro */
#include "LOT.h"
#include "OM_Internal.h"
#include "perThreadDS.h"
#include "perProcessDS.h"



/*@================================
 * OM_CompactPage()
 *================================*/
/*
 * Function: Four OM_CompactPage(SlottedPage*, Two)
 * 
 * Description : 
 *  (1) What to do?
 *  OM_CompactPage() reorganizes the page to make sure the unused bytes
 *  in the page are located contiguously "in the middle", between the tuples
 *  and the slot array. To compress out holes, objects must be moved toward
 *  the beginning of the page.
 *
 *  (2) How to do?
 *  a. Save the given page into the temporary page
 *  b. FOR each nonempty slot DO
 *	Fill the original page by copying the object from the saved page
 *          to the data area of original page pointed by 'apageDataOffset'
 *	Update the slot offset
 *	Get 'apageDataOffet' to point the next moved position
 *     ENDFOR
 *   c. Update the 'freeStart' and 'unused' field of the page
 *   d. Return
 *	
 * Returns:
 *  error code
 *    eNOERROR
 *
 * Side Effects :
 *  The slotted page is reorganized to comact the space.
 */
Four OM_CompactPage(
    Four handle,
    SlottedPage	*apage,		/* IN slotted page to compact */
    Two         slotNo)		/* IN slotNo to go to the end */
{
    SlottedPage	tpage;				/* temporay page used to save the given page */
    Object 		*obj;				/* pointer to the object in the data area */
    Two    		apageDataOffset;	/* where the next object is to be moved */
    Four   		len;				/* length of object + length of ObjectHdr */
    Two    		lastSlot;			/* last non empty slot */
    Two    		i;					/* index variable */

    
    TR_PRINT(TR_OM, TR1, ("OM_CompactPage(handle, apage=%P, slotNo=%ld)", apage, slotNo));

    /*@ save the slotted page */
    tpage = *apage;

    apageDataOffset = 0;	/* start at the beginning of the data area */
    lastSlot = 0;
    
    for (i = 0; i < tpage.header.nSlots; i++) 
	if (tpage.slot[-i].offset != EMPTYSLOT && i != slotNo) {

	    lastSlot = i;

	    /* obj points to the currently moved object. */
	    obj = (Object *)&tpage.data[tpage.slot[-i].offset];

	    /* copy the entire object to the reorganized page */
	    if (obj->header.properties & P_MOVED) {
		/* this object has the moved ObjectID instead of data */
		len = sizeof(ObjectHdr) + sizeof(ObjectID);
		
	    } else if (obj->header.properties & P_LRGOBJ) {

		len = LOT_GetLengthWithHdr(handle, obj);
		
	    } else {
		/* This object has data itself */
		len = sizeof(ObjectHdr) +
		    MAX(sizeof(ShortPageID), ALIGNED_LENGTH(obj->header.length));
	    }
	    
	    memcpy(&apage->data[apageDataOffset], (char*)obj, len);
	    apage->slot[-i].offset = apageDataOffset;
	    
	    apageDataOffset += len; /* make it point the next move position */
	}

    if (slotNo != NIL) {
	if (slotNo > lastSlot) lastSlot = slotNo;
	
	/* move the specified object to the end */
	obj = (Object *)&tpage.data[tpage.slot[-slotNo].offset];
	
	if (obj->header.properties & P_MOVED) {
	    /* this object has the moved ObjectID instead of data */
	    len = sizeof(ObjectHdr) + sizeof(ObjectID);
	    
	} else if (obj->header.properties & P_LRGOBJ) {

	    len = LOT_GetLengthWithHdr(handle, obj);
	    
	} else {
	    /* This object has data itself */
	    len = sizeof(ObjectHdr) +
		MAX(sizeof(ShortPageID), ALIGNED_LENGTH(obj->header.length));
	}
	
	memcpy(&apage->data[apageDataOffset], (char*)obj, len);
	apage->slot[-slotNo].offset = apageDataOffset;
	
	apageDataOffset += len; /* make it point the next move position */
    
    }

    /* set the control variables */
    apage->header.nSlots = lastSlot+1;
    apage->header.free = apageDataOffset; /* start pos. of contiguous space */
    apage->header.unused = 0;		  /* no fragmented unused space */

    return(eNOERROR);
    
} /* OM_CompactPage */
