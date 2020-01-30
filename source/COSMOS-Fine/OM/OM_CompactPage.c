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
/*    Fine-Granule Locking Version                                            */
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
 *  and the slot array. To compress out holes, objects must be moved toward
 *  the beginning of the page.
 *  When this function is called, the page must be locked.
 *
 * Exports:
 *  Four OM_CompactPage(Four, SlottedPage*, Four)
 *  void om_CompactPage(Four, SlottedPage*, Four)
 *
 * Return Values :
 *  Error Code
 *    eNOERROR
 *
 * Side Effects :
 *  The slotted page is reorganized to compact the space.
 */


#include <string.h>
#include "common.h"
#include "error.h"
#include "trace.h"      /* for tracing : TR_PRINT(handle, ) macro */
#include "latch.h"
#include "BfM.h"        /* for the buffer manager call */
#include "LOG.h"
#include "OM.h"
#include "LOT.h"
#include "perProcessDS.h"
#include "perThreadDS.h"


/*
 * Function: Four OM_CompactPage(Four, SlottedPage*, Four)
 *
 * Description:
 *  This is wrap-around function to provide om_CompactPage( ) as an interface function.
 *
 * Returns:
 *  eNOERROR
 */
Four OM_CompactPage(
    Four 	handle,
    SlottedPage	*apage,		/* IN slotted page to compact */
    Four        slotNo)		/* IN slotNo to go to the end */
{
    TR_PRINT(handle, TR_OM, TR1, ("OM_CompactPage(apage=%P, slotNo=%ld)", apage, slotNo));

    (void) om_CompactPage(handle, apage, slotNo);

    return(eNOERROR);

} /* OM_CompactPage( ) */



/*
 * Module: void om_CompactPage(Four, SlottedPage*, Four)
 *
 * Description:
 *  om_CompactPage( ) reorganizes the page to make sure the unused bytes
 *  in the page are located contiguously "in the middle", between the tuples
 *  and the slot array. To compress out holes, objects must be moved toward
 *  the beginning of the page.
 *  When this function is called, the page must be locked.
 *
 * Returns:
 *  None
 */
void om_CompactPage(
    Four 	handle,
    SlottedPage	*apage,			/* IN slotted page to compact */
    Four        slotNo)			/* IN slotNo to go to the end */
{
    SlottedPage	tpage;			/* temporay page used to save the given page */
    Object 	*obj;			/* pointer to the object in the data area */
    Four   	apageDataOffset;	/* where the next object is to be moved */
    Four   	len;			/* length of object + length of ObjectHdr */
    Four   	lastSlot;		/* last non empty slot */
    Four   	i;			/* index variable */


    TR_PRINT(handle, TR_OM, TR1, ("om_CompactPage(apage=%P, slotNo=%ld)", apage, slotNo));

    /* save the slotted page */
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

		len = sizeof(ObjectHdr) + MAX(MIN_OBJECT_DATA_SIZE, ALIGNED_LENGTH(LOT_GetSize(handle, obj->data, IS_LRGOBJ_ROOTWITHHDR(obj->header.properties))));

	    } else {
		/* This object has data itself */
		len = sizeof(ObjectHdr) +
		    MAX(MIN_OBJECT_DATA_SIZE, ALIGNED_LENGTH(obj->header.length));
	    }

	    memcpy(&apage->data[apageDataOffset], obj, len);
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

            len = sizeof(ObjectHdr) + MAX(MIN_OBJECT_DATA_SIZE, ALIGNED_LENGTH(LOT_GetSize(handle, obj->data, IS_LRGOBJ_ROOTWITHHDR(obj->header.properties))));

	} else {
	    /* This object has data itself */
	    len = sizeof(ObjectHdr) +
		MAX(MIN_OBJECT_DATA_SIZE, ALIGNED_LENGTH(obj->header.length));
	}

	memcpy(&apage->data[apageDataOffset], obj, len);
	apage->slot[-slotNo].offset = apageDataOffset;

	apageDataOffset += len; /* make it point the next move position */

    }

    /* set the control variables */
    /* apage->header.nSlots = lastSlot+1; */
    apage->header.free = apageDataOffset; /* start pos. of contiguous space */
    apage->header.unused = 0;		  /* no fragmented unused space */

} /* om_CompactPage */
