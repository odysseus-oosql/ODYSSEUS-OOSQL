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
 * Module: btm_Compact.c
 *
 * Description:
 *  Two functions btm_CompactInternalPage( ) and btm_CompactLeafPage( ) are
 *  used to compact the internal page and the leaf page, respectively.
 *
 * Exports:
 *  void btm_CompactInternalPage(Four, BtreeInternal*, Four)
 *  void btm_CompactLeafPage(Four, BtreeLeaf*, Four)
 */


#include <string.h>
#include "common.h"
#include "error.h"
#include "trace.h"		/* for tracing : TR_PRINT(handle, ) macro */
#include "BfM.h"
#include "BtM.h"
#include "perProcessDS.h"
#include "perThreadDS.h"



/*@================================
 * btm_CompactInternalPage( )
 *================================*/
/*
 * Function: btm_CompactInternalPage(handle, BtreeInternal*, Four)
 *
 * Description:
 *  Reorganize the internal page to make sure the unused bytes in the page
 *  are located contiguously "in the middle", between the entries and the
 *  slot array. To compress out holes, entries must be moved toward the
 *  beginning of the page.
 *
 * Returns:
 *  None
 *
 * Side effects:
 *  The leaf page is reorganized to compact the space.
 */
void btm_CompactInternalPage(
    Four 		handle,
    BtreeInternal 	*apage,			/* INOUT leaf page to compact */
    Four      		slotNo)			/* IN slot to go to the boundary of free space */
{
    BtreeInternal 	tpage;			/* temporay page used to save the given page */
    Four   		apageDataOffset;	/* where the next object is to be moved */
    Four   		len;			/* length of the leaf entry */
    Four   		i;			/* index variable */
    btm_InternalEntry 	*entry;			/* an entry in leaf page */


    TR_PRINT(handle, TR_BTM, TR1,
	     ("btm_CompactInternalPage(handle, apage=%P, slotNo=%ld)", apage, slotNo));

    /*@ save the slotted page */
    tpage = *apage;

    apageDataOffset = 0;	/* start at the beginning of the data area */

    for (i = 0; i < tpage.hdr.nSlots; i++) {

        /* 'entry' points to the currently moved leaf entry. */
        entry = (btm_InternalEntry*)&tpage.data[tpage.slot[-i]];

        /* copy the entire entry to the reorganized page */
        len = sizeof(ShortPageID) + ALIGNED_LENGTH(sizeof(Two)+entry->klen);

        if (i != slotNo) {
            apage->slot[-i] = apageDataOffset;
            apageDataOffset += len;
        } else {
            apage->slot[-i] = apage->hdr.free - apage->hdr.unused - len;
        }

        memcpy(&(apage->data[apage->slot[-i]]), entry, len);
    }

    /*@ set the control variables */
    apage->hdr.free -= apage->hdr.unused; /* start pos. of contiguous space */
    apage->hdr.unused = 0;		  /* no fragmented unused space */

} /* btm_CompactInternalPage() */



/*@================================
 * btm_CompactLeafPage( )
 *================================*/
/*
 * Function: Void btm_CompactLeafPage(handle, BtreeLeaf*, Four)
 *
 * Description:
 *  Reorganizes the leaf page to make sure the unused bytes in the page
 *  are located contiguously "in the middle", between the entries and the
 *  slot array. To compress out holes, entries must be moved toward the
 *  beginning of the page.
 *
 * Return Values :
 *  None
 *
 * Side Effects :
 *  The leaf page is reorganized to comact the space.
 */
void btm_CompactLeafPage(
    Four 		handle,
    BtreeLeaf 		*apage,			/* INOUT leaf page to compact */
    Four      		slotNo)			/* IN slot to go to the boundary of free space */
{
    BtreeLeaf 		tpage;			/* temporay page used to save the given page */
    Four   		apageDataOffset;	/* where the next object is to be moved */
    Four   		len;			/* length of the leaf entry */
    Four   		i;			/* index variable */
    btm_LeafEntry 	*entry;			/* an entry in leaf page */


    TR_PRINT(handle, TR_BTM, TR1,
	     ("btm_CompactLeafPage(handle, apage=%P, slotNo=%ld)", apage, slotNo));

    /*@ save the slotted page */
    tpage = *apage;

    apageDataOffset = 0;	/* start at the beginning of the data area */

    for (i = 0; i < tpage.hdr.nSlots; i++) {

        /* 'entry' points to the currently moved leaf entry. */
        entry = (btm_LeafEntry*)&tpage.data[tpage.slot[-i]];

        /* copy the entire entry to the reorganized page */
        len = BTM_LEAF_ENTRY_LENGTH(entry->klen, entry->nObjects);

	if (i != slotNo) {
            apage->slot[-i] = apageDataOffset;
            apageDataOffset += len;
        } else {
            apage->slot[-i] = apage->hdr.free - apage->hdr.unused - len;
        }

        memcpy(&apage->data[apage->slot[-i]], entry, len);
    }

    /*@ set the control variables */
    apage->hdr.free -= apage->hdr.unused; /* start pos. of contiguous space */
    apage->hdr.unused = 0;		   /* no fragmented unused space */

} /* btm_CompactLeafPage() */
