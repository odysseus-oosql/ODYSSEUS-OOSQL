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
/******************************************************************************/
/*                                                                            */
/*    This module has been implemented based on "The Multilevel Grid File     */
/*    (MLGF) Version 4.0," which can be downloaded at                         */
/*    "http://dblab.kaist.ac.kr/Open-Software/MLGF/main.html".                */
/*                                                                            */
/******************************************************************************/

/*
 * Module: mlgf_CompactPage.c
 *
 * Description:
 *  Includes page compage routines.
 *
 * Exports:
 *  void mlgf_CompactLeafPage(mlgf_LeafPage*, One, Two, Two)
 */


#include <string.h>
#include "common.h"
#include "error.h"
#include "trace.h"		/* for tracing : TR_PRINT() macro */
#include "Util.h"
#include "MLGF_Internal.h"
#include "perThreadDS.h"
#include "perProcessDS.h"



/*
 * Function: void mlgf_CompactLeafPage(mlgf_LeafPage*, One*, Two, Two)
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
void mlgf_CompactLeafPage(
    Four		handle,
    mlgf_LeafPage       *apage,                 /* INOUT leaf page to compact */
    One                 nKeys,                  /* IN # of keys */
    Two                 extraDataLen,           /* IN extra data length */
    SlotNo              slotNo)                 /* IN slot to go to the boundary of free space */
{
    mlgf_LeafPage       tpage;                  /* temporay page used to save the given page */
    Two                 apageDataOffset;        /* where the next object is to be moved */
    Two                 len;                    /* length of the leaf entry */
    Two                 i;                      /* index variable */
    mlgf_LeafEntry      *entry;                 /* an entry in leaf page */


    TR_PRINT(TR_MLGF, TR1, ("mlgf_CompactLeafPage(handle)"));


    /* save the slotted page */
    tpage = *apage;

    apageDataOffset = 0;	/* start at the beginning of the data area */

    for (i = 0; i < tpage.hdr.nEntries; i++)
	if (i != slotNo) {

	    /* 'entry' points to the currently moved leaf entry. */
	    entry = MLGF_ITH_LEAFENTRY(&tpage, i); 

	    /* copy the entire entry to the reorganized page */
	    len = MLGF_LEAFENTRY_LENGTH(nKeys, extraDataLen, entry->nObjects);

	    memcpy(&apage->data[apageDataOffset], (char*)entry, len);
	    apage->slot[-i] = apageDataOffset;

	    apageDataOffset += len; /* make it point the next move position */
	}

    if (slotNo != NIL) {

	/* move the specified object to the end */

	/* 'entry' points to the currently moved leaf entry. */
	entry =  MLGF_ITH_LEAFENTRY(&tpage, slotNo); 

	/* copy the entire entry to the reorganized page */
	len = MLGF_LEAFENTRY_LENGTH(nKeys, extraDataLen, entry->nObjects);

	memcpy(&apage->data[apageDataOffset], (char*)entry, len);
	apage->slot[-slotNo] = apageDataOffset;

	apageDataOffset += len; /* make it point the next move position */
    }

    /* set the control variables */
    apage->hdr.free = apageDataOffset; /* start pos. of contiguous space */
    apage->hdr.unused = 0;		   /* no fragmented unused space */


} /* mlgf_CompactLeafPage() */
