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
 * Module: mlgf_SplitLeafRegion.c
 *
 * Description:
 *  Split the given leaf region into two regions. If all entries in the
 *  given region are included in only one region of the two splitted regions,
 *  then split the region which contains all entries again. The split is
 *  repeated until both the splitted regions have at least one entry.
 *  Returns the domain number which are used as a split domain finally.
 *
 * Exports:
 *  void mlgf_SplitLeafRegion(mlgf_LeafPage*, MLGF_KeyDesc*, mlgf_LeafEntry*, mlgf_DirectoryEntry*, mlgf_DirectoryEntry*, One*);
 *
 * Returns:
 *  Error code
 *    eNOERROR
 */


#include <string.h>
#include "common.h"
#include "error.h"
#include "trace.h"
#include "Util.h"
#include "MLGF_Internal.h"
#include "perThreadDS.h"
#include "perProcessDS.h"


void mlgf_SplitLeafRegion(
    Four		handle,
    mlgf_LeafPage       *leafPage,      /* IN a leaf page of MLGF */
    MLGF_KeyDesc        *kdesc,         /* IN key descriptor of used index */
    MLGF_HashValue      keys[],         /* IN keys of the inserted object */
    mlgf_DirectoryEntry *srcEntry,      /* INOUT entry for original directory page */
    mlgf_DirectoryEntry *dstEntry,      /* OUT entry for new directory page */
    One                 *splitDomain)   /* OUT domain used for split */
{
    Two                 i;
    One                 k;              /* index variables */
    One                 domain;         /* split domain */
    Boolean             done;           /* loop control variable */
    mlgf_LeafEntry      *entry;         /* a leaf entry */
    MLGF_HashValue      bitmask;        /* bitmask for region test */


    TR_PRINT(TR_MLGF, TR1,
             ("mlgf_SplitLeafRegion(handle, leafPage=%P, kdesc=%P, keys=%P, srcEntry=%P, dstEntry=%P, splitDomain=%P)",
	      leafPage, kdesc, keys, srcEntry, dstEntry, splitDomain));


    /* Get the domain number used for first split domain. */
    domain = 0;
    for (k = 1; k < kdesc->nKeys; k++)
	if (srcEntry->nValidBits[k-1] != srcEntry->nValidBits[k]) {
	    domain = k;
	    break;
	}

    for (done = FALSE; !done; domain = (domain+1) % kdesc->nKeys) {

#ifdef TRACE
	if (srcEntry->nValidBits[domain] == MLGF_MAXNUM_VALIDBITS) {
	    printf("exceed maxnum valid bits\n");
	}
#endif /* TRACE */

	/* Increment the number of valid bits of the selected domain. */
	srcEntry->nValidBits[domain] ++;

	/* Get bit mask used for dividing the entries into two groups. */
	bitmask = MLGF_HASHVALUE_ITH_BIT_SET(srcEntry->nValidBits[domain]);

	/*
	 * Check if the entries are distributed into two page
	 * by the given vector of number of valid bits.
	 */
	for (i = 0; i < leafPage->hdr.nEntries; i++) {

	    entry = MLGF_ITH_LEAFENTRY(leafPage, i);

	    if ((keys[domain] ^ entry->keys[domain]) & bitmask) {
		*splitDomain = domain;
		done = TRUE;
		break;
	    }
	}
    }

    /* Set the hash values of srcEntry. */
    memcpy((char*)MLGF_DIRENTRY_HASHVALUEPTR(srcEntry, kdesc->nKeys),
	   (char*)keys, kdesc->nKeys*sizeof(MLGF_HashValue));

    /* Reset the split-boundary bit of the domain-th hash value of the srcEntry to 0. */
    MLGF_DIRENTRY_HASHVALUEPTR(srcEntry, kdesc->nKeys)[*splitDomain] &= ~bitmask;

    /* copy srcEntry into dstEntry */
    memcpy((char*)dstEntry, (char*)srcEntry, MLGF_DIRENTRY_LENGTH(kdesc->nKeys));

    /* Set the split-boundary bit of the domain-th hash value of the dstEntry to 1. */
    MLGF_DIRENTRY_HASHVALUEPTR(dstEntry, kdesc->nKeys)[*splitDomain] |= bitmask;

} /* mlgf_SplitLeafRegion() */

