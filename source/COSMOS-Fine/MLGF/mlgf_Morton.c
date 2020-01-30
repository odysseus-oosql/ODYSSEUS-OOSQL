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
/******************************************************************************/
/*                                                                            */
/*    This module has been implemented based on "The Multilevel Grid File     */
/*    (MLGF) Version 4.0," which can be downloaded at                         */
/*    "http://dblab.kaist.ac.kr/Open-Software/MLGF/main.html".                */
/*                                                                            */
/******************************************************************************/

/*
 * Module: mlgf_Morton.c
 *
 * Description:
 *  Morton Order Value Handling Routines
 *
 * Exports:
 *  Four mlgf_GetMortonValue(Four, MLGF_HashValue[], One[], mlgf_MortonValue*, Four)
 *  Boolean mlgf_SearchDirPageInMortonOrder(Four, mlgf_DirectoryPage*, MLGF_KeyDesc*,
 *                                          mlgf_MortonValue*, Four*)
 *  Boolean mlgf_SearchLeafPageInMortonOrder(Four, mlgf_LeafPage*, MLGF_KeyDesc*,
 *                                           mlgf_MortonValue*, Four*)
 *
 * Assumes:
 *  cyclic split and merge with the splitted buddy
 */


#include <string.h>
#include "common.h"
#include "error.h"
#include "trace.h"
#include "Util.h"
#include "TM.h"
#include "MLGF.h"
#include "perProcessDS.h"
#include "perThreadDS.h"



/*
 * Function: Four mlgf_GetMortonValue(Four, MLGF_HashValue[], One[], mlgf_MortonValue*, Four)
 *
 * Description:
 *  Get the Morton order value from the hash values.
 *
 * Returns:
 *  number of bits to be used in morton value
 */
Four mlgf_GetMortonValue(
    Four 			handle,
    MLGF_HashValue 		hash[],			/* IN hash values */
    One 			nValidBits[],		/* IN number of valid bits of each hash value */
    mlgf_MortonValue 		*morton,		/* OUT Morton order values */
    Four 			nKeys)			/* IN # of hash values */
{
    Four 			i, j;
    register MortonValue 	*mortonPtr;		/* pointer to destination */
    register MLGF_HashValue 	srcMask; 		/* mask for source */
    register MLGF_HashValue 	dstMask; 		/* mask for destination */


    TR_PRINT(handle, TR_MLGF, TR1,
	     ("mlgf_GetMortonValue(handle, hash=%P, nValidBits=%P, morton=%P, nKeys=%ld)",
	      hash, nValidBits, morton, nKeys));


    /* Initialize the destination mask. */
    dstMask = MLGF_MORTONVALUE_MSB_SET;

    /* Clear all bits. */
    memset((char*)(morton->val), 0, sizeof(MLGF_HashValue)*nKeys);

    mortonPtr = morton->val;

    srcMask = MLGF_HASHVALUE_MSB_SET;
    for (i = 0; i < MLGF_MAXNUM_VALIDBITS; i++, srcMask >>= (unsigned)1) {

	for (j = 0; j < nKeys; j++) {

	    if (i >= nValidBits[j]) {
                morton->nBits = i*nKeys + j;
                return(morton->nBits);
            }

	    /* Set the correspondent bit if the source bit is set. */
	    if (hash[j] & srcMask) *mortonPtr |= dstMask;

	    dstMask >>= (unsigned)1;
	    if (!dstMask) {
		dstMask = MLGF_MORTONVALUE_MSB_SET;
		mortonPtr ++;	/* points to the next element */
	    }
	}
    }

    morton->nBits = nKeys*MLGF_MAXNUM_VALIDBITS;

    return(morton->nBits);

} /* mlgf_GetMortonValue() */




/*
 * Function: Four mlgf_CompareMortonValue(Four, mlgf_MortonValue*, mlgf_MortonValue*, Boolean)
 *
 * Description:
 *  Compare two morton values.
 *
 * Returns:
 *  Result of comparison
 *    EQUAL : morton_x and morton_y are same
 *    GREAT : morton_x is greater than morton_y
 *    LESS  : morton_x is less than morton_y
 */
Four mlgf_CompareMortonValue(
    Four 		handle,
    mlgf_MortonValue 	*morton_x,		/* IN morton value of x */
    mlgf_MortonValue 	*morton_y,		/* IN morton value of y */
    Boolean 		fullCompareFlag)    	/* IN comare full value if TRUE, compare only valid bits otherwise */
{
    register Four 	i;
    Four 		remainedBits;
    MortonValue 	mask;
    Four 		nBits;


    TR_PRINT(handle, TR_MLGF, TR1, ("mlgf_CompareMortonValue()"));


    nBits = MIN(morton_x->nBits, morton_y->nBits);

    for (i = 0; i < nBits/(sizeof(morton_x->val[0])*CHAR_BIT); i++) { 
	if (morton_x->val[i] > morton_y->val[i]) return(GREAT);

	if (morton_x->val[i] < morton_y->val[i]) return(LESS);
    }

    remainedBits = nBits % (sizeof(morton_x->val[0]) * CHAR_BIT); 

    if (remainedBits > 0) {
	mask = MLGF_MORTONVALUE_UPPER_N_BITS_SET(remainedBits);

	if ((morton_x->val[i]&mask) > (morton_y->val[i]&mask)) return(GREAT);
	if ((morton_x->val[i]&mask) < (morton_y->val[i]&mask)) return(LESS);
    }

    if (!fullCompareFlag) return(EQUAL); /* same value when compare only valid bits */ 

    if (morton_x->nBits > morton_y->nBits) return(GREAT);
    if (morton_x->nBits < morton_y->nBits) return(LESS);

    return(EQUAL);

} /* mlgf_CompareMortonValue() */



/*
 * Function: Boolean mlgf_SearchDirPageInMortonOrder(Four, mlgf_DirectoryPage*,
 *                                    MLGF_KeyDesc*, mlgf_MortonValue*, Four*)
 *
 * Description:
 *  Search a directory page for the entry whose morton value is the smallest
 *  but not less than the given morton value. When a directory entry's
 *  morton value is comapred, only the valid bits are used in the comparison.
 *
 * Returns:
 *  TRUE if we find an entry which includes the key serarched for
 *  FALSE otherwise
 */
Boolean mlgf_SearchDirPageInMortonOrder(
    Four 		handle,
    mlgf_DirectoryPage 	*dirPage,      		/* IN a directory page */
    MLGF_KeyDesc       	*kdesc,	      		/* IN key descriptor */
    mlgf_MortonValue   	*keyMortonVal, 		/* IN mroton value for the key */
    Boolean            	fullCompareFlag, 	/* IN compare full value if TRUE */ 
    Four               	*entryNo)      		/* OUT entry no to be returned */
{
    Four 		cmp;			/* result of comparison */
    Four 		nBits;			/* total valid bits in an entry */
    Four 		entryLen;		/* length of a directory entry */
    Four 		low, mid, high;		/* variables for binary search */
    mlgf_DirectoryEntry *dirEntry; 		/* a directory entry */
    mlgf_MortonValue 	entryMortonVal; 	/* morton value for the entry */


    TR_PRINT(handle, TR_MLGF, TR1, ("mlgf_SearchDirPageInMortonOrder()"));


    /* Get the directory entry length. */
    entryLen = MLGF_DIRENTRY_LENGTH(kdesc->nKeys);

    low = 0;
    high = dirPage->hdr.nEntries - 1;

    while (low <= high) {
	mid = (low + high) / 2;

	/* dirEntry points to 'mid'-th entry. */
	dirEntry = MLGF_ITH_DIRENTRY(dirPage, mid, entryLen);

	mlgf_GetMortonValue(handle, MLGF_DIRENTRY_HASHVALUEPTR(dirEntry, kdesc->nKeys),
                            dirEntry->nValidBits, &entryMortonVal, kdesc->nKeys);

	/* Compare the search_for_key's morton value to the entry morton value. */
	cmp = mlgf_CompareMortonValue(handle, keyMortonVal, &entryMortonVal, fullCompareFlag); 

	if (cmp == EQUAL) {	/* found!!! */
	    *entryNo = mid;
	    return(TRUE);
	}

	if (cmp == LESS) high = mid - 1;
	else low = mid + 1;
    }

    *entryNo = low;
    return(FALSE);		/* not found!!! */

} /* mlgf_SearchDirPageInMortonOrder() */



/*
 * Function: Boolean mlgf_SearchLeafPageInMortonOrder(Four, mlgf_LeafPage*,
 *                                    MLGF_KeyDesc*, mlgf_MortonValue*, Four*)
 *
 * Description:
 *  Search a leaf page for the entry whose morton value is the smallest
 *  but not less than the given morton value.
 *
 * Returns:
 *  TRUE if we find an entry which includes the key serarched for
 *  FALSE otherwise
 */
Boolean mlgf_SearchLeafPageInMortonOrder(
    Four 		handle,
    mlgf_LeafPage    	*leafPage,      		/* IN a leaf page */
    MLGF_KeyDesc     	*kdesc,	    			/* IN key descriptor */
    mlgf_MortonValue 	*keyMortonVal, 			/* IN mroton value for the key */
    Four             	*entryNo)	    		/* OUT entry no to be returned */
{
    Four 		i;
    Four 		cmp;				/* result of comparison */
    Four 		low, mid, high;			/* variables for binary search */
    mlgf_LeafEntry 	*entry;				/* a leaf entry */
    mlgf_MortonValue 	entryMortonVal; 		/* morton value for the entry */
    One 		nValidBits[MLGF_MAXNUM_KEYS]; 	/* used for getting morton value */


    TR_PRINT(handle, TR_MLGF, TR1,
	     ("mlgf_SearchLeafPageInMortonOrder(handle, leafPage=%P, kdesc=%P, keyMortonVal=%P, entryNo=%P)",
	      leafPage, kdesc, keyMortonVal, entryNo));


    /* Initialize the array of number of valid bits used for Morton Value. */
    for (i = 0; i < MLGF_MAXNUM_KEYS; i++)
	nValidBits[i] = MLGF_MAXNUM_VALIDBITS;


    low = 0;
    high = leafPage->hdr.nEntries - 1;

    while (low <= high) {
	mid = (low + high) / 2;

	/* entry points to 'mid'-th entry. */
	entry = MLGF_ITH_LEAFENTRY(leafPage, mid);

	mlgf_GetMortonValue(handle, entry->keys, nValidBits, &entryMortonVal, kdesc->nKeys);

	/* Compare the search_for_key's morton value to the entry morton value. */
	cmp = mlgf_CompareMortonValue(handle, keyMortonVal, &entryMortonVal, TRUE);

	if (cmp == EQUAL) {	/* found!!! */
	    *entryNo = mid;	
	    return(TRUE);
	}

	if (cmp == LESS) high = mid - 1;
	else low = mid + 1;
    }

    *entryNo = low;
    return(FALSE);		/* not found!!! */

} /* mlgf_SearchLeafPageInMortonOrder() */


