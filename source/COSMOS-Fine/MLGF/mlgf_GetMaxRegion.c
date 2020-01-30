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
 * Module: mlgf_GetMaxRegion.c
 *
 * Description:
 *  Initially 'regionVector' contains a point. We increase the region as big
 *  as possible. The region cannot be bigger than the directory page region
 *  and cannot overlap other regions in the same directory page..
 *
 * Exports:
 *  void mlgf_GetMaxRegion(Four, Four, mlgf_DirectoryPage*, mlgf_DirectoryEntry*,
 *                         MLGF_HashValue[], mlgf_DirectoryEntry*)
 *
 * Returns:
 *  None
 */


#include "common.h"
#include "error.h"
#include "trace.h"
#include "Util.h"
#include "TM.h"
#include "MLGF.h"
#include "perProcessDS.h"
#include "perThreadDS.h"


void mlgf_GetMaxRegion(
    Four 		handle,
    MLGF_KeyDesc 	*kdesc,        	/* IN key descriptor of MLGF */
    mlgf_DirectoryPage 	*dirPage, 	/* IN a directory page */
    mlgf_DirectoryEntry *parentEntry,  	/* IN entry for directory page */
    MLGF_HashValue 	*keys,	       	/* IN keys of a point */
    mlgf_DirectoryEntry *childEntry)   	/* OUT entry for the given point */
{
    Four 		i, j;		/* index variables */
    Four 		entryLen;	/* length of a directory entry */
    MLGF_HashValue 	*hx;		/* pointer to array of hash values */
    Boolean 		changeFlag;	/* TRUE if a change occurs */
    mlgf_DirectoryEntry *otherEntry; 	/* temporary directory entries */


    TR_PRINT(handle, TR_MLGF, TR1,
	     ("mlgf_GetMaxRegion(handle, kdesc=%P, dirPage=%P, parentEntry=%P, keys=%P, childEntry=%P)",
	      kdesc, dirPage, parentEntry, keys, childEntry));


    /* Calculate the length of a directory entry. */
    entryLen = MLGF_DIRENTRY_LENGTH(kdesc->nKeys);

    /* 'hx' points to array of hash values of the given region vector. */
    hx = MLGF_DIRENTRY_HASHVALUEPTR(childEntry, kdesc->nKeys);

    /* Initialize 'childEntry'. */
    for (i = 0; i < kdesc->nKeys; i++) {
	childEntry->nValidBits[i] = parentEntry->nValidBits[i];
	hx[i] = keys[i];
    }

    /* find the split starting point; this method is valid when using cyclic split policy */
    for (i = 1; i < kdesc->nKeys; i++)
	if (childEntry->nValidBits[i-1] != childEntry->nValidBits[i]) break;
    if (i == kdesc->nKeys) i = 0;

    for ( ; ; ) {
	/* test if the entry has common region */
	/* with other regions in directory page or not */
	for (j = 0; j < dirPage->hdr.nEntries; j++) {
	    otherEntry = (mlgf_DirectoryEntry*)&dirPage->data[entryLen*j];
	    if (mlgf_CommonRegionTest(handle, kdesc->nKeys, childEntry, otherEntry) == TRUE) break;
	}

	/* If there is no region conflict, then the region is maximized. */
	if (j == dirPage->hdr.nEntries) break;

	/* There is a region conflict. */
	/* increase the number of valid bits by 1. */
	childEntry->nValidBits[i] ++;
	i = (i+1) % kdesc->nKeys;
    }

    /*
     * Set MBR.
     */
    for (i = 0; i < kdesc->nKeys; i++) {
        if (MLGF_KEYDESC_IS_MINTYPE(*kdesc, i))
            hx[i] &= MLGF_HASHVALUE_ALL_BITS_SET; /* set maximum value */
        else
            hx[i] &= MLGF_HASHVALUE_UPPER_N_BITS_SET(childEntry->nValidBits[i]); /* set minimum value */
    }

} /* mlgf_GetMaxRegion() */
