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
 * Module: mlgf_FindEntry.c
 *
 * Description:
 *  Find an entry corresponding to the region which contains the given point.
 *
 * Exports:
 *  Boolean mlgf_FindEntry(mlgf_DirectroyPage*, One, MLGF_HashValue[], Two*)
 *
 * Returns:
 *  TRUE : if there is an entry associated with a region conaining the given point.
 *  FALSE : otherwise.
 */


#include "common.h"
#include "error.h"
#include "trace.h"
#include "Util.h"
#include "MLGF_Internal.h"
#include "perThreadDS.h"
#include "perProcessDS.h"


Boolean mlgf_FindEntry(
    Four handle,
    mlgf_DirectoryPage  *dirPage,               /* IN a directory page */
    One                 nKeys,                  /* IN # of keys of MLGF */
    MLGF_HashValue      *keys,                  /* IN hash values of keys */
    Two                 *entryNo)               /* OUT entry which includes the keys */
{
    Two                 i;                      /* index variable */
    One                 j;                      /* index variable */
    Two                 entryLen;               /* length of a directroy entry */
    MLGF_HashValue      xor;                    /* XOR value of two hash values */
    One                 *entryNumValidBits;     /* points to nValidBits[] field in an entry */
    MLGF_HashValue      *entryHashValues;       /* points to array of hash values in an entry */
    mlgf_DirectoryEntry *entry;                 /* a directory entry of MLGF */


    TR_PRINT(TR_MLGF, TR1,
             ("mlgf_FindEntry(handle, dirPage=%P, nKeys=%ld, keys=%P, entryNo=%P)",
	      nKeys, dirPage, keys, entryNo));


    /* Calculate the directory entry length. */
    entryLen = MLGF_DIRENTRY_LENGTH(nKeys);

    /*
    ** Find an entry containing the given point.
    */
    /* first entry */
    entry = (mlgf_DirectoryEntry*)&dirPage->data[0];
    entryNumValidBits = entry->nValidBits;
    entryHashValues = MLGF_DIRENTRY_HASHVALUEPTR(entry, nKeys);

    for (i = 0; i < dirPage->hdr.nEntries; i++) {

	for (j = 0; j < nKeys; j++) {
	    if (entryNumValidBits[j] == 0) continue;

	    xor = entryHashValues[j] ^ keys[j];

	    if ((xor >> (MLGF_MAXNUM_VALIDBITS - entryNumValidBits[j])) != 0) break;
	}

	if (j == nKeys) {	/* found */
	    *entryNo = i;
	    return(TRUE);
	}

	/* Points to the nValidBits field in the next entry. */
	entryNumValidBits = entryNumValidBits + entryLen;

	/* Points to the field, hashValues[] in the next entry. */
	entryHashValues = (MLGF_HashValue*)((char*)entryHashValues + entryLen);
    }

    return(FALSE);

} /* mlgf_FindEntry() */
