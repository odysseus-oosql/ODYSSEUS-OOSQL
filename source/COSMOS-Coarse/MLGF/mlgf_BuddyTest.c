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
 * Module: mlgf_BuddyTest.c
 *
 * Description:
 *  Test if the given two entries are buddy.
 *
 * Exports:
 *  Boolean mlgf_BuddyTest(One, mlgf_DirectoryEntry*, mlgf_DirectoryEntry*, One*)
 *
 * Returns:
 *  TRUE if two entries are buddy entries
 *  FALSE otherwise
 */


#include "common.h"
#include "error.h"
#include "trace.h"
#include "Util.h"
#include "MLGF_Internal.h"
#include "perThreadDS.h"
#include "perProcessDS.h"


Boolean mlgf_BuddyTest(
    Four handle,
    One                 nKeys,          /* IN number of keys of MLGF index */
    mlgf_DirectoryEntry *entry_x,       /* IN one entry */
    mlgf_DirectoryEntry *entry_y,       /* IN other entry */
    One                 *buddyKey)      /* OUT buddy key no */
{
    One                 i;              /* index */
    One                 buddyMatch;     /* number of buddy matches */
    MLGF_HashValue      *hx;            /* pointer to array of hash values of entry_x */
    MLGF_HashValue      *hy;            /* pointer to array of hash values of entry_y */
    MLGF_HashValue      xor;            /* XOR value of two hash values */


    TR_PRINT(TR_MLGF, TR1,
             ("mlgf_BuddyTest(nKeys=%ld, entry_x=%P, entry_y=%P, buddyKey=%P",
	      nKeys, entry_x, entry_y, buddyKey));


    /* initialization */
    buddyMatch = 0;

    /* hx/hy points to array of hash values of entry_x/entry_y. */
    hx = MLGF_DIRENTRY_HASHVALUEPTR(entry_x, nKeys);
    hy = MLGF_DIRENTRY_HASHVALUEPTR(entry_y, nKeys);

    /* for all the keys */
    for (i = 0; i < nKeys; i++) {
	/* do buddy prefix test */

	if (entry_x->nValidBits[i] != entry_y->nValidBits[i]) return(FALSE);

	xor = (*hx ^ *hy) >> (MLGF_MAXNUM_VALIDBITS - entry_x->nValidBits[i]);

	if (xor == 1) {
	    if (buddyMatch++) return(FALSE);
	    *buddyKey = i;
	} else if (xor != 0)
	    return(FALSE);

        hx++; hy++;
    }

    if (buddyMatch == 0) return(FALSE);

    /* Buddys are regions which were splitted from the same region. */
    for (i = 0; i < nKeys-1; i++)
	if (entry_x->nValidBits[i] != entry_x->nValidBits[i+1]) break;

    if (*buddyKey != i) return(FALSE);

    return TRUE;

} /* mlgf_BuddyTest() */
