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
 * Module: mlgf_MaximizeRegions.c
 *
 * Description:
 *  For each entry in the given directory page we descrease the number of
 *  valid bits of the entry so that the entry will have the largest region
 *  which have no common region with other entries.
 */


#include <string.h>
#include "common.h"
#include "error.h"
#include "trace.h"
#include "Util.h"
#include "MLGF_Internal.h"
#include "perThreadDS.h"
#include "perProcessDS.h"


/*@================================
 * mlgf_MaximizeRegions()
 *================================*/
Four mlgf_MaximizeRegions(
    Four handle,
    mlgf_DirectoryPage          *dirPage,       /* IN pointer to a directory page */
    MLGF_KeyDesc                *kdesc,         /* IN key descriptor of used index */
    mlgf_DirectoryEntry         *entryToDirPage)/* IN entry for this directory page */
{
    Four                        e;              /* error code */
    Two                         i;
    Two                         j;
    One                         k;              /* index variables */
    Two                         entryLen;       /* length of a directory entry */
    Boolean                     changeFlag;     /* TRUE if some changes occur in a loop */
    mlgf_DirectoryEntry         *entry;         /* entry pointed by 'entryNo' */
    mlgf_DirectoryEntry         *otherEntry;    /* directory entry to compare with 'entry' */
    mlgf_DirectoryEntry         entry_tmp;      /* temporary copy of entry for new update */


    TR_PRINT(TR_MLGF, TR1,
             ("mlgf_MaximizeRegions(handle, dirPage=%P, kdesc=%P, entryToDirPage=%P)",
	      dirPage, kdesc, entryToDirPage));


    /*@ Calculate the length of a directory entry. */
    entryLen = MLGF_DIRENTRY_LENGTH(kdesc->nKeys);

    /*@ Initially 'entry' points to the first directory entry. */
    entry = MLGF_ITH_DIRENTRY(dirPage, 0, entryLen);

    for (i = 0; i < dirPage->hdr.nEntries; i++) {

        /* copy the entry */
        memcpy(&entry_tmp, entry, entryLen);

        /*@ Get the domain number used for first expansion domain. */
        for (k = 0; k < kdesc->nKeys-1; k++)
            if (entry_tmp.nValidBits[k] != entry_tmp.nValidBits[k+1]) break;

	/* repeat until the entry have no common region with other entries */
	for ( ; ; ) {

            /* if the # of valid bits of entry is same that of parent, go on. */
            if (entry_tmp.nValidBits[k] == entryToDirPage->nValidBits[k]) break;

            /* decrease # of valid bits:  expand the region */
            entry_tmp.nValidBits[k] --;

            /*@ test if the entry has common region with other regions in directory page or not */
            for (j = 0; j < dirPage->hdr.nEntries; j++) {
                /* skip the current entry */
                if (j == i) continue;

                /* test entry if it has common region with entryPtr */
                otherEntry = MLGF_ITH_DIRENTRY(dirPage, j, entryLen);

                if (mlgf_CommonRegionTest(handle, kdesc->nKeys, otherEntry, &entry_tmp) == TRUE) break;
            }

            if (j != dirPage->hdr.nEntries) { /* there is conflict */
                /* restore # of valid bits: shrink the region */
                entry_tmp.nValidBits[k] ++;

                break;          /* exit this loop */
            }

            /*@ repeat the loop for the next expansion domain */
            if (--k == -1) k = kdesc->nKeys - 1;
        }

        if (memcmp(entry->nValidBits, entry_tmp.nValidBits, kdesc->nKeys)) {
            /* there was update. */

            memcpy(entry->nValidBits, entry_tmp.nValidBits, kdesc->nKeys);
            e = BfM_SetDirty(handle, &dirPage->hdr.pid, PAGE_BUF);
            if (e < 0) ERR(handle, e);
        }

	/*@ 'entry' points to the next directory entry. */
	entry = MLGF_NEXT_DIRENTRY(entry, entryLen);
    }

    return(eNOERROR); 

} /* mlgf_MaximizeRegions() */
