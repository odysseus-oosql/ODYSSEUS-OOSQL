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
 * Module: mlgf_MergeDirectoryPage.c
 *
 * Description:
 *  Merge two directory pages into one.
 */


#include <assert.h>
#include <string.h>
#include "common.h"
#include "error.h"
#include "trace.h"
#include "Util.h"
#include "RDsM_Internal.h"
#include "BfM.h"
#include "MLGF_Internal.h"
#include "perThreadDS.h"
#include "perProcessDS.h"


Four mlgf_MergeDirectoryPage(
    Four handle,
    mlgf_DirectoryPage  *dirPage,               /* INOUT a directory page */
    MLGF_KeyDesc        *kdesc,                 /* IN key descriptor of used index */
    Two                 mergedEntryNo,          /* IN entry to be merged */
    Pool                *dlPool,                /* INOUT pool of dealloc list elements */
    DeallocListElem     *dlHead)                /* INOUT head of the dealloc list */
{
    Four                e;                      /* error code */
    Two                 entryLen;               /* the length of a directory entry */
    Two                 buddyEntryNo;           /* entry no of buddy entry of merged entry */
    One                 buddyKey;               /* attribute no of buddy key */
    PageID              pid;                    /* a temporary PageID */
    PageID              mergePid;
    Boolean             found;                  /* TRUE if buddy entry is found */
    Boolean             mergeFlag;              /* TRUE if a merge occurs */
    mlgf_DirectoryPage  *mergedPage;            /* merged directory page */
    mlgf_DirectoryPage  *buddyPage;             /* buddy page of merged directory page */
    mlgf_DirectoryEntry *mergedEntry;           /* entry for merged page */
    mlgf_DirectoryEntry *buddyEntry;            /* entry for buddy page of merged page */
    mlgf_DirectoryEntry buddyEntry_old;         /* old version of buddy entry */
    MLGF_HashValue      *extremeHashValues_x, *extremeHashValues_y;     /* points to array of extreme(= min or max) hash values */
    One                 k;                 
    DeallocListElem     *dlElem;                /* an element of dealloc list */

#ifndef NDEBUG
    Four count = 0;
#endif


    TR_PRINT(TR_MLGF, TR1, ("mlgf_MergeDirectoryPage(handle)"));


    /* Calculate the length of a directory entry. */
    entryLen = MLGF_DIRENTRY_LENGTH(kdesc->nKeys);

    mergedEntry = MLGF_ITH_DIRENTRY(dirPage, mergedEntryNo, entryLen);

    mergeFlag = FALSE;		/* Intialize mergeFlag to FALSE. */

    /* Repeat until the merge is impossible. */
    for ( ; ; ) {

	TR_PRINT(TR_MLGF, TR1, ("merge count: %ld\n", ++count));

	/* Find the buddy entry. */
	found = FALSE;
	for (buddyEntryNo = mergedEntryNo-1; buddyEntryNo <= mergedEntryNo+1; buddyEntryNo+=2) {

	    if (buddyEntryNo < 0 || buddyEntryNo >= dirPage->hdr.nEntries) continue;

	    buddyEntry = MLGF_ITH_DIRENTRY(dirPage, buddyEntryNo, entryLen);

	    if (mlgf_BuddyTest(handle, kdesc->nKeys, mergedEntry, buddyEntry, &buddyKey)) {
		found = TRUE;
		break;
	    }
	}

	/* if there is not buddy region, exit the loop. */
	if (!found) break;

	/* there is not enough space to merge two pages */
	if (mergedEntry->theta + buddyEntry->theta > PAGESIZE - MLGF_DP_FIXED) 
	    break;


	/* From now, merge proceeds. */
	mergeFlag = TRUE;

	if (buddyEntryNo > mergedEntryNo) { /* swap two entries */
	    buddyEntryNo = mergedEntryNo;
	    mergedEntryNo = buddyEntryNo + 1;

	    buddyEntry = MLGF_ITH_DIRENTRY(dirPage, buddyEntryNo, entryLen);
	    mergedEntry = MLGF_ITH_DIRENTRY(dirPage, mergedEntryNo, entryLen);
	}


	/*
	 * Merge the page pointed by 'mergedEntry'
	 * into the page pointed by 'buddyEntry'.
	 */

	/* get PageIDs of buddy page and merged page */
	MAKE_PAGEID(pid, dirPage->hdr.pid.volNo, buddyEntry->spid);
	MAKE_PAGEID(mergePid, dirPage->hdr.pid.volNo, mergedEntry->spid);

	/* Read the buddy page into the buffer. */
	e = BfM_GetTrain(handle, &pid, (char**)&buddyPage, PAGE_BUF);
	if (e < 0) ERR(handle, e);

	/* Read the merged page into the buffer. */
	e = BfM_GetTrain(handle, &mergePid, (char**)&mergedPage, PAGE_BUF);
	if (e < 0) {
	    ERRB1(handle, e, &pid, PAGE_BUF);
	}

	/* move entries in 'mergedPage' into the 'buddyPage' */
	memcpy((char*)MLGF_ITH_DIRENTRY(buddyPage, buddyPage->hdr.nEntries, entryLen),
	       (char*)MLGF_ITH_DIRENTRY(mergedPage, 0, entryLen),
	       mergedPage->hdr.nEntries*entryLen);


	/* Increase the # of entries in buddyPage. */
	buddyPage->hdr.nEntries += mergedPage->hdr.nEntries;

	mergedPage->hdr.type = MLGF_FREEPAGE; /* any more not used in this index */
	e = BfM_SetDirty(handle, &mergePid, PAGE_BUF);
        if (e < 0) ERRB1(handle, e, &pid, PAGE_BUF);

	e = BfM_FreeTrain(handle, &mergePid, PAGE_BUF);
	if (e < 0) ERRB1(handle, e, &pid, PAGE_BUF);

        /*
        ** Insert a new node for the dropped file.
        */    
        e = Util_getElementFromPool(handle, dlPool, &dlElem);
        if (e < 0) ERRB1(handle, e, &pid, PAGE_BUF);

        dlElem->type = DL_PAGE;
        dlElem->elem.pid = mergePid; /* save the deallcoated PageID. */
        dlElem->next = dlHead->next; /* insert to the list */
        dlHead->next = dlElem;       /* new first element of the list */


	/* Update the buddyEntry. */
        memcpy(&buddyEntry_old, buddyEntry, entryLen);
	buddyEntry->nValidBits[buddyKey] --;
	buddyEntry->theta = MLGF_DP_THETA(buddyPage, entryLen);

        /*
         * Set the MBR of the buddy page
         */
        extremeHashValues_x = MLGF_DIRENTRY_HASHVALUEPTR(buddyEntry, kdesc->nKeys);
        extremeHashValues_y = MLGF_DIRENTRY_HASHVALUEPTR(mergedEntry, kdesc->nKeys);
        for (k = 0; k < kdesc->nKeys; k++) {

	    if (MLGF_KEYDESC_IS_MINTYPE(*kdesc, k)) {
                if (extremeHashValues_x[k] > extremeHashValues_y[k]) extremeHashValues_x[k] = extremeHashValues_y[k];
            } else { /* MAXTYPE */
                if (extremeHashValues_x[k] < extremeHashValues_y[k]) extremeHashValues_x[k] = extremeHashValues_y[k];
            }
        }

	e = BfM_SetDirty(handle, &pid, PAGE_BUF);
        if (e < 0) ERRB1(handle, e, &pid, PAGE_BUF);

	e = BfM_FreeTrain(handle, &pid, PAGE_BUF);
	if (e < 0) ERR(handle, e);
        
	/* Delete 'mergedEntry'. */
        e = mlgf_DeleteFromDirectory(handle, dirPage, kdesc, mergedEntryNo);
        if (e < 0) ERR(handle, e);

	mergedEntry = buddyEntry;
	mergedEntryNo = buddyEntryNo;
    }

    if (mergeFlag) {
        e = BfM_SetDirty(handle, &dirPage->hdr.pid, PAGE_BUF);
        if (e < 0) ERR(handle, e);
    }

    assert(dirPage->hdr.nEntries != 0);

    return(eNOERROR);

} /* mlgf_MergeDirectoryPage() */
