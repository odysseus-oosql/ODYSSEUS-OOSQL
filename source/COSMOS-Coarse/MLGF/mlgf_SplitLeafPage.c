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
 * Module: mlgf_SplitLeafPage.c
 *
 * Description:
 *  Split a given leaf page.
 *
 * Exports:
 *  Four mlgf_SplitLeafPage(ObjectID*, PageID*, MLGF_KeyDesc*, MLGF_HashValue[], ObjectID*,
 *                          char*, mlgf_DirectoryEntry*, mlgf_DirectoryEntry*)
 *
 * Returns:
 *  Error code
 *    some errors caused by function calls
 */


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

Four mlgf_SplitLeafPage(
    Four handle,
    ObjectID            *catObjForFile,         /* IN catalog object of index file */
    PageID              *leafPid,               /* IN page to split */
    MLGF_KeyDesc        *kdesc,                 /* IN key descriptor of used index */
    MLGF_HashValue      keys[],                 /* IN key values of the inserted object */
    ObjectID            *oid,                   /* IN ObjectID of the inserted object */
    char                *data,                  /* IN extra data of the inserted object */
    mlgf_DirectoryEntry *srcEntry,              /* INOUT entry for original directory page */
    mlgf_DirectoryEntry *dstEntry)              /* OUT entry for new directory page */
{
    Four                e;                      /* error code */
    Boolean             isTmp;                  
    Two                 i;
    Two                 j;
    One                 k;                      /* index variable */
    One                 domain;                 /* domain used for split */
    Two                 offset;                 /* points to a position in data area */
    Two                 entryLen;               /* length of a leaf entry */
    PageID              newPid;                 /* PageID of the newly allocated page */
    MLGF_HashValue      bitmask;                /* bitmask for region test */
    mlgf_LeafEntry      *entry;                 /* a leaf entry */
    mlgf_LeafPage       *leafPage;              /* a original leaf page */
    mlgf_LeafPage       *newPage;               /* a newly allocated leaf page */
    MLGF_HashValue      *extremeHashValues;     /* points to array of extreme(= min or max) hash values within the leaf page */


    TR_PRINT(TR_MLGF, TR1,
             ("mlgf_SplitLeafPage(handle, leafPid=%P, kdesc=%P, keys=%P, oid=%P, data=%P, srcEntry=%P, dstEntry=%P)",
	      leafPid, kdesc, keys, oid, data, srcEntry, dstEntry));


    /* read leaf page from disk */
    e = BfM_GetTrain(handle, leafPid, (char**)&leafPage, PAGE_BUF);
    if (e < 0) ERR(handle, e);

    /* select domain to split */
    mlgf_SplitLeafRegion(handle, leafPage, kdesc, keys, srcEntry, dstEntry, &domain);


    /* Allocate a new leaf page. */
    e = mlgf_AllocPage(handle, catObjForFile, leafPid, &newPid);
    if (e < 0) ERRB1(handle, e, leafPid, PAGE_BUF);

    /* check this MLGF is temporary */
    e = mlgf_IsTemporary(handle, catObjForFile, &isTmp);
    if (e < 0)  ERR(handle, e);

    /* Read the new page into the buffer. */
    e = BfM_GetNewTrain(handle, &newPid, (char**)&newPage, PAGE_BUF);
    if (e < 0) ERRB1(handle, e, leafPid, PAGE_BUF);

    /* Initialize the new leaf page. */
    MLGF_INIT_LEAF_PAGE(newPage, isTmp, newPid, kdesc->nKeys, kdesc->extraDataLen);

    /* Get bit mask used for dividing the entries into two groups. */
    bitmask =  MLGF_HASHVALUE_ITH_BIT_SET(srcEntry->nValidBits[domain]);

    /*
     * Divide the leaf page into two direcory page.
     */
    /* Search the split boundary. */
    for (i = 0; i < leafPage->hdr.nEntries; i++) {
	entry = MLGF_ITH_LEAFENTRY(leafPage, i);

	if (entry->keys[domain] & bitmask) break;
    }

    newPage->hdr.nEntries = leafPage->hdr.nEntries-i;

    /* Move the entries to the new page. */
    for (offset = 0, j = 0; i < leafPage->hdr.nEntries; i++, j++) {

	entry = MLGF_ITH_LEAFENTRY(leafPage, i);
	entryLen = MLGF_LEAFENTRY_LENGTH(kdesc->nKeys, kdesc->extraDataLen, entry->nObjects);

	memcpy(&newPage->data[offset], (char*)entry, entryLen);

	newPage->slot[-j] = offset;
	offset += entryLen;
    }

    newPage->hdr.free = offset;

    leafPage->hdr.unused += offset;
    leafPage->hdr.nEntries -= newPage->hdr.nEntries;

    /* Update the `srcEntry' and `dstEntry'. */
    srcEntry->theta = MLGF_LP_THETA(leafPage);
    dstEntry->theta = MLGF_LP_THETA(newPage);
    dstEntry->spid = newPid.pageNo;


    /*
    ** Set the MBR of the newPage.
    */
    extremeHashValues = MLGF_DIRENTRY_HASHVALUEPTR(dstEntry, kdesc->nKeys);
    for (k = 0; k < kdesc->nKeys; k++) {
	if (MLGF_KEYDESC_IS_MINTYPE(*kdesc, k))
            extremeHashValues[k] |= MLGF_HASHVALUE_SET_EXCEPT_UPPER_N_BITS(dstEntry->nValidBits[k]);
        else
            extremeHashValues[k] = MLGF_HASHVALUE_MASK_UPPER_N_BITS(extremeHashValues[k], dstEntry->nValidBits[k]);
    }

    for (i = 0; i < newPage->hdr.nEntries; i++) {
	entry = MLGF_ITH_LEAFENTRY(newPage, i);

	for (k = 0; k < kdesc->nKeys; k++) {

	    if ((MLGF_KEYDESC_IS_MINTYPE(*kdesc, k) && (extremeHashValues[k] > entry->keys[k])) ||
		(MLGF_KEYDESC_IS_MAXTYPE(*kdesc, k) && (extremeHashValues[k] < entry->keys[k]))) {
		extremeHashValues[k] = entry->keys[k];
	    }
	}
    }


    /* Set the dirty flag of the buffer for new page. */
    e = BfM_SetDirty(handle, &newPid, PAGE_BUF);
    if (e < 0) ERRB2(handle, e, &newPid, PAGE_BUF, leafPid, PAGE_BUF);
    
    e = BfM_FreeTrain(handle, &newPid, PAGE_BUF);
    if (e < 0) ERRB1(handle, e, leafPid, PAGE_BUF);


    /*
    ** Set the MBR of the leafPage.
    */
    extremeHashValues = MLGF_DIRENTRY_HASHVALUEPTR(srcEntry, kdesc->nKeys);
    for (k = 0; k < kdesc->nKeys; k++) {
	if (MLGF_KEYDESC_IS_MINTYPE(*kdesc, k))
            extremeHashValues[k] |= MLGF_HASHVALUE_SET_EXCEPT_UPPER_N_BITS(srcEntry->nValidBits[k]);
        else
            extremeHashValues[k] = MLGF_HASHVALUE_MASK_UPPER_N_BITS(extremeHashValues[k], srcEntry->nValidBits[k]);
    }

    for (i = 0; i < leafPage->hdr.nEntries; i++) {
	entry = MLGF_ITH_LEAFENTRY(leafPage, i);

	for (k = 0; k < kdesc->nKeys; k++) {

	    if ((MLGF_KEYDESC_IS_MINTYPE(*kdesc, k) && (extremeHashValues[k] > entry->keys[k])) ||
		(MLGF_KEYDESC_IS_MAXTYPE(*kdesc, k) && (extremeHashValues[k] < entry->keys[k]))) {
		extremeHashValues[k] = entry->keys[k];
	    }
	}
    }

    /* Set the dirty flag of the buffer for original page. */
    e = BfM_SetDirty(handle, leafPid, PAGE_BUF);
    if (e < 0) ERRB1(handle, e, leafPid, PAGE_BUF);
    
    e = BfM_FreeTrain(handle, leafPid, PAGE_BUF);
    if (e < 0) ERR(handle, e);

    return(eNOERROR);

} /* mlgf_SplitLeafPage() */
