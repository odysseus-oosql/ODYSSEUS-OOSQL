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
 * Module: MLGF_DeleteObject.c
 *
 * Description:
 *  Delete an object from the given MLGF index.
 *
 * Exports:
 *  Four MLGF_DeleteObject(IndexID*, MLGF_KeyDesc*, MLGF_HashValue*,
 *                         ObjectID*, LocalPool*, DeallocListElem*)
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

/* Internal Function Prototypes */
Four mlgf_DeleteRecursive(Four, PageID*, MLGF_KeyDesc*,
                          MLGF_HashValue*, ObjectID*, char*, mlgf_DirectoryEntry*,
                          Pool*, DeallocListElem*, mlgf_DeleteStatus_T*);

/*
 * Function: Four MLGF_DeleteObject(IndexID*, MLGF_KeyDesc*, MLGF_HashValue*,
 *                                  ObjectID*)
 *
 * Description:
 *  Delete an object from the given MLGF index. The parameter `keys' have
 *  hash values of the keys of the deleted objects.
 *
 * Returns:
 *  eNOERROR
 *  MLGF_STATUS_NOTFOUND
 *  Error code
 *   eBADPARAMETER
 *   some errors caused by function calls
 */
Four MLGF_DeleteObject(
    Four handle,
    PageID                      *rootPid,               /* IN PageID of the root page */ 
    MLGF_KeyDesc                *kdesc,                 /* IN key descriptor for MLGF index */
    MLGF_HashValue              *keys,                  /* IN hash values of keys */
    ObjectID                    *oid,
    Pool                        *dlPool,                /* INOUT pool of dealloc list elements */
    DeallocListElem             *dlHead)                /* INOUT head of the dealloc list */
{
    Four                        e;                      /* error code */
    One                         i;                      /* index variable */
    mlgf_DirectoryEntry         rootEntry;              /* directory entry for old root page */
    char                        extraData[PAGESIZE];    /* extra data of the deleted object */
    PageID                      tmpPid;
    mlgf_DeleteStatus_T         status;


    TR_PRINT(TR_MLGF, TR1,
             ("MLGF_DeleteObject(rootPid=%P, kdesc=%P, keys=%P, oid=%P",
	      rootPid, kdesc, keys, oid));


    if (rootPid == NULL || kdesc == NULL || keys == NULL || oid == NULL) 
	ERR(handle, eBADPARAMETER);


    /* rootEntry is the entry for root page. */
    rootEntry.spid = rootPid->pageNo; 
    rootEntry.theta = 0;	/* ignore this field for root page */
    for (i = 0; i < kdesc->nKeys; i++) rootEntry.nValidBits[i] = 0;

    /* call recursion */
    status.allFlags = 0;
    e = mlgf_DeleteRecursive(handle, rootPid, kdesc, keys, oid, extraData, 
                             &rootEntry, dlPool, dlHead, &status);
    if (e < 0) ERR(handle, e);

    if (status.flags.notFound) ERR(handle, eNOTFOUND);

    return(eNOERROR);

} /* MLGF_DeleteObject() */



/*
 * Function: Four mlgf_DeleteRecursive(PageID*, MLGF_KeyDesc*, MLGF_HashValue*,
 *             ObjectID*, char*, mlgf_DirectoryEntry*, Pool*, DeallocListElem*)
 *
 * Description:
 *  This function is an auxiliary function of MLGF_DeleteObject().
 *
 * Returns:
 *  Error code
 *    some errors caused by function calls
 */
Four mlgf_DeleteRecursive(
    Four handle,
    PageID              *root,                  /* IN root of subtree */
    MLGF_KeyDesc        *kdesc,                 /* IN key descriptor for MLGF */
    MLGF_HashValue      *keys,                  /* IN hash values of the deleted object */
    ObjectID            *oid,                   /* IN object to delete */
    char                *data,                  /* OUT extra data of the deleted object */
    mlgf_DirectoryEntry *entryToRoot,           /* IN entry for the current node in parent node */
    Pool                *dlPool,                /* INOUT pool of dealloc list elements */
    DeallocListElem     *dlHead,                /* INOUT head of the dealloc list */
    mlgf_DeleteStatus_T *status)                /* INOUT delete status */

{
    Four                e;                      /* error code */
    Two                 entryLen;               /* length of a directory entry */
    Two                 entryNo;                /* index to directory entry */
    PageID              child;                  /* PageID of the child page */
    Boolean             found;                  /* TRUE when we find something to search */
    Boolean             mbrChangeFlag;          /* TRUE if the extreme hash values are updated */
    mlgf_Page           *apage;                 /* an MLGF page */
    mlgf_DirectoryPage  *childPage;             /* a child page */
    mlgf_DirectoryEntry *entryToChild;          /* entry pointing to the child */
    mlgf_DirectoryEntry entryToChild_tmp;       /* temporary copy for new update */
    MLGF_HashValue      *extremeHashValues;     /* points to array of extreme hash values in the given root node */
    MLGF_HashValue      *childHashValues;       /* points to array of extreme hash values in the child node */
    MLGF_HashValue      *hx, *hy;               /* points to arrary of hash values in an entry */
    MLGF_HashValue      *entryHashValues;       /* points to arrary of hash values in an entry */
    One                 k;
    Two                 i;
    PageID              tmpPid;
    DeallocListElem     *dlElem;                /* an element of dealloc list */


    TR_PRINT(TR_MLGF, TR1, ("mlgf_DeleteRecursive(handle)"));

    /* Read the current node into a buffer. */
    e = BfM_GetTrain(handle, root, (char**)&apage, PAGE_BUF);
    if (e < 0) ERR(handle, e);

    if (apage->any.hdr.type & MLGF_LEAFPAGE) { /* leaf page */

	/* delete the given object from the leaf page. */
	e = mlgf_DeleteObjectFromLeaf(handle, root, &apage->leaf, kdesc, keys, oid, data, 
                                      entryToRoot, dlPool, dlHead, status);
	if (e < 0) ERRB1(handle, e, root, PAGE_BUF);

	e = BfM_FreeTrain(handle, root, PAGE_BUF);
	if (e < 0) ERR(handle, e);

	return(eNOERROR);
    }

    /* From this point the node is the directory node. */

    /* Calculate the length of a directory entry. */
    entryLen = MLGF_DIRENTRY_LENGTH(kdesc->nKeys);

    /* find region to go */
    found = mlgf_FindEntry(handle, &apage->directory, kdesc->nKeys, keys, &entryNo);

    if (!found) {
        status->flags.notFound = 1;

	e = BfM_FreeTrain(handle, root, PAGE_BUF);
	if (e < 0) ERR(handle, e);

	return(eNOERROR);
    }

    entryToChild = MLGF_ITH_DIRENTRY(&apage->directory, entryNo, entryLen);
    MAKE_PAGEID(child, root->volNo, entryToChild->spid);


    /* call recursion */
    memcpy(&entryToChild_tmp, entryToChild, entryLen);
    e = mlgf_DeleteRecursive(handle, &child, kdesc, keys, oid, data,
                             &entryToChild_tmp, dlPool, dlHead, status);
    if (e < 0) ERRB1(handle, e, root, PAGE_BUF);


    if (status->flags.thetaUpdated) {

        entryToChild->theta = entryToChild_tmp.theta;
        e = BfM_SetDirty(handle, root, PAGE_BUF);
        if (e < 0) ERRB1(handle, e, root, PAGE_BUF);

        status->flags.thetaUpdated = 0;
    }

    if (status->flags.mbrUpdated) {

        hx = MLGF_DIRENTRY_HASHVALUEPTR(entryToChild, kdesc->nKeys);
        hy = MLGF_DIRENTRY_HASHVALUEPTR(&entryToChild_tmp, kdesc->nKeys);
        
        for (k = 0; k < kdesc->nKeys; k++) hx[k] = hy[k];
	e = BfM_SetDirty(handle, root, PAGE_BUF);
        if (e < 0) ERRB1(handle, e, root, PAGE_BUF);

        status->flags.mbrUpdated = 0;

        /*
        ** Update the MBR of the entry 'entryToRoot'
        */
	hx = MLGF_DIRENTRY_HASHVALUEPTR(entryToRoot, kdesc->nKeys);
	for (k = 0; k < kdesc->nKeys; k++) {

	    if ((MLGF_KEYDESC_IS_MINTYPE(*kdesc, k) && (hx[k] < keys[k])) ||
		(MLGF_KEYDESC_IS_MAXTYPE(*kdesc, k) && (hx[k] > keys[k])))
		continue;

            if (MLGF_KEYDESC_IS_MINTYPE(*kdesc, k))
                hx[k] |= MLGF_HASHVALUE_SET_EXCEPT_UPPER_N_BITS(entryToRoot->nValidBits[k]);
            else
                hx[k] = MLGF_HASHVALUE_MASK_UPPER_N_BITS(hx[k], entryToRoot->nValidBits[k]);

            status->flags.mbrUpdated = 1;

	    hy = MLGF_DIRENTRY_HASHVALUEPTR(
		MLGF_ITH_DIRENTRY(&(apage->directory), 0, entryLen), kdesc->nKeys);
	    for (i = 0; i < apage->directory.hdr.nEntries; i++) {
		if ((MLGF_KEYDESC_IS_MINTYPE(*kdesc, k) && (hx[k] > hy[k])) ||
		    (MLGF_KEYDESC_IS_MAXTYPE(*kdesc, k) && (hx[k] < hy[k]))) {

		    hx[k] = hy[k];
		}

		hy = (MLGF_HashValue*)((char*)hy + entryLen);
	    }
        }
    }

    if (status->flags.underflow) {
	/* maximize regions in directory page */
	e = mlgf_MaximizeRegions(handle, &apage->directory, kdesc, entryToRoot);
        if (e < 0) ERRB1(handle, e, root, PAGE_BUF);

	if (apage->directory.hdr.height == 1) /* merge leaf page */
	    e = mlgf_MergeLeafPage(handle, &apage->directory, kdesc, entryNo, dlPool, dlHead);
	else 	/* merge directory page */
	    e = mlgf_MergeDirectoryPage(handle, &apage->directory, kdesc, entryNo, dlPool, dlHead);
	if (e < 0) ERRB1(handle, e, root, PAGE_BUF);

        status->flags.underflow = 0;

    } else if (status->flags.emptyPage) {
	/* delete entry */
        e = mlgf_DeleteFromDirectory(handle, &apage->directory, kdesc, entryNo);
        if (e < 0) ERRB1(handle, e, root, PAGE_BUF);

	e = BfM_SetDirty(handle, root, PAGE_BUF);
        if (e < 0) ERRB1(handle, e, root, PAGE_BUF);

        status->flags.emptyPage = 0;
    }

    if (MLGF_DP_THETA(&apage->directory, entryLen) != entryToRoot->theta) {
        /* update the theta value */
	entryToRoot->theta = MLGF_DP_THETA(&apage->directory, entryLen);
        status->flags.thetaUpdated = 1;
    }

    if (apage->directory.hdr.nEntries == 0)
        status->flags.emptyPage = 1;
    else if (entryToRoot->theta < MLGF_DP_THRESHOLD)
        status->flags.underflow = 1;

    if (status->flags.emptyPage && !(apage->any.hdr.type & MLGF_ROOTPAGE)) { 
        apage->any.hdr.type = MLGF_FREEPAGE; /* any more not used in this index */

        /*
        ** Insert a new node for the dropped file.
        */    
        e = Util_getElementFromPool(handle, dlPool, &dlElem);
        if (e < 0) ERRB1(handle, e, root, PAGE_BUF);

        dlElem->type = DL_PAGE;
        dlElem->elem.pid = apage->any.hdr.pid; /* save the deallcoated PageID. */
        dlElem->next = dlHead->next; /* insert to the list */
        dlHead->next = dlElem;       /* new first element of the list */
    }

    if (apage->any.hdr.type & MLGF_ROOTPAGE) {	/* level down */

	/* if the root page has only one entry, let level down */
	while (apage->directory.hdr.nEntries == 1 && apage->directory.hdr.height > 1) {

	    /* Copy the only one child into the root. */
	    entryToChild = MLGF_ITH_DIRENTRY(&apage->directory, 0, entryLen);
	    MAKE_PAGEID(child, root->volNo, entryToChild->spid);

	    /* Read the child into the buffer. */
	    e = BfM_GetTrain(handle, &child, (char**)&childPage, PAGE_BUF);
	    if (e < 0) ERRB1(handle, e, root, PAGE_BUF);
            
            memcpy(&apage->directory.data[0], &childPage->data[0], entryLen*childPage->hdr.nEntries);
            apage->directory.hdr.nEntries = childPage->hdr.nEntries;
            apage->directory.hdr.height --;
	    e = BfM_SetDirty(handle, root, PAGE_BUF);
            if (e < 0) ERRB2(handle, e, &child, PAGE_BUF, root, PAGE_BUF);

            childPage->hdr.type = MLGF_FREEPAGE; /* any more not used in this index */
            
	    e = BfM_SetDirty(handle, &child, PAGE_BUF);
            if (e < 0) ERRB2(handle, e, &child, PAGE_BUF, root, PAGE_BUF);

	    e = BfM_FreeTrain(handle, &child, PAGE_BUF);
	    if (e < 0) ERRB1(handle, e, root, PAGE_BUF);

            /*
            ** Insert a new node for the dropped file.
            */    
            e = Util_getElementFromPool(handle, dlPool, &dlElem);
            if (e < 0) ERRB1(handle, e, root, PAGE_BUF);

            dlElem->type = DL_PAGE;
            dlElem->elem.pid = child; /* save the deallcoated PageID. */
            dlElem->next = dlHead->next; /* insert to the list */
            dlHead->next = dlElem;       /* new first element of the list */
	}
    }

    e = BfM_FreeTrain(handle, root, PAGE_BUF);
    if (e < 0) ERR(handle, e);

    return(eNOERROR);

} /* mlgf_DeleteRecursive() */

