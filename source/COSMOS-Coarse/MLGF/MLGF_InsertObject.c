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
/*    "http://dblab.kaist.ac.kr/Open-Software/MLGF/main.html".               */
/*                                                                            */
/******************************************************************************/

/*
 * Module: MLGF_InsertObject.c
 *
 * Description:
 *  Insert an object and its data into the given MLGF index.
 *
 * Exports:
 *  Four MLGF_InsertObject(IndexID*, MLGF_KeyDesc*, MLGF_HashValue[], ObjectID*, char*)
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


/* Internal Fucntion Prototypes */
Four mlgf_InsertRecursive(Four, ObjectID*, PageID*, MLGF_KeyDesc*, MLGF_HashValue[],
                          ObjectID*, char*, mlgf_DirectoryEntry*, mlgf_DirectoryEntry*,
                          mlgf_InsertStatus_T*);


/*
 * Function: Four MLGF_InsertObject(ObjectID*, IndexID*, MLGF_KeyDesc*, MLGF_HashValue[], ObjectID*, char*)
 *
 * Description:
 *  Insert a pair of an ObjectID and its associated data into the given MLGF
 *  index.
 *
 * Returns:
 *  Error code
 *    some errors caused by function calls
 */
Four MLGF_InsertObject(
    Four handle,
    ObjectID            *catObjForFile,         /* IN catalog object of index file */
    PageID              *rootPid,               /* IN root page of MLGF */ 
    MLGF_KeyDesc        *kdesc,                 /* IN key descriptor of MLGF */
    MLGF_HashValue      *keys,                  /* IN hash values of keys */
    ObjectID            *oid,                   /* IN Object to insert */
    char                *data)                  /* IN additional data to store */
{
    Four                e;                      /* error code */
    One                 i;                      /* index */
    Two                 entryLen;               /* length of a directory entry */
    PageID              newPid;                 /* PageID of the newly allocated page */
    mlgf_DirectoryPage  *newPage;               /* pointer to buffer for new page  */
    mlgf_DirectoryPage  *rootPage;              /* pointer to buffer for root page */
    mlgf_DirectoryEntry newEntry;               /* directory entry for new page */
    mlgf_DirectoryEntry rootEntry;              /* directory entry for old root page */
    mlgf_DirectoryEntry overflowInRoot;         /* directroy entry which are overflowed */
    PageID              tmpPid;
    mlgf_InsertStatus_T status;


    TR_PRINT(TR_MLGF, TR1,
             ("MLGF_InsertObject(catObjForFile=%P, rootPid=%P, kdesc=%P, keys=%P, oid=%P, data=%P",
	      catObjForFile, rootPid, kdesc, keys, oid, data));


    /* Check parameters. */
    if (rootPid == NULL || kdesc == NULL || keys == NULL || oid == NULL || 
	(kdesc->extraDataLen != 0 && data == NULL))
	ERR(handle, eBADPARAMETER);

    /* `rootEntry' is the entry for root page. */
    rootEntry.spid = rootPid->pageNo; 
    rootEntry.theta = 0;	/* ignore this field for root page */
    for (i = 0; i < kdesc->nKeys; i++) rootEntry.nValidBits[i] = 0;

    do {
        /* call recursion */
        status.allFlags = 0;
        e = mlgf_InsertRecursive(handle, catObjForFile, rootPid, kdesc, keys, 
                                 oid, data, &rootEntry, &overflowInRoot,
                                 &status);
        if (e < 0) ERR(handle, e);

        if (status.flags.overflow) {	/* root page split */
            TR_PRINT(TR_MLGF, TR1, ("Split\n"));

            /* Allocate a new page for the directory page. */
            e = mlgf_AllocPage(handle, catObjForFile, rootPid, &newPid);
            if (e < 0) ERR(handle, e);

            e = BfM_GetNewTrain(handle, &newPid, (char**)&newPage, PAGE_BUF);
            if (e < 0) ERR(handle, e);

            /* Read the root page into the buffer. */
            e = BfM_GetTrain(handle, rootPid, (char**)&rootPage, PAGE_BUF);
            if (e < 0) ERRB1(handle, e, &newPid, PAGE_BUF);

            /* Copy the root page contents to the new page. */
            MLGF_COPY_DIRECTORY_PAGE(newPage, newPid, rootPage, FALSE);

            e= BfM_SetDirty(handle, &newPid, PAGE_BUF);
            if (e < 0) ERRB1(handle, e, rootPid, PAGE_BUF);

            rootEntry.spid = newPid.pageNo;
            e = mlgf_SplitDirectoryPage(handle, catObjForFile, &newPid, kdesc, &overflowInRoot,
                                        &rootEntry, &newEntry);
            if (e < 0) ERRB1(handle, e, rootPid, PAGE_BUF);

            /* 'entryLen' is the length of a directory entry. */
            entryLen = MLGF_DIRENTRY_LENGTH(kdesc->nKeys);

            /* Construct the new root page. */
            memcpy(&rootPage->data[0], (char*)&rootEntry, entryLen);
            memcpy(&rootPage->data[entryLen], (char*)&newEntry, entryLen);

            rootPage->hdr.nEntries = 2;
            rootPage->hdr.height ++;

            e = BfM_FreeTrain(handle, &newPid, PAGE_BUF);
            if (e < 0) ERRB1(handle, e, rootPid, PAGE_BUF);

            e = BfM_SetDirty(handle, rootPid, PAGE_BUF);
            if (e < 0) ERRB1(handle, e, rootPid, PAGE_BUF);

            e = BfM_FreeTrain(handle, rootPid, PAGE_BUF);
            if (e < 0) ERR(handle, e);
        }

    } while (!status.flags.objectInserted);

    return(eNOERROR);

} /* MLGF_InsertObject() */



/*
 * Module: Four mlgf_InsertRecursive(PageID*, MLGF_KeyDesc*, MLGF_HashValue*,
 *                ObjectID*, char*, MLGF_DirectoryEntry*, MLGF_DirectoryEntry*)
 *
 * Description:
 *  insert an object into mlgf index, recursively
 *
 * Returns:
 *  Error code
 */
Four mlgf_InsertRecursive(
    Four handle,
    ObjectID            *catObjForFile,         /* IN catalog object of index file */
    PageID              *root,                  /* IN root of subtree */
    MLGF_KeyDesc        *kdesc,                 /* IN key descriptor of MLGF */
    MLGF_HashValue      *keys,                  /* IN hash values of keys */
    ObjectID            *oid,                   /* IN object to insert */
    char                *data,                  /* IN data to store */
    mlgf_DirectoryEntry *entryToRoot,           /* INOUT entry which indicates parent */
    mlgf_DirectoryEntry *overflowInRoot,        /* OUT entry which failed to insert */
    mlgf_InsertStatus_T *status)                /* INOUT current status */
{
    Four                e;                      /* error code */
    Boolean             isTmp;
    Two                 entryNo;                /* index to some entry */
    Two                 entryLen;               /* length of a directory entry */
    One                 buddyKey;               /* key on which buddys can be merged */
    PageID              child;                  /* PageID of the child page */
    PageID              newPid;                 /* PageID of the newly allocated page */
    Boolean             found;                  /* TRUE if we find something */
    Boolean             insertEntryFlag;        /* TRUE when we are to insert an entry in root */
    mlgf_Page           *apage;                 /* an MLGF page */
    mlgf_Page           *newPage;               /* an MLGF page */
    mlgf_DirectoryEntry *entryToChild;          /* entry pointing the child page */
    mlgf_DirectoryEntry entryToChild_tmp;       /* temporary copy for new update */
    mlgf_DirectoryEntry overflowInChild;        /* entry overflowed in child page */
    MLGF_HashValue      *hx, *hy;               /* points to arrary of hash values in an entry */
    One                 k;


    TR_PRINT(TR_MLGF, TR1,
             ("mlgf_InsertRecursive(handle, root=%P, kdesc=%P, keys=%P, oid=%P, data=%P, entryToRoot=%P, overflowInRoot=%P)",
	      root, kdesc, keys, oid, data, entryToRoot, overflowInRoot));

    TR_PRINT(TR_MLGF, TR1, ("recursive\n"));

    /* Read the current page into the buffer. */
    e = BfM_GetTrain(handle, root, (char**)&apage, PAGE_BUF);
    if (e < 0) ERR(handle, e);

    if (apage->any.hdr.type & MLGF_LEAFPAGE) { /* leaf page */

	/* insert the new object into the leaf page */
	e = mlgf_InsertIntoLeaf(handle, catObjForFile, root, &apage->leaf, kdesc, keys, oid, data, entryToRoot, status); 
	if (e < 0) ERRB1(handle, e, root, PAGE_BUF);

	e = BfM_FreeTrain(handle, root, PAGE_BUF);
	if (e < 0) ERR(handle, e);

	return(eNOERROR);
    }

    /* From this point, the page is a directory page. */

    /* Initialize 'insertEntryFlag' to FALSE. */
    /* It has TRUE when we should insert a new entry into current page. */
    insertEntryFlag = FALSE;

    /* Calculate the length of a directory entry. */
    entryLen = MLGF_DIRENTRY_LENGTH(kdesc->nKeys);

    /* find region to go next time */
    found = mlgf_FindEntry(handle, &apage->directory, kdesc->nKeys, keys, &entryNo);

    /*
     * If found is FALSE, it means that we are inserting
     * record into empty region.
     * So we first find mergable(buddy) region.
     * If we find one, insert record into that region,
     * else allocate new page.
     */
    if (found) {
	entryToChild = MLGF_ITH_DIRENTRY(&apage->directory, entryNo, entryLen);

	MAKE_PAGEID(child, root->volNo, entryToChild->spid);

    } else {

	status->flags.objectInEmptyRegion = 1;

	/* find maximum region which buddy entry can contain. */
	mlgf_GetMaxRegion(handle, kdesc, &apage->directory, entryToRoot, keys, overflowInRoot);

	/* find buddy region */
	found = mlgf_FindBuddyEntry(handle, &apage->directory, kdesc->nKeys,
				    overflowInRoot, &entryNo, &buddyKey);

	if (found) {
	    entryToChild = MLGF_ITH_DIRENTRY(&apage->directory, entryNo, entryLen);

            memcpy(overflowInRoot, entryToChild, entryLen);
            overflowInRoot->nValidBits[buddyKey] -- ;

            e = mlgf_DeleteFromDirectory(handle, &apage->directory, kdesc, entryNo);
            if (e < 0) ERRB1(handle, e, root, PAGE_BUF);

	    e = mlgf_InsertIntoDirectory(handle, &apage->directory, kdesc, overflowInRoot);
            if (e < 0) ERRB1(handle, e, root, PAGE_BUF);

            found = mlgf_FindEntry(handle, &apage->directory, kdesc->nKeys, keys, &entryNo);
            assert(found == TRUE);

            entryToChild = MLGF_ITH_DIRENTRY(&apage->directory, entryNo, entryLen);
	    MAKE_PAGEID(child, root->volNo, entryToChild->spid);

	    e = BfM_SetDirty(handle, root, PAGE_BUF);
            if (e < 0) ERRB1(handle, e, root, PAGE_BUF);

	} else {
	    /* Allocate a page for the child. */
            e = mlgf_AllocPage(handle, catObjForFile, root, &newPid);
	    if (e < 0) {
		ERRB1(handle, e, root, PAGE_BUF);
	    }
            
            /* check this MLGF is temporary */
            e = mlgf_IsTemporary(handle, catObjForFile, &isTmp);
            if (e < 0)  ERR(handle, e);

	    /* Read the leaf page into the buffer. */
	    e = BfM_GetNewTrain(handle, &newPid, (char**)&newPage, PAGE_BUF);
	    if (e < 0) {
		ERRB1(handle, e, root, PAGE_BUF);
	    }

	    if (apage->directory.hdr.height == 1) /* child is the leaf page. */
                MLGF_INIT_LEAF_PAGE(&newPage->leaf, isTmp, newPid, kdesc->nKeys, kdesc->extraDataLen);
	    else
                MLGF_INIT_DIRECTORY_PAGE(&newPage->directory, isTmp,
                                         newPid, apage->directory.hdr.height - 1, FALSE, kdesc->nKeys);

            e = BfM_SetDirty(handle, &newPid, PAGE_BUF);
            if (e < 0) ERRB1(handle, e, root, PAGE_BUF);

	    e = BfM_FreeTrain(handle, &newPid, PAGE_BUF);
	    if (e < 0) {
		ERRB1(handle, e, root, PAGE_BUF);
	    }

	    overflowInRoot->spid = newPid.pageNo;
	    entryToChild = overflowInRoot;
	    MAKE_PAGEID(child, root->volNo, entryToChild->spid);
	    insertEntryFlag = TRUE;
	    entryNo = MLGF_NOENTRY;
	}
    }

    /* go to next page */
    memcpy(&entryToChild_tmp, entryToChild, entryLen);
    e = mlgf_InsertRecursive(handle, catObjForFile, &child, kdesc, keys, oid, data,
			     &entryToChild_tmp, &overflowInChild, status);
    if (e < 0) ERRB1(handle, e, root, PAGE_BUF);

    if (status->flags.thetaUpdated) {

        entryToChild->theta = entryToChild_tmp.theta;
        e = BfM_SetDirty(handle, root, PAGE_BUF);
        if (e < 0) ERR(handle, e);

        status->flags.thetaUpdated = 0;
    }

    if (status->flags.mbrUpdated) {

        hx = MLGF_DIRENTRY_HASHVALUEPTR(entryToChild, kdesc->nKeys);
        hy = MLGF_DIRENTRY_HASHVALUEPTR(&entryToChild_tmp, kdesc->nKeys);

        for (k = 0; k < kdesc->nKeys; k++) hx[k] = hy[k];
	e = BfM_SetDirty(handle, root, PAGE_BUF);
        if (e < 0) ERR(handle, e);

        status->flags.mbrUpdated = 0;

        /*
        ** Update the MBR of the entry 'entryToRoot'
        */
        hx = MLGF_DIRENTRY_HASHVALUEPTR(entryToRoot, kdesc->nKeys);
        for (k = 0; k < kdesc->nKeys; k++) {

            if ((MLGF_KEYDESC_IS_MINTYPE(*kdesc, k) && (hx[k] > hy[k])) ||
                (MLGF_KEYDESC_IS_MAXTYPE(*kdesc, k) && (hx[k] < hy[k]))) {
                hx[k] = hy[k];
                status->flags.mbrUpdated = 1;
            }
        }
    }

    if (insertEntryFlag == TRUE) {

        /*
        ** Update the MBR of the entry 'entryToRoot'
        */
        hx = MLGF_DIRENTRY_HASHVALUEPTR(entryToRoot, kdesc->nKeys);
        hy = MLGF_DIRENTRY_HASHVALUEPTR(overflowInRoot, kdesc->nKeys);
        for (k = 0; k < kdesc->nKeys; k++) {

            if ((MLGF_KEYDESC_IS_MINTYPE(*kdesc, k) && (hx[k] > hy[k])) ||
                (MLGF_KEYDESC_IS_MAXTYPE(*kdesc, k) && (hx[k] < hy[k]))) {
                hx[k] = hy[k];
                status->flags.mbrUpdated = 1;
            }
        }
    }

    if (status->flags.overflow) {
	/* Split the child page. */
	if (apage->directory.hdr.height == 1) { /* child is the leaf page. */

	    e = mlgf_SplitLeafPage(handle, catObjForFile, &child, kdesc, keys, oid, data,
				   &entryToChild_tmp, overflowInRoot);
	} else
	    e = mlgf_SplitDirectoryPage(handle, catObjForFile, &child, kdesc, &overflowInChild,
					&entryToChild_tmp, overflowInRoot);

	if (e < 0) ERRB1(handle, e, root, PAGE_BUF);

	/* entryToChild was updated in the previous call. */
        memcpy(entryToChild, &entryToChild_tmp, entryLen);
	e = BfM_SetDirty(handle, root, PAGE_BUF);
        if (e < 0) ERR(handle, e);

        status->flags.overflow = 0;
	insertEntryFlag = TRUE;
    }

    if (insertEntryFlag) {
	if (apage->directory.hdr.nEntries < MLGF_MAX_DIRENTRIES(kdesc->nKeys)) {

	    /* Insert a new entry. */
	    e = mlgf_InsertIntoDirectory(handle, &apage->directory, kdesc, overflowInRoot);
            if (e < 0) ERRB1(handle, e, root, PAGE_BUF);

	    entryToRoot->theta += entryLen;
            status->flags.thetaUpdated = 1;

	    e = BfM_SetDirty(handle, root, PAGE_BUF);
            if (e < 0) ERRB1(handle, e, root, PAGE_BUF);

	} else
	    status->flags.overflow = 1;
    }

    e = BfM_FreeTrain(handle, root, PAGE_BUF);
    if (e < 0) ERR(handle, e);

    return(eNOERROR);

} /* mlgf_InsertRecursive() */
