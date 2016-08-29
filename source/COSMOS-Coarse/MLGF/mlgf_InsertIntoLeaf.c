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
 * Module: mlgf_InsertIntoLeaf.c
 *
 * Description:
 *  Insert an object into the given leaf page.
 *  It is assumed that there is no overflow because the caller has checked
 *  the free space before calling.
 */


#include <string.h>
#include "common.h"
#include "error.h"
#include "trace.h"
#include "Util.h"
#include "MLGF_Internal.h"
#include "perThreadDS.h"
#include "perProcessDS.h"


Four mlgf_InsertIntoLeaf(
    Four handle,
    ObjectID                    *catObjForFile,         /* IN catalog object of index file */
    PageID                      *root,                  /* IN  root page ID */ 
    mlgf_LeafPage               *apage,                 /* INOUT pointer to a leaf page */
    MLGF_KeyDesc                *kdesc,                 /* IN key descriptor for MLGF */
    MLGF_HashValue              keys[],                 /* IN hash values of key values */
    ObjectID                    *oid,                   /* IN ObjectID of object to store */
    char                        *data,                  /* IN data to store */
    mlgf_DirectoryEntry         *entryToLeaf,           /* INOUT entry pointing to this leaf page */
    mlgf_InsertStatus_T         *status)                /* INOUT program current status */
{
    Four                        e;                      /* error code */
    One                         i;                      /* temporary index variable */
    Two                         elemNo;                 /* index on object array */
    Two                         entryLen;               /* length of a leaf entry */
    Two                         objectItemLen;          /* length of an object item in object array */
    Two                         neededSpace;            /* amount of space needed to insert new object */
    Two                         entryNo;                /* slot No. of entry with the given keys */
    char                        *objectItemPtr;         /* points to object item where the new object will be inserted */
    PageID                      ovPid;                  /* PageID of the first overflow page of an entry */
    Boolean                     found;                  /* TRUE if we find something to want */
    mlgf_LeafEntry              *entry;                 /* points to leaf entry */
    mlgf_MortonValue            keyMortonVal;           /* mroton value for the key */
    One                         nValidBits[MLGF_MAXNUM_KEYS]; /* used for getting morton value */
    MLGF_HashValue              *entryHashValues;       /* points to arrary of hash values in an entry */
    One                         k;


    TR_PRINT(TR_MLGF, TR1, ("mlgf_InsertIntoLeaf(handle)"));


    /* If the object is to be inserted into the empty region, there should
       be an SMO before this call. Try the insertion again. */
    if (status->flags.objectInEmptyRegion == 1) return(eNOERROR);


    /* Get the morton value of the keys. */
    for (i = 0; i < kdesc->nKeys; i++)
	nValidBits[i] = MLGF_MAXNUM_VALIDBITS;

    mlgf_GetMortonValue(handle, keys, nValidBits, &keyMortonVal, kdesc->nKeys);


    /* Search the leaf page for the entry whose keys are equal to the given keys. */
    found = mlgf_SearchLeafPageInMortonOrder(handle, apage, kdesc, &keyMortonVal, &entryNo);

    if (found) {		/* same keys exist */
	entry = MLGF_ITH_LEAFENTRY(apage, entryNo);

	if (entry->nObjects < 0) { /* overflow chain */
	    /* Get the PageID of the first overflow page. */
	    MAKE_PAGEID(ovPid, apage->hdr.pid.volNo, MLGF_LEAFENTRY_FIRST_OVERFLOW(kdesc->nKeys, entry));

	    /* Insert the new object into the overflow chain. */
	    e = mlgf_InsertOverflow(handle, catObjForFile, root, &ovPid, kdesc, keys, oid, data); 
	    if (e < 0) ERR(handle, e);

	    status->flags.objectInserted = 1;

	} else {		/* normal entry */
	    entryLen = MLGF_LEAFENTRY_LENGTH(kdesc->nKeys, kdesc->extraDataLen, entry->nObjects);
	    neededSpace = MLGF_LEAFENTRY_OBJECTITEM_LEN(kdesc->extraDataLen);

	    if (entryLen + neededSpace > MLGF_OP_IN_THRESHOLD) {
		/* make an overflow chain */
		e = mlgf_CreateOverflow(handle, catObjForFile, root, apage, kdesc, entryNo); 
		if (e < 0) ERR(handle, e);
                
	    } else {
		if (neededSpace > MLGF_LP_FREE(apage)) {
                    status->flags.overflow = 1;
                    return(eNOERROR);
                }

                e = mlgf_ChangeLeafEntrySize(handle, apage, kdesc->nKeys, kdesc->extraDataLen, entryNo, entryLen, entryLen + neededSpace);
                if (e < 0) ERR(handle, e);

                /* In mlgf_ChangeLeafEntrySize(), entry may be moved to someplace. */
                entry = MLGF_ITH_LEAFENTRY(apage, entryNo);

		/* Get the length of an object item in object array. */
		objectItemLen = MLGF_LEAFENTRY_OBJECTITEM_LEN(kdesc->extraDataLen);

		/* Search the insert position of the new object. */
		mlgf_BinarySearchObjectArray(
		    handle, MLGF_LEAFENTRY_ITH_OBJECTITEM(kdesc->nKeys, kdesc->extraDataLen, entry, 0),
		    oid, entry->nObjects, objectItemLen, &elemNo);

		/* points to the insert position. */
		objectItemPtr = MLGF_LEAFENTRY_ITH_OBJECTITEM(kdesc->nKeys, kdesc->extraDataLen, entry, elemNo);

		/* Reserve space for (elemNo)-th object. */
		memmove(objectItemPtr + objectItemLen, objectItemPtr,
			(entry->nObjects - elemNo)*objectItemLen);

		*((ObjectID*)objectItemPtr) = *oid;
		if (kdesc->extraDataLen != 0)
		    memcpy(objectItemPtr + sizeof(ObjectID), data, kdesc->extraDataLen);

		entry->nObjects ++;
                status->flags.objectInserted = 1;
	    }

            e = BfM_SetDirty(handle, &apage->hdr.pid, PAGE_BUF);
            if (e < 0) ERR(handle, e);
	}

    } else {			/* need a new entry */
	/* We also need space for slot array. */
	neededSpace = MLGF_NEW_LEAFENTRY_LENGTH(kdesc->nKeys, kdesc->extraDataLen) + sizeof(Two);

	if (neededSpace > MLGF_LP_FREE(apage)) {
            status->flags.overflow = 1;
            return(eNOERROR);
        }

	if (neededSpace > MLGF_LP_CFREE(apage))
	    mlgf_CompactLeafPage(handle, apage, kdesc->nKeys, kdesc->extraDataLen, NIL);

	/* Allocate space for the new object. */
	/* move slot values */
        MLGF_INSERT_SLOTS_IN_MLGF_PAGE(apage, entryNo, 1);
	apage->slot[-entryNo] = apage->hdr.free;
	apage->hdr.nEntries++;
	apage->hdr.free += neededSpace - sizeof(Two);

	/* Insert entry. */
	entry = MLGF_ITH_LEAFENTRY(apage, entryNo);
	entry->nObjects = 1;
	for (i = 0; i < kdesc->nKeys; i++) entry->keys[i] = keys[i];
	objectItemPtr = MLGF_LEAFENTRY_ITH_OBJECTITEM(kdesc->nKeys, kdesc->extraDataLen, entry, 0);
	*((ObjectID*)objectItemPtr) = *oid;
	if (kdesc->extraDataLen != 0)
	    memcpy(objectItemPtr + sizeof(ObjectID), data, kdesc->extraDataLen);

	status->flags.objectInserted = 1;
	e = BfM_SetDirty(handle, &apage->hdr.pid, PAGE_BUF);
        if (e < 0) ERR(handle, e);
    }

    /*
     * Update 'THETA' value
     */
    if (MLGF_LP_THETA(apage) != entryToLeaf->theta) {
        entryToLeaf->theta = MLGF_LP_THETA(apage);
        status->flags.thetaUpdated = 1;
    }

    /*
    ** Update the MBR of the entry 'entryToLeaf'
    */
    if (status->flags.objectInserted) {
        entryHashValues = MLGF_DIRENTRY_HASHVALUEPTR(entryToLeaf, kdesc->nKeys);
        for (k = 0; k < kdesc->nKeys; k++) {

            if ((MLGF_KEYDESC_IS_MINTYPE(*kdesc, k) && (entryHashValues[k] > keys[k])) ||
                (MLGF_KEYDESC_IS_MAXTYPE(*kdesc, k) && (entryHashValues[k] < keys[k]))) {
                entryHashValues[k] = keys[k];
                status->flags.mbrUpdated = 1;
            }
        }
    }

    return(eNOERROR);

} /* mlgf_InsertIntoLeaf() */
