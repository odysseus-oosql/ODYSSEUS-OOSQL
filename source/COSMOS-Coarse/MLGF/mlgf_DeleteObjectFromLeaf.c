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
 * Module: mlgf_DeleteObjectFromLeaf.c
 *
 * Description:
 *  Delete the given object from a leaf page.
 */


#include <string.h>
#include "common.h"
#include "error.h"
#include "trace.h"
#include "Util.h"
#include "RDsM_Internal.h"
#include "MLGF_Internal.h"
#include "perThreadDS.h"
#include "perProcessDS.h"


Four mlgf_DeleteObjectFromLeaf(
    Four handle,
    PageID                      *root,                  /* IN root of subtree */ 
    mlgf_LeafPage               *apage,                 /* INOUT leaf page */
    MLGF_KeyDesc                *kdesc,                 /* IN key descriptor of MLGF index */
    MLGF_HashValue              keys[],                 /* IN hash values of the delete object */
    ObjectID                    *oid,                   /* IN object to delete */
    char                        *data,                  /* OUT extra data of the deleted object */
    mlgf_DirectoryEntry         *entryToLeaf,           /* INOUT entry for the leaf node in parent node */
    Pool                        *dlPool,                /* INOUT pool of dealloc list elements */
    DeallocListElem             *dlHead,                /* INOUT head of the dealloc list */
    mlgf_DeleteStatus_T         *status)                /* INOUT program status */
{
    Four                        e;                      /* error code */
    Two                         i;                      /* index variable */
    Four                        of;                     /* # of objects in overflow apge when underflow */
    Two                         oidArrayElemNo;         /* index on object array */
    Two                         entryNo;                /* slot No. of entry with the given keys */
    Two                         entryOldLen;            /* length of an entry before object deletion */
    Two                         entryNewLen;            /* length of an entry after object deletion */
    Two                         objectItemLen;          /* length of an object item in object array */
    char                        *objectItemPtr;         /* points to object item which will be deleted */
    PageID                      ovPid;                  /* PageID of the first overflow page of an entry */
    Boolean                     found;                  /* TRUE if we find something to want */
    mlgf_LeafEntry              *entry;                 /* points to leaf entry */
    mlgf_OverflowPage           *opage;                 /* points to an overflow page */
    mlgf_MortonValue            keyMortonVal;           /* mroton value for the key */
    One                         nValidBits[MLGF_MAXNUM_KEYS]; /* used for getting morton value */
    MLGF_HashValue              *dirEntryHashValues;    /* points to arrary of hash values in an entry */
    One                         k;
    DeallocListElem             *dlElem;                /* an element of dealloc list */


    TR_PRINT(TR_MLGF, TR1, ("mlgf_DeleteObjectFromLeaf(handle)"));


    /* find object to delete */

    /* Get the morton value of the keys. */
    for (k = 0; k < kdesc->nKeys; k++)
        nValidBits[k] = MLGF_MAXNUM_VALIDBITS;

    mlgf_GetMortonValue(handle, keys, nValidBits, &keyMortonVal, kdesc->nKeys);

    /* Search the leaf page for the entry whose keys are equal to the given keys. */
    found = mlgf_SearchLeafPageInMortonOrder(handle, apage, kdesc, &keyMortonVal, &entryNo);

    if (!found) return(MLGF_STATUS_NOTFOUND);

    /* Get the length of an object item in object array. */
    objectItemLen = MLGF_LEAFENTRY_OBJECTITEM_LEN(kdesc->extraDataLen);

    /* 'entry' points to the leaf entry which contains the deleted object. */
    entry = MLGF_ITH_LEAFENTRY(apage, entryNo);

    if (entry->nObjects > 0) { /* normal entry */

	found = mlgf_BinarySearchObjectArray(
	    handle, MLGF_LEAFENTRY_FIRST_OBJECT(kdesc->nKeys, entry),
	    oid, entry->nObjects, objectItemLen, &oidArrayElemNo);

	if (!found) {
            status->flags.notFound = 1;
            return(eNOERROR);
        }

	/* 'objectItemPtr' points to the deleted object. */
	objectItemPtr = MLGF_LEAFENTRY_ITH_OBJECTITEM(kdesc->nKeys, kdesc->extraDataLen, entry, oidArrayElemNo);
	if (data) memcpy(data, objectItemPtr + sizeof(ObjectID), kdesc->extraDataLen); 

	if (entry->nObjects == 1) {

	    memmove(&apage->slot[-(apage->hdr.nEntries-2)], &apage->slot[-(apage->hdr.nEntries-1)],
		    (apage->hdr.nEntries-entryNo-1)*sizeof(Two));
	    apage->hdr.nEntries--;

	    apage->hdr.unused += MLGF_LEAFENTRY_LENGTH(kdesc->nKeys, kdesc->extraDataLen, entry->nObjects);

	} else {

	    memmove(objectItemPtr, objectItemPtr + objectItemLen,
		    (entry->nObjects - oidArrayElemNo - 1)*objectItemLen);

	    entry->nObjects --;
	    apage->hdr.unused += objectItemLen;
	}

        /* Set the dirty flag. */
        e = BfM_SetDirty(handle, &apage->hdr.pid, PAGE_BUF);
        if (e < 0) ERR(handle, e);

    } else {		   /* overflow page */
	MAKE_PAGEID(ovPid, apage->hdr.pid.volNo, MLGF_LEAFENTRY_FIRST_OVERFLOW(kdesc->nKeys, entry));

	e = mlgf_DeleteOverflow(handle, root, &ovPid, kdesc, keys, oid, data, &of, dlPool, dlHead); 
	if (e < 0) ERR(handle, e);

	/* of = # of objects when the number is less than 1/4 of the maximum */
	/* number of objects in one overflow page. */

        entryOldLen = MLGF_LEAFENTRY_LENGTH(kdesc->nKeys, kdesc->extraDataLen, entry->nObjects);
        entryNewLen = MLGF_LEAFENTRY_LENGTH(kdesc->nKeys, kdesc->extraDataLen, of);
        if ((entryNewLen < MLGF_OP_OUT_THRESHOLD) && (entryNewLen - entryOldLen <= MLGF_LP_FREE(apage))) {
            /*@ get the page */
            e = BfM_GetTrain(handle, &ovPid, (char**)&opage, PAGE_BUF);
            if (e < eNOERROR) ERR(handle, e);

            if (of == 0) { /* Delete the leaf entry. */
                /* Move slots to delete the slot */
                for (i = entryNo+1; i < apage->hdr.nEntries; i++)
                    apage->slot[-(i-1)] = apage->slot[-i];

                /* free the space and decrement the # of entries by 1 */
                apage->hdr.unused += entryOldLen;
                apage->hdr.nEntries --;

            } else {    /* of > 0 */
                mlgf_ChangeLeafEntrySize(handle, apage, kdesc->nKeys, kdesc->extraDataLen, entryNo, entryOldLen, entryNewLen);

                /* entry was moved on the above call */
                entry = MLGF_ITH_LEAFENTRY(apage, entryNo);

                /* the original leaf entry is on free space boundary */
                memcpy(MLGF_LEAFENTRY_FIRST_OBJECT(kdesc->nKeys, entry),
                       MLGF_OVERFLOW_ITH_OBJECTITEM(objectItemLen, opage, 0),
                       of *objectItemLen);

                entry->nObjects = of;
            }

            opage->hdr.type = MLGF_FREEPAGE;
            
            e = BfM_SetDirty(handle, &ovPid, PAGE_BUF);
            if (e < 0) ERRB1(handle, e, &ovPid, PAGE_BUF);
            
            e = BfM_FreeTrain(handle, &ovPid, PAGE_BUF);
            if (e < eNOERROR) ERR(handle, e);

            /*
            ** Insert a new node for the dropped file.
            ** Original code:
            ** e = RDsM_FreeTrain(&ovPid, PAGESIZE2);
            ** if (e < eNOERROR) ERR(handle, e);
            */    
            e = Util_getElementFromPool(handle, dlPool, &dlElem);
            if (e < 0) ERR(handle, e);

            dlElem->type = DL_PAGE;
            dlElem->elem.pid = ovPid; /* save the deallcoated PageID. */
            /* dlElem->pFid = *pFid;*/  /* save the file identifier */
            dlElem->next = dlHead->next; /* insert to the list */
            dlHead->next = dlElem;       /* new first element of the list */

            /* Set the dirty flag. */
            e = BfM_SetDirty(handle, &apage->hdr.pid, PAGE_BUF);
            if (e < 0) ERR(handle, e);
        }
    }

    /*
    ** Update the extreme hash values of the parent entry.
    */
    dirEntryHashValues = MLGF_DIRENTRY_HASHVALUEPTR(entryToLeaf, kdesc->nKeys);
    for (k = 0; k < kdesc->nKeys; k++) {

	if ((MLGF_KEYDESC_IS_MINTYPE(*kdesc, k) && (dirEntryHashValues[k] < keys[k])) ||
	    (MLGF_KEYDESC_IS_MAXTYPE(*kdesc, k) && (dirEntryHashValues[k] > keys[k])))
	    continue;

        status->flags.mbrUpdated = 1;
	if (MLGF_KEYDESC_IS_MINTYPE(*kdesc, k))
            dirEntryHashValues[k] |= MLGF_HASHVALUE_SET_EXCEPT_UPPER_N_BITS(entryToLeaf->nValidBits[k]);
        else
            dirEntryHashValues[k] = MLGF_HASHVALUE_MASK_UPPER_N_BITS(dirEntryHashValues[k], entryToLeaf->nValidBits[k]);

	for (i = 0; i < apage->hdr.nEntries; i++) {
	    entry = MLGF_ITH_LEAFENTRY(apage, i);

	    if ((MLGF_KEYDESC_IS_MINTYPE(*kdesc, k) && (dirEntryHashValues[k] > entry->keys[k])) ||
		(MLGF_KEYDESC_IS_MAXTYPE(*kdesc, k) && (dirEntryHashValues[k] < entry->keys[k]))) {
		dirEntryHashValues[k] = entry->keys[k];
	    }
	}
    }

    /* Set the theta value. */
    if (MLGF_LP_THETA(apage) != entryToLeaf->theta) {
        entryToLeaf->theta = MLGF_LP_THETA(apage);
        status->flags.thetaUpdated = 1;
    }

    if (apage->hdr.nEntries == 0)
        status->flags.emptyPage = 1;
    else if (MLGF_LP_THETA(apage) < MLGF_LP_THRESHOLD)
        status->flags.underflow = 1;

    if (status->flags.emptyPage) {

        apage->hdr.type = MLGF_FREEPAGE;
        
        /*
        ** Insert a new node for the dropped file.
        ** Original code:
        ** e = RDsM_FreeTrain(&apage->hdr.pid, PAGESIZE2);
        ** if (e < 0) ERR(handle, e);
        */    
        e = Util_getElementFromPool(handle, dlPool, &dlElem);
        if (e < 0) ERR(handle, e);

        dlElem->type = DL_PAGE;
        dlElem->elem.pid = apage->hdr.pid; /* save the deallcoated PageID. */
        dlElem->next = dlHead->next; /* insert to the list */
        dlHead->next = dlElem;       /* new first element of the list */
    }

    return(eNOERROR);

} /* mlgf_DeleteObjectFromLeaf() */
