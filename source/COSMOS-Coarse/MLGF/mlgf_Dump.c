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
 * Module: mlgf_Dump.c
 *
 * Description:
 *  Includes routines dump the data structures used in the MLGF index.
 *
 * Exports:
 *  Four mlgf_DumpDirectoryPage(PageID*, MLGF_KeyDesc*)
 *  Four mlgf_DumpLeafPage(PageID*, MLGF_KeyDesc*)
 *  Four mlgf_DumpOverflowPage(PageID*, MLGF_KeyDesc*)
 */


#include <ctype.h>
#include "common.h"
#include "error.h"
#include "trace.h"
#include "Util.h"
#include "MLGF_Internal.h"
#include "perThreadDS.h"
#include "perProcessDS.h"


/* Internal Function Prototypes */
void mlgf_DumpDirectoryEntry(Four, mlgf_DirectoryEntry*, One);
void mlgf_DumpLeafEntry(Four, mlgf_LeafEntry*, MLGF_KeyDesc*);
void mlgf_DumpObjectItem(Four, char*, Two);


/*
 * Function: Four mlgf_DumpDirectoryPage(PageID*, MLGF_KeyDesc*)
 *
 * Description:
 *  Dump the directory page.
 *
 * Returns:
 *  Error code
 *    some errors caused by function calls
 */
Four mlgf_DumpDirectoryPage(
    Four handle,
    PageID              *pid)                   /* IN PageID of a directory page */
{
    Four                e;                      /* error code */
    Two                 i;                      /* index variable */
    Two                 entryLen;               /* the length of a directory entry */
    mlgf_DirectoryEntry *entry;                 /* a directory entry */

    mlgf_DirectoryPage *apage; /* pointer to buffer for the directory page */


    TR_PRINT(TR_MLGF, TR1, ("mlgf_DumpDirectoryPage(handle)"));


    e = BfM_GetTrain(handle, pid, (char**)&apage, PAGE_BUF);
    if (e < 0) ERR(handle, e);

    if (!(apage->hdr.type & MLGF_DIRECTORYPAGE)) {

	printf("Directory Page Expected!!!\n");

	e = BfM_FreeTrain(handle, pid, PAGE_BUF);
	if (e < 0) ERR(handle, e);

	return(eNOERROR);
    }

    printf("------------------------------------------------------------\n");
    printf("| PageID = (%ld, %ld)\n",
	   apage->hdr.pid.volNo, apage->hdr.pid.pageNo); 
    printf("| TYPE: DIRECTORY%s HEIGHT=%ld\n",
	   (apage->hdr.type & MLGF_ROOTPAGE) ? "|ROOT":"", apage->hdr.height);
    printf("| # of Keys = %ld    # of Entries = %ld\n", apage->hdr.nKeys, apage->hdr.nEntries);
    printf("------------------------------------------------------------\n");

    /* Get the length of a directory entry. */
    entryLen = MLGF_DIRENTRY_LENGTH(apage->hdr.nKeys);

    for (i = 0; i < apage->hdr.nEntries; i++) {
	entry = MLGF_ITH_DIRENTRY(apage, i, entryLen);

        printf("|%2ld|", i);
	mlgf_DumpDirectoryEntry(handle, entry, apage->hdr.nKeys);
    }

    printf("------------------------------------------------------------\n");

    e = BfM_FreeTrain(handle, pid, PAGE_BUF);
    if (e < 0) ERR(handle, e);

   return(eNOERROR);

} /* mlgf_DumpDirectoryPage() */


/*
 * Function: Four mlgf_DumpLeafPage(PageID*, MLGF_KeyDesc*)
 *
 * Description:
 *  Dump the leaf page.
 *
 * Returns:
 *  Error code
 *    some errors caused by function calls
 */
Four mlgf_DumpLeafPage(
    Four handle,
    PageID              *pid,           /* IN PageID of a leaf page */
    MLGF_KeyDesc        *kdesc)         /* IN key descriptor of an MLGF index */
{
    Four                e;              /* error code */
    Two                 i;              /* index variable */
    Two                 entryLen;       /* the length of a leaf entry */
    mlgf_LeafEntry      *entry;         /* a leaf entry */
    mlgf_LeafPage       *apage;         /* pointer to buffer for the leaf page */


    TR_PRINT(TR_MLGF, TR1, ("mlgf_DumpLeafPage(handle, pid=%P, kdesc=%P)", pid, kdesc));


    e = BfM_GetTrain(handle, pid, (char**)&apage, PAGE_BUF);
    if (e < 0) ERR(handle, e);

    if (!(apage->hdr.type & MLGF_LEAFPAGE)) {

	printf("Leaf Page Expected!!!\n");

	e = BfM_FreeTrain(handle, pid, PAGE_BUF);
	if (e < 0) ERR(handle, e);

	return(eNOERROR);
    }

    printf("------------------------------------------------------------\n");
    printf("|  PageID = (%ld, %ld)\n",
	   apage->hdr.pid.volNo, apage->hdr.pid.pageNo); 
    printf("| TYPE: LEAF   free=%ld    unused=%ld\n", apage->hdr.free, apage->hdr.unused);
    printf("| # of Entries = %ld\n", apage->hdr.nEntries);
    printf("------------------------------------------------------------\n");

    for (i = 0; i < apage->hdr.nEntries; i++) {
	entry = MLGF_ITH_LEAFENTRY(apage, i);

        printf("|[%2ld]%4ld| ", i, apage->slot[-i]);
	mlgf_DumpLeafEntry(handle, entry, kdesc);
    }

    printf("------------------------------------------------------------\n");

    e = BfM_FreeTrain(handle, pid, PAGE_BUF);
    if (e < 0) ERR(handle, e);

    return(eNOERROR);

} /* mlgf_DumpLeafPage() */



/*
 * Function: Four mlgf_DumpOverflowPage(PageID*, MLGF_KeyDesc*)
 *
 * Description:
 *  Dump the overflow page.
 *
 * Returns:
 *  Error code
 *    some errors caused by function calls
 */
Four mlgf_DumpOverflowPage(
    Four handle,
    PageID              *pid,           /* IN PageID of a overflow page */
    MLGF_KeyDesc        *kdesc)         /* IN key descriptor of an MLGF index */
{
    Four                e;              /* error code */
    Four                i;              /* index variable */
    Two                 objectItemLen;  /* the length of an object item in object array */
    char                *objectItemPtr; /* pointer to an object item in object array */
    mlgf_OverflowPage   *apage;         /* pointer to buffer for the overflow page */


    TR_PRINT(TR_MLGF, TR1, ("mlgf_DumpOverflowPage(handle, pid=%P, kdesc=%P)", pid, kdesc));


    e = BfM_GetTrain(handle, pid, (char**)&apage, PAGE_BUF);
    if (e < 0) ERR(handle, e);

    if (!(apage->hdr.type & MLGF_OVERFLOWPAGE)) {

	printf("Overflow Page Expected!!!\n");

	e = BfM_FreeTrain(handle, pid, PAGE_BUF);
	if (e < 0) ERR(handle, e);

	return(eNOERROR);
    }

    printf("------------------------------------------------------------\n");
    printf("| PageID = (%ld, %ld)\n",
	   apage->hdr.pid.volNo, apage->hdr.pid.pageNo); 
    printf("| TYPE: OVERFLOW  prevPage=%ld  nextPage=%ld\n", apage->hdr.prevPage, apage->hdr.nextPage);
    printf("| # of Objects = %ld\n", apage->hdr.nObjects);
    printf("------------------------------------------------------------\n");

    for (i = 0; i < apage->hdr.nObjects; i++) {
	objectItemPtr = MLGF_OVERFLOW_ITH_OBJECTITEM(objectItemLen, apage, i);

        printf("|[%2ld]| ", i);
	mlgf_DumpObjectItem(handle, objectItemPtr, kdesc->extraDataLen);
    }

    printf("------------------------------------------------------------\n");

    e = BfM_FreeTrain(handle, pid, PAGE_BUF);
    if (e < 0) ERR(handle, e);

    return(eNOERROR);

} /* mlgf_DumpOverflowPage() */



/*
 * Function: void mlgf_DumpDirectoryEntry(mlgf_DirectoryEntry*, One)
 *
 * Description:
 *  Dump the given directory entry.
 *
 * Returns:
 *  None
 */
void mlgf_DumpDirectoryEntry(
    Four		handle, 
    mlgf_DirectoryEntry *entry,         /* IN entry to dump */
    One                 nKeys)          /* IN # of keys of this index */
{
    One                 k;              /* index variable */
    MLGF_HashValue      *hashValuePtr;  /* starting offset of array of hash values */
    mlgf_MortonValue    morton;


    hashValuePtr = MLGF_DIRENTRY_HASHVALUEPTR(entry, nKeys);
    mlgf_GetMortonValue(handle, hashValuePtr, entry->nValidBits, &morton, nKeys);

    printf("spid=%ld, theta=%ld, region=", entry->spid, entry->theta);


    for (k = 0; k < nKeys; k++) {
        printf("[%ld|%.8X]", entry->nValidBits[k], hashValuePtr[k]);
    }

    printf("morton=[%ld|", morton.nBits);
    for (k = 0; k < nKeys; k++) {
        printf("%.8X", morton.val[k]);
    }
    printf("]\n");

} /* mlgf_DumpDirectoryEntry() */



/*
 * Function: void mlgf_DumpLeafEntry(mlgf_LeafEntry*, MLGF_KeyDesc*)
 *
 * Description:
 *  Dump the given leaf entry.
 *
 * Returns:
 *  None
 */
void mlgf_DumpLeafEntry(
    Four		handle,
    mlgf_LeafEntry      *entry,         /* IN entry to dump */
    MLGF_KeyDesc        *kdesc)         /* IN key descriptor of used index */
{
    Two                 i;
    One                 k;              /* index variable */
    ObjectID            *oid;           /* points to ObjectID */
    char                *objectItemPtr; /* points to an object item in object array */
    char                *data;          /* points to the extra data */


    printf("len=%ld, nObjects=%ld, keys=",
	   MLGF_LEAFENTRY_LENGTH(kdesc->nKeys, kdesc->extraDataLen, entry->nObjects), entry->nObjects);

    for (k = 0; k < kdesc->nKeys; k++) printf("[%0P]", entry->keys[k]);

    if (entry->nObjects > 0) { /* normal entry */
	objectItemPtr = MLGF_LEAFENTRY_FIRST_OBJECT(kdesc->nKeys, entry);

	oid = (ObjectID*)objectItemPtr;
        printf(", [(%ld,%ld,%ld,%ld)|", oid->volNo, oid->pageNo, oid->slotNo, oid->unique);

	data = objectItemPtr + sizeof(ObjectID);
	for (i = 0; i < kdesc->extraDataLen && i < 10; i++)
	    printf("%c", (isprint(data[i])) ? data[i]:'~');
	printf("]");

    } else {
        printf(", overflow=%ld", *((ShortPageID*)MLGF_LEAFENTRY_FIRST_OVERFLOW(kdesc->nKeys, entry)));
    }

    printf("\n");

} /* mlgf_DumpLeafEntry() */



/*
 * Function: void mlgf_DumpObjectItem(char*, Two)
 *
 * Description:
 *  Dump the given object item.
 *
 * Returns:
 *  None
 */
void mlgf_DumpObjectItem(
    Four		handle,
    char                *objectItemPtr, /* IN pointer to an object item */
    Two                 extraDataLen)   /* IN length of extra data */
{
    Two                 i;              /* index variable */
    ObjectID            *oid;           /* points to ObjectID */
    char                *data;          /* points to the extra data */


    oid = (ObjectID*)objectItemPtr;
    printf("[(%ld,%ld,%ld,%ld)|", oid->volNo, oid->pageNo, oid->slotNo, oid->unique);

    data = objectItemPtr + sizeof(ObjectID);
    for (i = 0; i < extraDataLen && i < 10; i++)
	printf("%c", (isprint(data[i])) ? data[i]:'~');
    printf("]\n");

} /* mlgf_DumpLeafEntry() */
