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
#ifndef _OM_INTERNAL_H_
#define _OM_INTERNAL_H_

#include "Util_pool.h"		/* to get Pool */


/*@
 * Constant Definitions
 */
/* do the operation on bytes from the given position to the end */
#define REMAINDER -1


/*@
 * Type Definitions
 */

/*
 *----------------- Typedefs for Page Structures --------------------
 */

/*
 * define a type for a slot that includes a unique number
 */
typedef struct {
    Two 	offset;		/* points to actual storage area */
    Unique	unique;		/* unique number */
} SlottedPageSlot;

/*
 * Typedef for the header of slotted page
 */
typedef struct {
    PageID pid;                 /* page id of this page, should be located on the beginnig */
    Four flags;                 /* flag to store page information */
    Four reserved;              /* reserved space to store page information */
    Two nSlots;			/* slots in use on the page */
    Two free;			/* offset of contiguous free area on page */
    Two unused;			/* number of unused bytes which are not part of the contiguous freespace */

    /* 
     * In Sparc machine, at this position 2 fill chars are included 
     */

    FileID fid;			/* fileID within its volume */
    Unique unique;		/* unique number to allocate */
    Unique uniqueLimit;		/* limit of valid unique numbers */
    ShortPageID nextPage;	/* Next PageID of data file */
    ShortPageID prevPage;	/* Prev PageID of data file */
    ShortPageID	spaceListPrev;	/* double linked list of xx% free pages */
    ShortPageID	spaceListNext;	/*   within the same file */
} SlottedPageHdr;

/*
 * Typedef for slotted page
 */
#define SP_FIXED (sizeof(SlottedPageHdr) + sizeof(SlottedPageSlot))

typedef struct {
    SlottedPageHdr header;	  /* header of the slotted page */
    char data[PAGESIZE-SP_FIXED]; /* data area */
    SlottedPageSlot slot[1];	  /* slot arrays, indexes backwards */
} SlottedPage;


/*@
 * Macro Function Definitions
 */
/* useful macros */
/* the total freespace */
#define SP_FREE(p)  ((p)->header.unused + SP_CFREE(p))
#define SP_CFREE(p) \
(PAGESIZE - SP_FIXED - (p)->header.free - ((p)->header.nSlots-1)*((CONSTANT_CASTING_TYPE)sizeof(SlottedPageSlot))) 

#define SP_10SIZE       ((CONSTANT_CASTING_TYPE)((PAGESIZE-SP_FIXED)/10))
#define SP_20SIZE       ((CONSTANT_CASTING_TYPE)(((PAGESIZE-SP_FIXED)/10L)*2))
#define SP_30SIZE       ((CONSTANT_CASTING_TYPE)(((PAGESIZE-SP_FIXED)/10L)*3))
#define SP_40SIZE       ((CONSTANT_CASTING_TYPE)(((PAGESIZE-SP_FIXED)/10L)*4))
#define SP_50SIZE       ((CONSTANT_CASTING_TYPE)((PAGESIZE-SP_FIXED)/2))


/* object length's minimum limit */
/* When only the Small object */
/* #define OBJECT_MINIMUMLENGTH sizeof(ObjectID) */
/* When use the large object tree */
#define OBJECT_MINIMUMLENGTH sizeof(ShortPageID)


/* constant macro for the empty slot */
/* The empty slots have EMPTYSLOT with the 'offset' */
#define EMPTYSLOT		-1

#define IS_VALID_OBJECTID(oid, s_page) \
    (((s_page->slot[-(oid)->slotNo].offset == EMPTYSLOT) || \
      (s_page->slot[-(oid)->slotNo].unique != (oid)->unique)) ? FALSE : TRUE)

#define OBJECT_LENGTH(dataLength)

#define LRGOBJ_THRESHOLD (PAGESIZE - SP_FIXED - sizeof(ObjectHdr))

#define GET_PTR_TO_CATENTRY_FOR_DATA(catObjForFile, catPage, catEntry) \
{   \
    Four offset = catPage->slot[-(catObjForFile->slotNo)].offset; \
    Object *obj = (Object *)&(catPage->data[offset]); \
    catEntry = (sm_CatOverlayForData*)obj->data; \
}


/*
 *  For sort
 */

/* for size of keyAttrOffsetArray which located before sort keys in small object */
#define OFFSETARRAYSIZE(numKeyParts)    (sizeof(Two) * (numKeyParts))

/* Macro to check variable type */
#define IS_VARIABLE_FOR_SORT(type)  (type == SM_VARSTRING)

typedef struct {
    Two nparts;
    struct {
        Two             type;
        Four 		offset; /* offset from the start of tuple */
        Two             length;
    } parts[MAXNUMKEYPARTS];
} omSortKeyAttrInfo;

typedef Four (*omGetKeyAttrsFuncPtr_T) (Object*, void*, SortKeyDesc*, omSortKeyAttrInfo*);

Four om_GetTempObject(Four, Object*, Object*, Four*, SortKeyDesc*, omGetKeyAttrsFuncPtr_T, void*);
Four om_RestoreTempObject(Four, Object*, Object*, omSortKeyAttrInfo*, Four, Two, Boolean);
Four om_CalculateSortKeyLengthFromAttrInfo(Four, omSortKeyAttrInfo*, Two*, Two*);
Four om_CalculateSortKeyLengthFromSortKey(Four, Object *, omSortKeyAttrInfo*);
Four OM_ReadLargeObject(Four, Object*, Four, Four, char*);
Four om_ConvertToLargeObject(Four, Object*, Four, Two);
Four om_ConvertToLargeObjectRootWithHdr(Object*);
Four om_RestoreLargeObject(Four, Object*, Object*, Four, Four, Two, Boolean);
Four OM_SortInto(Four, VolID, ObjectID*, FileID*, sm_CatOverlayForData*, SortKeyDesc*, omGetKeyAttrsFuncPtr_T, void*, Boolean, Boolean, Pool*, DeallocListElem*); 


/*@
 * Function Prototypes
 */
/*
** Object Manager Internal Function Prototypes
*/
Four om_CreateObject(Four, ObjectID*, ObjectID*, ObjectHdr*, Four, char*, ObjectID*);
Four om_GetUnique(Four, PageID*, Unique*);
Four om_FileMapAddPage(Four, ObjectID*, PageID*, PageID*);
Four om_FileMapDeletePage(Four, ObjectID*, PageID*);
Four om_PutInAvailSpaceList(Four, ObjectID*, PageID*, SlottedPage*);
Four om_RemoveFromAvailSpaceList(Four, ObjectID*, PageID*, SlottedPage*);
Four om_DumpSlottedPage(Four, SlottedPage *);
Four OM_DumpSlottedPage(Four, PageID *);
Four om_DumpObject(Four, Object*, ObjectID*);
Four OM_DumpObject(Four, ObjectID *);
Four om_DumpSpaceList(Four, Four, ShortPageID);
Four om_IsTemporary(Four, FileID*, Boolean*); 


/*
** Object Manager Interface Function Prototypes
*/
Four OM_AppendToObject(Four, ObjectID*, ObjectID*, Four, char*, Pool*, DeallocListElem*);
Four OM_CompactPage(Four, SlottedPage *apage, Two);
Four OM_CreateFile(Four, FileID*, Two, sm_CatOverlayForData*, Boolean); 
Four OM_CreateObject(Four, ObjectID*, ObjectID*, ObjectHdr*, Four, char*, ObjectID*);
Four OM_DeleteFromObject(Four, ObjectID*, ObjectID*, Four, Four, Pool*, DeallocListElem*);
Four OM_DestroyObject(Four, ObjectID*, ObjectID*, Pool*, DeallocListElem*);
Four OM_DropFile(Four, PhysicalFileID*, Pool*, DeallocListElem*); 
char *OM_Err(Four);
Four OM_GetObjectHdr(Four, ObjectID*, ObjectHdr*);
Four OM_NextObject(Four, ObjectID*, ObjectID*, ObjectID*, ObjectHdr*);
Four OM_InsertInObject(Four, ObjectID*, ObjectID*, Four, Four, char*, Pool*, DeallocListElem*);
Four OM_PrevObject(Four, ObjectID*, ObjectID*, ObjectID*, ObjectHdr*);
Four OM_SetObjectHdr(Four, ObjectID*, ObjectHdr*);
Four OM_ReadObject(Four, ObjectID*, Four, Four, char*);
Four OM_WriteObject(Four, ObjectID*, Four, Four, char*);
Four OM_GetStatistics_DataFilePageInfo(Four, PageID*, Four*, sm_PageInfo*); 
    
#endif /* _OM_INTERNAL_H_ */
