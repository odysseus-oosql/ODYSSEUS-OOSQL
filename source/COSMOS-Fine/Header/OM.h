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
/*    Fine-Granule Locking Version                                            */
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
#ifndef __OM_H__
#define __OM_H__

#include "Util_pool.h"		/* to get Pool */
#include "xactTable.h"
#include "LOG.h"
#include "BfM.h"

/* do the operation on bytes from the given position to the end */
#define REMAINDER -1


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
    PageID pid;                 /* page id of this page */
    Four pageFlags;             /* flags for page characteristics */
    Four pageReserved;          /* reserved space for page characteristics */
    Lsn_T lsn;                  /* page lsn */
    Four logRecLen;             /* log record length */
    FileID fid;			/* fileID within its volume */
    Two nSlots;			/* slots in use on the page */
    Two free;			/* offset of contiguous free area on page */
    Two unused;			/* number of unused bytes which are not
				   part of the contiguous freespace */
#ifdef CCRL
    /* BEGIN: for space reservation */
    Two totalFreeSpace;         /* free space counter; i.e. actual amount of */
                                /* free space on this page */
    Two rsvd;                   /* reserved space count */
    Two trsvd;                  /* space count reserved by trans */
    XactID trans;               /* transaction to reserve space on this page */
    /* END: for space reservation */
#endif /* CCRL */

    /*
     * In Sparc machine, at this position 2 fill chars are included
     */

    Unique unique;	    	/* unique number to allocate */
    Unique uniqueLimit;   	/* limit of valid unique number */
    ShortPageID nextPage;	/* Next PageID of data file */
    ShortPageID prevPage;	/* Prev PageID of data file */
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


/* useful macros */
/* the total freespace */
#ifdef CCPL
#define SP_FREE(_xactId, p)  ((p)->header.unused + SP_CFREE(p))
#define SP_CFREE(p) \
(PAGESIZE - SP_FIXED - (p)->header.free - ((p)->header.nSlots-1)*((Four)sizeof(SlottedPageSlot))) 
#endif /* CCPL */

#ifdef CCRL
#define SP_FREE(_xactId, p)  ((p)->header.totalFreeSpace - (p)->header.rsvd + (XACTID_CMP_EQ(_xactId,(p)->header.trans) ? (p)->header.trsvd:0))
#define SP_CFREE(p) \
(PAGESIZE - SP_FIXED - (p)->header.free - ((p)->header.nSlots-1)*((Four)sizeof(SlottedPageSlot))) 
#endif /* CCRL */

#ifdef CCPL
#define OM_INIT_SLOTTED_PAGE(_apage, _fid, _pid) \
BEGIN_MACRO \
SET_PAGE_TYPE(_apage, SLOTTED_PAGE_TYPE); \
_apage->header.fid = _fid; \
_apage->header.pid = _pid; \
_apage->header.nSlots = 1; \
_apage->header.free = 0; \
_apage->header.unused = 0; \
_apage->header.unique = 1; \
_apage->header.uniqueLimit = 0; \
_apage->header.prevPage = NIL; \
_apage->header.nextPage = NIL; \
_apage->slot[0].offset = EMPTYSLOT; \
END_MACRO
#endif /* CCPL */

#ifdef CCRL
#define OM_INIT_SLOTTED_PAGE(_apage, _fid, _pid) \
BEGIN_MACRO \
SET_PAGE_TYPE(_apage, SLOTTED_PAGE_TYPE); \
_apage->header.fid = _fid; \
_apage->header.pid = _pid; \
_apage->header.nSlots = 1; \
_apage->header.free = 0; \
_apage->header.unused = 0; \
_apage->header.unique = 1; \
_apage->header.uniqueLimit = 0; \
_apage->header.prevPage = NIL; \
_apage->header.nextPage = NIL; \
_apage->header.totalFreeSpace = PAGESIZE - SP_FIXED; \
_apage->header.rsvd = 0; \
_apage->header.trsvd = 0; \
SET_NIL_XACTID(_apage->header.trans); \
_apage->slot[0].offset = EMPTYSLOT; \
END_MACRO
#endif /* CCRL */

/* constant macro for the empty slot */
/* The empty slots have EMPTYSLOT with the 'offset' */
#define EMPTYSLOT		-1

#define IS_LRGOBJ_ROOTWITHHDR(_properties) (((_properties) & P_LRGOBJ_ROOTWITHHDR) ? TRUE:FALSE)
#define IS_VALID_OBJECTID(oid, s_page) \
    (((s_page->slot[-(oid)->slotNo].offset == EMPTYSLOT) || \
      (s_page->slot[-(oid)->slotNo].unique != (oid)->unique)) ? FALSE : TRUE)

#define LRGOBJ_THRESHOLD (PAGESIZE - SP_FIXED - sizeof(ObjectHdr))

#define GET_PTR_TO_CATENTRY_FOR_DATA(slotNo, catPage, catEntry) \
{   \
    Object *obj = (Object *)&(catPage->data[(catPage)->slot[-(slotNo)].offset]); \
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
        Four type;
        Four offset; /* offset from the start of tuple */
        Four length;
    } parts[MAXNUMKEYPARTS];
} omSortKeyAttrInfo;

typedef Four (*omGetKeyAttrsFuncPtr_T) (Four, Object*, void*, SortKeyDesc*, omSortKeyAttrInfo*);

Four om_GetTempObject(Four, Object*, Object*, Four*, SortKeyDesc*, omGetKeyAttrsFuncPtr_T, void*);
Four om_RestoreTempObject(Four, XactTableEntry_T*, FileID*, SegmentID_T*, SegmentID_T*, Object*, Object*, omSortKeyAttrInfo*, Boolean, LogParameter_T*);
Four om_CalculateSortKeyLengthFromAttrInfo(Four, omSortKeyAttrInfo*, Four*, Four*);
Four om_CalculateSortKeyLengthFromSortKey(Four, Object *, omSortKeyAttrInfo*);
Four OM_ReadLargeObject(Four, Object*, Four, Four, char*);
Four om_ConvertToLargeObject(Four, XactTableEntry_T*, DataFileInfo*, Object*, LogParameter_T*);
Four om_RestoreLargeObject(Four, XactTableEntry_T*, FileID*, SegmentID_T*, SegmentID_T*, Object*, Object*, Four, Four, LogParameter_T*);
Four OM_SortInto(Four, XactTableEntry_T*, VolID, DataFileInfo*, FileID*, sm_CatOverlayForData*, SortKeyDesc*, omGetKeyAttrsFuncPtr_T, void*, Boolean, Boolean, LogParameter_T*);

/*
** Object Manager Internal Function Prototypes
*/
Four om_ConvertToLarge(Four, XactTableEntry_T*, DataFileInfo*, SlottedPage*, Four, LockParameter*, LogParameter_T*);
Four om_ChangeObjectSize(Four, XactID*, SlottedPage*, Four, Four, Four, Boolean);
void om_CompactPage(Four, SlottedPage*, Four);
Four om_CreateObject(Four, XactTableEntry_T*, DataFileInfo*, ObjectID*, ObjectHdr*, Four, char*, LockParameter*, ObjectID*, LogParameter_T*);
Four om_GetUnique(Four, XactTableEntry_T*, Buffer_ACC_CB*, Unique*, LogParameter_T*);
Four om_AddPageToFile(Four, XactTableEntry_T*, DataFileInfo*, PageID*, SlottedPage*, LockParameter*, LogParameter_T*);
Four om_DeletePageFromFile(Four, XactTableEntry_T*, DataFileInfo*, SlottedPage*, LockParameter*, LogParameter_T*);
Four om_LoadUnique(Four, Four);
Four om_DumpSlottedPage(Four, SlottedPage *);
Four OM_DumpSlottedPage(Four, XactTableEntry_T*, FileID *, PageID *, LockParameter*);
Four om_DumpObject(Four, XactTableEntry_T*, Object*, ObjectID*);
Four OM_DumpObject(Four, XactTableEntry_T*, FileID *, ObjectID *, LockParameter*);
Four om_DumpSpaceList(Four, Four, ShortPageID);
Four OM_DumpSpaceList(Four, ObjectID*);
Four om_InitSlottedPage(Four, XactTableEntry_T*, SlottedPage*, FileID*, PageID*, LogParameter_T*);
Four om_SaveUnique(Four, Four);
Four om_GetPhysicalFileID(Four, XactTableEntry_T*, DataFileInfo*, PhysicalFileID*, LockParameter*); 

#ifdef CCRL
Four om_AcquireSpace(Four, XactID*, SlottedPage*, Four, Four, Boolean*, Boolean*);
void om_InitReserve(Four);
Four om_ReleaseSpace(Four, XactID*, SlottedPage*, Four, Boolean*);
Four om_UndoAcquire(Four, XactID*, SlottedPage*, Four, Boolean*);
Four om_UndoRelease(Four, XactID*, SlottedPage*, Four, Boolean*);
#endif /* CCRL */

/*
** Object Manager Interface Function Prototypes
*/
Four OM_AppendToObject(Four, XactTableEntry_T*, DataFileInfo*, ObjectID*, Four,
		       char*, LockParameter*, LogParameter_T*);
Four OM_CompactPage(Four, SlottedPage *apage, Four);
Four OM_CreateFile(Four, XactTableEntry_T*, FileID*, sm_CatOverlayForData*, LogParameter_T*);
Four OM_CreateFirstObjectInSysTables(Four, XactTableEntry_T*, PhysicalFileID*, ObjectHdr*, Four, char*, ObjectID*, LockParameter*, LogParameter_T*);
Four OM_CreateObject(Four, XactTableEntry_T*, DataFileInfo*, ObjectID*, ObjectHdr*, Four, char*, ObjectID*, LockParameter*, LogParameter_T*);
Four OM_DeleteFromObject(Four, XactTableEntry_T*, DataFileInfo*, ObjectID*, Four, Four, LockParameter*, LogParameter_T*);
Four OM_DestroyObject(Four, XactTableEntry_T*, DataFileInfo*, ObjectID*, LockParameter*, LogParameter_T*);
Four OM_DropFile(Four, XactTableEntry_T*, PhysicalFileID*, SegmentID_T*, SegmentID_T*, Boolean, LogParameter_T*);
Four OM_GetObjectHdr(Four, XactTableEntry_T*, DataFileInfo*, ObjectID*, ObjectHdr*, LockParameter*);
Four OM_NextObject(Four, XactTableEntry_T*, DataFileInfo*, ObjectID*, ObjectID*, ObjectHdr*, LockParameter*);
Four OM_InsertInObject(Four, XactTableEntry_T*, DataFileInfo*, ObjectID*, Four, Four, char*, LockParameter*, LogParameter_T*);
Four OM_PrevObject(Four, XactTableEntry_T*, DataFileInfo*, ObjectID*, ObjectID*, ObjectHdr*, LockParameter*);
Four OM_SetObjectHdr(Four, XactTableEntry_T*, DataFileInfo*,ObjectID*, ObjectHdr*, LockParameter*, LogParameter_T*);
Four OM_ReadObject(Four, XactTableEntry_T*, FileID*, ObjectID*, Four, Four, char*, LockParameter*); 
Four OM_WriteObject(Four, XactTableEntry_T*, DataFileInfo*, ObjectID*, Four, Four, char*, LockParameter*, LogParameter_T*);
Four OM_WriteObjectRedoOnly(Four, XactTableEntry_T*, DataFileInfo*, ObjectID*, Four, Four, char*, LockParameter*, LogParameter_T*);
Four OM_InitLocalDS(Four);    
Four OM_InitSharedDS(Four);    
Four OM_GetStatistics_DataFilePageInfo(Four, XactTableEntry_T*, DataFileInfo*, PageID*, Four*, sm_PageInfo*, LockParameter*); 

#ifdef CCRL
Four OM_GetFileIdFromObjectId(Four, ObjectID*, FileID*);
#endif /* CCRL */

Four om_GetSegmentIDFromDataFileInfo(Four, XactTableEntry_T*, DataFileInfo*, SegmentID_T*, Four);
Four om_SetSegmentIDToCatOverlayForDataUsingDataFileInfo(Four, XactTableEntry_T*, DataFileInfo*, SegmentID_T*, Four, LogParameter_T*);
Four om_SetSegmentIDToCatOverlayForDataUsingCatOverlayForData(Four, sm_CatOverlayForData*, SegmentID_T*, Four);

#endif /* __OM_H__ */
