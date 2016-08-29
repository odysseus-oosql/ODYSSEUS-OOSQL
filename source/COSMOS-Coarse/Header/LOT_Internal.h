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
#ifndef _LOT_INTERNAL_H_
#define _LOT_INTERNAL_H_

#include "OM_Internal.h"


/*@
 * Constant Definitions
 */
/* delete all the remainder from the given position */
#define TO_END -1


/*@
 * Type Definitions
 */
/*
 * Format of (count, page-pointer) pair for internal node
 */
typedef struct {
    ShortPageID	spid;		/* pointer to root of child */
    Four	count;		/* child's byte count */
} L_O_T_INodeEntry;

/* 
 * define the header field of a large object node
 */
typedef struct {
    /*
     * When the root node is placed in the slotted page with the object header,
     * the following pid is not used: it is wasted. 
     */
    PageID pid;                 /* page id of this page, should be located on the beginning */
    Four flags;                 /* flag to store page information */
    Four reserved;              /* reserved space to store page information */
    One	height;			/* distance to leaf nodes */
    Two	nEntries;		/* number of slots in node */
} L_O_T_INodeHdr;

/* 
 * Internal node format 
 */
#ifndef NDEBUG
#define LOT_MAXENTRIES 10
#else /* NDEBUG */
#define LOT_MAXENTRIES	((PAGESIZE - sizeof(L_O_T_INodeHdr))/sizeof(L_O_T_INodeEntry))
#endif /* NDEBUG */

#define LOT_MAXENTRIES_ROOTWITHHDR (LOT_MAXENTRIES/3)

#define LOT_HALFENTRIES ((LOT_MAXENTRIES+1)/2)

#define LOT_INODE_FIXED (sizeof(L_O_T_INodeHdr) + sizeof(L_O_T_INodeEntry)*LOT_MAXENTRIES)

typedef struct {
    L_O_T_INodeHdr header;	/* header of the node */
    L_O_T_INodeEntry entry[LOT_MAXENTRIES]; /* array of slots themselves */
    char dummy[PAGESIZE-LOT_INODE_FIXED];
} L_O_T_INode;

#define LOT_LNODE_MAXFREE (TRAINSIZE - sizeof(L_O_T_LNodeHdr))
#define LOT_LNODE_HALFFREE ((LOT_LNODE_MAXFREE+1)/2)

typedef struct {
    PageID pid;                 /* page id of this page, should be located on the beginning */
    Four flags;                 /* flag to store page information */
    Four reserved;              /* reserved space to store page information */
} L_O_T_LNodeHdr;

typedef struct {
    L_O_T_LNodeHdr hdr;		/* Leaf Node Header */
    char data[LOT_LNODE_MAXFREE];	/* Data Area */
} L_O_T_LNode;

/*
 * ItemList used for overflow handling
 */
typedef struct {
    Four nEntries;		/* # of entries in the list */
    Four nReplaces;		/* # of entries to replace original entries */
    VarArray *entryArrayPtr;    /* pointer to entry array */ 
    L_O_T_INodeEntry *entry;	/* Here comes the entries */
} L_O_T_ItemList;


/*
 * L_O_T_Path
 *  - cut path information
 */
#define LOT_MAXDEPTH 5

typedef struct {
    PageID     	pid;		/* PageID of the node */
    Two        	rootSlotNo;	/* slot no of root if root is with header */
    L_O_T_INode *node;		/* Pointer to the buffer holding the node */
    Two         c_idx;		/* entry index of the child node */
    Boolean    	c_uf;		/* Indicates the underflow of the child node */
} L_O_T_PathItem;

typedef struct {
    Four top;
    L_O_T_PathItem item[LOT_MAXDEPTH];
} L_O_T_Path;


/*@
 * Macro Definitions
 */


#define LEFT 0
#define RIGHT 1


/*@
 * Function Prototypes
 */
/* Large Object Tree Manager Internal Function Prototypes */
Four lot_AppendToDataPage(Four, ObjectID*, Four, char*, L_O_T_ItemList*, Boolean*);
Four lot_AppendToObject(Four, ObjectID*, PageID*, Two, Four, char*, L_O_T_ItemList*, Boolean*);
Four lot_DeleteFromLeaf(Four, ObjectID*, L_O_T_ItemList*, Four, Four, Boolean*, Boolean*, Pool*, DeallocListElem*);
Four lot_DistributeLeaf(Four, PageID*, Four*, PageID*, Four*);
Four lot_DeleteFromObject(Four, ObjectID*, L_O_T_ItemList*, Two, Four, Four, Two*, Boolean*, Boolean*, L_O_T_Path*, Pool*, DeallocListElem*);
Four lot_DropTree(Four, PageID*, Pool*, DeallocListElem*);
void lot_FinalPath(Four, L_O_T_Path*);
Four lot_InsertInDataPage(Four, ObjectID*, Four, Four, char *, L_O_T_ItemList*, Boolean*);
Four lot_InsertInObject(Four, ObjectID*, PageID*, Two, Four, Four, char*, L_O_T_ItemList*, Boolean*);
Four lot_InsertInRootWithHdr(Four, SlottedPage*, Two, Two, L_O_T_ItemList*);
Four lot_InsertInternal(Four, ObjectID*, PageID*, Two, Two, L_O_T_ItemList*, L_O_T_ItemList*, Boolean*);
Four lot_MakeRoot(Four, ObjectID*, PageID*, Two, Four, L_O_T_ItemList*);
Four lot_MakeRootWithHdr(Four, PageID*, SlottedPage*, Two, PageID*, Pool*, DeallocListElem*);
Four lot_MergeOrDistribute(Four, ObjectID*, L_O_T_ItemList*, Four*, Boolean*, Boolean*, Pool*, DeallocListElem*);
Four lot_MergeOrDistributeLeaf(Four, ObjectID *, L_O_T_ItemList *, Boolean *, Boolean *, Pool*, DeallocListElem*);
void lot_InitPath(Four, L_O_T_Path*);
Boolean lot_EmptyPath(Four, L_O_T_Path*);
Four lot_PushPath(Four, L_O_T_Path*, PageID*, Two, L_O_T_INode*, Two, Boolean);
Four lot_PopPath(Four, L_O_T_Path*, PageID*, Two*, L_O_T_INode**, Two*, Boolean*);
Four lot_ReplaceTop(Four, L_O_T_Path*, PageID*, Two);
Four lot_ReadData(Four, PageID*, Four, Four, char*);
Four lot_ReadObject(Four, PageID *, Two, Four, Four, char *);
Four lot_RebalanceTree(Four, ObjectID*, L_O_T_Path*, PageID*, Boolean*, Boolean*, Pool*, DeallocListElem*);
Four lot_SearchInNode(Four, L_O_T_INode*, Four);
Four lot_SeparateRootNode(Four, ObjectID*, PageID*, L_O_T_INode*, PageID*); 
Four lot_GetCount(Four, L_O_T_INode*, Two);
Four lot_InitInternal(Four, PageID*, Four, Four, Boolean); 
void lot_GetNodePointer(Four, Page*, Two, L_O_T_INode**);
Four lot_WriteData(Four, PageID*, Four, Four, char*);
Four lot_WriteObject(Four, PageID*, Two, Four, Four, char*);
Four lot_IsTemporary(Four, FileID*, Boolean*); 

/* Large Object Tree Manager Interface Function Prototypes */
Four LOT_AppendToObject(Four, ObjectID*, PageID*, Two, Four, char*);
Four LOT_ConvertToLarge(Four, ObjectID*, SlottedPage*, Two, Pool*, DeallocListElem*);
Four LOT_DeleteFromObject(Four, ObjectID*, PageID*, Two, Four, Four, Pool*, DeallocListElem*);
Four LOT_DestroyObject(Four, PageID*, Two, Pool*, DeallocListElem*);
Four LOT_DumpTree(Four, PageID *);
Four LOT_DumpInternal(Four, PageID *);
Four LOT_DumpLeaf(Four, PageID *, Four);
char *LOT_Err(Four);
Four LOT_GetLengthWithHdr(Four, Object*);
Four LOT_InsertInObject(Four, ObjectID*, PageID*, Two, Four, Four, char*);
Four LOT_ReadObject(Four, PageID*, Two, Four, Four, char*);
Four LOT_LengthCheck(Four, PageID*);
Four LOT_StructCheck(Four, PageID*, Four);
Four LOT_WriteObject(Four, PageID*, Two, Four, Four, char*);

Four LOT_BlkLd_CreateLargeObject(Four, ObjectID*, PageID*, PageID*);
Four LOT_BlkLd_AppendToObject(Four, ObjectID*, PageID*, Four, char*);
Four LOT_BlkLd_WriteObject(Four, PageID*, Four, Four, char*);

Four LOT_Init(Four);
Four LOT_Final(Four);
#endif /* _LOT_INTERNAL_H_ */


