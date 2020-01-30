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
#ifndef __LOT_H__
#define __LOT_H__

#include "BfM.h"
#include "TM.h"

/* delete all the remainder from the given position */
#define TO_END 	-1

/*
 * Format of (count, page-pointer) pair for internal node
 */
typedef struct {
    ShortPageID	spid;			/* pointer to root of child */
    Four	count;			/* child's byte count */
} L_O_T_INodeEntry;

/*
 * define the header field of a large object internal node page
 */
typedef struct {
    PageID 	pid;                 	/* page id of this page */
    Four 	pageFlags;             	/* flags for page characteristics */
    Four 	pageReserved;          	/* reserved space for page characteristics */
    Lsn_T 	lsn;                  	/* page lsn */
    Four 	logRecLen;             	/* log record length */
    FileID 	fid;			/* fileID within its volume */
} L_O_T_INodePageHdr;

/*
 * define the header field of a large object node
 */
typedef struct {
    One		height;			/* distance to leaf nodes */
    Two		nEntries;		/* number of slots in node */
} L_O_T_INodeHdr;

/*
 * Internal node format
 */
/* (PAGESIZE-1) is to reserve 1 byte for dummy byte */ 
#define LOT_MAXENTRIES	(((PAGESIZE-1) - sizeof(L_O_T_INodePageHdr) - sizeof(L_O_T_INodeHdr))/sizeof(L_O_T_INodeEntry)) 

#define LOT_MAXENTRIES_ROOTWITHHDR (LOT_MAXENTRIES/3)

#define LOT_HALFENTRIES ((LOT_MAXENTRIES+1)/2)

#define LOT_INODE_FIXED (sizeof(L_O_T_INodeHdr) + sizeof(L_O_T_INodeEntry)*LOT_MAXENTRIES)

typedef struct {
    L_O_T_INodeHdr 	header;			/* header of the node */
    L_O_T_INodeEntry 	entry[LOT_MAXENTRIES]; 	/* array of slots themselves */
} L_O_T_INode;

typedef struct {
    L_O_T_INodePageHdr 	header;
    L_O_T_INode 	node;
    char 		dummy[PAGESIZE-sizeof(L_O_T_INodePageHdr)-sizeof(L_O_T_INode)];
} L_O_T_INodePage;

#define LOT_LNODE_MAXFREE (TRAINSIZE - sizeof(L_O_T_LNodeHdr))
#define LOT_LNODE_HALFFREE ((LOT_LNODE_MAXFREE+1)/2)

typedef struct {
    PageID 		pid;                 	/* page id of this page */
    Four 		pageFlags;             	/* flags for page characteristics */
    Four 		pageReserved;          	/* reserved space for page characteristics */
    Lsn_T 		lsn;                  	/* page lsn */
    Four 		logRecLen;             	/* log record length */
    FileID 		fid;			/* fileID within its volume */
} L_O_T_LNodeHdr;

typedef struct {
    L_O_T_LNodeHdr 	header;				/* Header : reserved area */
    char 		data[LOT_LNODE_MAXFREE];	/* Data Area */
} L_O_T_LNode;

/*
 * ItemList used for overflow handling
 */
typedef struct {
    Four 		nEntries;		/* # of entries in the list */
    Four 		nReplaces;		/* # of entries to replace original entries */
    VarArray 		*entryArrayPtr;    	/* pointer to entry array */ 
    L_O_T_INodeEntry 	*entry;			/* Here comes the entries */
} L_O_T_ItemList;


/*
 * L_O_T_Path
 *  - cut path information
 */
#define LOT_MAXDEPTH 	5

typedef struct {
    Buffer_ACC_CB 	*page_BCBP;   	/* buffer access control block of the node */
    L_O_T_INode 	*node;          /* pointer to the node when root is with header */
    Four       		c_idx;		/* entry index of the child node */
    Boolean    		c_uf;		/* Indicates the underflow of the child node */
} L_O_T_PathItem;

typedef struct {
    Four 		top;
    L_O_T_PathItem 	item[LOT_MAXDEPTH];
} L_O_T_Path;

#define LEFT 	0
#define RIGHT 	1

#define LOT_INIT_LEAF_NODE(_anode, _fid, _pid) \
BEGIN_MACRO \
    SET_PAGE_TYPE(_anode, LOT_L_NODE_TYPE); \
    (_anode)->header.pid = (_pid); \
    (_anode)->header.fid = (_fid); \
END_MACRO

#define LOT_INODE_USED_SIZE(_anode) \
    (sizeof(L_O_T_INodeHdr) + (_anode)->header.nEntries*sizeof(L_O_T_INodeEntry))

#define LOT_INIT_INODE_PAGE_HDR(_apage, _fid, _pid) \
BEGIN_MACRO \
    SET_PAGE_TYPE(_apage, LOT_I_NODE_TYPE); \
    (_apage)->header.pid = (_pid); \
    (_apage)->header.fid = (_fid); \
END_MACRO

#define LOT_GET_ENTRY_ARRAY(_handle) (perThreadTable[_handle].lotDS.lot_entryArraySwitch ^= 1, &(perThreadTable[_handle].lotDS.lot_entryArray[perThreadTable[_handle].lotDS.lot_entryArraySwitch]))


/* Large Object Tree Manager Internal Function Prototypes */
Four lot_AppendToDataPage(Four, XactTableEntry_T*, DataFileInfo*, Four, char*,
                          L_O_T_ItemList*, Boolean*, LogParameter_T*);
Four lot_AppendToObject(Four, XactTableEntry_T*, DataFileInfo*, PageID*, PageID*,
                        L_O_T_INode*, Four, char*, L_O_T_ItemList*, Boolean*,
                        Four*, LogParameter_T*); 
Four lot_DeleteFromLeaf(Four, XactTableEntry_T*, DataFileInfo*, L_O_T_ItemList*, Four,
                        Four, Boolean*, Boolean*, LogParameter_T*);
Four lot_DeleteFromObject(Four, XactTableEntry_T*, DataFileInfo*, L_O_T_ItemList*,
                          L_O_T_INode*, Four, Four, Four*, Boolean*, Boolean*,
                          L_O_T_Path*, LogParameter_T*);
Four lot_DropTree(Four, XactTableEntry_T*, PageID*, Boolean, LogParameter_T*);
void lot_FinalPath(Four, L_O_T_Path*);
Four lot_InsertInDataPage(Four, XactTableEntry_T*, DataFileInfo*, Four, Four, char *,
                          L_O_T_ItemList*, Boolean*, LogParameter_T*);
Four lot_InsertInObject(Four, XactTableEntry_T*, DataFileInfo*, PageID*, PageID*, L_O_T_INode*,
                        Four, Four, char*, L_O_T_ItemList*, Boolean*, Four*, LogParameter_T*); 
Four lot_InsertInternal(Four, XactTableEntry_T*, DataFileInfo*, PageID*, L_O_T_INodePage*,
                        L_O_T_INode*, Four, L_O_T_ItemList*, L_O_T_ItemList*,
                        Boolean*, LogParameter_T*); 
Four lot_MakeRoot(Four, XactTableEntry_T*, DataFileInfo*, PageID*, char*, Boolean*, Four,
                  Four, L_O_T_ItemList*, LogParameter_T*); 
Four lot_MakeRootWithHdr(Four, XactTableEntry_T*, Buffer_ACC_CB*, Four, PageID*, LogParameter_T*);
Four lot_MergeOrDistribute(Four, XactTableEntry_T*, DataFileInfo*, L_O_T_ItemList*,
                           Four*, Boolean*, Boolean*, LogParameter_T*);
Four lot_MergeOrDistributeLeaf(Four, XactTableEntry_T*, DataFileInfo *, L_O_T_ItemList *,
                               Boolean *, Boolean *, LogParameter_T*);
void lot_InitPath(Four, L_O_T_Path*);
Boolean lot_EmptyPath(Four, L_O_T_Path*);
Four lot_PushPath(Four, L_O_T_Path*, Buffer_ACC_CB*, L_O_T_INode*, Four, Boolean);
Four lot_PopPath(Four, L_O_T_Path*, Buffer_ACC_CB**, L_O_T_INode**, Four*, Boolean*); 
Four lot_ReplaceTop(Four, L_O_T_Path*, PageID*, Four);
Four lot_ReadData(Four, PageID*, Four, Four, char*);
Four lot_ReadObject(Four, Four, PageID *, L_O_T_INode*, Four, Four, char *);
Four lot_RebalanceTree(Four, XactTableEntry_T*, DataFileInfo*, L_O_T_Path*, char*,
                       Boolean*, Four, Boolean*, LogParameter_T*);
Four lot_SearchInNode(Four, L_O_T_INode*, Four);
Four lot_SeparateRootNode(Four, XactTableEntry_T*, DataFileInfo*, PageID*, char*, Boolean*, LogParameter_T*); 
Four lot_GetCount(Four, L_O_T_INode*, Four);
Four lot_InitInternal(Four, PageID*, Four, Four);
Four lot_WriteData(Four, XactTableEntry_T*, PageID*, Four, Four, char*, LogParameter_T*);
Four lot_WriteObject(Four, XactTableEntry_T*, Four, PageID*, L_O_T_INode*, Four, Four, char*, LogParameter_T*);

/* Large Object Tree Manager Interface Function Prototypes */
Four LOT_AppendToObject(Four, XactTableEntry_T*, DataFileInfo*, PageID*, char*, Boolean*,
                        Four, Four, char*, LogParameter_T*); 
Four LOT_CreateObject(Four, XactTableEntry_T*, DataFileInfo*, PageID*, char*, Boolean*, Four, Four, char*, LogParameter_T*); 
Four LOT_DeleteFromObject(Four, XactTableEntry_T*, DataFileInfo*, char*, Boolean*,
                          Four, Four, Four, LogParameter_T*);
Four LOT_DestroyObject(Four, XactTableEntry_T*, DataFileInfo*, char*, Boolean, LogParameter_T*);
Four LOT_DumpTree(Four, PageID *);
Four LOT_DumpInternal(Four, PageID *);
Four LOT_DumpLeaf(Four, PageID *, Four);
char *LOT_Err(Four);
Four LOT_GetSize(Four, char*, Boolean);
Four LOT_InsertInObject(Four, XactTableEntry_T*, DataFileInfo*, PageID*, char*, Boolean*,
                        Four, Four, Four, char*, LogParameter_T*); 
Four LOT_ReadObject(Four, XactTableEntry_T*, Four, char*, Boolean, Four, Four, char*);
Four LOT_LengthCheck(Four, PageID*);
Four LOT_StructCheck(Four, PageID*, Four);
Four LOT_WriteObject(Four, XactTableEntry_T*, DataFileInfo*, char*, Boolean, Four, Four, char*, LogParameter_T*);
Four LOT_InitLocalDS(Four);
Four LOT_InitSharedDS(Four);

Four LOT_BlkLd_CreateLargeObject(Four, XactTableEntry_T*, DataFileInfo*, PageID*, PageID*, LogParameter_T*);
Four LOT_BlkLd_AppendToObject(Four, XactTableEntry_T*, DataFileInfo*, PageID*, Four, char*, LogParameter_T*);
Four LOT_BlkLd_WriteObject(Four, XactTableEntry_T*, DataFileInfo*, PageID*, Four, Four, char*, LogParameter_T*);

Four LOT_FinalSharedDS(Four);
Four LOT_FinalLocalDS(Four);
#endif /* __LOT_H__ */


