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
#ifndef __UTIL_SORT_H__
#define __UTIL_SORT_H__


/* for om_AddTupleToInBufferArray */
#define	SUCCESS		100
#define FAIL		101


/* for om_QuickSort */
#define MAXSTACKDEPTH	50
#define QUICKSORTLIMIT	10


/* Macro to access 'pnoArray', 'runArray' & 'tuples' */

#define PNO_ARRAY(pnoArray)  ((PageNo *)((pnoArray).ptr))
#define RUN_ARRAY(runArray)  ((Partition *)((runArray).ptr))
#define TUPLE_ARRAY(tuples)  ((void **)((tuples).ptr))

#define SWAP_VAR_ARRAY(x, y) \
BEGIN_MACRO \
VarArray tmp; \
tmp = (x); (x) = (y); (y) = tmp; \
END_MACRO

#define SWAP_PNO_ARRAY(x, y)  SWAP_VAR_ARRAY(x, y)
#define SWAP_RUN_ARRAY(x, y)  SWAP_VAR_ARRAY(x, y)


/* type for stack which is used in om_Quicksort */
typedef struct {
	Four start;
	Four end;
} Partition;


typedef struct {
    Two         		nparts;         /* # of key parts */
    Two         		hdrSize;        /* size of header in front of sorted tuple */
    struct {
        Four            	type;           /* part's type */
        Four            	length;         /* part's length */
        Four_Invariable         flag;           /* ascending/descendig = SORTKEYDESC_ATTR_ASC/SORTKEYDESC_ATTR_DESC */
    } parts[MAXNUMKEYPARTS];
} SortTupleDesc;


/*
 * Typedef for slotted page for sort
 * Note!! tuples in slotted page for sort are located contiguously without alignment
 */
typedef struct {
    Two                		offset;          /* points to actual storage area */
} SlottedPageForSortSlot;

typedef struct {

    /* common part of all page types of COSMOS */
    PageID              pid;             /* page id of this page, should be located on the beginnig */
    Four                flags;           /* flag to store page information */
    Four                reserved;        /* reserved space to store page information */

    Two                 nSlots;          /* slots in use on the page */
    Two                 free;            /* offset of contiguous free area on page */
} SlottedPageForSortHdr;

#define SP_FOR_SORT_FIXED (sizeof(SlottedPageForSortHdr) + sizeof(SlottedPageForSortSlot))

typedef struct {
    SlottedPageForSortHdr  header;                           /* header of the slotted page */
    char                   data[PAGESIZE-SP_FOR_SORT_FIXED]; /* data area */
    SlottedPageForSortSlot slot[1];                          /* slot arrays, indexes backwards */
} SlottedPageForSort;

#define SP_FOR_SORT_FREE(p) \
(PAGESIZE - SP_FOR_SORT_FIXED - (p)->header.free - ((p)->header.nSlots-1)*((CONSTANT_CASTING_TYPE)sizeof(SlottedPageForSortSlot))) 



/*
 * Typedef for input tuples
 */
typedef struct {
    Two         len;
    char*       data;
} SortStreamTuple;

typedef struct {
    Two         len;
    char        data[1];
} SlottedPageForSortTuple;



typedef struct {

    /* 
     *  Volume ID in which temporary files are allocated
     *  Note!! if volId is NILL, this entry is empty 
     */
    VolID               volId;

    /* 
     *  Sort key descriptor 
     */
    SortTupleDesc       sortTupleDesc;

    /*
     *  External sort flag
     */
    Boolean             externalSortFlag;

    /* 
     *  # of tuples in sortInBufferArray 
     */
    Four                numTuples;
    VarArray            tuples;          /* array which points each tuples in buffer */

    /* 
     *  Temporary files' ID 
     */
    PhysicalFileID      tmpFile1;
    PhysicalFileID      tmpFile2;

    /* 
     *  Input/Output buffer 
     */
    Four                sortOutBufferArrayIdx;
    SlottedPageForSort* sortOutBufferArray;
    Four                sortInBufferArrayIdx;
    SlottedPageForSort* sortInBufferArray;

    /* 
     *  For PNo array 
     */
    Four                numSrcPno;
    Four                numDstPno;
    VarArray            srcPnoArray;     /* points source temporary file's pages */
    VarArray            dstPnoArray;     /* points destination temporary file's pages */

    /* 
     *  For managing run 
     */
    /* Note!! each entry of the run array points to the start page and the last page of the run. */
    Four                numSrcRun;
    Four                numDstRun;
    VarArray            srcRunArray;     /* points source temporary file's runs */
    VarArray            dstRunArray;     /* points destination temporary file's runs */

    /*
     *  For merge phase
     */
    Four                winner;
    Four                numRunInBuffer; 
    Four                loserTreeSize;          /* size of loser tree */
    Four                loserTree[MAX_NUM_RUN]; /* internal node of loser tree */
    Four                pageIdxInRun[MAX_NUM_RUN];

    /*
     *  total # of tuples & total size of tuples in this stream
     */
    Four                totalNumTuples;
    Two                 totalSizeOfTuples;

} SortStreamTableEntry;


Four Util_Sort_Init(Four);
Four Util_OpenSortStream(Four, VolID, SortTupleDesc*);
Four Util_CloseSortStream(Four, Four);
Four Util_SortingSortStream(Four, Four);
Four Util_PutTuplesIntoSortStream(Four, Four, Four, SortStreamTuple*);
Four Util_GetTuplesFromSortStream(Four, Four, Four*, SortStreamTuple*, Boolean*);
Four util_CreateRun(Four, SortStreamTableEntry*);
Four util_PutTupleIntoSortInBufferArray(Four, SortStreamTableEntry*, Two, char*);
Four util_PutTupleIntoSortOutBufferArray(Four, SortStreamTableEntry*, Two, char*);
Four util_FlushOutBuffer(Four, SortStreamTableEntry*);
Four util_ReadIthRunIntoInBuffer(Four, SortStreamTableEntry*, Four, Four);
Four util_CreateLoserTree(Four, SortTupleDesc*, void**, Four*, Four);
Four util_FixLoserTree(Four, SortTupleDesc*, void**, Four*, Four, Four);
Four util_QuickSort(Four, SortTupleDesc*, void**, Four);
Four util_SortKeyCompare(Four, SortTupleDesc*, void*, void*);
Four util_WriteTrains(Four, char *, PageID *, Four, Two);
Four util_ReadTrains(Four, PageID *, char *, Four, Two);
Four Util_GetNumTuplesInSortStream(Four, Four);
Four Util_GetSizeOfSortStream(Four, Four);

Four Util_OpenStream(Four, VolID);
Four Util_CloseStream(Four, Four);
Four Util_PutTuplesIntoStream(Four, Four, Four, SortStreamTuple*);
Four Util_ChangePhaseStream(Four, Four);
Four Util_GetTuplesFromStream(Four, Four, Four*, SortStreamTuple*, Boolean*);
Four Util_GetNumTuplesInStream(Four, Four);
Four Util_GetSizeOfStream(Four, Four);


#endif /* __UTIL_SORT_H__ */
