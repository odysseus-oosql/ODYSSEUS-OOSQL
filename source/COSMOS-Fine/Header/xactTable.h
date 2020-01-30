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
#ifndef __XACTTABLE_H__
#define __XACTTABLE_H__

#include "latch.h"

#define MAX_DEPTH_OF_NESTED_TOP_ACTIONS 	3
#define DEALLOC_PAGE_ARRAY_INCREASE_SIZE  	5
#define DEALLOC_TRAIN_ARRAY_INCREASE_SIZE  	5

/*
 * XactTable_T : transaction table
 */
typedef struct NestedTopActionInfo_T_tag {
    Two   	nestedTopActionNo;    		/* nested top action no */
    Lsn_T 	undoNextLsn;        		/* undo next lsn for this nested top action */
    Lsn_T 	deallocLsn;           		/* LSN for logging of the deallocated pages/trains */
    Two   	idxOnDeallocPageArray; 
    Two   	idxOnDeallocTrainArray; 
} NestedTopActionInfo_T;

typedef struct XactTableEntry_T_tag {
    One    		status;					/* transaction status */
    Two    		nestedTopActionStackIdx; 		/* index for the nested top action stack */
    XactID 		xactId;					/* transaction identifier */
    GlobalXactID 	*globalXactId; 				/* global transaction id */
    Lsn_T  		firstLsn;            			/* first LSN for this transaction */
    Lsn_T  		lastLsn;				/* lastLsn for this transaction */
    Lsn_T  		undoNextLsn;				/* undoNextLsn for this transaction */
    Lsn_T  		deallocLsn;				/* deallocLsn for this transaction */
    Four   		nestedTopActionNoCounter; 		/* counter for the nested top action no */
    NestedTopActionInfo_T nestedTopActions[MAX_DEPTH_OF_NESTED_TOP_ACTIONS]; /* stack for the nested top actions */
    Two 		sizeOfDeallocPageArray;
    Two 		sizeOfDeallocTrainArray;
    LOGICAL_PTR_TYPE(PageID *) deallocPageArray; 		/* array of the deallocated pages */
    LOGICAL_PTR_TYPE(TrainID *) deallocTrainArray;     		/* array of the deallocated trains */ 
    Two 		idxOnDeallocPageSegmentArray; 
    Two 		idxOnDeallocTrainSegmentArray;    
    Two 		sizeOfDeallocPageSegmentArray; 
    Two 		sizeOfDeallocTrainSegmentArray;
    LOGICAL_PTR_TYPE(SegmentID_T *) deallocPageSegmentArray; 	/* array of the deallocated pages */ 
    LOGICAL_PTR_TYPE(SegmentID_T *) deallocTrainSegmentArray;   /* array of the deallocated trains */ 
    DeallocListElem 	dlHead;					/* deallocated List for this transaction */    
    LOGICAL_PTR_TYPE(struct XactTableEntry_T_tag *) nextEntry; 	/* next entry in the hash chain */ 
    LATCH_TYPE 		latch;           			/* control entry update/access */

} XactTableEntry_T;

typedef struct XactTable_T_tag {
    LATCH_TYPE 		latch;					/* control entry allocation/free */
    UFour 		hashTableSize_1;			/* hash table size - 1 (for hashing function) */
    LOGICAL_PTR_TYPE(XactTableEntry_T **) hashTable; 		/* hash table */ 
    LOGICAL_PTR_TYPE(XactTableEntry_T *)  freeEntryListHdr; 	/* header of the free list of XCB entries */
} XactTable_T;


/*
 * ActiveXactRec_T
 */
typedef struct ActiveXactRec_T_tag {
    One    	status;			/* transaction status */
    XactID 	xactId;			/* transaction identifier */
    Lsn_T  	firstLsn;            	/* first LSN for this transaction */ 
    Lsn_T  	lastLsn;		/* lastLsn for this transaction */
    Lsn_T  	undoNextLsn;		/* undoNextLsn for this transaction */
    Lsn_T  	deallocLsn;		/* deallocLsn for this transaction */
} ActiveXactRec_T;

#endif /* __XACTTABLE_H__ */

