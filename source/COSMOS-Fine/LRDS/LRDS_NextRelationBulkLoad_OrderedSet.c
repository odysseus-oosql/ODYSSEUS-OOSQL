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
/*
 * Module: LRDS_NextRelationBulkLoad_OrderedSet.c
 *
 * Description:
 *  Process the data file bulk load.
 *
 * Exports:
 *  Four LRDS_NextRelationBulkLoad_OrderedSet(Four, ColListStruct*, Boolean)
 */


#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include "common.h"
#include "error.h"
#include "trace.h"
#include "Util.h"
#include "SM.h"
#include "LRDS.h"
#include "BL_LRDS.h"
#include "perProcessDS.h"
#include "perThreadDS.h"
#include "RDsM.h"


#ifdef ORDEREDSET_BACKWARD_SCAN

/*
 * LRDS Ordered Set Forward & Backward Scan APIs
 */


#define HDR_SIZE_OF_ORDEREDSET_WITHOUT_NESTED_INDEX  OFFSET_OF(OrderedSetColHdr_T,nestedIndexId)
#define HDR_SIZE_OF_ORDEREDSET_WITH_NESTED_INDEX  sizeof(OrderedSetColHdr_T)
#define IS_LONG_ORDEREDSET(_colSize) (_colSize > (3*TRAINSIZE))

typedef Four OrderedSet_ElementOffset_T;
#define NO_MORE_SLOT (LONG_MIN+1)
#define UNUSED_SLOT  LONG_MIN 


/* internal function prototype */
Four lrds_NextRelationBulkLoad_FlushTuple(Four, LRDS_BlkLdTableEntry*, TupleID*);


/*@================================================
 *  LRDS_NextRelationBulkLoad_OrderedSetBulkLoad()
 * ===============================================*/

/*
 * Function : Four LRDS_NextRelationBulkLoad_OrderedSetBulkLoad()
 *
 * Description :
 *
 * Return Values :
 *  error code.
 *
 * Side Effects :
 */
Four LRDS_NextRelationBulkLoad_OrderedSetBulkLoad(
    Four	       		handle,
    Four               		tmpVolId,            /* IN temporary volume in which sort stream is created */
    Four               		blkLdId,             /* IN bulkload ID */
    Four               		colNo,               /* IN column number of ordered set type */
    Four               		nElements,           /* IN # of elements to append */
    Four               		elementsBufSize,     /* IN buffer size */
    char*             		elementsBuf,         /* IN elements to append */
    Boolean            		endOfTuple,          /* IN flag which indicates this ordered set is end of tuple or not */
#ifndef COMPRESSION
    TupleID*           		tid)                 /* OUT tuple ID */
#else
    TupleID*           		tid,                 /* OUT tuple ID */
    char*              		uncompressedElementsBuf,              
    VolNo              		volNoOfPostingTupleID,
    Four               		lastDocId)                 
#endif    
{
    Four               		e;                   /* error code */
    Four               		i;                   /* index variable */
    Four               		slotNo;              /* slot number of each element */
    char*              		ptr;                 /* pointer which points element in elementsBuf */
    Four               		length;              /* length of element including length field */
    Four               		varColumnNo;         /* column number of variable column */
    Boolean            		needNestedIndex;     /* flag which indicates nested index is needed */
    Four               		orderedSetHdrLen;    /* length of ordered set's header */
    Four               		orderedSetColLen;    /* length of ordered set column */
    OrderedSetColHdr_T 		orderedSetColHdr;    /* header of ordered set */
    Four               		slotArraySize;       /* size of slot array for nested index */
    Four               		slotArrayOffset;     /* offset of slot array for nested index in the tuple */
    Four               		keyLen;              /* length of key in each element */
    KeyValue           		kval;                /* key value of each element */
    ObjectID           		oid;                 /* oid which contains slotNo of each element */
    Four               		nestedIndexBlkLdId;  /* sort stream ID for nested index bulkload */
    Four               		remainedLen;         /* remained length of slotArray */
    Four               		nowInBufLen;         /* length of the slotArray in tuple buffer */
    ColDesc*           		complexTypeColDesc;  /* complex type column descriptor */
    OrderedSetAuxColInfo_T*     auxColInfo; 	     /* auxiliary column information for ordered set */
    LRDS_BlkLdTableEntry*       blkLdEntry;          /* bulkload table entry pointer */
    OrderedSet_ElementOffset_T* slotArray;           /* slot array */
    OrderedSet_ElementLength    elementLen;          /* length of element in ordered set */
    OrderedSet_ElementOffset_T  elementOffset;       /* offset of element in ordered set */
    Four			totalElementLength; /* total element length */
    ColDesc *blkLdRelTableEntry_cdesc;
#ifdef COMPRESSION
    Four               		lengthOfUncompressed;
    OrderedSet_ElementLength    elementLenOfUncompressed;
#endif

    /*
     *  parameter check
     */
    if (elementsBufSize <= 0 || elementsBuf == NULL || nElements <= 0) ERR(handle, eBADPARAMETER);


    /*
     *  O. Set entry for fast access
     */
    blkLdEntry = &LRDS_BLKLD_TABLE(handle)[blkLdId];
    blkLdRelTableEntry_cdesc = PHYSICAL_PTR(blkLdEntry->lrdsBlkLdRelTableEntry->cdesc);

    /*
     *  I. Update global variables for data file bulkload
     */

    /* error check */
    /* Note!! this function must be called only once */
    if (colNo <= blkLdEntry->lrdsBlkLdRemainFlagArrayIdxForVarCols) ERR(handle, eINTERNAL);

    /* update 'lrdsBlkLdRemainFlagArrayIdxForVarCols' */
    blkLdEntry->lrdsBlkLdRemainFlagArrayIdxForVarCols = colNo;

    /* set corresponding entry of 'lrdsBlkLdRemainFlagArray' as FALSE */
    blkLdEntry->lrdsBlkLdRemainFlagArray[colNo] = FALSE;

    /* reset the corresponding bit of null vector. */
    BITRESET(blkLdEntry->lrdsBlkLdNullVector, colNo);


    /*
     *  II. Set complex type column description for fast access
     */

    /* set complexTypeColDesc */
    complexTypeColDesc = &blkLdRelTableEntry_cdesc[colNo];

    /* error check */
    if (complexTypeColDesc->complexType != SM_COMPLEXTYPE_ORDEREDSET) ERR(handle, eBADPARAMETER);

    /* assertion check */
    assert(complexTypeColDesc->varColNo != NIL);


    /*
     *  III. Construct header of ordered set column
     */

    /* set 'auxColInfo' for fast access */
    auxColInfo = (OrderedSetAuxColInfo_T *) PHYSICAL_PTR(complexTypeColDesc->auxInfo);

    /* set 'nElements' */
    orderedSetColHdr.nElements = nElements;

#ifdef COMPRESSION
    /* set 'lastDocId' */
    orderedSetColHdr.volNo      = volNoOfPostingTupleID;
    orderedSetColHdr.lastDocId  = lastDocId;
#endif

    /* set 'needNestedIndex' */
    needNestedIndex = IS_LONG_ORDEREDSET(elementsBufSize) && auxColInfo->nestedIndexFlag;

    /* III-1. if nested index(sub-index) is needed */
    if (needNestedIndex) {

#ifndef NDEBUG
        printf("NestedIndex Created!! \n");
#endif

        /* Create nested index */
        e = SM_AddIndex(handle, &blkLdEntry->lrdsBlkLdRelTableEntry->ri.fid, &orderedSetColHdr.nestedIndexId, NULL);
        if (e < eNOERROR) ERR(handle, e);


        /* Initialize nested index bulkload */
        nestedIndexBlkLdId = SM_InitIndexBulkLoad(handle, tmpVolId, &auxColInfo->kdesc);
        if (nestedIndexBlkLdId < 0) ERR(handle, nestedIndexBlkLdId);


        /* set slot information */
        orderedSetColHdr.nSlots = orderedSetColHdr.nElements + 1;
        orderedSetColHdr.freeSlotListHdr = NO_MORE_SLOT;

        /* calculate 'slotArraySize' */
        slotArraySize = orderedSetColHdr.nSlots * sizeof(OrderedSet_ElementOffset_T);

        /* initialize slotNo & elementOffset */
        /*
         * The first slot(0-th slot) is not used to avoid confusion of two zeros
         * in the slot array: one represents offset 0, and the other 0-th slot in
         * the free slot list.
         */
        slotNo = 1;
        elementOffset = 0;

        /* allocate memory for slot array */
        slotArray = (OrderedSet_ElementOffset_T *) malloc(slotArraySize);
        slotArray[0] = UNUSED_SLOT;

        /* initialize 'orderedSetColLen' */
        /* Note!! we must consider slot array */
        orderedSetColLen = HDR_SIZE_OF_ORDEREDSET_WITH_NESTED_INDEX + slotArraySize;

        /* calculate 'orderedSetHdrLen' */
        orderedSetHdrLen = HDR_SIZE_OF_ORDEREDSET_WITH_NESTED_INDEX;


        /* update 'nElements' */
        orderedSetColHdr.nElements *= -1;
    }
    /* III-2. if nested index(sub-index) isn't needed */
    else {

       /* initialize 'orderedSetColLen' */
       orderedSetColLen = HDR_SIZE_OF_ORDEREDSET_WITHOUT_NESTED_INDEX;

       /* calculate 'orderedSetHdrLen' */
       orderedSetHdrLen = HDR_SIZE_OF_ORDEREDSET_WITHOUT_NESTED_INDEX;
    }


    /*
     *  IV. Append header of ordered set into ordered set column
     */

    /* if tuple buffer is overflowed, flush out tuple buffer */
    if (blkLdEntry->lrdsBlkLdTupBufOffset + orderedSetHdrLen > SIZE_OF_LRDS_TUPLE_BUFFER) {

        /*** Flush ***/
        e = SM_NextDataFileBulkLoad(handle, blkLdEntry->smDataFileBlkLdId,
                                    blkLdEntry->lrdsBlkLdTupBuf, blkLdEntry->lrdsBlkLdTupBufOffset, FALSE, NULL);
        if (e < eNOERROR) ERR(handle, e);

        /*** Reset ***/
        blkLdEntry->lrdsBlkLdTupBufOffset = 0;
        blkLdEntry->lrdsBlkLdIsFirstTupBuf = FALSE;
    }

    /* append ordered set header to tuple */
    memcpy(&blkLdEntry->lrdsBlkLdTupBuf[blkLdEntry->lrdsBlkLdTupBufOffset], &orderedSetColHdr, orderedSetHdrLen);

    /* update 'lrdsBlkLdTupBufOffset' */
    blkLdEntry->lrdsBlkLdTupBufOffset += orderedSetHdrLen;


    /*
     *  V. Append dummy slot array into tuple if needed
     *     Note!! slot array size can be exceed to buffer size
     */

    if (needNestedIndex) {

        /* initialize 'remaindedLen' */
        remainedLen = slotArraySize;

        while (blkLdEntry->lrdsBlkLdTupBufOffset + remainedLen > SIZE_OF_LRDS_TUPLE_BUFFER) {

            /* calculate available space */
            nowInBufLen = SIZE_OF_LRDS_TUPLE_BUFFER - blkLdEntry->lrdsBlkLdTupBufOffset;

            /* update 'remainedLen' & 'lrdsBlkLdTupBufOffset' */
            remainedLen -= nowInBufLen;
            blkLdEntry->lrdsBlkLdTupBufOffset += nowInBufLen;

            /*** Flush ***/
            e = SM_NextDataFileBulkLoad(handle, blkLdEntry->smDataFileBlkLdId,
                                        blkLdEntry->lrdsBlkLdTupBuf, blkLdEntry->lrdsBlkLdTupBufOffset, FALSE, NULL);
            if (e < eNOERROR) ERR(handle, e);

            /*** Reset ***/
            blkLdEntry->lrdsBlkLdTupBufOffset = 0;
            blkLdEntry->lrdsBlkLdIsFirstTupBuf = FALSE;
        }

        /* update 'lrdsBlkLdTupBufOffset' */
        blkLdEntry->lrdsBlkLdTupBufOffset += remainedLen;
    }


    /*
     *  VI-1. For each elements in input buffer, append element ordered offset into ordered set column
     */

    /* initialize ptr */
    ptr = elementsBuf;

    /* initialize totalElementLength */
    totalElementLength = 0;

    /* for each element in input buffer, append into ordered set */
    for (i = 0; i < nElements; i++ ) {

        /* if tuple buffer is overflowed, flush out tuple buffer */
        if (blkLdEntry->lrdsBlkLdTupBufOffset + sizeof(OrderedSet_ElementLength) > SIZE_OF_LRDS_TUPLE_BUFFER) {

            /*** Flush ***/
            e = SM_NextDataFileBulkLoad(handle, blkLdEntry->smDataFileBlkLdId,
                                        blkLdEntry->lrdsBlkLdTupBuf, blkLdEntry->lrdsBlkLdTupBufOffset, FALSE, NULL);
            if (e < eNOERROR) ERR(handle, e);

            /*** Reset ***/
            blkLdEntry->lrdsBlkLdTupBufOffset = 0;
            blkLdEntry->lrdsBlkLdIsFirstTupBuf = FALSE;
       }

       /*
        * write an element offset to the lrds bulkload tuple buffer
        */
       memcpy(&blkLdEntry->lrdsBlkLdTupBuf[blkLdEntry->lrdsBlkLdTupBufOffset],
	      &totalElementLength,
              sizeof(OrderedSet_ElementLength));

       /* update 'lrdsBlkLdTupBufOffset' */
       blkLdEntry->lrdsBlkLdTupBufOffset += sizeof(OrderedSet_ElementLength);

       /*
        *  get length of ith element
        */
       memcpy(&elementLen, ptr, sizeof(OrderedSet_ElementLength));
       length = elementLen + sizeof(OrderedSet_ElementLength);
       totalElementLength += elementLen;

       /*
        *  update ptr
        */
       ptr += length;
    }


    /*
     *  VI-2. For each elements in input buffer, append element into ordered set column
     */

    /* initialize ptr */
    ptr = elementsBuf;

    /* calculate 'keyLen' */
    for (i = 0, keyLen = 0; i < auxColInfo->kdesc.nparts; i++ ) {
        keyLen += auxColInfo->kdesc.kpart[i].length;
    }

    /* for each element in input buffer, append into ordered set */
    for (i = 0; i < nElements; i++ ) {

        /*
         *  get length of ith element
         */
        memcpy(&elementLen, ptr, sizeof(OrderedSet_ElementLength));
        length = elementLen + sizeof(OrderedSet_ElementLength);

#ifdef COMPRESSION
        memcpy(&elementLenOfUncompressed, uncompressedElementsBuf, sizeof(OrderedSet_ElementLength));
        lengthOfUncompressed = elementLenOfUncompressed + sizeof(OrderedSet_ElementLength);
#endif        

        /*
         *  Append element into ordered set
         */

        /* if tuple buffer is overflowed, flush out tuple buffer */
        if (blkLdEntry->lrdsBlkLdTupBufOffset + elementLen > SIZE_OF_LRDS_TUPLE_BUFFER) {

            /*** Flush ***/
            e = SM_NextDataFileBulkLoad(handle, blkLdEntry->smDataFileBlkLdId,
                                        blkLdEntry->lrdsBlkLdTupBuf, blkLdEntry->lrdsBlkLdTupBufOffset, FALSE, NULL);
            if (e < eNOERROR) ERR(handle, e);

            /*** Reset ***/
            blkLdEntry->lrdsBlkLdTupBufOffset = 0;
            blkLdEntry->lrdsBlkLdIsFirstTupBuf = FALSE;
        }

        /* append the element of the ordered set */
        /* 만약 elementLen가 SIZE_OF_LRDS_TUPLE_BUFFER 보다 크다면, memory 에러가 발생한다.
           그러므로 SIZE_OF_LRDS_TUPLE_BUFFER로 잘라서 객체를 나누어서 넣는다. */
		/* If the elementLen is larger than SIZE_OF_LRDS_TUPLE_BUFFER, memory error is occurred.
		   So divide the object into several objects whose size is SIZE_OF_LRDS_TUPLE_BUFFER and then, insert that. */
        if(elementLen > SIZE_OF_LRDS_TUPLE_BUFFER)
        {
            Four   j;
            char*  p;

            p = &ptr[sizeof(OrderedSet_ElementLength)];
            for(j = 0; j < elementLen / SIZE_OF_LRDS_TUPLE_BUFFER; j++)
            {
                e = SM_NextDataFileBulkLoad(handle, blkLdEntry->smDataFileBlkLdId,
					    p, SIZE_OF_LRDS_TUPLE_BUFFER, FALSE, NULL);
                if (e < eNOERROR) ERR(handle, e);
                p += SIZE_OF_LRDS_TUPLE_BUFFER;
            }
            if(elementLen % SIZE_OF_LRDS_TUPLE_BUFFER)
            {
                e = SM_NextDataFileBulkLoad(handle, blkLdEntry->smDataFileBlkLdId,
                                            p, elementLen % SIZE_OF_LRDS_TUPLE_BUFFER,
                                            FALSE, NULL);
                if (e < eNOERROR) ERR(handle, e);
            }
        }
        else
        {
            memcpy(&blkLdEntry->lrdsBlkLdTupBuf[blkLdEntry->lrdsBlkLdTupBufOffset], &ptr[sizeof(OrderedSet_ElementLength)], elementLen);
            /* update 'lrdsBlkLdTupBufOffset' */
            blkLdEntry->lrdsBlkLdTupBufOffset += elementLen;
        }

        /* update 'orderedSetColLen' */
        orderedSetColLen += length;


        /*
         *  insert corresponding entry into nested index & slot array
         */
        if (needNestedIndex) {

            /*
             *  Extract key from element and insert into nested index by bulkload
             */

            /* construct key */
            kval.len = keyLen;
            
#ifndef COMPRESSION
            memcpy(kval.val, ptr+sizeof(OrderedSet_ElementLength), keyLen);
#else
            memcpy(kval.val, uncompressedElementsBuf+sizeof(OrderedSet_ElementLength), keyLen);
#endif            

            /* construct oid */
            oid.volNo = NIL;
            oid.pageNo = slotNo;
            oid.slotNo = NIL;
            oid.unique = 0;

            /* insert into sort stream for nested index bulkload */
            e = SM_NextIndexBulkLoad(handle, nestedIndexBlkLdId, &kval, &oid);
            if (e < eNOERROR) ERR(handle, e);


            /*
             *  insert corresponding entry into slot array for input element
             */

            /* set corresponding entry of slot array */
            slotArray[slotNo] = elementOffset;


            /*
             *  update 'slotNo' & 'elementOffset'
             */
            slotNo ++;
            elementOffset += sizeof(OrderedSet_ElementLength);

        } /* if */


        /*
         *  update ptr
         */
        ptr += length;

#ifdef COMPRESSION
        uncompressedElementsBuf += lengthOfUncompressed;
#endif

    } /* for */

    /* assertion check */
    assert(!needNestedIndex || slotNo == orderedSetColHdr.nElements || slotNo == -orderedSetColHdr.nElements);


    /*
     *  VII. finalize nested index bulkload and insert slot array into tuple
     */

    /* finalize nested index bulkload and insert slot array into tuple */
    if (needNestedIndex) {

        /*
         * finalize nested index bulkload
         */
        e = SM_FinalIndexBulkLoad(handle, nestedIndexBlkLdId, &orderedSetColHdr.nestedIndexId, 100, 100, &blkLdEntry->lockup);
        if (e < eNOERROR) ERR(handle, e);


        /*
         *  insert slot array
         */

        /* assertion check */
        /* Note!! nested index isn't needed for small object */
        assert(!blkLdEntry->lrdsBlkLdIsFirstTupBuf);

        /* set varColumnNo for fast access */
        varColumnNo = complexTypeColDesc->varColNo;

        /* calculate 'slotArrayOffset' */
        slotArrayOffset = (varColumnNo == 0) ?
                          blkLdEntry->lrdsBlkLdTupHdrPtr->firstVarColOffset + orderedSetHdrLen :
                          blkLdEntry->lrdsBlkLdTupHdrPtr->varColOffset[varColumnNo-1] + orderedSetHdrLen;

        /* insert slot array to tuple */
        e = SM_NextDataFileBulkLoadWriteLOT(handle, blkLdEntry->smDataFileBlkLdId,
                                            slotArrayOffset, slotArraySize, (char*) slotArray, FALSE, NULL);
        if (e < eNOERROR) ERR(handle, e);


        /*
         * finalize slot array for ordered set bulkload
         */
        free(slotArray);

    } /* if */


    /*
     *  VIII. set varColOffset
     */

    /* set varColumnNo for fast access */
    varColumnNo = complexTypeColDesc->varColNo;

    /* set 'varColOffset' */
    blkLdEntry->lrdsBlkLdTupHdrPtr->varColOffset[varColumnNo] = (varColumnNo == 0) ?
                                                    blkLdEntry->lrdsBlkLdTupHdrPtr->firstVarColOffset + orderedSetColLen :
                                                    blkLdEntry->lrdsBlkLdTupHdrPtr->varColOffset[varColumnNo-1] + orderedSetColLen;


    /*
     *  IX. if end of tuple, flush out tuple header
     */
    if (endOfTuple == TRUE) {
        e = lrds_NextRelationBulkLoad_FlushTuple(handle, blkLdEntry, tid);
        if (e < eNOERROR) ERR(handle, e);
    } /* if */


    return(eNOERROR);

} /* LRDS_NextRelationBulkLoad_OrderedSetBulkLoad() */

#else

/*
 * LRDS Ordered Set Only Forward Scan APIs
 */


#define HDR_SIZE_OF_ORDEREDSET_WITHOUT_NESTED_INDEX  OFFSET_OF(OrderedSetColHdr_T,nestedIndexId)
#define HDR_SIZE_OF_ORDEREDSET_WITH_NESTED_INDEX  sizeof(OrderedSetColHdr_T)
#define IS_LONG_ORDEREDSET(_colSize) (_colSize > (3*TRAINSIZE))

typedef Four OrderedSet_ElementOffset_T;
#define NO_MORE_SLOT (LONG_MIN+1)
#define UNUSED_SLOT  LONG_MIN


/* internal function prototype */
Four lrds_NextRelationBulkLoad_FlushTuple(Four, LRDS_BlkLdTableEntry*, TupleID*);


/*@================================================
 *  LRDS_NextRelationBulkLoad_OrderedSetBulkLoad()
 * ===============================================*/

/*
 * Function : Four LRDS_NextRelationBulkLoad_OrderedSetBulkLoad()
 *
 * Description :
 *
 * Return Values :
 *  error code.
 *
 * Side Effects :
 */
Four LRDS_NextRelationBulkLoad_OrderedSetBulkLoad(
    Four               		handle,              /* IN handle */
    Four               		tmpVolId,            /* IN temporary volume in which sort stream is created */
    Four               		blkLdId,             /* IN bulkload ID */
    Four               		colNo,               /* IN column number of ordered set type */
    Four               		nElements,           /* IN # of elements to append */
    Four               		elementsBufSize,     /* IN buffer size */
    char*              		elementsBuf,         /* IN elements to append */
    Boolean            		endOfTuple,          /* IN flag which indicates this ordered set is end of tuple or not */
#ifndef COMPRESSION
    TupleID*           		tid)                 /* OUT tuple ID */
#else
    TupleID*           		tid,                 /* OUT tuple ID */
    char*              		buffer,              
    VolNo              		volNoOfPostingTupleID,
    Four               		lastDocId)      
#endif    
{
    Four               		e;                   /* error code */
    Four               		i;                   /* index variable */
    Four               		slotNo;              /* slot number of each element */
    char*              		ptr;                 /* pointer which points element in elementsBuf */
    Four               		length;              /* length of element including length field */
    Four               		varColumnNo;         /* column number of variable column */
    Boolean            		needNestedIndex;     /* flag which indicates nested index is needed */
    Four               		orderedSetHdrLen;    /* length of ordered set's header */
    Four               		orderedSetColLen;    /* length of ordered set column */
    OrderedSetColHdr_T 		orderedSetColHdr;    /* header of ordered set */
    Four               		slotArraySize;       /* size of slot array for nested index */
    Four               		slotArrayOffset;     /* offset of slot array for nested index in the tuple */
    Four              		keyLen;              /* length of key in each element */
    KeyValue           		kval;                /* key value of each element */
    ObjectID           		oid;                 /* oid which contains slotNo of each element */
    Four               		nestedIndexBlkLdId;  /* sort stream ID for nested index bulkload */
    Four               		remainedLen;         /* remained length of slotArray */
    Four               		nowInBufLen;         /* length of the slotArray in tuple buffer */
    ColDesc*           		complexTypeColDesc;  /* complex type column descriptor */
    OrderedSetAuxColInfo_T*     auxColInfo;          /* auxiliary column information for ordered set */
    LRDS_BlkLdTableEntry*       blkLdEntry;          /* bulkload table entry pointer */
    OrderedSet_ElementOffset_T* slotArray;           /* slot array */
    OrderedSet_ElementLength    elementLen;          /* length of element in ordered set */
    OrderedSet_ElementOffset_T  elementOffset;       /* offset of element in ordered set */
    ColDesc 			*blkLdRelTableEntry_cdesc;
#ifdef COMPRESSION
    Four               		lengthOfUncompressed;
    OrderedSet_ElementLength    elementLenOfUncompressed;
#endif


    /*
     *  parameter check
     */
    if (elementsBufSize <= 0 || elementsBuf == NULL || nElements <= 0) ERR(handle, eBADPARAMETER);


    /*
     *  O. Set entry for fast access
     */
    blkLdEntry = &LRDS_BLKLD_TABLE(handle)[blkLdId];
    blkLdRelTableEntry_cdesc = PHYSICAL_PTR(blkLdEntry->lrdsBlkLdRelTableEntry->cdesc);


    /*
     *  I. Update global variables for data file bulkload
     */

    /* error check */
    /* Note!! this function must be called only once */
    if (colNo <= blkLdEntry->lrdsBlkLdRemainFlagArrayIdxForVarCols) ERR(handle, eINTERNAL);

    /* update 'lrdsBlkLdRemainFlagArrayIdxForVarCols' */
    blkLdEntry->lrdsBlkLdRemainFlagArrayIdxForVarCols = colNo;

    /* set corresponding entry of 'lrdsBlkLdRemainFlagArray' as FALSE */
    blkLdEntry->lrdsBlkLdRemainFlagArray[colNo] = FALSE;

    /* reset the corresponding bit of null vector. */
    BITRESET(blkLdEntry->lrdsBlkLdNullVector, colNo);


    /*
     *  II. Set complex type column description for fast access
     */

    /* set complexTypeColDesc */
    complexTypeColDesc = &blkLdRelTableEntry_cdesc[colNo];

    /* error check */
    if (complexTypeColDesc->complexType != SM_COMPLEXTYPE_ORDEREDSET) ERR(handle, eBADPARAMETER);

    /* assertion check */
    assert(complexTypeColDesc->varColNo != NIL);


    /*
     *  III. Construct header of ordered set column
     */

    /* set 'auxColInfo' for fast access */
    auxColInfo = (OrderedSetAuxColInfo_T *) PHYSICAL_PTR(complexTypeColDesc->auxInfo);

    /* set 'nElements' */
    orderedSetColHdr.nElements = nElements;

#ifdef COMPRESSION
    /* set 'lastDocId' */
    orderedSetColHdr.volNo      = volNoOfPostingTupleID;
    orderedSetColHdr.lastDocId  = lastDocId;
#endif

    /* set 'needNestedIndex' */
    needNestedIndex = IS_LONG_ORDEREDSET(elementsBufSize) && auxColInfo->nestedIndexFlag;

    /* III-1. if nested index(sub-index) is needed */
    if (needNestedIndex) {

#ifndef NDEBUG
        printf("NestedIndex Created!! \n");
#endif

        /* Create nested index */
        e = SM_AddIndex(handle, &blkLdEntry->lrdsBlkLdRelTableEntry->ri.fid, &orderedSetColHdr.nestedIndexId, NULL);
        if (e < eNOERROR) ERR(handle, e);


        /* Initialize nested index bulkload */
        nestedIndexBlkLdId = SM_InitIndexBulkLoad(handle, tmpVolId, &auxColInfo->kdesc);
        if (nestedIndexBlkLdId < 0) ERR(handle, nestedIndexBlkLdId);

        /* set slot information */
        /*
         * The first slot(0-th slot) is not used to avoid confusion of two zeros
         * in the slot array: one represents offset 0, and the other 0-th slot in
         * the free slot list.
         */
        orderedSetColHdr.nSlots = orderedSetColHdr.nElements + 1;
        orderedSetColHdr.freeSlotListHdr = NO_MORE_SLOT;

        /* calculate 'slotArraySize' */
        slotArraySize = orderedSetColHdr.nSlots * sizeof(OrderedSet_ElementOffset_T);

        /* initialize slotNo & elementOffset */
        slotNo = 1;
        elementOffset = 0;

        /* allocate memory for slot array */
        slotArray = (OrderedSet_ElementOffset_T *) malloc(slotArraySize);

	/* initialize first slot array value */
	slotArray[0] = UNUSED_SLOT;


        /* initialize 'orderedSetColLen' */
        /* Note!! we must consider slot array */
        orderedSetColLen = HDR_SIZE_OF_ORDEREDSET_WITH_NESTED_INDEX + slotArraySize;

        /* calculate 'orderedSetHdrLen' */
        orderedSetHdrLen = HDR_SIZE_OF_ORDEREDSET_WITH_NESTED_INDEX;


            /* update 'nElements' */
        orderedSetColHdr.nElements *= -1;
    }
    /* III-2. if nested index(sub-index) isn't needed */
    else {

       /* initialize 'orderedSetColLen' */
       orderedSetColLen = HDR_SIZE_OF_ORDEREDSET_WITHOUT_NESTED_INDEX;

       /* calculate 'orderedSetHdrLen' */
       orderedSetHdrLen = HDR_SIZE_OF_ORDEREDSET_WITHOUT_NESTED_INDEX;
    }


    /*
     *  IV. Append header of ordered set into ordered set column
     */

    /* if tuple buffer is overflowed, flush out tuple buffer */
    if (blkLdEntry->lrdsBlkLdTupBufOffset + orderedSetHdrLen > SIZE_OF_LRDS_TUPLE_BUFFER) {

        /*** Flush ***/
        e = SM_NextDataFileBulkLoad(handle, blkLdEntry->smDataFileBlkLdId, blkLdEntry->lrdsBlkLdTupBuf, blkLdEntry->lrdsBlkLdTupBufOffset, FALSE, NULL);
        if (e < eNOERROR) ERR(handle, e);

        /*** Reset ***/
        blkLdEntry->lrdsBlkLdTupBufOffset = 0;
        blkLdEntry->lrdsBlkLdIsFirstTupBuf = FALSE;
    }

    /* append ordered set header to tuple */
    memcpy(&blkLdEntry->lrdsBlkLdTupBuf[blkLdEntry->lrdsBlkLdTupBufOffset], &orderedSetColHdr, orderedSetHdrLen);

    /* update 'lrdsBlkLdTupBufOffset' */
    blkLdEntry->lrdsBlkLdTupBufOffset += orderedSetHdrLen;


    /*
     *  V. Append dummy slot array into tuple if needed
     *     Note!! slot array size can be exceed to buffer size
     */

    if (needNestedIndex) {

        /* initialize 'remaindedLen' */
        remainedLen = slotArraySize;

        while (blkLdEntry->lrdsBlkLdTupBufOffset + remainedLen > SIZE_OF_LRDS_TUPLE_BUFFER) {

            /* calculate available space */
            nowInBufLen = SIZE_OF_LRDS_TUPLE_BUFFER - blkLdEntry->lrdsBlkLdTupBufOffset;

            /* update 'remainedLen' & 'lrdsBlkLdTupBufOffset' */
            remainedLen -= nowInBufLen;
            blkLdEntry->lrdsBlkLdTupBufOffset += nowInBufLen;

            /*** Flush ***/
            e = SM_NextDataFileBulkLoad(handle, blkLdEntry->smDataFileBlkLdId, blkLdEntry->lrdsBlkLdTupBuf, blkLdEntry->lrdsBlkLdTupBufOffset, FALSE, NULL);
            if (e < eNOERROR) ERR(handle, e);

            /*** Reset ***/
            blkLdEntry->lrdsBlkLdTupBufOffset = 0;
            blkLdEntry->lrdsBlkLdIsFirstTupBuf = FALSE;
        }

        /* update 'lrdsBlkLdTupBufOffset' */
        blkLdEntry->lrdsBlkLdTupBufOffset += remainedLen;
    }


    /*
     *  VI. For each elements in input buffer, append it into ordered set column
     */

    /* initialize ptr */
    ptr = elementsBuf;

    /* calculate 'keyLen' */
    for (i = 0, keyLen = 0; i < auxColInfo->kdesc.nparts; i++ ) {
        keyLen += auxColInfo->kdesc.kpart[i].length;
    }

    /* for each element in input buffer, append into ordered set */
    for (i = 0; i < nElements; i++ ) {

        /*
         *  get length of ith element
         */
        memcpy(&elementLen, ptr, sizeof(OrderedSet_ElementLength));
        length = elementLen + sizeof(OrderedSet_ElementLength);

#ifdef COMPRESSION
        memcpy(&elementLenOfUncompressed, uncompressedElementsBuf, sizeof(OrderedSet_ElementLength));
        lengthOfUncompressed = elementLenOfUncompressed + sizeof(OrderedSet_ElementLength);
#endif        

        /*
         *  Append element into ordered set
         */

        /* if tuple buffer is overflowed, flush out tuple buffer */
        if (blkLdEntry->lrdsBlkLdTupBufOffset + length > SIZE_OF_LRDS_TUPLE_BUFFER) {

            /*** Flush ***/
            e = SM_NextDataFileBulkLoad(handle, blkLdEntry->smDataFileBlkLdId, blkLdEntry->lrdsBlkLdTupBuf, blkLdEntry->lrdsBlkLdTupBufOffset, FALSE, NULL);
            if (e < eNOERROR) ERR(handle, e);

            /*** Reset ***/
            blkLdEntry->lrdsBlkLdTupBufOffset = 0;
            blkLdEntry->lrdsBlkLdIsFirstTupBuf = FALSE;
        }

        /* append the element of the ordered set */
        /* 만약 length가 SIZE_OF_LRDS_TUPLE_BUFFER 보다 크다면, memory 에러가 발생한다.
           그러므로 SIZE_OF_LRDS_TUPLE_BUFFER로 잘라서 객체를 나누어서 넣는다. */
		/* If the elementLen is larger than SIZE_OF_LRDS_TUPLE_BUFFER, memory error is occurred.
		   So divide the object into several objects whose size is SIZE_OF_LRDS_TUPLE_BUFFER and then, insert that. */
        if(length > SIZE_OF_LRDS_TUPLE_BUFFER)
        {
	    /* 만약 length가 SIZE_OF_LRDS_TUPLE_BUFFER 보다 크다면,
	       위에서 Flush가 수행되고, blkLdEntry->lrdsBlkLdTupBufOffset의 값은 0이 된다. */
	    /* If the elementLen is larger than SIZE_OF_LRDS_TUPLE_BUFFER, 
		   flush is executed above, and the value of blkLdEntry->lrdsBlkLdTupBufOffset becomes to be 0. */

            Four   j;
            char*  p;

            p = ptr;
            for(j = 0; j < length / SIZE_OF_LRDS_TUPLE_BUFFER; j++)
            {
                e = SM_NextDataFileBulkLoad(handle, blkLdEntry->smDataFileBlkLdId,
					    p, SIZE_OF_LRDS_TUPLE_BUFFER, FALSE, NULL);
                if (e < eNOERROR) ERR(handle, e);
                p += SIZE_OF_LRDS_TUPLE_BUFFER;
            }
            if(length % SIZE_OF_LRDS_TUPLE_BUFFER)
            {
                e = SM_NextDataFileBulkLoad(handle, blkLdEntry->smDataFileBlkLdId,
					    p, length % SIZE_OF_LRDS_TUPLE_BUFFER, FALSE, NULL);
                if (e < eNOERROR) ERR(handle, e);
            }
        }
        else
        {
            memcpy(&blkLdEntry->lrdsBlkLdTupBuf[blkLdEntry->lrdsBlkLdTupBufOffset], ptr, length);

            /* update 'lrdsBlkLdTupBufOffset' */
            blkLdEntry->lrdsBlkLdTupBufOffset += length;
        }

        /* update 'orderedSetColLen' */
        orderedSetColLen += length;


        /*
         *  insert corresponding entry into nested index & slot array
         */
        if (needNestedIndex) {

            /*
             *  Extract key from element and insert into nested index by bulkload
             */

            /* construct key */
            kval.len = keyLen;

#ifndef COMPRESSION
            memcpy(kval.val, ptr+sizeof(OrderedSet_ElementLength), keyLen);
#else
            memcpy(kval.val, uncompressedElementsBuf+sizeof(OrderedSet_ElementLength), keyLen);
#endif            

            /* construct oid */
            oid.volNo = NIL;
            oid.pageNo = slotNo;
            oid.slotNo = NIL;
            oid.unique = 0;

            /* insert into sort stream for nested index bulkload */
            e = SM_NextIndexBulkLoad(handle, nestedIndexBlkLdId, &kval, &oid);
            if (e < eNOERROR) ERR(handle, e);


            /*
             *  insert corresponding entry into slot array for input element
             */

            /* set corresponding entry of slot array */
            slotArray[slotNo] = elementOffset;


            /*
             *  update 'slotNo' & 'elementOffset'
             */
            slotNo ++;
            elementOffset += length;

        } /* if */


        /*
         *  update ptr
         */
        ptr += length;

#ifdef COMPRESSION
        uncompressedElementsBuf += lengthOfUncompressed;
#endif

    } /* for */

    /* assertion check */
    assert(!needNestedIndex || slotNo == orderedSetColHdr.nElements || slotNo == -orderedSetColHdr.nElements);


    /*
     *  VII. finalize nested index bulkload and insert slot array into tuple
     */

    /* finalize nested index bulkload and insert slot array into tuple */
    if (needNestedIndex) {

        /*
         * finalize nested index bulkload
         */
        e = SM_FinalIndexBulkLoad(handle, nestedIndexBlkLdId, &orderedSetColHdr.nestedIndexId, 100, 100, &blkLdEntry->lockup);
        if (e < eNOERROR) ERR(handle, e);


        /*
         *  insert slot array
         */

        /* assertion check */
        /* Note!! nested index isn't needed for small object */
        assert(!blkLdEntry->lrdsBlkLdIsFirstTupBuf);

        /* set varColumnNo for fast access */
        varColumnNo = complexTypeColDesc->varColNo;

        /* calculate 'slotArrayOffset' */
        slotArrayOffset = (varColumnNo == 0) ?
                          blkLdEntry->lrdsBlkLdTupHdrPtr->firstVarColOffset + orderedSetHdrLen :
                          blkLdEntry->lrdsBlkLdTupHdrPtr->varColOffset[varColumnNo-1] + orderedSetHdrLen;

        /* insert slot array to tuple */
        e = SM_NextDataFileBulkLoadWriteLOT(handle, blkLdEntry->smDataFileBlkLdId, slotArrayOffset, slotArraySize, (char*) slotArray, FALSE, NULL);
        if (e < eNOERROR) ERR(handle, e);


        /*
         * finalize slot array for ordered set bulkload
         */
        free(slotArray);

    } /* if */


    /*
     *  VIII. set varColOffset
     */

    /* set varColumnNo for fast access */
    varColumnNo = complexTypeColDesc->varColNo;

    /* set 'varColOffset' */
    blkLdEntry->lrdsBlkLdTupHdrPtr->varColOffset[varColumnNo] = (varColumnNo == 0) ?
                                                    blkLdEntry->lrdsBlkLdTupHdrPtr->firstVarColOffset + orderedSetColLen :
                                                    blkLdEntry->lrdsBlkLdTupHdrPtr->varColOffset[varColumnNo-1] + orderedSetColLen;


    /*
     *  IX. if end of tuple, flush out tuple header
     */
    if (endOfTuple == TRUE) {
        e = lrds_NextRelationBulkLoad_FlushTuple(handle, blkLdEntry, tid);
        if (e < eNOERROR) ERR(handle, e);
    } /* if */


    return(eNOERROR);

} /* LRDS_NextRelationBulkLoad_OrderedSetBulkLoad() */
#endif
