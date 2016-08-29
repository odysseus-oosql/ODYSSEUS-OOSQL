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
/*    ODYSSEUS/OOSQL DB-IR-Spatial Tightly-Integrated DBMS                    */
/*    Version 5.0                                                             */
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

#ifndef SLIMDOWN_TEXTIR

#include <malloc.h>
#include <string.h>
#include <stdlib.h>
#include <signal.h>
#ifndef WIN32   
#include <sys/wait.h>
#endif
#include "LOM_Internal.h"
#include "LOM_Param.h"
#include "LOM.h"
#include "Catalog.h"
#include "Catalog_Internal.h"
#ifdef WIN32
#include <windows.h>
#endif

static Four lom_Text_IsKeywordExistInInvertedIndex(
    LOM_Handle*     handle,                     /* IN  LOM system handle */
    Four            ocnOrScanId,                /* IN: text open scan number */
    Boolean         useScanFlag,                /* IN: flag */
    Two             colNo,                      /* IN: column number */
    char*           keyword                     /* IN  keyword to check */
)
{
    Four            e;
    IndexID         iid;
    LockParameter   lockup;     /* lock parameter */
    BoundCond       bound;
    Two             keyLen;
    Four            eos;
    Four            lrdsScanNum;
    Four            orn;
    TupleID         tid;

    /* open relation number for inverted index table */
    orn = lom_Text_GetInvertedIndexTableORN(handle, ocnOrScanId, useScanFlag, colNo);
    if(orn < 0) LOM_ERROR(handle, eBADPARAMETER_LOM);

    /* get index id */
    e = lom_Text_GetKeywordIndex(handle, ocnOrScanId, useScanFlag, colNo, &iid);
    if(e < 0) LOM_ERROR(handle, eBADPARAMETER_LOM);

    bound.op = SM_EQ;
    keyLen = strlen(keyword);
    bound.key.len = sizeof(Two) + keyLen;
    bcopy(&keyLen,&(bound.key.val[0]),sizeof(Two));
    bcopy(keyword,&(bound.key.val[sizeof(Two)]),keyLen);

    lockup.mode = L_IS;
    lockup.duration = L_COMMIT;
    lrdsScanNum = LRDS_OpenIndexScan(LOM_GET_LRDS_HANDLE(handle), orn, &iid, &bound, &bound, 0, NULL, &lockup);
    if( lrdsScanNum < eNOERROR) LOM_ERROR(handle, lrdsScanNum);

    eos = e = LRDS_NextTuple(LOM_GET_LRDS_HANDLE(handle), lrdsScanNum, &tid, NULL);
    if(e < eNOERROR) LOM_ERROR(handle, e);

    e = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), lrdsScanNum);
    if(e < eNOERROR) LOM_ERROR(handle, e);

    if(eos == EOS)
        return SM_FALSE;
    else
        return SM_TRUE;
}

/************************************/
/* DocumentID-index Interface       */
/************************************/
/*
 * Function: lom_Text_AddInvertedIndexEntryFromBuf(handle, Four, char *, Four, lom_PostingBuffer *)
 *
 * Description:
 *  add inverted index entry into inverted-index table from posting buffer
 *
 * Retuns:
 *  error code
 */
Four lom_Text_AddInvertedIndexEntryFromBuf(
    LOM_Handle*     handle, 
    Four            temporaryVolId,             /* IN: id for temporary volume used in sorting and etc. */
    Four            ocnOrScanId,                /* IN: text open scan number */
    Boolean         useScanFlag,                /* IN: flag */
    Four            lrdsBulkLoadId,             /* IN: lrds level bulk load id */
    Four            lrdsTextBulkLoadId,         /* IN: lrds level text bulk load id */
    Boolean         useBulkLoadFlag,            /* IN: use bulk loading feature */
    Two             colNo,                      /* IN: column number */
    char*           keyword,                    /* IN: keyword */
#ifdef COMPRESSION
    lom_Text_PostingInfoForReading* postingInfo,        /* IN: posting info */
    char*			ptrToCompressedPostingList,			/* IN: pointer to posting list */
#endif
    Four            nPostings,                  /* IN: number of postings */
    Four            lengthOfPostingList,        /* IN: length of posting-list */
    char*           ptrToPostingList,           /* IN: pointer to posting list */
    TupleID*        tid,                        /* OUT: tid for index entry */
    Boolean         isLogicalIdOrder,           /* IN: isLogicalIdOrder : 주어진 데이타를 logical id 순이 되도록 한다. */
    Boolean         buildReverseKeywordIndex,   /* IN: build index for reverse keyword in this function */
    Boolean*        newlyRegisteredKeyword      /* OUT: is keyword a newly registered? */
)
{
    Four e; /* error code */
    IndexID iid;
    char reverseKeyword[LOM_MAXKEYWORDSIZE + 1 + sizeof(Two)];
    Four orn;
    LockParameter lockup; /* lock parameter */
    BoundCond bound;
    Two keyLen;
    ColListStruct clist[5];
    Four count;
    Two length;
    char *posting;
    LRDS_Cursor *cursor;
    Four lrdsScanNum;
    Four offset;
    Four nPositions;
    Four e2;
    Four logicalDocId;          /* logical doc-id */
    lom_PostingBuffer postingBuffer;
    Four offsetInBuffer;
    Four nPostingsInIndex;
    Four currentPostingLogicalId;
#ifdef TRACE
    Four sentenceNum;
    Two wordNum;
#endif
    Four postingLengthFieldSize = LOM_LONG_SIZE_VAR;
    Four postingLength;
    Four ocn;
    LockParameter lockupForBulkloading;
#ifdef COMPRESSION
	VolNo   volNoOfPostingTupleID;
	Four    lastDocId;
	FILE    *fp;
#endif

    /* get logical Id */
    bcopy(&ptrToPostingList[postingLengthFieldSize], &logicalDocId, sizeof(Four));

    /* get open class number of relation */
    if(useScanFlag)
        ocn = LRDS_SCANTABLE(LOM_GET_LRDS_HANDLE(handle))[LOM_SCANTABLE(handle)[ocnOrScanId].lrdsScanId].orn;
    else
        ocn = ocnOrScanId;

    /* open relation number for inverted index table */
    orn = lom_Text_GetInvertedIndexTableORN(handle, ocnOrScanId, useScanFlag, colNo);
    if(orn < 0) LOM_ERROR(handle, eBADPARAMETER_LOM);

    /* get index id */
    e = lom_Text_GetKeywordIndex(handle, ocnOrScanId, useScanFlag, colNo, &iid);
    if(e < 0) LOM_ERROR(handle, eBADPARAMETER_LOM);

    /* set the lockup parameter */
    if(useScanFlag) {
        lockup.duration = LOM_SCANTABLE(handle)[ocnOrScanId].lockup.duration;
        lockup.mode = LOM_SCANTABLE(handle)[ocnOrScanId].lockup.mode;
    }
    else {
        lockup.duration = L_COMMIT;
        lockup.mode = L_IX;
    }

    /* set lockup parameter for bulkloading */
    lockupForBulkloading.mode = L_X;
    lockupForBulkloading.duration = L_COMMIT;

    /* set bound condition */
    bound.op = SM_EQ;
    keyLen = strlen(keyword);
    bound.key.len = sizeof(Two) + keyLen;
    bcopy(&keyLen,&(bound.key.val[0]),sizeof(Two));
    bcopy(keyword,&(bound.key.val[sizeof(Two)]),keyLen);

    lrdsScanNum = LRDS_OpenIndexScan(LOM_GET_LRDS_HANDLE(handle), orn, &iid, &bound, &bound, 0, NULL, &lockup);
    if( lrdsScanNum < eNOERROR) LOM_ERROR(handle, lrdsScanNum);

#ifndef SUBINDEX
    /* initialize posting buffer */
    e = LOM_AllocPostingBuffer(handle, &postingBuffer, LOM_DEFAULTPOSTINGBUFFERSIZE);
    if(e < eNOERROR) LOM_ERROR(handle, e);
    LOM_FREEOFFSET_POSTINGBUFFER(postingBuffer) = 0;

#define DEALLOCMEM_lom_Text_AddInvertedIndexEntryFromBuf(handle, e, e2) \
    e2 = LOM_FreePostingBuffer(handle, &postingBuffer); \
    if(e2 < eNOERROR) LOM_ERROR(handle, e2);    \
    LOM_ERROR(handle, e);

#else
#define DEALLOCMEM_lom_Text_AddInvertedIndexEntryFromBuf(handle, e, e2) LOM_ERROR(handle, e);
#endif  /* #ifndef SUBINDEX */

    /* CONSTRUCT clist */
    count = 0;
#ifndef SUBINDEX
    /* posting list */
    clist[count].colNo = LOM_INVERTEDINDEX_POSTINGLIST_COLNO;
    clist[count].nullFlag = SM_FALSE;
    if(isLogicalIdOrder) {
        currentOffset = 0;
        clist[count].start = currentOffset;
        clist[count].length = LOM_DEFAULTPOSTINGBUFFERSIZE;
        clist[count].dataLength = LOM_DEFAULTPOSTINGBUFFERSIZE;
        clist[count].data.ptr = LOM_PTR_POSTINGBUFFER(postingBuffer);
    }
    else {
        clist[count].start = TO_END;
        clist[count].length = 0;
        clist[count].dataLength = lengthOfPostingList;
        clist[count].data.ptr = ptrToPostingList;
    }
#endif
    count++;

    /* posting size */
#ifndef SUBINDEX
    clist[count].colNo = LOM_INVERTEDINDEX_SIZEOFPOSTING_COLNO;
    clist[count].start = ALL_VALUE;
    clist[count].dataLength = sizeof(Four);
    ASSIGN_VALUE_TO_COL_LIST(clist[count], lengthOfPostingList, sizeof(Four));
    clist[count].nullFlag = SM_FALSE;
#endif
    count++;

    /* number of postions */
    clist[count].colNo = LOM_INVERTEDINDEX_NPOSTINGS_COLNO;
    clist[count].start = ALL_VALUE;
    clist[count].dataLength = sizeof(Four);
    ASSIGN_VALUE_TO_COL_LIST(clist[count], nPostings, sizeof(Four));
    clist[count].nullFlag = SM_FALSE;
    count++;

    /* keyword */
    clist[count].colNo = LOM_INVERTEDINDEX_KEYWORD_COLNO;
    clist[count].start = ALL_VALUE;
    clist[count].dataLength = strlen(keyword);
    clist[count].data.ptr = keyword;
    clist[count].nullFlag = SM_FALSE;
    count++;

    *newlyRegisteredKeyword = FALSE; 

    /* get tuple */
    e = LRDS_NextTuple(LOM_GET_LRDS_HANDLE(handle), lrdsScanNum, tid, &cursor);
    if(e == EOS) { /* the corresponding tuple for the given keyword doesnot exists */

        *newlyRegisteredKeyword = TRUE;

#ifdef SUBINDEX
        /* create tuple(logical index entry) for inverted index table */
        /* clist[2] is for n postings (colNo = 2)
           clist[3] is for keyword (colNo = 0) */
        if(useBulkLoadFlag)
        {
            clist[4].nullFlag = SM_TRUE;

            e = LRDS_NextRelationBulkLoad(LOM_GET_LRDS_HANDLE(handle), lrdsBulkLoadId, 1, &clist[2], SM_FALSE, tid);    /* 2 */
            if(e < eNOERROR) { DEALLOCMEM_lom_Text_AddInvertedIndexEntryFromBuf(handle, e, e2); }

            clist[4].colNo = 3; /* pad null value by manually */
            e = LRDS_NextRelationBulkLoad(LOM_GET_LRDS_HANDLE(handle), lrdsBulkLoadId, 1, &clist[4], SM_FALSE, tid);    /* 3 */
            if(e < eNOERROR) { DEALLOCMEM_lom_Text_AddInvertedIndexEntryFromBuf(handle, e, e2); }

            e = LRDS_NextRelationBulkLoad(LOM_GET_LRDS_HANDLE(handle), lrdsBulkLoadId, 1, &clist[3], SM_FALSE, tid);    /* 0 */
            if(e < eNOERROR) { DEALLOCMEM_lom_Text_AddInvertedIndexEntryFromBuf(handle, e, e2); }

            clist[4].colNo = 1; /* pad null value by manually */
            e = LRDS_NextRelationBulkLoad(LOM_GET_LRDS_HANDLE(handle), lrdsBulkLoadId, 1, &clist[4], SM_FALSE, tid);    /* 1 */
            if(e < eNOERROR) { DEALLOCMEM_lom_Text_AddInvertedIndexEntryFromBuf(handle, e, e2); }
        }
        else
        {
            e = LRDS_CreateTuple(LOM_GET_LRDS_HANDLE(handle), lrdsScanNum, SM_TRUE, 2, &clist[2], tid);
            if(e < eNOERROR) { DEALLOCMEM_lom_Text_AddInvertedIndexEntryFromBuf(handle, e, e2); }
        }
#else
        /* create tuple(logical index entry) for inverted index table */
        /* clist[0] is for posting list (colNo = 4)
           clist[1] is for size of posting (colNo = 3)
           clist[2] is for n postings (colNo = 2) 
           clist[3] is for keyword (colNo = 0)  */
        if(useBulkLoadFlag)
        {
            e = LRDS_NextRelationBulkLoad(LOM_GET_LRDS_HANDLE(handle), lrdsBulkLoadId, 1, &clist[2], SM_FALSE, tid);
            if(e < eNOERROR) { DEALLOCMEM_lom_Text_AddInvertedIndexEntryFromBuf(handle, e, e2); }
            e = LRDS_NextRelationBulkLoad(LOM_GET_LRDS_HANDLE(handle), lrdsBulkLoadId, 1, &clist[1], SM_FALSE, tid);
            if(e < eNOERROR) { DEALLOCMEM_lom_Text_AddInvertedIndexEntryFromBuf(handle, e, e2); }
            e = LRDS_NextRelationBulkLoad(LOM_GET_LRDS_HANDLE(handle), lrdsBulkLoadId, 1, &clist[3], SM_FALSE, tid);
            if(e < eNOERROR) { DEALLOCMEM_lom_Text_AddInvertedIndexEntryFromBuf(handle, e, e2); }
            e = LRDS_NextRelationBulkLoad(LOM_GET_LRDS_HANDLE(handle), lrdsBulkLoadId, 1, &clist[0], SM_TRUE, tid);
            if(e < eNOERROR) { DEALLOCMEM_lom_Text_AddInvertedIndexEntryFromBuf(handle, e, e2); }
        }
        else
        {
            e = LRDS_CreateTuple(LOM_GET_LRDS_HANDLE(handle), lrdsScanNum, SM_TRUE, 4, &clist[0], tid);
            if(e < eNOERROR) { DEALLOCMEM_lom_Text_AddInvertedIndexEntryFromBuf(handle, e, e2); }
        }
#endif

#ifdef SUBINDEX
        /* create posting-list for the inverted-index table */
        if(useBulkLoadFlag)
        {
#ifndef COMPRESSION
            e = LRDS_NextRelationBulkLoad_OrderedSetBulkLoad(LOM_GET_LRDS_HANDLE(handle), temporaryVolId, lrdsBulkLoadId, LOM_INVERTEDINDEX_POSTINGLIST_COLNO,
													         nPostings, lengthOfPostingList, ptrToPostingList, SM_TRUE, tid);
#else
			e = lom_Text_PostingList_Compression(postingInfo, nPostings, &lengthOfPostingList, ptrToPostingList, ptrToCompressedPostingList, &volNoOfPostingTupleID, SM_TRUE, &lastDocId);
            if(e < eNOERROR) {
                DEALLOCMEM_lom_Text_AddInvertedIndexEntryFromBuf(handle, e, e2);
            }
			e = LRDS_NextRelationBulkLoad_OrderedSetBulkLoad(LOM_GET_LRDS_HANDLE(handle), temporaryVolId, lrdsBulkLoadId, LOM_INVERTEDINDEX_POSTINGLIST_COLNO,
													         nPostings, lengthOfPostingList, ptrToCompressedPostingList, SM_TRUE, tid, ptrToPostingList, volNoOfPostingTupleID, lastDocId);
#endif
			if(e < eNOERROR) {
				DEALLOCMEM_lom_Text_AddInvertedIndexEntryFromBuf(handle, e, e2);
        }
		}
        else
        {
            e = LRDS_OrderedSet_Create(LOM_GET_LRDS_HANDLE(handle), lrdsScanNum, SM_TRUE, tid, LOM_INVERTEDINDEX_POSTINGLIST_COLNO, NULL);
            if(e < eNOERROR) {
                DEALLOCMEM_lom_Text_AddInvertedIndexEntryFromBuf(handle, e, e2);
            }

#ifndef COMPRESSION
            e = LRDS_OrderedSet_AppendSortedElements(LOM_GET_LRDS_HANDLE(handle), 
                lrdsScanNum, 
                SM_TRUE,
                tid, 
                LOM_INVERTEDINDEX_POSTINGLIST_COLNO,
                nPostings,
                lengthOfPostingList,
                ptrToPostingList,
                NULL);
#else
			e = lom_Text_PostingList_Compression(postingInfo, nPostings, &lengthOfPostingList, ptrToPostingList, ptrToCompressedPostingList, &volNoOfPostingTupleID, SM_FALSE, &lastDocId);
            if(e < eNOERROR) {
                DEALLOCMEM_lom_Text_AddInvertedIndexEntryFromBuf(handle, e, e2);
            }
			e = LRDS_OrderedSet_SpecifyVolNo(LOM_GET_LRDS_HANDLE(handle), lrdsScanNum, SM_TRUE, tid, LOM_INVERTEDINDEX_POSTINGLIST_COLNO, volNoOfPostingTupleID, NULL);
			if(e < eNOERROR) {
				DEALLOCMEM_lom_Text_AddInvertedIndexEntryFromBuf(handle, e, e2);
        }
			e = LRDS_OrderedSet_AppendSortedElements(LOM_GET_LRDS_HANDLE(handle), 
				lrdsScanNum, 
				SM_TRUE,
				tid, 
				LOM_INVERTEDINDEX_POSTINGLIST_COLNO,
				nPostings,
				lengthOfPostingList,
				ptrToCompressedPostingList,
				NULL);
#endif
			if(e < eNOERROR) {
				DEALLOCMEM_lom_Text_AddInvertedIndexEntryFromBuf(handle, e, e2);
			}
		}
#endif
        if(useBulkLoadFlag)
        {
            if(buildReverseKeywordIndex == TRUE)
            {
                /* make reverse keyword */
                length = (Two)strlen(keyword);
                bcopy(&length, &reverseKeyword[0], sizeof(Two));
                makeReverseStr(keyword, &reverseKeyword[sizeof(Two)], length);

                /* insert reverse keyword into text-btree */
                e = LRDS_NextTextBulkLoad(LOM_GET_LRDS_HANDLE(handle), lrdsTextBulkLoadId, tid, 1, reverseKeyword);
                if(e < eNOERROR) {
                    DEALLOCMEM_lom_Text_AddInvertedIndexEntryFromBuf(handle, e, e2);
                }                          
            }
        }
        else
        {
            if(buildReverseKeywordIndex == TRUE)
            {
                /* make reverse keyword */
                length = (Two)strlen(keyword);
                bcopy(&length, &reverseKeyword[0], sizeof(Two));
                makeReverseStr(keyword, &reverseKeyword[sizeof(Two)], length);

                /* insert reverse keyword into text-btree */
                e = LRDS_Text_AddKeywords(LOM_GET_LRDS_HANDLE(handle), lrdsScanNum, SM_TRUE, tid, LOM_INVERTEDINDEX_REVKEYWORD_COLNO, 1, reverseKeyword);
                if(e < eNOERROR) {
                    DEALLOCMEM_lom_Text_AddInvertedIndexEntryFromBuf(handle, e, e2);
                } 
            }
        }
    }
    else {
        if(e < eNOERROR) {
            DEALLOCMEM_lom_Text_AddInvertedIndexEntryFromBuf(handle, e, e2);
        }

        if(isLogicalIdOrder) {
            /* 
             * NOTICE:
             * We here regard only the # of 
             * to-be-inserted postings as 1 
             */
#ifndef SUBINDEX
            clist[0].length     = LOM_DEFAULTPOSTINGBUFFERSIZE;
            clist[0].dataLength = LOM_DEFAULTPOSTINGBUFFERSIZE;
#endif

            /* read the size of posting list */
#ifdef SUBINDEX
            e = LRDS_FetchTuple(LOM_GET_LRDS_HANDLE(handle), lrdsScanNum, SM_TRUE, tid, 1, &clist[2]); /* posting size and number of posting */
#else
            e = LRDS_FetchTuple(LOM_GET_LRDS_HANDLE(handle), lrdsScanNum, SM_TRUE, tid, 3, &clist[0]); /* posting size and number of posting */
#endif
            if( e < eNOERROR) {
                DEALLOCMEM_lom_Text_AddInvertedIndexEntryFromBuf(handle, e, e2);
            }
    
            /* initialize */
            nPostingsInIndex = GET_VALUE_FROM_COL_LIST(clist[2], sizeof(nPostingsInIndex));
            offsetInBuffer = 0;
    
#ifdef SUBINDEX
#ifndef COMPRESSION
            e  = LRDS_OrderedSet_InsertElement(LOM_GET_LRDS_HANDLE(handle), 
                lrdsScanNum, 
                SM_TRUE,
                tid, 
                LOM_INVERTEDINDEX_POSTINGLIST_COLNO,
                ptrToPostingList,
                NULL);
#else
			e = lom_Text_PostingList_Compression(postingInfo, nPostings, &lengthOfPostingList, ptrToPostingList, ptrToCompressedPostingList, &volNoOfPostingTupleID, SM_FALSE, &lastDocId);
			if(e < eNOERROR) {
				DEALLOCMEM_lom_Text_AddInvertedIndexEntryFromBuf(handle, e, e2);
			}
			e = LRDS_OrderedSet_SpecifyVolNo(LOM_GET_LRDS_HANDLE(handle), lrdsScanNum, SM_TRUE, tid, LOM_INVERTEDINDEX_POSTINGLIST_COLNO, volNoOfPostingTupleID, NULL);
			if(e < eNOERROR) {
				DEALLOCMEM_lom_Text_AddInvertedIndexEntryFromBuf(handle, e, e2);
			}
			e  = LRDS_OrderedSet_InsertElement(LOM_GET_LRDS_HANDLE(handle), 
				lrdsScanNum, 
				SM_TRUE,
				tid, 
				LOM_INVERTEDINDEX_POSTINGLIST_COLNO,
				ptrToCompressedPostingList,
				NULL);
#endif

            if(e < eNOERROR) {
                DEALLOCMEM_lom_Text_AddInvertedIndexEntryFromBuf(handle, e, e2);
            }
#else
            while(nPostingsInIndex) {
                /* get logical id of the current posting */
                bcopy(&LOM_PTR_POSTINGBUFFER(postingBuffer)[offsetInBuffer], &currentPostingLogicalId, sizeof(Four));
    
                /* We now find the offset to insert the given posting */
                if(logicalDocId < currentPostingLogicalId) break;
    
                /* decrement the number of postings to read */
                nPostingsInIndex --;
    
                /* get the number of positions of the current posting */
                bcopy(&LOM_PTR_POSTINGBUFFER(postingBuffer)[offsetInBuffer + sizeof(Four) + sizeof(TupleID)], &nPositions, sizeof(Four));
    
                if(isContainingSentenceAndWordNum)
                {
                    offsetInBuffer += LOM_POSTING_SIZE(nPositions, sizeof(Four) + sizeof(Two));
                    currentOffset += LOM_POSTING_SIZE(nPositions, sizeof(Four) + sizeof(Two));
                }
                else if(isContainingByteOffset)
                {
                    offsetInBuffer += LOM_POSTING_SIZE(nPositions, sizeof(Four));
                    currentOffset += LOM_POSTING_SIZE(nPositions, sizeof(Four));
                }
                else
                {
                    offsetInBuffer += LOM_POSTING_SIZE(nPositions, 0);
                    currentOffset += LOM_POSTING_SIZE(nPositions, 0);
                }

                if(offsetInBuffer + sizeof(Four) + sizeof(TupleID) + sizeof(Four) <= clist[0].retLength) {
                    /* We have already read the very next posting */
                    /* go back to while-loop */
                    continue;
                }
    
                /* We have read all postings before currentOffset */
                clist[0].start = currentOffset;
    
                e = LRDS_FetchTuple(LOM_GET_LRDS_HANDLE(handle), lrdsScanNum, SM_TRUE, tid, 1, &clist[0]);
                if(e < eNOERROR) {
                    DEALLOCMEM_lom_Text_AddInvertedIndexEntryFromBuf(handle, e, e2);
                }
    
                offsetInBuffer = 0;
            }
#endif
    
#ifndef SUBINDEX
            /* append posting to posting-list */
#ifndef SUPPORT_LARGE_DATABASE2
            clist[1].data.l += lengthOfPostingList; /* size of posting-list */
#else
            clist[1].data.ll += lengthOfPostingList;    /* size of posting-list */
#endif
            clist[1].nullFlag = SM_FALSE;   
    
            /* put postings in posting list */
            clist[0].start = currentOffset;
            clist[0].length = 0;
            clist[0].dataLength = lengthOfPostingList;
            clist[0].data.ptr = ptrToPostingList;
            clist[0].nullFlag = SM_FALSE;   
#endif
#ifndef SUPPORT_LARGE_DATABASE2
            clist[2].data.l++;
#else
            clist[2].data.ll++;
#endif
            clist[2].nullFlag = SM_FALSE;   
    
#ifdef SUBINDEX
            e = LRDS_UpdateTuple(LOM_GET_LRDS_HANDLE(handle), lrdsScanNum, SM_TRUE, tid, 1, &clist[2]); /* posting-list, size of posting-list and the number of postings */
#else
            e = LRDS_UpdateTuple(LOM_GET_LRDS_HANDLE(handle), lrdsScanNum, SM_TRUE, tid, 3, &clist[0]); /* posting-list, size of posting-list and the number of postings */
#endif
            if( e < eNOERROR) {
                DEALLOCMEM_lom_Text_AddInvertedIndexEntryFromBuf(handle, e, e2);
            }
        }
        else {
            /* read the size of posting list */
#ifdef SUBINDEX
            e = LRDS_FetchTuple(LOM_GET_LRDS_HANDLE(handle), lrdsScanNum, SM_TRUE, tid, 1, &clist[2]); /* posting size and number of posting */
#else
            e = LRDS_FetchTuple(LOM_GET_LRDS_HANDLE(handle), lrdsScanNum, SM_TRUE, tid, 2, &clist[1]); /* posting size and number of posting */
#endif
            if(e < eNOERROR) {
                DEALLOCMEM_lom_Text_AddInvertedIndexEntryFromBuf(handle, e, e2);
            }
    
#ifndef SUBINDEX
            /* append posting to posting-list */
#ifndef SUPPORT_LARGE_DATABASE2
            clist[1].data.l += lengthOfPostingList; /* size of posting-list */
#else
            clist[1].data.ll += lengthOfPostingList;    /* size of posting-list */
#endif
            clist[1].nullFlag = SM_FALSE;   /* size of posting-list */
#endif
#ifndef SUPPORT_LARGE_DATABASE2
            clist[2].data.l += nPostings;          /* the number of postings */
#else
            clist[2].data.ll += nPostings;          /* the number of postings */
#endif
            clist[2].nullFlag = SM_FALSE;   
            
#ifdef SUBINDEX
            e = LRDS_UpdateTuple(LOM_GET_LRDS_HANDLE(handle), lrdsScanNum, SM_TRUE, tid, 1, &clist[2]); /* posting-list, size of posting-list and the number of postings */
#else
            e = LRDS_UpdateTuple(LOM_GET_LRDS_HANDLE(handle), lrdsScanNum, SM_TRUE, tid, 3, &clist[0]); /* posting-list, size of posting-list and the number of postings */
#endif
            if(e < eNOERROR) {
                DEALLOCMEM_lom_Text_AddInvertedIndexEntryFromBuf(handle, e, e2);
            }
#ifdef SUBINDEX
            /*
            if(useBulkLoadFlag)
            {
                e = LRDS_OrderedSetAppendBulkLoad(LOM_GET_LRDS_HANDLE(handle), temporaryVolId, orn, SM_FALSE, tid,
                                                  LOM_INVERTEDINDEX_POSTINGLIST_COLNO, 
                                                  nPostings, lengthOfPostingList, ptrToPostingList, 
                                                  &lockupForBulkloading, &lockupForBulkloading);
                if(e < eNOERROR) {
                    DEALLOCMEM_lom_Text_AddInvertedIndexEntryFromBuf(handle, e, e2);
                }
            }
            else
            */
            {
#ifndef COMPRESSION
                e = LRDS_OrderedSet_AppendSortedElements(LOM_GET_LRDS_HANDLE(handle), 
                    lrdsScanNum,
                    SM_TRUE,
                    tid,
                    LOM_INVERTEDINDEX_POSTINGLIST_COLNO,
                    nPostings,
                    lengthOfPostingList,
                    ptrToPostingList,
					NULL);
#else
				e = lom_Text_PostingList_Compression(postingInfo, nPostings, &lengthOfPostingList, ptrToPostingList, ptrToCompressedPostingList, &volNoOfPostingTupleID, SM_FALSE, &lastDocId);
				if(e < eNOERROR) {
					DEALLOCMEM_lom_Text_AddInvertedIndexEntryFromBuf(handle, e, e2);
				}
				e = LRDS_OrderedSet_SpecifyVolNo(LOM_GET_LRDS_HANDLE(handle), lrdsScanNum, SM_TRUE, tid, LOM_INVERTEDINDEX_POSTINGLIST_COLNO, volNoOfPostingTupleID, NULL);
				if(e < eNOERROR) {
					DEALLOCMEM_lom_Text_AddInvertedIndexEntryFromBuf(handle, e, e2);
				}
				e = LRDS_OrderedSet_AppendSortedElements(LOM_GET_LRDS_HANDLE(handle), 
					lrdsScanNum,
					SM_TRUE,
					tid,
					LOM_INVERTEDINDEX_POSTINGLIST_COLNO,
					nPostings,
					lengthOfPostingList,
					ptrToCompressedPostingList,
                    NULL);
#endif

                if(e < eNOERROR) {
                    DEALLOCMEM_lom_Text_AddInvertedIndexEntryFromBuf(handle, e, e2);
                } 
            }
#endif
        }
    }

    e = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), lrdsScanNum);
    if(e < eNOERROR) {
        DEALLOCMEM_lom_Text_AddInvertedIndexEntryFromBuf(handle, e, e2);
    }

#ifndef SUBINDEX
    e = LOM_FreePostingBuffer(handle, &postingBuffer);
    if(e < eNOERROR) LOM_ERROR(handle, e);
#endif

    return eNOERROR;
}

/*
 * Function: lom_Text_RemoveInvertedIndexEntry(handle, Four, OID *, Two)
 *
 * Description:
 *  remove posting information related with the given oid
 *
 * Retuns:
 *  error code
 */
Four lom_Text_RemoveInvertedIndexEntry( 
    LOM_Handle *handle, 
    Four ocnOrScanId,       /* IN: ocn or scanId */
    Boolean useScanFlag,        /* IN: flag */
    Four logicalDocId,          /* IN: document id */
    Two colNo           /* IN: column number */
)
{
    Four ornForInvertedIndexTable;  /* open relation number for inverted index table */
    IndexID iid;
    IndexID docId_iid;
    Four e;
    TupleID tid;
    Four count;
    lom_PostingBuffer postingBuffer;
    Four currentOffset; /* current offset starting from position-list stored on disk */
    char *ptrToPostingBuffer;
    Four offsetInBuffer;
    Four nPostings;
    Four e2;
    LRDS_Cursor *cursor;
    char keyword[LOM_MAXKEYWORDSIZE + 1];       /* temporary variable */
    char reverseKeyword[sizeof(Two) + LOM_MAXKEYWORDSIZE + 1];
    Two length; 
    LockParameter lockup;
    BoundCond bound;
    Two keyLen;
    ColListStruct clist[10];
    Four nPositions;
    Four ornForDocIdIndexTable; 
    Four lrdsScanNumForDocIdIndexTable;
    ColListStruct clistForDocIdIndex[1];
    Four pointerBufferIdx;
    Four currentIdxInDocIdIndexEntry;
    TupleID *pointerBuffer;
    Four numOfPointers;
    ColLengthInfoListStruct lengthInfoStruct;
    TupleID tidForDocIndexTable;
#ifdef SUBINDEX
    KeyValue kval;
#endif
#ifdef COMPRESSION
    char	*uncompressedData = NULL;
	char	*tmpPointerBuffer = NULL;
    Four	uncompressedDataLength;
	Four	i, inIndex, outIndex;
	VolNo	volNo;
#endif

    /* open relation number for inverted index table */
    ornForInvertedIndexTable = lom_Text_GetInvertedIndexTableORN(handle, ocnOrScanId, useScanFlag, colNo);
    if(ornForInvertedIndexTable < 0) LOM_ERROR(handle, eBADPARAMETER_LOM);

    /* get index id */
    e = lom_Text_GetDocIdIndex(handle, ocnOrScanId, useScanFlag, colNo, &iid);
    if(e < 0) LOM_ERROR(handle, eBADPARAMETER_LOM);

    /* get open class number */
    ornForDocIdIndexTable = lom_Text_GetDocIdIndexTableORN(handle, ocnOrScanId, useScanFlag, colNo);
    if(ornForDocIdIndexTable < 0) LOM_ERROR(handle, eBADPARAMETER_LOM);


    /* OPEN INDEX SCAN ON KEYWORD COLUMN */
    /* set the lockup parameter */
    if(useScanFlag) {
        lockup.duration = LOM_SCANTABLE(handle)[ocnOrScanId].lockup.duration;
        lockup.mode = LOM_SCANTABLE(handle)[ocnOrScanId].lockup.mode;
    }
    else {
        lockup.duration = L_COMMIT;
        lockup.mode = L_IX;
    }

    /* set bound condition */
    bound.op = SM_EQ;
    keyLen = LOM_LONG_SIZE_VAR;
    bound.key.len = sizeof(Two) + keyLen;
    bcopy(&keyLen, &(bound.key.val[0]), sizeof(Two));
    bcopy(&logicalDocId, &(bound.key.val[sizeof(Two)]), keyLen);

    lrdsScanNumForDocIdIndexTable = LRDS_OpenIndexScan(LOM_GET_LRDS_HANDLE(handle), ornForDocIdIndexTable, &iid, &bound, &bound, 0, NULL, &lockup);
    if( lrdsScanNumForDocIdIndexTable < eNOERROR) LOM_ERROR(handle, lrdsScanNumForDocIdIndexTable);

    /* initialize pointer buffer for temproary file */
#ifndef COMPRESSION
    pointerBuffer = (char *)malloc(sizeof(TupleID) * INIT_NUMOF_DOCID_POINTER);
    if(pointerBuffer == NULL) LOM_ERROR(handle, eOUTOFMEMORY_LOM);
#else
	pointerBuffer = NULL;
#endif

    /* initialize posting buffer */
    e = LOM_AllocPostingBuffer(handle, &postingBuffer, LOM_DEFAULTPOSTINGBUFFERSIZE);
    if(e < eNOERROR) LOM_ERROR(handle, e);
    LOM_FREEOFFSET_POSTINGBUFFER(postingBuffer) = 0;

#define DEALLOCMEM_lom_Text_RemoveInvertedIndexEntry(handle, e, e2) \
	if(pointerBuffer != NULL) free(pointerBuffer);			\
    e2 = LOM_FreePostingBuffer(handle, &postingBuffer); \
    if(e2 < eNOERROR) LOM_ERROR(handle, e2);    \
    LOM_ERROR(handle, e);

    currentIdxInDocIdIndexEntry = 0;
    
    /* CONSTRUCT clist */
    /* poiner-list */
    clistForDocIdIndex[0].colNo = LOM_DOCIDTABLE_POINTERLIST_COLNO;
#ifndef COMPRESSION
    clistForDocIdIndex[0].start = currentIdxInDocIdIndexEntry * sizeof(TupleID);
    clistForDocIdIndex[0].length = sizeof(TupleID) * INIT_NUMOF_DOCID_POINTER;
    clistForDocIdIndex[0].dataLength = sizeof(TupleID) * INIT_NUMOF_DOCID_POINTER;
    clistForDocIdIndex[0].data.ptr = pointerBuffer;
#endif

    /* get tuple */
    e = LRDS_NextTuple(LOM_GET_LRDS_HANDLE(handle), lrdsScanNumForDocIdIndexTable, &tidForDocIndexTable, &cursor);
    if(e < eNOERROR) {
        DEALLOCMEM_lom_Text_RemoveInvertedIndexEntry(handle, eINTERNAL_LOM, e2);
    }

    if(e == EOS) {
        /* close scan */
        e = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), lrdsScanNumForDocIdIndexTable);
        if( e < eNOERROR) {
            DEALLOCMEM_lom_Text_RemoveInvertedIndexEntry(handle, e, e2);
        }

        /* free pointer buffer */
        free(pointerBuffer);

        /* free posting buffer */
        e = LOM_FreePostingBuffer(handle, &postingBuffer);  
        if(e < eNOERROR) LOM_ERROR(handle, e);

        return eNOERROR;
    }

    lengthInfoStruct.colNo = LOM_DOCIDTABLE_POINTERLIST_COLNO;

    e = LRDS_FetchColLength(LOM_GET_LRDS_HANDLE(handle), lrdsScanNumForDocIdIndexTable, SM_TRUE, &tidForDocIndexTable, 1, &lengthInfoStruct);
    if(e < eNOERROR) {
        DEALLOCMEM_lom_Text_RemoveInvertedIndexEntry(handle, e, e2);
    }

#ifndef COMPRESSION
    numOfPointers = lengthInfoStruct.length / sizeof(TupleID);
#else
    pointerBuffer = (TupleID *)malloc(lengthInfoStruct.length);
    if(pointerBuffer == NULL) 
	{
		DEALLOCMEM_lom_Text_RemoveInvertedIndexEntry(handle, eOUTOFMEMORY_LOM, e2);
	}

    clistForDocIdIndex[0].start = ALL_VALUE;
    clistForDocIdIndex[0].length = lengthInfoStruct.length;
    clistForDocIdIndex[0].dataLength = lengthInfoStruct.length;
	clistForDocIdIndex[0].data.ptr = pointerBuffer;
#endif

    e = LRDS_FetchTuple(LOM_GET_LRDS_HANDLE(handle), lrdsScanNumForDocIdIndexTable, SM_TRUE, &tidForDocIndexTable, 1, clistForDocIdIndex);
    if(e < eNOERROR) {
        DEALLOCMEM_lom_Text_RemoveInvertedIndexEntry(handle, e, e2);
    }


#ifdef COMPRESSION
    uncompressedDataLength = sizeof(TupleID) * INIT_NUMOF_DOCID_POINTER;

    e = lom_Text_Uncompression(handle, clistForDocIdIndex[0].data.ptr, clistForDocIdIndex[0].retLength, &uncompressedData, &uncompressedDataLength);
    if(e < eNOERROR) 
	{
		if(uncompressedData != NULL) free(uncompressedData);
		DEALLOCMEM_lom_Text_RemoveInvertedIndexEntry(handle, e, e2);
	}

	numOfPointers = (uncompressedDataLength - sizeof(VolNo)) / (sizeof(TupleID) - sizeof(VolNo));
	pointerBuffer = (char *)realloc(pointerBuffer, numOfPointers * sizeof(TupleID));
    if(pointerBuffer == NULL) 
	{
		if(uncompressedData != NULL) free(uncompressedData);
		DEALLOCMEM_lom_Text_RemoveInvertedIndexEntry(handle, eOUTOFMEMORY_LOM, e2);
	}

	memcpy(&volNo, uncompressedData, sizeof(VolNo));

	inIndex = sizeof(VolNo);
	outIndex = 0;
	tmpPointerBuffer = (char*)pointerBuffer;

	for(i = 0; i < numOfPointers; i++)
	{
		memcpy(&tmpPointerBuffer[outIndex], &uncompressedData[inIndex], sizeof(PageNo));
		inIndex += sizeof(PageNo);
		outIndex += sizeof(PageNo);

		memcpy(&tmpPointerBuffer[outIndex], &volNo, sizeof(VolNo));
		outIndex += sizeof(VolNo);

		memcpy(&tmpPointerBuffer[outIndex], &uncompressedData[inIndex], sizeof(SlotNo) + sizeof(Unique));
		inIndex += sizeof(SlotNo) + sizeof(Unique);
		outIndex += sizeof(SlotNo) + sizeof(Unique);
	}

    if(uncompressedData != NULL) free(uncompressedData);
	pointerBuffer = (TupleID*)tmpPointerBuffer;
#endif

    /* CONSTRUCT clist */
    count = 0;

    /* posting size */
    clist[count].colNo = LOM_INVERTEDINDEX_SIZEOFPOSTING_COLNO;
    clist[count].start = ALL_VALUE;
    clist[count].dataLength = sizeof(Four);
    count++;

    /* number of postings */
    clist[count].colNo = LOM_INVERTEDINDEX_NPOSTINGS_COLNO;
    clist[count].start = ALL_VALUE;
    clist[count].dataLength = sizeof(Four);
    count++;

    /* keyword */
    clist[count].colNo = LOM_INVERTEDINDEX_KEYWORD_COLNO;
    clist[count].start = ALL_VALUE;
    clist[count].length = LOM_MAXKEYWORDSIZE;
    clist[count].dataLength = LOM_MAXKEYWORDSIZE;
    clist[count].data.ptr = keyword;
    count++;

    /* posting list */
    clist[count].colNo = LOM_INVERTEDINDEX_POSTINGLIST_COLNO;
    clist[count].start = 0;
    clist[count].length = LOM_DEFAULTPOSTINGBUFFERSIZE;
    clist[count].dataLength = LOM_DEFAULTPOSTINGBUFFERSIZE;
    clist[count].data.ptr = LOM_PTR_POSTINGBUFFER(postingBuffer);
    count++;

    pointerBufferIdx = 0;

    while(currentIdxInDocIdIndexEntry < numOfPointers) {

        tid = pointerBuffer[pointerBufferIdx];

        currentOffset = 0;
        clist[3].start = 0;     
        clist[3].length = LOM_DEFAULTPOSTINGBUFFERSIZE; 
        clist[3].dataLength = LOM_DEFAULTPOSTINGBUFFERSIZE; 

        /* fetch tuple */
#ifdef SUBINDEX
        e = LRDS_FetchTuple(LOM_GET_LRDS_HANDLE(handle), ornForInvertedIndexTable, SM_FALSE, &tid, count - 2, &clist[1]);
#else
        e = LRDS_FetchTuple(LOM_GET_LRDS_HANDLE(handle), ornForInvertedIndexTable, SM_FALSE, &tid, count, &clist[0]);
#endif
        if(e == eBADOBJECTID_OM)
        {
            currentIdxInDocIdIndexEntry++;
            pointerBufferIdx++;

#ifndef COMPRESSION
            if(pointerBufferIdx == INIT_NUMOF_DOCID_POINTER) {
                /* read next pointers */
                clistForDocIdIndex[0].start = currentIdxInDocIdIndexEntry * sizeof(TupleID);

                e = LRDS_FetchTuple(LOM_GET_LRDS_HANDLE(handle), lrdsScanNumForDocIdIndexTable, SM_TRUE, &tidForDocIndexTable, 1, clistForDocIdIndex);
                if(e < eNOERROR) {
                    DEALLOCMEM_lom_Text_RemoveInvertedIndexEntry(handle, e, e2);
                }
                
                pointerBufferIdx = 0;
            }
#endif COMPRESSION
            continue;
        }
        else if(e < eNOERROR) {
            DEALLOCMEM_lom_Text_RemoveInvertedIndexEntry(handle, e, e2);
        }

        ptrToPostingBuffer = LOM_PTR_POSTINGBUFFER(postingBuffer);
        offsetInBuffer = 0;
        nPostings = GET_VALUE_FROM_COL_LIST(clist[1], sizeof(nPostings));

        /* make reverse keyword */
        length = (Two)clist[2].retLength;
        bcopy(&length, &reverseKeyword[0], sizeof(Two));
        makeReverseStr(keyword, &reverseKeyword[sizeof(Two)],length);

        /* if the number of posting is 1, then destroy tuple itself */
        if(nPostings == 1) {    

            /* DELETE REVERSE-KEYWORD FROM INDEX */
            e = LRDS_Text_DeleteKeywords(LOM_GET_LRDS_HANDLE(handle), ornForInvertedIndexTable, SM_FALSE, &tid, LOM_INVERTEDINDEX_REVKEYWORD_COLNO, 1, reverseKeyword);
            if(e < eNOERROR) {
                DEALLOCMEM_lom_Text_RemoveInvertedIndexEntry(handle, e, e2);
            }

#ifdef SUBINDEX
            /* DELETE ORDERED SET */
            e = LRDS_OrderedSet_Destroy(LOM_GET_LRDS_HANDLE(handle), ornForInvertedIndexTable, SM_FALSE, &tid, LOM_INVERTEDINDEX_POSTINGLIST_COLNO, NULL);
            if(e < eNOERROR) {
                DEALLOCMEM_lom_Text_RemoveInvertedIndexEntry(handle, e, e2);
            }
#endif
            /* destroy tuple */
            e = LRDS_DestroyTuple(LOM_GET_LRDS_HANDLE(handle), ornForInvertedIndexTable, SM_FALSE, &tid);
            if( e < eNOERROR) {
                DEALLOCMEM_lom_Text_RemoveInvertedIndexEntry(handle, e, e2);
            }

        } /* if */
        else { 
#ifdef SUBINDEX
            kval.len = LOM_LONG_SIZE_VAR;
            bcopy(&logicalDocId, &(kval.val[0]), LOM_LONG_SIZE_VAR);
            e = LRDS_OrderedSet_DeleteElement(LOM_GET_LRDS_HANDLE(handle), ornForInvertedIndexTable, SM_FALSE, &tid, LOM_INVERTEDINDEX_POSTINGLIST_COLNO, &kval, NULL);
            if( e < eNOERROR) {
                DEALLOCMEM_lom_Text_RemoveInvertedIndexEntry(handle, e, e2);
            }
#else
            while(nPostings) {
                /* skip logical id field */
                if(!bcmp(&logicalDocId, &ptrToPostingBuffer[offsetInBuffer], sizeof(Four))) { 
                    bcopy(&ptrToPostingBuffer[offsetInBuffer + sizeof(Four)+ sizeof(TupleID)], &nPositions, sizeof(Four));
                    break;
                }
                /* decrement the number of postings to read */
                nPostings--; 

                if(nPostings == 0) {
                    DEALLOCMEM_lom_Text_RemoveInvertedIndexEntry(handle, eINTERNAL_LOM, e2);
                }

                /* get the number of position of the current posting */
                bcopy(&ptrToPostingBuffer[offsetInBuffer + sizeof(Four)+ sizeof(TupleID)], &nPositions, sizeof(Four));

                /* check if the very next posting is in memory buffer or not */
                if(isContainingSentenceAndWordNum)
                {
                    offsetInBuffer += LOM_POSTING_SIZE(nPositions, sizeof(Four) + sizeof(Two));
                    currentOffset += LOM_POSTING_SIZE(nPositions, sizeof(Four) + sizeof(Two));
                }
                else if(isContainingByteOffset)
                {
                    offsetInBuffer += LOM_POSTING_SIZE(nPositions, sizeof(Four));
                    currentOffset += LOM_POSTING_SIZE(nPositions, sizeof(Four));
                }
                else
                {
                    offsetInBuffer += LOM_POSTING_SIZE(nPositions, 0);
                    currentOffset += LOM_POSTING_SIZE(nPositions, 0);
                }

                if( offsetInBuffer + sizeof(Four)+ sizeof(TupleID) + sizeof(Four) <= clist[3].retLength) {
                    /* we have already read the very next posting */
                    /* go back to while-loop */
                    continue;
                }

                /* we have read all postings before currentOffset */
                clist[3].start = currentOffset;

                e = LRDS_FetchTuple(LOM_GET_LRDS_HANDLE(handle), ornForInvertedIndexTable, SM_FALSE, &tid, 1, &clist[3]);
                if( e < eNOERROR) {
                    DEALLOCMEM_lom_Text_RemoveInvertedIndexEntry(handle, e, e2);
                }
                offsetInBuffer = 0;
            }
#endif

#ifndef SUBINDEX
            /* size */
#ifndef SUPPORT_LARGE_DATABASE2
            if(isContainingSentenceAndWordNum)
                clist[0].data.l -= LOM_POSTING_SIZE(nPositions, sizeof(Four) + sizeof(Two));
            else if(isContainingByteOffset)
                clist[0].data.l -= LOM_POSTING_SIZE(nPositions, sizeof(Four));
            else
                clist[0].data.l -= LOM_POSTING_SIZE(nPositions, 0);
#else
            if(isContainingSentenceAndWordNum)
                clist[0].data.ll -= LOM_POSTING_SIZE(nPositions, sizeof(Four) + sizeof(Two));
            else if(isContainingByteOffset)
                clist[0].data.ll -= LOM_POSTING_SIZE(nPositions, sizeof(Four));
            else
                clist[0].data.ll -= LOM_POSTING_SIZE(nPositions, 0);
#endif

            clist[0].nullFlag = SM_FALSE;
#endif

            /* the number of postings */
#ifndef SUPPORT_LARGE_DATABASE2
            clist[1].data.l--;
#else
            clist[1].data.ll--;
#endif
            clist[1].nullFlag = SM_FALSE;

#ifndef SUBINDEX
            /* posting-list */
            clist[3].start = currentOffset;
            if(isContainingSentenceAndWordNum)
                clist[3].length = LOM_POSTING_SIZE(nPositions, sizeof(Four) + sizeof(Two));
            else if(isContainingByteOffset)
                clist[3].length = LOM_POSTING_SIZE(nPositions, sizeof(Four));
            else
                clist[3].length = LOM_POSTING_SIZE(nPositions, 0);

            clist[3].dataLength = 0;
            clist[3].nullFlag = SM_FALSE;
#endif

            /* update posting information */
#ifdef SUBINDEX
            e = LRDS_UpdateTuple(LOM_GET_LRDS_HANDLE(handle), ornForInvertedIndexTable, SM_FALSE, &tid, 1, &clist[1]);
#else
            e = LRDS_UpdateTuple(LOM_GET_LRDS_HANDLE(handle), ornForInvertedIndexTable, SM_FALSE, &tid, 2, &clist[0]);
            if( e < eNOERROR) {
                DEALLOCMEM_lom_Text_RemoveInvertedIndexEntry(handle, e, e2);
            }

            e = LRDS_UpdateTuple(LOM_GET_LRDS_HANDLE(handle), ornForInvertedIndexTable, SM_FALSE, &tid, 1, &clist[3]);
#endif
            if( e < eNOERROR) {
                DEALLOCMEM_lom_Text_RemoveInvertedIndexEntry(handle, e, e2);
            }

        } /* else */
        currentIdxInDocIdIndexEntry++;
        pointerBufferIdx++;

#ifndef COMPRESSION
        if(pointerBufferIdx == INIT_NUMOF_DOCID_POINTER) {
            /* read next pointers */
            clistForDocIdIndex[0].start = currentIdxInDocIdIndexEntry * sizeof(TupleID);

            e = LRDS_FetchTuple(LOM_GET_LRDS_HANDLE(handle), lrdsScanNumForDocIdIndexTable, SM_TRUE, &tidForDocIndexTable, 1, clistForDocIdIndex);
            if(e < eNOERROR) {
                DEALLOCMEM_lom_Text_RemoveInvertedIndexEntry(handle, e, e2);
            }
            
            pointerBufferIdx = 0;
        }
#endif COMPRESSION
    }

    /* close scan */
    e = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), lrdsScanNumForDocIdIndexTable);
    if( e < eNOERROR) {
        DEALLOCMEM_lom_Text_RemoveInvertedIndexEntry(handle, e, e2);
    }

    /* free pointer buffer */
    free(pointerBuffer);

    /* free posting buffer */
    e = LOM_FreePostingBuffer(handle, &postingBuffer);  
    if(e < eNOERROR) LOM_ERROR(handle, e);

    return eNOERROR;
}

Four lom_Text_AddDocIdIndexEntryFromBuf(
    LOM_Handle* handle, 
    Four        ocnOrScanId,
    Boolean     useScanFlag,
    Four        lrdsBulkLoadId,
    Four        lrdsTextBulkLoadId,
    Boolean     useBulkLoadFlag,
    Two         colNo,
    Four        logicalDocId, 
    Four        numOfPointers, 
#ifndef COMPRESSION 
    TupleID*    pointerBuffer
#else
	char*	    pointerBuffer,
    char 		*compressedData,
    Four        compressedDataLength
#endif
)
{
    ColListStruct   clist[1];
    TupleID         tid;
    char            docId[sizeof(Two) + LOM_LONG_SIZE_VAR];
    Two             length;
    Four            e;
    Four            ornForDocIdIndexTable;

#ifdef COMPRESSION
	FILE *fp;
	
	e = lom_Text_Compression(handle, pointerBuffer, numOfPointers, compressedData, &compressedDataLength);
	if(e < eNOERROR) LOM_ERROR(handle, e);
	
#endif

    /* CONSTRUCT clist */
    clist[0].colNo = LOM_DOCIDTABLE_POINTERLIST_COLNO;

#ifdef COMPRESSION
	clist[0].dataLength = compressedDataLength;
	clist[0].data.ptr = compressedData;
#else
    clist[0].dataLength = numOfPointers * sizeof(TupleID);
    clist[0].data.ptr = pointerBuffer;
#endif

    clist[0].nullFlag = SM_FALSE;
    clist[0].start = ALL_VALUE;

#ifdef COMPRESSION
	clist[0].length = compressedDataLength;
#else
    clist[0].length = numOfPointers * sizeof(TupleID);
#endif

    /* create tuple(logical index entry) for docId index table */
    if(useBulkLoadFlag)
    {
        e = LRDS_NextRelationBulkLoad(LOM_GET_LRDS_HANDLE(handle), lrdsBulkLoadId, 1, clist, SM_TRUE, &tid);
        if(e < eNOERROR) LOM_ERROR(handle, e);

        length = LOM_LONG_SIZE_VAR;
        bcopy(&length, &docId[0], sizeof(Two));
        bcopy(&logicalDocId, &docId[sizeof(Two)], length);

        e = LRDS_NextTextBulkLoad(LOM_GET_LRDS_HANDLE(handle), lrdsTextBulkLoadId, &tid, 1, docId);
        if(e < eNOERROR) LOM_ERROR(handle, e);
    }
    else
    {
        ornForDocIdIndexTable = lom_Text_GetDocIdIndexTableORN(handle, ocnOrScanId, useScanFlag, colNo);
        if(ornForDocIdIndexTable < 0) LOM_ERROR(handle, eBADPARAMETER_LOM);

        e = LRDS_CreateTuple(LOM_GET_LRDS_HANDLE(handle), ornForDocIdIndexTable, SM_FALSE, 1, clist, &tid);
        if(e < eNOERROR) LOM_ERROR(handle, e);

        length = LOM_LONG_SIZE_VAR;
        bcopy(&length, &docId[0], sizeof(Two));
        bcopy(&logicalDocId, &docId[sizeof(Two)], length);

        e = LRDS_Text_AddKeywords(LOM_GET_LRDS_HANDLE(handle), ornForDocIdIndexTable, SM_FALSE, &tid, LOM_DOCIDTABLE_DOCID_COLNO, 1, docId);
        if(e < eNOERROR) LOM_ERROR(handle, e);
    }       

    return eNOERROR;
}

 /*
  * Function: Four lom_Text_AddReverseKeywordIndexEntryFromTempFile(handle, Four, Boolean, Two, char*);
  *
  * Description:
  * add reverse keyword index entry
  *
  * Retuns:
  *  error code
  */
Four lom_Text_AddReverseKeywordIndexEntryFromTempFile(
    LOM_Handle  *handle, 
    Four        temporaryVolId,         /* IN: id for temporary volume used in sorting and etc. */
    Four        ocnOrScanId,            /* IN: ocn or scanId */
    Boolean     useScanFlag,            /* IN: flag */
    Two         colNo,
    char        *reverseKeywordFileName /* IN: temporary reverse keyword file name */
)
{
    Four e, e2;
    Four orn;
    Four lrdsScanNum;
    Four t1, t2, t3, t4;    /* temporary variables */
    Two  keyLen;
    FILE *fp;
    char *sortedFileName;
    char reverseKeyword[LOM_MAXKEYWORDSIZE + 1];
    char str[LOM_MAXKEYWORDSIZE + 1];
    char line[LOM_MAXKEYWORDSIZE+30];
    char *tempDir;
    LockParameter lockup;
    BoundCond startBound, stopBound;
    IndexID iid;
    TupleID tid;
    Four    lrdsTextBulkLoadId;
    LockParameter lockupForBulkloading;

    /* set lockup parameter for bulkloading */
    lockupForBulkloading.mode = L_X;
    lockupForBulkloading.duration = L_COMMIT;

    tempDir = getenv(TEXT_TEMP_PATH);
    if(tempDir == NULL) LOM_ERROR(handle, eCONFIGURATION_LOM);

    sortedFileName = tempnam(tempDir, "TEXT");
    if(sortedFileName == NULL) LOM_ERROR(handle, eOUTOFMEMORY_LOM);
 
    e = lom_Text_SortPostingFileByKeyword(handle, reverseKeywordFileName, sortedFileName);
    if (e < eNOERROR)
    {
        free(sortedFileName);
        LOM_ERROR(handle, e);
    }
 
#define DEALLOCMEM_lom_Text_AddReverseKeywordIndexEntryFromTempFile(handle, e, e2) \
    e2 = lom_Text_DestroyTempFile(handle, sortedFileName); \
    free(sortedFileName);   \
    LOM_ERROR(handle, e);
 
    /* open reverse keyword and tid pair file */
    e = lom_Text_OpenTempFile(handle, sortedFileName, "r", &fp);
    if (e < eNOERROR)
    {
        DEALLOCMEM_lom_Text_AddReverseKeywordIndexEntryFromTempFile(handle, e, e2);
    }
 
    /* open relation number for inverted index table */
    orn = lom_Text_GetInvertedIndexTableORN(handle, ocnOrScanId, useScanFlag, colNo);
    if(orn < 0)
    {
        DEALLOCMEM_lom_Text_AddReverseKeywordIndexEntryFromTempFile(handle, orn, e2)
    }

    lrdsTextBulkLoadId = LRDS_InitTextBulkLoad(LOM_GET_LRDS_HANDLE(handle), temporaryVolId, orn, SM_FALSE, SM_FALSE, LOM_INVERTEDINDEX_REVKEYWORD_COLNO, &lockupForBulkloading);
    if(lrdsTextBulkLoadId < 0)
    {
        DEALLOCMEM_lom_Text_AddReverseKeywordIndexEntryFromTempFile(handle, lrdsTextBulkLoadId, e2)
    }

    /* get index id */
    e = lom_Text_GetKeywordIndex(handle, ocnOrScanId, useScanFlag, colNo, &iid);
    if (e < 0)
    {
        DEALLOCMEM_lom_Text_AddReverseKeywordIndexEntryFromTempFile(handle, e, e2)
    }
 
    /* set the lockup parameter */
    if(useScanFlag) {
        lockup.duration = LOM_SCANTABLE(handle)[ocnOrScanId].lockup.duration;
        lockup.mode = LOM_SCANTABLE(handle)[ocnOrScanId].lockup.mode;
    }
    else {
        lockup.duration = L_COMMIT;
        lockup.mode = L_IX;
    }
 
    /* set bound condition */
    startBound.op = SM_BOF;
    stopBound.op = SM_EOF;
 
    lrdsScanNum = LRDS_OpenIndexScan(LOM_GET_LRDS_HANDLE(handle), orn, &iid, &startBound, &stopBound, 0, NULL, &lockup);
    if( lrdsScanNum < eNOERROR)
    {
        DEALLOCMEM_lom_Text_AddReverseKeywordIndexEntryFromTempFile(handle, lrdsScanNum, e2);
    }
 
    while (Util_fgets(line, LOM_MAXKEYWORDSIZE+30, fp) != NULL)
    {
        /* read reverse keyword and tid */
        e = sscanf(line, "%s %ld %ld %ld %ld\n", str, &t1, &t2, &t3, &t4);
        if (e != 5)
        {
            DEALLOCMEM_lom_Text_AddReverseKeywordIndexEntryFromTempFile(handle, e, e2)
        }
        tid.pageNo = t1;
        tid.volNo = (Two)t2;
        tid.slotNo = (Two)t3;
        tid.unique = t4;
 
        /* add index entry to reverse keyword index */
        keyLen = (Two)strlen(str);
        memcpy(&reverseKeyword[0], &keyLen, sizeof(Two));
        memcpy(&reverseKeyword[sizeof(Two)], str, keyLen);

        e = LRDS_NextTextBulkLoad(LOM_GET_LRDS_HANDLE(handle), lrdsTextBulkLoadId, &tid, 1, reverseKeyword);
        if(e < eNOERROR) { DEALLOCMEM_lom_Text_AddReverseKeywordIndexEntryFromTempFile(handle, e, e2) }
    }

    /* if it's not end-of-file, Error!! */
    if (!feof(fp))
    {
        DEALLOCMEM_lom_Text_AddReverseKeywordIndexEntryFromTempFile(handle, eUNIXFILEREADERROR_LOM, e2);
    }

    /* close and destroy reverse keyword and tid pair file */
    e = lom_Text_CloseTempFile(handle, fp);
    if (e < eNOERROR) LOM_ERROR(handle, e);

    e = lom_Text_DestroyTempFile(handle, sortedFileName);
    if (e < eNOERROR) LOM_ERROR(handle, e);

    free(sortedFileName);

    /* close scan */
    e = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), lrdsScanNum);
    if (e < eNOERROR) LOM_ERROR(handle, e);

    e = LRDS_FinalTextBulkLoad(LOM_GET_LRDS_HANDLE(handle), lrdsTextBulkLoadId);
    if (e < eNOERROR) LOM_ERROR(handle, e);

    return eNOERROR;
}

/*
 * Function: Four lom_Text_AddInvertedIndexEntryFromTempFile(handle, Four, char *);
 *
 * Description:
 *  add inverted entry into inverted-index table from posting file
 *
 * Retuns:
 *  error code
 */
Four lom_Text_AddInvertedIndexEntryFromTempFile(
    LOM_Handle* handle, 
    Four        temporaryVolId,                     /* IN: id for temporary volume used in sorting and etc. */
    Four        ocnOrScanId,                        /* IN: ocn or scanId */
    Boolean     useScanFlag,                        /* IN: flag */
    Two         colNo,                              /* IN: column number */
    char*       tempPostingFileName,                /* IN: temporary posting file name */
    char*       docIdFileName,                      /* INOUT: DocIdFile name */
    Boolean     isLogicalIdOrder,                   /* IN: isLogicalIdOrder */
    lom_Text_ConfigForInvertedIndexBuild* config    /* IN: index building configurations */
)
{
    lom_PostingBuffer postingBuffer;        /* posting buffer for temporary file */
    lom_PostingBuffer postingListBuffer;    /* posting-list buffer for inserting posting into inverted index table */
#ifdef COMPRESSION
    lom_PostingBuffer compressedPostingListBuffer;	/* compressed posting-list buffer */
#endif	
    Four e, e2;                             /* error code */
    FILE *postingFile;
    Four postingBufferLength;
    char *ptrToPostingBuffer;
    char keyword1[LOM_MAXKEYWORDSIZE + 1];      /* temporary variable */
    char keyword2[LOM_MAXKEYWORDSIZE + 1];      /* temporary variable */
    char reverseKeyword[MAXKEYWORDLEN + 1];     /* temporary variable for reverse keyword */
    Boolean noPosting;  
    Four nPositions;                        /* the number of positions */
    Four nPostings;                         /* the number of postings */
    char *ptrToPosting;
    Four numOfKeywords = 0;
    FILE *docIdFile;
    TupleID tidForInvertedIndexEntry;
    Four postingLengthFieldSize = LOM_LONG_SIZE_VAR;
    Four postingLength;
    Boolean newlyRegisteredKeyword; 
    char *reverseKeywordFileName;
    FILE *reverseKeywordFilePtr;
    char *tempDir;
    IndexID iid;
    Four len;
    Four    orn, lrdsOrn;
    Four    lrdsBulkLoadId, lrdsTextBulkLoadId;
    char    invertedIndexName[LOM_MAXCLASSNAME];
    char    className[LOM_MAXCLASSNAME];
    Four    idxForClassInfo, v;
    catalog_SysClassesOverlay*      ptrToSysClasses;
    catalog_SysAttributesOverlay*   ptrToSysAttributes;
    Four                            keywordLength1;
    lom_Text_PostingInfoForReading  postingInfo;
    Four                            sortStreamIdForDocId;
    Four                            sortStreamIdForPosting;
    LockParameter                   lockupForBulkloading;
    Four                            ocnForTempClass, scanIdForTempClass;
    Four                            count;

    /* set lockup parameter for bulkloading */
    lockupForBulkloading.mode = L_X;
    lockupForBulkloading.duration = L_COMMIT;

    /* get inverted index name */
    if(useScanFlag)
        orn = LRDS_SCANTABLE(LOM_GET_LRDS_HANDLE(handle))[LOM_SCANTABLE(handle)[ocnOrScanId].lrdsScanId].orn;
    else
        orn = ocnOrScanId;
    e = lom_GetClassName(handle, lom_GetVolIdFromOcn(handle, orn), LOM_USEROPENCLASSTABLE(handle)[orn].classID, className);
    if(e < 0) LOM_ERROR(handle, e);
    v = Catalog_GetVolIndex(handle, lom_GetVolIdFromOcn(handle, orn));
    if(v < 0) LOM_ERROR(handle, v);
    e = Catalog_GetClassInfo(handle, lom_GetVolIdFromOcn(handle, orn), LOM_USEROPENCLASSTABLE(handle)[orn].classID, &idxForClassInfo);
    if(e < 0) LOM_ERROR(handle, e);
    ptrToSysClasses    = &CATALOG_GET_CLASSINFOTBL(handle, v)[idxForClassInfo];
    ptrToSysAttributes = &CATALOG_GET_ATTRINFOTBL(handle, v)[CATALOG_GET_ATTRINFOTBL_INDEX(ptrToSysClasses)];
    lom_Text_MakeInvertedIndexName(handle, lom_GetVolIdFromOcn(handle, orn), className, ptrToSysAttributes[colNo].name, invertedIndexName);

    /* posting infos for reading temporary file */
    e = lom_Text_PreparePostingInfoForReading(handle, lom_GetVolIdFromOcn(handle, orn),
                                              LOM_USEROPENCLASSTABLE(handle)[orn].classID,
                                              colNo, &postingInfo);
    if(e < 0) LOM_ERROR(handle, e);

    /* open sort stream for docid index */
    if(config->isBuildingDocIdIndex)
    {
        sortStreamIdForDocId = lom_Text_OpenSortStreamForDocIdIndex(handle, temporaryVolId);
        if(sortStreamIdForDocId < 0) LOM_ERROR(handle, sortStreamIdForDocId);
    }
    else
        sortStreamIdForDocId = -1;

    /* open bulk loading */
    lrdsOrn = LRDS_OpenRelation(LOM_GET_LRDS_HANDLE(handle), lom_GetVolIdFromOcn(handle, orn), invertedIndexName);
    if(lrdsOrn < 0) LOM_ERROR(handle, lrdsOrn);
    
    if(config->isUsingBulkLoading)
    {
        lrdsBulkLoadId = LRDS_InitRelationBulkLoad(LOM_GET_LRDS_HANDLE(handle), lom_GetVolIdFromOcn(handle, orn), temporaryVolId, invertedIndexName, SM_FALSE,
                                                   config->isUsingKeywordIndexBulkLoading,
                                                   100,         /* bulkloading page fill factor   : */
                                                   100,         /* bulkloading extent fill factor : */
                                                   &lockupForBulkloading);
        if(lrdsBulkLoadId < 0) LOM_ERROR(handle, lrdsBulkLoadId);

        lrdsTextBulkLoadId = LRDS_InitTextBulkLoad(LOM_GET_LRDS_HANDLE(handle), temporaryVolId, lrdsOrn, SM_FALSE, config->isUsingReverseKeywordIndexBulkLoading, LOM_INVERTEDINDEX_REVKEYWORD_COLNO, &lockupForBulkloading);
        if(lrdsTextBulkLoadId < 0) LOM_ERROR(handle, lrdsTextBulkLoadId);
    }
    else
    {
        lrdsBulkLoadId     = -1;
        lrdsTextBulkLoadId = -1;
    }

    if(config->isBuildingExternalReverseKeywordFile)
    {
        /* prepare reverse keyword file */
        tempDir = getenv(TEXT_TEMP_PATH);
        if(tempDir == NULL) LOM_ERROR(handle, eCONFIGURATION_LOM);

        reverseKeywordFileName = tempnam(tempDir, "TEXT");
        if(reverseKeywordFileName == NULL) LOM_ERROR(handle, eOUTOFMEMORY_LOM);

        e = lom_Text_OpenTempFile(handle, reverseKeywordFileName, "w", &reverseKeywordFilePtr);
        if (e < eNOERROR)
        {
            free(reverseKeywordFileName);
            LOM_ERROR(handle, e);
        }
    }

    /* initialize posting buffer for temproary file */
    e = LOM_AllocPostingBuffer(handle, &postingBuffer, LOM_DEFAULTPOSTINGBUFFERSIZE);
    if(e < eNOERROR)
    {
        if(config->isBuildingExternalReverseKeywordFile)
        {
            e2 = lom_Text_CloseTempFile(handle, reverseKeywordFilePtr);
            if (e2 < eNOERROR)
            {
                free(reverseKeywordFileName);
                LOM_ERROR(handle, e2);
            }
    
            e2 = lom_Text_DestroyTempFile(handle, reverseKeywordFileName);
            if (e2 < eNOERROR)
            {
                free(reverseKeywordFileName);
                LOM_ERROR(handle, e2);
            }
            free(reverseKeywordFileName);
        }
        
        LOM_ERROR(handle, e);
    }
    LOM_FREEOFFSET_POSTINGBUFFER(postingBuffer) = 0;
    /* posting buffer looks like the following */
    /*
    ** variable array of |keyword|docid(logicalDocId,OID)|nPositions|postions|
    */

    e = LOM_AllocPostingBuffer(handle, &postingListBuffer, LOM_DEFAULTPOSTINGBUFFERSIZE);
    if(e < eNOERROR) {
        e2 = LOM_FreePostingBuffer(handle, &postingBuffer);
        if( e2 < eNOERROR) LOM_ERROR(handle, e2);

        if(config->isBuildingExternalReverseKeywordFile)
        {
            e2 = lom_Text_CloseTempFile(handle, reverseKeywordFilePtr);
            if (e2 < eNOERROR) 
            {
                free(reverseKeywordFileName);
                LOM_ERROR(handle, e2);
            }

            e2 = lom_Text_DestroyTempFile(handle, reverseKeywordFileName);
            if (e2 < eNOERROR) 
            {
                free(reverseKeywordFileName);
                LOM_ERROR(handle, e2);
            }

            free(reverseKeywordFileName);
        }

        LOM_ERROR(handle, e);
    }
    LOM_FREEOFFSET_POSTINGBUFFER(postingListBuffer) = 0;
    /* postingListBuffer looks like the following 
    ** variable array of |docid(logicalDocId,OID)|nPositions|postions|
    */
    
#ifdef COMPRESSION    
	e = LOM_AllocPostingBuffer(handle, &compressedPostingListBuffer, LOM_DEFAULTPOSTINGBUFFERSIZE);
	if(e < eNOERROR) {
		e2 = LOM_FreePostingBuffer(handle, &postingBuffer);
		if( e2 < eNOERROR) LOM_ERROR(handle, e2);
		
		e2 = LOM_FreePostingBuffer(handle, &postingListBuffer);
		if( e2 < eNOERROR) LOM_ERROR(handle, e2);

		if(config->isBuildingExternalReverseKeywordFile)
		{
			e2 = lom_Text_CloseTempFile(handle, reverseKeywordFilePtr);
			if (e2 < eNOERROR) 
			{
				free(reverseKeywordFileName);
				LOM_ERROR(handle, e2);
			}

			e2 = lom_Text_DestroyTempFile(handle, reverseKeywordFileName);
			if (e2 < eNOERROR) 
			{
				free(reverseKeywordFileName);
				LOM_ERROR(handle, e2);
			}

			free(reverseKeywordFileName);
		}

		LOM_ERROR(handle, e);
			}
	LOM_FREEOFFSET_POSTINGBUFFER(compressedPostingListBuffer) = 0;		
#endif	

    /* we define macro which deallocate memory allocated in the lom_Text_AddInvertedIndexEntryFromTempFile function */

#ifndef COMPRESSION    

#define DEALLOCMEM_lom_Text_AddInvertedIndexEntryFromTempFile(handle, e, e2) \
	if(config->isBuildingExternalReverseKeywordFile) \
	{ \
		e2 = lom_Text_CloseTempFile(handle, reverseKeywordFilePtr);	 \
		if (e2 < eNOERROR) LOM_ERROR(handle, e2);	\
		e2 = lom_Text_DestroyTempFile(handle, reverseKeywordFileName);	\
		if (e2 < eNOERROR) LOM_ERROR(handle, e2);	\
		free(reverseKeywordFileName);  \
		LOM_ERROR(handle, e); \
	} \
	e2 = LOM_FreePostingBuffer(handle, &postingBuffer);	\
	if(e2 < eNOERROR) LOM_ERROR(handle, e2);	\
	e2 = LOM_FreePostingBuffer(handle, &postingListBuffer);	\
	if(e2 < eNOERROR) LOM_ERROR(handle, e2); \
	LOM_ERROR(handle, e);

#else

#define DEALLOCMEM_lom_Text_AddInvertedIndexEntryFromTempFile(handle, e, e2) \
    if(config->isBuildingExternalReverseKeywordFile) \
    { \
        e2 = lom_Text_CloseTempFile(handle, reverseKeywordFilePtr);  \
        if (e2 < eNOERROR) LOM_ERROR(handle, e2);   \
        e2 = lom_Text_DestroyTempFile(handle, reverseKeywordFileName);  \
        if (e2 < eNOERROR) LOM_ERROR(handle, e2);   \
        free(reverseKeywordFileName);  \
        LOM_ERROR(handle, e); \
    } \
    e2 = LOM_FreePostingBuffer(handle, &postingBuffer); \
    if(e2 < eNOERROR) LOM_ERROR(handle, e2);    \
    e2 = LOM_FreePostingBuffer(handle, &postingListBuffer); \
    if(e2 < eNOERROR) LOM_ERROR(handle, e2); \
	e2 = LOM_FreePostingBuffer(handle, &compressedPostingListBuffer);	\
	if(e2 < eNOERROR) LOM_ERROR(handle, e2); \
    LOM_ERROR(handle, e);

#endif	

    /* open temporary posting file */
    if(config->isUsingStoredPosting)
        e = lom_Text_OpenTempClassForPosting(handle, lom_GetVolIdFromOcn(handle, orn), className, ptrToSysAttributes[colNo].name, &ocnForTempClass, &scanIdForTempClass);
    else
        e = lom_Text_OpenTempFile(handle, tempPostingFileName, "r", &postingFile);
    if(e < eNOERROR) {
        DEALLOCMEM_lom_Text_AddInvertedIndexEntryFromTempFile(handle, e,e2);
    }

    if(config->isSortingPostingFile)
    {
        /* read and sort posting file */
        sortStreamIdForPosting = lom_Text_OpenSortStreamForPosting(handle, temporaryVolId);
        if(sortStreamIdForPosting < 0) 
        {
            DEALLOCMEM_lom_Text_AddInvertedIndexEntryFromTempFile(handle, sortStreamIdForPosting, e2);
        }
    
        puts("reading posting");
        count = 0;
        while(1)
        {
            if(config->isUsingStoredPosting)
            {
                e = lom_Text_GetPostingFromTempClass(
                    handle,
                    scanIdForTempClass,
                    &postingBuffer);
            }
            else
            {
                e = lom_Text_GetPostingFromTempFile(
                    handle,
                    ocnOrScanId,
                    useScanFlag,
                    colNo,
                    &postingInfo,
                    postingFile,
                    &postingBuffer);
            }

            if(e < eNOERROR)
            {
                DEALLOCMEM_lom_Text_AddInvertedIndexEntryFromTempFile(handle, e, e2);
            }
            else if(e == EOS)
                break;

            e = lom_Text_AddPostingIntoSortStream(handle, sortStreamIdForPosting, 
                                                  LOM_SIZE_POSTINGBUFFER(postingBuffer),
                                                  LOM_PTR_POSTINGBUFFER(postingBuffer));
            if(e < eNOERROR)
            {
                DEALLOCMEM_lom_Text_AddInvertedIndexEntryFromTempFile(handle, e, e2);
            }

            count ++;
            if((count % 10000) == 0)
            {
                printf("%ld postings are read\n", count); fflush(stdout);
            }
        }

        puts("sorting posting");
        e = lom_Text_SortPostingSortStream(handle, sortStreamIdForPosting);
        if(e < eNOERROR) LOM_ERROR(handle, e);
        puts("done");
    }

    if(!config->isBuildingDocIdIndex)
    {
        /* 본함수에서 만들지 않을 것이면 외부 파일로 출력한다. */
        /* open temporary doc-id file */
        e = lom_Text_OpenTempFile(handle, docIdFileName, "w", &docIdFile);
        if(e < eNOERROR) {
            DEALLOCMEM_lom_Text_AddInvertedIndexEntryFromTempFile(handle, e,e2);
        }
    }

    /* temporary copy */
    postingBufferLength = LOM_DEFAULTPOSTINGBUFFERSIZE;

    keyword2[0] = '\0';
    noPosting = SM_TRUE;
    nPostings = 0;

    while(SM_TRUE) 
    {
        if(!config->isSortingPostingFile)
        {
            if(config->isUsingStoredPosting)
            {
                e = lom_Text_GetPostingFromTempClass(
                    handle,
                    scanIdForTempClass,
                    &postingBuffer);
            }
            else
            {
                /* get posting from temporary posting file */
                e = lom_Text_GetPostingFromTempFile(
                    handle,
                    ocnOrScanId,
                    useScanFlag,
                    colNo,
                    &postingInfo,
                    postingFile,    /* file descriptor */
                    &postingBuffer);
            }
        }
        else
        {
            e = lom_Text_GetPostingFromSortStream(
                handle, 
                sortStreamIdForPosting,
                LOM_SIZE_POSTINGBUFFER(postingBuffer),
                LOM_PTR_POSTINGBUFFER(postingBuffer));
        }
        if(e < eNOERROR) {
            DEALLOCMEM_lom_Text_AddInvertedIndexEntryFromTempFile(handle, e,e2);
        }
        else if(e == EOS) {
            if(!noPosting) {
                e = lom_Text_AddInvertedIndexEntryFromBuf(
                        handle, 
                        temporaryVolId,
                        ocnOrScanId, 
                        useScanFlag,
                        lrdsBulkLoadId,
                        lrdsTextBulkLoadId,
                        config->isUsingBulkLoading,
                        colNo,
                        keyword2, 
#ifdef COMPRESSION
					    &postingInfo,        
					    LOM_PTR_POSTINGBUFFER(compressedPostingListBuffer),
#endif
                        nPostings, 
                        LOM_ALLOCATEDSIZE_POSTINGBUFFER(postingListBuffer), 
                        LOM_PTR_POSTINGBUFFER(postingListBuffer),
                        &tidForInvertedIndexEntry,
                        isLogicalIdOrder,
                        !config->isBuildingExternalReverseKeywordFile,
                        &newlyRegisteredKeyword);
                if( e < eNOERROR) {
                    DEALLOCMEM_lom_Text_AddInvertedIndexEntryFromTempFile(handle, e,e2);
                }

                if (newlyRegisteredKeyword && config->isBuildingExternalReverseKeywordFile)
                {
                    len = strlen(keyword2);
#ifndef SUPPORT_LARGE_DATABASE2
                    Util_fprintf(reverseKeywordFilePtr, "%s %d %hd %hd %d\n",
                            makeReverseStr(keyword2, reverseKeyword, len),
                            tidForInvertedIndexEntry.pageNo,
                            tidForInvertedIndexEntry.volNo,
                            tidForInvertedIndexEntry.slotNo,
                            tidForInvertedIndexEntry.unique);
#else
                    Util_fprintf(reverseKeywordFilePtr, "%s %ld %d %d %ld\n",
							makeReverseStr(keyword2, reverseKeyword, len),
							tidForInvertedIndexEntry.pageNo,
							tidForInvertedIndexEntry.volNo,
							tidForInvertedIndexEntry.slotNo,
							tidForInvertedIndexEntry.unique);
#endif
                }

                if(!config->isBuildingDocIdIndex)
                {
                    e = lom_Text_AddDocIdIndexEntryIntoTempFile(
                        handle, 
                        docIdFile,
                        nPostings, 
                        LOM_PTR_POSTINGBUFFER(postingListBuffer),
                        &tidForInvertedIndexEntry);
                    if( e < eNOERROR) {
                        DEALLOCMEM_lom_Text_AddInvertedIndexEntryFromTempFile(handle, e,e2);
                    }
                }
                else
                {
                    e = lom_Text_AddDocIdIndexEntryIntoSortStream(handle, sortStreamIdForDocId,
                                                                  nPostings, LOM_PTR_POSTINGBUFFER(postingListBuffer),
                                                                  &tidForInvertedIndexEntry);
                    if( e < eNOERROR) {
                        DEALLOCMEM_lom_Text_AddInvertedIndexEntryFromTempFile(handle, e,e2);
                    }                                        
                }

            }
            break;  /* break out of while-loop */
        }
        noPosting = SM_FALSE;

        /* copy keyword */
        strcpy(keyword1, LOM_PTR_POSTINGBUFFER(postingBuffer));

        keywordLength1 = strlen(keyword1) + 1;
        bcopy(&LOM_PTR_POSTINGBUFFER(postingBuffer)[keywordLength1], &postingLength, sizeof(Four));

        /* pointer to posting-list */
        ptrToPosting = &LOM_PTR_POSTINGBUFFER(postingBuffer)[keywordLength1];

        /* same keyword */
        /* append posting information */
        if(!strcmp(keyword1,keyword2)) {
            /* if size of posting is greater than buffer size */
            if(postingLengthFieldSize + postingLength > LOM_FREEBYTES_POSTINGBUFFER(postingListBuffer)) 
            {
                e = lom_Text_IsKeywordExistInInvertedIndex(handle, ocnOrScanId, useScanFlag, colNo, keyword2);
                if( e < eNOERROR) {
                    DEALLOCMEM_lom_Text_AddInvertedIndexEntryFromTempFile(handle, e,e2);
                }

                if(config->isUsingBulkLoading && e == SM_FALSE)
                {
                    e = LOM_ReallocPostingBuffer(handle, &postingListBuffer, LOM_SIZE_POSTINGBUFFER(postingListBuffer) * 2);
					if( e < eNOERROR) {
						DEALLOCMEM_lom_Text_AddInvertedIndexEntryFromTempFile(handle, e,e2);
					}
#ifdef COMPRESSION					
					e = LOM_ReallocPostingBuffer(handle, &compressedPostingListBuffer, LOM_SIZE_POSTINGBUFFER(compressedPostingListBuffer) * 2);
                    if( e < eNOERROR) {
                        DEALLOCMEM_lom_Text_AddInvertedIndexEntryFromTempFile(handle, e,e2);
                    }
#endif					
                }
                else
                {
                    /* add inverted index entry */
                    e = lom_Text_AddInvertedIndexEntryFromBuf(
                        handle, 
                        temporaryVolId,
                        ocnOrScanId, 
                        useScanFlag,
                        lrdsBulkLoadId,
                        lrdsTextBulkLoadId,
                        config->isUsingBulkLoading, 
                        colNo,
                        keyword2, 
#ifdef COMPRESSION
    					&postingInfo,        
    					LOM_PTR_POSTINGBUFFER(compressedPostingListBuffer),
#endif
                        nPostings, 
                        LOM_ALLOCATEDSIZE_POSTINGBUFFER(postingListBuffer),
                        LOM_PTR_POSTINGBUFFER(postingListBuffer),
                        &tidForInvertedIndexEntry,
                        isLogicalIdOrder,
                        !config->isBuildingExternalReverseKeywordFile,
                        &newlyRegisteredKeyword);
                    if( e < eNOERROR) {
                        DEALLOCMEM_lom_Text_AddInvertedIndexEntryFromTempFile(handle, e,e2);
                    }

                    if (newlyRegisteredKeyword && config->isBuildingExternalReverseKeywordFile)
                    {
                        len = strlen(keyword2);
#ifndef SUPPORT_LARGE_DATABASE2
                        Util_fprintf(reverseKeywordFilePtr, "%s %d %hd %hd %d\n",
                                makeReverseStr(keyword2, reverseKeyword, len),
                                tidForInvertedIndexEntry.pageNo,
                                tidForInvertedIndexEntry.volNo,
                                tidForInvertedIndexEntry.slotNo,
                                tidForInvertedIndexEntry.unique);
#else
						Util_fprintf(reverseKeywordFilePtr, "%s %ld %d %d %ld\n",
								makeReverseStr(keyword2, reverseKeyword, len),
								tidForInvertedIndexEntry.pageNo,
								tidForInvertedIndexEntry.volNo,
								tidForInvertedIndexEntry.slotNo,
								tidForInvertedIndexEntry.unique);
#endif
                    }

                    if(!config->isBuildingDocIdIndex)
                    {
                        e = lom_Text_AddDocIdIndexEntryIntoTempFile(
                            handle, 
                            docIdFile,
                            nPostings, 
                            LOM_PTR_POSTINGBUFFER(postingListBuffer),
                            &tidForInvertedIndexEntry);
                        if( e < eNOERROR) {
                            DEALLOCMEM_lom_Text_AddInvertedIndexEntryFromTempFile(handle, e,e2);
                        }
                    }
                    else
                    {
                        e = lom_Text_AddDocIdIndexEntryIntoSortStream(handle, sortStreamIdForDocId,
                                                                      nPostings, LOM_PTR_POSTINGBUFFER(postingListBuffer),
                                                                      &tidForInvertedIndexEntry);
                        if( e < eNOERROR) {
                            DEALLOCMEM_lom_Text_AddInvertedIndexEntryFromTempFile(handle, e,e2);
                        }                                        
                    }

                    LOM_FREEOFFSET_POSTINGBUFFER(postingListBuffer) = 0;
#ifdef COMPRESSION
                    LOM_FREEOFFSET_POSTINGBUFFER(compressedPostingListBuffer) = 0;
#endif                    
                    nPostings = 0;
                }
            }
            lom_Text_AppendPostingToPostingList(
                handle, 
                &postingListBuffer, 
#ifdef COMPRESSION
				&compressedPostingListBuffer, 
#endif                
                postingLengthFieldSize + postingLength, 
                ptrToPosting);
            nPostings++;
        } 
        else {
            if(keyword2[0] == '\0') { /* is first */
                /* MAKE POSTING */
                lom_Text_AppendPostingToPostingList(
                    handle, 
                    &postingListBuffer, 
#ifdef COMPRESSION
				&compressedPostingListBuffer, 
#endif                
                    postingLengthFieldSize + postingLength, 
                    ptrToPosting);
            }
            else {
                /* add inverted index entry */
                e = lom_Text_AddInvertedIndexEntryFromBuf(  
                    handle, 
                    temporaryVolId,
                    ocnOrScanId, 
                    useScanFlag,
                    lrdsBulkLoadId,
                    lrdsTextBulkLoadId,
                    config->isUsingBulkLoading, 
                    colNo,
                    keyword2, 
#ifdef COMPRESSION
    				&postingInfo,        
    				LOM_PTR_POSTINGBUFFER(compressedPostingListBuffer),
#endif
                    nPostings, 
                    LOM_ALLOCATEDSIZE_POSTINGBUFFER(postingListBuffer),
                    LOM_PTR_POSTINGBUFFER(postingListBuffer),
                    &tidForInvertedIndexEntry,
                    isLogicalIdOrder,
                    !config->isBuildingExternalReverseKeywordFile,
                    &newlyRegisteredKeyword);
                if( e < eNOERROR) {
                    DEALLOCMEM_lom_Text_AddInvertedIndexEntryFromTempFile(handle, e,e2);
                }

                if (newlyRegisteredKeyword && config->isBuildingExternalReverseKeywordFile)
                {
                    len = strlen(keyword2);
#ifndef SUPPORT_LARGE_DATABASE2
                    Util_fprintf(reverseKeywordFilePtr, "%s %d %hd %hd %d\n",
                            makeReverseStr(keyword2, reverseKeyword, len),
                            tidForInvertedIndexEntry.pageNo,
                            tidForInvertedIndexEntry.volNo,
                            tidForInvertedIndexEntry.slotNo,
                            tidForInvertedIndexEntry.unique);
#else
                    Util_fprintf(reverseKeywordFilePtr, "%s %ld %d %d %ld\n",
							makeReverseStr(keyword2, reverseKeyword, len),
							tidForInvertedIndexEntry.pageNo,
							tidForInvertedIndexEntry.volNo,
							tidForInvertedIndexEntry.slotNo,
							tidForInvertedIndexEntry.unique);

#endif
                }

                if(!config->isBuildingDocIdIndex)
                {
                    e = lom_Text_AddDocIdIndexEntryIntoTempFile(
                        handle, 
                        docIdFile,
                        nPostings, 
                        LOM_PTR_POSTINGBUFFER(postingListBuffer),
                        &tidForInvertedIndexEntry);
                    if( e < eNOERROR) {
                        DEALLOCMEM_lom_Text_AddInvertedIndexEntryFromTempFile(handle, e,e2);
                    }
                }
                else
                {
                    e = lom_Text_AddDocIdIndexEntryIntoSortStream(handle, sortStreamIdForDocId,
                                                                  nPostings, LOM_PTR_POSTINGBUFFER(postingListBuffer),
                                                                  &tidForInvertedIndexEntry);
                    if( e < eNOERROR) {
                        DEALLOCMEM_lom_Text_AddInvertedIndexEntryFromTempFile(handle, e,e2);
                    }                                        
                }

                LOM_FREEOFFSET_POSTINGBUFFER(postingListBuffer) = 0;
#ifdef COMPRESSION
                LOM_FREEOFFSET_POSTINGBUFFER(compressedPostingListBuffer) = 0;
#endif				

                /* append posting to posting list */
                lom_Text_AppendPostingToPostingList(
                    handle, 
                    &postingListBuffer, 
#ifdef COMPRESSION
					&compressedPostingListBuffer, 
#endif				
                    postingLengthFieldSize + postingLength, 
                    ptrToPosting);

            }
            strcpy(keyword2, keyword1);
            nPostings = 1;

            numOfKeywords ++;
            if(!(numOfKeywords % 10000))
            {
#ifndef SUPPORT_LARGE_DATABASE2
				printf("%d keywords are inserted\n", numOfKeywords); fflush(stdout);
#else
				printf("%ld keywords are inserted\n", numOfKeywords); fflush(stdout);
#endif
            }
        } /* else */ /* two keywords are different */
    }
#ifndef SUPPORT_LARGE_DATABASE2
	printf("%d keywords are inserted\n", numOfKeywords); fflush(stdout);
#else
	printf("%ld keywords are inserted\n", numOfKeywords); fflush(stdout);
#endif

    /* final bulk loading */
    if(config->isUsingBulkLoading)
    {
        e = LRDS_FinalTextBulkLoad(LOM_GET_LRDS_HANDLE(handle), lrdsTextBulkLoadId);
        if (e < eNOERROR) LOM_ERROR(handle, e);

        e = LRDS_FinalRelationBulkLoad(LOM_GET_LRDS_HANDLE(handle), lrdsBulkLoadId);
        if (e < eNOERROR) LOM_ERROR(handle, e);
    }

    e = LRDS_CloseRelation(LOM_GET_LRDS_HANDLE(handle), lrdsOrn);
    if (e < eNOERROR) LOM_ERROR(handle, e);

    /* close temporary docId file */
    if(!config->isBuildingDocIdIndex)
    {
        e = lom_Text_CloseTempFile(handle, docIdFile);
        if(e < eNOERROR) LOM_ERROR(handle, e);
    }

    /* close temporary posting file */
    if(config->isUsingStoredPosting)
        e = lom_Text_CloseTempClassForPosting(handle, ocnForTempClass, scanIdForTempClass);
    else
        e = lom_Text_CloseTempFile(handle, postingFile);
    if(e < eNOERROR) LOM_ERROR(handle, e);

    /* free posting buffer */
    e2 = LOM_FreePostingBuffer(handle, &postingBuffer);
    if(e2 < eNOERROR) LOM_ERROR(handle, e2);

    /* free posting-list buffer */
    e2 = LOM_FreePostingBuffer(handle, &postingListBuffer); 
    if(e2 < eNOERROR) LOM_ERROR(handle, e2);

#ifdef COMPRESSION    
    e2 = LOM_FreePostingBuffer(handle, &compressedPostingListBuffer);	
	if(e2 < eNOERROR) LOM_ERROR(handle, e2);
#endif	

    if(config->isBuildingExternalReverseKeywordFile)
    {
        e = lom_Text_CloseTempFile(handle, reverseKeywordFilePtr);
        if (e < eNOERROR) LOM_ERROR(handle, e);

        e = lom_Text_AddReverseKeywordIndexEntryFromTempFile(handle, temporaryVolId, ocnOrScanId, useScanFlag, colNo, reverseKeywordFileName);
        if (e < eNOERROR) LOM_ERROR(handle, e);

        e = lom_Text_DestroyTempFile(handle, reverseKeywordFileName);
        if (e < eNOERROR) LOM_ERROR(handle, e);

        free(reverseKeywordFileName);
    }

    if(config->isSortingPostingFile)
    {
        e = lom_Text_CloseSortStreamForPosting(handle, sortStreamIdForPosting);
        if(e < 0) LOM_ERROR(handle, e);
    }

    if(config->isBuildingDocIdIndex)
    {
        /* sort sort-stream for doc id index */
        e = lom_Text_SortDocIdSortStream(handle, sortStreamIdForDocId);
        if (e < eNOERROR) LOM_ERROR(handle, e);

        /* build docid index using sort-stream */
        e = lom_Text_AddDocIdIndexEntryFromSortStream(handle, temporaryVolId, ocnOrScanId, useScanFlag,
                                                      colNo, sortStreamIdForDocId);
        if (e < eNOERROR) LOM_ERROR(handle, e);

        /* close sort-stream */
        e = lom_Text_CloseSortStreamForDocIdIndex(handle, sortStreamIdForDocId);
        if (e < eNOERROR) LOM_ERROR(handle, e);
    }

    return eNOERROR;
}

Four lom_Text_AddDocIdIndexEntryFromTempFile(
    LOM_Handle  *handle, 
    Four        temporaryVolId, /* IN: id for temporary volume used in sorting and etc. */
    Four        ocnOrScanId,    /* IN: ocn or scanId */
    Boolean     useScanFlag,    /* IN: flag */
    Two         colNo,          /* IN: column number */
    char        *docIdFileName  /* INOUT: DocIdFile name */
)
{
    Four e, e2; /* error code */
    FILE *docIdFile;
    TupleID tidForInvertedIndexEntry;
#ifndef COMPRESSION	
    TupleID *pointerBuffer;
#else
	char *pointerBuffer;
#endif
    Four     pointerBufferSize;
    Four pointerBufferIdx;
    Four docId1, docId2;
    Boolean noDocOid;

    Four    orn, lrdsOrn;
    Four    lrdsBulkLoadId, lrdsTextBulkLoadId;
    char    docIdIndexName[LOM_MAXCLASSNAME];
    char    className[LOM_MAXCLASSNAME];
    Four    idxForClassInfo, v;
    catalog_SysClassesOverlay*      ptrToSysClasses;
    catalog_SysAttributesOverlay*   ptrToSysAttributes;
    Four                            count = 0;
    LockParameter                   lockupForBulkloading;
#ifdef COMPRESSION
	char	*compressedData = NULL;
	Four    compressedDataLength;
	Four    sizeofTupleIDwithoutVolNo = sizeof(TupleID) - sizeof(VolNo);
#endif

    /* set lockup parameter for bulkloading */
    lockupForBulkloading.mode = L_X;
    lockupForBulkloading.duration = L_COMMIT;

    /* get inverted index name */
    if(useScanFlag)
        orn = LRDS_SCANTABLE(LOM_GET_LRDS_HANDLE(handle))[LOM_SCANTABLE(handle)[ocnOrScanId].lrdsScanId].orn;
    else
        orn = ocnOrScanId;
    e = lom_GetClassName(handle, lom_GetVolIdFromOcn(handle, orn), LOM_USEROPENCLASSTABLE(handle)[orn].classID, className);
    if(e < 0) LOM_ERROR(handle, e);
    e = Catalog_GetClassInfo(handle, lom_GetVolIdFromOcn(handle, orn), LOM_USEROPENCLASSTABLE(handle)[orn].classID, &idxForClassInfo);
    if(e < 0) LOM_ERROR(handle, e);
    v = Catalog_GetVolIndex(handle, lom_GetVolIdFromOcn(handle, orn));
    if(v < 0) LOM_ERROR(handle, v);
    ptrToSysClasses    = &CATALOG_GET_CLASSINFOTBL(handle, v)[idxForClassInfo];
    ptrToSysAttributes = &CATALOG_GET_ATTRINFOTBL(handle, v)[CATALOG_GET_ATTRINFOTBL_INDEX(ptrToSysClasses)];
    lom_Text_MakeDocIdTableName(handle, lom_GetVolIdFromOcn(handle, orn), className, ptrToSysAttributes[colNo].name, docIdIndexName);

    /* open bulk loading */
    lrdsOrn = LRDS_OpenRelation(LOM_GET_LRDS_HANDLE(handle), lom_GetVolIdFromOcn(handle, orn), docIdIndexName);
    if(lrdsOrn < 0) LOM_ERROR(handle, lrdsOrn);

    lrdsBulkLoadId = LRDS_InitRelationBulkLoad(LOM_GET_LRDS_HANDLE(handle), lom_GetVolIdFromOcn(handle, orn), temporaryVolId, docIdIndexName, SM_FALSE, SM_TRUE,
                                               100,         /* bulkloading page fill factor */
                                               100,         /* bulkloading extent fill factor */
                                               &lockupForBulkloading);
    if(lrdsBulkLoadId < 0) LOM_ERROR(handle, lrdsBulkLoadId);

    lrdsTextBulkLoadId = LRDS_InitTextBulkLoad(LOM_GET_LRDS_HANDLE(handle), temporaryVolId, lrdsOrn, SM_FALSE, SM_FALSE, LOM_DOCIDTABLE_DOCID_COLNO, &lockupForBulkloading);
    if(lrdsTextBulkLoadId < 0) LOM_ERROR(handle, lrdsTextBulkLoadId);

    /* initialize pointer buffer for temproary file */
#ifndef COMPRESSION	
    pointerBuffer = (TupleID *)malloc(sizeof(TupleID) * INIT_NUMOF_DOCID_POINTER);
    if(pointerBuffer == NULL) LOM_ERROR(handle, eOUTOFMEMORY_LOM);
    pointerBufferSize = INIT_NUMOF_DOCID_POINTER;
	pointerBufferIdx  = 0;
#else
	pointerBufferSize = sizeofTupleIDwithoutVolNo * INIT_NUMOF_DOCID_POINTER + sizeof(VolNo);
    pointerBuffer = (char *)malloc(pointerBufferSize);
    if(pointerBuffer == NULL) LOM_ERROR(handle, eOUTOFMEMORY_LOM);
    pointerBufferIdx  = 0;
	
	compressedDataLength = compressBound(pointerBufferSize);
	compressedData = (char *)malloc(compressedDataLength + sizeof(char));
	if(compressedData == NULL) LOM_ERROR(handle, eOUTOFMEMORY_LOM);
#endif

    /* we define macro which deallocate memory allocated in the lom_Text_AddDocIdIndexEntryFromTempFile function */

#ifndef COMPRESSION
#define DEALLOCMEM_lom_Text_AddDocIdIndexEntryFromTempFile(handle, e, e2) \
	free(pointerBuffer);	\
	LOM_ERROR(handle, e);
#else
#define DEALLOCMEM_lom_Text_AddDocIdIndexEntryFromTempFile(handle, e, e2) \
    free(pointerBuffer);    \
	if(compressedData != NULL) free(compressedData);	\
    LOM_ERROR(handle, e);
#endif

    /* open temporary doc-id file */
    e = lom_Text_OpenTempFile(handle, docIdFileName, "r", &docIdFile);
    if(e < eNOERROR) {
        DEALLOCMEM_lom_Text_AddDocIdIndexEntryFromTempFile(handle, e,e2);
    }

    docId2 = -1;
    noDocOid = SM_TRUE;

    while(SM_TRUE) {

        /* get posting from temporary posting file */
        e = lom_Text_GetDocIdAndPointerFromTempFile(
            handle, 
            docIdFile,  /* file descriptor */
            &docId1,
            &tidForInvertedIndexEntry);
        if(e < eNOERROR) {
                DEALLOCMEM_lom_Text_AddDocIdIndexEntryFromTempFile(handle, e,e2);
        }
        if(e == EOS) {
            if(!noDocOid) {
                e = lom_Text_AddDocIdIndexEntryFromBuf( 
                    handle, 
                    ocnOrScanId, 
                    useScanFlag,
                    lrdsBulkLoadId,
                    lrdsTextBulkLoadId,
                    SM_TRUE,
                    colNo, 
                    docId2, 
                    pointerBufferIdx, 
#ifndef COMPRESSION
                    pointerBuffer);
#else
					pointerBuffer,
					compressedData,
					compressedDataLength);
#endif
                if(e < eNOERROR) {
                    DEALLOCMEM_lom_Text_AddDocIdIndexEntryFromTempFile(handle, e,e2);
                }

            }
            break;  /* break out of while-loop */
        }
        noDocOid = SM_FALSE;

        /* same document-id */
        /* append posting information */
        if(docId1 == docId2) {
            /* if size of posting is greater than buffer size, then resize it */
			if(pointerBufferIdx + sizeof(TupleID) >= pointerBufferSize) {
#ifndef COMPRESSION						    
                pointerBuffer = (TupleID *)realloc(pointerBuffer, sizeof(TupleID) * pointerBufferSize * 2);
#else
                pointerBuffer = (char *)realloc(pointerBuffer, pointerBufferSize * 2);
#endif			
                if(pointerBuffer == NULL) LOM_ERROR(handle, eOUTOFMEMORY_LOM);
                pointerBufferSize *= 2;

#ifdef COMPRESSION
                compressedDataLength = compressBound(pointerBufferSize);
				compressedData = (char *)realloc(compressedData, compressedDataLength + sizeof(char));
				if(compressedData == NULL) LOM_ERROR(handle, eOUTOFMEMORY_LOM);
#endif
            }
#ifndef COMPRESSION			
            pointerBuffer[pointerBufferIdx] = tidForInvertedIndexEntry;
            pointerBufferIdx++;
#else
            if(pointerBufferIdx == 0)
            {
                memcpy(&pointerBuffer[pointerBufferIdx], (char*)&tidForInvertedIndexEntry+sizeof(PageNo), sizeof(VolNo));
                pointerBufferIdx += sizeof(VolNo);
            }
			memcpy(&pointerBuffer[pointerBufferIdx], (char*)&tidForInvertedIndexEntry, sizeof(PageNo));
			pointerBufferIdx += sizeof(PageNo);
			memcpy(&pointerBuffer[pointerBufferIdx], (char*)&tidForInvertedIndexEntry+sizeof(PageNo)+sizeof(VolNo), sizeof(SlotNo)+sizeof(Unique));
			pointerBufferIdx += sizeof(SlotNo)+sizeof(Unique);
#endif
			
        } 
        else {
            if(docId2 != -1) { /* it's not first time*/
                e = lom_Text_AddDocIdIndexEntryFromBuf(
                    handle, 
                    ocnOrScanId, 
                    useScanFlag,
                    lrdsBulkLoadId,
                    lrdsTextBulkLoadId,
                    SM_TRUE,
                    colNo, 
                    docId2, 
                    pointerBufferIdx, 
#ifndef COMPRESSION
                    pointerBuffer);
#else
					pointerBuffer,
					compressedData,
					compressedDataLength);
#endif
                if(e < eNOERROR) {
                    DEALLOCMEM_lom_Text_AddDocIdIndexEntryFromTempFile(handle, e,e2);
                }

                pointerBufferIdx = 0;
            }
#ifndef COMPRESSION			
            pointerBuffer[pointerBufferIdx] = tidForInvertedIndexEntry;
            pointerBufferIdx++;
#else
            if(pointerBufferIdx == 0)
            {
                memcpy(&pointerBuffer[pointerBufferIdx], (char*)&tidForInvertedIndexEntry+sizeof(PageNo), sizeof(VolNo));
                pointerBufferIdx += sizeof(VolNo);
            }
			memcpy(&pointerBuffer[pointerBufferIdx], (char*)&tidForInvertedIndexEntry, sizeof(PageNo));
			pointerBufferIdx += sizeof(PageNo);
			memcpy(&pointerBuffer[pointerBufferIdx], (char*)&tidForInvertedIndexEntry+sizeof(PageNo)+sizeof(VolNo), sizeof(SlotNo)+sizeof(Unique));
			pointerBufferIdx += sizeof(SlotNo)+sizeof(Unique);
#endif
            docId2 = docId1;

            count ++;
            if(!(count % 10000))
            {
                printf("%ld docids are inserted\n", count); fflush(stdout);
            }
        } /* else */ 
    }
    printf("Total %ld docids are inserted\n", count); fflush(stdout);

    /* close temporary docId file */
    e = lom_Text_CloseTempFile(handle, docIdFile);
    if(e < eNOERROR) LOM_ERROR(handle, e);

    free(pointerBuffer);

#ifdef COMPRESSION
	if(compressedData != NULL) free(compressedData);
#endif

    /* final bulk loading */
    e = LRDS_FinalTextBulkLoad(LOM_GET_LRDS_HANDLE(handle), lrdsTextBulkLoadId);
    if (e < eNOERROR) LOM_ERROR(handle, e);

    e = LRDS_FinalRelationBulkLoad(LOM_GET_LRDS_HANDLE(handle), lrdsBulkLoadId);
    if (e < eNOERROR) LOM_ERROR(handle, e);

    e = LRDS_CloseRelation(LOM_GET_LRDS_HANDLE(handle), lrdsOrn);
    if (e < eNOERROR) LOM_ERROR(handle, e);
    
    return eNOERROR;
}

Four lom_Text_AddDocIdIndexEntryFromSortStream(
    LOM_Handle  *handle, 
    Four        temporaryVolId, /* IN: id for temporary volume used in sorting and etc. */
    Four        ocnOrScanId,    /* IN: ocn or scanId */
    Boolean     useScanFlag,    /* IN: flag */
    Two         colNo,          /* IN: column number */
    Four        sortStreamId    /* IN: sort stream id */
)
{
    Four    e; /* error code */
    TupleID tidForInvertedIndexEntry;
#ifndef COMPRESSION	
    TupleID *pointerBuffer;
#else
	char *pointerBuffer;
#endif
    Four    pointerBufferSize;
    Four    pointerBufferIdx;
    Four    docId1, docId2;
    Boolean noDocOid;

    Four    orn, lrdsOrn;
    Four    lrdsBulkLoadId, lrdsTextBulkLoadId;
    char    docIdIndexName[LOM_MAXCLASSNAME];
    char    className[LOM_MAXCLASSNAME];
    Four    idxForClassInfo, v;
    catalog_SysClassesOverlay*      ptrToSysClasses;
    catalog_SysAttributesOverlay*   ptrToSysAttributes;
    Four                            count = 0;
    LockParameter                   lockupForBulkloading;
#ifdef COMPRESSION
	char	*compressedData = NULL;
	Four    compressedDataLength;
	Four    sizeofTupleIDwithoutVolNo = sizeof(TupleID) - sizeof(VolNo);	
#endif

    /* set lockup parameter for bulkloading */
    lockupForBulkloading.mode = L_X;
    lockupForBulkloading.duration = L_COMMIT;

    /* get inverted index name */
    if(useScanFlag)
        orn = LRDS_SCANTABLE(LOM_GET_LRDS_HANDLE(handle))[LOM_SCANTABLE(handle)[ocnOrScanId].lrdsScanId].orn;
    else
        orn = ocnOrScanId;
    e = lom_GetClassName(handle, lom_GetVolIdFromOcn(handle, orn), LOM_USEROPENCLASSTABLE(handle)[orn].classID, className);
    if(e < 0) LOM_ERROR(handle, e);
    e = Catalog_GetClassInfo(handle, lom_GetVolIdFromOcn(handle, orn), LOM_USEROPENCLASSTABLE(handle)[orn].classID, &idxForClassInfo);
    if(e < 0) LOM_ERROR(handle, e);
    v = Catalog_GetVolIndex(handle, lom_GetVolIdFromOcn(handle, orn));
    if(v < 0) LOM_ERROR(handle, v);
    ptrToSysClasses    = &CATALOG_GET_CLASSINFOTBL(handle, v)[idxForClassInfo];
    ptrToSysAttributes = &CATALOG_GET_ATTRINFOTBL(handle, v)[CATALOG_GET_ATTRINFOTBL_INDEX(ptrToSysClasses)];
    lom_Text_MakeDocIdTableName(handle, lom_GetVolIdFromOcn(handle, orn), className, ptrToSysAttributes[colNo].name, docIdIndexName);

    /* open bulk loading */
    lrdsOrn = LRDS_OpenRelation(LOM_GET_LRDS_HANDLE(handle), lom_GetVolIdFromOcn(handle, orn), docIdIndexName);
    if(lrdsOrn < 0) LOM_ERROR(handle, lrdsOrn);

    lrdsBulkLoadId = LRDS_InitRelationBulkLoad(LOM_GET_LRDS_HANDLE(handle), lom_GetVolIdFromOcn(handle, orn), temporaryVolId, docIdIndexName, SM_FALSE, SM_TRUE,
                                               100,         /* bulkloading page fill factor */
                                               100,         /* bulkloading extent fill factor */
                                               &lockupForBulkloading);
    if(lrdsBulkLoadId < 0) LOM_ERROR(handle, lrdsBulkLoadId);

    lrdsTextBulkLoadId = LRDS_InitTextBulkLoad(LOM_GET_LRDS_HANDLE(handle), temporaryVolId, lrdsOrn, SM_FALSE, SM_FALSE, LOM_DOCIDTABLE_DOCID_COLNO, &lockupForBulkloading);
    if(lrdsTextBulkLoadId < 0) LOM_ERROR(handle, lrdsTextBulkLoadId);

    /* initialize pointer buffer for temproary file */
#ifndef COMPRESSION	
    pointerBuffer = (TupleID *)malloc(sizeof(TupleID) * INIT_NUMOF_DOCID_POINTER);
    if(pointerBuffer == NULL) LOM_ERROR(handle, eOUTOFMEMORY_LOM);
    pointerBufferSize = INIT_NUMOF_DOCID_POINTER;
    pointerBufferIdx  = 0;
#else
	pointerBufferSize = sizeofTupleIDwithoutVolNo * INIT_NUMOF_DOCID_POINTER + sizeof(VolNo);
    pointerBuffer = (char *)malloc(pointerBufferSize);
    if(pointerBuffer == NULL) LOM_ERROR(handle, eOUTOFMEMORY_LOM);
	pointerBufferIdx  = 0;
	
	compressedDataLength = compressBound(pointerBufferSize);
	compressedData = (char *)malloc(compressedDataLength + sizeof(char));
	if(compressedData == NULL) LOM_ERROR(handle, eOUTOFMEMORY_LOM);
#endif

    /* we define macro which deallocate memory allocated in the lom_Text_AddDocIdIndexEntryFromTempFile function */

#ifndef COMPRESSION
#define DEALLOCMEM_lom_Text_AddDocIdIndexEntryFromTempFile(handle, e, e2) \
	free(pointerBuffer);	\
	LOM_ERROR(handle, e);
#else
#define DEALLOCMEM_lom_Text_AddDocIdIndexEntryFromTempFile(handle, e, e2) \
    free(pointerBuffer);    \
	if(compressedData != NULL) free(compressedData);	\
    LOM_ERROR(handle, e);
#endif

    docId2 = -1;
    noDocOid = SM_TRUE;

    while(SM_TRUE) {

        /* get posting from temporary posting file */
        e = lom_Text_GetDocIdAndPointerFromSortStream(
            handle, 
            sortStreamId,
            &docId1,
            &tidForInvertedIndexEntry);
        if(e < eNOERROR) {
                DEALLOCMEM_lom_Text_AddDocIdIndexEntryFromTempFile(handle, e,e2);
        }
        if(e == EOS) {
            if(!noDocOid) {
                e = lom_Text_AddDocIdIndexEntryFromBuf( 
                    handle, 
                    ocnOrScanId, 
                    useScanFlag,
                    lrdsBulkLoadId,
                    lrdsTextBulkLoadId,
                    SM_TRUE,
                    colNo, 
                    docId2, 
                    pointerBufferIdx, 
#ifndef COMPRESSION
                    pointerBuffer);
#else
					pointerBuffer,
					compressedData,
					compressedDataLength);
#endif
                if(e < eNOERROR) {
                    DEALLOCMEM_lom_Text_AddDocIdIndexEntryFromTempFile(handle, e,e2);
                }

            }
            break;  /* break out of while-loop */
        }
        noDocOid = SM_FALSE;

        /* same document-id */
        /* append posting information */
        if(docId1 == docId2) {

            /* if size of posting is greater than buffer size */
			if(pointerBufferIdx + sizeof(TupleID) >= pointerBufferSize) {
#ifndef COMPRESSION						    
                pointerBuffer = (TupleID *)realloc(pointerBuffer, sizeof(TupleID) * pointerBufferSize * 2);
#else
                pointerBuffer = (char *)realloc(pointerBuffer, pointerBufferSize * 2);
#endif			
                if(pointerBuffer == NULL) LOM_ERROR(handle, eOUTOFMEMORY_LOM);
                pointerBufferSize *= 2;

#ifdef COMPRESSION
                compressedDataLength = pointerBufferSize;
				compressedData = (char *)realloc(compressedData, compressedDataLength + sizeof(char));
				if(compressedData == NULL) LOM_ERROR(handle, eOUTOFMEMORY_LOM);
#endif
            }
#ifndef COMPRESSION			
            pointerBuffer[pointerBufferIdx] = tidForInvertedIndexEntry;
            pointerBufferIdx++;
#else
            if(pointerBufferIdx == 0)
            {
                memcpy(&pointerBuffer[pointerBufferIdx], (char*)&tidForInvertedIndexEntry+sizeof(PageNo), sizeof(VolNo));
                pointerBufferIdx += sizeof(VolNo);
            }
			memcpy(&pointerBuffer[pointerBufferIdx], (char*)&tidForInvertedIndexEntry, sizeof(PageNo));
			pointerBufferIdx += sizeof(PageNo);
			memcpy(&pointerBuffer[pointerBufferIdx], (char*)&tidForInvertedIndexEntry+sizeof(PageNo)+sizeof(VolNo), sizeof(SlotNo)+sizeof(Unique));
			pointerBufferIdx += sizeof(SlotNo)+sizeof(Unique);
#endif
        } 
        else {
            if(docId2 != -1) { /* it's not first time*/
                e = lom_Text_AddDocIdIndexEntryFromBuf(
                    handle, 
                    ocnOrScanId, 
                    useScanFlag,
                    lrdsBulkLoadId,
                    lrdsTextBulkLoadId,
                    SM_TRUE,
                    colNo, 
                    docId2, 
                    pointerBufferIdx, 
#ifndef COMPRESSION
                    pointerBuffer);
#else
					pointerBuffer,
					compressedData,
					compressedDataLength);
#endif
                if(e < eNOERROR) {
                    DEALLOCMEM_lom_Text_AddDocIdIndexEntryFromTempFile(handle, e,e2);
                }

                pointerBufferIdx = 0;
            }
#ifndef COMPRESSION			
            pointerBuffer[pointerBufferIdx] = tidForInvertedIndexEntry;
            pointerBufferIdx++;
#else
            if(pointerBufferIdx == 0)
            {
                memcpy(&pointerBuffer[pointerBufferIdx], (char*)&tidForInvertedIndexEntry+sizeof(PageNo), sizeof(VolNo));
                pointerBufferIdx += sizeof(VolNo);
            }
			memcpy(&pointerBuffer[pointerBufferIdx], (char*)&tidForInvertedIndexEntry, sizeof(PageNo));
			pointerBufferIdx += sizeof(PageNo);
			memcpy(&pointerBuffer[pointerBufferIdx], (char*)&tidForInvertedIndexEntry+sizeof(PageNo)+sizeof(VolNo), sizeof(SlotNo)+sizeof(Unique));
			pointerBufferIdx += sizeof(SlotNo)+sizeof(Unique);
#endif
            docId2 = docId1;

            count ++;
            if(!(count % 10000))
            {
                printf("%ld docids are inserted\n", count); fflush(stdout);
            }
        } /* else */ 
    }
    printf("Total %ld docids are inserted\n", count); fflush(stdout);

    free(pointerBuffer);

#ifdef COMPRESSION
	if(compressedData != NULL) free(compressedData);
#endif

    /* final bulk loading */
    e = LRDS_FinalTextBulkLoad(LOM_GET_LRDS_HANDLE(handle), lrdsTextBulkLoadId);
    if (e < eNOERROR) LOM_ERROR(handle, e);

    e = LRDS_FinalRelationBulkLoad(LOM_GET_LRDS_HANDLE(handle), lrdsBulkLoadId);
    if (e < eNOERROR) LOM_ERROR(handle, e);

    e = LRDS_CloseRelation(LOM_GET_LRDS_HANDLE(handle), lrdsOrn);
    if (e < eNOERROR) LOM_ERROR(handle, e);
    
    return eNOERROR;
}

Four lom_Text_RemoveDocIdIndexEntry(
    LOM_Handle *handle, 
    Four ocnOrScanId,                /* IN: ocn or scan id */
    Boolean useScanFlag,        /* IN: flag */
    Four logicalDocId,                       /* IN: document id */
    Two colNo                       /* IN: column number */
)
{
    Four ornForDocIdIndexTable;
    IndexID iid;
    Four lrdsScanNumForDocIdIndexTable;
    LockParameter lockup; /* lock parameter */
    BoundCond bound;
    TupleID tid;
    LRDS_Cursor *cursor;
    char docId[sizeof(Two) + LOM_LONG_SIZE_VAR];
    Four e;
    Two keyLen;

#ifndef DOC_DELETION_BY_KEYWORDEXTRACTOR
    /* get index id */
    e = lom_Text_GetDocIdIndex(handle, ocnOrScanId, useScanFlag, colNo, &iid);
    if(e < 0) LOM_ERROR(handle, eBADPARAMETER_LOM);

    /* get open class number */
    ornForDocIdIndexTable = lom_Text_GetDocIdIndexTableORN(handle, ocnOrScanId, useScanFlag, colNo);
    if(ornForDocIdIndexTable < 0) LOM_ERROR(handle, eBADPARAMETER_LOM);

    /* set the lockup parameter */
    if(useScanFlag) {
        lockup.duration = LOM_SCANTABLE(handle)[ocnOrScanId].lockup.duration;
        lockup.mode = LOM_SCANTABLE(handle)[ocnOrScanId].lockup.mode;
    }
    else {
        lockup.duration = L_COMMIT;
        lockup.mode = L_IX;
    }

    /* set bound condition */
    bound.op = SM_EQ;
    keyLen = LOM_LONG_SIZE_VAR;
    bound.key.len = sizeof(Two) + LOM_LONG_SIZE_VAR;
    bcopy(&keyLen,&(bound.key.val[0]),sizeof(Two));
    bcopy(&logicalDocId,&(bound.key.val[sizeof(Two)]),keyLen);

    lrdsScanNumForDocIdIndexTable = LRDS_OpenIndexScan(LOM_GET_LRDS_HANDLE(handle), ornForDocIdIndexTable, &iid, &bound, &bound, 0, NULL, &lockup);
    if(lrdsScanNumForDocIdIndexTable < eNOERROR) LOM_ERROR(handle, lrdsScanNumForDocIdIndexTable);

    /* get tuple */
    e = LRDS_NextTuple(LOM_GET_LRDS_HANDLE(handle), lrdsScanNumForDocIdIndexTable, &tid, &cursor);
    if(e == EOS) {
        e = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), lrdsScanNumForDocIdIndexTable);
        if( e < eNOERROR) LOM_ERROR(handle, e);

        return eNOERROR;
    }
    else {
        /* make doc-id */
        keyLen = LOM_LONG_SIZE_VAR;
        bcopy(&keyLen, &docId[0], sizeof(Two));
        bcopy(&logicalDocId, &docId[sizeof(Two)], keyLen);

        e = LRDS_Text_DeleteKeywords(LOM_GET_LRDS_HANDLE(handle), lrdsScanNumForDocIdIndexTable, SM_TRUE, &tid, LOM_DOCIDTABLE_DOCID_COLNO, 1, docId);
        if(e < eNOERROR) LOM_ERROR(handle, e);

        e = LRDS_DestroyTuple(LOM_GET_LRDS_HANDLE(handle), lrdsScanNumForDocIdIndexTable, SM_TRUE, &tid);
        if(e < eNOERROR) LOM_ERROR(handle, e);
    }

    e = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), lrdsScanNumForDocIdIndexTable);
    if( e < eNOERROR) LOM_ERROR(handle, e);

#endif
    return eNOERROR;
}

Four lom_Text_OpenTempClassForPosting(
    LOM_Handle                              *handle,            /* IN: system handle */
    Four                                    volId,              /* IN: volume id */
    char                                    *className,         /* IN */
    char                                    *attrName,          /* IN */
    Four                                    *ocn,               /* OUT */
    Four                                    *scanId             /* OUT */
)
{
    char            tempClassName[LOM_MAXCLASSNAME];
    LockParameter   lockup;
    Four            e;

    sprintf(tempClassName, "_%s_%s_Posting", className, attrName);

    e = LOM_OpenClass(handle, volId, tempClassName);
    if(e < eNOERROR) LOM_ERROR(handle, e);
    *ocn = e;

    lockup.mode     = L_S;
    lockup.duration = L_COMMIT;

    e = LOM_OpenSeqScan(handle, *ocn, FORWARD, 0, NULL, &lockup);
    if(e < eNOERROR) LOM_ERROR(handle, e);
    *scanId = e;

    return eNOERROR;
}

Four lom_Text_CloseTempClassForPosting(
    LOM_Handle                              *handle,            /* IN: system handle */
    Four                                    ocn,                /* IN */
    Four                                    scanId              /* IN */
)
{
    Four e;

    e = LOM_CloseScan(handle, scanId);
    if(e < eNOERROR) LOM_ERROR(handle, e);

    e = LOM_CloseClass(handle, ocn);
    if(e < eNOERROR) LOM_ERROR(handle, e);

    return eNOERROR;
}

Four lom_Text_GetPostingFromTempClass(
    LOM_Handle                              *handle, 
    Four                                    scanIdForTempClass, /* IN: scanId for temp calss */
    lom_PostingBuffer                       *postingBuffer      /* IN: posting buffer */
)
{
    Four                    e;
    OID                     oid;
    LOM_ColListStruct       clist[1];
    ColLengthInfoListStruct lengthInfoList[1];

    e = LOM_NextObject(handle, scanIdForTempClass, &oid, NULL);
    if(e < eNOERROR) LOM_ERROR(handle, e);

    lengthInfoList[0].colNo = 2;
    e = LOM_FetchColLength(handle, scanIdForTempClass, SM_TRUE, NULL, 1, lengthInfoList);
    if(e < eNOERROR) LOM_ERROR(handle, e);

    if(lengthInfoList[0].length > LOM_SIZE_POSTINGBUFFER(*postingBuffer))
    {
        e = LOM_ReallocPostingBuffer(handle, postingBuffer, lengthInfoList[0].length);
        if(e < eNOERROR) LOM_ERROR(handle, e);
    }

    clist[0].colNo      = 2;
    clist[0].data.ptr   = LOM_PTR_POSTINGBUFFER(*postingBuffer);
    clist[0].dataLength = LOM_SIZE_POSTINGBUFFER(*postingBuffer);
    clist[0].start      = ALL_VALUE;

    e = LOM_FetchObjectByColList(handle, scanIdForTempClass, SM_TRUE, &oid, 1, clist);
    if(e < eNOERROR) LOM_ERROR(handle, e);

    return eNOERROR;
}

Four lom_Text_GetPostingFromTempFile(
    LOM_Handle                              *handle, 
    Four                                    ocnOrScanId,    /* IN: open class no or scan id */
    Boolean                                 useScanFlag,    /* IN: flag */
    Two                                     colNo,          /* IN: column no */
    lom_Text_PostingInfoForReading*         postingInfo,    /* IN: posting info */
    FILE                                    *fd,            /* IN: file descriptor */
    lom_PostingBuffer                       *postingBuffer  /* IN: posting buffer */
)
{
    Four e, e2; 
    Four returnedNumOfPostings;
    Four requiredPostingBufferLength;
    Four volId;
    ClassID classId;
    Four orn;
    lrds_RelTableEntry *relTableEntry;

    if(useScanFlag) {
        orn = LRDS_SCANTABLE(LOM_GET_LRDS_HANDLE(handle))[LOM_SCANTABLE(handle)[ocnOrScanId].lrdsScanId].orn;
    }
    else orn = ocnOrScanId;

    classId = LOM_USEROPENCLASSTABLE(handle)[orn].classID;

    relTableEntry = LRDS_GET_RELTABLE_ENTRY(LOM_GET_LRDS_HANDLE(handle), orn);
    volId =  relTableEntry->ri.fid.volNo;

    /* get one posing from temporary posting file into posting buffer */
    e = lom_Text_NextPostingsFromTempFile(
        handle, 
        volId,
        classId,
        colNo,
        postingInfo,
        fd,
        LOM_SIZE_POSTINGBUFFER(*postingBuffer),
        LOM_PTR_POSTINGBUFFER(*postingBuffer),
        1,
        &returnedNumOfPostings,
        &requiredPostingBufferLength);

    /* no more posings in posting file */
    if(e == EOS) return EOS;

    /* if buffer is short of storing one posting */
    if(e == eMOREMEMORYREQUIRED_LOM) {
        e2 = LOM_ReallocPostingBuffer(handle, postingBuffer, requiredPostingBufferLength);
        if( e < eNOERROR) {
            if(e2 < eNOERROR) LOM_ERROR(handle, e2);
        }
    }
    else if (e < eNOERROR) LOM_ERROR(handle, e);
    else {
        return eNOERROR;
    }

    /* get one posing from temporary posting file into posting buffer */
    e = lom_Text_NextPostingsFromTempFile(
        handle, 
        volId, 
        classId,
        colNo,
        postingInfo,
        fd,
        LOM_SIZE_POSTINGBUFFER(*postingBuffer),
        LOM_PTR_POSTINGBUFFER(*postingBuffer),
        1,
        &returnedNumOfPostings,
        &requiredPostingBufferLength);
    if(e < eNOERROR) LOM_ERROR(handle, e);

    return eNOERROR;
}

/*
 * Function: Four lom_Text_AppendPostingToPostingList(handle, lom_PostingBuffer *, Four, char *);
 *
 * Description:
 *  append posting pointed by 'ptrToPosting' variable to posting buffer 'postingListBuffer'
 *
 * Retuns:
 *  error code
 */
void lom_Text_AppendPostingToPostingList(
    LOM_Handle *handle, 
    lom_PostingBuffer *postingListBuffer,
#ifdef COMPRESSION    
	lom_PostingBuffer *compressedPostingListBuffer,
#endif	
    Four postingLength, 
    char *ptrToPosting
)
{
    char *ptrToPostingListBuffer;

    ptrToPostingListBuffer = &LOM_PTR_POSTINGBUFFER((*postingListBuffer))[LOM_FREEOFFSET_POSTINGBUFFER(*postingListBuffer)];
    bcopy(ptrToPosting, ptrToPostingListBuffer, postingLength);
    LOM_FREEOFFSET_POSTINGBUFFER(*postingListBuffer) += postingLength;
#ifdef COMPRESSION    
	LOM_FREEOFFSET_POSTINGBUFFER(*compressedPostingListBuffer) += postingLength;
#endif	
}

Four lom_Text_GetDocIdAndPointerFromTempFile(
    LOM_Handle  *handle, 
    FILE        *docIdFile,      /* file descriptor */
    Four        *docId,
    TupleID     *tidForInvertedIndexEntry
)
{
    Four e;
    Four pageNo, volNo, slotNo, unique;

#ifndef SUPPORT_LARGE_DATABASE2
    if(Util_fscanf(docIdFile, "%d %d %hd %hd %d", 
        docId, &pageNo, &volNo, &slotNo, &unique) == EOF) return EOS;
#else
    if(Util_fscanf(docIdFile, "%ld %ld %d %d %ld", 
        docId, &pageNo, &volNo, &slotNo, &unique) == EOF) return EOS;
#endif

    /* make pointer */
    tidForInvertedIndexEntry->pageNo = pageNo;
    tidForInvertedIndexEntry->volNo = (Two)volNo;
    tidForInvertedIndexEntry->slotNo = (Two)slotNo;
    tidForInvertedIndexEntry->unique = unique;

    return eNOERROR;
}

Four lom_Text_AddDocIdIndexEntryIntoTempFile(
    LOM_Handle *handle, 
    FILE *docIdFile,
    Four nPostings,
    char *ptrToPostingList,
    TupleID *tidForInvertedIndexEntry
)
{
    Four i;
    Four offset;
    Four logicalDocId;
    Four nPositions;
    Four e;
    Four postingLengthFieldSize = LOM_LONG_SIZE_VAR;
    Four postingLength;

    offset = 0;
    for(i = 0; i < nPostings ;i++) {
        bcopy(&ptrToPostingList[offset + postingLengthFieldSize], &logicalDocId, sizeof(Four));
        bcopy(&ptrToPostingList[offset], &postingLength, sizeof(Four));

#ifndef SUPPORT_LARGE_DATABASE2
        Util_fprintf(docIdFile, "%d %d %hd %hd %d\n", logicalDocId, tidForInvertedIndexEntry->pageNo, tidForInvertedIndexEntry->volNo, tidForInvertedIndexEntry->slotNo, tidForInvertedIndexEntry->unique);
#else
        Util_fprintf(docIdFile, "%ld %ld %d %d %ld\n", logicalDocId, tidForInvertedIndexEntry->pageNo, tidForInvertedIndexEntry->volNo, tidForInvertedIndexEntry->slotNo, tidForInvertedIndexEntry->unique);
#endif
    
        offset += postingLength + postingLengthFieldSize;
    }
    return eNOERROR;

}

Four lom_Text_SortDocIdFile(
    LOM_Handle *handle, 
    char *srcDocIdFile,
    char *destDocIdFile
)
{
#ifndef WIN32
    char buf[4096];
    int rc;
    char *envBuf; 

    envBuf = getenv(TEXT_TEMP_PATH);
    if(envBuf == NULL) LOM_ERROR(handle, eCONFIGURATION_LOM);

    sprintf(buf,"sort -T %s -k 1n,2n +1 -2n %s > %s", envBuf, srcDocIdFile, destDocIdFile);

    rc = system(buf);

    if(WIFEXITED(rc) != 0 && WEXITSTATUS(rc) == 0 ) return eNOERROR;
    else return eFAILTOSORT_LOM;
#else
    char buf[4096];
    unsigned long retValue;
    char *envBuf; 
    STARTUPINFO startInfo;
    PROCESS_INFORMATION processInfo;

    envBuf = getenv(TEXT_TEMP_PATH);
    if(envBuf == NULL) LOM_ERROR(handle, eCONFIGURATION_LOM);

    sprintf(buf, "unixsort -T %s -k 1n,2n +1 -2n %s -o %s", envBuf, srcDocIdFile, destDocIdFile);

    memset((void *)&startInfo,0,sizeof(STARTUPINFO));
    startInfo.cb = sizeof(STARTUPINFO);
    startInfo.dwFlags = STARTF_USESHOWWINDOW;
    startInfo.wShowWindow = SW_HIDE;
    if (!CreateProcess(NULL,buf,NULL,NULL,FALSE,
                       REALTIME_PRIORITY_CLASS,
                       NULL,NULL,
                       &startInfo, &processInfo))
    {
            return eFAILTOSORT_LOM; 
    }

    while (GetExitCodeProcess(processInfo.hProcess, &retValue))
    {
            if (retValue != STILL_ACTIVE) break;
    }
#endif  
    return eNOERROR;
}

Four lom_Text_OpenSortStreamForDocIdIndex(
    LOM_Handle*     handle,                     /* IN: LOM system handle */
    Four            volId                       /* IN: volume Id */
)
{
    Four            e;
    Four            sortStreamIdForDocId;
    SortTupleDesc   sortTupleDesc;

    sortTupleDesc.hdrSize = 0;
    sortTupleDesc.nparts  = 1;
    sortTupleDesc.parts[0].type   = LOM_LONG_VAR;
    sortTupleDesc.parts[0].length = sizeof(Four);
    sortTupleDesc.parts[0].flag   = SORTKEYDESC_ATTR_ASC;
    
    sortStreamIdForDocId = LRDS_OpenSortStream(LOM_GET_LRDS_HANDLE(handle), (Two)volId, &sortTupleDesc);
    if(sortStreamIdForDocId < 0) LOM_ERROR(handle, sortStreamIdForDocId);

    return sortStreamIdForDocId;
}
Four lom_Text_CloseSortStreamForDocIdIndex(
    LOM_Handle*     handle,                     /* IN: LOM system handle */
    Four            sortStreamId                /* IN: sort stream id */
)
{
    Four e;

    e = LRDS_CloseSortStream(LOM_GET_LRDS_HANDLE(handle), sortStreamId);
    if(e < eNOERROR) LOM_ERROR(handle, e);

    return eNOERROR;
}

Four lom_Text_AddDocIdIndexEntryIntoSortStream(
    LOM_Handle* handle,                     /* IN: LOM system handle */
    Four        sortStreamId,               /* IN: sort stream id */
    Four        nPostings,                  /* IN: n of postings to add */
    char        *ptrToPostingList,          /* IN: posting list */
    TupleID     *tidForInvertedIndexEntry   /* IN: tid of index entry for given posting list */
)
{
    Four i;
    Four offset;
    Four logicalDocId;
    Four nPositions;
    Four e;
    Four postingLengthFieldSize = LOM_LONG_SIZE_VAR;
    Four postingLength;
    SortStreamTuple tuple;
    char data[sizeof(Four) + sizeof(TupleID)];

    offset = 0;
    for(i = 0; i < nPostings ;i++) {
        bcopy(&ptrToPostingList[offset + postingLengthFieldSize], &logicalDocId, sizeof(Four));
        bcopy(&ptrToPostingList[offset], &postingLength, sizeof(Four));

        memcpy(&data[0], &logicalDocId, sizeof(Four));
        memcpy(&data[0 + sizeof(Four)], tidForInvertedIndexEntry, sizeof(TupleID));
        tuple.len  = sizeof(Four) + sizeof(TupleID);
        tuple.data = data;

        e = LRDS_PutTuplesIntoSortStream(LOM_GET_LRDS_HANDLE(handle), sortStreamId, 1, &tuple);
        if(e < 0) LOM_ERROR(handle, e);

        offset += postingLength + postingLengthFieldSize;
    }

    return eNOERROR;
}

Four lom_Text_GetDocIdAndPointerFromSortStream(
    LOM_Handle      *handle,                    /* IN: system handle */
    Four            sortStreamId,               /* IN: sort stream id */
    Four            *docId,                     /* OUT: doc id (logical id) */
    TupleID         *tidForInvertedIndexEntry   /* OUT: tid of index entry associated with docid */
)
{
    Four e;
    Boolean            done = FALSE;        /* flag which indicates sort stream is empty or not */
    SortStreamTuple    sortTuple;           /* tuple for input sort stream */
    Four               numSortTuple;        /* # of sort tuple from sort stream. In this function, always 1 */
    char               data[sizeof(Four) + sizeof(TupleID)];

    numSortTuple   = 1;
    sortTuple.data = data;
    sortTuple.len  = sizeof(Four) + sizeof(TupleID);

    e = LRDS_GetTuplesFromSortStream(LOM_GET_LRDS_HANDLE(handle), sortStreamId, &numSortTuple, &sortTuple, &done);
    if(e < eNOERROR) LOM_ERROR(handle, e);

    if(done) return EOS;

    memcpy(docId, &data[0], sizeof(Four));
    memcpy(tidForInvertedIndexEntry, &data[0 + sizeof(Four)], sizeof(TupleID));

    return eNOERROR;
}


Four lom_Text_SortDocIdSortStream(
    LOM_Handle* handle,                     /* IN: LOM system handle */
    Four        sortStreamId                /* IN: sort stream id */
)
{
    Four e;

    e = LRDS_SortingSortStream(LOM_GET_LRDS_HANDLE(handle), sortStreamId);
    if(e < 0) LOM_ERROR(handle, e);

    return eNOERROR;
}

Four lom_Text_OpenSortStreamForPosting(
    LOM_Handle*     handle,                     /* IN: LOM system handle */
    Four            volId                       /* IN: volume Id */
)
{
    Four            e;
    Four            sortStreamIdForPosting;
    SortTupleDesc   sortTupleDesc;

    sortTupleDesc.hdrSize = 0;
    sortTupleDesc.nparts  = 3;
    /* keyword */
    sortTupleDesc.parts[0].type   = SM_VARSTRING;
    sortTupleDesc.parts[0].length = 0;
    sortTupleDesc.parts[0].flag   = SORTKEYDESC_ATTR_ASC;

    /* logical id */
    sortTupleDesc.parts[1].type   = LOM_LONG_VAR;
    sortTupleDesc.parts[1].length = LOM_LONG_SIZE_VAR;
    sortTupleDesc.parts[1].flag   = SORTKEYDESC_ATTR_ASC;

    sortTupleDesc.parts[2].type   = SM_SHORT;
    sortTupleDesc.parts[2].length = SM_SHORT_SIZE;
    sortTupleDesc.parts[2].flag   = SORTKEYDESC_ATTR_DESC;

    sortStreamIdForPosting = LRDS_OpenSortStream(LOM_GET_LRDS_HANDLE(handle), (Two)volId, &sortTupleDesc);
    if(sortStreamIdForPosting < 0) LOM_ERROR(handle, sortStreamIdForPosting);

    return sortStreamIdForPosting;
}

Four lom_Text_AddPostingIntoSortStream(
    LOM_Handle*         handle,                     /* IN: LOM system handle */
    Four                sortStreamId,               /* IN: sort stream id */
    Four                postingBufferLength,        /* IN: posting buffer length */
    char*               postingBuffer               /* IN: posting */
)
{
    SortStreamTuple tuple;
    char            data[LOM_PAGESIZE];
    char*           pData;
    char            keyword[LOM_MAXKEYWORDSIZE + 1];
    Four            postingLength;
    Two             keywordLength;
    Four            logicalId;
    Four            e;
    Two             splittedPartNo;
    Four            splittedDataSize;
    Four            remainDataSize;
    Four            nPartsTotal;
    char*           pPostingBuffer;
    Four            i;

    /* 
       the structure pointed by postingBuffer is as follows
       +--------------------------------+----------------+------------+-----+-------------------------+
       | zero terminated keyword string | posting length | logical id | tid | <position informations> |
       +--------------------------------+----------------+------------+-----+-------------------------+
       and the structure we are going to construct is as follows
       +-----------------+---------+-----------+------------------+----------------+------------+-----+-------------------------+
       | sizeof(keyword) | keyword | logicalid | splitted part no | posting length | logical id | tid | <position informations> |
       +-----------------+---------+-----------+------------------+----------------+------------+-----+-------------------------+
    */
    /* read keyword, postinglength, logicalId from postingBuffer */
    strcpy(keyword, postingBuffer);
    keywordLength = strlen(keyword) + 1;
    postingBuffer += keywordLength;

    memcpy(&postingLength, postingBuffer, sizeof(Four));    /* posting length means the length of posting which
                                                               consists of logicalid, tid, position informations */
    postingBuffer += sizeof(Four);

    memcpy(&logicalId, postingBuffer, sizeof(Four));        
    
    tuple.len  = sizeof(Two) + keywordLength + sizeof(Four) + sizeof(Two) + sizeof(Four) + postingLength;
    if(tuple.len <= sizeof(data)) 
    {
        /* write into data */
        /* construct sort key */
        splittedPartNo = 0;
        pData = data;
        memcpy(pData, &keywordLength, sizeof(Two)); pData += sizeof(Two);
        memcpy(pData, keyword, keywordLength); pData += keywordLength;
        memcpy(pData, &logicalId, sizeof(Four)); pData += sizeof(Four);
        memcpy(pData, &splittedPartNo, sizeof(Two)); pData += sizeof(Two);

        /* append information */
        memcpy(pData, &postingLength, sizeof(Four)); pData += sizeof(Four);
        memcpy(pData, postingBuffer, postingLength);

        tuple.data = data;

        e = LRDS_PutTuplesIntoSortStream(LOM_GET_LRDS_HANDLE(handle), sortStreamId, 1, &tuple);
        if(e < 0) LOM_ERROR(handle, e);
    }
    else
    {
        splittedDataSize = sizeof(data) * 2 / 3;    
        remainDataSize   = postingLength % splittedDataSize;
        nPartsTotal      = postingLength / splittedDataSize;
        pPostingBuffer   = postingBuffer;
        for(i = 0; i < nPartsTotal; i++)
        {
            /* construct sort key */
            splittedPartNo = nPartsTotal - i;
            pData = data;
            memcpy(pData, &keywordLength, sizeof(Two)); pData += sizeof(Two);
            memcpy(pData, keyword, keywordLength); pData += keywordLength;
            memcpy(pData, &logicalId, sizeof(Four)); pData += sizeof(Four);
            memcpy(pData, &splittedPartNo, sizeof(Two)); pData += sizeof(Two);

            /* append information */
            memcpy(pData, &postingLength, sizeof(Four)); pData += sizeof(Four);
            memcpy(pData, pPostingBuffer, splittedDataSize);
            pPostingBuffer += splittedDataSize;

            tuple.len  = sizeof(Two) + keywordLength + sizeof(Four) + sizeof(Two) + sizeof(Four) + splittedDataSize;
            tuple.data = data;

            e = LRDS_PutTuplesIntoSortStream(LOM_GET_LRDS_HANDLE(handle), sortStreamId, 1, &tuple);
            if(e < 0) LOM_ERROR(handle, e);
        }

        if(remainDataSize > 0)
        {
            /* construct sort key */
            splittedPartNo = nPartsTotal - i;
            pData = data;
            memcpy(pData, &keywordLength, sizeof(Two)); pData += sizeof(Two);
            memcpy(pData, keyword, keywordLength); pData += keywordLength;
            memcpy(pData, &logicalId, sizeof(Four)); pData += sizeof(Four);
            memcpy(pData, &splittedPartNo, sizeof(Two)); pData += sizeof(Two);

            /* append information */
            memcpy(pData, &postingLength, sizeof(Four)); pData += sizeof(Four);
            memcpy(pData, pPostingBuffer, remainDataSize);

            tuple.len  = sizeof(Two) + keywordLength + sizeof(Four) + sizeof(Two) + sizeof(Four) + remainDataSize;
            tuple.data = data;

            e = LRDS_PutTuplesIntoSortStream(LOM_GET_LRDS_HANDLE(handle), sortStreamId, 1, &tuple);
            if(e < 0) LOM_ERROR(handle, e);
        }
    }

    return eNOERROR;
}

Four lom_Text_GetPostingFromSortStream(
    LOM_Handle*         handle,                     /* IN: LOM system handle */
    Four                sortStreamId,               /* IN: sort stream id */
    Four                postingBufferLength,        /* IN: posting buffer length */
    char*               postingBuffer               /* OUT: posting */
)
{
    Four                e;
    Boolean             done = FALSE;        /* flag which indicates sort stream is empty or not */
    SortStreamTuple     sortTuple;           /* tuple for input sort stream */
    Four                numSortTuple;        /* # of sort tuple from sort stream. In this function, always 1 */
    char                data[LOM_PAGESIZE];
    char                keyword[LOM_MAXKEYWORDSIZE + 1];
    Two                 keywordLength;
    Four                postingLength;
    Four                logicalId;
    Two                 splittedPartNo;
    char*               pData;
    char*               pPostingBuffer;
    Four                splittedDataLength;
    Four                i;
    Four                nParts;

    numSortTuple   = 1;
    sortTuple.data = data;
    sortTuple.len  = sizeof(data);

    e = LRDS_GetTuplesFromSortStream(LOM_GET_LRDS_HANDLE(handle), sortStreamId, &numSortTuple, &sortTuple, &done);
    if(e < eNOERROR) LOM_ERROR(handle, e);

    if(done) return EOS;

    pData = data;
    pPostingBuffer = postingBuffer;
    memcpy(&keywordLength, pData, sizeof(Two)); pData += sizeof(Two);
    memcpy(keyword, pData, keywordLength); pData += keywordLength; 
    memcpy(pPostingBuffer, keyword, keywordLength); pPostingBuffer += keywordLength;
    memcpy(&logicalId, pData, sizeof(Four)); pData += sizeof(Four);
    memcpy(&splittedPartNo, pData, sizeof(Two)); pData += sizeof(Two);
    memcpy(&postingLength, pData, sizeof(Four));
    if(postingBufferLength < (sizeof(Two) + keywordLength + sizeof(Four) + postingLength))
        LOM_ERROR(handle, eMOREMEMORYREQUIRED_LOM);
    if(splittedPartNo == 0) 
        memcpy(pPostingBuffer, pData, sizeof(Four) + postingLength);
    else
    {
        splittedDataLength = sortTuple.len - (sizeof(Two) + keywordLength + sizeof(Four) + sizeof(Two) + sizeof(Four));
        memcpy(pPostingBuffer, pData, sizeof(Four) + splittedDataLength);
        pPostingBuffer += sizeof(Four) + splittedDataLength;

        nParts = splittedPartNo;
        for(i = 0; i < nParts; i++)
        {
            e = LRDS_GetTuplesFromSortStream(LOM_GET_LRDS_HANDLE(handle), sortStreamId, &numSortTuple, &sortTuple, &done);
            if(e < eNOERROR) LOM_ERROR(handle, e);

            if(done)
                LOM_ERROR(handle, eINTERNAL_LOM);

            splittedDataLength = sortTuple.len - (sizeof(Two) + keywordLength + sizeof(Four) + sizeof(Two) + sizeof(Four));
            pData = data;
            pData += sizeof(Two);       /* skip keywordLength */
            pData += keywordLength;     /* skip keyword */
            pData += sizeof(Four);      /* skip logical id */
            pData += sizeof(Two);       /* skip splittedPartNo */
            pData += sizeof(Four);      /* skip postingLength */

            /* append splitted posting */
            memcpy(pPostingBuffer, pData, splittedDataLength);
            pPostingBuffer += splittedDataLength;
        }
    }

    return eNOERROR;
}

Four lom_Text_SortPostingSortStream(
    LOM_Handle* handle,                     /* IN: LOM system handle */
    Four        sortStreamId                /* IN: sort stream id */
)
{
    Four e;

    e = LRDS_SortingSortStream(LOM_GET_LRDS_HANDLE(handle), sortStreamId);
    if(e < 0) LOM_ERROR(handle, e);

    return eNOERROR;
}

Four lom_Text_CloseSortStreamForPosting(
    LOM_Handle* handle,                     /* IN: LOM system handle */
    Four        sortStreamId                /* IN: sort stream id */
)
{
    Four e;

    e = LRDS_CloseSortStream(LOM_GET_LRDS_HANDLE(handle), sortStreamId);
    if(e < eNOERROR) LOM_ERROR(handle, e);

    return eNOERROR;
}

/************************************/
/* Catalog-Related Interface        */
/************************************/

/*
 * Function: Four lom_Text_CreateCatalogTable(handle, Four);
 *
 * Description:
 *  Create catalog table(LOM_SYSTEXTINDEXES )
 *
 * Retuns:
 *  error code
 */
Four lom_Text_CreateCatalogTable(
    LOM_Handle *handle, 
    Four volId
) 
{
    ColInfo lomSysTextIndexes[LOM_SYSTEXTINDEXES_NUM_COLS]; /* lomSysTextIndexes columns information */
    Four e;
    LRDS_IndexDesc idesc;
    IndexID iid;

    /* inverted index name */
    lomSysTextIndexes[LOM_SYSTEXTINDEXES_INVERTEDINDEXNAME_COLNO].complexType = SM_COMPLEXTYPE_BASIC;
    lomSysTextIndexes[LOM_SYSTEXTINDEXES_INVERTEDINDEXNAME_COLNO].type = SM_VARSTRING;
    lomSysTextIndexes[LOM_SYSTEXTINDEXES_INVERTEDINDEXNAME_COLNO].length = LOM_MAXINDEXNAME;

    /* index id for keyword column */
    lomSysTextIndexes[LOM_SYSTEXTINDEXES_KEYWORDINDEXID_COLNO].complexType = SM_COMPLEXTYPE_BASIC;
    lomSysTextIndexes[LOM_SYSTEXTINDEXES_KEYWORDINDEXID_COLNO].type = SM_INDEXID;
    lomSysTextIndexes[LOM_SYSTEXTINDEXES_KEYWORDINDEXID_COLNO].length = SM_INDEXID_SIZE;
    
    /* index id for reverse keyword column */
    lomSysTextIndexes[LOM_SYSTEXTINDEXES_REVKEYWORDINDEXID_COLNO].complexType = SM_COMPLEXTYPE_BASIC;
    lomSysTextIndexes[LOM_SYSTEXTINDEXES_REVKEYWORDINDEXID_COLNO].type = SM_INDEXID;
    lomSysTextIndexes[LOM_SYSTEXTINDEXES_REVKEYWORDINDEXID_COLNO].length = SM_INDEXID_SIZE;

    /* name of docId table name */
    lomSysTextIndexes[LOM_SYSTEXTINDEXES_DOCIDINDEXTABLENAME_COLNO].complexType = SM_COMPLEXTYPE_BASIC;
    lomSysTextIndexes[LOM_SYSTEXTINDEXES_DOCIDINDEXTABLENAME_COLNO].type = SM_VARSTRING;
    lomSysTextIndexes[LOM_SYSTEXTINDEXES_DOCIDINDEXTABLENAME_COLNO].length = LOM_MAXINDEXNAME;

    /* index id for docId column */
    lomSysTextIndexes[LOM_SYSTEXTINDEXES_DOCIDINDEXID_COLNO].complexType = SM_COMPLEXTYPE_BASIC;
    lomSysTextIndexes[LOM_SYSTEXTINDEXES_DOCIDINDEXID_COLNO].type = SM_INDEXID;
    lomSysTextIndexes[LOM_SYSTEXTINDEXES_DOCIDINDEXID_COLNO].length = SM_INDEXID_SIZE;
    
    lomSysTextIndexes[LOM_SYSTEXTINDEXES_COLUMNNO_COLNO].complexType = SM_COMPLEXTYPE_BASIC;
    lomSysTextIndexes[LOM_SYSTEXTINDEXES_COLUMNNO_COLNO].type = LOM_SHORT_VAR;
    lomSysTextIndexes[LOM_SYSTEXTINDEXES_COLUMNNO_COLNO].length = LOM_SHORT_SIZE_VAR;

    lomSysTextIndexes[LOM_SYSTEXTINDEXES_POSTINGSTRUCTUREINFO_COLNO].complexType = SM_COMPLEXTYPE_BASIC;
    lomSysTextIndexes[LOM_SYSTEXTINDEXES_POSTINGSTRUCTUREINFO_COLNO].type = SM_STRING;
    lomSysTextIndexes[LOM_SYSTEXTINDEXES_POSTINGSTRUCTUREINFO_COLNO].length = sizeof(PostingStructureInfo);

    /* create systextindex */
    e = LRDS_CreateRelation(LOM_GET_LRDS_HANDLE(handle), volId, LOM_SYSTEXTINDEXES_CLASSNAME, NULL, LOM_SYSTEXTINDEXES_NUM_COLS, &lomSysTextIndexes[0], SM_FALSE);
    if( e < eNOERROR) LOM_ERROR(handle, e);

    /* make index descriptor */
    idesc.indexType = SM_INDEXTYPE_BTREE;
    idesc.kinfo.btree.flag = KEYFLAG_UNIQUE;
    idesc.kinfo.btree.nColumns = 1;
    idesc.kinfo.btree.columns[0].colNo = LOM_SYSTEXTINDEXES_INVERTEDINDEXNAME_COLNO;
    idesc.kinfo.btree.columns[0].flag = KEYINFO_COL_ASC;

    /* add index on classname attribute of systextindex */
    e = LRDS_AddIndex(LOM_GET_LRDS_HANDLE(handle), volId, LOM_SYSTEXTINDEXES_CLASSNAME, &idesc, &iid);
    if( e < eNOERROR) LOM_ERROR(handle, e);

    return eNOERROR;
}

/*
 * Function: lom_Text_AddIndexInfoIntoCatalog(handle, Four, char *, char *, Two);
 *
 * Description:
 *  store information about inverted-index table into the catalog table(SYSTEXTINDEXES)
 *
 * Retuns:
 *  error code
 */
Four lom_Text_AddIndexInfoIntoCatalog(
    LOM_Handle *handle, 
    Four volId,     /* IN: volumn id */
    char *className, /* IN: class name */
    char *attrName, /* IN: attribute name */
    Two colNo   /* IN: column number */
)
{
    Four orn;
    IndexID revkeyword_iid;     /* index id for reverse-keyword */
    IndexID keyword_iid;        /* index id for keyword */
    IndexID docId_iid;      /* index id for doc-oid */
    char invertedIndexTableName[LOM_MAXCLASSNAME]; /* content class name */
    char docIdIndexTableName[LOM_MAXCLASSNAME];
    Four e;
    LRDS_IndexDesc idesc;       /* index descriptor */
    TupleID catalogTupleId;/* OUT: tuple id for newly created catalog tuple */
    ColListStruct clist[LOM_SYSTEXTINDEXES_NUM_COLS];
    LockParameter lockup;
    Four count;
    LOM_IndexID iid;
    PostingStructureInfo defaultPostingInfo;
    char                indexName[LOM_MAXINDEXNAME];

    /* make inverted index table name */
    sprintf(invertedIndexTableName, "_%s_%s_Inverted", className, attrName);

    /* make docId index table name */
    sprintf(docIdIndexTableName, "_%s_%s_docId", className, attrName);

    /* add index on inverted-index-table */
    /* index on keyword attribute */
    idesc.indexType = SM_INDEXTYPE_BTREE;
    idesc.kinfo.btree.flag = KEYFLAG_UNIQUE;
    idesc.kinfo.btree.nColumns = 1;
    idesc.kinfo.btree.columns[0].colNo = LOM_INVERTEDINDEX_KEYWORD_COLNO;
    idesc.kinfo.btree.columns[0].flag = KEYINFO_COL_ASC;
    e = LRDS_AddIndex(LOM_GET_LRDS_HANDLE(handle), volId, invertedIndexTableName, &idesc, &keyword_iid);
    if(e < eNOERROR) LOM_ERROR(handle, e);

    sprintf(indexName, "%s_Index_%ld", invertedIndexTableName, 1);
    e = Catalog_CreateIndexCatalog(handle, volId, invertedIndexTableName, indexName, &idesc, &keyword_iid);
    if(e < eNOERROR) LOM_ERROR(handle, e);

    /* open inverted index table for getting text index-id */
    orn = LRDS_OpenRelation(LOM_GET_LRDS_HANDLE(handle), volId, invertedIndexTableName);
    if(orn < eNOERROR) LOM_ERROR(handle, orn);

    /* get index-id for reverse-keyword */
    e =  LRDS_Text_GetIndexID(LOM_GET_LRDS_HANDLE(handle), orn, LOM_INVERTEDINDEX_REVKEYWORD_COLNO, &revkeyword_iid);
    if(e < eNOERROR) LOM_ERROR(handle, e);

    /* close inverted index table */
    e = LRDS_CloseRelation(LOM_GET_LRDS_HANDLE(handle), orn);
    if(e < eNOERROR) LOM_ERROR(handle, e);

    /* open docId index table */
    orn = LRDS_OpenRelation(LOM_GET_LRDS_HANDLE(handle), volId, docIdIndexTableName);
    if(orn < eNOERROR) LOM_ERROR(handle, orn);

    /* get index-id for docid */
    e =  LRDS_Text_GetIndexID(LOM_GET_LRDS_HANDLE(handle), orn, LOM_DOCIDTABLE_DOCID_COLNO, &docId_iid);
    if(orn < eNOERROR) LOM_ERROR(handle, e);

    /* close docId index table */
    e = LRDS_CloseRelation(LOM_GET_LRDS_HANDLE(handle), orn);
    if(e < eNOERROR) LOM_ERROR(handle, e);

    /* update index information to system catlog, LOM_SYSTEXTINDEXES */
    orn = LRDS_OpenRelation(LOM_GET_LRDS_HANDLE(handle), volId, LOM_SYSTEXTINDEXES_CLASSNAME);
    if(orn < eNOERROR) LOM_ERROR(handle, orn);
    
    count = 0;
    /* construct column struct list */
    /* inverted-index file name */
    clist[count].colNo = LOM_SYSTEXTINDEXES_INVERTEDINDEXNAME_COLNO;
    clist[count].start = ALL_VALUE;
    clist[count].dataLength = strlen(invertedIndexTableName);
    clist[count].data.ptr = invertedIndexTableName;
    clist[count].nullFlag = SM_FALSE;
    count++;

    /* keyword index */
    clist[count].colNo = LOM_SYSTEXTINDEXES_KEYWORDINDEXID_COLNO;
    clist[count].start = ALL_VALUE;
    clist[count].dataLength = sizeof(IndexID);
    clist[count].data.iid = keyword_iid;
    clist[count].nullFlag = SM_FALSE;
    count++;

    /* reverse-keyword index */
    clist[count].colNo = LOM_SYSTEXTINDEXES_REVKEYWORDINDEXID_COLNO;
    clist[count].start = ALL_VALUE;
    clist[count].dataLength = sizeof(IndexID);
    clist[count].data.iid = revkeyword_iid;
    clist[count].nullFlag = SM_FALSE;
    count++;

    /* docId index table name */
    clist[count].colNo = LOM_SYSTEXTINDEXES_DOCIDINDEXTABLENAME_COLNO;
    clist[count].start = ALL_VALUE;
    clist[count].dataLength = strlen(docIdIndexTableName);
    clist[count].data.ptr = docIdIndexTableName;
    clist[count].nullFlag = SM_FALSE;
    count++;

    /* docid index */
    clist[count].colNo = LOM_SYSTEXTINDEXES_DOCIDINDEXID_COLNO;
    clist[count].start = ALL_VALUE;
    clist[count].dataLength = sizeof(IndexID);
    clist[count].data.iid = docId_iid;
    clist[count].nullFlag = SM_FALSE;
    count++;

    /* column no */
    clist[count].colNo = LOM_SYSTEXTINDEXES_COLUMNNO_COLNO;
    clist[count].start = ALL_VALUE;
    clist[count].dataLength = LOM_SHORT_SIZE_VAR;
#ifdef TRACE
    printf("In lom_Text_AddIndexInfoIntoCatalog , colNo = %ld\n", colNo);
#endif
    ASSIGN_VALUE_TO_COL_LIST(clist[count], colNo, sizeof(Two));
    clist[count].nullFlag = SM_FALSE;
    count++;

    /* posting structure */
    defaultPostingInfo.isContainingTupleID = SM_TRUE;
    defaultPostingInfo.isContainingSentenceAndWordNum = SM_TRUE;
    defaultPostingInfo.isContainingByteOffset = SM_FALSE;
    defaultPostingInfo.nEmbeddedAttributes = 0;

    clist[count].colNo = LOM_SYSTEXTINDEXES_POSTINGSTRUCTUREINFO_COLNO;
    clist[count].start = ALL_VALUE;
    clist[count].dataLength = sizeof(PostingStructureInfo);
    clist[count].data.ptr = &defaultPostingInfo;
    clist[count].nullFlag = SM_FALSE;
    count++;

    /* create systextindex catlog tuple */
    e = LRDS_CreateTuple(LOM_GET_LRDS_HANDLE(handle), orn, SM_FALSE, count, clist, &catalogTupleId);
    if(e < eNOERROR) LOM_ERROR(handle, e);

    /* close relation */
    e = LRDS_CloseRelation(LOM_GET_LRDS_HANDLE(handle), orn);
    if(e < eNOERROR) LOM_ERROR(handle, e);

    /* SYSINDEXED */
    orn = LRDS_OpenRelation(LOM_GET_LRDS_HANDLE(handle), volId, LOM_SYSINDEXES_CLASSNAME);
    if (orn < 0) LOM_ERROR(handle, orn);

    /* Construct 'clist'. */
    clist[LOM_SYSINDEXES_INDEXNAME_COLNO].colNo = LOM_SYSINDEXES_INDEXNAME_COLNO;
    clist[LOM_SYSINDEXES_INDEXNAME_COLNO].start = ALL_VALUE;
    clist[LOM_SYSINDEXES_INDEXNAME_COLNO].dataLength = strlen(invertedIndexTableName);
    clist[LOM_SYSINDEXES_INDEXNAME_COLNO].data.ptr = invertedIndexTableName;
    clist[LOM_SYSINDEXES_INDEXNAME_COLNO].nullFlag = SM_FALSE;

    clist[LOM_SYSINDEXES_CLASSNAME_COLNO].colNo = LOM_SYSINDEXES_CLASSNAME_COLNO;
    clist[LOM_SYSINDEXES_CLASSNAME_COLNO].start = ALL_VALUE;
    clist[LOM_SYSINDEXES_CLASSNAME_COLNO].dataLength = strlen(className);
    clist[LOM_SYSINDEXES_CLASSNAME_COLNO].data.ptr = className;
    clist[LOM_SYSINDEXES_CLASSNAME_COLNO].nullFlag = SM_FALSE;

    clist[LOM_SYSINDEXES_INDEXID_COLNO].colNo = LOM_SYSINDEXES_INDEXID_COLNO;
    clist[LOM_SYSINDEXES_INDEXID_COLNO].start = ALL_VALUE;
    clist[LOM_SYSINDEXES_INDEXID_COLNO].dataLength = sizeof(LOM_IndexID);
    iid.isLogical = SM_TRUE;
    iid.index.logical_iid = catalogTupleId;
    clist[LOM_SYSINDEXES_INDEXID_COLNO].data.ptr = &iid;
    clist[LOM_SYSINDEXES_INDEXID_COLNO].nullFlag = SM_FALSE;

    e = LRDS_CreateTuple(LOM_GET_LRDS_HANDLE(handle), orn, SM_FALSE, LOM_SYSINDEXES_NUM_COLS, &(clist[0]), (TupleID*)NULL);
    if (e < 0) LOM_ERROR(handle, e);

    /* Close the LOM_SYSINDEXES file */
    e = LRDS_CloseRelation(LOM_GET_LRDS_HANDLE(handle), orn);
    if (e < 0) LOM_ERROR(handle, e);

    return eNOERROR;
}

Four lom_Text_DropIndexInfoFromCatalog(
    LOM_Handle *handle, 
    Four volId,     /* IN: volumn id */
    char *className, /* IN: class name */
    char *attrName  /* IN: attribute name */
)
{
    char invertedIndexTableName[LOM_MAXCLASSNAME]; /* content class name */
    BoundCond bound; /* bound condition */
    LockParameter lockup;
    Four orn;
    Four osn;
    lrds_RelTableEntry *relTableEntry;
    TupleID tid;     /* tid for sysindex */
    LOM_IndexID iid;    /* tid for systextindex */
    LRDS_Cursor *cursor;
    ColListStruct clist[3];
    Four e;
    Two keyLen;

    
    /* make inverted index table name */
    sprintf(invertedIndexTableName, "_%s_%s_Inverted", className, attrName);

    bound.op = SM_EQ;
    keyLen = strlen(invertedIndexTableName);
    bound.key.len = sizeof(Two) + keyLen;
    bcopy(&keyLen,&(bound.key.val[0]),sizeof(Two));
    bcopy(invertedIndexTableName,&(bound.key.val[sizeof(Two)]),keyLen);

    lockup.mode = L_IX;
    lockup.duration = L_COMMIT;

    
    /* SYSINDEXED */
    orn = LRDS_OpenRelation(LOM_GET_LRDS_HANDLE(handle), volId, LOM_SYSINDEXES_CLASSNAME);
    if (orn < 0) LOM_ERROR(handle, orn);

    relTableEntry = LRDS_GET_RELTABLE_ENTRY(LOM_GET_LRDS_HANDLE(handle), orn);

    osn = LRDS_OpenIndexScan(LOM_GET_LRDS_HANDLE(handle), orn, &(LRDS_GET_IDXINFO_FROM_RELTABLE_ENTRY(relTableEntry))[0].iid, &bound, &bound, 0, (BoolExp*)NULL, &lockup);
    if( osn < eNOERROR) LOM_ERROR(handle, osn);

    e = LRDS_NextTuple(LOM_GET_LRDS_HANDLE(handle), osn, &tid, &cursor);
    if( e < eNOERROR) LOM_ERROR(handle, e);
    
    if(e == EOS) LOM_ERROR(handle, eNOSUCHINDEXENTRY_LOM);

    clist[0].colNo = LOM_SYSINDEXES_INDEXID_COLNO;
    clist[0].start = ALL_VALUE;
    clist[0].length = sizeof(LOM_IndexID);
    clist[0].dataLength = sizeof(LOM_IndexID);
    clist[0].data.ptr = &iid;

    e = LRDS_FetchTuple(LOM_GET_LRDS_HANDLE(handle), osn, SM_TRUE, &tid, 1, &clist[0]);
    if( e < eNOERROR) LOM_ERROR(handle, e);

    e = LRDS_DestroyTuple(LOM_GET_LRDS_HANDLE(handle), osn, SM_TRUE, &tid);
    if( e < eNOERROR) LOM_ERROR(handle, e);

    e = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), osn);
    if( e < eNOERROR) LOM_ERROR(handle, e);

    e = LRDS_CloseRelation(LOM_GET_LRDS_HANDLE(handle), orn);
    if( e < eNOERROR) LOM_ERROR(handle, e);

    /* SYSTEXTINDEXES */
    orn = LRDS_OpenRelation(LOM_GET_LRDS_HANDLE(handle), volId, LOM_SYSTEXTINDEXES_CLASSNAME);
    if(orn < eNOERROR) LOM_ERROR(handle, orn);

    e = LRDS_DestroyTuple(LOM_GET_LRDS_HANDLE(handle), orn, SM_FALSE, &(iid.index.logical_iid));
    if( e < eNOERROR) LOM_ERROR(handle, e);

    e = LRDS_CloseRelation(LOM_GET_LRDS_HANDLE(handle), orn);
    if( e < eNOERROR) LOM_ERROR(handle, e);

    return eNOERROR;
}


/*************************************/
/* Schema-Creation/Deletion Interface*/
/*************************************/
/*
 * Function: Four lom_Text_CreateContentTable(handle, Four, char *);
 *
 * Description:
 *  Create content table for storing content of text type's attribute
 *
 * Retuns:
 *  error code
 */
Four lom_Text_CreateContentTable(
    LOM_Handle *handle, 
    Four volId,     /* IN: volumn id */
    char *className /* IN: class name */
)
{
    char contentTableName[LOM_MAXCLASSNAME]; /* content class name */
    Four e;     /* error code */
    ColInfo cinfo[LOM_CONTENT_NUM_COLS];

    /* check parameters */
    if (volId < 0) LOM_ERROR(handle, eBADPARAMETER_LOM);

    if (className == NULL) LOM_ERROR(handle, eBADPARAMETER_LOM);

    /* make content table name */
    sprintf(contentTableName, "_%s_Content", className);

    /* attribute information */
    cinfo[LOM_CONTENT_CONTENT_COLNO].complexType = SM_COMPLEXTYPE_BASIC;
    cinfo[LOM_CONTENT_CONTENT_COLNO].type = SM_VARSTRING;
    cinfo[LOM_CONTENT_CONTENT_COLNO].length = LOM_MAXLARGEOBJECTSIZE;

    /* create relation */
    e = LRDS_CreateRelation(LOM_GET_LRDS_HANDLE(handle), volId, contentTableName, NULL, LOM_CONTENT_NUM_COLS, cinfo, SM_FALSE);
    if(e < eNOERROR) LOM_ERROR(handle, e);
    
    return eNOERROR;
}

/*
 * Function: Four lom_Text_DestroyContentTable(handle, Four, char *);
 *
 * Description:
 *  Destroy content table for storing content of text type's attribute
 *
 * Retuns:
 *  error code
 */
Four lom_Text_DestroyContentTable(
    LOM_Handle *handle, 
    Four volId, 
    char *className
)
{
    char contentTableName[LOM_MAXCLASSNAME];
    Four e;

    /* check parameters */
    if (volId < 0) LOM_ERROR(handle, eBADPARAMETER_LOM);
    if (className == NULL) LOM_ERROR(handle, eBADPARAMETER_LOM);

    /* make content table name */
    sprintf(contentTableName, "_%s_Content", className);

    /* destroy content table */
    e = LRDS_DestroyRelation(LOM_GET_LRDS_HANDLE(handle), volId, contentTableName);
    if(e < eNOERROR) LOM_ERROR(handle, e);

    return eNOERROR;
}

/*
 * Function: Four lom_Text_CreateInvertedIndexTable(handle, Four, char *, char *, Two);
 *
 * Description:
 *  Create inverted index table which is logical index for text type's attribute 
 *
 * Retuns:
 *  error code
 */
Four lom_Text_CreateInvertedIndexTable(
    LOM_Handle *handle, 
    Four volId,     /* IN: volumn id */
    char *className, /* IN: class name */
    char *attrName, /* IN: attribute name */
    Two colNo   /* IN: column number */
)
{
    char     invertedIndexTableName[LOM_MAXCLASSNAME]; /* content class name */
    Four     e;     /* error code */
    ColInfo  cinfo[LOM_INVERTEDINDEX_NUM_COLS];
    char*    attrNames[LOM_INVERTEDINDEX_NUM_COLS];
    Four     classId;
#ifdef SUBINDEX
    OrderedSetAuxColInfo_T orderedSetAuxColInfo;
#endif

    /* check parameters */
    if (volId < 0) LOM_ERROR(handle, eBADPARAMETER_LOM);
    if (className == NULL) LOM_ERROR(handle, eBADPARAMETER_LOM);
    if (attrName == NULL) LOM_ERROR(handle, eBADPARAMETER_LOM);

    /* make inverted index table name */
    sprintf(invertedIndexTableName, "_%s_%s_Inverted", className, attrName);

    /* attribute information */
    /* keyword */
    cinfo[LOM_INVERTEDINDEX_KEYWORD_COLNO].complexType = SM_COMPLEXTYPE_BASIC;
    cinfo[LOM_INVERTEDINDEX_KEYWORD_COLNO].type = SM_VARSTRING;
    cinfo[LOM_INVERTEDINDEX_KEYWORD_COLNO].length = LOM_MAXKEYWORDSIZE;
    attrNames[LOM_INVERTEDINDEX_KEYWORD_COLNO] = LOM_INVERTEDINDEX_KEYWORD_COLNAME;

    /* reverse keyword */
    cinfo[LOM_INVERTEDINDEX_REVKEYWORD_COLNO].complexType = SM_COMPLEXTYPE_BASIC;
    cinfo[LOM_INVERTEDINDEX_REVKEYWORD_COLNO].type = SM_TEXT;
    cinfo[LOM_INVERTEDINDEX_REVKEYWORD_COLNO].length = LOM_MAXKEYWORDSIZE;
    attrNames[LOM_INVERTEDINDEX_REVKEYWORD_COLNO] = LOM_INVERTEDINDEX_REVKEYWORD_COLNAME;

    /* number of postings */
    cinfo[LOM_INVERTEDINDEX_NPOSTINGS_COLNO].complexType = SM_COMPLEXTYPE_BASIC;
    cinfo[LOM_INVERTEDINDEX_NPOSTINGS_COLNO].type = LOM_LONG_VAR;
    cinfo[LOM_INVERTEDINDEX_NPOSTINGS_COLNO].length = LOM_LONG_SIZE_VAR;
    attrNames[LOM_INVERTEDINDEX_NPOSTINGS_COLNO] = LOM_INVERTEDINDEX_NPOSTINGS_COLNAME;
    
    /* size of posting list  */
    cinfo[LOM_INVERTEDINDEX_SIZEOFPOSTING_COLNO].complexType = SM_COMPLEXTYPE_BASIC;
    cinfo[LOM_INVERTEDINDEX_SIZEOFPOSTING_COLNO].type = LOM_LONG_VAR;
    cinfo[LOM_INVERTEDINDEX_SIZEOFPOSTING_COLNO].length = LOM_LONG_SIZE_VAR;
    attrNames[LOM_INVERTEDINDEX_SIZEOFPOSTING_COLNO] = LOM_INVERTEDINDEX_SIZEOFPOSTING_COLNAME;

    /* posting list  */
#ifdef SUBINDEX
    cinfo[LOM_INVERTEDINDEX_POSTINGLIST_COLNO].complexType = SM_COMPLEXTYPE_ORDEREDSET;
#else
    cinfo[LOM_INVERTEDINDEX_POSTINGLIST_COLNO].complexType = SM_COMPLEXTYPE_BASIC;
#endif
    cinfo[LOM_INVERTEDINDEX_POSTINGLIST_COLNO].type = SM_VARSTRING;
    cinfo[LOM_INVERTEDINDEX_POSTINGLIST_COLNO].length = LOM_MAXLARGEOBJECTSIZE;
    attrNames[LOM_INVERTEDINDEX_POSTINGLIST_COLNO] = LOM_INVERTEDINDEX_POSTINGLIST_COLNAME;

#ifdef SUBINDEX
    cinfo[LOM_INVERTEDINDEX_POSTINGLIST_COLNO].auxInfo.orderedSet = &orderedSetAuxColInfo;
    orderedSetAuxColInfo.kdesc.flag = 0;
    orderedSetAuxColInfo.kdesc.nparts = 1;
    orderedSetAuxColInfo.kdesc.kpart[0].type = LOM_LONG_VAR;
    orderedSetAuxColInfo.kdesc.kpart[0].length = LOM_LONG_SIZE_VAR;
    orderedSetAuxColInfo.nestedIndexFlag = SM_TRUE;
#endif

    /* create relation */
    e = LRDS_CreateRelation(LOM_GET_LRDS_HANDLE(handle), volId, invertedIndexTableName, NULL, LOM_INVERTEDINDEX_NUM_COLS, cinfo, SM_FALSE);
    if(e < eNOERROR) LOM_ERROR(handle, e);

    /* create catalog */
    e = LOM_GetNewClassId(handle, volId, SM_FALSE, &classId);
    if(e < eNOERROR) LOM_ERROR(handle, e);

    e = Catalog_CreateLOMCatalogBasedOnLRDSCatalog(handle, volId, invertedIndexTableName, LOM_INVERTEDINDEX_NUM_COLS, attrNames, classId);
    if(e < eNOERROR) LOM_ERROR(handle, e);

    return eNOERROR;
}

/*
 * Function: Four lom_Text_DestroyInvertedIndexTable(handle, Four, char *, char *, Two, TupleID *);
 *
 * Description:
 *  Destroy inverted index table which is logical index for text type's attribute 
 *
 * Retuns:
 *  error code
 */
Four lom_Text_DestroyInvertedIndexTable(
    LOM_Handle *handle, 
    Four volId,         /* IN: volumn id */
    char *className,    /* IN: class name */
    char *attrName      /* IN: attribute name */
)   
{
    Four orn;   /* open relation number */
    Four osn;   /* open scan number */
    LockParameter lockup;   /* lock parameter */
    char invertedIndexTableName[LOM_MAXCLASSNAME];
    Four e;

    /* check parameters */
    if (volId < 0) LOM_ERROR(handle, eBADPARAMETER_LOM);
    if (className == NULL) LOM_ERROR(handle, eBADPARAMETER_LOM);
    if (attrName == NULL) LOM_ERROR(handle, eBADPARAMETER_LOM);

    /* make inverted index table name */
    sprintf(invertedIndexTableName, "_%s_%s_Inverted", className, attrName);

    /* destroy class catalog */
    e = Catalog_DestroyClassCatalog(handle, volId, invertedIndexTableName);
    if(e < eNOERROR) LOM_ERROR(handle, e);

    /* destroy inverted index table */
    e = LRDS_DestroyRelation(LOM_GET_LRDS_HANDLE(handle), volId, invertedIndexTableName);
    if(e < eNOERROR) LOM_ERROR(handle, e);

    return eNOERROR;
}

/*
 * Function: Four lom_Text_CreateDocIdIndexTable(handle, Four, char *, char *, Two);
 *
 * Description:
 *  Create document id index table 
 *
 * Retuns:
 *  error code
 */
Four lom_Text_CreateDocIdIndexTable(
    LOM_Handle *handle, 
    Four volId,     /* IN: volumn id */
    char *className, /* IN: class name */
    char *attrName, /* IN: attribute name */
    Two colNo   /* IN: column number */
)
{
    char    docIdIndexTableName[LOM_MAXCLASSNAME]; /* content class name */
    Four    e;      /* error code */
    ColInfo cinfo[LOM_DOCIDTABLE_NUM_COLS];
    Four    classId;
    char*   attrNames[LOM_DOCIDTABLE_NUM_COLS];

    /* check parameters */
    if (volId < 0) LOM_ERROR(handle, eBADPARAMETER_LOM);
    if (className == NULL) LOM_ERROR(handle, eBADPARAMETER_LOM);
    if (attrName == NULL) LOM_ERROR(handle, eBADPARAMETER_LOM);

    /* make inverted index table name */
    sprintf(docIdIndexTableName, "_%s_%s_docId", className, attrName);

    /* attribute information */
    /* docId  */
    cinfo[LOM_DOCIDTABLE_DOCID_COLNO].complexType = SM_COMPLEXTYPE_BASIC;
    cinfo[LOM_DOCIDTABLE_DOCID_COLNO].type = SM_TEXT;
    cinfo[LOM_DOCIDTABLE_DOCID_COLNO].length = LOM_MAXLARGEOBJECTSIZE;
    attrNames[LOM_DOCIDTABLE_DOCID_COLNO] = LOM_DOCIDTABLE_DOCID_COLNAME;

    /* posting list  */
    cinfo[LOM_DOCIDTABLE_POINTERLIST_COLNO].complexType = SM_COMPLEXTYPE_BASIC;
    cinfo[LOM_DOCIDTABLE_POINTERLIST_COLNO].type = SM_VARSTRING;
    cinfo[LOM_DOCIDTABLE_POINTERLIST_COLNO].length = LOM_MAXLARGEOBJECTSIZE;
    attrNames[LOM_DOCIDTABLE_POINTERLIST_COLNO] = LOM_DOCIDTABLE_POINTERLIST_COLNAME;

    /* create relation */
    e = LRDS_CreateRelation(LOM_GET_LRDS_HANDLE(handle), volId, docIdIndexTableName, NULL, LOM_DOCIDTABLE_NUM_COLS, cinfo, SM_FALSE);
    if(e < eNOERROR) LOM_ERROR(handle, e);

    /* create catalog */
    e = LOM_GetNewClassId(handle, volId, SM_FALSE, &classId);
    if(e < eNOERROR) LOM_ERROR(handle, e);

    e = Catalog_CreateLOMCatalogBasedOnLRDSCatalog(handle, volId, docIdIndexTableName, LOM_DOCIDTABLE_NUM_COLS, attrNames, classId);
    if(e < eNOERROR) LOM_ERROR(handle, e);

    return eNOERROR;
}

/*
 * Function: Four lom_Text_DestroyDocIdIndexTable(handle, Four, char *, char *)
 *
 * Description:
 *  Destroy document-id index table 
 *
 * Retuns:
 *  error code
 */
Four lom_Text_DestroyDocIdIndexTable(
    LOM_Handle *handle, 
    Four volId,         /* IN: volumn id */
    char *className,    /* IN: class name */
    char *attrName      /* IN: attribute name */
)   
{
    Four orn;   /* open relation number */
    Four osn;   /* open scan number */
    LockParameter lockup;   /* lock parameter */
    char docIdIndexTableName[LOM_MAXCLASSNAME];
    Four e;

    /* check parameters */
    if (volId < 0) LOM_ERROR(handle, eBADPARAMETER_LOM);
    if (className == NULL) LOM_ERROR(handle, eBADPARAMETER_LOM);
    if (attrName == NULL) LOM_ERROR(handle, eBADPARAMETER_LOM);

    /* make inverted index table name */
    sprintf(docIdIndexTableName, "_%s_%s_docId", className, attrName);

    /* destroy class catalog */
    e = Catalog_DestroyClassCatalog(handle, volId, docIdIndexTableName);
    if(e < eNOERROR) LOM_ERROR(handle, e);

    /* destroy inverted index table */
    e = LRDS_DestroyRelation(LOM_GET_LRDS_HANDLE(handle), volId, docIdIndexTableName);
    if(e < eNOERROR) LOM_ERROR(handle, e);

    return eNOERROR;
}

/**************************************/
/* Text-Object Manipulation Interface */
/**************************************/

/*
 * Function: Four lom_Text_CreateContentData(handle, Four, Four, char *, TupleID *);
 *
 * Description:
 *  Create content data
 *
 * Retuns:
 *  error code
 */
Four lom_Text_CreateContentData(
    LOM_Handle *handle, 
    Four ocnOrScanId,       /* IN: ocn or scan id */
    Boolean useScanFlag,        /* IN: flag */
    Four dataLength,    /* IN: length of data */
    char *data,         /* IN: data */
    TupleID *tid        /* OUT: tuple id */
)
{
    ColListStruct clist[LOM_CONTENT_NUM_COLS];
    Four orn;
    Four scanId;
    Four contentOrn;

#ifdef COMPRESSION
	Four e;
	char *compressedData;
	Four compressedDataLength;
#endif

    if(useScanFlag) {
        scanId =  LOM_SCANTABLE(handle)[ocnOrScanId].lrdsScanId;
        orn = LRDS_SCANTABLE(LOM_GET_LRDS_HANDLE(handle))[scanId].orn;
    }
    else orn = ocnOrScanId;

    contentOrn = LOM_USEROPENCLASSTABLE(handle)[orn].ornForContentTable;
	
#ifdef COMPRESSION
    compressedDataLength = compressBound(dataLength);
    compressedData = (char*)malloc(compressedDataLength + sizeof(char));
    if(compressedData == NULL) LOM_ERROR(handle, eOUTOFMEMORY_LOM);

	e = lom_Text_Compression(handle, data, dataLength, compressedData, &compressedDataLength);
	if(e < eNOERROR) 
	{
		if(compressedData != NULL) free(compressedData);
		LOM_ERROR(handle, e);
	}
#endif

    /* construct column struct list */
    clist[0].colNo = LOM_CONTENT_CONTENT_COLNO;
    clist[0].start = ALL_VALUE;

#ifdef COMPRESSION
	clist[0].dataLength = compressedDataLength;
	clist[0].data.ptr = compressedData;
#else
    clist[0].dataLength = dataLength;
    clist[0].data.ptr = data;
#endif

    clist[0].nullFlag = SM_FALSE;

#ifdef COMPRESSION
	e = LRDS_CreateTuple(LOM_GET_LRDS_HANDLE(handle), contentOrn, SM_FALSE, 1, clist, tid);
	if(e < eNOERROR) 
	{
		if(compressedData != NULL) free(compressedData);
		LOM_ERROR(handle, e); 
	}

	if(compressedData != NULL) free(compressedData);

	return eNOERROR;
#else
	return LRDS_CreateTuple(LOM_GET_LRDS_HANDLE(handle), contentOrn, SM_FALSE, 1, clist, tid);
#endif
}

Four lom_Text_DestroyContent(
    LOM_Handle *handle, 
    Four ocnOrScanId,   /* IN: ocn or scanId */
    Boolean useScanFlag,    /* IN: flag */
    OID *oid,       /* IN: oid */
    Two colNo,      /* IN: column number */
    LOM_TextDesc *textDesc  /* IN: text descriptor */
)
{
    Four contentOrn;
    Four e;
    Four logicalDocId;  
    Four orn;
    Four scanId;

    /* check parameters */
    if(ocnOrScanId < 0 || oid == NULL || colNo < 0 || textDesc == NULL) LOM_ERROR(handle, eBADPARAMETER_LOM);

    /* no content for the given oid */
    if(DOES_NOCONTENT_EXIST_TEXTDESC(*textDesc)) LOM_ERROR(handle, eBADPARAMETER_LOM);

    if(useScanFlag) {
        scanId =  LOM_SCANTABLE(handle)[ocnOrScanId].lrdsScanId;
        orn = LRDS_SCANTABLE(LOM_GET_LRDS_HANDLE(handle))[scanId].orn;
    }
    else orn = ocnOrScanId;

    contentOrn = LOM_USEROPENCLASSTABLE(handle)[orn].ornForContentTable;

    /* get logical id */
    logicalDocId = lom_Text_GetLogicalId(handle, ocnOrScanId, useScanFlag, oid);
    if(logicalDocId < 0) LOM_ERROR(handle, logicalDocId);

    /* delete index entry from inverted index table and posting table */
    e = lom_Text_RemoveInvertedIndexEntry(handle, ocnOrScanId, useScanFlag, logicalDocId, colNo);
    if(e < eNOERROR) LOM_ERROR(handle, e);

    e = lom_Text_RemoveDocIdIndexEntry(handle, ocnOrScanId, useScanFlag, logicalDocId, colNo);
    if(e < eNOERROR) LOM_ERROR(handle, e);

    /* delete content object */
    e = LRDS_DestroyTuple(LOM_GET_LRDS_HANDLE(handle), contentOrn, SM_FALSE, &(textDesc->contentTid));
    if(e < eNOERROR) LOM_ERROR(handle, e);

    return eNOERROR;
}

/*
 * Function: Four lom_Text_UpdateContentData(handle, Four, TupleID *, Four, Four, Four, char *);
 *
 * Description:
 *  Update content data
 *
 * Retuns:
 *  error code
 */
Four lom_Text_UpdateContentData(
    LOM_Handle *handle, 
    Four ocnOrScanId,       /* IN: open scan number */
    Boolean useScanFlag,        /* IN: flag */
    TupleID *tid,       /* IN: tuple id */
    Four start,     /* IN: start of data */
    Four length,        /* IN: length of to-be-updated data */
    Four dataLength,    /* IN: length of data */
    char *data      /* IN: data */
)
{
    ColListStruct clist[LOM_CONTENT_NUM_COLS];
    Four orn;
    Four contentOrn;
    Four scanId;

#ifdef COMPRESSION
	Four e;
	char *compressedData;
	Four compressedDataLength;
#endif

    if(useScanFlag) {
        scanId =  LOM_SCANTABLE(handle)[ocnOrScanId].lrdsScanId;
        orn = LRDS_SCANTABLE(LOM_GET_LRDS_HANDLE(handle))[scanId].orn;
    }
    else orn = ocnOrScanId;

    contentOrn = LOM_USEROPENCLASSTABLE(handle)[orn].ornForContentTable;

#ifdef COMPRESSION
    compressedDataLength = compressBound(dataLength);
    compressedData = (char*)malloc(compressedDataLength + sizeof(char));
    if(compressedData == NULL) LOM_ERROR(handle, eOUTOFMEMORY_LOM);
    
	e = lom_Text_Compression(handle, data, dataLength, compressedData, &compressedDataLength);
	if(e < eNOERROR) 
	{
		if(compressedData != NULL) free(compressedData);
		LOM_ERROR(handle, e);
	}
#endif

    /* construct column struct list */
    clist[0].colNo = LOM_CONTENT_CONTENT_COLNO;
    clist[0].start = start;
    clist[0].length = length;

#ifdef COMPRESSION
	clist[0].dataLength = compressedDataLength;
	clist[0].data.ptr = compressedData;
#else
    clist[0].dataLength = dataLength;
    clist[0].data.ptr = data;
#endif
    clist[0].nullFlag = SM_FALSE;

#ifdef COMPRESSION
	e = LRDS_UpdateTuple(LOM_GET_LRDS_HANDLE(handle), contentOrn, SM_FALSE, tid, 1, clist);
	if(e < eNOERROR) 
	{
		if(compressedData != NULL) free(compressedData);
		LOM_ERROR(handle, e); 
	}

	if(compressedData != NULL) free(compressedData);

	return eNOERROR;
#else
    return LRDS_UpdateTuple(LOM_GET_LRDS_HANDLE(handle), contentOrn, SM_FALSE, tid, 1, clist);
#endif
}

/*
 * Function: Four lom_Text_FetchContentData(handle, Four, TupleID *, Four, Four, Four, char *);
 *
 * Description:
 *  Fetch content data
 *
 * Retuns:
 *  error code
 */
Four lom_Text_FetchContentData(
    LOM_Handle *handle, 
    Four ocnOrScanId,       /* IN: open scan number */
    Boolean useScanFlag,        /* IN: flag */
    TupleID *tid,       /* IN: tuple id */
    Four start,     /* IN: start of data */
    Four length,        /* IN: length of to-be-updated data */
    Four dataLength,    /* IN: length of data */
    char *data      /* OUT: data */
)
{
    ColListStruct clist[LOM_CONTENT_NUM_COLS];
    Four e;
    Four orn;
    Four contentOrn;
    Four scanId;

#ifdef COMPRESSION
	char* uncompressedData = NULL;
	Four  uncompressedDataLength;
#endif

    if(useScanFlag) {
        scanId =  LOM_SCANTABLE(handle)[ocnOrScanId].lrdsScanId;
        orn = LRDS_SCANTABLE(LOM_GET_LRDS_HANDLE(handle))[scanId].orn;
    }
    else orn = ocnOrScanId;

    contentOrn = LOM_USEROPENCLASSTABLE(handle)[orn].ornForContentTable;

    /* construct column struct list */
    clist[0].colNo = LOM_CONTENT_CONTENT_COLNO;
    clist[0].start = start;
    clist[0].length = length;
    clist[0].dataLength = dataLength;
    clist[0].data.ptr = data;

    e = LRDS_FetchTuple(LOM_GET_LRDS_HANDLE(handle), contentOrn, SM_FALSE, tid, 1, clist);
    if(e < eNOERROR) LOM_ERROR(handle, e);

#ifdef COMPRESSION
	uncompressedDataLength = dataLength;

	e = lom_Text_Uncompression(handle, data, clist[0].retLength, &uncompressedData, &uncompressedDataLength);
	if(e < eNOERROR) 
	{
		if(uncompressedData != NULL) free(uncompressedData);
		LOM_ERROR(handle, e);
	}

	memcpy(data, uncompressedData, dataLength);
	if(uncompressedData != NULL) free(uncompressedData);

	clist[0].retLength = uncompressedDataLength;
#endif

	return clist[0].retLength;
}

/*******************************************/
/* Open Internal Tables                    */
/*******************************************/

/*
 * Function: Four lom_Text_OpenContentClass(handle, Four, char *);
 *
 * Description:
 *  Open content table for storing content of text type's attribute
 *
 * Retuns:
 *  error code
 */
Four lom_Text_OpenContentClass(
    LOM_Handle *handle, 
    Four volId,         /* IN: volumn id */
    char *className     /* IN: class name */
)
{
    char contentTableName[LOM_MAXCLASSNAME]; /* content class name */

    /* make content table name */
    sprintf(contentTableName, "_%s_Content", className);

    return LRDS_OpenRelation(LOM_GET_LRDS_HANDLE(handle), volId, contentTableName);
}

/*
 * Function: Four lom_Text_OpenInvertedIndexTable(handle, Four, char *, char *);
 *
 * Description:
 *  Open inverted index table which is logical index for text type's attribute 
 *
 * Retuns:
 *  error code
 */
Four lom_Text_OpenInvertedIndexTable(
    LOM_Handle *handle, 
    Four volId,         /* IN: volumn id */
    char *className,    /* IN: class name */
    char *attrName      /* IN: attribute name */
)
{
    char invertedIndexTableName[LOM_MAXCLASSNAME]; /* content class name */

    /* make inverted index table name */
    sprintf(invertedIndexTableName, "_%s_%s_Inverted", className, attrName);

    return LRDS_OpenRelation(LOM_GET_LRDS_HANDLE(handle), volId, invertedIndexTableName);
}

/*
 * Function: Four lom_Text_OpenDocIdIndexTable(handle, Four, char *, char *);
 *
 * Description:
 *  Open docId index table which is logical index for text type's attribute 
 *
 * Retuns:
 *  error code
 */
Four lom_Text_OpenDocIdIndexTable(
    LOM_Handle *handle,
    Four volId,         /* IN: volumn id */
    char *className,    /* IN: class name */
    char *attrName      /* IN: attribute name */
)
{
    char docIdIndexTableName[LOM_MAXCLASSNAME]; /* doc-id index table name */

    /* make docId index table name */
    sprintf(docIdIndexTableName, "_%s_%s_docId", className, attrName);

    return LRDS_OpenRelation(LOM_GET_LRDS_HANDLE(handle), volId, docIdIndexTableName);
}

/************************************/
/* Utility Interfaces               */
/************************************/

Four lom_Text_GetLogicalId(
    LOM_Handle *handle,
    Four ocnOrScanId,       /* IN: open scan number */
    Boolean useScanFlag,        /* IN: flag */
    OID *oid        /* IN: object id */
)
{
    ColListStruct clist[1]; /* column struct list */
    Four e;         /* error code */
    Four scanId;
    Four orn;

    if(useScanFlag) {
        scanId = LOM_SCANTABLE(handle)[ocnOrScanId].lrdsScanId;

        if (!LRDS_VALID_SCANID(LOM_GET_LRDS_HANDLE(handle), scanId)) LOM_ERROR(handle, eBADPARAMETER_LOM);

        orn = LRDS_SCANTABLE(LOM_GET_LRDS_HANDLE(handle))[scanId].orn;
    }
    else orn = ocnOrScanId;

    /* make column list struct */
    clist[0].colNo = 0;
    clist[0].start = ALL_VALUE;
    clist[0].length = LOM_LONG_SIZE_VAR;
    clist[0].dataLength = LOM_LONG_SIZE_VAR;

    /* get logical id from object */
    if(useScanFlag)
        e = LRDS_FetchTuple(LOM_GET_LRDS_HANDLE(handle), scanId, useScanFlag, (TupleID *)oid, 1, clist);
    else
        e = LRDS_FetchTuple(LOM_GET_LRDS_HANDLE(handle), orn, useScanFlag, (TupleID *)oid, 1, clist);
    if(e < eNOERROR) LOM_ERROR(handle, e);

    return GET_VALUE_FROM_COL_LIST(clist[0], sizeof(1L));
}

Four lom_Text_SetLogicalId(
    LOM_Handle *handle,
    Four ocnOrScanId,       /* IN: open scan number */
    Boolean useScanFlag,    /* IN: flag */
    OID *oid,               /* IN: object id */
    Four logicalId          /* IN: new logical id */
)
{
    ColListStruct clist[1]; /* column struct list */
    Four e;         /* error code */
    Four scanId;
    Four orn;

    if(useScanFlag) {
        scanId = LOM_SCANTABLE(handle)[ocnOrScanId].lrdsScanId;

        if (!LRDS_VALID_SCANID(LOM_GET_LRDS_HANDLE(handle), scanId)) LOM_ERROR(handle, eBADPARAMETER_LOM);

        orn = LRDS_SCANTABLE(LOM_GET_LRDS_HANDLE(handle))[scanId].orn;
    }
    else orn = ocnOrScanId;

    /* make column list struct */
    clist[0].colNo = 0;
    clist[0].start = ALL_VALUE;
    clist[0].length = LOM_LONG_SIZE_VAR;
    clist[0].dataLength = LOM_LONG_SIZE_VAR;
    ASSIGN_VALUE_TO_COL_LIST(clist[0], logicalId, sizeof(Four));

    /* get logical id from object */
    if(useScanFlag)
        e = LRDS_UpdateTuple(LOM_GET_LRDS_HANDLE(handle), scanId, useScanFlag, (TupleID *)oid, 1, clist);
    else
        e = LRDS_UpdateTuple(LOM_GET_LRDS_HANDLE(handle), orn, useScanFlag, (TupleID *)oid, 1, clist);
    if(e < eNOERROR) LOM_ERROR(handle, e);

    return eNOERROR;
}

Four lom_Text_GetInvertedIndexTableORN(
    LOM_Handle *handle,
    Four ocnOrScanId,
    Boolean useScanFlag,
    Two colNo       /* column number */
)
{
    Four i;     /* index variable */
    Four scanId;
    Four orn;

    if(useScanFlag) {
        scanId = LOM_SCANTABLE(handle)[ocnOrScanId].lrdsScanId;

        if (!LRDS_VALID_SCANID(LOM_GET_LRDS_HANDLE(handle), scanId)) LOM_ERROR(handle, eBADPARAMETER_LOM);

        orn = LRDS_SCANTABLE(LOM_GET_LRDS_HANDLE(handle))[scanId].orn;
    }
    else orn = ocnOrScanId;

    for(i = 0; i < LOM_USEROPENCLASSTABLE(handle)[orn].numOfTextAttrs; i++) {
        if(LOM_USEROPENCLASSTABLE(handle)[orn].textColNo[i] == colNo) 
            return LOM_USEROPENCLASSTABLE(handle)[orn].ornForInvertedIndexTable[i];
    }

    LOM_ERROR(handle, eINTERNAL_LOM);
}

Four lom_Text_GetDocIdIndexTableORN(
    LOM_Handle *handle,
    Four ocnOrScanId,               /* scan id */
    Boolean useScanFlag,        /* flag */
    Two colNo               /* column number */
)
{
    Four i;         /* index variable */
    Four orn;       /* open class number */
    Four scanId;

    if(useScanFlag) {
        scanId = LOM_SCANTABLE(handle)[ocnOrScanId].lrdsScanId;

        if (!LRDS_VALID_SCANID(LOM_GET_LRDS_HANDLE(handle), scanId)) LOM_ERROR(handle, eBADPARAMETER_LOM);

        orn = LRDS_SCANTABLE(LOM_GET_LRDS_HANDLE(handle))[scanId].orn;
    }
    else orn = ocnOrScanId;

    for(i = 0; i < LOM_USEROPENCLASSTABLE(handle)[orn].numOfTextAttrs; i++) {
        if(LOM_USEROPENCLASSTABLE(handle)[orn].textColNo[i] == colNo)
            return LOM_USEROPENCLASSTABLE(handle)[orn].ornForDocIdIndexTable[i];
    }

    LOM_ERROR(handle, eINTERNAL_LOM);
}

Four lom_Text_GetRevKeywordIndex(
    LOM_Handle *handle,
    Four ocnOrScanId,
    Boolean useScanFlag,
    Two colNo,
    IndexID *iid
)
{

    Four orn;   /* open class number */
    Four i;     /* index variable */
    Four e;     /* error code */
    catalog_SysClassesOverlay *sysCatalog;  /* pointer to SYSCLASSES */
    catalog_SysIndexesOverlay *ptrToSysIndexes; /* pointer to SYSINDEXES */
    lrds_RelTableEntry *relTableEntry; /* pointer to an entry of relation table */
    Four idxForClassInfo;
    Four v;
    Four scanId;

    if(useScanFlag) {
        scanId = LOM_SCANTABLE(handle)[ocnOrScanId].lrdsScanId;

        if (!LRDS_VALID_SCANID(LOM_GET_LRDS_HANDLE(handle), scanId)) LOM_ERROR(handle, eBADPARAMETER_LOM);

        orn = LRDS_SCANTABLE(LOM_GET_LRDS_HANDLE(handle))[scanId].orn;
    }
    else orn = ocnOrScanId;

    relTableEntry = LRDS_GET_RELTABLE_ENTRY(LOM_GET_LRDS_HANDLE(handle), orn);

    e = Catalog_GetClassInfo(handle, relTableEntry->ri.fid.volNo, LOM_USEROPENCLASSTABLE(handle)[orn].classID, &idxForClassInfo);
    if( e < eNOERROR) {
        LOM_ERROR(handle, e);
    }

    v = Catalog_GetVolIndex(handle, relTableEntry->ri.fid.volNo);
        if(v < eNOERROR) LOM_ERROR(handle, v);

    /* poiner to SYSCLASSES */
    sysCatalog = &CATALOG_GET_CLASSINFOTBL(handle, v)[idxForClassInfo];

    /* pointer to SYSINDEXES */
    ptrToSysIndexes = &CATALOG_GET_INDEXINFOTBL(handle, v)[CATALOG_GET_INDEXINFOTBL_INDEX(sysCatalog)];

    for( i = 0; i < CATALOG_GET_INDEXNUM(sysCatalog); i++)  {
        if(ptrToSysIndexes[i].indexType == LOM_INDEXTYPE_TEXT) {
            if(ptrToSysIndexes[i].colNo[0] == colNo) break;
        }
    }

    /* no such text-index built on the given column */
    if(i == CATALOG_GET_INDEXNUM(sysCatalog))
        LOM_ERROR(handle, eNOSUCHINDEX_LOM);

    /* copy index id */
    bcopy(&(ptrToSysIndexes[i].kdesc.invertedIndex.reverseKeywordIndex), iid, sizeof(IndexID));

    return eNOERROR;
}

Four lom_Text_GetKeywordIndex(
    LOM_Handle *handle,
    Four ocnOrScanId,
    Boolean useScanFlag,
    Two colNo,
    IndexID *iid
)
{

    Four orn;   /* open class number */
    Four i;     /* index variable */
    Four e;     /* error code */
    catalog_SysClassesOverlay *sysCatalog;  /* pointer to SYSCLASSES */
    catalog_SysIndexesOverlay *ptrToSysIndexes; /* pointer to SYSINDEXES */
    lrds_RelTableEntry *relTableEntry; /* pointer to an entry of relation table */
    Four idxForClassInfo;
    Four v;
    Four scanId;

    if(useScanFlag) {
        scanId = LOM_SCANTABLE(handle)[ocnOrScanId].lrdsScanId;

        if (!LRDS_VALID_SCANID(LOM_GET_LRDS_HANDLE(handle), scanId)) LOM_ERROR(handle, eBADPARAMETER_LOM);

        orn = LRDS_SCANTABLE(LOM_GET_LRDS_HANDLE(handle))[scanId].orn;
    }
    else orn = ocnOrScanId;

    relTableEntry = LRDS_GET_RELTABLE_ENTRY(LOM_GET_LRDS_HANDLE(handle), orn);

    e = Catalog_GetClassInfo(handle, relTableEntry->ri.fid.volNo, LOM_USEROPENCLASSTABLE(handle)[orn].classID, &idxForClassInfo);
    if( e < eNOERROR) {
        LOM_ERROR(handle, e);
    }

    v = Catalog_GetVolIndex(handle, relTableEntry->ri.fid.volNo);
        if(v < eNOERROR) LOM_ERROR(handle, v);

    /* poiner to SYSCLASSES */
    sysCatalog = &CATALOG_GET_CLASSINFOTBL(handle, v)[idxForClassInfo];

    /* pointer to SYSINDEXES */
    ptrToSysIndexes = &CATALOG_GET_INDEXINFOTBL(handle, v)[CATALOG_GET_INDEXINFOTBL_INDEX(sysCatalog)];

    for( i = 0; i < CATALOG_GET_INDEXNUM(sysCatalog); i++)  {
        if(ptrToSysIndexes[i].indexType == LOM_INDEXTYPE_TEXT) {
            if(ptrToSysIndexes[i].colNo[0] == colNo) break;
        }
    }

    /* no such text-index built on the given column */
    if(i == CATALOG_GET_INDEXNUM(sysCatalog))
        LOM_ERROR(handle, eNOSUCHINDEX_LOM);

    /* copy index id */
    bcopy(&(ptrToSysIndexes[i].kdesc.invertedIndex.keywordIndex), iid, sizeof(IndexID));

    return eNOERROR;
}

Four lom_Text_GetReverseKeywordIndex(
    LOM_Handle *handle,
    Four ocnOrScanId,
    Boolean useScanFlag,
    Two colNo,
    IndexID *iid
)
{

    Four orn;   /* open class number */
    Four i;     /* index variable */
    Four e;     /* error code */
    catalog_SysClassesOverlay *sysCatalog;  /* pointer to SYSCLASSES */
    catalog_SysIndexesOverlay *ptrToSysIndexes; /* pointer to SYSINDEXES */
    lrds_RelTableEntry *relTableEntry; /* pointer to an entry of relation table */
    Four idxForClassInfo;
    Four v;
    Four scanId;

    if(useScanFlag) {
        scanId = LOM_SCANTABLE(handle)[ocnOrScanId].lrdsScanId;

        if (!LRDS_VALID_SCANID(LOM_GET_LRDS_HANDLE(handle), scanId)) LOM_ERROR(handle, eBADPARAMETER_LOM);

        orn = LRDS_SCANTABLE(LOM_GET_LRDS_HANDLE(handle))[scanId].orn;
    }
    else orn = ocnOrScanId;

    relTableEntry = LRDS_GET_RELTABLE_ENTRY(LOM_GET_LRDS_HANDLE(handle), orn);

    e = Catalog_GetClassInfo(handle, relTableEntry->ri.fid.volNo, LOM_USEROPENCLASSTABLE(handle)[orn].classID, &idxForClassInfo);
    if( e < eNOERROR) {
        LOM_ERROR(handle, e);
    }

    v = Catalog_GetVolIndex(handle, relTableEntry->ri.fid.volNo);
        if(v < eNOERROR) LOM_ERROR(handle, v);

    /* poiner to SYSCLASSES */
    sysCatalog = &CATALOG_GET_CLASSINFOTBL(handle, v)[idxForClassInfo];

    /* pointer to SYSINDEXES */
    ptrToSysIndexes = &CATALOG_GET_INDEXINFOTBL(handle, v)[CATALOG_GET_INDEXINFOTBL_INDEX(sysCatalog)];

    for( i = 0; i < CATALOG_GET_INDEXNUM(sysCatalog); i++)  {
        if(ptrToSysIndexes[i].indexType == LOM_INDEXTYPE_TEXT) {
            if(ptrToSysIndexes[i].colNo[0] == colNo) break;
        }
    }

    /* no such text-index built on the given column */
    if(i == CATALOG_GET_INDEXNUM(sysCatalog))
        LOM_ERROR(handle, eNOSUCHINDEX_LOM);

    /* copy index id */
    bcopy(&(ptrToSysIndexes[i].kdesc.invertedIndex.reverseKeywordIndex), iid, sizeof(IndexID));

    return eNOERROR;
}

Four lom_Text_GetDocIdIndex(
    LOM_Handle *handle,
    Four ocnOrScanId,
    Boolean useScanFlag,
    Two colNo,
    IndexID *iid
)
{
    Four i;     /* index variable */
    Four e;     /* error code */
    catalog_SysClassesOverlay *sysCatalog;  /* pointer to SYSCLASSES */
    catalog_SysIndexesOverlay *ptrToSysIndexes; /* pointer to SYSINDEXES */
    lrds_RelTableEntry *relTableEntry; /* pointer to an entry of relation table */
    Four idxForClassInfo;
    Four v;
    Four scanId;
    Four orn;

    if(useScanFlag) {
        scanId = LOM_SCANTABLE(handle)[ocnOrScanId].lrdsScanId;

        if (!LRDS_VALID_SCANID(LOM_GET_LRDS_HANDLE(handle), scanId)) LOM_ERROR(handle, eBADPARAMETER_LOM);

        orn = LRDS_SCANTABLE(LOM_GET_LRDS_HANDLE(handle))[scanId].orn;
    }
    else orn = ocnOrScanId;

    relTableEntry = LRDS_GET_RELTABLE_ENTRY(LOM_GET_LRDS_HANDLE(handle), orn);

    e = Catalog_GetClassInfo(handle, relTableEntry->ri.fid.volNo, LOM_USEROPENCLASSTABLE(handle)[orn].classID, &idxForClassInfo);
    if( e < eNOERROR) LOM_ERROR(handle, e);

    v = Catalog_GetVolIndex(handle, relTableEntry->ri.fid.volNo);
        if(v < eNOERROR) LOM_ERROR(handle, v);

    /* poiner to SYSCLASSES */
    sysCatalog = &CATALOG_GET_CLASSINFOTBL(handle, v)[idxForClassInfo];

    /* pointer to SYSINDEXES */
    ptrToSysIndexes = &CATALOG_GET_INDEXINFOTBL(handle, v)[CATALOG_GET_INDEXINFOTBL_INDEX(sysCatalog)];

    for( i = 0; i < CATALOG_GET_INDEXNUM(sysCatalog); i++)  {
        if(ptrToSysIndexes[i].indexType == LOM_INDEXTYPE_TEXT) {
            if(ptrToSysIndexes[i].colNo[0] == colNo) break;
        }
    }

    /* no such text-index built on the given column */
    if(i == CATALOG_GET_INDEXNUM(sysCatalog))
        LOM_ERROR(handle, eNOSUCHINDEX_LOM);

    /* copy index id */
    bcopy(&(ptrToSysIndexes[i].kdesc.invertedIndex.docIdIndex), iid, sizeof(IndexID));

    return eNOERROR;
}


Four lom_Text_GetKeywordExtractorFPtr(
    LOM_Handle *handle,
    Four ocn,
    Four colNo,
    lom_FptrToKeywordExtractor* fptr
)
{
    Four e;
    Four j;
    for(j = 0; j < LOM_USEROPENCLASSTABLE(handle)[ocn].numOfTextAttrs; j++) {
        if(LOM_USEROPENCLASSTABLE(handle)[ocn].textColNo[j] == colNo) {
#ifndef PRELOAD_KEYWORDEXTRACTOR
            if(!(LOM_USEROPENCLASSTABLE(handle)[ocn].fptrToKeywordExtractor[j])) {
                lrds_RelTableEntry *relTableEntry;
                relTableEntry = LRDS_GET_RELTABLE_ENTRY(LOM_GET_LRDS_HANDLE(handle), ocn);
                /* get a function pointer to keyword extractor */
                e = LOM_Text_OpenHandleForKeywordExtractor(handle,
                        relTableEntry->ri.fid.volNo,
                        LOM_USEROPENCLASSTABLE(handle)[ocn].classID,
                        GET_USERLEVEL_COLNO(colNo),
                        &LOM_USEROPENCLASSTABLE(handle)[ocn].handleForDLOfKeywordExtractor[j],
                        &LOM_USEROPENCLASSTABLE(handle)[ocn].fptrToKeywordExtractor[j],
                        &LOM_USEROPENCLASSTABLE(handle)[ocn].fptrToGettingNextPostingInfo[j],
                        &LOM_USEROPENCLASSTABLE(handle)[ocn].fptrToFinalizeKeywordExtraction[j]);
                if(e < eNOERROR) LOM_ERROR(handle, e);
            }
#endif
            *fptr = LOM_USEROPENCLASSTABLE(handle)[ocn].fptrToKeywordExtractor[j];
            return eNOERROR;
        }
    }
    *fptr = NULL;
    return eNOERROR;
}

Four lom_Text_GetFilterFPtr(
    LOM_Handle *handle,
    Four ocn,
    Four colNo,
    lom_FptrToFilter* fptr
)
{
    Four j;

    for(j = 0; j < LOM_USEROPENCLASSTABLE(handle)[ocn].numOfTextAttrs; j++) {
        if(LOM_USEROPENCLASSTABLE(handle)[ocn].textColNo[j] == colNo) {
#ifndef PRELOAD_FILTER
            if(!(LOM_USEROPENCLASSTABLE(handle)[ocn].fptrToFilter[j])) {
                lrds_RelTableEntry *relTableEntry;
                Four e;
                relTableEntry = LRDS_GET_RELTABLE_ENTRY(LOM_GET_LRDS_HANDLE(handle), ocn);
                /* get a function pointer to filter */
                e = LOM_Text_OpenHandleForFilter(handle,
                    relTableEntry->ri.fid.volNo,
                    LOM_USEROPENCLASSTABLE(handle)[ocn].classID,
                    GET_USERLEVEL_COLNO(colNo),
                    &LOM_USEROPENCLASSTABLE(handle)[ocn].handleForDLOfFilter[j],
                    &LOM_USEROPENCLASSTABLE(handle)[ocn].fptrToFilter[j]);
                if(e < eNOERROR) LOM_ERROR(handle, e);
            }
#endif
            *fptr = LOM_USEROPENCLASSTABLE(handle)[ocn].fptrToFilter[j];
            return eNOERROR;
        }
    }
    *fptr = NULL;
    return eNOERROR;
}

Four lom_Text_GetGetNextPostingInfoFPtr(
    LOM_Handle *handle,
    Four ocn,
    Four colNo,
    lom_FptrToGetNextPostingInfo *fptr
)
{
    Four j;
    for(j = 0; j < LOM_USEROPENCLASSTABLE(handle)[ocn].numOfTextAttrs; j++) {
        if(LOM_USEROPENCLASSTABLE(handle)[ocn].textColNo[j] == colNo) 
        {
            *fptr = LOM_USEROPENCLASSTABLE(handle)[ocn].fptrToGettingNextPostingInfo[j];
            return eNOERROR;
        }
    }
    *fptr = NULL;
    return eNOERROR;
}

Four  lom_Text_GetFinalizeKeywordExtractionFPtr(
    LOM_Handle *handle,
    Four ocn,
    Four colNo,
    lom_FptrToFinalizeKeywordExtraction *fptr
)
{
    Four j;
    for(j = 0; j < LOM_USEROPENCLASSTABLE(handle)[ocn].numOfTextAttrs; j++) {
        if(LOM_USEROPENCLASSTABLE(handle)[ocn].textColNo[j] == colNo) 
        {
            *fptr = LOM_USEROPENCLASSTABLE(handle)[ocn].fptrToFinalizeKeywordExtraction[j]; 
            return eNOERROR;
        }
    }
    *fptr = NULL;
    return eNOERROR;
}


/************************************/
/* Misc. Interface                  */
/************************************/

void lom_Text_MakeLogicalIdIndexName(
    LOM_Handle *handle,
    Four volId,     /* IN: volumn id */
    char *className,/* IN: class name */
    char *logicalDocIdIndexName /* OUT: content table name */
)
{
    /* make content table name */
    sprintf(logicalDocIdIndexName, "_%s_LogicalIdIndex", className);
}

void lom_Text_MakeContentTableName(
    LOM_Handle *handle,
    Four volId,     /* IN: volumn id */
    char *className,/* IN: class name */
    char *contentTableName /* OUT: content table name */
)
{
    /* make content table name */
    sprintf(contentTableName, "_%s_Content", className);
}

void lom_Text_MakePostingTableName(
    LOM_Handle *handle,
    Four volId,     /* IN: volumn id */
    char *className,/* IN: class name */
    char *postingTableName /* OUT: content table name */
)
{
    /* make content table name */
    sprintf(postingTableName, "_%s_Posting", className);
}

void lom_Text_MakeInvertedIndexName(
    LOM_Handle *handle,
    Four volId,     /* IN: volumn id */
    char *className, /* IN: class name */
    char *attrName, /* IN: attribute name */
    char *invertedIndexTableName    /* OUT: inverted index name */
)
{
    /* make inverted index table name */
    sprintf(invertedIndexTableName, "_%s_%s_Inverted", className, attrName);
}

void lom_Text_MakeDocIdTableName(
    LOM_Handle *handle,
    Four volId, /* IN: volume id */
    char *className,    /* IN: class name */
    char *attrName,     /* IN: attribute name */
    char *docIdTableName    /* OUT : docId table name */
)
{
    /* make docId table name */
    sprintf(docIdTableName, "_%s_%s_docId", className, attrName);
}


/*
 * Function: char makeReverseStr(char *, char *);
 *
 * Description:
 *  make reverse string 
 *
 * Retuns:
 *  error code
 */
char *makeReverseStr(
    char *src,      /* IN: source string */
    char *dest,     /* OUT: reverse string */
    Four length
)
{
    register int i;

    for(i = 0; i < length; i++)
        dest[length - 1 - i] = src[i];
    dest[length] = '\0';

    return dest;
}

Four LOM_AllocPostingBuffer(LOM_Handle* handle, lom_PostingBuffer* postingBuffer, Four length)
{
    return LRDS_initVarArray(LOM_GET_LRDS_HANDLE(handle), &(postingBuffer->buffer), 1, (length));
}

Four LOM_ReallocPostingBuffer(LOM_Handle* handle, lom_PostingBuffer* postingBuffer, Four length)
{
    Four e;

    if(LOM_SIZE_POSTINGBUFFER(*postingBuffer) >= length)
        return eNOERROR;

    while(LOM_SIZE_POSTINGBUFFER(*postingBuffer) < length)
    {
        e = LRDS_doublesizeVarArray(LOM_GET_LRDS_HANDLE(handle), &(postingBuffer->buffer), 1);
        if(e < eNOERROR) LOM_ERROR(handle, eBADPARAMETER_LOM);
    }

    return eNOERROR;
}

Four LOM_FreePostingBuffer(LOM_Handle* handle, lom_PostingBuffer* postingBuffer)
{
    return LRDS_finalVarArray(LOM_GET_LRDS_HANDLE(handle), &(postingBuffer->buffer));
}

#else /* SLIMDOWN_TEXTIR */

#include "LOM_Internal.h"
#include "LOM_Param.h"
#include "LOM.h"
#include "Catalog.h"
#include "Catalog_Internal.h"


static Four lom_Text_IsKeywordExistInInvertedIndex(
	LOM_Handle*		handle,						/* IN  LOM system handle */
	Four			ocnOrScanId,				/* IN: text open scan number */
	Boolean			useScanFlag,				/* IN: flag */
	Two				colNo,						/* IN: column number */
	char*			keyword						/* IN  keyword to check */
)
{
	return eTEXTIR_NOTENABLED_LOM;
}


Four lom_Text_AddInvertedIndexEntryFromBuf(
	LOM_Handle*		handle, 
	Four			temporaryVolId,				/* IN: id for temporary volume used in sorting and etc. */
	Four			ocnOrScanId,				/* IN: text open scan number */
	Boolean			useScanFlag,				/* IN: flag */
	Four            lrdsBulkLoadId,				/* IN: lrds level bulk load id */
	Four            lrdsTextBulkLoadId,			/* IN: lrds level text bulk load id */
	Boolean         useBulkLoadFlag,			/* IN: use bulk loading feature */
	Two				colNo,						/* IN: column number */
	char*			keyword,					/* IN: keyword */
	Four			nPostings,					/* IN: number of postings */
	Four			lengthOfPostingList,		/* IN: length of posting-list */
	char*			ptrToPostingList,			/* IN: pointer to posting list */
	TupleID*		tid,						/* OUT: tid for index entry */
	Boolean			isLogicalIdOrder,			/* IN: isLogicalIdOrder */
	Boolean			buildReverseKeywordIndex,	/* IN: build index for reverse keyword in this function */
	Boolean*		newlyRegisteredKeyword		/* OUT: is keyword a newly registered? */
)
{
	return eTEXTIR_NOTENABLED_LOM;
}


Four lom_Text_RemoveInvertedIndexEntry(	
	LOM_Handle *handle, 
	Four ocnOrScanId,		/* IN: ocn or scanId */
	Boolean useScanFlag,		/* IN: flag */
	Four logicalDocId,			/* IN: document id */
	Two colNo 			/* IN: column number */
)
{
	return eTEXTIR_NOTENABLED_LOM;
}


Four lom_Text_AddDocIdIndexEntryFromBuf(
	LOM_Handle*	handle, 
	Four		ocnOrScanId,
	Boolean		useScanFlag,
	Four		lrdsBulkLoadId,
	Four		lrdsTextBulkLoadId,
	Boolean		useBulkLoadFlag,
	Two			colNo,
	Four		logicalDocId, 
	Four		numOfPointers, 
	TupleID*	pointerBuffer
)
{
	return eTEXTIR_NOTENABLED_LOM;
}


Four lom_Text_AddReverseKeywordIndexEntryFromTempFile(
 	LOM_Handle	*handle, 
	Four		temporaryVolId,			/* IN: id for temporary volume used in sorting and etc. */
 	Four		ocnOrScanId,			/* IN: ocn or scanId */
 	Boolean		useScanFlag,			/* IN: flag */
 	Two			colNo,
 	char		*reverseKeywordFileName /* IN: temporary reverse keyword file name */
)
{
	return eTEXTIR_NOTENABLED_LOM;
}


Four lom_Text_AddInvertedIndexEntryFromTempFile(
	LOM_Handle*	handle, 
	Four		temporaryVolId,						/* IN: id for temporary volume used in sorting and etc. */
	Four		ocnOrScanId,						/* IN: ocn or scanId */
	Boolean		useScanFlag,						/* IN: flag */
	Two			colNo,								/* IN: column number */
	char*		tempPostingFileName,				/* IN: temporary posting file name */
	char*		docIdFileName,						/* INOUT: DocIdFile name */
	Boolean		isLogicalIdOrder,					/* IN: isLogicalIdOrder */
	lom_Text_ConfigForInvertedIndexBuild* config	/* IN: index building configurations */
)
{
	return eTEXTIR_NOTENABLED_LOM;
}


Four lom_Text_AddDocIdIndexEntryFromTempFile(
	LOM_Handle	*handle, 
	Four		temporaryVolId,	/* IN: id for temporary volume used in sorting and etc. */
	Four		ocnOrScanId,	/* IN: ocn or scanId */
	Boolean		useScanFlag,	/* IN: flag */
	Two			colNo,			/* IN: column number */
	char		*docIdFileName	/* INOUT: DocIdFile name */
)
{
	return eTEXTIR_NOTENABLED_LOM;
}


Four lom_Text_AddDocIdIndexEntryFromSortStream(
	LOM_Handle	*handle, 
	Four		temporaryVolId,	/* IN: id for temporary volume used in sorting and etc. */
	Four		ocnOrScanId,	/* IN: ocn or scanId */
	Boolean		useScanFlag,	/* IN: flag */
	Two			colNo,			/* IN: column number */
	Four        sortStreamId	/* IN: sort stream id */
)
{
	return eTEXTIR_NOTENABLED_LOM;
}


Four lom_Text_RemoveDocIdIndexEntry(
	LOM_Handle *handle, 
	Four ocnOrScanId,                /* IN: ocn or scan id */
	Boolean useScanFlag,		/* IN: flag */
	Four logicalDocId,                       /* IN: document id */
	Two colNo                       /* IN: column number */
)
{
	return eTEXTIR_NOTENABLED_LOM;
}


Four lom_Text_OpenTempClassForPosting(
	LOM_Handle								*handle,			/* IN: system handle */
	Four									volId,				/* IN: volume id */
	char									*className,			/* IN */
	char									*attrName,			/* IN */
	Four									*ocn,				/* OUT */
	Four									*scanId				/* OUT */
)
{
	return eTEXTIR_NOTENABLED_LOM;
}


Four lom_Text_CloseTempClassForPosting(
	LOM_Handle								*handle,			/* IN: system handle */
	Four									ocn,				/* IN */
	Four									scanId				/* IN */
)
{
	return eTEXTIR_NOTENABLED_LOM;
}


Four lom_Text_GetPostingFromTempClass(
	LOM_Handle								*handle, 
	Four									scanIdForTempClass,	/* IN: scanId for temp calss */
	lom_PostingBuffer						*postingBuffer		/* IN: posting buffer */
)
{
	return eTEXTIR_NOTENABLED_LOM;
}


Four lom_Text_GetPostingFromTempFile(
	LOM_Handle								*handle, 
	Four									ocnOrScanId,	/* IN: open class no or scan id */
	Boolean									useScanFlag,	/* IN: flag */
	Two										colNo,			/* IN: column no */
	lom_Text_PostingInfoForReading*			postingInfo,	/* IN: posting info */
	FILE									*fd, 			/* IN: file descriptor */
	lom_PostingBuffer						*postingBuffer	/* IN: posting buffer */
)
{
	return eTEXTIR_NOTENABLED_LOM;
}


void lom_Text_AppendPostingToPostingList(
	LOM_Handle *handle, 
	lom_PostingBuffer *postingListBuffer,
	Four postingLength, 
	char *ptrToPosting
)
{
	;
}


Four lom_Text_GetDocIdAndPointerFromTempFile(
	LOM_Handle	*handle, 
	FILE		*docIdFile,      /* file descriptor */
	Four		*docId,
	TupleID		*tidForInvertedIndexEntry
)
{
	return eTEXTIR_NOTENABLED_LOM;
}


Four lom_Text_AddDocIdIndexEntryIntoTempFile(
	LOM_Handle *handle, 
	FILE *docIdFile,
	Four nPostings,
	char *ptrToPostingList,
	TupleID *tidForInvertedIndexEntry
)
{
	return eTEXTIR_NOTENABLED_LOM;
}


Four lom_Text_SortDocIdFile(
	LOM_Handle *handle, 
	char *srcDocIdFile,
	char *destDocIdFile
)
{
	return eTEXTIR_NOTENABLED_LOM;
}


Four lom_Text_OpenSortStreamForDocIdIndex(
	LOM_Handle*		handle,						/* IN: LOM system handle */
	Four			volId						/* IN: volume Id */
)
{
	return eTEXTIR_NOTENABLED_LOM;
}


Four lom_Text_CloseSortStreamForDocIdIndex(
	LOM_Handle*		handle,						/* IN: LOM system handle */
	Four			sortStreamId				/* IN: sort stream id */
)
{
	return eTEXTIR_NOTENABLED_LOM;
}


Four lom_Text_AddDocIdIndexEntryIntoSortStream(
	LOM_Handle* handle,						/* IN: LOM system handle */
	Four		sortStreamId,				/* IN: sort stream id */
	Four		nPostings,					/* IN: n of postings to add */
	char		*ptrToPostingList,			/* IN: posting list */
	TupleID		*tidForInvertedIndexEntry	/* IN: tid of index entry for given posting list */
)
{
	return eTEXTIR_NOTENABLED_LOM;
}


Four lom_Text_GetDocIdAndPointerFromSortStream(
	LOM_Handle		*handle,					/* IN: system handle */
	Four			sortStreamId,				/* IN: sort stream id */
	Four			*docId,						/* OUT: doc id (logical id) */
	TupleID			*tidForInvertedIndexEntry	/* OUT: tid of index entry associated with docid */
)
{
	return eTEXTIR_NOTENABLED_LOM;
}


Four lom_Text_SortDocIdSortStream(
	LOM_Handle* handle,						/* IN: LOM system handle */
	Four		sortStreamId				/* IN: sort stream id */
)
{
	return eTEXTIR_NOTENABLED_LOM;
}


Four lom_Text_OpenSortStreamForPosting(
	LOM_Handle*		handle,						/* IN: LOM system handle */
	Four			volId						/* IN: volume Id */
)
{
	return eTEXTIR_NOTENABLED_LOM;
}


Four lom_Text_AddPostingIntoSortStream(
	LOM_Handle*			handle,						/* IN: LOM system handle */
	Four				sortStreamId,				/* IN: sort stream id */
	Four                postingBufferLength,		/* IN: posting buffer length */
	char*				postingBuffer				/* IN: posting */
)
{
	return eTEXTIR_NOTENABLED_LOM;
}


Four lom_Text_GetPostingFromSortStream(
	LOM_Handle*			handle,						/* IN: LOM system handle */
	Four				sortStreamId,				/* IN: sort stream id */
	Four				postingBufferLength,		/* IN: posting buffer length */
	char*				postingBuffer				/* OUT: posting */
)
{
	return eTEXTIR_NOTENABLED_LOM;
}


Four lom_Text_SortPostingSortStream(
	LOM_Handle* handle,						/* IN: LOM system handle */
	Four		sortStreamId				/* IN: sort stream id */
)
{
	return eTEXTIR_NOTENABLED_LOM;
}


Four lom_Text_CloseSortStreamForPosting(
	LOM_Handle* handle,						/* IN: LOM system handle */
	Four		sortStreamId				/* IN: sort stream id */
)
{
	return eTEXTIR_NOTENABLED_LOM;
}


Four lom_Text_CreateCatalogTable(
	LOM_Handle *handle, 
	Four volId
) 
{
	return eTEXTIR_NOTENABLED_LOM;
}


Four lom_Text_AddIndexInfoIntoCatalog(
	LOM_Handle *handle, 
	Four volId, 	/* IN: volumn id */
	char *className, /* IN: class name */
	char *attrName,	/* IN: attribute name */
	Two colNo 	/* IN: column number */
)
{
	return eTEXTIR_NOTENABLED_LOM;
}


Four lom_Text_DropIndexInfoFromCatalog(
	LOM_Handle *handle, 
	Four volId, 	/* IN: volumn id */
	char *className, /* IN: class name */
	char *attrName 	/* IN: attribute name */
)
{
	return eTEXTIR_NOTENABLED_LOM;
}


Four lom_Text_CreateContentTable(
	LOM_Handle *handle, 
	Four volId, 	/* IN: volumn id */
	char *className	/* IN: class name */
)
{
	return eTEXTIR_NOTENABLED_LOM;
}


Four lom_Text_DestroyContentTable(
	LOM_Handle *handle, 
	Four volId, 
	char *className
)
{
	return eTEXTIR_NOTENABLED_LOM;
}


Four lom_Text_CreateInvertedIndexTable(
	LOM_Handle *handle, 
	Four volId, 	/* IN: volumn id */
	char *className, /* IN: class name */
	char *attrName,	/* IN: attribute name */
	Two colNo 	/* IN: column number */
)
{
	return eTEXTIR_NOTENABLED_LOM;
}


Four lom_Text_DestroyInvertedIndexTable(
	LOM_Handle *handle, 
	Four volId, 		/* IN: volumn id */
	char *className, 	/* IN: class name */
	char *attrName 		/* IN: attribute name */
)	
{
	return eTEXTIR_NOTENABLED_LOM;
}


Four lom_Text_CreateDocIdIndexTable(
	LOM_Handle *handle, 
	Four volId, 	/* IN: volumn id */
	char *className, /* IN: class name */
	char *attrName,	/* IN: attribute name */
	Two colNo 	/* IN: column number */
)
{
	return eTEXTIR_NOTENABLED_LOM;
}


Four lom_Text_DestroyDocIdIndexTable(
	LOM_Handle *handle, 
	Four volId, 		/* IN: volumn id */
	char *className, 	/* IN: class name */
	char *attrName 		/* IN: attribute name */
)	
{
	return eTEXTIR_NOTENABLED_LOM;
}


Four lom_Text_CreateContentData(
	LOM_Handle *handle, 
	Four ocnOrScanId, 		/* IN: ocn or scan id */
	Boolean useScanFlag,		/* IN: flag */
	Four dataLength, 	/* IN: length of data */
	char *data, 		/* IN: data */
	TupleID *tid		/* OUT: tuple id */
)
{
	return eTEXTIR_NOTENABLED_LOM;
}


Four lom_Text_DestroyContent(
	LOM_Handle *handle, 
	Four ocnOrScanId,	/* IN: ocn or scanId */
	Boolean useScanFlag,	/* IN: flag */
	OID *oid,		/* IN: oid */
	Two colNo,		/* IN: column number */
	LOM_TextDesc *textDesc	/* IN: text descriptor */
)
{
	return eTEXTIR_NOTENABLED_LOM;
}


Four lom_Text_UpdateContentData(
	LOM_Handle *handle, 
	Four ocnOrScanId, 		/* IN: open scan number */
	Boolean useScanFlag,		/* IN: flag */
	TupleID *tid,		/* IN: tuple id */
	Four start,		/* IN: start of data */
	Four length,		/* IN: length of to-be-updated data */
	Four dataLength, 	/* IN: length of data */
	char *data 		/* IN: data */
)
{
	return eTEXTIR_NOTENABLED_LOM;
}


Four lom_Text_FetchContentData(
	LOM_Handle *handle, 
	Four ocnOrScanId, 		/* IN: open scan number */
	Boolean useScanFlag,		/* IN: flag */
	TupleID *tid,		/* IN: tuple id */
	Four start,		/* IN: start of data */
	Four length,		/* IN: length of to-be-updated data */
	Four dataLength, 	/* IN: length of data */
	char *data 		/* OUT: data */
)
{
	return eTEXTIR_NOTENABLED_LOM;
}


Four lom_Text_OpenContentClass(
	LOM_Handle *handle, 
	Four volId, 		/* IN: volumn id */
	char *className		/* IN: class name */
)
{
	return eTEXTIR_NOTENABLED_LOM;
}


Four lom_Text_OpenInvertedIndexTable(
	LOM_Handle *handle, 
	Four volId, 		/* IN: volumn id */
	char *className, 	/* IN: class name */
	char *attrName		/* IN: attribute name */
)
{
	return eTEXTIR_NOTENABLED_LOM;
}


Four lom_Text_OpenDocIdIndexTable(
	LOM_Handle *handle,
	Four volId, 		/* IN: volumn id */
	char *className, 	/* IN: class name */
	char *attrName		/* IN: attribute name */
)
{
	return eTEXTIR_NOTENABLED_LOM;
}

/* This two functions should not be slimed down */
/* This functions are used in other module */
Four lom_Text_GetLogicalId(
	LOM_Handle *handle,
	Four ocnOrScanId,		/* IN: open scan number */
	Boolean useScanFlag,		/* IN: flag */
	OID *oid		/* IN: object id */
)
{
	ColListStruct clist[1];	/* column struct list */
	Four e;			/* error code */
	Four scanId;
	Four orn;

	if(useScanFlag) {
		scanId = LOM_SCANTABLE(handle)[ocnOrScanId].lrdsScanId;

		if (!LRDS_VALID_SCANID(LOM_GET_LRDS_HANDLE(handle), scanId)) LOM_ERROR(handle, eBADPARAMETER_LOM);

		orn = LRDS_SCANTABLE(LOM_GET_LRDS_HANDLE(handle))[scanId].orn;
	}
	else orn = ocnOrScanId;

	/* make column list struct */
	clist[0].colNo = 0;
	clist[0].start = ALL_VALUE;
	clist[0].length = SM_LONG_SIZE;
	clist[0].dataLength = SM_LONG_SIZE;

	/* get logical id from object */
	if(useScanFlag)
		e = LRDS_FetchTuple(LOM_GET_LRDS_HANDLE(handle), scanId, useScanFlag, (TupleID *)oid, 1, clist);
	else
		e = LRDS_FetchTuple(LOM_GET_LRDS_HANDLE(handle), orn, useScanFlag, (TupleID *)oid, 1, clist);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	return clist[0].data.l;
}

Four lom_Text_SetLogicalId(
	LOM_Handle *handle,
	Four ocnOrScanId,		/* IN: open scan number */
	Boolean useScanFlag,	/* IN: flag */
	OID *oid,				/* IN: object id */
	Four logicalId			/* IN: new logical id */
)
{
	ColListStruct clist[1];	/* column struct list */
	Four e;			/* error code */
	Four scanId;
	Four orn;

	if(useScanFlag) {
		scanId = LOM_SCANTABLE(handle)[ocnOrScanId].lrdsScanId;

		if (!LRDS_VALID_SCANID(LOM_GET_LRDS_HANDLE(handle), scanId)) LOM_ERROR(handle, eBADPARAMETER_LOM);

		orn = LRDS_SCANTABLE(LOM_GET_LRDS_HANDLE(handle))[scanId].orn;
	}
	else orn = ocnOrScanId;

	/* make column list struct */
	clist[0].colNo = 0;
	clist[0].start = ALL_VALUE;
	clist[0].length = SM_LONG_SIZE;
	clist[0].dataLength = SM_LONG_SIZE;
	clist[0].data.l = logicalId;

	/* get logical id from object */
	if(useScanFlag)
		e = LRDS_UpdateTuple(LOM_GET_LRDS_HANDLE(handle), scanId, useScanFlag, (TupleID *)oid, 1, clist);
	else
		e = LRDS_UpdateTuple(LOM_GET_LRDS_HANDLE(handle), orn, useScanFlag, (TupleID *)oid, 1, clist);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	return eNOERROR;
}


Four lom_Text_GetInvertedIndexTableORN(
	LOM_Handle *handle,
	Four ocnOrScanId,
	Boolean useScanFlag,
	Two colNo		/* column number */
)
{
	return eTEXTIR_NOTENABLED_LOM;
}


Four lom_Text_GetDocIdIndexTableORN(
	LOM_Handle *handle,
	Four ocnOrScanId,               /* scan id */
	Boolean useScanFlag,		/* flag */
	Two colNo               /* column number */
)
{
	return eTEXTIR_NOTENABLED_LOM;
}


Four lom_Text_GetRevKeywordIndex(
	LOM_Handle *handle,
	Four ocnOrScanId,
	Boolean useScanFlag,
	Two colNo,
	IndexID *iid
)
{
	return eTEXTIR_NOTENABLED_LOM;
}


Four lom_Text_GetKeywordIndex(
	LOM_Handle *handle,
	Four ocnOrScanId,
	Boolean useScanFlag,
	Two colNo,
	IndexID *iid
)
{
	return eTEXTIR_NOTENABLED_LOM;
}


Four lom_Text_GetReverseKeywordIndex(
	LOM_Handle *handle,
	Four ocnOrScanId,
	Boolean useScanFlag,
	Two colNo,
	IndexID *iid
)
{
	return eTEXTIR_NOTENABLED_LOM;
}


Four lom_Text_GetDocIdIndex(
	LOM_Handle *handle,
	Four ocnOrScanId,
	Boolean useScanFlag,
	Two colNo,
	IndexID *iid
)
{
	return eTEXTIR_NOTENABLED_LOM;
}


Four lom_Text_GetKeywordExtractorFPtr(
	LOM_Handle *handle,
	Four ocn,
	Four colNo,
	lom_FptrToKeywordExtractor* fptr
)
{
	return eTEXTIR_NOTENABLED_LOM;
}


Four lom_Text_GetFilterFPtr(
	LOM_Handle *handle,
	Four ocn,
	Four colNo,
	lom_FptrToFilter* fptr
)
{
	return eTEXTIR_NOTENABLED_LOM;
}


Four lom_Text_GetGetNextPostingInfoFPtr(
	LOM_Handle *handle,
	Four ocn,
	Four colNo,
	lom_FptrToGetNextPostingInfo *fptr
)
{
	return eTEXTIR_NOTENABLED_LOM;
}


Four  lom_Text_GetFinalizeKeywordExtractionFPtr(
	LOM_Handle *handle,
	Four ocn,
	Four colNo,
	lom_FptrToFinalizeKeywordExtraction *fptr
)
{
	return eTEXTIR_NOTENABLED_LOM;
}


void lom_Text_MakeLogicalIdIndexName(
	LOM_Handle *handle,
	Four volId, 	/* IN: volumn id */
	char *className,/* IN: class name */
	char *logicalDocIdIndexName /* OUT: content table name */
)
{
	;
}


void lom_Text_MakeContentTableName(
	LOM_Handle *handle,
	Four volId, 	/* IN: volumn id */
	char *className,/* IN: class name */
	char *contentTableName /* OUT: content table name */
)
{
	;
}


void lom_Text_MakePostingTableName(
	LOM_Handle *handle,
	Four volId, 	/* IN: volumn id */
	char *className,/* IN: class name */
	char *postingTableName /* OUT: content table name */
)
{
	;
}


void lom_Text_MakeInvertedIndexName(
	LOM_Handle *handle,
	Four volId, 	/* IN: volumn id */
	char *className, /* IN: class name */
	char *attrName,	/* IN: attribute name */
	char *invertedIndexTableName	/* OUT: inverted index name */
)
{
	;
}


void lom_Text_MakeDocIdTableName(
	LOM_Handle *handle,
	Four volId,	/* IN: volume id */
	char *className,	/* IN: class name */
	char *attrName,		/* IN: attribute name */
	char *docIdTableName	/* OUT : docId table name */
)
{
	;
}


char *makeReverseStr(
	char *src,		/* IN: source string */
	char *dest,		/* OUT: reverse string */
	Four length
)
{
	return NULL;
}


Four LOM_AllocPostingBuffer(LOM_Handle* handle, lom_PostingBuffer* postingBuffer, Four length)
{
	return eTEXTIR_NOTENABLED_LOM;
}


Four LOM_ReallocPostingBuffer(LOM_Handle* handle, lom_PostingBuffer* postingBuffer, Four length)
{
	return eTEXTIR_NOTENABLED_LOM;
}


Four LOM_FreePostingBuffer(LOM_Handle* handle, lom_PostingBuffer* postingBuffer)
{
	return eTEXTIR_NOTENABLED_LOM;
}


#endif /* SLIMDOWN_TEXTIR */
