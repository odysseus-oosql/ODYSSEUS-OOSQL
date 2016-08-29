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

#include "LOM_Internal.h"
#include "LOM.h"

static void lom_PrintWarningMessage(LOM_Handle* handle, Four e)
{
	printf("Warning : object might be already deleted by user\n");
	LOM_PTRERROR(handle, e);
}

Four LOM_DeferredDestroyObject(
	LOM_Handle*		handle,			/* IN LOM system handle */
	Four			ocnOrScanId,	/* IN ocn or scan-id */
	Boolean			useScanFlag,	/* IN use ocnOrScanId as scanId if true */
	OID*			oid)			/* OUT object id of the object to be destroyed */
{
	Four e;

	e = lom_AppendObjectToDeferredDeletionList(handle, ocnOrScanId, useScanFlag, oid);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	return eNOERROR;
}

typedef struct lom_DeletionListElement {
	Four logicalId;
	OID	 oid;
} lom_DeletionListElement; 
#define DELETIONLIST_BUFFER_SIZE	64

Four LOM_BatchDestroyByDeferredDeletionList(
	LOM_Handle*		handle,			/* IN LOM system handle */
	Four			temporaryVolId,	/* IN temporary volume id for sorting */
	Four			ocnOrScanId,	/* IN ocn or scan-id */
	Boolean			useScanFlag)	/* IN use ocnOrScanId as scanId if true */
{
	Four					e;
	lom_DeletionListElement deletionListElements[DELETIONLIST_BUFFER_SIZE];
	Four					nElementsRead;
	Four					lrdsOrnOrScanId;
	Four					ocn;
	Four					numOfTextAttrs;
	Four					deletionListScanId;
	Four					i;

	if(useScanFlag) 
	{
		lrdsOrnOrScanId = LOM_SCANTABLE(handle)[ocnOrScanId].lrdsScanId;
		ocn = LOM_SCANTABLE(handle)[ocnOrScanId].ocn;
	}
	else 
	{
		lrdsOrnOrScanId = ocnOrScanId;
		ocn = ocnOrScanId;
	}

	if(LOM_USEROPENCLASSTABLE(handle)[ocn].ornForDeletionListTable == NIL)
	{
		char className[LOM_MAXCLASSNAME];

		/* get class name */
		e = lom_GetClassName(handle, LOM_USEROPENCLASSTABLE(handle)[ocn].volId, 
							 LOM_USEROPENCLASSTABLE(handle)[ocn].classID, className);
		if(e < eNOERROR) LOM_ERROR(handle, e);
			
		e = lom_OpenDeferredDeletionListTable(handle, LOM_USEROPENCLASSTABLE(handle)[ocn].volId, className);
		if(e == eRELATIONNOTFOUND_LRDS)
		{
			return eNOERROR;
		}
		else if(e < eNOERROR) 
		{
			LOM_ERROR(handle, e);
		}

		LOM_USEROPENCLASSTABLE(handle)[ocn].ornForDeletionListTable = e;

		/* get first tuple id */
		e = lom_GetFirstTupleIdFromDeferredDeletionListTable(handle, 
																 LOM_USEROPENCLASSTABLE(handle)[ocn].ornForDeletionListTable, 
																 SM_FALSE,
																 &(LOM_USEROPENCLASSTABLE(handle)[ocn].tidForDeletionList));
		if(e < eNOERROR) LOM_ERROR(handle, e);
	}

	numOfTextAttrs = LOM_USEROPENCLASSTABLE(handle)[ocn].numOfTextAttrs;

	printf("Sort Deferred Deletion List\n");
	e = lom_SortDeferredDeletionList(handle, temporaryVolId, ocnOrScanId, useScanFlag);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	for(i = 0; i < numOfTextAttrs; i++)
	{
		Two textColNo;

		textColNo = LOM_USEROPENCLASSTABLE(handle)[ocn].textColNo[i];

		printf("Delete Inverted Index of %ld-th Column\n", GET_USERLEVEL_COLNO(textColNo));
		e = lom_Text_DeleteInvertedIndexEntryByDeferredDeletionList(handle, temporaryVolId, ocnOrScanId, useScanFlag, textColNo);
		if(e < eNOERROR) LOM_ERROR(handle, e);

		printf("Delete Docid Index of %ld-th Column\n", GET_USERLEVEL_COLNO(textColNo));
		e = lom_Text_DeleteDocIdIndexEntryByDeferredDeletionList(handle, ocnOrScanId, useScanFlag, textColNo);
		if(e < eNOERROR) LOM_ERROR(handle, e);

		printf("Delete Text Content of %ld-th Column\n", GET_USERLEVEL_COLNO(textColNo));
		e = lom_Text_DestoryContentByDeferredDeletionList(handle, ocnOrScanId, useScanFlag, textColNo);
		if(e < eNOERROR) LOM_ERROR(handle, e);
	}

	printf("Delete Objects\n");
	deletionListScanId = e = lom_DeferredDeletionList_Scan_Open(handle, ocnOrScanId, useScanFlag);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	while((e = lom_DeferredDeletionList_Scan_NextElements(handle, deletionListScanId, DELETIONLIST_BUFFER_SIZE, sizeof(deletionListElements), deletionListElements)) != 0)
	{
		if(e < eNOERROR) LOM_ERROR(handle, e);
		nElementsRead = e;

		for(i = 0; i < nElementsRead; i++)
		{
			e = LRDS_DestroyTuple(LOM_GET_LRDS_HANDLE(handle), lrdsOrnOrScanId, useScanFlag, (TupleID*)&deletionListElements[i].oid);
			if(e == eBADOBJECTID_OM)
			{
				lom_PrintWarningMessage(handle, e);
			}
			else if(e < eNOERROR)
			{
				LOM_ERROR(handle, e);
			}
		}
	}

	e = lom_DeferredDeletionList_Scan_Close(handle, deletionListScanId);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	e = lom_CleanDeferredDeletionList(handle, ocnOrScanId, useScanFlag);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	printf("Done\n");

	return eNOERROR;
}

Four lom_SortDeferredDeletionList(
	LOM_Handle*		handle,			/* IN LOM system handle */
	Four			temporaryVolId,	/* IN temporary volume id for sorting */
	Four			ocnOrScanId,	/* IN ocn or scan-id */
	Boolean			useScanFlag)	/* IN use ocnOrScanId as scanId if true */
{
	Four			sortStreamId;
	SortTupleDesc	sortTupleDesc;
	SortStreamTuple sortTuple;
	Four			e;
	Four			deletionListScanId;
	Boolean			done;
	Four			deletionListOrn;
	TupleID*		deletionListTid;
	Four			ocn;
	Four			nElementsRead;
	Four			numSortTuples;
	Four			i;
	Four			prevLogicalId, currentLogicalId;
	OID				prevOID, currentOID;
	lom_DeletionListElement deletionListElements[DELETIONLIST_BUFFER_SIZE];

	if(useScanFlag) 
		ocn = LOM_SCANTABLE(handle)[ocnOrScanId].ocn;
	else 
		ocn = ocnOrScanId;

	if(LOM_USEROPENCLASSTABLE(handle)[ocn].ornForDeletionListTable == NIL)
		return eBADPARAMETER;

	sortTupleDesc.hdrSize = 0;
	sortTupleDesc.nparts  = 2;
	sortTupleDesc.parts[0].type   = LOM_LONG_VAR;
	sortTupleDesc.parts[0].length = sizeof(Four);
	sortTupleDesc.parts[0].flag   = SORTKEYDESC_ATTR_ASC;
	sortTupleDesc.parts[1].type   = SM_OID;
	sortTupleDesc.parts[1].length = sizeof(OID);
	sortTupleDesc.parts[1].flag   = SORTKEYDESC_ATTR_ASC;

	e = sortStreamId = LRDS_OpenSortStream(LOM_GET_LRDS_HANDLE(handle), (Two)temporaryVolId, &sortTupleDesc);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	deletionListScanId = e = lom_DeferredDeletionList_Scan_Open(handle, ocnOrScanId, useScanFlag);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	while((e = lom_DeferredDeletionList_Scan_NextElements(handle, deletionListScanId, DELETIONLIST_BUFFER_SIZE, sizeof(deletionListElements), deletionListElements)) != 0)
	{
		if(e < eNOERROR) LOM_ERROR(handle, e);
		nElementsRead = e;

		sortTuple.len  = LOM_DELETIONLIST_DELETIONLIST_ELEMENTSIZE;
		for(i = 0; i < nElementsRead; i++)
		{
			sortTuple.data = (char*)&deletionListElements[i];

			e = LRDS_PutTuplesIntoSortStream(LOM_GET_LRDS_HANDLE(handle), sortStreamId, 1, &sortTuple);
			if(e < 0) LOM_ERROR(handle, e);
		}
	}

	e = lom_DeferredDeletionList_Scan_Close(handle, deletionListScanId);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	e = LRDS_SortingSortStream(LOM_GET_LRDS_HANDLE(handle), sortStreamId);
	if(e < 0) LOM_ERROR(handle, e);

	e = lom_CleanDeferredDeletionList(handle, ocnOrScanId, useScanFlag);
	if(e < 0) LOM_ERROR(handle, e);

	done = SM_FALSE;
	deletionListOrn  = LOM_USEROPENCLASSTABLE(handle)[ocn].ornForDeletionListTable;
	deletionListTid  = &LOM_USEROPENCLASSTABLE(handle)[ocn].tidForDeletionList;
	prevLogicalId    = -1;
	currentLogicalId = -1;
	memset(&prevOID, 0, sizeof(OID));
	memset(&currentOID, 0, sizeof(OID));
	while(1)
	{
		numSortTuples	= 1;
		sortTuple.len	= LOM_DELETIONLIST_DELETIONLIST_ELEMENTSIZE;
		sortTuple.data	= (char*)&deletionListElements[0];
		
		e = LRDS_GetTuplesFromSortStream(LOM_GET_LRDS_HANDLE(handle), sortStreamId, &numSortTuples, &sortTuple, &done);
		if(e < eNOERROR) LOM_ERROR(handle, e);

		if(done) break;

		currentLogicalId = deletionListElements[0].logicalId;
		currentOID       = deletionListElements[0].oid;
		if(prevLogicalId != currentLogicalId || (currentLogicalId == -1 && memcmp(&prevOID, &currentOID, sizeof(OID))))
		{
			e = LRDS_CollectionList_AppendElements(LOM_GET_LRDS_HANDLE(handle), deletionListOrn, SM_FALSE, deletionListTid,
												   LOM_DELETIONLIST_DELETIONLIST_COLNO, 1,
												   NULL, &deletionListElements[0]);
			if(e < eNOERROR) LOM_ERROR(handle, e);
		}
		prevLogicalId = currentLogicalId;
		memcpy(&prevOID, &currentOID, sizeof(OID));
	}

	e = LRDS_CloseSortStream(LOM_GET_LRDS_HANDLE(handle), sortStreamId);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	return eNOERROR;
}

static Four lom_UpdateInvertedIndexEntry(
	LOM_Handle*		handle,			/* IN LOM system handle */
	Four			ocnOrScanId,	/* IN ocn or scan-id of inverted index */
	Boolean			useScanFlag,	/* IN use ocnOrScanId as scanId if true */
	TupleID*		tid				/* IN inverted index entry to be updated */
)
{
	Four			e;
	char			keyword[LOM_MAXKEYWORDSIZE];
	char			reverseKeyword[sizeof(Two) + LOM_MAXKEYWORDSIZE];
	Four			nElementsInOrderedSet;
	ColListStruct	clist[1];
	Two				length;

	e = LRDS_OrderedSet_GetN_Elements(LOM_GET_LRDS_HANDLE(handle), ocnOrScanId, useScanFlag,
					                  tid, LOM_INVERTEDINDEX_POSTINGLIST_COLNO, 
									  &nElementsInOrderedSet, NULL);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	if(nElementsInOrderedSet == 0)
	{
		clist[0].colNo		= LOM_INVERTEDINDEX_KEYWORD_COLNO;
		clist[0].start		= ALL_VALUE;
		clist[0].length		= LOM_MAXKEYWORDSIZE;
		clist[0].dataLength = LOM_MAXKEYWORDSIZE;
		clist[0].data.ptr	= keyword;

		e = LRDS_FetchTuple(LOM_GET_LRDS_HANDLE(handle), ocnOrScanId, useScanFlag, tid, 1, clist);
		if(e < eNOERROR) LOM_ERROR(handle, e);

		length = (Two)clist[0].retLength;
		bcopy(&length, &reverseKeyword[0], sizeof(Two));
		makeReverseStr(keyword, &reverseKeyword[sizeof(Two)],length);
		e = LRDS_Text_DeleteKeywords(LOM_GET_LRDS_HANDLE(handle), ocnOrScanId, useScanFlag, tid,
						             LOM_INVERTEDINDEX_REVKEYWORD_COLNO, 1, reverseKeyword);
		if(e < eNOERROR) LOM_ERROR(handle, e);

		e = LRDS_OrderedSet_Destroy(LOM_GET_LRDS_HANDLE(handle), ocnOrScanId, useScanFlag, tid,
						            LOM_INVERTEDINDEX_POSTINGLIST_COLNO, NULL);
		if(e < eNOERROR) LOM_ERROR(handle, e);

		e = LRDS_DestroyTuple(LOM_GET_LRDS_HANDLE(handle), ocnOrScanId, useScanFlag, tid);
		if(e < eNOERROR) LOM_ERROR(handle, e);
	}
	else
	{
		/* update nPostings */
		clist[0].colNo		= LOM_INVERTEDINDEX_NPOSTINGS_COLNO;
		clist[0].start		= ALL_VALUE;
		clist[0].dataLength = sizeof(Four);
		ASSIGN_VALUE_TO_COL_LIST(clist[0], nElementsInOrderedSet, sizeof(Four));
		clist[0].nullFlag	= SM_FALSE;
		
		e = LRDS_UpdateTuple(LOM_GET_LRDS_HANDLE(handle), ocnOrScanId, useScanFlag, tid, 1, clist);
		if(e < eNOERROR) LOM_ERROR(handle, e);
	}

	return eNOERROR;
}

typedef struct lom_InvertedDocIdElement {
	TupleID		tid;
	Four		logicalId;
} lom_InvertedDocIdElement;
#define INVERTEDDOCID_BUFFER_SIZE		128
#define TID_BUFFER_SIZE					128
#define KEYVALUE_BUFFER_SIZE			1024

Four lom_Text_DeleteInvertedIndexEntryByDeferredDeletionList(
	LOM_Handle*		handle,			/* IN LOM system handle */
	Four			temporaryVolId,	/* IN temporary volume id for sorting */
	Four			ocnOrScanId,	/* IN ocn or scan-id */
	Boolean			useScanFlag,	/* IN use ocnOrScanId as scanId if true */
	Two				textColNo		/* IN text column number that the operation applied */
)
{
	Four			e;
	Four			i, j;
	Four			deletionListScanId;
	Four			docIdIndexScanId;
	BoundCond		bound;
	LockParameter	lockup;
	IndexID			iid;
	Four			ornForDocIdIndexTable;
	TupleID			tid;
	Four			sortStreamId;
	SortTupleDesc	sortTupleDesc;
	SortStreamTuple sortTuple;
	Four			ocn;
	TupleID			tidBuffer[TID_BUFFER_SIZE];
	ColListStruct	clist[1];
	Four			nTidsRead, nTotalTidsRead;
	KeyValue		kval[KEYVALUE_BUFFER_SIZE];
	Four			kvalIndex;
	Four			nElementsRead;
	Boolean			done;
	Four			deletionListOrn;
	TupleID*		deletionListTid;
	Four			ornForInvertedIndexTable;
	Four			numSortTuples;
	Two				keyLen;
	TupleID			currentTid, prevTid;
	lom_DeletionListElement		deletionListElements[DELETIONLIST_BUFFER_SIZE];
	lom_InvertedDocIdElement	invertedDocIdElements[INVERTEDDOCID_BUFFER_SIZE];

#ifdef COMPRESSION
    char* 					uncompressedData = NULL;
    Four  					uncompressedDataLength;
	TupleID*				pointerBuffer = NULL;
	char*					tmpPointerBuffer = NULL;
    ColLengthInfoListStruct lengthInfoStruct;
	Four					inIndex, outIndex;
	VolNo					volNo;
#endif

	if(useScanFlag) 
		ocn = LOM_SCANTABLE(handle)[ocnOrScanId].ocn;
	else 
		ocn = ocnOrScanId;

	if(LOM_USEROPENCLASSTABLE(handle)[ocn].ornForDeletionListTable == NIL)
		return eBADPARAMETER;

	sortTupleDesc.hdrSize = 0;
	sortTupleDesc.nparts  = 1;
	sortTupleDesc.parts[0].type   = SM_OID;
	sortTupleDesc.parts[0].length = sizeof(OID);
	sortTupleDesc.parts[0].flag   = SORTKEYDESC_ATTR_ASC;

	e = sortStreamId = LRDS_OpenSortStream(LOM_GET_LRDS_HANDLE(handle), (Two)temporaryVolId, &sortTupleDesc);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	e = lom_Text_GetDocIdIndex(handle, ocnOrScanId, useScanFlag, textColNo, &iid);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	e = ornForDocIdIndexTable = lom_Text_GetDocIdIndexTableORN(handle, ocnOrScanId, useScanFlag, textColNo);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	lockup.duration = L_COMMIT;
	lockup.mode     = L_S;

	/* prepare bound condition for index scan */
	keyLen          = LOM_LONG_SIZE_VAR;
	bound.op		= SM_EQ;
	bound.key.len	= sizeof(Two) + LOM_LONG_SIZE_VAR;

	memcpy(&(bound.key.val[0]), &keyLen, sizeof(Two));

	deletionListScanId = e = lom_DeferredDeletionList_Scan_Open(handle, ocnOrScanId, useScanFlag);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	/* construct clist sturct to read tidlist */
	clist[0].colNo		= LOM_DOCIDTABLE_POINTERLIST_COLNO;
#ifndef COMPRESSION
	clist[0].length		= sizeof(tidBuffer);
	clist[0].dataLength = sizeof(tidBuffer);
	clist[0].data.ptr	= tidBuffer;
#endif

	while((e = lom_DeferredDeletionList_Scan_NextElements(handle, deletionListScanId, DELETIONLIST_BUFFER_SIZE, sizeof(deletionListElements), deletionListElements)) != 0)
	{
		if(e < eNOERROR) LOM_ERROR(handle, e);
		nElementsRead = e;

		for(i = 0; i < nElementsRead; i++)
		{
			/* set bound condition */
			memcpy(&(bound.key.val[sizeof(Two)]), &deletionListElements[i].logicalId, sizeof(Four));

			docIdIndexScanId = e = LRDS_OpenIndexScan(LOM_GET_LRDS_HANDLE(handle), ornForDocIdIndexTable, &iid, &bound, &bound, 0, NULL, &lockup);
			if(e < eNOERROR) LOM_ERROR(handle, e);

			e = LRDS_NextTuple(LOM_GET_LRDS_HANDLE(handle), docIdIndexScanId, &tid, NULL);
			if(e < eNOERROR) LOM_ERROR(handle, e);

			if(e != EOS)
			{
				invertedDocIdElements[0].logicalId = deletionListElements[i].logicalId;

				sortTuple.len  = sizeof(invertedDocIdElements[0]);
				sortTuple.data = (char*)&invertedDocIdElements[0];
#ifndef COMPRESSION
				nTotalTidsRead = 0;
				while(1)
				{
					clist[0].start = sizeof(TupleID) * nTotalTidsRead;
					e = LRDS_FetchTuple(LOM_GET_LRDS_HANDLE(handle), docIdIndexScanId, SM_TRUE, &tid, 1, clist);
					if(e < eNOERROR) LOM_ERROR(handle, e);

					if(clist[0].retLength <= 0)
						break;

					nTidsRead = clist[0].retLength / sizeof(TupleID);
					nTotalTidsRead += nTidsRead;

					for(j = 0; j < nTidsRead; j++)
					{
						memcpy(&invertedDocIdElements[0].tid, &tidBuffer[j], sizeof(TupleID));

						e = LRDS_PutTuplesIntoSortStream(LOM_GET_LRDS_HANDLE(handle), sortStreamId, 1, &sortTuple);
						if(e < eNOERROR) LOM_ERROR(handle, e);
					}	
				}
#else
    			lengthInfoStruct.colNo = LOM_DOCIDTABLE_POINTERLIST_COLNO;

    			e = LRDS_FetchColLength(LOM_GET_LRDS_HANDLE(handle), docIdIndexScanId, SM_TRUE, &tid, 1, &lengthInfoStruct);
						if(e < eNOERROR) LOM_ERROR(handle, e);

				pointerBuffer = (char *)malloc(lengthInfoStruct.length);
    			if(pointerBuffer == NULL) LOM_ERROR(handle, eOUTOFMEMORY_LOM);

				clist[0].start 		= ALL_VALUE;
				clist[0].length		= lengthInfoStruct.length;
				clist[0].dataLength = lengthInfoStruct.length;
				clist[0].data.ptr	= pointerBuffer;

				e = LRDS_FetchTuple(LOM_GET_LRDS_HANDLE(handle), docIdIndexScanId, SM_TRUE, &tid, 1, clist);
				if(e < eNOERROR) 
				{
					if(pointerBuffer != NULL) free(pointerBuffer);
					LOM_ERROR(handle, e);
				}

				uncompressedDataLength = sizeof(TupleID) * INIT_NUMOF_DOCID_POINTER;

    			e = lom_Text_Uncompression(handle, clist[0].data.ptr, clist[0].retLength, &uncompressedData, &uncompressedDataLength);
    			if(e < eNOERROR) 
				{
					if(pointerBuffer != NULL) free(pointerBuffer);
					if(uncompressedData != NULL) free(uncompressedData);
					LOM_ERROR(handle, e);
				}

    			nTidsRead = (uncompressedDataLength - sizeof(VolNo)) / (sizeof(TupleID) - sizeof(VolNo));
    			pointerBuffer = (char *)realloc(pointerBuffer, nTidsRead * sizeof(TupleID));
    			if(pointerBuffer == NULL) 
    			{
        			if(uncompressedData != NULL) free(uncompressedData);
        			LOM_ERROR(handle, eOUTOFMEMORY_LOM);
    			}

    			memcpy(&volNo, uncompressedData, sizeof(VolNo));
    
    			inIndex = sizeof(VolNo);
    			outIndex = 0;
    			tmpPointerBuffer = (char*)pointerBuffer;
    
    			for(j = 0; j < nTidsRead; j++)
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

				for(j = 0; j < nTidsRead; j++)
				{
					memcpy(&invertedDocIdElements[0].tid, &pointerBuffer[j], sizeof(TupleID));

					e = LRDS_PutTuplesIntoSortStream(LOM_GET_LRDS_HANDLE(handle), sortStreamId, 1, &sortTuple);
					if(e < eNOERROR) 
					{
						if(pointerBuffer != NULL) free(pointerBuffer);
					LOM_ERROR(handle, e);
					}	
				}
				if(pointerBuffer != NULL) free(pointerBuffer);
#endif
			}

			e = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), docIdIndexScanId);
			if( e < eNOERROR) LOM_ERROR(handle, e);
		}
	}

	e = lom_DeferredDeletionList_Scan_Close(handle, deletionListScanId);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	e = LRDS_SortingSortStream(LOM_GET_LRDS_HANDLE(handle), sortStreamId);
	if(e < 0) LOM_ERROR(handle, e);

	done = SM_FALSE;
	deletionListOrn = LOM_USEROPENCLASSTABLE(handle)[ocn].ornForDeletionListTable;
	deletionListTid = &LOM_USEROPENCLASSTABLE(handle)[ocn].tidForDeletionList;

	ornForInvertedIndexTable = e = lom_Text_GetInvertedIndexTableORN(handle, ocnOrScanId, useScanFlag, textColNo);
	if(e < 0) LOM_ERROR(handle, e);

	/* init key value length */
	for(i = 0; i < KEYVALUE_BUFFER_SIZE; i++)
		kval[i].len = LOM_LONG_SIZE_VAR;
	kvalIndex = 0;
	memset(&currentTid, 0, sizeof(TupleID));
	memset(&prevTid, 0, sizeof(TupleID));
	while(1)
	{
		numSortTuples	= 1;
		sortTuple.len	= sizeof(invertedDocIdElements[0]);
		sortTuple.data	= (char*)&invertedDocIdElements[0];
		
		e = LRDS_GetTuplesFromSortStream(LOM_GET_LRDS_HANDLE(handle), sortStreamId, &numSortTuples, &sortTuple, &done);
		if(e < eNOERROR) LOM_ERROR(handle, e);

		if(!done) currentTid = invertedDocIdElements[0].tid;

		if(done || memcmp(&prevTid, &currentTid, sizeof(TupleID)) || kvalIndex >= KEYVALUE_BUFFER_SIZE)	
		{
			if(!(prevTid.pageNo == 0 && prevTid.slotNo == 0 && prevTid.unique == 0 && prevTid.volNo == 0))
			{
				/* flush kval buffer */
				e = LRDS_OrderedSet_DeleteElements(LOM_GET_LRDS_HANDLE(handle), ornForInvertedIndexTable, SM_FALSE, &prevTid,
												   LOM_INVERTEDINDEX_POSTINGLIST_COLNO, 
												   kvalIndex, &kval[0], NULL);
				if(e == eBADOBJECTID_OM) 
				{
					lom_PrintWarningMessage(handle, e);
				}
				else if(e < eNOERROR) LOM_ERROR(handle, e);

				kvalIndex = 0;
			}
		}

		if(done || memcmp(&prevTid, &currentTid, sizeof(TupleID)))	
		{
			if(!(prevTid.pageNo == 0 && prevTid.slotNo == 0 && prevTid.unique == 0 && prevTid.volNo == 0))
			{
				e = lom_UpdateInvertedIndexEntry(handle, ornForInvertedIndexTable, SM_FALSE, &prevTid);
				if(e == eBADOBJECTID_OM) 
				{
					lom_PrintWarningMessage(handle, e);
				}
				else if(e < eNOERROR) LOM_ERROR(handle, e);
			}
		}
		
		if(done) break;

		memcpy(&prevTid, &currentTid, sizeof(TupleID));
		memcpy(&(kval[kvalIndex].val[0]), &invertedDocIdElements[0].logicalId, LOM_LONG_SIZE_VAR);
		kvalIndex ++;
	}

	e = LRDS_CloseSortStream(LOM_GET_LRDS_HANDLE(handle), sortStreamId);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	return eNOERROR;
}

Four lom_Text_DeleteDocIdIndexEntryByDeferredDeletionList(
	LOM_Handle*		handle,			/* IN LOM system handle */
	Four			ocnOrScanId,	/* IN ocn or scan-id */
	Boolean			useScanFlag,	/* IN use ocnOrScanId as scanId if true */
	Two				textColNo		/* IN text column number that the operation applied */
)
{
	Four			e;
	Four			i;
	Four			deletionListScanId;
	Four			docIdIndexScanId;
	char			docId[sizeof(Two) + LOM_LONG_SIZE_VAR];
	BoundCond		bound;
	LockParameter	lockup;
	IndexID			iid;
	Four			ornForDocIdIndexTable;
	TupleID			tid;
	Four			nElementsRead;
	Two				keyLen;
	lom_DeletionListElement deletionListElements[DELETIONLIST_BUFFER_SIZE];

	e = lom_Text_GetDocIdIndex(handle, ocnOrScanId, useScanFlag, textColNo, &iid);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	e = ornForDocIdIndexTable = lom_Text_GetDocIdIndexTableORN(handle, ocnOrScanId, useScanFlag, textColNo);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	lockup.duration = L_COMMIT;
	lockup.mode     = L_X;

	/* prepare bound condition for index scan */
	keyLen			= LOM_LONG_SIZE_VAR;
	bound.op		= SM_EQ;
	bound.key.len	= sizeof(Two) + LOM_LONG_SIZE_VAR;
	memcpy(&(bound.key.val[0]), &keyLen, sizeof(Two));

	deletionListScanId = e = lom_DeferredDeletionList_Scan_Open(handle, ocnOrScanId, useScanFlag);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	while((e = lom_DeferredDeletionList_Scan_NextElements(handle, deletionListScanId, DELETIONLIST_BUFFER_SIZE, sizeof(deletionListElements), deletionListElements)) != 0)
	{
		if(e < eNOERROR) LOM_ERROR(handle, e);
		nElementsRead = e;

		for(i = 0; i < nElementsRead; i++)
		{
			/* set bound condition */
			bound.op		= SM_EQ;
			bound.key.len	= sizeof(Two) + LOM_LONG_SIZE_VAR;
			memcpy(&(bound.key.val[0]), &keyLen, sizeof(Two));
			memcpy(&(bound.key.val[sizeof(Two)]), &deletionListElements[i].logicalId, sizeof(Four));

			docIdIndexScanId = e = LRDS_OpenIndexScan(LOM_GET_LRDS_HANDLE(handle), ornForDocIdIndexTable, &iid, &bound, &bound, 0, NULL, &lockup);
			if(e < eNOERROR) LOM_ERROR(handle, e);

			e = LRDS_NextTuple(LOM_GET_LRDS_HANDLE(handle), docIdIndexScanId, &tid, NULL);
			if(e < eNOERROR) LOM_ERROR(handle, e);

			if(e != EOS)
			{
				keyLen = LOM_LONG_SIZE_VAR;
				memcpy(&docId[0], &keyLen, sizeof(Two));
				memcpy(&docId[sizeof(Two)], &deletionListElements[i].logicalId, keyLen);

				e = LRDS_Text_DeleteKeywords(LOM_GET_LRDS_HANDLE(handle), docIdIndexScanId, SM_TRUE, &tid, LOM_DOCIDTABLE_DOCID_COLNO, 1, docId);
				if(e < eNOERROR) LOM_ERROR(handle, e);

				e = LRDS_DestroyTuple(LOM_GET_LRDS_HANDLE(handle), docIdIndexScanId, SM_TRUE, &tid);
				if(e < eNOERROR) LOM_ERROR(handle, e);
			}

			e = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), docIdIndexScanId);
			if( e < eNOERROR) LOM_ERROR(handle, e);
		}
	}

	e = lom_DeferredDeletionList_Scan_Close(handle, deletionListScanId);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	return eNOERROR;
}

Four lom_Text_DestoryContentByDeferredDeletionList(
	LOM_Handle*		handle,			/* IN LOM system handle */
	Four			ocnOrScanId,	/* IN ocn or scan-id */
	Boolean			useScanFlag,	/* IN use ocnOrScanId as scanId if true */
	Two				textColNo		/* IN text column number that the operation applied */
)
{
	Four			e;
	Four			i;
	Four			deletionListScanId;
	LOM_TextDesc	textDesc;
	Four			ocn;
	Four			contentOrn;
	Four			nElementsRead;
	lom_DeletionListElement deletionListElements[DELETIONLIST_BUFFER_SIZE];

	if(useScanFlag) 
		ocn = LOM_SCANTABLE(handle)[ocnOrScanId].ocn;
	else 
		ocn = ocnOrScanId;

	contentOrn = LOM_USEROPENCLASSTABLE(handle)[ocn].ornForContentTable;

	deletionListScanId = e = lom_DeferredDeletionList_Scan_Open(handle, ocnOrScanId, useScanFlag);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	while((e = lom_DeferredDeletionList_Scan_NextElements(handle, deletionListScanId, DELETIONLIST_BUFFER_SIZE, sizeof(deletionListElements), deletionListElements)) != 0)
	{
		if(e < eNOERROR) LOM_ERROR(handle, e);
		nElementsRead = e;

		for(i = 0; i < nElementsRead; i++)
		{
			e = LOM_Text_GetDescriptor(handle, ocnOrScanId, useScanFlag, &deletionListElements[i].oid, 
									   (Two)GET_USERLEVEL_COLNO(textColNo), &textDesc);
			if(e == eBADOBJECTID_OM) 
			{
				lom_PrintWarningMessage(handle, e);
			}
			else if(e == eNOERROR)
			{
				if(!DOES_NOCONTENT_EXIST_TEXTDESC(textDesc)) 
				{
					e = LRDS_DestroyTuple(LOM_GET_LRDS_HANDLE(handle), contentOrn, SM_FALSE, &textDesc.contentTid);
					if(e < eNOERROR) LOM_ERROR(handle, e);
				}
			}
			else 
			{
				LOM_ERROR(handle, e);
			}
		}
	}

	e = lom_DeferredDeletionList_Scan_Close(handle, deletionListScanId);
	if(e < eNOERROR) LOM_ERROR(handle, e);


	return eNOERROR;
}

Four lom_CleanDeferredDeletionList(
	LOM_Handle*		handle,			/* IN LOM system handle */
	Four			ocnOrScanId,	/* IN ocn or scan-id */
	Boolean			useScanFlag)	/* IN use ocnOrScanId as scanId if true */
{
	Four		e;
	Four		deletionListOrn;
	TupleID*	deletionListTid;
	Four		ocn;

	if(useScanFlag) 
		ocn = LOM_SCANTABLE(handle)[ocnOrScanId].ocn;
	else 
		ocn = ocnOrScanId;

	if(LOM_USEROPENCLASSTABLE(handle)[ocn].ornForDeletionListTable == NIL)
	{
		char className[LOM_MAXCLASSNAME];

		/* get class name */
		e = lom_GetClassName(handle, LOM_USEROPENCLASSTABLE(handle)[ocn].volId, 
							 LOM_USEROPENCLASSTABLE(handle)[ocn].classID, className);
		if(e < eNOERROR) LOM_ERROR(handle, e);
			
		e = lom_OpenDeferredDeletionListTable(handle, LOM_USEROPENCLASSTABLE(handle)[ocn].volId, className);
		if(e == eRELATIONNOTFOUND_LRDS)
		{
			return eBADPARAMETER;
		}
		else if(e < eNOERROR) 
		{
			LOM_ERROR(handle, e);
		}

		LOM_USEROPENCLASSTABLE(handle)[ocn].ornForDeletionListTable = e;

		/* get first tuple id */
		e = lom_GetFirstTupleIdFromDeferredDeletionListTable(handle, 
																 LOM_USEROPENCLASSTABLE(handle)[ocn].ornForDeletionListTable, 
																 SM_FALSE,
																 &(LOM_USEROPENCLASSTABLE(handle)[ocn].tidForDeletionList));
		if(e < eNOERROR) LOM_ERROR(handle, e);
	}

	deletionListOrn = LOM_USEROPENCLASSTABLE(handle)[ocn].ornForDeletionListTable;
	deletionListTid = &LOM_USEROPENCLASSTABLE(handle)[ocn].tidForDeletionList;

	e = LRDS_CollectionList_DeleteAll(LOM_GET_LRDS_HANDLE(handle), deletionListOrn, SM_FALSE, deletionListTid,
		                              LOM_DELETIONLIST_DELETIONLIST_COLNO);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	return eNOERROR;
}

Four lom_CreateDeferredDeletionListTable(
	LOM_Handle*		handle,			/* IN LOM system handle */
	Four			volId,			/* IN volume id */
	char*			className)		/* IN class name */
{
	char		deletionListTableName[LOM_MAXCLASSNAME];	/* content class name */
	Four		e;											/* error code */
	ColInfo		cinfo[LOM_INVERTEDINDEX_NUM_COLS];
	Four		ocn;
	TupleID		tid;
    char*       attrNames[LOM_MAXNUMOFATTRIBUTE];
    Four        classId;

	/* check parameters */
	if (volId < 0) LOM_ERROR(handle, eBADPARAMETER_LOM);
	if (className == NULL) LOM_ERROR(handle, eBADPARAMETER_LOM);

	/* make inverted index table name */
	sprintf(deletionListTableName, "_%s_DeletionList", className);

	/* attribute information */
	/* deletionList */
	cinfo[LOM_DELETIONLIST_DELETIONLIST_COLNO].complexType	= SM_COMPLEXTYPE_COLLECTIONLIST;
	cinfo[LOM_DELETIONLIST_DELETIONLIST_COLNO].type			= SM_STRING;
	cinfo[LOM_DELETIONLIST_DELETIONLIST_COLNO].length		= LOM_DELETIONLIST_DELETIONLIST_ELEMENTSIZE;

	/* create realtion */
	e = LRDS_CreateRelation(LOM_GET_LRDS_HANDLE(handle), volId, deletionListTableName, NULL, LOM_DELETIONLIST_NUM_COLS, cinfo, SM_FALSE);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	/* create an empty list object */
	e = ocn = LRDS_OpenRelation(LOM_GET_LRDS_HANDLE(handle), volId, deletionListTableName);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	e = LRDS_CreateTuple(LOM_GET_LRDS_HANDLE(handle), ocn, SM_FALSE, 0, NULL, &tid);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	e = LRDS_CollectionList_Create(LOM_GET_LRDS_HANDLE(handle), ocn, SM_FALSE, &tid, LOM_DELETIONLIST_DELETIONLIST_COLNO);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	e = LRDS_CloseRelation(LOM_GET_LRDS_HANDLE(handle), ocn);
	if(e < eNOERROR) LOM_ERROR(handle, e);

    /* create lom catalog to access from SQL level */
    e = LOM_GetNewClassId(handle, volId, SM_FALSE, &classId);
    if(e < eNOERROR) LOM_ERROR(handle, e);

    attrNames[0] = "deletionList";
    e = Catalog_CreateLOMCatalogBasedOnLRDSCatalog(handle, volId, deletionListTableName, 1, attrNames, classId);
	if(e < eNOERROR) LOM_ERROR(handle, e);
    
	return eNOERROR;
}

Four lom_DestroyDeferredDeletionListTable(
	LOM_Handle*		handle,			/* IN LOM system handle */
	Four			volId,			/* IN volume id */
	char*			className)		/* IN class name */
{
	char	deletionListTableName[LOM_MAXCLASSNAME];
	Four	e;

	/* check parameters */
	if (volId < 0) LOM_ERROR(handle, eBADPARAMETER_LOM);
	if (className == NULL) LOM_ERROR(handle, eBADPARAMETER_LOM);

	/* make content table name */
	sprintf(deletionListTableName, "_%s_DeletionList", className);

    /* destroy class catalog */
    e = Catalog_DestroyClassCatalog(handle, volId, deletionListTableName);
    if(e < eNOERROR) LOM_ERROR(handle, e);

	/* destroy content table */
	e = LRDS_DestroyRelation(LOM_GET_LRDS_HANDLE(handle), volId, deletionListTableName);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	return eNOERROR;
}

Four lom_GetFirstTupleIdFromDeferredDeletionListTable(
	LOM_Handle*		handle,			/* IN LOM system handle */
	Four			ocnOrScanId,	/* IN ocn or scan-id of deletion list table*/
	Boolean			useScanFlag,	/* IN use ocnOrScanId as scanId if true */
	TupleID*		tid)			/* OUT tuple id of first tuple of the DeferredDeletionList */
{
	Four			e;
	Four			scanId;
	Four			orn;
	LockParameter	lockup;

	if(useScanFlag) 
		orn = LOM_SCANTABLE(handle)[ocnOrScanId].ocn;
	else 
		orn = ocnOrScanId;

	lockup.duration = L_COMMIT;
	lockup.mode     = L_IS;

	scanId = e = LRDS_OpenSeqScan(LOM_GET_LRDS_HANDLE(handle), orn, FORWARD, 0, NULL, &lockup);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	e = LRDS_NextTuple(LOM_GET_LRDS_HANDLE(handle), scanId, tid, NULL);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	if(e == EOS)
	{
		e = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), scanId);
		if(e < eNOERROR) LOM_ERROR(handle, e);

		LOM_ERROR(handle, eINTERNAL_LOM);
	}

	e = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), scanId);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	return eNOERROR;
}

Four lom_OpenDeferredDeletionListTable(
	LOM_Handle*		handle,			/* IN LOM system handle */
	Four			volId,			/* IN volume id */
	char*			className)		/* IN class name */
{
	char	deletionListTableName[LOM_MAXCLASSNAME];
	Four	e;
	Four	orn;

	/* check parameters */
	if (volId < 0) LOM_ERROR(handle, eBADPARAMETER_LOM);
	if (className == NULL) LOM_ERROR(handle, eBADPARAMETER_LOM);

	/* make content table name */
	sprintf(deletionListTableName, "_%s_DeletionList", className);

	/* open content table */
	e = orn = LRDS_OpenRelation(LOM_GET_LRDS_HANDLE(handle), volId, deletionListTableName);
	if(e == eRELATIONNOTFOUND_LRDS) return e;
	else if(e < eNOERROR) LOM_ERROR(handle, e);

	return orn;
}

Four lom_CloseDeferredDeletionListTable(
	LOM_Handle*		handle,			/* IN LOM system handle */
	Four			orn)			/* IN open class num. */
{
	Four e;

	e = LRDS_CloseRelation(LOM_GET_LRDS_HANDLE(handle), orn);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	return eNOERROR;
}

Four lom_AppendObjectToDeferredDeletionList(
	LOM_Handle*		handle,			/* IN LOM system handle */
	Four			ocnOrScanId,	/* IN ocn or scan-id */
	Boolean			useScanFlag,	/* IN use ocnOrScanId as scanId if true */
	OID*			oid)			/* OUT object id of the object to be destroyed */
{
	Four		e;
	Four		logicalId;
	lom_DeletionListElement deletionListElement[1];
	Four		deletionListOrn;
	TupleID*	deletionListTid;
	Four		ocn;

	if(useScanFlag) 
		ocn = LOM_SCANTABLE(handle)[ocnOrScanId].ocn;
	else 
		ocn = ocnOrScanId;

	if(LOM_USEROPENCLASSTABLE(handle)[ocn].ornForDeletionListTable == NIL)
	{
		char className[LOM_MAXCLASSNAME];

		/* get class name */
		e = lom_GetClassName(handle, LOM_USEROPENCLASSTABLE(handle)[ocn].volId, 
							 LOM_USEROPENCLASSTABLE(handle)[ocn].classID, className);
		if(e < eNOERROR) LOM_ERROR(handle, e);
			
		e = lom_OpenDeferredDeletionListTable(handle, LOM_USEROPENCLASSTABLE(handle)[ocn].volId, className);
		if(e == eRELATIONNOTFOUND_LRDS)
		{
			/* create deletion list table and open it */
			e = lom_CreateDeferredDeletionListTable(handle, LOM_USEROPENCLASSTABLE(handle)[ocn].volId, className);
			if(e < eNOERROR) LOM_ERROR(handle, e);

			LOM_USEROPENCLASSTABLE(handle)[ocn].ornForDeletionListTable = e = 
				lom_OpenDeferredDeletionListTable(handle, LOM_USEROPENCLASSTABLE(handle)[ocn].volId, className);
			if(e < eNOERROR) LOM_ERROR(handle, e);

			/* get first tuple id */
			e = lom_GetFirstTupleIdFromDeferredDeletionListTable(handle, 
																 LOM_USEROPENCLASSTABLE(handle)[ocn].ornForDeletionListTable, 
																 SM_FALSE,
																 &(LOM_USEROPENCLASSTABLE(handle)[ocn].tidForDeletionList));
			if(e < eNOERROR) LOM_ERROR(handle, e);
		}
		else if(e >= eNOERROR)
		{
			LOM_USEROPENCLASSTABLE(handle)[ocn].ornForDeletionListTable = e;

			/* get first tuple id */
			e = lom_GetFirstTupleIdFromDeferredDeletionListTable(handle, 
																 LOM_USEROPENCLASSTABLE(handle)[ocn].ornForDeletionListTable, 
																 SM_FALSE,
																 &(LOM_USEROPENCLASSTABLE(handle)[ocn].tidForDeletionList));
			if(e < eNOERROR) LOM_ERROR(handle, e);
		}
		else if(e < eNOERROR)
		{
			LOM_ERROR(handle, e);
		}
	}

	/* get logical id from oid */
	if(LOM_USEROPENCLASSTABLE(handle)[ocn].numOfTextAttrs > 0)
	{
		e = logicalId = lom_Text_GetLogicalId(handle, ocnOrScanId, useScanFlag, oid);
		if(e < eNOERROR) LOM_ERROR(handle, e);
	}
	else
		logicalId = -1;

	/* construct deletionListElement */
	deletionListElement[0].logicalId	= logicalId;
	deletionListElement[0].oid			= *oid;

	/* append it to deletion list */
	deletionListOrn = LOM_USEROPENCLASSTABLE(handle)[ocn].ornForDeletionListTable;
	deletionListTid = &LOM_USEROPENCLASSTABLE(handle)[ocn].tidForDeletionList;

	e = LRDS_CollectionList_AppendElements(LOM_GET_LRDS_HANDLE(handle), deletionListOrn, SM_FALSE, deletionListTid,
		                                   LOM_DELETIONLIST_DELETIONLIST_COLNO, 1,
										   NULL, deletionListElement);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	return eNOERROR;
}

Four lom_DeferredDeletionList_Scan_Open(
	LOM_Handle*		handle,			/* IN LOM system handle */
	Four			ocnOrScanId,	/* IN ocn or scan-id */
	Boolean			useScanFlag)	/* IN use ocnOrScanId as scanId if true */
{
	Four		scanId;
	Four		e;
	Four		deletionListOrn;
	TupleID*	deletionListTid;
	Four		ocn;

	if(useScanFlag) 
		ocn = LOM_SCANTABLE(handle)[ocnOrScanId].ocn;
	else 
		ocn = ocnOrScanId;

	if(LOM_USEROPENCLASSTABLE(handle)[ocn].ornForDeletionListTable == NIL)
		return eBADPARAMETER;

	/* get deletionListTable's orn */
	deletionListOrn = LOM_USEROPENCLASSTABLE(handle)[ocn].ornForDeletionListTable;

	/* get first object's oid */
	deletionListTid = &LOM_USEROPENCLASSTABLE(handle)[ocn].tidForDeletionList;

	e = scanId = LRDS_CollectionList_Scan_Open(LOM_GET_LRDS_HANDLE(handle), deletionListOrn, SM_FALSE, deletionListTid,
		                                       LOM_DELETIONLIST_DELETIONLIST_COLNO);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	return scanId;
}

Four lom_DeferredDeletionList_Scan_Close(
	LOM_Handle*		handle,			/* IN LOM system handle */
	Four			scanId)			/* IN scan id to close */
{
	Four		e;

	e = LRDS_CollectionList_Scan_Close(LOM_GET_LRDS_HANDLE(handle), scanId);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	return eNOERROR;
}

Four lom_DeferredDeletionList_Scan_NextElements(
	LOM_Handle*		handle,			/* IN LOM system handle */
	Four			scanId,			/* IN scan id */
    Four			nElements,      /* IN # of elements to read */
	Four			elementsSize,	/* IN size of elements */
    void*			elements)       /* OUT buffer to return the read elements */
{
	Four		e;

	e = LRDS_CollectionList_Scan_NextElements(LOM_GET_LRDS_HANDLE(handle), scanId, nElements, NULL, elementsSize, elements);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	return e;
}

Four lom_ShowDeferredDeletionListStatus(
	LOM_Handle*		handle,			/* IN LOM system handle */
	Four			volId,			
	char*			className		
)
{
	Four				e;
	Four				ocn;
	Four				deletionListScanId;
	Four				nElementsRead;
	Four				nTotalElementsRead;
	lom_DeletionListElement		deletionListElements[DELETIONLIST_BUFFER_SIZE];

	/* check parameters */
    if (volId < 0) LOM_ERROR(handle, eBADPARAMETER_LOM);
    if (className == NULL) LOM_ERROR(handle, eBADPARAMETER_LOM);

	/* open class */
    ocn = e = LOM_OpenClass(handle, volId, className);
    if(e < eNOERROR) LOM_ERROR(handle, e);

	if(LOM_USEROPENCLASSTABLE(handle)[ocn].ornForDeletionListTable == NIL)
	{
		char className[LOM_MAXCLASSNAME];

		/* get class name */
		e = lom_GetClassName(handle, LOM_USEROPENCLASSTABLE(handle)[ocn].volId, 
							 LOM_USEROPENCLASSTABLE(handle)[ocn].classID, className);
		if(e < eNOERROR) LOM_ERROR(handle, e);
			
		e = lom_OpenDeferredDeletionListTable(handle, LOM_USEROPENCLASSTABLE(handle)[ocn].volId, className);
		if(e == eRELATIONNOTFOUND_LRDS)
		{
			return eNOERROR;
		}
		else if(e < eNOERROR) 
		{
			LOM_ERROR(handle, e);
		}

		LOM_USEROPENCLASSTABLE(handle)[ocn].ornForDeletionListTable = e;

		/* get first tuple id */
		e = lom_GetFirstTupleIdFromDeferredDeletionListTable(handle, 
																 LOM_USEROPENCLASSTABLE(handle)[ocn].ornForDeletionListTable, 
																 SM_FALSE,
																 &(LOM_USEROPENCLASSTABLE(handle)[ocn].tidForDeletionList));
		if(e < eNOERROR) LOM_ERROR(handle, e);
	}

	deletionListScanId = e = lom_DeferredDeletionList_Scan_Open(handle, ocn, SM_FALSE);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	nTotalElementsRead = 0;
	while((e = lom_DeferredDeletionList_Scan_NextElements(handle, deletionListScanId, DELETIONLIST_BUFFER_SIZE, sizeof(deletionListElements), deletionListElements)) != 0)
	{
		if(e < eNOERROR) LOM_ERROR(handle, e);
		nElementsRead = e;

		nTotalElementsRead += nElementsRead;
	}

	e = lom_DeferredDeletionList_Scan_Close(handle, deletionListScanId);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	/* close class */
	e = LOM_CloseClass(handle, ocn);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	printf("Total %ld objects are deferred to delete in the class '%s'\n", nTotalElementsRead, className);

	return eNOERROR;
}
