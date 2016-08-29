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

/*
 * File: LOM_Text.c
 *
 * Description:
 * Text facilities support in LOM
 *
 * Imports:
 *
 * Exports:
 *	Four lom_Text_CreateContentClass(handle, Four volId, char *className);
 *	Four lom_Text_CreateInvertedIndexTable(handle, Four volId, char *className, char *attrName);
 *	Four lom_Text_DestroyContentClass(handle, Four volId, char *className);
 *
 * Returns:
 *  Error code
 *     eBADPARAMETER_LOM
 *     eCOLUMNVALUEEXPECTED_LRDS
 *     eWRONGCOLUMNVALUE_LRDS
 *     some errors caused by function calls
 *
 * Assumption:
 */


#include <malloc.h>
#include <string.h>
#include <stdlib.h>
#include "LOM_Internal.h"
#include "LOM.h"
#include "Catalog.h"
#include "Catalog_Internal.h"

Four LOM_Text_GetOIDFromLogicalDocId(
	LOM_Handle	*handle,
	Four		ocn,
	Four		logicalDocId,
	OID			*oid
)
{
	Four e;
	Four v;
	Four i;
	Four volId;
	Four idxForClassInfo;
	Four lomScanNum;
	catalog_SysIndexesOverlay *ptrToSysIndexes;	
	catalog_SysClassesOverlay *ptrToSysClasses;
	catalog_SysAttributesOverlay *ptrToSysAttributes;
	LOM_IndexID iid;
	BoundCond bound;		/* stop bound */
	LockParameter lockup;

	volId = LOM_USEROPENCLASSTABLE(handle)[ocn].volId;

	/* get class information */
	e = Catalog_GetClassInfo(handle, volId, LOM_USEROPENCLASSTABLE(handle)[ocn].classID, &idxForClassInfo);
	if( e < eNOERROR) LOM_ERROR(handle, e);

	v = Catalog_GetVolIndex(handle, volId);
    if(v < eNOERROR) LOM_ERROR(handle, v);

	/* set memory pointer */
	ptrToSysClasses = &CATALOG_GET_CLASSINFOTBL(handle, v)[idxForClassInfo];

	ptrToSysIndexes =  &CATALOG_GET_INDEXINFOTBL(handle, v)[CATALOG_GET_INDEXINFOTBL_INDEX(ptrToSysClasses)];

	ptrToSysAttributes =  &CATALOG_GET_ATTRINFOTBL(handle, v)[CATALOG_GET_ATTRINFOTBL_INDEX(ptrToSysClasses)];

	for(i = 0; i < CATALOG_GET_INDEXNUM(ptrToSysClasses); i++) {
		if(CATALOG_GET_INDEXCOLNO(ptrToSysIndexes)[i] == LOM_LOGICALID_COLNO)  {
			iid = ptrToSysIndexes->iid;
			break;
		}
	}

	if(i == CATALOG_GET_INDEXNUM(ptrToSysClasses)) LOM_ERROR(handle, eINTERNAL_LOM);

	/* make bound condition */
	bound.op = SM_EQ;
	bound.key.len = LOM_LONG_SIZE_VAR;
	memcpy(&logicalDocId, &(bound.key.val[0]), LOM_LONG_SIZE_VAR);

	/* lock parameter */
	/* we here donot release file-level lock until commit */
	lockup.mode = L_IS;
	lockup.duration = L_COMMIT;

	/* open index scan */
	lomScanNum = LOM_OpenIndexScan(handle, ocn, &iid, &bound, &bound, 0, (BoolExp *)NULL, &lockup);
	if(lomScanNum < eNOERROR) LOM_ERROR(handle, lomScanNum);

	e = LOM_NextObject(handle, lomScanNum, oid, NULL);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	if(e == EOS) return eOBJECTNOTFOUND_LOM;

	e = LOM_CloseScan(handle, lomScanNum);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	return eNOERROR;
}


/* START: PostingStructure */
/*****************************************************/
/* User-Defined Posting Structure Related Interfaces */
/*****************************************************/
Four LOM_Text_DefinePostingStructure(
	LOM_Handle *handle,
	Four volId,
	char *className,
	char *attrName,
	PostingStructureInfo *postingInfo
)
{
	Four orn;
	Four e;
	LockParameter lockup;
	char invertedIndexTableName[LOM_MAXCLASSNAME]; /* content class name */
	ColListStruct clist[1];
	Two colNo;
	PostingStructureInfo newPostingInfo;
	Four i;
	Four osn;
	TupleID tid;
	LRDS_Cursor *cursor;
	BoundCond bound;
	Two keyLen;
	lrds_RelTableEntry *relTableEntry;
	ClassID classId;

	if(handle == NULL || volId < 0 || className == NULL || attrName == NULL || postingInfo == NULL)
		LOM_ERROR(handle, eBADPARAMETER_LOM);

	if(postingInfo->nEmbeddedAttributes < 0 || postingInfo->nEmbeddedAttributes > MAX_NUM_EMBEDDEDATTRIBUTES)
		LOM_ERROR(handle, eBADPARAMETER_LOM);

	/* column number conversion */
	newPostingInfo = *postingInfo;
	for(i = 0; i < postingInfo->nEmbeddedAttributes; i++) 
		newPostingInfo.embeddedAttrNo[i] = GET_SYSTEMLEVEL_COLNO(postingInfo->embeddedAttrNo[i]);

	lockup.mode = L_IX;
	lockup.duration = L_COMMIT;

	/* check if class has already objects */
	orn = LRDS_OpenRelation(LOM_GET_LRDS_HANDLE(handle), volId, className);
	if (orn < 0) LOM_ERROR(handle, orn);

	osn = LRDS_OpenSeqScan(LOM_GET_LRDS_HANDLE(handle), orn, FORWARD, 0, NULL, &lockup);
	if (osn < 0) LOM_ERROR(handle, osn);

	e = LRDS_NextTuple(LOM_GET_LRDS_HANDLE(handle), osn, &tid, &cursor);
	if( e < eNOERROR) LOM_ERROR(handle, e);

	if(e != EOS) {
		e = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), osn);
		if( e < eNOERROR) LOM_ERROR(handle, e);

		e = LRDS_CloseRelation(LOM_GET_LRDS_HANDLE(handle), orn);
		if( e < eNOERROR) LOM_ERROR(handle, e);

		LOM_ERROR(handle, eOBJECTSALREADYEXIST_LOM);
	}

	e = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), osn);
	if( e < eNOERROR) LOM_ERROR(handle, e);

	e = LRDS_CloseRelation(LOM_GET_LRDS_HANDLE(handle), orn);
	if( e < eNOERROR) LOM_ERROR(handle, e);

	orn = LRDS_OpenRelation(LOM_GET_LRDS_HANDLE(handle), volId, LOM_SYSTEXTINDEXES_CLASSNAME);
	if (orn < 0) LOM_ERROR(handle, orn);

	relTableEntry = LRDS_GET_RELTABLE_ENTRY(LOM_GET_LRDS_HANDLE(handle), orn);

	/* make inverted index table name */
	sprintf(invertedIndexTableName, "_%s_%s_Inverted", className, attrName);

	/* make boundary condition */
	bound.op = SM_EQ;
	keyLen = strlen(invertedIndexTableName);
	bound.key.len = sizeof(Two) + keyLen;
	bcopy(&keyLen,&(bound.key.val[0]),sizeof(Two));
	bcopy(invertedIndexTableName,&(bound.key.val[sizeof(Two)]),keyLen);

	/* open index scan */
	osn = LRDS_OpenIndexScan(LOM_GET_LRDS_HANDLE(handle), orn, &(LRDS_GET_IDXINFO_FROM_RELTABLE_ENTRY(relTableEntry))[0].iid, &bound, &bound,0, (BoolExp*)NULL, &lockup);
	if (osn < 0) LOM_ERROR(handle, osn);

	e = LRDS_NextTuple(LOM_GET_LRDS_HANDLE(handle), osn, &tid, &cursor);
	if( e < eNOERROR) LOM_ERROR(handle, e);

	if(e == EOS) LOM_ERROR(handle, eNOSUCHINDEXENTRY_LOM);

	clist[0].colNo = LOM_SYSTEXTINDEXES_COLUMNNO_COLNO;
	clist[0].start = ALL_VALUE;
	clist[0].dataLength = clist[0].length = LOM_SHORT_SIZE_VAR;

	e = LRDS_FetchTuple(LOM_GET_LRDS_HANDLE(handle), osn, SM_TRUE, NULL, 1, clist);
	if( e < eNOERROR) LOM_ERROR(handle, e);

	/* get column number */
	colNo = GET_VALUE_FROM_COL_LIST(clist[0], sizeof(colNo));

	clist[0].colNo = LOM_SYSTEXTINDEXES_POSTINGSTRUCTUREINFO_COLNO;
	clist[0].start = ALL_VALUE;
	clist[0].dataLength = sizeof(PostingStructureInfo);
	clist[0].data.ptr = &newPostingInfo;
	clist[0].nullFlag = SM_FALSE;

	e = LRDS_UpdateTuple(LOM_GET_LRDS_HANDLE(handle), osn, SM_TRUE, NULL, 1, clist);
	if( e < eNOERROR) LOM_ERROR(handle, e);

	e = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), osn);
	if( e < eNOERROR) LOM_ERROR(handle, e);

	e = LRDS_CloseRelation(LOM_GET_LRDS_HANDLE(handle), orn);
	if( e < eNOERROR) LOM_ERROR(handle, e);

	classId = lom_GetClassId(handle, volId, className, &classId);
	if( e < eNOERROR) LOM_ERROR(handle, e);

	e = Catalog_Text_DefinePostingStructure(handle, volId, classId, colNo, &newPostingInfo);
	if( e < eNOERROR) LOM_ERROR(handle, e);

	return eNOERROR;
}
/* END: PostingStructure */

/***************************************/
/* Text Object Manipulation Interfaces */
/***************************************/

Four LOM_Text_CreateContent(
	LOM_Handle *handle, 
	Four ocnOrScanId,	/* IN: one of the following open scan numbers : LOM_OpenScan, LOM_OpenIndexScan, LOM_Text_OpenIndexScan, LOM_Text_Scan_Open */
	Boolean useScanFlag,	/* IN: flag */
	OID *oid,		/* IN: object containging the give text type attribute */
	Two colNum,		/* IN: column number */
	TextColStruct *text,	/* IN: text column struct */
	LOM_TextDesc	*textDesc 	/* INOUT: text descriptor */
)
{
	ColListStruct clist[1];		/* collist struct */
	Four e,e2;				/* error code */
	lom_PostingBuffer postingBuffer;	/* posting buffer */
#ifdef COMPRESSION
    lom_PostingBuffer compressedPostingBuffer;	/* compressed posting-list buffer */
#endif	
	Four nKeywords;				/* number of keywords */
	char keyword[LOM_MAXKEYWORDSIZE];	/* temporary variable */
	char *ptrToPosting;			/* ptr to Posting */
	Four offset;				/* offset */
	Four logicalDocId;			/* document logical id */
	Four orn;				/* open class number for the given class */
	Two colNo;				/* column number : given by system not user */
	TupleID tidForInvertedIndexEntry;
#ifndef COMPRESSION
	TupleID *pointerBuffer;
#else
    char	*pointerBuffer;
    Four    pointerBufferSize;
#endif
	Four pointerBufferIdx;
	Boolean isLogicalIdOrder;
	Boolean newlyRegisteredKeyword;
#ifdef SUBINDEX
	Four postingLengthFieldSize = LOM_LONG_SIZE_VAR;
#else
	Four postingLengthFieldSize = 0;
#endif
	Four scanId;
	Four postingLength;
	LOM_TextDesc tmpTextDesc;
	lom_FptrToKeywordExtractor keywordExtractorFPtr;
	lom_FptrToGetNextPostingInfo getNextPostingInfoFPtr;
	lom_FptrToFinalizeKeywordExtraction finalizeKeywordExtractionFPtr;
	lom_FptrToFilter filterFPtr;
#ifdef COMPRESSION
    lom_Text_PostingInfoForReading  postingInfo;      
    char    *compressedData = NULL; 
    Four    compressedDataLength;
    Four    sizeofTupleIDwithoutVolNo = sizeof(TupleID) - sizeof(VolNo);
#endif

	/* check parameter */
	if(ocnOrScanId < 0) LOM_ERROR(handle, eBADPARAMETER_LOM);
	if(oid == NULL) LOM_ERROR(handle, eBADPARAMETER_LOM);
	if(text == NULL < 0) LOM_ERROR(handle, eBADPARAMETER_LOM);
	if(colNum < 0) LOM_ERROR(handle, eBADPARAMETER_LOM);
	if(textDesc == NULL) LOM_ERROR(handle, eBADPARAMETER_LOM);
	if(text->dataLength < 0)  LOM_ERROR(handle, eBADPARAMETER_LOM);

	if(!DOES_NOCONTENT_EXIST_TEXTDESC(*textDesc)) LOM_ERROR(handle, eBADPARAMETER_LOM);

	/* get open class number */
	if(useScanFlag) {
		scanId = LOM_SCANTABLE(handle)[ocnOrScanId].lrdsScanId;

		if (!LRDS_VALID_SCANID(LOM_GET_LRDS_HANDLE(handle), scanId)) LOM_ERROR(handle, eBADPARAMETER_LOM);

		orn = LRDS_SCANTABLE(LOM_GET_LRDS_HANDLE(handle))[scanId].orn;
	}
	else orn = ocnOrScanId;
	
	/* convert user-known column number to system column number */
	colNo = GET_SYSTEMLEVEL_COLNO(colNum);

#ifdef COMPRESSION
    e = lom_Text_PreparePostingInfoForReading(handle, lom_GetVolIdFromOcn(handle, orn),
                                              LOM_USEROPENCLASSTABLE(handle)[orn].classID,
                                              colNo, &postingInfo);
    if(e < 0) LOM_ERROR(handle, e);
#endif

	/* create content data */
	e = lom_Text_CreateContentData(
		handle,  
		ocnOrScanId,
		useScanFlag,
		text->dataLength, 
		text->data, 
		&textDesc->contentTid);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	textDesc->size = text->dataLength;

	tmpTextDesc.size = textDesc->size;
	tmpTextDesc.contentTid = textDesc->contentTid;
	if(text->indexMode == LOM_IMMEDIATE_MODE) {
		tmpTextDesc.isIndexed = SM_TRUE;
		tmpTextDesc.hasBeenIndexed = SM_TRUE;
	}
	else {
		tmpTextDesc.isIndexed = SM_FALSE;
		tmpTextDesc.hasBeenIndexed = textDesc->hasBeenIndexed;
	}

	/* store text descriptor information */
	clist[0].colNo = colNo; 
	clist[0].start = ALL_VALUE;
	clist[0].length = sizeof(LOM_TextDesc);
	clist[0].dataLength = sizeof(LOM_TextDesc);
	clist[0].data.ptr = &tmpTextDesc;
	clist[0].nullFlag = SM_FALSE;

	/* update text descriptor */
	if(useScanFlag)
		e = LRDS_UpdateTuple(LOM_GET_LRDS_HANDLE(handle), scanId, useScanFlag, (TupleID *)oid, 1, &clist[0]);
	else
		e = LRDS_UpdateTuple(LOM_GET_LRDS_HANDLE(handle), ocnOrScanId, useScanFlag, (TupleID *)oid, 1, &clist[0]);
	if( e < eNOERROR) LOM_ERROR(handle, e);

	if(text->indexMode == LOM_IMMEDIATE_MODE) {

		/* get logical id */
		logicalDocId = lom_Text_GetLogicalId(handle,  ocnOrScanId, useScanFlag, oid);
		if(logicalDocId < 0) LOM_ERROR(handle, logicalDocId);

		/* allocate posting buffer */
		e = LOM_AllocPostingBuffer(handle, &postingBuffer, LOM_DEFAULTPOSTINGBUFFERSIZE);
		if(e < eNOERROR) LOM_ERROR(handle, e);
		LOM_FREEOFFSET_POSTINGBUFFER(postingBuffer) = 0;

#ifndef COMPRESSION
		pointerBuffer = (TupleID *)malloc(sizeof(TupleID) * INIT_NUMOF_DOCID_POINTER);
		if(pointerBuffer == NULL) LOM_ERROR(handle, eOUTOFMEMORY_LOM);
		pointerBufferIdx = 0;
#else
    	pointerBufferSize = sizeofTupleIDwithoutVolNo * INIT_NUMOF_DOCID_POINTER + sizeof(VolNo);
    	pointerBuffer = (char *)malloc(pointerBufferSize);
    	if(pointerBuffer == NULL) LOM_ERROR(handle, eOUTOFMEMORY_LOM);
    	pointerBufferIdx  = 0;
#endif

	
#ifndef COMPRESSION

#define DEALLOCMEM_LOM_Text_CreateContent(handle, e, e2)	\
    	e2 = LOM_FreePostingBuffer(handle, &postingBuffer);	\
    	if(e2 < eNOERROR) LOM_ERROR(handle, e2);		\
    	free(pointerBuffer);				\
    	LOM_ERROR(handle, e);	    

#else

        e = LOM_AllocPostingBuffer(handle, &compressedPostingBuffer, LOM_DEFAULTPOSTINGBUFFERSIZE);
		if(e < eNOERROR) 
		{
		    e2 = LOM_FreePostingBuffer(handle, &postingBuffer);	
    	    if(e2 < eNOERROR) LOM_ERROR(handle, e2);		
		    LOM_ERROR(handle, e);
		}
		LOM_FREEOFFSET_POSTINGBUFFER(compressedPostingBuffer) = 0;
			    
        compressedDataLength = compressBound(pointerBufferSize);
	    compressedData = (char *)malloc(compressedDataLength + sizeof(char));
	    if(compressedData == NULL) 
	    {
	        e2 = LOM_FreePostingBuffer(handle, &postingBuffer);
        	if(e2 < eNOERROR) LOM_ERROR(handle, e2);	
        	e2 = LOM_FreePostingBuffer(handle, &compressedPostingBuffer);
        	if(e2 < eNOERROR) LOM_ERROR(handle, e2);	
	        LOM_ERROR(handle, eOUTOFMEMORY_LOM);
	    }

#define DEALLOCMEM_LOM_Text_CreateContent(handle, e, e2)	\
	e2 = LOM_FreePostingBuffer(handle, &postingBuffer);	\
	if(e2 < eNOERROR) LOM_ERROR(handle, e2);		\
    	e2 = LOM_FreePostingBuffer(handle, &compressedPostingBuffer);	\
    	if(e2 < eNOERROR) LOM_ERROR(handle, e2);		\
	free(pointerBuffer);				\
    	if(compressedData != NULL) free(compressedData); \
	LOM_ERROR(handle, e);
#endif

		/* extract keywords from content buffer and put keywords and postings
		   into posting buffer */
		e = lom_Text_GetKeywordExtractorFPtr(handle, orn, colNo, &keywordExtractorFPtr);
		if(e < eNOERROR) LOM_ERROR(handle, e);
		e = lom_Text_GetGetNextPostingInfoFPtr(handle, orn, colNo, &getNextPostingInfoFPtr);
		if(e < eNOERROR) LOM_ERROR(handle, e);
		e = lom_Text_GetFinalizeKeywordExtractionFPtr(handle, orn, colNo, &finalizeKeywordExtractionFPtr);
		if(e < eNOERROR) LOM_ERROR(handle, e);
		e = lom_Text_GetFilterFPtr(handle, orn, colNo, &filterFPtr);
		if(e < eNOERROR) LOM_ERROR(handle, e);

		e = lom_Text_ExtractKeywordFromContentBufferIntoPostingBuffer(
			handle, 
			keywordExtractorFPtr,
			getNextPostingInfoFPtr,
			finalizeKeywordExtractionFPtr,
			filterFPtr,
			ocnOrScanId,
			useScanFlag,
			oid,
			logicalDocId,
			colNo,
			LOM_DEFAULTPOSTINGBUFFERSIZE,
			LOM_PTR_POSTINGBUFFER(postingBuffer),
			&nKeywords);
		if(e < eNOERROR) {
			LOM_ERROR(handle, eFAILTOCALLKEYWORDEXTRACTOR_LOM);
		}

		/* set pointer to posting buffer */
		ptrToPosting = LOM_PTR_POSTINGBUFFER(postingBuffer);
		offset = 0;

		if(HASBEENINDEXED_TEXTDESC(*textDesc)) isLogicalIdOrder = SM_TRUE;
		else isLogicalIdOrder = SM_FALSE;

		/* handle when no keywords are extracted */
		while(nKeywords) {
			/* get keyword */
			strcpy(keyword, &ptrToPosting[offset]);
			offset += strlen(keyword) + 1; /* skip null character */

			/* get postingLength */
			bcopy(&ptrToPosting[offset], &postingLength, sizeof(Four));

			/* add inverted index entry from posting buffer */
			e = lom_Text_AddInvertedIndexEntryFromBuf(
				handle, 
				lom_GetVolIdFromOcn(handle, orn),			
				ocnOrScanId,
				useScanFlag,
				-1,	-1,				/* dummy bulk loading id */
				SM_FALSE,			/* do not use bulk loading feature */
				colNo,
				keyword,
#ifdef COMPRESSION
				&postingInfo,
				LOM_PTR_POSTINGBUFFER(compressedPostingBuffer),
#endif
				1,
				postingLengthFieldSize + postingLength,
				&ptrToPosting[offset],
				&tidForInvertedIndexEntry,
				isLogicalIdOrder,
				TRUE,
				&newlyRegisteredKeyword);
			if(e < eNOERROR) {
				DEALLOCMEM_LOM_Text_CreateContent(handle, e, e2);
			}
	
#ifndef COMPRESSION
			if(pointerBufferIdx >= INIT_NUMOF_DOCID_POINTER) {
				e = lom_Text_AddDocIdIndexEntryFromBuf(
					handle, 
					ocnOrScanId,
					useScanFlag,
					-1,	-1,			/* dummy bulk load id */
					SM_FALSE,		/* disable bulk loading */
					colNo, 
					logicalDocId, 
					pointerBufferIdx, 
					pointerBuffer);
				if(e < eNOERROR) {
					DEALLOCMEM_LOM_Text_CreateContent(handle, e, e2);
				}

				pointerBufferIdx = 0;
			}
			pointerBuffer[pointerBufferIdx] = tidForInvertedIndexEntry;
			pointerBufferIdx++;
#else
            if(pointerBufferIdx + sizeof(TupleID) >= pointerBufferSize) 
			{
                pointerBufferSize *= 2;
                pointerBuffer = (char *)realloc(pointerBuffer, pointerBufferSize * 2);
                if(pointerBuffer == NULL) LOM_ERROR(handle, eOUTOFMEMORY_LOM);

                compressedDataLength = compressBound(pointerBufferSize);
                compressedData = (char *)realloc(compressedData, compressedDataLength + 1);
                if(compressedData == NULL) LOM_ERROR(handle, eOUTOFMEMORY_LOM);
            }

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

			nKeywords--;

			offset += postingLengthFieldSize + postingLength;
		}
	
		if(pointerBufferIdx != 0) {
			e = lom_Text_AddDocIdIndexEntryFromBuf(
				handle, 
				ocnOrScanId,
				useScanFlag,
				-1,	-1,			/* dummy bulk load id */
				SM_FALSE,		/* disable bulk loading */
				colNo, 
				logicalDocId, 
				pointerBufferIdx, 
#ifndef COMPRESSION
				pointerBuffer);
#else
				pointerBuffer,
				compressedData,
				&compressedDataLength);
#endif
			if(e < eNOERROR) LOM_ERROR(handle, e);
		}
		textDesc->isIndexed = SM_TRUE;

		textDesc->hasBeenIndexed = SM_TRUE;

		free(pointerBuffer);

#ifdef COMPRESSION
	    if(compressedData != NULL) free(compressedData);
    
        e = LOM_FreePostingBuffer(handle, &compressedPostingBuffer);	
    	if(e < eNOERROR) LOM_ERROR(handle, e);	    
#endif

		e = LOM_FreePostingBuffer(handle, &postingBuffer);
		if(e < eNOERROR) LOM_ERROR(handle, e);
	}
	else textDesc->isIndexed = SM_FALSE;

	return eNOERROR;
}


/*
 * Function: Four LOM_Text_GetDescriptor(handle, Four, OID *, Two, LOM_TextDesc *);
 *
 * Description:
 *  Get text-type attribute's descriptor
 *
 * Retuns:
 *  error code
 */
Four LOM_Text_GetDescriptor(
	LOM_Handle *handle, 
	Four ocnOrScanId,	/* IN: open scan number */
	Boolean useScanFlag,
	OID *oid,	/* IN: oid */
	Two colNo,	/* IN: column number */
	LOM_TextDesc	*textDesc	/* OUT: text descriptor */
)
{
	ColListStruct clist[1];	/* column struct */
	Four e;
	Four scanId;
	Four orn;

	/* parameter checking */
	if(ocnOrScanId < 0) LOM_ERROR(handle, eBADPARAMETER_LOM);
	if(oid == NULL) LOM_ERROR(handle, eBADPARAMETER_LOM);
	if(colNo < 0) LOM_ERROR(handle, eBADPARAMETER_LOM);
	if(textDesc == NULL) LOM_ERROR(handle, eBADPARAMETER_LOM);

	if(useScanFlag) {
		scanId = LOM_SCANTABLE(handle)[ocnOrScanId].lrdsScanId;

		if (!LRDS_VALID_SCANID(LOM_GET_LRDS_HANDLE(handle), scanId)) LOM_ERROR(handle, eBADPARAMETER_LOM);

		orn = LRDS_SCANTABLE(LOM_GET_LRDS_HANDLE(handle))[scanId].orn;
	}
	else orn = ocnOrScanId;

	/* store text descriptor information */
	clist[0].colNo = GET_SYSTEMLEVEL_COLNO(colNo); /* logical id column */
	clist[0].start = ALL_VALUE;
	clist[0].length = sizeof(LOM_TextDesc);
	clist[0].dataLength = sizeof(LOM_TextDesc);
	clist[0].data.ptr = textDesc;

	if(useScanFlag)
		e = LRDS_FetchTuple(LOM_GET_LRDS_HANDLE(handle), scanId, useScanFlag, (TupleID *)oid, 1, &clist[0]);
	else
		e = LRDS_FetchTuple(LOM_GET_LRDS_HANDLE(handle), ocnOrScanId, useScanFlag, (TupleID *)oid, 1, &clist[0]);
	if( e < eNOERROR) LOM_ERROR(handle, e);

	/* null column */
	if(clist[0].nullFlag)
		MAKE_NULLTEXTDESC(*textDesc);

	return eNOERROR;
}


/*
 * Function: Four LOM_Text_UpdateDescriptor(handle, Four, OID *, Two, LOM_TextDesc *);
 *
 * Description:
 *  Update text-type attribute's descriptor
 *
 * Retuns:
 *  error code
 */
Four LOM_Text_UpdateDescriptor(
	LOM_Handle *handle, 
	Four ocnOrScanId,	/* IN: open scan number */
	Boolean useScanFlag, /* IN: flag */
	OID *oid,	/* IN: oid */
	Two colNo,	/* IN: column number */
	LOM_TextDesc	*textDesc	/* OUT: text descriptor */
)
{
	ColListStruct clist[1];	/* column struct */
	Four e;
	Four scanId;
	Four orn;

	/* parameter checking */
	if(ocnOrScanId < 0) LOM_ERROR(handle, eBADPARAMETER_LOM);
	if(oid == NULL) LOM_ERROR(handle, eBADPARAMETER_LOM);
	if(colNo < 0) LOM_ERROR(handle, eBADPARAMETER_LOM);
	if(textDesc == NULL) LOM_ERROR(handle, eBADPARAMETER_LOM);

	if(useScanFlag) {
		scanId = LOM_SCANTABLE(handle)[ocnOrScanId].lrdsScanId;

		if (!LRDS_VALID_SCANID(LOM_GET_LRDS_HANDLE(handle), scanId)) LOM_ERROR(handle, eBADPARAMETER_LOM);

		orn = LRDS_SCANTABLE(LOM_GET_LRDS_HANDLE(handle))[scanId].orn;
	}
	else orn = ocnOrScanId;

	/* store text descriptor information */
	clist[0].colNo = GET_SYSTEMLEVEL_COLNO(colNo); /* logical id column */
	clist[0].start = ALL_VALUE;
	clist[0].length = sizeof(LOM_TextDesc);
	clist[0].dataLength = sizeof(LOM_TextDesc);
	clist[0].data.ptr = textDesc;
	clist[0].nullFlag = SM_FALSE;

	if(useScanFlag)
		e = LRDS_UpdateTuple(LOM_GET_LRDS_HANDLE(handle), scanId, useScanFlag, (TupleID *)oid, 1, &clist[0]);
	else
		e = LRDS_UpdateTuple(LOM_GET_LRDS_HANDLE(handle), ocnOrScanId, useScanFlag, (TupleID *)oid, 1, &clist[0]);
	if( e < eNOERROR) LOM_ERROR(handle, e);

	return eNOERROR;
}

/*
 * Function: Four LOM_Text_DestroyContent(handle, Four, OID *, Two, LOM_TextDesc *);
 *
 * Description:
 *  Destroy text-type attribute value(content)
 *
 * Retuns:
 *  error code
 */
Four LOM_Text_DestroyContent(
	LOM_Handle *handle, 
	Four ocnOrScanId,		/* open scan number */
	Boolean useScanFlag,	/* flag */
	OID *oid,		/* oid */
	Two colNo,		/* column number */
	LOM_TextDesc *textDesc 	/* INOUT: text descriptor */
)	
{
	Four contentOrn;
	Four e;
	ColListStruct clist[2];
	Four logicalDocId;
	Four scanId;
	Four orn;

	/* check parameters */
	if(ocnOrScanId < 0 || oid == NULL || colNo < 0 || textDesc == NULL) LOM_ERROR(handle, eBADPARAMETER_LOM);

	/* no content for the given oid */
	if(DOES_NOCONTENT_EXIST_TEXTDESC(*textDesc)) LOM_ERROR(handle, eBADPARAMETER_LOM);

	if(useScanFlag) {
		scanId = LOM_SCANTABLE(handle)[ocnOrScanId].lrdsScanId;
		orn = LRDS_SCANTABLE(LOM_GET_LRDS_HANDLE(handle))[scanId].orn;
	}
	else
		orn = ocnOrScanId;

	contentOrn = LOM_USEROPENCLASSTABLE(handle)[orn].ornForContentTable;

	if(ISINDEXED_TEXTDESC(*textDesc)) {
		/* get logical id */
		logicalDocId = lom_Text_GetLogicalId(handle, ocnOrScanId, useScanFlag, oid);
		if(logicalDocId < 0) LOM_ERROR(handle, logicalDocId);

		/* delete index entry from inveted index table and posting file */
		e = lom_Text_RemoveInvertedIndexEntry(handle, ocnOrScanId, useScanFlag, logicalDocId, (Two)GET_SYSTEMLEVEL_COLNO(colNo));
		if(e < eNOERROR) LOM_ERROR(handle, e);

		e = lom_Text_RemoveDocIdIndexEntry(handle, ocnOrScanId, useScanFlag, logicalDocId, (Two)GET_SYSTEMLEVEL_COLNO(colNo));
		if(e < eNOERROR) LOM_ERROR(handle, e);
	}

	/* delete content object */
	e = LRDS_DestroyTuple(LOM_GET_LRDS_HANDLE(handle), contentOrn, SM_FALSE, &(textDesc->contentTid));
	if(e < eNOERROR) LOM_ERROR(handle, e);

	/* set tuple id to be null */
	RESET_CONTENTSIZE_TEXTDESC(*textDesc);
	RESET_ISINDEXED_TEXTDESC(*textDesc);

	/* make collist struct */
	clist[0].colNo = GET_SYSTEMLEVEL_COLNO(colNo);
	clist[0].start = ALL_VALUE;
	clist[0].length = sizeof(LOM_TextDesc);
	clist[0].dataLength = sizeof(LOM_TextDesc);
	clist[0].data.ptr = textDesc;
	clist[0].nullFlag = SM_FALSE;

	/* update text descriptor */
	if(useScanFlag)
		e = LRDS_UpdateTuple(LOM_GET_LRDS_HANDLE(handle), scanId, useScanFlag, (TupleID *)oid, 1, &clist[0]);
	else
		e = LRDS_UpdateTuple(LOM_GET_LRDS_HANDLE(handle), ocnOrScanId, useScanFlag, (TupleID *)oid, 1, &clist[0]);
	if( e < eNOERROR) LOM_ERROR(handle, e);

	return eNOERROR;
}

Four LOM_Text_FetchContent(
	LOM_Handle *handle, 
	Four ocnOrScanId,		/* open scan number */
	Boolean useScanFlag,
	OID *oid,		/* oid */
	Two colNo,		/* column number */
	TextColStruct *text,	/* INOUT; text */
	LOM_TextDesc *textDesc 	/* INOUT: text descriptor */
)	
{
	/* check parameters */
	if(ocnOrScanId < 0 || oid == NULL || colNo < 0 || textDesc == NULL || text == NULL) LOM_ERROR(handle, eBADPARAMETER_LOM);

	/* no content for the given oid */
	if(DOES_NOCONTENT_EXIST_TEXTDESC(*textDesc)) {
		text->retLength = -1;
		return eNOERROR;
	}

	/* make collist struct */
	text->retLength = lom_Text_FetchContentData(
		handle,  
		ocnOrScanId, 
		useScanFlag,
		&textDesc->contentTid, 
		text->start, 
		text->length, 
		text->dataLength,
		text->data
		);
	if(text->retLength < eNOERROR) LOM_ERROR(handle, text->retLength);

	return eNOERROR;
}

Four LOM_Text_UpdateContent(
	LOM_Handle *handle, 
	Four ocnOrScanId,		/* open scan number */
	Boolean useScanFlag,
	OID *oid,
	Two colNo,
	TextColStruct *text,	/* IN: text column struct */
	LOM_TextDesc *textDesc 	/* INOUT: text descriptor */
)	
{

	char *tempName;		/* temporary name */
	char *tempDir;		/* temporary directory */
	ColListStruct clist[1];	/* column struct */
	Four e, e2;
	Four logicalDocId;
	Four acutallyInsertedDataLength;
	Four orn;	
	char *tempDocIdName;	
	Boolean isLogicalIdOrder;
	Four scanId;
	lom_Text_ConfigForInvertedIndexBuild config;
	lom_FptrToKeywordExtractor keywordExtractorFPtr;
	lom_FptrToGetNextPostingInfo getNextPostingInfoFPtr;
	lom_FptrToFinalizeKeywordExtraction finalizeKeywordExtractionFPtr;
	lom_FptrToFilter filterFPtr;

	/* set inverted index building configuration */
	config.isBuildingDocIdIndex						= SM_FALSE;
	config.isBuildingExternalReverseKeywordFile		= SM_FALSE;
	config.isSortingPostingFile						= SM_FALSE;
	config.isUsingBulkLoading						= SM_FALSE;
	config.isUsingKeywordIndexBulkLoading			= SM_FALSE;
	config.isUsingReverseKeywordIndexBulkLoading	= SM_FALSE;

	/* check parameter */
	if(ocnOrScanId < 0) LOM_ERROR(handle, eBADPARAMETER_LOM);
	if(oid == NULL) LOM_ERROR(handle, eBADPARAMETER_LOM);
	if(colNo < 0) LOM_ERROR(handle, eBADPARAMETER_LOM);
	if(textDesc == 0) LOM_ERROR(handle, eBADPARAMETER_LOM);
	if(text == 0) LOM_ERROR(handle, eBADPARAMETER_LOM);
	if(DOES_NOCONTENT_EXIST_TEXTDESC(*textDesc)) LOM_ERROR(handle, eBADPARAMETER_LOM);
	if(text->start < 0) LOM_ERROR(handle, eBADPARAMETER_LOM);

	/* if index has already built on this content,
	   we regard it as an error 
	 */
	if(ISINDEXED_TEXTDESC(*textDesc)) 
	 	LOM_ERROR(handle, eCANNOTUPDATECONTENTWHENINDEXISBUILT_LOM);

	if(useScanFlag) {
		scanId = LOM_SCANTABLE(handle)[ocnOrScanId].lrdsScanId;

		if (!LRDS_VALID_SCANID(LOM_GET_LRDS_HANDLE(handle), scanId)) LOM_ERROR(handle, eBADPARAMETER_LOM);

		orn = LRDS_SCANTABLE(LOM_GET_LRDS_HANDLE(handle))[scanId].orn;
	}
	else {
		orn = ocnOrScanId;
	}

	/* udpate content data */
	e = lom_Text_UpdateContentData(
		handle, 
		ocnOrScanId,
		useScanFlag,
		&(textDesc->contentTid),
		text->start,
		text->length,
		text->dataLength,
		text->data
	);
	if( e < eNOERROR) LOM_ERROR(handle, e);

	if(text->indexMode == LOM_IMMEDIATE_MODE) {

		tempDir = getenv(TEXT_TEMP_PATH);
		if(tempDir == NULL) LOM_ERROR(handle, eCONFIGURATION_LOM); 

		tempName = tempnam(tempDir, "TEXT");
		if(tempName == NULL) LOM_ERROR(handle, eOUTOFMEMORY_LOM);

		tempDocIdName = tempnam(tempDir, "TEXT_DOC");
		if(tempDocIdName == NULL) LOM_ERROR(handle, eOUTOFMEMORY_LOM);

		/* get logical id */
		logicalDocId = lom_Text_GetLogicalId(handle, ocnOrScanId, useScanFlag, oid);
		if(logicalDocId < 0) LOM_ERROR(handle, logicalDocId);

		/* extract keywords from content object and posting information
		   put them into temporoary posting file */
		e = lom_Text_GetKeywordExtractorFPtr(handle, orn, GET_SYSTEMLEVEL_COLNO(colNo), &keywordExtractorFPtr);
		if(e < eNOERROR) LOM_ERROR(handle, e);
		e = lom_Text_GetGetNextPostingInfoFPtr(handle, orn, GET_SYSTEMLEVEL_COLNO(colNo), &getNextPostingInfoFPtr);
		if(e < eNOERROR) LOM_ERROR(handle, e);
		e = lom_Text_GetFinalizeKeywordExtractionFPtr(handle, orn, GET_SYSTEMLEVEL_COLNO(colNo), &finalizeKeywordExtractionFPtr);
		if(e < eNOERROR) LOM_ERROR(handle, e);
		e = lom_Text_GetFilterFPtr(handle, orn, GET_SYSTEMLEVEL_COLNO(colNo), &filterFPtr);
		if(e < eNOERROR) LOM_ERROR(handle, e);

		e = lom_Text_ExtractKeywordFromContentObjectIntoTempFile(
				handle, 
				keywordExtractorFPtr,
				getNextPostingInfoFPtr,
				finalizeKeywordExtractionFPtr,
				filterFPtr,
				ocnOrScanId,
				useScanFlag,
				&(textDesc->contentTid),
				oid,
				logicalDocId,
				(Two)GET_SYSTEMLEVEL_COLNO(colNo),
				tempName);
		if(e < eNOERROR) {
			e2 = lom_Text_DestroyTempFile(handle, tempName);
			if(e2 < eNOERROR) {
				free(tempName);
				LOM_ERROR(handle, e2);
			}
			free(tempName);
			LOM_ERROR(handle, e);

			e2 = lom_Text_DestroyTempFile(handle, tempDocIdName);
			if(e2 < eNOERROR) {
				free(tempDocIdName);
				LOM_ERROR(handle, e2);
			}
			free(tempDocIdName);
			LOM_ERROR(handle, e);
		}

		/* I ommited the following case , thus when updating text file,
		   the text has been indexed,
		*/
		if(HASBEENINDEXED_TEXTDESC(*textDesc)) isLogicalIdOrder = SM_TRUE;
		else isLogicalIdOrder = SM_FALSE;

		e = lom_Text_AddInvertedIndexEntryFromTempFile(
			handle, 
			lom_GetVolIdFromOcn(handle, orn),
			ocnOrScanId, 
			useScanFlag,
			(Two)GET_SYSTEMLEVEL_COLNO(colNo), 
			tempName, 
			tempDocIdName, 
			isLogicalIdOrder,
			&config);
		if(e < eNOERROR) {
			e2 = lom_Text_DestroyTempFile(handle, tempName);
			if(e2 < eNOERROR) {
				free(tempName);
				LOM_ERROR(handle, e2);
			}
			free(tempName);

			e2 = lom_Text_DestroyTempFile(handle, tempDocIdName);
			if(e2 < eNOERROR) {
				free(tempDocIdName);
				LOM_ERROR(handle, e2);
			}
			free(tempDocIdName);
			LOM_ERROR(handle, e);
		}

		/* add docId-index entry */
		e = lom_Text_AddDocIdIndexEntryFromTempFile(
			handle, 
			lom_GetVolIdFromOcn(handle, orn),
			ocnOrScanId, 
			useScanFlag,
			(Two)GET_SYSTEMLEVEL_COLNO(colNo), 
			tempDocIdName);
		if(e2 < eNOERROR) {
			e2 = lom_Text_DestroyTempFile(handle, tempName);
			if(e < eNOERROR) {
				free(tempName);
				LOM_ERROR(handle, e2);
			}
			free(tempName);
			e2 = lom_Text_DestroyTempFile(handle, tempDocIdName);
			if(e2 < eNOERROR) {
				free(tempDocIdName);
				LOM_ERROR(handle, e2);
			}
			free(tempDocIdName);
			LOM_ERROR(handle, e);
		}

		/* destroy temporary posting file */
		e = lom_Text_DestroyTempFile(handle, tempName);
		if(e < eNOERROR) {
			free(tempName);
			LOM_ERROR(handle, e);
		}

		free(tempName);

		/* destroy temporary doc-id file */
		e = lom_Text_DestroyTempFile(handle, tempDocIdName );
		if(e < eNOERROR) {
			free(tempDocIdName);
			LOM_ERROR(handle, e);
		}
		free(tempDocIdName);

		/* set text descriptor */
		SET_ISINDEXED_TEXTDESC(*textDesc);
		SET_HASBEENINDEXED_TEXTDESC(*textDesc);
	}

	acutallyInsertedDataLength = text->dataLength - text->length;
	textDesc->size += acutallyInsertedDataLength;

	/* store text descriptor information */
	clist[0].colNo = GET_SYSTEMLEVEL_COLNO(colNo); /* logical id column */
	clist[0].start = ALL_VALUE;
	clist[0].length = sizeof(LOM_TextDesc);
	clist[0].dataLength = sizeof(LOM_TextDesc);
	clist[0].data.ptr = textDesc;
	clist[0].nullFlag = SM_FALSE;

	/* update text descriptor */
	if(useScanFlag)
		e = LRDS_UpdateTuple(LOM_GET_LRDS_HANDLE(handle), scanId, useScanFlag, (TupleID *)oid, 1, &clist[0]);
	else
		e = LRDS_UpdateTuple(LOM_GET_LRDS_HANDLE(handle), ocnOrScanId, useScanFlag, (TupleID *)oid, 1, &clist[0]);
	if( e < eNOERROR) LOM_ERROR(handle, e);

	return eNOERROR;
}

/***************************************/
/* Scan-related Interfaces             */
/***************************************/

Four LOM_Text_OpenIndexScan(
	LOM_Handle*		handle,				/* IN: lom system handle */
	Four			ocn,				/* IN: open class number */
	LOM_IndexID*	indexId,			/* IN: index id */
	Four			keywordKind,		/* IN: kind of keyword */
	BoundCond*		keywordStartBound,	/* IN: start bound */
	BoundCond*		keywordStopBound,	/* IN: stop bound */
	LockParameter*	lockup				/* IN: lock parameter */
)
{

	Four idxForIndexInfoTbl;	/* index for Catalog IndexInfo Table */
	catalog_SysIndexesOverlay *sysIndexes;
	Two colNo;
	Four i, e, e2;
	Four ornForInvertedIndexTable;
	Four scanId;
	lrds_RelTableEntry *relTableEntry; /* pointer to an entry of relation table */
	Four v;

	/* parameter check */
	if(ocn < 0) LOM_ERROR(handle, eBADPARAMETER_LOM);
	if(indexId == NULL) LOM_ERROR(handle, eBADPARAMETER_LOM);
	if(keywordStartBound == NULL) LOM_ERROR(handle, eBADPARAMETER_LOM);
	if(keywordStopBound == NULL) LOM_ERROR(handle, eBADPARAMETER_LOM);
	if(lockup == NULL) LOM_ERROR(handle, eBADPARAMETER_LOM);
	if(!(keywordKind == KEYWORD || keywordKind == REVERSEKEYWORD)) LOM_ERROR(handle, eBADPARAMETER_LOM);

	/* Find the empty scan table entry. */
	scanId = e = lom_ScanTableAllocEntry(handle);
	if (e < 0) LOM_ERROR(handle, e);

	/* set open class num */
	LOM_SCANTABLE(handle)[scanId].ocn = ocn;
	
	/* copy lock mode */
	LOM_SCANTABLE(handle)[scanId].lockup.duration = lockup->duration;
	LOM_SCANTABLE(handle)[scanId].lockup.mode = lockup->mode;

	/* open lrds-level scan */
	if(lockup->mode == L_S || lockup->mode == L_IS)
		LOM_SCANTABLE(handle)[scanId].lrdsScanId = LOM_USEROPENCLASSTABLE(handle)[ocn].lrdsScanIdForTextScan;
	else
	{
		LOM_SCANTABLE(handle)[scanId].lrdsScanId = e = LRDS_OpenSeqScan(LOM_GET_LRDS_HANDLE(handle), ocn, FORWARD, 0, NULL, lockup);
		if(e < eNOERROR) {
			e2 = lom_ScanTableFreeEntry(handle, scanId);
			if (e2 < eNOERROR) LOM_ERROR(handle, e2);
			LOM_ERROR(handle, e);
		}
	}

	/* open content-table scan */
	relTableEntry = LRDS_GET_RELTABLE_ENTRY(LOM_GET_LRDS_HANDLE(handle), ocn);

	/* on which column this index is built */
	/* get index information by using index id */
	e = Catalog_GetIndexInfoByIndexID(handle, 
		indexId->index.logical_iid.volNo,
		LOM_USEROPENCLASSTABLE(handle)[ocn].classID,
		indexId,
		&idxForIndexInfoTbl);
	if( e < eNOERROR) {
		e2 = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), LOM_SCANTABLE(handle)[scanId].lrdsScanId);
		if(e2 < eNOERROR) LOM_ERROR(handle, e2);

		e2 = lom_ScanTableFreeEntry(handle, scanId);
		if(e2 < eNOERROR) LOM_ERROR(handle, e2);

		LOM_ERROR(handle, e);
	}

	v = Catalog_GetVolIndex(handle, indexId->index.logical_iid.volNo);
    if(v < eNOERROR) {
		e2 = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), LOM_SCANTABLE(handle)[scanId].lrdsScanId);
		if(e2 < eNOERROR) LOM_ERROR(handle, e2);

		e2 = lom_ScanTableFreeEntry(handle, scanId);
		if(e2 < eNOERROR) LOM_ERROR(handle, e2);

		LOM_ERROR(handle, v);
    }

	sysIndexes = &CATALOG_GET_INDEXINFOTBL(handle, v)[idxForIndexInfoTbl];
	colNo = sysIndexes->colNo[0];

	/* get open relation number for inverted index */
	for(i = 0; i< LOM_USEROPENCLASSTABLE(handle)[ocn].numOfTextAttrs; i++) {
		if(LOM_USEROPENCLASSTABLE(handle)[ocn].textColNo[i] == colNo) {
			ornForInvertedIndexTable = LOM_USEROPENCLASSTABLE(handle)[ocn].ornForInvertedIndexTable[i];
			break;
		}
		else if (LOM_USEROPENCLASSTABLE(handle)[ocn].textColNo[i] < 0) {
			e2 = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), LOM_SCANTABLE(handle)[scanId].lrdsScanId);
			if(e2 < eNOERROR) LOM_ERROR(handle, e2);

			e2 = lom_ScanTableFreeEntry(handle, scanId);
			if(e2 < eNOERROR) LOM_ERROR(handle, e2);

			LOM_ERROR(handle, eINTERNAL_LOM);
		}
	}
	
	/* open content table scan */
	if(lockup->mode == L_S || lockup->mode == L_IS)
		LOM_SCANTABLE(handle)[scanId].contentTableScanId = LOM_USEROPENCLASSTABLE(handle)[ocn].contentTableScanIdForTextScan;
	else
	{
		LOM_SCANTABLE(handle)[scanId].contentTableScanId = e = LRDS_OpenSeqScan(LOM_GET_LRDS_HANDLE(handle), LOM_USEROPENCLASSTABLE(handle)[ocn].ornForContentTable, FORWARD, 0, NULL, lockup);
		if( e < eNOERROR) {
			e2 = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), LOM_SCANTABLE(handle)[scanId].lrdsScanId);
			if(e2 < eNOERROR) LOM_ERROR(handle, e2);

			e2 = lom_ScanTableFreeEntry(handle, scanId);
			if(e2 < eNOERROR) LOM_ERROR(handle, e2);

			LOM_ERROR(handle, e);
		}
	}

	LOM_SCANTABLE(handle)[scanId].textScanInfo.keywordBasedScan.invertedOrn = ornForInvertedIndexTable;

	/* get index id on inverted index */
	if(keywordKind == KEYWORD) {
		LOM_SCANTABLE(handle)[scanId].textScanInfo.keywordBasedScan.invertedScanId = e = 
		 	LRDS_OpenIndexScan(LOM_GET_LRDS_HANDLE(handle), 
				ornForInvertedIndexTable, 
				&(sysIndexes->kdesc.invertedIndex.keywordIndex),
				keywordStartBound,
				keywordStopBound,
				0,
				NULL,
				lockup);
		if( e < eNOERROR) {
			e2 = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), LOM_SCANTABLE(handle)[scanId].lrdsScanId);
			if(e2 < eNOERROR) LOM_ERROR(handle, e2);

			e2 = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), LOM_SCANTABLE(handle)[scanId].contentTableScanId);
			if(e2 < eNOERROR) LOM_ERROR(handle, e2);

			e2 = lom_ScanTableFreeEntry(handle, scanId);
			if(e2 < eNOERROR) LOM_ERROR(handle, e2);

			LOM_ERROR(handle, e);
		}
	}
	else if(keywordKind == REVERSEKEYWORD) {	/* REVERSEKEYWORD */
		LOM_SCANTABLE(handle)[scanId].textScanInfo.keywordBasedScan.invertedScanId = e = 
			LRDS_OpenIndexScan(LOM_GET_LRDS_HANDLE(handle), 
				ornForInvertedIndexTable, 
				&(sysIndexes->kdesc.invertedIndex.reverseKeywordIndex),
				keywordStartBound,
				keywordStopBound,
				0,
				NULL,
				lockup);
		if(e < eNOERROR) {
			e2 = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), LOM_SCANTABLE(handle)[scanId].lrdsScanId);
			if(e2 < eNOERROR) LOM_ERROR(handle, e2);

			e2 = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), LOM_SCANTABLE(handle)[scanId].contentTableScanId);
			if(e2 < eNOERROR) LOM_ERROR(handle, e2);

			e2 = lom_ScanTableFreeEntry(handle, scanId);
			if(e2 < eNOERROR) LOM_ERROR(handle, e2);

			LOM_ERROR(handle, e);
		}
	}
	else LOM_ERROR(handle, eBADPARAMETER_LOM);

	/* set scan type */
	LOM_SCANTABLE(handle)[scanId].scanType = LOM_INVERTEDFILE_KEYWORDBASED_SCAN;

	/* set mode */
	LOM_SCANTABLE(handle)[scanId].textScanInfo.keywordBasedScan.onlyFetchMode = SM_FALSE;

	/* the number of total postings for the given keyword */
	LOM_SCANTABLE(handle)[scanId].textScanInfo.keywordBasedScan.nPostings = -1;

	/* the number of remained postings to read for the given keyword */
	LOM_SCANTABLE(handle)[scanId].textScanInfo.keywordBasedScan.nRemainedPostings = 0;

	/* current posting offset */
	LOM_SCANTABLE(handle)[scanId].textScanInfo.keywordBasedScan.currentPostingOffset = 0;

	LOM_SCANTABLE(handle)[scanId].textScanInfo.keywordBasedScan.colNo = colNo;

	LOM_SCANTABLE(handle)[scanId].textScanInfo.keywordBasedScan.cursor = NULL;

#ifdef SUBINDEX
	LOM_SCANTABLE(handle)[scanId].textScanInfo.keywordBasedScan.orderedSetScanId = NIL;
#endif

	e = lom_ConstructEmbeddedAttrTranslationInfo(handle, scanId);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	return scanId;

}

Four LOM_Text_OpenIndexScan_GivenInvertedEntryTupleID(
	LOM_Handle*		handle,						/* IN: lom system handle */
	Four			ocn,						/* IN: open class number */
	Two				colNo,						/* IN: colNo */
	TupleID*		invertedTableEntryTid,	/* IN: inverted index entry tupleID for given keyword */
	LockParameter*	lockup						/* IN: lock parameter */
)
{
	Four	e, e2;
	Four	scanId;
	Four	ornForInvertedIndexTable;
	Four	i;

	/* parameter check */
	if(ocn < 0) LOM_ERROR(handle, eBADPARAMETER_LOM);
	if(lockup == NULL) LOM_ERROR(handle, eBADPARAMETER_LOM);

	/* Find the empty scan table entry. */
	scanId = e = lom_ScanTableAllocEntry(handle);
	if (e < 0) LOM_ERROR(handle, e);

	/* set open class num */
	LOM_SCANTABLE(handle)[scanId].ocn = ocn;
	
	/* copy lock mode */
	LOM_SCANTABLE(handle)[scanId].lockup.duration = lockup->duration;
	LOM_SCANTABLE(handle)[scanId].lockup.mode = lockup->mode;

	/* get open relation number for inverted index */
	for(i = 0; i< LOM_USEROPENCLASSTABLE(handle)[ocn].numOfTextAttrs; i++) 
	{
		if(LOM_USEROPENCLASSTABLE(handle)[ocn].textColNo[i] == GET_SYSTEMLEVEL_COLNO(colNo)) 
		{
			ornForInvertedIndexTable = LOM_USEROPENCLASSTABLE(handle)[ocn].ornForInvertedIndexTable[i];
			break;
		}
		else if (LOM_USEROPENCLASSTABLE(handle)[ocn].textColNo[i] < 0) 
		{
			e2 = lom_ScanTableFreeEntry(handle, scanId);
			if(e2 < eNOERROR) LOM_ERROR(handle, e2);
			LOM_ERROR(handle, eINTERNAL_LOM);
		}
	}

	/* open lrds-level scan */
	if(lockup->mode == L_S || lockup->mode == L_IS)
		LOM_SCANTABLE(handle)[scanId].lrdsScanId = LOM_USEROPENCLASSTABLE(handle)[ocn].lrdsScanIdForTextScan;
	else
	{
		LOM_SCANTABLE(handle)[scanId].lrdsScanId = e = LRDS_OpenSeqScan(LOM_GET_LRDS_HANDLE(handle), ocn, FORWARD, 0, NULL, lockup);
		if(e < eNOERROR) {
			e2 = lom_ScanTableFreeEntry(handle, scanId);
			if (e2 < eNOERROR) LOM_ERROR(handle, e2);
			LOM_ERROR(handle, e);
		}
	}

	/* open content table scan */
	if(lockup->mode == L_S || lockup->mode == L_IS)
		LOM_SCANTABLE(handle)[scanId].contentTableScanId = LOM_USEROPENCLASSTABLE(handle)[ocn].contentTableScanIdForTextScan;
	else
	{
		LOM_SCANTABLE(handle)[scanId].contentTableScanId = e = LRDS_OpenSeqScan(LOM_GET_LRDS_HANDLE(handle), LOM_USEROPENCLASSTABLE(handle)[ocn].ornForContentTable, FORWARD, 0, NULL, lockup);
		if( e < eNOERROR) {
			e2 = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), LOM_SCANTABLE(handle)[scanId].lrdsScanId);
			if(e2 < eNOERROR) LOM_ERROR(handle, e2);

			e2 = lom_ScanTableFreeEntry(handle, scanId);
			if(e2 < eNOERROR) LOM_ERROR(handle, e2);

			LOM_ERROR(handle, e);
		}
	}

	/* get scanid, orn for inverted index */
	LOM_SCANTABLE(handle)[scanId].textScanInfo.keywordBasedScan.invertedOrn    = ornForInvertedIndexTable;
	LOM_SCANTABLE(handle)[scanId].textScanInfo.keywordBasedScan.invertedScanId = NIL;
	LOM_SCANTABLE(handle)[scanId].textScanInfo.keywordBasedScan.invertedTableEntryTid = *invertedTableEntryTid;
	LOM_SCANTABLE(handle)[scanId].textScanInfo.keywordBasedScan.invertedTableEntryTidCount = 1;

	/* set scan type */
	LOM_SCANTABLE(handle)[scanId].scanType = LOM_INVERTEDFILE_KEYWORDBASED_SCAN;

	/* set mode */
	LOM_SCANTABLE(handle)[scanId].textScanInfo.keywordBasedScan.onlyFetchMode = SM_FALSE;

	/* the number of total postings for the given keyword */
	LOM_SCANTABLE(handle)[scanId].textScanInfo.keywordBasedScan.nPostings = -1;

	/* the number of remained postings to read for the given keyword */
	LOM_SCANTABLE(handle)[scanId].textScanInfo.keywordBasedScan.nRemainedPostings = 0;

	/* current posting offset */
	LOM_SCANTABLE(handle)[scanId].textScanInfo.keywordBasedScan.currentPostingOffset = 0;

	LOM_SCANTABLE(handle)[scanId].textScanInfo.keywordBasedScan.colNo = colNo;

	LOM_SCANTABLE(handle)[scanId].textScanInfo.keywordBasedScan.cursor = NULL;

#ifdef SUBINDEX
	LOM_SCANTABLE(handle)[scanId].textScanInfo.keywordBasedScan.orderedSetScanId = NIL;
#endif

	e = lom_ConstructEmbeddedAttrTranslationInfo(handle, scanId);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	return scanId;

}

Four LOM_Text_Scan_Open(
	LOM_Handle *handle, 
	Four ocn,	/* IN: open class number */
	OID *oid,	/* IN: oid */
	Two colNo,	/* IN: column number */
	Four keywordKind,	/* IN: kind of keyword */
	BoundCond *keywordStartBound, /* IN: start bound */
	BoundCond *keywordStopBound,	/* IN: stop bound */
	LockParameter *lockup		/* IN: lock parameter */
)
{

	Four idxForIndexInfoTbl;	/* index for Catalog IndexInfo Table */
	catalog_SysIndexesOverlay *sysIndexes;
	Four i, e, e2;
	Four ornForInvertedIndexTable;
	Four scanId;
	lrds_RelTableEntry *relTableEntry; /* pointer to an entry of relation tale */
	Four v;
	Four numOfReturnedIndex;       /* number Of Returned Index */

	/* parameter check */
	if(ocn < 0) LOM_ERROR(handle, eBADPARAMETER_LOM);
	if(oid == NULL) LOM_ERROR(handle, eBADPARAMETER_LOM);
	if(colNo < 0) LOM_ERROR(handle, eBADPARAMETER_LOM);
	if(keywordStartBound == NULL) LOM_ERROR(handle, eBADPARAMETER_LOM);
	if(keywordStopBound == NULL) LOM_ERROR(handle, eBADPARAMETER_LOM);
	if(lockup == NULL) LOM_ERROR(handle, eBADPARAMETER_LOM);
	if(!(keywordKind == KEYWORD || keywordKind == REVERSEKEYWORD)) LOM_ERROR(handle, eBADPARAMETER_LOM);

	/* Find the empty scan table entry. */
	scanId = e = lom_ScanTableAllocEntry(handle);
	if (e < 0) LOM_ERROR(handle, e);

	/* set open class num */
	LOM_SCANTABLE(handle)[scanId].ocn = ocn;

	/* copy lock mode */
	LOM_SCANTABLE(handle)[scanId].lockup.duration = lockup->duration;
	LOM_SCANTABLE(handle)[scanId].lockup.mode = lockup->mode;

	/* open lrds-level scan */
	if(lockup->mode == L_S || lockup->mode == L_IS)
		LOM_SCANTABLE(handle)[scanId].lrdsScanId = LOM_USEROPENCLASSTABLE(handle)[ocn].lrdsScanIdForTextScan;
	else
	{
		LOM_SCANTABLE(handle)[scanId].lrdsScanId = e = LRDS_OpenSeqScan(LOM_GET_LRDS_HANDLE(handle), ocn, FORWARD, 0, NULL, lockup);
		if(e < eNOERROR) {
			e2 = lom_ScanTableFreeEntry(handle, scanId);
			if (e2 < eNOERROR) LOM_ERROR(handle, e2);
			LOM_ERROR(handle, e);
		}
	}

	/* open content-table scan */
	relTableEntry = LRDS_GET_RELTABLE_ENTRY(LOM_GET_LRDS_HANDLE(handle), ocn);

	/* get open relation number for inverted index */
	for(i = 0; i< LOM_USEROPENCLASSTABLE(handle)[ocn].numOfTextAttrs; i++) {
		if(LOM_USEROPENCLASSTABLE(handle)[ocn].textColNo[i] == GET_SYSTEMLEVEL_COLNO(colNo)) {
			ornForInvertedIndexTable = LOM_USEROPENCLASSTABLE(handle)[ocn].ornForInvertedIndexTable[i];
			break;
		}
		else if (LOM_USEROPENCLASSTABLE(handle)[ocn].textColNo[i] < 0) {
			e2 = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), LOM_SCANTABLE(handle)[scanId].lrdsScanId);
			if(e2 < eNOERROR) LOM_ERROR(handle, e2);

			e2 = lom_ScanTableFreeEntry(handle, scanId);
			if(e2 < eNOERROR) LOM_ERROR(handle, e2);

			LOM_ERROR(handle, eINTERNAL_LOM);
		}
	}

	/* get index id on inverted index using colNo */
	numOfReturnedIndex = 1;
	e = Catalog_GetIndexInfoByAttrNum(handle, 
			relTableEntry->ri.fid.volNo,
			LOM_USEROPENCLASSTABLE(handle)[ocn].classID,
			colNo,
			&numOfReturnedIndex,
			&idxForIndexInfoTbl);
	if(e < eNOERROR) {
		e2 = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), LOM_SCANTABLE(handle)[scanId].lrdsScanId);
		if(e2 < eNOERROR) LOM_ERROR(handle, e2);

		e2 = lom_ScanTableFreeEntry(handle, scanId);
		if(e2 < eNOERROR) LOM_ERROR(handle, e2);

		LOM_ERROR(handle, e);
	}
	if(numOfReturnedIndex != 1) LOM_ERROR(handle, eINTERNAL_LOM);

	v = Catalog_GetVolIndex(handle, relTableEntry->ri.fid.volNo);
    if(v < eNOERROR) {
		e2 = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), LOM_SCANTABLE(handle)[scanId].lrdsScanId);
		if(e2 < eNOERROR) LOM_ERROR(handle, e2);

		e2 = lom_ScanTableFreeEntry(handle, scanId);
		if(e2 < eNOERROR) LOM_ERROR(handle, e2);

		LOM_ERROR(handle, v);
    }

	sysIndexes = &CATALOG_GET_INDEXINFOTBL(handle, v)[idxForIndexInfoTbl];

	LOM_SCANTABLE(handle)[scanId].textScanInfo.oidBasedScan.isContainingTupleID = 
		sysIndexes->kdesc.invertedIndex.postingInfo.isContainingTupleID;

	LOM_SCANTABLE(handle)[scanId].textScanInfo.oidBasedScan.invertedOrn = ornForInvertedIndexTable;
	if(keywordKind == KEYWORD) {
		 LOM_SCANTABLE(handle)[scanId].textScanInfo.oidBasedScan.invertedScanId = e = 
		 	LRDS_OpenIndexScan(LOM_GET_LRDS_HANDLE(handle), 
				ornForInvertedIndexTable, 
				&(sysIndexes->kdesc.invertedIndex.keywordIndex),
				keywordStartBound,
				keywordStopBound,
				0,
				NULL,
				lockup);
		if(e < eNOERROR) {
			e2 = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), LOM_SCANTABLE(handle)[scanId].lrdsScanId);
			if(e2 < eNOERROR) LOM_ERROR(handle, e2);

			e2 = lom_ScanTableFreeEntry(handle, scanId);
			if(e2 < eNOERROR) LOM_ERROR(handle, e2);

			LOM_ERROR(handle, e);
		}

	}
	else if(keywordKind == REVERSEKEYWORD) {	/* REVERSEKEYWORD */

		 LOM_SCANTABLE(handle)[scanId].textScanInfo.oidBasedScan.invertedScanId = e = 
		 	LRDS_OpenIndexScan(LOM_GET_LRDS_HANDLE(handle), 
				ornForInvertedIndexTable, 
				&(sysIndexes->kdesc.invertedIndex.reverseKeywordIndex),
				keywordStartBound,
				keywordStopBound,
				0,
				NULL,
				lockup);
		if(e < eNOERROR) {
			e2 = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), LOM_SCANTABLE(handle)[scanId].lrdsScanId);
			if(e2 < eNOERROR) LOM_ERROR(handle, e2);

			e2 = lom_ScanTableFreeEntry(handle, scanId);
			if(e2 < eNOERROR) LOM_ERROR(handle, e2);

			LOM_ERROR(handle, e);
		}

	}
	else LOM_ERROR(handle, eBADPARAMETER_LOM);

	/* set scan type */
	LOM_SCANTABLE(handle)[scanId].scanType = LOM_INVERTEDFILE_OIDBASED_SCAN;

	/* set mode */
	LOM_SCANTABLE(handle)[scanId].textScanInfo.oidBasedScan.onlyFetchMode = SM_FALSE;

	LOM_SCANTABLE(handle)[scanId].textScanInfo.oidBasedScan.colNo = GET_SYSTEMLEVEL_COLNO(colNo);

	LOM_SCANTABLE(handle)[scanId].textScanInfo.oidBasedScan.cursor = NULL;

	LOM_SCANTABLE(handle)[scanId].textScanInfo.oidBasedScan.logicalDocId = lom_Text_GetLogicalId(handle,  ocn, SM_FALSE, oid);
	if(LOM_SCANTABLE(handle)[scanId].textScanInfo.oidBasedScan.logicalDocId < eNOERROR) 
		LOM_ERROR(handle, LOM_SCANTABLE(handle)[scanId].textScanInfo.oidBasedScan.logicalDocId);

	e = lom_ConstructEmbeddedAttrTranslationInfo(handle, scanId);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	return scanId;
}

Four LOM_Text_Scan_Close(
	LOM_Handle *handle, 
	Four osn
)
{
	Four e;
	/* check parameters */
	if(LOM_SCANTABLE(handle)[osn].lrdsScanId < 0) LOM_ERROR(handle, eBADPARAMETER_LOM);
	if(LOM_SCANTABLE(handle)[osn].scanType != LOM_INVERTEDFILE_OIDBASED_SCAN) LOM_ERROR(handle, eBADPARAMETER_LOM);

	/* close posting scan */
	if(LOM_SCANTABLE(handle)[osn].textScanInfo.oidBasedScan.invertedScanId != NIL) {
		e = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), LOM_SCANTABLE(handle)[osn].textScanInfo.oidBasedScan.invertedScanId);
		if( e < eNOERROR) LOM_ERROR(handle, e);
	}

	if(LOM_SCANTABLE(handle)[osn].lockup.mode == L_IX || LOM_SCANTABLE(handle)[osn].lockup.mode == L_X)
	{
	 	/* close scan */
		if(LOM_SCANTABLE(handle)[osn].lrdsScanId != NIL)
		{
	 		e = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), LOM_SCANTABLE(handle)[osn].lrdsScanId);
	 		if(e < eNOERROR) LOM_ERROR(handle, e);
		}

	 	/* close content table scan */
	 	if(LOM_SCANTABLE(handle)[osn].contentTableScanId != NIL) 
		{
	 		e = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), LOM_SCANTABLE(handle)[osn].contentTableScanId);
	 		if(e < eNOERROR) LOM_ERROR(handle, e);
		}
	}

	/* reset scan entry */
	LOM_SCANTABLE(handle)[osn].scanType = NIL;
	LOM_SCANTABLE(handle)[osn].textScanInfo.oidBasedScan.invertedScanId = NIL;

	e = lom_ScanTableFreeEntry(handle, osn);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	return eNOERROR;
}

Four LOM_Text_Scan_NextPosting(
	LOM_Handle *handle, 
	Four textScan,		/* open scan number */
	Four bufferLength,
	char *postingBuffer,
	Four *requiredSize,
	PostingWeight *weight
)
{

	ColListStruct clist[10];
	Four e;
	Four e2;
	Four nPositions;
	Four nPostings;
	TupleID tid;
	lom_PostingBuffer lomPostingBuffer;
	Four currentOffset;
	Four lrdsScanId;
	char *ptrToPostingBuffer;
	Four offsetInBuffer;
	Boolean found;
	Boolean onlyFetchMode;
	Four postingLengthFieldSize = LOM_LONG_SIZE_VAR;
#ifdef SUBINDEX
	KeyValue kval;
#endif
	Four postingLength;

	/* check paramters */
	if(textScan < 0) LOM_ERROR(handle, eBADPARAMETER_LOM);
	if(bufferLength < 0) LOM_ERROR(handle, eBADPARAMETER_LOM);
	if(postingBuffer == NULL) LOM_ERROR(handle, eBADPARAMETER_LOM);
	if(requiredSize == NULL) LOM_ERROR(handle, eBADPARAMETER_LOM);

	/* local copy */
	onlyFetchMode = LOM_SCANTABLE(handle)[textScan].textScanInfo.oidBasedScan.onlyFetchMode;

	/* initialize posting buffer */
	e = LOM_AllocPostingBuffer(handle, &lomPostingBuffer, LOM_DEFAULTPOSTINGBUFFERSIZE);
	if(e < eNOERROR) LOM_ERROR(handle, e);
	LOM_FREEOFFSET_POSTINGBUFFER(lomPostingBuffer) = 0;

#define DEALLOCMEM_LOM_Text_Scan_NextPosting(handle, e, e2) \
	e2 = LOM_FreePostingBuffer(handle, &lomPostingBuffer); \
	if(e2 < eNOERROR) LOM_ERROR(handle, e2);		\
	LOM_ERROR(handle, e);

#ifndef SUBINDEX
	/* make column struct */
	/* posting size */
	clist[0].colNo = LOM_INVERTEDINDEX_SIZEOFPOSTING_COLNO;
	clist[0].start = ALL_VALUE;
	clist[0].length = clist[0].dataLength = sizeof(Four);
#endif

	clist[1].colNo = LOM_INVERTEDINDEX_NPOSTINGS_COLNO;
	clist[1].start = ALL_VALUE;
	clist[1].length = sizeof(Four);
	clist[1].dataLength = sizeof(Four);


#ifndef SUBINDEX
	clist[2].colNo = LOM_INVERTEDINDEX_POSTINGLIST_COLNO;
	clist[2].start = 0;
	clist[2].length = LOM_DEFAULTPOSTINGBUFFERSIZE;
	clist[2].dataLength = LOM_DEFAULTPOSTINGBUFFERSIZE;
	clist[2].data.ptr = LOM_PTR_POSTINGBUFFER(lomPostingBuffer);
#endif

	lrdsScanId = LOM_SCANTABLE(handle)[textScan].textScanInfo.oidBasedScan.invertedScanId;
	if(lrdsScanId < 0) LOM_ERROR(handle, eBADPARAMETER_LOM);

	found = SM_FALSE;

	while(SM_TRUE) {
		if(!onlyFetchMode) {
			e = LRDS_NextTuple(LOM_GET_LRDS_HANDLE(handle), lrdsScanId, &tid, &(LOM_SCANTABLE(handle)[textScan].textScanInfo.oidBasedScan.cursor));
			if(e < eNOERROR) {
				DEALLOCMEM_LOM_Text_Scan_NextPosting(handle, e, e2);
			}
			if(e == EOS) {
				*requiredSize = 0;

				e = LOM_FreePostingBuffer(handle, &lomPostingBuffer);
				if(e < eNOERROR) LOM_ERROR(handle, e);
	
				return EOS;
			}
		}

		/* fetch tuple */
#ifdef SUBINDEX
		e = LRDS_FetchTuple(LOM_GET_LRDS_HANDLE(handle), lrdsScanId, SM_TRUE, &tid, 1, &clist[1]);
#else
		e = LRDS_FetchTuple(LOM_GET_LRDS_HANDLE(handle), lrdsScanId, SM_TRUE, &tid, 3, &clist[0]);
#endif
		if(e < eNOERROR) {
			DEALLOCMEM_LOM_Text_Scan_NextPosting(handle, e, e2);
		}

		ptrToPostingBuffer = LOM_PTR_POSTINGBUFFER(lomPostingBuffer);
		offsetInBuffer = 0;
		currentOffset = 0;

		/* local copy to nPostings */
		nPostings = GET_VALUE_FROM_COL_LIST(clist[1], sizeof(nPostings));
#ifdef SUBINDEX
		/* by using logicalDocId, we find the requested posting */
		kval.len = LOM_LONG_SIZE_VAR;
		bcopy(&(LOM_SCANTABLE(handle)[textScan].textScanInfo.oidBasedScan.logicalDocId), &(kval.val[0]), LOM_LONG_SIZE_VAR);
		e = LRDS_OrderedSet_IsMember(LOM_GET_LRDS_HANDLE(handle), 
			lrdsScanId,
			SM_TRUE,
			&tid,
			LOM_INVERTEDINDEX_POSTINGLIST_COLNO,
			&kval,
			LOM_DEFAULTPOSTINGBUFFERSIZE,
			ptrToPostingBuffer,
			NULL);
		if(e < eNOERROR) {
			DEALLOCMEM_LOM_Text_Scan_NextPosting(handle, e, e2);
		}
		if(e == SM_TRUE) found = SM_TRUE;
		else found = SM_FALSE;

		/* get posting length */
		bcopy(&ptrToPostingBuffer[offsetInBuffer], &postingLength, sizeof(Four));

		if(found == SM_TRUE) {
			if(postingLengthFieldSize + postingLength > LOM_DEFAULTPOSTINGBUFFERSIZE) {
				e = LOM_ReallocPostingBuffer(handle, &lomPostingBuffer, postingLengthFieldSize + postingLength);
				if(e < eNOERROR) {
					DEALLOCMEM_LOM_Text_Scan_NextPosting(handle, e, e2);
				}
				e = LRDS_OrderedSet_IsMember(LOM_GET_LRDS_HANDLE(handle), 
					lrdsScanId,
					SM_TRUE,
					&tid,
					LOM_INVERTEDINDEX_POSTINGLIST_COLNO,
					&kval,
					postingLengthFieldSize + postingLength,
					ptrToPostingBuffer,
					NULL);
				if(e < eNOERROR) {
					DEALLOCMEM_LOM_Text_Scan_NextPosting(handle, e, e2);
				}
				if(e == SM_TRUE) found = SM_TRUE;
				else found = SM_FALSE;
			}
		}

#else
		while(nPostings) {
			/* skip logical id field */
			if(!bcmp(&(LOM_SCANTABLE(handle)[textScan].textScanInfo.oidBasedScan.logicalDocId),
				&ptrToPostingBuffer[postingLengthFieldSize + offsetInBuffer],
				LOM_LONG_SIZE_VAR)) {

				/* we found the requested answer */
				found = SM_TRUE;
				/* get out of this while loop */
				break;
			}

			/* decrement the number of postings to read */
			nPostings --;

			if(nPostings == 0) {
				if(onlyFetchMode) {
					DEALLOCMEM_LOM_Text_Scan_NextPosting(handle, eINTERNAL_LOM, e2);
				}
				else break;
			}


			/* get posting length */
			bcopy(&ptrToPostingBuffer[offsetInBuffer], &postingLength, sizeof(Four));

			/* check if the very next posting is in memory buffer or not */
			offsetInBuffer += postingLengthFieldSize + postingLength;
			currentOffset += postingLengthFieldSize + postingLength;

			if(offsetInBuffer + postingLengthSize + sizeof(Four) <= clist[2].retLength) {
				/* we have already read the very next posting */
				/* go back to while-loop */
				continue;
			}

			/* we have read all postings before currentOffset */
			clist[2].start = currentOffset;

			e = LRDS_FetchTuple(LOM_GET_LRDS_HANDLE(handle), lrdsScanId, SM_TRUE, &tid, 1, &clist[2]);
			if( e < eNOERROR) {
				DEALLOCMEM_LOM_Text_Scan_NextPosting(handle, e, e2);
			}

			/* initialize offsetInBuffer after reading posting-list */
			offsetInBuffer = 0;
		}
#endif

		if(found) break;
	} /* while */	

	/* we now return a requested posting */

	if(postingLengthFieldSize + postingLength > bufferLength) {
		LOM_SCANTABLE(handle)[textScan].textScanInfo.oidBasedScan.onlyFetchMode = SM_TRUE;
		*requiredSize = postingLengthFieldSize + postingLength;

		/* error report to user */
		return eBIGGERPOSTINGBUFFERNEEDED_LOM;
	}
	else {
		Four tupleIDFieldSize;
		nPostings = GET_VALUE_FROM_COL_LIST(clist[1], sizeof(nPostings));

		if(LOM_SCANTABLE(handle)[textScan].textScanInfo.oidBasedScan.isContainingTupleID) tupleIDFieldSize = sizeof(TupleID);
		else tupleIDFieldSize = 0;
		
		/* get number of positions */
		bcopy(&ptrToPostingBuffer[offsetInBuffer + postingLengthFieldSize + sizeof(Four) + tupleIDFieldSize], &nPositions, sizeof(Four));
		*weight = CALCULATE_WEIGHT(nPositions, nPostings);

		bcopy(&ptrToPostingBuffer[offsetInBuffer], &postingBuffer[0], postingLengthFieldSize + postingLength);
		*requiredSize = postingLengthFieldSize + postingLength;
	}

	LOM_SCANTABLE(handle)[textScan].textScanInfo.oidBasedScan.onlyFetchMode = SM_FALSE;

	e = LOM_FreePostingBuffer(handle, &lomPostingBuffer);
	if(e < eNOERROR) LOM_ERROR(handle, e);
        
	return eNOERROR;
}

Four LOM_Text_GetNPostingsOfCurrentKeyword(
	LOM_Handle *handle, 
	Four textScan,
	Four *nPostings
)
{
	*nPostings = LOM_SCANTABLE(handle)[textScan].textScanInfo.keywordBasedScan.nPostings;
	return eNOERROR;
}

Four LOM_Text_NextPostings(
	LOM_Handle*	handle,						/* IN: LOM system handle */
	Four		textScan,					/* IN: text scan Id */
	Four		postingLengthBufferSize,	/* IN: size of buffer to hold postings' length */
	char*		postingLengthBuffer,		/* IN: buffer to hold postings' length */
	Four		postingBufferSize,			/* IN: size of buffer to hold postings */
	char*		postingBuffer,				/* IN: buffer to hold postings */
	Four		scanDirection,				/* IN: scan direction to read postings */
	Four		logicalIdHints,				/* IN: logical Id hints to skip postings */
	Four*		nReturnedPosting,			/* OUT: number of read postings */
#ifndef COMPRESSION
	Four*		requiredSize				/* OUT: sufficient size of buffer to hold postings */
#else
	Four*		requiredSize,				/* OUT: sufficient size of buffer to hold postings */
	VolNo*	    volNoOfPostingTupleID,		/* OUT */
	Four*		lastDocId
#endif
)
{
	ColListStruct clist[2];
	Four e;
	Four nPostings;
	Four offsetInBuffer;
	TupleID tid;
#if defined (SUBINDEX) && !defined (ORDEREDSET_BACKWARD_SCAN)
	Four postingLengthFieldSize = LOM_LONG_SIZE_VAR;
#else
	Four postingLengthFieldSize = 0;
#endif
	Four postingLength;

	/* check paramters */
	if(textScan < 0) LOM_ERROR(handle, eBADPARAMETER_LOM);
	if(postingBufferSize < 0) LOM_ERROR(handle, eBADPARAMETER_LOM);
	if(postingBuffer == NULL) LOM_ERROR(handle, eBADPARAMETER_LOM);
	if(requiredSize == NULL) LOM_ERROR(handle, eBADPARAMETER_LOM);

	/* make column struct */
	clist[0].colNo = LOM_INVERTEDINDEX_POSTINGLIST_COLNO;
	clist[0].start = LOM_SCANTABLE(handle)[textScan].textScanInfo.keywordBasedScan.currentPostingOffset;
	clist[0].length = postingBufferSize;
	clist[0].dataLength = postingBufferSize;
	clist[0].data.ptr = postingBuffer;

	clist[1].colNo = LOM_INVERTEDINDEX_NPOSTINGS_COLNO;
	clist[1].start = ALL_VALUE;
	clist[1].length = sizeof(Four);
	clist[1].dataLength = sizeof(Four);

	/* maybe 'onlyFetchMode' is unnecesary */
	if(!LOM_SCANTABLE(handle)[textScan].textScanInfo.keywordBasedScan.onlyFetchMode) 
	{
		if(LOM_SCANTABLE(handle)[textScan].textScanInfo.keywordBasedScan.invertedScanId != NIL)
		{
			e = LRDS_NextTuple(LOM_GET_LRDS_HANDLE(handle), LOM_SCANTABLE(handle)[textScan].textScanInfo.keywordBasedScan.invertedScanId, &tid,
				               &(LOM_SCANTABLE(handle)[textScan].textScanInfo.keywordBasedScan.cursor));
			if(e < eNOERROR) LOM_ERROR(handle, e);

			if(e == EOS) 
			{
				*requiredSize = 0;
				return EOS;
			}
			LOM_SCANTABLE(handle)[textScan].textScanInfo.keywordBasedScan.invertedTableEntryTid = tid;
		}
		else
		{
			tid = LOM_SCANTABLE(handle)[textScan].textScanInfo.keywordBasedScan.invertedTableEntryTid;
			if(LOM_SCANTABLE(handle)[textScan].textScanInfo.keywordBasedScan.invertedTableEntryTidCount == 0)
			{
				*requiredSize = 0;
				return EOS;
			}
			else
				LOM_SCANTABLE(handle)[textScan].textScanInfo.keywordBasedScan.invertedTableEntryTidCount --;
		}

#ifdef SUBINDEX
		/* open ordered-set scan */
		if(LOM_SCANTABLE(handle)[textScan].textScanInfo.keywordBasedScan.orderedSetScanId >=0)
		{
			e = LRDS_OrderedSet_Scan_Close(LOM_GET_LRDS_HANDLE(handle), LOM_SCANTABLE(handle)[textScan].textScanInfo.keywordBasedScan.orderedSetScanId);
			if(e < eNOERROR) LOM_ERROR(handle, e);
		}
		
#if defined (ORDEREDSET_BACKWARD_SCAN)
		e = LRDS_OrderedSet_Scan_Open(LOM_GET_LRDS_HANDLE(handle), LOM_SCANTABLE(handle)[textScan].textScanInfo.keywordBasedScan.invertedOrn, SM_FALSE, &tid, LOM_INVERTEDINDEX_POSTINGLIST_COLNO, scanDirection, NULL);
#else
		e = LRDS_OrderedSet_Scan_Open(LOM_GET_LRDS_HANDLE(handle), LOM_SCANTABLE(handle)[textScan].textScanInfo.keywordBasedScan.invertedOrn, SM_FALSE, &tid, LOM_INVERTEDINDEX_POSTINGLIST_COLNO, NULL);
#endif	
		if(e < eNOERROR) LOM_ERROR(handle, e);
		LOM_SCANTABLE(handle)[textScan].textScanInfo.keywordBasedScan.orderedSetScanId = e;
#endif
	}
	else
		tid = LOM_SCANTABLE(handle)[textScan].textScanInfo.keywordBasedScan.invertedTableEntryTid;

	if(LOM_SCANTABLE(handle)[textScan].textScanInfo.keywordBasedScan.nRemainedPostings == 0)
	{
#ifdef SUBINDEX
		e = LRDS_FetchTuple(LOM_GET_LRDS_HANDLE(handle), LOM_SCANTABLE(handle)[textScan].textScanInfo.keywordBasedScan.invertedOrn, SM_FALSE, &tid, 1, &clist[1]);
#else
		e = LRDS_FetchTuple(LOM_GET_LRDS_HANDLE(handle), LOM_SCANTABLE(handle)[textScan].textScanInfo.keywordBasedScan.invertedOrn, SM_FALSE, &tid, 2, clist);
#endif
		if(e < eNOERROR) LOM_ERROR(handle, e);

		LOM_SCANTABLE(handle)[textScan].textScanInfo.keywordBasedScan.nRemainedPostings = GET_VALUE_FROM_COL_LIST(clist[1], sizeof(Four));
		LOM_SCANTABLE(handle)[textScan].textScanInfo.keywordBasedScan.nPostings = GET_VALUE_FROM_COL_LIST(clist[1], sizeof(Four));
		LOM_SCANTABLE(handle)[textScan].textScanInfo.keywordBasedScan.currentPostingOffset = 0;
	}
	else
	{
#ifdef SUBINDEX
		e = LRDS_FetchTuple(LOM_GET_LRDS_HANDLE(handle), LOM_SCANTABLE(handle)[textScan].textScanInfo.keywordBasedScan.invertedOrn, SM_FALSE, &tid, 1, &clist[1]);
#else
		e = LRDS_FetchTuple(LOM_GET_LRDS_HANDLE(handle), LOM_SCANTABLE(handle)[textScan].textScanInfo.keywordBasedScan.invertedOrn, SM_FALSE, &tid, 2, clist);
#endif
		if(e < eNOERROR) LOM_ERROR(handle, e);
	}

	nPostings = GET_VALUE_FROM_COL_LIST(clist[1], sizeof(Four));

	/* set onlyFetchMode SM_TRUE */
	LOM_SCANTABLE(handle)[textScan].textScanInfo.keywordBasedScan.onlyFetchMode = SM_TRUE;

	offsetInBuffer = 0;

#ifdef SUBINDEX

#ifdef COMPRESSION
	/* get volNo */
	e = LRDS_OrderedSet_GetVolNo(LOM_GET_LRDS_HANDLE(handle), LOM_SCANTABLE(handle)[textScan].textScanInfo.keywordBasedScan.invertedOrn, SM_FALSE, &tid, LOM_INVERTEDINDEX_POSTINGLIST_COLNO, volNoOfPostingTupleID, NULL); 
	if(e < eNOERROR) LOM_ERROR(handle, e);
#endif

	while(1)
	{
		if(logicalIdHints >= 0)
		{
			e = LRDS_OrderedSet_Scan_SkipElementsUntilGivenKeyValue(LOM_GET_LRDS_HANDLE(handle), 
					LOM_SCANTABLE(handle)[textScan].textScanInfo.keywordBasedScan.orderedSetScanId, 
					sizeof(logicalIdHints), (char*)&logicalIdHints);
			if(e < eNOERROR) LOM_ERROR(handle, e);
		}

#if defined (ORDEREDSET_BACKWARD_SCAN)
#ifndef COMPRESSION
		*nReturnedPosting = LRDS_OrderedSet_Scan_NextElements(LOM_GET_LRDS_HANDLE(handle), LOM_SCANTABLE(handle)[textScan].textScanInfo.keywordBasedScan.orderedSetScanId, postingLengthBufferSize, postingLengthBuffer, postingBufferSize, postingBuffer);
#else
		*nReturnedPosting = LRDS_OrderedSet_Scan_NextElements(LOM_GET_LRDS_HANDLE(handle), LOM_SCANTABLE(handle)[textScan].textScanInfo.keywordBasedScan.orderedSetScanId, postingLengthBufferSize, postingLengthBuffer, postingBufferSize, postingBuffer, lastDocId);
#endif
#else
		*nReturnedPosting = LRDS_OrderedSet_Scan_NextElements(LOM_GET_LRDS_HANDLE(handle), LOM_SCANTABLE(handle)[textScan].textScanInfo.keywordBasedScan.orderedSetScanId, postingBufferSize, postingBuffer);
#endif
		if(*nReturnedPosting < eNOERROR) LOM_ERROR(handle, *nReturnedPosting);

		if(*nReturnedPosting == 0)
		{
			/*
			   If we had read nothing, there can be two cases.
			     case 1: The size of buffer is not sufficient to hold a posting.
			     case 2: We have read all postings. So there is no remained posting.
			*/

			if (scanDirection == LOM_TEXT_SCAN_FORWARD) offsetInBuffer = 0;
			else offsetInBuffer = postingLengthBufferSize - sizeof(Four);

#if defined (ORDEREDSET_BACKWARD_SCAN)
			bcopy(&postingLengthBuffer[offsetInBuffer], &postingLength, sizeof(Four));
#else
			bcopy(&postingBuffer[offsetInBuffer], &postingLength, sizeof(Four));
#endif

			if(postingBufferSize <= (postingLength + postingLengthFieldSize)) 
			{
				/* Case 1: The size of buffer is not sufficient to hold a posting. */
				
				*requiredSize = postingLengthFieldSize + postingLength;
				return eBIGGERPOSTINGBUFFERNEEDED_LOM;
			}
			else
			{
				/* Case 2: We have read all postings. So there is no remained posting. */
				
				*requiredSize = 0;

				LOM_SCANTABLE(handle)[textScan].textScanInfo.keywordBasedScan.onlyFetchMode = SM_FALSE;
				LOM_SCANTABLE(handle)[textScan].textScanInfo.keywordBasedScan.currentPostingOffset = 0;
				LOM_SCANTABLE(handle)[textScan].textScanInfo.keywordBasedScan.nRemainedPostings = 0;

				/* Execute recursive call to try more postings in next keyword. */
#ifndef COMPRESSION
				e = LOM_Text_NextPostings(handle, textScan, postingLengthBufferSize, postingLengthBuffer, postingBufferSize, postingBuffer, scanDirection, logicalIdHints, nReturnedPosting, requiredSize);
#else
				e = LOM_Text_NextPostings(handle, textScan, postingLengthBufferSize, postingLengthBuffer, postingBufferSize, postingBuffer, scanDirection, logicalIdHints, nReturnedPosting, requiredSize, volNoOfPostingTupleID, lastDocId);
#endif
				if(e < eNOERROR) LOM_ERROR(handle, e);

				return e;
			}
		}
		else
		{
			/* When we had read something, we can't examine nRemainedPostings if we skipped some postings. */
			/* So, we do not calculate nRemainedPostings in this case. */

			*requiredSize = 0;

			LOM_SCANTABLE(handle)[textScan].textScanInfo.keywordBasedScan.onlyFetchMode = SM_TRUE;
			
			return eNOERROR;
		}
	}
#else
	*nReturnedPosting = 0;

	while(LOM_SCANTABLE(handle)[textScan].textScanInfo.keywordBasedScan.nRemainedPostings)
	{
		/* nPosition */
		bcopy(&postingBuffer[offsetInBuffer], &postingLength, sizeof(Four));

		if(postingLengthFieldSize + offsetInBuffer + postingLength <= clist[0].retLength)
		{
			*nReturnedPosting = *nReturnedPosting + 1;
			LOM_SCANTABLE(handle)[textScan].textScanInfo.keywordBasedScan.nRemainedPostings --;

			offsetInBuffer += postingLengthFieldSize + postingLength;
			LOM_SCANTABLE(handle)[textScan].textScanInfo.keywordBasedScan.currentPostingOffset += postingLengthFieldSize + postingLength;
		}
		else
		{
			if(*nReturnedPosting == 0)
			{
				*requiredSize = postingLengthFieldSize + postingLength;
				return eBIGGERPOSTINGBUFFERNEEDED_LOM;
			}
			else
			{
				LOM_SCANTABLE(handle)[textScan].textScanInfo.keywordBasedScan.onlyFetchMode = SM_TRUE;
				*requiredSize = offsetInBuffer;
				return eNOERROR;
			}
		}
	} /* while-loop */

	*requiredSize = offsetInBuffer;

	LOM_SCANTABLE(handle)[textScan].textScanInfo.keywordBasedScan.onlyFetchMode = SM_FALSE;
	LOM_SCANTABLE(handle)[textScan].textScanInfo.keywordBasedScan.currentPostingOffset = 0;

	return eNOERROR;
#endif
}

Four LOM_Text_GetCursorKeyword(
	LOM_Handle *handle, 
	Four textScan,
	char *keyword
)
{
	LRDS_Cursor *cursor;

	if(LOM_SCANTABLE(handle)[textScan].scanType == LOM_GENERAL_SCAN) LOM_ERROR(handle, eBADPARAMETER_LOM);
	
	if(LOM_SCANTABLE(handle)[textScan].scanType == LOM_INVERTEDFILE_KEYWORDBASED_SCAN)
		cursor = LOM_SCANTABLE(handle)[textScan].textScanInfo.keywordBasedScan.cursor;
	else if(LOM_SCANTABLE(handle)[textScan].scanType == LOM_INVERTEDFILE_OIDBASED_SCAN)
		cursor = LOM_SCANTABLE(handle)[textScan].textScanInfo.oidBasedScan.cursor;
	else
	{
		strcpy(keyword, "");
		return eNOERROR;
	}

	memcpy(keyword, cursor->btree.key.val, cursor->btree.key.len);
	keyword[cursor->btree.key.len] = 0;

	return eNOERROR;

}

/* Batch Index Building */

Four LOM_Text_BatchInvertedIndexBuild(
	LOM_Handle *handle, 
	Four volId,
	Four temporaryVolId,
	char *className
)
{
	Four e, e2;
	Four ocn;
	Four idxForClassInfo;		/* index for class information */
	Four i;
	catalog_SysIndexesOverlay *ptrToSysIndexes;	
	catalog_SysClassesOverlay *ptrToSysClasses;
	catalog_SysAttributesOverlay *ptrToSysAttributes;
	LOM_IndexID iid;
	BoundCond startBound;		/* start bound */
	BoundCond stopBound;		/* stop bound */
	Four lomScanNum;
	LockParameter lockup;
	char *tempName[LOM_MAXNUMOFATTRIBUTE];
	char *tempDocIdName[LOM_MAXNUMOFATTRIBUTE];
	char *tempDocIdName2;
	Boolean tagToSortFile[LOM_MAXNUMOFATTRIBUTE];
	char *tempName2;
	char *tempDir;
	OID oid;
	LOM_TextDesc textDesc;
	Four logicalDocId;
	Two colNo;
	Four v;
	ColListStruct clist[1];
#ifdef TEXT_TEST
	Four count=1;
#endif
	lom_Text_ConfigForInvertedIndexBuild config;
	lom_FptrToKeywordExtractor keywordExtractorFPtr;
	lom_FptrToGetNextPostingInfo getNextPostingInfoFPtr;
	lom_FptrToFinalizeKeywordExtraction finalizeKeywordExtractionFPtr;
	lom_FptrToFilter filterFPtr;

	/* set inverted index building configuration for maximum performance */
	config.isBuildingDocIdIndex						= SM_TRUE;
	config.isBuildingExternalReverseKeywordFile		= SM_FALSE;
	config.isSortingPostingFile						= SM_TRUE;
	config.isUsingBulkLoading						= SM_FALSE;		
	config.isUsingKeywordIndexBulkLoading			= SM_FALSE;
	config.isUsingReverseKeywordIndexBulkLoading	= SM_FALSE;	
	config.isUsingStoredPosting                     = SM_FALSE;

	tempDir = getenv(TEXT_TEMP_PATH);
	if(tempDir == NULL) LOM_ERROR(handle, eCONFIGURATION_LOM);

	/* open class */
	ocn = LOM_OpenClass(handle, volId, className);
	if( ocn < eNOERROR) LOM_ERROR(handle, ocn);

	/* get class information */
	e = Catalog_GetClassInfo(handle, volId, LOM_USEROPENCLASSTABLE(handle)[ocn].classID, &idxForClassInfo);
	if( e < eNOERROR) LOM_ERROR(handle, e);

	v = Catalog_GetVolIndex(handle, volId);
	if(v < eNOERROR) LOM_ERROR(handle, v);

	/* set memory pointer */
	ptrToSysClasses = &CATALOG_GET_CLASSINFOTBL(handle, v)[idxForClassInfo];
	ptrToSysIndexes =  &CATALOG_GET_INDEXINFOTBL(handle, v)[CATALOG_GET_INDEXINFOTBL_INDEX(ptrToSysClasses)];
	ptrToSysAttributes =  &CATALOG_GET_ATTRINFOTBL(handle, v)[CATALOG_GET_ATTRINFOTBL_INDEX(ptrToSysClasses)];

	for(i = 0; i < CATALOG_GET_INDEXNUM(ptrToSysClasses); i++) {
		if(CATALOG_GET_INDEXCOLNO(ptrToSysIndexes)[i] == LOM_LOGICALID_COLNO)  {
			iid = ptrToSysIndexes->iid;
			break;
		}
	}

	if(i == CATALOG_GET_INDEXNUM(ptrToSysClasses)) LOM_ERROR(handle, eINTERNAL_LOM);

	startBound.op = SM_BOF;
	stopBound.op = SM_EOF;

	/* lock parameter */
	/* we here donot release file-level lock until commit */
	lockup.mode = L_X;
	lockup.duration = L_COMMIT;

	/* open index scan */
	lomScanNum = LOM_OpenIndexScan(handle, ocn, &iid, &startBound, &stopBound, 0, (BoolExp *)NULL, &lockup);
	if(lomScanNum < eNOERROR) LOM_ERROR(handle, lomScanNum);

	for(i = 0; i < LOM_USEROPENCLASSTABLE(handle)[ocn].numOfTextAttrs; i++) { 
		tempName[i] = tempnam(tempDir, "TEXT"); 
		if(tempName[i] == NULL) LOM_ERROR(handle, eOUTOFMEMORY_LOM);
		
		tempDocIdName[i] = tempnam(tempDir, "TEXT_DOC");
		if(tempDocIdName[i] == NULL) LOM_ERROR(handle, eOUTOFMEMORY_LOM);
		tagToSortFile[i] = SM_FALSE;
	}

#define ERROR_LOM_Text_BatchInvertedIndexBuild(handle, e) \
	for(i = 0; i < LOM_USEROPENCLASSTABLE(handle)[ocn].numOfTextAttrs; i++) { \
		free(tempName[i]); \
		free(tempDocIdName[i]); \
	} \
	LOM_ERROR(handle, e);

	tempName2 = NULL;
	tempDocIdName2 = NULL;

	while((e = LOM_NextObject(handle, lomScanNum, &oid, NULL)) != EOS) {
		logicalDocId = lom_Text_GetLogicalId(handle, lomScanNum, SM_TRUE, &oid);
		if(logicalDocId < 0) LOM_ERROR(handle, logicalDocId);

		for(i = 0; i < LOM_USEROPENCLASSTABLE(handle)[ocn].numOfTextAttrs; i++) {

			e = LOM_Text_GetDescriptor(handle, lomScanNum, SM_TRUE, &oid, (Two)GET_USERLEVEL_COLNO(LOM_USEROPENCLASSTABLE(handle)[ocn].textColNo[i]), &textDesc);
			if(e < eNOERROR) {
				ERROR_LOM_Text_BatchInvertedIndexBuild(handle, e);
			}

			if(ISINDEXED_TEXTDESC(textDesc)) continue;	/* have already built index on this text attribute column */

			if(DOES_NOCONTENT_EXIST_TEXTDESC(textDesc)) {
				RESET_CONTENTSIZE_TEXTDESC(textDesc);
				RESET_ISINDEXED_TEXTDESC(textDesc);
				SET_HASBEENINDEXED_TEXTDESC(textDesc);
				e = LOM_Text_UpdateDescriptor(handle, lomScanNum, SM_TRUE, &oid, (Two)GET_USERLEVEL_COLNO(LOM_USEROPENCLASSTABLE(handle)[ocn].textColNo[i]), &textDesc);
				if(e < eNOERROR) {
					ERROR_LOM_Text_BatchInvertedIndexBuild(handle, e);
				}
				continue;
			}

			colNo = LOM_USEROPENCLASSTABLE(handle)[ocn].textColNo[i];

			if(HASBEENINDEXED_TEXTDESC(textDesc)) {
				if(tempName2 == NULL) tempName2 = tempnam(tempDir, "TEXT");
				if(tempName2 == NULL) {
					ERROR_LOM_Text_BatchInvertedIndexBuild(handle, eOUTOFMEMORY_LOM);
				}

				if(tempDocIdName2 == NULL) tempDocIdName2 = tempnam(tempDir, "TEXT_DOC"); 
				if(tempDocIdName2 == NULL) {
					ERROR_LOM_Text_BatchInvertedIndexBuild(handle, eOUTOFMEMORY_LOM);
				}

				/* extract keywords from content object and posting information
		   		   put them into temporoary posting file */
				e = lom_Text_GetKeywordExtractorFPtr(handle, ocn, LOM_USEROPENCLASSTABLE(handle)[ocn].textColNo[i], &keywordExtractorFPtr);
				if(e < eNOERROR) LOM_ERROR(handle, e);
				e = lom_Text_GetGetNextPostingInfoFPtr(handle, ocn, LOM_USEROPENCLASSTABLE(handle)[ocn].textColNo[i], &getNextPostingInfoFPtr);
				if(e < eNOERROR) LOM_ERROR(handle, e);
				e = lom_Text_GetFinalizeKeywordExtractionFPtr(handle, ocn, LOM_USEROPENCLASSTABLE(handle)[ocn].textColNo[i], &finalizeKeywordExtractionFPtr);
				if(e < eNOERROR) LOM_ERROR(handle, e);
				e = lom_Text_GetFilterFPtr(handle, ocn, LOM_USEROPENCLASSTABLE(handle)[ocn].textColNo[i], &filterFPtr);
				if(e < eNOERROR) LOM_ERROR(handle, e);

				e = lom_Text_ExtractKeywordFromContentObjectIntoTempFile(handle, 
					keywordExtractorFPtr,
					getNextPostingInfoFPtr,
					finalizeKeywordExtractionFPtr,
					filterFPtr,
 					lomScanNum,
 					SM_TRUE,
					&textDesc.contentTid,
					&oid,
					logicalDocId,
					colNo,
					tempName2);
				if(e < eNOERROR) {
					e2 = lom_Text_DestroyTempFile(handle, tempName2);
					if(e2 < eNOERROR) {
						free(tempName2);
						ERROR_LOM_Text_BatchInvertedIndexBuild(handle, e);
					}
					free(tempName2);
					ERROR_LOM_Text_BatchInvertedIndexBuild(handle, e);
				}

				/* add inverted index entry from temproary posting file */
				e = lom_Text_AddInvertedIndexEntryFromTempFile(
						handle, 
						temporaryVolId,
						lomScanNum, 
						SM_TRUE, 
						colNo, 
						tempName2, 
						tempDocIdName2, 
						SM_TRUE, 
						&config);
				if(e < eNOERROR) {
					e2 = lom_Text_DestroyTempFile(handle, tempName2 );
					if(e2 < eNOERROR) {
						free(tempName2);
						LOM_ERROR(handle, e);
					}
					free(tempName2);
					ERROR_LOM_Text_BatchInvertedIndexBuild(handle, e);
				}

				/* destroy temporary posting file */
				e = lom_Text_DestroyTempFile(handle, tempName2);
				if(e < eNOERROR) {
					free(tempName);
					ERROR_LOM_Text_BatchInvertedIndexBuild(handle, e);
				}

				SET_ISINDEXED_TEXTDESC(textDesc);
				SET_HASBEENINDEXED_TEXTDESC(textDesc);
			}
			else {
				tagToSortFile[i] = SM_TRUE;		/* ok. we have file(s) to sort */

				e = lom_Text_GetKeywordExtractorFPtr(handle, ocn, LOM_USEROPENCLASSTABLE(handle)[ocn].textColNo[i], &keywordExtractorFPtr);
				if(e < eNOERROR) LOM_ERROR(handle, e);
				e = lom_Text_GetGetNextPostingInfoFPtr(handle, ocn, LOM_USEROPENCLASSTABLE(handle)[ocn].textColNo[i], &getNextPostingInfoFPtr);
				if(e < eNOERROR) LOM_ERROR(handle, e);
				e = lom_Text_GetFinalizeKeywordExtractionFPtr(handle, ocn, LOM_USEROPENCLASSTABLE(handle)[ocn].textColNo[i], &finalizeKeywordExtractionFPtr);
				if(e < eNOERROR) LOM_ERROR(handle, e);
				e = lom_Text_GetFilterFPtr(handle, ocn, LOM_USEROPENCLASSTABLE(handle)[ocn].textColNo[i], &filterFPtr);
				if(e < eNOERROR) LOM_ERROR(handle, e);

				e = lom_Text_ExtractKeywordFromContentObjectIntoTempFile(
						handle, 
						keywordExtractorFPtr,
						getNextPostingInfoFPtr,
						finalizeKeywordExtractionFPtr,
						filterFPtr,
						lomScanNum,
						SM_TRUE,
						&textDesc.contentTid,
						&oid,
						logicalDocId,
						colNo,
						tempName[i]);
				if(e < eNOERROR) {
					ERROR_LOM_Text_BatchInvertedIndexBuild(handle, eOUTOFMEMORY_LOM);
				}
			}
			SET_ISINDEXED_TEXTDESC(textDesc);
			SET_HASBEENINDEXED_TEXTDESC(textDesc);
			clist[0].colNo = LOM_USEROPENCLASSTABLE(handle)[ocn].textColNo[i];
			clist[0].start = ALL_VALUE;
			clist[0].length = sizeof(LOM_TextDesc);
			clist[0].dataLength = sizeof(LOM_TextDesc);
			clist[0].data.ptr = &textDesc;
			clist[0].nullFlag = SM_FALSE;
			e = LRDS_UpdateTuple(LOM_GET_LRDS_HANDLE(handle), LOM_SCANTABLE(handle)[lomScanNum].lrdsScanId, SM_TRUE, (TupleID *)&oid, 1, &clist[0]);
			if(e < eNOERROR) {
				ERROR_LOM_Text_BatchInvertedIndexBuild(handle, e);
			}

		} /* for */
	} /* while */

	if(tempName2 != NULL) free(tempName2);

	/* add inverted index entry from temproary posting file */
	for(i = 0; i < LOM_USEROPENCLASSTABLE(handle)[ocn].numOfTextAttrs; i++) {
		if(tagToSortFile[i] == SM_TRUE) {	
			colNo = LOM_USEROPENCLASSTABLE(handle)[ocn].textColNo[i];

			e = lom_Text_AddInvertedIndexEntryFromTempFile(
					handle, 
					temporaryVolId,
					lomScanNum, 
					SM_TRUE,
					colNo, 
					tempName[i], 
					tempDocIdName[i], 
					SM_FALSE,
					&config);
			if(e < eNOERROR) {
				/* destroy temporary files */
		 		for(i = 0; i < LOM_USEROPENCLASSTABLE(handle)[ocn].numOfTextAttrs; i++) {
 					e2 = lom_Text_DestroyTempFile(handle, tempName[i]);
					if( e2 < eNOERROR) LOM_ERROR(handle, e2);
 				}
 				ERROR_LOM_Text_BatchInvertedIndexBuild(handle, e);
			}
		}
	}

	/* destroy temporary files */
 	for(i = 0; i < LOM_USEROPENCLASSTABLE(handle)[ocn].numOfTextAttrs; i++) {
		if(tagToSortFile[i] == SM_TRUE) {	
	 		e = lom_Text_DestroyTempFile(handle, tempName[i]);
			if( e < eNOERROR) {
				ERROR_LOM_Text_BatchInvertedIndexBuild(handle, e);
			}
		}
		free(tempName[i]); 
		free(tempDocIdName[i]); 
	}

 	e = LOM_CloseScan(handle, lomScanNum);
 	if(e < eNOERROR) LOM_ERROR(handle, e);

 	e = LOM_CloseClass(handle, ocn);
 	if(e < eNOERROR) LOM_ERROR(handle, e);

	return eNOERROR;
}
 
/***************************************/
/* Utility Interfaces                  */
/***************************************/

Four LOM_Text_GetIndexID(
	LOM_Handle *handle, 
	Four ocn,		/* IN: open class number */
	Four colNo,		/* IN: column number */
	LOM_IndexID *iid	/* OUT: index id */
)
{

	Four numOfReturnedIndex;
	Four idxForIndexInfoTbl;
	Four e;
	lrds_RelTableEntry *relTableEntry; /* pointer to an entry of relation table */
	catalog_SysIndexesOverlay *sysIndexes;
	Four v;

	if(ocn < 0) LOM_ERROR(handle, eBADPARAMETER_LOM);

	if(colNo < 0) LOM_ERROR(handle, eBADPARAMETER_LOM);

	if(iid == NULL) LOM_ERROR(handle, eBADPARAMETER_LOM);

	relTableEntry = LRDS_GET_RELTABLE_ENTRY(LOM_GET_LRDS_HANDLE(handle), ocn);
	numOfReturnedIndex = 1;

	e = Catalog_GetIndexInfoByAttrNum(
		handle, 
		relTableEntry->ri.fid.volNo,
		LOM_USEROPENCLASSTABLE(handle)[ocn].classID,
		colNo,		/* colNo shuld be user-level colno */
		&numOfReturnedIndex,
		&idxForIndexInfoTbl
		);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	v = Catalog_GetVolIndex(handle, relTableEntry->ri.fid.volNo);
	if(v < eNOERROR) LOM_ERROR(handle, e);

	sysIndexes = &CATALOG_GET_INDEXINFOTBL(handle, v)[idxForIndexInfoTbl];

	*iid = CATALOG_GET_INDEXID(sysIndexes);

	return eNOERROR;
}

Four LOM_Text_GetNPostings(
	LOM_Handle *handle, 
	Four ocn,	/* IN: open class number */
	LOM_IndexID *indexId,	/* IN: index id */
	Four keywordKind,	/* IN: kind of keyword */
	BoundCond *keywordStartBound, /* IN: start bound */
	BoundCond *keywordStopBound,	/* IN: stop bound */
	LockParameter *lockup,		/* IN: lock parameter */
	Four *nPostings
)
{

	Four textScan;
	Four e;
	ColListStruct clist[1];
	Four numOfReturnedPostings;
	TupleID tid;

	textScan = LOM_Text_OpenIndexScan(handle, ocn, indexId, keywordKind, keywordStartBound, keywordStopBound, lockup);
	if(textScan < eNOERROR) LOM_ERROR(handle, textScan);

	clist[0].colNo = LOM_INVERTEDINDEX_NPOSTINGS_COLNO;
	clist[0].start = ALL_VALUE;
	clist[0].length = sizeof(Four);
	clist[0].dataLength = sizeof(Four);

	numOfReturnedPostings = 0;
	while((e = LRDS_NextTuple(LOM_GET_LRDS_HANDLE(handle), LOM_SCANTABLE(handle)[textScan].textScanInfo.keywordBasedScan.invertedScanId, &tid, &(LOM_SCANTABLE(handle)[textScan].textScanInfo.keywordBasedScan.cursor)))!= EOS)
	{
		if(e < eNOERROR) LOM_ERROR(handle, e);

		e = LRDS_FetchTuple(LOM_GET_LRDS_HANDLE(handle), LOM_SCANTABLE(handle)[textScan].textScanInfo.keywordBasedScan.invertedScanId, SM_TRUE, NULL, 1, &clist[0]);
		if(e < eNOERROR) LOM_ERROR(handle, e);

		numOfReturnedPostings += GET_VALUE_FROM_COL_LIST(clist[0], sizeof(numOfReturnedPostings));
	}
	e = LOM_CloseScan(handle, textScan);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	*nPostings = numOfReturnedPostings;

	return eNOERROR;
}

Four LOM_Text_Keyword_Scan_Open(
	LOM_Handle*			handle,			/* IN  lom system handle */
	Four				ocn,			/* IN  open class number of to be scanned */
	Two					colNo,			/* IN  column of of to be scanned */
	char*				keyword			/* IN  a string ended with * which represents keyword range to be scanned (example - KOREA*) */
)
{
	Four			e;
	IndexID			iid;
	LockParameter	lockup;		/* lock parameter */
	Four			lrdsScanNum;
	Four			orn;
	char			startKeyword[LOM_MAXKEYWORDSIZE];
    char			stopKeyword[LOM_MAXKEYWORDSIZE];
	Two				startKeyLen;
	Two				stopKeyLen;
	BoundCond		keywordStartBound;
	BoundCond		keywordStopBound;
	Four			i;
	Four			keywordLength;
	
	/* open relation number for inverted index table */
	orn = lom_Text_GetInvertedIndexTableORN(handle, ocn, SM_FALSE, (Two)GET_SYSTEMLEVEL_COLNO(colNo));
	if(orn < 0) LOM_ERROR(handle, eBADPARAMETER_LOM);

	/* get index id */
	e = lom_Text_GetKeywordIndex(handle, ocn, SM_FALSE, (Two)GET_SYSTEMLEVEL_COLNO(colNo), &iid);
	if(e < 0) LOM_ERROR(handle, eBADPARAMETER_LOM);

	/* keywordStartBound, keywordStopBound */
	keywordLength = strlen(keyword);
	for(i = 0; i < keywordLength; i++)
	{
		if(keyword[i] == '*')
		{
			startKeyword[i] = (unsigned char)0;
			stopKeyword[i]  = (unsigned char)0xff;
			stopKeyword[i + 1] = 0;
			break;
		}
		else
		{
			startKeyword[i] = keyword[i];
			stopKeyword[i]  = keyword[i];
		}
	}
	startKeyword[i] = 0;
	stopKeyword[i]  = 0;

	startKeyLen = strlen(startKeyword);
	keywordStartBound.op = SM_GE;
	keywordStartBound.key.len = sizeof(Two) + startKeyLen;
	memcpy(&(keywordStartBound.key.val[0]), &startKeyLen, sizeof(Two));
	memcpy(&(keywordStartBound.key.val[sizeof(Two)]), startKeyword, startKeyLen);

	stopKeyLen = strlen(stopKeyword);
	keywordStopBound.op = SM_LE;
	keywordStopBound.key.len = sizeof(Two) + stopKeyLen;
	memcpy(&(keywordStopBound.key.val[0]), &stopKeyLen, sizeof(Two));
	memcpy(&(keywordStopBound.key.val[sizeof(Two)]), stopKeyword, stopKeyLen);

	lockup.mode = L_S;
	lockup.duration = L_COMMIT;
	lrdsScanNum = LRDS_OpenIndexScan(LOM_GET_LRDS_HANDLE(handle), orn, &iid, &keywordStartBound, &keywordStopBound, 0, NULL, &lockup);
	if(lrdsScanNum < eNOERROR) LOM_ERROR(handle, lrdsScanNum);

	return lrdsScanNum;
}

Four LOM_Text_Keyword_Scan_Open_WithBoundCondition(
	LOM_Handle*			handle,			/* IN  lom system handle */
	Four				ocn,			/* IN  open class number of to be scanned */
	Two					colNo,			/* IN  column of of to be scanned */
	Four				keywordKind,	/* IN  kind of keyword, KEYWORD or REVERSEKEYWORD */
	Two					startBoundLen,	/* IN  start bound condition length */
	char*				startBoundVal,	/* IN  start bound condition value */
	Two					stopBoundLen,	/* IN  stop bound condition length */
	char*				stopBoundVal	/* IN  stop bound condition value */
)
{
	Four			e;
	IndexID			iid;
	LockParameter	lockup;		/* lock parameter */
	Four			lrdsScanNum;
	Four			orn;
	BoundCond		keywordStartBound;
	BoundCond		keywordStopBound;

	/* parameter check */
	if(!(keywordKind == KEYWORD || keywordKind == REVERSEKEYWORD)) LOM_ERROR(handle, eBADPARAMETER_LOM);

	/* open relation number for inverted index table */
	orn = lom_Text_GetInvertedIndexTableORN(handle, ocn, SM_FALSE, (Two)GET_SYSTEMLEVEL_COLNO(colNo));
	if(orn < 0) LOM_ERROR(handle, eBADPARAMETER_LOM);

	/* get index id */
	if(keywordKind == KEYWORD)
	{
		e = lom_Text_GetKeywordIndex(handle, ocn, SM_FALSE, (Two)GET_SYSTEMLEVEL_COLNO(colNo), &iid);
		if(e < 0) LOM_ERROR(handle, eBADPARAMETER_LOM);
	}
	else
	{
		e = lom_Text_GetReverseKeywordIndex(handle, ocn, SM_FALSE, (Two)GET_SYSTEMLEVEL_COLNO(colNo), &iid);
		if(e < 0) LOM_ERROR(handle, eBADPARAMETER_LOM);
	}

	keywordStartBound.op = SM_GE;
	keywordStartBound.key.len = sizeof(Two) + startBoundLen;
	memcpy(&(keywordStartBound.key.val[0]), &startBoundLen, sizeof(Two));
	memcpy(&(keywordStartBound.key.val[sizeof(Two)]), startBoundVal, startBoundLen);

	keywordStopBound.op = SM_LE;
	keywordStopBound.key.len = sizeof(Two) + stopBoundLen;
	memcpy(&(keywordStopBound.key.val[0]), &stopBoundLen, sizeof(Two));
	memcpy(&(keywordStopBound.key.val[sizeof(Two)]), stopBoundVal, stopBoundLen);

	lockup.mode = L_S;
	lockup.duration = L_COMMIT;
	lrdsScanNum = LRDS_OpenIndexScan(LOM_GET_LRDS_HANDLE(handle), orn, &iid, &keywordStartBound, &keywordStopBound, 0, NULL, &lockup);
	if(lrdsScanNum < eNOERROR) LOM_ERROR(handle, lrdsScanNum);

	return lrdsScanNum;
}

Four LOM_Text_Keyword_Scan_Close(
	LOM_Handle*			handle,			/* IN  lom system handle */
	Four				scanId			/* IN  scan id to be closed */
)
{
	Four e;

	e = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), scanId);
	if(e < eNOERROR) LOM_ERROR(handle, e);

	return eNOERROR;
}

Four LOM_Text_Keyword_Scan_Next(
	LOM_Handle*			handle,			/* IN  lom system handle */
	Four				scanId,			/* IN  scan id */
	Four				keywordKind,	/* IN  keyword kind KEYWORD or REVERSEKEYWORD */
	char*				keyword,		/* OUT keyword buffer to be returned */
	Four*               nDocuments,		/* OUT number of document associated with the keyword */
	TupleID*			invertedIndexEntryTupleID /* OUT tuple id of inverted index entry */
)
{
	TupleID				tid;
	ColListStruct		clist[1];
	Four				e;
	LRDS_Cursor*		cursor;
	Two					length;
	char				reverseKeyword[LOM_MAXKEYWORDSIZE];

	if(!(keywordKind == KEYWORD || keywordKind == REVERSEKEYWORD)) LOM_ERROR(handle, eBADPARAMETER_LOM);

	e = LRDS_NextTuple(LOM_GET_LRDS_HANDLE(handle), scanId, &tid, &cursor);
	if(e < eNOERROR) LOM_ERROR(handle, e);
	if(e == EOS) return EOS;

	memcpy(&length, &cursor->btree.key.val[0], sizeof(Two));
	memcpy(keyword, &cursor->btree.key.val[sizeof(Two)], length);
	if(length >= 0) keyword[length] = 0;

	if(keywordKind == REVERSEKEYWORD)
	{
		makeReverseStr(keyword, reverseKeyword, length);
		strcpy(keyword, reverseKeyword);
	}

	if(nDocuments)
	{
		clist[0].colNo		= LOM_INVERTEDINDEX_NPOSTINGS_COLNO;
		clist[0].start		= ALL_VALUE;
		clist[0].dataLength = sizeof(Four);

		e = LRDS_FetchTuple(LOM_GET_LRDS_HANDLE(handle), scanId, SM_TRUE, &tid, 1, &clist[0]);
		if(e < eNOERROR) LOM_ERROR(handle, e);

		*nDocuments = GET_VALUE_FROM_COL_LIST(clist[0], sizeof(*nDocuments));
	}
	
	if(invertedIndexEntryTupleID)
		memcpy(invertedIndexEntryTupleID, &tid, sizeof(TupleID));

	return eNOERROR;
}


Four LOM_Text_GetNumOfTextObjectsInClass(
	LOM_Handle*			handle,			/* IN  lom system handle */
	Four				ocn,			/* IN  open class number */
	Four*				nObjects		/* OUT number of objects in class */
)
{
	lrds_RelTableEntry* relTableEntry;

	relTableEntry = LRDS_GET_RELTABLE_ENTRY(LOM_GET_LRDS_HANDLE(handle), ocn);

	*nObjects = relTableEntry->ri.nTuples;

	return eNOERROR;
}


#else /* SLIMDOWN_TEXTIR */

#include "LOM_Internal.h"
#include "LOM.h"
#include "Catalog.h"
#include "Catalog_Internal.h"

Four LOM_Text_GetOIDFromLogicalDocId(
	LOM_Handle	*handle,
	Four		ocn,
	Four		logicalDocId,
	OID			*oid
)
{
	return eTEXTIR_NOTENABLED_LOM;
} 


Four LOM_Text_DefinePostingStructure(
	LOM_Handle *handle,
	Four volId,
	char *className,
	char *attrName,
	PostingStructureInfo *postingInfo
)
{
	return eTEXTIR_NOTENABLED_LOM;
} 


Four LOM_Text_CreateContent(
	LOM_Handle *handle, 
	Four ocnOrScanId,	/* IN: one of the following open scan numbers : LOM_OpenScan, LOM_OpenIndexScan, LOM_Text_OpenIndexScan, LOM_Text_Scan_Open */
	Boolean useScanFlag,	/* IN: flag */
	OID *oid,		/* IN: object containging the give text type attribute */
	Two colNum,		/* IN: column number */
	TextColStruct *text,	/* IN: text column struct */
	LOM_TextDesc	*textDesc 	/* INOUT: text descriptor */
)
{
	return eTEXTIR_NOTENABLED_LOM;
} 


Four LOM_Text_GetDescriptor(
	LOM_Handle *handle, 
	Four ocnOrScanId,	/* IN: open scan number */
	Boolean useScanFlag,
	OID *oid,	/* IN: oid */
	Two colNo,	/* IN: column number */
	LOM_TextDesc	*textDesc	/* OUT: text descriptor */
)
{
	return eTEXTIR_NOTENABLED_LOM;
} 


Four LOM_Text_UpdateDescriptor(
	LOM_Handle *handle, 
	Four ocnOrScanId,	/* IN: open scan number */
	Boolean useScanFlag, /* IN: flag */
	OID *oid,	/* IN: oid */
	Two colNo,	/* IN: column number */
	LOM_TextDesc	*textDesc	/* OUT: text descriptor */
)
{
	return eTEXTIR_NOTENABLED_LOM;
} 


Four LOM_Text_DestroyContent(
	LOM_Handle *handle, 
	Four ocnOrScanId,		/* open scan number */
	Boolean useScanFlag,	/* flag */
	OID *oid,		/* oid */
	Two colNo,		/* column number */
	LOM_TextDesc *textDesc 	/* INOUT: text descriptor */
)	
{
	return eTEXTIR_NOTENABLED_LOM;
} 


Four LOM_Text_FetchContent(
	LOM_Handle *handle, 
	Four ocnOrScanId,		/* open scan number */
	Boolean useScanFlag,
	OID *oid,		/* oid */
	Two colNo,		/* column number */
	TextColStruct *text,	/* INOUT; text */
	LOM_TextDesc *textDesc 	/* INOUT: text descriptor */
)	
{
	return eTEXTIR_NOTENABLED_LOM;
} 


Four LOM_Text_UpdateContent(
	LOM_Handle *handle, 
	Four ocnOrScanId,		/* open scan number */
	Boolean useScanFlag,
	OID *oid,
	Two colNo,
	TextColStruct *text,	/* IN: text column struct */
	LOM_TextDesc *textDesc 	/* INOUT: text descriptor */
)	
{
	return eTEXTIR_NOTENABLED_LOM;
} 


Four LOM_Text_OpenIndexScan(
	LOM_Handle*		handle,				/* IN: lom system handle */
	Four			ocn,				/* IN: open class number */
	LOM_IndexID*	indexId,			/* IN: index id */
	Four			keywordKind,		/* IN: kind of keyword */
	BoundCond*		keywordStartBound,	/* IN: start bound */
	BoundCond*		keywordStopBound,	/* IN: stop bound */
	LockParameter*	lockup				/* IN: lock parameter */
)
{
	return eTEXTIR_NOTENABLED_LOM;
} 

Four LOM_Text_OpenIndexScan_GivenInvertedEntryTupleID(
	LOM_Handle*		handle,						/* IN: lom system handle */
	Four			ocn,						/* IN: open class number */
	Two				colNo,						/* IN: colNo */
	TupleID*		invertedTableEntryTid,	/* IN: inverted index entry tupleID for given keyword */
	LockParameter*	lockup						/* IN: lock parameter */
)
{
	return eTEXTIR_NOTENABLED_LOM;
} 


Four LOM_Text_Scan_Open(
	LOM_Handle *handle, 
	Four ocn,	/* IN: open class number */
	OID *oid,	/* IN: oid */
	Two colNo,	/* IN: column number */
	Four keywordKind,	/* IN: kind of keyword */
	BoundCond *keywordStartBound, /* IN: start bound */
	BoundCond *keywordStopBound,	/* IN: stop bound */
	LockParameter *lockup		/* IN: lock parameter */
)
{
	return eTEXTIR_NOTENABLED_LOM;
} 


Four LOM_Text_Scan_Close(
	LOM_Handle *handle, 
	Four osn
)
{
	return eTEXTIR_NOTENABLED_LOM;
} 


Four LOM_Text_Scan_NextPosting(
	LOM_Handle *handle, 
	Four textScan,		/* open scan number */
	Four bufferLength,
	char *postingBuffer,
	Four *requiredSize,
	PostingWeight *weight
)
{
	return eTEXTIR_NOTENABLED_LOM;
} 


Four LOM_Text_GetNPostingsOfCurrentKeyword(
	LOM_Handle *handle, 
	Four textScan,
	Four *nPostings
)
{
	return eTEXTIR_NOTENABLED_LOM;
} 


Four LOM_Text_NextPostings(
	LOM_Handle*	handle,						/* IN: LOM system handle */
	Four		textScan,					/* IN: text scan Id */
	Four		postingLengthBufferSize,	/* IN: size of buffer to hold postings' length */
	char*		postingLengthBuffer,		/* IN: buffer to hold postings' length */
	Four		postingBufferSize,			/* IN: size of buffer to hold postings */
	char*		postingBuffer,				/* IN: buffer to hold postings */
	Four		scanDirection,				/* IN: scan direction to read postings */
	Four		logicalIdHints,				/* IN: logical Id hints to skip postings */
	Four*		nReturnedPosting,			/* OUT: number of read postings */
	Four*		requiredSize				/* OUT: sufficient size of buffer to hold postings */
)
{
	return eTEXTIR_NOTENABLED_LOM;
} 

Four LOM_Text_GetCursorKeyword(
	LOM_Handle *handle, 
	Four textScan,
	char *keyword
)
{
	return eTEXTIR_NOTENABLED_LOM;
} 


Four LOM_Text_BatchInvertedIndexBuild(
	LOM_Handle *handle, 
	Four volId,
	Four temporaryVolId,
	char *className
)
{
	return eTEXTIR_NOTENABLED_LOM;
} 


Four LOM_Text_GetIndexID(
	LOM_Handle *handle, 
	Four ocn,		/* IN: open class number */
	Four colNo,		/* IN: column number */
	LOM_IndexID *iid	/* OUT: index id */
)
{
	return eTEXTIR_NOTENABLED_LOM;
} 

Four LOM_Text_GetNPostings(
	LOM_Handle *handle, 
	Four ocn,	/* IN: open class number */
	LOM_IndexID *indexId,	/* IN: index id */
	Four keywordKind,	/* IN: kind of keyword */
	BoundCond *keywordStartBound, /* IN: start bound */
	BoundCond *keywordStopBound,	/* IN: stop bound */
	LockParameter *lockup,		/* IN: lock parameter */
	Four *nPostings
)
{
	return eTEXTIR_NOTENABLED_LOM;
} 


Four LOM_Text_Keyword_Scan_Open(
	LOM_Handle*			handle,			/* IN  lom system handle */
	Four				ocn,			/* IN  open class number of to be scanned */
	Two					colNo,			/* IN  column of of to be scanned */
	char*				keyword			/* IN  a string ended with * which represents keyword range to be scanned (example - KOREA*) */
)
{
	return eTEXTIR_NOTENABLED_LOM;
} 


Four LOM_Text_Keyword_Scan_Open_WithBoundCondition(
	LOM_Handle*			handle,			/* IN  lom system handle */
	Four				ocn,			/* IN  open class number of to be scanned */
	Two					colNo,			/* IN  column of of to be scanned */
	Four				keywordKind,	/* IN  kind of keyword, KEYWORD or REVERSEKEYWORD */
	Two					startBoundLen,	/* IN  start bound condition length */
	char*				startBoundVal,	/* IN  start bound condition value */
	Two					stopBoundLen,	/* IN  stop bound condition length */
	char*				stopBoundVal	/* IN  stop bound condition value */
)
{
	return eTEXTIR_NOTENABLED_LOM;
} 


Four LOM_Text_Keyword_Scan_Close(
	LOM_Handle*			handle,			/* IN  lom system handle */
	Four				scanId			/* IN  scan id to be closed */
)
{
	return eTEXTIR_NOTENABLED_LOM;
} 


Four LOM_Text_Keyword_Scan_Next(
	LOM_Handle*			handle,			/* IN  lom system handle */
	Four				scanId,			/* IN  scan id */
	Four				keywordKind,	/* IN  keyword kind KEYWORD or REVERSEKEYWORD */
	char*				keyword,		/* OUT keyword buffer to be returned */
	Four*               nDocuments,		/* OUT number of document associated with the keyword */
	TupleID*			invertedIndexEntryTupleID /* OUT tuple id of inverted index entry */
)
{
	return eTEXTIR_NOTENABLED_LOM;
} 


Four LOM_Text_GetNumOfTextObjectsInClass(
	LOM_Handle*			handle,			/* IN  lom system handle */
	Four				ocn,			/* IN  open class number */
	Four*				nObjects		/* OUT number of objects in class */
)
{
	return eTEXTIR_NOTENABLED_LOM;
} 

#endif /* SLIMDOWN_TEXTIR */
