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
#include "LOM.h"
#include "Catalog.h"
#include "Catalog_Internal.h"

Four lom_ConstructEmbeddedAttrTranslationInfo(
	LOM_Handle	*handle,
	Four		textScanId
)
{
	Four scanId;
	Four orn;
	lrds_RelTableEntry *relTableEntry;
	Four volId;
	ClassID classId;
	Four v;
	Four classEntryIndex;
	Four indexInfoTblIndex;
	catalog_SysClassesOverlay *classInfo;
	catalog_SysIndexesOverlay *indexInfo;
	catalog_SysAttributesOverlay *attrInfo;
	Boolean isContainingSentenceAndWordNum;
	Boolean isContainingTupleID;
	Boolean isContainingByteOffset;
	Two nEmbeddedAttributes;
	Two *embeddedAttrVarColNo;
	Two *embeddedAttrNo;
	Two *embeddedAttrOffset;
	Two systemLevelColNo;
	Four nVarCols;
	unsigned char *nullVector;
	Four i,j;
	Four firstVarColOffset = 0;
	Two start;
	Two length;
	Four bufIndex;
	Four e;
	Four numOfReturnedIndex;
	LOM_EmbeddedAttrTranslationInfo* embeddedAttrTranslationInfo;

	scanId = LOM_SCANTABLE(handle)[textScanId].lrdsScanId;
	if (!LRDS_VALID_SCANID(LOM_GET_LRDS_HANDLE(handle), scanId)) LOM_ERROR(handle, eBADPARAMETER_LOM);

	orn = LRDS_SCANTABLE(LOM_GET_LRDS_HANDLE(handle))[scanId].orn;

	if(LOM_SCANTABLE(handle)[textScanId].scanType == LOM_INVERTEDFILE_OIDBASED_SCAN)
		systemLevelColNo = LOM_SCANTABLE(handle)[textScanId].textScanInfo.oidBasedScan.colNo;
	else if(LOM_SCANTABLE(handle)[textScanId].scanType == LOM_INVERTEDFILE_KEYWORDBASED_SCAN)
		systemLevelColNo = LOM_SCANTABLE(handle)[textScanId].textScanInfo.keywordBasedScan.colNo;
	else LOM_ERROR(handle, eBADPARAMETER_LOM);

	embeddedAttrTranslationInfo = &LOM_SCANTABLE(handle)[textScanId].embeddedAttrTranslationInfo;

	relTableEntry = LRDS_GET_RELTABLE_ENTRY(LOM_GET_LRDS_HANDLE(handle), orn);

	volId = relTableEntry->ri.fid.volNo;
	classId = LOM_USEROPENCLASSTABLE(handle)[orn].classID;

	v = Catalog_GetVolIndex(handle, volId);

	if((e = Catalog_GetClassInfo(handle, volId, classId, &classEntryIndex)) < eNOERROR)
		LOM_ERROR(handle, e);

	classInfo = &CATALOG_GET_CLASSINFOTBL(handle, v)[classEntryIndex];
	attrInfo = &CATALOG_GET_ATTRINFOTBL(handle, v)[CATALOG_GET_ATTRINFOTBL_INDEX(classInfo)];

	numOfReturnedIndex = 1;
	if(e = Catalog_GetIndexInfoByAttrNum(handle, volId, classId, GET_USERLEVEL_COLNO(systemLevelColNo), &numOfReturnedIndex, &indexInfoTblIndex) < eNOERROR)
		LOM_ERROR(handle, e);

	indexInfo = &(CATALOG_GET_INDEXINFOTBL(handle, v)[indexInfoTblIndex]);

	nEmbeddedAttributes = indexInfo->kdesc.invertedIndex.postingInfo.nEmbeddedAttributes;
	embeddedAttrNo = indexInfo->kdesc.invertedIndex.postingInfo.embeddedAttrNo;
	embeddedAttrOffset = indexInfo->kdesc.invertedIndex.postingInfo.embeddedAttrOffset;
	embeddedAttrVarColNo = indexInfo->kdesc.invertedIndex.postingInfo.embeddedAttrVarColNo;

	nVarCols = 0;
	for(i = 0; i < nEmbeddedAttributes; i++) 
	{
		if(CATALOG_GET_ATTRTYPE(&attrInfo[embeddedAttrNo[i]]) != LOM_VARSTRING) 
		{
			firstVarColOffset += CATALOG_GET_ATTRLENGTH(&attrInfo[embeddedAttrNo[i]]);
		}
		else nVarCols++;

	}
	for(i = 0; i < nEmbeddedAttributes; i++) 
	{
		embeddedAttrTranslationInfo->embeddedAttrInfo[i].type   = CATALOG_GET_ATTRTYPE(&attrInfo[embeddedAttrNo[i]]);
		embeddedAttrTranslationInfo->embeddedAttrInfo[i].length = CATALOG_GET_ATTRLENGTH(&attrInfo[embeddedAttrNo[i]]);

		if(CATALOG_GET_ATTRTYPE(&attrInfo[embeddedAttrNo[i]]) != LOM_VARSTRING) 
		{
			embeddedAttrTranslationInfo->embeddedAttrInfo[i].start = embeddedAttrOffset[i];
		}
		else
		{
			if(embeddedAttrVarColNo[i] == 0)
				embeddedAttrTranslationInfo->embeddedAttrInfo[i].start = firstVarColOffset;
			else
				embeddedAttrTranslationInfo->embeddedAttrInfo[i].start = -1;
		}
	}
	for(i = 0; i < classInfo->nCols; i++) 
	{
		for(j = 0; j < nEmbeddedAttributes; j++) 
		{
			if(GET_SYSTEMLEVEL_COLNO(i) == embeddedAttrNo[j]) 
			{
				embeddedAttrTranslationInfo->realColNoToEmbeddedColNo[i] = j;
				break;
			}
		}
	}

	bufIndex = (nVarCols)*sizeof(Two) + (nEmbeddedAttributes + 7)/8;

	embeddedAttrTranslationInfo->nEmbeddedAttributes	 = nEmbeddedAttributes;
	embeddedAttrTranslationInfo->embeddedAttributeOffset = bufIndex;
	embeddedAttrTranslationInfo->nVarCols				 = nVarCols;
	memcpy(embeddedAttrTranslationInfo->embeddedAttrVarColNo, embeddedAttrVarColNo, sizeof(embeddedAttrTranslationInfo->embeddedAttrVarColNo));

	return eNOERROR;
}

Four LOM_GetEmbeddedAttrTranslationInfo(
	LOM_Handle*							handle,
	Four								textScanId,
	LOM_EmbeddedAttrTranslationInfo*	embeddedAttrTranslationInfo
)
{
	LOM_EmbeddedAttrTranslationInfo* srcEmbeddedAttrTranslationInfo;

	srcEmbeddedAttrTranslationInfo = &LOM_SCANTABLE(handle)[textScanId].embeddedAttrTranslationInfo;
	memcpy(embeddedAttrTranslationInfo, srcEmbeddedAttrTranslationInfo, sizeof(LOM_EmbeddedAttrTranslationInfo));

	return eNOERROR;
}

Four LOM_GetEmbeddedAttrsVal(
	LOM_Handle *handle,
	Four textScanId,
	char *ptrToEmbeddedAttrsBuf,
    Four embeddedAttrSize,
	Four nCols,
	LOM_ColListStruct *clist
)
{
	Two				*embeddedAttrVarColNo;
	unsigned char	*nullVector;
	Four			i,j;
	Two				start;
	Two				length;
	Four			e;
	LOM_EmbeddedAttrTranslationInfo* embeddedAttrTranslationInfo;
	Four			embeddedAttributeOffset;
	Two*			realColNoToEmbeddedColNo;
	Four			embeddedColNo;
	LOM_EmbeddedAttrInfo* embeddedAttrInfo;
	Two				nextOffset;
	Four			colNo;
    Four            nEmbeddedAttributes;

    if(embeddedAttrSize < 0) LOM_ERROR(handle, eBADPARAMETER_LOM);

	embeddedAttrTranslationInfo = &LOM_SCANTABLE(handle)[textScanId].embeddedAttrTranslationInfo;

	embeddedAttrVarColNo	 = embeddedAttrTranslationInfo->embeddedAttrVarColNo;
	embeddedAttributeOffset  = embeddedAttrTranslationInfo->embeddedAttributeOffset;
	realColNoToEmbeddedColNo = embeddedAttrTranslationInfo->realColNoToEmbeddedColNo;
	embeddedAttrInfo		 = embeddedAttrTranslationInfo->embeddedAttrInfo;
    nEmbeddedAttributes      = embeddedAttrTranslationInfo->nEmbeddedAttributes;
	nullVector				 = (unsigned char *)&(ptrToEmbeddedAttrsBuf[(embeddedAttrTranslationInfo->nVarCols)*sizeof(Two)]);

	for(i = 0; i < nCols; i++) 
	{
		colNo = clist[i].colNo;
		if(BITTEST(nullVector, realColNoToEmbeddedColNo[colNo])) 
		{
			clist[i].nullFlag = SM_TRUE;
			clist[i].retLength = NULL_LENGTH;
		}
		else 
        {
			clist[i].nullFlag = SM_FALSE;

			embeddedColNo = realColNoToEmbeddedColNo[colNo];
			if(embeddedColNo == -1) return eBADPARAMETER_LOM;

			if(embeddedAttrInfo[embeddedColNo].type != LOM_VARSTRING && embeddedAttrInfo[embeddedColNo].type != LOM_STRING)
			{
				start  = embeddedAttrInfo[embeddedColNo].start;
				length = embeddedAttrInfo[embeddedColNo].length;

				memcpy(&(clist[i].data.s), &ptrToEmbeddedAttrsBuf[embeddedAttributeOffset + start], length);
				clist[i].retLength = length;
			}
			else if(embeddedAttrInfo[embeddedColNo].type == LOM_STRING)
			{
				start  = embeddedAttrInfo[embeddedColNo].start;
				length = embeddedAttrInfo[embeddedColNo].length;

				memcpy(clist[i].data.ptr, &ptrToEmbeddedAttrsBuf[embeddedAttributeOffset + start], length);
				clist[i].retLength = length;
			}
			else /* type is LOM_VARSTRING */
			{
				start  = embeddedAttrInfo[embeddedColNo].start;
				if(start == -1)
					memcpy(&start, &(ptrToEmbeddedAttrsBuf[sizeof(Two) * embeddedAttrVarColNo[embeddedColNo]]), sizeof(Two));
                if(embeddedColNo + 1 == nEmbeddedAttributes)   
                    nextOffset = embeddedAttrSize - embeddedAttributeOffset;
                else
				    memcpy(&nextOffset, &(ptrToEmbeddedAttrsBuf[sizeof(Two) * embeddedAttrVarColNo[embeddedColNo + 1]]), sizeof(Two));
				length = nextOffset - start;

				memcpy(clist[i].data.ptr, &ptrToEmbeddedAttrsBuf[embeddedAttributeOffset + start], length);
				clist[i].retLength = length;
			}
		}
	}

	return eNOERROR;
}

Four lom_EncodeEmbeddedAttrs(
	Four nEmbeddedAttributes, 		/* IN: the number of embedded attributes */
	catalog_SysAttributesOverlay *attrInfo, /* IN: pointer to attribute-info. catalog */
	Four bufLength,				/* IN: lenghth for buffer for encoded embedded attributes */
	char *bufForEncodedEmbeddedAttrs,	/* OUT: buffer for encoded embedded attribute */
	LOM_ColListStruct *clist, 		/* IN: column list struct which has embedded attributes values */
	Four *actualLengthForEncodedEmbeddedAttrs	/* OUT: actual length of encodedEmbedded attributes */
);

Four lom_MakeColListStruct(
    LOM_Handle *handle,
	Four nAttrs,
	Two *attrNos,
	catalog_SysAttributesOverlay *attrInfo,
	LOM_ColListStruct *clist,               /* OUT: collist struct */
	Four bufferLength,
	char *bufferForStrOrVarStr
);

/*
 * Function: lom_Text_UpdateEmbeddedAttrs(handle, Four, OID *, Two)
 *
 * Description:
 *  Update embedded attributes
 *
 * Retuns:
 *  error code
 */
Four lom_Text_UpdateEmbeddedAttrs(	
	LOM_Handle *handle, 
	Four ocnOrScanId,		/* IN: ocn or scanId */
	Boolean useScanFlag,		/* IN: flag */
	OID *docOid,			/* IN: oid */
	Four logicalDocId,		/* IN: document id */
	Two colNo 			/* IN: column number */
)
{
	Four ornForInvertedIndexTable;	/* open relation number for inverted index table */
	IndexID iid;
	IndexID docId_iid;
	Four e;
	TupleID tid;
	TupleID PostingTid;
	Four count;
	lom_PostingBuffer postingBuffer;
	Four currentOffset;	/* current offset starting from position-list stored on disk */
	Boolean deletePosting;
	char *ptrToPostingBuffer;
	Four offsetInBuffer;
	Four nPostings;
	Four e2;
	LRDS_Cursor *cursor;
	char keyword[LOM_MAXKEYWORDSIZE];		/* temporary variable */
	char docIdPlusKeyword[sizeof(TupleID) + LOM_MAXKEYWORDSIZE];
	Two length;	
	LockParameter lockup;
	BoundCond bound;
	Two keyLen;
	ColListStruct postingClist[1];
	TupleID postingTid;
	Four nPositions;
	Four ornForDocIdIndexTable;	
	Four lrdsScanNumForDocIdIndexTable;
	ColListStruct clistForDocIdIndex[1];
	Four pointerBufferIdx;
	Four currentIdxInDocIdIndexEntry;
	TupleID pointerBuffer[INIT_NUMOF_DOCID_POINTER];
	Four numOfPointers;
	ColLengthInfoListStruct lengthInfoStruct;
	TupleID tidForDocIndexTable;
#ifdef SUBINDEX
	KeyValue kval;
#endif
	char* encodingBuffer;
	char  postingHeader[256];
	Four v;
	Four indexInfoTblIndex;
	catalog_SysClassesOverlay *classInfo;
	catalog_SysIndexesOverlay *indexInfo;
	catalog_SysAttributesOverlay *attrInfo;
	Boolean isContainingSentenceAndWordNum;
	Boolean isContainingTupleID;
	Boolean isContainingByteOffset;
	Two nEmbeddedAttributes;
	Two *embeddedAttrNo;
	LOM_ColListStruct clist[LOM_MAXNUMOFATTRIBUTE];
	char encodedStr[LOM_MAXVARSTRINGSIZE];
	Four offset;
	Four byteOffset;
	Four volId;
	Four orn;
	Four scanId;
	Four offsetForEmbeddedAttrs;
	Four tupleIDFieldLength;
	lrds_RelTableEntry *relTableEntry;
	Four newPostingLength;
	Four postingLength;
	ClassID classId;
	Four classEntryIndex;
	Four numOfReturnedIndex;
	Four encodedAttrLength;
	Four prevEncodedAttrLength;
	LOM_ColListStruct clistForEmbeddedAttrs[LOM_MAXNUMOFATTRIBUTE];
	Four i;
#ifdef COMPRESSION
    char		*uncompressedData = NULL;
    Four		uncompressedDataLength;
	VolNo		volNoOfPostingTupleID;
	TupleID		*pointerList = NULL;
	char		*tmpPointerList = NULL;
    Four		inIndex, outIndex;
    VolNo		volNo;
#endif

	if(useScanFlag) {
		scanId = LOM_SCANTABLE(handle)[ocnOrScanId].lrdsScanId;

		if (!LRDS_VALID_SCANID(LOM_GET_LRDS_HANDLE(handle), scanId)) LOM_ERROR(handle, eBADPARAMETER_LOM);

		orn = LRDS_SCANTABLE(LOM_GET_LRDS_HANDLE(handle))[scanId].orn;
	}
	else orn = ocnOrScanId;

	relTableEntry = LRDS_GET_RELTABLE_ENTRY(LOM_GET_LRDS_HANDLE(handle), orn);

	volId = relTableEntry->ri.fid.volNo;
	classId = LOM_USEROPENCLASSTABLE(handle)[orn].classID;

	v = Catalog_GetVolIndex(handle, volId);

	if((e = Catalog_GetClassInfo(handle, volId, classId, &classEntryIndex)) < eNOERROR)
		LOM_ERROR(handle, e);

	classInfo = &CATALOG_GET_CLASSINFOTBL(handle, v)[classEntryIndex];
	attrInfo = &CATALOG_GET_ATTRINFOTBL(handle, v)[CATALOG_GET_ATTRINFOTBL_INDEX(classInfo)];

	/* copy to local variables */
	numOfReturnedIndex = 1;
	if(e = Catalog_GetIndexInfoByAttrNum(handle, volId, classId, GET_USERLEVEL_COLNO(colNo), &numOfReturnedIndex, &indexInfoTblIndex) < eNOERROR)
		LOM_ERROR(handle, e);

	indexInfo = &(CATALOG_GET_INDEXINFOTBL(handle, v)[indexInfoTblIndex]);

	isContainingSentenceAndWordNum = indexInfo->kdesc.invertedIndex.postingInfo.isContainingSentenceAndWordNum;
	isContainingTupleID = indexInfo->kdesc.invertedIndex.postingInfo.isContainingTupleID;
	isContainingByteOffset = indexInfo->kdesc.invertedIndex.postingInfo.isContainingByteOffset;
	nEmbeddedAttributes = indexInfo->kdesc.invertedIndex.postingInfo.nEmbeddedAttributes;
	embeddedAttrNo = indexInfo->kdesc.invertedIndex.postingInfo.embeddedAttrNo;

	if(nEmbeddedAttributes == 0)
		return eNOERROR;

	/* open relation number for inverted index table */
	ornForInvertedIndexTable = lom_Text_GetInvertedIndexTableORN(handle, ocnOrScanId, useScanFlag, colNo);
	if(ornForInvertedIndexTable < 0) LOM_ERROR(handle, eBADPARAMETER_LOM);

	/* get index id */
	e = lom_Text_GetDocIdIndex(handle, ocnOrScanId, useScanFlag, colNo, &iid);
	if(e < 0) LOM_ERROR(handle, eBADPARAMETER_LOM);

	/* get open class number */
	ornForDocIdIndexTable = lom_Text_GetDocIdIndexTableORN(handle, ocnOrScanId, useScanFlag, colNo);

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

	/* initialize posting buffer */
	encodingBuffer = (char*)malloc(LOM_DEFAULTPOSTINGBUFFERSIZE);
	if(!encodingBuffer) LOM_ERROR(handle, eOUTOFMEMORY_LOM);
	e = LOM_AllocPostingBuffer(handle, &postingBuffer, LOM_DEFAULTPOSTINGBUFFERSIZE);
	if(e < eNOERROR) LOM_ERROR(handle, e);
	LOM_FREEOFFSET_POSTINGBUFFER(postingBuffer) = 0;

#ifndef COMPRESSION
#define DEALLOCMEM_lom_Text_UpdateEmbeddedAttrs(handle, e, e2)	\
	e2 = LOM_FreePostingBuffer(handle, &postingBuffer);			\
	if(e2 < eNOERROR) LOM_ERROR(handle, e2);					\
	free(encodingBuffer); 										\
	LOM_ERROR(handle, e);
#else
#define DEALLOCMEM_lom_Text_UpdateEmbeddedAttrs(handle, e, e2) \
	e2 = LOM_FreePostingBuffer(handle, &postingBuffer);	\
	if(e2 < eNOERROR) LOM_ERROR(handle, e2); \
	free(encodingBuffer); \
	if(pointerList != NULL) free(pointerList); 					\
	LOM_ERROR(handle, e);
#endif

	currentIdxInDocIdIndexEntry = 0;
	
	/* CONSTRUCT clist */
	/* poiner-list */
	clistForDocIdIndex[0].colNo = LOM_DOCIDTABLE_POINTERLIST_COLNO;
#ifndef COMPRESSION
	clistForDocIdIndex[0].start = currentIdxInDocIdIndexEntry * sizeof(TupleID);
	clistForDocIdIndex[0].length = sizeof(pointerBuffer);
	clistForDocIdIndex[0].dataLength = sizeof(pointerBuffer);
	clistForDocIdIndex[0].data.ptr = pointerBuffer;
#endif

	/* get tuple */
	e = LRDS_NextTuple(LOM_GET_LRDS_HANDLE(handle), lrdsScanNumForDocIdIndexTable, &tidForDocIndexTable, &cursor);
	if(e < eNOERROR) {
		DEALLOCMEM_lom_Text_UpdateEmbeddedAttrs(handle, eINTERNAL_LOM, e2);
	}

	if(e == EOS) {
		/* close scan */
		e = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), lrdsScanNumForDocIdIndexTable);
		if( e < eNOERROR) {
			DEALLOCMEM_lom_Text_UpdateEmbeddedAttrs(handle, e, e2);
		}

		/* free posting buffer */
		free(encodingBuffer);
		e = LOM_FreePostingBuffer(handle, &postingBuffer);	
		if(e < eNOERROR) LOM_ERROR(handle, e);

		return eNOERROR;
	}

	lengthInfoStruct.colNo = LOM_DOCIDTABLE_POINTERLIST_COLNO;

	e = LRDS_FetchColLength(LOM_GET_LRDS_HANDLE(handle), lrdsScanNumForDocIdIndexTable, SM_TRUE, &tidForDocIndexTable, 1, &lengthInfoStruct);
	if(e < eNOERROR) {
		DEALLOCMEM_lom_Text_UpdateEmbeddedAttrs(handle, e, e2);
	}

#ifndef COMPRESSION
	numOfPointers = lengthInfoStruct.length / sizeof(TupleID);
#else
    pointerList = (char *)malloc(lengthInfoStruct.length);
    if(pointerList == NULL) 
	{
		DEALLOCMEM_lom_Text_UpdateEmbeddedAttrs(handle, eOUTOFMEMORY_LOM, e2);
	}

    clistForDocIdIndex[0].start = ALL_VALUE;
    clistForDocIdIndex[0].length = lengthInfoStruct.length;
    clistForDocIdIndex[0].dataLength = lengthInfoStruct.length;
    clistForDocIdIndex[0].data.ptr = pointerList;
#endif

	e = LRDS_FetchTuple(LOM_GET_LRDS_HANDLE(handle), lrdsScanNumForDocIdIndexTable, SM_TRUE, &tidForDocIndexTable, 1, clistForDocIdIndex);
	if(e < eNOERROR) {
		DEALLOCMEM_lom_Text_UpdateEmbeddedAttrs(handle, e, e2);
	}

#ifdef COMPRESSION
    uncompressedDataLength = sizeof(TupleID) * INIT_NUMOF_DOCID_POINTER;

    e = lom_Text_Uncompression(handle, clistForDocIdIndex[0].data.ptr, clistForDocIdIndex[0].retLength, &uncompressedData, &uncompressedDataLength);
    if(e < eNOERROR) 
	{
    	if(uncompressedData != NULL) free(uncompressedData);
		DEALLOCMEM_lom_Text_UpdateEmbeddedAttrs(handle, e, e2);
	}

    numOfPointers = (uncompressedDataLength - sizeof(VolNo)) / (sizeof(TupleID) - sizeof(VolNo));
    pointerList = (char *)realloc(pointerList, numOfPointers * sizeof(TupleID));
    if(pointerList == NULL) 
    {
        if(uncompressedData != NULL) free(uncompressedData);
        DEALLOCMEM_lom_Text_UpdateEmbeddedAttrs(handle, eOUTOFMEMORY_LOM, e2);
    }

    memcpy(&volNo, uncompressedData, sizeof(VolNo));
    
    inIndex = sizeof(VolNo);
    outIndex = 0;
    tmpPointerList = (char*)pointerList;
    
    for(i = 0; i < numOfPointers; i++)
    {   
        memcpy(&tmpPointerList[outIndex], &uncompressedData[inIndex], sizeof(PageNo));
        inIndex += sizeof(PageNo);
        outIndex += sizeof(PageNo);

        memcpy(&tmpPointerList[outIndex], &volNo, sizeof(VolNo));
        outIndex += sizeof(VolNo);

        memcpy(&tmpPointerList[outIndex], &uncompressedData[inIndex], sizeof(SlotNo) + sizeof(Unique));
        inIndex += sizeof(SlotNo) + sizeof(Unique);
        outIndex += sizeof(SlotNo) + sizeof(Unique);
    }   

    if(uncompressedData != NULL) free(uncompressedData); 
    pointerList = (TupleID*)tmpPointerList;
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

	/* posting list */
	clist[count].colNo = LOM_INVERTEDINDEX_POSTINGLIST_COLNO;
	clist[count].start = 0;
	clist[count].length = LOM_DEFAULTPOSTINGBUFFERSIZE;
	clist[count].dataLength = LOM_DEFAULTPOSTINGBUFFERSIZE;
	clist[count].data.ptr = LOM_PTR_POSTINGBUFFER(postingBuffer);
	count++;

	pointerBufferIdx = 0;

	e = lom_MakeColListStruct(handle, nEmbeddedAttributes, embeddedAttrNo, attrInfo, clistForEmbeddedAttrs, 
		                      LOM_DEFAULTPOSTINGBUFFERSIZE, encodingBuffer);
	if( e < eNOERROR) {
		DEALLOCMEM_lom_Text_UpdateEmbeddedAttrs(handle, e, e2);
	}

	e = LOM_FetchObjectByColList(handle, ocnOrScanId, useScanFlag, docOid, nEmbeddedAttributes, clistForEmbeddedAttrs);
	if( e < eNOERROR) {
		DEALLOCMEM_lom_Text_UpdateEmbeddedAttrs(handle, e, e2);
	}
	for(i = 0; i < nEmbeddedAttributes; i++) {
		clistForEmbeddedAttrs[i].dataLength = clistForEmbeddedAttrs[i].length = clistForEmbeddedAttrs[i].retLength;
	}

	ptrToPostingBuffer = LOM_PTR_POSTINGBUFFER(postingBuffer);
	e = lom_EncodeEmbeddedAttrs(nEmbeddedAttributes, attrInfo, LOM_DEFAULTPOSTINGBUFFERSIZE, ptrToPostingBuffer, 
		                        clistForEmbeddedAttrs, &encodedAttrLength);
	if( e < eNOERROR) {
		DEALLOCMEM_lom_Text_UpdateEmbeddedAttrs(handle, e, e2);
	}

	while(currentIdxInDocIdIndexEntry < numOfPointers) 
	{
#ifndef COMPRESSION
		tid = pointerBuffer[pointerBufferIdx];
#else
		tid = pointerList[pointerBufferIdx];
#endif COMPRESSION

		currentOffset       = 0;
		clist[2].start      = 0;		
		clist[2].length     = LOM_DEFAULTPOSTINGBUFFERSIZE; 
		clist[2].dataLength = LOM_DEFAULTPOSTINGBUFFERSIZE; 

		/* fetch tuple */
#ifdef SUBINDEX
		e = LRDS_FetchTuple(LOM_GET_LRDS_HANDLE(handle), ornForInvertedIndexTable, SM_FALSE, &tid, count - 2, (ColListStruct *)&clist[1]);
#else
		e = LRDS_FetchTuple(LOM_GET_LRDS_HANDLE(handle), ornForInvertedIndexTable, SM_FALSE, &tid, count, (ColListStruct *)&clist[0]);
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
					DEALLOCMEM_lom_Text_UpdateEmbeddedAttrs(handle, e, e2);
				}
				
				pointerBufferIdx = 0;
			}
#endif 
			continue;
		}
		else if( e < eNOERROR) {
			DEALLOCMEM_lom_Text_UpdateEmbeddedAttrs(handle, e, e2);
		}

		ptrToPostingBuffer = LOM_PTR_POSTINGBUFFER(postingBuffer);
		offsetInBuffer = 0;
		nPostings = GET_VALUE_FROM_COL_LIST(clist[1], sizeof(nPostings));


		if(isContainingTupleID) tupleIDFieldLength = sizeof(TupleID);
		else tupleIDFieldLength = 0;

#ifdef SUBINDEX
		kval.len = LOM_LONG_SIZE_VAR;
		bcopy(&logicalDocId, &(kval.val[0]), LOM_LONG_SIZE_VAR);
		e = LRDS_OrderedSet_IsMember(LOM_GET_LRDS_HANDLE(handle), ornForInvertedIndexTable, SM_FALSE, &tid, LOM_INVERTEDINDEX_POSTINGLIST_COLNO,
									 &kval, sizeof(postingHeader), postingHeader, NULL);
		if( e < eNOERROR) {
			DEALLOCMEM_lom_Text_UpdateEmbeddedAttrs(handle, e, e2);
		}

		memcpy(&postingLength, &postingHeader[0], sizeof(Four));

#ifndef COMPRESSION
		if(isContainingSentenceAndWordNum || isContainingByteOffset) 
		{
			memcpy(&nPositions, &postingHeader[sizeof(Four)/* postingLengthField */ + sizeof(Four)/* logical ID */ + tupleIDFieldLength], sizeof(Four)); 
			offsetForEmbeddedAttrs = sizeof(Four) + sizeof(Four) + tupleIDFieldLength + sizeof(Four)/*nPositions*/;
			if(isContainingSentenceAndWordNum) {
				offsetForEmbeddedAttrs += nPositions*(sizeof(Four) + sizeof(Two));
			}
			if(isContainingByteOffset) {
				offsetForEmbeddedAttrs += nPositions*sizeof(Four);
			}
		}
		else {
			offsetForEmbeddedAttrs = sizeof(Four) + sizeof(Four) + tupleIDFieldLength + sizeof(Four);
		}
#else
		e = lom_Text_GetOffsetOfEmbeddedAttrs(&indexInfo->kdesc.invertedIndex.postingInfo, &postingHeader[sizeof(Four)], &offsetForEmbeddedAttrs); 
		if( e < eNOERROR) {
			DEALLOCMEM_lom_Text_UpdateEmbeddedAttrs(handle, e, e2);
		}
#endif 

		prevEncodedAttrLength = postingLength + sizeof(Four) - offsetForEmbeddedAttrs;
		e = LRDS_OrderedSet_UpdateElement(LOM_GET_LRDS_HANDLE(handle), ornForInvertedIndexTable, SM_FALSE, &tid, LOM_INVERTEDINDEX_POSTINGLIST_COLNO,
			                              &kval, offsetForEmbeddedAttrs, prevEncodedAttrLength, encodedAttrLength, 
										  ptrToPostingBuffer, NULL);
		if( e < eNOERROR) {
			DEALLOCMEM_lom_Text_UpdateEmbeddedAttrs(handle, e, e2);
		}

		/*
		if(prevEncodedAttrLength != encodedAttrLength)
		{
			postingLength += encodedAttrLength - prevEncodedAttrLength;
			e = LRDS_OrderedSet_UpdateElement(LOM_GET_LRDS_HANDLE(handle), ornForInvertedIndexTable, SM_FALSE, &tid, LOM_INVERTEDINDEX_POSTINGLIST_COLNO,
			                                  &kval, 0, sizeof(Four), sizeof(Four), 
										      &postingLength, NULL);
			if( e < eNOERROR) {
				DEALLOCMEM_lom_Text_UpdateEmbeddedAttrs(handle, e, e2);
			}
		}
		*/
#else
		while(nPostings) {
			/* skip logical id field */
			if(!bcmp(&logicalDocId, &ptrToPostingBuffer[offsetInBuffer+sizeof(Four)], LOM_LONG_SIZE_VAR)) { 
				bcopy(&ptrToPostingBuffer[offsetInBuffer], &postingLength, sizeof(Four));
				break;
			}
			/* decrement the number of postings to read */
			nPostings--; 

			if(nPostings == 0) {
				DEALLOCMEM_lom_Text_UpdateEmbeddedAttrs(handle, eINTERNAL_LOM, e2);
			}

			/* postingLength */
			bcopy(&ptrToPostingBuffer[offsetInBuffer], &postingLength, sizeof(Four));

			/* check if the very next posting is in memory buffer or not */
			offsetInBuffer += sizeof(Four) + postingLength;
			currentOffset += sizeof(Four) + postingLength;

			if( offsetInBuffer + sizeof(Four)+ tupleIDFieldLength + sizeof(Four) <= clist[2].retLength) {
				/* we have already read the very next posting */
				/* go back to while-loop */
				continue;
			}

			/* we have read all postings before currentOffset */
			clist[2].start = currentOffset;

			e = LRDS_FetchTuple(LOM_GET_LRDS_HANDLE(handle), ornForInvertedIndexTable, SM_FALSE, &tid, 1, &clist[2]);
			if( e < eNOERROR) {
				DEALLOCMEM_lom_Text_UpdateEmbeddedAttrs(handle, e, e2);
			}
			offsetInBuffer = 0;
		}
		clist[2].start = currentOffset;
		clist[2].length = sizeof(Four) + postingLength;
		e = LRDS_FetchTuple(LOM_GET_LRDS_HANDLE(handle), ornForInvertedIndexTable, SM_FALSE, &tid, 1, clist[2]);
		if( e < eNOERROR) {
			DEALLOCMEM_lom_Text_UpdateEmbeddedAttrs(handle, e, e2);
		}

		if(isContainingSentenceAndWordNum || isContainingByteOffset) {
			memcpy(&nPositions, &ptrToPostingBuffer[sizeof(Four)/* postingLengthField */ + sizeof(Four)/* logical ID */ + tupleIDFieldLength], sizeof(Four)); 
			offsetForEmbeddedAttrs = sizeof(Four) + sizeof(Four) + tupleIDFieldLength + sizeof(Four)/*nPositions*/;
			if(isContainingSentenceAndWordNum) {
				offsetForEmbeddedAttrs += nPositions*(sizeof(Four) + sizeof(Two));
			}
			if(isContainingByteOffset) {
				offsetForEmbeddedAttrs += nPositions*sizeof(Four);
			}
		}
		else {
			offsetForEmbeddedAttrs = sizeof(Four) + sizeof(Four) + tupleIDFieldLength + sizeof(Four);
		}

		if(nEmbeddedAttributes > 0) {
			postingBuf = (char *)malloc(LOM_DEFAULTPOSTINGBUFFERSIZE);
			if(postingBuf == NULL) {
				DEALLOCMEM_lom_Text_UpdateEmbeddedAttrs(handle, e, e2);
			}

			/* fill embedded attributes */
			e = lom_MakeColListStruct(handle, nEmbeddedAttributes, embeddedAttrNo, attrInfo, clistForEmbeddedAttrs, LOM_DEFAULTPOSTINGBUFFERSIZE, postingBuf);
			if( e < eNOERROR) {
				DEALLOCMEM_lom_Text_UpdateEmbeddedAttrs(handle, e, e2);
			}

			e = LOM_FetchObjectByColList(handle, ocnOrScanId, useScanFlag, docOid, nEmbeddedAttributes, clistForEmbeddedAttrs);
			if( e < eNOERROR) {
				DEALLOCMEM_lom_Text_UpdateEmbeddedAttrs(handle, e, e2);
			}

			for(i = 0; i < nEmbeddedAttributes; i++) {
				clistForEmbeddedAttrs[i].dataLength = clistForEmbeddedAttrs[i].length = clistForEmbeddedAttrs[i].retLength;
			}

			e = lom_EncodeEmbeddedAttrs(
				nEmbeddedAttributes, 
				attrInfo, 
				LOM_DEFAULTPOSTINGBUFFERSIZE, 
				&ptrToPostingBuffer[offsetForEmbeddedAttrs], 
				clistForEmbeddedAttrs, 
				&encodedAttrLength);
			if( e < eNOERROR) {
				DEALLOCMEM_lom_Text_UpdateEmbeddedAttrs(handle, e, e2);
			}

			free(postingBuf);
		}

		newPostingLength = offsetForEmbeddedAttrs - sizeof(Four) + encodedAttrLength;
		memcpy(&ptrToPostingBuffer[0], &newPostingLength, sizeof(Four));

		/* size */
#ifndef SUPPORT_LARGE_DATABASE2
		clist[0].data.l += (newPostingLength - postingLength);
#else
		clist[0].data.ll += (newPostingLength - postingLength);
#endif
		clist[0].nullFlag = SM_FALSE;

		/* posting-list */
		clist[2].start = currentOffset;
		clist[2].length = sizeof(Four) + postingLength;
		clist[2].dataLength = sizeof(Four) + newPostingLength;
		clist[2].nullFlag = SM_FALSE;

		clist[2].data.ptr =  ptrToPostingBuffer;

		/* update posting information */
		e = LRDS_UpdateTuple(LOM_GET_LRDS_HANDLE(handle), ornForInvertedIndexTable, SM_FALSE, &tid, 3, (ColListStruct *)&clist[0]);
		if( e < eNOERROR) {
			DEALLOCMEM_lom_Text_UpdateEmbeddedAttrs(handle, e, e2);
		}
#endif

		currentIdxInDocIdIndexEntry++;
		pointerBufferIdx++;

#ifndef COMPRESSION
		if(pointerBufferIdx == INIT_NUMOF_DOCID_POINTER) {
			/* read next pointers */
			clistForDocIdIndex[0].start = currentIdxInDocIdIndexEntry * sizeof(TupleID);

			e = LRDS_FetchTuple(LOM_GET_LRDS_HANDLE(handle), lrdsScanNumForDocIdIndexTable, SM_TRUE, &tidForDocIndexTable, 1, clistForDocIdIndex);
			if(e < eNOERROR) {
				DEALLOCMEM_lom_Text_UpdateEmbeddedAttrs(handle, e, e2);
			}
			
			pointerBufferIdx = 0;
		}
#endif
	}

	/* close scan */
	e = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), lrdsScanNumForDocIdIndexTable);
	if( e < eNOERROR) {
		DEALLOCMEM_lom_Text_UpdateEmbeddedAttrs(handle, e, e2);
	}

	/* free posting buffer */
	free(encodingBuffer);
	e = LOM_FreePostingBuffer(handle, &postingBuffer);	
	if(e < eNOERROR) LOM_ERROR(handle, e);

#ifdef COMPRESSION
	if(pointerList != NULL) free(pointerList);
#endif
	return eNOERROR;
}

#else /* SLIMDOWN_TEXTIR */

#include "LOM_Internal.h"
#include "LOM.h"
#include "Catalog.h"
#include "Catalog_Internal.h"


Four lom_ConstructEmbeddedAttrTranslationInfo(
	LOM_Handle	*handle,
	Four		textScanId
)
{
	return eTEXTIR_NOTENABLED_LOM;
} 


Four LOM_GetEmbeddedAttrTranslationInfo(
	LOM_Handle*							handle,
	Four								textScanId,
	LOM_EmbeddedAttrTranslationInfo*	embeddedAttrTranslationInfo
)
{
	return eTEXTIR_NOTENABLED_LOM;
} 

/* This function should not be slimed down */
/* This function is used in other module */
Four LOM_GetEmbeddedAttrsVal(
	LOM_Handle *handle,
	Four textScanId,
	char *ptrToEmbeddedAttrsBuf,
    Four embeddedAttrSize,
	Four nCols,
	LOM_ColListStruct *clist
)
{
	Two				*embeddedAttrVarColNo;
	unsigned char	*nullVector;
	Four			i,j;
	Two				start;
	Two				length;
	Four			e;
	LOM_EmbeddedAttrTranslationInfo* embeddedAttrTranslationInfo;
	Four			embeddedAttributeOffset;
	Two*			realColNoToEmbeddedColNo;
	Four			embeddedColNo;
	LOM_EmbeddedAttrInfo* embeddedAttrInfo;
	Two				nextOffset;
	Four			colNo;
    Four            nEmbeddedAttributes;

    if(embeddedAttrSize < 0) LOM_ERROR(handle, eBADPARAMETER_LOM);

	embeddedAttrTranslationInfo = &LOM_SCANTABLE(handle)[textScanId].embeddedAttrTranslationInfo;

	embeddedAttrVarColNo	 = embeddedAttrTranslationInfo->embeddedAttrVarColNo;
	embeddedAttributeOffset  = embeddedAttrTranslationInfo->embeddedAttributeOffset;
	realColNoToEmbeddedColNo = embeddedAttrTranslationInfo->realColNoToEmbeddedColNo;
	embeddedAttrInfo		 = embeddedAttrTranslationInfo->embeddedAttrInfo;
    nEmbeddedAttributes      = embeddedAttrTranslationInfo->nEmbeddedAttributes;
	nullVector				 = (unsigned char *)&(ptrToEmbeddedAttrsBuf[(embeddedAttrTranslationInfo->nVarCols)*sizeof(Two)]);

	for(i = 0; i < nCols; i++) 
	{
		colNo = clist[i].colNo;
		if(BITTEST(nullVector, realColNoToEmbeddedColNo[colNo])) 
		{
			clist[i].nullFlag = SM_TRUE;
			clist[i].retLength = NULL_LENGTH;
		}
		else 
        {
			clist[i].nullFlag = SM_FALSE;

			embeddedColNo = realColNoToEmbeddedColNo[colNo];
			if(embeddedColNo == -1) return eBADPARAMETER_LOM;

			if(embeddedAttrInfo[embeddedColNo].type != LOM_VARSTRING && embeddedAttrInfo[embeddedColNo].type != LOM_STRING)
			{
				start  = embeddedAttrInfo[embeddedColNo].start;
				length = embeddedAttrInfo[embeddedColNo].length;

				memcpy(&(clist[i].data.s), &ptrToEmbeddedAttrsBuf[embeddedAttributeOffset + start], length);
				clist[i].retLength = length;
			}
			else if(embeddedAttrInfo[embeddedColNo].type == LOM_STRING)
			{
				start  = embeddedAttrInfo[embeddedColNo].start;
				length = embeddedAttrInfo[embeddedColNo].length;

				memcpy(clist[i].data.ptr, &ptrToEmbeddedAttrsBuf[embeddedAttributeOffset + start], length);
				clist[i].retLength = length;
			}
			else /* type is LOM_VARSTRING */
			{
				start  = embeddedAttrInfo[embeddedColNo].start;
				if(start == -1)
					memcpy(&start, &(ptrToEmbeddedAttrsBuf[sizeof(Two) * embeddedAttrVarColNo[embeddedColNo]]), sizeof(Two));
                if(embeddedColNo + 1 == nEmbeddedAttributes)   
                    nextOffset = embeddedAttrSize - embeddedAttributeOffset;
                else
				    memcpy(&nextOffset, &(ptrToEmbeddedAttrsBuf[sizeof(Two) * embeddedAttrVarColNo[embeddedColNo + 1]]), sizeof(Two));
				length = nextOffset - start;

				memcpy(clist[i].data.ptr, &ptrToEmbeddedAttrsBuf[embeddedAttributeOffset + start], length);
				clist[i].retLength = length;
			}
		}
	}

	return eNOERROR;
}


Four lom_Text_UpdateEmbeddedAttrs(	
	LOM_Handle *handle, 
	Four ocnOrScanId,		/* IN: ocn or scanId */
	Boolean useScanFlag,		/* IN: flag */
	OID *docOid,			/* IN: oid */
	Four logicalDocId,		/* IN: document id */
	Two colNo 			/* IN: column number */
)
{
	return eTEXTIR_NOTENABLED_LOM;
} 

#endif /* SLIMDOWN_TEXTIR */
