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

#include "OOSQL_StorageSystemHeaders.h"
#include "OOSQL_APIs_Internal.hxx"
#include "OOSQL_Error.h"
#include "OOSQL_ServerQuery.hxx"

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
scan identifier
eBADPARAMETER_OOSQL 
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OOSQL_Text_KeywordInfoScan_Open(
	OOSQL_SystemHandle* systemHandle,	// IN 
	Four				volID,			// IN  
	char*				className,		// IN  
	char*				columnName,		// IN  
	char*				keyword			// IN 
)
{
	Four			e;
	OOSQL_Handle	handle;
	char			queryString[4096];

	e = OOSQL_AllocHandle(systemHandle, volID, &handle); 
	if(e < 0) OOSQL_ERROR(systemHandle, e);

	sprintf(queryString, "select keyword, nPostings from _%s_%s_Inverted where keyword like '%s'", className, columnName, keyword);

	e = OOSQL_Prepare(systemHandle, handle, queryString, NULL);
	if(e < 0) OOSQL_ERROR(systemHandle, e);

	e = OOSQL_Execute(systemHandle, handle);
	if(e < 0) OOSQL_ERROR(systemHandle, e);

	return handle;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eBADPARAMETER_OOSQL 
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OOSQL_Text_KeywordInfoScan_Close(
	OOSQL_SystemHandle* systemHandle,	// IN  
	Four				scanId			// IN  
)
{
	Four e;

	if(!OOSQL_CHECKGDSINSTTABLE(systemHandle))
		return eBADPARAMETER_OOSQL;

	e = OOSQL_FreeHandle(systemHandle, scanId);
	if(e < 0) OOSQL_ERROR(systemHandle, e);

	return eNOERROR;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eBADPARAMETER_OOSQL 
< eNOERROR         

IMPLEMENTATION:
****************************************************************************/
Four OOSQL_Text_KeywordInfoScan_Next(
	OOSQL_SystemHandle* systemHandle,	// IN  
	Four				scanId,			// IN  
	char*				keyword,		// OUT 
	Four*				nDocuments,		// OUT 
	Four*				nPositions		// OUT 
)
{
	Four	e;
	Four	resultLength;

	e = OOSQL_Next(systemHandle, scanId);
	if(e < 0) OOSQL_ERROR(systemHandle, e);
	if(e == ENDOFEVAL)
		return ENDOFEVAL;

	e = OOSQL_GetData(systemHandle, scanId, 0, 0, keyword, MAXKEYWORDSIZE, &resultLength);
	if(e < 0) OOSQL_ERROR(systemHandle, e);

	if(resultLength < 0)
		strcpy(keyword, "");
	else
		keyword[resultLength] = '\0';

	e = OOSQL_GetData(systemHandle, scanId, 1, 0, nDocuments, sizeof(nDocuments), &resultLength);
	if(e < 0) OOSQL_ERROR(systemHandle, e);

	if(resultLength < 0)
		*nDocuments = 0;

	*nPositions = 0;

    return eNOERROR;
}

Four OOSQL_Text_KeywordInfoScanForDocument_Open(
	OOSQL_SystemHandle* systemHandle,	// IN  
	Four				volID,			// IN  
	char*				className,		// IN  
	OID*				oid,			// IN  
	char*				columnName,		// IN  
	char*				keyword			// IN  
)
{
    return eNOERROR;
}

Four OOSQL_Text_KeywordInfoScanForDocument_Close(
	OOSQL_SystemHandle* systemHandle,	// IN  
	Four				scanId			// IN  
)
{
    return eNOERROR;
}


Four OOSQL_Text_KeywordInfoScanForDocument_Next(
	OOSQL_SystemHandle* systemHandle,	// IN  
	Four				scanId,			// IN  
	char*				keyword,		// OUT 
	Four*				nDocuments,		// OUT 
	Four*				nPositions		// OUT 
)
{
    return eNOERROR;
}

Four OOSQL_Text_FetchContent(
	OOSQL_SystemHandle* systemHandle,	// IN  
	Four				volId,			// IN  
	char*				className,		// IN
	Four				colNo,			// IN  
	OID*				oid,			// IN  
	Four				bufferLength,	// IN  
	void*				buffer,			// INOUT  
	Four*				returnLength	// OTU 
)
{
	LOM_Handle*		handle;
	Four			ocn;
	Four			e;
	TextColStruct	tclist[1];
	LOM_TextDesc	textDesc;

	handle = (LOM_Handle*)systemHandle;

	ocn = LOM_OpenClass(handle, volId, className);
	if(ocn < 0) OOSQL_ERROR(systemHandle, ocn);

	tclist[0].start = ALL_VALUE;
	tclist[0].dataLength = bufferLength;
	tclist[0].data = buffer;

	e = LOM_Text_GetDescriptor(handle, ocn, SM_FALSE, oid, colNo, &textDesc);
	if(e < 0) OOSQL_ERROR(systemHandle, e);

	e = LOM_Text_FetchContent(handle, ocn, SM_FALSE, oid, colNo, &tclist[0], &textDesc);
	if(e < 0) OOSQL_ERROR(systemHandle, e);

	*returnLength = tclist[0].retLength;

	e = LOM_CloseClass(handle, ocn);
	if(e < 0) OOSQL_ERROR(systemHandle, e);

	return eNOERROR;
}

Four OOSQL_Text_ConvertStatementToQueryString(
	OOSQL_SystemHandle* systemHandle,			// IN  
	Four				volId,					// IN  
	char*				className,				// IN  
	char*				columnName,				// IN  
	char*				operatorString,			// IN  
	char*				statement,				// IN  
	Four				queryBufferLength,		// IN  
	char*				queryBuffer				// OUT 
)
{
	Four								e;
	LOM_Handle*							handle;
	Four								ocn;
	Four								textColIndex;
	Four								colNo;
	Four								classId;
	Four								attrInfo;
	Four								mv;
	lom_FptrToKeywordExtractor			fptrToKeywordExtractor;
	lom_FptrToGetNextPostingInfo		fptrToGettingNextPostingInfo;
	lom_FptrToFinalizeKeywordExtraction fptrToFinalizeKeywordExtraction;
	Four								resultHandle;
	char								sentenceWordPositionBuf[LOM_MAXPOSITIONLENGTH];
	Four								nPositions;
	char								keyword[LOM_MAXKEYWORDSIZE];
	Four								i;


	handle = (LOM_Handle*)systemHandle;

	e = LOM_GetOpenClassNum(handle, volId, className);
	if(e < 0) OOSQL_ERROR(systemHandle, e);
	ocn = e;

	classId = LOM_USEROPENCLASSTABLE(handle)[ocn].classID;

    e = Catalog_GetMountTableInfo(handle, volId, &mv);
	if(e < 0) OOSQL_ERROR(systemHandle, e);

	e = Catalog_GetAttrInfoByName(handle, volId, classId, columnName, &attrInfo);
	if(e < 0) OOSQL_ERROR(systemHandle, e);
	colNo = CATALOG_GET_ATTRCOLNO(&(CATALOG_GET_ATTRINFOTBL(handle, mv)[attrInfo]));


	for(i = 0; i < LOM_USEROPENCLASSTABLE(handle)[ocn].numOfTextAttrs; i++)
		if(LOM_USEROPENCLASSTABLE(handle)[ocn].textColNo[i] == colNo)
			break;

	if(i == LOM_USEROPENCLASSTABLE(handle)[ocn].numOfTextAttrs)
	{
		OOSQL_ERR(eNOTTEXTCOLUMN_OOSQL);
	}
	else
		textColIndex = i;

	if(!(LOM_USEROPENCLASSTABLE(handle)[ocn].fptrToKeywordExtractor[textColIndex])) 
	{
		// get a function pointer to keyword extractor
		e = LOM_Text_OpenHandleForKeywordExtractor(
				handle,
				volId,
				classId,
				GET_USERLEVEL_COLNO(colNo),
				&LOM_USEROPENCLASSTABLE(handle)[ocn].handleForDLOfKeywordExtractor[textColIndex],
				&LOM_USEROPENCLASSTABLE(handle)[ocn].fptrToKeywordExtractor[textColIndex],
				&LOM_USEROPENCLASSTABLE(handle)[ocn].fptrToGettingNextPostingInfo[textColIndex],
				&LOM_USEROPENCLASSTABLE(handle)[ocn].fptrToFinalizeKeywordExtraction[textColIndex]);
		if(e < 0) OOSQL_ERROR(systemHandle, e);
	}
	
	fptrToKeywordExtractor			= LOM_USEROPENCLASSTABLE(handle)[ocn].fptrToKeywordExtractor[textColIndex];
	fptrToGettingNextPostingInfo	= LOM_USEROPENCLASSTABLE(handle)[ocn].fptrToGettingNextPostingInfo[textColIndex];
	fptrToFinalizeKeywordExtraction = LOM_USEROPENCLASSTABLE(handle)[ocn].fptrToFinalizeKeywordExtraction[textColIndex];

	e = lom_CallDll_KeywordExtractorInit(fptrToKeywordExtractor, TEXT_IN_MEMORY, NULL, 0, "", NULL, -1, statement, &resultHandle);
    if(e < 0) OOSQL_ERROR(systemHandle, e);

	strcpy(queryBuffer, "");
	while ((lom_CallDll_KeywordExtractorNext(fptrToGettingNextPostingInfo, resultHandle, keyword, &nPositions, (char*)sentenceWordPositionBuf)) != TEXT_DONE)
	{
		if(e < 0) OOSQL_ERROR(systemHandle, e);

		if(queryBuffer[0] == '\0')
		{
			if(queryBufferLength > strlen(keyword) + 3)
			{
				strcat(queryBuffer, "\"");
				strcat(queryBuffer, keyword);
				strcat(queryBuffer, "\"");

				queryBufferLength -= strlen(keyword) + 2;
			}
		}
		else
		{
			if(queryBufferLength > strlen(operatorString) + strlen(keyword) + 3)
			{
				strcat(queryBuffer, operatorString);
				strcat(queryBuffer, "\"");
				strcat(queryBuffer, keyword);
				strcat(queryBuffer, "\"");

				queryBufferLength -= strlen(operatorString) + strlen(keyword) + 3;
			}
		}
	}

	e = lom_CallDll_KeywordExtractorFinal(fptrToFinalizeKeywordExtraction, resultHandle);
	if(e < 0) OOSQL_ERROR(systemHandle, e);

	return eNOERROR;
}

static Four oosql_Text_GetDocIdEntryForDocument(
	OOSQL_SystemHandle* systemHandle,			// IN  
	Four				volId,					// IN  
	OID*				oid,					// IN  
	char*				columnName,				// IN  
	Four*				ornForDocIdEntry,		// OUT orn for reading docid entry
	TupleID*			tidForDocIdEntry		// OUT tid for docid entry
)
{
	Four	e;

	Four classId;
	
	classId = oid->classID;

	LOM_Handle*	handle;
	Four		mv;
	Four		attrInfo;
	Four		colNo;

	handle = (LOM_Handle*)systemHandle;
    e = Catalog_GetMountTableInfo(handle, volId, &mv);
	if(e < 0) OOSQL_ERROR(systemHandle, e);
	e = Catalog_GetAttrInfoByName(handle, volId, classId, columnName, &attrInfo);
	if(e < 0) OOSQL_ERROR(systemHandle, e);
	colNo = CATALOG_GET_ATTRCOLNO(&(CATALOG_GET_ATTRINFOTBL(handle, mv)[attrInfo]));

	Four ocn;
	char className[MAXCLASSNAME];

	e = LOM_GetClassName(handle, volId, classId, className);
	if(e < 0) OOSQL_ERROR(systemHandle, e);
	e = ocn = LOM_GetOpenClassNum(handle, volId, className);
	if(e < 0) OOSQL_ERROR(systemHandle, e);

	Four documentId;

	e = documentId = lom_Text_GetLogicalId(handle, ocn, SM_FALSE, oid);
	if(e < 0) OOSQL_ERROR(systemHandle, e);

	Four ornForDocIdIndexTable;

	e = ornForDocIdIndexTable = lom_Text_GetDocIdIndexTableORN(handle, ocn, SM_FALSE, colNo);
	if(e < 0) OOSQL_ERROR(systemHandle, e);

	BoundCond		bound;
	Two				keyLen;
	Four			scanId;
	LockParameter	lockup;
	IndexID			iid;
	TupleID			tid;

	bound.op = SM_EQ;
	keyLen   = LOM_LONG_SIZE_VAR;
	bound.key.len = sizeof(Two) + LOM_LONG_SIZE_VAR;

	memcpy(&(bound.key.val[0]), &keyLen, sizeof(Two));
	memcpy(&(bound.key.val[sizeof(Two)]), &documentId, keyLen);
	lockup.mode = L_IS;
	lockup.duration = L_COMMIT;
	
	e = lom_Text_GetDocIdIndex(handle, ocn, SM_FALSE, colNo, &iid);
	if(e < 0) OOSQL_ERROR(systemHandle, e);

	e = scanId = LRDS_OpenIndexScan(LOM_GET_LRDS_HANDLE(handle), ornForDocIdIndexTable, &iid, &bound, &bound, 0, NULL, &lockup);
	if(e < 0) OOSQL_ERROR(systemHandle, e);
	
	e = LRDS_NextTuple(LOM_GET_LRDS_HANDLE(handle), scanId, &tid, NULL);
	if(e < 0) OOSQL_ERROR(systemHandle, e);

	if(e == EOS)
	{
		*ornForDocIdEntry = -1;
		*tidForDocIdEntry = tid;

		e = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), scanId);
		if(e < 0) OOSQL_ERROR(systemHandle, e);

		return eNOERROR;
	}

	e = LRDS_CloseScan(LOM_GET_LRDS_HANDLE(handle), scanId);
	if(e < 0) OOSQL_ERROR(systemHandle, e);

	*ornForDocIdEntry = ornForDocIdIndexTable;
	*tidForDocIdEntry = tid;

	return eNOERROR;
}

Four OOSQL_Text_GetNumKeywordsInDocument(
	OOSQL_SystemHandle* systemHandle,			// IN  
	Four				volId,					// IN  
	OID*				oid,					// IN  
	char*				columnName,				// IN  
	Four*				numKeywords				// OUT number of keywords in the document
)
{
	Four e;
	LOM_Handle* handle;

	handle = &(OOSQL_GET_LOM_SYSTEMHANDLE(systemHandle));


	Four	ornForDocIdEntry;
	TupleID tidForDocIdEntry;

	e = oosql_Text_GetDocIdEntryForDocument(systemHandle, volId, oid, columnName, &ornForDocIdEntry, &tidForDocIdEntry);
	if(e < 0) OOSQL_ERROR(systemHandle, e);

	ColLengthInfoListStruct lengthInfo[1];

	lengthInfo[0].colNo = LOM_DOCIDTABLE_POINTERLIST_COLNO;
	e = LRDS_FetchColLength(LOM_GET_LRDS_HANDLE(handle), ornForDocIdEntry, SM_FALSE, &tidForDocIdEntry, 1, lengthInfo);
	if(e < 0) OOSQL_ERROR(systemHandle, e);

#ifdef COMPRESSION
	char* compressedData = NULL;
    char* uncompressedData = NULL;
    Four  uncompressedDataLength;
	ColListStruct	clist[1];

    compressedData = (char *)malloc(lengthInfo[0].length);
	if(compressedData == NULL) OOSQL_ERROR(systemHandle, eMEMORYALLOCERR_OOSQL);

	clist[0].colNo = LOM_DOCIDTABLE_POINTERLIST_COLNO;
	clist[0].start = ALL_VALUE;
	clist[0].length = lengthInfo[0].length;
	clist[0].dataLength = lengthInfo[0].length;
	clist[0].data.ptr = compressedData;

    e = LRDS_FetchTuple(LOM_GET_LRDS_HANDLE(handle), ornForDocIdEntry, SM_FALSE, &tidForDocIdEntry, 1, clist);
	if(e < 0) 
	{
    	if(compressedData != NULL) free(compressedData);
		OOSQL_ERROR(systemHandle, e);
	}

    uncompressedDataLength = sizeof(TupleID) * INIT_NUMOF_DOCID_POINTER;

    e = lom_Text_Uncompression(handle, compressedData, clist[0].retLength, &uncompressedData, &uncompressedDataLength);
    if(e < eNOERROR)
	{
    	if(compressedData != NULL) free(compressedData);
        if(uncompressedData != NULL) free(uncompressedData);
		OOSQL_ERROR(systemHandle, e);
	}

    if(compressedData != NULL) free(compressedData);
    if(uncompressedData != NULL) free(uncompressedData);

    *numKeywords = (uncompressedDataLength - sizeof(VolNo)) / (sizeof(TupleID) - sizeof(VolNo));
#else
	*numKeywords = lengthInfo[0].length / sizeof(TupleID);
#endif

	return eNOERROR;
}

Four OOSQL_Text_GetIthKeywordInDocument(
	OOSQL_SystemHandle* systemHandle,			// IN  
	Four				volId,					// IN  
	OID*				oid,					// IN  document oid
	char*				columnName,				// IN  document column name
	Four				ith,					// IN 
	char*				keyword					// OUT keyword buffer
)
{
	Four e;

	Four classId;
	
	classId = oid->classID;

	LOM_Handle*	handle;
	Four		mv;
	Four		attrInfo;
	Four		colNo;

	handle = (LOM_Handle*)systemHandle;
    e = Catalog_GetMountTableInfo(handle, volId, &mv);
	if(e < 0) OOSQL_ERROR(systemHandle, e);
	e = Catalog_GetAttrInfoByName(handle, volId, classId, columnName, &attrInfo);
	if(e < 0) OOSQL_ERROR(systemHandle, e);
	colNo = CATALOG_GET_ATTRCOLNO(&(CATALOG_GET_ATTRINFOTBL(handle, mv)[attrInfo]));

	Four ocn;
	char className[MAXCLASSNAME];

	e = LOM_GetClassName(handle, volId, classId, className);
	if(e < 0) OOSQL_ERROR(systemHandle, e);
	e = ocn = LOM_GetOpenClassNum(handle, volId, className);
	if(e < 0) OOSQL_ERROR(systemHandle, e);

	Four	ornForDocIdEntry;
	TupleID tidForDocIdEntry;

	e = oosql_Text_GetDocIdEntryForDocument(systemHandle, volId, oid, columnName, &ornForDocIdEntry, &tidForDocIdEntry);
	if(e < 0) OOSQL_ERROR(systemHandle, e);

	ColLengthInfoListStruct lengthInfo[1];

	lengthInfo[0].colNo = LOM_DOCIDTABLE_POINTERLIST_COLNO;
	e = LRDS_FetchColLength(LOM_GET_LRDS_HANDLE(handle), ornForDocIdEntry, SM_FALSE, &tidForDocIdEntry, 1, lengthInfo);
	if(e < 0) OOSQL_ERROR(systemHandle, e);

	Four numKeywords;
#ifdef COMPRESSION
	TupleID	*pointerBuffer = NULL;
	char	*tmpPointerBuffer = NULL;
    char	*uncompressedData = NULL;
    Four	uncompressedDataLength;
	Four	i, inIndex, outIndex;
	VolNo	volNo;
	ColListStruct	clist[1];

    pointerBuffer = (TupleID *)malloc(lengthInfo[0].length);
	if(pointerBuffer == NULL) OOSQL_ERROR(systemHandle, eMEMORYALLOCERR_OOSQL);

	clist[0].colNo = LOM_DOCIDTABLE_POINTERLIST_COLNO;
	clist[0].start = ALL_VALUE;
	clist[0].length = lengthInfo[0].length;
	clist[0].dataLength = lengthInfo[0].length;
	clist[0].data.ptr = pointerBuffer;

    e = LRDS_FetchTuple(LOM_GET_LRDS_HANDLE(handle), ornForDocIdEntry, SM_FALSE, &tidForDocIdEntry, 1, clist);
	if(e < 0) 
	{
    	if(pointerBuffer != NULL) free(pointerBuffer);
		OOSQL_ERROR(systemHandle, e);
	}

    uncompressedDataLength = sizeof(TupleID) * INIT_NUMOF_DOCID_POINTER;

    e = lom_Text_Uncompression(handle, (char*)clist[0].data.ptr, clist[0].retLength, &uncompressedData, &uncompressedDataLength);
    if(e < eNOERROR)
    {
        if(pointerBuffer != NULL) free(pointerBuffer);
        if(uncompressedData != NULL) free(uncompressedData);
		OOSQL_ERROR(systemHandle, e);
    }

    numKeywords = (uncompressedDataLength - sizeof(VolNo)) / (sizeof(TupleID) - sizeof(VolNo));
    pointerBuffer = (TupleID *)realloc(pointerBuffer, numKeywords * sizeof(TupleID));
    if(pointerBuffer == NULL)
    {   
        if(uncompressedData != NULL) free(uncompressedData);
		OOSQL_ERROR(systemHandle, eMEMORYALLOCERR_OOSQL);
    }

    memcpy(&volNo, uncompressedData, sizeof(VolNo));

    inIndex = sizeof(VolNo);
    outIndex = 0;
    tmpPointerBuffer = (char*)pointerBuffer;

    for(i = 0; i < numKeywords; i++)
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
#else
	numKeywords = lengthInfo[0].length / sizeof(TupleID);
#endif

	if(ith >= 0 && ith < numKeywords)
	{
		TupleID			tidForInvertedEntry;
#ifndef COMPRESSION
		ColListStruct	clist[1];

		clist[0].colNo = LOM_DOCIDTABLE_POINTERLIST_COLNO;
		clist[0].start = sizeof(TupleID) * ith;
		clist[0].length = sizeof(TupleID);
		clist[0].dataLength = sizeof(TupleID);
		clist[0].data.ptr = &tidForInvertedEntry;

		e = LRDS_FetchTuple(LOM_GET_LRDS_HANDLE(handle), ornForDocIdEntry, SM_FALSE, &tidForDocIdEntry, 1, clist);
		if(e < 0) OOSQL_ERROR(systemHandle, e);
#else
		memcpy(&tidForInvertedEntry, &pointerBuffer[ith], sizeof(TupleID));
#endif

		Four ornForInvertedIndexTable;

		e = ornForInvertedIndexTable = lom_Text_GetInvertedIndexTableORN(handle, ocn, SM_FALSE, colNo);
#ifndef COMPRSSION
		if(e < 0) OOSQL_ERROR(systemHandle, e);
#else
		if(e < 0)
		{
    		if(pointerBuffer != NULL) free(pointerBuffer);
			OOSQL_ERROR(systemHandle, e);
		}
#endif

		clist[0].colNo = LOM_INVERTEDINDEX_KEYWORD_COLNO;
		clist[0].start = ALL_VALUE;
		clist[0].length = ALL_VALUE;
		clist[0].dataLength = LOM_MAXKEYWORDSIZE;
		clist[0].data.ptr = keyword;

		e = LRDS_FetchTuple(LOM_GET_LRDS_HANDLE(handle), ornForInvertedIndexTable, SM_FALSE, &tidForInvertedEntry, 1, clist);
#ifndef COMPRSSION
		if(e < 0) OOSQL_ERROR(systemHandle, e);
#else
		if(e < 0)
		{
    		if(pointerBuffer != NULL) free(pointerBuffer);
			OOSQL_ERROR(systemHandle, e);
		}
#endif

		// makes keyword null terminating string
		if(clist[0].retLength >= 0 && clist[0].retLength < LOM_MAXKEYWORDSIZE)
			keyword[clist[0].retLength] = 0;
		else
			keyword[LOM_MAXKEYWORDSIZE - 1] = 0;
	}
	else
	{
		strcpy(keyword, "");
	}

#ifdef COMPRSSION
    if(pointerBuffer != NULL) free(pointerBuffer);
#endif
	return eNOERROR;

}

#else /* SLIMDOWN_TEXTIR */

#include "OOSQL_StorageSystemHeaders.h"
#include "OOSQL_APIs_Internal.hxx"
#include "OOSQL_Error.h"
#include "OOSQL_ServerQuery.hxx"


Four OOSQL_Text_KeywordInfoScan_Open(
	OOSQL_SystemHandle* systemHandle,	// IN  
	Four				volID,			// IN  
	char*				className,		// IN  
	char*				columnName,		// IN  
	char*				keyword			// IN  
)
{
	return eTEXTIR_NOTENABLED_OOSQL;
}


Four OOSQL_Text_KeywordInfoScan_Close(
	OOSQL_SystemHandle* systemHandle,	// IN  
	Four				scanId			// IN  
)
{
	return eTEXTIR_NOTENABLED_OOSQL;
}


Four OOSQL_Text_KeywordInfoScan_Next(
	OOSQL_SystemHandle* systemHandle,	// IN  
	Four				scanId,			// IN  
	char*				keyword,		// OUT 
	Four*				nDocuments,		// OUT
	Four*				nPositions		// OUT
)
{
	return eTEXTIR_NOTENABLED_OOSQL;
}


Four OOSQL_Text_KeywordInfoScanForDocument_Open(
	OOSQL_SystemHandle* systemHandle,	// IN 
	Four				volID,			// IN  
	char*				className,		// IN  
	OID*				oid,			// IN  
	char*				columnName,		// IN  
	char*				keyword			// IN  
)
{
	return eTEXTIR_NOTENABLED_OOSQL;
}


Four OOSQL_Text_KeywordInfoScanForDocument_Close(
	OOSQL_SystemHandle* systemHandle,	// IN  
	Four				scanId			// IN  
)
{
	return eTEXTIR_NOTENABLED_OOSQL;
}


Four OOSQL_Text_KeywordInfoScanForDocument_Next(
	OOSQL_SystemHandle* systemHandle,	// IN  
	Four				scanId,			// IN  
	char*				keyword,		// OUT 
	Four*				nDocuments,		// OUT
	Four*				nPositions		// OUT 
)
{
	return eTEXTIR_NOTENABLED_OOSQL;
}


Four OOSQL_Text_FetchContent(
	OOSQL_SystemHandle* systemHandle,	// IN 
	Four				volId,			// IN  
	char*				className,		// IN 
	Four				colNo,			// IN  
	OID*				oid,			// IN 
	Four				bufferLength,	// IN 
	void*				buffer,			// INOUT  
	Four*				returnLength	// OUT
)
{
	return eTEXTIR_NOTENABLED_OOSQL;
}


Four OOSQL_Text_ConvertStatementToQueryString(
	OOSQL_SystemHandle* systemHandle,			// IN 
	Four				volId,					// IN  
	char*				className,				// IN  
	char*				columnName,				// IN  
	char*				operatorString,			// IN  
	char*				statement,				// IN  
	Four				queryBufferLength,		// IN  
	char*				queryBuffer				// OUT 
)
{
	return eTEXTIR_NOTENABLED_OOSQL;
}


static Four oosql_Text_GetDocIdEntryForDocument(
	OOSQL_SystemHandle* systemHandle,			// IN  
	Four				volId,					// IN  
	OID*				oid,					// IN  
	char*				columnName,				// IN  
	Four*				ornForDocIdEntry,		// OUT orn for reading docid entry
	TupleID*			tidForDocIdEntry		// OUT tid for docid entry
)
{
	return eTEXTIR_NOTENABLED_OOSQL;
}


Four OOSQL_Text_GetNumKeywordsInDocument(
	OOSQL_SystemHandle* systemHandle,			// IN  
	Four				volId,					// IN 
	OID*				oid,					// IN  
	char*				columnName,				// IN  
	Four*				numKeywords				// OUT number of keywords in the document
)
{
	return eTEXTIR_NOTENABLED_OOSQL;
}


Four OOSQL_Text_GetIthKeywordInDocument(
	OOSQL_SystemHandle* systemHandle,			// IN  
	Four				volId,					// IN  
	OID*				oid,					// IN  
	char*				columnName,				// IN  
	Four				ith,					// IN  
	char*				keyword					// OUT keyword buffer
)
{
	return eTEXTIR_NOTENABLED_OOSQL;
}


#endif /* SLIMDOWN_TEXTIR */

