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

#ifndef WIN32
#include <unistd.h>
#endif
#include <malloc.h>
#include <string.h>
#include <stdlib.h>
#include "LOM_Internal.h"
#include "LOM.h"
#include "Catalog.h"
#include "Catalog_Internal.h"
#ifdef WIN32
#include <windows.h>
#endif

Four lom_EncodeEmbeddedAttrs(
	Four nEmbeddedAttributes, 		/* IN: the number of embedded attributes */
	catalog_SysAttributesOverlay *attrInfo, /* IN: pointer to attribute-info. catalog */
	Four bufLength,				/* IN: lenghth for buffer for encoded embedded attributes */
	char *ptrToEmbeddedAttrsBuf,	/* OUT: buffer for encoded embedded attribute */
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

char *wsfgets(
	char *lineBuffer, 
	int buffersize, 
	FILE *fd)
{
	int offset = 0;
	char newline = 10;
	int c;
	while((c = fgetc(fd)) != EOF) {
		if(c == 10) return lineBuffer;
		lineBuffer[offset] = (char)c;
		offset++;
	}
	lineBuffer[offset]=0;
	return NULL;
}

Four lom_EncodeStringVal(
	LOM_ColListStruct *clist,
	char *buf
)
{
	Four i;
	Four offset = 0;
	char *str = clist->data.ptr;
	Four strLength = clist->retLength;

	buf[offset++] = '\'';
	
	for(i = 0;i < strLength && str[i] != '\0'; i++) {
		if(str[i] == '\n') {
			buf[offset++] = '\\';
			buf[offset++] = 'n';
		}
		else if(str[i] == '\t') {
			buf[offset++] = '\\';
			buf[offset++] = 't';
		}
		else if(str[i] == '\'') {
			buf[offset++] = '\\';
			buf[offset++] = str[i];
		}
		else if(str[i] == ' ') {
			buf[offset++] = '\\';
			buf[offset++] = 's';
		}
		/* remove nonprintable 1byte character */ 
        else if(str[i] > 0x1f && str[i] < 0x7f) {
		    buf[offset++] = str[i];
        }
	}

	buf[offset++] = '\'';
	buf[offset] = '\0';

	return eNOERROR;
}

Four lom_DecodeStringVal(
	char *buf,
	LOM_ColListStruct *clist
)
{
	Four i;
	Four offset = 0;	 /* skip the first character */
	Four strLength = strlen(buf);
	char *str = clist->data.ptr;

	for(i = 1; i < strLength - 1 && i < clist->dataLength; i++) {
		if(buf[i] == '\\') {
			if(buf[i+1] == 'n') {
				str[offset++] = '\n';
				i++;
			}
			else if(buf[i+1] == 't') {
				str[offset++] = '\t';
				i++;
			}
			else if(buf[i+1] == '\'') {
				str[offset++] = '\'';
				i++;
			}
			else if(buf[i+1] == 's') {
				str[offset++] = ' ';
				i++;
			}
			else str[offset++] = buf[i];
		}
		else str[offset++] = buf[i];
	}

	for(i = offset; i < clist->dataLength; i++) 
		str[offset++] = '\0';
	
	return eNOERROR;
}

static Four IsInvalidKeyword(char* str)
{
	char* s = str;

	while(*s)
	{
		if(!(*s == ' ' || *s == '\t' || *s == '\n'))
			break;
		s ++;
	}

	if(*s == '\0')
		return 1;	

	while(*s)
	{
		if(*s == ' ' || *s == '\t' || *s == '\n')
			break;
		s ++;
	}

	if(*s == '\0')
		return 0;	
	
	while(*s)
	{
		if(!(*s == ' ' || *s == '\t' || *s == '\n'))
			break;
		s ++;
	}

	if(*s == '\0')
		return 0;
	else
		return 1;	
}

static int MakeLowercase(char *buffer)
{
	int i, j;
	int length;

	length = strlen(buffer);
	for(i = 0; i < length; i++) 
	{
		if((unsigned char)buffer[i] > (unsigned char)0x80)
			i++;
		else
			buffer[i] = tolower(buffer[i]);
	}
	
	for(i = 0; i < length; i++)
	{
		if(!isspace(buffer[i]) || (unsigned)buffer[i] > (unsigned)0x80)
			break;
	}

	if(i != 0)
	{
		for(j = 0; i < length; i++, j++)
			buffer[j] = buffer[i];
		buffer[j] = '\0';
	}

	length = strlen(buffer);
	for(i = length - 1; i >= 0; i--)
	{
		if(i > 0 && (unsigned)buffer[i - 1] > (unsigned)0x80)
			break;
		if(!isspace(buffer[i]))
			break;
	}
	
	if(i != length - 1)
		buffer[i] = '\0';	

	return eNOERROR;
}

Four lom_Text_ExtractKeywordFromContentBufferIntoPostingBuffer(
	LOM_Handle *handle, 
	lom_FptrToKeywordExtractor fptrToKeywordExtractor,	/* IN: function pointer to keyword extractor */
	lom_FptrToGetNextPostingInfo fptrToGetNextPostingInfo,
	lom_FptrToFinalizeKeywordExtraction fptrToFinalizeKeywordExtraction,
	lom_FptrToFilter fptrToFilter,	/* IN: function pointer to filter */
	Four ocnOrScanId,		/* IN: ocnOrScanId */
	Boolean useScanFlag,		/* IN: useScanFlag */
	OID  *docOid,			/* IN: document oid */
	Four docLogicalId,		/* IN: document logical id */
	Two systemLevelColNo,			/* IN: system level column number */
	Four postingBufferLength,	/* IN: length of posting buffer */
	char *postingBuffer,		/* OUT: posting buffer */
	Four *nKeywords			/* OUT: number of keywords contained in content buffer */
)
{
	char filteredFile[L_tmpnam];	/* for temporary file name */
	char keyword[LOM_MAXKEYWORDSIZE];
	int  rindex, windex;
	Four count;			/* for the number of postings */
	Four iWordPos;
	Two  wordPos;			/* for word position	*/
	Four sentencePos;		/* for sentence position */
	Four e;
	Four postingLength; 
	Four resultHandle;
	Four indexForLengthField;
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
	Four byteOffset;
	Four classEntryIndex;
	char* postingBuf;	
	Four numOfReturnedIndex;
	Four encodedAttrLength;
	Four keywordLength;

	postingBuf = malloc(LOM_DEFAULTPOSTINGBUFFERSIZE);
	if(postingBuf == NULL) LOM_ERROR(handle, eOUTOFMEMORY_LOM);

	v = Catalog_GetVolIndex(handle, docOid->volNo);
	if(v < 0) LOM_ERROR(handle, v);

	if((e = Catalog_GetClassInfo(handle, docOid->volNo, docOid->classID, &classEntryIndex)) < eNOERROR)
	{
		free(postingBuf);
		LOM_ERROR(handle, e);
	}

	classInfo = &CATALOG_GET_CLASSINFOTBL(handle, v)[classEntryIndex];
	attrInfo = &CATALOG_GET_ATTRINFOTBL(handle, v)[CATALOG_GET_ATTRINFOTBL_INDEX(classInfo)];

	/* keyword extraction */
	if(fptrToFilter) {
		tmpnam(filteredFile);

		if(lom_CallDll_Filter(fptrToFilter, TEXT_IN_DB, handle, docOid->volNo, CATALOG_GET_CLASSNAME(classInfo), docOid, (Two)GET_USERLEVEL_COLNO(systemLevelColNo), NULL, filteredFile) < eNOERROR) 
		{
			free(postingBuf);
			LOM_ERROR(handle, eCHECKFILTER_LOM);
		}

		if(lom_CallDll_KeywordExtractorInit(fptrToKeywordExtractor, TEXT_IN_FILE, handle, docOid->volNo, CATALOG_GET_CLASSNAME(classInfo), NULL, -1, filteredFile, &resultHandle) < eNOERROR) 
		{
			free(postingBuf);
			LOM_ERROR(handle, eCHECKKEYWORDEXTRACTOR_LOM);
		}
	}
	else {
		if(lom_CallDll_KeywordExtractorInit(fptrToKeywordExtractor, TEXT_IN_DB, handle, docOid->volNo, CATALOG_GET_CLASSNAME(classInfo), docOid, (Two)GET_USERLEVEL_COLNO(systemLevelColNo), "", &resultHandle) < eNOERROR) 
		{
			free(postingBuf);
			LOM_ERROR(handle, eCHECKKEYWORDEXTRACTOR_LOM);
		}
	}

	/* copy to local variables */
	numOfReturnedIndex = 1;
	if(e = Catalog_GetIndexInfoByAttrNum(handle, docOid->volNo, docOid->classID, GET_USERLEVEL_COLNO(systemLevelColNo), &numOfReturnedIndex, &indexInfoTblIndex) < eNOERROR)
	{
		free(postingBuf);
		LOM_ERROR(handle, e);
	}

	indexInfo = &(CATALOG_GET_INDEXINFOTBL(handle, v)[indexInfoTblIndex]);

	isContainingSentenceAndWordNum = indexInfo->kdesc.invertedIndex.postingInfo.isContainingSentenceAndWordNum;
	isContainingTupleID = indexInfo->kdesc.invertedIndex.postingInfo.isContainingTupleID;
	isContainingByteOffset = indexInfo->kdesc.invertedIndex.postingInfo.isContainingByteOffset;
	nEmbeddedAttributes = indexInfo->kdesc.invertedIndex.postingInfo.nEmbeddedAttributes;
	embeddedAttrNo = indexInfo->kdesc.invertedIndex.postingInfo.embeddedAttrNo;

	windex = 0;
	rindex = 0;
	*nKeywords = 0;
	count = 0;
	while ((lom_CallDll_KeywordExtractorNext(fptrToGetNextPostingInfo, resultHandle, keyword, &count, postingBuf)) != TEXT_DONE) {
		int i;
		Four startIndex;

        if(isContainingSentenceAndWordNum && count >= LOM_DEFAULTPOSTINGBUFFERSIZE / (sizeof(Four) + sizeof(Four)))
        {
            LOM_ERROR(handle, eBIGGERPOSTINGBUFFERNEEDED_LOM);
        }
        else if(isContainingByteOffset && count >= LOM_DEFAULTPOSTINGBUFFERSIZE / sizeof(Four))
        {
            LOM_ERROR(handle, eBIGGERPOSTINGBUFFERNEEDED_LOM);
        }

		/* check keyword */
		/* 만약 keyword가 white space로만 이루워진 것이라면 keyword로서 인정을 하지 않는다. */
		if(IsInvalidKeyword(keyword))
			continue;

		MakeLowercase(keyword);

		(*nKeywords) ++;

		/* copy keyword into the posting buffer */
		startIndex = windex;
		keywordLength = strlen(keyword) + 1;
		memcpy(&postingBuffer[windex], keyword, keywordLength);
		windex += strlen(keyword) + 1;

		indexForLengthField = windex;
		windex += sizeof(Four);	/* skip length field */

		/* copy logicalID into the posting buffer */
		memcpy(&postingBuffer[windex], (void *)&docLogicalId, sizeof(Four));
		windex += sizeof(Four);

		/* copy TID into the posting buffer */
		if(isContainingTupleID) {
			memcpy(&postingBuffer[windex], (void *)docOid, sizeof(TupleID));
			windex += sizeof(TupleID);
		}

		/* copy the number of postings into buffer */
		memcpy(&postingBuffer[windex], (void *)&count, sizeof(Four));
		windex += sizeof(Four);

		if(isContainingSentenceAndWordNum || isContainingByteOffset)
		{
			for (i = 0; i < count; i ++)
			{
				if(isContainingSentenceAndWordNum)
				{
					/* copy sentence position into buffer */
					memcpy(&sentencePos, &postingBuf[rindex], sizeof(Four));
					rindex += sizeof(Four);
					memcpy(&postingBuffer[windex], (void *)&sentencePos, sizeof(Four));
					windex += sizeof(Four);

					/* copy word position into buffer */
					memcpy(&iWordPos, &postingBuf[rindex], sizeof(Four));
					rindex += sizeof(Four);
					wordPos = (Two)iWordPos;
					memcpy(&postingBuffer[windex], (void *)&wordPos, sizeof(Two));
					windex += sizeof(Two);
				}

				if(isContainingByteOffset) 
				{
					memcpy(&byteOffset, &postingBuf[rindex], sizeof(Four));
					rindex += sizeof(Four);

					memcpy(&postingBuffer[windex], (void *)&byteOffset, sizeof(Four));
					windex += sizeof(Four);
				}
			}
		}

		if(nEmbeddedAttributes > 0) {
			/* fill embedded attributes */
			e = lom_MakeColListStruct(handle, nEmbeddedAttributes, embeddedAttrNo, attrInfo, clist, LOM_DEFAULTPOSTINGBUFFERSIZE, postingBuf);
			if(e< eNOERROR) 
			{
				free(postingBuf);
				LOM_ERROR(handle, e);
			}

			e = LOM_FetchObjectByColList(handle, ocnOrScanId, useScanFlag, docOid, nEmbeddedAttributes, clist);
			if(e< eNOERROR) 
			{
				free(postingBuf);
				LOM_ERROR(handle, e);
			}

			for(i = 0; i < nEmbeddedAttributes; i++) {
				clist[i].dataLength = clist[i].length = clist[i].retLength;
			}

			e = lom_EncodeEmbeddedAttrs(nEmbeddedAttributes, attrInfo, postingBufferLength - windex, &postingBuffer[windex], clist, &encodedAttrLength);
			if(e < eNOERROR) 
			{
				free(postingBuf);
				LOM_ERROR(handle, e);
			}

			windex += encodedAttrLength;
		}

		postingLength = (windex - startIndex) - keywordLength - sizeof(Four)/* lengthFieldSize */;

		/* copy the length of a posting */
		memcpy(&postingBuffer[indexForLengthField], &postingLength, sizeof(Four));
	}

	if((e = lom_CallDll_KeywordExtractorFinal(fptrToFinalizeKeywordExtraction, resultHandle)) < eNOERROR) 
	{
		free(postingBuf);
		LOM_ERROR(handle, e);
	}

	free(postingBuf);

	return eNOERROR;
}

Four lom_MakeColListStruct(
	LOM_Handle *handle,
	Four nAttrs,
	Two *attrNos,
	catalog_SysAttributesOverlay *attrInfo,
	LOM_ColListStruct *clist,		/* OUT: collist struct */
	Four bufferLength,
	char *bufferForStrOrVarStr	/* OUT: buffer for storing variable-string or fixed-string */
)
{
	Four i;
	Four offset = 0;

	for(i = 0; i < nAttrs; i++) {
		clist[i].colNo = GET_USERLEVEL_COLNO(attrNos[i]);
		clist[i].start = ALL_VALUE;

		switch(CATALOG_GET_ATTRTYPE(&(attrInfo[attrNos[i]]))) {
			case LOM_SHORT:
			case LOM_USHORT:
				clist[i].length = clist[i].dataLength = LOM_SHORT_SIZE;
				break;
			case LOM_LONG:
			case LOM_ULONG:
				clist[i].length = clist[i].dataLength = LOM_LONG_SIZE;
				break;
			case LOM_LONG_LONG:
				clist[i].length = clist[i].dataLength = LOM_LONG_LONG_SIZE;
				break;
			case LOM_INT:
				clist[i].length = clist[i].dataLength = LOM_INT_SIZE;
				break;
			case LOM_FLOAT:
				clist[i].length = clist[i].dataLength = LOM_FLOAT_SIZE;
				break;
			case LOM_DOUBLE:
				clist[i].length = clist[i].dataLength = LOM_DOUBLE_SIZE;
				break;
			case LOM_DATE:
				clist[i].length = clist[i].dataLength = LOM_DATE_SIZE;
				break;
			case LOM_TIME:
				clist[i].length = clist[i].dataLength = LOM_TIME_SIZE;
				break;
			case LOM_TIMESTAMP:
				clist[i].length = clist[i].dataLength = LOM_TIMESTAMP_SIZE;
				break;
			case LOM_INTERVAL:
				clist[i].length = clist[i].dataLength = LOM_INTERVAL_SIZE;
				break;
			case LOM_OID:
				clist[i].length = clist[i].dataLength = LOM_OID_SIZE;
				break;
			case LOM_STRING:
			case LOM_VARSTRING:
				clist[i].length = clist[i].dataLength = CATALOG_GET_ATTRLENGTH(&(attrInfo[attrNos[i]]));
				clist[i].data.ptr = &bufferForStrOrVarStr[offset];
				offset += clist[i].length;
				if(offset > bufferLength)
					LOM_ERROR(handle, eBIGGERPOSTINGBUFFERNEEDED_LOM);
				break;
			default:
				LOM_ERROR(handle, eNOTALLOWDEDTYPE_LOM);
				break;
		}
	}

	return eNOERROR;

}

Four lom_Text_ExtractKeywordFromContentObjectIntoTempFile(
	LOM_Handle *handle, 
	lom_FptrToKeywordExtractor fptrToKeywordExtractor,	/* IN: function pointer to keyword extractor */
	lom_FptrToGetNextPostingInfo fptrToGetNextPostingInfo,
	lom_FptrToFinalizeKeywordExtraction fptrToFinalizeKeywordExtraction,
	lom_FptrToFilter fptrToFilter,	/* IN: function pointer to filter */
	Four ocnOrScanId,		/* IN: open scan number for content table */
	Boolean useScanFlag,		/* IN: flag */
	TupleID *contentOid,		/* IN: object id for content object */
	OID *docOid,			/* IN: document oid */
	Four docLogicalId,		/* IN: document logical id */
	Two systemLevelColNo,			/* IN: system level column number */
	char *tempPostingFileName 	/* IN: temporary file */
)
{
	char filteredFile[L_tmpnam];	/* for temporary file name */
	char keyword[LOM_MAXKEYWORDSIZE];
	FILE *outFd;
	Four count;			/* for the number of postings */
	Two  wordPos;			/* for word position */
	Four iWordPos;
	Four sentencePos;		/* for sentence position */
	Four e;
	Four orn;
	Four scanId;
	Four resultHandle;		/* handle used for storing result posting info. */
	char* postingBuf;
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
	Four classEntryIndex;
	Four numOfReturnedIndex;
	
	postingBuf = malloc(LOM_DEFAULTPOSTINGBUFFERSIZE);
	if(postingBuf == NULL) LOM_ERROR(handle, eOUTOFMEMORY_LOM);

	if(useScanFlag) {
		scanId = LOM_SCANTABLE(handle)[ocnOrScanId].lrdsScanId;
		orn = LRDS_SCANTABLE(LOM_GET_LRDS_HANDLE(handle))[scanId].orn;
	}
	else orn = ocnOrScanId;

	v = Catalog_GetVolIndex(handle, docOid->volNo);
	if(v < 0) LOM_ERROR(handle, v);

	if((e = Catalog_GetClassInfo(handle, docOid->volNo, docOid->classID, &classEntryIndex)) < eNOERROR)
	{
		free(postingBuf);
		LOM_ERROR(handle, e);
	}

	classInfo = &CATALOG_GET_CLASSINFOTBL(handle, v)[classEntryIndex];
	attrInfo = &CATALOG_GET_ATTRINFOTBL(handle, v)[CATALOG_GET_ATTRINFOTBL_INDEX(classInfo)];

	/* extraction keyword */
	if(fptrToFilter) {
		tmpnam(filteredFile);
		if(lom_CallDll_Filter(fptrToFilter, TEXT_IN_DB, handle, docOid->volNo, CATALOG_GET_CLASSNAME(classInfo), docOid, (Two)GET_USERLEVEL_COLNO(systemLevelColNo), NULL, filteredFile) < eNOERROR) 
		{
			free(postingBuf);
			LOM_ERROR(handle, eCHECKFILTER_LOM);
		}

		if(lom_CallDll_KeywordExtractorInit(fptrToKeywordExtractor, TEXT_IN_FILE, handle, docOid->volNo, CATALOG_GET_CLASSNAME(classInfo), NULL, -1, filteredFile, &resultHandle) < eNOERROR) 
		{
			free(postingBuf);
			LOM_ERROR(handle, eCHECKKEYWORDEXTRACTOR_LOM);
		}
	}
	else {
		if(lom_CallDll_KeywordExtractorInit(fptrToKeywordExtractor, TEXT_IN_DB, handle, docOid->volNo, CATALOG_GET_CLASSNAME(classInfo), docOid, (Two)GET_USERLEVEL_COLNO(systemLevelColNo), "", &resultHandle) < eNOERROR) 
		{
			free(postingBuf);
			LOM_ERROR(handle, eCHECKKEYWORDEXTRACTOR_LOM);
		}
	}

	/* copy to local variables */
	numOfReturnedIndex = 1;
	if(e = Catalog_GetIndexInfoByAttrNum(handle, docOid->volNo, docOid->classID, GET_USERLEVEL_COLNO(systemLevelColNo), &numOfReturnedIndex, &indexInfoTblIndex) < eNOERROR)
	{
		free(postingBuf);
		LOM_ERROR(handle, e);
	}

	indexInfo = &(CATALOG_GET_INDEXINFOTBL(handle, v)[indexInfoTblIndex]);

	isContainingSentenceAndWordNum = indexInfo->kdesc.invertedIndex.postingInfo.isContainingSentenceAndWordNum;
	isContainingTupleID = indexInfo->kdesc.invertedIndex.postingInfo.isContainingTupleID;
	isContainingByteOffset = indexInfo->kdesc.invertedIndex.postingInfo.isContainingByteOffset;
	nEmbeddedAttributes = indexInfo->kdesc.invertedIndex.postingInfo.nEmbeddedAttributes;
	embeddedAttrNo = indexInfo->kdesc.invertedIndex.postingInfo.embeddedAttrNo;

	/* make posting list */
	if ((e = lom_Text_OpenTempFile(handle, tempPostingFileName,"a+b",&outFd)) != eNOERROR)
	{
		free(postingBuf);
		LOM_ERROR(handle, e);
	}

	while ((lom_CallDll_KeywordExtractorNext(fptrToGetNextPostingInfo, resultHandle, keyword, &count, postingBuf)) != TEXT_DONE) 
    {
		int i;

        if(isContainingSentenceAndWordNum && count >= LOM_DEFAULTPOSTINGBUFFERSIZE / (sizeof(Four) + sizeof(Four)))
        {
            LOM_ERROR(handle, eBIGGERPOSTINGBUFFERNEEDED_LOM);
        }
        else if(isContainingByteOffset && count >= LOM_DEFAULTPOSTINGBUFFERSIZE / sizeof(Four))
        {
            LOM_ERROR(handle, eBIGGERPOSTINGBUFFERNEEDED_LOM);
        }

		/* check keyword */
		/* 만약 keyword가 white space로만 이루워진 것이라면 keyword로서 인정을 하지 않는다. */
		if(IsInvalidKeyword(keyword))
			continue;

		MakeLowercase(keyword);

		/* copy keyword and logicalID into the posting buffer */
#ifndef SUPPORT_LARGE_DATABASE2
		Util_fprintf(outFd,"%s %d ",keyword, docLogicalId);
#else
		Util_fprintf(outFd,"%s %ld ",keyword, docLogicalId);
#endif
		if(isContainingTupleID) {
			/* copy TupleID into the posting buffer */
#ifndef SUPPORT_LARGE_DATABASE2
			Util_fprintf(outFd,"%d %hd %hd %d ",docOid->pageNo, docOid->volNo, 
				docOid->slotNo, docOid->unique);
#else
			Util_fprintf(outFd,"%ld %d %d %ld ",docOid->pageNo, docOid->volNo, 
				docOid->slotNo, docOid->unique);
#endif
		}

		/* write number of positions */
#ifndef SUPPORT_LARGE_DATABASE2
		Util_fprintf(outFd, "%d ", count);
#else
		Util_fprintf(outFd, "%ld ", count);
#endif

		if(isContainingSentenceAndWordNum || isContainingByteOffset)
		{
			offset = 0;
			for (i = 0; i < count; i ++) {
				/* read sentence and word position. */
				if(isContainingSentenceAndWordNum) {
					bcopy(&postingBuf[offset], &sentencePos, sizeof(Four));
					offset += sizeof(Four);

					bcopy(&postingBuf[offset], &iWordPos, sizeof(Four));
					offset += sizeof(Four);
					wordPos = (Two)iWordPos;

					/* copy sentence position, word position into buffer */
#ifndef SUPPORT_LARGE_DATABASE2
					Util_fprintf(outFd,"%d %hd ", sentencePos, wordPos);
#else
					Util_fprintf(outFd,"%ld %d ", sentencePos, wordPos);
#endif
				}

				if(isContainingByteOffset) {

					bcopy(&postingBuf[offset], &byteOffset, sizeof(Four));
					offset += sizeof(Four);

					/* read byte offset. */
#ifndef SUPPORT_LARGE_DATABASE2
					Util_fprintf(outFd,"%d ", byteOffset);
#else
					Util_fprintf(outFd,"%ld ", byteOffset);
#endif
				}
			}
		}

		if(nEmbeddedAttributes > 0) {
			/* fill embedded attributes */
			e = lom_MakeColListStruct(handle, nEmbeddedAttributes, embeddedAttrNo, attrInfo, clist, LOM_DEFAULTPOSTINGBUFFERSIZE, postingBuf);
			if(e< eNOERROR) 
			{
				free(postingBuf);
				LOM_ERROR(handle, e);
			}

			e = LOM_FetchObjectByColList(handle, ocnOrScanId, useScanFlag, docOid, nEmbeddedAttributes, clist);
			if(e< eNOERROR) 
			{
				free(postingBuf);
				LOM_ERROR(handle, e);
			}
		}

		for(i = 0; i < nEmbeddedAttributes; i++) {
			/* if value is null */
			if(clist[i].nullFlag) {
				Util_fprintf(outFd,"1 ");
				continue;
			}

			else Util_fprintf(outFd, "0 ");

			switch(CATALOG_GET_ATTRTYPE(&(attrInfo[embeddedAttrNo[i]]))) {
				case LOM_LONG_LONG:
					Util_fprintf(outFd,"%lld ", clist[i].data.ll);
					break;
				case LOM_LONG:
				case LOM_ULONG:
					Util_fprintf(outFd,"%d ", clist[i].data.l);
					break;
				case LOM_INT:
					Util_fprintf(outFd,"%d ", clist[i].data.i);
					break;
				case LOM_SHORT:
				case LOM_USHORT:
					Util_fprintf(outFd,"%hd ", clist[i].data.s);
					break;
				case LOM_FLOAT:
					Util_fprintf(outFd,"%f ", clist[i].data.f);
					break;
				case LOM_DOUBLE:
					Util_fprintf(outFd,"%lf ", clist[i].data.d);
					break;
				case LOM_DATE:
					Util_fprintf(outFd,"%d ", clist[i].data.date);
					break;
				case LOM_TIME:
					Util_fprintf(outFd,"%hd:%hd:%hd:%hd:%hd:%hd ", clist[i].data.time._tzHour, clist[i].data.time._tzMinute, clist[i].data.time._Hour, clist[i].data.time._Minute, clist[i].data.time._Second, clist[i].data.time._100thSec);
					break;
				case LOM_OID:
#ifndef SUPPORT_LARGE_DATABASE2
					Util_fprintf(outFd,"%d:%hd:%hd:%d:%d ", clist[i].data.oid.pageNo, clist[i].data.oid.volNo, clist[i].data.oid.slotNo, clist[i].data.oid.unique, clist[i].data.oid.classID);
#else
					Util_fprintf(outFd,"%ld:%d:%d:%ld:%ld ", clist[i].data.oid.pageNo, clist[i].data.oid.volNo, clist[i].data.oid.slotNo, clist[i].data.oid.unique, clist[i].data.oid.classID);
#endif
					break;
				case LOM_STRING:
				case LOM_VARSTRING:
					lom_EncodeStringVal(&(clist[i]), encodedStr);
					Util_fputs(encodedStr, outFd);
					fputc(' ', outFd);
					break;
			}
		}

		fputc('\n',outFd);
	}

	if((e = lom_CallDll_KeywordExtractorFinal(fptrToFinalizeKeywordExtraction, resultHandle)) < eNOERROR) 
	{
		free(postingBuf);
		LOM_ERROR(handle, e);
	}

	/* close output file */
	if ((e = lom_Text_CloseTempFile(handle, outFd)) != eNOERROR)
	{
		free(postingBuf);
		LOM_ERROR(handle, e);
	}

	free(postingBuf);

	return eNOERROR;
}

Four lom_Text_SortPostingFileByKeyword(
	LOM_Handle *handle, 
	char *fileName,
	char *sortedFileName
)
{
#ifndef WIN32
	char* tempDir;
	char buf[4096];

	tempDir = getenv("O_TEMP_PATH");
	if(tempDir == NULL) LOM_ERROR(handle, eCONFIGURATION_LOM);


	sprintf(buf,"sort -T %s -k 1,1 +1 -2n %s > %s", tempDir, fileName, sortedFileName); 
	system(buf);
#else
	char* tempDir;
	char buf[4096];
	unsigned long retValue;
	STARTUPINFO startInfo;
	PROCESS_INFORMATION processInfo;

	tempDir = getenv("O_TEMP_PATH");
	if(tempDir == NULL) LOM_ERROR(handle, eCONFIGURATION_LOM);

	sprintf(buf, "unixsort -T %s -k 1,1 +1 -2n %s -o %s", tempDir, fileName, sortedFileName);

	memset((void *)&startInfo,0,sizeof(STARTUPINFO));
	startInfo.cb = sizeof(STARTUPINFO);
	startInfo.dwFlags = STARTF_USESHOWWINDOW;
	startInfo.wShowWindow = SW_HIDE;
	if (!CreateProcess(NULL,buf,NULL,NULL,FALSE, 
						REALTIME_PRIORITY_CLASS,
						NULL,NULL,
						&startInfo, &processInfo))
	{
		return -1;      // error code 
	}

	while (GetExitCodeProcess(processInfo.hProcess, &retValue))
	{
		if (retValue != STILL_ACTIVE) break;
	}
#endif

	return eNOERROR;
}

Four lom_Text_CreateTempFile(
	LOM_Handle *handle, 
	char *fileName 		/* IN: file name */
)
{
	FILE *pfile = Util_fopen(fileName,"w");

	if (pfile == NULL) LOM_ERROR(handle, eBADPARAMETER_LOM);

	if (Util_fclose(pfile) == EOF) LOM_ERROR(handle, eFAILTOCLOSEUNIXFILE_LOM);

	return eNOERROR;
}

Four lom_Text_DestroyTempFile(
	LOM_Handle *handle, 
	char *fileName 		/* IN: file name */
)
{
	if (unlink(fileName) != 0) LOM_ERROR(handle, eFAILTODESTROYUNIXFILE_LOM);

	return eNOERROR;
}

Four lom_Text_OpenTempFile(
	LOM_Handle *handle, 
	char *fileName, 		/* IN: file name */
	char *type, 			/* IN: open type */ 
	FILE **fd			/* OUT: file descriptor */
)
{
	*fd = Util_fopen(fileName, type);

	if (*fd == NULL) {
		fprintf(stderr, "File Name: %s\n", fileName);
		LOM_ERROR(handle, eFAILTOOPENUNIXFILE_LOM);
	}

	return eNOERROR;
}

Four lom_Text_CloseTempFile(
	LOM_Handle *handle, 
	FILE *fd			/* IN: file descriptor */
)
{
	if (Util_fclose(fd) == EOF) LOM_ERROR(handle, eFAILTOCLOSEUNIXFILE_LOM);

	return eNOERROR;
}

Four lom_GetMaxEncodedEmbeddedAttrs(
	Four nEmbeddedAttributes, 
	catalog_SysAttributesOverlay *attrInfo, 
	Two *attrNos
)
{
	Two nVarCols = 0;
	Four i;
	Four headerLengthForEncodedEmbeddedAttrs = 0;
	Four lengthForEncodedEmbeddedAttrs = 0;

	for(i = 0; i < nEmbeddedAttributes; i++) {
		if(CATALOG_GET_ATTRTYPE(&(attrInfo[attrNos[i]])) == LOM_VARSTRING) nVarCols++;
	}

	headerLengthForEncodedEmbeddedAttrs = nVarCols * sizeof(Two) /* slot array */
					+ (nEmbeddedAttributes + 7)/8; /* null vector size */

	lengthForEncodedEmbeddedAttrs = headerLengthForEncodedEmbeddedAttrs;
	for(i = 0; i < nEmbeddedAttributes; i++) {
		lengthForEncodedEmbeddedAttrs += CATALOG_GET_ATTRLENGTH(&(attrInfo[attrNos[i]])); 
	}

	return lengthForEncodedEmbeddedAttrs;
}

Four lom_EncodeEmbeddedAttrs(
	Four nEmbeddedAttributes, 		/* IN: the number of embedded attributes */
	catalog_SysAttributesOverlay *attrInfo, /* IN: pointer to attribute-info. catalog */
	Four bufLength,				/* IN: lenghth for buffer for encoded embedded attributes */
	char *ptrToEmbeddedAttrsBuf,	/* OUT: buffer for encoded embedded attribute */
	LOM_ColListStruct *clist, 		/* IN: column list struct which has embedded attributes values */
	Four *actualLengthForEncodedEmbeddedAttrs	/* OUT: actual length of encodedEmbedded attributes */
)
{
	Four i;
	Four firstVarColOffset = 0;
	unsigned char *nullVector;	/* bit array of null flag */
	Four nVarCols;
	Two tmpVarOffset;
	Four bufIndex;
	Four j;

	nVarCols = 0;
	for(i = 0; i < nEmbeddedAttributes; i++) {
		if(CATALOG_GET_ATTRTYPE(&attrInfo[GET_SYSTEMLEVEL_COLNO(clist[i].colNo)]) != LOM_VARSTRING) {
			firstVarColOffset += CATALOG_GET_ATTRLENGTH(&attrInfo[GET_SYSTEMLEVEL_COLNO(clist[i].colNo)]);
		}
		else nVarCols++;
	}

	nullVector = (unsigned char *)&(ptrToEmbeddedAttrsBuf[(nVarCols)*sizeof(Two)]);

	bufIndex = (nVarCols)*sizeof(Two) + (nEmbeddedAttributes + 7)/8;

	/* store fixed-size column value */
	for(i = 0; i < nEmbeddedAttributes; i++) {
		if(CATALOG_GET_ATTRTYPE(&attrInfo[GET_SYSTEMLEVEL_COLNO(clist[i].colNo)]) != LOM_VARSTRING) {
			if(clist[i].nullFlag) BITSET(nullVector, i);
			else {
				BITRESET(nullVector, i);

				if(CATALOG_GET_ATTRTYPE(&attrInfo[GET_SYSTEMLEVEL_COLNO(clist[i].colNo)]) == LOM_STRING) {
					memcpy(&ptrToEmbeddedAttrsBuf[bufIndex], clist[i].data.ptr, clist[i].dataLength);
				}
				else {
					memcpy(&ptrToEmbeddedAttrsBuf[bufIndex], &(clist[i].data), clist[i].dataLength);
				}
			}
			bufIndex += CATALOG_GET_ATTRLENGTH(&attrInfo[GET_SYSTEMLEVEL_COLNO(clist[i].colNo)]);
		}
	}

	/* store variable-size column value */
	if(nVarCols > 0) {
		j = 0;
		tmpVarOffset = (Two)firstVarColOffset;

		for(i = 0; i < nEmbeddedAttributes; i++) {
			if(CATALOG_GET_ATTRTYPE(&attrInfo[GET_SYSTEMLEVEL_COLNO(clist[i].colNo)]) == LOM_VARSTRING) {
				if(clist[i].nullFlag) {
					BITSET(nullVector, i);
					memcpy(&ptrToEmbeddedAttrsBuf[j*sizeof(Two)], &tmpVarOffset, sizeof(Two));
				}
				else {
					BITRESET(nullVector, i);
					memcpy(&ptrToEmbeddedAttrsBuf[j*sizeof(Two)], &tmpVarOffset, sizeof(Two));
					memcpy(&ptrToEmbeddedAttrsBuf[bufIndex], clist[i].data.ptr, clist[i].dataLength);
					bufIndex += clist[i].dataLength;
                    tmpVarOffset += (Two)clist[i].dataLength;
				}
				j++;
			}
		}
	}

	*actualLengthForEncodedEmbeddedAttrs = bufIndex;

	return eNOERROR;
}


Four lom_Text_PreparePostingInfoForReading(
	LOM_Handle*								handle, 
	Four									volId,				/* IN: volume id */
	ClassID									classId,			/* IN: class id */
	Two										colNo,				/* IN: system level column number */
	lom_Text_PostingInfoForReading*			postingInfo 		/* OUT: posting info */
)
{
	Four							idxForClassInfo, v;
	catalog_SysClassesOverlay*		ptrToSysClasses;
	catalog_SysAttributesOverlay*	ptrToSysAttributes;
	catalog_SysIndexesOverlay*		indexInfo;
	Four							indexInfoTblIndex;
	Four							numOfReturnedIndex;
	Four							attrInfoIndex;
	Four                            e;

	v = Catalog_GetVolIndex(handle, volId);
	if(v < 0) LOM_ERROR(handle, v);
	e = Catalog_GetClassInfo(handle, volId, classId, &idxForClassInfo);
	if(e < 0) LOM_ERROR(handle, e);
	ptrToSysClasses    = &CATALOG_GET_CLASSINFOTBL(handle, v)[idxForClassInfo];
	ptrToSysAttributes = &CATALOG_GET_ATTRINFOTBL(handle, v)[CATALOG_GET_ATTRINFOTBL_INDEX(ptrToSysClasses)];
	attrInfoIndex      = CATALOG_GET_ATTRINFOTBL_INDEX(ptrToSysClasses);
	numOfReturnedIndex = 1;
	e = Catalog_GetIndexInfoByAttrNum(handle, volId, classId, GET_USERLEVEL_COLNO(colNo), &numOfReturnedIndex, &indexInfoTblIndex);
	if(e < eNOERROR) LOM_ERROR(handle, e);
	indexInfo = &(CATALOG_GET_INDEXINFOTBL(handle, v)[indexInfoTblIndex]);

	postingInfo->isContainingTupleID            = indexInfo->kdesc.invertedIndex.postingInfo.isContainingTupleID;
	postingInfo->isContainingSentenceAndWordNum = indexInfo->kdesc.invertedIndex.postingInfo.isContainingSentenceAndWordNum;
	postingInfo->isContainingByteOffset         = indexInfo->kdesc.invertedIndex.postingInfo.isContainingByteOffset;
	postingInfo->nEmbeddedAttributes            = indexInfo->kdesc.invertedIndex.postingInfo.nEmbeddedAttributes;
	postingInfo->attrInfoIndex                  = attrInfoIndex;
	postingInfo->volumeIndex                    = v;
	memcpy(postingInfo->embeddedAttrNo, indexInfo->kdesc.invertedIndex.postingInfo.embeddedAttrNo, sizeof(postingInfo->embeddedAttrNo));

	postingInfo->maxEmbeddedAttrLength = 0;
	if(postingInfo->nEmbeddedAttributes > 0) {
		postingInfo->maxEmbeddedAttrLength = lom_GetMaxEncodedEmbeddedAttrs(postingInfo->nEmbeddedAttributes, ptrToSysAttributes, postingInfo->embeddedAttrNo);
	}

	return eNOERROR;
}

/* if file pointer reaches end of file , it returns EOS */
Four lom_Text_NextPostingsFromTempFile(
	LOM_Handle *handle, 
	Four volId,											/* IN: volume id */
	ClassID classId,									/* IN: class id */
	Two systemLevelColNo,								/* IN: system level column number */
	lom_Text_PostingInfoForReading* postingInfo,		/* IN: posting info */
	FILE *fd, 											/* IN: file descriptor */
	Four postingBufferLength,							/* IN: length of posting buffer */
	char *postingBuffer,								/* IN: posting buffer */
	Four requestedNumOfPostings,						/* IN: the requested number of posings */
	Four *returnedNumOfPostings,						/* OUT: the returned number of postings */
	Four *requiredPostingBufferLength					/* OUT: required posting buffer length for containing at least one posting */
)
{
	char keyword[LOM_MAXKEYWORDSIZE];
	Four count;
	Four logicalId;
	Four pageNo;
	Four volNo;
	Four slotNo;
	UFour unique;
	OID	 oid;
	int  index;
	Four sentencePos;
	Two wordPos;
	Four postingLength;
#ifdef SUBINDEX
	Four postingLengthFieldSize = LOM_LONG_SIZE_VAR;
#else
	Four postingLengthFieldSize = 0;
#endif
	LOM_ColListStruct clist[LOM_MAXNUMOFATTRIBUTE];
	char postingBuf[LOM_DEFAULTPOSTINGBUFFERSIZE];
	Four indexForLengthField;
	Four byteOffset;
	Four e;
	char encodedStr[LOM_MAXVARSTRINGSIZE];
	Four encodedAttrLength;
	Four keywordLength;
	catalog_SysAttributesOverlay*	attrInfo;
	filepos_t filePos;

	attrInfo = &CATALOG_GET_ATTRINFOTBL(handle, postingInfo->volumeIndex)[postingInfo->attrInfoIndex];
	*requiredPostingBufferLength = 0;
	*returnedNumOfPostings = 0;
	index = 0;

	filePos = Util_ftell(fd);
#ifndef SUPPORT_LARGE_DATABASE2
	while (Util_fscanf(fd,"%s%d",keyword, &logicalId) != EOF) {
#else
	while (Util_fscanf(fd,"%s%ld",keyword, &logicalId) != EOF) {
#endif
		int i;

		if(postingInfo->isContainingTupleID) {
#ifndef SUPPORT_LARGE_DATABASE2
			Util_fscanf(fd,"%d %hd %hd %d",&pageNo, &volNo, &slotNo, &unique);
#else
			Util_fscanf(fd,"%ld %d %d %ld",&pageNo, &volNo, &slotNo, &unique);
#endif

			oid.pageNo = (PageNo)pageNo;
			oid.volNo = (VolID)volNo;
			oid.slotNo = (SlotNo)slotNo;
			oid.unique = (Unique)unique;
		}
		
		/* read the number of postings */
#ifndef SUPPORT_LARGE_DATABASE2
		Util_fscanf(fd,"%d",&count);
#else
		Util_fscanf(fd,"%ld",&count);
#endif

		/* calculate required posting_buffer_length */
		keywordLength = strlen(keyword) + 1; /* length of keyword */
		if(postingInfo->isContainingSentenceAndWordNum)
			*requiredPostingBufferLength = keywordLength
											+ postingLengthFieldSize
											+ LOM_POSTING_SIZE(count, sizeof(Four) + sizeof(Two))
											+ postingInfo->maxEmbeddedAttrLength;
		else if(postingInfo->isContainingByteOffset)
			*requiredPostingBufferLength = keywordLength
											+ postingLengthFieldSize
											+ LOM_POSTING_SIZE(count, sizeof(One))
											+ postingInfo->maxEmbeddedAttrLength;
		else
			*requiredPostingBufferLength = keywordLength
											+ postingLengthFieldSize
											+ LOM_POSTING_SIZE(count, 0)
											+ postingInfo->maxEmbeddedAttrLength;

		/* check whether posting buffer is sufficient */
		if (postingBufferLength - index < *requiredPostingBufferLength)
		{
			Util_fseek(fd, filePos, 0);
			return eMOREMEMORYREQUIRED_LOM;
		}
		
		/* copy keyword into buffer */
		memcpy(&postingBuffer[index], keyword, keywordLength);
		index += keywordLength;
		indexForLengthField = index;
		index += sizeof(Four);

		/* copy logicalId into buffer */
		memcpy(&postingBuffer[index], (void *)&logicalId, sizeof(Four));
		index += sizeof(Four);

		/* copy TID into buffer */
		if(postingInfo->isContainingTupleID) {
			memcpy(&postingBuffer[index], (void *)&oid, sizeof(TupleID));
			index += sizeof(TupleID);
		}

		/* copy the number of positions into buffer */
		memcpy(&postingBuffer[index], (void *)&count, sizeof(Four));
		index += sizeof(Four);

		/* copy positions into buffer */
		if(postingInfo->isContainingSentenceAndWordNum || postingInfo->isContainingByteOffset) 
        {
			for (i = 0; i < count; i++)
			{
				Four tmpWordPos; /* because of memory alignment */
	
				/* read sentence position, word position */
#ifndef SUPPORT_LARGE_DATABASE2
				Util_fscanf(fd,"%d%d",&sentencePos, &tmpWordPos);
#else
				Util_fscanf(fd,"%ld%ld",&sentencePos, &tmpWordPos);
#endif

				wordPos = (Two)tmpWordPos; /* because of memory alignment */

				/* copy the sentence position into buffer */
				memcpy(&postingBuffer[index], (void *)&sentencePos, sizeof(Four));
				index += sizeof(Four);
	
				memcpy(&postingBuffer[index], (void *)&wordPos, sizeof(Two));
				index += sizeof(Two);

				/* copy the word position into buffer */
				if(postingInfo->isContainingByteOffset) {
#ifndef SUPPORT_LARGE_DATABASE2
					Util_fscanf(fd,"%d",&byteOffset);
#else
					Util_fscanf(fd,"%ld",&byteOffset);
#endif
					memcpy(&postingBuffer[index], (void *)&byteOffset, sizeof(Four));
					index += sizeof(Four);
				}
			}
		}

		if(postingInfo->nEmbeddedAttributes > 0) {
			e = lom_MakeColListStruct(handle, postingInfo->nEmbeddedAttributes, postingInfo->embeddedAttrNo, attrInfo, clist, LOM_DEFAULTPOSTINGBUFFERSIZE, postingBuf);
			if(e< eNOERROR) 
			{
				free(postingBuf);
				LOM_ERROR(handle, e);
			}

			for(i = 0; i < postingInfo->nEmbeddedAttributes; i++) {
				Four nullFlag;
#ifndef SUPPORT_LARGE_DATABASE2
				Util_fscanf(fd,"%d", &nullFlag);
#else
				Util_fscanf(fd,"%ld ", &nullFlag);
#endif

				clist[i].nullFlag = (char)nullFlag;
				if(nullFlag) continue;

				switch(CATALOG_GET_ATTRTYPE(&(attrInfo[postingInfo->embeddedAttrNo[i]]))) {
					case LOM_LONG:
					case LOM_ULONG:
						Util_fscanf(fd,"%ld ", &(clist[i].data.l));
						break;
					case LOM_LONG_LONG:
						Util_fscanf(fd,"%ld ", &(clist[i].data.ll));
						break;
					case LOM_INT:
						Util_fscanf(fd,"%d", &(clist[i].data.i));
						break;
					case LOM_SHORT:
					case LOM_USHORT:
						Util_fscanf(fd,"%hi", &(clist[i].data.s));
						break;
					case LOM_FLOAT:
						Util_fscanf(fd,"%f", &(clist[i].data.f));
						break;
					case LOM_DOUBLE:
						Util_fscanf(fd,"%f", &(clist[i].data.d));
						break;
					case LOM_DATE:
						Util_fscanf(fd,"%ld", &(clist[i].data.date));
						break;
					case LOM_TIME:
						Util_fscanf(fd,"%hd:%hd:%hd:%hd:%hd:%hd", &(clist[i].data.time._tzHour), &(clist[i].data.time._tzMinute), &(clist[i].data.time._Hour), &(clist[i].data.time._Minute), &(clist[i].data.time._Second), &(clist[i].data.time._100thSec));
						break;
					case LOM_OID:
#ifndef SUPPORT_LARGE_DATABASE2
						Util_fscanf(fd,"%d:%hd:%hd:%d:%d", &(clist[i].data.oid.pageNo), &(clist[i].data.oid.volNo), &(clist[i].data.oid.slotNo), &(clist[i].data.oid.unique), &(clist[i].data.oid.classID));
#else
						Util_fscanf(fd,"%ld:%d:%d:%ld:%ld", &(clist[i].data.oid.pageNo), &(clist[i].data.oid.volNo), &(clist[i].data.oid.slotNo), &(clist[i].data.oid.unique), &(clist[i].data.oid.classID));
#endif
						break;
					case LOM_STRING:
						Util_fscanf(fd,"%s", encodedStr);
						lom_DecodeStringVal(encodedStr, &(clist[i]));
						break;
					case LOM_VARSTRING:
						Util_fscanf(fd,"%s", encodedStr);
						lom_DecodeStringVal(encodedStr, &(clist[i]));
						clist[i].dataLength = clist[i].length = strlen(clist[i].data.ptr);
						break;
				}
			}
			e = lom_EncodeEmbeddedAttrs(postingInfo->nEmbeddedAttributes, attrInfo, postingBufferLength - index, &postingBuffer[index], clist, &encodedAttrLength);
			if(e < eNOERROR) 
			{
				free(postingBuf);
				LOM_ERROR(handle, e);
			}

			index += encodedAttrLength;
		}

		/* calculate the length of posting */
		postingLength = index - keywordLength - sizeof(Four)/* length field */;

		/* copy the length of a posting */
		memcpy(&postingBuffer[indexForLengthField], &postingLength, sizeof(Four));

		(*returnedNumOfPostings) ++;
		if ((*returnedNumOfPostings) == requestedNumOfPostings) {
			return eNOERROR;
		}

		filePos = Util_ftell(fd);
	}

	/* file point reach end of file */
	return EOS;
}



/* EOF */

#else /* SLIMDOWN_TEXTIR */

#include "LOM_Internal.h"
#include "LOM.h"
#include "Catalog.h"
#include "Catalog_Internal.h"


Four lom_EncodeStringVal(
	LOM_ColListStruct *clist,
	char *buf
)
{
	return eTEXTIR_NOTENABLED_LOM;
} 


Four lom_DecodeStringVal(
	char *buf,
	LOM_ColListStruct *clist
)
{
	return eTEXTIR_NOTENABLED_LOM;
} 

Four lom_Text_ExtractKeywordFromContentBufferIntoPostingBuffer(
	LOM_Handle *handle, 
	lom_FptrToKeywordExtractor fptrToKeywordExtractor,	/* IN: function pointer to keyword extractor */
	lom_FptrToGetNextPostingInfo fptrToGetNextPostingInfo,
	lom_FptrToFinalizeKeywordExtraction fptrToFinalizeKeywordExtraction,
	lom_FptrToFilter fptrToFilter,	/* IN: function pointer to filter */
	Four ocnOrScanId,		/* IN: ocnOrScanId */
	Boolean useScanFlag,		/* IN: useScanFlag */
	OID  *docOid,			/* IN: document oid */
	Four docLogicalId,		/* IN: document logical id */
	Two systemLevelColNo,			/* IN: system level column number */
	Four postingBufferLength,	/* IN: length of posting buffer */
	char *postingBuffer,		/* OUT: posting buffer */
	Four *nKeywords			/* OUT: number of keywords contained in content buffer */
)
{
	return eTEXTIR_NOTENABLED_LOM;
} 

Four lom_MakeColListStruct(
	LOM_Handle *handle,
	Four nAttrs,
	Two *attrNos,
	catalog_SysAttributesOverlay *attrInfo,
	LOM_ColListStruct *clist,		/* OUT: collist struct */
	Four bufferLength,
	char *bufferForStrOrVarStr	/* OUT: buffer for storing variable-string or fixed-string */
)
{
	return eTEXTIR_NOTENABLED_LOM;
} 


Four lom_Text_ExtractKeywordFromContentObjectIntoTempFile(
	LOM_Handle *handle, 
	lom_FptrToKeywordExtractor fptrToKeywordExtractor,	/* IN: function pointer to keyword extractor */
	lom_FptrToGetNextPostingInfo fptrToGetNextPostingInfo,
	lom_FptrToFinalizeKeywordExtraction fptrToFinalizeKeywordExtraction,
	lom_FptrToFilter fptrToFilter,	/* IN: function pointer to filter */
	Four ocnOrScanId,		/* IN: open scan number for content table */
	Boolean useScanFlag,		/* IN: flag */
	TupleID *contentOid,		/* IN: object id for content object */
	OID *docOid,			/* IN: document oid */
	Four docLogicalId,		/* IN: document logical id */
	Two systemLevelColNo,			/* IN: system level column number */
	char *tempPostingFileName 	/* IN: temporary file */
)
{
	return eTEXTIR_NOTENABLED_LOM;
} 


Four lom_Text_SortPostingFileByKeyword(
	LOM_Handle *handle, 
	char *fileName,
	char *sortedFileName
)
{
	return eTEXTIR_NOTENABLED_LOM;
} 


Four lom_Text_CreateTempFile(
	LOM_Handle *handle, 
	char *fileName 		/* IN: file name */
)
{
	return eTEXTIR_NOTENABLED_LOM;
} 


Four lom_Text_DestroyTempFile(
	LOM_Handle *handle, 
	char *fileName 		/* IN: file name */
)
{
	return eTEXTIR_NOTENABLED_LOM;
} 


Four lom_Text_OpenTempFile(
	LOM_Handle *handle, 
	char *fileName, 		/* IN: file name */
	char *type, 			/* IN: open type */ 
	FILE **fd			/* OUT: file descriptor */
)
{
	return eTEXTIR_NOTENABLED_LOM;
} 


Four lom_Text_CloseTempFile(
	LOM_Handle *handle, 
	FILE *fd			/* IN: file descriptor */
)
{
	return eTEXTIR_NOTENABLED_LOM;
} 


Four lom_GetMaxEncodedEmbeddedAttrs(
	Four nEmbeddedAttributes, 
	catalog_SysAttributesOverlay *attrInfo, 
	Two *attrNos
)
{
	return eTEXTIR_NOTENABLED_LOM;
} 


Four lom_EncodeEmbeddedAttrs(
	Four nEmbeddedAttributes, 		/* IN: the number of embedded attributes */
	catalog_SysAttributesOverlay *attrInfo, /* IN: pointer to attribute-info. catalog */
	Four bufLength,				/* IN: lenghth for buffer for encoded embedded attributes */
	char *ptrToEmbeddedAttrsBuf,	/* OUT: buffer for encoded embedded attribute */
	LOM_ColListStruct *clist, 		/* IN: column list struct which has embedded attributes values */
	Four *actualLengthForEncodedEmbeddedAttrs	/* OUT: actual length of encodedEmbedded attributes */
)
{
	return eTEXTIR_NOTENABLED_LOM;
} 


Four lom_Text_PreparePostingInfoForReading(
	LOM_Handle*								handle, 
	Four									volId,				/* IN: volume id */
	ClassID									classId,			/* IN: class id */
	Two										colNo,				/* IN: system level column number */
	lom_Text_PostingInfoForReading*			postingInfo 		/* OUT: posting info */
)
{
	return eTEXTIR_NOTENABLED_LOM;
} 

/* if file pointer reaches end of file , it returns EOS */
Four lom_Text_NextPostingsFromTempFile(
	LOM_Handle *handle, 
	Four volId,											/* IN: volume id */
	ClassID classId,									/* IN: class id */
	Two systemLevelColNo,								/* IN: system level column number */
	lom_Text_PostingInfoForReading* postingInfo,		/* IN: posting info */
	FILE *fd, 											/* IN: file descriptor */
	Four postingBufferLength,							/* IN: length of posting buffer */
	char *postingBuffer,								/* IN: posting buffer */
	Four requestedNumOfPostings,						/* IN: the requested number of posings */
	Four *returnedNumOfPostings,						/* OUT: the returned number of postings */
	Four *requiredPostingBufferLength					/* OUT: required posting buffer length for containing at least one posting */
)
{
	return eTEXTIR_NOTENABLED_LOM;
} 

/* EOF */

#endif /* SLIMDOWN_TEXTIR */
