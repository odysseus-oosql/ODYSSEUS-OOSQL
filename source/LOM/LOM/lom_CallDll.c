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
 * Module: lom_CallDll.c
 *
 * Description:
 *  calling dynamic linked function module
 *
 * Imports:
 *  None
 *
 * Exports:
 *
 */
#include "LOM_Internal.h"
#include "LOM.h"

Four  lom_CallDll_KeywordExtractorInit(lom_FptrToKeywordExtractor funcPtr, Four locationOfContent, LOM_Handle *handle, Four volId, char *className, OID *oid, Two colNo, char *inFileName, Four *resultHandle)
{
	lom_dllFunc* dllFunc;
#ifndef USE_RPC	
	lom_KeywordExtractorFunc func;
#endif

	dllFunc = (lom_dllFunc*)funcPtr;

#ifdef USE_RPC
	if(dllFunc->dllClient)
	{
		if(locationOfContent == TEXT_IN_DB)
		{
			Four			ocn;
			TextColStruct	text;
			LOM_TextDesc	textDesc;
			char*			contentBuffer;
			Four			contentBufferSize;
			Four			e;

			ocn = LOM_GetOpenClassNum(handle, volId, className);
			if(ocn < 0) LOM_ERROR(handle, ocn);
			
			e = LOM_Text_GetDescriptor(handle, ocn, SM_FALSE, oid, colNo, &textDesc);
			if(e < 0) LOM_ERROR(handle, e);

			contentBufferSize = LOM_DEFAULTPOSTINGBUFFERSIZE;
			contentBuffer     = malloc(contentBufferSize);
			if(contentBuffer == NULL) LOM_ERROR(handle, eOUTOFMEMORY_LOM);

			while(1)
			{
				text.start		= ALL_VALUE;
				text.dataLength = contentBufferSize;
				text.data		= contentBuffer;

				e = LOM_Text_FetchContent(handle, ocn, SM_FALSE, oid, colNo, &text, &textDesc);
				if(e < 0) { free(contentBuffer); LOM_ERROR(handle, e); }

				if(text.retLength >= contentBufferSize - 1)
				{
					/* there is more data to read */
					/* doubling input buffer */
					contentBufferSize *= 2;
					contentBuffer     = (char*)realloc(contentBuffer, contentBufferSize);
					if(contentBuffer == NULL) LOM_ERROR(handle, eOUTOFMEMORY_LOM);
				}
				else
					break;
			}
			contentBuffer[text.retLength] = '\0';

			e = lom_RPCKeywordExtractorInit(funcPtr, TEXT_IN_MEMORY, NULL, 0, "", NULL, -1, contentBuffer, resultHandle);
			if(e < 0) { free(contentBuffer); LOM_ERROR(handle, e); }

			free(contentBuffer);

			return eNOERROR;
		}
		else
		{
			return lom_RPCKeywordExtractorInit(funcPtr, locationOfContent, handle, volId, className, oid, colNo, inFileName, resultHandle);
		}
	}
	else
	{
		/* process客 link等 function捞促. */
		lom_KeywordExtractorFunc func;

		func = (lom_KeywordExtractorFunc)dllFunc->dllFunc;

		return func(locationOfContent, handle, volId, className, oid, colNo, inFileName, resultHandle);
	}
#else
	/* process客 link等 function捞促. */
	func = (lom_KeywordExtractorFunc)dllFunc->dllFunc;

	return func(locationOfContent, handle, volId, className, oid, colNo, inFileName, resultHandle);
#endif
}

Four  lom_CallDll_KeywordExtractorNext(lom_FptrToGetNextPostingInfo funcPtr, Four resultHandle, char *keyword, Four *nPositions, char *positionList)
{
	lom_dllFunc* dllFunc;
#ifndef USE_RPC	
	lom_GetNextPostingInfoFunc func;
#endif

	dllFunc = (lom_dllFunc*)funcPtr;

#ifdef USE_RPC
	if(dllFunc->dllClient)
	{
		return lom_RPCKeywordExtractorNext(funcPtr, resultHandle, keyword, nPositions, positionList);
	}
	else
	{
		lom_GetNextPostingInfoFunc func;

		func = (lom_GetNextPostingInfoFunc)dllFunc->dllFunc;

		return func(resultHandle, keyword, nPositions, positionList);
	}
#else
	func = (lom_GetNextPostingInfoFunc)dllFunc->dllFunc;

	return func(resultHandle, keyword, nPositions, positionList);
#endif
}

Four  lom_CallDll_KeywordExtractorFinal(lom_FptrToFinalizeKeywordExtraction funcPtr, Four resultHandle)
{
	lom_dllFunc* dllFunc;
#ifndef USE_RPC	
	lom_FinalizeKeywordExtractionFunc func;
#endif

	dllFunc = (lom_dllFunc*)funcPtr;

#ifdef USE_RPC
	if(dllFunc->dllClient)
	{
		return lom_RPCKeywordExtractorFinal(funcPtr, resultHandle);
	}
	else
	{
		lom_FinalizeKeywordExtractionFunc func;

		func = (lom_FinalizeKeywordExtractionFunc)dllFunc->dllFunc;

		return func(resultHandle);
	}
#else
	func = (lom_FinalizeKeywordExtractionFunc)dllFunc->dllFunc;

	return func(resultHandle);
#endif
}

Four  lom_CallDll_Filter(lom_FptrToStemizer funcPtr, Four locationOfContent, LOM_Handle *handle, Four volId, char *className, OID *oid, Two colNo, char *inFileName, char *outFileName)
{
	lom_dllFunc* dllFunc;
#ifndef USE_RPC	
	lom_FilterFunc func;
#endif

	dllFunc = (lom_dllFunc*)funcPtr;
	
#ifdef USE_RPC
	if(dllFunc->dllClient)
	{
		LOM_ERROR(handle, eNOTIMPLEMENTED_LOM);
	}
	else
	{
		lom_FilterFunc func;

		func = (lom_FilterFunc)dllFunc->dllFunc;

		return func(locationOfContent, handle, volId, className, oid, colNo, inFileName, outFileName);
	}
#else
	func = (lom_FilterFunc)dllFunc->dllFunc;

	return func(locationOfContent, handle, volId, className, oid, colNo, inFileName, outFileName);
#endif
}

Four  lom_CallDll_Stemizer(lom_FptrToStemizer funcPtr, char *keyword, char *stemizedKeyword)
{
	lom_dllFunc* dllFunc;
#ifndef USE_RPC	
	lom_StemizerFunc func;
#endif

	dllFunc = (lom_dllFunc*)funcPtr;

#ifdef USE_RPC
	if(dllFunc->dllClient)
	{
		LOM_ERROR(handle, eNOTIMPLEMENTED_LOM);
	}
	else
	{
		lom_StemizerFunc func;

		func = (lom_StemizerFunc)dllFunc->dllFunc;

		return func(keyword, stemizedKeyword);
	}
#else
	func = (lom_StemizerFunc)dllFunc->dllFunc;

	return func(keyword, stemizedKeyword);
#endif
}
#else /* SLIMDOWN_TEXTIR */

#include "LOM_Internal.h"
#include "LOM.h"

Four  lom_CallDll_KeywordExtractorInit(lom_FptrToKeywordExtractor funcPtr, Four locationOfContent, LOM_Handle *handle, Four volId, char *className, OID *oid, Two colNo, char *inFileName, Four *resultHandle)
{
	    return eTEXTIR_NOTENABLED_LOM;
}


Four  lom_CallDll_KeywordExtractorNext(lom_FptrToGetNextPostingInfo funcPtr, Four resultHandle, char *keyword, Four *nPositions, char *positionList)
{
	    return eTEXTIR_NOTENABLED_LOM;
}


Four  lom_CallDll_KeywordExtractorFinal(lom_FptrToFinalizeKeywordExtraction funcPtr, Four resultHandle)
{
	    return eTEXTIR_NOTENABLED_LOM;
}


Four  lom_CallDll_Filter(lom_FptrToStemizer funcPtr, Four locationOfContent, LOM_Handle *handle, Four volId, char *className, OID *oid, Two colNo, char *inFileName, char *outFileName)
{
	    return eTEXTIR_NOTENABLED_LOM;
}


Four  lom_CallDll_Stemizer(lom_FptrToStemizer funcPtr, char *keyword, char *stemizedKeyword)
{
	    return eTEXTIR_NOTENABLED_LOM;
}


#endif /* SLIMDOWN_TEXTIR */

