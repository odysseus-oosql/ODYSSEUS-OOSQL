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

#include "OOSQL_ExternalFunctionDispatcher.hxx"
#include "OOSQL_Error.h"
#include "dlfcn.h"

typedef Four (*OOSQL_ExtFunc_FuncType)(...);
typedef Four (*OOSQL_ExtFunc_Init_FuncType)(...);

OOSQL_ExternalFunctionDispatcher::OOSQL_ExternalFunctionDispatcher(OOSQL_SystemHandle* oosqlSystemHandle, const OOSQL_DbInfo& dbInfo)
: m_dispatchedFunctions(pMemoryManager, true), m_externalProcessFunctionExecutor(oosqlSystemHandle, OOSQL_ExternalProcessFunctionExecutor::OOSQL_SERVER, dbInfo, pMemoryManager)
{
	m_oosqlSystemHandle = oosqlSystemHandle;
	m_dbInfo = dbInfo;
}

OOSQL_ExternalFunctionDispatcher::~OOSQL_ExternalFunctionDispatcher()
{
	int i;

	for(i = 0; i < (int)m_dispatchedFunctions.numberOfItems(); i++)
	{
		if(m_dispatchedFunctions[i].m_functionPtr)
		{
			if(dlclose(m_dispatchedFunctions[i].m_dlHandle))
				OOSQL_PRTERR(eEXTERNALFUNCTION_DLCLOSE_FAIL_OOSQL)
		}
		m_dispatchedFunctions[i].m_dlHandle = NULL;
	}
}

Four ExtractModuleNameAndFuncName(const char* implementation, OOSQL_TCDynStr& moduleName, OOSQL_TCDynStr& funcName)
{
	char	temp[1024];
	int 	i, j;
	Boolean isModuleName = SM_TRUE;

	for(i = 0, j = 0; i < (int)strlen(implementation); i++)
	{
		if(implementation[i] == ':' && implementation[i + 1] != '\\')
		{
			temp[j] = 0;
			moduleName = temp;
			isModuleName = SM_FALSE;
			j = 0;
		}
		else
		{
			temp[j] = implementation[i];
			j++;
		}
	}

	if(isModuleName)
	{
		temp[j] = 0;
		moduleName = temp;
		funcName = temp;
	}
	else
	{
		temp[j] = 0;
		funcName = temp;
	}

	return eNOERROR;
}

Four OOSQL_ExternalFunctionDispatcher::ExecuteFenced(OOSQL_ExternalFunctionInfo& functionInfo, OOSQL_DB_Value& returnValue, Four nParams, OOSQL_DB_Value* parameters, char* errorMessage, Four finalCall)
{
	Four e;

	e = m_externalProcessFunctionExecutor.Execute(functionInfo, returnValue, nParams, parameters, errorMessage, finalCall);
	OOSQL_CHECK_ERR(e);

	return eNOERROR;
}

Four OOSQL_ExternalFunctionDispatcher::ExecuteNotFenced(OOSQL_ExternalFunctionInfo& functionInfo, OOSQL_DB_Value& returnValue, Four nParams, OOSQL_DB_Value* parameters, char* errorMessage, Four finalCall)
{
	Four							e;
	Four							i;
	Four							funcIndex;
	OOSQL_TCDynStr					moduleName(pMemoryManager);
	OOSQL_TCDynStr					funcName(pMemoryManager);
	OOSQL_DispatchedFunctionInfo	dispatchedFunctionInfo(pMemoryManager);
	OOSQL_ExtFunc_FuncType			externalFunction;
	void*							paramPtrs[90];
	void*							paramReturn;
	Four                             paramReturnLength;
	Two                           paramNullIndicator[90];
	Two                           paramReturnNullIndicator;

	if(nParams != functionInfo.m_paramCount)
		OOSQL_ERR(eBADPARAMETER_OOSQL);

	// get parameter pointers
	paramReturnNullIndicator = SM_FALSE;
	switch(returnValue.type)
	{
	case TYPEID_STRING:
	case TYPEID_VARSTRING:
	case TYPEID_TEXT:
		paramReturn = returnValue.data.ptr;
		break;
	default:
#ifndef SUPPORT_LARGE_DATABASE2
		paramReturn = (void*)&returnValue.data.s;
#else
		paramReturn = (void*)&returnValue.data.i;
#endif
		break;
	}
	
	for(i = 0; i < nParams; i++)
	{
		switch(parameters[i].type)
		{
		case TYPEID_STRING:
		case TYPEID_VARSTRING:
		case TYPEID_TEXT:
			paramPtrs[i]          = (void*)parameters[i].data.ptr;
			if(parameters[i].nullFlag)
				paramNullIndicator[i] = SM_TRUE;
			else
				paramNullIndicator[i] = SM_FALSE;
			break;
		default:
#ifndef SUPPORT_LARGE_DATABASE2
			paramPtrs[i]          = (void*)&(parameters[i].data.s);
#else
			paramPtrs[i]          = (void*)&(parameters[i].data.i);
#endif
			if(parameters[i].nullFlag)
				paramNullIndicator[i] = SM_TRUE;
			else
				paramNullIndicator[i] = SM_FALSE;
			break;
		}
	}

	for(i = 0; i < m_dispatchedFunctions.numberOfItems(); i++)
	{
		if(m_dispatchedFunctions[i].m_volID == functionInfo.m_volID && m_dispatchedFunctions[i].m_funcID == functionInfo.m_funcID)
		{
			funcIndex = i;
			break;
		}
	}

	if(i == m_dispatchedFunctions.numberOfItems()) // not found
	{
		OOSQL_ExtFunc_Init_FuncType OOSQL_ExtFunc_Init;

		e = ExtractModuleNameAndFuncName(functionInfo.m_implementation, moduleName, funcName);
		OOSQL_CHECK_ERR(e);

		dispatchedFunctionInfo.m_dlHandle = dlopen((char*)(const char*)moduleName, RTLD_LAZY);
		if(dispatchedFunctionInfo.m_dlHandle == NULL)
			OOSQL_ERR(eEXTERNALFUNCTION_DLOPEN_FAIL_OOSQL);

		dispatchedFunctionInfo.m_functionPtr = dlsym(dispatchedFunctionInfo.m_dlHandle, 
				                                     (char*)(const char*)funcName);
		if(dispatchedFunctionInfo.m_functionPtr == NULL)
			OOSQL_ERR(eEXTERNALFUNCTION_DLSYM_FAIL_OOSQL);

		dispatchedFunctionInfo.m_volID			= functionInfo.m_volID;
		dispatchedFunctionInfo.m_funcID			= functionInfo.m_funcID;

		OOSQL_ExtFunc_Init = (OOSQL_ExtFunc_Init_FuncType)dlsym(dispatchedFunctionInfo.m_dlHandle, "OOSQL_ExtFunc_Init");
		if(OOSQL_ExtFunc_Init)
		{
		    e = OOSQL_ExtFunc_Init(
				    OOSQL_CreateSystemHandle,
				    OOSQL_DestroySystemHandle,
				    OOSQL_Connect,
				    OOSQL_Disconnect,
				    OOSQL_SetUserDefaultVolumeID,
				    OOSQL_GetUserDefaultVolumeID,
				    OOSQL_GetVolumeID,
				    OOSQL_MountDB,
				    OOSQL_DismountDB,
				    OOSQL_AllocHandle,
				    OOSQL_FreeHandle,
				    OOSQL_Mount,
				    OOSQL_Dismount,
				    OOSQL_TransBegin,
				    OOSQL_TransCommit,
				    OOSQL_TransAbort,
				    OOSQL_Prepare,
				    OOSQL_Execute,
				    OOSQL_ExecDirect,
				    OOSQL_Next,
				    OOSQL_GetData,
				    OOSQL_GetMultiColumnData,
				    OOSQL_PutData,
				    OOSQL_GetOID,
				    OOSQL_GetNumResultCols,
				    OOSQL_GetResultColName,
				    OOSQL_GetResultColType,
				    OOSQL_GetResultColLength,
				    OOSQL_GetErrorMessage,
				    OOSQL_GetErrorName,
				    OOSQL_GetQueryErrorMessage,
				    OOSQL_OIDToOIDString,
				    OOSQL_Text_AddKeywordExtractor,
				    OOSQL_Text_AddDefaultKeywordExtractor,
				    OOSQL_Text_DropKeywordExtractor,
				    OOSQL_Text_SetKeywordExtractor,
				    OOSQL_Text_AddFilter,
				    OOSQL_Text_DropFilter,
				    OOSQL_Text_SetFilter,
				    OOSQL_Text_MakeIndex,
				    OOSQL_Text_KeywordInfoScan_Open,
				    OOSQL_Text_KeywordInfoScan_Close,
				    OOSQL_Text_KeywordInfoScan_Next,
				    OOSQL_Text_KeywordInfoScanForDocument_Open,
				    OOSQL_Text_KeywordInfoScanForDocument_Close,
				    OOSQL_Text_KeywordInfoScanForDocument_Next,
				    OOSQL_Text_DefinePostingStructure,
				    OOSQL_GetLocalTimeZone,
				    OOSQL_SetCurTime,
				    OOSQL_GetHour,
				    OOSQL_GetMinute,
				    OOSQL_GetSecond,
				    OOSQL_GetYear,
				    OOSQL_GetMonth,
				    OOSQL_GetDay,
				    OOSQL_GetVersionString,
				    OOSQL_GetTimeElapsed,
				    OOSQL_ResetTimeElapsed
			    );
		    OOSQL_CHECK_ERR(e);
		}
		m_dispatchedFunctions.add(dispatchedFunctionInfo);
	}
	else
		dispatchedFunctionInfo = m_dispatchedFunctions[funcIndex];

	externalFunction = (OOSQL_ExtFunc_FuncType)dispatchedFunctionInfo.m_functionPtr;
	
	switch(nParams)
	{
	case 0:
		e = externalFunction(paramReturn, &paramReturnLength,
			                 &paramReturnNullIndicator,
							 functionInfo.m_functionName, functionInfo.m_specificName, errorMessage, 
							 m_oosqlSystemHandle,
							 &dispatchedFunctionInfo.scratchPad, &m_dbInfo);
		OOSQL_CHECK_ERR(e);
		break;
	case 1:
		e = externalFunction(paramPtrs[0], 
							 paramReturn, &paramReturnLength,
							 &paramNullIndicator[0],
							 &paramReturnNullIndicator,
							 functionInfo.m_functionName, functionInfo.m_specificName, errorMessage, 
							 m_oosqlSystemHandle,
							 &dispatchedFunctionInfo.scratchPad, &m_dbInfo);
		OOSQL_CHECK_ERR(e);
		break;
	case 2:
		e = externalFunction(paramPtrs[0], paramPtrs[1],
			                 paramReturn, &paramReturnLength,
							 &paramNullIndicator[0], &paramNullIndicator[1],
							 &paramReturnNullIndicator,
							 functionInfo.m_functionName, functionInfo.m_specificName, errorMessage, 
							 m_oosqlSystemHandle,
							 &dispatchedFunctionInfo.scratchPad, &m_dbInfo);
		OOSQL_CHECK_ERR(e);
		break;
	case 3:
		e = externalFunction(paramPtrs[0], paramPtrs[1], paramPtrs[2],
							 paramReturn, &paramReturnLength,
							 &paramNullIndicator[0], &paramNullIndicator[1], &paramNullIndicator[2],
							 &paramReturnNullIndicator,
							 functionInfo.m_functionName, functionInfo.m_specificName, errorMessage, 
							 m_oosqlSystemHandle,
							 &dispatchedFunctionInfo.scratchPad, &m_dbInfo);
		OOSQL_CHECK_ERR(e);
		break;
	case 4:
		e = externalFunction(paramPtrs[0], paramPtrs[1], paramPtrs[2], paramPtrs[3],
							 paramReturn, &paramReturnLength,
							 &paramNullIndicator[0], &paramNullIndicator[1], &paramNullIndicator[2], &paramNullIndicator[3],
							 &paramReturnNullIndicator,
							 functionInfo.m_functionName, functionInfo.m_specificName, errorMessage, 
							 m_oosqlSystemHandle,
							 &dispatchedFunctionInfo.scratchPad, &m_dbInfo);
		OOSQL_CHECK_ERR(e);
		break;
	case 5:
		e = externalFunction(paramPtrs[0], paramPtrs[1], paramPtrs[2], paramPtrs[3], paramPtrs[4],
							 paramReturn, &paramReturnLength,
							 &paramNullIndicator[0], &paramNullIndicator[1], &paramNullIndicator[2], &paramNullIndicator[3],  &paramNullIndicator[4],
							 &paramReturnNullIndicator,
							 functionInfo.m_functionName, functionInfo.m_specificName, errorMessage, 
							 m_oosqlSystemHandle,
							 &dispatchedFunctionInfo.scratchPad, &m_dbInfo);
		OOSQL_CHECK_ERR(e);
		break;
	case 6:
		e = externalFunction(paramPtrs[0], paramPtrs[1], paramPtrs[2], paramPtrs[3], paramPtrs[4],
							 paramPtrs[5], 
							 paramReturn, &paramReturnLength,
							 &paramNullIndicator[0], &paramNullIndicator[1], &paramNullIndicator[2], &paramNullIndicator[3],  &paramNullIndicator[4],
							 &paramNullIndicator[5], 
							 &paramReturnNullIndicator,
							 functionInfo.m_functionName, functionInfo.m_specificName, errorMessage, 
							 m_oosqlSystemHandle,
							 &dispatchedFunctionInfo.scratchPad, &m_dbInfo);
		OOSQL_CHECK_ERR(e);
		break;
	case 7:
		e = externalFunction(paramPtrs[0], paramPtrs[1], paramPtrs[2], paramPtrs[3], paramPtrs[4],
				             paramPtrs[5], paramPtrs[6],
							 paramReturn, &paramReturnLength,
							 &paramNullIndicator[0], &paramNullIndicator[1], &paramNullIndicator[2], &paramNullIndicator[3], &paramNullIndicator[4],
							 &paramNullIndicator[5], &paramNullIndicator[6], 
							 &paramReturnNullIndicator,
							 functionInfo.m_functionName, functionInfo.m_specificName, errorMessage, 
							 m_oosqlSystemHandle,
							 &dispatchedFunctionInfo.scratchPad, &m_dbInfo);
		OOSQL_CHECK_ERR(e);
		break;
	case 8:
		e = externalFunction(paramPtrs[0], paramPtrs[1], paramPtrs[2], paramPtrs[3], paramPtrs[4],
				             paramPtrs[5], paramPtrs[6], paramPtrs[7],
							 paramReturn, &paramReturnLength,
							 &paramNullIndicator[0], &paramNullIndicator[1], &paramNullIndicator[2], &paramNullIndicator[3], &paramNullIndicator[4], 
							 &paramNullIndicator[5], &paramNullIndicator[6], &paramNullIndicator[7],
							 &paramReturnNullIndicator,
							 functionInfo.m_functionName, functionInfo.m_specificName, errorMessage, 
							 m_oosqlSystemHandle,
							 &dispatchedFunctionInfo.scratchPad, &m_dbInfo);
		OOSQL_CHECK_ERR(e);
		break;
	case 9:
		e = externalFunction(paramPtrs[0], paramPtrs[1], paramPtrs[2], paramPtrs[3], paramPtrs[4],
				             paramPtrs[5], paramPtrs[6], paramPtrs[7], paramPtrs[8],
							 paramReturn, &paramReturnLength,
							 &paramNullIndicator[0], &paramNullIndicator[1], &paramNullIndicator[2], &paramNullIndicator[3], &paramNullIndicator[4], 
							 &paramNullIndicator[5], &paramNullIndicator[6], &paramNullIndicator[7], &paramNullIndicator[8],
							 &paramReturnNullIndicator,
							 functionInfo.m_functionName, functionInfo.m_specificName, errorMessage, 
							 m_oosqlSystemHandle,
							 &dispatchedFunctionInfo.scratchPad, &m_dbInfo);
		OOSQL_CHECK_ERR(e);
		break;
	case 10:
		e = externalFunction(paramPtrs[0], paramPtrs[1], paramPtrs[2], paramPtrs[3], paramPtrs[4],
				             paramPtrs[5], paramPtrs[6], paramPtrs[7], paramPtrs[8], paramPtrs[9],
							 paramReturn, &paramReturnLength,
							 &paramNullIndicator[0], &paramNullIndicator[1], &paramNullIndicator[2], &paramNullIndicator[3], &paramNullIndicator[4], 
							 &paramNullIndicator[5], &paramNullIndicator[6], &paramNullIndicator[7], &paramNullIndicator[8], &paramNullIndicator[9],
							 &paramReturnNullIndicator,
							 functionInfo.m_functionName, functionInfo.m_specificName, errorMessage, 
							 m_oosqlSystemHandle,
							 &dispatchedFunctionInfo.scratchPad, &m_dbInfo);
		OOSQL_CHECK_ERR(e);
		break;
	case 11:
		e = externalFunction(paramPtrs[0], paramPtrs[1], paramPtrs[2], paramPtrs[3], paramPtrs[4],
				             paramPtrs[5], paramPtrs[6], paramPtrs[7], paramPtrs[8], paramPtrs[9],
							 paramPtrs[10],
							 paramReturn, &paramReturnLength,
							 &paramNullIndicator[0], &paramNullIndicator[1], &paramNullIndicator[2], &paramNullIndicator[3], &paramNullIndicator[4], 
							 &paramNullIndicator[5], &paramNullIndicator[6], &paramNullIndicator[7], &paramNullIndicator[8], &paramNullIndicator[9],
							 &paramNullIndicator[10], 
							 &paramReturnNullIndicator,
							 functionInfo.m_functionName, functionInfo.m_specificName, errorMessage, 
							 m_oosqlSystemHandle,
							 &dispatchedFunctionInfo.scratchPad, &m_dbInfo);
		OOSQL_CHECK_ERR(e);
		break;
	case 12:
		e = externalFunction(paramPtrs[0], paramPtrs[1], paramPtrs[2], paramPtrs[3], paramPtrs[4],
				             paramPtrs[5], paramPtrs[6], paramPtrs[7], paramPtrs[8], paramPtrs[9],
							 paramPtrs[10], paramPtrs[11],
							 paramReturn, &paramReturnLength,
							 &paramNullIndicator[0], &paramNullIndicator[1], &paramNullIndicator[2], &paramNullIndicator[3], &paramNullIndicator[4], 
							 &paramNullIndicator[5], &paramNullIndicator[6], &paramNullIndicator[7], &paramNullIndicator[8], &paramNullIndicator[9],
							 &paramNullIndicator[10], &paramNullIndicator[11], 
							 &paramReturnNullIndicator,
							 functionInfo.m_functionName, functionInfo.m_specificName, errorMessage, 
							 m_oosqlSystemHandle,
							 &dispatchedFunctionInfo.scratchPad, &m_dbInfo);
		OOSQL_CHECK_ERR(e);
		break;
	case 13:
		e = externalFunction(paramPtrs[0], paramPtrs[1], paramPtrs[2], paramPtrs[3], paramPtrs[4],
				             paramPtrs[5], paramPtrs[6], paramPtrs[7], paramPtrs[8], paramPtrs[9],
							 paramPtrs[10], paramPtrs[11], paramPtrs[12],
							 paramReturn, &paramReturnLength,
							 &paramNullIndicator[0], &paramNullIndicator[1], &paramNullIndicator[2], &paramNullIndicator[3], &paramNullIndicator[4], 
							 &paramNullIndicator[5], &paramNullIndicator[6], &paramNullIndicator[7], &paramNullIndicator[8], &paramNullIndicator[9],
							 &paramNullIndicator[10], &paramNullIndicator[11], &paramNullIndicator[12], 
							 &paramReturnNullIndicator,
							 functionInfo.m_functionName, functionInfo.m_specificName, errorMessage, 
							 m_oosqlSystemHandle,
							 &dispatchedFunctionInfo.scratchPad, &m_dbInfo);
		OOSQL_CHECK_ERR(e);
		break;
	case 14:
		e = externalFunction(paramPtrs[0], paramPtrs[1], paramPtrs[2], paramPtrs[3], paramPtrs[4],
				             paramPtrs[5], paramPtrs[6], paramPtrs[7], paramPtrs[8], paramPtrs[9],
							 paramPtrs[10], paramPtrs[11], paramPtrs[12], paramPtrs[13],
							 paramReturn, &paramReturnLength,
							 &paramNullIndicator[0], &paramNullIndicator[1], &paramNullIndicator[2], &paramNullIndicator[3], &paramNullIndicator[4], 
							 &paramNullIndicator[5], &paramNullIndicator[6], &paramNullIndicator[7], &paramNullIndicator[8], &paramNullIndicator[9],
							 &paramNullIndicator[10], &paramNullIndicator[11], &paramNullIndicator[12], &paramNullIndicator[13], 
							 &paramReturnNullIndicator,
							 functionInfo.m_functionName, functionInfo.m_specificName, errorMessage, 
							 m_oosqlSystemHandle,
							 &dispatchedFunctionInfo.scratchPad, &m_dbInfo);
		OOSQL_CHECK_ERR(e);
		break;
	case 15:
		e = externalFunction(paramPtrs[0], paramPtrs[1], paramPtrs[2], paramPtrs[3], paramPtrs[4],
				             paramPtrs[5], paramPtrs[6], paramPtrs[7], paramPtrs[8], paramPtrs[9],
							 paramPtrs[10], paramPtrs[11], paramPtrs[12], paramPtrs[13], paramPtrs[14],
							 paramReturn, &paramReturnLength,
							 &paramNullIndicator[0], &paramNullIndicator[1], &paramNullIndicator[2], &paramNullIndicator[3], &paramNullIndicator[4], 
							 &paramNullIndicator[5], &paramNullIndicator[6], &paramNullIndicator[7], &paramNullIndicator[8], &paramNullIndicator[9],
							 &paramNullIndicator[10], &paramNullIndicator[11], &paramNullIndicator[12], &paramNullIndicator[13], &paramNullIndicator[14], 
							 &paramReturnNullIndicator,
							 functionInfo.m_functionName, functionInfo.m_specificName, errorMessage, 
							 m_oosqlSystemHandle,
							 &dispatchedFunctionInfo.scratchPad, &m_dbInfo);
		OOSQL_CHECK_ERR(e);
		break;
	case 16:
		e = externalFunction(paramPtrs[0], paramPtrs[1], paramPtrs[2], paramPtrs[3], paramPtrs[4],
				             paramPtrs[5], paramPtrs[6], paramPtrs[7], paramPtrs[8], paramPtrs[9],
							 paramPtrs[10], paramPtrs[11], paramPtrs[12], paramPtrs[13], paramPtrs[14],
							 paramPtrs[15],
							 paramReturn, &paramReturnLength,
							 &paramNullIndicator[0], &paramNullIndicator[1], &paramNullIndicator[2], &paramNullIndicator[3], &paramNullIndicator[4], 
							 &paramNullIndicator[5], &paramNullIndicator[6], &paramNullIndicator[7], &paramNullIndicator[8], &paramNullIndicator[9],
							 &paramNullIndicator[10], &paramNullIndicator[11], &paramNullIndicator[12], &paramNullIndicator[13], &paramNullIndicator[14], 
							 &paramNullIndicator[15], 
							 &paramReturnNullIndicator,
							 functionInfo.m_functionName, functionInfo.m_specificName, errorMessage, 
							 m_oosqlSystemHandle,
							 &dispatchedFunctionInfo.scratchPad, &m_dbInfo);
		OOSQL_CHECK_ERR(e);
		break;
	case 17:
		e = externalFunction(paramPtrs[0], paramPtrs[1], paramPtrs[2], paramPtrs[3], paramPtrs[4],
				             paramPtrs[5], paramPtrs[6], paramPtrs[7], paramPtrs[8], paramPtrs[9],
							 paramPtrs[10], paramPtrs[11], paramPtrs[12], paramPtrs[13], paramPtrs[14],
							 paramPtrs[15], paramPtrs[16],
							 paramReturn, &paramReturnLength,
							 &paramNullIndicator[0], &paramNullIndicator[1], &paramNullIndicator[2], &paramNullIndicator[3], &paramNullIndicator[4], 
							 &paramNullIndicator[5], &paramNullIndicator[6], &paramNullIndicator[7], &paramNullIndicator[8], &paramNullIndicator[9],
							 &paramNullIndicator[10], &paramNullIndicator[11], &paramNullIndicator[12], &paramNullIndicator[13], &paramNullIndicator[14], 
							 &paramNullIndicator[15], &paramNullIndicator[16], 
							 &paramReturnNullIndicator,
							 functionInfo.m_functionName, functionInfo.m_specificName, errorMessage, 
							 m_oosqlSystemHandle,
							 &dispatchedFunctionInfo.scratchPad, &m_dbInfo);
		OOSQL_CHECK_ERR(e);
		break;
	case 18:
		e = externalFunction(paramPtrs[0], paramPtrs[1], paramPtrs[2], paramPtrs[3], paramPtrs[4],
				             paramPtrs[5], paramPtrs[6], paramPtrs[7], paramPtrs[8], paramPtrs[9],
							 paramPtrs[10], paramPtrs[11], paramPtrs[12], paramPtrs[13], paramPtrs[14],
							 paramPtrs[15], paramPtrs[16], paramPtrs[17],
							 paramReturn, &paramReturnLength,
							 &paramNullIndicator[0], &paramNullIndicator[1], &paramNullIndicator[2], &paramNullIndicator[3], &paramNullIndicator[4], 
							 &paramNullIndicator[5], &paramNullIndicator[6], &paramNullIndicator[7], &paramNullIndicator[8], &paramNullIndicator[9],
							 &paramNullIndicator[10], &paramNullIndicator[11], &paramNullIndicator[12], &paramNullIndicator[13], &paramNullIndicator[14], 
							 &paramNullIndicator[15], &paramNullIndicator[16], &paramNullIndicator[17], 
							 &paramReturnNullIndicator,
							 functionInfo.m_functionName, functionInfo.m_specificName, errorMessage, 
							 m_oosqlSystemHandle,
							 &dispatchedFunctionInfo.scratchPad, &m_dbInfo);
		OOSQL_CHECK_ERR(e);
		break;
	case 19:
		e = externalFunction(paramPtrs[0], paramPtrs[1], paramPtrs[2], paramPtrs[3], paramPtrs[4],
				             paramPtrs[5], paramPtrs[6], paramPtrs[7], paramPtrs[8], paramPtrs[9],
							 paramPtrs[10], paramPtrs[11], paramPtrs[12], paramPtrs[13], paramPtrs[14],
							 paramPtrs[15], paramPtrs[16], paramPtrs[17], paramPtrs[18],
							 paramReturn, &paramReturnLength,
							 &paramNullIndicator[0], &paramNullIndicator[1], &paramNullIndicator[2], &paramNullIndicator[3], &paramNullIndicator[4], 
							 &paramNullIndicator[5], &paramNullIndicator[6], &paramNullIndicator[7], &paramNullIndicator[8], &paramNullIndicator[9],
							 &paramNullIndicator[10], &paramNullIndicator[11], &paramNullIndicator[12], &paramNullIndicator[13], &paramNullIndicator[14], 
							 &paramNullIndicator[15], &paramNullIndicator[16], &paramNullIndicator[17], &paramNullIndicator[18], 
							 &paramReturnNullIndicator,
							 functionInfo.m_functionName, functionInfo.m_specificName, errorMessage, 
							 m_oosqlSystemHandle,
							 &dispatchedFunctionInfo.scratchPad, &m_dbInfo);
		OOSQL_CHECK_ERR(e);
		break;
	case 20:
		e = externalFunction(paramPtrs[0], paramPtrs[1], paramPtrs[2], paramPtrs[3], paramPtrs[4],
				             paramPtrs[5], paramPtrs[6], paramPtrs[7], paramPtrs[8], paramPtrs[9],
							 paramPtrs[10], paramPtrs[11], paramPtrs[12], paramPtrs[13], paramPtrs[14],
							 paramPtrs[15], paramPtrs[16], paramPtrs[17], paramPtrs[18], paramPtrs[19],
							 paramReturn, &paramReturnLength,
							 &paramNullIndicator[0], &paramNullIndicator[1], &paramNullIndicator[2], &paramNullIndicator[3], &paramNullIndicator[4], 
							 &paramNullIndicator[5], &paramNullIndicator[6], &paramNullIndicator[7], &paramNullIndicator[8], &paramNullIndicator[9],
							 &paramNullIndicator[10], &paramNullIndicator[11], &paramNullIndicator[12], &paramNullIndicator[13], &paramNullIndicator[14], 
							 &paramNullIndicator[15], &paramNullIndicator[16], &paramNullIndicator[17], &paramNullIndicator[18], &paramNullIndicator[19], 
							 &paramReturnNullIndicator,
							 functionInfo.m_functionName, functionInfo.m_specificName, errorMessage, 
							 m_oosqlSystemHandle,
							 &dispatchedFunctionInfo.scratchPad, &m_dbInfo);
		OOSQL_CHECK_ERR(e);
		break;
	default:
		OOSQL_ERR(eTOO_MANY_PARAMS_IN_FUNC_CALL_OOSQL);
	}
	
	if(paramReturnNullIndicator)
		returnValue.nullFlag = SM_TRUE;
	else
		returnValue.nullFlag = SM_FALSE;

	returnValue.length = paramReturnLength;

	return eNOERROR;
}

Four OOSQL_ExternalFunctionDispatcher::Execute(OOSQL_ExternalFunctionInfo& functionInfo, OOSQL_DB_Value& returnValue, Four nParams, OOSQL_DB_Value* parameters, char* errorMessage, Four finalCall)
{
	Four							e;

	if(nParams != functionInfo.m_paramCount)
		OOSQL_ERR(eBADPARAMETER_OOSQL);

	returnValue.type   = functionInfo.m_returnType;
	if(returnValue.type == OOSQL_TYPE_STRING || returnValue.type == OOSQL_TYPE_VARSTRING)
	{
		returnValue.PrepareData(OOSQL_MAX_RETURN_VALUE_LENGTH);
	}

	if(functionInfo.m_fenced)
	{
		e = ExecuteFenced(functionInfo, returnValue, nParams, parameters, errorMessage, finalCall);
		OOSQL_CHECK_ERR(e);
	}
	else
	{
		e = ExecuteNotFenced(functionInfo, returnValue, nParams, parameters, errorMessage, finalCall);
		OOSQL_CHECK_ERR(e);
	}

	return eNOERROR;
}

