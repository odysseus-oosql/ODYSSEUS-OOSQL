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

#include "OOSQL_ExternalFunctionManager.hxx"
#include "OOSQL_Eval_Util.hxx"

OOSQL_ExternalFunctionManager::OOSQL_ExternalFunctionManager(OOSQL_SystemHandle* oosqlSystemHandle) 
: m_functionInfos(pMemoryManager, false)
{
	m_oosqlSystemHandle = oosqlSystemHandle;
}

OOSQL_ExternalFunctionManager::~OOSQL_ExternalFunctionManager() 
{
}

Four OOSQL_ExternalFunctionManager::Find(Four volID, char* functionName, OOSQL_TCArray<OOSQL_ExternalFunctionInfo>& functionInfos)
{
	Four			i;
	Four			e;
	OOSQL_Handle	funcQueryHandle;
	OOSQL_Handle	paramQueryHandle;

	functionInfos.empty();
	for(i = 0; i < m_functionInfos.numberOfItems(); i++)
	{
		if(m_functionInfos[i].m_volID == volID && m_functionInfos[i].m_functionName == functionName)
		{
			OOSQL_ExternalFunctionInfo info = m_functionInfos[i];
			functionInfos.add(info);
		}
	}

	if(functionInfos.isEmpty())
	{
		char*	funcSchema;
		char*	funcName;
		char*	specificName;
		char*	definer;
		Four	funcID;
		Two		returnType;
		char	origin[2];
		char	type[2];
		Two		paramCount;
		char*	paramSignature;
		OOSQL_StorageManager::Timestamp createTime;
		char	deterministic[2];
		char	sideEffects[2];
		char	fenced[2];
		char	nullCall[2];
		char	castFunction[2];
		char	assignFunction[2];
		char	scratchPad[2];
		char	finalCall[2];
		char	parallelizable[2];
		char	dbinfo[2];
		Two		resultCols;
		char	language[9];
		char*	implementation;
		char	paramStyle[9];
		char*	sourceSchema;
		char*	sourceSpecific;
		char	rowType[2];
		char*	parmName;
		char*	typeSchema;
		char*	typeName;
		Four	typeID;
		Four	length;
		Two		scale;
		Two		codePage;
		Four	castFuncID;
		char	asLocator[2];
		char*   temp;

		funcSchema = (char*)pMemoryManager->Alloc(65);
		funcName = (char*)pMemoryManager->Alloc(65);
		specificName = (char*)pMemoryManager->Alloc(65);
		definer = (char*)pMemoryManager->Alloc(65);
		paramSignature = (char*)pMemoryManager->Alloc(65);
		temp = (char*)pMemoryManager->Alloc(256);
		implementation = (char*)pMemoryManager->Alloc(65);
		sourceSchema = (char*)pMemoryManager->Alloc(65);
		sourceSpecific = (char*)pMemoryManager->Alloc(65);
		parmName = (char*)pMemoryManager->Alloc(65);
		typeSchema = (char*)pMemoryManager->Alloc(65);
		typeName = (char*)pMemoryManager->Alloc(65);

		OOSQL_TCDynStr queryString(pMemoryManager);

		queryString  = "SELECT FuncSchema, FuncName, SpecificName, Definer, FuncID, ReturnType, ";
		queryString += "Origin, Type, ParamCount, ParamSignature, CreateTime, Deterministic, ";
		queryString += "SideEffects, Fenced, NullCall, CastFunction, AssignFunction, ScratchPad, ";
		queryString += "FinalCall, Parallelizable, Dbinfo, ResultCols, Language, Implementation, ";
		queryString += "ParamStyle, SourceSchema, SourceSpecific ";
		queryString += "FROM LOM_SYS_FUNCTIONS ";
		queryString += "WHERE FuncName = '";
		queryString += functionName;
		queryString += "'";

		e = oosql_SQL_Init(m_oosqlSystemHandle, volID, &funcQueryHandle);
		OOSQL_CHECK_ERR(e);
		e = oosql_SQL_Exec(m_oosqlSystemHandle, funcQueryHandle, queryString);
		OOSQL_CHECK_ERR(e);
		while((e = oosql_SQL_Next(m_oosqlSystemHandle, funcQueryHandle, 
			                      funcSchema, funcName, specificName, definer, &funcID, &returnType, 
								  origin, type, &paramCount, paramSignature, &createTime, deterministic,
		                          sideEffects, fenced, nullCall, castFunction, assignFunction, scratchPad, 
								  finalCall, parallelizable, dbinfo, &resultCols, language, implementation, 
								  paramStyle, sourceSchema, sourceSpecific)) != eNORESULTS_OOSQL)
		{
			OOSQL_ExternalFunctionInfo* functionInfo;

			functionInfo = (OOSQL_ExternalFunctionInfo*)pMemoryManager->Alloc(sizeof(OOSQL_ExternalFunctionInfo));

			functionInfo->m_volID = volID;
			strcpy(functionInfo->m_functionSchema, funcSchema);
			strcpy(functionInfo->m_functionName, funcName);
			strcpy(functionInfo->m_specificName, specificName);
			strcpy(functionInfo->m_definer, definer);
			functionInfo->m_funcID = funcID;
			functionInfo->m_procID = -1;
			functionInfo->m_returnType = (TypeID)returnType;
			functionInfo->m_returnAsLocator = SM_FALSE;
			functionInfo->m_origin = origin[0];
			functionInfo->m_type = type[0];
			functionInfo->m_paramCount = paramCount;
			strcpy(functionInfo->m_paramSignature, paramSignature);
			if(toupper(deterministic[0]) == 'Y')
				functionInfo->m_deterministic = SM_TRUE;
			else
				functionInfo->m_deterministic = SM_FALSE;
			if(toupper(sideEffects[0]) == 'Y')
				functionInfo->m_sideEffects = SM_TRUE;
			else
				functionInfo->m_sideEffects = SM_FALSE;
			if(toupper(fenced[0]) == 'Y')
				functionInfo->m_fenced = SM_TRUE;
			else
				functionInfo->m_fenced = SM_FALSE;
			if(toupper(nullCall[0]) == 'Y')
				functionInfo->m_nullCall = SM_TRUE;
			else
				functionInfo->m_nullCall = SM_FALSE;
			if(toupper(castFunction[0]) == 'Y')
				functionInfo->m_castFunction = SM_TRUE;
			else
				functionInfo->m_castFunction = SM_FALSE;
			if(toupper(assignFunction[0]) == 'Y')
				functionInfo->m_assignFunction = SM_TRUE;
			else
				functionInfo->m_assignFunction = SM_FALSE;
			if(toupper(scratchPad[0]) == 'Y')
				functionInfo->m_scratchPad = SM_TRUE;
			else
				functionInfo->m_scratchPad = SM_FALSE;
			if(toupper(finalCall[0]) == 'Y')
				functionInfo->m_finalCall = SM_TRUE;
			else
				functionInfo->m_finalCall = SM_FALSE;
			if(toupper(parallelizable[0]) == 'Y')
				functionInfo->m_allowParallel = SM_TRUE;
			else
				functionInfo->m_allowParallel = SM_FALSE;
			if(toupper(dbinfo[0]) == 'Y')
				functionInfo->m_dbInfo = SM_TRUE;
			else
				functionInfo->m_dbInfo = SM_FALSE;
			functionInfo->m_resultCols = resultCols;
			strcpy(functionInfo->m_language, language);
			strcpy(functionInfo->m_implementation, implementation);
			strcpy(functionInfo->m_parameterStyle, paramStyle);
			strcpy(functionInfo->m_sourceSchema, sourceSchema);
			strcpy(functionInfo->m_sourceSpecific, sourceSpecific);
			
			queryString  = "SELECT RowType, ParmName, TypeSchema, TypeName, TypeID, ";
			queryString += "Length, Scale, CodePage, CastFuncID, AsLocator ";
			queryString += "FROM LOM_SYS_FUNCPARMS ";
			queryString += "WHERE FuncID = ";
			sprintf(temp, "%ld ", funcID);
			queryString += temp;
			queryString += "ORDER BY Ordinal";

			e = oosql_SQL_Init(m_oosqlSystemHandle, volID, &paramQueryHandle);
			OOSQL_CHECK_ERR(e);
			e = oosql_SQL_Exec(m_oosqlSystemHandle, paramQueryHandle, queryString);
			OOSQL_CHECK_ERR(e);

			Four index = 0;
			while((e = oosql_SQL_Next(m_oosqlSystemHandle, paramQueryHandle, 
				                      rowType, parmName, typeSchema, typeName, &typeID,
									  &length, &scale, &codePage, &castFuncID, asLocator)) != eNORESULTS_OOSQL)
			{
				OOSQL_CHECK_ERR(e);

				OOSQL_ExternalFunctionParameterInfo paramInfo;

				strcpy(paramInfo.m_argumentName, parmName);
				paramInfo.m_argumentType = (TypeID)typeID;
				strcpy(paramInfo.m_argumentTypeName, typeName);
				paramInfo.m_argumentTypeLength = length;
				paramInfo.m_parameterMode = PARM_IN;
				if(toupper(asLocator[0]) == 'Y')
					paramInfo.m_asLocator = SM_TRUE;
				else
					paramInfo.m_asLocator = SM_FALSE;
				
				functionInfo->m_parameters[index] = paramInfo;
				index ++;
			}
			if(e != eNORESULTS_OOSQL) OOSQL_CHECK_ERR(e);
			e = oosql_SQL_Final(m_oosqlSystemHandle, paramQueryHandle);
			OOSQL_CHECK_ERR(e);

			functionInfos.add(*functionInfo);
			m_functionInfos.add(*functionInfo);			

			pMemoryManager->Free(functionInfo);
		}
		if(e != eNORESULTS_OOSQL) OOSQL_CHECK_ERR(e);
		e = oosql_SQL_Final(m_oosqlSystemHandle, funcQueryHandle);
		OOSQL_CHECK_ERR(e);

		pMemoryManager->Free(funcSchema);
		pMemoryManager->Free(funcName);
		pMemoryManager->Free(specificName);
		pMemoryManager->Free(definer);
		pMemoryManager->Free(paramSignature);
		pMemoryManager->Free(temp);
		pMemoryManager->Free(implementation);
		pMemoryManager->Free(sourceSchema);
		pMemoryManager->Free(sourceSpecific);
		pMemoryManager->Free(parmName);
		pMemoryManager->Free(typeSchema);
		pMemoryManager->Free(typeName);
	}

	return eNOERROR;
}

Four OOSQL_ExternalFunctionManager::Find(Four volID, Four funcID, OOSQL_ExternalFunctionInfo& functionInfo)
{
	Four			i;
	Four			e;
	OOSQL_Handle	funcQueryHandle;
	OOSQL_Handle	paramQueryHandle;

	for(i = 0; i < m_functionInfos.numberOfItems(); i++)
	{
		if(m_functionInfos[i].m_volID == volID && m_functionInfos[i].m_funcID == funcID)
		{
			functionInfo = m_functionInfos[i];
			break;
		}
	}

	if(i == m_functionInfos.numberOfItems())
	{
		char*	funcSchema;
		char*	funcName;
		char*	specificName;
		char*	definer;
		Two		returnType;
		char	origin[2];
		char	type[2];
		Two		paramCount;
		char*	paramSignature;
		OOSQL_StorageManager::Timestamp createTime;
		char	deterministic[2];
		char	sideEffects[2];
		char	fenced[2];
		char	nullCall[2];
		char	castFunction[2];
		char	assignFunction[2];
		char	scratchPad[2];
		char	finalCall[2];
		char	parallelizable[2];
		char	dbinfo[2];
		Two		resultCols;
		char	language[9];
		char*	implementation;
		char	paramStyle[9];
		char*	sourceSchema;
		char*	sourceSpecific;
		char	rowType[2];
		char*	parmName;
		char*	typeSchema;
		char*	typeName;
		Four	typeID;
		Four	length;
		Two		scale;
		Two		codePage;
		Four	castFuncID;
		char	asLocator[2];
		char*   temp;

		funcSchema = (char*)pMemoryManager->Alloc(65);
		funcName = (char*)pMemoryManager->Alloc(65);
		specificName = (char*)pMemoryManager->Alloc(65);
		definer = (char*)pMemoryManager->Alloc(65);
		paramSignature = (char*)pMemoryManager->Alloc(181);
		temp = (char*)pMemoryManager->Alloc(256);
		implementation = (char*)pMemoryManager->Alloc(1001);
		sourceSchema = (char*)pMemoryManager->Alloc(65);
		sourceSpecific = (char*)pMemoryManager->Alloc(65);
		parmName = (char*)pMemoryManager->Alloc(65);
		typeSchema = (char*)pMemoryManager->Alloc(65);
		typeName = (char*)pMemoryManager->Alloc(65);

		OOSQL_TCDynStr queryString(pMemoryManager);

		queryString  = "SELECT FuncSchema, FuncName, SpecificName, Definer, FuncID, ReturnType, ";
		queryString += "Origin, Type, ParamCount, ParamSignature, CreateTime, Deterministic, ";
		queryString += "SideEffects, Fenced, NullCall, CastFunction, AssignFunction, ScratchPad, ";
		queryString += "FinalCall, Parallelizable, Dbinfo, ResultCols, Language, Implementation, ";
		queryString += "ParamStyle, SourceSchema, SourceSpecific ";
		queryString += "FROM LOM_SYS_FUNCTIONS ";
		queryString += "WHERE FuncID = ";
		sprintf(temp, "%ld", temp);
		queryString += temp;
		
		e = oosql_SQL_Init(m_oosqlSystemHandle, volID, &funcQueryHandle);
		OOSQL_CHECK_ERR(e);
		e = oosql_SQL_Exec(m_oosqlSystemHandle, funcQueryHandle, queryString);
		OOSQL_CHECK_ERR(e);
		while((e = oosql_SQL_Next(m_oosqlSystemHandle, funcQueryHandle, funcSchema, funcName, specificName, definer, &funcID,
		                          &returnType, origin, type, &paramCount, paramSignature, &createTime, deterministic,
		                          sideEffects, fenced, nullCall, castFunction, assignFunction, scratchPad, finalCall,
		                          parallelizable, dbinfo, &resultCols, language, implementation, paramStyle,
		                          sourceSchema, sourceSpecific)) != eNORESULTS_OOSQL)
		{
			functionInfo.m_volID = volID;
			strcpy(functionInfo.m_functionSchema, funcSchema);
			strcpy(functionInfo.m_functionName, funcName);
			strcpy(functionInfo.m_specificName, specificName);
			strcpy(functionInfo.m_definer, definer);
			functionInfo.m_funcID = funcID;
			functionInfo.m_procID = -1;
			functionInfo.m_returnType = (TypeID)returnType;
			functionInfo.m_returnAsLocator = SM_FALSE;
			functionInfo.m_origin = origin[0];
			functionInfo.m_type = type[0];
			functionInfo.m_paramCount = paramCount;
			strcpy(functionInfo.m_paramSignature, paramSignature);
			if(toupper(deterministic[0]) == 'Y')
				functionInfo.m_deterministic = SM_TRUE;
			else
				functionInfo.m_deterministic = SM_FALSE;
			if(toupper(sideEffects[0]) == 'Y')
				functionInfo.m_sideEffects = SM_TRUE;
			else
				functionInfo.m_sideEffects = SM_FALSE;
			if(toupper(fenced[0]) == 'Y')
				functionInfo.m_fenced = SM_TRUE;
			else
				functionInfo.m_fenced = SM_FALSE;
			if(toupper(nullCall[0]) == 'Y')
				functionInfo.m_nullCall = SM_TRUE;
			else
				functionInfo.m_nullCall = SM_FALSE;
			if(toupper(castFunction[0]) == 'Y')
				functionInfo.m_castFunction = SM_TRUE;
			else
				functionInfo.m_castFunction = SM_FALSE;
			if(toupper(assignFunction[0]) == 'Y')
				functionInfo.m_assignFunction = SM_TRUE;
			else
				functionInfo.m_assignFunction = SM_FALSE;
			if(toupper(scratchPad[0]) == 'Y')
				functionInfo.m_scratchPad = SM_TRUE;
			else
				functionInfo.m_scratchPad = SM_FALSE;
			if(toupper(finalCall[0]) == 'Y')
				functionInfo.m_finalCall = SM_TRUE;
			else
				functionInfo.m_finalCall = SM_FALSE;
			if(toupper(parallelizable[0]) == 'Y')
				functionInfo.m_allowParallel = SM_TRUE;
			else
				functionInfo.m_allowParallel = SM_FALSE;
			if(toupper(dbinfo[0]) == 'Y')
				functionInfo.m_dbInfo = SM_TRUE;
			else
				functionInfo.m_dbInfo = SM_FALSE;
			functionInfo.m_resultCols = resultCols;
			strcpy(functionInfo.m_language, language);
			strcpy(functionInfo.m_implementation, implementation);
			strcpy(functionInfo.m_parameterStyle, paramStyle);
			strcpy(functionInfo.m_sourceSchema, sourceSchema);
			strcpy(functionInfo.m_sourceSpecific, sourceSpecific);

			queryString  = "SELECT RowType, ParmName, TypeSchema, TypeName, TypeID, ";
			queryString += "Length, Scale, CodePage, CastFuncID, AsLocator ";
			queryString += "FROM LOM_SYS_FUNCPARMS ";
			queryString += "WHERE FuncID = ";
			sprintf(temp, "%ld ", funcID);
			queryString += temp;
			queryString += "ORDER BY Ordinal";

			e = oosql_SQL_Init(m_oosqlSystemHandle, volID, &paramQueryHandle);
			OOSQL_CHECK_ERR(e);
			e = oosql_SQL_Exec(m_oosqlSystemHandle, paramQueryHandle, queryString);
			OOSQL_CHECK_ERR(e);
			Four index = 0;
			while((e = oosql_SQL_Next(m_oosqlSystemHandle, paramQueryHandle, 
				                      rowType, parmName, typeSchema, typeName, &typeID,
									  &length, &scale, &codePage, &castFuncID, asLocator)) != eNORESULTS_OOSQL)
			{
				OOSQL_ExternalFunctionParameterInfo paramInfo;

				strcpy(paramInfo.m_argumentName, parmName);
				paramInfo.m_argumentType = (TypeID)typeID;
				strcpy(paramInfo.m_argumentTypeName, typeName);
				paramInfo.m_argumentTypeLength = length;
				paramInfo.m_parameterMode = PARM_IN;
				if(toupper(asLocator[0]) == 'Y')
					paramInfo.m_asLocator = SM_TRUE;
				else
					paramInfo.m_asLocator = SM_FALSE;
				
				functionInfo.m_parameters[index] = paramInfo;
				index ++;
			}
			if(e != eNORESULTS_OOSQL) OOSQL_CHECK_ERR(e);
			e = oosql_SQL_Final(m_oosqlSystemHandle, paramQueryHandle);
			OOSQL_CHECK_ERR(e);

			m_functionInfos.add(functionInfo);
		}
		if(e != eNORESULTS_OOSQL) OOSQL_CHECK_ERR(e);
		e = oosql_SQL_Final(m_oosqlSystemHandle, funcQueryHandle);
		OOSQL_CHECK_ERR(e);

		pMemoryManager->Free(funcSchema);
		pMemoryManager->Free(funcName);
		pMemoryManager->Free(specificName);
		pMemoryManager->Free(definer);
		pMemoryManager->Free(paramSignature);
		pMemoryManager->Free(temp);
		pMemoryManager->Free(implementation);
		pMemoryManager->Free(sourceSchema);
		pMemoryManager->Free(sourceSpecific);
		pMemoryManager->Free(parmName);
		pMemoryManager->Free(typeSchema);
		pMemoryManager->Free(typeName);
	}

	return eNOERROR;
}

Four OOSQL_ExternalFunctionManager::Invalidate(Four volID, char* functionName)
{
	Four i;

	for(i = 0; i < m_functionInfos.numberOfItems();)
	{
		if(m_functionInfos[i].m_volID == volID && m_functionInfos[i].m_functionName == functionName)
			m_functionInfos.remove(i);
		else
			i++;
	}

	return eNOERROR;
}
