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

#include "OOSQL_StorageSystemHeaders.h"
#include "OOSQL_ServerQuery.hxx"
#include "OOSQL_AccessPlan.hxx"
#include "OOSQL_Compiler.hxx"
#include "OOSQL_Evaluator.hxx"
#include "OOSQL_Error.h"
#include <string.h>

#ifdef COSMOS_MULTITHREAD
cosmos_thread_mutex_t mutexVar2 = COSMOS_THREAD_MUTEX_INIT_FOR_INTRAPROCESS;
#endif


/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OOSQL_ServerQuery::OOSQL_ServerQuery(OOSQL_ExternalFunctionManager* externalFunctionManager, 
									 OOSQL_ExternalFunctionDispatcher* externalFunctionDispatcher)
{
	m_status						= OOSQL_NOTREADY;
	m_accessPlan					= NULL;
	m_compiler						= NULL;
	m_evaluator						= NULL;
	pMemoryManager					= NULL;
	m_storageManager				= NULL;
	m_catalog						= NULL;
	m_errorMessage					= NULL;
	m_externalFunctionManager		= externalFunctionManager;
	m_externalFunctionDispatcher	= externalFunctionDispatcher;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OOSQL_ServerQuery::~OOSQL_ServerQuery()
{
	DestroyQuery();
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eOUTOFMEMORY_OOSQL  
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OOSQL_ServerQuery::CreateQuery(OOSQL_SystemHandle* oosqlSystemHandle, LOM_Handle* systemHandle, Four volumeId)
{
	Four	e;		// error code

	m_volID   = volumeId;
	m_status  = OOSQL_NOTREADY;

	// access plan, m_compiler, m_evaluator
	m_accessPlan   = NULL;
	m_compiler     = NULL;
	m_evaluator    = NULL;
	m_systemHandle = systemHandle;
	m_oosqlSystemHandle = oosqlSystemHandle;

	// init memory manager
	if(pMemoryManager == NULL)
	{
#ifndef _LP64
        pMemoryManager = new OOSQL_QuickFitMemoryManager;	
#else
		pMemoryManager = new OOSQL_MemoryManager;			
#endif		
		if(pMemoryManager == NULL)
			OOSQL_ERR(eOUTOFMEMORY_OOSQL);
	}

	// create storage manager object and catalog object
#ifdef SLIMDOWN_OPENGIS
	OOSQL_NEW(m_storageManager, pMemoryManager, OOSQL_StorageManagerLOM(m_oosqlSystemHandle, m_systemHandle));
#else /* SLIMDOWN_OPENGIS */
	OOSQL_NEW(m_storageManager, pMemoryManager, OOSQL_StorageManagerGEO(m_oosqlSystemHandle, m_systemHandle));
#endif /* SLIMDOWN_OPENGIS */
	OOSQL_NEW(m_catalog, pMemoryManager, OOSQL_Catalog(m_systemHandle, m_volID));

	OOSQL_NEW(m_errorMessage, pMemoryManager, OOSQL_ErrorMessage);

	return eNOERROR;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OOSQL_ServerQuery::DestroyQuery()
{
	Four e;

	// free resource
	if(m_evaluator)
	{
		e = m_evaluator->End();			
		OOSQL_CHECK_ERR(e);
	    OOSQL_DELETE(m_evaluator);		
	}

	if(m_accessPlan) 
	    OOSQL_DELETE(m_accessPlan);

	if(m_compiler) 
		OOSQL_DELETE(m_compiler);		

	if(m_storageManager)
		OOSQL_DELETE(m_storageManager);

	if(m_catalog)
		OOSQL_DELETE(m_catalog);

	if(m_errorMessage)
		OOSQL_DELETE(m_errorMessage);

	if(pMemoryManager)
		delete pMemoryManager;

	pMemoryManager = NULL;

	// init m_status
	m_status			= OOSQL_NOTREADY;
	m_accessPlan		= NULL;
	m_compiler			= NULL;
	m_evaluator			= NULL;
	m_storageManager	= NULL;
	m_catalog			= NULL;
	m_errorMessage      = NULL;
	m_errorMessage      = NULL;

	return eNOERROR;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
ePREPARATION_FAILED_OOSQL 
eNEEDMORESORTBUFFERMEMORY_OOSQL 
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OOSQL_ServerQuery::Prepare(char* stmtText, OOSQL_SortBufferInfo* sortBufferInfo)
{
	Four	e;		// error code
	Four	_e;

	// check if Execute() is called since this instance has been created
	if(m_status == OOSQL_NOTREADY) 
	{
	    // create new OOSQL m_compiler
		OOSQL_NEW(m_compiler, pMemoryManager, OOSQL_Compiler(m_volID, m_storageManager, m_catalog, m_errorMessage, m_externalFunctionManager));

	    // create new OOSQL m_evaluator
		m_transID.high = 0;
		m_transID.low  = 0;
		OOSQL_NEW(m_evaluator, pMemoryManager, OOSQL_Evaluator(m_storageManager, m_catalog, m_errorMessage, 
			                                                   m_externalFunctionManager, m_externalFunctionDispatcher,
															   m_transID, m_volID));

	    // initialize OOSQL m_status
	    m_status = OOSQL_READY;
	}
	else
	{
		if(m_evaluator)
		{
			Four e2 = m_evaluator->End();	
			OOSQL_CHECK_ERR(e2);
			OOSQL_DELETE(m_evaluator);
		}
		m_evaluator = NULL;
		m_transID.high = 0;
		m_transID.low  = 0;
		OOSQL_NEW(m_evaluator, pMemoryManager, OOSQL_Evaluator(m_storageManager, m_catalog, m_errorMessage, 
			                                                   m_externalFunctionManager, m_externalFunctionDispatcher,
															   m_transID, m_volID));

		e = m_compiler->ReInit();
		if(e < eNOERROR) OOSQL_ERR(e);

		if(m_accessPlan) 
		    OOSQL_DELETE(m_accessPlan);
		m_accessPlan = NULL;
	}

	// compile query: 
	//	1. syntactic analysis
	//	2. semantic analysis & global data structure generation
	//	3. access plan generation
	// step 1. do syntactic analysis

#ifdef COSMOS_MULTITHREAD
    _e = cosmos_thread_mutex_lock(&mutexVar2);
    if (_e < eNOERROR) OOSQL_ERR(_e);
#endif

	e = m_compiler->parse(stmtText);
	if (e < eNOERROR) 
	{
#ifdef COSMOS_MULTITHREAD
        _e = cosmos_thread_mutex_unlock(&mutexVar2);
        if (_e < eNOERROR) OOSQL_ERR(_e);
#endif
		OOSQL_ERR(e);
	}

#ifdef COSMOS_MULTITHREAD
    _e = cosmos_thread_mutex_unlock(&mutexVar2);
    if (_e < eNOERROR) OOSQL_ERR(_e);
#endif

	// step 2. do semantic analysis and generate global data structures
	e = m_compiler->smtChkAndGenGDS();
	if (e < eNOERROR) OOSQL_ERR(e);

	// step 3. generate access plan
	e = m_compiler->genAccessPlan(m_accessPlan);
	if (e < eNOERROR) OOSQL_ERR(e);

	// change the m_status information
	m_status    = OOSQL_COMPILED;
	m_queryType = m_compiler->GetQueryType();

	// prepare evaluation
	e = m_evaluator->Prepare(m_accessPlan, m_compiler->getPool(), sortBufferInfo, m_queryType);
	if(e < eNOERROR)
	{
		if(m_evaluator)
		{
			Four e2 = m_evaluator->End();
			OOSQL_CHECK_ERR(e2);
			OOSQL_DELETE(m_evaluator);		
		}
		m_evaluator = NULL;

		if(m_accessPlan) 
		    OOSQL_DELETE(m_accessPlan);
		m_accessPlan = NULL;

		if(m_compiler) 
			OOSQL_DELETE(m_compiler);		
		m_compiler = NULL;

		m_status = OOSQL_NOTREADY;

		if(e == eNEEDMORESORTBUFFERMEMORY_OOSQL)
			return eNEEDMORESORTBUFFERMEMORY_OOSQL;
#ifdef eVOLUMELOCKBLOCK						// defined in only COSMOS-single
		else if(e == eVOLUMELOCKBLOCK)
			return eVOLUMELOCKBLOCK_OOSQL;
#endif
#ifdef eDEADLOCK							// defined in only COSMOS-multi
		else if(e == eDEADLOCK)
			return eDEADLOCK;
#endif
		else
			OOSQL_ERR(ePREPARATION_FAILED_OOSQL);
	}

    #ifndef SLIMDOWN_OPENGIS
    #ifdef ENABLE_OPENGIS_OPTIMIZATION
    e = GEO_PrepareQuery(pMemoryManager);
    OOSQL_CHECK_ERR(e);
    #endif
    #endif

	return eNOERROR;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eNOTCOMPILED_OOSQL  
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OOSQL_ServerQuery::Execute(Four nEvaluationsToDo, void* headerBuffer, Four headerBufferSize, void* dataBuffer, Four dataBufferSize, Four* nEvaluationsDone)
{
	Four e;

	if(m_status != OOSQL_COMPILED)
		OOSQL_ERR(eNOTCOMPILED_OOSQL);

	e = m_evaluator->EvalAndFetch(nEvaluationsToDo, headerBuffer, headerBufferSize, dataBuffer, dataBufferSize, nEvaluationsDone);
	if (e < eNOERROR) OOSQL_ERR(e);

	return e;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eNOTCOMPILED_OOSQL  
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OOSQL_ServerQuery::GetData(Two columnNumber, Four startPos, void *bufferPtr, Four bufferLength, Four *returnLength)
{
	Four e;

	if(m_status != OOSQL_COMPILED)
		OOSQL_ERR(eNOTCOMPILED_OOSQL);

	e = m_evaluator->GetData(columnNumber, startPos, bufferPtr, bufferLength, returnLength);
	if (e < eNOERROR) OOSQL_ERR(e);

	return eNOERROR;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eNOTCOMPILED_OOSQL  
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OOSQL_ServerQuery::GetMultiColumnData(Four nColumns, OOSQL_GetDataStruct* getDataStruct)
{
	Four e;

	if(m_status != OOSQL_COMPILED)
		OOSQL_ERR(eNOTCOMPILED_OOSQL);

	e = m_evaluator->GetMultiColumnData(nColumns, getDataStruct);
	if (e < eNOERROR) OOSQL_ERR(e);

	return eNOERROR;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eNOTCOMPILED_OOSQL  
< eNOERROR         

IMPLEMENTATION:
****************************************************************************/
Four OOSQL_ServerQuery::PutData(Two paramNumber, Four startPos, void *columnValuePtr, Four bufferLength)
{
	Four e;

	if(m_status != OOSQL_COMPILED)
		OOSQL_ERR(eNOTCOMPILED_OOSQL);

	e = m_evaluator->PutData(paramNumber, startPos, columnValuePtr, bufferLength);
	if (e < eNOERROR) OOSQL_ERR(e);

	return eNOERROR;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eNOTCOMPILED_OOSQL 
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OOSQL_ServerQuery::GetResultColName(Two columnNumber, char *columnName, Four bufferLength)
{
	// check if the current m_status is valid to get query result info.
	if(m_status != OOSQL_COMPILED)
		OOSQL_ERR(eNOTCOMPILED_OOSQL);

	return m_compiler->GetResultColName(columnNumber, columnName, bufferLength);
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eNOTCOMPILED_OOSQL  
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OOSQL_ServerQuery::GetResultColType(Two columnNumber, Four* type)
{
	if(m_status != OOSQL_COMPILED)
		OOSQL_ERR(eNOTCOMPILED_OOSQL);

	return m_compiler->GetResultColType(columnNumber, type);
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eNOTCOMPILED_OOSQL  
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OOSQL_ServerQuery::GetPutDataParamType(Two paramNumber, Four* type)
{
	Four e;

	if(m_status != OOSQL_COMPILED)
		OOSQL_ERR(eNOTCOMPILED_OOSQL);

	e = m_evaluator->GetPutDataParamType(paramNumber);
	OOSQL_CHECK_ERR(e);

	*type = e;
	return eNOERROR;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eNOTCOMPILED_OOSQL  
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OOSQL_ServerQuery::GetResultColLength(Two columnNumber, Four* resultColLength)
{
	Four	e;

	if(m_status != OOSQL_COMPILED)
		OOSQL_ERR(eNOTCOMPILED_OOSQL);

	e = m_evaluator->GetLength(columnNumber);
	if (e < eNOERROR) OOSQL_ERR(e);

	*resultColLength = e;

	return eNOERROR;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eNOTCOMPILED_OOSQL  
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OOSQL_ServerQuery::GetNumResultCols(Two *nCols)
{
	if(m_status != OOSQL_COMPILED)
		OOSQL_ERR(eNOTCOMPILED_OOSQL);

	return m_compiler->GetNumResultCols(nCols);
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OOSQL_ServerQuery::GetErrorMessage(char *messageBuffer, Four bufferLength)
{
	int  messageLength;

	memset(messageBuffer, 0, bufferLength);

	if(!m_errorMessage)
		return eNOERROR;

	if(bufferLength > strlen(m_errorMessage->ErrorMessage()))
		messageLength = strlen(m_errorMessage->ErrorMessage());
	else
		messageLength = bufferLength;
		
	memcpy(messageBuffer, m_errorMessage->ErrorMessage(), messageLength);
	messageBuffer[bufferLength - 1] = 0;

	return eNOERROR;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eNOTCOMPILED_OOSQL  
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OOSQL_ServerQuery::GetOID(Two targetNo, OOSQL_StorageManager::OID& oid)
{
	Four e;

	if(m_status != OOSQL_COMPILED)
		OOSQL_ERR(eNOTCOMPILED_OOSQL);

	e = m_evaluator->GetOID(targetNo, oid);
	if (e < eNOERROR) OOSQL_ERR(e);

	return eNOERROR;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eNOTCOMPILED_OOSQL  
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OOSQL_ServerQuery::DumpPlan(void* outBuffer, int outBufferSize)
{
	if(m_status != OOSQL_COMPILED)
		OOSQL_ERR(eNOTCOMPILED_OOSQL);

	// dump access plan and pools
	// OQL_OutStream outstream;				// to use this, print into stdout
	OQL_StringOutStream outstream;			// to use this, print into string buffer
	outstream << *m_compiler;
	if(outBufferSize > 1)
		strncpy((char*)outBuffer, outstream.GetString(), outBufferSize - 1);
	else if(outBufferSize == 1)
		strcpy((char*)outBuffer, "");
	return eNOERROR;
}

Four OOSQL_ServerQuery::GetComplexTypeInfo(Two columnNo, OOSQL_ComplexTypeInfo* complexTypeInfo)
{
	Four e;

	if(m_status != OOSQL_COMPILED)
		OOSQL_ERR(eNOTCOMPILED_OOSQL);

	e = m_evaluator->GetComplexTypeInfo(columnNo, complexTypeInfo);
	if (e < eNOERROR) OOSQL_ERR(e);

	return eNOERROR;
}

Four OOSQL_ServerQuery::GetClassName(Two targetNo, char* className, Four bufferLength)
{
	Four e;

	if(m_status != OOSQL_COMPILED)
		OOSQL_ERR(eNOTCOMPILED_OOSQL);

	return m_compiler->GetClassName(targetNo, className, bufferLength);
}


Four OOSQL_ServerQuery::EstimateNumResults(Four* nResults)
{
	Four 	e;

	if (m_status != OOSQL_COMPILED)
		OOSQL_ERR(eNOTCOMPILED_OOSQL);

	e = m_evaluator->EstimateNumResults(nResults);
	if (e < eNOERROR) OOSQL_ERR(e);

	return eNOERROR;
}
