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

#ifndef _OOSQL_SERVERQUERY_HXX_
#define _OOSQL_SERVERQUERY_HXX_

/*
    MODULE:
        OOSQL_ServerQuery.hxx

    DESCRIPTION:
        This header file defines OOSQL_ServerQuery class which acts as the interfaces for
        OOSQL query processor.

*/



#include "OOSQL_Common.h"             // common header file
#include "OOSQL_Compiler.hxx"
#include "OOSQL_Evaluator.hxx"
#include "OOSQL_MemoryManager.hxx"
#include "OOSQL_StorageManager.hxx"
#include "OOSQL_ErrorMessage.hxx"

// constant definitions for OOSQL Server status
#define OOSQL_NOTREADY          0       // NOT READY to execute
#define OOSQL_READY             1       // READY to execute
#define OOSQL_COMPILED          2       // EXECUTED the input query
#define OOSQL_FETCHING          3       // FETCHING the query results
#define OOSQL_FETCHEDALL        4       // FETCHED ALL query results

#define OOSQL_EXECUTE_ALL		-1		

/* forward class declaration */
class   OOSQL_AccessPlan;
class   OOSQL_Compiler;
class   OOSQL_Evaluator;
class	OOSQL_ExternalFunctionManager;
class	OOSQL_ExternalFunctionDispatcher;

class OOSQL_ServerQuery {
    // transaction informations
    OOSQL_StorageManager::XactID		m_transID;		// transaction ID : reserved 
    Four								m_volID;			// database ID 

    // OOSQL processor status flag
    Four								m_status;		// OOSQL Server status flag
    Four								m_queryKind;	// kind of query

    // data structures trasferred from the compiler to the evaluator
    OOSQL_AccessPlan*					m_accessPlan;   // pointer to access plan

    // sub-modules of OOSQL processor
    OOSQL_Compiler*						m_compiler;     // OOSQL Query Compiler
    OOSQL_Evaluator*					m_evaluator;    // pointer to OOSQL evalauator
	OOSQL_QueryType						m_queryType;
	OOSQL_ExternalFunctionManager*		m_externalFunctionManager;
	OOSQL_ExternalFunctionDispatcher*	m_externalFunctionDispatcher;

	// stroage manager and it's catalog
	OOSQL_StorageManager*				m_storageManager;
	OOSQL_Catalog*						m_catalog;
	LOM_Handle*							m_systemHandle;
	OOSQL_SystemHandle*					m_oosqlSystemHandle;
	
	// error message holder
	OOSQL_ErrorMessage*					m_errorMessage;

public:
	OOSQL_MemoryManager*				pMemoryManager;

public:
    OOSQL_ServerQuery(OOSQL_ExternalFunctionManager* externalFunctionManager, OOSQL_ExternalFunctionDispatcher* externalFunctionDispatcher);
    virtual ~OOSQL_ServerQuery();

    Four    CreateQuery(OOSQL_SystemHandle* oosqlSystemHandle, LOM_Handle* systemHandle, Four volumeID);
    Four    DestroyQuery();

	Four	Prepare(char* stmtText, OOSQL_SortBufferInfo* sortBufferInfo);
	Four	Execute(Four nEvaluationsToDo, void* headerBuffer, Four headerBufferSize, void* dataBuffer, Four dataBufferSize, Four* nEvaluationsDone);

	Four	GetData(Two columnNumber, Four startPos, void *columnValuePtr, Four bufferLength, Four *dataLength);
	Four	GetMultiColumnData(Four nColumns, OOSQL_GetDataStruct* getDataStruct);	
	Four	GetMultipleResult(Four nResultsToRead, void* headerBuffer, Four headerBufferSize, void* dataBuffer, Four dataBufferSize, Four* nResultsRead);
	Four	PutData(Two paramNumber, Four startPos, void *columnValuePtr, Four bufferLength);
	Four	GetOID(Two targetNo, OOSQL_StorageManager::OID& oid);

	Four	GetResultColName(Two columnNumber, char *columnName, Four bufferLength);
	Four	GetResultColType(Two columnNumber, Four* type);
	Four	GetResultColLength(Two columnNumber, Four* resultColLength);
	Four	GetPutDataParamType(Two paramNumber, Four* type);
	Four	GetNumResultCols(Two *nCols);
	Four	GetErrorMessage(char *messageBuffer, Four bufferLength);
	Four	GetComplexTypeInfo(Two columnNo, OOSQL_ComplexTypeInfo* complexTypeInfo);
	Four    GetClassName(Two targetNo, char* className, Four bufferLength);
	Four	EstimateNumResults(Four* nResults);
	Four	DumpPlan(void* outBuffer, int outBufferLength);
	OOSQL_QueryType GetQueryType() { return m_queryType; }

	Four	GetVolumeID() { return m_volID; }
};


#endif 

