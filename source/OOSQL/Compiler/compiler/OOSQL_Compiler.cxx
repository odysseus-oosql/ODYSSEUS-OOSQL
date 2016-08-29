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

#include <stdio.h>
#include <locale.h>


#include "OQL_AST.hxx"
#include "OQL_GDS.hxx"
#include "OQL_ASTtoGDS.hxx"
#include "OQL_AST_Util.hxx"
#include "OQL_CommonAP.hxx"
#include "OQL_GDStoCommonAP.hxx"
#include "OOSQL_Catalog.hxx"
#include "OQL_OutStream.hxx"

#include "OOSQL_Compiler.hxx"
#include "OOSQL_AccessPlan.hxx"

// routines in c_parser
extern "C" {
    extern int c_parser(char *str,ASTNodeIdx *node);
    extern int free_parser();
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OOSQL_Compiler::OOSQL_Compiler(
	Four                            volId,
	OOSQL_StorageManager*			storageManager, 
	OOSQL_Catalog*					catalog, 
	OOSQL_ErrorMessage*				errorMessage,
	OOSQL_ExternalFunctionManager*	externalFunctionManager)
{
    // initialize m_status
    m_status = STANDBY;

    // initialize objects for processing AST, GDS, PLAN...
	m_volId             = volId;
    m_ast 				= NULL;
    m_gds 				= NULL;
    m_pool 				= NULL;
    m_astToGds 			= NULL;
    m_astUtil			= NULL;
    m_commonAP 			= NULL;
    m_gdsToCommonAP		= NULL;
    m_catalog			= catalog;
    m_storageManager    = storageManager;
	m_nResultColumns	= 0;
	m_resultTypes		= NULL;
	m_resultNames		= NULL;
	m_errorMessage      = errorMessage;
	m_externalFunctionManager = externalFunctionManager;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OOSQL_Compiler::~OOSQL_Compiler()
{
    destroyObjects();
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OOSQL_Compiler::destroyObjects()
{
	Four	i;

    if(m_ast)
        OOSQL_DELETE(m_ast);
    if(m_gds)
        OOSQL_DELETE(m_gds);
    if(m_pool)
        OOSQL_DELETE(m_pool);
    if(m_astToGds)
        OOSQL_DELETE(m_astToGds);
	if(m_astUtil)
        OOSQL_DELETE(m_astUtil);
    if(m_commonAP)
        OOSQL_DELETE(m_commonAP);
    if(m_gdsToCommonAP)
        OOSQL_DELETE(m_gdsToCommonAP);

    if(m_resultTypes)
		pMemoryManager->Free(m_resultTypes);

	if(m_resultNames)
	{
		for(i = 0; i < m_nResultColumns; i++)
			pMemoryManager->Free(m_resultNames[i]);
		pMemoryManager->Free(m_resultNames);
	}

    m_ast 				= NULL;
    m_gds 	       		= NULL;
    m_pool 	       		= NULL;
    m_astToGds 			= NULL;
    m_astUtil			= NULL;
    m_commonAP 			= NULL;
    m_gdsToCommonAP 	= NULL;
	m_nResultColumns	= 0;
	m_resultTypes		= NULL;
	m_resultNames		= NULL;
    
    return eNOERROR;
}


// c_parser constructs int, real, string, string index m_pool, and m_ast tree
extern int* *strIndex;
extern int* strIdxTop;                  /* top of string index table    */

extern char* *strPool;
extern int* stringTop;                  /* top of string table          */

extern long* *intPool;
extern int* intTop;                     /* top of table                 */

extern float* *realPool;
extern int* realTop;                    /* top of table                 */

extern "C" {	// DEFINED IN OQL.lex.l
	extern int   LINE_NUM;
	extern int   COLM_NUM;
	extern char* yytext;
}

extern "C" {	// DEFINED IN OQL_AST.c
	extern int astNodeTop;		
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OOSQL_Compiler::parse(
	char* queryStr		// IN 
)
{
	Four e;

    m_status = STR_TO_AST;

    // initialize pointer 
    destroyObjects();

    // parse string
    e = c_parser(queryStr, &m_astRoot);
	if(e == ePARSE_ERROR_OOSQL)
	{
		m_errorMessage->Clear();
		m_errorMessage->Append("Parsing error is occurred at column ");
		m_errorMessage->Append((Four)COLM_NUM);
		m_errorMessage->Append(", line ");
		m_errorMessage->Append((Four)LINE_NUM);

		free_parser();

		return e;
	}
	else
	{
		if(e < eNOERROR) 
		{
			free_parser();
	
			OOSQL_ERR(e);
		}
	}

    // construct obejcts
	OOSQL_NEW(m_ast, pMemoryManager, OQL_AST(*intPool, *intTop + 1,
										     *realPool, *realTop + 1,
										     *strPool, *stringTop + 1,
										     *strIndex, *strIdxTop + 1,
											 astNodePool, astNodeTop + 1));
	OOSQL_NEW(m_gds, pMemoryManager, OQL_GDS);
	OOSQL_NEW(m_pool, pMemoryManager, OQL_GDSPOOL);
	OOSQL_NEW(m_astToGds, pMemoryManager, OQL_ASTtoGDS(*m_ast, *m_gds, *m_pool, m_volId, *m_storageManager, *m_catalog, *m_errorMessage, *m_externalFunctionManager));
	OOSQL_NEW(m_astUtil, pMemoryManager, OQL_AST_Util(*m_ast, *m_pool));
	OOSQL_NEW(m_commonAP, pMemoryManager, OQL_CommonAP);
	OOSQL_NEW(m_gdsToCommonAP, pMemoryManager, OQL_GDStoCommonAP(*m_gds, *m_pool, *m_catalog, *m_commonAP, *m_errorMessage));

	free_parser();

    return eNOERROR;
}


/****************************************************************************
DESCRIPTION:

RETURN VALUE:
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OOSQL_Compiler::smtChkAndGenGDS()
{
    Four e;

    // m_status check and transition
    if(m_status != STR_TO_AST)
        return -1; // some valid error code....
    else
        m_status = AST_TO_GDS;

    // do semantic check
    e = m_astToGds->smtChk(m_astRoot);
    OOSQL_CHECK_ERR(e);

    return eNOERROR;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OOSQL_Compiler::genAccessPlan(
	OOSQL_AccessPlan*& oosql_accessPlan		// OUT Access Plan
)
{
    Four 	      					e;
    CommonAP_PoolElements			accessPlan(m_pool->commonAP_Pool);
	Four							i;
	AP_ProjectionListPoolElements	projList;
	AP_ProjectionPoolElements		projection;
	char							stringBuffer[OOSQL_EVALBUFFER_MAXSTRINGSIZE];
	Four							usedSize;
	Four							lastPlanNo;

    // m_status check and transition
    if(m_status != AST_TO_GDS)
        return -1; // some valid error code....
    else
        m_status = GDS_TO_PLAN;
	
    // generate access plan
    e = m_gdsToCommonAP->genAccessPlan();
    OOSQL_CHECK_ERR(e);

	// get query result informations
	// get # of result cols, type of result cols, name of result cols
	// get # of resultcols
	lastPlanNo = m_pool->commonAP_Pool.nElements() - 1;
	if(lastPlanNo >= 0 && m_gds->queryType == OQL_GDS::SELECT_QUERY)
	{
		projList         = m_pool->commonAP_Pool[lastPlanNo].projectionList.getElements(m_pool->projectionListPool);
		projection       = projList[0].projectionInfo.getElements(m_pool->projectionPool);

		m_nResultColumns = projection.size - m_pool->targetListPool.nElements();
	}
	else
	{
		m_nResultColumns = 0;
	}

	if(m_nResultColumns > 0)	
	{
		// allocate memory
		m_resultTypes = (Four*)pMemoryManager->Alloc(sizeof(Four) * m_nResultColumns);
		if(m_resultTypes == NULL)
			OOSQL_ERR(eMEMORYALLOCERR_OOSQL);
		m_resultNames = (char**)pMemoryManager->Alloc(sizeof(char*) * m_nResultColumns);
		if(m_resultNames == NULL)
			OOSQL_ERR(eMEMORYALLOCERR_OOSQL);

		// get type and name information
		for(i = 0; i < m_nResultColumns; i++)
		{
			m_resultTypes[i] = (Four)projection[i].resultType;
			e = selectClause_GetIthTerm(i, sizeof(stringBuffer), stringBuffer, &usedSize);
			OOSQL_CHECK_ERR(e);
			
			m_resultNames[i] = (char*)pMemoryManager->Alloc(usedSize);
			if(m_resultNames[i] == NULL)
				OOSQL_ERR(eMEMORYALLOCERR_OOSQL);
			strcpy(m_resultNames[i], stringBuffer);
		}
	}

    // convert m_commonAP to oosql_accessPlan
    // create OOSQL_AccessPlan object
    accessPlan = m_commonAP->commonAP;
    OOSQL_NEW(oosql_accessPlan, pMemoryManager, OOSQL_AccessPlan(accessPlan, m_storageManager, m_catalog,
                         &m_pool->ap_condListPool, m_pool->ap_condListPool.nElements(),
                         &m_pool->ap_exprPool, m_pool->ap_exprPool.nElements(),
                         &m_pool->subClassPool, m_pool->subClassPool.nElements(),
                         &m_pool->ap_aggrFuncPool, m_pool->ap_aggrFuncPool.nElements(),
                         &m_pool->colNoMapPool, m_pool->colNoMapPool.nElements(),
                         &m_pool->usedColPool, m_pool->usedColPool.nElements(),
                         &m_pool->methodNoMapPool, m_pool->methodNoMapPool.nElements(),
                         &m_pool->usedMethodPool, m_pool->usedMethodPool.nElements(),
                         &m_pool->projectionListPool, m_pool->projectionListPool.nElements(),
                         &m_pool->projectionPool, m_pool->projectionPool.nElements(),
                         &m_pool->ap_argumentPool, m_pool->ap_argumentPool.nElements(),
                         &m_pool->valuePool, m_pool->valuePool.nElements(),
                         &m_pool->intPool, m_pool->intPool.nElements(),
                         &m_pool->realPool, m_pool->realPool.nElements(),
                         &m_pool->stringPool, m_pool->stringPool.nElements(),
                         &m_pool->funcPool, m_pool->funcPool.nElements(),
                         &m_pool->collectionPool, m_pool->collectionPool.nElements(),
                         &m_pool->domainPool, m_pool->domainPool.nElements(),
                         &m_pool->mbrPool, m_pool->mbrPool.nElements(),
                         &m_pool->memberPool, m_pool->memberPool.nElements(),
                         &m_pool->indexInfoPool, m_pool->indexInfoPool.nElements(),
                         &m_pool->textIndexCondPool, m_pool->textIndexCondPool.nElements(),
                         &m_pool->datePool, m_pool->datePool.nElements(),
                         &m_pool->timePool, m_pool->timePool.nElements(),
                         &m_pool->timestampPool, m_pool->timestampPool.nElements(),
                         &m_pool->intervalPool, m_pool->intervalPool.nElements(),
						 &m_pool->ap_insertValuePool, m_pool->ap_insertValuePool.nElements(),
						 &m_pool->ap_updateValuePool, m_pool->ap_updateValuePool.nElements()
                         ));    
    return eNOERROR;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
Four OOSQL_Compiler::selectClause_GetN_Terms()
{
	if(m_gds->queryType == OQL_GDS::SELECT_QUERY && m_pool->selectQueryPool[0].selListType & STAR_BIT)
	{
		return m_pool->selectQueryPool[0].selList.size;
	}
	else
	    return m_astUtil->select_GetN_Terms(m_astRoot);
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OOSQL_Compiler::selectClause_GetIthTerm(
	Four		ith,					// IN  
	Four		stringBufferSize,		// IN  
    char*		stringBuffer,			// OUT string buffer
    Four*		usedStringBufferSize	// OUT 
)
{
    Four e;
	SelListPoolElements		selList(m_pool->selListPool);
	PathExprPoolElements	pathExpr(m_pool->pathExprPool);
	char					attributeName[255];

    
	if(m_pool->selectQueryPool[0].selListType & STAR_BIT)
	{
		selList = m_pool->selectQueryPool[0].selList;
		pathExpr = selList[ith].pathExpr;
		e = m_catalog->attr_CataAttrInfo_to_AttrName(pathExpr[1].classInfo, 
			                                       pathExpr[1].attr.attrInfo,
												   attributeName);
		OOSQL_CHECK_ERR(e);

		if(strlen(attributeName) + 1 < stringBufferSize)
			strcpy(stringBuffer, attributeName);
		else
		{
			memcpy(stringBuffer, attributeName, stringBufferSize);
			stringBuffer[stringBufferSize - 1] = '\0';
		}
		
		*usedStringBufferSize = strlen(stringBuffer) + 1;

		return eNOERROR;
	}
	else
	{
		e = m_astUtil->select_GetIthTerm(m_astRoot, ith, stringBufferSize,
									   stringBuffer);

		if(e == eNOERROR)
		{
			*usedStringBufferSize = strlen(stringBuffer) + 1;
			return eNOERROR;
		}
		else if(e == eSTRINGBUFFER_OVERFLOW_OOSQL)
		{
    		return eINSUFFICIENT_BUFSIZE_OOSQL;
		}
		else
			return e;
	}
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
OOSQL_UNKNOWN_QUERY
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
OOSQL_QueryType OOSQL_Compiler::GetQueryType()
{
	Four i;

	if(m_pool->dbCommandPool.nElements() > 0)
	{
		switch(m_pool->dbCommandPool[0].command)
		{
		case DBCOMMAND_ALTER_TABLE:
			return OOSQL_ALTER_TABLE_QUERY;
		case DBCOMMAND_CREATE_SEQUENCE:
			return OOSQL_CREATE_SEQUENCE_QUERY;
		case DBCOMMAND_DROP_SEQUENCE:
			return OOSQL_DROP_SEQUENCE_QUERY;
		case DBCOMMAND_CREATE_TABLE:
			return OOSQL_CREATE_TABLE_QUERY;
		case DBCOMMAND_CREATE_INDEX:
			return OOSQL_CREATE_INDEX_QUERY;
		case DBCOMMAND_DROP_TABLE:
			return OOSQL_DROP_TABLE_QUERY;
		case DBCOMMAND_DROP_INDEX:
			return OOSQL_DROP_INDEX_QUERY;
		case DBCOMMAND_CREATE_FUNCTION:
			return OOSQL_CREATE_FUNCTION_QUERY;
		case DBCOMMAND_DROP_FUNCTION:
			return OOSQL_DROP_FUNCTION_QUERY;
		case DBCOMMAND_CREATE_PROCEDURE:
			return OOSQL_CREATE_PROCEDURE_QUERY;
		case DBCOMMAND_DROP_PROCEDURE:
			return OOSQL_DROP_PROCEDURE_QUERY;
		case DBCOMMAND_CALL_PROCEDURE:
			return OOSQL_CALL_PROCEDURE_QUERY;
		default:
			return OOSQL_UNKNOWN_QUERY;
		}
	}
	else
	{
		switch(m_gds->queryType)
		{
		case OQL_GDS::SELECT_QUERY:
			return OOSQL_SELECT_QUERY;
		case OQL_GDS::UPDATE_QUERY:
			for(i = 0; i < m_pool->updateValuePool.nElements(); i++)
				if(m_pool->updateValuePool[i].isParam == SM_TRUE)
					return OOSQL_UPDATE_QUERY_WITH_PARAM;
			return OOSQL_UPDATE_QUERY;
		case OQL_GDS::INSERT_QUERY:
			for(i = 0; i < m_pool->insertValuePool.nElements(); i++)
				if(m_pool->insertValuePool[i].isParam == SM_TRUE)
					return OOSQL_INSERT_QUERY_WITH_PARAM;
			return OOSQL_INSERT_QUERY;
		case OQL_GDS::DELETE_QUERY:
			return OOSQL_DELETE_QUERY;
		default:
			return OOSQL_UNKNOWN_QUERY;
		}
	}
	return OOSQL_UNKNOWN_QUERY;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OOSQL_Compiler::ReInit()
{
	destroyObjects();
	m_status = STANDBY;
	return eNOERROR;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eBADPARAMETER_OOSQL 
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OOSQL_Compiler::GetResultColName(
	Two		columnNumber,	// IN  
	char*	columnName,		// OUT 
	Four	bufferLength	// IN  
)
{
	if(columnNumber >= m_nResultColumns)
	{
		m_errorMessage->Clear();
		m_errorMessage->Append("The number of results of this query is ");
		m_errorMessage->Append(m_nResultColumns);
		m_errorMessage->Append(" but the user requests to get ");
		m_errorMessage->Append(columnNumber);
		m_errorMessage->Append("th column's name.");

		OOSQL_ERR(eBADPARAMETER_OOSQL);
	}

	if(bufferLength > strlen(m_resultNames[columnNumber]) + 1)
		strcpy(columnName, m_resultNames[columnNumber]);
	else
	{
		memcpy(columnName, m_resultNames[columnNumber], bufferLength);
		columnName[bufferLength - 1] = '\0';
	}
	return eNOERROR;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eBADPARAMETER_OOSQL 
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OOSQL_Compiler::GetResultColType(
	Two		columnNumber,	// IN  
	Four*	type			// OUT 
)
{
	if(columnNumber >= m_nResultColumns)
	{
		m_errorMessage->Clear();
		m_errorMessage->Append("The number of results of this query is ");
		m_errorMessage->Append(m_nResultColumns);
		m_errorMessage->Append(" but the user requests to get ");
		m_errorMessage->Append(columnNumber);
		m_errorMessage->Append("th column's type.");
		OOSQL_ERR(eBADPARAMETER_OOSQL);
	}

	*type = m_resultTypes[columnNumber];

	return eNOERROR;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OOSQL_Compiler::GetNumResultCols(
	Two*	nCols		// OUT 
)
{
	*nCols = (Two)m_nResultColumns;
	return eNOERROR;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
OQL_OutStream& operator<<(
	OQL_OutStream&	os,		// IN  
	OOSQL_Compiler& object	// IN  
)
{
	os << *object.m_pool;

	return os;
}

Four OOSQL_Compiler::GetClassName(Two targetNo, char* className, Four bufferLength)
{
    Four e;
	TargetListPoolElements	targetList(m_pool->targetListPool);
	PathExprPoolElements	pathExpr(m_pool->pathExprPool);
	char					classNameBuffer[255];

	if(m_pool->selectQueryPool[0].selListType)
	{
		targetList = m_pool->selectQueryPool[0].targetList;

		e = m_catalog->class_CataClassInfo_to_ClassName(targetList[targetNo].collectionInfo.classInfo, classNameBuffer);
		OOSQL_CHECK_ERR(e);

		memcpy(className, classNameBuffer, bufferLength);
		className[bufferLength - 1] = '\0';
	}
	return eNOERROR;
}
