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

#ifndef __OOSQL_Compiler_hxx__
#define __OOSQL_Compiler_hxx__

#include "OOSQL_Common.h"
#include "OOSQL_MemoryManager.hxx"
#include "OOSQL_MemoryManagedObject.hxx"
#include "OOSQL_ErrorMessage.hxx"
#include "OQL_OutStream.hxx"

/*
    MODULE:
        OOSQL_Compiler.hxx

    DESCRIPTION:
        This header defines OOSQL query compilation module.
        Query compilation includes the following steps.
            1. lexical & syntactic analysis
            2. semantic analysis & global data structure generation
            3. query optimization & access plan generation

        NOTE: Now this file defines null class for testing.
*/

class OQL_AST;
class OQL_GDS;
class OQL_GDSPOOL;
class OQL_ASTtoGDS;
class OQL_AST_Util;
class OQL_CommonAP;
class OQL_GDStoCommonAP;
class OOSQL_Catalog;
class OOSQL_StorageManager;
class OOSQL_AccessPlan;
class OOSQL_ExternalFunctionManager;

// constant definitions for the kind of input query
#ifndef OOSQL_QueryType
enum OOSQL_QueryType {
	OOSQL_UNKNOWN_QUERY,
	OOSQL_SELECT_QUERY,				// SELECT statement
	OOSQL_INSERT_QUERY,				// INSERT statement
	OOSQL_INSERT_QUERY_WITH_PARAM,	// INSERT statement with parameter
	OOSQL_DELETE_QUERY,				// DELETE statement
	OOSQL_UPDATE_QUERY,				// UPDATE statement
	OOSQL_UPDATE_QUERY_WITH_PARAM,	// UPDATE statement with parameter
	OOSQL_ALTER_TABLE_QUERY,		// ALTER TABLE statement
	OOSQL_CREATE_SEQUENCE_QUERY,	// CREATE SEQUENCE statement
	OOSQL_DROP_SEQUENCE_QUERY,		// DROP SEQUENCE statement
	OOSQL_CREATE_TABLE_QUERY,		// CREATE TABLE statement
	OOSQL_DROP_TABLE_QUERY,			// DROP   TABLE statement
	OOSQL_CREATE_INDEX_QUERY,		// CREATE INDEX statement
	OOSQL_DROP_INDEX_QUERY,			// DROP   INDEX statement
	OOSQL_CREATE_VIEW_QUERY,		// reserved for future extension
	OOSQL_CREATE_FUNCTION_QUERY,
	OOSQL_DROP_FUNCTION_QUERY,
	OOSQL_CREATE_PROCEDURE_QUERY,
	OOSQL_DROP_PROCEDURE_QUERY,
	OOSQL_CALL_PROCEDURE_QUERY
};
#endif

class   OOSQL_Compiler : public OOSQL_MemoryManagedObject {
private:
    enum CompilerStatus {STANDBY, STR_TO_AST, AST_TO_GDS, GDS_TO_PLAN};
    CompilerStatus      m_status;

    ASTNodeIdx          m_astRoot;
    
	Four                m_volId;
    OQL_AST*			m_ast;
    OQL_GDS*			m_gds;
    OQL_GDSPOOL*		m_pool;
    OQL_ASTtoGDS*		m_astToGds;
    OQL_AST_Util*		m_astUtil;
    OQL_CommonAP*		m_commonAP;
    OQL_GDStoCommonAP*	m_gdsToCommonAP;
    OOSQL_Catalog*		m_catalog;
    OOSQL_StorageManager* m_storageManager;
	Four				m_nResultColumns;
	Four*				m_resultTypes;
	char**				m_resultNames;
	OOSQL_ErrorMessage* m_errorMessage;
	OOSQL_ExternalFunctionManager* m_externalFunctionManager;

public:
    OOSQL_Compiler(Four volId, OOSQL_StorageManager* storageManager, OOSQL_Catalog* catalog, OOSQL_ErrorMessage* errorMessage, OOSQL_ExternalFunctionManager* externalFunctionManager);
    virtual ~OOSQL_Compiler();
    
    Four    parse(char *queryStr);

    Four    smtChkAndGenGDS();
    
    Four    genAccessPlan(OOSQL_AccessPlan* &);

	OQL_GDSPOOL& getPool() { return *m_pool; }

	friend OQL_OutStream& operator<<(OQL_OutStream& os, OOSQL_Compiler& object);

	OOSQL_QueryType GetQueryType();

	Four	ReInit();

	Four	GetResultColName(Two columnNumber, char *columnName, Four bufferLength);
	Four	GetResultColType(Two columnNumber, Four* type);
	Four	GetNumResultCols(Two *nCols);
	Four	GetClassName(Two targetNo, char* className, Four bufferLength);
private:
    // Utility functions
    Four    selectClause_GetN_Terms();
    Four    selectClause_GetIthTerm(Four ith, Four stringBufferSize,
                                    char* stringBuffer,
                                    Four* usedStringBufferSize);
    Four	destroyObjects();
};

#endif  





