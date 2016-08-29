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

#ifndef __OQLGDS_GlobalDataStructure_H__
#define __OQLGDS_GlobalDataStructure_H__

#include <string.h>

extern "C" {
#include "OQL.yacc.h"
#include "OQL_AST.h"
#undef AST
}
#include "OQL_GDS.hxx"
#include "OQL_GDSPOOL.hxx"
#include "OQL_AST.hxx"
#include "OOSQL_Catalog.hxx"
#include "OQL_NameStack.hxx"
#include "OQL_SimpleString.hxx"
#include "OOSQL_Common.h"
#include "OOSQL_MemoryManager.hxx"
#include "OOSQL_MemoryManagedObject.hxx"
#include "OOSQL_ErrorMessage.hxx"
#include "OOSQL_ExternalFunctionManager.hxx"

enum ProgressStatus {
    SELECT_CLAUSE, FROM_CLAUSE, WHERE_CLAUSE, 
    GROUPBY_CLAUSE, HAVING_CLAUSE, ORDERBY_CLAUSE,
	LIMIT_CLAUSE,
    NO_CALUSE
};

enum OperatorGroup {
    OG_NONE, OG_COMPARE, OG_BOOLEAN, OG_SIMPLE
};

struct ReturnType {
    TypeID          typeID;
    union {
        CataClassInfo classInfo;            // additional info : when typeID == TYPEID_OID
        TypeID        elementTypeID;        // additional info : when typeID == {TYPEID_SET, TYPEID_LIST, TYPEID_ARRAY, TYPEID_BAG}
    };
    PoolType        poolType;
    PoolIndexBase   poolIndex;

    void set(TypeID rid, PoolType pt, PoolIndexBase pi)
    {
        typeID    = rid; 
        poolType  = pt;
        poolIndex = pi;
    }
};

class QGNodeIndexArray: public OOSQL_TCArray<Four>
{
public:
	QGNodeIndexArray(OOSQL_MemoryManager* memoryManager = NULL,bool memoryManagedObjectFlag = false):OOSQL_TCArray<Four>(memoryManager, memoryManagedObjectFlag) {}

	
	void add(Four item)
	{
		for(Four i = 0; i < size(); i++)
		{
			if(data[i] == item)
				return;
		}
		OOSQL_TCArray<Four>::add(item);
	}
};
typedef Four (*StemizerFuncPtr)(char *keyword, char *stemizedKeyword);

class OQL_ASTtoGDS : public OOSQL_MemoryManagedObject {
public:
    OQL_ASTtoGDS(OQL_AST& ast, OQL_GDS& gds, OQL_GDSPOOL& pool, Four volId, OOSQL_StorageManager& storageManager, OOSQL_Catalog& catalog, OOSQL_ErrorMessage& errorMessage, OOSQL_ExternalFunctionManager& externalFunctionManager);
    virtual ~OQL_ASTtoGDS();

    Four smtChk(ASTNodeIdx root);
    Four smtChkQuDef(ASTNodeIdx node, ReturnType& returnType);
    Four smtChkQuery(ASTNodeIdx node, ReturnType& returnType, Boolean forceExprPool=SM_FALSE);
    Four smtChkQuSmp(ASTNodeIdx node, ReturnType& returnType);
    Four smtChkQuCmp(ASTNodeIdx node, ReturnType& returnType);
    Four smtChkQuBln(ASTNodeIdx node, ReturnType& returnType);
    Four smtChkQuCns(ASTNodeIdx node, ReturnType& returnType);
    Four smtChkQuAccPaex(ASTNodeIdx node, ReturnType& returnType);
    Four smtChkQuAccPaexRecursive(ASTNodeIdx node, ReturnType& returnType, CataClassInfo& lastClassInfo,
                                  PathExprPoolElements& currentPathExpr);
    Four smtChkIcltnElem(ASTNodeIdx node, ReturnType& returnType);
    Four smtChkQuAccIcltnSub(ASTNodeIdx node, ReturnType& returnType);
    Four smtChkQuAccIcltnFr(ASTNodeIdx node, ReturnType& returnType);
    Four smtChkQuAccIcltnLs(ASTNodeIdx node, ReturnType& returnType);
    Four smtChkQuCltnAll(ASTNodeIdx node, ReturnType& returnType);
    Four smtChkQuCltnEx(ASTNodeIdx node, ReturnType& returnType);
    Four smtChkQuCltnExany(ASTNodeIdx node, ReturnType& returnType);
    Four smtChkQuCltnUni(ASTNodeIdx node, ReturnType& returnType);
    Four smtChkQuCltnIn(ASTNodeIdx node, ReturnType& returnType);
    Four smtChkQuCltnCmpSome(ASTNodeIdx node, ReturnType& returnType);
    Four smtChkQuCltnCmpAny(ASTNodeIdx node, ReturnType& returnType);
    Four smtChkQuCltnCmpAll(ASTNodeIdx node, ReturnType& returnType);
    Four smtChkQuCltnAgg(ASTNodeIdx node, ReturnType& returnType);
    Four smtChkQuCltnAggDist(ASTNodeIdx node, ReturnType& returnType);
    Four smtChkQuSet(ASTNodeIdx node, ReturnType& returnType);
    Four smtChkQuCnvL2s(ASTNodeIdx node, ReturnType& returnType);
    Four smtChkQuCnvElem(ASTNodeIdx node, ReturnType& returnType);
    Four smtChkQuCnvDist(ASTNodeIdx node, ReturnType& returnType);
    Four smtChkQuCnvFlat(ASTNodeIdx node, ReturnType& returnType);
    Four smtChkQuCnvType(ASTNodeIdx node, ReturnType& returnType);
    Four smtChkID(ASTNodeIdx node, ReturnType& returnType);
	Four smtChkQuOgisTranslatableOp(ASTNodeIdx node, ReturnType& returnType);
	Four smtChkQuOgisGeometricOp(ASTNodeIdx node, ReturnType& returnType);
	Four smtChkQuOgisRelationalOp(ASTNodeIdx node, ReturnType& returnType);
	Four smtChkQuOgisMiscellaneousOp(ASTNodeIdx node, ReturnType& returnType);
	Four isOGISTypeID(TypeID typeID);
    Four smtChkQuIrFnMatch(ASTNodeIdx node, ReturnType& returnType);
    Four smtChkQuIrFnNmatch(ASTNodeIdx node, ReturnType& returnType);
    Four smtChkQuIrFnWeight(ASTNodeIdx node, ReturnType& returnType);
    Four smtChkQuAccDref(ASTNodeIdx node, ReturnType& returnType);
    Four smtChkQuAccIcltnElem(ASTNodeIdx node, ReturnType& returnType);
    Four smtChkFnOrCltnobj(ASTNodeIdx node, ReturnType& returnType);
    Four smtChkArguments(ASTNodeIdx node, ReturnType& returnType, CataClassInfo classInfo, CataMethodInfo methodInfo);
    Four smtChkValue(ASTNodeIdx node, ReturnType& returnType);
    Four smtChkTextIrExpression(ASTNodeIdx node, StemizerFuncPtr stemizer, ReturnType& returnType);
    Four smtChkTextIrOperator(ASTNodeIdx node, StemizerFuncPtr stemizer, ReturnType& returnType);
	Four smtChkValueLiteralComplex(ASTNodeIdx node, ReturnType& returnType);

    Four smtChkQuSel(ASTNodeIdx node, ReturnType& returnType);
	Four smtChkQuUpd(ASTNodeIdx node, ReturnType& returnType);
	Four smtChkQuIns(ASTNodeIdx node, ReturnType& returnType);
	Four smtChkQuDel(ASTNodeIdx node, ReturnType& returnType);
	Four smtChkQuAlterTbl(ASTNodeIdx node, ReturnType& returnType);
	Four smtChkQuCreateSeq(ASTNodeIdx node, ReturnType& returnType);
	Four smtChkQuDropSeq(ASTNodeIdx node, ReturnType& returnType);
	Four smtChkQuCreateTbl(ASTNodeIdx node, ReturnType& returnType);
	Four smtChkQuCreateIdx(ASTNodeIdx node, ReturnType& returnType);
	Four smtChkQuDropTbl(ASTNodeIdx node, ReturnType& returnType);
	Four smtChkQuDropIdx(ASTNodeIdx node, ReturnType& returnType);
	Four smtChkQuCreateFunc(ASTNodeIdx node, ReturnType& returnType);
	Four smtChkQuDropFunc(ASTNodeIdx node, ReturnType& returnType);
	Four smtChkQuCreateProc(ASTNodeIdx node, ReturnType& returnType);
	Four smtChkQuDropProc(ASTNodeIdx node, ReturnType& returnType);
	Four smtChkQuCallProc(ASTNodeIdx node, ReturnType& returnType);

    Four smtChkClSel(ASTNodeIdx node, ReturnType& returnType);
    Four smtChkClFr(ASTNodeIdx node, ReturnType& returnType);
    Four smtChkClWh(ASTNodeIdx node, ReturnType& returnType);
    Four smtChkClGrp(ASTNodeIdx node, ReturnType& returnType);
    Four smtChkClHav(ASTNodeIdx node, ReturnType& returnType);
    Four smtChkClOrd(ASTNodeIdx node, ReturnType& returnType);
	Four smtChkClLim(ASTNodeIdx node, ReturnType& returnType);

    Four smtChkInterClauseRelationship(ASTNodeIdx node, ReturnType& returnType);
    Four smtChkTextIrWeightFunction();

	Four smtChkValueLiteralDate(ASTNodeIdx node, ReturnType& returnType);
	Four smtChkValueLiteralTime(ASTNodeIdx node, ReturnType& returnType);
	Four smtChkValueLiteralTimestamp(ASTNodeIdx node, ReturnType& returnType);
	Four smtChkValueLiteralInterval(ASTNodeIdx node, ReturnType& returnType);
	Four smtChkValueText(ASTNodeIdx node, ReturnType& returnType);

	Four smtChkValueSequence(ASTNodeIdx node, ReturnType& returnType);

    static Four testCompatibility(TypeID op1, TypeID op2, Boolean &flag);
    static Four determineResultType(TypeID op1, TypeID op2, TypeID &resultType);

private:

	Four getStringPoolIndexFromSTRING_Node(StringPoolIndex& poolIndex, ASTNodeIdx node);
	Four getStringPoolIndexFromID_Node(StringPoolIndex& poolIndex, ASTNodeIdx node);
    Four makeStringFromStringPool(SimpleString&, StringPoolIndex poolIndex);
    Four getFourFromINTEGER_NODE(int& integer, ASTNodeIdx node);
    Four getFloatFromREAL_Node(float& real, ASTNodeIdx node);
    Four getStringFromSTRING_Node(SimpleString& string, ASTNodeIdx node);
    Four getStringFromID_Node(SimpleString& string, ASTNodeIdx node);

    Four isID_AliasName(ASTNodeIdx node, NameAssociatedInfo& ninfo, Boolean &flag);
    Four isID_ClassName(ASTNodeIdx node, CataClassInfo& classInfo, Boolean &flag);
    Four isID_AttributeName(ASTNodeIdx node, CataClassInfo& classInfo, CataAttrInfo& attrInfo, Boolean &flag);
    Four isID_MethodName(ASTNodeIdx node, CataClassInfo& classInfo, CataMethodInfo& methodInfo, Boolean &flag);


    Four makeOperandInExprPool(ReturnType opType, ExprPoolIndex &poolIndex);
    Four makeParameterInArgumentPool(ReturnType opType, ArgumentPoolIndex &poolIndex);
    Four makeParameterInArgument(ReturnType opType, ArgumentElement &argument);
    Four nodeNameToOperatorID(Four nodeName, OperatorID &opId);
    Four nodeNameToFunctionID(Four nodeName, FunctionID &funcId);
    Four nodeNameToAggrFunctionID(Four nodeName, AggrFunctionID &aggrFuncId);

    Four checkIfSpatialClass(CataClassInfo classInfo, Boolean &flag);

    Four qg_consInitialQG_Nodes();
    Four qg_consPathExpr(PathExprPoolIndex pathExprPoolIndex);
    Four qg_consSubClasses(QGNodePoolIndex curQGnode);
    Four qg_addNodeToRefClassList(QGNodePoolElements &node, QGNodePoolElements &addedNode);
    Four qg_addNodeToUsedMemberList(QGNodePoolElements &node, MemberPoolElements &addedNode);
    Four qg_addNodeToExplicitJoinList(QGNodePoolElements &node, JoinInfoPoolElements &addedNode);
    Four qg_addNodeToPathExprList(QGNodePoolElements &node, PathExprInfoPoolElements &addedNode);
    Four qg_addNodeToConditionList(QGNodePoolElements &node, CondListPoolElements &addedNode);
    Four qg_isExistInMemberList(QGNodePoolElements &node, PathExprElement pathExprNode, Boolean &flag);
    
    Four nodeName_to_CollectionTypeID(Four nodeName, CollectionTypeID &collectionTypeID);
    Four typeID_to_ColElementTypeID(TypeID TypeID, ColElementTypeID &colElementTypeID);
    Four collectionTypeID_to_TypeID(CollectionTypeID collectionTypeID, TypeID &typeID);

    Four setExplicitJoinLinkInQG();

    Four exprElementSetTypeInfo(ExprPoolElements &epElements);
    Four exprElementSetTypeInfo(ExprPoolElements &epElements, ReturnType& returnType);

    Four getQGnodes_traverseExprTree(ExprPoolElements& expr, QGNodeIndexArray &results);
    Four getQGnodes_argument(ArgumentPoolElements& argument, QGNodeIndexArray &results);

    Four function_GetTypeID(Four functionID);
    Four function_GetLength(Four functionID);
	
	Four getTypeID_FromSelectClause(Four columnNo, TypeID& typeID);
	Four convertOidStringToOID(SimpleString& oidString, OOSQL_StorageManager::OID& oid);

	TypeID typeNameToTypeID(SimpleString& typeName);
	Four typeNameToTypeLength(SimpleString& typeName);
	Four typeID_ToTypeLength(TypeID type);
	StringPoolIndex installStringIntoStringPool(char* string);
	Four getStemizerFuncPtr(CataClassInfo classInfo, CataAttrInfo attrInfo, StemizerFuncPtr* stemizer);		
	
	Four isSamePathExpr(PathExprPoolElements &a, PathExprPoolElements &b);
    Four isSameArgument(ArgumentPoolElements &a, ArgumentPoolElements &b);
    Four isSameFunction(FunctionPoolElements &a, FunctionPoolElements &b);
    Four isSameObject(ObjectPoolElements &a, ObjectPoolElements &b);
    Four isSameValue(ValuePoolElements &a, ValuePoolElements &b);
    Four isSameDomain(DomainPoolElements &a, DomainPoolElements &b);
    Four isSameAggrFunc(AggrFuncPoolElements &a, AggrFuncPoolElements &b);

private:
    ASTNode AST(ASTNodeIdx index) { return m_ast->getASTNode(index); }

private:
    // private data member
    OQL_AST*				m_ast;
    OQL_GDS*				m_gds;
    OQL_GDSPOOL*			m_pool;
    OOSQL_Catalog*			m_catalog;
	OOSQL_StorageManager*	m_storageManager;
	OOSQL_ErrorMessage*		m_errorMessage;
	OOSQL_ExternalFunctionManager* m_externalFunctionManager;
	Four					m_volId;
    
    ProgressStatus  m_progressStatus;
    ExprPoolIndex   m_connectionPoint;
    OperatorGroup   m_recentOperGroup;
	Boolean         m_conditionNotBound;
    NameStack*      m_nameStack;

    #ifdef ENABLE_OPENGIS_OPTIMIZATION
    Four            m_currentUDFNo;     // UDF OPTIMIZATION
    #endif

};

#endif

