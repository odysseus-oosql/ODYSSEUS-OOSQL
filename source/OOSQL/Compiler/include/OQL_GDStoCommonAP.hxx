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

#ifndef __OQL_GDS_TO_COMMON_AP_H__
#define __OQL_GDS_TO_COMMON_AP_H__

#include "OOSQL_Common.h"
#include "OQL_GDS_Struct.hxx"
#include "OQL_Plan_Struct.hxx"
#include "OQL_GDS.hxx"
#include "OQL_GDSPOOL.hxx"
#include "OQL_CommonAP.hxx"
#include "OOSQL_Catalog.hxx"
#include "OQL_SimpleString.hxx"
#include "OQL_Dictionary.hxx"
#include "OOSQL_MemoryManager.hxx"
#include "OOSQL_MemoryManagedObject.hxx"
#include "OOSQL_ErrorMessage.hxx"

/*
 *  MatchFuncDirectionary
 */
struct MatchFuncInfo {
    Four    planNo;
    Four    funcNo;
};

#ifndef TEMPLATE_NOT_SUPPORTED

class  MatchFuncDictionary : public Dictionary<Four,MatchFuncInfo> {
};

#else

declare2(Dictionary,Four,MatchFuncInfo);

class  MatchFuncDictionary : public Dictionary(Four,MatchFuncInfo) {
};

#endif

/*
 *  TempFileInfoStruct
 */
#define MAX_TEMP_FILES  30

enum TempFileUsage {
    TEMPFILE_FOR_GROUPBY,
    TEMPFILE_FOR_ORDERBY,
    TEMPFILE_FOR_AGGR_DISTINCT,
    TEMPFILE_FOR_SELECT_DISTINCT
};

struct TempFileInfoStruct {
    AP_TempFileInfoPoolIndex    tempFileInfo;
    TempFileUsage               usage;
    Four                        pos;        // from which pos. of the clause this tempfile made
                                            // useful when find tempfile number from aggr func in select, etc.
};

class OQL_GDStoCommonAP : public OOSQL_MemoryManagedObject {
public:
    OQL_GDStoCommonAP(OQL_GDS& gds, OQL_GDSPOOL& pool, OOSQL_Catalog& catalog, OQL_CommonAP& commonAP, OOSQL_ErrorMessage& errorMessage);
    virtual ~OQL_GDStoCommonAP();

    Four genAccessPlan();

private:
    Four traverseQGandMakePlan(QGNodePoolIndex qgpoolIndex, Four parentPlanNo);
    Four convertCataClassInfo_to_ClassID(CataClassInfo classInfo, Four &classId);
    Four convertCataAttrInfo_to_ColNo(CataAttrInfo attrInfo, Two &colNo);
    Four convertCataMethodInfo_to_MehtodNo(CataMethodInfo methodInfo, Two &methodNo);
    Four convertExpr_to_AP_Expr(ExprPoolElements& expr, AP_ExprPoolElements& ap_expr);
    Four convertArgument_to_AP_Argument(ArgumentPoolElements& argument, AP_ArgumentPoolElements& ap_argument);
    Four traverseExprTree_convertAP_Expr(ExprPoolIndex exprPoolIndex, AP_ExprPoolIndex& ap_exprPoolIndex);
    Four convertPathExpr_to_PathExprAccessInfo(PathExprPoolIndex pathExpr, AP_PathExprAccessInfo& pathExprAccessInfo);
    Four convertFunc_to_FuncEvalInfo(FunctionPoolIndex func, AP_FuncEvalInfo& funcAccessInfo);
    Four convertFunc_to_FuncResultAccessInfo(FunctionPoolIndex funcPoolIndex, AP_FuncResultAccessInfo& funcResultAccessInfo);
    Four convertAggrFunc_to_AggrFuncAccessInfo(AggrFuncPoolIndex aggrFuncPoolIndex, AP_AggrFuncResultAccessInfo& aggrFuncAccessInfo);
	Four convertUpdateValue_to_AP_UpdateValue(CataClassInfo classInfo, UpdateValuePoolElements& updateValueList, AP_UpdateValuePoolElements& ap_updateValueList);
	Four convertInsertValue_to_AP_InsertValue(CataClassInfo classInfo, InsertValuePoolElements& insertValueList, AP_InsertValuePoolElements& ap_insertValueList);

    Four setProjection(AP_ProjectionElement& projection, GroupByListElement& groupBy);
    Four setProjection(AP_ProjectionElement& projection, OrderByListElement& orderBy);
    Four setProjection(AP_ProjectionElement& projection, SelListElement& selList);
    Four setProjection(AP_ProjectionElement& projection, ArgumentElement& argument);

    Four makePlan_processImplicitJoin(CommonAP_PoolElements& apNode, QGNodePoolElements& qgNode, Four parentPlanNo);
    Four makePlan_processExplicitJoin(CommonAP_PoolElements& apNode, QGNodePoolElements& qgNode);
    Four makePlan_connectExpressionInfoToAP(CommonAP_PoolElements& apNode, QGNodePoolElements& qgNode);
    Four makePlan_constructColNoMap(CommonAP_PoolElements& apNode, QGNodePoolElements& qgNode);
    Four makePlan_constructMethodNoMap(CommonAP_PoolElements& apNode, QGNodePoolElements& qgNode);
    
    Four makePlan_determineScanMethod(CommonAP_PoolElements& apNode, QGNodePoolElements& qgNode);
    Four makePlan_determineScanMethod(Four currentPlanNo, Four classId, AP_CondListPoolElements conds, Four &accessMethod, 
                                      AP_IndexInfoPoolIndex& indexInfo, AP_CondListPoolIndex& resultConds);
	Four makePlan_determineScanMethod_MakeBoolExpr(Four currentPlanNo, AP_CondListPoolIndex condsPoolIndex,  
                                                   AP_IndexInfoPoolIndex indexInfoPoolIndex, AP_CondListPoolIndex& resultConds);
	Four makePlan_determineScanMethod_MakeBoolExpr_AttachBoolExpr(AP_IndexInfoPoolElements indexInfo, AP_BoolExprPoolElements boolExprs);
    Four makePlan_determineScanMethod_BTreeScanAndJoin(Four currentPlanNo, Four classId, AP_CondListPoolElements conds, Four condNumber,
                                                       AP_IndexInfoPoolIndex& indexInfo, AP_CondListPoolIndex& resultConds);
    Four makePlan_determineScanMethod_TextScan(Four currentPlanNo, Four classId, AP_CondListPoolElements conds, Four condNumber, 
                                               AP_IndexInfoPoolIndex& indexInfo, AP_CondListPoolIndex& resultConds);
	Four makePlan_determineScanMethod_TextScan_MakeIndexNode(Four currentPlanNo, Four classId, Four operand1_indexType, Four operand2_indexType, 
															 AP_ExprPoolElements& expr, AP_IndexInfoPoolIndex& indexInfoPoolIndex, 
															 Boolean& isCurrentCondsInBoundConds);
	Four makePlan_determineScanMethod_TextScan_RecursiveMakeIndexNode(Four currentPlanNo, Four classId, 
				                                                      AP_ExprPoolElements& expr, AP_IndexInfoPoolIndex& indexInfoPoolIndex);
	Four get_nKeysUsedInCondition(Four currentPlanNo, Four classId,	Four &nKeys, 
									AP_CondListPoolElements conds, Four condNumber, 
									Four indexType,	Boolean joinFlag);
	
	Four makePlan_determineScanMethod_MlgfScanAndJoin(Four currentPlanNo, Four classId, AP_CondListPoolElements conds, 
														Four condNumber, AP_IndexInfoPoolIndex& indexInfo);
	
	Four makePlan_determineBoundConditionForMlgfScanAndJoin(Four planNo, Two* colNos, AP_CondListPoolElements conds,
														AP_MlgfIndexCond& mlgf, CataClassInfo classInfo);
	
	Four makePlan_constructBoundConditionForMlgfScanAndJoin(OperatorID operatorId, AP_ExprPoolElements& operand, 
													Four colIndex, AP_MlgfIndexCond& mlgf, Boolean joinFlag);

	Four makePlan_determineScanMethod_MlgfMbrScan(Four currentPlanNo, Four classId, AP_CondListPoolElements conds, 
													Four condNumber, AP_IndexInfoPoolIndex& indexInfo, 
													AP_CondListPoolIndex& resultConds);

    Four makePlan_determineScanMethod_MlgfMbrJoin(Four currentPlanNo, Four classId, AP_CondListPoolElements conds, 
													Four condNumber,
													AP_IndexInfoPoolIndex& indexInfo, AP_CondListPoolIndex& resultConds);
    Four makePlan_determineBoundConditionForB_TreeScan(Four planNo, Four nKeyCols, Two* colNos, AP_CondListPoolElements conds,
                                                       AP_BoundCondInfo& startBound, AP_BoundCondInfo& stopBound,
                                                       AP_CondListPoolIndex& resultConds, Boolean operandSwapped,
													   CataClassInfo classInfo, OOSQL_StorageManager::KeyDesc& btreeKeyDesc);
    Four makePlan_determineBoundConditionForB_TreeJoin(Four planNo, Four nKeyCols, Two* colNos, AP_CondListPoolElements conds,
                                                       AP_BoundCondInfo& startBound, AP_BoundCondInfo& stopBound,
                                                       AP_CondListPoolIndex& resultConds, Boolean operandSwapped, OOSQL_StorageManager::KeyDesc& btreeKeyDesc);
    Four makePlan_constructBoundConditionForExpression(AP_ExprPoolElements& expr,
																		  AP_ExprPoolElements&	operand1, 
																		  AP_ExprPoolElements&	operand2, 
																		  Four					colIndex,
																		  TypeID				attrTypeID,
																		  Four					attrLength,
																		  AP_BoundCondInfo&		startBound, 
																		  AP_BoundCondInfo&		stopBound,
																		  AP_ExprPoolElements&	startKey, 
																		  AP_ExprPoolElements&	stopKey, 
																		  Boolean&				isCurrentCondsInBoundConds,
																		  Boolean				isJoinCondition);
    Four makePlan_retouchVarStringBoundCondition(AP_ExprPoolElements&   expr,
																	Four					colIndex,
																	Four					attrLength,
																	AP_ExprPoolElements&	startKey, 
																	AP_ExprPoolElements&	stopKey);
    Four makePlan_retouchStringBoundCondition(AP_ExprPoolElements&  expr,
																 Four					colIndex,
																 Four					attrLength,
																 AP_ExprPoolElements&	startKey, 
																 AP_ExprPoolElements&	stopKey);
    Four makePlan_constructUnboundConditionAsMinMaxValue(Four nKeyParts,
																			OOSQL_StorageManager::KeyDesc& btreeKeyDesc,
																			AP_BoundCondInfo*		startBoundArray, 
																			AP_BoundCondInfo*		stopBoundArray,
																			AP_ExprPoolElements&	startKey, 
																			AP_ExprPoolElements&	stopKey);
    Four makePlan_detremineMultikeyBoundCondition(Four nKeyParts,
																	 AP_BoundCondInfo* startBoundArray, 
																	 AP_BoundCondInfo* stopBoundArray, 
																	 AP_BoundCondInfo& startBound, 
																	 AP_BoundCondInfo& stopBound);

    Four makePlan_constructGroupByHaving();
    Four makePlan_constructOrderBy();
    Four makePlan_constructSelect();
	Four makePlan_constructFinalProjection();
	Four makePlan_constructUpdate();
	Four makePlan_constructUpdate_isMakeTemporary();
	Four makePlan_constructDelete();
	Four makePlan_constructDelete_isMakeTemporary(); 
	Four makePlan_constructInsert();
	Four makePlan_constructInsert_isMakeTemporary();
    Four makePlan_constructAggrFuncInfo(CommonAP_PoolElements &apNode);
    Four makePlan_coustructAggrFuncInfoExceptionalCase(CommonAP_PoolElements &apNode);
    
	Four makePlan_constructTempFileInfo(AP_TempFileInfoElement& tempFileInfo, AP_ProjectionElement& projection);
    Four makePlan_constructTempFileInfo(AP_TempFileInfoElement& tempFileInfo, AP_ProjectionElement& projection, OrderByListElement& orderBy);
    Four makePlan_constructTempFileInfo(AP_TempFileInfoElement& tempFileInfo, AP_ProjectionElement& projection, GroupByListElement& groupBy);
    Four makePlan_constructTempFileInfo(AP_TempFileInfoElement& tempFileInfo, AP_ProjectionElement& projection, SelListElement& selList);
    Four makePlan_constructTempFileInfo(AP_TempFileInfoElement& tempFileInfo, AP_ProjectionElement& projection, ArgumentElement& argument);
    Four makePlan_constructTextIrSubPlan(CataClassInfo classInfo, CataAttrInfo attrInfo, CataIndexInfo indexInfo, ExprPoolIndex exprPoolIndex, AP_TextIndexSubPlan& subPlan, Four irScanDirection, const OOSQL_StorageManager::InvertedIndexDesc& invertedIndexKeyDesc);
	Four makePlan_constructStarToOrQueryInTextIrSubPlan(CataClassInfo classInfo, CataAttrInfo attrInfo, CataIndexInfo indexInfo, AP_TextIndexSubPlan& subPlan, Four irScanDirection);
	Four makePlan_ConstructOrQueryTextIrSubPlan(CataClassInfo classInfo, CataAttrInfo attrInfo, CataIndexInfo indexInfo, const AP_TextIndexKeywordInfo &keywordInfo, AP_TextIndexCondPoolIndex& textIndexCondPoolIndex, Four irScanDirection);

    Four makePlan_constructTextIndexKeywordInfo(CataClassInfo classInfo, CataAttrInfo attrInfo, CataIndexInfo indexInfo, StringPoolIndex string, AP_TextIndexKeywordInfo& keywordInfo);
	Four makePlan_constructTextIndexKeywordInfo(CataClassInfo classInfo, CataAttrInfo attrInfo, CataIndexInfo indexInfo, SimpleString& string, AP_TextIndexKeywordInfo& keywordInfo);
    Four makePlan_constructTextIndexStartBound(SimpleString& string, StringPoolElements& startBound);
    Four makePlan_constructTextIndexStopBound(SimpleString& string, StringPoolElements& stopBound);
    Four makePlan_getCaseOfTextIndex(SimpleString& string);
    Four traverseExprAndMakeTextIndexConds(CataClassInfo classInfo, CataAttrInfo attrInfo, CataIndexInfo indexInfo, ExprPoolIndex exprPoolIndex, AP_TextIndexCondPoolIndex& textIndexCondPoolIndex, OperatorID operatorID, Four irScanDirection, const OOSQL_StorageManager::InvertedIndexDesc& invertedIndexKeyDesc);
	Four traverseIrSubPlanAndConvertStarToOrQuery(CataClassInfo classInfo, CataAttrInfo attrInfo, CataIndexInfo indexInfo, AP_TextIndexCondPoolIndex& textIndexCondPoolIndex, Four irScanDirection);

    Four getPlanNoFromArgument(ArgumentPoolElements& argument);
    Four getTempFileColNo(Four tempFileNum, PathExprPoolElements& pathExpr, Two& tempFileColNo);
	Four getTempFileColNo(Four tempFileNum, FunctionPoolElements& func, Two& tempFileColNo);
	Four getTempFileColNo(Four tempFileNum, AggrFuncPoolElements& aggrFunc, Two& tempFileColNo);
    Four getAggrFuncIndex(AggrFuncPoolElements& aggrFunc, Two& aggrFuncIndex);
    Four getAggrFuncPlanNo(Four aggrFuncInfo_planNo, Four aggrFuncInfo_index, Four& planNo, Four& colNo, Boolean& flag);
    Four getTypeId_and_TypeLength_of_PathExpr(PathExprPoolIndex poolIndex, TypeID& typeId, Four& length);
    Four getTypeId_and_TypeLength_of_AggrFunc(AggrFuncPoolIndex poolIndex, TypeID& typeId, Four& length);

    Four isSamePathExpr(PathExprPoolElements &a, PathExprPoolElements &b);
    Four isSameArgument(ArgumentPoolElements &a, ArgumentPoolElements &b);
    Four isSameFunction(FunctionPoolElements &a, FunctionPoolElements &b);
    Four isSameObject(ObjectPoolElements &a, ObjectPoolElements &b);
    Four isSameValue(ValuePoolElements &a, ValuePoolElements &b);
    Four isSameDomain(DomainPoolElements &a, DomainPoolElements &b);
    Four isSameAggrFunc(AggrFuncPoolElements &a, AggrFuncPoolElements &b);

    Four getNewTempFileNum();
    Four getTempFileInfoPoolIndex(Four num, AP_TempFileInfoPoolIndex& poolIndex);
    Four setTempFileInfoPoolIndex(Four num, AP_TempFileInfoPoolIndex poolIndex);
    Four getTempFileNum(TempFileUsage usage, Four pos, Four& tempFileNum);
    Four setTempFileUsage(Four tempFileNum, TempFileUsage usage, Four pos);

    Four compareValueInExpr(ValuePoolIndex value_a, ValuePoolIndex value_b, int& compareResult);
    Four getFloatFromRealPool(RealPoolIndex index, float& real);
    Four getIntegerFromIntegerPool(IntegerPoolIndex index, long& integer);
    Four getStringFromStringPool(StringPoolIndex index, SimpleString& string);

    Four replacePathExprForGroupbyKey(AP_ExprPoolElements& ap_expr, Four groupby_planNo);
    Four traverseAP_ExprTreeReplacePathExpr(AP_ExprPoolIndex ap_exprPoolIndex, Four groupby_planNo);
    
    Four markInnermostPlan();

    Four reverseString(SimpleString& string);
    OperatorID reverseComparisionOperatorID(OperatorID op);
	Four checkIfThereAreMatchFunctionsInAllExpression(AP_ExprPoolElements expr);
	Four GetIndexTypeOfExpression(Four classId, AP_ExprPoolIndex exprPoolIndex);
	ValuePoolIndex constructMaxValueOfTheType(Four type, Four length);
	ValuePoolIndex constructMinValueOfTheType(Four type, Four length);
	bool isEqualString(AP_StringPoolIndex string1, AP_StringPoolIndex string2);

	Four isOGISOPID(Four operatorID);
	Four IsRectanglePolygon(AP_OperatorStruct oper);

private:
    OQL_GDS                 *m_gds;          
    OQL_GDSPOOL             *m_pool;         
    OOSQL_Catalog           *m_catalog;       
    OQL_CommonAP            *m_commonAP;      
	OOSQL_ErrorMessage      *m_errorMessage;

    PlanPoolIndex           m_aggrFuncInfo_planNo;

    TempFileInfoStruct      m_tempFiles[MAX_TEMP_FILES];  
    Four                    m_tempFileTop;    

    Four                    m_currentPlanNo;      
    Four                    m_currentMatchFuncNo; 
    MatchFuncDictionary*    m_matchFuncDictionary;
};

#endif /* __OQL_GDS_TO_COMMON_P_H__ */

