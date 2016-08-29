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
#include <string.h>
#include <limits.h>
#include <float.h>
#include "OQL_ASTtoGDS.hxx"
#include "OQL_GDStoCommonAP.hxx"
#include "OQL_Plan_Struct.hxx"

#ifndef MLGF_MIN_HASHVALUE
#define MLGF_MIN_HASHVALUE	((UFour_Invariable)0)
#endif

#ifndef MLGF_MAX_HASHVALUE
#define MLGF_MAX_HASHVALUE	((UFour_Invariable)0xFFFFFFFF)
#endif

#ifndef max
#define max(x, y) (((x) > (y))?(x):(y))
#endif
#ifndef min
#define min(x, y) (((x) < (y))?(x):(y))
#endif

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
Nothing

IMPLEMENTATION:
****************************************************************************/
OQL_GDStoCommonAP::OQL_GDStoCommonAP(
    OQL_GDS&        agds,				// IN      
    OQL_GDSPOOL&    apool,				// INOUT    
    OOSQL_Catalog&  acatalog,			// IN       
    OQL_CommonAP&   acommonAP,			// OUT     
	OOSQL_ErrorMessage& errorMessage	// INOUT	
)
{
    m_gds      = &agds;
    m_pool     = &apool;
    m_catalog  = &acatalog;
    m_commonAP = &acommonAP;
	m_errorMessage = &errorMessage;

    m_matchFuncDictionary = new (pMemoryManager) MatchFuncDictionary;
	if(m_matchFuncDictionary == NULL)
		OOSQL_ERR_EXIT(eMEMORYALLOCERR_OOSQL);

    m_aggrFuncInfo_planNo.setNull();
    m_tempFileTop = 0;
}


/****************************************************************************
DESCRIPTION:

RETURN VALUE:
Nothing

IMPLEMENTATION:
****************************************************************************/
OQL_GDStoCommonAP::~OQL_GDStoCommonAP()
{
	OOSQL_DELETE(m_matchFuncDictionary);
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
error code

IMPLEMENTATION:
****************************************************************************/
Four OQL_GDStoCommonAP::genAccessPlan()
{
    Four					i;
    QGNodePoolIndex			rootnodePoolIndex;
    Four					e;
	QGNodePoolIndex			queryGraph;
	GroupByListPoolIndex	groupByList;
	OrderByListPoolIndex	orderByList;
	CommonAP_PoolElements   apNode(m_pool->commonAP_Pool);

	switch(m_gds->queryType)
	{
	case OQL_GDS::SELECT_QUERY:
	case OQL_GDS::INSERT_QUERY:
		e = m_gds->getQueryGraph(m_pool, queryGraph);   OOSQL_CHECK_ERR(e);
		e = m_gds->getGroupByList(m_pool, groupByList); OOSQL_CHECK_ERR(e);
		e = m_gds->getOrderByList(m_pool, orderByList); OOSQL_CHECK_ERR(e);

		// Generate common access plan from GDS (query graph)
    
		// Traverse query graph and insert new access plan element for each query graph node
		m_commonAP->commonAP.setNull();
    
		for(i = queryGraph.startIndex;i < queryGraph.startIndex + queryGraph.size; i++)
		{   // for each root node in Query Graph
			// traverse sub graph of each root node
			rootnodePoolIndex.setPoolIndex(i, 1);   
			e = traverseQGandMakePlan(rootnodePoolIndex, -1);
			OOSQL_CHECK_ERR(e);
		}

		// mark inner most plan
		e = markInnermostPlan();
		OOSQL_CHECK_ERR(e);

		// m_aggrFuncInfo_planNo represents the position which aggr-functions to be applied
		// this variable set by makePlan_constructGroupByHaving, 
		//                      makePlan_constructOrderBy,
		//                      makePlan_constructSelect
		// and used by makePlan_constructAggrFuncInfo
		m_aggrFuncInfo_planNo.setNull();

		// make group by, order by, projection info
		if(groupByList != NULL_POOLINDEX)
		{
			// construct group by, having information
			e = makePlan_constructGroupByHaving();
			OOSQL_CHECK_ERR(e);
		}
    
		// construct projection info
		e = makePlan_constructSelect();
		OOSQL_CHECK_ERR(e);

		if(orderByList != NULL_POOLINDEX)
		{
			// construct order by information
			e = makePlan_constructOrderBy();
			OOSQL_CHECK_ERR(e);
		}

		e = makePlan_constructFinalProjection();
		OOSQL_CHECK_ERR(e);

		if(m_gds->queryType == OQL_GDS::INSERT_QUERY)
		{
			e = makePlan_constructInsert();
			OOSQL_CHECK_ERR(e);
		}

		break;

	case OQL_GDS::UPDATE_QUERY:
	case OQL_GDS::DELETE_QUERY:
		// Generate common access plan from GDS (query graph)
		// Traverse query graph and insert new access plan element for each query graph node
		m_commonAP->commonAP.setNull();
    
		e = m_gds->getQueryGraph(m_pool, queryGraph);   OOSQL_CHECK_ERR(e);
		if(queryGraph != NULL_POOLINDEX)
		{
			for(i = queryGraph.startIndex;i < queryGraph.startIndex + queryGraph.size; i++)
			{   // for each root node in Query Graph
				// traverse sub graph of each root node
				rootnodePoolIndex.setPoolIndex(i, 1);   
				e = traverseQGandMakePlan(rootnodePoolIndex, -1);
				OOSQL_CHECK_ERR(e);
			}
		}
		else
		{
			apNode = m_pool->commonAP_Pool.addNewEntry();
			apNode[0].init();
	        m_commonAP->commonAP = apNode.getPoolIndex();
		}

		// mark inner most plan
		e = markInnermostPlan();
		OOSQL_CHECK_ERR(e);

		switch(m_gds->queryType)
		{
		case OQL_GDS::UPDATE_QUERY:
			e = makePlan_constructUpdate();
			OOSQL_CHECK_ERR(e);					
			break;

		case OQL_GDS::DELETE_QUERY:
			e = makePlan_constructDelete();
			OOSQL_CHECK_ERR(e);					
			break;
		
		default:
			OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
		}

		break;

	case OQL_GDS::NO_QUERY:
		break;

	default:
		OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
	}

    return eNOERROR;
}

Four OQL_GDStoCommonAP::markInnermostPlan()
{
    Four                        i;
    CommonAP_PoolElements       apNode(m_pool->commonAP_Pool);


    apNode = m_commonAP->commonAP;

    for(i = 0; i < apNode.size; i++)
    {
        if(apNode[i].joinMethod == CAP_JOINMETHOD_OUTERMOST_CLASS && i > 1)
        {
            apNode[i - 1].isInnermostPlanElem = SM_TRUE;
        }
        else
            apNode[i].isInnermostPlanElem = SM_FALSE;
    }
	if(i > 0)
		apNode[i - 1].isInnermostPlanElem = SM_TRUE;

    return eNOERROR;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
error code

IMPLEMENTATION:
****************************************************************************/
Four OQL_GDStoCommonAP::traverseQGandMakePlan(QGNodePoolIndex qgpoolIndex, Four parentPlanNo)
{
    Four                        i;                  // general loop counter
    CommonAP_PoolElements       apNode(m_pool->commonAP_Pool);
    QGNodePoolElements          refClass(m_pool->qgNodePool);
    QGNodePoolElements          qgNode(m_pool->qgNodePool);
    PathExprInfoPoolElements    pathExprInfo(m_pool->pathExprInfoPool);   
                                                    // inidcates pathExprInfoElement in pathExprInfoList
    PathExprPoolElements        pathExpr(m_pool->pathExprPool);

    QGNodePoolIndex             rootnodePoolIndex;  // indicates node to traverse
    Four                        e;

        
    qgNode = qgpoolIndex;

    // make new AP element
    apNode = m_pool->commonAP_Pool.addNewEntry();
    apNode[0].init();
    if(m_commonAP->commonAP != NULL_POOLINDEX)
        m_commonAP->commonAP.size ++;
    else
        m_commonAP->commonAP = apNode.getPoolIndex();

    // initialize currentPlanNo and m_currentMatchFuncNo for processing match function
    m_currentPlanNo      = apNode.startIndex;
    m_currentMatchFuncNo = 0;

    // connect qgNode to apNode
    qgNode[0].planNo = apNode.getPoolIndex();    

    // mark each pathexpression with the index to AP elements
    // for each pathexpr in pathExprList, set planNo
    pathExprInfo = qgNode[0].pathExprList;
    while(pathExprInfo != NULL_POOLINDEX)   // for each pathexpr connected to QG
    {                                       // set plan no
        pathExpr = pathExprInfo[0].pathExpr;

        for(i = 0;i < pathExpr.size; i++)
            pathExpr[i].planNo = apNode.getPoolIndex();

        pathExprInfo = pathExprInfo[0].nextPathExprInfo;
    }

    // set class info
    apNode[0].classInfo.classKind = CLASSKIND_PERSISTENT;
    e = m_catalog->class_ClassInfo_to_ClassId(qgNode[0].classInfo, apNode[0].classInfo.classId);
    OOSQL_CHECK_ERR(e);

    // set subclass array
    apNode[0].subClassInfo = qgNode[0].subClasses;
    for(i = apNode[0].subClassInfo.startIndex; i < apNode[0].subClassInfo.startIndex +
        apNode[0].subClassInfo.size; i++)
    {
        // convert classInfo to classId
        e = m_catalog->class_ClassInfo_to_ClassId(            
                        m_pool->subClassPool[i].subClassInfo,
                        m_pool->subClassPool[i].classId);
        m_pool->subClassPool[i].accessMethod = CAP_ACCESSMETHOD_SEQSCAN;  // default scan method
        OOSQL_CHECK_ERR(e);
    }

    // process implicit join
    e = makePlan_processImplicitJoin(apNode, qgNode, parentPlanNo);
    OOSQL_CHECK_ERR(e);

    // process explicit join
    e = makePlan_processExplicitJoin(apNode, qgNode);
    OOSQL_CHECK_ERR(e);
    
    // connect expression information to AP
    e = makePlan_connectExpressionInfoToAP(apNode, qgNode);
    OOSQL_CHECK_ERR(e);

    // construct colNoMap, usedColInfo
    e = makePlan_constructColNoMap(apNode, qgNode);
    OOSQL_CHECK_ERR(e);

    // construct methodNoMap, usedMethodInfo
    e = makePlan_constructMethodNoMap(apNode, qgNode);
    OOSQL_CHECK_ERR(e);

    // determine scan method
	if(qgNode[0].isTargetOid)
	{
		apNode[0].isUseOid = SM_TRUE;
		apNode[0].oid      = qgNode[0].oid;

		apNode[0].accessMethod = CAP_ACCESSMETHOD_OIDFETCH;
	}
	else
	{
		e = makePlan_determineScanMethod(apNode, qgNode);
	    OOSQL_CHECK_ERR(e);
	}

    // determine nUsedFuncMatch
    apNode[0].nUsedFuncMatch = m_currentMatchFuncNo;

    // recursively traverse and construct CommonAP
    refClass = qgNode[0].refClassList;

    while(refClass != NULL_POOLINDEX)
    {
        rootnodePoolIndex = refClass.getPoolIndex();    
        traverseQGandMakePlan(rootnodePoolIndex, apNode.startIndex);        
                                                    // recursive call
        refClass = refClass[0].nextRefClass;        // next ref class
    }

    return eNOERROR;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
error code

IMPLEMENTATION:
****************************************************************************/
Four OQL_GDStoCommonAP::convertExpr_to_AP_Expr(ExprPoolElements& expr, AP_ExprPoolElements& ap_expr)
{
    AP_ExprPoolIndex    ap_exprPoolIndex;
    Four                e;

    e = traverseExprTree_convertAP_Expr(expr.getPoolIndex(), ap_exprPoolIndex);
    OOSQL_CHECK_ERR(e);

    ap_expr = ap_exprPoolIndex;

    return eNOERROR;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
error code

IMPLEMENTATION:
****************************************************************************/
Four OQL_GDStoCommonAP::traverseExprTree_convertAP_Expr(ExprPoolIndex exprPoolIndex, AP_ExprPoolIndex& ap_exprPoolIndex)
{
    ExprPoolElements        expr(m_pool->exprPool);
    AP_ExprPoolElements     new_ap_expr(m_pool->ap_exprPool);
    AP_ExprPoolIndex        operand1, operand2, operand3;
    FunctionPoolElements    func(m_pool->funcPool);
    Four                    e;

    expr        = exprPoolIndex;
    new_ap_expr = m_pool->ap_exprPool.addNewEntry();
    
    new_ap_expr[0].resultType   = expr[0].resultType;
    new_ap_expr[0].resultLength = expr[0].resultLength;
    
    switch(expr[0].exprKind)
    {
    case EXPR_KIND_PATHEXPR:
        new_ap_expr[0].exprKind = EXPR_KIND_PATHEXPR;
        e = convertPathExpr_to_PathExprAccessInfo(expr[0].pathExpr, new_ap_expr[0].pathExpr);
        OOSQL_CHECK_ERR(e);
        break;
    case EXPR_KIND_AGGRFUNC:
        new_ap_expr[0].exprKind = EXPR_KIND_AGGRFUNCRESULT;
        e = convertAggrFunc_to_AggrFuncAccessInfo(expr[0].aggrFunc, new_ap_expr[0].aggrFuncResult);
        OOSQL_CHECK_ERR(e);
        break;
    case EXPR_KIND_FUNCTION:
        func = expr[0].func;
        if(func[0].functionID == FUNCTION_TEXTIR_WEIGHT)
        {
            new_ap_expr[0].exprKind = EXPR_KIND_FUNCRESULT;

            e = convertFunc_to_FuncResultAccessInfo(expr[0].func, new_ap_expr[0].funcResult);
            OOSQL_CHECK_ERR(e);
        }
        else
        {
            new_ap_expr[0].exprKind = EXPR_KIND_FUNCEVAL;

            e = convertFunc_to_FuncEvalInfo(expr[0].func, new_ap_expr[0].funcEval);
            OOSQL_CHECK_ERR(e);
        }
        break;
    case EXPR_KIND_OPER:
        new_ap_expr[0].exprKind        = EXPR_KIND_OPER;
        new_ap_expr[0].oper.operatorId = expr[0].oper.operatorId;

        e = traverseExprTree_convertAP_Expr(expr[0].oper.operand1, operand1);
        OOSQL_CHECK_ERR(e);
        if(expr[0].oper.operand2 != NULL_POOLINDEX) // check for unary operator
        {
            e = traverseExprTree_convertAP_Expr(expr[0].oper.operand2, operand2);
            OOSQL_CHECK_ERR(e);
        }
        else
            operand2.setNull();
		if(expr[0].oper.operand3 != NULL_POOLINDEX) // check for unary operator
        {
            e = traverseExprTree_convertAP_Expr(expr[0].oper.operand3, operand3);
            OOSQL_CHECK_ERR(e);
        }
        else
            operand3.setNull();

        new_ap_expr[0].oper.operand1     = operand1; 
        new_ap_expr[0].oper.operand2     = operand2;
		new_ap_expr[0].oper.operand3     = operand3;
        new_ap_expr[0].oper.operatorType = expr[0].oper.operatorType;
        #ifdef ENABLE_OPENGIS_OPTIMIZATION
        new_ap_expr[0].oper.UDFNo        = expr[0].oper.UDFNo;
        new_ap_expr[0].oper.isConstant   = expr[0].oper.isConstant;
        new_ap_expr[0].oper.isEvaluated  = SM_FALSE;
        new_ap_expr[0].oper.result       = m_pool->valuePool.addNewEntry();
        #endif
		
        break;
    case EXPR_KIND_VALUE:
        new_ap_expr[0].exprKind = EXPR_KIND_VALUE;
        new_ap_expr[0].value    = expr[0].value;
        break;
    case EXPR_KIND_CONS:
        new_ap_expr[0].exprKind = EXPR_KIND_CONS;
        new_ap_expr[0].cons     = expr[0].cons;
        break;
    }

    ap_exprPoolIndex = new_ap_expr.getPoolIndex();

    return eNOERROR;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
error code

IMPLEMENTATION:
****************************************************************************/
Four OQL_GDStoCommonAP::convertPathExpr_to_PathExprAccessInfo(PathExprPoolIndex pathExprPoolIndex, AP_PathExprAccessInfo& pathExprAccessInfo)
{
    PathExprPoolElements    pathExpr(m_pool->pathExprPool);
    ArgumentPoolElements    argument(m_pool->argumentPool);
    AP_ArgumentPoolElements ap_argument(m_pool->ap_argumentPool);
    Four                    lastElemIndex;  // last element's index of pathExpr
    Four                    e;
    
    pathExpr = pathExprPoolIndex;
    lastElemIndex = pathExpr.size - 1;

    switch(pathExpr[lastElemIndex].fromAttrKind)
    {
    case PATHEXPR_KIND_METHOD:
        pathExprAccessInfo.kind             = PATHEXPR_KIND_METHOD;
        pathExprAccessInfo.method.planNo    = pathExpr[lastElemIndex].planNo.startIndex;
        e = m_catalog->method_MethodInfo_to_MethodNo(pathExpr[lastElemIndex].classInfo,
                                                   pathExpr[lastElemIndex].method.methodInfo,
                                                   pathExprAccessInfo.method.methodNo);
        OOSQL_CHECK_ERR(e);

        argument = pathExpr[lastElemIndex].method.argument;
        e = convertArgument_to_AP_Argument(argument, ap_argument);
        break;

    case PATHEXPR_KIND_ATTR:
        pathExprAccessInfo.kind       = PATHEXPR_KIND_ATTR;
        pathExprAccessInfo.col.planNo = pathExpr[lastElemIndex].planNo.startIndex;
        e = m_catalog->attr_AttrInfo_to_ColNo(pathExpr[lastElemIndex].classInfo,
                                              pathExpr[lastElemIndex].attr.attrInfo,
                                              pathExprAccessInfo.col.colNo);
        OOSQL_CHECK_ERR(e);
        break;

    case PATHEXPR_KIND_OBJECT:
        pathExprAccessInfo.kind   = PATHEXPR_KIND_OBJECT;
        pathExprAccessInfo.planNo = pathExpr[lastElemIndex].planNo.startIndex; // ????
        OOSQL_ERR(eNOTIMPLEMENTED_OOSQL);
        break;

    case PATHEXPR_KIND_CLASS:
        pathExprAccessInfo.kind   = PATHEXPR_KIND_OID;
        pathExprAccessInfo.planNo = pathExpr[lastElemIndex].planNo.startIndex;
        break;

	case PATHEXPR_KIND_LOGICALID:
		pathExprAccessInfo.kind   = PATHEXPR_KIND_LOGICALID;
		pathExprAccessInfo.planNo = pathExpr[lastElemIndex].planNo.startIndex;
		break;

    default:
        OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
        break;
    }
    
    return eNOERROR;
}

Four OQL_GDStoCommonAP::convertAggrFunc_to_AggrFuncAccessInfo(AggrFuncPoolIndex aggrFuncPoolIndex, AP_AggrFuncResultAccessInfo& aggrFuncAccessInfo)
{
    AggrFuncPoolElements    aggrFunc(m_pool->aggrFuncPool);
    Four                    e;
    Two                     aggrFuncIndex;

    aggrFunc = aggrFuncPoolIndex;

    // if aggrFuncInfo is not generated, then error. 
    // example case : select avg(age) from A where avg(age) > 10                error case
    //                                             ^^^^^^^^^^^^^
    //                select avg(*) from A group by name having avg(age) > 10   o.k.
    //                                                          ^^^^^^^^^^^^^
    if(m_aggrFuncInfo_planNo == NULL_POOLINDEX)
        OOSQL_ERR(eUNHANDLED_CASE_OOSQL);

    e = getAggrFuncIndex(aggrFunc, aggrFuncIndex);
    OOSQL_CHECK_ERR(e);

    aggrFuncAccessInfo.aggrFunctionID = aggrFunc[0].aggrFunctionID;
    aggrFuncAccessInfo.planNo         = m_aggrFuncInfo_planNo.startIndex;
    aggrFuncAccessInfo.aggrFuncIndex  = aggrFuncIndex;

    return eNOERROR;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
error code

IMPLEMENTATION:
****************************************************************************/
Four OQL_GDStoCommonAP::convertFunc_to_FuncEvalInfo(FunctionPoolIndex funcPoolIndex, AP_FuncEvalInfo& funcEvalInfo)
{
    FunctionPoolElements    func(m_pool->funcPool);
    ArgumentPoolElements    argument(m_pool->argumentPool);
    AP_ArgumentPoolElements ap_argument(m_pool->ap_argumentPool);
    PathExprPoolElements    pathExpr(m_pool->pathExprPool);
    ValuePoolElements       value(m_pool->valuePool);
    Four                    e;
    Four                    aliasNumber;
	Four					irScanDirection;
    MatchFuncInfo           matchFuncInfo;

    func = funcPoolIndex;

    switch(func[0].functionID)
    {
    case FUNCTION_TEXTIR_WEIGHT:
        OOSQL_ERR(eUNHANDLED_CASE_OOSQL);

    case FUNCTION_TEXTIR_MATCH:
		argument = func[0].argument;

        // get match's alias number
		if(argument[2].argumentKind != ARGUMENT_KIND_VALUE)
			OOSQL_ERR(eINTERNALERROR_OOSQL);
		value = argument[2].value;

		if(value[0].valueKind != VALUE_KIND_INTEGER)
			OOSQL_ERR(eINTERNALERROR_OOSQL);

		aliasNumber = m_pool->intPool[value[0].integer.startIndex];

        // get scan direction for postings
		if(argument[3].argumentKind != ARGUMENT_KIND_VALUE)
			OOSQL_ERR(eINTERNALERROR_OOSQL);
		value = argument[3].value;

		if(value[0].valueKind != VALUE_KIND_INTEGER)
			OOSQL_ERR(eINTERNALERROR_OOSQL);

		irScanDirection = m_pool->intPool[value[0].integer.startIndex];

        // register matchFuncInfo
        // search m_matchFuncDictionary for test duplicated registering
        e = m_matchFuncDictionary->FindItem(aliasNumber, matchFuncInfo);

        if(e == eNOTFOUND_DICTIONARY)
        {
            // register
            matchFuncInfo.planNo = m_currentPlanNo;
            matchFuncInfo.funcNo = m_currentMatchFuncNo;  m_currentMatchFuncNo ++;

            e = m_matchFuncDictionary->AddEntry(aliasNumber, matchFuncInfo);
            OOSQL_CHECK_ERR(e);
        }
        else if(e == eNOERROR)
        {
            // not the same information, then error : possibly there are duplicated labels in weight function
            if(aliasNumber == WEIGHT_DEFAULT_LABEL)
            {
                matchFuncInfo.planNo = m_currentPlanNo;
                matchFuncInfo.funcNo = m_currentMatchFuncNo;  m_currentMatchFuncNo ++;
            }
            else
            {
                if(!(matchFuncInfo.planNo == m_currentPlanNo && matchFuncInfo.funcNo == m_currentMatchFuncNo))
                {
                    OOSQL_ERR(eINTERNALERROR_OOSQL);
                }
            }
        }
        else
            OOSQL_ERR(e);

        // make new argument
        ap_argument = m_pool->ap_argumentPool.addNewEntry(2);

        argument = func[0].argument;
        pathExpr = argument[0].pathExpr;

        ap_argument[0].argumentKind = ARGUMENT_KIND_PATHEXPR;
        e = convertPathExpr_to_PathExprAccessInfo(pathExpr.getPoolIndex(), ap_argument[0].pathExpr);
        OOSQL_CHECK_ERR(e);
        if(ap_argument[0].pathExpr.kind != PATHEXPR_KIND_ATTR)
            OOSQL_ERR(eINTERNALERROR_OOSQL);

		// read index description
		OOSQL_StorageManager::InvertedIndexDesc	invertedIndexKeyDesc;
		OOSQL_StorageManager::IndexID			indexId_array[1];
		Four									nIndexes;
		Four									lastElemIndex;
		CataIndexInfo							cataIndexInfo;

		// from PathExpression get index id
		nIndexes = 1;
		lastElemIndex = pathExpr.size - 1;
		e = m_catalog->index_getTextIndexes(pathExpr[lastElemIndex].classInfo, pathExpr[lastElemIndex].attr.attrInfo, nIndexes, indexId_array);
        OOSQL_CHECK_ERR(e);

		// from index id get index info
		e = m_catalog->index_IndexId_to_IndexInfo(pathExpr[lastElemIndex].classInfo, indexId_array[0], cataIndexInfo);
		OOSQL_CHECK_ERR(e);

		// from index info get index description
		e = m_catalog->index_GetIndexDesc(pathExpr[lastElemIndex].classInfo, cataIndexInfo, invertedIndexKeyDesc);
		OOSQL_CHECK_ERR(e);

		// construct text ir subplan
        ap_argument[1].argumentKind = ARGUMENT_KIND_TEXTIR_SUBPLAN;
        e = makePlan_constructTextIrSubPlan(pathExpr[lastElemIndex].classInfo, 
			                                pathExpr[lastElemIndex].attr.attrInfo, 
											cataIndexInfo, argument[1].expr, ap_argument[1].textIndexSubPlan, irScanDirection, invertedIndexKeyDesc);
        OOSQL_CHECK_ERR(e);

        ap_argument[1].textIndexSubPlan.matchFuncNum = matchFuncInfo.funcNo;

		// convert star query into a group of or queries
		e = makePlan_constructStarToOrQueryInTextIrSubPlan(pathExpr[lastElemIndex].classInfo, 
			                                               pathExpr[lastElemIndex].attr.attrInfo, 
														   cataIndexInfo, ap_argument[1].textIndexSubPlan, irScanDirection);
		OOSQL_CHECK_ERR(e);

        funcEvalInfo.functionID   = func[0].functionID;
        funcEvalInfo.argument     = ap_argument.getPoolIndex();
        funcEvalInfo.returnType   = func[0].returnType;
        funcEvalInfo.returnLength = func[0].returnLength;
        break;

    default:
        funcEvalInfo.functionID = func[0].functionID;
		funcEvalInfo.userDefinedFunctionID = func[0].userDefinedFunctionID;
        argument = func[0].argument;
        e = convertArgument_to_AP_Argument(argument, ap_argument);
        OOSQL_CHECK_ERR(e);
        funcEvalInfo.argument     = ap_argument.getPoolIndex();
        funcEvalInfo.returnType   = func[0].returnType;
        funcEvalInfo.returnLength = func[0].returnLength;
        break;
    }
    return eNOERROR;
}

Four OQL_GDStoCommonAP::convertFunc_to_FuncResultAccessInfo(FunctionPoolIndex funcPoolIndex, AP_FuncResultAccessInfo& funcResultAccessInfo)
{
    FunctionPoolElements    func(m_pool->funcPool);
    ArgumentPoolElements    argument(m_pool->argumentPool);
    PathExprPoolElements    pathExpr(m_pool->pathExprPool);
    ValuePoolElements       value(m_pool->valuePool);
    Four                    e;
    Four                    aliasNumber;
    MatchFuncInfo           matchFuncInfo;

    func = funcPoolIndex;

    switch(func[0].functionID)
    {
    case FUNCTION_TEXTIR_WEIGHT:
        // get weight's alias number
        argument = func[0].argument;
        if(argument[0].argumentKind != ARGUMENT_KIND_VALUE)
            OOSQL_ERR(eINTERNALERROR_OOSQL);
        value = argument[0].value;
        if(value[0].valueKind != VALUE_KIND_INTEGER)
            OOSQL_ERR(eINTERNALERROR_OOSQL);
        aliasNumber = m_pool->intPool[value[0].integer.startIndex];

        // search m_matchFuncDictionary
        e = m_matchFuncDictionary->FindItem(aliasNumber, matchFuncInfo);

        if(e == eNOTFOUND_DICTIONARY)
        {
            OOSQL_ERR(eINTERNALERROR_OOSQL);
        }
        else if(e == eNOERROR)
        {
            funcResultAccessInfo.functionID = func[0].functionID;
            funcResultAccessInfo.planNo     = matchFuncInfo.planNo;
            funcResultAccessInfo.funcIndex  = matchFuncInfo.funcNo;
        }
        else
            OOSQL_ERR(e);

        break;

    default:
        OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
    }

    return eNOERROR;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
error code

IMPLEMENTATION:
****************************************************************************/
Four OQL_GDStoCommonAP::convertArgument_to_AP_Argument(ArgumentPoolElements& argument, AP_ArgumentPoolElements& ap_argument)
{
    Four                    i;
    Four                    e;
    ExprPoolElements        expr(m_pool->exprPool);
    AP_ExprPoolElements     ap_expr(m_pool->ap_exprPool);
    FunctionPoolElements    func(m_pool->funcPool);
    AP_FuncEvalInfo         funcEval;


    ap_argument = m_pool->ap_argumentPool.addNewEntry(argument.size);
    for(i = 0;i < argument.size; i++)
    {
        ap_argument[i].argumentKind = argument[i].argumentKind;
        switch(argument[i].argumentKind)
        {
        case ARGUMENT_KIND_PATHEXPR:
            e = convertPathExpr_to_PathExprAccessInfo(argument[i].pathExpr, ap_argument[i].pathExpr);
            OOSQL_CHECK_ERR(e);
            break;
        case ARGUMENT_KIND_FUNC:
            func = argument[i].func;
            if(func[0].functionID == FUNCTION_TEXTIR_WEIGHT)
            {
                ap_argument[i].argumentKind = ARGUMENT_KIND_FUNCRESULT;

                e = convertFunc_to_FuncResultAccessInfo(argument[i].func, ap_argument[i].funcResult);
                OOSQL_CHECK_ERR(e);
            }
            else
            {
                ap_argument[i].argumentKind = ARGUMENT_KIND_FUNCEVAL;

                e = convertFunc_to_FuncEvalInfo(argument[i].func, funcEval);
                ap_argument[i].funcEval = funcEval;
                OOSQL_CHECK_ERR(e);
            }

            break;
        case ARGUMENT_KIND_VALUE:
            ap_argument[i].value = argument[i].value;
            break;
        case ARGUMENT_KIND_DOMAIN:
            ap_argument[i].domain = argument[i].domain;
            break;
        case ARGUMENT_KIND_AGGRFUNC:
            // ????
            OOSQL_ERR(eNOTIMPLEMENTED_OOSQL);
            break;
        case ARGUMENT_KIND_EXPR:
            expr    = argument[i].expr;
            
            e = convertExpr_to_AP_Expr(expr, ap_expr);
            OOSQL_CHECK_ERR(e);

            ap_argument[i].expr = ap_expr.getPoolIndex();
            break;
        default:
            OOSQL_ERR(eNOTIMPLEMENTED_OOSQL);
        }
    }

    return eNOERROR;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
error code

IMPLEMENTATION:
****************************************************************************/
Four OQL_GDStoCommonAP::makePlan_processImplicitJoin(CommonAP_PoolElements& apNode, QGNodePoolElements& qgNode, Four parentPlanNo)
{
    Four    e;

    //  member to be set : joinMethod, joinClass, isMethod, 
    //                     (implicitJoinColNo, implicitJoinMethodNo),
    //                     accessMethod in access plan node
    //                     
    //                     planNo in QGnode
    //
    // if QGnode.implicitJoinAttrKind is not QGNODE_KIND_FROMNONE, then there is a 
    // implicit join.
    if(qgNode[0].implicitJoinAttrKind != QGNODE_KIND_FROMNONE)  //implicit join exists
    {
        apNode[0].joinMethod   = CAP_JOINMETHOD_IMPLICIT_FORWARD;
        apNode[0].joinClass    = parentPlanNo;                  // plan no of implicited joined class
        apNode[0].accessMethod = CAP_ACCESSMETHOD_OIDFETCH;
        if(parentPlanNo == -1)
            OOSQL_ERR(eUNHANDLED_CASE_OOSQL);

        if(qgNode[0].implicitJoinAttrKind == QGNODE_KIND_FROMATTR)
        {
            apNode[0].isMethod = SM_FALSE;
            e = m_catalog->attr_AttrInfo_to_ColNo(qgNode[0].classInfo , qgNode[0].fromAttrInfo, apNode[0].implicitJoinColNo);
            OOSQL_CHECK_ERR(e);
        }
        else if(qgNode[0].implicitJoinAttrKind == QGNODE_KIND_FROMMETHOD)
        {
            apNode[0].isMethod = SM_TRUE;
            e = m_catalog->method_MethodInfo_to_MethodNo(qgNode[0].classInfo, qgNode[0].fromMethodInfo, apNode[0].implicitJoinMethodNo);
            OOSQL_CHECK_ERR(e);
        }
    }

    return eNOERROR;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
error code

IMPLEMENTATION:
****************************************************************************/
Four OQL_GDStoCommonAP::makePlan_processExplicitJoin(CommonAP_PoolElements& apNode, QGNodePoolElements& qgNode)
{
    JoinInfoPoolElements explicitJoinInfo(m_pool->joinInfoPool);
    
    if(apNode[0].joinMethod == CAP_JOINMETHOD_IMPLICIT_FORWARD ||
       apNode[0].joinMethod == CAP_JOINMETHOD_IMPLICIT_BACKWARD)
    {
        // if this class can be read by implicit join, then skip explicit join
        // which has havey processing time.
        return eNOERROR;
    }

    //  member to be set : joinMethod, joinClass, accessMethod in access plan node
    //
    if(qgNode[0].explicitJoinList != NULL_POOLINDEX)    // explicit join exists
    {
        if(apNode.startIndex == 0)
        {
            apNode[0].joinMethod = CAP_JOINMETHOD_OUTERMOST_CLASS;
            apNode[0].joinClass  = -1;   
        }
        else
        {
            apNode[0].joinMethod = CAP_JOINMETHOD_EXPLICIT_NESTEDLOOP;
            apNode[0].joinClass  = apNode.startIndex - 1;   
        }
    }
    else
    {
        if(apNode.startIndex == 0)
        {
            apNode[0].joinMethod = CAP_JOINMETHOD_OUTERMOST_CLASS;
            apNode[0].joinClass  = -1;   
        }
        else
        {
            apNode[0].joinMethod = CAP_JOINMETHOD_CARTESIAN_PRODUCT;
            apNode[0].joinClass  = -1;   
        }
    }

    return eNOERROR;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
error code

IMPLEMENTATION:
****************************************************************************/
Four OQL_GDStoCommonAP::makePlan_connectExpressionInfoToAP(CommonAP_PoolElements& apNode, QGNodePoolElements& qgNode)
{
    CondListPoolElements        conditionInfo(m_pool->condInfoPool);
    AP_CondListPoolElements     ap_conditionList(m_pool->ap_condListPool);
    ExprPoolElements            expr(m_pool->exprPool);
    AP_ExprPoolElements         ap_expr(m_pool->ap_exprPool);
    Four                        e;
    Four                        i;
    Four                        nConditions;        // # of conditions to be connected to AP

    //  1. count number of expressions to be connected to AP
    //  2. allocate ap_condInfos
    //  3. fill ap_condInfos with conversion from ExprElement to AP_ExprElement
    //  4. connect it to AP

    // traverse conditionList in QGnode and count how many conditions to be connected AP
    conditionInfo = qgNode[0].conditionList;
    nConditions = 0;
    while(conditionInfo != NULL_POOLINDEX)
    {
        expr = conditionInfo[0].expr;

        if(expr[0].refCount >= 0)
            expr[0].refCount --;
        
        if(expr[0].refCount == 0)       // binding expression to plan
        {
            if(expr[0].operandType == EXPR_OPERANDTYPE_CONTAIN_METHOD)
            {
                // bind to client access plan
                OOSQL_ERR(eNOTIMPLEMENTED_OOSQL);
            }
            else if(expr[0].operandType == EXPR_OPERANDTYPE_CONTAIN_NOMETHOD)
            {
                // bind to common access plan
                nConditions ++;
            }
            else
                OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
        }

        conditionInfo = conditionInfo[0].nextCondInfo;  // next conditionInfo
    }

    if(nConditions > 0)     // if there is a condition to be conbined
    {
        // allocate condInfos
        ap_conditionList = m_pool->ap_condListPool.addNewEntry(nConditions);
        
        // fill ap_condInfo
        conditionInfo = qgNode[0].conditionList;
        i = 0;
        while(conditionInfo != NULL_POOLINDEX)
        {
            expr = conditionInfo[0].expr;

            if(expr[0].refCount == 0)       // binding expression to plan
            {
                if(expr[0].operandType == EXPR_OPERANDTYPE_CONTAIN_METHOD)
                {
                    // bind to client access plan
                    OOSQL_ERR(eNOTIMPLEMENTED_OOSQL);
                }
                else if(expr[0].operandType == EXPR_OPERANDTYPE_CONTAIN_NOMETHOD)
                {
                    // bind to common access plan
                    // convert expr to ap_expr
                    e = convertExpr_to_AP_Expr(expr, ap_expr);
                    OOSQL_CHECK_ERR(e);

                    // fill ap_condInfo
                    ap_conditionList[i].expr          = ap_expr.getPoolIndex();
                    i ++;
                }
                else
                    OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
            }

            conditionInfo = conditionInfo[0].nextCondInfo;  // next conditionInfo
        }
        apNode[0].condNodes = ap_conditionList.getPoolIndex();
    }

	if(apNode.startIndex == 0)
	{
		CondListPoolElements unusedCondition(m_pool->condInfoPool);

		apNode           = m_commonAP->commonAP;
		unusedCondition  = m_gds->m_unusedConditionList;
		
		while(unusedCondition != NULL_POOLINDEX)
		{
			expr = unusedCondition[0].expr;

			e = convertExpr_to_AP_Expr(expr, ap_expr);
			OOSQL_CHECK_ERR(e);

			ap_conditionList = m_pool->ap_condListPool.addNewEntry();
			if(apNode[0].condNodes == NULL_POOLINDEX)
				apNode[0].condNodes = ap_conditionList.getPoolIndex();
			else
				apNode[0].condNodes.size ++;

			ap_conditionList[0].expr = ap_expr.getPoolIndex();

			unusedCondition = unusedCondition[0].nextCondInfo;  // next conditionInfo
		}
	}

    return eNOERROR;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
error code

IMPLEMENTATION:
****************************************************************************/
Four OQL_GDStoCommonAP::makePlan_constructColNoMap(CommonAP_PoolElements& apNode, QGNodePoolElements& qgNode)
{
    Four                    nAttrsInClass;
    Four                    nAttrsUsed;
    Four                    e;
    Four                    i, j;
    Two                     attrNo;
    AP_ColNoMapPoolElements colNoMap(m_pool->colNoMapPool);
    AP_UsedColPoolElements  usedAttrs(m_pool->usedColPool);
    MemberPoolElements      usedMember(m_pool->memberPool);
	TypeID					typeId;

    // get total number of attributes in this class
    e = m_catalog->class_GetN_AttrsIncludingDroppedCol(qgNode[0].classInfo, nAttrsInClass);
    OOSQL_CHECK_ERR(e);

    if(nAttrsInClass == 0)
        return eNOERROR;

    // initialize colNoMap
    colNoMap = m_pool->colNoMapPool.addNewEntry(nAttrsInClass);
    for(i = 0; i < nAttrsInClass; i++)
        colNoMap[i].offset = NOT_USED_COLUMN;    // means N/A

    // get total number of used attributes in this class
    usedMember = qgNode[0].usedMemberList;
    nAttrsUsed = 0;
    while(usedMember != NULL_POOLINDEX)
    {
        if(usedMember[0].memberKind == MEMBER_KIND_ATTR)
		{
			e = m_catalog->attr_GetTypeID(qgNode[0].classInfo, usedMember[0].attr.attrInfo,
                                        typeId);
            OOSQL_CHECK_ERR(e);
			nAttrsUsed ++;
		}
        usedMember = usedMember[0].nextMember;
    }

    // initialize usedColInfo
    if(nAttrsUsed)
    {
        usedAttrs = m_pool->usedColPool.addNewEntry(nAttrsUsed);

        // mapping colNoMap to usedColInfo
        i = 0;
        usedMember = qgNode[0].usedMemberList;
        while(usedMember != NULL_POOLINDEX)
        {
            if(usedMember[0].memberKind == MEMBER_KIND_ATTR)
            {
                e = m_catalog->attr_GetTypeID(qgNode[0].classInfo, usedMember[0].attr.attrInfo,
                                              usedAttrs[i].typeId);
                OOSQL_CHECK_ERR(e);

				e = m_catalog->attr_GetLength(qgNode[0].classInfo, usedMember[0].attr.attrInfo,
											  usedAttrs[i].length);
				OOSQL_CHECK_ERR(e);
				e = m_catalog->attr_AttrInfo_to_ColNo(qgNode[0].classInfo, usedMember[0].attr.attrInfo,
													  attrNo);
				OOSQL_CHECK_ERR(e);
	
				usedAttrs[i].colNo = attrNo;
				i ++;
            }
            usedMember = usedMember[0].nextMember;
        }

        for(i = 0; i < nAttrsUsed - 1; i++)
        {
            for(j = 0; j < nAttrsUsed - 1 - i; j++)
            {
                if(usedAttrs[j + 1].colNo < usedAttrs[j].colNo)
                {
                    // swap
                    AP_UsedColElement tmpUsedAttr;
                    tmpUsedAttr      = usedAttrs[j];
                    usedAttrs[j]     = usedAttrs[j + 1];
                    usedAttrs[j + 1] = tmpUsedAttr;
                }
            }
        }

        for(i = 0; i < nAttrsUsed; i++)
        {
            attrNo = usedAttrs[i].colNo;
            colNoMap[attrNo].offset = i;
        }
    }
    else
        usedAttrs.setNull();

    // connect colNoMap and usedAttrs to AP
    apNode[0].colNoMap    = colNoMap.getPoolIndex();
    apNode[0].usedColInfo = usedAttrs.getPoolIndex();

    return eNOERROR;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
error code

IMPLEMENTATION:
****************************************************************************/
Four OQL_GDStoCommonAP::makePlan_constructMethodNoMap(CommonAP_PoolElements& apNode, QGNodePoolElements& qgNode)
{
    Four						nMethodsInClass;
    Four						nMethodsUsed;
    Four						e;
    Four						i;
    Two							methodNo;
    AP_MethodNoMapPoolElements  methodNoMap(m_pool->methodNoMapPool);
    AP_UsedMethodPoolElements   usedMethods(m_pool->usedMethodPool);
    MemberPoolElements			usedMember(m_pool->memberPool);

    // get total number of attributes in this class
    e = m_catalog->class_GetN_Methods(qgNode[0].classInfo, nMethodsInClass);
    OOSQL_CHECK_ERR(e);

    if(nMethodsInClass == 0)
        return eNOERROR;

    // initialize colNoMap
    methodNoMap = m_pool->methodNoMapPool.addNewEntry(nMethodsInClass);
    for(i = 0;i < nMethodsInClass; i++)
        methodNoMap[i].offset = -1; // -1 means N/A

    // get total number of used attributes in this class
    usedMember = qgNode[0].usedMemberList;
    nMethodsUsed = 0;
    while(usedMember != NULL_POOLINDEX)
    {
        if(usedMember[0].memberKind == MEMBER_KIND_METHOD)
            nMethodsUsed ++;
        usedMember = usedMember[0].nextMember;
    }

    // initialize usedColInfo
    if(nMethodsUsed)
    {
        usedMethods = m_pool->usedMethodPool.addNewEntry(nMethodsUsed);

        // mapping colNoMap to usedColInfo
        i = 0;
        usedMember = qgNode[0].usedMemberList;
        while(usedMember != NULL_POOLINDEX)
        {
            if(usedMember[0].memberKind == MEMBER_KIND_METHOD)
            {
                e = m_catalog->method_GetReturnTypeID(qgNode[0].classInfo, usedMember[0].method.methodInfo,
                                                      usedMethods[i].returnType);
                OOSQL_CHECK_ERR(e);
                e = m_catalog->method_GetReturnTypeLength(qgNode[0].classInfo, usedMember[0].method.methodInfo,
                                                          usedMethods[i].returnLength);
                OOSQL_CHECK_ERR(e); 
                e = m_catalog->method_GetPtr(qgNode[0].classInfo, usedMember[0].method.methodInfo,
                                             usedMethods[i].pMethod);
                OOSQL_CHECK_ERR(e);
                e = m_catalog->method_MethodInfo_to_MethodNo(qgNode[0].classInfo, usedMember[0].method.methodInfo,
                                                             methodNo);
                OOSQL_CHECK_ERR(e);
                methodNoMap[methodNo].offset = i;
                i ++;
            }
            usedMember = usedMember[0].nextMember;
        }
    }
    else
        usedMethods.setNull();

    // connect methodNoMap and usedMethods to AP
    apNode[0].methodNoMap    = methodNoMap.getPoolIndex();
    apNode[0].usedMethodInfo = usedMethods.getPoolIndex();

    return eNOERROR;
}

Four OQL_GDStoCommonAP::checkIfThereAreMatchFunctionsInAllExpression(AP_ExprPoolElements expr)
{
	AP_ExprPoolElements op1(m_pool->ap_exprPool);
	AP_ExprPoolElements op2(m_pool->ap_exprPool);
	Four				op1_result, op2_result;

	switch(expr[0].exprKind)
	{
	case EXPR_KIND_VALUE:
	case EXPR_KIND_CONS:
	case EXPR_KIND_AGGRFUNCRESULT:
	case EXPR_KIND_FUNCRESULT:    
	case EXPR_KIND_PATHEXPR:
		return SM_FALSE;
		break;

	case EXPR_KIND_FUNCEVAL:
		if(expr[0].funcEval.functionID == FUNCTION_TEXTIR_MATCH)
			return SM_TRUE;
		else
			return SM_FALSE;
		break;

	case EXPR_KIND_OPER:
		op1 = expr[0].oper.operand1;
		op2 = expr[0].oper.operand2;

		if(!op1.isNull())
			op1_result = checkIfThereAreMatchFunctionsInAllExpression(op1);
		else
			op1_result = SM_FALSE;

		if(!op2.isNull())
			op2_result = checkIfThereAreMatchFunctionsInAllExpression(op2);
		else
			op2_result = SM_FALSE;
		
		if(expr[0].oper.operatorId == OP_EQ || expr[0].oper.operatorId == OP_NE ||
           expr[0].oper.operatorId == OP_GT || expr[0].oper.operatorId == OP_LT ||
           expr[0].oper.operatorId == OP_GE || expr[0].oper.operatorId == OP_LE)
		{
			if(op1_result || op2_result)
				return SM_TRUE;
			else
				return SM_FALSE;
		}
		else if (expr[0].oper.operatorId == OP_ISNULL || expr[0].oper.operatorId == OP_ISNOTNULL)
		{
			if(op1_result)
				return SM_TRUE;
			else
				return SM_FALSE;
		}
		else
		{
			if(op1_result && op2_result)
				return SM_TRUE;
			else
				return SM_FALSE;
		}
		break;

	
	default:
		OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
		break;
	}
	return SM_FALSE;
}

Four OQL_GDStoCommonAP::GetIndexTypeOfExpression(Four classId, AP_ExprPoolIndex exprPoolIndex)
{
	AP_ExprPoolElements		expression(m_pool->ap_exprPool);
	CataClassInfo			classInfo;
	CataAttrInfo			attrInfo;
	CommonAP_PoolElements   apNode(m_pool->commonAP_Pool);
	Four					e;
	Four					indexType = 0;

	// set reference pointer
    apNode     = m_commonAP->commonAP;
	expression = exprPoolIndex;

	if(expression[0].exprKind == EXPR_KIND_PATHEXPR)
	{
		if(expression[0].pathExpr.kind == PATHEXPR_KIND_ATTR)
		{
			e = m_catalog->class_ClassId_to_ClassInfo(classId, classInfo);
			OOSQL_CHECK_ERR(e);

			e = m_catalog->attr_ColNo_to_AttrInfo(classInfo, expression[0].pathExpr.col.colNo, attrInfo);
            OOSQL_CHECK_ERR(e);

			e = m_catalog->index_isB_TreeIndexExist(classInfo, attrInfo);
            OOSQL_CHECK_ERR(e);

            if(e == SM_TRUE)
                indexType |= INDEXTYPE_BTREE;
			
			e = m_catalog->index_isMlgfIndexExist(classInfo, attrInfo);
            OOSQL_CHECK_ERR(e);

            if(e == SM_TRUE)
			{
                TypeID attrTypeID;
				
				e = m_catalog->attr_GetTypeID(classInfo, attrInfo, attrTypeID);
				OOSQL_CHECK_ERR(e);

				if (attrTypeID == TYPEID_OGIS_GEOMETRY              ||
				    attrTypeID == TYPEID_OGIS_POINT                 ||
				    attrTypeID == TYPEID_OGIS_LINESTRING            ||
			        attrTypeID == TYPEID_OGIS_POLYGON               ||
			        attrTypeID == TYPEID_OGIS_GEOMETRYCOLLECTION    ||
			        attrTypeID == TYPEID_OGIS_MULTIPOINT            ||
				    attrTypeID == TYPEID_OGIS_MULTILINESTRING       ||
					attrTypeID == TYPEID_OGIS_MULTIPOLYGON)

					indexType |= INDEXTYPE_MLGF_MBR;
				else
                indexType |= INDEXTYPE_MLGF;
			}
			
			if(indexType == 0)
				indexType = INDEXTYPE_NONE;

			return indexType;
		}
        else if(expression[0].pathExpr.kind == PATHEXPR_KIND_OBJECT || 
			    expression[0].pathExpr.kind == PATHEXPR_KIND_CLASS  ||
				expression[0].pathExpr.kind == PATHEXPR_KIND_OID)
        {
            // check if there is a MLGF index
			e = m_catalog->class_ClassId_to_ClassInfo(classId, classInfo);
			OOSQL_CHECK_ERR(e);

			// MBR column number is 1 
            e = m_catalog->attr_ColNo_to_AttrInfo(classInfo, 1, attrInfo);
            OOSQL_CHECK_ERR(e);

            e = m_catalog->index_isMlgfIndexExist(classInfo, attrInfo);
            OOSQL_CHECK_ERR(e);

            if(e == SM_TRUE)
                return INDEXTYPE_MLGF_MBR;
        }
    }
    else if(expression[0].exprKind == EXPR_KIND_FUNCEVAL)
    {
		// check TEXT index
        if(expression[0].funcEval.functionID == FUNCTION_TEXTIR_MATCH)
            return INDEXTYPE_TEXT;
    }
	
	return INDEXTYPE_NONE;
}


/****************************************************************************
DESCRIPTION:

RETURN VALUE:
error code

IMPLEMENTATION:
****************************************************************************/
Four OQL_GDStoCommonAP::makePlan_determineScanMethod(Four currentPlanNo, Four classId, AP_CondListPoolElements conds, 
                                                     Four &accessMethod, AP_IndexInfoPoolIndex& indexInfoPoolIndex,
                                                     AP_CondListPoolIndex& resultConds)
{
    Four                    operand1_indexType;
    Four                    operand2_indexType;
    Four                    i;
    Four                    e;
    Four                    priority, disjunctPriority;
    Four                    disjunctIndex;
    AP_ExprPoolElements     expr(m_pool->ap_exprPool);

    const int TYPE_NONE  = 0;
    const int BTREE_SCAN = 1;
    const int BTREE_JOIN = 2;
    const int TEXT_SCAN  = 3;
    const int MLGF_SCAN  = 4;
    const int MLGF_JOIN  = 5;
	const int MLGF_MBR_SCAN  = 6;
    const int MLGF_MBR_JOIN  = 7;

    Four disjunctScanType;
	Four isKNNQuery = 0;
	Four kNNQueryIndex = -1;

    /* 
	   scan disjunct and determine access method and index type
       IndexType Determination Table
       Priority        Condition Pattern                   IndexType
       -------------------------------------------------------------
           1           a = 0, a = b, 1 in a, b in a         B+ Tree, MLGF
           2           A overlap [0,0,1,1], A overlap B     MLGF_MBR
           3           Match(a, 'test') > 0                 Text
           4           a > 0, a > b                         B+ Tree, MLGF
           5           A disjoint [0,0,1,1], A disjoint B   MLGF_MBR
       -------------------------------------------------------------
	*/ 
    
	disjunctIndex    = -1;

#ifndef SUPPORT_LARGE_DATABASE2
	disjunctPriority = INT_MAX;		  // max value of Four type
#else
    disjunctPriority = LONG_MAX;      // max value of Four type
#endif	
    disjunctScanType = TYPE_NONE;

	for(i = conds.size - 1; i >= 0; i--)
    {                                   
        // get index type from QG's conditionList
		if(conds[i].expr.getElements(m_pool->ap_exprPool, 0).exprKind == EXPR_KIND_OPER)
		{
			if(conds[i].expr.getElements(m_pool->ap_exprPool, 0).oper.operatorId != OP_ISNULL &&
				conds[i].expr.getElements(m_pool->ap_exprPool, 0).oper.operatorId != OP_ISNOTNULL)
			{
				operand1_indexType = GetIndexTypeOfExpression(classId, conds[i].expr.getElements(m_pool->ap_exprPool, 0).oper.operand1);
				operand2_indexType = GetIndexTypeOfExpression(classId, conds[i].expr.getElements(m_pool->ap_exprPool, 0).oper.operand2);
			}
			else
			{
				operand1_indexType = INDEXTYPE_NONE;
				operand2_indexType = INDEXTYPE_NONE;
			}
		}
		else
		{
			operand1_indexType = INDEXTYPE_NONE;
			operand2_indexType = INDEXTYPE_NONE;
		}

        expr               = conds[i].expr;

		if(((operand1_indexType & INDEXTYPE_BTREE) && 
			(operand1_indexType & INDEXTYPE_MLGF) && 
			(operand2_indexType & INDEXTYPE_NONE)) ||
		   ((operand2_indexType & INDEXTYPE_BTREE) && 
			(operand2_indexType & INDEXTYPE_MLGF) && 
			(operand1_indexType & INDEXTYPE_NONE)))
		{
			if(expr[0].oper.operatorId == OP_EQ || expr[0].oper.operatorId == OP_IN)
                priority = 1;
            else
                priority = 4;

            if(priority <= disjunctPriority)
            {
				Four nBtreeKeys, nMlgfKeys;

                disjunctIndex    = i;
                disjunctPriority = priority;

				e = get_nKeysUsedInCondition(currentPlanNo, classId, nBtreeKeys, conds, disjunctIndex, INDEXTYPE_BTREE, SM_FALSE);
				OOSQL_CHECK_ERR(e);

				e = get_nKeysUsedInCondition(currentPlanNo, classId, nMlgfKeys, conds, disjunctIndex, INDEXTYPE_MLGF, SM_FALSE);
				OOSQL_CHECK_ERR(e);

				if(nBtreeKeys >= nMlgfKeys)
				{
					disjunctScanType = BTREE_SCAN;
				}
				else
				{
					disjunctScanType = MLGF_SCAN;
				}
            }			
		}
		else if((((operand1_indexType & INDEXTYPE_BTREE) && (operand1_indexType & INDEXTYPE_MLGF)) &&
				((operand2_indexType & INDEXTYPE_BTREE) || (operand2_indexType & INDEXTYPE_MLGF))) ||
				(((operand2_indexType & INDEXTYPE_BTREE) && (operand2_indexType & INDEXTYPE_MLGF)) &&
				((operand1_indexType & INDEXTYPE_BTREE) || (operand1_indexType & INDEXTYPE_MLGF))))
		{
			if(expr[0].oper.operatorId == OP_EQ || expr[0].oper.operatorId == OP_IN)
                priority = 1;
            else
                priority = 4;

            if(priority <= disjunctPriority)
            {
				Four nBtreeKeys, nMlgfKeys;

                disjunctIndex    = i;
                disjunctPriority = priority;

				e = get_nKeysUsedInCondition(currentPlanNo, classId, nBtreeKeys, conds, disjunctIndex, INDEXTYPE_BTREE, SM_TRUE);
				OOSQL_CHECK_ERR(e);

				e = get_nKeysUsedInCondition(currentPlanNo, classId, nMlgfKeys, conds, disjunctIndex, INDEXTYPE_MLGF, SM_FALSE);
				OOSQL_CHECK_ERR(e);

				if(nBtreeKeys >= nMlgfKeys)
				{
					if((operand1_indexType & INDEXTYPE_BTREE) && (operand2_indexType & INDEXTYPE_BTREE))
						disjunctScanType = BTREE_JOIN;
					else
						disjunctScanType = BTREE_SCAN;
				}
				else
				{
					if((operand1_indexType & INDEXTYPE_MLGF) && (operand2_indexType & INDEXTYPE_MLGF))
						disjunctScanType = MLGF_JOIN;
					else
						disjunctScanType = MLGF_SCAN;
				}
            }			
		}
        else if(( (operand1_indexType & INDEXTYPE_BTREE) && (operand2_indexType & INDEXTYPE_NONE) ) ||
           ( (operand1_indexType & INDEXTYPE_NONE) && (operand2_indexType & INDEXTYPE_BTREE) ))
        {
            // B+Tree Scan
            if(expr[0].oper.operatorId == OP_EQ || expr[0].oper.operatorId == OP_IN)
                priority = 1;
            else
                priority = 4;

            if(priority < disjunctPriority)
            {
                disjunctIndex    = i;
                disjunctPriority = priority;
                disjunctScanType = BTREE_SCAN;
            }
        }
        else if( (operand1_indexType & INDEXTYPE_BTREE) && (operand2_indexType & INDEXTYPE_BTREE) )
        {
            // B+Tree Join
            if(expr[0].oper.operatorId == OP_EQ || expr[0].oper.operatorId == OP_IN)
                priority = 1;
            else
                priority = 4;

            if(priority < disjunctPriority)
            {
                disjunctIndex    = i;
                disjunctPriority = priority;
                disjunctScanType = BTREE_JOIN;
            }

        }
        else if(( (operand1_indexType & INDEXTYPE_TEXT) && (operand2_indexType & INDEXTYPE_NONE) ) ||
                ( (operand1_indexType & INDEXTYPE_NONE) && (operand2_indexType & INDEXTYPE_TEXT) ))
        {
            // TEXT Scan
            priority = 3;

            if(priority < disjunctPriority)
            {
                disjunctIndex    = i;
                disjunctPriority = priority;
                disjunctScanType = TEXT_SCAN;
            }
        }
        else if( (operand1_indexType & INDEXTYPE_TEXT) && (operand2_indexType & INDEXTYPE_TEXT) )
        {
            // TEXT Join
            OOSQL_ERR(eNOTIMPLEMENTED_OOSQL);
        }
        else if(( (operand1_indexType & INDEXTYPE_MLGF_MBR) && (operand2_indexType & INDEXTYPE_NONE) ) ||
                ( (operand1_indexType & INDEXTYPE_NONE) && (operand2_indexType & INDEXTYPE_MLGF_MBR) ))
        {
            // MLGF scan
            if(expr[0].oper.operatorId == OP_GEO_NORTH || expr[0].oper.operatorId == OP_GEO_SOUTH || 
               expr[0].oper.operatorId == OP_GEO_EAST  || expr[0].oper.operatorId == OP_GEO_WEST  ||
               expr[0].oper.operatorId == OP_GEO_DISJOINT)
                priority = 5;
			else if(expr[0].oper.operatorId == OP_OGIS_KNN)
			{
				priority = 2;
				isKNNQuery = 1;
				kNNQueryIndex = i;
			}
            else
                priority = 2;

            if(priority < disjunctPriority)
            {
                disjunctIndex    = i;
                disjunctPriority = priority;
                disjunctScanType = MLGF_MBR_SCAN;
            }
        }
        else if( (operand1_indexType & INDEXTYPE_MLGF_MBR) && (operand2_indexType & INDEXTYPE_MLGF_MBR) )
        {
            // MLGF join
            if(expr[0].oper.operatorId == OP_GEO_NORTH || expr[0].oper.operatorId == OP_GEO_SOUTH || 
               expr[0].oper.operatorId == OP_GEO_EAST  || expr[0].oper.operatorId == OP_GEO_WEST  ||
               expr[0].oper.operatorId == OP_GEO_DISJOINT)
                priority = 5;
            else
                priority = 2;

            if(priority < disjunctPriority)
            {
                disjunctIndex    = i;
                disjunctPriority = priority;
                disjunctScanType = MLGF_MBR_JOIN;
            }
        }
		else if( ( (operand1_indexType & INDEXTYPE_MLGF) && (operand2_indexType & INDEXTYPE_NONE) ) ||
                 ( (operand1_indexType & INDEXTYPE_NONE) && (operand2_indexType & INDEXTYPE_MLGF) ) )
        {
            if(expr[0].oper.operatorId == OP_EQ || expr[0].oper.operatorId == OP_IN)
                priority = 1;
            else
                priority = 4;

            if(priority < disjunctPriority)
            {
                disjunctIndex    = i;
                disjunctPriority = priority;
                disjunctScanType = MLGF_SCAN;
            }
        }
		else if( (operand1_indexType & INDEXTYPE_MLGF) && (operand2_indexType & INDEXTYPE_MLGF) )
        {
            if(expr[0].oper.operatorId == OP_EQ || expr[0].oper.operatorId == OP_IN)
                priority = 1;
            else
                priority = 4;

            if(priority < disjunctPriority)
            {
                disjunctIndex    = i;
                disjunctPriority = priority;
                disjunctScanType = MLGF_JOIN;
            }
        }
		else
		{
			// MATCH(a, 'text1') > 0 or MATCH(b, 'text2') > 0 or MATCH(c, 'text3') > 0 or ...
			if(checkIfThereAreMatchFunctionsInAllExpression(expr))
			{
				// TEXT Scan
				priority = 3;

				if(priority < disjunctPriority)
				{
					disjunctIndex    = i;
					disjunctPriority = priority;
					disjunctScanType = TEXT_SCAN;
				}
			}
		}
    }

    // initialize bound condition
    indexInfoPoolIndex.setNull();
    resultConds = conds.getPoolIndex();

    // set accessMethod and indexType and boundconditions
    switch(disjunctScanType)
    {
    case BTREE_SCAN:
		if(isKNNQuery == 0)
		{
        accessMethod = CAP_ACCESSMETHOD_BTREE_INDEXSCAN;
        e = makePlan_determineScanMethod_BTreeScanAndJoin(currentPlanNo, classId, conds, disjunctIndex,
                                                          indexInfoPoolIndex, resultConds);
        OOSQL_CHECK_ERR(e);
		if(e == SM_FALSE)
            accessMethod = CAP_ACCESSMETHOD_SEQSCAN;
		else
		{
			e = makePlan_determineScanMethod_MakeBoolExpr(currentPlanNo, resultConds, 
				                                          indexInfoPoolIndex, resultConds);
			OOSQL_CHECK_ERR(e);
		}
		}
		else
		{
		// First process using MLGF index
	        accessMethod = CAP_ACCESSMETHOD_MLGF_MBR_INDEXSCAN;
    	    e = makePlan_determineScanMethod_MlgfMbrScan(currentPlanNo, classId, conds, kNNQueryIndex,
        	                                          indexInfoPoolIndex, resultConds);
    	    OOSQL_CHECK_ERR(e);
			if(e == SM_FALSE)
	            accessMethod = CAP_ACCESSMETHOD_SEQSCAN;
    	    else
			{
				// And temporaily process using B+-tree index and save it.
				AP_IndexInfoPoolIndex 	tidJoinIndexInfo;
				e = makePlan_determineScanMethod_BTreeScanAndJoin(currentPlanNo, classId, conds, disjunctIndex,
			    	                                              tidJoinIndexInfo, resultConds);

			    AP_IndexInfoPoolElements indexInfo(m_pool->indexInfoPool);
				indexInfo = indexInfoPoolIndex;
				indexInfo[0].scan.mlgfmbr.tidJoinIndexInfo = tidJoinIndexInfo;
																  
				e = makePlan_determineScanMethod_MakeBoolExpr(currentPlanNo, resultConds, 
					                                          indexInfoPoolIndex, resultConds);
				OOSQL_CHECK_ERR(e);
			}
		}
        break;

    case BTREE_JOIN:
        accessMethod = CAP_ACCESSMETHOD_BTREE_INDEXSCAN;
        e = makePlan_determineScanMethod_BTreeScanAndJoin(currentPlanNo, classId, conds, disjunctIndex,
                                                          indexInfoPoolIndex, resultConds);
        OOSQL_CHECK_ERR(e);
		if(e == SM_FALSE)
            accessMethod = CAP_ACCESSMETHOD_SEQSCAN;
		break;

    case TEXT_SCAN:
        accessMethod = CAP_ACCESSMETHOD_TEXT_INDEXSCAN;
        e = makePlan_determineScanMethod_TextScan(currentPlanNo, classId, conds, disjunctIndex,
                                                  indexInfoPoolIndex, resultConds);
        OOSQL_CHECK_ERR(e);
		if(e == SM_FALSE)
            accessMethod = CAP_ACCESSMETHOD_SEQSCAN;
        else
		{
			e = makePlan_determineScanMethod_MakeBoolExpr(currentPlanNo, resultConds, 
				                                          indexInfoPoolIndex, resultConds);
			OOSQL_CHECK_ERR(e);
		}
		break;

    case MLGF_MBR_SCAN:
        accessMethod = CAP_ACCESSMETHOD_MLGF_MBR_INDEXSCAN;
        e = makePlan_determineScanMethod_MlgfMbrScan(currentPlanNo, classId, conds, disjunctIndex,
                                                  indexInfoPoolIndex, resultConds);
        OOSQL_CHECK_ERR(e);
		if(e == SM_FALSE)
            accessMethod = CAP_ACCESSMETHOD_SEQSCAN;
        else
		{
			AP_IndexInfoPoolElements indexInfo(m_pool->indexInfoPool);
			indexInfo = indexInfoPoolIndex;
			indexInfo[0].scan.mlgfmbr.tidJoinIndexInfo.setNull();
			e = makePlan_determineScanMethod_MakeBoolExpr(currentPlanNo, resultConds, 
				                                          indexInfoPoolIndex, resultConds);
			OOSQL_CHECK_ERR(e);
		}
		break;

    case MLGF_MBR_JOIN:
        accessMethod = CAP_ACCESSMETHOD_MLGF_MBR_INDEXSCAN;
        e = makePlan_determineScanMethod_MlgfMbrJoin(currentPlanNo, classId, conds, disjunctIndex,
                                                  indexInfoPoolIndex, resultConds);
        OOSQL_CHECK_ERR(e);
		if(e == SM_FALSE)
            accessMethod = CAP_ACCESSMETHOD_SEQSCAN;
		else
		{
		    AP_IndexInfoPoolElements indexInfo(m_pool->indexInfoPool);
			indexInfo = indexInfoPoolIndex;
			indexInfo[0].scan.mlgfmbr.tidJoinIndexInfo.setNull();
		}
		break;

	case MLGF_SCAN:
	case MLGF_JOIN:
        accessMethod = CAP_ACCESSMETHOD_MLGF_INDEXSCAN;
        e = makePlan_determineScanMethod_MlgfScanAndJoin
			(currentPlanNo, classId, conds, disjunctIndex, indexInfoPoolIndex);
        OOSQL_CHECK_ERR(e);
		if(e == SM_FALSE)
            accessMethod = CAP_ACCESSMETHOD_SEQSCAN;
		break;

    default:
        accessMethod = CAP_ACCESSMETHOD_SEQSCAN;
        break;
    }

    return eNOERROR;
}

Four OQL_GDStoCommonAP::makePlan_determineScanMethod_MakeBoolExpr(Four planNo, AP_CondListPoolIndex condsPoolIndex,  
                                                                  AP_IndexInfoPoolIndex indexInfoPoolIndex, 
																  AP_CondListPoolIndex& resultConds)
{
	CommonAP_PoolElements       apNode(m_pool->commonAP_Pool);
	AP_CondListPoolElements		conds(m_pool->ap_condListPool);
	AP_BoolExprPoolElements		boolExpr(m_pool->ap_boolExprPool);
	AP_BoolExprPoolElements		resultBoolExprs(m_pool->ap_boolExprPool);
	AP_CondListPoolElements     resultCondsElements(m_pool->ap_condListPool);
	AP_ExprPoolElements         operand1(m_pool->ap_exprPool), operand2(m_pool->ap_exprPool);
	AP_ExprPoolElements         expr(m_pool->ap_exprPool);
	AP_ExprPoolElements			boolExprKey(m_pool->ap_exprPool);
	Boolean                     isCurrentCondsInBoundConds;
	OperatorID                  operatorId;
	Four						i, j;
	Four						e;
	CataClassInfo				classInfo;
	CataAttrInfo				attrInfo;
	TypeID						type;
	Four						length;

	resultConds.setNull();
	resultBoolExprs.setNull();
	conds = condsPoolIndex;

	e = m_catalog->class_ClassId_to_ClassInfo(apNode[planNo].classInfo.classId, classInfo);
	OOSQL_CHECK_ERR(e);

	for(i = 0; i < conds.size; i++)
	{
		expr = conds[i].expr;

		isCurrentCondsInBoundConds = SM_FALSE;

        if((expr[0].oper.operatorId == OP_EQ || expr[0].oper.operatorId == OP_NE ||
            expr[0].oper.operatorId == OP_GT || expr[0].oper.operatorId == OP_LT ||
            expr[0].oper.operatorId == OP_GE || expr[0].oper.operatorId == OP_LE))
        {
            // check each operand
            operand1 = expr[0].oper.operand1;
            operand2 = expr[0].oper.operand2;

            if((operand1[0].exprKind == EXPR_KIND_PATHEXPR && operand2[0].exprKind == EXPR_KIND_VALUE) || 
               (operand2[0].exprKind == EXPR_KIND_PATHEXPR && operand1[0].exprKind == EXPR_KIND_VALUE))
            {
                if(operand1[0].exprKind == EXPR_KIND_VALUE)
                {   // swap operand1 and operand2
                    operand1 = expr[0].oper.operand2;
                    operand2 = expr[0].oper.operand1;

					operatorId = reverseComparisionOperatorID(expr[0].oper.operatorId);
                }
				else
                    operatorId = expr[0].oper.operatorId;

				if(operand1[0].pathExpr.col.planNo == planNo)
				{
					e = m_catalog->attr_ColNo_to_AttrInfo(classInfo, operand1[0].pathExpr.col.colNo, attrInfo);
					OOSQL_CHECK_ERR(e);
					e = m_catalog->attr_GetTypeID(classInfo, attrInfo, type);
					OOSQL_CHECK_ERR(e);
					e = m_catalog->attr_GetLength(classInfo, attrInfo, length);
					OOSQL_CHECK_ERR(e);

					if(type == OOSQL_TYPE_STRING || type == OOSQL_TYPE_VARSTRING)
					{
						if(length > MAXKEYLEN)
							continue;	
					}
					else if(type == OOSQL_TYPE_TEXT)
						continue;		

					boolExpr = m_pool->ap_boolExprPool.addNewEntry();
					if(resultBoolExprs.isNull())
					{
						resultBoolExprs = boolExpr;
					}
					else
						resultBoolExprs.size ++;

					boolExpr[0].colNo	= operand1[0].pathExpr.col.colNo;
					boolExpr[0].type	= type;
					boolExpr[0].length	= length;
					switch(operatorId)
					{
					case OP_EQ:
						boolExpr[0].op = OOSQL_StorageManager::SM_EQ; break;
					case OP_NE:
						boolExpr[0].op = OOSQL_StorageManager::SM_NE; break;
					case OP_GT:
						boolExpr[0].op = OOSQL_StorageManager::SM_GT; break;
					case OP_LT:
						boolExpr[0].op = OOSQL_StorageManager::SM_LT; break;
					case OP_GE:
						boolExpr[0].op = OOSQL_StorageManager::SM_GE; break;
					case OP_LE:
						boolExpr[0].op = OOSQL_StorageManager::SM_LE; break;
					default:
						OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
					}

					boolExprKey = boolExpr[0].key = m_pool->ap_exprPool.addNewEntry();
					boolExprKey[0].exprKind     = EXPR_KIND_VALUE;
					boolExprKey[0].resultType   = boolExpr[0].type;
					boolExprKey[0].resultLength = boolExpr[0].length;
					boolExprKey[0].value        = operand2[0].value;

					isCurrentCondsInBoundConds = SM_TRUE;
				}
			}
		}

		if(isCurrentCondsInBoundConds == SM_FALSE)
		{
			resultCondsElements = m_pool->ap_condListPool.addNewEntry();

			if(resultConds.isNull())
				resultConds = resultCondsElements.getPoolIndex();
			else
				resultConds.size ++;

			resultCondsElements[0] = conds[i];
		}
	}

	AP_IndexInfoPoolElements indexInfo(m_pool->indexInfoPool);
	indexInfo = indexInfoPoolIndex;
	
	e = makePlan_determineScanMethod_MakeBoolExpr_AttachBoolExpr(indexInfo, resultBoolExprs);
	OOSQL_CHECK_ERR(e);

	return eNOERROR;
}

Four OQL_GDStoCommonAP::makePlan_determineScanMethod_MakeBoolExpr_AttachBoolExpr(AP_IndexInfoPoolElements indexInfo, AP_BoolExprPoolElements boolExprs)
{
	Four						e;
	Four						i, j;
	AP_BoolExprPoolElements		resultBoolExprs(m_pool->ap_boolExprPool);

	if(indexInfo[0].nodeKind == INDEXINFO_SCAN)
	{
		if(boolExprs.size == 0)
			indexInfo[0].scan.boolExprs.setNull();
		else
		{
			resultBoolExprs = m_pool->ap_boolExprPool.addNewEntry(boolExprs.size);
			for(i = 0; i < boolExprs.size; i++)
				resultBoolExprs[i] = boolExprs[i];

			indexInfo[0].scan.boolExprs = resultBoolExprs.getPoolIndex();

			for(i = 0; i < resultBoolExprs.size; i++)
			{
				for(j = 0; j < indexInfo[0].scan.nCols; j++)
				{
					if(resultBoolExprs[i].colNo == indexInfo[0].scan.columns[j].colNo)
						break;
				}
				if(j < indexInfo[0].scan.nCols)
				{
					resultBoolExprs[i].canReadFromIndexCursor = SM_TRUE;
				}
				else
					resultBoolExprs[i].canReadFromIndexCursor = SM_FALSE;
			}
		}
	}
	else if(indexInfo[0].nodeKind == INDEXINFO_OPERATOR)
	{
		AP_IndexInfoPoolElements op1IndexInfo(m_pool->indexInfoPool);
		AP_IndexInfoPoolElements op2IndexInfo(m_pool->indexInfoPool);

		op1IndexInfo = indexInfo[0].oper.op1;
		op2IndexInfo = indexInfo[0].oper.op2;

		e = makePlan_determineScanMethod_MakeBoolExpr_AttachBoolExpr(op1IndexInfo, boolExprs);
		OOSQL_CHECK_ERR(e);
		e = makePlan_determineScanMethod_MakeBoolExpr_AttachBoolExpr(op2IndexInfo, boolExprs);
		OOSQL_CHECK_ERR(e);
	}
	else
	{
		OOSQL_ERR(eINTERNALERROR_OOSQL);
	}

	return eNOERROR;
}

Four OQL_GDStoCommonAP::makePlan_determineScanMethod_BTreeScanAndJoin(Four currentPlanNo, Four classId, AP_CondListPoolElements conds, Four condNumber,
                                                                      AP_IndexInfoPoolIndex& indexInfoPoolIndex, AP_CondListPoolIndex& resultConds)
{
    CommonAP_PoolElements       apNode_sub(m_pool->commonAP_Pool);
    AP_ExprPoolElements         expr(m_pool->ap_exprPool);
    AP_ExprPoolElements         operand1(m_pool->ap_exprPool), operand2(m_pool->ap_exprPool);
    AP_IndexInfoPoolElements    indexInfo(m_pool->indexInfoPool);
    Boolean                     operandSwapped;
    Four                        e;
	Boolean                     flag;
	CataClassInfo				classInfo;

	e = m_catalog->class_ClassId_to_ClassInfo(classId, classInfo);
	OOSQL_CHECK_ERR(e);

    // check if there is a restriction predicate, if it exists and it has an index then use INDEXSCAN
    expr = conds[condNumber].expr;

    // check operator, only arithmetic operator can be applied
    if(!(expr[0].oper.operatorId == OP_EQ || expr[0].oper.operatorId == OP_NE ||
         expr[0].oper.operatorId == OP_GT || expr[0].oper.operatorId == OP_LT ||
         expr[0].oper.operatorId == OP_GE || expr[0].oper.operatorId == OP_LE ||
		 expr[0].oper.operatorId == OP_LIKE || expr[0].oper.operatorId == OP_IN))
        return SM_FALSE;       // index can't be used

    // check each operand
    operand1 = expr[0].oper.operand1;
    operand2 = expr[0].oper.operand2;
    operandSwapped = SM_FALSE;

    if((operand1[0].exprKind == EXPR_KIND_PATHEXPR && operand2[0].exprKind == EXPR_KIND_VALUE) || 
       (operand2[0].exprKind == EXPR_KIND_PATHEXPR && operand1[0].exprKind == EXPR_KIND_VALUE))
    {   
		// check restriction predicate
        if(operand1[0].exprKind == EXPR_KIND_VALUE)
        {   // swap operand1 and operand2
            operand1 = expr[0].oper.operand2;
            operand2 = expr[0].oper.operand1;

            operandSwapped = SM_TRUE;
        }
            
        // check if there is an index
        if(operand1[0].pathExpr.kind == PATHEXPR_KIND_ATTR)
        {
            // reference ap that operand1.pathExpr is accessed
            apNode_sub.setPoolIndex(operand1[0].pathExpr.col.planNo, 1);

            if(apNode_sub[0].classInfo.classKind == CLASSKIND_PERSISTENT)
            {
                CataClassInfo classInfo;
                CataAttrInfo  attrInfo;

                e = m_catalog->class_ClassId_to_ClassInfo(classId, classInfo);
                OOSQL_CHECK_ERR(e);
                e = m_catalog->attr_ColNo_to_AttrInfo(classInfo, operand1[0].pathExpr.col.colNo, attrInfo);
                OOSQL_CHECK_ERR(e);
                    
                e = m_catalog->index_isB_TreeIndexExist(classInfo, attrInfo);
                OOSQL_CHECK_ERR(e);
                if(e == SM_TRUE)
                {
					OOSQL_StorageManager::IndexID	indexId_array[1];
                    Four							nIndexes = 1;

                    e = m_catalog->index_getB_TreeIndexes(classInfo, attrInfo, nIndexes, indexId_array);
                    OOSQL_CHECK_ERR(e);

                    // make indexInfo
                    indexInfo                       = m_pool->indexInfoPool.addNewEntry();
                    indexInfoPoolIndex              = indexInfo.getPoolIndex();
                    indexInfo[0].nodeKind           = INDEXINFO_SCAN;
                    indexInfo[0].scan.indexType     = INDEXTYPE_BTREE;
                    indexInfo[0].scan.indexId       = indexId_array[0];
					indexInfo[0].scan.boolExprs.setNull();
					indexInfo[0].scan.nCols         = 0;
                    
					e = m_catalog->class_ClassInfo_to_ClassId(classInfo, indexInfo[0].scan.classId);
					OOSQL_CHECK_ERR(e);

					e = m_catalog->attr_AttrInfo_to_ColNo(classInfo, attrInfo, indexInfo[0].scan.colNo);
					OOSQL_CHECK_ERR(e);

					Two								keyColNo[MAXNUMKEYPARTS];
					OOSQL_StorageManager::KeyDesc	btreeKeyDesc;
					AP_ColNoMapPoolElements			colNoMap;
					AP_UsedColPoolElements			usedColInfo;
					Four							i, j;
					CataIndexInfo					cataIndexInfo;

					colNoMap    = m_pool->commonAP_Pool[currentPlanNo].colNoMap.getElements(m_pool->colNoMapPool);
					usedColInfo = m_pool->commonAP_Pool[currentPlanNo].usedColInfo.getElements(m_pool->usedColPool);

					e = m_catalog->index_IndexId_to_IndexInfo(classInfo, indexId_array[0], cataIndexInfo);
					OOSQL_CHECK_ERR(e);

					e = m_catalog->index_GetIndexDesc(classInfo, cataIndexInfo, btreeKeyDesc);
					OOSQL_CHECK_ERR(e);
					memcpy(&indexInfo[0].scan.btreeKeyDesc, &btreeKeyDesc, sizeof(btreeKeyDesc));

					e = m_catalog->index_GetIndexColumns(classInfo, cataIndexInfo, keyColNo);
					OOSQL_CHECK_ERR(e);

                    // determine start bound and stop bound
                    e = makePlan_determineBoundConditionForB_TreeScan(operand1[0].pathExpr.col.planNo,
                                                                      btreeKeyDesc.nparts, keyColNo, conds, 
                                                                      indexInfo[0].scan.btree.startBound,
                                                                      indexInfo[0].scan.btree.stopBound,
                                                                      resultConds, operandSwapped,
																	  classInfo, btreeKeyDesc);
                    OOSQL_CHECK_ERR(e);
    
					indexInfo[0].scan.nCols = 0;
					for(i = 0; i < colNoMap.size; i++)			
					{
						if(colNoMap[i].offset != NOT_USED_COLUMN)	
						{
							for(j = 0; j < btreeKeyDesc.nparts; j++)
							{
								if(keyColNo[j] == usedColInfo[colNoMap[i].offset].colNo)
									break;
							}
							if(j == btreeKeyDesc.nparts)
								break;							
						}
					}

					if(i > 0 && i == colNoMap.size)				
					{
						indexInfo[0].scan.readObjectValueFromIndexFlag = SM_TRUE;
						for(i = 0; i < btreeKeyDesc.nparts; i++)
						{
							indexInfo[0].scan.columns[i].colNo  = keyColNo[i];
							indexInfo[0].scan.columns[i].type   = btreeKeyDesc.kpart[i].type;
							indexInfo[0].scan.columns[i].length = btreeKeyDesc.kpart[i].length;
							
						}
						indexInfo[0].scan.nCols = btreeKeyDesc.nparts;
					}
					else							
					{
						indexInfo[0].scan.readObjectValueFromIndexFlag = SM_FALSE;
						indexInfo[0].scan.nCols                        = 0;
					}

                    // scan method is determined
                    return SM_TRUE;
                }
            }
        }
    }
    else if(operand1[0].exprKind == EXPR_KIND_PATHEXPR && operand2[0].exprKind == EXPR_KIND_PATHEXPR)
	{
        flag = SM_FALSE;
        if(operand1[0].pathExpr.kind == PATHEXPR_KIND_ATTR)
        {
            if(currentPlanNo == operand1[0].pathExpr.col.planNo)
                flag = SM_TRUE;
        }
        
        if(operand2[0].pathExpr.kind == PATHEXPR_KIND_ATTR && !flag)
        {
            if(currentPlanNo == operand2[0].pathExpr.col.planNo)
            {
                flag = SM_TRUE;

                // swap operand1 and operand2
                operand1 = expr[0].oper.operand2;
                operand2 = expr[0].oper.operand1;

                operandSwapped = SM_TRUE;
            }
        }

        if(flag)
        {
            // check if there is an index
            if(operand1[0].pathExpr.kind == PATHEXPR_KIND_ATTR)
            {
                // reference ap that operand1.pathExpr is accessed
                apNode_sub.setPoolIndex(operand1[0].pathExpr.col.planNo, 1);

                if(apNode_sub[0].classInfo.classKind == CLASSKIND_PERSISTENT)
                {
                    CataClassInfo classInfo;
                    CataAttrInfo  attrInfo;

                    e = m_catalog->class_ClassId_to_ClassInfo(classId, classInfo);
                    OOSQL_CHECK_ERR(e);
                    e = m_catalog->attr_ColNo_to_AttrInfo(classInfo, operand1[0].pathExpr.col.colNo, attrInfo);
                    OOSQL_CHECK_ERR(e);

                    e = m_catalog->index_isB_TreeIndexExist(classInfo, attrInfo);
                    OOSQL_CHECK_ERR(e);

                    if(e == SM_TRUE)
                    {
                        OOSQL_StorageManager::IndexID indexId_array[1];
                        Four        nIndexes = 1;

                        e = m_catalog->index_getB_TreeIndexes(classInfo, attrInfo, nIndexes, indexId_array);
                        OOSQL_CHECK_ERR(e);

                        // make indexInfo
                        indexInfo                   = m_pool->indexInfoPool.addNewEntry();
                        indexInfoPoolIndex          = indexInfo.getPoolIndex();
                        indexInfo[0].nodeKind       = INDEXINFO_SCAN;
                        indexInfo[0].scan.indexType = INDEXTYPE_BTREE;
                        indexInfo[0].scan.indexId   = indexId_array[0];
						indexInfo[0].scan.boolExprs.setNull();
						indexInfo[0].scan.nCols     = 0;
						
						e = m_catalog->class_ClassInfo_to_ClassId(classInfo, indexInfo[0].scan.classId);
						OOSQL_CHECK_ERR(e);

						e = m_catalog->attr_AttrInfo_to_ColNo(classInfo, attrInfo, indexInfo[0].scan.colNo);
						OOSQL_CHECK_ERR(e);
					
						Two								keyColNo[MAXNUMKEYPARTS];
						OOSQL_StorageManager::KeyDesc	btreeKeyDesc;
						AP_ColNoMapPoolElements			colNoMap;
						AP_UsedColPoolElements			usedColInfo;
						Four							i, j;
						CataIndexInfo					cataIndexInfo;

						colNoMap    = m_pool->commonAP_Pool[currentPlanNo].colNoMap.getElements(m_pool->colNoMapPool);
						usedColInfo = m_pool->commonAP_Pool[currentPlanNo].usedColInfo.getElements(m_pool->usedColPool);

						e = m_catalog->index_IndexId_to_IndexInfo(classInfo, indexId_array[0], cataIndexInfo);
						OOSQL_CHECK_ERR(e);

						e = m_catalog->index_GetIndexDesc(classInfo, cataIndexInfo, btreeKeyDesc);
						OOSQL_CHECK_ERR(e);
						memcpy(&indexInfo[0].scan.btreeKeyDesc, &btreeKeyDesc, sizeof(btreeKeyDesc));

						e = m_catalog->index_GetIndexColumns(classInfo, cataIndexInfo, keyColNo);
						OOSQL_CHECK_ERR(e);

						// determine start bound and stop bound
                        e = makePlan_determineBoundConditionForB_TreeJoin(operand1[0].pathExpr.col.planNo,
                                                                          btreeKeyDesc.nparts, keyColNo, conds, 
                                                                          indexInfo[0].scan.btree.startBound,
                                                                          indexInfo[0].scan.btree.stopBound,
                                                                          resultConds, operandSwapped, btreeKeyDesc);
                        OOSQL_CHECK_ERR(e);

						indexInfo[0].scan.nCols = 0;
						for(i = 0; i < colNoMap.size; i++)	
						{
							if(colNoMap[i].offset != NOT_USED_COLUMN)	
							{
								for(j = 0; j < btreeKeyDesc.nparts; j++)
								{
									if(keyColNo[j] == usedColInfo[colNoMap[i].offset].colNo)
										break;
								}
								if(j == btreeKeyDesc.nparts)
									break;							
							}
						}

						if(i > 0 && i == colNoMap.size)	
						{
							indexInfo[0].scan.readObjectValueFromIndexFlag = SM_TRUE;
							for(i = 0; i < btreeKeyDesc.nparts; i++)
							{
								indexInfo[0].scan.columns[i].colNo  = keyColNo[i];
								indexInfo[0].scan.columns[i].type   = btreeKeyDesc.kpart[i].type;
								indexInfo[0].scan.columns[i].length = btreeKeyDesc.kpart[i].length;
							}
							indexInfo[0].scan.nCols = btreeKeyDesc.nparts;
						}
						else							
						{
							indexInfo[0].scan.readObjectValueFromIndexFlag = SM_FALSE;
							indexInfo[0].scan.nCols                        = 0;
						}

                        // scan method is determined
                        return SM_TRUE;
                    }
                }
            }
        }
	}

    return SM_FALSE;
}

Four OQL_GDStoCommonAP::makePlan_determineScanMethod_MlgfScanAndJoin
(
	Four					currentPlanNo,
	Four					classId, 
	AP_CondListPoolElements	conds, 
	Four					condNumber,
    AP_IndexInfoPoolIndex&	indexInfoPoolIndex
)
{
	CommonAP_PoolElements       apNode_sub(m_pool->commonAP_Pool);
    AP_ExprPoolElements         expr(m_pool->ap_exprPool);
    AP_ExprPoolElements         operand1(m_pool->ap_exprPool), operand2(m_pool->ap_exprPool);
    AP_IndexInfoPoolElements    indexInfo(m_pool->indexInfoPool);
	AP_ValuePoolElements        value(m_pool->valuePool);
    Four                        e;
    Boolean						flag;

	// check if there is a restriction predicate, if it exists and it has an index then use INDEXSCAN
    expr = conds[condNumber].expr;

    // check operator, only arithmetic operator can be applied
	if(!(expr[0].oper.operatorId == OP_EQ || expr[0].oper.operatorId == OP_NE ||
		 expr[0].oper.operatorId == OP_GT || expr[0].oper.operatorId == OP_LT ||
		 expr[0].oper.operatorId == OP_GE || expr[0].oper.operatorId == OP_LE ||
		 expr[0].oper.operatorId == OP_ISNULL || expr[0].oper.operatorId == OP_ISNOTNULL ))
		return SM_FALSE;       // index can't be used

    // check each operand
    operand1 = expr[0].oper.operand1;
    operand2 = expr[0].oper.operand2;

    if((operand1[0].exprKind == EXPR_KIND_PATHEXPR && operand2[0].exprKind == EXPR_KIND_VALUE) || 
       (operand2[0].exprKind == EXPR_KIND_PATHEXPR && operand1[0].exprKind == EXPR_KIND_VALUE))
    {   
		// check restriction predicate
        if(operand1[0].exprKind == EXPR_KIND_VALUE)
        {   
			// swap operand1 and operand2
            operand1 = expr[0].oper.operand2;
            operand2 = expr[0].oper.operand1;
        }
        
        if( (operand1[0].pathExpr.kind == PATHEXPR_KIND_ATTR) && 
        	(operand1[0].pathExpr.col.planNo == currentPlanNo) )
		{
        	flag = SM_TRUE;
		}
	}
    else if(operand1[0].exprKind == EXPR_KIND_PATHEXPR && operand2[0].exprKind == EXPR_KIND_PATHEXPR)
	{
        flag = SM_FALSE;
        if(operand1[0].pathExpr.kind == PATHEXPR_KIND_ATTR)
        {
            if(currentPlanNo == operand1[0].pathExpr.col.planNo)
                flag = SM_TRUE;
        }
        
        if(operand2[0].pathExpr.kind == PATHEXPR_KIND_ATTR && !flag)
        {
            if(currentPlanNo == operand2[0].pathExpr.col.planNo)
            {
                flag = SM_TRUE;

                // swap operand1 and operand2
                operand1 = expr[0].oper.operand2;
                operand2 = expr[0].oper.operand1;
            }
        }
    }
        
    // check if there is an index
    if(flag)
    {
        // reference ap that operand1.pathExpr is accessed
        apNode_sub.setPoolIndex(operand1[0].pathExpr.col.planNo, 1);

        if(apNode_sub[0].classInfo.classKind == CLASSKIND_PERSISTENT)
        {
            CataClassInfo classInfo;
            CataAttrInfo  attrInfo;

            e = m_catalog->class_ClassId_to_ClassInfo(classId, classInfo);
            OOSQL_CHECK_ERR(e);

            e = m_catalog->attr_ColNo_to_AttrInfo(classInfo, operand1[0].pathExpr.col.colNo, attrInfo);
            OOSQL_CHECK_ERR(e);
                
            e = m_catalog->index_isMlgfIndexExist(classInfo, attrInfo);
            OOSQL_CHECK_ERR(e);
            
            if(e == SM_TRUE)
            {
				OOSQL_StorageManager::IndexID	indexId_array[1];
                Four							nIndexes = 1;

                e = m_catalog->index_getMlgfIndexes(classInfo, attrInfo, nIndexes, indexId_array);
                OOSQL_CHECK_ERR(e);

                // make indexInfo
                indexInfo                       = m_pool->indexInfoPool.addNewEntry();
                indexInfoPoolIndex              = indexInfo.getPoolIndex();
                indexInfo[0].nodeKind           = INDEXINFO_SCAN;
                indexInfo[0].scan.indexType     = INDEXTYPE_MLGF;
                indexInfo[0].scan.indexId       = indexId_array[0];
				indexInfo[0].scan.boolExprs.setNull();
				indexInfo[0].scan.nCols         = 0;
                
				e = m_catalog->class_ClassInfo_to_ClassId(classInfo, indexInfo[0].scan.classId);
				OOSQL_CHECK_ERR(e);

				e = m_catalog->attr_AttrInfo_to_ColNo(classInfo, attrInfo, indexInfo[0].scan.colNo);
				OOSQL_CHECK_ERR(e);

				Two									keyColNo[MLGF_MAXNUM_KEYS];
				OOSQL_StorageManager::MLGF_KeyDesc	mlgfKeyDesc;					
				CataIndexInfo						cataIndexInfo;

				e = m_catalog->index_IndexId_to_IndexInfo(classInfo, indexId_array[0], cataIndexInfo);
				OOSQL_CHECK_ERR(e);

				e = m_catalog->index_GetIndexDesc(classInfo, cataIndexInfo, mlgfKeyDesc);
				OOSQL_CHECK_ERR(e);

				memcpy(&indexInfo[0].scan.mlgfKeyDesc, &mlgfKeyDesc, sizeof(mlgfKeyDesc));

				e = m_catalog->index_GetIndexColumns(classInfo, cataIndexInfo, keyColNo);
				OOSQL_CHECK_ERR(e);
				
				indexInfo[0].scan.mlgf.nKeys = mlgfKeyDesc.nKeys;

				e = makePlan_determineBoundConditionForMlgfScanAndJoin(operand1[0].pathExpr.col.planNo,
																keyColNo,
																conds,
																indexInfo[0].scan.mlgf,
																classInfo);
				OOSQL_CHECK_ERR(e);

				return SM_TRUE;
			}
		}
	}

	return SM_FALSE;
}

Four OQL_GDStoCommonAP::get_nKeysUsedInCondition
(
	Four					currentPlanNo,
	Four					classId,
	Four					&nKeys, 
	AP_CondListPoolElements	conds, 
	Four					condNumber, 
	Four					indexType,
	Boolean					joinFlag
)
{
	Four e, i, j, colIndex, nKeyCols;
	AP_ExprPoolElements	expr(m_pool->ap_exprPool);
	AP_ExprPoolElements	key(m_pool->ap_exprPool);
	AP_ExprPoolElements	operand1(m_pool->ap_exprPool);
	AP_ExprPoolElements	operand2(m_pool->ap_exprPool);

	CataClassInfo						classInfo;
	CataAttrInfo						attrInfo;
	OOSQL_StorageManager::IndexID		indexId_array[1];
	Four								nIndexes = 1;
	CataIndexInfo						cataIndexInfo;
	
	OOSQL_StorageManager::MLGF_KeyDesc	mlgfKeyDesc;
	OOSQL_StorageManager::KeyDesc		btreeKeyDesc;

	Two									*keyColNo;

	expr = conds[condNumber].expr;
	operand1 = expr[0].oper.operand1;
    operand2 = expr[0].oper.operand2;

	if((operand1[0].exprKind == EXPR_KIND_PATHEXPR && operand2[0].exprKind == EXPR_KIND_VALUE) || 
	   (operand2[0].exprKind == EXPR_KIND_PATHEXPR && operand1[0].exprKind == EXPR_KIND_VALUE))
	{
		if(operand1[0].exprKind == EXPR_KIND_VALUE)
		{   
			key = expr[0].oper.operand2;
		}
		else
		{
			key = expr[0].oper.operand1;
		}
	}
	else if(operand1[0].exprKind == EXPR_KIND_PATHEXPR && operand2[0].exprKind == EXPR_KIND_PATHEXPR)
	{
		Boolean flag;
		
        flag = SM_FALSE;
        if(operand1[0].pathExpr.kind == PATHEXPR_KIND_ATTR)
        {
            if(currentPlanNo == operand1[0].pathExpr.col.planNo)
			{
                flag = SM_TRUE;
				key = expr[0].oper.operand1;
			}
        }
        
        if(operand2[0].pathExpr.kind == PATHEXPR_KIND_ATTR && !flag)
        {
            if(currentPlanNo == operand2[0].pathExpr.col.planNo)
            {
                key = expr[0].oper.operand2;
            }
        }
	}
	else
		return eNOERROR;

	e = m_catalog->class_ClassId_to_ClassInfo(classId, classInfo);
	OOSQL_CHECK_ERR(e);

	e = m_catalog->attr_ColNo_to_AttrInfo(classInfo, key[0].pathExpr.col.colNo, attrInfo);
	OOSQL_CHECK_ERR(e);
    
	if(indexType == INDEXTYPE_BTREE)
	{
		e = m_catalog->index_getB_TreeIndexes(classInfo, attrInfo, nIndexes, indexId_array);
		OOSQL_CHECK_ERR(e);
	}
	else if(indexType == INDEXTYPE_MLGF)
	{
		e = m_catalog->index_getMlgfIndexes(classInfo, attrInfo, nIndexes, indexId_array);
		OOSQL_CHECK_ERR(e);
	}
	
	e = m_catalog->index_IndexId_to_IndexInfo(classInfo, indexId_array[0], cataIndexInfo);
	OOSQL_CHECK_ERR(e);

	if(indexType == INDEXTYPE_BTREE)
	{
		e = m_catalog->index_GetIndexDesc(classInfo, cataIndexInfo, btreeKeyDesc);
		OOSQL_CHECK_ERR(e);

		nKeyCols = btreeKeyDesc.nparts;
	}
	else if(indexType == INDEXTYPE_MLGF)
	{
		e = m_catalog->index_GetIndexDesc(classInfo, cataIndexInfo, mlgfKeyDesc);
		OOSQL_CHECK_ERR(e);

		nKeyCols = mlgfKeyDesc.nKeys;	
	}

	keyColNo = (Two*)pMemoryManager->Alloc(sizeof(Two) * nKeyCols);
	
	e = m_catalog->index_GetIndexColumns(classInfo, cataIndexInfo, keyColNo);
	OOSQL_CHECK_ERR(e);

	for(nKeys = 0, i = 0; i < conds.size; i++)
	{
		expr = conds[i].expr;

        // check operator, only arithmetic operator can be applied
        if((expr[0].oper.operatorId == OP_EQ || expr[0].oper.operatorId == OP_NE ||
            expr[0].oper.operatorId == OP_GT || expr[0].oper.operatorId == OP_LT ||
            expr[0].oper.operatorId == OP_GE || expr[0].oper.operatorId == OP_LE))
        {
            // check each operand
            operand1 = expr[0].oper.operand1;
            operand2 = expr[0].oper.operand2;
			
			if(joinFlag == SM_FALSE || indexType == INDEXTYPE_MLGF)
			{
				if((operand1[0].exprKind == EXPR_KIND_PATHEXPR && operand2[0].exprKind == EXPR_KIND_VALUE) || 
				   (operand2[0].exprKind == EXPR_KIND_PATHEXPR && operand1[0].exprKind == EXPR_KIND_VALUE))
				{
					if(operand1[0].exprKind == EXPR_KIND_VALUE)
					{   
						key = expr[0].oper.operand2;
					}
					else
					{
						key = expr[0].oper.operand1;
					}

					for(colIndex = -1, j = 0; j < nKeyCols; j++)
					{
						if(key[0].pathExpr.col.planNo == currentPlanNo && key[0].pathExpr.col.colNo == keyColNo[j])
						{
							colIndex = j;
							break;
						}
					}
					
					if(colIndex >= 0)	// find the matching column
					{
						nKeys++;
					}
				}
			}
			
			if(joinFlag == SM_TRUE || indexType == INDEXTYPE_MLGF)
			{
				if((operand1[0].exprKind == EXPR_KIND_PATHEXPR && operand2[0].exprKind == EXPR_KIND_PATHEXPR))
				{
					for(colIndex = -1, j = 0; j < nKeyCols; j++)
					{
						if(operand2[0].pathExpr.col.planNo == currentPlanNo && operand2[0].pathExpr.col.colNo == keyColNo[j])
						{
							colIndex = j;
							break;
						}
					}

					if(colIndex >= 0)
					{   
						key = expr[0].oper.operand2;
					}
					else
					{
						key = expr[0].oper.operand1;
					}
					
					for(colIndex = -1, j = 0; j < nKeyCols; j++)
					{
						if(key[0].pathExpr.col.planNo == currentPlanNo && key[0].pathExpr.col.colNo == keyColNo[j])
						{
							colIndex = j;
							break;
						}
					}

					if(colIndex >= 0)	// find the matching column
					{
						nKeys++;
					}
				}			
			}
		}
	}

	pMemoryManager->Free(keyColNo);

	return eNOERROR;
}

Four OQL_GDStoCommonAP::makePlan_determineBoundConditionForMlgfScanAndJoin
(
	Four					planNo,
	Two*					colNos,
	AP_CondListPoolElements	conds,
	AP_MlgfIndexCond&		mlgf,
	CataClassInfo			classInfo
)
{
	Four				e, i, j, colIndex;
	AP_ExprPoolElements	expr(m_pool->ap_exprPool);
	AP_ExprPoolElements	operand1(m_pool->ap_exprPool);
	AP_ExprPoolElements	operand2(m_pool->ap_exprPool);
	OperatorID          operatorId;
	Boolean				operandSwapped = SM_FALSE;
	Four				nKeyCols = mlgf.nKeys;

	mlgf.lowerBoundExpr = m_pool->ap_exprPool.addNewEntry(nKeyCols);
	mlgf.upperBoundExpr = m_pool->ap_exprPool.addNewEntry(nKeyCols);
	
	for(i = 0; i < nKeyCols; i++)
	{
		mlgf.lowerBound[i] = MLGF_MIN_HASHVALUE;
		mlgf.upperBound[i] = MLGF_MAX_HASHVALUE;

		mlgf.lowerBoundExprFlag[i] = SM_FALSE;
		mlgf.upperBoundExprFlag[i] = SM_FALSE;
	}

	for(i = 0; i < conds.size; i++)
	{
		expr = conds[i].expr;

        // check operator, only arithmetic operator can be applied
        if((expr[0].oper.operatorId == OP_EQ || expr[0].oper.operatorId == OP_NE ||
            expr[0].oper.operatorId == OP_GT || expr[0].oper.operatorId == OP_LT ||
            expr[0].oper.operatorId == OP_GE || expr[0].oper.operatorId == OP_LE))
        {
            // check each operand
            operand1 = expr[0].oper.operand1;
            operand2 = expr[0].oper.operand2;
			
            if((operand1[0].exprKind == EXPR_KIND_PATHEXPR && operand2[0].exprKind == EXPR_KIND_VALUE) || 
               (operand2[0].exprKind == EXPR_KIND_PATHEXPR && operand1[0].exprKind == EXPR_KIND_VALUE))
            {
                if(operand1[0].exprKind == EXPR_KIND_VALUE)
                {   
					// swap operand1 and operand2
                    operand1 = expr[0].oper.operand2;
                    operand2 = expr[0].oper.operand1;
					operandSwapped = SM_TRUE;
                }

				for(colIndex = -1, j = 0; j < nKeyCols; j++)
				{
					if(operand1[0].pathExpr.col.planNo == planNo && operand1[0].pathExpr.col.colNo == colNos[j])
					{
						colIndex = j;
						break;
					}
				}
				
                if(colIndex >= 0)	// find the matching column
                {
                    // found condition which used in index scan
                    if(operandSwapped)
                        operatorId = reverseComparisionOperatorID(expr[0].oper.operatorId);
                    else
						operatorId = expr[0].oper.operatorId;

					e = makePlan_constructBoundConditionForMlgfScanAndJoin(operatorId, operand2, colIndex, mlgf, SM_FALSE);
					OOSQL_CHECK_ERR(e);
                }
			}
            else if((operand1[0].exprKind == EXPR_KIND_PATHEXPR && operand2[0].exprKind == EXPR_KIND_PATHEXPR))
            {
				for(colIndex = -1, j = 0; j < nKeyCols; j++)
				{
					if(operand2[0].pathExpr.col.planNo == planNo && operand2[0].pathExpr.col.colNo == colNos[j])
					{
						colIndex = j;
						break;
					}
				}

                if(colIndex >= 0)
                {   
					// swap operand1 and operand2
                    operand1 = expr[0].oper.operand2;
                    operand2 = expr[0].oper.operand1;
					operandSwapped = SM_TRUE;
                }
				else
				{
					for(colIndex = -1, j = 0; j < nKeyCols; j++)
					{
						if(operand1[0].pathExpr.col.planNo == planNo && operand1[0].pathExpr.col.colNo == colNos[j])
						{
							colIndex = j;
							break;
						}
					}
				}

                if(colIndex >= 0 && operand2[0].pathExpr.col.planNo < planNo)	
                {
                    // found condition which used in index scan
                    if(operandSwapped)
                        operatorId = reverseComparisionOperatorID(expr[0].oper.operatorId);
                    else
						operatorId = expr[0].oper.operatorId;

					e = makePlan_constructBoundConditionForMlgfScanAndJoin(operatorId, operand2, colIndex, mlgf, SM_TRUE);
					OOSQL_CHECK_ERR(e);

                }
			}
		}
	}

	return eNOERROR;
}

Four OQL_GDStoCommonAP::makePlan_constructBoundConditionForMlgfScanAndJoin
(
	OperatorID              operatorId,
	AP_ExprPoolElements&	operand,
	Four					colIndex,
	AP_MlgfIndexCond&		mlgf,
	Boolean					joinFlag
)
{
	Four				e;
	
	AP_ExprPoolElements lowerBoundExpr(m_pool->ap_exprPool);
	AP_ExprPoolElements upperBoundExpr(m_pool->ap_exprPool);
	
	ValuePoolElements   value(m_pool->valuePool);
	long				intVal;
	
	lowerBoundExpr = mlgf.lowerBoundExpr;
	upperBoundExpr = mlgf.upperBoundExpr;

	if(joinFlag == SM_FALSE)
	{
		value = operand[0].value;
		
		if(value[0].valueKind == VALUE_KIND_INTEGER)
		{
			e = getIntegerFromIntegerPool(value[0].integer, intVal);
			OOSQL_CHECK_ERR(e);
		}
		else
		{
			OOSQL_ERR(eTYPE_ERROR_OOSQL);
		}
	}
	
	switch(operatorId)
	{
	case OP_EQ:
		if(joinFlag)
		{
			mlgf.lowerBoundExprFlag[colIndex] = SM_TRUE;
			mlgf.upperBoundExprFlag[colIndex] = SM_TRUE;

			lowerBoundExpr[colIndex] = operand[0];
			upperBoundExpr[colIndex] = operand[0];
		}
		else
		{
			mlgf.lowerBound[colIndex] = intVal;
			mlgf.upperBound[colIndex] = intVal;
		}
		break;

	case OP_NE:
		break;

	case OP_GT:
	case OP_GE:
		if(joinFlag)
		{
			if(mlgf.upperBound[colIndex] == MLGF_MAX_HASHVALUE && mlgf.lowerBound[colIndex] == MLGF_MIN_HASHVALUE)
			{
				mlgf.lowerBoundExprFlag[colIndex] = SM_TRUE;
				lowerBoundExpr[colIndex] = operand[0];
			}
		}
		else if(mlgf.lowerBound[colIndex] != mlgf.upperBound[colIndex])
		{
			mlgf.lowerBoundExprFlag[colIndex] = SM_FALSE;
			mlgf.upperBoundExprFlag[colIndex] = SM_FALSE;
			mlgf.lowerBound[colIndex] = MAX(mlgf.lowerBound[colIndex], intVal);
		}
		break;

	case OP_LT:
	case OP_LE:
		if(joinFlag)
		{
			if(mlgf.upperBound[colIndex] == MLGF_MAX_HASHVALUE && mlgf.lowerBound[colIndex] == MLGF_MIN_HASHVALUE)
			{
				mlgf.upperBoundExprFlag[colIndex] = SM_TRUE;
				upperBoundExpr[colIndex] = operand[0];
			}
		}
		else if(mlgf.lowerBound[colIndex] != mlgf.upperBound[colIndex])
		{
			mlgf.lowerBoundExprFlag[colIndex] = SM_FALSE;
			mlgf.upperBoundExprFlag[colIndex] = SM_FALSE;
			mlgf.upperBound[colIndex] = MIN(mlgf.upperBound[colIndex], intVal);
		}
		break;
	case OP_ISNULL:
	case OP_ISNOTNULL:
		break;

	default:
		OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
		break;
	}

	return eNOERROR;
}


Four OQL_GDStoCommonAP::isOGISOPID
(
	Four operatorID
)
{
	if(operatorID == OP_OGIS_GEOMETRYFT ||
		operatorID == OP_OGIS_POINTFT ||
		operatorID == OP_OGIS_LINESTRINGFT ||
		operatorID == OP_OGIS_POLYGONFT ||
		operatorID == OP_OGIS_MULTIPOINTFT ||
		operatorID == OP_OGIS_MULTILINESTRINGFT ||
		operatorID == OP_OGIS_MULTIPOLYGONFT ||
		operatorID == OP_OGIS_GEOMETRYCOLLECTIONFT ||
		operatorID == OP_OGIS_GEOMETRYFB ||
		operatorID == OP_OGIS_POINTFB ||
		operatorID == OP_OGIS_LINESTRINGFB ||
		operatorID == OP_OGIS_POLYGONFB ||
		operatorID == OP_OGIS_MULTIPOINTFB ||
		operatorID == OP_OGIS_MULTILINESTRINGFB ||
		operatorID == OP_OGIS_MULTIPOLYGONFB ||
		operatorID == OP_OGIS_GEOMETRYCOLLECTIONFB ||
		operatorID == OP_OGIS_ASTEXT ||
		operatorID == OP_OGIS_ASBINARY ||
		operatorID == OP_OGIS_DIMENSION ||
		operatorID == OP_OGIS_GEOMETRYTYPE ||
		operatorID == OP_OGIS_SRID ||
		operatorID == OP_OGIS_BOUNDARY ||
		operatorID == OP_OGIS_LENGTH ||
		operatorID == OP_OGIS_X ||
		operatorID == OP_OGIS_Y ||
		operatorID == OP_OGIS_AREA ||
		operatorID == OP_OGIS_NUMGEOMETRIES ||
		operatorID == OP_OGIS_NUMPOINTS ||
		operatorID == OP_OGIS_NUMINTERIORRINGS ||
		operatorID == OP_OGIS_ISEMPTY ||
		operatorID == OP_OGIS_ISSIMPLE ||
		operatorID == OP_OGIS_ISCLOSED ||
		operatorID == OP_OGIS_ISRING ||
		operatorID == OP_OGIS_CONTAINS ||
		operatorID == OP_OGIS_CROSSES ||
		operatorID == OP_OGIS_DISJOINT ||
		operatorID == OP_OGIS_EQUALS ||
		operatorID == OP_OGIS_INTERSECTS ||
		operatorID == OP_OGIS_OVERLAPS ||
		operatorID == OP_OGIS_RELATED ||
		operatorID == OP_OGIS_TOUCHES ||
		operatorID == OP_OGIS_WITHIN ||
		operatorID == OP_OGIS_DIFFERENCE ||
		operatorID == OP_OGIS_INTERSECTION ||
		operatorID == OP_OGIS_SYMDIFFERENCE ||
		operatorID == OP_OGIS_UNION ||
		operatorID == OP_OGIS_DISTANCE ||
		operatorID == OP_OGIS_ENVELOPE ||
		operatorID == OP_OGIS_BUFFER ||
		operatorID == OP_OGIS_CONVEXHULL ||
		operatorID == OP_OGIS_EXTERIORRING ||
		operatorID == OP_OGIS_INTERIORRINGN ||
		operatorID == OP_OGIS_CENTRIOD ||
		operatorID == OP_OGIS_STARTPOINT ||
		operatorID == OP_OGIS_ENDPOINT ||
		operatorID == OP_OGIS_POINTONSURFACE ||
		operatorID == OP_OGIS_POINTN ||
		operatorID == OP_OGIS_GEOMETRYN)
		return SM_TRUE;
	else
		return SM_FALSE;
}

Four OQL_GDStoCommonAP::IsRectanglePolygon
(
	AP_OperatorStruct oper
)
{
    AP_ExprPoolElements         operand1(m_pool->ap_exprPool);
	AP_ValuePoolElements			value(m_pool->valuePool);
	StringPoolElements			str(m_pool->stringPool);	
	if(!(oper.operatorId == OP_OGIS_POLYGONFT || oper.operatorId == OP_OGIS_GEOMETRYFT))
	{
		return SM_FALSE;
	}
	
	operand1 = oper.operand1;

	if(operand1[0].exprKind != EXPR_KIND_VALUE)
	{
		return SM_FALSE;
	}

	value = operand1[0].value;
	str = value[0].string;

	int i, valueidx;
	float values[10];
	char* st = new char[str.size+1];
	for(i = 0; i < str.size; i++)
		st[i] = m_pool->stringPool[i + str.startIndex];
	st[i] = '\0';
	char* s = st;
	char* new_s;
	valueidx = 0;

	while(!(s[0] == '\0' || valueidx == 10)) 
	{
		if(s[0] == ' ' || s[0] == '\t' || s[0] == '\r' || s[0] == '\n' || s[0] == ',' || s[0] == '(' || s[0] == ')' || ('A' <= s[0] && s[0] <= 'Z') || ('a' <= s[0] && s[0] <= 'z'))
		{
			s = &s[1];
		}

		if(('0' <= s[0] && s[0] <= '9') || s[0] == '+' || s[0] == '-' || s[0] == '.')
		{
			values[valueidx] = strtod(s, &new_s);
			valueidx ++;
			s = new_s;
		}
	}
	if(valueidx != 10) return SM_FALSE;

	if(!(values[0] == values[8] && values[1] == values[9])) return SM_FALSE;

	if(values[2] != values[0] && values[3] == values[1])
	// right or left
	{
		//up or down,then left or right, down or up
		if(values[4] == values[2] && values[5] != values[3] && values[6] != values[4] && values[7] == values[5] && values[8] == values[6] && values[9] != values[7])
			return SM_TRUE;
		else
			return SM_FALSE;
	}
	else
	// up or down
	{
		//left or right,then down or up, right or left
		if(values[4] != values[2] && values[5] == values[3] && values[6] == values[4] && values[7] != values[5] && values[8] != values[6] && values[9] == values[7])
			return SM_TRUE;
		else
			return SM_FALSE;
	}
}	

Four OQL_GDStoCommonAP::makePlan_determineScanMethod_MlgfMbrScan(Four currentPlanNo, Four classId, AP_CondListPoolElements conds, Four condNumber,
                                                              AP_IndexInfoPoolIndex& indexInfoPoolIndex, AP_CondListPoolIndex& resultConds)
{
    CommonAP_PoolElements       apNode_sub(m_pool->commonAP_Pool);
    AP_ExprPoolElements         expr(m_pool->ap_exprPool);
    AP_ExprPoolElements         operand1(m_pool->ap_exprPool), operand2(m_pool->ap_exprPool);
    AP_IndexInfoPoolElements    indexInfo(m_pool->indexInfoPool);
	AP_CondListPoolElements     resultCondsElements(m_pool->ap_condListPool); 
	AP_ValuePoolElements        value(m_pool->valuePool);
    Boolean                     operandSwapped;
    Four                        e;

    // check if there is a restriction predicate, if it exists and it has an index then use INDEXSCAN
    expr = conds[condNumber].expr;

    if(expr[0].oper.operatorId == OP_OGIS_CONTAINS   || expr[0].oper.operatorId == OP_OGIS_CROSSES  ||
       expr[0].oper.operatorId == OP_OGIS_DISJOINT   || expr[0].oper.operatorId == OP_OGIS_EQUALS   || 
       expr[0].oper.operatorId == OP_OGIS_INTERSECTS || expr[0].oper.operatorId == OP_OGIS_OVERLAPS ||
       expr[0].oper.operatorId == OP_OGIS_RELATED    || expr[0].oper.operatorId == OP_OGIS_TOUCHES  || 
       expr[0].oper.operatorId == OP_OGIS_WITHIN
	   || expr[0].oper.operatorId == OP_OGIS_KNN)
    {
        operand1 = expr[0].oper.operand1;
        operand2 = expr[0].oper.operand2;
        operandSwapped = SM_FALSE;

        if(operand1[0].exprKind != EXPR_KIND_PATHEXPR)
        {   // swap operand1 and operand2
            operand1 = expr[0].oper.operand2;
			operand2 = expr[0].oper.operand1;

            operandSwapped = SM_TRUE;
        }

		// check if there is an index
		if(operand1[0].pathExpr.kind == PATHEXPR_KIND_ATTR)
		{
			CataClassInfo				classInfo;
			CataAttrInfo				attrInfo;

			e = m_catalog->class_ClassId_to_ClassInfo(classId, classInfo);
			OOSQL_CHECK_ERR(e);

			e = m_catalog->attr_ColNo_to_AttrInfo(classInfo, operand1[0].pathExpr.col.colNo, attrInfo);
			OOSQL_CHECK_ERR(e);

			e = m_catalog->index_isMlgfIndexExist(classInfo, attrInfo);
			OOSQL_CHECK_ERR(e);
			if(e == SM_TRUE)
			{
				OOSQL_StorageManager::IndexID	indexId_array[1];
				Four							nIndexes = 1;

				e = m_catalog->index_getMlgfIndexes(classInfo, attrInfo, nIndexes, indexId_array);
				OOSQL_CHECK_ERR(e);

				// make indexInfo
				indexInfo                       = m_pool->indexInfoPool.addNewEntry();
				indexInfoPoolIndex              = indexInfo.getPoolIndex();
				indexInfo[0].nodeKind           = INDEXINFO_SCAN;
				indexInfo[0].scan.indexType     = INDEXTYPE_MLGF;
				indexInfo[0].scan.indexId       = indexId_array[0];
				indexInfo[0].scan.boolExprs.setNull();
				indexInfo[0].scan.nCols         = 0;

				OperatorID spatialOp;
				if(expr[0].oper.operatorId == OP_OGIS_CONTAINS)
					spatialOp = OP_GEO_CONTAIN;
				else if(expr[0].oper.operatorId == OP_OGIS_CROSSES)
					spatialOp = OP_GEO_OVERLAP;
				else if(expr[0].oper.operatorId == OP_OGIS_DISJOINT)
					spatialOp = OP_GEO_DISJOINT;
				else if(expr[0].oper.operatorId == OP_OGIS_EQUALS)
					spatialOp = OP_GEO_EQUAL;
				else if(expr[0].oper.operatorId == OP_OGIS_INTERSECTS)
					spatialOp = OP_GEO_OVERLAP;
				else if(expr[0].oper.operatorId == OP_OGIS_OVERLAPS)
					spatialOp = OP_GEO_OVERLAP;
				else if(expr[0].oper.operatorId == OP_OGIS_RELATED)
				{
					OOSQL_ERR(eNOTIMPLEMENTED_OOSQL);
				}
				else if(expr[0].oper.operatorId == OP_OGIS_TOUCHES)
					spatialOp = OP_GEO_MEET;
				else if(expr[0].oper.operatorId == OP_OGIS_WITHIN)
					spatialOp = OP_GEO_CONTAINED;
				else if(expr[0].oper.operatorId == OP_OGIS_KNN)
					spatialOp = OP_OGIS_KNN;
				else
					OOSQL_ERR(eUNHANDLED_CASE_OOSQL);

				// determine start bound and stop bound
				if(operandSwapped)
				{
					// reverse operator
					indexInfo[0].scan.mlgfmbr.spatialOp = reverseComparisionOperatorID(spatialOp);
				}
				else
					indexInfo[0].scan.mlgfmbr.spatialOp = spatialOp;

				if(operand2[0].exprKind == EXPR_KIND_PATHEXPR)
				{
					indexInfo[0].scan.mlgfmbr.operandType = AP_SPATIAL_OPERAND_TYPE_PATHEXPR;
					indexInfo[0].scan.mlgfmbr.pathExpr    = operand2[0].pathExpr;
				}
				else if(operand2[0].exprKind == EXPR_KIND_OPER)
				{
					indexInfo[0].scan.mlgfmbr.operandType = AP_SPATIAL_OPERAND_TYPE_OPER;
					indexInfo[0].scan.mlgfmbr.oper        = operand2[0].oper;
				}
				else
					OOSQL_ERR(eUNHANDLED_CASE_OOSQL);

				CataIndexInfo cataIndexInfo;

				e = m_catalog->index_IndexId_to_IndexInfo(classInfo, indexId_array[0], cataIndexInfo);
				OOSQL_CHECK_ERR(e);

				e = m_catalog->index_GetIndexDesc(classInfo, cataIndexInfo, indexInfo[0].scan.mlgfKeyDesc);
				OOSQL_CHECK_ERR(e);

				// reconstruct condition list
				resultConds.setNull();
				// also kNN operator does.
				if((expr[0].oper.operatorId == OP_OGIS_CONTAINS && IsRectanglePolygon(operand2[0].oper))
					|| expr[0].oper.operatorId == OP_OGIS_KNN )
				{
					for(int i = 0; i < conds.size; i++)
					{

						if(i == condNumber) continue;
						resultCondsElements = m_pool->ap_condListPool.addNewEntry();
						
						if(resultConds.isNull())
							resultConds = resultCondsElements.getPoolIndex();
						else
							resultConds.size++;

						resultCondsElements[0] = conds[i];
					}
				}
				else
				// use original condition list as refinment
					resultConds = conds.getPoolIndex();
				// scan method is determined
				return SM_TRUE;
			}
		}
	}
	
	else
	{

    // check operator, only spatial operator can be applied
    if(!(expr[0].oper.operatorId == OP_GEO_NORTH    || expr[0].oper.operatorId == OP_GEO_SOUTH     || 
         expr[0].oper.operatorId == OP_GEO_EAST     || expr[0].oper.operatorId == OP_GEO_WEST      || 
         expr[0].oper.operatorId == OP_GEO_COVER    || expr[0].oper.operatorId == OP_GEO_COVERED   ||
         expr[0].oper.operatorId == OP_GEO_CONTAIN  || expr[0].oper.operatorId == OP_GEO_CONTAINED || 
         expr[0].oper.operatorId == OP_GEO_DISJOINT || expr[0].oper.operatorId == OP_GEO_EQUAL     || 
         expr[0].oper.operatorId == OP_GEO_MEET     || expr[0].oper.operatorId == OP_GEO_OVERLAP))
        return SM_FALSE;

    // check each operand
    operand1 = expr[0].oper.operand1;
    operand2 = expr[0].oper.operand2;
    operandSwapped = SM_FALSE;

    if((operand1[0].exprKind == EXPR_KIND_PATHEXPR && operand2[0].exprKind == EXPR_KIND_VALUE) || 
       (operand2[0].exprKind == EXPR_KIND_PATHEXPR && operand1[0].exprKind == EXPR_KIND_VALUE))
    {   // check restriction predicate
        if(operand1[0].exprKind == EXPR_KIND_VALUE)
        {   // swap operand1 and operand2
            operand1 = expr[0].oper.operand2;
            operand2 = expr[0].oper.operand1;

            operandSwapped = SM_TRUE;
        }
            
        // check if there is an index
        if(operand1[0].pathExpr.kind == PATHEXPR_KIND_OID)
        {
            // reference ap that operand1.pathExpr is accessed
            apNode_sub.setPoolIndex(operand1[0].pathExpr.planNo, 1);

            if(apNode_sub[0].classInfo.classKind == CLASSKIND_PERSISTENT)
            {
                CataClassInfo classInfo;
                CataAttrInfo  attrInfo;

                e = m_catalog->class_ClassId_to_ClassInfo(classId, classInfo);
                OOSQL_CHECK_ERR(e);

                // mlgf index always exists at the first column of the object
				// MBR column number is 1 
                e = m_catalog->attr_ColNo_to_AttrInfo(classInfo, 1, attrInfo);
                OOSQL_CHECK_ERR(e);
                    
                e = m_catalog->index_isMlgfIndexExist(classInfo, attrInfo);
                OOSQL_CHECK_ERR(e);
                if(e == SM_TRUE)
                {
                    OOSQL_StorageManager::IndexID	indexId_array[1];
                    Four							nIndexes = 1;

                    e = m_catalog->index_getMlgfIndexes(classInfo, attrInfo, nIndexes, indexId_array);
                    OOSQL_CHECK_ERR(e);

                    // make indexInfo
                    indexInfo                       = m_pool->indexInfoPool.addNewEntry();
                    indexInfoPoolIndex              = indexInfo.getPoolIndex();
                    indexInfo[0].nodeKind           = INDEXINFO_SCAN;
                    indexInfo[0].scan.indexType     = INDEXTYPE_MLGF;
                    indexInfo[0].scan.indexId       = indexId_array[0];
					indexInfo[0].scan.boolExprs.setNull();
					indexInfo[0].scan.nCols         = 0;
					
                    // determine start bound and stop bound
                    if(operandSwapped)
                    {   
                        // reverse operator
                        indexInfo[0].scan.mlgfmbr.spatialOp = reverseComparisionOperatorID(expr[0].oper.operatorId);
                    }
                    else
                        indexInfo[0].scan.mlgfmbr.spatialOp = expr[0].oper.operatorId;

					value = operand2[0].value;
                    indexInfo[0].scan.mlgfmbr.operandType = AP_SPATIAL_OPERAND_TYPE_MBR;
                    indexInfo[0].scan.mlgfmbr.mbr         = value[0].mbr;

					CataIndexInfo cataIndexInfo;

					e = m_catalog->index_IndexId_to_IndexInfo(classInfo, indexId_array[0], cataIndexInfo);
					OOSQL_CHECK_ERR(e);

					e = m_catalog->index_GetIndexDesc(classInfo, cataIndexInfo, indexInfo[0].scan.mlgfKeyDesc);
					OOSQL_CHECK_ERR(e);
					
                    // reconstruct condition list
					// use original condition list as refinment
					resultConds = conds.getPoolIndex();
					
                    // scan method is determined
                    return SM_TRUE;
                }
            }
        }
    }
    }
    return SM_FALSE;
}

Four OQL_GDStoCommonAP::makePlan_determineScanMethod_MlgfMbrJoin(Four currentPlanNo, Four classId, AP_CondListPoolElements conds, Four condNumber,
                                                              AP_IndexInfoPoolIndex& indexInfoPoolIndex, AP_CondListPoolIndex& resultConds)
{
    CommonAP_PoolElements       apNode_sub(m_pool->commonAP_Pool);
    AP_ExprPoolElements         expr(m_pool->ap_exprPool);
    AP_ExprPoolElements         operand1(m_pool->ap_exprPool), operand2(m_pool->ap_exprPool);
    AP_IndexInfoPoolElements    indexInfo(m_pool->indexInfoPool);
    Boolean                     operandSwapped;
    Four                        e;
    Boolean                     flag;

    // check if there is a restriction predicate, if it exists and it has an index then use INDEXSCAN
    expr   = conds[condNumber].expr;

    if(expr[0].oper.operatorId == OP_OGIS_CONTAINS   || expr[0].oper.operatorId == OP_OGIS_CROSSES  ||
       expr[0].oper.operatorId == OP_OGIS_DISJOINT   || expr[0].oper.operatorId == OP_OGIS_EQUALS   ||
       expr[0].oper.operatorId == OP_OGIS_INTERSECTS || expr[0].oper.operatorId == OP_OGIS_OVERLAPS ||
       expr[0].oper.operatorId == OP_OGIS_RELATED    || expr[0].oper.operatorId == OP_OGIS_TOUCHES  ||
       expr[0].oper.operatorId == OP_OGIS_WITHIN)
	{
		// check each arguments
        operand1 = expr[0].oper.operand1;
        operand2 = expr[0].oper.operand2;

		operandSwapped = SM_FALSE;

		if(currentPlanNo == operand2[0].pathExpr.planNo)
		{   // swap operand1 and operand2
            operand1 = expr[0].oper.operand2;
            operand2 = expr[0].oper.operand1;

			operandSwapped = SM_TRUE;
		}

		CataClassInfo				classInfo;
		CataAttrInfo				attrInfo;

		e = m_catalog->class_ClassId_to_ClassInfo(classId, classInfo);
		OOSQL_CHECK_ERR(e);

		e = m_catalog->attr_ColNo_to_AttrInfo(classInfo, operand1[0].pathExpr.col.colNo, attrInfo);
		OOSQL_CHECK_ERR(e);

		e = m_catalog->index_isMlgfIndexExist(classInfo, attrInfo);
		OOSQL_CHECK_ERR(e);
		if(e == SM_TRUE)
		{
			OOSQL_StorageManager::IndexID	indexId_array[1];
			Four							nIndexes = 1;

			e = m_catalog->index_getMlgfIndexes(classInfo, attrInfo, nIndexes, indexId_array);
			OOSQL_CHECK_ERR(e);

			// make indexInfo
			indexInfo                       = m_pool->indexInfoPool.addNewEntry();
			indexInfoPoolIndex              = indexInfo.getPoolIndex();
			indexInfo[0].nodeKind           = INDEXINFO_SCAN;
			indexInfo[0].scan.indexType     = INDEXTYPE_MLGF;
			indexInfo[0].scan.indexId       = indexId_array[0];
			indexInfo[0].scan.boolExprs.setNull();
			indexInfo[0].scan.nCols         = 0;

            OperatorID spatialOp;
            if(expr[0].oper.operatorId == OP_OGIS_CONTAINS)
                spatialOp = OP_GEO_CONTAIN;
            else if(expr[0].oper.operatorId == OP_OGIS_CROSSES)
                spatialOp = OP_GEO_OVERLAP;
            else if(expr[0].oper.operatorId == OP_OGIS_DISJOINT)
                spatialOp = OP_GEO_DISJOINT;
            else if(expr[0].oper.operatorId == OP_OGIS_EQUALS)
                spatialOp = OP_GEO_EQUAL;
            else if(expr[0].oper.operatorId == OP_OGIS_INTERSECTS)
                spatialOp = OP_GEO_OVERLAP;
            else if(expr[0].oper.operatorId == OP_OGIS_OVERLAPS)
                spatialOp = OP_GEO_OVERLAP;
            else if(expr[0].oper.operatorId == OP_OGIS_RELATED)
            {
                OOSQL_ERR(eNOTIMPLEMENTED_OOSQL);
            }
            else if(expr[0].oper.operatorId == OP_OGIS_TOUCHES)
                spatialOp = OP_GEO_MEET;
            else if(expr[0].oper.operatorId == OP_OGIS_WITHIN)
                spatialOp = OP_GEO_CONTAINED;
            else
                OOSQL_ERR(eUNHANDLED_CASE_OOSQL);

			// determine start bound and stop bound
			if(operandSwapped)
			{
				// reverse operator
				indexInfo[0].scan.mlgfmbr.spatialOp = reverseComparisionOperatorID(spatialOp);
			}
			else
				indexInfo[0].scan.mlgfmbr.spatialOp = spatialOp;

			if(operand2[0].exprKind == EXPR_KIND_PATHEXPR)
			{
				indexInfo[0].scan.mlgfmbr.operandType = AP_SPATIAL_OPERAND_TYPE_PATHEXPR;
				indexInfo[0].scan.mlgfmbr.pathExpr    = operand2[0].pathExpr;
			}
			else if(operand2[0].exprKind == EXPR_KIND_OPER)
			{
				indexInfo[0].scan.mlgfmbr.operandType = AP_SPATIAL_OPERAND_TYPE_OPER;
				indexInfo[0].scan.mlgfmbr.oper        = operand2[0].oper;
			}
			else
				OOSQL_ERR(eUNHANDLED_CASE_OOSQL);

			CataIndexInfo cataIndexInfo;

			e = m_catalog->index_IndexId_to_IndexInfo(classInfo, indexId_array[0], cataIndexInfo);
			OOSQL_CHECK_ERR(e);

			e = m_catalog->index_GetIndexDesc(classInfo, cataIndexInfo, indexInfo[0].scan.mlgfKeyDesc);
			OOSQL_CHECK_ERR(e);

			// reconstruct condition list
			// use original condition list as refinment
			resultConds = conds.getPoolIndex();

			// scan method is determined
			return SM_TRUE;
		}
	}
	
	else {

    // check operator, only spatial operator can be applied
    if(!(expr[0].oper.operatorId == OP_GEO_NORTH    || expr[0].oper.operatorId == OP_GEO_SOUTH     || 
         expr[0].oper.operatorId == OP_GEO_EAST     || expr[0].oper.operatorId == OP_GEO_WEST      || 
         expr[0].oper.operatorId == OP_GEO_COVER    || expr[0].oper.operatorId == OP_GEO_COVERED   ||
         expr[0].oper.operatorId == OP_GEO_CONTAIN  || expr[0].oper.operatorId == OP_GEO_CONTAINED || 
         expr[0].oper.operatorId == OP_GEO_DISJOINT || expr[0].oper.operatorId == OP_GEO_EQUAL     || 
         expr[0].oper.operatorId == OP_GEO_MEET     || expr[0].oper.operatorId == OP_GEO_OVERLAP))
        return SM_FALSE;

    // check each operand
    operand1 = expr[0].oper.operand1;
    operand2 = expr[0].oper.operand2;
    operandSwapped = SM_FALSE;

    if(operand1[0].exprKind == EXPR_KIND_PATHEXPR && operand2[0].exprKind == EXPR_KIND_PATHEXPR)
    {   // check join predicate
        // get last binded pathexpr (by checking if pathexpr's class is the same of apnode

        flag = SM_FALSE;
        if(operand1[0].pathExpr.kind == PATHEXPR_KIND_OID)
        {
            if(currentPlanNo == operand1[0].pathExpr.planNo)
                flag = SM_TRUE;
        }
        
        if(operand2[0].pathExpr.kind == PATHEXPR_KIND_OID && !flag)
        {
            if(currentPlanNo == operand2[0].pathExpr.planNo)
            {
                flag = SM_TRUE;

                // swap operand1 and operand2
                operand1 = expr[0].oper.operand2;
                operand2 = expr[0].oper.operand1;

                operandSwapped = SM_TRUE;
            }
        }

        if(flag)
        {
            // check if there is an index
            if(operand1[0].pathExpr.kind == PATHEXPR_KIND_OID)
            {
                // reference ap that operand1.pathExpr is accessed
                apNode_sub.setPoolIndex(operand1[0].pathExpr.planNo, 1);

                if(apNode_sub[0].classInfo.classKind == CLASSKIND_PERSISTENT)
                {
                    CataClassInfo classInfo;
                    CataAttrInfo  attrInfo;

                    e = m_catalog->class_ClassId_to_ClassInfo(classId, classInfo);
                    OOSQL_CHECK_ERR(e);

            		// MBR column number is 1 
					// mlgf index always exists at the first column of the object
                    e = m_catalog->attr_ColNo_to_AttrInfo(classInfo, 1, attrInfo);
                    OOSQL_CHECK_ERR(e);

                    e = m_catalog->index_isMlgfIndexExist(classInfo, attrInfo);
                    OOSQL_CHECK_ERR(e);

                    if(e == SM_TRUE)
                    {
                        OOSQL_StorageManager::IndexID indexId_array[1];
                        Four        nIndexes = 1;

                        e = m_catalog->index_getMlgfIndexes(classInfo, attrInfo, nIndexes, indexId_array);
                        OOSQL_CHECK_ERR(e);

                        // make indexInfo
                        indexInfo                   = m_pool->indexInfoPool.addNewEntry();
                        indexInfoPoolIndex          = indexInfo.getPoolIndex();
                        indexInfo[0].nodeKind       = INDEXINFO_SCAN;
                        indexInfo[0].scan.indexType = INDEXTYPE_MLGF_MBR;	
                        indexInfo[0].scan.indexId   = indexId_array[0];
                        indexInfo[0].scan.boolExprs.setNull();
						indexInfo[0].scan.nCols     = 0;

                        // determine start bound and stop bound
						if(operandSwapped)
						{   
							// reverse operator
							indexInfo[0].scan.mlgfmbr.spatialOp = reverseComparisionOperatorID(expr[0].oper.operatorId);
						}
						else
							indexInfo[0].scan.mlgfmbr.spatialOp = expr[0].oper.operatorId;

						indexInfo[0].scan.mlgfmbr.operandType = AP_SPATIAL_OPERAND_TYPE_PATHEXPR;
						indexInfo[0].scan.mlgfmbr.pathExpr    = operand2[0].pathExpr;

						CataIndexInfo cataIndexInfo;

						e = m_catalog->index_IndexId_to_IndexInfo(classInfo, indexId_array[0], cataIndexInfo);
						OOSQL_CHECK_ERR(e);

						e = m_catalog->index_GetIndexDesc(classInfo, cataIndexInfo, indexInfo[0].scan.mlgfKeyDesc);
						OOSQL_CHECK_ERR(e);

						// reconstruct condition list
						// use original condition list as refinment
						resultConds = conds.getPoolIndex();

                        // scan method is determined
                        return SM_TRUE;
                    }
                }
            }
        }
    }
    }
    return SM_FALSE;
}

Four OQL_GDStoCommonAP::makePlan_determineScanMethod_TextScan_MakeIndexNode(Four currentPlanNo, Four classId, Four operand1_indexType, Four operand2_indexType, 
																			AP_ExprPoolElements& expr, AP_IndexInfoPoolIndex& indexInfoPoolIndex, 
																			Boolean& isCurrentCondsInBoundConds)
{
	AP_ExprPoolElements             operand1(m_pool->ap_exprPool), operand2(m_pool->ap_exprPool);
    Boolean                         operandSwapped;
    MatchFuncInfo                   matchFuncInfo;
    AP_ArgumentPoolElements         ap_argument(m_pool->ap_argumentPool);
    ArgumentPoolElements            argument(m_pool->argumentPool);
    FunctionPoolElements            func(m_pool->funcPool);
    ValuePoolElements               value(m_pool->valuePool);
	AP_TextIndexCondPoolElements    textIndexCond(m_pool->textIndexCondPool);
	AP_IndexInfoPoolElements		indexInfo(m_pool->indexInfoPool);
	Four                            i, j;
	Four                            e;

	if((operand1_indexType == INDEXTYPE_TEXT && operand2_indexType == INDEXTYPE_NONE) ||
	   (operand1_indexType == INDEXTYPE_NONE && operand2_indexType == INDEXTYPE_TEXT))
	{
		CataClassInfo classInfo;
		CataAttrInfo  attrInfo;

		// check each operand
		if(operand1_indexType == INDEXTYPE_TEXT)
		{
			operand1 = expr[0].oper.operand1;
			operand2 = expr[0].oper.operand2;

			operandSwapped = SM_FALSE;
		}
		else
		{
			operand2 = expr[0].oper.operand1;
			operand1 = expr[0].oper.operand2;

			operandSwapped = SM_TRUE;
		}
 
		indexInfo = indexInfoPoolIndex = m_pool->indexInfoPool.addNewEntry();	

		indexInfo[0].nodeKind       = INDEXINFO_SCAN;
		indexInfo[0].scan.indexType = INDEXTYPE_TEXT;
		indexInfo[0].scan.nCols     = 0;

		ap_argument = operand1[0].funcEval.argument;
    
		e = m_catalog->class_ClassId_to_ClassInfo(classId, classInfo);
		OOSQL_CHECK_ERR(e);
		e = m_catalog->attr_ColNo_to_AttrInfo(classInfo, ap_argument[0].pathExpr.col.colNo, attrInfo);
		OOSQL_CHECK_ERR(e);

		e = m_catalog->index_isTextIndexExist(classInfo, attrInfo);
		OOSQL_CHECK_ERR(e);

		if(e == SM_TRUE)   // index exist
		{
			OOSQL_StorageManager::IndexID   indexId_array[1];
			Four          nIndexes = 1;

			e = m_catalog->index_getTextIndexes(classInfo, attrInfo, nIndexes, indexId_array);
			OOSQL_CHECK_ERR(e);
			indexInfo[0].scan.indexId   = indexId_array[0];
			indexInfo[0].scan.boolExprs.setNull();
			
			e = m_catalog->class_ClassInfo_to_ClassId(classInfo, indexInfo[0].scan.classId);
			OOSQL_CHECK_ERR(e);

			e = m_catalog->attr_AttrInfo_to_ColNo(classInfo, attrInfo, indexInfo[0].scan.colNo);
			OOSQL_CHECK_ERR(e);

			// construct sub plan
			// do not evaluate subplan, use previously evaluated subplan
			indexInfo[0].scan.text = ap_argument[1].textIndexSubPlan;   // second argument has the subplan

			// add comparison operator in sub plan
			textIndexCond = m_pool->textIndexCondPool.addNewEntry(2);

			textIndexCond[0].nodeKind = TEXTINDEXCOND_OPERATOR;
			if(operandSwapped)
				textIndexCond[0].oper.operatorID = reverseComparisionOperatorID(expr[0].oper.operatorId);
			else
				textIndexCond[0].oper.operatorID = expr[0].oper.operatorId;

			textIndexCond[0].oper.op1        = indexInfo[0].scan.text.textIndexCond;
			textIndexCond[0].oper.op2.setPoolIndex(textIndexCond.startIndex + 1, 1);
			textIndexCond[0].oper.op3.setNull();

			if(operand2[0].exprKind != EXPR_KIND_VALUE)
				OOSQL_ERR(eINTERNALERROR_OOSQL);

			textIndexCond[1].nodeKind = TEXTINDEXCOND_CONSTANT;
			textIndexCond[1].constant = operand2[0].value;

			indexInfo[0].scan.text.textIndexCond.setPoolIndex(textIndexCond.startIndex, 1);

			OOSQL_StorageManager::InvertedIndexDesc	invertedIndexKeyDesc;
			AP_ColNoMapPoolElements					colNoMap;
			AP_UsedColPoolElements					usedColInfo;
			Four									i;
			CataIndexInfo							cataIndexInfo;

			colNoMap    = m_pool->commonAP_Pool[currentPlanNo].colNoMap.getElements(m_pool->colNoMapPool);
			usedColInfo = m_pool->commonAP_Pool[currentPlanNo].usedColInfo.getElements(m_pool->usedColPool);

			e = m_catalog->index_IndexId_to_IndexInfo(classInfo, indexId_array[0], cataIndexInfo);
			OOSQL_CHECK_ERR(e);

			e = m_catalog->index_GetIndexDesc(classInfo, cataIndexInfo, invertedIndexKeyDesc);
			OOSQL_CHECK_ERR(e);
			memcpy(&indexInfo[0].scan.invertedIndexKeyDesc, &invertedIndexKeyDesc, sizeof(invertedIndexKeyDesc));

			indexInfo[0].scan.nCols = 0;
			for(i = 0; i < colNoMap.size; i++)	
			{
				if(colNoMap[i].offset != NOT_USED_COLUMN)	
				{
					for(j = 0; j < invertedIndexKeyDesc.postingInfo.nEmbeddedAttributes; j++)
					{
						if(invertedIndexKeyDesc.postingInfo.embeddedAttrNo[j] == usedColInfo[colNoMap[i].offset].colNo)
							break;
                        else if(usedColInfo[colNoMap[i].offset].colNo == 0) 
					        break;                                         
                    }
					if(j == invertedIndexKeyDesc.postingInfo.nEmbeddedAttributes && 
						usedColInfo[colNoMap[i].offset].colNo != 0)
						break;					
				}
			}

			if(i > 0 && i == colNoMap.size)		
			{
				indexInfo[0].scan.readObjectValueFromIndexFlag = SM_TRUE;
				for(i = 0; i < invertedIndexKeyDesc.postingInfo.nEmbeddedAttributes; i++)
				{
					indexInfo[0].scan.columns[i].colNo  = invertedIndexKeyDesc.postingInfo.embeddedAttrNo[i];
					indexInfo[0].scan.columns[i].type   = usedColInfo[colNoMap[invertedIndexKeyDesc.postingInfo.embeddedAttrNo[i]].offset].typeId;
					indexInfo[0].scan.columns[i].length = usedColInfo[colNoMap[invertedIndexKeyDesc.postingInfo.embeddedAttrNo[i]].offset].length;
				}
				indexInfo[0].scan.nCols = invertedIndexKeyDesc.postingInfo.nEmbeddedAttributes;
			}
			else									
			{
				indexInfo[0].scan.readObjectValueFromIndexFlag = SM_FALSE;
				indexInfo[0].scan.nCols                        = 0;
			}

			isCurrentCondsInBoundConds = SM_TRUE;
		}
	}
	return eNOERROR;
}

Four OQL_GDStoCommonAP::makePlan_determineScanMethod_TextScan_RecursiveMakeIndexNode(Four currentPlanNo, Four classId, 
				                                                                     AP_ExprPoolElements& expr, AP_IndexInfoPoolIndex& indexInfoPoolIndex)
{
	Four						operand1_indexType;
    Four						operand2_indexType;
	Boolean						isCurrentCondsInBoundConds;
	AP_ExprPoolElements			operand1(m_pool->ap_exprPool);
	AP_ExprPoolElements			operand2(m_pool->ap_exprPool);
	AP_IndexInfoPoolIndex		indexOperand1; 
	AP_IndexInfoPoolIndex		indexOperand2;
	AP_IndexInfoPoolElements	indexInfo(m_pool->indexInfoPool);
	Four						e;

	switch(expr[0].exprKind)
	{
	case EXPR_KIND_OPER:
		operand1 = expr[0].oper.operand1;
		operand2 = expr[0].oper.operand2;

		if(expr[0].oper.operatorId == OP_EQ || expr[0].oper.operatorId == OP_NE ||
           expr[0].oper.operatorId == OP_GT || expr[0].oper.operatorId == OP_LT ||
           expr[0].oper.operatorId == OP_GE || expr[0].oper.operatorId == OP_LE)
		{
			isCurrentCondsInBoundConds = SM_FALSE;

			if(operand1[0].exprKind == EXPR_KIND_FUNCEVAL && operand1[0].funcEval.functionID == FUNCTION_TEXTIR_MATCH)
			{
				operand1_indexType = INDEXTYPE_TEXT;
	            operand2_indexType = INDEXTYPE_NONE;
			}
			else if(operand2[0].exprKind == EXPR_KIND_FUNCEVAL && operand2[0].funcEval.functionID == FUNCTION_TEXTIR_MATCH)
			{
				operand2_indexType = INDEXTYPE_TEXT;
	            operand1_indexType = INDEXTYPE_NONE;
			}
			else
			{
				operand1_indexType = INDEXTYPE_NONE;
				operand2_indexType = INDEXTYPE_NONE;
			}

			if((operand1_indexType == INDEXTYPE_TEXT && operand2_indexType == INDEXTYPE_NONE) ||
               (operand1_indexType == INDEXTYPE_NONE && operand2_indexType == INDEXTYPE_TEXT))
			{
				e = makePlan_determineScanMethod_TextScan_MakeIndexNode(currentPlanNo, classId, 
																		operand1_indexType,
																		operand2_indexType,
																		expr, indexInfoPoolIndex, 
																		isCurrentCondsInBoundConds);
				OOSQL_CHECK_ERR(e);
			}
			else
				OOSQL_ERR(eUNHANDLED_CASE_OOSQL);		
											
			if(!isCurrentCondsInBoundConds)
				OOSQL_ERR(eUNHANDLED_CASE_OOSQL);		
		}
		else if(expr[0].oper.operatorId == OP_OR)
		{
			e = makePlan_determineScanMethod_TextScan_RecursiveMakeIndexNode(currentPlanNo, classId, operand1, 
				  														     indexOperand1);
			OOSQL_CHECK_ERR(e);

			e = makePlan_determineScanMethod_TextScan_RecursiveMakeIndexNode(currentPlanNo, classId, operand2, 
																		     indexOperand2);
			OOSQL_CHECK_ERR(e);

			indexInfo = indexInfoPoolIndex = m_pool->indexInfoPool.addNewEntry();
			indexInfo[0].nodeKind        = INDEXINFO_OPERATOR;
            indexInfo[0].oper.operatorID = OP_INDEX_OR;
			indexInfo[0].oper.op1 = indexOperand1.getPoolIndex();
            indexInfo[0].oper.op2 = indexOperand2.getPoolIndex();
		}
		else if(expr[0].oper.operatorId == OP_AND)
		{
			e = makePlan_determineScanMethod_TextScan_RecursiveMakeIndexNode(currentPlanNo, classId, operand1, 
				  														     indexOperand1);
			OOSQL_CHECK_ERR(e);

			e = makePlan_determineScanMethod_TextScan_RecursiveMakeIndexNode(currentPlanNo, classId, operand2, 
																		     indexOperand2);
			OOSQL_CHECK_ERR(e);

			indexInfo = indexInfoPoolIndex = m_pool->indexInfoPool.addNewEntry();
			indexInfo[0].nodeKind        = INDEXINFO_OPERATOR;
            indexInfo[0].oper.operatorID = OP_INDEX_AND;
			indexInfo[0].oper.op1 = indexOperand1.getPoolIndex();
            indexInfo[0].oper.op2 = indexOperand2.getPoolIndex();
		}
		else
			OOSQL_ERR(eUNHANDLED_CASE_OOSQL);		

		break;

	case EXPR_KIND_PATHEXPR:
	case EXPR_KIND_VALUE:
	case EXPR_KIND_CONS:
	case EXPR_KIND_AGGRFUNCRESULT:
	case EXPR_KIND_FUNCRESULT:
	case EXPR_KIND_FUNCEVAL:
		break;

	default:
		OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
	}
	return eNOERROR;
}

Four OQL_GDStoCommonAP::makePlan_determineScanMethod_TextScan(Four currentPlanNo, Four classId, AP_CondListPoolElements conds, Four condNumber, 
                                                              AP_IndexInfoPoolIndex& indexInfoPoolIndex, AP_CondListPoolIndex& resultConds)
{
	CommonAP_PoolElements           apNode_sub(m_pool->commonAP_Pool);
    AP_ExprPoolElements             expr(m_pool->ap_exprPool);
    AP_IndexInfoPoolElements        indexInfo(m_pool->indexInfoPool);
	AP_IndexInfoPoolIndex			newIndexInfoPoolIndex;
    Four                            e;
    Four                            i;                      
    AP_CondListPoolElements         resultCondsElements(m_pool->ap_condListPool);
    Boolean                         isCurrentCondsInBoundConds;
	Four                            operand1_indexType;
    Four                            operand2_indexType;

	resultConds.setNull();
	indexInfoPoolIndex.setNull();
    for(i = 0; i < conds.size; i++)
    {
        expr = conds[i].expr;

        if((expr[0].oper.operatorId == OP_EQ || expr[0].oper.operatorId == OP_NE ||
            expr[0].oper.operatorId == OP_GT || expr[0].oper.operatorId == OP_LT ||
            expr[0].oper.operatorId == OP_GE || expr[0].oper.operatorId == OP_LE))
        {
			isCurrentCondsInBoundConds = SM_FALSE;

			if(conds[i].expr.getElements(m_pool->ap_exprPool, 0).exprKind == EXPR_KIND_OPER)
			{
				operand1_indexType = GetIndexTypeOfExpression(classId, conds[i].expr.getElements(m_pool->ap_exprPool, 0).oper.operand1);
				operand2_indexType = GetIndexTypeOfExpression(classId, conds[i].expr.getElements(m_pool->ap_exprPool, 0).oper.operand2);
			}
			else
			{
				operand1_indexType = INDEXTYPE_NONE;
				operand2_indexType = INDEXTYPE_NONE;
			}

			if((operand1_indexType == INDEXTYPE_TEXT && operand2_indexType == INDEXTYPE_NONE) ||
               (operand1_indexType == INDEXTYPE_NONE && operand2_indexType == INDEXTYPE_TEXT))
			{
				e = makePlan_determineScanMethod_TextScan_MakeIndexNode(currentPlanNo, classId, 
																		operand1_indexType,
																		operand2_indexType,
																		expr, newIndexInfoPoolIndex, 
																		isCurrentCondsInBoundConds);
				OOSQL_CHECK_ERR(e);

				if(indexInfoPoolIndex.isNull())
					indexInfoPoolIndex = newIndexInfoPoolIndex;
				else
				{
					indexInfo = m_pool->indexInfoPool.addNewEntry();
					indexInfo[0].nodeKind        = INDEXINFO_OPERATOR;
					indexInfo[0].oper.operatorID = OP_INDEX_AND;
					indexInfo[0].oper.op1 = indexInfoPoolIndex;
					indexInfo[0].oper.op2 = newIndexInfoPoolIndex;

					indexInfoPoolIndex = indexInfo.getPoolIndex();
				}
			}

			if(!isCurrentCondsInBoundConds)
			{
				resultCondsElements = m_pool->ap_condListPool.addNewEntry();

				if(resultConds == NULL_POOLINDEX)
					resultConds = resultCondsElements.getPoolIndex();
				else
					resultConds.size ++;

				resultCondsElements[0] = conds[i];
			}
        }
		else if(expr[0].oper.operatorId == OP_OR)
		{
			if(checkIfThereAreMatchFunctionsInAllExpression(expr))
			{
				e = makePlan_determineScanMethod_TextScan_RecursiveMakeIndexNode(currentPlanNo, classId, 
																				 expr, newIndexInfoPoolIndex);
				OOSQL_CHECK_ERR(e);

				if(indexInfoPoolIndex.isNull())
					indexInfoPoolIndex = newIndexInfoPoolIndex;
				else
				{
					indexInfo = m_pool->indexInfoPool.addNewEntry();
					indexInfo[0].nodeKind        = INDEXINFO_OPERATOR;
					indexInfo[0].oper.operatorID = OP_INDEX_AND;
					indexInfo[0].oper.op1 = indexInfoPoolIndex;
					indexInfo[0].oper.op2 = newIndexInfoPoolIndex;

					indexInfoPoolIndex = indexInfo.getPoolIndex();
				}
			}
			else
			{
				resultCondsElements = m_pool->ap_condListPool.addNewEntry();

				if(resultConds == NULL_POOLINDEX)
					resultConds = resultCondsElements.getPoolIndex();
				else
					resultConds.size ++;

				resultCondsElements[0] = conds[i];
			}
		}
		else
		{
			resultCondsElements = m_pool->ap_condListPool.addNewEntry();

			if(resultConds == NULL_POOLINDEX)
				resultConds = resultCondsElements.getPoolIndex();
			else
				resultConds.size ++;

			resultCondsElements[0] = conds[i];
		}
    }   

    return SM_TRUE;
}

/****************************************************************************
DESCRIPTION:

IMPLEMENTATION:
****************************************************************************/
Four OQL_GDStoCommonAP::makePlan_determineBoundConditionForB_TreeScan(Four planNo, Four nKeyCols, Two* colNos, 
                                                                      AP_CondListPoolElements conds,
                                                                      AP_BoundCondInfo& startBound, 
                                                                      AP_BoundCondInfo& stopBound,
                                                                      AP_CondListPoolIndex& resultConds,
                                                                      Boolean operandSwapped, 
																	  CataClassInfo classInfo,
																	  OOSQL_StorageManager::KeyDesc& btreeKeyDesc)
{
    AP_ExprPoolElements         startKey(m_pool->ap_exprPool);
    AP_ExprPoolElements         stopKey(m_pool->ap_exprPool);
    AP_ExprPoolElements         expr(m_pool->ap_exprPool);
    AP_ExprPoolElements         operand1(m_pool->ap_exprPool), operand2(m_pool->ap_exprPool);
    AP_CondListPoolElements     resultCondsElements(m_pool->ap_condListPool);
    Four                        nKeyParts;    
    Four                        i, j;
    Four                        e;
    Boolean                     isCurrentCondsInBoundConds;
    OperatorID                  operatorId;
	Four						colIndex;
	TypeID						attrTypeID;
	Four						attrLength;
	CataAttrInfo				attrInfo;
	Boolean						isNullKeyValueExist;
	AP_BoundCondInfo			startBoundArray[MAXNUMKEYPARTS];
	AP_BoundCondInfo			stopBoundArray[MAXNUMKEYPARTS];

    resultConds.setNull();
	
    // get nKeyParts
    nKeyParts = nKeyCols;

    startBound.op  = OOSQL_StorageManager::SM_BOF;
    startBound.key = m_pool->ap_exprPool.addNewEntry(nKeyParts);
    startKey       = startBound.key;
    for(i = 0; i < nKeyParts; i++)
    {
		startKey[i].exprKind        = EXPR_KIND_VALUE;
		startKey[i].resultType      = TYPEID_NONE;
		startKey[i].resultLength    = 0;
		startKey[i].value.setNull();
		startBoundArray[i].op = OOSQL_StorageManager::SM_BOF;
    }

    stopBound.op   = OOSQL_StorageManager::SM_EOF;
    stopBound.key  = m_pool->ap_exprPool.addNewEntry(nKeyParts);
    stopKey        = stopBound.key;
    for(i = 0; i < nKeyParts; i++)
    {
		stopKey[i].exprKind         = EXPR_KIND_VALUE;
		stopKey[i].resultType       = TYPEID_NONE;
		stopKey[i].resultLength     = 0;
		stopKey[i].value.setNull();
		stopBoundArray[i].op = OOSQL_StorageManager::SM_EOF;
    }

    for(i = 0; i < conds.size; i++)
    {
        isCurrentCondsInBoundConds = SM_FALSE;

        expr = conds[i].expr;
        
		// for B+-tree and MLGF tid intersection, KNN operator want to be lost in resultConds
		if(expr[0].oper.operatorId == OP_OGIS_KNN) isCurrentCondsInBoundConds = SM_TRUE;

        // check operator, only arithmetic operator can be applied
        if((expr[0].oper.operatorId == OP_EQ || expr[0].oper.operatorId == OP_NE ||
            expr[0].oper.operatorId == OP_GT || expr[0].oper.operatorId == OP_LT ||
            expr[0].oper.operatorId == OP_GE || expr[0].oper.operatorId == OP_LE ||
			expr[0].oper.operatorId == OP_LIKE || expr[0].oper.operatorId == OP_IN))
        {
            // check each operand
            operand1 = expr[0].oper.operand1;
            operand2 = expr[0].oper.operand2;

            if((operand1[0].exprKind == EXPR_KIND_PATHEXPR && operand2[0].exprKind == EXPR_KIND_VALUE) || 
               (operand2[0].exprKind == EXPR_KIND_PATHEXPR && operand1[0].exprKind == EXPR_KIND_VALUE))
            {
                if(operand1[0].exprKind == EXPR_KIND_VALUE)
                {   // swap operand1 and operand2
                    operand1 = expr[0].oper.operand2;
                    operand2 = expr[0].oper.operand1;
                }

				for(colIndex = -1, j = 0; j < nKeyParts; j++)
				{
					if(operand1[0].pathExpr.col.planNo == planNo && operand1[0].pathExpr.col.colNo == colNos[j])
					{
						colIndex = j;
						break;
					}
				}

                if(colIndex >= 0)	// find the matching column
                {
					e = m_catalog->attr_ColNo_to_AttrInfo(classInfo, colNos[colIndex], attrInfo);
					OOSQL_CHECK_ERR(e);

					e = m_catalog->attr_GetTypeID(classInfo, attrInfo, attrTypeID);
					OOSQL_CHECK_ERR(e);

					e = m_catalog->attr_GetLength(classInfo, attrInfo, attrLength);
					OOSQL_CHECK_ERR(e);

                    // found condition which used in index scan
                    if(operandSwapped)
                        operatorId = reverseComparisionOperatorID(expr[0].oper.operatorId);
                    else
                        operatorId = expr[0].oper.operatorId;

					e = makePlan_constructBoundConditionForExpression(expr, operand1, operand2, colIndex, attrTypeID, attrLength,
																	  startBoundArray[colIndex], stopBoundArray[colIndex], startKey, stopKey, isCurrentCondsInBoundConds, 
																	  SM_FALSE /* isJoinCondition */); 
					OOSQL_CHECK_ERR(e);

                }
            }   
        }

		if(isCurrentCondsInBoundConds)
		{
			if(attrTypeID == TYPEID_STRING)
			{
				e = makePlan_retouchStringBoundCondition(expr, colIndex, attrLength, startKey, stopKey);
				OOSQL_CHECK_ERR(e);
			}
			else if(attrTypeID == TYPEID_VARSTRING)
			{
				e = makePlan_retouchVarStringBoundCondition(expr, colIndex, attrLength, startKey, stopKey);
				OOSQL_CHECK_ERR(e);
			}
		}
	

		if(expr[0].oper.operatorId == OP_LIKE)
			isCurrentCondsInBoundConds = SM_FALSE;

		if(!isCurrentCondsInBoundConds || 
		   nKeyParts > 1)					
		{
			resultCondsElements = m_pool->ap_condListPool.addNewEntry();

			if(resultConds == NULL_POOLINDEX)
				resultConds = resultCondsElements.getPoolIndex();
			else
				resultConds.size ++;

			resultCondsElements[0] = conds[i];
		}
	}

	e = makePlan_constructUnboundConditionAsMinMaxValue(nKeyParts, btreeKeyDesc, startBoundArray, stopBoundArray, startKey, stopKey);
	OOSQL_CHECK_ERR(e);

	e = makePlan_detremineMultikeyBoundCondition(nKeyParts, startBoundArray, stopBoundArray, startBound, stopBound);
	OOSQL_CHECK_ERR(e);
	
    return eNOERROR;
}

Four OQL_GDStoCommonAP::makePlan_constructUnboundConditionAsMinMaxValue(Four nKeyParts, 
																		OOSQL_StorageManager::KeyDesc& btreeKeyDesc,
																		AP_BoundCondInfo*		startBoundArray, 
                                                                        AP_BoundCondInfo*		stopBoundArray,
																	    AP_ExprPoolElements&	startKey, 
																	    AP_ExprPoolElements&	stopKey)
{
	Four	i;
	Boolean	isNullKeyValueExist;
	enum {OPERATOR_LITTLE, OPERATOR_EQUAL, OPERATOR_GREAT} prevOperatorGroup;

	if(startBoundArray[0].op == OOSQL_StorageManager::SM_BOF ||
       startBoundArray[0].op == OOSQL_StorageManager::SM_GT  ||
	   startBoundArray[0].op == OOSQL_StorageManager::SM_GE)
		prevOperatorGroup = OPERATOR_LITTLE;
	else if(startBoundArray[0].op == OOSQL_StorageManager::SM_EOF ||
			startBoundArray[0].op == OOSQL_StorageManager::SM_LT  ||
			startBoundArray[0].op == OOSQL_StorageManager::SM_LE)
		prevOperatorGroup = OPERATOR_GREAT;
	else 
		prevOperatorGroup = OPERATOR_EQUAL;
		
	isNullKeyValueExist = SM_FALSE;
	for(i = 0; i < nKeyParts; i++)
	{
		if(startKey[i].value.isNull())
			isNullKeyValueExist = SM_TRUE;

		switch(prevOperatorGroup)
		{
		case OPERATOR_LITTLE:
			if(startBoundArray[i].op == OOSQL_StorageManager::SM_EOF ||
			   startBoundArray[i].op == OOSQL_StorageManager::SM_LT  ||
			   startBoundArray[i].op == OOSQL_StorageManager::SM_LE)
				isNullKeyValueExist = SM_TRUE;
			break;
		case OPERATOR_GREAT:
			if(startBoundArray[i].op == OOSQL_StorageManager::SM_BOF ||
			   startBoundArray[i].op == OOSQL_StorageManager::SM_GT  ||
			   startBoundArray[i].op == OOSQL_StorageManager::SM_GE)
				isNullKeyValueExist = SM_TRUE;
			break;
		case OPERATOR_EQUAL:
			if(startBoundArray[i].op == OOSQL_StorageManager::SM_BOF ||
			   startBoundArray[i].op == OOSQL_StorageManager::SM_GT  ||
			   startBoundArray[i].op == OOSQL_StorageManager::SM_GE)
				prevOperatorGroup = OPERATOR_LITTLE;
			else if(startBoundArray[i].op == OOSQL_StorageManager::SM_EOF ||
					startBoundArray[i].op == OOSQL_StorageManager::SM_LT  ||
					startBoundArray[i].op == OOSQL_StorageManager::SM_LE)
				prevOperatorGroup = OPERATOR_GREAT;
			else 
				prevOperatorGroup = OPERATOR_EQUAL;
			break;
		}

		if(isNullKeyValueExist || startBoundArray[i].op == OOSQL_StorageManager::SM_BOF)
		{
			startKey[i].exprKind        = EXPR_KIND_VALUE;
			startKey[i].resultType      = (TypeID)btreeKeyDesc.kpart[i].type;
			startKey[i].resultLength    = btreeKeyDesc.kpart[i].length;
			startKey[i].value           = constructMinValueOfTheType(startKey[i].resultType, startKey[i].resultLength);
			startBoundArray[i].op		= OOSQL_StorageManager::SM_GE;
		}
	}

	if(stopBoundArray[0].op == OOSQL_StorageManager::SM_BOF ||
       stopBoundArray[0].op == OOSQL_StorageManager::SM_GT  ||
	   stopBoundArray[0].op == OOSQL_StorageManager::SM_GE)
		prevOperatorGroup = OPERATOR_LITTLE;
	else if(stopBoundArray[0].op == OOSQL_StorageManager::SM_EOF ||
			stopBoundArray[0].op == OOSQL_StorageManager::SM_LT  ||
			stopBoundArray[0].op == OOSQL_StorageManager::SM_LE)
		prevOperatorGroup = OPERATOR_GREAT;
	else 
		prevOperatorGroup = OPERATOR_EQUAL;

	isNullKeyValueExist = SM_FALSE;
	for(i = 0; i < nKeyParts; i++)
	{
		if(stopKey[i].value.isNull())
			isNullKeyValueExist = SM_TRUE;

		switch(prevOperatorGroup)
		{
		case OPERATOR_LITTLE:
			if(stopBoundArray[i].op == OOSQL_StorageManager::SM_EOF ||
			   stopBoundArray[i].op == OOSQL_StorageManager::SM_LT  ||
			   stopBoundArray[i].op == OOSQL_StorageManager::SM_LE)
				isNullKeyValueExist = SM_TRUE;
			break;
		case OPERATOR_GREAT:
			if(stopBoundArray[i].op == OOSQL_StorageManager::SM_BOF ||
			   stopBoundArray[i].op == OOSQL_StorageManager::SM_GT  ||
			   stopBoundArray[i].op == OOSQL_StorageManager::SM_GE)
				isNullKeyValueExist = SM_TRUE;
			break;
		case OPERATOR_EQUAL:
			if(stopBoundArray[i].op == OOSQL_StorageManager::SM_BOF ||
			   stopBoundArray[i].op == OOSQL_StorageManager::SM_GT  ||
			   stopBoundArray[i].op == OOSQL_StorageManager::SM_GE)
				prevOperatorGroup = OPERATOR_LITTLE;
			else if(stopBoundArray[i].op == OOSQL_StorageManager::SM_EOF ||
					stopBoundArray[i].op == OOSQL_StorageManager::SM_LT  ||
					stopBoundArray[i].op == OOSQL_StorageManager::SM_LE)
				prevOperatorGroup = OPERATOR_GREAT;
			else 
				prevOperatorGroup = OPERATOR_EQUAL;
			break;
		}

		if(isNullKeyValueExist || stopBoundArray[i].op == OOSQL_StorageManager::SM_EOF)
		{
			stopKey[i].exprKind         = EXPR_KIND_VALUE;
			stopKey[i].resultType       = (TypeID)btreeKeyDesc.kpart[i].type;
			stopKey[i].resultLength     = btreeKeyDesc.kpart[i].length;
			stopKey[i].value            = constructMaxValueOfTheType(stopKey[i].resultType, stopKey[i].resultLength);
			stopBoundArray[i].op		= OOSQL_StorageManager::SM_LE;
		}
	}

	return eNOERROR;
}

Four OQL_GDStoCommonAP::makePlan_retouchStringBoundCondition(AP_ExprPoolElements&	expr, 
															 Four					colIndex,
															 Four					attrLength,
															 AP_ExprPoolElements&	startKey, 
															 AP_ExprPoolElements&	stopKey)
{
	if(startKey[colIndex].value != NULL_POOLINDEX)
	{
		StringPoolElements string(m_pool->stringPool);
		StringPoolElements srcstring(m_pool->stringPool);
		ValuePoolElements  value(m_pool->valuePool);
		ValuePoolElements  srcvalue(m_pool->valuePool);
		
		srcvalue  = startKey[colIndex].value;
		srcstring = srcvalue[0].string;

		value  = m_pool->valuePool.addNewEntry();
		string = m_pool->stringPool.addNewEntry(attrLength);
		value[0].valueKind = VALUE_KIND_STRING;
		value[0].string    = string.getPoolIndex();

		if(expr[0].oper.operatorId == OP_LIKE)
		{
			int i;	
			int skipFlag;

			skipFlag = SM_FALSE;
			for(i = 0; i < string.size; i++)
			{
				if(i < srcstring.size && !skipFlag)
				{
					if(srcstring[i] == '*' || srcstring[i] == '%' ||
					   srcstring[i] == '?' || srcstring[i] == '_')
					{
						skipFlag = SM_TRUE;
						string[i] = 0;
					}
					else
						string[i] = srcstring[i];
				}
				else
					string[i] = 0;
			}
		}
		else
		{
			for(int i = 0; i < string.size; i++)
			{
				if(i < srcstring.size)
					string[i] = srcstring[i];
				else
					string[i] = 0;
			}
		}

		startKey[colIndex].value = value.getPoolIndex();
		startKey[colIndex].resultLength = string.size;
	}
	if(stopKey[colIndex].value != NULL_POOLINDEX)
	{
		StringPoolElements string(m_pool->stringPool);
		StringPoolElements srcstring(m_pool->stringPool);
		ValuePoolElements  value(m_pool->valuePool);
		ValuePoolElements  srcvalue(m_pool->valuePool);
		
		srcvalue  = stopKey[colIndex].value;
		srcstring = srcvalue[0].string;

		value  = m_pool->valuePool.addNewEntry();
		string = m_pool->stringPool.addNewEntry(attrLength);
		value[0].valueKind = VALUE_KIND_STRING;
		value[0].string    = string.getPoolIndex();

		if(expr[0].oper.operatorId == OP_LIKE)
		{
			int i;	
			int skipFlag;

			skipFlag = SM_FALSE;
			for(i = 0; i < string.size; i++)
			{
				if(i < srcstring.size && !skipFlag)
				{
					if(srcstring[i] == '*' || srcstring[i] == '%' ||
					   srcstring[i] == '?' || srcstring[i] == '_')
					{
						skipFlag = SM_TRUE;
						string[i] = 0xff;
					}
					else
						string[i] = srcstring[i];
				}
				else
					string[i] = 0xff;
			}
		}
		else
		{
			for(int i = 0; i < string.size; i++)
			{
				if(i < srcstring.size)
					string[i] = srcstring[i];
				else
					string[i] = 0;
			}
		}

		stopKey[colIndex].value = value.getPoolIndex();
		stopKey[colIndex].resultLength = string.size;
	}
	return eNOERROR;
}

Four OQL_GDStoCommonAP::makePlan_retouchVarStringBoundCondition(AP_ExprPoolElements&	expr, 
																Four					colIndex,
																Four					attrLength,
																AP_ExprPoolElements&	startKey, 
																AP_ExprPoolElements&	stopKey)
{
	if(startKey[colIndex].value != NULL_POOLINDEX)
	{
		StringPoolElements string(m_pool->stringPool);
		StringPoolElements srcstring(m_pool->stringPool);
		ValuePoolElements  value(m_pool->valuePool);
		ValuePoolElements  srcvalue(m_pool->valuePool);

		srcvalue  = startKey[colIndex].value;
		srcstring = srcvalue[0].string;

		value  = m_pool->valuePool.addNewEntry();
		
		if(expr[0].oper.operatorId == OP_LIKE)
		{
			int i;	
			int prefixSize;

			for(prefixSize = 0, i = 0; i < srcstring.size; i++)
			{
				if(srcstring[i] == '*' || srcstring[i] == '%' ||
				   srcstring[i] == '?' || srcstring[i] == '_')
					break;
				else
					prefixSize ++;
			}

			string = m_pool->stringPool.addNewEntry(prefixSize);
			value[0].valueKind = VALUE_KIND_STRING;
			value[0].string    = string.getPoolIndex();

			for(i = 0; i < prefixSize; i++)
				string[i] = srcstring[i];
		}
		else
		{
			string = m_pool->stringPool.addNewEntry(srcstring.size);
			value[0].valueKind = VALUE_KIND_STRING;
			value[0].string    = string.getPoolIndex();

			for(int i = 0; i < srcstring.size; i++)
				string[i] = srcstring[i];
		}

		startKey[colIndex].value = value.getPoolIndex();
		startKey[colIndex].resultLength = string.size;
	}
	if(stopKey[colIndex].value != NULL_POOLINDEX)
	{
		StringPoolElements string(m_pool->stringPool);
		StringPoolElements srcstring(m_pool->stringPool);
		ValuePoolElements  value(m_pool->valuePool);
		ValuePoolElements  srcvalue(m_pool->valuePool);

		srcvalue  = stopKey[colIndex].value;
		srcstring = srcvalue[0].string;

		value  = m_pool->valuePool.addNewEntry();

		if(expr[0].oper.operatorId == OP_LIKE)
		{
			int i;	
			int prefixSize;

			for(prefixSize = 0, i = 0; i < srcstring.size; i++)
			{
				if(srcstring[i] == '*' || srcstring[i] == '%' ||
				   srcstring[i] == '?' || srcstring[i] == '_')
					break;
				else
					prefixSize ++;
			}

			string = m_pool->stringPool.addNewEntry(prefixSize + 1);
			value[0].valueKind = VALUE_KIND_STRING;
			value[0].string    = string.getPoolIndex();

			for(i = 0; i < prefixSize; i++)
				string[i] = srcstring[i];
			string[i] = 0xff;
		}
		else
		{
			string = m_pool->stringPool.addNewEntry(srcstring.size);
			value[0].valueKind = VALUE_KIND_STRING;
			value[0].string    = string.getPoolIndex();

			for(int i = 0; i < srcstring.size; i++)
				string[i] = srcstring[i];
		}

		stopKey[colIndex].value = value.getPoolIndex();
		stopKey[colIndex].resultLength = string.size;
	}
	
	return eNOERROR;
}

Four OQL_GDStoCommonAP::makePlan_constructBoundConditionForExpression(AP_ExprPoolElements&	expr, 
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
																	  Boolean				isJoinCondition)
{
	int		compareResult;
	Four	e;

    switch(expr[0].oper.operatorId)
    {
    case OP_EQ:     // adjust startBound and stopBound
	case OP_IN:
		if(isJoinCondition)
		{
            if(!(startBound.op == OOSQL_StorageManager::SM_BOF && stopBound.op == OOSQL_StorageManager::SM_EOF))
                break;  

            isCurrentCondsInBoundConds = SM_TRUE;

            startBound.op		= OOSQL_StorageManager::SM_EQ;
            startKey[colIndex]  = operand2[0];

            stopBound.op		= OOSQL_StorageManager::SM_EQ;
            stopKey[colIndex]   = operand2[0];
		}
		else
		{
			if(startKey[colIndex].value != NULL_POOLINDEX)
			{
				// if new key value is greater than old key value, update key value
				e = compareValueInExpr(startKey[colIndex].value, operand2[0].value, compareResult);
				OOSQL_CHECK_ERR(e);
			}
			else
				compareResult = 0;
			if(compareResult <= 0)
			{
				isCurrentCondsInBoundConds		= SM_TRUE;
				startBound.op					= OOSQL_StorageManager::SM_GE;
				startKey[colIndex].resultType   = attrTypeID;
				startKey[colIndex].resultLength = attrLength;
				startKey[colIndex].value        = operand2[0].value;
			}

			if(stopKey[colIndex].value != NULL_POOLINDEX)
			{
				// if new key value is smaller than old key value, update key value
				e = compareValueInExpr(stopKey[colIndex].value, operand2[0].value, compareResult);
				OOSQL_CHECK_ERR(e);
			}
			else
				compareResult = 0;
			if(compareResult >= 0)
			{
				isCurrentCondsInBoundConds		= SM_TRUE;
				stopBound.op					= OOSQL_StorageManager::SM_LE;
				stopKey[colIndex].resultType    = attrTypeID;
				stopKey[colIndex].resultLength  = attrLength;
				stopKey[colIndex].value         = operand2[0].value;
			}
		}
        break;

    case OP_NE:     // adjust startBound and stopBound
        break;

    case OP_GT:     // adjust startBound
        
		if(isJoinCondition)
		{
            if(!(startBound.op == OOSQL_StorageManager::SM_BOF))
                break;      

            isCurrentCondsInBoundConds = SM_TRUE;

            startBound.op		= OOSQL_StorageManager::SM_GT;
            startKey[colIndex]  = operand2[0];
		}
		else
		{
			if(startKey[colIndex].value != NULL_POOLINDEX)
			{
				// if new key value is greater than old key value, update key value
				e = compareValueInExpr(startKey[colIndex].value, operand2[0].value, compareResult);
				OOSQL_CHECK_ERR(e);

				if(compareResult > 0)
					break;              // ignore current condition
			}

			// update condition
			isCurrentCondsInBoundConds		= SM_TRUE;
			startBound.op					= OOSQL_StorageManager::SM_GT;
			startKey[colIndex].resultType   = attrTypeID;
			startKey[colIndex].resultLength = attrLength;
			startKey[colIndex].value        = operand2[0].value;

		}
        break;
    case OP_LT:     // adjust stopBound
		if(isJoinCondition)
		{
            if(!(stopBound.op == OOSQL_StorageManager::SM_EOF))
                break;     

            isCurrentCondsInBoundConds = SM_TRUE;

            stopBound.op		= OOSQL_StorageManager::SM_LT;
            stopKey[colIndex]   = operand2[0];
		}
		else
		{
			if(stopKey[colIndex].value != NULL_POOLINDEX)
			{
				// if new key value is smaller than old key value, update key value
				e = compareValueInExpr(stopKey[colIndex].value, operand2[0].value, compareResult);
				OOSQL_CHECK_ERR(e);

				if(compareResult < 0)
					break;
			}
			isCurrentCondsInBoundConds      = SM_TRUE;
			stopBound.op					= OOSQL_StorageManager::SM_LT;
			stopKey[colIndex].resultType    = attrTypeID;
			stopKey[colIndex].resultLength  = attrLength;
			stopKey[colIndex].value         = operand2[0].value;
		}
        break;

    case OP_GE:     // adjust startBound
		if(isJoinCondition)
		{
            if(!(startBound.op == OOSQL_StorageManager::SM_BOF))
                break;     

            isCurrentCondsInBoundConds = SM_TRUE;

            startBound.op		= OOSQL_StorageManager::SM_GE;
            startKey[colIndex]  = operand2[0];
		}
		else
		{
			if(startKey[colIndex].value != NULL_POOLINDEX)
			{
				// if new key value is greater than old key value, update key value
				e = compareValueInExpr(startKey[colIndex].value, operand2[0].value, compareResult);
				OOSQL_CHECK_ERR(e);

				if(compareResult >= 0)
					break;
			}

			isCurrentCondsInBoundConds      = SM_TRUE;
			startBound.op					= OOSQL_StorageManager::SM_GE;
			startKey[colIndex].resultType   = attrTypeID;
			startKey[colIndex].resultLength = attrLength;
			startKey[colIndex].value        = operand2[0].value;
		}
        break;

    case OP_LE:     // adjust stopBound
		if(isJoinCondition)
		{
            if(!(stopBound.op == OOSQL_StorageManager::SM_EOF))
                break;     

            isCurrentCondsInBoundConds = SM_TRUE;

            stopBound.op		= OOSQL_StorageManager::SM_LE;
            stopKey[colIndex]   = operand2[0];
		}
		else
		{
			if(stopKey[colIndex].value != NULL_POOLINDEX)
			{
				// if new key value is smaller than old key value, update key value
				e = compareValueInExpr(stopKey[colIndex].value, operand2[0].value, compareResult);
				OOSQL_CHECK_ERR(e);

				if(compareResult <= 0)
					break;
			}

			isCurrentCondsInBoundConds      = SM_TRUE;
			stopBound.op					= OOSQL_StorageManager::SM_LE;
			stopKey[colIndex].resultType    = attrTypeID;
			stopKey[colIndex].resultLength  = attrLength;
			stopKey[colIndex].value         = operand2[0].value;
		}
        break;
	case OP_LIKE:
		if(isJoinCondition)
		{
			isCurrentCondsInBoundConds      = SM_TRUE;
			startBound.op                   = OOSQL_StorageManager::SM_GE;
			startKey[colIndex]				= operand2[0];

			stopBound.op                    = OOSQL_StorageManager::SM_LE;
			stopKey[colIndex]				= operand2[0];
		}
		else
		{
			isCurrentCondsInBoundConds      = SM_TRUE;
			startBound.op                   = OOSQL_StorageManager::SM_GE;
			startKey[colIndex].resultType   = attrTypeID;
			startKey[colIndex].resultLength = attrLength;
			startKey[colIndex].value        = operand2[0].value;

			stopBound.op                    = OOSQL_StorageManager::SM_LE;
			stopKey[colIndex].resultType    = attrTypeID;
			stopKey[colIndex].resultLength  = attrLength;
			stopKey[colIndex].value         = operand2[0].value;
		}
		break;
	default:
		OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
		break;
    }

	return eNOERROR;
}

ValuePoolIndex OQL_GDStoCommonAP::constructMaxValueOfTheType(Four type, Four length)
{
	ValuePoolElements	value(m_pool->valuePool);
	IntegerPoolElements integer(m_pool->intPool);
	RealPoolElements	real(m_pool->realPool);
	StringPoolElements	string(m_pool->stringPool);
	Four				i;
	Two					s;

	value  = m_pool->valuePool.addNewEntry();

	switch(type)
	{
    case TYPEID_SHORT:
		integer            = m_pool->intPool.addNewEntry();
		integer[0]         = SHRT_MAX;
		value[0].valueKind = VALUE_KIND_INTEGER;
		value[0].integer   = integer.getPoolIndex();
		break;

    case TYPEID_INT:
		integer            = m_pool->intPool.addNewEntry();
		integer[0]         = INT_MAX;
		value[0].valueKind = VALUE_KIND_INTEGER;
		value[0].integer   = integer.getPoolIndex();
		break;

	case TYPEID_LONG:
		integer            = m_pool->intPool.addNewEntry();
		integer[0]         = INT_MAX;
		value[0].valueKind = VALUE_KIND_INTEGER;
		value[0].integer   = integer.getPoolIndex();
		break;

	case TYPEID_LONG_LONG:
		integer            = m_pool->intPool.addNewEntry();
		integer[0]         = LONG_MAX;
		value[0].valueKind = VALUE_KIND_INTEGER;
		value[0].integer   = integer.getPoolIndex();
		break;

    case TYPEID_FLOAT:
		real               = m_pool->realPool.addNewEntry();
		real[0]            = FLT_MAX;
		value[0].valueKind = VALUE_KIND_REAL;
		value[0].real      = real.getPoolIndex();
		break;

    case TYPEID_DOUBLE:
		real               = m_pool->realPool.addNewEntry();
		real[0]            = DBL_MAX;
		value[0].valueKind = VALUE_KIND_REAL;
		value[0].real      = real.getPoolIndex();
		break;

    case TYPEID_STRING:
	case TYPEID_VARSTRING:
		string = m_pool->stringPool.addNewEntry(length);
		for(i = 0; i < length; i++)
			string[i] = 0xff;
		value[0].valueKind = VALUE_KIND_STRING;
		value[0].string    = string.getPoolIndex();
		break;
	
    case TYPEID_MBR:
	case TYPEID_DATE:
	case TYPEID_TIME:
	case TYPEID_TIMESTAMP:
	case TYPEID_INTERVAL:
    default:
		value.setNull();
	    break;
	}

	return value.getPoolIndex();
}

ValuePoolIndex OQL_GDStoCommonAP::constructMinValueOfTheType(Four type, Four length)
{
	ValuePoolElements	value(m_pool->valuePool);
	IntegerPoolElements integer(m_pool->intPool);
	RealPoolElements	real(m_pool->realPool);
	StringPoolElements	string(m_pool->stringPool);
	Four				i;
	Two					s;

	value  = m_pool->valuePool.addNewEntry();

	switch(type)
	{
    case TYPEID_SHORT:
		integer            = m_pool->intPool.addNewEntry();
		integer[0]         = SHRT_MIN;
		value[0].valueKind = VALUE_KIND_INTEGER;
		value[0].integer   = integer.getPoolIndex();
		break;

    case TYPEID_INT:
		integer            = m_pool->intPool.addNewEntry();
		integer[0]         = INT_MIN;
		value[0].valueKind = VALUE_KIND_INTEGER;
		value[0].integer   = integer.getPoolIndex();
		break;

	case TYPEID_LONG:
		integer            = m_pool->intPool.addNewEntry();
		integer[0]         = LONG_MIN;
		value[0].valueKind = VALUE_KIND_INTEGER;
		value[0].integer   = integer.getPoolIndex();
		break;

	case TYPEID_LONG_LONG:
		integer            = m_pool->intPool.addNewEntry();
		integer[0]         = LONG_MIN;
		value[0].valueKind = VALUE_KIND_INTEGER;
		value[0].integer   = integer.getPoolIndex();
		break;

    case TYPEID_FLOAT:
		real               = m_pool->realPool.addNewEntry();
		real[0]            = FLT_MIN;
		value[0].valueKind = VALUE_KIND_REAL;
		value[0].real      = real.getPoolIndex();
		break;

    case TYPEID_DOUBLE:
		real               = m_pool->realPool.addNewEntry();
		real[0]            = DBL_MIN;
		value[0].valueKind = VALUE_KIND_REAL;
		value[0].real      = real.getPoolIndex();
		break;

    case TYPEID_STRING:
	case TYPEID_VARSTRING:
		string = m_pool->stringPool.addNewEntry(length);
		for(i = 0; i < length; i++)
			string[i] = 0;
		value[0].valueKind = VALUE_KIND_STRING;
		value[0].string    = string.getPoolIndex();
		break;
	
    case TYPEID_MBR:
	case TYPEID_DATE:
	case TYPEID_TIME:
	case TYPEID_TIMESTAMP:
	case TYPEID_INTERVAL:
    default:
		value.setNull();
	    break;
	}

	return value.getPoolIndex();
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
error code

IMPLEMENTATION:
****************************************************************************/
Four OQL_GDStoCommonAP::makePlan_determineBoundConditionForB_TreeJoin(Four planNo, Four nKeyCols, Two* colNos, 
                                                                      AP_CondListPoolElements conds,
                                                                      AP_BoundCondInfo& startBound, 
                                                                      AP_BoundCondInfo& stopBound,
                                                                      AP_CondListPoolIndex& resultConds, 
                                                                      Boolean operandSwapped,
																	  OOSQL_StorageManager::KeyDesc& btreeKeyDesc)
{
    AP_ExprPoolElements         startKey(m_pool->ap_exprPool);
    AP_ExprPoolElements         stopKey(m_pool->ap_exprPool);
    AP_ExprPoolElements         expr(m_pool->ap_exprPool);
    AP_ExprPoolElements         operand1(m_pool->ap_exprPool), operand2(m_pool->ap_exprPool);
    AP_CondListPoolElements     resultCondsElements(m_pool->ap_condListPool);
    Four                        nKeyParts;      
    int                         compareResult;
    Four                        i, j;
    Boolean                     isCurrentCondsInBoundConds;
	TypeID						attrTypeID;
	Four						attrLength;
	CataClassInfo				classInfo;
	CataAttrInfo				attrInfo;
    OperatorID                  operatorId;
	Boolean						isNullKeyValueExist;
	Four						colIndex;
	AP_BoundCondInfo			startBoundArray[MAXNUMKEYPARTS];
	AP_BoundCondInfo			stopBoundArray[MAXNUMKEYPARTS];
	CommonAP_PoolElements       apNode(m_pool->commonAP_Pool);
	Four						e;
	
    resultConds.setNull();

    // get nKeyParts
    nKeyParts = nKeyCols;

    startBound.op  = OOSQL_StorageManager::SM_BOF;
    startBound.key = m_pool->ap_exprPool.addNewEntry(nKeyParts);
    startKey       = startBound.key;
    for(i = 0;i < nKeyParts; i++)
    {
        startKey[i].exprKind        = EXPR_KIND_VALUE;
        startKey[i].resultType      = TYPEID_NONE;
        startKey[i].resultLength    = 0;
        startKey[i].value.setNull();
		startBoundArray[i].op = OOSQL_StorageManager::SM_BOF;
    }

    stopBound.op   = OOSQL_StorageManager::SM_EOF;
    stopBound.key  = m_pool->ap_exprPool.addNewEntry(nKeyParts);
    stopKey        = stopBound.key;
    for(i = 0;i < nKeyParts; i++)
    {
        stopKey[i].exprKind         = EXPR_KIND_VALUE;
        stopKey[i].resultType       = TYPEID_NONE;
        stopKey[i].resultLength     = 0;
        stopKey[i].value.setNull();
		stopBoundArray[i].op = OOSQL_StorageManager::SM_EOF;
    }

    for(i = 0;i < conds.size; i++)
    {
        isCurrentCondsInBoundConds = SM_FALSE;

        expr = conds[i].expr;
        
        // check operator, only arithmetic operator can be applied
        if((expr[0].oper.operatorId == OP_EQ || expr[0].oper.operatorId == OP_NE ||
            expr[0].oper.operatorId == OP_GT || expr[0].oper.operatorId == OP_LT ||
            expr[0].oper.operatorId == OP_GE || expr[0].oper.operatorId == OP_LE))
        {
            // check each operand
            operand1 = expr[0].oper.operand1;
            operand2 = expr[0].oper.operand2;

            if((operand1[0].exprKind == EXPR_KIND_PATHEXPR && operand2[0].exprKind == EXPR_KIND_PATHEXPR))
            {
				for(colIndex = -1, j = 0; j < nKeyParts; j++)
				{
					if(operand2[0].pathExpr.col.planNo == planNo && operand2[0].pathExpr.col.colNo == colNos[j])
					{
						colIndex = j;
						break;
					}
				}

                if(colIndex >= 0)
                {   // swap operand1 and operand2
                    operand1 = expr[0].oper.operand2;
                    operand2 = expr[0].oper.operand1;
                }

				for(colIndex = -1, j = 0; j < nKeyParts; j++)
				{
					if(operand1[0].pathExpr.col.planNo == planNo && operand1[0].pathExpr.col.colNo == colNos[j])
					{
						colIndex = j;
						break;
					}
				}

                if(colIndex >= 0)
                {
					e = m_catalog->class_ClassId_to_ClassInfo(apNode[planNo].classInfo.classId, classInfo);
					OOSQL_CHECK_ERR(e);

					e = m_catalog->attr_ColNo_to_AttrInfo(classInfo, colNos[colIndex], attrInfo);
					OOSQL_CHECK_ERR(e);

					e = m_catalog->attr_GetTypeID(classInfo, attrInfo, attrTypeID);
					OOSQL_CHECK_ERR(e);

					e = m_catalog->attr_GetLength(classInfo, attrInfo, attrLength);
					OOSQL_CHECK_ERR(e);

                    // found condition which used in index scan
                    if(operandSwapped)
                        operatorId = reverseComparisionOperatorID(expr[0].oper.operatorId);
                    else
                        operatorId = expr[0].oper.operatorId;

					e = makePlan_constructBoundConditionForExpression(expr, operand1, operand2, colIndex, attrTypeID, attrLength,
																	  startBoundArray[colIndex], stopBoundArray[colIndex], 
																	  startKey, stopKey, isCurrentCondsInBoundConds,
																	  SM_TRUE /* isJoinCondition */);
					OOSQL_CHECK_ERR(e);
                }
            }   
        }
        // check operator, only arithmetic operator can be applied
        if((expr[0].oper.operatorId == OP_EQ || expr[0].oper.operatorId == OP_NE ||
            expr[0].oper.operatorId == OP_GT || expr[0].oper.operatorId == OP_LT ||
            expr[0].oper.operatorId == OP_GE || expr[0].oper.operatorId == OP_LE ||
            expr[0].oper.operatorId == OP_LIKE || expr[0].oper.operatorId == OP_IN))
        {
            // check each operand
            operand1 = expr[0].oper.operand1;
            operand2 = expr[0].oper.operand2;
 
            if((operand1[0].exprKind == EXPR_KIND_PATHEXPR && operand2[0].exprKind == EXPR_KIND_VALUE) ||
               (operand2[0].exprKind == EXPR_KIND_PATHEXPR && operand1[0].exprKind == EXPR_KIND_VALUE))
            {
                if(operand1[0].exprKind == EXPR_KIND_VALUE)
                {   // swap operand1 and operand2
                    operand1 = expr[0].oper.operand2;
                    operand2 = expr[0].oper.operand1;
                }
 
                for(colIndex = -1, j = 0; j < nKeyParts; j++)
                {
                    if(operand1[0].pathExpr.col.planNo == planNo && operand1[0].pathExpr.col.colNo == colNos[j])
                    {
                        colIndex = j;
                        break;
                    }
                }
 
                if(colIndex >= 0)   // find the matching column
                {
                    e = m_catalog->class_ClassId_to_ClassInfo(apNode[planNo].classInfo.classId, classInfo);
                    OOSQL_CHECK_ERR(e);
 
                    e = m_catalog->attr_ColNo_to_AttrInfo(classInfo, colNos[colIndex], attrInfo);
                    OOSQL_CHECK_ERR(e);
 
                    e = m_catalog->attr_GetTypeID(classInfo, attrInfo, attrTypeID);
                    OOSQL_CHECK_ERR(e);
 
                    e = m_catalog->attr_GetLength(classInfo, attrInfo, attrLength);
                    OOSQL_CHECK_ERR(e);
 
                    // found condition which used in index scan
                    if(operandSwapped)
                        operatorId = reverseComparisionOperatorID(expr[0].oper.operatorId);
                    else
                        operatorId = expr[0].oper.operatorId;
 
                    e = makePlan_constructBoundConditionForExpression(expr, operand1, operand2, colIndex, attrTypeID, attrLength,
                                                                      startBoundArray[colIndex], stopBoundArray[colIndex], startKey, stopKey, isCurrentCondsInBoundConds,
                                                                      SM_TRUE /* isJoinCondition */);
                    OOSQL_CHECK_ERR(e);
 
                }
 
                if(isCurrentCondsInBoundConds)
                {
                    if(attrTypeID == TYPEID_STRING)
                    {
                        e = makePlan_retouchStringBoundCondition(expr, colIndex, attrLength, startKey, stopKey);
                        OOSQL_CHECK_ERR(e);
                    }
                    else if(attrTypeID == TYPEID_VARSTRING)
                    {
                        e = makePlan_retouchVarStringBoundCondition(expr, colIndex, attrLength, startKey, stopKey);
                        OOSQL_CHECK_ERR(e);
                    }
                }
 
            }
        }

        if(expr[0].oper.operatorId == OP_LIKE)
            isCurrentCondsInBoundConds = SM_FALSE;
	
        if(!isCurrentCondsInBoundConds ||
		   nKeyParts > 1)						
        {
            resultCondsElements = m_pool->ap_condListPool.addNewEntry();

            if(resultConds == NULL_POOLINDEX)
                resultConds = resultCondsElements.getPoolIndex();
            else
                resultConds.size ++;

            resultCondsElements[0] = conds[i];
        }
    }

	e = makePlan_constructUnboundConditionAsMinMaxValue(nKeyParts, btreeKeyDesc, startBoundArray, stopBoundArray, startKey, stopKey);
	OOSQL_CHECK_ERR(e);

	e = makePlan_detremineMultikeyBoundCondition(nKeyParts, startBoundArray, stopBoundArray, startBound, stopBound);
	OOSQL_CHECK_ERR(e);


    return eNOERROR;
}

Four OQL_GDStoCommonAP::makePlan_detremineMultikeyBoundCondition(Four nKeyParts, 
																 AP_BoundCondInfo* startBoundArray, 
																 AP_BoundCondInfo* stopBoundArray, 
																 AP_BoundCondInfo& startBound, 
																 AP_BoundCondInfo& stopBound)
{
	Four						 i;
	OOSQL_StorageManager::CompOp prevOp;
	Boolean						 isAllSameOperator;


	// determine startbound
	isAllSameOperator = SM_TRUE;
	prevOp = startBoundArray[0].op;
	for(i = 1; i < nKeyParts; i++)
	{
		if(prevOp != startBoundArray[i].op)
		{
			isAllSameOperator = SM_FALSE;
			break;
		}
	}
	if(!isAllSameOperator)
		startBound.op = OOSQL_StorageManager::SM_GE;
	else 
		startBound.op = startBoundArray[0].op;
	
	// determine stopbound
	isAllSameOperator = SM_TRUE;
	prevOp = stopBoundArray[0].op;
	for(i = 1; i < nKeyParts; i++)
	{
		if(prevOp != stopBoundArray[i].op)
		{
			isAllSameOperator = SM_FALSE;
			break;
		}
	}
	if(!isAllSameOperator)
		stopBound.op = OOSQL_StorageManager::SM_LE;
	else 
		stopBound.op = stopBoundArray[0].op;

	return eNOERROR;
}


/****************************************************************************
DESCRIPTION:

RETURN VALUE:
error code

IMPLEMENTATION:
****************************************************************************/
Four OQL_GDStoCommonAP::compareValueInExpr(ValuePoolIndex a,ValuePoolIndex b, int& compareResult)
{
    ValuePoolElements   value_a(m_pool->valuePool);
    ValuePoolElements   value_b(m_pool->valuePool);
    SimpleString        string_a, string_b;
    long                int_a, int_b;
    float               float_a, float_b;
    Four                e;

    value_a = a;
    value_b = b;

    if(value_a[0].valueKind != value_b[0].valueKind)
        OOSQL_ERR(eINTERNALERROR_OOSQL);
    
    switch(value_a[0].valueKind)
    {
    case VALUE_KIND_STRING:
        e = getStringFromStringPool(value_a[0].string, string_a);
        OOSQL_CHECK_ERR(e);

        e = getStringFromStringPool(value_b[0].string, string_b);
        OOSQL_CHECK_ERR(e);

        if(strcmp((char*)string_a, (char*)string_b) < 0)
            compareResult = -1;
        else if(strcmp((char*)string_a, (char*)string_b) > 0)
            compareResult = 1;
        else
            compareResult = 0;
        break;

    case VALUE_KIND_INTEGER:
        e = getIntegerFromIntegerPool(value_a[0].integer, int_a);
        OOSQL_CHECK_ERR(e);

        e = getIntegerFromIntegerPool(value_b[0].integer, int_b);
        OOSQL_CHECK_ERR(e);

        if(int_a < int_b)
            compareResult = -1;
        else if(int_a > int_b)
            compareResult = 1;
        else
            compareResult = 0;

        break;

    case VALUE_KIND_REAL:
        e = getFloatFromRealPool(value_a[0].real, float_a);
        OOSQL_CHECK_ERR(e);

        e = getFloatFromRealPool(value_b[0].real, float_b);
        OOSQL_CHECK_ERR(e);

        if(float_a < float_b)
            compareResult = -1;
        else if(float_a > float_b)
            compareResult = 1;
        else
            compareResult = 0;

        break;

    case VALUE_KIND_BOOL:
    case VALUE_KIND_NIL:
    case VALUE_KIND_MBR:
        OOSQL_ERR(eINTERNALERROR_OOSQL);
        break;
    }

    return eNOERROR;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
error code

IMPLEMENTATION:
****************************************************************************/
Four OQL_GDStoCommonAP::getStringFromStringPool(StringPoolIndex index, SimpleString& string)
{
    StringPoolElements  spElements(m_pool->stringPool);
    Four                i;

    spElements = index;
    for(i = 0; i < spElements.size; i++)
        string[i] = spElements[i];
    string[i] = '\0';

    return eNOERROR;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
error code

IMPLEMENTATION:
****************************************************************************/
Four OQL_GDStoCommonAP::getIntegerFromIntegerPool(IntegerPoolIndex index, long& integer)
{
    IntegerPoolElements ipElements(m_pool->intPool);

    ipElements = index;

    integer = ipElements[0];

    return eNOERROR;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
error code

IMPLEMENTATION:
****************************************************************************/
Four OQL_GDStoCommonAP::getFloatFromRealPool(RealPoolIndex index, float& real)
{
    RealPoolElements rpElements(m_pool->realPool);

    rpElements = index;

    real = rpElements[0];

    return eNOERROR;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
error code

IMPLEMENTATION:
****************************************************************************/
Four OQL_GDStoCommonAP::makePlan_determineScanMethod(CommonAP_PoolElements& apNode, QGNodePoolElements& qgNode)
{
    AP_CondListPoolElements     conds(m_pool->ap_condListPool);
    SubClassPoolElements        subClasses(m_pool->subClassPool);
    Four                        i;
    Four                        e;

    // -------------------------------
    // determine class's access method
    // -------------------------------
    if(apNode[0].classInfo.classKind != CLASSKIND_PERSISTENT)
        OOSQL_ERR(eINTERNALERROR_OOSQL);

    // if accessMethod is OIDFETCH then skip determining scan method.
    if(apNode[0].accessMethod != CAP_ACCESSMETHOD_OIDFETCH)
    {
        conds = apNode[0].condNodes;
        apNode[0].accessMethod = CAP_ACCESSMETHOD_SEQSCAN;
        e = makePlan_determineScanMethod(apNode.startIndex, apNode[0].classInfo.classId, conds, 
                                         apNode[0].accessMethod, 
                                         apNode[0].indexInfo, 
                                         apNode[0].condNodes);
        OOSQL_CHECK_ERR(e);
    }

    // -----------------------------------
    // determine subclasses' access method
    // -----------------------------------
    subClasses = apNode[0].subClassInfo;

    for(i = 0; i < subClasses.size; i++)
    {
        if(apNode[0].accessMethod == CAP_ACCESSMETHOD_OIDFETCH)
        {
            subClasses[i].accessMethod = CAP_ACCESSMETHOD_OIDFETCH;
			subClasses[i].condNodes = apNode[0].condNodes;
        }
        else if(apNode[0].accessMethod == CAP_ACCESSMETHOD_NONE)
        {
            OOSQL_ERR(eINTERNALERROR_OOSQL);
        }
        else
        {
            // apNode[0].accessMethod is CAP_ACCESSMETHOD_SEQSCAN or CAP_ACCESSMETHOD_INDEXSCAN (BTREE, TEXT, MLGF)
            // determine subclass's scan method from m_catalog information
			e = makePlan_determineScanMethod(apNode.startIndex, subClasses[i].classId, conds, 
                                             subClasses[i].accessMethod, 
                                             subClasses[i].indexInfo,
                                             subClasses[i].condNodes);
            OOSQL_CHECK_ERR(e);
        }
    }

    return eNOERROR;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
plan number

IMPLEMENTATION:
****************************************************************************/
Four OQL_GDStoCommonAP::getPlanNoFromArgument(ArgumentPoolElements& argument)
{
    Four        planNo;

    // this function can process aggregrate function's argument
    // aggregate function has only one argument
    if(argument.size != 1)          // no argument or more than one argument
        OOSQL_ERR(eUNHANDLED_CASE_OOSQL);

    switch(argument[0].argumentKind)
    {
    case ARGUMENT_KIND_PATHEXPR:
        Four        lastElementIndex;

        lastElementIndex = argument[0].pathExpr.startIndex + argument[0].pathExpr.size - 1;
        planNo = m_pool->pathExprPool[lastElementIndex].planNo.startIndex;
        break;

    case ARGUMENT_KIND_VALUE:
    case ARGUMENT_KIND_FUNC:
    case ARGUMENT_KIND_DOMAIN:
    case ARGUMENT_KIND_AGGRFUNC:
        planNo = -1;
        break;
    }

    return planNo;  
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
error code

IMPLEMENTATION:
****************************************************************************/
Four OQL_GDStoCommonAP::makePlan_constructGroupByHaving()
{
    CommonAP_PoolElements           apNode(m_pool->commonAP_Pool);
    CommonAP_PoolElements           apNode_temp(m_pool->commonAP_Pool);
    AP_ProjectionListPoolElements   projectionList(m_pool->projectionListPool);
    AP_ProjectionPoolElements       projectionInfo(m_pool->projectionPool);
    AP_ProjectionPoolElements       projectionInfoForAggrFunc(m_pool->projectionPool);
    SelListPoolElements             selList(m_pool->selListPool);
    GroupByListPoolElements         groupByList(m_pool->groupByListPool);
    AP_ColNoMapPoolElements         colNoMap(m_pool->colNoMapPool);
    AP_UsedColPoolElements          usedColInfo(m_pool->usedColPool);
    AggrFuncPoolElements            aggrFunc(m_pool->aggrFuncPool);
    ArgumentPoolElements            argument(m_pool->argumentPool);
    AP_TempFileInfoPoolElements     tempFileInfo(m_pool->tempFileInfoPool);
    AP_TempFileInfoPoolElements     tempFileInfoForAggrFunc(m_pool->tempFileInfoPool);
    Four                            tempFileNum;
    Four                            tempFileNumForGroupBy;
    Four                            length;
    Four                            i, j;   // loop counter
    Four                            index;  // loop counter
    Four                            nGrpByKeys;
    Four                            e;      // error code
    ExprPoolElements                havingCond(m_pool->exprPool);
    ExprPoolElements                expr(m_pool->exprPool);
    ExprPoolIndex                   exprPoolIndex;
	PathExprPoolElements			pathExpr(m_pool->pathExprPool);
	OrderByListPoolElements         orderByList(m_pool->orderByListPool);

	e = m_gds->getSelList(m_pool, selList);			OOSQL_CHECK_ERR(e);
	e = m_gds->getGroupByList(m_pool, groupByList);	OOSQL_CHECK_ERR(e);
	e = m_gds->getHavingCond(m_pool, havingCond);	OOSQL_CHECK_ERR(e);
	e = m_gds->getOrderByList(m_pool, orderByList);

    // -------------------
    // for last AP element
    // -------------------
    apNode.setPoolIndex(m_commonAP->commonAP.size - 1, 1);

    // count projectionList length and allocate
    // length = 1 (for group by) + aggr function with DISTINCT
    // projectionList[0]    : for group by
    // projectionList[1...] : for aggr func with DISTINCT
    for(i = 0, length = 1; i < selList.size; i++)
    {
        if(selList[i].selElemKind == SELLIST_KIND_AGGRFUNC)
        {
            aggrFunc = selList[i].aggrFunc;
            if(aggrFunc[0].distinctFlag == SM_TRUE)    // aggr function with DISTINCT
                length ++;
        }
    }

    projectionList = m_pool->projectionListPool.addNewEntry(length);

    // initialize each projection info
    for(i = 0; i < projectionList.size; i++) projectionList[i].init();

    // connect projection with accessplan
    apNode[0].projectionList = projectionList.getPoolIndex();

    // count tempFileInfo length for group by (for projectionList[0]) and allocate
    // length = groupby pathexpr elements + selList pathExpr or aggrfunc elements without DISTINCT 
    length = 0;
    for(i = 0; i < groupByList.size; i++)
    {
        if(groupByList[i].grpByKeyKind == GROUPBY_KIND_PATHEXPR)
            length ++;
    }
    for(i = 0; i < selList.size; i++)
    {
        if(selList[i].selElemKind == SELLIST_KIND_PATHEXPR)
            length ++;
        else if(selList[i].selElemKind == SELLIST_KIND_AGGRFUNC)
        {
            aggrFunc = selList[i].aggrFunc;
            if(aggrFunc[0].distinctFlag == SM_FALSE)
                length ++;
        }
    }

    for (i = 0; i < havingCond.size; i++)
    {
        //if the number of operand is 3
        //for (j = 0; j < 3; j ++)
        //if the number of operand is 2
        for (j = 0; j < 2; j ++)
        {
            //0 means operand1, 1 means operand2, 2 means operand3
            switch(j){
            case 0:
                exprPoolIndex = havingCond[i].oper.operand1;
                expr = exprPoolIndex;
                break;
            case 1:
                exprPoolIndex = havingCond[i].oper.operand2;
                expr = exprPoolIndex;
                break;
            // if there is operand3,
            /*
            case 2:
                exprPoolIndex = havingCond[i].oper.operand3;
                expr = exprPoolIndex;
                break;
             */
            default:
                OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
                break;
            }

            if(expr[0].exprKind == EXPR_KIND_AGGRFUNC)
                length++;
        }

    }

	for (i = 0; i < orderByList.size; i++){
		if (orderByList[i].ordByKeyKind == ORDERBYLIST_KIND_AGGRFUNC)
			length ++;
	}

    tempFileInfo = m_pool->tempFileInfoPool.addNewEntry(length);
    tempFileNumForGroupBy = tempFileNum  = getNewTempFileNum(); OOSQL_CHECK_ERR(tempFileNum);
    e = setTempFileInfoPoolIndex(tempFileNum, tempFileInfo.getPoolIndex()); OOSQL_CHECK_ERR(e);
    e = setTempFileUsage(tempFileNum, TEMPFILE_FOR_GROUPBY, 0);             OOSQL_CHECK_ERR(e);
    projectionList[0].tempFileNum = tempFileNum;

    // count projectionList length for group by - this value is the same with tempFileInfo length for group by
    // allocate
    projectionInfo = m_pool->projectionPool.addNewEntry(length);
    projectionList[0].projectionInfo = projectionInfo.getPoolIndex();

    // make projectionList[0] and tempFileInfo
    // for groupByList
    for(i = 0, index = 0, nGrpByKeys = 0; i < groupByList.size; i++)
    {
        switch(groupByList[i].grpByKeyKind)
        {
        case GROUPBY_KIND_PATHEXPR:
        case GROUPBY_KIND_AGGRFUNC:
            e = setProjection(projectionInfo[index], groupByList[i]);
            OOSQL_CHECK_ERR(e);

            // set sort information
            projectionList[0].sortKeys[index]    = index;
            projectionList[0].sortAscDesc[index] = SORTORDER_ASC; 
            projectionList[0].nSortKeys ++;

            // set tempFileInfo
            e = makePlan_constructTempFileInfo(tempFileInfo[index], projectionInfo[index], groupByList[i]);
            OOSQL_CHECK_ERR(e);

            nGrpByKeys ++;
            index ++;
            break;

        case GROUPBY_KIND_EXPR:
            OOSQL_ERR(eNOTIMPLEMENTED_OOSQL);
            break;
        default:
            OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
            break;
        }
    }

    // for selList
    for(i = 0; i < selList.size; i++)
    {
        switch(selList[i].selElemKind)
        {
        case SELLIST_KIND_PATHEXPR:
        case SELLIST_KIND_AGGRFUNC:
            // if aggrfunc without DISTINCT, skip
            if(selList[i].selElemKind == SELLIST_KIND_AGGRFUNC)
            {
                aggrFunc = selList[i].aggrFunc;
                if(aggrFunc[0].distinctFlag == SM_TRUE)
                    break;
            }

            // fill projection info
            e = setProjection(projectionInfo[index], selList[i]);
            OOSQL_CHECK_ERR(e);

            // set tempFileInfo
            if(selList[i].selElemKind == SELLIST_KIND_AGGRFUNC)
            {
                argument = aggrFunc[0].argument;
                e = makePlan_constructTempFileInfo(tempFileInfo[index], projectionInfo[index], argument[0]);
            }
            else
                e = makePlan_constructTempFileInfo(tempFileInfo[index], projectionInfo[index], selList[i]);
            OOSQL_CHECK_ERR(e);

            index ++;
            break;
        case SELLIST_KIND_FUNC:
            OOSQL_ERR(eNOTIMPLEMENTED_OOSQL);
            break;
        case SELLIST_KIND_VALUE: 
        case SELLIST_KIND_OPER:
        case SELLIST_KIND_CONS:
            break;
        default:
            OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
            break;
        }
    }

    //for having
    for (i = 0; i < havingCond.size; i++)
    {
        //if the number of operand is 3
        //for (j = 0; j < 3; j ++)
        //if the number of operand is 2
        for (j = 0; j < 2; j ++)
        {
            //0 means operand1, 1 means operand2, 2 means operand3
            switch(j){
            case 0:
                exprPoolIndex = havingCond[i].oper.operand1;
                expr = exprPoolIndex;
                break;
            case 1:
                exprPoolIndex = havingCond[i].oper.operand2;
                expr = exprPoolIndex;
                break;
            // if there is operand3,
            /*
               case 2:
               exprPoolIndex = havingCond[i].oper.operand3;
               expr = exprPoolIndex;
               break;
             */
            default:
                OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
                break;

            }

            switch(expr[0].exprKind){
            case EXPR_KIND_PATHEXPR:
				argument[0].pathExpr = expr[0].pathExpr;
				argument[0].argumentKind = ARGUMENT_KIND_PATHEXPR;
				e = setProjection(projectionInfo[index], argument[0]);
				OOSQL_CHECK_ERR(e);
				
				// set tempFilInfo
				e = makePlan_constructTempFileInfo(tempFileInfo[index], projectionInfo[index], argument[0]);
				OOSQL_CHECK_ERR(e);
				
				index++;
				break;
            case EXPR_KIND_AGGRFUNC:
                // fill proejction info
                aggrFunc = expr[0].aggrFunc;
                argument = aggrFunc[0].argument;
                e = setProjection(projectionInfo[index], argument[0]);
                OOSQL_CHECK_ERR(e);

                // set tempFilInfo
                e = makePlan_constructTempFileInfo(tempFileInfo[index], projectionInfo[index], argument[0]);
                OOSQL_CHECK_ERR(e);

                index++;
                break;
            case EXPR_KIND_FUNCTION:
                OOSQL_ERR(eNOTIMPLEMENTED_OOSQL);
                break;
            case EXPR_KIND_OPER:
            case EXPR_KIND_VALUE:
            case EXPR_KIND_CONS:
            case EXPR_KIND_AGGRFUNCRESULT:
            case EXPR_KIND_FUNCRESULT:
            case EXPR_KIND_FUNCEVAL:
                break;
            default:
                OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
                break;
            }
        }
    }

	for (i = 0; i < orderByList.size; i++){
		switch(orderByList[i].ordByKeyKind){
		case ORDERBYLIST_KIND_PATHEXPR:
			break;
		case ORDERBYLIST_KIND_AGGRFUNC:
			aggrFunc = orderByList[i].pathExpr;
			argument = aggrFunc[0].argument;
			e = setProjection(projectionInfo[index], argument[0]);
			OOSQL_CHECK_ERR(e);
			
			e = makePlan_constructTempFileInfo(tempFileInfo[index], projectionInfo[index], argument[0]);
			OOSQL_CHECK_ERR(e);

			index++;
			break;
		case ORDERBYLIST_KIND_FUNC:
			break;
		}
	}

    // fill projectionList for aggrfunc with DISTINCT flag
    for(i = 0, index = 1; i < selList.size; i++)
    {
        if(selList[i].selElemKind == SELLIST_KIND_AGGRFUNC)
        {
            aggrFunc = selList[i].aggrFunc;
            argument = aggrFunc[0].argument;
            if(aggrFunc[0].distinctFlag == SM_TRUE)    // aggr function with DISTINCT
            {
                // alloc tempFileInfoForAggrFunc
                tempFileInfoForAggrFunc = m_pool->tempFileInfoPool.addNewEntry(nGrpByKeys + argument.size);
                tempFileNum = getNewTempFileNum();  OOSQL_CHECK_ERR(tempFileNum);
                e = setTempFileInfoPoolIndex(tempFileNum, tempFileInfoForAggrFunc.getPoolIndex());  
                OOSQL_CHECK_ERR(e);
                e = setTempFileUsage(tempFileNum, TEMPFILE_FOR_AGGR_DISTINCT, i);
                OOSQL_CHECK_ERR(e);
                projectionList[index].tempFileNum= tempFileNum;

                // alloc projectionInfoForAggrFunc
                projectionInfoForAggrFunc = m_pool->projectionPool.addNewEntry(nGrpByKeys + argument.size);
                projectionList[index].projectionInfo = projectionInfoForAggrFunc.getPoolIndex();

                // fill tempFileInfoForAggrFunc and projectionInfoForAggrFunc
                for(j = 0; j < nGrpByKeys; j++)
                {
                    tempFileInfoForAggrFunc[j]   = tempFileInfo[j];
                    projectionInfoForAggrFunc[j] = projectionInfo[j];
                }

                // fill last element's tempFileInfoForAggrFunc and projectionInfoForAggrFunc
                for(j = 0; j < argument.size; j++)
                {
                    e = setProjection(projectionInfoForAggrFunc[j + nGrpByKeys], argument[j]);
                    OOSQL_CHECK_ERR(e);
                    
                    e = makePlan_constructTempFileInfo(tempFileInfoForAggrFunc[j + nGrpByKeys], projectionInfoForAggrFunc[j + nGrpByKeys], argument[j]);
                    OOSQL_CHECK_ERR(e);
                }

                // fill sort information
                for(j = 0; j < projectionInfoForAggrFunc.size; j++)
                {
                    projectionList[index].sortKeys[j]    = j;
                    projectionList[index].sortAscDesc[j] = SORTORDER_ASC;
                }
                projectionList[index].nSortKeys = projectionInfoForAggrFunc.size;

                index ++;
            }
        }
    }

    // -------------------
    // make new AP element
    // -------------------
    apNode = m_pool->commonAP_Pool.addNewEntry();
    apNode[0].init();

    if(m_commonAP->commonAP != NULL_POOLINDEX)
        m_commonAP->commonAP.size ++;
    else
        m_commonAP->commonAP = apNode.getPoolIndex();
    
    // set accessMethod and joinMethod
    apNode[0].accessMethod = CAP_ACCESSMETHOD_SEQSCAN;
    apNode[0].joinMethod   = CAP_JOINMETHOD_OUTERMOST_CLASS;

    // set classInfo
    apNode[0].classInfo.classKind   = CLASSKIND_SORTSTREAM;
    apNode[0].classInfo.tempFileNum = tempFileNumForGroupBy;

    // set usedColInfo
    colNoMap    = m_pool->colNoMapPool.addNewEntry(tempFileInfo.size);
    usedColInfo = m_pool->usedColPool.addNewEntry(tempFileInfo.size);
    apNode[0].colNoMap    = colNoMap.getPoolIndex();
    apNode[0].usedColInfo = usedColInfo.getPoolIndex();
    for(i = 0;i < tempFileInfo.size; i++)
    {
        colNoMap[i].offset    = i;
        usedColInfo[i].typeId = tempFileInfo[i].typeId;
        usedColInfo[i].length = tempFileInfo[i].length;
		usedColInfo[i].colNo  = i;
    }

    // set group by information
    apNode[0].nGrpByKeys = nGrpByKeys;
    for(i = 0; i < nGrpByKeys; i++)
        apNode[0].grpByKeys[i] = i;

    // set m_aggrFuncInfo_planNo
    if(m_aggrFuncInfo_planNo == NULL_POOLINDEX)
    {
        m_aggrFuncInfo_planNo = apNode.getPoolIndex();

        // make aggrFuncInfo
        e = makePlan_constructAggrFuncInfo(apNode);
        OOSQL_CHECK_ERR(e);
    }
    
    // set having condition
    if(havingCond != NULL_POOLINDEX)
    {
		ExprPoolElements		expr(m_pool->exprPool);
        AP_ExprPoolElements		ap_expr(m_pool->ap_exprPool);
		AP_CondListPoolElements ap_condList(m_pool->ap_condListPool);

        // set number of having condition
        apNode[0].noHavConds = 1;

        expr = havingCond;

        // convert expr to ap_expr
        e = convertExpr_to_AP_Expr(expr, ap_expr);
        OOSQL_CHECK_ERR(e);

        e = replacePathExprForGroupbyKey(ap_expr, apNode.startIndex);
        OOSQL_CHECK_ERR(e);

        // fill having condition
		ap_condList = apNode[0].havCondNodes = m_pool->ap_condListPool.addNewEntry();
		ap_condList[0].expr = ap_expr.getPoolIndex();
    }
    
    return eNOERROR;
}

Four OQL_GDStoCommonAP::replacePathExprForGroupbyKey(AP_ExprPoolElements& ap_expr, Four groupby_planNo)
{
    Four e;

    e = traverseAP_ExprTreeReplacePathExpr(ap_expr.getPoolIndex(), groupby_planNo);
    OOSQL_CHECK_ERR(e);

    return eNOERROR;
}

Four OQL_GDStoCommonAP::traverseAP_ExprTreeReplacePathExpr(AP_ExprPoolIndex ap_exprPoolIndex, Four groupby_planNo)
{
    AP_ExprPoolElements     ap_expr(m_pool->ap_exprPool);
    Four                    e;
    Four                i,  index;
    Four                    planNo;
    Two                     colNo;
    GroupByListPoolElements groupByList(m_pool->groupByListPool);
    PathExprPoolElements    pathExpr(m_pool->pathExprPool);
    Four                    lastElemIndex;
    
    ap_expr = ap_exprPoolIndex;
    
    switch(ap_expr[0].exprKind)
    {
      case EXPR_KIND_PATHEXPR:
        
        if(ap_expr[0].pathExpr.kind == PATHEXPR_KIND_ATTR)
        {
            // check (ap_expr[0].pathExpr.col.planNo, ap_expr[0].pathExpr.col.colNo) is equal to groupby key
			e = m_gds->getGroupByList(m_pool, groupByList); OOSQL_CHECK_ERR(e);
            for(i = 0, index = 0; i < groupByList.size; i++)
            {
                if(groupByList[i].grpByKeyKind == GROUPBY_KIND_PATHEXPR)
                {
                    pathExpr      = groupByList[i].pathExpr;
                    lastElemIndex = pathExpr.size - 1;
                    if(pathExpr[lastElemIndex].fromAttrKind  == PATHEXPR_KIND_ATTR)
                    {
                        planNo = pathExpr[lastElemIndex].planNo.startIndex;
                        
                        if(ap_expr[0].pathExpr.col.planNo == planNo)
                        {
                            e = m_catalog->attr_AttrInfo_to_ColNo(pathExpr[lastElemIndex].classInfo,
                                                                pathExpr[lastElemIndex].attr.attrInfo,
                                                                colNo);
                            OOSQL_CHECK_ERR(e);
                            
                            if(ap_expr[0].pathExpr.col.colNo == colNo)
                            {
                                // matched
                                // replace pathexpr for groupby key
                                ap_expr[0].pathExpr.col.planNo = groupby_planNo;
                                ap_expr[0].pathExpr.col.colNo  = i;
                            }
                        }
                        
                        index ++;
                    }
                    else
                        OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
                }
            }
        }

        break;
        
      case EXPR_KIND_OPER:
        e = traverseAP_ExprTreeReplacePathExpr(ap_expr[0].oper.operand1, groupby_planNo);
        OOSQL_CHECK_ERR(e);
        
        if(ap_expr[0].oper.operand2 != NULL_POOLINDEX)  // check for unary operator
        {
            e = traverseAP_ExprTreeReplacePathExpr(ap_expr[0].oper.operand2, groupby_planNo);
            OOSQL_CHECK_ERR(e);
        }
        
        break;
        
      default:
        break;
    }
    
    return eNOERROR;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
error code

IMPLEMENTATION:
****************************************************************************/
Four OQL_GDStoCommonAP::makePlan_constructOrderBy()
{
    CommonAP_PoolElements           apNode(m_pool->commonAP_Pool);
    CommonAP_PoolElements           apNode_temp(m_pool->commonAP_Pool);
    SelListPoolElements             selList;
    OrderByListPoolElements         orderByList;
	TargetListPoolElements			targetList;
    PathExprPoolElements            pathExpr(m_pool->pathExprPool);
    AP_ProjectionListPoolElements   projectionList(m_pool->projectionListPool);
    AP_ProjectionPoolElements       projectionInfo(m_pool->projectionPool);
    AP_ColNoMapPoolElements         colNoMap(m_pool->colNoMapPool);
    AP_UsedColPoolElements          usedColInfo(m_pool->usedColPool);
    AggrFuncPoolElements            aggrFunc(m_pool->aggrFuncPool);
    ArgumentPoolElements            argument(m_pool->argumentPool);
    AP_TempFileInfoPoolElements     tempFileInfo(m_pool->tempFileInfoPool);
	FunctionPoolElements            func(m_pool->funcPool);
    Four                            tempFileNum;
    Four                            tempFileNumForOrderBy;
    Four                            length;
    Four                            i;
    Four                            index;
    Four                            nOrdByKeys;
    Four                            e;

	e = m_gds->getSelList(m_pool, selList);			OOSQL_CHECK_ERR(e);
	e = m_gds->getOrderByList(m_pool, orderByList);	OOSQL_CHECK_ERR(e);
	e = m_gds->getTargetList(m_pool, targetList);   OOSQL_CHECK_ERR(e);

	// -------------------
    // for last AP element
    // -------------------
    apNode.setPoolIndex(m_commonAP->commonAP.size - 1, 1);

    // count projectionList length and allocate 
    // length = 1 (why one, because aggrfunc will be processed in group by)
    //            (select avg(distinct age), name order by name <- error, so we ignore this case)
    //            (select avg(distince age), name group by name order by avg(distinct age) <- correct)
    projectionList = m_pool->projectionListPool.addNewEntry();
    projectionList[0].init();
    apNode[0].projectionList = projectionList.getPoolIndex();

    // count tempFileInfo length and allocate
    // length = order by pathexpr or aggrfunc elements + selList PathExpr or aggrfunc
    length = 0;
    for(i = 0; i < orderByList.size; i++)
    {
        if(orderByList[i].ordByKeyKind == ORDERBYLIST_KIND_PATHEXPR ||
           orderByList[i].ordByKeyKind == ORDERBYLIST_KIND_AGGRFUNC ||
		   orderByList[i].ordByKeyKind == ORDERBYLIST_KIND_FUNC)
            length ++;
    }
    for(i = 0; i< selList.size; i++)
    {
        if(selList[i].selElemKind == SELLIST_KIND_PATHEXPR ||
           selList[i].selElemKind == SELLIST_KIND_AGGRFUNC ||
		   selList[i].selElemKind == SELLIST_KIND_FUNC)
            length ++;
    }
    tempFileInfo = m_pool->tempFileInfoPool.addNewEntry(length);
    tempFileNumForOrderBy = tempFileNum = getNewTempFileNum();  OOSQL_CHECK_ERR(tempFileNum);
    e = setTempFileInfoPoolIndex(tempFileNum, tempFileInfo.getPoolIndex()); OOSQL_CHECK_ERR(e);
    e = setTempFileUsage(tempFileNum, TEMPFILE_FOR_ORDERBY, 0);             OOSQL_CHECK_ERR(e);
    projectionList[0].tempFileNum = tempFileNum;

    // count projectionList length for order by - this value is the same with tempFileInfo length for order by
    // allocate
    projectionInfo = m_pool->projectionPool.addNewEntry(length);
    projectionList[0].projectionInfo = projectionInfo.getPoolIndex();

    // make projection info and tempFileInfo
    for(i = 0, index = 0, nOrdByKeys = 0; i < orderByList.size; i++)
    {
        switch(orderByList[i].ordByKeyKind)
        {
        case ORDERBYLIST_KIND_PATHEXPR:
        case ORDERBYLIST_KIND_AGGRFUNC:
            // fill projection info
            switch(orderByList[i].ordByKeyKind)
            {
            case ORDERBYLIST_KIND_PATHEXPR:
                // if current apnode is tempclass
                if(apNode[0].classInfo.classKind == CLASSKIND_TEMPORARY || apNode[0].classInfo.classKind == CLASSKIND_SORTSTREAM)
                {
                    e = setProjection(projectionInfo[index], orderByList[i]);
                    OOSQL_CHECK_ERR(e);

                    // patch projection to access temp. file
					if(projectionInfo[index].resultType == TYPEID_OID)
					{
						int targetIndex;

						apNode_temp.setPoolIndex(projectionInfo[index].pathExpr.col.planNo, 1);
						if(apNode_temp[0].classInfo.classKind == CLASSKIND_PERSISTENT)
						{
							for(targetIndex = 0; targetIndex < targetList.size; targetIndex++)
							{
								Four classId;

								e = m_catalog->class_ClassInfo_to_ClassId(targetList[targetIndex].collectionInfo.classInfo, classId);
								OOSQL_CHECK_ERR(e);

								if(apNode_temp[0].classInfo.classId == classId)
									break;
							}
							if(targetIndex == targetList.size)
								OOSQL_ERR(eINTERNALERROR_OOSQL);
						}
						else
							OOSQL_ERR(eINTERNALERROR_OOSQL);

						projectionInfo[index].pathExpr.kind       = PATHEXPR_KIND_ATTR;
						projectionInfo[index].pathExpr.col.planNo = apNode.startIndex;
						projectionInfo[index].pathExpr.col.colNo  = apNode[0].projectionList.getElements(m_pool->projectionListPool, 0).projectionInfo.size - targetList.size + targetIndex;
					}
					else
					{
						projectionInfo[index].pathExpr.col.planNo = apNode.startIndex;
						pathExpr = orderByList[i].pathExpr;
						e = getTempFileColNo(apNode[0].classInfo.tempFileNum, pathExpr, 
							                 projectionInfo[index].pathExpr.col.colNo);
						OOSQL_CHECK_ERR(e);
					}

					if(projectionInfo[index].resultType == TYPEID_TEXT)
					{
						projectionInfo[index].resultType   = TYPEID_VARSTRING;
#ifndef SUPPORT_LARGE_DATABASE2
						projectionInfo[index].resultLength = INT_MAX;
#else
						projectionInfo[index].resultLength = LONG_MAX;
#endif
					}
                }
                else
                {
                    e = setProjection(projectionInfo[index], orderByList[i]);
                    OOSQL_CHECK_ERR(e);
                }
                break;

            case ORDERBYLIST_KIND_AGGRFUNC:
                aggrFunc = orderByList[i].aggrFunc;
                argument = aggrFunc[0].argument;

				if(m_aggrFuncInfo_planNo != NULL_POOLINDEX)
				{
					// setProjection uses aggrFuncInfo
					e = setProjection(projectionInfo[index], orderByList[i]);
					OOSQL_CHECK_ERR(e);
				}
				else
				{
					e = setProjection(projectionInfo[index], argument[0]);
					OOSQL_CHECK_ERR(e);
				}
				break;
            }

            // set sort information
            projectionList[0].sortKeys[index]    = index;
            projectionList[0].sortAscDesc[index] = (orderByList[i].asc_or_desc_Flag)?SORTORDER_ASC:SORTORDER_DESC;
            projectionList[0].nSortKeys ++;

            // set tempFileInfo
            e = makePlan_constructTempFileInfo(tempFileInfo[index], projectionInfo[index], orderByList[i]);
            OOSQL_CHECK_ERR(e);

            index ++;
            nOrdByKeys ++;
            break;
        
        case ORDERBYLIST_KIND_FUNC:
			func = orderByList[i].func;
			if(func[0].functionID == FUNCTION_TEXTIR_WEIGHT)
			{
				projectionInfo[index].projectionKind = PROJECTION_KIND_FUNCRESULT;

				e = convertFunc_to_FuncResultAccessInfo(orderByList[i].func, projectionInfo[index].funcResult);
				OOSQL_CHECK_ERR(e);
			}
			else
			{
				projectionInfo[index].projectionKind = PROJECTION_KIND_FUNCEVAL;

				e = convertFunc_to_FuncEvalInfo(orderByList[i].func, projectionInfo[index].funcEval);
				OOSQL_CHECK_ERR(e);
			}

			projectionInfo[index].resultType   = func[0].returnType;
			projectionInfo[index].resultLength = func[0].returnLength;

            // set sort information
            projectionList[0].sortKeys[index]    = index;
            projectionList[0].sortAscDesc[index] = (orderByList[i].asc_or_desc_Flag)?SORTORDER_ASC:SORTORDER_DESC;
            projectionList[0].nSortKeys ++;

            // set tempFileInfo
            e = makePlan_constructTempFileInfo(tempFileInfo[index], projectionInfo[index], orderByList[i]);
            OOSQL_CHECK_ERR(e);

            index ++;
            nOrdByKeys ++;
            break;

        default:
            OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
            break;
        }
    }
    
    // for selList
    for(i = 0; i < selList.size; i++)
    {
        switch(selList[i].selElemKind)
        {
        case SELLIST_KIND_PATHEXPR:
            // fill projection info
            // if current apnode is tempclass
            if(apNode[0].classInfo.classKind == CLASSKIND_TEMPORARY || apNode[0].classInfo.classKind == CLASSKIND_SORTSTREAM)
            {
                e = setProjection(projectionInfo[index], selList[i]);
                OOSQL_CHECK_ERR(e);

                // patch projection to access temp. file
				if(projectionInfo[index].resultType == TYPEID_OID)
				{
					int targetIndex;

					apNode_temp.setPoolIndex(projectionInfo[index].pathExpr.col.planNo, 1);
					if(apNode_temp[0].classInfo.classKind == CLASSKIND_PERSISTENT)
					{
						for(targetIndex = 0; targetIndex < targetList.size; targetIndex++)
						{
							Four classId;

							e = m_catalog->class_ClassInfo_to_ClassId(targetList[targetIndex].collectionInfo.classInfo, classId);
							OOSQL_CHECK_ERR(e);

							if(apNode_temp[0].classInfo.classId == classId)
								break;
						}
						if(targetIndex == targetList.size)
							OOSQL_ERR(eINTERNALERROR_OOSQL);
					}
					else
						OOSQL_ERR(eINTERNALERROR_OOSQL);

					projectionInfo[index].pathExpr.kind       = PATHEXPR_KIND_ATTR;
					projectionInfo[index].pathExpr.col.planNo = apNode.startIndex;
					projectionInfo[index].pathExpr.col.colNo  = apNode[0].projectionList.getElements(m_pool->projectionListPool, 0).projectionInfo.size - targetList.size + targetIndex;
				}
				else
				{
					projectionInfo[index].pathExpr.col.planNo = apNode.startIndex;
					pathExpr = selList[i].pathExpr;
					e = getTempFileColNo(apNode[0].classInfo.tempFileNum, pathExpr, 
										 projectionInfo[index].pathExpr.col.colNo);
					OOSQL_CHECK_ERR(e);
				}

				if(projectionInfo[index].resultType == TYPEID_TEXT)
				{
					projectionInfo[index].resultType = TYPEID_VARSTRING;
#ifndef SUPPORT_LARGE_DATABASE2
					projectionInfo[index].resultLength = INT_MAX;
#else
					projectionInfo[index].resultLength = LONG_MAX;
#endif
				}
            }
            else
            {
                e = setProjection(projectionInfo[index], selList[i]);
                OOSQL_CHECK_ERR(e);
            }

            // set tempFileInfo
            e = makePlan_constructTempFileInfo(tempFileInfo[index], projectionInfo[index], selList[i]);
            OOSQL_CHECK_ERR(e);

            index ++;
            break;

        case SELLIST_KIND_AGGRFUNC:
            if(m_aggrFuncInfo_planNo == NULL_POOLINDEX)
                OOSQL_ERR(eINTERNALERROR_OOSQL);

            // fill projection info
            e = setProjection(projectionInfo[index], selList[i]);
            OOSQL_CHECK_ERR(e);

            // set tempFileInfo
            argument = aggrFunc[0].argument;
            e = makePlan_constructTempFileInfo(tempFileInfo[index], projectionInfo[index], argument[0]);
            OOSQL_CHECK_ERR(e);

            index ++;
            break;
        case SELLIST_KIND_FUNC:
            // fill projection info
            e = setProjection(projectionInfo[index], selList[i]);
            OOSQL_CHECK_ERR(e);

            // set tempFileInfo
            e = makePlan_constructTempFileInfo(tempFileInfo[index], projectionInfo[index], selList[i]);
            OOSQL_CHECK_ERR(e);

            index ++;
            break;
        case SELLIST_KIND_VALUE: 
        case SELLIST_KIND_OPER:
        case SELLIST_KIND_CONS:
            break;
        default:
            OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
            break;
        }
    }

    // -------------------
    // make new AP element
    // -------------------

    apNode = m_pool->commonAP_Pool.addNewEntry();
    apNode[0].init();

    if(m_commonAP->commonAP != NULL_POOLINDEX)
        m_commonAP->commonAP.size ++;
    else
        m_commonAP->commonAP = apNode.getPoolIndex();

    // set accessMethod and joinMethod
    apNode[0].accessMethod = CAP_ACCESSMETHOD_SEQSCAN;
    apNode[0].joinMethod   = CAP_JOINMETHOD_OUTERMOST_CLASS;

    // set classInfo
    apNode[0].classInfo.classKind   = CLASSKIND_SORTSTREAM;
    apNode[0].classInfo.tempFileNum = tempFileNumForOrderBy;

    // set usedColInfo
    colNoMap    = m_pool->colNoMapPool.addNewEntry(tempFileInfo.size);
    usedColInfo = m_pool->usedColPool.addNewEntry(tempFileInfo.size);
    apNode[0].colNoMap    = colNoMap.getPoolIndex();
    apNode[0].usedColInfo = usedColInfo.getPoolIndex();
    for(i = 0;i < tempFileInfo.size; i++)
    {
        colNoMap[i].offset = i;
        usedColInfo[i].typeId = tempFileInfo[i].typeId;
        usedColInfo[i].length = tempFileInfo[i].length;
		usedColInfo[i].colNo  = i;
    }

    // set m_aggrFuncInfo_planNo
    if(m_aggrFuncInfo_planNo == NULL_POOLINDEX)
    {
        m_aggrFuncInfo_planNo = apNode.getPoolIndex();

        // make aggrFuncInfo
        e = makePlan_constructAggrFuncInfo(apNode);
        OOSQL_CHECK_ERR(e);
    }
    
    return eNOERROR;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
error code

IMPLEMENTATION:
****************************************************************************/
Four OQL_GDStoCommonAP::makePlan_constructSelect()
{
    CommonAP_PoolElements           apNode(m_pool->commonAP_Pool);
    CommonAP_PoolElements           apNode_temp(m_pool->commonAP_Pool);
    AP_ProjectionListPoolElements   projectionList(m_pool->projectionListPool);
    AP_ProjectionPoolElements       projectionInfo(m_pool->projectionPool);
    AP_ProjectionPoolElements       projectionInfoForAggrFunc(m_pool->projectionPool);
    SelListPoolElements             selList;
	OrderByListPoolElements			orderByList;
	TargetListPoolElements			targetList;
	LimitClausePoolElements			limitClause;
    ArgumentPoolElements            argument(m_pool->argumentPool);
    AP_ArgumentPoolElements         ap_argument(m_pool->ap_argumentPool);
    FunctionPoolElements            func(m_pool->funcPool);
    PathExprPoolElements            pathExpr(m_pool->pathExprPool);
    AggrFuncPoolElements            aggrFunc(m_pool->aggrFuncPool);
    AP_TempFileInfoPoolElements     tempFileInfo(m_pool->tempFileInfoPool);
    AP_TempFileInfoPoolElements     tempFileInfoForAggrFunc(m_pool->tempFileInfoPool);
    AP_ColNoMapPoolElements         colNoMap(m_pool->colNoMapPool);
    AP_UsedColPoolElements          usedColInfo(m_pool->usedColPool);
    Four                            i, j;
    Four                            aggrFuncIndex;
    Four                            index;
    Four                            length;
    Four                            tempFileNum;
    Four                            e;
	Four							selListType;
	
	e = m_gds->getSelList(m_pool, selList);			OOSQL_CHECK_ERR(e);
	e = m_gds->getSelListType(m_pool, selListType);	OOSQL_CHECK_ERR(e);
	e = m_gds->getOrderByList(m_pool, orderByList); OOSQL_CHECK_ERR(e);
	e = m_gds->getTargetList(m_pool, targetList);   OOSQL_CHECK_ERR(e);
	e = m_gds->getLimitClause(m_pool, limitClause); OOSQL_CHECK_ERR(e);

	if(selList == NULL_POOLINDEX)
		return eNOERROR;

    // -------------------
    // for last AP element
    // -------------------
    apNode.setPoolIndex(m_commonAP->commonAP.size - 1, 1);

    if(m_aggrFuncInfo_planNo == NULL_POOLINDEX)
    {   
        // aggrFuncInfo has not been constructed. therefore, there is no tempfiles 
        // for aggr function with DISTINCT. so make it.
        // count projectionList length and allocate
        for(i = 0, length = 0; i < selList.size; i++)
        {
            if(selList[i].selElemKind == SELLIST_KIND_AGGRFUNC)
            {
                aggrFunc = selList[i].aggrFunc;
                if(aggrFunc[0].distinctFlag == SM_TRUE)    // aggr function with DISTINCT
					length++;
            }
        }

        if(length != 0) // there is an aggrFunc with DISTINCT, process it
        {
            projectionList = m_pool->projectionListPool.addNewEntry(length);

            // initialize each projection info
            for(i = 0; i < length; i++) projectionList[i].init();

            // connect projection with access plan
            apNode[0].projectionList = projectionList.getPoolIndex();

            // fill projectionList for aggrfunc with DISTINCT flag
            for(i = 0, index = 0; i < selList.size; i++)
            {
                if(selList[i].selElemKind == SELLIST_KIND_AGGRFUNC)
                {
                    aggrFunc = selList[i].aggrFunc;
                    argument = aggrFunc[0].argument;
                    if(aggrFunc[0].distinctFlag == SM_TRUE)    // aggr function with DISTINCT
                    {
                        // alloc tempFileInfoForAggrFunc
                        tempFileInfoForAggrFunc = m_pool->tempFileInfoPool.addNewEntry(argument.size);
                        tempFileNum = getNewTempFileNum();  OOSQL_CHECK_ERR(tempFileNum);
                        e = setTempFileInfoPoolIndex(tempFileNum, tempFileInfoForAggrFunc.getPoolIndex());
                        OOSQL_CHECK_ERR(e);
                        e = setTempFileUsage(tempFileNum, TEMPFILE_FOR_AGGR_DISTINCT, i);
                        OOSQL_CHECK_ERR(e);
                        projectionList[index].tempFileNum = tempFileNum;

                        // alloc projectionInfoForAggrFunc
                        projectionInfoForAggrFunc = m_pool->projectionPool.addNewEntry(argument.size);
                        projectionList[index].projectionInfo = projectionInfoForAggrFunc.getPoolIndex();

                        // fill tempFileInfoForAggrFunc and projectionInfoForAggrFunc
                        for(j = 0; j < argument.size; j++)
                        {
                            e = setProjection(projectionInfoForAggrFunc[j], argument[j]);
                            OOSQL_CHECK_ERR(e);

                            e = makePlan_constructTempFileInfo(tempFileInfoForAggrFunc[j], projectionInfoForAggrFunc[j], argument[j]);
                            OOSQL_CHECK_ERR(e);
                        }
                        
                        // fill sort information
                        for(j = 0; j < projectionInfoForAggrFunc.size; j++)
                        {
                            projectionList[index].sortKeys[j]    = j;
                            projectionList[index].sortAscDesc[j] = SORTORDER_ASC;
                        }
                        projectionList[index].nSortKeys = projectionInfoForAggrFunc.size;

                        index ++;
                    }

                }
            }
            
            // -------------------
            // make new AP element
            // -------------------
            apNode = m_pool->commonAP_Pool.addNewEntry();
            apNode[0].init();

            if(m_commonAP->commonAP != NULL_POOLINDEX)
                m_commonAP->commonAP.size ++;
            else
                m_commonAP->commonAP = apNode.getPoolIndex();
            
            // set classInfo
            apNode[0].classInfo.classKind = CLASSKIND_NULL_AGGRFUNC_ONLY;
        }
    }

    // set m_aggrFuncInfo_planNo
    if(m_aggrFuncInfo_planNo == NULL_POOLINDEX)
    {
        m_aggrFuncInfo_planNo = apNode.getPoolIndex();

        // construct aggrfunc information
        //
        // special case : 
        //  select avg(age), avg(DISTINCT age)
        //  from   Employee
        //
        if(apNode[0].classInfo.classKind == CLASSKIND_NULL_AGGRFUNC_ONLY)
            e = makePlan_coustructAggrFuncInfoExceptionalCase(apNode);
        else
            e = makePlan_constructAggrFuncInfo(apNode);
        OOSQL_CHECK_ERR(e);
    }

    // -------------------
    // process DISTINCT
    // -------------------
	if(selListType & DIST_BIT)     // select DISTINCT
    {
        // make projectionList for tempFile
        // count length of tempFileInfo
		if(orderByList != NULL_POOLINDEX)
		{
			for(i = 0, length = 0; i < selList.size - targetList.size; i++)
				if(selList[i].selElemKind == SELLIST_KIND_PATHEXPR || 
				   selList[i].selElemKind == SELLIST_KIND_AGGRFUNC) 
					length ++;
			for(i = 0; i < orderByList.size; i++)
				if(orderByList[i].ordByKeyKind == ORDERBYLIST_KIND_PATHEXPR || 
				   orderByList[i].ordByKeyKind == ORDERBYLIST_KIND_PATHEXPR)
			 		length ++;
			length += targetList.size;
		}
		else
		{
			for(i = 0, length = 0; i < selList.size; i++)
				if(selList[i].selElemKind == SELLIST_KIND_PATHEXPR || 
				   selList[i].selElemKind == SELLIST_KIND_AGGRFUNC)
					length ++;
		}

        tempFileInfo = m_pool->tempFileInfoPool.addNewEntry(length);
        tempFileNum  = getNewTempFileNum(); OOSQL_CHECK_ERR(tempFileNum);
        e = setTempFileInfoPoolIndex(tempFileNum, tempFileInfo.getPoolIndex()); OOSQL_CHECK_ERR(e);
        e = setTempFileUsage(tempFileNum, TEMPFILE_FOR_GROUPBY, 0);             OOSQL_CHECK_ERR(e);

		// make projectionList
		projectionList = m_pool->projectionListPool.addNewEntry();
        projectionList[0].init();
        apNode[0].projectionList = projectionList.getPoolIndex();

		// attach tempFileNum into projectionList
        projectionList[0].tempFileNum = tempFileNum;

        // construct length of projectionList
        projectionInfo = m_pool->projectionPool.addNewEntry(length);
        projectionList[0].projectionInfo = projectionInfo.getPoolIndex();

        for(i = 0, index = 0; i < selList.size - targetList.size; i++)
        {
            if(selList[i].selElemKind == SELLIST_KIND_PATHEXPR)
            {
                // fill projection info
                projectionInfo[index].projectionKind = PROJECTION_KIND_PATHEXPR;
                if(apNode[0].classInfo.classKind == CLASSKIND_TEMPORARY || apNode[0].classInfo.classKind == CLASSKIND_SORTSTREAM)
                {
                    e = setProjection(projectionInfo[index], selList[i]);
                    OOSQL_CHECK_ERR(e);

					switch(projectionInfo[index].pathExpr.kind)
					{
					case PATHEXPR_KIND_ATTR:
						projectionInfo[index].pathExpr.col.planNo = apNode.startIndex;
						pathExpr = selList[i].pathExpr;
						e = getTempFileColNo(apNode[0].classInfo.tempFileNum, pathExpr, 
											 projectionInfo[index].pathExpr.col.colNo);
						OOSQL_CHECK_ERR(e);

						if(projectionInfo[index].resultType == TYPEID_TEXT)
						{
							projectionInfo[index].resultType = TYPEID_VARSTRING;
#ifndef SUPPORT_LARGE_DATABASE2
							projectionInfo[index].resultLength = INT_MAX;
#else
							projectionInfo[index].resultLength = LONG_MAX;
#endif
						}

						break;

					case PATHEXPR_KIND_OBJECT:
					case PATHEXPR_KIND_CLASS:
					case PATHEXPR_KIND_OID:
						projectionInfo[index].pathExpr.kind = PATHEXPR_KIND_ATTR;
						projectionInfo[index].pathExpr.col.planNo = apNode.startIndex;
						pathExpr = selList[i].pathExpr;
						e = getTempFileColNo(apNode[0].classInfo.tempFileNum, pathExpr,
											 projectionInfo[index].pathExpr.col.colNo);
						break;
					default:
						OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
					}
                }
                else
                {
                    e = setProjection(projectionInfo[index], selList[i]);
                    OOSQL_CHECK_ERR(e);
                }

                // set sort information
                projectionList[0].sortKeys[index]    = index;
                projectionList[0].sortAscDesc[index] = SORTORDER_ASC; 
                projectionList[0].nSortKeys ++;

                // set tempFileInfo
                e = makePlan_constructTempFileInfo(tempFileInfo[index], projectionInfo[index], selList[i]);
                OOSQL_CHECK_ERR(e);

                index ++;
            }
			else if(selList[i].selElemKind == SELLIST_KIND_AGGRFUNC)
			{
				aggrFunc = selList[i].aggrFunc;
				argument = aggrFunc[0].argument;
				
				e = setProjection(projectionInfo[index], argument[0]);
				OOSQL_CHECK_ERR(e);

				// set tempFileInfo
				e = makePlan_constructTempFileInfo(tempFileInfo[index], projectionInfo[index], selList[i]);
				OOSQL_CHECK_ERR(e);
				
				index ++;
				break;
			}
        }

		for(i = 0; i < orderByList.size; i++)
		{
			switch(orderByList[i].ordByKeyKind)
			{
			case ORDERBYLIST_KIND_PATHEXPR:
			case ORDERBYLIST_KIND_AGGRFUNC:
				// fill projection info
				switch(orderByList[i].ordByKeyKind)
				{
				case ORDERBYLIST_KIND_PATHEXPR:
					// if current apnode is tempclass
					if(apNode[0].classInfo.classKind == CLASSKIND_TEMPORARY || apNode[0].classInfo.classKind == CLASSKIND_SORTSTREAM)
					{
						e = setProjection(projectionInfo[index], orderByList[i]);
						OOSQL_CHECK_ERR(e);

						// patch projection to access temp. file
						if(projectionInfo[index].resultType == TYPEID_OID)
						{
							int targetIndex;

							apNode_temp.setPoolIndex(projectionInfo[index].pathExpr.col.planNo, 1);
							if(apNode_temp[0].classInfo.classKind == CLASSKIND_PERSISTENT)
							{
								for(targetIndex = 0; targetIndex < targetList.size; targetIndex++)
								{
									Four classId;

									e = m_catalog->class_ClassInfo_to_ClassId(targetList[targetIndex].collectionInfo.classInfo, classId);
									OOSQL_CHECK_ERR(e);

									if(apNode_temp[0].classInfo.classId == classId)
										break;
								}
								if(targetIndex == targetList.size)
									OOSQL_ERR(eINTERNALERROR_OOSQL);
							}
							else
								OOSQL_ERR(eINTERNALERROR_OOSQL);

							projectionInfo[index].pathExpr.kind       = PATHEXPR_KIND_ATTR;
							projectionInfo[index].pathExpr.col.planNo = apNode.startIndex;
							projectionInfo[index].pathExpr.col.colNo  = apNode[0].projectionList.getElements(m_pool->projectionListPool, 0).projectionInfo.size - targetList.size + targetIndex;
						}
						else
						{
							projectionInfo[index].pathExpr.col.planNo = apNode.startIndex;
							pathExpr = orderByList[i].pathExpr;
							e = getTempFileColNo(apNode[0].classInfo.tempFileNum, pathExpr, 
												 projectionInfo[index].pathExpr.col.colNo);
							OOSQL_CHECK_ERR(e);
						}

						if(projectionInfo[index].resultType == TYPEID_TEXT)
						{
							projectionInfo[index].resultType   = TYPEID_VARSTRING;
#ifndef SUPPORT_LARGE_DATABASE2
							projectionInfo[index].resultLength = INT_MAX;
#else
							projectionInfo[index].resultLength = LONG_MAX;
#endif
						}
					}
					else
					{
						e = setProjection(projectionInfo[index], orderByList[i]);
						OOSQL_CHECK_ERR(e);
					}
					break;

				case ORDERBYLIST_KIND_AGGRFUNC:
					aggrFunc = orderByList[i].aggrFunc;
					argument = aggrFunc[0].argument;

					// if aggrFuncInfo already has been made, use it
					if(m_aggrFuncInfo_planNo != NULL_POOLINDEX)
					{
						// setProjection uses aggrFuncInfo
						e = setProjection(projectionInfo[index], orderByList[i]);
						OOSQL_CHECK_ERR(e);
					}
					else
					{
						// if current apnode is tempclass
						if(apNode[0].classInfo.classKind == CLASSKIND_TEMPORARY || apNode[0].classInfo.classKind == CLASSKIND_SORTSTREAM)
						{
							e = setProjection(projectionInfo[index], argument[0]);
							OOSQL_CHECK_ERR(e);

							// patch projectionInfo to access temp. file
							if(projectionInfo[index].resultType == TYPEID_OID)
							{
								int targetIndex;

								apNode_temp.setPoolIndex(projectionInfo[index].pathExpr.col.planNo, 1);
								if(apNode_temp[0].classInfo.classKind == CLASSKIND_PERSISTENT)
								{
									for(targetIndex = 0; targetIndex < targetList.size; targetIndex++)
									{
										Four classId;

										e = m_catalog->class_ClassInfo_to_ClassId(targetList[targetIndex].collectionInfo.classInfo, classId);
										OOSQL_CHECK_ERR(e);

										if(apNode_temp[0].classInfo.classId == classId)
											break;
									}
									if(targetIndex == targetList.size)
										OOSQL_ERR(eINTERNALERROR_OOSQL);
								}
								else
									OOSQL_ERR(eINTERNALERROR_OOSQL);

								projectionInfo[index].pathExpr.kind       = PATHEXPR_KIND_ATTR;
								projectionInfo[index].pathExpr.col.planNo = apNode.startIndex;
								projectionInfo[index].pathExpr.col.colNo  = apNode[0].projectionList.getElements(m_pool->projectionListPool, 0).projectionInfo.size - targetList.size + targetIndex;
							}
							else
							{
								projectionInfo[index].pathExpr.col.planNo = apNode.startIndex;
								pathExpr = orderByList[i].pathExpr;
								e = getTempFileColNo(apNode[0].classInfo.tempFileNum, pathExpr, 
													 projectionInfo[index].pathExpr.col.colNo);
								OOSQL_CHECK_ERR(e);
							}

							if(projectionInfo[index].resultType == TYPEID_TEXT)
							{
								projectionInfo[index].resultType = TYPEID_VARSTRING;
#ifndef SUPPORT_LARGE_DATABASE2
								projectionInfo[index].resultLength = INT_MAX;
#else
								projectionInfo[index].resultLength = LONG_MAX;
#endif
							}
						}
						else
						{
							e = setProjection(projectionInfo[index], argument[0]);
							OOSQL_CHECK_ERR(e);
						}
						break;
					}
				}

				// set tempFileInfo
				e = makePlan_constructTempFileInfo(tempFileInfo[index], projectionInfo[index], orderByList[i]);
				OOSQL_CHECK_ERR(e);

				index ++;
				break;
        
			case ORDERBYLIST_KIND_FUNC:
				OOSQL_ERR(eNOTIMPLEMENTED_OOSQL);
				break;
			default:
				OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
				break;
			}
		}
        for(i = selList.size - targetList.size; i < selList.size; i++)
        {
            if(selList[i].selElemKind == SELLIST_KIND_PATHEXPR)
            {
                // fill projection info
                projectionInfo[index].projectionKind = PROJECTION_KIND_PATHEXPR;
                if(apNode[0].classInfo.classKind == CLASSKIND_TEMPORARY || apNode[0].classInfo.classKind == CLASSKIND_SORTSTREAM)
                {
                    e = setProjection(projectionInfo[index], selList[i]);
                    OOSQL_CHECK_ERR(e);

					switch(projectionInfo[index].pathExpr.kind)
					{
					case PATHEXPR_KIND_ATTR:
						projectionInfo[index].pathExpr.col.planNo = apNode.startIndex;
						pathExpr = selList[i].pathExpr;
						e = getTempFileColNo(apNode[0].classInfo.tempFileNum, pathExpr, 
											 projectionInfo[index].pathExpr.col.colNo);
						OOSQL_CHECK_ERR(e);

						if(projectionInfo[index].resultType == TYPEID_TEXT)
						{
							projectionInfo[index].resultType = TYPEID_VARSTRING;
#ifndef SUPPORT_LARGE_DATABASE2
							projectionInfo[index].resultLength = INT_MAX;
#else
							projectionInfo[index].resultLength = LONG_MAX;
#endif
						}

						break;

					case PATHEXPR_KIND_OBJECT:
					case PATHEXPR_KIND_CLASS:
					case PATHEXPR_KIND_OID:
						projectionInfo[index].pathExpr.kind = PATHEXPR_KIND_ATTR;
						projectionInfo[index].pathExpr.col.planNo = apNode.startIndex;
						pathExpr = selList[i].pathExpr;
						e = getTempFileColNo(apNode[0].classInfo.tempFileNum, pathExpr,
											 projectionInfo[index].pathExpr.col.colNo);
						break;
					default:
						OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
					}
                }
                else
                {
                    e = setProjection(projectionInfo[index], selList[i]);
                    OOSQL_CHECK_ERR(e);
                }

                // set tempFileInfo
                e = makePlan_constructTempFileInfo(tempFileInfo[index], projectionInfo[index], selList[i]);
                OOSQL_CHECK_ERR(e);

                index ++;
            }           
			else if(selList[i].selElemKind == SELLIST_KIND_AGGRFUNC)
			{
				aggrFunc = selList[i].aggrFunc;
				argument = aggrFunc[0].argument;

				e = setProjection(projectionInfo[index], argument[0]);                                                          
				OOSQL_CHECK_ERR(e);
								
				// set tempFileInfo
				e = makePlan_constructTempFileInfo(tempFileInfo[index], projectionInfo[index], selList[i]);
				OOSQL_CHECK_ERR(e);
				
				index ++;
				break;
			}
        }

        // -------------------
        // make new AP element
        // -------------------
        apNode = m_pool->commonAP_Pool.addNewEntry();
        apNode[0].init();

        if(m_commonAP->commonAP != NULL_POOLINDEX)
            m_commonAP->commonAP.size ++;
        else
            m_commonAP->commonAP = apNode.getPoolIndex();
        
        // set accessMethod and joinMethod
        apNode[0].accessMethod = CAP_ACCESSMETHOD_SEQSCAN;
        apNode[0].joinMethod   = CAP_JOINMETHOD_OUTERMOST_CLASS;

        // set classInfo
        apNode[0].classInfo.classKind   = CLASSKIND_SORTSTREAM;
        apNode[0].classInfo.tempFileNum = tempFileNum;

        // set usedColInfo
        colNoMap    = m_pool->colNoMapPool.addNewEntry(tempFileInfo.size);
        usedColInfo = m_pool->usedColPool.addNewEntry(tempFileInfo.size);
        for(i = 0;i < tempFileInfo.size; i++)
        {
            colNoMap[i].offset    = i;
            usedColInfo[i].typeId = tempFileInfo[i].typeId;
            usedColInfo[i].length = tempFileInfo[i].length;
			usedColInfo[i].colNo  = i;
        }
        apNode[0].colNoMap    = colNoMap.getPoolIndex();
        apNode[0].usedColInfo = usedColInfo.getPoolIndex();
    }

    if(selListType & DIST_BIT)
        apNode[0].selDistinctFlag = SM_TRUE;
    else
        apNode[0].selDistinctFlag = SM_FALSE;  

	if(limitClause != NULL_POOLINDEX)
	{
		apNode[0].limitStart = limitClause[0].limitStart;
		apNode[0].limitCount = limitClause[0].limitCount + 1;
	}
	else
	{
		apNode[0].limitStart = 0;
		apNode[0].limitCount = 0;
	}

    return eNOERROR;
}

Four OQL_GDStoCommonAP::makePlan_constructFinalProjection()
{
	CommonAP_PoolElements           apNode(m_pool->commonAP_Pool);
    CommonAP_PoolElements           apNode_temp(m_pool->commonAP_Pool);
    AP_ProjectionListPoolElements   projectionList(m_pool->projectionListPool);
    AP_ProjectionPoolElements       projectionInfo(m_pool->projectionPool);
    SelListPoolElements             selList(m_pool->selListPool);
    ArgumentPoolElements            argument(m_pool->argumentPool);
    AP_ArgumentPoolElements         ap_argument(m_pool->ap_argumentPool);
    FunctionPoolElements            func(m_pool->funcPool);
    PathExprPoolElements            pathExpr(m_pool->pathExprPool);
    AggrFuncPoolElements            aggrFunc(m_pool->aggrFuncPool);
    Four                            i, j;
    Four                            aggrFuncIndex;
    Four                            e;
	Four							selListType;
	
	e = m_gds->getSelList(m_pool, selList);			OOSQL_CHECK_ERR(e);
	e = m_gds->getSelListType(m_pool, selListType);	OOSQL_CHECK_ERR(e);
	
	if(selList == NULL_POOLINDEX)
		return eNOERROR;

    // -------------------
    // for last AP element
    // -------------------
    apNode.setPoolIndex(m_commonAP->commonAP.size - 1, 1);

	// make final projectionList and attach into apNode and fill projection info
    projectionList = m_pool->projectionListPool.addNewEntry();
    projectionList[0].init();
    apNode[0].projectionList = projectionList.getPoolIndex();

	projectionInfo = m_pool->projectionPool.addNewEntry(selList.size);
    for(i = 0, aggrFuncIndex = 0; i < selList.size; i++)
    {
        switch(selList[i].selElemKind)
        {
        case SELLIST_KIND_PATHEXPR:
            projectionInfo[i].projectionKind = PROJECTION_KIND_PATHEXPR;

            // if current apnode is tempclass
            if(apNode[0].classInfo.classKind == CLASSKIND_TEMPORARY || apNode[0].classInfo.classKind == CLASSKIND_SORTSTREAM)
            {
                e = setProjection(projectionInfo[i], selList[i]);
                OOSQL_CHECK_ERR(e);

                // patch
                switch(projectionInfo[i].pathExpr.kind)
                {
                case PATHEXPR_KIND_ATTR:
                    projectionInfo[i].pathExpr.col.planNo = apNode.startIndex;
                    pathExpr = selList[i].pathExpr;
                    e = getTempFileColNo(apNode[0].classInfo.tempFileNum, pathExpr, 
                                         projectionInfo[i].pathExpr.col.colNo);
                    OOSQL_CHECK_ERR(e);

					if(projectionInfo[i].resultType == TYPEID_TEXT)
					{
						projectionInfo[i].resultType = TYPEID_VARSTRING;
#ifndef SUPPORT_LARGE_DATABASE2
						projectionInfo[i].resultLength = INT_MAX;
#else
						projectionInfo[i].resultLength = LONG_MAX;
#endif
					}

                    break;

                case PATHEXPR_KIND_OBJECT:
                case PATHEXPR_KIND_CLASS:
                case PATHEXPR_KIND_OID:
                    projectionInfo[i].pathExpr.kind = PATHEXPR_KIND_ATTR;
                    projectionInfo[i].pathExpr.col.planNo = apNode.startIndex;
                    pathExpr = selList[i].pathExpr;
                    e = getTempFileColNo(apNode[0].classInfo.tempFileNum, pathExpr,
                                         projectionInfo[i].pathExpr.col.colNo);
                    break;
                default:
                    OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
                }
            }
            else
            {
                e = setProjection(projectionInfo[i], selList[i]);
                OOSQL_CHECK_ERR(e);
            }
            break;

        case SELLIST_KIND_AGGRFUNC:
            projectionInfo[i].projectionKind  = PROJECTION_KIND_AGGRFUNCRESULT;

            aggrFunc = selList[i].aggrFunc;
            argument = aggrFunc[0].argument;

            // if aggrFuncInfo already has been made, use it
            if(m_aggrFuncInfo_planNo != NULL_POOLINDEX)
            {
                e = setProjection(projectionInfo[i], selList[i]);
                OOSQL_CHECK_ERR(e);
            }
            else
                OOSQL_ERR(eINTERNALERROR_OOSQL);

            break;

        case SELLIST_KIND_FUNC:
			projectionInfo[i].projectionKind  = PROJECTION_KIND_FUNCEVAL;

            // if current apnode is tempclass
            if(apNode[0].classInfo.classKind == CLASSKIND_TEMPORARY || apNode[0].classInfo.classKind == CLASSKIND_SORTSTREAM)
            {
                e = setProjection(projectionInfo[i], selList[i]);
                OOSQL_CHECK_ERR(e);

				func = selList[i].func;

				projectionInfo[i].projectionKind      = PROJECTION_KIND_PATHEXPR;
                projectionInfo[i].pathExpr.kind       = PATHEXPR_KIND_ATTR;
				projectionInfo[i].pathExpr.col.planNo = apNode.startIndex;

                e = getTempFileColNo(apNode[0].classInfo.tempFileNum, func, 
                                     projectionInfo[i].pathExpr.col.colNo);
                OOSQL_CHECK_ERR(e);
            }
            else
            {
                e = setProjection(projectionInfo[i], selList[i]);
                OOSQL_CHECK_ERR(e);
            }
            break;
        case SELLIST_KIND_VALUE: 
        case SELLIST_KIND_OPER:
            e = setProjection(projectionInfo[i], selList[i]);
            OOSQL_CHECK_ERR(e);
            break;

        case SELLIST_KIND_CONS:
        default:
            OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
        }
        
    }

    projectionList[0].projectionInfo  = projectionInfo.getPoolIndex();

	return eNOERROR;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
error code

IMPLEMENTATION:

****************************************************************************/
Four OQL_GDStoCommonAP::makePlan_constructUpdate()
{
	CommonAP_PoolElements			apNode(m_pool->commonAP_Pool);
	CommonAP_PoolElements			prevApNode(m_pool->commonAP_Pool);
	UpdateValuePoolElements			updateValueList(m_pool->updateValuePool);
	AP_UpdateValuePoolElements		ap_updateValueList(m_pool->ap_updateValuePool);
	AP_ProjectionListPoolElements	projectionList;
	AP_ProjectionPoolElements		projectionInfo;
	AP_TempFileInfoPoolElements     tempFileInfo(m_pool->tempFileInfoPool);
	Four							tempFileNum;
	CataClassInfo					classInfo;
	Four							e;
	Four							i;
	AP_ColNoMapPoolElements			colNoMap(m_pool->colNoMapPool);
	AP_UsedColPoolElements          usedColInfo(m_pool->usedColPool);

	// -------------------
    // for last AP element
    // -------------------
    apNode.setPoolIndex(m_commonAP->commonAP.size - 1, 1);

	if(!makePlan_constructUpdate_isMakeTemporary())
	{
		// make new ap_updateValueList
		e = m_gds->getUpdateValueList(m_pool, updateValueList);
		OOSQL_CHECK_ERR(e);

		ap_updateValueList = m_pool->ap_updateValuePool.addNewEntry(updateValueList.size);
		
		if(apNode[0].classInfo.classKind != CLASSKIND_PERSISTENT)
			OOSQL_ERR(eINTERNALERROR_OOSQL);
		e = m_catalog->class_ClassId_to_ClassInfo(apNode[0].classInfo.classId, classInfo);
		OOSQL_CHECK_ERR(e);
		e = convertUpdateValue_to_AP_UpdateValue(classInfo, updateValueList, ap_updateValueList);
		OOSQL_CHECK_ERR(e);

		// connect access plan
		apNode[0].projectionList = m_pool->projectionListPool.addNewEntry();
		projectionList = apNode[0].projectionList.getElements(m_pool->projectionListPool);
		projectionList[0].init(PROJECTION_UPDATE);
		projectionList[0].updateInfo.classInfo.classKind = CLASSKIND_PERSISTENT;
		projectionList[0].updateInfo.classInfo.classId   = apNode[0].classInfo.classId;
		projectionList[0].updateInfo.updateValueList     = ap_updateValueList.getPoolIndex();
	}
	else
	{
		// make new ap_updateValueList
		e = m_gds->getUpdateValueList(m_pool, updateValueList);
		OOSQL_CHECK_ERR(e);
		ap_updateValueList = m_pool->ap_updateValuePool.addNewEntry(updateValueList.size);
		
		if(apNode[0].classInfo.classKind != CLASSKIND_PERSISTENT)
			OOSQL_ERR(eINTERNALERROR_OOSQL);
		e = m_catalog->class_ClassId_to_ClassInfo(apNode[0].classInfo.classId, classInfo);
		OOSQL_CHECK_ERR(e);
		e = convertUpdateValue_to_AP_UpdateValue(classInfo, updateValueList, ap_updateValueList);
		OOSQL_CHECK_ERR(e);

		apNode[0].projectionList = m_pool->projectionListPool.addNewEntry();
		projectionList = apNode[0].projectionList.getElements(m_pool->projectionListPool);
		projectionList[0].init();
		projectionList[0].projectionInfo = m_pool->projectionPool.addNewEntry(ap_updateValueList.size + 1);
		projectionInfo = projectionList[0].projectionInfo.getElements(m_pool->projectionPool);

		projectionInfo[0].resultType     = TYPEID_OID;
		projectionInfo[0].resultLength   = TYPEID_OID_SIZE;
		projectionInfo[0].projectionKind = PROJECTION_KIND_OID;
		projectionInfo[0].oid_of_plan    = apNode.startIndex;
		for(i = 0; i < ap_updateValueList.size; i++)
		{
			projectionInfo[i + 1].resultType     = ap_updateValueList[i].type;
			projectionInfo[i + 1].resultLength   = ap_updateValueList[i].length;
			projectionInfo[i + 1].projectionKind = PROJECTION_KIND_EXPR;
			projectionInfo[i + 1].expr           = ap_updateValueList[i].expr;
		}

        tempFileInfo = m_pool->tempFileInfoPool.addNewEntry(projectionInfo.size);
        tempFileNum  = getNewTempFileNum(); OOSQL_CHECK_ERR(tempFileNum);
        e = setTempFileInfoPoolIndex(tempFileNum, tempFileInfo.getPoolIndex()); OOSQL_CHECK_ERR(e);
        e = setTempFileUsage(tempFileNum, TEMPFILE_FOR_SELECT_DISTINCT, 0);     OOSQL_CHECK_ERR(e);
        projectionList[0].tempFileNum = tempFileNum;

        // construct tempFileInfo
        for(i = 0; i < projectionInfo.size; i++)
        {
            // set tempFileInfo
            e = makePlan_constructTempFileInfo(tempFileInfo[i], projectionInfo[i]);
            OOSQL_CHECK_ERR(e);
        }

		prevApNode = apNode;
		apNode = m_pool->commonAP_Pool.addNewEntry();
		apNode[0].init();
		if(m_commonAP->commonAP != NULL_POOLINDEX)
			m_commonAP->commonAP.size ++;
		else
			m_commonAP->commonAP = apNode.getPoolIndex();
		
		// set accessMethod and joinMethod
        apNode[0].accessMethod = CAP_ACCESSMETHOD_SEQSCAN;
        apNode[0].joinMethod   = CAP_JOINMETHOD_OUTERMOST_CLASS;

        // set classInfo
        apNode[0].classInfo.classKind   = CLASSKIND_TEMPORARY;
        apNode[0].classInfo.tempFileNum = tempFileNum;

        // set usedColInfo
        colNoMap    = m_pool->colNoMapPool.addNewEntry(tempFileInfo.size);
        usedColInfo = m_pool->usedColPool.addNewEntry(tempFileInfo.size);
        for(i = 0;i < tempFileInfo.size; i++)
        {
            colNoMap[i].offset    = i;
            usedColInfo[i].typeId = tempFileInfo[i].typeId;
            usedColInfo[i].length = tempFileInfo[i].length;
			// use column number of temporary table from 1 since colNo 0 is system reserved.
			usedColInfo[i].colNo  = i + 1;
        }
        apNode[0].colNoMap    = colNoMap.getPoolIndex();
        apNode[0].usedColInfo = usedColInfo.getPoolIndex();

		// connect access plan
		apNode[0].projectionList = m_pool->projectionListPool.addNewEntry();
		projectionList = apNode[0].projectionList.getElements(m_pool->projectionListPool);
		projectionList[0].init(PROJECTION_UPDATE);
		projectionList[0].updateInfo.classInfo.classKind = CLASSKIND_PERSISTENT;
		projectionList[0].updateInfo.classInfo.classId   = prevApNode[0].classInfo.classId;
		projectionList[0].updateInfo.updateValueList     = ap_updateValueList.getPoolIndex();
	}

	return eNOERROR;
}


Four OQL_GDStoCommonAP::makePlan_constructDelete_isMakeTemporary()
{
	CommonAP_PoolElements			apNode(m_pool->commonAP_Pool);
	AP_IndexInfoPoolElements		indexInfo;

    apNode.setPoolIndex(m_commonAP->commonAP.size - 1, 1);

	indexInfo = apNode[0].indexInfo.getElements(m_pool->indexInfoPool);
	
	if(indexInfo == NULL_POOLINDEX)
		return SM_FALSE;

	if(indexInfo[0].nodeKind == INDEXINFO_SCAN)
	{
		if(indexInfo[0].scan.indexType == INDEXTYPE_TEXT)
		{
			return SM_TRUE;
		}
	}
	return SM_FALSE;
}


Four OQL_GDStoCommonAP::makePlan_constructUpdate_isMakeTemporary()
{
	CommonAP_PoolElements			apNode(m_pool->commonAP_Pool);
	Four							e;
	AP_IndexInfoPoolElements		indexInfo;
	UpdateValuePoolElements			updateValueList;
	CataClassInfo					classInfo;
	CataAttrInfo					attrInfo;
	OOSQL_StorageManager::IndexID						indexId_array[1];
	Four							nIndexes;
	Four							i;

    apNode.setPoolIndex(m_commonAP->commonAP.size - 1, 1);
	if(apNode[0].classInfo.classKind != CLASSKIND_PERSISTENT)
		OOSQL_ERR(eINTERNALERROR_OOSQL);
	e = m_catalog->class_ClassId_to_ClassInfo(apNode[0].classInfo.classId, classInfo);
	OOSQL_CHECK_ERR(e);

	indexInfo = apNode[0].indexInfo.getElements(m_pool->indexInfoPool);
	
	if(indexInfo == NULL_POOLINDEX)
		return SM_FALSE;

	e = m_gds->getUpdateValueList(m_pool, updateValueList);
	OOSQL_CHECK_ERR(e);

	for(i = 0; i < updateValueList.size; i++)
	{
		attrInfo = updateValueList[i].attrInfo;
		nIndexes = 0;
		if(m_catalog->index_isB_TreeIndexExist(classInfo, attrInfo))
		{
			nIndexes = 1;
			e = m_catalog->index_getB_TreeIndexes(classInfo, attrInfo, nIndexes, indexId_array);
			OOSQL_CHECK_ERR(e);
		}
		else if(m_catalog->index_isTextIndexExist(classInfo, attrInfo))
		{
			nIndexes = 1;
			e = m_catalog->index_getTextIndexes(classInfo, attrInfo, nIndexes, indexId_array);
			OOSQL_CHECK_ERR(e);
		}
		else if(m_catalog->index_isMlgfIndexExist(classInfo, attrInfo))
		{
			nIndexes = 1;
			e = m_catalog->index_getMlgfIndexes(classInfo, attrInfo, nIndexes, indexId_array);
			OOSQL_CHECK_ERR(e);
		}

		if(nIndexes != 0)
		{
			if(indexInfo[0].nodeKind == INDEXINFO_SCAN)
			{
				switch(indexInfo[0].scan.indexType)
				{
				case INDEXTYPE_BTREE:
					if(!memcmp(&indexInfo[0].scan.indexId, &indexId_array[0], sizeof(OOSQL_StorageManager::IndexID)))
						return SM_TRUE;
					break;
				case INDEXTYPE_TEXT:
					if(!memcmp(&indexInfo[0].scan.indexId, &indexId_array[0], sizeof(OOSQL_StorageManager::IndexID)))
						return SM_TRUE;
					break;
				case INDEXTYPE_MLGF:
					if(!memcmp(&indexInfo[0].scan.indexId, &indexId_array[0], sizeof(OOSQL_StorageManager::IndexID)))
						return SM_TRUE;
					break;
				default:
					OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
				}
			}
			else
			{
				OOSQL_ERR(eNOTIMPLEMENTED_OOSQL);
			}
		}
	}

	return SM_FALSE;
}

Four OQL_GDStoCommonAP::makePlan_constructInsert_isMakeTemporary()
{
	CommonAP_PoolElements			apNode(m_pool->commonAP_Pool);
	Four							e;
	CataClassInfo					insertTarget;
	TargetListPoolElements			targetList;
	AP_ProjectionListPoolElements	projectionList;
	Four							i;

    apNode.setPoolIndex(m_commonAP->commonAP.size - 1, 1);
	
	projectionList = apNode[0].projectionList.getElements(m_pool->projectionListPool);
	if(projectionList[0].tempFileNum != -1)
		return SM_FALSE;

	e = m_gds->getTargetList(m_pool, targetList);
	OOSQL_CHECK_ERR(e);

	if(targetList != NULL_POOLINDEX)
	{
		insertTarget = m_gds->insertQuery.getElements(m_pool->insertQueryPool, 0).insertTarget;
		for(i = 0; i < targetList.size; i++)
		{
			if(targetList[i].collectionInfo.classInfo == insertTarget)
				return SM_TRUE;
		}
	}

	return SM_FALSE;
}

Four OQL_GDStoCommonAP::convertUpdateValue_to_AP_UpdateValue(CataClassInfo classInfo, UpdateValuePoolElements& updateValueList, AP_UpdateValuePoolElements& ap_updateValueList)
{
	ExprPoolElements	expr(m_pool->exprPool);
	AP_ExprPoolElements	ap_expr(m_pool->ap_exprPool);
	Four				e;
	Four				i;

	for(i = 0; i < updateValueList.size; i++)
	{
		e = m_catalog->attr_AttrInfo_to_ColNo(classInfo, updateValueList[i].attrInfo, ap_updateValueList[i].colNo);
		OOSQL_CHECK_ERR(e);

		expr    = updateValueList[i].expr;

		if(expr != NULL_POOLINDEX)
		{
			e = convertExpr_to_AP_Expr(expr, ap_expr);
			OOSQL_CHECK_ERR(e);
		}
		else
			ap_expr.setNull();

		ap_updateValueList[i].isParam = updateValueList[i].isParam;
		ap_updateValueList[i].expr	  = ap_expr.getPoolIndex();
		ap_updateValueList[i].type	  = updateValueList[i].type;
		ap_updateValueList[i].length  = updateValueList[i].length;
		ap_updateValueList[i].seqValueType = updateValueList[i].seqValueType;
	}

	return eNOERROR;
}

Four OQL_GDStoCommonAP::convertInsertValue_to_AP_InsertValue(CataClassInfo classInfo, InsertValuePoolElements& insertValueList, AP_InsertValuePoolElements& ap_insertValueList)
{
	Four				e;
	Four				i;

	ExprPoolElements		exprPool(m_pool->exprPool);
	AP_ExprPoolElements		ap_exprPool(m_pool->ap_exprPool);


	for(i = 0; i < insertValueList.size; i++)
	{
		e = m_catalog->attr_AttrInfo_to_ColNo(classInfo, insertValueList[i].attrInfo, ap_insertValueList[i].colNo);
		OOSQL_CHECK_ERR(e);
		
		ap_insertValueList[i].isParam = insertValueList[i].isParam;
        ap_insertValueList[i].poolType = insertValueList[i].poolType;
        if(ap_insertValueList[i].poolType == PT_EXPRPOOL)
        {
			exprPool = insertValueList[i].expr;
            e = convertExpr_to_AP_Expr(exprPool, ap_exprPool);
            OOSQL_CHECK_ERR(e);
			ap_insertValueList[i].expr.setPoolIndex(ap_exprPool.startIndex, ap_exprPool.size);
        }
		else
			ap_insertValueList[i].value	  = insertValueList[i].value;
		ap_insertValueList[i].type	  = insertValueList[i].type;
		ap_insertValueList[i].length  = insertValueList[i].length;
		ap_insertValueList[i].seqValueType = insertValueList[i].seqValueType;
	}

	return eNOERROR;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
error code

IMPLEMENTATION:

****************************************************************************/
Four OQL_GDStoCommonAP::makePlan_constructDelete()
{
	CommonAP_PoolElements           apNode(m_pool->commonAP_Pool);
	CommonAP_PoolElements			prevApNode(m_pool->commonAP_Pool);
	AP_ProjectionListPoolElements	projectionList;
	AP_ProjectionPoolElements		projectionInfo;
	AP_TempFileInfoPoolElements     tempFileInfo(m_pool->tempFileInfoPool);
	Four							tempFileNum;
	Boolean							isDeferredDelete;
	Four							e;
	Four							i;
	AP_ColNoMapPoolElements			colNoMap(m_pool->colNoMapPool);
    AP_UsedColPoolElements          usedColInfo(m_pool->usedColPool);

	e = m_gds->getDeferredDeleteFlag(m_pool, isDeferredDelete);
	OOSQL_CHECK_ERR(e);
	
	if(!makePlan_constructDelete_isMakeTemporary())
	{
	
		// -------------------
	    // for last AP element
    	// -------------------
	    apNode.setPoolIndex(m_commonAP->commonAP.size - 1, 1);
		apNode[0].projectionList = m_pool->projectionListPool.addNewEntry();
		projectionList = apNode[0].projectionList.getElements(m_pool->projectionListPool);
		projectionList[0].init(PROJECTION_DELETE);
		projectionList[0].deleteInfo.classInfo.classKind = CLASSKIND_PERSISTENT;
		projectionList[0].deleteInfo.classInfo.classId   = apNode[0].classInfo.classId;
		projectionList[0].deleteInfo.isDeferredDelete    = isDeferredDelete;
	}
	else
	{
		apNode[0].projectionList = m_pool->projectionListPool.addNewEntry();
		projectionList = apNode[0].projectionList.getElements(m_pool->projectionListPool);
		projectionList[0].init();
		projectionList[0].projectionInfo = m_pool->projectionPool.addNewEntry(1); // one for OID field
		projectionInfo = projectionList[0].projectionInfo.getElements(m_pool->projectionPool);

		projectionInfo[0].resultType     = TYPEID_OID;
		projectionInfo[0].resultLength   = TYPEID_OID_SIZE;
		projectionInfo[0].projectionKind = PROJECTION_KIND_OID;
		projectionInfo[0].oid_of_plan    = apNode.startIndex;

        tempFileInfo = m_pool->tempFileInfoPool.addNewEntry(projectionInfo.size);
        tempFileNum  = getNewTempFileNum(); OOSQL_CHECK_ERR(tempFileNum);
        e = setTempFileInfoPoolIndex(tempFileNum, tempFileInfo.getPoolIndex()); OOSQL_CHECK_ERR(e);
        e = setTempFileUsage(tempFileNum, TEMPFILE_FOR_SELECT_DISTINCT, 0);     OOSQL_CHECK_ERR(e);
        projectionList[0].tempFileNum = tempFileNum;

        // construct tempFileInfo
        for(i = 0; i < projectionInfo.size; i++)
        {
            // set tempFileInfo
            e = makePlan_constructTempFileInfo(tempFileInfo[i], projectionInfo[i]);
            OOSQL_CHECK_ERR(e);
        }

		prevApNode = apNode;
		apNode = m_pool->commonAP_Pool.addNewEntry();
		apNode[0].init();
		if(m_commonAP->commonAP != NULL_POOLINDEX)
			m_commonAP->commonAP.size ++;
		else
			m_commonAP->commonAP = apNode.getPoolIndex();
		
		// set accessMethod and joinMethod
        apNode[0].accessMethod = CAP_ACCESSMETHOD_SEQSCAN;
        apNode[0].joinMethod   = CAP_JOINMETHOD_OUTERMOST_CLASS;

        // set classInfo
        apNode[0].classInfo.classKind   = CLASSKIND_TEMPORARY;
        apNode[0].classInfo.tempFileNum = tempFileNum;

        // set usedColInfo
        colNoMap    = m_pool->colNoMapPool.addNewEntry(tempFileInfo.size);
        usedColInfo = m_pool->usedColPool.addNewEntry(tempFileInfo.size);
        for(i = 0;i < tempFileInfo.size; i++)
        {
            colNoMap[i].offset    = i;
            usedColInfo[i].typeId = tempFileInfo[i].typeId;
            usedColInfo[i].length = tempFileInfo[i].length;
			usedColInfo[i].colNo  = i + 1;
        }
        apNode[0].colNoMap    = colNoMap.getPoolIndex();
        apNode[0].usedColInfo = usedColInfo.getPoolIndex();

		// connect access plan
		apNode[0].projectionList = m_pool->projectionListPool.addNewEntry();
		projectionList = apNode[0].projectionList.getElements(m_pool->projectionListPool);
		projectionList[0].init(PROJECTION_DELETE);
		projectionList[0].deleteInfo.classInfo.classKind = CLASSKIND_PERSISTENT;
		projectionList[0].deleteInfo.classInfo.classId   = prevApNode[0].classInfo.classId;
		projectionList[0].deleteInfo.isDeferredDelete    = isDeferredDelete;
	}

	return eNOERROR;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
error code

IMPLEMENTATION:

****************************************************************************/
Four OQL_GDStoCommonAP::makePlan_constructInsert()
{
	CommonAP_PoolElements			apNode(m_pool->commonAP_Pool);
	InsertValuePoolElements			insertValueList(m_pool->insertValuePool);
	AP_InsertValuePoolElements		ap_insertValueList(m_pool->ap_insertValuePool);
	AP_ProjectionListPoolElements	projectionList;
	AP_ProjectionPoolElements		projectionInfo;
	AP_TempFileInfoPoolElements     tempFileInfo(m_pool->tempFileInfoPool);
	Four							tempFileNum;
	Four							classId;
	CataClassInfo                   classInfo;
	Four							e;
	Four							i;
	AP_ColNoMapPoolElements			colNoMap(m_pool->colNoMapPool);
    AP_UsedColPoolElements          usedColInfo(m_pool->usedColPool);

	// -------------------
    // for last AP element
    // -------------------
	if(m_commonAP->commonAP == NULL_POOLINDEX)
	{
		apNode = m_pool->commonAP_Pool.addNewEntry();
		apNode[0].init();
		apNode[0].joinMethod = CAP_JOINMETHOD_OUTERMOST_CLASS;
		m_commonAP->commonAP = apNode.getPoolIndex();
	}
	else
		apNode.setPoolIndex(m_commonAP->commonAP.size - 1, 1);

	if(!makePlan_constructInsert_isMakeTemporary())
	{
		// make new ap_insertValueList
		e = m_gds->getInsertValueList(m_pool, insertValueList);
		OOSQL_CHECK_ERR(e);

		ap_insertValueList = m_pool->ap_insertValuePool.addNewEntry(insertValueList.size);

		classInfo = m_gds->insertQuery.getElements(m_pool->insertQueryPool, 0).insertTarget;
		e = m_catalog->class_ClassInfo_to_ClassId(classInfo, classId);
		OOSQL_CHECK_ERR(e);
		e = convertInsertValue_to_AP_InsertValue(classInfo, insertValueList, ap_insertValueList);
		OOSQL_CHECK_ERR(e);

		if(apNode[0].projectionList == NULL_POOLINDEX)
		{
			// make new projectionList and connect it to access plan
			apNode[0].projectionList = m_pool->projectionListPool.addNewEntry();
			apNode[0].projectionList.getElements(m_pool->projectionListPool,0).init(PROJECTION_INSERT);
		}
		else
			apNode[0].projectionList.getElements(m_pool->projectionListPool,0).projectionType = PROJECTION_INSERT;
		
		projectionList = apNode[0].projectionList.getElements(m_pool->projectionListPool);
		projectionList[0].insertInfo.classInfo.classKind = CLASSKIND_PERSISTENT;
		projectionList[0].insertInfo.classInfo.classId   = classId;
		projectionList[0].insertInfo.insertValueList     = ap_insertValueList.getPoolIndex();
	}
	else
	{
		projectionList = apNode[0].projectionList.getElements(m_pool->projectionListPool);
		projectionInfo = projectionList[0].projectionInfo.getElements(m_pool->projectionPool);

		if(projectionList == NULL_POOLINDEX || projectionInfo == NULL_POOLINDEX)
			OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
		if(projectionList[0].tempFileNum != -1)
			OOSQL_ERR(eUNHANDLED_CASE_OOSQL);

        tempFileInfo = m_pool->tempFileInfoPool.addNewEntry(projectionInfo.size);
        tempFileNum  = getNewTempFileNum(); OOSQL_CHECK_ERR(tempFileNum);
        e = setTempFileInfoPoolIndex(tempFileNum, tempFileInfo.getPoolIndex()); OOSQL_CHECK_ERR(e);
        e = setTempFileUsage(tempFileNum, TEMPFILE_FOR_SELECT_DISTINCT, 0);     OOSQL_CHECK_ERR(e);
        projectionList[0].tempFileNum = tempFileNum;

        // construct tempFileInfo
        for(i = 0; i < projectionInfo.size; i++)
        {
            // set tempFileInfo
            e = makePlan_constructTempFileInfo(tempFileInfo[i], projectionInfo[i]);
            OOSQL_CHECK_ERR(e);
        }

		apNode = m_pool->commonAP_Pool.addNewEntry();
		apNode[0].init();
		if(m_commonAP->commonAP != NULL_POOLINDEX)
			m_commonAP->commonAP.size ++;
		else
			m_commonAP->commonAP = apNode.getPoolIndex();

		// set accessMethod and joinMethod
        apNode[0].accessMethod = CAP_ACCESSMETHOD_SEQSCAN;
        apNode[0].joinMethod   = CAP_JOINMETHOD_OUTERMOST_CLASS;

        // set classInfo
        apNode[0].classInfo.classKind   = CLASSKIND_SORTSTREAM;
        apNode[0].classInfo.tempFileNum = tempFileNum;

        // set usedColInfo
        colNoMap    = m_pool->colNoMapPool.addNewEntry(tempFileInfo.size);
        usedColInfo = m_pool->usedColPool.addNewEntry(tempFileInfo.size);
        for(i = 0;i < tempFileInfo.size; i++)
        {
            colNoMap[i].offset    = i;
            usedColInfo[i].typeId = tempFileInfo[i].typeId;
            usedColInfo[i].length = tempFileInfo[i].length;
			usedColInfo[i].colNo  = i;
        }
        apNode[0].colNoMap    = colNoMap.getPoolIndex();
        apNode[0].usedColInfo = usedColInfo.getPoolIndex();

		e = m_gds->getInsertValueList(m_pool, insertValueList);
		OOSQL_CHECK_ERR(e);

		ap_insertValueList = m_pool->ap_insertValuePool.addNewEntry(insertValueList.size);

		classInfo = m_gds->insertQuery.getElements(m_pool->insertQueryPool, 0).insertTarget;
		e = m_catalog->class_ClassInfo_to_ClassId(classInfo, classId);
		OOSQL_CHECK_ERR(e);
		e = convertInsertValue_to_AP_InsertValue(classInfo, insertValueList, ap_insertValueList);
		OOSQL_CHECK_ERR(e);

		// make new projectionList and connect it to access plan
		apNode[0].projectionList = m_pool->projectionListPool.addNewEntry();
		apNode[0].projectionList.getElements(m_pool->projectionListPool,0).init(PROJECTION_INSERT);
		
		projectionList = apNode[0].projectionList.getElements(m_pool->projectionListPool);
		projectionList[0].insertInfo.classInfo.classKind = CLASSKIND_PERSISTENT;
		projectionList[0].insertInfo.classInfo.classId   = classId;
		projectionList[0].insertInfo.insertValueList     = ap_insertValueList.getPoolIndex();
	}

	return eNOERROR;
}

Four OQL_GDStoCommonAP::makePlan_coustructAggrFuncInfoExceptionalCase(CommonAP_PoolElements &apNode)
{
    CommonAP_PoolElements       apNode_temp(m_pool->commonAP_Pool);
    SelListPoolElements         selList(m_pool->selListPool);
    AggrFuncPoolElements        aggrFunc(m_pool->aggrFuncPool);
    AP_AggrFuncPoolElements     ap_aggrFuncWdist(m_pool->ap_aggrFuncPool);
    AP_AggrFuncPoolElements     ap_aggrFuncWOdist(m_pool->ap_aggrFuncPool);
    ArgumentPoolElements        argument(m_pool->argumentPool);
    AP_ArgumentPoolElements     ap_argument(m_pool->ap_argumentPool);
    Four                        i;
    Four                        lengthWdist;        // length of aggrFuncInfo with distinct
    Four                        lengthWOdist;       // legnth of aggrFuncInfo without distinct
    Four                        indexWdist, indexWOdist;
    Four                        e;

	e = m_gds->getSelList(m_pool, selList); OOSQL_CHECK_ERR(e);
    
    // count length of ap_aggrFunc
    for(i = 0, lengthWdist = 0, lengthWOdist = 0; i < selList.size; i++)
    {
        if(selList[i].selElemKind == SELLIST_KIND_AGGRFUNC)
        {
            aggrFunc = selList[i].aggrFunc;

            if(aggrFunc[0].distinctFlag)
                lengthWdist ++;
            else
                lengthWOdist ++;
        }
    }
    
    // there is no aggr func, return
    if(lengthWdist == 0 && lengthWOdist == 0)
        return eNOERROR;

    // allocate ap_aggrFunc
    apNode_temp = m_aggrFuncInfo_planNo;      // set apNode_temp in which aggr func info set
    if(lengthWdist)
    {
        ap_aggrFuncWdist = m_pool->ap_aggrFuncPool.addNewEntry(lengthWdist);
        apNode_temp[0].aggrFuncInfo = ap_aggrFuncWdist.getPoolIndex();
    }
    else
        apNode_temp[0].aggrFuncInfo.setNull();

    apNode_temp.startIndex --;
    if(lengthWOdist)
    {
        ap_aggrFuncWOdist = m_pool->ap_aggrFuncPool.addNewEntry(lengthWOdist);
        apNode_temp[0].aggrFuncInfo = ap_aggrFuncWOdist.getPoolIndex();
    }
    else
        apNode_temp[0].aggrFuncInfo.setNull();

    // construct aggrFuncInfo
    for(i = 0, indexWdist = 0, indexWOdist = 0; i < selList.size; i++)
    {
        switch(selList[i].selElemKind)
        {
        case SELLIST_KIND_AGGRFUNC:
            aggrFunc    = selList[i].aggrFunc;
                
            if(aggrFunc[0].distinctFlag)
            {
                AP_TempFileInfoPoolElements tempFileInfo(m_pool->tempFileInfoPool);
                AP_TempFileInfoPoolIndex    poolIndex;
                AP_UsedColPoolElements      usedColInfo(m_pool->usedColPool);
                Four                        tempFileNum;
                Four                        j;
            
                ap_aggrFuncWdist[indexWdist].aggrFunctionID = aggrFunc[0].aggrFunctionID;
                ap_aggrFuncWdist[indexWdist].distinctFlag   = aggrFunc[0].distinctFlag;
                ap_aggrFuncWdist[indexWdist].srcAggrFunc    = aggrFunc.getPoolIndex();

                // from selList info, find tempFileNum
                e = getTempFileNum(TEMPFILE_FOR_AGGR_DISTINCT, i, tempFileNum);
                OOSQL_CHECK_ERR(e);

                e = getTempFileInfoPoolIndex(tempFileNum, poolIndex);
                OOSQL_CHECK_ERR(e);
                tempFileInfo = poolIndex;
                ap_aggrFuncWdist[indexWdist].tempFileNum = tempFileNum;
                
                // construct usedColInfo
                usedColInfo = m_pool->usedColPool.addNewEntry(tempFileInfo.size);
                ap_aggrFuncWdist[indexWdist].usedColInfo = usedColInfo.getPoolIndex();
                for(j = 0; j < tempFileInfo.size; j++)
                {
                    usedColInfo[j].typeId = tempFileInfo[j].typeId;
                    usedColInfo[j].length = tempFileInfo[j].length;
					usedColInfo[j].colNo  = j;
                }

                // construct aggrfunc's argument
                argument = aggrFunc[0].argument;

                ap_argument = m_pool->ap_argumentPool.addNewEntry(aggrFunc[0].argument.size);
                ap_aggrFuncWdist[indexWdist].argument = ap_argument.getPoolIndex();

                for(j = 0; j < aggrFunc[0].argument.size; j++)
                {
                    ap_argument[j].argumentKind            = ARGUMENT_KIND_TEMPFILECOL;
                    ap_argument[j].tempFileCol.tempFileNum = tempFileNum; 
                    ap_argument[j].tempFileCol.colNo       = tempFileInfo.size - aggrFunc[0].argument.size + j;
                }

                indexWdist ++;
            }
            else
            {
                ap_aggrFuncWOdist[indexWOdist].aggrFunctionID = aggrFunc[0].aggrFunctionID;
                ap_aggrFuncWOdist[indexWOdist].distinctFlag   = aggrFunc[0].distinctFlag;
                ap_aggrFuncWOdist[indexWOdist].srcAggrFunc    = aggrFunc.getPoolIndex();

                // if there is a temp file for group by or order by then use it
                if(apNode[0].classInfo.classKind == CLASSKIND_TEMPORARY || apNode[0].classInfo.classKind == CLASSKIND_SORTSTREAM)
                {
                    AP_TempFileInfoPoolElements tempFileInfo(m_pool->tempFileInfoPool);
                    AP_TempFileInfoPoolIndex    poolIndex;
                    AP_UsedColPoolElements      usedColInfo(m_pool->usedColPool);
                    Four                        j;
                    
                    // there is a temp file to use. get value from temp file
                    // get tempFileInfo
                    e = getTempFileInfoPoolIndex(apNode[0].classInfo.tempFileNum, poolIndex);
                    OOSQL_CHECK_ERR(e);
                    tempFileInfo = poolIndex;
                    ap_aggrFuncWOdist[indexWOdist].tempFileNum = apNode[0].classInfo.tempFileNum;

                    // construct aggrfunc's argument
                    argument = aggrFunc[0].argument;

                    ap_argument = m_pool->ap_argumentPool.addNewEntry(aggrFunc[0].argument.size);
                    ap_aggrFuncWOdist[indexWOdist].argument = ap_argument.getPoolIndex();

                    // apNode[0] access tempFileInfo so, use plan instead of tempfile
                    for(j = 0; j < aggrFunc[0].argument.size; j++)
                    {
                        ap_argument[j].argumentKind        = ARGUMENT_KIND_PATHEXPR;
                        ap_argument[j].pathExpr.kind       = PATHEXPR_KIND_ATTR;
                        ap_argument[j].pathExpr.col.planNo = apNode.startIndex;
                    }

                    // determine col no.
                    for(j = 0; j < tempFileInfo.size; j++)
                    {
                        if(tempFileInfo[j].kind == TFS_KIND_PATHEXPR)
                        {
                            PathExprPoolElements    argumentPathExpr(m_pool->pathExprPool);
                            PathExprPoolElements    tempFilePathExpr(m_pool->pathExprPool);

                            argumentPathExpr = argument[0].pathExpr;
                            tempFilePathExpr = tempFileInfo[j].pathExpr;
                            if(isSamePathExpr(argumentPathExpr, tempFilePathExpr))
                                break;  // found
                        }
                    }

                    if(j < tempFileInfo.size)
                        ap_argument[0].pathExpr.col.colNo = j;
                    else
                        OOSQL_ERR(eNOT_FOUND_OOSQL);
                }
                else
                {
                    // there is no temp file to use. get value from plan
                    argument = aggrFunc[0].argument;
                    e = convertArgument_to_AP_Argument(argument, ap_argument);
                    OOSQL_CHECK_ERR(e);
                    ap_aggrFuncWOdist[indexWOdist].argument = ap_argument.getPoolIndex();
                }
                
                indexWOdist ++;
            }

            
            break;
        case SELLIST_KIND_PATHEXPR:
        case SELLIST_KIND_FUNC:
        case SELLIST_KIND_VALUE: 
        case SELLIST_KIND_OPER:
        case SELLIST_KIND_CONS:
            break;
        default:
            OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
        }
    }

    return eNOERROR;
}

Four OQL_GDStoCommonAP::makePlan_constructAggrFuncInfo(CommonAP_PoolElements &apNode)
{
    CommonAP_PoolElements       apNode_temp(m_pool->commonAP_Pool);
    SelListPoolElements         selList(m_pool->selListPool);
    AggrFuncPoolElements        aggrFunc(m_pool->aggrFuncPool);
    AP_AggrFuncPoolElements     ap_aggrFunc(m_pool->ap_aggrFuncPool);
    ArgumentPoolElements        argument(m_pool->argumentPool);
    AP_ArgumentPoolElements     ap_argument(m_pool->ap_argumentPool);
    Four                        i;
    Four                        length;
    Four                        index;
    Four                        e;
	Four						j;
    ExprPoolElements                havingCond(m_pool->exprPool);
    ExprPoolElements                expr(m_pool->exprPool);
    ExprPoolIndex                   exprPoolIndex;
	OrderByListPoolElements         orderByList(m_pool->orderByListPool);



    e = m_gds->getSelList(m_pool, selList); OOSQL_CHECK_ERR(e);
    e = m_gds->getHavingCond(m_pool, havingCond);   OOSQL_CHECK_ERR(e);
	e = m_gds->getOrderByList(m_pool, orderByList);	OOSQL_CHECK_ERR(e);


    // count length of ap_aggrFunc
    for(i = 0, length = 0; i < selList.size; i++)
    {
        if(selList[i].selElemKind == SELLIST_KIND_AGGRFUNC)
            length ++;
    }

    for (i = 0; i < havingCond.size; i++)
    {
        //if the number of operand is 3
        //for (j = 0; j < 3; j ++)
        //if the number of operand is 2
        for (j = 0; j < 2; j ++){
            //0 means operand1, 1 means operand2, 2 means operand3
            switch(j){
                case 0:
                    exprPoolIndex = havingCond[i].oper.operand1;
                    expr = exprPoolIndex;
                    break;
                case 1:
                    exprPoolIndex = havingCond[i].oper.operand2;
                    expr = exprPoolIndex;
                    break;
                // if there is operand3,
                /*
                case 2:
                    exprPoolIndex = havingCond[i].oper.operand3;
                    expr = exprPoolIndex;
                    break;
                 */
                default:
                    OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
                    break;
            }

            if(expr[0].exprKind == EXPR_KIND_AGGRFUNC)
                length++;
        }
    }

	for (i = 0; i < orderByList.size; i++){
		if (orderByList[i].ordByKeyKind == ORDERBYLIST_KIND_AGGRFUNC)
			length ++;
	}

    // there is no aggr func, return
    if(length == 0)
        return eNOERROR;

    // set apNode_temp in which aggr func info set
    apNode_temp = m_aggrFuncInfo_planNo;

    // allocate ap_aggrFunc
    ap_aggrFunc = m_pool->ap_aggrFuncPool.addNewEntry(length);
    apNode_temp[0].aggrFuncInfo = ap_aggrFunc.getPoolIndex();

    // construct aggrFuncInfo
    for(i = 0, index = 0; i < selList.size; i++)
    {
        switch(selList[i].selElemKind)
        {
        case SELLIST_KIND_AGGRFUNC:
            aggrFunc    = selList[i].aggrFunc;

            ap_aggrFunc[index].aggrFunctionID = aggrFunc[0].aggrFunctionID;
            ap_aggrFunc[index].distinctFlag   = aggrFunc[0].distinctFlag;
            ap_aggrFunc[index].srcAggrFunc    = aggrFunc.getPoolIndex();

            if(aggrFunc[0].distinctFlag)
            {
                AP_TempFileInfoPoolElements tempFileInfo(m_pool->tempFileInfoPool);
                AP_TempFileInfoPoolIndex    poolIndex;
                AP_UsedColPoolElements      usedColInfo(m_pool->usedColPool);
                Four                        tempFileNum;
                Four                        j;
            
                // from selList info, find tempFileNum
                e = getTempFileNum(TEMPFILE_FOR_AGGR_DISTINCT, i, tempFileNum);
                OOSQL_CHECK_ERR(e);

                e = getTempFileInfoPoolIndex(tempFileNum, poolIndex);
                OOSQL_CHECK_ERR(e);
                tempFileInfo = poolIndex;
                ap_aggrFunc[index].tempFileNum = tempFileNum;
                
                // construct usedColInfo
                usedColInfo = m_pool->usedColPool.addNewEntry(tempFileInfo.size);
                ap_aggrFunc[index].usedColInfo = usedColInfo.getPoolIndex();
                for(j = 0; j < tempFileInfo.size; j++)
                {
                    usedColInfo[j].typeId = tempFileInfo[j].typeId;
                    usedColInfo[j].length = tempFileInfo[j].length;
					usedColInfo[j].colNo  = j;
                }

                // construct aggrfunc's argument
                argument = aggrFunc[0].argument;

                ap_argument = m_pool->ap_argumentPool.addNewEntry(aggrFunc[0].argument.size);
                ap_aggrFunc[index].argument = ap_argument.getPoolIndex();

                for(j = 0; j < aggrFunc[0].argument.size; j++)
                {
                    ap_argument[j].argumentKind            = ARGUMENT_KIND_TEMPFILECOL;
                    ap_argument[j].tempFileCol.tempFileNum = tempFileNum; 
                    ap_argument[j].tempFileCol.colNo       = tempFileInfo.size - aggrFunc[0].argument.size + j;
                }
            }
            else
            {
                // if there is a temp file for group by or order by then use it
                if(apNode[0].classInfo.classKind == CLASSKIND_TEMPORARY || apNode[0].classInfo.classKind == CLASSKIND_SORTSTREAM)
                {
                    AP_TempFileInfoPoolElements tempFileInfo(m_pool->tempFileInfoPool);
                    AP_TempFileInfoPoolIndex    poolIndex;
                    AP_UsedColPoolElements      usedColInfo(m_pool->usedColPool);
                    Four                        j;
                    
                    // there is a temp file to use. get value from temp file
                    // get tempFileInfo
                    e = getTempFileInfoPoolIndex(apNode[0].classInfo.tempFileNum, poolIndex);
                    OOSQL_CHECK_ERR(e);
                    tempFileInfo = poolIndex;
                    ap_aggrFunc[index].tempFileNum = apNode[0].classInfo.tempFileNum;

                    // construct aggrfunc's argument
                    argument = aggrFunc[0].argument;

                    ap_argument = m_pool->ap_argumentPool.addNewEntry(aggrFunc[0].argument.size);
                    ap_aggrFunc[index].argument = ap_argument.getPoolIndex();

                    // apNode[0] access tempFileInfo so, use plan instead of tempfile
                    for(j = 0; j < aggrFunc[0].argument.size; j++)
                    {
                        ap_argument[j].argumentKind        = ARGUMENT_KIND_PATHEXPR;
                        ap_argument[j].pathExpr.kind       = PATHEXPR_KIND_ATTR;
                        ap_argument[j].pathExpr.col.planNo = apNode.startIndex;
                    }

                    // determine col no.
                    for(j = 0; j < tempFileInfo.size; j++)
                    {
                        if(tempFileInfo[j].kind == TFS_KIND_PATHEXPR)
                        {
                            PathExprPoolElements    argumentPathExpr(m_pool->pathExprPool);
                            PathExprPoolElements    tempFilePathExpr(m_pool->pathExprPool);

                            argumentPathExpr = argument[0].pathExpr;
                            tempFilePathExpr = tempFileInfo[j].pathExpr;
                            if(isSamePathExpr(argumentPathExpr, tempFilePathExpr))
                                break;  // found
                        }
                    }

                    if(j < tempFileInfo.size)
                        ap_argument[0].pathExpr.col.colNo = j;
                    else
                        OOSQL_ERR(eNOT_FOUND_OOSQL);
                }
                else
                {
                    // there is no temp file to use. get value from plan
                    argument = aggrFunc[0].argument;
                    e = convertArgument_to_AP_Argument(argument, ap_argument);
                    OOSQL_CHECK_ERR(e);
                    ap_aggrFunc[index].argument = ap_argument.getPoolIndex();
                }
            }

            index ++;
            break;
        case SELLIST_KIND_PATHEXPR:
        case SELLIST_KIND_FUNC:
        case SELLIST_KIND_VALUE: 
        case SELLIST_KIND_OPER:
        case SELLIST_KIND_CONS:
            break;
        default:
            OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
        }
    }

    for (i = 0; i < havingCond.size; i++)
    {
        //if the number of operand is 3
        //for (j = 0; j < 3; j ++)
        //if the number of operand is 2
        for (j = 0; j < 2; j++){
            //0 means operand1, 1 means operand2, 2 means operand3
            switch(j){
            case 0:
                exprPoolIndex = havingCond[i].oper.operand1;
                expr = exprPoolIndex;
                break;
            case 1:
                exprPoolIndex = havingCond[i].oper.operand2;
                expr = exprPoolIndex;
                break;
            // if there is operand3,
            /*
            case 2:
                exprPoolIndex = havingCond[i].oper.operand3;
                expr = exprPoolIndex;
                break;
            */
            default:
                OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
                break;
            }

            switch(expr[0].exprKind){
            case EXPR_KIND_AGGRFUNC:
                    aggrFunc    = expr[0].aggrFunc;

                    ap_aggrFunc[index].aggrFunctionID = aggrFunc[0].aggrFunctionID;
                    ap_aggrFunc[index].distinctFlag   = aggrFunc[0].distinctFlag;
                    ap_aggrFunc[index].srcAggrFunc    = aggrFunc.getPoolIndex();

                    // if there is a temp file for group by or order by then use it
                    if(apNode[0].classInfo.classKind == CLASSKIND_TEMPORARY || apNode[0].classInfo.classKind == CLASSKIND_SORTSTREAM)
                    {
                        AP_TempFileInfoPoolElements tempFileInfo(m_pool->tempFileInfoPool);
                        AP_TempFileInfoPoolIndex    poolIndex;
                        AP_UsedColPoolElements      usedColInfo(m_pool->usedColPool);
                        Four                        k;

                        // there is a temp file to use. get value from temp file
                        // get tempFileInfo
                        e = getTempFileInfoPoolIndex(apNode[0].classInfo.tempFileNum, poolIndex);
                        OOSQL_CHECK_ERR(e);
                        tempFileInfo = poolIndex;
                        ap_aggrFunc[index].tempFileNum = apNode[0].classInfo.tempFileNum;

                        // construct aggrfunc's argument
                        argument = aggrFunc[0].argument;

                        ap_argument = m_pool->ap_argumentPool.addNewEntry(aggrFunc[0].argument.size);
                        ap_aggrFunc[index].argument = ap_argument.getPoolIndex();

                        // apNode[0] access tempFileInfo so, use plan instead of tempfile
                        for(k = 0; k < aggrFunc[0].argument.size; k++)
                        {
                            ap_argument[k].argumentKind        = ARGUMENT_KIND_PATHEXPR;
                            ap_argument[k].pathExpr.kind       = PATHEXPR_KIND_ATTR;
                            ap_argument[k].pathExpr.col.planNo = apNode.startIndex;
                        }

                        // determine col no.
                        for(k = 0; k < tempFileInfo.size; k++)
                        {
                            if(tempFileInfo[k].kind == TFS_KIND_PATHEXPR)
                            {
                                PathExprPoolElements    argumentPathExpr(m_pool->pathExprPool);
                                PathExprPoolElements    tempFilePathExpr(m_pool->pathExprPool);

                                argumentPathExpr = argument[0].pathExpr;
                                tempFilePathExpr = tempFileInfo[k].pathExpr;
                                if(isSamePathExpr(argumentPathExpr, tempFilePathExpr))
                                    break;  // found
                            }
                        }

                        if(k < tempFileInfo.size)
                            ap_argument[0].pathExpr.col.colNo = k;
                        else
                            OOSQL_ERR(eNOT_FOUND_OOSQL);
                    }
                    else
                    {
                        // there is no temp file to use. get value from plan
                        argument = aggrFunc[0].argument;
                        e = convertArgument_to_AP_Argument(argument, ap_argument);
                        OOSQL_CHECK_ERR(e);
                        ap_aggrFunc[index].argument = ap_argument.getPoolIndex();
                    }
                    index++;
                    break;
            case EXPR_KIND_PATHEXPR:
            case EXPR_KIND_FUNCTION:
            case EXPR_KIND_OPER:
            case EXPR_KIND_VALUE:
            case EXPR_KIND_CONS:
            case EXPR_KIND_AGGRFUNCRESULT:
            case EXPR_KIND_FUNCRESULT:
            case EXPR_KIND_FUNCEVAL:
                    break;
            default:
                    OOSQL_ERR(eUNHANDLED_CASE_OOSQL);

            }

        }
    }
	for (i = 0; i < orderByList.size; i++){
		switch(orderByList[i].ordByKeyKind){
		case ORDERBYLIST_KIND_AGGRFUNC:
			aggrFunc    = orderByList[i].aggrFunc;
			
			ap_aggrFunc[index].aggrFunctionID = aggrFunc[0].aggrFunctionID;
			ap_aggrFunc[index].distinctFlag   = aggrFunc[0].distinctFlag;
			ap_aggrFunc[index].srcAggrFunc    = aggrFunc.getPoolIndex();
			
			// if there is a temp file for group by or order by then use it
			if(apNode[0].classInfo.classKind == CLASSKIND_TEMPORARY || apNode[0].classInfo.classKind == CLASSKIND_SORTSTREAM)
			{
				AP_TempFileInfoPoolElements tempFileInfo(m_pool->tempFileInfoPool);
				AP_TempFileInfoPoolIndex    poolIndex;
				AP_UsedColPoolElements      usedColInfo(m_pool->usedColPool);
				Four                        j;
				
				// there is a temp file to use. get value from temp file
				// get tempFileInfo
				e = getTempFileInfoPoolIndex(apNode[0].classInfo.tempFileNum, poolIndex);
				OOSQL_CHECK_ERR(e);
				tempFileInfo = poolIndex;
				ap_aggrFunc[index].tempFileNum = apNode[0].classInfo.tempFileNum;
				
				// construct aggrfunc's argument
				argument = aggrFunc[0].argument;
				
				ap_argument = m_pool->ap_argumentPool.addNewEntry(aggrFunc[0].argument.size);
				ap_aggrFunc[index].argument = ap_argument.getPoolIndex();
				
				// apNode[0] access tempFileInfo so, use plan instead of tempfile
				for(j = 0; j < aggrFunc[0].argument.size; j++)
				{
					ap_argument[j].argumentKind        = ARGUMENT_KIND_PATHEXPR;
					ap_argument[j].pathExpr.kind       = PATHEXPR_KIND_ATTR;
					ap_argument[j].pathExpr.col.planNo = apNode.startIndex;
				}
				
				// determine col no.
				for(j = 0; j < tempFileInfo.size; j++)
				{
					if(tempFileInfo[j].kind == TFS_KIND_PATHEXPR)
					{
						PathExprPoolElements    argumentPathExpr(m_pool->pathExprPool);
						PathExprPoolElements    tempFilePathExpr(m_pool->pathExprPool);
						
						argumentPathExpr = argument[0].pathExpr;
						tempFilePathExpr = tempFileInfo[j].pathExpr;
						if(isSamePathExpr(argumentPathExpr, tempFilePathExpr))
							break;  // found
					}
				}
				
				if(j < tempFileInfo.size)
					ap_argument[0].pathExpr.col.colNo = j;
				else
					OOSQL_ERR(eNOT_FOUND_OOSQL);
			}
			else
			{
				// there is no temp file to use. get value from plan
				argument = aggrFunc[0].argument;
				e = convertArgument_to_AP_Argument(argument, ap_argument);
				OOSQL_CHECK_ERR(e);
				ap_aggrFunc[index].argument = ap_argument.getPoolIndex();
			}
			index++;
			break;

		case ORDERBYLIST_KIND_PATHEXPR:
		case ORDERBYLIST_KIND_FUNC:
			break;
		}
	}
    return eNOERROR;
}


Four OQL_GDStoCommonAP::isSameValue(ValuePoolElements &a, ValuePoolElements &b)
{
    StringPoolElements  a_string(m_pool->stringPool), b_string(m_pool->stringPool);
    IntegerPoolElements a_int(m_pool->intPool), b_int(m_pool->intPool);
    RealPoolElements    a_real(m_pool->realPool), b_real(m_pool->realPool);
    MBRPoolElements     a_mbr(m_pool->mbrPool), b_mbr(m_pool->mbrPool);
    int                 i;

    if(a[0].valueKind != b[0].valueKind)
        return SM_FALSE;

    switch(a[0].valueKind)
    {
    case VALUE_KIND_STRING:
        a_string = a[0].string;
        b_string = b[0].string;
        if(a_string.size != b_string.size)
            return SM_FALSE;
        for(i = 0; i < a_string.size; i++)
            if(a_string[i] != b_string[i])
                return SM_FALSE;
        break;

    case VALUE_KIND_INTEGER:
        a_int = a[0].integer;
        b_int = b[0].integer;
        if(a_int[0] != b_int[0])
            return SM_FALSE;
        break;

    case VALUE_KIND_REAL:
        a_real = a[0].real;
        b_real = b[0].real;
        if(a_real[0] != b_real[0])
            return SM_FALSE;
        break;

    case VALUE_KIND_BOOL:
    case VALUE_KIND_NIL:
        if(a != b)
            return SM_FALSE;
        break;
    case VALUE_KIND_MBR:
        a_mbr = a[0].mbr;
        b_mbr = b[0].mbr;
        
        a_int = a_mbr[0].x1;   b_int = b_mbr[0].x1;
        if(a_int != b_int)
            return SM_FALSE;
        a_int = a_mbr[0].x2;   b_int = b_mbr[0].x2;
        if(a_int != b_int)
            return SM_FALSE;
        a_int = a_mbr[0].y1;   b_int = b_mbr[0].y1;
        if(a_int != b_int)
            return SM_FALSE;
        a_int = a_mbr[0].y2;   b_int = b_mbr[0].y2;
        if(a_int != b_int)
            return SM_FALSE;
    }
    return SM_TRUE;
}

Four OQL_GDStoCommonAP::isSameDomain(DomainPoolElements &a, DomainPoolElements &b)
{
    if(a[0].domainClass != b[0].domainClass)
        return SM_FALSE;

    if(a[0].starFlag != b[0].starFlag)
        return SM_FALSE;

    return SM_TRUE;
}

Four OQL_GDStoCommonAP::isSameAggrFunc(AggrFuncPoolElements &a, AggrFuncPoolElements &b)
{
    ArgumentPoolElements    a_argument(m_pool->argumentPool), b_argument(m_pool->argumentPool);

    if(a[0].aggrFunctionID != b[0].aggrFunctionID)
        return SM_FALSE;
    
    if(a[0].distinctFlag != b[0].distinctFlag)
        return SM_FALSE;

    a_argument = a[0].argument;
    b_argument = b[0].argument;
    if(!isSameArgument(a_argument, b_argument))
        return SM_FALSE;

    return SM_TRUE;
}

Four OQL_GDStoCommonAP::isSameArgument(ArgumentPoolElements &a, ArgumentPoolElements &b)
{
    ArgumentPoolElements    a_argument(m_pool->argumentPool), b_argument(m_pool->argumentPool);
    PathExprPoolElements    a_pathExpr(m_pool->pathExprPool), b_pathExpr(m_pool->pathExprPool);
    ValuePoolElements       a_value(m_pool->valuePool), b_value(m_pool->valuePool);
    FunctionPoolElements    a_func(m_pool->funcPool), b_func(m_pool->funcPool);
    DomainPoolElements      a_domain(m_pool->domainPool), b_domain(m_pool->domainPool);
    AggrFuncPoolElements    a_aggrFunc(m_pool->aggrFuncPool), b_aggrFunc(m_pool->aggrFuncPool);
    int                     i;

    // check size
    if(a.size != b.size)
        return SM_FALSE;

    // check body
    for(i = 0; i < a.size; i++)
    {
        if(a[i].argumentKind != b[i].argumentKind)
            return SM_FALSE;

        switch(a[i].argumentKind)
        {
        case ARGUMENT_KIND_PATHEXPR:
            a_pathExpr = a[i].pathExpr; 
            b_pathExpr = b[i].pathExpr;

            if(!isSamePathExpr(a_pathExpr, b_pathExpr))
                return SM_FALSE;
            break;

        case ARGUMENT_KIND_VALUE:
            a_value = a[i].value;
            b_value = b[i].value;

            if(!isSameValue(a_value, b_value))
                return SM_FALSE;
            break;

        case ARGUMENT_KIND_FUNC:
            a_func = a[i].func;
            b_func = b[i].func;
            if(!isSameFunction(a_func, b_func))
                return SM_FALSE;
            break;

        case ARGUMENT_KIND_DOMAIN:
            a_domain = a[i].domain;
            b_domain = b[i].domain;
            if(!isSameDomain(a_domain, b_domain))
                return SM_FALSE;
            break;

        case ARGUMENT_KIND_AGGRFUNC:
            a_aggrFunc = a[i].aggrFunc;
            b_aggrFunc = b[i].aggrFunc;
            if(!isSameAggrFunc(a_aggrFunc, b_aggrFunc))
                return SM_FALSE;
            break;

        default:
            OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
            break;
        }
    }

    return SM_TRUE;
}

Four OQL_GDStoCommonAP::isSameFunction(FunctionPoolElements &a, FunctionPoolElements &b)
{
    ArgumentPoolElements    a_argument(m_pool->argumentPool), b_argument(m_pool->argumentPool);

    if(a[0].functionID != b[0].functionID)
        return SM_FALSE;

    a_argument = a[0].argument;
    b_argument = b[0].argument;
    if(!isSameArgument(a_argument, b_argument))
        return SM_FALSE;

    return SM_TRUE;
}

Four OQL_GDStoCommonAP::isSameObject(ObjectPoolElements &a, ObjectPoolElements &b)
{
    OOSQL_ERR(eNOTIMPLEMENTED_OOSQL);
    return SM_TRUE;
}

Four OQL_GDStoCommonAP::isSamePathExpr(PathExprPoolElements &a, PathExprPoolElements &b)
{
    Four                    i, j;
    Four                    a_stringIndex, b_stringIndex;
    ArgumentPoolElements    a_argument(m_pool->argumentPool), b_argument(m_pool->argumentPool);
    FunctionPoolElements    a_func(m_pool->funcPool), b_func(m_pool->funcPool);
    ObjectPoolElements      a_object(m_pool->objectPool), b_object(m_pool->objectPool);

    // check size
    if(a.size != b.size)
        return SM_FALSE;

    // check body
    for(i = 0; i < a.size; i++)
    {
        if(a[i].classInfo != b[i].classInfo)
            return SM_FALSE;

        if(a[i].fromAttrKind != b[i].fromAttrKind)
            return SM_FALSE;

        switch(a[i].fromAttrKind)
        {
        case PATHEXPR_KIND_METHOD:
            if(a[i].method.methodInfo != b[i].method.methodInfo)
                return SM_FALSE;

            a_argument = a[i].method.argument;
            b_argument = b[i].method.argument;
            if(!isSameArgument(a_argument, b_argument))
                return SM_FALSE;
            break;

        case PATHEXPR_KIND_ATTR:
            if(a[i].attr.attrInfo != b[i].attr.attrInfo)
                return SM_FALSE;
            break;

        case PATHEXPR_KIND_FUNC: 
            a_func = a[i].func;
            b_func = b[i].func;
            if(!isSameFunction(a_func, b_func))
                return SM_FALSE;
            break;

        case PATHEXPR_KIND_CLASS:
            if(a[i].aliasName != b[i].aliasName)
            {
                // check string size
                if(a[i].aliasName.size != b[i].aliasName.size)
                    return SM_FALSE;

                // check string contents
                a_stringIndex = a[i].aliasName.startIndex;
                b_stringIndex = b[i].aliasName.startIndex;
                for(j = 0;j < a[i].aliasName.size; j++)
                    if(m_pool->stringPool[a_stringIndex + j] != m_pool->stringPool[b_stringIndex + j])
                        return SM_FALSE;
            }

            break;
        case PATHEXPR_KIND_OBJECT:
            a_object = a[i].object;
            b_object = b[i].object;
            if(!isSameObject(a_object, b_object))
                return SM_FALSE;
            break;

        default:
            OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
            break;
        }
    }
    return SM_TRUE;
}

Four OQL_GDStoCommonAP::getTempFileColNo(Four tempFileNum, PathExprPoolElements& pathExpr, Two& tempFileColNo)
{
    AP_TempFileInfoPoolIndex    poolIndex;
    AP_TempFileInfoPoolElements tempFileInfo(m_pool->tempFileInfoPool);
    PathExprPoolElements        tempFilePathExpr(m_pool->pathExprPool);
    Four                        i;
    Four                        e;

    // prepare tempFileInfo to access temp file
    e = getTempFileInfoPoolIndex(tempFileNum, poolIndex);
    OOSQL_CHECK_ERR(e);
    tempFileInfo = poolIndex;

    // search each column of temp file for pathExpr matching with 'pathExpr'
    for(i = 0; i < tempFileInfo.size; i++)
    {
        if(tempFileInfo[i].kind == TFS_KIND_PATHEXPR)
        {
            // check by poolindex
            if(tempFileInfo[i].pathExpr == pathExpr)
                break;  // find

            // check by poolcontents
            tempFilePathExpr = tempFileInfo[i].pathExpr; 
            if(isSamePathExpr(tempFilePathExpr, pathExpr))
                break;  // find
        }
    }

    if(i != tempFileInfo.size)
    {
        tempFileColNo = i;
        return eNOERROR;
    }
    else
        return eNOT_FOUND_OOSQL;
}

Four OQL_GDStoCommonAP::getTempFileColNo(Four tempFileNum, FunctionPoolElements& func, Two& tempFileColNo)
{
    AP_TempFileInfoPoolIndex    poolIndex;
    AP_TempFileInfoPoolElements tempFileInfo(m_pool->tempFileInfoPool);
    FunctionPoolElements        tempFileFunc(m_pool->funcPool);
    Four                        i;
    Four                        e;

    // prepare tempFileInfo to access temp file
    e = getTempFileInfoPoolIndex(tempFileNum, poolIndex);
    OOSQL_CHECK_ERR(e);
    tempFileInfo = poolIndex;

    // search each column of temp file for pathExpr matching with 'pathExpr'
    for(i = 0; i < tempFileInfo.size; i++)
    {
        if(tempFileInfo[i].kind == TFS_KIND_FUNC)
        {
            // check by poolindex
            if(tempFileInfo[i].func == func)
                break;  // find

            // check by poolcontents
            tempFileFunc = tempFileInfo[i].func; 
            if(isSameFunction(tempFileFunc, func))
                break;  // find
        }
    }

    if(i != tempFileInfo.size)
    {
        tempFileColNo = i;
        return eNOERROR;
    }
    else
        return eNOT_FOUND_OOSQL;
}

Four OQL_GDStoCommonAP::getTempFileColNo(Four tempFileNum, AggrFuncPoolElements& aggrFunc, Two& tempFileColNo)
{
    AP_TempFileInfoPoolIndex    poolIndex;
    AP_TempFileInfoPoolElements tempFileInfo(m_pool->tempFileInfoPool);
    AggrFuncPoolElements        tempFileAggrFunc(m_pool->aggrFuncPool);
    Four                        i;
    Four                        e;

    // prepare tempFileInfo to access temp file
    e = getTempFileInfoPoolIndex(tempFileNum, poolIndex);
    OOSQL_CHECK_ERR(e);
    tempFileInfo = poolIndex;

    // search each column of temp file for pathExpr matching with 'pathExpr'
    for(i = 0; i < tempFileInfo.size; i++)
    {
        if(tempFileInfo[i].kind == TFS_KIND_AGGRFUNC)
        {
            // check by poolindex
            if(tempFileInfo[i].aggrFunc == aggrFunc)
                break;  // find

            // check by poolcontents
            tempFileAggrFunc = tempFileInfo[i].aggrFunc; 
            if(isSameAggrFunc(tempFileAggrFunc, aggrFunc))
                break;  // find
        }
    }

    if(i != tempFileInfo.size)
    {
        tempFileColNo = i;
        return eNOERROR;
    }
    else
        return eNOT_FOUND_OOSQL;
}

Four OQL_GDStoCommonAP::getAggrFuncIndex(AggrFuncPoolElements& aggrFunc, Two& aggrFuncIndex)
{
    CommonAP_PoolElements   apNode(m_pool->commonAP_Pool);
    AP_AggrFuncPoolElements aggrFuncInfo(m_pool->ap_aggrFuncPool);
    AggrFuncPoolElements    srcAggrFunc(m_pool->aggrFuncPool);
    PathExprPoolElements    pathExpr(m_pool->pathExprPool);
    ArgumentPoolElements    argument(m_pool->argumentPool);
    ArgumentPoolElements    srcArgument(m_pool->argumentPool);
    AP_ArgumentPoolElements ap_argument(m_pool->ap_argumentPool);
    Four                    i;
    Four                    e;
    Four                    lastElemIndex;
    
    argument     = aggrFunc[0].argument;
    apNode       = m_aggrFuncInfo_planNo;
    aggrFuncInfo = apNode[0].aggrFuncInfo;

    for(i = 0; i < aggrFuncInfo.size; i++)
    {
        if(aggrFuncInfo[i].srcAggrFunc == aggrFunc)
            break;      // found
        
        srcAggrFunc = aggrFuncInfo[i].srcAggrFunc;
        srcArgument = srcAggrFunc[0].argument;

        if(srcAggrFunc[0].aggrFunctionID != aggrFunc[0].aggrFunctionID)
            continue;
        if(srcAggrFunc[0].distinctFlag != aggrFunc[0].distinctFlag)
            continue;

        if(!isSameArgument(srcArgument, argument))
            continue;

        break;          // found
    }
    
    if(i != aggrFuncInfo.size)
    {
        aggrFuncIndex = i;
        return eNOERROR;
    }
    
    return eNOT_FOUND_OOSQL;
}

Four OQL_GDStoCommonAP::getNewTempFileNum()
{
    Four    num;

    if(m_tempFileTop > MAX_TEMP_FILES)
        OOSQL_ERR(eUNHANDLED_CASE_OOSQL);

    num = m_tempFileTop;
    m_tempFileTop ++;

    return num;
}

Four OQL_GDStoCommonAP::getTempFileInfoPoolIndex(Four num, AP_TempFileInfoPoolIndex& poolIndex)
{
    poolIndex = m_tempFiles[num].tempFileInfo;
    return eNOERROR;
}

Four OQL_GDStoCommonAP::setTempFileInfoPoolIndex(Four num, AP_TempFileInfoPoolIndex poolIndex)
{
    m_tempFiles[num].tempFileInfo = poolIndex;
    return eNOERROR;
}

Four OQL_GDStoCommonAP::getTempFileNum(TempFileUsage usage, Four pos, Four& tempFileNum)
{
    Four    i;

    for(i = m_tempFileTop; i >= 0; i--)
    {
        if(m_tempFiles[i].usage == usage && m_tempFiles[i].pos == pos)
        {
            tempFileNum = i;
            return eNOERROR;
        }
    }

    return eNOT_FOUND_OOSQL;
}

Four OQL_GDStoCommonAP::setTempFileUsage(Four tempFileNum, TempFileUsage usage, Four pos)
{
    m_tempFiles[tempFileNum].usage = usage;
    m_tempFiles[tempFileNum].pos   = pos;

    return eNOERROR;
}

Four OQL_GDStoCommonAP::makePlan_constructTempFileInfo(AP_TempFileInfoElement& tempFileInfo, AP_ProjectionElement& projectionInfo)
{
	if(projectionInfo.resultType == TYPEID_TEXT)		
	{													
		tempFileInfo.typeId   = TYPEID_VARSTRING;		
#ifndef SUPPORT_LARGE_DATABASE2
		tempFileInfo.length   = INT_MAX;				
#else
		tempFileInfo.length   = LONG_MAX;				
#endif

	}
	else
	{
		tempFileInfo.typeId   = projectionInfo.resultType;
		tempFileInfo.length   = projectionInfo.resultLength;
	}
	tempFileInfo.kind     = TFS_KIND_NONE;

	return eNOERROR;
}

Four OQL_GDStoCommonAP::makePlan_constructTempFileInfo(AP_TempFileInfoElement& tempFileInfo, AP_ProjectionElement& projectionInfo, OrderByListElement& orderBy)
{
    switch(projectionInfo.projectionKind)
    {
    case PROJECTION_KIND_PATHEXPR:
		if(projectionInfo.resultType == TYPEID_TEXT)		
		{
			tempFileInfo.typeId   = TYPEID_VARSTRING;
#ifndef SUPPORT_LARGE_DATABASE2
			tempFileInfo.length   = INT_MAX;
#else                                                       
			tempFileInfo.length   = LONG_MAX;
#endif
		}
		else
		{
			tempFileInfo.typeId   = projectionInfo.resultType;
			tempFileInfo.length   = projectionInfo.resultLength;
		}
        tempFileInfo.kind     = TFS_KIND_PATHEXPR;
        tempFileInfo.pathExpr = orderBy.pathExpr;
        break;

    case PROJECTION_KIND_AGGRFUNCRESULT:
        tempFileInfo.typeId   = projectionInfo.resultType;
        tempFileInfo.length   = projectionInfo.resultLength;
        tempFileInfo.kind     = TFS_KIND_AGGRFUNC;
        tempFileInfo.aggrFunc = orderBy.aggrFunc;
        break;

	case PROJECTION_KIND_FUNCRESULT:
        tempFileInfo.typeId   = projectionInfo.resultType;
        tempFileInfo.length   = projectionInfo.resultLength;
        tempFileInfo.kind     = TFS_KIND_FUNC;
        tempFileInfo.func     = orderBy.func;
		break;

    case PROJECTION_KIND_FUNCEVAL:
    case PROJECTION_KIND_VALUE:
    case PROJECTION_KIND_OPER:
        OOSQL_ERR(eNOTIMPLEMENTED_OOSQL);
        break;

    default:
        OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
        break;
    }

    return eNOERROR;
}

Four OQL_GDStoCommonAP::makePlan_constructTempFileInfo(AP_TempFileInfoElement& tempFileInfo, AP_ProjectionElement& projectionInfo, GroupByListElement& groupBy)
{
    switch(projectionInfo.projectionKind)
    {
    case PROJECTION_KIND_PATHEXPR:
		if(projectionInfo.resultType == TYPEID_TEXT)		
		{
			tempFileInfo.typeId   = TYPEID_VARSTRING;
#ifndef SUPPORT_LARGE_DATABASE2
			tempFileInfo.length   = INT_MAX;
#else       
			tempFileInfo.length   = LONG_MAX;
#endif
		}
		else
		{
			tempFileInfo.typeId   = projectionInfo.resultType;
			tempFileInfo.length   = projectionInfo.resultLength;
		}
        tempFileInfo.kind     = TFS_KIND_PATHEXPR;
        tempFileInfo.pathExpr = groupBy.pathExpr;
        break;

    case PROJECTION_KIND_AGGRFUNCRESULT:
        OOSQL_ERR(eINTERNALERROR_OOSQL);
        break;

    case PROJECTION_KIND_FUNCRESULT:
        OOSQL_ERR(eINTERNALERROR_OOSQL);
        break;

    case PROJECTION_KIND_FUNCEVAL:
    case PROJECTION_KIND_VALUE:
    case PROJECTION_KIND_OPER:
        OOSQL_ERR(eNOTIMPLEMENTED_OOSQL);
        break;

    default:
        OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
        break;
    }

    return eNOERROR;
}

Four OQL_GDStoCommonAP::makePlan_constructTempFileInfo(AP_TempFileInfoElement& tempFileInfo, AP_ProjectionElement& projectionInfo, SelListElement& selList)
{
    switch(projectionInfo.projectionKind)
    {
    case PROJECTION_KIND_PATHEXPR:
		if(projectionInfo.resultType == TYPEID_TEXT)		
		{
			tempFileInfo.typeId   = TYPEID_VARSTRING;
#ifndef SUPPORT_LARGE_DATABASE2
	        tempFileInfo.length   = INT_MAX;
#else                                                       
	        tempFileInfo.length   = LONG_MAX;
#endif
		}
		else
		{
			tempFileInfo.typeId   = projectionInfo.resultType;
			tempFileInfo.length   = projectionInfo.resultLength;
		}
        tempFileInfo.kind     = TFS_KIND_PATHEXPR;
        tempFileInfo.pathExpr = selList.pathExpr;
        break;


    case PROJECTION_KIND_AGGRFUNCRESULT:
        tempFileInfo.typeId   = projectionInfo.resultType;
        tempFileInfo.length   = projectionInfo.resultLength;
        tempFileInfo.kind     = TFS_KIND_AGGRFUNC;
        tempFileInfo.aggrFunc = selList.aggrFunc;
        break;

    case PROJECTION_KIND_FUNCRESULT:
        tempFileInfo.typeId   = projectionInfo.resultType;
        tempFileInfo.length   = projectionInfo.resultLength;
        tempFileInfo.kind     = TFS_KIND_FUNC;
        tempFileInfo.func     = selList.func;
		break;

    case PROJECTION_KIND_FUNCEVAL:
    case PROJECTION_KIND_VALUE:
    case PROJECTION_KIND_OPER:
        OOSQL_ERR(eNOTIMPLEMENTED_OOSQL);
        break;

    default:
        OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
        break;
    }

    return eNOERROR;
}

Four OQL_GDStoCommonAP::makePlan_constructTempFileInfo(AP_TempFileInfoElement& tempFileInfo, AP_ProjectionElement& projectionInfo, ArgumentElement& argument)
{
    switch(projectionInfo.projectionKind)
    {
    case PROJECTION_KIND_PATHEXPR:
		if(projectionInfo.resultType == TYPEID_TEXT)		
		{
			tempFileInfo.typeId   = TYPEID_VARSTRING;
#ifndef SUPPORT_LARGE_DATABASE2
	        tempFileInfo.length   = INT_MAX;
#else                                                       
	        tempFileInfo.length   = LONG_MAX;
#endif
		}
		else
		{
			tempFileInfo.typeId   = projectionInfo.resultType;
			tempFileInfo.length   = projectionInfo.resultLength;
		}
        tempFileInfo.kind     = TFS_KIND_PATHEXPR;
        tempFileInfo.pathExpr = argument.pathExpr;
        break;

    case PROJECTION_KIND_AGGRFUNCRESULT:
        tempFileInfo.typeId   = projectionInfo.resultType;
        tempFileInfo.length   = projectionInfo.resultLength;
        tempFileInfo.kind     = TFS_KIND_AGGRFUNC;
        tempFileInfo.aggrFunc = argument.aggrFunc;
        break;

    case PROJECTION_KIND_FUNCRESULT:
    case PROJECTION_KIND_FUNCEVAL:
    case PROJECTION_KIND_VALUE:
    case PROJECTION_KIND_OPER:
        OOSQL_ERR(eNOTIMPLEMENTED_OOSQL);
        break;

    default:
        OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
        break;
    }

    return eNOERROR;
}

Four OQL_GDStoCommonAP::makePlan_constructStarToOrQueryInTextIrSubPlan(CataClassInfo classInfo, CataAttrInfo attrInfo, CataIndexInfo indexInfo, AP_TextIndexSubPlan& subPlan, Four irScanDirection)
{
	Four	e;

	// traverse text sub plan and convert star query into or a group of OR queries
	e = traverseIrSubPlanAndConvertStarToOrQuery(classInfo, attrInfo, indexInfo, subPlan.textIndexCond, irScanDirection);
	OOSQL_CHECK_ERR(e);

	return eNOERROR;
}

Four OQL_GDStoCommonAP::makePlan_constructTextIrSubPlan(CataClassInfo classInfo, CataAttrInfo attrInfo, CataIndexInfo indexInfo, 
														ExprPoolIndex exprPoolIndex, AP_TextIndexSubPlan& subPlan, Four irScanDirection, const OOSQL_StorageManager::InvertedIndexDesc& invertedIndexKeyDesc)
{
    Four	e;

    // traverse expr tree and make text sub plan
    e = traverseExprAndMakeTextIndexConds(classInfo, attrInfo, indexInfo, exprPoolIndex, subPlan.textIndexCond, OP_NONE, irScanDirection, invertedIndexKeyDesc);
    OOSQL_CHECK_ERR(e);

    // subPlan.matchFuncNum values setted in caller

    return eNOERROR;
}

Four OQL_GDStoCommonAP::reverseString(SimpleString& string)
{
    Four i;
    Four length;
    Four tempChar;

    length = string.length();
    // reverse escape string and hangul
    for(i = 0; i < length - 1; i++)
    {
        if(string[i] == '\\')
        {
            // swap(string[i], string[i + 1]);
            tempChar = string[i];
            string[i] = string[i + 1];
            string[i + 1] = tempChar;
            i++;
        }
    }

    for(i = 0; i < length / 2; i++)
    {
        // swap(string[i], string[length - i]);
        tempChar = string[i];
        string[i] = string[length - 1 - i];
        string[length - 1 - i] = tempChar;
    }


    return eNOERROR;
}

static Four isWildChar(char c)
{
    if(c == '*' || c == '%' || c == '?' || c == '_')
        return SM_TRUE;
    else
        return SM_FALSE;
}

Four OQL_GDStoCommonAP::makePlan_constructTextIndexStartBound(SimpleString& string, StringPoolElements& startBound)
{
    Four    i;
    Four    index;
    Four    boundStringSize;

    for(index = 0, i = 0; i < string.length(); i++)
    {
        if(string[i] == '\\')   // escape character
        {
            i++;                // skip one character
            startBound[index] = string[i];          index ++;
        }
        else if(string[i] == '?' || string[i] == '_')
        {
            startBound[index] = (char)0;         index ++;
            startBound[index] = (char)0;         index ++;  
        }
        else if(string[i] == '*' || string[i] == '%')
        {
            break;
        }
        else
        {
            startBound[index] = string[i];          index ++;
        }
    }

    // resize bound string size
    if(index < startBound.size)
    {
        boundStringSize = index;

        // fill default value
        for(; index < startBound.size; index++)
            startBound[index] = (char)0;

        startBound.size = boundStringSize;
    }

    return eNOERROR;
}

Four OQL_GDStoCommonAP::makePlan_constructTextIndexStopBound(SimpleString& string, StringPoolElements& stopBound)
{
    Four    i;
    Four    index;
    Four    boundStringSize;

    for(index = 0, i = 0; i < string.length(); i++)
    {
        if(string[i] == '\\')   // escape character
        {
            i++;                // skip one character
            stopBound[index] = string[i];           index ++;
        }
        else if(string[i] == '?' || string[i] == '_')
        {
            stopBound[index] = (char)0xff;          index ++;
            stopBound[index] = (char)0xff;          index ++;
        }
        else if(string[i] == '*' || string[i] == '%')
        {
            for(; index < stopBound.size; index++)
                stopBound[index] = (char)0xff;
        }
        else
        {
            stopBound[index] = string[i];           index ++;
        }
    }

    // resize bound string size
    if(index < stopBound.size)
    {
        boundStringSize = index;

        for(; index < stopBound.size; index++)
            stopBound[index] = (char)0;

        stopBound.size = boundStringSize;
    }

    return eNOERROR;
}

static Four isCase0(SimpleString& string)
{
    Four    i;

    for(i = 0; i < string.length(); i++)
    {
        if(string[i] == '\\')   // escape character
        {
            i++;                // skip one character
            continue;
        }

        if(isWildChar(string[i]))
            break;              // not a case 0
    }

    if(i == string.length())
        return SM_TRUE;
    else
        return SM_FALSE;
}

static Four isCase1(SimpleString& string)
{
    Four    i;
	Four	length;

	if(string.length() == 1 && isWildChar(string[0]))	// *
		return SM_TRUE;

    for(i = 0; i < string.length(); i++)
    {
        if(string[i] == '\\')   // escape character
        {
            i++;                // skip one character
            continue;
        }

        if(isWildChar(string[i]))
            break;              
    }

    if(i == 0)                  // *keyord
        return SM_FALSE;

    for(;i < string.length(); i++)
    {
        if(!isWildChar(string[i]))
            break;              // not a case 1
    }

    if(i == string.length())
        return SM_TRUE;
    else
        return SM_FALSE;
}

static Four isCase2(SimpleString& string)
{
    Four    i;

    for(i = 0; i < string.length(); i++)
    {
        if(!isWildChar(string[i]))
            break;              
    }

    if(i == 0)                  // keyord
        return SM_FALSE;

    for(;i < string.length(); i++)
    {
        if(string[i] == '\\')   // escape character
        {
            i++;                // skip one character
            continue;
        }

        if(isWildChar(string[i]))
            break;              
    }

    if(i == string.length())
        return SM_TRUE;
    else
        return SM_FALSE;
}

static Four isCase3(SimpleString& string)
{
    Four    i;

    for(i = 0; i < string.length(); i++)
    {
        if(string[i] == '\\')   // escape character
        {
            i++;                // skip one character
            continue;
        }

        if(isWildChar(string[i]))
            break;              
    }

    if(i == 0 || i == string.length())  // *keyord or keyword*
        return SM_FALSE;

    if(isWildChar(string[string.length() - 1]))
        return SM_TRUE;
    else
        return SM_FALSE;
}

static Four isCase4(SimpleString& string)
{
    Four    i;

    for(i = string.length() - 1; i >= 0 ; i--)
    {
        if(isWildChar(string[i]))
        {   
            if(i > 0 && string[i - 1] == '\\')  // escape character, skip
                continue;
            break;                              // wild character, stop
        }
    }

    if(i == string.length() - 1 || i == -1) // keyord* or *keyword
        return SM_FALSE;

    if(isWildChar(string[0]))
        return SM_TRUE;
    else
        return SM_FALSE;
}

static Four isCase5(SimpleString& string)
{
    Four    i;
    Four    keyword1_length;
    Four    keyword2_length;

    keyword1_length = 0;
    keyword2_length = 0;

    for(i = 0; i < string.length(); i++)
    {
        if(isWildChar(string[i]))
        {   
            if(!(i > 0 && string[i - 1] == '\\'))
                break;                          
        }
        keyword1_length ++;
    }
    if(i == string.length())
        return SM_FALSE;

    for(i = string.length() - 1; i >= 0; i--)
    {
        if(isWildChar(string[i]))
        {   
            if(!(i > 0 && string[i - 1] == '\\'))
                break;                              
        }
        keyword2_length ++;
    }
    if(i == -1)
        return SM_FALSE;

    if(keyword1_length >= keyword2_length)
        return SM_TRUE;
    else
        return SM_FALSE;
}

static Four isCase6(SimpleString& string)
{
    Four    i;
    Four    keyword1_length;
    Four    keyword2_length;

    keyword1_length = 0;
    keyword2_length = 0;

    for(i = 0; i < string.length(); i++)
    {
        if(isWildChar(string[i]))
        {   
            if(!(i > 0 && string[i - 1] == '\\'))
                break;                          
        }
        keyword1_length ++;
    }
    if(i == string.length())
        return SM_FALSE;

    for(i = string.length() - 1; i >= 0; i--)
    {
        if(isWildChar(string[i]))
        {   
            if(!(i > 0 && string[i - 1] == '\\'))
                break;                              
        }
        keyword2_length ++;
    }
    if(i == -1)
        return SM_FALSE;

    if(keyword1_length < keyword2_length)
        return SM_TRUE;
    else
        return SM_FALSE;
}

Four OQL_GDStoCommonAP::makePlan_getCaseOfTextIndex(SimpleString& string)
{
    // check if case 0 ?
    if(isCase0(string))
        return 0;
    else if(isCase1(string))
        return 1;
    else if(isCase2(string))
        return 2;
    else if(isCase3(string))
        return 3;
    else if(isCase4(string))
        return 4;
    else if(isCase5(string))
        return 5;
    else if(isCase6(string))
        return 6;
    return 7;
}

Four OQL_GDStoCommonAP::makePlan_constructTextIndexKeywordInfo(CataClassInfo classInfo, CataAttrInfo attrInfo, CataIndexInfo indexInfo, 
															   AP_StringPoolIndex stringPoolIndex, AP_TextIndexKeywordInfo& keywordInfo)
{
	SimpleString        string;
	Four				e;

    e = getStringFromStringPool(stringPoolIndex, string);
    OOSQL_CHECK_ERR(e);

	e = makePlan_constructTextIndexKeywordInfo(classInfo, attrInfo, indexInfo, string, keywordInfo);
	OOSQL_CHECK_ERR(e);

	return eNOERROR;
}

Four OQL_GDStoCommonAP::makePlan_constructTextIndexKeywordInfo(CataClassInfo classInfo, CataAttrInfo attrInfo, CataIndexInfo indexInfo, 
															   SimpleString &string, AP_TextIndexKeywordInfo& keywordInfo)
{
    //  whild character  : *, %, ?, _
    //  escape character : \
    //   case 0 : keyword
    //   case 1 : keyword*
    //   case 2 : *keyword
    //   case 3 : keyword1*keyword2*
    //   case 4 : *keyword1*keyword2
    //   case 5 : keyword1*keyword2, strlen(keyword1) >= strlen(keyword2)
    //   case 6 : keyword1*keyword2, strlen(keyword1) <  strlen(keyword2)
    //   case 7 : otherwise
    StringPoolElements  startBound(m_pool->stringPool);
    StringPoolElements  stopBound(m_pool->stringPool);
    StringPoolElements  keywordWithWildChar(m_pool->stringPool);
    Four                i;
    Four                e;

	keywordInfo.nPostings                       = NIL;
	keywordInfo.invertedIndexEntryTupleID.volNo = NIL;

    switch(makePlan_getCaseOfTextIndex(string))
    {
    case 0: // case 0 : keyword
        keywordInfo.accessMethod = KEYWORD_IDX_SCAN;
    
        startBound = m_pool->stringPool.addNewEntry(MAXKEYWORDSIZE);
        stopBound  = m_pool->stringPool.addNewEntry(MAXKEYWORDSIZE);
        keywordWithWildChar.setNull();
		keywordInfo.keywordWithWildChar.setNull();

        // make bound condition
        e = makePlan_constructTextIndexStartBound(string, startBound);
        OOSQL_CHECK_ERR(e);

        e = makePlan_constructTextIndexStopBound(string, stopBound);
        OOSQL_CHECK_ERR(e);

        keywordInfo.startBound = startBound.getPoolIndex();
        keywordInfo.stopBound  = stopBound.getPoolIndex();
        break;

    case 1: // case 1 : keyword*
        keywordInfo.accessMethod = KEYWORD_IDX_SCAN;

        startBound = m_pool->stringPool.addNewEntry(MAXKEYWORDSIZE);
        stopBound  = m_pool->stringPool.addNewEntry(MAXKEYWORDSIZE);
        keywordWithWildChar.setNull();
		keywordInfo.keywordWithWildChar.setNull();

        // make bound condition
        e = makePlan_constructTextIndexStartBound(string, startBound);
        OOSQL_CHECK_ERR(e);

        e = makePlan_constructTextIndexStopBound(string, stopBound);
        OOSQL_CHECK_ERR(e);

        keywordInfo.startBound = startBound.getPoolIndex();
        keywordInfo.stopBound  = stopBound.getPoolIndex();
		keywordInfo.nPostings  = NIL;

        break;

    case 2: // case 2 : *keyword
        keywordInfo.accessMethod = REVERSEKEYWORD_IDX_SCAN;

        startBound = m_pool->stringPool.addNewEntry(MAXKEYWORDSIZE);
        stopBound  = m_pool->stringPool.addNewEntry(MAXKEYWORDSIZE);
        keywordWithWildChar.setNull();
		keywordInfo.keywordWithWildChar.setNull();
        
        // reverse string
        e = reverseString(string);
        OOSQL_CHECK_ERR(e);

        // make bound condition
        e = makePlan_constructTextIndexStartBound(string, startBound);
        OOSQL_CHECK_ERR(e);

        e = makePlan_constructTextIndexStopBound(string, stopBound);
        OOSQL_CHECK_ERR(e);

        keywordInfo.startBound = startBound.getPoolIndex();
        keywordInfo.stopBound  = stopBound.getPoolIndex();
		keywordInfo.nPostings  = NIL;

        break;

    case 3: // case 3 : keyword1*keyword2*
        keywordInfo.accessMethod = KEYWORD_IDX_SCAN;

        startBound = m_pool->stringPool.addNewEntry(MAXKEYWORDSIZE);
        stopBound  = m_pool->stringPool.addNewEntry(MAXKEYWORDSIZE);
        keywordWithWildChar = keywordInfo.keywordWithWildChar = m_pool->stringPool.addNewEntry(MAXKEYWORDSIZE);

        // make bound condition
        e = makePlan_constructTextIndexStartBound(string, startBound);
        OOSQL_CHECK_ERR(e);

        e = makePlan_constructTextIndexStopBound(string, stopBound);
        OOSQL_CHECK_ERR(e);

        keywordInfo.startBound = startBound.getPoolIndex();
        keywordInfo.stopBound  = stopBound.getPoolIndex();
		keywordInfo.nPostings  = NIL;

        break;

    case 4: // case 4 : *keyword1*keyword2
        keywordInfo.accessMethod = REVERSEKEYWORD_IDX_SCAN;

        startBound = m_pool->stringPool.addNewEntry(MAXKEYWORDSIZE);
        stopBound  = m_pool->stringPool.addNewEntry(MAXKEYWORDSIZE);
        keywordWithWildChar = keywordInfo.keywordWithWildChar = m_pool->stringPool.addNewEntry(MAXKEYWORDSIZE);

        // reverse string
        e = reverseString(string);
        OOSQL_CHECK_ERR(e);

        // make bound condition
        e = makePlan_constructTextIndexStartBound(string, startBound);
        OOSQL_CHECK_ERR(e);

        e = makePlan_constructTextIndexStopBound(string, stopBound);
        OOSQL_CHECK_ERR(e);

        keywordInfo.startBound = startBound.getPoolIndex();
        keywordInfo.stopBound  = stopBound.getPoolIndex();
		keywordInfo.nPostings  = NIL;

        break;

    case 5: // case 5 : keyword1*keyword2, strlen(keyword1) >= strlen(keyword2)
        keywordInfo.accessMethod = KEYWORD_IDX_SCAN;

        startBound = m_pool->stringPool.addNewEntry(MAXKEYWORDSIZE);
        stopBound  = m_pool->stringPool.addNewEntry(MAXKEYWORDSIZE);
        keywordWithWildChar = keywordInfo.keywordWithWildChar = m_pool->stringPool.addNewEntry(MAXKEYWORDSIZE);

        // make bound condition
        e = makePlan_constructTextIndexStartBound(string, startBound);
        OOSQL_CHECK_ERR(e);

        e = makePlan_constructTextIndexStopBound(string, stopBound);
        OOSQL_CHECK_ERR(e);

        keywordInfo.startBound = startBound.getPoolIndex();
        keywordInfo.stopBound  = stopBound.getPoolIndex();
		keywordInfo.nPostings  = NIL;

        break;

    case 6: // case 6 : keyword1*keyword2, strlen(keyword1) < strlen(keyword2)
        keywordInfo.accessMethod = REVERSEKEYWORD_IDX_SCAN;

        startBound = m_pool->stringPool.addNewEntry(MAXKEYWORDSIZE);
        stopBound  = m_pool->stringPool.addNewEntry(MAXKEYWORDSIZE);
        keywordWithWildChar = keywordInfo.keywordWithWildChar = m_pool->stringPool.addNewEntry(MAXKEYWORDSIZE);

        // reverse string
        e = reverseString(string);
        OOSQL_CHECK_ERR(e);

        // make bound condition
        e = makePlan_constructTextIndexStartBound(string, startBound);
        OOSQL_CHECK_ERR(e);

        e = makePlan_constructTextIndexStopBound(string, stopBound);
        OOSQL_CHECK_ERR(e);

        keywordInfo.startBound = startBound.getPoolIndex();
        keywordInfo.stopBound  = stopBound.getPoolIndex();
		keywordInfo.nPostings  = NIL;

        break;

    case 7: // case 7 : otherwise
        keywordInfo.accessMethod = KEYWORD_SEQ_SCAN;
        
        startBound.setNull();
        stopBound.setNull();
        keywordWithWildChar = keywordInfo.keywordWithWildChar = m_pool->stringPool.addNewEntry(MAXKEYWORDSIZE);

        keywordInfo.startBound = startBound.getPoolIndex();
        keywordInfo.stopBound  = stopBound.getPoolIndex();
		keywordInfo.nPostings  = NIL;

        break;

    default:
        OOSQL_ERR(eINTERNALERROR_OOSQL);
    }

	if(keywordWithWildChar != NULL_POOLINDEX)
	{
		for(i = 0; i < string.length(); i++)
			keywordWithWildChar[i] = string[i];

		for(; i < keywordWithWildChar.size; i++)
			keywordWithWildChar[i] = 0;
	}

    return eNOERROR;
}

Four OQL_GDStoCommonAP::traverseIrSubPlanAndConvertStarToOrQuery(CataClassInfo classInfo, CataAttrInfo attrInfo,
																 CataIndexInfo indexInfo, 
																 AP_TextIndexCondPoolIndex& textIndexCondPoolIndex,
																 Four irScanDirection)
{
	Four							e;
	AP_TextIndexCondPoolElements    textIndexCond(m_pool->textIndexCondPool);
	AP_TextIndexCondPoolIndex		subTextIndexCondPoolIndex;
	AP_TextIndexCondPoolElements	subTextIndexCond(m_pool->textIndexCondPool);

	textIndexCond = textIndexCondPoolIndex;

	switch(textIndexCond[0].nodeKind)
	{
	case TEXTINDEXCOND_OPERATOR:
		if(!textIndexCond[0].oper.op1.isNull())
		{
			e = traverseIrSubPlanAndConvertStarToOrQuery(classInfo, attrInfo, indexInfo, textIndexCond[0].oper.op1, irScanDirection);
			OOSQL_CHECK_ERR(e);
		}
		if(!textIndexCond[0].oper.op2.isNull())
		{
			e = traverseIrSubPlanAndConvertStarToOrQuery(classInfo, attrInfo, indexInfo, textIndexCond[0].oper.op2, irScanDirection);
			OOSQL_CHECK_ERR(e);
		}
		if(!textIndexCond[0].oper.op3.isNull())
		{
			e = traverseIrSubPlanAndConvertStarToOrQuery(classInfo, attrInfo, indexInfo, textIndexCond[0].oper.op3, irScanDirection);
			OOSQL_CHECK_ERR(e);
		}
		break;

	case TEXTINDEXCOND_KEYWORD:
		if(!isEqualString(textIndexCond[0].keyword.startBound, textIndexCond[0].keyword.stopBound))
		{
			e = makePlan_ConstructOrQueryTextIrSubPlan(classInfo, attrInfo, indexInfo, 
				                                       textIndexCond[0].keyword,
													   subTextIndexCondPoolIndex,
													   irScanDirection);
			OOSQL_CHECK_ERR(e);

			subTextIndexCond = subTextIndexCondPoolIndex;
			textIndexCond[0] = subTextIndexCond[0];
		}
		break;

	default:
		break;
	}

	return eNOERROR;
}

Four OQL_GDStoCommonAP::makePlan_ConstructOrQueryTextIrSubPlan(CataClassInfo classInfo, CataAttrInfo attrInfo, CataIndexInfo indexInfo, const AP_TextIndexKeywordInfo& keywordInfo, AP_TextIndexCondPoolIndex& textIndexCondPoolIndex, Four irScanDirection)
{
	SimpleString					startKeyword;
	SimpleString					stopKeyword;
	Four							e;
	Four							scanId;
	Four							nPostings;
	SimpleString					keyword, prevKeyword;
	AP_TextIndexCondPoolElements    textIndexCond(m_pool->textIndexCondPool);
	Four							nKeywords;
	AP_StringPoolIndex				startBoundPoolIndex;
	AP_StringPoolIndex				stopBoundPoolIndex;
	OOSQL_StorageManager::TupleID   invertedIndexEntryTupleID;
	Four							keywordKind;
	Four							nPostingsForRangeQuery;
	SimpleString					rangeStartKeyword;
	SimpleString					rangeStopKeyword;
	StringPoolElements				startBound(m_pool->stringPool);
	StringPoolElements				stopBound(m_pool->stringPool);
	Four							i;
	Four							eos;
	OOSQL_StorageManager::InvertedIndexDesc	invertedIndexKeyDesc;

	startBoundPoolIndex = keywordInfo.startBound;
	stopBoundPoolIndex  = keywordInfo.stopBound;

	e = m_catalog->index_GetIndexDesc(classInfo, indexInfo, invertedIndexKeyDesc);
	OOSQL_CHECK_ERR(e);

	e = getStringFromStringPool(startBoundPoolIndex, startKeyword);
	OOSQL_CHECK_ERR(e);

	e = getStringFromStringPool(stopBoundPoolIndex, stopKeyword);
	OOSQL_CHECK_ERR(e);

	if(keywordInfo.accessMethod == REVERSEKEYWORD_IDX_SCAN)
		keywordKind = REVERSEKEYWORD;
	else
		keywordKind = KEYWORD;

	scanId = e = m_catalog->textindex_KeywordScan_Open(classInfo, attrInfo, indexInfo, keywordKind,
													   (Two)startBoundPoolIndex.size, (char*)startKeyword,
													   (Two)stopBoundPoolIndex.size, (char*)stopKeyword);
	OOSQL_CHECK_ERR(e);

	nKeywords = 0;
	textIndexCondPoolIndex.setNull();
	prevKeyword       = "";
	rangeStartKeyword = "";	
	rangeStopKeyword  = "";	
	                        
	while(1)
	{
		e = m_catalog->textindex_KeywordScan_Next(scanId, keywordKind, (char*)keyword, nPostings, invertedIndexEntryTupleID);
		OOSQL_CHECK_ERR(e);

		if(e == EOS)
		{
			keyword   = "";
			nPostings = 0;
			eos       = SM_TRUE;
		}
		else
			eos       = SM_FALSE;

		if(!eos)
		{
			// count number of keywords
			nKeywords ++;

			if(rangeStartKeyword[0] == '\0')
			{
				nPostingsForRangeQuery = nPostings;
				rangeStartKeyword = keyword;
			}
			else
			{
				if(nPostings <= TEXTIR_MINNPOSTINGS_FOR_SUBPLANTERMINAL)
					nPostingsForRangeQuery += nPostings;
			}
		}

		if((eos || nPostings > TEXTIR_MINNPOSTINGS_FOR_SUBPLANTERMINAL || nPostingsForRangeQuery > TEXTIR_MAXNPOSTINGS_FOR_SUBPLANTERMINAL) &&
			rangeStartKeyword[0] != '\0')
		{
			if(e == EOS || nPostings > TEXTIR_MINNPOSTINGS_FOR_SUBPLANTERMINAL)
			{
				rangeStopKeyword = prevKeyword;
			}
			else
			{
				rangeStopKeyword = keyword;
			}

			if(textIndexCondPoolIndex.isNull())
				textIndexCond = textIndexCondPoolIndex = m_pool->textIndexCondPool.addNewEntry();
			else
			{
				textIndexCond = m_pool->textIndexCondPool.addNewEntry();
				textIndexCondPoolIndex.size ++;
			}

			textIndexCond[0].nodeKind = TEXTINDEXCOND_KEYWORD;

			startBound = m_pool->stringPool.addNewEntry(rangeStartKeyword.length());
			stopBound  = m_pool->stringPool.addNewEntry(rangeStopKeyword.length());

			textIndexCond[0].keyword.keywordWithWildChar.setNull();

			// make bound condition
			if(keywordKind == KEYWORD)
			{
				textIndexCond[0].keyword.accessMethod = KEYWORD_IDX_SCAN;
				for(i = 0; i < startBound.size; i++)
					startBound[i] = rangeStartKeyword[i];

				for(i = 0; i < stopBound.size; i++)
					stopBound[i] = rangeStopKeyword[i];
			}
			else
			{
				textIndexCond[0].keyword.accessMethod = REVERSEKEYWORD_IDX_SCAN;
				for(i = 0; i < startBound.size; i++)
					startBound[i] = rangeStartKeyword[startBound.size - i - 1];

				for(i = 0; i < stopBound.size; i++)
					stopBound[i] = rangeStopKeyword[stopBound.size - i - 1];
			}

			textIndexCond[0].keyword.scanDirection = irScanDirection;

			textIndexCond[0].keyword.startBound = startBound.getPoolIndex();
			textIndexCond[0].keyword.stopBound  = stopBound.getPoolIndex();
			textIndexCond[0].keyword.nPostings  = nPostingsForRangeQuery;

			textIndexCond[0].keyword.isPostingContainingTupleID            = invertedIndexKeyDesc.postingInfo.isContainingTupleID;
			textIndexCond[0].keyword.isPostingContainingSentenceAndWordNum = invertedIndexKeyDesc.postingInfo.isContainingSentenceAndWordNum;
			textIndexCond[0].keyword.isPostingContainingByteOffset         = invertedIndexKeyDesc.postingInfo.isContainingByteOffset;
			textIndexCond[0].keyword.keywordWithWildChar                   = keywordInfo.keywordWithWildChar;
			textIndexCond[0].keyword.invertedIndexEntryTupleID.volNo       = NIL;

			rangeStartKeyword = "";
			rangeStopKeyword  = "";
		}

		if(eos) break;

		if(nPostings > TEXTIR_MINNPOSTINGS_FOR_SUBPLANTERMINAL)
		{
			if(textIndexCondPoolIndex.isNull())
				textIndexCond = textIndexCondPoolIndex = m_pool->textIndexCondPool.addNewEntry();
			else
			{
				textIndexCond = m_pool->textIndexCondPool.addNewEntry();
				textIndexCondPoolIndex.size ++;
			}

			textIndexCond[0].nodeKind = TEXTINDEXCOND_KEYWORD;
			e = makePlan_constructTextIndexKeywordInfo(classInfo, attrInfo, indexInfo,
													   keyword, textIndexCond[0].keyword);
			OOSQL_CHECK_ERR(e);

			textIndexCond[0].keyword.scanDirection                         = irScanDirection;

			textIndexCond[0].keyword.isPostingContainingTupleID            = invertedIndexKeyDesc.postingInfo.isContainingTupleID;
			textIndexCond[0].keyword.isPostingContainingSentenceAndWordNum = invertedIndexKeyDesc.postingInfo.isContainingSentenceAndWordNum;
			textIndexCond[0].keyword.isPostingContainingByteOffset         = invertedIndexKeyDesc.postingInfo.isContainingByteOffset;
			textIndexCond[0].keyword.keywordWithWildChar                   = keywordInfo.keywordWithWildChar;
			textIndexCond[0].keyword.nPostings							   = nPostings;
			textIndexCond[0].keyword.invertedIndexEntryTupleID             = invertedIndexEntryTupleID;

			rangeStartKeyword = "";
			rangeStopKeyword  = "";
		}

		prevKeyword = keyword;
	}

	e = m_catalog->textindex_KeywordScan_Close(scanId);
	OOSQL_CHECK_ERR(e);

	if(nKeywords == 0)
	{
		textIndexCond = m_pool->textIndexCondPool.addNewEntry();

		textIndexCond[0].nodeKind = TEXTINDEXCOND_KEYWORD;
		keyword = startKeyword;
		e = makePlan_constructTextIndexKeywordInfo(classInfo, attrInfo, indexInfo,
				                                   keyword, textIndexCond[0].keyword);
		OOSQL_CHECK_ERR(e);

		textIndexCond[0].keyword.scanDirection                         = irScanDirection;

		textIndexCond[0].keyword.isPostingContainingTupleID            = invertedIndexKeyDesc.postingInfo.isContainingTupleID;
		textIndexCond[0].keyword.isPostingContainingSentenceAndWordNum = invertedIndexKeyDesc.postingInfo.isContainingSentenceAndWordNum;
		textIndexCond[0].keyword.isPostingContainingByteOffset         = invertedIndexKeyDesc.postingInfo.isContainingByteOffset;

		textIndexCondPoolIndex = textIndexCond.getPoolIndex();
	}
	else
	{
		while(textIndexCondPoolIndex.size > 1)
		{	
			AP_TextIndexCondPoolElements bottomElements(m_pool->textIndexCondPool);
			AP_TextIndexCondPoolElements buildingElements(m_pool->textIndexCondPool);
			Four						 i;

			bottomElements   = textIndexCondPoolIndex;
			buildingElements = m_pool->textIndexCondPool.addNewEntry(bottomElements.size / 2 + bottomElements.size % 2);

			for(i = 0; i < bottomElements.size / 2; i++)
			{
				buildingElements[i].nodeKind        = TEXTINDEXCOND_OPERATOR;
				buildingElements[i].oper.operatorID = OP_TEXTIR_OR;
				buildingElements[i].oper.op1.setPoolIndex(bottomElements.startIndex + i * 2 + 0, 1);
				buildingElements[i].oper.op2.setPoolIndex(bottomElements.startIndex + i * 2 + 1, 1);
				buildingElements[i].oper.op3.setNull();

				buildingElements[i].oper.scanDirection = irScanDirection;
			}
			if(bottomElements.size % 2)
				buildingElements[i] = bottomElements[bottomElements.size - 1];

			textIndexCondPoolIndex = buildingElements.getPoolIndex();
		}
	}

	return eNOERROR;
}

Four OQL_GDStoCommonAP::traverseExprAndMakeTextIndexConds(
	CataClassInfo									classInfo, 
	CataAttrInfo									attrInfo, 
	CataIndexInfo									indexInfo, 
	ExprPoolIndex									exprPoolIndex, 
	AP_TextIndexCondPoolIndex&						textIndexCondPoolIndex, 
	OperatorID										operatorID,
	Four											irScanDirection,
	const OOSQL_StorageManager::InvertedIndexDesc&	invertedIndexKeyDesc)
{
    ExprPoolElements                expr(m_pool->exprPool);
    AP_TextIndexCondPoolElements    textIndexCond(m_pool->textIndexCondPool);
    AP_TextIndexCondPoolIndex       operand1, operand2, operand3;
    ValuePoolElements               value(m_pool->valuePool);
	ValuePoolElements				value1(m_pool->valuePool);
	ValuePoolElements				value2(m_pool->valuePool);
    Four                            e;
	char							string1[256];
	char							string2[256];
	Four							strCmpResult;
	Four							i;


    // make new node
    expr          = exprPoolIndex;
    textIndexCond = textIndexCondPoolIndex = m_pool->textIndexCondPool.addNewEntry();

    switch(expr[0].exprKind)
    {
    case EXPR_KIND_OPER:
        // evaluate node in pre-order
        switch(expr[0].oper.operatorId)
        {
        case OP_TEXTIR_ACCUMULATE:
        case OP_TEXTIR_OR:
        case OP_TEXTIR_AND:
        case OP_TEXTIR_MINUS:
        case OP_TEXTIR_THRESHOLD:
        case OP_TEXTIR_MULTIPLY:
        case OP_TEXTIR_MAX:
        case OP_TEXTIR_NEAR:
		case OP_TEXTIR_NEAR_WITH_ORDER:
            textIndexCond[0].nodeKind            = TEXTINDEXCOND_OPERATOR;
            textIndexCond[0].oper.operatorID     = expr[0].oper.operatorId;
			textIndexCond[0].oper.scanDirection  = irScanDirection;		
			textIndexCond[0].oper.logicalIdHints = -1;			
            break;
		case OP_TEXTIR_BETWEEN:			// between("a", "b")
			if(!(expr[0].oper.operand1.getElements(m_pool->exprPool, 0).exprKind == EXPR_KIND_VALUE && 
			     expr[0].oper.operand2.getElements(m_pool->exprPool, 0).exprKind == EXPR_KIND_VALUE))
				OOSQL_ERR(eINTERNALERROR_OOSQL);
			value1 = expr[0].oper.operand1.getElements(m_pool->exprPool, 0).value;
			value2 = expr[0].oper.operand2.getElements(m_pool->exprPool, 0).value;

			if(!(value1[0].valueKind == VALUE_KIND_STRING && value2[0].valueKind == VALUE_KIND_STRING))
				OOSQL_ERR(eINTERNALERROR_OOSQL);

			textIndexCond[0].nodeKind = TEXTINDEXCOND_KEYWORD;
			textIndexCond[0].keyword.accessMethod  = KEYWORD_IDX_SCAN;
			textIndexCond[0].keyword.scanDirection = irScanDirection;	
			textIndexCond[0].keyword.keywordWithWildChar.setNull();
			
			if(value1[0].string.size > 256 || value2[0].string.size > 256)
				OOSQL_ERR(eINTERNALERROR_OOSQL);
			
			for (i=0; i < value1[0].string.size; i++)
				string1[i] = m_pool->stringPool[i + value1[0].string.startIndex];

			for (i=0; i < value2[0].string.size; i++)
				string2[i] = m_pool->stringPool[i + value2[0].string.startIndex];

			strCmpResult = strcmp(string1, string2);

			if(strCmpResult > 0)
			{
				// chage the startBound and end stopBound to make startBound is less or equal than stopBound
				textIndexCond[0].keyword.startBound = value2[0].string;
				textIndexCond[0].keyword.startBound.size --;			

				textIndexCond[0].keyword.stopBound  = value1[0].string;
				textIndexCond[0].keyword.stopBound.size --;				
			}
			else
			{
				textIndexCond[0].keyword.startBound = value1[0].string;
				textIndexCond[0].keyword.startBound.size --;		

				textIndexCond[0].keyword.stopBound  = value2[0].string;
				textIndexCond[0].keyword.stopBound.size --;			
			}

			textIndexCond[0].keyword.isPostingContainingTupleID            = invertedIndexKeyDesc.postingInfo.isContainingTupleID;
			textIndexCond[0].keyword.isPostingContainingSentenceAndWordNum = invertedIndexKeyDesc.postingInfo.isContainingSentenceAndWordNum;
			textIndexCond[0].keyword.isPostingContainingByteOffset         = invertedIndexKeyDesc.postingInfo.isContainingByteOffset;
			textIndexCond[0].keyword.nPostings                             = NIL;

			break;
        default:
            OOSQL_ERR(eINTERNALERROR_OOSQL);
        }

		if(expr[0].oper.operatorId != OP_TEXTIR_BETWEEN)
		{
			e = traverseExprAndMakeTextIndexConds(classInfo, attrInfo, indexInfo, expr[0].oper.operand1, operand1, expr[0].oper.operatorId, irScanDirection, invertedIndexKeyDesc);
			OOSQL_CHECK_ERR(e);
			e = traverseExprAndMakeTextIndexConds(classInfo, attrInfo, indexInfo, expr[0].oper.operand2, operand2, expr[0].oper.operatorId, irScanDirection, invertedIndexKeyDesc);
			OOSQL_CHECK_ERR(e);
			if(!expr[0].oper.operand3.isNull())
			{
				e = traverseExprAndMakeTextIndexConds(classInfo, attrInfo, indexInfo, expr[0].oper.operand3, operand3, expr[0].oper.operatorId, irScanDirection, invertedIndexKeyDesc);
				OOSQL_CHECK_ERR(e);
			}
			else
				operand3.setNull();

			textIndexCond[0].oper.op1 = operand1;
			textIndexCond[0].oper.op2 = operand2;
			textIndexCond[0].oper.op3 = operand3;
		}

        break;

    case EXPR_KIND_VALUE:
        value = expr[0].value;

        switch(value[0].valueKind)
        {
        case VALUE_KIND_STRING:
            textIndexCond[0].nodeKind = TEXTINDEXCOND_KEYWORD;

            e = makePlan_constructTextIndexKeywordInfo(classInfo, attrInfo, indexInfo, value[0].string, textIndexCond[0].keyword);
            OOSQL_CHECK_ERR(e);

			textIndexCond[0].keyword.scanDirection                         = irScanDirection;

			textIndexCond[0].keyword.isPostingContainingTupleID            = invertedIndexKeyDesc.postingInfo.isContainingTupleID;
			textIndexCond[0].keyword.isPostingContainingSentenceAndWordNum = invertedIndexKeyDesc.postingInfo.isContainingSentenceAndWordNum;
			textIndexCond[0].keyword.isPostingContainingByteOffset         = invertedIndexKeyDesc.postingInfo.isContainingByteOffset;
            break;

        default:
            textIndexCond[0].nodeKind = TEXTINDEXCOND_CONSTANT;
            textIndexCond[0].constant = value.getPoolIndex();
            break;
        }
        
        break;
    default:
        OOSQL_ERR(eINTERNALERROR_OOSQL);
    }

    return eNOERROR;
}

Four OQL_GDStoCommonAP::setProjection(AP_ProjectionElement& projectionInfo, GroupByListElement& groupBy)
{
    AggrFuncPoolElements aggrFunc(m_pool->aggrFuncPool);
    Two                  aggrFuncIndex;
    Four                 e;

    switch(groupBy.grpByKeyKind)
    {
    case GROUPBY_KIND_PATHEXPR: 
        projectionInfo.projectionKind = PROJECTION_KIND_PATHEXPR;
        e = convertPathExpr_to_PathExprAccessInfo(groupBy.pathExpr, projectionInfo.pathExpr);
        OOSQL_CHECK_ERR(e);
        
        e = getTypeId_and_TypeLength_of_PathExpr(groupBy.pathExpr, 
                                                 projectionInfo.resultType,
                                                 projectionInfo.resultLength);

        OOSQL_CHECK_ERR(e);

        // process exceptional case - for TEXT attribute
        if(projectionInfo.resultType == TYPEID_TEXT)
            projectionInfo.resultLength += TYPEID_OID_SIZE + TYPEID_LONG_SIZE_VAR + TYPEID_SHORT_SIZE_VAR;
        break;

    case GROUPBY_KIND_AGGRFUNC:
        aggrFunc = groupBy.aggrFunc;
        e = getAggrFuncIndex(aggrFunc, aggrFuncIndex);
        OOSQL_CHECK_ERR(e);

        projectionInfo.projectionKind                 = PROJECTION_KIND_AGGRFUNCRESULT;
        projectionInfo.aggrFuncResult.aggrFunctionID  = aggrFunc[0].aggrFunctionID;
        projectionInfo.aggrFuncResult.planNo          = m_aggrFuncInfo_planNo.startIndex;
        projectionInfo.aggrFuncResult.aggrFuncIndex   = aggrFuncIndex;
        break;

    case GROUPBY_KIND_EXPR:
        OOSQL_ERR(eNOTIMPLEMENTED_OOSQL);
        break;

    default:
        OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
        break;
    }

    return eNOERROR;
}

Four OQL_GDStoCommonAP::setProjection(AP_ProjectionElement& projectionInfo, OrderByListElement& orderBy)
{
    Four                    e;
    AggrFuncPoolElements    aggrFunc(m_pool->aggrFuncPool);
    ArgumentPoolElements    argument(m_pool->argumentPool);

    switch(orderBy.ordByKeyKind)
    {
    case ORDERBYLIST_KIND_PATHEXPR:
        projectionInfo.projectionKind = PROJECTION_KIND_PATHEXPR;
        e = convertPathExpr_to_PathExprAccessInfo(orderBy.pathExpr, projectionInfo.pathExpr);
        OOSQL_CHECK_ERR(e);

        e = getTypeId_and_TypeLength_of_PathExpr(orderBy.pathExpr, 
                                                 projectionInfo.resultType,
                                                 projectionInfo.resultLength);
        OOSQL_CHECK_ERR(e);

        // process exceptional case - for TEXT attribute
        if(projectionInfo.resultType == TYPEID_TEXT)
            projectionInfo.resultLength += TYPEID_OID_SIZE + TYPEID_LONG_SIZE_VAR + TYPEID_SHORT_SIZE_VAR;
        break;

    case ORDERBYLIST_KIND_AGGRFUNC:
        if(m_aggrFuncInfo_planNo == NULL_POOLINDEX)
        {
            aggrFunc = orderBy.aggrFunc;
            argument = aggrFunc[0].argument;

            if(argument[0].argumentKind != ARGUMENT_KIND_PATHEXPR)
                OOSQL_ERR(eINTERNALERROR_OOSQL);

            // project aggr function as a pathexpr
            projectionInfo.projectionKind = PROJECTION_KIND_PATHEXPR;
            
            e = convertPathExpr_to_PathExprAccessInfo(argument[0].pathExpr, projectionInfo.pathExpr);
            OOSQL_CHECK_ERR(e);

            e = getTypeId_and_TypeLength_of_PathExpr(argument[0].pathExpr, 
                                                     projectionInfo.resultType,
                                                     projectionInfo.resultLength);
            OOSQL_CHECK_ERR(e);

            // process exceptional case - for TEXT attribute
            if(projectionInfo.resultType == TYPEID_TEXT)
                projectionInfo.resultLength += TYPEID_OID_SIZE + TYPEID_LONG_SIZE_VAR + TYPEID_SHORT_SIZE_VAR;

        }
        else
        {
            Two     aggrFuncIndex;
            Four    planNo;
            Four    colNo;
            Boolean flag;

            // get aggrFuncIndex
            aggrFunc = orderBy.aggrFunc;
			
            e = getAggrFuncIndex(aggrFunc, aggrFuncIndex);
			OOSQL_CHECK_ERR(e);
			
			// check if this aggrFunc is used in previous plan to process GroupBy or OrderBy
			// if so, then use plan no instead of aggrFuncInfo to access aggrFuncInfo
			e = getAggrFuncPlanNo(m_aggrFuncInfo_planNo.startIndex,   // plan no of aggrFuncInfo
					aggrFuncIndex,                    // index of aggrFuncInfo
					planNo,                           // out : planNo
					colNo,                            // out : colNo
					flag);                            // out : true if tempFileNum and colNo are valid
			OOSQL_CHECK_ERR(e);
			
			if(flag)
			{
				// use temp file
				projectionInfo.projectionKind      = PROJECTION_KIND_PATHEXPR;
				projectionInfo.pathExpr.kind       = PATHEXPR_KIND_ATTR;
				projectionInfo.pathExpr.col.planNo = planNo;
				projectionInfo.pathExpr.col.colNo  = colNo;
				
			}
			else
			{
				// use aggrFuncInfo
				projectionInfo.projectionKind                = PROJECTION_KIND_AGGRFUNCRESULT;
				projectionInfo.aggrFuncResult.aggrFunctionID = aggrFunc[0].aggrFunctionID;
				projectionInfo.aggrFuncResult.planNo         = m_aggrFuncInfo_planNo.startIndex;
				projectionInfo.aggrFuncResult.aggrFuncIndex  = aggrFuncIndex;
				
			}
			e = getTypeId_and_TypeLength_of_AggrFunc(aggrFunc.getPoolIndex(),
					projectionInfo.resultType,
					projectionInfo.resultLength);
			OOSQL_CHECK_ERR(e);
		}
		break;

    case ORDERBYLIST_KIND_FUNC:
        OOSQL_ERR(eNOTIMPLEMENTED_OOSQL);
        break;

    default:
        OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
        break;
    }

    return eNOERROR;
}

Four OQL_GDStoCommonAP::setProjection(AP_ProjectionElement& projectionInfo, SelListElement& selList)
{
    Four                    e;
    AggrFuncPoolElements    aggrFunc(m_pool->aggrFuncPool);
    ArgumentPoolElements    argument(m_pool->argumentPool);
    FunctionPoolElements    func(m_pool->funcPool);
    ValuePoolElements       value(m_pool->valuePool);
    ExprPoolElements        operand1(m_pool->exprPool);
    ExprPoolElements        operand2(m_pool->exprPool);
	AP_ExprPoolElements     ap_operand1(m_pool->ap_exprPool);	
    AP_ExprPoolElements     ap_operand2(m_pool->ap_exprPool);	

    switch(selList.selElemKind)
    {
    case SELLIST_KIND_PATHEXPR:
        projectionInfo.projectionKind = PROJECTION_KIND_PATHEXPR;

        e = convertPathExpr_to_PathExprAccessInfo(selList.pathExpr, projectionInfo.pathExpr);
        OOSQL_CHECK_ERR(e);

        e = getTypeId_and_TypeLength_of_PathExpr(selList.pathExpr, 
                                                 projectionInfo.resultType,
                                                 projectionInfo.resultLength);
        OOSQL_CHECK_ERR(e);

        // process exceptional case - for TEXT attribute
        if(projectionInfo.resultType == TYPEID_TEXT)
            projectionInfo.resultLength += TYPEID_OID_SIZE + TYPEID_LONG_SIZE_VAR + TYPEID_SHORT_SIZE_VAR;
        break;

    case SELLIST_KIND_AGGRFUNC:
        if(m_aggrFuncInfo_planNo == NULL_POOLINDEX)
        {
            aggrFunc = selList.aggrFunc;
            argument = aggrFunc[0].argument;

            if(argument[0].argumentKind != ARGUMENT_KIND_PATHEXPR)
                OOSQL_ERR(eINTERNALERROR_OOSQL);

            // project aggr function as a pathexpr
            projectionInfo.projectionKind = PROJECTION_KIND_PATHEXPR;
            
            e = convertPathExpr_to_PathExprAccessInfo(argument[0].pathExpr, projectionInfo.pathExpr);
            OOSQL_CHECK_ERR(e);

            e = getTypeId_and_TypeLength_of_PathExpr(argument[0].pathExpr, 
                                                     projectionInfo.resultType,
                                                     projectionInfo.resultLength);
            OOSQL_CHECK_ERR(e);

            // process exceptional case - for TEXT attribute
            if(projectionInfo.resultType == TYPEID_TEXT)
                projectionInfo.resultLength += TYPEID_OID_SIZE + TYPEID_LONG_SIZE_VAR + TYPEID_SHORT_SIZE_VAR;
        }
        else
        {
            Two     aggrFuncIndex;
            Four    planNo;
            Four    colNo;
            Boolean flag;

            // get aggrFuncIndex
            aggrFunc = selList.aggrFunc;
            e = getAggrFuncIndex(aggrFunc, aggrFuncIndex);
            if(e == eNOERROR)
            {
                // check if this aggrFunc is used in previous plan to process GroupBy or OrderBy
                // if so, then use plan no instead of aggrFuncInfo to access aggrFuncInfo
                e = getAggrFuncPlanNo(m_aggrFuncInfo_planNo.startIndex,   // plan no of aggrFuncInfo
                                      aggrFuncIndex,                    // index of aggrFuncInfo
                                      planNo,                           // out : planNo
                                      colNo,                            // out : colNo
                                      flag);                            // out : true if tempFileNum and colNo are valid
                OOSQL_CHECK_ERR(e);

                if(flag)
                {
                    // use temp file
                    projectionInfo.projectionKind      = PROJECTION_KIND_PATHEXPR;
                    projectionInfo.pathExpr.kind       = PATHEXPR_KIND_ATTR;
                    projectionInfo.pathExpr.col.planNo = planNo;
                    projectionInfo.pathExpr.col.colNo  = colNo;

                }
                else
                {
                    // use aggrFuncInfo
                    projectionInfo.projectionKind                 = PROJECTION_KIND_AGGRFUNCRESULT;
                    projectionInfo.aggrFuncResult.aggrFunctionID  = aggrFunc[0].aggrFunctionID;
                    projectionInfo.aggrFuncResult.planNo          = m_aggrFuncInfo_planNo.startIndex;
                    projectionInfo.aggrFuncResult.aggrFuncIndex   = aggrFuncIndex;
                }
            }
            else
            {
                m_aggrFuncInfo_planNo.startIndex --;
                aggrFunc = selList.aggrFunc;
                e = getAggrFuncIndex(aggrFunc, aggrFuncIndex);
                OOSQL_CHECK_ERR(e);

                projectionInfo.projectionKind                 = PROJECTION_KIND_AGGRFUNCRESULT;
                projectionInfo.aggrFuncResult.aggrFunctionID  = aggrFunc[0].aggrFunctionID;
                projectionInfo.aggrFuncResult.planNo          = m_aggrFuncInfo_planNo.startIndex;
                projectionInfo.aggrFuncResult.aggrFuncIndex   = aggrFuncIndex;

                m_aggrFuncInfo_planNo.startIndex ++;
            }

            e = getTypeId_and_TypeLength_of_AggrFunc(aggrFunc.getPoolIndex(),
                                                     projectionInfo.resultType,
                                                     projectionInfo.resultLength);
            OOSQL_CHECK_ERR(e);
        }
        break;

    case SELLIST_KIND_FUNC:
        func = selList.func;
        if(func[0].functionID == FUNCTION_TEXTIR_WEIGHT)
        {
            projectionInfo.projectionKind = PROJECTION_KIND_FUNCRESULT;

            e = convertFunc_to_FuncResultAccessInfo(selList.func, projectionInfo.funcResult);
            OOSQL_CHECK_ERR(e);
        }
        else
        {
            projectionInfo.projectionKind = PROJECTION_KIND_FUNCEVAL;

            e = convertFunc_to_FuncEvalInfo(selList.func, projectionInfo.funcEval);
            OOSQL_CHECK_ERR(e);
        }

        projectionInfo.resultType   = func[0].returnType;
        projectionInfo.resultLength = func[0].returnLength;
        break;

    case SELLIST_KIND_VALUE:
        value = selList.value;
        switch(value[0].valueKind)
        {
        case VALUE_KIND_STRING:
            projectionInfo.resultType     = TYPEID_STRING;
            projectionInfo.resultLength   = value[0].string.size;
            break;
        case VALUE_KIND_INTEGER:
            projectionInfo.resultType     = TYPEID_INT;
            projectionInfo.resultLength   = TYPEID_INT_SIZE;
            break;
        case VALUE_KIND_REAL:
            projectionInfo.resultType     = TYPEID_FLOAT;
            projectionInfo.resultLength   = TYPEID_FLOAT_SIZE;
            break;
        case VALUE_KIND_BOOL:
            projectionInfo.resultType     = TYPEID_BOOL;
            projectionInfo.resultLength   = TYPEID_BOOL_SIZE;
            break;
        case VALUE_KIND_NIL:
            projectionInfo.resultType     = TYPEID_NIL;
            projectionInfo.resultLength   = TYPEID_NIL_SIZE;
            break;
        case VALUE_KIND_MBR:
            projectionInfo.resultType     = TYPEID_MBR;
            projectionInfo.resultLength   = TYPEID_MBR_SIZE;
            break;
        default:
            OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
            break;  
        }
        projectionInfo.projectionKind = PROJECTION_KIND_VALUE;
        projectionInfo.value          = value.getPoolIndex();
        break;

    case SELLIST_KIND_OPER:
        // get the type of operand1 and operand2
        operand1 = selList.oper.operand1;
        operand2 = selList.oper.operand2;

		if(OP_OGIS_STARTMARKER < selList.oper.operatorId && selList.oper.operatorId < OP_OGIS_ENDMARKER)
		{
			switch(selList.oper.operatorId)
			{
                case OP_OGIS_GEOMETRYFT:
                case OP_OGIS_GEOMETRYFB:
                case OP_OGIS_BOUNDARY:
                case OP_OGIS_DIFFERENCE:
                case OP_OGIS_INTERSECTION:
                case OP_OGIS_SYMDIFFERENCE:
                case OP_OGIS_UNION:
                case OP_OGIS_BUFFER:
                case OP_OGIS_CONVEXHULL:
                case OP_OGIS_GEOMETRYN:
			        projectionInfo.resultType   = TYPEID_OGIS_GEOMETRY;
			        projectionInfo.resultLength = TYPEID_OGIS_GEOMETRY_SIZE;
			        break;

                case OP_OGIS_POINTFT:
                case OP_OGIS_POINTFB:
                case OP_OGIS_CENTRIOD:
                case OP_OGIS_STARTPOINT:
                case OP_OGIS_ENDPOINT:
                case OP_OGIS_POINTONSURFACE:
                case OP_OGIS_POINTN:
			        projectionInfo.resultType   = TYPEID_OGIS_POINT;
			        projectionInfo.resultLength = TYPEID_OGIS_POINT_SIZE;
			        break;

                case OP_OGIS_LINESTRINGFT:
                case OP_OGIS_LINESTRINGFB:
                case OP_OGIS_EXTERIORRING:
                case OP_OGIS_INTERIORRINGN:
			        projectionInfo.resultType   = TYPEID_OGIS_LINESTRING;
			        projectionInfo.resultLength = TYPEID_OGIS_LINESTRING_SIZE;
			        break;

                case OP_OGIS_POLYGONFT:
                case OP_OGIS_POLYGONFB:
                case OP_OGIS_ENVELOPE:
			        projectionInfo.resultType   = TYPEID_OGIS_POLYGON;
			        projectionInfo.resultLength = TYPEID_OGIS_POLYGON_SIZE;
			        break;

                case OP_OGIS_MULTIPOINTFT:
                case OP_OGIS_MULTIPOINTFB:
			        projectionInfo.resultType   = TYPEID_OGIS_MULTIPOINT;
			        projectionInfo.resultLength = TYPEID_OGIS_MULTIPOINT_SIZE;
			        break;

                case OP_OGIS_MULTILINESTRINGFT:
                case OP_OGIS_MULTILINESTRINGFB:
			        projectionInfo.resultType   = TYPEID_OGIS_MULTILINESTRING;
			        projectionInfo.resultLength = TYPEID_OGIS_MULTILINESTRING_SIZE;
			        break;

                case OP_OGIS_MULTIPOLYGONFT:
                case OP_OGIS_MULTIPOLYGONFB:
			        projectionInfo.resultType   = TYPEID_OGIS_MULTIPOLYGON;
			        projectionInfo.resultLength = TYPEID_OGIS_MULTIPOLYGON_SIZE;
			        break;

                case OP_OGIS_GEOMETRYCOLLECTIONFT:
                case OP_OGIS_GEOMETRYCOLLECTIONFB:
			        projectionInfo.resultType   = TYPEID_OGIS_GEOMETRYCOLLECTION;
            		projectionInfo.resultLength = TYPEID_OGIS_GEOMETRYCOLLECTION_SIZE;
			        break;

                case OP_OGIS_ASTEXT:
                case OP_OGIS_ASBINARY:
                case OP_OGIS_GEOMETRYTYPE:
			        projectionInfo.resultType   = TYPEID_STRING;
			        projectionInfo.resultLength = TYPEID_STRING_SIZE;
			        break;

                case OP_OGIS_DIMENSION:
                case OP_OGIS_SRID:
                case OP_OGIS_NUMGEOMETRIES:
                case OP_OGIS_NUMPOINTS:
                case OP_OGIS_NUMINTERIORRINGS:
			        projectionInfo.resultType   = TYPEID_INT;
			        projectionInfo.resultLength = TYPEID_INT_SIZE;
			        break;

                case OP_OGIS_LENGTH:
                case OP_OGIS_X:
                case OP_OGIS_Y:
                case OP_OGIS_AREA:
                case OP_OGIS_DISTANCE:
			        projectionInfo.resultType   = TYPEID_FLOAT;
			        projectionInfo.resultLength = TYPEID_FLOAT_SIZE;
			        break;

                case OP_OGIS_ISEMPTY:
                case OP_OGIS_ISSIMPLE:
                case OP_OGIS_ISCLOSED:
                case OP_OGIS_ISRING:
                case OP_OGIS_CONTAINS:
                case OP_OGIS_CROSSES:
                case OP_OGIS_DISJOINT:
                case OP_OGIS_EQUALS:
                case OP_OGIS_INTERSECTS:
                case OP_OGIS_OVERLAPS:
                case OP_OGIS_RELATED:
                case OP_OGIS_TOUCHES:
                case OP_OGIS_WITHIN:
			        projectionInfo.resultType   = TYPEID_BOOL;
			        projectionInfo.resultLength = TYPEID_BOOL_SIZE;
			        break;
				default:
            		OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
            		break;  
			}
		}
		else
		{
	        if(operand2 != NULL_POOLINDEX)
    	    {
        	    // binary operator
            	e = OQL_ASTtoGDS::determineResultType(operand1[0].resultType, operand2[0].resultType, 
	                                                  projectionInfo.resultType);
    	        OOSQL_CHECK_ERR(e);
        	    projectionInfo.resultLength   = max(operand1[0].resultLength, operand2[0].resultLength);
	        }
    	    else
        	{
            	// unary operator
	            projectionInfo.resultType   = operand1[0].resultType;
    	        projectionInfo.resultLength = operand1[0].resultLength;
        	}
		}

		if(operand1 != NULL_POOLINDEX)
		{
			e = convertExpr_to_AP_Expr(operand1, ap_operand1);
			OOSQL_CHECK_ERR(e);
		}
		else
			ap_operand1 = NULL_POOLINDEX;
		if(operand2 != NULL_POOLINDEX)
		{
			e = convertExpr_to_AP_Expr(operand2, ap_operand2);
			OOSQL_CHECK_ERR(e);
		}
		else
			ap_operand2 = NULL_POOLINDEX;

        projectionInfo.projectionKind = PROJECTION_KIND_OPER;
        projectionInfo.oper.operatorId = selList.oper.operatorId;
		projectionInfo.oper.operand1.setPoolIndex(ap_operand1.startIndex, ap_operand1.size);
		projectionInfo.oper.operand2.setPoolIndex(ap_operand2.startIndex, ap_operand2.size);
		projectionInfo.oper.operatorType = selList.oper.operatorType;
        #ifdef ENABLE_OPENGIS_OPTIMIZATION
        projectionInfo.oper.UDFNo        = selList.oper.UDFNo;
        projectionInfo.oper.isConstant   = selList.oper.isConstant;
        projectionInfo.oper.isEvaluated  = SM_FALSE;
        projectionInfo.oper.result       = m_pool->valuePool.addNewEntry();
        #endif
        break;

    case SELLIST_KIND_CONS:
        OOSQL_ERR(eNOTIMPLEMENTED_OOSQL);
        break;

    default:
        OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
        break;
    }

    return eNOERROR;
}

Four OQL_GDStoCommonAP::setProjection(AP_ProjectionElement& projectionInfo, ArgumentElement &argument)
{
    Four    e;

    switch(argument.argumentKind)
    {
    case ARGUMENT_KIND_PATHEXPR:
        projectionInfo.projectionKind = PROJECTION_KIND_PATHEXPR;
        e = convertPathExpr_to_PathExprAccessInfo(argument.pathExpr, projectionInfo.pathExpr);
        OOSQL_CHECK_ERR(e);

        e = getTypeId_and_TypeLength_of_PathExpr(argument.pathExpr, 
                                                 projectionInfo.resultType,
                                                 projectionInfo.resultLength);
        OOSQL_CHECK_ERR(e);

        // process exceptional case - for TEXT attribute
        if(projectionInfo.resultType == TYPEID_TEXT)
            projectionInfo.resultLength += TYPEID_OID_SIZE + TYPEID_LONG_SIZE_VAR + TYPEID_SHORT_SIZE_VAR;
        break;

    case ARGUMENT_KIND_VALUE:
    case ARGUMENT_KIND_FUNC:
    case ARGUMENT_KIND_DOMAIN:
    case ARGUMENT_KIND_AGGRFUNC:
        OOSQL_ERR(eNOTIMPLEMENTED_OOSQL);
        break;

    case ARGUMENT_KIND_TEMPFILECOL:
    default:
        OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
        break;
    }

    return eNOERROR;
}               

Four OQL_GDStoCommonAP::getTypeId_and_TypeLength_of_PathExpr(PathExprPoolIndex poolIndex, TypeID& typeId, Four& length)
{
    PathExprPoolElements    pathExpr(m_pool->pathExprPool);
    Four                    last;
    Four                    e;

    pathExpr = poolIndex;
    last     = pathExpr.size - 1;

    switch(pathExpr[last].fromAttrKind)
    {
    case PATHEXPR_KIND_METHOD:
        e = m_catalog->attr_GetTypeID(pathExpr[last].classInfo, pathExpr[last].method.methodInfo, typeId);
        OOSQL_CHECK_ERR(e);

        e = m_catalog->attr_GetLength(pathExpr[last].classInfo, pathExpr[last].method.methodInfo, length);
        OOSQL_CHECK_ERR(e);
        break;

    case PATHEXPR_KIND_ATTR:
        e = m_catalog->attr_GetTypeID(pathExpr[last].classInfo, pathExpr[last].attr.attrInfo, typeId);
        OOSQL_CHECK_ERR(e);

        e = m_catalog->attr_GetLength(pathExpr[last].classInfo, pathExpr[last].attr.attrInfo, length);
        OOSQL_CHECK_ERR(e);

        break;

    case PATHEXPR_KIND_OBJECT:
    case PATHEXPR_KIND_CLASS:
        typeId = TYPEID_OID;
        length = TYPEID_OID_SIZE;
        break;

	case PATHEXPR_KIND_LOGICALID:
		typeId = TYPEID_LONG_VAR;
		length = TYPEID_LONG_SIZE_VAR;
		break;

    case PATHEXPR_KIND_FUNC:
        OOSQL_ERR(eINTERNALERROR_OOSQL);
        break;

    default:
        OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
        break;
    }

    return eNOERROR;
}

Four OQL_GDStoCommonAP::getTypeId_and_TypeLength_of_AggrFunc(AggrFuncPoolIndex poolIndex, TypeID& typeId, Four& length)
{
    AggrFuncPoolElements    aggrFunc(m_pool->aggrFuncPool);
    ArgumentPoolElements    argument(m_pool->argumentPool);
    Four                    e;

    aggrFunc = poolIndex;
    argument = aggrFunc[0].argument;

    switch(aggrFunc[0].aggrFunctionID)
    {
    case AGGRFUNC_COUNTALL:
    case AGGRFUNC_COUNT:
        typeId = TYPEID_LONG_VAR;
        length = TYPEID_LONG_SIZE_VAR;
        break;

    case AGGRFUNC_SUM:
        switch(argument[0].argumentKind)
        {
        case ARGUMENT_KIND_PATHEXPR:
            e = getTypeId_and_TypeLength_of_PathExpr(argument[0].pathExpr,
                                                     typeId, length);
            OOSQL_CHECK_ERR(e);

            switch(typeId)
            {
            case TYPEID_SHORT:
            case TYPEID_INT:
            case TYPEID_LONG:
                typeId = TYPEID_LONG;
                length = TYPEID_LONG_SIZE;
				break;
			case TYPEID_LONG_LONG:
                typeId = TYPEID_LONG_LONG;
                length = TYPEID_LONG_LONG_SIZE;
                break;
            case TYPEID_FLOAT:
            case TYPEID_DOUBLE:
                typeId = TYPEID_DOUBLE;
                length = TYPEID_DOUBLE_SIZE;
                break;
            default:
                OOSQL_ERR(eAGGRFUNC_ARGUMENT_ERR_OOSQL);
                break;
            }
            break;

        case ARGUMENT_KIND_VALUE:
        case ARGUMENT_KIND_FUNC:
        case ARGUMENT_KIND_DOMAIN:
        case ARGUMENT_KIND_AGGRFUNC:
        case ARGUMENT_KIND_TEMPFILECOL:
            OOSQL_ERR(eNOTIMPLEMENTED_OOSQL);
            break;

        default:
            OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
            break;
        }
        break;

    case AGGRFUNC_MIN:
    case AGGRFUNC_MAX:
        switch(argument[0].argumentKind)
        {
        case ARGUMENT_KIND_PATHEXPR:
            e = getTypeId_and_TypeLength_of_PathExpr(argument[0].pathExpr,
                                                     typeId, length);
            OOSQL_CHECK_ERR(e);
            break;

        case ARGUMENT_KIND_VALUE:
        case ARGUMENT_KIND_FUNC:
        case ARGUMENT_KIND_DOMAIN:
        case ARGUMENT_KIND_AGGRFUNC:
        case ARGUMENT_KIND_TEMPFILECOL:
            OOSQL_ERR(eNOTIMPLEMENTED_OOSQL);
            break;
        default:
            OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
            break;
        }
        break;

    case AGGRFUNC_AVG:
        switch(argument[0].argumentKind)
        {
        case ARGUMENT_KIND_PATHEXPR:
            e = getTypeId_and_TypeLength_of_PathExpr(argument[0].pathExpr,
                                                     typeId, length);
            OOSQL_CHECK_ERR(e);

            switch(typeId)
            {
            case TYPEID_SHORT:
            case TYPEID_INT:
            case TYPEID_LONG:
			case TYPEID_LONG_LONG:
            case TYPEID_FLOAT:
            case TYPEID_DOUBLE:
                typeId = TYPEID_DOUBLE;
                length = TYPEID_DOUBLE_SIZE;
                break;
            default:
                OOSQL_ERR(eAGGRFUNC_ARGUMENT_ERR_OOSQL);
                break;
            }
            break;

        case ARGUMENT_KIND_VALUE:
        case ARGUMENT_KIND_FUNC:
        case ARGUMENT_KIND_DOMAIN:
        case ARGUMENT_KIND_AGGRFUNC:
        case ARGUMENT_KIND_TEMPFILECOL:
            OOSQL_ERR(eNOTIMPLEMENTED_OOSQL);
            break;
        default:
            OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
            break;
        }
        break;

    case AGGRFUNC_NONE:
        OOSQL_ERR(eINTERNALERROR_OOSQL);
        break;

    default:
        OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
        break;
    }

    return eNOERROR;
}

Four OQL_GDStoCommonAP::getAggrFuncPlanNo(Four m_aggrFuncInfo_planNo, Four aggrFuncInfo_index, Four& planNo, Four& colNo, Boolean& flag)
{
    Four                            i, j, k, l;
    CommonAP_PoolElements           apNode(m_pool->commonAP_Pool);
    AP_ProjectionListPoolElements   projectionList(m_pool->projectionListPool);
    AP_ProjectionPoolElements       projectionInfo(m_pool->projectionPool);

    flag = SM_FALSE;

    apNode = m_commonAP->commonAP;
    for(i = apNode.size - 1; i >= 0 && !flag; i--)
    {
        // find projectionList which projects aggrFuncInfo
        projectionList = apNode[i].projectionList;
        for(j = 0; j < projectionList.size && !flag; j ++)
        {
            projectionInfo = projectionList[j].projectionInfo;
            for(k = 0; k < projectionInfo.size && !flag; k++)
            {
                if(projectionInfo[k].projectionKind == PROJECTION_KIND_AGGRFUNCRESULT &&
                   projectionInfo[k].aggrFuncResult.planNo == m_aggrFuncInfo_planNo &&
                   projectionInfo[k].aggrFuncResult.aggrFuncIndex == aggrFuncInfo_index)
                {
                    // find it
                    // check projectionList[0].tempFileNum is referenced as a plan element
                    for(l = apNode.size - 1; l >= 0 && !flag; l--)
                    {
                        if((apNode[l].classInfo.classKind == CLASSKIND_TEMPORARY || apNode[l].classInfo.classKind == CLASSKIND_SORTSTREAM) &&
                            apNode[l].classInfo.tempFileNum == projectionList[0].tempFileNum)
                        {
                            flag   = SM_TRUE;  // fount it, exit all loops
                            planNo = l;
                            colNo  = k;
                        }
                    }
                }
            }
        }
    }

    return eNOERROR;
}

OperatorID OQL_GDStoCommonAP::reverseComparisionOperatorID(OperatorID id)
{
    switch(id)
    {
    case OP_EQ:
    case OP_NE:
        return id;
    case OP_GT:
        return OP_LT;
    case OP_LT:
        return OP_GT;
    case OP_GE:
        return OP_LE;
    case OP_LE:
        return OP_GE;
	case OP_ISNULL:
		return OP_ISNOTNULL;
	case OP_ISNOTNULL:
		return OP_ISNULL;
	case OP_IN:
		return OP_IN;
    case OP_GEO_NORTH: 
        return OP_GEO_SOUTH;
    case OP_GEO_SOUTH:
        return OP_GEO_NORTH;
    case OP_GEO_EAST:
        return OP_GEO_WEST;
    case OP_GEO_WEST:
        return OP_GEO_EAST;
    case OP_GEO_COVER:
        return OP_GEO_COVERED;
    case OP_GEO_COVERED:
        return OP_GEO_COVER;
    case OP_GEO_CONTAIN:
        return OP_GEO_CONTAINED;
    case OP_GEO_CONTAINED:
        return OP_GEO_CONTAIN;
    case OP_GEO_DISJOINT:
        return OP_GEO_DISJOINT;
    case OP_GEO_EQUAL:
        return OP_GEO_EQUAL;
    case OP_GEO_MEET:
        return OP_GEO_MEET;
    case OP_GEO_OVERLAP:
        return OP_GEO_OVERLAP;
    default:
        OOSQL_ERR_EXIT(eINTERNALERROR_OOSQL);
    }

    return OP_NONE;
}

bool OQL_GDStoCommonAP::isEqualString(AP_StringPoolIndex string1PoolIndex, AP_StringPoolIndex string2PoolIndex)
{
	AP_StringPoolElements	string1(m_pool->stringPool);
	AP_StringPoolElements	string2(m_pool->stringPool);
	Four					i;

	string1 = string1PoolIndex;
	string2 = string2PoolIndex;

	if(string1.size != string2.size)
		return false;
	
	for(i = 0; i < string1.size; i++)
	{
		if(string1[i] != string2[i])
			return false;
	}

	return true;
}

#ifdef  TEMPLATE_NOT_SUPPORTED

implement2(Dictionary,Four,MatchFuncInfo);

#endif  /* TEMPLATE_NOT_SUPPORTED */
