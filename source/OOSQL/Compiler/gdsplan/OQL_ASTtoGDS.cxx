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

#include "OQL_GDS.hxx"
#include "OQL_ASTtoGDS.hxx"
#include "OOSQL_Error.h"
#include "OOSQL_Eval_Util.hxx"

extern "C" {
#include "OQL.yacc.h"
#include "y.tab.h"
#include "OQL_AST.h"
#undef AST
}

#ifndef max
#define max(x, y) (((x) > (y))?(x):(y))
#endif
#ifndef min
#define min(x, y) (((x) < (y))?(x):(y))
#endif

#define ERRORMESSAGE_APPEND_POSITION(m_errorMessage, node) \
{ \
	m_errorMessage->Append("column "); \
	m_errorMessage->Append((Four)AST(node).column); \
	m_errorMessage->Append(", line "); \
	m_errorMessage->Append((Four)AST(node).line); \
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
void makeLowerCaseString(SimpleString& string)
{
	Four i;

	for(i = 0; i < string.length(); i++)
		string[i] = tolower(string[i]);
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_ASTtoGDS::OQL_ASTtoGDS(OQL_AST& ast, OQL_GDS& gds, OQL_GDSPOOL& pool, Four volId, OOSQL_StorageManager& storageManager, OOSQL_Catalog& catalog, OOSQL_ErrorMessage& errorMessage, OOSQL_ExternalFunctionManager& externalFunctionManager)
{
    Four                i, j;

    m_nameStack = new (pMemoryManager) NameStack;

    m_ast						= &ast;
    m_gds						= &gds;
    m_pool						= &pool;
    m_catalog					= &catalog;
	m_storageManager			= &storageManager;
	m_errorMessage				= &errorMessage;
	m_externalFunctionManager	= &externalFunctionManager;
	m_volId						= volId;

    m_progressStatus    = NO_CALUSE;
    m_connectionPoint.setNull();
	m_conditionNotBound = SM_TRUE;
    m_recentOperGroup   = OG_NONE;
    #ifdef ENABLE_OPENGIS_OPTIMIZATION
    m_currentUDFNo      = 0; 
    #endif

    // copy AST's interger, real, string m_pool into GDS
    m_pool->intPool.addNewEntry(m_ast->getIntPoolSize());
    for(i = 0;i < m_ast->getIntPoolSize(); i++)
        m_pool->intPool[i] = m_ast->getInt(i);
	
    m_pool->realPool.addNewEntry(m_ast->getRealPoolSize());
    for(i = 0;i < m_ast->getRealPoolSize(); i++)
        m_pool->realPool[i] = m_ast->getReal(i);

    // do special processing to string and stringIndexPool
    // insert '\0' at the end of the string
    Four                stringLength;
    Four                startIndex;
    StringPoolElements  string(m_pool->stringPool);   

    m_pool->stringIndexPool.addNewEntry(m_ast->getStringIndexPoolSize());
    for(i = 0, startIndex = 0; i < m_ast->getStringIndexPoolSize(); i++)
    {
        // get string length
        if((i + 1) == m_ast->getStringIndexPoolSize())
            stringLength = m_ast->getStringIndexPoolSize() - m_ast->getStringIndex(i);
        else
            stringLength = m_ast->getStringIndex(i + 1) - m_ast->getStringIndex(i);

        // construct string
        string = m_pool->stringPool.addNewEntry(stringLength + 1);
        for(j = 0; j < stringLength; j++)
            string[j] = m_ast->getString(j + startIndex);
        string[j] = '\0';

        // construct string index
        m_pool->stringIndexPool[i] = string.startIndex;

        startIndex += stringLength;
    }
    m_pool->stringIndexPool[i] = string.startIndex + string.size;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
OQL_ASTtoGDS::~OQL_ASTtoGDS()
{
	if(m_nameStack)
		OOSQL_DELETE(m_nameStack);
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eINVALIDAST_OOSQL  
< eNOERROR         

IMPLEMENTATION:
****************************************************************************/
Four OQL_ASTtoGDS::smtChk(ASTNodeIdx root)
{
    ASTNodeIdx  s;          // index used for indicating first child of root
    ReturnType  returnType;
    Four        e;

    if ( AST(root).nodeName != QuProg ) 
    {
        // Error: Invalid AST 
        OOSQL_ERR(eINVALIDAST_OOSQL);
    }

    s = AST(root).son;
    if ( AST(s).nodeName == QuDef ) 
    {   
        // 'Query Definition' statement 
        while ( AST(s).nodeName == QuDef ) 
        {
            e = smtChkQuDef(s, returnType);
            OOSQL_CHECK_ERR(e);

            s = AST(s).brother;         // next 'Query Definition' statement 
        }
    }
    else 
        s = AST(s).brother;             // skip to 'Query' node 
    
    // semantic check 'Query' 
    if( AST(s).nodeName == null)
    {
        // Error: Invalid AST 
        OOSQL_ERR(eINVALIDAST_OOSQL);
    }
    else
    {
        e = smtChkQuery(s, returnType);
        OOSQL_CHECK_ERR(e);
    }

    return eNOERROR;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eNOTIMPLEMENTED_OOSQL  

IMPLEMENTATION:
****************************************************************************/
Four OQL_ASTtoGDS::smtChkQuDef(
    ASTNodeIdx  node, 
    ReturnType& returnType
)
{
	m_errorMessage->Clear();
	m_errorMessage->Append("OQL query definition has not been implemented.");
    OOSQL_ERR(eNOTIMPLEMENTED_OOSQL);
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eNOTIMPLEMENTED_OOSQL  
< eNOERROR         

IMPLEMENTATION:
****************************************************************************/
Four OQL_ASTtoGDS::smtChkQuery(
    ASTNodeIdx node, 
    ReturnType& returnType,
	Boolean forceExprPool		
)
{
    Four    e;
	ExprPoolElements    epElements(m_pool->exprPool);
	ExprPoolIndex		oldConnectionPoint;

	epElements.setNull();
	if(forceExprPool)
	{
		switch (AST(node).nodeName) 
		{
		case QuAccPaex:
		case QuGeoFnNum:
		case QuGeoFnSpa:
		case QuIrFnMatch:
		case QuIrFnNmatch:
		case QuIrFnWeight:
		case VaNil:
		case VaTr:
		case VaFls:
		case INTEGER:
		case REAL:
		case STRING:
		case FnOrCltnobj:
		case VaLtDate:
		case VaLtTime:
		case VaLtTimestamp:
		case VaLtInterval:
		case VaLtComplex:
		case VaText:
			epElements = m_pool->exprPool.addNewEntry();
			epElements[0].operandType = EXPR_OPERANDTYPE_CONTAIN_NOMETHOD;
			epElements[0].refCount    = 0;

			oldConnectionPoint = m_connectionPoint;
			if(m_connectionPoint == NULL_POOLINDEX)
			{
				m_connectionPoint   = epElements.getPoolIndex();
				m_conditionNotBound = SM_TRUE;
			}
			break;
		default:
			break;
		}
	}

    switch (AST(node).nodeName) 
    {
    case null:              // null node
        returnType.set(TYPEID_NULL, PT_NONE, NULL_POOLINDEX);
        break;

    case QuSmp:             // Simple Query : arithmetic and string expressions
        e = smtChkQuSmp(node, returnType);
        OOSQL_CHECK_ERR(e);
        break;

    case QuCmp:             // Comparision Query : =, !=, >, <, >=, <=, like
        e = smtChkQuCmp(node, returnType);
        OOSQL_CHECK_ERR(e);
        break;

    case QuBln:             // Boolean Query : and, or, not
        e = smtChkQuBln(node, returnType);
        OOSQL_CHECK_ERR(e);
        break;

    case QuCltnIn:          // Collection Query : membership testing (in operator)
        e = smtChkQuCltnIn(node, returnType);
        OOSQL_CHECK_ERR(e);
        break;

    case QuCltnCmpSome:     // Collection Query : quantified comparison (existential)
        e = smtChkQuCltnCmpSome(node, returnType);
        OOSQL_CHECK_ERR(e);
        break;

    case QuCltnCmpAny:      // Collection Query : quantified comparison (existential)
        e = smtChkQuCltnCmpAny(node, returnType);
        OOSQL_CHECK_ERR(e);
        break;

    case QuCltnCmpAll:      // Collection Query : quantified comparison universal
        e = smtChkQuCltnCmpAll(node, returnType);
        OOSQL_CHECK_ERR(e);
        break;
    
    case QuSet:             // Set Query : intersect, union, except
        e = smtChkQuSet(node, returnType);
        OOSQL_CHECK_ERR(e);
        break;

// OpenGIS Query
    case QuOgisTranslatableOp: // OpenGIS Translatable Operator
        e = smtChkQuOgisTranslatableOp(node, returnType);
        OOSQL_CHECK_ERR(e);
        break;
        
    case QuOgisGeometricOp: // OpenGIS Geometric Operator
        e = smtChkQuOgisGeometricOp(node, returnType);
        OOSQL_CHECK_ERR(e);
        break;
        
    case QuOgisRelationalOp: // OpenGIS Relational Operator
        e = smtChkQuOgisRelationalOp(node, returnType);
        OOSQL_CHECK_ERR(e);
        break;
        
    case QuOgisMiscellaneousOp: // OpenGIS Miscellaneous Operator
        e = smtChkQuOgisMiscellaneousOp(node, returnType);
        OOSQL_CHECK_ERR(e);
        break;
        
    case QuAccPaex:         // Accessor Query : path expr
        e = smtChkQuAccPaex(node, returnType);
        OOSQL_CHECK_ERR(e);
        break;

    case QuAccDref:         // Accessor Query : dereferencing an object
        // Not Implemented
        e = smtChkQuAccDref(node, returnType);
        OOSQL_CHECK_ERR(e);
        break;

    case QuAccIcltnElem:    // Accessor Query : getting an element of indexed collection
        // Not Implemented
        e = smtChkQuAccIcltnElem(node, returnType);
        OOSQL_CHECK_ERR(e);
        break;

    case QuAccIcltnSub:     // Accessor Query : extracting and subcollection of indexed collection
        // Not Implemented
        e = smtChkQuAccIcltnSub(node, returnType);
        OOSQL_CHECK_ERR(e);
        break;

    case QuAccIcltnFr:      // Accessor Query : getting the first element of indexed collection
        // Not Implemented
        e = smtChkQuAccIcltnFr(node, returnType);
        OOSQL_CHECK_ERR(e);
        break;

    case QuAccIcltnLs:      // Accessor Query : getting the last element of indexed collection
        // Not Implemented
        e = smtChkQuAccIcltnLs(node, returnType);
        OOSQL_CHECK_ERR(e);
        break;

    case QuCltnAll:         // Collection Query : universal quantification (for all)
        e = smtChkQuCltnAll(node, returnType);
        OOSQL_CHECK_ERR(e);
        break;

    case QuCltnEx:          // Collection Query : existential quantification (exists in)
        e = smtChkQuCltnEx(node, returnType);
        OOSQL_CHECK_ERR(e);
        break;

    case QuCltnExany:       // Collection Query : existential quantification (exists)
        e = smtChkQuCltnExany(node, returnType);
        OOSQL_CHECK_ERR(e);
        break;

    case QuCltnUni:         // Collection Query : existential quantification (unique)
        e = smtChkQuCltnUni(node, returnType);
        OOSQL_CHECK_ERR(e);
        break;

    case QuCltnAggDist: 
        e = smtChkQuCltnAggDist(node, returnType);
        OOSQL_CHECK_ERR(e);
        break;

    case QuCltnAgg:         // Collection Query : aggregate functions
        e = smtChkQuCltnAgg(node, returnType);
        OOSQL_CHECK_ERR(e);
        break;

    case QuSel:             // Select Query
        e = smtChkQuSel(node, returnType);
        OOSQL_CHECK_ERR(e);
        break;

	case QuUpd:				// Update Query
		e = smtChkQuUpd(node, returnType);
		OOSQL_CHECK_ERR(e);
		break;

	case QuDel:				// Delete Query
	case QuDeferredDel:
		e = smtChkQuDel(node, returnType);
		OOSQL_CHECK_ERR(e);
		break;

	case QuIns:				// Insert Query
		e = smtChkQuIns(node, returnType);
		OOSQL_CHECK_ERR(e);
		break;

	case QuAlterTbl:		// Alter Table
		e = smtChkQuAlterTbl(node, returnType);
		OOSQL_CHECK_ERR(e);
		break;		

	case QuCreateSeq:		// Create Sequence
		e = smtChkQuCreateSeq(node, returnType);
		OOSQL_CHECK_ERR(e);
		break;		

	case QuDropSeq:		// Drop Sequence
		e = smtChkQuDropSeq(node, returnType);
		OOSQL_CHECK_ERR(e);
		break;		

	case QuCreateTbl:		// Create Table
	case QuCreateTempTbl:	// Create Temporary Table
		e = smtChkQuCreateTbl(node, returnType);
		OOSQL_CHECK_ERR(e);
		break;

	case QuCreateIdx:		// Create Index
		e = smtChkQuCreateIdx(node, returnType);
		OOSQL_CHECK_ERR(e);
		break;

	case QuDropTbl:			// Drop Table
		e = smtChkQuDropTbl(node, returnType);
		OOSQL_CHECK_ERR(e);
		break;

	case QuDropIdx:			// Drop Index
		e = smtChkQuDropIdx(node, returnType);
		OOSQL_CHECK_ERR(e);
		break;

	case QuCreateFunc:	// Create Function
		e = smtChkQuCreateFunc(node, returnType);
		OOSQL_CHECK_ERR(e);
		break;

	case QuDropFunc:	// Drop Function
	case QuDropSpecFunc:
		e = smtChkQuDropFunc(node, returnType);
		OOSQL_CHECK_ERR(e);
		break;
	
	case QuCreateProc:	// Create Procedure
		e = smtChkQuCreateProc(node, returnType);
		OOSQL_CHECK_ERR(e);
		break;

	case QuDropProc:	// Drop Procedure
	case QuDropSpecProc:
		e = smtChkQuDropProc(node, returnType);
		OOSQL_CHECK_ERR(e);
		break;

	case QuCallProc:	// Call Procedure
		e = smtChkQuCallProc(node, returnType);
		OOSQL_CHECK_ERR(e);
		break;
		
    case QuCnvL2s:          // Conversion Query : converting list to set
        e = smtChkQuCnvL2s(node, returnType);
        OOSQL_CHECK_ERR(e);
        break;

    case QuCnvElem:         // Conversion Query : extracting the element of a singleton
        e = smtChkQuCnvElem(node, returnType);
        OOSQL_CHECK_ERR(e);
        break;

    case QuCnvDist:         // Conversion Query : removing duplicates
        e = smtChkQuCnvDist(node, returnType);
        OOSQL_CHECK_ERR(e);
        break;

    case QuCnvFlat:         // Conversion Query : flattening collection of collections
        e = smtChkQuCnvFlat(node, returnType);
        OOSQL_CHECK_ERR(e);
        break;

    case QuCnvType:         // Conversion Query : typing an expressions
        e = smtChkQuCnvType(node, returnType);
        OOSQL_CHECK_ERR(e);
        break;

    case QuIrFnMatch:
        e = smtChkQuIrFnMatch(node, returnType);
        OOSQL_CHECK_ERR(e);
        break;

    case QuIrFnNmatch:
        e = smtChkQuIrFnNmatch(node, returnType);
        OOSQL_CHECK_ERR(e);
        break;

    case QuIrFnWeight:
        e = smtChkQuIrFnWeight(node, returnType);
        OOSQL_CHECK_ERR(e);
        break;

    case VaNil:             // Value : Nil
    case VaTr:              // Value : True
    case VaFls:             // Value : False
    case INTEGER:
    case REAL:
    case STRING:
        e = smtChkValue(node, returnType);
        OOSQL_CHECK_ERR(e);
        break;

    case QuCnsObj:          // Construction Query : object
    case QuCnsStruct:       // Construction Query : structure literal
    case QuCnsSet:          // Construction Query : set literal
    case QuCnsBag:          // Construction Query : bag literal
    case QuCnsLst:          // Construction Query : list literal
    case QuCnsLstrn:        // Construction Query : list literal with range
    case QuCnsArr:          // Construction Query : array literal
        e = smtChkQuCns(node, returnType);
        OOSQL_CHECK_ERR(e);
        break;

    case FnOrCltnobj:       // Function call or collection object construction
        e = smtChkFnOrCltnobj(node, returnType);
        OOSQL_CHECK_ERR(e);
        break;

    case ID:                // ID : alias name, class name, attribute name, method name
        e = smtChkQuAccPaex(node, returnType);
        if(e == eNOT_PATHEXPR_ID_OOSQL)
        {
            // ID is not a alias name, class name. attribute name, moethod name,
            // so it is a newly generated ID in from or group by clause
            StringPoolIndex     poolIndex;

            // get position in string m_pool
            poolIndex.setPoolIndex(m_pool->stringIndexPool[AST(node).tokenVal],
                                   m_pool->stringIndexPool[AST(node).tokenVal + 1] -
                                   m_pool->stringIndexPool[AST(node).tokenVal] - 1);

            returnType.set(TYPEID_ID, PT_STRINGPOOL, poolIndex);
        }
        else if(e == eBADPARAMETER_OOSQL || e == eUNHANDLED_CASE_OOSQL)
		{
			m_errorMessage->Clear();
			m_errorMessage->Append("Undefined Identifier is used at ");
			ERRORMESSAGE_APPEND_POSITION(m_errorMessage, node);

			OOSQL_ERR(eSYNTAX_ERROR_OOSQL)
		}
		else
            OOSQL_CHECK_ERR(e);
        break;
    
    case VaLt:              // Value : integer, float, character, string literal
        OOSQL_ERR(eNOTIMPLEMENTED_OOSQL);
        break;

	case VaSeqCurr:
	case VaSeqNext:
		e = smtChkValueSequence(node, returnType);
        OOSQL_CHECK_ERR(e);
		break;
		
	case VaLtDate:
		e = smtChkValueLiteralDate(node, returnType);
        OOSQL_CHECK_ERR(e);
		break;

	case VaLtTime:
		e = smtChkValueLiteralTime(node, returnType);
		OOSQL_CHECK_ERR(e);
		break;

	case VaLtTimestamp:
		e = smtChkValueLiteralTimestamp(node, returnType);
		OOSQL_CHECK_ERR(e);
		break;

	case VaLtInterval:
		e = smtChkValueLiteralInterval(node, returnType);
		OOSQL_CHECK_ERR(e);
		break;

	case VaLtComplex:
		e = smtChkValueLiteralComplex(node, returnType);
		OOSQL_CHECK_ERR(e);
		break;

	case VaText:
		e = smtChkValueText(node, returnType);
		OOSQL_CHECK_ERR(e);
		break;

    default:
        OOSQL_ERR(eNOTIMPLEMENTED_OOSQL);
        break;
    }

	if(returnType.poolType != PT_EXPRPOOL)
	{
		if(forceExprPool)
		{
			if(epElements.isNull())
				OOSQL_ERR(eINTERNALERROR_OOSQL);
        
			switch(returnType.poolType)
			{
			case PT_PATHEXPRPOOL:
				epElements[0].exprKind = EXPR_KIND_PATHEXPR;
				epElements[0].pathExpr.setPoolIndex(returnType.poolIndex);
				break;

			case PT_AGGRFUNCPOOL:
				epElements[0].exprKind = EXPR_KIND_AGGRFUNC;
				epElements[0].aggrFunc.setPoolIndex(returnType.poolIndex);
				break;

			case PT_FUNCPOOL:
				epElements[0].exprKind = EXPR_KIND_FUNCTION;
				epElements[0].func.setPoolIndex(returnType.poolIndex);
				break;

			case PT_VALUEPOOL:
				epElements[0].exprKind = EXPR_KIND_VALUE;
				epElements[0].value.setPoolIndex(returnType.poolIndex);
				break;

			default:
				OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
			}

			e = exprElementSetTypeInfo(epElements, returnType);
			OOSQL_CHECK_ERR(e);
			
			if(m_connectionPoint!= NULL_POOLINDEX && oldConnectionPoint == NULL_POOLINDEX && m_conditionNotBound && m_progressStatus != HAVING_CLAUSE)
			{
				e = m_gds->addNodeToUnusedConditionList(m_pool, m_connectionPoint);
				OOSQL_CHECK_ERR(e);
			}
			m_connectionPoint = oldConnectionPoint;

			returnType.set(returnType.typeID, PT_EXPRPOOL, epElements.getPoolIndex());
		}
	}

    return eNOERROR;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eUNHANDLED_CASE_OOSQL
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OQL_ASTtoGDS::smtChkValue( 
    ASTNodeIdx  node,
    ReturnType& returnType
)
{
    ValuePoolElements vpElements(m_pool->valuePool);
    Four              e;

    switch(AST(node).nodeName)
    {
    case VaNil:             // Value : Nil
        vpElements = m_pool->valuePool.addNewEntry();
        
        vpElements[0].valueKind = VALUE_KIND_NIL;
        vpElements[0].nil.setPoolIndex(0, 1);

        returnType.set(TYPEID_NIL, PT_VALUEPOOL, vpElements.getPoolIndex());
        break;          

    case VaTr:              // Value : True
        vpElements = m_pool->valuePool.addNewEntry();
        
        vpElements[0].valueKind = VALUE_KIND_BOOL;
        vpElements[0].boolean.setPoolIndex(SM_TRUE, 1);

        returnType.set(TYPEID_BOOL, PT_VALUEPOOL, vpElements.getPoolIndex());
        break;

    case VaFls:             // Value : False
        vpElements = m_pool->valuePool.addNewEntry();
        
        vpElements[0].valueKind = VALUE_KIND_BOOL;
        vpElements[0].boolean.setPoolIndex(SM_FALSE, 1);

        returnType.set(TYPEID_BOOL, PT_VALUEPOOL, vpElements.getPoolIndex());
        break;

    case INTEGER:
        vpElements = m_pool->valuePool.addNewEntry();
        
        vpElements[0].valueKind = VALUE_KIND_INTEGER;
        vpElements[0].integer.setPoolIndex(AST(node).tokenVal, 1);
		
        returnType.set(TYPEID_INT, PT_VALUEPOOL, vpElements.getPoolIndex());
        break;

    case REAL:
        vpElements = m_pool->valuePool.addNewEntry();
        
        vpElements[0].valueKind = VALUE_KIND_REAL;
        vpElements[0].real.setPoolIndex(AST(node).tokenVal, 1);

        returnType.set(TYPEID_DOUBLE, PT_VALUEPOOL, vpElements.getPoolIndex());
        break;

    case STRING:
        vpElements = m_pool->valuePool.addNewEntry();
        
        vpElements[0].valueKind = VALUE_KIND_STRING;
        vpElements[0].string.setPoolIndex(m_pool->stringIndexPool[AST(node).tokenVal],
                                          m_pool->stringIndexPool[AST(node).tokenVal + 1] -
                                          m_pool->stringIndexPool[AST(node).tokenVal] - 1);

        returnType.set(TYPEID_STRING, PT_VALUEPOOL, vpElements.getPoolIndex());    
        break;
    default:
		m_errorMessage->Clear();
		m_errorMessage->Append("Undefine value type is used\n");
        OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
    }

    return eNOERROR;
}

Four OQL_ASTtoGDS::smtChkValueSequence( 
    ASTNodeIdx  node,
    ReturnType& returnType
)
{
    ValuePoolElements	vpElements(m_pool->valuePool);
    Four				e;
	ASTNodeIdx			tmpNode;
	char*				seqName;

	tmpNode = AST(node).son;
    switch(AST(tmpNode).nodeName)
    {
    case ID:
        vpElements = m_pool->valuePool.addNewEntry();
        
        vpElements[0].valueKind = VALUE_KIND_STRING;
        vpElements[0].string.setPoolIndex(m_pool->stringIndexPool[AST(tmpNode).tokenVal],
                                          m_pool->stringIndexPool[AST(tmpNode).tokenVal + 1] -
                                          m_pool->stringIndexPool[AST(tmpNode).tokenVal] - 1);
		
		seqName = &(vpElements[0].string.getElements(m_pool->stringPool, 0));
        
		e = m_storageManager->CheckSequence(m_volId, seqName);
		if (e == eNOT_FOUND_OOSQL)
		{
				m_errorMessage->Clear();
				m_errorMessage->Append("The sequence '");
				m_errorMessage->Append((char*)seqName);
				m_errorMessage->Append("' is not defined in the database.");

				OOSQL_ERR(eSEQUENCENOTDEFINED_OOSQL);
		}

		returnType.set(TYPEID_INT, PT_VALUEPOOL, vpElements.getPoolIndex());    
        break;
    
    default:
		m_errorMessage->Clear();
		m_errorMessage->Append("Undefine value type is used\n");
        OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
    }

    return eNOERROR;
}


/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eSYNTAX_ERROR_OOSQL 
eTYPE_ERROR_OOSQL   
eUNHANDLED_CASE_OOSQL 
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OQL_ASTtoGDS::smtChkQuSmp( 
    // Simple Query : arithmetic and string expressions
    ASTNodeIdx  node,
    ReturnType& returnType
)
{
    ASTNodeIdx          s;
    ASTNodeIdx          op1;
    ASTNodeIdx          op2;
    ReturnType          op1Type;
    ReturnType          op2Type;
    ExprPoolElements    epElements(m_pool->exprPool);
    OperatorGroup       oldOperGroup;
    Four                e;
    Boolean             flag;
    TypeID              resultType;
    OperatorType        operatorType;
    ExprPoolIndex       exprPoolIndex;

    oldOperGroup    = m_recentOperGroup;
    m_recentOperGroup = OG_SIMPLE;

    s = AST(node).son;

    // divide operators into binary or unary operator
    if(AST(s).nodeName == OpUnMin || AST(s).nodeName == OpUnAbs)
    {
        // unary operator
        switch(AST(s).nodeName)
        {
        case OpUnMin:
            // 1. int, float unary minus
        case OpUnAbs:
            // 1. int, float absolute value
            op1 = AST(s).brother;
            
            e = smtChkQuery(op1, op1Type);
            OOSQL_CHECK_ERR(e);

			if(op1Type.typeID == TYPEID_ID)
			{
				m_errorMessage->Clear();
				m_errorMessage->Append("Undefined Identifier is used at ");
				ERRORMESSAGE_APPEND_POSITION(m_errorMessage, op1);

				OOSQL_ERR(eSYNTAX_ERROR_OOSQL)
			}
            if(!(op1Type.typeID == TYPEID_INT || 
                 op1Type.typeID == TYPEID_SHORT ||
                 op1Type.typeID == TYPEID_LONG ||
                 op1Type.typeID == TYPEID_LONG_LONG ||
                 op1Type.typeID == TYPEID_DOUBLE ||
                 op1Type.typeID == TYPEID_FLOAT))
			{
				m_errorMessage->Clear();
				m_errorMessage->Append("The type of the value used at ");
				ERRORMESSAGE_APPEND_POSITION(m_errorMessage, op1);
				m_errorMessage->Append(" is not a integer, short, long, double, float where one of those is expected.");

				OOSQL_ERR(eTYPE_ERROR_OOSQL);
			}

            operatorType = ARITHMETIC_OPERATION;
            break;

        default:
            OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
            break;
        }
        
        // Construct operator node
        epElements = m_pool->exprPool.addNewEntry();

        epElements[0].exprKind			= EXPR_KIND_OPER;
        e = nodeNameToOperatorID(AST(s).nodeName, epElements[0].oper.operatorId);
        OOSQL_CHECK_ERR(e);
        epElements[0].oper.operatorType = operatorType;
        epElements[0].operandType       = EXPR_OPERANDTYPE_CONTAIN_NOMETHOD;
        epElements[0].refCount          = 0;

        // make operand nodes
        e = makeOperandInExprPool(op1Type, exprPoolIndex);
        epElements[0].oper.operand1 = exprPoolIndex;
        OOSQL_CHECK_ERR(e);
        epElements[0].oper.operand2.setNull();       // no second operand
		epElements[0].oper.operand3.setNull();       // no third operand

        returnType.set(op1Type.typeID, PT_EXPRPOOL, epElements.getPoolIndex());

        e = exprElementSetTypeInfo(epElements, returnType);
        OOSQL_CHECK_ERR(e);
    }
    else
    {
        // binary operators
        switch ( AST(s).nodeName )      // kind of operator
        {   
        case OpBiPlu:
            // 1. list, array, or string concatenation
            // 2. int, float addition
            op1 = AST(s).brother;
            op2 = AST(op1).brother;
            
            e = smtChkQuery(op1, op1Type);  OOSQL_CHECK_ERR(e);
            e = smtChkQuery(op2, op2Type);  OOSQL_CHECK_ERR(e);
            
			if(op1Type.typeID == TYPEID_ID)
			{
				m_errorMessage->Clear();
				m_errorMessage->Append("Undefined Identifier is used at ");
				ERRORMESSAGE_APPEND_POSITION(m_errorMessage, op1);

				OOSQL_ERR(eSYNTAX_ERROR_OOSQL)
			}
            if(!(op1Type.typeID == TYPEID_LIST   ||
                 op1Type.typeID == TYPEID_ARRAY  ||
				 op1Type.typeID == TYPEID_VARSTRING ||
                 op1Type.typeID == TYPEID_STRING ||
                 op1Type.typeID == TYPEID_INT    ||
                 op1Type.typeID == TYPEID_SHORT  ||
                 op1Type.typeID == TYPEID_LONG   ||
                 op1Type.typeID == TYPEID_LONG_LONG   ||
                 op1Type.typeID == TYPEID_DOUBLE ||
                 op1Type.typeID == TYPEID_FLOAT))
			{
				m_errorMessage->Clear();
				m_errorMessage->Append("The type of the value used at ");
				ERRORMESSAGE_APPEND_POSITION(m_errorMessage, op1);
				m_errorMessage->Append(" is not a integer, short, long, double, float, char, varchar, array, list where one of those is expected.");

                OOSQL_ERR(eTYPE_ERROR_OOSQL);
			}

			if(op2Type.typeID == TYPEID_ID)
			{
				m_errorMessage->Clear();
				m_errorMessage->Append("Undefined Identifier is used at ");
				ERRORMESSAGE_APPEND_POSITION(m_errorMessage, op2);

				OOSQL_ERR(eSYNTAX_ERROR_OOSQL)
			}
            if(!(op2Type.typeID == TYPEID_LIST   ||
                 op2Type.typeID == TYPEID_ARRAY  ||
				 op2Type.typeID == TYPEID_VARSTRING ||
                 op2Type.typeID == TYPEID_STRING ||
                 op2Type.typeID == TYPEID_INT    ||
                 op2Type.typeID == TYPEID_SHORT  ||
                 op2Type.typeID == TYPEID_LONG   ||
                 op2Type.typeID == TYPEID_LONG_LONG   ||
                 op2Type.typeID == TYPEID_DOUBLE ||
                 op2Type.typeID == TYPEID_FLOAT))
			{
				m_errorMessage->Clear();
				m_errorMessage->Append("The type of the value used at ");
				ERRORMESSAGE_APPEND_POSITION(m_errorMessage, op2);
				m_errorMessage->Append(" is not a integer, short, long, double, float, char, varchar, array, list where one of those is expected.");

                OOSQL_ERR(eTYPE_ERROR_OOSQL);
			}
                
            switch(op1Type.typeID)
            {
            case TYPEID_LIST:
            case TYPEID_ARRAY:
                operatorType = COLLECTION_OPERATION;
                break;
			
			case TYPEID_VARSTRING:
            case TYPEID_STRING:
                operatorType = STRING_OPERATION;
                break;

            case TYPEID_INT:
            case TYPEID_SHORT:
            case TYPEID_LONG:
            case TYPEID_LONG_LONG:
            case TYPEID_FLOAT:
            case TYPEID_DOUBLE:
                operatorType = ARITHMETIC_OPERATION;
                break;
            }

            break;
                
        case OpBiMin:
            // 1. int, float subtraction
        case OpBiMul:
            // 1. int, float multiplication
        case OpBiDiv:
            // 1. int, float division
             
            op1 = AST(s).brother;
            op2 = AST(op1).brother;
            
            e = smtChkQuery(op1, op1Type);  OOSQL_CHECK_ERR(e);
            e = smtChkQuery(op2, op2Type);  OOSQL_CHECK_ERR(e);
            
			if(op1Type.typeID == TYPEID_ID)
			{
				m_errorMessage->Clear();
				m_errorMessage->Append("Undefined Identifier is used at ");
				ERRORMESSAGE_APPEND_POSITION(m_errorMessage, op1);

				OOSQL_ERR(eSYNTAX_ERROR_OOSQL)
			}
            if(!(op1Type.typeID == TYPEID_INT    ||
                 op1Type.typeID == TYPEID_SHORT  ||
                 op1Type.typeID == TYPEID_LONG   ||
                 op1Type.typeID == TYPEID_LONG_LONG   ||
                 op1Type.typeID == TYPEID_DOUBLE ||
                 op1Type.typeID == TYPEID_FLOAT))
			{
				m_errorMessage->Clear();
				m_errorMessage->Append("The type of the value used at ");
				ERRORMESSAGE_APPEND_POSITION(m_errorMessage, op1);
				m_errorMessage->Append(" is not a integer, short, long, double, float where one of those is expected.");

                OOSQL_ERR(eTYPE_ERROR_OOSQL);
			}

			if(op2Type.typeID == TYPEID_ID)
			{
				m_errorMessage->Clear();
				m_errorMessage->Append("Undefined Identifier is used at ");
				ERRORMESSAGE_APPEND_POSITION(m_errorMessage, op2);

				OOSQL_ERR(eSYNTAX_ERROR_OOSQL)
			}
            if(!(op2Type.typeID == TYPEID_INT    ||
                 op2Type.typeID == TYPEID_SHORT ||
				 op2Type.typeID == TYPEID_LONG ||
                 op2Type.typeID == TYPEID_LONG_LONG ||
                 op2Type.typeID == TYPEID_DOUBLE ||
                 op2Type.typeID == TYPEID_FLOAT))
			{
				m_errorMessage->Clear();
				m_errorMessage->Append("The type of the value used at ");
				ERRORMESSAGE_APPEND_POSITION(m_errorMessage, op2);
				m_errorMessage->Append(" is not a integer, short, long, double, float where one of those is expected.");

                OOSQL_ERR(eTYPE_ERROR_OOSQL);
			}
            
            operatorType = ARITHMETIC_OPERATION;

            break;

        case OpBiMod:
            // 1. int modulo
             
            op1 = AST(s).brother;
            op2 = AST(op1).brother;
            
            e = smtChkQuery(op1, op1Type);  OOSQL_CHECK_ERR(e);
            e = smtChkQuery(op2, op2Type);  OOSQL_CHECK_ERR(e);
            
			if(op1Type.typeID == TYPEID_ID)
			{
				m_errorMessage->Clear();
				m_errorMessage->Append("Undefined Identifier is used at ");
				ERRORMESSAGE_APPEND_POSITION(m_errorMessage, op1);

				OOSQL_ERR(eSYNTAX_ERROR_OOSQL)
			}
			if(!(op1Type.typeID == TYPEID_INT    ||
                 op1Type.typeID == TYPEID_SHORT  ||
                 op1Type.typeID == TYPEID_LONG  ||
                 op1Type.typeID == TYPEID_LONG_LONG))
			{
				m_errorMessage->Clear();
				m_errorMessage->Append("The type of the value used at ");
				ERRORMESSAGE_APPEND_POSITION(m_errorMessage, op1);
				m_errorMessage->Append(" is not a integer, short, long where one of those is expected.");

                OOSQL_ERR(eTYPE_ERROR_OOSQL);
			}

			if(op2Type.typeID == TYPEID_ID)
			{
				m_errorMessage->Clear();
				m_errorMessage->Append("Undefined Identifier is used at ");
				ERRORMESSAGE_APPEND_POSITION(m_errorMessage, op2);

				OOSQL_ERR(eSYNTAX_ERROR_OOSQL)
			}
			if(!(op2Type.typeID == TYPEID_INT    ||
                 op2Type.typeID == TYPEID_SHORT  ||
                 op2Type.typeID == TYPEID_LONG  ||
                 op2Type.typeID == TYPEID_LONG_LONG))
			{
				m_errorMessage->Clear();
				m_errorMessage->Append("The type of the value used at ");
				ERRORMESSAGE_APPEND_POSITION(m_errorMessage, op2);
				m_errorMessage->Append(" is not a integer, short, long where one of those is expected.");

                OOSQL_ERR(eTYPE_ERROR_OOSQL);
			}
                
            operatorType = ARITHMETIC_OPERATION;

            break;

        case OpBiStrcat:
            // 1. string concatenation
            op1 = AST(s).brother;
            op2 = AST(op1).brother;
            
            e = smtChkQuery(op1, op1Type);  OOSQL_CHECK_ERR(e);
            e = smtChkQuery(op2, op2Type);  OOSQL_CHECK_ERR(e);
            
			if(op1Type.typeID == TYPEID_ID)
			{
				m_errorMessage->Clear();
				m_errorMessage->Append("Undefined Identifier is used at ");
				ERRORMESSAGE_APPEND_POSITION(m_errorMessage, op1);

				OOSQL_ERR(eSYNTAX_ERROR_OOSQL)
			}
            if(!(op1Type.typeID == TYPEID_STRING || op1Type.typeID == TYPEID_VARSTRING))
			{
				m_errorMessage->Clear();
				m_errorMessage->Append("The type of the value used at ");
				ERRORMESSAGE_APPEND_POSITION(m_errorMessage, op1);
				m_errorMessage->Append(" is not a char, varchar where one of those is expected.");

                OOSQL_ERR(eTYPE_ERROR_OOSQL);
			}

			if(op2Type.typeID == TYPEID_ID)
			{
				m_errorMessage->Clear();
				m_errorMessage->Append("Undefined Identifier is used at ");
				ERRORMESSAGE_APPEND_POSITION(m_errorMessage, op2);

				OOSQL_ERR(eSYNTAX_ERROR_OOSQL)
			}
            if(!(op2Type.typeID == TYPEID_STRING || op2Type.typeID == TYPEID_VARSTRING))
			{
				m_errorMessage->Clear();
				m_errorMessage->Append("The type of the value used at ");
				ERRORMESSAGE_APPEND_POSITION(m_errorMessage, op2);
				m_errorMessage->Append(" is not a char, varchar where one of those is expected.");

                OOSQL_ERR(eTYPE_ERROR_OOSQL);
            }

            operatorType = STRING_OPERATION;

            break;
            
        default:
            OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
        }   

        // check type compatibility between op1 and op2
        e = testCompatibility(op1Type.typeID, op2Type.typeID, flag);
        OOSQL_CHECK_ERR(e);
        if(flag == SM_FALSE)
		{
			m_errorMessage->Clear();
			m_errorMessage->Append("The type of the value used at ");
			ERRORMESSAGE_APPEND_POSITION(m_errorMessage, op1);
			m_errorMessage->Append(" is not compatible with the type of the value used at ");
			ERRORMESSAGE_APPEND_POSITION(m_errorMessage, op2);
			m_errorMessage->Append(".");

            OOSQL_ERR(eTYPE_ERROR_OOSQL);
		}
        
        // construct operator node
        epElements = m_pool->exprPool.addNewEntry();

        epElements[0].exprKind      = EXPR_KIND_OPER;
        e = nodeNameToOperatorID(AST(s).nodeName, epElements[0].oper.operatorId);
        OOSQL_CHECK_ERR(e);
        epElements[0].oper.operatorType = operatorType;
        epElements[0].operandType       = EXPR_OPERANDTYPE_CONTAIN_NOMETHOD;
        epElements[0].refCount          = 0;
            
        // make operand nodes;
        e = makeOperandInExprPool(op1Type, exprPoolIndex);
        epElements[0].oper.operand1 = exprPoolIndex;
        OOSQL_CHECK_ERR(e);
        e = makeOperandInExprPool(op2Type, exprPoolIndex);
        epElements[0].oper.operand2 = exprPoolIndex;
        OOSQL_CHECK_ERR(e);
		epElements[0].oper.operand3.setNull();       // no third operand

        e = determineResultType(op1Type.typeID, op2Type.typeID, resultType);
        OOSQL_CHECK_ERR(e);

        returnType.set(resultType, PT_EXPRPOOL, epElements.getPoolIndex());

        e = exprElementSetTypeInfo(epElements, returnType);
        OOSQL_CHECK_ERR(e);
    }

    m_recentOperGroup = oldOperGroup;

    return eNOERROR;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eSYNTAX_ERROR_OOSQL 
eTYPE_ERROR_OOSQL   
eUNHANDLED_CASE_OOSQL 
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OQL_ASTtoGDS::smtChkQuCmp( 
    ASTNodeIdx  node,
    ReturnType& returnType
)
{
    ASTNodeIdx       s;
    ASTNodeIdx       op1;
    ASTNodeIdx       op2;
    ReturnType       op1Type;
    ReturnType       op2Type;
    ExprPoolElements epElements(m_pool->exprPool);
    ExprPoolIndex    oldConnectionPoint;
    OperatorGroup    oldOperGroup;
    Four             e;
    Boolean          flag;
    OperatorType     operatorType;
    ExprPoolIndex    exprPoolIndex;

    oldOperGroup    = m_recentOperGroup;
    m_recentOperGroup = OG_COMPARE;

    s = AST(node).son;

    // construct operator node
    epElements = m_pool->exprPool.addNewEntry();
    epElements[0].operandType = EXPR_OPERANDTYPE_CONTAIN_NOMETHOD;
    epElements[0].refCount    = 0;

    oldConnectionPoint = m_connectionPoint;
    if(m_connectionPoint == NULL_POOLINDEX)
	{
        m_connectionPoint   = epElements.getPoolIndex();
		m_conditionNotBound = SM_TRUE;
	}
                
    switch ( AST(s).nodeName ) 
    {
    case OpCmpLike:
        // 1. string matching: opr1:(string), opr2:(string literal containing wild char.)
        op1 = AST(s).brother;
        op2 = AST(op1).brother;

        e = smtChkQuery(op1, op1Type);  OOSQL_CHECK_ERR(e);
        e = smtChkQuery(op2, op2Type);  OOSQL_CHECK_ERR(e);

        // check children's type
		if(op1Type.typeID == TYPEID_ID)
		{
			m_errorMessage->Clear();
			m_errorMessage->Append("Undefined Identifier is used at ");
			ERRORMESSAGE_APPEND_POSITION(m_errorMessage, op1);

			OOSQL_ERR(eSYNTAX_ERROR_OOSQL)
		}
        if(!(op1Type.typeID == TYPEID_STRING || op1Type.typeID == TYPEID_VARSTRING))
		{
			m_errorMessage->Clear();
			m_errorMessage->Append("The type of the value used at ");
			ERRORMESSAGE_APPEND_POSITION(m_errorMessage, op1);
			m_errorMessage->Append(" is not a char, varchar where one of those is expected.");

			OOSQL_ERR(eTYPE_ERROR_OOSQL);
		}

		if(op2Type.typeID == TYPEID_ID)
		{
			m_errorMessage->Clear();
			m_errorMessage->Append("Undefined Identifier is used at ");
			ERRORMESSAGE_APPEND_POSITION(m_errorMessage, op2);

			OOSQL_ERR(eSYNTAX_ERROR_OOSQL)
		}
		if(!(op2Type.typeID == TYPEID_STRING || op2Type.typeID == TYPEID_VARSTRING))
		{
   			m_errorMessage->Clear();
			m_errorMessage->Append("The type of the value used at ");
			ERRORMESSAGE_APPEND_POSITION(m_errorMessage, op2);
			m_errorMessage->Append(" is not a char, varchar where one of those is expected.");

			OOSQL_ERR(eTYPE_ERROR_OOSQL);
		}

        operatorType = STRING_OPERATION;
        break;

    case OpCmpEq:
    case OpCmpNe:
        // 1. literal (int, real, char, string)
        // 2. mutable object
        // 3. dereferenced object (value)
        op1 = AST(s).brother;
        op2 = AST(op1).brother;
        
        e = smtChkQuery(op1, op1Type);  OOSQL_CHECK_ERR(e);
        e = smtChkQuery(op2, op2Type);  OOSQL_CHECK_ERR(e);

		if(op1Type.typeID == TYPEID_ID)
		{
			m_errorMessage->Clear();
			m_errorMessage->Append("Undefined Identifier is used at ");
			ERRORMESSAGE_APPEND_POSITION(m_errorMessage, op1);

			OOSQL_ERR(eSYNTAX_ERROR_OOSQL)
		}
        if(!(op1Type.typeID == TYPEID_INT    ||
             op1Type.typeID == TYPEID_SHORT  ||
             op1Type.typeID == TYPEID_LONG   ||
             op1Type.typeID == TYPEID_LONG_LONG   ||
             op1Type.typeID == TYPEID_DOUBLE ||
             op1Type.typeID == TYPEID_FLOAT  ||
			 op1Type.typeID == TYPEID_VARSTRING ||
             op1Type.typeID == TYPEID_STRING ||
			 op1Type.typeID == TYPEID_TEXT	 ||
             op1Type.typeID == TYPEID_BOOL   ||
			 op1Type.typeID == TYPEID_DATE   ||
			 op1Type.typeID == TYPEID_TIME   ||
             op1Type.typeID == TYPEID_OID    ||
			 op1Type.typeID == TYPEID_TIMESTAMP ||
			 op1Type.typeID == TYPEID_INTERVAL ))
		{
			m_errorMessage->Clear();
			m_errorMessage->Append("The type of the value used at ");
			ERRORMESSAGE_APPEND_POSITION(m_errorMessage, op1);
			m_errorMessage->Append(" is not a integer, short, long, double, float, char, varchar, text, bool, date, time, timestamp, interval where one of those is expected.");

            OOSQL_ERR(eTYPE_ERROR_OOSQL);
		}

		if(op2Type.typeID == TYPEID_ID)
		{
			m_errorMessage->Clear();
			m_errorMessage->Append("Undefined Identifier is used at ");
			ERRORMESSAGE_APPEND_POSITION(m_errorMessage, op2);

			OOSQL_ERR(eSYNTAX_ERROR_OOSQL)
		}
        if(!(op2Type.typeID == TYPEID_INT    ||
             op2Type.typeID == TYPEID_SHORT  ||
             op2Type.typeID == TYPEID_LONG   ||
             op2Type.typeID == TYPEID_LONG_LONG   ||
             op2Type.typeID == TYPEID_DOUBLE ||
             op2Type.typeID == TYPEID_FLOAT  ||
			 op2Type.typeID == TYPEID_VARSTRING ||
             op2Type.typeID == TYPEID_STRING ||
			 op2Type.typeID == TYPEID_TEXT	 ||
             op2Type.typeID == TYPEID_BOOL   ||
			 op2Type.typeID == TYPEID_DATE   ||
			 op2Type.typeID == TYPEID_TIME   ||
             op2Type.typeID == TYPEID_OID    ||
			 op2Type.typeID == TYPEID_TIMESTAMP ||
			 op2Type.typeID == TYPEID_INTERVAL ))
		{
			m_errorMessage->Clear();
			m_errorMessage->Append("The type of the value used at ");
			ERRORMESSAGE_APPEND_POSITION(m_errorMessage, op2);
			m_errorMessage->Append(" is not a integer, short, long, double, float, char, varchar, text, bool, date, time, timestamp, interval where one of those is expected.");

            OOSQL_ERR(eTYPE_ERROR_OOSQL);
		}

        if(op1Type.typeID == TYPEID_STRING || op1Type.typeID == TYPEID_VARSTRING || op1Type.typeID == TYPEID_TEXT)
            operatorType = STRING_OPERATION;
        else
            operatorType = ARITHMETIC_OPERATION;

        break;
        
    case OpCmpGt:
    case OpCmpLt:
    case OpCmpGe:
    case OpCmpLe:
        // 1. literal (int, real, char, string)
        // 2. set, bag inclusion

        op1 = AST(s).brother;
        op2 = AST(op1).brother;
        
        e = smtChkQuery(op1, op1Type);  OOSQL_CHECK_ERR(e);
        e = smtChkQuery(op2, op2Type);  OOSQL_CHECK_ERR(e);
        
		if(op1Type.typeID == TYPEID_ID)
		{
			m_errorMessage->Clear();
			m_errorMessage->Append("Undefined Identifier is used at ");
			ERRORMESSAGE_APPEND_POSITION(m_errorMessage, op1);

			OOSQL_ERR(eSYNTAX_ERROR_OOSQL)
		}
        if(!(op1Type.typeID == TYPEID_SET    ||
             op1Type.typeID == TYPEID_BAG    ||
             op1Type.typeID == TYPEID_STRING ||
			 op1Type.typeID == TYPEID_VARSTRING ||
             op1Type.typeID == TYPEID_INT    ||
             op1Type.typeID == TYPEID_SHORT  ||
             op1Type.typeID == TYPEID_LONG   ||
             op1Type.typeID == TYPEID_LONG_LONG   ||
             op1Type.typeID == TYPEID_DOUBLE ||
             op1Type.typeID == TYPEID_FLOAT  ||
			 op1Type.typeID == TYPEID_DATE   ||
			 op1Type.typeID == TYPEID_TIME   ||
			 op1Type.typeID == TYPEID_TIMESTAMP ||
			 op1Type.typeID == TYPEID_INTERVAL))
		{
			m_errorMessage->Clear();
			m_errorMessage->Append("The type of the value used at ");
			ERRORMESSAGE_APPEND_POSITION(m_errorMessage, op1);
			m_errorMessage->Append(" is not a integer, short, long, double, float, char, varchar, bool, date, time, timestamp, interval, set, bag where one of those is expected.");

            OOSQL_ERR(eTYPE_ERROR_OOSQL);
		}

		if(op2Type.typeID == TYPEID_ID)
		{
			m_errorMessage->Clear();
			m_errorMessage->Append("Undefined Identifier is used at ");
			ERRORMESSAGE_APPEND_POSITION(m_errorMessage, op2);

			OOSQL_ERR(eSYNTAX_ERROR_OOSQL)
		}
        if(!(op2Type.typeID == TYPEID_SET    ||
             op2Type.typeID == TYPEID_BAG    ||
             op2Type.typeID == TYPEID_STRING ||
			 op2Type.typeID == TYPEID_VARSTRING ||
             op2Type.typeID == TYPEID_INT    ||
             op2Type.typeID == TYPEID_SHORT  ||
             op2Type.typeID == TYPEID_LONG   ||
             op2Type.typeID == TYPEID_LONG_LONG   ||
             op2Type.typeID == TYPEID_DOUBLE ||
             op2Type.typeID == TYPEID_FLOAT  ||
			 op2Type.typeID == TYPEID_DATE   ||
			 op2Type.typeID == TYPEID_TIME   ||
			 op2Type.typeID == TYPEID_TIMESTAMP ||
			 op2Type.typeID == TYPEID_INTERVAL))
		{
			m_errorMessage->Clear();
			m_errorMessage->Append("The type of the value used at ");
			ERRORMESSAGE_APPEND_POSITION(m_errorMessage, op2);
			m_errorMessage->Append(" is not a integer, short, long, double, float, char, varchar, bool, date, time, timestamp, interval, set, bag where one of those is expected.");

            OOSQL_ERR(eTYPE_ERROR_OOSQL);
		}

        switch(op2Type.typeID)
        {
        case TYPEID_SET:
        case TYPEID_BAG:
            operatorType = COLLECTION_OPERATION;
            break;

		case TYPEID_VARSTRING:
        case TYPEID_STRING:
            operatorType = STRING_OPERATION;
            break;

        case TYPEID_INT:
        case TYPEID_SHORT:
        case TYPEID_LONG:
        case TYPEID_LONG_LONG:
        case TYPEID_FLOAT:
        case TYPEID_DOUBLE:
		case TYPEID_DATE:
		case TYPEID_TIME:
		case TYPEID_TIMESTAMP:
		case TYPEID_INTERVAL:
            operatorType = ARITHMETIC_OPERATION;
        }

        break;
	case OpCmpIsNull:
	case OpCmpIsNotNull:
		op1 = AST(s).brother;
		
		e = smtChkQuery(op1, op1Type);  OOSQL_CHECK_ERR(e);
		
		// check children's type
		if(op1Type.typeID == TYPEID_ID)
		{
			m_errorMessage->Clear();
			m_errorMessage->Append("Undefined Identifier is used at ");
			ERRORMESSAGE_APPEND_POSITION(m_errorMessage, op1);
			
			OOSQL_ERR(eSYNTAX_ERROR_OOSQL)
		}
		
		operatorType = NULLCHECK_OPERATION;
		break;
	}	
	if(m_connectionPoint!= NULL_POOLINDEX && oldConnectionPoint == NULL_POOLINDEX && m_conditionNotBound && m_progressStatus != HAVING_CLAUSE)
	{
		e = m_gds->addNodeToUnusedConditionList(m_pool, m_connectionPoint);
		OOSQL_CHECK_ERR(e);
	}
    m_connectionPoint = oldConnectionPoint;

    // check type compatibility between op1 and op2

	if(AST(s).nodeName!=OpCmpIsNull && AST(s).nodeName!=OpCmpIsNotNull)
	{

    	e = testCompatibility(op1Type.typeID, op2Type.typeID, flag);
	    OOSQL_CHECK_ERR(e);
	    if(flag == SM_FALSE)
		{
			m_errorMessage->Clear();
			m_errorMessage->Append("The type of the value used at ");
			ERRORMESSAGE_APPEND_POSITION(m_errorMessage, op1);
			m_errorMessage->Append(" is not compatible with the type of the value used at colume ");
			ERRORMESSAGE_APPEND_POSITION(m_errorMessage, op2);
			m_errorMessage->Append(".");
	
	        OOSQL_ERR(eTYPE_ERROR_OOSQL);
	    }
	}

	epElements[0].exprKind      = EXPR_KIND_OPER;
    e = nodeNameToOperatorID(AST(s).nodeName, epElements[0].oper.operatorId);
    OOSQL_CHECK_ERR(e);
    epElements[0].oper.operatorType = operatorType;

    // make operand nodes;
    e = makeOperandInExprPool(op1Type, exprPoolIndex);
    epElements[0].oper.operand1 = exprPoolIndex;
    OOSQL_CHECK_ERR(e);

    if ( AST(s).nodeName != OpCmpIsNull && AST(s).nodeName != OpCmpIsNotNull )
	{
		e = makeOperandInExprPool(op2Type, exprPoolIndex);
		epElements[0].oper.operand2 = exprPoolIndex;
		OOSQL_CHECK_ERR(e);
	}
	else
	{
		epElements[0].oper.operand2.setNull();       // no second operand
	}

	epElements[0].oper.operand3.setNull();       // no third operand

    returnType.set(TYPEID_BOOL, PT_EXPRPOOL, epElements.getPoolIndex());

    e = exprElementSetTypeInfo(epElements, returnType);
    OOSQL_CHECK_ERR(e);

    m_recentOperGroup = oldOperGroup;

    return eNOERROR;
}

Four OQL_ASTtoGDS::smtChkQuBln( 
    ASTNodeIdx  node,
    ReturnType& returnType
)
{
    ASTNodeIdx       s, op1, op2;
    ReturnType       op1Type, op2Type;
    ExprPoolElements epElements(m_pool->exprPool);
    OperatorGroup    oldOperGroup;
    Four             e;
    Boolean          flag;
    OperatorType     operatorType;
    ExprPoolIndex    exprPoolIndex;

    oldOperGroup    = m_recentOperGroup;
    m_recentOperGroup = OG_BOOLEAN;
    
    s = AST(node).son;

    if(AST(s).nodeName == OpBlnNot)
    {
		ExprPoolIndex   oldConnectionPoint;

        // unary operator
        switch(AST(s).nodeName)
        {
        case OpBlnNot:
			// construct operator node
			epElements = m_pool->exprPool.addNewEntry();
			epElements[0].operandType = EXPR_OPERANDTYPE_CONTAIN_NOMETHOD;
			epElements[0].refCount    = 0;
	
            op1 = AST(s).brother;

			oldConnectionPoint = m_connectionPoint;
			if(m_connectionPoint == NULL_POOLINDEX)
			{
				m_connectionPoint   = epElements.getPoolIndex();
				m_conditionNotBound = SM_TRUE;
			}

			e = smtChkQuery(op1, op1Type, SM_TRUE);  OOSQL_CHECK_ERR(e);

			if(op1Type.typeID == TYPEID_ID)
			{
				m_errorMessage->Clear();
				m_errorMessage->Append("Undefined Identifier is used at ");
				ERRORMESSAGE_APPEND_POSITION(m_errorMessage, op1);
				
				OOSQL_ERR(eSYNTAX_ERROR_OOSQL)
			}
			e = testCompatibility(op1Type.typeID, TYPEID_BOOL, flag);
			OOSQL_CHECK_ERR(e);
			if(!flag)
			{
				m_errorMessage->Clear();
				m_errorMessage->Append("The type of the value used at ");
				ERRORMESSAGE_APPEND_POSITION(m_errorMessage, op1);
				m_errorMessage->Append(" is not a bool where one of those is expected.");
				
				OOSQL_ERR(eTYPE_ERROR_OOSQL);
			}

            operatorType = ARITHMETIC_OPERATION;

			if(m_connectionPoint!= NULL_POOLINDEX && oldConnectionPoint == NULL_POOLINDEX && m_conditionNotBound && m_progressStatus != HAVING_CLAUSE)
			{
				e = m_gds->addNodeToUnusedConditionList(m_pool, m_connectionPoint);
				OOSQL_CHECK_ERR(e);
			}
			m_connectionPoint = oldConnectionPoint;
			break;
			
        default:
            OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
            break;  
        }
        // Construct operator node
		epElements[0].exprKind      = EXPR_KIND_OPER;
		e = nodeNameToOperatorID(AST(s).nodeName, epElements[0].oper.operatorId);
		OOSQL_CHECK_ERR(e);
		epElements[0].oper.operatorType = operatorType;
		
		// make operand nodes;
		e = makeOperandInExprPool(op1Type, exprPoolIndex);
		epElements[0].oper.operand1 = exprPoolIndex;
		OOSQL_CHECK_ERR(e);
		
        epElements[0].oper.operand2.setNull();       // no second operand
		epElements[0].oper.operand3.setNull();       // no third operand
		
		returnType.set(TYPEID_BOOL, PT_EXPRPOOL, epElements.getPoolIndex());
		e = exprElementSetTypeInfo(epElements, returnType);
		OOSQL_CHECK_ERR(e);
    }
    else
    {
        ExprPoolIndex   oldConnectionPoint;

        // construct operator node
        epElements = m_pool->exprPool.addNewEntry();
        epElements[0].operandType = EXPR_OPERANDTYPE_CONTAIN_NOMETHOD;
        epElements[0].refCount    = 0;

        // binary operator
        switch (AST(s).nodeName) 
		{ 
        case OpBlnAnd:
            op1 = AST(s).brother;
            op2 = AST(op1).brother;
            
            e = smtChkQuery(op1, op1Type, SM_TRUE);  OOSQL_CHECK_ERR(e);
            e = smtChkQuery(op2, op2Type, SM_TRUE);  OOSQL_CHECK_ERR(e);


			if(op1Type.typeID == TYPEID_ID)
			{
				m_errorMessage->Clear();
				m_errorMessage->Append("Undefined Identifier is used at ");
				ERRORMESSAGE_APPEND_POSITION(m_errorMessage, op1);

				OOSQL_ERR(eSYNTAX_ERROR_OOSQL)
			}
			e = testCompatibility(op1Type.typeID, TYPEID_BOOL, flag);
			OOSQL_CHECK_ERR(e);
            if(!flag)
			{
				m_errorMessage->Clear();
				m_errorMessage->Append("The type of the value used at ");
				ERRORMESSAGE_APPEND_POSITION(m_errorMessage, op1);
				m_errorMessage->Append(" is not a bool where one of those is expected.");

                OOSQL_ERR(eTYPE_ERROR_OOSQL);
			}

			if(op2Type.typeID == TYPEID_ID)
			{
				m_errorMessage->Clear();
				m_errorMessage->Append("Undefined Identifier is used at ");
				ERRORMESSAGE_APPEND_POSITION(m_errorMessage, op2);

				OOSQL_ERR(eSYNTAX_ERROR_OOSQL)
			}
			e = testCompatibility(op2Type.typeID, TYPEID_BOOL, flag);
			OOSQL_CHECK_ERR(e);
            if(!flag)
			{
				m_errorMessage->Clear();
				m_errorMessage->Append("The type of the value used at ");
				ERRORMESSAGE_APPEND_POSITION(m_errorMessage, op2);
				m_errorMessage->Append(" is not a bool where one of those is expected.");

                OOSQL_ERR(eTYPE_ERROR_OOSQL);
			}

            operatorType = ARITHMETIC_OPERATION;

            break;

        case OpBlnOr:
			op1 = AST(s).brother;
            op2 = AST(op1).brother;

			oldConnectionPoint = m_connectionPoint;
			if(m_connectionPoint == NULL_POOLINDEX)
			{
				m_connectionPoint   = epElements.getPoolIndex();
				m_conditionNotBound = SM_TRUE;
			}

			e = smtChkQuery(op1, op1Type, SM_TRUE);  OOSQL_CHECK_ERR(e);
			e = smtChkQuery(op2, op2Type, SM_TRUE);  OOSQL_CHECK_ERR(e);

			if(op1Type.typeID == TYPEID_ID)
			{
				m_errorMessage->Clear();
				m_errorMessage->Append("Undefined Identifier is used at ");
				ERRORMESSAGE_APPEND_POSITION(m_errorMessage, op1);

				OOSQL_ERR(eSYNTAX_ERROR_OOSQL)
			}
			e = testCompatibility(op1Type.typeID, TYPEID_BOOL, flag);
			OOSQL_CHECK_ERR(e);
			if(!flag)
			{
				m_errorMessage->Clear();
				m_errorMessage->Append("The type of the value used at ");
				ERRORMESSAGE_APPEND_POSITION(m_errorMessage, op1);
				m_errorMessage->Append(" is not a bool where one of those is expected.");

				OOSQL_ERR(eTYPE_ERROR_OOSQL);
			}

			if(op2Type.typeID == TYPEID_ID)
			{
				m_errorMessage->Clear();
				m_errorMessage->Append("Undefined Identifier is used at ");
				ERRORMESSAGE_APPEND_POSITION(m_errorMessage, op2);

				OOSQL_ERR(eSYNTAX_ERROR_OOSQL)
			}
			e = testCompatibility(op2Type.typeID, TYPEID_BOOL, flag);
			OOSQL_CHECK_ERR(e);
			if(!flag)
			{
				m_errorMessage->Clear();
				m_errorMessage->Append("The type of the value used at ");
				ERRORMESSAGE_APPEND_POSITION(m_errorMessage, op2);
				m_errorMessage->Append(" is not a bool where one of those is expected.");

				OOSQL_ERR(eTYPE_ERROR_OOSQL);
			}

			operatorType = ARITHMETIC_OPERATION;
        
			if(m_connectionPoint!= NULL_POOLINDEX && oldConnectionPoint == NULL_POOLINDEX && m_conditionNotBound && m_progressStatus != HAVING_CLAUSE)
			{
				e = m_gds->addNodeToUnusedConditionList(m_pool, m_connectionPoint);
				OOSQL_CHECK_ERR(e);
			}
	        m_connectionPoint = oldConnectionPoint;
            break;

        default:
            OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
            break;  
        }
        
        // check type compatibility between op1 and op2
        e = testCompatibility(op1Type.typeID, op2Type.typeID, flag);
        OOSQL_CHECK_ERR(e);
        if(flag == SM_FALSE)
		{
			m_errorMessage->Clear();
			m_errorMessage->Append("The type of the value used at ");
			ERRORMESSAGE_APPEND_POSITION(m_errorMessage, op1);
			m_errorMessage->Append(" is not compatible with the type of the value used at ");
			ERRORMESSAGE_APPEND_POSITION(m_errorMessage, op2);
			m_errorMessage->Append(".");

            OOSQL_ERR(eTYPE_ERROR_OOSQL);
        }

        epElements[0].exprKind      = EXPR_KIND_OPER;
        e = nodeNameToOperatorID(AST(s).nodeName, epElements[0].oper.operatorId);
        OOSQL_CHECK_ERR(e);
        epElements[0].oper.operatorType = operatorType;

        // make operand nodes;
        e = makeOperandInExprPool(op1Type, exprPoolIndex);
        epElements[0].oper.operand1 = exprPoolIndex;
        OOSQL_CHECK_ERR(e);
        e = makeOperandInExprPool(op2Type, exprPoolIndex);
        epElements[0].oper.operand2 = exprPoolIndex;
        OOSQL_CHECK_ERR(e);
		epElements[0].oper.operand3.setNull();       // no third operand

        returnType.set(TYPEID_BOOL, PT_EXPRPOOL, epElements.getPoolIndex());


        e = exprElementSetTypeInfo(epElements, returnType);
        OOSQL_CHECK_ERR(e);
    }

    m_recentOperGroup = oldOperGroup;

    return eNOERROR;
}

Four OQL_ASTtoGDS::smtChkQuCns( 
    ASTNodeIdx  node,
    ReturnType& returnType 
)
{
    ASTNodeIdx          s, s1, s2, b;
    ReturnType          sType, s1Type, s2Type, bType;
    TypeID              elementTypeID, attrTypeID;
    Four                fromInteger, toInteger, i;
    Four                constructStartIndex, constructSize;
    CataClassInfo       classInfo;
    CataAttrInfo        attrInfo;
    Four                e;
    TypeID              typeID;
    Boolean             flag;

    ObjectPoolElements     opElements(m_pool->objectPool);
    StructurePoolElements  spElements(m_pool->structurePool);
    CollectionPoolElements cpElements(m_pool->collectionPool);
    IntegerPoolElements    ipElements(m_pool->intPool);
    ValuePoolElements      vpElements(m_pool->valuePool);
    ArgumentPoolElements   apElements(m_pool->argumentPool);
    ConstructPoolElements  conspElements(m_pool->constructPool);


    switch ( AST(node).nodeName ) 
    {
    case QuCnsObj:
        s = AST(node).son;

        // check object name
        e = isID_ClassName(s, classInfo, flag);
        OOSQL_CHECK_ERR(e);
        if(!flag)
		{
			SimpleString classname;

			e = getStringFromID_Node(classname, s);
			OOSQL_CHECK_ERR(e);

			m_errorMessage->Clear();
			m_errorMessage->Append("The class '");
			m_errorMessage->Append((char*)classname);
			m_errorMessage->Append("' is not defined in the database.");

            OOSQL_ERR(eCLASSNOTDEFINED_OOSQL);
		}

        // construct a node in objectPool
        opElements = m_pool->objectPool.addNewEntry();
        opElements[0].classInfo = classInfo;

        s = AST(s).brother;
        b = AST(s).brother;

        constructStartIndex = -1;
        constructSize       = 0;
        while(s != null)
        {
            // check type of s : it must be ID
            if(AST(s).nodeName != ID)
                OOSQL_ERR(eBADASTNODE_OOSQL);

            // check value
            e = smtChkQuery(b, bType);  
            OOSQL_CHECK_ERR(e);

            spElements = m_pool->structurePool.addNewEntry();
            if(constructStartIndex == -1)
                constructStartIndex = spElements.startIndex;
            constructSize ++;

            spElements[0].name.startIndex = m_pool->stringIndexPool[AST(s).tokenVal];
            spElements[0].name.size       = m_pool->stringIndexPool[AST(s).tokenVal + 1] -
                                            m_pool->stringIndexPool[AST(s).tokenVal] - 1;

            apElements = m_pool->argumentPool.addNewEntry();
            switch(bType.poolType)
            {
            case PT_PATHEXPRPOOL:
                apElements[0].argumentKind = ARGUMENT_KIND_PATHEXPR;
                apElements[0].pathExpr.setPoolIndex(bType.poolIndex);
                break;

            case PT_VALUEPOOL:
                apElements[0].argumentKind = ARGUMENT_KIND_VALUE;
                apElements[0].value.setPoolIndex(bType.poolIndex);
                break;

            case PT_FUNCPOOL:
                apElements[0].argumentKind = ARGUMENT_KIND_FUNC;
                apElements[0].func.setPoolIndex(bType.poolIndex);
                break;

            case PT_DOMAINPOOL:
                apElements[0].argumentKind = ARGUMENT_KIND_DOMAIN;
                apElements[0].domain.setPoolIndex(bType.poolIndex);
                break;

            case PT_AGGRFUNCPOOL:
                apElements[0].argumentKind = ARGUMENT_KIND_AGGRFUNC;
                apElements[0].aggrFunc.setPoolIndex(bType.poolIndex);
                break;

            default:
                OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
            }
            spElements[0].structure = apElements.getPoolIndex();

            // check attribute name
            e = isID_AttributeName(s, classInfo, attrInfo, flag);
            OOSQL_CHECK_ERR(e);
            if(!flag)
			{
				char classname[MAXCLASSNAME];
				SimpleString attrname;

				e = getStringFromID_Node(attrname, s);
				OOSQL_CHECK_ERR(e);

				e = m_catalog->class_CataClassInfo_to_ClassName(classInfo, classname);
				OOSQL_CHECK_ERR(e);

				m_errorMessage->Clear();
				m_errorMessage->Append("The attribute '");
				m_errorMessage->Append((char*)attrname);
				m_errorMessage->Append("' is not defined in the class '");
				m_errorMessage->Append(classname);
				m_errorMessage->Append("'.");

                OOSQL_ERR(eATTRNOTDEFINED_OOSQL);
			}

            // check type 
            e = m_catalog->attr_GetTypeID(classInfo, attrInfo, attrTypeID);
            OOSQL_CHECK_ERR(e);

            e = testCompatibility(bType.typeID, attrTypeID, flag);
            OOSQL_CHECK_ERR(e);

            if(!flag)
			{
				char classname[MAXCLASSNAME];
				char attrname[MAXATTRNAME];

				e = m_catalog->class_CataClassInfo_to_ClassName(classInfo, classname);
				OOSQL_CHECK_ERR(e);

				e = m_catalog->attr_CataAttrInfo_to_AttrName(classInfo, attrInfo, attrname);
				OOSQL_CHECK_ERR(e);

				m_errorMessage->Clear();
				m_errorMessage->Append("The type of the value used at ");
				ERRORMESSAGE_APPEND_POSITION(m_errorMessage, b);
				m_errorMessage->Append(" is not compatible with the type of ");
				m_errorMessage->Append(classname);
				m_errorMessage->Append(".");
				m_errorMessage->Append(attrname);
				m_errorMessage->Append(".");

                OOSQL_ERR(eTYPE_ERROR_OOSQL);
			}

            s = AST(b).brother;
            b = AST(s).brother;
        }

        opElements[0].structure.setPoolIndex(constructStartIndex, constructSize);

        conspElements = m_pool->constructPool.addNewEntry();
        conspElements[0].consKind = CONSTRUCT_KIND_OBJECT;
        conspElements[0].object= opElements.getPoolIndex();

        returnType.set(TYPEID_STRUCTURE, PT_CONSTRUCTPOOL, conspElements.getPoolIndex());
        break;

    case QuCnsStruct:
        s = AST(node).son;
        b = AST(s).brother;
        
        constructStartIndex = -1;
        constructSize       = 0;
        while(s != null)
        {
            // check type of s : it must be ID
            if(AST(s).nodeName != ID)
                OOSQL_ERR(eBADASTNODE_OOSQL);

            // check value
            e = smtChkQuery(b, bType);
            OOSQL_CHECK_ERR(e);

            spElements = m_pool->structurePool.addNewEntry();
            if(constructStartIndex == -1)
                constructStartIndex = spElements.startIndex;
            constructSize ++;

            spElements[0].name.startIndex = m_pool->stringIndexPool[AST(s).tokenVal];
            spElements[0].name.size       = m_pool->stringIndexPool[AST(s).tokenVal + 1] -
                                            m_pool->stringIndexPool[AST(s).tokenVal] - 1;


            apElements = m_pool->argumentPool.addNewEntry();
            switch(bType.poolType)
            {
            case PT_PATHEXPRPOOL:
                apElements[0].argumentKind = ARGUMENT_KIND_PATHEXPR;
                apElements[0].pathExpr.setPoolIndex(bType.poolIndex);
                break;

            case PT_VALUEPOOL:
                apElements[0].argumentKind = ARGUMENT_KIND_VALUE;
                apElements[0].value.setPoolIndex(bType.poolIndex);
                break;

            case PT_FUNCPOOL:
                apElements[0].argumentKind = ARGUMENT_KIND_FUNC;
                apElements[0].func.setPoolIndex(bType.poolIndex);
                break;

            case PT_DOMAINPOOL:
                apElements[0].argumentKind = ARGUMENT_KIND_DOMAIN;
                apElements[0].domain.setPoolIndex(bType.poolIndex);
                break;

            case PT_AGGRFUNCPOOL:
                apElements[0].argumentKind = ARGUMENT_KIND_AGGRFUNC;
                apElements[0].aggrFunc.setPoolIndex(bType.poolIndex);
                break;

            default:
                OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
            }
            spElements[0].structure = apElements.getPoolIndex();

            s = AST(b).brother;
            b = AST(s).brother;
        }

        conspElements = m_pool->constructPool.addNewEntry();
        conspElements[0].consKind = CONSTRUCT_KIND_STRUCTURE;
        conspElements[0].structure.setPoolIndex(constructStartIndex, constructSize);

        returnType.set(TYPEID_STRUCTURE, PT_CONSTRUCTPOOL, conspElements.getPoolIndex());
        break;

    case QuCnsSet:
    case QuCnsBag:
    case QuCnsLst:
    case QuCnsArr:
        cpElements = m_pool->collectionPool.addNewEntry();
        
        s = AST(node).son;

        e = smtChkQuery(s, sType);
        OOSQL_CHECK_ERR(e);
        s = AST(s).brother;

        elementTypeID = sType.typeID;

        e = nodeName_to_CollectionTypeID(AST(node).nodeName, cpElements[0].collectionType);
        OOSQL_CHECK_ERR(e);
        e = typeID_to_ColElementTypeID(elementTypeID, cpElements[0].elementType);
        OOSQL_CHECK_ERR(e);
        cpElements[0].value.startIndex = sType.poolIndex.startIndex;
        cpElements[0].value.size       = sType.poolIndex.size;

        while(s != AST_NULL)
        {
            e = smtChkQuery(s, sType); 
            OOSQL_CHECK_ERR(e);

            e = testCompatibility(sType.typeID, elementTypeID, flag);
            OOSQL_CHECK_ERR(e);
            if(flag == SM_FALSE)
			{
				m_errorMessage->Clear();
				m_errorMessage->Append("The type of the value used at ");
				ERRORMESSAGE_APPEND_POSITION(m_errorMessage, s);
				m_errorMessage->Append(" is not compatible with the type of its elements.");

                OOSQL_ERR(eCOLLECTIONELEMENT_ERROR_OOSQL);
			}

            cpElements[0].value.size += sType.poolIndex.size;

            s = AST(s).brother;
        }

        conspElements = m_pool->constructPool.addNewEntry();
        conspElements[0].consKind   = CONSTRUCT_KIND_COLLECTION;
        conspElements[0].collection = cpElements.getPoolIndex();

        e = collectionTypeID_to_TypeID(cpElements[0].collectionType, typeID);
        OOSQL_CHECK_ERR(e);
        returnType.set(typeID, PT_CONSTRUCTPOOL, conspElements.getPoolIndex());
        returnType.elementTypeID = elementTypeID;
        break;

    case QuCnsLstrn:
        cpElements = m_pool->collectionPool.addNewEntry();
        
        s1 = AST(node).son;
        s2 = AST(s1).brother;

        e = smtChkQuery(s1, s1Type);    OOSQL_CHECK_ERR(e);
        e = smtChkQuery(s2, s2Type);    OOSQL_CHECK_ERR(e);

        // check type of s1 and s2
        if(!(s1Type.typeID == TYPEID_INT && s1Type.poolType == PT_VALUEPOOL &&
             m_pool->valuePool[s1Type.poolIndex.startIndex].valueKind == VALUE_KIND_INTEGER))
		{
			m_errorMessage->Clear();
			m_errorMessage->Append("The type of the value used at ");
			ERRORMESSAGE_APPEND_POSITION(m_errorMessage, s1);
			m_errorMessage->Append(" is not a integer where one of those is expected.");

            OOSQL_ERR(eCOLLECTIONELEMENT_ERROR_OOSQL);
		}

		if(!(s2Type.typeID == TYPEID_INT && s2Type.typeID == PT_VALUEPOOL &&
             m_pool->valuePool[s2Type.poolIndex.startIndex].valueKind == VALUE_KIND_INTEGER))
		{
			m_errorMessage->Clear();
			m_errorMessage->Append("The type of the value used at ");
			ERRORMESSAGE_APPEND_POSITION(m_errorMessage, s2);
			m_errorMessage->Append(" is not a integer where one of those is expected.");

            OOSQL_ERR(eCOLLECTIONELEMENT_ERROR_OOSQL);
		}

		elementTypeID = s1Type.typeID;

        // check range of [from .. to]
        fromInteger = m_pool->intPool[m_pool->valuePool[s1Type.poolIndex.startIndex].integer.startIndex];
        toInteger   = m_pool->intPool[m_pool->valuePool[s2Type.poolIndex.startIndex].integer.startIndex];

        if(fromInteger >= toInteger)
		{
			m_errorMessage->Clear();
			m_errorMessage->Append("From index is more than To index in range definition at ");
			ERRORMESSAGE_APPEND_POSITION(m_errorMessage, node);
			m_errorMessage->Append(".");

            OOSQL_ERR(eCOLLECTIONELEMENT_ERROR_OOSQL);
		}

        // generate intermediate values
        for(i = fromInteger + 1;i <= toInteger - 1;i ++)
        {
            ipElements = m_pool->intPool.addNewEntry();
            ipElements[0] = i;

            vpElements = m_pool->valuePool.addNewEntry();
            vpElements[0].valueKind = VALUE_KIND_INTEGER;
            vpElements[0].integer   = ipElements.getPoolIndex();
        }
        
        e = nodeName_to_CollectionTypeID(QuCnsLstrn, cpElements[0].collectionType);
        OOSQL_CHECK_ERR(e);
        e = typeID_to_ColElementTypeID(elementTypeID, cpElements[0].elementType);
        OOSQL_CHECK_ERR(e);
        cpElements[0].value.setPoolIndex(s1Type.poolIndex.startIndex,
                                         toInteger - fromInteger + 1);

        conspElements = m_pool->constructPool.addNewEntry();
        conspElements[0].consKind = CONSTRUCT_KIND_COLLECTION;
        conspElements[0].collection = cpElements.getPoolIndex();

        e = collectionTypeID_to_TypeID(cpElements[0].collectionType, typeID);
        OOSQL_CHECK_ERR(e);
        returnType.set(typeID, PT_CONSTRUCTPOOL, conspElements.getPoolIndex()); 
        returnType.elementTypeID = elementTypeID;
        break;
    }

    return eNOERROR;
}

Four OQL_ASTtoGDS::smtChkQuAccPaex(
    ASTNodeIdx  node,
    ReturnType& returnType
)
{
    CataClassInfo           lastClassInfo;
    PathExprPoolElements    currentPathExpr(m_pool->pathExprPool);
    ExprPoolElements        epElements(m_pool->exprPool);
    ExprPoolIndex           oldConnectionPoint;
    Boolean                 pathExprInExprPool;
    Four                    e;

    // init variables used for constructing path expr]
    currentPathExpr.setNull();
    lastClassInfo = -1;

    oldConnectionPoint = m_connectionPoint;
    pathExprInExprPool = SM_FALSE;
    if((m_progressStatus == WHERE_CLAUSE || m_progressStatus == HAVING_CLAUSE) &&
       m_connectionPoint == NULL_POOLINDEX &&
       (m_recentOperGroup == OG_NONE || m_recentOperGroup == OG_BOOLEAN))
    {
        epElements = m_pool->exprPool.addNewEntry();
        
        epElements[0].operandType = EXPR_OPERANDTYPE_CONTAIN_NOMETHOD;
        epElements[0].refCount    = 0;

        m_connectionPoint    = epElements.getPoolIndex();
        pathExprInExprPool = SM_TRUE;
		m_conditionNotBound  = SM_TRUE;
    }

    // construct path expression
    e = smtChkQuAccPaexRecursive(node, returnType, lastClassInfo, currentPathExpr);
    if(e == eNOT_PATHEXPR_ID_OOSQL)
    {
        m_connectionPoint = oldConnectionPoint;
        return e;
    }
    else
    {
        OOSQL_CHECK_ERR(e);
    }
	
	if(m_connectionPoint!= NULL_POOLINDEX && oldConnectionPoint == NULL_POOLINDEX && m_conditionNotBound && m_progressStatus != HAVING_CLAUSE)
	{
		e = m_gds->addNodeToUnusedConditionList(m_pool, m_connectionPoint);
		OOSQL_CHECK_ERR(e);
	}
    m_connectionPoint = oldConnectionPoint;

    // construct query graph
    e = qg_consPathExpr(currentPathExpr.getPoolIndex());
    OOSQL_CHECK_ERR(e);

    if(pathExprInExprPool)
    {
        epElements[0].exprKind = EXPR_KIND_PATHEXPR;
        epElements[0].pathExpr.setPoolIndex(returnType.poolIndex);

        returnType.set(returnType.typeID, PT_EXPRPOOL, epElements.getPoolIndex());

        e = exprElementSetTypeInfo(epElements, returnType);
        OOSQL_CHECK_ERR(e);
    }
    
    return eNOERROR;
}

Four OQL_ASTtoGDS::smtChkQuAccPaexRecursive(
    ASTNodeIdx              node,
    ReturnType&             returnType,
    CataClassInfo&          lastClassInfo,
    PathExprPoolElements&   currentPathExpr
)
{
    ASTNodeIdx          b1,b2;
    ReturnType          b1Type, b2Type;
    TypeID              attrTypeID, methodTypeID;
    Four                i, count, targetListIndex;
    SimpleString        idString;
    NameAssociatedInfo  idAssociatedInfo;
    CataClassInfo       classInfo;
    CataAttrInfo        attrInfo;
    CataMethodInfo      methodInfo;
    Four                e;
    Boolean             flag;
    Four                nArguments;
	TargetListPoolIndex targetList;
	
	PathExprPoolElements    ppElements(m_pool->pathExprPool);

    classInfo = lastClassInfo;

    switch (AST(node).nodeName) 
    {
    case QuAccPaex:
        b1 = AST(node).son;
        b2 = AST(b1).brother;

        if(AST(b1).nodeName == QuCnvType)
        {
            e = smtChkQuAccPaexRecursive(b1, b1Type, lastClassInfo, currentPathExpr);
            OOSQL_CHECK_ERR(e);

            // save pathexpr
            ppElements = currentPathExpr;

            e = smtChkQuAccPaexRecursive(b2, b2Type, lastClassInfo, currentPathExpr);
            OOSQL_CHECK_ERR(e);

            // set type casting information in b2
            ppElements[ppElements.size].typeCastingFlag = SM_TRUE;
            ppElements[ppElements.size].typeCastedClass = b1Type.classInfo; // type casted class
        }
        else if(AST(b1).nodeName == QuCnvTypeStar)
        {
            e = smtChkQuAccPaexRecursive(b1, b1Type, lastClassInfo, currentPathExpr);
            OOSQL_CHECK_ERR(e);

            // save pathexpr
            ppElements = currentPathExpr;

            e = smtChkQuAccPaexRecursive(b2, b2Type, lastClassInfo, currentPathExpr);
            OOSQL_CHECK_ERR(e);

            // set type casting information in b2
            ppElements[ppElements.size].domainSubstFlag  = SM_TRUE;
            ppElements[ppElements.size].substitutedClass = b1Type.classInfo; // type casted class
        }
        else
        {
            e = smtChkQuAccPaexRecursive(b1, b1Type, lastClassInfo, currentPathExpr);
            OOSQL_CHECK_ERR(e);
            e = smtChkQuAccPaexRecursive(b2, b2Type, lastClassInfo, currentPathExpr);
            OOSQL_CHECK_ERR(e);
        }

        returnType.set(b2Type.typeID, PT_PATHEXPRPOOL, currentPathExpr.getPoolIndex());
        returnType.classInfo = lastClassInfo;
        break;

    case ID:
        // check if id is a class name or a some class's attribute name or 
        // a symbol in the m_nameStack
        // if id is a symbol in the m_nameStack, the symbol must have been created in
        // smtChkClFr(). id in the m_nameStack must be placed in the head of pathexpr.
        e = isID_AliasName(node, idAssociatedInfo, flag);
        OOSQL_CHECK_ERR(e);
        if(flag && currentPathExpr.size == 0)
        {
            if(idAssociatedInfo.kind != NA_KIND_FROM)
			{
				m_errorMessage->Clear();
				m_errorMessage->Append("The path expression flagment at ");
				ERRORMESSAGE_APPEND_POSITION(m_errorMessage, node);
				m_errorMessage->Append(" is invalid.");

                OOSQL_ERR(eINVALID_PATHEXPR_OOSQL);
			}

            // construct path expression element
            ppElements = m_pool->pathExprPool.addNewEntry();
            currentPathExpr.startIndex = ppElements.startIndex;
            currentPathExpr.size ++;

            classInfo = lastClassInfo = m_pool->targetListPool[idAssociatedInfo.target.startIndex].collectionInfo.classInfo;
            ppElements[0].aliasName   = m_pool->targetListPool[idAssociatedInfo.target.startIndex].aliasName;

            ppElements[0].classInfo         = classInfo;
            ppElements[0].fromAttrKind      = PATHEXPR_KIND_CLASS;
            ppElements[0].domainSubstFlag   = SM_FALSE;
            ppElements[0].substitutedClass  = -1;
            ppElements[0].typeCastingFlag   = SM_FALSE;
            ppElements[0].typeCastedClass   = -1;
            ppElements[0].planNo.setNull();
            ppElements[0].pathExprKind      = 0;

            //returnType.set(TYPEID_OID, PT_NONE, NULL_POOLINDEX);
            returnType.set(TYPEID_OID, PT_PATHEXPRPOOL, currentPathExpr.getPoolIndex());

            returnType.classInfo = lastClassInfo;
            return eNOERROR;
        }

        e = isID_ClassName(node, classInfo, flag);
        OOSQL_CHECK_ERR(e);

        if(flag)
        {
			e = m_gds->getTargetList(m_pool, targetList); OOSQL_CHECK_ERR(e);
	        for(i = targetList.startIndex;i < targetList.startIndex + targetList.size;i ++)
   	     	{
   	       	  	if(m_pool->targetListPool[i].collectionInfo.classInfo == classInfo)
            	{
	            	break;
            	}
        	}
        	if(i == targetList.startIndex + targetList.size)
        	{
        		flag = SM_FALSE;
        		classInfo = -1;
			}
		}

        if(flag && currentPathExpr.size == 0)
        {
            lastClassInfo = classInfo;

            // construct path expression element
            ppElements = m_pool->pathExprPool.addNewEntry();
            currentPathExpr.startIndex = ppElements.startIndex;
            currentPathExpr.size ++;

            ppElements[0].classInfo            = classInfo;
            ppElements[0].fromAttrKind         = PATHEXPR_KIND_CLASS;
            ppElements[0].aliasName.setNull();
            ppElements[0].domainSubstFlag      = SM_FALSE;
            ppElements[0].substitutedClass     = -1;        
            ppElements[0].typeCastingFlag      = SM_FALSE;
            ppElements[0].typeCastedClass      = -1;
            ppElements[0].planNo.setNull();
            ppElements[0].pathExprKind         = 0;

            // check binding ambiguity
            // scan "from clause", there must be only one class which matches "classInfo"
            count = 0;
			e = m_gds->getTargetList(m_pool, targetList); OOSQL_CHECK_ERR(e);
            for(i = targetList.startIndex;i < targetList.startIndex + targetList.size;i ++)
            {
                if(m_pool->targetListPool[i].collectionInfo.classInfo == classInfo)
                {
                    if(count == 0)
                        targetListIndex = i;
                    count ++;
                }
            }

            if(count != 1)
			{
				char classname[MAXCLASSNAME];

				e = m_catalog->class_CataClassInfo_to_ClassName(classInfo, classname);
				OOSQL_CHECK_ERR(e);

				m_errorMessage->Clear();
				m_errorMessage->Append("The class '");
				m_errorMessage->Append(classname);
				m_errorMessage->Append("' is not specified in FROM caluse.");

                OOSQL_ERR(eCLASSNOTINTARGETLIST_OOSQL);
			}

            // if there is one class and it has alias name, then set alias name
            ppElements[0].aliasName = m_pool->targetListPool[targetListIndex].aliasName;
            
            returnType.set(TYPEID_OID, PT_PATHEXPRPOOL, currentPathExpr.getPoolIndex());
            returnType.classInfo = classInfo;
            return eNOERROR;
        }

        // if classInfo = -1 then it searches the attribute from all possible classes
        e = isID_AttributeName(node, classInfo, attrInfo, flag);
        OOSQL_CHECK_ERR(e);
        if(flag)
        {
            if(currentPathExpr.size == 0)
            {
                // if this position is the head of the path expression
                // then construct the node representing class
                ppElements = m_pool->pathExprPool.addNewEntry();
                currentPathExpr.startIndex = ppElements.startIndex;
                currentPathExpr.size ++;

                ppElements[0].classInfo            = classInfo;
                ppElements[0].fromAttrKind         = PATHEXPR_KIND_CLASS;
                ppElements[0].aliasName.setNull();
                ppElements[0].domainSubstFlag      = SM_FALSE; 
                ppElements[0].substitutedClass     = -1;        
                ppElements[0].typeCastingFlag      = SM_FALSE;
                ppElements[0].typeCastedClass      = -1;
                ppElements[0].planNo.setNull();
                ppElements[0].pathExprKind         = 0;
                
                // check binding ambiguity
                // scan "from clause", there must be only one class which matches "classInfo"
                count = 0;
				e = m_gds->getTargetList(m_pool, targetList); OOSQL_CHECK_ERR(e);
                for(i = targetList.startIndex;i < targetList.startIndex + targetList.size;i ++)
                {
                    if(m_pool->targetListPool[i].collectionInfo.classInfo == classInfo)
                    {
                        if(count == 0)
                            targetListIndex = i;
                        count ++;
                    }
                }

                if(count == 0)
                {
					char classname[MAXCLASSNAME];

					e = m_catalog->class_CataClassInfo_to_ClassName(classInfo, classname);
					OOSQL_CHECK_ERR(e);

					m_errorMessage->Clear();
					m_errorMessage->Append("The class '");
					m_errorMessage->Append(classname);
					m_errorMessage->Append("' is not specified in FROM caluse.");

                    OOSQL_ERR(eCLASSNOTINTARGETLIST_OOSQL);
                }
                else if(count > 1)
                {
					char classname[MAXCLASSNAME];
					char attrname[MAXATTRNAME];

					e = m_catalog->class_CataClassInfo_to_ClassName(classInfo, classname);
					OOSQL_CHECK_ERR(e);

					e = m_catalog->attr_CataAttrInfo_to_AttrName(classInfo, attrInfo, attrname);
					OOSQL_CHECK_ERR(e);

					m_errorMessage->Clear();
					m_errorMessage->Append("The class '");
					m_errorMessage->Append(classname);
					m_errorMessage->Append("' is specified in multiple at FROM caluse and the attribute '");
					m_errorMessage->Append(attrname);
					m_errorMessage->Append("' is used ambiguously in the query.");

                    OOSQL_ERR(eBINDING_AMBIGUITY_OOSQL);
                }

                ppElements[0].aliasName = m_pool->targetListPool[targetListIndex].aliasName;
            }

            // make new path expression
            ppElements = m_pool->pathExprPool.addNewEntry();
            if(currentPathExpr.size == 0)
                currentPathExpr.startIndex = ppElements.startIndex;
            currentPathExpr.size ++;

            ppElements[0].classInfo         = classInfo;
            ppElements[0].fromAttrKind      = PATHEXPR_KIND_ATTR;
            ppElements[0].attr.attrInfo     = attrInfo;
            ppElements[0].domainSubstFlag   = SM_FALSE;    
            ppElements[0].substitutedClass  = -1;       
            ppElements[0].typeCastingFlag   = SM_FALSE;    
            ppElements[0].typeCastedClass   = -1;
            ppElements[0].planNo.setNull();
            ppElements[0].pathExprKind      = 0;

            // make return type information
            e = m_catalog->attr_GetTypeID(classInfo, attrInfo, attrTypeID);
            OOSQL_CHECK_ERR(e);
            
            returnType.set(attrTypeID, PT_PATHEXPRPOOL, currentPathExpr.getPoolIndex());
            if(returnType.typeID == TYPEID_OID)
            {
                e = m_catalog->attr_GetReferencingClass(classInfo, attrInfo, returnType.classInfo);
                OOSQL_CHECK_ERR(e);
            }
            else
                returnType.classInfo        = -1;
            lastClassInfo = returnType.classInfo;

            return eNOERROR;
        }

        // if classInfo = -1 then it searches the method from all possible classes
        e = isID_MethodName(node, classInfo, methodInfo, flag);
        OOSQL_CHECK_ERR(e);
        if(flag)
        {
            // Only method which has no argument can be written like attribute
            // others must have parenthsis.
            e = m_catalog->method_GetN_Arguments(classInfo, methodInfo, nArguments);
            OOSQL_CHECK_ERR(e);
            if(nArguments == 0)
            {
                if(currentPathExpr.size == 0)
                {
                    // if this position is the head of the path expression
                    // then construct the node representing class
                    ppElements = m_pool->pathExprPool.addNewEntry();
                    currentPathExpr.startIndex = ppElements.startIndex;
                    currentPathExpr.size ++;

                    ppElements[0].classInfo            = classInfo;
                    ppElements[0].fromAttrKind         = PATHEXPR_KIND_CLASS;
                    ppElements[0].aliasName.setNull();
                    ppElements[0].domainSubstFlag      = SM_FALSE; 
                    ppElements[0].substitutedClass     = -1;        
                    ppElements[0].typeCastingFlag      = SM_FALSE;
                    ppElements[0].typeCastedClass      = -1;
                    ppElements[0].planNo.setNull();
                    ppElements[0].pathExprKind         = 0;

                    // check binding ambiguity
                    // scan "from clause", there must be only one class which matches "classInfo"
                    count = 0;
					e = m_gds->getTargetList(m_pool, targetList); OOSQL_CHECK_ERR(e);
                    for(i = targetList.startIndex;i < targetList.startIndex + targetList.size;i ++)
                    {
                        if(m_pool->targetListPool[i].collectionInfo.classInfo == classInfo)
                        {
                            if(count == 0)
                                targetListIndex = i;
                            count ++;
                        }
                    }

                    if(count != 1)
					{
						char classname[MAXCLASSNAME];

						e = m_catalog->class_CataClassInfo_to_ClassName(classInfo, classname);
						OOSQL_CHECK_ERR(e);

						m_errorMessage->Clear();
						m_errorMessage->Append("The class '");
						m_errorMessage->Append(classname);
						m_errorMessage->Append("' is not specified in FROM caluse.");

                        OOSQL_ERR(eCLASSNOTINTARGETLIST_OOSQL);
					}

                    // if there is one class and it has alias name, then set alias name
                    ppElements[0].aliasName = m_pool->targetListPool[targetListIndex].aliasName;
                }

                // make new path expression
                ppElements = m_pool->pathExprPool.addNewEntry();
                if(currentPathExpr.size == 0)
                    currentPathExpr.startIndex = ppElements.startIndex;
                currentPathExpr.size ++;

                ppElements[0].classInfo             = classInfo;
                ppElements[0].pathExprKind          = PATHEXPR_KIND_METHOD;
                ppElements[0].method.methodInfo     = methodInfo;
                ppElements[0].method.argument.setNull();
                ppElements[0].domainSubstFlag       = SM_FALSE;    
                ppElements[0].substitutedClass      = -1;       
                ppElements[0].typeCastingFlag       = SM_FALSE;    
                ppElements[0].typeCastedClass       = -1;
                ppElements[0].planNo.setNull();
                ppElements[0].pathExprKind         = 0;

                // make return type information
                e = m_catalog->method_GetReturnTypeID(classInfo, methodInfo, methodTypeID);
                OOSQL_CHECK_ERR(e);
                returnType.set(methodTypeID, PT_PATHEXPRPOOL, currentPathExpr.getPoolIndex());
                if(returnType.typeID == TYPEID_OID)
                {
                    e = m_catalog->method_GetReferencingClass(classInfo, methodInfo, returnType.classInfo);
                    OOSQL_CHECK_ERR(e);
                }
                else
                    returnType.classInfo = -1;
                lastClassInfo = returnType.classInfo;
                return eNOERROR;
            }
        }
        
        // ID is newly generated symbol so it is a ID not a path expression
        return eNOT_PATHEXPR_ID_OOSQL;
        break;

    case FnOrCltnobj:
        // this node has many children, whose number is not known
        // first child is an id, which represents method name
        // rest of them are argument list

        // FnOrCltnobj can appear in the head of pathexpr, not in the body
        if(currentPathExpr.size != 0)
		{
			m_errorMessage->Clear();
			m_errorMessage->Append("Function is used in the middle of path expression at ");
			ERRORMESSAGE_APPEND_POSITION(m_errorMessage, node);
			m_errorMessage->Append(".");

            OOSQL_ERR(eINVALID_PATHEXPR_OOSQL);
		}

        b1 = AST(node).son;
        b2 = AST(b1).brother;

        // if classInfo = -1 then it searches the attribute from all possible classes
        e = isID_MethodName(b1, classInfo, methodInfo, flag);
        OOSQL_CHECK_ERR(e);
        if(flag)
        {
            // this node is method node
            // make new path expression
            // construct path expression element

            // construct first node, which represents class
            ppElements = m_pool->pathExprPool.addNewEntry(2);
            currentPathExpr.startIndex = ppElements.startIndex;
            currentPathExpr.size += 2;

            ppElements[0].classInfo            = classInfo;
            ppElements[0].fromAttrKind         = PATHEXPR_KIND_CLASS;
            ppElements[0].aliasName.setNull();
            ppElements[0].domainSubstFlag      = SM_FALSE; 
            ppElements[0].substitutedClass     = -1;        
            ppElements[0].typeCastingFlag      = SM_FALSE;
            ppElements[0].typeCastedClass      = -1;
            ppElements[0].planNo.setNull();
            ppElements[0].pathExprKind         = 0;

            // construct second node, which represent method
            e = smtChkArguments(b2, b2Type, classInfo, methodInfo);
            OOSQL_CHECK_ERR(e);

            ppElements[1].classInfo             = classInfo;
            ppElements[1].fromAttrKind          = PATHEXPR_KIND_METHOD;
            ppElements[1].method.methodInfo     = methodInfo;
            ppElements[1].method.argument.setPoolIndex(b2Type.poolIndex);
            ppElements[1].domainSubstFlag       = SM_FALSE;    
            ppElements[1].substitutedClass      = -1;        
            ppElements[1].typeCastingFlag       = SM_FALSE;
            ppElements[1].typeCastedClass       = -1;
            ppElements[1].planNo.setNull();
            ppElements[1].pathExprKind         = 0;

            // make return type information
            e = m_catalog->method_GetReturnTypeID(classInfo, methodInfo, methodTypeID);
            OOSQL_CHECK_ERR(e);
            returnType.set(methodTypeID, PT_PATHEXPRPOOL, currentPathExpr.getPoolIndex());
            if(returnType.typeID == TYPEID_OID)
            {
                e = m_catalog->method_GetReferencingClass(classInfo, methodInfo, returnType.classInfo);
                OOSQL_CHECK_ERR(e);
            }
            else
                returnType.classInfo = -1;
            lastClassInfo = returnType.classInfo;
            return eNOERROR;
        }
        else
        {
            // this node is a collection, it can not appear in the path expression
			m_errorMessage->Clear();
			m_errorMessage->Append("Collection is used in the middle of path expression at ");
			ERRORMESSAGE_APPEND_POSITION(m_errorMessage, node);
			m_errorMessage->Append(".");

            OOSQL_ERR(eINVALID_PATHEXPR_OOSQL);
        }
            
        break;

    case QuCnvType:
    case QuCnvTypeStar:
        // this node has two children
        // first child is an id, which represents substituted class
        // second child is an pathexpr
        b1 = AST(node).son;
        b2 = AST(b1).brother;

        // check first child and get the information of it
        e = isID_ClassName(b1, classInfo, flag);
        OOSQL_CHECK_ERR(e);

        if(!flag)
		{
			m_errorMessage->Clear();
			m_errorMessage->Append("The Path expression at ");
			ERRORMESSAGE_APPEND_POSITION(m_errorMessage, node);
			m_errorMessage->Append(" is invalid.");

            OOSQL_ERR(eINVALID_PATHEXPR_OOSQL);
		}
        
        // check second child
        e = smtChkQuAccPaexRecursive(b2, b2Type, lastClassInfo, currentPathExpr);
        OOSQL_CHECK_ERR(e);

        // writing type casting information is done in calling procedure

        // make return type information
        returnType.set(b2Type.typeID, PT_PATHEXPRPOOL, currentPathExpr.getPoolIndex());
        returnType.classInfo = classInfo;       // change classInfo to type casted classInfo
        lastClassInfo        = b2Type.classInfo;

        break;

    case Method:
        b1 = AST(node).son;
        b2 = AST(b1).brother;

        classInfo = -1;     // this indicates it searches the method from all possible classes
        e = isID_MethodName(b1, classInfo, methodInfo, flag);
        OOSQL_CHECK_ERR(e);
        if(flag)
        {
            // this node is method node
            // make new path expression
            // construct path expression element

            // construct first node, which represents class
            if(currentPathExpr.size == 0)
            {
                ppElements = m_pool->pathExprPool.addNewEntry();
                currentPathExpr.startIndex = ppElements.startIndex;
                currentPathExpr.size ++;

                ppElements[0].classInfo            = classInfo;
                ppElements[0].fromAttrKind         = PATHEXPR_KIND_CLASS;
                ppElements[0].aliasName.setNull();
                ppElements[0].domainSubstFlag      = SM_FALSE; 
                ppElements[0].substitutedClass     = -1;        
                ppElements[0].typeCastingFlag      = SM_FALSE;
                ppElements[0].typeCastedClass      = -1;
                ppElements[0].planNo.setNull();
                ppElements[0].pathExprKind         = 0;
            }

            // construct second node, which represent method
            ppElements = m_pool->pathExprPool.addNewEntry();
            currentPathExpr.size ++;

            e = smtChkArguments(b2, b2Type, classInfo, methodInfo);
            OOSQL_CHECK_ERR(e);

            ppElements[0].classInfo             = classInfo;
            ppElements[0].fromAttrKind          = PATHEXPR_KIND_METHOD;
            ppElements[0].method.methodInfo     = methodInfo;
            ppElements[0].method.argument.setPoolIndex(b2Type.poolIndex);
            ppElements[0].domainSubstFlag       = SM_FALSE;    
            ppElements[0].substitutedClass      = -1;        
            ppElements[0].typeCastingFlag       = SM_FALSE;
            ppElements[0].typeCastedClass       = -1;
            ppElements[0].planNo.setNull();
            ppElements[0].pathExprKind         = 0;

            // make return type information
            e = m_catalog->method_GetReturnTypeID(classInfo, methodInfo, methodTypeID);
            OOSQL_CHECK_ERR(e);
            returnType.set(methodTypeID, PT_PATHEXPRPOOL, currentPathExpr.getPoolIndex());
            if(returnType.typeID == TYPEID_OID)
            {
                e = m_catalog->method_GetReferencingClass(classInfo, methodInfo, returnType.classInfo);
                OOSQL_CHECK_ERR(e);
            }
            else
                returnType.classInfo = -1;

            lastClassInfo = returnType.classInfo;
            return eNOERROR;
        }

		m_errorMessage->Clear();
		m_errorMessage->Append("The Path expression at ");
		ERRORMESSAGE_APPEND_POSITION(m_errorMessage, node);
		m_errorMessage->Append(" is invalid.");

        OOSQL_ERR(eINVALID_PATHEXPR_OOSQL);
        break;
    case QuCnsObj:
        b1 = AST(node).son;
        b2 = AST(b1).brother;

        // QuGeoFnSpa can appear in the head of pathexpr, not in the body
        if(currentPathExpr.size != 0)
		{
			m_errorMessage->Clear();
			m_errorMessage->Append("Function is used in the middle of path expression at ");
			ERRORMESSAGE_APPEND_POSITION(m_errorMessage, node);
			m_errorMessage->Append(".");

            OOSQL_ERR(eINVALID_PATHEXPR_OOSQL);
		}

        e = smtChkQuCns(b1, b1Type);
        OOSQL_CHECK_ERR(e);

        if(b1Type.typeID != TYPEID_OID)
		{
			m_errorMessage->Clear();
			m_errorMessage->Append("The Path expression at ");
			ERRORMESSAGE_APPEND_POSITION(m_errorMessage, node);
			m_errorMessage->Append(" is invalid.");

            OOSQL_ERR(eINVALID_PATHEXPR_OOSQL);
		}

        // construct first node, which represents class
        ppElements = m_pool->pathExprPool.addNewEntry();
        currentPathExpr.startIndex = ppElements.startIndex;
        currentPathExpr.size ++;

        ppElements[0].classInfo            = b1Type.classInfo;
        ppElements[0].fromAttrKind         = PATHEXPR_KIND_OBJECT;
        ppElements[0].func.setPoolIndex(b1Type.poolIndex);
        ppElements[0].domainSubstFlag      = SM_FALSE; 
        ppElements[0].substitutedClass     = -1;        
        ppElements[0].typeCastingFlag      = SM_FALSE;
        ppElements[0].typeCastedClass      = -1;
        ppElements[0].planNo.setNull();
        ppElements[0].pathExprKind         = 0;

        // make return type information
        returnType.set(b1Type.typeID, PT_PATHEXPRPOOL, currentPathExpr.getPoolIndex());
        returnType.classInfo = b1Type.classInfo;
        lastClassInfo        = returnType.classInfo;

        break;

    default:
        OOSQL_ERR(eBADASTNODE_OOSQL);
        break;
    }

    return eNOERROR;
}

Four OQL_ASTtoGDS::smtChkIcltnElem(
    ASTNodeIdx  node,
    ReturnType& returnType
)
{
    OOSQL_ERR(eNOTIMPLEMENTED_OOSQL);
}

Four OQL_ASTtoGDS::smtChkQuAccIcltnSub(
    ASTNodeIdx  node,
    ReturnType& returnType
)
{
    OOSQL_ERR(eNOTIMPLEMENTED_OOSQL);
}

Four OQL_ASTtoGDS::smtChkQuAccIcltnFr(
    ASTNodeIdx  node,
    ReturnType& returnType
)
{
    OOSQL_ERR(eNOTIMPLEMENTED_OOSQL);
}

Four OQL_ASTtoGDS::smtChkQuAccIcltnLs(
    ASTNodeIdx  node,
    ReturnType& returnType
)
{
    OOSQL_ERR(eNOTIMPLEMENTED_OOSQL);
}

Four OQL_ASTtoGDS::smtChkQuCltnAll( 
    ASTNodeIdx  node,
    ReturnType& returnType 
)
{
    OOSQL_ERR(eNOTIMPLEMENTED_OOSQL);
}
            
Four OQL_ASTtoGDS::smtChkQuCltnEx( 
    ASTNodeIdx  node,
    ReturnType& returnType 
)
{
    OOSQL_ERR(eNOTIMPLEMENTED_OOSQL);
}

Four OQL_ASTtoGDS::smtChkQuCltnExany( 
    ASTNodeIdx  node,
    ReturnType& returnType 
)
{
    OOSQL_ERR(eNOTIMPLEMENTED_OOSQL);
}
            
Four OQL_ASTtoGDS::smtChkQuCltnUni( 
    ASTNodeIdx  node,
    ReturnType& returnType 
)
{
    OOSQL_ERR(eNOTIMPLEMENTED_OOSQL);
}

Four OQL_ASTtoGDS::smtChkQuCltnIn( 
    ASTNodeIdx  node,
    ReturnType& returnType 
)
{
    ASTNodeIdx          s;
    ASTNodeIdx          op1;
    ASTNodeIdx          op2;
    ReturnType          op1Type;
    ReturnType          op2Type;
    ExprPoolElements    epElements(m_pool->exprPool);
    ExprPoolIndex       oldConnectionPoint;
    OperatorGroup       oldOperGroup;
    Four                e;
    Boolean             flag;
    OperatorType        operatorType;
    ExprPoolIndex       exprPoolIndex;
    
    oldOperGroup    = m_recentOperGroup;
    m_recentOperGroup = OG_COMPARE;

    // construct operator node
    epElements = m_pool->exprPool.addNewEntry();
    epElements[0].operandType = EXPR_OPERANDTYPE_CONTAIN_NOMETHOD;
    epElements[0].refCount    = 0;

    oldConnectionPoint = m_connectionPoint;
    if(m_connectionPoint == NULL_POOLINDEX)
	{
        m_connectionPoint   = epElements.getPoolIndex();
		m_conditionNotBound = SM_TRUE;
	}
    
    s   = node;
    op1 = AST(s).son;   
    op2 = AST(op1).brother;

    e = smtChkQuery(op1, op1Type);  OOSQL_CHECK_ERR(e);
    e = smtChkQuery(op2, op2Type);  OOSQL_CHECK_ERR(e);

    operatorType = COLLECTION_OPERATION;

	if(m_connectionPoint!= NULL_POOLINDEX && oldConnectionPoint == NULL_POOLINDEX && m_conditionNotBound && m_progressStatus != HAVING_CLAUSE)
	{
		e = m_gds->addNodeToUnusedConditionList(m_pool, m_connectionPoint);
		OOSQL_CHECK_ERR(e);
	}
    m_connectionPoint = oldConnectionPoint;

    // check type
	if(op2Type.typeID == TYPEID_ID)
	{
		m_errorMessage->Clear();
		m_errorMessage->Append("Undefined Identifier is used at ");
		ERRORMESSAGE_APPEND_POSITION(m_errorMessage, op2);

		OOSQL_ERR(eSYNTAX_ERROR_OOSQL)
	}
    if(OOSQL_MASK_COMPLEXTYPE(op2Type.typeID) != COMPLEXTYPEID_SET && 
	   OOSQL_MASK_COMPLEXTYPE(op2Type.typeID) != OOSQL_COMPLEXTYPE_BAG && 
	   OOSQL_MASK_COMPLEXTYPE(op2Type.typeID) != OOSQL_COMPLEXTYPE_LIST && 
	   OOSQL_MASK_COMPLEXTYPE(op2Type.typeID) != OOSQL_COMPLEXTYPE_ODMG_SET &&
	   OOSQL_MASK_COMPLEXTYPE(op2Type.typeID) != OOSQL_COMPLEXTYPE_ODMG_BAG &&
	   OOSQL_MASK_COMPLEXTYPE(op2Type.typeID) != OOSQL_COMPLEXTYPE_ODMG_LIST &&
	   OOSQL_MASK_COMPLEXTYPE(op2Type.typeID) != OOSQL_COMPLEXTYPE_ODMG_ARRAY
	)
	{
		m_errorMessage->Clear();
		m_errorMessage->Append("The type of the value used at ");
		ERRORMESSAGE_APPEND_POSITION(m_errorMessage, op2);
		m_errorMessage->Append(" is not a set, bag, list, array where one of those is expected.");
        OOSQL_ERR(eTYPE_ERROR_OOSQL);
	}

    // check compatibility between op1 and op2
    e = testCompatibility(op1Type.typeID, OOSQL_MASK_TYPE(op2Type.typeID), flag);
    if(flag == SM_FALSE)
	{
		m_errorMessage->Clear();
		m_errorMessage->Append("The type of the value used at ");
		ERRORMESSAGE_APPEND_POSITION(m_errorMessage, op1);
		m_errorMessage->Append(" is not compatible with the type of the value used at ");
		ERRORMESSAGE_APPEND_POSITION(m_errorMessage, op2);
		m_errorMessage->Append(".");

        OOSQL_ERR(eTYPE_ERROR_OOSQL);
    }

    epElements[0].exprKind = EXPR_KIND_OPER;
    e = nodeNameToOperatorID(AST(s).nodeName, epElements[0].oper.operatorId);
    OOSQL_CHECK_ERR(e);
    epElements[0].oper.operatorType = operatorType;

    // make operand nodes;
    e = makeOperandInExprPool(op1Type, exprPoolIndex);
    epElements[0].oper.operand1 = exprPoolIndex;
    OOSQL_CHECK_ERR(e);
    e = makeOperandInExprPool(op2Type, exprPoolIndex);
    epElements[0].oper.operand2 = exprPoolIndex;
    OOSQL_CHECK_ERR(e);
	epElements[0].oper.operand3.setNull();       // no third operand

    returnType.set(TYPEID_BOOL, PT_EXPRPOOL, epElements.getPoolIndex());

    e = exprElementSetTypeInfo(epElements, returnType);
    OOSQL_CHECK_ERR(e);

    m_recentOperGroup = oldOperGroup;

    return eNOERROR;
}
            
Four OQL_ASTtoGDS::smtChkQuCltnCmpSome( 
    ASTNodeIdx  node,
    ReturnType& returnType 
)
{
    OOSQL_ERR(eNOTIMPLEMENTED_OOSQL);
}
            
Four OQL_ASTtoGDS::smtChkQuCltnCmpAny( 
    ASTNodeIdx  node,
    ReturnType& returnType 
)
{
    OOSQL_ERR(eNOTIMPLEMENTED_OOSQL);
}

Four OQL_ASTtoGDS::smtChkQuCltnCmpAll( 
    ASTNodeIdx  node,
    ReturnType& returnType 
)
{
    OOSQL_ERR(eNOTIMPLEMENTED_OOSQL);
}

Four OQL_ASTtoGDS::smtChkQuCltnAggDist(
    ASTNodeIdx  node,
    ReturnType& returnType
)
{
	Four					e;
	AggrFuncPoolElements	afpElements(m_pool->aggrFuncPool);
	e = smtChkQuCltnAgg(node, returnType);
	OOSQL_CHECK_ERR(e);
	afpElements = returnType.poolIndex;
	afpElements[0].distinctFlag = SM_TRUE;
	return eNOERROR;

	//OOSQL_ERR(eINVALIDSELECT_OOSQL)
}

Four OQL_ASTtoGDS::smtChkQuCltnAgg(
    ASTNodeIdx  node,
    ReturnType& returnType
)
{
    ASTNodeIdx              s;
    ASTNodeIdx              p1;
    ReturnType              p1Type;
    AggrFuncPoolElements    afpElements(m_pool->aggrFuncPool);
    ArgumentPoolElements    apElements(m_pool->argumentPool);
    TargetListPoolElements  targetList(m_pool->targetListPool);
    PathExprPoolElements    pathExpr(m_pool->pathExprPool);
    Four                    e;
    Four                    i;

    s = AST(node).son;

    switch(AST(s).nodeName) 
    {
    case FnAggCntall:       // count(*)
        // construct function node
        afpElements = m_pool->aggrFuncPool.addNewEntry();

        e = nodeNameToAggrFunctionID(AST(s).nodeName, afpElements[0].aggrFunctionID);
        OOSQL_CHECK_ERR(e);

        afpElements[0].distinctFlag = SM_FALSE;

        // make arguments which project all the classes in FROM clause
        apElements              = m_pool->argumentPool.addNewEntry(targetList.size);  // generate argument
        afpElements[0].argument = apElements.getPoolIndex();
		e = m_gds->getTargetList(m_pool, targetList); OOSQL_CHECK_ERR(e);
        for(i = 0; i < targetList.size; i++)
        {
            // construct pathexpr
            pathExpr = m_pool->pathExprPool.addNewEntry();
            pathExpr[0].aliasName         = targetList[i].aliasName;
            pathExpr[0].classInfo         = targetList[i].collectionInfo.classInfo;
            pathExpr[0].fromAttrKind      = PATHEXPR_KIND_CLASS;
            pathExpr[0].domainSubstFlag   = SM_FALSE;
            pathExpr[0].substitutedClass  = -1;
            pathExpr[0].typeCastingFlag   = SM_FALSE;
            pathExpr[0].typeCastedClass   = -1;
            pathExpr[0].planNo.setNull();
            pathExpr[0].pathExprKind      = 0;

            // construct query graph
            e = qg_consPathExpr(pathExpr.getPoolIndex());
            OOSQL_CHECK_ERR(e);

            // connect pathExpr to argument
            apElements[i].argumentKind = ARGUMENT_KIND_PATHEXPR;
            apElements[i].pathExpr     = pathExpr.getPoolIndex();
        }
        
        // construct return type
        // according to aggr func type, return type varies
        returnType.set(TYPEID_LONG_VAR, PT_AGGRFUNCPOOL, afpElements.getPoolIndex());
        break;

    case FnAggCnt:          // count(...)
    case FnAggSum:          // sum(...)
    case FnAggMin:          // min(...)
    case FnAggMax:          // max(...)
    case FnAggAvg:          // avg(...)
        p1 = AST(s).brother;

        // check the number of parameters
        // aggr func has only one parameter
        if(AST(p1).brother != AST_NULL)
		{
			m_errorMessage->Clear();
			m_errorMessage->Append("The parameter of the function at ");
			ERRORMESSAGE_APPEND_POSITION(m_errorMessage, s);
			m_errorMessage->Append(" is invalid.");

            OOSQL_ERR(eFUNCTIONPARAMETER_ERROR_OOSQL);
		}
        
        e = smtChkQuery(p1, p1Type);
        OOSQL_CHECK_ERR(e);

        // check the type of the parameter
        if(!(p1Type.poolType == PT_PATHEXPRPOOL || 
             p1Type.poolType == PT_FUNCPOOL))
		{
			m_errorMessage->Clear();
			m_errorMessage->Append("The parameter of the function at ");
			ERRORMESSAGE_APPEND_POSITION(m_errorMessage, s);
			m_errorMessage->Append(" is invalid.");

            OOSQL_ERR(eFUNCTIONPARAMETER_ERROR_OOSQL);
        }

        // construct function node
        afpElements = m_pool->aggrFuncPool.addNewEntry();

        e = nodeNameToAggrFunctionID(AST(s).nodeName, afpElements[0].aggrFunctionID);
        OOSQL_CHECK_ERR(e);
        afpElements[0].distinctFlag   = SM_FALSE;          
        
        apElements = m_pool->argumentPool.addNewEntry();
        switch(p1Type.poolType)
        {
        case PT_PATHEXPRPOOL:
            apElements[0].argumentKind = ARGUMENT_KIND_PATHEXPR;
            apElements[0].pathExpr.setPoolIndex(p1Type.poolIndex);
            break;

        case PT_FUNCPOOL:
            apElements[0].argumentKind = ARGUMENT_KIND_FUNC;
            apElements[0].func.setPoolIndex(p1Type.poolIndex);
            break;
        }

        afpElements[0].argument = apElements.getPoolIndex();
        
        // construct return type
        // according to aggr func type, return type varies
        switch(AST(s).nodeName)
        {
        case FnAggCnt:
            returnType.set(TYPEID_LONG_VAR, PT_AGGRFUNCPOOL, afpElements.getPoolIndex());
            break;
        case FnAggSum:
            switch(p1Type.typeID)
            {
            case TYPEID_SHORT:
            case TYPEID_INT:
            case TYPEID_LONG:
                returnType.set(TYPEID_LONG, PT_AGGRFUNCPOOL, afpElements.getPoolIndex());
                break;
            case TYPEID_LONG_LONG:
                returnType.set(TYPEID_LONG_LONG, PT_AGGRFUNCPOOL, afpElements.getPoolIndex());
                break;
            case TYPEID_FLOAT:
            case TYPEID_DOUBLE:
                returnType.set(TYPEID_DOUBLE, PT_AGGRFUNCPOOL, afpElements.getPoolIndex());
                break;
            default:
                OOSQL_ERR(eAGGRFUNC_ARGUMENT_ERR_OOSQL);
                break;
            }
            break;
        case FnAggMin:
        case FnAggMax:
            returnType.set(p1Type.typeID, PT_AGGRFUNCPOOL, afpElements.getPoolIndex());
            break;
        case FnAggAvg:
            returnType.set(TYPEID_DOUBLE, PT_AGGRFUNCPOOL, afpElements.getPoolIndex());
            break;
        }
        break;
    }

    return eNOERROR;
}

Four OQL_ASTtoGDS::smtChkQuSel( 
    ASTNodeIdx  node,
    ReturnType& returnType
)
{
    ASTNodeIdx  sSelect, sFrom, sWhere, sGroupBy, sHaving, sOrderBy;
	ASTNodeIdx 	sLimit;		
    ReturnType  sType;      // dummy variable
    Four        e;

	if(m_gds->queryType == OQL_GDS::NO_QUERY)
	{
		e = m_gds->init(m_pool, OQL_GDS::SELECT_QUERY);
		OOSQL_CHECK_ERR(e);
	}

    sSelect  = AST(node).son;
    sFrom    = AST(sSelect).brother;
    sWhere   = AST(sFrom).brother;
    sGroupBy = AST(sWhere).brother;
    sHaving  = AST(sGroupBy).brother;
    sOrderBy = AST(sHaving).brother;
    sLimit   = AST(sOrderBy).brother;

    m_progressStatus = FROM_CLAUSE;       e = smtChkClFr(sFrom, sType);       OOSQL_CHECK_ERR(e);
    m_progressStatus = GROUPBY_CLAUSE;    e = smtChkClGrp(sGroupBy, sType);   OOSQL_CHECK_ERR(e);
    m_progressStatus = HAVING_CLAUSE;     e = smtChkClHav(sHaving, sType);    OOSQL_CHECK_ERR(e);
    m_progressStatus = SELECT_CLAUSE;     e = smtChkClSel(sSelect, sType);    OOSQL_CHECK_ERR(e);
    m_progressStatus = WHERE_CLAUSE;      e = smtChkClWh(sWhere, sType);      OOSQL_CHECK_ERR(e);
    m_progressStatus = ORDERBY_CLAUSE;    e = smtChkClOrd(sOrderBy, sType);   OOSQL_CHECK_ERR(e);
    m_progressStatus = LIMIT_CLAUSE;      e = smtChkClLim(sLimit, sType);     OOSQL_CHECK_ERR(e); 

    e = smtChkInterClauseRelationship(node, returnType);
    OOSQL_CHECK_ERR(e);

    e = smtChkTextIrWeightFunction();
    OOSQL_CHECK_ERR(e);

    e = setExplicitJoinLinkInQG();
    OOSQL_CHECK_ERR(e);

    return eNOERROR;
}

Four OQL_ASTtoGDS::smtChkQuUpd(ASTNodeIdx node, ReturnType& returnType)
{
	ASTNodeIdx    sFrom, sSetList, sWhere;
	ASTNodeIdx	  sSet, sUpdateCol, sExpr;
    ReturnType    sType;
	TypeID		  updateColType;
	Four          updateColLength;
	ReturnType    exprType;
	ExprPoolIndex exprPoolIndex;
    Four          e;
	Boolean       flag;
	UpdateValuePoolElements updateValueElements(m_pool->updateValuePool);
	UpdateValuePoolIndex    updateValuePoolIndex;
	TargetListPoolElements  targetList;
	CataClassInfo			classInfo;
	CataAttrInfo			attrInfo;
	ExprPoolElements		expr(m_pool->exprPool);
	ValuePoolElements		value(m_pool->valuePool);

	updateValuePoolIndex.setNull();

	e = m_gds->init(m_pool, OQL_GDS::UPDATE_QUERY);
	OOSQL_CHECK_ERR(e);

    sFrom    = AST(node).son;
	sSetList = AST(sFrom).brother;
    sWhere   = AST(sSetList).brother;

	m_progressStatus = FROM_CLAUSE;       e = smtChkClFr(sFrom, sType);       OOSQL_CHECK_ERR(e);
	m_progressStatus = WHERE_CLAUSE;      e = smtChkClWh(sWhere, sType);      OOSQL_CHECK_ERR(e);
	m_progressStatus = NO_CALUSE;

	e = m_gds->getTargetList(m_pool, targetList);
	OOSQL_CHECK_ERR(e);
	classInfo = targetList[0].collectionInfo.classInfo;

	sSet = AST(sSetList).son;
	while(sSet != AST_NULL)
	{
		if(AST(sSet).nodeName != QuUpdSet)
			OOSQL_ERR(eINTERNALERROR_OOSQL);

        // set <UpdateCol>=<Expr>
		sUpdateCol = AST(sSet).son;
		sExpr      = AST(sUpdateCol).brother;

		if(AST(sUpdateCol).nodeName == ID)
		{
			e = isID_AttributeName(sUpdateCol, classInfo, attrInfo, flag);
			OOSQL_CHECK_ERR(e);

			if(!flag)
			{
				char classname[MAXCLASSNAME];
				SimpleString attrname;

				e = getStringFromID_Node(attrname, sUpdateCol);
				OOSQL_CHECK_ERR(e);

				e = m_catalog->class_CataClassInfo_to_ClassName(classInfo, classname);
				OOSQL_CHECK_ERR(e);

				m_errorMessage->Clear();
				m_errorMessage->Append("The attribute '");
				m_errorMessage->Append((char*)attrname);
				m_errorMessage->Append("' is not defined in the class '");
				m_errorMessage->Append(classname);
				m_errorMessage->Append("'.");

				OOSQL_ERR(eNOMATCHING_COLUMN_OOSQL);	
			}
		}
        else
            OOSQL_ERR(eINVALIDAST_OOSQL);

		e = m_catalog->attr_GetTypeID(classInfo, attrInfo, updateColType);
		OOSQL_CHECK_ERR(e);

		e = m_catalog->attr_GetLength(classInfo, attrInfo, updateColLength);
		OOSQL_CHECK_ERR(e);

		// if(AST(sExpr).nodeName != VaParam)
		if(AST(sExpr).nodeName == VaSeqCurr || AST(sExpr).nodeName == VaSeqNext)
		{
			e = smtChkQuery(sExpr, exprType); OOSQL_CHECK_ERR(e);

			e = makeOperandInExprPool(exprType, exprPoolIndex);
			OOSQL_CHECK_ERR(e);

			updateValueElements = m_pool->updateValuePool.addNewEntry();
			updateValueElements[0].attrInfo = attrInfo;
			updateValueElements[0].isParam  = SM_FALSE;
			updateValueElements[0].expr.setPoolIndex(exprPoolIndex.startIndex, exprPoolIndex.size);
			updateValueElements[0].type		= updateColType;
			updateValueElements[0].length   = updateColLength;

			if (AST(sExpr).nodeName == VaSeqCurr)
				updateValueElements[0].seqValueType	= SEQ_CURRVAL;
			else if (AST(sExpr).nodeName == VaSeqNext)
				updateValueElements[0].seqValueType	= SEQ_NEXTVAL;
		}
		else if(AST(sExpr).nodeName != VaParam)
		{
			e = smtChkQuery(sExpr, exprType);	      OOSQL_CHECK_ERR(e);

			e = makeOperandInExprPool(exprType, exprPoolIndex);
			OOSQL_CHECK_ERR(e);

			e = testCompatibility(updateColType, exprType.typeID, flag);
			OOSQL_CHECK_ERR(e);
			if(flag == SM_FALSE)
			{
				char classname[MAXCLASSNAME];
				char attrname[MAXATTRNAME];

				e = m_catalog->class_CataClassInfo_to_ClassName(classInfo, classname);
				OOSQL_CHECK_ERR(e);
				e = m_catalog->attr_CataAttrInfo_to_AttrName(classInfo, attrInfo, attrname);
				OOSQL_CHECK_ERR(e);

				m_errorMessage->Clear();
				m_errorMessage->Append("The type of the upadated column ");
				m_errorMessage->Append(classname);
				m_errorMessage->Append(".");
				m_errorMessage->Append(attrname);
				m_errorMessage->Append(" is not compatible with the type of the value used at ");
				ERRORMESSAGE_APPEND_POSITION(m_errorMessage, sExpr);
				m_errorMessage->Append(".");

				OOSQL_ERR(eTYPE_ERROR_OOSQL);
			}

			updateValueElements = m_pool->updateValuePool.addNewEntry();
			updateValueElements[0].attrInfo = attrInfo;
			updateValueElements[0].expr.setPoolIndex(exprPoolIndex.startIndex, exprPoolIndex.size);
			updateValueElements[0].type		= updateColType;
			updateValueElements[0].length   = updateColLength;
		    updateValueElements[0].seqValueType	= NO_SEQ;

			expr = updateValueElements[0].expr;
			if(expr[0].exprKind == EXPR_KIND_VALUE)
			{
				value = expr[0].value;
				if(value[0].valueKind == VALUE_KIND_TEXT)
				{
					if(value[0].text.isNull())
						updateValueElements[0].isParam = SM_TRUE;
					else
						updateValueElements[0].isParam = SM_FALSE;
				}
				else
					updateValueElements[0].isParam  = SM_FALSE;
			}
			else
				updateValueElements[0].isParam  = SM_FALSE;
		}
		else
		{
			updateValueElements = m_pool->updateValuePool.addNewEntry();
			updateValueElements[0].attrInfo = attrInfo;
			updateValueElements[0].isParam  = SM_TRUE;
			updateValueElements[0].expr.setNull();
			updateValueElements[0].type		= updateColType;
			updateValueElements[0].length   = updateColLength;
		    updateValueElements[0].seqValueType	= NO_SEQ;
		}

		if(updateValuePoolIndex == NULL_POOLINDEX)
			updateValuePoolIndex = updateValueElements.getPoolIndex();
		else
			updateValuePoolIndex.size ++;

		sSet = AST(sSet).brother;
	}

	e = m_gds->setUpdateValueList(m_pool, updateValuePoolIndex);
	OOSQL_CHECK_ERR(e);

	return eNOERROR;
}

Four OQL_ASTtoGDS::smtChkQuIns(ASTNodeIdx node, ReturnType& returnType)
{
	ASTNodeIdx				sTarget, sInsertColList, sInsertValueList, sSelectQuery;
	ASTNodeIdx				sInsertCol, sInsertValue;
	ReturnType				sType;
	ReturnType				valueType;
	Four					e;
	Four					insertColNo;
	CataClassInfo			classInfo;
    CataAttrInfo			attrInfo;
	Boolean					flag;
	Four					nAttrs;
	TypeID					attrTypeID, selectTypeID;
	Four					attrLength;
	PathExprPoolElements	ppElements(m_pool->pathExprPool);
	InsertValuePoolElements insertValueElements(m_pool->insertValuePool);
	InsertValuePoolIndex	insertValuePoolIndex;
	SelListPoolElements		selList(m_pool->selListPool);
	TargetListPoolElements	targetList(m_pool->targetListPool);
	ValuePoolElements		value(m_pool->valuePool);
    Four                    isLrdsRelation;

	e = m_gds->init(m_pool, OQL_GDS::INSERT_QUERY);
	OOSQL_CHECK_ERR(e);

	sTarget = AST(node).son;
	switch(AST(AST(sTarget).brother).nodeName)
	{
	case QuInsCol:
		sInsertColList   = AST(sTarget).brother;
		switch(AST(AST(sInsertColList).brother).nodeName)
		{
		case QuInsValue:
			sInsertValueList = AST(sInsertColList).brother;
			sSelectQuery     = AST_NULL;
			break;
		case QuSel:
			sInsertValueList = AST_NULL;
			sSelectQuery     = AST(sInsertColList).brother;
			break;
		default:
			OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
		}
		break;
	case QuInsValue:
		sInsertColList   = AST_NULL;
		sInsertValueList = AST(sTarget).brother;
		sSelectQuery     = AST_NULL;
		break;
	case QuSel:
		sInsertColList   = AST_NULL;
		sInsertValueList = AST_NULL;
		sSelectQuery     = AST(sTarget).brother;
		break;
	default:
		OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
	}

	sTarget = AST(sTarget).son;
	if(AST(sTarget).nodeName == FrCltn)
	{
		sTarget = AST(sTarget).son;
		if(AST(sTarget).nodeName == ID)
		{
			e = isID_ClassName(sTarget, classInfo, flag);
			OOSQL_CHECK_ERR(e);
			if(!flag)
			{
				SimpleString classname;

				e = getStringFromID_Node(classname, sTarget);
				OOSQL_CHECK_ERR(e);

				m_errorMessage->Clear();
				m_errorMessage->Append("The class '");
				m_errorMessage->Append((char*)classname);
				m_errorMessage->Append("' is not defined in the database.");

				OOSQL_ERR(eTARGETNOTPERSISTENT_OOSQL);
			}
			
			m_gds->insertQuery.getElements(m_pool->insertQueryPool, 0).insertTarget = classInfo;
		}
		else
			OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
	}
	else
		OOSQL_ERR(eUNHANDLED_CASE_OOSQL);

	e = m_catalog->class_GetN_Attrs(classInfo, nAttrs); OOSQL_CHECK_ERR(e);

    isLrdsRelation = m_catalog->class_IsLrdsRelation(classInfo);

	insertValuePoolIndex.setNull();
	if(sInsertValueList != AST_NULL)
	{
		sInsertValue = AST(sInsertValueList).son;
		if(sInsertColList != AST_NULL)
			sInsertCol = AST(sInsertColList).son;
		else
			sInsertCol = AST_NULL;

		insertColNo = 0;
		if(AST(sInsertValue).nodeName != null)
		{
			while(sInsertValue != AST_NULL)
			{
				if(sInsertColList != AST_NULL)
				{
					if(AST(sInsertCol).nodeName == ID)
					{
						e = isID_AttributeName(sInsertCol, classInfo, attrInfo, flag);
						OOSQL_CHECK_ERR(e);

						if(!flag)
						{
							char classname[MAXCLASSNAME];
							SimpleString attrname;

							e = getStringFromID_Node(attrname, sInsertCol);
							OOSQL_CHECK_ERR(e);

							e = m_catalog->class_CataClassInfo_to_ClassName(classInfo, classname);
							OOSQL_CHECK_ERR(e);

							m_errorMessage->Clear();
							m_errorMessage->Append("The attribute '");
							m_errorMessage->Append((char*)attrname);
							m_errorMessage->Append("' is not defined in the class '");
							m_errorMessage->Append(classname);
							m_errorMessage->Append("'.");

							OOSQL_ERR(eNOMATCHING_COLUMN_OOSQL);			
						}
					}
					else
					{
						OOSQL_ERR(eNOMATCHING_COLUMNLIST_VALUELIST_OOSQL);		
					}
				}
				else
				{
                    if(isLrdsRelation)
                    {
					    if(insertColNo >= nAttrs)
						    OOSQL_ERR(eMOREVALUELIST_THAN_COLUMNS_OOSQL);	
					    e = m_catalog->attr_ColNo_to_AttrInfo(classInfo, (short)insertColNo, attrInfo); 
                        OOSQL_CHECK_ERR(e);
                    }
                    else
                    {
					    if(insertColNo >= nAttrs - 1)
						    OOSQL_ERR(eMOREVALUELIST_THAN_COLUMNS_OOSQL);	
					    e = m_catalog->attr_ColNo_to_AttrInfo(classInfo, (short)insertColNo + 1, attrInfo); 
                        OOSQL_CHECK_ERR(e);
                    }
				}

				e = m_catalog->attr_GetTypeID(classInfo, attrInfo, attrTypeID);
				OOSQL_CHECK_ERR(e);

				e = m_catalog->attr_GetLength(classInfo, attrInfo, attrLength);
				OOSQL_CHECK_ERR(e);

				if(AST(sInsertValue).nodeName == VaSeqCurr || AST(sInsertValue).nodeName == VaSeqNext)
				{
					e = smtChkQuery(sInsertValue, valueType); OOSQL_CHECK_ERR(e);

					insertValueElements = m_pool->insertValuePool.addNewEntry();
					insertValueElements[0].attrInfo = attrInfo;
					insertValueElements[0].isParam  = SM_FALSE;
					insertValueElements[0].value.setPoolIndex(valueType.poolIndex);
					insertValueElements[0].type     = attrTypeID;
					insertValueElements[0].length   = attrLength;

					if (AST(sInsertValue).nodeName == VaSeqCurr)
						insertValueElements[0].seqValueType	= SEQ_CURRVAL;
					else if (AST(sInsertValue).nodeName == VaSeqNext)
						insertValueElements[0].seqValueType	= SEQ_NEXTVAL;
				}
				else if(AST(sInsertValue).nodeName != VaParam)
				{
					e = smtChkQuery(sInsertValue, valueType); OOSQL_CHECK_ERR(e);

					if((valueType.poolType != PT_VALUEPOOL) && (valueType.poolType != PT_EXPRPOOL))
						OOSQL_ERR(eINTERNALERROR_OOSQL);

					e = testCompatibility(attrTypeID, valueType.typeID, flag);
					OOSQL_CHECK_ERR(e);
					if(flag == SM_FALSE)
					{
						char classname[MAXCLASSNAME];
						char attrname[MAXATTRNAME];

						e = m_catalog->class_CataClassInfo_to_ClassName(classInfo, classname);
						OOSQL_CHECK_ERR(e);
						e = m_catalog->attr_CataAttrInfo_to_AttrName(classInfo, attrInfo, attrname);
						OOSQL_CHECK_ERR(e);

						m_errorMessage->Clear();
						m_errorMessage->Append("The type of the inserted column ");
						m_errorMessage->Append(classname);
						m_errorMessage->Append(".");
						m_errorMessage->Append(attrname);
						m_errorMessage->Append(" is not compatible with the type of the value used at ");
						ERRORMESSAGE_APPEND_POSITION(m_errorMessage, sInsertValue);
						m_errorMessage->Append(".");

						OOSQL_ERR(eTYPE_ERROR_OOSQL);
					}

					insertValueElements = m_pool->insertValuePool.addNewEntry();
					insertValueElements[0].attrInfo = attrInfo;
					if (valueType.poolType == PT_EXPRPOOL)
					{
						insertValueElements[0].poolType = PT_EXPRPOOL;
						insertValueElements[0].expr.setPoolIndex(valueType.poolIndex);
						insertValueElements[0].isParam = SM_FALSE;
					}
					else
					{
						insertValueElements[0].poolType = PT_VALUEPOOL;
					insertValueElements[0].value.setPoolIndex(valueType.poolIndex);
					}
					insertValueElements[0].type     = attrTypeID;
					insertValueElements[0].length   = attrLength;
					insertValueElements[0].seqValueType	= NO_SEQ;

					if (valueType.poolType != PT_EXPRPOOL) {
					value = insertValueElements[0].value;
					if(value[0].valueKind == VALUE_KIND_TEXT)
					{
						if(value[0].text.isNull())
							insertValueElements[0].isParam = SM_TRUE;
						else
							insertValueElements[0].isParam = SM_FALSE;
					}
						else {
						insertValueElements[0].isParam  = SM_FALSE;
				        }
					}
				}
				else
				{
					insertValueElements = m_pool->insertValuePool.addNewEntry();
					insertValueElements[0].attrInfo = attrInfo;
					insertValueElements[0].isParam  = SM_TRUE;
					insertValueElements[0].value.setNull();
					insertValueElements[0].type     = attrTypeID;
					insertValueElements[0].length   = attrLength;
					insertValueElements[0].seqValueType	= NO_SEQ;
				}

				if(insertValuePoolIndex == NULL_POOLINDEX)
					insertValuePoolIndex = insertValueElements.getPoolIndex();
				else
					insertValuePoolIndex.size ++;

				sInsertValue = AST(sInsertValue).brother;
				if(sInsertCol != AST_NULL)
					sInsertCol = AST(sInsertCol).brother;

				insertColNo ++;
			}
			if(sInsertColList != AST_NULL && sInsertCol != AST_NULL)
				OOSQL_ERR(eMOREVALUELIST_THAN_COLUMNS_OOSQL);	
		}
	}
	
	if(sSelectQuery != AST_NULL)
	{
		e = smtChkQuSel(sSelectQuery, sType);
		OOSQL_CHECK_ERR(e);

		e = m_gds->getSelList(m_pool, selList);
		OOSQL_CHECK_ERR(e);

		e = m_gds->getTargetList(m_pool, targetList);
		OOSQL_CHECK_ERR(e);

		if(sInsertColList == AST_NULL)
		{
			for(insertColNo = 0; insertColNo < (selList.size - targetList.size); insertColNo ++)
			{
				if(isLrdsRelation)
				{
					if(insertColNo >= nAttrs)
							OOSQL_ERR(eMOREVALUELIST_THAN_COLUMNS_OOSQL);	
							e = m_catalog->attr_ColNo_to_AttrInfo(classInfo, (short)insertColNo, attrInfo); 
					        OOSQL_CHECK_ERR(e);
				}
				else
				{
					if(insertColNo >= nAttrs - 1)
						OOSQL_ERR(eMOREVALUELIST_THAN_COLUMNS_OOSQL);	
					e = m_catalog->attr_ColNo_to_AttrInfo(classInfo, (short)insertColNo + 1, attrInfo); 
			        OOSQL_CHECK_ERR(e);
				} 



				e = m_catalog->attr_GetTypeID(classInfo, attrInfo, attrTypeID);
				OOSQL_CHECK_ERR(e);
				
				e = m_catalog->attr_GetLength(classInfo, attrInfo, attrLength);
				OOSQL_CHECK_ERR(e);

				e = getTypeID_FromSelectClause(insertColNo, selectTypeID);
				OOSQL_CHECK_ERR(e);

				e = testCompatibility(attrTypeID, selectTypeID, flag);
				OOSQL_CHECK_ERR(e);
				if(flag == SM_FALSE)
				{
					char classname[MAXCLASSNAME];
					char attrname[MAXATTRNAME];

					e = m_catalog->class_CataClassInfo_to_ClassName(classInfo, classname);
					OOSQL_CHECK_ERR(e);
					e = m_catalog->attr_CataAttrInfo_to_AttrName(classInfo, attrInfo, attrname);
					OOSQL_CHECK_ERR(e);

					m_errorMessage->Clear();
					m_errorMessage->Append("The type of the inserted column ");
					m_errorMessage->Append(classname);
					m_errorMessage->Append(".");
					m_errorMessage->Append(attrname);
					m_errorMessage->Append(" is not compatible with the type of ");
					m_errorMessage->Append((Four)insertColNo);
					m_errorMessage->Append("th column of the select clause.");

					OOSQL_ERR(eTYPE_ERROR_OOSQL);
				}

				insertValueElements = m_pool->insertValuePool.addNewEntry();
				insertValueElements[0].attrInfo = attrInfo;
				insertValueElements[0].isParam  = SM_FALSE;
				insertValueElements[0].value.setNull();
				insertValueElements[0].type		= attrTypeID;
				insertValueElements[0].length   = attrLength;
				insertValueElements[0].seqValueType	= NO_SEQ;

				if(insertValuePoolIndex == NULL_POOLINDEX)
					insertValuePoolIndex = insertValueElements.getPoolIndex();
				else
					insertValuePoolIndex.size ++;
			}
		}
		else
		{
			insertColNo = 0;
			sInsertCol = AST(sInsertColList).son;
			
			while(sInsertCol != AST_NULL)
			{
				if(AST(sInsertCol).nodeName == ID)
				{
					e = isID_AttributeName(sInsertCol, classInfo, attrInfo, flag);
					OOSQL_CHECK_ERR(e);

					if(!flag)
					{
						char classname[MAXCLASSNAME];
						SimpleString attrname;

						e = getStringFromID_Node(attrname, sInsertCol);
						OOSQL_CHECK_ERR(e);

						e = m_catalog->class_CataClassInfo_to_ClassName(classInfo, classname);
						OOSQL_CHECK_ERR(e);

						m_errorMessage->Clear();
						m_errorMessage->Append("The attribute '");
						m_errorMessage->Append((char*)attrname);
						m_errorMessage->Append("' is not defined in the class '");
						m_errorMessage->Append(classname);
						m_errorMessage->Append("'.");

						OOSQL_ERR(eNOMATCHING_COLUMN_OOSQL);				
					}
				}
				else
				{
					OOSQL_ERR(eNOMATCHING_COLUMNLIST_VALUELIST_OOSQL);	
				}
				
				e = m_catalog->attr_GetTypeID(classInfo, attrInfo, attrTypeID);
				OOSQL_CHECK_ERR(e);

				e = m_catalog->attr_GetLength(classInfo, attrInfo, attrLength);
				OOSQL_CHECK_ERR(e);

				e = getTypeID_FromSelectClause(insertColNo, selectTypeID);
				OOSQL_CHECK_ERR(e);

				e = testCompatibility(attrTypeID, selectTypeID, flag);
				OOSQL_CHECK_ERR(e);
				if(flag == SM_FALSE)
				{
					char classname[MAXCLASSNAME];
					char attrname[MAXATTRNAME];

					e = m_catalog->class_CataClassInfo_to_ClassName(classInfo, classname);
					OOSQL_CHECK_ERR(e);
					e = m_catalog->attr_CataAttrInfo_to_AttrName(classInfo, attrInfo, attrname);
					OOSQL_CHECK_ERR(e);

					m_errorMessage->Clear();
					m_errorMessage->Append("The type of the inserted column ");
					m_errorMessage->Append(classname);
					m_errorMessage->Append(".");
					m_errorMessage->Append(attrname);
					m_errorMessage->Append(" is not compatible with the type of ");
					m_errorMessage->Append((Four)insertColNo);
					m_errorMessage->Append("th column of the select clause.");

					OOSQL_ERR(eTYPE_ERROR_OOSQL);
				}

				insertValueElements = m_pool->insertValuePool.addNewEntry();
				insertValueElements[0].attrInfo = attrInfo;
				insertValueElements[0].isParam  = SM_FALSE;
				insertValueElements[0].value.setNull();;
				insertValueElements[0].type		= attrTypeID;
				insertValueElements[0].length   = attrLength;
				insertValueElements[0].seqValueType	= NO_SEQ;

				if(insertValuePoolIndex == NULL_POOLINDEX)
					insertValuePoolIndex = insertValueElements.getPoolIndex();
				else
					insertValuePoolIndex.size ++;

				sInsertCol = AST(sInsertCol).brother;
				insertColNo ++;
			}
		}
	}

	e = m_gds->setInsertValueList(m_pool, insertValuePoolIndex);
	OOSQL_CHECK_ERR(e);
	
	return eNOERROR;
}

Four OQL_ASTtoGDS::getTypeID_FromSelectClause(Four columnNo, TypeID& typeID)
{
	SelListPoolElements  selList(m_pool->selListPool);
	Four                 e;
	CataClassInfo		 classInfo;
	PathExprPoolElements pathExprElements;
	ArgumentPoolElements argumentElements; 

	switch(selList[columnNo].selElemKind)
	{
	case SELLIST_KIND_PATHEXPR:
		pathExprElements = selList[columnNo].pathExpr.getElements(m_pool->pathExprPool);

		classInfo = pathExprElements[pathExprElements.size - 1].classInfo;
		switch(pathExprElements[pathExprElements.size - 1].fromAttrKind)
		{
		case PATHEXPR_KIND_METHOD:
			e = m_catalog->method_GetReturnTypeID(classInfo, 
				                          pathExprElements[pathExprElements.size - 1].method.methodInfo, 
								  		  typeID); 
			OOSQL_CHECK_ERR(e);
			break;

		case PATHEXPR_KIND_ATTR:
			e = m_catalog->attr_GetTypeID(classInfo, 
				                        pathExprElements[pathExprElements.size - 1].attr.attrInfo, 
										typeID); 
			OOSQL_CHECK_ERR(e);
			break;

		case PATHEXPR_KIND_FUNC:
			typeID = pathExprElements[pathExprElements.size - 1].func.getElements(m_pool->funcPool, 0).returnType;
			break;

		case PATHEXPR_KIND_CLASS:
			typeID = TYPEID_OID;
			break;

		default:
			OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
		}
		
		break;
	case SELLIST_KIND_AGGRFUNC:
		switch(selList[columnNo].aggrFunc.getElements(m_pool->aggrFuncPool, 0).aggrFunctionID)
		{
		case AGGRFUNC_COUNTALL:
		case AGGRFUNC_COUNT:
			typeID = TYPEID_INT;
			break;
		case AGGRFUNC_SUM:
		case AGGRFUNC_MIN:
		case AGGRFUNC_MAX:
			argumentElements = selList[columnNo].aggrFunc.getElements(m_pool->aggrFuncPool, 0).argument.getElements(m_pool->argumentPool);
			if(argumentElements[0].argumentKind == ARGUMENT_KIND_PATHEXPR)
			{
				pathExprElements = argumentElements[0].pathExpr.getElements(m_pool->pathExprPool);
				classInfo = pathExprElements[pathExprElements.size - 1].classInfo;
				switch(pathExprElements[pathExprElements.size - 1].fromAttrKind)
				{
				case PATHEXPR_KIND_METHOD:
					e = m_catalog->method_GetReturnTypeID(classInfo, 
												  pathExprElements[pathExprElements.size - 1].method.methodInfo, 
								  				  typeID); 
					OOSQL_CHECK_ERR(e);
					break;

				case PATHEXPR_KIND_ATTR:
					e = m_catalog->attr_GetTypeID(classInfo, 
												pathExprElements[pathExprElements.size - 1].attr.attrInfo, 
												typeID); 
					OOSQL_CHECK_ERR(e);
					break;

				case PATHEXPR_KIND_FUNC:
					typeID = pathExprElements[pathExprElements.size - 1].func.getElements(m_pool->funcPool, 0).returnType;
					break;

				case PATHEXPR_KIND_CLASS:
					typeID = TYPEID_OID;
					break;

				default:
					OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
				}
			}
			else
				OOSQL_ERR(eUNHANDLED_CASE_OOSQL);

			break;
		case AGGRFUNC_AVG:
			typeID = TYPEID_DOUBLE;
			break;
		case AGGRFUNC_NONE:
		default:
			OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
		}
		break;
	case SELLIST_KIND_FUNC:
		typeID = selList[columnNo].func.getElements(m_pool->funcPool, 0).returnType;
		break;
	case SELLIST_KIND_VALUE:
		switch(selList[columnNo].value.getElements(m_pool->valuePool, 0).valueKind)
		{
		case VALUE_KIND_STRING:
			typeID = TYPEID_STRING;		break;
		case VALUE_KIND_INTEGER:	
			typeID = TYPEID_INT;		break;
		case VALUE_KIND_REAL:
			typeID = TYPEID_DOUBLE;		break;
		case VALUE_KIND_BOOL:
			typeID = TYPEID_BOOL;		break;
		case VALUE_KIND_NIL:
			typeID = TYPEID_NIL;		break;
		case VALUE_KIND_MBR:
			typeID = TYPEID_MBR;		break;
		case VALUE_KIND_DATE:
			typeID = TYPEID_DATE;		break;
		case VALUE_KIND_TIME:
			typeID = TYPEID_TIME;		break;
		case VALUE_KIND_TIMESTAMP:
			typeID = TYPEID_TIMESTAMP;	break;
		case VALUE_KIND_INTERVAL:
			typeID = TYPEID_INTERVAL;	break;
		default:
			OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
		}
		break;
	case SELLIST_KIND_OPER: 
		OOSQL_ERR(eINTERNALERROR_OOSQL); 
		break;
	case SELLIST_KIND_CONS:
		OOSQL_ERR(eINTERNALERROR_OOSQL);
		break;
	default:
		OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
	}

	return eNOERROR;
}

Four OQL_ASTtoGDS::smtChkQuDel(ASTNodeIdx node, ReturnType& returnType)
{
	ASTNodeIdx  sFrom, sWhere;
    ReturnType  sType;
	Four e;

	e = m_gds->init(m_pool, OQL_GDS::DELETE_QUERY);
	OOSQL_CHECK_ERR(e);

	if(AST(node).nodeName == QuDeferredDel)
	{
		e = m_gds->setDeferredDeleteFlag(m_pool, SM_TRUE);
		OOSQL_CHECK_ERR(e);
	}
	else
	{
		e = m_gds->setDeferredDeleteFlag(m_pool, SM_FALSE);
		OOSQL_CHECK_ERR(e);
	}

    sFrom    = AST(node).son;
    sWhere   = AST(sFrom).brother;

	m_progressStatus = FROM_CLAUSE;       e = smtChkClFr(sFrom, sType);       OOSQL_CHECK_ERR(e);
	m_progressStatus = WHERE_CLAUSE;      e = smtChkClWh(sWhere, sType);      OOSQL_CHECK_ERR(e);
	m_progressStatus = NO_CALUSE;

	return eNOERROR;
}

Four OQL_ASTtoGDS::getQGnodes_argument(ArgumentPoolElements& argumentToCheck, QGNodeIndexArray &results)
{
    PathExprPoolElements        pathExpr(m_pool->pathExprPool);
    FunctionPoolElements        func(m_pool->funcPool);
    AggrFuncPoolElements        aggrFunc(m_pool->aggrFuncPool);
    ArgumentPoolElements        argument(m_pool->argumentPool);
    ExprPoolElements            expr(m_pool->exprPool);
    Four                        i, j;
    Four                        e;

    for(i = 0;i < argumentToCheck.size; i++)
    {
        switch(argumentToCheck[i].argumentKind)
        {
        case ARGUMENT_KIND_PATHEXPR:
            pathExpr             = argumentToCheck[i].pathExpr;
            results.add(pathExpr[pathExpr.size - 1].qgNodeNo.startIndex);

            for(j = 0; j < pathExpr.size; j++)
            {
                if(pathExpr[j].fromAttrKind == PATHEXPR_KIND_METHOD)
                {
                    argument = pathExpr[j].method.argument;
                    e = getQGnodes_argument(argument, results);
                    OOSQL_CHECK_ERR(e);
                }

                if(pathExpr[j].fromAttrKind == PATHEXPR_KIND_FUNC)
                {
                    func = pathExpr[j].func;
                    argument = func[0].argument;
                    e = getQGnodes_argument(argument, results);
                    OOSQL_CHECK_ERR(e);
                }
            }
            break;

        case ARGUMENT_KIND_AGGRFUNC:
            // check arguments
            aggrFunc = argumentToCheck[i].aggrFunc;
            argument = aggrFunc[0].argument;
            e = getQGnodes_argument(argument, results);
            OOSQL_CHECK_ERR(e);
            break;

        case ARGUMENT_KIND_FUNC:
            // check arguments
            func     = argumentToCheck[i].func;
            argument = func[0].argument;
            e = getQGnodes_argument(argument, results);
            OOSQL_CHECK_ERR(e);
            break;

        case ARGUMENT_KIND_DOMAIN: 
            break;
        case ARGUMENT_KIND_VALUE:
            break;
        case ARGUMENT_KIND_EXPR:
            expr = argumentToCheck[i].expr;
            e = getQGnodes_traverseExprTree(expr, results);
            OOSQL_CHECK_ERR(e);
            break;
        default:
            OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
            break;
        }
    }

    return eNOERROR;
}

Four OQL_ASTtoGDS::smtChkQuAlterTbl(ASTNodeIdx node, ReturnType& returnType)
{
	DBCommandPoolElements		dbCommand(m_pool->dbCommandPool);
	ASTNodeIdx sClassName, sActList;
	ASTNodeIdx sAct, sAttr, sAttrList, sAttrName, sDomainName, sDomainNameSub;
	ASTNodeIdx sPrecision1, sPrecision2;
	ASTNodeIdx sOptions, sOptionName, sOptionValues;

	Four						i, j;
	Four						e;
	Four						addColNum;
	Four						dropColNum;
	Four						addColIdx;
	Four						dropColIdx;
	Four						embeddedAttributeNo;
	CataClassInfo				classInfo;
	SimpleString				className;
	SimpleString				attributeName;
	SimpleString				domainName;
	SimpleString				domainNameSub;
	SimpleString                optionName;
	SimpleString				optionValue;
	CataAttrInfo				attrInfo;
	CataAttrInfo				cataAttrInfo;
	StringPoolIndex				attributeNamePoolIndex;
	AttributeInfoPoolElements	addColInfo(m_pool->attributeInfoPool);
	AttributeInfoPoolElements	dropColInfo(m_pool->attributeInfoPool);
	char						addColNameList[LOM_MAXNUMOFATTRIBUTE][MAXATTRNAME];
	char						dropColNameList[LOM_MAXNUMOFATTRIBUTE][MAXATTRNAME];
    const Four					subClassArraySize = 20;
    CataClassInfo				subClassArray[subClassArraySize];
    Four						nSubClasses = subClassArraySize;
    Four						nthSubClass;
	Four						nAttrs;	
	Two							attributeNoIncludingDroppedCol;
	Four						nAttrsIncludingDroppedCol;	


	e = m_gds->init(m_pool, OQL_GDS::NO_QUERY);
	OOSQL_CHECK_ERR(e);


	// create new DBCommand
	dbCommand = m_pool->dbCommandPool.addNewEntry();
	dbCommand[0].command = DBCOMMAND_ALTER_TABLE;
	dbCommand[0].alterTableInfo.addColList.setNull();
	dbCommand[0].alterTableInfo.dropColList.setNull();


	// traverse AST
	sClassName		= AST(node).son;
	sActList		= AST(sClassName).brother;


	e = getStringPoolIndexFromID_Node(dbCommand[0].alterTableInfo.className, sClassName);
	OOSQL_CHECK_ERR(e);

	e = makeStringFromStringPool(className, dbCommand[0].alterTableInfo.className);
	OOSQL_CHECK_ERR(e);

	e = m_catalog->class_ClassName_to_CataClassInfo(className, classInfo);
    if(e == eNOT_FOUND_OOSQL)
    {
        m_errorMessage->Clear();
		m_errorMessage->Append("The class '");
		m_errorMessage->Append((char*)className);
		m_errorMessage->Append("' is not defined in the database.");

        OOSQL_ERR(eCLASSNOTDEFINED_OOSQL);
    }
	else
		OOSQL_CHECK_ERR(e);

	Four classId;
	e = m_catalog->class_ClassInfo_to_ClassId(classInfo, classId);
	OOSQL_CHECK_ERR(e);
	if(classId < 0) {
		m_errorMessage->Clear();
		m_errorMessage->Append("ALTER command is not allowed for a temoporary class. The class '");
		m_errorMessage->Append((char*)className);
		m_errorMessage->Append("' is a temporary class.");

		OOSQL_ERR(eCANTALTERTABLE_OOSQL);
	}

	sAct = AST(sActList).son;
	addColNum = 0;
	dropColNum = 0;
	if(AST(sAct).nodeName != null)
	{
		while(sAct != AST_NULL)
		{
			// add column clause
			if(AST(sAct).nodeName == AltActAddCol)
			{
				sAttr	= AST(sAct).son;

				while (sAttr != AST_NULL)
				{					
					sAttrName   = AST(sAttr).son;
					sDomainName = AST(sAttrName).brother;
					
					sAttrName   = AST(sAttrName).son;
					sDomainName = AST(sDomainName).son;

					// get attribute name
					e = getStringPoolIndexFromID_Node(attributeNamePoolIndex, sAttrName);
					OOSQL_CHECK_ERR(e);

					e = makeStringFromStringPool(attributeName, attributeNamePoolIndex);
					OOSQL_CHECK_ERR(e);

					e = m_catalog->attr_AttrName_to_CataAttrInfo(classInfo, attributeName, attrInfo);
					if(e != eNOT_FOUND_OOSQL)
					{
						m_errorMessage->Clear();
						m_errorMessage->Append("The attribute '");
						m_errorMessage->Append(attributeName);
						m_errorMessage->Append("' is already defined in the class '");
						m_errorMessage->Append(className);
						m_errorMessage->Append("'.");

						OOSQL_ERR(eATTRALREADYDEFINED_OOSQL);
					}

					for (i = 0; i < addColNum; i++)
					{
						if (strcmp(addColNameList[i], (char*)attributeName) == 0)
						{
							m_errorMessage->Clear();
							m_errorMessage->Append("A ADD column clause can't be used with other ADD column clause having same column name.");

							OOSQL_ERR(eCANTADDCOLUMN_OOSQL);
						}
					}
					strcpy(addColNameList[addColNum], (char*)attributeName);
					addColNum++;

					sAttr = AST(sAttr).brother;
				}
			}		
			// drop column clause
			else if(AST(sAct).nodeName == AltActDropCol)
			{
				sAttrName	= AST(sAct).son;

				// get attribute name
				e = getStringPoolIndexFromID_Node(attributeNamePoolIndex, sAttrName);
				OOSQL_CHECK_ERR(e);

				e = makeStringFromStringPool(attributeName, attributeNamePoolIndex);
				OOSQL_CHECK_ERR(e);

				e = m_catalog->attr_AttrName_to_CataAttrInfo(classInfo, attributeName, attrInfo);
				if(e == eNOT_FOUND_OOSQL)
				{
					m_errorMessage->Clear();
					m_errorMessage->Append("The attribute '");
					m_errorMessage->Append(attributeName);
					m_errorMessage->Append("' is not defined in the class '");
					m_errorMessage->Append(className);
					m_errorMessage->Append("'.");

					OOSQL_ERR(eATTRNOTDEFINED_OOSQL);
				}

				for (i = 0; i < dropColNum; i++)
				{
					if (strcmp(dropColNameList[i], (char*)attributeName) == 0)
					{
						m_errorMessage->Clear();
						m_errorMessage->Append("A DROP column clause can't be used with other DROP column clause having same column name.");

						OOSQL_ERR(eCANTDROPCOLUMN_OOSQL);
					}
				}
				strcpy(dropColNameList[dropColNum], (char*)attributeName);
				dropColNum++;
			}
			// drop column list clause
			else if(AST(sAct).nodeName == AltActDropColList)
			{
				sAttrName	= AST(sAct).son;

				while (sAttrName != AST_NULL)
				{					
					// get attribute name
					e = getStringPoolIndexFromID_Node(attributeNamePoolIndex, sAttrName);
					OOSQL_CHECK_ERR(e);

					e = makeStringFromStringPool(attributeName, attributeNamePoolIndex);
					OOSQL_CHECK_ERR(e);

					e = m_catalog->attr_AttrName_to_CataAttrInfo(classInfo, attributeName, attrInfo);
					if(e == eNOT_FOUND_OOSQL)
					{
						m_errorMessage->Clear();
						m_errorMessage->Append("The attribute '");
						m_errorMessage->Append(attributeName);
						m_errorMessage->Append("' is not defined in the class '");
						m_errorMessage->Append(className);
						m_errorMessage->Append("'.");

						OOSQL_ERR(eATTRNOTDEFINED_OOSQL);
					}

					for (i = 0; i < dropColNum; i++)
					{
						if (strcmp(dropColNameList[i], (char*)attributeName) == 0)
						{
							m_errorMessage->Clear();
							m_errorMessage->Append("A DROP column clause cann't be used with other DROP column clause having same column name.");

							OOSQL_ERR(eCANTDROPCOLUMN_OOSQL);
						}
					}
					strcpy(dropColNameList[dropColNum], (char*)attributeName);
					dropColNum++;

					sAttrName = AST(sAttrName).brother;
				}
			}

			// next alter table clause
			sAct = AST(sAct).brother;
		}
	}

	// check if given class is leaf class or not
    nthSubClass = 0;
    nSubClasses = m_catalog->class_GetSubClasses(classInfo, nthSubClass, subClassArraySize, subClassArray);
	if (nSubClasses != 0)
	{
		m_errorMessage->Clear();
		m_errorMessage->Append("ALTER TABLE statement can be executed for only class that have no subclasses.");

		OOSQL_ERR(eCANTALTERTABLE_OOSQL);
	}

	// check if given class have only one column 
	e = m_catalog->class_GetN_Attrs(classInfo, nAttrs);
	OOSQL_CHECK_ERR(e);
	
	if (nAttrs - 1 == dropColNum) // -1 adjusts the existance of _logicalId
	{
		m_errorMessage->Clear();
		m_errorMessage->Append("Can't drop all columns in given table/class");

		OOSQL_ERR(eCANTDROPCOLUMN_OOSQL);
	}

	if (nAttrs + addColNum > LOM_MAXNUMOFATTRIBUTE)
	{
		SimpleString maxNumOfCols;
		sprintf(maxNumOfCols, "%ld", LOM_MAXNUMOFATTRIBUTE - 1); // -1 adjusts the existance of _logicalId
		m_errorMessage->Clear();
		m_errorMessage->Append("Too many attributes. A class cannot have more than ");
		m_errorMessage->Append(maxNumOfCols);
		m_errorMessage->Append(" attributes.");

		OOSQL_ERR(eTOOMANYATTR_OOSQL);
	}


	dbCommand[0].alterTableInfo.addColList = m_pool->attributeInfoPool.addNewEntry(addColNum);
	dbCommand[0].alterTableInfo.addColList.setPoolIndex(0, addColNum);
	addColInfo = dbCommand[0].alterTableInfo.addColList;

	dbCommand[0].alterTableInfo.dropColList = m_pool->attributeInfoPool.addNewEntry(dropColNum);
	dbCommand[0].alterTableInfo.dropColList.setPoolIndex(addColNum, dropColNum);
	dropColInfo = dbCommand[0].alterTableInfo.dropColList;




	addColIdx = 0;
	dropColIdx = 0;
	sAct = AST(sActList).son;
	if(AST(sAct).nodeName != null)
	{
		while(sAct != AST_NULL)
		{
			if(AST(sAct).nodeName == AltActAddCol)
			{
				sAttr	= AST(sAct).son;

				while (sAttr != AST_NULL)
				{					
					sAttrName   = AST(sAttr).son;
					sDomainName = AST(sAttrName).brother;
					
					sAttrName   = AST(sAttrName).son;
					sDomainName = AST(sDomainName).son;

					if(AST(sDomainName).brother != AST_NULL && AST(AST(sDomainName).brother).nodeName == ID)
					{
						sDomainNameSub = AST(sDomainName).brother;
						sPrecision1    = AST(sDomainNameSub).brother;
					}
					else
					{
						sDomainNameSub = AST_NULL;
						sPrecision1    = AST(sDomainName).brother;
					}

					if(sPrecision1 != AST_NULL)
						sPrecision2 = AST(sPrecision1).brother;
					else
						sPrecision2 = AST_NULL;

					// get attribute name
					e = getStringPoolIndexFromID_Node(attributeNamePoolIndex, sAttrName);
					OOSQL_CHECK_ERR(e);

					e = makeStringFromStringPool(attributeName, attributeNamePoolIndex);
					OOSQL_CHECK_ERR(e);

					// set attribute name
					addColInfo[addColIdx].attributeName = attributeNamePoolIndex;

					// get domain name
					e = getStringFromID_Node(domainName, sDomainName);
					OOSQL_CHECK_ERR(e);
					makeLowerCaseString(domainName);

					if (domainName == (char*)"text")
					{
						if(strlen(className) > (LOM_MAXCLASSNAME - 20)/2)
						{
							SimpleString maxClassName;
							sprintf(maxClassName, "%ld", (LOM_MAXCLASSNAME - 20)/2);
							m_errorMessage->Clear();
							m_errorMessage->Append("Class name too long. (Maximum ");
							m_errorMessage->Append(maxClassName);
							m_errorMessage->Append(" characters available for the class including the text attribute)");
		
							OOSQL_ERR(eTOOLONGID_OOSQL);
						}
						if(strlen(attributeName) > (LOM_MAXCLASSNAME - 20)/2)
						{
							SimpleString maxAttrName;
							sprintf(maxAttrName, "%ld", (LOM_MAXCLASSNAME - 20)/2);
							m_errorMessage->Clear();
							m_errorMessage->Append("Attribute name too long. (Maximum ");
							m_errorMessage->Append(maxAttrName);
							m_errorMessage->Append(" characters available for the text attribute)");
		
							OOSQL_ERR(eTOOLONGID_OOSQL);
						}
					}
					if (strlen(attributeName) > LOM_MAXCLASSNAME - 20)
					{
						SimpleString maxAttrName;
						sprintf(maxAttrName, "%ld", (LOM_MAXCLASSNAME - 20));
						m_errorMessage->Clear();
						m_errorMessage->Append("Attribute name too long. (Maximum ");
						m_errorMessage->Append(maxAttrName);
						m_errorMessage->Append(" characters available)");
	
						OOSQL_ERR(eTOOLONGID_OOSQL);
					}

					if(sDomainNameSub != AST_NULL)
					{
						e = getStringFromID_Node(domainNameSub, sDomainNameSub);
						OOSQL_CHECK_ERR(e);
						makeLowerCaseString(domainNameSub);
					}		

					// In GCC-3.x or higher, must identify the type of variables
					if(domainName == (char* )"set" 		|| domainName == (char* )"bag" 		|| domainName == (char* )"list" ||
					   domainName == (char* )"odmg_set" || domainName == (char* )"odmg_bag" || domainName == (char* )"odmg_list" || 
					   domainName == (char* )"odmg_array")
					{
						addColInfo[addColIdx].inheritedFrom = -1;
						if(domainName == (char* )"set")			
							addColInfo[addColIdx].complexType   = LOM_COMPLEXTYPE_COLLECTIONSET;
						else if(domainName == (char* )"bag")	
							addColInfo[addColIdx].complexType   = LOM_COMPLEXTYPE_COLLECTIONBAG;
						else if(domainName == (char* )"list")
							addColInfo[addColIdx].complexType   = LOM_COMPLEXTYPE_COLLECTIONLIST;
						else if(domainName == (char* )"odmg_set")	
							addColInfo[addColIdx].complexType   = LOM_COMPLEXTYPE_ODMG_COLLECTIONSET;
						else if(domainName == (char* )"odmg_bag")	
							addColInfo[addColIdx].complexType   = LOM_COMPLEXTYPE_ODMG_COLLECTIONBAG;
						else if(domainName == (char* )"odmg_list")	
							addColInfo[addColIdx].complexType   = LOM_COMPLEXTYPE_ODMG_COLLECTIONLIST;
						else if(domainName == (char* )"odmg_array")	
							addColInfo[addColIdx].complexType   = LOM_COMPLEXTYPE_ODMG_COLLECTIONARRAY;
						
						addColInfo[addColIdx].domain = typeNameToTypeID(domainNameSub);
						addColInfo[addColIdx].length = typeNameToTypeLength(domainNameSub);

						if(addColInfo[addColIdx].domain == TYPEID_STRING)
							addColInfo[addColIdx].length = m_pool->intPool[AST(sPrecision1).tokenVal];
						else if(addColInfo[addColIdx].domain == TYPEID_VARSTRING)
							addColInfo[addColIdx].length = m_pool->intPool[AST(sPrecision1).tokenVal];
						else if(addColInfo[addColIdx].domain == TYPEID_NONE)
						{
							m_errorMessage->Clear();
							m_errorMessage->Append("Type '");
							m_errorMessage->Append((char*)domainName);
							m_errorMessage->Append("' at ");
							ERRORMESSAGE_APPEND_POSITION(m_errorMessage, sDomainName);
							m_errorMessage->Append(" is not supported type.");

							OOSQL_ERR(eNOTSUPPORTED_ATTR_TYPE_OOSQL);
						}
					}
					else
					{
						addColInfo[addColIdx].inheritedFrom = -1;
						addColInfo[addColIdx].complexType   = LOM_COMPLEXTYPE_BASIC;

						addColInfo[addColIdx].domain = typeNameToTypeID(domainName);
						addColInfo[addColIdx].length = typeNameToTypeLength(domainName);

						if(addColInfo[addColIdx].domain == TYPEID_STRING)
							addColInfo[addColIdx].length = m_pool->intPool[AST(sPrecision1).tokenVal];
						else if(addColInfo[addColIdx].domain == TYPEID_VARSTRING)
							addColInfo[addColIdx].length = m_pool->intPool[AST(sPrecision1).tokenVal];
						else if(addColInfo[addColIdx].domain == TYPEID_NONE)
						{
							m_errorMessage->Clear();
							m_errorMessage->Append("Type '");
							m_errorMessage->Append((char*)domainName);
							m_errorMessage->Append("' at ");
							ERRORMESSAGE_APPEND_POSITION(m_errorMessage, sDomainName);
							m_errorMessage->Append(" is not supported type.");

							OOSQL_ERR(eNOTSUPPORTED_ATTR_TYPE_OOSQL); 
						}
#ifdef  SLIMDOWN_TEXTIR
                        else if(addColInfo[addColIdx].domain== TYPEID_TEXT)
                        {
                            OOSQL_ERR(eTEXTIR_NOTENABLED_OOSQL);
                        }
#endif

						
					}
					addColIdx++;

					sAttr = AST(sAttr).brother;
				}
			}
			else if(AST(sAct).nodeName == AltActDropCol)
			{
				sAttrName	= AST(sAct).son;


				// get attribute name
				e = getStringPoolIndexFromID_Node(attributeNamePoolIndex, sAttrName);
				OOSQL_CHECK_ERR(e);

				e = makeStringFromStringPool(attributeName, attributeNamePoolIndex);
				OOSQL_CHECK_ERR(e);

				// set attribute name
				dropColInfo[dropColIdx].attributeName = attributeNamePoolIndex;
				dropColIdx++;
			}		
			else if(AST(sAct).nodeName == AltActDropColList)
			{
				sAttrName	= AST(sAct).son;

				while (sAttrName != AST_NULL)
				{					
					// get attribute name
					e = getStringPoolIndexFromID_Node(attributeNamePoolIndex, sAttrName);
					OOSQL_CHECK_ERR(e);

					e = makeStringFromStringPool(attributeName, attributeNamePoolIndex);
					OOSQL_CHECK_ERR(e);

					// set attribute name
					dropColInfo[dropColIdx].attributeName = attributeNamePoolIndex;
					dropColIdx++;

					sAttrName = AST(sAttrName).brother;
				}
			}
			
			sAct = AST(sAct).brother;
		}
	}


	sAct = AST(sActList).son;

	addColInfo = dbCommand[0].alterTableInfo.addColList;
	if(AST(sAct).nodeName != null)
	{
		while(sAct != AST_NULL)
		{
			if(AST(sAct).nodeName == AltActAddCol)
			{
				sAttr	= AST(sAct).son;

				while (sAttr != AST_NULL)
				{					
					sAttrName   = AST(sAttr).son;
					sDomainName = AST(sAttrName).brother;
					
					sAttrName   = AST(sAttrName).son;
					sDomainName = AST(sDomainName).son;
					
					// get attribute name
					e = getStringFromID_Node(attributeName, sAttrName);
					OOSQL_CHECK_ERR(e);

					// get domain name
					e = getStringFromID_Node(domainName, sDomainName);
					OOSQL_CHECK_ERR(e);

					makeLowerCaseString(domainName);

					if(domainName == (char* )"text" && AST(sDomainName).brother != AST_NULL && 
						AST(AST(sDomainName).brother).nodeName == QuUserFunction)
					{
						dbCommand = m_pool->dbCommandPool.addNewEntry();
						dbCommand[0].command = DBCOMMAND_DEFINE_POSTING_STRUCTURE;
						e = getStringPoolIndexFromID_Node(dbCommand[0].definePostingStructureInfo.className, sClassName);
						OOSQL_CHECK_ERR(e);
						e = getStringPoolIndexFromID_Node(dbCommand[0].definePostingStructureInfo.attributeName, sAttrName);
						OOSQL_CHECK_ERR(e);
						dbCommand[0].definePostingStructureInfo.isContainingTupleID				= SM_TRUE;
						dbCommand[0].definePostingStructureInfo.isContainingSentenceAndWordNum	= SM_TRUE;
						dbCommand[0].definePostingStructureInfo.isContainingByteOffset			= SM_FALSE;
						dbCommand[0].definePostingStructureInfo.nEmbeddedAttributes				= 0;

						sOptions = AST(sDomainName).brother;
						while(sOptions != AST_NULL)
						{
							if(AST(sOptions).nodeName != QuUserFunction)
							{
								m_errorMessage->Clear();
								m_errorMessage->Append("Text type option used at ");
								ERRORMESSAGE_APPEND_POSITION(m_errorMessage, sOptions);
								m_errorMessage->Append(" is not supported.");

								OOSQL_ERR(eBAD_TEXT_DOMAIN_OPTION_OOSQL);
							}

							sOptionName  = AST(sOptions).son;
							sOptionValues = AST(sOptionName).brother;
							e = getStringFromID_Node(optionName, sOptionName);
							OOSQL_CHECK_ERR(e);

							makeLowerCaseString(optionName);
						
							if(optionName == (char* )"contain_tupleid")	
							{
								if(AST(sOptionValues).nodeName == VaTr)
									dbCommand[0].definePostingStructureInfo.isContainingTupleID = SM_TRUE;
								else if(AST(sOptionValues).nodeName == VaFls)
									dbCommand[0].definePostingStructureInfo.isContainingTupleID = SM_FALSE;
								else
									OOSQL_ERR(eINTERNALERROR_OOSQL);
							}
							else if(optionName == (char* )"contain_sentence_word_offset")
							{
								if(AST(sOptionValues).nodeName == VaTr)
									dbCommand[0].definePostingStructureInfo.isContainingSentenceAndWordNum = SM_TRUE;
								else if(AST(sOptionValues).nodeName == VaFls)
									dbCommand[0].definePostingStructureInfo.isContainingSentenceAndWordNum = SM_FALSE;
								else
									OOSQL_ERR(eINTERNALERROR_OOSQL);
							}
							else if(optionName == (char* )"contain_byte_offset")
							{
								if(AST(sOptionValues).nodeName == VaTr)
									dbCommand[0].definePostingStructureInfo.isContainingByteOffset = SM_TRUE;
								else if(AST(sOptionValues).nodeName == VaFls)
									dbCommand[0].definePostingStructureInfo.isContainingByteOffset = SM_FALSE;
								else
									OOSQL_ERR(eINTERNALERROR_OOSQL);
							}
							else if(optionName == (char* )"embedded_attributes")
							{
								while(sOptionValues != AST_NULL)
								{
									e = getStringFromID_Node(optionValue, sOptionValues);
									OOSQL_CHECK_ERR(e);

									for(i = 0; i < nAttrs; i++)
									{
										e = m_catalog->attr_ColNo_to_AttrInfo(classInfo, (short)i, cataAttrInfo);
										OOSQL_CHECK_ERR(e);

										e = m_catalog->attr_CataAttrInfo_to_AttrName(classInfo, cataAttrInfo, attributeName);
										OOSQL_CHECK_ERR(e);

										if(optionValue == attributeName)
										{	
											e = m_catalog->attr_ColNo_to_ColNoIncludingDroppedCol(classInfo, (short)i, attributeNoIncludingDroppedCol);
											embeddedAttributeNo = attributeNoIncludingDroppedCol;
											OOSQL_CHECK_ERR(e);
											break;
										}
									}
									for(j = 0; j < addColInfo.size; j++)
									{
										e = makeStringFromStringPool(attributeName, addColInfo[j].attributeName);
										OOSQL_CHECK_ERR(e);

										if(optionValue == attributeName)
										{	
											e = m_catalog->class_GetN_AttrsIncludingDroppedCol(classInfo, nAttrsIncludingDroppedCol);
											embeddedAttributeNo = j + nAttrsIncludingDroppedCol;
											break;
										}
									}
									if(i == nAttrs && j == addColInfo.size)
									{
										m_errorMessage->Clear();
										m_errorMessage->Append("Text type option used at ");
										ERRORMESSAGE_APPEND_POSITION(m_errorMessage, sOptionValues);
										m_errorMessage->Append(" is not supported.");

										OOSQL_ERR(eBAD_TEXT_DOMAIN_OPTION_OOSQL);	
									}

									if(addColInfo[embeddedAttributeNo].domain == TYPEID_TEXT)
									{
										m_errorMessage->Clear();
										m_errorMessage->Append("Text type option used at ");
										ERRORMESSAGE_APPEND_POSITION(m_errorMessage, sOptionValues);
										m_errorMessage->Append(" is not supported.");

										OOSQL_ERR(eBAD_TEXT_DOMAIN_OPTION_OOSQL);	
									}

									dbCommand[0].definePostingStructureInfo.embeddedAttrNo[dbCommand[0].definePostingStructureInfo.nEmbeddedAttributes] = (short)embeddedAttributeNo;
									dbCommand[0].definePostingStructureInfo.nEmbeddedAttributes ++;

									sOptionValues = AST(sOptionValues).brother;
								}
							}
							else
							{
								m_errorMessage->Clear();
								m_errorMessage->Append("Text type option used at ");
								ERRORMESSAGE_APPEND_POSITION(m_errorMessage, sOptionName);
								m_errorMessage->Append(" is not supported.");

								OOSQL_ERR(eBAD_TEXT_DOMAIN_OPTION_OOSQL);
							}

							sOptions = AST(sOptions).brother;
						}
					}

					sAttr = AST(sAttr).brother;
				}
			} // if(AST(sAct).nodeName == AltActAddCol) end
			sAct = AST(sAct).brother;
		}
	}

	return eNOERROR;
}


Four OQL_ASTtoGDS::smtChkQuCreateSeq(ASTNodeIdx node, ReturnType& returnType)
{
	DBCommandPoolElements		dbCommand(m_pool->dbCommandPool);
	ASTNodeIdx sSequenceName, sOptList;
	ASTNodeIdx sOpt;

	Four						i, j;
	Four						e;
	int							startWith;
	SimpleString				sequenceName;

	

	e = m_gds->init(m_pool, OQL_GDS::NO_QUERY);
	OOSQL_CHECK_ERR(e);


	// create new DBCommand
	dbCommand = m_pool->dbCommandPool.addNewEntry();
	dbCommand[0].command = DBCOMMAND_CREATE_SEQUENCE;


	// traverse AST
	sSequenceName	= AST(node).son;
	sOptList		= AST(sSequenceName).brother;


	e = getStringPoolIndexFromID_Node(dbCommand[0].createSequenceInfo.sequenceName, sSequenceName);
	OOSQL_CHECK_ERR(e);

	e = makeStringFromStringPool(sequenceName, dbCommand[0].createSequenceInfo.sequenceName);
	OOSQL_CHECK_ERR(e);

	e = m_storageManager->CheckSequence(m_volId, sequenceName);
    if(e != eNOT_FOUND_OOSQL)
    {
        m_errorMessage->Clear();
		m_errorMessage->Append("The sequence '");
		m_errorMessage->Append((char*)sequenceName);
		m_errorMessage->Append("' is already defined in the database.");

        OOSQL_ERR(eSEQUENCEALREADYDEFINED_OOSQL);	
   }

	// #define SM_COUNTER_NAME_MAX_LEN LOM_MAXCLASSNAME
	if (strlen(sequenceName) > LOM_MAXCLASSNAME - 1) // null termination
	{
		SimpleString maxSeqName;
		sprintf(maxSeqName, "%ld", LOM_MAXCLASSNAME -1);
		m_errorMessage->Clear();
		m_errorMessage->Append("Sequence name too long. (Maximum ");
		m_errorMessage->Append(maxSeqName);
		m_errorMessage->Append(" characters available)");

		OOSQL_ERR(eTOOLONGID_OOSQL);
	}

	dbCommand[0].createSequenceInfo.startWith = 0;

	sOpt = AST(sOptList).son;
	if(AST(sOpt).nodeName != null)
	{
		while(sOpt != AST_NULL)
		{
			if(AST(sOpt).nodeName == SeqStartWith)
			{
				e = getFourFromINTEGER_NODE(startWith, AST(sOpt).son);
				OOSQL_CHECK_ERR(e);

				dbCommand[0].createSequenceInfo.startWith = startWith - 1;
			}		
			sOpt = AST(sOpt).brother;
		}
	}

	return eNOERROR;
}

Four OQL_ASTtoGDS::smtChkQuDropSeq(ASTNodeIdx node, ReturnType& returnType)
{
	DBCommandPoolElements		dbCommand(m_pool->dbCommandPool);
	ASTNodeIdx sSequenceName, sOptList;
	ASTNodeIdx sOpt;

	Four						i, j;
	Four						e;
	int							startWith;
	SimpleString				sequenceName;

	

	e = m_gds->init(m_pool, OQL_GDS::NO_QUERY);
	OOSQL_CHECK_ERR(e);


	// create new DBCommand
	dbCommand = m_pool->dbCommandPool.addNewEntry();
	dbCommand[0].command = DBCOMMAND_DROP_SEQUENCE;


	// traverse AST
	sSequenceName	= AST(node).son;
	sOptList		= AST(sSequenceName).brother;


	e = getStringPoolIndexFromID_Node(dbCommand[0].dropSequenceInfo.sequenceName, sSequenceName);
	OOSQL_CHECK_ERR(e);

	e = makeStringFromStringPool(sequenceName, dbCommand[0].dropSequenceInfo.sequenceName);
	OOSQL_CHECK_ERR(e);

	e = m_storageManager->CheckSequence(m_volId, sequenceName);
    if(e == eNOT_FOUND_OOSQL)
    {
        m_errorMessage->Clear();
		m_errorMessage->Append("The sequence '");
		m_errorMessage->Append((char*)sequenceName);
		m_errorMessage->Append("' is not defined in the database.");

        OOSQL_ERR(eSEQUENCENOTDEFINED_OOSQL);	
    }

	return eNOERROR;
}


Four OQL_ASTtoGDS::smtChkQuCreateTbl(ASTNodeIdx node, ReturnType& returnType)
{
	ASTNodeIdx sClassName, sSuperClasses, sKeyDef, sAttrList;
	ASTNodeIdx sSuperClass;
	ASTNodeIdx sAttr, sAttrName, sDomainName, sDomainNameSub, sPrecision1, sPrecision2;
	ASTNodeIdx sOptions, sOptionName, sOptionValues;
	ASTNodeIdx sFuncName;
	ASTNodeIdx sFuncArgList, sFuncArg, sFuncArgType, sFuncArgLocactorFlag;
	ASTNodeIdx sFuncReturns;
	ASTNodeIdx sFuncSpecific;
	ASTNodeIdx sFuncExternalName;
	ASTNodeIdx sFuncDeterministic;
	ASTNodeIdx sFuncExternalAction;
	ASTNodeIdx sFuncFenced;
	ASTNodeIdx sFuncNullCall;
	ASTNodeIdx sFuncLanguage;
	ASTNodeIdx sFuncParameterStyle;
	ASTNodeIdx sFuncScratchpad;
	ASTNodeIdx sFuncFinalCall;
	ASTNodeIdx sFuncParallel;
	ASTNodeIdx sFuncDbinfo;
	
	CataClassInfo				classInfo;
	SimpleString				className;
	SimpleString				domainName;
	SimpleString				domainNameSub;
	SimpleString				attributeName;
	SimpleString				prevAttributeName;
	SimpleString				keyAttributeName;
	SimpleString                optionName;
	SimpleString				optionValue;
	SimpleString				language, parameterStyle;
	SimpleString				argumentTypeName, argumentName, typeName;
	SimpleString				methodName, externalName, specificName;
	DBCommandPoolElements		dbCommand(m_pool->dbCommandPool);
	StringPoolIndex				attributeNamePoolIndex;
	AttributeInfoPoolElements	attributeInfo(m_pool->attributeInfoPool);
	MethodInfoPoolElements		methodInfo(m_pool->methodInfoPool);
	KeyInfoPoolElements			keyInfo(m_pool->keyInfoPool);
	CataClassInfo				superClassInfo;
	SimpleString				superClassName;
	Four						superClassId;
	SuperClassPoolElements		superClass(m_pool->superClassPool);
	Four						nAttrs, nMethods, nArguments;
	TypeID						typeId;
	Four						length;
	Four						inheritedFrom;
	Four						complexType;
	CataAttrInfo				cataAttrInfo;
	CataMethodInfo				cataMethodInfo;
	Four						i, j;
	Four						e;
	Four						embeddedAttributeNo;
	ArgumentTypePoolElements	argType(m_pool->argumentTypePool), argumentTypeList(m_pool->argumentTypePool);
	Boolean						deterministicFlag, externalActionFlag, fencedFlag, nullCallFlag, scratchPadFlag;
	Boolean						finalCallFlag, allowParallelFlag, dbInfoFlag, asLocatorFlag;
	ParameterMode				parameterMode;

	e = m_gds->init(m_pool, OQL_GDS::NO_QUERY);
	OOSQL_CHECK_ERR(e);

	// create new DBCommand
	dbCommand = m_pool->dbCommandPool.addNewEntry();
	dbCommand[0].command = DBCOMMAND_CREATE_TABLE;
	dbCommand[0].createTableInfo.attributeList.setNull();
	dbCommand[0].createTableInfo.keyList.setNull();
	dbCommand[0].createTableInfo.superClasses.setNull();

	if(AST(node).nodeName == QuCreateTempTbl)
		dbCommand[0].createTableInfo.isTemporary = SM_TRUE;
	else
		dbCommand[0].createTableInfo.isTemporary = SM_FALSE;

	// traverse AST
	sClassName		= AST(node).son;
	sSuperClasses	= AST(sClassName).brother;
	sKeyDef			= AST(sSuperClasses).brother;
	sAttrList		= AST(sKeyDef).brother;

	// get class name
	e = getStringPoolIndexFromID_Node(dbCommand[0].createTableInfo.className, sClassName);
	OOSQL_CHECK_ERR(e);

	e = makeStringFromStringPool(className, dbCommand[0].createTableInfo.className);
	OOSQL_CHECK_ERR(e);

	// check if class already exists
	e = m_catalog->class_ClassName_to_CataClassInfo(className, classInfo);
	if(e == eNOERROR)
	{
		m_errorMessage->Clear();
		m_errorMessage->Append("The class '");
		m_errorMessage->Append(className);
		m_errorMessage->Append("' is duplicated in the database.");

		OOSQL_ERR(eDUPLCATED_CLASS_DEFINITION_OOSQL); 
	} 
	else if(e != eNOT_FOUND_OOSQL)
	{
		OOSQL_ERR(e);
	}

	if(strlen(className) > LOM_MAXCLASSNAME - 20)
	{
		SimpleString maxClassName;
		sprintf(maxClassName, "%ld", LOM_MAXCLASSNAME - 20);
		m_errorMessage->Clear();
		m_errorMessage->Append("Class name too long. (Maximum ");
		m_errorMessage->Append(maxClassName);
		m_errorMessage->Append(" characters available)");

		OOSQL_ERR(eTOOLONGID_OOSQL);
	}


	// get superclasses
	sSuperClass = AST(sSuperClasses).son;
	if(AST(sSuperClass).nodeName != null)
	{
		while(sSuperClass != AST_NULL)
		{
			// get super class name
			e = getStringFromID_Node(superClassName, sSuperClass);
			OOSQL_CHECK_ERR(e);

			// get super class info
			e = m_catalog->class_ClassName_to_CataClassInfo(superClassName, superClassInfo);
			if(e == eNOT_FOUND_OOSQL)
			{
				m_errorMessage->Clear();
				m_errorMessage->Append("The class '");
				m_errorMessage->Append(superClassName);
				m_errorMessage->Append("' is not defined in the database.");

				OOSQL_ERR(eNOSUCH_SUPERCLASS_EXIST_OOSQL); 
			}

			e = m_catalog->class_ClassInfo_to_ClassId(superClassInfo, superClassId);
			OOSQL_CHECK_ERR(e);

			if(superClassId < 0)
			{
				m_errorMessage->Clear();
				m_errorMessage->Append("A class cannot be created under a temporary class. The class '");
				m_errorMessage->Append(superClassName);
				m_errorMessage->Append("' is a temporary class.");

				OOSQL_ERR(eNOSUCH_SUPERCLASS_EXIST_OOSQL);
			}

			superClass = m_pool->superClassPool.addNewEntry();
			if(dbCommand[0].createTableInfo.superClasses == NULL_POOLINDEX)
				dbCommand[0].createTableInfo.superClasses = superClass.getPoolIndex();
			else
				dbCommand[0].createTableInfo.superClasses.size ++;

			e = getStringPoolIndexFromID_Node(superClass[0].className, sSuperClass);
			OOSQL_CHECK_ERR(e);

			e = m_catalog->class_GetN_Attrs(superClassInfo, nAttrs);
			OOSQL_CHECK_ERR(e);

            bool excludeFirstAttr = false;
            if(nAttrs > 0)
            {
                if(m_catalog->class_IsLrdsRelation(superClassInfo))
                {
                    
                    excludeFirstAttr = false;
                }
                else
                {
                    e = m_catalog->attr_ColNo_to_AttrInfo(superClassInfo, (short)0, cataAttrInfo);
				    OOSQL_CHECK_ERR(e);
                    e = m_catalog->attr_CataAttrInfo_to_AttrName(superClassInfo, cataAttrInfo, attributeName);
				    OOSQL_CHECK_ERR(e);
				
					if(attributeName == (char* )"_logicalId")	
                        excludeFirstAttr = true;
                    else
                        excludeFirstAttr = false;
                }

                if(excludeFirstAttr)
                    nAttrs --;

                attributeInfo = m_pool->attributeInfoPool.addNewEntry(nAttrs);
    			if(dbCommand[0].createTableInfo.attributeList == NULL_POOLINDEX)
    				dbCommand[0].createTableInfo.attributeList = attributeInfo.getPoolIndex();
    			else
    				dbCommand[0].createTableInfo.attributeList.size += nAttrs;
			}

			for(i = 0; i < nAttrs; i++)
			{
                if(excludeFirstAttr)
				    e = m_catalog->attr_ColNo_to_AttrInfo(superClassInfo, (short)i + 1, cataAttrInfo);
                else
                    e = m_catalog->attr_ColNo_to_AttrInfo(superClassInfo, (short)i, cataAttrInfo);
				OOSQL_CHECK_ERR(e);

				e = m_catalog->attr_CataAttrInfo_to_AttrName(superClassInfo, cataAttrInfo, attributeName);
				OOSQL_CHECK_ERR(e);
				e = m_catalog->attr_GetTypeID(superClassInfo, cataAttrInfo, typeId);
				OOSQL_CHECK_ERR(e);
				e = m_catalog->attr_GetLength(superClassInfo, cataAttrInfo, length);
				OOSQL_CHECK_ERR(e);
				e = m_catalog->attr_GetInheritedFrom(superClassInfo, cataAttrInfo, inheritedFrom); 
				OOSQL_CHECK_ERR(e);

				if(inheritedFrom == -1)
					inheritedFrom = superClassId;

				attributeInfo[i].attributeName	= installStringIntoStringPool(attributeName);
				attributeInfo[i].complexType    = OOSQL_MASK_COMPLEXTYPE(typeId);
				attributeInfo[i].domain			= OOSQL_MASK_TYPE(typeId);
				attributeInfo[i].length			= length;
				attributeInfo[i].inheritedFrom  = inheritedFrom;
			}

			e = m_catalog->class_GetN_Methods(superClassInfo, nMethods);
			OOSQL_CHECK_ERR(e);

			if(nMethods > 0)
			{
    			methodInfo = m_pool->methodInfoPool.addNewEntry(nMethods);
    			if(dbCommand[0].createTableInfo.methodList == NULL_POOLINDEX)
    				dbCommand[0].createTableInfo.methodList = methodInfo.getPoolIndex();
    			else
    				dbCommand[0].createTableInfo.methodList.size += nMethods;
			}

			for(i = 0; i < nMethods; i++)
			{
				e = m_catalog->method_MethodNo_to_MethodInfo(superClassInfo, (short)i, cataMethodInfo);
				OOSQL_CHECK_ERR(e);

				e = m_catalog->method_GetN_Arguments(superClassInfo, cataMethodInfo, nArguments);
				OOSQL_CHECK_ERR(e);

				argumentTypeList = m_pool->argumentTypePool.addNewEntry(nArguments);
				for(j = 0; j < nArguments; j++)
				{
					e = m_catalog->method_GetNTH_ArgumentTypeID(superClassInfo, cataMethodInfo, j, typeId); OOSQL_CHECK_ERR(e);
					e = m_catalog->method_GetNTH_ArgumentName(superClassInfo, cataMethodInfo, j, argumentName); OOSQL_CHECK_ERR(e);
					e = m_catalog->method_GetNTH_ArgumentTypeName(superClassInfo, cataMethodInfo, j, argumentTypeName); OOSQL_CHECK_ERR(e);
					e = m_catalog->method_GetNTH_ArgumentAsLocatorFlag(superClassInfo, cataMethodInfo, j, asLocatorFlag); OOSQL_CHECK_ERR(e);
					e = m_catalog->method_GetNTH_ArgumentParameterMode(superClassInfo, cataMethodInfo, j, parameterMode); OOSQL_CHECK_ERR(e);

					argumentTypeList[j].argumentName       = installStringIntoStringPool(argumentName);
					argumentTypeList[j].argumentType       = typeId;
					argumentTypeList[j].argumentTypeName   = installStringIntoStringPool(argumentTypeName);
					argumentTypeList[j].argumentTypeLength = typeNameToTypeLength(argumentTypeName);
					argumentTypeList[j].parameterMode      = parameterMode;
					argumentTypeList[j].asLocator          = asLocatorFlag;
				}
				
				e = m_catalog->method_CataMethodInfo_to_MethodName(superClassInfo, cataMethodInfo, methodName); OOSQL_CHECK_ERR(e);
				e = m_catalog->method_GetExternalName(superClassInfo, cataMethodInfo, externalName); OOSQL_CHECK_ERR(e);
				e = m_catalog->method_GetSpecificName(superClassInfo, cataMethodInfo, specificName); OOSQL_CHECK_ERR(e);
				e = m_catalog->method_GetDeterinisticFlag(superClassInfo, cataMethodInfo, deterministicFlag); OOSQL_CHECK_ERR(e);
				e = m_catalog->method_GetExternalActionFlag(superClassInfo, cataMethodInfo, externalActionFlag); OOSQL_CHECK_ERR(e);
				e = m_catalog->method_GetFencedFlag(superClassInfo, cataMethodInfo, fencedFlag); OOSQL_CHECK_ERR(e);
				e = m_catalog->method_GetNullCallFlag(superClassInfo, cataMethodInfo, nullCallFlag); OOSQL_CHECK_ERR(e);
				e = m_catalog->method_GetScratchPadFlag(superClassInfo, cataMethodInfo, scratchPadFlag); OOSQL_CHECK_ERR(e);
				e = m_catalog->method_GetFinalCallFlag(superClassInfo, cataMethodInfo, finalCallFlag); OOSQL_CHECK_ERR(e);
				e = m_catalog->method_GetAllowParallelFlag(superClassInfo, cataMethodInfo, allowParallelFlag); OOSQL_CHECK_ERR(e);
				e = m_catalog->method_GetDbInfoFlag(superClassInfo, cataMethodInfo, dbInfoFlag); OOSQL_CHECK_ERR(e);
				e = m_catalog->method_GetInheritedFrom(superClassInfo, cataMethodInfo, inheritedFrom); OOSQL_CHECK_ERR(e);
				e = m_catalog->method_GetLanguage(superClassInfo, cataMethodInfo, language); OOSQL_CHECK_ERR(e);
				e = m_catalog->method_GetParameterStyle(superClassInfo, cataMethodInfo, parameterStyle); OOSQL_CHECK_ERR(e);

				e = m_catalog->method_GetReturnTypeID(superClassInfo, cataMethodInfo, typeId); OOSQL_CHECK_ERR(e);
				e = m_catalog->method_GetReturnTypeName(superClassInfo, cataMethodInfo, typeName); OOSQL_CHECK_ERR(e);
				e = m_catalog->method_GetReturnParameterMode(superClassInfo, cataMethodInfo, parameterMode); OOSQL_CHECK_ERR(e);
				e = m_catalog->method_GetReturnAsLocatorFlag(superClassInfo, cataMethodInfo, asLocatorFlag); OOSQL_CHECK_ERR(e);

				if(inheritedFrom == -1)
					inheritedFrom = superClassId;

				methodInfo[i].functionName		= installStringIntoStringPool(methodName);
				methodInfo[i].argumentTypeList	= argumentTypeList.getPoolIndex();
				methodInfo[i].returnType.argumentName.setNull();
				methodInfo[i].returnType.argumentType		= typeId;
				methodInfo[i].returnType.argumentTypeName	= installStringIntoStringPool(typeName);
				methodInfo[i].returnType.argumentTypeLength = typeNameToTypeLength(typeName);
				methodInfo[i].returnType.parameterMode      = parameterMode;
				methodInfo[i].returnType.asLocator			= asLocatorFlag;
				methodInfo[i].specificName		= installStringIntoStringPool(externalName);
				methodInfo[i].externalName		= installStringIntoStringPool(specificName);
				methodInfo[i].deterministic		= deterministicFlag;
				methodInfo[i].externalAction	= externalActionFlag;
				methodInfo[i].fenced			= fencedFlag;
				methodInfo[i].nullCall			= nullCallFlag;
				methodInfo[i].language			= installStringIntoStringPool(language);
				methodInfo[i].parameterStyle	= installStringIntoStringPool(parameterStyle);
				methodInfo[i].scratchPad		= scratchPadFlag;
				methodInfo[i].finalCall			= finalCallFlag;
				methodInfo[i].allowParallel		= allowParallelFlag;
				methodInfo[i].dbInfo			= dbInfoFlag;
			}

			sSuperClass = AST(sSuperClass).brother;
		}
	}

	sAttr = AST(sAttrList).son;
	if(AST(sAttr).nodeName != null)
	{
		while(sAttr != AST_NULL)
		{
			if(AST(sAttr).nodeName == QuCreateMethod)
			{
				// check m_ast node structur and assign its index to local vars.
				sFuncName = AST(node).son;
				if(sFuncName == AST_NULL) OOSQL_ERR(eBADASTNODE_OOSQL);
				
				sFuncArgList = AST(sFuncName).brother;
				if(sFuncArgList == AST_NULL) OOSQL_ERR(eBADASTNODE_OOSQL);
				if(AST(sFuncArgList).nodeName != FuncArgList) OOSQL_ERR(eBADASTNODE_OOSQL);
				
				sFuncReturns = AST(sFuncArgList).brother;
				if(sFuncReturns == AST_NULL) OOSQL_ERR(eBADASTNODE_OOSQL);
				if(AST(sFuncReturns).nodeName != FuncReturns) OOSQL_ERR(eBADASTNODE_OOSQL);
				
				sFuncSpecific = AST(sFuncReturns).brother;
				if(sFuncSpecific == AST_NULL) OOSQL_ERR(eBADASTNODE_OOSQL);
				if(AST(sFuncSpecific).nodeName != FuncSpecific) OOSQL_ERR(eBADASTNODE_OOSQL);
				
				sFuncExternalName = AST(sFuncSpecific).brother;
				if(sFuncExternalName == AST_NULL) OOSQL_ERR(eBADASTNODE_OOSQL);
				if(AST(sFuncExternalName).nodeName != FuncExternalName) OOSQL_ERR(eBADASTNODE_OOSQL);
				
				sFuncDeterministic = AST(sFuncExternalName).brother;
				if(sFuncDeterministic == AST_NULL) OOSQL_ERR(eBADASTNODE_OOSQL);
				if(AST(sFuncDeterministic).nodeName != FuncDeterministic) OOSQL_ERR(eBADASTNODE_OOSQL);
				
				sFuncExternalAction = AST(sFuncDeterministic).brother;
				if(sFuncExternalAction == AST_NULL) OOSQL_ERR(eBADASTNODE_OOSQL);
				if(AST(sFuncExternalAction).nodeName != FuncExternalAction) OOSQL_ERR(eBADASTNODE_OOSQL);
				
				sFuncFenced = AST(sFuncExternalAction).brother;
				if(sFuncFenced == AST_NULL) OOSQL_ERR(eBADASTNODE_OOSQL);
				if(AST(sFuncFenced).nodeName != FuncFenced) OOSQL_ERR(eBADASTNODE_OOSQL);
				
				sFuncNullCall = AST(sFuncFenced).brother;
				if(sFuncNullCall == AST_NULL) OOSQL_ERR(eBADASTNODE_OOSQL);
				if(AST(sFuncNullCall).nodeName != FuncNullCall) OOSQL_ERR(eBADASTNODE_OOSQL);
				
				sFuncLanguage = AST(sFuncNullCall).brother;
				if(sFuncLanguage == AST_NULL) OOSQL_ERR(eBADASTNODE_OOSQL);
				if(AST(sFuncLanguage).nodeName != FuncLanguage) OOSQL_ERR(eBADASTNODE_OOSQL);
				
				sFuncParameterStyle = AST(sFuncLanguage).brother;
				if(sFuncParameterStyle == AST_NULL) OOSQL_ERR(eBADASTNODE_OOSQL);
				if(AST(sFuncParameterStyle).nodeName != FuncParameterStyle) OOSQL_ERR(eBADASTNODE_OOSQL);
				
				sFuncScratchpad = AST(sFuncParameterStyle).brother;
				if(sFuncScratchpad == AST_NULL) OOSQL_ERR(eBADASTNODE_OOSQL);
				if(AST(sFuncScratchpad).nodeName != FuncScratchpad) OOSQL_ERR(eBADASTNODE_OOSQL);
				
				sFuncFinalCall = AST(sFuncScratchpad).brother;
				if(sFuncFinalCall == AST_NULL) OOSQL_ERR(eBADASTNODE_OOSQL);
				if(AST(sFuncFinalCall).nodeName != FuncFinalCall) OOSQL_ERR(eBADASTNODE_OOSQL);
				
				sFuncParallel = AST(sFuncFinalCall).brother;
				if(sFuncParallel == AST_NULL) OOSQL_ERR(eBADASTNODE_OOSQL);
				if(AST(sFuncParallel).nodeName != FuncParallel) OOSQL_ERR(eBADASTNODE_OOSQL);
				
				sFuncDbinfo = AST(sFuncParallel).brother;
				if(sFuncDbinfo == AST_NULL) OOSQL_ERR(eBADASTNODE_OOSQL);
				if(AST(sFuncDbinfo).nodeName != FuncDbinfo) OOSQL_ERR(eBADASTNODE_OOSQL);

				methodInfo = m_pool->methodInfoPool.addNewEntry();
				if(dbCommand[0].createTableInfo.methodList == NULL_POOLINDEX)
					dbCommand[0].createTableInfo.methodList = methodInfo.getPoolIndex();
				else
					dbCommand[0].createTableInfo.methodList.size ++;

				// function name
				e = getStringPoolIndexFromID_Node(methodInfo[0].functionName, sFuncName);
				OOSQL_CHECK_ERR(e);

				// argument list
				sFuncArg = AST(sFuncArgList).son;
				methodInfo[0].argumentTypeList.setNull();
				while(sFuncArg != AST_NULL)
				{
					if(AST(sFuncArg).nodeName != FuncArg)
						OOSQL_ERR(eBADASTNODE_OOSQL);

					argType = m_pool->argumentTypePool.addNewEntry();
					if(methodInfo[0].argumentTypeList == NULL_POOLINDEX)
						methodInfo[0].argumentTypeList = argType.getPoolIndex();
					else
						methodInfo[0].argumentTypeList.size ++;

					sFuncArgType = AST(sFuncArg).son;
					sFuncArgLocactorFlag = AST(sFuncArgType).brother;

					argType[0].argumentName.setNull();

					e = getStringFromID_Node(argumentTypeName, sFuncArgType);
					OOSQL_CHECK_ERR(e);

					makeLowerCaseString(argumentTypeName);

					argType[0].argumentType = typeNameToTypeID(argumentTypeName);
					if(argType[0].argumentType == TYPEID_NONE)
						OOSQL_ERR(eNOTSUPPORTED_ATTR_TYPE_OOSQL);

					e = getStringPoolIndexFromID_Node(argType[0].argumentTypeName, sFuncArgType);
					OOSQL_CHECK_ERR(e);

					argType[0].argumentTypeLength = typeNameToTypeLength(argumentTypeName);

					argType[0].parameterMode = PARM_IN;

					if(sFuncArgLocactorFlag != AST_NULL && AST(sFuncArgLocactorFlag).nodeName == FucnArgAsLocator)
						argType[0].asLocator = SM_TRUE;
					else
						argType[0].asLocator = SM_FALSE;

					sFuncArg = AST(sFuncArg).brother;
				}

				// function returns
				sFuncArgType = AST(AST(sFuncReturns).son).son;
				sFuncArgLocactorFlag = AST(sFuncArgType).brother;

				methodInfo[0].returnType.argumentName.setNull();
				e = getStringFromID_Node(argumentTypeName, sFuncArgType);
				OOSQL_CHECK_ERR(e);
				makeLowerCaseString(argumentTypeName);
				methodInfo[0].returnType.argumentType = typeNameToTypeID(argumentTypeName);
				methodInfo[0].returnType.parameterMode = PARM_OUT;
				if(sFuncArgLocactorFlag != AST_NULL && AST(sFuncArgLocactorFlag).nodeName == FucnArgAsLocator)
					methodInfo[0].returnType.asLocator = SM_TRUE;
				else
					methodInfo[0].returnType.asLocator = SM_FALSE;

				// specific
				e = getStringPoolIndexFromID_Node(methodInfo[0].specificName, AST(sFuncSpecific).son);
				OOSQL_CHECK_ERR(e);

				// external name
				e = getStringPoolIndexFromSTRING_Node(methodInfo[0].externalName, AST(sFuncExternalName).son);
				OOSQL_CHECK_ERR(e);

				// deterministic
				if(AST(AST(sFuncDeterministic).son).nodeName == VaTr)
					methodInfo[0].deterministic = SM_TRUE;
				else
					methodInfo[0].deterministic = SM_FALSE;

				// external action
				if(AST(AST(sFuncExternalAction).son).nodeName == VaTr)
					methodInfo[0].externalAction = SM_TRUE;
				else
					methodInfo[0].externalAction = SM_FALSE;

				// fenced
				if(AST(AST(sFuncFenced).son).nodeName == VaTr)
					methodInfo[0].fenced = SM_TRUE;
				else
					methodInfo[0].fenced = SM_FALSE;

				// null call
				if(AST(AST(sFuncFenced).son).nodeName == VaTr)
					methodInfo[0].nullCall = SM_TRUE;
				else
					methodInfo[0].nullCall = SM_FALSE;

				// language
				// check language is if C, JAVA
				e = getStringPoolIndexFromID_Node(methodInfo[0].language, AST(sFuncLanguage).son);
				OOSQL_CHECK_ERR(e);
				
				e = makeStringFromStringPool(language, methodInfo[0].language);
				OOSQL_CHECK_ERR(e);

				makeLowerCaseString(language);

				if(!(language == (char* )"c" || language == (char *)"java"))	
					OOSQL_ERR(eUNSUPPORTED_LANGUAGE_OOSQL);
				
				// parameter style
				// check laguage is if OOSQL
				e = getStringPoolIndexFromID_Node(methodInfo[0].parameterStyle, AST(sFuncParameterStyle).son);
				OOSQL_CHECK_ERR(e);
				
				e = makeStringFromStringPool(parameterStyle, methodInfo[0].parameterStyle);
				OOSQL_CHECK_ERR(e);

				makeLowerCaseString(parameterStyle);

				if(!(parameterStyle == (char* )"oosql"))	
					OOSQL_ERR(eUNSUPPORTED_PARAMETER_STYLE_OOSQL);

				// scratchpad
				if(AST(AST(sFuncScratchpad).son).nodeName == VaTr)
					methodInfo[0].scratchPad = SM_TRUE;
				else
					methodInfo[0].scratchPad = SM_FALSE;

				// final call
				if(AST(AST(sFuncFinalCall).son).nodeName == VaTr)
					methodInfo[0].finalCall = SM_TRUE;
				else
					methodInfo[0].finalCall = SM_FALSE;

				// parallel
				if(AST(AST(sFuncParallel).son).nodeName == VaTr)
					methodInfo[0].allowParallel = SM_TRUE;
				else
					methodInfo[0].allowParallel = SM_FALSE;

				// dbinfo
				if(AST(AST(sFuncDbinfo).son).nodeName == VaTr)
					methodInfo[0].dbInfo = SM_TRUE;
				else
					methodInfo[0].dbInfo = SM_FALSE;

			}
			else /* AST(sAttr).name == TblAttr */
			{

				sAttrName   = AST(sAttr).son;
				sDomainName = AST(sAttrName).brother;
				
				sAttrName   = AST(sAttrName).son;
				sDomainName = AST(sDomainName).son;
				if(AST(sDomainName).brother != AST_NULL && AST(AST(sDomainName).brother).nodeName == ID)
				{
					sDomainNameSub = AST(sDomainName).brother;
					sPrecision1    = AST(sDomainNameSub).brother;
				}
				else
				{
					sDomainNameSub = AST_NULL;
					sPrecision1    = AST(sDomainName).brother;
				}

				if(sPrecision1 != AST_NULL)
					sPrecision2 = AST(sPrecision1).brother;
				else
					sPrecision2 = AST_NULL;

				// get attribute name
				e = getStringPoolIndexFromID_Node(attributeNamePoolIndex, sAttrName);
				OOSQL_CHECK_ERR(e);

				e = makeStringFromStringPool(attributeName, attributeNamePoolIndex);
				OOSQL_CHECK_ERR(e);

				attributeInfo = dbCommand[0].createTableInfo.attributeList;
				for(i = 0; i < attributeInfo.size; i ++)
				{
					e = makeStringFromStringPool(prevAttributeName, attributeInfo[i].attributeName);
					OOSQL_CHECK_ERR(e);

					if(prevAttributeName == attributeName)
					{	
						i = -1;
						break;
					}
				}
				if(i == -1)
				{
					sAttr = AST(sAttr).brother;
					continue;
				}

				attributeInfo = m_pool->attributeInfoPool.addNewEntry();
				if(dbCommand[0].createTableInfo.attributeList == NULL_POOLINDEX)
					dbCommand[0].createTableInfo.attributeList = attributeInfo.getPoolIndex();
				else
					dbCommand[0].createTableInfo.attributeList.size ++;

				if (dbCommand[0].createTableInfo.attributeList.size > LOM_MAXNUMOFATTRIBUTE - 1)
				{
					SimpleString maxNumOfCols;
					sprintf(maxNumOfCols, "%ld", LOM_MAXNUMOFATTRIBUTE - 1); // -1 adjusts the existance of _logicalId
					m_errorMessage->Clear();
					m_errorMessage->Append("Too many attributes. A class cannot have more than ");
					m_errorMessage->Append(maxNumOfCols);
					m_errorMessage->Append(" attributes.");

					OOSQL_ERR(eTOOMANYATTR_OOSQL);
				}

				// set attribute name
				attributeInfo[0].attributeName = attributeNamePoolIndex;

				// get domain name
				e = getStringFromID_Node(domainName, sDomainName);
				OOSQL_CHECK_ERR(e);
				makeLowerCaseString(domainName);

				if (domainName == (char*)"text")
				{
					if(strlen(className) > (LOM_MAXCLASSNAME - 20)/2)
					{
						SimpleString maxClassName;
						sprintf(maxClassName, "%ld", (LOM_MAXCLASSNAME - 20)/2);
						m_errorMessage->Clear();
						m_errorMessage->Append("Class name too long. (Maximum ");
						m_errorMessage->Append(maxClassName);
						m_errorMessage->Append(" characters available for the class including the text attribute)");
	
						OOSQL_ERR(eTOOLONGID_OOSQL);
					}
					if(strlen(attributeName) > (LOM_MAXCLASSNAME - 20)/2)
					{
						SimpleString maxAttrName;
						sprintf(maxAttrName, "%ld", (LOM_MAXCLASSNAME - 20)/2);
						m_errorMessage->Clear();
						m_errorMessage->Append("Attribute name too long. (Maximum ");
						m_errorMessage->Append(maxAttrName);
						m_errorMessage->Append(" characters available for the text attribute)");
	
						OOSQL_ERR(eTOOLONGID_OOSQL);
					}
				}
				if (strlen(attributeName) > LOM_MAXCLASSNAME - 20)
				{
					SimpleString maxAttrName;
					sprintf(maxAttrName, "%ld", (LOM_MAXCLASSNAME - 20));
					m_errorMessage->Clear();
					m_errorMessage->Append("Attribute name too long. (Maximum ");
					m_errorMessage->Append(maxAttrName);
					m_errorMessage->Append(" characters available)");

					OOSQL_ERR(eTOOLONGID_OOSQL);
				}



				#ifdef SLIMDOWN_TEXTIR
				if (domainName == (char*)"text")
					OOSQL_ERR(eTEXTIR_NOTENABLED_OOSQL);
				#endif

				#ifdef SLIMDOWN_OPENGIS
                if(domainName == (char*)"geometry"||
                   domainName == (char*)"point"||
                   domainName == (char*)"linestring"||
                   domainName == (char*)"polygon"||
                   domainName == (char*)"geometrycollection"||
                   domainName == (char*)"multipoint"||
                   domainName == (char*)"multilinestring"||
                   domainName == (char*)"multipolygon")
                    OOSQL_ERR(eOPENGIS_NOTENABLED_OOSQL);
                #endif

				if (dbCommand[0].createTableInfo.isTemporary && domainName == (char*)"text")
					OOSQL_ERR(eNOTENABLED_TEMPORARYTABLE_OOSQL);
				if (dbCommand[0].createTableInfo.isTemporary && (domainName == (char*)"geometry"||
							domainName == (char*)"point"||
							domainName == (char*)"linestring"||
							domainName == (char*)"polygon"||
							domainName == (char*)"geometrycollection"||
							domainName == (char*)"multipoint"||
							domainName == (char*)"multilinestring"||
							domainName == (char*)"multipolygon"))
					OOSQL_ERR(eNOTENABLED_TEMPORARYTABLE_OOSQL);

				if(sDomainNameSub != AST_NULL)
				{
					e = getStringFromID_Node(domainNameSub, sDomainNameSub);
					OOSQL_CHECK_ERR(e);
					makeLowerCaseString(domainNameSub);
				}		

				if(domainName == (char* )"set"      || domainName == (char* )"bag"      || domainName == (char* )"list"      ||
				   domainName == (char* )"odmg_set" || domainName == (char* )"odmg_bag" || domainName == (char* )"odmg_list" || 
				   domainName == (char* )"odmg_array")
				{
					attributeInfo[0].inheritedFrom = -1;
					if(domainName == (char* )"set")				
						attributeInfo[0].complexType   = LOM_COMPLEXTYPE_COLLECTIONSET;
					else if(domainName == (char* )"bag")		
						attributeInfo[0].complexType   = LOM_COMPLEXTYPE_COLLECTIONBAG;
					else if(domainName == (char* )"list")		
						attributeInfo[0].complexType   = LOM_COMPLEXTYPE_COLLECTIONLIST;
					else if(domainName == (char* )"odmg_set")
						attributeInfo[0].complexType   = LOM_COMPLEXTYPE_ODMG_COLLECTIONSET;
					else if(domainName == (char* )"odmg_bag")	
						attributeInfo[0].complexType   = LOM_COMPLEXTYPE_ODMG_COLLECTIONBAG;
					else if(domainName == (char* )"odmg_list")	
						attributeInfo[0].complexType   = LOM_COMPLEXTYPE_ODMG_COLLECTIONLIST;
					else if(domainName == (char* )"odmg_array")
						attributeInfo[0].complexType   = LOM_COMPLEXTYPE_ODMG_COLLECTIONARRAY;
					
					attributeInfo[0].domain = typeNameToTypeID(domainNameSub);
					attributeInfo[0].length = typeNameToTypeLength(domainNameSub);

					if(attributeInfo[0].domain == TYPEID_STRING || attributeInfo[0].domain == TYPEID_VARSTRING)
					{
						if(sPrecision1 == AST_NULL)
							return eSYNTAX_ERROR_OOSQL;
						else
							attributeInfo[0].length = m_pool->intPool[AST(sPrecision1).tokenVal];		
					}
					else if(attributeInfo[0].domain == TYPEID_NONE)
					{
						m_errorMessage->Clear();
						m_errorMessage->Append("Type '");
						m_errorMessage->Append((char*)domainName);
						m_errorMessage->Append("' at ");
						ERRORMESSAGE_APPEND_POSITION(m_errorMessage, sDomainName);
						m_errorMessage->Append(" is not supported type.");

						OOSQL_ERR(eNOTSUPPORTED_ATTR_TYPE_OOSQL); 
					}
				}
				else
				{
					attributeInfo[0].inheritedFrom = -1;
					attributeInfo[0].complexType   = LOM_COMPLEXTYPE_BASIC;
					attributeInfo[0].domain = typeNameToTypeID(domainName);
					attributeInfo[0].length = typeNameToTypeLength(domainName);
					attributeInfo[0].referencingClassId = -1;

					if(attributeInfo[0].domain == TYPEID_STRING || attributeInfo[0].domain == TYPEID_VARSTRING)
					{
						if(sPrecision1 == AST_NULL)
							return eSYNTAX_ERROR_OOSQL;
						else
						attributeInfo[0].length = m_pool->intPool[AST(sPrecision1).tokenVal];
					}
					else if(attributeInfo[0].domain == TYPEID_NONE)
					{
						m_errorMessage->Clear();
						m_errorMessage->Append("Type '");
						m_errorMessage->Append((char*)domainName);
						m_errorMessage->Append("' at ");
						ERRORMESSAGE_APPEND_POSITION(m_errorMessage, sDomainName);
						m_errorMessage->Append(" is not supported type.");

						OOSQL_ERR(eNOTSUPPORTED_ATTR_TYPE_OOSQL); 
					}
                    else if(attributeInfo[0].domain == TYPEID_OID)
                    {
                        ASTNodeIdx      sRefClassName = AST(sDomainName).brother;
                        StringPoolIndex refClassNamePoolIndex;
                        SimpleString    refClassName;
                        Four            refClassId;
                        CataClassInfo   refClassInfo;

						if(sRefClassName != AST_NULL)
						{
							e = getStringPoolIndexFromID_Node(refClassNamePoolIndex, sRefClassName);
							OOSQL_CHECK_ERR(e);

							e = makeStringFromStringPool(refClassName, refClassNamePoolIndex);
							OOSQL_CHECK_ERR(e);

							e = m_catalog->class_ClassName_to_CataClassInfo(refClassName, refClassInfo);
							OOSQL_CHECK_ERR(e);

							e = m_catalog->class_ClassInfo_to_ClassId(refClassInfo, refClassId);
							OOSQL_CHECK_ERR(e);
						}
						else
							refClassId = attributeInfo[0].domain;

                        attributeInfo[0].referencingClassId = refClassId;
                    }
#ifdef  SLIMDOWN_TEXTIR
                    else if(attributeInfo[0].domain == TYPEID_TEXT)
                    {
                        OOSQL_ERR(eTEXTIR_NOTENABLED_OOSQL);
                    }
#endif

				}
			}		
			sAttr = AST(sAttr).brother;
		}
	}

	sKeyDef = AST(sKeyDef).son;
	if(AST(sKeyDef).nodeName != null)
	{
		attributeInfo = dbCommand[0].createTableInfo.attributeList;
		while(sKeyDef != AST_NULL)
		{
			e = getStringFromID_Node(keyAttributeName, sKeyDef);
			OOSQL_CHECK_ERR(e);

			for(i = 0; i < attributeInfo.size; i++)
			{
				e = makeStringFromStringPool(attributeName, attributeInfo[i].attributeName);
				OOSQL_CHECK_ERR(e);

				if(keyAttributeName == attributeName)
					break;
			}

			if(i == attributeInfo.size)
			{
				m_errorMessage->Clear();
				m_errorMessage->Append("The attribute '");
				m_errorMessage->Append(keyAttributeName);
				m_errorMessage->Append("' at ");
				ERRORMESSAGE_APPEND_POSITION(m_errorMessage, sKeyDef);
				m_errorMessage->Append(" is not defined in the class '");
				m_errorMessage->Append((char*)className);
				m_errorMessage->Append("'.");

				OOSQL_ERR(eNOMATCHING_COLUMN_OOSQL); 
			}

			keyInfo = m_pool->keyInfoPool.addNewEntry();
			if(dbCommand[0].createTableInfo.keyList == NULL_POOLINDEX)
				dbCommand[0].createTableInfo.keyList = keyInfo.getPoolIndex();
			else
				dbCommand[0].createTableInfo.keyList.size ++;

			keyInfo[0].keyColNo = i + 1; 

			sKeyDef = AST(sKeyDef).brother;
		}
	}

	sAttr = AST(sAttrList).son;

	attributeInfo = dbCommand[0].createTableInfo.attributeList;
	if(AST(sAttr).nodeName != null)
	{
		while(sAttr != AST_NULL)
		{
			sAttrName   = AST(sAttr).son;
			sDomainName = AST(sAttrName).brother;
			
			sAttrName   = AST(sAttrName).son;
			sDomainName = AST(sDomainName).son;
			
			// get attribute name
			e = getStringFromID_Node(attributeName, sAttrName);
			OOSQL_CHECK_ERR(e);

			// get domain name
			e = getStringFromID_Node(domainName, sDomainName);
			OOSQL_CHECK_ERR(e);

			makeLowerCaseString(domainName);

			if(domainName == (char* )"text" && AST(sDomainName).brother != AST_NULL && AST(AST(sDomainName).brother).nodeName == QuUserFunction)
			{
				dbCommand = m_pool->dbCommandPool.addNewEntry();
				dbCommand[0].command = DBCOMMAND_DEFINE_POSTING_STRUCTURE;
				e = getStringPoolIndexFromID_Node(dbCommand[0].definePostingStructureInfo.className, sClassName);
				OOSQL_CHECK_ERR(e);
				e = getStringPoolIndexFromID_Node(dbCommand[0].definePostingStructureInfo.attributeName, sAttrName);
				OOSQL_CHECK_ERR(e);
				dbCommand[0].definePostingStructureInfo.isContainingTupleID				= SM_TRUE;
				dbCommand[0].definePostingStructureInfo.isContainingSentenceAndWordNum	= SM_TRUE;
				dbCommand[0].definePostingStructureInfo.isContainingByteOffset			= SM_FALSE;
				dbCommand[0].definePostingStructureInfo.nEmbeddedAttributes				= 0;

				sOptions = AST(sDomainName).brother;
				while(sOptions != AST_NULL)
				{
					if(AST(sOptions).nodeName != QuUserFunction)
					{
						m_errorMessage->Clear();
						m_errorMessage->Append("Text type option used at ");
						ERRORMESSAGE_APPEND_POSITION(m_errorMessage, sOptions);
						m_errorMessage->Append(" is not supported.");

						OOSQL_ERR(eBAD_TEXT_DOMAIN_OPTION_OOSQL);
					}

					sOptionName  = AST(sOptions).son;
					sOptionValues = AST(sOptionName).brother;
					e = getStringFromID_Node(optionName, sOptionName);
					OOSQL_CHECK_ERR(e);

					makeLowerCaseString(optionName);
					
					if(optionName == (char* )"contain_tupleid")	
					{
						if(AST(sOptionValues).nodeName == VaTr)
							dbCommand[0].definePostingStructureInfo.isContainingTupleID = SM_TRUE;
						else if(AST(sOptionValues).nodeName == VaFls)
							dbCommand[0].definePostingStructureInfo.isContainingTupleID = SM_FALSE;
						else
							OOSQL_ERR(eINTERNALERROR_OOSQL);
					}
					else if(optionName == (char* )"contain_sentence_word_offset")	
					{
						if(AST(sOptionValues).nodeName == VaTr)
							dbCommand[0].definePostingStructureInfo.isContainingSentenceAndWordNum = SM_TRUE;
						else if(AST(sOptionValues).nodeName == VaFls)
							dbCommand[0].definePostingStructureInfo.isContainingSentenceAndWordNum = SM_FALSE;
						else
							OOSQL_ERR(eINTERNALERROR_OOSQL);
					}
					else if(optionName == (char* )"contain_byte_offset")	
					{
						if(AST(sOptionValues).nodeName == VaTr)
							dbCommand[0].definePostingStructureInfo.isContainingByteOffset = SM_TRUE;
						else if(AST(sOptionValues).nodeName == VaFls)
							dbCommand[0].definePostingStructureInfo.isContainingByteOffset = SM_FALSE;
						else
							OOSQL_ERR(eINTERNALERROR_OOSQL);
					}
					else if(optionName == (char* )"embedded_attributes")	
					{
						while(sOptionValues != AST_NULL)
						{
							e = getStringFromID_Node(optionValue, sOptionValues);
							OOSQL_CHECK_ERR(e);

							for(i = 0; i < attributeInfo.size; i ++)
							{
								e = makeStringFromStringPool(attributeName, attributeInfo[i].attributeName);
								OOSQL_CHECK_ERR(e);

								if(optionValue == attributeName)
								{	
									embeddedAttributeNo = i;
									break;
								}
							}
							if(i == attributeInfo.size)
							{
								m_errorMessage->Clear();
								m_errorMessage->Append("Text type option used at ");
								ERRORMESSAGE_APPEND_POSITION(m_errorMessage, sOptionValues);
								m_errorMessage->Append(" is not supported.");

								OOSQL_ERR(eBAD_TEXT_DOMAIN_OPTION_OOSQL);	
							}

							if(attributeInfo[embeddedAttributeNo].domain == TYPEID_TEXT)
							{
								m_errorMessage->Clear();
								m_errorMessage->Append("Text type option used at ");
								ERRORMESSAGE_APPEND_POSITION(m_errorMessage, sOptionValues);
								m_errorMessage->Append(" is not supported.");

								OOSQL_ERR(eBAD_TEXT_DOMAIN_OPTION_OOSQL);	
							}

							dbCommand[0].definePostingStructureInfo.embeddedAttrNo[dbCommand[0].definePostingStructureInfo.nEmbeddedAttributes] = (short)embeddedAttributeNo;
							dbCommand[0].definePostingStructureInfo.nEmbeddedAttributes ++;

							sOptionValues = AST(sOptionValues).brother;
						}
                        Two     newEmbeddedAttrNo[MAX_NUM_EMBEDDEDATTRIBUTES];
                        Four    index = 0;
                        for(i = 0; i < dbCommand[0].definePostingStructureInfo.nEmbeddedAttributes; i++)
                        {
                            embeddedAttributeNo = dbCommand[0].definePostingStructureInfo.embeddedAttrNo[i];
                            if(attributeInfo[embeddedAttributeNo].domain != TYPEID_VARSTRING)
                            {
                                newEmbeddedAttrNo[index] = embeddedAttributeNo;
                                index ++;
                            }
                        }
                        for(i = 0; i < dbCommand[0].definePostingStructureInfo.nEmbeddedAttributes; i++)
                        {
                            embeddedAttributeNo = dbCommand[0].definePostingStructureInfo.embeddedAttrNo[i];
                            if(attributeInfo[embeddedAttributeNo].domain == TYPEID_VARSTRING)
                            {
                                newEmbeddedAttrNo[index] = embeddedAttributeNo;
                                index ++;
                            }
                        }
                        memcpy(dbCommand[0].definePostingStructureInfo.embeddedAttrNo, newEmbeddedAttrNo, sizeof(newEmbeddedAttrNo));
					}
					else
					{
						m_errorMessage->Clear();
						m_errorMessage->Append("Text type option used at ");
						ERRORMESSAGE_APPEND_POSITION(m_errorMessage, sOptionName);
						m_errorMessage->Append(" is not supported.");

						OOSQL_ERR(eBAD_TEXT_DOMAIN_OPTION_OOSQL);
					}

					sOptions = AST(sOptions).brother;
				}
			}
			sAttr = AST(sAttr).brother;
		}
	}

	return eNOERROR;
}

Four OQL_ASTtoGDS::smtChkQuCreateIdx(ASTNodeIdx node, ReturnType& returnType)
{
	ASTNodeIdx					sIndexType, sIndexName, sClassName, sKeyList;
	ASTNodeIdx					sKeyAttr;
	CataClassInfo				classInfo;
	CataAttrInfo				attrInfo;
	SimpleString				keyAttributeName;
	SimpleString				className;
	DBCommandPoolElements		dbCommand(m_pool->dbCommandPool);
	AttributeInfoPoolElements	attributeInfo(m_pool->attributeInfoPool);
	KeyInfoPoolElements			keyInfo(m_pool->keyInfoPool);
	Four						e;
	Two							colNo;
    TypeID                      colTypeID;
    Four                        colLength;
    Four                        totalKeyLength;

	e = m_gds->init(m_pool, OQL_GDS::NO_QUERY);
	OOSQL_CHECK_ERR(e);

	// create new DBCommand
	dbCommand = m_pool->dbCommandPool.addNewEntry();
	dbCommand[0].command = DBCOMMAND_CREATE_INDEX;
	dbCommand[0].createIndexInfo.keyList.setNull();

	// traverse AST
	sIndexType = AST(node).son;
	sIndexName = AST(sIndexType).brother;
	sClassName = AST(sIndexName).brother;
	sKeyList   = AST(sClassName).brother;

	// determine index type
	switch(AST(sIndexType).nodeName)
	{
	case IdxTypeNormal:
		dbCommand[0].createIndexInfo.isUnique     = SM_FALSE;
		dbCommand[0].createIndexInfo.isClustering = SM_FALSE;
		dbCommand[0].createIndexInfo.indexType = INDEXTYPE_BTREE;
		break;

	case IdxTypeUnique:
		dbCommand[0].createIndexInfo.isUnique     = SM_TRUE;
		dbCommand[0].createIndexInfo.isClustering = SM_FALSE;
		dbCommand[0].createIndexInfo.indexType = INDEXTYPE_BTREE;
		break;

	case IdxTypeCluster:
		dbCommand[0].createIndexInfo.isUnique     = SM_FALSE;
		dbCommand[0].createIndexInfo.isClustering = SM_TRUE;
		dbCommand[0].createIndexInfo.indexType = INDEXTYPE_BTREE;
		break;

	case IdxTypeUniqueCluster:
		dbCommand[0].createIndexInfo.isUnique     = SM_TRUE;
		dbCommand[0].createIndexInfo.isClustering = SM_TRUE;
		dbCommand[0].createIndexInfo.indexType	= INDEXTYPE_BTREE;
		break;
	case IdxTypeMlgf:
		dbCommand[0].createIndexInfo.isUnique     = SM_FALSE;
		dbCommand[0].createIndexInfo.isClustering = SM_FALSE;
		dbCommand[0].createIndexInfo.indexType = INDEXTYPE_MLGF;
		break;

	case IdxTypeUniqueMlgf:
		dbCommand[0].createIndexInfo.isUnique     = SM_TRUE;
		dbCommand[0].createIndexInfo.isClustering = SM_FALSE;
		dbCommand[0].createIndexInfo.indexType = INDEXTYPE_MLGF;
		break;
	
	case IdxTypeClusterMlgf:
		dbCommand[0].createIndexInfo.isUnique     = SM_FALSE;
		dbCommand[0].createIndexInfo.isClustering = SM_TRUE;
		dbCommand[0].createIndexInfo.indexType = INDEXTYPE_MLGF;
		break;
	
	case IdxTypeUniqueClusterMlgf:
		dbCommand[0].createIndexInfo.isUnique     = SM_TRUE;
		dbCommand[0].createIndexInfo.isClustering = SM_TRUE;
		dbCommand[0].createIndexInfo.indexType = INDEXTYPE_MLGF;
		break;

	default:
		OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
	}

	// get index name
	e = getStringPoolIndexFromID_Node(dbCommand[0].createIndexInfo.indexName, sIndexName);
	OOSQL_CHECK_ERR(e);

	SimpleString indexName;
	e = makeStringFromStringPool(indexName, dbCommand[0].createIndexInfo.indexName);
	OOSQL_CHECK_ERR(e);

	if (strlen(indexName) > LOM_MAXINDEXNAME - 1) // null termination
	{
		SimpleString maxIndexName;
		sprintf(maxIndexName, "%ld", LOM_MAXINDEXNAME -1);
		m_errorMessage->Clear();
		m_errorMessage->Append("Index name too long. (Maximum ");
		m_errorMessage->Append(maxIndexName);
		m_errorMessage->Append(" characters available)");

		OOSQL_ERR(eTOOLONGID_OOSQL);
	}


	// get class name
	e = getStringPoolIndexFromID_Node(dbCommand[0].createIndexInfo.className, sClassName);
	OOSQL_CHECK_ERR(e);

	e = makeStringFromStringPool(className, dbCommand[0].createIndexInfo.className);
	OOSQL_CHECK_ERR(e);

	e = m_catalog->class_ClassName_to_CataClassInfo(className,classInfo);
	OOSQL_CHECK_ERR(e);

	// get keylist
    totalKeyLength = 0;

	Four numGeometryColumn = 0;
	Four numTotalColumn = 0;

	sKeyAttr = AST(sKeyList).son;
	while(sKeyAttr != AST_NULL)
	{
		keyInfo = m_pool->keyInfoPool.addNewEntry();
		if(dbCommand[0].createIndexInfo.keyList == NULL_POOLINDEX)
			dbCommand[0].createIndexInfo.keyList = keyInfo.getPoolIndex();
		else
			dbCommand[0].createIndexInfo.keyList.size ++;

		e = getStringFromID_Node(keyAttributeName, sKeyAttr);
		OOSQL_CHECK_ERR(e);

		e = m_catalog->attr_AttrName_to_CataAttrInfo(classInfo, keyAttributeName, attrInfo);
		if(e == eNOT_FOUND_OOSQL)
		{
			char classname[MAXCLASSNAME];

			e = m_catalog->class_CataClassInfo_to_ClassName(classInfo, classname);
			OOSQL_CHECK_ERR(e);

			m_errorMessage->Clear();
			m_errorMessage->Append("The attribute '");
			m_errorMessage->Append(keyAttributeName);
			m_errorMessage->Append("' is not defined in the class '");
			m_errorMessage->Append(classname);
			m_errorMessage->Append("'.");

			return eATTRNOTDEFINED_OOSQL;
		}
		else
			OOSQL_CHECK_ERR(e);

		e = m_catalog->attr_AttrInfo_to_ColNo(classInfo, attrInfo, colNo);
		OOSQL_CHECK_ERR(e);

        e = m_catalog->attr_GetLength(classInfo, attrInfo, colLength);
		OOSQL_CHECK_ERR(e);

        e = m_catalog->attr_GetTypeID(classInfo, attrInfo, colTypeID);
		OOSQL_CHECK_ERR(e);

		numTotalColumn ++;

        totalKeyLength += colLength;
        if (colTypeID == TYPEID_VARSTRING)
            totalKeyLength += 2;

		else if (isOGISTypeID(colTypeID) == SM_TRUE)
		{
			numGeometryColumn ++;
		}
		
		if(dbCommand[0].createIndexInfo.indexType == INDEXTYPE_MLGF && 
		   !(isOGISTypeID(colTypeID) || colTypeID == TYPEID_SHORT || colTypeID == TYPEID_INT || colTypeID == TYPEID_LONG || colTypeID == TYPEID_LONG_LONG))
			OOSQL_ERR(eTYPE_ERROR_OOSQL);

        keyInfo[0].keyColNo = colNo;

		sKeyAttr = AST(sKeyAttr).brother;
	}
	if(numGeometryColumn == 1 && numTotalColumn == 1)
	{
	// If one geometery column exists, we force index type to MLGF.
		dbCommand[0].createIndexInfo.indexType = INDEXTYPE_MLGF;
	}
	else if(numGeometryColumn == 1 && numTotalColumn > 1)
	{
	// Combination between non-geometry and geometry column is not implemented.
		return eNOTIMPLEMENTED_OOSQL;
	}
	else if(numGeometryColumn > 1)
	{
	// Combined index of more than one more geometry column is not implemented.
		return eNOTIMPLEMENTED_OOSQL;
	}

	if (dbCommand[0].createIndexInfo.indexType == INDEXTYPE_BTREE)
    	if (totalKeyLength > MAXKEYLEN)
		    OOSQL_ERR(eBTREE_KEYLENGTH_EXCESS_OOSQL);

	if (dbCommand[0].createIndexInfo.indexType == INDEXTYPE_BTREE)
		if (numTotalColumn > MAXNUMKEYPARTS - 1) // avoid buggy situation, decrement 1. should be solved.
		{
			SimpleString maxNumKeyParts;
			sprintf(maxNumKeyParts, "%ld", MAXNUMKEYPARTS - 1);
			m_errorMessage->Clear();
			m_errorMessage->Append("Too many number of key attributes in an index. (Maximum ");
			m_errorMessage->Append(maxNumKeyParts);
			m_errorMessage->Append(" attributes available)");

			OOSQL_ERR(eTOOMANYATTRINDEX_OOSQL);
		}

	if (dbCommand[0].createIndexInfo.indexType == INDEXTYPE_MLGF)
		if (numTotalColumn > MLGF_MAXNUM_KEYS - 1 - 2) // -2 temporary. fix it!
		{
			SimpleString maxNumKeyParts;
			sprintf(maxNumKeyParts, "%ld", MLGF_MAXNUM_KEYS - 1 - 2);
			m_errorMessage->Clear();
			m_errorMessage->Append("Too many number of key attributes in an index. (Maximum ");
			m_errorMessage->Append(maxNumKeyParts);
			m_errorMessage->Append(" attributes available)");

			OOSQL_ERR(eTOOMANYATTRINDEX_OOSQL);
		}

	return eNOERROR;
}

Four OQL_ASTtoGDS::smtChkQuDropTbl(ASTNodeIdx node, ReturnType& returnType)
{
	ASTNodeIdx					sClassName;
	DBCommandPoolElements		dbCommand(m_pool->dbCommandPool);
	SimpleString                classname;
	CataClassInfo				classInfo;
	Four						e;

	e = m_gds->init(m_pool, OQL_GDS::NO_QUERY);
	OOSQL_CHECK_ERR(e);

	// create new DBCommand
	dbCommand = m_pool->dbCommandPool.addNewEntry();
	dbCommand[0].command = DBCOMMAND_DROP_TABLE;

	// get class name
	sClassName = AST(node).son;
	e = getStringPoolIndexFromID_Node(dbCommand[0].dropTableInfo.className, sClassName);
	OOSQL_CHECK_ERR(e);

    e = getStringFromID_Node(classname, sClassName);
    OOSQL_CHECK_ERR(e);

	e = m_catalog->class_ClassName_to_CataClassInfo(classname, classInfo);
    if(e == eNOT_FOUND_OOSQL)
    {
        m_errorMessage->Clear();
		m_errorMessage->Append("The class '");
		m_errorMessage->Append((char*)classname);
		m_errorMessage->Append("' is not defined in the database.");

        return eCLASSNOTDEFINED_OOSQL;
    }
	else
		OOSQL_CHECK_ERR(e);

	return eNOERROR;
}

Four OQL_ASTtoGDS::smtChkQuDropIdx(ASTNodeIdx node, ReturnType& returnType)
{
	ASTNodeIdx					sIndexName;
	DBCommandPoolElements		dbCommand(m_pool->dbCommandPool);
	Four						e;

	e = m_gds->init(m_pool, OQL_GDS::NO_QUERY);
	OOSQL_CHECK_ERR(e);

	// create new DBCommand
	dbCommand = m_pool->dbCommandPool.addNewEntry();
	dbCommand[0].command = DBCOMMAND_DROP_INDEX;

	// get index name
	sIndexName = AST(node).son;
	e = getStringPoolIndexFromID_Node(dbCommand[0].dropIndexInfo.indexName, sIndexName);
	OOSQL_CHECK_ERR(e);


	return eNOERROR;
}

Four OQL_ASTtoGDS::smtChkQuCreateFunc(ASTNodeIdx node, ReturnType& returnType)
{
	ASTNodeIdx sFuncName;
	ASTNodeIdx sFuncArgList, sFuncArg, sFuncArgType, sFuncArgLocactorFlag;
	ASTNodeIdx sFuncReturns;
	ASTNodeIdx sFuncSpecific;
	ASTNodeIdx sFuncExternalName;
	ASTNodeIdx sFuncDeterministic;
	ASTNodeIdx sFuncExternalAction;
	ASTNodeIdx sFuncFenced;
	ASTNodeIdx sFuncNullCall;
	ASTNodeIdx sFuncLanguage;
	ASTNodeIdx sFuncParameterStyle;
	ASTNodeIdx sFuncScratchpad;
	ASTNodeIdx sFuncFinalCall;
	ASTNodeIdx sFuncParallel;
	ASTNodeIdx sFuncDbinfo;
	SimpleString language, parameterStyle, argumentTypeName;
	DBCommandPoolElements		dbCommand(m_pool->dbCommandPool);
	ArgumentTypePoolElements	argType(m_pool->argumentTypePool);
	Four						e;

	// check m_ast node structur and assign its index to local vars.
	sFuncName = AST(node).son;
	if(sFuncName == AST_NULL) OOSQL_ERR(eBADASTNODE_OOSQL);
	
	sFuncArgList = AST(sFuncName).brother;
	if(sFuncArgList == AST_NULL) OOSQL_ERR(eBADASTNODE_OOSQL);
	if(AST(sFuncArgList).nodeName != FuncArgList) OOSQL_ERR(eBADASTNODE_OOSQL);
	
	sFuncReturns = AST(sFuncArgList).brother;
	if(sFuncReturns == AST_NULL) OOSQL_ERR(eBADASTNODE_OOSQL);
	if(AST(sFuncReturns).nodeName != FuncReturns) OOSQL_ERR(eBADASTNODE_OOSQL);
	
	sFuncSpecific = AST(sFuncReturns).brother;
	if(sFuncSpecific == AST_NULL) OOSQL_ERR(eBADASTNODE_OOSQL);
	if(AST(sFuncSpecific).nodeName != FuncSpecific) OOSQL_ERR(eBADASTNODE_OOSQL);
	
	sFuncExternalName = AST(sFuncSpecific).brother;
	if(sFuncExternalName == AST_NULL) OOSQL_ERR(eBADASTNODE_OOSQL);
	if(AST(sFuncExternalName).nodeName != FuncExternalName) OOSQL_ERR(eBADASTNODE_OOSQL);
	
	sFuncDeterministic = AST(sFuncExternalName).brother;
	if(sFuncDeterministic == AST_NULL) OOSQL_ERR(eBADASTNODE_OOSQL);
	if(AST(sFuncDeterministic).nodeName != FuncDeterministic) OOSQL_ERR(eBADASTNODE_OOSQL);
	
	sFuncExternalAction = AST(sFuncDeterministic).brother;
	if(sFuncExternalAction == AST_NULL) OOSQL_ERR(eBADASTNODE_OOSQL);
	if(AST(sFuncExternalAction).nodeName != FuncExternalAction) OOSQL_ERR(eBADASTNODE_OOSQL);
	
	sFuncFenced = AST(sFuncExternalAction).brother;
	if(sFuncFenced == AST_NULL) OOSQL_ERR(eBADASTNODE_OOSQL);
	if(AST(sFuncFenced).nodeName != FuncFenced) OOSQL_ERR(eBADASTNODE_OOSQL);
	
	sFuncNullCall = AST(sFuncFenced).brother;
	if(sFuncNullCall == AST_NULL) OOSQL_ERR(eBADASTNODE_OOSQL);
	if(AST(sFuncNullCall).nodeName != FuncNullCall) OOSQL_ERR(eBADASTNODE_OOSQL);
	
	sFuncLanguage = AST(sFuncNullCall).brother;
	if(sFuncLanguage == AST_NULL) OOSQL_ERR(eBADASTNODE_OOSQL);
	if(AST(sFuncLanguage).nodeName != FuncLanguage) OOSQL_ERR(eBADASTNODE_OOSQL);
	
	sFuncParameterStyle = AST(sFuncLanguage).brother;
	if(sFuncParameterStyle == AST_NULL) OOSQL_ERR(eBADASTNODE_OOSQL);
	if(AST(sFuncParameterStyle).nodeName != FuncParameterStyle) OOSQL_ERR(eBADASTNODE_OOSQL);
	
	sFuncScratchpad = AST(sFuncParameterStyle).brother;
	if(sFuncScratchpad == AST_NULL) OOSQL_ERR(eBADASTNODE_OOSQL);
	if(AST(sFuncScratchpad).nodeName != FuncScratchpad) OOSQL_ERR(eBADASTNODE_OOSQL);
	
	sFuncFinalCall = AST(sFuncScratchpad).brother;
	if(sFuncFinalCall == AST_NULL) OOSQL_ERR(eBADASTNODE_OOSQL);
	if(AST(sFuncFinalCall).nodeName != FuncFinalCall) OOSQL_ERR(eBADASTNODE_OOSQL);
	
	sFuncParallel = AST(sFuncFinalCall).brother;
	if(sFuncParallel == AST_NULL) OOSQL_ERR(eBADASTNODE_OOSQL);
	if(AST(sFuncParallel).nodeName != FuncParallel) OOSQL_ERR(eBADASTNODE_OOSQL);
	
	sFuncDbinfo = AST(sFuncParallel).brother;
	if(sFuncDbinfo == AST_NULL) OOSQL_ERR(eBADASTNODE_OOSQL);
	if(AST(sFuncDbinfo).nodeName != FuncDbinfo) OOSQL_ERR(eBADASTNODE_OOSQL);

	e = m_gds->init(m_pool, OQL_GDS::NO_QUERY);
	OOSQL_CHECK_ERR(e);

	// create new DBCommand
	dbCommand = m_pool->dbCommandPool.addNewEntry();
	dbCommand[0].command = DBCOMMAND_CREATE_FUNCTION;

	// function name
	e = getStringPoolIndexFromID_Node(dbCommand[0].createFunctionInfo.functionName, sFuncName);
	OOSQL_CHECK_ERR(e);

	// argument list
	sFuncArg = AST(sFuncArgList).son;
	dbCommand[0].createFunctionInfo.argumentTypeList.setNull();
	while(sFuncArg != AST_NULL)
	{
		if(AST(sFuncArg).nodeName != FuncArg)
			OOSQL_ERR(eBADASTNODE_OOSQL);

		argType = m_pool->argumentTypePool.addNewEntry();
		if(dbCommand[0].createFunctionInfo.argumentTypeList == NULL_POOLINDEX)
			dbCommand[0].createFunctionInfo.argumentTypeList = argType.getPoolIndex();
		else
			dbCommand[0].createFunctionInfo.argumentTypeList.size ++;

		sFuncArgType = AST(sFuncArg).son;
		sFuncArgLocactorFlag = AST(sFuncArgType).brother;

		argType[0].argumentName.setNull();

		e = getStringFromID_Node(argumentTypeName, sFuncArgType);
		OOSQL_CHECK_ERR(e);

		makeLowerCaseString(argumentTypeName);

		argType[0].argumentType = typeNameToTypeID(argumentTypeName);
		if(argType[0].argumentType == TYPEID_NONE)
			OOSQL_ERR(eNOTSUPPORTED_ATTR_TYPE_OOSQL);

		e = getStringPoolIndexFromID_Node(argType[0].argumentTypeName, sFuncArgType);
		OOSQL_CHECK_ERR(e);

		argType[0].argumentTypeLength = typeNameToTypeLength(argumentTypeName);

		argType[0].parameterMode = PARM_IN;

		if(sFuncArgLocactorFlag != AST_NULL && AST(sFuncArgLocactorFlag).nodeName == FucnArgAsLocator)
			argType[0].asLocator = SM_TRUE;
		else
			argType[0].asLocator = SM_FALSE;

		sFuncArg = AST(sFuncArg).brother;
	}

	// function returns
	sFuncArgType = AST(AST(sFuncReturns).son).son;
	sFuncArgLocactorFlag = AST(sFuncArgType).brother;

	dbCommand[0].createFunctionInfo.returnType.argumentName.setNull();
	e = getStringFromID_Node(argumentTypeName, sFuncArgType);
	OOSQL_CHECK_ERR(e);
	makeLowerCaseString(argumentTypeName);
	dbCommand[0].createFunctionInfo.returnType.argumentType = typeNameToTypeID(argumentTypeName);
	if(dbCommand[0].createFunctionInfo.returnType.argumentType == TYPEID_NONE)
		OOSQL_ERR(eNOTSUPPORTED_ATTR_TYPE_OOSQL);
	dbCommand[0].createFunctionInfo.returnType.parameterMode = PARM_OUT;
	if(sFuncArgLocactorFlag != AST_NULL && AST(sFuncArgLocactorFlag).nodeName == FucnArgAsLocator)
		dbCommand[0].createFunctionInfo.returnType.asLocator = SM_TRUE;
	else
		dbCommand[0].createFunctionInfo.returnType.asLocator = SM_FALSE;

	// specific
	e = getStringPoolIndexFromID_Node(dbCommand[0].createFunctionInfo.specificName, AST(sFuncSpecific).son);
	OOSQL_CHECK_ERR(e);

	// external name
	e = getStringPoolIndexFromSTRING_Node(dbCommand[0].createFunctionInfo.externalName, AST(sFuncExternalName).son);
	OOSQL_CHECK_ERR(e);

	// deterministic
	if(AST(AST(sFuncDeterministic).son).nodeName == VaTr)
		dbCommand[0].createFunctionInfo.deterministic = SM_TRUE;
	else
		dbCommand[0].createFunctionInfo.deterministic = SM_FALSE;

	// external action
	if(AST(AST(sFuncExternalAction).son).nodeName == VaTr)
		dbCommand[0].createFunctionInfo.externalAction = SM_TRUE;
	else
		dbCommand[0].createFunctionInfo.externalAction = SM_FALSE;

	// fenced
	if(AST(AST(sFuncFenced).son).nodeName == VaTr)
		dbCommand[0].createFunctionInfo.fenced = SM_TRUE;
	else
		dbCommand[0].createFunctionInfo.fenced = SM_FALSE;

	// null call
	if(AST(AST(sFuncFenced).son).nodeName == VaTr)
		dbCommand[0].createFunctionInfo.nullCall = SM_TRUE;
	else
		dbCommand[0].createFunctionInfo.nullCall = SM_FALSE;

	// language
	// check language is if C, JAVA
	e = getStringPoolIndexFromID_Node(dbCommand[0].createFunctionInfo.language, AST(sFuncLanguage).son);
	OOSQL_CHECK_ERR(e);
	
	e = makeStringFromStringPool(language, dbCommand[0].createFunctionInfo.language);
	OOSQL_CHECK_ERR(e);

	makeLowerCaseString(language);

	if(!(language == (char* )"c" || language == (char* )"java"))	
		OOSQL_ERR(eUNSUPPORTED_LANGUAGE_OOSQL);
	
	// parameter style
	// check laguage is if OOSQL
	e = getStringPoolIndexFromID_Node(dbCommand[0].createFunctionInfo.parameterStyle, AST(sFuncParameterStyle).son);
	OOSQL_CHECK_ERR(e);
	
	e = makeStringFromStringPool(parameterStyle, dbCommand[0].createFunctionInfo.parameterStyle);
	OOSQL_CHECK_ERR(e);

	makeLowerCaseString(parameterStyle);

	if(!(parameterStyle == (char* )"oosql"))	
		OOSQL_ERR(eUNSUPPORTED_PARAMETER_STYLE_OOSQL);

	// scratchpad
	if(AST(AST(sFuncScratchpad).son).nodeName == VaTr)
		dbCommand[0].createFunctionInfo.scratchPad = SM_TRUE;
	else
		dbCommand[0].createFunctionInfo.scratchPad = SM_FALSE;

	// final call
	if(AST(AST(sFuncFinalCall).son).nodeName == VaTr)
		dbCommand[0].createFunctionInfo.finalCall = SM_TRUE;
	else
		dbCommand[0].createFunctionInfo.finalCall = SM_FALSE;

	// parallel
	if(AST(AST(sFuncParallel).son).nodeName == VaTr)
		dbCommand[0].createFunctionInfo.allowParallel = SM_TRUE;
	else
		dbCommand[0].createFunctionInfo.allowParallel = SM_FALSE;

	// dbinfo
	if(AST(AST(sFuncDbinfo).son).nodeName == VaTr)
		dbCommand[0].createFunctionInfo.dbInfo = SM_TRUE;
	else
		dbCommand[0].createFunctionInfo.dbInfo = SM_FALSE;

	return eNOERROR;
}

Four OQL_ASTtoGDS::smtChkQuDropFunc(ASTNodeIdx node, ReturnType& returnType)
{
	DBCommandPoolElements		dbCommand(m_pool->dbCommandPool);
	Four						e;
	ASTNodeIdx					sFuncName;
	ASTNodeIdx					sFuncArgList, sFuncArg, sFuncArgType, sFuncArgLocactorFlag;
	ArgumentTypePoolElements	argType(m_pool->argumentTypePool);
	SimpleString				argumentTypeName;

	// check m_ast node structur and assign its index to local vars.
	sFuncName = AST(node).son;
	if(sFuncName == AST_NULL) OOSQL_ERR(eBADASTNODE_OOSQL);
	
	sFuncArgList = AST(sFuncName).brother;
	if(sFuncArgList != AST_NULL && AST(sFuncArgList).nodeName != FuncArgList) OOSQL_ERR(eBADASTNODE_OOSQL);

	e = m_gds->init(m_pool, OQL_GDS::NO_QUERY);
	OOSQL_CHECK_ERR(e);

	// create new DBCommand
	dbCommand = m_pool->dbCommandPool.addNewEntry();
	dbCommand[0].command = DBCOMMAND_DROP_FUNCTION;

	// function name
	e = getStringPoolIndexFromID_Node(dbCommand[0].dropFunctionInfo.functionName, sFuncName);
	OOSQL_CHECK_ERR(e);

	if(AST(node).nodeName == QuDropSpecFunc)
		dbCommand[0].dropFunctionInfo.specific = SM_TRUE;
	else
		dbCommand[0].dropFunctionInfo.specific = SM_FALSE;

	// argument list
	dbCommand[0].dropFunctionInfo.argumentTypeList.setNull();
	if(!dbCommand[0].dropFunctionInfo.specific && sFuncArgList != AST_NULL && 
		AST(sFuncArgList).nodeName == FuncArgList)
	{
		// argument list
		sFuncArg = AST(sFuncArgList).son;
		while(sFuncArg != AST_NULL)
		{
			if(AST(sFuncArg).nodeName != FuncArg)
				OOSQL_ERR(eBADASTNODE_OOSQL);

			argType = m_pool->argumentTypePool.addNewEntry();
			if(dbCommand[0].dropFunctionInfo.argumentTypeList == NULL_POOLINDEX)
				dbCommand[0].dropFunctionInfo.argumentTypeList = argType.getPoolIndex();
			else
				dbCommand[0].dropFunctionInfo.argumentTypeList.size ++;

			sFuncArgType = AST(sFuncArg).son;
			sFuncArgLocactorFlag = AST(sFuncArgType).brother;

			argType[0].argumentName.setNull();

			e = getStringFromID_Node(argumentTypeName, sFuncArgType);
			OOSQL_CHECK_ERR(e);

			makeLowerCaseString(argumentTypeName);

			argType[0].argumentType = typeNameToTypeID(argumentTypeName);
			if(argType[0].argumentType == TYPEID_NONE)
				OOSQL_ERR(eNOTSUPPORTED_ATTR_TYPE_OOSQL);

			argType[0].parameterMode = PARM_IN;

			if(sFuncArgLocactorFlag != AST_NULL && AST(sFuncArgLocactorFlag).nodeName == FucnArgAsLocator)
				argType[0].asLocator = SM_TRUE;
			else
				argType[0].asLocator = SM_FALSE;

			sFuncArg = AST(sFuncArg).brother;
		}
	}

	return eNOERROR;
}

Four OQL_ASTtoGDS::smtChkQuCreateProc(ASTNodeIdx node, ReturnType& returnType)
{
	ASTNodeIdx					sProcName;
	ASTNodeIdx					sProcArgList, sProcArg, sProcArgInOutType, sProcArgName, sProcArgType;
	ASTNodeIdx					sProcSpecific;
	ASTNodeIdx					sProcResultSet;
	ASTNodeIdx					sProcExternalName;
	ASTNodeIdx					sProcLanguage;
	ASTNodeIdx					sProcNullCall;
	ASTNodeIdx					sProcParameterStyle;
	ASTNodeIdx					sProcDeterministic;
	ASTNodeIdx					sProcFenced;
	SimpleString				language, parameterStyle, argumentTypeName;
	DBCommandPoolElements		dbCommand(m_pool->dbCommandPool);
	ArgumentTypePoolElements	argType(m_pool->argumentTypePool);
	Four						e;

	// check m_ast node structur and assign its index to local vars.
	sProcName = AST(node).son;
	if(sProcName == AST_NULL) OOSQL_ERR(eBADASTNODE_OOSQL);
	
	sProcArgList = AST(sProcName).brother;
	if(sProcArgList == AST_NULL) OOSQL_ERR(eBADASTNODE_OOSQL);
	if(AST(sProcArgList).nodeName != ProcArgList) OOSQL_ERR(eBADASTNODE_OOSQL);
	
	sProcSpecific = AST(sProcArgList).brother;
	if(sProcSpecific == AST_NULL) OOSQL_ERR(eBADASTNODE_OOSQL);
	if(AST(sProcSpecific).nodeName != FuncSpecific) OOSQL_ERR(eBADASTNODE_OOSQL);

	sProcResultSet = AST(sProcSpecific).brother;
	if(sProcResultSet == AST_NULL) OOSQL_ERR(eBADASTNODE_OOSQL);
	if(AST(sProcResultSet).nodeName != ProcResultSet) OOSQL_ERR(eBADASTNODE_OOSQL);

	sProcExternalName = AST(sProcResultSet).brother;
	if(sProcExternalName == AST_NULL) OOSQL_ERR(eBADASTNODE_OOSQL);
	if(AST(sProcExternalName).nodeName != FuncExternalName) OOSQL_ERR(eBADASTNODE_OOSQL);
	
	sProcLanguage = AST(sProcExternalName).brother;
	if(sProcLanguage == AST_NULL) OOSQL_ERR(eBADASTNODE_OOSQL);
	if(AST(sProcLanguage).nodeName != FuncLanguage) OOSQL_ERR(eBADASTNODE_OOSQL);
	
	sProcNullCall = AST(sProcLanguage).brother;
	if(sProcNullCall == AST_NULL) OOSQL_ERR(eBADASTNODE_OOSQL);
	if(AST(sProcNullCall).nodeName != FuncNullCall) OOSQL_ERR(eBADASTNODE_OOSQL);

	sProcParameterStyle = AST(sProcNullCall).brother;
	if(sProcParameterStyle == AST_NULL) OOSQL_ERR(eBADASTNODE_OOSQL);
	if(AST(sProcParameterStyle).nodeName != FuncParameterStyle) OOSQL_ERR(eBADASTNODE_OOSQL);
	
	sProcDeterministic = AST(sProcParameterStyle).brother;
	if(sProcDeterministic == AST_NULL) OOSQL_ERR(eBADASTNODE_OOSQL);
	if(AST(sProcDeterministic).nodeName != FuncDeterministic) OOSQL_ERR(eBADASTNODE_OOSQL);
	
	sProcFenced = AST(sProcDeterministic).brother;
	if(sProcFenced == AST_NULL) OOSQL_ERR(eBADASTNODE_OOSQL);
	if(AST(sProcFenced).nodeName != FuncFenced) OOSQL_ERR(eBADASTNODE_OOSQL);
	
	e = m_gds->init(m_pool, OQL_GDS::NO_QUERY);
	OOSQL_CHECK_ERR(e);

	// create new DBCommand
	dbCommand = m_pool->dbCommandPool.addNewEntry();
	dbCommand[0].command = DBCOMMAND_CREATE_PROCEDURE;

	// procedure name
	e = getStringPoolIndexFromID_Node(dbCommand[0].createProcedureInfo.procedureName, sProcName);
	OOSQL_CHECK_ERR(e);

	// argument list
	sProcArg = AST(sProcArgList).son;
	dbCommand[0].createProcedureInfo.argumentTypeList.setNull();
	while(sProcArg != AST_NULL)
	{
		if(AST(sProcArg).nodeName != ProcArg)
			OOSQL_ERR(eBADASTNODE_OOSQL);

		argType = m_pool->argumentTypePool.addNewEntry();
		if(dbCommand[0].createProcedureInfo.argumentTypeList == NULL_POOLINDEX)
			dbCommand[0].createProcedureInfo.argumentTypeList = argType.getPoolIndex();
		else
			dbCommand[0].createProcedureInfo.argumentTypeList.size ++;

		sProcArgInOutType = AST(sProcArg).son;
		sProcArgName = AST(sProcArgInOutType).brother;
		sProcArgType = AST(sProcArgName).brother;

		if(AST(sProcArgInOutType).nodeName == ProcArgModeIn)
			argType[0].parameterMode = PARM_IN;
		else if(AST(sProcArgInOutType).nodeName == ProcArgModeOut)
			argType[0].parameterMode = PARM_OUT;
		else if(AST(sProcArgInOutType).nodeName == ProcArgModeInOut)
			argType[0].parameterMode = PARM_INOUT;
		else
			OOSQL_ERR(eBADASTNODE_OOSQL);

		e = getStringPoolIndexFromID_Node(argType[0].argumentName, sProcArgName);
		OOSQL_CHECK_ERR(e);

		e = getStringFromID_Node(argumentTypeName, sProcArgType);
		OOSQL_CHECK_ERR(e);

		makeLowerCaseString(argumentTypeName);

		argType[0].argumentType = typeNameToTypeID(argumentTypeName);
		if(argType[0].argumentType == TYPEID_NONE)
			OOSQL_ERR(eNOTSUPPORTED_ATTR_TYPE_OOSQL);

		e = getStringPoolIndexFromID_Node(argType[0].argumentTypeName, sProcArgType);
		OOSQL_CHECK_ERR(e);

		argType[0].argumentTypeLength = typeNameToTypeLength(argumentTypeName);

		argType[0].asLocator = SM_FALSE;

		sProcArg = AST(sProcArg).brother;
	}

	// specific
	e = getStringPoolIndexFromID_Node(dbCommand[0].createProcedureInfo.specificName, AST(sProcSpecific).son);
	OOSQL_CHECK_ERR(e);

	// result set
	if(AST(sProcResultSet).son != AST_NULL)
	{
		e = getFourFromINTEGER_NODE(dbCommand[0].createProcedureInfo.resultsets, AST(sProcResultSet).son);
		OOSQL_CHECK_ERR(e);
	}
	else
		dbCommand[0].createProcedureInfo.resultsets = 0;

	// external name
	e = getStringPoolIndexFromSTRING_Node(dbCommand[0].createProcedureInfo.externalName, AST(sProcExternalName).son);
	OOSQL_CHECK_ERR(e);

	// deterministic
	if(AST(AST(sProcDeterministic).son).nodeName == VaTr)
		dbCommand[0].createProcedureInfo.deterministic = SM_TRUE;
	else
		dbCommand[0].createProcedureInfo.deterministic = SM_FALSE;

	// fenced
	if(AST(AST(sProcFenced).son).nodeName == VaTr)
		dbCommand[0].createProcedureInfo.fenced = SM_TRUE;
	else
		dbCommand[0].createProcedureInfo.fenced = SM_FALSE;

	// null call
	if(AST(AST(sProcFenced).son).nodeName == VaTr)
		dbCommand[0].createProcedureInfo.nullCall = SM_TRUE;
	else
		dbCommand[0].createProcedureInfo.nullCall = SM_FALSE;

	// language
	// check language is if C, JAVA
	e = getStringPoolIndexFromID_Node(dbCommand[0].createProcedureInfo.language, AST(sProcLanguage).son);
	OOSQL_CHECK_ERR(e);
	
	e = makeStringFromStringPool(language, dbCommand[0].createProcedureInfo.language);
	OOSQL_CHECK_ERR(e);

	makeLowerCaseString(language);

	if(!(language == (char* )"c" || language == (char* )"java"))	
		OOSQL_ERR(eUNSUPPORTED_LANGUAGE_OOSQL);
	
	// parameter style
	// check laguage is if OOSQL
	e = getStringPoolIndexFromID_Node(dbCommand[0].createProcedureInfo.parameterStyle, AST(sProcParameterStyle).son);
	OOSQL_CHECK_ERR(e);
	
	e = makeStringFromStringPool(parameterStyle, dbCommand[0].createProcedureInfo.parameterStyle);
	OOSQL_CHECK_ERR(e);

	makeLowerCaseString(parameterStyle);

	if(!(parameterStyle == (char* )"oosql"))
		OOSQL_ERR(eUNSUPPORTED_PARAMETER_STYLE_OOSQL);

	return eNOERROR;
}

Four OQL_ASTtoGDS::smtChkQuDropProc(ASTNodeIdx node, ReturnType& returnType)
{
	DBCommandPoolElements		dbCommand(m_pool->dbCommandPool);
	Four						e;
	ASTNodeIdx					sProcName;

	// check m_ast node structur and assign its index to local vars.
	sProcName = AST(node).son;
	if(sProcName == AST_NULL) OOSQL_ERR(eBADASTNODE_OOSQL);
	
	e = m_gds->init(m_pool, OQL_GDS::NO_QUERY);
	OOSQL_CHECK_ERR(e);

	// create new DBCommand
	dbCommand = m_pool->dbCommandPool.addNewEntry();
	dbCommand[0].command = DBCOMMAND_DROP_PROCEDURE;

	// procedure name
	e = getStringPoolIndexFromID_Node(dbCommand[0].dropProcedureInfo.procedureName, sProcName);
	OOSQL_CHECK_ERR(e);

	if(AST(node).nodeName == QuDropSpecProc)
		dbCommand[0].dropProcedureInfo.specific = SM_TRUE;
	else
		dbCommand[0].dropProcedureInfo.specific = SM_FALSE;

	return eNOERROR;
}

Four OQL_ASTtoGDS::smtChkQuCallProc(ASTNodeIdx node, ReturnType& returnType)
{
	DBCommandPoolElements		dbCommand(m_pool->dbCommandPool);
	Four						e;
	ASTNodeIdx					sProcName;
	ASTNodeIdx					sProcArg;
	ArgumentPoolIndex			argumentPoolIndex;
	ArgumentPoolElements		argument(m_pool->argumentPool);
	AP_ArgumentPoolElements		ap_argument(m_pool->ap_argumentPool);
	ReturnType					argType;

	// check m_ast node structur and assign its index to local vars.
	sProcName = AST(node).son;
	if(sProcName == AST_NULL) OOSQL_ERR(eBADASTNODE_OOSQL);
	
	sProcArg = AST(sProcName).brother;

	e = m_gds->init(m_pool, OQL_GDS::NO_QUERY);
	OOSQL_CHECK_ERR(e);

	// create new DBCommand
	dbCommand = m_pool->dbCommandPool.addNewEntry();
	dbCommand[0].command = DBCOMMAND_CALL_PROCEDURE;

	// procedure name
	e = getStringPoolIndexFromID_Node(dbCommand[0].callProcedureInfo.procedureName, sProcName);
	OOSQL_CHECK_ERR(e);

	// argument list
	dbCommand[0].callProcedureInfo.argumentList.setNull();
	while(sProcArg != AST_NULL)
	{
		e = smtChkQuery(sProcArg, argType);
        OOSQL_CHECK_ERR(e);

		e = makeParameterInArgumentPool(argType, argumentPoolIndex);
		OOSQL_CHECK_ERR(e);

		argument = argumentPoolIndex;
		ap_argument = m_pool->ap_argumentPool.addNewEntry();

		switch(argument[0].argumentKind)
		{
		case ARGUMENT_KIND_VALUE:
			ap_argument[0].argumentKind = argument[0].argumentKind;
			ap_argument[0].value = argument[0].value;
			break;
		default:
			OOSQL_ERR(eUNSUPPORTED_PROCEDURE_PARAMETER_OOSQL);
		}

		if(dbCommand[0].callProcedureInfo.argumentList == NULL_POOLINDEX)
			dbCommand[0].callProcedureInfo.argumentList = ap_argument.getPoolIndex();
		else
			dbCommand[0].callProcedureInfo.argumentList.size ++;

		sProcArg = AST(sProcArg).brother;
	}

	return eNOERROR;
}

Four OQL_ASTtoGDS::smtChkTextIrWeightFunction()
{
    Four                    i, j;
    FunctionPoolElements    func(m_pool->funcPool);
    ArgumentPoolElements    argument(m_pool->argumentPool);
    ValuePoolElements       value(m_pool->valuePool);
    Four                    weightAliasNumber;
    Four                    matchAliasNumber;
    
    func.setPoolIndex(0, m_pool->funcPool.nElements());

    // check weight function, weight function must connected with match function
    for(i = 0; i < func.size; i++)
    {
        if(func[i].functionID == FUNCTION_TEXTIR_WEIGHT)
        {
            // get weight's alias number
            argument = func[i].argument;
            if(argument[0].argumentKind != ARGUMENT_KIND_VALUE)
                OOSQL_ERR(eINTERNALERROR_OOSQL);

            value = argument[0].value;
            if(value[0].valueKind != VALUE_KIND_INTEGER)
                OOSQL_ERR(eINTERNALERROR_OOSQL);

            weightAliasNumber = m_pool->intPool[value[0].integer.startIndex];

            if(weightAliasNumber != WEIGHT_DEFAULT_LABEL)
            {
                // find match function
                for(j = 0; j < func.size; j++)
                {
                    if(func[j].functionID == FUNCTION_TEXTIR_MATCH)
                    {
                        argument = func[j].argument;
                        if(argument[2].argumentKind != ARGUMENT_KIND_VALUE)
                            OOSQL_ERR(eINTERNALERROR_OOSQL);

                        value = argument[2].value;
                        if(value[0].valueKind != VALUE_KIND_INTEGER)
                            OOSQL_ERR(eINTERNALERROR_OOSQL);

                        matchAliasNumber = m_pool->intPool[value[0].integer.startIndex];

                        if(weightAliasNumber == matchAliasNumber)
                            break;              // find
                    }
                }
            
                if(j == func.size)
                    OOSQL_ERR(eWEIGHT_FUNCTION_ERR_OOSQL)
            }
        }
    }

    return eNOERROR;
}

Four OQL_ASTtoGDS::getQGnodes_traverseExprTree(ExprPoolElements& expr, QGNodeIndexArray& results)
{
    ExprPoolElements            leftExpr(m_pool->exprPool);
    ExprPoolElements            rightExpr(m_pool->exprPool);
    PathExprPoolElements        pathExpr(m_pool->pathExprPool);
    FunctionPoolElements        func(m_pool->funcPool);
    AggrFuncPoolElements        aggrFunc(m_pool->aggrFuncPool);
    ArgumentPoolElements        argument(m_pool->argumentPool);
    Four                        i;
    Four                        e;

    switch(expr[0].exprKind)
    {
    case EXPR_KIND_PATHEXPR:
        pathExpr             = expr[0].pathExpr;
        results.add(pathExpr[pathExpr.size - 1].qgNodeNo.startIndex);

        // if pathexpr had some member functions and it has argument, then check arguments
        for(i = 0; i < pathExpr.size; i++)
        {
            if(pathExpr[i].fromAttrKind == PATHEXPR_KIND_METHOD)
            {
                argument = pathExpr[i].method.argument;
                e = getQGnodes_argument(argument, results);
                OOSQL_CHECK_ERR(e);
            }

            if(pathExpr[i].fromAttrKind == PATHEXPR_KIND_FUNC)
            {
                func = pathExpr[i].func;
                argument = func[0].argument;
                e = getQGnodes_argument(argument, results);
                OOSQL_CHECK_ERR(e);
            }
        }
        break;

    case EXPR_KIND_AGGRFUNC:
        // check arguments
        aggrFunc = expr[0].aggrFunc;
        argument = aggrFunc[0].argument;
        e = getQGnodes_argument(argument, results);
        OOSQL_CHECK_ERR(e);
        
        break;

    case EXPR_KIND_FUNCTION:
        // check arguments
        func     = expr[0].func;
        argument = func[0].argument;
        e = getQGnodes_argument(argument, results);
        OOSQL_CHECK_ERR(e);

        break;

    case EXPR_KIND_OPER:
        // chech if operator is unary operator or binary operator
        switch(expr[0].oper.operatorId)
        {
        case OP_UNARY_MINUS:
        case OP_ABS:
		case OP_ISNULL:
		case OP_ISNOTNULL:

            // unary case
            leftExpr  = expr[0].oper.operand1;

            e = getQGnodes_traverseExprTree(leftExpr,  results);
            OOSQL_CHECK_ERR(e);
            break;

        default:
            // binary case
            leftExpr  = expr[0].oper.operand1;
            rightExpr = expr[0].oper.operand2;

            e = getQGnodes_traverseExprTree(leftExpr,  results);
            OOSQL_CHECK_ERR(e);

            e = getQGnodes_traverseExprTree(rightExpr, results);
            OOSQL_CHECK_ERR(e);
            break;
        }
        break;

    case EXPR_KIND_VALUE:
        break;

    case EXPR_KIND_CONS:
        break;

    default:
        OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
        break;
    }

    return eNOERROR;
}

Four OQL_ASTtoGDS::setExplicitJoinLinkInQG()
{
    QGNodePoolElements          queryGraph(m_pool->qgNodePool);
    QGNodePoolElements          qgNode(m_pool->qgNodePool);
    CondListPoolElements        conditionInfo(m_pool->condInfoPool);
    ExprPoolElements            expr(m_pool->exprPool);
    JoinInfoPoolElements        joinInfo(m_pool->joinInfoPool);
	QGNodeIndexArray			qgNodes(pMemoryManager, false);
    Four                        i, j, k;
    Four                        e;

	e = m_gds->getQueryGraph(m_pool, queryGraph);
	OOSQL_CHECK_ERR(e);

    for(k = 0; k < queryGraph.size; k++)    
    {
        // for each QG node
        conditionInfo = queryGraph[k].conditionList;
        while(conditionInfo != NULL_POOLINDEX)
        {   
            // for each condition
            expr = conditionInfo[0].expr;

			e = getQGnodes_traverseExprTree(expr, qgNodes);
			OOSQL_CHECK_ERR(e);

			for(i = 0; i < qgNodes.size(); i++)
			{
				for(j = i + 1; j < qgNodes.size(); j++)
				{
					if(qgNodes[i] != qgNodes[j])
					{
						qgNode.setPoolIndex(qgNodes[i], 1);
						joinInfo = m_pool->joinInfoPool.addNewEntry();
						joinInfo[0].joinClass.setPoolIndex(qgNodes[j], 1);
						qg_addNodeToExplicitJoinList(qgNode, joinInfo);

						qgNode.setPoolIndex(qgNodes[j], 1);
						joinInfo = m_pool->joinInfoPool.addNewEntry();
						joinInfo[0].joinClass.setPoolIndex(qgNodes[i], 1);
						qg_addNodeToExplicitJoinList(qgNode, joinInfo);
					}
				}
			}

            conditionInfo = conditionInfo[0].nextCondInfo;  // next conditionInfo
        }

    }

    return eNOERROR;
}

Four OQL_ASTtoGDS::smtChkClSel(
    ASTNodeIdx  node,
    ReturnType& returnType
)
{
    ASTNodeIdx              s1, s2;
    ReturnType              s2Type;
    Four                    length;
    SelListPoolElements     slpElements(m_pool->selListPool);
    AggrFuncPoolElements    afpElements(m_pool->aggrFuncPool);
    PathExprPoolElements    pathExpr(m_pool->pathExprPool);
    TargetListPoolElements  targetList(m_pool->targetListPool);
    Four                    e;
    Four                    i, j;
	Four					selListType = 0;
	Four					nAttrs, totalProjected, index;
	CataAttrInfo			attrInfo;
    Four                    isLrdsRelation;
    Boolean                 flag;
    NameAssociatedInfo      idAssociatedInfo;
    CataClassInfo           classInfo;


    switch ( AST(node).nodeName ) {
    case ClSelAll:
        // SELECT ... (NO DISTINCT)
        break;
    case ClSelDist:
        // SELECT DISTINCT ...
        selListType |= DIST_BIT;
        break;
    default:
        OOSQL_ERR(eBADASTNODE_OOSQL);
        break;
    }

    s1 = AST(node).son;

    length = 0;
    while(s1 != AST_NULL)
    {
        switch ( AST(s1).nodeName ) {
		case ProAllLogicalID:
			// select #
            selListType |= STAR_BIT;

			e = m_gds->getTargetList(m_pool, targetList); OOSQL_CHECK_ERR(e);
			for(totalProjected = 0, i = 0; i < targetList.size; i++)
			{
				e = m_catalog->class_GetN_Attrs(targetList[i].collectionInfo.classInfo, nAttrs);
				OOSQL_CHECK_ERR(e);
				totalProjected += nAttrs;
			}

            slpElements = m_pool->selListPool.addNewEntry(totalProjected);
            length     += totalProjected;

            for(index = 0, i = 0; i < targetList.size; i++)
            {
				e = m_catalog->class_GetN_Attrs(targetList[i].collectionInfo.classInfo, nAttrs);
				OOSQL_CHECK_ERR(e);

				for(j = 0; j < nAttrs; j++, index++)
				{
					e = m_catalog->attr_ColNo_to_AttrInfo(targetList[i].collectionInfo.classInfo, (short)j, attrInfo);
					OOSQL_CHECK_ERR(e);

					// construct pathexpr
					pathExpr = m_pool->pathExprPool.addNewEntry(2);

					pathExpr[0].aliasName.setNull();
					pathExpr[0].classInfo         = targetList[i].collectionInfo.classInfo;
					pathExpr[0].fromAttrKind      = PATHEXPR_KIND_CLASS;
					pathExpr[0].domainSubstFlag   = SM_FALSE;
					pathExpr[0].substitutedClass  = -1;
					pathExpr[0].typeCastingFlag   = SM_FALSE;
					pathExpr[0].typeCastedClass   = -1;
					pathExpr[0].planNo.setNull();
					pathExpr[0].pathExprKind      = 0;
				
					pathExpr[1].classInfo         = targetList[i].collectionInfo.classInfo;
					pathExpr[1].fromAttrKind      = PATHEXPR_KIND_ATTR;
					pathExpr[1].attr.attrInfo     = attrInfo;
					pathExpr[1].domainSubstFlag   = SM_FALSE;
					pathExpr[1].substitutedClass  = -1;
					pathExpr[1].typeCastingFlag   = SM_FALSE;
					pathExpr[1].typeCastedClass   = -1;
					pathExpr[1].planNo.setNull();
					pathExpr[1].pathExprKind      = 0;

					// construct query graph
					e = qg_consPathExpr(pathExpr.getPoolIndex());
					OOSQL_CHECK_ERR(e);

					// connect pathExpr to slpElements
					slpElements[index].selElemKind = SELLIST_KIND_PATHEXPR;
					slpElements[index].pathExpr    = pathExpr.getPoolIndex();
				}
            }
        
            break;

        case ProAll:
            // select *
            selListType |= STAR_BIT;

			e = m_gds->getTargetList(m_pool, targetList); OOSQL_CHECK_ERR(e);
			for(totalProjected = 0, i = 0; i < targetList.size; i++)
			{
				e = m_catalog->class_GetN_Attrs(targetList[i].collectionInfo.classInfo, nAttrs);
				OOSQL_CHECK_ERR(e);

                isLrdsRelation = m_catalog->class_IsLrdsRelation(targetList[i].collectionInfo.classInfo);
                if(!isLrdsRelation)
                    nAttrs --;      
				totalProjected += nAttrs;
			}

            slpElements = m_pool->selListPool.addNewEntry(totalProjected);
            length     += totalProjected;

            for(index = 0, i = 0; i < targetList.size; i++)
            {
				e = m_catalog->class_GetN_Attrs(targetList[i].collectionInfo.classInfo, nAttrs);
				OOSQL_CHECK_ERR(e);

                isLrdsRelation = m_catalog->class_IsLrdsRelation(targetList[i].collectionInfo.classInfo);
                if(!isLrdsRelation) j = 1;  
                else                j = 0;  
				for(; j < nAttrs; j++, index++)
				{
					e = m_catalog->attr_ColNo_to_AttrInfo(targetList[i].collectionInfo.classInfo, (short)j, attrInfo);
					OOSQL_CHECK_ERR(e);

					// construct pathexpr
					pathExpr = m_pool->pathExprPool.addNewEntry(2);

					pathExpr[0].aliasName.setNull();
					pathExpr[0].classInfo         = targetList[i].collectionInfo.classInfo;
					pathExpr[0].fromAttrKind      = PATHEXPR_KIND_CLASS;
					pathExpr[0].domainSubstFlag   = SM_FALSE;
					pathExpr[0].substitutedClass  = -1;
					pathExpr[0].typeCastingFlag   = SM_FALSE;
					pathExpr[0].typeCastedClass   = -1;
					pathExpr[0].planNo.setNull();
					pathExpr[0].pathExprKind      = 0;
				
					pathExpr[1].classInfo         = targetList[i].collectionInfo.classInfo;
					pathExpr[1].fromAttrKind      = PATHEXPR_KIND_ATTR;
					pathExpr[1].attr.attrInfo     = attrInfo;
					pathExpr[1].domainSubstFlag   = SM_FALSE;
					pathExpr[1].substitutedClass  = -1;
					pathExpr[1].typeCastingFlag   = SM_FALSE;
					pathExpr[1].typeCastedClass   = -1;
					pathExpr[1].planNo.setNull();
					pathExpr[1].pathExprKind      = 0;

					// construct query graph
					e = qg_consPathExpr(pathExpr.getPoolIndex());
					OOSQL_CHECK_ERR(e);

					// connect pathExpr to slpElements
					slpElements[index].selElemKind = SELLIST_KIND_PATHEXPR;
					slpElements[index].pathExpr    = pathExpr.getPoolIndex();
				}
            }
        
            break;
        case ProSmp:
            s2 = AST(s1).son;
            if (AST(s2).nodeName == QuAccPaexAll)
            {
                e = isID_AliasName(AST(s2).son, idAssociatedInfo, flag);
                OOSQL_CHECK_ERR(e);

                if (flag)
                {
                    // select *
                    selListType |= STAR_BIT;

                    if (idAssociatedInfo.kind != NA_KIND_FROM)
                    {
                        m_errorMessage->Clear();
                        m_errorMessage->Append("The path expression flagment at ");
                        ERRORMESSAGE_APPEND_POSITION(m_errorMessage, AST(s2).son);
                        m_errorMessage->Append(" is invalid.");

                        OOSQL_ERR(eINVALID_PATHEXPR_OOSQL);

                    }

                    classInfo = m_pool->targetListPool[idAssociatedInfo.target.startIndex].collectionInfo.classInfo;

                    e = m_catalog->class_GetN_Attrs(classInfo, nAttrs);
                    OOSQL_CHECK_ERR(e);

                    isLrdsRelation = m_catalog->class_IsLrdsRelation(classInfo);
                    if(!isLrdsRelation){
                        nAttrs--;
                        i = 1;
                    }
                    else
                        i = 0;


                    // construct path expression element
                    slpElements = m_pool->selListPool.addNewEntry(nAttrs);
                    length      += nAttrs;

                    e = m_catalog->class_GetN_Attrs(classInfo, nAttrs);
                    OOSQL_CHECK_ERR(e);

                    for (index = 0;i < nAttrs; i++, index++){
                        e = m_catalog->attr_ColNo_to_AttrInfo(classInfo, (short)i, attrInfo);
                        OOSQL_CHECK_ERR(e);


                        // construct pathexpr

                        pathExpr = m_pool->pathExprPool.addNewEntry(2);

                        pathExpr[0].aliasName.setNull();
                        pathExpr[0].classInfo         = classInfo;
                        pathExpr[0].fromAttrKind      = PATHEXPR_KIND_CLASS;
                        pathExpr[0].substitutedClass  = -1;
                        pathExpr[0].typeCastingFlag   = SM_FALSE;
                        pathExpr[0].typeCastedClass   = -1;
                        pathExpr[0].planNo.setNull();
                        pathExpr[0].pathExprKind      = 0;

                        pathExpr[1].classInfo         = classInfo;
                        pathExpr[1].fromAttrKind      = PATHEXPR_KIND_ATTR;
                        pathExpr[1].attr.attrInfo     = attrInfo;
                        pathExpr[1].substitutedClass  = -1;
                        pathExpr[1].typeCastingFlag   = SM_FALSE;
                        pathExpr[1].typeCastedClass   = -1;
                        pathExpr[1].planNo.setNull();
                        pathExpr[1].pathExprKind      = 0;

                        // construct query graph
                        e = qg_consPathExpr(pathExpr.getPoolIndex());
                        OOSQL_CHECK_ERR(e);

                        // connect pathExpr to slpElements
                        slpElements[index].selElemKind = SELLIST_KIND_PATHEXPR;
                        slpElements[index].pathExpr    = pathExpr.getPoolIndex();
                    }
                    break;
                }


                e = isID_ClassName(AST(s2).son, classInfo, flag);
                OOSQL_CHECK_ERR(e);

                if (flag)
                {
                    // select *
                    selListType |= STAR_BIT;

                    e = m_gds->getTargetList(m_pool, targetList); OOSQL_CHECK_ERR(e);
                    for(i = targetList.startIndex;i < targetList.startIndex + targetList.size; i++)
                    {
                        if(m_pool->targetListPool[i].collectionInfo.classInfo == classInfo)
                        {
                            break;
                        }
                    }

                    if(i == targetList.startIndex + targetList.size)
                    {
                        flag = SM_FALSE;
                        classInfo = -1;
                    }

                    e = m_catalog->class_GetN_Attrs(classInfo, nAttrs);
                    OOSQL_CHECK_ERR(e);

                    isLrdsRelation = m_catalog->class_IsLrdsRelation(targetList[i].collectionInfo.classInfo);
                    if(!isLrdsRelation){
                        nAttrs --;      
                        i = 1;
                    }
                    else
                        i = 0;


                    slpElements = m_pool->selListPool.addNewEntry(nAttrs);
                    length     += nAttrs;

                    e = m_catalog->class_GetN_Attrs(classInfo, nAttrs);
                    OOSQL_CHECK_ERR(e);

                    for (index = 0;i < nAttrs; i++, index++){
                        e = m_catalog->attr_ColNo_to_AttrInfo(classInfo, (short)i, attrInfo);
                        OOSQL_CHECK_ERR(e);

                        // construct pathexpr
                        pathExpr = m_pool->pathExprPool.addNewEntry(2);
                        pathExpr[0].aliasName.setNull();
                        pathExpr[0].classInfo         = classInfo;
                        pathExpr[0].fromAttrKind      = PATHEXPR_KIND_CLASS;
                        pathExpr[0].substitutedClass  = -1;
                        pathExpr[0].typeCastingFlag   = SM_FALSE;
                        pathExpr[0].typeCastedClass   = -1;
                        pathExpr[0].planNo.setNull();
                        pathExpr[0].pathExprKind      = 0;

                        pathExpr[1].classInfo         = classInfo;
                        pathExpr[1].fromAttrKind      = PATHEXPR_KIND_ATTR;
                        pathExpr[1].attr.attrInfo     = attrInfo;
                        pathExpr[1].domainSubstFlag   = SM_FALSE;
                        pathExpr[1].substitutedClass  = -1;
                        pathExpr[1].typeCastingFlag   = SM_FALSE;
                        pathExpr[1].typeCastedClass   = -1;
                        pathExpr[1].planNo.setNull();
                        pathExpr[1].pathExprKind      = 0;

                        // construct query graph
                        e = qg_consPathExpr(pathExpr.getPoolIndex());
                        OOSQL_CHECK_ERR(e);

                        // connect pathExpr to slpElements
                        slpElements[index].selElemKind = SELLIST_KIND_PATHEXPR;
                        slpElements[index].pathExpr    = pathExpr.getPoolIndex();
                    }
                }
            }


            else
            {

            slpElements = m_pool->selListPool.addNewEntry();
			length ++;

            e = smtChkQuery(s2, s2Type);
            OOSQL_CHECK_ERR(e);
            
            switch(s2Type.poolType)
            {
            case PT_PATHEXPRPOOL:
                selListType |= PATH_BIT;
                slpElements[0].selElemKind = SELLIST_KIND_PATHEXPR;
                slpElements[0].pathExpr.setPoolIndex(s2Type.poolIndex);
                break;

            case PT_AGGRFUNCPOOL:
				afpElements = s2Type.poolIndex;
				if(afpElements[0].distinctFlag)
					selListType |= AGGR_DIST_BIT;
                selListType |= AGGR_BIT;
                slpElements[0].selElemKind = SELLIST_KIND_AGGRFUNC;
                slpElements[0].aggrFunc.setPoolIndex(s2Type.poolIndex);
                break;

            case PT_FUNCPOOL:
                slpElements[0].selElemKind = SELLIST_KIND_FUNC;
                slpElements[0].func.setPoolIndex(s2Type.poolIndex);
                break;

            case PT_VALUEPOOL:
                slpElements[0].selElemKind = SELLIST_KIND_VALUE;
                slpElements[0].value.setPoolIndex(s2Type.poolIndex);
                break;

            case PT_EXPRPOOL:
                slpElements[0].selElemKind = SELLIST_KIND_OPER;
                slpElements[0].oper        = m_pool->exprPool[s2Type.poolIndex.startIndex].oper;
                break;

            case PT_CONSTRUCTPOOL:
                slpElements[0].selElemKind = SELLIST_KIND_CONS;
                slpElements[0].pathExpr.setPoolIndex(s2Type.poolIndex);
                break;

			case PT_STRINGPOOL:
				m_errorMessage->Clear();
				m_errorMessage->Append("Undefined Identifier is used at ");
				ERRORMESSAGE_APPEND_POSITION(m_errorMessage, node);

				OOSQL_ERR(eSYNTAX_ERROR_OOSQL);
				break;

            default:
                OOSQL_ERR(eBADASTNODE_OOSQL);
            }
            }
            break;

        case ProAs:
            // select query AS ID
            OOSQL_ERR(eNOTIMPLEMENTED_OOSQL);
            break;
        default:
            OOSQL_ERR(eBADASTNODE_OOSQL);
            break;
        }
        s1 = AST(s1).brother;
        
    }
    
	// make selList which project all the classes in FROM clause
    e = m_gds->getTargetList(m_pool, targetList); OOSQL_CHECK_ERR(e);

    for(i = 0; i < targetList.size; i++)
    {
        // construct pathexpr
        pathExpr = m_pool->pathExprPool.addNewEntry();
        pathExpr[0].aliasName         = targetList[i].aliasName;
        pathExpr[0].classInfo         = targetList[i].collectionInfo.classInfo;
        pathExpr[0].fromAttrKind      = PATHEXPR_KIND_CLASS;
        pathExpr[0].domainSubstFlag   = SM_FALSE;
        pathExpr[0].substitutedClass  = -1;
        pathExpr[0].typeCastingFlag   = SM_FALSE;
        pathExpr[0].typeCastedClass   = -1;
        pathExpr[0].planNo.setNull();
        pathExpr[0].pathExprKind      = 0;

        // construct query graph
        e = qg_consPathExpr(pathExpr.getPoolIndex());
        OOSQL_CHECK_ERR(e);

		// allocate new sellist element
		slpElements = m_pool->selListPool.addNewEntry();
		length ++;

        // connect pathExpr to slpElements
        slpElements[0].selElemKind = SELLIST_KIND_PATHEXPR;
        slpElements[0].pathExpr    = pathExpr.getPoolIndex();
    }

	e = m_gds->setSelListType(m_pool, selListType);
	OOSQL_CHECK_ERR(e);

	SelListPoolIndex selList;
	selList.setPoolIndex(0, length);
	e = m_gds->setSelList(m_pool, selList);
	OOSQL_CHECK_ERR(e);

    return eNOERROR;
}

Four OQL_ASTtoGDS::smtChkClFr(
    ASTNodeIdx  node,
    ReturnType& returnType
)
{
    ASTNodeIdx					s, b1, b2;
    ReturnType					b2Type;
    Four						length;
    CataClassInfo				classInfo;
    NameAssociatedInfo			idAssociatedInfo;
    SimpleString				idString;
	SimpleString				oidString;
    TargetListPoolElements		tlpElements(m_pool->targetListPool);
    Four						e;
    Boolean						flag;
	OOSQL_StorageManager::OID	oid;

    if(AST(node).nodeName != ClFr)
        OOSQL_ERR(eBADASTNODE_OOSQL);   

    length = 0;
	s = AST(node).son;

    while(s != AST_NULL)
    {
		switch(AST(s).nodeName)
		{
		case FrObject:
			b1 = AST(s).son;
			
			// get oid from oid string
			e = getStringFromSTRING_Node(oidString, b1);
			OOSQL_CHECK_ERR(e);

			e = convertOidStringToOID(oidString, oid);
			OOSQL_CHECK_ERR(e);

			// get class info from oid
			e = m_catalog->class_ClassId_to_ClassInfo(oid.classID, classInfo);
			OOSQL_CHECK_ERR(e);

			// construct GDS (TargetList)
			tlpElements = m_pool->targetListPool.addNewEntry();

			tlpElements[0].collectionInfo.classInfo = classInfo;
			tlpElements[0].aliasName.setNull();
			tlpElements[0].starFlag                 = SM_FALSE;
			tlpElements[0].isTargetOid				= SM_TRUE;
			tlpElements[0].oid						= oid;
			break;

		case FrCltn:
			b1 = AST(s).son;
			b2 = AST(b1).brother;
			// check target class
			// possible casses
			//    1. b1 is a ID
			//    2. b1 is a path expression
			if(AST(b1).nodeName == ID)
			{
				// possible cases
				//   1. ID is a class name
				//   2. ID is a alias name in NameStack
				e = isID_AliasName(b1, idAssociatedInfo, flag);
				OOSQL_CHECK_ERR(e);
				if(flag)
				{
					OOSQL_ERR(eNOTIMPLEMENTED_OOSQL);
				}
				else
				{
					// check ID is a class name. if true, get it's class info
					e = isID_ClassName(b1, classInfo, flag);
					OOSQL_CHECK_ERR(e);
					if(!flag)
					{
						SimpleString classname;

						e = getStringFromID_Node(classname, b1);
						OOSQL_CHECK_ERR(e);

						m_errorMessage->Clear();
						m_errorMessage->Append("The class '");
						m_errorMessage->Append((char*)classname);
						m_errorMessage->Append("' is not defined in the database.");

						OOSQL_ERR(eTARGETNOTPERSISTENT_OOSQL);
					}
				}
			}
			else
			{
				OOSQL_ERR(eNOTIMPLEMENTED_OOSQL);
			}   
        
			// check alias name 
			// Possible type of b2Type
			//   1. TYPEID_ID, PT_STRINGPOOL
			//   2. TYPEID_NULL
			e = smtChkQuery(b2, b2Type);
			OOSQL_CHECK_ERR(e);

			if(!((b2Type.typeID == TYPEID_ID && b2Type.poolType == PT_STRINGPOOL) || 
				  b2Type.typeID == TYPEID_NULL))
			{
				m_errorMessage->Clear();
				m_errorMessage->Append("The type of the value used at ");
				ERRORMESSAGE_APPEND_POSITION(m_errorMessage, b2);
				m_errorMessage->Append(" is not a identifier, string where one of those is expected.");

				OOSQL_ERR(eTYPE_ERROR_OOSQL);
			}

			// construct GDS (TargetList)
			tlpElements = m_pool->targetListPool.addNewEntry();

			tlpElements[0].collectionInfo.classInfo = classInfo;
			tlpElements[0].starFlag                 = SM_TRUE;     // always SM_TRUE in OQL
			tlpElements[0].isTargetOid				= SM_FALSE;

			if(b2Type.typeID == TYPEID_NULL)
			{
				tlpElements[0].aliasName.setNull();
			}
			else
			{
				NameAssociatedInfo  info;

				tlpElements[0].aliasName.setPoolIndex(b2Type.poolIndex);

				// Register new ID into m_nameStack
				// first argument of AddItem is a string and 
				// second argument is a name associated info
				info.astNodeIdx = b1;
				info.kind       = NA_KIND_FROM;
				info.target     = tlpElements.getPoolIndex();
            
				e = getStringFromID_Node(idString, b2);
				OOSQL_CHECK_ERR(e);

				e = m_nameStack->AddEntry(idString, info);
				if(e == eDUPLICATED_KEY_DICTIONARY)
				{
					m_errorMessage->Clear();
					m_errorMessage->Append("The aliasname used at ");
					ERRORMESSAGE_APPEND_POSITION(m_errorMessage, b2);
					m_errorMessage->Append(" is duplicated.");

					OOSQL_ERR(eSAME_ALIASNAME_OOSQL);
				}
			}
			break;
		default:
			OOSQL_ERR(eBADASTNODE_OOSQL);
		}

		s = AST(s).brother;
        length ++;
    }

	TargetListPoolIndex targetList;
	targetList.setPoolIndex(0, length);
	e = m_gds->setTargetList(m_pool, targetList);
	OOSQL_CHECK_ERR(e);

    // construct initial QG nodes
    e = qg_consInitialQG_Nodes();
    OOSQL_CHECK_ERR(e);

    return eNOERROR;
}

Four OQL_ASTtoGDS::convertOidStringToOID(SimpleString& oidString, OOSQL_StorageManager::OID& oid)
{
	Four length = oidString.length();
	if(length != sizeof(Four)*8)
		return eBADPARAMETER_OOSQL;

    return oosql_ConvertFromOIDStringToOIDStructure((char*)oidString, &oid);
}



Four OQL_ASTtoGDS::smtChkClWh(
    ASTNodeIdx  node,
    ReturnType& returnType
)
{
    ASTNodeIdx          s;
    ExprPoolElements    epElements(m_pool->exprPool);
    Four                e;
    ExprPoolIndex       exprPoolIndex;
	Boolean				flag;

    switch(AST(node).nodeName)
    {
    case ClWh:
        break;
    case null:
        returnType.set(TYPEID_NULL, PT_NONE, NULL_POOLINDEX);
        return eNOERROR;
    default:
        OOSQL_ERR(eBADASTNODE_OOSQL);
    }

    // construct condition tree
    s = AST(node).son;
    e = smtChkQuery(s, returnType, SM_TRUE);
    OOSQL_CHECK_ERR(e);

	e = testCompatibility(returnType.typeID, TYPEID_BOOL, flag);
	OOSQL_CHECK_ERR(e);
	if(!flag) 
        OOSQL_ERR(eEXPRERR_WHERE_OOSQL);           

    if(returnType.poolType != PT_EXPRPOOL)
    {
        epElements = m_pool->exprPool.addNewEntry();

        switch(returnType.poolType)
        {
        case PT_PATHEXPRPOOL:
            epElements[0].exprKind = EXPR_KIND_PATHEXPR;
            epElements[0].pathExpr.setPoolIndex(returnType.poolIndex);
            break;

        case PT_FUNCPOOL:
            epElements[0].exprKind = EXPR_KIND_FUNCTION;
            epElements[0].func.setPoolIndex(returnType.poolIndex);
            break;

        case PT_VALUEPOOL:
            epElements[0].exprKind = EXPR_KIND_VALUE;
            epElements[0].value.setPoolIndex(returnType.poolIndex);
            break;

        default:
            OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
        }
        epElements[0].operandType     = EXPR_OPERANDTYPE_CONTAIN_NOMETHOD;
        epElements[0].refCount        = 0;

        e = exprElementSetTypeInfo(epElements, returnType);
        OOSQL_CHECK_ERR(e);

		e = m_gds->setWhereCond(m_pool, epElements.getPoolIndex());
		OOSQL_CHECK_ERR(e);

        exprPoolIndex = epElements.getPoolIndex();
		e = m_gds->addNodeToUnusedConditionList(m_pool, exprPoolIndex);
		OOSQL_CHECK_ERR(e);
    }
    else
	{
		ExprPoolIndex whereCond;
		whereCond.setPoolIndex(returnType.poolIndex);
		e = m_gds->setWhereCond(m_pool, whereCond);
		OOSQL_CHECK_ERR(e);
	}

    return eNOERROR;
}

Four OQL_ASTtoGDS::smtChkClGrp(
    ASTNodeIdx  node,
    ReturnType& returnType
)
{
    ASTNodeIdx              s;
    ASTNodeIdx              b1, b2;
    ReturnType              b1Type, b2Type;
    Four                    length;
    GroupByListPoolElements glpElements(m_pool->groupByListPool);
    Four                    e;
	SimpleString			keyString;

    switch(AST(node).nodeName)
    {
    case ClGrp:
        break;
    case null:
        returnType.set(TYPEID_NULL, PT_NONE, NULL_POOLINDEX);
        return eNOERROR;
    default:
        OOSQL_ERR(eBADASTNODE_OOSQL);
    }
        
    length = 0;
    s = AST(node).son;
    while(s != AST_NULL)
    {
        if(!(AST(s).nodeName == ProSmp || AST(s).nodeName == ProAs))
            OOSQL_ERR(eBADASTNODE_OOSQL);

        b1 = AST(s).son;

        e = smtChkQuery(b1, b1Type);
        OOSQL_CHECK_ERR(e);

        // check group by key is path expression or aggr function 
        if(!(b1Type.poolType == PT_PATHEXPRPOOL || b1Type.poolType == PT_AGGRFUNCPOOL))
            OOSQL_ERR(eBADASTNODE_OOSQL);

        // construct GDS (GroupByList)
        glpElements = m_pool->groupByListPool.addNewEntry();
        
        // GROUPBY_KIND_EXPR
        switch(b1Type.poolType)
        {
        case PT_PATHEXPRPOOL:
            glpElements[0].grpByKeyKind = GROUPBY_KIND_PATHEXPR;
            glpElements[0].pathExpr.setPoolIndex(b1Type.poolIndex);
            break;

        case PT_AGGRFUNCPOOL:
            glpElements[0].grpByKeyKind = GROUPBY_KIND_AGGRFUNC;
            glpElements[0].aggrFunc.setPoolIndex(b1Type.poolIndex);
            break;
        }

        if(AST(s).nodeName == ProAs)        // there is an alias name
        {
            if(glpElements[0].grpByKeyKind != GROUPBY_KIND_PATHEXPR)
                OOSQL_ERR(eNOTIMPLEMENTED_OOSQL);

            NameAssociatedInfo  info;

            b2 = AST(b1).brother;
            
            e = smtChkQuery(b2, b2Type);
            OOSQL_CHECK_ERR(e);

            if(b2Type.typeID != TYPEID_STRING || b2Type.typeID != TYPEID_VARSTRING)
                OOSQL_ERR(eBADASTNODE_OOSQL);

            glpElements[0].aliasName.setPoolIndex(b2Type.poolIndex);

            // Register new ID into m_nameStack
            // first argument of AddItem is a string and second argument is a ASTNodeIdx
            // second argumnet is associated with first argument and first argument is
            // used for search key.
            info.astNodeIdx          = b1;
            info.kind                = NA_KIND_GROUPBY;
            info.pathExpr            = glpElements[0].pathExpr;

			keyString = &(m_pool->stringPool[b2Type.poolIndex.startIndex]);
            m_nameStack->AddEntry(keyString, info);
        }
        else    // ProSmp
        {
            glpElements[0].aliasName.setNull();
        }

        s = AST(s).brother;
        length ++;
    }

	GroupByListPoolIndex groupByList;
    groupByList.setPoolIndex(0, length);
	e = m_gds->setGroupByList(m_pool, groupByList);
	OOSQL_CHECK_ERR(e);

    return eNOERROR;
}

Four OQL_ASTtoGDS::smtChkClHav(
    ASTNodeIdx  node,
    ReturnType& returnType
)
{
    ASTNodeIdx				s;
    ExprPoolElements		epElements(m_pool->exprPool);
    Four					e;    
	GroupByListPoolIndex	groupByList;
	Boolean				flag;

	e = m_gds->getGroupByList(m_pool, groupByList);
	OOSQL_CHECK_ERR(e);

    // check node
    switch(AST(node).nodeName)
    {
    case ClHav:
        if(groupByList.size == 0)
            OOSQL_ERR(eHAVINGWITHOUTGROUPBY_OOSQL);
        break;
    case null:
        returnType.set(TYPEID_NULL, PT_NONE, NULL_POOLINDEX);
        return eNOERROR;
    default:
        OOSQL_ERR(eBADASTNODE_OOSQL);
    }
    
    // construct condition tree
    s = AST(node).son;
    e = smtChkQuery(s, returnType, SM_TRUE);
    OOSQL_CHECK_ERR(e);

	e = testCompatibility(returnType.typeID, TYPEID_BOOL, flag);
	OOSQL_CHECK_ERR(e);
    if(!flag)
        OOSQL_ERR(eEXPRERR_HAVING_OOSQL);

    if(returnType.poolType != PT_EXPRPOOL)
    {
        epElements = m_pool->exprPool.addNewEntry();

        switch(returnType.poolType)
        {
        case PT_PATHEXPRPOOL:
            epElements[0].exprKind = EXPR_KIND_PATHEXPR;
            epElements[0].pathExpr.setPoolIndex(returnType.poolIndex);
            break;

        case PT_FUNCPOOL:
            epElements[0].exprKind = EXPR_KIND_FUNCTION;
            epElements[0].func.setPoolIndex(returnType.poolIndex);
            break;

        case PT_VALUEPOOL:
            epElements[0].exprKind = EXPR_KIND_VALUE;
            epElements[0].value.setPoolIndex(returnType.poolIndex);
            break;

        default:
            OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
        }
        epElements[0].operandType  = EXPR_OPERANDTYPE_CONTAIN_NOMETHOD;
        epElements[0].refCount     = 0;

        e = exprElementSetTypeInfo(epElements, returnType);
        OOSQL_CHECK_ERR(e);

		e = m_gds->setHavingCond(m_pool, epElements.getPoolIndex());
		OOSQL_CHECK_ERR(e);
    }
    else
	{
		ExprPoolIndex havingCond;
		havingCond.setPoolIndex(returnType.poolIndex);
		e = m_gds->setHavingCond(m_pool, havingCond);
		OOSQL_CHECK_ERR(e);
	}

    return eNOERROR;
}

Four OQL_ASTtoGDS::smtChkClOrd(
    ASTNodeIdx  node,
    ReturnType& returnType
)
{
    ASTNodeIdx              b1, b2;
    ReturnType              b1Type;
    Four                    length;
    Four                    e;
    OrderByListPoolElements olpElements(m_pool->orderByListPool);
	SelListPoolIndex		selList;

	e = m_gds->getSelList(m_pool, selList);
	OOSQL_CHECK_ERR(e);

    if(AST(node).nodeName == null)
    {
        returnType.set(TYPEID_NULL, PT_NONE, NULL_POOLINDEX);
        return eNOERROR;
    }
    else if ( AST(node).nodeName != ClOrd )
    {
        OOSQL_ERR(eBADASTNODE_OOSQL);
    }

    length = 0;
    b1 = AST(node).son;
    b2 = AST(b1).brother;
    while(b1 != AST_NULL)
    {
        if(AST(b1).nodeName == INTEGER)
        {
            int                     integer;
            SelListPoolElements     slpElements(m_pool->selListPool);

            e = getFourFromINTEGER_NODE(integer, b1);
            OOSQL_CHECK_ERR(e);

            integer --;
            if(integer < 0 || selList.size <= integer)
                OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
            slpElements.setPoolIndex(integer, 1);

            olpElements = m_pool->orderByListPool.addNewEntry();
            switch(slpElements[0].selElemKind)
            {
            case SELLIST_KIND_PATHEXPR:
                olpElements[0].ordByKeyKind = ORDERBYLIST_KIND_PATHEXPR;
                olpElements[0].pathExpr     = slpElements[0].pathExpr;
                break;

            case SELLIST_KIND_AGGRFUNC:
                olpElements[0].ordByKeyKind = ORDERBYLIST_KIND_AGGRFUNC;
                olpElements[0].aggrFunc     = slpElements[0].aggrFunc;
                break;

            case SELLIST_KIND_FUNC:
                olpElements[0].ordByKeyKind = ORDERBYLIST_KIND_FUNC;
                olpElements[0].func         = slpElements[0].func;
                break;

            case SELLIST_KIND_VALUE:
            case SELLIST_KIND_OPER:
            case SELLIST_KIND_CONS:
                OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
            }
        }

		/* Distance() function in ORDER BY clause should be handled for kNN */
		else if(AST(AST(b1).son).nodeName == QuOgisMiDistance)
		{
			ASTNodeIdx op1, op2;
			ReturnType op1Type, op2Type, pointType, pathExprType;
			ExprPoolElements epElements(m_pool->exprPool);
			ReturnType returnType;
			ExprPoolIndex exprPoolIndex;
			
			if(length != 0)
			{
				OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
			}
        	returnType.set(TYPEID_NULL, PT_NONE, NULL_POOLINDEX);
			
			op1 = AST(AST(b1).son).son;
			op2 = AST(op1).brother;

			e = smtChkQuery(op1, op1Type);  OOSQL_CHECK_ERR(e);
			e = smtChkQuery(op2, op2Type);  OOSQL_CHECK_ERR(e);

			if(op1Type.typeID == TYPEID_OGIS_POINT && op1Type.poolType == PT_EXPRPOOL && op2Type.poolType == PT_PATHEXPRPOOL)
			{
				pointType = op1Type;
				pathExprType = op2Type;
			}
			else if(op2Type.typeID == TYPEID_OGIS_POINT && op2Type.poolType == PT_EXPRPOOL && op1Type.poolType == PT_PATHEXPRPOOL)
			{
				pointType = op2Type;
				pathExprType = op1Type;
			}
			else
			{
				OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
			}
			
			epElements = m_pool->exprPool.addNewEntry();
			epElements[0].oper.operatorId = OP_OGIS_KNN;
			epElements[0].operandType = EXPR_OPERANDTYPE_CONTAIN_NOMETHOD;
			epElements[0].refCount = 0;

			ExprPoolIndex poolIndex; poolIndex = epElements.getPoolIndex(); e = m_gds->addNodeToUnusedConditionList(m_pool, poolIndex);
			OOSQL_CHECK_ERR(e);
			epElements[0].exprKind = EXPR_KIND_OPER;
			epElements[0].oper.operatorType = OBJECT_OPERATION;

			// make 1st operand node
			e = makeOperandInExprPool(pathExprType, exprPoolIndex);
			epElements[0].oper.operand1 = exprPoolIndex;
			OOSQL_CHECK_ERR(e);

			// make 2nd operand nodes
			e = makeOperandInExprPool(pointType, exprPoolIndex);
			epElements[0].oper.operand2 = exprPoolIndex;
			OOSQL_CHECK_ERR(e);

			epElements[0].oper.operand3.setNull();       // no 3rd operand

			returnType.set(TYPEID_BOOL, PT_EXPRPOOL, epElements.getPoolIndex());

			e = exprElementSetTypeInfo(epElements, returnType);
			OOSQL_CHECK_ERR(e);
        	return eNOERROR;
		}
        else
        {
            e = smtChkQuery(b1, b1Type);
            OOSQL_CHECK_ERR(e);

            olpElements = m_pool->orderByListPool.addNewEntry();

            // fill order by key info
            switch(b1Type.poolType)
            {
            case PT_PATHEXPRPOOL:
                olpElements[0].ordByKeyKind = ORDERBYLIST_KIND_PATHEXPR;
                olpElements[0].pathExpr.setPoolIndex(b1Type.poolIndex);
                break;

            case PT_FUNCPOOL:
                olpElements[0].ordByKeyKind = ORDERBYLIST_KIND_FUNC;
                olpElements[0].func.setPoolIndex(b1Type.poolIndex);
                break;

            case PT_AGGRFUNCPOOL:
                olpElements[0].ordByKeyKind = ORDERBYLIST_KIND_AGGRFUNC;
                olpElements[0].aggrFunc.setPoolIndex(b1Type.poolIndex);
                break;

            default:
                OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
            }
        }
        
        // fill ordering info
        if(AST(b2).nodeName == OrdAsc)
            olpElements[0].asc_or_desc_Flag = SM_TRUE; 
        else if(AST(b2).nodeName == OrdDesc)
            olpElements[0].asc_or_desc_Flag = SM_FALSE;
        else
            OOSQL_ERR(eBADASTNODE_OOSQL);

        b1 = AST(b2).brother;
        if(b1 != AST_NULL)
            b2 = AST(b1).brother;
        else
            b2 = AST_NULL;

        length ++;
    }

	OrderByListPoolIndex orderByList;
	orderByList.setPoolIndex(0, length);
	e = m_gds->setOrderByList(m_pool, orderByList);
	OOSQL_CHECK_ERR(e);

    return eNOERROR;
}

Four OQL_ASTtoGDS::smtChkClLim(
    ASTNodeIdx  node,
    ReturnType& returnType
)
{
    ASTNodeIdx              b1;
    Four                    limitValue1;
    Four                    e;
    LimitClausePoolElements lcElements(m_pool->limitClausePool);
	LimitClausePoolIndex	limitClause;

    if(AST(node).nodeName == null)
    {
        returnType.set(TYPEID_NULL, PT_NONE, NULL_POOLINDEX);
		limitClause.setNull();
		e = m_gds->setLimitClause(m_pool, limitClause);
        return eNOERROR;
    }
    else if (AST(node).nodeName != ClLim)
    {
        OOSQL_ERR(eBADASTNODE_OOSQL);
    }

    b1 = AST(node).son;
	
	e = getFourFromINTEGER_NODE((int&)limitValue1, b1);
	OOSQL_CHECK_ERR(e);

	lcElements = m_pool->limitClausePool.addNewEntry();

	lcElements[0].limitCount = limitValue1;
	limitClause.setPoolIndex(0, 1);
	e = m_gds->setLimitClause(m_pool, limitClause);
	OOSQL_CHECK_ERR(e);

	return eNOERROR;
}


Four OQL_ASTtoGDS::smtChkInterClauseRelationship(ASTNodeIdx, ReturnType& returnType)
{
    // check inter-clause relationship between select, group by, having, order by

    // select, group by
	Four					selListType;
	GroupByListPoolElements groupByList;
	SelListPoolElements		selList;
	TargetListPoolElements	targetList;
	ExprPoolElements		havExprList;
	OrderByListPoolElements orderByList;
	Four					e;

	e = m_gds->getSelListType(m_pool, selListType);
	OOSQL_CHECK_ERR(e);

	e = m_gds->getGroupByList(m_pool, groupByList);
	OOSQL_CHECK_ERR(e);

	e = m_gds->getSelList(m_pool, selList);
	OOSQL_CHECK_ERR(e);

	e = m_gds->getTargetList(m_pool, targetList);
	OOSQL_CHECK_ERR(e);

	e = m_gds->getHavingCond(m_pool, havExprList);
	OOSQL_CHECK_ERR(e);
	e = m_gds->getOrderByList(m_pool, orderByList);
	OOSQL_CHECK_ERR(e);


	  if((selListType & AGGR_BIT) && 
	 	(selListType & (PATH_BIT | STAR_BIT)) &&
		(groupByList.size == 0))

	{
		OOSQL_ERR(eINVALIDSELECT_OOSQL);
	}

	if(groupByList.size > 0 && (selListType & (PATH_BIT | STAR_BIT)))
	{
		Four i, j;

		for(i = 0; i < (selList.size - targetList.size); i++)
		{
			switch(selList[i].selElemKind)
			{
			case SELLIST_KIND_PATHEXPR:
				for(j = 0; j < groupByList.size; j++)
				{
					if(groupByList[j].grpByKeyKind == GROUPBY_KIND_PATHEXPR)
					{
						PathExprPoolElements a(m_pool->pathExprPool), b(m_pool->pathExprPool);

						a = groupByList[j].pathExpr;
						b = selList[i].pathExpr;

						if(isSamePathExpr(a, b))
							break;
					}
				}
				if(j == groupByList.size)
				{
					OOSQL_ERR(eINVALIDSELECT_OOSQL);
				}
				break;
			case SELLIST_KIND_FUNC:
				OOSQL_ERR(eNOTIMPLEMENTED_OOSQL);
				break;
			case SELLIST_KIND_OPER:
				OOSQL_ERR(eNOTIMPLEMENTED_OOSQL);
				break;
			}
		}
	}
	if (groupByList.size > 0){
		Four i, j;
		for (i = 0; i < havExprList.size; i++)
		{
			ExprPoolElements op1(m_pool->exprPool), op2(m_pool->exprPool);

			op1 = havExprList[i].oper.operand1;
			op2 = havExprList[i].oper.operand2;

			if (op1[0].exprKind == EXPR_KIND_PATHEXPR)
			{
				for(j = 0; j < groupByList.size; j++)
				{
					if(groupByList[j].grpByKeyKind == GROUPBY_KIND_PATHEXPR)
					{
						PathExprPoolElements a(m_pool->pathExprPool), b(m_pool->pathExprPool);
						
						a = groupByList[j].pathExpr;
						b = op1[0].pathExpr;
					
						if(isSamePathExpr(a, b))
							break;
					}
				}

				if(j == groupByList.size)
				{
					OOSQL_ERR(eEXPRERR_HAVING_OOSQL);
				}
			}
			if (op2[0].exprKind == EXPR_KIND_PATHEXPR)
			{
				
				for(j = 0; j < groupByList.size; j++)
				{
					if(groupByList[j].grpByKeyKind == GROUPBY_KIND_PATHEXPR)
					{
						PathExprPoolElements a(m_pool->pathExprPool), b(m_pool->pathExprPool);
						
						a = groupByList[j].pathExpr;
						b = op2[0].pathExpr;
						
						if(isSamePathExpr(a, b))
							break;
					}
				}
				if(j == groupByList.size)
				{
					OOSQL_ERR(eEXPRERR_HAVING_OOSQL);
				}
			}
		}
	}

    if (groupByList.size > 0){
		Four i, j;
		for (i = 0; i < orderByList.size; i++)
		{
			switch(orderByList[i].ordByKeyKind)
			{
			case ORDERBYLIST_KIND_PATHEXPR:
				for(j = 0; j < groupByList.size; j++)
				{
					if(groupByList[j].grpByKeyKind == GROUPBY_KIND_PATHEXPR)
					{
						PathExprPoolElements a(m_pool->pathExprPool), b(m_pool->pathExprPool);
						
						a = groupByList[j].pathExpr;
						b = orderByList[i].pathExpr;
						
						if(isSamePathExpr(a, b))
							break;
					}
				}
				if(j == groupByList.size)
				{
					OOSQL_ERR(eEXPRERR_HAVING_OOSQL);
				}
				
			case ORDERBYLIST_KIND_AGGRFUNC:
			case ORDERBYLIST_KIND_FUNC:
				break;
			}
		}
	}

    return eNOERROR;
}

Four OQL_ASTtoGDS::smtChkQuSet( 
    ASTNodeIdx  node,
    ReturnType& returnType 
)
{
    ASTNodeIdx          s;
    ASTNodeIdx          op1;
    ASTNodeIdx          op2;
    ReturnType          op1Type;
    ReturnType          op2Type;
    ExprPoolElements    epElements(m_pool->exprPool);
    Four                e;
    Boolean             flag;
    OperatorType        operatorType;
    ExprPoolIndex       exprPoolIndex;

    s = AST(node).son;

    switch ( AST(s).nodeName ) 
    {
    case OpSetInt:
    case OpSetUni:
    case OpSetExc:
        op1 = AST(s).brother;
        op2 = AST(op1).brother;
            
        e = smtChkQuery(op1, op1Type);  OOSQL_CHECK_ERR(e);
        e = smtChkQuery(op2, op2Type);  OOSQL_CHECK_ERR(e);
            
        // check if operands are SET
		if(op1Type.typeID == TYPEID_ID)
		{
			m_errorMessage->Clear();
			m_errorMessage->Append("Undefined Identifier is used at ");
			ERRORMESSAGE_APPEND_POSITION(m_errorMessage, op1);

			OOSQL_ERR(eSYNTAX_ERROR_OOSQL)
		}
        if(!(OOSQL_MASK_COMPLEXTYPE(op1Type.typeID) == COMPLEXTYPEID_SET ||
			 OOSQL_MASK_COMPLEXTYPE(op1Type.typeID) == COMPLEXTYPEID_BAG ||
			 OOSQL_MASK_COMPLEXTYPE(op1Type.typeID) == COMPLEXTYPEID_LIST ||
			 op1Type.typeID == TYPEID_COMPLEX))
		{
			m_errorMessage->Clear();
			m_errorMessage->Append("The type of the value used at ");
			ERRORMESSAGE_APPEND_POSITION(m_errorMessage, op1);
			m_errorMessage->Append(" is not a set where one of those is expected.");

            OOSQL_ERR(eTYPE_ERROR_OOSQL);
		}

		if(op2Type.typeID == TYPEID_ID)
		{
			m_errorMessage->Clear();
			m_errorMessage->Append("Undefined Identifier is used at ");
			ERRORMESSAGE_APPEND_POSITION(m_errorMessage, op2);

			OOSQL_ERR(eSYNTAX_ERROR_OOSQL)
		}
        if(!(OOSQL_MASK_COMPLEXTYPE(op2Type.typeID) == COMPLEXTYPEID_SET ||
			 OOSQL_MASK_COMPLEXTYPE(op2Type.typeID) == COMPLEXTYPEID_BAG ||
			 OOSQL_MASK_COMPLEXTYPE(op2Type.typeID) == COMPLEXTYPEID_LIST ||
			 op2Type.typeID == TYPEID_COMPLEX))
		{
			m_errorMessage->Clear();
			m_errorMessage->Append("The type of the value used at ");
			ERRORMESSAGE_APPEND_POSITION(m_errorMessage, op2);
			m_errorMessage->Append(" is not a set where one of those is expected.");

            OOSQL_ERR(eTYPE_ERROR_OOSQL);
		}

        // check if element of each set has the same type
		if(!(op1Type.typeID == TYPEID_COMPLEX || op2Type.typeID == TYPEID_COMPLEX))
			if(!(OOSQL_MASK_TYPE(op1Type.typeID) == OOSQL_MASK_TYPE(op2Type.typeID)))
				OOSQL_ERR(eTYPE_ERROR_OOSQL);

        operatorType = COLLECTION_OPERATION;

        break;

    default:
        OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
    }

    // check type compatibility between op1 and op2
    e = testCompatibility(op1Type.typeID, op2Type.typeID, flag);
    OOSQL_CHECK_ERR(e);
    if(flag == SM_FALSE)
	{
		m_errorMessage->Clear();
		m_errorMessage->Append("The type of the value used at ");
		ERRORMESSAGE_APPEND_POSITION(m_errorMessage, op1);
		m_errorMessage->Append(" is not compatible with the type of the value used at ");
		ERRORMESSAGE_APPEND_POSITION(m_errorMessage, op2);
		m_errorMessage->Append(".");

        OOSQL_ERR(eTYPE_ERROR_OOSQL);
    }

    // construct operator node
    epElements = m_pool->exprPool.addNewEntry();

    epElements[0].exprKind      = EXPR_KIND_OPER;
    e = nodeNameToOperatorID(AST(s).nodeName, epElements[0].oper.operatorId);
    OOSQL_CHECK_ERR(e);
    epElements[0].oper.operatorType = operatorType;
    epElements[0].operandType       = EXPR_OPERANDTYPE_CONTAIN_NOMETHOD;
    epElements[0].refCount          = 0;
    
    // make operand nodes;
    e = makeOperandInExprPool(op1Type, exprPoolIndex);
    epElements[0].oper.operand1 = exprPoolIndex;
    OOSQL_CHECK_ERR(e);
    e = makeOperandInExprPool(op2Type, exprPoolIndex);
    epElements[0].oper.operand2 = exprPoolIndex;
    OOSQL_CHECK_ERR(e);
	epElements[0].oper.operand3.setNull();       // no third operand

	if(op1Type.typeID == TYPEID_COMPLEX && op2Type.typeID == TYPEID_COMPLEX)
		returnType.set(TYPEID_COMPLEX, PT_EXPRPOOL, epElements.getPoolIndex());
	else if(op1Type.typeID == TYPEID_COMPLEX)
		returnType.set(op2Type.typeID, PT_EXPRPOOL, epElements.getPoolIndex());
	else
		returnType.set(op1Type.typeID, PT_EXPRPOOL, epElements.getPoolIndex());
    returnType.elementTypeID = op1Type.elementTypeID;

    e = exprElementSetTypeInfo(epElements, returnType);
    OOSQL_CHECK_ERR(e);

    return eNOERROR;
}

Four OQL_ASTtoGDS::smtChkQuCnvL2s( 
    ASTNodeIdx  node,
    ReturnType& returnType 
)
{

    OOSQL_ERR(eNOTIMPLEMENTED_OOSQL);
}

Four OQL_ASTtoGDS::smtChkQuCnvElem( 
    ASTNodeIdx  node,
    ReturnType& returnType 
)
{

    OOSQL_ERR(eNOTIMPLEMENTED_OOSQL);   
}

Four OQL_ASTtoGDS::smtChkQuCnvDist( 
    ASTNodeIdx  node,
    ReturnType& returnType 
)
{

    OOSQL_ERR(eNOTIMPLEMENTED_OOSQL);
}

Four OQL_ASTtoGDS::smtChkQuCnvFlat( 
    ASTNodeIdx  node,
    ReturnType& returnType 
)
{

    OOSQL_ERR(eNOTIMPLEMENTED_OOSQL);
}

Four OQL_ASTtoGDS::smtChkQuCnvType( 
    ASTNodeIdx  node,
    ReturnType& returnType 
)
{

    OOSQL_ERR(eNOTIMPLEMENTED_OOSQL);
}

Four OQL_ASTtoGDS::smtChkID(
    ASTNodeIdx  node,
    ReturnType& returnType
)
{
    //1. from variable name
    //2. defined query name
    //3. named object
    //4. class name
    OOSQL_ERR(eNOTIMPLEMENTED_OOSQL);
}

Four OQL_ASTtoGDS::isOGISTypeID(
	TypeID typeID
)
{
    if (typeID == TYPEID_OGIS_GEOMETRY				||
        typeID == TYPEID_OGIS_POINT					||
        typeID == TYPEID_OGIS_LINESTRING			||
        typeID == TYPEID_OGIS_POLYGON				||
        typeID == TYPEID_OGIS_GEOMETRYCOLLECTION	||
        typeID == TYPEID_OGIS_MULTIPOINT			||
        typeID == TYPEID_OGIS_MULTILINESTRING		||
        typeID == TYPEID_OGIS_MULTIPOLYGON)
        return SM_TRUE;
    else
    	return SM_FALSE;
}

Four OQL_ASTtoGDS::smtChkQuOgisTranslatableOp( 
    ASTNodeIdx  node,
    ReturnType& returnType 
)
{
    ASTNodeIdx          s;
    ASTNodeIdx          op1;
    ASTNodeIdx          op2;
    ReturnType          op1Type;
    ReturnType          op2Type;
    ExprPoolElements    epElements(m_pool->exprPool);
    ExprPoolIndex       oldConnectionPoint;
    Four                e;
    Boolean             flag;
    OperatorType        operatorType;
    ExprPoolIndex       exprPoolIndex;

#ifdef SLIMDOWN_OPENGIS
    OOSQL_ERR(eOPENGIS_NOTENABLED_OOSQL);
#endif

    s = AST(node).son;

    // construct operator node
    epElements = m_pool->exprPool.addNewEntry();
    epElements[0].operandType = EXPR_OPERANDTYPE_CONTAIN_NOMETHOD;
    epElements[0].refCount    = 0;

    switch ( AST(s).nodeName ) 
    {
    case QuOgisTrGeometryFT:
    case QuOgisTrPointFT:
    case QuOgisTrLineStringFT:
    case QuOgisTrPolygonFT:
    case QuOgisTrMultiPointFT:
    case QuOgisTrMultiLineStringFT:
    case QuOgisTrMultiPolygonFT:
    case QuOgisTrGeometryCollectionFT:
    case QuOgisTrGeometryFB:
    case QuOgisTrPointFB:
    case QuOgisTrLineStringFB:
    case QuOgisTrPolygonFB:
    case QuOgisTrMultiPointFB:
    case QuOgisTrMultiLineStringFB:
    case QuOgisTrMultiPolygonFB:
    case QuOgisTrGeometryCollectionFB:
        op1 = AST(s).son;
        op2 = AST(op1).brother;

        e = smtChkQuery(op1, op1Type);  OOSQL_CHECK_ERR(e);
        e = smtChkQuery(op2, op2Type);  OOSQL_CHECK_ERR(e);

		if(op1Type.typeID != TYPEID_STRING)
		{
			m_errorMessage->Clear();
			m_errorMessage->Append("Op1 must be string: at ");
			ERRORMESSAGE_APPEND_POSITION(m_errorMessage, op1);

			OOSQL_ERR(eSYNTAX_ERROR_OOSQL)
		}

		if(op2Type.typeID != TYPEID_INT)
		{
			m_errorMessage->Clear();
			m_errorMessage->Append("Op2 must be integer: at ");
			ERRORMESSAGE_APPEND_POSITION(m_errorMessage, op2);

			OOSQL_ERR(eSYNTAX_ERROR_OOSQL)
		}
        operatorType = OBJECT_OPERATION;
        break;
    case QuOgisTrAsText:
    case QuOgisTrAsBinary:
        op1 = AST(s).son;

        e = smtChkQuery(op1, op1Type);  OOSQL_CHECK_ERR(e);

		if(isOGISTypeID(op1Type.typeID) == SM_FALSE)
		{
			m_errorMessage->Clear();
			m_errorMessage->Append("Op1 must be OGIS type: at ");
			ERRORMESSAGE_APPEND_POSITION(m_errorMessage, op1);

			OOSQL_ERR(eSYNTAX_ERROR_OOSQL)
		}
        operatorType = OBJECT_OPERATION;
        break;

    default:
        OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
    }

    epElements[0].exprKind      = EXPR_KIND_OPER;
    e = nodeNameToOperatorID(AST(s).nodeName, epElements[0].oper.operatorId);
    OOSQL_CHECK_ERR(e);
    epElements[0].oper.operatorType = operatorType;
    #ifdef ENABLE_OPENGIS_OPTIMIZATION
	epElements[0].oper.isDeterministic = SM_TRUE; // Be all OGIS Operator deterministic
    #endif    

    // make 1st operand node
    e = makeOperandInExprPool(op1Type, exprPoolIndex);
    epElements[0].oper.operand1 = exprPoolIndex;
    OOSQL_CHECK_ERR(e);
    
    // make 2nd operand nodes
    
    if(AST(s).nodeName == QuOgisTrAsText || AST(s).nodeName == QuOgisTrAsBinary)
    {
    	epElements[0].oper.operand2.setNull();       // no 2nd operand
    }
    else
    {
    	e = makeOperandInExprPool(op2Type, exprPoolIndex);
    	epElements[0].oper.operand2 = exprPoolIndex;
    	OOSQL_CHECK_ERR(e);
    }
  	
  	epElements[0].oper.operand3.setNull();       // no 3rd operand

    #ifdef ENABLE_OPENGIS_OPTIMIZATION
    /*
       If this UDF is DETERMINISTIC, check whether all arguments are constant or not.
       If all arguments are constant, set isConstant flag of func[0].
    */
    OperatorStruct nestedOp;
    if(epElements[0].oper.isDeterministic == SM_TRUE)
    {
        ExprPoolElements    nestedExpr(m_pool->exprPool);
        Boolean isAllArgumentConstant;
        isAllArgumentConstant = SM_TRUE;
		// for 1 st op
		nestedExpr = epElements[0].oper.operand1.getElements(m_pool->exprPool);
        if(!nestedExpr.isNull())
        {
            if(nestedExpr[0].exprKind == EXPR_KIND_OPER)
            {
                if (nestedExpr[0].oper.isDeterministic != SM_TRUE)
                {
                    isAllArgumentConstant = SM_FALSE;
                }
			}
            else if(nestedExpr[0].exprKind == EXPR_KIND_PATHEXPR || nestedExpr[0].exprKind == EXPR_KIND_CONS ||
                    nestedExpr[0].exprKind == EXPR_KIND_AGGRFUNC || nestedExpr[0].exprKind == EXPR_KIND_FUNCTION )
            {
                isAllArgumentConstant = SM_FALSE;
            }
        }
		// for 2 nd op
		nestedExpr = epElements[0].oper.operand2.getElements(m_pool->exprPool);
        if(!nestedExpr.isNull())
        {
            if(nestedExpr[0].exprKind == EXPR_KIND_OPER)
            {
                if (nestedExpr[0].oper.isDeterministic != SM_TRUE)
                {
                    isAllArgumentConstant = SM_FALSE;
                }
			}
            else if(nestedExpr[0].exprKind == EXPR_KIND_PATHEXPR || nestedExpr[0].exprKind == EXPR_KIND_CONS ||
                    nestedExpr[0].exprKind == EXPR_KIND_AGGRFUNC || nestedExpr[0].exprKind == EXPR_KIND_FUNCTION )
            {
                isAllArgumentConstant = SM_FALSE;
            }
        }
		// for 3 rd op
		nestedExpr = epElements[0].oper.operand3.getElements(m_pool->exprPool);
        if(!nestedExpr.isNull())
        {
            if(nestedExpr[0].exprKind == EXPR_KIND_OPER)
            {
                if (nestedExpr[0].oper.isDeterministic != SM_TRUE)
                {
                    isAllArgumentConstant = SM_FALSE;
                }
			}
            else if(nestedExpr[0].exprKind == EXPR_KIND_PATHEXPR || nestedExpr[0].exprKind == EXPR_KIND_CONS ||
                    nestedExpr[0].exprKind == EXPR_KIND_AGGRFUNC || nestedExpr[0].exprKind == EXPR_KIND_FUNCTION )
            {
                isAllArgumentConstant = SM_FALSE;
            }
        }

        if (isAllArgumentConstant == SM_TRUE)
            epElements[0].oper.isConstant = SM_TRUE;
        else    epElements[0].oper.isConstant = SM_FALSE;
    }
    else
    {
        epElements[0].oper.isConstant = SM_FALSE;
    }

    epElements[0].oper.UDFNo = m_currentUDFNo;
	m_currentUDFNo++;
    #endif

  	switch ( AST(s).nodeName ) 
    {
    case QuOgisTrGeometryFT:
    case QuOgisTrGeometryFB:
        returnType.set(TYPEID_OGIS_GEOMETRY, PT_EXPRPOOL, epElements.getPoolIndex());
        break;
    
    case QuOgisTrPointFT:
    case QuOgisTrPointFB:
        returnType.set(TYPEID_OGIS_POINT, PT_EXPRPOOL, epElements.getPoolIndex());
        break;
    
    case QuOgisTrLineStringFT:
    case QuOgisTrLineStringFB:
        returnType.set(TYPEID_OGIS_LINESTRING, PT_EXPRPOOL, epElements.getPoolIndex());
        break;
    
    case QuOgisTrPolygonFT:
    case QuOgisTrPolygonFB:
        returnType.set(TYPEID_OGIS_POLYGON, PT_EXPRPOOL, epElements.getPoolIndex());
        break;
    
    case QuOgisTrMultiPointFT:
    case QuOgisTrMultiPointFB:
        returnType.set(TYPEID_OGIS_MULTIPOINT, PT_EXPRPOOL, epElements.getPoolIndex());
        break;
    
    case QuOgisTrMultiLineStringFT:
    case QuOgisTrMultiLineStringFB:
        returnType.set(TYPEID_OGIS_MULTILINESTRING, PT_EXPRPOOL, epElements.getPoolIndex());
        break;
    
    case QuOgisTrMultiPolygonFT:
    case QuOgisTrMultiPolygonFB:
        returnType.set(TYPEID_OGIS_MULTIPOLYGON, PT_EXPRPOOL, epElements.getPoolIndex());
        break;
    
    case QuOgisTrGeometryCollectionFT:
    case QuOgisTrGeometryCollectionFB:
        returnType.set(TYPEID_OGIS_GEOMETRYCOLLECTION, PT_EXPRPOOL, epElements.getPoolIndex());
        break;
    case QuOgisTrAsText:
    case QuOgisTrAsBinary:
        returnType.set(TYPEID_STRING, PT_EXPRPOOL, epElements.getPoolIndex());
        break;
        
    default:
        OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
    }
    
    e = exprElementSetTypeInfo(epElements, returnType);
    OOSQL_CHECK_ERR(e);

    return eNOERROR;
}


Four OQL_ASTtoGDS::smtChkQuOgisGeometricOp( 
    ASTNodeIdx  node,
    ReturnType& returnType 
)
{
    ASTNodeIdx          s;
    ASTNodeIdx          op1;
    ASTNodeIdx          op2;
    ReturnType          op1Type;
    ReturnType          op2Type;
    ExprPoolElements    epElements(m_pool->exprPool);
    ExprPoolIndex       oldConnectionPoint;
    Four                e;
    Boolean             flag;
    OperatorType        operatorType;
    ExprPoolIndex       exprPoolIndex;

#ifdef SLIMDOWN_OPENGIS
    OOSQL_ERR(eOPENGIS_NOTENABLED_OOSQL);
#endif

    s = AST(node).son;

    // construct operator node
    epElements = m_pool->exprPool.addNewEntry();
    epElements[0].operandType = EXPR_OPERANDTYPE_CONTAIN_NOMETHOD;
    epElements[0].refCount    = 0;

    op1 = AST(s).son;

    e = smtChkQuery(op1, op1Type);  OOSQL_CHECK_ERR(e);

    if(isOGISTypeID(op1Type.typeID) == SM_FALSE)
    {
        m_errorMessage->Clear();
        m_errorMessage->Append("Op1 must be OGIS type: at ");
        ERRORMESSAGE_APPEND_POSITION(m_errorMessage, op1);

        OOSQL_ERR(eSYNTAX_ERROR_OOSQL)
    }

        operatorType = OBJECT_OPERATION;

    epElements[0].exprKind      = EXPR_KIND_OPER;
    e = nodeNameToOperatorID(AST(s).nodeName, epElements[0].oper.operatorId);
    OOSQL_CHECK_ERR(e);
    epElements[0].oper.operatorType = operatorType;
    #ifdef ENABLE_OPENGIS_OPTIMIZATION
	epElements[0].oper.isDeterministic = SM_TRUE; // Be all OGIS Operator deterministic
    #endif    

    // make 1st operand node
    e = makeOperandInExprPool(op1Type, exprPoolIndex);
    epElements[0].oper.operand1 = exprPoolIndex;
    OOSQL_CHECK_ERR(e);

    epElements[0].oper.operand2.setNull();       // no 2nd operand
    epElements[0].oper.operand3.setNull();       // no 3rd operand

    #ifdef ENABLE_OPENGIS_OPTIMIZATION
    /*
       If this UDF is DETERMINISTIC, check whether all arguments are constant or not.
       If all arguments are constant, set isConstant flag of func[0].
    */
    OperatorStruct nestedOp;
    if(epElements[0].oper.isDeterministic == SM_TRUE)
    {
        ExprPoolElements    nestedExpr(m_pool->exprPool);
        Boolean isAllArgumentConstant;
        isAllArgumentConstant = SM_TRUE;
		// for 1 st op
		nestedExpr = epElements[0].oper.operand1.getElements(m_pool->exprPool);
        if(!nestedExpr.isNull())
        {
            if(nestedExpr[0].exprKind == EXPR_KIND_OPER)
            {
                if (nestedExpr[0].oper.isDeterministic != SM_TRUE)
                {
                    isAllArgumentConstant = SM_FALSE;
                }
			}
            else if(nestedExpr[0].exprKind == EXPR_KIND_PATHEXPR || nestedExpr[0].exprKind == EXPR_KIND_CONS ||
                    nestedExpr[0].exprKind == EXPR_KIND_AGGRFUNC || nestedExpr[0].exprKind == EXPR_KIND_FUNCTION )
            {
                isAllArgumentConstant = SM_FALSE;
            }
        }
		// for 2 nd op
		nestedExpr = epElements[0].oper.operand2.getElements(m_pool->exprPool);
        if(!nestedExpr.isNull())
        {
            if(nestedExpr[0].exprKind == EXPR_KIND_OPER)
            {
                if (nestedExpr[0].oper.isDeterministic != SM_TRUE)
                {
                    isAllArgumentConstant = SM_FALSE;
                }
			}
            else if(nestedExpr[0].exprKind == EXPR_KIND_PATHEXPR || nestedExpr[0].exprKind == EXPR_KIND_CONS ||
                    nestedExpr[0].exprKind == EXPR_KIND_AGGRFUNC || nestedExpr[0].exprKind == EXPR_KIND_FUNCTION )
            {
                isAllArgumentConstant = SM_FALSE;
            }
        }
		// for 3 rd op
		nestedExpr = epElements[0].oper.operand3.getElements(m_pool->exprPool);
        if(!nestedExpr.isNull())
        {
            if(nestedExpr[0].exprKind == EXPR_KIND_OPER)
            {
                if (nestedExpr[0].oper.isDeterministic != SM_TRUE)
                {
                    isAllArgumentConstant = SM_FALSE;
                }
			}
            else if(nestedExpr[0].exprKind == EXPR_KIND_PATHEXPR || nestedExpr[0].exprKind == EXPR_KIND_CONS ||
                    nestedExpr[0].exprKind == EXPR_KIND_AGGRFUNC || nestedExpr[0].exprKind == EXPR_KIND_FUNCTION )
            {
                isAllArgumentConstant = SM_FALSE;
            }
        }

        if (isAllArgumentConstant == SM_TRUE)
            epElements[0].oper.isConstant = SM_TRUE;
        else    epElements[0].oper.isConstant = SM_FALSE;
    }
    else
    {
        epElements[0].oper.isConstant = SM_FALSE;
    }

    epElements[0].oper.UDFNo = m_currentUDFNo;
	m_currentUDFNo++;
    #endif

    switch ( AST(s).nodeName )
    {
    case QuOgisGeDimension:
    case QuOgisGeSRID:
    case QuOgisGeNumGeometries:
    case QuOgisGeNumPoints:
    case QuOgisGeNumInteriorRings:
        returnType.set(TYPEID_INT, PT_EXPRPOOL, epElements.getPoolIndex());
        break;
    
    case QuOgisGeLength:
    case QuOgisGeX:
    case QuOgisGeY:
    case QuOgisGeArea:
        returnType.set(TYPEID_FLOAT, PT_EXPRPOOL, epElements.getPoolIndex());
        break;
    
    case QuOgisGeGeometryType:
        returnType.set(TYPEID_STRING, PT_EXPRPOOL, epElements.getPoolIndex());
        break;
    
    case QuOgisGeBoundary:
        returnType.set(TYPEID_OGIS_GEOMETRY, PT_EXPRPOOL, epElements.getPoolIndex());
        break;
        
    default:
        OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
    }
    
    e = exprElementSetTypeInfo(epElements, returnType);
    OOSQL_CHECK_ERR(e);

    return eNOERROR;
}


Four OQL_ASTtoGDS::smtChkQuOgisRelationalOp( 
    ASTNodeIdx  node,
    ReturnType& returnType 
)
{
    ASTNodeIdx          s;
    ASTNodeIdx          op1;
    ASTNodeIdx          op2;
    ASTNodeIdx          op3;
    ReturnType          op1Type;
    ReturnType          op2Type;
    ReturnType          op3Type;
    ExprPoolElements    epElements(m_pool->exprPool);
    ExprPoolIndex       oldConnectionPoint;
    Four                e;
    Boolean             flag;
    OperatorType        operatorType;
    ExprPoolIndex       exprPoolIndex;

#ifdef SLIMDOWN_OPENGIS
    OOSQL_ERR(eOPENGIS_NOTENABLED_OOSQL);
#endif

    s = AST(node).son;

    // construct operator node
    epElements = m_pool->exprPool.addNewEntry();
    epElements[0].operandType = EXPR_OPERANDTYPE_CONTAIN_NOMETHOD;
    epElements[0].refCount    = 0;

    oldConnectionPoint = m_connectionPoint;
    if(m_connectionPoint == NULL_POOLINDEX)
	{
        m_connectionPoint   = epElements.getPoolIndex();
		m_conditionNotBound = SM_TRUE;
	}

    switch ( AST(s).nodeName ) 
    {
    case QuOgisReContains:
    case QuOgisReCrosses:
    case QuOgisReDisjoint:
    case QuOgisReEquals:
    case QuOgisReIntersects:
    case QuOgisReOverlaps:
    case QuOgisReTouches:
    case QuOgisReWithin:

        op1 = AST(s).son;
        op2 = AST(op1).brother;

        e = smtChkQuery(op1, op1Type);  OOSQL_CHECK_ERR(e);
        e = smtChkQuery(op2, op2Type);  OOSQL_CHECK_ERR(e);

		if(isOGISTypeID(op1Type.typeID) == SM_FALSE)
		{
			m_errorMessage->Clear();
			m_errorMessage->Append("Op1 must be OGIS type: at ");
			ERRORMESSAGE_APPEND_POSITION(m_errorMessage, op1);

			OOSQL_ERR(eSYNTAX_ERROR_OOSQL)
		}

		if(isOGISTypeID(op2Type.typeID) == SM_FALSE)
		{
			m_errorMessage->Clear();
			m_errorMessage->Append("Op2 must be OGIS type: at ");
			ERRORMESSAGE_APPEND_POSITION(m_errorMessage, op2);

			OOSQL_ERR(eSYNTAX_ERROR_OOSQL)
		}
        operatorType = OBJECT_OPERATION;
        break;
        
    case QuOgisReIsEmpty:
    case QuOgisReIsSimple:
    case QuOgisReIsClosed:
    case QuOgisReIsRing:

        op1 = AST(s).son;

        e = smtChkQuery(op1, op1Type);  OOSQL_CHECK_ERR(e);

		if(isOGISTypeID(op1Type.typeID) == SM_FALSE)
		{
			m_errorMessage->Clear();
			m_errorMessage->Append("Op1 must be OGIS type: at ");
			ERRORMESSAGE_APPEND_POSITION(m_errorMessage, op1);

			OOSQL_ERR(eSYNTAX_ERROR_OOSQL)
		}
        operatorType = OBJECT_OPERATION;
        break;

    case QuOgisReRelated:

        op1 = AST(s).son;
        op2 = AST(op1).brother;
        op3 = AST(op2).brother;

        e = smtChkQuery(op1, op1Type);  OOSQL_CHECK_ERR(e);
        e = smtChkQuery(op2, op2Type);  OOSQL_CHECK_ERR(e);
        e = smtChkQuery(op3, op3Type);  OOSQL_CHECK_ERR(e);

		if(isOGISTypeID(op1Type.typeID) == SM_FALSE)
		{
			m_errorMessage->Clear();
			m_errorMessage->Append("Op1 must be OGIS type: at ");
			ERRORMESSAGE_APPEND_POSITION(m_errorMessage, op1);

			OOSQL_ERR(eSYNTAX_ERROR_OOSQL)
		}

		if(isOGISTypeID(op2Type.typeID) == SM_FALSE)
		{
			m_errorMessage->Clear();
			m_errorMessage->Append("Op2 must be OGIS type: at ");
			ERRORMESSAGE_APPEND_POSITION(m_errorMessage, op2);

			OOSQL_ERR(eSYNTAX_ERROR_OOSQL)
		}
		
		if(op3Type.typeID != TYPEID_STRING)
		{
			m_errorMessage->Clear();
			m_errorMessage->Append("Op3 must be string type: at ");
			ERRORMESSAGE_APPEND_POSITION(m_errorMessage, op3);

			OOSQL_ERR(eSYNTAX_ERROR_OOSQL)
		}
		
        operatorType = OBJECT_OPERATION;
        break;
        

    default:
        OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
    }

	if(m_connectionPoint!= NULL_POOLINDEX && oldConnectionPoint == NULL_POOLINDEX && m_conditionNotBound && m_progressStatus != HAVING_CLAUSE)
	{
		e = m_gds->addNodeToUnusedConditionList(m_pool, m_connectionPoint);
		OOSQL_CHECK_ERR(e);
	}
    m_connectionPoint = oldConnectionPoint;

    epElements[0].exprKind      = EXPR_KIND_OPER;
    e = nodeNameToOperatorID(AST(s).nodeName, epElements[0].oper.operatorId);
    OOSQL_CHECK_ERR(e);
    epElements[0].oper.operatorType = operatorType;
    #ifdef ENABLE_OPENGIS_OPTIMIZATION
	epElements[0].oper.isDeterministic = SM_TRUE; // Be all OGIS Operator deterministic
    #endif    

    // make 1st operand node
    e = makeOperandInExprPool(op1Type, exprPoolIndex);
    epElements[0].oper.operand1 = exprPoolIndex;
    OOSQL_CHECK_ERR(e);
    
    // make 2nd operand nodes
    
    if( AST(s).nodeName == QuOgisReIsEmpty 
    	|| AST(s).nodeName == QuOgisReIsSimple
    	|| AST(s).nodeName == QuOgisReIsClosed
    	|| AST(s).nodeName == QuOgisReIsRing)
    { 
    	epElements[0].oper.operand2.setNull();       // no 2nd operand
    }
    else
    {
    	e = makeOperandInExprPool(op2Type, exprPoolIndex);
    	epElements[0].oper.operand2 = exprPoolIndex;
    	OOSQL_CHECK_ERR(e);
    }
    
    if( AST(s).nodeName != QuOgisReRelated)
    {
    	epElements[0].oper.operand3.setNull();       // no 3rd operand
    }
    else
    {
    	e = makeOperandInExprPool(op3Type, exprPoolIndex);
    	epElements[0].oper.operand3 = exprPoolIndex;
    	OOSQL_CHECK_ERR(e);
    }

    #ifdef ENABLE_OPENGIS_OPTIMIZATION
    /*
       If this UDF is DETERMINISTIC, check whether all arguments are constant or not.
       If all arguments are constant, set isConstant flag of func[0].
    */
    OperatorStruct nestedOp;
    if(epElements[0].oper.isDeterministic == SM_TRUE)
    {
        ExprPoolElements    nestedExpr(m_pool->exprPool);
        Boolean isAllArgumentConstant;
        isAllArgumentConstant = SM_TRUE;
		// for 1 st op
		nestedExpr = epElements[0].oper.operand1.getElements(m_pool->exprPool);
        if(!nestedExpr.isNull())
        {
            if(nestedExpr[0].exprKind == EXPR_KIND_OPER)
            {
                if (nestedExpr[0].oper.isDeterministic != SM_TRUE)
                {
                    isAllArgumentConstant = SM_FALSE;
                }
			}
            else if(nestedExpr[0].exprKind == EXPR_KIND_PATHEXPR || nestedExpr[0].exprKind == EXPR_KIND_CONS ||
                    nestedExpr[0].exprKind == EXPR_KIND_AGGRFUNC || nestedExpr[0].exprKind == EXPR_KIND_FUNCTION )
            {
                isAllArgumentConstant = SM_FALSE;
            }
        }
		// for 2 nd op
		nestedExpr = epElements[0].oper.operand2.getElements(m_pool->exprPool);
        if(!nestedExpr.isNull())
        {
            if(nestedExpr[0].exprKind == EXPR_KIND_OPER)
            {
                if (nestedExpr[0].oper.isDeterministic != SM_TRUE)
                {
                    isAllArgumentConstant = SM_FALSE;
                }
			}
            else if(nestedExpr[0].exprKind == EXPR_KIND_PATHEXPR || nestedExpr[0].exprKind == EXPR_KIND_CONS ||
                    nestedExpr[0].exprKind == EXPR_KIND_AGGRFUNC || nestedExpr[0].exprKind == EXPR_KIND_FUNCTION )
            {
                isAllArgumentConstant = SM_FALSE;
            }
        }
		// for 3 rd op
		nestedExpr = epElements[0].oper.operand3.getElements(m_pool->exprPool);
        if(!nestedExpr.isNull())
        {
            if(nestedExpr[0].exprKind == EXPR_KIND_OPER)
            {
                if (nestedExpr[0].oper.isDeterministic != SM_TRUE)
                {
                    isAllArgumentConstant = SM_FALSE;
                }
			}
            else if(nestedExpr[0].exprKind == EXPR_KIND_PATHEXPR || nestedExpr[0].exprKind == EXPR_KIND_CONS ||
                    nestedExpr[0].exprKind == EXPR_KIND_AGGRFUNC || nestedExpr[0].exprKind == EXPR_KIND_FUNCTION )
            {
                isAllArgumentConstant = SM_FALSE;
            }
        }

        if (isAllArgumentConstant == SM_TRUE)
            epElements[0].oper.isConstant = SM_TRUE;
        else    epElements[0].oper.isConstant = SM_FALSE;
    }
    else
    {
        epElements[0].oper.isConstant = SM_FALSE;
    }

    epElements[0].oper.UDFNo = m_currentUDFNo;
	m_currentUDFNo++;
    #endif
  	
    returnType.set(TYPEID_BOOL, PT_EXPRPOOL, epElements.getPoolIndex());
    
    e = exprElementSetTypeInfo(epElements, returnType);
    OOSQL_CHECK_ERR(e);

    return eNOERROR;
}



Four OQL_ASTtoGDS::smtChkQuOgisMiscellaneousOp( 
    ASTNodeIdx  node,
    ReturnType& returnType 
)
{
    ASTNodeIdx          s;
    ASTNodeIdx          op1;
    ASTNodeIdx          op2;
    ReturnType          op1Type;
    ReturnType          op2Type;
    ExprPoolElements    epElements(m_pool->exprPool);
    ExprPoolIndex       oldConnectionPoint;
    Four                e;
    Boolean             flag;
    OperatorType        operatorType;
    ExprPoolIndex       exprPoolIndex;

#ifdef SLIMDOWN_OPENGIS
    OOSQL_ERR(eOPENGIS_NOTENABLED_OOSQL);
#endif

    s = AST(node).son;

    // construct operator node
    epElements = m_pool->exprPool.addNewEntry();
    epElements[0].operandType = EXPR_OPERANDTYPE_CONTAIN_NOMETHOD;
    epElements[0].refCount    = 0;

    switch ( AST(s).nodeName )
    {
    case QuOgisMiDifference:
    case QuOgisMiIntersection:
    case QuOgisMiSymDifference:
    case QuOgisMiUnion:
    case QuOgisMiDistance:
    
        op1 = AST(s).son;
        op2 = AST(op1).brother;

        e = smtChkQuery(op1, op1Type);  OOSQL_CHECK_ERR(e);
        e = smtChkQuery(op2, op2Type);  OOSQL_CHECK_ERR(e);

		if(isOGISTypeID(op1Type.typeID) == SM_FALSE)
		{
			m_errorMessage->Clear();
			m_errorMessage->Append("Op1 must be OGIS type: at ");
			ERRORMESSAGE_APPEND_POSITION(m_errorMessage, op1);

			OOSQL_ERR(eSYNTAX_ERROR_OOSQL)
		}

		if(isOGISTypeID(op2Type.typeID) == SM_FALSE)
		{
			m_errorMessage->Clear();
			m_errorMessage->Append("Op2 must be OGIS type: at ");
			ERRORMESSAGE_APPEND_POSITION(m_errorMessage, op2);

			OOSQL_ERR(eSYNTAX_ERROR_OOSQL)
		}
        operatorType = OBJECT_OPERATION;
        break;
        
    case QuOgisMiEnvelope:
    case QuOgisMiConvexHull:
    case QuOgisMiExteriorRing:
    case QuOgisMiCentriod:
    case QuOgisMiStartPoint:
    case QuOgisMiEndPoint:
    case QuOgisMiPointOnSurface:

        op1 = AST(s).son;

        e = smtChkQuery(op1, op1Type);  OOSQL_CHECK_ERR(e);

		if(isOGISTypeID(op1Type.typeID) == SM_FALSE)
		{
			m_errorMessage->Clear();
			m_errorMessage->Append("Op1 must be OGIS type: at ");
			ERRORMESSAGE_APPEND_POSITION(m_errorMessage, op1);

			OOSQL_ERR(eSYNTAX_ERROR_OOSQL)
		}

        operatorType = OBJECT_OPERATION;
        break;

    case QuOgisMiBuffer:

        op1 = AST(s).son;
        op2 = AST(op1).brother;

        e = smtChkQuery(op1, op1Type);  OOSQL_CHECK_ERR(e);
        e = smtChkQuery(op2, op2Type);  OOSQL_CHECK_ERR(e);

		if(isOGISTypeID(op1Type.typeID) == SM_FALSE)
		{
			m_errorMessage->Clear();
			m_errorMessage->Append("Op1 must be OGIS type: at ");
			ERRORMESSAGE_APPEND_POSITION(m_errorMessage, op1);

			OOSQL_ERR(eSYNTAX_ERROR_OOSQL)
		}

		if(op2Type.typeID != TYPEID_FLOAT)
		{
			m_errorMessage->Clear();
			m_errorMessage->Append("Op2 must be float type: at ");
			ERRORMESSAGE_APPEND_POSITION(m_errorMessage, op2);

			OOSQL_ERR(eSYNTAX_ERROR_OOSQL)
		}
        operatorType = OBJECT_OPERATION;
        break;
        

    case QuOgisMiInteriorRingN:
    case QuOgisMiPointN:
    case QuOgisMiGeometryN:

        op1 = AST(s).son;
        op2 = AST(op1).brother;

        e = smtChkQuery(op1, op1Type);  OOSQL_CHECK_ERR(e);
        e = smtChkQuery(op2, op2Type);  OOSQL_CHECK_ERR(e);

		if(isOGISTypeID(op1Type.typeID) == SM_FALSE)
		{
			m_errorMessage->Clear();
			m_errorMessage->Append("Op1 must be OGIS type: at ");
			ERRORMESSAGE_APPEND_POSITION(m_errorMessage, op1);

			OOSQL_ERR(eSYNTAX_ERROR_OOSQL)
		}

		if(op2Type.typeID != TYPEID_INT)
		{
			m_errorMessage->Clear();
			m_errorMessage->Append("Op2 must be int type: at ");
			ERRORMESSAGE_APPEND_POSITION(m_errorMessage, op2);

			OOSQL_ERR(eSYNTAX_ERROR_OOSQL)
		}
        operatorType = OBJECT_OPERATION;
        break;
        

    default:
        OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
    }


    epElements[0].exprKind      = EXPR_KIND_OPER;
    e = nodeNameToOperatorID(AST(s).nodeName, epElements[0].oper.operatorId);
    OOSQL_CHECK_ERR(e);
    epElements[0].oper.operatorType = operatorType;
    #ifdef ENABLE_OPENGIS_OPTIMIZATION
	epElements[0].oper.isDeterministic = SM_TRUE; // Be all OGIS Operator deterministic
    #endif    

    // make 1st operand node
    e = makeOperandInExprPool(op1Type, exprPoolIndex);
    epElements[0].oper.operand1 = exprPoolIndex;
    OOSQL_CHECK_ERR(e);
    
    // make 2nd operand nodes
    
    if(AST(s).nodeName == QuOgisMiEnvelope
        || AST(s).nodeName == QuOgisMiConvexHull
        || AST(s).nodeName == QuOgisMiExteriorRing
        || AST(s).nodeName == QuOgisMiCentriod
        || AST(s).nodeName == QuOgisMiStartPoint
        || AST(s).nodeName == QuOgisMiEndPoint
        || AST(s).nodeName == QuOgisMiPointOnSurface)
    { 
    	epElements[0].oper.operand2.setNull();       // no 2nd operand
    }
    else
    {
    	e = makeOperandInExprPool(op2Type, exprPoolIndex);
    	epElements[0].oper.operand2 = exprPoolIndex;
    	OOSQL_CHECK_ERR(e);
    }
  	
  	epElements[0].oper.operand3.setNull();       // no 3rd operand
 
    #ifdef ENABLE_OPENGIS_OPTIMIZATION
    /*
       If this UDF is DETERMINISTIC, check whether all arguments are constant or not.
       If all arguments are constant, set isConstant flag of func[0].
    */
    OperatorStruct nestedOp;
    if(epElements[0].oper.isDeterministic == SM_TRUE)
    {
        ExprPoolElements    nestedExpr(m_pool->exprPool);
        Boolean isAllArgumentConstant;
        isAllArgumentConstant = SM_TRUE;
		// for 1 st op
		nestedExpr = epElements[0].oper.operand1.getElements(m_pool->exprPool);
        if(!nestedExpr.isNull())
        {
            if(nestedExpr[0].exprKind == EXPR_KIND_OPER)
            {
                if (nestedExpr[0].oper.isDeterministic != SM_TRUE)
                {
                    isAllArgumentConstant = SM_FALSE;
                }
			}
            else if(nestedExpr[0].exprKind == EXPR_KIND_PATHEXPR || nestedExpr[0].exprKind == EXPR_KIND_CONS ||
                    nestedExpr[0].exprKind == EXPR_KIND_AGGRFUNC || nestedExpr[0].exprKind == EXPR_KIND_FUNCTION )
            {
                isAllArgumentConstant = SM_FALSE;
            }
        }
		// for 2 nd op
		nestedExpr = epElements[0].oper.operand2.getElements(m_pool->exprPool);
        if(!nestedExpr.isNull())
        {
            if(nestedExpr[0].exprKind == EXPR_KIND_OPER)
            {
                if (nestedExpr[0].oper.isDeterministic != SM_TRUE)
                {
                    isAllArgumentConstant = SM_FALSE;
                }
			}
            else if(nestedExpr[0].exprKind == EXPR_KIND_PATHEXPR || nestedExpr[0].exprKind == EXPR_KIND_CONS ||
                    nestedExpr[0].exprKind == EXPR_KIND_AGGRFUNC || nestedExpr[0].exprKind == EXPR_KIND_FUNCTION )
            {
                isAllArgumentConstant = SM_FALSE;
            }
        }
		// for 3 rd op
		nestedExpr = epElements[0].oper.operand3.getElements(m_pool->exprPool);
        if(!nestedExpr.isNull())
        {
            if(nestedExpr[0].exprKind == EXPR_KIND_OPER)
            {
                if (nestedExpr[0].oper.isDeterministic != SM_TRUE)
                {
                    isAllArgumentConstant = SM_FALSE;
                }
			}
            else if(nestedExpr[0].exprKind == EXPR_KIND_PATHEXPR || nestedExpr[0].exprKind == EXPR_KIND_CONS ||
                    nestedExpr[0].exprKind == EXPR_KIND_AGGRFUNC || nestedExpr[0].exprKind == EXPR_KIND_FUNCTION )
            {
                isAllArgumentConstant = SM_FALSE;
            }
        }

        if (isAllArgumentConstant == SM_TRUE)
            epElements[0].oper.isConstant = SM_TRUE;
        else    epElements[0].oper.isConstant = SM_FALSE;
    }
    else
    {
        epElements[0].oper.isConstant = SM_FALSE;
    }

    epElements[0].oper.UDFNo = m_currentUDFNo;
	m_currentUDFNo++;
    #endif
 	
  	switch ( AST(s).nodeName ) 
    {
    case QuOgisMiDifference:
    case QuOgisMiIntersection:
    case QuOgisMiSymDifference:
    case QuOgisMiUnion:
    case QuOgisMiBuffer:
    case QuOgisMiConvexHull:
    case QuOgisMiGeometryN:
        returnType.set(TYPEID_OGIS_GEOMETRY, PT_EXPRPOOL, epElements.getPoolIndex());
        break;
    
    case QuOgisMiCentriod:
    case QuOgisMiStartPoint:
    case QuOgisMiEndPoint:
    case QuOgisMiPointOnSurface:
    case QuOgisMiPointN:
        returnType.set(TYPEID_OGIS_POINT, PT_EXPRPOOL, epElements.getPoolIndex());
        break;
    
    case QuOgisMiExteriorRing:
    case QuOgisMiInteriorRingN:
        returnType.set(TYPEID_OGIS_LINESTRING, PT_EXPRPOOL, epElements.getPoolIndex());
        break;
    
    case QuOgisMiEnvelope:
        returnType.set(TYPEID_OGIS_POLYGON, PT_EXPRPOOL, epElements.getPoolIndex());
        break;
    
    case QuOgisMiDistance:
        returnType.set(TYPEID_FLOAT, PT_EXPRPOOL, epElements.getPoolIndex());
        break;
    
        
    default:
        OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
    }
    
    e = exprElementSetTypeInfo(epElements, returnType);
    OOSQL_CHECK_ERR(e);

    return eNOERROR;
}

Four OQL_ASTtoGDS::smtChkTextIrOperator(
    ASTNodeIdx					node, 
	StemizerFuncPtr 			stemizer,
    ReturnType&					returnType
)
{
    Four                e;
    ASTNodeIdx          op1, op2, op3;
    ReturnType          op1Type, op2Type, op3Type;
    ExprPoolElements    epElements(m_pool->exprPool);
    ExprPoolElements    childExpr(m_pool->exprPool);
    ExprPoolIndex       exprPoolIndex;

    // construct operator node
    epElements = m_pool->exprPool.addNewEntry();
    epElements[0].operandType = EXPR_OPERANDTYPE_CONTAIN_NOMETHOD;
    epElements[0].refCount    = 0;

    switch(AST(node).nodeName)
    {
    case OpIrBlnAccum:          // +
    case OpIrBlnOr:             // |
    case OpIrBlnAnd:            // &
    case OpIrBlnMinus:          // -
    case OpIrBlnThreshold:      // >
    case OpIrBlnMultiply:       // *
    case OpIrBlnMax:            // :
    case OpIrBlnNear:           // ~
	case OpIrBlnNearWithOrder:	// ^
	case OpIrBetween:			// between("a", "b")
        op1 = AST(node).son;
        op2 = AST(op1).brother;
		if(AST(node).nodeName == OpIrBlnNear || AST(node).nodeName == OpIrBlnNearWithOrder)
			op3 = AST(op2).brother;

        e = smtChkTextIrExpression(op1, stemizer, op1Type);   OOSQL_CHECK_ERR(e);
        e = smtChkTextIrExpression(op2, stemizer, op2Type);   OOSQL_CHECK_ERR(e);

        if(!(op1Type.typeID == TYPEID_FLOAT || op1Type.typeID == TYPEID_DOUBLE || op1Type.typeID == TYPEID_INT))
		{
			m_errorMessage->Clear();
			m_errorMessage->Append("The type of the value used at ");
			ERRORMESSAGE_APPEND_POSITION(m_errorMessage, op1);
			m_errorMessage->Append(" is not a integer, float, double where one of those is expected.");

            OOSQL_ERR(eTYPE_ERROR_OOSQL);
		}

        if(!(op2Type.typeID == TYPEID_FLOAT || op2Type.typeID == TYPEID_DOUBLE || op2Type.typeID == TYPEID_INT))
		{
			m_errorMessage->Clear();
			m_errorMessage->Append("The type of the value used at ");
			ERRORMESSAGE_APPEND_POSITION(m_errorMessage, op2);
			m_errorMessage->Append(" is not a integer, float, double where one of those is expected.");

            OOSQL_ERR(eTYPE_ERROR_OOSQL);
		}

        // check extended boolean expression in match function, max must be at the end of the expression
        if(op1Type.poolType == PT_EXPRPOOL)
        {
            childExpr = op1Type.poolIndex;

            if(childExpr[0].exprKind == EXPR_KIND_OPER)
                if(childExpr[0].oper.operatorId == OP_TEXTIR_MAX)
                    OOSQL_ERR(eEXT_BOOL_EXPR_ERR_OOSQL);
        }

        if(op2Type.poolType == PT_EXPRPOOL)
        {
            childExpr = op2Type.poolIndex;

            if(childExpr[0].exprKind == EXPR_KIND_OPER)
                if(childExpr[0].oper.operatorId == OP_TEXTIR_MAX)
                    OOSQL_ERR(eEXT_BOOL_EXPR_ERR_OOSQL);
        }

        // check type compatibility between op1 and op2, but it is useless
        
        // fill expr node
        epElements[0].exprKind = EXPR_KIND_OPER;
        e = nodeNameToOperatorID(AST(node).nodeName, epElements[0].oper.operatorId);
        OOSQL_CHECK_ERR(e);
        epElements[0].oper.operatorType = TEXT_OPERATION;

        // make operand nodes;
        e = makeOperandInExprPool(op1Type, exprPoolIndex);
        epElements[0].oper.operand1 = exprPoolIndex;
        OOSQL_CHECK_ERR(e);
        e = makeOperandInExprPool(op2Type, exprPoolIndex);
        epElements[0].oper.operand2 = exprPoolIndex;
        OOSQL_CHECK_ERR(e);

		if(AST(node).nodeName == OpIrBlnNear || AST(node).nodeName == OpIrBlnNearWithOrder)
		{
			e = smtChkQuery(op3, op3Type);	OOSQL_CHECK_ERR(e);
			if(op3Type.typeID != TYPEID_INT)
			{
				m_errorMessage->Clear();
				m_errorMessage->Append("The type of the value used at ");
				ERRORMESSAGE_APPEND_POSITION(m_errorMessage, op3);
				m_errorMessage->Append(" is not a integer where one of those is expected.");

				OOSQL_ERR(eTYPE_ERROR_OOSQL);
			}

			e = makeOperandInExprPool(op3Type, exprPoolIndex);
			epElements[0].oper.operand3 = exprPoolIndex;
			OOSQL_CHECK_ERR(e);
		}
		else
			epElements[0].oper.operand3.setNull();       // no third operand

        returnType.set(TYPEID_DOUBLE, PT_EXPRPOOL, epElements.getPoolIndex());

        e = exprElementSetTypeInfo(epElements, returnType);
        OOSQL_CHECK_ERR(e);

        return eNOERROR;

    default:
        OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
    }
}

static int MakeLowercase(char *buffer)
{
	int i, j;
	int length;

	length = strlen(buffer);
	for(i = 0; i < length; i++) 
	{
		if((unsigned char)buffer[i] > (unsigned char)0x80)
			i++;
		else
			buffer[i] = tolower(buffer[i]);
	}
	
	for(i = 0; i < length; i++)
	{
		if(!isspace(buffer[i]) || (unsigned)buffer[i] > (unsigned)0x80)
			break;
	}

	if(i != 0)
	{
		for(j = 0; i < length; i++, j++)
			buffer[j] = buffer[i];
		buffer[j] = '\0';
	}

	length = strlen(buffer);
	for(i = length - 1; i >= 0; i--)
	{
		if(i > 0 && (unsigned)buffer[i - 1] > (unsigned)0x80)
			break;
		if(!isspace(buffer[i]))
			break;
	}
	
	if(i != length - 1)
		buffer[i] = '\0';	

	return eNOERROR;
}

Four OQL_ASTtoGDS::smtChkTextIrExpression(
    ASTNodeIdx					node, 
	StemizerFuncPtr 			stemizer,
    ReturnType&					returnType
)
{
    char						string[MAXKEYWORDSIZE + 1];
	char						stemizedString[MAXKEYWORDSIZE + 1];
    ValuePoolElements			value(m_pool->valuePool);
    StringPoolElements			str(m_pool->stringPool);
    Four						e;
    Four						i;

    switch(AST(node).nodeName)
    {
    case OpIrBlnAccum:          // +
    case OpIrBlnOr:             // |
    case OpIrBlnAnd:            // &
    case OpIrBlnMinus:          // -
    case OpIrBlnThreshold:      // >
    case OpIrBlnMultiply:       // *
    case OpIrBlnMax:            // :
    case OpIrBlnNear:           // ~
	case OpIrBlnNearWithOrder:	// ^
	case OpIrBetween:			// between
        e = smtChkTextIrOperator(node, stemizer, returnType);
        break;

    case STRING:
        e = smtChkValue(node, returnType);
        OOSQL_CHECK_ERR(e);

        // get string from value m_pool
        value = returnType.poolIndex;
        str   = value[0].string;
		if(str[0] == '!') 
        {
            for(i = 1; i < str.size; i++)
                string[i - 1] = str[i];
            string[i - 1] = '\0';
        }
        else
        {
            for(i = 0; i < str.size && i < MAXKEYWORDSIZE; i++)
                string[i] = str[i];
            string[i] = '\0';
 
            // stemize string
			if(stemizer)
			{
				e = stemizer(string, stemizedString);
				OOSQL_CHECK_ERR(e);
				strcpy(string, stemizedString);
			}
        }                    

        // make lowercase
        MakeLowercase(string);

        // set string to value m_pool
        str = m_pool->stringPool.addNewEntry(strlen(string) + 1);
        for(i = 0; i < (int)strlen(string); i++)
            str[i] = string[i];
        str[i] = '\0';

        // connect value
        value[0].string = str.getPoolIndex();

        returnType.typeID = TYPEID_DOUBLE;	
        break;

    case REAL:
    case INTEGER:
        e = smtChkValue(node, returnType);
        OOSQL_CHECK_ERR(e);
        break;

    default:
        OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
    }

    return eNOERROR;
}

Four OQL_ASTtoGDS::smtChkQuIrFnMatch(
    ASTNodeIdx					node,
    ReturnType&					returnType
)
{
    ASTNodeIdx              s1, s2, s3, s4;
    ReturnType              s1Type, s2Type, s3Type, s4Type;
	Four					aliasNumber;
    Four					irScanDirection;
	Four                    e;
    FunctionPoolElements    fpElements(m_pool->funcPool);
    ValuePoolElements       value(m_pool->valuePool);
    IntegerPoolElements     integer(m_pool->intPool);
    ExprPoolElements        expr(m_pool->exprPool);
    ArgumentPoolIndex       poolIndex;
	PathExprPoolElements    ppElements(m_pool->pathExprPool);
	StemizerFuncPtr			stemizer;

    s1 = AST(node).son;
    s2 = AST(s1).brother;
    s3 = AST(s2).brother;
    s4 = AST(s3).brother;    

    // check each operand type and make global data structure
    e = smtChkQuery(s1, s1Type);
    OOSQL_CHECK_ERR(e);
    if(!(s1Type.typeID == TYPEID_TEXT && s1Type.poolType == PT_PATHEXPRPOOL))
	{
		m_errorMessage->Clear();
		m_errorMessage->Append("The type of the value used at ");
		ERRORMESSAGE_APPEND_POSITION(m_errorMessage, s1);
		m_errorMessage->Append(" is not a text where one of those is expected.");

        OOSQL_ERR(eTYPE_ERROR_OOSQL);
	}

	ppElements = s1Type.poolIndex;

	/* get stemizer through output parameter */
	e = getStemizerFuncPtr(ppElements[ppElements.size - 1].classInfo, ppElements[ppElements.size - 1].attr.attrInfo, &stemizer);
	OOSQL_CHECK_ERR(e);

    e = smtChkTextIrExpression(s2, stemizer, s2Type);     
    OOSQL_CHECK_ERR(e);
    if(!(s2Type.typeID == TYPEID_DOUBLE && s2Type.poolType == PT_EXPRPOOL))
    {   
        if(s2Type.typeID == TYPEID_DOUBLE && s2Type.poolType == PT_VALUEPOOL)
        {
            expr = m_pool->exprPool.addNewEntry();
            expr[0].exprKind     = EXPR_KIND_VALUE;
            expr[0].value.setPoolIndex(s2Type.poolIndex);
            expr[0].operandType  = EXPR_OPERANDTYPE_CONTAIN_NOMETHOD;
            expr[0].refCount     = 0;
            expr[0].resultType   = TYPEID_DOUBLE;
            expr[0].complexType  = COMPLEX_TYPE_SIMPLE;
            expr[0].resultLength = TYPEID_DOUBLE_SIZE;

            s2Type.typeID    = TYPEID_DOUBLE;
            s2Type.poolType  = PT_EXPRPOOL;
            s2Type.poolIndex = expr.getPoolIndex();
        }
        else
		{
			m_errorMessage->Clear();
			m_errorMessage->Append("The type of the value used at ");
			ERRORMESSAGE_APPEND_POSITION(m_errorMessage, s2);
			m_errorMessage->Append(" is not a double where one of those is expected.");

            OOSQL_ERR(eTYPE_ERROR_OOSQL);
		}
    }
		
	if(AST(s3).nodeName != null)
	{
        e = smtChkQuery(s3, s3Type);
        OOSQL_CHECK_ERR(e);
        if(!(s3Type.typeID == TYPEID_INT && s3Type.poolType == PT_VALUEPOOL))
		{
			m_errorMessage->Clear();
			m_errorMessage->Append("The type of the value used at ");
			ERRORMESSAGE_APPEND_POSITION(m_errorMessage, s3);
			m_errorMessage->Append(" is not a integer where one of those is expected.");

            OOSQL_ERR(eTYPE_ERROR_OOSQL);
		}

        // get integer
        value = s3Type.poolIndex;
        if(value[0].valueKind != VALUE_KIND_INTEGER)
		{
			m_errorMessage->Clear();
			m_errorMessage->Append("The type of the value used at ");
			ERRORMESSAGE_APPEND_POSITION(m_errorMessage, s3);
			m_errorMessage->Append(" is not a integer where one of those is expected.");

            OOSQL_ERR(eTYPE_ERROR_OOSQL);
		}
    }
	else
	{
		// set as default alias number
		integer = m_pool->intPool.addNewEntry();
		integer[0] = WEIGHT_DEFAULT_LABEL;

		value = m_pool->valuePool.addNewEntry();
		value[0].valueKind = VALUE_KIND_INTEGER;
		value[0].integer   = integer.getPoolIndex();

		s3Type.set(TYPEID_INT, PT_VALUEPOOL, value.getPoolIndex());
	}

	if(AST(s4).nodeName == OpIrScanForward || AST(s4).nodeName == null)
	{
		irScanDirection = TEXTIR_SCAN_FORWARD;

		integer = m_pool->intPool.addNewEntry();
		integer[0] = irScanDirection;

		value = m_pool->valuePool.addNewEntry();
		value[0].valueKind = VALUE_KIND_INTEGER;
		value[0].integer   = integer.getPoolIndex();

		s4Type.set(TYPEID_INT, PT_VALUEPOOL, value.getPoolIndex());
	}
	else
	{
#if defined (ORDEREDSET_BACKWARD_SCAN)
		irScanDirection = TEXTIR_SCAN_BACKWARD_NOORDERING;

		integer = m_pool->intPool.addNewEntry();
		integer[0] = irScanDirection;

		value = m_pool->valuePool.addNewEntry();
		value[0].valueKind = VALUE_KIND_INTEGER;
		value[0].integer   = integer.getPoolIndex();

		s4Type.set(TYPEID_INT, PT_VALUEPOOL, value.getPoolIndex());
#else
		OOSQL_ERR(eUNSUPPORTED_SCANDIRECTION_OOSQL);
#endif	/* #if defined (ORDEREDSET_BACKWARD_SCAN) */
	}

    // make global data structure
    // construct function node
    fpElements = m_pool->funcPool.addNewEntry();

    fpElements[0].functionID = FUNCTION_TEXTIR_MATCH;

	// path expression argument
    e = makeParameterInArgumentPool(s1Type, poolIndex);
    OOSQL_CHECK_ERR(e);
    fpElements[0].argument = poolIndex;

	// boolean expression argument
    e = makeParameterInArgumentPool(s2Type, poolIndex);
    OOSQL_CHECK_ERR(e);
    fpElements[0].argument.size ++;

	// alias number argument
    e = makeParameterInArgumentPool(s3Type, poolIndex);
    OOSQL_CHECK_ERR(e);
    fpElements[0].argument.size ++;

	// ir scan direction argument
    e = makeParameterInArgumentPool(s4Type, poolIndex);
    OOSQL_CHECK_ERR(e);
    fpElements[0].argument.size ++;

    e = function_GetTypeID(fpElements[0].functionID);
    OOSQL_CHECK_ERR(e);
    fpElements[0].returnType = (TypeID)e;

    e = function_GetLength(fpElements[0].functionID);
    OOSQL_CHECK_ERR(e);
    fpElements[0].returnLength = e;

    // construct return type
    returnType.set(TYPEID_DOUBLE, PT_FUNCPOOL, fpElements.getPoolIndex());

    return eNOERROR;
}

Four OQL_ASTtoGDS::smtChkQuIrFnNmatch(
    ASTNodeIdx  node,
    ReturnType& returnType
)
{
    OOSQL_ERR(eNOTIMPLEMENTED_OOSQL);
    return eNOERROR;
}

Four OQL_ASTtoGDS::smtChkQuIrFnWeight(
    ASTNodeIdx  node,
    ReturnType& returnType
)
{
    ASTNodeIdx              s;
    ReturnType              sType;
    Four                    e;
    FunctionPoolElements    fpElements(m_pool->funcPool);
    ValuePoolElements       value(m_pool->valuePool);
    IntegerPoolElements     integer(m_pool->intPool);
    ArgumentPoolIndex       poolIndex;
    
    s = AST(node).son;

    // check each operand type and make global data structure
    if(s == AST_NULL)
    {
        // there is no argument, make default argument
        integer    = m_pool->intPool.addNewEntry();
        integer[0] = WEIGHT_DEFAULT_LABEL;

        value = m_pool->valuePool.addNewEntry();

        value[0].valueKind = VALUE_KIND_INTEGER;
        value[0].integer = integer.getPoolIndex();

        sType.typeID    = TYPEID_INT;
        sType.poolType  = PT_VALUEPOOL;
        sType.poolIndex = value.getPoolIndex();
    }
    else
    {
        e = smtChkQuery(s, sType);
        OOSQL_CHECK_ERR(e);
        if(!(sType.typeID == TYPEID_INT && sType.poolType == PT_VALUEPOOL))
		{
			m_errorMessage->Clear();
			m_errorMessage->Append("The type of the value used at ");
			ERRORMESSAGE_APPEND_POSITION(m_errorMessage, s);
			m_errorMessage->Append(" is not a integer where one of those is expected.");

            OOSQL_ERR(eTYPE_ERROR_OOSQL);
		}
    }

    // make global data structure
    // construct function node
    fpElements = m_pool->funcPool.addNewEntry();

    fpElements[0].functionID = FUNCTION_TEXTIR_WEIGHT;

    e = makeParameterInArgumentPool(sType, poolIndex);
    OOSQL_CHECK_ERR(e);
    fpElements[0].argument = poolIndex;

    e = function_GetTypeID(fpElements[0].functionID);
    OOSQL_CHECK_ERR(e);
    fpElements[0].returnType = (TypeID)e;

    e = function_GetLength(fpElements[0].functionID);
    OOSQL_CHECK_ERR(e);
    fpElements[0].returnLength = e;

    // construct return type
    returnType.set(TYPEID_DOUBLE, PT_FUNCPOOL, fpElements.getPoolIndex());

    return eNOERROR;
}

Four OQL_ASTtoGDS::smtChkQuAccDref(ASTNodeIdx node, ReturnType& returnType)
{
    OOSQL_ERR(eNOTIMPLEMENTED_OOSQL);
}

Four OQL_ASTtoGDS::smtChkQuAccIcltnElem(ASTNodeIdx node, ReturnType& returnType)
{
    OOSQL_ERR(eNOTIMPLEMENTED_OOSQL);
}

Four OQL_ASTtoGDS::smtChkFnOrCltnobj(ASTNodeIdx node, ReturnType& returnType)
{
    ASTNodeIdx		s;
    ReturnType		sType;
	ASTNodeIdx		sArgument;
	ReturnType		sArgumentType;
    Four			e;	
	SimpleString	functionName;
	Four			i, j;
	Boolean			flag;

	FunctionPoolElements						func(m_pool->funcPool);
	ArgumentPoolElements						argument(m_pool->argumentPool);
	ArgumentPoolIndex							argumentPoolIndex;
	OOSQL_TCArray<OOSQL_ExternalFunctionInfo>	functionInfos(pMemoryManager, false);

    if(AST(node).nodeName != FnOrCltnobj)
        OOSQL_ERR(eBADASTNODE_OOSQL);
    
    // Possible type of FnOrCltnobj's is ID
    // ID is the name of member function or collection
    s = AST(node).son;
    e = smtChkQuery(s, sType);
    OOSQL_CHECK_ERR(e);

	sArgument = AST(s).brother;

    if(sType.typeID != TYPEID_ID)
        OOSQL_ERR(eBADASTNODE_OOSQL);

	e = getStringFromID_Node(functionName, s);
	OOSQL_CHECK_ERR(e);

	// read function information from external function manager
	e = m_externalFunctionManager->Find(m_catalog->getVolumeID(), functionName, functionInfos);
	OOSQL_CHECK_ERR(e);

	if(functionInfos.numberOfItems() == 0)
		OOSQL_ERR(eUNDEFINED_FUNCTION_USED_OOSQL);

	func = m_pool->funcPool.addNewEntry();

	func[0].functionID = FUNCTION_USER_DEFINED;
	func[0].argument.setNull();	

	ASTNodeIdx		sArgumentTemp = sArgument;
	int				nArguments = 0;
	while(sArgumentTemp != AST_NULL)
	{
		nArguments ++;
		sArgumentTemp = AST(sArgumentTemp).brother;
	}

	func[0].argument = m_pool->argumentPool.addNewEntry(nArguments);

	i = 0;
    
	while(sArgument != AST_NULL)
	{
		e = smtChkQuery(sArgument, sArgumentType);
		OOSQL_CHECK_ERR(e);

        e = makeParameterInArgument(sArgumentType, func[0].argument.getElements(m_pool->argumentPool, i));
		OOSQL_CHECK_ERR(e);

		sArgument = AST(sArgument).brother;
        i++;
	}

	argument = func[0].argument;
	for(i = 0; i < functionInfos.numberOfItems(); i++)
	{
		if(functionInfos[i].m_paramCount == argument.size)
		{
			for(j = 0; j < functionInfos[i].m_paramCount; j++)
			{
				e = testCompatibility(functionInfos[i].m_parameters[j].m_argumentType,
						              argument[j].type, flag);
				OOSQL_CHECK_ERR(e);

				if(flag == SM_FALSE)
					break;		// not compatible, break
			}
			if(j == functionInfos[i].m_paramCount)
				break;			// find
		}
	}
	if(i == functionInfos.numberOfItems())
		OOSQL_ERR(eUNDEFINED_FUNCTION_USED_OOSQL);

	func[0].userDefinedFunctionID = functionInfos[i].m_funcID;
	func[0].returnType   = functionInfos[i].m_returnType;
	func[0].returnLength = typeID_ToTypeLength(func[0].returnType);


	// construct return type
    returnType.set(func[0].returnType, PT_FUNCPOOL, func.getPoolIndex());

    return eNOERROR;
}

Four OQL_ASTtoGDS::testCompatibility(TypeID op1, TypeID op2, Boolean& flag)
{
    flag = SM_FALSE;

	if((OOSQL_MASK_TYPE(op1) == TYPEID_COMPLEX && OOSQL_MASK_COMPLEXTYPE(op2) == COMPLEXTYPEID_BASIC) ||
	   (OOSQL_MASK_TYPE(op2) == TYPEID_COMPLEX && OOSQL_MASK_COMPLEXTYPE(op1) == COMPLEXTYPEID_BASIC))
	{
		flag = SM_TRUE;
		return eNOERROR;
	}
	if(OOSQL_MASK_COMPLEXTYPE(op1) != OOSQL_MASK_COMPLEXTYPE(op2))
	{
		flag = SM_TRUE;
		return eNOERROR;
	}

	op1 = OOSQL_MASK_TYPE(op1);
	op2 = OOSQL_MASK_TYPE(op2);

    switch(op1)
    {
    case TYPEID_SHORT:
    case TYPEID_INT:
    case TYPEID_LONG:
    case TYPEID_LONG_LONG:
    case TYPEID_BOOL:
        if(op2 == TYPEID_FLOAT ||
           op2 == TYPEID_DOUBLE ||
           op2 == TYPEID_INT ||
           op2 == TYPEID_SHORT ||
           op2 == TYPEID_LONG ||
           op2 == TYPEID_LONG_LONG ||
		   op2 == TYPEID_NIL ||
		   op2 == TYPEID_BOOL ||
		   op2 == TYPEID_STRING ||
		   op2 == TYPEID_VARSTRING)
            flag = SM_TRUE;
        break;
    case TYPEID_FLOAT:
    case TYPEID_DOUBLE:
        if(op2 == TYPEID_FLOAT ||
           op2 == TYPEID_DOUBLE ||
           op2 == TYPEID_INT ||
           op2 == TYPEID_SHORT ||
           op2 == TYPEID_LONG ||
           op2 == TYPEID_LONG_LONG ||
		   op2 == TYPEID_NIL ||
		   op2 == TYPEID_BOOL)
            flag = SM_TRUE;
        break;
    case TYPEID_STRING:
    case TYPEID_VARSTRING:
	case TYPEID_TEXT:
        if(op2 == TYPEID_STRING ||
           op2 == TYPEID_VARSTRING ||
		   op2 == TYPEID_NIL ||
		   op2 == TYPEID_TEXT ||
		   op2 == TYPEID_BOOL)
            flag = SM_TRUE;
        break;
    case TYPEID_OID:
        if(op1 == op2 || op2 == TYPEID_STRING || op2 == TYPEID_NIL)
            flag = SM_TRUE;
        break;
    case TYPEID_PAGEID:
    case TYPEID_FILEID:
    case TYPEID_INDEXID:
    case TYPEID_MBR:
    case TYPEID_NONE:
    case TYPEID_SET:
    case TYPEID_BAG:
    case TYPEID_LIST:
    case TYPEID_ARRAY:
    case TYPEID_DATE:
    case TYPEID_TIME:
    case TYPEID_TIMESTAMP:
    case TYPEID_INTERVAL:
    case TYPEID_STRUCTURE:
    case TYPEID_ID:
    case TYPEID_DOMAIN:
        if(op1 == op2 || op2 == TYPEID_NIL)
            flag = SM_TRUE;
        break;

	case TYPEID_NIL:
		flag = SM_TRUE;
		break;
		
    case TYPEID_OGIS_GEOMETRY:
    case TYPEID_OGIS_POINT:
    case TYPEID_OGIS_LINESTRING:
    case TYPEID_OGIS_POLYGON:
    case TYPEID_OGIS_GEOMETRYCOLLECTION:
    case TYPEID_OGIS_MULTIPOINT:
    case TYPEID_OGIS_MULTILINESTRING:
    case TYPEID_OGIS_MULTIPOLYGON:
		if (op2 == TYPEID_OGIS_GEOMETRY			 ||
		    op2 == TYPEID_OGIS_POINT 			 ||
		    op2 == TYPEID_OGIS_LINESTRING 		 ||
		    op2 == TYPEID_OGIS_POLYGON 		 	 ||
		    op2 == TYPEID_OGIS_GEOMETRYCOLLECTION ||
		    op2 == TYPEID_OGIS_MULTIPOINT		 ||
		    op2 == TYPEID_OGIS_MULTILINESTRING	 ||
		    op2 == TYPEID_OGIS_MULTIPOLYGON)
			flag = SM_TRUE;
		break;
	
    default:
        OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
        break;
    }
    
    return eNOERROR;
}

Four OQL_ASTtoGDS::determineResultType(TypeID op1, TypeID op2, TypeID &resultType)
{
    switch(op1)
    {
    case TYPEID_SHORT:
        if(op2 == TYPEID_SHORT)
            resultType = TYPEID_SHORT;
        else if(op2 == TYPEID_INT)
            resultType = TYPEID_INT;
        else if(op2 == TYPEID_LONG)
            resultType = TYPEID_LONG;
        else if(op2 == TYPEID_LONG_LONG)
            resultType = TYPEID_LONG_LONG;
        else if(op2 == TYPEID_FLOAT)
            resultType = TYPEID_FLOAT;
        else if(op2 == TYPEID_DOUBLE)
            resultType = TYPEID_DOUBLE;
        else
            OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
        break;
    case TYPEID_INT:
        if(op2 == TYPEID_SHORT)
            resultType = TYPEID_INT;
        else if(op2 == TYPEID_INT)
            resultType = TYPEID_INT;
        else if(op2 == TYPEID_LONG)
            resultType = TYPEID_LONG;
        else if(op2 == TYPEID_LONG_LONG)
            resultType = TYPEID_LONG_LONG;
        else if(op2 == TYPEID_FLOAT)
            resultType = TYPEID_FLOAT;
        else if(op2 == TYPEID_DOUBLE)
            resultType = TYPEID_DOUBLE;
        else
            OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
        break;
    case TYPEID_LONG:
        if(op2 == TYPEID_SHORT)
            resultType = TYPEID_LONG;
        else if(op2 == TYPEID_INT)
            resultType = TYPEID_LONG;
        else if(op2 == TYPEID_LONG)
            resultType = TYPEID_LONG;
        else if(op2 == TYPEID_FLOAT)
            resultType = TYPEID_FLOAT;
        else if(op2 == TYPEID_DOUBLE)
            resultType = TYPEID_DOUBLE;
        else
            OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
        break;
    case TYPEID_LONG_LONG:
        if(op2 == TYPEID_SHORT)
            resultType = TYPEID_LONG_LONG;
        else if(op2 == TYPEID_INT)
            resultType = TYPEID_LONG_LONG;
        else if(op2 == TYPEID_LONG)
            resultType = TYPEID_LONG_LONG;
        else if(op2 == TYPEID_LONG_LONG)
            resultType = TYPEID_LONG_LONG;
        else if(op2 == TYPEID_FLOAT)
            resultType = TYPEID_FLOAT;
        else if(op2 == TYPEID_DOUBLE)
            resultType = TYPEID_DOUBLE;
        else
            OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
        break;
    case TYPEID_FLOAT:
        if(op2 == TYPEID_FLOAT || op2 == TYPEID_SHORT || op2 == TYPEID_INT || op2 == TYPEID_LONG || op2 == TYPEID_LONG_LONG)
            resultType = TYPEID_FLOAT;
        else if(op2 == TYPEID_DOUBLE)
            resultType = TYPEID_DOUBLE;
        else
            OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
        break;
    case TYPEID_DOUBLE:
        if(op2 == TYPEID_FLOAT || op2 == TYPEID_SHORT || op2 == TYPEID_INT || op2 == TYPEID_LONG || op2 == TYPEID_LONG_LONG)
            resultType = TYPEID_DOUBLE;
        else if(op2 == TYPEID_DOUBLE)
            resultType = TYPEID_DOUBLE;
        else
            OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
        break;
    case TYPEID_STRING:
        if(op2 == TYPEID_STRING)
            resultType = TYPEID_STRING;
        else if(op2 == TYPEID_VARSTRING)
            resultType = TYPEID_VARSTRING;
        else
            OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
        break;
    case TYPEID_VARSTRING:
        if(op2 == TYPEID_STRING)
            resultType = TYPEID_VARSTRING;
        else if(op2 == TYPEID_VARSTRING)
            resultType = TYPEID_VARSTRING;
        else
            OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
        break;
    case TYPEID_PAGEID:
    case TYPEID_FILEID:
    case TYPEID_INDEXID:
    case TYPEID_OID:
    case TYPEID_TEXT:
    case TYPEID_MBR:
    case TYPEID_NONE:
    case TYPEID_NULL:
    case TYPEID_SET:
    case TYPEID_BAG:
    case TYPEID_LIST:
    case TYPEID_ARRAY:
    case TYPEID_DATE:
    case TYPEID_TIME:
    case TYPEID_TIMESTAMP:
    case TYPEID_INTERVAL:
    case TYPEID_STRUCTURE:
    case TYPEID_NIL:
    case TYPEID_BOOL:
    case TYPEID_ID:
    case TYPEID_DOMAIN:
	case TYPEID_OGIS_GEOMETRY:
	case TYPEID_OGIS_POINT:
	case TYPEID_OGIS_LINESTRING:
	case TYPEID_OGIS_POLYGON:
	case TYPEID_OGIS_GEOMETRYCOLLECTION:
	case TYPEID_OGIS_MULTIPOINT:
	case TYPEID_OGIS_MULTILINESTRING:
	case TYPEID_OGIS_MULTIPOLYGON:
        if(op1 == op2)
            resultType = op1;
        else
            OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
        break;

    default:
        OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
        break;
    }
    
    return eNOERROR;
}

Four OQL_ASTtoGDS::makeStringFromStringPool(SimpleString& s, StringPoolIndex poolIndex)
{
    int i;

    for(i = 0;i < poolIndex.size; i++)
        s[i] = m_pool->stringPool[i + poolIndex.startIndex];
    s[i] = '\0';

    return eNOERROR;
}

Four OQL_ASTtoGDS::smtChkArguments(ASTNodeIdx node, ReturnType& returnType, CataClassInfo classInfo, CataMethodInfo methodInfo)
{
    // Construct argument list in argumentPool
    Four                    nth;
    Four                    nArguments;
    ASTNodeIdx              b;
    ReturnType              bType;
    SimpleString            string;
    ArgumentPoolElements    apElements(m_pool->argumentPool);
    TypeID                  arguType;
    Four                    e;
    Boolean                 flag;
    Four                    nFormalArguments;
    
    // count the number of arguments
    b           = node;
    nArguments  = 0;
    while(b != AST_NULL)
    {
        b = AST(b).brother;
        nArguments ++;
    }

    // check the number of arguments
    e = m_catalog->method_GetN_Arguments(classInfo, methodInfo, nFormalArguments);
    OOSQL_CHECK_ERR(e);
    if(nFormalArguments != nArguments)
	{
		m_errorMessage->Clear();
		m_errorMessage->Append("The parameter of the function at ");
		ERRORMESSAGE_APPEND_POSITION(m_errorMessage, node);
		m_errorMessage->Append(" is invalid.");

        OOSQL_ERR(eNOTMATCHMETHODPARAMETER_OOSQL);
	}
    
    // alloc argument m_pool elements
    apElements = m_pool->argumentPool.addNewEntry(nArguments);
    
    // fill argument m_pool elements
    b     = node;
    nth   = 0;
    while(b != AST_NULL)
    {
        // possible type of b are
        //  1. path expression
        //  2. value (string, integer, real, bool, structure, collection)
        //  3. function
        //  4. domain

        e = smtChkQuery(b, bType);
        OOSQL_CHECK_ERR(e);
        
        // make a node in argument m_pool
        switch(bType.poolType)
        {
        case PT_PATHEXPRPOOL:
            apElements[nth].argumentKind = ARGUMENT_KIND_PATHEXPR;
            apElements[nth].pathExpr.setPoolIndex(bType.poolIndex);
            break;

        case PT_VALUEPOOL:
            apElements[nth].argumentKind = ARGUMENT_KIND_VALUE;
            apElements[nth].value.setPoolIndex(bType.poolIndex);
            break;

        case PT_FUNCPOOL:
            apElements[nth].argumentKind = ARGUMENT_KIND_FUNC;
            apElements[nth].func.setPoolIndex(bType.poolIndex);
            break;

        case PT_DOMAINPOOL:
            apElements[nth].argumentKind = ARGUMENT_KIND_DOMAIN;
            apElements[nth].domain.setPoolIndex(bType.poolIndex);
            break;

        case PT_AGGRFUNCPOOL:
            apElements[nth].argumentKind = ARGUMENT_KIND_AGGRFUNC;
            apElements[nth].aggrFunc.setPoolIndex(bType.poolIndex);
            break;

        default:
            OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
        }

        // check compatibility of actual parameter with formal parameter
        e = m_catalog->method_GetNTH_ArgumentTypeID(classInfo, methodInfo, nth, arguType);
        OOSQL_CHECK_ERR(e);
        e = testCompatibility(bType.typeID, arguType, flag);
        OOSQL_CHECK_ERR(e);
        if(!flag)
		{
			m_errorMessage->Clear();
			m_errorMessage->Append("The type of the value used at ");
			ERRORMESSAGE_APPEND_POSITION(m_errorMessage, b);
			m_errorMessage->Append(" is not compatible with the type of the argument.");

            OOSQL_ERR(eFUNCTIONPARAMETER_ERROR_OOSQL);
		}

        b = AST(b).brother;
        nth   ++;
    }

    // construct return type
    returnType.set(TYPEID_NONE, PT_ARGUMENTPOOL, apElements.getPoolIndex());

    return eNOERROR;
}

Four OQL_ASTtoGDS::getFourFromINTEGER_NODE(int& integer, ASTNodeIdx node)
{
    if(AST(node).nodeName != INTEGER)
        OOSQL_ERR(eBADASTNODE_OOSQL);
    integer = m_pool->intPool[AST(node).tokenVal];

    return eNOERROR;
}

Four OQL_ASTtoGDS::getFloatFromREAL_Node(float& real, ASTNodeIdx node)
{
    if(AST(node).nodeName != REAL)
        OOSQL_ERR(eBADASTNODE_OOSQL);
    real = m_pool->realPool[AST(node).tokenVal];

    return eNOERROR;
}

Four OQL_ASTtoGDS::getStringPoolIndexFromID_Node(StringPoolIndex& poolIndex, ASTNodeIdx node)
{
    if(AST(node).nodeName != ID)
        OOSQL_ERR(eBADASTNODE_OOSQL);

    poolIndex.setPoolIndex(m_pool->stringIndexPool[AST(node).tokenVal],
                           m_pool->stringIndexPool[AST(node).tokenVal + 1] -
                           m_pool->stringIndexPool[AST(node).tokenVal] - 1);

    return eNOERROR;
}

Four OQL_ASTtoGDS::getStringPoolIndexFromSTRING_Node(StringPoolIndex& poolIndex, ASTNodeIdx node)
{
    if(AST(node).nodeName != STRING)
        OOSQL_ERR(eBADASTNODE_OOSQL);

    poolIndex.setPoolIndex(m_pool->stringIndexPool[AST(node).tokenVal],
                           m_pool->stringIndexPool[AST(node).tokenVal + 1] -
                           m_pool->stringIndexPool[AST(node).tokenVal] - 1);

    return eNOERROR;
}

Four OQL_ASTtoGDS::getStringFromSTRING_Node(SimpleString& string, ASTNodeIdx node)
{
    StringPoolIndex poolIndex;
    Four            e;

    if(AST(node).nodeName != STRING)
        OOSQL_ERR(eBADASTNODE_OOSQL);

    poolIndex.setPoolIndex(m_pool->stringIndexPool[AST(node).tokenVal],
                           m_pool->stringIndexPool[AST(node).tokenVal + 1] -
                           m_pool->stringIndexPool[AST(node).tokenVal] - 1);

    e = makeStringFromStringPool(string, poolIndex);
    OOSQL_CHECK_ERR(e);

    return eNOERROR;
}

Four OQL_ASTtoGDS::getStringFromID_Node(SimpleString& string, ASTNodeIdx node)
{
    StringPoolIndex poolIndex;
    Four            e;

    if(AST(node).nodeName != ID)
        OOSQL_ERR(eBADASTNODE_OOSQL);

    poolIndex.setPoolIndex(m_pool->stringIndexPool[AST(node).tokenVal],
                           m_pool->stringIndexPool[AST(node).tokenVal + 1] -
                           m_pool->stringIndexPool[AST(node).tokenVal] - 1);

    e = makeStringFromStringPool(string, poolIndex);
    OOSQL_CHECK_ERR(e);
    
    return eNOERROR;
}

Four OQL_ASTtoGDS::isID_AliasName(ASTNodeIdx node, NameAssociatedInfo& ninfo, Boolean& flag)
{
    SimpleString    idString;
    Four            e;

    flag = SM_FALSE;

    e = getStringFromID_Node(idString, node);
    OOSQL_CHECK_ERR(e);

    e = m_nameStack->FindItem(idString, ninfo);
    
    if(e == eNOTFOUND_DICTIONARY)
    {
        flag = SM_FALSE;
        return eNOERROR;
    }
    else if(e == eNOERROR)
    {
        flag = SM_TRUE;
        return eNOERROR;
    }
    else
        OOSQL_ERR(e);
}

Four OQL_ASTtoGDS::isID_ClassName(ASTNodeIdx node, CataClassInfo& classInfo, Boolean &flag)
{
    SimpleString    className;
    Four            e;

    flag = SM_FALSE;

    // convert node to string
    e = getStringFromID_Node(className, node);
    OOSQL_CHECK_ERR(e);

    // get m_catalog info
    e = m_catalog->class_ClassName_to_CataClassInfo(className, classInfo);
    if(e == eNOT_FOUND_OOSQL)
    {
        flag = SM_FALSE;
        return eNOERROR;
    }
    else if(e == eNOERROR)
    {
        flag = SM_TRUE;
        return eNOERROR;
    }
    else
        OOSQL_ERR(e);
}

Four OQL_ASTtoGDS::isID_AttributeName(ASTNodeIdx node, CataClassInfo& classInfo, CataAttrInfo& attrInfo, Boolean &flag)
{
    SimpleString        attrName;
    SimpleString        className;
    Four                i;
    Four                e;
	TargetListPoolIndex	targetList;

	e = m_gds->getTargetList(m_pool, targetList);
	OOSQL_CHECK_ERR(e);

    flag = SM_FALSE;

    // convert node to string
    e = getStringFromID_Node(attrName, node);
    OOSQL_CHECK_ERR(e);

    // get m_catalog info
    if(classInfo == -1)
    {
        // search all classes in target list for the attribute
        for(i = targetList.startIndex; i < targetList.startIndex + targetList.size; i++)
        {
            e = m_catalog->attr_AttrName_to_CataAttrInfo(m_pool->targetListPool[i].collectionInfo.classInfo, 
                                                       attrName, attrInfo);
            if(e == eNOT_FOUND_OOSQL)
                continue;
            else if(flag == SM_FALSE && e == eNOERROR)
            {
                classInfo = m_pool->targetListPool[i].collectionInfo.classInfo;
                flag      = SM_TRUE;
            }
            else if(flag == SM_TRUE && e == eNOERROR)
                return eBINDING_AMBIGUITY_OOSQL;
            else
                OOSQL_ERR(e);
        }
        return eNOERROR;
    }
    else
    {
        e = m_catalog->attr_AttrName_to_CataAttrInfo(classInfo, attrName, attrInfo);
        if(e == eNOT_FOUND_OOSQL)
        {
            flag = SM_FALSE;
            return eNOERROR;
        }
        else if(e == eNOERROR)
        {
            flag = SM_TRUE;
            return eNOERROR;
        }
        else
            OOSQL_ERR(e);
    }
}

Four OQL_ASTtoGDS::isID_MethodName(ASTNodeIdx node, CataClassInfo& classInfo, CataMethodInfo& methodInfo, Boolean &flag)
{
    SimpleString    methodName;
    SimpleString    className;
    Four            i;
    Four            e;
	TargetListPoolIndex targetList;

	e = m_gds->getTargetList(m_pool, targetList);
	OOSQL_CHECK_ERR(e);

    flag = SM_FALSE;

    // convert node to string
    e = getStringFromID_Node(methodName, node);
    OOSQL_CHECK_ERR(e);

    // get m_catalog info
    if(classInfo == -1)
    {
        // search all classes in target list for the attribute
        for(i = targetList.startIndex; i < targetList.startIndex + targetList.size; i++)
        {
            e = m_catalog->method_MethodName_to_CataMethodInfo(m_pool->targetListPool[i].collectionInfo.classInfo, 
                                                             methodName, methodInfo);
            if(e == eNOT_FOUND_OOSQL)
                continue;
            else if(e == eNOERROR)
            {
                classInfo = m_pool->targetListPool[i].collectionInfo.classInfo;
                flag      = SM_TRUE;
                return eNOERROR;
            }
            else
                OOSQL_ERR(e);
        }   
        flag = SM_FALSE;
        return eNOERROR;
    }
    else
    {
        e = m_catalog->method_MethodName_to_CataMethodInfo(classInfo, methodName, methodInfo);
        if(e == eNOT_FOUND_OOSQL)
        {
            flag = SM_FALSE;
            return eNOERROR;
        }
        else if(e == eNOERROR)
        {
            flag = SM_TRUE;
            return eNOERROR;
        }
        else
            OOSQL_ERR(e);
    }
}

Four OQL_ASTtoGDS::makeOperandInExprPool(ReturnType opType, ExprPoolIndex& poolIndex)
{
    ExprPoolElements        epElements(m_pool->exprPool);
    ValuePoolElements       vpElements(m_pool->valuePool);
    ConstructPoolElements   conspElements(m_pool->constructPool);
    Four                    e;

    // possible m_pool types which opType can have are
    //  PT_NONE, PT_SELLISTPOOL, PT_TARGETLISTPOOL, PT_EXPRPOOL, PT_GROUPBYLISTPOOL,
    //  PT_QGNODEPOOL, PT_PATHEXPRPOOL, PT_ARGUMENTPOOL, PT_STRUCTUREPOOL,
    //  PT_VALUEPOOL, PT_INTPOOL, PT_REALPOOL, PT_STRINGPOOL, PT_LLISTNODEPOOL,
    //  PT_FROMATTRPOOL, PT_AGGRFUNCPOOL, PT_FUNCPOOL, PT_DOMAINPOOL, PT_COLLECTIONPOOL,
    //  PT_MBRPOOL, PT_BOOLPOOL, PT_CONSTRUCTPOOL, PT_NILPOOL, PT_OBJECTPOOL
    switch(opType.poolType)
    {
    case PT_EXPRPOOL:
        epElements = opType.poolIndex;
        break;
    case PT_PATHEXPRPOOL:
    case PT_AGGRFUNCPOOL:
    case PT_VALUEPOOL:
    case PT_FUNCPOOL:
    case PT_CONSTRUCTPOOL:
        epElements = m_pool->exprPool.addNewEntry();
        epElements[0].operandType = EXPR_OPERANDTYPE_CONTAIN_NOMETHOD;
        epElements[0].refCount    = 0;

        switch(opType.poolType)
        {
        case PT_PATHEXPRPOOL:
            epElements[0].exprKind = EXPR_KIND_PATHEXPR;
            epElements[0].pathExpr.setPoolIndex(opType.poolIndex);
            break;
        case PT_AGGRFUNCPOOL:
            epElements[0].exprKind = EXPR_KIND_AGGRFUNC;
            epElements[0].aggrFunc.setPoolIndex(opType.poolIndex);
            break;
        case PT_VALUEPOOL:
            epElements[0].exprKind = EXPR_KIND_VALUE;
            epElements[0].value.setPoolIndex(opType.poolIndex);  
            break;
        case PT_FUNCPOOL:
            epElements[0].exprKind = EXPR_KIND_FUNCTION;
            epElements[0].func.setPoolIndex(opType.poolIndex);      
            break;
        case PT_CONSTRUCTPOOL:
            epElements[0].exprKind = EXPR_KIND_CONS;
            epElements[0].cons.setPoolIndex(opType.poolIndex);      
            break;
        }
        e = exprElementSetTypeInfo(epElements, opType);
        OOSQL_CHECK_ERR(e);
        break;

    case PT_STRUCTUREPOOL:
    case PT_COLLECTIONPOOL:
    case PT_OBJECTPOOL:
        conspElements = m_pool->constructPool.addNewEntry();

        switch(opType.poolType)
        {
        case PT_OBJECTPOOL:
            conspElements[0].consKind = CONSTRUCT_KIND_OBJECT;
            conspElements[0].object.setPoolIndex(opType.poolIndex);
            break;
        case PT_STRUCTUREPOOL:
            conspElements[0].consKind  = CONSTRUCT_KIND_STRUCTURE;
            conspElements[0].structure.setPoolIndex(opType.poolIndex);
            break;
        case PT_COLLECTIONPOOL:
            conspElements[0].consKind   = CONSTRUCT_KIND_COLLECTION;
            conspElements[0].collection.setPoolIndex(opType.poolIndex);
            break;
        }
        epElements = m_pool->exprPool.addNewEntry();
        epElements[0].operandType = EXPR_OPERANDTYPE_CONTAIN_NOMETHOD;
        epElements[0].refCount    = 0;

        epElements[0].exprKind = EXPR_KIND_CONS;
        epElements[0].cons         = conspElements.getPoolIndex();

        e = exprElementSetTypeInfo(epElements, opType);
        OOSQL_CHECK_ERR(e);
        break;      

    case PT_INTPOOL:
    case PT_REALPOOL:
    case PT_STRINGPOOL:
    case PT_MBRPOOL:
    case PT_BOOLPOOL:
    case PT_NILPOOL:
        vpElements = m_pool->valuePool.addNewEntry();

        switch(opType.poolType)
        {
        case PT_INTPOOL:
            vpElements[0].valueKind = VALUE_KIND_INTEGER;
            vpElements[0].integer.setPoolIndex(opType.poolIndex);
            break;
        case PT_REALPOOL:
            vpElements[0].valueKind = VALUE_KIND_REAL;
            vpElements[0].real.setPoolIndex(opType.poolIndex);
            break;
        case PT_STRINGPOOL:
            vpElements[0].valueKind = VALUE_KIND_STRING;
            vpElements[0].string.setPoolIndex(opType.poolIndex);
            break;
        case PT_MBRPOOL:
            vpElements[0].valueKind = VALUE_KIND_MBR;
            vpElements[0].mbr.setPoolIndex(opType.poolIndex);
            break;
        case PT_BOOLPOOL:
            vpElements[0].valueKind = VALUE_KIND_BOOL;
            vpElements[0].boolean   = opType.poolIndex;
            break;
        case PT_NILPOOL:
            vpElements[0].valueKind = VALUE_KIND_NIL;
            vpElements[0].nil       = opType.poolIndex;
            break;
        default:
            OOSQL_ERR(eINTERNALERROR_OOSQL);
            break;
        }

        epElements = m_pool->exprPool.addNewEntry();
        epElements[0].operandType = EXPR_OPERANDTYPE_CONTAIN_NOMETHOD;
        epElements[0].refCount    = 0;

        epElements[0].exprKind = EXPR_KIND_VALUE;
        epElements[0].value        = vpElements.getPoolIndex();

        e = exprElementSetTypeInfo(epElements, opType);
        OOSQL_CHECK_ERR(e);
        break;

    default:
        OOSQL_ERR(eINTERNALERROR_OOSQL);
        break;
    }

    poolIndex = epElements.getPoolIndex();

    return eNOERROR;
}

Four OQL_ASTtoGDS::makeParameterInArgument(ReturnType opType, ArgumentElement &argument)
{
    ValuePoolElements       vpElements(m_pool->valuePool);
    ConstructPoolElements   conspElements(m_pool->constructPool);

    // possible m_pool types which opType can have are
    //  PT_NONE, PT_SELLISTPOOL, PT_TARGETLISTPOOL, PT_EXPRPOOL, PT_GROUPBYLISTPOOL,
    //  PT_QGNODEPOOL, PT_PATHEXPRPOOL, PT_ARGUMENTPOOL, PT_STRUCTUREPOOL,
    //  PT_VALUEPOOL, PT_INTPOOL, PT_REALPOOL, PT_STRINGPOOL, PT_LLISTNODEPOOL,
    //  PT_FROMATTRPOOL, PT_AGGRFUNCPOOL, PT_FUNCPOOL, PT_DOMAINPOOL, PT_COLLECTIONPOOL
    //  PT_MBRPOOL, PT_BOOLPOOL, PT_CONSTRUCTPOOL, PT_NILPOOL, PT_OBJECTPOOL, PT_EXPRPOOL
    switch(opType.poolType)
    {
    case PT_PATHEXPRPOOL:
    case PT_VALUEPOOL:
    case PT_FUNCPOOL:
    case PT_DOMAINPOOL:
    case PT_AGGRFUNCPOOL:
    case PT_EXPRPOOL:
    
        switch(opType.poolType)
        {
        case PT_PATHEXPRPOOL:
            argument.argumentKind = ARGUMENT_KIND_PATHEXPR;
            argument.pathExpr.setPoolIndex(opType.poolIndex);
            break;

        case PT_VALUEPOOL:
            argument.argumentKind = ARGUMENT_KIND_VALUE;
            argument.value.setPoolIndex(opType.poolIndex);
            break;

        case PT_FUNCPOOL:
            argument.argumentKind = ARGUMENT_KIND_FUNC;
            argument.func.setPoolIndex(opType.poolIndex);
            break;

        case PT_DOMAINPOOL:
            argument.argumentKind = ARGUMENT_KIND_DOMAIN;
            argument.domain.setPoolIndex(opType.poolIndex);
            break;

        case PT_AGGRFUNCPOOL:
            argument.argumentKind = ARGUMENT_KIND_AGGRFUNC;
            argument.aggrFunc.setPoolIndex(opType.poolIndex);
            break;

        case PT_EXPRPOOL:
            argument.argumentKind = ARGUMENT_KIND_EXPR;
            argument.expr.setPoolIndex(opType.poolIndex);
            break;
        }
        
        break;

    case PT_INTPOOL:
    case PT_REALPOOL:
    case PT_STRINGPOOL:
    case PT_COLLECTIONPOOL:
    case PT_BOOLPOOL:
    case PT_NILPOOL:
    case PT_MBRPOOL:
        vpElements = m_pool->valuePool.addNewEntry();

        switch(opType.poolType)
        {
        case PT_INTPOOL:
            vpElements[0].valueKind = VALUE_KIND_INTEGER;
            vpElements[0].integer.setPoolIndex(opType.poolIndex);
            break;
        case PT_REALPOOL:
            vpElements[0].valueKind = VALUE_KIND_REAL;
            vpElements[0].real.setPoolIndex(opType.poolIndex);
            break;
        case PT_STRINGPOOL:
            vpElements[0].valueKind = VALUE_KIND_STRING;
            vpElements[0].string.setPoolIndex(opType.poolIndex);
            break;
        case PT_MBRPOOL:
            vpElements[0].valueKind = VALUE_KIND_MBR;
            vpElements[0].mbr.setPoolIndex(opType.poolIndex);
            break;
        case PT_BOOLPOOL:
            vpElements[0].valueKind = VALUE_KIND_BOOL;
            vpElements[0].boolean   = opType.poolIndex;
            break;
        case PT_NILPOOL:
            vpElements[0].valueKind = VALUE_KIND_NIL;
            vpElements[0].nil       = opType.poolIndex;
            break;
        default:
            OOSQL_ERR(eINTERNALERROR_OOSQL);
            break;
        }
        argument.argumentKind = ARGUMENT_KIND_VALUE;
        argument.value        = vpElements.getPoolIndex();
        break;

    default:
        OOSQL_ERR(eINTERNALERROR_OOSQL);
        break;
    }

	argument.type   = opType.typeID;
	argument.length = typeID_ToTypeLength(opType.typeID);

    return eNOERROR;
}

Four OQL_ASTtoGDS::makeParameterInArgumentPool(ReturnType opType, ArgumentPoolIndex &poolIndex)
{
    ArgumentPoolElements    apElements(m_pool->argumentPool);
	Four					e;

	apElements = m_pool->argumentPool.addNewEntry();
	e = makeParameterInArgument(opType, apElements[0]);
	OOSQL_CHECK_ERR(e);

    poolIndex = apElements.getPoolIndex();

    return eNOERROR;
}

Four OQL_ASTtoGDS::nodeNameToOperatorID(Four nodeName, OperatorID &operID)
{
    Four    i;

    // convert m_ast node name to operator id
    struct NN_to_OpID_Entry {
        Four        nodeName;
        OperatorID  operatorID;
    };

    static NN_to_OpID_Entry nnToOpID_Table[] = {
        {OpUnMin,				OP_UNARY_MINUS},
        {OpUnAbs,				OP_ABS},
        {OpBiPlu,				OP_PLUS},
        {OpBiMin,				OP_MINUS},
        {OpBiMul,				OP_MULTIPLY},
        {OpBiDiv,				OP_DIVIDE},
        {OpBiMod,				OP_MOD},
        {OpBiStrcat,			OP_STRING_CONCAT},
        {OpCmpLike,				OP_LIKE}, 
        {OpCmpEq,				OP_EQ}, 
        {OpCmpNe,				OP_NE}, 
        {OpCmpGt,				OP_GT}, 
        {OpCmpLt,				OP_LT}, 
        {OpCmpGe,				OP_GE}, 
        {OpCmpLe,				OP_LE},
		{OpCmpIsNull,           OP_ISNULL},
		{OpCmpIsNotNull,        OP_ISNOTNULL},
        {OpBlnNot,				OP_NOT}, 
        {OpBlnAnd,				OP_AND}, 
        {OpBlnOr,				OP_OR}, 
        {OpSetInt,				OP_INTERSECT}, 
        {OpSetUni,				OP_UNION}, 
        {OpSetExc,				OP_EXCEPT},
        {QuCltnIn,				OP_IN},
        {OpGeoNorth,			OP_GEO_NORTH}, 
        {OpGeoSouth,			OP_GEO_SOUTH}, 
        {OpGeoEast,				OP_GEO_EAST}, 
        {OpGeoWest,				OP_GEO_WEST}, 
        {OpGeoCover,			OP_GEO_COVER}, 
        {OpGeoCovered,			OP_GEO_COVERED},
        {OpGeoContain,			OP_GEO_CONTAIN}, 
        {OpGeoContained,		OP_GEO_CONTAINED}, 
        {OpGeoDisjoint,			OP_GEO_DISJOINT}, 
        {OpGeoEqual,			OP_GEO_EQUAL}, 
        {OpGeoMeet,				OP_GEO_MEET}, 
        {OpGeoOverlap,			OP_GEO_OVERLAP},
        {OpIrBlnAccum,			OP_TEXTIR_ACCUMULATE},
        {OpIrBlnOr,				OP_TEXTIR_OR},
        {OpIrBlnAnd,			OP_TEXTIR_AND},
        {OpIrBlnMinus,			OP_TEXTIR_MINUS},
        {OpIrBlnThreshold,		OP_TEXTIR_THRESHOLD},
        {OpIrBlnMultiply,		OP_TEXTIR_MULTIPLY},
        {OpIrBlnMax,			OP_TEXTIR_MAX},
        {OpIrBlnNear,			OP_TEXTIR_NEAR},
		{OpIrBlnNearWithOrder,	OP_TEXTIR_NEAR_WITH_ORDER},
		{OpIrBetween,			OP_TEXTIR_BETWEEN},
        {QuOgisTrGeometryFT,	OP_OGIS_GEOMETRYFT},
		{QuOgisTrPointFT,		OP_OGIS_POINTFT},
		{QuOgisTrLineStringFT,	OP_OGIS_LINESTRINGFT},
		{QuOgisTrPolygonFT,		OP_OGIS_POLYGONFT},
		{QuOgisTrMultiPointFT,	OP_OGIS_MULTIPOINTFT},
		{QuOgisTrMultiLineStringFT,		OP_OGIS_MULTILINESTRINGFT},
		{QuOgisTrMultiPolygonFT,		OP_OGIS_MULTIPOLYGONFT},
		{QuOgisTrGeometryCollectionFT,	OP_OGIS_GEOMETRYCOLLECTIONFT},
		{QuOgisTrGeometryFB,	OP_OGIS_GEOMETRYFB},
		{QuOgisTrPointFB,		OP_OGIS_POINTFB},
		{QuOgisTrLineStringFB,	OP_OGIS_LINESTRINGFB},
		{QuOgisTrPolygonFB,		OP_OGIS_POLYGONFB},
		{QuOgisTrMultiPointFB,	OP_OGIS_MULTIPOINTFB},
		{QuOgisTrMultiLineStringFB,		OP_OGIS_MULTILINESTRINGFB},
		{QuOgisTrMultiPolygonFB,		OP_OGIS_MULTIPOLYGONFB},
		{QuOgisTrGeometryCollectionFB,	OP_OGIS_GEOMETRYCOLLECTIONFB},
		{QuOgisTrAsText,		OP_OGIS_ASTEXT},
		{QuOgisTrAsBinary,		OP_OGIS_ASBINARY},
		{QuOgisGeDimension,		OP_OGIS_DIMENSION},
		{QuOgisGeGeometryType,	OP_OGIS_GEOMETRYTYPE},
		{QuOgisGeSRID,			OP_OGIS_SRID},
		{QuOgisGeBoundary,		OP_OGIS_BOUNDARY},
		{QuOgisGeLength,		OP_OGIS_LENGTH},
		{QuOgisGeX,				OP_OGIS_X},
		{QuOgisGeY,				OP_OGIS_Y},
		{QuOgisGeArea,			OP_OGIS_AREA},
		{QuOgisGeNumGeometries,	OP_OGIS_NUMGEOMETRIES},
		{QuOgisGeNumPoints,		OP_OGIS_NUMPOINTS},
		{QuOgisGeNumInteriorRings,		OP_OGIS_NUMINTERIORRINGS},
		{QuOgisReIsEmpty,		OP_OGIS_ISEMPTY},
		{QuOgisReIsSimple,		OP_OGIS_ISSIMPLE},
		{QuOgisReIsClosed,		OP_OGIS_ISCLOSED},
		{QuOgisReIsRing,		OP_OGIS_ISRING},
		{QuOgisReContains,		OP_OGIS_CONTAINS},
		{QuOgisReCrosses,		OP_OGIS_CROSSES},
		{QuOgisReDisjoint,		OP_OGIS_DISJOINT},
		{QuOgisReEquals,		OP_OGIS_EQUALS},
		{QuOgisReIntersects,	OP_OGIS_INTERSECTS},
		{QuOgisReOverlaps,		OP_OGIS_OVERLAPS},
		{QuOgisReRelated,		OP_OGIS_RELATED},
		{QuOgisReTouches,		OP_OGIS_TOUCHES},
		{QuOgisReWithin,		OP_OGIS_WITHIN},
		{QuOgisMiDifference,	OP_OGIS_DIFFERENCE},
		{QuOgisMiIntersection,	OP_OGIS_INTERSECTION},
		{QuOgisMiSymDifference,	OP_OGIS_SYMDIFFERENCE},
		{QuOgisMiUnion,			OP_OGIS_UNION},
		{QuOgisMiDistance,		OP_OGIS_DISTANCE},
		{QuOgisMiEnvelope,		OP_OGIS_ENVELOPE},
		{QuOgisMiBuffer,		OP_OGIS_BUFFER},
		{QuOgisMiConvexHull,	OP_OGIS_CONVEXHULL},
		{QuOgisMiExteriorRing,	OP_OGIS_EXTERIORRING},
		{QuOgisMiInteriorRingN,	OP_OGIS_INTERIORRINGN},
		{QuOgisMiCentriod,		OP_OGIS_CENTRIOD},
		{QuOgisMiStartPoint,	OP_OGIS_STARTPOINT},
		{QuOgisMiEndPoint,		OP_OGIS_ENDPOINT},
		{QuOgisMiPointOnSurface,		OP_OGIS_POINTONSURFACE},
		{QuOgisMiPointN,		OP_OGIS_POINTN},
		{QuOgisMiGeometryN,		OP_OGIS_GEOMETRYN},
        {-1,					OP_NONE}                    // end of table
    };

    for(i = 0;nnToOpID_Table[i].nodeName != -1;i++)
    {
        if(nnToOpID_Table[i].nodeName == nodeName)
        {
            operID = nnToOpID_Table[i].operatorID;
            return eNOERROR;
        }
    }

    operID = OP_NONE;
    OOSQL_ERR(eBADASTNODE_OOSQL);
}

Four OQL_ASTtoGDS::nodeNameToFunctionID(Four nodeName, FunctionID &functionID)
{
    Four    i;

    // convert m_ast node name to operator id
    struct NN_to_FuncID_Entry {
        Four        nodeName;
        FunctionID  functionID;
    };

    // (FUNCTION_IR_WEIGHT, FUNCTION_IR_MATCH, FUNCTION_IR_NMATCH)
    static NN_to_FuncID_Entry   nnToFuncID_Table[] = {
        {FnGeoNearest,      FUNCTION_GEO_NEAREST}, 
        {FnGeoBoundary,     FUNCTION_GEO_BOUNDARY}, 
        {FnGeoInterior,     FUNCTION_GEO_INTERIOR}, 
        {FnGeoBuffer,       FUNCTION_GEO_BUFFER}, 
        {FnGeoUnion,        FUNCTION_GEO_UNION}, 
        {FnGeoIntersect,    FUNCTION_GEO_INTERSECT},
        {FnGeoDifference,   FUNCTION_GEO_DIFFERENCE}, 
        {FnGeoDistance,     FUNCTION_GEO_DISTANCE}, 
        {FnGeoArea,         FUNCTION_GEO_AREA}, 
        {FnGeoLength,       FUNCTION_GEO_LENGTH}, 
        {-1,                FUNCTION_NONE}                // end of table
    };

    for(i = 0;nnToFuncID_Table[i].nodeName != -1;i++)
    {
        if(nnToFuncID_Table[i].nodeName == nodeName)
        {
            functionID = nnToFuncID_Table[i].functionID;
            return eNOERROR;
        }
    }

    functionID = FUNCTION_NONE;
    OOSQL_ERR(eBADASTNODE_OOSQL);
}

Four OQL_ASTtoGDS::nodeNameToAggrFunctionID(Four nodeName, AggrFunctionID &aggrFuncID)
{
    Four    i;

    // convert m_ast node name to operator id
    struct NN_to_AggrFuncID_Entry {
        Four            nodeName;
        AggrFunctionID  aggrFunctionID;
    };

    static NN_to_AggrFuncID_Entry   nnToAggrFuncID_Table[] = {
        {FnAggCntall,   AGGRFUNC_COUNTALL},
        {FnAggCnt,      AGGRFUNC_COUNT},
        {FnAggSum,      AGGRFUNC_SUM},
        {FnAggMin,      AGGRFUNC_MIN},
        {FnAggMax,      AGGRFUNC_MAX},
        {FnAggAvg,      AGGRFUNC_AVG},
        {-1,            AGGRFUNC_NONE}                   // end of table
    };

    for(i = 0;nnToAggrFuncID_Table[i].nodeName != -1;i++)
    {
        if(nnToAggrFuncID_Table[i].nodeName == nodeName)
        {
            aggrFuncID = nnToAggrFuncID_Table[i].aggrFunctionID;
            return eNOERROR;
        }
    }

    aggrFuncID = AGGRFUNC_NONE;

    OOSQL_ERR(eBADASTNODE_OOSQL);
}

Four OQL_ASTtoGDS::checkIfSpatialClass(CataClassInfo classInfo, Boolean& flag)
{
	Four e;

	e = m_catalog->class_IsSpatialClass(classInfo);
	OOSQL_CHECK_ERR(e);

	if(e) flag = SM_TRUE;
	else  flag = SM_FALSE;

    return eNOERROR;
}

Four OQL_ASTtoGDS::qg_consInitialQG_Nodes()
{
    // construct initial QG node from target list
    // thic function must be called after targetList has been constructed.
    QGNodePoolElements  qgpElements(m_pool->qgNodePool);
    Four                i;
    Four                startIndex = -1;
    Four                e;
	TargetListPoolIndex	targetList;

	e = m_gds->getTargetList(m_pool, targetList);
	OOSQL_CHECK_ERR(e);

    for(i = targetList.startIndex;i < targetList.startIndex + targetList.size; i++)
    {
        // make new QG node
        qgpElements = m_pool->qgNodePool.addNewEntry();
        qgpElements[0].init();

        if(startIndex == -1)
            startIndex = qgpElements.startIndex;

        // fill information in QG node
        qgpElements[0].classInfo            = m_pool->targetListPool[i].collectionInfo.classInfo;
        qgpElements[0].aliasName            = m_pool->targetListPool[i].aliasName;
        qgpElements[0].implicitJoinAttrKind = QGNODE_KIND_FROMNONE;
		qgpElements[0].isTargetOid			= m_pool->targetListPool[i].isTargetOid;
		qgpElements[0].oid					= m_pool->targetListPool[i].oid;

        // construct sub classes
        if(m_pool->targetListPool[0].starFlag == SM_TRUE)
        {
            e = qg_consSubClasses(qgpElements.getPoolIndex());
            OOSQL_CHECK_ERR(e);
        }
    }
    
	QGNodePoolIndex queryGraph;

    queryGraph.setPoolIndex(startIndex, targetList.size);
	e = m_gds->setQueryGraph(m_pool, queryGraph);
	OOSQL_CHECK_ERR(e);


    return eNOERROR;
}

Four OQL_ASTtoGDS::qg_consPathExpr(PathExprPoolIndex pathExprPoolIndex)
{
    Four                        index, i;
    Four                        matchIndex;
    CataClassInfo               refClass;
    Boolean                     findFlag;
    Boolean                     methodExist;
    PathExprPoolElements        ppElements(m_pool->pathExprPool);
    QGNodePoolElements          queryGraph(m_pool->qgNodePool);
    QGNodePoolElements          cursor(m_pool->qgNodePool);
    QGNodePoolElements          curQGnode(m_pool->qgNodePool);
    MemberPoolElements          usedMemberInfo(m_pool->memberPool);
    JoinInfoPoolElements        explicitJoinInfo(m_pool->joinInfoPool);
    PathExprInfoPoolElements    pathExprInfo(m_pool->pathExprInfoPool);
    Four                        e;
    Boolean                     flag;

	e = m_gds->getQueryGraph(m_pool, queryGraph);
	OOSQL_CHECK_ERR(e);

    cursor.setNull();
    curQGnode.setNull();
    ppElements  = pathExprPoolIndex;
    methodExist = SM_FALSE;

    // find root node
    for(i = 0; i < queryGraph.size; i++)
    {
        // search query graph
        if(((ppElements[0].aliasName != NULL_POOLINDEX && ppElements[0].aliasName == queryGraph[i].aliasName) || 
			 ppElements[0].aliasName == NULL_POOLINDEX) &&
             ppElements[0].classInfo == queryGraph[i].classInfo)
        {
            curQGnode.setPoolIndex(i, 1);
            cursor.setPoolIndex(i, 1);
            break;
        }
    }
    if(curQGnode == NULL_POOLINDEX)
    {
        // cannot find start qg node
        OOSQL_ERR(eNOT_FOUND_OOSQL);
    }

    // scan pathexpression and follow QGnode refclass link
    // if there is a QGnode matched with current pathexpr node scanned,
    // follow it, otherwise create new QGnode.
    for(index = 1;index < ppElements.size; index ++)
    {
        if(ppElements[index].fromAttrKind == PATHEXPR_KIND_METHOD)
            methodExist = SM_TRUE;

        // determine refClass
        if(ppElements[index].domainSubstFlag)                   // check domain substitute
            refClass = ppElements[index].substitutedClass;
        else if(ppElements[index].typeCastingFlag)              // check type casting
            refClass = ppElements[index].typeCastedClass;
        else
            refClass = ppElements[index].classInfo;

        if(index >= 2)  // 0 and 1 has same class, so skip
        {
            // scan queryGraph if first step otherwise scan current QGNode's refClasses
            findFlag   = SM_FALSE;
            matchIndex = -1;

            // scan currentQGNode's refClassList
            cursor = curQGnode[0].refClassList;
                
            while(cursor != NULL_POOLINDEX)
            {
                if(cursor[0].classInfo == refClass)
                {
                    findFlag = SM_TRUE;
                    break;
                }

                cursor = cursor[0].nextRefClass;
            }

            // if find it, navigate through it. Otherwise make new node
            if(findFlag)
                curQGnode = cursor;
            else
            {
                // if curQGnodeIndex was not defined previously, this means there is no starting
                // point in target list
                // In this case, there must be an error in constructing path expression, so
                // this is an internal error
                if(curQGnode == NULL_POOLINDEX)
                    OOSQL_ERR(eINTERNALERROR_OOSQL);

                // make new QG node
                cursor = m_pool->qgNodePool.addNewEntry();
                cursor[0].init();

                // add it to refClassList
                e = qg_addNodeToRefClassList(curQGnode, cursor);
                OOSQL_CHECK_ERR(e);

                curQGnode = cursor;

                // intiialize from member(attr or method) info and class info
                curQGnode[0].classInfo = refClass;
                switch(ppElements[index - 1].fromAttrKind)
                {
                case PATHEXPR_KIND_ATTR:
                    curQGnode[0].implicitJoinAttrKind = QGNODE_KIND_FROMATTR;
                    curQGnode[0].fromAttrInfo         = ppElements[index - 1].attr.attrInfo;
                    break;

                case PATHEXPR_KIND_METHOD:
                    curQGnode[0].implicitJoinAttrKind = QGNODE_KIND_FROMMETHOD;
                    curQGnode[0].fromMethodInfo       = ppElements[index - 1].method.methodInfo;
                    break;
                
                case PATHEXPR_KIND_FUNC:
                case PATHEXPR_KIND_OBJECT:
                case PATHEXPR_KIND_CLASS:
                    curQGnode[0].implicitJoinAttrKind = QGNODE_KIND_FROMNONE;
                    break;

                default:
                    OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
                }
            }
        }

        if(ppElements[index].fromAttrKind == PATHEXPR_KIND_ATTR)
        {
            TypeID typeId;

            e = m_catalog->attr_GetTypeID(ppElements[index].classInfo,
                                        ppElements[index].attr.attrInfo,
                                        typeId);
            OOSQL_CHECK_ERR(e);

            if(typeId == TYPEID_TEXT || 
			   OOSQL_MASK_COMPLEXTYPE(typeId) == COMPLEXTYPEID_SET ||
			   OOSQL_MASK_COMPLEXTYPE(typeId) == COMPLEXTYPEID_BAG ||
			   OOSQL_MASK_COMPLEXTYPE(typeId) == COMPLEXTYPEID_LIST)
                flag = SM_TRUE;			    
            else
            {
                e = qg_isExistInMemberList(curQGnode, ppElements[index], flag);
                OOSQL_CHECK_ERR(e);
            }
		}
		else
		{
			e = qg_isExistInMemberList(curQGnode, ppElements[index], flag);
			OOSQL_CHECK_ERR(e);
		}

        if(!flag)
        {
            // make new usedMemberInfo
            usedMemberInfo = m_pool->memberPool.addNewEntry();

            // add it to usedMemberList
            e = qg_addNodeToUsedMemberList(curQGnode, usedMemberInfo);
            OOSQL_CHECK_ERR(e);
            
            // initialize access member(attr or method) info of the node in access member list
            switch(ppElements[index].fromAttrKind)
            {
            case PATHEXPR_KIND_ATTR:
                usedMemberInfo[0].memberKind    = MEMBER_KIND_ATTR;
                usedMemberInfo[0].attr.attrInfo = ppElements[index].attr.attrInfo;
                break;

            case PATHEXPR_KIND_METHOD:
                usedMemberInfo[0].memberKind        = MEMBER_KIND_METHOD;
                usedMemberInfo[0].method.methodInfo = ppElements[index].method.methodInfo;
                break;
        
            case PATHEXPR_KIND_FUNC:
            case PATHEXPR_KIND_OBJECT:
            case PATHEXPR_KIND_CLASS:
                usedMemberInfo[0].memberKind = MEMBER_KIND_NONE;
                break;

            default:
                OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
            }
        }

        if(!ppElements[index].domainSubstFlag)
        {
            // construct current QGnode's subClasses
            if(curQGnode[0].subClasses == NULL_POOLINDEX)
                e = qg_consSubClasses(curQGnode.getPoolIndex());
            OOSQL_CHECK_ERR(e);
        }
    }

    // make new pathExprInfo
    pathExprInfo = m_pool->pathExprInfoPool.addNewEntry();
    pathExprInfo[0].pathExpr = pathExprPoolIndex;

    // add it to pathExprList
    e = qg_addNodeToPathExprList(curQGnode, pathExprInfo);
    OOSQL_CHECK_ERR(e);

    // set pathexpr's qgNodeNo
    ppElements[ppElements.size - 1].qgNodeNo = curQGnode.getPoolIndex();
    
    // connect QG with Condition tree
    // having condition is excluded from condition tree connected to QG
    if(m_connectionPoint != NULL_POOLINDEX && m_progressStatus != HAVING_CLAUSE)
    {
        ExprPoolElements        epElements(m_pool->exprPool);
        CondListPoolElements    cipElements(m_pool->condInfoPool);
        Boolean                 alreadyConnected;

		m_conditionNotBound = SM_FALSE;

        epElements = m_connectionPoint;

        // check if already epElements inserted in conditionList
        alreadyConnected = SM_FALSE;
        cipElements = curQGnode[0].conditionList;
        while(cipElements != NULL_POOLINDEX)
        {
            if(cipElements[0].expr == epElements)
            {
                alreadyConnected = SM_TRUE;
                break;
            }
            cipElements = cipElements[0].nextCondInfo;
        }

        if(!alreadyConnected)
        {
            cipElements = m_pool->condInfoPool.addNewEntry();
            e = qg_addNodeToConditionList(curQGnode, cipElements);
            OOSQL_CHECK_ERR(e);
        
            cipElements[0].expr          = epElements.getPoolIndex();
            epElements[0].refCount ++;

            if(methodExist)
                epElements[0].operandType = EXPR_OPERANDTYPE_CONTAIN_NOMETHOD;
        }
    }
    
    return eNOERROR;
}

Four OQL_ASTtoGDS::qg_isExistInMemberList(QGNodePoolElements &node, PathExprElement pathExprNode, Boolean& flag)
{
    MemberPoolElements      member(m_pool->memberPool);

    member = node[0].usedMemberList;
    while(member != NULL_POOLINDEX)
    {
        if(pathExprNode.fromAttrKind == PATHEXPR_KIND_ATTR)
        {
            if(member[0].memberKind == MEMBER_KIND_ATTR && 
               member[0].attr.attrInfo == pathExprNode.attr.attrInfo)
            {
                flag = SM_TRUE;
                return eNOERROR;
            }
        }
        else if(pathExprNode.fromAttrKind == PATHEXPR_KIND_METHOD)
        {
            if(member[0].memberKind == MEMBER_KIND_METHOD && 
               member[0].method.methodInfo == pathExprNode.method.methodInfo)
            {
                flag = SM_TRUE;
                return eNOERROR;
            }
        }

        member = member[0].nextMember;
    }

    flag = SM_FALSE;
    return eNOERROR;
}

Four OQL_ASTtoGDS::qg_consSubClasses(QGNodePoolIndex curQGnode)
{
    const Four           subClassArraySize = 20;
    CataClassInfo        subClassArray[subClassArraySize];
    Four                 nSubClasses = subClassArraySize;
    Four                 nthSubClass;
    SubClassPoolElements subClasses(m_pool->subClassPool);
    QGNodePoolElements   cur_qgpElements(m_pool->qgNodePool);
    Four                 i;
    

    if(curQGnode.size == 0)
        OOSQL_ERR(eBADPARAMETER_OOSQL);

    cur_qgpElements = curQGnode;
    cur_qgpElements[0].subClasses.setNull();

    nthSubClass = 0;
    while(nSubClasses == subClassArraySize)
    {
        nSubClasses = m_catalog->class_GetSubClasses(cur_qgpElements[0].classInfo, 
                                                   nthSubClass,
                                                   subClassArraySize,
                                                   subClassArray);
        OOSQL_CHECK_ERR(nSubClasses);
        if(nSubClasses == 0)
            break;

        // make new QGnode
        subClasses = m_pool->subClassPool.addNewEntry(nSubClasses);
        for(i = 0; i < nSubClasses; i++)
        {
            subClasses[i].subClassInfo  = subClassArray[i];
            subClasses[i].indexInfo.setNull();				// determined in GDStoCommonAP      
            subClasses[i].condNodes.setNull();				// determined in GDStoCommonAP      
        }

        // if cur_qgpElements is not initialized to indicate subclasses, do it.
        if(cur_qgpElements[0].subClasses == NULL_POOLINDEX)
            cur_qgpElements[0].subClasses.startIndex = subClasses.startIndex;

        cur_qgpElements[0].subClasses.size += nSubClasses;

        nthSubClass += nSubClasses; 
    }

    return eNOERROR;
}

Four OQL_ASTtoGDS::qg_addNodeToRefClassList(QGNodePoolElements &node, QGNodePoolElements &addedNode)
{
    if(node == NULL_POOLINDEX || addedNode == NULL_POOLINDEX)
        OOSQL_ERR(eBADPARAMETER_OOSQL);
    
    addedNode[0].nextRefClass = node[0].refClassList;
    node[0].refClassList = addedNode.getPoolIndex();

    return eNOERROR;
}

Four OQL_ASTtoGDS::qg_addNodeToUsedMemberList(QGNodePoolElements &node, MemberPoolElements &addedNode)
{
    if(node == NULL_POOLINDEX || addedNode == NULL_POOLINDEX)
        OOSQL_ERR(eBADPARAMETER_OOSQL);

    addedNode[0].nextMember = node[0].usedMemberList;
    node[0].usedMemberList = addedNode.getPoolIndex();

    return eNOERROR;
}

Four OQL_ASTtoGDS::qg_addNodeToExplicitJoinList(QGNodePoolElements &node, JoinInfoPoolElements &addedNode)
{
    if(node == NULL_POOLINDEX || addedNode == NULL_POOLINDEX)
        OOSQL_ERR(eBADPARAMETER_OOSQL);

    addedNode[0].nextJoinInfo = node[0].explicitJoinList;
    node[0].explicitJoinList  = addedNode.getPoolIndex();

    return eNOERROR;
}

Four OQL_ASTtoGDS::qg_addNodeToPathExprList(QGNodePoolElements &node, PathExprInfoPoolElements &addedNode)
{
    if(node == NULL_POOLINDEX || addedNode == NULL_POOLINDEX)
        OOSQL_ERR(eBADPARAMETER_OOSQL);

    addedNode[0].nextPathExprInfo = node[0].pathExprList;
    node[0].pathExprList          = addedNode.getPoolIndex();

    return eNOERROR;
}

Four OQL_ASTtoGDS::qg_addNodeToConditionList(QGNodePoolElements &node, CondListPoolElements &addedNode)
{
    if(node == NULL_POOLINDEX || addedNode == NULL_POOLINDEX)
        OOSQL_ERR(eBADPARAMETER_OOSQL);

    addedNode[0].nextCondInfo = node[0].conditionList;
    node[0].conditionList = addedNode.getPoolIndex();

    return eNOERROR;
}

Four OQL_ASTtoGDS::collectionTypeID_to_TypeID(CollectionTypeID collectionTypeID, TypeID &typeID)
{
    switch(collectionTypeID)
    {
    case COLTYPE_SET:
        typeID = TYPEID_SET;
        break;
    case COLTYPE_BAG:
        typeID = TYPEID_BAG;
        break;
    case COLTYPE_LIST:
        typeID = TYPEID_LIST;
        break;
    case COLTYPE_ARRAY:
        typeID = TYPEID_ARRAY;
        break;
    }
    return eNOERROR;
}

Four OQL_ASTtoGDS::nodeName_to_CollectionTypeID(Four nodeName, CollectionTypeID &collectionType)
{
    switch(nodeName)
    {
    case QuCnsSet:
        collectionType   = COLTYPE_SET;
        break;
    case QuCnsBag:
        collectionType   = COLTYPE_BAG;
        break;
    case QuCnsLst:
        collectionType   = COLTYPE_LIST;
        break;
    case QuCnsArr:
        collectionType   = COLTYPE_ARRAY;
        break;
    default:
        OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
        break;
    }

    return eNOERROR;
}

Four OQL_ASTtoGDS::typeID_to_ColElementTypeID(TypeID typeID, ColElementTypeID &colElementType)
{
    switch(typeID)
    {
    case TYPEID_INT:
        colElementType = COLELMTYPE_INTEGER;
        break;
    case TYPEID_FLOAT:
        colElementType = COLELMTYPE_REAL;
        break;
    case TYPEID_STRING:
        colElementType = COLELMTYPE_STRING;
        break;
    case TYPEID_BOOL:
        colElementType = COLELMTYPE_BOOL;
        break;
    case TYPEID_MBR:
        colElementType = COLELMTYPE_MBR;
        break;
    default:
        OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
        break;
    }

    return eNOERROR;
}

Four OQL_ASTtoGDS::exprElementSetTypeInfo(ExprPoolElements &epElements, ReturnType& returnType)
{
    ValuePoolElements       vpElements(m_pool->valuePool);
    StructurePoolElements   spElements(m_pool->structurePool);
    ExprPoolElements        operand1(m_pool->exprPool), operand2(m_pool->exprPool);
    PathExprPoolElements    pathExpr(m_pool->pathExprPool);
    Four                    e;

    // fill ExprElement.typeID, ExprElement.complexType, ExprElement.size
    if(OOSQL_MASK_COMPLEXTYPE(returnType.typeID) == OOSQL_COMPLEXTYPE_SET       || OOSQL_MASK_COMPLEXTYPE(returnType.typeID) == OOSQL_COMPLEXTYPE_BAG ||
       OOSQL_MASK_COMPLEXTYPE(returnType.typeID) == OOSQL_COMPLEXTYPE_LIST      || OOSQL_MASK_COMPLEXTYPE(returnType.typeID) == OOSQL_COMPLEXTYPE_ODMG_SET ||
	   OOSQL_MASK_COMPLEXTYPE(returnType.typeID) == OOSQL_COMPLEXTYPE_ODMG_BAG  || OOSQL_MASK_COMPLEXTYPE(returnType.typeID) == OOSQL_COMPLEXTYPE_ODMG_LIST || 
	   OOSQL_MASK_COMPLEXTYPE(returnType.typeID) == OOSQL_COMPLEXTYPE_ODMG_ARRAY)
    {
        epElements[0].resultType = returnType.typeID;
        switch(OOSQL_MASK_COMPLEXTYPE(returnType.typeID))
        {
        case OOSQL_COMPLEXTYPE_SET:
		case OOSQL_COMPLEXTYPE_ODMG_SET:
            epElements[0].complexType = COMPLEX_TYPE_SET;
            break;
        case OOSQL_COMPLEXTYPE_BAG:
		case OOSQL_COMPLEXTYPE_ODMG_BAG:
            epElements[0].complexType = COMPLEX_TYPE_BAG;
            break;
        case OOSQL_COMPLEXTYPE_LIST:
		case OOSQL_COMPLEXTYPE_ODMG_LIST:
            epElements[0].complexType = COMPLEX_TYPE_LIST;
            break;
		case OOSQL_COMPLEXTYPE_ODMG_ARRAY:
            epElements[0].complexType = COMPLEX_TYPE_ARRAY;
            break;
        }
        
		if(epElements[0].exprKind == EXPR_KIND_PATHEXPR)
		{
			Four length;

			pathExpr = epElements[0].pathExpr;
			if(pathExpr[pathExpr.size - 1].fromAttrKind == PATHEXPR_KIND_ATTR)
			{
				e = m_catalog->attr_GetLength(pathExpr[pathExpr.size - 1].classInfo, pathExpr[pathExpr.size - 1].attr.attrInfo, length);
				OOSQL_CHECK_ERR(e);
			}
			else
				length = -1;

			epElements[0].resultLength = length;
		}
		else
			epElements[0].resultLength = -1;
    }
    else
    {
        epElements[0].resultType  = returnType.typeID;
        epElements[0].complexType = COMPLEX_TYPE_SIMPLE;
        switch(returnType.typeID)
        {
        case TYPEID_NONE:
            epElements[0].resultLength = TYPEID_NONE_SIZE;
            break;
        case TYPEID_NULL:
            epElements[0].resultLength = TYPEID_NULL_SIZE;
            break;
        case TYPEID_SHORT:
            epElements[0].resultLength = TYPEID_SHORT_SIZE;
            break;
        case TYPEID_INT:
            epElements[0].resultLength = TYPEID_INT_SIZE;
            break;
        case TYPEID_LONG:
            epElements[0].resultLength = TYPEID_LONG_SIZE;
            break;
        case TYPEID_LONG_LONG:
            epElements[0].resultLength = TYPEID_LONG_LONG_SIZE;
            break;
        case TYPEID_DOUBLE:
            epElements[0].resultLength = TYPEID_DOUBLE_SIZE;
            break;
        case TYPEID_FLOAT:
            epElements[0].resultLength = TYPEID_FLOAT_SIZE;
            break;
		case TYPEID_VARSTRING:
        case TYPEID_STRING:         // variable
            // if epElements[0].exprKind is a 
            //  EXPR_KIND_PATHEXPR : get max string size from m_catalog
            //  EXPR_KIND_AGGRFUNC : err
            //  EXPR_KIND_FUNCTION : returntype of function
            //  EXPR_KIND_OPER     : max(operand1's size, operand2's size)
            //  EXPR_KIND_VALUE    : get string literial size
            //  EXPR_KIND_CONS     : err
            switch(epElements[0].exprKind)
            {
            case EXPR_KIND_PATHEXPR:
                // get max string size from m_catalog
                pathExpr = epElements[0].pathExpr;
                
                if(pathExpr[pathExpr.size - 1].fromAttrKind == PATHEXPR_KIND_ATTR)
                {
                    e = m_catalog->attr_GetLength(pathExpr[pathExpr.size - 1].classInfo, 
                                                pathExpr[pathExpr.size - 1].attr.attrInfo, 
                                                epElements[0].resultLength);
                    OOSQL_CHECK_ERR(e);
                }
                else if(pathExpr[pathExpr.size - 1].fromAttrKind == PATHEXPR_KIND_METHOD)
                {
                    e = m_catalog->method_GetReturnTypeLength(pathExpr[pathExpr.size - 1].classInfo, 
                                                              pathExpr[pathExpr.size - 1].method.methodInfo, 
                                                              epElements[0].resultLength);
                    OOSQL_CHECK_ERR(e);
                }
                else
                {
                    epElements[0].resultLength = TYPEID_OID_SIZE;
                }

                break;
            case EXPR_KIND_AGGRFUNC:
                OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
            case EXPR_KIND_FUNCTION:
                OOSQL_ERR(eNOTIMPLEMENTED_OOSQL);
            case EXPR_KIND_OPER:
                // max(operand1's size, operand2's size) 
                operand1 = epElements[0].oper.operand1;
                operand2 = epElements[0].oper.operand2;

                epElements[0].resultLength = max(operand1[0].resultLength, operand2[0].resultLength);
                break;
            case EXPR_KIND_VALUE:
                // get string literial size
                vpElements = epElements[0].value;
#ifdef OOSQL_DEBUG
                if(vpElements[0].valueKind != VALUE_KIND_STRING)
                    OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
#endif
                epElements[0].resultLength = vpElements[0].string.size;
                break;
            case EXPR_KIND_CONS:
                OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
            default:
                OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
            }
            
            break;
        case TYPEID_STRUCTURE:      // variable
            // if epElements[0].exprKind is a 
            //  EXPR_KIND_PATHEXPR : err
            //  EXPR_KIND_AGGRFUNC : err
            //  EXPR_KIND_FUNCTION : err
            //  EXPR_KIND_OPER     : max(operand1's size, operand2's size)
            //  EXPR_KIND_VALUE    : get structure literial size
            //  EXPR_KIND_CONS     : err
            switch(epElements[0].exprKind)
            {
            case EXPR_KIND_PATHEXPR:
                OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
            case EXPR_KIND_AGGRFUNC:
                OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
            case EXPR_KIND_FUNCTION:
				epElements[0].resultLength = typeID_ToTypeLength(returnType.typeID);
				break;
            case EXPR_KIND_OPER:
                // max(operand1's size, operand2's size) 
                operand1 = epElements[0].oper.operand1;
                operand2 = epElements[0].oper.operand2;

                epElements[0].resultLength = max(operand1[0].resultLength, operand2[0].resultLength);
                break;
            case EXPR_KIND_VALUE:
                // get structure literial size
                OOSQL_ERR(eNOTIMPLEMENTED_OOSQL);
            case EXPR_KIND_CONS:
                OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
            default:
                OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
            }
            epElements[0].resultLength = TYPEID_STRUCTURE_SIZE;
            break;
        case TYPEID_OID:
            epElements[0].resultLength = TYPEID_OID_SIZE;
            break;
        case TYPEID_NIL:
            epElements[0].resultLength = TYPEID_NIL_SIZE;
            break;
        case TYPEID_BOOL:
            epElements[0].resultLength = TYPEID_BOOL_SIZE;
            break;
        case TYPEID_MBR:
            epElements[0].resultLength = TYPEID_MBR_SIZE;
            break;
        case TYPEID_ID:
            epElements[0].resultLength = TYPEID_ID_SIZE;
            break;
        case TYPEID_DOMAIN:
            epElements[0].resultLength = TYPEID_DOMAIN_SIZE;
            break;
        case TYPEID_TEXT:
            epElements[0].resultLength = TYPEID_TEXT_SIZE;
            break;
		case TYPEID_DATE:
			epElements[0].resultLength = TYPEID_DATE_SIZE;
			break;
		case TYPEID_TIME:
			epElements[0].resultLength = TYPEID_TIME_SIZE;
			break;
		case TYPEID_TIMESTAMP:
			epElements[0].resultLength = TYPEID_TIMESTAMP_SIZE;
			break;
		case TYPEID_INTERVAL:
			epElements[0].resultLength = TYPEID_INTERVAL_SIZE;
			break;
		case TYPEID_COMPLEX:
			epElements[0].resultLength = 0;
			break;
		case TYPEID_OGIS_GEOMETRY:
			epElements[0].resultLength = TYPEID_OGIS_GEOMETRY_SIZE;
			break;
		case TYPEID_OGIS_POINT:
			epElements[0].resultLength = TYPEID_OGIS_POINT_SIZE;
			break;
		case TYPEID_OGIS_LINESTRING:
			epElements[0].resultLength = TYPEID_OGIS_LINESTRING_SIZE;
			break;
		case TYPEID_OGIS_POLYGON:
			epElements[0].resultLength = TYPEID_OGIS_POLYGON_SIZE;
			break;
		case TYPEID_OGIS_GEOMETRYCOLLECTION:
			epElements[0].resultLength = TYPEID_OGIS_GEOMETRYCOLLECTION_SIZE;
			break;
		case TYPEID_OGIS_MULTIPOINT:
			epElements[0].resultLength = TYPEID_OGIS_MULTIPOINT_SIZE;
			break;
		case TYPEID_OGIS_MULTILINESTRING:
			epElements[0].resultLength = TYPEID_OGIS_MULTILINESTRING_SIZE;
			break;
		case TYPEID_OGIS_MULTIPOLYGON:
			epElements[0].resultLength = TYPEID_OGIS_MULTIPOLYGON_SIZE;
			break;
        default:
			OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
        }
    }
    return eNOERROR;
}

Four OQL_ASTtoGDS::function_GetTypeID(Four functionID)
{
    Four    i;

    // convert m_ast node name to operator id
    struct FuncID_to_Type_Entry {
        FunctionID  functionID;
        TypeID      type;
    };

    static FuncID_to_Type_Entry funcID_ToType_Table[] = {
        {FUNCTION_GEO_NEAREST,      TYPEID_OID},            // OOSQL_StorageManager::OID of spatial class
        {FUNCTION_GEO_BOUNDARY,     TYPEID_OID},            // OOSQL_StorageManager::OID of spatial class
        {FUNCTION_GEO_INTERIOR,     TYPEID_OID},            // area struct
        {FUNCTION_GEO_BUFFER,       TYPEID_OID},            // area struct
        {FUNCTION_GEO_UNION,        TYPEID_OID},            // area struct
        {FUNCTION_GEO_INTERSECT,    TYPEID_OID},            // area struct
        {FUNCTION_GEO_DIFFERENCE,   TYPEID_OID},            // area struct
        {FUNCTION_GEO_DISTANCE,     TYPEID_DOUBLE},          
        {FUNCTION_GEO_AREA,         TYPEID_DOUBLE}, 
        {FUNCTION_GEO_LENGTH,       TYPEID_DOUBLE}, 
        {FUNCTION_TEXTIR_WEIGHT,    TYPEID_DOUBLE},
        {FUNCTION_TEXTIR_NMATCH,    TYPEID_DOUBLE},
        {FUNCTION_TEXTIR_MATCH,     TYPEID_DOUBLE},
        {FUNCTION_NONE,             TYPEID_NONE}            // end of table
    };

    for(i = 0; funcID_ToType_Table[i].functionID != FUNCTION_NONE; i++)
    {
        if(funcID_ToType_Table[i].functionID == functionID)
        {
            return (Four)funcID_ToType_Table[i].type;
        }
    }

    OOSQL_ERR(eUNDEFINED_FUNCTIONID_USED_OOSQL);			// bad function id
}

Four OQL_ASTtoGDS::function_GetLength(Four functionID)
{
    Four    i;

    // convert m_ast node name to operator id
    struct FuncID_to_Length_Entry {
        FunctionID  functionID;
        Four        length;
    };

    static FuncID_to_Length_Entry funcID_ToLength_Table[] = {
        {FUNCTION_GEO_NEAREST,      TYPEID_OID_SIZE},           // OOSQL_StorageManager::OID of spatial class
        {FUNCTION_GEO_BOUNDARY,     TYPEID_OID_SIZE},           // OOSQL_StorageManager::OID of spatial class
        {FUNCTION_GEO_INTERIOR,     TYPEID_OID_SIZE},           // area struct
        {FUNCTION_GEO_BUFFER,       TYPEID_OID_SIZE},           // area struct
        {FUNCTION_GEO_UNION,        TYPEID_OID_SIZE},           // area struct
        {FUNCTION_GEO_INTERSECT,    TYPEID_OID_SIZE},           // area struct
        {FUNCTION_GEO_DIFFERENCE,   TYPEID_OID_SIZE},           // area struct
        {FUNCTION_GEO_DISTANCE,     TYPEID_DOUBLE_SIZE},         
        {FUNCTION_GEO_AREA,         TYPEID_DOUBLE_SIZE}, 
        {FUNCTION_GEO_LENGTH,       TYPEID_DOUBLE_SIZE}, 
        {FUNCTION_TEXTIR_WEIGHT,    TYPEID_DOUBLE_SIZE},
        {FUNCTION_TEXTIR_NMATCH,    TYPEID_DOUBLE_SIZE},
        {FUNCTION_TEXTIR_MATCH,     TYPEID_DOUBLE_SIZE},
        {FUNCTION_NONE,             TYPEID_NONE_SIZE}           // end of table
    };

    for(i = 0; funcID_ToLength_Table[i].functionID != FUNCTION_NONE; i++)
    {
        if(funcID_ToLength_Table[i].functionID == functionID)
        {
            return (Four)funcID_ToLength_Table[i].length;
        }
    }

    OOSQL_ERR(eUNDEFINED_FUNCTIONID_USED_OOSQL);				// bad function id
}

Four parseDateString(char* dateString, Two_Invariable& year, Two_Invariable& month, Two_Invariable& day)
{
	char              string[256];
	Four              i, index;
	char              dilimiter;

	// initialize
	year  = 0;
	month = 0;
	day   = 0;

	// check dilimiter
	for(i = 0; i < (int)strlen(dateString); i++)
	{
		if('0' <= dateString[i] && dateString[i] <= '9')
			continue;
		if(dateString[i] == '-')
		{
			dilimiter = '-';
			break;
		}
		else if(dateString[i] == '/')
		{
			dilimiter = '/';
			break;
		}
		else if(dateString[i] == '.')
		{
			dilimiter = '.';
			break;
		}
		else
			OOSQL_ERR(eBAD_DATE_STRING_OOSQL);	
	}

	// parse date string
	string[0] = '\0';
	for(i = 0, index = 0; i < (int)strlen(dateString) && (dateString[i] != dilimiter); i++)
	{
		if('0' <= dateString[i] && dateString[i] <= '9')
			string[index++] = dateString[i];
		else
			OOSQL_ERR(eBAD_DATE_STRING_OOSQL);	
	}
	if(string[0] != '\0')
	{
		i ++;
		string[index] = '\0';
		if(dilimiter == '/' || dilimiter == '.')
			month = atoi(string);
		else
			year  = atoi(string);
	}

	string[0] = '\0';
	for(index = 0; i < (int)strlen(dateString) && (dateString[i] != dilimiter); i++)
	{
		if('0' <= dateString[i] && dateString[i] <= '9')
			string[index++] = dateString[i];
		else
			OOSQL_ERR(eBAD_DATE_STRING_OOSQL);
	}
	if(string[0] != '\0')
	{
		i ++;
		string[index] = '\0';
		if(dilimiter == '/' || dilimiter == '.')
			day  = atoi(string);
		else
			month = atoi(string);
	}

	string[0] = '\0';
	for(index = 0; i < (int)strlen(dateString) && (dateString[i] != dilimiter && dateString[i] != ' '); i++)
	{
		if('0' <= dateString[i] && dateString[i] <= '9')
			string[index++] = dateString[i];
		else
			OOSQL_ERR(eBAD_DATE_STRING_OOSQL);
	}
	if(string[0] != '\0')
	{
		i ++;
		string[index] = '\0';
		if(dilimiter == '/' || dilimiter == '.')
			year = atoi(string);
		else
			day  = atoi(string);
	}

	return i;
}

Four parseTimeString(char* timeString, Two_Invariable& hour, Two_Invariable& minute, float& second)
{
	char              string[256];
	Four              i, index;
	char              dilimiter;

	// initialize
	hour   = 0;
	minute = 0;
	second = 0;

	// check dilimiter
	for(i = 0; i < (int)strlen(timeString); i++)
	{
		if('0' <= timeString[i] && timeString[i] <= '9')
			continue;
		if(timeString[i] == ':')
		{
			dilimiter = ':';
			break;
		}
		else if(timeString[i] == '.')
		{
			dilimiter = '.';
			break;
		}
		else
			OOSQL_ERR(eBAD_TIME_STRING_OOSQL);	
	}

	// parse time string
	string[0] = '\0';
	for(i = 0, index = 0; i < (int)strlen(timeString) && (timeString[i] != dilimiter); i++)
	{
		if('0' <= timeString[i] && timeString[i] <= '9')
			string[index++] = timeString[i];
		else
			OOSQL_ERR(eBAD_TIME_STRING_OOSQL);
	}
	if(string[0] != '\0')
	{
		i ++;
		string[index] = '\0';
		hour = atoi(string);
	}

	string[0] = '\0';
	for(index = 0; i < (int)strlen(timeString) && (timeString[i] != dilimiter); i++)
	{
		if('0' <= timeString[i] && timeString[i] <= '9')
			string[index++] = timeString[i];
		else
			OOSQL_ERR(eBAD_TIME_STRING_OOSQL);
	}
	if(string[0] != '\0')
	{
		i ++;
		string[index] = '\0';
		minute = atoi(string);
	}

	string[0] = '\0';
	for(index = 0; i < (int)strlen(timeString) && (timeString[i] != dilimiter); i++)
	{
		if('0' <= timeString[i] && timeString[i] <= '9')
			string[index++] = timeString[i];
		else
			OOSQL_ERR(eBAD_TIME_STRING_OOSQL);
	}
	if(string[0] != '\0')
	{
		i ++;
		string[index] = '\0';
		second = (float)atof(string);
	}

	return i;
}

Four OQL_ASTtoGDS::smtChkValueLiteralDate(ASTNodeIdx node, ReturnType& returnType)
{
	ValuePoolElements vpElements(m_pool->valuePool);
	DatePoolElements  dpElements(m_pool->datePool);
	Four              e;
	Two_Invariable    year, month, day;
	SimpleString      dateString;

	// get time string
	year  = 0;
	month = 1;
	day   = 1;
	if(AST(AST(node).son).nodeName == STRING)
	{
		e = getStringFromSTRING_Node(dateString, AST(node).son);
		OOSQL_CHECK_ERR(e);
		
		e = parseDateString(dateString, year, month, day);
		OOSQL_CHECK_ERR(e);
	}
	else if(AST(AST(node).son).nodeName == INTEGER)
	{
		year = m_pool->intPool[AST(AST(node).son).tokenVal];
	}
	else
		OOSQL_ERR(eBADASTNODE_OOSQL);
	
	// construct GDS
	dpElements          = m_pool->datePool.addNewEntry();
	dpElements[0].year  = year;
	dpElements[0].month = month;
	dpElements[0].day   = day;

	vpElements              = m_pool->valuePool.addNewEntry();
	vpElements[0].valueKind = VALUE_KIND_DATE;
	vpElements[0].date      = dpElements.getPoolIndex();
		
	returnType.set(TYPEID_DATE, PT_VALUEPOOL, vpElements.getPoolIndex());

	return eNOERROR;
}

Four OQL_ASTtoGDS::smtChkValueLiteralTime(ASTNodeIdx node, ReturnType& returnType)
{
	ValuePoolElements vpElements(m_pool->valuePool);
	TimePoolElements  tpElements(m_pool->timePool);
	Four              e;
	Two_Invariable	  hour, minute;
	float			  second;
	SimpleString      timeString;

	// get time string
	e = getStringFromSTRING_Node(timeString, AST(node).son);
	OOSQL_CHECK_ERR(e);

	// parse time string
	hour   = 0;
	minute = 0;
	second = 0;
	e = parseTimeString(timeString, hour, minute, second);
	OOSQL_CHECK_ERR(e);

	// construct GDS
	tpElements           = m_pool->timePool.addNewEntry();
	tpElements[0].hour   = hour;
	tpElements[0].minute = minute;
	tpElements[0].second = second;

	vpElements              = m_pool->valuePool.addNewEntry();
	vpElements[0].valueKind = VALUE_KIND_TIME;
	vpElements[0].time      = tpElements.getPoolIndex();
		
	returnType.set(TYPEID_TIME, PT_VALUEPOOL, vpElements.getPoolIndex());

	return eNOERROR;
}

Four OQL_ASTtoGDS::smtChkValueLiteralTimestamp(ASTNodeIdx node, ReturnType& returnType)
{
	ValuePoolElements		vpElements(m_pool->valuePool);
	TimestampPoolElements	tspElements(m_pool->timestampPool);
	Four					e;
	int						index;
	Two_Invariable		    year, month, day;
	Two_Invariable		    hour, minute;
	float					second;
	SimpleString			timestampString;

	// get time string
	e = getStringFromSTRING_Node(timestampString, AST(node).son);
	OOSQL_CHECK_ERR(e);

	// parse timestamp string
	year  = 0;
	month = 1;
	day   = 1;
	if(AST(AST(node).son).nodeName == STRING)
	{
		e = getStringFromSTRING_Node(timestampString, AST(node).son);
		OOSQL_CHECK_ERR(e);

		e = parseDateString(&timestampString[0], year, month, day);
		OOSQL_CHECK_ERR(e);
		index = e;
	}
	else if(AST(AST(node).son).nodeName == INTEGER)
	{
		year = m_pool->intPool[AST(AST(node).son).tokenVal];
	}
	else
		OOSQL_ERR(eBADASTNODE_OOSQL);

	hour   = 0;
	minute = 0;
	second = 0;
	e = parseTimeString(&timestampString[index], hour, minute, second);
	OOSQL_CHECK_ERR(e);

	// construct GDS
	tspElements           = m_pool->timestampPool.addNewEntry();
	tspElements[0].year   = year;
	tspElements[0].month  = month;
	tspElements[0].day    = day;
	tspElements[0].hour   = hour;
	tspElements[0].minute = minute;
	tspElements[0].second = second;

	vpElements              = m_pool->valuePool.addNewEntry();
	vpElements[0].valueKind = VALUE_KIND_TIMESTAMP;
	vpElements[0].timestamp = tspElements.getPoolIndex();
		
	returnType.set(TYPEID_TIMESTAMP, PT_VALUEPOOL, vpElements.getPoolIndex());

	return eNOERROR;
}

Four OQL_ASTtoGDS::smtChkValueLiteralInterval(ASTNodeIdx node, ReturnType& returnType)
{
	OOSQL_ERR(eNOTIMPLEMENTED_OOSQL);	
}

Four OQL_ASTtoGDS::smtChkValueLiteralComplex(ASTNodeIdx node, ReturnType& returnType)
{
	ValuePoolElements vpElements(m_pool->valuePool);
	ValuePoolElements complexValues(m_pool->valuePool);
	ASTNodeIdx		  sComplexValue;
	ReturnType		  complexReturnType;
    Four              e;
	TypeID			  elementType = TYPEID_NONE;

	if(AST(node).nodeName != VaLtComplex)
		OOSQL_ERR(eUNHANDLED_CASE_OOSQL);

	sComplexValue = AST(node).son;
	if(AST(sComplexValue).nodeName == null)
	{
		complexValues.setNull();
	}
	else
	{
		complexValues.setNull();
		while(sComplexValue != AST_NULL)
		{
			e = smtChkValue(sComplexValue, complexReturnType);
			OOSQL_CHECK_ERR(e);

			if(elementType == TYPEID_NONE)
				elementType = complexReturnType.typeID;
			else if(elementType != complexReturnType.typeID)
			{
				m_errorMessage->Clear();
				m_errorMessage->Append("Elements in complex type are not compatible with each other\n");
				OOSQL_ERR(eINVALID_VALUE_OOSQL);
			}

			if(complexValues == NULL_POOLINDEX)
				complexValues = complexReturnType.poolIndex;
			else
			{
				if(complexReturnType.poolType != PT_VALUEPOOL)
				{
					OOSQL_ERR(eINTERNALERROR_OOSQL);
				}
				if(complexReturnType.poolIndex.startIndex != (complexValues.startIndex + complexValues.size))
				{
					OOSQL_ERR(eINTERNALERROR_OOSQL);
				}
				complexValues.size ++;
			}

			sComplexValue = AST(sComplexValue).brother;
		}
	}

	vpElements = m_pool->valuePool.addNewEntry();
	vpElements[0].valueKind = VALUE_KIND_COMPLEX;
	vpElements[0].complex   = complexValues.getPoolIndex();

	returnType.set(TYPEID_COMPLEX, PT_VALUEPOOL, vpElements.getPoolIndex());

	return eNOERROR;
}

Four OQL_ASTtoGDS::smtChkValueText(ASTNodeIdx node, ReturnType& returnType)
{
	ValuePoolElements		vpElements(m_pool->valuePool);
	Four					sText, sUpdateMode;

	sUpdateMode = AST(node).son;
	sText       = AST(sUpdateMode).brother;

	vpElements              = m_pool->valuePool.addNewEntry();
	vpElements[0].valueKind = VALUE_KIND_TEXT;
	if(AST(sText).nodeName == STRING)
	{
		vpElements[0].text.setPoolIndex(m_pool->stringIndexPool[AST(sText).tokenVal],
			                            m_pool->stringIndexPool[AST(sText).tokenVal + 1] -
				                        m_pool->stringIndexPool[AST(sText).tokenVal] - 1);
	}
	else
		vpElements[0].text.setNull();

	if(AST(sUpdateMode).nodeName == TextUpdModeDeferred)
		vpElements[0].textUpdateMode = TEXT_UPDATE_MODE_DEFERRED;
	else
		vpElements[0].textUpdateMode = TEXT_UPDATE_MODE_IMMEDIATE;
		
	returnType.set(TYPEID_TEXT, PT_VALUEPOOL, vpElements.getPoolIndex());

	return eNOERROR;
}

TypeID OQL_ASTtoGDS::typeNameToTypeID(SimpleString& typeName)
{
	if(typeName == (char* )"char" || typeName == (char* )"string")	
		return TYPEID_STRING;
	else if(typeName == (char* )"varchar" || typeName == (char* )"varstring")	
		return TYPEID_VARSTRING;
	else if(typeName == (char* )"integer")	
		return TYPEID_INT;
	else if(typeName == (char* )"long")		
		return TYPEID_LONG;
	else if(typeName == (char* )"longlong")	
		return TYPEID_LONG_LONG;
	else if(typeName == (char* )"smallint")	
		return TYPEID_SHORT;
	else if(typeName == (char* )"numeric")	
		return TYPEID_INT;
	else if(typeName == (char* )"decimal")	
		return TYPEID_INT;
	else if(typeName == (char* )"float")	
		return TYPEID_FLOAT;
	else if(typeName == (char* )"real")	
		return TYPEID_FLOAT;
	else if(typeName == (char* )"double")	
		return TYPEID_DOUBLE;
	else if(typeName == (char* )"date")		
		return TYPEID_DATE;
	else if(typeName == (char* )"time")		
		return TYPEID_TIME;
	else if(typeName == (char* )"timestamp")
		return TYPEID_TIMESTAMP;
	else if(typeName == (char* )"text")		
		return TYPEID_TEXT;
	else if(typeName == (char* )"oid")	
		return TYPEID_OID;
	else if(typeName == (char*)"geometry")
		return TYPEID_OGIS_GEOMETRY;
	else if(typeName == (char*)"point")
		return TYPEID_OGIS_POINT;
	else if(typeName == (char*)"linestring")
		return TYPEID_OGIS_LINESTRING;
	else if(typeName == (char*)"polygon")
		return TYPEID_OGIS_POLYGON;
	else if(typeName == (char*)"geometrycollection")
		return TYPEID_OGIS_GEOMETRYCOLLECTION;
	else if(typeName == (char*)"multipoint")
		return TYPEID_OGIS_MULTIPOINT;
	else if(typeName == (char*)"multilinestring")
		return TYPEID_OGIS_MULTILINESTRING;
	else if(typeName == (char*)"multipolygon")
		return TYPEID_OGIS_MULTIPOLYGON;

	return TYPEID_NONE;
}

Four OQL_ASTtoGDS::typeID_ToTypeLength(TypeID type)
{
	switch(type)
	{
	case TYPEID_SHORT:
		return TYPEID_SHORT_SIZE;
    case TYPEID_INT:
		return TYPEID_INT_SIZE;
    case TYPEID_LONG:
		return TYPEID_LONG_SIZE;
    case TYPEID_LONG_LONG:
		return TYPEID_LONG_LONG_SIZE;
    case TYPEID_FLOAT:
		return TYPEID_FLOAT_SIZE;
    case TYPEID_DOUBLE:
		return TYPEID_DOUBLE;
    case TYPEID_STRING:
		return TYPEID_STRING_SIZE;
    case TYPEID_OID:
		return TYPEID_OID_SIZE;
    case TYPEID_MBR:
		return TYPEID_MBR_SIZE;
    case TYPEID_STRUCTURE:
		return TYPEID_STRUCTURE_SIZE;
    case TYPEID_NIL:
		return TYPEID_NIL_SIZE;
    case TYPEID_BOOL:
		return TYPEID_BOOL_SIZE;
    case TYPEID_ID:
		return TYPEID_ID_SIZE;
    case TYPEID_DOMAIN:
		return TYPEID_DOMAIN_SIZE;
    case TYPEID_NONE:
		return TYPEID_NONE_SIZE;
    case TYPEID_NULL:
		return TYPEID_NULL_SIZE;
    case TYPEID_SET:
		return TYPEID_SET_SIZE;
	case TYPEID_BAG:
		return TYPEID_BAG_SIZE;
    case TYPEID_LIST:
		return TYPEID_LIST_SIZE;
    case TYPEID_ARRAY:
		return TYPEID_ARRAY_SIZE;
	case TYPEID_TEXT:
		return TYPEID_TEXT_SIZE;
    case TYPEID_DATE:
		return TYPEID_DATE_SIZE;
    case TYPEID_TIME:
		return TYPEID_TIME_SIZE;
    case TYPEID_TIMESTAMP:
		return TYPEID_TIMESTAMP_SIZE;
    case TYPEID_INTERVAL:
		return TYPEID_INTERVAL_SIZE;
	case TYPEID_VARSTRING:
		return TYPEID_VARSTRING_SIZE;
    case TYPEID_PAGEID:
		return TYPEID_PAGEID_SIZE;
    case TYPEID_FILEID:
		return TYPEID_FILEID_SIZE;
    case TYPEID_INDEXID:
		return TYPEID_INDEXID_SIZE;
    case TYPEID_OGIS_GEOMETRY:
		return TYPEID_OGIS_GEOMETRY_SIZE;
    case TYPEID_OGIS_POINT:
		return TYPEID_OGIS_POINT_SIZE;
    case TYPEID_OGIS_LINESTRING:
		return TYPEID_OGIS_LINESTRING_SIZE;
    case TYPEID_OGIS_POLYGON:
		return TYPEID_OGIS_POLYGON_SIZE;
    case TYPEID_OGIS_GEOMETRYCOLLECTION:
		return TYPEID_OGIS_GEOMETRYCOLLECTION_SIZE;
    case TYPEID_OGIS_MULTIPOINT:
		return TYPEID_OGIS_MULTIPOINT_SIZE;
    case TYPEID_OGIS_MULTILINESTRING:
		return TYPEID_OGIS_MULTILINESTRING_SIZE;
    case TYPEID_OGIS_MULTIPOLYGON:
		return TYPEID_OGIS_MULTIPOLYGON_SIZE;
	default:
		OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
	}

	return 0;
}

Four OQL_ASTtoGDS::typeNameToTypeLength(SimpleString& typeName)
{
    if(typeName == (char*)"char" || typeName == (char*)"string")
        return 0;
    else if(typeName == (char*)"varchar" || typeName == (char*)"varstring")
        return 0;
    else if(typeName == (char*)"integer")
        return TYPEID_INT_SIZE;
	else if(typeName == (char*)"long")
		return TYPEID_LONG_SIZE;
	else if(typeName == (char*)"longlong")
		return TYPEID_LONG_LONG_SIZE;
    else if(typeName == (char*)"smallint")
        return TYPEID_SHORT_SIZE;
    else if(typeName == (char*)"numeric")
        return TYPEID_INT_SIZE;
    else if(typeName == (char*)"decimal")
        return TYPEID_INT_SIZE;
    else if(typeName == (char*)"float")
        return TYPEID_FLOAT_SIZE;
    else if(typeName == (char*)"real")
        return TYPEID_FLOAT_SIZE;
    else if(typeName == (char*)"double")
        return TYPEID_DOUBLE_SIZE;
    else if(typeName == (char*)"date")
        return TYPEID_DATE_SIZE;
    else if(typeName == (char*)"time")
        return TYPEID_TIME_SIZE;
    else if(typeName == (char*)"timestamp")
        return TYPEID_TIMESTAMP_SIZE;
    else if(typeName == (char*)"text")
        return TYPEID_TEXT_SIZE;
    else if(typeName == (char*)"oid")
        return TYPEID_OID_SIZE;
    else if(typeName == (char*)"geometry")
        return TYPEID_OGIS_GEOMETRY_SIZE;
    else if(typeName == (char*)"point")
        return TYPEID_OGIS_POINT_SIZE;
    else if(typeName == (char*)"linestring")
        return TYPEID_OGIS_LINESTRING_SIZE;
    else if(typeName == (char*)"polygon")
        return TYPEID_OGIS_POLYGON_SIZE;
    else if(typeName == (char*)"geometrycollection")
        return TYPEID_OGIS_GEOMETRYCOLLECTION_SIZE;
    else if(typeName == (char*)"multipoint")
        return TYPEID_OGIS_MULTIPOINT_SIZE;
    else if(typeName == (char*)"multilinestring")
        return TYPEID_OGIS_MULTILINESTRING_SIZE;
    else if(typeName == (char*)"multipolygon")
        return TYPEID_OGIS_MULTIPOLYGON_SIZE;


    OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
    return 0;
}

StringPoolIndex OQL_ASTtoGDS::installStringIntoStringPool(char* string)
{
	Four				i;
	Four				length;
	StringPoolElements	stringPoolElements(m_pool->stringPool);

	length = strlen(string);

	stringPoolElements = m_pool->stringPool.addNewEntry(length + 1);
	for(i = 0; i < length; i++)
		stringPoolElements[i] = string[i];
	stringPoolElements[i] = '\0';

	return stringPoolElements.getPoolIndex();
}

/* get stemizer through output parameter */
Four OQL_ASTtoGDS::getStemizerFuncPtr(CataClassInfo classInfo, CataAttrInfo attrInfo, StemizerFuncPtr* stemizer)
{
	Four classId;
	Two  colNo;
	Four ocn;
	Four e;

	e = m_catalog->class_ClassInfo_to_ClassId(classInfo, classId);
	OOSQL_CHECK_ERR(e);

	e = m_storageManager->GetOpenClassNum(m_volId, classId);
	OOSQL_CHECK_ERR(e);
	ocn = e;

	e = m_catalog->attr_AttrInfo_to_ColNo(classInfo, attrInfo, colNo);
	OOSQL_CHECK_ERR(e);

	e = m_storageManager->Text_GetStemizerFPtr(ocn, colNo, (void **)stemizer);
	OOSQL_CHECK_ERR(e);

	return eNOERROR;
}

Four OQL_ASTtoGDS::isSameValue(ValuePoolElements &a, ValuePoolElements &b)
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

Four OQL_ASTtoGDS::isSameDomain(DomainPoolElements &a, DomainPoolElements &b)
{
    if(a[0].domainClass != b[0].domainClass)
        return SM_FALSE;

    if(a[0].starFlag != b[0].starFlag)
        return SM_FALSE;

    return SM_TRUE;
}

Four OQL_ASTtoGDS::isSameAggrFunc(AggrFuncPoolElements &a, AggrFuncPoolElements &b)
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

Four OQL_ASTtoGDS::isSameArgument(ArgumentPoolElements &a, ArgumentPoolElements &b)
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

Four OQL_ASTtoGDS::isSameFunction(FunctionPoolElements &a, FunctionPoolElements &b)
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

Four OQL_ASTtoGDS::isSameObject(ObjectPoolElements &a, ObjectPoolElements &b)
{
    OOSQL_ERR(eNOTIMPLEMENTED_OOSQL);
    return SM_TRUE;
}

Four OQL_ASTtoGDS::isSamePathExpr(PathExprPoolElements &a, PathExprPoolElements &b)
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
