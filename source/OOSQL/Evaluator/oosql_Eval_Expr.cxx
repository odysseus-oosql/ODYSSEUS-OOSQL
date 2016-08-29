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

/*
    MODULE:
        oosql_Eval_Expr.cxx

    DESCRIPTION:

    IMPORTS:

    EXPORTS:
*/

#include "OOSQL_Evaluator.hxx"
#include "OOSQL_ExternalFunctionManager.hxx"
#include "OOSQL_ExternalFunctionDispatcher.hxx"
#include "oosql_Eval_Expr.hxx"
#include "UDF.hxx"
#include <math.h>
#include <string.h>

Four OOSQL_Evaluator::checkWhereCond()
/*
    Function:
        Check where condition for the current access plan element.

    Side effect:

    Referenced member variables:
        m_currPlanIndex

    Return value:
        SM_TRUE    if the newly read-in object satisfies the corresponding predicate(s)
        FASLE   otherwise
*/
{
    OOSQL_DB_Value			res(pMemoryManager);    /* result of evaluating expression */
    Four					i;						/* loop iteration variable */
    Four					e;                      /* error code */
	AP_CondListPoolElements whereCondNodes(m_pool->ap_condListPool);

    m_isCheckingHavingCond = SM_FALSE;
	whereCondNodes = m_currWhereCondNodes;
    for (i = 0; i < GET_POOLSIZE(whereCondNodes); i++) 
	{
        /* check the where condition which is expressed as DNF(Disjunctive Normal Form) */
        /*
        e = evalExpression(&(whereCondNodes[i].expr), &res);
        if (e < eNOERROR) OOSQL_ERR(e);

        if (res.data.i == SM_FALSE) 
            return SM_FALSE;
        */
        e = evalExpression(&(whereCondNodes[i].expr), &res);
        OOSQL_CHECK_ERR(e);

		if (res.nullFlag)
			return SM_FALSE;

		e = res.convertType(OOSQL_TYPE_BOOL);
		OOSQL_CHECK_ERR(e);
		if (res.data.b == 0) 
			return SM_FALSE;
    }

    return SM_TRUE;
}

Four OOSQL_Evaluator::checkHavingCond()
/*
    Function:

    Side effect:

    Return value:
*/
{
    AP_CondListPoolElements condList;				/* ptr. to condition list for the current access plan elem. */
    OOSQL_DB_Value			res(pMemoryManager);    /* result of evaluating expression */
    Four					i;                      /* loop iteration variable */
    Four					e;                      /* error code */

    condList = ACCESSPLAN.getHavingCondPool(m_currPlanIndex);

    m_isCheckingHavingCond = SM_TRUE;		

    for (i = 0; i < GET_POOLSIZE(condList); i++) {
        /* check condition expressed as DNF(Disjunctive Normal Form) */
        e = evalExpression( &(condList[i].expr), &res );
        if (e < eNOERROR) 
            OOSQL_ERR(e);

		if (res.nullFlag)
			return SM_FALSE;

		e = res.convertType(OOSQL_TYPE_BOOL);
		OOSQL_CHECK_ERR(e);
		if (res.data.b == 0) 
			return SM_FALSE;
    }

    m_isCheckingHavingCond = SM_FALSE;		

    return SM_TRUE;
}

Four OOSQL_Evaluator::evalFunction(
        AP_FuncEvalInfo		 *funcInfo,      // IN: index to expression m_pool
        OOSQL_DB_Value       *res            // OUT: evaluation result
)
/*
    Function:
        Evaluate an expression node of which kind is an function.

    Side effect:

    Return value:
        An instance of DB_Value containing the evaluation result.
*/
{
    Four    e;

    /* check input parameters */
#ifdef  OOSQL_DEBUG
    if (funcInfo == NULL) 
        OOSQL_ERR(eBADPARAMETER_OOSQL);
    if (res == NULL) 
        OOSQL_ERR(eBADPARAMETER_OOSQL);
#endif

    /* call appropriate evaluation function according to the function ID */
    switch (funcInfo->functionID) 
    {
    case FUNCTION_TEXTIR_MATCH:
        e = execTextIR_FnMatch(funcInfo, res);
        if (e < eNOERROR) OOSQL_ERR(e);
        break;

    case FUNCTION_GEO_DISTANCE:
    	e = geoSpatialArithDistance(funcInfo, res);
    	if(e < eNOERROR) OOSQL_ERR(e);
    	break;

    case FUNCTION_GEO_AREA:
    	e = geoSpatialArithArea(funcInfo, res);
    	if(e < eNOERROR) OOSQL_ERR(e);
        break;

    case FUNCTION_GEO_LENGTH:
    	e = geoSpatialArithLength(funcInfo, res);
    	if(e < eNOERROR) OOSQL_ERR(e);
        break;

	case FUNCTION_USER_DEFINED:
		e = execUserDefinedFunction(funcInfo, res);
		if(e < eNOERROR) OOSQL_ERR(e);
		break;

    default:    /* invalid function ID */
        OOSQL_ERR(eINVALID_FUNC_OOSQL);
    }

    /* return */
    return eNOERROR;
}

Four OOSQL_Evaluator::evalExpression(
    AP_ExprPoolIndex    *exprIdx,       // IN: index to a expression m_pool (denotes a disjunct)
    OOSQL_DB_Value		*res            // OUT: evaluation result
)
/*
    Function:
        Evaluate a predicate formed as a disjunct.

    Side effect:

    Return value:
        SM_TRUE    if the evaluation result is SM_TRUE
        SM_FALSE   otherwise
*/
{
    AP_ExprElement*			exprNode;
    AP_ColNoMapPoolElements	pColNoMap;			/* column no. mapping structure */
    AP_UsedColPoolElements	pColInfoPool;       /* used column info. */
    Four					planNo;             /* index to access plan element */
    Four					colNo;              /* column number */
    Four					mappedColNo;        /* column number */
    ValueElement*			pValue;             /* ptr. to value m_pool element */
    Four					e;                  /* error code */

    exprNode = ACCESSPLAN.getExprElem(*exprIdx);

    switch (ACCESSPLAN.getExprElem(*exprIdx)->exprKind) {
        case EXPR_KIND_PATHEXPR:
			e = evalExpression(&ACCESSPLAN.getExprElem(*exprIdx)->pathExpr, res);
			OOSQL_CHECK_ERR(e);

            break;

        case EXPR_KIND_AGGRFUNCRESULT:
            e = evalAggrFuncResult( exprIdx, res );
            if (e < eNOERROR) 
                OOSQL_ERR(e);
            break;

        /* evaluate function and get the result */
        case EXPR_KIND_FUNCEVAL:
            e = evalExpression( &(ACCESSPLAN.getExprElem(*exprIdx)->funcEval), res );
            OOSQL_CHECK_ERR(e);
            break;

        /* get the function result which is already evaluated 
         * and stored in the evaluation buffer
         */
        case EXPR_KIND_FUNCRESULT:
            e = evalFunctionResult( &(ACCESSPLAN.getExprElem(*exprIdx)->funcResult), res );
            if (e < eNOERROR)
                OOSQL_ERR(e);
            break;

        case EXPR_KIND_VALUE:
			e = evalExpression(&ACCESSPLAN.getExprElem(*exprIdx)->value, res, exprNode->resultType);
			OOSQL_CHECK_ERR(e);
            break;

        case EXPR_KIND_OPER:
            e = evalOperator( &(ACCESSPLAN.getExprElem(*exprIdx)->oper), res );
            if (e < eNOERROR)
                OOSQL_ERR(e);
            break;

        case EXPR_KIND_CONS:
            OOSQL_ERR( eNOTIMPLEMENTED_CONSEXPR_OOSQL);

        default:
            OOSQL_ERR( eINVALID_EXPRESSION_OOSQL);
    }

    /* return */
    return(eNOERROR);
}

Four OOSQL_Evaluator::evalOperator(
    AP_OperatorStruct	*op,         /* IN: index to expression m_pool element */
    OOSQL_DB_Value		*res         /* OUT: evaluation result */
)
/*
    Function:
        Evaluate an expression node of which kind is an operator.

    Side effect:

    Return value:
        DB_Value containing the evaluation result.
*/
{
    OOSQL_DB_Value	op1(pMemoryManager), op2(pMemoryManager);  // result of each operand
    Four			cmpResult;
    Four			e;             // return code

	res->nullFlag = SM_FALSE; // default setting

    switch (op->operatorId) {

        /*********************
         * Boolean Operators *
         *********************/

        case OP_OR: /* OpBlnOr: Boolean Operator Or */
            // evaluate the 1st operand recursively 
            e = evalExpression( &(op->operand1), &op1 );
            if (e < eNOERROR)
                OOSQL_ERR(e);

			if (op1.nullFlag)
			{
				//evaluate the 2nd operand recursively
				e = evalExpression( &(op->operand2), &op2 );
				if (e < eNOERROR)
					OOSQL_ERR(e);
				
				if (op2.nullFlag) {
					res->nullFlag = SM_TRUE;
				}
				else if (op2.data.i == SM_TRUE) {
					res->data.i = SM_TRUE;
					res->SetType(OOSQL_TYPE_INT);
				}
				else {  // second operand is FALSE
                    res->nullFlag = SM_TRUE;
				}
			}
			else if (op1.data.i == SM_TRUE) {
				res->data.i = SM_TRUE;
				res->SetType(OOSQL_TYPE_INT);
			}
			else {  // op1.data.i == SM_FALSE
				// evaluate the 2nd operand recursively 
				e = evalExpression( &(op->operand2), &op2 );
				if (e < eNOERROR)
					OOSQL_ERR(e);
				
				if (op2.nullFlag)
				{
					res->nullFlag = SM_TRUE;
				}
				else if (op2.data.i == SM_TRUE) {
					res->data.i = SM_TRUE;
					res->SetType(OOSQL_TYPE_INT);
				}
				else {  /* second operand is FALSE */
					res->data.i = SM_FALSE;
					res->SetType(OOSQL_TYPE_INT);
				}
			}
			break;

        case OP_NOT:    /* OpBlnNot: Boolean Operator Not */
            e = evalExpression( &(op->operand1), &op1 );
            if (e < eNOERROR)
                OOSQL_ERR(e);

			if(op1.nullFlag)
			{
				res->nullFlag = SM_TRUE;
			}
			else
			{
				res->data.i = (op1.data.i == SM_TRUE)? SM_FALSE: SM_TRUE;
				res->SetType(OOSQL_TYPE_INT);
			}

            break;

        case OP_AND:    /* OpBlnAnd: Boolean Operator And */
            // evaluate the 1st operand recursively 
            e = evalExpression( &(op->operand1), &op1 );
            if (e < eNOERROR)
                OOSQL_ERR(e);


			if (op1.nullFlag) {
				// evaluate the 2nd operand recursively 
				e = evalExpression( &(op->operand2), &op2 );
				if (e < eNOERROR)
					OOSQL_ERR(e);
				
				if (op2.nullFlag) {
					res->nullFlag = SM_TRUE;
				}
				else { // second operand is not NULL(UNKNOWN)
					e = op2.convertType(OOSQL_TYPE_BOOL);
					OOSQL_CHECK_ERR(e);
					if (op2.data.b == SM_TRUE) {
						res->nullFlag = SM_TRUE;
					}
					else {
						res->data.i = SM_FALSE;
						res->SetType(OOSQL_TYPE_INT);
					}
				}
			}
			else // first operand is not NULL(UNKNOWN)
			{
				e = op1.convertType(OOSQL_TYPE_BOOL);
				OOSQL_CHECK_ERR(e);
				
				if (op1.data.i == SM_TRUE) { // first operand is TRUE
					// evaluate the 2nd operand recursively 
					e = evalExpression( &(op->operand2), &op2 );
					if (e < eNOERROR)
						OOSQL_ERR(e);
					
					if (op2.nullFlag) { 
						res->nullFlag = SM_TRUE;
					}
					else { // second operand is not NULL(UNKNOWN)
						e = op2.convertType(OOSQL_TYPE_BOOL);
						OOSQL_CHECK_ERR(e);						
						if (op2.data.b == SM_TRUE) {
							res->data.i = SM_TRUE;
							res->SetType(OOSQL_TYPE_INT);
						}
						else {
							res->data.i = SM_FALSE;
							res->SetType(OOSQL_TYPE_INT);
						}
					}
				}
				else { // first operand is FALSE
					res->data.i = SM_FALSE;
					res->SetType(OOSQL_TYPE_INT);
				}
			}
			break;
				
        /************************
         * Arithmetic Operators *
         ************************/

        case OP_PLUS:   /* OpBiPlu: Binary Operator Plus */
            e = arithAdd( &(op->operand1), &(op->operand2), res );
            if (e < eNOERROR)
                OOSQL_ERR(e);
            break;

        case OP_MINUS:  /* OpBiMin: Binary Operator Minus */
            e = arithSub( &(op->operand1), &(op->operand2), res );
            if (e < eNOERROR)
                OOSQL_ERR(e);
            break;

        case OP_MULTIPLY:       /* OpBiMul: Binary Operator Multiplication */
            e = arithMul( &(op->operand1), &(op->operand2), res );
            if (e < eNOERROR)
                OOSQL_ERR(e);
            break;

        case OP_DIVIDE: /* OpBiDiv: Binary Operator Division */
            e = arithDiv( &(op->operand1), &(op->operand2), res );
            if (e < eNOERROR)
                OOSQL_ERR(e);
            break;

        case OP_MOD:    /* OpBiMod: Binary Operator Modulo */
            e = arithMod( &(op->operand1), &(op->operand2), res );
            if (e < eNOERROR)
                OOSQL_ERR(e);
            break;

        case OP_UNARY_MINUS:    /* OpUnMin: Unary Operator Minus */
            e = arithUnMin( &(op->operand1), res );
            if (e < eNOERROR)
                OOSQL_ERR(e);
            break;

        case OP_ABS:    /* OpUnAbs: Unary Operator Abstract value */
            e = arithAbs( &(op->operand1), res );
            if (e < eNOERROR)
                OOSQL_ERR(e);
            break;

        case OP_STRING_CONCAT:  /* OpBiStrcat: Binary Operator String concatenation */
            e = stringCat( &(op->operand1), &(op->operand2), res );
            if (e < eNOERROR)
                OOSQL_ERR(e);
            break;

        /************************
         * comparison operators *
         ************************/

        case OP_LIKE:   /* OpCmpLike: Comparison Operator Like (string matching) */
            e = evalOpLike( &(op->operand1), &(op->operand2), res );
            if (e < eNOERROR) 
                OOSQL_ERR(e);
            break;

        case OP_EQ:     /* OpCmpEq: Comparison Operator = */
        case OP_NE:     /* OpCmpNe: Comparison Operator: != */

            // compare two operands according to the operator type
            switch (op->operatorType) {
                case ARITHMETIC_OPERATION:
                    cmpResult = arithCmp( &(op->operand1), &(op->operand2) );
                    break;

                case STRING_OPERATION:
                    cmpResult = stringCmp( &(op->operand1), &(op->operand2) );
                    break;

                case OID_OPERATION:
                    cmpResult = oidCmp( &(op->operand1), &(op->operand2) );
                    break;

                case OBJECT_OPERATION:
                    cmpResult = objectCmp( &(op->operand1), &(op->operand2) );
                    break;

                default:
                    OOSQL_ERR( eILLEGAL_OP_OOSQL);
            }

            // return the evaluation result
			if ( cmpResult == CMP_UN ) {
				res->nullFlag = SM_TRUE;
			}
			else if ( op->operatorId == OP_EQ ) {
                res->data.i = (cmpResult == CMP_EQ)? SM_TRUE: SM_FALSE;
				res->SetType(OOSQL_TYPE_INT);		
            }
            else {      /* OP_NE */
                res->data.i = (cmpResult != CMP_EQ)? SM_TRUE: SM_FALSE;
				res->SetType(OOSQL_TYPE_INT);		
            }
            break;

        case OP_GT:     /* OpCmpGt: Comparison Operator > */
        case OP_LT:     /* OpCmpLt: Comparison Operator < */
        case OP_GE:     /* OpCmpGe: Comparison Operator >= */
        case OP_LE:     /* OpCmpLe: Comparison Operator <= */

            // compare two operands according to the operator type
            switch (op->operatorType) {
                case ARITHMETIC_OPERATION:
                    cmpResult = arithCmp( &(op->operand1), &(op->operand2) );
                    break;

                case STRING_OPERATION:
                    cmpResult = stringCmp( &(op->operand1), &(op->operand2) );
                    break;

                case COLLECTION_OPERATION:      /* set or bag inclusion */
                    cmpResult = collectionCmp( &(op->operand1), &(op->operand2) );
                    break;

                default:
                    OOSQL_ERR( eILLEGAL_OP_OOSQL);
            }

			if ( cmpResult == CMP_UN ) {
				res->nullFlag = SM_TRUE;
				break;
			}
			// return SM_TRUE/FLASE based on the comparison result
            switch (op->operatorId) {
                case OP_GT: 
                    res->data.i = (cmpResult == CMP_GT)? SM_TRUE: SM_FALSE;
					res->SetType(OOSQL_TYPE_INT);		
                    break;

                case OP_LT: 
                    res->data.i = (cmpResult == CMP_LT)? SM_TRUE: SM_FALSE;
					res->SetType(OOSQL_TYPE_INT);		
                    break;

                case OP_GE: 
                    res->data.i = (cmpResult == CMP_GT || cmpResult == CMP_EQ)? SM_TRUE: SM_FALSE;
					res->SetType(OOSQL_TYPE_INT);		
                    break;

                case OP_LE: 
                    res->data.i = (cmpResult == CMP_LT || cmpResult == CMP_EQ)? SM_TRUE: SM_FALSE;
					res->SetType(OOSQL_TYPE_INT);		
                    break;

                default:
                    OOSQL_ERR( eILLEGAL_OP_OOSQL);
            }
            break;
		case OP_ISNULL:		/* OpCmpIsNull: Comparison Operator IS NULL */
		case OP_ISNOTNULL:  /* OpCmpIsNotNull: Comparison Operator IS NOT NULL */
			// compare one operand with NULL according to the operator type
			switch (op->operatorType) {
				case NULLCHECK_OPERATION:
					cmpResult = nullCmp( &(op->operand1));
					break;
					
				default:
					OOSQL_ERR( eILLEGAL_OP_OOSQL);
			}
			switch (op->operatorId) {
				case OP_ISNULL:
					res->data.i = (cmpResult == CMP_EQ)? SM_TRUE: SM_FALSE;
					res->SetType(OOSQL_TYPE_INT);
					break;
					
				case OP_ISNOTNULL:
					res->data.i = (cmpResult != CMP_EQ)? SM_TRUE: SM_FALSE;
					res->SetType(OOSQL_TYPE_INT);
					break;
					
				default:
					OOSQL_ERR( eILLEGAL_OP_OOSQL);
			}
			break;


        /*****************
         * set operators *
         *****************/

        case OP_INTERSECT:      /* OpSetInt: Set Operator Intersect */
			OOSQL_ERR( eNOTIMPLEMENTED_SETOP_OOSQL);
        case OP_UNION:			/* OpSetUni: Set Operator Union */
			e = collectionUnion(&(op->operand1), &(op->operand2));
			OOSQL_CHECK_ERR(e);
			break;
        case OP_EXCEPT:			/* OpSetExc: Set Operator Except */
			e = collectionExcept(&(op->operand1), &(op->operand2));
			OOSQL_CHECK_ERR(e);
		case OP_IN:
			cmpResult   = collectionCmpIn(&(op->operand1), &(op->operand2));
			res->data.i = cmpResult;
			res->SetType(OOSQL_TYPE_INT);
			break;
			
		/*********************
         * OpenGIS operators *
         *********************/
#ifndef SLIMDOWN_OPENGIS
		case OP_OGIS_GEOMETRYFT:
		case OP_OGIS_POINTFT:
		case OP_OGIS_LINESTRINGFT:
		case OP_OGIS_POLYGONFT:
		case OP_OGIS_MULTIPOINTFT:
		case OP_OGIS_MULTILINESTRINGFT:
		case OP_OGIS_MULTIPOLYGONFT:
		case OP_OGIS_GEOMETRYCOLLECTIONFT:
		case OP_OGIS_GEOMETRYFB:
		case OP_OGIS_POINTFB:
		case OP_OGIS_LINESTRINGFB:
		case OP_OGIS_POLYGONFB:
		case OP_OGIS_MULTIPOINTFB:
		case OP_OGIS_MULTILINESTRINGFB:
		case OP_OGIS_MULTIPOLYGONFB:
		case OP_OGIS_GEOMETRYCOLLECTIONFB:
		case OP_OGIS_ASTEXT:
		case OP_OGIS_ASBINARY:
		case OP_OGIS_DIMENSION:
		case OP_OGIS_GEOMETRYTYPE:
		case OP_OGIS_SRID:
		case OP_OGIS_BOUNDARY:
		case OP_OGIS_LENGTH:
		case OP_OGIS_X:
		case OP_OGIS_Y:
		case OP_OGIS_AREA:
		case OP_OGIS_NUMGEOMETRIES:
		case OP_OGIS_NUMPOINTS:
		case OP_OGIS_NUMINTERIORRINGS:
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
		case OP_OGIS_DIFFERENCE:
		case OP_OGIS_INTERSECTION:
		case OP_OGIS_SYMDIFFERENCE:
		case OP_OGIS_UNION:
		case OP_OGIS_DISTANCE:
		case OP_OGIS_ENVELOPE:
		case OP_OGIS_BUFFER:
		case OP_OGIS_CONVEXHULL:
		case OP_OGIS_EXTERIORRING:
		case OP_OGIS_INTERIORRINGN:
		case OP_OGIS_CENTRIOD:
		case OP_OGIS_STARTPOINT:
		case OP_OGIS_ENDPOINT:
		case OP_OGIS_POINTONSURFACE:
		case OP_OGIS_POINTN:
		case OP_OGIS_GEOMETRYN:
            #ifdef ENABLE_OPENGIS_OPTIMIZATION
            if (op->isConstant == SM_TRUE && op->isEvaluated == SM_TRUE)
            {
                // The result is already calculated. Therefore, we only fetch the result.
                ValueElement*       resultValueElem;
                StringPoolElements  data(m_pool->stringPool);
                Four                resultSize;
                Four                resultIndex;
        
                // result size = size of type, nullFlag, data, length of data (+ size of variable length data)
                resultSize = sizeof(Two) + sizeof(Boolean) + sizeof(res->data) + sizeof(Four) ;
        
                resultValueElem = ACCESSPLAN.getValueElem(op->result);
        
                data = resultValueElem->string;
        
                resultIndex = 0;
        
                // get type
                memcpy(&(res->type), &data[resultIndex], sizeof(Two));
                resultIndex = resultIndex + sizeof(Two);
        
                // get nullFlag
                memcpy(&(res->nullFlag), &data[resultIndex], sizeof(Boolean));
                resultIndex = resultIndex + sizeof(Boolean);
        
                // get data
                memcpy(&(res->data), &data[resultIndex], sizeof(res->data));
                resultIndex = resultIndex + sizeof(res->data);
        
                // get length of data
                memcpy(&(res->length), &data[resultIndex], sizeof(Four));
                resultIndex = resultIndex + sizeof(Four);
        
                // in the case of variable data, we get the data from the pointer result->data.ptr
                if(res->type == OOSQL_TYPE_STRING || res->type == OOSQL_TYPE_VARSTRING)
                {
                    e = res->PrepareData(res->length); // should check meaning
                    OOSQL_CHECK_ERR(e);
        
                    memcpy(res->data.ptr, &data[resultIndex], res->length);
                }
            }
            else
            {
				e = evalOGISOperator(op, *res);
				OOSQL_CHECK_ERR(e);

                // if the user-defined function is constant, save the result in funcInfo->result
                if (op->isConstant == SM_TRUE)
                {
                    ValueElement*       resultValueElem;
                    StringPoolElements  data(m_pool->stringPool);
                    Four                resultSize;
                    Four                resultIndex;
        
                    // get ValueElement indexed by funcInfo->result
                    if(op->result != NULL_POOLINDEX)
                    {
                        resultValueElem = ACCESSPLAN.getValueElem(op->result);
                    }
                    else
                    {
                        op->result = m_pool->valuePool.addNewEntry();
                        resultValueElem = ACCESSPLAN.getValueElem(op->result);
                    }
        
                    // set funcInfo->isEvaluated
                    op->isEvaluated = SM_TRUE;
        
                    // store result
                    if(res->nullFlag == SM_TRUE)
                    {
                        resultValueElem->valueKind = VALUE_KIND_NIL;
                        resultValueElem->nil = NULL_POOLINDEX;
                    }
                    else
                    {
                        // result size = size of type, nullFlag, data, length of data (+ size of variable length data)
                        resultSize = sizeof(Two) + sizeof(Boolean) + sizeof(res->data) + sizeof(Four) ;
        
                        // in the case of variable data, we use extra memory space(result->length)
                        if(res->type == OOSQL_TYPE_STRING || res->type == OOSQL_TYPE_VARSTRING)
                            resultSize = resultSize + res->length;
        
                        // We use ValuElement to save result, and the result is packed in the string field of ValueElement
                        resultValueElem->valueKind = VALUE_KIND_STRING;
                        resultValueElem->string = m_pool->stringPool.addNewEntry(resultSize);
        
                        data = resultValueElem->string;
        
                        resultIndex = 0;
        
                        // store type
                        memcpy(&data[resultIndex], &(res->type), sizeof(Two));
                        resultIndex = resultIndex + sizeof(Two);
        
                        // store nullFlag
                        memcpy(&data[resultIndex], &(res->nullFlag), sizeof(Boolean));
                        resultIndex = resultIndex + sizeof(Boolean);
        
                        // store data
                        memcpy(&data[resultIndex], &(res->data), sizeof(res->data));
                        resultIndex = resultIndex + sizeof(res->data);
        
                        // store length of data
                        memcpy(&data[resultIndex], &(res->length), sizeof(Four));
                        resultIndex = resultIndex + sizeof(Four);
        
                        // in the case of variable data, we should store the data the pointer result->data.ptr is pointing
                        if(res->type == OOSQL_TYPE_STRING || res->type == OOSQL_TYPE_VARSTRING)
                            memcpy(&data[resultIndex], res->data.ptr, res->length);
                    } // end of if(result->nullFlag == SM_TRUE) else
                } // end of if (*(op->isConstant) == SM_TRUE)
            } // end of if (*(op->isConstant) == SM_TRUE && funcInfo->op == SM_TRUE) else
            #else
			e = evalOGISOperator(op, *res);
			OOSQL_CHECK_ERR(e);
			#endif
			break;
#endif

        default:
            OOSQL_ERR(eILLEGAL_OP_OOSQL);
    } /* enf of switch */

    // return the evaluation result
    return eNOERROR;
}

Four OOSQL_Evaluator::collectionUnion(
    AP_ExprPoolIndex	*op1Idx,	// IN: index to the 1st operand
    AP_ExprPoolIndex	*op2Idx)	// IN: index to the 2nd operand
{
	OOSQL_DB_Value						op1(pMemoryManager), op2(pMemoryManager);	// the evaluation result for each operand
	Four								e;
	AP_ExprElement*						exprNode1;
	AP_ExprElement*						exprNode2;
	AP_ExprElement*						exprNode;
	Four								planNo;
	Four								colNo;
	Four								ocn;
	OOSQL_StorageManager::OID			oid;
	Four								elementSize;
	void*								element;
	ValuePoolElements					value, complexValues(m_pool->valuePool);
	Four								j;
	Two_Invariable						shortAttr;
	Four_Invariable						intAttr;
	Eight_Invariable					eightAttr;
	float								floatAttr;
	double								doubleAttr;
	char*								stringBuffer;
	OOSQL_StorageManager::Date			dateAttr;
	OOSQL_StorageManager::Time			timeAttr;
	OOSQL_StorageManager::Timestamp		timestampAttr;
	Four								pos;

	exprNode1 = ACCESSPLAN.getExprElem(*op1Idx);
	exprNode2 = ACCESSPLAN.getExprElem(*op2Idx);

	if(exprNode1->resultType == TYPEID_COMPLEX && exprNode2->resultType == TYPEID_COMPLEX)
	{
		OOSQL_ERR(eNOTIMPLEMENTED_OOSQL)
	}
	else if(exprNode1->resultType == TYPEID_COMPLEX || exprNode2->resultType == TYPEID_COMPLEX)
	{
		if(exprNode1->resultType == TYPEID_COMPLEX)
		{
			e = evalExpression(op1Idx, &op1);
			if (e < eNOERROR) OOSQL_ERR(e);

			planNo  = ACCESSPLAN.getExprElem(*op1Idx)->pathExpr.col.planNo;
			colNo   = ACCESSPLAN.getExprElem(*op1Idx)->pathExpr.col.colNo;
			ocn		= EVAL_ACCESSLISTTABLE[planNo].getCurrOcn();
			memcpy(&oid, EVAL_EVALBUFFER[planNo].getOID_Ptr(), sizeof(OOSQL_StorageManager::OID));

			exprNode = exprNode2;
		}
		else if(exprNode2->resultType == TYPEID_COMPLEX)
		{
			e = evalExpression(op2Idx, &op2);
			if (e < eNOERROR) OOSQL_ERR(e);

			planNo  = ACCESSPLAN.getExprElem(*op1Idx)->pathExpr.col.planNo;
			colNo   = ACCESSPLAN.getExprElem(*op1Idx)->pathExpr.col.colNo;
			ocn		= EVAL_ACCESSLISTTABLE[planNo].getCurrOcn();
			memcpy(&oid, EVAL_EVALBUFFER[planNo].getOID_Ptr(), sizeof(OOSQL_StorageManager::OID));

			exprNode = exprNode1;
		}

		value         = exprNode2->value.getElements(m_pool->valuePool);
		complexValues = value[0].complex;

		for(j = 0; j < complexValues.size; j++)
		{
			stringBuffer = NULL;
			switch(OOSQL_MASK_TYPE(exprNode->resultType))
			{
			case TYPEID_SHORT:
				if(complexValues[j].valueKind == VALUE_KIND_INTEGER)
				{
					shortAttr   = complexValues[j].integer.getElements(m_pool->intPool, 0);
					elementSize = sizeof(short);
					element     = &shortAttr;
				}
				else if(complexValues[j].valueKind == VALUE_KIND_REAL)
				{
					shortAttr   = complexValues[j].real.getElements(m_pool->realPool, 0);
					elementSize = sizeof(short);
					element     = &shortAttr;
				}
				else
					OOSQL_ERR(eNOTIMPLEMENTED_OOSQL); // unhandled case error
				break;
			case TYPEID_INT:
			case TYPEID_LONG:
				if(complexValues[j].valueKind == VALUE_KIND_INTEGER)
				{
					intAttr   = complexValues[j].integer.getElements(m_pool->intPool, 0);
					elementSize = sizeof(int);
					element     = &intAttr;
				}
				else if(complexValues[j].valueKind == VALUE_KIND_REAL)
				{
					intAttr   = complexValues[j].real.getElements(m_pool->realPool, 0);
					elementSize = sizeof(int);
					element     = &intAttr;
				}
				else
					OOSQL_ERR(eNOTIMPLEMENTED_OOSQL); // unhandled case error
				break;
			case TYPEID_LONG_LONG:
				if(complexValues[j].valueKind == VALUE_KIND_INTEGER)
				{
					eightAttr   = complexValues[j].integer.getElements(m_pool->intPool, 0);
					elementSize = sizeof(Eight_Invariable);
					element     = &eightAttr;
				}
				else if(complexValues[j].valueKind == VALUE_KIND_REAL)
				{
					eightAttr   = complexValues[j].real.getElements(m_pool->realPool, 0);
					elementSize = sizeof(Eight_Invariable);
					element     = &eightAttr;
				}
				else
					OOSQL_ERR(eNOTIMPLEMENTED_OOSQL); // unhandled case error
				break;
			case TYPEID_FLOAT:
				if(complexValues[j].valueKind == VALUE_KIND_INTEGER)
				{
					floatAttr   = complexValues[j].integer.getElements(m_pool->intPool, 0);
					elementSize = sizeof(float);
					element     = &floatAttr;
				}
				else if(complexValues[j].valueKind == VALUE_KIND_REAL)
				{
					floatAttr   = complexValues[j].real.getElements(m_pool->realPool, 0);
					elementSize = sizeof(float);
					element     = &floatAttr;
				}
				else
					OOSQL_ERR(eNOTIMPLEMENTED_OOSQL); // unhandled case error
				break;
			case TYPEID_DOUBLE:
				if(complexValues[j].valueKind == VALUE_KIND_INTEGER)
				{
					doubleAttr  = complexValues[j].integer.getElements(m_pool->intPool, 0);
					elementSize = sizeof(float);
					element     = &doubleAttr;
				}
				else if(complexValues[j].valueKind == VALUE_KIND_REAL)
				{
					doubleAttr  = complexValues[j].real.getElements(m_pool->realPool, 0);
					elementSize = sizeof(float);
					element     = &doubleAttr;
				}
				else
					OOSQL_ERR(eNOTIMPLEMENTED_OOSQL); // unhandled case error
				break;
			case TYPEID_STRING:
				if(complexValues[j].valueKind == VALUE_KIND_STRING)
				{
					elementSize = exprNode->resultLength;

					stringBuffer = (char*)pMemoryManager->Alloc(elementSize);
					memset(stringBuffer, 0, elementSize);
					memcpy(stringBuffer, &(complexValues[j].string.getElements(m_pool->stringPool, 0)), complexValues[j].string.size);

					element     = stringBuffer;
				}
				else
					OOSQL_ERR(eNOTIMPLEMENTED_OOSQL); // unhandled case error
				break;
			case TYPEID_VARSTRING:
				if(complexValues[j].valueKind == VALUE_KIND_STRING)
				{
					elementSize = complexValues[j].string.size;
					element     = &(complexValues[j].string.getElements(m_pool->stringPool, 0));
				}
				else
					OOSQL_ERR(eNOTIMPLEMENTED_OOSQL); // unhandled case error
				break;

			case TYPEID_DATE:
				m_storageManager->SetDate(complexValues[j].date.getElements(m_pool->datePool, 0).year,
										  complexValues[j].date.getElements(m_pool->datePool, 0).month, 
										  complexValues[j].date.getElements(m_pool->datePool, 0).day,
										  &dateAttr);
				elementSize = sizeof(OOSQL_StorageManager::Date);
				element     = &dateAttr;
				break;
			case TYPEID_TIME:
				timeAttr._tzHour   = 0;
				timeAttr._tzMinute = 0;
				timeAttr._100thSec = 0;
				timeAttr._Hour     = complexValues[j].time.getElements(m_pool->timePool, 0).hour;
				timeAttr._Minute   = complexValues[j].time.getElements(m_pool->timePool, 0).minute;
				timeAttr._Second   = complexValues[j].time.getElements(m_pool->timePool, 0).second;

				elementSize = sizeof(OOSQL_StorageManager::Time);
				element     = &timeAttr;
				break;
			case TYPEID_TIMESTAMP:
				m_storageManager->SetDate(
							complexValues[j].timestamp.getElements(m_pool->timestampPool, 0).year,
							complexValues[j].timestamp.getElements(m_pool->timestampPool, 0).month, 
							complexValues[j].timestamp.getElements(m_pool->timestampPool, 0).day,
							&timestampAttr.d);
				timestampAttr.t._tzHour   = 0;
				timestampAttr.t._tzMinute = 0;
				timestampAttr.t._100thSec = 0;
				timestampAttr.t._Hour     = complexValues[j].timestamp.getElements(m_pool->timestampPool, 0).hour;
				timestampAttr.t._Minute   = complexValues[j].timestamp.getElements(m_pool->timestampPool, 0).minute;
				timestampAttr.t._Second   = complexValues[j].timestamp.getElements(m_pool->timestampPool, 0).second;

				elementSize = sizeof(OOSQL_StorageManager::Timestamp);
				element     = &timeAttr;
				break;
			case TYPEID_PAGEID:
			case TYPEID_FILEID:
			case TYPEID_INDEXID:
			case TYPEID_OID:
			case TYPEID_MBR:
			case TYPEID_INTERVAL:
				OOSQL_ERR(eNOTIMPLEMENTED_OOSQL);
			}

			switch(OOSQL_MASK_COMPLEXTYPE(exprNode->resultType))
			{
			case COMPLEXTYPEID_SET:
				e = m_storageManager->CollectionSet_IsMember(ocn, SM_FALSE, &oid, colNo, elementSize, element);
				OOSQL_CHECK_ERR(e);

				if(!e)
				{
					e = m_storageManager->CollectionSet_InsertElements(ocn, SM_FALSE, &oid, colNo, 1, &elementSize, element);
					OOSQL_CHECK_ERR(e);
				}
				break;
			case COMPLEXTYPEID_BAG:
				e = m_storageManager->CollectionBag_IsMember(ocn, SM_FALSE, &oid, colNo, elementSize, element);
				OOSQL_CHECK_ERR(e);

				if(!e)
				{
					e = m_storageManager->CollectionBag_InsertElements(ocn, SM_FALSE, &oid, colNo, 1, &elementSize, element);
					OOSQL_CHECK_ERR(e);
				}
				break;
			case COMPLEXTYPEID_LIST:
				e = m_storageManager->CollectionList_IsMember(ocn, SM_FALSE, &oid, colNo, elementSize, element, &pos);
				OOSQL_CHECK_ERR(e);

				if(!e)
				{
					e = m_storageManager->CollectionList_InsertElements(ocn, SM_FALSE, &oid, colNo, 0, 1, &elementSize, element);
					OOSQL_CHECK_ERR(e);
				}
				break;
			}
			if(stringBuffer)
				pMemoryManager->Free(stringBuffer);
			stringBuffer = NULL;
		}
	}
	else
	{
		OOSQL_ERR(eUNHANDLED_CASE_OOSQL)
	}


	return eNOERROR;
}

Four OOSQL_Evaluator::collectionExcept(
    AP_ExprPoolIndex	*op1Idx,	// IN: index to the 1st operand
    AP_ExprPoolIndex	*op2Idx)	// IN: index to the 2nd operand
{
	OOSQL_DB_Value						op1(pMemoryManager), op2(pMemoryManager);	// the evaluation result for each operand
	Four								e;
	AP_ExprElement*						exprNode1;
	AP_ExprElement*						exprNode2;
	AP_ExprElement*						exprNode;
	Four								planNo;
	Four								colNo;
	Four								ocn;
	OOSQL_StorageManager::OID			oid;
	Four								elementSize;
	void*								element;
	ValuePoolElements					value, complexValues(m_pool->valuePool);
	Four								j;
	Two_Invariable						shortAttr;
	Four_Invariable						intAttr;
	Eight_Invariable					eightAttr;
	float								floatAttr;
	double								doubleAttr;
	char*								stringBuffer;
	OOSQL_StorageManager::Date			dateAttr;
	OOSQL_StorageManager::Time			timeAttr;
	OOSQL_StorageManager::Timestamp		timestampAttr;
	Four								pos;

	exprNode1 = ACCESSPLAN.getExprElem(*op1Idx);
	exprNode2 = ACCESSPLAN.getExprElem(*op2Idx);

	if(exprNode1->resultType == TYPEID_COMPLEX && exprNode2->resultType == TYPEID_COMPLEX)
	{
		OOSQL_ERR(eNOTIMPLEMENTED_OOSQL)
	}
	else if(exprNode1->resultType == TYPEID_COMPLEX || exprNode2->resultType == TYPEID_COMPLEX)
	{
		if(exprNode1->resultType == TYPEID_COMPLEX)
		{
			e = evalExpression(op1Idx, &op1);
			if (e < eNOERROR) OOSQL_ERR(e);

			planNo  = ACCESSPLAN.getExprElem(*op1Idx)->pathExpr.col.planNo;
			colNo   = ACCESSPLAN.getExprElem(*op1Idx)->pathExpr.col.colNo;
			ocn		= EVAL_ACCESSLISTTABLE[planNo].getCurrOcn();
			memcpy(&oid, EVAL_EVALBUFFER[planNo].getOID_Ptr(), sizeof(OOSQL_StorageManager::OID));

			exprNode = exprNode2;
		}
		else if(exprNode2->resultType == TYPEID_COMPLEX)
		{
			e = evalExpression(op2Idx, &op2);
			if (e < eNOERROR) OOSQL_ERR(e);

			planNo  = ACCESSPLAN.getExprElem(*op1Idx)->pathExpr.col.planNo;
			colNo   = ACCESSPLAN.getExprElem(*op1Idx)->pathExpr.col.colNo;
			ocn		= EVAL_ACCESSLISTTABLE[planNo].getCurrOcn();
			memcpy(&oid, EVAL_EVALBUFFER[planNo].getOID_Ptr(), sizeof(OOSQL_StorageManager::OID));

			exprNode = exprNode1;
		}

		value         = exprNode2->value.getElements(m_pool->valuePool);
		complexValues = value[0].complex;

		for(j = 0; j < complexValues.size; j++)
		{
			stringBuffer = NULL;
			switch(OOSQL_MASK_TYPE(exprNode->resultType))
			{
			case TYPEID_SHORT:
				if(complexValues[j].valueKind == VALUE_KIND_INTEGER)
				{
					shortAttr   = complexValues[j].integer.getElements(m_pool->intPool, 0);
					elementSize = sizeof(short);
					element     = &shortAttr;
				}
				else if(complexValues[j].valueKind == VALUE_KIND_REAL)
				{
					shortAttr   = complexValues[j].real.getElements(m_pool->realPool, 0);
					elementSize = sizeof(short);
					element     = &shortAttr;
				}
				else
					OOSQL_ERR(eNOTIMPLEMENTED_OOSQL); // unhandled case error
				break;
			case TYPEID_INT:
			case TYPEID_LONG:
				if(complexValues[j].valueKind == VALUE_KIND_INTEGER)
				{
					intAttr   = complexValues[j].integer.getElements(m_pool->intPool, 0);
					elementSize = sizeof(int);
					element     = &intAttr;
				}
				else if(complexValues[j].valueKind == VALUE_KIND_REAL)
				{
					intAttr   = complexValues[j].real.getElements(m_pool->realPool, 0);
					elementSize = sizeof(int);
					element     = &intAttr;
				}
				else
					OOSQL_ERR(eNOTIMPLEMENTED_OOSQL); // unhandled case error
				break;
			case TYPEID_LONG_LONG:
				if(complexValues[j].valueKind == VALUE_KIND_INTEGER)
				{
					eightAttr   = complexValues[j].integer.getElements(m_pool->intPool, 0);
					elementSize = sizeof(Eight_Invariable);
					element     = &eightAttr;
				}
				else if(complexValues[j].valueKind == VALUE_KIND_REAL)
				{
					eightAttr   = complexValues[j].real.getElements(m_pool->realPool, 0);
					elementSize = sizeof(Eight_Invariable);
					element     = &eightAttr;
				}
				else
					OOSQL_ERR(eNOTIMPLEMENTED_OOSQL); // unhandled case error
				break;
			case TYPEID_FLOAT:
				if(complexValues[j].valueKind == VALUE_KIND_INTEGER)
				{
					floatAttr   = complexValues[j].integer.getElements(m_pool->intPool, 0);
					elementSize = sizeof(float);
					element     = &floatAttr;
				}
				else if(complexValues[j].valueKind == VALUE_KIND_REAL)
				{
					floatAttr   = complexValues[j].real.getElements(m_pool->realPool, 0);
					elementSize = sizeof(float);
					element     = &floatAttr;
				}
				else
					OOSQL_ERR(eNOTIMPLEMENTED_OOSQL); // unhandled case error
				break;
			case TYPEID_DOUBLE:
				if(complexValues[j].valueKind == VALUE_KIND_INTEGER)
				{
					doubleAttr  = complexValues[j].integer.getElements(m_pool->intPool, 0);
					elementSize = sizeof(float);
					element     = &doubleAttr;
				}
				else if(complexValues[j].valueKind == VALUE_KIND_REAL)
				{
					doubleAttr  = complexValues[j].real.getElements(m_pool->realPool, 0);
					elementSize = sizeof(float);
					element     = &doubleAttr;
				}
				else
					OOSQL_ERR(eNOTIMPLEMENTED_OOSQL); // unhandled case error
				break;
			case TYPEID_STRING:
				if(complexValues[j].valueKind == VALUE_KIND_STRING)
				{
					elementSize = exprNode->resultLength;

					stringBuffer = (char*)pMemoryManager->Alloc(elementSize);
					memset(stringBuffer, 0, elementSize);
					memcpy(stringBuffer, &(complexValues[j].string.getElements(m_pool->stringPool, 0)), complexValues[j].string.size);

					element     = stringBuffer;
				}
				else
					OOSQL_ERR(eNOTIMPLEMENTED_OOSQL); // unhandled case error
				break;
			case TYPEID_VARSTRING:
				if(complexValues[j].valueKind == VALUE_KIND_STRING)
				{
					elementSize = complexValues[j].string.size;
					element     = &(complexValues[j].string.getElements(m_pool->stringPool, 0));
				}
				else
					OOSQL_ERR(eNOTIMPLEMENTED_OOSQL); // unhandled case error
				break;

			case TYPEID_DATE:
				m_storageManager->SetDate(complexValues[j].date.getElements(m_pool->datePool, 0).year,
										  complexValues[j].date.getElements(m_pool->datePool, 0).month, 
										  complexValues[j].date.getElements(m_pool->datePool, 0).day,
										  &dateAttr);
				elementSize = sizeof(OOSQL_StorageManager::Date);
				element     = &dateAttr;
				break;
			case TYPEID_TIME:
				timeAttr._tzHour   = 0;
				timeAttr._tzMinute = 0;
				timeAttr._100thSec = 0;
				timeAttr._Hour     = complexValues[j].time.getElements(m_pool->timePool, 0).hour;
				timeAttr._Minute   = complexValues[j].time.getElements(m_pool->timePool, 0).minute;
				timeAttr._Second   = complexValues[j].time.getElements(m_pool->timePool, 0).second;

				elementSize = sizeof(OOSQL_StorageManager::Time);
				element     = &timeAttr;
				break;
			case TYPEID_TIMESTAMP:
				m_storageManager->SetDate(
							complexValues[j].timestamp.getElements(m_pool->timestampPool, 0).year,
							complexValues[j].timestamp.getElements(m_pool->timestampPool, 0).month, 
							complexValues[j].timestamp.getElements(m_pool->timestampPool, 0).day,
							&timestampAttr.d);
				timestampAttr.t._tzHour   = 0;
				timestampAttr.t._tzMinute = 0;
				timestampAttr.t._100thSec = 0;
				timestampAttr.t._Hour     = complexValues[j].timestamp.getElements(m_pool->timestampPool, 0).hour;
				timestampAttr.t._Minute   = complexValues[j].timestamp.getElements(m_pool->timestampPool, 0).minute;
				timestampAttr.t._Second   = complexValues[j].timestamp.getElements(m_pool->timestampPool, 0).second;

				elementSize = sizeof(OOSQL_StorageManager::Timestamp);
				element     = &timeAttr;
				break;
			case TYPEID_PAGEID:
			case TYPEID_FILEID:
			case TYPEID_INDEXID:
			case TYPEID_OID:
			case TYPEID_MBR:
			case TYPEID_INTERVAL:
				OOSQL_ERR(eNOTIMPLEMENTED_OOSQL);
			}

			switch(OOSQL_MASK_COMPLEXTYPE(exprNode->resultType))
			{
			case COMPLEXTYPEID_SET:
				e = m_storageManager->CollectionSet_IsMember(ocn, SM_FALSE, &oid, colNo, elementSize, element);
				OOSQL_CHECK_ERR(e);

				if(e)
				{
					e = m_storageManager->CollectionSet_DeleteElements(ocn, SM_FALSE, &oid, colNo, 1, &elementSize, element);
					OOSQL_CHECK_ERR(e);
				}
				break;
			case COMPLEXTYPEID_BAG:
				e = m_storageManager->CollectionBag_IsMember(ocn, SM_FALSE, &oid, colNo, elementSize, element);
				OOSQL_CHECK_ERR(e);

				if(e)
				{
					e = m_storageManager->CollectionBag_DeleteElements(ocn, SM_FALSE, &oid, colNo, 1, &elementSize, element);
					OOSQL_CHECK_ERR(e);
				}
				break;
			case COMPLEXTYPEID_LIST:
				e = m_storageManager->CollectionList_IsMember(ocn, SM_FALSE, &oid, colNo, elementSize, element, &pos);
				OOSQL_CHECK_ERR(e);

				if(e)
				{
					e = m_storageManager->CollectionList_DeleteElements(ocn, SM_FALSE, &oid, colNo, pos, 1);
					OOSQL_CHECK_ERR(e);
				}
				break;
			}
			if(stringBuffer)
				pMemoryManager->Free(stringBuffer);
			stringBuffer = NULL;
		}
	}
	else
	{
		OOSQL_ERR(eUNHANDLED_CASE_OOSQL)
	}


	return eNOERROR;
}

Four OOSQL_Evaluator::collectionCmpIn(
    AP_ExprPoolIndex	*op1Idx,	// IN: index to the 1st operand
    AP_ExprPoolIndex	*op2Idx)	// IN: index to the 2nd operand
{
	OOSQL_DB_Value					op1(pMemoryManager), op2(pMemoryManager);	// the evaluation result for each operand
	Four							e;
	AP_ExprElement*					exprNode1;
	AP_ExprElement*					exprNode2;
	Four							planNo;
	Four							colNo;
	Four							ocn;
	OOSQL_StorageManager::OID		oid;
	Four							length;
	void*							dataPtr;
	Four							pos;

	exprNode1 = ACCESSPLAN.getExprElem(*op1Idx);
	exprNode2 = ACCESSPLAN.getExprElem(*op2Idx);

	if(OOSQL_MASK_COMPLEXTYPE(exprNode1->resultType) == OOSQL_COMPLEXTYPE_BASIC)
	{
		e = evalExpression(op1Idx, &op1);
		if (e < eNOERROR) OOSQL_ERR(e);

		planNo  = ACCESSPLAN.getExprElem(*op2Idx)->pathExpr.col.planNo;
		colNo   = ACCESSPLAN.getExprElem(*op2Idx)->pathExpr.col.colNo;
		ocn		= EVAL_ACCESSLISTTABLE[planNo].getCurrOcn();
		memcpy(&oid, EVAL_EVALBUFFER[planNo].getOID_Ptr(), sizeof(OOSQL_StorageManager::OID));
		
		length = op1.length;
		if(op1.type == OOSQL_TYPE_STRING || op1.type == OOSQL_TYPE_VARSTRING)
			dataPtr = op1.data.ptr;
		else
			dataPtr = &op1.data.s;
		
		switch(OOSQL_MASK_COMPLEXTYPE(exprNode2->resultType))
		{
		case OOSQL_COMPLEXTYPE_SET:
            e = m_storageManager->CollectionSet_IsNull(ocn, SM_FALSE, &oid, colNo);
            if((Boolean)e == SM_TRUE)   return false;

			e = m_storageManager->CollectionSet_IsMember(ocn, SM_FALSE, &oid, colNo, length, dataPtr);
			OOSQL_CHECK_ERR(e);
			if(e)
				return true;
			else
				return false;
			break;
		case OOSQL_COMPLEXTYPE_BAG:
            e = m_storageManager->CollectionBag_IsNull(ocn, SM_FALSE, &oid, colNo);
            if((Boolean)e == SM_TRUE)   return false;

			e = m_storageManager->CollectionBag_IsMember(ocn, SM_FALSE, &oid, colNo, length, dataPtr);
			OOSQL_CHECK_ERR(e);
			if(e)
				return true;
			else
				return false;
			break;
		case OOSQL_COMPLEXTYPE_LIST:
            e = m_storageManager->CollectionList_IsNull(ocn, SM_FALSE, &oid, colNo);
            if((Boolean)e == SM_TRUE)   return false;

			e = m_storageManager->CollectionList_IsMember(ocn, SM_FALSE, &oid, colNo, length, dataPtr, &pos);
			OOSQL_CHECK_ERR(e);
			if(e)
				return true;
			else
				return false;
			break;
		default:
			OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
			break;
		}
	}
	else
	{
		e = evalExpression(op2Idx, &op2);
		if (e < eNOERROR) OOSQL_ERR(e);

		planNo  = ACCESSPLAN.getExprElem(*op1Idx)->pathExpr.col.planNo;
		colNo   = ACCESSPLAN.getExprElem(*op1Idx)->pathExpr.col.colNo;
		ocn		= EVAL_ACCESSLISTTABLE[planNo].getCurrOcn();
		memcpy(&oid, EVAL_EVALBUFFER[planNo].getOID_Ptr(), sizeof(OOSQL_StorageManager::OID));
		
		length = op2.length;
		if(op2.type == OOSQL_TYPE_STRING || op2.type == OOSQL_TYPE_VARSTRING)
			dataPtr = op2.data.ptr;
		else
			dataPtr = &op2.data.s;
		
		switch(OOSQL_MASK_COMPLEXTYPE(exprNode2->resultType))
		{
		case OOSQL_COMPLEXTYPE_SET:
            e = m_storageManager->CollectionSet_IsNull(ocn, SM_FALSE, &oid, colNo);
            if((Boolean)e == SM_TRUE)   return false;

			e = m_storageManager->CollectionSet_IsMember(ocn, SM_FALSE, &oid, colNo, length, dataPtr);
			OOSQL_CHECK_ERR(e);
			if(e)
				return true;
			else
				return false;
			break;
		case OOSQL_COMPLEXTYPE_BAG:
            e = m_storageManager->CollectionBag_IsNull(ocn, SM_FALSE, &oid, colNo);
            if((Boolean)e == SM_TRUE)   return false;

			e = m_storageManager->CollectionBag_IsMember(ocn, SM_FALSE, &oid, colNo, length, dataPtr);
			OOSQL_CHECK_ERR(e);
			if(e)
				return true;
			else
				return false;
			break;
		case OOSQL_COMPLEXTYPE_LIST:
            e = m_storageManager->CollectionList_IsNull(ocn, SM_FALSE, &oid, colNo);
            if((Boolean)e == SM_TRUE)   return false;

			e = m_storageManager->CollectionList_IsMember(ocn, SM_FALSE, &oid, colNo, length, dataPtr, &pos);
			OOSQL_CHECK_ERR(e);
			if(e)
				return true;
			else
				return false;
			break;
		default:
			OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
			break;
		}
	}

	return false;
}

Four OOSQL_Evaluator::geoSpatialCmpNorth(
    AP_ExprPoolIndex	*op1Idx,	// IN: index to the 1st operand
    AP_ExprPoolIndex	*op2Idx)	// IN: index to the 2nd operand
{
    OOSQL_DB_Value					op1(pMemoryManager), op2(pMemoryManager);	// the evaluation result for each operand
    Four							e;
	Four                            ocn1, ocn2;
    Four							planNo1, planNo2;
	OOSQL_StorageManager::Region    region;
	OOSQL_StorageManager::Point		regionPoints1[4], regionPoints2[4];
    
    e = evalExpression(op1Idx, &op1);
    if (e < eNOERROR) OOSQL_ERR(e);

    e = evalExpression(op2Idx, &op2);
    if (e < eNOERROR) OOSQL_ERR(e);

    if(op1.type == OOSQL_TYPE_OID && op2.type == OOSQL_TYPE_OID)
    {
        planNo1 = ACCESSPLAN.getExprElem(*op1Idx)->pathExpr.planNo;
        planNo2 = ACCESSPLAN.getExprElem(*op2Idx)->pathExpr.planNo;
		ocn1 = EVAL_ACCESSLISTTABLE[planNo1].getCurrOcn();
        ocn2 = EVAL_ACCESSLISTTABLE[planNo2].getCurrOcn();

        return m_storageManager->PDC_North(ocn1, SM_FALSE, &(op1.data.oid), ocn2, SM_FALSE, &(op2.data.oid));
    }
    else if(op1.type == OOSQL_TYPE_OID && op2.type == OOSQL_TYPE_MBR)
    {
        planNo1 = ACCESSPLAN.getExprElem(*op1Idx)->pathExpr.planNo;
        ocn1 = EVAL_ACCESSLISTTABLE[planNo1].getCurrOcn();

        region.x1 = op2.data.mbr.values[0];
        region.y1 = op2.data.mbr.values[1];
        region.x2 = op2.data.mbr.values[2];
        region.y2 = op2.data.mbr.values[3];
        
        return m_storageManager->PDC_NorthOfRegion(ocn1, SM_FALSE, &(op1.data.oid), region);
    }
    else if(op1.type == OOSQL_TYPE_MBR && op2.type == OOSQL_TYPE_OID)
    {
        planNo2 = ACCESSPLAN.getExprElem(*op2Idx)->pathExpr.planNo;
        ocn2 = EVAL_ACCESSLISTTABLE[planNo2].getCurrOcn();

        region.x1 = op1.data.mbr.values[0];
        region.y1 = op1.data.mbr.values[1];
        region.x2 = op1.data.mbr.values[2];
        region.y2 = op1.data.mbr.values[3];
        
        return m_storageManager->PDC_SouthOfRegion(ocn2, SM_FALSE, &(op2.data.oid), region);
    }
    else if(op1.type == OOSQL_TYPE_MBR && op2.type == OOSQL_TYPE_MBR)
    {
		regionPoints1[0].x = op1.data.mbr.values[0];
		regionPoints1[0].y = op1.data.mbr.values[1];
		regionPoints1[1].x = op1.data.mbr.values[2];
		regionPoints1[1].y = op1.data.mbr.values[1];
		regionPoints1[2].x = op1.data.mbr.values[2];
		regionPoints1[2].y = op1.data.mbr.values[3];
		regionPoints1[3].x = op1.data.mbr.values[0];
		regionPoints1[3].y = op1.data.mbr.values[3];

		regionPoints2[0].x = op2.data.mbr.values[0];
		regionPoints2[0].y = op2.data.mbr.values[1];
		regionPoints2[1].x = op2.data.mbr.values[2];
		regionPoints2[1].y = op2.data.mbr.values[1];
		regionPoints2[2].x = op2.data.mbr.values[2];
		regionPoints2[2].y = op2.data.mbr.values[3];
		regionPoints2[3].x = op2.data.mbr.values[0];
		regionPoints2[3].y = op2.data.mbr.values[3];

        return m_storageManager->PDC_North(OOSQL_StorageManager::POLYGON_CLASSTYPE, 4, regionPoints1,
			                               OOSQL_StorageManager::POLYGON_CLASSTYPE, 4, regionPoints2);
    }

    return SM_FALSE;
}

Four OOSQL_Evaluator::geoSpatialCmpSouth(
    AP_ExprPoolIndex	*op1Idx,	// IN: index to the 1st operand
    AP_ExprPoolIndex	*op2Idx)	// IN: index to the 2nd operand
{
    OOSQL_DB_Value					op1(pMemoryManager), op2(pMemoryManager);	// the evaluation result for each operand
    Four							e;
    Four                            ocn1, ocn2;
    Four							planNo1, planNo2;
	OOSQL_StorageManager::Region    region;
	OOSQL_StorageManager::Point		regionPoints1[4], regionPoints2[4];
    
    e = evalExpression(op1Idx, &op1);
    if (e < eNOERROR) OOSQL_ERR(e);

    e = evalExpression(op2Idx, &op2);
    if (e < eNOERROR) OOSQL_ERR(e);

    if(op1.type == OOSQL_TYPE_OID && op2.type == OOSQL_TYPE_OID)
    {
        planNo1 = ACCESSPLAN.getExprElem(*op1Idx)->pathExpr.planNo;
        planNo2 = ACCESSPLAN.getExprElem(*op2Idx)->pathExpr.planNo;
        ocn1 = EVAL_ACCESSLISTTABLE[planNo1].getCurrOcn();
        ocn2 = EVAL_ACCESSLISTTABLE[planNo2].getCurrOcn();

        return m_storageManager->PDC_South(ocn1, SM_FALSE, &(op1.data.oid), ocn2, SM_FALSE, &(op2.data.oid));
    }
    else if(op1.type == OOSQL_TYPE_OID && op2.type == OOSQL_TYPE_MBR)
    {
        planNo1 = ACCESSPLAN.getExprElem(*op1Idx)->pathExpr.planNo;
        ocn1 = EVAL_ACCESSLISTTABLE[planNo1].getCurrOcn();

        region.x1 = op2.data.mbr.values[0];
        region.y1 = op2.data.mbr.values[1];
        region.x2 = op2.data.mbr.values[2];
        region.y2 = op2.data.mbr.values[3];
        
        return m_storageManager->PDC_SouthOfRegion(ocn1, SM_FALSE, &(op1.data.oid), region);
    }
    else if(op1.type == OOSQL_TYPE_MBR && op2.type == OOSQL_TYPE_OID)
    {
        planNo2 = ACCESSPLAN.getExprElem(*op2Idx)->pathExpr.planNo;
        ocn2 = EVAL_ACCESSLISTTABLE[planNo2].getCurrOcn();

        region.x1 = op1.data.mbr.values[0];
        region.y1 = op1.data.mbr.values[1];
        region.x2 = op1.data.mbr.values[2];
        region.y2 = op1.data.mbr.values[3];
        
        return m_storageManager->PDC_NorthOfRegion(ocn2, SM_FALSE, &(op2.data.oid), region);
    }
    else if(op1.type == OOSQL_TYPE_MBR && op2.type == OOSQL_TYPE_MBR)
    {
		regionPoints1[0].x = op1.data.mbr.values[0];
		regionPoints1[0].y = op1.data.mbr.values[1];
		regionPoints1[1].x = op1.data.mbr.values[2];
		regionPoints1[1].y = op1.data.mbr.values[1];
		regionPoints1[2].x = op1.data.mbr.values[2];
		regionPoints1[2].y = op1.data.mbr.values[3];
		regionPoints1[3].x = op1.data.mbr.values[0];
		regionPoints1[3].y = op1.data.mbr.values[3];

		regionPoints2[0].x = op2.data.mbr.values[0];
		regionPoints2[0].y = op2.data.mbr.values[1];
		regionPoints2[1].x = op2.data.mbr.values[2];
		regionPoints2[1].y = op2.data.mbr.values[1];
		regionPoints2[2].x = op2.data.mbr.values[2];
		regionPoints2[2].y = op2.data.mbr.values[3];
		regionPoints2[3].x = op2.data.mbr.values[0];
		regionPoints2[3].y = op2.data.mbr.values[3];

        return m_storageManager->PDC_South(OOSQL_StorageManager::POLYGON_CLASSTYPE, 4, regionPoints1,
			                               OOSQL_StorageManager::POLYGON_CLASSTYPE, 4, regionPoints2);
    }

    return SM_FALSE;
}

Four OOSQL_Evaluator::geoSpatialCmpEast( 
    AP_ExprPoolIndex	*op1Idx,	// IN: index to the 1st operand
    AP_ExprPoolIndex	*op2Idx)	// IN: index to the 2nd operand{
{
    OOSQL_DB_Value					op1(pMemoryManager), op2(pMemoryManager);	// the evaluation result for each operand
    Four							e;
    Four                            ocn1, ocn2;
    Four							planNo1, planNo2;
	OOSQL_StorageManager::Region    region;
	OOSQL_StorageManager::Point		regionPoints1[4], regionPoints2[4];
    
    e = evalExpression(op1Idx, &op1);
    if (e < eNOERROR) OOSQL_ERR(e);

    e = evalExpression(op2Idx, &op2);
    if (e < eNOERROR) OOSQL_ERR(e);

    if(op1.type == OOSQL_TYPE_OID && op2.type == OOSQL_TYPE_OID)
    {
        planNo1 = ACCESSPLAN.getExprElem(*op1Idx)->pathExpr.planNo;
        planNo2 = ACCESSPLAN.getExprElem(*op2Idx)->pathExpr.planNo;
        ocn1 = EVAL_ACCESSLISTTABLE[planNo1].getCurrOcn();
        ocn2 = EVAL_ACCESSLISTTABLE[planNo2].getCurrOcn();

        return m_storageManager->PDC_East(ocn1, SM_FALSE, &(op1.data.oid), ocn2, SM_FALSE, &(op2.data.oid));
    }
    else if(op1.type == OOSQL_TYPE_OID && op2.type == OOSQL_TYPE_MBR)
    {
        planNo1 = ACCESSPLAN.getExprElem(*op1Idx)->pathExpr.planNo;
        ocn1 = EVAL_ACCESSLISTTABLE[planNo1].getCurrOcn();

        region.x1 = op2.data.mbr.values[0];
        region.y1 = op2.data.mbr.values[1];
        region.x2 = op2.data.mbr.values[2];
        region.y2 = op2.data.mbr.values[3];
        
        return m_storageManager->PDC_EastOfRegion(ocn1, SM_FALSE, &(op1.data.oid), region);
    }
    else if(op1.type == OOSQL_TYPE_MBR && op2.type == OOSQL_TYPE_OID)
    {
        planNo2 = ACCESSPLAN.getExprElem(*op2Idx)->pathExpr.planNo;
        ocn2 = EVAL_ACCESSLISTTABLE[planNo2].getCurrOcn();

        region.x1 = op1.data.mbr.values[0];
        region.y1 = op1.data.mbr.values[1];
        region.x2 = op1.data.mbr.values[2];
        region.y2 = op1.data.mbr.values[3];
        
        return m_storageManager->PDC_WestOfRegion(ocn2, SM_FALSE, &(op2.data.oid), region);
    }
    else if(op1.type == OOSQL_TYPE_MBR && op2.type == OOSQL_TYPE_MBR)
    {
		regionPoints1[0].x = op1.data.mbr.values[0];
		regionPoints1[0].y = op1.data.mbr.values[1];
		regionPoints1[1].x = op1.data.mbr.values[2];
		regionPoints1[1].y = op1.data.mbr.values[1];
		regionPoints1[2].x = op1.data.mbr.values[2];
		regionPoints1[2].y = op1.data.mbr.values[3];
		regionPoints1[3].x = op1.data.mbr.values[0];
		regionPoints1[3].y = op1.data.mbr.values[3];

		regionPoints2[0].x = op2.data.mbr.values[0];
		regionPoints2[0].y = op2.data.mbr.values[1];
		regionPoints2[1].x = op2.data.mbr.values[2];
		regionPoints2[1].y = op2.data.mbr.values[1];
		regionPoints2[2].x = op2.data.mbr.values[2];
		regionPoints2[2].y = op2.data.mbr.values[3];
		regionPoints2[3].x = op2.data.mbr.values[0];
		regionPoints2[3].y = op2.data.mbr.values[3];

        return m_storageManager->PDC_East(OOSQL_StorageManager::POLYGON_CLASSTYPE, 4, regionPoints1,
			                              OOSQL_StorageManager::POLYGON_CLASSTYPE, 4, regionPoints2);
    }

    return SM_FALSE;
}

Four OOSQL_Evaluator::geoSpatialCmpWest( 
    AP_ExprPoolIndex	*op1Idx,	// IN: index to the 1st operand
    AP_ExprPoolIndex	*op2Idx)	// IN: index to the 2nd operand
{
    OOSQL_DB_Value					op1(pMemoryManager), op2(pMemoryManager);	// the evaluation result for each operand
    Four							e;
    Four                            ocn1, ocn2;
    Four							planNo1, planNo2;
	OOSQL_StorageManager::Region    region;
	OOSQL_StorageManager::Point		regionPoints1[4], regionPoints2[4];
    
    e = evalExpression(op1Idx, &op1);
    if (e < eNOERROR) OOSQL_ERR(e);

    e = evalExpression(op2Idx, &op2);
    if (e < eNOERROR) OOSQL_ERR(e);

    if(op1.type == OOSQL_TYPE_OID && op2.type == OOSQL_TYPE_OID)
    {
        planNo1 = ACCESSPLAN.getExprElem(*op1Idx)->pathExpr.planNo;
        planNo2 = ACCESSPLAN.getExprElem(*op2Idx)->pathExpr.planNo;
        ocn1 = EVAL_ACCESSLISTTABLE[planNo1].getCurrOcn();
        ocn2 = EVAL_ACCESSLISTTABLE[planNo2].getCurrOcn();

        return m_storageManager->PDC_West(ocn1, SM_FALSE, &(op1.data.oid), ocn2, SM_FALSE, &(op2.data.oid));
    }
    else if(op1.type == OOSQL_TYPE_OID && op2.type == OOSQL_TYPE_MBR)
    {
        planNo1 = ACCESSPLAN.getExprElem(*op1Idx)->pathExpr.planNo;
        ocn1 = EVAL_ACCESSLISTTABLE[planNo1].getCurrOcn();

        region.x1 = op2.data.mbr.values[0];
        region.y1 = op2.data.mbr.values[1];
        region.x2 = op2.data.mbr.values[2];
        region.y2 = op2.data.mbr.values[3];
        
        return m_storageManager->PDC_WestOfRegion(ocn1, SM_FALSE, &(op1.data.oid), region);
    }
    else if(op1.type == OOSQL_TYPE_MBR && op2.type == OOSQL_TYPE_OID)
    {
        planNo2 = ACCESSPLAN.getExprElem(*op2Idx)->pathExpr.planNo;
        ocn2 = EVAL_ACCESSLISTTABLE[planNo2].getCurrOcn();

        region.x1 = op1.data.mbr.values[0];
        region.y1 = op1.data.mbr.values[1];
        region.x2 = op1.data.mbr.values[2];
        region.y2 = op1.data.mbr.values[3];
        
        return m_storageManager->PDC_EastOfRegion(ocn2, SM_FALSE, &(op2.data.oid), region);
    }
    else if(op1.type == OOSQL_TYPE_MBR && op2.type == OOSQL_TYPE_MBR)
    {
		regionPoints1[0].x = op1.data.mbr.values[0];
		regionPoints1[0].y = op1.data.mbr.values[1];
		regionPoints1[1].x = op1.data.mbr.values[2];
		regionPoints1[1].y = op1.data.mbr.values[1];
		regionPoints1[2].x = op1.data.mbr.values[2];
		regionPoints1[2].y = op1.data.mbr.values[3];
		regionPoints1[3].x = op1.data.mbr.values[0];
		regionPoints1[3].y = op1.data.mbr.values[3];

		regionPoints2[0].x = op2.data.mbr.values[0];
		regionPoints2[0].y = op2.data.mbr.values[1];
		regionPoints2[1].x = op2.data.mbr.values[2];
		regionPoints2[1].y = op2.data.mbr.values[1];
		regionPoints2[2].x = op2.data.mbr.values[2];
		regionPoints2[2].y = op2.data.mbr.values[3];
		regionPoints2[3].x = op2.data.mbr.values[0];
		regionPoints2[3].y = op2.data.mbr.values[3];

        return m_storageManager->PDC_West(OOSQL_StorageManager::POLYGON_CLASSTYPE, 4, regionPoints1,
			                              OOSQL_StorageManager::POLYGON_CLASSTYPE, 4, regionPoints2);
    }

    return SM_FALSE;
}

Four OOSQL_Evaluator::geoSpatialCmpContain(
    AP_ExprPoolIndex	*op1Idx,	// IN: index to the 1st operand
    AP_ExprPoolIndex	*op2Idx)	// IN: index to the 2nd operand
{
    OOSQL_DB_Value					op1(pMemoryManager), op2(pMemoryManager);	// the evaluation result for each operand
    Four							e;
    Four							ocn1, ocn2;
    Four							planNo1, planNo2;
	OOSQL_StorageManager::Region    region;
	OOSQL_StorageManager::Point		regionPoints1[4], regionPoints2[4];
    
    e = evalExpression(op1Idx, &op1);
    if (e < eNOERROR) OOSQL_ERR(e);

    e = evalExpression(op2Idx, &op2);
    if (e < eNOERROR) OOSQL_ERR(e);

    if(op1.type == OOSQL_TYPE_OID && op2.type == OOSQL_TYPE_OID)
    {
        planNo1 = ACCESSPLAN.getExprElem(*op1Idx)->pathExpr.planNo;
        planNo2 = ACCESSPLAN.getExprElem(*op2Idx)->pathExpr.planNo;
        ocn1 = EVAL_ACCESSLISTTABLE[planNo1].getCurrOcn();
        ocn2 = EVAL_ACCESSLISTTABLE[planNo2].getCurrOcn();

        return m_storageManager->PDC_Contain(ocn1, SM_FALSE, &(op1.data.oid), ocn2, SM_FALSE, &(op2.data.oid));
    }
    else if(op1.type == OOSQL_TYPE_OID && op2.type == OOSQL_TYPE_MBR)
    {
        planNo1 = ACCESSPLAN.getExprElem(*op1Idx)->pathExpr.planNo;
        ocn1 = EVAL_ACCESSLISTTABLE[planNo1].getCurrOcn();

        region.x1 = op2.data.mbr.values[0];
        region.y1 = op2.data.mbr.values[1];
        region.x2 = op2.data.mbr.values[2];
        region.y2 = op2.data.mbr.values[3];
        
        return m_storageManager->PDC_ContainRegion(ocn1, SM_FALSE, &(op1.data.oid), region);
    }
    else if(op1.type == OOSQL_TYPE_MBR && op2.type == OOSQL_TYPE_OID)
    {
        planNo2 = ACCESSPLAN.getExprElem(*op2Idx)->pathExpr.planNo;
        ocn2 = EVAL_ACCESSLISTTABLE[planNo2].getCurrOcn();

        region.x1 = op1.data.mbr.values[0];
        region.y1 = op1.data.mbr.values[1];
        region.x2 = op1.data.mbr.values[2];
        region.y2 = op1.data.mbr.values[3];
        
        return m_storageManager->PDC_ContainedInRegion(ocn2, SM_FALSE, &(op2.data.oid), region);
    }
    else if(op1.type == OOSQL_TYPE_MBR && op2.type == OOSQL_TYPE_MBR)
    {
		regionPoints1[0].x = op1.data.mbr.values[0];
		regionPoints1[0].y = op1.data.mbr.values[1];
		regionPoints1[1].x = op1.data.mbr.values[2];
		regionPoints1[1].y = op1.data.mbr.values[1];
		regionPoints1[2].x = op1.data.mbr.values[2];
		regionPoints1[2].y = op1.data.mbr.values[3];
		regionPoints1[3].x = op1.data.mbr.values[0];
		regionPoints1[3].y = op1.data.mbr.values[3];

		regionPoints2[0].x = op2.data.mbr.values[0];
		regionPoints2[0].y = op2.data.mbr.values[1];
		regionPoints2[1].x = op2.data.mbr.values[2];
		regionPoints2[1].y = op2.data.mbr.values[1];
		regionPoints2[2].x = op2.data.mbr.values[2];
		regionPoints2[2].y = op2.data.mbr.values[3];
		regionPoints2[3].x = op2.data.mbr.values[0];
		regionPoints2[3].y = op2.data.mbr.values[3];

        return m_storageManager->PDC_Contain(OOSQL_StorageManager::POLYGON_CLASSTYPE, 4, regionPoints1,
			                                 OOSQL_StorageManager::POLYGON_CLASSTYPE, 4, regionPoints2);
    }

    return SM_FALSE;
}

Four OOSQL_Evaluator::geoSpatialCmpCover( 
    AP_ExprPoolIndex	*op1Idx,	// IN: index to the 1st operand
    AP_ExprPoolIndex	*op2Idx)	// IN: index to the 2nd operand
{
    OOSQL_DB_Value					op1(pMemoryManager), op2(pMemoryManager);	// the evaluation result for each operand
    Four							e;
    Four							ocn1, ocn2;
    Four							planNo1, planNo2;
    OOSQL_StorageManager::Region    region;
	OOSQL_StorageManager::Point		regionPoints1[4], regionPoints2[4];
    
    e = evalExpression(op1Idx, &op1);
    if (e < eNOERROR) OOSQL_ERR(e);

    e = evalExpression(op2Idx, &op2);
    if (e < eNOERROR) OOSQL_ERR(e);

    if(op1.type == OOSQL_TYPE_OID && op2.type == OOSQL_TYPE_OID)
    {
        planNo1 = ACCESSPLAN.getExprElem(*op1Idx)->pathExpr.planNo;
        planNo2 = ACCESSPLAN.getExprElem(*op2Idx)->pathExpr.planNo;
        ocn1 = EVAL_ACCESSLISTTABLE[planNo1].getCurrOcn();
        ocn2 = EVAL_ACCESSLISTTABLE[planNo2].getCurrOcn();

        return m_storageManager->PDC_Cover(ocn1, SM_FALSE, &(op1.data.oid), ocn2, SM_FALSE, &(op2.data.oid));
    }
    else if(op1.type == OOSQL_TYPE_OID && op2.type == OOSQL_TYPE_MBR)
    {
        planNo1 = ACCESSPLAN.getExprElem(*op1Idx)->pathExpr.planNo;
        ocn1 = EVAL_ACCESSLISTTABLE[planNo1].getCurrOcn();

        region.x1 = op2.data.mbr.values[0];
        region.y1 = op2.data.mbr.values[1];
        region.x2 = op2.data.mbr.values[2];
        region.y2 = op2.data.mbr.values[3];
        
        return m_storageManager->PDC_ContainRegion(ocn1, SM_FALSE, &(op1.data.oid), region);
    }
    else if(op1.type == OOSQL_TYPE_MBR && op2.type == OOSQL_TYPE_OID)
    {
        planNo2 = ACCESSPLAN.getExprElem(*op2Idx)->pathExpr.planNo;
        ocn2 = EVAL_ACCESSLISTTABLE[planNo2].getCurrOcn();

        region.x1 = op1.data.mbr.values[0];
        region.y1 = op1.data.mbr.values[1];
        region.x2 = op1.data.mbr.values[2];
        region.y2 = op1.data.mbr.values[3];
        
        return m_storageManager->PDC_ContainedInRegion(ocn2, SM_FALSE, &(op2.data.oid), region);
    }
    else if(op1.type == OOSQL_TYPE_MBR && op2.type == OOSQL_TYPE_MBR)
    {
        regionPoints1[0].x = op1.data.mbr.values[0];
		regionPoints1[0].y = op1.data.mbr.values[1];
		regionPoints1[1].x = op1.data.mbr.values[2];
		regionPoints1[1].y = op1.data.mbr.values[1];
		regionPoints1[2].x = op1.data.mbr.values[2];
		regionPoints1[2].y = op1.data.mbr.values[3];
		regionPoints1[3].x = op1.data.mbr.values[0];
		regionPoints1[3].y = op1.data.mbr.values[3];

		regionPoints2[0].x = op2.data.mbr.values[0];
		regionPoints2[0].y = op2.data.mbr.values[1];
		regionPoints2[1].x = op2.data.mbr.values[2];
		regionPoints2[1].y = op2.data.mbr.values[1];
		regionPoints2[2].x = op2.data.mbr.values[2];
		regionPoints2[2].y = op2.data.mbr.values[3];
		regionPoints2[3].x = op2.data.mbr.values[0];
		regionPoints2[3].y = op2.data.mbr.values[3];

        return m_storageManager->PDC_Cover(OOSQL_StorageManager::POLYGON_CLASSTYPE, 4, regionPoints1,
			                               OOSQL_StorageManager::POLYGON_CLASSTYPE, 4, regionPoints2);
    }

    return SM_FALSE;
}

Four OOSQL_Evaluator::geoSpatialCmpDisjoint(
    AP_ExprPoolIndex	*op1Idx,	// IN: index to the 1st operand
    AP_ExprPoolIndex	*op2Idx)	// IN: index to the 2nd operand
{
    OOSQL_DB_Value					op1(pMemoryManager), op2(pMemoryManager);	// the evaluation result for each operand
    Four							e;
    Four							ocn1, ocn2;
    Four							planNo1, planNo2;
    OOSQL_StorageManager::Region    region;
	OOSQL_StorageManager::Point		regionPoints1[4], regionPoints2[4];

    e = evalExpression(op1Idx, &op1);
    if (e < eNOERROR) OOSQL_ERR(e);

    e = evalExpression(op2Idx, &op2);
    if (e < eNOERROR) OOSQL_ERR(e);

    if(op1.type == OOSQL_TYPE_OID && op2.type == OOSQL_TYPE_OID)
    {
        planNo1 = ACCESSPLAN.getExprElem(*op1Idx)->pathExpr.planNo;
        planNo2 = ACCESSPLAN.getExprElem(*op2Idx)->pathExpr.planNo;
        ocn1 = EVAL_ACCESSLISTTABLE[planNo1].getCurrOcn();
        ocn2 = EVAL_ACCESSLISTTABLE[planNo2].getCurrOcn();

        return m_storageManager->PDC_Disjoint(ocn1, SM_FALSE, &(op1.data.oid), ocn2, SM_FALSE, &(op2.data.oid));
    }
    else if(op1.type == OOSQL_TYPE_OID && op2.type == OOSQL_TYPE_MBR)
    {
        planNo1 = ACCESSPLAN.getExprElem(*op1Idx)->pathExpr.planNo;
        ocn1 = EVAL_ACCESSLISTTABLE[planNo1].getCurrOcn();

        region.x1 = op2.data.mbr.values[0];
        region.y1 = op2.data.mbr.values[1];
        region.x2 = op2.data.mbr.values[2];
        region.y2 = op2.data.mbr.values[3];
        
        return m_storageManager->PDC_DisjointWithRegion(ocn1, SM_FALSE, &(op1.data.oid), region);
    }
    else if(op1.type == OOSQL_TYPE_MBR && op2.type == OOSQL_TYPE_OID)
    {
        planNo2 = ACCESSPLAN.getExprElem(*op2Idx)->pathExpr.planNo;
        ocn2 = EVAL_ACCESSLISTTABLE[planNo2].getCurrOcn();

        region.x1 = op1.data.mbr.values[0];
        region.y1 = op1.data.mbr.values[1];
        region.x2 = op1.data.mbr.values[2];
        region.y2 = op1.data.mbr.values[3];
        
        return m_storageManager->PDC_DisjointWithRegion(ocn2, SM_FALSE, &(op2.data.oid), region);
    }
    else if(op1.type == OOSQL_TYPE_MBR && op2.type == OOSQL_TYPE_MBR)
    {
        regionPoints1[0].x = op1.data.mbr.values[0];
		regionPoints1[0].y = op1.data.mbr.values[1];
		regionPoints1[1].x = op1.data.mbr.values[2];
		regionPoints1[1].y = op1.data.mbr.values[1];
		regionPoints1[2].x = op1.data.mbr.values[2];
		regionPoints1[2].y = op1.data.mbr.values[3];
		regionPoints1[3].x = op1.data.mbr.values[0];
		regionPoints1[3].y = op1.data.mbr.values[3];

		regionPoints2[0].x = op2.data.mbr.values[0];
		regionPoints2[0].y = op2.data.mbr.values[1];
		regionPoints2[1].x = op2.data.mbr.values[2];
		regionPoints2[1].y = op2.data.mbr.values[1];
		regionPoints2[2].x = op2.data.mbr.values[2];
		regionPoints2[2].y = op2.data.mbr.values[3];
		regionPoints2[3].x = op2.data.mbr.values[0];
		regionPoints2[3].y = op2.data.mbr.values[3];

        return m_storageManager->PDC_Disjoint(OOSQL_StorageManager::POLYGON_CLASSTYPE, 4, regionPoints1,
			                                  OOSQL_StorageManager::POLYGON_CLASSTYPE, 4, regionPoints2);
    }

    return SM_FALSE;
}

Four OOSQL_Evaluator::geoSpatialCmpEqual(
    AP_ExprPoolIndex	*op1Idx,	// IN: index to the 1st operand
    AP_ExprPoolIndex	*op2Idx)	// IN: index to the 2nd operand
{
    OOSQL_DB_Value					op1(pMemoryManager), op2(pMemoryManager);	// the evaluation result for each operand
    Four							e;
    Four							ocn1, ocn2;
    Four							planNo1, planNo2;
	OOSQL_StorageManager::Region    region;
	OOSQL_StorageManager::Point		regionPoints1[4], regionPoints2[4];

    e = evalExpression(op1Idx, &op1);
    if (e < eNOERROR) OOSQL_ERR(e);

    e = evalExpression(op2Idx, &op2);
    if (e < eNOERROR) OOSQL_ERR(e);

    if(op1.type == OOSQL_TYPE_OID && op2.type == OOSQL_TYPE_OID)
    {
        planNo1 = ACCESSPLAN.getExprElem(*op1Idx)->pathExpr.planNo;
        planNo2 = ACCESSPLAN.getExprElem(*op2Idx)->pathExpr.planNo;
        ocn1 = EVAL_ACCESSLISTTABLE[planNo1].getCurrOcn();
        ocn2 = EVAL_ACCESSLISTTABLE[planNo2].getCurrOcn();

        return m_storageManager->PDC_Equal(ocn1, SM_FALSE, &(op1.data.oid), ocn2, SM_FALSE, &(op2.data.oid));
    }
    else if(op1.type == OOSQL_TYPE_OID && op2.type == OOSQL_TYPE_MBR)
    {
        planNo1 = ACCESSPLAN.getExprElem(*op1Idx)->pathExpr.planNo;
        ocn1 = EVAL_ACCESSLISTTABLE[planNo1].getCurrOcn();

        region.x1 = op2.data.mbr.values[0];
        region.y1 = op2.data.mbr.values[1];
        region.x2 = op2.data.mbr.values[2];
        region.y2 = op2.data.mbr.values[3];
        
        return m_storageManager->PDC_EqualWithRegion(ocn1, SM_FALSE, &(op1.data.oid), region);
    }
    else if(op1.type == OOSQL_TYPE_MBR && op2.type == OOSQL_TYPE_OID)
    {
        planNo2 = ACCESSPLAN.getExprElem(*op2Idx)->pathExpr.planNo;
        ocn2 = EVAL_ACCESSLISTTABLE[planNo2].getCurrOcn();

        region.x1 = op1.data.mbr.values[0];
        region.y1 = op1.data.mbr.values[1];
        region.x2 = op1.data.mbr.values[2];
        region.y2 = op1.data.mbr.values[3];
        
        return m_storageManager->PDC_EqualWithRegion(ocn2, SM_FALSE, &(op2.data.oid), region);
    }
    else if(op1.type == OOSQL_TYPE_MBR && op2.type == OOSQL_TYPE_MBR)
    {
        regionPoints1[0].x = op1.data.mbr.values[0];
		regionPoints1[0].y = op1.data.mbr.values[1];
		regionPoints1[1].x = op1.data.mbr.values[2];
		regionPoints1[1].y = op1.data.mbr.values[1];
		regionPoints1[2].x = op1.data.mbr.values[2];
		regionPoints1[2].y = op1.data.mbr.values[3];
		regionPoints1[3].x = op1.data.mbr.values[0];
		regionPoints1[3].y = op1.data.mbr.values[3];

		regionPoints2[0].x = op2.data.mbr.values[0];
		regionPoints2[0].y = op2.data.mbr.values[1];
		regionPoints2[1].x = op2.data.mbr.values[2];
		regionPoints2[1].y = op2.data.mbr.values[1];
		regionPoints2[2].x = op2.data.mbr.values[2];
		regionPoints2[2].y = op2.data.mbr.values[3];
		regionPoints2[3].x = op2.data.mbr.values[0];
		regionPoints2[3].y = op2.data.mbr.values[3];

        return m_storageManager->PDC_Equal(OOSQL_StorageManager::POLYGON_CLASSTYPE, 4, regionPoints1,
			                               OOSQL_StorageManager::POLYGON_CLASSTYPE, 4, regionPoints2);
    }
    return SM_FALSE;
}

Four OOSQL_Evaluator::geoSpatialCmpMeet( 
    AP_ExprPoolIndex	*op1Idx,	// IN: index to the 1st operand
    AP_ExprPoolIndex	*op2Idx)	// IN: index to the 2nd operand
{
    OOSQL_DB_Value					op1(pMemoryManager), op2(pMemoryManager);	// the evaluation result for each operand
    Four							e;
    Four							ocn1, ocn2;
    Four							planNo1, planNo2;
	OOSQL_StorageManager::Region    region;
    OOSQL_StorageManager::Point		regionPoints1[4], regionPoints2[4];

    e = evalExpression(op1Idx, &op1);
    if (e < eNOERROR) OOSQL_ERR(e);

    e = evalExpression(op2Idx, &op2);
    if (e < eNOERROR) OOSQL_ERR(e);

    if(op1.type == OOSQL_TYPE_OID && op2.type == OOSQL_TYPE_OID)
    {
        planNo1 = ACCESSPLAN.getExprElem(*op1Idx)->pathExpr.planNo;
        planNo2 = ACCESSPLAN.getExprElem(*op2Idx)->pathExpr.planNo;
        ocn1 = EVAL_ACCESSLISTTABLE[planNo1].getCurrOcn();
        ocn2 = EVAL_ACCESSLISTTABLE[planNo2].getCurrOcn();

        return m_storageManager->PDC_Meet(ocn1, SM_FALSE, &(op1.data.oid), ocn2, SM_FALSE, &(op2.data.oid));
    }
    else if(op1.type == OOSQL_TYPE_OID && op2.type == OOSQL_TYPE_MBR)
    {
        planNo1 = ACCESSPLAN.getExprElem(*op1Idx)->pathExpr.planNo;
        ocn1 = EVAL_ACCESSLISTTABLE[planNo1].getCurrOcn();

        region.x1 = op2.data.mbr.values[0];
        region.y1 = op2.data.mbr.values[1];
        region.x2 = op2.data.mbr.values[2];
        region.y2 = op2.data.mbr.values[3];
        
        return m_storageManager->PDC_MeetWithRegion(ocn1, SM_FALSE, &(op1.data.oid), region);
    }
    else if(op1.type == OOSQL_TYPE_MBR && op2.type == OOSQL_TYPE_OID)
    {
        planNo2 = ACCESSPLAN.getExprElem(*op2Idx)->pathExpr.planNo;
        ocn2 = EVAL_ACCESSLISTTABLE[planNo2].getCurrOcn();

        region.x1 = op1.data.mbr.values[0];
        region.y1 = op1.data.mbr.values[1];
        region.x2 = op1.data.mbr.values[2];
        region.y2 = op1.data.mbr.values[3];
        
        return m_storageManager->PDC_MeetWithRegion(ocn2, SM_FALSE, &(op2.data.oid), region);
    }
    else if(op1.type == OOSQL_TYPE_MBR && op2.type == OOSQL_TYPE_MBR)
    {
        regionPoints1[0].x = op1.data.mbr.values[0];
		regionPoints1[0].y = op1.data.mbr.values[1];
		regionPoints1[1].x = op1.data.mbr.values[2];
		regionPoints1[1].y = op1.data.mbr.values[1];
		regionPoints1[2].x = op1.data.mbr.values[2];
		regionPoints1[2].y = op1.data.mbr.values[3];
		regionPoints1[3].x = op1.data.mbr.values[0];
		regionPoints1[3].y = op1.data.mbr.values[3];

		regionPoints2[0].x = op2.data.mbr.values[0];
		regionPoints2[0].y = op2.data.mbr.values[1];
		regionPoints2[1].x = op2.data.mbr.values[2];
		regionPoints2[1].y = op2.data.mbr.values[1];
		regionPoints2[2].x = op2.data.mbr.values[2];
		regionPoints2[2].y = op2.data.mbr.values[3];
		regionPoints2[3].x = op2.data.mbr.values[0];
		regionPoints2[3].y = op2.data.mbr.values[3];

        return m_storageManager->PDC_Meet(OOSQL_StorageManager::POLYGON_CLASSTYPE, 4, regionPoints1,
			                              OOSQL_StorageManager::POLYGON_CLASSTYPE, 4, regionPoints2);
    }
    
    return SM_FALSE;
}

Four OOSQL_Evaluator::geoSpatialCmpOverlap( 
    AP_ExprPoolIndex	*op1Idx,	// IN: index to the 1st operand
    AP_ExprPoolIndex	*op2Idx)	// IN: index to the 2nd operand
{
    OOSQL_DB_Value					op1(pMemoryManager), op2(pMemoryManager);	// the evaluation result for each operand
    Four							e;
    Four							ocn1, ocn2;
    Four							planNo1, planNo2;
    OOSQL_StorageManager::Region    region;
	OOSQL_StorageManager::Point		regionPoints1[4], regionPoints2[4];
    
    e = evalExpression(op1Idx, &op1);
    if (e < eNOERROR) OOSQL_ERR(e);

    e = evalExpression(op2Idx, &op2);
    if (e < eNOERROR) OOSQL_ERR(e);

    if(op1.type == OOSQL_TYPE_OID && op2.type == OOSQL_TYPE_OID)
    {
        planNo1 = ACCESSPLAN.getExprElem(*op1Idx)->pathExpr.planNo;
        planNo2 = ACCESSPLAN.getExprElem(*op2Idx)->pathExpr.planNo;
        ocn1 = EVAL_ACCESSLISTTABLE[planNo1].getCurrOcn();
        ocn2 = EVAL_ACCESSLISTTABLE[planNo2].getCurrOcn();

        return m_storageManager->PDC_Intersect(ocn1, SM_FALSE, &(op1.data.oid), ocn2, SM_FALSE, &(op2.data.oid));
    }
    else if(op1.type == OOSQL_TYPE_OID && op2.type == OOSQL_TYPE_MBR)
    {
        planNo1 = ACCESSPLAN.getExprElem(*op1Idx)->pathExpr.planNo;
        ocn1 = EVAL_ACCESSLISTTABLE[planNo1].getCurrOcn();

        region.x1 = op2.data.mbr.values[0];
        region.y1 = op2.data.mbr.values[1];
        region.x2 = op2.data.mbr.values[2];
        region.y2 = op2.data.mbr.values[3];
        
        return m_storageManager->PDC_IntersectWithRegion(ocn1, SM_FALSE, &(op1.data.oid), region);
    }
    else if(op1.type == OOSQL_TYPE_MBR && op2.type == OOSQL_TYPE_OID)
    {
        planNo2 = ACCESSPLAN.getExprElem(*op2Idx)->pathExpr.planNo;
        ocn2 = EVAL_ACCESSLISTTABLE[planNo2].getCurrOcn();

        region.x1 = op1.data.mbr.values[0];
        region.y1 = op1.data.mbr.values[1];
        region.x2 = op1.data.mbr.values[2];
        region.y2 = op1.data.mbr.values[3];

        return m_storageManager->PDC_IntersectWithRegion(ocn2, SM_FALSE, &(op2.data.oid), region);
    }
    else if(op1.type == OOSQL_TYPE_MBR && op2.type == OOSQL_TYPE_MBR)
    {
        regionPoints1[0].x = op1.data.mbr.values[0];
		regionPoints1[0].y = op1.data.mbr.values[1];
		regionPoints1[1].x = op1.data.mbr.values[2];
		regionPoints1[1].y = op1.data.mbr.values[1];
		regionPoints1[2].x = op1.data.mbr.values[2];
		regionPoints1[2].y = op1.data.mbr.values[3];
		regionPoints1[3].x = op1.data.mbr.values[0];
		regionPoints1[3].y = op1.data.mbr.values[3];

		regionPoints2[0].x = op2.data.mbr.values[0];
		regionPoints2[0].y = op2.data.mbr.values[1];
		regionPoints2[1].x = op2.data.mbr.values[2];
		regionPoints2[1].y = op2.data.mbr.values[1];
		regionPoints2[2].x = op2.data.mbr.values[2];
		regionPoints2[2].y = op2.data.mbr.values[3];
		regionPoints2[3].x = op2.data.mbr.values[0];
		regionPoints2[3].y = op2.data.mbr.values[3];

        return m_storageManager->PDC_Intersect(OOSQL_StorageManager::POLYGON_CLASSTYPE, 4, regionPoints1,
			                                   OOSQL_StorageManager::POLYGON_CLASSTYPE, 4, regionPoints2);
    }

    return SM_FALSE;
}

Four OOSQL_Evaluator::geoSpatialArithArea(
	AP_FuncEvalInfo *funcInfo,              // IN:
    OOSQL_DB_Value  *result                 // OUT:
)
{
	Four						e;
	Four						ocn;
	AP_ArgumentPoolElements		argList;
	OOSQL_StorageManager::OID*	oid;
	double						area;


	/* check input parameters */
#ifdef  OOSQL_DEBUG
	if(funcInfo == NULL) OOSQL_ERR(eBADPARAMETER_OOSQL);
	if(result == NULL) OOSQL_ERR(eBADPARAMETER_OOSQL);
#endif

	/* get the arguement list */
    argList = ACCESSPLAN.getArgumentPool( funcInfo->argument );

	/* get open class number for the class designated by the first arguement */
    ocn = EVAL_ACCESSLISTTABLE[argList[0].pathExpr.col.planNo].getCurrOcn();
	oid = EVAL_EVALBUFFER[argList[0].pathExpr.col.planNo].getOID_Ptr();

	e = m_storageManager->PDC_GetArea(ocn, SM_FALSE, oid, &area);
	OOSQL_CHECK_ERR(e);

	result->data.d = area;
	result->SetType(OOSQL_TYPE_DOUBLE);

	return eNOERROR;
}

Four OOSQL_Evaluator::geoSpatialArithLength(
	AP_FuncEvalInfo *funcInfo,              // IN:
    OOSQL_DB_Value  *result                 // OUT:
)
{
	Four						e;
	Four						ocn;
	AP_ArgumentPoolElements		argList;
	OOSQL_StorageManager::OID*	oid;
	double						length;


	/* check input parameters */
#ifdef  OOSQL_DEBUG
	if(funcInfo == NULL) OOSQL_ERR(eBADPARAMETER_OOSQL);
	if(result == NULL) OOSQL_ERR(eBADPARAMETER_OOSQL);
#endif

	/* get the arguement list */
    argList = ACCESSPLAN.getArgumentPool( funcInfo->argument );

	/* get open class number for the class designated by the first arguement */
    ocn = EVAL_ACCESSLISTTABLE[argList[0].pathExpr.col.planNo].getCurrOcn();
	oid = EVAL_EVALBUFFER[argList[0].pathExpr.col.planNo].getOID_Ptr();

	e = m_storageManager->PDC_GetLength(ocn, SM_FALSE, oid, &length);
	OOSQL_CHECK_ERR(e);

	result->data.d = length;
	result->SetType(OOSQL_TYPE_DOUBLE);

	return eNOERROR;
}

Four OOSQL_Evaluator::geoSpatialArithDistance(
	AP_FuncEvalInfo *funcInfo,              // IN:
    OOSQL_DB_Value  *result                 // OUT:
)
{
	Four						e;
	AP_ArgumentPoolElements		argList;
	Four						ocn1;
	OOSQL_StorageManager::OID*	oid1;
	Four						ocn2;
	OOSQL_StorageManager::OID*	oid2;
	double						distance;
	OOSQL_StorageManager::Point queryPoint1, queryPoint2;
	ValueElement*				pValue;
	

	/* check input parameters */
#ifdef  OOSQL_DEBUG
	if(funcInfo == NULL) OOSQL_ERR(eBADPARAMETER_OOSQL);
	if(result == NULL) OOSQL_ERR(eBADPARAMETER_OOSQL);
#endif

	/* get the arguement list */
    argList = ACCESSPLAN.getArgumentPool( funcInfo->argument );

	// possible argument combition
	//   1. distance([0, 0], object)
	//   2. distance(object, [0, 0])
	//   3. distance([0, 0], [10, 10])
	//   4. distance(object1, object2)

	/* get open class number for the class designated by the first arguement */
	if(argList[0].argumentKind == ARGUMENT_KIND_VALUE && argList[1].argumentKind == ARGUMENT_KIND_PATHEXPR)
	{
		ocn2 = EVAL_ACCESSLISTTABLE[argList[1].pathExpr.col.planNo].getCurrOcn();
		oid2 = EVAL_EVALBUFFER[argList[1].pathExpr.col.planNo].getOID_Ptr();

		pValue = ACCESSPLAN.getValueElem(argList[0].value);
		queryPoint1.x = ACCESSPLAN.getMBRValue(pValue->mbr, 0);
		queryPoint1.y = ACCESSPLAN.getMBRValue(pValue->mbr, 1);

		e = m_storageManager->PDC_GetDistanceWithPoint(ocn2, SM_FALSE, oid2, queryPoint1, &distance);
		OOSQL_CHECK_ERR(e);
	} 
	else if(argList[0].argumentKind == ARGUMENT_KIND_PATHEXPR && argList[1].argumentKind == ARGUMENT_KIND_VALUE)
	{
		ocn1 = EVAL_ACCESSLISTTABLE[argList[0].pathExpr.col.planNo].getCurrOcn();
		oid1 = EVAL_EVALBUFFER[argList[0].pathExpr.col.planNo].getOID_Ptr();

		pValue = ACCESSPLAN.getValueElem(argList[1].value);
		queryPoint1.x = ACCESSPLAN.getMBRValue(pValue->mbr, 0);
		queryPoint1.y = ACCESSPLAN.getMBRValue(pValue->mbr, 1);

		e = m_storageManager->PDC_GetDistanceWithPoint(ocn1, SM_FALSE, oid1, queryPoint1, &distance);
		OOSQL_CHECK_ERR(e);
	}
	else if(argList[0].argumentKind == ARGUMENT_KIND_VALUE && argList[1].argumentKind == ARGUMENT_KIND_VALUE)
	{
		pValue = ACCESSPLAN.getValueElem(argList[0].value);
		queryPoint1.x = ACCESSPLAN.getMBRValue(pValue->mbr, 0);
		queryPoint1.y = ACCESSPLAN.getMBRValue(pValue->mbr, 1);

		pValue = ACCESSPLAN.getValueElem(argList[1].value);
		queryPoint2.x = ACCESSPLAN.getMBRValue(pValue->mbr, 0);
		queryPoint2.y = ACCESSPLAN.getMBRValue(pValue->mbr, 1);

		distance = sqrt((queryPoint1.x - queryPoint2.x) * (queryPoint1.x - queryPoint2.x) +
		                (queryPoint1.y - queryPoint2.y) * (queryPoint1.y - queryPoint2.y));
	}
	else if(argList[0].argumentKind == ARGUMENT_KIND_PATHEXPR && argList[1].argumentKind == ARGUMENT_KIND_PATHEXPR)
	{
		ocn1 = EVAL_ACCESSLISTTABLE[argList[0].pathExpr.col.planNo].getCurrOcn();
		oid1 = EVAL_EVALBUFFER[argList[0].pathExpr.col.planNo].getOID_Ptr();

		ocn2 = EVAL_ACCESSLISTTABLE[argList[1].pathExpr.col.planNo].getCurrOcn();
		oid2 = EVAL_EVALBUFFER[argList[1].pathExpr.col.planNo].getOID_Ptr();

		e = m_storageManager->PDC_GetDistance(ocn1, SM_FALSE, oid1, ocn2, SM_FALSE, oid2, &distance);
		OOSQL_CHECK_ERR(e);
	}

	result->data.d = distance;
	result->SetType(OOSQL_TYPE_DOUBLE);

	return eNOERROR;
}

Four OOSQL_Evaluator::execUserDefinedFunction(
	AP_FuncEvalInfo *funcInfo,              // IN:
    OOSQL_DB_Value  *result                 // OUT:
)
{
	OOSQL_ExternalFunctionInfo		functionInfo;
	Four							e;
	AP_ArgumentPoolElements			arguments(m_pool->ap_argumentPool);
	Four							i;
	OOSQL_TCArray<OOSQL_DB_Value>	dbValues(pMemoryManager, true);
	OOSQL_DbInfo					dbInfo;
	char							errorMessage[OOSQL_EXTFUNC_ERRORMESSAGE_SIZE];

	// get user defined function information
	e = m_externalFunctionManager->Find(m_volID, funcInfo->userDefinedFunctionID, functionInfo);
	OOSQL_CHECK_ERR(e);

	// prepare argument
	dbValues.resize(functionInfo.m_paramCount);
	arguments = funcInfo->argument;
	for(i = 0; i < functionInfo.m_paramCount; i++)
	{
		switch(arguments[i].argumentKind)
		{
		case ARGUMENT_KIND_PATHEXPR: 
			e = evalExpression(&arguments[i].pathExpr, &dbValues[i]);
			OOSQL_CHECK_ERR(e);
			break;
		case ARGUMENT_KIND_VALUE:
			e = evalExpression(&arguments[i].value, &dbValues[i], (Four)functionInfo.m_parameters[i].m_argumentType);
			OOSQL_CHECK_ERR(e);
			break;
		case ARGUMENT_KIND_EXPR:
			e = evalExpression(&arguments[i].expr, &dbValues[i]);
			OOSQL_CHECK_ERR(e);
			break;
		case ARGUMENT_KIND_FUNCEVAL:
			e = evalExpression(&arguments[i].funcEval, &dbValues[i]);
			OOSQL_CHECK_ERR(e);
			break;

		case ARGUMENT_KIND_TEXTIR_SUBPLAN:
		case ARGUMENT_KIND_FUNC:
		case ARGUMENT_KIND_DOMAIN: 
		case ARGUMENT_KIND_AGGRFUNC:
		case ARGUMENT_KIND_TEMPFILECOL:
		case ARGUMENT_KIND_AGGRFUNCRESULT:
		case ARGUMENT_KIND_FUNCRESULT:
		default:
			OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
			break;
		}

		if((functionInfo.m_parameters[i].m_argumentType == TYPEID_FLOAT || functionInfo.m_parameters[i].m_argumentType == TYPEID_DOUBLE) &&
		   (dbValues[i].type == TYPEID_SHORT || dbValues[i].type == TYPEID_INT || dbValues[i].type == TYPEID_LONG || dbValues[i].type == TYPEID_LONG_LONG))
		{
			Four   temp;
			
			switch(dbValues[i].type)
			{
			case TYPEID_SHORT:
				temp = dbValues[i].data.s; break;
			case TYPEID_INT:
				temp = dbValues[i].data.i; break;
			case TYPEID_LONG:
				temp = dbValues[i].data.l; break;
			case TYPEID_LONG_LONG:
				temp = dbValues[i].data.ll; break;
			}

			switch(functionInfo.m_parameters[i].m_argumentType)
			{
			case TYPEID_FLOAT:
				dbValues[i].data.f = temp; break;
			case TYPEID_DOUBLE:
				dbValues[i].data.d = temp; break;
			}
			
			dbValues[i].type = (Two)functionInfo.m_parameters[i].m_argumentType;
		}
		else if((functionInfo.m_parameters[i].m_argumentType == TYPEID_SHORT || functionInfo.m_parameters[i].m_argumentType == TYPEID_INT || functionInfo.m_parameters[i].m_argumentType == TYPEID_LONG || functionInfo.m_parameters[i].m_argumentType == TYPEID_LONG_LONG) &&
		        (dbValues[i].type == TYPEID_FLOAT || dbValues[i].type == TYPEID_DOUBLE))
		{
			double   temp;
			
			switch(dbValues[i].type)
			{
			case TYPEID_FLOAT:
				temp = dbValues[i].data.f; break;
			case TYPEID_DOUBLE:
				temp = dbValues[i].data.d; break;
			}

			switch(functionInfo.m_parameters[i].m_argumentType)
			{
			case TYPEID_SHORT:
				dbValues[i].data.s = temp; break;
			case TYPEID_INT:
				dbValues[i].data.i = temp; break;
			case TYPEID_LONG:
				dbValues[i].data.l = temp; break;
			case TYPEID_LONG_LONG:
				dbValues[i].data.ll = temp; break;
			}
			
			dbValues[i].type = (Two)functionInfo.m_parameters[i].m_argumentType;
		}
	}

	e = m_externalFunctionDispatcher->Execute(functionInfo, *result, functionInfo.m_paramCount, &dbValues[0], errorMessage, 
		                                      0);
	OOSQL_CHECK_ERR(e);

	return eNOERROR;
}


#ifndef SLIMDOWN_OPENGIS
Four OOSQL_Evaluator::evalOGISOperator(
    AP_OperatorStruct	*op,         /* IN: index to expression m_pool element */
    OOSQL_DB_Value		&result      /* OUT: evaluation result */
)
/*
    Function:
        Evaluate an expression node of which kind is an OGIS operator.

    Side effect:

    Return value:
        DB_Value containing the evaluation result.
*/
{
    OOSQL_DB_Value	op1(pMemoryManager);  // result of the 1st operand
    OOSQL_DB_Value  op2(pMemoryManager);  // result of the 2nd operand
    OOSQL_DB_Value  op3(pMemoryManager);  // result of the 3rd operand
    char 			errorMessage[OOSQL_EXTFUNC_ERRORMESSAGE_SIZE];
	Four 			e;
    Boolean 		isNullReturnValue;
	Boolean			isConstantArg[3];

	void*			paramReturn;
	int*			paramReturnLength;
	int 			paramSizes[3];
	void*			paramPtrs[3];

	OOSQL_DB_Value*	res = &result;

	switch (op->operatorId)
	{
        // OOSQL_TYPE_GEOMETRY;
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
        
        // OOSQL_TYPE_POINT
        case OP_OGIS_POINTFT:
        case OP_OGIS_POINTFB:
        case OP_OGIS_CENTRIOD:
        case OP_OGIS_STARTPOINT:
        case OP_OGIS_ENDPOINT:
        case OP_OGIS_POINTONSURFACE:
        case OP_OGIS_POINTN:

        // OOSQL_TYPE_LINESTRING
        case OP_OGIS_LINESTRINGFT:
        case OP_OGIS_LINESTRINGFB:
        case OP_OGIS_EXTERIORRING:
        case OP_OGIS_INTERIORRINGN:

        // OOSQL_TYPE_POLYGON
        case OP_OGIS_POLYGONFT:
        case OP_OGIS_POLYGONFB:
        case OP_OGIS_ENVELOPE:

        // OOSQL_TYPE_MULTIPOINT
        case OP_OGIS_MULTIPOINTFT:
        case OP_OGIS_MULTIPOINTFB:

        // OOSQL_TYPE_MULTILINESTRING
        case OP_OGIS_MULTILINESTRINGFT:
        case OP_OGIS_MULTILINESTRINGFB:

        // OOSQL_TYPE_MULTIPOLYGON
        case OP_OGIS_MULTIPOLYGONFT:
        case OP_OGIS_MULTIPOLYGONFB:

        // OOSQL_TYPE_GEOMETRYCOLLECTION
        case OP_OGIS_GEOMETRYCOLLECTIONFT:
        case OP_OGIS_GEOMETRYCOLLECTIONFB:

        // OOSQL_TYPE_STRING
        case OP_OGIS_ASTEXT:
        case OP_OGIS_ASBINARY:
        case OP_OGIS_GEOMETRYTYPE:
        
            result.PrepareData(UDTOBJECT_DEFAULT_SIZE);
            paramReturn = result.data.ptr;
			result.SetType(OOSQL_TYPE_VARSTRING);
            
            break;

		// OOSQL_TYPE_INT
        case OP_OGIS_DIMENSION:
        case OP_OGIS_SRID:
        case OP_OGIS_NUMGEOMETRIES:
        case OP_OGIS_NUMPOINTS:
        case OP_OGIS_NUMINTERIORRINGS:

            result.PrepareData(OOSQL_TYPE_INT_SIZE);
            paramReturn = (void*)&(result.data.i);
			result.SetType(OOSQL_TYPE_INT);

            break;

        // OOSQL_TYPE_FLOAT
        case OP_OGIS_LENGTH:
        case OP_OGIS_X:
        case OP_OGIS_Y:
        case OP_OGIS_AREA:
        case OP_OGIS_DISTANCE:
        
            result.PrepareData(OOSQL_TYPE_FLOAT_SIZE);
            paramReturn = (void*)&(result.data.f);
			result.SetType(OOSQL_TYPE_FLOAT);

            break;

        // OOSQL_TYPE_BOOL
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
        
            result.PrepareData(OOSQL_TYPE_BOOL_SIZE);
            paramReturn = (void*)&(result.data.b);
			result.SetType(OOSQL_TYPE_BOOL);
			
            break;

		default:
			OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
			break;
    }

	paramReturnLength = (int*)&(result.length);
	
    switch (op->operatorId) {
		case OP_OGIS_ASTEXT:
		case OP_OGIS_ASBINARY:
		case OP_OGIS_DIMENSION:
		case OP_OGIS_GEOMETRYTYPE:
		case OP_OGIS_SRID:
		case OP_OGIS_BOUNDARY:
		case OP_OGIS_LENGTH:
		case OP_OGIS_X:
		case OP_OGIS_Y:
		case OP_OGIS_AREA:
		case OP_OGIS_NUMGEOMETRIES:
		case OP_OGIS_NUMPOINTS:
		case OP_OGIS_NUMINTERIORRINGS:
		case OP_OGIS_ISEMPTY:
		case OP_OGIS_ISSIMPLE:
		case OP_OGIS_ISCLOSED:
		case OP_OGIS_ISRING:
		case OP_OGIS_ENVELOPE:
		case OP_OGIS_CONVEXHULL:
		case OP_OGIS_EXTERIORRING:
		case OP_OGIS_CENTRIOD:
		case OP_OGIS_STARTPOINT:
		case OP_OGIS_ENDPOINT:
		case OP_OGIS_POINTONSURFACE: // unary operators
		
			// evaluate 1st operand recursively
			e = evalExpression( &(op->operand1), &op1 );
			if (e < eNOERROR) {
				OOSQL_ERR(e);
			}
			paramSizes[0] = op1.length;
			#ifdef ENABLE_OPENGIS_OPTIMIZATION
			isConstantArg[0] = ACCESSPLAN.getExprElem(op->operand1)->oper.isConstant;
			#endif

			switch (op1.type)
			{
			case TYPEID_STRING:
			case TYPEID_VARSTRING:
			case TYPEID_TEXT:
            case TYPEID_OGIS_GEOMETRY:
            case TYPEID_OGIS_POINT:
            case TYPEID_OGIS_LINESTRING:
            case TYPEID_OGIS_POLYGON:
            case TYPEID_OGIS_GEOMETRYCOLLECTION:
            case TYPEID_OGIS_MULTIPOINT:
            case TYPEID_OGIS_MULTILINESTRING:
            case TYPEID_OGIS_MULTIPOLYGON:
				paramPtrs[0] = (void*)op1.data.ptr;
				break;
			default:
				paramPtrs[0] = (void*)&(op1.data.s);
				break;
			}

			break;
			
		case OP_OGIS_GEOMETRYFT:
		case OP_OGIS_POINTFT:
		case OP_OGIS_LINESTRINGFT:
		case OP_OGIS_POLYGONFT:
		case OP_OGIS_MULTIPOINTFT:
		case OP_OGIS_MULTILINESTRINGFT:
		case OP_OGIS_MULTIPOLYGONFT:
		case OP_OGIS_GEOMETRYCOLLECTIONFT:
		case OP_OGIS_GEOMETRYFB:
		case OP_OGIS_POINTFB:
		case OP_OGIS_LINESTRINGFB:
		case OP_OGIS_POLYGONFB:
		case OP_OGIS_MULTIPOINTFB:
		case OP_OGIS_MULTILINESTRINGFB:
		case OP_OGIS_MULTIPOLYGONFB:
		case OP_OGIS_GEOMETRYCOLLECTIONFB:
		case OP_OGIS_CONTAINS:
		case OP_OGIS_CROSSES:
		case OP_OGIS_DISJOINT:
		case OP_OGIS_EQUALS:
		case OP_OGIS_INTERSECTS:
		case OP_OGIS_OVERLAPS:
		case OP_OGIS_TOUCHES:
		case OP_OGIS_WITHIN:
		case OP_OGIS_DIFFERENCE:
		case OP_OGIS_INTERSECTION:
		case OP_OGIS_SYMDIFFERENCE:
		case OP_OGIS_UNION:
		case OP_OGIS_DISTANCE:
		case OP_OGIS_BUFFER:
		case OP_OGIS_INTERIORRINGN:
		case OP_OGIS_POINTN:
		case OP_OGIS_GEOMETRYN: // binary operators
		
			// evaluate 1st operand recursively
			e = evalExpression( &(op->operand1), &op1 );
			if (e < eNOERROR) {
				OOSQL_ERR(e);
			}
			
			// evaluate 2nd operand recursively
			e = evalExpression( &(op->operand2), &op2 );
			if (e < eNOERROR) {
				OOSQL_ERR(e);
			}
			paramSizes[0] = op1.length;
			paramSizes[1] = op2.length;
			#ifdef ENABLE_OPENGIS_OPTIMIZATION
			isConstantArg[0] = ACCESSPLAN.getExprElem(op->operand1)->oper.isConstant;
			isConstantArg[1] = ACCESSPLAN.getExprElem(op->operand2)->oper.isConstant;
			#endif


			switch (op1.type)
			{
			case TYPEID_STRING:
			case TYPEID_VARSTRING:
			case TYPEID_TEXT:
            case TYPEID_OGIS_GEOMETRY:
            case TYPEID_OGIS_POINT:
            case TYPEID_OGIS_LINESTRING:
            case TYPEID_OGIS_POLYGON:
            case TYPEID_OGIS_GEOMETRYCOLLECTION:
            case TYPEID_OGIS_MULTIPOINT:
            case TYPEID_OGIS_MULTILINESTRING:
            case TYPEID_OGIS_MULTIPOLYGON:
				paramPtrs[0] = (void*)op1.data.ptr;
				break;
			default:
				paramPtrs[0] = (void*)&(op1.data.s);
				break;
			}

			switch (op2.type)
			{
			case TYPEID_STRING:
			case TYPEID_VARSTRING:
			case TYPEID_TEXT:
            case TYPEID_OGIS_GEOMETRY:
            case TYPEID_OGIS_POINT:
            case TYPEID_OGIS_LINESTRING:
            case TYPEID_OGIS_POLYGON:
            case TYPEID_OGIS_GEOMETRYCOLLECTION:
            case TYPEID_OGIS_MULTIPOINT:
            case TYPEID_OGIS_MULTILINESTRING:
            case TYPEID_OGIS_MULTIPOLYGON:
				paramPtrs[1] = (void*)op2.data.ptr;
				break;
			default:
				paramPtrs[1] = (void*)&(op2.data.s);
				break;
			}
			break;
		
		case OP_OGIS_RELATED: // 3-ary operators
		
			// evaluate 1st operand recursively
			e = evalExpression( &(op->operand1), &op1 );
			if (e < eNOERROR) {
				OOSQL_ERR(e);
			}
			
			// evaluate 2nd operand recursively
			e = evalExpression( &(op->operand2), &op2 );
			if (e < eNOERROR) {
				OOSQL_ERR(e);
			}
			
			// evaluate 3rd operand recursively
			e = evalExpression( &(op->operand3), &op3 );
			if (e < eNOERROR) {
				OOSQL_ERR(e);
			}
			paramSizes[0] = op1.length;
			paramSizes[1] = op2.length;
			paramSizes[2] = op3.length;
			#ifdef ENABLE_OPENGIS_OPTIMIZATION
			isConstantArg[0] = ACCESSPLAN.getExprElem(op->operand1)->oper.isConstant;
			isConstantArg[1] = ACCESSPLAN.getExprElem(op->operand2)->oper.isConstant;
			isConstantArg[2] = ACCESSPLAN.getExprElem(op->operand3)->oper.isConstant;
			#endif

			switch (op1.type)
			{
			case TYPEID_STRING:
			case TYPEID_VARSTRING:
			case TYPEID_TEXT:
            case TYPEID_OGIS_GEOMETRY:
            case TYPEID_OGIS_POINT:
            case TYPEID_OGIS_LINESTRING:
            case TYPEID_OGIS_POLYGON:
            case TYPEID_OGIS_GEOMETRYCOLLECTION:
            case TYPEID_OGIS_MULTIPOINT:
            case TYPEID_OGIS_MULTILINESTRING:
            case TYPEID_OGIS_MULTIPOLYGON:
				paramPtrs[0] = (void*)op1.data.ptr;
				break;
			default:
				paramPtrs[0] = (void*)&(op1.data.s);
				break;
			}

			switch (op2.type)
			{
			case TYPEID_STRING:
			case TYPEID_VARSTRING:
			case TYPEID_TEXT:
            case TYPEID_OGIS_GEOMETRY:
            case TYPEID_OGIS_POINT:
            case TYPEID_OGIS_LINESTRING:
            case TYPEID_OGIS_POLYGON:
            case TYPEID_OGIS_GEOMETRYCOLLECTION:
            case TYPEID_OGIS_MULTIPOINT:
            case TYPEID_OGIS_MULTILINESTRING:
            case TYPEID_OGIS_MULTIPOLYGON:
				paramPtrs[1] = (void*)op2.data.ptr;
				break;
			default:
				paramPtrs[1] = (void*)&(op2.data.s);
				break;
			}

			switch (op3.type)
			{
			case TYPEID_STRING:
			case TYPEID_VARSTRING:
			case TYPEID_TEXT:
            case TYPEID_OGIS_GEOMETRY:
            case TYPEID_OGIS_POINT:
            case TYPEID_OGIS_LINESTRING:
            case TYPEID_OGIS_POLYGON:
            case TYPEID_OGIS_GEOMETRYCOLLECTION:
            case TYPEID_OGIS_MULTIPOINT:
            case TYPEID_OGIS_MULTILINESTRING:
            case TYPEID_OGIS_MULTIPOLYGON:
				paramPtrs[2] = (void*)op3.data.ptr;
				break;
			default:
				paramPtrs[2] = (void*)&(op3.data.s);
				break;
			}
			break;
		default:
			OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
	}
	
	// 1 to 1 match for each OGIS operator and UDF.cxx function
    switch (op->operatorId) {
		case OP_OGIS_GEOMETRYFT:
			e = OGIS_GeometryFromText(paramPtrs[0], paramPtrs[1], paramReturn, paramReturnLength,
                        &(op1.nullFlag), &(op2.nullFlag), &isNullReturnValue,
                        0, 0, errorMessage, paramSizes, m_storageManager->GetOOSQL_SystemHandle(),
                        0, /*m_dbInfo*/0
                        #ifdef ENABLE_OPENGIS_OPTIMIZATION
                        ,op->UDFNo, isConstantArg
                        #endif
                        );
			OOSQL_CHECK_ERR(e);
			break;
			
		case OP_OGIS_POINTFT:
			e = OGIS_PointFromText(paramPtrs[0], paramPtrs[1], paramReturn, paramReturnLength,
                        &(op1.nullFlag), &(op2.nullFlag), &isNullReturnValue,
                        0, 0, errorMessage, paramSizes, m_storageManager->GetOOSQL_SystemHandle(),
                        0, /*m_dbInfo*/0
                        #ifdef ENABLE_OPENGIS_OPTIMIZATION
                        ,op->UDFNo, isConstantArg
                        #endif
                        );
			OOSQL_CHECK_ERR(e);
			break;
			
		case OP_OGIS_LINESTRINGFT:
			e = OGIS_LineStringFromText(paramPtrs[0], paramPtrs[1], paramReturn, paramReturnLength,
                        &(op1.nullFlag), &(op2.nullFlag), &isNullReturnValue,
                        0, 0, errorMessage, paramSizes, m_storageManager->GetOOSQL_SystemHandle(),
                        0, /*m_dbInfo*/0
                        #ifdef ENABLE_OPENGIS_OPTIMIZATION
                        ,op->UDFNo, isConstantArg
                        #endif
                        );
			OOSQL_CHECK_ERR(e);
			break;
			
		case OP_OGIS_POLYGONFT:
			e = OGIS_PolygonFromText(paramPtrs[0], paramPtrs[1], paramReturn, paramReturnLength,
                        &(op1.nullFlag), &(op2.nullFlag), &isNullReturnValue,
                        0, 0, errorMessage, paramSizes, m_storageManager->GetOOSQL_SystemHandle(),
                        0, /*m_dbInfo*/0
                        #ifdef ENABLE_OPENGIS_OPTIMIZATION
                        ,op->UDFNo, isConstantArg
                        #endif
                        );
			OOSQL_CHECK_ERR(e);
			break;
			
		case OP_OGIS_MULTIPOINTFT:
			e = OGIS_MultiPointFromText(paramPtrs[0], paramPtrs[1], paramReturn, paramReturnLength,
                        &(op1.nullFlag), &(op2.nullFlag), &isNullReturnValue,
                        0, 0, errorMessage, paramSizes, m_storageManager->GetOOSQL_SystemHandle(),
                        0, /*m_dbInfo*/0
                        #ifdef ENABLE_OPENGIS_OPTIMIZATION
                        ,op->UDFNo, isConstantArg
                        #endif
                        );
			OOSQL_CHECK_ERR(e);
			break;
			
		case OP_OGIS_MULTILINESTRINGFT:
			e = OGIS_MultiLineStringFromText(paramPtrs[0], paramPtrs[1], paramReturn, paramReturnLength,
                        &(op1.nullFlag), &(op2.nullFlag), &isNullReturnValue,
                        0, 0, errorMessage, paramSizes, m_storageManager->GetOOSQL_SystemHandle(),
                        0, /*m_dbInfo*/0
                        #ifdef ENABLE_OPENGIS_OPTIMIZATION
                        ,op->UDFNo, isConstantArg
                        #endif
                        );
			OOSQL_CHECK_ERR(e);
			break;
			
		case OP_OGIS_MULTIPOLYGONFT:
			e = OGIS_MultiPolygonFromText(paramPtrs[0], paramPtrs[1], paramReturn, paramReturnLength,
                        &(op1.nullFlag), &(op2.nullFlag), &isNullReturnValue,
                        0, 0, errorMessage, paramSizes, m_storageManager->GetOOSQL_SystemHandle(),
                        0, /*m_dbInfo*/0
                        #ifdef ENABLE_OPENGIS_OPTIMIZATION
                        ,op->UDFNo, isConstantArg
                        #endif
                        );
			OOSQL_CHECK_ERR(e);
			break;
			
		case OP_OGIS_GEOMETRYCOLLECTIONFT:
			e = OGIS_GeometryCollectionFromText(paramPtrs[0], paramPtrs[1], paramReturn, paramReturnLength,
                        &(op1.nullFlag), &(op2.nullFlag), &isNullReturnValue,
                        0, 0, errorMessage, paramSizes, m_storageManager->GetOOSQL_SystemHandle(),
                        0, /*m_dbInfo*/0
                        #ifdef ENABLE_OPENGIS_OPTIMIZATION
                        ,op->UDFNo, isConstantArg
                        #endif
                        );
			OOSQL_CHECK_ERR(e);
			break;
			
		case OP_OGIS_GEOMETRYFB:
			e = OGIS_GeometryFromWKB(paramPtrs[0], paramPtrs[1], paramReturn, paramReturnLength,
                        &(op1.nullFlag), &(op2.nullFlag), &isNullReturnValue,
                        0, 0, errorMessage, paramSizes, m_storageManager->GetOOSQL_SystemHandle(),
                        0, /*m_dbInfo*/0
                        #ifdef ENABLE_OPENGIS_OPTIMIZATION
                        ,op->UDFNo, isConstantArg
                        #endif
                        );
			OOSQL_CHECK_ERR(e);
			break;
			
		case OP_OGIS_POINTFB:
			e = OGIS_PointFromWKB(paramPtrs[0], paramPtrs[1], paramReturn, paramReturnLength,
                        &(op1.nullFlag), &(op2.nullFlag), &isNullReturnValue,
                        0, 0, errorMessage, paramSizes, m_storageManager->GetOOSQL_SystemHandle(),
                        0, /*m_dbInfo*/0
                        #ifdef ENABLE_OPENGIS_OPTIMIZATION
                        ,op->UDFNo, isConstantArg
                        #endif
                        );
			OOSQL_CHECK_ERR(e);
			break;
			
		case OP_OGIS_LINESTRINGFB:
			e = OGIS_LineStringFromWKB(paramPtrs[0], paramPtrs[1], paramReturn, paramReturnLength,
                        &(op1.nullFlag), &(op2.nullFlag), &isNullReturnValue,
                        0, 0, errorMessage, paramSizes, m_storageManager->GetOOSQL_SystemHandle(),
                        0, /*m_dbInfo*/0
                        #ifdef ENABLE_OPENGIS_OPTIMIZATION
                        ,op->UDFNo, isConstantArg
                        #endif
                        );
			OOSQL_CHECK_ERR(e);
			break;
			
		case OP_OGIS_POLYGONFB:
			e = OGIS_PolygonFromWKB(paramPtrs[0], paramPtrs[1], paramReturn, paramReturnLength,
                        &(op1.nullFlag), &(op2.nullFlag), &isNullReturnValue,
                        0, 0, errorMessage, paramSizes, m_storageManager->GetOOSQL_SystemHandle(),
                        0, /*m_dbInfo*/0
                        #ifdef ENABLE_OPENGIS_OPTIMIZATION
                        ,op->UDFNo, isConstantArg
                        #endif
                        );
			OOSQL_CHECK_ERR(e);
			break;
			
		case OP_OGIS_MULTIPOINTFB:
			e = OGIS_MultiPointFromWKB(paramPtrs[0], paramPtrs[1], paramReturn, paramReturnLength,
                        &(op1.nullFlag), &(op2.nullFlag), &isNullReturnValue,
                        0, 0, errorMessage, paramSizes, m_storageManager->GetOOSQL_SystemHandle(),
                        0, /*m_dbInfo*/0
                        #ifdef ENABLE_OPENGIS_OPTIMIZATION
                        ,op->UDFNo, isConstantArg
                        #endif
                        );
			OOSQL_CHECK_ERR(e);
			break;
			
		case OP_OGIS_MULTILINESTRINGFB:
			e = OGIS_MultiLineStringFromWKB(paramPtrs[0], paramPtrs[1], paramReturn, paramReturnLength,
                        &(op1.nullFlag), &(op2.nullFlag), &isNullReturnValue,
                        0, 0, errorMessage, paramSizes, m_storageManager->GetOOSQL_SystemHandle(),
                        0, /*m_dbInfo*/0
                        #ifdef ENABLE_OPENGIS_OPTIMIZATION
                        ,op->UDFNo, isConstantArg
                        #endif
                        );
			OOSQL_CHECK_ERR(e);
			break;
			
		case OP_OGIS_MULTIPOLYGONFB:
			e = OGIS_MultiPolygonFromWKB(paramPtrs[0], paramPtrs[1], paramReturn, paramReturnLength,
                        &(op1.nullFlag), &(op2.nullFlag), &isNullReturnValue,
                        0, 0, errorMessage, paramSizes, m_storageManager->GetOOSQL_SystemHandle(),
                        0, /*m_dbInfo*/0
                        #ifdef ENABLE_OPENGIS_OPTIMIZATION
                        ,op->UDFNo, isConstantArg
                        #endif
                        );
			OOSQL_CHECK_ERR(e);
			break;
			
		case OP_OGIS_GEOMETRYCOLLECTIONFB:
			e = OGIS_GeometryCollectionFromWKB(paramPtrs[0], paramPtrs[1], paramReturn, paramReturnLength,
                        &(op1.nullFlag), &(op2.nullFlag), &isNullReturnValue,
                        0, 0, errorMessage, paramSizes, m_storageManager->GetOOSQL_SystemHandle(),
                        0, /*m_dbInfo*/0
                        #ifdef ENABLE_OPENGIS_OPTIMIZATION
                        ,op->UDFNo, isConstantArg
                        #endif
                        );
			OOSQL_CHECK_ERR(e);
			break;
			
		case OP_OGIS_ASTEXT:
			e = OGIS_AsText(paramPtrs[0], paramReturn, paramReturnLength,
                        &(op1.nullFlag), &isNullReturnValue,
                        0, 0, errorMessage, paramSizes, m_storageManager->GetOOSQL_SystemHandle(),
                        0, /*m_dbInfo*/0
                        #ifdef ENABLE_OPENGIS_OPTIMIZATION
                        ,op->UDFNo, isConstantArg
                        #endif
                        );
			OOSQL_CHECK_ERR(e);
			break;
			
		case OP_OGIS_ASBINARY:
			e = OGIS_AsBinary(paramPtrs[0], paramReturn, paramReturnLength,
                        &(op1.nullFlag), &isNullReturnValue,
                        0, 0, errorMessage, paramSizes, m_storageManager->GetOOSQL_SystemHandle(),
                        0, /*m_dbInfo*/0
                        #ifdef ENABLE_OPENGIS_OPTIMIZATION
                        ,op->UDFNo, isConstantArg
                        #endif
                        );
			OOSQL_CHECK_ERR(e);
			break;
			
		case OP_OGIS_DIMENSION:
			e = OGIS_Dimension(paramPtrs[0], paramReturn, paramReturnLength,
                        &(op1.nullFlag), &isNullReturnValue,
                        0, 0, errorMessage, paramSizes, m_storageManager->GetOOSQL_SystemHandle(),
                        0, /*m_dbInfo*/0
                        #ifdef ENABLE_OPENGIS_OPTIMIZATION
                        ,op->UDFNo, isConstantArg
                        #endif
                        );
			OOSQL_CHECK_ERR(e);
			break;
			
		case OP_OGIS_GEOMETRYTYPE:
			e = OGIS_GeometryType(paramPtrs[0], paramReturn, paramReturnLength,
                        &(op1.nullFlag), &isNullReturnValue,
                        0, 0, errorMessage, paramSizes, m_storageManager->GetOOSQL_SystemHandle(),
                        0, /*m_dbInfo*/0
                        #ifdef ENABLE_OPENGIS_OPTIMIZATION
                        ,op->UDFNo, isConstantArg
                        #endif
                        );
			OOSQL_CHECK_ERR(e);
			break;
			
		case OP_OGIS_SRID:
			e = OGIS_SRID(paramPtrs[0], paramReturn, paramReturnLength,
                        &(op1.nullFlag), &isNullReturnValue,
                        0, 0, errorMessage, paramSizes, m_storageManager->GetOOSQL_SystemHandle(),
                        0, /*m_dbInfo*/0
                        #ifdef ENABLE_OPENGIS_OPTIMIZATION
                        ,op->UDFNo, isConstantArg
                        #endif
                        );
			OOSQL_CHECK_ERR(e);
			break;
			
		case OP_OGIS_BOUNDARY:
			e = OGIS_Boundary(paramPtrs[0], paramReturn, paramReturnLength,
                        &(op1.nullFlag), &isNullReturnValue,
                        0, 0, errorMessage, paramSizes, m_storageManager->GetOOSQL_SystemHandle(),
                        0, /*m_dbInfo*/0
                        #ifdef ENABLE_OPENGIS_OPTIMIZATION
                        ,op->UDFNo, isConstantArg
                        #endif
                        );
			OOSQL_CHECK_ERR(e);
			break;
			
		case OP_OGIS_LENGTH:
			e = OGIS_Length(paramPtrs[0], paramReturn, paramReturnLength,
                        &(op1.nullFlag), &isNullReturnValue,
                        0, 0, errorMessage, paramSizes, m_storageManager->GetOOSQL_SystemHandle(),
                        0, /*m_dbInfo*/0
                        #ifdef ENABLE_OPENGIS_OPTIMIZATION
                        ,op->UDFNo, isConstantArg
                        #endif
                        );
			OOSQL_CHECK_ERR(e);
			break;
			
		case OP_OGIS_X:
			e = OGIS_X(paramPtrs[0], paramReturn, paramReturnLength,
                        &(op1.nullFlag), &isNullReturnValue,
                        0, 0, errorMessage, paramSizes, m_storageManager->GetOOSQL_SystemHandle(),
                        0, /*m_dbInfo*/0
                        #ifdef ENABLE_OPENGIS_OPTIMIZATION
                        ,op->UDFNo, isConstantArg
                        #endif
                        );
			OOSQL_CHECK_ERR(e);
			break;
			
		case OP_OGIS_Y:
			e = OGIS_Y(paramPtrs[0], paramReturn, paramReturnLength,
                        &(op1.nullFlag), &isNullReturnValue,
                        0, 0, errorMessage, paramSizes, m_storageManager->GetOOSQL_SystemHandle(),
                        0, /*m_dbInfo*/0
                        #ifdef ENABLE_OPENGIS_OPTIMIZATION
                        ,op->UDFNo, isConstantArg
                        #endif
                        );
			OOSQL_CHECK_ERR(e);
			break;
			
		case OP_OGIS_AREA:
			e = OGIS_Area(paramPtrs[0], paramReturn, paramReturnLength,
                        &(op1.nullFlag), &isNullReturnValue,
                        0, 0, errorMessage, paramSizes, m_storageManager->GetOOSQL_SystemHandle(),
                        0, /*m_dbInfo*/0
                        #ifdef ENABLE_OPENGIS_OPTIMIZATION
                        ,op->UDFNo, isConstantArg
                        #endif
                        );
			OOSQL_CHECK_ERR(e);
			break;
			
		case OP_OGIS_NUMGEOMETRIES:
			e = OGIS_NumGeometries(paramPtrs[0], paramReturn, paramReturnLength,
                        &(op1.nullFlag), &isNullReturnValue,
                        0, 0, errorMessage, paramSizes, m_storageManager->GetOOSQL_SystemHandle(),
                        0, /*m_dbInfo*/0
                        #ifdef ENABLE_OPENGIS_OPTIMIZATION
                        ,op->UDFNo, isConstantArg
                        #endif
                        );
			OOSQL_CHECK_ERR(e);
			break;
			
		case OP_OGIS_NUMPOINTS:
			e = OGIS_NumPoints(paramPtrs[0], paramReturn, paramReturnLength,
                        &(op1.nullFlag), &isNullReturnValue,
                        0, 0, errorMessage, paramSizes, m_storageManager->GetOOSQL_SystemHandle(),
                        0, /*m_dbInfo*/0
                        #ifdef ENABLE_OPENGIS_OPTIMIZATION
                        ,op->UDFNo, isConstantArg
                        #endif
                        );
			OOSQL_CHECK_ERR(e);
			break;
			
		case OP_OGIS_NUMINTERIORRINGS:
			e = OGIS_NumInteriorRings(paramPtrs[0], paramReturn, paramReturnLength,
                        &(op1.nullFlag), &isNullReturnValue,
                        0, 0, errorMessage, paramSizes, m_storageManager->GetOOSQL_SystemHandle(),
                        0, /*m_dbInfo*/0
                        #ifdef ENABLE_OPENGIS_OPTIMIZATION
                        ,op->UDFNo, isConstantArg
                        #endif
                        );
			OOSQL_CHECK_ERR(e);
			break;
			
		case OP_OGIS_ISEMPTY:
			e = OGIS_IsEmpty(paramPtrs[0], paramReturn, paramReturnLength,
                        &(op1.nullFlag), &isNullReturnValue,
                        0, 0, errorMessage, paramSizes, m_storageManager->GetOOSQL_SystemHandle(),
                        0, /*m_dbInfo*/0
                        #ifdef ENABLE_OPENGIS_OPTIMIZATION
                        ,op->UDFNo, isConstantArg
                        #endif
                        );
			OOSQL_CHECK_ERR(e);
			break;
			
		case OP_OGIS_ISSIMPLE:
			e = OGIS_IsSimple(paramPtrs[0], paramReturn, paramReturnLength,
                        &(op1.nullFlag), &isNullReturnValue,
                        0, 0, errorMessage, paramSizes, m_storageManager->GetOOSQL_SystemHandle(),
                        0, /*m_dbInfo*/0
                        #ifdef ENABLE_OPENGIS_OPTIMIZATION
                        ,op->UDFNo, isConstantArg
                        #endif
                        );
			OOSQL_CHECK_ERR(e);
			break;
			
		case OP_OGIS_ISCLOSED:
			e = OGIS_IsClosed(paramPtrs[0], paramReturn, paramReturnLength,
                        &(op1.nullFlag), &isNullReturnValue,
                        0, 0, errorMessage, paramSizes, m_storageManager->GetOOSQL_SystemHandle(),
                        0, /*m_dbInfo*/0
                        #ifdef ENABLE_OPENGIS_OPTIMIZATION
                        ,op->UDFNo, isConstantArg
                        #endif
                        );
			OOSQL_CHECK_ERR(e);
			break;
			
		case OP_OGIS_ISRING:
			e = OGIS_IsRing(paramPtrs[0], paramReturn, paramReturnLength,
                        &(op1.nullFlag), &isNullReturnValue,
                        0, 0, errorMessage, paramSizes, m_storageManager->GetOOSQL_SystemHandle(),
                        0, /*m_dbInfo*/0
                        #ifdef ENABLE_OPENGIS_OPTIMIZATION
                        ,op->UDFNo, isConstantArg
                        #endif
                        );
			OOSQL_CHECK_ERR(e);
			break;
			
		case OP_OGIS_CONTAINS:
			e = OGIS_Contains(paramPtrs[0], paramPtrs[1], paramReturn, paramReturnLength,
                        &(op1.nullFlag), &(op2.nullFlag), &isNullReturnValue,
                        0, 0, errorMessage, paramSizes, m_storageManager->GetOOSQL_SystemHandle(),
                        0, /*m_dbInfo*/0
                        #ifdef ENABLE_OPENGIS_OPTIMIZATION
                        ,op->UDFNo, isConstantArg
                        #endif
                        );
			OOSQL_CHECK_ERR(e);
			break;
			
		case OP_OGIS_CROSSES:
			e = OGIS_Crosses(paramPtrs[0], paramPtrs[1], paramReturn, paramReturnLength,
                        &(op1.nullFlag), &(op2.nullFlag), &isNullReturnValue,
                        0, 0, errorMessage, paramSizes, m_storageManager->GetOOSQL_SystemHandle(),
                        0, /*m_dbInfo*/0
                        #ifdef ENABLE_OPENGIS_OPTIMIZATION
                        ,op->UDFNo, isConstantArg
                        #endif
                        );
			OOSQL_CHECK_ERR(e);
			break;
			
		case OP_OGIS_DISJOINT:
			e = OGIS_Disjoint(paramPtrs[0], paramPtrs[1], paramReturn, paramReturnLength,
                        &(op1.nullFlag), &(op2.nullFlag), &isNullReturnValue,
                        0, 0, errorMessage, paramSizes, m_storageManager->GetOOSQL_SystemHandle(),
                        0, /*m_dbInfo*/0
                        #ifdef ENABLE_OPENGIS_OPTIMIZATION
                        ,op->UDFNo, isConstantArg
                        #endif
                        );
			OOSQL_CHECK_ERR(e);
			break;
			
		case OP_OGIS_EQUALS:
			e = OGIS_Equals(paramPtrs[0], paramPtrs[1], paramReturn, paramReturnLength,
                        &(op1.nullFlag), &(op2.nullFlag), &isNullReturnValue,
                        0, 0, errorMessage, paramSizes, m_storageManager->GetOOSQL_SystemHandle(),
                        0, /*m_dbInfo*/0
                        #ifdef ENABLE_OPENGIS_OPTIMIZATION
                        ,op->UDFNo, isConstantArg
                        #endif
                        );
			OOSQL_CHECK_ERR(e);
			break;
			
		case OP_OGIS_INTERSECTS:
			e = OGIS_Intersects(paramPtrs[0], paramPtrs[1], paramReturn, paramReturnLength,
                        &(op1.nullFlag), &(op2.nullFlag), &isNullReturnValue,
                        0, 0, errorMessage, paramSizes, m_storageManager->GetOOSQL_SystemHandle(),
                        0, /*m_dbInfo*/0
                        #ifdef ENABLE_OPENGIS_OPTIMIZATION
                        ,op->UDFNo, isConstantArg
                        #endif
                        );
			OOSQL_CHECK_ERR(e);
			break;
			
		case OP_OGIS_OVERLAPS:
			e = OGIS_Overlaps(paramPtrs[0], paramPtrs[1], paramReturn, paramReturnLength,
                        &(op1.nullFlag), &(op2.nullFlag), &isNullReturnValue,
                        0, 0, errorMessage, paramSizes, m_storageManager->GetOOSQL_SystemHandle(),
                        0, /*m_dbInfo*/0
                        #ifdef ENABLE_OPENGIS_OPTIMIZATION
                        ,op->UDFNo, isConstantArg
                        #endif
                        );
			OOSQL_CHECK_ERR(e);
			break;
			
		case OP_OGIS_RELATED:
			e = OGIS_Related(paramPtrs[0], paramPtrs[1], paramPtrs[2], paramReturn, paramReturnLength,
                        &(op1.nullFlag), &(op2.nullFlag), &(op3.nullFlag), &isNullReturnValue,
                        0, 0, errorMessage, paramSizes, m_storageManager->GetOOSQL_SystemHandle(),
                        0, /*m_dbInfo*/0
                        #ifdef ENABLE_OPENGIS_OPTIMIZATION
                        ,op->UDFNo, isConstantArg
                        #endif
                        );
			OOSQL_CHECK_ERR(e);
			break;
			
		case OP_OGIS_TOUCHES:
			e = OGIS_Touches(paramPtrs[0], paramPtrs[1], paramReturn, paramReturnLength,
                        &(op1.nullFlag), &(op2.nullFlag), &isNullReturnValue,
                        0, 0, errorMessage, paramSizes, m_storageManager->GetOOSQL_SystemHandle(),
                        0, /*m_dbInfo*/0
                        #ifdef ENABLE_OPENGIS_OPTIMIZATION
                        ,op->UDFNo, isConstantArg
                        #endif
                        );
			OOSQL_CHECK_ERR(e);
			break;
			
		case OP_OGIS_WITHIN:
			e = OGIS_Within(paramPtrs[0], paramPtrs[1], paramReturn, paramReturnLength,
                        &(op1.nullFlag), &(op2.nullFlag), &isNullReturnValue,
                        0, 0, errorMessage, paramSizes, m_storageManager->GetOOSQL_SystemHandle(),
                        0, /*m_dbInfo*/0
                        #ifdef ENABLE_OPENGIS_OPTIMIZATION
                        ,op->UDFNo, isConstantArg
                        #endif
                        );
			OOSQL_CHECK_ERR(e);
			break;
			
		case OP_OGIS_DIFFERENCE:
			e = OGIS_Difference(paramPtrs[0], paramPtrs[1], paramReturn, paramReturnLength,
                        &(op1.nullFlag), &(op2.nullFlag), &isNullReturnValue,
                        0, 0, errorMessage, paramSizes, m_storageManager->GetOOSQL_SystemHandle(),
                        0, /*m_dbInfo*/0
                        #ifdef ENABLE_OPENGIS_OPTIMIZATION
                        ,op->UDFNo, isConstantArg
                        #endif
                        );
			OOSQL_CHECK_ERR(e);
			break;
			
		case OP_OGIS_INTERSECTION:
			e = OGIS_Intersection(paramPtrs[0], paramPtrs[1], paramReturn, paramReturnLength,
                        &(op1.nullFlag), &(op2.nullFlag), &isNullReturnValue,
                        0, 0, errorMessage, paramSizes, m_storageManager->GetOOSQL_SystemHandle(),
                        0, /*m_dbInfo*/0
                        #ifdef ENABLE_OPENGIS_OPTIMIZATION
                        ,op->UDFNo, isConstantArg
                        #endif
                        );
			OOSQL_CHECK_ERR(e);
			break;
			
		case OP_OGIS_SYMDIFFERENCE:
			e = OGIS_SymDifference(paramPtrs[0], paramPtrs[1], paramReturn, paramReturnLength,
                        &(op1.nullFlag), &(op2.nullFlag), &isNullReturnValue,
                        0, 0, errorMessage, paramSizes, m_storageManager->GetOOSQL_SystemHandle(),
                        0, /*m_dbInfo*/0
                        #ifdef ENABLE_OPENGIS_OPTIMIZATION
                        ,op->UDFNo, isConstantArg
                        #endif
                        );
			OOSQL_CHECK_ERR(e);
			break;
			
		case OP_OGIS_UNION:
			e = OGIS_Union(paramPtrs[0], paramPtrs[1], paramReturn, paramReturnLength,
                        &(op1.nullFlag), &(op2.nullFlag), &isNullReturnValue,
                        0, 0, errorMessage, paramSizes, m_storageManager->GetOOSQL_SystemHandle(),
                        0, /*m_dbInfo*/0
                        #ifdef ENABLE_OPENGIS_OPTIMIZATION
                        ,op->UDFNo, isConstantArg
                        #endif
                        );
			OOSQL_CHECK_ERR(e);
			break;
			
		case OP_OGIS_DISTANCE:
			e = OGIS_Distance(paramPtrs[0], paramPtrs[1], paramReturn, paramReturnLength,
                        &(op1.nullFlag), &(op2.nullFlag), &isNullReturnValue,
                        0, 0, errorMessage, paramSizes, m_storageManager->GetOOSQL_SystemHandle(),
                        0, /*m_dbInfo*/0
                        #ifdef ENABLE_OPENGIS_OPTIMIZATION
                        ,op->UDFNo, isConstantArg
                        #endif
                        );
			OOSQL_CHECK_ERR(e);
			break;
			
		case OP_OGIS_ENVELOPE:
			e = OGIS_Envelope(paramPtrs[0], paramReturn, paramReturnLength,
                        &(op1.nullFlag), &isNullReturnValue,
                        0, 0, errorMessage, paramSizes, m_storageManager->GetOOSQL_SystemHandle(),
                        0, /*m_dbInfo*/0
                        #ifdef ENABLE_OPENGIS_OPTIMIZATION
                        ,op->UDFNo, isConstantArg
                        #endif
                        );
			OOSQL_CHECK_ERR(e);
			break;
			
		case OP_OGIS_BUFFER:
			e = OGIS_Buffer(paramPtrs[0], paramPtrs[1], paramReturn, paramReturnLength,
                        &(op1.nullFlag), &(op2.nullFlag), &isNullReturnValue,
                        0, 0, errorMessage, paramSizes, m_storageManager->GetOOSQL_SystemHandle(),
                        0, /*m_dbInfo*/0
                        #ifdef ENABLE_OPENGIS_OPTIMIZATION
                        ,op->UDFNo, isConstantArg
                        #endif
                        );
			OOSQL_CHECK_ERR(e);
			break;
			
		case OP_OGIS_CONVEXHULL:
			e = OGIS_ConvexHull(paramPtrs[0], paramReturn, paramReturnLength,
                        &(op1.nullFlag), &isNullReturnValue,
                        0, 0, errorMessage, paramSizes, m_storageManager->GetOOSQL_SystemHandle(),
                        0, /*m_dbInfo*/0
                        #ifdef ENABLE_OPENGIS_OPTIMIZATION
                        ,op->UDFNo, isConstantArg
                        #endif
                        );
			OOSQL_CHECK_ERR(e);
			break;
			
		case OP_OGIS_EXTERIORRING:
			e = OGIS_ExteriorRing(paramPtrs[0], paramReturn, paramReturnLength,
                        &(op1.nullFlag), &isNullReturnValue,
                        0, 0, errorMessage, paramSizes, m_storageManager->GetOOSQL_SystemHandle(),
                        0, /*m_dbInfo*/0
                        #ifdef ENABLE_OPENGIS_OPTIMIZATION
                        ,op->UDFNo, isConstantArg
                        #endif
                        );
			OOSQL_CHECK_ERR(e);
			break;
			
		case OP_OGIS_INTERIORRINGN:
			e = OGIS_InteriorRingN(paramPtrs[0], paramPtrs[1], paramReturn, paramReturnLength,
                        &(op1.nullFlag), &(op2.nullFlag), &isNullReturnValue,
                        0, 0, errorMessage, paramSizes, m_storageManager->GetOOSQL_SystemHandle(),
                        0, /*m_dbInfo*/0
                        #ifdef ENABLE_OPENGIS_OPTIMIZATION
                        ,op->UDFNo, isConstantArg
                        #endif
                        );
			OOSQL_CHECK_ERR(e);
			break;
			
		case OP_OGIS_CENTRIOD:
			e = OGIS_Centroid(paramPtrs[0], paramReturn, paramReturnLength,
                        &(op1.nullFlag), &isNullReturnValue,
                        0, 0, errorMessage, paramSizes, m_storageManager->GetOOSQL_SystemHandle(),
                        0, /*m_dbInfo*/0
                        #ifdef ENABLE_OPENGIS_OPTIMIZATION
                        ,op->UDFNo, isConstantArg
                        #endif
                        );
			OOSQL_CHECK_ERR(e);
			break;
			
		case OP_OGIS_STARTPOINT:
			e = OGIS_StartPoint(paramPtrs[0], paramReturn, paramReturnLength,
                        &(op1.nullFlag), &isNullReturnValue,
                        0, 0, errorMessage, paramSizes, m_storageManager->GetOOSQL_SystemHandle(),
                        0, /*m_dbInfo*/0
                        #ifdef ENABLE_OPENGIS_OPTIMIZATION
                        ,op->UDFNo, isConstantArg
                        #endif
                        );
			OOSQL_CHECK_ERR(e);
			break;
			
		case OP_OGIS_ENDPOINT:
			e = OGIS_EndPoint(paramPtrs[0], paramReturn, paramReturnLength,
                        &(op1.nullFlag), &isNullReturnValue,
                        0, 0, errorMessage, paramSizes, m_storageManager->GetOOSQL_SystemHandle(),
                        0, /*m_dbInfo*/0
                        #ifdef ENABLE_OPENGIS_OPTIMIZATION
                        ,op->UDFNo, isConstantArg
                        #endif
                        );
			OOSQL_CHECK_ERR(e);
			break;
			
		case OP_OGIS_POINTONSURFACE:
			e = OGIS_PointOnSurface(paramPtrs[0], paramReturn, paramReturnLength,
                        &(op1.nullFlag), &isNullReturnValue,
                        0, 0, errorMessage, paramSizes, m_storageManager->GetOOSQL_SystemHandle(),
                        0, /*m_dbInfo*/0
                        #ifdef ENABLE_OPENGIS_OPTIMIZATION
                        ,op->UDFNo, isConstantArg
                        #endif
                        );
			OOSQL_CHECK_ERR(e);
			break;
			
		case OP_OGIS_POINTN:
			e = OGIS_PointN(paramPtrs[0], paramPtrs[1], paramReturn, paramReturnLength,
                        &(op1.nullFlag), &(op2.nullFlag), &isNullReturnValue,
                        0, 0, errorMessage, paramSizes, m_storageManager->GetOOSQL_SystemHandle(),
                        0, /*m_dbInfo*/0
                        #ifdef ENABLE_OPENGIS_OPTIMIZATION
                        ,op->UDFNo, isConstantArg
                        #endif
                        );
			OOSQL_CHECK_ERR(e);
			break;
			
		case OP_OGIS_GEOMETRYN:
			e = OGIS_GeometryN(paramPtrs[0], paramPtrs[1], paramReturn, paramReturnLength,
                        &(op1.nullFlag), &(op2.nullFlag), &isNullReturnValue,
                        0, 0, errorMessage, paramSizes, m_storageManager->GetOOSQL_SystemHandle(),
                        0, /*m_dbInfo*/0
                        #ifdef ENABLE_OPENGIS_OPTIMIZATION
                        ,op->UDFNo, isConstantArg
                        #endif
                        );
			OOSQL_CHECK_ERR(e);
			break;
			
		default:
			OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
	}

	return eNOERROR;
}
#endif
