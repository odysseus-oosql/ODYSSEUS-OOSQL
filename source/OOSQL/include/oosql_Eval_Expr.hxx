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

#ifndef __OOSQL_EVAL_EXPR_HXX__
#define __OOSQL_EVAL_EXPR_HXX__

#include "OOSQL_Evaluator.hxx"

inline Four OOSQL_Evaluator::evalExpression(
    AP_PathExprAccessInfo*  pathExprInfo, 
    OOSQL_DB_Value*         res            // OUT: evaluation result
)
{
    AP_UsedColPoolElements  pColInfoPool;       /* used column info. */
    Four                    planNo;             /* index to access plan element */
    Four                    colNo;              /* column number */
    Four                    mappedColNo;        /* column number */
    Four                    e;                  /* error code */

    switch (pathExprInfo->kind) 
    {
    case PATHEXPR_KIND_OID:
        planNo      = pathExprInfo->planNo;
        res->SetType(OOSQL_TYPE_OID);
        memcpy(&(res->data.oid), EVAL_EVALBUFFER[planNo].getOID_Ptr(), sizeof(OOSQL_StorageManager::OID));
        break;

    case PATHEXPR_KIND_OBJECT:
        planNo = pathExprInfo->planNo;
        break;

    case PATHEXPR_KIND_ATTR:
        planNo = pathExprInfo->col.planNo;
        colNo = pathExprInfo->col.colNo;
        mappedColNo = ACCESSPLAN.getMappedColNo( planNo, colNo );

        if(mappedColNo >= 0)
        {
            pColInfoPool = ACCESSPLAN.getUsedColPool(planNo);

            if(m_isCheckingHavingCond)
                e = copyColListStructToValue( pColInfoPool[mappedColNo].typeId,
                        EVAL_EVALBUFFER[planNo].getGrpBySlotPtr(mappedColNo), res );
            else
                e = copyColListStructToValue( pColInfoPool[mappedColNo].typeId,
                        EVAL_EVALBUFFER[planNo].getColSlotPtr(mappedColNo), res ); 
            if (e < eNOERROR)
                OOSQL_ERR(e);
        }
        else
        {
            Four                                ocn;
            OOSQL_StorageManager::OID*          oid;
            OOSQL_StorageManager::TextDesc      textDesc;
            OOSQL_StorageManager::TextColStruct text;
            TypeID                              typeID;
            CataClassInfo                       classInfo;
            CataAttrInfo                        attrInfo;

            oid = EVAL_EVALBUFFER[planNo].getOID_Ptr();
            ocn = EVAL_ACCESSLISTTABLE[planNo].getCurrOcn();

            e = m_catalog->class_ClassId_to_ClassInfo(ACCESSPLANELEMENTS[planNo].classInfo.classId, classInfo);
            OOSQL_CHECK_ERR(e);
            e = m_catalog->attr_ColNo_to_AttrInfo(classInfo, colNo, attrInfo);
            OOSQL_CHECK_ERR(e);
            e = m_catalog->attr_GetTypeID(classInfo, attrInfo, typeID);
            OOSQL_CHECK_ERR(e);
            if(typeID == TYPEID_TEXT)
            {
                e = m_storageManager->Text_GetDescriptor(ocn, SM_FALSE, oid, colNo, &textDesc);
                OOSQL_CHECK_ERR(e);

                res->type = TYPEID_STRING;
                res->PrepareData(OOSQL_EVALBUFFER_MAXSTRINGSIZE + 1);
                text.start      = 0;
                text.length     = OOSQL_EVALBUFFER_MAXSTRINGSIZE;
                text.dataLength = OOSQL_EVALBUFFER_MAXSTRINGSIZE;
                text.data       = res->data.ptr;
                e = m_storageManager->Text_FetchContent(ocn, SM_FALSE, oid, colNo, &text, &textDesc);
                OOSQL_CHECK_ERR(e);

                memset((char*)res->data.ptr + text.retLength, 0, 1);    // null termination
                res->length = text.retLength;

				if (res -> length == -1)
					res->nullFlag = SM_TRUE;
				else
					res->nullFlag = SM_FALSE;
            }
            else
			{
				OOSQL_ComplexTypeInfo   complexTypeInfo;
				
				e = GetComplexTypeInfo(colNo - 1, &complexTypeInfo);
				OOSQL_CHECK_ERR(e);
				
				e = OOSQL_ComplexType_IsNULL(&complexTypeInfo);
				OOSQL_CHECK_ERR(e);
				
				if(e)
				{
					res->nullFlag = SM_TRUE;
				}
				else
				{
					OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
				}
			}
        }

        break;

    case PATHEXPR_KIND_METHOD:
        OOSQL_ERR( eNOTIMPLEMENTED_METHOD_OOSQL);

    default:
        OOSQL_ERR( eINVALID_PATHEXPR_OOSQL);
    }

    return eNOERROR;
}

inline Four OOSQL_Evaluator::evalExpression( 
    AP_ValuePoolIndex*      valueIdx, 
    OOSQL_DB_Value*         res,           // OUT: evaluation result
    Four                    valueType
)
{
    ValueElement*           pValue;             /* ptr. to value pool element */
    Four                    e;                  /* error code */

    pValue = ACCESSPLAN.getValueElem(*valueIdx);

    res->nullFlag = SM_FALSE;
    switch (pValue->valueKind) {
        case VALUE_KIND_STRING:
            res->SetTypeLength(OOSQL_TYPE_STRING, GET_POOLSIZE(pValue->string));
            res->PrepareData(res->length + 1);            
            memcpy(res->data.ptr, (char*)ACCESSPLAN.getStringValue(pValue->string), res->length);
            memset((char*)res->data.ptr + res->length, 0, 1);
            break;

        case VALUE_KIND_INTEGER:
            if(valueType == OOSQL_TYPE_SHORT)
            {
                res->SetType(OOSQL_TYPE_SHORT); 
                res->data.s = ACCESSPLAN.getIntValue(pValue->integer);
            }
            else if(valueType == OOSQL_TYPE_INT)
            {
                res->SetType(OOSQL_TYPE_INT);       
                res->data.i = ACCESSPLAN.getIntValue(pValue->integer);
            }
            else if(valueType == OOSQL_TYPE_LONG)
            {
                res->SetType(OOSQL_TYPE_LONG);      
                res->data.l = ACCESSPLAN.getIntValue(pValue->integer);
            }
            else
            {
                res->SetType(OOSQL_TYPE_LONG_LONG);     
                res->data.ll = ACCESSPLAN.getIntValue(pValue->integer);
            }
            break;

        case VALUE_KIND_REAL:
            if(valueType == OOSQL_TYPE_FLOAT)
            {
                res->SetType(OOSQL_TYPE_FLOAT); 
                res->data.f = ACCESSPLAN.getRealValue(pValue->real);
            }
            else
            {
                res->SetType(OOSQL_TYPE_DOUBLE);    
                res->data.d = ACCESSPLAN.getRealValue(pValue->real);
            }
            break;

        case VALUE_KIND_BOOL:
            res->SetType(OOSQL_TYPE_INT);       
            res->data.i = pValue->boolean.startIndex;
            break;
        case VALUE_KIND_NIL:
            res->SetTypeLength(OOSQL_TYPE_STRING, 0);   
            res->nullFlag = SM_TRUE;
            break;
        case VALUE_KIND_MBR:
            res->SetType(OOSQL_TYPE_MBR);
            res->data.mbr.values[0] = ACCESSPLAN.getMBRValue(pValue->mbr, 0);
            res->data.mbr.values[1] = ACCESSPLAN.getMBRValue(pValue->mbr, 1);
            res->data.mbr.values[2] = ACCESSPLAN.getMBRValue(pValue->mbr, 2);
            res->data.mbr.values[3] = ACCESSPLAN.getMBRValue(pValue->mbr, 3);
            break;
        case VALUE_KIND_DATE:
            res->SetType(OOSQL_TYPE_DATE);
            e = ACCESSPLAN.getDateValue(pValue->date, &res->data.date);
            if(e < eNOERROR) OOSQL_ERR(e);
            break;
        case VALUE_KIND_TIME:
            res->SetType(OOSQL_TYPE_TIME);
            e = ACCESSPLAN.getTimeValue(pValue->time, &res->data.time);
            if(e < eNOERROR) OOSQL_ERR(e);
            break;
        case VALUE_KIND_TIMESTAMP:
            res->SetType(OOSQL_TYPE_TIMESTAMP);
            e = ACCESSPLAN.getTimestampValue(pValue->timestamp, &res->data.timestamp);
            if(e < eNOERROR) OOSQL_ERR(e);
            break;
        case VALUE_KIND_INTERVAL:
            res->SetType(OOSQL_TYPE_INTERVAL);
            e = ACCESSPLAN.getIntervalValue(pValue->interval, &res->data.interval);
            if(e < eNOERROR) OOSQL_ERR(e);
            break;
        default:
            break;
    }
    return eNOERROR;
}

inline Four OOSQL_Evaluator::evalExpression( 
    AP_FuncEvalInfo*        funcEvalInfo, 
    OOSQL_DB_Value*         res            // OUT: evaluation result
)
{
    Four e;

    e = evalFunction(funcEvalInfo, res);
    OOSQL_CHECK_ERR(e);

    return eNOERROR;
}

inline Four OOSQL_Evaluator::evalAggrFuncResult(
        AP_ExprPoolIndex   *exprIndex,     // IN: index to expression pool element
        OOSQL_DB_Value     *res            // OUT: evaluation result
)
/*
    Function:
        Evaluate an expression node of which kind is an aggregate function result.

    Side effect:

    Return value:
        DB_Value containing the evaluation result.
*/
{
        AP_ExprElement  *exprNode;
        EVAL_EvalBufferSlot     *aggrFuncRes;
        Four    e;              // return code

        /* check input parameters */
#ifdef  OOSQL_DEBUG
        if (exprIndex == NULL) OOSQL_ERR(eBADPARAMETER_OOSQL);
        if (res == NULL) OOSQL_ERR(eBADPARAMETER_OOSQL);
#endif

        /* get pointer to expression node */
        exprNode = ACCESSPLAN.getExprElem(*exprIndex);

        /* check if expression node kind is EXPR_KIND_AGGRFUNCRESULT */
#ifdef  OOSQL_DEBUG
        if (exprNode->exprKind != EXPR_KIND_AGGRFUNCRESULT)
            OOSQL_ERR(eBADPARAMETER_OOSQL);
#endif

        /* get pointer to evaluation buffer slot for the aggregate function result */
        aggrFuncRes = EVAL_EVALBUFFER[ exprNode->aggrFuncResult.planNo ].getAggrFuncResSlotPtr(
                        exprNode->aggrFuncResult.aggrFuncIndex);

        /* copy the aggregate function result */
        e = copyColListStructToValue( exprNode->resultType, aggrFuncRes, res );
        if (e < eNOERROR) OOSQL_ERR(e);

        /* return */
        return eNOERROR;
}

inline Four OOSQL_Evaluator::evalFunctionResult(
        AP_FuncResultAccessInfo *funcResult,    // IN:
        OOSQL_DB_Value          *result         // OUT:
)
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
        Four    e;

#ifdef  TRACE
        printf("\nOOSQL_Evaluator::evalFunctionResult( funcResult = %X, result = %X )", funcResult, result);
#endif

        /* check input parameters */
#ifdef  OOSQL_DEBUG
        if (funcResult == NULL) {
            OOSQL_ERR(eBADPARAMETER_OOSQL);
        }
        if (result == NULL) {
            OOSQL_ERR(eBADPARAMETER_OOSQL);
        }
#endif

        /* get the function result according to the function ID */
        switch ( funcResult->functionID ) {
            case FUNCTION_TEXTIR_WEIGHT:
                e = execTextIR_FnWeight( funcResult, result );
                if (e < eNOERROR) OOSQL_ERR(e);

                break;

            default:    /* invalid function ID */
                OOSQL_ERR(eINVALID_FUNC_OOSQL);
        }

        /* return */
        return eNOERROR;
}


inline Four OOSQL_Evaluator::arithAdd( 
        AP_ExprPoolIndex   *op1Idx,        // IN: index to the 1st operand
        AP_ExprPoolIndex   *op2Idx,        // IN: index to the 2nd operand
        OOSQL_DB_Value     *res            // IN/OUT: addition result
)
/*
    Function:
        Perform arithmetic binary addition.

    Side effect:
        'res' is filled with the addition result.

    Return value:
        eNOERROR        if no error
        < eNOERROR      error code
*/
{
        OOSQL_DB_Value      op1(pMemoryManager), op2(pMemoryManager);       // the evaluation result for each operand
        Four                e;

        e = evalExpression( op1Idx, &op1 );
        if (e < eNOERROR) {
            OOSQL_ERR(e);
        }
        e = evalExpression( op2Idx, &op2 );
        if (e < eNOERROR) {
            OOSQL_ERR(e);
        }


		if(op1.nullFlag || op2.nullFlag)
		{
			res->nullFlag = SM_TRUE;
			return eNOERROR;
		}
		else
		{
			res->nullFlag = SM_FALSE;
		}

        switch (ACCESSPLAN.getExprElem(*op1Idx)->resultType) {
            case TYPEID_SHORT:
                switch (ACCESSPLAN.getExprElem(*op2Idx)->resultType) {
                    case TYPEID_SHORT:
                        res->data.s = op1.data.s + op2.data.s;
                        res->SetType(OOSQL_TYPE_SHORT);     
                        break;

                    case TYPEID_INT:
                        res->data.i = (Four_Invariable)op1.data.s + op2.data.i;
                        res->SetType(OOSQL_TYPE_INT);       
                        break;

                    case TYPEID_LONG:
                        res->data.l = (Four_Invariable)op1.data.s + op2.data.l;
                        res->SetType(OOSQL_TYPE_LONG);          
                        break;
                        
                    case TYPEID_LONG_LONG:
                        res->data.ll = (Eight_Invariable)op1.data.s + op2.data.ll;
                        res->SetType(OOSQL_TYPE_LONG_LONG); 
                        break;

                    case TYPEID_FLOAT:
                        res->data.f = (float)op1.data.s + op2.data.f;
                        res->SetType(OOSQL_TYPE_FLOAT);     
                        break;

                    case TYPEID_DOUBLE:
                        res->data.d = (double)op1.data.s + op2.data.d;
                        res->SetType(OOSQL_TYPE_DOUBLE);        
                        break;

                    default:
                        OOSQL_ERR( eINCOMPATIBLE_TYPE_OOSQL);
                }

                break;
                
            case TYPEID_INT:

                switch (ACCESSPLAN.getExprElem(*op2Idx)->resultType) {
                    case TYPEID_SHORT:
                        res->data.i = op1.data.i + (Four_Invariable)op2.data.s;
                        res->SetType(OOSQL_TYPE_INT);       
                        break;

                    case TYPEID_INT:
                        res->data.i = op1.data.i + op2.data.i;
                        res->SetType(OOSQL_TYPE_INT);       
                        break;

                    case TYPEID_LONG:
                        res->data.l = (Four_Invariable)op1.data.i + op2.data.l;
                        res->SetType(OOSQL_TYPE_LONG);          
                        break;

                    case TYPEID_LONG_LONG:
                        res->data.ll = (Eight_Invariable)op1.data.i + op2.data.ll;
                        res->SetType(OOSQL_TYPE_LONG_LONG);         
                        break;

                    case TYPEID_FLOAT:
                        res->data.f = (float)op1.data.i + op2.data.f;
                        res->SetType(OOSQL_TYPE_FLOAT);     
                        break;

                    case TYPEID_DOUBLE:
                        res->data.d = (double)op1.data.i + op2.data.d;
                        res->SetType(OOSQL_TYPE_DOUBLE);        
                        break;

                    default:
                        OOSQL_ERR( eINCOMPATIBLE_TYPE_OOSQL);
                }

                break;

            case TYPEID_LONG:

                switch (ACCESSPLAN.getExprElem(*op2Idx)->resultType) {
                    case TYPEID_SHORT:
                        res->data.l = op1.data.l + (Four_Invariable)op2.data.s;
                        res->SetType(OOSQL_TYPE_LONG);          
                        break;

                    case TYPEID_INT:
                        res->data.l = op1.data.l + (Four_Invariable)op2.data.i;
                        res->SetType(OOSQL_TYPE_LONG);          
                        break;

                    case TYPEID_LONG:
                        res->data.l = op1.data.l + op2.data.l;
                        res->SetType(OOSQL_TYPE_LONG);          
                        break;

                    case TYPEID_LONG_LONG:
                        res->data.ll = (Eight_Invariable)op1.data.l + op2.data.ll;
                        res->SetType(OOSQL_TYPE_LONG_LONG);         
                        break;

                    case TYPEID_FLOAT:
                        res->data.f = (float)op1.data.l + op2.data.f;
                        res->SetType(OOSQL_TYPE_FLOAT);     
                        break;

                    case TYPEID_DOUBLE:
                        res->data.d = (double)op1.data.l + op2.data.d;
                        res->SetType(OOSQL_TYPE_DOUBLE);        
                        break;

                    default:
                        OOSQL_ERR( eINCOMPATIBLE_TYPE_OOSQL);
                }

                break;

            case TYPEID_LONG_LONG:
                switch (ACCESSPLAN.getExprElem(*op2Idx)->resultType) 
                {
                    case TYPEID_SHORT:
                        res->data.ll = op1.data.ll + (Eight_Invariable)op2.data.s;
                        res->SetType(OOSQL_TYPE_LONG_LONG);         
                        break;

                    case TYPEID_INT:
                        res->data.ll = op1.data.ll + (Eight_Invariable)op2.data.i;
                        res->SetType(OOSQL_TYPE_LONG_LONG);         
                        break;

                    case TYPEID_LONG:
                        res->data.ll = op1.data.ll + (Eight_Invariable)op2.data.l;
                        res->SetType(OOSQL_TYPE_LONG_LONG);         
                        break;

                    case TYPEID_LONG_LONG:
                        res->data.ll = op1.data.ll + op2.data.ll;
                        res->SetType(OOSQL_TYPE_LONG_LONG);         
                        break;

                    case TYPEID_FLOAT:
                        res->data.f = (float)op1.data.ll + op2.data.f;
                        res->SetType(OOSQL_TYPE_FLOAT);     
                        break;

                    case TYPEID_DOUBLE:
                        res->data.d = (double)op1.data.ll + op2.data.d;
                        res->SetType(OOSQL_TYPE_DOUBLE);        
                        break;

                    default:
                        OOSQL_ERR( eINCOMPATIBLE_TYPE_OOSQL);
                }

                break;

            case TYPEID_FLOAT:

                switch (ACCESSPLAN.getExprElem(*op2Idx)->resultType) {
                    case TYPEID_SHORT:
                        res->data.f = op1.data.f + (float)op2.data.s;
                        res->SetType(OOSQL_TYPE_FLOAT);     
                        break;

                    case TYPEID_INT:
                        res->data.f = op1.data.f + (float)op2.data.i;
                        res->SetType(OOSQL_TYPE_FLOAT);     
                        break;

                    case TYPEID_LONG:
                        res->data.f = op1.data.f + (float)op2.data.l;
                        res->SetType(OOSQL_TYPE_FLOAT);     
                        break;

                    case TYPEID_LONG_LONG:
                        res->data.f = op1.data.f + (float)op2.data.ll;
                        res->SetType(OOSQL_TYPE_FLOAT);     
                        break;

                    case TYPEID_FLOAT:
                        res->data.f = op1.data.f + op2.data.f;
                        res->SetType(OOSQL_TYPE_FLOAT);     
                        break;

                    case TYPEID_DOUBLE:
                        res->data.d = (double)op1.data.f + op2.data.d;
                        res->SetType(OOSQL_TYPE_DOUBLE);        
                        break;

                    default:
                        OOSQL_ERR( eINCOMPATIBLE_TYPE_OOSQL);
                }

                break;

            case TYPEID_DOUBLE:

                switch (ACCESSPLAN.getExprElem(*op2Idx)->resultType) {
                    case TYPEID_SHORT:
                        res->data.d = op1.data.d + (double)op2.data.s;
                        res->SetType(OOSQL_TYPE_DOUBLE);        
                        break;

                    case TYPEID_INT:
                        res->data.d = op1.data.d + (double)op2.data.i;
                        res->SetType(OOSQL_TYPE_DOUBLE);        
                        break;

                    case TYPEID_LONG:
                        res->data.d = op1.data.d + (double)op2.data.l;
                        res->SetType(OOSQL_TYPE_DOUBLE);        
                        break;

                    case TYPEID_LONG_LONG:
                        res->data.d = op1.data.d + (double)op2.data.ll;
                        res->SetType(OOSQL_TYPE_DOUBLE);        
                        break;
                        
                    case TYPEID_FLOAT:
                        res->data.d = op1.data.d + (double)op2.data.f;
                        res->SetType(OOSQL_TYPE_DOUBLE);        
                        break;

                    case TYPEID_DOUBLE:
                        res->data.d = (double)op1.data.d + op2.data.d;
                        res->SetType(OOSQL_TYPE_DOUBLE);        
                        break;

                    default:
                        OOSQL_ERR( eINCOMPATIBLE_TYPE_OOSQL);
                }

                break;

            default:
                OOSQL_ERR( eINCOMPATIBLE_TYPE_OOSQL);
        }

        return eNOERROR;
}


inline Four OOSQL_Evaluator::arithSub( 
        AP_ExprPoolIndex   *op1Idx,        // IN: index to the 1st operand
        AP_ExprPoolIndex   *op2Idx,        // IN: index to the 2nd operand
        OOSQL_DB_Value     *res            // IN/OUT: subtraction result
)
/*
    Function:
        Perform arithmetic binary subtraction.

    Side effect:
        'res' is filled with the subtraction result.

    Return value:
        eNOERROR        if no error
        < eNOERROR      error code
*/
{
    OOSQL_DB_Value  op1(pMemoryManager), op2(pMemoryManager);       // the evaluation result for each operand
    Four            e;

    e = evalExpression( op1Idx, &op1 );
    if (e < eNOERROR) {
        OOSQL_ERR(e);
    }
    e = evalExpression( op2Idx, &op2 );
    if (e < eNOERROR) {
        OOSQL_ERR(e);
    }
	
	if(op1.nullFlag || op2.nullFlag)
	{
		res->nullFlag = SM_TRUE;
		return eNOERROR;
	}
	else
	{
		res->nullFlag = SM_FALSE;
	}

    switch (ACCESSPLAN.getExprElem(*op1Idx)->resultType) 
    {
    case TYPEID_SHORT:

        switch (ACCESSPLAN.getExprElem(*op2Idx)->resultType) 
        {
        case TYPEID_SHORT:
            res->data.s = op1.data.s - op2.data.s;
            res->SetType(OOSQL_TYPE_SHORT);     
            break;

        case TYPEID_INT:
            res->data.i = (Four_Invariable)op1.data.s - op2.data.i;
            res->SetType(OOSQL_TYPE_INT);       
            break;

        case TYPEID_LONG:
            res->data.l = (Four_Invariable)op1.data.s - op2.data.l;
            res->SetType(OOSQL_TYPE_LONG);          
            break;

        case TYPEID_LONG_LONG:
            res->data.ll = (Eight_Invariable)op1.data.s - op2.data.ll;
            res->SetType(OOSQL_TYPE_LONG_LONG);         
            break;

        case TYPEID_FLOAT:
            res->data.f = (float)op1.data.s - op2.data.f;
            res->SetType(OOSQL_TYPE_FLOAT);     
            break;

        case TYPEID_DOUBLE:
            res->data.d = (double)op1.data.s - op2.data.d;
            res->SetType(OOSQL_TYPE_DOUBLE);        
            break;

        default:
            OOSQL_ERR( eINCOMPATIBLE_TYPE_OOSQL);
        }

        break;
        
    case TYPEID_INT:

        switch (ACCESSPLAN.getExprElem(*op2Idx)->resultType) 
        {
        case TYPEID_SHORT:
            res->data.i = op1.data.i - (Four_Invariable)op2.data.s;
            res->SetType(OOSQL_TYPE_INT);       
            break;

        case TYPEID_INT:
            res->data.i = op1.data.i - op2.data.i;
            res->SetType(OOSQL_TYPE_INT);       
            break;

        case TYPEID_LONG:
            res->data.l = (Four_Invariable)op1.data.i - op2.data.l;
            res->SetType(OOSQL_TYPE_LONG);          
            break;

        case TYPEID_LONG_LONG:
            res->data.ll = (Eight_Invariable)op1.data.i - op2.data.ll;
            res->SetType(OOSQL_TYPE_LONG_LONG);         
            break;

        case TYPEID_FLOAT:
            res->data.f = (float)op1.data.i - op2.data.f;
            res->SetType(OOSQL_TYPE_FLOAT);     
            break;

        case TYPEID_DOUBLE:
            res->data.d = (double)op1.data.i - op2.data.d;
            res->SetType(OOSQL_TYPE_DOUBLE);        
            break;

        default:
            OOSQL_ERR( eINCOMPATIBLE_TYPE_OOSQL);
        }

        break;

    case TYPEID_LONG:

        switch (ACCESSPLAN.getExprElem(*op2Idx)->resultType) 
        {
        case TYPEID_SHORT:
            res->data.l = op1.data.l - (Four_Invariable)op2.data.s;
            res->SetType(OOSQL_TYPE_LONG);          
            break;

        case TYPEID_INT:
            res->data.l = op1.data.l - (Four_Invariable)op2.data.i;
            res->SetType(OOSQL_TYPE_LONG);          
            break;

        case TYPEID_LONG:
            res->data.l = op1.data.l - op2.data.l;
            res->SetType(OOSQL_TYPE_LONG);          
            break;

        case TYPEID_LONG_LONG:
            res->data.ll = (Eight_Invariable)op1.data.l - op2.data.ll;
            res->SetType(OOSQL_TYPE_LONG_LONG);         
            break;

        case TYPEID_FLOAT:
            res->data.f = (float)op1.data.l - op2.data.f;
            res->SetType(OOSQL_TYPE_FLOAT);     
            break;

        case TYPEID_DOUBLE:
            res->data.d = (double)op1.data.l - op2.data.d;
            res->SetType(OOSQL_TYPE_DOUBLE);        
            break;

        default:
            OOSQL_ERR( eINCOMPATIBLE_TYPE_OOSQL);
        }

        break;

    case TYPEID_LONG_LONG:
        switch (ACCESSPLAN.getExprElem(*op2Idx)->resultType) 
        {
        case TYPEID_SHORT:
            res->data.ll = op1.data.ll - (Eight_Invariable)op2.data.s;
            res->SetType(OOSQL_TYPE_LONG_LONG);         
            break;

        case TYPEID_INT:
            res->data.ll = op1.data.ll - (Eight_Invariable)op2.data.i;
            res->SetType(OOSQL_TYPE_LONG_LONG);         
            break;

        case TYPEID_LONG:
            res->data.ll = op1.data.ll - (Eight_Invariable)op2.data.l;
            res->SetType(OOSQL_TYPE_LONG_LONG);         
            break;

        case TYPEID_LONG_LONG:
            res->data.ll = op1.data.ll - op2.data.ll;
            res->SetType(OOSQL_TYPE_LONG_LONG);         
            break;

        case TYPEID_FLOAT:
            res->data.f = (float)op1.data.ll - op2.data.f;
            res->SetType(OOSQL_TYPE_FLOAT);     
            break;

        case TYPEID_DOUBLE:
            res->data.d = (double)op1.data.ll - op2.data.d;
            res->SetType(OOSQL_TYPE_DOUBLE);        
            break;

        default:
            OOSQL_ERR(eINCOMPATIBLE_TYPE_OOSQL);
        }

        break;

    case TYPEID_FLOAT:

        switch (ACCESSPLAN.getExprElem(*op2Idx)->resultType) 
        {
        case TYPEID_SHORT:
            res->data.f = op1.data.f - (float)op2.data.s;
            res->SetType(OOSQL_TYPE_FLOAT);     
            break;

        case TYPEID_INT:
            res->data.f = op1.data.f - (float)op2.data.i;
            res->SetType(OOSQL_TYPE_FLOAT);     
            break;

        case TYPEID_LONG:
            res->data.f = op1.data.f - (float)op2.data.l;
            res->SetType(OOSQL_TYPE_FLOAT);     
            break;

        case TYPEID_LONG_LONG:
            res->data.f = op1.data.f - (float)op2.data.ll;
            res->SetType(OOSQL_TYPE_FLOAT);     
            break;

        case TYPEID_FLOAT:
            res->data.f = op1.data.f - op2.data.f;
            res->SetType(OOSQL_TYPE_FLOAT);     
            break;

        case TYPEID_DOUBLE:
            res->data.d = (double)op1.data.f - op2.data.d;
            res->SetType(OOSQL_TYPE_DOUBLE);        
            break;

        default:
            OOSQL_ERR( eINCOMPATIBLE_TYPE_OOSQL);
        }

        break;

    case TYPEID_DOUBLE:

        switch (ACCESSPLAN.getExprElem(*op2Idx)->resultType) 
        {
        case TYPEID_SHORT:
            res->data.d = op1.data.d - (double)op2.data.s;
            res->SetType(OOSQL_TYPE_DOUBLE);        
            break;

        case TYPEID_INT:
            res->data.d = op1.data.d - (double)op2.data.i;
            res->SetType(OOSQL_TYPE_DOUBLE);        
            break;

        case TYPEID_LONG:
            res->data.d = op1.data.d - (double)op2.data.l;
            res->SetType(OOSQL_TYPE_DOUBLE);        
            break;

        case TYPEID_LONG_LONG:
            res->data.d = op1.data.d - (double)op2.data.ll;
            res->SetType(OOSQL_TYPE_DOUBLE);        
            break;

        case TYPEID_FLOAT:
            res->data.d = op1.data.d - (double)op2.data.f;
            res->SetType(OOSQL_TYPE_DOUBLE);        
            break;

        case TYPEID_DOUBLE:
            res->data.d = (double)op1.data.d - op2.data.d;
            res->SetType(OOSQL_TYPE_DOUBLE);        
            break;

        default:
            OOSQL_ERR( eINCOMPATIBLE_TYPE_OOSQL);
        }

        break;

    default:
        OOSQL_ERR( eINCOMPATIBLE_TYPE_OOSQL);
    }

    return eNOERROR;
}


inline Four OOSQL_Evaluator::arithMul( 
        AP_ExprPoolIndex   *op1Idx,        // IN: index to the 1st operand
        AP_ExprPoolIndex   *op2Idx,        // IN: index to the 2nd operand
        OOSQL_DB_Value     *res            // IN/OUT: multiplication result
)
/*
    Function:
        Perform arithmetic binary multiplication.

    Side effect:
        'res' is filled with the multiplication result.

    Return value:
        eNOERROR        if no error
        < eNOERROR      error code
*/
{
        OOSQL_DB_Value  op1(pMemoryManager), op2(pMemoryManager);       // the evaluation result for each operand
        Four            e;

        e = evalExpression( op1Idx, &op1 );
        if (e < eNOERROR) {
            OOSQL_ERR(e);
        }
        e = evalExpression( op2Idx, &op2 );
        if (e < eNOERROR) {
            OOSQL_ERR(e);
        }

		if(op1.nullFlag || op2.nullFlag)
		{   
			res->nullFlag = SM_TRUE;
			return eNOERROR;
		}
		else
		{
			res->nullFlag = SM_FALSE;
		}

        switch (ACCESSPLAN.getExprElem(*op1Idx)->resultType) {
            case TYPEID_SHORT:

                switch (ACCESSPLAN.getExprElem(*op2Idx)->resultType) {
                    case TYPEID_SHORT:
                        res->data.s = op1.data.s * op2.data.s;
                        res->SetType(OOSQL_TYPE_SHORT);     
                        break;

                    case TYPEID_INT:
                        res->data.i = (Four_Invariable)op1.data.s * op2.data.i;
                        res->SetType(OOSQL_TYPE_INT);       
                        break;

                    case TYPEID_LONG:
                        res->data.l = (Four_Invariable)op1.data.s * op2.data.l;
                        res->SetType(OOSQL_TYPE_LONG);          
                        break;

                    case TYPEID_LONG_LONG:
                        res->data.ll = (Eight_Invariable)op1.data.s * op2.data.ll;
                        res->SetType(OOSQL_TYPE_LONG_LONG);         
                        break;

                    case TYPEID_FLOAT:
                        res->data.f = (float)op1.data.s * op2.data.f;
                        res->SetType(OOSQL_TYPE_FLOAT);     
                        break;

                    case TYPEID_DOUBLE:
                        res->data.d = (double)op1.data.s * op2.data.d;
                        res->SetType(OOSQL_TYPE_DOUBLE);        
                        break;

                    default:
                        OOSQL_ERR( eINCOMPATIBLE_TYPE_OOSQL);
                }

                break;
                
            case TYPEID_INT:

                switch (ACCESSPLAN.getExprElem(*op2Idx)->resultType) {
                    case TYPEID_SHORT:
                        res->data.i = op1.data.i * (Four_Invariable)op2.data.s;
                        res->SetType(OOSQL_TYPE_INT);       
                        break;

                    case TYPEID_INT:
                        res->data.i = op1.data.i * op2.data.i;
                        res->SetType(OOSQL_TYPE_INT);       
                        break;

                    case TYPEID_LONG:
                        res->data.l = (Four_Invariable)op1.data.i * op2.data.l;
                        res->SetType(OOSQL_TYPE_LONG);          
                        break;

                    case TYPEID_LONG_LONG:
                        res->data.ll = (Eight_Invariable)op1.data.i * op2.data.ll;
                        res->SetType(OOSQL_TYPE_LONG_LONG);         
                        break;

                    case TYPEID_FLOAT:
                        res->data.f = (float)op1.data.i * op2.data.f;
                        res->SetType(OOSQL_TYPE_FLOAT);     
                        break;

                    case TYPEID_DOUBLE:
                        res->data.d = (double)op1.data.i * op2.data.d;
                        res->SetType(OOSQL_TYPE_DOUBLE);        
                        break;

                    default:
                        OOSQL_ERR( eINCOMPATIBLE_TYPE_OOSQL);
                }

                break;

            case TYPEID_LONG:

                switch (ACCESSPLAN.getExprElem(*op2Idx)->resultType) {
                    case TYPEID_SHORT:
                        res->data.l = op1.data.l * (Four_Invariable)op2.data.s;
                        res->SetType(OOSQL_TYPE_LONG);          
                        break;

                    case TYPEID_INT:
                        res->data.l = op1.data.l * (Four_Invariable)op2.data.i;
                        res->SetType(OOSQL_TYPE_LONG);          
                        break;

                    case TYPEID_LONG:
                        res->data.l = op1.data.l * op2.data.l;
                        res->SetType(OOSQL_TYPE_LONG);          
                        break;

                    case TYPEID_LONG_LONG:
                        res->data.ll = (Eight_Invariable)op1.data.l * op2.data.ll;
                        res->SetType(OOSQL_TYPE_LONG);          
                        break;

                    case TYPEID_FLOAT:
                        res->data.f = (float)op1.data.l * op2.data.f;
                        res->SetType(OOSQL_TYPE_FLOAT);     
                        break;

                    case TYPEID_DOUBLE:
                        res->data.d = (double)op1.data.l * op2.data.d;
                        res->SetType(OOSQL_TYPE_DOUBLE);        
                        break;

                    default:
                        OOSQL_ERR( eINCOMPATIBLE_TYPE_OOSQL);
                }

                break;

            case TYPEID_LONG_LONG:
                switch (ACCESSPLAN.getExprElem(*op2Idx)->resultType) 
                {
                    case TYPEID_SHORT:
                        res->data.ll = op1.data.ll * (Eight_Invariable)op2.data.s;
                        res->SetType(OOSQL_TYPE_LONG_LONG);         
                        break;

                    case TYPEID_INT:
                        res->data.ll = op1.data.ll * (Eight_Invariable)op2.data.i;
                        res->SetType(OOSQL_TYPE_LONG_LONG);         
                        break;

                    case TYPEID_LONG:
                        res->data.ll = op1.data.ll * (Eight_Invariable)op2.data.l;
                        res->SetType(OOSQL_TYPE_LONG_LONG);         
                        break;

                    case TYPEID_LONG_LONG:
                        res->data.ll = op1.data.ll * op2.data.ll;
                        res->SetType(OOSQL_TYPE_LONG_LONG);         
                        break;

                    case TYPEID_FLOAT:
                        res->data.f = (float)op1.data.ll * op2.data.f;
                        res->SetType(OOSQL_TYPE_FLOAT);     
                        break;

                    case TYPEID_DOUBLE:
                        res->data.d = (double)op1.data.ll * op2.data.d;
                        res->SetType(OOSQL_TYPE_DOUBLE);        
                        break;

                    default:
                        OOSQL_ERR( eINCOMPATIBLE_TYPE_OOSQL);
                }

                break;

            case TYPEID_FLOAT:

                switch (ACCESSPLAN.getExprElem(*op2Idx)->resultType) {
                    case TYPEID_SHORT:
                        res->data.f = op1.data.f * (float)op2.data.s;
                        res->SetType(OOSQL_TYPE_FLOAT);     
                        break;

                    case TYPEID_INT:
                        res->data.f = op1.data.f * (float)op2.data.i;
                        res->SetType(OOSQL_TYPE_FLOAT);     
                        break;

                    case TYPEID_LONG:
                        res->data.f = op1.data.f * (float)op2.data.l;
                        res->SetType(OOSQL_TYPE_FLOAT);     
                        break;

                    case TYPEID_LONG_LONG:
                        res->data.f = op1.data.f * (float)op2.data.ll;
                        res->SetType(OOSQL_TYPE_FLOAT);     
                        break;

                    case TYPEID_FLOAT:
                        res->data.f = op1.data.f * op2.data.f;
                        res->SetType(OOSQL_TYPE_FLOAT);     
                        break;

                    case TYPEID_DOUBLE:
                        res->data.d = (double)op1.data.f * op2.data.d;
                        res->SetType(OOSQL_TYPE_DOUBLE);        
                        break;

                    default:
                        OOSQL_ERR( eINCOMPATIBLE_TYPE_OOSQL);
                }

                break;

            case TYPEID_DOUBLE:

                switch (ACCESSPLAN.getExprElem(*op2Idx)->resultType) {
                    case TYPEID_SHORT:
                        res->data.d = op1.data.d * (double)op2.data.s;
                        res->SetType(OOSQL_TYPE_DOUBLE);        
                        break;

                    case TYPEID_INT:
                        res->data.d = op1.data.d * (double)op2.data.i;
                        res->SetType(OOSQL_TYPE_DOUBLE);        
                        break;

                    case TYPEID_LONG:
                        res->data.d = op1.data.d * (double)op2.data.l;
                        res->SetType(OOSQL_TYPE_DOUBLE);        
                        break;

                    case TYPEID_LONG_LONG:
                        res->data.d = op1.data.d * (double)op2.data.ll;
                        res->SetType(OOSQL_TYPE_DOUBLE);        
                        break;

                    case TYPEID_FLOAT:
                        res->data.d = op1.data.d * (double)op2.data.f;
                        res->SetType(OOSQL_TYPE_DOUBLE);        
                        break;

                    case TYPEID_DOUBLE:
                        res->data.d = (double)op1.data.d * op2.data.d;
                        res->SetType(OOSQL_TYPE_DOUBLE);        
                        break;

                    default:
                        OOSQL_ERR( eINCOMPATIBLE_TYPE_OOSQL);
                }

                break;

            default:
                OOSQL_ERR( eINCOMPATIBLE_TYPE_OOSQL);
        }

        return eNOERROR;
}


inline Four OOSQL_Evaluator::arithDiv( 
        AP_ExprPoolIndex   *op1Idx,        // IN: index to the 1st operand
        AP_ExprPoolIndex   *op2Idx,        // IN: index to the 2nd operand
        OOSQL_DB_Value     *res            // IN/OUT: division result
)
/*
    Function:
        Perform arithmetic binary division.

    Side effect:
        'res' is filled with the division result.

    Return value:
        eNOERROR        if no error
        < eNOERROR      error code
*/
{
        OOSQL_DB_Value  op1(pMemoryManager), op2(pMemoryManager);       // the evaluation result for each operand
        Four            e;

        e = evalExpression( op1Idx, &op1 );
        if (e < eNOERROR) {
            OOSQL_ERR(e);
        }
        e = evalExpression( op2Idx, &op2 );
        if (e < eNOERROR) {
            OOSQL_ERR(e);
        }

		if(op1.nullFlag || op2.nullFlag)
		{   
			res->nullFlag = SM_TRUE;
			return eNOERROR;
		}
		else
		{
			res->nullFlag = SM_FALSE;
		}

        switch (ACCESSPLAN.getExprElem(*op1Idx)->resultType) {
            case TYPEID_SHORT:

                switch (ACCESSPLAN.getExprElem(*op2Idx)->resultType) {
                    case TYPEID_SHORT:
                        res->data.s = op1.data.s / op2.data.s;
                        res->SetType(OOSQL_TYPE_SHORT);     
                        break;

                    case TYPEID_INT:
                        res->data.i = (Four_Invariable)op1.data.s / op2.data.i;
                        res->SetType(OOSQL_TYPE_INT);       
                        break;

                    case TYPEID_LONG:
                        res->data.l = (Four_Invariable)op1.data.s / op2.data.l;
                        res->SetType(OOSQL_TYPE_LONG);          
                        break;

                    case TYPEID_LONG_LONG:
                        res->data.ll = (Eight_Invariable)op1.data.s / op2.data.ll;
                        res->SetType(OOSQL_TYPE_LONG_LONG);         
                        break;

                    case TYPEID_FLOAT:
                        res->data.f = (float)op1.data.s / op2.data.f;
                        res->SetType(OOSQL_TYPE_FLOAT);     
                        break;

                    case TYPEID_DOUBLE:
                        res->data.d = (double)op1.data.s / op2.data.d;
                        res->SetType(OOSQL_TYPE_DOUBLE);        
                        break;

                    default:
                        OOSQL_ERR( eINCOMPATIBLE_TYPE_OOSQL);
                }

                break;
                
            case TYPEID_INT:

                switch (ACCESSPLAN.getExprElem(*op2Idx)->resultType) {
                    case TYPEID_SHORT:
                        res->data.i = op1.data.i / (Four_Invariable)op2.data.s;
                        res->SetType(OOSQL_TYPE_INT);       
                        break;

                    case TYPEID_INT:
                        res->data.i = op1.data.i / op2.data.i;
                        res->SetType(OOSQL_TYPE_INT);       
                        break;

                    case TYPEID_LONG:
                        res->data.l = (Four_Invariable)op1.data.i / op2.data.l;
                        res->SetType(OOSQL_TYPE_LONG);          
                        break;

                    case TYPEID_LONG_LONG:
                        res->data.ll = (Eight_Invariable)op1.data.i / op2.data.ll;
                        res->SetType(OOSQL_TYPE_LONG_LONG);         
                        break;

                    case TYPEID_FLOAT:
                        res->data.f = (float)op1.data.i / op2.data.f;
                        res->SetType(OOSQL_TYPE_FLOAT);     
                        break;

                    case TYPEID_DOUBLE:
                        res->data.d = (double)op1.data.i / op2.data.d;
                        res->SetType(OOSQL_TYPE_DOUBLE);        
                        break;

                    default:
                        OOSQL_ERR( eINCOMPATIBLE_TYPE_OOSQL);
                }

                break;

            case TYPEID_LONG:

                switch (ACCESSPLAN.getExprElem(*op2Idx)->resultType) {
                    case TYPEID_SHORT:
                        res->data.l = op1.data.l / (Four_Invariable)op2.data.s;
                        res->SetType(OOSQL_TYPE_LONG);          
                        break;

                    case TYPEID_INT:
                        res->data.l = op1.data.l / (Four_Invariable)op2.data.i;
                        res->SetType(OOSQL_TYPE_LONG);          
                        break;

                    case TYPEID_LONG:
                        res->data.l = op1.data.l / op2.data.l;
                        res->SetType(OOSQL_TYPE_LONG);          
                        break;

                    case TYPEID_LONG_LONG:
                        res->data.ll = (Eight_Invariable)op1.data.l / op2.data.ll;
                        res->SetType(OOSQL_TYPE_LONG);          
                        break;

                    case TYPEID_FLOAT:
                        res->data.f = (float)op1.data.l / op2.data.f;
                        res->SetType(OOSQL_TYPE_FLOAT);     
                        break;

                    case TYPEID_DOUBLE:
                        res->data.d = (double)op1.data.l / op2.data.d;
                        res->SetType(OOSQL_TYPE_DOUBLE);        
                        break;

                    default:
                        OOSQL_ERR( eINCOMPATIBLE_TYPE_OOSQL);
                }

                break;

            case TYPEID_LONG_LONG:

                switch (ACCESSPLAN.getExprElem(*op2Idx)->resultType) {
                    case TYPEID_SHORT:
                        res->data.ll = op1.data.ll / (Eight_Invariable)op2.data.s;
                        res->SetType(OOSQL_TYPE_LONG_LONG);         
                        break;

                    case TYPEID_INT:
                        res->data.ll = op1.data.ll / (Eight_Invariable)op2.data.i;
                        res->SetType(OOSQL_TYPE_LONG_LONG);         
                        break;

                    case TYPEID_LONG:
                        res->data.ll = op1.data.ll / (Eight_Invariable)op2.data.l;
                        res->SetType(OOSQL_TYPE_LONG_LONG);         
                        break;

                    case TYPEID_LONG_LONG:
                        res->data.ll = op1.data.ll / op2.data.ll;
                        res->SetType(OOSQL_TYPE_LONG_LONG);         
                        break;

                    case TYPEID_FLOAT:
                        res->data.f = (float)op1.data.ll / op2.data.f;
                        res->SetType(OOSQL_TYPE_FLOAT);     
                        break;

                    case TYPEID_DOUBLE:
                        res->data.d = (double)op1.data.ll / op2.data.d;
                        res->SetType(OOSQL_TYPE_DOUBLE);        
                        break;

                    default:
                        OOSQL_ERR( eINCOMPATIBLE_TYPE_OOSQL);
                }

                break;

            case TYPEID_FLOAT:

                switch (ACCESSPLAN.getExprElem(*op2Idx)->resultType) {
                    case TYPEID_SHORT:
                        res->data.f = op1.data.f / (float)op2.data.s;
                        res->SetType(OOSQL_TYPE_FLOAT);     
                        break;

                    case TYPEID_INT:
                        res->data.f = op1.data.f / (float)op2.data.i;
                        res->SetType(OOSQL_TYPE_FLOAT);     
                        break;

                    case TYPEID_LONG:
                        res->data.f = op1.data.f / (float)op2.data.l;
                        res->SetType(OOSQL_TYPE_FLOAT);     
                        break;

                    case TYPEID_LONG_LONG:
                        res->data.f = op1.data.f / (float)op2.data.ll;
                        res->SetType(OOSQL_TYPE_FLOAT);     
                        break;

                    case TYPEID_FLOAT:
                        res->data.f = op1.data.f / op2.data.f;
                        res->SetType(OOSQL_TYPE_FLOAT);     
                        break;

                    case TYPEID_DOUBLE:
                        res->data.d = (double)op1.data.f / op2.data.d;
                        res->SetType(OOSQL_TYPE_DOUBLE);        
                        break;

                    default:
                        OOSQL_ERR( eINCOMPATIBLE_TYPE_OOSQL);
                }

                break;

            case TYPEID_DOUBLE:

                switch (ACCESSPLAN.getExprElem(*op2Idx)->resultType) {
                    case TYPEID_SHORT:
                        res->data.d = op1.data.d / (double)op2.data.s;
                        res->SetType(OOSQL_TYPE_DOUBLE);        
                        break;

                    case TYPEID_INT:
                        res->data.d = op1.data.d / (double)op2.data.i;
                        res->SetType(OOSQL_TYPE_DOUBLE);        
                        break;

                    case TYPEID_LONG:
                        res->data.d = op1.data.d / (double)op2.data.l;
                        res->SetType(OOSQL_TYPE_DOUBLE);        
                        break;

                    case TYPEID_LONG_LONG:
                        res->data.d = op1.data.d / (double)op2.data.ll;
                        res->SetType(OOSQL_TYPE_DOUBLE);        
                        break;

                    case TYPEID_FLOAT:
                        res->data.d = op1.data.d / (double)op2.data.f;
                        res->SetType(OOSQL_TYPE_DOUBLE);        
                        break;

                    case TYPEID_DOUBLE:
                        res->data.d = (double)op1.data.d / op2.data.d;
                        res->SetType(OOSQL_TYPE_DOUBLE);        
                        break;

                    default:
                        OOSQL_ERR( eINCOMPATIBLE_TYPE_OOSQL);
                }

                break;

            default:
                OOSQL_ERR( eINCOMPATIBLE_TYPE_OOSQL);
        }

        return eNOERROR;
}


inline Four OOSQL_Evaluator::arithMod( 
        AP_ExprPoolIndex   *op1Idx,        // IN: index to the 1st operand
        AP_ExprPoolIndex   *op2Idx,        // IN: index to the 2nd operand
        OOSQL_DB_Value     *res            // IN/OUT: modulo result
)
/*
    Function:
        Perform arithmetic binary modulo.

    Side effect:
        'res' is filled with the modulo result.

    Return value:
        eNOERROR        if no error
        < eNOERROR      error code
*/
{
        OOSQL_DB_Value  op1(pMemoryManager), op2(pMemoryManager);       // the evaluation result for each operand
        Four            e;              // error code

        e = evalExpression( op1Idx, &op1 );
        if (e < eNOERROR) OOSQL_ERR(e);

        e = evalExpression( op2Idx, &op2 );
        if (e < eNOERROR) OOSQL_ERR(e);

		if(op1.nullFlag || op2.nullFlag)
		{   
			res->nullFlag = SM_TRUE;
			return eNOERROR;
		}
		else
		{
			res->nullFlag = SM_FALSE;
		}

        switch (ACCESSPLAN.getExprElem(*op1Idx)->resultType) {
            case TYPEID_SHORT:

                switch (ACCESSPLAN.getExprElem(*op2Idx)->resultType) {
                    case TYPEID_SHORT:
                        res->data.s = op1.data.s % op2.data.s;
                        res->SetType(OOSQL_TYPE_SHORT);     
                        break;

                    case TYPEID_INT:
                        res->data.i = (Four_Invariable)op1.data.s % op2.data.i;
                        res->SetType(OOSQL_TYPE_INT);       
                        break;

                    case TYPEID_LONG:
                        res->data.l = (Four_Invariable)op1.data.s % op2.data.l;
                        res->SetType(OOSQL_TYPE_LONG);          
                        break;

                    case TYPEID_LONG_LONG:
                        res->data.ll = (Eight_Invariable)op1.data.s % op2.data.ll;
                        res->SetType(OOSQL_TYPE_LONG_LONG);         
                        break;

                    default:
                        OOSQL_ERR( eINCOMPATIBLE_TYPE_OOSQL);
                }

                break;
                
            case TYPEID_INT:

                switch (ACCESSPLAN.getExprElem(*op2Idx)->resultType) {
                    case TYPEID_SHORT:
                        res->data.i = op1.data.i % (Four_Invariable)op2.data.s;
                        res->SetType(OOSQL_TYPE_INT);       
                        break;

                    case TYPEID_INT:
                        res->data.i = op1.data.i % op2.data.i;
                        res->SetType(OOSQL_TYPE_INT);       
                        break;

                    case TYPEID_LONG:
                        res->data.l = (Four_Invariable)op1.data.i % op2.data.l;
                        res->SetType(OOSQL_TYPE_LONG);          
                        break;

                    case TYPEID_LONG_LONG:
                        res->data.ll = (Eight_Invariable)op1.data.i % op2.data.ll;
                        res->SetType(OOSQL_TYPE_LONG_LONG);         
                        break;

                    default:
                        OOSQL_ERR( eINCOMPATIBLE_TYPE_OOSQL);
                }

                break;

            case TYPEID_LONG:

                switch (ACCESSPLAN.getExprElem(*op2Idx)->resultType) {
                    case TYPEID_SHORT:
                        res->data.l = op1.data.l % (Four_Invariable)op2.data.s;
                        res->SetType(OOSQL_TYPE_LONG);          
                        break;

                    case TYPEID_INT:
                        res->data.l = op1.data.l % (Four_Invariable)op2.data.i;
                        res->SetType(OOSQL_TYPE_LONG);          
                        break;

                    case TYPEID_LONG:
                        res->data.l = op1.data.l % op2.data.l;
                        res->SetType(OOSQL_TYPE_LONG);          
                        break;

                    case TYPEID_LONG_LONG:
                        res->data.ll = (Eight_Invariable)op1.data.l % op2.data.ll;
                        res->SetType(OOSQL_TYPE_LONG_LONG);          
                        break;

                    default:
                        OOSQL_ERR( eINCOMPATIBLE_TYPE_OOSQL);
                }

                break;

            case TYPEID_LONG_LONG:

                switch (ACCESSPLAN.getExprElem(*op2Idx)->resultType) {
                    case TYPEID_SHORT:
                        res->data.ll = op1.data.ll % (Eight_Invariable)op2.data.s;
                        res->SetType(OOSQL_TYPE_LONG_LONG);         
                        break;

                    case TYPEID_INT:
                        res->data.ll = op1.data.ll % (Eight_Invariable)op2.data.i;
                        res->SetType(OOSQL_TYPE_LONG_LONG);         
                        break;

                    case TYPEID_LONG:
                        res->data.ll = op1.data.ll % (Eight_Invariable)op2.data.l;
                        res->SetType(OOSQL_TYPE_LONG_LONG);         
                        break;

                    case TYPEID_LONG_LONG:
                        res->data.ll = op1.data.ll % op2.data.ll;
                        res->SetType(OOSQL_TYPE_LONG_LONG);         
                        break;

                    default:
                        OOSQL_ERR(eINCOMPATIBLE_TYPE_OOSQL);
                }

                break;

            case TYPEID_FLOAT:

            case TYPEID_DOUBLE:

            default:
                OOSQL_ERR( eINCOMPATIBLE_TYPE_OOSQL);
        }

        return eNOERROR;
}


inline Four OOSQL_Evaluator::arithUnMin( 
        AP_ExprPoolIndex   *op1Idx,        // IN: index to the operand
        OOSQL_DB_Value     *res            // IN/OUT: unary minus result
)
/*
    Function:
        Perform arithmetic unary minus division.

    Side effect:
        'res' is filled with the unary minus result.

    Return value:
        eNOERROR        if no error
        < eNOERROR      error code
*/
{
        OOSQL_DB_Value  op1(pMemoryManager);    // the evaluation result for the operand
        Four            e;

        e = evalExpression( op1Idx, &op1 );
        if (e < eNOERROR) OOSQL_ERR(e);

		if(op1.nullFlag)
		{   
			res->nullFlag = SM_TRUE;
			return eNOERROR;
		}
		else
		{
			res->nullFlag = SM_FALSE;
		}

        switch (ACCESSPLAN.getExprElem(*op1Idx)->resultType) {
            case TYPEID_SHORT:
                res->data.s = - op1.data.s;                 
                res->SetType(OOSQL_TYPE_SHORT);     
                break;

            case TYPEID_INT:
                res->data.i = - op1.data.i;                 
                res->SetType(OOSQL_TYPE_INT);           
                break;

            case TYPEID_LONG:
                res->data.l = - op1.data.l;                 
                res->SetType(OOSQL_TYPE_LONG);          
                break;

            case TYPEID_LONG_LONG:
                res->data.ll = - op1.data.ll;                   
                res->SetType(OOSQL_TYPE_LONG_LONG);         
                break;

            case TYPEID_FLOAT:
                res->data.f = - op1.data.f;                 
                res->SetType(OOSQL_TYPE_FLOAT);     
                break;

            case TYPEID_DOUBLE:
                res->data.d = - op1.data.d;                 
                res->SetType(OOSQL_TYPE_DOUBLE);        
                break;

            default:
                OOSQL_ERR( eINCOMPATIBLE_TYPE_OOSQL);
        }

        return eNOERROR;
}


inline Four OOSQL_Evaluator::arithAbs( 
        AP_ExprPoolIndex   *op1Idx,        // IN: index to the operand
        OOSQL_DB_Value     *res            // IN/OUT: abstraction result
)
/*
    Function:
        Perform arithmetic unary abstraction.

    Side effect:
        'res' is filled with the unary abstraction.

    Return value:
        eNOERROR        if no error
        < eNOERROR      error code
*/
{
        OOSQL_DB_Value  op1(pMemoryManager);    // the evaluation result for the operand
        Four            e;

        e = evalExpression( op1Idx, &op1 );
        if (e < eNOERROR) OOSQL_ERR(e);

		if(op1.nullFlag)
		{   
			res->nullFlag = SM_TRUE;
			return eNOERROR;
		}
		else
		{
			res->nullFlag = SM_FALSE;
		}

        switch (ACCESSPLAN.getExprElem(*op1Idx)->resultType) {
			case TYPEID_SHORT: 
				if (op1.data.s < 0)         
					res->data.s = - op1.data.s;
				else    res->data.s = op1.data.s;
				
				res->SetType(OOSQL_TYPE_SHORT);
				break;
				
			case TYPEID_INT:
				if (op1.data.i < 0) 
					res->data.i = - op1.data.i;
				else    res->data.i = op1.data.i;
				
				res->SetType(OOSQL_TYPE_INT);
				break;
				
			case TYPEID_LONG:
				if (op1.data.l < 0)            
					res->data.l = - op1.data.l;
				else    res->data.l = op1.data.l;

				res->SetType(OOSQL_TYPE_LONG);
				break;																																																				            
			case TYPEID_LONG_LONG:
				if (op1.data.ll < 0)
					res->data.ll = - op1.data.ll;
				else res->data.ll = op1.data.ll;
				
				res->SetType(OOSQL_TYPE_LONG_LONG);
				break;

			case TYPEID_FLOAT: 
				if (op1.data.f < 0) 
					res->data.f = - op1.data.f;
				else    res->data.f = op1.data.f;
				
				res->SetType(OOSQL_TYPE_FLOAT);
				break;
				
			case TYPEID_DOUBLE:
				if (op1.data.d < 0)            
					res->data.d = - op1.data.d;
				else    res->data.d = op1.data.d;
				
				res->SetType(OOSQL_TYPE_DOUBLE);
				break;
            default:
                OOSQL_ERR( eINCOMPATIBLE_TYPE_OOSQL);
        }

        return eNOERROR;
}

inline Four OOSQL_Evaluator::stringCat(
        AP_ExprPoolIndex   *str1Idx,       // IN: index to the 1st string
        AP_ExprPoolIndex   *str2Idx,       // IN: index to the 2nd string
        OOSQL_DB_Value     *res            // IN/OUT: string concatenation result
)
/*
    Function:
        Concatenating two strings.

    Side effect:
        Memory is allocated for the result string.

    Return value:
        eNOERROR        if no error
        < eNOERROR
*/
{
        OOSQL_DB_Value  op1(pMemoryManager), op2(pMemoryManager);       // evaluation result for two operands
        Four            e;              // return code

        OOSQL_ERR(eNOTIMPLEMENTED_OOSQL);
}


inline Four OOSQL_Evaluator::evalOpLike(
        AP_ExprPoolIndex   *op1Idx,
        AP_ExprPoolIndex   *op2Idx,
        OOSQL_DB_Value     *res
)
/*
    Function:

    Side effect:

    Return value:
*/
{
        OOSQL_DB_Value  op1(pMemoryManager), op2(pMemoryManager);
        Four            e;
        Four            op1Length, op2Length;

        e = evalExpression( op1Idx, &op1 );
        if (e < eNOERROR) OOSQL_ERR(e);

        e = evalExpression( op2Idx, &op2 );
        if (e < eNOERROR) OOSQL_ERR(e);

		if(op1.nullFlag || op2.nullFlag)
		{   
			res->nullFlag = SM_TRUE;
			return eNOERROR;
		}
		else
		{
			res->nullFlag = SM_FALSE;
		}

        // make null terminated string
        op1Length = op1.length;
        op2Length = op2.length;
        op1.PrepareData(op1Length + 1);
        op2.PrepareData(op2Length + 1);
        ((char*)op1.data.ptr)[op1Length] = 0;
        ((char*)op2.data.ptr)[op2Length] = 0;

        e = OQL_EvalOpLike((char*)op1.data.ptr, 
                           (char*)op2.data.ptr);
        if (e == SM_TRUE)
            res->data.i = SM_TRUE;
        else
            res->data.i = SM_FALSE;
        res->SetType(OOSQL_TYPE_INT);       

        return eNOERROR;
}


inline Four OOSQL_Evaluator::arithCmp(
        AP_ExprPoolIndex   *op1Idx,        // IN: index to the 1st operand
        AP_ExprPoolIndex   *op2Idx         // IN: index to the 2nd operand
)
/*
    Function:
        Compare two arithmetic values.

    Side effect:

    Return value:
        CMP_GT          if the 1st operand is greater than the 2nd one.
        CMP_LT          if the 1st operand is less than the 2nd one.
        CMP_EQ          if the 1st operand is equal to the 2nd one.
		CMP_UN			if either 1st operand or 2nd one is Null.
        < eNOERROR      error code
*/
{
        OOSQL_DB_Value        res(pMemoryManager);            // the evaluation result
        OOSQL_DB_Value        op1(pMemoryManager), op2(pMemoryManager);       // the evaluation result for each operand
        Four                  e;

        e = evalExpression( op1Idx, &op1 );
        if (e < eNOERROR) OOSQL_ERR(e);

        e = evalExpression( op2Idx, &op2 );
        if (e < eNOERROR) OOSQL_ERR(e);

		if(op1.nullFlag || op2.nullFlag)
		{   
			return CMP_UN;
		}			 

        switch (ACCESSPLAN.getExprElem(*op1Idx)->resultType) {
            case TYPEID_SHORT:

                switch (ACCESSPLAN.getExprElem(*op2Idx)->resultType) {
                    case TYPEID_SHORT:
                        res.data.s = op1.data.s - op2.data.s;
                        return diffRes2CmpRes( &res, TYPEID_SHORT);

                    case TYPEID_INT:
                        res.data.i = (Four_Invariable)op1.data.s - op2.data.i;
                        return diffRes2CmpRes( &res, TYPEID_INT);

                    case TYPEID_LONG:
                        res.data.l = (Four_Invariable)op1.data.s - op2.data.l;
                        return diffRes2CmpRes( &res, TYPEID_LONG);

                    case TYPEID_LONG_LONG:
                        res.data.ll = (Eight_Invariable)op1.data.s - op2.data.ll;
                        return diffRes2CmpRes( &res, TYPEID_LONG_LONG);

                    case TYPEID_FLOAT:
                        res.data.f = (float)op1.data.s - op2.data.f;
                        return diffRes2CmpRes( &res, TYPEID_FLOAT);

                    case TYPEID_DOUBLE:
                        res.data.d = (double)op1.data.s - op2.data.d;
                        return diffRes2CmpRes( &res, TYPEID_DOUBLE);

                    default:
                        OOSQL_ERR( eINCOMPATIBLE_TYPE_OOSQL);
                }
                break;
                
            case TYPEID_INT:

                switch (ACCESSPLAN.getExprElem(*op2Idx)->resultType) {
                    case TYPEID_SHORT:
                        res.data.i = op1.data.i - (Four_Invariable)op2.data.s;
                        return diffRes2CmpRes( &res, TYPEID_INT);

                    case TYPEID_INT:
                        res.data.i = op1.data.i - op2.data.i;
                        return diffRes2CmpRes( &res, TYPEID_INT);

                    case TYPEID_LONG:
                        res.data.l = (Four_Invariable)op1.data.i - op2.data.l;
                        return diffRes2CmpRes( &res, TYPEID_LONG);

                    case TYPEID_LONG_LONG:
                        res.data.ll = (Eight_Invariable)op1.data.i - op2.data.ll;
                        return diffRes2CmpRes( &res, TYPEID_LONG_LONG);

                    case TYPEID_FLOAT:
                        res.data.f = (float)op1.data.i - op2.data.f;
                        return diffRes2CmpRes( &res, TYPEID_FLOAT);

                    case TYPEID_DOUBLE:
                        res.data.d = (double)op1.data.i - op2.data.d;
                        return diffRes2CmpRes( &res, TYPEID_DOUBLE);

                    default:
                        OOSQL_ERR( eINCOMPATIBLE_TYPE_OOSQL);
                }
                break;

            case TYPEID_LONG:
                switch (ACCESSPLAN.getExprElem(*op2Idx)->resultType) {
                    case TYPEID_SHORT:
                        res.data.l = op1.data.l - (Four_Invariable)op2.data.s;
                        return diffRes2CmpRes( &res, TYPEID_LONG);

                    case TYPEID_INT:
                        res.data.l = op1.data.l - (Four_Invariable)op2.data.i;
                        return diffRes2CmpRes( &res, TYPEID_LONG);

                    case TYPEID_LONG:
                        res.data.l = op1.data.l - op2.data.l;
                        return diffRes2CmpRes( &res, TYPEID_LONG);

                    case TYPEID_LONG_LONG:
                        res.data.ll = (Eight_Invariable)op1.data.l - op2.data.ll;
                        return diffRes2CmpRes( &res, TYPEID_LONG_LONG);

                    case TYPEID_FLOAT:
                        res.data.f = (float)op1.data.l - op2.data.f;
                        return diffRes2CmpRes( &res, TYPEID_FLOAT);

                    case TYPEID_DOUBLE:
                        res.data.d = (double)op1.data.l - op2.data.d;
                        return diffRes2CmpRes( &res, TYPEID_DOUBLE);

                    default:
                        OOSQL_ERR( eINCOMPATIBLE_TYPE_OOSQL);
                }
                break;
            
            case TYPEID_LONG_LONG:
                switch (ACCESSPLAN.getExprElem(*op2Idx)->resultType) {
                    case TYPEID_SHORT:
                        res.data.ll = op1.data.ll - (Eight_Invariable)op2.data.s;
                        return diffRes2CmpRes(&res, TYPEID_LONG_LONG);

                    case TYPEID_INT:
                        res.data.ll = op1.data.ll - (Eight_Invariable)op2.data.i;
                        return diffRes2CmpRes(&res, TYPEID_LONG_LONG);

                    case TYPEID_LONG:
                        res.data.ll = op1.data.ll - (Eight_Invariable)op2.data.l;
                        return diffRes2CmpRes(&res, TYPEID_LONG_LONG);

                    case TYPEID_LONG_LONG:
                        res.data.ll = op1.data.ll - op2.data.ll;
                        return diffRes2CmpRes(&res, TYPEID_LONG_LONG);

                    case TYPEID_FLOAT:
                        res.data.f = (float)op1.data.ll - op2.data.f;
                        return diffRes2CmpRes(&res, TYPEID_FLOAT);

                    case TYPEID_DOUBLE:
                        res.data.d = (double)op1.data.ll - op2.data.d;
                        return diffRes2CmpRes(&res, TYPEID_DOUBLE);

                    default:
                        OOSQL_ERR( eINCOMPATIBLE_TYPE_OOSQL);
                }
                break;

            case TYPEID_FLOAT:

                switch (ACCESSPLAN.getExprElem(*op2Idx)->resultType) {
                    case TYPEID_SHORT:
                        res.data.f = op1.data.f - (float)op2.data.s;
                        return diffRes2CmpRes( &res, TYPEID_FLOAT);

                    case TYPEID_INT:
                        res.data.f = op1.data.f - (float)op2.data.i;
                        return diffRes2CmpRes( &res, TYPEID_FLOAT);

                    case TYPEID_LONG:
                        res.data.f = op1.data.f - (float)op2.data.l;
                        return diffRes2CmpRes( &res, TYPEID_FLOAT);

                    case TYPEID_LONG_LONG:
                        res.data.f = op1.data.f - (float)op2.data.ll;
                        return diffRes2CmpRes( &res, TYPEID_FLOAT);

                    case TYPEID_FLOAT:
                        res.data.f = op1.data.f - op2.data.f;
                        return diffRes2CmpRes( &res, TYPEID_FLOAT);

                    case TYPEID_DOUBLE:
                        res.data.d = (double)op1.data.f - op2.data.d;
                        return diffRes2CmpRes( &res, TYPEID_DOUBLE);

                    default:
                        OOSQL_ERR( eINCOMPATIBLE_TYPE_OOSQL);
                }
                break;

            case TYPEID_DOUBLE:

                switch (ACCESSPLAN.getExprElem(*op2Idx)->resultType) {
                    case TYPEID_SHORT:
                        res.data.d = op1.data.d - (double)op2.data.s;
                        return diffRes2CmpRes( &res, TYPEID_DOUBLE);

                    case TYPEID_INT:
                        res.data.d = op1.data.d - (double)op2.data.i;
                        return diffRes2CmpRes( &res, TYPEID_DOUBLE);

                    case TYPEID_LONG_LONG:
                        res.data.d = op1.data.d - (double)op2.data.ll;
                        return diffRes2CmpRes( &res, TYPEID_DOUBLE);

                    case TYPEID_FLOAT:
                        res.data.d = op1.data.d - (double)op2.data.f;
                        return diffRes2CmpRes( &res, TYPEID_DOUBLE);

                    case TYPEID_DOUBLE:
                        res.data.d = (double)op1.data.d - op2.data.d;
                        return diffRes2CmpRes( &res, TYPEID_DOUBLE);

                    default:
                        OOSQL_ERR( eINCOMPATIBLE_TYPE_OOSQL);
                }
                break;

            case TYPEID_DATE:
              e = m_storageManager->CompareDate(&op1.data.date, &op2.data.date);
              if(e > 0)
                  return CMP_GT;
              else if(e < 0)
                  return CMP_LT;
              else
                  return CMP_EQ;
              break;

            case TYPEID_TIME:
              e = m_storageManager->CompareTime(&op1.data.time, &op2.data.time);
              if(e > 0)
                  return CMP_GT;
              else if(e < 0)
                  return CMP_LT;
              else
                  return CMP_EQ;
              break;    

            case TYPEID_TIMESTAMP:
              e = m_storageManager->CompareTimestamp(&op1.data.timestamp, &op2.data.timestamp);
              if(e > 0)
                  return CMP_GT;
              else if(e < 0)
                  return CMP_LT;
              else
                  return CMP_EQ;
              break;    
              
            case TYPEID_INTERVAL:
              OOSQL_ERR(eNOTIMPLEMENTED_OOSQL);
              break;    

            case TYPEID_BOOL:
                if(op1.data.i == op2.data.i)
                    return CMP_EQ;
                else
                    return CMP_NE;
                break;

            case TYPEID_OID:
                if(op2.type == TYPEID_STRING && 
                   op2.length == sizeof(OOSQL_StorageManager::OID) * 2)
                {
                    OOSQL_StorageManager::OID oid;

                    e = oosql_ConvertFromOIDStringToOIDStructure((char*)op2.data.ptr, &oid);
                    OOSQL_CHECK_ERR(e);

                    if(!memcmp(&op1.data.oid, &oid, sizeof(oid)))
                        return CMP_EQ;
                    else
                        return CMP_NE;
                }
                else if(op2.type == TYPEID_OID)
                {
                    if(!memcmp(&op1.data.oid, &op2.data.oid, sizeof(OOSQL_StorageManager::OID)))
                        return CMP_EQ;
                    else
                        return CMP_NE;
                }
                else
                    return CMP_NE;
                break;

            default:
                OOSQL_ERR( eINCOMPATIBLE_TYPE_OOSQL);
        }

        return eNOERROR;
}


inline Four OOSQL_Evaluator::diffRes2CmpRes(
        OOSQL_DB_Value  *diffRes,
        Four            resultType
)
/*
    Function:
        Return the comparison result based on the result of arithmetic difference.

    Side effect:

    Return value:
        CMP_GT          if the 1st operand is greater than the 2nd one.
        CMP_LT          if the 1st operand is less than the 2nd one.
        CMP_EQ          if the 1st operand is equal to the 2nd one.
        < eNOERROR      error code
*/
{
        switch (resultType) {
            case TYPEID_SHORT:
                if (diffRes->data.s > 0)
                        return CMP_GT;
                else if (diffRes->data.s < 0)
                        return CMP_LT;
                else
                        return CMP_EQ;

            case TYPEID_INT:
                if (diffRes->data.i > 0)
                        return CMP_GT;
                else if (diffRes->data.i < 0)
                        return CMP_LT;
                else
                        return CMP_EQ;

            case TYPEID_LONG:
                if (diffRes->data.l > 0)
                        return CMP_GT;
                else if (diffRes->data.l < 0)
                        return CMP_LT;
                else
                        return CMP_EQ;

            case TYPEID_LONG_LONG:
                if (diffRes->data.ll > 0)
                        return CMP_GT;
                else if (diffRes->data.ll < 0)
                        return CMP_LT;
                else
                        return CMP_EQ;

            case TYPEID_FLOAT:
                if (diffRes->data.f > 0)
                        return CMP_GT;
                else if (diffRes->data.f < 0)
                        return CMP_LT;
                else
                        return CMP_EQ;

            case TYPEID_DOUBLE:
                if (diffRes->data.d > 0)
                        return CMP_GT;
                else if (diffRes->data.d < 0)
                        return CMP_LT;
                else
                        return CMP_EQ;

            default:
                OOSQL_ERR( eILLEGAL_OP_OOSQL);
        }
}


inline Four OOSQL_Evaluator::collectionCmp(
        AP_ExprPoolIndex   *cltn1,         // IN: index to the 1st collection
        AP_ExprPoolIndex   *cltn2          // IN: index to the 2nd collection
)
/*
    Function:
        Compares two collection objects (or constants).

    Side effect:

    Return value:
*/
{
        return eNOERROR;
}


inline Four OOSQL_Evaluator::stringCmp(
        AP_ExprPoolIndex   *str1Idx,       // IN: index to the 1st string
        AP_ExprPoolIndex   *str2Idx        // IN: index to the 2nd string
)
/*
    Function:

    Side effect:

    Return value:
*/
{
        OOSQL_DB_Value      op1(pMemoryManager), op2(pMemoryManager);       // the evaluation result for each operand
        unsigned char       *ch1;   // ptr. to 1st string
        unsigned char       *ch2;   // ptr. to 2nd string
        Four                ch1length;
        Four                ch2length;
        Four                e;
        Four                i;
        Four                j;
        Four                k;

        e = evalExpression( str1Idx, &op1 );
        if (e < eNOERROR) OOSQL_ERR(e);

        e = evalExpression( str2Idx, &op2 );
        if (e < eNOERROR) OOSQL_ERR(e);

		if(op1.nullFlag || op2.nullFlag)                    
		{                                   
			return CMP_UN;                                          
		}                           

        ch1 = (unsigned char*)op1.data.ptr;
        ch2 = (unsigned char*)op2.data.ptr;
        ch1length = op1.length;
        ch2length = op2.length;

        for (i = 0; i < ch1length && i < ch2length; ch1++, ch2++, i++) {
            if (*ch1 > *ch2) {
                return CMP_GT;
            }
            else if (*ch1 < *ch2) {
                return CMP_LT;
            }
        }

        for(j = i; j < ch1length; j++, ch1++)
            if(*ch1 != 0) break;
        for(k = i; k < ch2length; k++, ch2++)
            if(*ch2 != 0) break;
        if(j == ch1length && k == ch2length)
            return CMP_EQ;

        if(ch1length == ch2length) 
        {
            return CMP_EQ;
        }
        else if (ch1length < ch2length) 
        {
            return CMP_LT;
        }
        else 
        {
            return CMP_GT;
        }
} 

inline Four OOSQL_Evaluator::nullCmp(
		AP_ExprPoolIndex   *op1Idx        // IN: index to the 1st operand
)
/*
   Function:
   
   Side effect:
   
   Return value:
*/
{
	OOSQL_DB_Value      op1(pMemoryManager); // the evaluation result for operand
	Four                e;
	
	e = evalExpression( op1Idx, &op1 );
	if (e < eNOERROR) OOSQL_ERR(e);
	
	if(op1.nullFlag)
		return CMP_EQ;
	else
		return CMP_NE;
}


inline Four OOSQL_Evaluator::oidCmp( 
        AP_ExprPoolIndex   *oidIdx1,       // IN: index to the 1st OID
        AP_ExprPoolIndex   *oidIdx2        // IN: index to the 2nd OID
)
/*
    Function:

    Side effect:

    Return value:
*/
{
        return eNOERROR;
}


inline Four OOSQL_Evaluator::objectCmp( 
        AP_ExprPoolIndex   *objIdx1,       // IN: index to the 1st object (dereferenced object)
        AP_ExprPoolIndex   *objIdx2        // IN: index to the 2nd object (dereferenced object)
)
/*
    Function:

    Side effect:

    Return value:
*/
{
        return eNOERROR;
}


inline Four OOSQL_Evaluator::compareColListStruct(
        Four                                    colType,
        OOSQL_StorageManager::ColListStruct*    clist1,
        OOSQL_StorageManager::ColListStruct*    clist2
)
/*
    Function:

    Side effect:

    Return value:
*/
{
    double  diff;   /* result of difference:
                     *   1) > 0 if clist1 > clist2
                     *   2) < 0 if clist1 < clist2
                     *   3) = 0 if clist1 = clist2
                     */
	if (clist1->nullFlag || clist2->nullFlag)
		return CMP_UN;

    switch (colType) 
    {
    case OOSQL_TYPE_short:
        diff = clist1->data.s - clist2->data.s;
        break;

    case OOSQL_TYPE_int:
        diff = clist1->data.i - clist2->data.i;
        break;

    case OOSQL_TYPE_long:
        diff = clist1->data.l - clist2->data.l;
        break;

    case OOSQL_TYPE_long_long:
        diff = clist1->data.ll - clist2->data.ll;
        break;

    case OOSQL_TYPE_float:
        diff = clist1->data.f - clist2->data.f;
        break;

    case OOSQL_TYPE_double:
        diff = clist1->data.d - clist2->data.d;
        break;

    case OOSQL_TYPE_string:
    case OOSQL_TYPE_varstring:
        diff = compareStringColumn( clist1, clist2 );
        break;

	case OOSQL_TYPE_DATE:
		diff = clist1->data.date - clist2->data.date;
		break;

    case OOSQL_TYPE_OID:
        if ( OOSQL_StorageManager::OIDGT(clist1->data.oid, clist2->data.oid) == SM_TRUE ) {
            return CMP_GT;
        }
        else if ( OOSQL_StorageManager::OIDLT(clist1->data.oid, clist2->data.oid) == SM_TRUE ) {
            return CMP_LT;
        }
        else {
            return CMP_EQ;
        }

    default:
        OOSQL_ERR( eINVALID_COLTYPE_OOSQL);
    }

    if (diff > 0) {
        return CMP_GT;
    }
    else if (diff < 0) {
        return CMP_LT;
    }
    else {
        return CMP_EQ;
    }
}


inline Four OOSQL_Evaluator::compareStringColumn(
        OOSQL_StorageManager::ColListStruct   *clist1,
        OOSQL_StorageManager::ColListStruct   *clist2
)
/*
    Function:

    Side effect:

    Return value:
*/
{
    Four    diff;

    if (clist1->retLength < clist2->retLength) {
        diff = memcmp( clist1->data.ptr, clist2->data.ptr, (int)clist1->retLength );
        if (diff == 0) 
            diff = -1;  // negative value indicating (clist1->data < clist2->data)
    }
    else if (clist1->retLength > clist2->retLength) {
        diff = memcmp( clist1->data.ptr, clist2->data.ptr, (int)clist2->retLength );
        if (diff == 0) {
            diff = 1;   // positive value indicating (clist1->data > clist2->data)
        }
    }
    else {  /* clist1->retLength == clist2->length */
        diff = memcmp( clist1->data.ptr, clist2->data.ptr, (int)clist1->retLength );
    }   

    return diff;
}



#endif 
