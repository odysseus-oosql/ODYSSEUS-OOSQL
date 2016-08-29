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
        oosql_Eval_Aggregation.cxx

    DESCRIPTION:
        This module implements member functions of OOSQL_Evaluator
        that are related to aggregation processing.

    IMPORTS:
        Four    OOSQL_Evaluator::copyArgumentToColListStruct()

    EXPORTS:
        Four    OOSQL_Evaluator::aggregation()
*/


/*
 * include header files
 */
#include "OOSQL_Evaluator.hxx"
#include "oosql_Eval_Expr.hxx"
#include "OOSQL_SortStream.hxx"

Four    OOSQL_Evaluator::aggregation()
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
    EVAL_EvalBufferSlot *aggrFuncResult;
    AP_AggrFuncPoolElements aggrFuncList;
    Four aggrFuncResultType;
    AP_ArgumentElement *argument;
    Four argTempFileNum;
    Four argColNo;
    Four cmpResult;
    Four i;
    Four e;

#ifdef  OOSQL_DEBUG
    /* assert that aggregation is necessary for the current plan element */
    if (IS_NULL_POOLINDEX(ACCESSPLANELEMENTS[m_currPlanIndex].aggrFuncInfo)) {
        OOSQL_ERR(eINTERNAL_INCORRECTEXECSEQUENCE_OOSQL);
    }
#endif

    switch (m_evalStatus.aggregationStatus) {
        case EVALSTATUS_INIT:   /* initial processing of aggregation */

            /* get the aggregate function list for the current plan element */
            aggrFuncList = ACCESSPLAN.getAggrFuncPool(m_currPlanIndex);

            /* get pointer to evaluation buffer for aggregation */
            aggrFuncResult = EVAL_EVALBUFFER[m_currPlanIndex].getAggrFuncResSlotPtr();

            for (i = 0; i < GET_POOLSIZE(aggrFuncList); i++) {

                if (aggrFuncList[i].distinctFlag == SM_TRUE) {
                    /* clear OID initialize aggregate function argument buffer */
                    argTempFileNum = (ACCESSPLAN.getArgumentElem(aggrFuncList[i].argument))->tempFileCol.tempFileNum;
                    OOSQL_StorageManager::OIDCLEAR( EVAL_TEMPFILEINFOTABLEELEMENTS[argTempFileNum]->oid );

                    /* read argument of aggregate function with distinct */
                    e = readArgumentForAggrDist(&aggrFuncList[i]);
                    if (e < eNOERROR)
                        OOSQL_ERR(e);
                }

                /*
                 * initialize the aggregate function result which is temporarily stored
                 * in the evaluation buffer
                 */
                switch (aggrFuncList[i].aggrFunctionID) {
                    case AGGRFUNC_COUNT:
                    case AGGRFUNC_COUNTALL:
                        /* NOTE: we use long type for counting */
                        ASSIGN_VALUE_TO_COL_LIST(aggrFuncResult[i], 0, sizeof(Four));
						aggrFuncResult[i].retLength = OOSQL_TYPE_LONG_SIZE_VAR; 
                        break;

                    case AGGRFUNC_AVG:
						EVAL_EVALBUFFER[m_currPlanIndex].nTuplesForSumAndAvg[i] = 0;
                        aggrFuncResult[i].data.d = 0;
						aggrFuncResult[i].retLength = OOSQL_TYPE_DOUBLE_SIZE; 
                        break;

                    case AGGRFUNC_SUM:
						EVAL_EVALBUFFER[m_currPlanIndex].nTuplesForSumAndAvg[i] = 0;
                        aggrFuncResultType = getAggrFuncResultType(&aggrFuncList[i]);
                        if (aggrFuncResultType < eNOERROR)
                            OOSQL_ERR(aggrFuncResultType);
                        switch (aggrFuncResultType) {
                            case OOSQL_TYPE_long:
                                aggrFuncResult[i].data.l = 0;
								aggrFuncResult[i].retLength = OOSQL_TYPE_LONG_SIZE; 
                                break;

                            case OOSQL_TYPE_long_long:
                                aggrFuncResult[i].data.ll = 0;
								aggrFuncResult[i].retLength = OOSQL_TYPE_LONG_LONG_SIZE; 
                                break;

                            case OOSQL_TYPE_double:
                                aggrFuncResult[i].data.d = 0;
								aggrFuncResult[i].retLength = OOSQL_TYPE_DOUBLE_SIZE;
                                break;

                            default:
                                OOSQL_ERR(eINVALID_CASE_OOSQL);
                        }
                        break;

                    case AGGRFUNC_MIN:
                    case AGGRFUNC_MAX:
                        /* initialize the minumum(maximum) value by the first value of argument */
                        e = copyArgumentToColListStruct( &(aggrFuncList[i].argument), &aggrFuncResult[i] );
                        if (e < eNOERROR)
                            OOSQL_ERR(e);
                        break;

                    default:
                        OOSQL_ERR(eINVALID_CASE_OOSQL);
                }

                /*
                 * calculate the intermediate result of aggregate function 
                 */
                e = processAggrFunc(m_currPlanIndex, i);
                if (e < eNOERROR)
                    OOSQL_ERR(e);

                /* if distinct flag is used, save the current argument.
                 * NOTE: to do this, we use the dual buffer scheme to interchange the one buffer
                 * which contains the current argument value and the other one which will
                 * contain the next argument value if exists.
                 */
                if (aggrFuncList[i].distinctFlag == SM_TRUE) {
                    e = EVAL_TEMPFILEINFOTABLEELEMENTS[argTempFileNum]->saveCurrTuple();
                    if (e < eNOERROR)
                        OOSQL_ERR(e);
                }
            }

            /* record the last plan index of the current nested-loop */
            if (m_evalStatus.lastPlanElemOfCurrNestedLoop < m_currPlanIndex)
                m_evalStatus.lastPlanElemOfCurrNestedLoop = m_currPlanIndex;

            if (ACCESSPLANELEMENTS[m_currPlanIndex].classInfo.classKind == CLASSKIND_NULL_AGGRFUNC_ONLY)
                m_evalStatus.nullClassEndOfScan = SM_FALSE;

            /* change aggregation status */
            m_evalStatus.aggregationStatus = EVALSTATUS_PROCESSING;

            break;

        case EVALSTATUS_PROCESSING:
            /* caluculate the intermediate result of aggregation */

            /* get the pool elements for the aggregate function */
            aggrFuncList = ACCESSPLAN.getAggrFuncPool(m_currPlanIndex);

            for (i = 0; i < GET_POOLSIZE(aggrFuncList); i++) {
                /* if distinct flag exists, read the next value */
                if (aggrFuncList[i].distinctFlag == SM_TRUE) {
                    e = readArgumentForAggrDist(&aggrFuncList[i]);
                    if (e < eNOERROR) {
                        OOSQL_ERR(e);
                    }
                    else if (e == ENDOFSCAN) {
                        /* [Internal Documentation]
                         * when the class kind of the current plan element is CLASSKIND_NULL_AGGRFUNC_ONLY
                         * (which does not access any class but processes aggregate function(s) with distinct flag),
                         * if we encounter the end-of-scan at one of the argument temporary files 
                         * (that contains the argument values of each aggregate function in sorted order),
                         * we can assume that we will encounter the end-of-scan for the other ones.
                         */ 
                        if (ACCESSPLANELEMENTS[m_currPlanIndex].classInfo.classKind == CLASSKIND_NULL_AGGRFUNC_ONLY)
                            m_evalStatus.nullClassEndOfScan = SM_TRUE;
                        return(eNOERROR);
                    }

                    /* get argument information of the aggregate function with distinct */
                    argument = ACCESSPLAN.getArgumentElem(aggrFuncList[i].argument);
                    argTempFileNum = argument->tempFileCol.tempFileNum;
                    argColNo = argument->tempFileCol.colNo;

                    /* check if the current argument value is distinct one */
                    cmpResult = compareColListStruct(EVAL_TEMPFILEINFOTABLEELEMENTS[argTempFileNum]->attrInfo[argColNo].type,
                                &(EVAL_TEMPFILEINFOTABLEELEMENTS[argTempFileNum]->clist[argColNo]),
                                &(EVAL_TEMPFILEINFOTABLEELEMENTS[argTempFileNum]->prevColList[argColNo]) );
                    if (cmpResult == CMP_EQ) {  /* the current argument value is not a distinct one */
                        /* continue to process the next aggregate function */
                        continue;
                    }
					else if (cmpResult == CMP_UN) {  /* Either or both argument value is NULL */
						/* if both argument values are NULL, we suppose this values is not distinct one */
						/* continue to process the next aggregate function */
						if ((EVAL_TEMPFILEINFOTABLEELEMENTS[argTempFileNum]->clist[argColNo]).nullFlag &&
							(EVAL_TEMPFILEINFOTABLEELEMENTS[argTempFileNum]->prevColList[argColNo]).nullFlag)
							continue;
					}

                    /* process aggregate function */
                    e = processAggrFunc( m_currPlanIndex, i );
                    if (e < eNOERROR)
                        OOSQL_ERR(e);

                    /* save the current argument value to select disinct value */
                    e = EVAL_TEMPFILEINFOTABLEELEMENTS[argTempFileNum]->saveCurrTuple();
                    if (e < eNOERROR)
                        OOSQL_ERR(e);
                }
                else { /* aggregate function without distinct flag */
                    /* process aggregate function */
                    e = processAggrFunc( m_currPlanIndex, i );
                    if (e < eNOERROR)
                        OOSQL_ERR(e);
                }
            }

            break;

        case EVALSTATUS_END:

            /* get the pool elements for the aggregate function */
            aggrFuncList = ACCESSPLAN.getAggrFuncPool(m_currPlanIndex);

            for (i = 0; i < GET_POOLSIZE(aggrFuncList); i++) {
				if (aggrFuncList[i].aggrFunctionID == AGGRFUNC_SUM || aggrFuncList[i].aggrFunctionID == AGGRFUNC_AVG) {
					e = calcAggrFuncResult( m_currPlanIndex, i );
                    if (e < eNOERROR)
                        OOSQL_ERR(e);
                }
            }

            break;

        default:
            OOSQL_ERR(eINVALID_CASE_OOSQL);
    }

    return(eNOERROR);
}


Four    OOSQL_Evaluator::processAggrFunc(
    Four planNo,                // IN: plan index
    Four aggrFuncIdx            // IN: index to aggregate function
)
/*
    Function:

    Side effect:

    Return value:
*/
{
    AP_AggrFuncElement *aggrFunc;
    OOSQL_StorageManager::ColListStruct *aggrFuncResult;
    Four aggrFuncResultType;
    Four cmpResult;
    Four e;
	Boolean flag;

    aggrFuncResult = EVAL_EVALBUFFER[planNo].getAggrFuncResSlotPtr(aggrFuncIdx);

    aggrFunc = ACCESSPLAN.getAggrFuncElem( planNo, aggrFuncIdx );

    switch (aggrFunc->aggrFunctionID) {
		case AGGRFUNC_COUNTALL:
#ifndef SUPPORT_LARGE_DATABASE2
			aggrFuncResult->data.l++;
#else
			aggrFuncResult->data.ll++;
#endif
			break;
		case AGGRFUNC_COUNT:
			e = isNullArgumentValue( &(aggrFunc->argument), &flag);
			if (e < eNOERROR)
				OOSQL_ERR(e);
			
			// count only if argument is not null
			if (flag == SM_FALSE){
#ifndef SUPPORT_LARGE_DATABASE2
				aggrFuncResult->data.l++;
#else                                   
				aggrFuncResult->data.ll++;
#endif   
			}
			break;
		case AGGRFUNC_SUM:              /* argument type: int or real */
			e = isNullArgumentValue( &(aggrFunc->argument), &flag);
			if (e < eNOERROR)
				OOSQL_ERR(e);

			// accumulate only if argument is not null
			if (flag == SM_FALSE)
			{
				aggrFuncResultType = getAggrFuncResultType(aggrFunc);
				if (aggrFuncResultType < eNOERROR)
					OOSQL_ERR(aggrFuncResultType);
				
				e = accumulateArgumentToColListStruct( &(aggrFunc->argument), aggrFuncResultType, aggrFuncResult );
				if (e < eNOERROR)
					OOSQL_ERR(e);
				
				EVAL_EVALBUFFER[planNo].nTuplesForSumAndAvg[aggrFuncIdx] ++;
			}
            break;

        case AGGRFUNC_AVG:              /* argument type: int or real */
			e = isNullArgumentValue( &(aggrFunc->argument), &flag);
			if (e < eNOERROR)
				OOSQL_ERR(e);
			
			// accumulate only if argument is not null
			if (flag == SM_FALSE)
			{
				aggrFuncResultType = getAggrFuncResultType( aggrFunc );
				if (aggrFuncResultType < eNOERROR)
					OOSQL_ERR(aggrFuncResultType);
				
				e = accumulateArgumentToColListStruct( &(aggrFunc->argument), aggrFuncResultType, aggrFuncResult );
				if (e < eNOERROR)
					OOSQL_ERR(e);
				
				EVAL_EVALBUFFER[planNo].nTuplesForSumAndAvg[aggrFuncIdx] ++;
			}						

            break;

        case AGGRFUNC_MIN:              // int, real
			if (aggrFuncResult->nullFlag)
			{
				e = copyArgumentToColListStruct( &(aggrFunc->argument), aggrFuncResult );
				if (e < eNOERROR)
					OOSQL_ERR(e);
			}
			else // aggrFuncResult->nullFlag != SM_TRUE
			{
				e = isNullArgumentValue( &(aggrFunc->argument), &flag);
				if (e < eNOERROR)
					OOSQL_ERR(e);
				
				// consider only if argument is not null
				if (flag == SM_FALSE)
				{
					cmpResult = compareArgumentWithColListStruct( &(aggrFunc->argument), aggrFuncResult );
					if (cmpResult < eNOERROR)
						OOSQL_ERR(cmpResult);
					
					if (cmpResult == CMP_LT) {
						e = copyArgumentToColListStruct( &(aggrFunc->argument), aggrFuncResult );
						if (e < eNOERROR)
							OOSQL_ERR(e);
					}
				} // end of if (flag == SM_FALSE)
			} // end of if (aggrFuncResult->nullFlag) else
            break;

        case AGGRFUNC_MAX:
			if (aggrFuncResult->nullFlag)
			{   
				e = copyArgumentToColListStruct( &(aggrFunc->argument), aggrFuncResult );
				if (e < eNOERROR)
					OOSQL_ERR(e);
			}
			else // aggrFuncResult->nullFlag != SM_TRUE
			{   
				e = isNullArgumentValue( &(aggrFunc->argument), &flag);
				if (e < eNOERROR)
					OOSQL_ERR(e);
				
				// consider only if argument is not null
				if (flag == SM_FALSE)
				{   
					cmpResult = compareArgumentWithColListStruct( &(aggrFunc->argument), aggrFuncResult );
					if (cmpResult < eNOERROR)
						OOSQL_ERR(cmpResult);
					
					if (cmpResult == CMP_GT) {
						e = copyArgumentToColListStruct( &(aggrFunc->argument), aggrFuncResult );
						if (e < eNOERROR)
							OOSQL_ERR(e);
					}
				} // end of if (flag == SM_FALSE)
			} // end of if (aggrFuncResult->nullFlag) else
			
			break;

        default:
            OOSQL_ERR( eINVALID_AGGRFUNC_OOSQL);
    }

    return(eNOERROR);
}


Four    OOSQL_Evaluator::readArgumentForAggrDist( 
        AP_AggrFuncElement      *aggrFunc
)
/*
    Function:

    Side effect:

    Return value:
*/
{
        OOSQL_TempFileInfo      *tempFileInfo;
        Four            e;
        Four            i;

        tempFileInfo = EVAL_TEMPFILEINFOTABLEELEMENTS[aggrFunc->tempFileNum];

		if(tempFileInfo->sortStream)
		{
			if(tempFileInfo->useFastEncoding)
			{
				if(!tempFileInfo->firstFastEncodedNextScan)
				{
					e = tempFileInfo->sortStream->FastEncodedNextScan();
					if(e == eNOERROR)
						return eNOERROR;
					else if(e == EOS)
						return ENDOFSCAN;
					else
						OOSQL_CHECK_ERR(e);
				}
				else
				{
					OOSQL_FastEncodingInfo encodingInfo[MAXNUMOFATTRIBUTE];

					e = tempFileInfo->sortStream->GetFastEncodingInfo(tempFileInfo->nCols, encodingInfo);
					OOSQL_CHECK_ERR(e);

					for(i = 0; i < tempFileInfo->nCols; i++)
					{
						if(encodingInfo[i].type == OOSQL_TYPE_STRING || encodingInfo[i].type == OOSQL_TYPE_VARSTRING)
							encodingInfo[i].ptr  = (char*)tempFileInfo->clist[i].data.ptr;
						else
							encodingInfo[i].ptr  = (char*)&tempFileInfo->clist[i].data.s;

						encodingInfo[i].size     = &tempFileInfo->clist[i].retLength;
						encodingInfo[i].nullFlag = &tempFileInfo->clist[i].nullFlag;
					}

					e = tempFileInfo->sortStream->SetFastEncodingInfo(tempFileInfo->nCols, encodingInfo);
					OOSQL_CHECK_ERR(e);

					tempFileInfo->firstFastEncodedNextScan = SM_FALSE;
					
					e = tempFileInfo->sortStream->FastEncodedNextScan();
					if(e == eNOERROR)
						return eNOERROR;
					else if(e == EOS)
						return EOS;
					else
						OOSQL_CHECK_ERR(e);
				}
			}
			else
			{
				e = tempFileInfo->sortStream->FastNextScan(tempFileInfo->nCols, tempFileInfo->clist);
				if(e == eNOERROR)
					return eNOERROR;
				else if(e == EOS)
					return ENDOFSCAN;
				else
					OOSQL_CHECK_ERR(e);
			}
		}
		else
		{
			e = m_storageManager->NextObject( tempFileInfo->osn, &(tempFileInfo->oid), NULL );
			OOSQL_CHECK_ERR(e);

			if(e == EOS)
				return(ENDOFSCAN);

			for(i = 0; i < tempFileInfo->nCols; i++)
				tempFileInfo->clist[i].dataLength = tempFileInfo->clist[i].length;

			e = m_storageManager->FetchObjectByColList( tempFileInfo->osn, SM_TRUE, &(tempFileInfo->oid), 
														tempFileInfo->nCols, tempFileInfo->clist );
			if (e < eNOERROR) OOSQL_ERR(e);
		}

        return eNOERROR;
}


Four    OOSQL_Evaluator::calcAggrFuncResult(
    Four planNo,                /* IN: */
    Four aggrFuncIdx            /* IN: */
)
/*
    Function:

    Side effect:

    Return value:
*/
{
    AP_AggrFuncElement *aggrFunc;
    OOSQL_StorageManager::ColListStruct *aggrFuncResult;

    aggrFuncResult = EVAL_EVALBUFFER[planNo].getAggrFuncResSlotPtr(aggrFuncIdx);

    aggrFunc = ACCESSPLAN.getAggrFuncElem( planNo, aggrFuncIdx );
    switch (aggrFunc->aggrFunctionID) {
        /* Check where there is tuple to participate in SUM or not */
        case AGGRFUNC_SUM:
			if(EVAL_EVALBUFFER[planNo].nTuplesForSumAndAvg[aggrFuncIdx] == 0)
				aggrFuncResult->nullFlag = SM_TRUE;
			else    aggrFuncResult->nullFlag = SM_FALSE;
									            break;
        case AGGRFUNC_AVG:              // int, real
			/* Check where there is tuple to participate in AVG or not */
			// aggrFuncResult->data.d = aggrFuncResult->data.d / EVAL_EVALBUFFER[planNo].nTuplesForAvg[aggrFuncIdx];
			if(EVAL_EVALBUFFER[planNo].nTuplesForSumAndAvg[aggrFuncIdx] == 0)
			{               
				aggrFuncResult->nullFlag = SM_TRUE;
			}           
			else        
			{       
				aggrFuncResult->data.d = aggrFuncResult->data.d / EVAL_EVALBUFFER[planNo].nTuplesForSumAndAvg[aggrFuncIdx];
				aggrFuncResult->nullFlag = SM_FALSE;
			}
            break;

        default:
            break;
    }

    return(eNOERROR);
}


Four    OOSQL_Evaluator::getAggrFuncResultType(
        AP_AggrFuncElement      *aggrFunc
)
/*
    Function:

    Side effect:

    Return value:
*/
{
    Four    resultType;
    Four    resultLength;
    Four    e;

    switch (aggrFunc->aggrFunctionID) 
	{
    case AGGRFUNC_COUNTALL:
    case AGGRFUNC_COUNT:
#ifndef SUPPORT_LARGE_DATABASE2
        resultType = OOSQL_TYPE_long;
#else
        resultType = OOSQL_TYPE_long_long;
#endif
        break;

    case AGGRFUNC_SUM:
        e = getArgumentInfo( &(aggrFunc->argument), &resultType, &resultLength );
        if (e < eNOERROR) OOSQL_ERR(e);

        switch (resultType) 
		{
        case OOSQL_TYPE_float:
        case OOSQL_TYPE_double:
            resultType = OOSQL_TYPE_double;
            break;

        default:
#ifndef SUPPORT_LARGE_DATABASE2
	        resultType = OOSQL_TYPE_long;
#else
			resultType = OOSQL_TYPE_long_long;
#endif
            break;
        }
        break;

    case AGGRFUNC_AVG:
        resultType = OOSQL_TYPE_double;
        break;

    case AGGRFUNC_MIN:
    case AGGRFUNC_MAX:
        e = getArgumentInfo( &(aggrFunc->argument), &resultType, &resultLength );
        if (e < eNOERROR) OOSQL_ERR(e);
        break;

    default:
        OOSQL_ERR(eINVALID_CASE_OOSQL);
    }

    return resultType;
}

