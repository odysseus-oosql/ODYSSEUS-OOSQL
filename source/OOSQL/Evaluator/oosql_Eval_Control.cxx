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
        oosql_Eval_Control.cxx

    DESCRIPTION:

    IMPORTS:

    EXPORTS:
*/

#include "OOSQL_Evaluator.hxx"
#include "oosql_Eval_Control.hxx"
#include "oosql_Eval_Access.hxx"

Four OOSQL_Evaluator::moveToAndOpenNextPlanElement()
/*
    Function:

    Side effect:
        m_currPlanIndex is increased by 1

    Referenced member variables:
        m_currPlanIndex

    Return value:
*/
{
    Four					numClasses;
    OOSQL_AccessElement*	accessElement;
    Four					i;
    Four					e;

    /*
     * Step 1: move forward the current evaluation control
     */
    m_currPlanIndex = m_currPlanIndex + 1;
	resetCurrentWhereCondNodes();		

	if(m_currPlanIndex >= ACCESSPLAN.m_nAP_Elem)
		return eNOERROR;

    /*
     * Step 2: open scan for each access element of the current access plan element
     */
    numClasses = EVAL_ACCESSLISTTABLE[m_currPlanIndex].numClasses;
    for (i = 0; i < numClasses; i++) {
        accessElement = &(EVAL_ACCESSLISTTABLE[m_currPlanIndex].accessList[i]);

        /* open scan(s) to evaluate the current access plan element */
        switch (accessElement->accessMethod) {
            case ACCESSMETHOD_OID_FETCH:
                EVAL_ACCESSLISTTABLE[m_currPlanIndex].endOfCurrAccess = SM_FALSE;
                break;

#ifdef  FUTURE_EXTENSION        
            case ACCESSMETHOD_OID_SETSCAN:
                e = openOID_SetScan(m_currPlanIndex, i);
                if (e < eNOERROR)
                    OOSQL_ERR(e);
                break;
#endif

            case ACCESSMETHOD_SEQ_SCAN:
                /* set sequential scan direction */
                switch (EVAL_ACCESSLISTTABLE[m_currPlanIndex].accessDirection) {
                    case ACCESSDIRECTION_FORWARD:
                        accessElement->scanDirection = FORWARD;
                        break;
                    case ACCESSDIRECTION_BACKWARD:
                        accessElement->scanDirection = BACKWARD;
                        break;
                    default:
                        OOSQL_ERR(eINVALID_CASE_OOSQL);
                }

                /* open sequential scan */
                e = openSeqScan(m_currPlanIndex, i);
                if (e < eNOERROR)
                    OOSQL_ERR(e);
                break;

            case ACCESSMETHOD_BTREE_IDXSCAN:
                e = openBtreeIndexScan(m_currPlanIndex, i);
                if (e < eNOERROR)
                    OOSQL_ERR(e);
                break;

            case ACCESSMETHOD_TEXT_IDXSCAN:
				if(i == 0)
				{
					e = openTextIndexScan(m_currPlanIndex, i);
					if (e < eNOERROR)
						OOSQL_ERR(e);
				}
                break;

            case ACCESSMETHOD_MLGF_MBR_IDXSCAN:
                e = openMLGF_MBR_IndexScan(m_currPlanIndex, i);
                if (e < eNOERROR)
                    OOSQL_ERR(e);
                break;
			
            case ACCESSMETHOD_MLGF_IDXSCAN:
                e = openMLGF_IndexScan(m_currPlanIndex, i);
                if (e < eNOERROR)
                    OOSQL_ERR(e);
                break;

            default:
                OOSQL_ERR(eINVALID_CASE_OOSQL);
        } /* end of switch */
    } /* end of for */

    /*
     * Step 3: update the first plan index and the last one of the current nested-loop if necessary
     */
    /* Step 3.1: check if the current access plan element is the first one of the current nested-loop */
    if (ACCESSPLANELEMENTS[m_currPlanIndex].joinMethod == JOINMETHOD_OUTERMOST_CLASS) 
        m_evalStatus.firstPlanElemOfCurrNestedLoop = m_currPlanIndex;

    /* Step 3.2: update the index to the last access plan element of the current nested-loop */
    if (m_evalStatus.lastPlanElemOfCurrNestedLoop < m_currPlanIndex)
        m_evalStatus.lastPlanElemOfCurrNestedLoop = m_currPlanIndex;

    /*
     * Step 4: reset or set the flag related to the evaluation status
     */
    /* reset the evaluation status: groupingStatus, aggregationStatus, and prepareAndSortStatus */
    if (m_evalStatus.groupingStatus != EVALSTATUS_PROCESSING)			
    	m_evalStatus.groupingStatus = EVALSTATUS_INIT;
	if (m_evalStatus.aggregationStatus != EVALSTATUS_PROCESSING)		
    	m_evalStatus.aggregationStatus = EVALSTATUS_INIT;
	if (m_evalStatus.prepareAndSortStatus != EVALSTATUS_PROCESSING)	
    	m_evalStatus.prepareAndSortStatus = EVALSTATUS_INIT;

    /* check if any processing is necessary for the current access plan element */
    if (ACCESSPLANELEMENTS[m_currPlanIndex].nGrpByKeys > 0 ||
        !IS_NULL_POOLINDEX(ACCESSPLANELEMENTS[m_currPlanIndex].aggrFuncInfo) ||
        !IS_NULL_POOLINDEX(ACCESSPLANELEMENTS[m_currPlanIndex].projectionList)) {
        m_evalStatus.needToProcessCurrPlanElem = SM_TRUE;
    }
    else {
        m_evalStatus.needToProcessCurrPlanElem = SM_FALSE;
    }

    /* if the class kind of the current plan element is CLASSKIND_NULL_AGGRFUNC_ONLY,
     * initialize the end-of-scan flag for this class kind.
     */
    if (ACCESSPLANELEMENTS[m_currPlanIndex].classInfo.classKind == CLASSKIND_NULL_AGGRFUNC_ONLY) {
        m_evalStatus.nullClassEndOfScan = SM_FALSE;
    }

    return(eNOERROR);
}


Four    OOSQL_Evaluator::backtrack()
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
    OOSQL_AccessElement *accessElement;
    Four i;
    Four e;

    /* close scans for inner class of nested-loop except for implicit nested-loop
     * because access method of OID fetch is used for it
     */
    for (i = 0; i < EVAL_ACCESSLISTTABLE[m_currPlanIndex].numClasses; i++) {
        accessElement = &(EVAL_ACCESSLISTTABLE[m_currPlanIndex].accessList[i]);

        switch (accessElement->accessMethod) {
            case ACCESSMETHOD_SEQ_SCAN:
                e = closeSeqScan(m_currPlanIndex, i);
                if (e < eNOERROR)
                    OOSQL_ERR(e);
                break;

            case ACCESSMETHOD_BTREE_IDXSCAN:
                e = closeBtreeIndexScan(m_currPlanIndex, i);
                if (e < eNOERROR)
                    OOSQL_ERR(e);
                break;

            case ACCESSMETHOD_TEXT_IDXSCAN:
				if(i == 0)
				{
					e = closeTextIndexScan(m_currPlanIndex, i);
					if (e < eNOERROR)
						OOSQL_ERR(e);
				}
                break;

            case ACCESSMETHOD_MLGF_IDXSCAN:
			case ACCESSMETHOD_MLGF_MBR_IDXSCAN: 
                e = closeMLGF_IndexScan(m_currPlanIndex, i);
                if (e < eNOERROR)
                    OOSQL_ERR(e);
                break;
        }
    }

    /* reverse scan direction for all access elements to utilize
     *  LRU buffer replacement and alos reset the endOfCurrAccess flag
     */
    e = EVAL_ACCESSLISTTABLE[m_currPlanIndex].reverseAccessDirection();
    if (e < eNOERROR)
        OOSQL_ERR(e);

    /* move backward the current evaluation control */
    m_currPlanIndex = m_currPlanIndex - 1;

	resetCurrentWhereCondNodes();	

    /* check if any processing is necessary for the current access plan element */
    if (ACCESSPLANELEMENTS[m_currPlanIndex].nGrpByKeys > 0 ||
        !IS_NULL_POOLINDEX(ACCESSPLANELEMENTS[m_currPlanIndex].aggrFuncInfo) ||
        !IS_NULL_POOLINDEX(ACCESSPLANELEMENTS[m_currPlanIndex].projectionList)) {

        m_evalStatus.needToProcessCurrPlanElem = SM_TRUE;
    }
    else {
        m_evalStatus.needToProcessCurrPlanElem = SM_FALSE;
    }

    return(eNOERROR);
}


Four    OOSQL_Evaluator::moveToLastPlanElemOfNestedLoop()
/*
    Function:
        Move the evaluation control to the last plan element of the current nested-loop.

         ***************************************************************************************
         * [Internal Documentation]
         * This function is called only when the last step processing is necessary
         * for the current nested-loop, e.g. processing the last group, calculating the
         * aggregate function result, sorting the temporary file etc.
         * So it not only moves the evaluation control but also changes the evaluation status.
         *
         * Evaluation status can be changed from EVALSTATU_PROCESSING to EVALSTATUS_END
         * for m_evalStatus.groupingStatus, m_evalStatus.aggregationStatus, and prepareAndSortStatus
         * if one of them is EVALSTATUS_PROCESSING.
         ***************************************************************************************

    Side effect:
        m_currPlanIndex is changed.
        m_evalStatus can be changed.

    Referenced member variables:

    Return value:
*/
{
	// to avoid "open scan on ordered set" error processing update query using IR index scan
	/* Note: this code is originated from function backtrack() */

    OOSQL_AccessElement *accessElement;
    Four i;
    Four e;

    /* close scans for inner class of nested-loop except for implicit nested-loop
     * because access method of OID fetch is used for it
     */
    for (i = 0; i < EVAL_ACCESSLISTTABLE[m_currPlanIndex].numClasses; i++) {
        accessElement = &(EVAL_ACCESSLISTTABLE[m_currPlanIndex].accessList[i]);

        switch (accessElement->accessMethod) {
            case ACCESSMETHOD_SEQ_SCAN:
                e = closeSeqScan(m_currPlanIndex, i);
                if (e < eNOERROR)
                    OOSQL_ERR(e);
                break;

            case ACCESSMETHOD_BTREE_IDXSCAN:
                e = closeBtreeIndexScan(m_currPlanIndex, i);
                if (e < eNOERROR)
                    OOSQL_ERR(e);
                break;

            case ACCESSMETHOD_TEXT_IDXSCAN:
				if(i == 0)
				{
					e = closeTextIndexScan(m_currPlanIndex, i);
					if (e < eNOERROR)
						OOSQL_ERR(e);
				}
                break;

            case ACCESSMETHOD_MLGF_IDXSCAN:
			case ACCESSMETHOD_MLGF_MBR_IDXSCAN: 
                e = closeMLGF_IndexScan(m_currPlanIndex, i);
                if (e < eNOERROR)
                    OOSQL_ERR(e);
                break;
        }
    }



    /* move the evaluation control to the last plan element of the current nested-loop */
    m_currPlanIndex = m_evalStatus.lastPlanElemOfCurrNestedLoop;

    /* change status from PROCESSING to END in order to do last-step processing */
    if (m_evalStatus.groupingStatus == EVALSTATUS_PROCESSING)
	{
        m_evalStatus.groupingStatus = EVALSTATUS_END;
		/* To prevent process function called twice when both GROUP BY and 
		   ORDER BY clause are used, the following line is inserted.
		*/
		m_evalStatus.aggregationStatus = EVALSTATUS_END;

		if(!isLastPlanElement()) 
			process();
	}

    if (m_evalStatus.aggregationStatus == EVALSTATUS_PROCESSING)
	{
        m_evalStatus.aggregationStatus = EVALSTATUS_END;

		if(!isLastPlanElement()) 
			process();
	}

    if (m_evalStatus.prepareAndSortStatus == EVALSTATUS_PROCESSING)
        m_evalStatus.prepareAndSortStatus = EVALSTATUS_END;

    return(eNOERROR);
}

Four    OOSQL_Evaluator::saveEvaluatorStatus(
        Four    planNo          // index to the access plan element currently evaluated
)
/*
    Function:
        Save the current status of the Evaluator.

    Side effect:

    Return value:
        eNOERROR        if no error
*/
{
    // check input parameter
    if (planNo < 0 || ACCESSPLAN.getNumAP_Elem() <= planNo) 
	{
		OOSQL_ERR( eBADPARAMETER_OOSQL);
    }

    m_currPlanIndex = planNo;

    return eNOERROR;
}

