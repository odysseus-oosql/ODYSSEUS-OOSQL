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
        oosql_Eval_Grouping.cxx

    DESCRIPTION:
        This module implements member functions of OOSQL_Evaluator
        that are related to grouping processing.

    IMPORTS:

    EXPORTS:
        Four    OOSQL_Evaluator::grouping()
*/


/*
 * include header files
 */
#include "OOSQL_Evaluator.hxx"
#include "oosql_Eval_Expr.hxx"


Four    OOSQL_Evaluator::grouping()
/*
    Function:

    Side effect:
        m_evalStatus.groupingStatus and m_evalStatus.aggregationStatus can be changed.

    Referenced member variables:
        m_evalStatus.groupingStatus

    Return value:
*/
{
    One isSameGroup;
    Four e;

    switch(m_evalStatus.groupingStatus) {
        case EVALSTATUS_INIT:
            /* set new Group By keys */
            e = setGroupByKey();
            if (e < eNOERROR)
                OOSQL_ERR(e);

            /* change grouping status */
            m_evalStatus.groupingStatus = EVALSTATUS_PROCESSING;

            /* record the last plan index of the current nested-loop */
            if (m_evalStatus.lastPlanElemOfCurrNestedLoop < m_currPlanIndex)
                m_evalStatus.lastPlanElemOfCurrNestedLoop = m_currPlanIndex;
            break;

        case EVALSTATUS_PROCESSING:
            /* check if the current object belongs to the current group */
            isSameGroup = isSameGroupByKey();
            if (isSameGroup < eNOERROR)
                OOSQL_ERR(isSameGroup);

            if (!isSameGroup) { /* the current object does not belong to the current group */
                /* change groupingStatus to notify new group has been made */
                m_evalStatus.groupingStatus = EVALSTATUS_END;

                /* check if aggregation is necessary and
                 * if necessary, change aggregationStatus to calculate the aggregate function
                 * results for the current group
                 */
                if (!IS_NULL_POOLINDEX(ACCESSPLANELEMENTS[m_currPlanIndex].aggrFuncInfo))
                    m_evalStatus.aggregationStatus = EVALSTATUS_END;
            }

            break;

        case EVALSTATUS_END:
            /* a group has already been determined so just return */
            return(eNOERROR);

        default:
            OOSQL_ERR(eINVALID_CASE_OOSQL);
    }

    return(eNOERROR);
}


Four    OOSQL_Evaluator::setGroupByKey()
/*
    Function:

    Side effect:

    Referenced member variables:
        m_currPlanIndex

    Return value:
*/
{
    AP_UsedColPoolElements pUsedColPool;
    Four grpByColNo;
    Four grpByKeyType;
    Four i;
    Four e;

    /* get column information pool for the current plan element */
    pUsedColPool = ACCESSPLAN.getUsedColPool(m_currPlanIndex);

    /* initialize group by key(s) */
    for (i = 0; i < ACCESSPLANELEMENTS[m_currPlanIndex].nGrpByKeys; i++) {
        /* get column no. and column type of Group By key */
        grpByColNo = ACCESSPLANELEMENTS[m_currPlanIndex].grpByKeys[i];
        grpByKeyType = pUsedColPool[grpByColNo].typeId;

        /* copy Group By key */
        e = copyColListStruct( grpByKeyType, 
                EVAL_EVALBUFFER[m_currPlanIndex].getColSlotPtr(grpByColNo), 
                EVAL_EVALBUFFER[m_currPlanIndex].getGrpBySlotPtr(grpByColNo) );
		OOSQL_CHECK_ERR(e);
    }

    return(eNOERROR);
}


Four    OOSQL_Evaluator::isSameGroupByKey()
/*
    Function:
        Check if the current object belongs to the current group.

    Side effect:

    Referenced member variables:
        m_currPlanIndex

    Return value:
        SM_TRUE/SM_FALSE
*/
{
    AP_UsedColPoolElements pUsedColPool;
    Four grpByColNo;
    Four i;
    Four e;

    pUsedColPool = ACCESSPLAN.getUsedColPool(m_currPlanIndex);

    for (i = 0; i < ACCESSPLANELEMENTS[m_currPlanIndex].nGrpByKeys; i++) {
        grpByColNo = ACCESSPLANELEMENTS[m_currPlanIndex].grpByKeys[i];
        e = compareColListStruct(pUsedColPool[i].typeId, 
                EVAL_EVALBUFFER[m_currPlanIndex].getColSlotPtr(grpByColNo),
                EVAL_EVALBUFFER[m_currPlanIndex].getGrpBySlotPtr(grpByColNo));
        if (e < eNOERROR) OOSQL_ERR(e);

		if (e == CMP_UN) {  /* Either or both argument value is NULL */
			/* if both argument values are NULL, we suppose this values are not distinct one */
			if ( !(EVAL_EVALBUFFER[m_currPlanIndex].getColSlotPtr(grpByColNo)->nullFlag)||
				!(EVAL_EVALBUFFER[m_currPlanIndex].getGrpBySlotPtr(grpByColNo)->nullFlag))
				return SM_FALSE;
		}
		else if (e != CMP_EQ) {      // different group 
			return SM_FALSE;
        }

    }

    return SM_TRUE;
}
