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

#ifndef _OOSQL_ACCESSPLAN_HXX_
#define _OOSQL_ACCESSPLAN_HXX_

/*
    MODULE:
        OOSQL_AccessPlan.hxx

    DESCRIPTION:
        This header file defines the Access Plan structure of OOSQL processor.

*/

#include "OOSQL_Common.h"
#include "OOSQL_DB_Value.hxx"
#include "OOSQL_Error.h"
#include "OOSQL_MemoryManager.hxx"
#include "OOSQL_MemoryManagedObject.hxx"

/*
 * include OOSQL Global Data Structures
 */
#include "OQL_Plan_Struct.hxx"

// include the query result information structure
#include "OOSQL_ResultInfo.hxx"


/****************************************
 * constant definitions for access plan *
 ****************************************/


/* join method
 * NOTE: constants CAP_* are defined in OQL_Plan_Struct.hxx
 */
#define JOINMETHOD_OUTERMOST_CLASS      CAP_JOINMETHOD_OUTERMOST_CLASS
#define JOINMETHOD_CARTESIAN_PRODUCT    CAP_JOINMETHOD_CARTESIAN_PRODUCT
#define JOINMETHOD_IMPLICIT_FORWARD_NESTEDLOOP  CAP_JOINMETHOD_IMPLICIT_FORWARD
#define JOINMETHOD_IMPLICIT_BACKWARD_SORTDOMAIN CAP_JOINMETHOD_IMPLICIT_BACKWARD
#define JOINMETHOD_EXPLICIT_NESTEDLOOP  CAP_JOINMETHOD_EXPLICIT_NESTEDLOOP
#define JOINMETHOD_EXPLICIT_SORTMERGE   CAP_JOINMETHOD_EXPLICIT_SORTMERGE

/* access method
 * NOTE: CAP_* are defined in OQL_Plan_Struct.hxx
 */
#define ACCESSMETHOD_OID_FETCH  CAP_ACCESSMETHOD_OIDFETCH
/* reserved for future extension */
#define ACCESSMETHOD_OID_SETSCAN        10
#define ACCESSMETHOD_SEQ_SCAN   CAP_ACCESSMETHOD_SEQSCAN
/* subdivided index scan access method into B+ tree, Text, and MLGF index scan */
#define ACCESSMETHOD_BTREE_IDXSCAN      CAP_ACCESSMETHOD_BTREE_INDEXSCAN
#define ACCESSMETHOD_TEXT_IDXSCAN       CAP_ACCESSMETHOD_TEXT_INDEXSCAN
#define ACCESSMETHOD_MLGF_IDXSCAN       CAP_ACCESSMETHOD_MLGF_INDEXSCAN
#define ACCESSMETHOD_MLGF_MBR_IDXSCAN   CAP_ACCESSMETHOD_MLGF_MBR_INDEXSCAN 

/* macro definitions for checking join method */
#define IS_IMPLICITFORWARD(joinMethod)  \
        (joinMethod == JOINMETHOD_IMPLICIT_FORWARD_NESTEDLOOP)


// forward declaration
class   OOSQL_Evaluator;

// define OOSQL_AP_Element to reuse common access plan structure
#define OOSQL_AP_Element        CommonAP_Element
#define OOSQL_AP_PoolElements   CommonAP_PoolElements


class OOSQL_StorageManager;
class OOSQL_Catalog;

class   OOSQL_AccessPlan : public OOSQL_MemoryManagedObject {

		friend class OOSQL_Evaluator;

		OOSQL_StorageManager*	m_storageManager;
		OOSQL_Catalog*			m_catalog;
        Four					m_nAP_Elem;       // the # of access plan element
        CommonAP_Element*		m_accessPlanElements;

        /**********************************************************
         * pool structures referenced by the access plan elements *
         **********************************************************/

        AP_CondListPool *condListPool;          // ptr. to condition(disjunct) list pool
        Four            condListPoolSize;
        AP_ExprPool     *exprPool;              // ptr. to expression pool
        Four            exprPoolSize;
        SubClassPool    *subClassPool;          // ptr. to subclass info. pool
        Four            subClassPoolSize;
        AP_AggrFuncPool *aggrFuncPool;          // ptr. to aggregate function info. pool
        Four            aggrFuncPoolSize;
        AP_ColNoMapPool *colNoMapPool;          // ptr. to column no. mapping pool
        Four            colNoMapPoolSize;
        AP_UsedColPool  *usedColPool;           // ptr. to used column info. pool
        Four            usedColPoolSize;
        AP_MethodNoMapPool      *methodNoMapPool;       // ptr. to method no. mapping pool
        Four                    methodNoMapPoolSize;
        AP_UsedMethodPool       *usedMethodPool;        // ptr. to used method info. pool
        Four                    usedMethodPoolSize;
        AP_ProjectionListPool   *projectionListPool;    // ptr. to projection list pool (to support mutiple projection)
        Four                    projectionListPoolSize;
        AP_ProjectionPool       *projectionPool;        // ptr. to projection info. pool
        Four                    projectionPoolSize;

        AP_ArgumentPool *argumentPool;          // ptr. to argument info. pool
        Four            argumentPoolSize;
        ValuePool       *valuePool;             // ptr. to value pool
        Four            valuePoolSize;
        IntegerPool     *intPool;               // ptr. to integer constant pool
        Four            intPoolSize;
        RealPool        *realPool;              // ptr. to real constant pool
        Four            realPoolSize;
        StringPool      *stringPool;            // ptr. to string constant pool
        Four            stringPoolSize;
        FunctionPool    *funcPool;              // ptr. to function pool
        Four            funcPoolSize;
        CollectionPool  *collectionPool;        // ptr. to collection constant pool
        Four            collectionPoolSize;
        DomainPool      *domainPool;            // ptr. to domain pool
        Four            domainPoolSize;
        MBRPool         *mbrPool;               // ptr. to MBR pool
        Four            mbrPoolSize;
        MemberPool      *memberPool;            // ptr. to member pool
        Four            memberPoolSize;
        AP_IndexInfoPool        *indexInfoPool;         // ptr. to index info pool
        Four                    indexInfoPoolSize;
        AP_TextIndexCondPool    *textIndexCondPool;     // ptr. to text index condition pool
        Four                    textIndexCondPoolSize;

        Four          datePoolSize;
        DatePool      *datePool;
        Four          timePoolSize;
        TimePool      *timePool;
        Four          timestampPoolSize;
        TimestampPool *timestampPool;
        Four          intervalPoolSize;
        IntervalPool  *intervalPool;
		Four					insertValuePoolSize;
		AP_InsertValuePool*		insertValuePool;
		Four					updateValuePoolSize;
		AP_UpdateValuePool*		updateValuePool;
public:
    OOSQL_AccessPlan( 
        OOSQL_AP_PoolElements,
        OOSQL_StorageManager*,
        OOSQL_Catalog*,
        AP_CondListPool*,       // ptr. to condition list pool
        Four,
        AP_ExprPool*,           // ptr. to expression pool
        Four,
        SubClassPool*,          // ptr. to subclass info. pool
        Four,
        AP_AggrFuncPool*,       // ptr. to aggregate function info. pool
        Four,
        AP_ColNoMapPool*,       // ptr. to column no. mapping pool
        Four,
        AP_UsedColPool*,        // ptr. to used column info. pool
        Four,
        AP_MethodNoMapPool*,    // ptr. to method no. mapping pool
        Four,
        AP_UsedMethodPool*,     // ptr. to used method info. pool
        Four,
        AP_ProjectionListPool*, // ptr. to multiple projection
        Four,
        AP_ProjectionPool*,     // ptr. to projection info. pool
        Four,
        AP_ArgumentPool*,       // ptr. to argument info. pool
        Four,
        ValuePool*,             // ptr. to value pool
        Four,
        IntegerPool*,           // ptr. to integer constant pool
        Four,
        RealPool*,              // ptr. to real constant pool
        Four,
        StringPool*,            // ptr. to string constant pool
        Four,
        FunctionPool*,          // ptr. to function pool
        Four,
        CollectionPool*,        // ptr. to collection constant pool
        Four,
        DomainPool*,            // ptr. to domain pool
        Four,
        MBRPool*,               // ptr. to MBR pool
        Four,
        MemberPool*,            // ptr. to member pool
        Four,
        AP_IndexInfoPool*,      // ptr. to index info pool
        Four,
        AP_TextIndexCondPool*,  // ptr. to text index condition pool
        Four,
        DatePool*,              // ptr. to date pool
        Four,
        TimePool*,              // ptr. to time pool
        Four,
        TimestampPool*,         // ptr. to timestamp pool
        Four,
        IntervalPool*,          // ptr. to interval pool
        Four,
		AP_InsertValuePool*,	// ptr. to insert value pool
		Four,
		AP_UpdateValuePool*,	// ptr. to update value pool
		Four
    );
    
    
    /*
     * functions for accessing pool elements
     */
    AP_UsedColElement*          getUsedColElem( Four, Two );
    AP_UsedColPoolElements      getUsedColPool( Four );
    AP_UsedColPoolElements      getUsedColPool( AP_UsedColPoolIndex );
    AP_UsedMethodElement*       getUsedMethodElem( Four, Two );
    AP_UsedMethodPoolElements   getUsedMethodPool( Four );
    AP_ColNoMapPoolElements     getColNoMapPool( Four );
    AP_MethodNoMapPoolElements  getMethodNoMapPool( Four );
    AP_CondListPoolElements     getWhereCondPool( Four );
    AP_CondListPoolElements     getHavingCondPool( Four );
    AP_ExprPoolElements         getExprPool( AP_ExprPoolIndex );
    AP_ExprElement*             getExprElem( AP_ExprPoolIndex );
    AP_SubClassPoolElements     getSubClassPool( Four );
    AP_AggrFuncPoolElements     getAggrFuncPool( Four );
    AP_AggrFuncElement*         getAggrFuncElem( Four, Four );
    AP_AggrFuncElement*         getAggrFuncElem( AP_AggrFuncPoolIndex );
    AP_ProjectionListPoolElements getProjectionListPool( Four );
    AP_ProjectionPoolElements   getProjectionPool( AP_ProjectionPoolIndex );
    AP_ProjectionElement*       getProjectionElem( AP_ProjectionPoolIndex );
    AP_ArgumentPoolElements     getArgumentPool( AP_ArgumentPoolIndex );
    AP_ArgumentElement*         getArgumentElem( AP_ArgumentPoolIndex );
    ValueElement*               getValueElem( ValuePoolIndex );
    long                        getIntValue( IntegerPoolIndex );
    float                       getRealValue( RealPoolIndex );
    AP_StringPoolElements       getStringPool( StringPoolIndex );
    
    AP_IndexInfoElement*        getIndexInfoElem( AP_IndexInfoPoolIndex );
    Four                        getIndexInfoPoolSize();
    AP_TextIndexCondPoolElements getTextIndexCondPool( AP_TextIndexCondPoolIndex );
    AP_TextIndexCondElement*    getTextIndexCondElem( AP_TextIndexCondPoolIndex );
    Four                        getTextIndexCondPoolSize();
    
    Four                        getValue( ValuePoolIndex, OOSQL_DB_Value* );
    const char*					getStringValue( StringPoolIndex );
    
	Four						getMBRValue(MBRPoolIndex mbr, Four dim);

    Four						getDateValue(DatePoolIndex, OOSQL_StorageManager::Date*);
    Four						getTimeValue(TimePoolIndex, OOSQL_StorageManager::Time*);
    Four						getTimestampValue(TimestampPoolIndex, OOSQL_StorageManager::Timestamp*);
    Four						getIntervalValue(IntervalPoolIndex, OOSQL_StorageManager::Interval*);
    
    // return the # of access plan elements
    Four        getNumAP_Elem();
    
    // return the mapped column no. for (plan no, column no.)
    Four        getMappedColNo( Four, Four );
    
    // return the # of query result columns
    Four        getNumOfQueryResCols();
    Four        getQueryResultInfoLengthInChar();
    
    // return query result information
    Four        getQueryResultInfo( OOSQL_ResultInfo* );
    Four        getQueryResultInfoByString( Four, char*, Four* );
};

inline AP_UsedColElement* OOSQL_AccessPlan::getUsedColElem( 
    Four    planNo, 
    Two     colNo
)
/*
    Function:
        Return pointer to UsedColPool element for (planNo, colNo).

    Side effect:

    Return value:
        Pointer to an UsedColPool element.
*/
{
    AP_UsedColPoolElements  usedColPoolElements(*usedColPool);
    Four                    mappedColNo;
	
    // check input parameter
#ifdef OOSQL_DEBUG
    if (planNo < 0 || m_nAP_Elem <= planNo) 
	{
        OOSQL_ERR_EXIT( eINVALID_PLANNO_OOSQL);
    }

    if (colNo < 0 || GET_POOLSIZE(m_accessPlanElements[planNo].colNoMap) <= colNo) 
	{
        OOSQL_ERR_EXIT( eINVALID_COLNO_OOSQL);
    }
#endif

    usedColPoolElements = m_accessPlanElements[planNo].usedColInfo;

    mappedColNo = getColNoMapPool(planNo)[colNo].offset;

    return &usedColPoolElements[mappedColNo];
}

inline AP_UsedColPoolElements OOSQL_AccessPlan::getUsedColPool( 
    Four    planNo
)
/*
    Function:
        Return pointer to UsedColPool for (planNo).

    Side effect:

    Return value:
        Pointer to an UsedColPool.
*/
{
    AP_UsedColPoolElements  usedColPoolElements(*usedColPool);

    // check input parameter
#ifdef OOSQL_DEBUG
    if (planNo < 0 || m_nAP_Elem <= planNo) {
        OOSQL_ERR_EXIT( eINVALID_PLANNO_OOSQL);
    }
#endif

    usedColPoolElements = m_accessPlanElements[planNo].usedColInfo;

    return usedColPoolElements;
}


inline AP_UsedColPoolElements OOSQL_AccessPlan::getUsedColPool( 
    AP_UsedColPoolIndex     usedColPoolIndex
)
/*
    Function:
        Return pointer to UsedColPool for (planNo).

    Side effect:

    Return value:
        Pointer to an UsedColPool.
*/
{
    AP_UsedColPoolElements  usedColPoolElements(*usedColPool);

    // check input parameter
#ifdef  OOSQL_DEBUG
    if (IS_NULL_POOLINDEX(usedColPoolIndex) == SM_TRUE) {
        OOSQL_ERR_EXIT( eBADPARAMETER_OOSQL);
    }
#endif

    usedColPoolElements = usedColPoolIndex;

    return usedColPoolElements;
}


inline AP_UsedMethodElement* OOSQL_AccessPlan::getUsedMethodElem( 
    Four    planNo, 
    Two     methodNo
)
/*
    Function:
        Return pointer to UsedMethodPool element for (planNo, methodNo).

    Side effect:

    Return value:
        Pointer to an UsedMethodPool element.
*/
{
    AP_UsedMethodPoolElements usedMethodPoolElements(*usedMethodPool);
    Four                    mappedMethodNo;

    // check input parameter
#ifdef OOSQL_DEBUG
    if (planNo < 0 || m_nAP_Elem <= planNo) {
        OOSQL_ERR_EXIT( eINVALID_PLANNO_OOSQL);
    }

    if (methodNo < 0 || GET_POOLSIZE(m_accessPlanElements[planNo].methodNoMap) <= methodNo) {
        OOSQL_ERR_EXIT( eINVALID_METHODNO_OOSQL);
    }
#endif

    usedMethodPoolElements = m_accessPlanElements[planNo].usedMethodInfo;

    mappedMethodNo = getMethodNoMapPool(planNo)[methodNo].offset;

    return &usedMethodPoolElements[mappedMethodNo];
}


inline AP_UsedMethodPoolElements OOSQL_AccessPlan::getUsedMethodPool( 
    Four    planNo
)
/*
    Function:
        Return pointer to UsedMethodPool for (planNo).

    Side effect:

    Return value:
        Pointer to an UsedMethodPool.
*/
{
    AP_UsedMethodPoolElements usedMethodPoolElements(*usedMethodPool);

#ifdef OOSQL_DEBUG
    // check input parameter
    if (planNo < 0 || m_nAP_Elem <= planNo) {
        OOSQL_ERR_EXIT( eINVALID_PLANNO_OOSQL);
    }
#endif

    usedMethodPoolElements = m_accessPlanElements[planNo].usedMethodInfo;

    return usedMethodPoolElements;
}


inline AP_ColNoMapPoolElements OOSQL_AccessPlan::getColNoMapPool( 
    Four    planNo
)
/*
    Function:
        Return pointer to ColNoMapPool for (planNo).

    Side effect:

    Return value:
        Pointer to an ColNoMapPool.
*/
{
    AP_ColNoMapPoolElements colNoMapPoolElements(*colNoMapPool);

#ifdef OOSQL_DEBUG
    // check input parameter
    if (planNo < 0 || m_nAP_Elem <= planNo) {
        OOSQL_ERR_EXIT( eINVALID_PLANNO_OOSQL);
    }
#endif

    colNoMapPoolElements = m_accessPlanElements[planNo].colNoMap;

    return colNoMapPoolElements;
}

inline AP_MethodNoMapPoolElements OOSQL_AccessPlan::getMethodNoMapPool( 
    Four    planNo
)
/*
    Function:
        Return pointer to MethodNoMapPool for (planNo).

    Side effect:

    Return value:
        Pointer to an MethodNoMapPool.
*/
{
    AP_MethodNoMapPoolElements      methodNoMapPoolElements(*methodNoMapPool);

    // check input parameter
#ifdef OOSQL_DEBUG
    if (planNo < 0 || m_nAP_Elem <= planNo) {
        OOSQL_ERR_EXIT( eINVALID_PLANNO_OOSQL);
    }
#endif

    methodNoMapPoolElements = m_accessPlanElements[planNo].methodNoMap;

    return methodNoMapPoolElements;
}


inline AP_CondListPoolElements OOSQL_AccessPlan::getWhereCondPool(
    Four    planNo          // IN: access plan element for condition list pool
)
/*
    Function:
        Return pointer to condition list pool for 'planNo'-th access plan element.

    Side effect:

    Return value:
        Pointer to a condition list pool.
*/
{
    AP_CondListPoolElements condListPoolElements(*condListPool);

    condListPoolElements = m_accessPlanElements[planNo].condNodes;

    return condListPoolElements;
}


inline AP_CondListPoolElements OOSQL_AccessPlan::getHavingCondPool(
    Four    planNo          // IN: access plan element for condition list pool
)
/*
    Function:
        Return pointer to condition list pool for 'planNo'-th access plan element.

    Side effect:

    Return value:
        Pointer to a condition list pool.
*/
{
    AP_CondListPoolElements condListPoolElements(*condListPool);

    condListPoolElements = m_accessPlanElements[planNo].havCondNodes;

    return condListPoolElements;
}


inline AP_ExprPoolElements OOSQL_AccessPlan::getExprPool( 
    AP_ExprPoolIndex   exprPoolIdx
)
/*
    Function:
        Return pointer to ExprPool for exprPoolIdx.

    Side effect:

    Return value:
        Pointer to an ExprPool.
*/
{
    AP_ExprPoolElements     exprPoolElements(*exprPool);

    /* check input parameter */
#ifdef OOSQL_DEBUG
    if (exprPoolIdx.startIndex < 0) {
        OOSQL_ERR_EXIT(eINVALID_POOLINDEX_OOSQL);
	}
#endif

	if(exprPool->nElements() <= exprPoolIdx.startIndex)
		exprPoolElements.setNull();
	else
		exprPoolElements = exprPoolIdx;

    return exprPoolElements;
}

inline AP_ExprElement* OOSQL_AccessPlan::getExprElem( 
    AP_ExprPoolIndex   exprPoolIdx
)
/*
    Function:
        Return pointer to ExprPool for exprPoolIdx.

    Side effect:

    Return value:
        Pointer to an ExprPool.
*/
{
    AP_ExprPoolElements     exprPoolElements(*exprPool);

    /* check input parameter */
#ifdef OOSQL_DEBUG
    if (exprPoolIdx.startIndex < 0) {
        OOSQL_ERR_EXIT( eINVALID_POOLINDEX_OOSQL);
	}
#endif

	if(exprPool->nElements() <= exprPoolIdx.startIndex)
		exprPoolElements.setNull();
	else
		exprPoolElements = exprPoolIdx;

    return &exprPoolElements[0];
}


inline SubClassPoolElements OOSQL_AccessPlan::getSubClassPool( 
    Four    planNo
)
/*
    Function:
        Return pointer to SubClassPool for subClassPoolIdx.

    Side effect:

    Return value:
        Pointer to an SubClassPool.
*/
{
    SubClassPoolElements    subClassPoolElements(*subClassPool);

    /* check input parameter */
#ifdef OOSQL_DEBUG
    if (planNo < 0) {
        OOSQL_ERR_EXIT( eINVALID_POOLINDEX_OOSQL);
    }
#endif

	if(subClassPool->nElements() <= planNo)
		subClassPoolElements.setNull();
	else
		subClassPoolElements = m_accessPlanElements[planNo].subClassInfo;

    return subClassPoolElements;
}


inline AP_AggrFuncPoolElements OOSQL_AccessPlan::getAggrFuncPool( 
    Four    planNo
)
/*
    Function:
        Return pointer to AggrFuncPool for aggrFuncPoolIdx.

    Side effect:

    Return value:
        Pointer to an AggrFuncPool.
*/
{
    AP_AggrFuncPoolElements aggrFuncPoolElements(*aggrFuncPool);

    /* check input parameter */
#ifdef OOSQL_DEBUG
    if (planNo < 0 || m_nAP_Elem <= planNo) {
        OOSQL_ERR_EXIT( eINVALID_POOLINDEX_OOSQL);
    }
#endif

	if(planNo < 0 || m_nAP_Elem <= planNo)
		aggrFuncPoolElements.setNull();
	else
		aggrFuncPoolElements = m_accessPlanElements[planNo].aggrFuncInfo;

    return aggrFuncPoolElements;
}

inline AP_AggrFuncElement* OOSQL_AccessPlan::getAggrFuncElem( 
    AP_AggrFuncPoolIndex aggrFuncPoolIdx
)
/*
    Function:
        Return pointer to AggrFuncPool for aggrFuncPoolIdx.

    Side effect:

    Return value:
        Pointer to an AggrFuncPool.
*/
{
    AP_AggrFuncPoolElements aggrFuncPoolElements(*aggrFuncPool);

    /* check input parameter */
#ifdef OOSQL_DEBUG
    if (aggrFuncPoolIdx.startIndex < 0) {
        OOSQL_ERR_EXIT( eINVALID_POOLINDEX_OOSQL);
	}
#endif

	if(aggrFuncPool->nElements() <= aggrFuncPoolIdx.startIndex)
		aggrFuncPoolElements.setNull();
	else
		aggrFuncPoolElements = aggrFuncPoolIdx;

    return &aggrFuncPoolElements[0];
}


inline AP_AggrFuncElement* OOSQL_AccessPlan::getAggrFuncElem( 
    Four    planNo,
    Four    aggrFuncIdx
)
/*
    Function:
        Return pointer to AggrFuncPool for aggrFuncPoolIdx.

    Side effect:

    Return value:
        Pointer to an AggrFuncPool.
*/
{
    AP_AggrFuncPoolElements aggrFuncPoolElements(*aggrFuncPool);

    /* check input parameter
    */

    aggrFuncPoolElements = getAggrFuncPool(planNo);

    return &aggrFuncPoolElements[aggrFuncIdx];
}


inline AP_ProjectionListPoolElements OOSQL_AccessPlan::getProjectionListPool(
    Four    planNo
)
/*
    Function:

    Side effect:

    Return value:
*/
{
    AP_ProjectionListPoolElements   projListPool(*projectionListPool);

    projListPool = m_accessPlanElements[planNo].projectionList;

    return projListPool;
}


inline AP_ProjectionPoolElements OOSQL_AccessPlan::getProjectionPool( 
    AP_ProjectionPoolIndex  projPoolIndex
)
/*
    Function:
        Return pointer to ProjectionPool for 'planNo'-th access plan element.

    Side effect:

    Return value:
        Pointer to an projection pool element.
*/
{
	AP_ProjectionPoolElements       projPool(*projectionPool);
	
	if(projPoolIndex.startIndex < 0 || projectionPool->nElements() <= projPoolIndex.startIndex)
		projPool.setNull();
	else
		projPool = projPoolIndex;

	return projPool;
}


inline AP_ProjectionElement* OOSQL_AccessPlan::getProjectionElem( 
    AP_ProjectionPoolIndex projPoolIdx      // IN: projection pool index
)
/*
    Function:
        Return pointer to ProjectionPool for projPoolIdx.

    Side effect:

    Return value:
        Pointer to an projection pool element.
*/
{
    AP_ProjectionPoolElements       projectionPoolElements(*projectionPool);

	if(projPoolIdx.startIndex < 0 || projectionPool->nElements() <= projPoolIdx.startIndex)
		projectionPoolElements.setNull();
	else
		projectionPoolElements = projPoolIdx;

    return &projectionPoolElements[0];
}

inline AP_ArgumentPoolElements OOSQL_AccessPlan::getArgumentPool(
    AP_ArgumentPoolIndex       argPoolIndex
)
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
    AP_ArgumentPoolElements argumentPoolElements(*argumentPool);

    argumentPoolElements = argPoolIndex;

    return argumentPoolElements;
}


inline AP_ArgumentElement* OOSQL_AccessPlan::getArgumentElem(
    AP_ArgumentPoolIndex       argIndex
)
/*
    Function:

    Side effect:

    Return value:
*/
{
	AP_ArgumentPoolElements argumentPoolElements(*argumentPool);

	argumentPoolElements = argIndex;

	return &argumentPoolElements[0];
}


inline ValueElement* OOSQL_AccessPlan::getValueElem(
    ValuePoolIndex  valPoolIdx      // IN: value pool index
)
/*
    Function:
        Return reference to value pool for valPoolIdx.

    Side effect:

    Return value:
        Pointer to an value pool element.
*/
{
    ValuePoolElements       valuePoolElements(*valuePool);

    valuePoolElements = valPoolIdx;

    return &valuePoolElements[0];
}


inline long OOSQL_AccessPlan::getIntValue(
        IntegerPoolIndex        intPoolIdx      // IN: integer pool index
)
/*
    Function:
        Return reference to integer pool for intPoolIdx.

    Side effect:

    Return value:
        Pointer to an integer pool element.
*/
{
    IntegerPoolElements     intPoolElements(*intPool);
	
    intPoolElements = intPoolIdx;

    return intPoolElements[0];
}


inline float OOSQL_AccessPlan::getRealValue(
        RealPoolIndex   realPoolIdx     // IN: real pool index
)
/*
    Function:
        Return reference to real pool for intPoolIdx.

    Side effect:

    Return value:
        Pointer to an real pool element.
*/
{
	RealPoolElements        realPoolElements(*realPool);

	realPoolElements = realPoolIdx;

	return realPoolElements[0];
}

inline StringPoolElements OOSQL_AccessPlan::getStringPool(
        StringPoolIndex strPoolIdx      // IN: string pool index
)
/*
    Function:
        Return reference to string pool for intPoolIdx.

    Side effect:

    Return value:
        Pointer to an string pool element.
*/
{
    StringPoolElements      strPoolElements(*stringPool);

    strPoolElements = strPoolIdx;

    return strPoolElements;
}


inline AP_IndexInfoElement* OOSQL_AccessPlan::getIndexInfoElem(
        AP_IndexInfoPoolIndex   indexInfo
)
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
    AP_IndexInfoPoolElements indexInfoPoolElements(*indexInfoPool);

    indexInfoPoolElements = indexInfo;

    return &indexInfoPoolElements[0];
}

inline Four OOSQL_AccessPlan::getIndexInfoPoolSize()
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
    return indexInfoPoolSize;
}


inline AP_TextIndexCondPoolElements OOSQL_AccessPlan::getTextIndexCondPool(
    AP_TextIndexCondPoolIndex textIndexInfo
)
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
    AP_TextIndexCondPoolElements poolElements(*textIndexCondPool);

    poolElements = textIndexInfo;

    return poolElements;
}

inline AP_TextIndexCondElement* OOSQL_AccessPlan::getTextIndexCondElem(
    AP_TextIndexCondPoolIndex textIndexInfo
)
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
    AP_TextIndexCondPoolElements poolElements(*textIndexCondPool);

    poolElements = textIndexInfo;

    return &poolElements[0];
}

inline Four OOSQL_AccessPlan::getTextIndexCondPoolSize()
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
    return textIndexCondPoolSize;
}

inline Four OOSQL_AccessPlan::getValue(
    ValuePoolIndex  valueInfo,
    OOSQL_DB_Value  *value
)
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
    ValueElement          *valueElem;
    
    /* check input paramters */
#ifdef  OOSQL_DEBUG
    if (IS_NULL_POOLINDEX(valueInfo) == SM_TRUE)
            OOSQL_ERR(eBADPARAMETER_OOSQL);
    if (value == NULL)
            OOSQL_ERR(eBADPARAMETER_OOSQL);
#endif

    /* get pointer to value pool element */
    valueElem = getValueElem(valueInfo);

    /* get value according to its type */
    switch (valueElem->valueKind) {
        case VALUE_KIND_INTEGER:
            value->type   = OOSQL_TYPE_LONG_VAR;
#ifndef SUPPORT_LARGE_DATABASE2
            value->data.l = getIntValue( valueElem->integer );
#else
            value->data.ll = getIntValue( valueElem->integer );
#endif
            break;

        case VALUE_KIND_REAL:
            value->type   = OOSQL_TYPE_DOUBLE;
            value->data.d = getRealValue( valueElem->real );
            break;

        case VALUE_KIND_STRING:
        case VALUE_KIND_NIL:
        case VALUE_KIND_BOOL:
        case VALUE_KIND_MBR:
        default:
            OOSQL_ERR(eINVALID_CASE_OOSQL);
    }

    /* return */
    return eNOERROR;
}

inline const char* OOSQL_AccessPlan::getStringValue(
        StringPoolIndex strIdx
)
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
    StringPoolElements      strPoolElements(*stringPool);

    strPoolElements = strIdx;

    return (const char*) &strPoolElements[0];
}

inline Four OOSQL_AccessPlan::getDateValue(
    DatePoolIndex					datePoolIdx,
	OOSQL_StorageManager::Date*     date
)
{
    DatePoolElements datePoolElements(*datePool);

    datePoolElements = datePoolIdx;

    m_storageManager->SetDate(datePoolElements[0].year,
                              datePoolElements[0].month,
                              datePoolElements[0].day,
                              date);
    
    return eNOERROR;
}

inline Four OOSQL_AccessPlan::getTimeValue(
    TimePoolIndex						timePoolIdx,
    OOSQL_StorageManager::Time*         time
)
{
    TimePoolElements timePoolElements(*timePool);

	timePoolElements = timePoolIdx;

    time->_tzHour   = 0;
	time->_tzMinute = 0;
	time->_100thSec = 0;
	time->_Hour   = timePoolElements[0].hour;
	time->_Minute = timePoolElements[0].minute;
	time->_Second = timePoolElements[0].second;

    return eNOERROR;
}

inline Four OOSQL_AccessPlan::getTimestampValue(
    TimestampPoolIndex  timestampPoolIdx,
    OOSQL_StorageManager::Timestamp*      timestamp
)
{
    TimestampPoolElements timestampPoolElements(*timestampPool);

	timestampPoolElements = timestampPoolIdx;

    m_storageManager->SetDate(timestampPoolElements[0].year,
                              timestampPoolElements[0].month,
                              timestampPoolElements[0].day,
                              &timestamp->d);
    timestamp->t._tzHour   = 0;
	timestamp->t._tzMinute = 0;
	timestamp->t._100thSec = 0;
	timestamp->t._Hour     = timestampPoolElements[0].hour;
	timestamp->t._Minute   = timestampPoolElements[0].minute;
	timestamp->t._Second   = timestampPoolElements[0].second;

    return eNOERROR;
}

inline Four OOSQL_AccessPlan::getIntervalValue(
    IntervalPoolIndex   intervalPoolIdx,
    OOSQL_StorageManager::Interval*       interval
)
{
    IntervalPoolElements intervalPoolElements(*intervalPool);

    OOSQL_ERR(eNOTIMPLEMENTED_OOSQL);
        
    return eNOERROR;
}


inline Four    OOSQL_AccessPlan::getNumAP_Elem()
/*
    Function:

    Side effect:

    Return value:
*/
{
    return m_nAP_Elem;
}


inline Four OOSQL_AccessPlan::getMappedColNo(
    Four    planNo,
    Four    colNo
)
/*
    Function:

    Side effect:

    Return value:
*/
{
    AP_ColNoMapPoolElements colNoMap(*colNoMapPool);

    // check input parameter */
#ifdef OOSQL_DEBUG
    if (planNo < 0 || m_nAP_Elem <= planNo) {
		OOSQL_ERR_EXIT(eINVALID_POOLINDEX_OOSQL);
    }
#endif

    colNoMap = getColNoMapPool( planNo );

    return colNoMap[colNo].offset;
}


inline Four OOSQL_AccessPlan::getNumOfQueryResCols()
/*
    Function:
        Get the # of query result columns.

    Side effect:

    Return value:
        The # of query result columns.
*/
{
    AP_ProjectionListPoolElements projList(*projectionListPool);
    Four    nCols;

	if(m_nAP_Elem > 0)
    {
		projList = m_accessPlanElements[m_nAP_Elem - 1].projectionList;
		if(projList == NULL_POOLINDEX)
			nCols = 0;
		else
			nCols = GET_POOLSIZE( projList[0].projectionInfo );
	}
	else
		nCols = 0;

    return nCols;
}

inline Four OOSQL_AccessPlan::getMBRValue(
    MBRPoolIndex        mbrIdx,
    Four                dim
)
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
    MBRPoolElements mbrPoolElements(*mbrPool);

    mbrPoolElements = mbrIdx;

    if(dim == 0)
        return getIntValue(mbrPoolElements[0].x1);
    else if(dim == 1)
        return getIntValue(mbrPoolElements[0].y1);
    else if(dim == 2)
        return getIntValue(mbrPoolElements[0].x2);
    else /* dim == 3 */
        return getIntValue(mbrPoolElements[0].y2);
}

inline Four OOSQL_AccessPlan::getQueryResultInfo(
    OOSQL_ResultInfo *resultInfo    // IN/OUT: ptr. to query result information
)
/*
    Function:
        Get query result information from access plan.

        NOTE: Query result consists of the result of final projection.
              So, query result information is constructed from the projection information
              of last access plan element.

    Side effect:

    Return value:
*/
{
    AP_ProjectionListPoolElements projListPool;
    AP_ProjectionPoolElements pProjection;  // ptr. to query result projection info. pool
    Four                    fixedColOffset;
    Four                    i;              // loop iteration variable

    /* check input parameter
     * NOTE: 'nCols' field is already initialized by constructor (or new operator)
     */
    if ( resultInfo->nCols != getNumOfQueryResCols() ) {
        OOSQL_ERR( eINSUFFICIENT_BUFSIZE_OOSQL);
    }

    // set pointer to projection pool
	if(m_nAP_Elem == 0)
		projListPool = NULL_POOLINDEX;
	else
		projListPool = getProjectionListPool( m_nAP_Elem - 1 );

	if(projListPool != NULL_POOLINDEX)
	{
		pProjection = getProjectionPool( projListPool[0].projectionInfo );

		// initialize local variables
		resultInfo->nVarCols = 0;
		resultInfo->firstVarColOffset = 0;
		for (i = 0; i < resultInfo->nCols; i++) {
			if (pProjection[i].resultType == OOSQL_TYPE_varstring) {
				resultInfo->nVarCols ++;
			}
			else {
				resultInfo->firstVarColOffset += pProjection[i].resultLength;
			}
		}

		fixedColOffset = GET_TUPLEHEADERSIZE(resultInfo->nVarCols);

		for (i = 0; i < resultInfo->nCols; i++) {
			resultInfo->colInfo[i].typeId = pProjection[i].resultType;
			resultInfo->colInfo[i].length = pProjection[i].resultLength;
			if (resultInfo->colInfo[i].typeId == OOSQL_TYPE_varstring) {
				resultInfo->nVarCols ++;
			}
			else {
				resultInfo->colInfo[i].offset = fixedColOffset;
				fixedColOffset += resultInfo->colInfo[i].length;
			}
		}
	}
	else
	{
		resultInfo->nVarCols = 0;
		resultInfo->firstVarColOffset = 0;
	}

    return eNOERROR;
}


inline Four OOSQL_AccessPlan::getQueryResultInfoByString(
    Four    resultInfoLength,       // IN: byte length of 'resultInfo'
    char    *resultInfo,            // IN/OUT: query result information
    Four    *actualResultInfoLength // OUT: actual byte length of 'resultInfo'
)
/*
    Function:
        Get query result information from access plan.

        NOTE: Query result consists of the result of final projection.
              So, query result information is constructed from the projection information
              of last access plan element.

    Side effect:

    Return value:
*/
{
    AP_ProjectionListPoolElements projListPool;
    AP_ProjectionPoolElements pProjection;
    Four                    nCols;
    Four                    nVarCols;
    char                    *pResInfo;
    Four                    fixedColOffset;
    Four                    firstVarColOffset;
    Four                    i;

    if ( resultInfoLength < getQueryResultInfoLengthInChar() ) {
        *actualResultInfoLength = getQueryResultInfoLengthInChar();
        OOSQL_ERR( eINSUFFICIENT_BUFSIZE_OOSQL);
    }

    // set pointer to the projection information
	if(m_nAP_Elem > 0)
		projListPool = getProjectionListPool( m_nAP_Elem - 1 );
	else
		projListPool = NULL_POOLINDEX;

	if(projListPool != NULL_POOLINDEX)
	{
		pProjection = getProjectionPool( projListPool[0].projectionInfo );

		nCols = GET_POOLSIZE( pProjection );

		// set the # of result columns
		pResInfo = resultInfo;
		memcpy( pResInfo, &nCols, sizeof(Four) );

		// calculate the # of variable sized columns
		nVarCols = 0;
		for (i = 0; i < nCols; i++) {
			if (pProjection[i].resultType == OOSQL_TYPE_varstring) {
				nVarCols ++;
			}
		}
		// set 'nVarCols' field
		pResInfo += sizeof(Four);
		memcpy( pResInfo, &nVarCols, sizeof(Four) );

		// calculate 'firstVarColOffset'
		firstVarColOffset = GET_TUPLEHEADERSIZE(nVarCols);
		for (i = 0; i < nCols; i++) {
			if (pProjection[i].resultType != OOSQL_TYPE_varstring) {
				firstVarColOffset += pProjection[i].resultLength;
			}
		}
		// set 'firstVarColOffset' field
		pResInfo += sizeof(Four);
		memcpy( pResInfo, &firstVarColOffset, sizeof(Four) );

		// set (type, length, offset) information for each column
		fixedColOffset = GET_TUPLEHEADERSIZE(nVarCols);
		for (i = 0; i < nCols; i++) {
			// set 'typeId' field of OOSQL_ResultColInfo
			pResInfo += sizeof(Four);
			memcpy( pResInfo, &(pProjection[i].resultType), sizeof(Four) );

			// set 'length' field of OOSQL_ResultColInfo
			pResInfo += sizeof(Four);
			memcpy( pResInfo, &(pProjection[i].resultLength), sizeof(Four) );

			// set 'offset' field of OOSQL_ResultColInfo
			pResInfo += sizeof(Four);
			if (pProjection[i].resultType == OOSQL_TYPE_varstring) {
				memset( pResInfo, NULL, sizeof(Four) );
			}
			else {
				memcpy( pResInfo, &fixedColOffset, sizeof(Four) );
				fixedColOffset += pProjection[i].resultLength;
			}
		}
	}
	else
	{
		nCols             = 0;
		nVarCols          = 0;
		firstVarColOffset = 0;

		pResInfo = resultInfo;
		memcpy( pResInfo, &nCols, sizeof(Four) );
		pResInfo += sizeof(Four);
		memcpy( pResInfo, &nVarCols, sizeof(Four) );
		pResInfo += sizeof(Four);
		memcpy( pResInfo, &firstVarColOffset, sizeof(Four) );
	}

	*actualResultInfoLength = getQueryResultInfoLengthInChar();

    return eNOERROR;
}


inline Four OOSQL_AccessPlan::getQueryResultInfoLengthInChar()
/*
    Function:

    Side effect:

    Return value:
*/
{
    AP_ProjectionListPoolElements   projList;
    Four                            nCols;

	if(m_nAP_Elem > 0)
	{
		projList = getProjectionListPool( m_nAP_Elem - 1 );

		nCols = GET_POOLSIZE( projList[0].projectionInfo );
	}
	else
	{
		nCols = 0;
	}

	return OOSQL_ResultInfo::getResultInfoLengthInChar(nCols);
}


#endif

