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
        OOSQL_AccessPlan.C

    DESCRIPTION:
        This module implements member functions of class OOSQL_AccessPlan.

    IMPORTS:

    EXPORTS:
*/

#include "OOSQL_AccessPlan.hxx"
#include <string.h>

OOSQL_AccessPlan::OOSQL_AccessPlan(
    OOSQL_AP_PoolElements apPoolElements,       // index to access plan element pool
    OOSQL_StorageManager* storageManager,
    OOSQL_Catalog*      catalog,
    AP_CondListPool     *pCondListPool,         // ptr. to condition list pool
    Four                in_CondListPoolSize,
    AP_ExprPool         *pExprPool,             // ptr. to expression pool
    Four                in_ExprPoolSize,
    SubClassPool        *pSubClassPool,         // ptr. to subclass info. pool
    Four                in_SubClassPoolSize,
    AP_AggrFuncPool     *pAggrFuncPool,         // ptr. to aggregate function info. pool
    Four                in_AggrFuncPoolSize,
    AP_ColNoMapPool     *pColNoMapPool,         // ptr. to column no. mapping pool
    Four                in_ColNoMapPoolSize,
    AP_UsedColPool      *pUsedColPool,          // ptr. to used column info. pool
    Four                in_UsedColPoolSize,
    AP_MethodNoMapPool  *pMethodNoMapPool,      // ptr. to method no. mapping pool
    Four                in_MethodNoMapPoolSize,
    AP_UsedMethodPool   *pUsedMethodPool,       // ptr. to used method info. pool
    Four                in_UsedMethodPoolSize,
    AP_ProjectionListPool *pProjectionListPool, // ptr. to projection info. pool
    Four                in_ProjectionListPoolSize,
    AP_ProjectionPool   *pProjectionPool,       // ptr. to projection info. pool
    Four                in_ProjectionPoolSize,
    AP_ArgumentPool     *pArgumentPool,         // ptr. to argument info. pool
    Four                in_ArgumentPoolSize,
    ValuePool           *pValuePool,            // ptr. to value pool
    Four                in_ValuePoolSize,
    IntegerPool         *pIntPool,              // ptr. to integer constant pool
    Four                in_IntPoolSize,
    RealPool            *pRealPool,             // ptr. to real constant pool
    Four                in_RealPoolSize,
    StringPool          *pStrPool,              // ptr. to string constant pool
    Four                in_StrPoolSize,
    FunctionPool        *pFuncPool,             // ptr. to function pool
    Four                in_FuncPoolSize,
    CollectionPool      *pCollectionPool,       // ptr. to collection constant pool
    Four                in_CollectionPoolSize,
    DomainPool          *pDomainPool,           // ptr. to domain pool
    Four                in_DomainPoolSize,
    MBRPool             *pMBR_Pool,             // ptr. to MBR pool
    Four                in_MBR_PoolSize,
    MemberPool          *pMemberPool,           // ptr. to member pool
    Four                in_MemberPoolSize,
    AP_IndexInfoPool    *pIndexInfoPool,          // ptr. to index info pool
    Four                in_IndexInfoPoolSize,
    AP_TextIndexCondPool *pTextIndexCondPool,      // ptr. to text index condition pool
    Four                in_TextIndexCondPoolSize,
    DatePool            *pDatePool,
    Four                in_DatePoolSize,
    TimePool            *pTimePool,
    Four                in_TimePoolSize,
    TimestampPool       *pTimestampPool,
    Four                in_TimestampPoolSize,
    IntervalPool        *pIntervalPool,
    Four                in_IntervalPoolSize,
	AP_InsertValuePool*	pInsertValuePool,	
	Four				in_InsertValuePoolSize,
	AP_UpdateValuePool*	pUpdateValuePool,	
	Four				in_UpdateValuePoolSize
)
/*
    Function:
        Constructor.

    Side effect:
        This OOSQL_AccessPlan object is initialized with pointers to an array of access plan elem.
        and pools containing data for evaluation.
*/
{

    m_nAP_Elem				= GET_POOLSIZE(apPoolElements);
	m_storageManager		= storageManager;
	m_catalog				= catalog;
    m_accessPlanElements	= &apPoolElements[0];    // index to pool of access plan elements 

    condListPool = pCondListPool;           // ptr. to condition(disjunct) list pool
    condListPoolSize = in_CondListPoolSize; // size of condition(disjunct) list pool
    exprPool = pExprPool;           // ptr. to expression pool
    exprPoolSize = in_ExprPoolSize; // size of expression pool
    subClassPool = pSubClassPool;           // ptr. to subclass info. pool
    subClassPoolSize = in_SubClassPoolSize; // size of subclass info. pool
    aggrFuncPool = pAggrFuncPool;           // ptr. to aggregate function info. pool
    aggrFuncPoolSize = in_AggrFuncPoolSize; // size of aggregate function info. pool
    colNoMapPool = pColNoMapPool;           // ptr. to column no. mapping pool
    colNoMapPoolSize = in_ColNoMapPoolSize; // size of column no. mapping pool
    usedColPool = pUsedColPool;             // ptr. to used column info. pool
    usedColPoolSize = in_UsedColPoolSize;   // size of used column info. pool
    methodNoMapPool = pMethodNoMapPool;     // ptr. to method no. mapping pool
    methodNoMapPoolSize = in_MethodNoMapPoolSize;   // size of method no. mapping pool
    usedMethodPool = pUsedMethodPool;       // ptr. to used method info. pool
    usedMethodPoolSize = in_UsedMethodPoolSize;     // size of used method info. pool
    projectionListPool = pProjectionListPool; // ptr. to projection info. pool
    projectionListPoolSize = in_ProjectionListPoolSize; // size of projection info. pool
    projectionPool = pProjectionPool;       // ptr. to projection info. pool
    projectionPoolSize = in_ProjectionPoolSize;     // size of projection info. pool

    argumentPool = pArgumentPool;           // ptr. to argument info. pool
    argumentPoolSize = in_ArgumentPoolSize; // size of argument info. pool
    valuePool = pValuePool;                 // ptr. to value pool
    valuePoolSize = in_ValuePoolSize;       // size of value pool
    intPool = pIntPool;						// ptr. to integer constant pool
    intPoolSize = in_IntPoolSize;			// size of integer constant pool
    realPool = pRealPool;                   // ptr. to real constant pool
    realPoolSize = in_RealPoolSize;         // size of real constant pool
    stringPool = pStrPool;					// ptr. to string constant pool
    stringPoolSize = in_StrPoolSize;        // size of string constant pool
    funcPool = pFuncPool;					// ptr. to function pool
    funcPoolSize = in_FuncPoolSize;         // size of function pool
    collectionPool = pCollectionPool;       // ptr. to collection constant pool
    collectionPoolSize = in_CollectionPoolSize;     // size of collection constant pool
    domainPool = pDomainPool;               // ptr. to domain pool
    domainPoolSize = in_DomainPoolSize;     // size of domain pool
    mbrPool = pMBR_Pool;					// ptr. to MBR pool
    mbrPoolSize = in_MBR_PoolSize;			// size of MBR pool
    memberPool = pMemberPool;               // ptr. to member pool
    memberPoolSize = in_MemberPoolSize;     // size of member pool

    indexInfoPool			= pIndexInfoPool;			// ptr. to index info pool
    indexInfoPoolSize		= in_IndexInfoPoolSize;		// size of index info pool
    textIndexCondPool		= pTextIndexCondPool;		// ptr. to text index condition pool
    textIndexCondPoolSize	= in_TextIndexCondPoolSize;

    datePool			= pDatePool;
    datePoolSize		= in_DatePoolSize;
    timePool			= pTimePool;
    timePoolSize		= in_TimePoolSize;
    timestampPool		= pTimestampPool;
    timestampPoolSize	= in_TimestampPoolSize;
    intervalPool		= pIntervalPool;
    intervalPoolSize	= in_IntervalPoolSize;
	insertValuePool		= pInsertValuePool;
	insertValuePoolSize = in_InsertValuePoolSize;
	updateValuePool		= pUpdateValuePool;
	updateValuePoolSize = in_UpdateValuePoolSize;
}






