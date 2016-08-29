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
        OOSQL_Evaluator.C

    DESCRIPTION:
        This module implements the evaluation module of OOSQL processor
        which gets an access plan as its input and produces a set of query result tuples.

    IMPORTS:

    EXPORTS:

*/

#include "OOSQL_Evaluator.hxx"
#include "oosql_Eval_Control.hxx"
#include "oosql_Eval_Access.hxx"
#include "OOSQL_MultipleResultBuffer.hxx"
#include "oosql_Eval_Expr.hxx"
#include <string.h>

OOSQL_Evaluator::OOSQL_Evaluator(
	OOSQL_StorageManager*				storageManager,
	OOSQL_Catalog*						catalog,
	OOSQL_ErrorMessage*					errorMessage,
	OOSQL_ExternalFunctionManager*		externalFunctionManager,
	OOSQL_ExternalFunctionDispatcher*	externalFunctionDispatcher,
    OOSQL_StorageManager::XactID		transactionId,	
    Four								volumeId          
	): m_sortBufferMemory(pMemoryManager)
/*
    Function:
        Constructor.

    Side effect:
*/
{
	// initialize storage manager and catalog object
	m_storageManager			 = storageManager;
	m_catalog					 = catalog;
	m_errorMessage				 = errorMessage;
	m_externalFunctionManager	 = externalFunctionManager;
	m_externalFunctionDispatcher = externalFunctionDispatcher;

    // initialize transaction ID and database ID 
    m_transID = transactionId;
    m_volID    = volumeId;

    // initialize status information 
    m_evalStatus.globalStatus		= EVALSTATUS_GLOBAL_INIT;
    m_isCheckingHavingCond			= SM_FALSE;
   
    // initialize pointer variables
    m_accessPlan					= NULL;
    m_evalAccessListTable			= NULL;
    m_evalIndexScanInfoTable		= NULL;
	m_evalIndexScanInfoTableElements= NULL;
    m_evalTempFileInfoTable			= NULL;
	m_evalTempFileInfoTableElements = NULL;
    m_queryResultInfo				= NULL;
    m_evaluationBuffer				= NULL;
    m_evalTextIR_SubPlanEvalBuffer	= NULL;
	m_indexEvaluationBuffer         = NULL;

	m_getoid_nTargetClass           = 0;
	m_getoid_TargetNoToEvalBufferNo = NULL;

	m_getdata_nInfo = 0;
	m_getdata_info  = NULL;
}


OOSQL_Evaluator::~OOSQL_Evaluator()
/*
    Function:
        Destructor.

    Side effect:
*/
{
	if(m_evalAccessListTable)
		OOSQL_ARRAYDELETE(OOSQL_AccessList, m_evalAccessListTable);
	
	if(m_evalIndexScanInfoTable)
		OOSQL_DELETE(m_evalIndexScanInfoTable);
	
	if(m_indexEvaluationBuffer)
		OOSQL_ARRAYDELETE(OOSQL_EvalIndexBuffer, m_indexEvaluationBuffer);

	if(m_evalTempFileInfoTable)
	{
		for(int i = 0; i < EVAL_TEMPFILEINFOTABLE.nTempFiles; i++)
			OOSQL_DELETE(m_evalTempFileInfoTable->tempFileInfo[i]);

		OOSQL_DELETE(m_evalTempFileInfoTable);
	}
	
	if(m_queryResultInfo)
		OOSQL_DELETE(m_queryResultInfo);
	
	if(m_evaluationBuffer)
		OOSQL_ARRAYDELETE(OOSQL_EvalBuffer, m_evaluationBuffer);
	
	if(m_evalTextIR_SubPlanEvalBuffer)
		OOSQL_DELETE(m_evalTextIR_SubPlanEvalBuffer);

	if(m_getoid_TargetNoToEvalBufferNo)
		pMemoryManager->Free(m_getoid_TargetNoToEvalBufferNo);

	if(m_getdata_info)
		pMemoryManager->Free(m_getdata_info);
}


Four    OOSQL_Evaluator::Prepare(
    OOSQL_AccessPlan        *pAccessPlan,   // IN: input access plan
	OQL_GDSPOOL&			apool,			// IN: m_pool
	OOSQL_SortBufferInfo	*sortBufferInfo,// IN: TextIR Sort Buffer Information
	OOSQL_QueryType			queryType
)
/*
    Function:
        Prepare evaluation with the input access plan.

    Side effect:

    Return value:
*/
{
    Four								e;              // error code
	static Boolean						alwaysFalse = SM_FALSE;
	static Four							oidSize = sizeof(OOSQL_StorageManager::OID);
	static OOSQL_StorageManager::OID	nullOid;

	/* set pointer to the input access plan(including various m_pool structures)
     *
     * NOTE: OOSQL_Evaluator MUST deallocate the memory for the access plan,
     *       because callee does not care the access plan after it passed
     *       the constructed access plan.
     */
    m_accessPlan			= pAccessPlan;
	m_pool					= &apool;
	m_queryType				= queryType;
	m_accessPlanElements	= m_accessPlan->m_accessPlanElements;

	m_memorySizeNeededForStarQuery = 0;
	if(sortBufferInfo == NULL)
	{
		m_sortBufferInfo.mode = OOSQL_SB_USE_DISK;
		m_sortBufferInfo.diskInfo.sortVolID = m_volID;
	}
	else
	{
		m_sortBufferInfo								  = *sortBufferInfo;
		m_sortBufferInfo.memoryInfo.sortBufferUsedLength  = 0;

		if(m_sortBufferInfo.mode == OOSQL_SB_USE_MEMORY ||
		   m_sortBufferInfo.mode == OOSQL_SB_USE_MEMORY_WITH_DISK)
		{
			e = m_sortBufferMemory.SetupUserMemory(m_sortBufferInfo.memoryInfo.sortBufferPtr, 
				                                   m_sortBufferInfo.memoryInfo.sortBufferLength);
			OOSQL_CHECK_ERR(e);

#ifndef SLIMDOWN_TEXTIR
			e = execTextIR_GetMemorySizeToSortPosting(m_memorySizeNeededForStarQuery);
			if(e < eNOERROR) OOSQL_ERR(e);
#endif

			if(m_sortBufferInfo.memoryInfo.sortBufferLength < m_memorySizeNeededForStarQuery)
			{
				if(m_sortBufferInfo.mode == OOSQL_SB_USE_MEMORY)
				{
					sortBufferInfo->memoryInfo.sortBufferUsedLength = m_memorySizeNeededForStarQuery;
					return eNEEDMORESORTBUFFERMEMORY_OOSQL;	
				}
				else
				{
					m_sortBufferInfo.mode = OOSQL_SB_USE_MEMORY_WITH_DISK;
					m_sortBufferForStarQuery = NULL;
					m_usedSortBufferForStarQuery = 0;
				}
			}
			else
			{
				m_sortBufferForStarQuery = (char*)m_sortBufferMemory.Alloc(m_memorySizeNeededForStarQuery);
				if(!m_sortBufferForStarQuery) OOSQL_ERR(eMEMORYALLOCERR_OOSQL);
				m_usedSortBufferForStarQuery = 0;
			}
		}
		else
		{
			e = m_sortBufferMemory.SetupUserMemory(NULL, 0);
			OOSQL_CHECK_ERR(e);

			m_sortBufferForStarQuery = NULL;
			m_usedSortBufferForStarQuery = 0;
		}
	}

    /* make query result information */
	e = prepareQueryResultInfo();
	if(e < eNOERROR) OOSQL_ERR(e);

	if(ACCESSPLAN.getNumAP_Elem() > 0)
	{
		/* create temporary files used in the access plan */
		e = prepareTempFile();
		if(e < eNOERROR) OOSQL_ERR( e);

		/* allocate evaluation buffer */
		e = prepareEvalBuffer();
		if(e < eNOERROR) OOSQL_ERR(e);

#ifndef SLIMDOWN_TEXTIR
		/* 
		 * allocate Text IR evaluation buffer 
		 */
		e = allocTextIR_SubPlanEvalBuffer();
		if(e < eNOERROR) OOSQL_ERR(e);
#endif		

		/*
		 * allocate memory for the Access List Table and
		 * open scans for each access plan element if necessary 
		 */
		e = initAccessListTable();
		if(e < eNOERROR) OOSQL_ERR(e);
	}
	
	/* set status as READY for evaluation */
	m_evalStatus.globalStatus = EVALSTATUS_GLOBAL_READY;
	m_currPlanIndex = 0;              // index to the 1st access plan element

	if(m_queryType == OOSQL_SELECT_QUERY && m_pool->groupByListPool.nElements() == 0 && 
	   m_pool->orderByListPool.nElements() == 0 && !(m_pool->selectQueryPool[0].selListType & DIST_BIT) &&
	   !(m_pool->selectQueryPool[0].selListType & AGGR_DIST_BIT))
	{
		m_callEvalAndFetchForSelectOnlyFlag = SM_TRUE;
	}
	else
	{
		m_callEvalAndFetchForSelectOnlyFlag = SM_FALSE;
	}

	if(!m_getoid_TargetNoToEvalBufferNo)
	{                                
		if(m_pool->updateQueryPool.nElements() > 0 || m_pool->insertQueryPool.nElements() > 0)
		{
			m_getoid_nTargetClass = 1;

			m_getoid_TargetNoToEvalBufferNo    = (Four*)pMemoryManager->Alloc(sizeof(Four) * m_getoid_nTargetClass);
			m_getoid_TargetNoToEvalBufferNo[0] = ACCESSPLAN.m_nAP_Elem - 1;
		}
		else
		{
			Four classId;
			Four classInfo;
			Four targetNo, i;	// loop variable

			m_getoid_nTargetClass = m_pool->targetListPool.nElements();

			m_getoid_TargetNoToEvalBufferNo = (Four*)pMemoryManager->Alloc(sizeof(Four) * m_getoid_nTargetClass);
			for(targetNo = 0; targetNo < m_getoid_nTargetClass; targetNo++)
			{
				classInfo = m_pool->targetListPool[targetNo].collectionInfo.classInfo;
				e = m_catalog->class_ClassInfo_to_ClassId(classInfo, classId); OOSQL_CHECK_ERR(e);

				for(i = 0; i < ACCESSPLAN.m_nAP_Elem; i++)
				{
					if(ACCESSPLANELEMENTS[i].classInfo.classKind == CLASSKIND_PERSISTENT && 
					   ACCESSPLANELEMENTS[i].classInfo.classId == classId)
					{
						m_getoid_TargetNoToEvalBufferNo[targetNo] = i;
						break;
					}
				}
				if(i == ACCESSPLAN.m_nAP_Elem)
					OOSQL_ERR(eINTERNALERROR_OOSQL); 
			}
		}
	}

	if(m_getdata_info)
		pMemoryManager->Free(m_getdata_info);

	m_getdata_nInfo = 0;
	m_getdata_info  = NULL;

	if(m_queryType == OOSQL_SELECT_QUERY)
	{
		Four							colNo;
		AP_ProjectionListPoolElements	projList;
		AP_ProjectionPoolElements		pProjection;

		projList    = ACCESSPLAN.getProjectionListPool(ACCESSPLAN.m_nAP_Elem - 1);
		pProjection = ACCESSPLAN.getProjectionPool( projList[0].projectionInfo );

		m_getdata_nInfo = pProjection.size;
		m_getdata_info  = (OOSQL_GetDataInfo*)pMemoryManager->Alloc(sizeof(OOSQL_GetDataInfo) * m_getdata_nInfo);
		
		for(colNo = 0; colNo < m_getdata_nInfo; colNo ++)
		{
			Four									projPlanNo;         /* projection plan no. */
			Two										projColNo;          /* projection column no.  */
			EVAL_EvalBufferSlot*					projBufSlot; 
			AP_UsedColPoolElements					pUsedColPool;       /* ptr. to used column m_pool for the current A.P. elem. */
			Four									mappedColNo;        /* mapped column no. for projection col. no. */
			Four									ocn;
			OOSQL_StorageManager::OID*				oid;
			Boolean									grpByFlag;
			OOSQL_DB_Value							result(pMemoryManager);


			projList    = ACCESSPLAN.getProjectionListPool(ACCESSPLAN.m_nAP_Elem - 1);
			pProjection = ACCESSPLAN.getProjectionPool(projList[0].projectionInfo);

			if(m_pool->groupByListPool.nElements() > 0 && m_pool->orderByListPool.nElements() == 0)
				grpByFlag = SM_TRUE;
			else
				grpByFlag = SM_FALSE;

			m_getdata_info[colNo].projectionKind = pProjection[colNo].projectionKind;
			switch(pProjection[colNo].projectionKind) 
			{
			case PROJECTION_KIND_PATHEXPR:
				switch(pProjection[colNo].pathExpr.kind) 
				{
				case PATHEXPR_KIND_OBJECT:
					OOSQL_ERR(eNOTIMPLEMENTED_OBJECTPROJ_OOSQL);

				case PATHEXPR_KIND_OID:
					m_getdata_info[colNo].type		  = OOSQL_TYPE_OID;
					m_getdata_info[colNo].projColNo   = pProjection[colNo].pathExpr.col.planNo;
					m_getdata_info[colNo].oid         = EVAL_EVALBUFFER[pProjection[colNo].pathExpr.col.planNo].getOID_Ptr();
					m_getdata_info[colNo].ocn         = EVAL_ACCESSLISTTABLE[pProjection[colNo].pathExpr.col.planNo].getCurrOcn();
					m_getdata_info[colNo].colNo       = NIL;
					m_getdata_info[colNo].srcPtr      = (char*)EVAL_EVALBUFFER[pProjection[colNo].pathExpr.col.planNo].getOID_Ptr();
					m_getdata_info[colNo].srcLength   = &oidSize;
					m_getdata_info[colNo].nullFlag    = &alwaysFalse; // this means always FALSE
					break;

				case PATHEXPR_KIND_ATTR:
					projPlanNo   = pProjection[colNo].pathExpr.col.planNo;
					projColNo    = pProjection[colNo].pathExpr.col.colNo;
					pUsedColPool = ACCESSPLAN.getUsedColPool(projPlanNo);
					mappedColNo  = ACCESSPLAN.getMappedColNo(projPlanNo, projColNo);
					oid          = EVAL_EVALBUFFER[projPlanNo].getOID_Ptr();
					ocn		 	 = EVAL_ACCESSLISTTABLE[projPlanNo].getCurrOcn();

					if(pProjection[colNo].resultType != OOSQL_TYPE_TEXT)
					{
						if(grpByFlag == SM_TRUE) 
							projBufSlot = EVAL_EVALBUFFER[projPlanNo].getGrpBySlotPtr(mappedColNo);
						else 
							projBufSlot = EVAL_EVALBUFFER[projPlanNo].getColSlotPtr(mappedColNo);
					}

					m_getdata_info[colNo].type        = pProjection[colNo].resultType;
					m_getdata_info[colNo].projColNo   = projColNo;
					m_getdata_info[colNo].oid         = oid;
					m_getdata_info[colNo].ocn         = ocn;
					if(mappedColNo == NOT_USED_COLUMN)
						m_getdata_info[colNo].colNo   = projColNo;
					else
						m_getdata_info[colNo].colNo   = pUsedColPool[mappedColNo].colNo;

					switch(pProjection[colNo].resultType) 
					{
					case OOSQL_TYPE_TEXT:
						m_getdata_info[colNo].srcPtr    = NULL;
						m_getdata_info[colNo].srcLength = NULL;
						m_getdata_info[colNo].nullFlag  = &alwaysFalse; // this means always FALSE
						break;

					case OOSQL_TYPE_MBR:
					case OOSQL_TYPE_SHORT:
					case OOSQL_TYPE_INT:
					case OOSQL_TYPE_LONG:
					case OOSQL_TYPE_LONG_LONG:
					case OOSQL_TYPE_FLOAT:
					case OOSQL_TYPE_DOUBLE:
					case OOSQL_TYPE_OID:
					case OOSQL_TYPE_DATE:
					case OOSQL_TYPE_TIME:
					case OOSQL_TYPE_TIMESTAMP:
					case OOSQL_TYPE_INTERVAL:
					case OOSQL_TYPE_FILEID:
					case OOSQL_TYPE_PAGEID:
					case OOSQL_TYPE_INDEXID:
						m_getdata_info[colNo].srcPtr    = (char*)&(projBufSlot->data.s);
						m_getdata_info[colNo].srcLength = &projBufSlot->retLength;
						m_getdata_info[colNo].nullFlag  = &projBufSlot->nullFlag;
						break;

					case OOSQL_TYPE_GEOMETRY:
					case OOSQL_TYPE_POINT:
					case OOSQL_TYPE_LINESTRING:
					case OOSQL_TYPE_POLYGON:	
					case OOSQL_TYPE_GEOMETRYCOLLECTION:
					case OOSQL_TYPE_MULTIPOINT:
					case OOSQL_TYPE_MULTILINESTRING:
					case OOSQL_TYPE_MULTIPOLYGON:
					case OOSQL_TYPE_VARSTRING:
					case OOSQL_TYPE_STRING:
						m_getdata_info[colNo].srcPtr	= (char*)projBufSlot->data.ptr;
						m_getdata_info[colNo].srcLength = &projBufSlot->retLength;
						m_getdata_info[colNo].nullFlag  = &projBufSlot->nullFlag;
						break;

					default:
						if(OOSQL_MASK_COMPLEXTYPE(m_getdata_info[colNo].type) == OOSQL_COMPLEXTYPE_BASIC)
						{
							OOSQL_ERR(eINVALID_CASE_OOSQL);
						}
						else
						{
							m_getdata_info[colNo].srcPtr    = NULL;
							m_getdata_info[colNo].srcLength = NULL;
							m_getdata_info[colNo].nullFlag  = &alwaysFalse; // this means always FALSE
						}
					}

					break;

				case PATHEXPR_KIND_LOGICALID:
					projPlanNo   = pProjection[colNo].pathExpr.col.planNo;
					projColNo    = pProjection[colNo].pathExpr.col.colNo;
					pUsedColPool = ACCESSPLAN.getUsedColPool(projPlanNo);
					mappedColNo  = ACCESSPLAN.getMappedColNo(projPlanNo, projColNo);
					oid          = EVAL_EVALBUFFER[projPlanNo].getOID_Ptr();
					ocn		 	 = EVAL_ACCESSLISTTABLE[projPlanNo].getCurrOcn();

					m_getdata_info[colNo].srcPtr    = NULL;
					m_getdata_info[colNo].srcLength = NULL;
					m_getdata_info[colNo].nullFlag  = &alwaysFalse; // this means always FALSE
					m_getdata_info[colNo].type      = OOSQL_TYPE_INT;
					m_getdata_info[colNo].oid       = oid;
					m_getdata_info[colNo].ocn       = ocn;
					m_getdata_info[colNo].projectionKind = PATHEXPR_KIND_LOGICALID;
					break;
					
				case PATHEXPR_KIND_METHOD:
					OOSQL_ERR(eNOTIMPLEMENTED_METHOD_OOSQL);

				default:
					OOSQL_ERR(eINVALID_CASE_OOSQL);
				}
				break;

			case PROJECTION_KIND_AGGRFUNCRESULT:
				projPlanNo  = pProjection[colNo].aggrFuncResult.planNo;
				projColNo   = pProjection[colNo].aggrFuncResult.aggrFuncIndex;
				projBufSlot = EVAL_EVALBUFFER[projPlanNo].getAggrFuncResSlotPtr(projColNo);

				m_getdata_info[colNo].srcPtr    = (char*)&(projBufSlot->data.s);
				m_getdata_info[colNo].srcLength = &projBufSlot->retLength;
				//m_getdata_info[colNo].nullFlag  = &alwaysFalse; // this means always FALSE
                m_getdata_info[colNo].nullFlag  = &projBufSlot->nullFlag;
				m_getdata_info[colNo].type      = pProjection[colNo].resultType;
				m_getdata_info[colNo].oid       = &nullOid;
				break;

			case PROJECTION_KIND_FUNCEVAL:
				m_getdata_info[colNo].srcPtr      = NULL;
				m_getdata_info[colNo].srcLength   = &pProjection[colNo].resultLength;
				m_getdata_info[colNo].nullFlag    = &alwaysFalse; // this means always FALSE
				m_getdata_info[colNo].type        = pProjection[colNo].resultType;
				m_getdata_info[colNo].oid       = &nullOid;
				m_getdata_info[colNo].pProjection = &pProjection[colNo];
				break;

			case PROJECTION_KIND_FUNCRESULT:
				projPlanNo = pProjection[colNo].funcResult.planNo;
				projColNo  = pProjection[colNo].funcResult.funcIndex;

				switch( pProjection[colNo].funcResult.functionID ) 
				{
				case FUNCTION_TEXTIR_WEIGHT:
					/* get function result */
					projBufSlot = EVAL_EVALBUFFER[projPlanNo].getFnMatchSlotPtr(projColNo);

					m_getdata_info[colNo].srcPtr    = (char*)&(projBufSlot->data.s);
					m_getdata_info[colNo].srcLength = &pProjection[colNo].resultLength;
					m_getdata_info[colNo].nullFlag  = &alwaysFalse; // this means always FALSE
					m_getdata_info[colNo].type      = OOSQL_TYPE_DOUBLE;
					m_getdata_info[colNo].oid       = &nullOid;
					break;
				default:
					OOSQL_ERR( eNOTIMPLEMENTED_FUNCPROJ_OOSQL);
				}

				break;
			case PROJECTION_KIND_VALUE:
				switch(pProjection[colNo].value.getElements(m_pool->valuePool, 0).valueKind)
				{
				case VALUE_KIND_STRING:
					m_getdata_info[colNo].srcPtr    = (char*)&(pProjection[colNo].value.getElements(m_pool->valuePool, 0).string.getElements(m_pool->stringPool, 0));
					m_getdata_info[colNo].srcLength = &pProjection[colNo].value.getElements(m_pool->valuePool, 0).string.size;
					m_getdata_info[colNo].nullFlag  = &alwaysFalse; // this means always FALSE
					m_getdata_info[colNo].type      = OOSQL_TYPE_STRING;
					m_getdata_info[colNo].oid       = &nullOid;
					break;
				case VALUE_KIND_INTEGER: 
					m_getdata_info[colNo].srcPtr    = (char*)&(pProjection[colNo].value.getElements(m_pool->valuePool, 0).integer.getElements(m_pool->intPool, 0));
					m_getdata_info[colNo].srcLength = &pProjection[colNo].resultLength;
					m_getdata_info[colNo].nullFlag  = &alwaysFalse; // this means always FALSE
					m_getdata_info[colNo].type      = OOSQL_TYPE_INT;
					m_getdata_info[colNo].oid       = &nullOid;
					break;
				case VALUE_KIND_REAL:
					m_getdata_info[colNo].srcPtr    = (char*)&(pProjection[colNo].value.getElements(m_pool->valuePool, 0).real.getElements(m_pool->realPool, 0));
					m_getdata_info[colNo].srcLength = &pProjection[colNo].resultLength;
					m_getdata_info[colNo].nullFlag  = &alwaysFalse; // this means always FALSE
					m_getdata_info[colNo].type      = OOSQL_TYPE_FLOAT;
					m_getdata_info[colNo].oid       = &nullOid;
					break;
				case VALUE_KIND_BOOL:
					m_getdata_info[colNo].srcPtr    = (char*)&(pProjection[colNo].value.getElements(m_pool->valuePool, 0).boolean.startIndex);
					m_getdata_info[colNo].srcLength = &pProjection[colNo].resultLength;
					m_getdata_info[colNo].nullFlag  = &alwaysFalse; // this means always FALSE
					m_getdata_info[colNo].type      = OOSQL_TYPE_INT;
					m_getdata_info[colNo].oid       = &nullOid;
					break;
				case VALUE_KIND_NIL:
				case VALUE_KIND_MBR:
				case VALUE_KIND_DATE:
				case VALUE_KIND_TIME:
				case VALUE_KIND_TIMESTAMP:
				case VALUE_KIND_INTERVAL:
				default:
					OOSQL_ERR(eNOTIMPLEMENTED_OOSQL); // unhandled case error
					break;
				}
				break;
			case PROJECTION_KIND_OPER:
				m_getdata_info[colNo].srcPtr      = NULL;
				m_getdata_info[colNo].srcLength   = &pProjection[colNo].resultLength;
				if (m_pool->orderByListPool.nElements() != 0 || m_pool->groupByListPool.nElements() != 0)
					m_getdata_info[colNo].nullFlag  = &projBufSlot->nullFlag;
				else
					m_getdata_info[colNo].nullFlag    = &alwaysFalse; // this means always FALSE
				m_getdata_info[colNo].nullFlag    = &alwaysFalse; // this means always FALSE
				m_getdata_info[colNo].type        = pProjection[colNo].resultType;
				m_getdata_info[colNo].oid         = &nullOid;
				m_getdata_info[colNo].pProjection = &pProjection[colNo];
				break;
			}
		}
	}

    /* return */
    return eNOERROR;
}


Four    OOSQL_Evaluator::RePrepare()
/*
    Function:
        Reprepare for evaluation with the previously initialized access plan.

    Side effect:

    Return value:
*/
{
        return eNOERROR;
}


/***********************************************
 * Query Evaluation and Result Fetching Module *
 ***********************************************/

Four OOSQL_Evaluator::EvalAndFetchForSelectOnly(
	Four	nEvaluationsToDo,			// IN
	void*	headerBuffer,				// IN
	Four	headerBufferSize,			// IN
	void*	dataBuffer,					// IN
	Four	dataBufferSize,				// IN
	Four*	nEvaluationsDone			// OUT
)
{
	Four 		whereCond;             /* result of checking WHERE condition */
    Four 		e;                     /* error code */
	Boolean		isResultRemained;

	isResultRemained = SM_FALSE;

    /* 
     * check the current status and change it if necessary 
     */
    switch(m_evalStatus.globalStatus) 
	{
        case EVALSTATUS_GLOBAL_INIT:
            OOSQL_ERR(eNOTREADY_OOSQL);

        case EVALSTATUS_GLOBAL_READY:
            /* initialize evaluation status */
            m_evalStatus.globalStatus				= EVALSTATUS_GLOBAL_EVALUATING;
            m_evalStatus.endOfEval					= SM_FALSE;
            m_evalStatus.queryResultBufferFull		= SM_FALSE;
            m_evalStatus.wasLastProjectionCompleted	= SM_TRUE;
            m_evalStatus.groupingStatus				= EVALSTATUS_INIT;
            m_evalStatus.aggregationStatus			= EVALSTATUS_INIT;
            m_evalStatus.prepareAndSortStatus		= EVALSTATUS_INIT;

            /* initialize the index to access plan element to be evaluated */
            m_currPlanIndex							= -1;
            m_evalStatus.lastPlanElemOfCurrNestedLoop = -1;

            /* move the evaluation control to the first access plan element */
            e = moveToAndOpenNextPlanElement();
            if(e < eNOERROR)
                OOSQL_ERR(e);

			break;

        case EVALSTATUS_GLOBAL_EVALUATING:
            break;

        case EVALSTATUS_GLOBAL_END:
            /* close and reopen all scans */
            e = RePrepare();
            if(e < eNOERROR)
                OOSQL_ERR(e);

            m_evalStatus.globalStatus = EVALSTATUS_GLOBAL_EVALUATING;
            break;

        default:
            OOSQL_ERR(eINVALID_CASE_OOSQL);
    }

    // Initialize member variables for managing query result buffer 
	// 입력된 데이타를 사용하여 OOSQL_MultipleResultBuffer 객체를 생성한다.
	// 이 객체는 질의 결과를 주어진 buffer에 packing하는 역할을 담당한다.
	OOSQL_MultipleResultBufferWithHeader		multipleResultBufferWithHeader(pMemoryManager, this, nEvaluationsToDo, headerBuffer, headerBufferSize, dataBuffer, dataBufferSize);
	OOSQL_MultipleResultBufferWithData			multipleResultBufferWithData(pMemoryManager, this, nEvaluationsToDo, headerBuffer, headerBufferSize, dataBuffer, dataBufferSize);
	OOSQL_MultipleResultBufferWithHeaderAndData multipleResultBufferWithHeaderAndData(pMemoryManager, this, nEvaluationsToDo, headerBuffer, headerBufferSize, dataBuffer, dataBufferSize);

	if(!headerBuffer && !dataBuffer)		
		m_multipleResultBuffer = NULL;
	else 									
	{
		if(headerBuffer && dataBuffer)
			m_multipleResultBuffer = &multipleResultBufferWithHeaderAndData;
		else if(headerBuffer && !dataBuffer)
			m_multipleResultBuffer = &multipleResultBufferWithHeader;
		else if(!headerBuffer && dataBuffer)
			m_multipleResultBuffer = &multipleResultBufferWithData;
	}

    /* initialize evaluation status related to query result buffer */
    m_evalStatus.queryResultBufferFull = SM_FALSE;

    /* initialize the # of query results */
    m_numQueryResultTuples = 0;

    /*
     * check if the last projection of the last evaluation has been completed
     * we need not determine here if group by result or not
     */
    if(m_evalStatus.wasLastProjectionCompleted == SM_FALSE) {
        e = projection(m_evalStatus.wasGroupByResult);
        if(e < eNOERROR) OOSQL_ERR(e);
		m_evalStatus.wasLastProjectionCompleted = SM_TRUE;
    }

    /********************************************
     * main algorithm of Nested-Loop evaluation *
     ********************************************/

	resetCurrentWhereCondNodes();	

    /******************** begin of mainEvalLoop ********************/
    while(!m_evalStatus.endOfEval && !m_evalStatus.queryResultBufferFull) 
    {
        if(nEvaluationsToDo != -1 && m_numQueryResultTuples >= nEvaluationsToDo)
            break;
        
        /* read the next object of the current plan element */
        e = getNextObject();
		/* Implement LIMIT clause */
		if(e == eNOERROR && ACCESSPLANELEMENTS[m_currPlanIndex].limitCount == 1) e = ENDOFSCAN;

		if(e == eNOERROR)
		{	/* in case return code of getNextObject is not ENDOFSCAN */
			if(IS_NULL_POOLINDEX(m_currWhereCondNodes))
			{
				if(ACCESSPLANELEMENTS[m_currPlanIndex].limitStart == 0)
				{
					ACCESSPLANELEMENTS[m_currPlanIndex].limitCount --;
                	if(m_evalStatus.needToProcessCurrPlanElem) 
					{
                    	e = processForSelectOnly();
                    	if(e < eNOERROR) OOSQL_ERR(e);
					}
					if(!ACCESSPLANELEMENTS[m_currPlanIndex].isInnermostPlanElem) 
					{
                    	e = moveToAndOpenNextPlanElement();
                    	if(e < eNOERROR) OOSQL_ERR(e);
                	}
				}
				else
					ACCESSPLANELEMENTS[m_currPlanIndex].limitStart --;
            }
            else 
			{
                whereCond = checkWhereCond();
                if(whereCond < eNOERROR) OOSQL_ERR(whereCond);

				if(ACCESSPLANELEMENTS[m_currPlanIndex].limitStart != 0)
				{
					ACCESSPLANELEMENTS[m_currPlanIndex].limitStart --;
					whereCond = SM_FALSE;
				}
				else ACCESSPLANELEMENTS[m_currPlanIndex].limitCount --;

				if(whereCond == SM_TRUE)
				{
					if(m_evalStatus.needToProcessCurrPlanElem) 
					{
						e = processForSelectOnly();
						if(e < eNOERROR) OOSQL_ERR(e);
					}

					if(!ACCESSPLANELEMENTS[m_currPlanIndex].isInnermostPlanElem) 
					{
						e = moveToAndOpenNextPlanElement();
						if(e < eNOERROR) OOSQL_ERR(e);
					}
				}
            }
        } 
        else if(e == ENDOFSCAN) 
		{
            if(isOutermostClassOfNestedLoop()) 
			{
				if(m_evalStatus.aggregationStatus == EVALSTATUS_PROCESSING)
				{
                    e = moveToLastPlanElemOfNestedLoop();
                    if(e < eNOERROR)
                        OOSQL_ERR(e);

                    e = processForSelectOnly();
                    if(e < eNOERROR)
                        OOSQL_ERR(e);

                    if(isLastPlanElement()) 
					{
						if(m_numQueryResultTuples)
							isResultRemained = SM_TRUE;
                        m_evalStatus.endOfEval = SM_TRUE;
                    }
                    else 
					{
                        e = moveToAndOpenNextPlanElement();
                        if(e < eNOERROR)
                            OOSQL_ERR(e);
                    }
                } /* end if: last step processing of the current nested-loop */
                else 
				{
                    m_evalStatus.endOfEval = SM_TRUE;
                }
            } /* end if: outermost class of the current nested-loop */
            else 
			{
                e = backtrack();
                if(e < eNOERROR)
                    OOSQL_ERR(e);
            }
        }	/* end if: end of scan */
 		else if(e < eNOERROR) 
		{
            OOSQL_ERR(e);
        }	/* end else: not end of scan */
	} /* end while: main evaluation loop of nested-loop */
    /******************** end of mainEvalLoop ********************/

	// multipleResultBuffer를 더이상 reference하지 않도록 초기화 한다.
	m_multipleResultBuffer = NULL;
	if(nEvaluationsDone) *nEvaluationsDone = m_numQueryResultTuples;

    /* return */
    if(m_evalStatus.endOfEval && !isResultRemained && m_numQueryResultTuples == 0)
        return ENDOFEVAL;
    else 
        return eNOERROR;
}

Four OOSQL_Evaluator::EvalAndFetchForGeneralQuery(
	Four	nEvaluationsToDo,			// IN
	void*	headerBuffer,				// IN
	Four	headerBufferSize,			// IN
	void*	dataBuffer,					// IN
	Four	dataBufferSize,				// IN
	Four*	nEvaluationsDone			// OUT
)
{
    Four 		whereCond;             /* result of checking WHERE condition */
    Four 		e;                     /* error code */
	Boolean		isResultRemained;

	isResultRemained = SM_FALSE;

    /* 
     * check the current status and change it if necessary 
     */
    switch(m_evalStatus.globalStatus) 
	{
        case EVALSTATUS_GLOBAL_INIT:
            OOSQL_ERR(eNOTREADY_OOSQL);

        case EVALSTATUS_GLOBAL_READY:
            /* initialize evaluation status */
            m_evalStatus.globalStatus				= EVALSTATUS_GLOBAL_EVALUATING;
            m_evalStatus.endOfEval					= SM_FALSE;
            m_evalStatus.queryResultBufferFull		= SM_FALSE;
            m_evalStatus.wasLastProjectionCompleted	= SM_TRUE;
            m_evalStatus.groupingStatus				= EVALSTATUS_INIT;
            m_evalStatus.aggregationStatus			= EVALSTATUS_INIT;
            m_evalStatus.prepareAndSortStatus		= EVALSTATUS_INIT;

            /* initialize the index to access plan element to be evaluated */
            m_currPlanIndex							= -1;
            m_evalStatus.lastPlanElemOfCurrNestedLoop = -1;

            /* move the evaluation control to the first access plan element */
            e = moveToAndOpenNextPlanElement();
            if(e < eNOERROR)
                OOSQL_ERR(e);

			e = processDBCommand();
			if(e < eNOERROR) OOSQL_ERR(e);

			if(m_currPlanIndex < ACCESSPLAN.m_nAP_Elem)
			{
				if(ACCESSPLANELEMENTS[m_currPlanIndex].classInfo.classKind == CLASSKIND_NONE &&
				   ACCESSPLANELEMENTS[m_currPlanIndex].projectionList != NULL_POOLINDEX)
				{
					e = process();
					if(e < eNOERROR) OOSQL_ERR(e);
				}
			}

			break;

        case EVALSTATUS_GLOBAL_EVALUATING:
            break;

        case EVALSTATUS_GLOBAL_END:
            /* close and reopen all scans */
            e = RePrepare();
            if(e < eNOERROR)
                OOSQL_ERR(e);

            m_evalStatus.globalStatus = EVALSTATUS_GLOBAL_EVALUATING;
            break;

        default:
            OOSQL_ERR(eINVALID_CASE_OOSQL);
    }

    // Initialize member variables for managing query result buffer 
	// 입력된 데이타를 사용하여 OOSQL_MultipleResultBuffer 객체를 생성한다.
	// 이 객체는 질의 결과를 주어진 buffer에 packing하는 역할을 담당한다.
	OOSQL_MultipleResultBufferWithHeader		multipleResultBufferWithHeader(pMemoryManager, this, nEvaluationsToDo, headerBuffer, headerBufferSize, dataBuffer, dataBufferSize);
	OOSQL_MultipleResultBufferWithData			multipleResultBufferWithData(pMemoryManager, this, nEvaluationsToDo, headerBuffer, headerBufferSize, dataBuffer, dataBufferSize);
	OOSQL_MultipleResultBufferWithHeaderAndData multipleResultBufferWithHeaderAndData(pMemoryManager, this, nEvaluationsToDo, headerBuffer, headerBufferSize, dataBuffer, dataBufferSize);

	if(!headerBuffer && !dataBuffer)		
		m_multipleResultBuffer = NULL;
	else 									
	{
		if(headerBuffer && dataBuffer)
			m_multipleResultBuffer = &multipleResultBufferWithHeaderAndData;
		else if(headerBuffer && !dataBuffer)
			m_multipleResultBuffer = &multipleResultBufferWithHeader;
		else if(!headerBuffer && dataBuffer)
			m_multipleResultBuffer = &multipleResultBufferWithData;
	}

    /* initialize evaluation status related to query result buffer */
    m_evalStatus.queryResultBufferFull = SM_FALSE;

    /* initialize the # of query results */
    m_numQueryResultTuples = 0;

    /*
     * check if the last projection of the last evaluation has been completed
     * we need not determine here if group by result or not
     */
    if(m_evalStatus.wasLastProjectionCompleted == SM_FALSE) {
        e = projection(m_evalStatus.wasGroupByResult);
        if(e < eNOERROR) OOSQL_ERR(e);
		m_evalStatus.wasLastProjectionCompleted = SM_TRUE;
    }

	if(m_pool->updateValuePool.nElements() > 0)
	{
		Four i;

		for(i = 0; i < m_pool->updateValuePool.nElements(); i++)
		{
			if(m_pool->updateValuePool[i].isParam)
			{
				nEvaluationsToDo = 1;
				break;
			}
		}
	}

    /********************************************
     * main algorithm of Nested-Loop evaluation *
     ********************************************/

	resetCurrentWhereCondNodes();	

    /******************** begin of mainEvalLoop ********************/
    while(!m_evalStatus.endOfEval && !m_evalStatus.queryResultBufferFull) 
    {
        if(nEvaluationsToDo != -1 && m_numQueryResultTuples >= nEvaluationsToDo)
            break;
        
        if(ACCESSPLANELEMENTS[m_currPlanIndex].nGrpByKeys > 0) 
        {
            if(m_evalStatus.groupingStatus == EVALSTATUS_END) 
            {
                if(!EVAL_ACCESSLISTTABLE[m_evalStatus.firstPlanElemOfCurrNestedLoop].isEndOfAllAccess()) 
                {
                    m_evalStatus.groupingStatus = EVALSTATUS_PROCESSING;
                    
                    // set new Group By key(s) for the new group 
                    e = setGroupByKey();
                    if(e < eNOERROR)
                        OOSQL_ERR(e);
                    
                    // We should process aggregation here before reading the next object.
                    // The reason is that the current evaluation buffer will be filled with the next object
                    // when this function -process()- is called next time.
                    if(!IS_NULL_POOLINDEX(ACCESSPLANELEMENTS[m_currPlanIndex].aggrFuncInfo)) {
                        m_evalStatus.aggregationStatus = EVALSTATUS_INIT;
                        e = aggregation();
                        if(e < eNOERROR)
                            OOSQL_ERR(e);
                    }
                }
            }
        }
                
        /* read the next object of the current plan element */
        e = getNextObject();
        if(e == eNOERROR)
		{	/* in case return code of getNextObject is not ENDOFSCAN */
			if(IS_NULL_POOLINDEX(m_currWhereCondNodes))
			{
                /* if there is no condition to check, WHERE condition is SM_TRUE */
                whereCond = SM_TRUE;
            }
            else 
			{
                whereCond = checkWhereCond();	
                if(whereCond < eNOERROR)
                    OOSQL_ERR(whereCond);
            }

            if(whereCond == SM_TRUE) 
			{
                if(m_evalStatus.needToProcessCurrPlanElem) 
				{
                    /*
                     * process() do the followings.
                     * 1) group the tuples
                     * 2) calculate the intermediate result of each aggregate function
                     * 3) project tuple into the temporary file
                     */
                    e = process();
                    if(e < eNOERROR) OOSQL_ERR(e);
                }

                if(!ACCESSPLANELEMENTS[m_currPlanIndex].isInnermostPlanElem) 
				{
                    e = moveToAndOpenNextPlanElement();
                    if(e < eNOERROR) OOSQL_ERR(e);
                }
            } /* end if: Where condition is SM_TRUE */
        } 
        else if(e == ENDOFSCAN) 
		{
            if(isOutermostClassOfNestedLoop()) 
			{
                if(m_evalStatus.groupingStatus		== EVALSTATUS_PROCESSING ||
                    m_evalStatus.aggregationStatus	== EVALSTATUS_PROCESSING ||
                    m_evalStatus.prepareAndSortStatus == EVALSTATUS_PROCESSING) 
				{
                    /****************************************************************************************
                     *
                     * If m_evalStatus.groupingStatus is EVALSTATUS_PROCESSING,
                     *    check having condition for the last group and calculate aggregate function result
                     *   (NOTE: the last group is determined after we encounter ENDOFSCAN for the input file).
                     * If m_evalStatus.aggregationStatus == EVALSTATUS_PROCESSING,
                     *    calculate the aggregate function result e.g., avg = sum/#tuples
                     *   (this case occur when query contains aggregate function without Group By clause).
                     * If m_evalStatus.prepareAndSortStatus == EVALSTATUS_PROCESSING,
                     *    sort the temporary file into which tuples are projected
                     *   (this case can occur if the input query contains Group By or Order By clause or
                     *     DISTINCT flag in Select clause or in some aggregate function).
                     ****************************************************************************************/

                    /**************************************************************************************** 
                     * move evaluation control to the last plan element of the current nested-loop
                     * NOTE: evaluation status will be changed
                     *  - groupingStatus: PROCESSING -> END
                     *  - aggregationStatus: PROCESSING -> END
                     *  - prepareAndSortStatus: PROCESSING -> END
                     ****************************************************************************************/
                    e = moveToLastPlanElemOfNestedLoop();
                    if(e < eNOERROR)
                        OOSQL_ERR(e);

                    e = process();
                    if(e < eNOERROR)
                        OOSQL_ERR(e);

                    if(isLastPlanElement()) 
					{
						if(m_numQueryResultTuples)
							isResultRemained = SM_TRUE;
                        m_evalStatus.endOfEval = SM_TRUE;
                    }
                    else 
					{
                        e = moveToAndOpenNextPlanElement();
                        if(e < eNOERROR) OOSQL_ERR(e);
                    }
                } /* end if: last step processing of the current nested-loop */
                else 
				{
                    m_evalStatus.endOfEval = SM_TRUE;
                }
            } /* end if: outermost class of the current nested-loop */
            else 
			{
                e = backtrack();
                if(e < eNOERROR)
                    OOSQL_ERR(e);
            }
        }	/* end if: end of scan */
        else if(e < eNOERROR) 
		{
            OOSQL_ERR(e);
        }
    } /* end while: main evaluation loop of nested-loop */
    /******************** end of mainEvalLoop ********************/

	// multipleResultBuffer를 더이상 reference하지 않도록 초기화 한다.
	m_multipleResultBuffer = NULL;
	if(nEvaluationsDone) *nEvaluationsDone = m_numQueryResultTuples;

    /* return */
    if(m_evalStatus.endOfEval && !isResultRemained && m_numQueryResultTuples == 0)
        return ENDOFEVAL;
    else 
        return eNOERROR;
}

Four    OOSQL_Evaluator::ResumeAndFetch(
        Four    resultBufLength,        // IN
        char    *resultBuf,             // IN/OUT
        Four    *actualResultBufLength  // OUT
)
/*
    Function:

    Side effect:

    Return value:
*/
{
        return eNOERROR;
}


Four    OOSQL_Evaluator::End()
/*
    Function:
        Release all resources aquired for query evaluation.

    Side effect:

    Return value:
*/
{
        Four            e;

        /*
         * close all scans kept in scan list 
         */
        e = finalAccessListTable();
        if(e < eNOERROR) OOSQL_ERR(e);


        /*
         * close all scans of temp. files and destroy them
         */
        e = finalizeTempFiles();
        if(e < eNOERROR) OOSQL_ERR(e);

        /*
         * reset evaluation status
         */
        m_evalStatus.globalStatus = EVALSTATUS_GLOBAL_INIT;

        return eNOERROR;
}

Four OOSQL_Evaluator::GetData(Four colNo, Four startPos, void* buffer, Four bufferLength, Four* returnLength)
{
	Four	e;
	char*	srcPtr;
	Four	srcLength;
	Boolean nullFlag;
	Four    length;

	if(colNo < 0 || colNo >= m_getdata_nInfo)
		return eBADPARAMETER_OOSQL;

	if(OOSQL_MASK_COMPLEXTYPE(m_getdata_info[colNo].type) != OOSQL_COMPLEXTYPE_BASIC)
		return eBADPARAMETER_OOSQL;

	srcPtr = m_getdata_info[colNo].srcPtr;
	if(m_getdata_info[colNo].srcLength == NULL)
		srcLength = 0;
	else
		srcLength = *m_getdata_info[colNo].srcLength;

	nullFlag = *m_getdata_info[colNo].nullFlag;

	if(nullFlag)			
	{
		if(returnLength != NULL)
			*returnLength = -1;
	}
	else if(srcPtr != NULL && srcLength < OOSQL_EVALBUFFER_MAXSTRINGSIZE && srcLength >= startPos)
	{
		length = (srcLength - startPos);
		if(length > bufferLength)
			length = bufferLength;
		memcpy(buffer, srcPtr + startPos, length);
		if(returnLength != NULL)
			*returnLength = length;
	}
	else
	{
		Four								projColNo = m_getdata_info[colNo].projColNo;
		Four								ocn       = m_getdata_info[colNo].ocn;
		OOSQL_StorageManager::OID*			oid       = m_getdata_info[colNo].oid;
		OOSQL_StorageManager::TextColStruct	text;
		OOSQL_StorageManager::TextDesc		textDesc;
		OOSQL_StorageManager::ColListStruct	clist[1];
		OOSQL_DB_Value						result(pMemoryManager);

		switch(m_getdata_info[colNo].projectionKind) 
		{
		case PROJECTION_KIND_OPER:
			e = evalOperator(&m_getdata_info[colNo].pProjection->oper, &result);
			if(e < eNOERROR) OOSQL_ERR(e);

			if(result.nullFlag)
			{
				*returnLength = -1;
				break;
			}

			if(result.IsStringData())
			{
				srcPtr = (char*)(result.data.ptr);
				srcLength = result.length;
			}
			else
			{
			srcPtr = (char*)&(result.data.s);
			srcLength = result.length;
			}


			memcpy(buffer, srcPtr + startPos,(srcLength - startPos));
			if(returnLength != NULL)
				*returnLength = (srcLength - startPos);

			break;

		case PROJECTION_KIND_FUNCEVAL:
			e = evalFunction(&m_getdata_info[colNo].pProjection->funcEval, &result);
			if(e < eNOERROR) OOSQL_ERR(e);

			if(result.IsStringData())
			{
				srcPtr = (char*)(result.data.ptr);
				srcLength = result.length;
			}
			else
			{
				srcPtr = (char*)&(result.data.s);
				srcLength = result.length;
			}

			memcpy(buffer, srcPtr + startPos,(srcLength - startPos));
			if(returnLength != NULL)
				*returnLength = (srcLength - startPos);

			break;

		case PROJECTION_KIND_PATHEXPR:
			switch(m_getdata_info[colNo].type) 
			{
			case OOSQL_TYPE_TEXT:
				e = m_storageManager->Text_GetDescriptor(ocn, SM_FALSE, oid, projColNo, &textDesc);
				if(e < eNOERROR) OOSQL_ERR(e);

				text.start      = startPos;
				text.length     = bufferLength;
				text.dataLength = bufferLength;
				text.data       = buffer;
				e = m_storageManager->Text_FetchContent(ocn, SM_FALSE, oid, projColNo, &text, &textDesc );
				if(e < eNOERROR) OOSQL_ERR(e);

				if(returnLength != NULL)
					*returnLength = text.retLength;

				break;

			case OOSQL_TYPE_VARSTRING:
			case OOSQL_TYPE_STRING:
				clist[0].colNo      = m_getdata_info[colNo].colNo;         
				clist[0].start      = startPos;
				clist[0].length     = bufferLength;
				clist[0].dataLength = bufferLength;
				clist[0].data.ptr   = buffer;

				e = m_storageManager->FetchObjectByColList(ocn, SM_FALSE, oid, 1, clist);
				if(e < eNOERROR) OOSQL_ERR(e);

				if(returnLength != NULL)
					*returnLength = clist[0].retLength;

				break;

			default:
				OOSQL_ERR(eINVALID_CASE_OOSQL);
			}
			break;

		case PATHEXPR_KIND_LOGICALID:
			e = m_storageManager->Text_GetLogicalId(ocn, SM_FALSE, oid);
			OOSQL_CHECK_ERR(e);

			if(bufferLength >= sizeof(Four))
			{
				memcpy(buffer, &e, sizeof(Four));
				*returnLength = sizeof(Four);
			}
			else
				*returnLength = 0;
			break;

		default:
			OOSQL_ERR(eINVALID_CASE_OOSQL);
		}
	}

	return eNOERROR;
}

Four OOSQL_Evaluator::GetMultiColumnData(Four nColumns, OOSQL_GetDataStruct* getDataStruct)
{
	Four i;
	Four e;

	for(i = 0; i < nColumns; i++)
	{
		e = GetData(getDataStruct[i].columnNumber, getDataStruct[i].startPos, getDataStruct[i].bufferPtr, 
			        getDataStruct[i].bufferLength, &getDataStruct[i].returnLength);
		OOSQL_CHECK_ERR(e);
	}

	return eNOERROR;
}

Four OOSQL_Evaluator::PutData(Four paramNo, Four startPos, void* buffer, Four bufferLength)
{
	Four								targetNo;
	Four								colNo;
	OOSQL_StorageManager::OID*			oid;
	Four								writeOcn;
	OOSQL_StorageManager::ColListStruct	clist[1];
	OOSQL_StorageManager::TextColStruct	text;
	OOSQL_StorageManager::TextDesc		textDesc;
	Four								lastPlanIndex;
	Four								i, index;
	Four								e;
	Four								textUpdateMode;
	AP_ExprPoolElements					expr;
	ValuePoolElements					value;
	AP_ProjectionListPoolElements		projList;
	AP_UpdateValuePoolElements			updateValueList;
	AP_InsertValuePoolElements			insertValueList;
	Four								length;
	Four								attrType;
	OOSQL_StorageManager::ColLengthInfoListStruct			lengthInfoList[1];

	// get parameter information
	// targetNo, colNo, writeOcn
	lastPlanIndex = ACCESSPLAN.m_nAP_Elem - 1;
	projList      = ACCESSPLAN.getProjectionListPool(lastPlanIndex);
	switch(projList[0].projectionType)
	{
	case PROJECTION_UPDATE:
		updateValueList = projList[0].updateInfo.updateValueList.getElements(m_pool->ap_updateValuePool);
		for(index = 0, i = 0; i < updateValueList.size; i++)
		{
			if(updateValueList[i].isParam == SM_TRUE)
			{
				if(paramNo == index)
				{
					if(updateValueList[i].type == TYPEID_TEXT)
					{
						expr = updateValueList[i].expr.getElements(m_pool->ap_exprPool);
						value = expr[0].value.getElements(m_pool->valuePool);

						if(value[0].textUpdateMode == TEXT_UPDATE_MODE_DEFERRED)
							textUpdateMode = OOSQL_StorageManager::DEFERRED_MODE;
						else
							textUpdateMode = OOSQL_StorageManager::IMMEDIATE_MODE;
					}
					break;	// find it
				}
				index ++;
			}
		}
		if(i == updateValueList.size)
		{
			OOSQL_ERR(eBADPARAMETER_OOSQL); 
		}
		else
		{
			colNo    = updateValueList[i].colNo;
			attrType = updateValueList[i].type;
		}
		break;
	case PROJECTION_INSERT:
		insertValueList = projList[0].insertInfo.insertValueList.getElements(m_pool->ap_insertValuePool);
		for(index = 0, i = 0; i < insertValueList.size; i++)
		{
			if(insertValueList[i].isParam == SM_TRUE)
			{
				if(paramNo == index)
				{
					if(insertValueList[i].type == TYPEID_TEXT)
					{
						value = insertValueList[i].value.getElements(m_pool->valuePool);

						if(value[0].textUpdateMode == TEXT_UPDATE_MODE_DEFERRED)
							textUpdateMode = OOSQL_StorageManager::DEFERRED_MODE;
						else
							textUpdateMode = OOSQL_StorageManager::IMMEDIATE_MODE;
					}
					break;	// find it
				}
				index ++;
			}
		}
		if(i == insertValueList.size)
		{
			OOSQL_ERR(eBADPARAMETER_OOSQL); 
		}
		else
		{
			colNo    = insertValueList[i].colNo;
			attrType = insertValueList[i].type;
		}
		break;
	default:
		OOSQL_ERR(eNOTIMPLEMENTED_OOSQL); 
	}

	targetNo = 0;
	writeOcn   = EVAL_ACCESSLISTTABLE[lastPlanIndex].writeOcn;

	// get object id
	// 가정 : PutData를 사용할 수 있는 질의문은 다음과 같은 형태를 가진다.
	//  UPDATE xxx set a = ?, b = ?
	//  INSERT int xxx VALUES(?, ?)
	// 이들 질의는 모두 마지막 plan에 대상이 되는 객체의 OOSQL_StorageManager::OID를 가지고 있다.
	oid = EVAL_EVALBUFFER[lastPlanIndex].getOID_Ptr();

	// construct clist struct
	clist[0].colNo = colNo;

	switch(attrType)
	{
	case OOSQL_TYPE_SHORT:
		clist[0].nullFlag   = SM_FALSE;
		clist[0].start      = ALL_VALUE;
		clist[0].length     = OOSQL_TYPE_SHORT_SIZE;
		clist[0].dataLength = OOSQL_TYPE_SHORT_SIZE;
		clist[0].data.s     = *(Two_Invariable*)buffer;
		e = m_storageManager->UpdateObjectByColList(writeOcn, SM_FALSE, oid, 1, clist);
		if(e < eNOERROR) OOSQL_ERR(e);
		break;

	case OOSQL_TYPE_INT:
		clist[0].nullFlag   = SM_FALSE;
		clist[0].start      = ALL_VALUE;
		clist[0].length     = OOSQL_TYPE_INT_SIZE;
		clist[0].dataLength = OOSQL_TYPE_INT_SIZE;
		clist[0].data.i     = *(Four_Invariable*)buffer;
		e = m_storageManager->UpdateObjectByColList(writeOcn, SM_FALSE, oid, 1, clist);
		if(e < eNOERROR) OOSQL_ERR(e);
		break;

	case OOSQL_TYPE_LONG:
		clist[0].nullFlag   = SM_FALSE;
		clist[0].start      = ALL_VALUE;
		clist[0].length     = OOSQL_TYPE_LONG_SIZE;
		clist[0].dataLength = OOSQL_TYPE_LONG_SIZE;
		clist[0].data.l     = *(Four_Invariable*)buffer;
		e = m_storageManager->UpdateObjectByColList(writeOcn, SM_FALSE, oid, 1, clist);
		if(e < eNOERROR) OOSQL_ERR(e);
		break;

	case OOSQL_TYPE_LONG_LONG:
		clist[0].nullFlag   = SM_FALSE;
		clist[0].start      = ALL_VALUE;
		clist[0].length     = OOSQL_TYPE_LONG_LONG_SIZE;
		clist[0].dataLength = OOSQL_TYPE_LONG_LONG_SIZE;
		clist[0].data.ll     = *(Eight_Invariable*)buffer;
		e = m_storageManager->UpdateObjectByColList(writeOcn, SM_FALSE, oid, 1, clist);
		if(e < eNOERROR) OOSQL_ERR(e);
		break;

	case OOSQL_TYPE_FLOAT:
		clist[0].nullFlag   = SM_FALSE;
		clist[0].start      = ALL_VALUE;
		clist[0].length     = OOSQL_TYPE_FLOAT_SIZE;
		clist[0].dataLength = OOSQL_TYPE_FLOAT_SIZE;
		clist[0].data.f     = *(float*)buffer;
		e = m_storageManager->UpdateObjectByColList(writeOcn, SM_FALSE, oid, 1, clist);
		if(e < eNOERROR) OOSQL_ERR(e);
		break;

	case OOSQL_TYPE_DOUBLE:
		clist[0].nullFlag   = SM_FALSE;
		clist[0].start      = ALL_VALUE;
		clist[0].length     = OOSQL_TYPE_DOUBLE_SIZE;
		clist[0].dataLength = OOSQL_TYPE_DOUBLE_SIZE;
		clist[0].data.d     = *(double*)buffer;
		e = m_storageManager->UpdateObjectByColList(writeOcn, SM_FALSE, oid, 1, clist);
		if(e < eNOERROR) OOSQL_ERR(e);
		break;

	case OOSQL_TYPE_STRING:
	case OOSQL_TYPE_VARSTRING:
					case OOSQL_TYPE_GEOMETRY:
					case OOSQL_TYPE_POINT:
					case OOSQL_TYPE_LINESTRING:
					case OOSQL_TYPE_POLYGON:	
					case OOSQL_TYPE_GEOMETRYCOLLECTION:
					case OOSQL_TYPE_MULTIPOINT:
					case OOSQL_TYPE_MULTILINESTRING:
					case OOSQL_TYPE_MULTIPOLYGON:
		lengthInfoList[0].colNo = clist[0].colNo;
		e = m_storageManager->FetchColLength(writeOcn, SM_FALSE, oid, 1, lengthInfoList);
		if(e < eNOERROR) OOSQL_ERR(e);
		if(lengthInfoList[0].length <= startPos)
		{
			if(attrType == OOSQL_TYPE_VARSTRING)	
				length = 0;
			else
				length = bufferLength;	
		}
		else
			length = lengthInfoList[0].length - startPos;
		clist[0].nullFlag   = SM_FALSE;
		clist[0].start      = startPos;
		clist[0].length     = length;
		clist[0].dataLength = bufferLength;
		clist[0].data.ptr	= buffer;
		e = m_storageManager->UpdateObjectByColList(writeOcn, SM_FALSE, oid, 1, clist);
		if(e < eNOERROR) OOSQL_ERR(e);
		break;

	case OOSQL_TYPE_TEXT:
		text.start      = startPos;
		text.length     = bufferLength;
		text.dataLength = bufferLength;
		text.data       = buffer;
		text.indexMode  = textUpdateMode;

		// get descriptor
		e = m_storageManager->Text_GetDescriptor(writeOcn, SM_FALSE, oid, colNo, &textDesc);
		if(e < eNOERROR) OOSQL_ERR(e);

		if(!OOSQL_StorageManager::DoesNoContentExistTextDesc(textDesc))
		{
			e = m_storageManager->Text_DestroyContent(writeOcn, SM_FALSE, oid, colNo, &textDesc);
			if(e < eNOERROR) OOSQL_ERR(e);
		}

		// create new content
		e = m_storageManager->Text_CreateContent(writeOcn, SM_FALSE, oid, colNo, &text, &textDesc);
		if(e < eNOERROR) OOSQL_ERR(e);
		break;

	case OOSQL_TYPE_OID:
		clist[0].nullFlag   = SM_FALSE;
		clist[0].start      = ALL_VALUE;
		clist[0].length     = OOSQL_TYPE_OID_SIZE;
		clist[0].dataLength = OOSQL_TYPE_OID_SIZE;
		clist[0].data.oid   = *(OOSQL_StorageManager::OID*)buffer;
		e = m_storageManager->UpdateObjectByColList(writeOcn, SM_FALSE, oid, 1, clist);
		if(e < eNOERROR) OOSQL_ERR(e);
		break;

	case OOSQL_TYPE_DATE:
	case OOSQL_TYPE_TIME:
	case OOSQL_TYPE_TIMESTAMP:
	case OOSQL_TYPE_INTERVAL:
	default:
		OOSQL_ERR(eNOTIMPLEMENTED_OOSQL); // unhandled case error
	}
	
	return eNOERROR;
}

Four OOSQL_Evaluator::GetPutDataParamType(Four paramNo)
{
	Four								targetNo;
	Four								lastPlanIndex;
	Four								i, index;
	Four								e;
	AP_ProjectionListPoolElements		projList;
	AP_UpdateValuePoolElements			updateValueList;
	AP_InsertValuePoolElements			insertValueList;
	Four								attrType;

	// get parameter information
	// targetNo, colNo, writeOcn
	lastPlanIndex = ACCESSPLAN.m_nAP_Elem - 1;
	projList      = ACCESSPLAN.getProjectionListPool(lastPlanIndex);
	switch(projList[0].projectionType)
	{
	case PROJECTION_UPDATE:
		updateValueList = projList[0].updateInfo.updateValueList.getElements(m_pool->ap_updateValuePool);
		for(index = 0, i = 0; i < updateValueList.size; i++)
		{
			if(updateValueList[i].isParam == SM_TRUE)
			{
				if(paramNo == index)
				{
					break;	// find it
				}
				index ++;
			}
		}
		if(i == updateValueList.size)
		{
			OOSQL_ERR(eBADPARAMETER_OOSQL); 
		}
		else
		{
			attrType = updateValueList[i].type;
		}
		break;
	case PROJECTION_INSERT:
		insertValueList = projList[0].insertInfo.insertValueList.getElements(m_pool->ap_insertValuePool);
		for(index = 0, i = 0; i < insertValueList.size; i++)
		{
			if(insertValueList[i].isParam == SM_TRUE)
			{
				if(paramNo == index)
				{
					break;	// find it
				}
				index ++;
			}
		}
		if(i == insertValueList.size)
		{
			OOSQL_ERR(eBADPARAMETER_OOSQL); 
		}
		else
		{
			attrType = insertValueList[i].type;
		}
		break;
	default:
		OOSQL_ERR(eNOTIMPLEMENTED_OOSQL); 
	}

	return attrType;
}

Four OOSQL_Evaluator::GetOID(Four targetNo, OOSQL_StorageManager::OID& oid)
{
	Four e;
	Four colNo;
	Four resultLength;

	if(targetNo < 0 || targetNo >= m_getoid_nTargetClass)
		return eBADPARAMETER_OOSQL;

	if(m_queryType == OOSQL_SELECT_QUERY)
	{
			
		if((m_pool->groupByListPool.nElements() > 0) ||
		  (m_pool->selectQueryPool[0].selListType & AGGR_BIT))		
		{
			return eCANT_EXECUTE_GETOID_OOSQL;
		}
		else if((m_pool->orderByListPool.nElements() > 0) ||
			   (m_pool->selectQueryPool[0].selListType & DIST_BIT))	
		{
			colNo = m_getdata_nInfo - m_getoid_nTargetClass + targetNo;
			e = GetData(colNo, 0, &oid, sizeof(OOSQL_StorageManager::OID), &resultLength);
			OOSQL_CHECK_ERR(e);
		}
		else		
			oid = *(EVAL_EVALBUFFER[m_getoid_TargetNoToEvalBufferNo[targetNo]].getOID_Ptr());
	}
	else if(m_queryType == OOSQL_UPDATE_QUERY				|| 
			m_queryType == OOSQL_UPDATE_QUERY_WITH_PARAM	||
			m_queryType == OOSQL_INSERT_QUERY				||
			m_queryType == OOSQL_INSERT_QUERY_WITH_PARAM)
	{
		oid = *(EVAL_EVALBUFFER[ACCESSPLAN.m_nAP_Elem - 1].getOID_Ptr());
	}
	else
		return eINVALID_API_CALL_OOSQL;
	
	return eNOERROR;
}

Four OOSQL_Evaluator::GetType(Four colNo)
{
	AP_ProjectionListPoolElements	projList;
	AP_ProjectionPoolElements		pProjection;

	projList    = ACCESSPLAN.getProjectionListPool(ACCESSPLAN.m_nAP_Elem - 1);
	pProjection = ACCESSPLAN.getProjectionPool( projList[0].projectionInfo );

	if(colNo >= pProjection.size)
		return eBADPARAMETER_OOSQL;

	return(Four)pProjection[colNo].resultType;
}

Four OOSQL_Evaluator::GetLength(Four colNo)
{
	Four											e;
	Four											type;
	AP_ProjectionListPoolElements					projList;
	AP_ProjectionPoolElements						pProjection;
	OOSQL_StorageManager::ColLengthInfoListStruct	lengthInfoList[1];
	Four											lastPlanIndex;
	OOSQL_StorageManager::OID*						oid;
	Four											length;
	OOSQL_StorageManager::TextDesc					textDesc;
	Four											projPlanNo;
    Two                                             projColNo;
	Four											openClassNum;
	Four                                            mappedColNo;
	EVAL_EvalBufferSlot*			                projBufSlot; 

	if(OOSQL_MASK_COMPLEXTYPE(m_getdata_info[colNo].type) != OOSQL_COMPLEXTYPE_BASIC)
		return eBADPARAMETER_OOSQL;

	lastPlanIndex = ACCESSPLAN.m_nAP_Elem - 1;
	projList      = ACCESSPLAN.getProjectionListPool(lastPlanIndex);
	pProjection   = ACCESSPLAN.getProjectionPool( projList[0].projectionInfo );
	projPlanNo    = pProjection[colNo].pathExpr.col.planNo;
	projColNo     = pProjection[colNo].pathExpr.col.colNo;

	oid = EVAL_EVALBUFFER[projPlanNo].getOID_Ptr();

	type = GetType(colNo);
	switch(type)
	{
					case OOSQL_TYPE_GEOMETRY:
					case OOSQL_TYPE_POINT:
					case OOSQL_TYPE_LINESTRING:
					case OOSQL_TYPE_POLYGON:	
					case OOSQL_TYPE_GEOMETRYCOLLECTION:
					case OOSQL_TYPE_MULTIPOINT:
					case OOSQL_TYPE_MULTILINESTRING:
					case OOSQL_TYPE_MULTIPOLYGON:
        length = pProjection[colNo].resultLength;
        break;

	case OOSQL_TYPE_VARSTRING:
		if(m_evalStatus.globalStatus == EVALSTATUS_GLOBAL_READY)
		{
			length = pProjection[colNo].resultLength;
		}
		else
		{
		    if(m_pool->groupByListPool.nElements() > 0 && m_pool->orderByListPool.nElements() == 0)
		    {
			    mappedColNo = ACCESSPLAN.getMappedColNo(projPlanNo, projColNo);
			    projBufSlot = EVAL_EVALBUFFER[projPlanNo].getGrpBySlotPtr(mappedColNo);
			    length = projBufSlot->retLength;
		    }
		    else
		    {
			    lengthInfoList[0].colNo = m_getdata_info[colNo].colNo;         
			    openClassNum = EVAL_ACCESSLISTTABLE[projPlanNo].getCurrOcn();
			    e = m_storageManager->FetchColLength(openClassNum, SM_FALSE, oid, 1, lengthInfoList);
			    if(e < eNOERROR) OOSQL_ERR(e);
			    length = lengthInfoList[0].length;
		    }
        }
        break;

	case OOSQL_TYPE_TEXT:
		if(m_evalStatus.globalStatus == EVALSTATUS_GLOBAL_READY)
		{
			length = LONG_MAX;	// 2G
		}
		else
		{
	        openClassNum = EVAL_ACCESSLISTTABLE[projPlanNo].getCurrOcn();
		    e = m_storageManager->Text_GetDescriptor(openClassNum, SM_FALSE, oid, m_getdata_info[colNo].colNo, &textDesc);
		    if(e < eNOERROR) OOSQL_ERR(e);
		    length = textDesc.size;
        }
		break;

	default:
		length = pProjection[colNo].resultLength;
		break;
	}

	return length;
}

Four OOSQL_Evaluator::GetComplexTypeInfo(Two columnNo, OOSQL_ComplexTypeInfo* complexTypeInfo)
{
	AP_ProjectionListPoolElements	projList;
	AP_ProjectionPoolElements		pProjection;
	Four							type;
	Four							projPlanNo;         /* projection plan no. */
	Two								projColNo;          /* projection column no.  */
	EVAL_EvalBufferSlot*			projBufSlot; 
	AP_UsedColPoolElements			pUsedColPool;       /* ptr. to used column m_pool for the current A.P. elem. */
	Four							mappedColNo;        /* mapped column no. for projection col. no. */

	if(OOSQL_MASK_COMPLEXTYPE(m_getdata_info[columnNo].type) == OOSQL_COMPLEXTYPE_BASIC)
		return eBADPARAMETER_OOSQL;

	projList     = ACCESSPLAN.getProjectionListPool(ACCESSPLAN.m_nAP_Elem - 1);
	pProjection  = ACCESSPLAN.getProjectionPool(projList[0].projectionInfo);
	projPlanNo   = pProjection[columnNo].pathExpr.col.planNo;
	projColNo    = pProjection[columnNo].pathExpr.col.colNo;
	pUsedColPool = ACCESSPLAN.getUsedColPool(projPlanNo);
	mappedColNo  = ACCESSPLAN.getMappedColNo(projPlanNo, projColNo);

	if(columnNo >= pProjection.size)
		return eBADPARAMETER_OOSQL;

	complexTypeInfo->orn   = EVAL_ACCESSLISTTABLE[projPlanNo].getCurrOcn();
	complexTypeInfo->colNo = GET_USERLEVEL_COLNO(pProjection[columnNo].pathExpr.col.colNo);
	memcpy(&complexTypeInfo->oid, EVAL_EVALBUFFER[projPlanNo].getOID_Ptr(), sizeof(OOSQL_StorageManager::OID));

	type = GetType(columnNo);
	complexTypeInfo->complexType = OOSQL_MASK_COMPLEXTYPE(type);
	complexTypeInfo->elementType = OOSQL_MASK_TYPE(type);

	return eNOERROR;
}

#ifndef SLIMDOWN_TEXTIR
Four OOSQL_Evaluator::EstimateNumResults(Four* nResults)
{
	Four numOfEstimatedResults;
	Four e;

	e = execTextIR_EstimateNumOfResults(numOfEstimatedResults);
	if (e < eNOERROR) OOSQL_ERR(e);

	*nResults = numOfEstimatedResults;

	return eNOERROR;
}

#else /* SLIMDOWN_TEXTIR */
Four OOSQL_Evaluator::EstimateNumResults(Four* nResults)
{
    return eTEXTIR_NOTENABLED_OOSQL;
}

#endif /* SLIMDOWN_TEXTIR */


