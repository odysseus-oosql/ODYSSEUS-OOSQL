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
        oosql_Eval_Projection.cxx

    DESCRIPTION:

    IMPORTS:

    EXPORTS:
*/

#include "OOSQL_Evaluator.hxx"
#include "oosql_Eval_Control.hxx"
#include "oosql_Eval_Access.hxx"
#include "OOSQL_MultipleResultBuffer.hxx"
#include <string.h>

Four OOSQL_Evaluator::projection(
    One isGroupByResult         /* IN: */
)
/*
    Function:

    Side effect:
        m_evalStatus.queryResultBufferFull can be changed to SM_TRUE.

    Referenced member variables:
        m_currPlanIndex

    Return value:
*/
{
    AP_ProjectionListPoolElements	projList;
    Four							resultTupleSize;
    Four							i, j;
    Four							e;
	AP_UpdateValuePoolElements		updateValueList;
	AP_InsertValuePoolElements		insertValueList;
	Boolean							isParameterExist;
	Boolean							isAllValueNull;
	OOSQL_StorageManager::OID 		oid;
	Four							index;
	AP_ProjectionPoolElements		pProjection;
	AP_UsedColPoolElements			usedCol;
	AP_ColNoMapPoolElements			colNoMap;
	OOSQL_DB_Value					res(pMemoryManager);
	Four							nDistinctElements;

#ifdef  OOSQL_DEBUG
    /* assert that projection for the current plan element is necessary */
    if (IS_NULL_POOLINDEX(ACCESSPLANELEMENTS[m_currPlanIndex].projectionList))
        OOSQL_ERR(eINTERNAL_INCORRECTEXECSEQUENCE_OOSQL);
#endif

    /* get projection list for the current acccess plan element */
    projList = ACCESSPLAN.getProjectionListPool(m_currPlanIndex);
	
	/* get current plan's column information */
	colNoMap = ACCESSPLAN.getColNoMapPool(m_currPlanIndex);
	usedCol  = ACCESSPLAN.getUsedColPool(m_currPlanIndex);

	switch(projList[0].projectionType)
	{
	case PROJECTION_NONE:
		if(ACCESSPLANELEMENTS[m_currPlanIndex].selDistinctFlag)
		{	
			nDistinctElements = EVAL_EVALBUFFER[m_currPlanIndex].nCols - m_pool->targetListPool.nElements(); 
	
			pProjection = ACCESSPLAN.getProjectionPool(projList[0].projectionInfo);
			for(i = 0; i < nDistinctElements; i++)
			{
				if(EVAL_EVALBUFFER[m_currPlanIndex].clist[i].nullFlag != 
				   EVAL_EVALBUFFER[m_currPlanIndex].prevColList[i].nullFlag)
					break;
				
				if(EVAL_EVALBUFFER[m_currPlanIndex].clist[i].retLength != 
				   EVAL_EVALBUFFER[m_currPlanIndex].prevColList[i].retLength)
					break;

				if(!EVAL_EVALBUFFER[m_currPlanIndex].prevColList[i].nullFlag)
				{
					if(usedCol[colNoMap[i].offset].typeId == OOSQL_TYPE_STRING || usedCol[colNoMap[i].offset].typeId == OOSQL_TYPE_VARSTRING)
					{
						if(memcmp(EVAL_EVALBUFFER[m_currPlanIndex].clist[i].data.ptr, 
								  EVAL_EVALBUFFER[m_currPlanIndex].prevColList[i].data.ptr,
								  EVAL_EVALBUFFER[m_currPlanIndex].clist[i].retLength))
							break;
					}
					else
					{
						if(memcmp(&EVAL_EVALBUFFER[m_currPlanIndex].clist[i].data.s, 
								  &EVAL_EVALBUFFER[m_currPlanIndex].prevColList[i].data.s,
								  EVAL_EVALBUFFER[m_currPlanIndex].clist[i].retLength))
							break;
					}
				}
			}

			if(i == nDistinctElements)
				return eNOERROR;

			for(i = 0; i < nDistinctElements; i++)
			{
				EVAL_EVALBUFFER[m_currPlanIndex].prevColList[i].retLength = EVAL_EVALBUFFER[m_currPlanIndex].clist[i].retLength;
				EVAL_EVALBUFFER[m_currPlanIndex].prevColList[i].nullFlag  = EVAL_EVALBUFFER[m_currPlanIndex].clist[i].nullFlag;

				if(!EVAL_EVALBUFFER[m_currPlanIndex].prevColList[i].nullFlag)
				{
					if(usedCol[colNoMap[i].offset].typeId == OOSQL_TYPE_STRING || usedCol[colNoMap[i].offset].typeId == OOSQL_TYPE_VARSTRING)
					{
						memcpy(EVAL_EVALBUFFER[m_currPlanIndex].prevColList[i].data.ptr,
							   EVAL_EVALBUFFER[m_currPlanIndex].clist[i].data.ptr, 
							   EVAL_EVALBUFFER[m_currPlanIndex].clist[i].retLength);
					}
					else
					{
						memcpy(&EVAL_EVALBUFFER[m_currPlanIndex].prevColList[i].data.s,
							   &EVAL_EVALBUFFER[m_currPlanIndex].clist[i].data.s, 
  							   EVAL_EVALBUFFER[m_currPlanIndex].clist[i].retLength);
					}
				}
			}
		}

		/* if the current plan element is the last plan element,
		 * query results are projected to query result buffer
		 */
		if (isLastPlanElement()) 
		{
			if(m_multipleResultBuffer == NULL)
			{
				m_numQueryResultTuples ++;
				
				return eNOERROR;
			}

			e = m_multipleResultBuffer->AppendCurrentEvaluatedObject();
			if(e == eNOERROR)
			{
				/* increase the number of query result tuples */
				m_numQueryResultTuples ++;

				return eNOERROR;
			}
			else if(e == eMULTIPLERESULTBUFFER_FULL_OOSQL)
			{
				/* record that the current projection is not completed */
				m_evalStatus.wasLastProjectionCompleted = SM_FALSE;
				m_evalStatus.wasGroupByResult			= isGroupByResult;

				/* change evaluation status */
				m_evalStatus.queryResultBufferFull		= SM_TRUE;

				return eNOERROR;
			}
			else
				OOSQL_CHECK_ERR(e);
		}
		else { /* projection to temporary file(s) */
			/* check prepareAndSortStatus */
			switch (m_evalStatus.prepareAndSortStatus) {
				case EVALSTATUS_PROCESSING:
					// process each projection element
					if(m_evalStatus.aggregationStatus != EVALSTATUS_PROCESSING)
					{
						for (i = 0; i < GET_POOLSIZE(projList); i++) 
						{
							e = processProjectionToTempFile(&projList[i], (Boolean)isGroupByResult);
							if (e < eNOERROR)
								OOSQL_ERR(e);
						}
					}
					break;

				case EVALSTATUS_INIT:
					m_evalStatus.prepareAndSortStatus = EVALSTATUS_PROCESSING;

					/* save the index to access plan element from which projection is performed */
					if (m_evalStatus.lastPlanElemOfCurrNestedLoop < m_currPlanIndex)
						m_evalStatus.lastPlanElemOfCurrNestedLoop = m_currPlanIndex;

					if(m_evalStatus.aggregationStatus != EVALSTATUS_PROCESSING)
					{
						// process each projection element
						for (i = 0; i < GET_POOLSIZE(projList); i++) 
						{
							e = processProjectionToTempFile(&projList[i], (Boolean)isGroupByResult);
							if (e < eNOERROR)
								OOSQL_ERR(e);
						}
					}

					break;

				case EVALSTATUS_END:
					/* just return because projection is ended */
					return eNOERROR;

				default:
					OOSQL_ERR(eINVALID_CASE_OOSQL);
			}
		}
		break;

	case PROJECTION_UPDATE:
		e = projectionForUpdate(isGroupByResult);
		OOSQL_CHECK_ERR(e);
		break;

	case PROJECTION_INSERT:
		e = projectionForInsert(isGroupByResult);
		OOSQL_CHECK_ERR(e);
		break;

	case PROJECTION_DELETE:
		if(ACCESSPLANELEMENTS[m_currPlanIndex].classInfo.classKind == CLASSKIND_TEMPORARY || ACCESSPLANELEMENTS[m_currPlanIndex].classInfo.classKind == CLASSKIND_SORTSTREAM)
		{
			oid = EVAL_EVALBUFFER[m_currPlanIndex].getColSlotPtr(0)->data.oid;
		}
		else
		{
			oid = *(EVAL_EVALBUFFER[m_currPlanIndex].getOID_Ptr());
		}

		if(projList[0].deleteInfo.isDeferredDelete)
		{
			e = m_storageManager->DeferredDestroyObject(EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeOcn, SM_FALSE,
				                                        &oid/*(delete)EVAL_EVALBUFFER[m_currPlanIndex].getOID_Ptr()*/);
			OOSQL_CHECK_ERR(e);
		}
		else
		{
			e = m_storageManager->DestroyObject(EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeOcn, SM_FALSE,
				                                &oid/*(delete)EVAL_EVALBUFFER[m_currPlanIndex].getOID_Ptr()*/);
			OOSQL_CHECK_ERR(e);
		}
		break;
	}

    return eNOERROR;
}

Four OOSQL_Evaluator::projectionForUpdate(
    One isGroupByResult         /* IN: */
)
/*
    Function:

    Side effect:
        m_evalStatus.queryResultBufferFull can be changed to SM_TRUE.

    Referenced member variables:
        m_currPlanIndex

    Return value:
*/
{
    AP_ProjectionListPoolElements	projList;
    Four							resultTupleSize;
    Four							i, j;
    Four							e;
	AP_UpdateValuePoolElements		updateValueList;
	AP_InsertValuePoolElements		insertValueList;
	Boolean							isParameterExist;
	Boolean							isAllValueNull;
	OOSQL_StorageManager::OID 		oid;
	Four							index;
	AP_ProjectionPoolElements		pProjection;
	AP_UsedColPoolElements			usedCol;
	AP_ColNoMapPoolElements			colNoMap;
	OOSQL_DB_Value					res(pMemoryManager);
	Four							nDistinctElements;
	char							seqName[MAXCLASSNAME];
	Four							seqValue;
	AP_ExprPoolElements				value;


#ifdef  OOSQL_DEBUG
    /* assert that projection for the current plan element is necessary */
    if (IS_NULL_POOLINDEX(ACCESSPLANELEMENTS[m_currPlanIndex].projectionList))
        OOSQL_ERR(eINTERNAL_INCORRECTEXECSEQUENCE_OOSQL);
#endif

    /* get projection list for the current acccess plan element */
    projList = ACCESSPLAN.getProjectionListPool(m_currPlanIndex);
	
	/* get current plan's column information */
	colNoMap = ACCESSPLAN.getColNoMapPool(m_currPlanIndex);
	usedCol  = ACCESSPLAN.getUsedColPool(m_currPlanIndex);

	if(projList[0].projectionType != PROJECTION_UPDATE)
		OOSQL_ERR(eINTERNALERROR_OOSQL);

	m_numQueryResultTuples ++;
	updateValueList  = projList[0].updateInfo.updateValueList.getElements(m_pool->ap_updateValuePool);
	isParameterExist = SM_FALSE;
	for(index = 0, i = 0; i < updateValueList.size; i++)
	{
		OOSQL_StorageManager::ColListStruct*	colList;
		OOSQL_StorageManager::ColListStruct*	writeColList;

		if(updateValueList[i].isParam)
		{
			isParameterExist = SM_TRUE;
			continue;
		}

		if(ACCESSPLANELEMENTS[m_currPlanIndex].classInfo.classKind == CLASSKIND_TEMPORARY || ACCESSPLANELEMENTS[m_currPlanIndex].classInfo.classKind == CLASSKIND_SORTSTREAM)
		{
			if(updateValueList[i].type != TYPEID_TEXT)
			{
				colList = EVAL_EVALBUFFER[m_currPlanIndex].getColSlotPtr(i + 1);
				writeColList = &(EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index]);
				memcpy(&(writeColList->data), &(colList->data), sizeof(colList->data));
				
				if(updateValueList[i].type == TYPEID_STRING)
				{
					writeColList->data.ptr   = pMemoryManager->Alloc(updateValueList[i].length);
					writeColList->dataLength = updateValueList[i].length;

					for(j = 0; j < updateValueList[i].length; j++)
					{
						if(j < res.length)
							((char*)writeColList->data.ptr)[j] = ((char*)colList->data.ptr)[j];
						else
							((char*)writeColList->data.ptr)[j] = 0;
					}
				}

				index ++;
			}
		}
		else
		{
			if(updateValueList[i].type != TYPEID_TEXT)
			{
				e = evalExpression(&updateValueList[i].expr, &res);
				if(e < eNOERROR) OOSQL_ERR(e);
			}

			EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].nullFlag = res.nullFlag;

			switch(updateValueList[i].type)
			{
			case TYPEID_SHORT:
				if(updateValueList[i].seqValueType == SEQ_CURRVAL)
				{
					memcpy(seqName, res.data.ptr, res.length);
					seqName[res.length] = '\0';
					m_storageManager->GetSeqCurrVal(m_volID, seqName, &seqValue);
					EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].data.s = (Two_Invariable)seqValue;
				}
				else if(updateValueList[i].seqValueType == SEQ_NEXTVAL)
				{
					memcpy(seqName, res.data.ptr, res.length);
					seqName[res.length] = '\0';
					m_storageManager->GetSeqNextVal(m_volID, seqName, &seqValue);
					EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].data.s = (Two_Invariable)seqValue;
				}
				else
				EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].data.s = res.GetDataAsShort();
				index ++;
				break;
			case TYPEID_INT:
				if(updateValueList[i].seqValueType == SEQ_CURRVAL)
				{
					memcpy(seqName, res.data.ptr, res.length);
					seqName[res.length] = '\0';
					m_storageManager->GetSeqCurrVal(m_volID, seqName, &seqValue);
					EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].data.i = (Four_Invariable)seqValue;
				}
				else if(updateValueList[i].seqValueType == SEQ_NEXTVAL)
				{
					memcpy(seqName, res.data.ptr, res.length);
					seqName[res.length] = '\0';
					m_storageManager->GetSeqNextVal(m_volID, seqName, &seqValue);
					EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].data.i = (Four_Invariable)seqValue;
				}
				else
				EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].data.i = res.GetDataAsInteger();
				index ++;
				break;
			case TYPEID_LONG:
				if(updateValueList[i].seqValueType == SEQ_CURRVAL)
				{
					memcpy(seqName, res.data.ptr, res.length);
					seqName[res.length] = '\0';
					m_storageManager->GetSeqCurrVal(m_volID, seqName, &seqValue);
					EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].data.l = (Four_Invariable)seqValue;
				}
				else if(updateValueList[i].seqValueType == SEQ_NEXTVAL)
				{
					memcpy(seqName, res.data.ptr, res.length);
					seqName[res.length] = '\0';
					m_storageManager->GetSeqNextVal(m_volID, seqName, &seqValue);
					EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].data.l = (Four_Invariable)seqValue;
				}
				else
				EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].data.l = res.GetDataAsLong();
				index ++;
				break;
			case TYPEID_LONG_LONG:
				if(updateValueList[i].seqValueType == SEQ_CURRVAL)
				{
					memcpy(seqName, res.data.ptr, res.length);
					seqName[res.length] = '\0';
					m_storageManager->GetSeqCurrVal(m_volID, seqName, &seqValue);
					EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].data.ll = (Eight_Invariable)seqValue;
				}
				else if(updateValueList[i].seqValueType == SEQ_NEXTVAL)
				{
					memcpy(seqName, res.data.ptr, res.length);
					seqName[res.length] = '\0';
					m_storageManager->GetSeqNextVal(m_volID, seqName, &seqValue);
					EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].data.ll = (Eight_Invariable)seqValue;
				}
				else
				EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].data.ll = res.GetDataAsLongLong();
				index ++;
				break;
			case TYPEID_FLOAT:
				EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].data.f = res.GetDataAsFloat();
				index ++;
				break;
			case TYPEID_DOUBLE:
				EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].data.d = res.GetDataAsDouble();
				index ++;
				break;

			case TYPEID_STRING:
				EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].data.ptr   = pMemoryManager->Alloc(updateValueList[i].length);
				EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].dataLength = updateValueList[i].length;

				for(j = 0; j < updateValueList[i].length; j++)
				{
					if(j < res.length)
						((char*)EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].data.ptr)[j] = ((char*)res.data.ptr)[j];
					else
						((char*)EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].data.ptr)[j] = 0;
				}
				
				index ++;
				break;
			case TYPEID_VARSTRING:
				EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].data.ptr   = pMemoryManager->Alloc(res.length);
				EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].dataLength = res.length;

				for(j = 0; j < res.length; j++)
				{
					((char*)EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].data.ptr)[j] = ((char*)res.data.ptr)[j];
				}

				index ++;
				break;

			case TYPEID_TEXT:
				break;
			case TYPEID_DATE:
                EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].data.date = res.data.date;
                index ++;
                break;
			case TYPEID_OID:
                if(res.type == TYPEID_OID)
                {
				    EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].data.oid = res.data.oid;
                }
                else if(res.type == TYPEID_STRING)
                {
                    if(res.length != 32)
                        OOSQL_ERR(eINVALID_OID_STRING);
                    e = oosql_ConvertFromOIDStringToOIDStructure(
                        (char*)res.data.ptr, 
                        &EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].data.oid
                    );
                    OOSQL_CHECK_ERR(e);
                }
                else
                    OOSQL_ERR(eINTERNALERROR_OOSQL);
                index ++;
				break;
			case TYPEID_MBR:
				EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].data.mbr = res.data.mbr;
                index ++;
				break;
			case TYPEID_TIME:
				EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].data.time = res.data.time;
                index ++;
				break;
			case TYPEID_TIMESTAMP:
				EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].data.timestamp = res.data.timestamp;
                index ++;
				break;
			case TYPEID_INTERVAL:
				EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].data.interval = res.data.interval;
                index ++;
				break;
			case TYPEID_PAGEID:
			case TYPEID_FILEID:
			case TYPEID_INDEXID:
				OOSQL_ERR(eNOTIMPLEMENTED_OOSQL);
			default:
				switch(OOSQL_MASK_COMPLEXTYPE(updateValueList[i].type))
				{
				case COMPLEXTYPEID_SET:
				case COMPLEXTYPEID_BAG:
				case COMPLEXTYPEID_LIST:
					break;
				default:
					OOSQL_ERR(eNOTIMPLEMENTED_OOSQL); // unhandled case error
				}
				break;						
			}
		}
	}

	if(ACCESSPLANELEMENTS[m_currPlanIndex].classInfo.classKind == CLASSKIND_TEMPORARY || ACCESSPLANELEMENTS[m_currPlanIndex].classInfo.classKind == CLASSKIND_SORTSTREAM)
	{
		oid = EVAL_EVALBUFFER[m_currPlanIndex].getColSlotPtr(0)->data.oid;
		EVAL_EVALBUFFER[m_currPlanIndex].setOID(&oid);	
	}
	else
		oid = *(EVAL_EVALBUFFER[m_currPlanIndex].getOID_Ptr());

	if(index > 0)
	{
		e = m_storageManager->UpdateObjectByColList(EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeOcn, SM_FALSE, 
										            &oid, index,
										            &(EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[0]));
		if(e < eNOERROR) OOSQL_ERR(e);
	}

	for(index = 0, i = 0; i < updateValueList.size; i++)
	{
		if((updateValueList[i].type == TYPEID_STRING || updateValueList[i].type == TYPEID_VARSTRING) && !updateValueList[i].isParam)
        {
            if(EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].data.ptr)
            {
			    pMemoryManager->Free(EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].data.ptr);
                EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].data.ptr = NULL;
            }
        }
		if(updateValueList[i].type != TYPEID_TEXT && !updateValueList[i].isParam)
		{
			if (OOSQL_MASK_COMPLEXTYPE(updateValueList[i].type) != COMPLEXTYPEID_SET && 
				OOSQL_MASK_COMPLEXTYPE(updateValueList[i].type) != COMPLEXTYPEID_BAG &&
				OOSQL_MASK_COMPLEXTYPE(updateValueList[i].type) != COMPLEXTYPEID_LIST)
				index ++;
		}
	}

	for(i = 0; i < updateValueList.size; i++)
	{
		if(updateValueList[i].type == TYPEID_TEXT && !updateValueList[i].isParam)
		{
			OOSQL_StorageManager::TextColStruct		text;
			OOSQL_StorageManager::TextDesc			textDesc;
			AP_ExprPoolElements						expr;
			ValuePoolElements						value;

			expr = updateValueList[i].expr.getElements(m_pool->ap_exprPool);
			if(expr[0].exprKind != EXPR_KIND_VALUE)
				continue;

			value = expr[0].value.getElements(m_pool->valuePool);

			if(value[0].valueKind == VALUE_KIND_NIL)
			{
				e = m_storageManager->Text_GetDescriptor(EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeOcn, SM_FALSE,
													&oid, updateValueList[i].colNo, &textDesc);
				if(e < eNOERROR) OOSQL_ERR(e);

				if(!OOSQL_StorageManager::DoesNoContentExistTextDesc(textDesc))
				{
					e = m_storageManager->Text_DestroyContent(EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeOcn, SM_FALSE, 
															  &oid, updateValueList[i].colNo, &textDesc);
					if(e < eNOERROR) OOSQL_ERR(e);
				}
			}
			else
			{
				text.start      = 0;
				text.length     = value[0].text.size;
				text.dataLength = value[0].text.size;
				text.data       = &(value[0].text.getElements(m_pool->stringPool, 0));
				if(value[0].textUpdateMode == TEXT_UPDATE_MODE_DEFERRED)
					text.indexMode = OOSQL_StorageManager::DEFERRED_MODE;
				else
					text.indexMode = OOSQL_StorageManager::IMMEDIATE_MODE;

				e = m_storageManager->Text_GetDescriptor(EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeOcn, SM_FALSE,
													&oid, updateValueList[i].colNo, &textDesc);
				if(e < eNOERROR) OOSQL_ERR(e);

				if(!OOSQL_StorageManager::DoesNoContentExistTextDesc(textDesc))
				{
					e = m_storageManager->Text_DestroyContent(EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeOcn, SM_FALSE, 
															  &oid, updateValueList[i].colNo, &textDesc);
					if(e < eNOERROR) OOSQL_ERR(e);
				}

				// create new content
				e = m_storageManager->Text_CreateContent(EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeOcn, SM_FALSE,
														 &oid, updateValueList[i].colNo, &text, &textDesc);
				if(e < eNOERROR) OOSQL_ERR(e);
			}
		}
		else if((OOSQL_MASK_COMPLEXTYPE(updateValueList[i].type) == COMPLEXTYPEID_SET ||
				 OOSQL_MASK_COMPLEXTYPE(updateValueList[i].type) == COMPLEXTYPEID_BAG ||
				 OOSQL_MASK_COMPLEXTYPE(updateValueList[i].type) == COMPLEXTYPEID_LIST) && 
				!updateValueList[i].isParam)
		{
			AP_ExprPoolElements				    expr;
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
			Four								elementSize;
			void*								element;

			expr = updateValueList[i].expr.getElements(m_pool->ap_exprPool);
			if(expr[0].exprKind != EXPR_KIND_VALUE)
				continue;

			value         = expr[0].value.getElements(m_pool->valuePool);
			complexValues = value[0].complex;

			if(value[0].valueKind == VALUE_KIND_NIL)
			{
				switch(OOSQL_MASK_COMPLEXTYPE(updateValueList[i].type))
				{
				case COMPLEXTYPEID_SET: 
                    e = m_storageManager->CollectionSet_IsNull(EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeOcn, SM_FALSE, &oid, 
																updateValueList[i].colNo);
                    if ((Boolean)e == SM_FALSE)
                    {
					    e = m_storageManager->CollectionSet_Destroy(EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeOcn, SM_FALSE, &oid, 
																    updateValueList[i].colNo);
    					OOSQL_CHECK_ERR(e);
                    }
					break;
				case COMPLEXTYPEID_BAG: 
                    e = m_storageManager->CollectionBag_IsNull(EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeOcn, SM_FALSE, &oid, 
																updateValueList[i].colNo);
                    if ((Boolean)e == SM_FALSE)
                    {
					    e = m_storageManager->CollectionBag_Destroy(EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeOcn, SM_FALSE, &oid, 
																    updateValueList[i].colNo);
					    OOSQL_CHECK_ERR(e);
                    }
					break;
				case COMPLEXTYPEID_LIST: 
                    e = m_storageManager->CollectionList_IsNull(EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeOcn, SM_FALSE, &oid, 
																updateValueList[i].colNo);
                    if ((Boolean)e == SM_FALSE)
                    {
					    e = m_storageManager->CollectionList_Destroy(EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeOcn, SM_FALSE, &oid, 
																     updateValueList[i].colNo);
					    OOSQL_CHECK_ERR(e);
                    }
					break;
				}
			}
			else
			{
				switch(OOSQL_MASK_COMPLEXTYPE(updateValueList[i].type))
				{
				case COMPLEXTYPEID_SET: 
                    e = m_storageManager->CollectionSet_IsNull(EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeOcn, SM_FALSE, &oid, 
																updateValueList[i].colNo);
                    if ((Boolean)e == SM_TRUE)
                    {
					    e = m_storageManager->CollectionSet_Create(EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeOcn, SM_FALSE, &oid, 
															       updateValueList[i].colNo, ALL_VALUE);
						OOSQL_CHECK_ERR(e);
                    }
                    else
                    {
						e = m_storageManager->CollectionSet_DeleteAll(EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeOcn, SM_FALSE, &oid, 
																	  updateValueList[i].colNo);
						OOSQL_CHECK_ERR(e);
					}
					break;
				case COMPLEXTYPEID_BAG: 
                    e = m_storageManager->CollectionBag_IsNull(EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeOcn, SM_FALSE, &oid, 
																updateValueList[i].colNo);
                    if ((Boolean)e == SM_TRUE)
                    {
					    e = m_storageManager->CollectionBag_Create(EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeOcn, SM_FALSE, &oid, 
															       updateValueList[i].colNo, ALL_VALUE);
						OOSQL_CHECK_ERR(e);
                    }
                    else
                    {
						e = m_storageManager->CollectionBag_DeleteAll(EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeOcn, SM_FALSE, &oid, 
																	  updateValueList[i].colNo);
						OOSQL_CHECK_ERR(e);
					}
					break;
				case COMPLEXTYPEID_LIST: 
                    e = m_storageManager->CollectionList_IsNull(EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeOcn, SM_FALSE, &oid, 
																updateValueList[i].colNo);
                    if ((Boolean)e == SM_TRUE)
                    {
					    e = m_storageManager->CollectionList_Create(EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeOcn, SM_FALSE, &oid, 
															        updateValueList[i].colNo);
						OOSQL_CHECK_ERR(e);
                    }
                    else
                    {
						e = m_storageManager->CollectionList_DeleteAll(EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeOcn, SM_FALSE, &oid, 
															 		   updateValueList[i].colNo);
						OOSQL_CHECK_ERR(e);
					}
					break;
				}

				for(j = 0; j < complexValues.size; j++)
				{
					stringBuffer = NULL;
					switch(OOSQL_MASK_TYPE(updateValueList[i].type))
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
							elementSize = sizeof(double);
							element     = &doubleAttr;
						}
						else if(complexValues[j].valueKind == VALUE_KIND_REAL)
						{
							doubleAttr  = complexValues[j].real.getElements(m_pool->realPool, 0);
							elementSize = sizeof(double);
							element     = &doubleAttr;
						}
						else
							OOSQL_ERR(eNOTIMPLEMENTED_OOSQL); // unhandled case error
						break;
					case TYPEID_STRING:
						if(complexValues[j].valueKind == VALUE_KIND_STRING)
						{
							elementSize = updateValueList[i].length;

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

					switch(OOSQL_MASK_COMPLEXTYPE(updateValueList[i].type))
					{
					case COMPLEXTYPEID_SET:
						e = m_storageManager->CollectionSet_InsertElements(EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeOcn, SM_FALSE, &oid, 
																		   updateValueList[i].colNo, 1, &elementSize, element);
						if(e == eCOLLECTIONSET_ELEMENTEXIST_LRDS)
							OOSQL_ERR(eDUPLICATED_ELEMENTS_ARE_INSERTED_SET_OOSQL)
						OOSQL_CHECK_ERR(e);
						break;
					case COMPLEXTYPEID_BAG:
						e = m_storageManager->CollectionBag_InsertElements(EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeOcn, SM_FALSE, &oid, 
																		   updateValueList[i].colNo, 1, &elementSize, element);
						OOSQL_CHECK_ERR(e);
						break;
					case COMPLEXTYPEID_LIST:
						e = m_storageManager->CollectionList_InsertElements(EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeOcn, SM_FALSE, &oid, 
																			updateValueList[i].colNo, j, 1, &elementSize, element);
						OOSQL_CHECK_ERR(e);
						break;
					}
					if(stringBuffer)
						pMemoryManager->Free(stringBuffer);
					stringBuffer = NULL;
				}
			}
		}
	}

	return eNOERROR;
}

Four OOSQL_Evaluator::projectionForInsert(
    One isGroupByResult         /* IN: */
)
/*
    Function:

    Side effect:
        m_evalStatus.queryResultBufferFull can be changed to SM_TRUE.

    Referenced member variables:
        m_currPlanIndex

    Return value:
*/
{
    AP_ProjectionListPoolElements	projList;
    Four							resultTupleSize;
    Four							i, j;
    Four							e;
	AP_UpdateValuePoolElements		updateValueList;
	AP_InsertValuePoolElements		insertValueList;
	Boolean							isParameterExist;
	Boolean							isAllValueNull;
	OOSQL_StorageManager::OID 		oid;
	Four							index;
	AP_ProjectionPoolElements		pProjection;
	AP_UsedColPoolElements			usedCol;
	AP_ColNoMapPoolElements			colNoMap;
	OOSQL_DB_Value					res(pMemoryManager);
	Four							nDistinctElements;
	char							seqName[MAXCLASSNAME];
	Four							seqValue;
	AP_OperatorStruct				getGeometryTypeForRes;
	OOSQL_DB_Value 					geometryTypeRes(pMemoryManager);
	AP_ExprPoolElements				exprPool(m_pool->ap_exprPool);

#ifdef  OOSQL_DEBUG
    /* assert that projection for the current plan element is necessary */
    if (IS_NULL_POOLINDEX(ACCESSPLANELEMENTS[m_currPlanIndex].projectionList))
        OOSQL_ERR(eINTERNAL_INCORRECTEXECSEQUENCE_OOSQL);
#endif

    /* get projection list for the current acccess plan element */
    projList = ACCESSPLAN.getProjectionListPool(m_currPlanIndex);
	
	/* get current plan's column information */
	colNoMap = ACCESSPLAN.getColNoMapPool(m_currPlanIndex);
	usedCol  = ACCESSPLAN.getUsedColPool(m_currPlanIndex);

	if(projList[0].projectionType != PROJECTION_INSERT)
		OOSQL_ERR(eINTERNALERROR_OOSQL);

	m_numQueryResultTuples ++;
	insertValueList  = projList[0].insertInfo.insertValueList.getElements(m_pool->ap_insertValuePool);
	isParameterExist = SM_FALSE;
	isAllValueNull   = SM_TRUE;

	for(i = 0; i < insertValueList.size; i++)
	{
		if(insertValueList[i].isParam)
			isParameterExist = SM_TRUE;
		if(insertValueList[i].value != NULL_POOLINDEX)
			isAllValueNull = SM_FALSE;
	}

	if(isAllValueNull == SM_TRUE && isParameterExist == SM_FALSE)
	{
		if (isLastPlanElement()) 
		{
			if(ACCESSPLANELEMENTS[m_currPlanIndex].selDistinctFlag)
			{	
				nDistinctElements = EVAL_EVALBUFFER[m_currPlanIndex].nCols - m_pool->targetListPool.nElements(); 

				pProjection = ACCESSPLAN.getProjectionPool(projList[0].projectionInfo);
				for(i = 0; i < nDistinctElements; i++)
				{
					if(EVAL_EVALBUFFER[m_currPlanIndex].clist[i].nullFlag != 
					   EVAL_EVALBUFFER[m_currPlanIndex].prevColList[i].nullFlag)
						break;

					if(EVAL_EVALBUFFER[m_currPlanIndex].clist[i].retLength != 
					   EVAL_EVALBUFFER[m_currPlanIndex].prevColList[i].retLength)
						break;

					if(!EVAL_EVALBUFFER[m_currPlanIndex].prevColList[i].nullFlag)
					{
						if(pProjection[i].resultType == OOSQL_TYPE_STRING || pProjection[i].resultType == OOSQL_TYPE_VARSTRING)
						{
							if(memcmp(EVAL_EVALBUFFER[m_currPlanIndex].clist[i].data.ptr, 
									  EVAL_EVALBUFFER[m_currPlanIndex].prevColList[i].data.ptr,
									  EVAL_EVALBUFFER[m_currPlanIndex].clist[i].retLength))
								break;
						}
						else
						{
							if(memcmp(&EVAL_EVALBUFFER[m_currPlanIndex].clist[i].data.s, 
									  &EVAL_EVALBUFFER[m_currPlanIndex].prevColList[i].data.s,
									  EVAL_EVALBUFFER[m_currPlanIndex].clist[i].retLength))
								break;
						}
					}
				}

				if(i == nDistinctElements)
					return eNOERROR;

				for(i = 0; i < nDistinctElements; i++)
				{
					EVAL_EVALBUFFER[m_currPlanIndex].prevColList[i].retLength = EVAL_EVALBUFFER[m_currPlanIndex].clist[i].retLength;
					EVAL_EVALBUFFER[m_currPlanIndex].prevColList[i].nullFlag  = EVAL_EVALBUFFER[m_currPlanIndex].clist[i].nullFlag;
				
					if(!EVAL_EVALBUFFER[m_currPlanIndex].prevColList[i].nullFlag)
					{
						if(pProjection[i].resultType == OOSQL_TYPE_STRING || pProjection[i].resultType == OOSQL_TYPE_VARSTRING)
						{
							memcpy(EVAL_EVALBUFFER[m_currPlanIndex].prevColList[i].data.ptr,
								   EVAL_EVALBUFFER[m_currPlanIndex].clist[i].data.ptr, 
								   EVAL_EVALBUFFER[m_currPlanIndex].clist[i].retLength);
						}
						else
						{
							memcpy(&EVAL_EVALBUFFER[m_currPlanIndex].prevColList[i].data.s,
								   &EVAL_EVALBUFFER[m_currPlanIndex].clist[i].data.s, 
  								   EVAL_EVALBUFFER[m_currPlanIndex].clist[i].retLength);
						}
					}
				}
			}

			Four nColsCopied;
			e = projectToColListStruct(&projList[0], insertValueList.size, 
									   &(EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[0]), (Boolean)isGroupByResult, SM_TRUE,
									   nColsCopied);
			if(e < eNOERROR) OOSQL_ERR(e);
			index = nColsCopied;
		}
		else { /* projection to temporary file(s) */
			/* check prepareAndSortStatus */
			switch (m_evalStatus.prepareAndSortStatus) 
			{
				case EVALSTATUS_INIT:
					m_evalStatus.prepareAndSortStatus = EVALSTATUS_PROCESSING;

					/* save the index to access plan element from which projection is performed */
					if (m_evalStatus.lastPlanElemOfCurrNestedLoop < m_currPlanIndex)
						m_evalStatus.lastPlanElemOfCurrNestedLoop = m_currPlanIndex;
					break;

				case EVALSTATUS_PROCESSING:
					break;

				case EVALSTATUS_END:
					/* just return because projection is ended */
					return eNOERROR;

				default:
					OOSQL_ERR(eINVALID_CASE_OOSQL);
			}

			/* process each projection element */
			if(m_evalStatus.aggregationStatus != EVALSTATUS_PROCESSING)
			{
				for (i = 0; i < GET_POOLSIZE(projList); i++) 
				{
					e = processProjectionToTempFile(&projList[i], (Boolean)isGroupByResult);
					if (e < eNOERROR)
						OOSQL_ERR(e);
				}
			}
		}
	}
	else
	{
		for(index = 0, i = 0; i < insertValueList.size; i++)
		{
			ValuePoolElements	value;
			OOSQL_StorageManager::ColListStruct*		colList;
			OOSQL_StorageManager::ColListStruct*		writeColList;

			if(insertValueList[i].isParam)
				continue;

			if(ACCESSPLANELEMENTS[m_currPlanIndex].classInfo.classKind == CLASSKIND_TEMPORARY || ACCESSPLANELEMENTS[m_currPlanIndex].classInfo.classKind == CLASSKIND_SORTSTREAM)
			{
				colList = EVAL_EVALBUFFER[m_currPlanIndex].getColSlotPtr(i);
				writeColList = &(EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index]);
				memcpy(&(writeColList->data), &(colList->data), sizeof(colList->data));
				index ++;
			}
			else
			{
				value = insertValueList[i].value.getElements(m_pool->valuePool);

				EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].nullFlag = SM_FALSE;
				switch(insertValueList[i].type)
				{
				case TYPEID_SHORT:
					if(value[0].valueKind == VALUE_KIND_INTEGER)
						EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].data.s = value[0].integer.getElements(m_pool->intPool, 0);
					else if(value[0].valueKind == VALUE_KIND_REAL)
						EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].data.s = value[0].real.getElements(m_pool->realPool, 0);
					else if(value[0].valueKind == VALUE_KIND_NIL)
						EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].nullFlag = SM_TRUE;
					else if(insertValueList[i].seqValueType == SEQ_CURRVAL)
					{
						memcpy(seqName, &(value[0].string.getElements(m_pool->stringPool, 0)), value[0].string.size);
						seqName[value[0].string.size] = '\0';
						m_storageManager->GetSeqCurrVal(m_volID, seqName, &seqValue);
						EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].data.s = (Two_Invariable)seqValue;
					}
					else if(insertValueList[i].seqValueType == SEQ_NEXTVAL)
					{
						memcpy(seqName, &(value[0].string.getElements(m_pool->stringPool, 0)), value[0].string.size);
						seqName[value[0].string.size] = '\0';
						m_storageManager->GetSeqNextVal(m_volID, seqName, &seqValue);
						EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].data.s = (Two_Invariable)seqValue;
					}
					else
						OOSQL_ERR(eNOTIMPLEMENTED_OOSQL); // unhandled case error
					index ++;
					break;
				case TYPEID_INT:
					if(value[0].valueKind == VALUE_KIND_INTEGER)
						EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].data.i = value[0].integer.getElements(m_pool->intPool, 0);
					else if(value[0].valueKind == VALUE_KIND_REAL)
						EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].data.i = value[0].real.getElements(m_pool->realPool, 0);
					else if(value[0].valueKind == VALUE_KIND_NIL)
						EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].nullFlag = SM_TRUE;
					else if(insertValueList[i].seqValueType == SEQ_CURRVAL)
					{
						memcpy(seqName, &(value[0].string.getElements(m_pool->stringPool, 0)), value[0].string.size);
						seqName[value[0].string.size] = '\0';
						m_storageManager->GetSeqCurrVal(m_volID, seqName, &seqValue);
						EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].data.i = (Four_Invariable)seqValue;
					}
					else if(insertValueList[i].seqValueType == SEQ_NEXTVAL)
					{
						memcpy(seqName, &(value[0].string.getElements(m_pool->stringPool, 0)), value[0].string.size);
						seqName[value[0].string.size] = '\0';
						m_storageManager->GetSeqNextVal(m_volID, seqName, &seqValue);
						EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].data.i = (Four_Invariable)seqValue;
					}
					else
						OOSQL_ERR(eNOTIMPLEMENTED_OOSQL); // unhandled case error
					index ++;
					break;
				case TYPEID_LONG:
					if(value[0].valueKind == VALUE_KIND_INTEGER)
						EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].data.l = value[0].integer.getElements(m_pool->intPool, 0);
					else if(value[0].valueKind == VALUE_KIND_REAL)
						EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].data.l = value[0].real.getElements(m_pool->realPool, 0);
					else if(value[0].valueKind == VALUE_KIND_NIL)
						EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].nullFlag = SM_TRUE;
					else if(insertValueList[i].seqValueType == SEQ_CURRVAL)
					{
						memcpy(seqName, &(value[0].string.getElements(m_pool->stringPool, 0)), value[0].string.size);
						seqName[value[0].string.size] = '\0';
						m_storageManager->GetSeqCurrVal(m_volID, seqName, &seqValue);
						EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].data.l = (Four_Invariable)seqValue;
					}
					else if(insertValueList[i].seqValueType == SEQ_NEXTVAL)
					{
						memcpy(seqName, &(value[0].string.getElements(m_pool->stringPool, 0)), value[0].string.size);
						seqName[value[0].string.size] = '\0';
						m_storageManager->GetSeqNextVal(m_volID, seqName, &seqValue);
						EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].data.l = (Four_Invariable)seqValue;
					}
					else
						OOSQL_ERR(eNOTIMPLEMENTED_OOSQL); // unhandled case error
					index ++;
					break;
				case TYPEID_LONG_LONG:
					if(value[0].valueKind == VALUE_KIND_INTEGER)
						EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].data.ll = value[0].integer.getElements(m_pool->intPool, 0);
					else if(value[0].valueKind == VALUE_KIND_REAL)
						EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].data.ll = value[0].real.getElements(m_pool->realPool, 0);
					else if(value[0].valueKind == VALUE_KIND_NIL)
						EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].nullFlag = SM_TRUE;
					else if(insertValueList[i].seqValueType == SEQ_CURRVAL)
					{
						memcpy(seqName, &(value[0].string.getElements(m_pool->stringPool, 0)), value[0].string.size);
						seqName[value[0].string.size] = '\0';
						m_storageManager->GetSeqCurrVal(m_volID, seqName, &seqValue);
						EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].data.ll = (Eight_Invariable)seqValue;
					}
					else if(insertValueList[i].seqValueType == SEQ_NEXTVAL)
					{
						memcpy(seqName, &(value[0].string.getElements(m_pool->stringPool, 0)), value[0].string.size);
						seqName[value[0].string.size] = '\0';
						m_storageManager->GetSeqNextVal(m_volID, seqName, &seqValue);
						EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].data.ll = (Eight_Invariable)seqValue;
					}
					else
						OOSQL_ERR(eNOTIMPLEMENTED_OOSQL); // unhandled case error
					index ++;
					break;
				case TYPEID_FLOAT:
					if(value[0].valueKind == VALUE_KIND_INTEGER)
						EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].data.f = value[0].integer.getElements(m_pool->intPool, 0);
					else if(value[0].valueKind == VALUE_KIND_REAL)
						EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].data.f = value[0].real.getElements(m_pool->realPool, 0);
					else if(value[0].valueKind == VALUE_KIND_NIL)
						EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].nullFlag = SM_TRUE;
					else
						OOSQL_ERR(eNOTIMPLEMENTED_OOSQL); // unhandled case error
					index ++;
					break;
				case TYPEID_DOUBLE:
					if(value[0].valueKind == VALUE_KIND_INTEGER)
						EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].data.d = value[0].integer.getElements(m_pool->intPool, 0);
					else if(value[0].valueKind == VALUE_KIND_REAL)
						EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].data.d = value[0].real.getElements(m_pool->realPool, 0);
					else if(value[0].valueKind == VALUE_KIND_NIL)
						EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].nullFlag = SM_TRUE;
					else
						OOSQL_ERR(eNOTIMPLEMENTED_OOSQL); // unhandled case error
					index ++;
					break;

				case TYPEID_STRING:
					if(value[0].valueKind == VALUE_KIND_STRING)
					{
						EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].data.ptr   = pMemoryManager->Alloc(insertValueList[i].length);
						EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].dataLength = insertValueList[i].length;

						for(j = 0; j < insertValueList[i].length; j++)
						{
							if(j < value[0].string.size)
								((char*)EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].data.ptr)[j] = value[0].string.getElements(m_pool->stringPool, j);
							else
								((char*)EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].data.ptr)[j] = 0;
						}
					}
					else if(value[0].valueKind == VALUE_KIND_NIL)
						EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].nullFlag = SM_TRUE;
					else
						OOSQL_ERR(eNOTIMPLEMENTED_OOSQL); // unhandled case error
					index ++;
					break;

				case TYPEID_VARSTRING:
					if(value[0].valueKind == VALUE_KIND_STRING)
					{
						EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].data.ptr = &(value[0].string.getElements(m_pool->stringPool, 0));
						EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].dataLength = value[0].string.size; 
					}
					else if(value[0].valueKind == VALUE_KIND_NIL)
						EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].nullFlag = SM_TRUE;
					else
						OOSQL_ERR(eNOTIMPLEMENTED_OOSQL); // unhandled case error
					index ++;
					break;

				case TYPEID_TEXT:
					break;

				case TYPEID_DATE:
					if(value[0].valueKind == VALUE_KIND_DATE)
					{
						m_storageManager->SetDate(value[0].date.getElements(m_pool->datePool, 0).year,
											      value[0].date.getElements(m_pool->datePool, 0).month, 
									              value[0].date.getElements(m_pool->datePool, 0).day,
									              &EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].data.date);
					}
					else if(value[0].valueKind == VALUE_KIND_NIL)
						EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].nullFlag = SM_TRUE;
					else
						OOSQL_ERR(eNOTIMPLEMENTED_OOSQL); // unhandled case error
					index++;	
					break;
				case TYPEID_TIME:
					if(value[0].valueKind == VALUE_KIND_TIME)
					{
						EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].data.time._tzHour   = 0;
						EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].data.time._tzMinute = 0;
						EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].data.time._100thSec = 0;
						EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].data.time._Hour   = 
							value[0].time.getElements(m_pool->timePool, 0).hour;
						EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].data.time._Minute = 
							value[0].time.getElements(m_pool->timePool, 0).minute;
						EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].data.time._Second = 
							value[0].time.getElements(m_pool->timePool, 0).second;
					}
					else if(value[0].valueKind == VALUE_KIND_NIL)
						EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].nullFlag = SM_TRUE;
					else
						OOSQL_ERR(eNOTIMPLEMENTED_OOSQL); // unhandled case error
					index++;
					break;
				case TYPEID_TIMESTAMP:
					if(value[0].valueKind == VALUE_KIND_TIMESTAMP)
					{
						m_storageManager->SetDate(
									value[0].timestamp.getElements(m_pool->timestampPool, 0).year,
									value[0].timestamp.getElements(m_pool->timestampPool, 0).month, 
									value[0].timestamp.getElements(m_pool->timestampPool, 0).day,
									&EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].data.timestamp.d);
						EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].data.timestamp.t._tzHour   = 0;
						EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].data.timestamp.t._tzMinute = 0;
						EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].data.timestamp.t._100thSec = 0;
						EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].data.timestamp.t._Hour   = 
							value[0].timestamp.getElements(m_pool->timestampPool, 0).hour;
						EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].data.timestamp.t._Minute = 
							value[0].timestamp.getElements(m_pool->timestampPool, 0).minute;
						EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].data.timestamp.t._Second = 
							value[0].timestamp.getElements(m_pool->timestampPool, 0).second;
					}
					else if(value[0].valueKind == VALUE_KIND_NIL)
						EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].nullFlag = SM_TRUE;
					else
						OOSQL_ERR(eNOTIMPLEMENTED_OOSQL); // unhandled case error
					index++;
					break;
				case TYPEID_OID:
                    if(value[0].valueKind == VALUE_KIND_STRING)
                    {
                        e = oosql_ConvertFromOIDStringToOIDStructure(
                            &value[0].string.getElements(m_pool->stringPool, 0),
                            &EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].data.oid
                        );
                        OOSQL_CHECK_ERR(e);
                    }
					else if(value[0].valueKind == VALUE_KIND_NIL)
						EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].nullFlag = SM_TRUE;
                    else
                        OOSQL_ERR(eNOTIMPLEMENTED_OOSQL); // unhandled case error
                    index ++;
                    break;

				case TYPEID_OGIS_GEOMETRY:
#ifndef SLIMDOWN_OPENGIS
					exprPool =  insertValueList[i].expr;
					e = evalOGISOperator(&exprPool[0].oper, res);
					if (e < eNOERROR) OOSQL_ERR(e);
					
					EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].data.ptr = pMemoryManager->Alloc(res.length);
					EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].dataLength = res.length;
					EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].nullFlag = res.nullFlag;

					if(!res.nullFlag)
						memcpy(EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].data.ptr, res.data.ptr, res.length);
					index++;
#else
					OOSQL_ERR(eOPENGIS_NOTENABLED_OOSQL);
#endif
					break;

				case TYPEID_OGIS_POINT:
#ifndef SLIMDOWN_OPENGIS
					exprPool =  insertValueList[i].expr;
#ifdef ENABLE_OPENGIS_CHECKTYPE
					getGeometryTypeForRes.operatorId = OP_OGIS_GEOMETRYTYPE;
					getGeometryTypeForRes.operatorType = OBJECT_OPERATION;
					getGeometryTypeForRes.operand1 =  insertValueList[i].expr;
					
					e = evalOGISOperator(&getGeometryTypeForRes, geometryTypeRes);
					if (e < eNOERROR) OOSQL_ERR(e);

					if(strcmp((char*)geometryTypeRes.data.ptr, "Point") != 0)
						OOSQL_ERR(eTYPE_ERROR_OOSQL);
#endif
					e = evalOGISOperator(&exprPool[0].oper, res);
					if (e < eNOERROR) OOSQL_ERR(e);
					EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].data.ptr = pMemoryManager->Alloc(res.length);
					EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].dataLength = res.length;
					EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].nullFlag = res.nullFlag;

					if(!res.nullFlag)
						memcpy(EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].data.ptr, res.data.ptr, res.length);
					index++;
#else
					OOSQL_ERR(eOPENGIS_NOTENABLED_OOSQL);
#endif
					break;

				case TYPEID_OGIS_LINESTRING:
#ifndef SLIMDOWN_OPENGIS
					exprPool =  insertValueList[i].expr;
#ifdef ENABLE_OPENGIS_CHECKTYPE
					getGeometryTypeForRes.operatorId = OP_OGIS_GEOMETRYTYPE;
					getGeometryTypeForRes.operatorType = OBJECT_OPERATION;
					getGeometryTypeForRes.operand1 =  insertValueList[i].expr;
					
					e = evalOGISOperator(&getGeometryTypeForRes, geometryTypeRes);
					if (e < eNOERROR) OOSQL_ERR(e);

					if(strcmp((char*)geometryTypeRes.data.ptr, "LineString") != 0)
						OOSQL_ERR(eTYPE_ERROR_OOSQL);
#endif
					e = evalOGISOperator(&exprPool[0].oper, res);
					if (e < eNOERROR) OOSQL_ERR(e);
					EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].data.ptr = pMemoryManager->Alloc(res.length);
					EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].dataLength = res.length;
					EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].nullFlag = res.nullFlag;

					if(!res.nullFlag)
						memcpy(EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].data.ptr, res.data.ptr, res.length);
					index++;
#else
					OOSQL_ERR(eOPENGIS_NOTENABLED_OOSQL);
#endif
					break;
					
				case TYPEID_OGIS_POLYGON:
#ifndef SLIMDOWN_OPENGIS
					exprPool =  insertValueList[i].expr;
#ifdef ENABLE_OPENGIS_CHECKTYPE
					getGeometryTypeForRes.operatorId = OP_OGIS_GEOMETRYTYPE;
					getGeometryTypeForRes.operatorType = OBJECT_OPERATION;
					getGeometryTypeForRes.operand1 =  insertValueList[i].expr;
					
					e = evalOGISOperator(&getGeometryTypeForRes, geometryTypeRes);
					if (e < eNOERROR) OOSQL_ERR(e);

					if(strcmp((char*)geometryTypeRes.data.ptr, "Polygon") != 0)
						OOSQL_ERR(eTYPE_ERROR_OOSQL);
#endif
					e = evalOGISOperator(&exprPool[0].oper, res);
					if (e < eNOERROR) OOSQL_ERR(e);
					EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].data.ptr = pMemoryManager->Alloc(res.length);
					EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].dataLength = res.length;
					EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].nullFlag = res.nullFlag;

					if(!res.nullFlag)
						memcpy(EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].data.ptr, res.data.ptr, res.length);
					index++;
#else
					OOSQL_ERR(eOPENGIS_NOTENABLED_OOSQL);
#endif
					break;
					
				case TYPEID_OGIS_GEOMETRYCOLLECTION:
#ifndef SLIMDOWN_OPENGIS
					exprPool =  insertValueList[i].expr;
#ifdef ENABLE_OPENGIS_CHECKTYPE
					getGeometryTypeForRes.operatorId = OP_OGIS_GEOMETRYTYPE;
					getGeometryTypeForRes.operatorType = OBJECT_OPERATION;
					getGeometryTypeForRes.operand1 =  insertValueList[i].expr;
					
					e = evalOGISOperator(&getGeometryTypeForRes, geometryTypeRes);
					if (e < eNOERROR) OOSQL_ERR(e);

					if(!(
						!strcmp((char*)geometryTypeRes.data.ptr, "GeometryCollection") ||
						!strcmp((char*)geometryTypeRes.data.ptr, "MultiPoint") ||
						!strcmp((char*)geometryTypeRes.data.ptr, "MultiLineString") ||
						!strcmp((char*)geometryTypeRes.data.ptr, "MultiPolygon") ))
						OOSQL_ERR(eTYPE_ERROR_OOSQL);
#endif
					e = evalOGISOperator(&exprPool[0].oper, res);
					if (e < eNOERROR) OOSQL_ERR(e);
					EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].data.ptr = pMemoryManager->Alloc(res.length);
					EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].dataLength = res.length;
					EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].nullFlag = res.nullFlag;

					if(!res.nullFlag)
						memcpy(EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].data.ptr, res.data.ptr, res.length);
					index++;
#else
					OOSQL_ERR(eOPENGIS_NOTENABLED_OOSQL);
#endif
					break;
				case TYPEID_OGIS_MULTIPOINT:
#ifndef SLIMDOWN_OPENGIS
					exprPool =  insertValueList[i].expr;
#ifdef ENABLE_OPENGIS_CHECKTYPE
					getGeometryTypeForRes.operatorId = OP_OGIS_GEOMETRYTYPE;
					getGeometryTypeForRes.operatorType = OBJECT_OPERATION;
					getGeometryTypeForRes.operand1 =  insertValueList[i].expr;
					
					e = evalOGISOperator(&getGeometryTypeForRes, geometryTypeRes);
					if (e < eNOERROR) OOSQL_ERR(e);

					if(strcmp((char*)geometryTypeRes.data.ptr, "MultiPoint") != 0)
						OOSQL_ERR(eTYPE_ERROR_OOSQL);
#endif
					e = evalOGISOperator(&exprPool[0].oper, res);
					if (e < eNOERROR) OOSQL_ERR(e);
					EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].data.ptr = pMemoryManager->Alloc(res.length);
					EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].dataLength = res.length;
					EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].nullFlag = res.nullFlag;

					if(!res.nullFlag)
						memcpy(EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].data.ptr, res.data.ptr, res.length);
					index++;
#else
					OOSQL_ERR(eOPENGIS_NOTENABLED_OOSQL);
#endif
					break;
					
				case TYPEID_OGIS_MULTILINESTRING:
#ifndef SLIMDOWN_OPENGIS
					exprPool =  insertValueList[i].expr;
#ifdef ENABLE_OPENGIS_CHECKTYPE
					getGeometryTypeForRes.operatorId = OP_OGIS_GEOMETRYTYPE;
					getGeometryTypeForRes.operatorType = OBJECT_OPERATION;
					getGeometryTypeForRes.operand1 =  insertValueList[i].expr;
					
					e = evalOGISOperator(&getGeometryTypeForRes, geometryTypeRes);
					if (e < eNOERROR) OOSQL_ERR(e);

					if(strcmp((char*)geometryTypeRes.data.ptr, "MultiLineString") != 0)
						OOSQL_ERR(eTYPE_ERROR_OOSQL);
#endif
					e = evalOGISOperator(&exprPool[0].oper, res);
					if (e < eNOERROR) OOSQL_ERR(e);
					EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].data.ptr = pMemoryManager->Alloc(res.length);
					EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].dataLength = res.length;
					EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].nullFlag = res.nullFlag;

					if(!res.nullFlag)
						memcpy(EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].data.ptr, res.data.ptr, res.length);
					index++;
#else
					OOSQL_ERR(eOPENGIS_NOTENABLED_OOSQL);
#endif
					break;
					
				case TYPEID_OGIS_MULTIPOLYGON:
#ifndef SLIMDOWN_OPENGIS
					exprPool =  insertValueList[i].expr;
#ifdef ENABLE_OPENGIS_CHECKTYPE
					getGeometryTypeForRes.operatorId = OP_OGIS_GEOMETRYTYPE;
					getGeometryTypeForRes.operatorType = OBJECT_OPERATION;
					getGeometryTypeForRes.operand1 =  insertValueList[i].expr;
					
					e = evalOGISOperator(&getGeometryTypeForRes, geometryTypeRes);
					if (e < eNOERROR) OOSQL_ERR(e);

					if(strcmp((char*)geometryTypeRes.data.ptr, "MultiPolygon") != 0)
						OOSQL_ERR(eTYPE_ERROR_OOSQL);
#endif
					e = evalOGISOperator(&exprPool[0].oper, res);
					if (e < eNOERROR) OOSQL_ERR(e);
					EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].data.ptr = pMemoryManager->Alloc(res.length);
					EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].dataLength = res.length;
					EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].nullFlag = res.nullFlag;

					if(!res.nullFlag)
						memcpy(EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].data.ptr, res.data.ptr, res.length);
					index++;
#else
					OOSQL_ERR(eOPENGIS_NOTENABLED_OOSQL);
#endif
					break;

                case TYPEID_PAGEID:
				case TYPEID_FILEID:
				case TYPEID_INDEXID:
				case TYPEID_MBR:
				case TYPEID_INTERVAL:
					OOSQL_ERR(eNOTIMPLEMENTED_OOSQL);
				default:
					switch(OOSQL_MASK_COMPLEXTYPE(insertValueList[i].type))
					{
					case COMPLEXTYPEID_SET:
					case COMPLEXTYPEID_BAG:
					case COMPLEXTYPEID_LIST:
						break;
					default:
						OOSQL_ERR(eNOTIMPLEMENTED_OOSQL); // unhandled case error
					}
					break;						
				}
			}
		}
	}
	e = m_storageManager->CreateObjectByColList(EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeOcn, SM_FALSE,
									            index,	
									            &(EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[0]), &oid);
	if(e < eNOERROR) OOSQL_ERR(e);

	for(index = 0, i = 0; i < insertValueList.size; i++)
	{
		if(insertValueList[i].type == TYPEID_STRING && !insertValueList[i].isParam)	
        {
            if(EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].data.ptr)
            {
			    pMemoryManager->Free(EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].data.ptr);
                EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeColList[index].data.ptr = NULL;
            }
        }
		if(insertValueList[i].type != TYPEID_TEXT && !insertValueList[i].isParam)
			index ++;
	}

	for(i = 0; i < insertValueList.size; i++)
	{
		if(insertValueList[i].type == TYPEID_TEXT && !insertValueList[i].isParam)
		{
			OOSQL_StorageManager::TextColStruct				text;
			OOSQL_StorageManager::TextDesc					textDesc;
			ValuePoolElements								value;

			value = insertValueList[i].value.getElements(m_pool->valuePool);
			if(isAllValueNull == SM_TRUE && isParameterExist == SM_FALSE && value == NULL_POOLINDEX)
			{	
				AP_ProjectionPoolElements projection;

				projection = ACCESSPLAN.getProjectionPool(projList[0].projectionInfo);
				if (projection[i].projectionKind != PROJECTION_KIND_PATHEXPR)
					OOSQL_ERR(eBADPARAMETER_OOSQL);

				AP_PathExprAccessInfo pathExpr = projection[i].pathExpr;
				if(pathExpr.kind != PATHEXPR_KIND_ATTR)
					OOSQL_ERR(eBADPARAMETER_OOSQL);

				OOSQL_StorageManager::OID  sourceOID;
				Two  sourceColNo;
				Four sourceOcn;
				Four sourcePlanNo;

				sourcePlanNo = pathExpr.col.planNo; 
				sourceColNo  = pathExpr.col.colNo;
				sourceOcn    = EVAL_ACCESSLISTTABLE[sourcePlanNo].getCurrOcn();

				if(ACCESSPLANELEMENTS[sourcePlanNo].classInfo.classKind == CLASSKIND_TEMPORARY || ACCESSPLANELEMENTS[sourcePlanNo].classInfo.classKind == CLASSKIND_SORTSTREAM)
					sourceOID = EVAL_EVALBUFFER[sourcePlanNo].getColSlotPtr(0)->data.oid;
				else
					sourceOID = *(EVAL_EVALBUFFER[sourcePlanNo].getOID_Ptr());

				e = m_storageManager->Text_GetDescriptor(sourceOcn, SM_FALSE, &sourceOID, sourceColNo, &textDesc);
				if (e < eNOERROR) OOSQL_ERR(e);

				char* buffer = (char*)pMemoryManager->Alloc(OOSQL_EVALBUFFER_MAXSTRINGSIZE);
				Four  bufferSize = OOSQL_EVALBUFFER_MAXSTRINGSIZE;
				while(1)
				{
					text.start      = 0;
					text.length     = bufferSize;
					text.dataLength = bufferSize;
					text.data       = buffer;
					e = m_storageManager->Text_FetchContent(sourceOcn, SM_FALSE, &sourceOID, sourceColNo, &text, &textDesc);
					if (e < eNOERROR) OOSQL_ERR(e);

					if(text.retLength == bufferSize)
					{
						bufferSize = bufferSize * 2;
						buffer     = (char*)pMemoryManager->Realloc(buffer, bufferSize);
					}
					else
						break;
				}

				text.start      = 0;
				text.length     = text.retLength;
				text.dataLength = text.retLength;
				text.data       = buffer;
				text.indexMode  = OOSQL_StorageManager::IMMEDIATE_MODE;

				OOSQL_StorageManager::MakeNullTextDesc(textDesc);
				e = m_storageManager->Text_CreateContent(EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeOcn, SM_FALSE,
														 &oid, insertValueList[i].colNo, &text, &textDesc);
				if(e < eNOERROR) OOSQL_ERR(e);

				pMemoryManager->Free(buffer);
			}
			else if(value[0].valueKind != VALUE_KIND_NIL)
			{
				text.start      = 0;
				text.length     = value[0].text.size;
				text.dataLength = value[0].text.size;
				text.data       = &(value[0].text.getElements(m_pool->stringPool, 0));
				if(value[0].textUpdateMode == TEXT_UPDATE_MODE_DEFERRED)
					text.indexMode = OOSQL_StorageManager::DEFERRED_MODE;
				else
					text.indexMode = OOSQL_StorageManager::IMMEDIATE_MODE;

				OOSQL_StorageManager::MakeNullTextDesc(textDesc);

				e = m_storageManager->Text_CreateContent(EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeOcn, SM_FALSE,
														 &oid, insertValueList[i].colNo, &text, &textDesc);
				if(e < eNOERROR) OOSQL_ERR(e);
			}
		}
		else if((OOSQL_MASK_COMPLEXTYPE(insertValueList[i].type) == COMPLEXTYPEID_SET ||
				 OOSQL_MASK_COMPLEXTYPE(insertValueList[i].type) == COMPLEXTYPEID_BAG ||
				 OOSQL_MASK_COMPLEXTYPE(insertValueList[i].type) == COMPLEXTYPEID_LIST) && 
				!insertValueList[i].isParam)
		{
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
			Four								elementSize;
			void*								element;

			value = insertValueList[i].value.getElements(m_pool->valuePool);
			if(value != NULL_POOLINDEX)
				complexValues = value[0].complex;
			else
				complexValues.setNull();

			if(value[0].valueKind != VALUE_KIND_NIL)
			{
				switch(OOSQL_MASK_COMPLEXTYPE(insertValueList[i].type))
				{
				case COMPLEXTYPEID_SET: 
					e = m_storageManager->CollectionSet_Create(EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeOcn, SM_FALSE, &oid, 
														   insertValueList[i].colNo, ALL_VALUE);
					OOSQL_CHECK_ERR(e);
					break;
				case COMPLEXTYPEID_BAG: 
					e = m_storageManager->CollectionBag_Create(EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeOcn, SM_FALSE, &oid, 
														   insertValueList[i].colNo, ALL_VALUE);
					OOSQL_CHECK_ERR(e);
					break;
				case COMPLEXTYPEID_LIST: 
					e = m_storageManager->CollectionList_Create(EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeOcn, SM_FALSE, &oid, 
																insertValueList[i].colNo);
					OOSQL_CHECK_ERR(e);
					break;
				}

				if(isAllValueNull == SM_TRUE && isParameterExist == SM_FALSE && complexValues == NULL_POOLINDEX)
				{
					AP_ProjectionPoolElements projection;

					projection = ACCESSPLAN.getProjectionPool(projList[0].projectionInfo);
					if (projection[i].projectionKind != PROJECTION_KIND_PATHEXPR)
						OOSQL_ERR(eBADPARAMETER_OOSQL);

					AP_PathExprAccessInfo pathExpr = projection[i].pathExpr;
					if(pathExpr.kind != PATHEXPR_KIND_ATTR)
						OOSQL_ERR(eBADPARAMETER_OOSQL);

					OOSQL_StorageManager::OID  sourceOID;
					Two  sourceColNo;
					Four sourceOcn;
					Four sourcePlanNo;

					sourcePlanNo = pathExpr.col.planNo; 
					sourceColNo  = pathExpr.col.colNo;
					sourceOcn    = EVAL_ACCESSLISTTABLE[sourcePlanNo].getCurrOcn();
					if(ACCESSPLANELEMENTS[sourcePlanNo].classInfo.classKind == CLASSKIND_TEMPORARY || ACCESSPLANELEMENTS[sourcePlanNo].classInfo.classKind == CLASSKIND_SORTSTREAM)
						sourceOID = EVAL_EVALBUFFER[sourcePlanNo].getColSlotPtr(0)->data.oid;
					else
						sourceOID = *(EVAL_EVALBUFFER[sourcePlanNo].getOID_Ptr());

					switch(OOSQL_MASK_COMPLEXTYPE(insertValueList[i].type))
					{
					case COMPLEXTYPEID_SET:
						e = m_storageManager->CollectionSet_Assign(sourceOcn, SM_FALSE, &sourceOID, sourceColNo,
								                                   EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeOcn, SM_FALSE, &oid, 
																   insertValueList[i].colNo);
						OOSQL_CHECK_ERR(e);
						break;
					case COMPLEXTYPEID_BAG:
						e = m_storageManager->CollectionBag_Assign(sourceOcn, SM_FALSE, &sourceOID, sourceColNo,
								                                   EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeOcn, SM_FALSE, &oid, 
																   insertValueList[i].colNo);
						break;
					case COMPLEXTYPEID_LIST:
						e = m_storageManager->CollectionList_Assign(sourceOcn, SM_FALSE, &sourceOID, sourceColNo,
								                                    EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeOcn, SM_FALSE, &oid, 
																 	insertValueList[i].colNo);
						OOSQL_CHECK_ERR(e);
						break;
					}
				}
				else
				{
					for(j = 0; j < complexValues.size; j++)
					{
						stringBuffer = NULL;
						switch(OOSQL_MASK_TYPE(insertValueList[i].type))
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
								elementSize = sizeof(double);
								element     = &doubleAttr;
							}
							else if(complexValues[j].valueKind == VALUE_KIND_REAL)
							{
								doubleAttr  = complexValues[j].real.getElements(m_pool->realPool, 0);
								elementSize = sizeof(double);
								element     = &doubleAttr;
							}
							else
								OOSQL_ERR(eNOTIMPLEMENTED_OOSQL); // unhandled case error
							break;
						case TYPEID_STRING:
							if(complexValues[j].valueKind == VALUE_KIND_STRING)
							{
								elementSize = insertValueList[i].length;

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

						switch(OOSQL_MASK_COMPLEXTYPE(insertValueList[i].type))
						{
						case COMPLEXTYPEID_SET:
							e = m_storageManager->CollectionSet_InsertElements(EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeOcn, SM_FALSE, &oid, 
																			   insertValueList[i].colNo, 1, &elementSize, element);
							if(e == eCOLLECTIONSET_ELEMENTEXIST_LRDS)
								OOSQL_ERR(eDUPLICATED_ELEMENTS_ARE_INSERTED_SET_OOSQL)
							OOSQL_CHECK_ERR(e);
							break;
						case COMPLEXTYPEID_BAG:
							e = m_storageManager->CollectionBag_InsertElements(EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeOcn, SM_FALSE, &oid, 
																			   insertValueList[i].colNo, 1, &elementSize, element);
							OOSQL_CHECK_ERR(e);
							break;
						case COMPLEXTYPEID_LIST:
							e = m_storageManager->CollectionList_InsertElements(EVAL_ACCESSLISTTABLE[m_currPlanIndex].writeOcn, SM_FALSE, &oid, 
																				insertValueList[i].colNo, j, 1, &elementSize, element);
							OOSQL_CHECK_ERR(e);
							break;
						}
						if(stringBuffer)
							pMemoryManager->Free(stringBuffer);
						stringBuffer = NULL;
					}
				}
			}
		}
	}

	EVAL_EVALBUFFER[m_currPlanIndex].setOID(&oid);

	return eNOERROR;
}

Four    OOSQL_Evaluator::prepareQueryResultInfo()
/*
    Function:

    Side effect:

    Return value:
*/
{
    Four    nQueryResCols;  // the # of query result columns
    Four    e;

    // get query result information
    nQueryResCols = ACCESSPLAN.getNumOfQueryResCols();
	OOSQL_NEW(m_queryResultInfo, pMemoryManager, OOSQL_ResultInfo(nQueryResCols));
	if (m_queryResultInfo == NULL)
		OOSQL_ERR(eOUTOFMEMORY_OOSQL);

    e = ACCESSPLAN.getQueryResultInfo(m_queryResultInfo);
    if (e < eNOERROR) {
        OOSQL_ERR(e);
    }

    return eNOERROR;
}


Four    OOSQL_Evaluator::processProjectionToCharBuf(
    AP_ProjectionListElement *projInfo, /* IN: */
    Four bufSize,                       /* IN: result buffer size */
    char *resultBuf,                    /* IN/OUT: ptr. to result buffer */
    Four *resultTupleSize,              /* OUT: result tuple size */
    Boolean grpByFlag                   /* IN: flag indicating projection for group by result */
)
/*
    Function:
        Project query result into the result buffer pointed to by 'resultBuf'.

    Side effect:

    Return value:
        eINSUFFICIENT_BUFSIZE_OOSQL     if buffer space for the query result is not adequate
        eNOERROR                        if no error
*/
{
    AP_ProjectionPoolElements pProjection;      /* ptr. to projection m_pool for the current A.P. elem. */
    Four *pVarColHdr;                           /* the # of projected variable sized columns */
    char *pFixedCol;                            /* ptr. to buf. position into which the next fixed col. is stored */
    char *pVarCol;                              /* ptr. to buf. position into which the next variable col. is stored */
    Four projPlanNo;                            /* projection plan no. */
    Two  projColNo;                             /* projection column no.  */
    EVAL_EvalBufferSlot *projBufSlot; 
    AP_ColNoMapPoolElements pColNoMap;          /* ptr. to column no. map m_pool */
    AP_UsedColPoolElements pUsedColPool;        /* ptr. to used column m_pool for the current A.P. elem. */
    Four mappedColNo;                           /* mapped column no. for projection col. no. */
    Four scanId;
    OOSQL_StorageManager::OID *oid;
    Four logicalID;                             /* for debugging Text queries */
    Four textDescLen;
    Four i;                                     /* loop iteration variable */
    Four e;                                     /* error code */

    // set pointer to result buffer
    pVarColHdr = (Four*)resultBuf;
    pFixedCol = resultBuf + (VARCOL_HDRSIZE * m_queryResultInfo->nVarCols);
    pVarCol = pFixedCol + m_queryResultInfo->firstVarColOffset;

    // initialize the size of result tuple
    *resultTupleSize = 0;

    pProjection = ACCESSPLAN.getProjectionPool(projInfo->projectionInfo);

    for (i = 0; i < GET_POOLSIZE(pProjection); i++) {
        switch (pProjection[i].projectionKind) {
            case PROJECTION_KIND_PATHEXPR:
                switch (pProjection[i].pathExpr.kind) {
                    case PATHEXPR_KIND_OBJECT:
                        OOSQL_ERR(eNOTIMPLEMENTED_OBJECTPROJ_OOSQL);

                    case PATHEXPR_KIND_OID:
                        projPlanNo = pProjection[i].pathExpr.col.planNo;
                        oid = EVAL_EVALBUFFER[projPlanNo].getOID_Ptr();
                        memcpy(pFixedCol, oid, pProjection[i].resultLength);
                        pFixedCol += pProjection[i].resultLength;
                        *resultTupleSize += pProjection[i].resultLength;
                        break;

                    case PATHEXPR_KIND_LOGICALID:
                        projPlanNo = pProjection[i].pathExpr.col.planNo;
                        logicalID = EVAL_EVALBUFFER[projPlanNo].getLogicalID();
                        memcpy(pFixedCol, &logicalID, pProjection[i].resultLength);
                        pFixedCol += pProjection[i].resultLength;
                        *resultTupleSize += pProjection[i].resultLength;
                        break;

                    case PATHEXPR_KIND_ATTR:
                        projPlanNo = pProjection[i].pathExpr.col.planNo;
                        projColNo = pProjection[i].pathExpr.col.colNo;
                        pUsedColPool = ACCESSPLAN.getUsedColPool(projPlanNo);
                        mappedColNo = ACCESSPLAN.getMappedColNo(projPlanNo, projColNo);

                        if (grpByFlag == SM_TRUE) {
                            projBufSlot = EVAL_EVALBUFFER[projPlanNo].getGrpBySlotPtr(mappedColNo);
                        }
                        else {
                            projBufSlot = EVAL_EVALBUFFER[projPlanNo].getColSlotPtr(mappedColNo);
                        }

                        switch (pUsedColPool[mappedColNo].typeId) {
                            case OOSQL_TYPE_varstring:
                                e = copyColListStructToCharBuf(pUsedColPool[mappedColNo].typeId, 
                                                                pUsedColPool[mappedColNo].length, 
																projBufSlot, pVarCol);
                                if (e < eNOERROR) {
                                    OOSQL_ERR(e);
                                }
                                else {
                                    memcpy(pVarColHdr, &e, VARCOL_HDRSIZE);
                                    pVarColHdr += VARCOL_HDRSIZE;
                                    pVarCol = pVarCol + e;
                                }

                                break;

                            /* NOTE: we project {oid, column no., text descriptor} for
                             *   the Text type attribute. (oid is that of the object to
                             *   which the attribute belong.
                             */
                            case OOSQL_TYPE_TEXT:
                                /* project scan ID */
                                scanId = EVAL_ACCESSLISTTABLE[projPlanNo].getCurrScanID();
                                memcpy(pFixedCol, &scanId, sizeof(Four));
                                pFixedCol += sizeof(Four);

                                /* project OOSQL_StorageManager::OID to which this attribute belong */
                                oid = EVAL_EVALBUFFER[projPlanNo].getOID_Ptr();
                                memcpy(pFixedCol, oid, OOSQL_TYPE_OID_SIZE);
                                pFixedCol += OOSQL_TYPE_OID_SIZE;

                                /* project column no. of this attribute 
                                 * NOTE: the type of column no. is Two 
                                 */
                                memcpy(pFixedCol, &projColNo, sizeof(Two));
                                pFixedCol += sizeof(Two);

                                /* project Text Descriptor of this attribute 
                                 * NOTE: 
                                 */
                                textDescLen = sizeof(OOSQL_StorageManager::TextDesc);
                                memcpy(pFixedCol, projBufSlot->data.ptr, textDescLen);
                                pFixedCol += textDescLen;

                                *resultTupleSize += pProjection[i].resultLength;

                                break;

                            case OOSQL_TYPE_short:
                            case OOSQL_TYPE_int:
                            case OOSQL_TYPE_long:
                            case OOSQL_TYPE_long_long:
                            case OOSQL_TYPE_float:
                            case OOSQL_TYPE_double:
                            case OOSQL_TYPE_string:
                            case OOSQL_TYPE_OID:
                            case OOSQL_TYPE_DATE:
                            case OOSQL_TYPE_TIME:
                            case OOSQL_TYPE_TIMESTAMP:
                            case OOSQL_TYPE_INTERVAL:
                              e = copyColListStructToCharBuf(pUsedColPool[mappedColNo].typeId, 
                                                              pUsedColPool[mappedColNo].length, projBufSlot, pFixedCol);
                              if (e < eNOERROR)
                                  OOSQL_ERR(e);
                              
                              pFixedCol        += pUsedColPool[mappedColNo].length;
                              *resultTupleSize += pUsedColPool[mappedColNo].length;
                              break;
                              
                            case OOSQL_TYPE_MBR:
                                OOSQL_ERR(eNOTIMPLEMENTED_MBR_OOSQL);
                            default:
                                OOSQL_ERR(eINVALID_CASE_OOSQL);
                        }

                        break;

                    case PATHEXPR_KIND_METHOD:
                        OOSQL_ERR(eNOTIMPLEMENTED_METHOD_OOSQL);

                    default:
                        OOSQL_ERR(eINVALID_CASE_OOSQL);
                }
                break;

            case PROJECTION_KIND_AGGRFUNCRESULT:
                projPlanNo = pProjection[i].aggrFuncResult.planNo;
                projColNo = pProjection[i].aggrFuncResult.aggrFuncIndex;
                projBufSlot = EVAL_EVALBUFFER[projPlanNo].getAggrFuncResSlotPtr(projColNo);
                e = copyColListStructToCharBuf(pProjection[i].resultType, pProjection[i].resultLength,
                        projBufSlot, pFixedCol);
                if (e < eNOERROR) {
                    OOSQL_ERR(e);
                }
                else {
                    pFixedCol += pProjection[i].resultLength;
                    *resultTupleSize += pProjection[i].resultLength;
                }
                break;

            case PROJECTION_KIND_FUNCEVAL:
                OOSQL_ERR(eNOTIMPLEMENTED_FUNCPROJ_OOSQL);

            case PROJECTION_KIND_FUNCRESULT:
                projPlanNo = pProjection[i].funcResult.planNo;
                projColNo = pProjection[i].funcResult.funcIndex;

                switch (pProjection[i].funcResult.functionID) {
                    case FUNCTION_TEXTIR_WEIGHT:

                        /* get function result */
                        projBufSlot = EVAL_EVALBUFFER[projPlanNo].getFnMatchSlotPtr(projColNo);

                        /* encode the function result into byte stream buffer */
                        e = copyColListStructToCharBuf(pProjection[i].resultType, 
                                    pProjection[i].resultLength, projBufSlot, pFixedCol);
                        if (e < eNOERROR) {
                            OOSQL_ERR(e);
                        }
                        else {
                            pFixedCol += pProjection[i].resultLength;
                            *resultTupleSize += pProjection[i].resultLength;
                        }

                        break;

                    default:
                        OOSQL_ERR(eNOTIMPLEMENTED_FUNCPROJ_OOSQL);
                }

                break;

            case PROJECTION_KIND_VALUE:
                break;

            case PROJECTION_KIND_OPER:
                break;
        }
    }

    return eNOERROR;
}

Four    OOSQL_Evaluator::projectValueToColListStruct(
        AP_ProjectionElement    *proj, 
        OOSQL_StorageManager::ColListStruct           *clist 
)
/*
    Function:

    Side effect:

    Return value:
*/
{
    OOSQL_ERR(eNOTIMPLEMENTED_VALUEPROJ_OOSQL);
}


Four    OOSQL_Evaluator::projectOperatorToColListStruct(
        AP_ProjectionElement    *proj, 
        OOSQL_StorageManager::ColListStruct           *clist 
)
/*
    Function:

    Side effect:

    Return value:
*/
{
    OOSQL_ERR(eNOTIMPLEMENTED_EXPRPROJ_OOSQL);
}

Four    OOSQL_Evaluator::projectOidToColListStruct(
        AP_ProjectionElement    *proj, 
        OOSQL_StorageManager::ColListStruct           *clist 
)
{
	Four	projPlanNo;
	OOSQL_StorageManager::OID*	oid;

#ifdef  OOSQL_DEBUG
	if (proj->projectionKind != PROJECTION_KIND_OID) 
	{
		OOSQL_ERR(eBADPARAMETER_OOSQL);
    }
#endif

	projPlanNo	= proj->oid_of_plan;
	oid			= EVAL_EVALBUFFER[projPlanNo].getOID_Ptr();

	clist->data.oid = *oid;
	clist->length = proj->resultLength;

	return eNOERROR;
}

Four    OOSQL_Evaluator::projectionExprToColListStruct(
        AP_ProjectionElement    *proj, 
        OOSQL_StorageManager::ColListStruct           *clist 
)
{
	OOSQL_DB_Value	res(pMemoryManager);
	Four		e;
	AP_ExprPoolElements	expr;
	ValuePoolElements	value;

#ifdef  OOSQL_DEBUG
	if (proj->projectionKind != PROJECTION_KIND_EXPR) 
	{
		OOSQL_ERR(eBADPARAMETER_OOSQL);
    }
#endif

	if(proj->expr == NULL_POOLINDEX)		
		return eNOERROR;

	expr = proj->expr.getElements(m_pool->ap_exprPool);
	if(expr[0].exprKind == EXPR_KIND_VALUE)
	{
		value = expr[0].value.getElements(m_pool->valuePool);
		if(value[0].valueKind == VALUE_KIND_TEXT)
		{
			return eNOERROR;
		}
	}

	e = evalExpression(&proj->expr, &res);
	if(e < eNOERROR) OOSQL_ERR(e);
	
	clist->nullFlag = res.nullFlag;

	if (res.nullFlag)
		return eNOERROR;

	switch(proj->resultType)
	{
	case TYPEID_SHORT:
		clist->data.s = res.data.s;
		break;
	case TYPEID_INT:
		clist->data.i = res.data.i;
		break;
	case TYPEID_LONG:
		clist->data.l = res.data.l;
		break;
	case TYPEID_LONG_LONG:
		clist->data.ll = res.data.ll;
		break;
	case TYPEID_FLOAT:
		clist->data.f = res.data.f;
		break;
	case TYPEID_DOUBLE:
		clist->data.d = res.data.d;
		break;
	case TYPEID_STRING:
	case TYPEID_VARSTRING:
		memset(clist->data.ptr, 0, clist->dataLength); 
		memcpy(clist->data.ptr, res.data.ptr, res.length);
		break;
	case TYPEID_DATE:
		clist->data.date = res.data.date;
		break;
	case TYPEID_TIME:
		clist->data.time = res.data.time;
		break;
	case TYPEID_TIMESTAMP:
		clist->data.timestamp = res.data.timestamp;
		break;
	case TYPEID_INTERVAL:
		clist->data.interval = res.data.interval;
		break;
	case TYPEID_OID:
		clist->data.oid = res.data.oid;
		break;
	case TYPEID_MBR:
		clist->data.mbr = res.data.mbr;
		break;

	case TYPEID_PAGEID:
	case TYPEID_FILEID:
	case TYPEID_INDEXID:
	case TYPEID_TEXT:
		/* update statement with match clause has not been implemented since it does not semantically correct */
		OOSQL_ERR(eNOTIMPLEMENTED_OOSQL);
	default:
		OOSQL_ERR(eNOTIMPLEMENTED_OOSQL); // unhandled case error
	}

	return eNOERROR;
}

Four OOSQL_Evaluator::checkIfProjectionCanbeFastEncoded(
	AP_ProjectionListElement*				projInfo,		// IN: projection list
    Four									nCols           // IN: the # of columns in 'clist'
)
{
    AP_ProjectionPoolElements   pProjection;
    Four						i;

	pProjection = ACCESSPLAN.getProjectionPool(projInfo->projectionInfo);

	for(i = 0; i < nCols; i++) 
	{
		if(pProjection[i].resultType == OOSQL_TYPE_TEXT)
			return SM_FALSE;
		switch (pProjection[i].projectionKind) 
		{
		case PROJECTION_KIND_PATHEXPR:
		case PROJECTION_KIND_FUNCRESULT:
		case PROJECTION_KIND_VALUE:
		case PROJECTION_KIND_OID:
			break;

		case PROJECTION_KIND_OPER:
		case PROJECTION_KIND_EXPR:
		case PROJECTION_KIND_AGGRFUNCRESULT:
		case PROJECTION_KIND_FUNCEVAL:
			return SM_FALSE;

		default:
			OOSQL_ERR(eNOTIMPLEMENTED_OOSQL); // unhandled case error
		}
	}

    return SM_TRUE;
}

Four OOSQL_Evaluator::constructFastEncodingInfo(
    AP_ProjectionListElement*				projInfo,
    Four									nCols,          // IN : the # of columns
    Boolean									grpByFlag,      // IN : flag indicating projection for group by result
	OOSQL_FastEncodingInfo*					encodingInfo	// OUT: fast encoding informations
)
/*
    Function:

    Side effect:

    Return value:
*/
{
    AP_ProjectionPoolElements				pProjection;
    Four									i;
    Four									e;
    AP_PathExprAccessInfo					pathExpr;
    Four									projPlanNo;
    Four									projColNo;
    Four									mappedColNo;
	OOSQL_StorageManager::ColListStruct*	projColList;
	static Boolean							alwaysNull    = SM_TRUE;
	static Boolean							alwaysNotNull = SM_FALSE;
	static Four								oidSize		  = sizeof(OOSQL_StorageManager::OID);
	static Four								fourSize      = sizeof(Four);

	pProjection = ACCESSPLAN.getProjectionPool(projInfo->projectionInfo);

	for(i = 0; i < nCols; i++) 
	{
		encodingInfo[i].type = pProjection[i].resultType;

		switch (pProjection[i].projectionKind) 
		{
		case PROJECTION_KIND_PATHEXPR:
			pathExpr = pProjection[i].pathExpr;
			switch (pathExpr.kind) 
			{
	        case PATHEXPR_KIND_ATTR:
	            projPlanNo  = pathExpr.col.planNo;
		        projColNo   = pathExpr.col.colNo;
			    mappedColNo = ACCESSPLAN.getMappedColNo(projPlanNo, projColNo);
				if(mappedColNo >= 0)	
				{						
					if (grpByFlag == SM_TRUE) 
					{
						projColList = EVAL_EVALBUFFER[projPlanNo].getGrpBySlotPtr(mappedColNo);
						if(projColList == NULL)
							projColList = EVAL_EVALBUFFER[projPlanNo].getColSlotPtr(mappedColNo);
					}
					else 
					{
						projColList = EVAL_EVALBUFFER[projPlanNo].getColSlotPtr(mappedColNo);
					}

					encodingInfo[i].size     = &projColList->retLength;
					encodingInfo[i].nullFlag = &projColList->nullFlag;
					if(pProjection[i].resultType == OOSQL_TYPE_STRING || pProjection[i].resultType == OOSQL_TYPE_VARSTRING)
						encodingInfo[i].ptr  = (char*)projColList->data.ptr;
					else
						encodingInfo[i].ptr  = (char*)&projColList->data.s;
				}
				else
					encodingInfo[i].nullFlag = &alwaysNull;
				break;

			case PATHEXPR_KIND_OID:
				if(ACCESSPLANELEMENTS[pathExpr.planNo].classInfo.classKind == CLASSKIND_PERSISTENT)
				{
					encodingInfo[i].size     = &oidSize;
					encodingInfo[i].ptr      = (char*)EVAL_EVALBUFFER[pathExpr.planNo].getOID_Ptr();
					encodingInfo[i].nullFlag = &alwaysNotNull;
				}
				else
				{
					OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
				}
				break;

			case PATHEXPR_KIND_OBJECT:
				OOSQL_ERR(eNOTIMPLEMENTED_OBJECTPROJ_OOSQL);
				break;

			case PATHEXPR_KIND_METHOD:
				OOSQL_ERR(eNOTIMPLEMENTED_METHOD_OOSQL);
				break;

			default:
				OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
				break;
			}
			break;

		case PROJECTION_KIND_FUNCRESULT:
			/* get projection information */
			projPlanNo  = pProjection[i].funcResult.planNo;
			projColNo   = pProjection[i].funcResult.funcIndex;
			projColList = EVAL_EVALBUFFER[projPlanNo].getFnMatchSlotPtr(projColNo);

			switch (pProjection[i].funcResult.functionID) 
			{
			case FUNCTION_TEXTIR_WEIGHT:
				/* get function result */
				encodingInfo[i].size     = &projColList->retLength;
				encodingInfo[i].ptr      = (char*)&projColList->data.i;
				encodingInfo[i].nullFlag = &alwaysNotNull;

				break;

			default:
				OOSQL_ERR(eNOTIMPLEMENTED_FUNCPROJ_OOSQL);
			}
			break;

		case PROJECTION_KIND_VALUE:
			OOSQL_ERR(eNOTIMPLEMENTED_VALUEPROJ_OOSQL);

			break;

		case PROJECTION_KIND_OID:
			projPlanNo	= pProjection[i].oid_of_plan;

			encodingInfo[i].size     = &oidSize;
			encodingInfo[i].ptr      = (char*)EVAL_EVALBUFFER[pathExpr.planNo].getOID_Ptr();
			encodingInfo[i].nullFlag = &alwaysNotNull;
			break;

		default:
			OOSQL_ERR(eINTERNALERROR_OOSQL);
		}
	}

    return eNOERROR;
}

#ifndef _SOLARIS_2x_

#include "OOSQL_SortStream.hxx"

Four OOSQL_Evaluator::projectPathExprToColListStruct(
        AP_ProjectionElement    *proj,
        OOSQL_StorageManager::ColListStruct           *clist,         // IN/OUT
        Boolean                 grpByFlag
)
/*
    Function:

    Side effect:

    Return value:
*/
{
    AP_PathExprAccessInfo   pathExpr;
    Four                    projPlanNo;
    Four                    projColNo;
    OOSQL_StorageManager::ColListStruct           *projColList;
    Four                    mappedColNo;
    Four                    e;

#ifdef  OOSQL_DEBUG
    if (proj->projectionKind != PROJECTION_KIND_PATHEXPR) {
        OOSQL_ERR(eBADPARAMETER_OOSQL);
    }
#endif

    pathExpr = proj->pathExpr;

    switch (pathExpr.kind) {
        case PATHEXPR_KIND_ATTR:
            projPlanNo = pathExpr.col.planNo;
            projColNo = pathExpr.col.colNo;
            mappedColNo = ACCESSPLAN.getMappedColNo(projPlanNo, projColNo);
			if(mappedColNo >= 0)	
			{						
                if (grpByFlag == SM_TRUE) {
                    projColList = EVAL_EVALBUFFER[projPlanNo].getGrpBySlotPtr(mappedColNo);
                    if(projColList == NULL)
                        projColList = EVAL_EVALBUFFER[projPlanNo].getColSlotPtr(mappedColNo);
                }
                else {
                    projColList = EVAL_EVALBUFFER[projPlanNo].getColSlotPtr(mappedColNo);
                }
                e = copyColListStruct(proj->resultType, projColList, clist);
                if (e < eNOERROR) OOSQL_ERR(e);
            }
            else
            {
                clist->nullFlag = SM_TRUE;
            }
            break;

        case PATHEXPR_KIND_OID:
            clist->retLength  = OOSQL_TYPE_OID_SIZE;
            clist->dataLength = OOSQL_TYPE_OID_SIZE;
            clist->length     = OOSQL_TYPE_OID_SIZE;

            if(ACCESSPLANELEMENTS[pathExpr.planNo].classInfo.classKind == CLASSKIND_PERSISTENT)
            {
                memcpy(&clist->data.oid, EVAL_EVALBUFFER[pathExpr.planNo].getOID_Ptr(), sizeof(OOSQL_StorageManager::OID));
            }
            else
            {
                OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
            }
            break;

        case PATHEXPR_KIND_OBJECT:
            OOSQL_ERR(eNOTIMPLEMENTED_OBJECTPROJ_OOSQL);
            break;

        case PATHEXPR_KIND_METHOD:
            OOSQL_ERR(eNOTIMPLEMENTED_METHOD_OOSQL);
            break;

        default:
            OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
            break;
    }

    return eNOERROR;
}

Four OOSQL_Evaluator::projectAggrFuncToColListStruct(
        AP_ProjectionElement                        *proj,
        OOSQL_StorageManager::ColListStruct         *clist
)
/*
    Function:

    Side effect:

    Return value:
*/
{
    Four            planNo;
    Four            aggrFuncIdx;
    OOSQL_StorageManager::ColListStruct   *aggrFuncColList;
    Four            e;

    planNo = proj->aggrFuncResult.planNo;
    aggrFuncIdx = proj->aggrFuncResult.aggrFuncIndex;

    aggrFuncColList = EVAL_EVALBUFFER[planNo].getAggrFuncResSlotPtr(aggrFuncIdx);

    e = copyColListStruct(proj->resultType, aggrFuncColList, clist);
    if (e < eNOERROR) OOSQL_ERR(e);

    return eNOERROR;
}


Four OOSQL_Evaluator::projectFunctionToColListStruct(
        AP_ProjectionElement                        *proj,
        OOSQL_StorageManager::ColListStruct         *clist
)
/*
    Function:

    Side effect:

    Return value:
*/
{
    OOSQL_ERR(eNOTIMPLEMENTED_FUNCPROJ_OOSQL);
}


Four OOSQL_Evaluator::projectFuncResultToColListStruct(
        AP_ProjectionElement                        *proj,          // IN:
        OOSQL_StorageManager::ColListStruct         *clist          // IN:
)
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
    Four    projPlanNo;
    Four    projColNo;
    EVAL_EvalBufferSlot     *funcResult;
    Four    e;

    /* check input parameters */
#ifdef  OOSQL_DEBUG
    if (proj == NULL)
            OOSQL_ERR(eBADPARAMETER_OOSQL);
    if (clist == NULL)
            OOSQL_ERR(eBADPARAMETER_OOSQL);
#endif

    /* get projection information */
    projPlanNo = proj->funcResult.planNo;
    projColNo = proj->funcResult.funcIndex;

    switch (proj->funcResult.functionID) {
        case FUNCTION_TEXTIR_WEIGHT:
            /* get function result */
            funcResult = EVAL_EVALBUFFER[projPlanNo].getFnMatchSlotPtr(projColNo);

            break;

        default:
            OOSQL_ERR(eNOTIMPLEMENTED_FUNCPROJ_OOSQL);
    }

    /* copy the function result to column list structure */
    e = copyColListStruct(proj->resultType, funcResult, clist);
    if (e < eNOERROR) OOSQL_ERR(e);

    /* return */
    return eNOERROR;
}


Four OOSQL_Evaluator::projectToColListStruct(
    AP_ProjectionListElement*               projInfo,
    Four                                    nCols,          // IN: the # of columns in 'clist'
    OOSQL_StorageManager::ColListStruct*    clist,          // IN/OUT: column list struct
    Boolean                                 grpByFlag,      // IN: flag indicating projection for group by result
    Boolean                                 compactCopyFlag,// IN: flag indicating skip TEXT, COMPLEXTYPE columns
    Four&                                   nColsCopied     // OUT: # of columns copied to clist
)
/*
    Function:

    Side effect:

    Return value:
*/
{
    AP_ProjectionPoolElements   pProjection;
    Four                        i, index;
    Four                        e;

#ifdef  OOSQL_DEBUG
    if(nCols > GET_POOLSIZE(projInfo->projectionInfo))
    {
        OOSQL_ERR(eBADPARAMETER_OOSQL);
    }
#endif

    pProjection = ACCESSPLAN.getProjectionPool(projInfo->projectionInfo);

    for(i = 0, index = 0; i < nCols; i++)
    {
        if(compactCopyFlag)
        {
            if(pProjection[i].resultType == TYPEID_TEXT ||
               OOSQL_MASK_COMPLEXTYPE(pProjection[i].resultType) == COMPLEXTYPEID_SET ||
               OOSQL_MASK_COMPLEXTYPE(pProjection[i].resultType) == COMPLEXTYPEID_BAG ||
               OOSQL_MASK_COMPLEXTYPE(pProjection[i].resultType) == COMPLEXTYPEID_LIST)
            {
                continue;
            }
        }

        switch (pProjection[i].projectionKind)
        {
        case PROJECTION_KIND_PATHEXPR:
            e = projectPathExprToColListStruct(&pProjection[i], &clist[index], grpByFlag);
            if (e < eNOERROR) OOSQL_ERR(e);
            index ++;
            break;

        case PROJECTION_KIND_AGGRFUNCRESULT:
            e = projectAggrFuncToColListStruct(&pProjection[i], &clist[index]);
            if (e < eNOERROR) OOSQL_ERR(e);
            index ++;
            break;

        case PROJECTION_KIND_FUNCEVAL:
            e = projectFunctionToColListStruct(&pProjection[i], &clist[index]);
            if (e < eNOERROR) OOSQL_ERR(e);
            index ++;
            break;

        case PROJECTION_KIND_FUNCRESULT:
            e = projectFuncResultToColListStruct(&pProjection[i], &clist[index]);
            if (e < eNOERROR) OOSQL_ERR(e);
            index ++;
            break;

        case PROJECTION_KIND_VALUE:
            e = projectValueToColListStruct(&pProjection[i], &clist[index]);
            if (e < eNOERROR) OOSQL_ERR(e);
            index ++;
            break;

        case PROJECTION_KIND_OPER:
            e = projectOperatorToColListStruct(&pProjection[i], &clist[index]);
            if (e < eNOERROR) OOSQL_ERR(e);
            index ++;
            break;

        case PROJECTION_KIND_OID:
            e = projectOidToColListStruct(&pProjection[i], &clist[index]);
            if (e < eNOERROR) OOSQL_ERR(e);
            index ++;
            break;

        case PROJECTION_KIND_EXPR:
            e = projectionExprToColListStruct(&pProjection[i], &clist[index]);
            if (e < eNOERROR) OOSQL_ERR(e);
            index ++;
            break;

        default:
            OOSQL_ERR(eNOTIMPLEMENTED_OOSQL); // unhandled case error
        }
    }

    nColsCopied = index;
	
	return eNOERROR;
}

Four OOSQL_Evaluator::processProjectionToTempFile(
    AP_ProjectionListElement*   projInfo,
    Boolean                     grpByFlag       // IN: flag indicating projection for group by result
)
/*
    Function:
        Project query result into the result buffer pointed to by 'resultBuf'.

    Side effect:

    Return value:
        eINSUFFICIENT_BUFSIZE_OOSQL     if buffer space for the query result is not adequate
        eNOERROR                        if no error
*/
{
    OOSQL_TempFileInfo*                 pTempFileInfo;      // information about temporary file
    OOSQL_StorageManager::OID           tempFileRecordOid;  // oid of a created object
    Four                                e;                  // error code
    char                                buffer[OOSQL_EVALBUFFER_MAXSTRINGSIZE];
    OOSQL_StorageManager::ColListStruct clist[MAXNUMOFATTRIBUTE];
    OOSQL_FastEncodingInfo              encodingInfo[MAXNUMOFATTRIBUTE];
    Four                                i;
    Four                                nColsCopied;

#ifdef  OOSQL_DEBUG
    if (projInfo->tempFileNum < 0 || EVAL_TEMPFILEINFOTABLE.nTempFiles <= projInfo->tempFileNum)
        OOSQL_ERR(eINVALID_TEMPFILENUM_OOSQL);
#endif

    // set pointer to temp. file information
    pTempFileInfo = EVAL_TEMPFILEINFOTABLEELEMENTS[projInfo->tempFileNum];
    if(pTempFileInfo->sortStream)
    {
        if(pTempFileInfo->useFastEncoding == SM_TRUE)
        {
            e = pTempFileInfo->sortStream->FastEncodedCreateObject();
            OOSQL_CHECK_ERR(e);
        }
        else
        {
            if(pTempFileInfo->isTextAttrExist == SM_FALSE && pTempFileInfo->useFastEncoding == SM_FALSE)
            {
                e = projectToColListStruct(projInfo, pTempFileInfo->nCols, &(pTempFileInfo->clist[0]), grpByFlag, SM_FALSE, nColsCopied);
                OOSQL_CHECK_ERR(e);

                e = pTempFileInfo->sortStream->FastCreateObject(pTempFileInfo->nCols, pTempFileInfo->clist);
                OOSQL_CHECK_ERR(e);
            }
            else
            {
                if(pTempFileInfo->useFastEncoding == NIL)
                {
                    e = checkIfProjectionCanbeFastEncoded(projInfo, pTempFileInfo->nCols);
                    OOSQL_CHECK_ERR(e);

                    if(e == SM_TRUE)
                    {
                        e = constructFastEncodingInfo(projInfo, pTempFileInfo->nCols, grpByFlag, encodingInfo);
                        OOSQL_CHECK_ERR(e);

                        e = pTempFileInfo->sortStream->SetFastEncodingInfo(pTempFileInfo->nCols, encodingInfo);
                        OOSQL_CHECK_ERR(e);

                        e = pTempFileInfo->sortStream->FastEncodedCreateObject();
                        OOSQL_CHECK_ERR(e);

                        pTempFileInfo->useFastEncoding = SM_TRUE;

                        return eNOERROR;
                    }
                    else
                        pTempFileInfo->useFastEncoding = SM_FALSE;
                }

                e = projectToColListStruct(projInfo, pTempFileInfo->nCols, &(pTempFileInfo->clist[0]), grpByFlag, SM_FALSE, nColsCopied);
                OOSQL_CHECK_ERR(e);

                if(pTempFileInfo->isTextAttrExist == NIL)
                    pTempFileInfo->isTextAttrExist = SM_FALSE;
                for(i = 0; i < pTempFileInfo->nCols; i++)
                {
                    Four                                projPlanNo;         /* projection plan no. */
                    Two                                 projColNo;          /* projection column no.  */
                    AP_UsedColPoolElements              pUsedColPool;       /* ptr. to used column m_pool for the current A.P. elem. */
                    Four                                mappedColNo;        /* mapped column no. for projection col. no. */
                    AP_ProjectionPoolElements           pProjection;
                    OOSQL_StorageManager::TextColStruct text;
                    OOSQL_StorageManager::TextDesc      textDesc;
                    Four                                ocn;
                    OOSQL_StorageManager::OID*          oid;
                    Four                                startPos;

                    pProjection  = ACCESSPLAN.getProjectionPool(projInfo->projectionInfo);
                    if(pProjection[i].resultType == OOSQL_TYPE_TEXT)
                    {
                        projPlanNo   = pProjection[i].pathExpr.col.planNo;
                        projColNo    = pProjection[i].pathExpr.col.colNo;
                        pUsedColPool = ACCESSPLAN.getUsedColPool(projPlanNo);
                        mappedColNo  = ACCESSPLAN.getMappedColNo(projPlanNo, projColNo);
                        ocn          = EVAL_ACCESSLISTTABLE[projPlanNo].getCurrOcn();
                        oid          = EVAL_EVALBUFFER[projPlanNo].getOID_Ptr();
                        startPos     = 0;

                        e = m_storageManager->Text_GetDescriptor(ocn, SM_FALSE, oid, projColNo, &textDesc);
                        OOSQL_CHECK_ERR(e);

                        text.start      = startPos;
                        text.length     = pTempFileInfo->clist[i].dataLength;
                        text.dataLength = pTempFileInfo->clist[i].dataLength;
                        text.data       = pTempFileInfo->clist[i].data.ptr;

                        e = m_storageManager->Text_FetchContent(ocn, SM_FALSE, oid, projColNo, &text, &textDesc);
                        OOSQL_CHECK_ERR(e);

                        pTempFileInfo->clist[i].retLength = text.retLength;
                        if(text.retLength >= 0)
                            pTempFileInfo->clist[i].nullFlag = SM_FALSE;
                        else
                            pTempFileInfo->clist[i].nullFlag = SM_TRUE;


                        pTempFileInfo->isTextAttrExist = SM_TRUE;
                    }
                }
                for(i = 0; i < pTempFileInfo->nCols; i++)
                {
                    clist[i].colNo      = pTempFileInfo->clist[i].colNo;
                    clist[i].data       = pTempFileInfo->clist[i].data;
                    clist[i].nullFlag   = pTempFileInfo->clist[i].nullFlag;
                    clist[i].dataLength = pTempFileInfo->clist[i].retLength;
                    clist[i].length     = ALL_VALUE;
                    clist[i].start      = ALL_VALUE;
                }

                e = pTempFileInfo->sortStream->FastCreateObject(pTempFileInfo->nCols, clist);
                OOSQL_CHECK_ERR(e);
            }
        }
    }
    else
    {
        e = projectToColListStruct(projInfo, pTempFileInfo->nCols, &(pTempFileInfo->clist[0]), grpByFlag, SM_FALSE, nColsCopied);
        OOSQL_CHECK_ERR(e);

        e = m_storageManager->CreateObjectByColList(pTempFileInfo->ocn, SM_FALSE, pTempFileInfo->nCols, &(pTempFileInfo->clist[0]), &tempFileRecordOid);
        OOSQL_CHECK_ERR(e);

        for(int i = 0; i < pTempFileInfo->nCols; i++)
        {
            Four                                projPlanNo;         /* projection plan no. */
            Two                                 projColNo;          /* projection column no.  */
            AP_UsedColPoolElements              pUsedColPool;       /* ptr. to used column m_pool for the current A.P. elem. */
            Four                                mappedColNo;        /* mapped column no. for projection col. no. */
            AP_ProjectionPoolElements           pProjection;
            OOSQL_StorageManager::TextColStruct text;
            OOSQL_StorageManager::TextDesc      textDesc;
            Four                                ocn;
            OOSQL_StorageManager::OID*          oid;
            Four                                startPos;

            pProjection  = ACCESSPLAN.getProjectionPool(projInfo->projectionInfo);

            if(pProjection[i].resultType == OOSQL_TYPE_TEXT)
            {
				// if not pathexpr, prevent executing following lines
				if (pProjection[i].projectionKind != PROJECTION_KIND_PATHEXPR)
					return eNOERROR;
                projPlanNo   = pProjection[i].pathExpr.col.planNo;
                projColNo    = pProjection[i].pathExpr.col.colNo;
                pUsedColPool = ACCESSPLAN.getUsedColPool(projPlanNo);
                mappedColNo  = ACCESSPLAN.getMappedColNo(projPlanNo, projColNo);
                ocn          = EVAL_ACCESSLISTTABLE[projPlanNo].getCurrOcn();
                oid          = EVAL_EVALBUFFER[projPlanNo].getOID_Ptr();
                startPos     = 0;

                e = m_storageManager->Text_GetDescriptor(ocn, SM_FALSE, oid, projColNo, &textDesc);
                if (e < eNOERROR) OOSQL_ERR(e);

                clist[0].colNo      = pTempFileInfo->clist[i].colNo;
                clist[0].data.ptr   = buffer;
                clist[0].nullFlag   = SM_FALSE;

                do
                {
                    text.start      = startPos;
                    text.length     = sizeof(buffer);
                    text.dataLength = sizeof(buffer);
                    text.data       = buffer;
                    e = m_storageManager->Text_FetchContent(ocn, SM_FALSE, oid, projColNo, &text, &textDesc);
                    if (e < eNOERROR) OOSQL_ERR(e);

                    if(text.retLength > 0)
                    {
                        clist[0].start      = startPos;
                        clist[0].length     = ALL_VALUE;
                        clist[0].dataLength = text.retLength;
                        e = m_storageManager->UpdateObjectByColList(pTempFileInfo->ocn, SM_FALSE, &tempFileRecordOid, 1, clist);
                        OOSQL_CHECK_ERR(e);
                    }

                    startPos += sizeof(buffer);
                } while(text.retLength == sizeof(buffer));
            }
        }
    }

    return eNOERROR;
}
#endif
