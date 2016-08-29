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
        oosql_Eval_Process.cxx

    DESCRIPTION:

    IMPORTS:

    EXPORTS:
        Four    OOSQL_Evaluator::process()
*/

#include "OOSQL_Evaluator.hxx"
#include "oosql_Eval_Control.hxx"
#include "OOSQL_MultipleResultBuffer.hxx"
#include "oosql_Eval_Expr.hxx"
#include "OOSQL_Eval_DS.hxx"
#include "OOSQL_Eval_Util.hxx"
#include "OOSQL_String.hxx"
#include "OOSQL_ExternalFunctionManager.hxx"
#include <string.h>
#include <time.h>

Four OOSQL_Evaluator::processForSelectOnly()
{
    Four e;


	/* check if aggregation is necessary */
    if(!IS_NULL_POOLINDEX(ACCESSPLANELEMENTS[m_currPlanIndex].aggrFuncInfo)) { 
		/*
         * After aggregation() is called, m_evalStatus can be changed as following.
         *  - aggregationStatus:
         *      EVALSTATUS_INIT -> EVALSTATUS_PROCESSING (if initial processing)
         */
        e = aggregation();
        if(e < eNOERROR)
            OOSQL_ERR(e);
		if(isLastPlanElement()) 
		{
            if(m_evalStatus.aggregationStatus == EVALSTATUS_END) 
			{
				if(m_multipleResultBuffer == NULL)
					m_numQueryResultTuples ++;
				else
				{
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
						m_evalStatus.wasGroupByResult			= SM_FALSE;

						/* change evaluation status */
						m_evalStatus.queryResultBufferFull		= SM_TRUE;

						return eNOERROR;
					}
					else
						OOSQL_CHECK_ERR(e);
				}

				return eNOERROR;
            }
        }
    }
	else if(!IS_NULL_POOLINDEX(ACCESSPLANELEMENTS[m_currPlanIndex].projectionList)) 
	{
		if(isLastPlanElement())
		{
			if(m_multipleResultBuffer == NULL)
				m_numQueryResultTuples ++;
			else
			{
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
					m_evalStatus.wasGroupByResult			= SM_FALSE;

					/* change evaluation status */
					m_evalStatus.queryResultBufferFull		= SM_TRUE;
				}
				else
					OOSQL_CHECK_ERR(e);
			}
		}
		else
		{
			e = projection(SM_FALSE);
			if(e < eNOERROR) OOSQL_ERR(e);
		}
	}

    return(eNOERROR);
}

Four    OOSQL_Evaluator::process()
/*
    Function:

    Side effect:

    Referenced member variables:
        m_currPlanIndex

    Return value:
*/
{
    Four havingCond;
    Four e;


    /* check if grouping is necessary */
    if(ACCESSPLANELEMENTS[m_currPlanIndex].nGrpByKeys > 0 &&
        m_evalStatus.groupingStatus != EVALSTATUS_END) {

        /* [Internal Documentation]
         * After grouping() is called, m_evalStatus can be changed as following.
         *  - groupingStatus: 
         *      EVALSTATUS_INIT -> EVALSTATUS_PROCESSING (if initial processing)
         *      EVALSTATUS_PROCESSING -> EVALSTATUS_END (if a group is made)
         *  - aggregationStatus:
         *      EVALSTATUS_PROCESSING -> EVALSTATUS_END (if a group is made)
         */
        e = grouping();
        if(e < eNOERROR)
            OOSQL_ERR(e);
    }

    /* check if aggregation is necessary */
    if(!IS_NULL_POOLINDEX(ACCESSPLANELEMENTS[m_currPlanIndex].aggrFuncInfo)) { 
        /* [Internal Documentation]
         * After aggregation() is called, m_evalStatus can be changed as following.
         *  - aggregationStatus:
         *      EVALSTATUS_INIT -> EVALSTATUS_PROCESSING (if initial processing)
         */
        e = aggregation();
        if(e < eNOERROR)
            OOSQL_ERR(e);
    }

    /* check if having condition checking is possible */
    if(ACCESSPLANELEMENTS[m_currPlanIndex].nGrpByKeys > 0) {
        /* if grouping is ended */
        if(m_evalStatus.groupingStatus == EVALSTATUS_END) {
            if(IS_NULL_POOLINDEX(ACCESSPLANELEMENTS[m_currPlanIndex].havCondNodes)) {
                havingCond = SM_TRUE;
            }
            else {
                havingCond = checkHavingCond();
                if(havingCond < eNOERROR)
                    OOSQL_ERR(havingCond);
            }

            if(havingCond) {
                /* set the parameter(isGroupByResult) of projection() as true 
                 * After projection() is called, m_evalStatus.queryResultBufferFull can be set to SM_TRUE.
                 */
                e = projection(SM_TRUE);
                if(e < eNOERROR)
                    OOSQL_ERR(e);
            }
            
        } /* end of if */
    } /* end of if */
    /* check if aggregation processing ended */
    else if(!IS_NULL_POOLINDEX(ACCESSPLANELEMENTS[m_currPlanIndex].aggrFuncInfo)) {
        if(isLastPlanElement()) {
            if(m_evalStatus.aggregationStatus == EVALSTATUS_END) {
                /* set the parameter(isGroupByResult) of projection() as SM_FALSE
                 * After projection() is called, m_evalStatus.queryResultBufferFull can be set to SM_TRUE.
                 */
                e = projection(SM_FALSE);
                if(e < eNOERROR)
                    OOSQL_ERR(e);
            }
        }
        else {  /* the current access plan element is not the last one */
            if(!IS_NULL_POOLINDEX(ACCESSPLANELEMENTS[m_currPlanIndex].projectionList)) {
                /* set the parameter(isGroupByResult) of projection() as SM_FALSE
                 * in this case, do projection to temporary relation to process 
                 * aggregate function with distinct by sorting.
                 */
                e = projection(SM_FALSE);
                if(e < eNOERROR)
                    OOSQL_ERR(e);
            }
        }
    }
    /* check if projection is necessary when neither grouping nor aggregation are necessary */
    else if(!IS_NULL_POOLINDEX(ACCESSPLANELEMENTS[m_currPlanIndex].projectionList)) {
        /* set the parameter(isGroupByResult) of projection() as SM_FALSE
         * After projection() is called, m_evalStatus.queryResultBufferFull can be set to SM_TRUE.
         */
		AP_ProjectionListPoolElements	projList;

		projList = ACCESSPLAN.getProjectionListPool(m_currPlanIndex);
		if(isLastPlanElement() && projList[0].projectionType == PROJECTION_NONE && !ACCESSPLANELEMENTS[m_currPlanIndex].selDistinctFlag)
		{
			if(m_multipleResultBuffer == NULL)
				m_numQueryResultTuples ++;
			else
			{
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
					m_evalStatus.wasGroupByResult			= SM_FALSE;

					/* change evaluation status */
					m_evalStatus.queryResultBufferFull		= SM_TRUE;
				}
				else
					OOSQL_CHECK_ERR(e);
			}
		}
		else
		{
			e = projection(SM_FALSE);
			if(e < eNOERROR) OOSQL_ERR(e);
		}
    }

    /* check if sorting is necessary */
    if(m_evalStatus.prepareAndSortStatus == EVALSTATUS_END) {
        e = sorting();
        if(e < eNOERROR)
            OOSQL_ERR(e);
    }

    return(eNOERROR);
}

Four OOSQL_Evaluator::processDBCommand()
{
	Four							i, j;
	Four							e;
	DBCommandPoolElements			dbCommand(m_pool->dbCommandPool);
	KeyInfoPoolElements				keyList(m_pool->keyInfoPool);
	AttributeInfoPoolElements		attributeList(m_pool->attributeInfoPool);
	SuperClassPoolElements			superClassList(m_pool->superClassPool);
	char							className[MAXCLASSNAME];
	char							indexName[MAXINDEXNAME], *pIndexName;
	char							attrName[MAXATTRNAME];
	char							superClassNames[MAXSUPERCLASSID][MAXCLASSNAME]; 
	void*							pSuperClassNames;
	OOSQL_StorageManager::IndexDesc	indexDesc, *pIndexDesc;
	OOSQL_StorageManager::IndexID	indexId;
	OOSQL_StorageManager::AttrInfo	attrInfo[MAXNUMOFATTRIBUTE];
	OOSQL_StorageManager::AttrInfo	addColInfo[MAXNUMOFATTRIBUTE];
	OOSQL_StorageManager::AttrInfo	dropColInfo[MAXNUMOFATTRIBUTE];
	Four							nAddCol;
	Four							nDropCol;
	char							sequenceName[MAXCLASSNAME];
	Four							classId;
	Four							nSuperClasses;
	Four							nAttrs;
	Boolean							tempClassFlag;
	OOSQL_StorageManager::PostingStructureInfo postingStructureInfo;
	OOSQL_TCDynStr					queryString(pMemoryManager);
	char                            temp[1024];
	ArgumentTypePoolElements		argType(m_pool->argumentTypePool);
	time_t							clk;
	const struct tm*				now;
	Four							funcId, procId;
	OOSQL_StorageManager::CounterID counterId;
	OOSQL_Handle                    handle;

		
	dbCommand.setPoolIndex(0, m_pool->dbCommandPool.nElements());
	for(i = 0; i < dbCommand.size; i++)
	{
		switch(dbCommand[i].command)
		{
		case DBCOMMAND_ALTER_TABLE:
			e = makeStringFromStringPool(className, dbCommand[i].createTableInfo.className);
			if(e < eNOERROR) OOSQL_ERR(e);

			// process add column command
			attributeList = dbCommand[i].alterTableInfo.addColList;
			nAddCol = attributeList.size;
			for(j = 0; j < attributeList.size; j++)
			{
				e = makeStringFromStringPool(attrName, attributeList[j].attributeName);
				if(e < eNOERROR) OOSQL_ERR(e);

				addColInfo[j].complexType		= (Two)attributeList[j].complexType;
				addColInfo[j].type				= (Two)attributeList[j].domain;
				addColInfo[j].length			= attributeList[j].length;
				strcpy(addColInfo[j].name, attrName);
				addColInfo[j].inheritedFrom		= attributeList[j].inheritedFrom;
				addColInfo[j].domain			= attributeList[j].domain;			
			}

			// process drop column command
			attributeList = dbCommand[i].alterTableInfo.dropColList;
			nDropCol = attributeList.size;
			for(j = 0; j < attributeList.size; j++)
			{
				e = makeStringFromStringPool(attrName, attributeList[j].attributeName);
				if(e < eNOERROR) OOSQL_ERR(e);

				strcpy(dropColInfo[j].name, attrName);
			}

			e = m_storageManager->AlterClass(m_volID, className, nAddCol, addColInfo, nDropCol, dropColInfo);
			if(e < eNOERROR) OOSQL_ERR(e);
			break;

		case DBCOMMAND_CREATE_SEQUENCE:
			e = makeStringFromStringPool(sequenceName, dbCommand[i].createSequenceInfo.sequenceName);
			if(e < eNOERROR) OOSQL_ERR(e);

			e = m_storageManager->CreateSequence(m_volID, sequenceName, dbCommand[i].createSequenceInfo.startWith);
			if(e < eNOERROR) OOSQL_ERR(e);
			break;			

		case DBCOMMAND_DROP_SEQUENCE:
			e = makeStringFromStringPool(sequenceName, dbCommand[i].dropSequenceInfo.sequenceName);
			if(e < eNOERROR) OOSQL_ERR(e);

			e = m_storageManager->DropSequence(m_volID, sequenceName);
			if(e < eNOERROR) OOSQL_ERR(e);
			break;			

		case DBCOMMAND_CREATE_TABLE:
			e = makeStringFromStringPool(className, dbCommand[i].createTableInfo.className);
			if(e < eNOERROR) OOSQL_ERR(e);

			superClassList = dbCommand[i].createTableInfo.superClasses;
			nSuperClasses  = superClassList.size;

			if(nSuperClasses > MAXSUPERCLASSID)
				OOSQL_ERR(eNOTIMPLEMENTED_OOSQL);	
			
			if(nSuperClasses > 0)
			{
				for(j = 0; j < nSuperClasses; j++)
				{
					e = makeStringFromStringPool(superClassNames[j], superClassList[j].className);
					if(e < eNOERROR) OOSQL_ERR(e);
				}
				pSuperClassNames = superClassNames;
			}
			else
				pSuperClassNames = NULL;

			keyList = dbCommand[i].createTableInfo.keyList;

			if(keyList.size > 0)
			{
				strcpy(indexName, className);
				strcat(indexName, "_IDX");
			
				indexDesc.indexType  = SM_INDEXTYPE_BTREE;
				indexDesc.kinfo.btree.flag = KEYFLAG_UNIQUE | KEYFLAG_CLUSTERING;
				indexDesc.kinfo.btree.nColumns = (Two)dbCommand[i].createTableInfo.keyList.size;
			
				for(j = 0; j < keyList.size; j++) 
				{
					indexDesc.kinfo.btree.columns[j].colNo = keyList[j].keyColNo;
					indexDesc.kinfo.btree.columns[j].flag  = SORTKEYDESC_ATTR_ASC; // SORTKEYDESC_ATTR_DESC
				}
				pIndexName = indexName;
				pIndexDesc = &indexDesc;
			}
			else
			{
				pIndexName = NULL;
				pIndexDesc = NULL;
			}
				
			attributeList = dbCommand[i].createTableInfo.attributeList;
			nAttrs = attributeList.size;
			for(j = 0; j < attributeList.size; j++)
			{
				e = makeStringFromStringPool(attrName, attributeList[j].attributeName);
				if(e < eNOERROR) OOSQL_ERR(e);

				attrInfo[j].complexType		= (Two)attributeList[j].complexType;
				attrInfo[j].type			= (Two)attributeList[j].domain;
				attrInfo[j].length			= attributeList[j].length;
				strcpy(attrInfo[j].name, attrName);
				attrInfo[j].inheritedFrom	= attributeList[j].inheritedFrom;
				if(attrInfo[j].type == TYPEID_OID)
					attrInfo[j].domain		= attributeList[j].referencingClassId;
				else
					attrInfo[j].domain		= attributeList[j].domain;
			}
			
			tempClassFlag = dbCommand[i].createTableInfo.isTemporary;

			e = m_storageManager->CreateClass(m_volID, className, pIndexName, pIndexDesc, nAttrs, attrInfo, 
								              nSuperClasses, (char (*)[MAXCLASSNAME])pSuperClassNames , 
								              0, NULL, tempClassFlag, &classId);
			if(e < eNOERROR) OOSQL_ERR(e);
			break;

			

		case DBCOMMAND_CREATE_INDEX:
			e = makeStringFromStringPool(className, dbCommand[i].createIndexInfo.className);
			if(e < eNOERROR) OOSQL_ERR(e);

			e = makeStringFromStringPool(indexName, dbCommand[i].createIndexInfo.indexName);
			if(e < eNOERROR) OOSQL_ERR(e);

			switch(dbCommand[i].createIndexInfo.indexType)
			{
			case INDEXTYPE_BTREE:
				indexDesc.indexType = SM_INDEXTYPE_BTREE;
				indexDesc.kinfo.btree.flag = 0;
				if(dbCommand[i].createIndexInfo.isUnique)
					indexDesc.kinfo.btree.flag |= KEYFLAG_UNIQUE;
				if(dbCommand[i].createIndexInfo.isClustering)
					indexDesc.kinfo.btree.flag |= KEYFLAG_CLUSTERING;
				indexDesc.kinfo.btree.nColumns = (Two)dbCommand[i].createIndexInfo.keyList.size;
				keyList = dbCommand[i].createIndexInfo.keyList;

				for(j = 0; j < keyList.size; j++) 
				{
					indexDesc.kinfo.btree.columns[j].colNo = keyList[j].keyColNo;
					indexDesc.kinfo.btree.columns[j].flag  = SORTKEYDESC_ATTR_ASC; // SORTKEYDESC_ATTR_DESC
				}

				e = m_storageManager->AddIndex(m_volID, className, indexName, &indexDesc, &indexId);
				if(e == eINDEXDUPLICATED_OOSQL)
				{
					m_errorMessage->Clear();
					m_errorMessage->Append("The index '");
					m_errorMessage->Append(indexName);
					m_errorMessage->Append("' is already defined in the database.");

					OOSQL_ERR(e);
				}
				else if(e < eNOERROR) OOSQL_ERR(e);
				break;

			case INDEXTYPE_MLGF:
				indexDesc.indexType = SM_INDEXTYPE_MLGF;
				indexDesc.kinfo.mlgf.flag = 0;
				if(dbCommand[i].createIndexInfo.isUnique)
					indexDesc.kinfo.mlgf.flag |= KEYFLAG_UNIQUE;
				if(dbCommand[i].createIndexInfo.isClustering)
					indexDesc.kinfo.mlgf.flag |= KEYFLAG_CLUSTERING;
				indexDesc.kinfo.mlgf.nColumns = (Two)dbCommand[i].createIndexInfo.keyList.size;
				indexDesc.kinfo.mlgf.extraDataLen = 0;
				keyList = dbCommand[i].createIndexInfo.keyList;

				for(j = 0; j < keyList.size; j++) 
					indexDesc.kinfo.mlgf.colNo[j] = (Two)keyList[j].keyColNo;

				e = m_storageManager->AddIndex(m_volID, className, indexName, &indexDesc, &indexId);
				if(e == eINDEXDUPLICATED_OOSQL)
				{
					m_errorMessage->Clear();
					m_errorMessage->Append("The index '");
					m_errorMessage->Append(indexName);
					m_errorMessage->Append("' is already defined in the database.");

					OOSQL_ERR(e);
				}
				else if(e < eNOERROR) OOSQL_ERR(e);
				break;

			case INDEXTYPE_TEXT:
				OOSQL_ERR(eNOTIMPLEMENTED_OOSQL); // unhandled case error
				break;

			default:
				OOSQL_ERR(eNOTIMPLEMENTED_OOSQL); // unhandled case error
			}

			break;
		case DBCOMMAND_DROP_TABLE:
			e = makeStringFromStringPool(className, dbCommand[i].dropTableInfo.className);
			if(e < eNOERROR) OOSQL_ERR(e);

			e = m_storageManager->DestroyClass(m_volID, className);
			if(e < eNOERROR) OOSQL_ERR(e);

			break;
		case DBCOMMAND_DROP_INDEX:
			e = makeStringFromStringPool(indexName, dbCommand[i].dropIndexInfo.indexName);
			if(e < eNOERROR) OOSQL_ERR(e);

			e = m_storageManager->DropIndex(m_volID, indexName);
			if(e == eINDEXNOTDEFINED_OOSQL)
			{
				m_errorMessage->Clear();
				m_errorMessage->Append("The index '");
				m_errorMessage->Append(indexName);
				m_errorMessage->Append("' is not defined in the database.");

				OOSQL_ERR(e);
			}
			else if(e < eNOERROR) OOSQL_ERR(e);

			break;
		case DBCOMMAND_DEFINE_POSTING_STRUCTURE:
			e = makeStringFromStringPool(className, dbCommand[i].definePostingStructureInfo.className);
			if(e < eNOERROR) OOSQL_ERR(e);

			e = makeStringFromStringPool(attrName, dbCommand[i].definePostingStructureInfo.attributeName);
			if(e < eNOERROR) OOSQL_ERR(e);

			postingStructureInfo.isContainingTupleID            = dbCommand[i].definePostingStructureInfo.isContainingTupleID;
			postingStructureInfo.isContainingSentenceAndWordNum = dbCommand[i].definePostingStructureInfo.isContainingSentenceAndWordNum;
			postingStructureInfo.isContainingByteOffset         = dbCommand[i].definePostingStructureInfo.isContainingByteOffset;
			postingStructureInfo.nEmbeddedAttributes            = dbCommand[i].definePostingStructureInfo.nEmbeddedAttributes;
			for(j = 0; j < postingStructureInfo.nEmbeddedAttributes; j++)
				postingStructureInfo.embeddedAttrNo[j] = dbCommand[i].definePostingStructureInfo.embeddedAttrNo[j];

			e = m_storageManager->Text_DefinePostingStructure(m_volID, className, attrName, &postingStructureInfo);
			if(e < eNOERROR) OOSQL_ERR(e);

			break;
		case DBCOMMAND_CREATE_FUNCTION:
			queryString =  "INSERT INTO LOM_SYS_FUNCTIONS";
			queryString += "(FuncSchema, FuncName, SpecificName, Definer, FuncID, ReturnType, ";
			queryString += "Origin, Type, ParamCount, ParamSignature, CreateTime, Deterministic, ";
			queryString += "SideEffects, Fenced, NullCall, CastFunction, AssignFunction, ScratchPad, ";
			queryString += "FinalCall, Parallelizable, Dbinfo, ResultCols, Language, Implementation, ";
			queryString += "ParamStyle, SourceSchema, SourceSpecific) VALUES(";

			// FuncSchema
			queryString += "''";
			queryString += ",";

			// FuncName
			e = makeStringFromStringPool(temp, dbCommand[i].createFunctionInfo.functionName);
			if(e < eNOERROR) OOSQL_ERR(e);
			
			queryString += "'";
			queryString += temp;
			queryString += "'";
			queryString += ",";

			// SpecificName
			e = makeStringFromStringPool(temp, dbCommand[i].createFunctionInfo.specificName);
			if(e < eNOERROR) OOSQL_ERR(e);
			
			queryString += "'";
			queryString += temp;
			queryString += "'";
			queryString += ",";

			// Definer
			queryString += "''";
			queryString += ",";

			// FuncID
			e = m_storageManager->GetCounterId(m_volID, "LOM_SYS_FUNCTION_ID_COUNTER", &counterId);
			if(e < eNOERROR) OOSQL_ERR(e);

			e = m_storageManager->GetCounterValues(m_volID, &counterId, 1, &funcId);
			if(e < eNOERROR) OOSQL_ERR(e);

			sprintf(temp, "%ld", funcId);
			queryString += temp;
			queryString += ",";

			// ReturnType
			sprintf(temp, "%ld", dbCommand[0].createFunctionInfo.returnType.argumentType);
			queryString += temp;
			queryString += ",";

			// Origin
			queryString += "'E'";
			queryString += ",";

			// Type
			queryString += "'S'";
			queryString += ",";

			// ParamCount
			sprintf(temp, "%ld", dbCommand[0].createFunctionInfo.argumentTypeList.size);
			queryString += temp;
			queryString += ",";

			// ParamSignature
			argType = dbCommand[0].createFunctionInfo.argumentTypeList;
			queryString += "'";
			for(i = 0; i < dbCommand[0].createFunctionInfo.argumentTypeList.size; i++)
			{
				sprintf(temp, "%02d", argType[i].argumentType);
				queryString += temp;
			}
			queryString += "'";
			queryString += ",";

			// CreateTime
			clk = time(0);
			now = localtime(&clk);

			sprintf(temp, "TIMESTAMP '%ld-%ld-%ld-%ld.%ld.%ld'", now->tm_year, now->tm_mon + 1, now->tm_mday, 
				                                           now->tm_hour, now->tm_min, now->tm_sec);
			queryString += temp;
			queryString += ",";

			// Deterministic
			if(dbCommand[0].createFunctionInfo.deterministic)
				queryString += "'Y'";
			else
				queryString += "'N'";
			queryString += ",";

			// SideEffects
			queryString += "'N'";
			queryString += ",";

			// Fenced
			if(dbCommand[0].createFunctionInfo.fenced)
				queryString += "'Y'";
			else
				queryString += "'N'";
			queryString += ",";

			// NullCall
			if(dbCommand[0].createFunctionInfo.nullCall)
				queryString += "'Y'";
			else
				queryString += "'N'";
			queryString += ",";

			// CastFunction
			queryString += "'N'";
			queryString += ",";

			// AssignFunction
			queryString += "'N'";
			queryString += ",";

			// ScratchPad
			if(dbCommand[0].createFunctionInfo.scratchPad)
				queryString += "'Y'";
			else
				queryString += "'N'";
			queryString += ",";

			// FinalCall
			if(dbCommand[0].createFunctionInfo.finalCall)
				queryString += "'Y'";
			else
				queryString += "'N'";
			queryString += ",";

			// Parallelizable
			if(dbCommand[0].createFunctionInfo.allowParallel)
				queryString += "'Y'";
			else
				queryString += "'N'";
			queryString += ",";

			// Dbinfo
			if(dbCommand[0].createFunctionInfo.dbInfo)
				queryString += "'Y'";
			else
				queryString += "'N'";
			queryString += ",";

			// ResultCols
			sprintf(temp, "%ld", 0);
			queryString += temp;
			queryString += ",";

			// Language
			e = makeStringFromStringPool(temp, dbCommand[0].createFunctionInfo.language);
			if(e < eNOERROR) OOSQL_ERR(e);
			queryString += "'";
			queryString += temp;
			queryString += "'";
			queryString += ",";

			// Implementation
			e = makeStringFromStringPool(temp, dbCommand[0].createFunctionInfo.externalName);
			if(e < eNOERROR) OOSQL_ERR(e);
			queryString += "'";
			queryString += temp;
			queryString += "'";
			queryString += ",";

			// ParamStyle
			e = makeStringFromStringPool(temp, dbCommand[0].createFunctionInfo.parameterStyle);
			if(e < eNOERROR) OOSQL_ERR(e);
			queryString += "'";
			queryString += temp;
			queryString += "'";
			queryString += ",";

			// SourceSchema
			queryString += "''";
			queryString += ",";

			// SourceSpecific
			queryString += "''";
			queryString += ")";

			e = oosql_SQL_InitExecFinal(m_storageManager->GetOOSQL_SystemHandle(), m_volID, queryString);
			if(e < eNOERROR) OOSQL_ERR(e);

			// create each parameters
			argType = dbCommand[0].createFunctionInfo.argumentTypeList;
			for(i = 0; i < dbCommand[0].createFunctionInfo.argumentTypeList.size; i++)
			{
				queryString  = "INSERT INTO LOM_SYS_FUNCPARMS";
				queryString += "(FuncSchema, FuncName, SpecificName, FuncID, RowType, Ordinal, ParmName, ";
				queryString += "TypeSchema, TypeName, TypeID, Length, Scale, CodePage, CastFuncID, AsLocator) ";
				queryString += "VALUES(";

				// FuncSchema
				queryString += "''";
				queryString += ",";

				// FuncName
				e = makeStringFromStringPool(temp, dbCommand[0].createFunctionInfo.functionName);
				if(e < eNOERROR) OOSQL_ERR(e);
				
				queryString += "'";
				queryString += temp;
				queryString += "'";
				queryString += ",";

				// SpecificName
				e = makeStringFromStringPool(temp, dbCommand[0].createFunctionInfo.specificName);
				if(e < eNOERROR) OOSQL_ERR(e);
				
				queryString += "'";
				queryString += temp;
				queryString += "'";
				queryString += ",";

				// FuncID
				sprintf(temp, "%ld", funcId);
				queryString += temp;
				queryString += ",";

				// RowType
				queryString += "'P'";		// A parameter of the function
				queryString += ",";

				// Ordinal
				sprintf(temp, "%ld", i + 1);	
				queryString += temp;		
				queryString += ",";

				// ParamName
				e = makeStringFromStringPool(temp, argType[i].argumentName);
				queryString += "'";
				queryString += temp;
				queryString += "'";
				queryString += ",";

				// TypeSchema
				queryString += "''";
				queryString += ",";

				// TypeName
				e = makeStringFromStringPool(temp, argType[i].argumentTypeName);
				queryString += "'";
				queryString += temp;
				queryString += "'";
				queryString += ",";

				// TypeID
				sprintf(temp, "%ld", argType[i].argumentType);
				queryString += temp;
				queryString += ",";

				// Length
				sprintf(temp, "%ld", argType[i].argumentTypeLength);
				queryString += temp;
				queryString += ",";

				// Scale
				queryString += "0";
				queryString += ",";

				// CodePage
				queryString += "0";
				queryString += ",";

				// CastFuncID
				queryString += "0";
				queryString += ",";

				// AsLocator
				if(argType[i].asLocator)
					queryString += "'Y'";
				else
					queryString += "'N'";
				queryString += ")";

				e = oosql_SQL_InitExecFinal(m_storageManager->GetOOSQL_SystemHandle(), m_volID, queryString);
				if(e < eNOERROR) OOSQL_ERR(e);
			}

			// invalidate function information in external function manager
			e = makeStringFromStringPool(temp, dbCommand[i].createFunctionInfo.functionName);
			if(e < eNOERROR) OOSQL_ERR(e);

			e = m_externalFunctionManager->Invalidate(m_volID, temp);
			if(e < eNOERROR) OOSQL_ERR(e);

			break;
		case DBCOMMAND_DROP_FUNCTION:
			// get FuncID
			if(dbCommand[0].dropFunctionInfo.specific)
			{
				queryString =  "SELECT FuncID FROM LOM_SYS_FUNCTIONS WHERE ";
				queryString += "SpecificName = '";
				e = makeStringFromStringPool(temp, dbCommand[0].dropFunctionInfo.functionName);
				if(e < eNOERROR) OOSQL_ERR(e);
				queryString += temp;
				queryString += "'";
			}
			else
			{
				queryString =  "SELECT FuncID FROM LOM_SYS_FUNCTIONS WHERE ";
				queryString += "FuncName = '";
				e = makeStringFromStringPool(temp, dbCommand[0].dropFunctionInfo.functionName);
				if(e < eNOERROR) OOSQL_ERR(e);
				queryString += temp;
				queryString += "'";

				if(dbCommand[0].dropFunctionInfo.argumentTypeList != NULL_POOLINDEX)
				{
					queryString += " AND ParamSignature = '";
					argType = dbCommand[0].dropFunctionInfo.argumentTypeList;
					for(i = 0; i < dbCommand[0].dropFunctionInfo.argumentTypeList.size; i++)
					{
						sprintf(temp, "%02d", argType[i].argumentType);
						queryString += temp;
					}
					queryString += "'";
				}
			}

			e = oosql_SQL_Init(m_storageManager->GetOOSQL_SystemHandle(), m_volID, &handle);
			if(e < eNOERROR) OOSQL_ERR(e);
			e = oosql_SQL_Exec(m_storageManager->GetOOSQL_SystemHandle(), handle, queryString);
			if(e < eNOERROR) OOSQL_ERR(e);
			e = oosql_SQL_Next(m_storageManager->GetOOSQL_SystemHandle(), handle, &funcId);
			if(e == eNORESULTS_OOSQL)
			{
				OOSQL_ERR(eNOSUCH_FUNCTION_EXIST_OOSQL);
			}
			else if(e < eNOERROR) OOSQL_ERR(e);
			e = oosql_SQL_Final(m_storageManager->GetOOSQL_SystemHandle(), handle);
			if(e < eNOERROR) OOSQL_ERR(e);

			// delete LOM_SYS_FUNCTIONS
			queryString  = "DELETE FROM LOM_SYS_FUNCTIONS WHERE FuncID = ";
			sprintf(temp, "%ld", funcId);
			queryString += temp;

			e = oosql_SQL_InitExecFinal(m_storageManager->GetOOSQL_SystemHandle(), m_volID, queryString);
			if(e < eNOERROR) OOSQL_ERR(e);

			// delete LOM_SYS_FUNCPARMS
			queryString  = "DELETE FROM LOM_SYS_FUNCPARMS WHERE FuncID = ";
			sprintf(temp, "%ld", funcId);
			queryString += temp;

			e = oosql_SQL_InitExecFinal(m_storageManager->GetOOSQL_SystemHandle(), m_volID, queryString);
			if(e < eNOERROR) OOSQL_ERR(e);

			// invalidate function information in external function manager
			e = makeStringFromStringPool(temp, dbCommand[i].dropFunctionInfo.functionName);
			if(e < eNOERROR) OOSQL_ERR(e);

			e = m_externalFunctionManager->Invalidate(m_volID, temp);
			if(e < eNOERROR) OOSQL_ERR(e);
			
			break;
		case DBCOMMAND_CREATE_PROCEDURE:
			queryString =  "INSERT INTO LOM_SYS_PROCEDURES";
			queryString += "(ProcSchema, ProcName, SpecificName, Definer, ProcID, ";
			queryString += "Origin, Type, ParamCount, ParamSignature, CreateTime, Deterministic, ";
			queryString += "Fenced, NullCall, Language, Implementation, ParamStyle, ResultSets, ";
			queryString += "Remarks) VALUES(";

			// ProcSchema
			queryString += "''";
			queryString += ",";

			// procName
			e = makeStringFromStringPool(temp, dbCommand[i].createProcedureInfo.procedureName);
			if(e < eNOERROR) OOSQL_ERR(e);
			
			queryString += "'";
			queryString += temp;
			queryString += "'";
			queryString += ",";

			// SpecificName
			e = makeStringFromStringPool(temp, dbCommand[i].createProcedureInfo.specificName);
			if(e < eNOERROR) OOSQL_ERR(e);
			
			queryString += "'";
			queryString += temp;
			queryString += "'";
			queryString += ",";

			// Definer
			queryString += "''";
			queryString += ",";

			// ProcID
			e = m_storageManager->GetCounterId(m_volID, "LOM_SYS_FUNCTION_ID_COUNTER", &counterId);
			if(e < eNOERROR) OOSQL_ERR(e);

			e = m_storageManager->GetCounterValues(m_volID, &counterId, 1, &procId);
			if(e < eNOERROR) OOSQL_ERR(e);

			sprintf(temp, "%ld", procId);
			queryString += temp;
			queryString += ",";

			// Origin
			queryString += "'E'";
			queryString += ",";

			// Type
			queryString += "'S'";
			queryString += ",";

			// ParamCount
			sprintf(temp, "%ld", dbCommand[0].createProcedureInfo.argumentTypeList.size);
			queryString += temp;
			queryString += ",";

			// ParamSignature
			argType = dbCommand[0].createProcedureInfo.argumentTypeList;
			queryString += "'";
			for(i = 0; i < dbCommand[0].createProcedureInfo.argumentTypeList.size; i++)
			{
				sprintf(temp, "%02d", argType[i].argumentType);
				queryString += temp;
			}
			queryString += "'";
			queryString += ",";

			// CreateTime
			clk = time(0);
			now = localtime(&clk);

			sprintf(temp, "TIMESTAMP '%ld-%ld-%ld-%ld.%ld.%ld'", now->tm_year, now->tm_mon + 1, now->tm_mday, 
				                                           now->tm_hour, now->tm_min, now->tm_sec);
			queryString += temp;
			queryString += ",";

			// Deterministic
			if(dbCommand[0].createProcedureInfo.deterministic)
				queryString += "'Y'";
			else
				queryString += "'N'";
			queryString += ",";

			// Fenced
			if(dbCommand[0].createProcedureInfo.fenced)
				queryString += "'Y'";
			else
				queryString += "'N'";
			queryString += ",";

			// NullCall
			if(dbCommand[0].createProcedureInfo.nullCall)
				queryString += "'Y'";
			else
				queryString += "'N'";
			queryString += ",";

			// Language
			e = makeStringFromStringPool(temp, dbCommand[0].createProcedureInfo.language);
			if(e < eNOERROR) OOSQL_ERR(e);
			queryString += "'";
			queryString += temp;
			queryString += "'";
			queryString += ",";

			// Implementation
			e = makeStringFromStringPool(temp, dbCommand[0].createProcedureInfo.externalName);
			if(e < eNOERROR) OOSQL_ERR(e);
			queryString += "'";
			queryString += temp;
			queryString += "'";
			queryString += ",";

			// ParamStyle
			e = makeStringFromStringPool(temp, dbCommand[0].createProcedureInfo.parameterStyle);
			if(e < eNOERROR) OOSQL_ERR(e);
			queryString += "'";
			queryString += temp;
			queryString += "'";
			queryString += ",";

			// ResultSets
			sprintf(temp, "%ld", dbCommand[0].createProcedureInfo.resultsets);
			queryString += temp;
			queryString += ",";

			// Remarks
			queryString += "''";
			queryString += ")";

			e = oosql_SQL_InitExecFinal(m_storageManager->GetOOSQL_SystemHandle(), m_volID, queryString);
			if(e < eNOERROR) OOSQL_ERR(e);
			
			// create each parameters
			argType = dbCommand[0].createProcedureInfo.argumentTypeList;
			for(i = 0; i < dbCommand[0].createProcedureInfo.argumentTypeList.size; i++)
			{
				queryString  = "INSERT INTO LOM_SYS_PROCPARMS";
				queryString += "(ProcSchema, ProcName, SpecificName, ProcID, RowType, Ordinal, ParmName, ";
				queryString += "TypeSchema, TypeName, TypeID, Length, Scale, CodePage, ParamMode, ";
				queryString += "AsLocator) VALUES(";

				// ProcSchema
				queryString += "''";
				queryString += ",";

				// ProcName
				e = makeStringFromStringPool(temp, dbCommand[0].createProcedureInfo.procedureName);
				if(e < eNOERROR) OOSQL_ERR(e);
				
				queryString += "'";
				queryString += temp;
				queryString += "'";
				queryString += ",";

				// SpecificName
				e = makeStringFromStringPool(temp, dbCommand[0].createProcedureInfo.specificName);
				if(e < eNOERROR) OOSQL_ERR(e);
				
				queryString += "'";
				queryString += temp;
				queryString += "'";
				queryString += ",";

				// ProcID
				sprintf(temp, "%ld", procId);
				queryString += temp;
				queryString += ",";

				// RowType
				queryString += "'P'";		// A parameter of the function
				queryString += ",";

				// Ordinal
				sprintf(temp, "%ld", i + 1);	
				queryString += temp;		
				queryString += ",";

				// ParamName
				e = makeStringFromStringPool(temp, argType[i].argumentName);
				queryString += "'";
				queryString += temp;
				queryString += "'";
				queryString += ",";

				// TypeSchema
				queryString += "''";
				queryString += ",";

				// TypeName
				e = makeStringFromStringPool(temp, argType[i].argumentTypeName);
				queryString += "'";
				queryString += temp;
				queryString += "'";
				queryString += ",";

				// TypeID
				sprintf(temp, "%ld", argType[i].argumentType);
				queryString += temp;
				queryString += ",";

				// Length
				sprintf(temp, "%ld", argType[i].argumentTypeLength);
				queryString += temp;
				queryString += ",";

				// Scale
				queryString += "0";
				queryString += ",";

				// CodePage
				queryString += "0";
				queryString += ",";

				// ParamMode
				switch(argType[i].parameterMode)
				{
				case PARM_IN:
					queryString += "'IN'";
					break;
				case PARM_OUT:
					queryString += "'OUT'";
					break;
				case PARM_INOUT:
					queryString += "'INOUT'";
					break;
				default:
					OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
				}
				queryString += ",";

				// AsLocator
				if(argType[i].asLocator)
					queryString += "'Y'";
				else
					queryString += "'N'";
				queryString += ")";

				
				e = oosql_SQL_InitExecFinal(m_storageManager->GetOOSQL_SystemHandle(), m_volID, queryString);
				if(e < eNOERROR) OOSQL_ERR(e);
			}

			// invalidate function information in external function manager
			e = makeStringFromStringPool(temp, dbCommand[i].createProcedureInfo.procedureName);
			if(e < eNOERROR) OOSQL_ERR(e);

			e = m_externalFunctionManager->Invalidate(m_volID, temp);
			if(e < eNOERROR) OOSQL_ERR(e);

			break;
		case DBCOMMAND_DROP_PROCEDURE:
			// get ProcID
			if(dbCommand[0].dropProcedureInfo.specific)
			{
				queryString =  "SELECT ProcID FROM LOM_SYS_PROCEDURES WHERE ";
				queryString += "SpecificName = '";
				e = makeStringFromStringPool(temp, dbCommand[0].dropProcedureInfo.procedureName);
				if(e < eNOERROR) OOSQL_ERR(e);
				queryString += temp;
				queryString += "'";
			}
			else
			{
				queryString =  "SELECT ProcID FROM LOM_SYS_PROCEDURES WHERE ";
				queryString += "ProcName = '";
				e = makeStringFromStringPool(temp, dbCommand[0].dropProcedureInfo.procedureName);
				if(e < eNOERROR) OOSQL_ERR(e);
				queryString += temp;
				queryString += "'";
			}

			e = oosql_SQL_Init(m_storageManager->GetOOSQL_SystemHandle(), m_volID, &handle);
			if(e < eNOERROR) OOSQL_ERR(e);
			e = oosql_SQL_Exec(m_storageManager->GetOOSQL_SystemHandle(), handle, queryString);
			if(e < eNOERROR) OOSQL_ERR(e);
			e = oosql_SQL_Next(m_storageManager->GetOOSQL_SystemHandle(), handle, &procId);
			if(e == eNORESULTS_OOSQL)
			{
				OOSQL_ERR(eNOSUCH_PROCEDURE_EXIST_OOSQL);
			}
			else if(e < eNOERROR) OOSQL_ERR(e);
			e = oosql_SQL_Final(m_storageManager->GetOOSQL_SystemHandle(), handle);
			if(e < eNOERROR) OOSQL_ERR(e);
			
			// delete LOM_SYS_PROCPARMS
			queryString  = "DELETE FROM LOM_SYS_PROCEDURES WHERE ProcID = ";
			sprintf(temp, "%ld", procId);
			queryString += temp;

			
			e = oosql_SQL_InitExecFinal(m_storageManager->GetOOSQL_SystemHandle(), m_volID, queryString);
			if(e < eNOERROR) OOSQL_ERR(e);
			
			// delete LOM_SYS_PROCPARMS
			queryString  = "DELETE FROM LOM_SYS_PROCPARMS WHERE ProcID = ";
			sprintf(temp, "%ld", procId);
			queryString += temp;

			e = oosql_SQL_InitExecFinal(m_storageManager->GetOOSQL_SystemHandle(), m_volID, queryString);
			if(e < eNOERROR) OOSQL_ERR(e);
			
			// invalidate function information in external function manager
			e = makeStringFromStringPool(temp, dbCommand[i].dropProcedureInfo.procedureName);
			if(e < eNOERROR) OOSQL_ERR(e);

			e = m_externalFunctionManager->Invalidate(m_volID, temp);
			if(e < eNOERROR) OOSQL_ERR(e);

			break;
		case DBCOMMAND_CALL_PROCEDURE:
			break;
		default:
			OOSQL_ERR(eNOTIMPLEMENTED_OOSQL); // unhandled case error
		}
	}
	return eNOERROR;
}
