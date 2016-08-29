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
        oosql_Eval_Access.cxx

    DESCRIPTION:

*/

#include <unistd.h>
#include "OOSQL_Evaluator.hxx"
#include "oosql_Eval_Access.hxx"
#include "OOSQL_SortStream.hxx"

Four    OOSQL_Evaluator::initAccessListTable()
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
    Four planIndex;
    Four numClasses;
    OOSQL_AccessElement *pAccessList;
    AP_SubClassPoolElements pSubClassInfo;
    Four ocn;
    Four i;
    Four e;

    /* allocate memory for the Access List Table */
	OOSQL_ARRAYNEW(m_evalAccessListTable, pMemoryManager, OOSQL_AccessList, ACCESSPLAN.getNumAP_Elem());
    if (m_evalAccessListTable == NULL)
        OOSQL_ERR(eOUTOFMEMORY_OOSQL);

    /* allocate memory for B tree index scan
     * NOTE: We now allocate an array of Four as long as the size of 
     *   index info. m_pool. But this does not waste so much memory 
     *   because scan ID is Four type (of which length is four).
     */
	OOSQL_NEW(m_evalIndexScanInfoTable, pMemoryManager, OOSQL_ScanInfoTable(ACCESSPLAN.getIndexInfoPoolSize()));
    if(m_evalIndexScanInfoTable == NULL)
        OOSQL_ERR(eOUTOFMEMORY_OOSQL);
	m_evalIndexScanInfoTableElements = m_evalIndexScanInfoTable->scanInfos;
	
	// allocate index evaluation buffer
	if(ACCESSPLAN.getIndexInfoPoolSize() > 0)
	{
		OOSQL_ARRAYNEW(m_indexEvaluationBuffer, pMemoryManager, OOSQL_EvalIndexBuffer, ACCESSPLAN.getIndexInfoPoolSize());
		if(m_indexEvaluationBuffer == NULL)
			OOSQL_ERR(eOUTOFMEMORY_OOSQL);
	}

    /*******************************
     * construct access list table *
     *******************************/

    for (planIndex = 0; planIndex < ACCESSPLAN.getNumAP_Elem(); planIndex++) {
        /* 
         * construct scan list for the current access plan elem. 
         */
        switch ( ACCESSPLANELEMENTS[planIndex].classInfo.classKind ) {
            case CLASSKIND_PERSISTENT:

                /* allocate memory for the current access information 
                 * NOTE: 'numClasses' to access = the # of subclasses + 1 (the current class) 
                 */
                numClasses = GET_POOLSIZE( ACCESSPLANELEMENTS[planIndex].subClassInfo ) + 1;

				e = EVAL_ACCESSLISTTABLE[planIndex].init(numClasses);
				OOSQL_CHECK_ERR(e);

                /* get pointer to the access list for the current access plan elem. */
                pAccessList = &(EVAL_ACCESSLISTTABLE[planIndex].accessList[0]);

                /* copy class ID to the current access information */
                pAccessList[0].classId = ACCESSPLANELEMENTS[planIndex].classInfo.classId;

                /* copy access method and in case of index scan get index info. m_pool index */
                pAccessList[0].accessMethod = ACCESSPLANELEMENTS[planIndex].accessMethod;
                switch ( ACCESSPLANELEMENTS[planIndex].accessMethod ) {
                    case ACCESSMETHOD_SEQ_SCAN:
                        /* initialize scan direction as forward */
                        pAccessList[0].scanDirection = FORWARD;
                        break;
                    case ACCESSMETHOD_BTREE_IDXSCAN:
                    case ACCESSMETHOD_TEXT_IDXSCAN:
                    case ACCESSMETHOD_MLGF_IDXSCAN:
                    case ACCESSMETHOD_MLGF_MBR_IDXSCAN: 
                        pAccessList[0].indexInfo = ACCESSPLANELEMENTS[planIndex].indexInfo;
                        break;
                }

                /* get open class num. for the current class */
                ocn = m_storageManager->GetOpenClassNum( m_volID, pAccessList[0].classId );
                if (ocn < eNOERROR) {
					if(ocn == eDEADLOCK)
					{
						OOSQL_ERR(eDEADLOCK);
					}
					else
					{
                    	OOSQL_ERR( eCLASS_NOTOPENED_OOSQL);
					}
                }
                else {
                    pAccessList[0].ocn = ocn;
                }

                /*
                 * fill scan list elem. in order to open scan(s) for the subclass(es)
                 * NOTE: subclass access info. begins at the 2nd element of access list,
                 *       so the index to access list is (i+1) 
                 *       when the index to subclass info. m_pool is i.
                 */

                /* get pointer to the subclass information m_pool */
                pSubClassInfo = ACCESSPLAN.getSubClassPool(planIndex);

                for (i = 0; i < (numClasses - 1); i++) {
                    /* copy class ID to the current access information */
                    pAccessList[i+1].classId = pSubClassInfo[i].classId;

                    /* get open class num. for the current subclass */
                    ocn = m_storageManager->GetOpenClassNum( m_volID, pAccessList[i+1].classId );
                    if (ocn < eNOERROR) {
                        OOSQL_ERR( eCLASS_NOTOPENED_OOSQL);
                    }
                    else {
                        pAccessList[i+1].ocn = ocn;
                    }

                    /* get access method and in case of index scan get index info. m_pool index */
                    pAccessList[i+1].accessMethod = pSubClassInfo[i].accessMethod;
                    switch ( pAccessList[i+1].accessMethod ) {
                        case ACCESSMETHOD_SEQ_SCAN:
                            /* initialize scan direction as forward */
                            pAccessList[i+1].scanDirection = FORWARD;
                            break;
                        case ACCESSMETHOD_BTREE_IDXSCAN:
                        case ACCESSMETHOD_TEXT_IDXSCAN:
                        case ACCESSMETHOD_MLGF_IDXSCAN:
                        case ACCESSMETHOD_MLGF_MBR_IDXSCAN: 
                            pAccessList[i+1].indexInfo = pSubClassInfo[i].indexInfo;
                            break;
                    }
                }

                break;

            case CLASSKIND_TEMPORARY:
			case CLASSKIND_SORTSTREAM:
                /* get open class num. for the temporary file
                 * NOTE: This temporary file has been created and it doesn't have any subclass.
                 */
                numClasses = 1;
				e = EVAL_ACCESSLISTTABLE[planIndex].init(numClasses);
				OOSQL_CHECK_ERR(e);

                /* get pointer to the access list for the current access plan elem. */
                pAccessList = &(EVAL_ACCESSLISTTABLE[planIndex].accessList[0]);

                /* get open class num. from the temporary file info. table */
                pAccessList[0].ocn = EVAL_TEMPFILEINFOTABLEELEMENTS[ ACCESSPLANELEMENTS[planIndex].classInfo.tempFileNum ]->getOcn();

                /* get access method and in case of index scan, get index info. m_pool index */
                pAccessList[0].accessMethod = ACCESSPLANELEMENTS[planIndex].accessMethod;
                switch (pAccessList[0].accessMethod) {
                    case ACCESSMETHOD_SEQ_SCAN:
                        /* [Internal Documentation]
                         * To utilize LRU page buffer replacement we reverse sequential scan 
                         * direction for each access of an inner class of a nested-loop.
                         * Reversing is performed at moveToAndOpenNextPlanElement() before
                         * opening scan.
                         * So we initialize scan direction as backward.
                         *
                         * NOTE: This scheme is useless for an outer class of a nested-loop,
                         *           but we apply the same (and simple) method to it.
                         */
                        pAccessList[0].scanDirection = BACKWARD;
                        break;

                    case ACCESSMETHOD_BTREE_IDXSCAN:
                    case ACCESSMETHOD_TEXT_IDXSCAN:
                    case ACCESSMETHOD_MLGF_IDXSCAN:
                    case ACCESSMETHOD_MLGF_MBR_IDXSCAN: 
                        pAccessList[0].indexInfo = ACCESSPLANELEMENTS[planIndex].indexInfo;
                        break;
                }

                break;

			case CLASSKIND_NONE:
				break;

            case CLASSKIND_NULL_AGGRFUNC_ONLY:
			
            /* NOTE: In this case, we just process aggregate function(s) 
             *       but do not access any class.
             */
                numClasses = 0;
				e = EVAL_ACCESSLISTTABLE[planIndex].init(numClasses);
				OOSQL_CHECK_ERR(e);
                break;

            default:    /* invalid class kind */
                OOSQL_ERR(eINVALID_CASE_OOSQL);
        } /* switch */

		// construct write collist struct information
		// DML을 위한 처리
		switch ( ACCESSPLANELEMENTS[planIndex].classInfo.classKind ) 
		{
        case CLASSKIND_PERSISTENT:
        case CLASSKIND_TEMPORARY:
		case CLASSKIND_SORTSTREAM:
			if(ACCESSPLANELEMENTS[planIndex].projectionList != NULL_POOLINDEX &&
			   ACCESSPLANELEMENTS[planIndex].projectionList.getElements(m_pool->projectionListPool,0).projectionType != PROJECTION_NONE)
			{
				OOSQL_StorageManager::LockParameter					lockup;
				Four							classId;
				AP_ProjectionListPoolElements	projectionList;
				AP_UpdateValuePoolElements		updateValueList;
				AP_InsertValuePoolElements		insertValueList;
				AP_ExprPoolElements				expr;
				Four							colListSize;
				Four							index;

				projectionList = ACCESSPLANELEMENTS[planIndex].projectionList.getElements(m_pool->projectionListPool);

				switch(projectionList[0].projectionType)
				{
				case PROJECTION_UPDATE:
					classId = projectionList[0].updateInfo.classInfo.classId;

					updateValueList = projectionList[0].updateInfo.updateValueList.getElements(m_pool->ap_updateValuePool);
					for(colListSize = 0, i = 0; i < updateValueList.size; i++)
					{
						if(updateValueList[i].isParam)		
							continue;					

						if(updateValueList[i].type == TYPEID_TEXT ||
						   OOSQL_MASK_COMPLEXTYPE(updateValueList[i].type) == COMPLEXTYPEID_SET ||
						   OOSQL_MASK_COMPLEXTYPE(updateValueList[i].type) == COMPLEXTYPEID_BAG ||
						   OOSQL_MASK_COMPLEXTYPE(updateValueList[i].type) == COMPLEXTYPEID_LIST)
							continue;					
						colListSize ++;
					}
					
					EVAL_ACCESSLISTTABLE[planIndex].writeColList = (OOSQL_StorageManager::ColListStruct*)pMemoryManager->Alloc(sizeof(OOSQL_StorageManager::ColListStruct) * colListSize);
					
					for(index = 0, i = 0; i < updateValueList.size; i++)
					{
						if(updateValueList[i].isParam)		
							continue;					

						if(updateValueList[i].type == TYPEID_TEXT ||
						   OOSQL_MASK_COMPLEXTYPE(updateValueList[i].type) == COMPLEXTYPEID_SET ||
						   OOSQL_MASK_COMPLEXTYPE(updateValueList[i].type) == COMPLEXTYPEID_BAG ||
						   OOSQL_MASK_COMPLEXTYPE(updateValueList[i].type) == COMPLEXTYPEID_LIST)
							continue;				

						EVAL_ACCESSLISTTABLE[planIndex].writeColList[index].nullFlag   = SM_FALSE;
						EVAL_ACCESSLISTTABLE[planIndex].writeColList[index].colNo      = updateValueList[i].colNo;
						EVAL_ACCESSLISTTABLE[planIndex].writeColList[index].start      = ALL_VALUE;
						EVAL_ACCESSLISTTABLE[planIndex].writeColList[index].length     = ALL_VALUE; 
						EVAL_ACCESSLISTTABLE[planIndex].writeColList[index].dataLength = updateValueList[i].length;
						
						if(updateValueList[i].type == TYPEID_STRING || updateValueList[i].type == TYPEID_VARSTRING)
							EVAL_ACCESSLISTTABLE[planIndex].writeColList[index].data.ptr = NULL;
						index ++;
					}
					break;

				case PROJECTION_INSERT:
					classId = projectionList[0].insertInfo.classInfo.classId;
					insertValueList = projectionList[0].insertInfo.insertValueList.getElements(m_pool->ap_insertValuePool);

					insertValueList = projectionList[0].insertInfo.insertValueList.getElements(m_pool->ap_insertValuePool);
					for(colListSize = 0, i = 0; i < insertValueList.size; i++)
					{
						if(insertValueList[i].isParam)		
							continue;					
						if(insertValueList[i].type == TYPEID_TEXT ||
						   OOSQL_MASK_COMPLEXTYPE(insertValueList[i].type) == COMPLEXTYPEID_SET ||
						   OOSQL_MASK_COMPLEXTYPE(insertValueList[i].type) == COMPLEXTYPEID_BAG ||
						   OOSQL_MASK_COMPLEXTYPE(insertValueList[i].type) == COMPLEXTYPEID_LIST)
							continue;					
						colListSize ++;
					}

					EVAL_ACCESSLISTTABLE[planIndex].writeColList = (OOSQL_StorageManager::ColListStruct*)pMemoryManager->Alloc(sizeof(OOSQL_StorageManager::ColListStruct) * colListSize);

					for(index = 0, i = 0; i < insertValueList.size; i++)
					{
						if(insertValueList[i].isParam)		
							continue;					
						if(insertValueList[i].type == TYPEID_TEXT ||
						   OOSQL_MASK_COMPLEXTYPE(insertValueList[i].type) == COMPLEXTYPEID_SET ||
						   OOSQL_MASK_COMPLEXTYPE(insertValueList[i].type) == COMPLEXTYPEID_BAG ||
						   OOSQL_MASK_COMPLEXTYPE(insertValueList[i].type) == COMPLEXTYPEID_LIST)
							continue;				
						
						EVAL_ACCESSLISTTABLE[planIndex].writeColList[index].nullFlag   = SM_FALSE;
						EVAL_ACCESSLISTTABLE[planIndex].writeColList[index].colNo      = insertValueList[i].colNo;
						EVAL_ACCESSLISTTABLE[planIndex].writeColList[index].start      = ALL_VALUE;
						EVAL_ACCESSLISTTABLE[planIndex].writeColList[index].length     = insertValueList[i].length;
						EVAL_ACCESSLISTTABLE[planIndex].writeColList[index].dataLength = insertValueList[i].length;
						
						if(insertValueList[i].type == TYPEID_STRING || insertValueList[i].type == TYPEID_VARSTRING)
							EVAL_ACCESSLISTTABLE[planIndex].writeColList[index].data.ptr = NULL;

						index ++;
					}
					break;
				case PROJECTION_DELETE:
					classId = projectionList[0].deleteInfo.classInfo.classId;
					EVAL_ACCESSLISTTABLE[planIndex].writeColList = NULL;
					break;
				default:
					OOSQL_ERR(eNOTIMPLEMENTED_OOSQL);	// UNHANDLED CASE
					break;
				}
				
				lockup.mode     = OOSQL_StorageManager::L_IX;
                lockup.duration = OOSQL_StorageManager::L_COMMIT;
				
				e = m_storageManager->GetOpenClassNum(m_volID, classId);
				OOSQL_CHECK_ERR(e);
				EVAL_ACCESSLISTTABLE[planIndex].writeOcn = e;
			}
			
			break;


		case CLASSKIND_NONE:
			numClasses = 0;
			e = EVAL_ACCESSLISTTABLE[planIndex].init(numClasses);
			OOSQL_CHECK_ERR(e);

			if(ACCESSPLANELEMENTS[planIndex].projectionList != NULL_POOLINDEX &&
			   ACCESSPLANELEMENTS[planIndex].projectionList.getElements(m_pool->projectionListPool,0).projectionType != PROJECTION_NONE)
			{
				OOSQL_StorageManager::LockParameter					lockup;
				Four							classId;
				AP_ProjectionListPoolElements	projectionList;
				AP_InsertValuePoolElements		insertValueList;
				Four							colListSize;
				Four							index;

				projectionList = ACCESSPLANELEMENTS[planIndex].projectionList.getElements(m_pool->projectionListPool);

				switch(projectionList[0].projectionType)
				{
				case PROJECTION_INSERT:
					classId = projectionList[0].insertInfo.classInfo.classId;

					insertValueList = projectionList[0].insertInfo.insertValueList.getElements(m_pool->ap_insertValuePool);
					for(colListSize = 0, i = 0; i < insertValueList.size; i++)
					{
						if(insertValueList[i].isParam)		
							continue;					
						if(insertValueList[i].type == TYPEID_TEXT ||
						   OOSQL_MASK_COMPLEXTYPE(insertValueList[i].type) == COMPLEXTYPEID_SET ||
						   OOSQL_MASK_COMPLEXTYPE(insertValueList[i].type) == COMPLEXTYPEID_BAG ||
						   OOSQL_MASK_COMPLEXTYPE(insertValueList[i].type) == COMPLEXTYPEID_LIST)
							continue;					
						colListSize ++;
					}

					EVAL_ACCESSLISTTABLE[planIndex].writeColList = (OOSQL_StorageManager::ColListStruct*)pMemoryManager->Alloc(sizeof(OOSQL_StorageManager::ColListStruct) * colListSize);

					for(index = 0, i = 0; i < insertValueList.size; i++)
					{
						if(insertValueList[i].isParam)		
							continue;					
						if(insertValueList[i].type == TYPEID_TEXT ||
						   OOSQL_MASK_COMPLEXTYPE(insertValueList[i].type) == COMPLEXTYPEID_SET ||
						   OOSQL_MASK_COMPLEXTYPE(insertValueList[i].type) == COMPLEXTYPEID_BAG ||
						   OOSQL_MASK_COMPLEXTYPE(insertValueList[i].type) == COMPLEXTYPEID_LIST)
							continue;					

						EVAL_ACCESSLISTTABLE[planIndex].writeColList[index].nullFlag   = SM_FALSE;
						EVAL_ACCESSLISTTABLE[planIndex].writeColList[index].colNo      = insertValueList[i].colNo;
						EVAL_ACCESSLISTTABLE[planIndex].writeColList[index].start      = ALL_VALUE;
						EVAL_ACCESSLISTTABLE[planIndex].writeColList[index].length     = insertValueList[i].length;
						EVAL_ACCESSLISTTABLE[planIndex].writeColList[index].dataLength = insertValueList[i].length;

						if(insertValueList[i].type == TYPEID_STRING || insertValueList[i].type == TYPEID_VARSTRING)
							EVAL_ACCESSLISTTABLE[planIndex].writeColList[index].data.ptr = NULL;
						index ++;
					}
					break;
				default:
					OOSQL_ERR(eNOTIMPLEMENTED_OOSQL);	// UNHANDLED CASE
					break;
				}
				
				lockup.mode     = OOSQL_StorageManager::L_IX;
                lockup.duration = OOSQL_StorageManager::L_COMMIT;

				e = m_storageManager->GetOpenClassNum(m_volID, classId);
				OOSQL_CHECK_ERR(e);
				EVAL_ACCESSLISTTABLE[planIndex].writeOcn = e;
			}
			break;

        case CLASSKIND_NULL_AGGRFUNC_ONLY:
			break;

        default:    /* invalid class kind */
            OOSQL_ERR(eINVALID_CASE_OOSQL);
        } /* switch */
    } /* end of for */

    /* return */
    return eNOERROR;
}


Four    OOSQL_Evaluator::finalAccessListTable()
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
    Four planIndex;
    OOSQL_AccessElement *accessElement;
    Four i;
    Four e;

    if (IS_NULL_ACCESSPLAN == SM_TRUE)
        return(eNOERROR);

	if (EVAL_ACCESSLISTTABLE == NULL)
		return eNOERROR;

    /* close scans for each access plan element */
    for (planIndex = 0; planIndex < ACCESSPLAN.getNumAP_Elem(); planIndex++) {
        /* skip if there was no class to access 
         * NOTE: this case could occur if class kind = CLASSKIND_NULL_AGGRFUNC_ONLY
         */
        /* 
         * Close scans if the current access plan element is the outermost class of
         * a nested-loop (including a single class).
         * Scans of any inner class of a nested-loop are closed during backtrack()
         * but those of the outermost class are not.
         * So we close them here (at the time finalize all resources of a query processor instance).
         */
        if (ACCESSPLANELEMENTS[planIndex].joinMethod == JOINMETHOD_OUTERMOST_CLASS) {
			if(EVAL_ACCESSLISTTABLE[planIndex].writeColList != NULL)
			{
				pMemoryManager->Free(EVAL_ACCESSLISTTABLE[planIndex].writeColList);
				EVAL_ACCESSLISTTABLE[planIndex].writeColList = NULL;
			}

            for (i = 0; i < EVAL_ACCESSLISTTABLE[planIndex].numClasses; i++) {
                accessElement = &(EVAL_ACCESSLISTTABLE[planIndex].accessList[i]);

                switch (accessElement->accessMethod) {
                    case ACCESSMETHOD_OID_FETCH:
						break;

                    case ACCESSMETHOD_SEQ_SCAN:
                        e = closeSeqScan(planIndex, i);
                        if (e < eNOERROR && e != eSCANALREADYCLOSED_OOSQL)
                            OOSQL_ERR(e);

                        break;

                    case ACCESSMETHOD_BTREE_IDXSCAN:
                        e = closeBtreeIndexScan(planIndex, i);
                        if (e < eNOERROR && e != eSCANALREADYCLOSED_OOSQL)
                            OOSQL_ERR(e);
                        break;

                    case ACCESSMETHOD_TEXT_IDXSCAN:
						if(i == 0)
						{
							e = closeTextIndexScan(planIndex, i);
							if (e < eNOERROR && e != eSCANALREADYCLOSED_OOSQL)
								OOSQL_ERR(e);
						}

                        break;

                    case ACCESSMETHOD_MLGF_IDXSCAN:
                    case ACCESSMETHOD_MLGF_MBR_IDXSCAN: 
                        e = closeMLGF_IndexScan(planIndex, i);
                        if (e < eNOERROR && e != eSCANALREADYCLOSED_OOSQL)
                            OOSQL_ERR(e);

                        break;

                    default:
                        OOSQL_ERR(eINVALID_CASE_OOSQL);
                } /* end of switch */
            } /* end of for */
        }
        else {
            /* close scans if access method is OOSQL_StorageManager::OID fetch or index scan
             * NOTE: in case of index scan, we closes the scan to fetch object
             */
            for (i = 0; i < EVAL_ACCESSLISTTABLE[planIndex].numClasses; i++) {
                accessElement = &(EVAL_ACCESSLISTTABLE[planIndex].accessList[i]);

                switch (accessElement->accessMethod) {
                    case ACCESSMETHOD_OID_FETCH:
						break;

					case ACCESSMETHOD_SEQ_SCAN:
                        e = closeSeqScan(planIndex, i);
                        if (e < eNOERROR && e != eSCANALREADYCLOSED_OOSQL)
                            OOSQL_ERR(e);

                        break;

                    case ACCESSMETHOD_BTREE_IDXSCAN:
                        e = closeBtreeIndexScan(planIndex, i);
                        if (e < eNOERROR && e != eSCANALREADYCLOSED_OOSQL)	
                            OOSQL_ERR(e);
                        break;

                    case ACCESSMETHOD_TEXT_IDXSCAN:
						if(i == 0)
						{
							e = closeTextIndexScan(planIndex, i);
							if (e < eNOERROR && e != eSCANALREADYCLOSED_OOSQL)	
								OOSQL_ERR(e);
						}
                        break;

                    case ACCESSMETHOD_MLGF_IDXSCAN:
                    case ACCESSMETHOD_MLGF_MBR_IDXSCAN: 
                        e = closeMLGF_IndexScan(planIndex, i);
                        if (e < eNOERROR && e != eSCANALREADYCLOSED_OOSQL)	
                            OOSQL_ERR(e);
                        break;
                } /* end of switch */
            } /* end of for */
        } /* end of if */
    }

	/* deallocate memory */
	if(m_evalAccessListTable)
		OOSQL_ARRAYDELETE(OOSQL_AccessList, m_evalAccessListTable);
	m_evalAccessListTable = NULL;

	if(m_evalIndexScanInfoTable)
		OOSQL_DELETE(m_evalIndexScanInfoTable);
	m_evalIndexScanInfoTable = NULL;
	m_evalIndexScanInfoTableElements = NULL;

	if(m_indexEvaluationBuffer)
		OOSQL_ARRAYDELETE(OOSQL_EvalIndexBuffer, m_indexEvaluationBuffer);
	m_indexEvaluationBuffer = NULL;
    return(eNOERROR);
}


Four    OOSQL_Evaluator::openBtreeIndexScan(
    Four planIndex,             /* IN: */
    Four accessElemIndex        /* IN: */
)
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
    OOSQL_AccessElement					*accessElement;
    AP_IndexInfoElement					*indexInfoNode;
    OOSQL_StorageManager::BoundCond		startBound;
    OOSQL_StorageManager::BoundCond		stopBound;
    OOSQL_StorageManager::LockParameter lockup;
    Four								scanId;
    Four								e;

    /* check input parameters */
#ifdef  OOSQL_DEBUG
    if (planIndex < 0 || ACCESSPLAN.getNumAP_Elem() <= planIndex)
        OOSQL_ERR(eBADPARAMETER_OOSQL);
    if (accessElemIndex < 0 || EVAL_ACCESSLISTTABLE[planIndex].numClasses <= accessElemIndex)
        OOSQL_ERR(eBADPARAMETER_OOSQL);
#endif

    /* get pointer to the access element */
    accessElement = &(EVAL_ACCESSLISTTABLE[planIndex].accessList[accessElemIndex]);

    /* get index information for the current index info. tree node */
    indexInfoNode = ACCESSPLAN.getIndexInfoElem(accessElement->indexInfo);

    switch (indexInfoNode->nodeKind) { 
        case INDEXINFO_OPERATOR:
            OOSQL_ERR(eNOTIMPLEMENTED_BTREEINDEX_ANDOR_OOSQL);

        case INDEXINFO_SCAN:
            /* make bound condition 
             * NOTE: In access plan, bound conditions are represented as condition tree.
             *       So, we should convert those to the format of KAOSS.
             */
            e = makeBtreeIndexBoundCond( &(indexInfoNode->scan.btree), &startBound, &stopBound );
            OOSQL_CHECK_ERR(e);

            /* make lockup parameter and open scan */
            switch (ACCESSPLANELEMENTS[planIndex].classInfo.classKind) {
                case CLASSKIND_PERSISTENT:
                    /* set shared mode lock if persistent class and read query, otherwise exclusive lock */
					if(isUpdateQuery())
						lockup.mode = OOSQL_StorageManager::L_IX;
					else
						lockup.mode = OOSQL_StorageManager::L_IS;
                    lockup.duration = OOSQL_StorageManager::L_COMMIT;
                    break;

                case CLASSKIND_TEMPORARY:
				case CLASSKIND_SORTSTREAM:
                    /* set exclusive mode lock if temporary class */
                    lockup.mode = OOSQL_StorageManager::L_X;
                    lockup.duration = OOSQL_StorageManager::L_COMMIT;
                    break;

                default:
                    OOSQL_ERR(eINVALID_CASE_OOSQL);
            }

            scanId = m_storageManager->OpenIndexScan(accessElement->ocn, &(indexInfoNode->scan.indexId), &startBound, &stopBound, 
				                                     0, NULL, &lockup );
            if (scanId < eNOERROR) OOSQL_ERR(scanId);

            /* set scan Id in the entry of B+ tree index scan Id table */
            EVAL_INDEX_SCANINFOTABLEELEMENTS[accessElement->indexInfo.startIndex].scanId   = scanId;
			EVAL_INDEX_SCANINFOTABLEELEMENTS[accessElement->indexInfo.startIndex].ocn      = accessElement->ocn;
			EVAL_INDEX_SCANINFOTABLEELEMENTS[accessElement->indexInfo.startIndex].colNoMap = &ACCESSPLANELEMENTS[planIndex].colNoMap.getElements(m_pool->colNoMapPool, 0);
			EVAL_INDEX_SCANINFOTABLEELEMENTS[accessElement->indexInfo.startIndex].nCols    = EVAL_EVALBUFFER[planIndex].nCols;
			EVAL_INDEX_SCANINFOTABLEELEMENTS[accessElement->indexInfo.startIndex].clist    = EVAL_EVALBUFFER[planIndex].getColSlotPtr(0);
			EVAL_INDEX_SCANINFOTABLEELEMENTS[accessElement->indexInfo.startIndex].oid      = EVAL_EVALBUFFER[planIndex].getOID_Ptr();
			if(indexInfoNode->scan.boolExprs.size)
			{
				EVAL_INDEX_SCANINFOTABLE.prepareBoolExpression(accessElement->indexInfo.startIndex, indexInfoNode->scan.boolExprs.size);
				e = makeBoolExpression(indexInfoNode->scan.boolExprs, EVAL_INDEX_SCANINFOTABLEELEMENTS[accessElement->indexInfo.startIndex].boolExprs);
				OOSQL_CHECK_ERR(e);

				EVAL_INDEX_SCANINFOTABLE[accessElement->indexInfo.startIndex].boolExprInfos = &indexInfoNode->scan.boolExprs.getElements(m_pool->ap_boolExprPool, 0);
			}
			else
			{
				EVAL_INDEX_SCANINFOTABLE.resetBoolExpression(accessElement->indexInfo.startIndex);
				OOSQL_CHECK_ERR(e);

				EVAL_INDEX_SCANINFOTABLE[accessElement->indexInfo.startIndex].boolExprInfos = NULL;
			}

            break;

        default:        /* invalid node kind */
            OOSQL_ERR(eINVALID_CASE_OOSQL);
    }

    /* return */
    return(eNOERROR);
}

#ifndef SLIMDOWN_TEXTIR
Four    OOSQL_Evaluator::openTextIndexScan(
    Four planIndex,             /* IN: */
    Four accessElemIndex        /* IN: */
)
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
    OOSQL_AccessElement		*accessElement;
	AP_IndexInfoElement		*indexInfoNode;
    Four					e;

    /* check input parameters */
#ifdef  OOSQL_DEBUG
    if (planIndex < 0 || ACCESSPLAN.getNumAP_Elem() <= planIndex)
        OOSQL_ERR(eBADPARAMETER_OOSQL);
    if (accessElemIndex < 0 || EVAL_ACCESSLISTTABLE[planIndex].numClasses <= accessElemIndex)
        OOSQL_ERR(eBADPARAMETER_OOSQL);
#endif

    /* get pointer to the access element */
    accessElement = &(EVAL_ACCESSLISTTABLE[planIndex].accessList[accessElemIndex]);

	/* get index information for the current index info. tree node */
	indexInfoNode = ACCESSPLAN.getIndexInfoElem(accessElement->indexInfo);

    /* prepare to process text index condition */
    e = initTextIndexCond(accessElement->ocn, accessElement->indexInfo, planIndex);
    if (e < eNOERROR) OOSQL_ERR(e);

    return(eNOERROR);
}

Four    OOSQL_Evaluator::initTextIndexCond(
    Four ocn,                           /* IN: */
    AP_IndexInfoPoolIndex indexInfo,    /* IN: */
	Four                  planIndex		/* IN: */
)
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
    AP_IndexInfoElement *indexInfoNode;
    Four e;

    /* check input parameters */
#ifdef  OOSQL_DEBUG
    if (ocn < 0)
        OOSQL_ERR(eBADPARAMETER_OOSQL);
    if (IS_NULL_POOLINDEX(indexInfo))
        OOSQL_ERR(eBADPARAMETER_OOSQL);
#endif

    /* get index information for the current index info. tree node */
    indexInfoNode = ACCESSPLAN.getIndexInfoElem(indexInfo);

	EVAL_INDEX_SCANINFOTABLEELEMENTS[indexInfo.startIndex].scanId        = EVAL_ACCESSLISTTABLE[planIndex].getCurrScanID();
	EVAL_INDEX_SCANINFOTABLEELEMENTS[indexInfo.startIndex].ocn           = EVAL_ACCESSLISTTABLE[planIndex].getCurrOcn();
	EVAL_INDEX_SCANINFOTABLEELEMENTS[indexInfo.startIndex].colNoMap      = &ACCESSPLANELEMENTS[planIndex].colNoMap.getElements(m_pool->colNoMapPool, 0);
	EVAL_INDEX_SCANINFOTABLEELEMENTS[indexInfo.startIndex].nCols         = EVAL_EVALBUFFER[planIndex].nCols;
	EVAL_INDEX_SCANINFOTABLEELEMENTS[indexInfo.startIndex].clist         = EVAL_EVALBUFFER[planIndex].getColSlotPtr(0);
	EVAL_INDEX_SCANINFOTABLEELEMENTS[indexInfo.startIndex].oid           = EVAL_EVALBUFFER[planIndex].getOID_Ptr();
	EVAL_INDEX_SCANINFOTABLEELEMENTS[indexInfo.startIndex].indexNode     = ACCESSPLAN.getIndexInfoElem(indexInfo.getPoolIndex());

    switch (indexInfoNode->nodeKind) 
	{ 
    case INDEXINFO_OPERATOR:
        /* initialize index scan on the first operand */
        e = initTextIndexCond( ocn, indexInfoNode->oper.op1, planIndex );
        if (e < eNOERROR)
            OOSQL_ERR(e);

        /* initialize index scan on the second operand */
        e = initTextIndexCond( ocn, indexInfoNode->oper.op2, planIndex );
        if (e < eNOERROR) 
            OOSQL_ERR(e);

        break;

    case INDEXINFO_SCAN:
        /* prepare to evaluate Text IR query using Text index scan
         * for each keyword of the Text IR query formula
         */
        e = initTextIR_IndexScan( ocn, indexInfo );
        if (e < eNOERROR) OOSQL_ERR(e);

		EVAL_INDEX_SCANINFOTABLEELEMENTS[indexInfo.startIndex].fnMatchResult = EVAL_EVALBUFFER[planIndex].getFnMatchSlotPtr(indexInfoNode->scan.text.matchFuncNum);
		EVAL_INDEX_SCANINFOTABLEELEMENTS[indexInfo.startIndex].fnMatchResult->start      = 0;
		EVAL_INDEX_SCANINFOTABLEELEMENTS[indexInfo.startIndex].fnMatchResult->length     = TEXTIR_SIZE_WEIGHT;
		EVAL_INDEX_SCANINFOTABLEELEMENTS[indexInfo.startIndex].fnMatchResult->nullFlag   = SM_FALSE;
		EVAL_INDEX_SCANINFOTABLEELEMENTS[indexInfo.startIndex].fnMatchResult->dataLength = TEXTIR_SIZE_WEIGHT;
		EVAL_INDEX_SCANINFOTABLEELEMENTS[indexInfo.startIndex].fnMatchResult->retLength  = TEXTIR_SIZE_WEIGHT;

		if(indexInfoNode->scan.boolExprs.size)
		{
			EVAL_INDEX_SCANINFOTABLE.prepareBoolExpression(indexInfo.startIndex, indexInfoNode->scan.boolExprs.size);
			e = makeBoolExpression(indexInfoNode->scan.boolExprs, EVAL_INDEX_SCANINFOTABLEELEMENTS[indexInfo.startIndex].boolExprs);
			OOSQL_CHECK_ERR(e);

			EVAL_INDEX_SCANINFOTABLE[indexInfo.startIndex].boolExprInfos = &indexInfoNode->scan.boolExprs.getElements(m_pool->ap_boolExprPool, 0);
		}
		else
		{
			e = EVAL_INDEX_SCANINFOTABLE.resetBoolExpression(indexInfo.startIndex);
			OOSQL_CHECK_ERR(e);

			EVAL_INDEX_SCANINFOTABLE[indexInfo.startIndex].boolExprInfos = NULL;
		}

		break;

    default:        /* invalid node kind */
        OOSQL_ERR(eINVALID_CASE_OOSQL);
    }

    /* return */
    return(eNOERROR);
}
#else /* SLIMDOWN_TEXTIR */
Four    OOSQL_Evaluator::openTextIndexScan(
    Four planIndex,             /* IN: */
    Four accessElemIndex        /* IN: */
)
{
    return eTEXTIR_NOTENABLED_OOSQL;
}

Four    OOSQL_Evaluator::initTextIndexCond(
    Four ocn,                           /* IN: */
    AP_IndexInfoPoolIndex indexInfo,    /* IN: */
    Four                  planIndex     /* IN: */
)
{
    return eTEXTIR_NOTENABLED_OOSQL;
}
#endif /* SLIMDOWN_TEXTIR */


Four    OOSQL_Evaluator::openSeqScan(
    Four planIndex,             /* IN: */
    Four accessElemIndex        /* IN: */
)
/*
    Function:

    Side effect:

    Referenced member variabes:

    Return value:
*/
{
    OOSQL_AccessElement*				accessElement;
    Four								scanId;
    OOSQL_StorageManager::LockParameter lockup;
	Four								e;

    /* check input parameter */
#ifdef  OOSQL_DEBUG
    if (planIndex < 0 || ACCESSPLAN.getNumAP_Elem() <= planIndex)
        OOSQL_ERR(eBADPARAMETER_OOSQL);
    if (accessElemIndex < 0 || EVAL_ACCESSLISTTABLE[planIndex].numClasses <= accessElemIndex)
        OOSQL_ERR(eBADPARAMETER_OOSQL);
#endif

    /* get the pointer to the access element */
    accessElement = &(EVAL_ACCESSLISTTABLE[planIndex].accessList[accessElemIndex]);

    /* make lockup parameter and open scan */
    switch (ACCESSPLANELEMENTS[planIndex].classInfo.classKind) {
        case CLASSKIND_PERSISTENT:
            /* set shared mode lock if persistent class and read query, otherwise use exclusive lock */
			if(isUpdateQuery())
				lockup.mode = OOSQL_StorageManager::L_IX;
			else
				lockup.mode = OOSQL_StorageManager::L_IS;
            lockup.duration = OOSQL_StorageManager::L_COMMIT;

            /* NOTE: do not construct boolean exp. */
            scanId = m_storageManager->OpenSeqScan( accessElement->ocn, accessElement->scanDirection, 0, NULL, &lockup );
            if (scanId < eNOERROR)
                OOSQL_ERR(scanId);
            break;

        case CLASSKIND_TEMPORARY:
            /* set exclusive mode lock if temporary class */
            lockup.mode = OOSQL_StorageManager::L_X;
            lockup.duration = OOSQL_StorageManager::L_COMMIT;

            scanId = m_storageManager->OpenSeqScan( accessElement->ocn, accessElement->scanDirection, 0, NULL, &lockup );
            if (scanId < eNOERROR)
                OOSQL_ERR(scanId);
            break;

		case CLASSKIND_SORTSTREAM:
			scanId = NIL;
			e = EVAL_TEMPFILEINFOTABLEELEMENTS[ACCESSPLANELEMENTS[planIndex].classInfo.tempFileNum]->sortStream->OpenScan();
			OOSQL_CHECK_ERR(e);
			break;

        default:
            OOSQL_ERR(eINVALID_CASE_OOSQL);
    }

    /* set scan ID */
    accessElement->osn = scanId;

    /* return */
    return(eNOERROR);
}


Four    OOSQL_Evaluator::openTempSeqScan( 
        Four    planIndex,                      // IN:
        Four    accessListIndex         // IN:
)
/*
    Function:

    Side effect:

    Return value:
*/
{
        Four    scanId;

        /* check input parameters */
#ifdef  OOSQL_DEBUG
        if (planIndex < 0 || ACCESSPLAN.getNumAP_Elem() <= planIndex) 
                OOSQL_ERR(eBADPARAMETER_OOSQL);
#endif

        scanId = EVAL_TEMPFILEINFOTABLEELEMENTS[ ACCESSPLANELEMENTS[planIndex].classInfo.tempFileNum ]->osn;

        EVAL_ACCESSLISTTABLE[planIndex].accessList[accessListIndex].osn = scanId;

        /* return */
        return eNOERROR;
}

Four    OOSQL_Evaluator::closeSeqScan(
    Four planIndex,
    Four accessElemIndex
)
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
    Four scanId;
    Four e;

    /* check input parameters */
#ifdef  OOSQL_DEBUG
    if (planIndex < 0 || ACCESSPLAN.getNumAP_Elem() <= planIndex) 
        OOSQL_ERR(eBADPARAMETER_OOSQL);
    if (accessElemIndex < 0 || EVAL_ACCESSLISTTABLE[planIndex].numClasses <= accessElemIndex)
        OOSQL_ERR(eBADPARAMETER_OOSQL);
#endif

    /* get scan ID to close */
    scanId = EVAL_ACCESSLISTTABLE[planIndex].accessList[accessElemIndex].osn;
    if (scanId < 0) return(eNOERROR);	

    switch ( ACCESSPLANELEMENTS[planIndex].classInfo.classKind ) {
        case CLASSKIND_PERSISTENT:
            e = m_storageManager->CloseScan(scanId);
            if (e < eNOERROR) 
                OOSQL_ERR(e);
            break;

        case CLASSKIND_TEMPORARY:
            e = m_storageManager->CloseScan(scanId);
            OOSQL_CHECK_ERR(e);
            break;

		case CLASSKIND_SORTSTREAM:
			e = EVAL_TEMPFILEINFOTABLEELEMENTS[ACCESSPLANELEMENTS[planIndex].classInfo.tempFileNum]->sortStream->CloseScan();
			OOSQL_CHECK_ERR(e);
			break;

        default:
            OOSQL_ERR(eINVALID_CASE_OOSQL);
    }

    /* reset scan ID */
    EVAL_ACCESSLISTTABLE[planIndex].accessList[accessElemIndex].osn = NIL;

    /* return */
    return(eNOERROR);
}


Four    OOSQL_Evaluator::closeBtreeIndexScan(
    Four planIndex,             /* IN: */
    Four accessElemIndex        /* IN: */
)
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
    OOSQL_AccessElement *accessElement;
    Four scanId;
    Four e;

    /* check input parameters */
#ifdef  OOSQL_DEBUG
    if (planIndex < 0 || ACCESSPLAN.getNumAP_Elem() <= planIndex)
        OOSQL_ERR(eBADPARAMETER_OOSQL);
    if (accessElemIndex < 0 || EVAL_ACCESSLISTTABLE[planIndex].numClasses <= accessElemIndex)
        OOSQL_ERR(eBADPARAMETER_OOSQL); 
#endif

    accessElement = &(EVAL_ACCESSLISTTABLE[planIndex].accessList[accessElemIndex]);

    /* get scan ID to close */
    scanId = EVAL_INDEX_SCANINFOTABLEELEMENTS[accessElement->indexInfo.startIndex].scanId;
	if (scanId < 0) return(eNOERROR);	

    /* close scan */
    e = m_storageManager->CloseScan(scanId);
    if (e < eNOERROR) 
        OOSQL_ERR(e);

    /* reset scan ID */
    EVAL_INDEX_SCANINFOTABLEELEMENTS[accessElement->indexInfo.startIndex].scanId = NIL;

    /* return */
    return(eNOERROR);
}


#ifndef SLIMDOWN_TEXTIR
Four    OOSQL_Evaluator::closeTextIndexScan(
    Four planIndex,             /* IN: */
    Four accessElemIndex        /* IN: */
)
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
    OOSQL_AccessElement *accessElement;
    AP_IndexInfoElement *indexInfoNode;
    Four e;

    /* check input parameters */
#ifdef  OOSQL_DEBUG
    if (planIndex < 0 || ACCESSPLAN.getNumAP_Elem() <= planIndex)
        OOSQL_ERR(eBADPARAMETER_OOSQL);
    if (accessElemIndex < 0 || EVAL_ACCESSLISTTABLE[planIndex].numClasses <= accessElemIndex)
        OOSQL_ERR(eBADPARAMETER_OOSQL);
#endif

    accessElement = &(EVAL_ACCESSLISTTABLE[planIndex].accessList[accessElemIndex]);

    /* close scans for text index condition */
    e = finalTextIndexCond(accessElement->indexInfo);
    if (e < eNOERROR)
        OOSQL_ERR(e);

    return(eNOERROR);
}
#else /* SLIMDOWN_TEXTIR */

Four    OOSQL_Evaluator::closeTextIndexScan(
    Four planIndex,             /* IN: */
    Four accessElemIndex        /* IN: */
)
{
    return eTEXTIR_NOTENABLED_OOSQL;
}
#endif /* SLIMDOWN_TEXTIR */



Four    OOSQL_Evaluator::openMLGF_IndexScan(Four planIndex, Four accessElemIndex)
{
    OOSQL_AccessElement*				accessElement;
    AP_IndexInfoElement*				indexInfoNode;
	OOSQL_StorageManager::LockParameter	lockup;
    Four								scanId;
    Four								e;
	Four								i;

    /* check input parameters */
#ifdef	OOSQL_DEBUG
    if(planIndex < 0 || ACCESSPLAN.getNumAP_Elem() <= planIndex)
    	OOSQL_ERR(eBADPARAMETER_OOSQL);
    if(accessElemIndex < 0 || EVAL_ACCESSLISTTABLE[planIndex].numClasses <= accessElemIndex)
    	OOSQL_ERR(eBADPARAMETER_OOSQL);
#endif

    /* get pointer to the access element */
    accessElement = &(EVAL_ACCESSLISTTABLE[planIndex].accessList[accessElemIndex]);

    /* get index information for the current index info. tree node */
    indexInfoNode = ACCESSPLAN.getIndexInfoElem(accessElement->indexInfo);

    switch (indexInfoNode->nodeKind) 
	{ 
	case INDEXINFO_OPERATOR:
        OOSQL_ERR(eNOTIMPLEMENTED_MLGFINDEXSCAN_OOSQL);
		break;
        
    case INDEXINFO_SCAN:
        /* make lockup parameter and open scan */
        switch (ACCESSPLANELEMENTS[planIndex].classInfo.classKind) 
		{
        case CLASSKIND_PERSISTENT:
            /* set shared mode lock if persistent class and read query, otherwise use exclusive lock */
			if(isUpdateQuery())
				lockup.mode     = OOSQL_StorageManager::L_IX;
			else
				lockup.mode     = OOSQL_StorageManager::L_IS;
            lockup.duration = OOSQL_StorageManager::L_COMMIT;
            break;
            
        case CLASSKIND_TEMPORARY:
            /* set exclusive mode lock if temporary class */
            lockup.mode     = OOSQL_StorageManager::L_X;
            lockup.duration = OOSQL_StorageManager::L_COMMIT;
            break;
            
        default:
            OOSQL_ERR(eINVALID_CASE_OOSQL);
        }

		e = makeMlgfIndexBoundCond(indexInfoNode->scan.mlgf);
		OOSQL_CHECK_ERR(e);

        scanId = m_storageManager->OpenMlgfIndexScan(accessElement->ocn, 
													&(indexInfoNode->scan.indexId), 
													indexInfoNode->scan.mlgf.lowerBound, 
													indexInfoNode->scan.mlgf.upperBound,
													0, NULL, &lockup);
        if (scanId < eNOERROR) OOSQL_ERR(scanId);

        /* set scan Id in the entry of MLGF index scan Id table */
        EVAL_INDEX_SCANINFOTABLEELEMENTS[accessElement->indexInfo.startIndex].scanId   = scanId;
		EVAL_INDEX_SCANINFOTABLEELEMENTS[accessElement->indexInfo.startIndex].ocn      = accessElement->ocn;
		EVAL_INDEX_SCANINFOTABLEELEMENTS[accessElement->indexInfo.startIndex].colNoMap = &ACCESSPLANELEMENTS[planIndex].colNoMap.getElements(m_pool->colNoMapPool, 0);
		EVAL_INDEX_SCANINFOTABLEELEMENTS[accessElement->indexInfo.startIndex].nCols    = EVAL_EVALBUFFER[planIndex].nCols;
		EVAL_INDEX_SCANINFOTABLEELEMENTS[accessElement->indexInfo.startIndex].clist    = EVAL_EVALBUFFER[planIndex].getColSlotPtr(0);
		EVAL_INDEX_SCANINFOTABLEELEMENTS[accessElement->indexInfo.startIndex].oid      = EVAL_EVALBUFFER[planIndex].getOID_Ptr();
		
		if(indexInfoNode->scan.boolExprs.size)
		{
			EVAL_INDEX_SCANINFOTABLE.prepareBoolExpression(accessElement->indexInfo.startIndex, indexInfoNode->scan.boolExprs.size);
			e = makeBoolExpression(indexInfoNode->scan.boolExprs, EVAL_INDEX_SCANINFOTABLEELEMENTS[accessElement->indexInfo.startIndex].boolExprs);
			OOSQL_CHECK_ERR(e);

			EVAL_INDEX_SCANINFOTABLE[accessElement->indexInfo.startIndex].boolExprInfos = &indexInfoNode->scan.boolExprs.getElements(m_pool->ap_boolExprPool, 0);
		}
		else
		{
			e = EVAL_INDEX_SCANINFOTABLE.resetBoolExpression(accessElement->indexInfo.startIndex);
			OOSQL_CHECK_ERR(e);

			EVAL_INDEX_SCANINFOTABLE[accessElement->indexInfo.startIndex].boolExprInfos = NULL;
		}
		break;
    
	default:	/* invalid node kind */
        OOSQL_ERR(eINVALID_CASE_OOSQL);
    }
    
	return(eNOERROR);
}

Four    OOSQL_Evaluator::openMLGF_MBR_IndexScan(
    Four planIndex,                     /* IN: */
    Four accessElemIndex                /* IN: */
)
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
    OOSQL_AccessElement*				accessElement;
    AP_IndexInfoElement*				indexInfoNode;
	OOSQL_StorageManager::LockParameter	lockup;
    OOSQL_StorageManager::Region		region;
    Four								spatialOp;
    Four								scanId;
    Four								e;

    /* check input parameters */
#ifdef	OOSQL_DEBUG
    if(planIndex < 0 || ACCESSPLAN.getNumAP_Elem() <= planIndex)
    	OOSQL_ERR(eBADPARAMETER_OOSQL);
    if(accessElemIndex < 0 || EVAL_ACCESSLISTTABLE[planIndex].numClasses <= accessElemIndex)
    	OOSQL_ERR(eBADPARAMETER_OOSQL);
#endif

    /* get pointer to the access element */
    accessElement = &(EVAL_ACCESSLISTTABLE[planIndex].accessList[accessElemIndex]);

    /* get index information for the current index info. tree node */
    indexInfoNode = ACCESSPLAN.getIndexInfoElem(accessElement->indexInfo);

	AP_IndexInfoPoolIndex tidJoinIndexInfo = indexInfoNode->scan.mlgfmbr.tidJoinIndexInfo;
	AP_IndexInfoElement* indexInfo;
	OOSQL_StorageManager::BoundCond     startBound;
	OOSQL_StorageManager::BoundCond     stopBound;

    switch (indexInfoNode->nodeKind) 
	{ 
	case INDEXINFO_OPERATOR:
        OOSQL_ERR(eNOTIMPLEMENTED_MLGFINDEXSCAN_OOSQL);
        
    case INDEXINFO_SCAN:
        /* make lockup parameter and open scan */
        switch (ACCESSPLANELEMENTS[planIndex].classInfo.classKind) 
		{
        case CLASSKIND_PERSISTENT:
            /* set shared mode lock if persistent class and read query, otherwise use exclusive lock */
			if(isUpdateQuery())
				lockup.mode     = OOSQL_StorageManager::L_IX;
			else
				lockup.mode     = OOSQL_StorageManager::L_IS;
            lockup.duration = OOSQL_StorageManager::L_COMMIT;
            break;
            
        case CLASSKIND_TEMPORARY:
            /* set exclusive mode lock if temporary class */
            lockup.mode     = OOSQL_StorageManager::L_X;
            lockup.duration = OOSQL_StorageManager::L_COMMIT;
            break;
            
        default:
            OOSQL_ERR(eINVALID_CASE_OOSQL);
        }
        if(indexInfoNode->scan.mlgfmbr.operandType == AP_SPATIAL_OPERAND_TYPE_MBR)
        {
            region.x1 = ACCESSPLAN.getMBRValue(indexInfoNode->scan.mlgfmbr.mbr, 0);
            region.y1 = ACCESSPLAN.getMBRValue(indexInfoNode->scan.mlgfmbr.mbr, 1);
            region.x2 = ACCESSPLAN.getMBRValue(indexInfoNode->scan.mlgfmbr.mbr, 2);
            region.y2 = ACCESSPLAN.getMBRValue(indexInfoNode->scan.mlgfmbr.mbr, 3);
        }
        else if(indexInfoNode->scan.mlgfmbr.operandType == AP_SPATIAL_OPERAND_TYPE_PATHEXPR)
        {
            // get oid from planNo
            // indexInfoNode->scan.mlgf.pathExpr.planNo;
            // fetch mbr of the oid
            OOSQL_StorageManager::OID*      oid;
            Four                            ocn;
            Four                            planNo;
            Four                            colNo;

            planNo = indexInfoNode->scan.mlgfmbr.pathExpr.col.planNo;
            colNo  = indexInfoNode->scan.mlgfmbr.pathExpr.col.colNo;

            oid = EVAL_EVALBUFFER[planNo].getOID_Ptr();
            ocn = EVAL_ACCESSLISTTABLE[planNo].getCurrOcn();

            e = m_storageManager->Geometry_GetMBR(ocn, SM_FALSE, oid, colNo,
                                                  &region.x1, &region.y1, &region.x2, &region.y2);
            if(e < eNOERROR) OOSQL_ERR(e);
        }

        else if(indexInfoNode->scan.mlgfmbr.operandType == AP_SPATIAL_OPERAND_TYPE_OPER)
        {
            OOSQL_DB_Value              result(pMemoryManager);

            e = evalOperator(&indexInfoNode->scan.mlgfmbr.oper, &result);
            if(e < eNOERROR) OOSQL_ERR(e);

            e = m_storageManager->Geometry_GetMBR((char*)result.data.ptr, result.length,
                                                  &region.x1, &region.y1, &region.x2, &region.y2);
            if(e < eNOERROR) OOSQL_ERR(e);
        }
        else
            OOSQL_ERR(eINVALID_CASE_OOSQL);
            
        
        switch(indexInfoNode->scan.mlgfmbr.spatialOp)
        {
        case OP_GEO_NORTH:
            spatialOp = GEO_SPATIAL_SOUTH;
            break;
        case OP_GEO_SOUTH:
            spatialOp = GEO_SPATIAL_NORTH;
            break;
        case OP_GEO_EAST:
            spatialOp = GEO_SPATIAL_WEST;
            break;
        case OP_GEO_WEST:
            spatialOp = GEO_SPATIAL_EAST;
            break;
        case OP_GEO_CONTAIN:
            spatialOp = GEO_SPATIAL_CONTAINED;
            break;
        case OP_GEO_CONTAINED:
            spatialOp = GEO_SPATIAL_CONTAIN;
            break;
        case OP_GEO_COVER:
            spatialOp = GEO_SPATIAL_CONTAINED | GEO_SPATIAL_INTERSECT;
            break;
        case OP_GEO_COVERED:
            spatialOp = GEO_SPATIAL_CONTAIN | GEO_SPATIAL_INTERSECT;
            break;
        case OP_GEO_DISJOINT:
            spatialOp = GEO_SPATIAL_DISJOINT;
            break;
        case OP_GEO_OVERLAP:
            spatialOp = GEO_SPATIAL_CONTAIN |
                        GEO_SPATIAL_CONTAINED |
                        GEO_SPATIAL_INTERSECT;
            break;
        case OP_GEO_EQUAL:
            spatialOp = GEO_SPATIAL_EQUAL;
            break;
        case OP_GEO_MEET:
            spatialOp = GEO_SPATIAL_CONTAIN |
                        GEO_SPATIAL_CONTAINED |
                        GEO_SPATIAL_INTERSECT;
            break;
		case OP_OGIS_KNN:
			spatialOp = GEO_SPATIAL_KNN;
			break;
        }
        
		if(spatialOp == GEO_SPATIAL_KNN && !tidJoinIndexInfo.isNull())
		{
			indexInfo = ACCESSPLAN.getIndexInfoElem(tidJoinIndexInfo);
			e = makeBtreeIndexBoundCond( &(indexInfo->scan.btree), &startBound, &stopBound );
			OOSQL_CHECK_ERR(e);
			scanId = m_storageManager->OpenMBRqueryScan(accessElement->ocn, &(indexInfoNode->scan.indexId),
                 	 region, spatialOp, 0, NULL, &lockup, &(indexInfo->scan.indexId), &startBound, &stopBound);
		}
		else
		{
        scanId = m_storageManager->OpenMBRqueryScan(accessElement->ocn, &(indexInfoNode->scan.indexId),
                                                    region, spatialOp, 0, NULL, &lockup);
		}										   

        if(scanId < eNOERROR) OOSQL_ERR(scanId);
        
        /* set scan Id in the entry of mlgf index scan Id table */
        EVAL_INDEX_SCANINFOTABLEELEMENTS[accessElement->indexInfo.startIndex].scanId   = scanId;
		EVAL_INDEX_SCANINFOTABLEELEMENTS[accessElement->indexInfo.startIndex].ocn      = accessElement->ocn;
		EVAL_INDEX_SCANINFOTABLEELEMENTS[accessElement->indexInfo.startIndex].colNoMap = &ACCESSPLANELEMENTS[planIndex].colNoMap.getElements(m_pool->colNoMapPool, 0);
		EVAL_INDEX_SCANINFOTABLEELEMENTS[accessElement->indexInfo.startIndex].nCols    = EVAL_EVALBUFFER[planIndex].nCols;
		EVAL_INDEX_SCANINFOTABLEELEMENTS[accessElement->indexInfo.startIndex].clist    = EVAL_EVALBUFFER[planIndex].getColSlotPtr(0);
		EVAL_INDEX_SCANINFOTABLEELEMENTS[accessElement->indexInfo.startIndex].oid      = EVAL_EVALBUFFER[planIndex].getOID_Ptr();

       	if(indexInfoNode->scan.boolExprs.size)
		{
			EVAL_INDEX_SCANINFOTABLE.prepareBoolExpression(accessElement->indexInfo.startIndex, indexInfoNode->scan.boolExprs.size);
			e = makeBoolExpression(indexInfoNode->scan.boolExprs, EVAL_INDEX_SCANINFOTABLEELEMENTS[accessElement->indexInfo.startIndex].boolExprs);
			OOSQL_CHECK_ERR(e);

			EVAL_INDEX_SCANINFOTABLE[accessElement->indexInfo.startIndex].boolExprInfos = &indexInfoNode->scan.boolExprs.getElements(m_pool->ap_boolExprPool, 0);
		}
		else
		{
			EVAL_INDEX_SCANINFOTABLE.resetBoolExpression(accessElement->indexInfo.startIndex);
			EVAL_INDEX_SCANINFOTABLE[accessElement->indexInfo.startIndex].boolExprInfos = NULL;
		}

        break;
        
    default:	/* invalid node kind */
        OOSQL_ERR(eINVALID_CASE_OOSQL);
    }
    
    /* return */
    return eNOERROR;

}


Four    OOSQL_Evaluator::closeMLGF_IndexScan(
    Four planIndex,                     /* IN: */
    Four accessElemIndex                /* IN: */
)
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
    OOSQL_AccessElement*	accessElement;
    Four					scanId;
    Four					e;

    /* check input parameters */
#ifdef	OOSQL_DEBUG
    if (planIndex < 0 || ACCESSPLAN.getNumAP_Elem() <= planIndex)
		OOSQL_ERR(eBADPARAMETER_OOSQL);
    if (accessElemIndex < 0 || EVAL_ACCESSLISTTABLE[planIndex].numClasses <= accessElemIndex)
    	OOSQL_ERR(eBADPARAMETER_OOSQL);
#endif

    accessElement = &(EVAL_ACCESSLISTTABLE[planIndex].accessList[accessElemIndex]);

    /* get scan ID to close */
    scanId = EVAL_INDEX_SCANINFOTABLEELEMENTS[accessElement->indexInfo.startIndex].scanId;
    if (scanId < 0) return(eNOERROR);	

    /* close scan */
    e = m_storageManager->CloseScan(scanId);
    if (e < eNOERROR) OOSQL_ERR(e);

    /* reset scan ID */
    EVAL_INDEX_SCANINFOTABLEELEMENTS[accessElement->indexInfo.startIndex].scanId = NIL;

    /* return */
    return eNOERROR;
}


#ifndef SLIMDOWN_TEXTIR
Four    OOSQL_Evaluator::finalTextIndexCond(
    AP_IndexInfoPoolIndex indexInfo     /* IN: */
)
/*
    Function:
        Close scans traversing Text index condition tree.

    Side effect:

    Referenced member variables:

    Return value:
*/
{
    AP_IndexInfoElement *indexInfoNode;
    Four e;

    /* check input parameters */
#ifdef  OOSQL_DEBUG
    if (IS_NULL_POOLINDEX(indexInfo))
        OOSQL_ERR(eBADPARAMETER_OOSQL);
#endif

    /* get index information for the current index info. tree node */
    indexInfoNode = ACCESSPLAN.getIndexInfoElem(indexInfo);

    switch (indexInfoNode->nodeKind) { 
        case INDEXINFO_OPERATOR:
            /* close index scan on the first operand */
            e = finalTextIndexCond( indexInfoNode->oper.op1 );
            if (e < eNOERROR) 
                OOSQL_ERR(e);

            /* close index scan on the second operand */
            e = finalTextIndexCond( indexInfoNode->oper.op2 );
            if (e < eNOERROR) 
                OOSQL_ERR(e);
            break;

        case INDEXINFO_SCAN:
            e = closeTextIR_IndexScan( indexInfoNode->scan.text.textIndexCond );
            if (e < eNOERROR) 
                OOSQL_ERR(e);
            break;

        default:        /* invalid node kind */
            OOSQL_ERR(eINVALID_CASE_OOSQL);
    }

    /* return */
    return(eNOERROR);
}

#else /* SLIMDOWN_TEXTIR */

Four    OOSQL_Evaluator::finalTextIndexCond(
    AP_IndexInfoPoolIndex indexInfo     /* IN: */
)
{
    return eTEXTIR_NOTENABLED_OOSQL;
}
#endif /* SLIMDOWN_TEXTIR */


Four OOSQL_Evaluator::makeMlgfIndexBoundCond
(
	AP_MlgfIndexCond&	mlgf
)
{
	Four				e, i;

	Four				lowerBoundExprStartIndex;
	Four				upperBoundExprStartIndex;

	AP_ExprPoolIndex	lowerBoundExpr;
	AP_ExprPoolIndex	upperBoundExpr;
	
	lowerBoundExprStartIndex = mlgf.lowerBoundExpr.startIndex;
	upperBoundExprStartIndex = mlgf.upperBoundExpr.startIndex;

	OOSQL_DB_Value		value(pMemoryManager);

	for(i = 0; i < mlgf.nKeys; i++)
	{
		if(mlgf.lowerBoundExprFlag[i] == SM_TRUE)
		{
			lowerBoundExpr.setPoolIndex(lowerBoundExprStartIndex + i, 1);

			e = evalExpression(&lowerBoundExpr, &value);
			OOSQL_CHECK_ERR(e);

			switch(value.type)
			{
			case OOSQL_TYPE_SHORT:
				mlgf.lowerBound[i] = (MLGF_HashValue)value.data.s;
				break;

			case OOSQL_TYPE_INT:
				mlgf.lowerBound[i] = (MLGF_HashValue)value.data.i;
				break;

			case OOSQL_TYPE_LONG:
				mlgf.lowerBound[i] = (MLGF_HashValue)value.data.l;
				break;

			case OOSQL_TYPE_LONG_LONG:
				mlgf.lowerBound[i] = (MLGF_HashValue)value.data.ll;
				break;

			default:
				OOSQL_ERR(eTYPE_ERROR_OOSQL);
			}
		}

		if(mlgf.upperBoundExprFlag[i] == SM_TRUE)
		{
			upperBoundExpr.setPoolIndex(upperBoundExprStartIndex + i, 1);

			e = evalExpression(&upperBoundExpr, &value);
			OOSQL_CHECK_ERR(e);
			
			switch(value.type)
			{
			case OOSQL_TYPE_SHORT:
				mlgf.upperBound[i] = (MLGF_HashValue)value.data.s;
				break;

			case OOSQL_TYPE_INT:
				mlgf.upperBound[i] = (MLGF_HashValue)value.data.i;
				break;

			case OOSQL_TYPE_LONG:
				mlgf.upperBound[i] = (MLGF_HashValue)value.data.l;
				break;

			case OOSQL_TYPE_LONG_LONG:
				mlgf.upperBound[i] = (MLGF_HashValue)value.data.ll;
				break;

			default:
				OOSQL_ERR(eTYPE_ERROR_OOSQL);
			}
		}
	}

	return(eNOERROR);
}

Four    OOSQL_Evaluator::makeBtreeIndexBoundCond(
        AP_BtreeIndexCond       *condInfo,      // IN:
        OOSQL_StorageManager::BoundCond       *startBound,            // OUT:
        OOSQL_StorageManager::BoundCond       *stopBound              // OUT:
)
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
    AP_ExprPoolElements keyValExpr(m_pool->ap_exprPool);
	AP_ExprPoolIndex	keyValExprIndex;
    OOSQL_DB_Value		value(pMemoryManager);
    Four				numKeyValues;
    Four				i;
    Four				e;

    /* check input parameters */
    if (condInfo == NULL)
            OOSQL_ERR(eBADPARAMETER_OOSQL);
    if (startBound == NULL)
            OOSQL_ERR(eBADPARAMETER_OOSQL);
    if (stopBound == NULL)
            OOSQL_ERR(eBADPARAMETER_OOSQL);

    /* 
     * make start bound condition 
     */
    startBound->op = condInfo->startBound.op;
    startBound->key.len = 0;

    numKeyValues = condInfo->startBound.key.size;
    keyValExpr   = condInfo->startBound.key;

    for (i = 0; i < numKeyValues; i++) 
	{
		keyValExprIndex.setPoolIndex(keyValExpr.startIndex + i, 1);
		if(keyValExpr[i].value.isNull())
		{
			value.type     = OOSQL_TYPE_STRING;
			value.length   = 0;
			value.data.ptr = (void*)" "; // dummy data pointer
		}
		else
		{
			e = evalExpression(&keyValExprIndex, &value);
			OOSQL_CHECK_ERR(e);
			e = value.convertType(keyValExpr[i].resultType);
			OOSQL_CHECK_ERR(e);
		}

        e = appendKeyValueToBoundCond(startBound, &value);
        if (e < eNOERROR) OOSQL_ERR(e);
    }

    /* 
     * make stop bound condition 
     */
    stopBound->op = condInfo->stopBound.op;
    stopBound->key.len = 0;

    numKeyValues = condInfo->stopBound.key.size;
	keyValExpr   = condInfo->stopBound.key;

    for (i = 0; i < numKeyValues; i++) 
	{
		keyValExprIndex.setPoolIndex(keyValExpr.startIndex + i, 1);
		if(keyValExpr[i].value.isNull())
		{
			value.type     = OOSQL_TYPE_STRING;
			value.length   = 0;
			value.data.ptr = (void*)" "; // dummy data pointer
		}
		else
		{
			e = evalExpression(&keyValExprIndex, &value);
			OOSQL_CHECK_ERR(e);
			e = value.convertType(keyValExpr[i].resultType);
			OOSQL_CHECK_ERR(e);
		}

		e = appendKeyValueToBoundCond(stopBound, &value);
		if (e < eNOERROR) OOSQL_ERR(e);
    }

    /* return */
    return eNOERROR;
}

Four OOSQL_Evaluator::makeBoolExpression(
	AP_BoolExprPoolIndex			boolExprIndex,		// IN  
	OOSQL_StorageManager::BoolExp*	boolExprs			// OUT 
)	
{
	AP_BoolExprPoolElements boolExprElements(m_pool->ap_boolExprPool);
    AP_ExprPoolElements		keyValExpr(m_pool->ap_exprPool);
	AP_ExprPoolIndex		keyValExprIndex;
    OOSQL_DB_Value			value(pMemoryManager);
    Four					numKeyValues;
    Four					i;
    Four					e;

	boolExprElements = boolExprIndex;
	for(i = 0; i < boolExprElements.size; i++)
	{
		keyValExprIndex = boolExprElements[i].key;

		e = evalExpression(&keyValExprIndex, &value);
		OOSQL_CHECK_ERR(e);

		boolExprs[i].colNo  = boolExprElements[i].colNo;
		boolExprs[i].op     = boolExprElements[i].op;
		boolExprs[i].length = value.length;

		e = value.convertType(boolExprElements[i].type);
		OOSQL_CHECK_ERR(e);

		switch(value.type)
		{
		case OOSQL_TYPE_SHORT:
            boolExprs[i].data.s = value.data.s;
            break;
    
		case OOSQL_TYPE_INT:
            boolExprs[i].data.i = value.data.i;
            break;
        
		case OOSQL_TYPE_LONG:
            boolExprs[i].data.l = value.data.l;
            break;
        
		case OOSQL_TYPE_FLOAT:
            boolExprs[i].data.f = value.data.f;
            break;
        
		case OOSQL_TYPE_DOUBLE:
            boolExprs[i].data.d = value.data.d;
            break;
        
		case OOSQL_TYPE_OID:
            boolExprs[i].data.oid = value.data.oid;
            break;

        case OOSQL_TYPE_STRING:
        case OOSQL_TYPE_VARSTRING:
            if(value.length > sizeof(boolExprs[i].data.str))
				memcpy(boolExprs[i].data.str, value.data.ptr, sizeof(boolExprs[i].data.str));
			else
				memcpy(boolExprs[i].data.str, value.data.ptr, value.length);
            break;
		
		case OOSQL_TYPE_MBR:
			boolExprs[i].data.mbr = value.data.mbr;
			break;

		case OOSQL_TYPE_DATE:
			boolExprs[i].data.date = value.data.date;
			break;

		case OOSQL_TYPE_TIME:
			boolExprs[i].data.time = value.data.time;
			break;

		case OOSQL_TYPE_TIMESTAMP:			
			boolExprs[i].data.timestamp = value.data.timestamp;
			break;
            	
		default:
			OOSQL_ERR(eTYPE_ERROR_OOSQL);
		}

	}

	return eNOERROR;
}

Four    OOSQL_Evaluator::oidFetch(
	Four planIndex,             /* IN: */
    Four accessElemIndex)       /* IN: */
/*
    Function:

    Side effect:

    Referenced member variables:
        m_currPlanIndex

    Return value:
*/
{
    Four mappedColNo;
    Four joinClassIndex;
    EVAL_EvalBufferSlot *joinAttr;
    OOSQL_StorageManager::OID *oid;
    Four ocn;
    Four nCols;
    EVAL_EvalBufferSlot *clist;
    Four e;

    /* check if we already fetched the object by OOSQL_StorageManager::OID fetching */
    if (EVAL_ACCESSLISTTABLE[m_currPlanIndex].isEndOfCurrAccess())
        return(ENDOFSCAN);
    
    /* 
     * get the value (OOSQL_StorageManager::OID) of the join attribute 
     */
    if(ACCESSPLANELEMENTS[m_currPlanIndex].isUseOid)		
    {
        /* get pointer to OOSQL_StorageManager::OID in evaluation buffer slot */
        oid = EVAL_EVALBUFFER[m_currPlanIndex].getOID_Ptr();
        
        *oid = ACCESSPLANELEMENTS[m_currPlanIndex].oid;
    }
    else if (ACCESSPLANELEMENTS[m_currPlanIndex].isMethod == SM_TRUE) {
        OOSQL_ERR(eNOTIMPLEMENTED_ABNORMALPATHEXPR_OOSQL);
    }
    else {      
        
        /*
         * get the value of implicit join column (which is OOSQL_StorageManager::OID)
         */
        /* get plan index to which implicit join is performed */
        joinClassIndex = ACCESSPLANELEMENTS[m_currPlanIndex].joinClass;
        
        /* get mapped column no. for the implicit join attribute */
        mappedColNo = ACCESSPLAN.getMappedColNo( joinClassIndex, ACCESSPLANELEMENTS[m_currPlanIndex].implicitJoinColNo );
        
        /* get pointer to evaluation buffer slot for implicit join attribute */
        joinAttr = EVAL_EVALBUFFER[ ACCESSPLANELEMENTS[m_currPlanIndex].joinClass ].getColSlotPtr(mappedColNo);
        
        /* get pointer to OOSQL_StorageManager::OID in evaluation buffer slot */
        oid = EVAL_EVALBUFFER[m_currPlanIndex].getOID_Ptr();
        
        /* get the value of the implicit join attribute (OOSQL_StorageManager::OID) */
        *oid = joinAttr->data.oid;
       	
		if(joinAttr->retLength != joinAttr->length)
			return(ENDOFSCAN);

        /* check if OOSQL_StorageManager::OID is null */
        if (OOSQL_StorageManager::TESTOIDCLEARPTR(*oid))
            return(ENDOFSCAN);
    }
    
    /*
     * fetch object by OOSQL_StorageManager::OID
     */
    
    /* determine scan ID for the object to read */
    ocn = searchOcnByClassId(m_currPlanIndex, oid->classID);
    if (ocn < eNOERROR) {
        if (ocn == eOCN_NOTFOUND_OOSQL) {
            /* in case of domain substitution */
            return(ENDOFSCAN);
        }
        else {
            OOSQL_ERR(ocn);
        }
    }
    
    /* set 'nCols' as the # of used-attributes of this access plan element */
	nCols = EVAL_EVALBUFFER[m_currPlanIndex].nCols;
    
    if(nCols > 0)
    {
        /* get pointer to evaluation buffer slot */
        clist = EVAL_EVALBUFFER[m_currPlanIndex].getColSlotPtr(0);
        
        e = m_storageManager->FetchObjectByColList(ocn, SM_FALSE, oid, nCols, clist );
        if (e < eNOERROR) { /* dangling reference */
            OOSQL_ERR(eDANGLINGREFERENCE_OOSQL);
        }
    }
    
    /* mark that nested-loop implicit join ended because the object is fetched,
     * so the next trial of fetching object will get end-of-scan code
     */
    EVAL_ACCESSLISTTABLE[m_currPlanIndex].endOfCurrAccess = SM_TRUE;
    
    /* return */
    return(eNOERROR);
}


Four    OOSQL_Evaluator::openOID_SetScan(
    Four planIndex,             /* IN: */
    Four accessElemIndex        /* IN: */
)
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
    return(eNOTIMPLEMENTED_OIDSETSCAN_OOSQL);
}


Four    OOSQL_Evaluator::oidSetScanNext(
	Four planIndex,             /* IN: */
    Four accessElemIndex)       /* IN: */
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
    return(eNOTIMPLEMENTED_OIDSETSCAN_OOSQL);
}


Four    OOSQL_Evaluator::closeOID_SetScan(
    Four planIndex,             /* IN: */
    Four accessElemIndex        /* IN: */
)
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
    return(eNOTIMPLEMENTED_OIDSETSCAN_OOSQL);
}

Four    OOSQL_Evaluator::searchScanIdByClassId(
        Four    planIndex,                      // IN: index to access plan element
        Four    classId                 // IN: class ID
)
/*
    Function:
        Search the open scan number for 'classId' that was previously opened
        for the 'planIndex'-th access plan element.

    Side effect:

    Return value:
        >= 0            open scan number
        < eNOERROR      error code
*/
{
        Four            i;
        OOSQL_AccessList        *pScanList;

        // check input parameter
        if (planIndex < 0 || ACCESSPLAN.getNumAP_Elem() < planIndex) {
            OOSQL_ERR( eBADPARAMETER_OOSQL);
        }

        pScanList = &EVAL_ACCESSLISTTABLE[planIndex];

        for (i = 0; i < pScanList->numClasses; i++) {
            if (pScanList->accessList[i].classId == classId) {
                return pScanList->accessList[i].osn;
            }
        }

        // return error code
        return eSCAN_NOTFOUND_OOSQL;
}

Four    OOSQL_Evaluator::searchOcnByClassId(
        Four    planIndex,                      // IN: index to access plan element
        Four    classId                 // IN: class ID
)
/*
    Function:
        Search the open scan number for 'classId' that was previously opened
        for the 'planIndex'-th access plan element.

    Side effect:

    Return value:
        >= 0            open scan number
        < eNOERROR      error code
*/
{
        Four            i;
        OOSQL_AccessList        *pScanList;

        // check input parameter
        if (planIndex < 0 || ACCESSPLAN.getNumAP_Elem() < planIndex) {
            OOSQL_ERR( eBADPARAMETER_OOSQL);
        }

        pScanList = &EVAL_ACCESSLISTTABLE[planIndex];

        for (i = 0; i < pScanList->numClasses; i++) {
            if (pScanList->accessList[i].classId == classId) {
                return pScanList->accessList[i].ocn;
            }
        }

        // return error code
        return eOCN_NOTFOUND_OOSQL;
}


static Four getEmbeddedAttrsVal(OOSQL_StorageManager* storageSystem, Four scanId, char* embeddedAttrPtr, Four embeddedAttrSize, Four nCols, OOSQL_StorageManager::ColListStruct* clist, OOSQL_StorageManager::EmbeddedAttrTranslationInfo* embeddedAttrTranslationInfo)
{
	return storageSystem->GetEmbeddedAttrsVal(scanId, embeddedAttrPtr, embeddedAttrSize, nCols, clist);
}

static Four getEmbeddedAttrsValForOneFixedSizeAttr(OOSQL_StorageManager* storageSystem, Four scanId, char* embeddedAttrPtr, Four embeddedAttrSize, Four nCols, OOSQL_StorageManager::ColListStruct* clist, OOSQL_StorageManager::EmbeddedAttrTranslationInfo* embeddedAttrTranslationInfo)
{
	Four embeddedColNo;
	
	embeddedColNo = embeddedAttrTranslationInfo->realColNoToEmbeddedColNo[clist[0].colNo];

	if(!BITTEST(embeddedAttrPtr, embeddedColNo))
	{
		clist[0].nullFlag  = SM_FALSE;
		clist[0].retLength = embeddedAttrTranslationInfo->embeddedAttrInfo[embeddedColNo].length;
		if(embeddedAttrTranslationInfo->embeddedAttrInfo[embeddedColNo].type != OOSQL_TYPE_STRING)
		{
			memcpy(&clist[0].data.s, 
				   embeddedAttrPtr + embeddedAttrTranslationInfo->embeddedAttributeOffset + embeddedAttrTranslationInfo->embeddedAttrInfo[embeddedColNo].start, 
				   clist[0].retLength);
		}
		else
		{
			memcpy(clist[0].data.ptr, 
				   embeddedAttrPtr + embeddedAttrTranslationInfo->embeddedAttributeOffset + embeddedAttrTranslationInfo->embeddedAttrInfo[embeddedColNo].start, 
				   clist[0].retLength);
		}
	}
	else
	{
		clist[0].nullFlag  = SM_TRUE;
		clist[0].retLength = NULL_LENGTH;
	}

	return eNOERROR;
}

static Four getEmbeddedAttrsValForOneVarSizeAttr(OOSQL_StorageManager* storageSystem, Four scanId, char* embeddedAttrPtr, Four embeddedAttrSize, Four nCols, OOSQL_StorageManager::ColListStruct* clist, OOSQL_StorageManager::EmbeddedAttrTranslationInfo* embeddedAttrTranslationInfo)
{
	Four embeddedColNo;
	Four start;
	Four length;
	Four nextOffset;

	embeddedColNo = embeddedAttrTranslationInfo->realColNoToEmbeddedColNo[clist[0].colNo];

	if(!BITTEST(embeddedAttrPtr, embeddedColNo))
	{
		start  = embeddedAttrTranslationInfo->embeddedAttrInfo[embeddedColNo].start;
		if(start == -1)
			memcpy(&start, embeddedAttrPtr + sizeof(Two) * embeddedAttrTranslationInfo->embeddedAttrVarColNo[embeddedColNo], sizeof(Two));
        if(embeddedColNo + 1 == embeddedAttrTranslationInfo->nEmbeddedAttributes)   
            nextOffset = embeddedAttrSize - embeddedAttrTranslationInfo->embeddedAttributeOffset;
        else
            memcpy(&nextOffset, embeddedAttrPtr + sizeof(Two) * embeddedAttrTranslationInfo->embeddedAttrVarColNo[embeddedColNo + 1], sizeof(Two));
		length = nextOffset - start;

		clist[0].nullFlag  = SM_FALSE;
		clist[0].retLength = length;
		memcpy(clist[0].data.ptr, 
			   embeddedAttrPtr + embeddedAttrTranslationInfo->embeddedAttributeOffset + start, 
			   length);
	}
	else
	{
		clist[0].nullFlag  = SM_TRUE;
		clist[0].retLength = NULL_LENGTH;
	}

	return eNOERROR;
}

static Four getEmbeddedAttrsValForTwoFixedSizeAttr(OOSQL_StorageManager* storageSystem, Four scanId, char* embeddedAttrPtr, Four embeddedAttrSize, Four nCols, OOSQL_StorageManager::ColListStruct* clist, OOSQL_StorageManager::EmbeddedAttrTranslationInfo* embeddedAttrTranslationInfo)
{
	Four embeddedColNo;
	
	embeddedColNo = embeddedAttrTranslationInfo->realColNoToEmbeddedColNo[clist[0].colNo];

	if(!BITTEST(embeddedAttrPtr, embeddedColNo))
	{
		clist[0].nullFlag  = SM_FALSE;
		clist[0].retLength = embeddedAttrTranslationInfo->embeddedAttrInfo[embeddedColNo].length;
		if(embeddedAttrTranslationInfo->embeddedAttrInfo[embeddedColNo].type != OOSQL_TYPE_STRING)
		{
			memcpy(&clist[0].data.s, 
				   embeddedAttrPtr + embeddedAttrTranslationInfo->embeddedAttributeOffset + embeddedAttrTranslationInfo->embeddedAttrInfo[embeddedColNo].start, 
				   clist[0].retLength);
		}
		else
		{
			memcpy(clist[0].data.ptr, 
				   embeddedAttrPtr + embeddedAttrTranslationInfo->embeddedAttributeOffset + embeddedAttrTranslationInfo->embeddedAttrInfo[embeddedColNo].start, 
				   clist[0].retLength);
		}
	}
	else
	{
		clist[0].nullFlag  = SM_TRUE;
		clist[0].retLength = NULL_LENGTH;
	}

	embeddedColNo = embeddedAttrTranslationInfo->realColNoToEmbeddedColNo[clist[1].colNo];

	if(!BITTEST(embeddedAttrPtr, embeddedColNo))
	{
		clist[1].nullFlag  = SM_FALSE;
		clist[1].retLength = embeddedAttrTranslationInfo->embeddedAttrInfo[embeddedColNo].length;
		if(embeddedAttrTranslationInfo->embeddedAttrInfo[embeddedColNo].type != OOSQL_TYPE_STRING)
		{
			memcpy(&clist[1].data.s, 
				   embeddedAttrPtr + embeddedAttrTranslationInfo->embeddedAttributeOffset + embeddedAttrTranslationInfo->embeddedAttrInfo[embeddedColNo].start, 
				   clist[1].retLength);
		}
		else
		{
			memcpy(clist[1].data.ptr, 
				   embeddedAttrPtr + embeddedAttrTranslationInfo->embeddedAttributeOffset + embeddedAttrTranslationInfo->embeddedAttrInfo[embeddedColNo].start, 
				   clist[1].retLength);
		}
	}
	else
	{
		clist[1].nullFlag  = SM_TRUE;
		clist[1].retLength = NULL_LENGTH;
	}

	return eNOERROR;
}

static Four getEmbeddedAttrsValForThreeFixedSizeAttr(OOSQL_StorageManager* storageSystem, Four scanId, char* embeddedAttrPtr, Four embeddedAttrSize, Four nCols, OOSQL_StorageManager::ColListStruct* clist, OOSQL_StorageManager::EmbeddedAttrTranslationInfo* embeddedAttrTranslationInfo)
{
	Four embeddedColNo;
	
	embeddedColNo = embeddedAttrTranslationInfo->realColNoToEmbeddedColNo[clist[0].colNo];

	if(!BITTEST(embeddedAttrPtr, embeddedColNo))
	{
		clist[0].nullFlag  = SM_FALSE;
		clist[0].retLength = embeddedAttrTranslationInfo->embeddedAttrInfo[embeddedColNo].length;
		if(embeddedAttrTranslationInfo->embeddedAttrInfo[embeddedColNo].type != OOSQL_TYPE_STRING)
		{
			memcpy(&clist[0].data.s, 
				   embeddedAttrPtr + embeddedAttrTranslationInfo->embeddedAttributeOffset + embeddedAttrTranslationInfo->embeddedAttrInfo[embeddedColNo].start, 
				   clist[0].retLength);
		}
		else
		{
			memcpy(clist[0].data.ptr, 
				   embeddedAttrPtr + embeddedAttrTranslationInfo->embeddedAttributeOffset + embeddedAttrTranslationInfo->embeddedAttrInfo[embeddedColNo].start, 
				   clist[0].retLength);
		}
	}
	else
	{
		clist[0].nullFlag  = SM_TRUE;
		clist[0].retLength = NULL_LENGTH;
	}

	embeddedColNo = embeddedAttrTranslationInfo->realColNoToEmbeddedColNo[clist[1].colNo];

	if(!BITTEST(embeddedAttrPtr, embeddedColNo))
	{
		clist[1].nullFlag  = SM_FALSE;
		clist[1].retLength = embeddedAttrTranslationInfo->embeddedAttrInfo[embeddedColNo].length;
		if(embeddedAttrTranslationInfo->embeddedAttrInfo[embeddedColNo].type != OOSQL_TYPE_STRING)
		{
			memcpy(&clist[1].data.s, 
				   embeddedAttrPtr + embeddedAttrTranslationInfo->embeddedAttributeOffset + embeddedAttrTranslationInfo->embeddedAttrInfo[embeddedColNo].start, 
				   clist[1].retLength);
		}
		else
		{
			memcpy(clist[1].data.ptr, 
				   embeddedAttrPtr + embeddedAttrTranslationInfo->embeddedAttributeOffset + embeddedAttrTranslationInfo->embeddedAttrInfo[embeddedColNo].start, 
				   clist[1].retLength);
		}
	}
	else
	{
		clist[1].nullFlag  = SM_TRUE;
		clist[1].retLength = NULL_LENGTH;
	}

	embeddedColNo = embeddedAttrTranslationInfo->realColNoToEmbeddedColNo[clist[2].colNo];

	if(!BITTEST(embeddedAttrPtr, embeddedColNo))
	{
		clist[2].nullFlag  = SM_FALSE;
		clist[2].retLength = embeddedAttrTranslationInfo->embeddedAttrInfo[embeddedColNo].length;
		if(embeddedAttrTranslationInfo->embeddedAttrInfo[embeddedColNo].type != OOSQL_TYPE_STRING)
		{
			memcpy(&clist[2].data.s, 
				   embeddedAttrPtr + embeddedAttrTranslationInfo->embeddedAttributeOffset + embeddedAttrTranslationInfo->embeddedAttrInfo[embeddedColNo].start, 
				   clist[2].retLength);
		}
		else
		{
			memcpy(clist[2].data.ptr, 
				   embeddedAttrPtr + embeddedAttrTranslationInfo->embeddedAttributeOffset + embeddedAttrTranslationInfo->embeddedAttrInfo[embeddedColNo].start, 
				   clist[2].retLength);
		}
	}
	else
	{
		clist[2].nullFlag  = SM_TRUE;
		clist[2].retLength = NULL_LENGTH;
	}

	return eNOERROR;
}

Four OOSQL_Evaluator::determineFunctionForGetEmbeddedAttrVals(
	OOSQL_StorageManager::EmbeddedAttrTranslationInfo*	embeddedAttrTranslationInfo,	// IN  
	Four												nCols,							// IN  
	OOSQL_StorageManager::ColListStruct*				clist,							// IN  
	OOSQL_GetEmbeddedAttrsValFuncType*					func							// OUT
)
{
    if(clist[0].colNo == 0)
    {
        clist ++;
        nCols --;
    }

	if(nCols == 1)
	{
		switch(embeddedAttrTranslationInfo->embeddedAttrInfo[embeddedAttrTranslationInfo->realColNoToEmbeddedColNo[clist[0].colNo]].type)
		{
		case OOSQL_TYPE_INT:
		case OOSQL_TYPE_SHORT:
		case OOSQL_TYPE_FLOAT:
		case OOSQL_TYPE_DOUBLE:
		case OOSQL_TYPE_STRING:
			*func = getEmbeddedAttrsValForOneFixedSizeAttr;
			break;
		case OOSQL_TYPE_VARSTRING:
			*func = getEmbeddedAttrsValForOneVarSizeAttr;
			break;
		default:
			*func = getEmbeddedAttrsVal;
			break;
		}
	}
	else if(nCols == 2)
	{
		switch(embeddedAttrTranslationInfo->embeddedAttrInfo[embeddedAttrTranslationInfo->realColNoToEmbeddedColNo[clist[0].colNo]].type)
		{
		case OOSQL_TYPE_INT:
		case OOSQL_TYPE_SHORT:
		case OOSQL_TYPE_FLOAT:
		case OOSQL_TYPE_DOUBLE:
		case OOSQL_TYPE_STRING:
			switch(embeddedAttrTranslationInfo->embeddedAttrInfo[embeddedAttrTranslationInfo->realColNoToEmbeddedColNo[clist[1].colNo]].type)
			{
			case OOSQL_TYPE_INT:
			case OOSQL_TYPE_SHORT:
			case OOSQL_TYPE_FLOAT:
			case OOSQL_TYPE_DOUBLE:
			case OOSQL_TYPE_STRING:
				*func = getEmbeddedAttrsValForTwoFixedSizeAttr;
				break;
			default:
				*func = getEmbeddedAttrsVal; 
				break;
			}
			break;
		default:
			*func = getEmbeddedAttrsVal; 
			break;
		}
	}
	else if(nCols == 3)
	{
		switch(embeddedAttrTranslationInfo->embeddedAttrInfo[embeddedAttrTranslationInfo->realColNoToEmbeddedColNo[clist[0].colNo]].type)
		{
		case OOSQL_TYPE_INT:
		case OOSQL_TYPE_SHORT:
		case OOSQL_TYPE_FLOAT:
		case OOSQL_TYPE_DOUBLE:
		case OOSQL_TYPE_STRING:
			switch(embeddedAttrTranslationInfo->embeddedAttrInfo[embeddedAttrTranslationInfo->realColNoToEmbeddedColNo[clist[1].colNo]].type)
			{
			case OOSQL_TYPE_INT:
			case OOSQL_TYPE_SHORT:
			case OOSQL_TYPE_FLOAT:
			case OOSQL_TYPE_DOUBLE:
			case OOSQL_TYPE_STRING:
				switch(embeddedAttrTranslationInfo->embeddedAttrInfo[embeddedAttrTranslationInfo->realColNoToEmbeddedColNo[clist[2].colNo]].type)
				{
				case OOSQL_TYPE_INT:
				case OOSQL_TYPE_SHORT:
				case OOSQL_TYPE_FLOAT:
				case OOSQL_TYPE_DOUBLE:
				case OOSQL_TYPE_STRING:
					*func = getEmbeddedAttrsValForThreeFixedSizeAttr;
					break;
				default:
					*func = getEmbeddedAttrsVal; 
					break;
				}
				break;
			default:
				*func = getEmbeddedAttrsVal; 
				break;
			}
			break;
		default:
			*func = getEmbeddedAttrsVal;
			break;
		}
	}
	else
		*func = getEmbeddedAttrsVal;

	return eNOERROR;
}
