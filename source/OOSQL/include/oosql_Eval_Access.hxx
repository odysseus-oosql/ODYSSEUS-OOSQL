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

#ifndef __OOSQL_EVAL_ACCESS_HXX__
#define __OOSQL_EVAL_ACCESS_HXX__

#include "OOSQL_Evaluator.hxx"
#include "oosql_Eval_TextIR_Index.hxx"

inline Four OOSQL_Evaluator::seqScanNext(
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
    Four							scanId;
    OOSQL_StorageManager::OID*		oid;
    Four							nCols;
    EVAL_EvalBufferSlot*			clist;
    Four							e;
	OOSQL_TempFileInfo*				tempFileInfo;
	Four							i;

    /* get scan ID according to the class kind */
    switch(ACCESSPLANELEMENTS[planIndex].classInfo.classKind) 
	{
		case CLASSKIND_SORTSTREAM:
			nCols = EVAL_EVALBUFFER[planIndex].nCols;
			clist = EVAL_EVALBUFFER[planIndex].getColSlotPtr(0);

			tempFileInfo = EVAL_TEMPFILEINFOTABLEELEMENTS[ACCESSPLANELEMENTS[planIndex].classInfo.tempFileNum];

			if(tempFileInfo->useFastEncoding)
			{
				if(!tempFileInfo->firstFastEncodedNextScan)
				{
					e = tempFileInfo->sortStream->FastEncodedNextScan();
					if(e == eNOERROR)
						return eNOERROR;
					else if(e == EOS)
						return EOS;
					else
						OOSQL_CHECK_ERR(e);
				}
				else
				{
					OOSQL_FastEncodingInfo encodingInfo[MAXNUMOFATTRIBUTE];

					e = tempFileInfo->sortStream->GetFastEncodingInfo(nCols, encodingInfo);
					OOSQL_CHECK_ERR(e);

					for(i = 0; i < nCols; i++)
					{
						if(encodingInfo[i].type == OOSQL_TYPE_STRING || encodingInfo[i].type == OOSQL_TYPE_VARSTRING)
							encodingInfo[i].ptr  = (char*)clist[i].data.ptr;
						else
#ifndef SUPPORT_LARGE_DATABASE2
							encodingInfo[i].ptr  = (char*)&clist[i].data.s;
#else
							encodingInfo[i].ptr  = (char*)&clist[i].data.i;
#endif

						encodingInfo[i].size     = &clist[i].retLength;
						encodingInfo[i].nullFlag = &clist[i].nullFlag;
					}

					e = tempFileInfo->sortStream->SetFastEncodingInfo(nCols, encodingInfo);
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
				e = tempFileInfo->sortStream->FastNextScan(nCols, clist);
				if(e == eNOERROR)
					return eNOERROR;
				else if(e == EOS)
					return EOS;
				else
					OOSQL_CHECK_ERR(e);
			}
			break;

        case CLASSKIND_PERSISTENT:
            scanId = EVAL_ACCESSLISTTABLE[planIndex].getCurrScanID();
            break;

        case CLASSKIND_TEMPORARY:
            scanId = EVAL_TEMPFILEINFOTABLEELEMENTS[ ACCESSPLANELEMENTS[planIndex].classInfo.tempFileNum ]->getOsn();
            break;

        default:    /* invalid class kind */
            OOSQL_ERR(eINVALID_CLASSKIND_OOSQL);
    }

    /* get pointer to oid */
    oid = EVAL_EVALBUFFER[planIndex].getOID_Ptr();

    /* move scan cursor forward */
	if(scanId >= 0)
	{
		e = m_storageManager->NextObject( scanId, oid, NULL );
		if(e < eNOERROR) {
			OOSQL_ERR(e);
		}
		else if(e == EOS) {    /* end of scan */
			return e;
		}
	}
	else
		return EOS;

    /* get the # of column and pointer to the evaluation buffer 
     * NOTE: we should check if the # of used columns is zero
     *       because some query do not used any attribute.
     */
	nCols = EVAL_EVALBUFFER[planIndex].nCols;
    if(nCols == 0) 
        return eNOERROR;

    clist = EVAL_EVALBUFFER[planIndex].getColSlotPtr(0);

    /* fetch the next object */
    e = m_storageManager->FetchObjectByColList( scanId, SM_TRUE, oid, nCols, clist );
    if(e < eNOERROR) 
        OOSQL_ERR(e);

    /* return */
    return eNOERROR;
}


inline Four OOSQL_Evaluator::mlgfIndexScanNext(
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
    Four							scanId;
	OOSQL_StorageManager::OID*		oid;
    Four							nCols;
    EVAL_EvalBufferSlot*			clist;
    Four							e;
	OOSQL_StorageManager::Cursor*	cursor;
	AP_IndexInfoPoolElements		indexInfo(m_pool->indexInfoPool);
	Four							nBoolExprs;
	OOSQL_StorageManager::BoolExp*	boolExprs;
	AP_ColNoMapElement*				colNoMap;
	AP_BoolExprElement*				boolExprInfos;
	OOSQL_ScanInfo*					scanInfo;

    /* move the scan cursor forward and get the OOSQL_StorageManager::OID of the next object */
	indexInfo	  = EVAL_ACCESSLISTTABLE[planIndex].accessList[accessElemIndex].indexInfo;

	/* retrieve bool expression information */
	scanInfo      = &EVAL_INDEX_SCANINFOTABLEELEMENTS[indexInfo.startIndex];
	nBoolExprs    = scanInfo->nBoolExprs;
	boolExprs     = scanInfo->boolExprs;
	boolExprInfos = scanInfo->boolExprInfos;
	colNoMap      = scanInfo->colNoMap;
	nCols         = scanInfo->nCols;
	clist		  = scanInfo->clist;
	scanId        = scanInfo->scanId;
    oid			  = scanInfo->oid;

	while(1)
	{
		/* move the scan cursor forward and get the OID of the next object */
		e = execBtreeIndexInfoNode(indexInfo.getPoolIndex(), oid, cursor);
		if(e < eNOERROR) 
		{
			OOSQL_ERR(e);
		}
		else if(e == ENDOFSCAN)	/* end of scan */
			return e;

		if(nCols == 0) break;

		/* fetch the next object */
		e = m_storageManager->FetchObjectByColList(scanId, SM_TRUE, oid, nCols, clist);
		if(e < eNOERROR) OOSQL_ERR(e);

		if(nBoolExprs > 0)
		{
			e = checkBoolExpression(nCols, clist, nBoolExprs, boolExprs, boolExprInfos, colNoMap);
			OOSQL_CHECK_ERR(e);

			if(e == SM_TRUE)
				break;
		}
		else
			break;
	}

    /* return */
    return eNOERROR;
}

inline Four OOSQL_Evaluator::execBtreeIndexScan( 
    AP_IndexInfoPoolIndex				indexInfo,      // IN:
    OOSQL_StorageManager::OID*			oid,			// OUT:
	OOSQL_StorageManager::Cursor*&		cursor			// OUT:
)
/*
    Function:

    Side effect:

    Referenced member variables:
        m_currPlanIndex

    Return value:
*/
{
    Four    scanId;
    Four    e;

    /* get the scan ID for the current B+ tree index scan node */
    scanId = EVAL_INDEX_SCANINFOTABLEELEMENTS[indexInfo.startIndex].scanId;
	
    /* move the scan cursor forward for this B+ tree index */
	if(scanId >= 0)
	{
		e = m_storageManager->NextObject( scanId, oid, &cursor );
		if(e < eNOERROR) 
		{
			OOSQL_ERR(e);
		}
		else if(e == EOS) 
		{    /* end of scan */
			return(e);
		}
	}
	else
		return EOS;

    /* return */
    return eNOERROR;
}


inline Four OOSQL_Evaluator::getNextObject()
/*
    Function:

    Side effect:

    Referenced member variables:
        m_currPlanIndex


    Return value:
*/
{
    Four currAccessIndex;
    Four accessRC;              /* return code of access */
    Four e;

	if(ACCESSPLAN.m_nAP_Elem <= m_currPlanIndex)
		return ENDOFSCAN;

    /* do nothing if class kind is null */ 
    if(ACCESSPLANELEMENTS[m_currPlanIndex].classInfo.classKind == CLASSKIND_NULL_AGGRFUNC_ONLY) 
	{
        if(m_evalStatus.nullClassEndOfScan == SM_TRUE)
            return(ENDOFSCAN);
        else
            return eNOERROR;
    }
	else if(ACCESSPLANELEMENTS[m_currPlanIndex].classInfo.classKind == CLASSKIND_NONE)
		return ENDOFSCAN;

    do {
        /* get the current access element index */
        currAccessIndex = EVAL_ACCESSLISTTABLE[m_currPlanIndex].currAccessIndex;

        /* get the next object according to the access method of the current access element */
        switch(EVAL_ACCESSLISTTABLE[m_currPlanIndex].accessList[currAccessIndex].accessMethod) {
            case ACCESSMETHOD_TEXT_IDXSCAN:
                accessRC = textIndexScanNext(m_currPlanIndex, currAccessIndex);
				if(accessRC == eNOERROR)
					break;
                else if(accessRC == ENDOFSCAN) {
                    /* mark that the current scan is ended */
                    EVAL_ACCESSLISTTABLE[m_currPlanIndex].endOfCurrAccess = SM_TRUE;

                    /* check if all accesses are ended */
                    if(!EVAL_ACCESSLISTTABLE[m_currPlanIndex].isEndOfAllAccess()) {
						e = closeTextIndexScan(m_currPlanIndex, EVAL_ACCESSLISTTABLE[m_currPlanIndex].currAccessIndex);
						if(e < eNOERROR) OOSQL_ERR(e);

                        /* move to the next access element */
                        e = EVAL_ACCESSLISTTABLE[m_currPlanIndex].moveToNextAccessElem();
                        if(e < eNOERROR) OOSQL_ERR(e);

						resetCurrentWhereCondNodes();		

						e = openTextIndexScan(m_currPlanIndex, EVAL_ACCESSLISTTABLE[m_currPlanIndex].currAccessIndex);
						if(e < eNOERROR) OOSQL_ERR(e);

                        /* clear OOSQL_StorageManager::OID at evaluation buffer to read the first object of the next access element */
                        EVAL_EVALBUFFER[m_currPlanIndex].clearOID();
                    }
                }
                else if(accessRC < eNOERROR) {
                    OOSQL_ERR(accessRC);
                }

                break;

            case ACCESSMETHOD_SEQ_SCAN:
                accessRC = seqScanNext(m_currPlanIndex, currAccessIndex);
				if(accessRC == eNOERROR)
					break;
                else if(accessRC == ENDOFSCAN) {
                    /* mark that the current scan is ended */
                    EVAL_ACCESSLISTTABLE[m_currPlanIndex].endOfCurrAccess = SM_TRUE;

                    /* check if all accesses are ended */
                    if(!EVAL_ACCESSLISTTABLE[m_currPlanIndex].isEndOfAllAccess()) {
                        /* move to the next access element */
                        e = EVAL_ACCESSLISTTABLE[m_currPlanIndex].moveToNextAccessElem();
                        if(e < eNOERROR) OOSQL_ERR(e);

						resetCurrentWhereCondNodes();

                        /* clear OOSQL_StorageManager::OID at evaluation buffer to read the first object of the next access element */
                        EVAL_EVALBUFFER[m_currPlanIndex].clearOID();
                    }
                }
                if(accessRC < eNOERROR) {
                    OOSQL_ERR(accessRC);
                }

                break;

            case ACCESSMETHOD_BTREE_IDXSCAN:
                accessRC = btreeIndexScanNext(m_currPlanIndex, currAccessIndex);
				if(accessRC == eNOERROR)
					break;
                else if(accessRC == ENDOFSCAN) {
                    /* mark that the current scan is ended */
                    EVAL_ACCESSLISTTABLE[m_currPlanIndex].endOfCurrAccess = SM_TRUE;

                    /* check if all accesses are ended */
                    if(!EVAL_ACCESSLISTTABLE[m_currPlanIndex].isEndOfAllAccess()) {
                        /* move to the next access element */
                        e = EVAL_ACCESSLISTTABLE[m_currPlanIndex].moveToNextAccessElem();
                        if(e < eNOERROR) OOSQL_ERR(e);

						resetCurrentWhereCondNodes();		

                        /* clear OOSQL_StorageManager::OID at evaluation buffer to read the first object of the next access element */
                        EVAL_EVALBUFFER[m_currPlanIndex].clearOID();
                    }
                }
                else if(accessRC < eNOERROR) {
                    OOSQL_ERR(accessRC);
                }

                break;

            case ACCESSMETHOD_OID_FETCH:
                accessRC = oidFetch(m_currPlanIndex, currAccessIndex);
				if(accessRC == eNOERROR)
					break;
                else if(accessRC == ENDOFSCAN) {
                    e= EVAL_ACCESSLISTTABLE[m_currPlanIndex].setEndOfAllAccess();
                    if(e < eNOERROR) OOSQL_ERR(e);

					resetCurrentWhereCondNodes();		
                }
                else if(accessRC < eNOERROR) {
                    OOSQL_ERR(accessRC);
                }
                break;

            case ACCESSMETHOD_OID_SETSCAN:
                accessRC = oidSetScanNext(m_currPlanIndex, currAccessIndex);
				if(accessRC == eNOERROR)
					break;
                else if(accessRC == ENDOFSCAN) {
                    /* mark that the current scan is ended */
                    EVAL_ACCESSLISTTABLE[m_currPlanIndex].endOfCurrAccess = SM_TRUE;

                    /* check if all accesses are ended */
                    if(!EVAL_ACCESSLISTTABLE[m_currPlanIndex].isEndOfAllAccess()) {
                        /* move to the next access element */
                        e = EVAL_ACCESSLISTTABLE[m_currPlanIndex].moveToNextAccessElem();
                        if(e < eNOERROR) OOSQL_ERR(e);

						resetCurrentWhereCondNodes(); 

                        /* clear OOSQL_StorageManager::OID at evaluation buffer to read the first object of the next access element */
                        EVAL_EVALBUFFER[m_currPlanIndex].clearOID();
                    }
                }
                else if(accessRC < eNOERROR) {
                    OOSQL_ERR(accessRC);
                }

                break;

            case ACCESSMETHOD_MLGF_IDXSCAN:
            case ACCESSMETHOD_MLGF_MBR_IDXSCAN:
                accessRC = mlgfIndexScanNext(m_currPlanIndex, currAccessIndex);
				if(accessRC == eNOERROR)
					break;
                else if(accessRC == ENDOFSCAN) {
                    /* mark that the current scan is ended */
                    EVAL_ACCESSLISTTABLE[m_currPlanIndex].endOfCurrAccess = SM_TRUE;

                    /* check if all accesses are ended */
                    if(!EVAL_ACCESSLISTTABLE[m_currPlanIndex].isEndOfAllAccess()) {
                        /* move to the next access element */
                        e = EVAL_ACCESSLISTTABLE[m_currPlanIndex].moveToNextAccessElem();
                        if(e < eNOERROR) OOSQL_ERR(e);

						resetCurrentWhereCondNodes();	

                        /* clear OOSQL_StorageManager::OID at evaluation buffer to read the first object of the next access element */
                        EVAL_EVALBUFFER[m_currPlanIndex].clearOID();
                    }
                }
                else if(accessRC < eNOERROR) {
                    OOSQL_ERR(accessRC);
                }

                break;

            default:
                OOSQL_ERR(eINVALID_ACCESSMETHOD_OOSQL);
        }

		/* execute loop while scan of the current access element ended and
		 * there remains one or more access element to access
		*/
    } while(accessRC == ENDOFSCAN && !EVAL_ACCESSLISTTABLE[m_currPlanIndex].isEndOfAllAccess());

    /* check if end-of-scan occurred */
	if(accessRC == eNOERROR)
		return eNOERROR;
    else if(accessRC == ENDOFSCAN)
        return(ENDOFSCAN);
	return eNOERROR;
}

inline Four OOSQL_Evaluator::btreeIndexScanNext(
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
    Four							scanId;
    OOSQL_StorageManager::OID*		oid;
    Four							nCols;
    EVAL_EvalBufferSlot*			clist;
    Four							e;
	AP_IndexInfoPoolElements		indexInfo(m_pool->indexInfoPool);
	OOSQL_StorageManager::Cursor*	cursor;
	Four							nBoolExprs;
	OOSQL_StorageManager::BoolExp*	boolExprs;
	AP_ColNoMapElement*				colNoMap;
	AP_BoolExprElement*				boolExprInfos;
	OOSQL_ScanInfo*					scanInfo;

    /* move the scan cursor forward and get the OOSQL_StorageManager::OID of the next object */
	indexInfo	  = EVAL_ACCESSLISTTABLE[planIndex].accessList[accessElemIndex].indexInfo;

	/* retrieve bool expression information */
	scanInfo      = &EVAL_INDEX_SCANINFOTABLEELEMENTS[indexInfo.startIndex];
	nBoolExprs    = scanInfo->nBoolExprs;
	boolExprs     = scanInfo->boolExprs;
	boolExprInfos = scanInfo->boolExprInfos;
	colNoMap      = scanInfo->colNoMap;
	nCols         = scanInfo->nCols;
	clist		  = scanInfo->clist;
	scanId        = scanInfo->scanId;
    oid			  = scanInfo->oid;

	while(1)
	{
		e = execBtreeIndexInfoNode(indexInfo.getPoolIndex(), oid, cursor);
		if(e < eNOERROR) 
		{
			OOSQL_ERR(e);
		}
		else if(e == ENDOFSCAN) 
		{	/* end of scan */
			return(e);
		}

		if(nCols == 0) break;

		/* fetch the next object */
		if(indexInfo[0].scan.readObjectValueFromIndexFlag == SM_TRUE)
		{
			Four							clistIndex;
			char*							valPtr;
			Two								length;
			Four							i;

			valPtr   = cursor->btree.key.val;
			for(i = 0; i < indexInfo[0].scan.nCols; i++)
			{
				clistIndex = colNoMap[indexInfo[0].scan.columns[i].colNo].offset;

				if(indexInfo[0].scan.columns[i].type == TYPEID_STRING)
				{
					length = indexInfo[0].scan.columns[i].length;
					if(clistIndex != NOT_USED_COLUMN)
						memcpy(clist[clistIndex].data.ptr, valPtr, length);
					valPtr += length;
				}
				else if(indexInfo[0].scan.columns[i].type == TYPEID_VARSTRING)
				{
					memcpy(&length, valPtr, sizeof(Two));
					if(clistIndex != NOT_USED_COLUMN)
						memcpy(clist[clistIndex].data.ptr, valPtr + sizeof(Two), length);
					valPtr +=(length + sizeof(Two));
				}
				else
				{
					length = indexInfo[0].scan.columns[i].length;
					if(clistIndex != NOT_USED_COLUMN)
#ifndef SUPPORT_LARGE_DATABASE2
						memcpy(&clist[clistIndex].data.s, valPtr, length);
#else
						memcpy(&clist[clistIndex].data.i, valPtr, length);
#endif
					valPtr += length;
				}
				
				if(clistIndex != NOT_USED_COLUMN)
				{
					clist[clistIndex].retLength = length;
					clist[clistIndex].nullFlag  = SM_FALSE;
				}
			}
		}
		else
		{
			e = m_storageManager->FetchObjectByColList(scanId, SM_TRUE, oid, nCols, clist);
			if(e < eNOERROR) OOSQL_ERR(e);
		}

		if(nBoolExprs > 0)
		{
			e = checkBoolExpression(nCols, clist, nBoolExprs, boolExprs, boolExprInfos, colNoMap);
			OOSQL_CHECK_ERR(e);

			if(e == SM_TRUE)
				break;
		}
		else
			break;
	}

    /* return */
    return eNOERROR;
}

inline Four OOSQL_Evaluator::checkBoolExpression(Four nCols, OOSQL_StorageManager::ColListStruct* clist, Four nBoolExprs, OOSQL_StorageManager::BoolExp* boolExprs, AP_BoolExprElement* boolExprInfos, AP_ColNoMapElement* colNoMap)
{
	Four				i;
	Four				j;
	Four				clistIndex;
	double				cmpResult;
	Four				colStrLen;	
	char				*colStrPtr;
	AP_BoolExprElement* boolExprInfo;

	for(i = 0; i < nBoolExprs; i++)
	{
		boolExprInfo = &boolExprInfos[i];
		clistIndex   = colNoMap[boolExprs[i].colNo].offset;

		if(clistIndex == NOT_USED_COLUMN)
			return SM_FALSE;
		else
		{
			switch(boolExprInfo->type)
			{
			case OOSQL_TYPE_SHORT:
				cmpResult = clist[clistIndex].data.s - boolExprs[i].data.s;
				break;
    
			case OOSQL_TYPE_INT:
				cmpResult = clist[clistIndex].data.i - boolExprs[i].data.i;
				break;
        
			case OOSQL_TYPE_LONG:
				cmpResult = clist[clistIndex].data.l - boolExprs[i].data.l;
				break;
        
			case OOSQL_TYPE_FLOAT:
				cmpResult = clist[clistIndex].data.f - boolExprs[i].data.f;
				break;
        
			case OOSQL_TYPE_DOUBLE:
				cmpResult = clist[clistIndex].data.d - boolExprs[i].data.d;
				break;

			case OOSQL_TYPE_STRING:
			case OOSQL_TYPE_VARSTRING:
                if (boolExprInfo->type == OOSQL_TYPE_STRING)
                {
					colStrPtr = (char*)clist[clistIndex].data.ptr;
					for (j = 0; j < clist[clistIndex].retLength; j++)
						if (colStrPtr[j] == 0)	break;
                    colStrLen = j; 
                }
                else
                {
                    colStrLen = clist[clistIndex].retLength;
                }

				if (colStrLen < boolExprs[i].length) {
                    cmpResult = memcmp( clist[clistIndex].data.ptr, boolExprs[i].data.str, colStrLen );
                    if (cmpResult == 0) 
			            cmpResult = -1;  // negative value indicating 
                }
                else if (colStrLen > boolExprs[i].length) {
                    cmpResult = memcmp( clist[clistIndex].data.ptr, boolExprs[i].data.str, (int)boolExprs[i].length );
                    if (cmpResult == 0) {
                        cmpResult = 1;   // positive value indicating 
                    }
                }
                else {  /* clist[clistIndex].retLength == clist[clistIndex].retLength */
                    cmpResult = memcmp(clist[clistIndex].data.ptr, boolExprs[i].data.str, boolExprs[i].length);
                }	
				break;

			case OOSQL_TYPE_MBR:				
				OOSQL_ERR(eNOTIMPLEMENTED_OOSQL);

			case OOSQL_TYPE_DATE:
				cmpResult = m_storageManager->CompareDate(&(clist[clistIndex].data.date), &(boolExprs[i].data.date));
				break;

			case OOSQL_TYPE_TIME:
				cmpResult = m_storageManager->CompareTime(&(clist[clistIndex].data.time), &(boolExprs[i].data.time));
				break;

			case OOSQL_TYPE_TIMESTAMP:
				break;

			default:
				OOSQL_ERR(eTYPE_ERROR_OOSQL);
			}

			switch(boolExprInfo->op)
			{
			case OOSQL_StorageManager::SM_EQ:
				if(cmpResult == 0) break;
				else               return SM_FALSE;
			case OOSQL_StorageManager::SM_NE:
				if(cmpResult != 0) break;
				else               return SM_FALSE;
				break;
			case OOSQL_StorageManager::SM_GT:
				if(cmpResult > 0)  break;
				else               return SM_FALSE;
			case OOSQL_StorageManager::SM_LT:
				if(cmpResult < 0)  break;
				else               return SM_FALSE;
			case OOSQL_StorageManager::SM_GE:
				if(cmpResult >= 0) break;
				else               return SM_FALSE;
			case OOSQL_StorageManager::SM_LE:
				if(cmpResult <= 0) break;
				else               return SM_FALSE;
			default:
				return SM_FALSE;
			}
		}
	}

	return SM_TRUE;
}


#ifndef SLIMDOWN_TEXTIR
inline Four OOSQL_Evaluator::textIndexScanNext(
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
    Four							scanId;
	Four							ocn;
    OOSQL_StorageManager::OID*		oid;
    Four							nCols, nColsForEmbeddedAttrRead;
    EVAL_EvalBufferSlot*			clist;
    EVAL_EvalBufferSlot*			clistForEmbeddedAttrRead;
    Four							e;
	AP_IndexInfoPoolElements		indexInfo(m_pool->indexInfoPool);
	
	OOSQL_TextIR_Posting			*posting;
	OOSQL_TextIR_Posting			tempPosting;
	EVAL_EvalBufferSlot				*fnMatchResult;
	AP_IndexInfoElement				*indexNode;
	Four							logicalId;
	OOSQL_ScanInfo*					scanInfo;

	/* move the scan cursor forward and get the OOSQL_StorageManager::OID of the next object */
	indexInfo	  = EVAL_ACCESSLISTTABLE[planIndex].accessList[accessElemIndex].indexInfo;

	/* retrieve bool expression information */
	scanInfo      = &EVAL_INDEX_SCANINFOTABLEELEMENTS[indexInfo.startIndex];
	indexNode     = scanInfo->indexNode;
	fnMatchResult = scanInfo->fnMatchResult;
	nCols         = scanInfo->nCols;
	clist		  = scanInfo->clist;
	scanId        = scanInfo->scanId;
	ocn			  = scanInfo->ocn;
    oid			  = scanInfo->oid;
	OOSQL_TextIR_PostingQueue& postingQueue = scanInfo->postingQueue;

	while(1)
	{
#define NEED_EXACT_WEIGHT
#ifdef NEED_EXACT_WEIGHT
		// restore weight information of the columns that have been initialized by adjustWeightInfo()
		e = restoreWeightInfo(indexInfo.getPoolIndex());
		if (e < eNOERROR) OOSQL_ERR(e);
#endif	/* #ifdef NEED_EXACT_WEIGHT */

		while(postingQueue.IsEmpty())
		{
			/* move the scan cursor forward and get the OOSQL_StorageManager::OID of the next object */
			e = execTextIndexInfoNode(indexInfo.getPoolIndex(), postingQueue, TEXTIR_POSTINGSKIP_NO_HINTS);
			if(e < eNOERROR) OOSQL_ERR(e);
			if(e == TEXTIR_EOS) 
			{
				if(postingQueue.IsEmpty())
					return ENDOFSCAN;
				else
					break;
			}
		}

		posting = &postingQueue.Head();
		if(postingQueue.PopHead() == false)
			return ENDOFSCAN;
		
		// save oid to Evaluation Buffer
		*oid = posting->docId;

#ifdef NEED_EXACT_WEIGHT
		// reset weight information of the columns that have not been matched
		// while performing the "OR" operation among multiple columns
		e = adjustWeightInfo(indexInfo.getPoolIndex(), posting);
		if (e < eNOERROR) OOSQL_ERR(e);
#endif	/* #ifdef NEED_EXACT_WEIGHT */

		// save weight information
		if(fnMatchResult)
#if defined (EXTENDED_BOOLEAN_MODEL)
			fnMatchResult->data.d = posting->weight;
#elif defined (HEURISTIC_MODEL)
			fnMatchResult->data.d = posting->weight + posting->bonus;
#endif

		if(nCols == 0) break;

		// fetch the next object
		AP_IndexInfoPoolElements		postingIndexInfo(m_pool->indexInfoPool);
		OOSQL_ScanInfo*					postingScanInfo;
		Four							nBoolExprs;
		OOSQL_StorageManager::BoolExp*	boolExprs;
		AP_ColNoMapElement*				colNoMap;
		AP_BoolExprElement*				boolExprInfos;

		postingIndexInfo	= posting->indexInfoPoolIndex;
		postingScanInfo     = &EVAL_INDEX_SCANINFOTABLEELEMENTS[postingIndexInfo.startIndex];
		nBoolExprs			= postingScanInfo->nBoolExprs;
		boolExprs			= postingScanInfo->boolExprs;
		boolExprInfos		= postingScanInfo->boolExprInfos;
		colNoMap			= postingScanInfo->colNoMap;

		if(postingIndexInfo[0].scan.readObjectValueFromIndexFlag == SM_TRUE && 
           posting->ptrToEmbeddedAttrsBuf &&                        
           posting->embeddedAttrsBufSize >= 0)                     
		{
            if(clist[0].colNo == 0) 
            {
                ASSIGN_VALUE_TO_COL_LIST(clist[0], posting->logicalDocId, sizeof(Four));
                clist[0].retLength = OOSQL_TYPE_LONG_SIZE_VAR;
                nColsForEmbeddedAttrRead = nCols - 1;
                clistForEmbeddedAttrRead = &clist[1];
            }
            else
            {
                nColsForEmbeddedAttrRead = nCols;
                clistForEmbeddedAttrRead = clist;
            }

			if(*(posting->getEmbeddedAttrsValFuncPtr) && nCols > 0)
			{
				e = (*posting->getEmbeddedAttrsValFuncPtr)(m_storageManager, posting->scanId, 
                                                           posting->ptrToEmbeddedAttrsBuf, 
                                                           posting->embeddedAttrsBufSize, 
                                                           nColsForEmbeddedAttrRead, clistForEmbeddedAttrRead,
					                                       posting->embeddedAttrTranslationInfo);
				OOSQL_CHECK_ERR(e);
			}
			else if(nCols > 0)
			{
				e = determineFunctionForGetEmbeddedAttrVals(posting->embeddedAttrTranslationInfo, nCols, clist, 
															posting->getEmbeddedAttrsValFuncPtr);
				OOSQL_CHECK_ERR(e);

				e = (*posting->getEmbeddedAttrsValFuncPtr)(m_storageManager, posting->scanId, 
                                                           posting->ptrToEmbeddedAttrsBuf, 
                                                           posting->embeddedAttrsBufSize,
                                                           nColsForEmbeddedAttrRead, clistForEmbeddedAttrRead,
					                                       posting->embeddedAttrTranslationInfo);
				OOSQL_CHECK_ERR(e);
			}
		}
		else
		{
			e = m_storageManager->FetchObjectByColList(ocn, SM_FALSE, oid, nCols, clist);
			if(e < eNOERROR) 
			{
				OOSQL_ERR(e);
			}
		}

		if(nBoolExprs > 0)
		{
			e = checkBoolExpression(nCols, clist, nBoolExprs, boolExprs, boolExprInfos, colNoMap);
			OOSQL_CHECK_ERR(e);

			if(e == SM_TRUE)
				break;
		}
		else
			break;
	}

    /* return */
    return eNOERROR;
}
#else /* SLIMDOWN_TEXTIR */

inline Four OOSQL_Evaluator::textIndexScanNext(
    Four planIndex,             /* IN: */
    Four accessElemIndex)       /* IN: */
{
    return eTEXTIR_NOTENABLED_OOSQL;
}

#endif /* SLIMDOWN_TEXTIR */


inline Four OOSQL_Evaluator::execBtreeIndexInfoNode(
    AP_IndexInfoPoolIndex			indexInfo,		// IN:
    OOSQL_StorageManager::OID*		oid,            // OUT:
	OOSQL_StorageManager::Cursor*&	cursor
)
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
    AP_IndexInfoElement *indexNode;
    Four                e;

    /* check input paramter */
#ifdef  OOSQL_DEBUG
    if( IS_NULL_POOLINDEX(indexInfo) == SM_TRUE )
        OOSQL_ERR(eBADPARAMETER_OOSQL);
    if(oid == NULL)
        OOSQL_ERR(eBADPARAMETER_OOSQL);
#endif

    /* get index info. node for the current node */
    indexNode = ACCESSPLAN.getIndexInfoElem(indexInfo);
#ifdef  OOSQL_DEBUG
    if(indexNode == NULL) OOSQL_ERR(eNULL_POINTER_OOSQL);
#endif

    /* traverse index information tree */
    switch(indexNode->nodeKind) 
	{
        case INDEXINFO_OPERATOR: /* index ANDing/ORing operator node */
            OOSQL_ERR(eNOTIMPLEMENTED_BTREEINDEX_ANDOR_OOSQL);

        case INDEXINFO_SCAN: /* index scan node */
            e = execBtreeIndexScan(indexInfo, oid, cursor);
            if(e < eNOERROR) 
                OOSQL_ERR(e);

            return(e);

        default:        /* invalid node kind */
            OOSQL_ERR(eINVALID_INDEXINFONODE_OOSQL);
    }
}

#ifndef SLIMDOWN_TEXTIR
/* 
   IMPORTANT:
     You must use TEXTIR_DOCID_* style macros when comparing logicalIds.
                  ^^^^^^^^^^^^^^^^^^^^^^^^^^^
     Otherwise, backward scan is not working.
*/

inline Four OOSQL_Evaluator::execTextIndexInfoNode(
    AP_IndexInfoPoolIndex		indexInfo,			// IN
	OOSQL_TextIR_PostingQueue&	postingQueue,		// INOUT
	Four						logicalIdHints		// IN   
)
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
    AP_IndexInfoElement*		indexNode;
    Four						e;
	OOSQL_TextIR_Posting*		op1Posting;
	OOSQL_TextIR_Posting*		op2Posting;
	OOSQL_TextIR_Posting*		currPosting;
	OOSQL_ScanInfo*				scanInfo;
	OOSQL_ScanInfo*				scanInfo1;
	OOSQL_ScanInfo*				scanInfo2;
	OOSQL_TextIR_Weight			weight;
	OOSQL_TextIR_Bonus			bonus;

	scanInfo = &EVAL_INDEX_SCANINFOTABLEELEMENTS[indexInfo.startIndex];
	OOSQL_TextIR_PostingQueue&	op1PostingQueue = scanInfo->op1PostingQueue;
	OOSQL_TextIR_PostingQueue&	op2PostingQueue = scanInfo->op2PostingQueue;

    /* check input paramter */
#ifdef  OOSQL_DEBUG
    if( IS_NULL_POOLINDEX(indexInfo) == SM_TRUE )
        OOSQL_ERR(eBADPARAMETER_OOSQL);
#endif
    /* NOTE: logicalId can be NULL pointer. We will check it later */

    /* get index info. node for the current node */
    indexNode = ACCESSPLAN.getIndexInfoElem( indexInfo );
#ifdef  OOSQL_DEBUG
    if(indexNode == NULL) OOSQL_ERR(eNULL_POINTER_OOSQL);
#endif

    /* traverse index information tree */
    switch(indexNode->nodeKind) 
	{
    case INDEXINFO_OPERATOR:        /* index ANDing/ORing operator node */
        switch(indexNode->oper.operatorID) 
		{
        case OP_INDEX_AND:
			scanInfo1 = &EVAL_INDEX_SCANINFOTABLEELEMENTS[indexNode->oper.op1.startIndex];
			scanInfo2 = &EVAL_INDEX_SCANINFOTABLEELEMENTS[indexNode->oper.op2.startIndex];

			while(op1PostingQueue.IsEmpty() || op2PostingQueue.IsEmpty())
			{

				// determine logicalIdHints
				if(op1PostingQueue.IsEmpty() && !op2PostingQueue.IsEmpty())
    				logicalIdHints = op2PostingQueue.Head().logicalDocId;
				else if(!op1PostingQueue.IsEmpty() && op2PostingQueue.IsEmpty())
    				logicalIdHints = op1PostingQueue.Head().logicalDocId;
				else if(!op1PostingQueue.IsEmpty() && !op2PostingQueue.IsEmpty())
					/* Use the greater logicalId as logicalIdHints */
					/* Example
					     op1PostingQueue [1050,1060,1070,...,1300]
					     op2PostingQueue [1500,1510,1520,...,1700]
					     Since logicalIdHints = 1500, we can read the postings having logicalId >= 1500.
					 */
					if (TEXTIR_DOCID_GT(op1PostingQueue.Head().logicalDocId,op2PostingQueue.Head().logicalDocId,m_compareMode))	
    					logicalIdHints = op1PostingQueue.Head().logicalDocId;
					else
    					logicalIdHints = op2PostingQueue.Head().logicalDocId;

				// read the first operand
				while(op1PostingQueue.IsEmpty())
				{
					e = execTextIndexInfoNode(indexNode->oper.op1, op1PostingQueue, logicalIdHints);
					if(e < eNOERROR) OOSQL_ERR(e);
					if(e == TEXTIR_EOS)
					{
						if(postingQueue.IsEmpty())
							return TEXTIR_EOS;
						else
							return eNOERROR;
					}
				}

				/* We first read logicalIds from the first operand, and then,
				   use these logicalIds for reading the next operand. */
				if(!op1PostingQueue.IsEmpty())    
				{     
					/* Example
					     op1PostingQueue [1050,1060,1070,1080,...,1300]
					     Case 1: if logicalIdHints has not been computed, then logicalIdHints = 1050;
					             that is, read the postings having logicalId >= 1050 from the next operand.
					     Case 2: if logicalIdHints is 1080, then logicalIdHints = 1080;
					             that is, read the postings having logicalId >= 1080 from the next operand.
					 */
					if(logicalIdHints == TEXTIR_POSTINGSKIP_NO_HINTS)      
						logicalIdHints = op1PostingQueue.Head().logicalDocId;     
					else      
						logicalIdHints = TEXTIR_DOCID_MAX(op1PostingQueue.Head().logicalDocId, logicalIdHints, m_compareMode);    
				}
				
				// read the second operand
				while(op2PostingQueue.IsEmpty())
				{
					e = execTextIndexInfoNode(indexNode->oper.op2, op2PostingQueue, logicalIdHints);
					if(e < eNOERROR) OOSQL_ERR(e);
					if(e == TEXTIR_EOS)
					{
						if(postingQueue.IsEmpty())
							return TEXTIR_EOS;
						else
							return eNOERROR;
					}
				}

				/* fix the below lines to use TEXTIR_DOCID_LT macro. Otherwise, backward scan is not working. */
				if(TEXTIR_DOCID_LT(op1PostingQueue.Tail().logicalDocId,op2PostingQueue.Head().logicalDocId,m_compareMode))
					op1PostingQueue.MakeEmpty();
				else if(TEXTIR_DOCID_LT(op2PostingQueue.Tail().logicalDocId,op1PostingQueue.Head().logicalDocId,m_compareMode))
					op2PostingQueue.MakeEmpty();
			}


			// read the first operand
			op1Posting = &op1PostingQueue.Head();
			op1PostingQueue.PopHead();
			if(scanInfo1->fnMatchResult)
#if defined (EXTENDED_BOOLEAN_MODEL)
				scanInfo1->fnMatchResult->data.d = op1Posting->weight;
#elif defined (HEURISTIC_MODEL)
				scanInfo1->fnMatchResult->data.d = op1Posting->weight + op1Posting->bonus;
#endif

			// read the second operand
			op2Posting = &op2PostingQueue.Head();
			op2PostingQueue.PopHead();
			if(scanInfo2->fnMatchResult)
#if defined (EXTENDED_BOOLEAN_MODEL)
				scanInfo2->fnMatchResult->data.d = op2Posting->weight;
#elif defined (HEURISTIC_MODEL)
				scanInfo2->fnMatchResult->data.d = op2Posting->weight + op2Posting->bonus;
#endif

			/* skip either of the two operands until the two OOSQL_StorageManager::OID are equal */
			while(TEXTIR_DOCID_NE(op1Posting->logicalDocId,op2Posting->logicalDocId,m_compareMode))
			{
				if(TEXTIR_DOCID_LT(op1Posting->logicalDocId,op2Posting->logicalDocId,m_compareMode))
				{
					if(TEXTIR_DOCID_DIFF(op2Posting->logicalDocId,op1Posting->logicalDocId,m_compareMode) > TEXTIR_POSTINGSKIP_THRESHOLD)
						logicalIdHints = op2Posting->logicalDocId;
					else
						logicalIdHints = TEXTIR_POSTINGSKIP_NO_HINTS;


					/* skip the first operand */
					op1Posting = &op1PostingQueue.Head();
					if(op1PostingQueue.PopHead() == false)
					{
						op2PostingQueue.PushHead();
						return eNOERROR;
					}
					if(scanInfo1->fnMatchResult)
#if defined (EXTENDED_BOOLEAN_MODEL)
						scanInfo1->fnMatchResult->data.d = op1Posting->weight;
#elif defined (HEURISTIC_MODEL)
						scanInfo1->fnMatchResult->data.d = op1Posting->weight + op1Posting->bonus;
#endif
				}
				else 
				{
					if(TEXTIR_DOCID_DIFF(op1Posting->logicalDocId,op2Posting->logicalDocId,m_compareMode) > TEXTIR_POSTINGSKIP_THRESHOLD)
						logicalIdHints = op1Posting->logicalDocId;
					else
						logicalIdHints = TEXTIR_POSTINGSKIP_NO_HINTS;

					/* skip the second operand */
					op2Posting = &op2PostingQueue.Head();
					if(op2PostingQueue.PopHead() == false)
					{
						op1PostingQueue.PushHead();
						return eNOERROR;
					}
					if(scanInfo2->fnMatchResult)
#if defined (EXTENDED_BOOLEAN_MODEL)
						scanInfo2->fnMatchResult->data.d = op2Posting->weight;
#elif defined (HEURISTIC_MODEL)
						scanInfo2->fnMatchResult->data.d = op2Posting->weight + op2Posting->bonus;
#endif
				}
			}
			// calculate the minimum weight of the two postings
#if defined (EXTENDED_BOOLEAN_MODEL)
			if(op1Posting->weight < op2Posting->weight) 
				weight = op1Posting->weight;
			else 
				weight = op2Posting->weight;
#elif defined (HEURISTIC_MODEL)
				weight = op1Posting->weight + op2Posting->weight;
				bonus  = op1Posting->bonus + op2Posting->bonus;
#endif

			/* add to result postingQueue */
			postingQueue.PushTail();
			currPosting = &postingQueue.Tail();

			/* set the current posting */
#if defined (EXTENDED_BOOLEAN_MODEL)
			e = execTextIR_setPosting(currPosting, op1Posting, weight);
#elif defined (HEURISTIC_MODEL)
			e = execTextIR_setPosting(currPosting, op1Posting, weight, bonus);
#endif
			if(e < eNOERROR) OOSQL_ERR(e);

            break;

        case OP_INDEX_OR:
			scanInfo1 = &EVAL_INDEX_SCANINFOTABLEELEMENTS[indexNode->oper.op1.startIndex];
			scanInfo2 = &EVAL_INDEX_SCANINFOTABLEELEMENTS[indexNode->oper.op2.startIndex];

			if(op1PostingQueue.IsEmpty() || op2PostingQueue.IsEmpty())
    			logicalIdHints = TEXTIR_POSTINGSKIP_NO_HINTS;
			else                
				if (TEXTIR_DOCID_LT(op1PostingQueue.Head().logicalDocId,op2PostingQueue.Head().logicalDocId,m_compareMode))
    				logicalIdHints = op1PostingQueue.Head().logicalDocId;
				else	
    				logicalIdHints = op2PostingQueue.Head().logicalDocId;

			// read the first operand
			while(op1PostingQueue.IsEmpty())
			{
				e = execTextIndexInfoNode(indexNode->oper.op1, op1PostingQueue, logicalIdHints);
				if(e < eNOERROR) OOSQL_ERR(e);
				if(e == TEXTIR_EOS) break;
			}

			// read the second operand
			while(op2PostingQueue.IsEmpty())
			{
				e = execTextIndexInfoNode(indexNode->oper.op2, op2PostingQueue, logicalIdHints);
				if(e < eNOERROR) OOSQL_ERR(e);
				if(e == TEXTIR_EOS) break;
			}

			if(op1PostingQueue.IsEmpty() && op2PostingQueue.IsEmpty())
				return TEXTIR_EOS;
			else if(op1PostingQueue.IsEmpty()) 
			{
				if(scanInfo1->fnMatchResult)
					scanInfo1->fnMatchResult->data.d = 0;

				// process tuple-by-tuple to avoid overriding a weight value
				if(!op2PostingQueue.IsEmpty())
				{
					/* get next posting */
					op2Posting = &op2PostingQueue.Head();
					op2PostingQueue.PopHead();
					if(scanInfo2->fnMatchResult)
#if defined (EXTENDED_BOOLEAN_MODEL)
						scanInfo2->fnMatchResult->data.d = op2Posting->weight;
#elif defined (HEURISTIC_MODEL)
						scanInfo2->fnMatchResult->data.d = op2Posting->weight + op2Posting->bonus;
#endif

					/* add to result postingQueue */
					postingQueue.PushTail();
					currPosting = &postingQueue.Tail();

					/* in case op1 is null, make 'result' as op2 */
#if defined (EXTENDED_BOOLEAN_MODEL)
					e = execTextIR_setPosting(currPosting, op2Posting, op2Posting->weight);
#elif defined (HEURISTIC_MODEL)
					e = execTextIR_setPosting(currPosting, op2Posting, op2Posting->weight, op2Posting->bonus);
#endif
					if(e < eNOERROR) OOSQL_ERR(e);
				}

				return eNOERROR;
			}
			else if(op2PostingQueue.IsEmpty()) 
			{
				if(scanInfo2->fnMatchResult)
					scanInfo2->fnMatchResult->data.d = 0;

				// process tuple-by-tuple to avoid overriding a weight value
				if(!op1PostingQueue.IsEmpty())
				{
					/* get next posting */
					op1Posting = &op1PostingQueue.Head();
					op1PostingQueue.PopHead();	
					if(scanInfo1->fnMatchResult)
#if defined (EXTENDED_BOOLEAN_MODEL)
						scanInfo1->fnMatchResult->data.d = op1Posting->weight;
#elif defined (HEURISTIC_MODEL)
						scanInfo1->fnMatchResult->data.d = op1Posting->weight + op1Posting->bonus;
#endif

					/* add to result postingQueue */
					postingQueue.PushTail();
					currPosting = &postingQueue.Tail();

					/* in case op2 is null, make 'result' as op1 */
#if defined (EXTENDED_BOOLEAN_MODEL)
					e = execTextIR_setPosting(currPosting, op1Posting, op1Posting->weight);
#elif defined (HEURISTIC_MODEL)
					e = execTextIR_setPosting(currPosting, op1Posting, op1Posting->weight, op1Posting->bonus);
#endif
					if(e < eNOERROR) OOSQL_ERR(e);
				}

				return eNOERROR;
			}

			op1Posting = &op1PostingQueue.Head();
			if(op1PostingQueue.IsEmpty())
				TEXTIR_SET_NULLPOSTING(op1Posting);
			if(scanInfo1->fnMatchResult)
#if defined (EXTENDED_BOOLEAN_MODEL)
				scanInfo1->fnMatchResult->data.d = op1Posting->weight;
#elif defined (HEURISTIC_MODEL)
				scanInfo1->fnMatchResult->data.d = op1Posting->weight + op1Posting->bonus;
#endif

			// read the second operand 
			op2Posting = &op2PostingQueue.Head();
			if(op2PostingQueue.IsEmpty())
				TEXTIR_SET_NULLPOSTING(op2Posting);
			if(scanInfo2->fnMatchResult)
#if defined (EXTENDED_BOOLEAN_MODEL)
				scanInfo2->fnMatchResult->data.d = op2Posting->weight;
#elif defined (HEURISTIC_MODEL)
				scanInfo2->fnMatchResult->data.d = op2Posting->weight + op2Posting->bonus;
#endif

			/* check if end of scan */
			if(TEXTIR_IS_NULLPOSTING(op1Posting) || TEXTIR_IS_NULLPOSTING(op2Posting))
				return eNOERROR;

			/* do union of the two postings */
			if(TEXTIR_DOCID_EQ(op1Posting->logicalDocId,op2Posting->logicalDocId,m_compareMode))
			{
				/* add to result postingQueue */
				postingQueue.PushTail();
				currPosting = &postingQueue.Tail();

				op1PostingQueue.PopHead();
				if(scanInfo1->fnMatchResult)
#if defined (EXTENDED_BOOLEAN_MODEL)
					scanInfo1->fnMatchResult->data.d = op1Posting->weight;
#elif defined (HEURISTIC_MODEL)
					scanInfo1->fnMatchResult->data.d = op1Posting->weight + op1Posting->bonus;
#endif

				op2PostingQueue.PopHead();
				if(scanInfo2->fnMatchResult)
#if defined (EXTENDED_BOOLEAN_MODEL)
					scanInfo2->fnMatchResult->data.d = op2Posting->weight;
#elif defined (HEURISTIC_MODEL)
					scanInfo2->fnMatchResult->data.d = op2Posting->weight + op2Posting->bonus;
#endif

#if defined (EXTENDED_BOOLEAN_MODEL)
				/* calculate the maximum weight of the two postings */
				if(op1Posting->weight < op2Posting->weight)
					weight = op2Posting->weight;
				else
					weight = op1Posting->weight;
#elif defined (HEURISTIC_MODEL)
				weight = op1Posting->weight + op2Posting->weight;
				bonus  = op1Posting->bonus + op2Posting->bonus;
#endif

				/* set result posting */
#if defined (EXTENDED_BOOLEAN_MODEL)
				e = execTextIR_setPosting(currPosting, op1Posting, weight);
#elif defined (HEURISTIC_MODEL)
				e = execTextIR_setPosting(currPosting, op1Posting, weight, bonus);
#endif
				if(e < eNOERROR) OOSQL_ERR(e);
			}
			else if(TEXTIR_DOCID_LT(op1Posting->logicalDocId,op2Posting->logicalDocId,m_compareMode))
			{
				/* add to result postingQueue */
				postingQueue.PushTail();
				currPosting = &postingQueue.Tail();

				op1PostingQueue.PopHead();
				if(scanInfo1->fnMatchResult)
#if defined (EXTENDED_BOOLEAN_MODEL)
					scanInfo1->fnMatchResult->data.d = op1Posting->weight;
#elif defined (HEURISTIC_MODEL)
					scanInfo1->fnMatchResult->data.d = op1Posting->weight + op1Posting->bonus;
#endif
				if(scanInfo2->fnMatchResult)
					scanInfo2->fnMatchResult->data.d = 0;

				/* set result posting */
				weight = op1Posting->weight;
#if defined (EXTENDED_BOOLEAN_MODEL)
				e = execTextIR_setPosting(currPosting, op1Posting, weight);
#elif defined (HEURISTIC_MODEL)
				bonus  = op1Posting->bonus;
				e = execTextIR_setPosting(currPosting, op1Posting, weight, bonus);
#endif
				if(e < eNOERROR) OOSQL_ERR(e);
			}
			else 
			{	/* op1Posting->logicalDocId > op2Posting->logicalDocId */
				/* add to result postingQueue */
				postingQueue.PushTail();
				currPosting = &postingQueue.Tail();

				op2PostingQueue.PopHead();
				if(scanInfo2->fnMatchResult)
#if defined (EXTENDED_BOOLEAN_MODEL)
					scanInfo2->fnMatchResult->data.d = op2Posting->weight;
#elif defined (HEURISTIC_MODEL)
					scanInfo2->fnMatchResult->data.d = op2Posting->weight + op2Posting->bonus;
#endif
				if(scanInfo1->fnMatchResult)
					scanInfo1->fnMatchResult->data.d = 0;

				/* set result posting */
				weight = op2Posting->weight;
#if defined (EXTENDED_BOOLEAN_MODEL)
				e = execTextIR_setPosting(currPosting, op2Posting, weight);
#elif defined (HEURISTIC_MODEL)
				bonus  = op2Posting->bonus;
				e = execTextIR_setPosting(currPosting, op2Posting, weight, bonus);
#endif
				if(e < eNOERROR) OOSQL_ERR(e);
			}	
            break;

        default:        /* invalid operator */
            OOSQL_ERR(eINVALID_CASE_OOSQL);
        }
        break;

    case INDEXINFO_SCAN:    /* index scan node */
		while(postingQueue.IsEmpty())
		{
			e = execTextIR_IndexScan(indexInfo, postingQueue, logicalIdHints);
	        if(e < eNOERROR) OOSQL_ERR(e);
			if(e == TEXTIR_EOS)
			{
				if(postingQueue.IsEmpty())
					return TEXTIR_EOS;
				else
					return eNOERROR;
			}
		}
        break;

    default:        /* invalid node kind */
        OOSQL_ERR(eINVALID_INDEXINFONODE_OOSQL);
    }

    /* return */
    return eNOERROR;
}

// This function is the reverse operation of adjustWeightInfo().
// See the adjustWeightInfo().
inline Four OOSQL_Evaluator::restoreWeightInfo(AP_IndexInfoPoolIndex indexInfo)
{
	AP_IndexInfoElement*		indexNode;
	OOSQL_TextIR_Posting*		op1Posting;
	OOSQL_TextIR_Posting*		op2Posting;
	OOSQL_ScanInfo*				scanInfo;
	OOSQL_ScanInfo*				scanInfo1;
	OOSQL_ScanInfo*				scanInfo2;
	Four						e;
	
	scanInfo = &EVAL_INDEX_SCANINFOTABLEELEMENTS[indexInfo.startIndex];
	OOSQL_TextIR_PostingQueue&	op1PostingQueue = scanInfo->op1PostingQueue;
	OOSQL_TextIR_PostingQueue&	op2PostingQueue = scanInfo->op2PostingQueue;
	
	/* get index info. node for the current node */
	indexNode = ACCESSPLAN.getIndexInfoElem(indexInfo);

	switch(indexNode->nodeKind)
	{
	case INDEXINFO_OPERATOR:
		scanInfo1 = &EVAL_INDEX_SCANINFOTABLEELEMENTS[indexNode->oper.op1.startIndex];
		scanInfo2 = &EVAL_INDEX_SCANINFOTABLEELEMENTS[indexNode->oper.op2.startIndex];

		op1Posting = &op1PostingQueue.PreviousHead();
		op2Posting = &op2PostingQueue.PreviousHead();

		if(scanInfo1->fnMatchResult) {	/* if this node is a leaf in the query plan */
			if(scanInfo1->fnMatchResult->nullFlag) {
				scanInfo1->fnMatchResult->nullFlag = SM_FALSE;
#if defined (EXTENDED_BOOLEAN_MODEL)
				scanInfo1->fnMatchResult->data.d = (op1PostingQueue.IsUsed()?op1Posting->weight:0.0);
#elif defined (HEURISTIC_MODEL)
				scanInfo1->fnMatchResult->data.d = (op1PostingQueue.IsUsed()?op1Posting->weight + op1Posting->bonus:0.0);
#endif
			}
		}
		e = restoreWeightInfo(indexNode->oper.op1);
		if (e < eNOERROR) OOSQL_ERR(e);

		if(scanInfo2->fnMatchResult) {	/* if this node is a leaf in the query plan */
			if(scanInfo2->fnMatchResult->nullFlag) {
				scanInfo2->fnMatchResult->nullFlag = SM_FALSE;
#if defined (EXTENDED_BOOLEAN_MODEL)
				scanInfo2->fnMatchResult->data.d = (op2PostingQueue.IsUsed()?op2Posting->weight:0.0);
#elif defined (HEURISTIC_MODEL)
				scanInfo2->fnMatchResult->data.d = (op2PostingQueue.IsUsed()?op2Posting->weight + op2Posting->bonus:0.0);
#endif
			}
		}
		e = restoreWeightInfo(indexNode->oper.op2);
		if (e < eNOERROR) OOSQL_ERR(e);

		break;
	
	case INDEXINFO_SCAN:
		/* do nothing */
		break;

	default:		/* invalid node kind */
		OOSQL_ERR(eINVALID_INDEXINFONODE_OOSQL);
	}

	return eNOERROR;
}

inline Four OOSQL_Evaluator::adjustWeightInfo(AP_IndexInfoPoolIndex indexInfo, OOSQL_TextIR_Posting* posting)
{
	AP_IndexInfoElement*		indexNode;
	OOSQL_TextIR_Posting*		op1Posting;
	OOSQL_TextIR_Posting*		op2Posting;
	OOSQL_ScanInfo*				scanInfo;
	OOSQL_ScanInfo*				scanInfo1;
	OOSQL_ScanInfo*				scanInfo2;
	Four						op1PostingLogicalDocId;
	Four						op2PostingLogicalDocId;
	Four						e;
	
	scanInfo = &EVAL_INDEX_SCANINFOTABLEELEMENTS[indexInfo.startIndex];
	OOSQL_TextIR_PostingQueue&	op1PostingQueue = scanInfo->op1PostingQueue;
	OOSQL_TextIR_PostingQueue&	op2PostingQueue = scanInfo->op2PostingQueue;
	
	/* get index info. node for the current node */
	indexNode = ACCESSPLAN.getIndexInfoElem(indexInfo);

	switch(indexNode->nodeKind)
	{
	case INDEXINFO_OPERATOR:
		scanInfo1 = &EVAL_INDEX_SCANINFOTABLEELEMENTS[indexNode->oper.op1.startIndex];
		scanInfo2 = &EVAL_INDEX_SCANINFOTABLEELEMENTS[indexNode->oper.op2.startIndex];

		op1Posting = &op1PostingQueue.PreviousHead();
		op2Posting = &op2PostingQueue.PreviousHead();

		if (op1PostingQueue.IsUsed()) op1PostingLogicalDocId = op1Posting->logicalDocId;
		else op1PostingLogicalDocId = NIL;

		if (op2PostingQueue.IsUsed()) op2PostingLogicalDocId = op2Posting->logicalDocId;
		else op2PostingLogicalDocId = NIL;

		/* a posting has been popped from op1 and op2 */
		if(TEXTIR_DOCID_EQ(posting->logicalDocId,op1PostingLogicalDocId,m_compareMode) &&
		   TEXTIR_DOCID_EQ(posting->logicalDocId,op2PostingLogicalDocId,m_compareMode))
		{
			/* do nothing */
		}
		/* a posting has been popped from op1 */
		else if(TEXTIR_DOCID_EQ(posting->logicalDocId,op1PostingLogicalDocId,m_compareMode))
		{
			if(scanInfo2->fnMatchResult) {	/* if this node is a leaf in the query plan */
				scanInfo2->fnMatchResult->nullFlag = SM_TRUE;
				scanInfo2->fnMatchResult->data.d = 0.0;
			}
		}
		/* a posting has been popped from op2 */
		else if(TEXTIR_DOCID_EQ(posting->logicalDocId,op2PostingLogicalDocId,m_compareMode))
		{
			if(scanInfo1->fnMatchResult) {	/* if this node is a leaf in the query plan */
				scanInfo1->fnMatchResult->nullFlag = SM_TRUE;
				scanInfo1->fnMatchResult->data.d = 0.0;
			}
		}
		/* no posting has been popped from op1 and op2 */
		else
		{
			if(scanInfo1->fnMatchResult) {	/* if this node is a leaf in the query plan */
				scanInfo1->fnMatchResult->nullFlag = SM_TRUE;
				scanInfo1->fnMatchResult->data.d = 0.0;
			}
			if(scanInfo2->fnMatchResult) {	/* if this node is a leaf in the query plan */
				scanInfo2->fnMatchResult->nullFlag = SM_TRUE;
				scanInfo2->fnMatchResult->data.d = 0.0;
			}
		}

		e = adjustWeightInfo(indexNode->oper.op1, posting);
		if (e < eNOERROR) OOSQL_ERR(e);

		e = adjustWeightInfo(indexNode->oper.op2, posting);
		if (e < eNOERROR) OOSQL_ERR(e);

		break;
	
	case INDEXINFO_SCAN:
		/* do nothing */
		break;

	default:		/* invalid node kind */
		OOSQL_ERR(eINVALID_INDEXINFONODE_OOSQL);
	}

	return eNOERROR;
}

#else /* SLIMDOWN_TEXTIR */

inline Four OOSQL_Evaluator::execTextIndexInfoNode(
    AP_IndexInfoPoolIndex       indexInfo,          // IN
    OOSQL_TextIR_PostingQueue&  postingQueue,       // INOUT 
    Four                        logicalIdHints      // IN   
)
{
    return eTEXTIR_NOTENABLED_OOSQL;
}

inline Four OOSQL_Evaluator::restoreWeightInfo(AP_IndexInfoPoolIndex indexInfo)
{
    return eTEXTIR_NOTENABLED_OOSQL;
}

inline Four OOSQL_Evaluator::adjustWeightInfo(AP_IndexInfoPoolIndex indexInfo, OOSQL_TextIR_Posting* posting)
{
    return eTEXTIR_NOTENABLED_OOSQL;
}

#endif /* SLIMDOWN_TEXTIR */
#endif // #define __OOSQL_EVAL_ACCESS_HXX__

