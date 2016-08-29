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
        oosql_Eval_EvalBuffer.cxx

    DESCRIPTION:

    IMPORTS:

    EXPORTS:
*/

#include "OOSQL_Evaluator.hxx"
#include "OOSQL_SortStream.hxx"
#include <string.h>

Four    OOSQL_Evaluator::prepareEvalBuffer()
/*
    Function:

    Side effect:

    Return value:
*/
{
        Four    numCols;        // the # of columns of eval. buffer for an access plan elem.
        Four    strSize;        // memory size for string and varialbe string of eval. buffer
                                // for an access plan elem.
        Boolean useDualBuf;
        Four    i;
        Four    e;


		
        /* allocate evaluation buffer
         * 
         * NOTE: We allocate memory for string(including variable-length string) at one time. 
         *      And then we set the pointer(s) to the memory for each string column.
         */

		OOSQL_ARRAYNEW(m_evaluationBuffer, pMemoryManager, OOSQL_EvalBuffer, ACCESSPLAN.getNumAP_Elem());
		if(m_evaluationBuffer == NULL) OOSQL_ERR(eOUTOFMEMORY_OOSQL);

        for(i = 0; i < ACCESSPLAN.getNumAP_Elem(); i++) {

            // calculate the # of necessary buffer slot for the current access plan element
            e = calcEvalBufSize(i, numCols, strSize);
            if(e < eNOERROR)
                OOSQL_ERR(e);

            /* allocate memory for the current access plan element
             * NOTE: Dual buffer is used to process distinct flag in select clause efficiently,
             *       so if 'selDistinctFlag' is SM_TRUE, we use dual buffer scheme for evaluation buffer.
            */
            useDualBuf =(ACCESSPLANELEMENTS[i].selDistinctFlag == SM_TRUE)? SM_TRUE: SM_FALSE;
			EVAL_EVALBUFFER[i].init(numCols, ACCESSPLANELEMENTS[i].nGrpByKeys, GET_POOLSIZE(ACCESSPLANELEMENTS[i].aggrFuncInfo), ACCESSPLANELEMENTS[i].nUsedFuncMatch, strSize, useDualBuf);

            // fill column information of OOSQL_StorageManager::ColListStruct for evaluation buffer
            e = consColListStructForEvalBuf(i, &EVAL_EVALBUFFER[i]);
            if(e < eNOERROR) OOSQL_ERR(e);
        }

        return eNOERROR;
}


Four    OOSQL_Evaluator::calcEvalBufSize(
    Four planNo,        /* IN: index to the access plan element */
    Four &numCols,      /* OUT: the # of columns of eval. buffer for the A.P. element */
    Four &strSize       /* OUT: memory size for string and variable string for the A.P. element */
)
/*
    Function:
        calculate the evaluation buffer size for the 'planNo'-th access plan element

    Side effect:

    Return value:
        >= 0            # of OOSQL_StorageManager::ColListStruct
        < eNOERROR      error code
*/
{
    Four nUsedCols;
    Four nUsedMethods;
    AP_UsedColPoolElements pUsedColPool;
    AP_UsedMethodPoolElements pUsedMethodPool;
    AP_AggrFuncPoolElements aggrFuncPool;
    Four grpByCol;
    Four i;
    Four e;


    /* 
     * calculate the evaluation buffer size for used columns
     */

    /* get the # of used columns */
    nUsedCols = GET_POOLSIZE(ACCESSPLANELEMENTS[planNo].usedColInfo);

    /* set pointers for accessing column and method information */
    pUsedColPool = ACCESSPLAN.getUsedColPool(planNo);

    /* initialize the eval. buffer size */
    numCols = 0;
    strSize = 0; 
    for(i = 0; i < nUsedCols; i++) 
	{
        switch(pUsedColPool[i].typeId) 
		{
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
            /* reserved additional one byte to guarantee a string null-terminated */
			if(pUsedColPool[i].length < OOSQL_EVALBUFFER_MAXSTRINGSIZE)
				strSize += pUsedColPool[i].length + 1;
			else
				strSize += OOSQL_EVALBUFFER_MAXSTRINGSIZE;

			numCols ++;
            break;
        default:
			numCols ++;
            break;
        }
    }

    /* 
     * calculate the evaluation buffer size for used methods
     */

    /* get the # of used methods */
    nUsedMethods = GET_POOLSIZE(ACCESSPLANELEMENTS[planNo].usedMethodInfo);

    // set pointers for accessing method information
    pUsedMethodPool = ACCESSPLAN.getUsedMethodPool(planNo);

    for(i = 0; i < nUsedMethods; i++) 
	{
        switch(pUsedMethodPool[i].returnType) 
		{
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
            /* reserved additional one byte to guarantee a string null-terminated */
			if(pUsedMethodPool[i].returnLength < OOSQL_EVALBUFFER_MAXSTRINGSIZE)
				strSize += pUsedMethodPool[i].returnLength + 1;
			else
				strSize += OOSQL_EVALBUFFER_MAXSTRINGSIZE;
            break;
        default:
            break;
        }
    }
    numCols += nUsedMethods;

    /*
     * calculate string buffer size for group by keys
     */
    for(i = 0; i < ACCESSPLANELEMENTS[planNo].nGrpByKeys; i++) 
	{
        grpByCol = ACCESSPLANELEMENTS[planNo].grpByKeys[i];
        switch(pUsedColPool[grpByCol].typeId) 
		{
		case OOSQL_TYPE_TEXT:
			strSize +=(OOSQL_EVALBUFFER_MAXSTRINGSIZE + 1) * 2;
			break;
        case OOSQL_TYPE_string:
        case OOSQL_TYPE_varstring:
            /* reserved additional one byte to guarantee a string null-terminated */
			if(pUsedColPool[grpByCol].length < OOSQL_EVALBUFFER_MAXSTRINGSIZE)
				strSize +=(pUsedColPool[grpByCol].length + 1) * 2;
			else
				strSize +=(OOSQL_EVALBUFFER_MAXSTRINGSIZE + 1) * 2;
            break;
        default:
            break;
        }
    }

    /*
     * calculate string buffer size for aggregate function
     */
    Four        resultType;
    Four        resultLength;

    aggrFuncPool = ACCESSPLAN.getAggrFuncPool(planNo);
    /* calculate the evaluation buffer size for aggregate functions
     * NOTE: aggregate function results are stored in the eval. buffer
     */
    for(i = 0; i < GET_POOLSIZE(aggrFuncPool); i++) 
	{
        switch(aggrFuncPool[i].aggrFunctionID) 
		{
        case AGGRFUNC_MAX:
        case AGGRFUNC_MIN:
            e = getArgumentInfo(&(aggrFuncPool[i].argument), &resultType, &resultLength);
            if(e < eNOERROR)
                OOSQL_ERR(e);

            switch(resultType) {
                case OOSQL_TYPE_string:
                case OOSQL_TYPE_varstring:
                    /* reserved additional one byte to guarantee a string null-terminated */
					if(resultLength < OOSQL_EVALBUFFER_MAXSTRINGSIZE)
						strSize += resultLength;
					else
						strSize += OOSQL_EVALBUFFER_MAXSTRINGSIZE;
                    break;

                default:
                    break;
            }
            break;

        default:
            break;
        } /* end of switch */
    } /* end of for */

    return(eNOERROR);
}


Four    OOSQL_Evaluator::consColListStructForEvalBuf(
    Four planNo,                /* IN: index to access plan element */
    OOSQL_EvalBuffer *pEvalBuf  /* IN: ptr. to evaluation buffer */
)
/*
    Function:
        Construct OOSQL_StorageManager::ColListStruct for the evaluation buffer.

    Side effect:

    Return value:
        eNOERROR                if no error
*/
{
    AP_UsedColPoolElements pUsedColPool;				/* ptr. to used column info. pool */
    AP_AggrFuncPoolElements aggrFuncPool;
    Four mappedColNo;									/* mapped column no. */
    Four nCols;											/* the # of columns */
    OOSQL_StorageManager::ColListStruct *clist;			/* ptr. to array of OOSQL_StorageManager::ColListStruct */
    char *pStrBuf;										/* ptr. to string buffer */
    OOSQL_StorageManager::ColListStruct *prevColList;   /* ptr. to array of OOSQL_StorageManager::ColListStruct */
	OOSQL_StorageManager::ColListStruct *grpByColList;	
    Four i;												/* column index */
    Four e;												/* error code */

    // get pointer to column no. map pool and used column info. pool
    pUsedColPool = ACCESSPLAN.getUsedColPool(planNo);

    // get the # of columns for this access plan element
    clist = pEvalBuf->clist;
	grpByColList = pEvalBuf->grpByColList;	
    // set pointer to the start address of preallocated memory for string and variable string
    pStrBuf = pEvalBuf->strBuf;

    // set pointer to the secondary buffer if dual buffering is used
    if(pEvalBuf->isDualBuf == SM_TRUE) {
        prevColList = pEvalBuf->prevColList;
    }

    // construct OOSQL_StorageManager::ColListStruct for used columns
	nCols = GET_POOLSIZE(ACCESSPLANELEMENTS[planNo].colNoMap);	
    for(i = 0; i < nCols; i++) 
	{
        mappedColNo = ACCESSPLAN.getMappedColNo(planNo, i);	

        if(mappedColNo == NOT_USED_COLUMN)   
            continue;										// unused column
		
		// make ColListStruct
        // determine column number to be passed to LOM
        clist[mappedColNo].colNo = pUsedColPool[mappedColNo].colNo;
        clist[mappedColNo].start = 0;
		if(pUsedColPool[mappedColNo].length < OOSQL_EVALBUFFER_MAXSTRINGSIZE)
		{
			clist[mappedColNo].length     = pUsedColPool[mappedColNo].length;
			clist[mappedColNo].dataLength = pUsedColPool[mappedColNo].length;
		}
		else
		{
			clist[mappedColNo].length     = OOSQL_EVALBUFFER_MAXSTRINGSIZE;
			clist[mappedColNo].dataLength = OOSQL_EVALBUFFER_MAXSTRINGSIZE;
		}
        
		clist[mappedColNo].retLength = -1;

        if(pEvalBuf->isDualBuf == SM_TRUE) 
		{
            // make OOSQL_StorageManager::ColListStruct for the secondary buffer
            prevColList[mappedColNo].colNo = pUsedColPool[i].colNo;
            prevColList[mappedColNo].start = 0;
			if(pUsedColPool[mappedColNo].length < OOSQL_EVALBUFFER_MAXSTRINGSIZE)
			{
				prevColList[mappedColNo].length	   = pUsedColPool[mappedColNo].length;
				prevColList[mappedColNo].dataLength = pUsedColPool[mappedColNo].length;
			}
			else
			{
				prevColList[mappedColNo].length	   = OOSQL_EVALBUFFER_MAXSTRINGSIZE;
				prevColList[mappedColNo].dataLength = OOSQL_EVALBUFFER_MAXSTRINGSIZE;
			}
			prevColList[mappedColNo].retLength = -1;
        }

        // if column type is string or variable string, set pointer to memory
        switch(pUsedColPool[mappedColNo].typeId) 
		{
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
            clist[mappedColNo].data.ptr = pStrBuf;

            /* reserved additional one char to guarantee a string null-terminated */
            pStrBuf += clist[mappedColNo].length + 1;

            // added to support dual buffering
            if(pEvalBuf->isDualBuf == SM_TRUE) 
			{
                prevColList[mappedColNo].data.ptr = pStrBuf;
                /* reserved additional one char to guarantee a string null-terminated */
                pStrBuf += prevColList[mappedColNo].length + 1;
            }
            break;

        default:
            break;
        } /* switch */
    } /* end of for */

    /* construct OOSQL_StorageManager::ColListStruct for used methods
     * NOTE: this part should be implemented when
     *  method in the middle of path expression is supported.
     */

    /*
     * construct OOSQL_StorageManager::ColListStruct for group by keys
     */
    Four        colIdx;         // index to OOSQL_StorageManager::ColListStruct
    Four        grpByCol;

    // initialize index to evaluation buffer for group by keys
    colIdx = GET_POOLSIZE(pUsedColPool);

    for(i = 0; i < ACCESSPLANELEMENTS[planNo].nGrpByKeys; i++, colIdx++) {
        grpByCol = ACCESSPLANELEMENTS[planNo].grpByKeys[i];
        clist[colIdx].length = pUsedColPool[grpByCol].length;
		grpByColList[i].length = pUsedColPool[grpByCol].length;		
		grpByColList[i].nullFlag = SM_FALSE;
        switch(pUsedColPool[grpByCol].typeId) {
			case OOSQL_TYPE_TEXT:
				clist[colIdx].data.ptr = pStrBuf;
				pStrBuf += OOSQL_EVALBUFFER_MAXSTRINGSIZE + 1;
				grpByColList[i].data.ptr = pStrBuf;
				pStrBuf += OOSQL_EVALBUFFER_MAXSTRINGSIZE + 1;
                break;
            case OOSQL_TYPE_string:
            case OOSQL_TYPE_varstring:
                clist[colIdx].data.ptr = pStrBuf;
				if(clist[colIdx].length < OOSQL_EVALBUFFER_MAXSTRINGSIZE)
					pStrBuf += clist[colIdx].length + 1;
				else
					pStrBuf += OOSQL_EVALBUFFER_MAXSTRINGSIZE + 1;

				grpByColList[i].data.ptr = pStrBuf;

				if(grpByColList[i].length < OOSQL_EVALBUFFER_MAXSTRINGSIZE)
					pStrBuf += grpByColList[i].length + 1;
				else
					pStrBuf += OOSQL_EVALBUFFER_MAXSTRINGSIZE + 1;
                break;
            default:
                break;
        }
    }

    /* NOTE: We need not construct column information for aggregate functions
     *  because eval. buffer for aggregate functions is used only to store
     *  the aggregate function result in memory.
     */
    Four        resultType;
    Four        resultLength;

    aggrFuncPool = ACCESSPLAN.getAggrFuncPool(planNo);
    // calculate the evaluation buffer size for aggregate functions
    // NOTE: aggregate function results are stored in the eval. buffer
    for(i = 0; i < GET_POOLSIZE(aggrFuncPool); i++, colIdx++) {
        switch(aggrFuncPool[i].aggrFunctionID) {
            case AGGRFUNC_MAX:
            case AGGRFUNC_MIN:
                e = getArgumentInfo(&(aggrFuncPool[i].argument), &resultType, &resultLength);
                if(e < eNOERROR) OOSQL_ERR(e);

                switch(resultType) {
                    case OOSQL_TYPE_string:
                    case OOSQL_TYPE_varstring:
                        clist[colIdx].data.ptr = pStrBuf;
                        /* reserved additional one char to guarantee a string null-terminated */
                        pStrBuf += resultLength + 1;
                        break;

                    default:
                        break;
                }
                break;

            default:
                break;
        }
    }

    return eNOERROR;
}


Four    OOSQL_Evaluator::prepareTempFile()
/*
    Function:
        Create temporary file(s) according to temporary file information in the access plan.

    Side effect:

    Return value:
        < eNOERROR      error code
        eNOERROR        if no error
*/
{
    AP_ProjectionListPoolElements   projList;
    AP_AggrFuncPoolElements         aggrFuncList;
    Four                            numTempFile;
    Boolean                         useDualBuf;
    Four                            i, j, k, l;
    Four                            e;              // error code

    /* Initialize max. temporary file number.
     * NOTE: Temporary file no. is assigned as non-negative integer increasingly 
     *     (from 0 to n-1), so n is the max. number of used temporary files.
     */
    numTempFile = 0;
    for(i = 0; i < ACCESSPLAN.getNumAP_Elem(); i++) 
	{
        projList = ACCESSPLAN.getProjectionListPool(i);
        for(j = 0; j < GET_POOLSIZE(projList); j++) 
		{
            if(projList[j].tempFileNum >= 0)
                numTempFile ++;
        }
    }

    // allocate memory for temporary file information
	OOSQL_NEW(m_evalTempFileInfoTable, pMemoryManager, OOSQL_TempFileInfoTable(numTempFile));
	if(m_evalTempFileInfoTable == NULL)
		OOSQL_ERR(eOUTOFMEMORY_OOSQL);
	m_evalTempFileInfoTableElements = m_evalTempFileInfoTable->tempFileInfo;

    // make temp. file information
    for(i = 0; i < ACCESSPLAN.getNumAP_Elem(); i++) 
	{
        // prepare temp. file for class info.
        if(ACCESSPLANELEMENTS[i].classInfo.classKind == CLASSKIND_TEMPORARY || ACCESSPLANELEMENTS[i].classInfo.classKind == CLASSKIND_SORTSTREAM) 
		{
            useDualBuf =(ACCESSPLANELEMENTS[i].selDistinctFlag == SM_TRUE)? SM_TRUE: SM_FALSE;
			if(ACCESSPLANELEMENTS[i].classInfo.classKind == CLASSKIND_SORTSTREAM)
				e = makeTempFileInfo(ACCESSPLANELEMENTS[i].classInfo.tempFileNum, ACCESSPLANELEMENTS[i].usedColInfo, useDualBuf, SM_TRUE);
			else
				e = makeTempFileInfo(ACCESSPLANELEMENTS[i].classInfo.tempFileNum, ACCESSPLANELEMENTS[i].usedColInfo, useDualBuf, SM_FALSE);
            OOSQL_CHECK_ERR(e);

			// find proj list
			for(j = 0; j < ACCESSPLAN.getNumAP_Elem(); j++) 
			{
				projList = ACCESSPLAN.getProjectionListPool(j);
				if(!projList.isNull() && projList[0].tempFileNum == ACCESSPLANELEMENTS[i].classInfo.tempFileNum)
					break;
			}
			if(j == ACCESSPLAN.getNumAP_Elem())
				OOSQL_ERR(eINTERNALERROR_OOSQL);

			e = prepareTempFileOpenIt(ACCESSPLANELEMENTS[i].classInfo.tempFileNum, &(projList[0]));
			OOSQL_CHECK_ERR(e);
        }

        if(IS_NULL_POOLINDEX(ACCESSPLANELEMENTS[i].aggrFuncInfo) == SM_FALSE) 
		{
            aggrFuncList = ACCESSPLAN.getAggrFuncPool(i);

            for(j = 0; j < GET_POOLSIZE(aggrFuncList); j++) 
			{
                if(aggrFuncList[j].distinctFlag == SM_TRUE) 
				{
                    e = makeTempFileInfo(aggrFuncList[j].tempFileNum, aggrFuncList[j].usedColInfo, SM_TRUE, 
							             SM_TRUE); 
                    OOSQL_CHECK_ERR(e);

					// find proj list
					for(k = 0; k < ACCESSPLAN.getNumAP_Elem(); k++) 
					{
						projList = ACCESSPLAN.getProjectionListPool(k);
						for(l = 0; l < projList.size; l++) 
						{
							if(projList[l].tempFileNum == aggrFuncList[j].tempFileNum)
								break;
						}
						if(l < projList.size)
							break;
					}
					if(k == ACCESSPLAN.getNumAP_Elem())
						OOSQL_ERR(eINTERNALERROR_OOSQL);

					e = prepareTempFileOpenIt(aggrFuncList[j].tempFileNum, &projList[l]);
					OOSQL_CHECK_ERR(e);
                }
            }
        }
    }

    return eNOERROR;
}

Four OOSQL_Evaluator::prepareTempFileOpenIt(
	Four						tempFileNum,
	AP_ProjectionListElement*	projList
)
{
	OOSQL_TempFileInfo      *pTempFileInfo;
	Four					e;

    pTempFileInfo = EVAL_TEMPFILEINFOTABLEELEMENTS[tempFileNum];

	if(pTempFileInfo->sortStream)
	{
		e = createSortStream(pTempFileInfo, projList);
		if(e < eNOERROR) OOSQL_ERR(e);

	}
	else
	{
		e = createTempFile(pTempFileInfo);
		OOSQL_CHECK_ERR(e);

		// open the temporary file
		e = m_storageManager->OpenClass(m_sortBufferInfo.diskInfo.sortVolID, pTempFileInfo->name);
		OOSQL_CHECK_ERR(e);
		pTempFileInfo->ocn = e;

		OOSQL_StorageManager::LockParameter       lockup;

		/* NOTE: We give exclusive lock mode because this scan will be used for writing
		 *  intermediate result of query evaluation
		 */
		lockup.mode = OOSQL_StorageManager::L_X;
		lockup.duration = OOSQL_StorageManager::L_COMMIT;

		/* open scan for the temporary
		 * NOTE: This scan will be used for storing intermediate query results
		 *  that are projected according to the projection information of the access plan.
		*/
		e = m_storageManager->OpenSeqScan(pTempFileInfo->ocn, FORWARD, 0, NULL, &lockup);
		OOSQL_CHECK_ERR(e);
		pTempFileInfo->osn = e;
	}

	return eNOERROR;
}

Four    OOSQL_Evaluator::makeTempFileInfo(
        Four                    tempFileNum,			// IN:
        AP_UsedColPoolIndex     usedColPoolIndex,       // IN:
        Boolean                 useDualBuf,				// IN:
		Boolean					useSortStream			// IN:
)
/*
    Function:
        Make temporary file information used for 'planNo'-th access plan element.

    Side effect:
        Column information for the temporary file is constructed from projection information of
        the access plan element.

    Return value:
        < eNOERROR      error code
        eNOERROR        if no error
*/
{
        AP_UsedColPoolElements  usedColInfo;
        OOSQL_TempFileInfo      *pTempFileInfo; // pointer to temporary file information
        Four                    nCols;          // the # of columns of the temp. file
        Four                    strBufSize;     // string buffer size for the temp. file
        char                    *pCurrStrBuf;
        char                    *pCurrStrBuf2;
        Four                    i;              // loop iteration variable

        usedColInfo = ACCESSPLAN.getUsedColPool(usedColPoolIndex);

        nCols = GET_POOLSIZE(usedColInfo);

        // calculate the total length of string and variable string column
        strBufSize = 0;
        for(i = 0; i < nCols; i++) {
            if(usedColInfo[i].typeId == OOSQL_TYPE_STRING    || 
			   usedColInfo[i].typeId == OOSQL_TYPE_VARSTRING ||
			   usedColInfo[i].typeId == OOSQL_TYPE_TEXT)
			{
				if(usedColInfo[i].length < OOSQL_EVALBUFFER_MAXSTRINGSIZE)
					strBufSize += usedColInfo[i].length + 1;
				else
					strBufSize += OOSQL_EVALBUFFER_MAXSTRINGSIZE;
            }
        }

        // construct new OOSQL_TempFileInfo
		OOSQL_NEW(EVAL_TEMPFILEINFOTABLE.tempFileInfo[tempFileNum], pMemoryManager, OOSQL_TempFileInfo(nCols, strBufSize, useDualBuf));
		if(EVAL_TEMPFILEINFOTABLE.tempFileInfo[tempFileNum] == NULL)
                OOSQL_ERR(eOUTOFMEMORY_OOSQL);

        // set pointer to the 'tempFileNum'-th temp. file info
        pTempFileInfo = EVAL_TEMPFILEINFOTABLEELEMENTS[tempFileNum];
		if(useSortStream)
			OOSQL_NEW(pTempFileInfo->sortStream, pMemoryManager, OOSQL_SortStream(m_storageManager));
		else
			pTempFileInfo->sortStream = NULL;

        for(i = 0; i < pTempFileInfo->nCols; i++) {
            // make column information
			pTempFileInfo->attrInfo[i].complexType = SM_COMPLEXTYPE_BASIC;
            pTempFileInfo->attrInfo[i].type        = usedColInfo[i].typeId;
			pTempFileInfo->attrInfo[i].length      = usedColInfo[i].length;
			strcpy(pTempFileInfo->attrInfo[i].name, "");			// no column name
			pTempFileInfo->attrInfo[i].domain =  usedColInfo[i].typeId;

            // make column list structure
			pTempFileInfo->clist[i].nullFlag = SM_FALSE;
            pTempFileInfo->clist[i].colNo    = usedColInfo[i].colNo;
            pTempFileInfo->clist[i].start    = 0;
			if(usedColInfo[i].length < OOSQL_EVALBUFFER_MAXSTRINGSIZE)
			{
				pTempFileInfo->clist[i].length = usedColInfo[i].length;
				pTempFileInfo->clist[i].dataLength = usedColInfo[i].length;
			}
			else
			{
				pTempFileInfo->clist[i].length = OOSQL_EVALBUFFER_MAXSTRINGSIZE;
				pTempFileInfo->clist[i].dataLength = OOSQL_EVALBUFFER_MAXSTRINGSIZE;
			}

            if(pTempFileInfo->isDualBuf == SM_TRUE) {
				pTempFileInfo->prevColList[i].nullFlag = SM_FALSE;
                pTempFileInfo->prevColList[i].colNo    = usedColInfo[i].colNo;
                pTempFileInfo->prevColList[i].start    = 0;
				if(usedColInfo[i].length < OOSQL_EVALBUFFER_MAXSTRINGSIZE)
				{
					pTempFileInfo->prevColList[i].length = usedColInfo[i].length;
					pTempFileInfo->prevColList[i].dataLength = usedColInfo[i].length;
				}
				else
				{
					pTempFileInfo->prevColList[i].length = OOSQL_EVALBUFFER_MAXSTRINGSIZE;
					pTempFileInfo->prevColList[i].dataLength = OOSQL_EVALBUFFER_MAXSTRINGSIZE;
				}
            }
        }

        // set pointer to string buffer for string and variable string type
        pCurrStrBuf = pTempFileInfo->strBuf;
        if(pTempFileInfo->isDualBuf == SM_TRUE) {
            pCurrStrBuf2 = &(pTempFileInfo->strBuf[pTempFileInfo->strBufSize]);
        }
        for(i = 0; i < pTempFileInfo->nCols; i++) {
            if(pTempFileInfo->attrInfo[i].type == OOSQL_TYPE_STRING    || 
			   pTempFileInfo->attrInfo[i].type == OOSQL_TYPE_VARSTRING ||
			   pTempFileInfo->attrInfo[i].type == OOSQL_TYPE_TEXT)
			{
                pTempFileInfo->clist[i].data.ptr = pCurrStrBuf;

				if(usedColInfo[i].length < OOSQL_EVALBUFFER_MAXSTRINGSIZE)	
					pCurrStrBuf += usedColInfo[i].length + 1;
				else
					pCurrStrBuf += OOSQL_EVALBUFFER_MAXSTRINGSIZE;

                if(pTempFileInfo->isDualBuf == SM_TRUE) {
                    pTempFileInfo->prevColList[i].data.ptr = pCurrStrBuf2;
                    pCurrStrBuf2 += pTempFileInfo->attrInfo[i].length;
                }
            }
        }

        return eNOERROR;
}

Four	OOSQL_Evaluator::createSortStream(
	OOSQL_TempFileInfo*			pTempFileInfo,
	AP_ProjectionListElement*	projInfo          
)
{
    Four								e;              // error code
	Four								i;
	OOSQL_StorageManager::BTreeKeyInfo	sortKeyInfo;
	void*								userMemory;
	Four								userMemorySize;

    if(pTempFileInfo == NULL) 
	{
        OOSQL_ERR(eBADPARAMETER_OOSQL);
    }

	if(projInfo -> nSortKeys > MAXNUMKEYPARTS)
	{
		OOSQL_ERR(eBTREE_KEYLENGTH_EXCESS_OOSQL); 
	}


	sortKeyInfo.flag     = 0;      
    sortKeyInfo.nColumns = projInfo->nSortKeys;
    for (i = 0; i < projInfo->nSortKeys; i++) 
	{
        sortKeyInfo.columns[i].colNo = projInfo->sortKeys[i];
        if (projInfo->sortAscDesc[i] == SORTORDER_DESC)
            sortKeyInfo.columns[i].flag = KEYINFO_COL_DESC;
        else
            sortKeyInfo.columns[i].flag = KEYINFO_COL_ASC;
    }

	if(m_sortBufferInfo.mode == OOSQL_SB_USE_DISK)
	{
		userMemory     = NULL;
		userMemorySize = 0;
	}
	else
	{
		userMemorySize = m_sortBufferMemory.LargestFreeBlockSize();
		userMemory      = m_sortBufferMemory.Alloc(userMemorySize);
		if(!userMemory) OOSQL_ERR(eMEMORYALLOCERR_OOSQL);
	}

	e = pTempFileInfo->sortStream->CreateStream(m_sortBufferInfo.diskInfo.sortVolID, 
		                                        pTempFileInfo->nCols, 
												pTempFileInfo->attrInfo,
												&sortKeyInfo,
												userMemory, userMemorySize);
	OOSQL_CHECK_ERR(e);

    return eNOERROR;
}

Four    OOSQL_Evaluator::createTempFile(
    OOSQL_TempFileInfo      *pTempFileInfo
)
/*
    Function:
        Make a temporary file name which is unique in a database
        and create the temporary file.

    Side effect:
        New temporary file is created.

    Return value:
        eNOERROR        if a temporary file is created
        
*/
{
    char            tempFileName[MAXCLASSNAME];
    static Four     uniqueNo = 1;   // unique number for a temporary file name
    Four            e;              // error code
	Four			tmpClassId;


    if(pTempFileInfo == NULL) {
        OOSQL_ERR(eBADPARAMETER_OOSQL);
    }

    if(pTempFileInfo->name[0] != NULL) {
        OOSQL_ERR(eTEMPFILE_ALREADYCREATED_OOSQL);
    }

    do { 
        // make temp. file name by combining m_volID, m_transID, and unique number.
        sprintf(tempFileName, "tempFile%ld%ld%ld%ld", m_volID, m_transID.high, m_transID.low, uniqueNo++); 

        // create the temporary file 
        e = m_storageManager->CreateClass(m_sortBufferInfo.diskInfo.sortVolID, tempFileName, NULL, NULL, 
                                          pTempFileInfo->nCols, pTempFileInfo->attrInfo,
								          0, NULL, 0, NULL, 
										  SM_TRUE,					// Create this class as a temporary class
										  &tmpClassId);			// Dummy variable, Temporary class always has 0 class id

        // check if the temp. file name already exists
        if(e == eCLASSDUPLICATED_OOSQL) 
		{
            continue;
        }
        else {
            if(e < eNOERROR) {     /* some error has occurred */
                tempFileName[0] = NULL; // reset temporary file name
                OOSQL_ERR(e);
            }
            else {  /* temporary file having unique name has been created */
                // break out do-while loop
                break;
            }
        }
    } while(e == eCLASSDUPLICATED_OOSQL);        /* end of do-while */

    strcpy(pTempFileInfo->name, &tempFileName[0]);
    return eNOERROR;
}


Four    OOSQL_Evaluator::finalizeTempFiles()
/*
    Function:
        do the followings:
            1) Closes scan(s) for temporary files.
            2) Drop index(es) on temporary files if exists.
            3) Destroy the temporary files.

    Side effect:

    Referenced member variables:

    Return value:
*/
{
    OOSQL_TempFileInfo *pTempFileInfo;
    Four i;
    Four e;

    /* check if any temporary table is used */
    if(m_evalTempFileInfoTable == NULL) 
        return(eNOERROR);

    for(i = 0; i < EVAL_TEMPFILEINFOTABLE.nTempFiles; i++) {
        pTempFileInfo = EVAL_TEMPFILEINFOTABLEELEMENTS[i];


        /* close scan */
        if(pTempFileInfo->osn >= 0) {
            e = m_storageManager->CloseScan(pTempFileInfo->osn);
            if(e < eNOERROR) 
                OOSQL_ERR(e);
            RESET_OPENSCANNUM(pTempFileInfo->osn);
        }

        /* close temporary file */
        if(pTempFileInfo->ocn >= 0) {
            e = m_storageManager->CloseClass(pTempFileInfo->ocn);
            if(e < eNOERROR)
                OOSQL_ERR(e);
            RESET_OPENCLASSNUM(pTempFileInfo->ocn);
        }

        /* drop index if it was created */
        if(pTempFileInfo->indexName[0] != NULL) {
            e = m_storageManager->DropIndex(m_volID, &(pTempFileInfo->indexName[0]));
            if(e < eNOERROR)
                OOSQL_ERR(e);
            RESET_INDEXNAME(pTempFileInfo->indexName);
        }

        /* destroy temporary file */
        if(pTempFileInfo->name[0] != NULL) {
            e = m_storageManager->DestroyClass(m_sortBufferInfo.diskInfo.sortVolID, pTempFileInfo->name);
            if(e < eNOERROR) 
                OOSQL_ERR(e);
            RESET_CLASSNAME(pTempFileInfo->name);
        }

		/* destroy sort stream */
		if(pTempFileInfo->sortStream)
		{
			e = pTempFileInfo->sortStream->DestroyStream();
			OOSQL_CHECK_ERR(e);

			OOSQL_DELETE(pTempFileInfo->sortStream);
		}
		pTempFileInfo->sortStream = NULL;
    }

    return(eNOERROR);
}

