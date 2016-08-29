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

#ifndef SLIMDOWN_TEXTIR


/*
    MODULE:
        oosql_Eval_TextIR.cxx

    DESCRIPTION:

    IMPORTS:

    EXPORTS:
        Four    OOSQL_Evaluator::allocTextIR_SubPlanEvalBuffer();
        Four    OOSQL_Evaluator::initTextIR_IndexScan( Four, AP_IndexInfoPoolIndex );
        Four    OOSQL_Evaluator::closeTextIR_IndexScan( AP_IndexInfoPoolIndex );
        Four    OOSQL_Evaluator::execTextIR_IndexScan( AP_TextIndexSubPlan*, Four*, OOSQL_StorageManager::OID* );

        Four    OOSQL_Evaluator::execTextIR_FnMatch( Four, AP_ExprPoolIndex*, DB_Value* );
        Four    OOSQL_Evaluator::execTextIR_FnWeight( Four, AP_ExprPoolIndex*, DB_Value* );
*/


/*
 * include files
 */
#include "OOSQL_Evaluator.hxx"
#include "oosql_Eval_TextIR.hxx"
#include "oosql_Eval_TextIR_Index.hxx"
#include <string.h>

Four OOSQL_Evaluator::allocTextIR_SubPlanEvalBuffer()
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{

    AP_TextIndexCondPoolElements    textIndexCondPoolElements;
    AP_TextIndexCondPoolIndex       startIndex;
    Four                            i;

    // allocate memory for the Text IR subplan evaluation buffer
    // as long as the size of text index condition m_pool.
    OOSQL_NEW(m_evalTextIR_SubPlanEvalBuffer, pMemoryManager, OOSQL_TextIR_SubPlanEvalBuffer(ACCESSPLAN.getTextIndexCondPoolSize()));
    if(m_evalTextIR_SubPlanEvalBuffer == NULL) OOSQL_ERR(eOUTOFMEMORY_OOSQL);
    m_evalTextIR_SubPlanEvalBufferElements = &EVAL_TEXTIR_EVALBUF[0];

    // make a m_pool index to access the start node of the text index condition m_pool
    startIndex.setPoolIndex(0, ACCESSPLAN.getTextIndexCondPoolSize());

    // get pointer to m_pool elements
    textIndexCondPoolElements = ACCESSPLAN.getTextIndexCondPool(startIndex);

    /* allocate resoures for Text IR subplan element according to node type */
    for (i = 0; i < ACCESSPLAN.getTextIndexCondPoolSize(); i++) 
    {
        switch(textIndexCondPoolElements[i].nodeKind) 
        {
        case TEXTINDEXCOND_OPERATOR:
            if(textIndexCondPoolElements[i].oper.operatorID == OP_TEXTIR_MAX) 
            {
                /* create a Min-Max Heap to process 'max' operator */
                OOSQL_NEW(EVAL_TEXTIR_EVALBUFELEMENTS[i].minMaxHeap, pMemoryManager, OOSQL_MinMaxHeap);
                if(EVAL_TEXTIR_EVALBUFELEMENTS[i].minMaxHeap == NULL)
                    OOSQL_ERR(eOUTOFMEMORY_OOSQL);

                break;
            }
            break;

        case TEXTINDEXCOND_KEYWORD:
            /* allocate memory for scan posting information */
            OOSQL_NEW(EVAL_TEXTIR_EVALBUFELEMENTS[i].postingScanInfo, pMemoryManager, 
                      oosql_TextIR_PostingScanInfo(m_storageManager,
                                                   textIndexCondPoolElements[i].keyword.nPostings,
                                                   textIndexCondPoolElements[i].keyword.isPostingContainingTupleID,
                                                   textIndexCondPoolElements[i].keyword.isPostingContainingSentenceAndWordNum,
                                                   textIndexCondPoolElements[i].keyword.isPostingContainingByteOffset));
            if ( EVAL_TEXTIR_EVALBUFELEMENTS[i].postingScanInfo == NULL )
                OOSQL_ERR(eOUTOFMEMORY_OOSQL);

            break;
        }
    }

    /* return */
    return eNOERROR;
}


Four    OOSQL_Evaluator::initTextIR_IndexScan(
        Four                    ocn,          // IN:
        AP_IndexInfoPoolIndex   indexInfo     // IN:
)
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
        Four                e;
        AP_IndexInfoElement *indexInfoNode;

        indexInfoNode = ACCESSPLAN.getIndexInfoElem(indexInfo);

        /* check input parameters */
#ifdef  OOSQL_DEBUG
        if (ocn < 0) 
                OOSQL_ERR(eBADPARAMETER_OOSQL);
        if (indexInfoNode == NULL)
                OOSQL_ERR(eBADPARAMETER_OOSQL);
        if (indexInfoNode->scan.indexType != INDEXTYPE_TEXT) {
                OOSQL_ERR(eBADPARAMETER_OOSQL);
        }
#endif

        /* traverse Text IR subplan and open scan(s) necessary to evaluate it */
        e = initTextIR_SubPlanNode( ocn, indexInfoNode->scan.colNo, &(indexInfoNode->scan.indexId), 
                                    indexInfoNode->scan.text.textIndexCond, indexInfo );
        if (e < eNOERROR) OOSQL_ERR(e);

        /* return */
        return eNOERROR;
}


Four    OOSQL_Evaluator::closeTextIR_IndexScan( 
        AP_TextIndexCondPoolIndex       indexInfo       // IN:
)
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
        AP_TextIndexCondElement *indexNode;
        Four    osn;
        Four    e;

        /* check input paramter */
#ifdef  OOSQL_DEBUG
        if (IS_NULL_POOLINDEX(indexInfo) == SM_TRUE)
            OOSQL_ERR(eBADPARAMETER_OOSQL);
#endif
        /* get Text IR condition node for the current node */
        indexNode = ACCESSPLAN.getTextIndexCondElem(indexInfo);
#ifdef  OOSQL_DEBUG
        if (indexNode == NULL) OOSQL_ERR(eNULL_POINTER_OOSQL);
#endif

        switch (indexNode->nodeKind) {
            case TEXTINDEXCOND_OPERATOR:
                /* recursive call for the first operand */
                e = closeTextIR_IndexScan( indexNode->oper.op1 );
                if (e < eNOERROR) OOSQL_ERR(e);

                /* recursive call for the second operand */
                e = closeTextIR_IndexScan( indexNode->oper.op2 );
                if (e < eNOERROR) OOSQL_ERR(e);

                break;

            case TEXTINDEXCOND_KEYWORD:
                /* free temporyary file */
                if(EVAL_TEXTIR_EVALBUFELEMENTS[indexInfo.startIndex].postingScanInfo->m_tempFileInfo)
                {
                    e = execTextIR_FreeTempFile( EVAL_TEXTIR_EVALBUFELEMENTS[indexInfo.startIndex].postingScanInfo->m_tempFileInfo );
                    if (e < eNOERROR) OOSQL_ERR(e);

					EVAL_TEXTIR_EVALBUFELEMENTS[indexInfo.startIndex].postingScanInfo->m_tempFileInfo = NULL;
                }

                /* get open scan num. for the current node */
                osn = EVAL_TEXTIR_EVALBUFELEMENTS[indexInfo.startIndex].postingScanInfo->m_osn;

                /* call LOM interface */
                if(osn != NIL)
                {
                    e = m_storageManager->CloseScan(osn);
                    if (e < eNOERROR) OOSQL_ERR(e);
                }
                EVAL_TEXTIR_EVALBUFELEMENTS[indexInfo.startIndex].postingScanInfo->m_osn = NIL;

                break;
        }

        /* return */
        return eNOERROR;
}


/*
    Function:
        This function implements main evaluation logic of Text IR index scan.

    Side effect:

    Referenced member variables:

    Return value:
*/
Four    OOSQL_Evaluator::execTextIR_IndexScan( 
    AP_IndexInfoPoolIndex           indexInfo,          // IN   
    OOSQL_TextIR_PostingQueue&      postingQueue,       // INOUT 
    Four                            logicalIdHints      // IN   
)
{
    Four                    e;
    AP_IndexInfoElement     *indexNode;

    /* check input parameters */
#ifdef  OOSQL_DEBUG
    if (IS_NULL_POOLINDEX(indexInfo) == SM_TRUE)
        OOSQL_ERR(eBADPARAMETER_OOSQL);
#endif

    /* get the index info. node for the current node */
    indexNode = ACCESSPLAN.getIndexInfoElem(indexInfo);

    /* check input parameter */
#ifdef  OOSQL_DEBUG
    if (indexNode->nodeKind != INDEXINFO_SCAN)
        OOSQL_ERR(eBADPARAMETER_OOSQL);
    if (indexNode->scan.indexType != INDEXTYPE_TEXT)
        OOSQL_ERR(eBADPARAMETER_OOSQL);
#endif

    e = execTextIR_SubPlan(indexNode->scan.text.textIndexCond, postingQueue, logicalIdHints);
    if (e < eNOERROR) OOSQL_ERR(e);
    if(e == TEXTIR_EOS) return TEXTIR_EOS;

    /* return */
    return eNOERROR;
}


Four    OOSQL_Evaluator::execTextIR_FnMatch( 
        AP_FuncEvalInfo *funcInfo,              // IN:
        OOSQL_DB_Value  *result                 // OUT:
)
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
        AP_ArgumentPoolElements argList;
        Four    ocn;
        OOSQL_StorageManager::OID     *docId;
        Four    textColNo;
        AP_TextIndexCondPoolIndex subPlanIndex;
        OOSQL_TextIR_Posting    *posting;
        EVAL_EvalBufferSlot     *matchResult;
        Four    e;

        /* check input parameters */
#ifdef  OOSQL_DEBUG
        if (funcInfo == NULL) OOSQL_ERR(eBADPARAMETER_OOSQL);
        if (result == NULL) OOSQL_ERR(eBADPARAMETER_OOSQL);
#endif

        /* get the arguement list of the MATCH() function */
        argList = ACCESSPLAN.getArgumentPool( funcInfo->argument );

#ifdef  OOSQL_DEBUG
        /* check the type of the first argument */
        if ( argList[0].argumentKind != ARGUMENT_KIND_PATHEXPR ) {
            OOSQL_ERR(eBADPARAMETER_OOSQL);
        }
        else {
            if ( argList[0].pathExpr.kind != PATHEXPR_KIND_ATTR )
                OOSQL_ERR(eBADPARAMETER_OOSQL);
        }

        /* check the type of the second argument */
        if ( argList[1].argumentKind != ARGUMENT_KIND_TEXTIR_SUBPLAN ) {
            OOSQL_ERR(eBADPARAMETER_OOSQL);
        }
#endif

        /* get open class number for the class designated by the first arguement */
        ocn = EVAL_ACCESSLISTTABLE[ argList[0].pathExpr.col.planNo ].getCurrOcn();

        /* get the OOSQL_StorageManager::OID to which MATCH() will be applied */
        docId = EVAL_EVALBUFFER[ argList[0].pathExpr.col.planNo ].getOID_Ptr();
#ifdef  OOSQL_DEBUG
        if (docId == NULL) OOSQL_ERR(eNULL_POINTER_OOSQL);
#endif

        /* get Text column number to which MATCH() will be applied */
        textColNo = argList[0].pathExpr.col.colNo;

        /* get the second argument of this function (Text IR subplan) */
        subPlanIndex = argList[1].textIndexSubPlan.textIndexCond;

        /* open LOM_Text_Scan for each keyword node of the subplan */
        e = initTextIR_SubPlanNodeForFnMatch( ocn, docId, textColNo, subPlanIndex );
        if (e < eNOERROR) OOSQL_ERR(e);

        /* execute MATCH() function */
        e = execTextIR_SubPlanForOneDocument( subPlanIndex, &posting );
        if (e < eNOERROR) OOSQL_ERR(e);

        /* get pointer to the evaluation buffer slot for the current MATCH() */
        matchResult = EVAL_EVALBUFFER[ argList[0].pathExpr.col.planNo ].getFnMatchSlotPtr( 
                argList[1].textIndexSubPlan.matchFuncNum );
#ifdef  DEBUF
        if (matchResult == NULL) OOSQL_ERR(eNULL_POINTER_OOSQL);
#endif

        e = closeTextIR_Scan(subPlanIndex);
        if(e < eNOERROR) OOSQL_ERR(e);

        /* save the MATCH() result into the evaluation buffer */
        matchResult->data.d = posting->weight;

        /* set output parameter */
        result->data.d = posting->weight;

        /* return */
        return eNOERROR;
}

Four    OOSQL_Evaluator::execTextIR_FnWeight( 
        AP_FuncResultAccessInfo *funcResult,            // IN:
        OOSQL_DB_Value        *result                         // OUT:
)
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
        EVAL_EvalBufferSlot *fnMatchResult;

        /* check input parameters */
#ifdef  OOSQL_DEBUG
        if (funcResult == NULL) {
            OOSQL_ERR(eBADPARAMETER_OOSQL);
        }
        if (result == NULL) {
            OOSQL_ERR(eBADPARAMETER_OOSQL);
        }
        if ( funcResult->functionID != FUNCTION_TEXTIR_WEIGHT ) {
            OOSQL_ERR(eBADPARAMETER_OOSQL);
        }
#endif

        /* get pointer to eval. buffer slot for the MATCH() function result */
        fnMatchResult = EVAL_EVALBUFFER[ funcResult->planNo ].getFnMatchSlotPtr( funcResult->funcIndex );
#ifdef  OOSQL_DEBUG
        if (fnMatchResult == NULL) OOSQL_ERR(eNULL_POINTER_OOSQL);
#endif

        /* set result */
        result->type = OOSQL_TYPE_double;
        result->data.d = fnMatchResult->data.d;

        /* return */
        return eNOERROR;
}

Four OOSQL_Evaluator::execTextIR_GetNPostingsToSort(
    Four                                ocn,                // IN:
    OOSQL_StorageManager::IndexID*      indexId,            // IN:
    AP_TextIndexCondPoolIndex           indexCondPoolIndex, // IN:
    Four*                               nPostings)          // OUT
{
    AP_TextIndexCondPoolElements            textIndexCond(m_pool->textIndexCondPool);
    Four                                    e;
    Four                                    currentNPostings;
    Four                                    keywordKind;
    Two                                     startKeyLen;
    Two                                     stopKeyLen;
    char                                    startKeyword[MAXKEYWORDSIZE];
    char                                    stopKeyword[MAXKEYWORDSIZE];
    OOSQL_StorageManager::BoundCond         startBound;
    OOSQL_StorageManager::BoundCond         stopBound;
    OOSQL_StorageManager::LockParameter     lockup;

    textIndexCond = indexCondPoolIndex;

    if(textIndexCond == NULL_POOLINDEX)
        return eNOERROR;

    switch(textIndexCond[0].nodeKind)
    {
    case TEXTINDEXCOND_OPERATOR:
        e = execTextIR_GetNPostingsToSort(ocn, indexId, textIndexCond[0].oper.op1, nPostings);
        if(e < eNOERROR) OOSQL_ERR(e);

        e = execTextIR_GetNPostingsToSort(ocn, indexId, textIndexCond[0].oper.op2, nPostings);
        if(e < eNOERROR) OOSQL_ERR(e);
        break;
    case TEXTINDEXCOND_KEYWORD:
        /* get keyword kind */
        switch (textIndexCond[0].keyword.accessMethod) 
        {
        case KEYWORD_IDX_SCAN:
            keywordKind = KEYWORD;
            break;
        case REVERSEKEYWORD_IDX_SCAN:
            keywordKind = REVERSEKEYWORD;
            break;
        default:        /* ERROR: invalid access method */
            OOSQL_ERR(eINVALID_CASE_OOSQL);
        }

        /* copy start bound keyword (string value) to char string */
        startKeyLen = GET_POOLSIZE(textIndexCond[0].keyword.startBound);
        if(startKeyLen > MAXKEYWORDSIZE) 
            startKeyLen = MAXKEYWORDSIZE;
        e = copyStringValueToString(&(textIndexCond[0].keyword.startBound), &startKeyword[0], startKeyLen);
        if (e < eNOERROR) OOSQL_ERR(e);

        /* copy stop bound keyword (string value) to char string */
        stopKeyLen = GET_POOLSIZE(textIndexCond[0].keyword.stopBound);
        if(stopKeyLen > MAXKEYWORDSIZE)
            stopKeyLen = MAXKEYWORDSIZE;
        e = copyStringValueToString(&(textIndexCond[0].keyword.stopBound), &stopKeyword[0], stopKeyLen);
        if (e < eNOERROR) OOSQL_ERR(e);

        /* check if wild character is used */
        if (compareTextIR_Keyword(startKeyLen, &startKeyword[0], stopKeyLen, &stopKeyword[0]) == CMP_EQ) 
        {
            /* make bound condition operator */
            startBound.op = OOSQL_StorageManager::SM_EQ;
            stopBound.op = OOSQL_StorageManager::SM_EQ;
        }
        else 
        {
            /* make bound condition operator */
            startBound.op = OOSQL_StorageManager::SM_GE;
            stopBound.op = OOSQL_StorageManager::SM_LE;
        }

        /* make start bound condition */
        startBound.key.len = sizeof(Two) + startKeyLen;
        memcpy( &(startBound.key.val[0]), &startKeyLen, sizeof(Two) );
        memcpy( &(startBound.key.val[sizeof(Two)]), &startKeyword[0], startKeyLen );

        /* make start bound condition */
        stopBound.key.len = sizeof(Two) + stopKeyLen;
        memcpy( &(stopBound.key.val[0]), &stopKeyLen, sizeof(Two) );
        memcpy( &(stopBound.key.val[sizeof(Two)]), &stopKeyword[0], stopKeyLen );

        /* make lockup parameter */
        if(isUpdateQuery())
            lockup.mode     = OOSQL_StorageManager::L_IX;
        else
            lockup.mode     = OOSQL_StorageManager::L_IS;
        lockup.duration = OOSQL_StorageManager::L_COMMIT;

        if(startBound.op != stopBound.op)
        {
            if(textIndexCond[0].keyword.nPostings != NIL)
            {
                currentNPostings = textIndexCond[0].keyword.nPostings;
            }
            else
            {
                e = m_storageManager->Text_GetNPostings(ocn, indexId, keywordKind, &startBound, &stopBound, &lockup, &currentNPostings);
                if(e < eNOERROR) OOSQL_ERR(e);
            }

            *nPostings += currentNPostings;
        }
        break;
    }

    return eNOERROR;
}

Four OOSQL_Evaluator::execTextIR_GetNPostingsToSort(
    Four                        ocn,                // IN:
    AP_IndexInfoPoolIndex       indexInfoPoolIndex, // IN:
    Four*                       nPostings)          // OUT
{
    Four                            e;
    AP_IndexInfoPoolElements        indexInfo(m_pool->indexInfoPool);
    Four                            currentNPostings;

    indexInfo = indexInfoPoolIndex;

    if(indexInfo == NULL_POOLINDEX)
        return eNOERROR;

    switch(indexInfo[0].nodeKind)
    {
    case INDEXINFO_OPERATOR:
        e = execTextIR_GetNPostingsToSort(ocn, indexInfo[0].oper.op1, nPostings);
        if(e < eNOERROR) OOSQL_ERR(e);

        e = execTextIR_GetNPostingsToSort(ocn, indexInfo[0].oper.op2, nPostings);
        if(e < eNOERROR) OOSQL_ERR(e);
        break;

    case INDEXINFO_SCAN:
        currentNPostings = 0;
        if(indexInfo[0].scan.indexType == INDEXTYPE_TEXT)
        {
            e = execTextIR_GetNPostingsToSort(ocn, 
                                              &indexInfo[0].scan.indexId, 
                                              indexInfo[0].scan.text.textIndexCond, 
                                              &currentNPostings);
            if(e < eNOERROR) OOSQL_ERR(e);

            *nPostings += currentNPostings;
        }
        break;
    }

    return eNOERROR;
}

Four OOSQL_Evaluator::execTextIR_GetMemorySizeToSortPosting(Four& memorySize)
{
    Four        nTotalPostings, nPostings;
    Four        i;
    Four        ocn, e;

    for(nTotalPostings = 0, i = 0; i < ACCESSPLAN.m_nAP_Elem; i++)
    {
        if(ACCESSPLANELEMENTS[i].classInfo.classKind != CLASSKIND_PERSISTENT)
            continue;
        ocn = e = m_storageManager->OpenClass(m_volID, ACCESSPLANELEMENTS[i].classInfo.classId);
        if(e < eNOERROR) OOSQL_ERR(e);

        nPostings = 0;
        e = execTextIR_GetNPostingsToSort(ocn, ACCESSPLANELEMENTS[i].indexInfo, &nPostings);
        if(e < eNOERROR) OOSQL_ERR(e);

        nTotalPostings += nPostings;        

        e = m_storageManager->CloseClass(ocn);
        if(e < eNOERROR) OOSQL_ERR(e);
    }

    memorySize = nTotalPostings * OOSQL_MEMORYSORT_POSTINGSIZE;

    return eNOERROR;
}


Four OOSQL_Evaluator::execTextIR_GetNPostingsToEstimate(
    Four                                ocn,                // IN:
    OOSQL_StorageManager::IndexID*      indexId,            // IN:
    AP_TextIndexCondPoolIndex           indexCondPoolIndex, // IN:
    Four*                               nPostings           // OUT
)
{
    AP_TextIndexCondPoolElements            textIndexCond(m_pool->textIndexCondPool);
    Four                                    e;
    Four                                    nPostings1, nPostings2;
    Four                                    keywordKind;
    Two                                     startKeyLen;
    Two                                     stopKeyLen;
    char                                    startKeyword[MAXKEYWORDSIZE];
    char                                    stopKeyword[MAXKEYWORDSIZE];
    OOSQL_StorageManager::BoundCond         startBound;
    OOSQL_StorageManager::BoundCond         stopBound;
    OOSQL_StorageManager::LockParameter     lockup;

    textIndexCond = indexCondPoolIndex;

    if (textIndexCond == NULL_POOLINDEX)
        return eNOERROR;

    if (textIndexCond[0].nodeKind == TEXTINDEXCOND_OPERATOR)
    {
        e = execTextIR_GetNPostingsToEstimate(ocn, indexId, textIndexCond[0].oper.op1, &nPostings1);
        if (e < eNOERROR) OOSQL_ERR(e);

        e = execTextIR_GetNPostingsToEstimate(ocn, indexId, textIndexCond[0].oper.op2, &nPostings2);
        if (e < eNOERROR) OOSQL_ERR(e);

        switch (textIndexCond[0].oper.operatorID)
        {
        case OP_TEXTIR_AND:
            if (nPostings1 <= nPostings2) *nPostings = nPostings1;
            else *nPostings = nPostings2;
            break;
        case OP_TEXTIR_OR:
            *nPostings = nPostings1 + nPostings2;
            break;
        case OP_TEXTIR_ACCUMULATE:
            *nPostings = nPostings1 + nPostings2;
            break;
        case OP_TEXTIR_MINUS:
            *nPostings = nPostings1;
            break;
        /* consider other operators */
        default:
            *nPostings = nPostings1;
            break;
        }
    }
    else if (textIndexCond[0].nodeKind == TEXTINDEXCOND_KEYWORD)
    {
        /* get keyword kind */
        switch (textIndexCond[0].keyword.accessMethod) 
        {
        case KEYWORD_IDX_SCAN:
            keywordKind = KEYWORD;
            break;
        case REVERSEKEYWORD_IDX_SCAN:
            keywordKind = REVERSEKEYWORD;
            break;
        default:        /* ERROR: invalid access method */
            OOSQL_ERR(eINVALID_CASE_OOSQL);
        }

        /* copy start bound keyword (string value) to char string */
        startKeyLen = GET_POOLSIZE(textIndexCond[0].keyword.startBound);
        if(startKeyLen > MAXKEYWORDSIZE) 
            startKeyLen = MAXKEYWORDSIZE;
        e = copyStringValueToString(&(textIndexCond[0].keyword.startBound), &startKeyword[0], startKeyLen);
        if (e < eNOERROR) OOSQL_ERR(e);

        /* copy stop bound keyword (string value) to char string */
        stopKeyLen = GET_POOLSIZE(textIndexCond[0].keyword.stopBound);
        if(stopKeyLen > MAXKEYWORDSIZE)
            stopKeyLen = MAXKEYWORDSIZE;
        e = copyStringValueToString(&(textIndexCond[0].keyword.stopBound), &stopKeyword[0], stopKeyLen);
        if (e < eNOERROR) OOSQL_ERR(e);

        /* check if wild character is used */
        if (compareTextIR_Keyword(startKeyLen, &startKeyword[0], stopKeyLen, &stopKeyword[0]) == CMP_EQ) 
        {
            /* make bound condition operator */
            startBound.op = OOSQL_StorageManager::SM_EQ;
            stopBound.op = OOSQL_StorageManager::SM_EQ;
        }
        else 
        {
            /* make bound condition operator */
            startBound.op = OOSQL_StorageManager::SM_GE;
            stopBound.op = OOSQL_StorageManager::SM_LE;
        }

        /* make start bound condition */
        startBound.key.len = sizeof(Two) + startKeyLen;
        memcpy( &(startBound.key.val[0]), &startKeyLen, sizeof(Two) );
        memcpy( &(startBound.key.val[sizeof(Two)]), &startKeyword[0], startKeyLen );

        /* make stop bound condition */
        stopBound.key.len = sizeof(Two) + stopKeyLen;
        memcpy( &(stopBound.key.val[0]), &stopKeyLen, sizeof(Two) );
        memcpy( &(stopBound.key.val[sizeof(Two)]), &stopKeyword[0], stopKeyLen );

        /* make lockup parameter */
        if(isUpdateQuery())
            lockup.mode     = OOSQL_StorageManager::L_IX;
        else
            lockup.mode     = OOSQL_StorageManager::L_IS;
        lockup.duration = OOSQL_StorageManager::L_COMMIT;

        e = m_storageManager->Text_GetNPostings(ocn, indexId, keywordKind, &startBound, &stopBound, &lockup, nPostings);
        if (e < eNOERROR) OOSQL_ERR(e);
    }

    return eNOERROR;
}

Four OOSQL_Evaluator::execTextIR_GetNPostingsToEstimate(
    Four                        ocn,                // IN:
    AP_IndexInfoPoolIndex       indexInfoPoolIndex, // IN:
    Four*                       nPostings)          // OUT
{
    Four                            e;
    Four                            nPostings1, nPostings2;
    AP_IndexInfoPoolElements        indexInfo(m_pool->indexInfoPool);

    indexInfo = indexInfoPoolIndex;

    if (indexInfo == NULL_POOLINDEX)
        return eNOERROR;

    if (indexInfo[0].nodeKind == INDEXINFO_OPERATOR)
    {
        e = execTextIR_GetNPostingsToEstimate(ocn, indexInfo[0].oper.op1, &nPostings1);
        if (e < eNOERROR) OOSQL_ERR(e);

        e = execTextIR_GetNPostingsToEstimate(ocn, indexInfo[0].oper.op2, &nPostings2);
        if (e < eNOERROR) OOSQL_ERR(e);

        switch (indexInfo[0].oper.operatorID)
        {
        case OP_INDEX_AND:
            if (nPostings1 <= nPostings2) *nPostings = nPostings1;
            else *nPostings = nPostings2;
            break;
        case OP_INDEX_OR:
            *nPostings = nPostings1 + nPostings2;
            break;
        default:
            OOSQL_ERR(eINVALID_CASE_OOSQL);
        }
    }
    else if (indexInfo[0].nodeKind == INDEXINFO_SCAN)
    {
        if (indexInfo[0].scan.indexType == INDEXTYPE_TEXT)
        {
            e = execTextIR_GetNPostingsToEstimate(ocn, 
                                                  &indexInfo[0].scan.indexId, 
                                                  indexInfo[0].scan.text.textIndexCond, 
                                                  nPostings);
            if (e < eNOERROR) OOSQL_ERR(e);
        }
    }

    return eNOERROR;
}

Four OOSQL_Evaluator::execTextIR_EstimateNumOfResults(Four& estimatedResultNum)
{
    Four        nTotalPostings, nPostings;
    Four        i;
    Four        ocn, e;

    for (nTotalPostings = 0, i = 0; i < ACCESSPLAN.m_nAP_Elem; i++)
    {
        if (ACCESSPLANELEMENTS[i].classInfo.classKind == CLASSKIND_PERSISTENT)
        {
            ocn = e = m_storageManager->OpenClass(m_volID, ACCESSPLANELEMENTS[i].classInfo.classId);
            if (e < eNOERROR) OOSQL_ERR(e);

            e = execTextIR_GetNPostingsToEstimate(ocn, ACCESSPLANELEMENTS[i].indexInfo, &nPostings);
            if (e < eNOERROR) OOSQL_ERR(e);

            nTotalPostings += nPostings;        

            e = m_storageManager->CloseClass(ocn);
            if(e < eNOERROR) OOSQL_ERR(e);
        }
    }

    estimatedResultNum = nTotalPostings;

    return eNOERROR;
}


#else  /* SLIMDOWN_TEXTIR */


/*
 * include files
 */
#include "OOSQL_Evaluator.hxx"
#include "oosql_Eval_TextIR.hxx"
#include "oosql_Eval_TextIR_Index.hxx"
#include <string.h>

Four OOSQL_Evaluator::allocTextIR_SubPlanEvalBuffer()
{
	return eTEXTIR_NOTENABLED_OOSQL;
}


Four    OOSQL_Evaluator::initTextIR_IndexScan(
        Four					ocn,          // IN:
        AP_IndexInfoPoolIndex	indexInfo     // IN:
)
{
	return eTEXTIR_NOTENABLED_OOSQL;
}

Four    OOSQL_Evaluator::closeTextIR_IndexScan( 
        AP_TextIndexCondPoolIndex       indexInfo       // IN:
)
{
	return eTEXTIR_NOTENABLED_OOSQL;
}

Four    OOSQL_Evaluator::execTextIR_IndexScan( 
    AP_IndexInfoPoolIndex			indexInfo,			// IN    
	OOSQL_TextIR_PostingQueue&		postingQueue,		// INOUT 
	Four							logicalIdHints		// IN    
)
{
	return eTEXTIR_NOTENABLED_OOSQL;
}


Four    OOSQL_Evaluator::execTextIR_FnMatch( 
        AP_FuncEvalInfo *funcInfo,              // IN:
        OOSQL_DB_Value  *result                 // OUT:
)
{
	return eTEXTIR_NOTENABLED_OOSQL;
}


Four    OOSQL_Evaluator::execTextIR_FnWeight( 
        AP_FuncResultAccessInfo *funcResult,            // IN:
        OOSQL_DB_Value        *result                         // OUT:
)
{
	return eTEXTIR_NOTENABLED_OOSQL;
}


Four OOSQL_Evaluator::execTextIR_GetNPostingsToSort(
	Four								ocn,				// IN:
	OOSQL_StorageManager::IndexID*		indexId,			// IN:
	AP_TextIndexCondPoolIndex			indexCondPoolIndex, // IN:
	Four*								nPostings)			// OUT
{
	return eTEXTIR_NOTENABLED_OOSQL;
}


Four OOSQL_Evaluator::execTextIR_GetNPostingsToSort(
	Four						ocn,				// IN:
	AP_IndexInfoPoolIndex		indexInfoPoolIndex, // IN:
	Four*						nPostings)			// OUT
{
	return eTEXTIR_NOTENABLED_OOSQL;
}

Four OOSQL_Evaluator::execTextIR_GetMemorySizeToSortPosting(Four& memorySize)
{
	return eTEXTIR_NOTENABLED_OOSQL;
}


Four OOSQL_Evaluator::execTextIR_GetNPostingsToEstimate(
	Four								ocn,				// IN:
	OOSQL_StorageManager::IndexID*		indexId,			// IN:
	AP_TextIndexCondPoolIndex			indexCondPoolIndex, // IN:
	Four*								nPostings			// OUT
)
{
	return eTEXTIR_NOTENABLED_OOSQL;
}


Four OOSQL_Evaluator::execTextIR_GetNPostingsToEstimate(
	Four						ocn,				// IN:
	AP_IndexInfoPoolIndex		indexInfoPoolIndex, // IN:
	Four*						nPostings)			// OUT
{
	return eTEXTIR_NOTENABLED_OOSQL;
}

Four OOSQL_Evaluator::execTextIR_EstimateNumOfResults(Four& estimatedResultNum)
{
	return eTEXTIR_NOTENABLED_OOSQL;
}


#endif /* SLIMDOWN_TEXTIR */
