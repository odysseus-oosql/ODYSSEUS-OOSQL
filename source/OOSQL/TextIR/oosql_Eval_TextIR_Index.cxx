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
        oosql_Eval_TextIR_Index.cxx

    DESCRIPTION:

    IMPORTS:

    EXPORTS:
*/


/*
 * include files
 */
#include "OOSQL_Evaluator.hxx"
#include "oosql_Eval_TextIR.hxx"
#include "oosql_Eval_TextIR_Index.hxx"
#include "OOSQL_SortStream.hxx"
#include <string.h>

Four    OOSQL_Evaluator::initTextIR_SubPlanNode(
    Four                            ocn,                            // IN: open class num
    Two                             colNo,                          // IN: column number
    OOSQL_StorageManager::IndexID*  indexId,                        // IN: index id
    AP_TextIndexCondPoolIndex       indexCond,                      // IN: index condition
    AP_IndexInfoPoolIndex           indexInfoPoolIndex              // IN: indexInfo
)
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
        AP_TextIndexCondElement *indexCondNode;
        Four    scanId;
        Four    e;

        /* check input parameters */
#ifdef  OOSQL_DEBUG
        if(ocn < 0)
                OOSQL_ERR(eBADPARAMETER_OOSQL);
        if(indexId == NULL)
                OOSQL_ERR(eBADPARAMETER_OOSQL);
        if(IS_NULL_POOLINDEX(indexCond) == SM_TRUE)
                OOSQL_ERR(eBADPARAMETER_OOSQL);
#endif

        /* get the current node of the Text IR subplan 
         * NOTE: Text IR subplan is represented as condition tree
         */
        indexCondNode = ACCESSPLAN.getTextIndexCondElem(indexCond);
#ifdef  OOSQL_DEBUG
        if(indexCondNode == NULL) OOSQL_ERR(eNULL_POINTER_OOSQL);
#endif

        switch(indexCondNode->nodeKind) {
            case TEXTINDEXCOND_OPERATOR:

                /* recursive call for the left operand(that might be keyword node) */
                e = initTextIR_SubPlanNode(ocn, colNo, indexId, indexCondNode->oper.op1, indexInfoPoolIndex);
                OOSQL_CHECK_ERR(e);

                /* recursive call for the right operand(that might be keyword node) */
                e = initTextIR_SubPlanNode(ocn, colNo, indexId, indexCondNode->oper.op2, indexInfoPoolIndex);
                OOSQL_CHECK_ERR(e);


                /* make empty the posting queues associated with the operator */
                EVAL_TEXTIR_EVALBUFELEMENTS[indexCondNode->oper.op1.startIndex].op1PostingQueue.MakeEmpty();
                EVAL_TEXTIR_EVALBUFELEMENTS[indexCondNode->oper.op2.startIndex].op2PostingQueue.MakeEmpty();

                break;

            case TEXTINDEXCOND_KEYWORD:

                switch(indexCondNode->keyword.accessMethod) {
                    case KEYWORD_IDX_SCAN:
                    case REVERSEKEYWORD_IDX_SCAN:

                        /* open Text IR index scan for the keyword */
                        e = openTextIR_IndexScan(ocn, colNo, indexId, indexCond, &scanId);
                        OOSQL_CHECK_ERR(e);

                        e = m_storageManager->GetEmbeddedAttrTranslationInfo(
                                                scanId, &EVAL_TEXTIR_EVALBUFELEMENTS[indexCond.startIndex].postingScanInfo->m_embeddedAttrTranslationInfo);
                        OOSQL_CHECK_ERR(e);

                        e = EVAL_TEXTIR_EVALBUFELEMENTS[indexCond.startIndex].postingScanInfo->setOpenClassNoAndScanNo(ocn, scanId, indexInfoPoolIndex);
                        if(e < eNOERROR) OOSQL_ERR(e);

                        e = EVAL_TEXTIR_EVALBUFELEMENTS[indexCond.startIndex].postingScanInfo->setNumOfObjectsInClass(ocn);
                        if(e < eNOERROR) OOSQL_ERR(e);

                        EVAL_TEXTIR_EVALBUFELEMENTS[indexCond.startIndex].doneFlag = SM_FALSE;

                        break;

                    default:    /* ERROR: internal */
                        OOSQL_ERR(eINVALID_CASE_OOSQL);
                }

                break;

            case TEXTINDEXCOND_CONSTANT:

                /* not related to Text index scan, so just return */
                break;

            default:    /* invalid node kind */
                OOSQL_ERR(eINVALID_CASE_OOSQL);
        }

        /* return */
        return eNOERROR;
}


Four    OOSQL_Evaluator::openTextIR_IndexScan(
    Four                            ocn,            // IN:  open class number
    Two                             colNo,          // IN:  column no
    OOSQL_StorageManager::IndexID*  indexId,        // IN:  index id to use
    AP_TextIndexCondPoolIndex       scanInfo,       // IN:  text scan info
    Four*                           osn             // OUT: scan id
)
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
    AP_TextIndexCondElement *scanNode;
    oosql_TextIR_PostingScanInfo *postingScanInfo;
    Four keywordKind;   /* keyword or reverse keyword */
    Two startKeyLen;
    Two stopKeyLen;
    char startKeyword[MAXKEYWORDSIZE];
    char stopKeyword[MAXKEYWORDSIZE];
    OOSQL_StorageManager::BoundCond startBound;
    OOSQL_StorageManager::BoundCond stopBound;
    OOSQL_StorageManager::LockParameter lockup;
    Four scanId;
    Four e;

    /* check input parameters */
#ifdef  OOSQL_DEBUG
    if(ocn < 0)
        OOSQL_ERR(eBADPARAMETER_OOSQL);
    if(indexId == NULL) 
        OOSQL_ERR(eBADPARAMETER_OOSQL);
    if(IS_NULL_POOLINDEX(scanInfo) == SM_TRUE) 
        OOSQL_ERR(eBADPARAMETER_OOSQL);
    if(osn == NULL) 
        OOSQL_ERR(eBADPARAMETER_OOSQL);
#endif

    /* get pointer to the scan node */
    scanNode = ACCESSPLAN.getTextIndexCondElem(scanInfo);

    /* get keyword kind */
    switch(scanNode->keyword.accessMethod) {
        case KEYWORD_IDX_SCAN:
            keywordKind = KEYWORD;
            break;

        case REVERSEKEYWORD_IDX_SCAN:
            keywordKind = REVERSEKEYWORD;
            break;

        default:        /* ERROR: invalid access method */
            OOSQL_ERR(eINVALID_CASE_OOSQL);
    }

    /* copy start bound keyword(string value) to char string */
    startKeyLen = GET_POOLSIZE(scanNode->keyword.startBound);
    if(startKeyLen > MAXKEYWORDSIZE)
        startKeyLen = MAXKEYWORDSIZE;
    e = copyStringValueToString(&(scanNode->keyword.startBound), &startKeyword[0], startKeyLen);
    if(e < eNOERROR)
        OOSQL_ERR(e);

    /* copy stop bound keyword(string value) to char string */
    stopKeyLen = GET_POOLSIZE(scanNode->keyword.stopBound);
    if(stopKeyLen > MAXKEYWORDSIZE)
        stopKeyLen = MAXKEYWORDSIZE;
    e = copyStringValueToString(&(scanNode->keyword.stopBound), &stopKeyword[0], stopKeyLen);
    if(e < eNOERROR)
        OOSQL_ERR(e);

    /* check if wild character is used */
    if(compareTextIR_Keyword(startKeyLen, &startKeyword[0], stopKeyLen, &stopKeyword[0]) == CMP_EQ) {
        /* make bound condition operator */
        startBound.op = OOSQL_StorageManager::SM_EQ;
        stopBound.op = OOSQL_StorageManager::SM_EQ;
    }
    else {
        /* make bound condition operator */
        startBound.op = OOSQL_StorageManager::SM_GE;
        stopBound.op = OOSQL_StorageManager::SM_LE;

        /* get pointer to the posting scan information of the Text IR evaluation buffer */
        postingScanInfo = EVAL_TEXTIR_EVALBUFELEMENTS[scanInfo.startIndex].postingScanInfo;

        /* set wild char. flag */
        postingScanInfo->m_wildCharFlag = SM_TRUE;

        /* allocate temporary file information to process wild char. by sorting */
        OOSQL_NEW(postingScanInfo->m_tempFileInfo, pMemoryManager, oosql_TextIR_TempFileInfo);
        if(postingScanInfo->m_tempFileInfo == NULL)
            OOSQL_ERR(eOUTOFMEMORY_OOSQL);
    }

    /* make start bound condition */
    startBound.key.len = sizeof(Two) + startKeyLen;
    memcpy(&(startBound.key.val[0]), &startKeyLen, sizeof(Two));
    memcpy(&(startBound.key.val[sizeof(Two)]), &startKeyword[0], startKeyLen);

    /* make start bound condition */
    stopBound.key.len = sizeof(Two) + stopKeyLen;
    memcpy(&(stopBound.key.val[0]), &stopKeyLen, sizeof(Two));
    memcpy(&(stopBound.key.val[sizeof(Two)]), &stopKeyword[0], stopKeyLen);

    /* make lockup parameter */
    if(isUpdateQuery())
        lockup.mode = OOSQL_StorageManager::L_IX;
    else
        lockup.mode = OOSQL_StorageManager::L_IS;
    lockup.duration = OOSQL_StorageManager::L_COMMIT;

    /* call LOM intf. */
    if(startBound.op == SM_EQ && startBound.op == SM_EQ && scanNode->keyword.invertedIndexEntryTupleID.volNo != NIL)
        scanId = m_storageManager->Text_OpenIndexScan_GivenInvertedEntryTupleID(ocn, colNo, &scanNode->keyword.invertedIndexEntryTupleID, &lockup);
    else
        scanId = m_storageManager->Text_OpenIndexScan(ocn, indexId, keywordKind, &startBound, &stopBound, &lockup);
    if(scanId < eNOERROR) {
        OOSQL_ERR(scanId);
    }
    else {
        *osn = scanId;
    }

    /* return */
    return eNOERROR;
}

Four    OOSQL_Evaluator::execTextIR_ConstructMinMaxHeap(
        AP_TextIndexCondPoolIndex       operInfo,       // IN:
        Four                            logicalIdHints  // IN 
)
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
    AP_TextIndexCondElement     *operNode;
    OOSQL_TextIR_Posting        *op1Posting;
    AP_TextIndexCondElement     *constNode;
    OOSQL_DB_Value              op2Const(pMemoryManager);
    AP_TextIndexCondPoolIndex   condNodeIndex;
    AP_TextIndexCondElement*    indexCondNode;
    Four                        maxNumEntries;
    OOSQL_MinMaxHeap_EntryDesc  *minMaxHeapEntryDesc;
    OOSQL_MinMaxHeap            *minMaxHeap;
    OOSQL_MinMaxHeap_KeyValue   minKeyVal;
    OOSQL_MinMaxHeap_FieldList  entry[TEXTIR_NUMFIELDS_IN_POSTING_WITHOUTPOSITION];
    OOSQL_TextIR_PostingQueue&  postingQueue = EVAL_TEXTIR_EVALBUFELEMENTS[operInfo.startIndex].op1PostingQueue;
    Four                        e;

        /* check input parameter */
#ifdef  OOSQL_DEBUG
    if(IS_NULL_POOLINDEX(operInfo) == SM_TRUE)
        OOSQL_ERR(eBADPARAMETER_OOSQL);
#endif

    /* get Text IR condition node for the current node */
    operNode = ACCESSPLAN.getTextIndexCondElem(operInfo);
#ifdef  OOSQL_DEBUG
    if(operNode == NULL) OOSQL_ERR(eNULL_POINTER_OOSQL);
#endif

    /* get pointer to the Min-Max Heap */
    minMaxHeap = EVAL_TEXTIR_EVALBUFELEMENTS[operInfo.startIndex].minMaxHeap;
#ifdef  OOSQL_DEBUG
    if(minMaxHeap == NULL) OOSQL_ERR(eNULL_POINTER_OOSQL);
#endif

    /* get Text IR condition nodes for the constant */
    constNode = ACCESSPLAN.getTextIndexCondElem(operNode->oper.op2);

    /* get the second operand which is integer constant */
    e = ACCESSPLAN.getValue(constNode->constant, &op2Const);
    if(e < eNOERROR) OOSQL_ERR(e);

    /* get the maximum # of entries in the heap */
    maxNumEntries = GET_VALUE_FROM_COL_LIST(op2Const, sizeof(Four));

    /*************************
     * create a Min-Max Heap *
     *************************/

    /* allocate memory for Min-Max Heap entry descriptor
     * NOTE: we need not to store position information in Min-Max Heap
     */
    OOSQL_ARRAYNEW(minMaxHeapEntryDesc, pMemoryManager, OOSQL_MinMaxHeap_EntryDesc, TEXTIR_NUMFIELDS_IN_POSTING_WITHOUTPOSITION);
    if(minMaxHeapEntryDesc == NULL)
        OOSQL_ERR(eOUTOFMEMORY_OOSQL);

    /* construct Min-Max Heap entry description 
     * NOTE: Memory is allocated for 'minMaxHeapEntryDesc' after executing the following function.
     *       So we should deallocate it after usage.
     */
    e = execTextIR_makeMinMaxHeapEntryDescForPosting(&minMaxHeapEntryDesc);
    if(e < eNOERROR) {
        /* deallocate memory for Min-Max Heap entry description */
        OOSQL_ARRAYDELETE(OOSQL_MinMaxHeap_EntryDesc, minMaxHeapEntryDesc);

        OOSQL_ERR(e);
    }

    /* create Min-Max Heap in the Text IR evaluation buffer */
    e = minMaxHeap->Create(m_sortBufferInfo.diskInfo.sortVolID, minMaxHeapEntryDesc);
    if(e < eNOERROR) {
        /* deallocate memory for Min-Max Heap entry description */
        OOSQL_ARRAYDELETE(OOSQL_MinMaxHeap_EntryDesc, minMaxHeapEntryDesc);

        OOSQL_ERR(e);
    }

    /* deallocate memory for Min-Max Heap entry description */
    OOSQL_ARRAYDELETE(OOSQL_MinMaxHeap_EntryDesc, minMaxHeapEntryDesc);

    /******************************
     * construct the Min-Max Heap *
     ******************************/

    /* get the first input posting(op1) */
    condNodeIndex = operNode->oper.op1;
    indexCondNode = ACCESSPLAN.getTextIndexCondElem(condNodeIndex);
    e = execTextIR_SubPlan(condNodeIndex, postingQueue, logicalIdHints);
    if(e < eNOERROR)  OOSQL_ERR(e);
    if(e == TEXTIR_EOS) return TEXTIR_EOS;

    op1Posting = &postingQueue.Head();
    postingQueue.PopHead();

    /* insert entry into the heap */
    while(e != TEXTIR_EOS) 
    {
        /* check if the # of entries in the heap is less than 'maxNumEntries' */
        if(minMaxHeap->GetHeapSize() < maxNumEntries) 
        {
            /* convert format from posting to Min-Man Heap entry */
            e = execTextIR_convertPostingToMinMaxHeapEntry(op1Posting, &entry[0]);
            if(e < eNOERROR) OOSQL_ERR(e);

            /* insert the posting to the Min-Max Heap */
            e = minMaxHeap->InsertEntry(&entry[0]);
            if(e < eNOERROR) OOSQL_ERR(e);
        }
        else 
        {
            /* the # of entries in the heap is equal to 'maxNumEntries' */
            /* get the minimum key value of the heap */
            e = minMaxHeap->RetrieveMinKeyValue(&minKeyVal);
            if(e < eNOERROR) OOSQL_ERR(e);

            /* check if the current posting is greater than the minimum entry of the heap */
            if(minKeyVal.data.d < op1Posting->weight) {
                /* convert format from posting to Min-Max Heap entry */
                e = execTextIR_convertPostingToMinMaxHeapEntry(op1Posting, &entry[0]);
                if(e < eNOERROR) OOSQL_ERR(e);

                /* replace the minimum entry as the current posting */
                e = minMaxHeap->ReplaceMinEntry(&entry[0]);
                if(e < eNOERROR) OOSQL_ERR(e);
            }
        }

        /* get the next input posting(op1) */
        while(postingQueue.IsEmpty())
        {
            condNodeIndex = operNode->oper.op1;
            indexCondNode = ACCESSPLAN.getTextIndexCondElem(condNodeIndex);
            e = execTextIR_SubPlan(condNodeIndex, postingQueue, logicalIdHints);
            if(e < eNOERROR) OOSQL_ERR(e);
            if(e == TEXTIR_EOS) return TEXTIR_EOS;
        }
        op1Posting = &postingQueue.Head();
        postingQueue.PopHead();
    }

    /* return */
    return eNOERROR;
}

Four    OOSQL_Evaluator::execTextIR_NextMinMaxHeapEntry(
        AP_TextIndexCondPoolIndex       operInfo,       // IN:
        OOSQL_TextIR_Posting    **result                // OUT:
)
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
        OOSQL_MinMaxHeap      *minMaxHeap;
        OOSQL_MinMaxHeap_FieldList maxEntry[TEXTIR_NUMFIELDS_IN_POSTING_WITHOUTPOSITION];
        OOSQL_TextIR_Posting    *currPosting;
        Four    e;

        /* check input parameters */
#ifdef  OOSQL_DEBUG
        if(IS_NULL_POOLINDEX(operInfo) == SM_TRUE) OOSQL_ERR(eBADPARAMETER_OOSQL);
        if(result == NULL) OOSQL_ERR(eBADPARAMETER_OOSQL);
#endif

        /* get pointer to the posting of the current node */
        currPosting = &(EVAL_TEXTIR_EVALBUFELEMENTS[operInfo.startIndex].posting);

        /* get pointer to the Min-Max Heap */
        minMaxHeap = EVAL_TEXTIR_EVALBUFELEMENTS[operInfo.startIndex].minMaxHeap;
#ifdef  OOSQL_DEBUG
        if(minMaxHeap == NULL) OOSQL_ERR(eNULL_POINTER_OOSQL);
#endif

        /* check if the Min-Max Heap is empty */
        if(minMaxHeap->IsEmpty() == SM_TRUE) {
            TEXTIR_SET_NULLPOSTING(currPosting);
            *result = currPosting;
            return TEXTIR_EOS;
        }

        /* extract the maximum entry from the Min-Max Heap */
        e = minMaxHeap->ExtractMaxEntry(&maxEntry[0]);
        if(e < eNOERROR) {
            OOSQL_ERR(e);
        }

        e = execTextIR_convertMinMaxHeapEntryToPosting(&maxEntry[0], currPosting);
        if(e < eNOERROR) OOSQL_ERR(e);

        /* set pointer to the result posting */
        *result = currPosting;

        /* return */
        return eNOERROR;
}


Four memoryPostingSortCompareFunc(void *arg1, void *arg2)
{
    Four                        docID1, docID2;
    OOSQL_StorageManager::OID   oid1, oid2;
    OOSQL_TextIR_Weight         weight1, weight2;
    char                        *p1, *p2;

    p1 =(char*)arg1;
    p2 =(char*)arg2;

    docID1 = *(Four*)p1; 
    p1 += sizeof(Four);
    docID2 = *(Four*)p2; 
    p2 += sizeof(Four);
    oid1 = *(OOSQL_StorageManager::OID*)p1; p1 += sizeof(OOSQL_StorageManager::OID);
    oid2 = *(OOSQL_StorageManager::OID*)p2; p2 += sizeof(OOSQL_StorageManager::OID);
    weight1 = *(OOSQL_TextIR_Weight*)p1; p1 += TEXTIR_SIZE_WEIGHT;
    weight2 = *(OOSQL_TextIR_Weight*)p2; p2 += TEXTIR_SIZE_WEIGHT;

    if(docID1 == docID2)
    {
        if(weight1 < weight2)
            return -1;
        else if(weight1 > weight2)
            return 1;
        else
            return 0;
    }
    else
        return docID1 - docID2;
}

Four    OOSQL_Evaluator::execTextIR_FetchAllAndSortPostings(
        AP_TextIndexCondPoolIndex       scanInfo
)
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
    AP_TextIndexCondElement                 *scanNode;
    OOSQL_TextIR_Posting                    *currPosting;
    oosql_TextIR_TempFileInfo               *tempFileInfo;
    Four                                    unique = 0;
    Four                                    ocn;
    Four                                    osn;
    OOSQL_StorageManager::OID               oid;
    OOSQL_StorageManager::LockParameter     lockup;
    Four                                    numTuples = 0;
    Four                                    e;
    char*                                   pSortBufferWritePtr;
    Four                                    tmpClassId;
    OOSQL_StorageManager::LockParameter     lockupForSort;
    OOSQL_StorageManager::BTreeKeyInfo      sortKeyInfo;
    OOSQL_TextIR_PostingQueue&              postingQueue = EVAL_TEXTIR_EVALBUFELEMENTS[scanInfo.startIndex].op1PostingQueue;

    /* check input parameter */
#ifdef  OOSQL_DEBUG
    if(IS_NULL_POOLINDEX(scanInfo) == SM_TRUE) 
        OOSQL_ERR(eBADPARAMETER_OOSQL);
#endif

    /* get Text IR condition node for the current node */
    scanNode = ACCESSPLAN.getTextIndexCondElem(scanInfo);

    /* fetch the first posting */
    while(postingQueue.IsEmpty())
    {
        e = execTextIR_IndexScanNextPosting(scanInfo, postingQueue, TEXTIR_POSTINGSKIP_NO_HINTS);
        if(e < eNOERROR) {
            OOSQL_ERR(e);
        }
        else if(e == TEXTIR_EOS) {
            break;
        }
    }

    if(postingQueue.IsEmpty())
        return TEXTIR_EOS;

    currPosting = &postingQueue.Head();
    postingQueue.PopHead();
    
    if(m_sortBufferInfo.mode == OOSQL_SB_USE_DISK)
    {
        // Create sort stream 
        // get pointer to the temporary file information
        tempFileInfo = EVAL_TEXTIR_EVALBUFELEMENTS[scanInfo.startIndex].postingScanInfo->m_tempFileInfo;

        // create sort stream to sorting posting 
        OOSQL_NEW(tempFileInfo->sortStream, pMemoryManager, OOSQL_SortStream(m_storageManager));

        // prepare sort key info
        sortKeyInfo.flag             = 0;
        sortKeyInfo.nColumns         = 1;
        sortKeyInfo.columns[0].colNo = 0;
        sortKeyInfo.columns[0].flag  = KEYINFO_COL_ASC;

        // create a sort stream to store the postings 
        e = tempFileInfo->sortStream->CreateStream(m_volID, TEXTIR_NUMFIELDS_IN_POSTING_WITHOUTPOSITION, tempFileInfo->attrInfo, &sortKeyInfo);
        OOSQL_CHECK_ERR(e);
    }
    else
    {
        /* get pointer to the temporary file information */
        tempFileInfo = EVAL_TEXTIR_EVALBUFELEMENTS[scanInfo.startIndex].postingScanInfo->m_tempFileInfo;

        tempFileInfo->sortBufferPtr               = m_sortBufferForStarQuery + m_usedSortBufferForStarQuery;
        tempFileInfo->sortBufferCurrentReadOffset = 0;
        pSortBufferWritePtr                       = (char*)tempFileInfo->sortBufferPtr;
    }

    /* 
     * fetch the next posting until end of scan
     */
    while(true) {
        // convert the posting to column list structure
        e = execTextIR_convertPostingToColList(scanNode->keyword.usedPostingKind, 
                                                currPosting, tempFileInfo->clist);
        OOSQL_CHECK_ERR(e);

        if(m_sortBufferInfo.mode == OOSQL_SB_USE_DISK)
        {
            // save the current posting into the sort stream
            e = tempFileInfo->sortStream->FastCreateObject(tempFileInfo->nCols, tempFileInfo->clist);
            OOSQL_CHECK_ERR(e);
        }
        else
        {
            memcpy(pSortBufferWritePtr, &currPosting->logicalDocId, sizeof(Four));
            pSortBufferWritePtr += sizeof(Four);

            memcpy(pSortBufferWritePtr, &currPosting->docId, sizeof(OOSQL_StorageManager::OID));
            pSortBufferWritePtr += sizeof(OOSQL_StorageManager::OID);

            memcpy(pSortBufferWritePtr, &currPosting->weight, TEXTIR_SIZE_WEIGHT);
            pSortBufferWritePtr += TEXTIR_SIZE_WEIGHT;
        }

        numTuples++;

        while(postingQueue.IsEmpty())
        {
            e = execTextIR_IndexScanNextPosting(scanInfo, postingQueue, TEXTIR_POSTINGSKIP_NO_HINTS);
            OOSQL_CHECK_ERR(e);
            if(e == TEXTIR_EOS) break;
        }
        if(postingQueue.IsEmpty() && e == TEXTIR_EOS)
            break;
        else
        {
            currPosting = &postingQueue.Head();
            postingQueue.PopHead();
        }
    }

    if(m_sortBufferInfo.mode == OOSQL_SB_USE_DISK)
    {
        // do sort stream 
        e = tempFileInfo->sortStream->Sort();
        OOSQL_CHECK_ERR(e);

        // open scan to traverse sort stream
        e = tempFileInfo->sortStream->OpenScan();
        OOSQL_CHECK_ERR(e);
    }
    else
    {
        e = oosql_QuickSort(pMemoryManager, tempFileInfo->sortBufferPtr, numTuples, OOSQL_MEMORYSORT_POSTINGSIZE, memoryPostingSortCompareFunc);
        OOSQL_CHECK_ERR(e);

        tempFileInfo->sortBufferLength = (numTuples * OOSQL_MEMORYSORT_POSTINGSIZE);

        m_usedSortBufferForStarQuery += tempFileInfo->sortBufferLength;
    }

    /* initialize mark flag */
    tempFileInfo->markFlag = SM_FALSE;

    /* return */
    return eNOERROR;
}


#else /* SLIMDOWN_TEXTIR */
#include "OOSQL_Evaluator.hxx"
#include "oosql_Eval_TextIR.hxx"
#include "oosql_Eval_TextIR_Index.hxx"
#include "OOSQL_SortStream.hxx"
#include <string.h>

Four    OOSQL_Evaluator::initTextIR_SubPlanNode(
    Four                            ocn,                            // IN: open class num
    Two                             colNo,                          // IN: column number
    OOSQL_StorageManager::IndexID*  indexId,                        // IN: index id
    AP_TextIndexCondPoolIndex       indexCond,                      // IN: index condition
    AP_IndexInfoPoolIndex           indexInfoPoolIndex              // IN: indexInfo
)
{
    return eTEXTIR_NOTENABLED_OOSQL;
}


Four    OOSQL_Evaluator::openTextIR_IndexScan(
    Four                            ocn,            // IN:  open class number
    Two                             colNo,          // IN:  column no
    OOSQL_StorageManager::IndexID*  indexId,        // IN:  index id to use
    AP_TextIndexCondPoolIndex       scanInfo,       // IN:  text scan info
    Four*                           osn             // OUT: scan id
)
{
    return eTEXTIR_NOTENABLED_OOSQL;
}


Four    OOSQL_Evaluator::execTextIR_ConstructMinMaxHeap(
    AP_TextIndexCondPoolIndex       operInfo,       // IN:
    Four                            logicalIdHints  // IN  
)
{
    return eTEXTIR_NOTENABLED_OOSQL;
}


Four    OOSQL_Evaluator::execTextIR_NextMinMaxHeapEntry(
    AP_TextIndexCondPoolIndex       operInfo,       // IN:
    OOSQL_TextIR_Posting    **result                // OUT:
)
{
    return eTEXTIR_NOTENABLED_OOSQL;
}


Four    OOSQL_Evaluator::execTextIR_FetchAllAndSortPostings(
    AP_TextIndexCondPoolIndex       scanInfo
)
{
    return eTEXTIR_NOTENABLED_OOSQL;
}

#endif /* SLIMDOWN_TEXTIR */

