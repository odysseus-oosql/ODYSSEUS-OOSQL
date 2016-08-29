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
        oosql_Eval_TextIR_Function.cxx

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
#include <string.h>

Four    OOSQL_Evaluator::initTextIR_SubPlanNodeForFnMatch(
        Four    ocn,                            // IN:
        OOSQL_StorageManager::OID     *docId,   // IN:
        Four    textColNo,                      // IN:
        AP_TextIndexCondPoolIndex indexCond     // IN:
)
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
        AP_TextIndexCondElement *indexCondNode;
		AP_IndexInfoPoolIndex	nullPoolIndex;
        Four    scanId;
        Four    e;

        /* check input parameters */
#ifdef  OOSQL_DEBUG
        if(ocn < 0)
                OOSQL_ERR(eBADPARAMETER_OOSQL);
        if(docId == NULL)
                OOSQL_ERR(eBADPARAMETER_OOSQL);
        if(textColNo < 0)
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
                e = initTextIR_SubPlanNodeForFnMatch(ocn, docId, textColNo, indexCondNode->oper.op1);
                if(e < eNOERROR) OOSQL_ERR(e);

                /* recursive call for the right operand(that might be keyword node) */
                e = initTextIR_SubPlanNodeForFnMatch(ocn, docId, textColNo, indexCondNode->oper.op2);
                if(e < eNOERROR) OOSQL_ERR(e);

                break;

            case TEXTINDEXCOND_KEYWORD:

                switch(indexCondNode->keyword.accessMethod) {
                    case KEYWORD_IDX_SCAN:
                    case REVERSEKEYWORD_IDX_SCAN:

                        /* open Text IR index scan for the keyword */
                        e = openTextIR_Scan(ocn, docId, textColNo, indexCond, &scanId);
                        if(e < eNOERROR) OOSQL_ERR(e);

                        /* set open scan number in the evaluation buffer */
						nullPoolIndex.setNull();

                        e = EVAL_TEXTIR_EVALBUFELEMENTS[indexCond.startIndex].postingScanInfo->setOpenClassNoAndScanNo(ocn, scanId, nullPoolIndex);
                        if(e < eNOERROR) OOSQL_ERR(e);

                        e = EVAL_TEXTIR_EVALBUFELEMENTS[indexCond.startIndex].postingScanInfo->setNumOfObjectsInClass(ocn);
						if(e < eNOERROR) OOSQL_ERR(e);

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

Four    OOSQL_Evaluator::openTextIR_Scan(
        Four    ocn,                            // IN:
        OOSQL_StorageManager::OID     *docId,   // IN:
        Four    textColNo,                      // IN:
        AP_TextIndexCondPoolIndex scanInfo,     // IN:
        Four    *osn                            // OUT:
)
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
    AP_TextIndexCondElement     *scanNode;
    oosql_TextIR_PostingScanInfo *postingScanInfo;
    char        startKeyword[MAXKEYWORDSIZE];
    char        stopKeyword[MAXKEYWORDSIZE];
    Two startKeyLen;
    Two stopKeyLen;
    OOSQL_StorageManager::BoundCond   startBound;
    OOSQL_StorageManager::BoundCond   stopBound;
    OOSQL_StorageManager::LockParameter       lockup;
    Four        scanId;
    Four        e;
    Four keywordKind;   /* keyword or reverse keyword */

    /* check input parameters */
#ifdef  OOSQL_DEBUG
    if(ocn < 0) OOSQL_ERR(eBADPARAMETER_OOSQL);
    if(docId == NULL) OOSQL_ERR(eBADPARAMETER_OOSQL);
    if(textColNo < 0) OOSQL_ERR(eBADPARAMETER_OOSQL);
    if(IS_NULL_POOLINDEX(scanInfo) == SM_TRUE) OOSQL_ERR(eBADPARAMETER_OOSQL);
    if(osn == NULL) OOSQL_ERR(eBADPARAMETER_OOSQL);
#endif

    /* get pointer to the scan node */
    scanNode = ACCESSPLAN.getTextIndexCondElem(scanInfo);

    /* reverse keyword is supported ... */
    /*  
     *  LOM Text Scan for reverse keyword is not supported, so
     *  keyword kind is useless in the current structure.
     */
    /* get keyword kind */
    switch(scanNode->keyword.accessMethod) {
      case KEYWORD_IDX_SCAN:
        keywordKind = KEYWORD;
        break;
        
      case REVERSEKEYWORD_IDX_SCAN:
        keywordKind = REVERSEKEYWORD;
        break;
        
      default:  /* ERROR: invalid access method */
        OOSQL_ERR(eINVALID_CASE_OOSQL);
    }

    /* for future extension
    /+ get char pointer to the start and stop keywords of the index scan +/
    startKeyword = ACCESSPLAN.getStringValue(scanNode->keyword.startBound);
    stopKeyword = ACCESSPLAN.getStringValue(scanNode->keyword.stopBound);
    */

    /* copy start bound keyword(string value) to char string */
    startKeyLen = GET_POOLSIZE(scanNode->keyword.startBound);
	if(startKeyLen > MAXKEYWORDSIZE)
		startKeyLen = MAXKEYWORDSIZE;
    e = copyStringValueToString(&(scanNode->keyword.startBound), &startKeyword[0], startKeyLen);
    if(e < eNOERROR) OOSQL_ERR(e);

    /* copy stop bound keyword(string value) to char string */
    stopKeyLen = GET_POOLSIZE(scanNode->keyword.stopBound);
	if(stopKeyLen > MAXKEYWORDSIZE)
		stopKeyLen = MAXKEYWORDSIZE;
    e = copyStringValueToString(&(scanNode->keyword.stopBound), &stopKeyword[0], stopKeyLen);
    if(e < eNOERROR) OOSQL_ERR(e);

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

        /* NOTE: we does not allocate temporary file information because 
         *           all logical doc. IDs of the postings are equal for one document.
         */
    }

    /* make start bound condition */
    startBound.key.len = sizeof(Two) + startKeyLen;
    memcpy(&(startBound.key.val[0]), &startKeyLen, sizeof(Two));
    memcpy(&(startBound.key.val[sizeof(Two)]), &startKeyword[0], startKeyLen);

    /* make stop bound condition */
    stopBound.key.len = sizeof(Two) + stopKeyLen;
    memcpy(&(stopBound.key.val[0]), &stopKeyLen, sizeof(Two));
    memcpy(&(stopBound.key.val[sizeof(Two)]), &stopKeyword[0], stopKeyLen);

    /* make lockup parameter */
	if(isUpdateQuery())
		lockup.mode = OOSQL_StorageManager::L_IX;
	else
		lockup.mode = OOSQL_StorageManager::L_IS;
    lockup.duration = OOSQL_StorageManager::L_COMMIT;

    /* call LOM intf. 
     * NOTE.1: LOM Text Scan does not support reverse keyword.
     * NOTE.2: pass column number smaller by one than column number in catalog 
     */
    scanId = m_storageManager->Text_Scan_Open(ocn, docId, textColNo,
								               keywordKind,
											   &startBound, &stopBound, &lockup);
    if(scanId < eNOERROR) OOSQL_ERR(scanId);
    
    /* set output parameter */
    *osn = scanId;

    /* return */
    return eNOERROR;
}

Four    OOSQL_Evaluator::closeTextIR_Scan(
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
        if(IS_NULL_POOLINDEX(indexInfo) == SM_TRUE)
                OOSQL_ERR(eBADPARAMETER_OOSQL);
#endif

        /* get Text IR condition node for the current node */
        indexNode = ACCESSPLAN.getTextIndexCondElem(indexInfo);
#ifdef  OOSQL_DEBUG
        if(indexNode == NULL) OOSQL_ERR(eNULL_POINTER_OOSQL);
#endif

        switch(indexNode->nodeKind) {
            case TEXTINDEXCOND_OPERATOR:
                /* recursive call for the first operand */
                e = closeTextIR_Scan(indexNode->oper.op1); 
                if(e < eNOERROR) OOSQL_ERR(e);

                /* recursive call for the second operand */
                e = closeTextIR_Scan(indexNode->oper.op2); 
                if(e < eNOERROR) OOSQL_ERR(e);

                break;

            case TEXTINDEXCOND_KEYWORD:
                /* get open scan num. for the current node */
                osn = EVAL_TEXTIR_EVALBUFELEMENTS[indexInfo.startIndex].postingScanInfo->m_osn;

                /* call LOM interface */
                e = m_storageManager->Text_Scan_Close(osn);
                if(e < eNOERROR) OOSQL_ERR(e);

                break;
        }

        /* return */
        return eNOERROR;
}

Four    OOSQL_Evaluator::execTextIR_SubPlanForOneDocument(
        AP_TextIndexCondPoolIndex       condNodeIndex,  // IN:
        OOSQL_TextIR_Posting    **result                // OUT:
)
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
        AP_TextIndexCondElement *indexCondNode;
        Four    e;

        /* check input parameters */
#ifdef  OOSQL_DEBUG
        if(IS_NULL_POOLINDEX(condNodeIndex) == SM_TRUE)
                OOSQL_ERR(eBADPARAMETER_OOSQL);
        if(result == NULL)
                OOSQL_ERR(eBADPARAMETER_OOSQL);
#endif

        /* get Text IR index condition node */
        indexCondNode = ACCESSPLAN.getTextIndexCondElem(condNodeIndex);
#ifdef  OOSQL_DEBUG
        if(indexCondNode == NULL) OOSQL_ERR(eNULL_POINTER_OOSQL);
#endif

        /* execute according to the kind of the current condition node */
        switch(indexCondNode->nodeKind) {
            case TEXTINDEXCOND_OPERATOR:
                switch(indexCondNode->oper.operatorID) {
                    case OP_TEXTIR_AND:
                        e = execTextIR_OpAndForOneDocument(condNodeIndex, result);
                        if(e < eNOERROR) OOSQL_ERR(e);

                        break;

                    case OP_TEXTIR_OR:
                        e = execTextIR_OpOrForOneDocument(condNodeIndex, result);
                        if(e < eNOERROR) OOSQL_ERR(e);

                        break;

                    case OP_TEXTIR_MINUS:
                        e = execTextIR_OpMinusForOneDocument(condNodeIndex, result);
                        if(e < eNOERROR) OOSQL_ERR(e);

                        break;

                    case OP_TEXTIR_ACCUMULATE:
                        e = execTextIR_OpAccumulateForOneDocument(condNodeIndex, result);
                        if(e < eNOERROR) OOSQL_ERR(e);

                        break;

                    case OP_TEXTIR_MULTIPLY:
                        e = execTextIR_OpMultiplyForOneDocument(condNodeIndex, result);
                        if(e < eNOERROR) OOSQL_ERR(e);

                        break;

                    case OP_TEXTIR_THRESHOLD:
                        e = execTextIR_OpThresholdForOneDocument(condNodeIndex, result);
                        if(e < eNOERROR) OOSQL_ERR(e);

                        break;

                    case OP_TEXTIR_NEAR:
                        e = execTextIR_OpNearForOneDocument(condNodeIndex, result);
                        if(e < eNOERROR) OOSQL_ERR(e);

                        break;

					case OP_TEXTIR_NEAR_WITH_ORDER:
                        e = execTextIR_OpNearWithOrderForOneDocument(condNodeIndex, result);
                        if(e < eNOERROR) OOSQL_ERR(e);

                        break;

                    default:    /* invalid operator */
                        OOSQL_ERR(eINVALID_CASE_OOSQL);
                }

                break;

            case TEXTINDEXCOND_KEYWORD:
                e = execTextIR_KeywordNodeForOneDocument(condNodeIndex, result);
                if(e < eNOERROR) OOSQL_ERR(e);

                break;

            case TEXTINDEXCOND_CONSTANT:
                /* ERROR: constant can not be a text index condition */

            default:    /* invalid text index condition node */
                OOSQL_ERR(eINVALID_CASE_OOSQL);
        }

        /* return */
        return eNOERROR;
}

Four    OOSQL_Evaluator::execTextIR_OpAndForOneDocument(
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
        AP_TextIndexCondElement *operNode;
        OOSQL_TextIR_Posting    *currPosting;
        OOSQL_TextIR_Posting    *op1Posting;
        OOSQL_TextIR_Posting    *op2Posting;
        OOSQL_TextIR_Weight     weight;
        OOSQL_TextIR_Bonus		bonus;
        Four    e;

        /* check input parameters */
#ifdef  OOSQL_DEBUG
        if(IS_NULL_POOLINDEX(operInfo) == SM_TRUE) OOSQL_ERR(eBADPARAMETER_OOSQL);
        if(result == NULL) OOSQL_ERR(eBADPARAMETER_OOSQL);
#endif

        /* get Text IR condition node for the current node */
        operNode = ACCESSPLAN.getTextIndexCondElem(operInfo);
#ifdef  OOSQL_DEBUG
        if(operNode == NULL) OOSQL_ERR(eNULL_POINTER_OOSQL);
#endif

        /* get pointer to the posting of the current node */
        currPosting = &(EVAL_TEXTIR_EVALBUFELEMENTS[operInfo.startIndex].posting);

        /* read the first operand(posting) without marking */
        e = execTextIR_SubPlanForOneDocument(operNode->oper.op1, &op1Posting);
        if(e < eNOERROR) {
            OOSQL_ERR(e);
        }
        else if(e == TEXTIR_EOS) {     /* end of scan */
            TEXTIR_SET_NULLPOSTING(currPosting);
            *result = currPosting;
            return TEXTIR_EOS;
        }

        /* read the second operand posting without marking */
        e = execTextIR_SubPlanForOneDocument(operNode->oper.op2, &op2Posting);
        if(e < eNOERROR) {
            OOSQL_ERR(e);
        }
        else if(e == TEXTIR_EOS) {     /* end of scan */
            TEXTIR_SET_NULLPOSTING(currPosting);
            *result = currPosting;
            return TEXTIR_EOS;
        }

        /* check if either of the two posting is null */
        if(TEXTIR_IS_NULLPOSTING(op1Posting) == SM_TRUE || 
                TEXTIR_IS_NULLPOSTING(op2Posting) == SM_TRUE) {
            TEXTIR_SET_NULLPOSTING(currPosting);
        }
        else {
            /* calculate the minimum weight of the two postings */
#if defined (EXTENDED_BOOLEAN_MODEL)
            if(op1Posting->weight < op2Posting->weight)
                weight = op1Posting->weight;
            else
                weight = op2Posting->weight;
#elif defined (HEURISTIC_MODEL)
            weight = op1Posting->weight + op2Posting->weight;
            bonus  = op1Posting->bonus + op2Posting->bonus;
#endif

            /* set the current posting */
#if defined (EXTENDED_BOOLEAN_MODEL)
            e = execTextIR_setPosting(currPosting, op1Posting, weight);
#elif defined (HEURISTIC_MODEL)
            e = execTextIR_setPosting(currPosting, op1Posting, weight, bonus);
#endif
            if(e < eNOERROR) OOSQL_ERR(e);
        }

        /* set pointer to the result posting */
        *result = currPosting;

        /* return */
        return eNOERROR;
}

Four    OOSQL_Evaluator::execTextIR_OpOrForOneDocument(
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
        AP_TextIndexCondElement *operNode;
        OOSQL_TextIR_Posting    *currPosting;
        OOSQL_TextIR_Posting    *op1Posting;
        OOSQL_TextIR_Posting    *op2Posting;
        OOSQL_TextIR_Weight     weight;
        OOSQL_TextIR_Bonus		bonus;
        Four    e;

        /* check input parameters */
#ifdef  OOSQL_DEBUG
        if(IS_NULL_POOLINDEX(operInfo) == SM_TRUE) OOSQL_ERR(eBADPARAMETER_OOSQL);
        if(result == NULL) OOSQL_ERR(eBADPARAMETER_OOSQL);
#endif

        /* get Text IR condition node for the current node */
        operNode = ACCESSPLAN.getTextIndexCondElem(operInfo);
#ifdef  OOSQL_DEBUG
        if(operNode == NULL) OOSQL_ERR(eNULL_POINTER_OOSQL);
#endif

        /* get pointer to the posting of the current node */
        currPosting = &(EVAL_TEXTIR_EVALBUFELEMENTS[operInfo.startIndex].posting);

        /* read the first operand */
        e = execTextIR_SubPlanForOneDocument(operNode->oper.op1, &op1Posting);
        if(e < eNOERROR) {
            OOSQL_ERR(e);
        }
        else if(e == TEXTIR_EOS) {
            op1Posting = NULL;
        }
        /* read the second operand */
        e = execTextIR_SubPlanForOneDocument(operNode->oper.op2, &op2Posting);
        if(e < eNOERROR) {
            OOSQL_ERR(e);
        }
        else if(e == TEXTIR_EOS) {
            op2Posting = NULL;
        }

        /* check if end of scan */
        if(TEXTIR_IS_NULLPOSTING(op1Posting) && TEXTIR_IS_NULLPOSTING(op2Posting)) {
            TEXTIR_SET_NULLPOSTING(currPosting);
        }
        else if(TEXTIR_IS_NULLPOSTING(op1Posting)) {
        /* in case op1 is null, make 'result' as op2 */
#if defined (EXTENDED_BOOLEAN_MODEL)
            e = execTextIR_setPosting(currPosting, op2Posting, op2Posting->weight);
#elif defined (HEURISTIC_MODEL)
            e = execTextIR_setPosting(currPosting, op2Posting, op2Posting->weight, op2Posting->bonus);
#endif
            if(e < eNOERROR) OOSQL_ERR(e);
        }
        else if(TEXTIR_IS_NULLPOSTING(op2Posting)) {
        /* in case op2 is null, make 'result' as op1 */
#if defined (EXTENDED_BOOLEAN_MODEL)
            e = execTextIR_setPosting(currPosting, op1Posting, op1Posting->weight);
#elif defined (HEURISTIC_MODEL)
            e = execTextIR_setPosting(currPosting, op1Posting, op1Posting->weight, op1Posting->bonus);
#endif
            if(e < eNOERROR) OOSQL_ERR(e);
        }
        else {
#if defined (EXTENDED_BOOLEAN_MODEL)
            /* calculate the maximum weight of the two postings */
            if(op1Posting->weight < op2Posting->weight)
                weight = op2Posting->weight;
            else
                weight = op1Posting->weight;
#elif defined (HEURISTIC_MODEL)
            weight = op1Posting->weight + op2Posting->weight;
            bonus = op1Posting->bonus + op2Posting->bonus;
#endif

            /* set result posting */
#if defined (EXTENDED_BOOLEAN_MODEL)
            e = execTextIR_setPosting(currPosting, op1Posting, weight);
#elif defined (HEURISTIC_MODEL)
            e = execTextIR_setPosting(currPosting, op1Posting, weight, bonus);
#endif
            if(e < eNOERROR) OOSQL_ERR(e);
        }

        /* set pointer to the result posting */
        *result = currPosting;

        /* return */
        return eNOERROR;
}

Four    OOSQL_Evaluator::execTextIR_OpMinusForOneDocument(
        AP_TextIndexCondPoolIndex       operInfo, 
        OOSQL_TextIR_Posting    **result 
)
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
        AP_TextIndexCondElement *operNode;
        OOSQL_TextIR_Posting    *currPosting;
        OOSQL_TextIR_Posting    *op1Posting;
        OOSQL_TextIR_Posting    *op2Posting;
        OOSQL_TextIR_Weight     weight;
        OOSQL_TextIR_Bonus		bonus;
        Four    e;

        /* check input parameters */
#ifdef  OOSQL_DEBUG
        if(IS_NULL_POOLINDEX(operInfo) == SM_TRUE) OOSQL_ERR(eBADPARAMETER_OOSQL);
        if(result == NULL) OOSQL_ERR(eBADPARAMETER_OOSQL);
#endif

        /* get Text IR condition node for the current node */
        operNode = ACCESSPLAN.getTextIndexCondElem(operInfo);
#ifdef  OOSQL_DEBUG
        if(operNode == NULL) OOSQL_ERR(eNULL_POINTER_OOSQL);
#endif

        /* get pointer to the posting of the current node */
        currPosting = &(EVAL_TEXTIR_EVALBUFELEMENTS[operInfo.startIndex].posting);

        /* read the first operand */
        e = execTextIR_SubPlanForOneDocument(operNode->oper.op1, &op1Posting);
        if(e < eNOERROR) {
            OOSQL_ERR(e);
        }
        else if(e == TEXTIR_EOS) {
            TEXTIR_SET_NULLPOSTING(currPosting);
            *result = currPosting;
            return TEXTIR_EOS;
        }

        /* read the second operand */
        e = execTextIR_SubPlanForOneDocument(operNode->oper.op2, &op2Posting);
        if(e < eNOERROR) {
            OOSQL_ERR(e);
        }
        else if(e == TEXTIR_EOS) {
            op2Posting = NULL;
        }

        /* in case op2 is null, make 'result' as op1 */
        if(TEXTIR_IS_NULLPOSTING(op2Posting)) {
#if defined (EXTENDED_BOOLEAN_MODEL)
            e = execTextIR_setPosting(currPosting, op1Posting, op1Posting->weight);
#elif defined (HEURISTIC_MODEL)
            weight = op1Posting->weight * HEURISTIC_MINUS_WEIGHT_MULTIPLY_VALUE;
            bonus  = op1Posting->bonus + op2Posting->bonus;
            e = execTextIR_setPosting(currPosting, op1Posting, weight, bonus);
#endif
            if(e < eNOERROR) OOSQL_ERR(e);
        }
        else {
#if defined (EXTENDED_BOOLEAN_MODEL)
            /* calculate the difference of the weights of the two postings 
             * NOTE: If the result is less than 0, make it 0 
             *       which means the current document don't have the keyword. 
             */
            weight = op1Posting->weight - op2Posting->weight;
            if(weight < 0) weight = 0;

            /* set result posting */
            e = execTextIR_setPosting(currPosting, op1Posting, weight);
            if(e < eNOERROR) OOSQL_ERR(e);
#elif defined (HEURISTIC_MODEL)
			weight = bonus = 0;
            e = execTextIR_setPosting(currPosting, op1Posting, weight, bonus);
            if(e < eNOERROR) OOSQL_ERR(e);
#endif
        }

        /* set pointer to the result posting */
        *result = currPosting;

        /* return */
        return eNOERROR;
}

Four    OOSQL_Evaluator::execTextIR_OpAccumulateForOneDocument(
        AP_TextIndexCondPoolIndex       operInfo, 
        OOSQL_TextIR_Posting    **result 
)
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
        AP_TextIndexCondElement *operNode;
        OOSQL_TextIR_Posting    *currPosting;
        OOSQL_TextIR_Posting    *op1Posting;
        OOSQL_TextIR_Posting    *op2Posting;
        OOSQL_TextIR_Weight     weight;
        OOSQL_TextIR_Bonus		bonus;
        Four    e;

        /* check input parameters */
#ifdef  OOSQL_DEBUG
        if(IS_NULL_POOLINDEX(operInfo) == SM_TRUE) OOSQL_ERR(eBADPARAMETER_OOSQL);
        if(result == NULL) OOSQL_ERR(eBADPARAMETER_OOSQL);
#endif

        /* get Text IR condition node for the current node */
        operNode = ACCESSPLAN.getTextIndexCondElem(operInfo);
#ifdef  OOSQL_DEBUG
        if(operNode == NULL) OOSQL_ERR(eNULL_POINTER_OOSQL);
#endif

        /* get pointer to the posting of the current node */
        currPosting = &(EVAL_TEXTIR_EVALBUFELEMENTS[operInfo.startIndex].posting);

        /* read the first operand */
        e = execTextIR_SubPlanForOneDocument(operNode->oper.op1, &op1Posting);
        if(e < eNOERROR) {
            OOSQL_ERR(e);
        }
        else if(e == TEXTIR_EOS) {
            op1Posting = NULL;
        }

        /* read the second operand */
        e = execTextIR_SubPlanForOneDocument(operNode->oper.op2, &op2Posting);
        if(e < eNOERROR) {
            OOSQL_ERR(e);
        }
        else if(e == TEXTIR_EOS) {
            op2Posting = NULL;
        }

        /* check if end of scan */
        if(TEXTIR_IS_NULLPOSTING(op1Posting) && TEXTIR_IS_NULLPOSTING(op2Posting)) {
            TEXTIR_SET_NULLPOSTING(currPosting);
        }
        else if(TEXTIR_IS_NULLPOSTING(op1Posting)) {
        /* in case op1 is null, make 'result' as op2 */
#if defined (EXTENDED_BOOLEAN_MODEL)
            e = execTextIR_setPosting(currPosting, op2Posting, op2Posting->weight);
#elif defined (HEURISTIC_MODEL)
            e = execTextIR_setPosting(currPosting, op2Posting, op2Posting->weight, op2Posting->bonus);
#endif
            if(e < eNOERROR) OOSQL_ERR(e);
        }
        else if(TEXTIR_IS_NULLPOSTING(op2Posting)) {
        /* in case op2 is null, make 'result' as op1 */
#if defined (EXTENDED_BOOLEAN_MODEL)
            e = execTextIR_setPosting(currPosting, op1Posting, op1Posting->weight);
#elif defined (HEURISTIC_MODEL)
            e = execTextIR_setPosting(currPosting, op1Posting, op1Posting->weight, op1Posting->bonus);
#endif
            if(e < eNOERROR) OOSQL_ERR(e);
        }
        else {
            /* calculate the accumulated weight of the two postings */
            weight = op1Posting->weight + op2Posting->weight;

            /* set result posting */
#if defined (EXTENDED_BOOLEAN_MODEL)
            e = execTextIR_setPosting(currPosting, op1Posting, weight);
#elif defined (HEURISTIC_MODEL)
            bonus = op1Posting->bonus + op2Posting->bonus;
            e = execTextIR_setPosting(currPosting, op1Posting, weight, bonus);
#endif
            if(e < eNOERROR) OOSQL_ERR(e);
        }

        /* set pointer to the result posting */
        *result = currPosting;

        /* return */
        return eNOERROR;
}

Four    OOSQL_Evaluator::execTextIR_OpMultiplyForOneDocument(
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
        AP_TextIndexCondElement *operNode;
        OOSQL_TextIR_Posting    *currPosting;
        OOSQL_TextIR_Posting    *op1Posting;
        AP_TextIndexCondElement *constNode;
        OOSQL_DB_Value			op2Const(pMemoryManager);
        OOSQL_TextIR_Weight     multipliedWeight;
        Four    e;

        /* check input parameters */
#ifdef  OOSQL_DEBUG
        if(IS_NULL_POOLINDEX(operInfo) == SM_TRUE) OOSQL_ERR(eBADPARAMETER_OOSQL);
        if(result == NULL) OOSQL_ERR(eBADPARAMETER_OOSQL);
#endif

        /* get Text IR condition node for the current node */
        operNode = ACCESSPLAN.getTextIndexCondElem(operInfo);
#ifdef  OOSQL_DEBUG
        if(operNode == NULL) OOSQL_ERR(eNULL_POINTER_OOSQL);
#endif

        /* get pointer to the posting of the current node */
        currPosting = &(EVAL_TEXTIR_EVALBUFELEMENTS[operInfo.startIndex].posting);

        /* read the first operand */
        e = execTextIR_SubPlanForOneDocument(operNode->oper.op1, &op1Posting);
        if(e < eNOERROR) {
            OOSQL_ERR(e);
        }
        else if(e == TEXTIR_EOS) {
            TEXTIR_SET_NULLPOSTING(currPosting);
            *result = currPosting;
            return TEXTIR_EOS;
        }

        /* get Text IR condition nodes for the constant */
        constNode = ACCESSPLAN.getTextIndexCondElem(operNode->oper.op2);

        /* read the second operand(which is float constant) */
        e = ACCESSPLAN.getValue(constNode->constant, &op2Const);
        if(e < eNOERROR) OOSQL_ERR(e);

        /* calculate the result weight */
        if(op2Const.type == OOSQL_TYPE_long)
            multipliedWeight = op1Posting->weight * op2Const.data.l;
        else if(op2Const.type == OOSQL_TYPE_long_long)
            multipliedWeight = op1Posting->weight * op2Const.data.ll;
        else 
            multipliedWeight = op1Posting->weight * op2Const.data.d;

        /* set result posting */
#if defined (EXTENDED_BOOLEAN_MODEL)
        e = execTextIR_setPosting(currPosting, op1Posting, multipliedWeight);
#elif defined (HEURISTIC_MODEL)
        e = execTextIR_setPosting(currPosting, op1Posting, multipliedWeight, op1Posting->bonus);
#endif
        if(e < eNOERROR) OOSQL_ERR(e);

        /* set pointer to the result posting */
        *result = currPosting;

        /* return */
        return eNOERROR;
}

Four    OOSQL_Evaluator::execTextIR_OpThresholdForOneDocument(
        AP_TextIndexCondPoolIndex       operInfo, 
        OOSQL_TextIR_Posting    **result 
)
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
        AP_TextIndexCondElement *operNode;
        OOSQL_TextIR_Posting    *currPosting;
        OOSQL_TextIR_Posting    *op1Posting;
        AP_TextIndexCondElement *constNode;
        OOSQL_DB_Value        op2Const(pMemoryManager);
        Four    e;

        /* check input parameters */
#ifdef  OOSQL_DEBUG
        if(IS_NULL_POOLINDEX(operInfo) == SM_TRUE) OOSQL_ERR(eBADPARAMETER_OOSQL);
        if(result == NULL) OOSQL_ERR(eBADPARAMETER_OOSQL);
#endif

        /* get Text IR condition node for the current node */
        operNode = ACCESSPLAN.getTextIndexCondElem(operInfo);
#ifdef  OOSQL_DEBUG
        if(operNode == NULL) OOSQL_ERR(eNULL_POINTER_OOSQL);
#endif

        /* get pointer to the posting of the current node */
        currPosting = &(EVAL_TEXTIR_EVALBUFELEMENTS[operInfo.startIndex].posting);

        /* get Text IR condition nodes for the constant */
        constNode = ACCESSPLAN.getTextIndexCondElem(operNode->oper.op2);

        /* read the second operand(which is integer constant) */
        e = ACCESSPLAN.getValue(constNode->constant, &op2Const);
        if(e < eNOERROR) OOSQL_ERR(e);

        /* read the first operand */
        e = execTextIR_SubPlanForOneDocument(operNode->oper.op1, &op1Posting);
        if(e < eNOERROR) {
            OOSQL_ERR(e);
        }
        else if(e == TEXTIR_EOS) {
            op1Posting = NULL;
        }

        /* check if the posting satisfies the threshold */
        if(TEXTIR_IS_NULLPOSTING(op1Posting)) {
                TEXTIR_SET_NULLPOSTING(currPosting);
        }
        else if(op2Const.type == OOSQL_TYPE_long && op1Posting->weight <= op2Const.data.l) { 
                TEXTIR_SET_NULLPOSTING(currPosting);
        }
        else if(op2Const.type == OOSQL_TYPE_long_long && op1Posting->weight <= op2Const.data.ll) { 
                TEXTIR_SET_NULLPOSTING(currPosting);
        }
        else if(op2Const.type == OOSQL_TYPE_double && op1Posting->weight <= op2Const.data.d)
        {
                TEXTIR_SET_NULLPOSTING(currPosting);
        }
        else {
            /* set result posting */
#if defined (EXTENDED_BOOLEAN_MODEL)
            e = execTextIR_setPosting(currPosting, op1Posting, op1Posting->weight);
#elif defined (HEURISTIC_MODEL)
            e = execTextIR_setPosting(currPosting, op1Posting, op1Posting->weight, op1Posting->bonus);
#endif
            if(e < eNOERROR) OOSQL_ERR(e);
        }

        /* set pointer to the result posting */
        *result = currPosting;

        /* return */
        return eNOERROR;
}

Four    OOSQL_Evaluator::execTextIR_OpNearForOneDocument(
        AP_TextIndexCondPoolIndex       operInfo, 
        OOSQL_TextIR_Posting    **result 
)
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
        AP_TextIndexCondElement		*operNode;
        OOSQL_TextIR_Posting		*currPosting;
        OOSQL_TextIR_Posting		*op1Posting;
        OOSQL_TextIR_Posting		*op2Posting;
        OOSQL_TextIR_Weight			weight;
        OOSQL_TextIR_Bonus			bonus;
        Four						e;
		AP_TextIndexCondPoolIndex	condNodeIndex;
		AP_TextIndexCondElement*	indexCondNode;
		Four						distance;

        /* check input parameters */
#ifdef  OOSQL_DEBUG
        if(IS_NULL_POOLINDEX(operInfo) == SM_TRUE) OOSQL_ERR(eBADPARAMETER_OOSQL);
        if(result == NULL) OOSQL_ERR(eBADPARAMETER_OOSQL);
#endif

        /* get Text IR condition node for the current node */
        operNode = ACCESSPLAN.getTextIndexCondElem(operInfo);

#ifdef  OOSQL_DEBUG
        if(operNode == NULL) OOSQL_ERR(eNULL_POINTER_OOSQL);
#endif

		condNodeIndex = operNode->oper.op3;
		indexCondNode = ACCESSPLAN.getTextIndexCondElem(condNodeIndex);
		distance = indexCondNode->constant.getElements(m_pool->valuePool, 0).integer.getElements(m_pool->intPool, 0);

        /* get pointer to the posting of the current node */
        currPosting = &(EVAL_TEXTIR_EVALBUFELEMENTS[operInfo.startIndex].posting);

        /* read the first operand(posting) without marking */
        e = execTextIR_SubPlanForOneDocument(operNode->oper.op1, &op1Posting);
        if(e < eNOERROR) {
            OOSQL_ERR(e);
        }
        else if(e == TEXTIR_EOS) {     /* end of scan */
            TEXTIR_SET_NULLPOSTING(currPosting);
            *result = currPosting;
            return TEXTIR_EOS;
        }

        /* read the second operand posting without marking */
        e = execTextIR_SubPlanForOneDocument(operNode->oper.op2, &op2Posting);
        if(e < eNOERROR) {
            OOSQL_ERR(e);
        }
        else if(e == TEXTIR_EOS) {     /* end of scan */
            TEXTIR_SET_NULLPOSTING(currPosting);
            *result = currPosting;
            return TEXTIR_EOS;
        }

        /* check if either of the two posting is null */
        if(TEXTIR_IS_NULLPOSTING(op1Posting) == SM_TRUE || TEXTIR_IS_NULLPOSTING(op2Posting) == SM_TRUE) 
		{
            TEXTIR_SET_NULLPOSTING(currPosting);
        }
        else 
		{
			if(execTextIR_OpNear_IsNear(op1Posting, op2Posting, distance))
			{
#if defined (EXTENDED_BOOLEAN_MODEL)
				/* calculate the minimum weight of the two postings */
				if(op1Posting->weight < op2Posting->weight) 
					weight = op1Posting->weight;
				else 
                    weight = op2Posting->weight;
#elif defined (HEURISTIC_MODEL)
                weight = op1Posting->weight + op2Posting->weight;
                bonus  = op1Posting->bonus + op2Posting->bonus;
#endif

				/* set the current posting */
#if defined (EXTENDED_BOOLEAN_MODEL)
				e = execTextIR_setPosting(currPosting, op1Posting, weight);
#elif defined (HEURISTIC_MODEL)
				e = execTextIR_setPosting(currPosting, op1Posting, weight, bonus);
#endif
				if(e < eNOERROR) OOSQL_ERR(e);
			}
			else
			{
				TEXTIR_SET_NULLPOSTING(currPosting);
			}
        }

        /* set pointer to the result posting */
        *result = currPosting;

        /* return */
        return eNOERROR;
}

Four    OOSQL_Evaluator::execTextIR_OpNearWithOrderForOneDocument(
        AP_TextIndexCondPoolIndex       operInfo, 
        OOSQL_TextIR_Posting    **result 
)
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
        AP_TextIndexCondElement		*operNode;
        OOSQL_TextIR_Posting		*currPosting;
        OOSQL_TextIR_Posting		*op1Posting;
        OOSQL_TextIR_Posting		*op2Posting;
        OOSQL_TextIR_Weight			weight;
        OOSQL_TextIR_Bonus			bonus;
        Four						e;
		AP_TextIndexCondPoolIndex	condNodeIndex;
		AP_TextIndexCondElement*	indexCondNode;
		Four						distance;

        /* check input parameters */
#ifdef  OOSQL_DEBUG
        if(IS_NULL_POOLINDEX(operInfo) == SM_TRUE) OOSQL_ERR(eBADPARAMETER_OOSQL);
        if(result == NULL) OOSQL_ERR(eBADPARAMETER_OOSQL);
#endif

        /* get Text IR condition node for the current node */
        operNode = ACCESSPLAN.getTextIndexCondElem(operInfo);

#ifdef  OOSQL_DEBUG
        if(operNode == NULL) OOSQL_ERR(eNULL_POINTER_OOSQL);
#endif

		condNodeIndex = operNode->oper.op3;
		indexCondNode = ACCESSPLAN.getTextIndexCondElem(condNodeIndex);
		distance = indexCondNode->constant.getElements(m_pool->valuePool, 0).integer.getElements(m_pool->intPool, 0);

        /* get pointer to the posting of the current node */
        currPosting = &(EVAL_TEXTIR_EVALBUFELEMENTS[operInfo.startIndex].posting);

        /* read the first operand(posting) without marking */
        e = execTextIR_SubPlanForOneDocument(operNode->oper.op1, &op1Posting);
        if(e < eNOERROR) {
            OOSQL_ERR(e);
        }
        else if(e == TEXTIR_EOS) {     /* end of scan */
            TEXTIR_SET_NULLPOSTING(currPosting);
            *result = currPosting;
            return TEXTIR_EOS;
        }

        /* read the second operand posting without marking */
        e = execTextIR_SubPlanForOneDocument(operNode->oper.op2, &op2Posting);
        if(e < eNOERROR) {
            OOSQL_ERR(e);
        }
        else if(e == TEXTIR_EOS) {     /* end of scan */
            TEXTIR_SET_NULLPOSTING(currPosting);
            *result = currPosting;
            return TEXTIR_EOS;
        }

        /* check if either of the two posting is null */
        if(TEXTIR_IS_NULLPOSTING(op1Posting) == SM_TRUE || TEXTIR_IS_NULLPOSTING(op2Posting) == SM_TRUE) 
		{
            TEXTIR_SET_NULLPOSTING(currPosting);
        }
        else 
		{
			if(execTextIR_OpNear_IsNearWithOrder(op1Posting, op2Posting, distance))
			{
#if defined (EXTENDED_BOOLEAN_MODEL)
				/* calculate the minimum weight of the two postings */
				if(op1Posting->weight < op2Posting->weight) 
					weight = op1Posting->weight;
				else 
                    weight = op2Posting->weight;
#elif defined (HEURISTIC_MODEL)
                weight = op1Posting->weight + op2Posting->weight;
                bonus  = op1Posting->bonus + op2Posting->bonus;
#endif

				/* set the current posting */
#if defined (EXTENDED_BOOLEAN_MODEL)
				e = execTextIR_setPosting(currPosting, op1Posting, weight);
#elif defined (HEURISTIC_MODEL)
				e = execTextIR_setPosting(currPosting, op1Posting, weight, bonus);
#endif
				if(e < eNOERROR) OOSQL_ERR(e);
			}
			else
			{
				TEXTIR_SET_NULLPOSTING(currPosting);
			}
        }

        /* set pointer to the result posting */
        *result = currPosting;

        /* return */
        return eNOERROR;
}

Four    OOSQL_Evaluator::execTextIR_KeywordNodeForOneDocument(
        AP_TextIndexCondPoolIndex       keywordInfo,    // IN:
        OOSQL_TextIR_Posting    **result                // OUT:
)
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
        AP_TextIndexCondElement *keywordNode;
        OOSQL_TextIR_Posting    *currPosting;
        oosql_TextIR_PostingScanInfo *postingScanInfo;
        Four    e;

        /* check input parameters */
#ifdef  OOSQL_DEBUG
        if(IS_NULL_POOLINDEX(keywordInfo) == SM_TRUE) OOSQL_ERR(eBADPARAMETER_OOSQL);
        if(result == NULL) OOSQL_ERR(eBADPARAMETER_OOSQL);
#endif

        /* get Text IR condition node for the current node */
        keywordNode = ACCESSPLAN.getTextIndexCondElem(keywordInfo);
#ifdef  OOSQL_DEBUG
        if(keywordNode == NULL) OOSQL_ERR(eNULL_POINTER_OOSQL);
#endif

        /* get pointer to the posting of the current node */
        currPosting = &(EVAL_TEXTIR_EVALBUFELEMENTS[keywordInfo.startIndex].posting);

        /* get pointer to the posting scan information */
        postingScanInfo = EVAL_TEXTIR_EVALBUFELEMENTS[keywordInfo.startIndex].postingScanInfo;

        /* check if wild character is used in the input keyword */
        e = execTextIR_ScanNextPosting(keywordInfo, &currPosting);
        if(e < eNOERROR) {
            OOSQL_ERR(e);
        }
        else if(e == TEXTIR_EOS) {
            TEXTIR_SET_NULLPOSTING(currPosting);
            *result = currPosting;
            return TEXTIR_EOS;
        }
        
        /* set pointer to the result posting */
        *result = currPosting;

        /* return */
        return eNOERROR;
}

Four    OOSQL_Evaluator::execTextIR_ScanNextPosting(
        AP_TextIndexCondPoolIndex       scanInfo,       // IN:
        OOSQL_TextIR_Posting    **result                // OUT:
)
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
        AP_TextIndexCondElement *scanNode;
        OOSQL_TextIR_Posting    *currPosting;
        oosql_TextIR_PostingScanInfo    *postingScanInfo;
        char    currKeyword[MAXKEYWORDSIZE];
        char    inputKeyword[MAXKEYWORDSIZE+1];
        Two     keywordLen;
        Four    requiredBufferSize;
        Four    e;
        OOSQL_TextIR_Weight   maxWeight;   
        OOSQL_TextIR_Posting  tempPosting;
		OOSQL_TextIR_Weight   weight;
        
        /* check input parameters */
#ifdef  OOSQL_DEBUG
        if(IS_NULL_POOLINDEX(scanInfo) == SM_TRUE) OOSQL_ERR(eBADPARAMETER_OOSQL);
        if(result == NULL) OOSQL_ERR(eBADPARAMETER_OOSQL);
#endif

        /* get Text IR condition node for the current node */
        scanNode = ACCESSPLAN.getTextIndexCondElem(scanInfo);
#ifdef  OOSQL_DEBUG
        if(scanNode == NULL) OOSQL_ERR(eNULL_POINTER_OOSQL);
#endif

        /* get pointer to the posting of the current node */
        currPosting = &(EVAL_TEXTIR_EVALBUFELEMENTS[scanInfo.startIndex].posting);

        /* get pointer to the posting scan information */
        postingScanInfo = EVAL_TEXTIR_EVALBUFELEMENTS[scanInfo.startIndex].postingScanInfo;


        /* fetch the next posting */
        TEXTIR_SET_NULLPOSTING(currPosting);
        maxWeight = -1;
#ifndef USE_WITHOUT_WEIGHT
        while((e = m_storageManager->Text_Scan_NextPosting(postingScanInfo->m_osn,
															postingScanInfo->m_postingBufferSize, 
															postingScanInfo->m_postingBuffer,
															&requiredBufferSize)) != EOS)
#else
		while((e = m_storageManager->Text_Scan_NextPosting(postingScanInfo->m_osn,
															postingScanInfo->m_postingBufferSize, 
															postingScanInfo->m_postingBuffer,
															&requiredBufferSize, &weight)) != EOS)
#endif
        {
			if(e == eBIGGERPOSTINGBUFFERNEEDED_OOSQL)
			{
				while(postingScanInfo->m_postingBufferSize <= requiredBufferSize)
				{
					e = postingScanInfo->doublePostingBuffer();
					OOSQL_CHECK_ERR(e);
				}
#ifndef USE_WITHOUT_WEIGHT
				e = m_storageManager->Text_Scan_NextPosting(postingScanInfo->m_osn,
															postingScanInfo->m_postingBufferSize, 
															postingScanInfo->m_postingBuffer,
															&requiredBufferSize);
#else
				e = m_storageManager->Text_Scan_NextPosting(postingScanInfo->m_osn,
															postingScanInfo->m_postingBufferSize, 
															postingScanInfo->m_postingBuffer,
															&requiredBufferSize, &weight);
#endif
			}

            if(e < eNOERROR) {
                OOSQL_ERR(e);
            }

            /* reset the control information for the posting buffer 
             * NOTE: the # of returned posting from LOM intf. is always one
             */
            postingScanInfo->resetPostingBuffer(1, scanNode->keyword.scanDirection);

            postingScanInfo->getNextPostingWithoutDocId(&tempPosting);
#ifdef USE_WITHOUT_WEIGHT
			tempPosting.weight = weight;
#endif
#if defined (HEURISTIC_MODEL)
            tempPosting.bonus  = HEURISTIC_BOUNS_WEIGHT_VALUE;
#endif

            /* check if keyword filtering is necessary */
            if(IS_NULL_POOLINDEX(scanNode->keyword.keywordWithWildChar) == SM_FALSE)
            {
                /* get keyword for the current scan cursor */
                e = m_storageManager->Text_GetCursorKeyword(postingScanInfo->m_osn,
													         &currKeyword[0]);
                if(e < eNOERROR) OOSQL_ERR(e);

                /* copy input keyword to char string */
                keywordLen = GET_POOLSIZE(scanNode->keyword.keywordWithWildChar);
                e = copyStringValueToString(&(scanNode->keyword.keywordWithWildChar), 
                        &inputKeyword[0], keywordLen);
                if(e < eNOERROR) OOSQL_ERR(e);

                /* make the input keyword null-terminated */
                inputKeyword[keywordLen] = NULL;

                /* check if the current keyword satisfies the input keyword */
                // currKeyword is varstring which is encoded
                e = OQL_EvalOpLike(&currKeyword[2],(char*)inputKeyword);
                if(e == LIKE_FALSE)
                    continue;
            }

            /* find maximum weighted keyword */
            if(tempPosting.weight > maxWeight)
            {
                maxWeight    = tempPosting.weight;
                *currPosting = tempPosting;
            }
        }

        /* set pointer to the result posting */
        *result = currPosting;

        /* return */
        return eNOERROR;
}

#else /* SLIMDOWN_TEXTIR */

/*
 * include files
 */
#include "OOSQL_Evaluator.hxx"
#include "oosql_Eval_TextIR.hxx"
#include "oosql_Eval_TextIR_Index.hxx"
#include <string.h>

Four    OOSQL_Evaluator::initTextIR_SubPlanNodeForFnMatch(
        Four    ocn,                            // IN:
        OOSQL_StorageManager::OID     *docId,   // IN:
        Four    textColNo,                      // IN:
        AP_TextIndexCondPoolIndex indexCond     // IN:
)
{
	return eTEXTIR_NOTENABLED_OOSQL;
}


Four    OOSQL_Evaluator::openTextIR_Scan(
        Four    ocn,                            // IN:
        OOSQL_StorageManager::OID     *docId,   // IN:
        Four    textColNo,                      // IN:
        AP_TextIndexCondPoolIndex scanInfo,     // IN:
        Four    *osn                            // OUT:
)
{
	return eTEXTIR_NOTENABLED_OOSQL;
}


Four    OOSQL_Evaluator::closeTextIR_Scan(
        AP_TextIndexCondPoolIndex       indexInfo       // IN:
)
{
	return eTEXTIR_NOTENABLED_OOSQL;
}


Four    OOSQL_Evaluator::execTextIR_SubPlanForOneDocument(
        AP_TextIndexCondPoolIndex       condNodeIndex,  // IN:
        OOSQL_TextIR_Posting    **result                // OUT:
)
{
	return eTEXTIR_NOTENABLED_OOSQL;
}


Four    OOSQL_Evaluator::execTextIR_OpAndForOneDocument(
        AP_TextIndexCondPoolIndex       operInfo,       // IN:
        OOSQL_TextIR_Posting    **result                // OUT:
)
{
	return eTEXTIR_NOTENABLED_OOSQL;
}


Four    OOSQL_Evaluator::execTextIR_OpOrForOneDocument(
        AP_TextIndexCondPoolIndex       operInfo,       // IN:
        OOSQL_TextIR_Posting    **result                // OUT:
)
{
	return eTEXTIR_NOTENABLED_OOSQL;
}


Four    OOSQL_Evaluator::execTextIR_OpMinusForOneDocument(
        AP_TextIndexCondPoolIndex       operInfo, 
        OOSQL_TextIR_Posting    **result 
)
{
	return eTEXTIR_NOTENABLED_OOSQL;
}


Four    OOSQL_Evaluator::execTextIR_OpAccumulateForOneDocument(
        AP_TextIndexCondPoolIndex       operInfo, 
        OOSQL_TextIR_Posting    **result 
)
{
	return eTEXTIR_NOTENABLED_OOSQL;
}


Four    OOSQL_Evaluator::execTextIR_OpMultiplyForOneDocument(
        AP_TextIndexCondPoolIndex       operInfo,       // IN:
        OOSQL_TextIR_Posting    **result                // OUT:
)
{
	return eTEXTIR_NOTENABLED_OOSQL;
}


Four    OOSQL_Evaluator::execTextIR_OpThresholdForOneDocument(
        AP_TextIndexCondPoolIndex       operInfo, 
        OOSQL_TextIR_Posting    **result 
)
{
	return eTEXTIR_NOTENABLED_OOSQL;
}

Four    OOSQL_Evaluator::execTextIR_OpNearForOneDocument(
        AP_TextIndexCondPoolIndex       operInfo, 
        OOSQL_TextIR_Posting    **result 
)
{
	return eTEXTIR_NOTENABLED_OOSQL;
}


Four    OOSQL_Evaluator::execTextIR_OpNearWithOrderForOneDocument(
        AP_TextIndexCondPoolIndex       operInfo, 
        OOSQL_TextIR_Posting    **result 
)
{
	return eTEXTIR_NOTENABLED_OOSQL;
}


Four    OOSQL_Evaluator::execTextIR_KeywordNodeForOneDocument(
        AP_TextIndexCondPoolIndex       keywordInfo,    // IN:
        OOSQL_TextIR_Posting    **result                // OUT:
)
{
	return eTEXTIR_NOTENABLED_OOSQL;
}


Four    OOSQL_Evaluator::execTextIR_ScanNextPosting(
        AP_TextIndexCondPoolIndex       scanInfo,       // IN:
        OOSQL_TextIR_Posting    **result                // OUT:
)
{
	return eTEXTIR_NOTENABLED_OOSQL;
}


#endif /* SLIMDOWN_TEXTIR */
