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

#ifndef _OOSQL_EVAL_TEXTIR_INDEX_HXX_
#define _OOSQL_EVAL_TEXTIR_INDEX_HXX_

#include "OOSQL_Evaluator.hxx"
#include "OOSQL_SortStream.hxx"

inline Four OOSQL_Evaluator::execTextIR_SubPlan(
    AP_TextIndexCondPoolIndex   condNodeIndex,      // IN    
    OOSQL_TextIR_PostingQueue&  postingQueue,       // INOUT
    Four                        logicalIdHints      // IN   
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
#endif
    /* get Text IR index condition node */
    indexCondNode = ACCESSPLAN.getTextIndexCondElem(condNodeIndex);
#ifdef  OOSQL_DEBUG
    if(indexCondNode == NULL) OOSQL_ERR(eNULL_POINTER_OOSQL);
#endif

    /* execute according to the kind of the current condition node */
    switch(indexCondNode->nodeKind) 
    {
    case TEXTINDEXCOND_OPERATOR:
        switch(indexCondNode->oper.operatorID) 
        {
        /* comparison operator */
        case OP_EQ:
        case OP_NE:
        case OP_GT:
        case OP_GE:
        case OP_LT:
        case OP_LE:
            e = execTextIR_CmpOp(condNodeIndex, postingQueue, logicalIdHints);
            if(e < eNOERROR) OOSQL_ERR(e);
            if(e == TEXTIR_EOS) return TEXTIR_EOS;

            break;

        case OP_TEXTIR_AND:
            e = execTextIR_OpAnd(condNodeIndex, postingQueue, logicalIdHints);
            if(e < eNOERROR) OOSQL_ERR(e);
            if(e == TEXTIR_EOS) return TEXTIR_EOS;

            break;

        case OP_TEXTIR_OR:
            e = execTextIR_OpOr(condNodeIndex, postingQueue, logicalIdHints);
            if(e < eNOERROR) OOSQL_ERR(e);
            if(e == TEXTIR_EOS) return TEXTIR_EOS;

            break;

        case OP_TEXTIR_MINUS:
            e = execTextIR_OpMinus(condNodeIndex, postingQueue, logicalIdHints);
            if(e < eNOERROR) OOSQL_ERR(e);
            if(e == TEXTIR_EOS) return TEXTIR_EOS;

            break;

        case OP_TEXTIR_ACCUMULATE:
            e = execTextIR_OpAccumulate(condNodeIndex, postingQueue, logicalIdHints);
            if(e < eNOERROR) OOSQL_ERR(e);
            if(e == TEXTIR_EOS) return TEXTIR_EOS;

            break;

        case OP_TEXTIR_MULTIPLY:
            e = execTextIR_OpMultiply(condNodeIndex, postingQueue, logicalIdHints);
            if(e < eNOERROR) OOSQL_ERR(e);
            if(e == TEXTIR_EOS) return TEXTIR_EOS;

            break;

        case OP_TEXTIR_THRESHOLD:
            e = execTextIR_OpThreshold(condNodeIndex, postingQueue, logicalIdHints);
            if(e < eNOERROR) OOSQL_ERR(e);
            if(e == TEXTIR_EOS) return TEXTIR_EOS;

            break;

        case OP_TEXTIR_MAX:
            e = execTextIR_OpMax(condNodeIndex, postingQueue, logicalIdHints);
            if(e < eNOERROR) OOSQL_ERR(e);
            if(e == TEXTIR_EOS) return TEXTIR_EOS;

            break;

        case OP_TEXTIR_NEAR:
            e = execTextIR_OpNear(condNodeIndex, postingQueue, logicalIdHints);
            if(e < eNOERROR) OOSQL_ERR(e);
            if(e == TEXTIR_EOS) return TEXTIR_EOS;

            break;

        case OP_TEXTIR_NEAR_WITH_ORDER:
            e = execTextIR_OpNearWithOrder(condNodeIndex, postingQueue, logicalIdHints);
            if(e < eNOERROR) OOSQL_ERR(e);
            if(e == TEXTIR_EOS) return TEXTIR_EOS;

            break;

        default:    /* invalid operator */
            OOSQL_ERR(eINVALID_CASE_OOSQL);
        }

        break;

    case TEXTINDEXCOND_KEYWORD:
        e = execTextIR_KeywordNode(condNodeIndex, postingQueue, logicalIdHints);
        if(e < eNOERROR) OOSQL_ERR(e);
        if(e == TEXTIR_EOS) return TEXTIR_EOS;

        break;

    case TEXTINDEXCOND_CONSTANT:
        /* ERROR: constant can not be a text index condition */

    default:    /* invalid text index condition node */
        OOSQL_ERR(eINVALID_CASE_OOSQL);
    }

    /* return */
    return eNOERROR;
}

inline Four OOSQL_Evaluator::execTextIR_CmpOp(
    AP_TextIndexCondPoolIndex   operInfo,           // IN    
    OOSQL_TextIR_PostingQueue&  postingQueue,       // INOUT 
    Four                        logicalIdHints      // IN   
)
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
    AP_TextIndexCondElement     *operNode;
    OOSQL_TextIR_Posting        *posting;
    AP_TextIndexCondElement     *constNode;
    OOSQL_DB_Value              constant(pMemoryManager);
    OOSQL_TextIR_PostingQueue&  subPostingQueue = EVAL_TEXTIR_EVALBUFELEMENTS[operInfo.startIndex].postingQueue;
    Four                        e;

    /* check input parameters */
#if OOSQL_DEBUG
    if(IS_NULL_POOLINDEX(operInfo) == SM_TRUE)
        OOSQL_ERR(eBADPARAMETER_OOSQL);
#endif

    /* get Text IR condition node for the current node */
    operNode = ACCESSPLAN.getTextIndexCondElem(operInfo);

    /* evaluate two operands */
    /*
     * NOTE: The first operand must be Text IR query formaul and
     *       the second operand must be an integer constant.
     */
    /* get second operand */
    /* get Text IR condition nodes for the constant */
    constNode = ACCESSPLAN.getTextIndexCondElem(operNode->oper.op2);

    e = ACCESSPLAN.getValue(constNode->constant, &constant);
    if(e < eNOERROR) OOSQL_ERR(e);
    if(constant.type == OOSQL_TYPE_LONG)
    {
        constant.type = OOSQL_TYPE_DOUBLE;
        constant.data.d = constant.data.l;
    }
    else if(constant.type == OOSQL_TYPE_LONG_LONG)
    {
        constant.type = OOSQL_TYPE_DOUBLE;
        constant.data.d = constant.data.ll;
    }

    /* get first operand and evaluate it */
    if(operNode->oper.operatorID == OP_GT && constant.data.d == 0.0)
    {
        AP_TextIndexCondPoolIndex condNodeIndex = operNode->oper.op1;
        e = execTextIR_SubPlan(condNodeIndex, postingQueue, logicalIdHints);
        if(e < eNOERROR) OOSQL_ERR(e);
        if(e == TEXTIR_EOS) return TEXTIR_EOS;
    }
    else
    {
        AP_TextIndexCondPoolIndex condNodeIndex = operNode->oper.op1;
        e = execTextIR_SubPlan(condNodeIndex, subPostingQueue, logicalIdHints);
        if(e < eNOERROR) OOSQL_ERR(e);
        if(e == TEXTIR_EOS) return TEXTIR_EOS;

        while(true)
        {
            posting = &subPostingQueue.Head();
            if(subPostingQueue.PopHead() == false)
                break;

            /* do comparison according to the comparison operator */
            switch(operNode->oper.operatorID) 
            {
            case OP_EQ:
#if defined (EXTENDED_BOOLEAN_MODEL)
                if(posting->weight == constant.data.d)
#elif defined (HEURISTIC_MODEL)
                if((posting->weight + posting->bonus) == constant.data.d) 
#endif
                {
                    postingQueue.PushTail(*posting);
                }
                break;

            case OP_NE:
#if defined (EXTENDED_BOOLEAN_MODEL)
                if(posting->weight != constant.data.d)
#elif defined (HEURISTIC_MODEL)
                if((posting->weight + posting->bonus) != constant.data.d) 
#endif
                {
                    postingQueue.PushTail(*posting);
                }
                break;

            case OP_GT:
#if defined (EXTENDED_BOOLEAN_MODEL)
                if(posting->weight > constant.data.d)
#elif defined (HEURISTIC_MODEL)
                if((posting->weight + posting->bonus) > constant.data.d) 
#endif
                {
                    postingQueue.PushTail(*posting);
                }
                break;

            case OP_GE:
#if defined (EXTENDED_BOOLEAN_MODEL)
                if(posting->weight >= constant.data.d)
#elif defined (HEURISTIC_MODEL)
                if((posting->weight + posting->bonus) >= constant.data.d) 
#endif
                {
                    postingQueue.PushTail(*posting);
                }
                break;

            case OP_LT:
#if defined (EXTENDED_BOOLEAN_MODEL)
                if(posting->weight < constant.data.d)
#elif defined (HEURISTIC_MODEL)
                if((posting->weight + posting->bonus) < constant.data.d) 
#endif
                {
                    postingQueue.PushTail(*posting);
                }
                break;

            case OP_LE:
#if defined (EXTENDED_BOOLEAN_MODEL)
                if(posting->weight <= constant.data.d)
#elif defined (HEURISTIC_MODEL)
                if((posting->weight + posting->bonus) <= constant.data.d) 
#endif
                {
                    postingQueue.PushTail(*posting);
                }
                break;

            default:    /* ERROR: invalid operator */
                OOSQL_ERR(eINVALID_CASE_OOSQL);
            }
        }
    }

    /* return */
    return eNOERROR;
}

inline Four OOSQL_Evaluator::execTextIR_OpAnd(
    AP_TextIndexCondPoolIndex   operInfo,           // IN    
    OOSQL_TextIR_PostingQueue&  postingQueue,       // INOUT 
    Four                        logicalIdHints      // IN    
)
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
    AP_TextIndexCondElement     *operNode;
    OOSQL_TextIR_Posting        *currPosting;
    OOSQL_TextIR_Posting        *op1Posting;
    OOSQL_TextIR_Posting        *op2Posting;
    OOSQL_TextIR_Weight         weight;
    OOSQL_TextIR_Bonus          bonus;
    Four                        e;
    AP_TextIndexCondPoolIndex   condNodeIndex;
    AP_TextIndexCondElement*    indexCondNode;
    OOSQL_TextIR_PostingQueue&  op1PostingQueue = EVAL_TEXTIR_EVALBUFELEMENTS[operInfo.startIndex].op1PostingQueue;
    OOSQL_TextIR_PostingQueue&  op2PostingQueue = EVAL_TEXTIR_EVALBUFELEMENTS[operInfo.startIndex].op2PostingQueue;

    /* check input parameters */
#ifdef  OOSQL_DEBUG
    if(IS_NULL_POOLINDEX(operInfo) == SM_TRUE)
        OOSQL_ERR(eBADPARAMETER_OOSQL);
#endif

    /* get Text IR condition node for the current node */
    operNode = ACCESSPLAN.getTextIndexCondElem(operInfo);
#ifdef  OOSQL_DEBUG
    if(operNode == NULL) OOSQL_ERR(eNULL_POINTER_OOSQL);
#endif

    /* set compare mode */
    /* if backward scan is used, get 1's complement */
    if (operNode->oper.scanDirection == TEXTIR_SCAN_FORWARD) m_compareMode = 0;
    else m_compareMode = ULONG_MAX;

    /* determine logicalIdHints to skip more postings */
    if (operNode->oper.logicalIdHints != TEXTIR_POSTINGSKIP_NO_HINTS && logicalIdHints != TEXTIR_POSTINGSKIP_NO_HINTS)
        logicalIdHints = TEXTIR_DOCID_MAX(operNode->oper.logicalIdHints,logicalIdHints,m_compareMode);
    else if (operNode->oper.logicalIdHints != TEXTIR_POSTINGSKIP_NO_HINTS)
        logicalIdHints = operNode->oper.logicalIdHints;

    /* read the first operand(posting) */
    while(op1PostingQueue.IsEmpty())
    {
        condNodeIndex = operNode->oper.op1;
        indexCondNode = ACCESSPLAN.getTextIndexCondElem(condNodeIndex);
        e = execTextIR_SubPlan(condNodeIndex, op1PostingQueue, logicalIdHints);
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

    /* read the second operand posting without marking */
    while(op2PostingQueue.IsEmpty())
    {
        condNodeIndex = operNode->oper.op2;
        indexCondNode = ACCESSPLAN.getTextIndexCondElem(condNodeIndex);
        e = execTextIR_SubPlan(condNodeIndex, op2PostingQueue, logicalIdHints);
        if(e < eNOERROR) OOSQL_ERR(e);
        if(e == TEXTIR_EOS) 
        {
            if(postingQueue.IsEmpty())
                return TEXTIR_EOS;
            else
                return eNOERROR;
        }
    }

    while(1)
    {
        if(op1PostingQueue.IsEmpty())
            break;
        if(op2PostingQueue.IsEmpty())
            break;

        /* read the first operand */
        op1Posting = &op1PostingQueue.Head();
        op1PostingQueue.PopHead();

        /* read the second operand */
        op2Posting = &op2PostingQueue.Head();
        op2PostingQueue.PopHead();

        /* check if the logical doc. IDs of the two postings are equal */
        while(TEXTIR_DOCID_NE(op1Posting->logicalDocId,op2Posting->logicalDocId,m_compareMode))
        {
            if(TEXTIR_DOCID_LT(op1Posting->logicalDocId,op2Posting->logicalDocId,m_compareMode))
            {
                if(TEXTIR_DOCID_DIFF(op2Posting->logicalDocId,op1Posting->logicalDocId,m_compareMode) > TEXTIR_POSTINGSKIP_THRESHOLD)
                    operNode->oper.logicalIdHints = op2Posting->logicalDocId;
                else
                    operNode->oper.logicalIdHints = TEXTIR_POSTINGSKIP_NO_HINTS;

                /* skip the first operand */
                /* read the first operand(posting) without marking */
                op1Posting = &op1PostingQueue.Head();
                if(op1PostingQueue.PopHead() == false)
                {
                    op2PostingQueue.PushHead();
                    return eNOERROR;
                }
            }
            else 
            {
                if(TEXTIR_DOCID_DIFF(op1Posting->logicalDocId,op2Posting->logicalDocId,m_compareMode) > TEXTIR_POSTINGSKIP_THRESHOLD)
                    operNode->oper.logicalIdHints = op1Posting->logicalDocId;
                else
                    operNode->oper.logicalIdHints = TEXTIR_POSTINGSKIP_NO_HINTS;

                /* skip the second operand */
                /* read the first operand(posting) without marking */
                op2Posting = &op2PostingQueue.Head();
                if(op2PostingQueue.PopHead() == false)
                {
                    op1PostingQueue.PushHead();
                    return eNOERROR;
                }
            }
        }

#if defined (EXTENDED_BOOLEAN_MODEL)
        /* calculate the minimum weight of the two postings */
        if(op1Posting->weight < op2Posting->weight) 
            weight = op1Posting->weight;
        else 
            weight = op2Posting->weight;
#elif defined (HEURISTIC_MODEL)
        /* calculate the sum of two postings' weight */
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
    }


    /* return */
    return eNOERROR;
}

inline Four OOSQL_Evaluator::execTextIR_OpOr(
    AP_TextIndexCondPoolIndex   operInfo,           // IN    
    OOSQL_TextIR_PostingQueue&  postingQueue,       // INOUT 
    Four                        logicalIdHints      // IN   
)
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
    AP_TextIndexCondElement     *operNode;
    OOSQL_TextIR_Posting        *currPosting;
    OOSQL_TextIR_Posting        *op1Posting;
    OOSQL_TextIR_Posting        *op2Posting;
    OOSQL_TextIR_Weight         weight;
    OOSQL_TextIR_Bonus          bonus;
    Four                        e;
    AP_TextIndexCondPoolIndex   condNodeIndex;
    AP_TextIndexCondElement*    indexCondNode;
    OOSQL_TextIR_PostingQueue&  op1PostingQueue = EVAL_TEXTIR_EVALBUFELEMENTS[operInfo.startIndex].op1PostingQueue;
    OOSQL_TextIR_PostingQueue&  op2PostingQueue = EVAL_TEXTIR_EVALBUFELEMENTS[operInfo.startIndex].op2PostingQueue;

    /* check input parameters */
#ifdef  OOSQL_DEBUG
    if(IS_NULL_POOLINDEX(operInfo) == SM_TRUE) OOSQL_ERR(eBADPARAMETER_OOSQL);
#endif

    /* get Text IR condition node for the current node */
    operNode = ACCESSPLAN.getTextIndexCondElem(operInfo);
#ifdef  OOSQL_DEBUG
    if(operNode == NULL) OOSQL_ERR(eNULL_POINTER_OOSQL);
#endif

    /* set compare mode */
    /* if backward scan is used, get 1's complement */
    if (operNode->oper.scanDirection == TEXTIR_SCAN_FORWARD) m_compareMode = 0;
    else m_compareMode = ULONG_MAX;

    /* read the first operand */
    while(op1PostingQueue.IsEmpty())
    {
        condNodeIndex = operNode->oper.op1;
        indexCondNode = ACCESSPLAN.getTextIndexCondElem(condNodeIndex);
        e = execTextIR_SubPlan(condNodeIndex, op1PostingQueue, logicalIdHints);
        if(e < eNOERROR) OOSQL_ERR(e);
        if(e == TEXTIR_EOS) break;
    }

    // read the second operand 
    while(op2PostingQueue.IsEmpty())
    {
        condNodeIndex = operNode->oper.op2;
        indexCondNode = ACCESSPLAN.getTextIndexCondElem(condNodeIndex);
        e = execTextIR_SubPlan(condNodeIndex, op2PostingQueue, logicalIdHints);
        if(e < eNOERROR) OOSQL_ERR(e);  
        if(e == TEXTIR_EOS) break;
    }

    if(op1PostingQueue.IsEmpty() && op2PostingQueue.IsEmpty())
        return TEXTIR_EOS;
    else if(op1PostingQueue.IsEmpty()) 
    {
        while(!op2PostingQueue.IsEmpty())
        {
            /* get next posting */
            op2Posting = &op2PostingQueue.Head();
            op2PostingQueue.PopHead();

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
        while(!op1PostingQueue.IsEmpty())
        {
            /* get next posting */
            op1Posting = &op1PostingQueue.Head();
            op1PostingQueue.PopHead();  

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

    while(1)
    {
        // read the first operand 
        op1Posting = &op1PostingQueue.Head();
        if(op1PostingQueue.IsEmpty())
            TEXTIR_SET_NULLPOSTING(op1Posting);

        // read the second operand 
        op2Posting = &op2PostingQueue.Head();
        if(op2PostingQueue.IsEmpty())
            TEXTIR_SET_NULLPOSTING(op2Posting);

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
            op2PostingQueue.PopHead();

#if defined (EXTENDED_BOOLEAN_MODEL)
            /* calculate the maximum weight of the two postings */
            if(op1Posting->weight < op2Posting->weight)
                weight = op2Posting->weight;
            else
                weight = op1Posting->weight;
#elif defined (HEURISTIC_MODEL)
            /* calculate the sum of two postings' weight */
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
        {   /* op1Posting->logicalDocId > op2Posting->logicalDocId */
            /* add to result postingQueue */
            postingQueue.PushTail();
            currPosting = &postingQueue.Tail();

            op2PostingQueue.PopHead();

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
    }

    /* return */
    return eNOERROR;
}

inline Four OOSQL_Evaluator::execTextIR_OpMinus(
    AP_TextIndexCondPoolIndex   operInfo,           // IN    
    OOSQL_TextIR_PostingQueue&  postingQueue,       // INOUT 
    Four                        logicalIdHints      // IN    
)
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
    AP_TextIndexCondElement     *operNode;
    OOSQL_TextIR_Posting        *currPosting;
    OOSQL_TextIR_Posting        *op1Posting;
    OOSQL_TextIR_Posting        *op2Posting;
    OOSQL_TextIR_Weight         weight;
    OOSQL_TextIR_Bonus          bonus;
    Four                        e;
    AP_TextIndexCondPoolIndex   condNodeIndex;
    AP_TextIndexCondElement*    indexCondNode;
    OOSQL_TextIR_PostingQueue&  op1PostingQueue = EVAL_TEXTIR_EVALBUFELEMENTS[operInfo.startIndex].op1PostingQueue;
    OOSQL_TextIR_PostingQueue&  op2PostingQueue = EVAL_TEXTIR_EVALBUFELEMENTS[operInfo.startIndex].op2PostingQueue;

    /* check input parameters */
#ifdef  OOSQL_DEBUG
    if(IS_NULL_POOLINDEX(operInfo) == SM_TRUE)
        OOSQL_ERR(eBADPARAMETER_OOSQL);
#endif

    /* get Text IR condition node for the current node */
    operNode = ACCESSPLAN.getTextIndexCondElem(operInfo);
#ifdef  OOSQL_DEBUG
    if(operNode == NULL) OOSQL_ERR(eNULL_POINTER_OOSQL);
#endif

    /* set compare mode */
    /* if backward scan is used, get 1's complement */
    if (operNode->oper.scanDirection == TEXTIR_SCAN_FORWARD) m_compareMode = 0;
    else m_compareMode = ULONG_MAX;

    /* read the first operand(posting) */
    while(op1PostingQueue.IsEmpty())
    {
        condNodeIndex = operNode->oper.op1;
        indexCondNode = ACCESSPLAN.getTextIndexCondElem(condNodeIndex);
        e = execTextIR_SubPlan(condNodeIndex, op1PostingQueue, logicalIdHints);
        if(e < eNOERROR) OOSQL_ERR(e);
        if(e == TEXTIR_EOS)
        {
            if(postingQueue.IsEmpty())
                return TEXTIR_EOS;
            else
                return eNOERROR;
        }
    }

    while(1)
    {
        if(op1PostingQueue.IsEmpty())
            break;

        /* read the first operand(posting) */
        op1Posting = &op1PostingQueue.Head();
        op1PostingQueue.PopHead();

        /* 
         * read the second operand until the logical doc. ID of op2 is greater than 
         * or equal to that of op1.
         *
         * NOTE: If there is a preserved posting, read it.
         */
        do {
            while(op2PostingQueue.IsEmpty())
            {
                condNodeIndex = operNode->oper.op2;
                indexCondNode = ACCESSPLAN.getTextIndexCondElem(condNodeIndex);
                e = execTextIR_SubPlan(condNodeIndex, op2PostingQueue, logicalIdHints);
                if(e < eNOERROR) OOSQL_ERR(e);
                if(e == TEXTIR_EOS) break;
            }
            op2Posting = &op2PostingQueue.Head();
            if(op2PostingQueue.PopHead() == false)  
            {
                TEXTIR_SET_NULLPOSTING(op2Posting);
                break;      /* break out do-while loop */
            }
        } while(TEXTIR_DOCID_GT(op1Posting->logicalDocId,op2Posting->logicalDocId,m_compareMode));

        /* in case op2 is null, make 'result' as op1 */
        if(TEXTIR_IS_NULLPOSTING(op2Posting) == SM_TRUE) 
        {
            while(1)
            {
                postingQueue.PushTail();
                currPosting = &postingQueue.Tail();

#if defined (EXTENDED_BOOLEAN_MODEL)
                weight = op1Posting->weight;
                e = execTextIR_setPosting(currPosting, op1Posting, weight);
#elif defined (HEURISTIC_MODEL)
                weight = op1Posting->weight * HEURISTIC_MINUS_WEIGHT_MULTIPLY_VALUE;
                bonus  = op1Posting->bonus + op2Posting->bonus;
                e = execTextIR_setPosting(currPosting, op1Posting, weight, bonus);
#endif
                if(e < eNOERROR) OOSQL_ERR(e);

                op1Posting = &op1PostingQueue.Head();
                if(op1PostingQueue.PopHead() == false)
                    break;
            }

            return eNOERROR;
        }
        /* 
         * do difference of the weights of the two postings 
         */
        else if(TEXTIR_DOCID_EQ(op1Posting->logicalDocId,op2Posting->logicalDocId,m_compareMode)) 
        {
#if defined (EXTENDED_BOOLEAN_MODEL)
            /* calculate the difference of the weights of the two postings 
             * NOTE: If the result is less than 0, make it 0 
             *       which means the current document don't have the keyword. 
             */
            weight = op1Posting->weight - op2Posting->weight;
            if(weight < 0) weight = 0;

            postingQueue.PushTail();
            currPosting = &postingQueue.Tail();

            /* set result posting */
            e = execTextIR_setPosting(currPosting, op1Posting, weight);
            if(e < eNOERROR) OOSQL_ERR(e);
#endif
        }
        else if(TEXTIR_DOCID_LT(op1Posting->logicalDocId,op2Posting->logicalDocId,m_compareMode))
        {
            op2PostingQueue.PushHead();

            postingQueue.PushTail();
            currPosting = &postingQueue.Tail();

            /* set result posting */
#if defined (EXTENDED_BOOLEAN_MODEL)
            weight = op1Posting->weight;
            e = execTextIR_setPosting(currPosting, op1Posting, weight);
#elif defined (HEURISTIC_MODEL)
            weight = op1Posting->weight * HEURISTIC_MINUS_WEIGHT_MULTIPLY_VALUE;
            bonus  = op1Posting->bonus + op2Posting->bonus;
            e = execTextIR_setPosting(currPosting, op1Posting, weight, bonus);
#endif
            if(e < eNOERROR) OOSQL_ERR(e);
        }
    }

    /* return */
    return eNOERROR;
}


inline Four OOSQL_Evaluator::execTextIR_OpAccumulate(
    AP_TextIndexCondPoolIndex   operInfo,           // IN    
    OOSQL_TextIR_PostingQueue&  postingQueue,       // INOUT 
    Four                        logicalIdHints      // IN   
)
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
    AP_TextIndexCondElement     *operNode;
    OOSQL_TextIR_Posting        *currPosting;
    OOSQL_TextIR_Posting        *op1Posting;
    OOSQL_TextIR_Posting        *op2Posting;
    OOSQL_TextIR_Weight         addedWeight;
    OOSQL_TextIR_Bonus          addedBonus;
    Four                        e;
    AP_TextIndexCondPoolIndex   condNodeIndex;
    AP_TextIndexCondElement*    indexCondNode;
    OOSQL_TextIR_PostingQueue&  op1PostingQueue = EVAL_TEXTIR_EVALBUFELEMENTS[operInfo.startIndex].op1PostingQueue;
    OOSQL_TextIR_PostingQueue&  op2PostingQueue = EVAL_TEXTIR_EVALBUFELEMENTS[operInfo.startIndex].op2PostingQueue;

    /* check input parameters */
#ifdef  OOSQL_DEBUG
    if(IS_NULL_POOLINDEX(operInfo) == SM_TRUE)
        OOSQL_ERR(eBADPARAMETER_OOSQL);
#endif

    /* get Text IR condition node for the current node */
    operNode = ACCESSPLAN.getTextIndexCondElem(operInfo);
#ifdef  OOSQL_DEBUG
    if(operNode == NULL) OOSQL_ERR(eNULL_POINTER_OOSQL);
#endif

    /* set compare mode */
    /* if backward scan is used, get 1's complement */
    if (operNode->oper.scanDirection == TEXTIR_SCAN_FORWARD) m_compareMode = 0;
    else m_compareMode = ULONG_MAX;

    /* read the first operand */
    while(op1PostingQueue.IsEmpty())
    {
        condNodeIndex = operNode->oper.op1;
        indexCondNode = ACCESSPLAN.getTextIndexCondElem(condNodeIndex);
        e = execTextIR_SubPlan(condNodeIndex, op1PostingQueue, logicalIdHints);
        if(e < eNOERROR) OOSQL_ERR(e);
        if(e == TEXTIR_EOS) break;
    }

    /* read the second operand */
    while(op2PostingQueue.IsEmpty())
    {
        condNodeIndex = operNode->oper.op2;
        indexCondNode = ACCESSPLAN.getTextIndexCondElem(condNodeIndex);
        e = execTextIR_SubPlan(condNodeIndex, op2PostingQueue, logicalIdHints);
        if(e < eNOERROR) OOSQL_ERR(e);  
        if(e == TEXTIR_EOS) break;
    }

    if(op1PostingQueue.IsEmpty() && op2PostingQueue.IsEmpty())
        return TEXTIR_EOS;
    else if(op1PostingQueue.IsEmpty()) 
    {
        while(!op2PostingQueue.IsEmpty())
        {
            /* get next posting */
            op2Posting = &op2PostingQueue.Head();
            op2PostingQueue.PopHead();

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
        while(!op1PostingQueue.IsEmpty())
        {
            /* get next posting */
            op1Posting = &op1PostingQueue.Head();
            op1PostingQueue.PopHead();  

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

    while(1)
    {
        // read the first operand 
        op1Posting = &op1PostingQueue.Head();
        if(op1PostingQueue.IsEmpty())
            TEXTIR_SET_NULLPOSTING(op1Posting);

        // read the second operand 
        op2Posting = &op2PostingQueue.Head();
        if(op2PostingQueue.IsEmpty())
            TEXTIR_SET_NULLPOSTING(op2Posting);

        /* check if end of scan */
        if(TEXTIR_IS_NULLPOSTING(op1Posting) || TEXTIR_IS_NULLPOSTING(op2Posting))
            return eNOERROR;

        /* 
         * do union of the two postings 
         */
        if(TEXTIR_DOCID_EQ(op1Posting->logicalDocId,op2Posting->logicalDocId,m_compareMode))
        {
            /* add to result postingQueue */
            postingQueue.PushTail();
            currPosting = &postingQueue.Tail();

            op1PostingQueue.PopHead();
            op2PostingQueue.PopHead();

            /* calculate the accumulated weight of the two postings */
            addedWeight = op1Posting->weight + op2Posting->weight;

            /* set result posting */
#if defined (EXTENDED_BOOLEAN_MODEL)
            e = execTextIR_setPosting(currPosting, op1Posting, addedWeight);
#elif defined(HEURISTIC_MODEL)
            addedBonus  = op1Posting->bonus + op2Posting->bonus;
            e = execTextIR_setPosting(currPosting, op1Posting, addedWeight, addedBonus);
#endif
            if(e < eNOERROR) OOSQL_ERR(e);
        }
        else if(TEXTIR_DOCID_LT(op1Posting->logicalDocId,op2Posting->logicalDocId,m_compareMode))
        {
            /* add to result postingQueue */
            postingQueue.PushTail();
            currPosting = &postingQueue.Tail();

            op1PostingQueue.PopHead();

            /* set result posting */
            addedWeight = op1Posting->weight;
#if defined (EXTENDED_BOOLEAN_MODEL)
            e = execTextIR_setPosting(currPosting, op1Posting, addedWeight);
#elif defined (HEURISTIC_MODEL)
            addedBonus = op1Posting->bonus;
            e = execTextIR_setPosting(currPosting, op1Posting, addedWeight, addedBonus);
#endif
            if(e < eNOERROR) OOSQL_ERR(e);
        }
        else 
        {   /* op1Posting->logicalDocId > op2Posting->logicalDocId */
            /* unmark op2 if it was marked before */
            postingQueue.PushTail();
            currPosting = &postingQueue.Tail();

            op2PostingQueue.PopHead();

            /* set result posting */
            addedWeight = op2Posting->weight;
#if defined (EXTENDED_BOOLEAN_MODEL)
            e = execTextIR_setPosting(currPosting, op2Posting, addedWeight);
#elif defined (HEURISTIC_MODEL)
            addedBonus = op2Posting->bonus;
            e = execTextIR_setPosting(currPosting, op2Posting, addedWeight, addedBonus);
#endif
            if(e < eNOERROR) OOSQL_ERR(e);
        }
    }
    
    /* return */
    return eNOERROR;
}

inline Four OOSQL_Evaluator::execTextIR_OpMultiply(
    AP_TextIndexCondPoolIndex   operInfo,           // IN    
    OOSQL_TextIR_PostingQueue&  postingQueue,       // INOUT 
    Four                        logicalIdHints      // IN    
)
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
    AP_TextIndexCondElement     *operNode;
    AP_TextIndexCondElement     *constNode;
    OOSQL_DB_Value              op2Const(pMemoryManager);
    Four                        e;
    AP_TextIndexCondPoolIndex   condNodeIndex;
    AP_TextIndexCondElement*    indexCondNode;
    Four                        i;

    /* check input parameters */
#ifdef  OOSQL_DEBUG
    if(IS_NULL_POOLINDEX(operInfo) == SM_TRUE)
        OOSQL_ERR(eBADPARAMETER_OOSQL);
#endif

    /* get Text IR condition node for the current node */
    operNode = ACCESSPLAN.getTextIndexCondElem(operInfo);
#ifdef  OOSQL_DEBUG
    if(operNode == NULL) OOSQL_ERR(eNULL_POINTER_OOSQL);
#endif

    /* read the first operand */
    condNodeIndex = operNode->oper.op1;
    indexCondNode = ACCESSPLAN.getTextIndexCondElem(condNodeIndex);
    e = execTextIR_SubPlan(condNodeIndex, postingQueue, logicalIdHints);
    if(e < eNOERROR) OOSQL_ERR(e);
    if(e == TEXTIR_EOS) return TEXTIR_EOS;

    /* get Text IR condition nodes for the constant */
    constNode = ACCESSPLAN.getTextIndexCondElem(operNode->oper.op2);

    /* read the second operand(which is float constant) */
    e = ACCESSPLAN.getValue(constNode->constant, &op2Const);
    if(e < eNOERROR) OOSQL_ERR(e);

    /* calculate the result weight */
    if(op2Const.type == OOSQL_TYPE_long)
        for(i = 0; i < postingQueue.size(); i++)
            postingQueue[i].weight = postingQueue[i].weight * op2Const.data.l;
    else if(op2Const.type == OOSQL_TYPE_long_long)
        for(i = 0; i < postingQueue.size(); i++)
            postingQueue[i].weight = postingQueue[i].weight * op2Const.data.ll;
    else
        for(i = 0; i < postingQueue.size(); i++)
            postingQueue[i].weight = postingQueue[i].weight * op2Const.data.d;

    /* return */
    return eNOERROR;
}

inline Four OOSQL_Evaluator::execTextIR_OpThreshold(
    AP_TextIndexCondPoolIndex   operInfo,           // IN    
    OOSQL_TextIR_PostingQueue&  postingQueue,       // INOUT
    Four                        logicalIdHints      // IN    
)
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
    AP_TextIndexCondElement     *operNode;
    OOSQL_TextIR_Posting        *posting;
    AP_TextIndexCondElement     *constNode;
    OOSQL_DB_Value              op2Const(pMemoryManager);
    Four                        e;
    AP_TextIndexCondPoolIndex   condNodeIndex;
    AP_TextIndexCondElement*    indexCondNode;
    OOSQL_TextIR_PostingQueue&  subPostingQueue = EVAL_TEXTIR_EVALBUFELEMENTS[operInfo.startIndex].postingQueue;

    /* check input parameters */
#ifdef  OOSQL_DEBUG
    if(IS_NULL_POOLINDEX(operInfo) == SM_TRUE)
        OOSQL_ERR(eBADPARAMETER_OOSQL);
#endif

    /* get Text IR condition node for the current node */
    operNode = ACCESSPLAN.getTextIndexCondElem(operInfo);
#ifdef  OOSQL_DEBUG
    if(operNode == NULL) OOSQL_ERR(eNULL_POINTER_OOSQL);
#endif

    /* get Text IR condition nodes for the constant */
    constNode = ACCESSPLAN.getTextIndexCondElem(operNode->oper.op2);

    /* read the second operand(which is integer constant) */
    e = ACCESSPLAN.getValue(constNode->constant, &op2Const);
    if(e < eNOERROR) OOSQL_ERR(e);

    /* read the first operand */
    condNodeIndex = operNode->oper.op1;
    indexCondNode = ACCESSPLAN.getTextIndexCondElem(condNodeIndex);
    e = execTextIR_SubPlan(condNodeIndex, subPostingQueue, logicalIdHints);
    if(e < eNOERROR) OOSQL_ERR(e);
    if(e == TEXTIR_EOS) return TEXTIR_EOS;

    while(true)
    {
        posting = &subPostingQueue.Head();
        if(subPostingQueue.PopHead() == false)
            break;

        if((op2Const.type == OOSQL_TYPE_LONG   && posting->weight <= op2Const.data.l) ||
           (op2Const.type == OOSQL_TYPE_DOUBLE && posting->weight <= op2Const.data.d))
        {
            postingQueue.PushTail(*posting);
        }
        else if((op2Const.type == OOSQL_TYPE_LONG_LONG   && posting->weight <= op2Const.data.ll) ||
           (op2Const.type == OOSQL_TYPE_DOUBLE && posting->weight <= op2Const.data.d))
        {
            postingQueue.PushTail(*posting);
        }
    }

    /* return */
    return eNOERROR;
}

inline Four OOSQL_Evaluator::execTextIR_OpMax(
    AP_TextIndexCondPoolIndex   operInfo,           // IN    
    OOSQL_TextIR_PostingQueue&  postingQueue,       // INOUT 
    Four                        logicalIdHints      // IN   
)
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
    OOSQL_TextIR_Posting    *currPosting;
    Four                    e;
    Four                    nElementsHasBeenRead;

    /* check input parameters */
#ifdef  OOSQL_DEBUG
    if(IS_NULL_POOLINDEX(operInfo) == SM_TRUE)
        OOSQL_ERR(eBADPARAMETER_OOSQL);
#endif

    /* get pointer to the posting of the current node */
    currPosting = &(EVAL_TEXTIR_EVALBUFELEMENTS[operInfo.startIndex].posting);

    /* if it is the first time to process 'max' operator
     * we construct Min-Max Heap by processing the subplan of the left operand
     */
    if(EVAL_TEXTIR_EVALBUFELEMENTS[operInfo.startIndex].doneFlag == SM_FALSE) 
    {
        /* construct Min-Max Heap */
        e = execTextIR_ConstructMinMaxHeap(operInfo, logicalIdHints);
        if(e < eNOERROR) OOSQL_ERR(e);

        /* set 'doneFlag' as SM_TRUE */
        EVAL_TEXTIR_EVALBUFELEMENTS[operInfo.startIndex].doneFlag = SM_TRUE;
    }

    nElementsHasBeenRead = 0;
    while(1)
    {
        /* retrieve the maximum entry from the Min-Max Heap */
        e = execTextIR_NextMinMaxHeapEntry(operInfo, &currPosting);
        if(e < eNOERROR)
            OOSQL_ERR(e);
        if(e == TEXTIR_EOS) 
        {
            if(postingQueue.IsEmpty())
                return TEXTIR_EOS;
            else
                return eNOERROR;
        }

        /* add to result postingQueue */
        postingQueue.PushTail(*currPosting);
        nElementsHasBeenRead ++;
        if(nElementsHasBeenRead > TEXTIR_DEFAULTPOSTINGQUEUESIZE)
            return eNOERROR;
    }

    /* return */
    return eNOERROR;
}

inline Four OOSQL_Evaluator::execTextIR_OpNear(
    AP_TextIndexCondPoolIndex   operInfo,           // IN    
    OOSQL_TextIR_PostingQueue&  postingQueue,       // INOUT 
    Four                        logicalIdHints      // IN    
)
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
    AP_TextIndexCondElement     *operNode;
    OOSQL_TextIR_Posting        *currPosting;
    OOSQL_TextIR_Posting        *op1Posting;
    OOSQL_TextIR_Posting        *op2Posting;
    OOSQL_TextIR_Weight         weight;
    OOSQL_TextIR_Bonus          bonus;
    Four                        e;
    AP_TextIndexCondPoolIndex   condNodeIndex;
    AP_TextIndexCondElement*    indexCondNode;
    Four                        distance;
    OOSQL_TextIR_PostingQueue&  op1PostingQueue = EVAL_TEXTIR_EVALBUFELEMENTS[operInfo.startIndex].op1PostingQueue;
    OOSQL_TextIR_PostingQueue&  op2PostingQueue = EVAL_TEXTIR_EVALBUFELEMENTS[operInfo.startIndex].op2PostingQueue;

    /* check input parameters */
#ifdef  OOSQL_DEBUG
    if(IS_NULL_POOLINDEX(operInfo) == SM_TRUE)
        OOSQL_ERR(eBADPARAMETER_OOSQL);
#endif

    /* get Text IR condition node for the current node */
    operNode = ACCESSPLAN.getTextIndexCondElem(operInfo);

#ifdef  OOSQL_DEBUG
    if(operNode == NULL) OOSQL_ERR(eNULL_POINTER_OOSQL);
#endif

    condNodeIndex = operNode->oper.op3;
    indexCondNode = ACCESSPLAN.getTextIndexCondElem(condNodeIndex);
    distance = indexCondNode->constant.getElements(m_pool->valuePool, 0).integer.getElements(m_pool->intPool, 0);

    /* set compare mode */
    /* if backward scan is used, get 1's complement */
    if (operNode->oper.scanDirection == TEXTIR_SCAN_FORWARD) m_compareMode = 0;
    else m_compareMode = ULONG_MAX;

    /* determine logicalIdHints to skip more postings */
    if (operNode->oper.logicalIdHints != TEXTIR_POSTINGSKIP_NO_HINTS && logicalIdHints != TEXTIR_POSTINGSKIP_NO_HINTS)
        logicalIdHints = TEXTIR_DOCID_MAX(operNode->oper.logicalIdHints,logicalIdHints,m_compareMode);
    else if (operNode->oper.logicalIdHints != TEXTIR_POSTINGSKIP_NO_HINTS)
        logicalIdHints = operNode->oper.logicalIdHints;

    // read the first operand with position information 
    while(op1PostingQueue.IsEmpty())
    {
        condNodeIndex = operNode->oper.op1;
        indexCondNode = ACCESSPLAN.getTextIndexCondElem(condNodeIndex);
        e = execTextIR_SubPlan(condNodeIndex, op1PostingQueue, logicalIdHints);
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

    // read the second operand with position information
    while(op2PostingQueue.IsEmpty())
    {
        condNodeIndex = operNode->oper.op2;
        indexCondNode = ACCESSPLAN.getTextIndexCondElem(condNodeIndex);
        e = execTextIR_SubPlan(condNodeIndex, op2PostingQueue, logicalIdHints);
        if(e < eNOERROR) OOSQL_ERR(e);
        if(e == TEXTIR_EOS) 
        {
            if(postingQueue.IsEmpty())
                return TEXTIR_EOS;
            else
                return eNOERROR;
        }
    }

    while(1)
    {
        if(op1PostingQueue.IsEmpty())
            break;
        if(op2PostingQueue.IsEmpty())
            break;

        // read the first operand with position information 
        // NOTE: The first operand must be a keyword node and
        //       the fetched posting contains position information.
        op1Posting = &op1PostingQueue.Head();
        op1PostingQueue.PopHead();

        // read the second operand with position information
        // NOTE: The second operand must be a keyword node and
        //       the fetched posting contains position information.
        op2Posting = &op2PostingQueue.Head();
        op2PostingQueue.PopHead();

        /* check if the logical doc. IDs of the two postings are equal */
        while((TEXTIR_DOCID_NE(op1Posting->logicalDocId,op2Posting->logicalDocId,m_compareMode)) || 
              (TEXTIR_DOCID_EQ(op1Posting->logicalDocId,op2Posting->logicalDocId,m_compareMode) && 
               !execTextIR_OpNear_IsNear(op1Posting, op2Posting, distance)))
        {
            if(TEXTIR_DOCID_LT(op1Posting->logicalDocId,op2Posting->logicalDocId,m_compareMode)) 
            {
                if(TEXTIR_DOCID_DIFF(op2Posting->logicalDocId,op1Posting->logicalDocId,m_compareMode) > TEXTIR_POSTINGSKIP_THRESHOLD)
                    operNode->oper.logicalIdHints = op2Posting->logicalDocId;
                else
                    operNode->oper.logicalIdHints = TEXTIR_POSTINGSKIP_NO_HINTS;

                /* skip the first operand */
                /* read the first operand(posting) without marking */
                op1Posting = &op1PostingQueue.Head();
                if(op1PostingQueue.PopHead() == false)
                {
                    op2PostingQueue.PushHead();
                    return eNOERROR;
                }
            }
            else 
            {
                if(TEXTIR_DOCID_DIFF(op1Posting->logicalDocId,op2Posting->logicalDocId,m_compareMode) > TEXTIR_POSTINGSKIP_THRESHOLD)
                    operNode->oper.logicalIdHints = op1Posting->logicalDocId;
                else
                    operNode->oper.logicalIdHints = TEXTIR_POSTINGSKIP_NO_HINTS;

                /* skip the second operand */
                /* read the first operand(posting) without marking */
                op2Posting = &op2PostingQueue.Head();
                if(op2PostingQueue.PopHead() == false)
                {
                    op1PostingQueue.PushHead();
                    return eNOERROR;
                }
            }
        }
        
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
    }

    /* return */
    return eNOERROR;
}

inline Four OOSQL_Evaluator::execTextIR_OpNearWithOrder(
    AP_TextIndexCondPoolIndex   operInfo,           // IN    
    OOSQL_TextIR_PostingQueue&  postingQueue,       // INOUT 
    Four                        logicalIdHints      // IN    
)
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
    AP_TextIndexCondElement     *operNode;
    OOSQL_TextIR_Posting        *currPosting;
    OOSQL_TextIR_Posting        *op1Posting;
    OOSQL_TextIR_Posting        *op2Posting;
    OOSQL_TextIR_Weight         weight;
    OOSQL_TextIR_Bonus          bonus;
    Four                        e;
    AP_TextIndexCondPoolIndex   condNodeIndex;
    AP_TextIndexCondElement*    indexCondNode;
    Four                        distance;
    OOSQL_TextIR_PostingQueue&  op1PostingQueue = EVAL_TEXTIR_EVALBUFELEMENTS[operInfo.startIndex].op1PostingQueue;
    OOSQL_TextIR_PostingQueue&  op2PostingQueue = EVAL_TEXTIR_EVALBUFELEMENTS[operInfo.startIndex].op2PostingQueue;

    /* check input parameters */
#ifdef  OOSQL_DEBUG
    if(IS_NULL_POOLINDEX(operInfo) == SM_TRUE)
        OOSQL_ERR(eBADPARAMETER_OOSQL);
#endif

    /* get Text IR condition node for the current node */
    operNode = ACCESSPLAN.getTextIndexCondElem(operInfo);

#ifdef  OOSQL_DEBUG
    if(operNode == NULL) OOSQL_ERR(eNULL_POINTER_OOSQL);
#endif

    condNodeIndex = operNode->oper.op3;
    indexCondNode = ACCESSPLAN.getTextIndexCondElem(condNodeIndex);
    distance = indexCondNode->constant.getElements(m_pool->valuePool, 0).integer.getElements(m_pool->intPool, 0);

    /* set compare mode */
    /* if backward scan is used, get 1's complement */
    if (operNode->oper.scanDirection == TEXTIR_SCAN_FORWARD) m_compareMode = 0;
    else m_compareMode = ULONG_MAX;

    /* determine logicalIdHints to skip more postings */
    if (operNode->oper.logicalIdHints != TEXTIR_POSTINGSKIP_NO_HINTS && logicalIdHints != TEXTIR_POSTINGSKIP_NO_HINTS)
        logicalIdHints = TEXTIR_DOCID_MAX(operNode->oper.logicalIdHints,logicalIdHints,m_compareMode);
    else if (operNode->oper.logicalIdHints != TEXTIR_POSTINGSKIP_NO_HINTS)
        logicalIdHints = operNode->oper.logicalIdHints;

    // read the first operand with position information 
    while(op1PostingQueue.IsEmpty())
    {
        condNodeIndex = operNode->oper.op1;
        indexCondNode = ACCESSPLAN.getTextIndexCondElem(condNodeIndex);
        e = execTextIR_SubPlan(condNodeIndex, op1PostingQueue, logicalIdHints);
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

    // read the second operand with position information
    while(op2PostingQueue.IsEmpty())
    {
        condNodeIndex = operNode->oper.op2;
        indexCondNode = ACCESSPLAN.getTextIndexCondElem(condNodeIndex);
        e = execTextIR_SubPlan(condNodeIndex, op2PostingQueue, logicalIdHints);
        if(e < eNOERROR) OOSQL_ERR(e);
        if(e == TEXTIR_EOS) 
        {
            if(postingQueue.IsEmpty())
                return TEXTIR_EOS;
            else
                return eNOERROR;
        }
    }
    
    while(1)
    {
        if(op1PostingQueue.IsEmpty())
            break;
        if(op2PostingQueue.IsEmpty())
            break;

        // read the first operand with position information 
        // NOTE: The first operand must be a keyword node and
        //       the fetched posting contains position information.
        op1Posting = &op1PostingQueue.Head();
        op1PostingQueue.PopHead();

        // read the second operand with position information
        // NOTE: The second operand must be a keyword node and
        //       the fetched posting contains position information.
        op2Posting = &op2PostingQueue.Head();
        op2PostingQueue.PopHead();

        /* check if the logical doc. IDs of the two postings are equal */
        while((TEXTIR_DOCID_NE(op1Posting->logicalDocId,op2Posting->logicalDocId,m_compareMode)) || 
              (TEXTIR_DOCID_EQ(op1Posting->logicalDocId,op2Posting->logicalDocId,m_compareMode) && 
               !execTextIR_OpNear_IsNearWithOrder(op1Posting, op2Posting, distance)))
        {
            if(TEXTIR_DOCID_LT(op1Posting->logicalDocId,op2Posting->logicalDocId,m_compareMode))
            {
                if(TEXTIR_DOCID_DIFF(op2Posting->logicalDocId,op1Posting->logicalDocId,m_compareMode) > TEXTIR_POSTINGSKIP_THRESHOLD)
                    operNode->oper.logicalIdHints = op2Posting->logicalDocId;
                else
                    operNode->oper.logicalIdHints = TEXTIR_POSTINGSKIP_NO_HINTS;

                /* skip the first operand */
                /* read the first operand(posting) without marking */
                op1Posting = &op1PostingQueue.Head();
                if(op1PostingQueue.PopHead() == false)
                {
                    op2PostingQueue.PushHead();
                    return eNOERROR;
                }
            }
            else 
            {
                if(TEXTIR_DOCID_DIFF(op1Posting->logicalDocId,op2Posting->logicalDocId,m_compareMode) > TEXTIR_POSTINGSKIP_THRESHOLD)
                    operNode->oper.logicalIdHints = op1Posting->logicalDocId;
                else
                    operNode->oper.logicalIdHints = TEXTIR_POSTINGSKIP_NO_HINTS;

                /* skip the second operand */
                /* read the first operand(posting) without marking */
                op2Posting = &op2PostingQueue.Head();
                if(op2PostingQueue.PopHead() == false)
                {
                    op1PostingQueue.PushHead();
                    return eNOERROR;
                }
            }
        }
        
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
    }

    /* return */
    return eNOERROR;
}

inline Four OOSQL_Evaluator::execTextIR_OpNear_IsNear(
    OOSQL_TextIR_Posting*       op1Posting,
    OOSQL_TextIR_Posting*       op2Posting,
    Four                        distance
)
{
    void                        *op1Positions, *op2Positions;
    Four                        op1Sentense, op2Sentense;
    Two                         op1Word, op2Word;
    Four                        i, j;
    Four                        wordDistance;

    op1Positions = op1Posting->positions;
    if(op1Posting->positions && op2Posting->positions)
    {
        for(i = 0; i < op1Posting->nPositions; i++)
        {
            memcpy(&op1Sentense,(char*)op1Positions + 0, sizeof(Four));
            memcpy(&op1Word,    (char*)op1Positions + sizeof(Four), sizeof(Two));
            op2Positions = op2Posting->positions;
            for(j = 0; j < op2Posting->nPositions; j++)
            {
                memcpy(&op2Sentense,(char*)op2Positions + 0, sizeof(Four));
                memcpy(&op2Word,    (char*)op2Positions + sizeof(Four), sizeof(Two));

                if(op1Sentense == op2Sentense)
                {
                    wordDistance = op1Word - op2Word;
                    if(wordDistance < 0)
                        wordDistance = -wordDistance;
                    if(wordDistance <= distance)
                        return SM_TRUE;
                }
                op2Positions =(char*)op2Positions + sizeof(Two) + sizeof(Four);
            }
            op1Positions =(char*)op1Positions + sizeof(Two) + sizeof(Four);
        }
    }

    return SM_FALSE;
}

inline Four OOSQL_Evaluator::execTextIR_OpNear_IsNearWithOrder(
    OOSQL_TextIR_Posting*       op1Posting,
    OOSQL_TextIR_Posting*       op2Posting,
    Four                        distance
)
{
    void                        *op1Positions, *op2Positions;
    Four                        op1Sentense, op2Sentense;
    Two                         op1Word, op2Word;
    Four                        i, j;
    Four                        wordDistance;

    op1Positions = op1Posting->positions;
    if(op1Posting->positions && op2Posting->positions)
    {
        for(i = 0; i < op1Posting->nPositions; i++)
        {
            memcpy(&op1Sentense,(char*)op1Positions + 0, sizeof(Four));
            memcpy(&op1Word,    (char*)op1Positions + sizeof(Four), sizeof(Two));
            op2Positions = op2Posting->positions;
            for(j = 0; j < op2Posting->nPositions; j++)
            {
                memcpy(&op2Sentense,(char*)op2Positions + 0, sizeof(Four));
                memcpy(&op2Word,    (char*)op2Positions + sizeof(Four), sizeof(Two));

                if(op1Sentense == op2Sentense)
                {
                    wordDistance = op2Word - op1Word;
                    if(wordDistance <= distance && wordDistance >= 0)
                    {
                        return SM_TRUE;
                    }
                }
                op2Positions =(char*)op2Positions + sizeof(Four) + sizeof(Two);
            }
            op1Positions =(char*)op1Positions + sizeof(Four) + sizeof(Two);
        }
    }

    return SM_FALSE;
}

inline Four OOSQL_Evaluator::execTextIR_KeywordNode(
    AP_TextIndexCondPoolIndex   keywordInfo,        // IN    
    OOSQL_TextIR_PostingQueue&  postingQueue,       // INOUT 
    Four                        logicalIdHints      // IN   
)
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
    AP_TextIndexCondElement         *keywordNode;
    OOSQL_TextIR_Posting            *currPosting;
    oosql_TextIR_PostingScanInfo    *postingScanInfo;
    Four                            e;

    /* check input parameters */
#ifdef  OOSQL_DEBUG
    if(IS_NULL_POOLINDEX(keywordInfo) == SM_TRUE)
        OOSQL_ERR(eBADPARAMETER_OOSQL);
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

    /* determine from which we would fetch the posting
     * NOTE: If any wild character is used for the input keyword,
     *       we should fetch all postings and save them into a temporary file
     *       to sort them by logicalDocId and fetch the posting merged by logicalDocId.
     *       Otherwise, we fetch the posting through LOM interface.
     */
    if(postingScanInfo->isWildCharUsed() == SM_TRUE) 
    {
        if(EVAL_TEXTIR_EVALBUFELEMENTS[keywordInfo.startIndex].doneFlag == SM_FALSE) 
        {
            e = execTextIR_FetchAllAndSortPostings(keywordInfo);
            if(e < eNOERROR) 
                OOSQL_ERR(e);
            if(e == TEXTIR_EOS) 
                return TEXTIR_EOS;

            /* set done flag */
            EVAL_TEXTIR_EVALBUFELEMENTS[keywordInfo.startIndex].doneFlag = SM_TRUE;
        }
        
        e = execTextIR_NextPostingFromTempFile(keywordInfo, postingQueue, logicalIdHints);
        if(e < eNOERROR) OOSQL_ERR(e);
        if(e == TEXTIR_EOS) return TEXTIR_EOS;
    }
    else 
    {
        e = execTextIR_IndexScanNextPosting(keywordInfo, postingQueue, logicalIdHints);
        if(e < eNOERROR)    OOSQL_ERR(e);
        if(e == TEXTIR_EOS) return TEXTIR_EOS;
    }

    /* return */
    return eNOERROR;
}

inline Four OOSQL_Evaluator::execTextIR_IndexScanNextPosting(
    AP_TextIndexCondPoolIndex   scanInfo,           // IN    
    OOSQL_TextIR_PostingQueue&  postingQueue,       // INOUT 
    Four                        logicalIdHints      // IN   
)
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
    AP_TextIndexCondElement         *scanNode;
    OOSQL_TextIR_Posting            *currPosting;
    oosql_TextIR_PostingScanInfo    *postingScanInfo;
    Four                            nReturnedPosting;
    char                            currKeyword[MAXKEYWORDSIZE];
    char                            inputKeyword[MAXKEYWORDSIZE];
    Two                             keywordLen;
    Four                            requiredBufferSize;
    Four                            e;
    Four                            nPostings;
    Four                            classID;
#ifdef COMPRESSION
	VolNo							volNoOfPostingTupleID;
	Four							lastDocId;
#endif

    /* check input parameters */
#ifdef  OOSQL_DEBUG
    if(IS_NULL_POOLINDEX(scanInfo) == SM_TRUE)
        OOSQL_ERR(eBADPARAMETER_OOSQL);
#endif

    /* get Text IR condition node for the current node */
    scanNode = ACCESSPLAN.getTextIndexCondElem(scanInfo);

#ifdef  OOSQL_DEBUG
    if(scanNode == NULL) OOSQL_ERR(eNULL_POINTER_OOSQL);
#endif

    /* set compare mode */
    /* if backward scan is used, get 1's complement */
    if (scanNode->keyword.scanDirection == TEXTIR_SCAN_FORWARD) m_compareMode = 0;
    else m_compareMode = ULONG_MAX;

    /* get pointer to the posting of the current node */
    currPosting = &(EVAL_TEXTIR_EVALBUFELEMENTS[scanInfo.startIndex].posting);

    /* get pointer to the posting scan information */
    postingScanInfo = EVAL_TEXTIR_EVALBUFELEMENTS[scanInfo.startIndex].postingScanInfo;

    // get classid
    classID = EVAL_ACCESSLISTTABLE[m_currPlanIndex].accessList[EVAL_ACCESSLISTTABLE[m_currPlanIndex].currAccessIndex].classId;

    if(IS_NULL_POOLINDEX(scanNode->keyword.keywordWithWildChar))
    {
        if(postingScanInfo->isEmptyPostingBuffer())
        {
            e = m_storageManager->Text_NextPostings(
                                        postingScanInfo->m_osn,
                                        postingScanInfo->m_postingLengthBufferSize, 
                                        postingScanInfo->m_postingLengthBuffer,
                                        postingScanInfo->m_postingBufferSize, 
                                        postingScanInfo->m_postingBuffer,
                                        scanNode->keyword.scanDirection,
                                        logicalIdHints,
#ifndef COMPRESSION
                                        &nReturnedPosting, &requiredBufferSize);
#else
										&nReturnedPosting, &requiredBufferSize, &volNoOfPostingTupleID, &lastDocId);
#endif
            if(e == eBIGGERPOSTINGBUFFERNEEDED_OOSQL)
            {
                while(postingScanInfo->m_postingBufferSize <= requiredBufferSize)
                {
                    e = postingScanInfo->doublePostingBuffer();
                    OOSQL_CHECK_ERR(e);
                }
                e = m_storageManager->Text_NextPostings(
                                            postingScanInfo->m_osn,
                                            postingScanInfo->m_postingLengthBufferSize, 
                                            postingScanInfo->m_postingLengthBuffer,
                                            postingScanInfo->m_postingBufferSize, 
                                            postingScanInfo->m_postingBuffer,
                                            scanNode->keyword.scanDirection,
                                            logicalIdHints,
#ifndef COMPRESSION
                                            &nReturnedPosting, &requiredBufferSize);
#else
											&nReturnedPosting, &requiredBufferSize, &volNoOfPostingTupleID, &lastDocId);
#endif
            }

            if(e < eNOERROR)
            {
                OOSQL_ERR(e);
            }
            else if(e == EOS)
            {
                if(postingQueue.IsEmpty())
                    return TEXTIR_EOS;
                else
                    return eNOERROR;
            }

            e = m_storageManager->Text_GetNPostingsOfCurrentKeyword(postingScanInfo->m_osn, &postingScanInfo->m_nPostingsOfCurrentKeyword);
            if(e < eNOERROR) OOSQL_ERR(e);

            /* reset the control information for the posting buffer */
            postingScanInfo->resetPostingBuffer(nReturnedPosting, scanNode->keyword.scanDirection);
        }

#ifndef COMPRESSION		
        e = postingScanInfo->getNextPosting(postingQueue, logicalIdHints, classID, postingScanInfo->m_nPostingsOfCurrentKeyword, scanNode->keyword.scanDirection);
#else
		e = postingScanInfo->getNextPosting(postingQueue, logicalIdHints, classID, postingScanInfo->m_nPostingsOfCurrentKeyword, scanNode->keyword.scanDirection, volNoOfPostingTupleID, lastDocId);
#endif
        OOSQL_CHECK_ERR(e);
    }
    else
    {
        do {
            if(postingScanInfo->isEmptyPostingBuffer() == SM_TRUE) 
            {
                /* fetch the next posting(s) through LOM interface */
                e = m_storageManager->Text_NextPostings(
                                            postingScanInfo->m_osn,
                                            postingScanInfo->m_postingLengthBufferSize, 
                                            postingScanInfo->m_postingLengthBuffer,
                                            postingScanInfo->m_postingBufferSize, 
                                            postingScanInfo->m_postingBuffer,
                                            scanNode->keyword.scanDirection,
                                            logicalIdHints,
#ifndef COMPRESSION
                                            &nReturnedPosting, &requiredBufferSize);
#else
											&nReturnedPosting, &requiredBufferSize, &volNoOfPostingTupleID, &lastDocId);
#endif
                if(e == eBIGGERPOSTINGBUFFERNEEDED_OOSQL)
                {
                    while(postingScanInfo->m_postingBufferSize <= requiredBufferSize)
                    {
                        e = postingScanInfo->doublePostingBuffer();
                        OOSQL_CHECK_ERR(e);
                    }
                    e = m_storageManager->Text_NextPostings(
                                                postingScanInfo->m_osn,
                                                postingScanInfo->m_postingLengthBufferSize, 
                                                postingScanInfo->m_postingLengthBuffer,
                                                postingScanInfo->m_postingBufferSize, 
                                                postingScanInfo->m_postingBuffer,
                                                scanNode->keyword.scanDirection,
                                                logicalIdHints,
#ifndef COMPRESSION
                                                &nReturnedPosting, &requiredBufferSize);
#else
												&nReturnedPosting, &requiredBufferSize, &volNoOfPostingTupleID, &lastDocId);
#endif
                }

                if(e < eNOERROR) 
                    OOSQL_ERR(e);
                if(e == EOS)
                {
                    if(postingQueue.IsEmpty())
                        return TEXTIR_EOS;
                    else
                        return eNOERROR;
                }

                e = m_storageManager->Text_GetNPostingsOfCurrentKeyword(postingScanInfo->m_osn, &postingScanInfo->m_nPostingsOfCurrentKeyword);
                if(e < eNOERROR) OOSQL_ERR(e);

                /* reset the control information for the posting buffer */
                postingScanInfo->resetPostingBuffer(nReturnedPosting, scanNode->keyword.scanDirection);
            }

            /* fetch the next posting from the posting buffer */
#ifndef COMPRESSION			
            e = postingScanInfo->getNextPosting(currPosting, logicalIdHints, scanNode->keyword.scanDirection);
#else
			e = postingScanInfo->getNextPosting(currPosting, logicalIdHints, scanNode->keyword.scanDirection, volNoOfPostingTupleID, &lastDocId);
#endif
            OOSQL_CHECK_ERR(e);

#ifdef USE_WITHOUT_WEIGHT
            nPostings = postingScanInfo->m_nPostingsOfCurrentKeyword;

            currPosting->docId.classID = EVAL_ACCESSLISTTABLE[m_currPlanIndex].accessList[EVAL_ACCESSLISTTABLE[m_currPlanIndex].currAccessIndex].classId;
            if(currPosting->nPositions == 0)
                currPosting->weight = 1;
            else

#if defined (EXTENDED_BOOLEAN_MODEL)
                currPosting->weight = OOSQL_StorageManager::CalculateWeight(currPosting->nPositions, nPostings);
#elif defined (HEURISTIC_MODEL)
                currPosting->weight = OOSQL_StorageManager::CalculateWeight(currPosting->nPositions, (double)nPostings / (double)(postingScanInfo->m_nObjectsInClass));
#endif

#endif
            /* get keyword for the current scan cursor */
            e = m_storageManager->Text_GetCursorKeyword(postingScanInfo->m_osn, &currKeyword[0]);
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
            e = OQL_EvalOpLike(&currKeyword[sizeof(Two)],(char*)inputKeyword);
            OOSQL_CHECK_ERR(e);
        } while(IS_NULL_POOLINDEX(scanNode->keyword.keywordWithWildChar) == SM_FALSE && e == LIKE_FALSE);

        /* add to result postingQueue */
        postingQueue.PushTail(*currPosting);
    }

    /* return */
    return eNOERROR;
}

inline Four OOSQL_Evaluator::execTextIR_NextPostingFromTempFile(
    AP_TextIndexCondPoolIndex   scanInfo,           // IN
    OOSQL_TextIR_PostingQueue&  postingQueue,       // INOUT 
    Four                        logicalIdHints      // IN    
)
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
    AP_TextIndexCondElement         *scanNode;
    OOSQL_TextIR_Posting            *currPosting;
    oosql_TextIR_TempFileInfo       *tempFileInfo;
    oosql_TextIR_PostingScanInfo    *postingScanInfo;
    OOSQL_StorageManager::OID       oid;
    OOSQL_TextIR_LogicalDocID       logicalDocId;
    OOSQL_TextIR_Weight             weight;
    Four                            e;
    char*                           pSortBufferReadPtr;
    Four                            nElementsHasBeenRead;
    bool                            endOfEvaluation;

    /* check input parameters */
#ifdef  OOSQL_DEBUG
    if(IS_NULL_POOLINDEX(scanInfo) == SM_TRUE)
        OOSQL_ERR(eBADPARAMETER_OOSQL);
#endif

    /* get Text IR condition node for the current node */
    scanNode = ACCESSPLAN.getTextIndexCondElem(scanInfo);

    /* set compare mode */
    /* if backward scan is used, get 1's complement */
    if (scanNode->keyword.scanDirection == TEXTIR_SCAN_FORWARD) m_compareMode = 0;
    else m_compareMode = ULONG_MAX;

    /* get pointer to temporary file information */
    postingScanInfo = EVAL_TEXTIR_EVALBUFELEMENTS[scanInfo.startIndex].postingScanInfo;
    tempFileInfo    = postingScanInfo->m_tempFileInfo;

    /* get pointer to the posting of the current node */
    currPosting = &(EVAL_TEXTIR_EVALBUFELEMENTS[scanInfo.startIndex].posting);

    if(m_sortBufferInfo.mode == OOSQL_SB_USE_DISK)
    {
        if(tempFileInfo == NULL)
        {
            /* temporary file had been destroyed */
            return TEXTIR_EOS;
        }
    }
    else
    {
        if(tempFileInfo->sortBufferLength <= tempFileInfo->sortBufferCurrentReadOffset)
        {
            return TEXTIR_EOS;
        }
    }

    if(m_sortBufferInfo.mode == OOSQL_SB_USE_DISK)
    {
        nElementsHasBeenRead = 0;
        while(1)
        {
            if(tempFileInfo->markFlag == SM_TRUE) 
            {
                /* read posting from the column list buffer */
                e = execTextIR_convertColListToPosting(scanNode->keyword.usedPostingKind, 
                                                       tempFileInfo->clist, currPosting);
                if(e < eNOERROR) OOSQL_ERR(e);

                /* reset mark flag */
                tempFileInfo->markFlag = SM_FALSE;
            }
            else 
            {
                /* fetch the posting from the sort stream file */
                e = tempFileInfo->sortStream->FastNextScan(tempFileInfo->nCols, tempFileInfo->clist);
                if(e < eNOERROR) 
                {
                    OOSQL_ERR(e);
                }
                else if(e == EOS) 
                {
					if(tempFileInfo != NULL)
					{
                    	e = execTextIR_FreeTempFile(tempFileInfo);
                    	OOSQL_CHECK_ERR(e);

						postingScanInfo->m_tempFileInfo = NULL;
					}

                    if(postingQueue.IsEmpty())
                        return TEXTIR_EOS;
                    else
                        return eNOERROR;
                }

                /* convert column list to posting */
                e = execTextIR_convertColListToPosting(scanNode->keyword.usedPostingKind, 
                                                       tempFileInfo->clist, currPosting);
                if(e < eNOERROR) OOSQL_ERR(e);
            }

            if(TEXTIR_DOCID_GT(logicalIdHints,currPosting->logicalDocId,m_compareMode))
                continue;

            /* add to result postingQueue */
            postingQueue.PushTail(*currPosting);
            nElementsHasBeenRead ++;

            /*
             * find the maximum weight of the postings having the same logical doc. ID
             */
            /* fetch the next posting stored in the sort stream */
            while(1) 
            {
                /* move scan cursor to fetch the next posting from sort stream */
                e = tempFileInfo->sortStream->FastNextScan(tempFileInfo->nCols, tempFileInfo->clist);
                OOSQL_CHECK_ERR(e);
                if(e == EOS)
                {
					if(tempFileInfo != NULL)
					{
                    	e = execTextIR_FreeTempFile(tempFileInfo);
                    	OOSQL_CHECK_ERR(e);

						postingScanInfo->m_tempFileInfo = NULL;
					}

                    if(postingQueue.IsEmpty())
                        return TEXTIR_EOS;
                    else
                        return eNOERROR;
                }

                /* get logical doc. ID of the read-in posting 
                 * NOTE: the type of logical doc. ID is Four
                 */
                logicalDocId = GET_VALUE_FROM_COL_LIST(tempFileInfo->clist[0], sizeof(Four));

                /* check if logical doc. ID of the read-in posting is equal to the current posting */
                if(TEXTIR_DOCID_EQ(currPosting->logicalDocId,logicalDocId,m_compareMode))
                {
                    weight = tempFileInfo->clist[2].data.d;

                    /* check if the weight of the current posting with that of the read-in posting */
                    if(currPosting->weight < weight)
                        currPosting->weight = weight;
                }
                else 
                {
                    /* set mark flag for the next posting */
                    tempFileInfo->markFlag = SM_TRUE;
                    break;
                }
            }

            if(nElementsHasBeenRead > TEXTIR_DEFAULTPOSTINGQUEUESIZE)
                return eNOERROR;
        }
    }
    else 
    {
        endOfEvaluation = false;
        nElementsHasBeenRead = 0;

        currPosting->nPositions                  = 0;
        currPosting->positions                   = NULL;
        currPosting->ptrToEmbeddedAttrsBuf       = NULL;
        currPosting->embeddedAttrsBufSize        = NIL;
        currPosting->embeddedAttrTranslationInfo = NULL;
        currPosting->scanId                      = postingScanInfo->m_osn;
        currPosting->indexInfoPoolIndex          = postingScanInfo->m_indexInfoPoolIndex;
#if defined (HEURISTIC_MODEL)
        currPosting->bonus = HEURISTIC_BOUNS_WEIGHT_VALUE;
#endif


        while(!endOfEvaluation)
        {
            pSortBufferReadPtr =(char*)tempFileInfo->sortBufferPtr + tempFileInfo->sortBufferCurrentReadOffset;

            memcpy(&currPosting->logicalDocId, pSortBufferReadPtr, sizeof(Four));
            pSortBufferReadPtr += sizeof(Four);

            if(TEXTIR_DOCID_LE(logicalIdHints,currPosting->logicalDocId,m_compareMode))
            {
                memcpy(&currPosting->docId, pSortBufferReadPtr, sizeof(OOSQL_StorageManager::OID));
                pSortBufferReadPtr += sizeof(OOSQL_StorageManager::OID);

                memcpy(&currPosting->weight, pSortBufferReadPtr, TEXTIR_SIZE_WEIGHT);
                pSortBufferReadPtr += TEXTIR_SIZE_WEIGHT;

                postingQueue.PushTail(*currPosting);
                nElementsHasBeenRead ++;

                while(1)
                {
                    Four    nextPostingLogicalID;
                    
                    if(tempFileInfo->sortBufferLength <= tempFileInfo->sortBufferCurrentReadOffset)
                    {
                        endOfEvaluation = true;
                        break;  
                    }

                    pSortBufferReadPtr =(char*)tempFileInfo->sortBufferPtr + tempFileInfo->sortBufferCurrentReadOffset;
                    
                    memcpy(&nextPostingLogicalID, pSortBufferReadPtr, sizeof(Four));
                    
                    if(TEXTIR_DOCID_NE(currPosting->logicalDocId,nextPostingLogicalID,m_compareMode))
                        break;
                    else
                        tempFileInfo->sortBufferCurrentReadOffset += OOSQL_MEMORYSORT_POSTINGSIZE;
                }

                if(nElementsHasBeenRead > TEXTIR_DEFAULTPOSTINGQUEUESIZE)
                    endOfEvaluation = true;
            }
            else
            {
                tempFileInfo->sortBufferCurrentReadOffset += OOSQL_MEMORYSORT_POSTINGSIZE;
                continue;
            }
        }
    }

    /* return */
    return eNOERROR;
}

#endif

#else /* SLIMDOWN_TEXTIR */

#include "OOSQL_Evaluator.hxx"
#include "OOSQL_SortStream.hxx"


inline Four OOSQL_Evaluator::execTextIR_SubPlan(
    AP_TextIndexCondPoolIndex	condNodeIndex,		// IN    
	OOSQL_TextIR_PostingQueue&	postingQueue,		// INOUT 
	Four						logicalIdHints		// IN    
)
{
	return eTEXTIR_NOTENABLED_OOSQL;
}


inline Four OOSQL_Evaluator::execTextIR_CmpOp(
    AP_TextIndexCondPoolIndex	operInfo,			// IN    
	OOSQL_TextIR_PostingQueue&	postingQueue,		// INOUT 
	Four						logicalIdHints		// IN   
)
{
	return eTEXTIR_NOTENABLED_OOSQL;
}


inline Four OOSQL_Evaluator::execTextIR_OpAnd(
    AP_TextIndexCondPoolIndex	operInfo,			// IN    
	OOSQL_TextIR_PostingQueue&	postingQueue,		// INOUT 
	Four						logicalIdHints		// IN   
)
{
	return eTEXTIR_NOTENABLED_OOSQL;
}


inline Four OOSQL_Evaluator::execTextIR_OpOr(
    AP_TextIndexCondPoolIndex	operInfo,			// IN    
	OOSQL_TextIR_PostingQueue&	postingQueue,		// INOUT 
	Four						logicalIdHints		// IN   
)
{
	return eTEXTIR_NOTENABLED_OOSQL;
}


inline Four OOSQL_Evaluator::execTextIR_OpMinus(
    AP_TextIndexCondPoolIndex	operInfo,			// IN	 
	OOSQL_TextIR_PostingQueue&	postingQueue,		// INOUT 
	Four						logicalIdHints		// IN	 
)
{
	return eTEXTIR_NOTENABLED_OOSQL;
}


inline Four OOSQL_Evaluator::execTextIR_OpAccumulate(
    AP_TextIndexCondPoolIndex	operInfo,			// IN    
	OOSQL_TextIR_PostingQueue&	postingQueue,		// INOUT 
	Four						logicalIdHints		// IN    
)
{
	return eTEXTIR_NOTENABLED_OOSQL;
}


inline Four OOSQL_Evaluator::execTextIR_OpMultiply(
    AP_TextIndexCondPoolIndex	operInfo,			// IN    
	OOSQL_TextIR_PostingQueue&	postingQueue,		// INOUT 
	Four						logicalIdHints		// IN   
)
{
	return eTEXTIR_NOTENABLED_OOSQL;
}


inline Four OOSQL_Evaluator::execTextIR_OpThreshold(
    AP_TextIndexCondPoolIndex	operInfo,			// IN    
	OOSQL_TextIR_PostingQueue&	postingQueue,		// INOUT 
	Four						logicalIdHints		// IN   
)
{
	return eTEXTIR_NOTENABLED_OOSQL;
}

inline Four OOSQL_Evaluator::execTextIR_OpMax(
    AP_TextIndexCondPoolIndex	operInfo,			// IN    
	OOSQL_TextIR_PostingQueue&	postingQueue,		// INOUT 
	Four						logicalIdHints		// IN    
)
{
	return eTEXTIR_NOTENABLED_OOSQL;
}


inline Four OOSQL_Evaluator::execTextIR_OpNear(
    AP_TextIndexCondPoolIndex	operInfo,			// IN    
	OOSQL_TextIR_PostingQueue&	postingQueue,		// INOUT 
	Four						logicalIdHints		// IN   
)
{
	return eTEXTIR_NOTENABLED_OOSQL;
}


inline Four OOSQL_Evaluator::execTextIR_OpNearWithOrder(
    AP_TextIndexCondPoolIndex	operInfo,			// IN    
	OOSQL_TextIR_PostingQueue&	postingQueue,		// INOUT
	Four						logicalIdHints		// IN  
)
{
	return eTEXTIR_NOTENABLED_OOSQL;
}


inline Four OOSQL_Evaluator::execTextIR_OpNear_IsNear(
    OOSQL_TextIR_Posting*		op1Posting,
    OOSQL_TextIR_Posting*		op2Posting,
	Four						distance
)
{
	return eTEXTIR_NOTENABLED_OOSQL;
}


inline Four OOSQL_Evaluator::execTextIR_OpNear_IsNearWithOrder(
    OOSQL_TextIR_Posting*		op1Posting,
    OOSQL_TextIR_Posting*		op2Posting,
	Four						distance
)
{
	return eTEXTIR_NOTENABLED_OOSQL;
}


inline Four OOSQL_Evaluator::execTextIR_KeywordNode(
    AP_TextIndexCondPoolIndex	keywordInfo,		// IN   
	OOSQL_TextIR_PostingQueue&	postingQueue,		// INOUT
	Four						logicalIdHints		// IN	
)
{
	return eTEXTIR_NOTENABLED_OOSQL;
}


inline Four OOSQL_Evaluator::execTextIR_IndexScanNextPosting(
    AP_TextIndexCondPoolIndex	scanInfo,			// IN	
	OOSQL_TextIR_PostingQueue&	postingQueue,		// INOUT 
	Four						logicalIdHints		// IN	 
)
{
	return eTEXTIR_NOTENABLED_OOSQL;
}


inline Four OOSQL_Evaluator::execTextIR_NextPostingFromTempFile(
    AP_TextIndexCondPoolIndex   scanInfo,			// IN
	OOSQL_TextIR_PostingQueue&	postingQueue,		// INOUT 
	Four						logicalIdHints		// IN	 
)
{
	return eTEXTIR_NOTENABLED_OOSQL;
}


#endif /* SLIMDOWN_TEXTIR*/

