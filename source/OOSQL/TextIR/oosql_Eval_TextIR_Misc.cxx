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

#include "OOSQL_Evaluator.hxx"
#include "OOSQL_SortStream.hxx"
#include "oosql_Eval_TextIR.hxx"

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
< eNOERROR         

IMPLEMENTATION:
****************************************************************************/
#if defined (EXTENDED_BOOLEAN_MODEL)
Four OOSQL_Evaluator::execTextIR_setPosting(
    OOSQL_TextIR_Posting		*destPosting,   // OUT
	OOSQL_TextIR_Posting        *srcPosting,	// IN
    OOSQL_TextIR_Weight			weight          // IN
)
#elif defined (HEURISTIC_MODEL)
Four OOSQL_Evaluator::execTextIR_setPosting(
    OOSQL_TextIR_Posting		*destPosting,   // OUT
	OOSQL_TextIR_Posting        *srcPosting,	// IN
    OOSQL_TextIR_Weight			weight,         // IN
    OOSQL_TextIR_Bonus			bonus           // IN
)
#endif
{
    /* check input parameters */
#ifdef  OOSQL_DEBUG
	if(destPosting == NULL || srcPosting == NULL)
		OOSQL_ERR(eBADPARAMETER_OOSQL);
#endif

    /* set posting */
	memcpy(destPosting, srcPosting, sizeof(OOSQL_TextIR_Posting));
	destPosting->weight = weight;

#if defined (HEURISTIC_MODEL)
	destPosting->bonus  = bonus;
#endif

    /* return */
    return eNOERROR;
}


/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eBADPARAMETER_OOSQL
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four    OOSQL_Evaluator::execTextIR_makeMinMaxHeapEntryDescForPosting(
        OOSQL_MinMaxHeap_EntryDesc    **minMaxHeapEntryDesc   // IN/OUT:
)
{
    /* check input parameter */
#ifdef  OOSQL_DEBUG
    if (minMaxHeapEntryDesc == NULL)
		OOSQL_ERR(eBADPARAMETER_OOSQL);
#endif

    /* 
     * construct Min-Max Heap entry description 
     * NOTE: caller must deallocate this memory
     */
	OOSQL_ARRAYNEW(*minMaxHeapEntryDesc, pMemoryManager, OOSQL_MinMaxHeap_EntryDesc, TEXTIR_NUMFIELDS_IN_POSTING_WITHOUTPOSITION);
	if(*minMaxHeapEntryDesc == NULL)
		OOSQL_ERR(eOUTOFMEMORY_OOSQL);

    /* make field information for 'logicalDocId' */
    (*minMaxHeapEntryDesc)->fieldInfo[0].type = OOSQL_TYPE_LONG_VAR;
    (*minMaxHeapEntryDesc)->fieldInfo[0].length = OOSQL_TYPE_LONG_SIZE_VAR;

    /* make field information for 'docId' */
    (*minMaxHeapEntryDesc)->fieldInfo[1].type = OOSQL_TYPE_OID;
    (*minMaxHeapEntryDesc)->fieldInfo[1].length = OOSQL_TYPE_OID_SIZE;

    /* make field information for 'weight' */
    (*minMaxHeapEntryDesc)->fieldInfo[2].type = OOSQL_TYPE_FLOAT;
    (*minMaxHeapEntryDesc)->fieldInfo[2].length = OOSQL_TYPE_FLOAT_SIZE;

    /* make key field */
    (*minMaxHeapEntryDesc)->keyField = 2;   // 'weight' is key field

    /* return */
    return eNOERROR;
}

Four    OOSQL_Evaluator::execTextIR_convertPostingToMinMaxHeapEntry( 
        OOSQL_TextIR_Posting    *posting,       // IN:
        OOSQL_MinMaxHeap_FieldList    *fields         // IN/OUT:
)
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
        /* check input parameters */
#ifdef  OOSQL_DEBUG
        if (posting == NULL) OOSQL_ERR(eBADPARAMETER_OOSQL);
        if (fields == NULL) OOSQL_ERR(eBADPARAMETER_OOSQL);
#endif

        /* copy posting to the Min-Max Heap entry */
        ASSIGN_VALUE_TO_COL_LIST(fields[0], posting->logicalDocId, sizeof(Four));
        fields[1].data.oid = posting->docId;
        fields[2].data.f = posting->weight;

        /* return */
        return eNOERROR;
}


Four    OOSQL_Evaluator::execTextIR_convertMinMaxHeapEntryToPosting( 
        OOSQL_MinMaxHeap_FieldList    *fields,        // IN:
        OOSQL_TextIR_Posting    *posting        // IN/OUT:
)
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
        /* check input parameters */
#ifdef  OOSQL_DEBUG
        if (fields == NULL) OOSQL_ERR(eBADPARAMETER_OOSQL);
        if (posting == NULL) OOSQL_ERR(eBADPARAMETER_OOSQL);
#endif

        /* copy the Min Heap entry to the posting */
        posting->logicalDocId = GET_VALUE_FROM_COL_LIST(fields[0], sizeof(Four));
        posting->docId = fields[1].data.oid;
        posting->weight = fields[2].data.f;

        /* return */
        return eNOERROR;
}

Four    OOSQL_Evaluator::execTextIR_convertColListToPosting( 
        Four									postingKind,                    // IN:
        OOSQL_StorageManager::ColListStruct		*clist,                 // IN:
        OOSQL_TextIR_Posting					*result         // OUT:
)
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
        /* check input parameters */

        /* copy column list to posting */
        result->logicalDocId				  = GET_VALUE_FROM_COL_LIST(clist[0], sizeof(Four));
        result->docId						  = clist[1].data.oid;
        result->weight						  = clist[2].data.f;

		result->nPositions					  = 0;
		result->positions					  = NULL;
		result->ptrToEmbeddedAttrsBuf		  = NULL;
		result->embeddedAttrTranslationInfo   = NULL;
		result->getEmbeddedAttrsValFuncPtr	  = NULL;
		result->indexInfoPoolIndex.setNull();

        /* return */
        return eNOERROR;
}


Four    OOSQL_Evaluator::execTextIR_convertPostingToColList( 
        Four    postingKind,                    // IN:
        OOSQL_TextIR_Posting    *posting,       // OUT:
        OOSQL_StorageManager::ColListStruct   *clist                  // IN:
)
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
        /* check input parameters */
#ifdef  OOSQL_DEBUG
        if (posting == NULL) OOSQL_ERR(eBADPARAMETER_OOSQL);
        if (clist == NULL) OOSQL_ERR(eBADPARAMETER_OOSQL);
#endif

        /* copy column list to posting */
        ASSIGN_VALUE_TO_COL_LIST(clist[0], posting->logicalDocId, sizeof(Four));
        clist[1].data.oid = posting->docId;
        clist[2].data.f = posting->weight;

        /* return */
        return eNOERROR;
}


Four    OOSQL_Evaluator::execTextIR_FreeTempFile(
    oosql_TextIR_TempFileInfo       *tempFileInfo
)
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
	Four e;

	if(tempFileInfo->sortStream)
	{
		e = tempFileInfo->sortStream->DestroyStream();
		OOSQL_CHECK_ERR(e);

		OOSQL_DELETE(tempFileInfo->sortStream);
	}
	tempFileInfo->sortStream = NULL;

	OOSQL_DELETE(tempFileInfo);

    return eNOERROR;
}


Four    OOSQL_Evaluator::compareTextIR_Keyword(
        Two     key1Len,                // IN:
        char    *keyword1,              // IN:
        Two     key2Len,                // IN:
        char    *keyword2               // IN:
)
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
        Two     i1, i2;

        /* check input parameters */
#ifdef  OOSQL_DEBUG
        if (key1Len < 0) OOSQL_ERR(eBADPARAMETER_OOSQL);
        if (keyword1 == NULL) OOSQL_ERR(eBADPARAMETER_OOSQL);
        if (key2Len < 0) OOSQL_ERR(eBADPARAMETER_OOSQL);
        if (keyword2 == NULL) OOSQL_ERR(eBADPARAMETER_OOSQL);
#endif
        /* 
         * compare the two keywords 
         */
        for (i1 = 0, i2 = 0; i1 < key1Len && i2 < key2Len; i1++, i2++) {
            if ( keyword1[i1] > keyword2[i2] )
                return CMP_GT;
            else if ( keyword1[i1] < keyword2[i2] )
                return CMP_LT;
        }

        if (i1 < key1Len)
            return CMP_GT;
        else if (i2 < key2Len)
            return CMP_LT;
        else
            return CMP_EQ;
}

#else /* SLIMDOWN_TEXTIR */


#include "OOSQL_Evaluator.hxx"
#include "OOSQL_SortStream.hxx"
#include "oosql_Eval_TextIR.hxx"

#if defined (EXTENDED_BOOLEAN_MODEL)
Four OOSQL_Evaluator::execTextIR_setPosting(
    OOSQL_TextIR_Posting		*destPosting,   // OUT
	OOSQL_TextIR_Posting        *srcPosting,	// IN
    OOSQL_TextIR_Weight			weight          // IN
)
#elif defined (HEURISTIC_MODEL)
Four OOSQL_Evaluator::execTextIR_setPosting(
    OOSQL_TextIR_Posting		*destPosting,   // OUT
	OOSQL_TextIR_Posting        *srcPosting,	// IN
    OOSQL_TextIR_Weight			weight,         // IN
    OOSQL_TextIR_Bonus			bonus           // IN
)
#endif
{
	return eTEXTIR_NOTENABLED_OOSQL;
}


Four    OOSQL_Evaluator::execTextIR_makeMinMaxHeapEntryDescForPosting(
        OOSQL_MinMaxHeap_EntryDesc    **minMaxHeapEntryDesc   // IN/OUT:
)
{
	return eTEXTIR_NOTENABLED_OOSQL;
}


Four    OOSQL_Evaluator::execTextIR_convertPostingToMinMaxHeapEntry( 
        OOSQL_TextIR_Posting    *posting,       // IN:
        OOSQL_MinMaxHeap_FieldList    *fields         // IN/OUT:
)
{
	return eTEXTIR_NOTENABLED_OOSQL;
}


Four    OOSQL_Evaluator::execTextIR_convertMinMaxHeapEntryToPosting( 
        OOSQL_MinMaxHeap_FieldList    *fields,        // IN:
        OOSQL_TextIR_Posting    *posting        // IN/OUT:
)
{
	return eTEXTIR_NOTENABLED_OOSQL;
}


Four    OOSQL_Evaluator::execTextIR_convertColListToPosting( 
        Four									postingKind,                    // IN:
        OOSQL_StorageManager::ColListStruct		*clist,                 // IN:
        OOSQL_TextIR_Posting					*result         // OUT:
)
{
	return eTEXTIR_NOTENABLED_OOSQL;
}


Four    OOSQL_Evaluator::execTextIR_convertPostingToColList( 
        Four    postingKind,                    // IN:
        OOSQL_TextIR_Posting    *posting,       // OUT:
        OOSQL_StorageManager::ColListStruct   *clist                  // IN:
)
{
	return eTEXTIR_NOTENABLED_OOSQL;
}


Four    OOSQL_Evaluator::execTextIR_FreeTempFile(
    oosql_TextIR_TempFileInfo       *tempFileInfo
)
{
	return eTEXTIR_NOTENABLED_OOSQL;
}


Four    OOSQL_Evaluator::compareTextIR_Keyword(
        Two     key1Len,                // IN:
        char    *keyword1,              // IN:
        Two     key2Len,                // IN:
        char    *keyword2               // IN:
)
{
	return eTEXTIR_NOTENABLED_OOSQL;
}

#endif /* SLIMDOWN_TEXTIR */
