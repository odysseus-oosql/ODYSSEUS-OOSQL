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


#ifndef _oosql_Eval_TextIR_Internal_hxx_
#define _oosql_Eval_TextIR_Internal_hxx_

/*
    MODULE:
        oosql_Eval_TextIR_Internal.hxx

    DESCRIPTION:
*/


/*
 * include necessary module interface
 */
#include "OOSQL_Common.h"
#include "OOSQL_MinMaxHeap.hxx"
#include "OOSQL_MemoryManager.hxx"
#include "OOSQL_MemoryManagedObject.hxx"

class OOSQL_SortStream;
/*
 * constant definitions for manipulating Text IR posting
 */
#define TEXTIR_SIZE_LOGICALDOCID        sizeof(Four)
#define TEXTIR_SIZE_DOCID               sizeof(OOSQL_StorageManager::OID)
#define TEXTIR_SIZE_WEIGHT              sizeof(OOSQL_TextIR_Weight)
#define TEXTIR_SIZE_NPOSITION           sizeof(Four)
#define TEXTIR_POSTINGSKIP_NO_HINTS     -1              
#define TEXTIR_POSTINGSKIP_THRESHOLD    1000            

#if     !defined(USE_SUBINDEX) && !defined(USE_WITHOUT_WEIGHT)
#define TEXTIR_NPOSITIONOFFSET \
        ( TEXTIR_SIZE_LOGICALDOCID + TEXTIR_SIZE_DOCID + TEXTIR_SIZE_WEIGHT )
#define TEXTIR_SIZE_FIXEDPOSTINGFIELD \
        ( TEXTIR_SIZE_LOGICALDOCID + TEXTIR_SIZE_DOCID + \
          TEXTIR_SIZE_WEIGHT + TEXTIR_SIZE_NPOSITION )

#endif

#if     defined(USE_SUBINDEX) && !defined(USE_WITHOUT_WEIGHT)
#if     defined(ORDEREDSET_BACKWARD_SCAN)
#define TEXTIR_SIZE_POSTINGLENGTH       0
#else
#define TEXTIR_SIZE_POSTINGLENGTH       sizeof(Four)
#endif
#define TEXTIR_NPOSITIONOFFSET \
        ( TEXTIR_SIZE_POSTINGLENGTH + TEXTIR_SIZE_LOGICALDOCID + \
          TEXTIR_SIZE_DOCID + TEXTIR_SIZE_WEIGHT )
#define TEXTIR_SIZE_FIXEDPOSTINGFIELD \
        ( TEXTIR_SIZE_POSTINGLENGTH + TEXTIR_SIZE_LOGICALDOCID + \
          TEXTIR_SIZE_DOCID + TEXTIR_SIZE_WEIGHT + TEXTIR_SIZE_NPOSITION )
#endif

#if     !defined(USE_SUBINDEX) && defined(USE_WITHOUT_WEIGHT)
#undef  TEXTIR_SIZE_DOCID
#define TEXTIR_SIZE_DOCID       sizeof(OOSQL_StorageManager::TupleID)
#define TEXTIR_NPOSITIONOFFSET \
        (TEXTIR_SIZE_LOGICALDOCID + TEXTIR_SIZE_DOCID)
#define TEXTIR_SIZE_FIXEDPOSTINGFIELD \
        (TEXTIR_SIZE_LOGICALDOCID + TEXTIR_SIZE_DOCID + TEXTIR_SIZE_NPOSITION )
#endif

#if     defined(USE_SUBINDEX) && defined(USE_WITHOUT_WEIGHT)
#if     defined(ORDEREDSET_BACKWARD_SCAN)
#define TEXTIR_SIZE_POSTINGLENGTH       0
#else
#define TEXTIR_SIZE_POSTINGLENGTH       sizeof(Four)
#endif
#undef  TEXTIR_SIZE_DOCID
#define TEXTIR_SIZE_DOCID       sizeof(OOSQL_StorageManager::TupleID)
#define TEXTIR_NPOSITIONOFFSET \
        (TEXTIR_SIZE_POSTINGLENGTH + TEXTIR_SIZE_LOGICALDOCID + TEXTIR_SIZE_DOCID)
#define TEXTIR_SIZE_FIXEDPOSTINGFIELD \
        (TEXTIR_SIZE_POSTINGLENGTH + TEXTIR_SIZE_LOGICALDOCID + TEXTIR_SIZE_DOCID + TEXTIR_SIZE_NPOSITION )
#endif

#define TEXTIR_SIZE_MAXPOSITIONLIST \
        ( TEXTIR_MAXPOSTINGSIZE - TEXTIR_SIZE_FIXEDPOSTINGFIELD )
#define TEXTIR_SIZE_SENTENCEPOSITION    sizeof(Four)
#define TEXTIR_SIZE_WORDPOSITION        sizeof(Two)
#define TEXTIR_SIZE_POSITION            \
        ( TEXTIR_SIZE_SENTENCEPOSITION + TEXTIR_SIZE_WORDPOSITION )


/*
 * type definition for Text IR subplan evaluation buffer
 */

class   oosql_TextIR_TempFileInfo : public OOSQL_MemoryManagedObject {
public:
    Two                                 nCols;
    OOSQL_StorageManager::AttrInfo      attrInfo[TEXTIR_NUMFIELDS_IN_POSTING_WITHOUTPOSITION];
    Boolean                             markFlag;
    OOSQL_StorageManager::ColListStruct clist[TEXTIR_NUMFIELDS_IN_POSTING_WITHOUTPOSITION];
    void*                               sortBufferPtr;
    Four                                sortBufferLength;
    Four                                sortBufferCurrentReadOffset;
    OOSQL_SortStream*                   sortStream;

    // constructor
public:
    oosql_TextIR_TempFileInfo();
    ~oosql_TextIR_TempFileInfo() {}
};

class   oosql_TextIR_PostingScanInfo : public OOSQL_MemoryManagedObject {
public:
    Four                        m_osn;
    Four                        m_ocn;
    Four                        m_nPostings;
    Four                        m_postingLengthBufferSize;      
    char*                       m_postingLengthBuffer;          
    char*                       m_currPostingLengthPtr;         
    Four                        m_postingBufferSize;
    char*                       m_postingBuffer;
    char*                       m_currPostingPtr;
    Boolean                     m_wildCharFlag;                 // flag indicating if wild char. is used in the keyword
    Boolean                     m_isContainingTupleID;
    Boolean                     m_isContainingSentenceAndWordNum;
    Boolean                     m_isContainingByteOffset;
    oosql_TextIR_TempFileInfo*  m_tempFileInfo;                 // temporary file to process the wild char. by sorting
    Four                        m_nPostingsOfCurrentKeyword;   
    Four                        m_nObjectsInClass;              
    OOSQL_StorageManager*       m_storageManager;               
    OOSQL_StorageManager::EmbeddedAttrTranslationInfo          
                                m_embeddedAttrTranslationInfo;
    OOSQL_GetEmbeddedAttrsValFuncType                           
                                m_getEmbeddedAttrsValFunc;
    AP_IndexInfoPoolIndex       m_indexInfoPoolIndex;
#ifdef COMPRESSION
    Four						m_uncompressedPostingBufferSize;
    char*						m_uncompressedPostingBuffer;
    Four						m_uncompressedPostingBufferIndex;
#endif

    // constructor
    oosql_TextIR_PostingScanInfo(OOSQL_StorageManager* storageManager, Four nPostings, Boolean isContainingTupleID, Boolean isContainingSentenceAndWordNum, Boolean isContainingByteOffset);
    virtual ~oosql_TextIR_PostingScanInfo();

    // member functions to manipulate posting buffer
    Boolean isEmptyPostingBuffer();
    void    resetPostingBuffer(Four numPostings, Four scanDirection);
    
#ifndef COMPRESSION
    Four    getNextPosting(OOSQL_TextIR_Posting* posting, Four logicalIdHints, Four scanDirection);
    Four    getNextPosting(OOSQL_TextIR_PostingQueue& postingQueue, Four logicalIdHints, Four indexingClassID, Four nPostingsOfCurrentKeyword, Four scanDirection);
#else
    Four    getNextPosting(OOSQL_TextIR_Posting* posting, Four logicalIdHints, Four scanDirection, VolNo volNoOfPostingTupleID, Four *lastDocId);
	Four	getNextPosting(OOSQL_TextIR_PostingQueue& postingQueue, Four logicalIdHints, Four indexingClassID, Four nPostingsOfCurrentKeyword, Four scanDirection, VolNo volNoOfPostingTupleID, Four lastDocId);
#endif
    Four    getNextPostingWithoutDocId(OOSQL_TextIR_Posting* posting);

    Four    doublePostingBuffer();
    Four    setOpenClassNoAndScanNo(Four ocn, Four scanId, AP_IndexInfoPoolIndex indexInfoPoolIndex) { m_ocn = ocn, m_osn = scanId; m_indexInfoPoolIndex = indexInfoPoolIndex; return eNOERROR; }
    Four    setNumOfObjectsInClass(Four ocn);              

    // member function to manipulate wild character
    Boolean isWildCharUsed();
};

inline oosql_TextIR_PostingScanInfo::oosql_TextIR_PostingScanInfo(
    OOSQL_StorageManager*   storageManager,                 // IN  StorageManager Interface
    Four                    nPostings,                      // IN 
    Boolean                 isContainingTupleID,            // IN  
    Boolean                 isContainingSentenceAndWordNum, // IN 
    Boolean                 isContainingByteOffset          // IN  
) 
{
    m_osn                       = NIL;
    m_ocn                       = NIL;
    m_nPostings                 = 0;
    m_currPostingLengthPtr      = NULL;
    m_currPostingPtr            = NULL;
    m_getEmbeddedAttrsValFunc   = NULL;

    memset(&m_embeddedAttrTranslationInfo, 0, sizeof(m_embeddedAttrTranslationInfo));

    if(nPostings == -1 || (nPostings * TEXTIR_AVERAGE_POSTINGLENGTH) >= TEXTIR_MAXPOSTINGBUFFERSIZE)
    {
        m_postingBufferSize       = TEXTIR_MAXPOSTINGBUFFERSIZE;
        m_postingLengthBufferSize = TEXTIR_MAXPOSTINGLENGTHBUFFERSIZE;
    }
    else if((nPostings * TEXTIR_AVERAGE_POSTINGLENGTH) >= (TEXTIR_MAXPOSTINGBUFFERSIZE / 2))
    {
        m_postingBufferSize       = TEXTIR_MAXPOSTINGBUFFERSIZE / 2;
        m_postingLengthBufferSize = TEXTIR_MAXPOSTINGLENGTHBUFFERSIZE / 2;
    }
    else if((nPostings * TEXTIR_AVERAGE_POSTINGLENGTH) >= (TEXTIR_MAXPOSTINGBUFFERSIZE / 4))
    {
        m_postingBufferSize       = TEXTIR_MAXPOSTINGBUFFERSIZE / 4;
        m_postingLengthBufferSize = TEXTIR_MAXPOSTINGLENGTHBUFFERSIZE / 4;
    }
    else if((nPostings * TEXTIR_AVERAGE_POSTINGLENGTH) >= (TEXTIR_MAXPOSTINGBUFFERSIZE / 8))
    {
        m_postingBufferSize       = TEXTIR_MAXPOSTINGBUFFERSIZE / 8;
        m_postingLengthBufferSize = TEXTIR_MAXPOSTINGLENGTHBUFFERSIZE / 8;
    }
    else if((nPostings * TEXTIR_AVERAGE_POSTINGLENGTH) >= (TEXTIR_MAXPOSTINGBUFFERSIZE / 16))
    {
        m_postingBufferSize       = TEXTIR_MAXPOSTINGBUFFERSIZE / 16;
        m_postingLengthBufferSize = TEXTIR_MAXPOSTINGLENGTHBUFFERSIZE / 16;
    }
    else if((nPostings * TEXTIR_AVERAGE_POSTINGLENGTH) >= (TEXTIR_MAXPOSTINGBUFFERSIZE / 32))
    {
        m_postingBufferSize       = TEXTIR_MAXPOSTINGBUFFERSIZE / 32;
        m_postingLengthBufferSize = TEXTIR_MAXPOSTINGLENGTHBUFFERSIZE / 32;
    }
    else
    {
        m_postingBufferSize       = TEXTIR_MINPOSTINGBUFFERSIZE;
        m_postingLengthBufferSize = TEXTIR_MINPOSTINGLENGTHBUFFERSIZE;
    }
        
    m_postingLengthBuffer               = (char *)pMemoryManager->Alloc(m_postingLengthBufferSize);
    m_postingBuffer                     = (char *)pMemoryManager->Alloc(m_postingBufferSize);

#ifdef COMPRESSION 
#ifndef SUPPORT_LARGE_DATABASE2
    m_uncompressedPostingBufferSize     = m_postingBufferSize * 2;
#else
    m_uncompressedPostingBufferSize     = m_postingBufferSize * 4;
#endif
    m_uncompressedPostingBuffer		= (char *)pMemoryManager->Alloc(m_uncompressedPostingBufferSize);
#endif

    m_wildCharFlag                      = SM_FALSE;
    m_tempFileInfo                      = NULL;

    m_isContainingTupleID               = isContainingTupleID;
    m_isContainingSentenceAndWordNum    = isContainingSentenceAndWordNum;
    m_isContainingByteOffset            = isContainingByteOffset;

    m_storageManager                    = storageManager;
}

inline oosql_TextIR_PostingScanInfo::~oosql_TextIR_PostingScanInfo()
{
    if(m_postingLengthBuffer)
        pMemoryManager->Free(m_postingLengthBuffer);
    if(m_postingBuffer)
        pMemoryManager->Free(m_postingBuffer);
#ifdef COMPRESSION 
	if(m_uncompressedPostingBuffer)
		pMemoryManager->Free(m_uncompressedPostingBuffer);
#endif
}

inline Boolean oosql_TextIR_PostingScanInfo::isEmptyPostingBuffer()
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
    if (m_nPostings < 1) 
        return SM_TRUE;
    else
        return SM_FALSE;
}

inline void oosql_TextIR_PostingScanInfo::resetPostingBuffer(
    Four    numPostings,                    // IN
    Four    scanDirection                   // IN
)
{
    m_nPostings      = numPostings;

#ifdef COMPRESSION	
	m_uncompressedPostingBufferIndex = 0;
#endif	

    if (scanDirection == TEXTIR_SCAN_FORWARD)
    {
        m_currPostingLengthPtr = m_postingLengthBuffer;
        m_currPostingPtr       = m_postingBuffer;
    }
    else
    {
        Four lengthOfLastPosting;

        m_currPostingLengthPtr = m_postingLengthBuffer + m_postingLengthBufferSize - sizeof(Four);
        memcpy((char *)&lengthOfLastPosting, m_currPostingLengthPtr, sizeof(Four));
        
        m_currPostingPtr = m_postingBuffer + m_postingBufferSize - lengthOfLastPosting;
    }
}

inline Four oosql_TextIR_PostingScanInfo::getNextPosting(
    OOSQL_TextIR_Posting    *posting,           // OUT
    Four                    logicalIdHints,     // IN 
#ifndef COMPRESSION    
    Four                    scanDirection       // IN : (TEXTIR_SCAN_FORWARD | TEXTIR_SCAN_BACKWARD_NOORDERING)
#else	
    Four					scanDirection,			// IN : (TEXTIR_SCAN_FORWARD | TEXTIR_SCAN_BACKWARD_NOORDERING)
	VolNo					volNoOfPostingTupleID,		
	Four				    *lastDocId
#endif		
)
{
    Four    currPostingSize;
    Four    prevPostingSize;
    Four    offset = 0;
    Four    postingLength = 0;
    Four    e;

#ifdef COMPRESSION     
    Four    docIdGap; 
    Four	oldDocIdGap;
    Four    sentencePos;
    Four    sentencePosGap;
    Two     wordPos;
    Four    inIndex;
    Four    i;
    Four    count;
    char    *uncompressPositionBuffer;
    OOSQL_StorageManager::OID   *oid;    
    
	oldDocIdGap				 = 0;
	uncompressPositionBuffer = m_uncompressedPostingBuffer +  m_uncompressedPostingBufferIndex;
#endif

    /* check input parameter */
#ifdef  OOSQL_DEBUG
    if (posting == NULL) OOSQL_ERR(eBADPARAMETER_OOSQL);
#endif

    while(1)
    {
        /* check if posting buffer is not empty */
        if (isEmptyPostingBuffer() == SM_TRUE) 
        {
            TEXTIR_SET_NULLPOSTING(posting);
            return ePOSTINGBUFFER_EMPTY_OOSQL;
        }

#ifndef COMPRESSION

#if     !defined(USE_SUBINDEX) && !defined(USE_WITHOUT_WEIGHT)
        /* copy the current posting to 'posting' array */
        memcpy(posting, m_currPostingPtr, TEXTIR_SIZE_FIXEDPOSTINGFIELD );
        posting->positions = m_currPostingPtr + TEXTIR_SIZE_FIXEDPOSTINGFIELD;

        /* calculate the size of the current posting */
        memcpy( &nPositions, (m_currPostingPtr + TEXTIR_NPOSITIONOFFSET), TEXTIR_SIZE_NPOSITION );
        currPostingSize = TEXTIR_SIZE_LOGICALDOCID + TEXTIR_SIZE_DOCID + 
                          TEXTIR_SIZE_WEIGHT + TEXTIR_SIZE_NPOSITION +
                          ( nPositions * TEXTIR_SIZE_POSITION );
#else
#ifdef  USE_SUBINDEX
#if defined (ORDEREDSET_BACKWARD_SCAN)
        memcpy(&postingLength, m_currPostingLengthPtr, sizeof(Four));
#else
        memcpy(&postingLength, m_currPostingPtr + offset, sizeof(Four));
        offset += TEXTIR_SIZE_POSTINGLENGTH;    // skip POSTINGLENGTH
#endif  /* #if defined (ORDEREDSET_BACKWARD_SCAN) */
#endif

        memcpy(&posting->logicalDocId, m_currPostingPtr + offset, TEXTIR_SIZE_LOGICALDOCID);
        offset += TEXTIR_SIZE_LOGICALDOCID;

        if(m_isContainingTupleID)
        {
            memcpy(&posting->docId, m_currPostingPtr + offset, TEXTIR_SIZE_DOCID);
            offset += TEXTIR_SIZE_DOCID;
        }
        else
        {
            e = m_storageManager->Text_GetOIDFromLogicalDocId(m_ocn, posting->logicalDocId, &posting->docId);
            OOSQL_CHECK_ERR(e);
        }

#ifndef USE_WITHOUT_WEIGHT
        memcpy(&posting->weight, m_currPostingPtr + offset, TEXTIR_SIZE_WEIGHT);
        offset += TEXTIR_SIZE_WEIGHT;
#endif

        // read nPositions
        memcpy(&posting->nPositions, m_currPostingPtr + offset, TEXTIR_SIZE_NPOSITION);
        offset += TEXTIR_SIZE_NPOSITION;

        posting->positions                   = m_currPostingPtr + offset;
        posting->ptrToEmbeddedAttrsBuf       = posting->positions;
        posting->embeddedAttrsBufSize        = sizeof(postingLength) + postingLength - offset;
        posting->scanId                      = m_osn;
        posting->embeddedAttrTranslationInfo = &m_embeddedAttrTranslationInfo;
        posting->getEmbeddedAttrsValFuncPtr  = &m_getEmbeddedAttrsValFunc;
        posting->indexInfoPoolIndex          = m_indexInfoPoolIndex;

        if(m_isContainingSentenceAndWordNum)
        {
            // TEXTIR_SIZE_POSITION = sentense offset + word offset
            posting->ptrToEmbeddedAttrsBuf += (posting->nPositions * TEXTIR_SIZE_POSITION);
            posting->embeddedAttrsBufSize  -= (posting->nPositions * TEXTIR_SIZE_POSITION);
        }

        if(m_isContainingByteOffset)
        {
            posting->ptrToEmbeddedAttrsBuf += (posting->nPositions * sizeof(Four));
            posting->embeddedAttrsBufSize  -= (posting->nPositions * sizeof(Four));
        }

        if(!m_isContainingSentenceAndWordNum || m_isContainingByteOffset)
            OOSQL_ERR(eNOTIMPLEMENTED_OOSQL);

#ifdef USE_SUBINDEX
#if defined (ORDEREDSET_BACKWARD_SCAN)
        currPostingSize = postingLength;
#else
        currPostingSize = sizeof(Four) + postingLength;
#endif  /* #if defined (ORDEREDSET_BACKWARD_SCAN) */
#else
        currPostingSize = offset + (nPositions * TEXTIR_SIZE_POSITION);
#endif  /* #ifdef USE_SUBINDEX */

#endif  /* #if !defined(USE_SUBINDEX) && !defined(USE_WITHOUT_WEIGHT) */

        /* move to the next posting */
        m_nPostings --;
        if(scanDirection == TEXTIR_SCAN_FORWARD)
        {
#if defined (ORDEREDSET_BACKWARD_SCAN)
            m_currPostingLengthPtr += sizeof(Four);
#endif  /* #if defined (ORDEREDSET_BACKWARD_SCAN) */
            m_currPostingPtr += currPostingSize;
        }
        else
        {
            m_currPostingLengthPtr -= sizeof(Four);
            memcpy(&prevPostingSize, m_currPostingLengthPtr, sizeof(Four));
            m_currPostingPtr -= prevPostingSize;
        }

        if(scanDirection == TEXTIR_SCAN_FORWARD)
        {
            if(logicalIdHints == TEXTIR_POSTINGSKIP_NO_HINTS)
                break;
            else if(logicalIdHints <= posting->logicalDocId)
                break;
        }
        else
        {
            if(logicalIdHints == TEXTIR_POSTINGSKIP_NO_HINTS)
                break;
            else if(logicalIdHints >= posting->logicalDocId)
                break;
        }

        offset = 0;
		
#else   /* COMPRESSION */

    	inIndex = 0;
    	oid		= &posting->docId;
    	
		memcpy(&postingLength, m_currPostingLengthPtr, sizeof(Four));
        
		LOM_VARIABLE_BYTE_DECODING(m_currPostingPtr, inIndex, docIdGap);
		if(scanDirection == TEXTIR_SCAN_FORWARD)
		{
			*lastDocId += docIdGap;
		}
		else
		{
			*lastDocId -= oldDocIdGap;
        	oldDocIdGap = docIdGap;
		}
       	posting->logicalDocId = *lastDocId;
		if(m_isContainingTupleID)
		{
			LOM_VARIABLE_BYTE_DECODING(m_currPostingPtr, inIndex, oid->pageNo);
			oid->volNo = volNoOfPostingTupleID;
			LOM_VARIABLE_BYTE_DECODING(m_currPostingPtr, inIndex, oid->slotNo);
			LOM_VARIABLE_BYTE_DECODING(m_currPostingPtr, inIndex, oid->unique);
        }
		else
		{
			e = m_storageManager->Text_GetOIDFromLogicalDocId(m_ocn, posting->logicalDocId, oid);
			OOSQL_CHECK_ERR(e);
		}

		LOM_VARIABLE_BYTE_DECODING(m_currPostingPtr, inIndex, count);
		posting->nPositions = count;
		
        posting->positions = uncompressPositionBuffer;
        sentencePos = 0;
        
        m_uncompressedPostingBufferIndex += (sizeof(sentencePos) + sizeof(wordPos))*count;
        if(m_uncompressedPostingBufferIndex > m_uncompressedPostingBufferSize) 
        {
			LOM_ERROR(handle, eBIGGERPOSTINGBUFFERNEEDED_LOM);  
		}
		
		for(i = 0; i < count; i++)
        {
			LOM_VARIABLE_BYTE_DECODING(m_currPostingPtr, inIndex, sentencePosGap);
            sentencePos += sentencePosGap;

			LOM_VARIABLE_BYTE_DECODING(m_currPostingPtr, inIndex, wordPos);
            
            memcpy(uncompressPositionBuffer, &sentencePos, sizeof(sentencePos));
	        uncompressPositionBuffer += sizeof(sentencePos); 

			memcpy(uncompressPositionBuffer, &wordPos, sizeof(wordPos));
			uncompressPositionBuffer += sizeof(wordPos);
		}
		
		posting->ptrToEmbeddedAttrsBuf		 = m_currPostingPtr + inIndex;
        posting->embeddedAttrsBufSize        = postingLength - inIndex;

		posting->scanId						 = m_osn;
		posting->embeddedAttrTranslationInfo = &m_embeddedAttrTranslationInfo;
		posting->getEmbeddedAttrsValFuncPtr  = &m_getEmbeddedAttrsValFunc;
		posting->indexInfoPoolIndex			 = m_indexInfoPoolIndex;

		/* move to the next posting */
		m_nPostings --;
		if(scanDirection == TEXTIR_SCAN_FORWARD)
		{
			m_currPostingLengthPtr += sizeof(Four);
			m_currPostingPtr += postingLength;
			
			if(logicalIdHints == TEXTIR_POSTINGSKIP_NO_HINTS)
				break;
			else if(logicalIdHints <= posting->logicalDocId)
				break;
    }
		else
		{
			m_currPostingLengthPtr -= sizeof(Four);
			memcpy(&prevPostingSize, m_currPostingLengthPtr, sizeof(Four));
			m_currPostingPtr -= prevPostingSize;
			
			if(logicalIdHints == TEXTIR_POSTINGSKIP_NO_HINTS)
				break;
			else if(logicalIdHints >= posting->logicalDocId)
				break;
		}
#endif  /* COMPRESSION */
	}

    /* return */
    return eNOERROR;
}

inline Four oosql_TextIR_PostingScanInfo::getNextPosting(
    OOSQL_TextIR_PostingQueue&  postingQueue,               // INOUT
    Four                        logicalIdHints,             // IN  
    Four                        indexingClassID,            // IN   
    Four                        nPostingsOfCurrentKeyword,  // IN  
#ifndef COMPRESSION    
    Four                        scanDirection               // IN   : (TEXTIR_SCAN_FORWARD | TEXTIR_SCAN_BACKWARD_NOORDERING)
#else	
    Four						scanDirection,				// IN   : (TEXTIR_SCAN_FORWARD | TEXTIR_SCAN_BACKWARD_NOORDERING)
	VolNo						volNoOfPostingTupleID,		
	Four				        lastDocId
#endif		
)
{
    Four                    nPositions;
    Four                    currPostingSize;
    Four                    prevPostingSize;
    Four                    offset;
    Four                    postingLength = 0;
    Four                    e;
    Four                    logicalDocId;
    char                    currPostingPtr;
    Four                    logicalDocIdOffset;
    Four                    nPositionsOffset;
    OOSQL_TextIR_Posting    *posting;
    Four                    nElementsHasBeenRead;
    double                  nPostingsPerObject;         
	Four    				docIdGap; 
	Four					oldDocIdGap;
    Four    				sentencePos;
    Four    				sentencePosGap;
    Two     				wordPos;
    Four					byteOffset;
    Four					byteOffsetGap;
    Four    				inIndex;
    Four    				i;
    Four                    count;
    char                    *uncompressPositionBuffer;
    OOSQL_StorageManager::OID   *oid;

    nElementsHasBeenRead = 0;

    logicalDocIdOffset  = 0;
    nPositionsOffset    = 0;

    nPostingsPerObject = (double)nPostingsOfCurrentKeyword / (double)m_nObjectsInClass;

#ifndef COMPRESSION

#ifdef  USE_SUBINDEX
#if !defined (ORDEREDSET_BACKWARD_SCAN)
    logicalDocIdOffset += TEXTIR_SIZE_POSTINGLENGTH;
    nPositionsOffset   += TEXTIR_SIZE_POSTINGLENGTH;
#endif  /* #if !defined (ORDEREDSET_BACKWARD_SCAN) */
#endif

    nPositionsOffset += TEXTIR_SIZE_LOGICALDOCID;

    if(m_isContainingTupleID)
        nPositionsOffset += TEXTIR_SIZE_DOCID;

#ifndef USE_WITHOUT_WEIGHT
    nPositionsOffset += TEXTIR_SIZE_WEIGHT;
#endif

    if(logicalIdHints != TEXTIR_POSTINGSKIP_NO_HINTS)
    {
        while(1)
        {
            /* check if posting buffer is not empty */
            if (isEmptyPostingBuffer() == SM_TRUE) 
                return ePOSTINGBUFFER_EMPTY_OOSQL;

#if defined (ORDEREDSET_BACKWARD_SCAN)
            memcpy(&postingLength, m_currPostingLengthPtr, sizeof(Four));
#else
            memcpy(&postingLength, m_currPostingPtr, sizeof(Four));
#endif  /* #if defined (ORDEREDSET_BACKWARD_SCAN) */
            memcpy(&logicalDocId,  m_currPostingPtr + logicalDocIdOffset, TEXTIR_SIZE_LOGICALDOCID);
            memcpy(&nPositions,    m_currPostingPtr + nPositionsOffset, TEXTIR_SIZE_NPOSITION);

#ifdef USE_SUBINDEX
#if defined (ORDEREDSET_BACKWARD_SCAN)
            currPostingSize = postingLength;
#else
            currPostingSize = sizeof(Four) + postingLength;
#endif  /* #if defined (ORDEREDSET_BACKWARD_SCAN) */
#else
            if(m_isContainingSentenceAndWordNum)
                currPostingSize = nPositionsOffset + (nPositions * TEXTIR_SIZE_POSITION);
            else if(m_isContainingByteOffset)
                currPostingSize = nPositionsOffset + (nPositions * sizeof(Four));
            else
                currPostingSize = nPositionsOffset;
#endif

            // move next posting
            if(scanDirection == TEXTIR_SCAN_FORWARD)
            {
                if(logicalIdHints <= logicalDocId) break;

#if defined (ORDEREDSET_BACKWARD_SCAN)
                m_currPostingLengthPtr += sizeof(Four);
#endif  /* #if defined (ORDEREDSET_BACKWARD_SCAN) */
                m_currPostingPtr += currPostingSize;
            }
            else
            {
                if(logicalIdHints >= logicalDocId) break;

                m_currPostingLengthPtr -= sizeof(Four);
                memcpy(&prevPostingSize, m_currPostingLengthPtr, sizeof(Four));
                m_currPostingPtr -= prevPostingSize;
            }
            m_nPostings --;
        }
    }

#else   /* COMPRESSION */

    
	if(logicalIdHints != TEXTIR_POSTINGSKIP_NO_HINTS)
	{
	    logicalDocId    = lastDocId;	
		oldDocIdGap		= 0;
		
		while(1)
		{
			/* check if posting buffer is not empty */
			if (isEmptyPostingBuffer() == SM_TRUE) 
				return ePOSTINGBUFFER_EMPTY_OOSQL;

			memcpy(&postingLength, m_currPostingLengthPtr, sizeof(Four));
			
			inIndex = 0;
            LOM_VARIABLE_BYTE_DECODING(m_currPostingPtr, inIndex, docIdGap);
            
			// move next posting
			if(scanDirection == TEXTIR_SCAN_FORWARD)
			{
				logicalDocId += docIdGap;
				if(logicalIdHints <= logicalDocId) break;

				m_currPostingLengthPtr += sizeof(Four);
				m_currPostingPtr += postingLength;
			}
			else
			{
				logicalDocId -= oldDocIdGap;
            	oldDocIdGap = docIdGap;
            	
				if(logicalIdHints >= logicalDocId) break;

				m_currPostingLengthPtr -= sizeof(Four);
				memcpy(&prevPostingSize, m_currPostingLengthPtr, sizeof(Four));
				m_currPostingPtr -= prevPostingSize;
			}
			m_nPostings --;
		}
	}
	
#endif  /* COMPRESSION */

#if !defined(USE_SUBINDEX) && !defined(USE_WITHOUT_WEIGHT)
    while(isEmptyPostingBuffer() == SM_FALSE)
    {
        offset = 0;

        postingQueue.PushTail();
        posting = &postingQueue.Tail();

        /* copy the current posting to 'posting' array */
        memcpy(posting, m_currPostingPtr, TEXTIR_SIZE_FIXEDPOSTINGFIELD);
        posting->positions = m_currPostingPtr + TEXTIR_SIZE_FIXEDPOSTINGFIELD;

        /* calculate the size of the current posting */
        memcpy(&nPositions, (m_currPostingPtr + TEXTIR_NPOSITIONOFFSET), TEXTIR_SIZE_NPOSITION);
        if(m_isContainingSentenceAndWordNum))
            currPostingSize = TEXTIR_SIZE_LOGICALDOCID + TEXTIR_SIZE_DOCID + 
                              TEXTIR_SIZE_WEIGHT + TEXTIR_SIZE_NPOSITION +
                              ( nPositions * TEXTIR_SIZE_POSITION );
        else if(m_isContainingByteOffset)
            currPostingSize = TEXTIR_SIZE_LOGICALDOCID + TEXTIR_SIZE_DOCID + 
                              TEXTIR_SIZE_WEIGHT + TEXTIR_SIZE_NPOSITION +
                              ( nPositions * sizeof(Four) );
        else
            currPostingSize = TEXTIR_SIZE_LOGICALDOCID + TEXTIR_SIZE_DOCID + 
                              TEXTIR_SIZE_WEIGHT + TEXTIR_SIZE_NPOSITION;

        /* move to the next posting */
        m_nPostings --;
        m_currPostingPtr += currPostingSize;
        nElementsHasBeenRead ++;
    }
#else // defined(USE_SUBINDEX) || defined(USE_WITHOUT_WEIGHT)

#if defined(USE_SUBINDEX) && defined(USE_WITHOUT_WEIGHT)

#ifndef COMPRESSION
    if(m_isContainingTupleID && m_isContainingSentenceAndWordNum && !m_isContainingByteOffset)
    {
        while(isEmptyPostingBuffer() == SM_FALSE)
        {
            offset = 0;

            postingQueue.PushTail();
            posting = &postingQueue.Tail();

#if defined (ORDEREDSET_BACKWARD_SCAN)
            memcpy(&postingLength, m_currPostingLengthPtr, sizeof(Four));
#else
            memcpy(&postingLength, m_currPostingPtr + offset, sizeof(Four));
            offset += TEXTIR_SIZE_POSTINGLENGTH;    // skip POSTINGLENGTH
#endif  /* #if defined (ORDEREDSET_BACKWARD_SCAN) */

            memcpy(&posting->logicalDocId, m_currPostingPtr + offset, TEXTIR_SIZE_LOGICALDOCID + TEXTIR_SIZE_DOCID);
            offset += (TEXTIR_SIZE_LOGICALDOCID + TEXTIR_SIZE_DOCID);

            memcpy(&posting->nPositions, m_currPostingPtr + offset, TEXTIR_SIZE_NPOSITION);
            offset += TEXTIR_SIZE_NPOSITION;
            
#if defined (EXTENDED_BOOLEAN_MODEL)
            posting->weight = OOSQL_StorageManager::CalculateWeight(posting->nPositions, nPostingsOfCurrentKeyword);
#elif defined (HEURISTIC_MODEL)
            posting->weight = OOSQL_StorageManager::CalculateWeight(posting->nPositions, nPostingsPerObject);
            posting->bonus  = HEURISTIC_BOUNS_WEIGHT_VALUE;
#endif

            posting->docId.classID               = indexingClassID;
            posting->positions                   = m_currPostingPtr + offset;
            posting->ptrToEmbeddedAttrsBuf       = posting->positions + (posting->nPositions * TEXTIR_SIZE_POSITION);
            posting->embeddedAttrsBufSize        = sizeof(postingLength) + postingLength - offset - (posting->nPositions * TEXTIR_SIZE_POSITION);
            posting->scanId                      = m_osn;
            posting->embeddedAttrTranslationInfo = &m_embeddedAttrTranslationInfo;
            posting->getEmbeddedAttrsValFuncPtr  = &m_getEmbeddedAttrsValFunc;
            posting->indexInfoPoolIndex          = m_indexInfoPoolIndex;

            /* move to the next posting */
            m_nPostings --;
            if(scanDirection == TEXTIR_SCAN_FORWARD)
            {
#if defined (ORDEREDSET_BACKWARD_SCAN)
                m_currPostingLengthPtr += sizeof(Four);
                m_currPostingPtr += postingLength;
#else
                m_currPostingPtr += sizeof(Four) + postingLength;
#endif  /* #if defined (ORDEREDSET_BACKWARD_SCAN) */
            }
            else
            {
                m_currPostingLengthPtr -= sizeof(Four);
                memcpy(&prevPostingSize, m_currPostingLengthPtr, sizeof(Four));
                m_currPostingPtr -= prevPostingSize;
            }
            nElementsHasBeenRead ++;

            if(isEmptyPostingBuffer())
                break;

            offset = 0;

            postingQueue.PushTail();
            posting = &postingQueue.Tail();
            
#if defined (ORDEREDSET_BACKWARD_SCAN)
            memcpy(&postingLength, m_currPostingLengthPtr, sizeof(Four));
#else
            memcpy(&postingLength, m_currPostingPtr + offset, sizeof(Four));
            offset += TEXTIR_SIZE_POSTINGLENGTH;    // skip POSTINGLENGTH
#endif  /* #if defined (ORDEREDSET_BACKWARD_SCAN) */

            memcpy(&posting->logicalDocId, m_currPostingPtr + offset, TEXTIR_SIZE_LOGICALDOCID + TEXTIR_SIZE_DOCID);
            offset += TEXTIR_SIZE_LOGICALDOCID + TEXTIR_SIZE_DOCID;

            memcpy(&posting->nPositions, m_currPostingPtr + offset, TEXTIR_SIZE_NPOSITION);
            offset += TEXTIR_SIZE_NPOSITION;

#if defined (EXTENDED_BOOLEAN_MODEL)
            posting->weight = OOSQL_StorageManager::CalculateWeight(posting->nPositions, nPostingsOfCurrentKeyword);
#elif defined (HEURISTIC_MODEL)
            posting->weight = OOSQL_StorageManager::CalculateWeight(posting->nPositions, nPostingsPerObject);
            posting->bonus  = HEURISTIC_BOUNS_WEIGHT_VALUE;
#endif

            posting->docId.classID               = indexingClassID;
            posting->positions                   = m_currPostingPtr + offset;
            posting->ptrToEmbeddedAttrsBuf       = posting->positions + (posting->nPositions * TEXTIR_SIZE_POSITION);
            posting->embeddedAttrsBufSize        = sizeof(postingLength) + postingLength - offset - (posting->nPositions * TEXTIR_SIZE_POSITION);
            posting->scanId                      = m_osn;
            posting->embeddedAttrTranslationInfo = &m_embeddedAttrTranslationInfo;
            posting->getEmbeddedAttrsValFuncPtr  = &m_getEmbeddedAttrsValFunc;
            posting->indexInfoPoolIndex          = m_indexInfoPoolIndex;

            /* move to the next posting */
            m_nPostings --;
            if(scanDirection == TEXTIR_SCAN_FORWARD)
            {
#if defined (ORDEREDSET_BACKWARD_SCAN)
                m_currPostingLengthPtr += sizeof(Four);
                m_currPostingPtr += postingLength;
#else
                m_currPostingPtr += sizeof(Four) + postingLength;
#endif  /* #if defined (ORDEREDSET_BACKWARD_SCAN) */
            }
            else
            {
                m_currPostingLengthPtr -= sizeof(Four);
                memcpy(&prevPostingSize, m_currPostingLengthPtr, sizeof(Four));
                m_currPostingPtr -= prevPostingSize;
            }
            nElementsHasBeenRead ++;
        }
    }
    else if(m_isContainingTupleID && !m_isContainingSentenceAndWordNum && !m_isContainingByteOffset)
    {
        while(isEmptyPostingBuffer() == SM_FALSE)
        {
            offset = 0;

            postingQueue.PushTail();
            posting = &postingQueue.Tail();

#if defined (ORDEREDSET_BACKWARD_SCAN)
            memcpy(&postingLength, m_currPostingLengthPtr, sizeof(Four));
#else
            memcpy(&postingLength, m_currPostingPtr + offset, sizeof(Four));
            offset += TEXTIR_SIZE_POSTINGLENGTH;    // skip POSTINGLENGTH
#endif  /* #if defined (ORDEREDSET_BACKWARD_SCAN) */

            memcpy(&posting->logicalDocId, m_currPostingPtr + offset, TEXTIR_SIZE_LOGICALDOCID + TEXTIR_SIZE_DOCID);
            offset += TEXTIR_SIZE_LOGICALDOCID + TEXTIR_SIZE_DOCID;

            memcpy(&posting->nPositions, m_currPostingPtr + offset, TEXTIR_SIZE_NPOSITION);
            offset += TEXTIR_SIZE_NPOSITION;
            
#if defined (EXTENDED_BOOLEAN_MODEL)
            posting->weight = OOSQL_StorageManager::CalculateWeight(posting->nPositions, nPostingsOfCurrentKeyword);
#elif defined (HEURISTIC_MODEL)
            posting->weight = OOSQL_StorageManager::CalculateWeight(posting->nPositions, nPostingsPerObject);
            posting->bonus  = HEURISTIC_BOUNS_WEIGHT_VALUE;
#endif

            posting->docId.classID               = indexingClassID;
            posting->positions                   = m_currPostingPtr + offset;
            posting->ptrToEmbeddedAttrsBuf       = posting->positions;
            posting->embeddedAttrsBufSize        = sizeof(postingLength) + postingLength - offset;
            posting->scanId                      = m_osn;
            posting->embeddedAttrTranslationInfo = &m_embeddedAttrTranslationInfo;
            posting->getEmbeddedAttrsValFuncPtr  = &m_getEmbeddedAttrsValFunc;
            posting->indexInfoPoolIndex          = m_indexInfoPoolIndex;

            /* move to the next posting */
            m_nPostings --;
            if(scanDirection == TEXTIR_SCAN_FORWARD)
            {
#if defined (ORDEREDSET_BACKWARD_SCAN)
                m_currPostingLengthPtr += sizeof(Four);
                m_currPostingPtr += postingLength;
#else
                m_currPostingPtr += sizeof(Four) + postingLength;
#endif  /* #if defined (ORDEREDSET_BACKWARD_SCAN) */
            }
            else
            {
                m_currPostingLengthPtr -= sizeof(Four);
                memcpy(&prevPostingSize, m_currPostingLengthPtr, sizeof(Four));
                m_currPostingPtr -= prevPostingSize;
            }
            nElementsHasBeenRead ++;

            if(isEmptyPostingBuffer())
                break;
            
            offset = 0;

            postingQueue.PushTail();
            posting = &postingQueue.Tail();

#if defined (ORDEREDSET_BACKWARD_SCAN)
            memcpy(&postingLength, m_currPostingLengthPtr, sizeof(Four));
#else
            memcpy(&postingLength, m_currPostingPtr + offset, sizeof(Four));
            offset += TEXTIR_SIZE_POSTINGLENGTH;    // skip POSTINGLENGTH
#endif  /* #if defined (ORDEREDSET_BACKWARD_SCAN) */

            memcpy(&posting->logicalDocId, m_currPostingPtr + offset, TEXTIR_SIZE_LOGICALDOCID + TEXTIR_SIZE_DOCID);
            offset += TEXTIR_SIZE_LOGICALDOCID + TEXTIR_SIZE_DOCID;

            memcpy(&posting->nPositions, m_currPostingPtr + offset, TEXTIR_SIZE_NPOSITION);
            offset += TEXTIR_SIZE_NPOSITION;

#if defined (EXTENDED_BOOLEAN_MODEL)
            posting->weight = OOSQL_StorageManager::CalculateWeight(posting->nPositions, nPostingsOfCurrentKeyword);
#elif defined (HEURISTIC_MODEL)
            posting->weight = OOSQL_StorageManager::CalculateWeight(posting->nPositions, nPostingsPerObject);
            posting->bonus  = HEURISTIC_BOUNS_WEIGHT_VALUE;
#endif

            posting->docId.classID               = indexingClassID;
            posting->positions                   = m_currPostingPtr + offset;
            posting->ptrToEmbeddedAttrsBuf       = posting->positions;
            posting->embeddedAttrsBufSize        = sizeof(postingLength) + postingLength - offset;
            posting->scanId                      = m_osn;
            posting->embeddedAttrTranslationInfo = &m_embeddedAttrTranslationInfo;
            posting->getEmbeddedAttrsValFuncPtr  = &m_getEmbeddedAttrsValFunc;
            posting->indexInfoPoolIndex          = m_indexInfoPoolIndex;

            /* move to the next posting */
            m_nPostings --;
            if(scanDirection == TEXTIR_SCAN_FORWARD)
            {
#if defined (ORDEREDSET_BACKWARD_SCAN)
                m_currPostingLengthPtr += sizeof(Four);
                m_currPostingPtr += postingLength;
#else
                m_currPostingPtr += sizeof(Four) + postingLength;
#endif  /* #if defined (ORDEREDSET_BACKWARD_SCAN) */
            }
            else
            {
                m_currPostingLengthPtr -= sizeof(Four);
                memcpy(&prevPostingSize, m_currPostingLengthPtr, sizeof(Four));
                m_currPostingPtr -= prevPostingSize;
            }
            nElementsHasBeenRead ++;
        }
    }
    else
    {
        while(isEmptyPostingBuffer() == SM_FALSE)
        {
            offset = 0;

            postingQueue.PushTail();
            posting = &postingQueue.Tail();

#if defined (ORDEREDSET_BACKWARD_SCAN)
            memcpy(&postingLength, m_currPostingLengthPtr, sizeof(Four));
#else
            memcpy(&postingLength, m_currPostingPtr + offset, sizeof(Four));
            offset += TEXTIR_SIZE_POSTINGLENGTH;    // skip POSTINGLENGTH
#endif  /* #if defined (ORDEREDSET_BACKWARD_SCAN) */
        
            memcpy(&posting->logicalDocId, m_currPostingPtr + offset, TEXTIR_SIZE_LOGICALDOCID);
            offset += TEXTIR_SIZE_LOGICALDOCID;

            if(m_isContainingTupleID)
            {
                memcpy(&posting->docId, m_currPostingPtr + offset, TEXTIR_SIZE_DOCID);
                offset += TEXTIR_SIZE_DOCID;
            }
            else
            {
                e = m_storageManager->Text_GetOIDFromLogicalDocId(m_ocn, posting->logicalDocId, &posting->docId);
                OOSQL_CHECK_ERR(e);
            }
            posting->docId.classID = indexingClassID;

            // get nPositions
            memcpy(&posting->nPositions, m_currPostingPtr + offset, TEXTIR_SIZE_NPOSITION);
            offset += TEXTIR_SIZE_NPOSITION;

#if defined (EXTENDED_BOOLEAN_MODEL)
            posting->weight = OOSQL_StorageManager::CalculateWeight(posting->nPositions, nPostingsOfCurrentKeyword);
#elif defined (HEURISTIC_MODEL)
            posting->weight = OOSQL_StorageManager::CalculateWeight(posting->nPositions, nPostingsPerObject);
            posting->bonus  = HEURISTIC_BOUNS_WEIGHT_VALUE;
#endif

            posting->positions                   = m_currPostingPtr + offset;
            posting->ptrToEmbeddedAttrsBuf       = posting->positions;
            posting->embeddedAttrsBufSize        = sizeof(postingLength) + postingLength - offset;
            posting->scanId                      = m_osn;
            posting->embeddedAttrTranslationInfo = &m_embeddedAttrTranslationInfo;
            posting->getEmbeddedAttrsValFuncPtr  = &m_getEmbeddedAttrsValFunc;
            posting->indexInfoPoolIndex          = m_indexInfoPoolIndex;

            if(m_isContainingSentenceAndWordNum)
            {
                // TEXTIR_SIZE_POSITION = sentense offset + word offset
                posting->ptrToEmbeddedAttrsBuf += (posting->nPositions * TEXTIR_SIZE_POSITION);
                posting->embeddedAttrsBufSize  -= (posting->nPositions * TEXTIR_SIZE_POSITION);
            }

            if(m_isContainingByteOffset)
            {
                posting->ptrToEmbeddedAttrsBuf += (posting->nPositions * sizeof(Four));
                posting->embeddedAttrsBufSize  -= (posting->nPositions * sizeof(Four));
            }

            if(!m_isContainingSentenceAndWordNum || m_isContainingByteOffset)
                OOSQL_ERR(eNOTIMPLEMENTED_OOSQL);

            /* move to the next posting */
            m_nPostings --;
            if(scanDirection == TEXTIR_SCAN_FORWARD)
            {
#if defined (ORDEREDSET_BACKWARD_SCAN)
                m_currPostingLengthPtr += sizeof(Four);
                m_currPostingPtr += postingLength;
#else
                m_currPostingPtr += sizeof(Four) + postingLength;
#endif  /* #if defined (ORDEREDSET_BACKWARD_SCAN) */
            }
            else
            {
                m_currPostingLengthPtr -= sizeof(Four);
                memcpy(&prevPostingSize, m_currPostingLengthPtr, sizeof(Four));
                m_currPostingPtr -= prevPostingSize;
            }
            nElementsHasBeenRead ++;
        }
    }

#else   /* COMPRESSION */
    
	logicalDocId             = lastDocId;
	oldDocIdGap				 = 0;
	uncompressPositionBuffer = m_uncompressedPostingBuffer +  m_uncompressedPostingBufferIndex;
	
	if(m_isContainingTupleID && m_isContainingSentenceAndWordNum && !m_isContainingByteOffset)
	{
		while(isEmptyPostingBuffer() == SM_FALSE)
		{
		    inIndex = 0;
			postingQueue.PushTail();
			posting = &postingQueue.Tail();
            oid = &posting->docId;
            
			memcpy(&postingLength, m_currPostingLengthPtr, sizeof(Four));

    		LOM_VARIABLE_BYTE_DECODING(m_currPostingPtr, inIndex, docIdGap);
			
			LOM_VARIABLE_BYTE_DECODING(m_currPostingPtr, inIndex, oid->pageNo);
			oid->volNo = volNoOfPostingTupleID;
			LOM_VARIABLE_BYTE_DECODING(m_currPostingPtr, inIndex, oid->slotNo);
			LOM_VARIABLE_BYTE_DECODING(m_currPostingPtr, inIndex, oid->unique);
            oid->classID = indexingClassID;
            
			LOM_VARIABLE_BYTE_DECODING(m_currPostingPtr, inIndex, count);
			posting->nPositions = count;
			
#if defined (EXTENDED_BOOLEAN_MODEL)
			posting->weight = OOSQL_StorageManager::CalculateWeight(posting->nPositions, nPostingsOfCurrentKeyword);
#elif defined (HEURISTIC_MODEL)
			posting->weight = OOSQL_StorageManager::CalculateWeight(posting->nPositions, nPostingsPerObject);
			posting->bonus  = HEURISTIC_BOUNS_WEIGHT_VALUE;
#endif
            
            posting->positions = uncompressPositionBuffer;
            sentencePos = 0;
            
            m_uncompressedPostingBufferIndex += (sizeof(sentencePos) + sizeof(wordPos))*count;
            if(m_uncompressedPostingBufferIndex > m_uncompressedPostingBufferSize) 
            {
				LOM_ERROR(handle, eBIGGERPOSTINGBUFFERNEEDED_LOM);  
			}
				            
			for(i = 0; i < count; i++)
            {
				LOM_VARIABLE_BYTE_DECODING(m_currPostingPtr, inIndex, sentencePosGap);
                sentencePos += sentencePosGap;

				LOM_VARIABLE_BYTE_DECODING(m_currPostingPtr, inIndex, wordPos);
                
                memcpy(uncompressPositionBuffer, &sentencePos, sizeof(sentencePos));
		        uncompressPositionBuffer += sizeof(sentencePos); 

				memcpy(uncompressPositionBuffer, &wordPos, sizeof(wordPos));
				uncompressPositionBuffer += sizeof(wordPos);
    		}
            
			posting->ptrToEmbeddedAttrsBuf		 = m_currPostingPtr + inIndex;
            posting->embeddedAttrsBufSize        = postingLength - inIndex;
	
			posting->scanId						 = m_osn;
			posting->embeddedAttrTranslationInfo = &m_embeddedAttrTranslationInfo;
			posting->getEmbeddedAttrsValFuncPtr  = &m_getEmbeddedAttrsValFunc;
			posting->indexInfoPoolIndex          = m_indexInfoPoolIndex;

			/* move to the next posting */
			m_nPostings --;
			if(scanDirection == TEXTIR_SCAN_FORWARD)
			{
				logicalDocId += docIdGap;
				posting->logicalDocId = logicalDocId;

				m_currPostingLengthPtr += sizeof(Four);
				m_currPostingPtr += postingLength;
			}
			else
			{
			    logicalDocId -= oldDocIdGap;
            	oldDocIdGap = docIdGap;
            	posting->logicalDocId = logicalDocId;
            
				m_currPostingLengthPtr -= sizeof(Four);
				memcpy(&prevPostingSize, m_currPostingLengthPtr, sizeof(Four));
				m_currPostingPtr -= prevPostingSize;
			}
			nElementsHasBeenRead ++;
			
			if(isEmptyPostingBuffer())
				break;
			
			inIndex = 0;
			postingQueue.PushTail();
			posting = &postingQueue.Tail();
            oid = &posting->docId;
            
			memcpy(&postingLength, m_currPostingLengthPtr, sizeof(Four));

    		LOM_VARIABLE_BYTE_DECODING(m_currPostingPtr, inIndex, docIdGap);
			
			LOM_VARIABLE_BYTE_DECODING(m_currPostingPtr, inIndex, oid->pageNo);
			oid->volNo = volNoOfPostingTupleID;
			LOM_VARIABLE_BYTE_DECODING(m_currPostingPtr, inIndex, oid->slotNo);
			LOM_VARIABLE_BYTE_DECODING(m_currPostingPtr, inIndex, oid->unique);
            oid->classID = indexingClassID;
            
			LOM_VARIABLE_BYTE_DECODING(m_currPostingPtr, inIndex, count);
			posting->nPositions = count;
			
#if defined (EXTENDED_BOOLEAN_MODEL)
			posting->weight = OOSQL_StorageManager::CalculateWeight(posting->nPositions, nPostingsOfCurrentKeyword);
#elif defined (HEURISTIC_MODEL)
			posting->weight = OOSQL_StorageManager::CalculateWeight(posting->nPositions, nPostingsPerObject);
			posting->bonus  = HEURISTIC_BOUNS_WEIGHT_VALUE;
#endif
            
            posting->positions = uncompressPositionBuffer;
            sentencePos = 0;
            
            m_uncompressedPostingBufferIndex += (sizeof(sentencePos) + sizeof(wordPos))*count;
            if(m_uncompressedPostingBufferIndex > m_uncompressedPostingBufferSize) 
            {
				LOM_ERROR(handle, eBIGGERPOSTINGBUFFERNEEDED_LOM);  
			}
				            
			for(i = 0; i < count; i++)
            {
				LOM_VARIABLE_BYTE_DECODING(m_currPostingPtr, inIndex, sentencePosGap);
                sentencePos += sentencePosGap;

				LOM_VARIABLE_BYTE_DECODING(m_currPostingPtr, inIndex, wordPos);
                
                memcpy(uncompressPositionBuffer, &sentencePos, sizeof(sentencePos));
		        uncompressPositionBuffer += sizeof(sentencePos); 

				memcpy(uncompressPositionBuffer, &wordPos, sizeof(wordPos));
				uncompressPositionBuffer += sizeof(wordPos);
    		}
            
			posting->ptrToEmbeddedAttrsBuf		 = m_currPostingPtr + inIndex;
            posting->embeddedAttrsBufSize        = postingLength - inIndex;
	
			posting->scanId						 = m_osn;
			posting->embeddedAttrTranslationInfo = &m_embeddedAttrTranslationInfo;
			posting->getEmbeddedAttrsValFuncPtr  = &m_getEmbeddedAttrsValFunc;
			posting->indexInfoPoolIndex          = m_indexInfoPoolIndex;

			/* move to the next posting */
			m_nPostings --;
			
			if(scanDirection == TEXTIR_SCAN_FORWARD)
			{
				logicalDocId += docIdGap;
				posting->logicalDocId = logicalDocId;

				m_currPostingLengthPtr += sizeof(Four);
				m_currPostingPtr += postingLength;
			}
			else
			{
			    logicalDocId -= oldDocIdGap;
            	oldDocIdGap = docIdGap;
            	posting->logicalDocId = logicalDocId;
            
				m_currPostingLengthPtr -= sizeof(Four);
				memcpy(&prevPostingSize, m_currPostingLengthPtr, sizeof(Four));
				m_currPostingPtr -= prevPostingSize;
			}
			nElementsHasBeenRead ++;
		}
	}
	else if(m_isContainingTupleID && !m_isContainingSentenceAndWordNum && !m_isContainingByteOffset)
	{
	
		while(isEmptyPostingBuffer() == SM_FALSE)
		{
			inIndex = 0;
			postingQueue.PushTail();
			posting = &postingQueue.Tail();
			oid = &posting->docId;

			memcpy(&postingLength, m_currPostingLengthPtr, sizeof(Four));

    		LOM_VARIABLE_BYTE_DECODING(m_currPostingPtr, inIndex, docIdGap);

			LOM_VARIABLE_BYTE_DECODING(m_currPostingPtr, inIndex, oid->pageNo);
			oid->volNo = volNoOfPostingTupleID;
			LOM_VARIABLE_BYTE_DECODING(m_currPostingPtr, inIndex, oid->slotNo);
			LOM_VARIABLE_BYTE_DECODING(m_currPostingPtr, inIndex, oid->unique);
            oid->classID = indexingClassID;
            
			LOM_VARIABLE_BYTE_DECODING(m_currPostingPtr, inIndex, posting->nPositions);
			
#if defined (EXTENDED_BOOLEAN_MODEL)
			posting->weight = OOSQL_StorageManager::CalculateWeight(posting->nPositions, nPostingsOfCurrentKeyword);
#elif defined (HEURISTIC_MODEL)
			posting->weight = OOSQL_StorageManager::CalculateWeight(posting->nPositions, nPostingsPerObject);
			posting->bonus  = HEURISTIC_BOUNS_WEIGHT_VALUE;
#endif
			posting->positions					 = m_currPostingPtr + inIndex;
			posting->ptrToEmbeddedAttrsBuf		 = posting->positions;
            posting->embeddedAttrsBufSize        = postingLength - inIndex;
            
			posting->scanId						 = m_osn;
			posting->embeddedAttrTranslationInfo = &m_embeddedAttrTranslationInfo;
			posting->getEmbeddedAttrsValFuncPtr  = &m_getEmbeddedAttrsValFunc;
			posting->indexInfoPoolIndex          = m_indexInfoPoolIndex;

			/* move to the next posting */
			m_nPostings --;
			if(scanDirection == TEXTIR_SCAN_FORWARD)
			{
				logicalDocId += docIdGap;
				posting->logicalDocId = logicalDocId;
				
				m_currPostingLengthPtr += sizeof(Four);
				m_currPostingPtr += postingLength;
			}
			else
			{
				logicalDocId -= oldDocIdGap;
            	oldDocIdGap = docIdGap;
            	posting->logicalDocId = logicalDocId;
            	
				m_currPostingLengthPtr -= sizeof(Four);
				memcpy(&prevPostingSize, m_currPostingLengthPtr, sizeof(Four));
				m_currPostingPtr -= prevPostingSize;
			}
			nElementsHasBeenRead ++;
			
			if(isEmptyPostingBuffer())
				break;
				
			inIndex = 0;
			postingQueue.PushTail();
			posting = &postingQueue.Tail();
			oid = &posting->docId;

			memcpy(&postingLength, m_currPostingLengthPtr, sizeof(Four));

    		LOM_VARIABLE_BYTE_DECODING(m_currPostingPtr, inIndex, docIdGap);

			LOM_VARIABLE_BYTE_DECODING(m_currPostingPtr, inIndex, oid->pageNo);
			oid->volNo = volNoOfPostingTupleID;
			LOM_VARIABLE_BYTE_DECODING(m_currPostingPtr, inIndex, oid->slotNo);
			LOM_VARIABLE_BYTE_DECODING(m_currPostingPtr, inIndex, oid->unique);
            oid->classID = indexingClassID;
            
			LOM_VARIABLE_BYTE_DECODING(m_currPostingPtr, inIndex, posting->nPositions);
			
#if defined (EXTENDED_BOOLEAN_MODEL)
			posting->weight = OOSQL_StorageManager::CalculateWeight(posting->nPositions, nPostingsOfCurrentKeyword);
#elif defined (HEURISTIC_MODEL)
			posting->weight = OOSQL_StorageManager::CalculateWeight(posting->nPositions, nPostingsPerObject);
			posting->bonus  = HEURISTIC_BOUNS_WEIGHT_VALUE;
#endif
			posting->positions					 = m_currPostingPtr + inIndex;
			posting->ptrToEmbeddedAttrsBuf		 = posting->positions;
            posting->embeddedAttrsBufSize        = postingLength - inIndex;
            
			posting->scanId						 = m_osn;
			posting->embeddedAttrTranslationInfo = &m_embeddedAttrTranslationInfo;
			posting->getEmbeddedAttrsValFuncPtr  = &m_getEmbeddedAttrsValFunc;
			posting->indexInfoPoolIndex          = m_indexInfoPoolIndex;

			/* move to the next posting */
			m_nPostings --;
			if(scanDirection == TEXTIR_SCAN_FORWARD)
			{
				logicalDocId += docIdGap;
				posting->logicalDocId = logicalDocId;
				
				m_currPostingLengthPtr += sizeof(Four);
				m_currPostingPtr += postingLength;
			}
			else
			{
				logicalDocId -= oldDocIdGap;
            	oldDocIdGap = docIdGap;
            	posting->logicalDocId = logicalDocId;
            	
				m_currPostingLengthPtr -= sizeof(Four);
				memcpy(&prevPostingSize, m_currPostingLengthPtr, sizeof(Four));
				m_currPostingPtr -= prevPostingSize;
			}
			nElementsHasBeenRead ++;				
		}
	}
	else
	{
		while(isEmptyPostingBuffer() == SM_FALSE)
		{
			inIndex = 0;
			postingQueue.PushTail();
			posting = &postingQueue.Tail();
			oid = &posting->docId;

			memcpy(&postingLength, m_currPostingLengthPtr, sizeof(Four));
		
    		LOM_VARIABLE_BYTE_DECODING(m_currPostingPtr, inIndex, docIdGap);
			
			if(m_isContainingTupleID)
			{
				LOM_VARIABLE_BYTE_DECODING(m_currPostingPtr, inIndex, oid->pageNo);
				oid->volNo = volNoOfPostingTupleID;
				LOM_VARIABLE_BYTE_DECODING(m_currPostingPtr, inIndex, oid->slotNo);
				LOM_VARIABLE_BYTE_DECODING(m_currPostingPtr, inIndex, oid->unique);
			}
			else
			{
				e = m_storageManager->Text_GetOIDFromLogicalDocId(m_ocn, posting->logicalDocId, oid);
				OOSQL_CHECK_ERR(e);
			}
			oid->classID = indexingClassID;

			LOM_VARIABLE_BYTE_DECODING(m_currPostingPtr, inIndex, count);
            posting->nPositions = count;
            
#if defined (EXTENDED_BOOLEAN_MODEL)
			posting->weight = OOSQL_StorageManager::CalculateWeight(posting->nPositions, nPostingsOfCurrentKeyword);
#elif defined (HEURISTIC_MODEL)
			posting->weight = OOSQL_StorageManager::CalculateWeight(posting->nPositions, nPostingsPerObject);
			posting->bonus  = HEURISTIC_BOUNS_WEIGHT_VALUE;
#endif

			posting->positions					 = m_currPostingPtr + inIndex;
			posting->ptrToEmbeddedAttrsBuf		 = posting->positions;
            posting->embeddedAttrsBufSize        = postingLength - inIndex;
			
			posting->scanId						 = m_osn;
			posting->embeddedAttrTranslationInfo = &m_embeddedAttrTranslationInfo;
			posting->getEmbeddedAttrsValFuncPtr  = &m_getEmbeddedAttrsValFunc;
			posting->indexInfoPoolIndex          = m_indexInfoPoolIndex;

			posting->positions = uncompressPositionBuffer;
			
			if(m_isContainingSentenceAndWordNum)
			{
				sentencePos = 0;
				byteOffset = 0;
				
				if(m_isContainingByteOffset)
					m_uncompressedPostingBufferIndex += (sizeof(sentencePos) + sizeof(wordPos) + sizeof(byteOffset))*count;
				else
					m_uncompressedPostingBufferIndex += (sizeof(sentencePos) + sizeof(wordPos))*count;
				
	            if(m_uncompressedPostingBufferIndex > m_uncompressedPostingBufferSize) 
	            {
					LOM_ERROR(handle, eBIGGERPOSTINGBUFFERNEEDED_LOM);  
				}
			
				for(i = 0; i < count; i++)
	            {
					LOM_VARIABLE_BYTE_DECODING(m_currPostingPtr, inIndex, sentencePosGap);
	                sentencePos += sentencePosGap;
	
					LOM_VARIABLE_BYTE_DECODING(m_currPostingPtr, inIndex, wordPos);
	                
	                memcpy(uncompressPositionBuffer, &sentencePos, sizeof(sentencePos));
			        uncompressPositionBuffer += sizeof(sentencePos); 
	
					memcpy(uncompressPositionBuffer, &wordPos, sizeof(wordPos));
					uncompressPositionBuffer += sizeof(wordPos);
	    		
		    		if(m_isContainingByteOffset)
		            {
		               	LOM_VARIABLE_BYTE_DECODING(m_currPostingPtr, inIndex, byteOffsetGap);
	                	byteOffset += byteOffsetGap;
	    				
	    				memcpy(uncompressPositionBuffer, &byteOffset, sizeof(byteOffset));
					    uncompressPositionBuffer += sizeof(byteOffset);
		            }
	    		}
	    		
				posting->ptrToEmbeddedAttrsBuf		 = m_currPostingPtr + inIndex;
            	posting->embeddedAttrsBufSize        = postingLength - inIndex;
			}

			if(!m_isContainingSentenceAndWordNum || m_isContainingByteOffset)
				OOSQL_ERR(eNOTIMPLEMENTED_OOSQL);

			/* move to the next posting */
			m_nPostings --;
			if(scanDirection == TEXTIR_SCAN_FORWARD)
			{
				logicalDocId += docIdGap;
				posting->logicalDocId = logicalDocId;
				
				m_currPostingLengthPtr += sizeof(Four);
				m_currPostingPtr += postingLength;
			}
			else
			{
				logicalDocId -= oldDocIdGap;
            	oldDocIdGap = docIdGap;
            	posting->logicalDocId = logicalDocId;
            	
				m_currPostingLengthPtr -= sizeof(Four);
				memcpy(&prevPostingSize, m_currPostingLengthPtr, sizeof(Four));
				m_currPostingPtr -= prevPostingSize;
			}
			nElementsHasBeenRead ++;
		}
	}
#endif  /* COMPRESSION */
	
#else // !defined(USE_SUBINDEX) || !defined(USE_WITHOUT_WEIGHT)

    while(isEmptyPostingBuffer() == SM_FALSE)
    {
        offset = 0;

        postingQueue.PushTail();
        posting = &postingQueue.Tail();

#ifdef USE_SUBINDEX
#if defined (ORDEREDSET_BACKWARD_SCAN)
        memcpy(&postingLength, m_currPostingLengthPtr, sizeof(Four));
#else
        memcpy(&postingLength, m_currPostingPtr + offset, sizeof(Four));
        offset += TEXTIR_SIZE_POSTINGLENGTH;    // skip POSTINGLENGTH
#endif  /* #if defined (ORDEREDSET_BACKWARD_SCAN) */
#endif
        memcpy(&posting->logicalDocId, m_currPostingPtr + offset, TEXTIR_SIZE_LOGICALDOCID);
        offset += TEXTIR_SIZE_LOGICALDOCID;

        if(m_isContainingTupleID)
        {
            memcpy(&posting->docId, m_currPostingPtr + offset, TEXTIR_SIZE_DOCID);
            offset += TEXTIR_SIZE_DOCID;
        }
        else
        {
            e = m_storageManager->Text_GetOIDFromLogicalDocId(m_ocn, posting->logicalDocId, &posting->docId);
            OOSQL_CHECK_ERR(e);
        }
        posting->docId.classID = indexingClassID;

#ifndef USE_WITHOUT_WEIGHT
        memcpy(&posting->weight, m_currPostingPtr + offset, TEXTIR_SIZE_WEIGHT);
        offset += TEXTIR_SIZE_WEIGHT;
#endif

        memcpy(&posting->nPositions, m_currPostingPtr + offset, TEXTIR_SIZE_NPOSITION);
        offset += TEXTIR_SIZE_NPOSITION;

#ifdef USE_WITHOUT_WEIGHT

#if defined (EXTENDED_BOOLEAN_MODEL)
        posting->weight = OOSQL_StorageManager::CalculateWeight(posting->nPositions, nPostingsOfCurrentKeyword);
#elif defined (HEURISTIC_MODEL)
        posting->weight = OOSQL_StorageManager::CalculateWeight(posting->nPositions, nPostingsPerObject);
        posting->bonus  = HEURISTIC_BOUNS_WEIGHT_VALUE;
#endif

#endif

        posting->positions                   = m_currPostingPtr + offset;
        posting->ptrToEmbeddedAttrsBuf       = posting->positions;
        posting->embeddedAttrsBufSize        = sizeof(postingLength) + postingLength - offset;
        posting->scanId                      = m_osn;
        posting->embeddedAttrTranslationInfo = &m_embeddedAttrTranslationInfo;
        posting->getEmbeddedAttrsValFuncPtr  = &m_getEmbeddedAttrsValFunc;
        posting->indexInfoPoolIndex          = m_indexInfoPoolIndex;

        if(m_isContainingSentenceAndWordNum)
        {
            // TEXTIR_SIZE_POSITION = sentense offset + word offset
            posting->ptrToEmbeddedAttrsBuf += (posting->nPositions * TEXTIR_SIZE_POSITION);
            posting->embeddedAttrsBufSize  -= (posting->nPositions * TEXTIR_SIZE_POSITION);
        }

        if(m_isContainingByteOffset)
        {
            posting->ptrToEmbeddedAttrsBuf += (posting->nPositions * sizeof(Four));
            posting->embeddedAttrsBufSize  -= (posting->nPositions * sizeof(Four));
        }

        if(!m_isContainingSentenceAndWordNum || m_isContainingByteOffset)
            OOSQL_ERR(eNOTIMPLEMENTED_OOSQL);

#ifdef USE_SUBINDEX
#if defined (ORDEREDSET_BACKWARD_SCAN)
        currPostingSize = postingLength;
#else
        currPostingSize = sizeof(Four) + postingLength;
#endif  /* #if defined (ORDEREDSET_BACKWARD_SCAN) */
#else
        currPostingSize = offset + (posting->nPositions * TEXTIR_SIZE_POSITION);
#endif

        /* move to the next posting */
        m_nPostings --;
        if(scanDirection == TEXTIR_SCAN_FORWARD)
        {
#if defined (ORDEREDSET_BACKWARD_SCAN)
            m_currPostingLengthPtr += sizeof(Four);
#endif  /* #if defined (ORDEREDSET_BACKWARD_SCAN) */
            m_currPostingPtr += currPostingSize;
        }
        else
        {
            m_currPostingLengthPtr -= sizeof(Four);
            memcpy(&prevPostingSize, m_currPostingLengthPtr, sizeof(Four));
            m_currPostingPtr -= prevPostingSize;
        }
        nElementsHasBeenRead ++;
    }
#endif

#endif

    /* return */
    return nElementsHasBeenRead;
}

inline Four oosql_TextIR_PostingScanInfo::getNextPostingWithoutDocId(
        OOSQL_TextIR_Posting    *posting
)
/*
    Function:

        NOTE: the buffer structure for posting without doc ID is like the following.

              +-----------------+--------+------------+---------------+
              | logical doc. ID | weight | nPositions | position list |
              +-----------------+--------+------------+---------------+

    Side effect:

    Referenced member variables:

    Return value:
*/
{
    Four    offset;


    /* check input parameter */
#ifdef  OOSQL_DEBUG
    if(posting == NULL) OOSQL_ERR(eBADPARAMETER_OOSQL);
#endif

    /* check if posting buffer is not empty */
    if(isEmptyPostingBuffer() == SM_TRUE) 
    {
        TEXTIR_SET_NULLPOSTING(posting);
        OOSQL_ERR(ePOSTINGBUFFER_EMPTY_OOSQL);
    }

    /*************************************
     * fetch the current read-in posting *
     *************************************/

#ifndef COMPRESSION  

    offset = 0;

	// skip postingLength
	offset += sizeof(Four);

    // skip logical id
    offset += TEXTIR_SIZE_LOGICALDOCID;
    
    // skip doc id
    offset += TEXTIR_SIZE_DOCID;

    memcpy(&(posting->nPositions), m_currPostingPtr + offset, TEXTIR_SIZE_NPOSITION);
    offset += TEXTIR_SIZE_NPOSITION;
    
    posting->positions = m_currPostingPtr + offset;

#else
    Four    sentencePos;
    Four    sentencePosGap;
    Two     wordPos;
    Four    byteOffset;
    Four    byteOffsetGap;
    Four    docIdGap;
    Four    count;
    Four    i;
    Four    e;
    char    *uncompressPositionBuffer;

	uncompressPositionBuffer = m_uncompressedPostingBuffer +  m_uncompressedPostingBufferIndex;

    offset = 0;

	// skip postingLength
	offset += sizeof(Four);

	// skip logical id
	LOM_VARIABLE_BYTE_DECODING(m_currPostingPtr, offset, docIdGap);

	// skip doc id
	LOM_VARIABLE_BYTE_DECODING(m_currPostingPtr, offset, posting->docId.pageNo);
	LOM_VARIABLE_BYTE_DECODING(m_currPostingPtr, offset, posting->docId.slotNo);
	LOM_VARIABLE_BYTE_DECODING(m_currPostingPtr, offset, posting->docId.unique);

	LOM_VARIABLE_BYTE_DECODING(m_currPostingPtr, offset, posting->nPositions);
    count = posting->nPositions;
    
    posting->positions	= uncompressPositionBuffer;
    
    if(m_isContainingSentenceAndWordNum)
	{
	    sentencePos = 0;
        byteOffset = 0;
        
		if(m_isContainingByteOffset)
			m_uncompressedPostingBufferIndex += (sizeof(sentencePos) + sizeof(wordPos) + sizeof(byteOffset))*count;
		else
			m_uncompressedPostingBufferIndex += (sizeof(sentencePos) + sizeof(wordPos))*count;
		
        while(m_uncompressedPostingBufferIndex > m_uncompressedPostingBufferSize) 
        {
            e = doublePostingBuffer();
            OOSQL_CHECK_ERR(e);
		}
    
    	for(i = 0; i < posting->nPositions; i++)
        {
    		LOM_VARIABLE_BYTE_DECODING(m_currPostingPtr, offset, sentencePosGap);
            sentencePos += sentencePosGap;
    
    		LOM_VARIABLE_BYTE_DECODING(m_currPostingPtr, offset, wordPos);
                
            memcpy(uncompressPositionBuffer, &sentencePos, sizeof(sentencePos));
            uncompressPositionBuffer += sizeof(sentencePos); 
    
    		memcpy(uncompressPositionBuffer, &wordPos, sizeof(wordPos));
    		uncompressPositionBuffer += sizeof(wordPos);
    		
    		if(m_isContainingByteOffset)
            {
                LOM_VARIABLE_BYTE_DECODING(m_currPostingPtr, offset, byteOffsetGap);
                byteOffset += byteOffsetGap;
	    				
                memcpy(uncompressPositionBuffer, &byteOffset, sizeof(byteOffset));
                uncompressPositionBuffer += sizeof(byteOffset);
            }
    	}
    }
#endif

    /* decrement the # of remaining postings */
    m_nPostings --;

    /* return */
    return eNOERROR;
}

inline Four oosql_TextIR_PostingScanInfo::doublePostingBuffer()
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
    m_postingLengthBufferSize = m_postingLengthBufferSize * 2;
    m_postingLengthBuffer = (char *)pMemoryManager->Realloc(m_postingLengthBuffer, m_postingLengthBufferSize);
    if(m_postingLengthBuffer == NULL)
        OOSQL_ERR(eOUTOFMEMORY_OOSQL);  
    
    m_postingBufferSize = m_postingBufferSize * 2;
    m_postingBuffer = (char *)pMemoryManager->Realloc(m_postingBuffer, m_postingBufferSize);
    if(m_postingBuffer == NULL)
        OOSQL_ERR(eOUTOFMEMORY_OOSQL);

#ifdef COMPRESSION
    m_uncompressedPostingBufferSize = m_uncompressedPostingBufferSize * 2;
	m_uncompressedPostingBuffer = (char *)pMemoryManager->Realloc(m_uncompressedPostingBuffer, m_uncompressedPostingBufferSize);
	if(m_uncompressedPostingBuffer == NULL)
		OOSQL_ERR(eOUTOFMEMORY_OOSQL);
#endif
    
    return eNOERROR;
}

inline Boolean oosql_TextIR_PostingScanInfo::isWildCharUsed()
/*
    Function:

    Side effect:

    Referenced member variables:

    Return value:
*/
{
    return m_wildCharFlag;
}


inline Four oosql_TextIR_PostingScanInfo::setNumOfObjectsInClass(Four ocn)
{
    Four        e;

    e = m_storageManager->Text_GetNumOfTextObjectsInClass(ocn, &m_nObjectsInClass);
    OOSQL_CHECK_ERR(e);

    return eNOERROR;
}


class   oosql_TextIR_SubPlanEvalBufferElem : public OOSQL_MemoryManagedObject {
public:
    OOSQL_TextIR_PostingQueue       postingQueue;       
    OOSQL_TextIR_PostingQueue       op1PostingQueue;   
    OOSQL_TextIR_PostingQueue       op2PostingQueue;    

    OOSQL_TextIR_Posting            posting;            // result of the current node 
    Boolean                         doneFlag;           // flag indicating all results are constructed 
                                                        // NOTE: This flag used for processing 'max' operator and
                                                        //       keyword containing wild char
                                                        //
    oosql_TextIR_PostingScanInfo    *postingScanInfo;   // posting scan information to process keyword node 
    OOSQL_MinMaxHeap*               minMaxHeap;         // heap information for processing 'max' operator 

    // constructor
    oosql_TextIR_SubPlanEvalBufferElem();

    // destructor
    virtual ~oosql_TextIR_SubPlanEvalBufferElem();
};

inline oosql_TextIR_SubPlanEvalBufferElem::oosql_TextIR_SubPlanEvalBufferElem()
: postingQueue(pMemoryManager), op1PostingQueue(pMemoryManager), op2PostingQueue(pMemoryManager)
/*
    Function:
        constructor

    Side effect:

    Referenced member variables:

    Return value:
*/
{
    /* initialize member variables */
    doneFlag = SM_FALSE;

    postingScanInfo = NULL;

    minMaxHeap = NULL;
}

inline oosql_TextIR_SubPlanEvalBufferElem::~oosql_TextIR_SubPlanEvalBufferElem()
{
    if(postingScanInfo)
        OOSQL_DELETE(postingScanInfo);
}

class   OOSQL_TextIR_SubPlanEvalBuffer : public OOSQL_MemoryManagedObject {
/*
 * DESCRIPTION:
 *      Control information for Text IR evaluation.
 */

    Four                                    nBufElem;
    oosql_TextIR_SubPlanEvalBufferElem*     evalBufferPool;

public:
    // constructor allocates memory for the object
    OOSQL_TextIR_SubPlanEvalBuffer(Four poolSize);

    // destructor
    virtual ~OOSQL_TextIR_SubPlanEvalBuffer();

    oosql_TextIR_SubPlanEvalBufferElem& operator [] (AP_TextIndexCondPoolIndex poolIndex) 
    {
        return evalBufferPool[GET_POOLSTART(poolIndex)];
    }

    oosql_TextIR_SubPlanEvalBufferElem& operator [] (Four index)
    {
        return evalBufferPool[index];
    }
};

#endif  /* _oosql_Eval_TextIR_Internal_hxx_ */

#else /* SLIMDOWN_TEXTIR */

#ifndef _oosql_Eval_TextIR_Internal_hxx_
#define _oosql_Eval_TextIR_Internal_hxx_

/*
    MODULE:
        oosql_Eval_TextIR_Internal.hxx

    DESCRIPTION:
*/


/*
 * include necessary module interface
 */
#include "OOSQL_Common.h"
#include "OOSQL_MinMaxHeap.hxx"
#include "OOSQL_MemoryManager.hxx"
#include "OOSQL_MemoryManagedObject.hxx"

class OOSQL_SortStream;
/*
 * constant definitions for manipulating Text IR posting
 */
#define TEXTIR_SIZE_LOGICALDOCID        sizeof(Four)
#define TEXTIR_SIZE_DOCID               sizeof(OOSQL_StorageManager::OID)
#define TEXTIR_SIZE_WEIGHT              sizeof(OOSQL_TextIR_Weight)
#define TEXTIR_SIZE_NPOSITION           sizeof(Four)
#define TEXTIR_POSTINGSKIP_NO_HINTS		-1				
#define TEXTIR_POSTINGSKIP_THRESHOLD    1000		

#if		!defined(USE_SUBINDEX) && !defined(USE_WITHOUT_WEIGHT)
#define TEXTIR_NPOSITIONOFFSET \
        ( TEXTIR_SIZE_LOGICALDOCID + TEXTIR_SIZE_DOCID + TEXTIR_SIZE_WEIGHT )
#define TEXTIR_SIZE_FIXEDPOSTINGFIELD \
        ( TEXTIR_SIZE_LOGICALDOCID + TEXTIR_SIZE_DOCID + \
          TEXTIR_SIZE_WEIGHT + TEXTIR_SIZE_NPOSITION )

#endif

#if		defined(USE_SUBINDEX) && !defined(USE_WITHOUT_WEIGHT)
#if		defined(ORDEREDSET_BACKWARD_SCAN)
#define TEXTIR_SIZE_POSTINGLENGTH       0
#else
#define TEXTIR_SIZE_POSTINGLENGTH       sizeof(Four)
#endif
#define TEXTIR_NPOSITIONOFFSET \
        ( TEXTIR_SIZE_POSTINGLENGTH + TEXTIR_SIZE_LOGICALDOCID + \
          TEXTIR_SIZE_DOCID + TEXTIR_SIZE_WEIGHT )
#define TEXTIR_SIZE_FIXEDPOSTINGFIELD \
        ( TEXTIR_SIZE_POSTINGLENGTH + TEXTIR_SIZE_LOGICALDOCID + \
          TEXTIR_SIZE_DOCID + TEXTIR_SIZE_WEIGHT + TEXTIR_SIZE_NPOSITION )
#endif

#if		!defined(USE_SUBINDEX) && defined(USE_WITHOUT_WEIGHT)
#undef  TEXTIR_SIZE_DOCID
#define TEXTIR_SIZE_DOCID		sizeof(OOSQL_StorageManager::TupleID)
#define TEXTIR_NPOSITIONOFFSET \
        (TEXTIR_SIZE_LOGICALDOCID + TEXTIR_SIZE_DOCID)
#define TEXTIR_SIZE_FIXEDPOSTINGFIELD \
        (TEXTIR_SIZE_LOGICALDOCID + TEXTIR_SIZE_DOCID + TEXTIR_SIZE_NPOSITION )
#endif

#if     defined(USE_SUBINDEX) && defined(USE_WITHOUT_WEIGHT)
#if		defined(ORDEREDSET_BACKWARD_SCAN)
#define TEXTIR_SIZE_POSTINGLENGTH       0
#else
#define TEXTIR_SIZE_POSTINGLENGTH       sizeof(Four)
#endif
#undef  TEXTIR_SIZE_DOCID
#define TEXTIR_SIZE_DOCID       sizeof(OOSQL_StorageManager::TupleID)
#define TEXTIR_NPOSITIONOFFSET \
        (TEXTIR_SIZE_POSTINGLENGTH + TEXTIR_SIZE_LOGICALDOCID + TEXTIR_SIZE_DOCID)
#define TEXTIR_SIZE_FIXEDPOSTINGFIELD \
        (TEXTIR_SIZE_POSTINGLENGTH + TEXTIR_SIZE_LOGICALDOCID + TEXTIR_SIZE_DOCID + TEXTIR_SIZE_NPOSITION )
#endif

#define TEXTIR_SIZE_MAXPOSITIONLIST \
        ( TEXTIR_MAXPOSTINGSIZE - TEXTIR_SIZE_FIXEDPOSTINGFIELD )
#define TEXTIR_SIZE_SENTENCEPOSITION    sizeof(Four)
#define TEXTIR_SIZE_WORDPOSITION        sizeof(Two)
#define TEXTIR_SIZE_POSITION            \
        ( TEXTIR_SIZE_SENTENCEPOSITION + TEXTIR_SIZE_WORDPOSITION )


/*
 * type definition for Text IR subplan evaluation buffer
 */

class   oosql_TextIR_TempFileInfo : public OOSQL_MemoryManagedObject {
public:
    Two									nCols;
    OOSQL_StorageManager::AttrInfo		attrInfo[TEXTIR_NUMFIELDS_IN_POSTING_WITHOUTPOSITION];
    Boolean								markFlag;
    OOSQL_StorageManager::ColListStruct	clist[TEXTIR_NUMFIELDS_IN_POSTING_WITHOUTPOSITION];
	void*								sortBufferPtr;
	Four								sortBufferLength;
	Four								sortBufferCurrentReadOffset;
	OOSQL_SortStream*					sortStream;

    // constructor
public:
	oosql_TextIR_TempFileInfo();
};

class   oosql_TextIR_PostingScanInfo : public OOSQL_MemoryManagedObject {
public:
    Four						m_osn;
	Four						m_ocn;
    Four						m_nPostings;
    Four						m_postingLengthBufferSize;		
    char*						m_postingLengthBuffer;			
	char*						m_currPostingLengthPtr;			
    Four						m_postingBufferSize;
    char*						m_postingBuffer;
    char*						m_currPostingPtr;
    Boolean						m_wildCharFlag;					// flag indicating if wild char. is used in the keyword
	Boolean						m_isContainingTupleID;
	Boolean						m_isContainingSentenceAndWordNum;
	Boolean						m_isContainingByteOffset;
    oosql_TextIR_TempFileInfo*	m_tempFileInfo;					// temporary file to process the wild char. by sorting
	Four						m_nPostingsOfCurrentKeyword;
	Four						m_nObjectsInClass;			
	OOSQL_StorageManager*		m_storageManager;				
	OOSQL_StorageManager::EmbeddedAttrTranslationInfo			
								m_embeddedAttrTranslationInfo;
	OOSQL_GetEmbeddedAttrsValFuncType							
								m_getEmbeddedAttrsValFunc;
	AP_IndexInfoPoolIndex		m_indexInfoPoolIndex;

    // constructor
    oosql_TextIR_PostingScanInfo(OOSQL_StorageManager* storageManager, Four nPostings, Boolean isContainingTupleID, Boolean isContainingSentenceAndWordNum, Boolean isContainingByteOffset);
	virtual ~oosql_TextIR_PostingScanInfo();
};

inline oosql_TextIR_PostingScanInfo::oosql_TextIR_PostingScanInfo(
	OOSQL_StorageManager*	storageManager,					// IN  StorageManager Interface
	Four					nPostings,						// IN  
	Boolean					isContainingTupleID,			// IN  
	Boolean					isContainingSentenceAndWordNum,	// IN  
	Boolean					isContainingByteOffset			// IN  
) 
{
	;
}

inline oosql_TextIR_PostingScanInfo::~oosql_TextIR_PostingScanInfo()
{
	;
}


class   oosql_TextIR_SubPlanEvalBufferElem : public OOSQL_MemoryManagedObject {
public:
	OOSQL_TextIR_PostingQueue		postingQueue;		
	OOSQL_TextIR_PostingQueue		op1PostingQueue;	
	OOSQL_TextIR_PostingQueue		op2PostingQueue;	

    OOSQL_TextIR_Posting			posting;			// result of the current node 
    Boolean							doneFlag;			// flag indicating all results are constructed 
														// NOTE: This flag used for processing 'max' operator and
														//       keyword containing wild char
														//
    oosql_TextIR_PostingScanInfo    *postingScanInfo;	// posting scan information to process keyword node 
    OOSQL_MinMaxHeap*				minMaxHeap;			// heap information for processing 'max' operator 

    // constructor
    oosql_TextIR_SubPlanEvalBufferElem();

	// destructor
	virtual ~oosql_TextIR_SubPlanEvalBufferElem();
};

inline oosql_TextIR_SubPlanEvalBufferElem::oosql_TextIR_SubPlanEvalBufferElem()
: postingQueue(pMemoryManager), op1PostingQueue(pMemoryManager), op2PostingQueue(pMemoryManager)
{
	;
}

inline oosql_TextIR_SubPlanEvalBufferElem::~oosql_TextIR_SubPlanEvalBufferElem()
{
	;
}

class   OOSQL_TextIR_SubPlanEvalBuffer : public OOSQL_MemoryManagedObject {
/*
 * DESCRIPTION:
 *      Control information for Text IR evaluation.
 */

    Four									nBufElem;
    oosql_TextIR_SubPlanEvalBufferElem*     evalBufferPool;

public:
    // constructor allocates memory for the object
    OOSQL_TextIR_SubPlanEvalBuffer(Four poolSize);

	// destructor
	virtual ~OOSQL_TextIR_SubPlanEvalBuffer();
};

#endif  /* _oosql_Eval_TextIR_Internal_hxx_ */


#endif /* SLIMDOWN_TEXTIR */
