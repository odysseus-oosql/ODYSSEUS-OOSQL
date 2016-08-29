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

#ifndef _oosql_Eval_TextIR_hxx_
#define _oosql_Eval_TextIR_hxx_

/*
    MODULE:
        oosql_Eval_TextIR.hxx

    DESCRIPTION:
        This header defines data structures for evaluating 
        Text IR(Information Retrieval) part of query.
        From the architectural aspect, this module is placed in OOSQL evaluator and
        interacts with LOM as lower layer. So, this module is invisible from the OOSQL user.
*/

#include "OOSQL_Common.h"
#include "OOSQL_MemoryManager.hxx"
#include "OOSQL_MemoryManagedObject.hxx"
#include "OOSQL_Eval_DS.hxx"

typedef Four    OOSQL_TextIR_LogicalDocID;
typedef double  OOSQL_TextIR_Weight;
typedef double  OOSQL_TextIR_Bonus;
typedef Four    (*OOSQL_GetEmbeddedAttrsValFuncType)(OOSQL_StorageManager*, Four, char*, Four, Four, OOSQL_StorageManager::ColListStruct*, OOSQL_StorageManager::EmbeddedAttrTranslationInfo*);
typedef struct  {
    OOSQL_TextIR_LogicalDocID       logicalDocId;          
    OOSQL_StorageManager::OID       docId;                  
    Four                            nPositions;             
    OOSQL_TextIR_Weight             weight;                
    OOSQL_TextIR_Bonus              bonus;                 
    char*                           positions;              
    char*                           ptrToEmbeddedAttrsBuf;  
    Four                            embeddedAttrsBufSize;   
    Four                            scanId;                
    OOSQL_StorageManager::EmbeddedAttrTranslationInfo*      
                                    embeddedAttrTranslationInfo;
    OOSQL_GetEmbeddedAttrsValFuncType*                      
                                    getEmbeddedAttrsValFuncPtr;
    AP_IndexInfoPoolIndex           indexInfoPoolIndex;   
} OOSQL_TextIR_Posting;

class OOSQL_TextIR_PostingQueue : public OOSQL_MemoryManagedObject {
public:
    OOSQL_TextIR_PostingQueue(Four initialSize = TEXTIR_DEFAULTPOSTINGQUEUESIZE);
    OOSQL_TextIR_PostingQueue(OOSQL_MemoryManager* memoryManager, Four initialSize = TEXTIR_DEFAULTPOSTINGQUEUESIZE);
    ~OOSQL_TextIR_PostingQueue();

    bool PushTail(OOSQL_TextIR_Posting& posting);
    bool PushTail();
    bool PushHead(OOSQL_TextIR_Posting& posting);
    bool PushHead();
    bool PopHead(OOSQL_TextIR_Posting& posting);
    bool PopHead();
    bool PopTail(OOSQL_TextIR_Posting& posting);
    bool PopTail();
    OOSQL_TextIR_Posting& Head();
    OOSQL_TextIR_Posting& Tail();
	OOSQL_TextIR_Posting& PreviousHead();	
    bool IsEmpty() { return (m_postingArrayNElements == 0)?true:false; }
	bool IsUsed() { return (m_postingArray != NULL)?true:false; }	

    void MakeEmpty();

    Four  size() { return m_postingArrayNElements; }
    OOSQL_TextIR_Posting& operator[] (Four index) { return m_postingArray[index]; }

private:
    OOSQL_TextIR_Posting*   m_postingArray;
    Four                    m_postingArraySize;
    Four                    m_postingArrayHead;
    Four                    m_postingArrayTail;
    Four                    m_postingArrayNElements;
    Four                    m_initialSize;
};
    
inline OOSQL_TextIR_PostingQueue::OOSQL_TextIR_PostingQueue(Four initialSize)
{
    m_initialSize      = initialSize;
    m_postingArraySize = 0;
    m_postingArrayHead = NIL;
    m_postingArrayTail = NIL;
    m_postingArray     = NULL;
    m_postingArrayNElements = 0;
}

inline OOSQL_TextIR_PostingQueue::OOSQL_TextIR_PostingQueue(OOSQL_MemoryManager* memoryManager, Four initialSize) 
: OOSQL_MemoryManagedObject(memoryManager)
{
    m_initialSize      = initialSize;
    m_postingArraySize = 0;
    m_postingArrayHead = NIL;
    m_postingArrayTail = NIL;
    m_postingArray     = NULL;
    m_postingArrayNElements = 0;
}

inline OOSQL_TextIR_PostingQueue::~OOSQL_TextIR_PostingQueue()
{
    if(m_postingArray)
        pMemoryManager->Free(m_postingArray);
}

inline bool OOSQL_TextIR_PostingQueue::PushTail(OOSQL_TextIR_Posting& posting)
{
    if(m_postingArray)
    {
        m_postingArrayNElements ++;

        if(m_postingArrayNElements > m_postingArraySize)
        {
            OOSQL_TextIR_Posting* newPostingArray;

            newPostingArray = (OOSQL_TextIR_Posting*)pMemoryManager->Alloc(sizeof(OOSQL_TextIR_Posting) * m_postingArraySize * 2);
            if(m_postingArrayHead < m_postingArrayTail)
            {
                memcpy(newPostingArray, &m_postingArray[m_postingArrayHead], sizeof(OOSQL_TextIR_Posting) * (m_postingArraySize - m_postingArrayHead));
            }
            else
            {
                memcpy(newPostingArray, &m_postingArray[m_postingArrayHead], sizeof(OOSQL_TextIR_Posting) * (m_postingArraySize - m_postingArrayHead));
                memcpy(&newPostingArray[m_postingArraySize - m_postingArrayHead], &m_postingArray[0], sizeof(OOSQL_TextIR_Posting) * (m_postingArrayTail + 1));
            }
            m_postingArrayHead = 0;
            m_postingArrayTail = m_postingArraySize - 1;

            pMemoryManager->Free(m_postingArray);
            m_postingArray     = newPostingArray;
            m_postingArraySize = m_postingArraySize * 2;
        }
        if(m_postingArrayNElements > 1)
            m_postingArrayTail = (m_postingArrayTail + 1) % m_postingArraySize;

        m_postingArray[m_postingArrayTail] = posting;
        
        return true;
    }
    else
    {
        m_postingArraySize = m_initialSize;
        m_postingArray = (OOSQL_TextIR_Posting*)pMemoryManager->Alloc(sizeof(OOSQL_TextIR_Posting) * m_postingArraySize);

        m_postingArrayTail = 0;
        m_postingArrayHead = 0;

        m_postingArray[m_postingArrayTail] = posting;
        m_postingArrayNElements ++;

        return true;
    }
}

inline bool OOSQL_TextIR_PostingQueue::PushTail()
{
    if(m_postingArray)
    {
        m_postingArrayNElements ++;

        if(m_postingArrayNElements > m_postingArraySize)
        {
            OOSQL_TextIR_Posting* newPostingArray;

            newPostingArray = (OOSQL_TextIR_Posting*)pMemoryManager->Alloc(sizeof(OOSQL_TextIR_Posting) * m_postingArraySize * 2);
            if(m_postingArrayHead < m_postingArrayTail)
            {
                memcpy(newPostingArray, &m_postingArray[m_postingArrayHead], sizeof(OOSQL_TextIR_Posting) * (m_postingArraySize - m_postingArrayHead));
            }
            else
            {
                memcpy(newPostingArray, &m_postingArray[m_postingArrayHead], sizeof(OOSQL_TextIR_Posting) * (m_postingArraySize - m_postingArrayHead));
                memcpy(&newPostingArray[m_postingArraySize - m_postingArrayHead], &m_postingArray[0], sizeof(OOSQL_TextIR_Posting) * (m_postingArrayTail + 1));
            }
            m_postingArrayHead = 0;
            m_postingArrayTail = m_postingArraySize - 1;

            pMemoryManager->Free(m_postingArray);
            m_postingArray     = newPostingArray;
            m_postingArraySize = m_postingArraySize * 2;
        }
        if(m_postingArrayNElements > 1)
            m_postingArrayTail = (m_postingArrayTail + 1) % m_postingArraySize;

        return true;
    }
    else
    {
        m_postingArraySize = m_initialSize;
        m_postingArray = (OOSQL_TextIR_Posting*)pMemoryManager->Alloc(sizeof(OOSQL_TextIR_Posting) * m_postingArraySize);

        m_postingArrayTail = 0;
        m_postingArrayHead = 0;

        m_postingArrayNElements ++;

        return true;
    }
}

inline bool OOSQL_TextIR_PostingQueue::PushHead(OOSQL_TextIR_Posting& posting)
{
    if(m_postingArray)
    {
        m_postingArrayNElements ++;

        if(m_postingArrayNElements > m_postingArraySize)
        {
            OOSQL_TextIR_Posting* newPostingArray;

            newPostingArray = (OOSQL_TextIR_Posting*)pMemoryManager->Alloc(sizeof(OOSQL_TextIR_Posting) * m_postingArraySize * 2);
            if(m_postingArrayHead < m_postingArrayTail)
            {
                memcpy(newPostingArray, &m_postingArray[m_postingArrayHead], sizeof(OOSQL_TextIR_Posting) * (m_postingArraySize - m_postingArrayHead));
            }
            else
            {
                memcpy(newPostingArray, &m_postingArray[m_postingArrayHead], sizeof(OOSQL_TextIR_Posting) * (m_postingArraySize - m_postingArrayHead));
                memcpy(&newPostingArray[m_postingArraySize - m_postingArrayHead], &m_postingArray[0], sizeof(OOSQL_TextIR_Posting) * (m_postingArrayTail + 1));
            }
            m_postingArrayHead = 0;
            m_postingArrayTail = m_postingArraySize - 1;

            pMemoryManager->Free(m_postingArray);
            m_postingArray     = newPostingArray;
            m_postingArraySize = m_postingArraySize * 2;
        }
        if(m_postingArrayNElements > 1)
            m_postingArrayHead = (m_postingArraySize + m_postingArrayHead - 1) % m_postingArraySize;

        m_postingArray[m_postingArrayHead] = posting;
        
        return true;
    }
    else
    {
        m_postingArraySize = m_initialSize;
        m_postingArray = (OOSQL_TextIR_Posting*)pMemoryManager->Alloc(sizeof(OOSQL_TextIR_Posting) * m_postingArraySize);

        m_postingArrayTail = 0;
        m_postingArrayHead = 0;

        m_postingArray[m_postingArrayHead] = posting;
        m_postingArrayNElements ++;

        return true;
    }
}

inline bool OOSQL_TextIR_PostingQueue::PushHead()
{
    if(m_postingArray)
    {
        m_postingArrayNElements ++;

        if(m_postingArrayNElements > m_postingArraySize)
        {
            OOSQL_TextIR_Posting* newPostingArray;

            newPostingArray = (OOSQL_TextIR_Posting*)pMemoryManager->Alloc(sizeof(OOSQL_TextIR_Posting) * m_postingArraySize * 2);
            if(m_postingArrayHead < m_postingArrayTail)
            {
                memcpy(newPostingArray, &m_postingArray[m_postingArrayHead], sizeof(OOSQL_TextIR_Posting) * (m_postingArraySize - m_postingArrayHead));
            }
            else
            {
                memcpy(newPostingArray, &m_postingArray[m_postingArrayHead], sizeof(OOSQL_TextIR_Posting) * (m_postingArraySize - m_postingArrayHead));
                memcpy(&newPostingArray[m_postingArraySize - m_postingArrayHead], &m_postingArray[0], sizeof(OOSQL_TextIR_Posting) * (m_postingArrayTail + 1));
            }
            m_postingArrayHead = 0;
            m_postingArrayTail = m_postingArraySize - 1;

            pMemoryManager->Free(m_postingArray);
            m_postingArray     = newPostingArray;
            m_postingArraySize = m_postingArraySize * 2;
        }
        if(m_postingArrayNElements > 1)
            m_postingArrayHead = (m_postingArraySize + m_postingArrayHead - 1) % m_postingArraySize;

        return true;
    }
    else
    {
        m_postingArraySize = m_initialSize;
        m_postingArray = (OOSQL_TextIR_Posting*)pMemoryManager->Alloc(sizeof(OOSQL_TextIR_Posting) * m_postingArraySize);

        m_postingArrayTail = 0;
        m_postingArrayHead = 0;

        m_postingArrayNElements ++;

        return true;
    }
}

inline void OOSQL_TextIR_PostingQueue::MakeEmpty()
{
    m_postingArrayNElements = 0;
    m_postingArrayHead = 0;
    m_postingArrayTail = 0;
}

inline bool OOSQL_TextIR_PostingQueue::PopHead(OOSQL_TextIR_Posting& posting)
{
    if(m_postingArrayNElements == 0)
        return false;

    posting = m_postingArray[m_postingArrayHead];
    
    m_postingArrayNElements --;
    if(m_postingArrayNElements > 0)
        m_postingArrayHead = (m_postingArrayHead + 1) % m_postingArraySize;

    return true;
}

inline bool OOSQL_TextIR_PostingQueue::PopHead()
{
    if(m_postingArrayNElements == 0)
        return false;

    m_postingArrayNElements --;
    if(m_postingArrayNElements > 0)
        m_postingArrayHead = (m_postingArrayHead + 1) % m_postingArraySize;

    return true;
}

inline bool OOSQL_TextIR_PostingQueue::PopTail(OOSQL_TextIR_Posting& posting)
{
    if(m_postingArrayNElements == 0)
        return false;

    posting = m_postingArray[m_postingArrayTail];
    
    m_postingArrayNElements --;
    if(m_postingArrayNElements > 0)
        m_postingArrayTail = (m_postingArraySize + m_postingArrayTail - 1) % m_postingArraySize;

    return true;
}

inline bool OOSQL_TextIR_PostingQueue::PopTail()
{
    if(m_postingArrayNElements == 0)
        return false;

    m_postingArrayNElements --;
    if(m_postingArrayNElements > 0)
        m_postingArrayTail = (m_postingArraySize + m_postingArrayTail - 1) % m_postingArraySize;

    return true;
}

inline OOSQL_TextIR_Posting& OOSQL_TextIR_PostingQueue::Head()
{
    if(m_postingArrayHead >= 0)
        return m_postingArray[m_postingArrayHead];
    else
        return m_postingArray[0];
}

inline OOSQL_TextIR_Posting& OOSQL_TextIR_PostingQueue::Tail()
{
    if(m_postingArrayTail >= 0)
        return m_postingArray[m_postingArrayTail];
    else
        return m_postingArray[0];
}

// return the posting that has been popped most recently
inline OOSQL_TextIR_Posting& OOSQL_TextIR_PostingQueue::PreviousHead()
{
	if(m_postingArrayNElements == 0)
		return m_postingArray[m_postingArrayHead]; 
	else
		return m_postingArray[(m_postingArraySize + m_postingArrayHead - 1) % m_postingArraySize];
}


//
// macro definitions to manipulating posting structure
//
#define TEXTIR_SET_NULLPOSTING(posting)         {if(posting) (posting)->weight = -1;}
#define TEXTIR_IS_NULLPOSTING(posting)          (((posting) && (posting)->weight == -1) || !(posting))

#define TEXTIR_PRINT_POSTING(posting) \
        printf("{ logicalDocId=%ld, ", (posting)->logicalDocId); \
        printf("docId={%ld,%ld,%ld,%ld,%ld}, ", (posting)->docId.volNo, (posting)->docId.pageNo, \
                (posting)->docId.slotNo, (posting)->docId.classID, (posting)->docId.unique); \
        printf("weight=%f, ", (posting)->weight); \
        printf("nPositions=%ld, ", (posting)->nPositions); \
        printf("positions=%X }", (posting)->positions);


/*
 * constant definitions
 */
#define TEXTIR_EOS      EOS     /* end of Text IR scan */

#define TEXTIR_NUMFIELDS_IN_POSTING_WITHOUTPOSITION     3
#define TEXTIR_NUMFIELDS_IN_POSTING_WITHPOSITION        5


/********************************************
 * includes internally used data structures *
 ********************************************/

#include "oosql_Eval_TextIR_Internal.hxx"


#endif  /* _oosql_Eval_TextIR_hxx_ */

#else /* SLIMDOWN_TEXTIR */

#ifndef _oosql_Eval_TextIR_hxx_
#define _oosql_Eval_TextIR_hxx_

/*
    MODULE:
        oosql_Eval_TextIR.hxx

    DESCRIPTION:
        This header defines data structures for evaluating 
        Text IR(Information Retrieval) part of query.
        From the architectural aspect, this module is placed in OOSQL evaluator and
        interacts with LOM as lower layer. So, this module is invisible from the OOSQL user.
*/

#include "OOSQL_Common.h"
#include "OOSQL_MemoryManager.hxx"
#include "OOSQL_MemoryManagedObject.hxx"
#include "OOSQL_Eval_DS.hxx"

typedef Four    OOSQL_TextIR_LogicalDocID;
typedef double	OOSQL_TextIR_Weight;
typedef double	OOSQL_TextIR_Bonus;
typedef Four	(*OOSQL_GetEmbeddedAttrsValFuncType)(OOSQL_StorageManager*, Four, char*, Four, Four, OOSQL_StorageManager::ColListStruct*, OOSQL_StorageManager::EmbeddedAttrTranslationInfo*);
typedef struct  {
    OOSQL_TextIR_LogicalDocID		logicalDocId;			
    OOSQL_StorageManager::OID		docId;				
    Four							nPositions;				
    OOSQL_TextIR_Weight				weight;					
    OOSQL_TextIR_Bonus				bonus;			
    One*							positions;				
	One*							ptrToEmbeddedAttrsBuf;	
    Four                            embeddedAttrsBufSize;   
	Four                            scanId;					
	OOSQL_StorageManager::EmbeddedAttrTranslationInfo*	
									embeddedAttrTranslationInfo;
	OOSQL_GetEmbeddedAttrsValFuncType*					
									getEmbeddedAttrsValFuncPtr;
	AP_IndexInfoPoolIndex			indexInfoPoolIndex;	
} OOSQL_TextIR_Posting;

class OOSQL_TextIR_PostingQueue : public OOSQL_MemoryManagedObject {
public:
	OOSQL_TextIR_PostingQueue(int initialSize = TEXTIR_DEFAULTPOSTINGQUEUESIZE);
	OOSQL_TextIR_PostingQueue(OOSQL_MemoryManager* memoryManager, int initialSize = TEXTIR_DEFAULTPOSTINGQUEUESIZE);
	~OOSQL_TextIR_PostingQueue();

private:
	OOSQL_TextIR_Posting*	m_postingArray;
	Four					m_postingArraySize;
	Four					m_postingArrayHead;
	Four					m_postingArrayTail;
	Four					m_postingArrayNElements;
	Four					m_initialSize;
};
	
inline OOSQL_TextIR_PostingQueue::OOSQL_TextIR_PostingQueue(int initialSize)
{
	;
}

inline OOSQL_TextIR_PostingQueue::OOSQL_TextIR_PostingQueue(OOSQL_MemoryManager* memoryManager, int initialSize) 
: OOSQL_MemoryManagedObject(memoryManager)
{
	;
}

inline OOSQL_TextIR_PostingQueue::~OOSQL_TextIR_PostingQueue()
{
	;
}

//
// macro definitions to manipulating posting structure
//
#define TEXTIR_SET_NULLPOSTING(posting)         {if(posting) (posting)->weight = -1;}
#define TEXTIR_IS_NULLPOSTING(posting)          (((posting) && (posting)->weight == -1) || !(posting))

#define TEXTIR_PRINT_POSTING(posting) \
        printf("{ logicalDocId=%d, ", (posting)->logicalDocId); \
        printf("docId={%d,%d,%d,%d,%d}, ", (posting)->docId.volNo, (posting)->docId.pageNo, \
                (posting)->docId.slotNo, (posting)->docId.classID, (posting)->docId.unique); \
        printf("weight=%f, ", (posting)->weight); \
        printf("nPositions=%d, ", (posting)->nPositions); \
        printf("positions=%X }", (posting)->positions);


/*
 * constant definitions
 */
#define TEXTIR_EOS      EOS     /* end of Text IR scan */

#define TEXTIR_NUMFIELDS_IN_POSTING_WITHOUTPOSITION     3
#define TEXTIR_NUMFIELDS_IN_POSTING_WITHPOSITION        5


/********************************************
 * includes internally used data structures *
 ********************************************/

#include "oosql_Eval_TextIR_Internal.hxx"


#endif  /* _oosql_Eval_TextIR_hxx_ */

#endif /* SLIMDOWN_TEXTIR*/
