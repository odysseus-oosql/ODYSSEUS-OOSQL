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

#ifndef _OOSQL_MULTIPLERESULTBUFFER_H_
#define _OOSQL_MULTIPLERESULTBUFFER_H_

#include "OOSQL_Common.h"
#include "OOSQL_Evaluator.hxx"
#include "OOSQL_MemoryManager.hxx"
#include "OOSQL_MemoryManagedObject.hxx"
#include "OOSQL_Error.h"

class OOSQL_MultipleResultBuffer : public OOSQL_MemoryManagedObject {
public:
	OOSQL_MultipleResultBuffer(OOSQL_Evaluator* evaluator, Four nResultsToRead, void* headerBuffer, Four headerBufferSize, void* dataBuffer, Four dataBufferSize);
	OOSQL_MultipleResultBuffer(OOSQL_MemoryManager* memoryManager, OOSQL_Evaluator* evaluator, Four nResultsToRead, void* headerBuffer, Four headerBufferSize, void* dataBuffer, Four dataBufferSize);
	~OOSQL_MultipleResultBuffer();

	bool IsAppendable();
	virtual Four AppendCurrentEvaluatedObject()
	{
		if(m_appendObjectType == 0)
			return AppendCurrentEvaluatedObjectWithHeaderAndData();
		else if(m_appendObjectType == 1)
			return AppendCurrentEvaluatedObjectWithHeader();
		else if(m_appendObjectType == 2)
			return AppendCurrentEvaluatedObjectWithData();
		else if(m_appendObjectType == 3)
			return AppendCurrentEvaluatedObjectWithNothing();
		else
			return eNOERROR;
	}

	Four AppendCurrentEvaluatedObjectWithHeaderAndData();
	Four AppendCurrentEvaluatedObjectWithHeader();
	Four AppendCurrentEvaluatedObjectWithData();
	Four AppendCurrentEvaluatedObjectWithNothing();
	Four GetNObjectRead() { return m_nResultsRead; }

protected:
	void InitInternalData(OOSQL_Evaluator* evaluator, Four nResultsToRead, void* headerBuffer, Four headerBufferSize, void* dataBuffer, Four dataBufferSize);
	Four GetIthColumnRealSizeOfCurrentObject(Four ithColumn);
	Four GetIthColumnSizeOfCurrentObject(Four ithColumn);

	OOSQL_Evaluator*	m_evaluator;
	Four				m_nResultsToRead;
	char*				m_headerBuffer;
	Four				m_headerBufferSize;
	Four				m_headerBufferOffset;
	char*				m_dataBuffer;
	Four				m_dataBufferSize;
	Four				m_dataBufferOffset;

	Four				m_nResultsRead;		
	Four				m_headerSize;			
	Four				m_nColumns;				
	Four*				m_realColumnSizes;	

	Four				m_headerNullVectorOffset;
	Four				m_headerSizesOffset;
	Four				m_headerRealSizesOffset;
	Four				m_headerOidsOffset;
	Four				m_headerColNosOffset;

	OOSQL_GetDataInfo*	m_getdata_info;			

	Four				m_appendObjectType;
};

class OOSQL_MultipleResultBufferWithData : public OOSQL_MultipleResultBuffer {
public:
	OOSQL_MultipleResultBufferWithData(OOSQL_Evaluator* evaluator, Four nResultsToRead, void* headerBuffer, Four headerBufferSize, void* dataBuffer, Four dataBufferSize)
		: OOSQL_MultipleResultBuffer(evaluator, nResultsToRead, headerBuffer, headerBufferSize, dataBuffer, dataBufferSize)
	{ }
	OOSQL_MultipleResultBufferWithData(OOSQL_MemoryManager* memoryManager, OOSQL_Evaluator* evaluator, Four nResultsToRead, void* headerBuffer, Four headerBufferSize, void* dataBuffer, Four dataBufferSize)
		: OOSQL_MultipleResultBuffer(memoryManager, evaluator, nResultsToRead, headerBuffer, headerBufferSize, dataBuffer, dataBufferSize)
	{ }

	Four AppendCurrentEvaluatedObject()
	{
		return AppendCurrentEvaluatedObjectWithData();
	}
};

class OOSQL_MultipleResultBufferWithHeader : public OOSQL_MultipleResultBuffer {
public:
	OOSQL_MultipleResultBufferWithHeader(OOSQL_Evaluator* evaluator, Four nResultsToRead, void* headerBuffer, Four headerBufferSize, void* dataBuffer, Four dataBufferSize)
		: OOSQL_MultipleResultBuffer(evaluator, nResultsToRead, headerBuffer, headerBufferSize, dataBuffer, dataBufferSize)
	{ }
	OOSQL_MultipleResultBufferWithHeader(OOSQL_MemoryManager* memoryManager, OOSQL_Evaluator* evaluator, Four nResultsToRead, void* headerBuffer, Four headerBufferSize, void* dataBuffer, Four dataBufferSize)
		: OOSQL_MultipleResultBuffer(memoryManager, evaluator, nResultsToRead, headerBuffer, headerBufferSize, dataBuffer, dataBufferSize)
	{ }

	Four AppendCurrentEvaluatedObject()
	{
		return AppendCurrentEvaluatedObjectWithHeader();
	}
};

class OOSQL_MultipleResultBufferWithHeaderAndData : public OOSQL_MultipleResultBuffer {
public:
	OOSQL_MultipleResultBufferWithHeaderAndData(OOSQL_Evaluator* evaluator, Four nResultsToRead, void* headerBuffer, Four headerBufferSize, void* dataBuffer, Four dataBufferSize)
		: OOSQL_MultipleResultBuffer(evaluator, nResultsToRead, headerBuffer, headerBufferSize, dataBuffer, dataBufferSize)
	{ }
	OOSQL_MultipleResultBufferWithHeaderAndData(OOSQL_MemoryManager* memoryManager, OOSQL_Evaluator* evaluator, Four nResultsToRead, void* headerBuffer, Four headerBufferSize, void* dataBuffer, Four dataBufferSize)
		: OOSQL_MultipleResultBuffer(memoryManager, evaluator, nResultsToRead, headerBuffer, headerBufferSize, dataBuffer, dataBufferSize)
	{ }

	Four AppendCurrentEvaluatedObject()
	{
		return AppendCurrentEvaluatedObjectWithHeaderAndData();
	}
};

class OOSQL_MultipleResultBufferWithNothing : public OOSQL_MultipleResultBuffer {
public:
	OOSQL_MultipleResultBufferWithNothing(OOSQL_Evaluator* evaluator, Four nResultsToRead, void* headerBuffer, Four headerBufferSize, void* dataBuffer, Four dataBufferSize)
		: OOSQL_MultipleResultBuffer(evaluator, nResultsToRead, headerBuffer, headerBufferSize, dataBuffer, dataBufferSize)
	{ }
	OOSQL_MultipleResultBufferWithNothing(OOSQL_MemoryManager* memoryManager, OOSQL_Evaluator* evaluator, Four nResultsToRead, void* headerBuffer, Four headerBufferSize, void* dataBuffer, Four dataBufferSize)
		: OOSQL_MultipleResultBuffer(memoryManager, evaluator, nResultsToRead, headerBuffer, headerBufferSize, dataBuffer, dataBufferSize)
	{ }

	Four AppendCurrentEvaluatedObject()
	{
		return AppendCurrentEvaluatedObjectWithNothing();
	}
};

inline Four OOSQL_MultipleResultBuffer::GetIthColumnRealSizeOfCurrentObject(Four ithColumn)
{
	Four											size;
	OOSQL_StorageManager::ColLengthInfoListStruct	lengthInfoList[1];
	OOSQL_StorageManager::TextDesc					textDesc;
	Four											e;
	
	if(ithColumn >= m_evaluator->m_getdata_nInfo)
		return -1;

	size = m_realColumnSizes[ithColumn];
	if(size != NIL)
	{
		if(*m_getdata_info[ithColumn].nullFlag)	
			size = -1;
		return size;
	}
	else
	{
		switch(m_getdata_info[ithColumn].type)
		{
		case OOSQL_TYPE_STRING:
		case OOSQL_TYPE_VARSTRING:
			if(*m_getdata_info[ithColumn].nullFlag)	
				size = 0;
			else if(m_getdata_info[ithColumn].ocn != NIL)
			{
				lengthInfoList[0].colNo = m_getdata_info[ithColumn].colNo;

				e = m_evaluator->m_storageManager->FetchColLength(m_getdata_info[ithColumn].ocn, SM_FALSE, 
																  m_getdata_info[ithColumn].oid, 1, lengthInfoList);
				if(e < eNOERROR) OOSQL_ERR(e);

				size = lengthInfoList[0].length;

				if(m_getdata_info[ithColumn].type == OOSQL_TYPE_STRING)
					m_realColumnSizes[ithColumn] = size;
			}
			else
			{
				size = GetIthColumnSizeOfCurrentObject(ithColumn);
			}
			break;

		case OOSQL_TYPE_TEXT:
			if(m_getdata_info[ithColumn].ocn != NIL)
			{
				e = m_evaluator->m_storageManager->Text_GetDescriptor(m_getdata_info[ithColumn].ocn, SM_FALSE, 
																	  m_getdata_info[ithColumn].oid, 
																	  m_getdata_info[ithColumn].colNo, &textDesc);
				if(e < eNOERROR) OOSQL_ERR(e);

				size = textDesc.size;
			}
			else
			{
				size = GetIthColumnSizeOfCurrentObject(ithColumn);
			}
			if(size < 0)
				size = -1;
			break;

		default:
			OOSQL_ERR(eINTERNALERROR_OOSQL); 
		}
	}

	return size;
}

inline Four OOSQL_MultipleResultBuffer::GetIthColumnSizeOfCurrentObject(Four ithColumn)
{
	Four size;

	if(m_getdata_info[ithColumn].type != OOSQL_TYPE_TEXT)
	{
		if(*m_getdata_info[ithColumn].nullFlag || m_getdata_info[ithColumn].srcLength == NULL)	
			return 0;
		else
			return *m_getdata_info[ithColumn].srcLength;
	}
	else
	{
		size = GetIthColumnRealSizeOfCurrentObject(ithColumn);
		if(size < 0)			
			size = 0;
		else
		{
			if(size >= OOSQL_EVALBUFFER_MAXSTRINGSIZE)
				size = OOSQL_EVALBUFFER_MAXSTRINGSIZE;
		}

		return size;
	}
}

inline bool OOSQL_MultipleResultBuffer::IsAppendable()
{
	if(m_nResultsToRead != NIL && m_nResultsRead >= m_nResultsToRead)
		return false;

	if(m_headerBuffer &&(m_headerBufferSize - m_headerBufferOffset) <= m_headerSize)
		return false;

	if(m_dataBuffer == NULL)
		return true;

	Four objectSize = 0;
	Four i;

	for(i = 0; i < m_nColumns; i++)
		objectSize += GetIthColumnSizeOfCurrentObject(i);

	if((m_dataBufferSize - m_dataBufferOffset) <= objectSize)
		return false;

	return true;
}

#define MULTIPLERESULT_CHAR_BIT											8
#define MULTIPLERESULT_BITMASK(bit)										((unsigned)0x80 >>((bit) % MULTIPLERESULT_CHAR_BIT))
#define MULTIPLERESULT_BITSLOT(bit)										((bit) / MULTIPLERESULT_CHAR_BIT)
#define MULTIPLERESULT_BITSET(ary, bit)									((ary)[MULTIPLERESULT_BITSLOT(bit)] |= MULTIPLERESULT_BITMASK(bit))
#define MULTIPLERESULT_BITRESET(ary, bit)								((ary)[MULTIPLERESULT_BITSLOT(bit)] &= ~MULTIPLERESULT_BITMASK(bit))
#define MULTIPLERESULT_BITTEST(ary, bit)								((ary)[MULTIPLERESULT_BITSLOT(bit)] & MULTIPLERESULT_BITMASK(bit))

#define MULTIPLERESULT_HEADER_OFFSET(pHeader)							(*(Four*)(pHeader + 0))
#define MULTIPLERESULT_HEADER_SIZE(pHeader)								(*(Four*)(pHeader + sizeof(Four)))
#define MULTIPLERESULT_HEADER_NULLVECTORS_SET(pHeaderNullVectors, i)	MULTIPLERESULT_BITSET(pHeaderNullVectors, i)
#define MULTIPLERESULT_HEADER_NULLVECTORS_RESET(pHeaderNullVectors, i)	MULTIPLERESULT_BITRESET(pHeaderNullVectors, i)
#define MULTIPLERESULT_HEADER_SIZES(pHeaderSizes, i)					((Four*)pHeaderSizes)[i]
#define MULTIPLERESULT_HEADER_REALSIZES(pHeaderRealSizes, i)			((Four*)pHeaderRealSizes)[i]
#define MULTIPLERESULT_HEADER_OIDS(pHeaderOids, i)						((OOSQL_StorageManager::OID*)pHeaderOids)[i]
#define MULTIPLERESULT_HEADER_COLNOS(pHeaderColNos, i)					((Four*)pHeaderColNos)[i]

inline Four OOSQL_MultipleResultBuffer::AppendCurrentEvaluatedObjectWithHeaderAndData()
{
	char*	pHeader;
	char*	pData;
	Four	returnLength;
	Four	e;
	Four	i;
	Four	size;
	Four	currentColumnSize;

	if(m_nResultsToRead != NIL && m_nResultsRead >= m_nResultsToRead)
		return eMULTIPLERESULTBUFFER_FULL_OOSQL;

	pHeader = m_headerBuffer + m_headerBufferOffset;
	pData   = m_dataBuffer + m_dataBufferOffset;

	char* pHeaderNullVectors = pHeader + m_headerNullVectorOffset;
	char* pHeaderSizes       = pHeader + m_headerSizesOffset;
	char* pHeaderRealSizes   = pHeader + m_headerRealSizesOffset;
	char* pHeaderOids        = pHeader + m_headerOidsOffset;
	char* pHeaderColNos      = pHeader + m_headerColNosOffset;

	if((m_headerBufferSize - m_headerBufferOffset) <= m_headerSize)
		return eMULTIPLERESULTBUFFER_FULL_OOSQL;
	MULTIPLERESULT_HEADER_OFFSET(pHeader) = m_dataBufferOffset;
	m_headerBufferOffset += m_headerSize;

	size = 0;
	for(i = 0; i < m_nColumns; i++)
	{
		if(*m_getdata_info[i].nullFlag)
		{
			MULTIPLERESULT_HEADER_NULLVECTORS_SET(pHeaderNullVectors, i);
			MULTIPLERESULT_HEADER_SIZES(pHeaderSizes, i)         = 0;
			MULTIPLERESULT_HEADER_REALSIZES(pHeaderRealSizes, i) = NIL;
			MULTIPLERESULT_HEADER_OIDS(pHeaderOids, i)           = *(m_getdata_info[i].oid);
			MULTIPLERESULT_HEADER_COLNOS(pHeaderColNos, i)       = m_getdata_info[i].colNo;
		}
		else
		{
			MULTIPLERESULT_HEADER_NULLVECTORS_RESET(pHeaderNullVectors, i);
			MULTIPLERESULT_HEADER_SIZES(pHeaderSizes, i)         = currentColumnSize = GetIthColumnSizeOfCurrentObject(i);
			MULTIPLERESULT_HEADER_REALSIZES(pHeaderRealSizes, i) = GetIthColumnRealSizeOfCurrentObject(i);
			MULTIPLERESULT_HEADER_OIDS(pHeaderOids, i)           = *(m_getdata_info[i].oid);
			MULTIPLERESULT_HEADER_COLNOS(pHeaderColNos, i)       = m_getdata_info[i].colNo;

			if((m_dataBufferSize - m_dataBufferOffset) <= currentColumnSize)
				return eMULTIPLERESULTBUFFER_FULL_OOSQL;

			if(m_getdata_info[i].srcPtr != NULL)
			{
				memcpy(pData, m_getdata_info[i].srcPtr, currentColumnSize);
			}
			else
			{
				if(m_getdata_info[i].type == OOSQL_TYPE_TEXT && GetIthColumnRealSizeOfCurrentObject(i) < 0)
				{
					MULTIPLERESULT_HEADER_NULLVECTORS_SET(pHeaderNullVectors, i);
				}
				else
				{
					e = m_evaluator->GetData(i, 0, pData, currentColumnSize, &returnLength);
					OOSQL_CHECK_ERR(e);
				}
			}
			pData              += currentColumnSize;
			size               += currentColumnSize;
			m_dataBufferOffset += currentColumnSize;
		}
	}
	MULTIPLERESULT_HEADER_SIZE(pHeader) = size;
	m_nResultsRead++;

	return eNOERROR;
}

inline Four OOSQL_MultipleResultBuffer::AppendCurrentEvaluatedObjectWithHeader()
{
	char*	pHeader;
	char*	pData;
	Four	returnLength;
	Four	e;
	Four	i;
	Four	size;
	Four	currentColumnSize;

	if(m_nResultsToRead != NIL && m_nResultsRead >= m_nResultsToRead)
		return eMULTIPLERESULTBUFFER_FULL_OOSQL;

	pHeader = m_headerBuffer + m_headerBufferOffset;

	char* pHeaderNullVectors = pHeader + m_headerNullVectorOffset;
	char* pHeaderSizes       = pHeader + m_headerSizesOffset;
	char* pHeaderRealSizes   = pHeader + m_headerRealSizesOffset;
	char* pHeaderOids        = pHeader + m_headerOidsOffset;
	char* pHeaderColNos      = pHeader + m_headerColNosOffset;

	if((m_headerBufferSize - m_headerBufferOffset) <= m_headerSize)
		return eMULTIPLERESULTBUFFER_FULL_OOSQL;
	MULTIPLERESULT_HEADER_OFFSET(pHeader) = m_dataBufferOffset;
	m_headerBufferOffset += m_headerSize;

	size = 0;
	for(i = 0; i < m_nColumns; i++)
	{
		if(*m_getdata_info[i].nullFlag)
		{
			MULTIPLERESULT_HEADER_NULLVECTORS_SET(pHeaderNullVectors, i);
			MULTIPLERESULT_HEADER_SIZES(pHeaderSizes, i)         = 0;
			MULTIPLERESULT_HEADER_REALSIZES(pHeaderRealSizes, i) = NIL;
			MULTIPLERESULT_HEADER_OIDS(pHeaderOids, i)           = *(m_getdata_info[i].oid);
			MULTIPLERESULT_HEADER_COLNOS(pHeaderColNos, i)       = m_getdata_info[i].colNo;
		}
		else
		{
			MULTIPLERESULT_HEADER_NULLVECTORS_RESET(pHeaderNullVectors, i);
			MULTIPLERESULT_HEADER_SIZES(pHeaderSizes, i)         = currentColumnSize = GetIthColumnSizeOfCurrentObject(i);
			MULTIPLERESULT_HEADER_REALSIZES(pHeaderRealSizes, i) = GetIthColumnRealSizeOfCurrentObject(i);
			MULTIPLERESULT_HEADER_OIDS(pHeaderOids, i)           = *(m_getdata_info[i].oid);
			MULTIPLERESULT_HEADER_COLNOS(pHeaderColNos, i)       = m_getdata_info[i].colNo;

			if(m_getdata_info[i].type == OOSQL_TYPE_TEXT && GetIthColumnRealSizeOfCurrentObject(i) < 0)
			{
				MULTIPLERESULT_HEADER_NULLVECTORS_SET(pHeaderNullVectors, i);
			}

			size  += currentColumnSize;
		}
	}
	MULTIPLERESULT_HEADER_SIZE(pHeader) = size;
	m_nResultsRead++;

	return eNOERROR;
}

inline Four OOSQL_MultipleResultBuffer::AppendCurrentEvaluatedObjectWithData()
{
	char*	pHeader;
	char*	pData;
	Four	returnLength;
	Four	e;
	Four	i;
	Four	size;
	Four	currentColumnSize;

	if(m_nResultsToRead != NIL && m_nResultsRead >= m_nResultsToRead)
		return eMULTIPLERESULTBUFFER_FULL_OOSQL;

	pData   = m_dataBuffer + m_dataBufferOffset;

	for(i = 0; i < m_nColumns; i++)
	{
		if(*m_getdata_info[i].nullFlag == SM_FALSE)
		{
			currentColumnSize = GetIthColumnSizeOfCurrentObject(i);

			if((m_dataBufferSize - m_dataBufferOffset) <= currentColumnSize)
				return eMULTIPLERESULTBUFFER_FULL_OOSQL;

			if(m_getdata_info[i].srcPtr != NULL)
			{
				memcpy(pData, m_getdata_info[i].srcPtr, currentColumnSize);
			}
			else
			{
				if(OOSQL_MASK_COMPLEXTYPE(m_getdata_info[i].type) == OOSQL_COMPLEXTYPE_BASIC)
				{
					e = m_evaluator->GetData(i, 0, pData, currentColumnSize, &returnLength);
					OOSQL_CHECK_ERR(e);
				}
				else
					returnLength = 0;
			}
			pData              += currentColumnSize;
			m_dataBufferOffset += currentColumnSize;
		}
	}
	m_nResultsRead++;

	return eNOERROR;
}

inline Four OOSQL_MultipleResultBuffer::AppendCurrentEvaluatedObjectWithNothing()
{
	m_nResultsRead++;

	return eNOERROR;
}


#endif // _OOSQL_MULTIPLERESULTBUFFER_H_
