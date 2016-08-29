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

#ifndef _OOSQL_SORT_STREAM_H_
#define _OOSQL_SORT_STREAM_H_

#include "OOSQL_Common.h"
#include "OOSQL_Evaluator.hxx"
#include "OOSQL_MemoryManager.hxx"
#include "OOSQL_MemoryManagedObject.hxx"
#include "OOSQL_StorageManager.hxx"
#include "OOSQL_LargeTemporaryObject.hxx"
#include "OOSQL_Error.h"

struct OOSQL_KeyEncodingInfo {
	Four offset;
	Four size;
};

class OOSQL_SortStream : public OOSQL_MemoryManagedObject {
public:
	OOSQL_SortStream(OOSQL_StorageManager* storageManager);
	OOSQL_SortStream(OOSQL_MemoryManager* memoryManager, OOSQL_StorageManager* storageManager);
	~OOSQL_SortStream();

	Four CreateStream(Four volID, Four nAttrs, OOSQL_StorageManager::AttrInfo* attrInfo, OOSQL_StorageManager::BTreeKeyInfo* keyInfo, void* userMemory = NULL, Four userMemorySize = 0);
	Four DestroyStream();

	Four CreateObject(Four nCols, OOSQL_StorageManager::ColListStruct* clist);
	Four NextScan(Four nCols, OOSQL_StorageManager::ColListStruct* clist);
	
	Four FastCreateObject(Four nCols, OOSQL_StorageManager::ColListStruct* clist);
	Four FastNextScan(Four nCols, OOSQL_StorageManager::ColListStruct* clist);

	Four SetFastEncodingInfo(Four nCols, OOSQL_FastEncodingInfo* encodingInfo);
	Four GetFastEncodingInfo(Four nCols, OOSQL_FastEncodingInfo* encodingInfo);
	Four FastEncodedCreateObject();
	Four FastEncodedNextScan();
	
	Four Sort();

	Four OpenScan();
	Four CloseScan();

protected:
	Four PutTuple(OOSQL_StorageManager::SortStreamTuple* sortTuple);
	Four GetTuple(OOSQL_StorageManager::SortStreamTuple* sortTuple);
	Four SortTuples();
	Four QuickSort();
	Four MergeSort();
	Four MergeSort(Four lo, Four hi);

	Four									m_volID;				
	Four									m_sortStreamID;			
	OOSQL_StorageManager::AttrInfo*			m_attrInfo;				
	OOSQL_KeyEncodingInfo*					m_keyEncodingInfo;		
	OOSQL_StorageManager::SortTupleDesc		m_sortTupleDesc;		
	Four									m_nAttrs;			
	OOSQL_StorageManager*					m_storageManager;		
	char*									m_keyBuffer;			
	OOSQL_LargeTemporaryObject				m_largeTemporaryObject;
	OOSQL_StorageManager::SortStreamTuple	m_sortTuple;			
	Four									m_sortTupleDataLen;		
	bool									m_stringTypeExist;
	Four									m_offsetToLargeTemporaryObjectOffset;	

	bool									m_useInMemorySorting;
	bool									m_sortBufferShouldBeFreed;
	char*									m_sortBuffer;
	char*									m_sortBufferTail;
	Four									m_sortBufferSize;
	Four									m_sortBufferFreeSize;
	Four									m_nSortTuples;

	OOSQL_FastEncodingInfo*					m_fastEncodingInfos;
	Four									m_nFastEncodingInfos;

	char*									m_mergeBuffer;

	int (*m_compareFunc)(OOSQL_StorageManager::SortTupleDesc* sortTupleDesc, void *elem1, void *elem2);
};

#define SORTSTREAM_CHAR_BIT 8
#define SORTSTREAM_BITMASK(bit) ((unsigned)0x80 >> ((bit) % SORTSTREAM_CHAR_BIT))
#define SORTSTREAM_BITSLOT(bit) ((bit) / SORTSTREAM_CHAR_BIT)
#define SORTSTREAM_BITSET(ary, bit) ((ary)[SORTSTREAM_BITSLOT(bit)] |= SORTSTREAM_BITMASK(bit))
#define SORTSTREAM_BITRESET(ary, bit) ((ary)[SORTSTREAM_BITSLOT(bit)] &= ~SORTSTREAM_BITMASK(bit))
#define SORTSTREAM_BITTEST(ary, bit) ((ary)[SORTSTREAM_BITSLOT(bit)] & SORTSTREAM_BITMASK(bit))

inline Four OOSQL_SortStream::CreateObject(Four nCols, OOSQL_StorageManager::ColListStruct* clist)
{
	Four			e;
	Four			offset;
	Four			size;
	Four			i, j;
	Two				colNo;
	Four			colType;
	UFour			posHigh;
	UFour			posLow;
	Four			attrOffset[MAXNUMOFATTRIBUTE];
	char*			p;

	e = m_largeTemporaryObject.GetDataSize(posHigh, posLow);
	OOSQL_CHECK_ERR(e);
	
	p = m_keyBuffer + m_offsetToLargeTemporaryObjectOffset;
	memcpy(p, &posHigh, sizeof(UFour)); p += sizeof(UFour);
	memcpy(p, &posLow,  sizeof(UFour));

	for(i = 0; i < m_nAttrs; i++)
		attrOffset[i] = NIL;

	offset = sizeof(UFour) * m_nAttrs;	
	for(i = 0; i < nCols; i++)
	{
		colNo			  = clist[i].colNo;
		colType			  = m_attrInfo[colNo].type;
		attrOffset[colNo] = offset;

		offset += sizeof(OOSQL_StorageManager::ColListStruct);

		if(colType == OOSQL_TYPE_STRING || colType == OOSQL_TYPE_VARSTRING) 
			offset += clist[i].dataLength;
	}
	
	e = m_largeTemporaryObject.AppendData(sizeof(UFour) * m_nAttrs, &attrOffset);
	OOSQL_CHECK_ERR(e);

	for(i = 0; i < nCols; i++)
	{
		colNo   = clist[i].colNo;
		colType = m_attrInfo[colNo].type;

		offset = m_keyEncodingInfo[colNo].offset;
		size   = m_keyEncodingInfo[colNo].size;

		if(colType != OOSQL_TYPE_STRING && colType != OOSQL_TYPE_VARSTRING) 
		{
			clist[i].retLength = clist[i].dataLength;
			e = m_largeTemporaryObject.AppendData(sizeof(OOSQL_StorageManager::ColListStruct), &clist[i]);
			OOSQL_CHECK_ERR(e);

			if(offset != NIL)
			{
				memset(m_keyBuffer + offset, 0, size);
				if(!clist[i].nullFlag)
				{
					memcpy(m_keyBuffer + offset, &clist[i].data, size);
				}
			}
		}
		else
		{
			if(clist[i].nullFlag)
				clist[i].retLength = 0;
			else
				clist[i].retLength = clist[i].dataLength;
			e = m_largeTemporaryObject.AppendData(sizeof(OOSQL_StorageManager::ColListStruct), &clist[i]);
			OOSQL_CHECK_ERR(e);

			if(!clist[i].nullFlag)
			{
				e = m_largeTemporaryObject.AppendData(clist[i].dataLength, clist[i].data.ptr);
				OOSQL_CHECK_ERR(e);
			}

			if(offset != NIL)
			{
				if(!clist[i].nullFlag)
				{
					if(size < clist[i].dataLength)
					{
						memcpy(m_keyBuffer + offset, clist[i].data.ptr, size);
					}
					else
					{
						memset(m_keyBuffer + offset, 0, size);
						memcpy(m_keyBuffer + offset, clist[i].data.ptr, clist[i].dataLength);
					}
				}
				else
				{
					memset(m_keyBuffer + offset, 0, size);
				}
			}
		}
	}

	e = PutTuple(&m_sortTuple);
	OOSQL_CHECK_ERR(e);

	return eNOERROR;
}

inline Four OOSQL_SortStream::NextScan(Four nCols, OOSQL_StorageManager::ColListStruct* clist)
{
	Four				e;
	Four				offset;
	Four				i;
	Two					colNo;
	Two					length;
	Four				colType;
	UFour				posHigh, posLow, dataHigh, dataLow;
	Four				attrOffset[MAXNUMOFATTRIBUTE];
	char*				p;
	void*				ptr;

	e = GetTuple(&m_sortTuple);
	OOSQL_CHECK_ERR(e);

	if(e == EOS) return EOS;

	p = m_keyBuffer + m_offsetToLargeTemporaryObjectOffset;
	memcpy(&posHigh, p, sizeof(UFour)); p += sizeof(UFour);
	memcpy(&posLow,  p, sizeof(UFour));

	e = m_largeTemporaryObject.ReadData(posHigh, posLow, sizeof(Four) * m_nAttrs, attrOffset);
	OOSQL_CHECK_ERR(e);

	for(i = 0; i < nCols; i++)
	{
		colNo   = clist[i].colNo;
		colType = m_attrInfo[colNo].type;

		offset = attrOffset[colNo];

		if(colType != OOSQL_TYPE_STRING && colType != OOSQL_TYPE_VARSTRING)
		{
			e = m_largeTemporaryObject.AddOffset(posHigh, posLow, offset, dataHigh, dataLow);
			OOSQL_CHECK_ERR(e);

			e = m_largeTemporaryObject.ReadData(dataHigh, dataLow, sizeof(OOSQL_StorageManager::ColListStruct), &clist[i]);
			OOSQL_CHECK_ERR(e);
		}
		else
		{
			e = m_largeTemporaryObject.AddOffset(posHigh, posLow, offset, dataHigh, dataLow);
			OOSQL_CHECK_ERR(e);

			ptr = clist[i].data.ptr;
			e = m_largeTemporaryObject.ReadData(dataHigh, dataLow, sizeof(OOSQL_StorageManager::ColListStruct), &clist[i]);
			OOSQL_CHECK_ERR(e);

			e = m_largeTemporaryObject.AddOffset(posHigh, posLow, offset + sizeof(OOSQL_StorageManager::ColListStruct), dataHigh, dataLow);
			OOSQL_CHECK_ERR(e);

			clist[i].data.ptr = ptr;

			if(!clist[i].nullFlag)
			{
				e = m_largeTemporaryObject.ReadData(dataHigh, dataLow, clist[i].dataLength, clist[i].data.ptr);
				OOSQL_CHECK_ERR(e);
			}
		}
	}

	return eNOERROR;
}

inline Four OOSQL_SortStream::PutTuple(OOSQL_StorageManager::SortStreamTuple* sortTuple)
{
	Four e;
	Four i;

	if(m_useInMemorySorting)
	{
		m_sortBufferFreeSize -= m_sortTupleDataLen;
		if(m_sortBufferFreeSize > 0)
		{
			memcpy(m_sortBufferTail, sortTuple->data, m_sortTupleDataLen);
			m_sortBufferTail += m_sortTupleDataLen;
			m_nSortTuples ++;

			return eNOERROR;
		}
		else
		{
			// open sort stream
			e = m_storageManager->OpenSortStream(m_volID, &m_sortTupleDesc);
			OOSQL_CHECK_ERR(e);
			m_sortStreamID = e;

			m_useInMemorySorting = SM_FALSE;
			
			OOSQL_StorageManager::SortStreamTuple	tempSortTuple;
			char*									p;

			tempSortTuple = *sortTuple;
			p			  = m_sortBuffer;

			for(i = 0; i < m_nSortTuples; i++)
			{
				tempSortTuple.data = p;
				p += m_sortTupleDataLen;

				e = m_storageManager->PutTuplesIntoSortStream(m_sortStreamID, 1, &tempSortTuple);
				OOSQL_CHECK_ERR(e);
			}

			e = m_storageManager->PutTuplesIntoSortStream(m_sortStreamID, 1, sortTuple);
			OOSQL_CHECK_ERR(e);

			return eNOERROR;
		}
	}
	else
	{
		e = m_storageManager->PutTuplesIntoSortStream(m_sortStreamID, 1, sortTuple);
		OOSQL_CHECK_ERR(e);

		return eNOERROR;
	}
}

inline Four OOSQL_SortStream::GetTuple(OOSQL_StorageManager::SortStreamTuple* sortTuple)
{
	Four				e;
	Boolean				done;				/* flag which indicates sort stream is empty or not */
    Four				numSortTuple;       /* # of sort tuple from sort stream. In this function, always 1 */

	if(m_useInMemorySorting)
	{
		if(m_nSortTuples > 0)
		{
			memcpy(sortTuple->data, m_sortBufferTail, m_sortTupleDataLen);
			m_sortBufferTail += m_sortTupleDataLen;
			m_nSortTuples --;

			return eNOERROR;
		}
		else
			return EOS;
	}
	else
	{
		done = SM_FALSE;
		numSortTuple = 1;
		e = m_storageManager->GetTuplesFromSortStream(m_sortStreamID, &numSortTuple, sortTuple, &done);
		OOSQL_CHECK_ERR(e);

		if(done) return EOS;
		else	 return eNOERROR;
	}
}

inline Four OOSQL_SortStream::FastCreateObject(Four nCols, OOSQL_StorageManager::ColListStruct* clist)
{
	Four			e;
	Four			offset;
	Four			size;
	UFour			posHigh;
	UFour			posLow;
	Four			i;
	Four			colNo;
	Four			colType;
	char*			p;

	e = m_largeTemporaryObject.GetDataSize(posHigh, posLow);
	OOSQL_CHECK_ERR(e);
	
	p = m_keyBuffer + m_offsetToLargeTemporaryObjectOffset;
	memcpy(p, &posHigh, sizeof(UFour)); p += sizeof(UFour);
	memcpy(p, &posLow,  sizeof(UFour));

	for(i = 0; i < nCols; i++)
	{
		colNo   = clist[i].colNo;
		colType = m_attrInfo[colNo].type;

		offset = m_keyEncodingInfo[colNo].offset;
		size   = m_keyEncodingInfo[colNo].size;

		clist[i].retLength = clist[i].dataLength;

		if(offset != NIL)
		{
			if(!clist[i].nullFlag)
			{
				if(colType == OOSQL_TYPE_STRING || colType == OOSQL_TYPE_VARSTRING)
				{
					if(size < clist[i].retLength)
					{
						memcpy(m_keyBuffer + offset, clist[i].data.ptr, size);
					}
					else
					{
						memset(m_keyBuffer + offset, 0, size);
						memcpy(m_keyBuffer + offset, clist[i].data.ptr, clist[i].retLength);
					}
				}
				else
				{
					memcpy(m_keyBuffer + offset, &clist[i].data, size);
				}
			}
			else
			{
				memset(m_keyBuffer + offset, 0, size);
			}
		}
	}

	if(!m_stringTypeExist)
	{
		e = m_largeTemporaryObject.AppendData(sizeof(OOSQL_StorageManager::ColListStruct) * nCols, clist);
		OOSQL_CHECK_ERR(e);
	}
	else
	{
		for(i = 0; i < nCols; i++)
		{
			colNo   = clist[i].colNo;
			colType = m_attrInfo[colNo].type;

			clist[i].retLength = clist[i].dataLength;
			e = m_largeTemporaryObject.AppendData(sizeof(OOSQL_StorageManager::ColListStruct), &clist[i]);
			OOSQL_CHECK_ERR(e);

			if(colType == OOSQL_TYPE_STRING || colType == OOSQL_TYPE_VARSTRING)
			{
				if(!clist[i].nullFlag)
				{
					e = m_largeTemporaryObject.AppendData(clist[i].dataLength, clist[i].data.ptr);
					OOSQL_CHECK_ERR(e);
				}
			}
		}
	}

	e = PutTuple(&m_sortTuple);
	OOSQL_CHECK_ERR(e);

	return eNOERROR;
}

inline Four OOSQL_SortStream::FastNextScan(Four nCols, OOSQL_StorageManager::ColListStruct* clist)
{
	Four				e;
	UFour				posHigh, posLow;
	Four				colNo, colType;
	char*				p;
	void*				ptr;
	Four				i;
	
	e = GetTuple(&m_sortTuple);
	OOSQL_CHECK_ERR(e);

	if(e != EOS)
	{
		p = m_keyBuffer + m_offsetToLargeTemporaryObjectOffset;
		memcpy(&posHigh, p, sizeof(UFour)); p += sizeof(UFour);
		memcpy(&posLow,  p, sizeof(UFour));

		if(!m_stringTypeExist)
		{
			e = m_largeTemporaryObject.ReadData(posHigh, posLow, sizeof(OOSQL_StorageManager::ColListStruct) * nCols, clist);
			OOSQL_CHECK_ERR(e);
		}
		else
		{
			for(i = 0; i < nCols; i++)
			{
				colNo   = clist[i].colNo;
				colType = m_attrInfo[colNo].type;
				
				if(colType == OOSQL_TYPE_STRING || colType == OOSQL_TYPE_VARSTRING)
				{
					ptr = clist[i].data.ptr;
					e = m_largeTemporaryObject.ReadData(posHigh, posLow, sizeof(OOSQL_StorageManager::ColListStruct), &clist[i]);
					OOSQL_CHECK_ERR(e);

					e = m_largeTemporaryObject.AddOffset(posHigh, posLow, sizeof(OOSQL_StorageManager::ColListStruct), posHigh, posLow);
					OOSQL_CHECK_ERR(e);

					clist[i].data.ptr = ptr;

					if(!clist[i].nullFlag)
					{
						e = m_largeTemporaryObject.ReadData(posHigh, posLow, clist[i].dataLength, clist[i].data.ptr);
						OOSQL_CHECK_ERR(e);

						e = m_largeTemporaryObject.AddOffset(posHigh, posLow, clist[i].dataLength, posHigh, posLow);
						OOSQL_CHECK_ERR(e);
					}
				}
				else
				{
					e = m_largeTemporaryObject.ReadData(posHigh, posLow, sizeof(OOSQL_StorageManager::ColListStruct), &clist[i]);
					OOSQL_CHECK_ERR(e);

					e = m_largeTemporaryObject.AddOffset(posHigh, posLow, sizeof(OOSQL_StorageManager::ColListStruct), posHigh, posLow);
					OOSQL_CHECK_ERR(e);
				}
			}
		}

		return eNOERROR;
	}
	else
		return EOS;
}

inline Four OOSQL_SortStream::FastEncodedCreateObject()
{
	Four			e;
	Four			offset;
	UFour			posHigh;
	UFour			posLow;
	Four			i;
	char*			p;
	Four			size;

	e = m_largeTemporaryObject.GetDataSize(posHigh, posLow);
	OOSQL_CHECK_ERR(e);
	
	p = m_keyBuffer + m_offsetToLargeTemporaryObjectOffset;
	memcpy(p, &posHigh, sizeof(UFour)); p += sizeof(UFour);
	memcpy(p, &posLow,  sizeof(UFour));

	for(i = 0; i < m_nFastEncodingInfos; i++)
	{
		size = *m_fastEncodingInfos[i].size;

        if(*m_fastEncodingInfos[i].nullFlag)		    
			e = m_largeTemporaryObject.AppendAndGetWriteDataPtr(sizeof(Four) * 2, (void**)&p);
        else
            e = m_largeTemporaryObject.AppendAndGetWriteDataPtr(sizeof(Four) * 2 + size, (void**)&p);
		OOSQL_CHECK_ERR(e);
		memcpy(p, m_fastEncodingInfos[i].nullFlag, sizeof(Four)); p += sizeof(Four);
		memcpy(p, &size, sizeof(Four)); p += sizeof(Four);
		if(!*m_fastEncodingInfos[i].nullFlag)
		{
			memcpy(p, m_fastEncodingInfos[i].ptr, size);
		}

		offset = m_keyEncodingInfo[i].offset;

		if(offset != NIL)
		{
			memset(m_keyBuffer + offset, 0, m_keyEncodingInfo[i].size);
			if(!*m_fastEncodingInfos[i].nullFlag)
			{
				memcpy(m_keyBuffer + offset, m_fastEncodingInfos[i].ptr, size);
			}
		}
	}

	e = PutTuple(&m_sortTuple);
	OOSQL_CHECK_ERR(e);

	return eNOERROR;
}

inline Four OOSQL_SortStream::FastEncodedNextScan()
{
	Four				e;
	UFour				posHigh, posLow;
	char*				p;
	Four				size;
	Four				totalSize;
	const int			OBJECT_SIZE = 16384;
	Four				i;

	e = GetTuple(&m_sortTuple);
	OOSQL_CHECK_ERR(e);

	if(e != EOS)
	{
		p = m_keyBuffer + m_offsetToLargeTemporaryObjectOffset;
		memcpy(&posHigh, p, sizeof(UFour)); p += sizeof(UFour);
		memcpy(&posLow,  p, sizeof(UFour));

		e = m_largeTemporaryObject.GetReadDataPtr(posHigh, posLow, OBJECT_SIZE, (void**)&p);
		OOSQL_CHECK_ERR(e);

		totalSize = 0;

		for(i = 0; i < m_nFastEncodingInfos; i++)
		{
			memcpy(m_fastEncodingInfos[i].nullFlag, p, sizeof(Four)); p += sizeof(Four);
			memcpy(&size, p, sizeof(Four)); p += sizeof(Four);
			if(!*m_fastEncodingInfos[i].nullFlag)
			{
				memcpy(m_fastEncodingInfos[i].ptr, p, size); 
				p += size;
			}
			else
			{
				size = 0;
			}
			*m_fastEncodingInfos[i].size = size;
			totalSize += size + sizeof(Four) * 2;
		}

		if(totalSize < OBJECT_SIZE)
			return eNOERROR;
		else
		{
			e = m_largeTemporaryObject.GetReadDataPtr(posHigh, posLow, totalSize, (void**)&p);
			OOSQL_CHECK_ERR(e);

			for(i = 0; i < m_nFastEncodingInfos; i++)
			{
				memcpy(m_fastEncodingInfos[i].nullFlag, p, sizeof(Four)); p += sizeof(Four);
				memcpy(&size, p, sizeof(Four)); p += sizeof(Four);
				if(!*m_fastEncodingInfos[i].nullFlag)
				{
					memcpy(m_fastEncodingInfos[i].ptr, p, size); 
					p += size;
				}
				*m_fastEncodingInfos[i].size = size;
			}

			return eNOERROR;
		}
	}
	else
		return EOS;
}

#endif

