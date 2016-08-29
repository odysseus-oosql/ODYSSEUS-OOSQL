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

#ifndef _OOSQL_LARGE_TEMPORARY_FILE_H_
#define _OOSQL_LARGE_TEMPORARY_FILE_H_

#include "OOSQL_Common.h"
#include "OOSQL_MemoryManager.hxx"
#include "OOSQL_MemoryManagedObject.hxx"
#include "OOSQL_StorageManager.hxx"
#include "OOSQL_Error.h"

#define OOSQL_LTO_MAXOBJECTSIZE LONG_MAX

class OOSQL_LargeTemporaryObject : public OOSQL_MemoryManagedObject {
public:
	OOSQL_LargeTemporaryObject(OOSQL_StorageManager* storageManager);
	OOSQL_LargeTemporaryObject(OOSQL_MemoryManager* memoryManager, OOSQL_StorageManager* storageManager);

	~OOSQL_LargeTemporaryObject();

	Four CreateObject(Four volID);
	Four DestroyObject();

	Four GetDataSize(UFour& sizeHigh, UFour& sizeLow);
	Four AppendData(UFour size, void* data);
	Four ReadData(UFour posHigh, UFour posLow, UFour size, void* data);
	Four WriteData(UFour posHigh, UFour posLow, UFour size, void* data);
	Four AddOffset(UFour posHigh, UFour posLow, UFour offset, UFour& dataHigh, UFour& dataLow);
	Four AppendAndGetWriteDataPtr(UFour size, void** data);
	Four GetReadDataPtr(UFour posHigh, UFour posLow, UFour size, void** data);

private:
	Four CreateObjectIfNotExist(Four i);	

	Four CreateBuffer();
	Four DestroyBuffer();
	Four WriteBuffer(UFour posHigh, UFour posLow, UFour size, void* data);
	Four ReadBuffer(UFour posHigh, UFour posLow, UFour size, void* data);

	Four FlushBufferToDisk();
	Four LoadBufferFromDisk(UFour posHigh, UFour posLow, UFour size, void* data);
	Four CreateAndOpenTemporaryClass();
	Four CloseAndDestroyTemporaryClass();

	char						m_temporaryTableName[MAXCLASSNAME];
	Four						m_ocn;				// large temporary tableÀÇ open class number
	UFour						m_sizeHigh;			
	UFour						m_sizeLow;			
	UFour						m_realSizeHigh;		
	UFour						m_realSizeLow;	
	Four						m_volID;		
	Four						m_nObjectIDs;		
	OOSQL_StorageManager::OID*  m_objectIDs;	
	OOSQL_StorageManager*		m_storageManager;
	char*						m_buffer;			
	Four						m_writeBufferSize;	
	Four						m_readBufferSize;	
	Four						m_bufferOffsetHigh;	
	Four						m_bufferOffsetLow;
	Boolean						m_bufferFlushed;	
};

inline Four OOSQL_LargeTemporaryObject::AddOffset(UFour posHigh, UFour posLow, UFour offset, UFour& dataHigh, UFour& dataLow)
{
	if(OOSQL_LTO_MAXOBJECTSIZE - posLow > offset)
	{
		dataHigh = posHigh;
		dataLow  = posLow + offset;
	}
	else
	{
		dataHigh = posHigh + 1; 
		dataLow  = offset - (OOSQL_LTO_MAXOBJECTSIZE - posLow);
	}

	return eNOERROR;
}

inline Four OOSQL_LargeTemporaryObject::WriteBuffer(UFour posHigh, UFour posLow, UFour size, void* data)
{
	Four e;

	if(posHigh == m_bufferOffsetHigh && m_bufferOffsetLow <= posLow && posLow + size <= m_bufferOffsetLow + m_writeBufferSize)
	{
		memcpy(m_buffer + (posLow - m_bufferOffsetLow), data, size);
	}
	else
	{
		e = FlushBufferToDisk();
		OOSQL_CHECK_ERR(e);
				
		m_bufferOffsetHigh = posHigh;
		m_bufferOffsetLow  = posLow;
		m_bufferFlushed    = SM_FALSE;

		memcpy(m_buffer + (posLow - m_bufferOffsetLow), data, size);
	}

	return eNOERROR;
}

inline Four OOSQL_LargeTemporaryObject::ReadBuffer(UFour posHigh, UFour posLow, UFour size, void* data)
{
	Four e;

	if(posHigh == m_bufferOffsetHigh && m_bufferOffsetLow <= posLow && posLow + size <= m_bufferOffsetLow + m_readBufferSize)
	{
		memcpy(data, m_buffer + (posLow - m_bufferOffsetLow), size);
	}
	else
	{
		e = FlushBufferToDisk();
		OOSQL_CHECK_ERR(e);
				
		m_bufferOffsetHigh = posHigh;
		m_bufferOffsetLow  = posLow;
		m_bufferFlushed    = SM_TRUE;

		e = LoadBufferFromDisk(m_bufferOffsetHigh, m_bufferOffsetLow, size, data);
		OOSQL_CHECK_ERR(e);
		
		memcpy(data, m_buffer + (posLow - m_bufferOffsetLow), size);
	}

	return eNOERROR;
}


inline Four OOSQL_LargeTemporaryObject::AppendData(UFour size, void* data)
{
	Four e;

	e = WriteBuffer(m_sizeHigh, m_sizeLow, size, data);
	OOSQL_CHECK_ERR(e);

	AddOffset(m_sizeHigh, m_sizeLow, size, m_sizeHigh, m_sizeLow);

	return eNOERROR;
}

inline Four OOSQL_LargeTemporaryObject::AppendAndGetWriteDataPtr(UFour size, void** data)
{
	Four e;

	if(m_sizeHigh == m_bufferOffsetHigh && m_bufferOffsetLow <= m_sizeLow && m_sizeLow + size <= m_bufferOffsetLow + m_writeBufferSize)
	{
		*data = m_buffer + (m_sizeLow - m_bufferOffsetLow);

		AddOffset(m_sizeHigh, m_sizeLow, size, m_sizeHigh, m_sizeLow);

		return eNOERROR;
	}
	else
	{
		e = FlushBufferToDisk();
		OOSQL_CHECK_ERR(e);
				
		m_bufferOffsetHigh = m_sizeHigh;
		m_bufferOffsetLow  = m_sizeLow;
		m_bufferFlushed    = SM_FALSE;

		*data = m_buffer + (m_sizeLow - m_bufferOffsetLow);

		AddOffset(m_sizeHigh, m_sizeLow, size, m_sizeHigh, m_sizeLow);

		return eNOERROR;
	}
}

inline Four OOSQL_LargeTemporaryObject::GetReadDataPtr(UFour posHigh, UFour posLow, UFour size, void** data)
{
	Four								e;
	OOSQL_StorageManager::ColListStruct clist[1];

	if(posHigh == m_bufferOffsetHigh && m_bufferOffsetLow <= posLow && posLow + size <= m_bufferOffsetLow + m_readBufferSize)
	{
		*data = m_buffer + (posLow - m_bufferOffsetLow);

		return eNOERROR;
	}
	else
	{
		e = FlushBufferToDisk();
		OOSQL_CHECK_ERR(e);
				
		m_bufferOffsetHigh = posHigh;
		m_bufferOffsetLow  = posLow;
		m_bufferFlushed    = SM_TRUE;

		e = LoadBufferFromDisk(m_bufferOffsetHigh, m_bufferOffsetLow, size, data);
		OOSQL_CHECK_ERR(e);
		
		*data = m_buffer + (posLow - m_bufferOffsetLow);

		return eNOERROR;
	}
}

inline Four OOSQL_LargeTemporaryObject::GetDataSize(UFour& sizeHigh, UFour& sizeLow)
{
	sizeHigh = m_sizeHigh;
	sizeLow  = m_sizeLow;

	return eNOERROR;
}

inline Four OOSQL_LargeTemporaryObject::ReadData(UFour posHigh, UFour posLow, UFour size, void* data)
{
	Four								e;

	e = ReadBuffer(posHigh, posLow, size, data);
	OOSQL_CHECK_ERR(e);

	return eNOERROR;
}

inline Four OOSQL_LargeTemporaryObject::WriteData(UFour posHigh, UFour posLow, UFour size, void* data)
{
	Four	e;
	UFour	high, low;

	e = WriteBuffer(posHigh, posLow, size, data);
	OOSQL_CHECK_ERR(e);

	e = AddOffset(posHigh, posLow, size, high, low);
	OOSQL_CHECK_ERR(e);
	
	if(m_sizeHigh > high)
	{
		m_sizeHigh = high;
		m_sizeLow  = low;
	}
	else if(m_sizeHigh == high && low > m_sizeLow)
	{
		m_sizeLow = low;
	}

	return eNOERROR;
}


#endif // _OOSQL_LARGE_TEMPORARY_FILE_H_
