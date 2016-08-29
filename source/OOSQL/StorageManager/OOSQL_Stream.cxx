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

#include "OOSQL_Stream.hxx"

#define OOSQL_STREAM_BUFFER_SIZE	(2 * 1024 * 1024)
#define OOSQL_STREAM_OUT_SIZE		(2048)

OOSQL_Stream::OOSQL_Stream(OOSQL_StorageManager* storageManager)
{
	m_buffer         = NULL;
	m_bufferSize     = 0;
	m_bufferOffset   = 0;
	m_volID			 = NIL;
	m_streamID       = NIL;
	m_storageManager = storageManager;
}

OOSQL_Stream::OOSQL_Stream(OOSQL_MemoryManager* memoryManager, OOSQL_StorageManager* storageManager)
             :OOSQL_MemoryManagedObject(memoryManager)
{
	m_buffer         = NULL;
	m_bufferSize     = 0;
	m_bufferOffset   = 0;
	m_volID			 = NIL;
	m_streamID       = NIL;
	m_storageManager = storageManager;
}

OOSQL_Stream::~OOSQL_Stream()
{
	DestroyStream();
}

Four OOSQL_Stream::CreateStream(Four volID)
{
	Four e;

	if(!m_buffer)
	{
		m_buffer     = (char*)pMemoryManager->Alloc(OOSQL_STREAM_BUFFER_SIZE);
		m_bufferSize = OOSQL_STREAM_BUFFER_SIZE;
	}

	m_bufferOffset = 0;
	
	if(m_streamID != NIL)
	{
		e = m_storageManager->CloseStream(m_streamID);
		OOSQL_CHECK_ERR(e);
	}

	m_volID = volID;
	e = m_storageManager->OpenStream(m_volID);
	OOSQL_CHECK_ERR(e);
	m_streamID = e;

	return eNOERROR;
}

Four OOSQL_Stream::DestroyStream()
{
	Four e;

	if(m_buffer)
		pMemoryManager->Free(m_buffer);
	m_buffer         = NULL;
	m_bufferSize     = 0;
	m_bufferOffset   = 0;

	if(m_streamID != -1)
	{
		e = m_storageManager->CloseStream(m_streamID);
		OOSQL_CHECK_ERR(e);
	}

	m_volID          = NIL;
	m_streamID       = NIL;

	return eNOERROR;
}

Four OOSQL_Stream::PutData(void* p, Four size)
{
	Four freeSize;
	Four size1, size2;
	Four e;

	freeSize = m_bufferSize - m_bufferOffset;

	if(size <= freeSize)
	{
		memcpy(m_buffer + m_bufferOffset, p, size);
		m_bufferOffset += size;
	}
	else
	{
		size1 = freeSize;
		size2 = size - freeSize;

		memcpy(m_buffer + m_bufferOffset, p, size1);
		m_bufferOffset += size1;

		e = FlushBuffer();
		OOSQL_CHECK_ERR(e);

		memcpy(m_buffer, (char*)p + size1, size2);
		m_bufferOffset = size2;
	}

	return eNOERROR;
}

Four OOSQL_Stream::GetData(void* p, Four size)
{
	Four dataSize;
	Four size1, size2;
	Four e;

	dataSize = m_bufferSize - m_bufferOffset;

	if(size <= dataSize)
	{
		memcpy(p, m_buffer + m_bufferOffset, size);
		m_bufferOffset += size;
	}
	else
	{
		size1 = dataSize;
		size2 = size - dataSize;
		
		if(size1)
			memcpy(p, m_buffer + m_bufferOffset, size1);

		e = LoadBuffer();
		OOSQL_CHECK_ERR(e);

		if(size1 == 0 && m_bufferSize == 0 && e == EOS)
			return EOS;
		
		if(size2 < m_bufferSize)
		{
			memcpy((char*)p + size1, m_buffer, size2);
			m_bufferOffset = size2;
		}
		else
		{
			memcpy((char*)p + size1, m_buffer, m_bufferSize);
			m_bufferOffset = m_bufferSize;
		}
	}

	return eNOERROR;
}

Four OOSQL_Stream::OpenScan()
{
	Four e;

	e = FlushBuffer();
	OOSQL_CHECK_ERR(e);

	e = m_storageManager->ChangePhaseStream(m_streamID);
	OOSQL_CHECK_ERR(e);

	e = LoadBuffer();
	OOSQL_CHECK_ERR(e);

	return eNOERROR;
}

Four OOSQL_Stream::CloseScan()
{
	return eNOERROR;
}

Four OOSQL_Stream::FlushBuffer()
{
	OOSQL_StorageManager::SortStreamTuple	tuple;
	char*									p;
	Four									i;
	Four									e;

	p = m_buffer;

	for(i = 0; i < m_bufferOffset / OOSQL_STREAM_OUT_SIZE; i++)
	{
		tuple.len  = OOSQL_STREAM_OUT_SIZE;
		tuple.data = p;

		e = m_storageManager->PutTuplesIntoStream(m_streamID, 1, &tuple);
		OOSQL_CHECK_ERR(e);

		p += OOSQL_STREAM_OUT_SIZE;
	}
	
	if(m_bufferOffset % OOSQL_STREAM_OUT_SIZE)
	{
		tuple.len  = m_bufferOffset % OOSQL_STREAM_OUT_SIZE;
		tuple.data = p;

		e = m_storageManager->PutTuplesIntoStream(m_streamID, 1, &tuple);
		OOSQL_CHECK_ERR(e);
	}
	
	m_bufferOffset = 0;

	return eNOERROR;
}

Four OOSQL_Stream::LoadBuffer()
{
	OOSQL_StorageManager::SortStreamTuple	tuple;
	char*									p;
	Four									i;
	Boolean									done;				/* flag which indicates sort stream is empty or not */
    Four									numSortTuple;       /* # of sort tuple from sort stream. In this function, always 1 */
	Four									e;

	p = m_buffer;

	m_bufferOffset = 0;
	m_bufferSize   = 0;

	for(i = 0; i < OOSQL_STREAM_BUFFER_SIZE / OOSQL_STREAM_OUT_SIZE; i++)
	{
		tuple.len  = OOSQL_STREAM_OUT_SIZE;
		tuple.data = p;
		
		done = SM_FALSE;
		numSortTuple = 1;
		e = m_storageManager->GetTuplesFromStream(m_streamID, &numSortTuple, &tuple, &done);
		OOSQL_CHECK_ERR(e);

		if(done) return EOS;

		m_bufferSize += tuple.len;
		p += tuple.len;
	}

	return eNOERROR;
}
