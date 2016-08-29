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

#include "OOSQL_LargeTemporaryObject.hxx"

#ifndef SUPPORT_LARGE_DATABASE2
#define OOSQL_LTO_BUFFERSIZE	((unsigned long)(20 * 1024 * 1024))	
#else
#define OOSQL_LTO_BUFFERSIZE	((unsigned long)(40 * 1024 * 1024))	
#endif
#define OOSQL_LTO_READBUFFERSIZE (4096)						


OOSQL_LargeTemporaryObject::OOSQL_LargeTemporaryObject(OOSQL_StorageManager* storageManager)
{
	strcpy(m_temporaryTableName, "");

	m_volID				= NIL;
	m_sizeHigh			= 0;
	m_sizeLow			= 0;
	m_realSizeHigh		= 0;
	m_realSizeLow		= 0;
	m_nObjectIDs		= 0;
	m_objectIDs			= NULL;
	m_storageManager	= storageManager;
	m_ocn				= NIL;
	m_buffer            = NULL;
	m_writeBufferSize   = 0;
	m_readBufferSize    = 0;
	m_bufferOffsetHigh  = 0;
	m_bufferOffsetLow   = 0;
	m_bufferFlushed     = SM_FALSE;
}

OOSQL_LargeTemporaryObject::OOSQL_LargeTemporaryObject(OOSQL_MemoryManager* memoryManager, OOSQL_StorageManager* storageManager)
						   :OOSQL_MemoryManagedObject(memoryManager)
{
	strcpy(m_temporaryTableName, "");

	m_volID				= NIL;
	m_sizeHigh			= 0;
	m_sizeLow			= 0;
	m_realSizeHigh		= 0;
	m_realSizeLow		= 0;
	m_nObjectIDs		= 0;
	m_objectIDs			= NULL;
	m_storageManager	= storageManager;
	m_ocn				= NIL;
	m_buffer			= NULL;
	m_writeBufferSize   = 0;
	m_readBufferSize    = 0;
	m_bufferOffsetHigh  = 0;
	m_bufferOffsetLow   = 0;
	m_bufferFlushed     = SM_FALSE;
}

OOSQL_LargeTemporaryObject::~OOSQL_LargeTemporaryObject()
{
	Four e;
	Four i;

	if(m_objectIDs && m_ocn != NIL)
	{
		for(i = 0; i < m_nObjectIDs; i++)
		{
			e = m_storageManager->DestroyObject(m_ocn, SM_FALSE, &m_objectIDs[i]);
			if(e < eNOERROR) OOSQL_ERR_EXIT(e);
		}
		pMemoryManager->Free(m_objectIDs);
	}
	m_nObjectIDs		= 0;
	m_objectIDs			= NULL;

	e = CloseAndDestroyTemporaryClass();
	if(e < eNOERROR) OOSQL_ERR_EXIT(e);

	if(m_buffer)
		pMemoryManager->Free(m_buffer);
}

Four OOSQL_LargeTemporaryObject::CreateObject(Four volID)
{
	Four e;

	if(strcmp(m_temporaryTableName, ""))
		return eLARGETEMPOBJECT_ALREADY_CREATED_OOSQL;

	m_volID = volID;

	e = CreateBuffer();
	OOSQL_CHECK_ERR(e);


	return eNOERROR;
}

Four OOSQL_LargeTemporaryObject::CreateAndOpenTemporaryClass()
{
	Four e;
	Four counter;
	Four classId;
	OOSQL_StorageManager::AttrInfo attrInfo[1];

	if(m_ocn != NIL)
		return eNOERROR;

	attrInfo[0].complexType		= SM_COMPLEXTYPE_BASIC;
	attrInfo[0].type			= SM_VARSTRING;
	attrInfo[0].inheritedFrom	= NIL;
	attrInfo[0].length			= OOSQL_LTO_MAXOBJECTSIZE - 1;
	attrInfo[0].offset			= 0;
	attrInfo[0].domain          = NIL;
	strcpy(attrInfo[0].name, "largeobject");

	counter = 0;

	while(1)
	{
		sprintf(m_temporaryTableName, "largeTemporaryObject%ld", counter);

		e = m_storageManager->CreateClass(m_volID, m_temporaryTableName, NULL, NULL, 1, attrInfo, 0, NULL, 0, NULL, SM_TRUE, &classId);
		if(e == eNOERROR)
			break;
		else if(e == eCLASSDUPLICATED_OOSQL)
		{
			counter ++;
			continue;
		}
		else
		{
		    sprintf(m_temporaryTableName, "");
			OOSQL_CHECK_ERR(e);
        }
	}

	e = m_storageManager->OpenClass(m_volID, classId);
	OOSQL_CHECK_ERR(e);
	m_ocn = e;

	return eNOERROR;
}

Four OOSQL_LargeTemporaryObject::CloseAndDestroyTemporaryClass()
{
	Four e;

	if(m_ocn != NIL)
	{
		e = m_storageManager->CloseClass(m_ocn);
		if(e < eNOERROR) OOSQL_ERR_EXIT(e);

		m_ocn = NIL;
	}

	if(strcmp(m_temporaryTableName, ""))
	{
		e = m_storageManager->DestroyClass(m_volID, m_temporaryTableName);
		OOSQL_CHECK_ERR(e);
		strcpy(m_temporaryTableName, "");
	}

	return eNOERROR;
}

Four OOSQL_LargeTemporaryObject::DestroyObject()
{
	Four e;
	Four i;

	if(m_objectIDs && m_ocn != NIL)
	{
		for(i = 0; i < m_nObjectIDs; i++)
		{
			e = m_storageManager->DestroyObject(m_ocn, SM_FALSE, &m_objectIDs[i]);
			OOSQL_CHECK_ERR(e);
		}

		pMemoryManager->Free(m_objectIDs);
	}
	m_nObjectIDs		= 0;
	m_objectIDs			= NULL;

	e = CloseAndDestroyTemporaryClass();
	OOSQL_CHECK_ERR(e);

	e = DestroyBuffer();
	OOSQL_CHECK_ERR(e);

	m_volID				= NIL;
	m_sizeHigh			= 0;
	m_sizeLow			= 0;
	m_realSizeHigh		= 0;
	m_realSizeLow		= 0;

	return eNOERROR;
}

Four OOSQL_LargeTemporaryObject::CreateObjectIfNotExist(Four i)
{
	Four								j;
	Four								e;
	OOSQL_StorageManager::ColListStruct clist[1];

	if(i < m_nObjectIDs)
		return eNOERROR;
	else
	{
		if(m_ocn == NIL)
		{
			e = CreateAndOpenTemporaryClass();
			OOSQL_CHECK_ERR(e);
		}

		if(m_objectIDs)
			m_objectIDs = (OOSQL_StorageManager::OID*)pMemoryManager->Realloc(m_objectIDs, sizeof(OOSQL_StorageManager::OID) * (i + 1));
		else
			m_objectIDs = (OOSQL_StorageManager::OID*)pMemoryManager->Alloc(sizeof(OOSQL_StorageManager::OID) * (i + 1));

		clist[0].colNo		= GET_SYSTEMLEVEL_COLNO(0);
		clist[0].start		= ALL_VALUE;
		clist[0].length		= ALL_VALUE;
		clist[0].nullFlag	= SM_FALSE;
		clist[0].data.ptr	= (void*)"";
		clist[0].dataLength	= 0;

		for(j = m_nObjectIDs; j < i + 1; j++)
		{
			e = m_storageManager->CreateObjectByColList(m_ocn, SM_FALSE, 1, clist, &m_objectIDs[j]);
			OOSQL_CHECK_ERR(e);
		}
		m_nObjectIDs = i + 1;

		return eNOERROR;
	}
}

Four OOSQL_LargeTemporaryObject::CreateBuffer()
{
	Four e;

	if(m_buffer)
	{
		e = DestroyBuffer();
		OOSQL_CHECK_ERR(e);
	}

	m_buffer           = (char*)pMemoryManager->Alloc(OOSQL_LTO_BUFFERSIZE);
	m_writeBufferSize  = OOSQL_LTO_BUFFERSIZE;
	m_readBufferSize   = OOSQL_LTO_BUFFERSIZE;
	m_bufferOffsetHigh = 0;
	m_bufferOffsetLow  = 0;
	m_bufferFlushed    = SM_FALSE;

	return eNOERROR;
}

Four OOSQL_LargeTemporaryObject::DestroyBuffer()
{
	if(m_buffer)
		pMemoryManager->Free(m_buffer);

	m_buffer			= NULL;
	m_writeBufferSize   = 0;
	m_readBufferSize    = 0;
	m_bufferOffsetHigh  = 0;
	m_bufferOffsetLow   = 0;
	m_bufferFlushed     = SM_FALSE;

	return eNOERROR;
}

Four OOSQL_LargeTemporaryObject::FlushBufferToDisk()
{
	Four								e;
	OOSQL_StorageManager::ColListStruct clist[1];
	UFour								high, low;

	if(m_bufferFlushed == SM_FALSE)
	{
		if(OOSQL_LTO_MAXOBJECTSIZE - m_bufferOffsetLow > m_writeBufferSize)
		{
			clist[0].colNo		= GET_SYSTEMLEVEL_COLNO(0);
			clist[0].start		= m_bufferOffsetLow;
			clist[0].nullFlag	= SM_FALSE;
			clist[0].data.ptr	= m_buffer;
			clist[0].dataLength	= m_writeBufferSize;

			if(m_bufferOffsetLow < m_realSizeLow)
				clist[0].length = m_realSizeLow - m_bufferOffsetLow;
			else
				clist[0].length = 0;

			e = CreateObjectIfNotExist(m_bufferOffsetHigh);
			OOSQL_CHECK_ERR(e);

			e = m_storageManager->UpdateObjectByColList(m_ocn, SM_FALSE, &m_objectIDs[m_bufferOffsetHigh], 1, clist);
			OOSQL_CHECK_ERR(e);
		}
		else
		{
			clist[0].colNo		= GET_SYSTEMLEVEL_COLNO(0);
			clist[0].start		= m_bufferOffsetLow;
			clist[0].length		= 0;
			clist[0].nullFlag	= SM_FALSE;
			clist[0].data.ptr	= m_buffer;
			clist[0].dataLength	= OOSQL_LTO_MAXOBJECTSIZE - m_bufferOffsetLow;

			if(m_bufferOffsetHigh == m_realSizeHigh)
			{
				if(m_bufferOffsetLow < m_realSizeLow)
					clist[0].length = m_realSizeLow - m_bufferOffsetLow;
				else
					clist[0].length = 0;
			}
			else if(m_bufferOffsetHigh < m_realSizeHigh)
				clist[0].length = clist[0].dataLength;
			else
				clist[0].length = 0;

			e = CreateObjectIfNotExist(m_bufferOffsetHigh);
			OOSQL_CHECK_ERR(e);

			e = m_storageManager->UpdateObjectByColList(m_ocn, SM_FALSE, &m_objectIDs[m_bufferOffsetHigh], 1, clist);
			OOSQL_CHECK_ERR(e);
			
			clist[0].colNo		= GET_SYSTEMLEVEL_COLNO(0);
			clist[0].start		= 0;
			clist[0].length		= 0;
			clist[0].nullFlag	= SM_FALSE;
			clist[0].data.ptr	= (char*)m_buffer + (OOSQL_LTO_MAXOBJECTSIZE - m_bufferOffsetLow);
			clist[0].dataLength	= m_writeBufferSize - (OOSQL_LTO_MAXOBJECTSIZE - m_bufferOffsetLow);

			if(m_bufferOffsetHigh == m_realSizeHigh)
				clist[0].length = 0;
			else if(m_bufferOffsetHigh + 1 == m_realSizeHigh)
			{
				if(clist[0].dataLength < m_realSizeLow)
					clist[0].length = clist[0].dataLength;
				else
					clist[0].length = m_realSizeLow;
			}
			else if(m_bufferOffsetHigh + 1 < m_realSizeHigh)
				clist[0].length = clist[0].dataLength;
			else
				clist[0].length = 0;

			e = CreateObjectIfNotExist(m_bufferOffsetHigh + 1);
			OOSQL_CHECK_ERR(e);

			e = m_storageManager->UpdateObjectByColList(m_ocn, SM_FALSE, &m_objectIDs[m_bufferOffsetHigh + 1], 1, clist);
			OOSQL_CHECK_ERR(e);
		}
	}
	
	m_bufferFlushed  = SM_TRUE;
	m_readBufferSize = m_writeBufferSize;

	e = AddOffset(m_bufferOffsetHigh, m_bufferOffsetLow, m_writeBufferSize, high, low);
	OOSQL_CHECK_ERR(e);
	
	if(m_realSizeHigh < high)
	{
		m_realSizeHigh = high;
		m_realSizeLow  = low;
	}
	else if(m_realSizeHigh == high && low > m_realSizeLow)
	{
		m_realSizeLow = low;
	}

	return eNOERROR;
}

Four OOSQL_LargeTemporaryObject::LoadBufferFromDisk(UFour posHigh, UFour posLow, UFour size, void* data)
{
	Four								e;
	OOSQL_StorageManager::ColListStruct clist[1];

	if(m_ocn == NIL)
		return eLARGETEMPOBJECT_NOT_CREATED_OOSQL;

	if(size > OOSQL_LTO_READBUFFERSIZE)
		m_readBufferSize = size;
	else
		m_readBufferSize = OOSQL_LTO_READBUFFERSIZE;

	if(OOSQL_LTO_MAXOBJECTSIZE - m_bufferOffsetLow > m_readBufferSize)
	{
		clist[0].colNo		= GET_SYSTEMLEVEL_COLNO(0);
		clist[0].start		= m_bufferOffsetLow;
		clist[0].length		= m_readBufferSize;
		clist[0].nullFlag	= SM_FALSE;
		clist[0].data.ptr	= m_buffer;
		clist[0].dataLength	= m_readBufferSize;

		if(m_bufferOffsetHigh <= m_nObjectIDs)
		{
			e = m_storageManager->FetchObjectByColList(m_ocn, SM_FALSE, &m_objectIDs[m_bufferOffsetHigh], 1, clist);
			OOSQL_CHECK_ERR(e);
		}
	}
	else
	{
		clist[0].colNo		= GET_SYSTEMLEVEL_COLNO(0);
		clist[0].start		= m_bufferOffsetLow;
		clist[0].length		= OOSQL_LTO_MAXOBJECTSIZE - m_bufferOffsetLow;
		clist[0].nullFlag	= SM_FALSE;
		clist[0].data.ptr	= m_buffer;
		clist[0].dataLength	= OOSQL_LTO_MAXOBJECTSIZE - m_bufferOffsetLow;

		if(m_bufferOffsetHigh <= m_nObjectIDs)
		{
			e = m_storageManager->FetchObjectByColList(m_ocn, SM_FALSE, &m_objectIDs[m_bufferOffsetHigh], 1, clist);
			OOSQL_CHECK_ERR(e);
		}
		
		clist[0].colNo		= GET_SYSTEMLEVEL_COLNO(0);
		clist[0].start		= 0;
		clist[0].length		= m_readBufferSize - (OOSQL_LTO_MAXOBJECTSIZE - m_bufferOffsetLow);
		clist[0].nullFlag	= SM_FALSE;
		clist[0].data.ptr	= (char*)m_buffer + (OOSQL_LTO_MAXOBJECTSIZE - m_bufferOffsetLow);
		clist[0].dataLength	= m_readBufferSize - (OOSQL_LTO_MAXOBJECTSIZE - m_bufferOffsetLow);

		if(m_bufferOffsetHigh + 1 <= m_nObjectIDs)
		{
			e = m_storageManager->FetchObjectByColList(m_ocn, SM_FALSE, &m_objectIDs[m_bufferOffsetHigh + 1], 1, clist);
			OOSQL_CHECK_ERR(e);
		}
	}

	m_bufferFlushed   = SM_TRUE;
	m_writeBufferSize = m_readBufferSize;

	return eNOERROR;
}
