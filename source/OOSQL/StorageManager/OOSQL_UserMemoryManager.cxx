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

#include "OOSQL_UserMemoryManager.hxx"

OOSQL_UserMemoryManager::OOSQL_UserMemoryManager()
{
	m_userMemory			= NULL;
	m_userMemorySize		= 0;
	m_allocationTable		= NULL;
	m_allocationTableSize	= 0;
	m_freeTable				= NULL;
	m_freeTableSize			= 0;
}

OOSQL_UserMemoryManager::OOSQL_UserMemoryManager(OOSQL_MemoryManager* memoryManager)
                        :OOSQL_MemoryManagedObject(memoryManager)
{
	m_userMemory			= NULL;
	m_userMemorySize		= 0;
	m_allocationTable		= NULL;
	m_allocationTableSize	= 0;
	m_freeTable				= NULL;
	m_freeTableSize			= 0;
}

OOSQL_UserMemoryManager::~OOSQL_UserMemoryManager()
{
	if(m_allocationTable)
		pMemoryManager->Free(m_allocationTable);
	if(m_freeTable)
		pMemoryManager->Free(m_freeTable);
}

#define OOSQL_USER_MEMORY_MANAGER_INITIAL_TABLE_SIZE	20

Four OOSQL_UserMemoryManager::SetupUserMemory(void* p, Four size)
{
	Four i;

	if(!m_allocationTable)
	{
		m_allocationTableSize = OOSQL_USER_MEMORY_MANAGER_INITIAL_TABLE_SIZE;
		m_allocationTable     = (AllocationElement*)pMemoryManager->Alloc(sizeof(AllocationElement) * m_allocationTableSize);
	}
	
	if(!m_freeTable)
	{
		m_freeTableSize = OOSQL_USER_MEMORY_MANAGER_INITIAL_TABLE_SIZE;
		m_freeTable     = (FreeElement*)pMemoryManager->Alloc(sizeof(FreeElement) * m_freeTableSize);
	}
	
	for(i = 0; i < m_allocationTableSize; i++)
	{
		m_allocationTable[i].ptr  = NULL;
		m_allocationTable[i].size = NIL;
	}

	for(i = 0; i < m_freeTableSize; i++)
	{
		m_freeTable[i].ptr  = NULL;
		m_freeTable[i].size = NIL;
	}

	m_userMemory     = p;
	m_userMemorySize = size;

	m_freeTable[0].size = m_userMemorySize;
	m_freeTable[0].ptr  = m_userMemory;

	m_usedSize = 0;
	m_freeSize = m_userMemorySize;

	return eNOERROR;
}

void* OOSQL_UserMemoryManager::Alloc(Four size)
{
	Four	allocIndex, freeIndex;
	void*	p;

	freeIndex = FindFreeTableEntry(size);
	if(freeIndex == NIL)
	{
		OOSQL_PRTERR(eMEMORYALLOCERR_OOSQL);
		return NULL;
	}

	allocIndex = FindFreeAllocationTableEntry();

	p = m_freeTable[freeIndex].ptr;

	m_freeTable[freeIndex].ptr   = (char*)p + size;
	m_freeTable[freeIndex].size -= size;

	m_allocationTable[allocIndex].ptr  = p;
	m_allocationTable[allocIndex].size = size;

	m_usedSize += size;
	m_freeSize -= size;

	return p;
}

void  OOSQL_UserMemoryManager::Free(void* p)
{
	Four	allocIndex, freeIndex;
	Four	size;

	allocIndex = FindAllocationTableEntry(p);
	if(allocIndex == NIL)		
		return;

	size      = m_allocationTable[allocIndex].size;
	freeIndex = FindAdjacentFreeTableEntry(m_allocationTable[allocIndex].ptr, 
		                                   m_allocationTable[allocIndex].size);
	if(freeIndex == NIL)
	{
		freeIndex = FindFreeFreeTableEntry();
		if(freeIndex == NIL)	
			return;
		m_freeTable[freeIndex].ptr  = m_allocationTable[allocIndex].ptr;
		m_freeTable[freeIndex].size = m_allocationTable[allocIndex].size;
	}
	else
	{
		AppendFreeSpaceToFreeTableEntry(freeIndex, m_allocationTable[allocIndex].ptr, 
		                                           m_allocationTable[allocIndex].size);
	}

	m_allocationTable[allocIndex].ptr  = NULL;
	m_allocationTable[allocIndex].size = NIL;

	m_usedSize -= size;
	m_freeSize += size;
}

void* OOSQL_UserMemoryManager::Realloc(void* p, Four size)
{
	void* tmp;

	tmp = Alloc(size);
	memcpy(tmp, p, size);
	Free(p);

	return tmp;
}

Four  OOSQL_UserMemoryManager::Resize(void* p, Four size)
{
	Four	allocIndex, freeIndex;
	void*	freePtr;
	Four	freeSize;

	allocIndex = FindAllocationTableEntry(p);
	if(allocIndex == NIL)							
		return eRESIZE_MEMORY_BLOCK_FAIL_OOSQL;

	if(m_allocationTable[allocIndex].size < size)	
		return eRESIZE_MEMORY_BLOCK_FAIL_OOSQL;					

	freeSize = m_allocationTable[allocIndex].size - size;
	freePtr  = m_allocationTable[allocIndex].ptr;
	freePtr  = (char*)freePtr + size;

	m_allocationTable[allocIndex].size -= size;

	freeIndex = FindAdjacentFreeTableEntry(freePtr, freeSize);
	if(freeIndex == NIL)
	{
		freeIndex = FindFreeFreeTableEntry();
		if(freeIndex == NIL)	
			return eNOERROR;
		m_freeTable[freeIndex].ptr  = freePtr;
		m_freeTable[freeIndex].size = freeSize;
	}
	else
	{
		AppendFreeSpaceToFreeTableEntry(freeIndex, freePtr, freeSize);
	}

	m_usedSize -= freeSize;
	m_freeSize += freeSize;

	return eNOERROR;
}

Four  OOSQL_UserMemoryManager::FreeSize()
{
	return m_freeSize;
}

Four  OOSQL_UserMemoryManager::UsedSize()
{
	return m_usedSize;
}

Four OOSQL_UserMemoryManager::FindFreeAllocationTableEntry()
{
	Four i;
	Four freeIndex;

	for(i = 0; i < m_allocationTableSize; i++)
	{
		if(m_allocationTable[i].ptr == NULL)
			return i;
	}

	m_allocationTableSize *= 2;
	m_allocationTable = (AllocationElement*)pMemoryManager->Realloc(m_allocationTable, sizeof(AllocationElement) * m_allocationTableSize);
	freeIndex = i;
	for(; i < m_allocationTableSize; i++)
	{
		m_allocationTable[i].ptr = NULL;
		m_allocationTable[i].size = NIL;
	}

	return freeIndex;
}

Four OOSQL_UserMemoryManager::FindAllocationTableEntry(void* ptr)
{
	Four i;

	for(i = 0; i < m_allocationTableSize; i++)
	{
		if(m_allocationTable[i].ptr == ptr)
			return i;
	}

	return NIL;
}

Four OOSQL_UserMemoryManager::FindFreeFreeTableEntry()
{
	Four i;
	Four freeIndex;

	for(i = 0; i < m_freeTableSize; i++)
	{
		if(m_freeTable[i].ptr == NULL)
			return i;
	}

	m_freeTableSize *= 2;
	m_freeTable = (FreeElement*)pMemoryManager->Realloc(m_freeTable, sizeof(FreeElement) * m_freeTableSize);
	freeIndex = i;
	for(; i < m_freeTableSize; i++)
	{
		m_freeTable[i].ptr = NULL;
		m_freeTable[i].size = NIL;
	}

	return freeIndex;
}

Four OOSQL_UserMemoryManager::FindFreeTableEntry(Four size)
{
	Four i;

	for(i = 0; i < m_freeTableSize; i++)
	{
		if(m_freeTable[i].size >= size)
			return i;
	}

	return NIL;
}

Four OOSQL_UserMemoryManager::FindAdjacentFreeTableEntry(void* p, Four size)
{
	Four i;
	void* userLeftEnd;
	void* userRightEnd;
	void* leftEnd;
	void* rightEnd;

	userLeftEnd  = p;
	userRightEnd = (char*)p + size;

	for(i = 0; i < m_freeTableSize; i++)
	{
		if(m_freeTable[i].ptr)
		{
			leftEnd  = m_freeTable[i].ptr;
			rightEnd = (char*)m_freeTable[i].ptr + size;

			if(leftEnd == userRightEnd || rightEnd == userLeftEnd)
				return i;
		}
	}

	return NIL;
}

void OOSQL_UserMemoryManager::AppendFreeSpaceToFreeTableEntry(Four index, void* p, Four size)
{
	void* userLeftEnd;
	void* userRightEnd;
	void* leftEnd;
	void* rightEnd;

	userLeftEnd  = p;
	userRightEnd = (char*)p + size;

	leftEnd  = m_freeTable[index].ptr;
	rightEnd = (char*)m_freeTable[index].ptr + size;

	if(userLeftEnd == rightEnd)
	{
		m_freeTable[index].size += size;
	}
	else if(userRightEnd == leftEnd)
	{
		m_freeTable[index].ptr   = userLeftEnd;
		m_freeTable[index].size += size;
	}
}

void* OOSQL_UserMemoryManager::LargestFreeBlockPtr()
{
	Four i, index;
	Four size;
	
	size  = m_freeTable[0].size;
	index = 0;
	for(i = 0; i < m_freeTableSize; i++)
	{
		if(m_freeTable[i].size > size)
		{
			size = m_freeTable[i].size;
			index = i;
		}
	}

	return m_freeTable[index].ptr;
}

Four  OOSQL_UserMemoryManager::LargestFreeBlockSize()
{
	Four i, index;
	Four size;
	
	size  = m_freeTable[0].size;
	index = 0;
	for(i = 0; i < m_freeTableSize; i++)
	{
		if(m_freeTable[i].size > size)
		{
			size = m_freeTable[i].size;
			index = i;
		}
	}

	return m_freeTable[index].size;
}
