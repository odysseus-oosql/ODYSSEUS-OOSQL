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

#include "OOSQL_StorageManager.hxx"
#include "OOSQL_SortStream.hxx"
#include "OOSQL_Eval_Util.hxx"
#include "OOSQL_Error.h"

OOSQL_SortStream::OOSQL_SortStream(OOSQL_StorageManager* storageManager)
                 :m_largeTemporaryObject(pMemoryManager, storageManager)
{
	m_volID						= NIL;
	m_sortStreamID				= NIL;
	m_attrInfo					= NULL;
	m_nAttrs					= 0;
	m_storageManager			= storageManager;
	m_useInMemorySorting		= true;
	m_sortBufferShouldBeFreed	= true;
	m_sortBuffer				= NULL;
	m_nSortTuples				= 0;
	m_fastEncodingInfos         = NULL;
}

OOSQL_SortStream::OOSQL_SortStream(OOSQL_MemoryManager* memoryManager, OOSQL_StorageManager* storageManager)
                 :OOSQL_MemoryManagedObject(memoryManager), m_largeTemporaryObject(memoryManager, storageManager)
{
	m_volID						= NIL;
	m_sortStreamID				= NIL;
	m_attrInfo					= NULL;
	m_nAttrs					= 0;
	m_storageManager			= storageManager;
	m_useInMemorySorting		= true;
	m_sortBufferShouldBeFreed	= true;
	m_sortBuffer				= NULL;
	m_nSortTuples				= 0;
	m_fastEncodingInfos         = NULL;
}

OOSQL_SortStream::~OOSQL_SortStream()
{
	if(m_sortStreamID != NIL)
		m_storageManager->CloseSortStream(m_sortStreamID);
	m_sortStreamID = NIL;

	if(m_attrInfo)
		pMemoryManager->Free(m_attrInfo);
	m_attrInfo = NULL;

	if(m_keyBuffer)
		pMemoryManager->Free(m_keyBuffer);
	m_keyBuffer = NULL;

	if(m_keyEncodingInfo)
		pMemoryManager->Free(m_keyEncodingInfo);
	m_keyEncodingInfo = NULL;

	if(m_sortBufferShouldBeFreed && m_sortBuffer)
		pMemoryManager->Free(m_sortBuffer);
	m_sortBuffer = NULL;

	if(m_fastEncodingInfos)
		pMemoryManager->Free(m_fastEncodingInfos);
	m_fastEncodingInfos = NULL;

	m_largeTemporaryObject.DestroyObject();
}

Four OOSQL_SortStream::DestroyStream()
{
	Four e;

	if(m_sortStreamID != NIL)
	{
		e = m_storageManager->CloseSortStream(m_sortStreamID);
		OOSQL_CHECK_ERR(e);
	}
	m_sortStreamID = NIL;

	if(m_attrInfo)
		pMemoryManager->Free(m_attrInfo);
	m_attrInfo = NULL;

	if(m_keyBuffer)
		pMemoryManager->Free(m_keyBuffer);
	m_keyBuffer = NULL;

	if(m_keyEncodingInfo)
		pMemoryManager->Free(m_keyEncodingInfo);
	m_keyEncodingInfo = NULL;

	if(m_sortBufferShouldBeFreed && m_sortBuffer)
		pMemoryManager->Free(m_sortBuffer);
	m_sortBuffer = NULL;

	if(m_fastEncodingInfos)
		pMemoryManager->Free(m_fastEncodingInfos);
	m_fastEncodingInfos = NULL;

	e = m_largeTemporaryObject.DestroyObject();
	OOSQL_CHECK_ERR(e);

	return eNOERROR;
}

Four OOSQL_SortStream::CreateStream(Four volID, Four nAttrs, OOSQL_StorageManager::AttrInfo* attrInfo, OOSQL_StorageManager::BTreeKeyInfo* keyInfo, void* userMemory, Four userMemorySize)
{
	Four	i, j, k;
	Two		colNo;
	Four	e;
	Four	offset;

	// preparing member attributes
	m_volID			  = volID;
	m_nAttrs		  = nAttrs;
	m_attrInfo		  = (OOSQL_StorageManager::AttrInfo*)pMemoryManager->Alloc(sizeof(OOSQL_StorageManager::AttrInfo) * nAttrs);
	m_keyEncodingInfo = (OOSQL_KeyEncodingInfo*)pMemoryManager->Alloc(sizeof(OOSQL_KeyEncodingInfo) * nAttrs);

	m_stringTypeExist = false;
	for(i = 0; i < m_nAttrs; i++)
	{
		m_attrInfo[i] = attrInfo[i];
		if(m_attrInfo[i].type == OOSQL_TYPE_STRING || m_attrInfo[i].type == OOSQL_TYPE_VARSTRING)
			m_stringTypeExist = true;
	}

	// initialize sort tuple description and create lrds level sort stream
	for(i = 0; i < m_nAttrs; i++)
	{
		m_keyEncodingInfo[i].offset = NIL;
		m_keyEncodingInfo[i].size   = NIL;
	}

	m_sortTupleDesc.hdrSize = 0;
	m_sortTupleDesc.nparts  = keyInfo->nColumns;
	offset                  = 0;	
	for(i = 0; i < keyInfo->nColumns; i++)
	{
		colNo = keyInfo->columns[i].colNo;
		switch(m_attrInfo[colNo].type)
		{
		case OOSQL_TYPE_DATE:
			m_sortTupleDesc.parts[i].type = OOSQL_TYPE_LONG;
			break;
		case OOSQL_TYPE_TIME:
		case OOSQL_TYPE_TIMESTAMP:
			m_sortTupleDesc.parts[i].type = OOSQL_TYPE_STRING;
			break;
		case OOSQL_TYPE_INTERVAL:
			m_sortTupleDesc.parts[i].type = OOSQL_TYPE_DOUBLE;
			break;
		case OOSQL_TYPE_TEXT:
			OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
		case OOSQL_TYPE_VARSTRING:
			m_sortTupleDesc.parts[i].type = OOSQL_TYPE_STRING;
			break;
		case OOSQL_TYPE_STRING:
			m_sortTupleDesc.parts[i].type = OOSQL_TYPE_STRING;
			break;
		default:
			m_sortTupleDesc.parts[i].type = m_attrInfo[colNo].type;
			break;
		}

		if(m_attrInfo[colNo].length > MAXKEYLEN)
			m_sortTupleDesc.parts[i].length = MAXKEYLEN;
		else
			m_sortTupleDesc.parts[i].length = m_attrInfo[colNo].length;

		m_sortTupleDesc.parts[i].flag = keyInfo->columns[i].flag;

		m_keyEncodingInfo[colNo].offset = offset;
		m_keyEncodingInfo[colNo].size   = m_sortTupleDesc.parts[i].length;
		offset += m_keyEncodingInfo[colNo].size;
	}

	m_keyBuffer			= (char*)pMemoryManager->Alloc(offset + sizeof(Four) * 2);
	m_sortTuple.len		= offset + sizeof(Four) * 2;
	m_sortTuple.data	= (char*)m_keyBuffer;
	m_sortTupleDataLen  = m_sortTuple.len;
	m_offsetToLargeTemporaryObjectOffset = m_sortTupleDataLen - sizeof(Four) * 2;

	m_sortStreamID = NIL;

	e = m_largeTemporaryObject.CreateObject(m_volID);
	OOSQL_CHECK_ERR(e);

	// create in-memory sort buffer
	if(userMemory == NULL || (userMemory && userMemorySize < OOSQL_DEFAULT_INMEMORY_SORTBUFFER_FOR_SORTSTREAM))
	{
		m_sortBufferSize		  = OOSQL_DEFAULT_INMEMORY_SORTBUFFER_FOR_SORTSTREAM;
		m_sortBuffer			  = (char*)pMemoryManager->Alloc(m_sortBufferSize);
		m_sortBufferShouldBeFreed = true;
	}
	else
	{
		m_sortBufferSize		  = userMemorySize;
		m_sortBuffer			  = (char*)userMemory;
		m_sortBufferShouldBeFreed = false;
	}

	m_sortBufferFreeSize = m_sortBufferSize;
	m_sortBufferTail     = m_sortBuffer;
	m_nSortTuples        = 0;

	return eNOERROR;
}

Four OOSQL_SortStream::Sort()
{
	Four e;
	
	e = SortTuples();
	OOSQL_CHECK_ERR(e);

	return eNOERROR;
}

Four OOSQL_SortStream::OpenScan()
{
	/* do nothing */
	return eNOERROR;
}

Four OOSQL_SortStream::CloseScan()
{
	/* do nothing */
	return eNOERROR;
}

Four OOSQL_SortStream::SetFastEncodingInfo(Four nCols, OOSQL_FastEncodingInfo* encodingInfo)
{
	Four i;

	if(m_fastEncodingInfos)
		pMemoryManager->Free(m_fastEncodingInfos);
	m_fastEncodingInfos = NULL;

	m_nFastEncodingInfos = nCols;
	m_fastEncodingInfos  = (OOSQL_FastEncodingInfo*)pMemoryManager->Alloc(sizeof(OOSQL_FastEncodingInfo) * nCols);

	for(i = 0; i < nCols; i++)
		m_fastEncodingInfos[i] = encodingInfo[i];

	return eNOERROR;
}

Four OOSQL_SortStream::GetFastEncodingInfo(Four nCols, OOSQL_FastEncodingInfo* encodingInfo)
{
	Four i;

	for(i = 0; i < nCols; i++)
		encodingInfo[i] = m_fastEncodingInfos[i];

	return eNOERROR;
}


typedef struct {
	Four start;
	Four end;
} OOSQL_Partition;

#define TUPLES(base, width, i) ((char*)(base) + (width) * (i))
#define OOSQL_MAXSTACKDEPTH  50
#define OOSQL_SORTLIMIT 20

Four OOSQL_SortStream::QuickSort()
{
    OOSQL_Partition*	stack;						/* stack for emulating recursive call */ 
	Four				stackSize;
    Four				stackPtr = 0;				/* index of 'stack' */
    Four				curStart, curEnd;			/* variables which indicate current sorting partition */
    Four				i, j;						/* index variable */
    void*				pivot;						/* pivot which indicates split point */
    static long			rand=0;						/* varible for generating random variable */
	void*				swapBuffer;
	void				*base;
	size_t				num;
	size_t				width;

	base       = m_sortBuffer;
	num        = m_nSortTuples;
	width      = m_sortTupleDataLen;

	stack      = (OOSQL_Partition*)pMemoryManager->Alloc(OOSQL_MAXSTACKDEPTH * sizeof(OOSQL_Partition));
	swapBuffer = pMemoryManager->Alloc(width);
	pivot      = pMemoryManager->Alloc(width);
	stackSize  = OOSQL_MAXSTACKDEPTH;

    /* initialize 'curStart' & 'curEnd' */
    curStart = 0;
    curEnd   = num - 1;

    while (1) 
	{
        /* if partition size is smaller than limit value, other method will be used */
        if (curEnd - curStart < OOSQL_SORTLIMIT) 
		{
            /* insertion sort!! */
            for (i = curEnd; i > curStart; i-- ) 
			{
				memcpy(pivot, TUPLES(base, width, i), width);
                for (j = curStart; j < i; j++ ) 
				{
                    if (m_compareFunc(&m_sortTupleDesc, pivot, TUPLES(base, width, j)) < 0) 
					{
						memcpy(swapBuffer, TUPLES(base, width, j), width);
						memcpy(TUPLES(base, width, j), pivot, width);
						memcpy(pivot, swapBuffer, width);
                    }
                }
				memcpy(TUPLES(base, width, i), pivot, width);
            }

            /* stack empty */
            if (--stackPtr < 0) break;

            /* pop!! */
            curStart = stack[stackPtr].start;
            curEnd   = stack[stackPtr].end;
            continue;
        }

        /* by random number, determine pivot!! */
        rand = (rand*1103515245 +12345) & 0x7fffffff;
		memcpy(pivot, TUPLES(base, width, curStart+rand%(curEnd-curStart)), width);

        /* split!! */
        i = curStart; j = curEnd;
        while (i <= j) 
		{
            while (m_compareFunc(&m_sortTupleDesc, TUPLES(base, width, i), pivot) < 0) i++;
            while (m_compareFunc(&m_sortTupleDesc, pivot, TUPLES(base, width, j)) < 0) j--;
            if (i < j) 
			{
				memcpy(swapBuffer, TUPLES(base, width, i), width);
				memcpy(TUPLES(base, width, i), TUPLES(base, width, j), width);
				memcpy(TUPLES(base, width, j), swapBuffer, width);
            }
            if (i <= j) 
			{
                i++; j--;
            }
        }

        /* push the 'larger' partition on stack */
        if (j-curStart < curEnd-i) 
		{
            if (i < curEnd) 
			{
                stack[stackPtr].start = i;
                stack[stackPtr++].end = curEnd;

                /* check stack size and doubling if no more space left */
                if (stackPtr >= stackSize)
				{
					stackSize = stackSize * 2;
					stack     = (OOSQL_Partition*)pMemoryManager->Realloc(stack, stackSize * sizeof(OOSQL_Partition));
				}
            }
            curEnd = j;
        }
        else 
		{
            if (curStart < j) 
			{
                stack[stackPtr].start = curStart;	
                stack[stackPtr++].end = j;

				/* check stack size and doubling if no more space left */
                if (stackPtr >= stackSize)
				{
					stackSize = stackSize * 2;
					stack     = (OOSQL_Partition*)pMemoryManager->Realloc(stack, stackSize * sizeof(OOSQL_Partition));
				}
            }
            curStart = i;
        }
    }

	pMemoryManager->Free(stack);
	pMemoryManager->Free(swapBuffer);
	pMemoryManager->Free(pivot);

    return eNOERROR;
}

Four OOSQL_SortStream::MergeSort()
{
	Four e;

	// prepare merge buffer
	m_mergeBuffer = (char*)pMemoryManager->Alloc(m_nSortTuples * m_sortTupleDataLen);

	e = MergeSort(0, m_nSortTuples - 1);
	OOSQL_CHECK_ERR(e);

	// free merge buffer
	pMemoryManager->Free(m_mergeBuffer);

	return eNOERROR;
}

Four OOSQL_SortStream::MergeSort(Four lo, Four hi)
{
	Four mid;
	Four k, t_lo, t_hi;
	Four e;
	Four i, j;

	if(lo >= hi)							// recursive call stop condition
		return eNOERROR;
	else if((hi - lo) < OOSQL_SORTLIMIT)	// recursive call stop condition
	{
        for (i = hi; i > lo; i-- ) 
		{
			memcpy(TUPLES(m_mergeBuffer, m_sortTupleDataLen, 0), TUPLES(m_sortBuffer, m_sortTupleDataLen, i), m_sortTupleDataLen);
            for (j = lo; j < i; j++ ) 
			{
                if (m_compareFunc(&m_sortTupleDesc, TUPLES(m_mergeBuffer, m_sortTupleDataLen, 0), TUPLES(m_sortBuffer, m_sortTupleDataLen, j)) < 0) 
				{
					memcpy(TUPLES(m_mergeBuffer, m_sortTupleDataLen, 1), TUPLES(m_sortBuffer, m_sortTupleDataLen, j), m_sortTupleDataLen);
					memcpy(TUPLES(m_sortBuffer, m_sortTupleDataLen, j), TUPLES(m_mergeBuffer, m_sortTupleDataLen, 0), m_sortTupleDataLen);
					memcpy(TUPLES(m_mergeBuffer, m_sortTupleDataLen, 0), TUPLES(m_mergeBuffer, m_sortTupleDataLen, 1), m_sortTupleDataLen);
                }
            }
			memcpy(TUPLES(m_sortBuffer, m_sortTupleDataLen, i), TUPLES(m_mergeBuffer, m_sortTupleDataLen, 0), m_sortTupleDataLen);
        }

        return eNOERROR;
    }


	mid = (lo + hi) / 2;
	
	// by divide and conquer, sort two divided slice
	e = MergeSort(lo, mid);
	OOSQL_CHECK_ERR(e);

	e = MergeSort(mid + 1, hi);
	OOSQL_CHECK_ERR(e);

	// merge two divided slice
	t_lo = lo; 
	t_hi = mid + 1;
	for(k = lo; k <= hi; k++)
	{
		if((t_lo <= mid) && ((t_hi > hi) || 
			m_compareFunc(&m_sortTupleDesc, TUPLES(m_sortBuffer, m_sortTupleDataLen, t_lo),
			                                TUPLES(m_sortBuffer, m_sortTupleDataLen, t_hi)) < 0))
		{
			memcpy(TUPLES(m_mergeBuffer, m_sortTupleDataLen, k), TUPLES(m_sortBuffer, m_sortTupleDataLen, t_lo), m_sortTupleDataLen);
			t_lo ++;
		}
		else
		{
			memcpy(TUPLES(m_mergeBuffer, m_sortTupleDataLen, k), TUPLES(m_sortBuffer, m_sortTupleDataLen, t_hi), m_sortTupleDataLen);
			t_hi ++;
		}
	}

	// copy back merge buffer to sort buffer
	memcpy(TUPLES(m_sortBuffer, m_sortTupleDataLen, lo), TUPLES(m_mergeBuffer, m_sortTupleDataLen, lo), m_sortTupleDataLen * (hi - lo + 1));

	return eNOERROR;
}

static int CompareForGenernalCase(OOSQL_StorageManager::SortTupleDesc* sortTupleDesc, void* p1, void* p2)
{
    int                  result;         /* comparision result */
    char*                 left;           /* pointer which moves in tuple1 */
    char*                 right;          /* pointer which moves in tuple2 */
    Four                 i, j;           /* index variables */               
    Four                 kpartSize;      /* size of key part */
    Two                  len1, len2;     /* length of variable length attribute */ 
    short                s1, s2;         /* variables for 'Short' type */
    int                  i1, i2;         /* variables for 'Int' type */
    Four_Invariable      l1, l2;         /* variables for 'Long' type */
    Eight_Invariable     ll1, ll2;       /* variables for 'Long Long' type */
    float                f1, f2;         /* variables for 'float' type */
    double               d1, d2;         /* variables for 'double' type */
    OID                  oid1, oid2;            
	
    left  = (char*)p1;
    right = (char*)p2;

    /* for each key part, compare it!! */
    for (i = 0; i < sortTupleDesc->nparts; i++ ) 
	{
        switch (sortTupleDesc->parts[i].type) 
		{
          /* in case of 'Short' type */
          case OOSQL_TYPE_SHORT :

            memcpy(&s1, left, sizeof(short));
            memcpy(&s2, right, sizeof(short));

            if (sortTupleDesc->parts[i].flag & SORTKEYDESC_ATTR_ASC) {
                if (s1 > s2)      return 1;
                else if (s1 < s2) return -1;
            }
            else if (sortTupleDesc->parts[i].flag & SORTKEYDESC_ATTR_DESC) {
                if (s1 < s2)      return 1;
                else if (s1 > s2) return -1;
            }

            kpartSize = sizeof(short);

            break;


          /* in case of 'Int' type */
          case OOSQL_TYPE_INT :

            memcpy(&i1, left, sizeof(int));
            memcpy(&i2, right, sizeof(int));

            if (sortTupleDesc->parts[i].flag & SORTKEYDESC_ATTR_ASC) {
                if (i1 > i2)      return 1;
                else if (i1 < i2) return -1;
            }
            else if (sortTupleDesc->parts[i].flag & SORTKEYDESC_ATTR_DESC) {
                if (i1 < i2)      return 1;
                else if (i1 > i2) return -1;
            }

            kpartSize = sizeof(int);
		
            break;


          /* in case of 'Long' type */
          case OOSQL_TYPE_LONG :

            memcpy(&l1, left, sizeof(Four_Invariable));
            memcpy(&l2, right, sizeof(Four_Invariable));

            if (sortTupleDesc->parts[i].flag & SORTKEYDESC_ATTR_ASC) {
                if (l1 > l2)      return 1;
                else if (l1 < l2) return -1;
            }
            else if (sortTupleDesc->parts[i].flag & SORTKEYDESC_ATTR_DESC) {
                if (l1 < l2)      return 1;
                else if (l1 > l2) return -1;
            }

            kpartSize = sizeof(Four_Invariable);

            break;

          case OOSQL_TYPE_LONG_LONG :

            memcpy(&ll1, left, sizeof(Eight_Invariable));
            memcpy(&ll2, right, sizeof(Eight_Invariable));

            if (sortTupleDesc->parts[i].flag & SORTKEYDESC_ATTR_ASC) {
                if (ll1 > l2)      return 1;
                else if (ll1 < l2) return -1;
            }
            else if (sortTupleDesc->parts[i].flag & SORTKEYDESC_ATTR_DESC) {
                if (ll1 < l2)      return 1;
                else if (ll1 > l2) return -1;
            }

            kpartSize = sizeof(Eight_Invariable);

            break;


          /* in case of 'Float' type */
          case OOSQL_TYPE_FLOAT :

            memcpy(&f1, left, sizeof(float));
            memcpy(&f2, right, sizeof(float));

            if (sortTupleDesc->parts[i].flag & SORTKEYDESC_ATTR_ASC) {
                if (f1 > f2)      return 1;
                else if (f1 < f2) return -1;
            }
            else if (sortTupleDesc->parts[i].flag & SORTKEYDESC_ATTR_DESC) {
                if (f1 < f2)      return 1;
                else if (f1 > f2) return -1;
            }

            kpartSize = sizeof(float);

            break;


          /* in case of 'Double' type */
          case OOSQL_TYPE_DOUBLE :

            memcpy(&d1, left, sizeof(double));
            memcpy(&d2, right, sizeof(double));

            if (sortTupleDesc->parts[i].flag & SORTKEYDESC_ATTR_ASC) {
                if (d1 > d2)      return 1;
                else if (d1 < d2) return -1;
            }
            else if (sortTupleDesc->parts[i].flag & SORTKEYDESC_ATTR_DESC) {
                if (d1 < d2)      return 1;
                else if (d1 > d2) return -1;
            }
		
            kpartSize = sizeof(double);

            break;

          /* in case of 'Fixed String' type */
          case OOSQL_TYPE_STRING :

            result = memcmp(left, right, sortTupleDesc->parts[i].length);

            if (sortTupleDesc->parts[i].flag & SORTKEYDESC_ATTR_ASC) {
                if (result > 0)      return 1;
                else if (result < 0) return -1;
            }
            else if (sortTupleDesc->parts[i].flag & SORTKEYDESC_ATTR_DESC) {
                if (result < 0)      return 1;
                else if (result > 0) return -1;
            }

            kpartSize = sortTupleDesc->parts[i].length;

            break;


          /* in case of 'Variable String' type */
          case OOSQL_TYPE_VARSTRING :

            memcpy(&len1, left, sizeof(Two));
            memcpy(&len2, right, sizeof(Two));

            left += sizeof(Two);
            right += sizeof(Two);

			if(len1 < len2)
				result = memcmp(left, right, len1);
			else
				result = memcmp(left, right, len2);

            if (sortTupleDesc->parts[i].flag & SORTKEYDESC_ATTR_ASC) {
                if (result > 0)      return 1;
                else if (result < 0) return -1;

                if (len1 > len2)      return 1;
                else if (len1 < len2) return -1;
            }
            else if (sortTupleDesc->parts[i].flag & SORTKEYDESC_ATTR_DESC) {
                if (result < 0)      return 1;
                else if (result > 0) return -1;

                if (len1 < len2)      return 1;
                else if (len1 > len2) return -1;
            }

            /* Note!! at this point, two variable string is exactly equal */
			if(len1 < len2)
				kpartSize = len1;
			else
				kpartSize = len2;

            break;

          /* in case of 'OID' type */ 
          case OOSQL_TYPE_OID : 

            memcpy((char*)&oid1, (char*)left, SM_OID_SIZE);
            memcpy((char*)&oid2, (char*)right, SM_OID_SIZE);

            if (sortTupleDesc->parts[i].flag & SORTKEYDESC_ATTR_ASC) {

                if (oid1.volNo > oid2.volNo) return 1;
                else if (oid1.volNo < oid2.volNo) return -1;

                if (oid1.pageNo > oid2.pageNo) return 1;
                else if (oid1.pageNo < oid2.pageNo) return -1;

                if (oid1.slotNo > oid2.slotNo) return 1;
                else if (oid1.slotNo < oid2.slotNo) return -1;
            }
            else if (sortTupleDesc->parts[i].flag & SORTKEYDESC_ATTR_DESC) {

                if (oid1.volNo < oid2.volNo) return 1;
                else if (oid1.volNo > oid2.volNo) return -1;

                if (oid1.pageNo < oid2.pageNo) return 1;
                else if (oid1.pageNo > oid2.pageNo) return -1;

                if (oid1.slotNo < oid2.slotNo) return 1;
                else if (oid1.slotNo > oid2.slotNo) return -1;
            }
            
            kpartSize = SM_OID_SIZE;
            
            break;

        } /* switch */
		
        left += kpartSize;
        right += kpartSize;

    } /* for */


    return 0;
}

static int CompareForShort(OOSQL_StorageManager::SortTupleDesc* sortTupleDesc, void* p1, void* p2)
{
	short s1, s2;

    memcpy(&s1, p1, sizeof(short));
    memcpy(&s2, p2, sizeof(short));

    if (sortTupleDesc->parts[0].flag & SORTKEYDESC_ATTR_ASC) 
	{
        if (s1 > s2)      return 1;
        else if (s1 < s2) return -1;
    }
    else if (sortTupleDesc->parts[0].flag & SORTKEYDESC_ATTR_DESC) 
	{
        if (s1 < s2)      return 1;
        else if (s1 > s2) return -1;
    }

	return 0;
}

static int CompareForInt(OOSQL_StorageManager::SortTupleDesc* sortTupleDesc, void* p1, void* p2)
{
	int i1, i2;

	memcpy(&i1, p1, sizeof(int));
	memcpy(&i2, p2, sizeof(int));

	if (sortTupleDesc->parts[0].flag & SORTKEYDESC_ATTR_ASC) 
	{
		if (i1 > i2)      return 1;
		else if (i1 < i2) return -1;
	}
	else if (sortTupleDesc->parts[0].flag & SORTKEYDESC_ATTR_DESC) 
	{
		if (i1 < i2)      return 1;
		else if (i1 > i2) return -1;
	}

	return 0;
}

static int CompareForLong(OOSQL_StorageManager::SortTupleDesc* sortTupleDesc, void* p1, void* p2)
{
	Four_Invariable l1, l2;

	memcpy(&l1, p1, sizeof(Four_Invariable));
	memcpy(&l2, p2, sizeof(Four_Invariable));

	if (sortTupleDesc->parts[0].flag & SORTKEYDESC_ATTR_ASC) 
	{
		if (l1 > l2)      return 1;
		else if (l1 < l2) return -1;
	}
	else if (sortTupleDesc->parts[0].flag & SORTKEYDESC_ATTR_DESC) 
	{
		if (l1 < l2)      return 1;
		else if (l1 > l2) return -1;
	}

	return 0;
}

static int CompareForLongLong(OOSQL_StorageManager::SortTupleDesc* sortTupleDesc, void* p1, void* p2)
{
	Eight_Invariable l1, l2;

	memcpy(&l1, p1, sizeof(Eight_Invariable));
	memcpy(&l2, p2, sizeof(Eight_Invariable));

	if (sortTupleDesc->parts[0].flag & SORTKEYDESC_ATTR_ASC) 
	{
		if (l1 > l2)      return 1;
		else if (l1 < l2) return -1;
	}
	else if (sortTupleDesc->parts[0].flag & SORTKEYDESC_ATTR_DESC) 
	{
		if (l1 < l2)      return 1;
		else if (l1 > l2) return -1;
	}

	return 0;
}

static int CompareForFloat(OOSQL_StorageManager::SortTupleDesc* sortTupleDesc, void* p1, void* p2)
{
	float f1, f2;

	memcpy(&f1, p1, sizeof(float));
	memcpy(&f2, p2, sizeof(float));

    if (sortTupleDesc->parts[0].flag & SORTKEYDESC_ATTR_ASC) {
        if (f1 > f2)      return 1;
        else if (f1 < f2) return -1;
    }
    else if (sortTupleDesc->parts[0].flag & SORTKEYDESC_ATTR_DESC) {
        if (f1 < f2)      return 1;
        else if (f1 > f2) return -1;
    }

	return 0;
}

static int CompareForDouble(OOSQL_StorageManager::SortTupleDesc* sortTupleDesc, void* p1, void* p2)
{
	double d1, d2;

	d1 = *(double*)p1;
	d2 = *(double*)p2;

    if (sortTupleDesc->parts[0].flag & SORTKEYDESC_ATTR_ASC) {
        if (d1 > d2)      return 1;
        else if (d1 < d2) return -1;
    }
    else if (sortTupleDesc->parts[0].flag & SORTKEYDESC_ATTR_DESC) {
        if (d1 < d2)      return 1;
        else if (d1 > d2) return -1;
    }
		
	return 0;
}

static int CompareForString(OOSQL_StorageManager::SortTupleDesc* sortTupleDesc, void* p1, void* p2)
{
	Four result;

	result = memcmp(p1, p2, sortTupleDesc->parts[0].length);

    if (sortTupleDesc->parts[0].flag & SORTKEYDESC_ATTR_ASC) {
        if (result > 0)      return 1;
        else if (result < 0) return -1;
    }
    else if (sortTupleDesc->parts[0].flag & SORTKEYDESC_ATTR_DESC) {
        if (result < 0)      return 1;
        else if (result > 0) return -1;
    }

	return 0;
}

static int CompareForVarstring(OOSQL_StorageManager::SortTupleDesc* sortTupleDesc, void* p1, void* p2)
{
	Two  len1, len2;
	Four result;

	memcpy(&len1, p1, sizeof(Two));
    memcpy(&len2, p2, sizeof(Two));

	if(len1 < len2)
		result = memcmp((char*)p1 + sizeof(Two), (char*)p2 + sizeof(Two), len1);
	else
		result = memcmp((char*)p1 + sizeof(Two), (char*)p2 + sizeof(Two), len2);

    if (sortTupleDesc->parts[0].flag & SORTKEYDESC_ATTR_ASC) {
        if (result > 0)      return 1;
        else if (result < 0) return -1;

        if (len1 > len2)      return 1;
        else if (len1 < len2) return -1;
    }
    else if (sortTupleDesc->parts[0].flag & SORTKEYDESC_ATTR_DESC) {
        if (result < 0)      return 1;
        else if (result > 0) return -1;

        if (len1 < len2)      return 1;
        else if (len1 > len2) return -1;
    }

	return 0;
}

static int CompareForOID(OOSQL_StorageManager::SortTupleDesc* sortTupleDesc, void* p1, void* p2)
{
	OID oid1, oid2;

	oid1 = *(OID*)p1;
	oid2 = *(OID*)p2;

    if (sortTupleDesc->parts[0].flag & SORTKEYDESC_ATTR_ASC) {

        if (oid1.volNo > oid2.volNo) return 1;
        else if (oid1.volNo < oid2.volNo) return -1;

        if (oid1.pageNo > oid2.pageNo) return 1;
        else if (oid1.pageNo < oid2.pageNo) return -1;

        if (oid1.slotNo > oid2.slotNo) return 1;
        else if (oid1.slotNo < oid2.slotNo) return -1;
    }
    else if (sortTupleDesc->parts[0].flag & SORTKEYDESC_ATTR_DESC) {

        if (oid1.volNo < oid2.volNo) return 1;
        else if (oid1.volNo > oid2.volNo) return -1;

        if (oid1.pageNo < oid2.pageNo) return 1;
        else if (oid1.pageNo > oid2.pageNo) return -1;

        if (oid1.slotNo < oid2.slotNo) return 1;
        else if (oid1.slotNo > oid2.slotNo) return -1;
    }

	return 0;
}

Four OOSQL_SortStream::SortTuples()
{
	Four e;

	if(m_useInMemorySorting)
	{
		m_sortBufferTail = m_sortBuffer;

		m_compareFunc = CompareForGenernalCase;
		if(m_sortTupleDesc.nparts == 1)
		{
			switch(m_sortTupleDesc.parts[0].type)
			{
			case OOSQL_TYPE_SHORT:
				m_compareFunc = CompareForShort;
				break;
			case OOSQL_TYPE_INT:
				m_compareFunc = CompareForInt;
				break;
			case OOSQL_TYPE_LONG:
				m_compareFunc = CompareForLong;
				break;
			case OOSQL_TYPE_LONG_LONG:
				m_compareFunc = CompareForLongLong;
				break;
			case OOSQL_TYPE_FLOAT:
				m_compareFunc = CompareForFloat;
				break;
			case OOSQL_TYPE_DOUBLE:
				m_compareFunc = CompareForDouble;
				break;
			case OOSQL_TYPE_STRING:
				m_compareFunc = CompareForString;
				break;
			case OOSQL_TYPE_VARSTRING:
				m_compareFunc = CompareForVarstring;
				break;
			case OOSQL_TYPE_OID: 
				m_compareFunc = CompareForOID;
				break;
            }
		}

		e = QuickSort();     
		OOSQL_CHECK_ERR(e);

		return eNOERROR;
	}
	else
	{
		e = m_storageManager->SortingSortStream(m_sortStreamID);
		OOSQL_CHECK_ERR(e);

		return eNOERROR;
	}
}

