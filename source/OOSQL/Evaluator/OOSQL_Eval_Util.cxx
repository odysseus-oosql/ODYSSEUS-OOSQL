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

/*
    MODULE:
        OOSQL_Eval_Util.cxx

    DESCRIPTION:
        'like' operator evaluation module.

    IMPORTS:

    EXPORTS:
        int     OQL_EvalOpLike( char *text, char *pattern );
*/

#include <string.h>
#include <stdio.h>
#include <ctype.h>
#include "OOSQL_StorageSystemHeaders.h"
#include "OOSQL_APIs_Internal.hxx"
#include "OOSQL_Common.h"
#include "OOSQL_Error.h"
#include "OOSQL_Eval_Util.hxx"
#include <stdarg.h>
#if !defined(WIN32) && !defined(__GNUC__)
#include <varargs.h>
#endif

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
LIKE_ABORT          
LIKE_FALSE         
0                   
1                  
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
static int oql_DoMatch(
	char	*text,				// IN: ptr. to char. string
	char	*p					// IN: pattern
)
/*
    Function:
        Match text and p.

    Side effect:

    Return value:
        LIKE_TRUE       if p matches test
        LIKE_FALSE or LIKE_ABORT        otherwise
*/
{
	int matched;


    for ( ; *p; text++, p++) 
	{
        if(*text == '\0' && *p != '%' && *p != '*')
            return LIKE_ABORT;

        switch (*p) 
		{
        case '\\':
            /* Literal match with following character. */
            p++;
            continue;
        case '_':
        case '?':		
			if(*(unsigned char*)text >= (unsigned char)0x80)	
				text ++;    
            continue;
        case '%':
        case '*':			
			do {
				++p;
			} while(*p == '%' || *p == '*');	
                
            if(*p == '\0')
                /* Trailing percent matches everything. */
                return LIKE_TRUE;

            while (*text)
			{
                if(*(unsigned char*)text < (unsigned char)0x80)
				{
					matched = oql_DoMatch(text, p);
				
					text ++;
					if(matched != LIKE_FALSE)
						return matched;
				}
				else 
				{
					matched = oql_DoMatch(text, p);

					text += 2;
					if(matched != LIKE_FALSE)
						return matched;
				}
			}
            return LIKE_ABORT;
		default:
            if(*text != *p)
                return LIKE_FALSE;
			
			if(*(unsigned char*)text >= (unsigned char)0x80)
			{
				text ++;
				p ++;

				if(*text != *p)
					return LIKE_FALSE;
			}
            continue;
        }
    }

    return *text == '\0';
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
LIKE_ABORT          
LIKE_FALSE          
0                   
1                  
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
int     OQL_EvalOpLike(
    char    *text,          // IN: ptr. to char. string
    char    *pattern        // IN: ptr. to pattern
)
{

    if((pattern[0] == '*' || pattern[0] == '%') && pattern[1] == '\0')
        return LIKE_TRUE;

    return (oql_DoMatch(text, pattern) == LIKE_TRUE);
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
< eNOERROR          - ¿¡·¯

IMPLEMENTATION:
****************************************************************************/
Four oosql_SQL_Init(OOSQL_SystemHandle *systemHandle, Four volID, OOSQL_Handle* handle)
{
	Four e;

	e = OOSQL_AllocHandle(systemHandle, volID, handle);
	if(e < 0) OOSQL_ERROR(systemHandle, e);

	return eNOERROR;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four oosql_SQL_Exec(OOSQL_SystemHandle *systemHandle, OOSQL_Handle handle, const char* queryString)
{
    Four e;
	
    e = OOSQL_Prepare(systemHandle, handle, (char*)queryString, NULL);
	if(e < 0) OOSQL_ERROR(systemHandle, e);

    e = OOSQL_Execute(systemHandle, handle);
	if(e < 0) OOSQL_ERROR(systemHandle, e);

	return eNOERROR;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four oosql_SQL_Final(OOSQL_SystemHandle *systemHandle, OOSQL_Handle handle)
{
	Four e;

	e = OOSQL_FreeHandle(systemHandle, handle);
	if(e < 0) OOSQL_ERROR(systemHandle, e);

	return eNOERROR;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
< eNOERROR         

IMPLEMENTATION:
****************************************************************************/
Four oosql_SQL_Next(OOSQL_SystemHandle *systemHandle, OOSQL_Handle handle, ...)
{
	Four				e;
	Two                 nCols;
    Four                type;
    short               shortAttr;
    int                 intAttr;
	Eight_Invariable	eightAttr;
    float               floatAttr;
    double              doubleAttr;
    char                stringAttr[1024];
    Four                i;
	Four				retLength;
	OID					oid;
	OOSQL_Date          dateAttr;
	OOSQL_Time          timeAttr;
	OOSQL_Timestamp		timestampAttr;
	va_list				argument;
	void*				argumentPtr;

	e = OOSQL_GetNumResultCols(systemHandle, handle, &nCols); 
	if(e < 0) OOSQL_ERROR(systemHandle, e);

	if(nCols == 0)
		return eNORESULTS_OOSQL;
	
	e = OOSQL_Next(systemHandle, handle);
	if(e == ENDOFEVAL)
		return eNORESULTS_OOSQL;

#if defined(WIN32) || defined(__GNUC__)
	va_start(argument, handle);
#else
	va_start(argument);
#endif

	for(i = 0; i < nCols; i++)
	{
		e = OOSQL_GetResultColType(systemHandle, handle, i, &type); 
		if(e < 0) OOSQL_ERROR(systemHandle, e);

		argumentPtr = va_arg(argument, void*);
		
        switch(type)
        {
        case OOSQL_TYPE_CHAR:
        case OOSQL_TYPE_VARCHAR:
            e = OOSQL_GetData(systemHandle, handle, i, 0, &stringAttr, sizeof(stringAttr), &retLength); 
			if(e < 0) OOSQL_ERROR(systemHandle, e);
			
			if(retLength != -1)
			{
				stringAttr[retLength] = '\0';
				strcpy((char*)argumentPtr, stringAttr);
			}
			else
				strcpy((char*)argumentPtr, "");
			
            break;
        case OOSQL_TYPE_SMALLINT:
            e = OOSQL_GetData(systemHandle, handle, i, 0, &shortAttr, sizeof(short), &retLength); 
			if(e < 0) OOSQL_ERROR(systemHandle, e);
			if(retLength != -1)
				*(short*)argumentPtr = shortAttr;
            break;
        case OOSQL_TYPE_INTEGER:
        case OOSQL_TYPE_LONG:
            e = OOSQL_GetData(systemHandle, handle, i, 0, &intAttr, sizeof(int), &retLength); 
			if(e < 0) OOSQL_ERROR(systemHandle, e);
			if(retLength != -1)
				*(int*)argumentPtr = intAttr;
            break;
        case OOSQL_TYPE_LONG_LONG:
            e = OOSQL_GetData(systemHandle, handle, i, 0, &eightAttr, sizeof(Eight_Invariable), &retLength); 
			if(e < 0) OOSQL_ERROR(systemHandle, e);
			if(retLength != -1)
				*(Eight_Invariable*)argumentPtr = eightAttr;
            break;
		case OOSQL_TYPE_TIME:
			e = OOSQL_GetData(systemHandle, handle, i, 0, &timeAttr, sizeof(OOSQL_Time), &retLength); 
			if(e < 0) OOSQL_ERROR(systemHandle, e);
			if(retLength != -1)
				*(OOSQL_Time*)argumentPtr = timeAttr;
            break;
        case OOSQL_TYPE_DATE:
            e = OOSQL_GetData(systemHandle, handle, i, 0, &dateAttr, sizeof(OOSQL_Date), &retLength); 
			if(e < 0) OOSQL_ERROR(systemHandle, e);
			if(retLength != -1)
				*(OOSQL_Date*)argumentPtr = dateAttr;
            break;
		case OOSQL_TYPE_TIMESTAMP:
			e = OOSQL_GetData(systemHandle, handle, i, 0, &timestampAttr, sizeof(OOSQL_Timestamp), &retLength); 
			if(e < 0) OOSQL_ERROR(systemHandle, e);
			if(retLength != -1)
				*(OOSQL_Timestamp*)argumentPtr = timestampAttr;
            break;
        case OOSQL_TYPE_FLOAT:
            e = OOSQL_GetData(systemHandle, handle, i, 0, &floatAttr, sizeof(float), &retLength); 
			if(e < 0) OOSQL_ERROR(systemHandle, e);
			if(retLength != -1)
				*(float*)argumentPtr = floatAttr;
            break;
        case OOSQL_TYPE_DOUBLE:
            e = OOSQL_GetData(systemHandle, handle, i, 0, &doubleAttr, sizeof(double), &retLength); 
			if(e < 0) OOSQL_ERROR(systemHandle, e);
			if(retLength != -1)
				*(double*)argumentPtr = doubleAttr;
            break;
        case OOSQL_TYPE_TEXT:
            e = OOSQL_GetData(systemHandle, handle, i, 0, &stringAttr, sizeof(stringAttr), &retLength); 
			if(e < 0) OOSQL_ERROR(systemHandle, e);
            if(retLength == sizeof(stringAttr))
                stringAttr[retLength - 1] = '\0';
			else if(retLength != -1)
				stringAttr[retLength] = '\0';
			if(retLength != -1)
				strcpy((char*)argumentPtr, stringAttr);
			else
				strcpy((char*)argumentPtr, "");
            break;
		case OOSQL_TYPE_OID:
			e = OOSQL_GetData(systemHandle, handle, i, 0, &oid, sizeof(OID), &retLength); 
			if(e < 0) OOSQL_ERROR(systemHandle, e);
			if(retLength != -1)
				*(OID*)argumentPtr = oid;
			break;
		default:
			va_end(argument);
			return eUNHANDLED_CASE_OOSQL;
        }
	}

	va_end(argument);

	return eNOERROR;
}

Four oosql_SQL_InitExecFinal(OOSQL_SystemHandle *systemHandle, Four volID, const char* queryString)
{
	Four			e;
	OOSQL_Handle	handle;

	e = oosql_SQL_Init(systemHandle, volID, &handle);
	if(e < 0) OOSQL_ERROR(systemHandle, e);

	e = oosql_SQL_Exec(systemHandle, handle, queryString);
	if(e < 0) OOSQL_ERROR(systemHandle, e);
	
	e = oosql_SQL_Final(systemHandle, handle);
	if(e < 0) OOSQL_ERROR(systemHandle, e);

	return eNOERROR;
}

typedef struct {
	Four start;
	Four end;
} OOSQL_Partition;

#define TUPLES(base, width, i) ((char*)(base) + (width) * (i))

const int OOSQL_MAXSTACKDEPTH = 50;
const int QUICKSORTLIMIT      = 10;

Four oosql_QuickSort(OOSQL_MemoryManager* pMemoryManager, void *base, size_t num, size_t width, Four (*compare)(void *elem1, void *elem2))
{
    OOSQL_Partition*	stack;						/* stack for emulating recursive call */ 
	Four				stackSize;
    Four				stackPtr = 0;				/* index of 'stack' */
    Four				curStart, curEnd;			/* variables which indicate current sorting partition */
    Four				i, j;						/* index variable */
    void*				pivot;						/* pivot which indicates split point */
    static long			rand=0;						/* varible for generating random variable */
	void*				swapBuffer;

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
        if (curEnd - curStart < QUICKSORTLIMIT) 
		{
            /* insertion sort!! */
            for (i = curEnd; i > curStart; i-- ) 
			{
				memcpy(pivot, TUPLES(base, width, i), width);
                for (j = curStart; j < i; j++ ) 
				{
                    if (compare(pivot, TUPLES(base, width, j)) < 0) 
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
        rand = (rand*1103515245 +12345) & LONG_MAX;
		memcpy(pivot, TUPLES(base, width, curStart+rand%(curEnd-curStart)), width);

        /* split!! */
        i = curStart; j = curEnd;
        while (i <= j) 
		{
            while (compare(TUPLES(base, width, i), pivot) < 0) i++;
            while (compare(pivot, TUPLES(base, width, j)) < 0) j--;
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

    return(eNOERROR);
}


/****************************************************************************
DESCRIPTION:

RETURN VALUE:
****************************************************************************/
static int HEX_TO_DEC(char hex);
Four oosql_ConvertFromOIDStringToOIDStructure(char* oidString, OOSQL_StorageManager::OID* oidStructure)
{
	char*	oidPtr;
	Four	byteValue;
	Four	i;

	oidPtr = (char*)oidStructure;
	memset(oidPtr, 0, sizeof(OOSQL_StorageManager::OID));
	
	for(i = 0; i < (sizeof(OOSQL_StorageManager::OID) * 2); i+= 2)
	{
		byteValue = HEX_TO_DEC(oidString[i]) * 16 + HEX_TO_DEC(oidString[i + 1]);
		oidPtr[i / 2] = (char)byteValue;
	}

	return eNOERROR;
}

static int HEX_TO_DEC(char hex)
{
	hex = tolower(hex);

	if('0' <= hex && hex <= '9')
		return hex - '0';
	else
		return hex - 'a' + 10;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
****************************************************************************/
Four oosql_ConvertFromOIDStructureToOIDString(OOSQL_StorageManager::OID* oidStructure, char* oidString)
{
    Four			i;
	unsigned char*	oidPtr;
	Four			nibble1, nibble2;
	char			DEC_TO_HEX[] = {'0','1','2','3','4','5','6','7','8','9','A','B','C','D','E','F'};

	oidPtr = (unsigned char*)oidStructure;

	for(i = 0; i < sizeof(OOSQL_StorageManager::OID); i++)
	{
		nibble1 = oidPtr[i] / 16;
		nibble2 = oidPtr[i] % 16;

		oidString[i * 2 + 0] = DEC_TO_HEX[nibble1];
		oidString[i * 2 + 1] = DEC_TO_HEX[nibble2];
	}
	oidString[i * 2] = '\0';

    return eNOERROR;
}
