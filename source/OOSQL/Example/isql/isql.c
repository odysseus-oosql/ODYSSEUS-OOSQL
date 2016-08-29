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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <limits.h>

#include "OOSQL_APIs.h"

#define eISQL_INTERNAL_ERROR INT_MIN

#define PRINT_ERROR_MESSAGE(systemHandle, handle, e) \
do { \
if (e < 0) { \
	if(e != eISQL_INTERNAL_ERROR) \
    { \
	    char errorMessage[4096]; \
		OOSQL_GetErrorName(systemHandle, e, errorMessage, sizeof(errorMessage)); \
		printf("OOSQL ERROR(%s) : ", errorMessage); \
	    OOSQL_GetErrorMessage(systemHandle, e, errorMessage, sizeof(errorMessage)); \
	    puts(errorMessage); \
	    OOSQL_GetQueryErrorMessage(systemHandle, handle, errorMessage, sizeof(errorMessage)); \
	    puts(errorMessage); \
	} \
}\
} while(0);

#define CHECK_QUERY_ERROR(systemHandle, handle, e) \
do { \
if (e < 0) { \
	if(e != eISQL_INTERNAL_ERROR) \
    { \
	    char errorMessage[4096]; \
		OOSQL_GetErrorName(systemHandle, e, errorMessage, sizeof(errorMessage)); \
		printf("OOSQL ERROR(%s) : ", errorMessage); \
	    OOSQL_GetErrorMessage(systemHandle, e, errorMessage, sizeof(errorMessage)); \
	    puts(errorMessage); \
	    OOSQL_GetQueryErrorMessage(systemHandle, handle, errorMessage, sizeof(errorMessage)); \
	    puts(errorMessage); \
        return eISQL_INTERNAL_ERROR; \
	} \
	else \
	    return e; \
}\
} while(0);

#define CHECK_ERROR(systemHandle, e) \
do { \
if (e < 0) { \
	if(e != eISQL_INTERNAL_ERROR) \
    { \
	    char errorMessage[4096]; \
		OOSQL_GetErrorName(systemHandle, e, errorMessage, sizeof(errorMessage)); \
		printf("OOSQL ERROR(%s) : ", errorMessage); \
	    OOSQL_GetErrorMessage(systemHandle, e, errorMessage, sizeof(errorMessage)); \
	    puts(errorMessage); \
        return eISQL_INTERNAL_ERROR; \
	} \
	else \
	    return e; \
}\
} while(0);

#define CHECK_ERROR_XCT(systemHandle, e, xactId) \
do { \
if (e < 0) { \
    char errorMessage[4096]; \
	OOSQL_GetErrorName(systemHandle, e, errorMessage, sizeof(errorMessage)); \
	printf("OOSQL ERROR(%s) : ", errorMessage); \
    OOSQL_GetErrorMessage(systemHandle, e, errorMessage, sizeof(errorMessage)); \
    puts(errorMessage); \
    if((xactId) != NULL) OOSQL_TransAbort(systemHandle, xactId); \
    OOSQL_DestroySystemHandle(systemHandle, procIndex); \
    if (1) exit(1); \
}\
} while(0);

#define SORT_BUFFER_SIZE 1024 * 1024 * 2

Four OOSQL_DumpPlan(OOSQL_SystemHandle* systemHandle, OOSQL_Handle handle);

Four procIndex;

int execSQL(
	OOSQL_SystemHandle*	  systemHandle,
    Four				  volId,
    char*				  queryStr,
    OOSQL_SortBufferInfo* sortBufferInfo
)
{
    OOSQL_Handle			handle;
    Four					e;
    Two						nCols;
    Four					type;
    short					shortAttr;
    int						intAttr;
    float					floatAttr;
    double					doubleAttr;
    char					stringAttr[16 * 1024];
    Four					i;
	Four					retLength;
	OID						oid;
	OOSQL_Date				dateAttr;
	OOSQL_Time				timeAttr;
	OOSQL_Timestamp			timestampAttr;
	Four					timeElapsed;
	Four					nElements;
	OOSQL_ComplexTypeInfo	complexTypeInfo;

	e = OOSQL_ResetTimeElapsed(systemHandle);
	CHECK_ERROR(systemHandle, e);

    e = OOSQL_AllocHandle(systemHandle, volId, &handle); CHECK_ERROR(systemHandle, e);

    e = OOSQL_Prepare(systemHandle, handle, queryStr, sortBufferInfo);
    if(e < eNOERROR)
    {
		PRINT_ERROR_MESSAGE(systemHandle, handle, e);
        e = OOSQL_FreeHandle(systemHandle, handle); CHECK_ERROR(systemHandle, e);
		return eISQL_INTERNAL_ERROR;
    }

    e = OOSQL_GetNumResultCols(systemHandle, handle, &nCols); CHECK_QUERY_ERROR(systemHandle, handle, e);

    e = OOSQL_Execute(systemHandle, handle);
    if(e < eNOERROR)
    {
		PRINT_ERROR_MESSAGE(systemHandle, handle, e);
        e = OOSQL_FreeHandle(systemHandle, handle); CHECK_ERROR(systemHandle, e);
		return eISQL_INTERNAL_ERROR;
    }

	if(nCols == 0)
	{
		e = OOSQL_FreeHandle(systemHandle, handle); CHECK_ERROR(systemHandle, e);
		return eNOERROR;
	}
	
	for(i = 0; i < nCols; i++)
		printf("----------+");
	printf("\n");
	for(i = 0; i < nCols; i++)
	{
		e = OOSQL_GetResultColName(systemHandle, handle, i, stringAttr, sizeof(stringAttr));
		CHECK_QUERY_ERROR(systemHandle, handle, e);

		printf("%10s|", stringAttr);
	}
	printf("\n");
	for(i = 0; i < nCols; i++)
		printf("----------+");
	printf("\n");

    while((e = OOSQL_Next(systemHandle, handle)) != ENDOFEVAL)
    {
        CHECK_QUERY_ERROR(systemHandle, handle, e);

        for(i = 0; i < nCols; i++)
        {
            e = OOSQL_GetResultColType(systemHandle, handle, i, &type); CHECK_QUERY_ERROR(systemHandle, handle, e);
            switch(type)
            {
            case OOSQL_TYPE_CHAR:
            case OOSQL_TYPE_VARCHAR:
                e = OOSQL_GetData(systemHandle, handle, i, 0, &stringAttr, sizeof(stringAttr), &retLength); CHECK_QUERY_ERROR(systemHandle, handle, e);
				if(retLength == -1)
					printf("%10s|", "NULL");
				else
				{
					stringAttr[retLength] = '\0';
					printf("%10s|", stringAttr);
				}
                break;
            case OOSQL_TYPE_SMALLINT:
                e = OOSQL_GetData(systemHandle, handle, i, 0, &shortAttr, sizeof(short), &retLength); CHECK_QUERY_ERROR(systemHandle, handle, e);
				if(retLength == -1)
					printf("%10s|", "NULL");
				else
					printf("%10d|", (int)shortAttr);
                break;
            case OOSQL_TYPE_INTEGER:
            case OOSQL_TYPE_LONG:
                e = OOSQL_GetData(systemHandle, handle, i, 0, &intAttr, sizeof(int), &retLength); CHECK_QUERY_ERROR(systemHandle, handle, e);
				if(retLength == -1)
					printf("%10s|", "NULL");
				else
					printf("%10d|", intAttr);
                break;
			case OOSQL_TYPE_TIME:
				e = OOSQL_GetData(systemHandle, handle, i, 0, &timeAttr, sizeof(OOSQL_Time), &retLength); CHECK_QUERY_ERROR(systemHandle, handle, e);
				if(retLength == -1)
   		            printf("%10s|", "NULL");
    			else
    				printf("  %2d:%2d:%2d|", OOSQL_GetHour(systemHandle, &timeAttr),
    				                         OOSQL_GetMinute(systemHandle, &timeAttr),
    				                         OOSQL_GetSecond(systemHandle, &timeAttr));
                break;
            case OOSQL_TYPE_DATE:
                e = OOSQL_GetData(systemHandle, handle, i, 0, &dateAttr, sizeof(OOSQL_Date), &retLength); CHECK_QUERY_ERROR(systemHandle, handle, e);
				if(retLength == -1)
   		            printf("%10s|", "NULL");
    			else
    				printf("%4d-%2d-%2d|", OOSQL_GetYear(systemHandle, &dateAttr),
    				                       OOSQL_GetMonth(systemHandle, &dateAttr),
    				                       OOSQL_GetDay(systemHandle, &dateAttr));
                break;
			case OOSQL_TYPE_TIMESTAMP:
				e = OOSQL_GetData(systemHandle, handle, i, 0, &timestampAttr, sizeof(OOSQL_Timestamp), &retLength); CHECK_QUERY_ERROR(systemHandle, handle, e);
				if(retLength == -1)
   		            printf("%10s|", "NULL");
    			else
    				printf("%4d-%2d-%2d-%2d:%2d:%2d|", 
											OOSQL_GetYear(systemHandle, &timestampAttr.d),
    										OOSQL_GetMonth(systemHandle, &timestampAttr.d),
    										OOSQL_GetDay(systemHandle, &timestampAttr.d),
											OOSQL_GetHour(systemHandle, &timestampAttr.t),
    				                        OOSQL_GetMinute(systemHandle, &timestampAttr.t),
    				                        OOSQL_GetSecond(systemHandle, &timestampAttr.t));
                break;
            case OOSQL_TYPE_FLOAT:
                e = OOSQL_GetData(systemHandle, handle, i, 0, &floatAttr, sizeof(float), &retLength); CHECK_QUERY_ERROR(systemHandle, handle, e);
				if(retLength == -1)
					printf("%10s|", "NULL");
				else
					printf("%10f|", floatAttr);
                break;
            case OOSQL_TYPE_DOUBLE:
                e = OOSQL_GetData(systemHandle, handle, i, 0, &doubleAttr, sizeof(double), &retLength); CHECK_QUERY_ERROR(systemHandle, handle, e);
				if(retLength == -1)
					printf("%10s|", "NULL");
				else
					printf("%10f|", doubleAttr);
                break;
            case OOSQL_TYPE_TEXT:
                e = OOSQL_GetData(systemHandle, handle, i, 0, &stringAttr, sizeof(stringAttr), &retLength); CHECK_QUERY_ERROR(systemHandle, handle, e);
				if(retLength == -1)
					printf("%10s|", "NULL");
				else
				{
					if(retLength == sizeof(stringAttr))
						stringAttr[retLength - 1] = '\0';
					else
						stringAttr[retLength] = '\0';
					printf("%10s|", stringAttr);
				}
                break;
			case OOSQL_TYPE_OID:
				e = OOSQL_GetData(systemHandle, handle, i, 0, &oid, sizeof(OID), &retLength); CHECK_QUERY_ERROR(systemHandle, handle, e);
				if(retLength == -1)
					printf("%10s|", "NULL");
				else
				{
					e = OOSQL_OIDToOIDString(systemHandle, &oid, stringAttr); CHECK_QUERY_ERROR(systemHandle, handle, e);
					printf("%10s|", stringAttr);
				}
				break;
			default:
				if(OOSQL_MASK_COMPLEXTYPE(type) != OOSQL_COMPLEXTYPE_BASIC)
				{
					e = OOSQL_GetComplexTypeInfo(systemHandle, handle, i, &complexTypeInfo);
					CHECK_QUERY_ERROR(systemHandle, handle, e);

					e = OOSQL_ComplexType_GetNumElements(&complexTypeInfo, &nElements);
					CHECK_QUERY_ERROR(systemHandle, handle, e);

					e = OOSQL_ComplexType_GetElementsString(&complexTypeInfo, 0, nElements, stringAttr, sizeof(stringAttr));
					CHECK_QUERY_ERROR(systemHandle, handle, e);

					printf("%10s|", stringAttr);
				}
				break;
			}
        }
        printf("\n");
        
    }
	for(i = 0; i < nCols; i++)
		printf("----------+");
	printf("\n");

	e = OOSQL_GetTimeElapsed(systemHandle, &timeElapsed);
	CHECK_ERROR(systemHandle, e);

	printf("Time elapsed %d ms\n", timeElapsed);

	e = OOSQL_FreeHandle(systemHandle, handle); CHECK_ERROR(systemHandle, e);

    return eNOERROR;
}

int execISQL( 
	OOSQL_SystemHandle*	  systemHandle,		
    Four				  volId,
    OOSQL_SortBufferInfo* sortBufferInfo
)
{
    char            queryStr[16 * 1024];
    char            catStr[256];
    int             e;
	int				lineno = 1;

    while (1) 
    { 
        printf("OOSQL> ");

        /* get input query string */
        memset(queryStr, 0, sizeof(queryStr));
        while(1) 
        {
            if(gets(catStr) == NULL)
				continue;
            strcat(queryStr, catStr);
			
            if(queryStr[strlen(queryStr) - 1] == ';')
            {   
                queryStr[strlen(queryStr) - 1] = '\0';
                break;
            }
            else if(strcmp(queryStr, "quit") == 0) 
                return eNOERROR;
			
			strcat(queryStr, "\n");
			lineno ++;
			printf("%3d  ", lineno);
			fflush(stdout);
        }
        e = execSQL(systemHandle, volId, queryStr, sortBufferInfo);
        if(e < 0) printf("SQL Error\n");
		lineno = 1;
    }

    return eNOERROR;
}

int main(int argc, char* argv[])
{
    char				 DBPATH[256];
    Four				 volID;
	Four				 databaseID;
    XactID				 xactID;
    Four				 e;
	OOSQL_SystemHandle	 systemHandle;
    OOSQL_SortBufferInfo sortBufferInfo;
    char*                sortBuffer;

	puts(OOSQL_GetVersionString());

    if(argc != 2)
    {
       printf("USAGE : isql <database name>\n");
       exit(1);
    }

	sortBuffer = (char *)malloc(sizeof(char) * SORT_BUFFER_SIZE);

	sortBufferInfo.mode = OOSQL_SB_USE_MEMORY;
	sortBufferInfo.memoryInfo.sortBufferPtr = sortBuffer;
	sortBufferInfo.memoryInfo.sortBufferLength = SORT_BUFFER_SIZE;

    printf("OOSQL_Init\n");
    e = OOSQL_CreateSystemHandle(&systemHandle, &procIndex);
    CHECK_ERROR_XCT(&systemHandle, e, NULL);
	printf("\n");

    strcpy(DBPATH, argv[1]);

    e = OOSQL_MountDB(&systemHandle, DBPATH, &databaseID);
    CHECK_ERROR_XCT(&systemHandle, e, NULL);
    printf("Database %s is mounted\n", DBPATH);
    
	e = OOSQL_GetUserDefaultVolumeID(&systemHandle, databaseID, &volID);
	CHECK_ERROR_XCT(&systemHandle, e, NULL);

    printf("OOSQL_TransBegin\n");
    e = OOSQL_TransBegin(&systemHandle, &xactID, X_RR_RR);
    CHECK_ERROR_XCT(&systemHandle, e, &xactID);

	e = execISQL(&systemHandle, volID, &sortBufferInfo);
	CHECK_ERROR_XCT(&systemHandle, e, &xactID);

    printf("OOSQL_TransCommit\n");
    e = OOSQL_TransCommit(&systemHandle, &xactID);
    CHECK_ERROR_XCT(&systemHandle, e, &xactID);

    printf("OOSQL_Dismount\n");
    e = OOSQL_DismountDB(&systemHandle, databaseID);
    CHECK_ERROR_XCT(&systemHandle, e, NULL);
    
    printf("OOSQL_Final\n");
    e = OOSQL_DestroySystemHandle(&systemHandle, procIndex);
    CHECK_ERROR_XCT(&systemHandle, e, NULL);
    
	free(sortBuffer);

    return 1;
}
