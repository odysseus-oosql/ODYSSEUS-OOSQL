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
#include <ctype.h>

#include "OOSQL_APIs.h"

#define eISQL_INTERNAL_ERROR INT_MIN
#define MAXPOINTS			 1024

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
    if((systemHandle) != NULL) OOSQL_DestroySystemHandle(systemHandle, procIndex); \
    if (1) exit(1); \
}\
} while(0);

#define SORT_BUFFER_SIZE 1024 * 1024 * 2

Four			     cclevelIndex;
ConcurrencyLevel     indexToCclevel[] = {X_BROWSE_BROWSE, X_CS_BROWSE, X_CS_CS, X_RR_BROWSE, X_RR_CS, X_RR_RR};
XactID				 xactID;
extern Four			 procIndex;

int execSQL(
	OOSQL_SystemHandle*	  systemHandle,
    Four				  volId,
    char*				  queryStr,
    OOSQL_SortBufferInfo* sortBufferInfo
)
{
    OOSQL_Handle			handle;
    Four					e, e2;
    Two						nCols;
    Four					type;
    short					shortAttr;
    int						intAttr;
    float					floatAttr;
    double					doubleAttr;
    char					stringAttr[16 * 1024];
    Four					i, j;
	Four					retLength;
	OID						oid;
	OOSQL_Date				dateAttr;
	OOSQL_Time				timeAttr;
	OOSQL_Timestamp			timestampAttr;
	Four					timeElapsed;
	Four					nElements;
	Four					nResults;
	OOSQL_ComplexTypeInfo	complexTypeInfo;

	e = OOSQL_ResetTimeElapsed(systemHandle);
	CHECK_ERROR(systemHandle, e);
	e = OOSQL_ResetPageAccessed(systemHandle);
	CHECK_ERROR(systemHandle, e);

    e = OOSQL_AllocHandle(systemHandle, volId, &handle); CHECK_ERROR(systemHandle, e);

	while(1)
	{
		e = OOSQL_Prepare(systemHandle, handle, queryStr, sortBufferInfo);
		if(e == eNOERROR)
			break;
		else if(e == eVOLUMELOCKBLOCK_OOSQL)
		{
			char input[10];

			printf("Another process is using current database volume\n");
			printf("Retry operation or stop [y - retry/others - stop] : ");

			while(1)
			{
				if(gets(input) == NULL)
					continue;
				else
					break;
			}
			
			if(!strcmp(input, "y") || !strcmp(input, "Y"))
				continue;
			else
			{
				e = OOSQL_FreeHandle(systemHandle, handle); CHECK_ERROR(systemHandle, e);
				PRINT_ERROR_MESSAGE(systemHandle, handle, e);
				return eISQL_INTERNAL_ERROR;
			}
		}
		else if(e == eNEEDMORESORTBUFFERMEMORY_OOSQL)
		{
			sortBufferInfo->memoryInfo.sortBufferPtr = realloc(sortBufferInfo->memoryInfo.sortBufferPtr, sortBufferInfo->memoryInfo.sortBufferUsedLength);
			sortBufferInfo->memoryInfo.sortBufferLength = sortBufferInfo->memoryInfo.sortBufferUsedLength;
		}
		else if(e < eNOERROR)
		{
			e2 = OOSQL_FreeHandle(systemHandle, handle); CHECK_ERROR(systemHandle, e2);
			PRINT_ERROR_MESSAGE(systemHandle, handle, e);
			return eISQL_INTERNAL_ERROR;
		}
	}

	e = OOSQL_EstimateNumResults(systemHandle, handle, &nResults);
	CHECK_QUERY_ERROR(systemHandle, handle, e);

	printf("Estimated number of results = %d\n", nResults);

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
		e = OOSQL_GetResultColName(systemHandle, handle, (Two)i, stringAttr, sizeof(stringAttr));
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
            e = OOSQL_GetResultColType(systemHandle, handle, (Two)i, &type); CHECK_QUERY_ERROR(systemHandle, handle, e);
            switch(type)
            {
            case OOSQL_TYPE_CHAR:
            case OOSQL_TYPE_VARCHAR:
                e = OOSQL_GetData(systemHandle, handle, (Two)i, 0, &stringAttr, sizeof(stringAttr), &retLength); CHECK_QUERY_ERROR(systemHandle, handle, e);
				if(retLength == -1)
					printf("%10s|", "NULL");
				else
				{
					stringAttr[retLength] = '\0';
					printf("%10s|", stringAttr);
				}
                break;
            case OOSQL_TYPE_SMALLINT:
                e = OOSQL_GetData(systemHandle, handle, (Two)i, 0, &shortAttr, sizeof(short), &retLength); CHECK_QUERY_ERROR(systemHandle, handle, e);
				if(retLength == -1)
					printf("%10s|", "NULL");
				else
					printf("%10d|", (int)shortAttr);
                break;
            case OOSQL_TYPE_INTEGER:
            case OOSQL_TYPE_LONG:
                e = OOSQL_GetData(systemHandle, handle, (Two)i, 0, &intAttr, sizeof(int), &retLength); CHECK_QUERY_ERROR(systemHandle, handle, e);
				if(retLength == -1)
					printf("%10s|", "NULL");
				else
					printf("%10d|", intAttr);
                break;
			case OOSQL_TYPE_TIME:
				e = OOSQL_GetData(systemHandle, handle, (Two)i, 0, &timeAttr, sizeof(OOSQL_Time), &retLength); CHECK_QUERY_ERROR(systemHandle, handle, e);
				if(retLength == -1)
   		            printf("%10s|", "NULL");
    			else
    				printf("  %2d:%2d:%2d|", OOSQL_GetHour(systemHandle, &timeAttr),
    				                         OOSQL_GetMinute(systemHandle, &timeAttr),
    				                         OOSQL_GetSecond(systemHandle, &timeAttr));
                break;
            case OOSQL_TYPE_DATE:
                e = OOSQL_GetData(systemHandle, handle, (Two)i, 0, &dateAttr, sizeof(OOSQL_Date), &retLength); CHECK_QUERY_ERROR(systemHandle, handle, e);
				if(retLength == -1)
   		            printf("%10s|", "NULL");
    			else
    				printf("%4d-%2d-%2d|", OOSQL_GetYear(systemHandle, &dateAttr),
    				                       OOSQL_GetMonth(systemHandle, &dateAttr),
    				                       OOSQL_GetDay(systemHandle, &dateAttr));
                break;
			case OOSQL_TYPE_TIMESTAMP:
				e = OOSQL_GetData(systemHandle, handle, (Two)i, 0, &timestampAttr, sizeof(OOSQL_Timestamp), &retLength); CHECK_QUERY_ERROR(systemHandle, handle, e);
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
                e = OOSQL_GetData(systemHandle, handle, (Two)i, 0, &floatAttr, sizeof(float), &retLength); CHECK_QUERY_ERROR(systemHandle, handle, e);
				if(retLength == -1)
					printf("%10s|", "NULL");
				else
					printf("%10f|", floatAttr);
                break;
            case OOSQL_TYPE_DOUBLE:
                e = OOSQL_GetData(systemHandle, handle, (Two)i, 0, &doubleAttr, sizeof(double), &retLength); CHECK_QUERY_ERROR(systemHandle, handle, e);
				if(retLength == -1)
					printf("%10s|", "NULL");
				else
					printf("%10f|", doubleAttr);
                break;
            case OOSQL_TYPE_TEXT:
                e = OOSQL_GetData(systemHandle, handle, (Two)i, 0, &stringAttr, sizeof(stringAttr), &retLength); CHECK_QUERY_ERROR(systemHandle, handle, e);
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
				e = OOSQL_GetData(systemHandle, handle, (Two)i, 0, &oid, sizeof(OID), &retLength); CHECK_QUERY_ERROR(systemHandle, handle, e);
				if(retLength == -1)
					printf("%10s|", "NULL");
				else
				{
					e = OOSQL_OIDToOIDString(systemHandle, &oid, stringAttr); CHECK_QUERY_ERROR(systemHandle, handle, e);
#ifdef GEOM_INCLUDED
					e = OOSQL_Spatial_IsSpatialObject(systemHandle, volId, &oid); CHECK_QUERY_ERROR(systemHandle, handle, e);
#else
					e = FALSE;
#endif
					if(e == TRUE)
					{
						OOSQL_Point	points[MAXPOINTS];
						Four		nPoints;

						e = OOSQL_Spatial_GetPoints(systemHandle, volId, &oid, 0, MAXPOINTS, points);
						CHECK_QUERY_ERROR(systemHandle, handle, e);
						nPoints = e;

						for(j = 0; j < nPoints; j++)
						{
							printf("(%d,%d)", points[j].x, points[j].y);
							if(j < nPoints - 1)
								printf(",");
							else
								printf("|");
						}
					}
					else
						printf("%10s|", stringAttr);
				}
				break;
			default:
				if(OOSQL_MASK_COMPLEXTYPE(type) == OOSQL_COMPLEXTYPE_SET ||
				   OOSQL_MASK_COMPLEXTYPE(type) == OOSQL_COMPLEXTYPE_BAG ||
				   OOSQL_MASK_COMPLEXTYPE(type) == OOSQL_COMPLEXTYPE_LIST)
				{
					e = OOSQL_GetComplexTypeInfo(systemHandle, handle, (Two)i, &complexTypeInfo);
					CHECK_QUERY_ERROR(systemHandle, handle, e);

					e = OOSQL_ComplexType_GetNumElements(&complexTypeInfo, &nElements);
					CHECK_QUERY_ERROR(systemHandle, handle, e);

					e = OOSQL_ComplexType_GetElementsString(&complexTypeInfo, 0, nElements, stringAttr, sizeof(stringAttr));
					CHECK_QUERY_ERROR(systemHandle, handle, e);

					printf("%10s|", stringAttr);
				}
				else
					printf("%10s|", "...");
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
	OOSQL_ReportTimeAndPageAccess(systemHandle);

	e = OOSQL_FreeHandle(systemHandle, handle); CHECK_ERROR(systemHandle, e);

    return eNOERROR;
}

void StringStrip(char* string)
{
	int right, left;
	int i, j;
	int length;

	/* eliminate left and right white space */
	length = strlen(string);
	for(left = 0; left < length; left++)
	{
		if((unsigned)string[left] >= (unsigned)0x80 || !isspace(string[left]))
			break;
	}

	for(right = length - 1; right >= 0; right--)
	{
		if((unsigned)string[right] >= (unsigned)0x80 || !isspace(string[right]))
			break;
	}
	
	for(j = 0, i = left; i <= right; i++, j++)
	{
		string[j] = string[i];
	}
	string[j] = 0;
}

int execISQL( 
	OOSQL_SystemHandle*	  systemHandle,		
    Four				  volId,
    OOSQL_SortBufferInfo* sortBufferInfo
)
{
    char            queryStr[16 * 1024];
    char            catStr[1024 * 4];
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
			StringStrip(catStr);

            strcat(queryStr, catStr);
			
            if(queryStr[strlen(queryStr) - 1] == ';')
            {   
                queryStr[strlen(queryStr) - 1] = '\0';
                break;
            }
            else if(strcmp(queryStr, "quit") == 0) 
                return eNOERROR;
			else if(strcmp(queryStr, "commit") == 0)
			{

				printf("OOSQL_TransCommit\n");
				e = OOSQL_TransCommit(systemHandle, &xactID);
				CHECK_ERROR_XCT(systemHandle, e, &xactID);

				printf("OOSQL_TransBegin\n");
				e = OOSQL_TransBegin(systemHandle, &xactID, indexToCclevel[cclevelIndex]);
				CHECK_ERROR_XCT(systemHandle, e, &xactID);

				memset(queryStr, 0, sizeof(queryStr));
				printf("OOSQL> ");
			}
			else if(strcmp(queryStr, "abort") == 0) 
			{
				printf("OOSQL_TransAbort\n");
				e = OOSQL_TransAbort(systemHandle, &xactID);
				CHECK_ERROR_XCT(systemHandle, e, &xactID);

				printf("OOSQL_TransBegin\n");
				e = OOSQL_TransBegin(systemHandle, &xactID, indexToCclevel[cclevelIndex]);
				CHECK_ERROR_XCT(systemHandle, e, &xactID);

				memset(queryStr, 0, sizeof(queryStr));
				printf("OOSQL> ");
			}
			else
			{
				strcat(queryStr, "\n");
				lineno ++;
				printf("       ");
				fflush(stdout);
			}
        }
        e = execSQL(systemHandle, volId, queryStr, sortBufferInfo);
        if(e < 0) printf("SQL Error\n");
		lineno = 1;
    }

    return eNOERROR;
}

int usage(void)
{
	printf("USAGE : isql [-cclevel <cuncurrency level>] <database name> [<volume name>] [-temporary <database name> [<volume name>]]\n");
	printf("\n");
	printf("        currency level : 0, 1, 2, 3, 4, 5\n");
	printf("                         0 means X_BROWSE_BROWSE,\n");
	printf("                         1 means X_CS_BROWSE,\n");
	printf("                         2 means X_CS_CS,\n");
	printf("                         3 means X_RR_BROWSE,\n");
	printf("                         4 means X_RR_CS,\n");
	printf("                         5 means X_RR_RR. Default value is 5 (X_RR_RR)\n");
	printf("\n");
	printf("        temporary : indicating that isql uses separate volume to process\n");
	printf("                    orderby, group by, distinct\n");
	exit(1);

	return 0;
}

int main(int argc, char* argv[])
{
    Four				 volID;
	Four				 temporaryVolID;
	Four				 databaseID;
    Four				 e;
	OOSQL_SystemHandle	 systemHandle;
    OOSQL_SortBufferInfo sortBufferInfo;
	char*				 databaseName;
	char*				 volumeName;
	char*			     temporaryDatabaseName;
	char*				 temporaryVolumeName;
	Four				 count;

	puts(OOSQL_GetVersionString());

	/* parse argument */
	count = 1;
	if (count >= argc) usage();
	if(!strcmp(argv[count], "-cclevel"))
	{
		count++;	/* skip "-cclevel" */
		cclevelIndex = atoi(argv[count]);
		count++;
	}
	else
		cclevelIndex = 5;

	if(count >= argc) usage();
	databaseName = argv[count++];

	if ((argc > (count + 1) && !strcmp(argv[count + 1], "-temporary")) || (argc - 1) == count)		/* next argument is -temporary or there are one more argument to parse.  */
																									/* these argument consist of volumeName, datafilename */
		volumeName = argv[count++];
	else           
		volumeName = databaseName;

	if(count < argc && !strcmp(argv[count], "-temporary"))		/* temporary db is given */
	{
		count++;	/* skip "-temporary" */
		temporaryDatabaseName = argv[count++];
		if(argc > count)	/* there are one more arguments, that means temporary volume name is given */
			temporaryVolumeName = argv[count++];
		else
			temporaryVolumeName = temporaryDatabaseName;
	}
	else
	{
		temporaryDatabaseName = NULL;
		temporaryVolumeName   = NULL;
	}

    printf("OOSQL_Init\n");
    e = OOSQL_CreateSystemHandle(&systemHandle, &procIndex);
    CHECK_ERROR_XCT(NULL, e, NULL);
	printf("\n");

    e = OOSQL_MountDB(&systemHandle, databaseName, &databaseID);
    CHECK_ERROR_XCT(&systemHandle, e, NULL);
    printf("Database %s is mounted\n", databaseName);

	e = OOSQL_GetVolumeID(&systemHandle, databaseID, volumeName, &volID);
	CHECK_ERROR_XCT(&systemHandle, e, NULL);

	if(temporaryDatabaseName)
	{
		e = OOSQL_MountVolumeByVolumeName(&systemHandle, temporaryDatabaseName, temporaryVolumeName, &temporaryVolID);
		CHECK_ERROR_XCT(&systemHandle, e, NULL);
	}
	else
		temporaryVolID = volID;

	sortBufferInfo.mode = OOSQL_SB_USE_MEMORY;
	sortBufferInfo.memoryInfo.sortBufferPtr = (char *)malloc(sizeof(char) * SORT_BUFFER_SIZE);
	sortBufferInfo.memoryInfo.sortBufferLength = SORT_BUFFER_SIZE;
	sortBufferInfo.diskInfo.sortVolID = temporaryVolID;
    
    printf("OOSQL_TransBegin\n");
    e = OOSQL_TransBegin(&systemHandle, &xactID, indexToCclevel[cclevelIndex]);
    CHECK_ERROR_XCT(&systemHandle, e, &xactID);

	e = execISQL(&systemHandle, volID, &sortBufferInfo);
	CHECK_ERROR_XCT(&systemHandle, e, &xactID);

    printf("OOSQL_TransCommit\n");
    e = OOSQL_TransCommit(&systemHandle, &xactID);
    CHECK_ERROR_XCT(&systemHandle, e, &xactID);

    printf("OOSQL_Dismount\n");
    e = OOSQL_DismountDB(&systemHandle, databaseID);
    CHECK_ERROR_XCT(&systemHandle, e, NULL);

	if(temporaryVolID != volID)
	{
		e = OOSQL_Dismount(&systemHandle, temporaryVolID);
		CHECK_ERROR_XCT(&systemHandle, e, NULL);
    }

    printf("OOSQL_Final\n");
    e = OOSQL_DestroySystemHandle(&systemHandle, procIndex);
    CHECK_ERROR_XCT(&systemHandle, e, NULL);
    
	free(sortBufferInfo.memoryInfo.sortBufferPtr);

    return 1;
}
