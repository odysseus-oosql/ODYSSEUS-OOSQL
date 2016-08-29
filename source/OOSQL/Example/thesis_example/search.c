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

#include "OOSQL_APIs.h"

#define ERROR_CHECK(e) \
do { \
if (e < 0) { \
    printf("File:%s Line:%d\n", __FILE__, __LINE__); fflush(stdout); \
	return e; \
}\
} while(0);

#define ERROR_CHECK_XCT(systemHandle, e, xactId) \
do { \
if (e < 0) { \
	char errorMessage[4096]; \
	OOSQL_GetErrorName(systemHandle, e, errorMessage, sizeof(errorMessage)); \
	printf("OOSQL ERROR(%s) : ", errorMessage); \
    OOSQL_GetErrorMessage(systemHandle, e, errorMessage, sizeof(errorMessage)); \
    puts(errorMessage); \
    if((xactId) != NULL) (int) OOSQL_TransAbort(systemHandle, xactId); \
        (int) OOSQL_DestroySystemHandle(systemHandle, procIndex); \
    if (1) exit(1); \
}\
} while(0);

#define MAXVARSTRINGLEN		1024
#define MAXQUERYSTRLEN		2048

Four procIndex;

int make_query(char* queryString)
{
	char line[MAXVARSTRINGLEN];
	char oosql[MAXQUERYSTRLEN];
	char buf[MAXVARSTRINGLEN];
	short and_required = 0;

	printf("\nSelect attribute:\n");
	gets(line);

	strcpy(oosql, "select ");
	strcat(oosql, line);
	strcat(oosql, "\nfrom Paper\n");
	strcat(oosql, "where ");

	printf("\nInput keyword to search\n");

	/* Get input and make query string */

	printf("title      : ");
	gets(line);
	if (strlen(line) > 0)  {
		sprintf(buf, "MATCH(title, %s) > 0", line);
		strcat(oosql, buf);
		and_required = 1;
	}

	printf("author     : ");
	gets(line);
	if (strlen(line) > 0)  {
		if (and_required) strcat(oosql, " and ");
		sprintf(buf, "MATCH(author, %s) > 0", line);
		strcat(oosql, buf);
		and_required = 1;
	}

	printf("affiliation: ");
	gets(line);
	if (strlen(line) > 0)  {
		if (and_required) strcat(oosql, " and ");
		sprintf(buf, "MATCH(affiliation, %s) > 0", line);
		strcat(oosql, buf);
		and_required = 1;
	}

	printf("abstract   : ");
	gets(line);
	if (strlen(line) > 0)  {
		if (and_required) strcat(oosql, " and ");
		sprintf(buf, "MATCH(abstract, %s) > 0", line);
		strcat(oosql, buf);
		and_required = 1;
	}

	printf("keyword    : ");
	gets(line);
	if (strlen(line) > 0)  {
		if (and_required) strcat(oosql, " and ");
		sprintf(buf, "MATCH(keyword, %s) > 0", line);
		strcat(oosql, buf);
		and_required = 1;
	}

	printf("journal    : ");
	gets(line);
	if (strlen(line) > 0)  {
		if (and_required) strcat(oosql, " and ");
		sprintf(buf, "journal = %s", line);
		strcat(oosql, buf);
		and_required = 1;
	}

	printf("language   : ");
	gets(line);
	if (strlen(line) > 0)  {
		if (and_required) strcat(oosql, " and ");
		sprintf(buf, "language = %s", line);
		strcat(oosql, buf);
		and_required = 1;
	}

	printf("year       : ");
	gets(line);
	if (strlen(line) > 0)  {
		if (and_required) strcat(oosql, " and ");
		sprintf(buf, "year = %s", line);
		strcat(oosql, buf);
		and_required = 1;
	}

	if (!and_required) 
		oosql[strlen(oosql)-6] = '\0';

	strcpy(queryString, oosql);

	return eNOERROR;
}

int execute_query(OOSQL_SystemHandle* systemHandle, char *queryStr, Four volId, Boolean sortFlag)
{
	OOSQL_Handle handle;
	char colName[MAXVARSTRINGLEN];
	char stringAttr[MAXVARSTRINGLEN];
	short shortAttr;
	int intAttr;
	float floatAttr;
	double doubleAttr;
	Two nCols;
	Four type;
	Four count = 0;
	Four e;
	Four i;
	OOSQL_SortBufferInfo 	sortBufferInfo;
	char                	*sortBuffer;

	printf("\nNow Executing requested query:\n");
	printf("%s\n", queryStr);

	e = OOSQL_AllocHandle(systemHandle, volId, &handle); 
	ERROR_CHECK(e);

	if (sortFlag)
	/* Use memory sort */
	{
		sortBuffer = (char *)malloc(sizeof(char)*1024*1024);

		/* Set up sort buffer */
		sortBufferInfo.mode = OOSQL_SB_USE_MEMORY;
		sortBufferInfo.memoryInfo.sortBufferPtr     = sortBuffer;
		sortBufferInfo.memoryInfo.sortBufferLength  = sizeof(sortBuffer);
		sortBufferInfo.diskInfo.sortVolID           = volId;
		e = OOSQL_Prepare(systemHandle, handle, queryStr, &sortBufferInfo);
	}
	else 
	/* Use disk sort */
	{
		e = OOSQL_Prepare(systemHandle, handle, queryStr, NULL);
	}
		
	if(e < eNOERROR)
	{
		e = OOSQL_FreeHandle(systemHandle, handle); 
		ERROR_CHECK(e);
	}

	e = OOSQL_GetNumResultCols(systemHandle, handle, &nCols); 
	ERROR_CHECK(e);

	e = OOSQL_Execute(systemHandle, handle); 
	if(e < eNOERROR)
	{
		e = OOSQL_FreeHandle(systemHandle, handle); 
		ERROR_CHECK(e);
	}

	while((e = OOSQL_Next(systemHandle, handle)) != ENDOFEVAL)
	{
		for(i = 0; i < nCols; i++)
		{
			e = OOSQL_GetResultColType(systemHandle, handle, i, &type); 
			ERROR_CHECK(e);

			e = OOSQL_GetResultColName(systemHandle, handle, i, colName, MAXVARSTRINGLEN);
			ERROR_CHECK(e);

			printf("%10s: ", colName);

			switch(type)
			{
			case OOSQL_TYPE_CHAR:
			case OOSQL_TYPE_VARCHAR:
			case OOSQL_TYPE_TEXT:
				e = OOSQL_GetData(systemHandle, handle, i, 0, &stringAttr, sizeof(stringAttr), NULL); 
				ERROR_CHECK(e);
				printf("%s\n", stringAttr);
				break;

			case OOSQL_TYPE_SMALLINT:
				e = OOSQL_GetData(systemHandle, handle, i, 0, &shortAttr, sizeof(short), NULL); 
				ERROR_CHECK(e);
				printf("%d\n", (int)shortAttr);
				break;

			case OOSQL_TYPE_INTEGER:
			case OOSQL_TYPE_LONG:
				e = OOSQL_GetData(systemHandle, handle, i, 0, &intAttr, sizeof(int), NULL); 
				ERROR_CHECK(e);
				printf("%d\n", intAttr);
				break;

			case OOSQL_TYPE_FLOAT:
				e = OOSQL_GetData(systemHandle, handle, i, 0, &floatAttr, sizeof(float), NULL); 
				ERROR_CHECK(e);
				printf("%f\n", floatAttr);
				break;

			case OOSQL_TYPE_DOUBLE:
				e = OOSQL_GetData(systemHandle, handle, i, 0, &doubleAttr, sizeof(double), NULL); 
				ERROR_CHECK(e);
				printf("%e\n", doubleAttr);
				break;
			}
		}

		count++;

		printf("\n");
	}

	printf("\nThere are %d matches for your query.\n", count);
	
	e = OOSQL_FreeHandle(systemHandle, handle);
	ERROR_CHECK(e);

	if (sortFlag) free(sortBuffer);

	return (eNOERROR);
}

int main(int argc, char **argv)
{
    char DBPATH[256];
	char queryString[MAXQUERYSTRLEN];
	Four databaseID;
    Four volID;
    XactID xactID;
    Four e;
	Boolean sortFlag = FALSE;
	OOSQL_SystemHandle systemHandle;

    if (argc !=2 && argc != 3)
    {
       printf("USAGE : search <database name> [-memsort]\n");
       exit(1);
    }

    printf("OOSQL_CreateSystemHandle\n");
    e = OOSQL_CreateSystemHandle(&systemHandle, &procIndex);
    ERROR_CHECK_XCT(&systemHandle, e, NULL);

	strcpy(DBPATH, argv[1]);

    printf("OOSQL_MountDB\n");
    e = OOSQL_MountDB(&systemHandle, DBPATH, &databaseID);
    ERROR_CHECK_XCT(&systemHandle, e, NULL);

	e = OOSQL_GetUserDefaultVolumeID(&systemHandle, databaseID, &volID);
    ERROR_CHECK_XCT(&systemHandle, e, NULL);
    
    printf("OOSQL_TransBegin\n");
    e = OOSQL_TransBegin(&systemHandle, &xactID, X_RR_RR);
    ERROR_CHECK_XCT(&systemHandle, e, &xactID);

	e = make_query(queryString);
	ERROR_CHECK_XCT(&systemHandle, e, &xactID);

	if (argc == 3 && !strcmp(argv[2], "-memsort"))
		sortFlag = TRUE;

	e = execute_query(&systemHandle, queryString, volID, sortFlag);
	ERROR_CHECK_XCT(&systemHandle, e, &xactID);

    printf("OOSQL_TransCommit\n");
    e = OOSQL_TransCommit(&systemHandle, &xactID);
    ERROR_CHECK_XCT(&systemHandle, e, &xactID);

    printf("OOSQL_DismountDB\n");
    e = OOSQL_DismountDB(&systemHandle, databaseID);
    ERROR_CHECK_XCT(&systemHandle, e, NULL);
    
    printf("OOSQL_DestroySystemHandle\n");
    e = OOSQL_DestroySystemHandle(&systemHandle, procIndex);
    ERROR_CHECK_XCT(&systemHandle, e, NULL);
    
    return eNOERROR;
}
