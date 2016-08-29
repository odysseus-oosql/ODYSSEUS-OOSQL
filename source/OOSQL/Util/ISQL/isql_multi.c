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
    char					stringAttr[512 * 1024];
    Four					i, j;
	Four					retLength;
	OID						oid;
	OOSQL_Date				dateAttr;
	OOSQL_Time				timeAttr;
	OOSQL_Timestamp			timestampAttr;
	Four					timeElapsed;
	Four					nElements;
	OOSQL_ComplexTypeInfo	complexTypeInfo;
    OOSQL_UDTObject         udtAttr;

	char* dataBuffer;
	char* headerBuffer;
	Four dataBufferSize;
	Four nResultsRead = 0;
	Four nTotalResultsRead = 0;
	Four headerBufferSize;
	char OIDString[20];

	dataBufferSize = 1024000;
	headerBufferSize = 1024000;
	dataBuffer = (char*)malloc(dataBufferSize);
	headerBuffer = (char*)malloc(headerBufferSize);
	

	printf("nResultsRead = %d\n", nResultsRead);
	e = OOSQL_ResetTimeElapsed(systemHandle);
	CHECK_ERROR(systemHandle, e);

	e = OOSQL_ResetPageAccessed(systemHandle);
	CHECK_ERROR(systemHandle, e);
	
	e = OOSQL_AllocHandle(systemHandle, volId, &handle); CHECK_ERROR(systemHandle, e);
	CHECK_ERROR(systemHandle, e);

	e = OOSQL_Prepare(systemHandle, handle, "select webpage,description,pageno,_logicalId from webpage where match(title, \"kaist\") > 0", sortBufferInfo);
	CHECK_ERROR(systemHandle, e);
		
    e = OOSQL_Execute(systemHandle, handle);
	CHECK_ERROR(systemHandle, e);

	e = OOSQL_GetMultipleResults(systemHandle, handle, -1, headerBuffer, headerBufferSize, dataBuffer, dataBufferSize, &nResultsRead);
	CHECK_ERROR(systemHandle, e);
	printf("nResultsRead = %d\n", nResultsRead);


	printf("size: %d\n", OOSQL_MULTIPLERESULT_NTH_OBJECT_ITH_COLUMN_SIZE(headerBuffer, 4, 0, 1));
	printf("Real size: %d\n", OOSQL_MULTIPLERESULT_NTH_OBJECT_ITH_COLUMN_REALSIZE(headerBuffer, 4, 0, 1));

	oid =  OOSQL_MULTIPLERESULT_NTH_OBJECT_ITH_COLUMN_OID(headerBuffer, 4, 0, 0);
	OOSQL_OIDToOIDString(systemHandle, &oid, OIDString); 
	CHECK_ERROR(systemHandle, e);
	printf("OIDString: %s\n", OIDString);

	oid =  OOSQL_MULTIPLERESULT_NTH_OBJECT_ITH_COLUMN_OID(headerBuffer, 4, 0, 1);
	OOSQL_OIDToOIDString(systemHandle, &oid, OIDString); 
	CHECK_ERROR(systemHandle, e);
	printf("OIDString: %s\n", OIDString);

	oid =  OOSQL_MULTIPLERESULT_NTH_OBJECT_ITH_COLUMN_OID(headerBuffer, 4, 0, 2);
	OOSQL_OIDToOIDString(systemHandle, &oid, OIDString); 
	CHECK_ERROR(systemHandle, e);
	printf("OIDString: %s\n", OIDString);

	oid =  OOSQL_MULTIPLERESULT_NTH_OBJECT_ITH_COLUMN_OID(headerBuffer, 4, 0, 3);
	OOSQL_OIDToOIDString(systemHandle, &oid, OIDString); 
	CHECK_ERROR(systemHandle, e);
	printf("OIDString: %s\n", OIDString);



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
    char            queryStr[512 * 1024];
    char            catStr[1024 * 512];
    int             e;
	int				lineno = 1;

    
    e = execSQL(systemHandle, volId, queryStr, sortBufferInfo);
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
