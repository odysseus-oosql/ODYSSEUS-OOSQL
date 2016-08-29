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
    printf("Error Code: %d\n", e); \
    printf("File:%s Line:%d\n", __FILE__, __LINE__); fflush(stdout); \
    return e; \
}\
} while(0);

#define ERROR_CHECK_XCT(systemHandle, e, xactId) \
do { \
    char errorMessage[4096]; \
    OOSQL_GetErrorName(systemHandle, e, errorMessage, sizeof(errorMessage)); \
    printf("OOSQL ERROR(%s) : ", errorMessage); \
    OOSQL_GetErrorMessage(systemHandle, e, errorMessage, sizeof(errorMessage));\
    puts(errorMessage); \
	if (e < 0) { \
    if((xactId) != NULL) (int) OOSQL_TransAbort(systemHandle, xactId); \
        (int) OOSQL_DestroySystemHandle(systemHandle, procIndex); \
    if (1) exit(1); \
}\
} while(0);


Four procIndex;

int execSQL(
	OOSQL_SystemHandle*	systemHandle,	
    Four    			volId,
    char*   			queryStr
)
{
    OOSQL_Handle        handle;
    Four                e;
    Two                 nCols;
    Four                type;
    short               shortAttr;
    int                 intAttr;
    float               floatAttr;
    double              doubleAttr;
    char                stringAttr[100];
    OID                 oid;
    char                oidString[33];
    Four                length;
    Four                i;

    e = OOSQL_AllocHandle(systemHandle, volId, &handle); ERROR_CHECK(e);

    e = OOSQL_Prepare(systemHandle, handle, queryStr, NULL);
    if(e < eNOERROR)
    {
        e = OOSQL_FreeHandle(systemHandle, handle); ERROR_CHECK(e);
    }

    e = OOSQL_GetNumResultCols(systemHandle, handle, &nCols); ERROR_CHECK(e);

    e = OOSQL_Execute(systemHandle, handle);
    if(e < eNOERROR)
    {
        e = OOSQL_FreeHandle(systemHandle, handle); ERROR_CHECK(e);
    }
        
    while((e = OOSQL_Next(systemHandle, handle)) != ENDOFEVAL)
    {
        for(i = 0; i < nCols; i++)
        {
            e = OOSQL_GetResultColType(systemHandle, handle, i, &type); ERROR_CHECK(e);
            switch(type)
            {
            case OOSQL_TYPE_CHAR:
            case OOSQL_TYPE_VARCHAR:
                e = OOSQL_GetData(systemHandle, handle, i, 0, &stringAttr, sizeof(stringAttr), NULL); ERROR_CHECK(e);
                printf("%10s ", stringAttr);
                break;
            case OOSQL_TYPE_SMALLINT:
                e = OOSQL_GetData(systemHandle, handle, i, 0, &shortAttr, sizeof(short), NULL); ERROR_CHECK(e);
                printf("%10d ", (int)shortAttr);
                break;
            case OOSQL_TYPE_INTEGER:
            case OOSQL_TYPE_LONG:
                e = OOSQL_GetData(systemHandle, handle, i, 0, &intAttr, sizeof(int), NULL); ERROR_CHECK(e);
                printf("%10d ", intAttr);
                break;
            case OOSQL_TYPE_FLOAT:
                e = OOSQL_GetData(systemHandle, handle, i, 0, &floatAttr, sizeof(float), NULL); ERROR_CHECK(e);
                printf("%10f ", floatAttr);
                break;
            case OOSQL_TYPE_DOUBLE:
                e = OOSQL_GetData(systemHandle, handle, i, 0, &doubleAttr, sizeof(double), NULL); ERROR_CHECK(e);
                printf("%10e ", doubleAttr);
                break;
            case OOSQL_TYPE_TEXT:
                e = OOSQL_GetData(systemHandle, handle, i, 0, &stringAttr, sizeof(stringAttr), &length); ERROR_CHECK(e);
                if(length == sizeof(stringAttr))
                    stringAttr[length - 1] = '\0';
                printf("%10s ", stringAttr);
                break;
            case OOSQL_TYPE_OID:
                e = OOSQL_GetData(systemHandle, handle, i, 0, &oid, sizeof(OID), NULL); ERROR_CHECK(e);
                e = OOSQL_OIDToOIDString(systemHandle, &oid, oidString); ERROR_CHECK(e);
                printf("%s ", oidString);
                break;
            }
        }
        printf("\n");
    }

    e = OOSQL_FreeHandle(systemHandle, handle); ERROR_CHECK(e);

    return eNOERROR;
}

int TestOID_Operation( 
	OOSQL_SystemHandle*	systemHandle,
    Four    			volID)
{
    OOSQL_Handle        handle;
    Four                e;
    OID                 oid[10];
    Four                type;
    char                queryString[512];
    char                oidstring[33];
    Four        i;

    /* create table */
    e = execSQL(systemHandle, volID, "create table test (a integer, b char(10))"); ERROR_CHECK(e);

    /* make new objects */
    for(i = 0; i < 10; i++)
    {
        sprintf(queryString, "insert into test values(%d, 'string%d')", i, i);
        e = execSQL(systemHandle, volID, queryString); ERROR_CHECK(e);
    }
    /* get oid */
    e = OOSQL_AllocHandle(systemHandle, volID, &handle); ERROR_CHECK(e);

    e = OOSQL_Prepare(systemHandle, handle, "select test from test", NULL);
    if(e < eNOERROR) { e = OOSQL_FreeHandle(systemHandle, handle); ERROR_CHECK(e); }

    e = OOSQL_Execute(systemHandle, handle); 
    if(e < eNOERROR) { e = OOSQL_FreeHandle(systemHandle, handle); ERROR_CHECK(e); }

    i = 0;
    while((e = OOSQL_Next(systemHandle, handle)) != ENDOFEVAL)
    {
        e = OOSQL_GetData(systemHandle, handle, 0, 0, &oid[i], sizeof(OID), NULL); ERROR_CHECK(e);
        i++;
    }
    e = OOSQL_FreeHandle(systemHandle, handle);
    ERROR_CHECK(e);

    /* select object by oid */
    for(i = 0; i < 10; i++)
    {
        /* convert oid to oidstring */
        e = OOSQL_OIDToOIDString(systemHandle, &oid[i], oidstring); ERROR_CHECK(e);

        /* select object by oid */
        sprintf(queryString, "select * from object '%s'", oidstring);
        e = execSQL(systemHandle, volID, queryString); ERROR_CHECK(e);
    }
    /* destroy table */
    e = execSQL(systemHandle, volID, "drop table test"); ERROR_CHECK(e);

    return eNOERROR;
}

int main(int argc, char* argv[])
{
	OOSQL_SystemHandle 	systemHandle;
    char        		DBPATH[256];
    Four        		volID;
    XactID      		xactID;
    Four        		e;  

    if(argc != 2)
    {
       printf("USAGE : oid <volume name>\n");
       exit(1);
    }

    printf("OOSQL_Init\n");
    e = OOSQL_CreateSystemHandle(&systemHandle, &procIndex);
    ERROR_CHECK_XCT(&systemHandle, e, NULL);

    strcpy(DBPATH, argv[1]);
    printf("about to open %s\n", DBPATH);

    e = OOSQL_MountDB(&systemHandle, DBPATH, &volID);
    ERROR_CHECK_XCT(&systemHandle, e, NULL);
    printf("Volume %s is mounted\n", DBPATH);
    
    printf("OOSQL_TransBegin\n");
    e = OOSQL_TransBegin(&systemHandle, &xactID, X_RR_RR);
    ERROR_CHECK_XCT(&systemHandle, e, &xactID);

    e = TestOID_Operation(&systemHandle, volID);
    ERROR_CHECK_XCT(&systemHandle, e, &xactID);

    printf("OOSQL_TransCommit\n");
    e = OOSQL_TransCommit(&systemHandle, &xactID);
    ERROR_CHECK_XCT(&systemHandle, e, &xactID);

    printf("OOSQL_Dismount\n");
    e = OOSQL_Dismount(&systemHandle, volID);
    ERROR_CHECK_XCT(&systemHandle, e, NULL);
    
    printf("OOSQL_Final\n");
    e = OOSQL_DestroySystemHandle(&systemHandle, procIndex);
    ERROR_CHECK_XCT(&systemHandle, e, NULL);
    
    return 0;
}

