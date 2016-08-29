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
if (e < 0) { \
    char errorMessage[4096]; \
    OOSQL_GetErrorName(systemHandle, e, errorMessage, sizeof(errorMessage)); \
    printf("OOSQL ERROR(%s) : ", errorMessage); \
    OOSQL_GetErrorMessage(systemHandle, e, errorMessage, sizeof(errorMessage));\
    puts(errorMessage); \
    if((xactId) != NULL) (int) OOSQL_TransAbort(systemHandle, xactId); \
        (int) OOSQL_DestroySystemHandle(systemHandle, procIndex); \
    if (1) exit(1); \
}\
} while(0);


Four procIndex;

int main( int argc, char* argv[] )
{
    Four               databaseID;
    Four               volID;
    XactID             xactID;
    Four               e;
    OOSQL_SystemHandle systemHandle;
	char*              keywordextractor_name;
	char*              class_name;
	char*              attribute_name;
	Four			   no;
	char*              databaseName;
	char*              volumeName;

    if(argc != 5 && argc != 6)
    {
       printf("USAGE : SetupKeywordExtractor <database name> [<volume name>] <keyword extractor name> <class name> <attribute name>\n");
       exit(1);
    }

	if(argc == 5)
	{
		databaseName            = argv[1];
		volumeName              = argv[1];
		keywordextractor_name   = argv[2];
		class_name              = argv[3];
		attribute_name			= argv[4];
	}
	else
	{
		databaseName            = argv[1];
		volumeName              = argv[2];
		keywordextractor_name   = argv[3];
		class_name              = argv[4];
		attribute_name			= argv[5];
	}

    printf("OOSQL_Init\n");
    e = OOSQL_CreateSystemHandle(&systemHandle, &procIndex);
    ERROR_CHECK_XCT(&systemHandle, e, NULL);

    printf("about to open %s(%s)\n", databaseName, volumeName);

    e = OOSQL_MountDB(&systemHandle, databaseName, &databaseID);
    ERROR_CHECK_XCT(&systemHandle, e, NULL);
    printf("Database %s is mounted\n", databaseName);

	e = OOSQL_GetVolumeID(&systemHandle, databaseID, volumeName, &volID);
    ERROR_CHECK_XCT(&systemHandle, e, NULL);

    printf("OOSQL_TransBegin\n");
    e = OOSQL_TransBegin(&systemHandle, &xactID, X_RR_RR);
    ERROR_CHECK_XCT(&systemHandle, e, &xactID);

	e = OOSQL_Text_GetKeywordExtractorNo(&systemHandle, volID, keywordextractor_name, 1, &no);
	ERROR_CHECK_XCT(&systemHandle, e, &xactID);

	e = OOSQL_Text_SetKeywordExtractor(&systemHandle, volID, class_name, attribute_name, no);
	ERROR_CHECK_XCT(&systemHandle, e, &xactID);

    printf("OOSQL_TransCommit\n");
    e = OOSQL_TransCommit(&systemHandle, &xactID);
    ERROR_CHECK_XCT(&systemHandle, e, &xactID);

    printf("OOSQL_Dismount\n");
    e = OOSQL_DismountDB(&systemHandle, databaseID);
    ERROR_CHECK_XCT(&systemHandle, e, NULL);
    
    printf("OOSQL_Final\n");
    e = OOSQL_DestroySystemHandle(&systemHandle, procIndex);
    ERROR_CHECK_XCT(&systemHandle, e, NULL);
    
    return 0;
}
