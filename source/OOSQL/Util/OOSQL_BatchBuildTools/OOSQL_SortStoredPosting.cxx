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
#include <malloc.h>
#include <string.h>
#include <stdlib.h>
#include "OOSQL_Tool.hxx"

extern "C" Four procIndex;

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
void usage(
	char*	com			// IN 
)
{
    printf("usage: %s <database name> [<volume name>] [-temporary <database name> [<volume name>]] <class name> <attr name>\n", com);
}

char* databaseName;
char* volumeName;
char* className;
char* attrName;
char* temporaryDatabaseName;
char* temporaryVolumeName;

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
Four setOption
(
	int    argc,		// IN  
	char** argv			// IN 
)
{
	Four i = 1;

	if (i >= argc) return -1;
	databaseName = argv[i++];

	if (i >= argc) return -1;
	if ((argc > (i + 1) && !strcmp(argv[i + 1], "-temporary")) || (argc - 1) == i + 2)	// next argument is -temporary or there are three more argument to parse. 
		                                                                                // these argument consist of volumeName, className, attributeName
		volumeName = argv[i++];
	else           
		volumeName = NULL;

	if (i >= argc) return -1;
	if(!strcmp(argv[i], "-temporary"))		// temporary db is given
	{
		i++;	// skip "-temporary"
		temporaryDatabaseName = argv[i++];
		if(argc > i + 2)					// there are three more arguments, that means temporary volume name is given
			temporaryVolumeName = argv[i++];
		else
			temporaryVolumeName = temporaryDatabaseName;
	}
	else
	{
		temporaryDatabaseName = NULL;
		temporaryVolumeName   = NULL;
	}

	if (i >= argc) return -1;
	className = argv[i++];

	if (i >= argc) return -1;
	attrName = argv[i++];

	return eNOERROR;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
int main(int argc, char* argv[])
{
    Four   e;
    Four   databaseId;
    Four   volId;
	Four   temporaryVolId;
    XactID xactId;
    OOSQL_SystemHandle handle;

    if (setOption(argc, argv) < 0)
    {
		usage(argv[0]);
        exit(1);
    }

	e = oosql_Tool_Initialize(&handle, &procIndex, databaseName, volumeName, &databaseId, &volId, 
		           temporaryDatabaseName, temporaryVolumeName, &temporaryVolId, 
				   &xactId, X_RR_RR);
    ERROR_CHECK(&handle, e, procIndex, NULL);

    e = oosql_Tool_SortStoredPosting(&handle, volId, temporaryVolId, className, attrName);
    ERROR_CHECK(&handle, e, procIndex, NULL);

	if(temporaryVolId == volId) temporaryVolId = NIL;
	e = oosql_Tool_Finalize(&handle, procIndex, databaseId, temporaryVolId, &xactId);
    ERROR_CHECK(&handle, e, procIndex, NULL);

    return eNOERROR;
}
