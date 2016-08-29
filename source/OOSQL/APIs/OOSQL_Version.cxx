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

#include "OOSQL_StorageSystemHeaders.h"
#include "OOSQL_APIs_Internal.hxx"
#include "OOSQL_Error.h"
#include "OOSQL_ServerQuery.hxx"

#define OOSQL_VERSION  "4.0"
#define OOSQL_REVISION "1"

/****************************************************************************
DESCRIPTION:

RETURN VALUE:

IMPLEMENTATION:
****************************************************************************/
char* OOSQL_GetVersionString(void)
{
	static char versionstring[256];

	sprintf(versionstring, "ODYSSEUS/OOSQL Version %s Revision %s - Production on %s %s\n", 
		                    OOSQL_VERSION, OOSQL_REVISION, __TIME__, __DATE__);
	strcat(versionstring,  "Copyright (C) 2011 by Kyu-Young Whang\n");
	strcat(versionstring,  "Database and Multimedia Laboratory\n");
	strcat(versionstring,  "Computer Science Department\n");
	strcat(versionstring,  "Korea Advanced Institute of Science and Technology (KAIST)");

	return versionstring;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
compilation parameter string

IMPLEMENTATION:
****************************************************************************/
char* OOSQL_GetCompilationParamString(void)
{
	static char paramstring[4096];
	char tempstring[256];

	strcpy(paramstring, "");
	sprintf(tempstring, "PAGESIZE : %ld\n", PAGESIZE); strcat(paramstring, tempstring);
	sprintf(tempstring, "TRAINSIZE : %ld\n", TRAINSIZE); strcat(paramstring, tempstring);
#ifdef RM_REDO_BUFFER_SIZE				// defined in only COSMOS-single
	sprintf(tempstring, "RM_REDO_BUFFER_SIZE : %ld\n", RM_REDO_BUFFER_SIZE); strcat(paramstring, tempstring);
#endif
#ifdef VOLUMELOCK_TIMEOUT_RETRY_WAIT	// defined in only COSMOS-single
	sprintf(tempstring, "VOLUMELOCK_TIMEOUT_RETRY_WAIT : %f\n", VOLUMELOCK_TIMEOUT_RETRY_WAIT); strcat(paramstring, tempstring);
#endif
#ifdef VOLUMELOCK_TIMEOUT_RETRY_NUMBER	// defined in only COSMOS-single
	sprintf(tempstring, "VOLUMELOCK_TIMEOUT_RETRY_NUMBER : %ld\n", VOLUMELOCK_TIMEOUT_RETRY_NUMBER); strcat(paramstring, tempstring);
#endif
	sprintf(tempstring, "NUM_PAGE_BUFS : %ld\n", NUM_PAGE_BUFS); strcat(paramstring, tempstring);
	sprintf(tempstring, "NUM_LOT_LEAF_BUFS : %ld\n", NUM_LOT_LEAF_BUFS); strcat(paramstring, tempstring);
#ifdef BFM_BULKFLUSH_DISKWRITE_SIZE		// defined in only COSMOS-single
	sprintf(tempstring, "BFM_BULKFLUSH_DISKWRITE_SIZE : %ld\n", BFM_BULKFLUSH_DISKWRITE_SIZE); strcat(paramstring, tempstring);
#endif
#ifdef BFM_BULKFLUSH_MAX_BUFFER_SIZE	// defined in only COSMOS-single
	sprintf(tempstring, "BFM_BULKFLUSH_MAX_BUFFER_SIZE : %ld\n", BFM_BULKFLUSH_MAX_BUFFER_SIZE); strcat(paramstring, tempstring);
#endif
	sprintf(tempstring, "MAXKEYLEN : %ld\n", MAXKEYLEN); strcat(paramstring, tempstring);
	sprintf(tempstring, "MAXNUMKEYPARTS : %ld\n", MAXNUMKEYPARTS); strcat(paramstring, tempstring);
	sprintf(tempstring, "MLGF_MAXNUM_KEYS : %ld\n", MLGF_MAXNUM_KEYS); strcat(paramstring, tempstring);
	sprintf(tempstring, "SIZE_OF_SORT_OUT_BUFFER : %ld\n", SIZE_OF_SORT_OUT_BUFFER); strcat(paramstring, tempstring);
	sprintf(tempstring, "SIZE_OF_SORT_IN_BUFFER : %ld\n", SIZE_OF_SORT_IN_BUFFER); strcat(paramstring, tempstring);
	sprintf(tempstring, "MAXNUMOFOPENRELS : %ld\n", MAXNUMOFOPENRELS); strcat(paramstring, tempstring);
	sprintf(tempstring, "MAX_NUM_OF_USEROPENRELS : %ld\n", MAX_NUM_OF_USEROPENRELS); strcat(paramstring, tempstring);
	sprintf(tempstring, "TEXT_TEMP_PATH : %s\n", TEXT_TEMP_PATH); strcat(paramstring, tempstring); strcat(paramstring, tempstring);
	sprintf(tempstring, "OOSQL_EVALBUFFER_MAXSTRINGSIZE : %ld\n", OOSQL_EVALBUFFER_MAXSTRINGSIZE); strcat(paramstring, tempstring);
	sprintf(tempstring, "OOSQL_MEMORYSORT_POSTINGSIZE : %ld\n", OOSQL_MEMORYSORT_POSTINGSIZE); strcat(paramstring, tempstring);
	sprintf(tempstring, "OOSQL_MAX_RETURN_VALUE_LENGTH : %ld\n", OOSQL_MAX_RETURN_VALUE_LENGTH); strcat(paramstring, tempstring);
	sprintf(tempstring, "OOSQL_MEMORYMANAGER_SIZE : %ld\n", OOSQL_MEMORYMANAGER_SIZE); strcat(paramstring, tempstring);
	sprintf(tempstring, "OOSQL_DEFAULT_INMEMORY_SORTBUFFER_FOR_SORTSTREAM : %ld\n", OOSQL_DEFAULT_INMEMORY_SORTBUFFER_FOR_SORTSTREAM); strcat(paramstring, tempstring);
	sprintf(tempstring, "TEXTIR_MAXNPOSTINGS_FOR_SUBPLANTERMINAL : %ld\n", TEXTIR_MAXNPOSTINGS_FOR_SUBPLANTERMINAL); strcat(paramstring, tempstring);
	sprintf(tempstring, "TEXTIR_MINNPOSTINGS_FOR_SUBPLANTERMINAL : %ld\n", TEXTIR_MINNPOSTINGS_FOR_SUBPLANTERMINAL); strcat(paramstring, tempstring);

#ifdef USE_LARGE_FILE
	sprintf(tempstring, "USE_LARGE_FILE : defined\n"); strcat(paramstring, tempstring);
#else 
	sprintf(tempstring, "USE_LARGE_FILE : undefined\n"); strcat(paramstring, tempstring);
#endif

#if defined(AIX64)
	sprintf(tempstring, "PLATFORM : AIX64\n"); strcat(paramstring, tempstring);
#elif defined(SOLARIS64)
	sprintf(tempstring, "PLATFORM : SOLARIS64\n"); strcat(paramstring, tempstring);
#elif defined(LINUX64)
	sprintf(tempstring, "PLATFORM : LINUX64\n"); strcat(paramstring, tempstring);
#elif defined(WIN32)
	sprintf(tempstring, "PLATFORM : WIN32\n"); strcat(paramstring, tempstring);
#elif defined(WIN64)
	sprintf(tempstring, "PLATFORM : WIN64\n"); strcat(paramstring, tempstring);
#else
	sprintf(tempstring, "PLATFORM : undefined\n"); strcat(paramstring, tempstring);
#endif

#ifdef EIGHT_NOT_DEFINED
	sprintf(tempstring, "EIGHT TYPE : undefined\n"); strcat(paramstring, tempstring);
#else
	sprintf(tempstring, "EIGHT TYPE : defined\n"); strcat(paramstring, tempstring);
#endif

#ifdef ORDEREDSET_BACKWARD_SCAN
	sprintf(tempstring, "ORDEREDSET BACKWARD SCAN : defined\n"); strcat(paramstring, tempstring);
#else
	sprintf(tempstring, "ORDEREDSET BACKWARD SCAN : undefined\n"); strcat(paramstring, tempstring);
#endif

	return paramstring;
}

