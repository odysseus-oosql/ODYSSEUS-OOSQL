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

#ifndef OOSQL_TOOL_HXX
#define OOSQL_TOOL_HXX

#include "OOSQL_StorageSystemHeaders.h"
#include "OOSQL_APIs_Internal.hxx"

#define MAXNUMOFATTR 256
#define MAXNUMOFCLASS 128
#ifndef MAXNAMELEN
#define MAXNAMELEN 50
#endif
#ifndef MAXLINELEN
#define MAXLINELEN 102400
#endif

#define OOSQL_CHECK_ERROR(e) \
do {\
	if ((e) < 0) OOSQL_ERR(e);\
} while(0);

#undef ERROR_CHECK
#define ERROR_CHECK(systemHandle, e, procIndex, xactId) \
do { \
if (e < 0) { \
    char errorMessage[4096]; \
    OOSQL_GetErrorName(systemHandle, e, errorMessage, sizeof(errorMessage)); \
    printf("OOSQL ERROR(%s) : ", errorMessage); \
    OOSQL_GetErrorMessage(systemHandle, e, errorMessage, sizeof(errorMessage)); \
    puts(errorMessage); \
    if((xactId) != NULL) OOSQL_TransAbort(systemHandle, xactId); \
    if(systemHandle) OOSQL_DestroySystemHandle(systemHandle, procIndex); \
    if (1) exit(1); \
}\
} while(0);

#ifdef __cplusplus
extern "C" {
#endif

Four oosql_Tool_Initialize(OOSQL_SystemHandle*, Four*, char*, char*, Four*, Four*, char*, char*, Four*, XactID*, ConcurrencyLevel);
Four oosql_Tool_Finalize(OOSQL_SystemHandle*, Four, Four, Four, XactID*);
Four oosql_Tool_BuildTextIndex(OOSQL_SystemHandle* systemHandle, Four volId, Four temporaryVolId, char* className, char* attrName, lom_Text_ConfigForInvertedIndexBuild* config);
Four oosql_Tool_DeleteTextIndex(OOSQL_SystemHandle* systemHandle, Four volId, char* className, char* attrName);
Four oosql_Tool_GetDatabaseStatistics(OOSQL_SystemHandle* systemHandle, char* databaseName, Four databaseId);
Four oosql_Tool_ExtractKeyword(OOSQL_SystemHandle* systemHandle, Four volId, char* className, char* attrName, char* dataFileName, char* outputFileName, Four startObjectNo, Four endObjectNo, Four alwaysUsePreviousPostingFile);
Four oosql_Tool_MapPosting(OOSQL_SystemHandle *systemHandle, Four volId, char* className, char* attrName, Four nPostingFiles, char** postingFileNames, char* newPostingFileName, char* oidFileName, Four sortMergeMode, char* pageRankFile, Four pageRankMode);
Four oosql_Tool_MergePosting(OOSQL_SystemHandle* systemHandle, Four nPostingFiles, char** postingFileNames, char* newPostingFileName);
Four oosql_Tool_SortPosting(char* postingFileName, char* sortedPostingFileName);
Four oosql_Tool_SortStoredPosting(OOSQL_SystemHandle* systemHandle, Four volId, Four temporaryVolId, char* className, char* attrName);
Four oosql_Tool_StorePosting(OOSQL_SystemHandle* systemHandle, Four volId, char* className, char* attrName, Boolean clearFlag);
Four oosql_Tool_UpdateTextDescriptor(OOSQL_SystemHandle* systemHandle, Four volId, char* className);
Four oosql_Tool_LoadDB(OOSQL_SystemHandle* systemHandle, Four volId, Four temporaryVolId, Boolean isDeferredTextIndexMode, Boolean smallUpdateFlag, Boolean useBulkloading, Boolean useDescriptorUpdating, char* datafileName, char* pageRankFileName, Four pageRankMode);
Four oosql_Tool_BatchDeleteFromFile(OOSQL_SystemHandle* systemHandle, Four volId, Four temporaryVolId, char* className, char* oidFileName);
Four oosql_Tool_BatchDeleteByDeferredDeletionList(OOSQL_SystemHandle* systemHandle,	Four volId, Four temporaryVolId, char* className);
Four oosql_Tool_ShowBatchDeleteStatus(OOSQL_SystemHandle* systemHandle, Four volId, char* className);
Four oosql_Tool_CleanBatchDeletionList(OOSQL_SystemHandle* systemHandle, Four volId, char* className);
Four oosql_Tool_CheckDataSyntax(OOSQL_SystemHandle* systemHandle, Four volId, char* datafileName, Boolean verboseFlag);

#ifdef __cplusplus
}
#endif

#endif // OOSQL_TOOL_HXX
