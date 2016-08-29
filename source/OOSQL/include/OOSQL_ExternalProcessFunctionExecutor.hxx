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

#ifndef _OOSQL_EXTERNALPROCESSFUNCTIONEXECUTOR_H_
#define _OOSQL_EXTERNALPROCESSFUNCTIONEXECUTOR_H_

#include "OOSQL_Common.h"
#include "OOSQL_MemoryManager.hxx"
#include "OOSQL_MemoryManagedObject.hxx"
#include "OOSQL_ExternalFunctionManager.hxx"
#include "OOSQL_DB_Value.hxx"
#include "OOSQL_ExternalProcessMessage.hxx"

class OOSQL_ExternalProcessFunctionExecutor : public OOSQL_MemoryManagedObject {
public:
	enum RunningMode {OOSQL_SERVER, EXTERNAL_PROCESS};

	OOSQL_ExternalProcessFunctionExecutor(OOSQL_SystemHandle* oosqlSystemHandle, RunningMode mode, const OOSQL_DbInfo& dbInfo);
	OOSQL_ExternalProcessFunctionExecutor(OOSQL_SystemHandle* oosqlSystemHandle, RunningMode mode, const OOSQL_DbInfo& dbInfo, OOSQL_MemoryManager* memoryManager);
	~OOSQL_ExternalProcessFunctionExecutor();
	
	Four Execute(OOSQL_ExternalFunctionInfo& functionInfo, OOSQL_DB_Value& returnValue, Four nParams, OOSQL_DB_Value* parameters, char* errorMessage, Four finalCall);
	Four SetPipes(int rPipe, int wPile);
	Four PipeWaitMessage(Four& ret, OOSQL_DB_Value* returnValue, char* errorMessage);
private:
	struct MessageHeader {
		OOSQL_ExternalProcessMessageID messageID;
		Four						   messageSize;
	};

	Four PipeOpen();
	Four PipeClose();
	Four PipeIsOpen();
	Four PipeReadHeader(OOSQL_ExternalProcessMessageID& messageID, Four& messageSize);
	Four PipeWriteHeader(OOSQL_ExternalProcessMessageID messageID, Four messageSize);
	Four PipeReadData(void* data, Four size);
	Four PipeWriteData(const void* data, Four size);

	int									m_rPipe;
	int									m_wPipe;
	pid_t								m_executorPid;
	int									m_pipe1[2];
	int									m_pipe2[2];
	RunningMode							m_runningMode;
	OOSQL_ExternalFunctionDispatcher*	m_externalFunctionDispatcher;
	OOSQL_SystemHandle*					m_oosqlSystemHandle;

public:
	Four 	OOSQL_CreateSystemHandle(OOSQL_SystemHandle* systemHandle, Four *procIndex);
	Four 	OOSQL_DestroySystemHandle(OOSQL_SystemHandle* systemHandle, Four procIndex);
	Four    OOSQL_Connect(char* hostAddress, char* protocolString, OOSQL_SystemHandle* systemHandle);
	Four    OOSQL_Disconnect(OOSQL_SystemHandle* systemHandle);
	Four 	OOSQL_SetUserDefaultVolumeID(OOSQL_SystemHandle* systemHandle, Four databaseID, Four volumeID);
	Four 	OOSQL_GetUserDefaultVolumeID(OOSQL_SystemHandle* systemHandle, Four databaseID, Four* volumeID);
	Four 	OOSQL_GetVolumeID(OOSQL_SystemHandle* systemHandle, Four databaseID, char* volumeName, Four* volumeID);
	Four 	OOSQL_MountDB(OOSQL_SystemHandle* systemHandle, char* databaseName, Four* databaseID);
	Four 	OOSQL_DismountDB(OOSQL_SystemHandle* systemHandle, Four databaseID);
	Four	OOSQL_AllocHandle(OOSQL_SystemHandle* systemHandle, Four volID, OOSQL_Handle* handle);
	Four	OOSQL_FreeHandle(OOSQL_SystemHandle* systemHandle, OOSQL_Handle handle);
	Four	OOSQL_Mount(OOSQL_SystemHandle* systemHandle, Four numDevices, char** devNames, Four* volID);
	Four	OOSQL_Dismount(OOSQL_SystemHandle* systemHandle, Four volID);
	Four	OOSQL_TransBegin(OOSQL_SystemHandle* systemHandle, XactID *xactId, ConcurrencyLevel ccLevel);
	Four	OOSQL_TransCommit(OOSQL_SystemHandle* systemHandle, XactID *xactId);
	Four	OOSQL_TransAbort(OOSQL_SystemHandle* systemHandle, XactID *xactId);
	Four	OOSQL_Prepare(OOSQL_SystemHandle* systemHandle, OOSQL_Handle handle, char* stmtText, OOSQL_SortBufferInfo* sortBufferInfo);
	Four	OOSQL_Execute(OOSQL_SystemHandle* systemHandle, OOSQL_Handle handle);
	Four	OOSQL_ExecDirect(OOSQL_SystemHandle* systemHandle, OOSQL_Handle handle, char* stmtText, OOSQL_SortBufferInfo* sortBufferInfo);
	Four	OOSQL_Next(OOSQL_SystemHandle* systemHandle, OOSQL_Handle handle);
	Four	OOSQL_GetData(OOSQL_SystemHandle* systemHandle, OOSQL_Handle handle, Two columnNumber, Four startPos, void* columnValuePtr, Four bufferLength, Four* dataLength);
	Four	OOSQL_GetMultiColumnData(OOSQL_SystemHandle* systemHandle, OOSQL_Handle handle, Four nColumns, OOSQL_GetDataStruct* getDataStruct);
	Four	OOSQL_PutData(OOSQL_SystemHandle* systemHandle, OOSQL_Handle handle, Two columnNumber, Four startPos, void* columnValuePtr, Four bufferLength);
	Four	OOSQL_GetOID(OOSQL_SystemHandle* systemHandle, OOSQL_Handle handle, Two targetNumber, OID* oid);
	Four	OOSQL_GetNumResultCols(OOSQL_SystemHandle* systemHandle, OOSQL_Handle handle, Two* nCols);
	Four	OOSQL_GetResultColName(OOSQL_SystemHandle* systemHandle, OOSQL_Handle handle, Two columnNumber, char* columnNameBuffer, Four bufferLength);
	Four	OOSQL_GetResultColType(OOSQL_SystemHandle* systemHandle, OOSQL_Handle handle, Two columnNumber, Four* columnType);
	Four	OOSQL_GetResultColLength(OOSQL_SystemHandle* systemHandle, OOSQL_Handle handle, Two columnNumber, Four* resultColLength);
	Four	OOSQL_GetErrorMessage(OOSQL_SystemHandle* systemHandle, Four errorCode, char* messageBuffer, Four bufferLength);
	Four	OOSQL_GetErrorName(OOSQL_SystemHandle* systemHandle, Four errorCode, char* messageBuffer, Four bufferLength);
	Four	OOSQL_GetQueryErrorMessage(OOSQL_SystemHandle* systemHandle, OOSQL_Handle handle, char* messageBuffer, Four bufferLength);
	Four	OOSQL_OIDToOIDString(OOSQL_SystemHandle* systemHandle, OID* oid, char* oidString);
	Four	OOSQL_Text_AddKeywordExtractor(OOSQL_SystemHandle* systemHandle, Four volID, char *keywordExtractor, Four version, char *keywordExtractorFilePath, char *keywordExtractorFunctionName, char *getNextPostingFunctionName, char *finalizeKeywordExtractorFunctionName, Four *keywordExtractorNo);
	Four	OOSQL_Text_AddDefaultKeywordExtractor(OOSQL_SystemHandle* systemHandle, Four volID, char *keywordExtractor, Four version, char *keywordExtractorFilePath, char *keywordExtractorFunctionName, char *getNextPostingFunctionName, char *finalizeKeywordExtractorFunctionName);
	Four	OOSQL_Text_DropKeywordExtractor(OOSQL_SystemHandle* systemHandle, Four volID, char *keywordExtractorName, Four version);
	Four	OOSQL_Text_SetKeywordExtractor(OOSQL_SystemHandle* systemHandle, Four volID, char* className, char* columeName, Four keywordExtractorNo);
	Four	OOSQL_Text_AddFilter(OOSQL_SystemHandle* systemHandle, Four volID, char *filterName, Four version, char *filterFilePath, char *filterFunctionName, Four *filterNo);
	Four	OOSQL_Text_DropFilter(OOSQL_SystemHandle* systemHandle, Four volID, char *filterName, Four version);
	Four	OOSQL_Text_SetFilter(OOSQL_SystemHandle* systemHandle, Four volID, char* className, char* columnName, Four filterNo);
	Four	OOSQL_Text_MakeIndex(OOSQL_SystemHandle* systemHandle, Four volID, Four temporaryVolId, char* className);
	Four 	OOSQL_Text_KeywordInfoScan_Open(OOSQL_SystemHandle* systemHandle, Four volID, char* className, char* columnName, char* keyword);
	Four 	OOSQL_Text_KeywordInfoScan_Close(OOSQL_SystemHandle* systemHandle, Four scanId);
	Four 	OOSQL_Text_KeywordInfoScan_Next(OOSQL_SystemHandle* systemHandle, Four scanId, char* keyword, Four* nDocuments, Four* nPositions);
	Four 	OOSQL_Text_KeywordInfoScanForDocument_Open(OOSQL_SystemHandle* systemHandle, Four volID, char* className, OID* oid, char* columnName, char* keyword);
	Four 	OOSQL_Text_KeywordInfoScanForDocument_Close(OOSQL_SystemHandle* systemHandle, Four scanId);
	Four 	OOSQL_Text_KeywordInfoScanForDocument_Next(OOSQL_SystemHandle* systemHandle, Four  scanId, char* keyword, Four* nDocuments, Four* nPositions);
	Four	OOSQL_Text_DefinePostingStructure(OOSQL_SystemHandle *systemHandle, Four volID, char *className, char *attrName, OOSQL_PostingStructureInfo *postingInfo);
	OOSQL_TimeZone OOSQL_GetLocalTimeZone(OOSQL_SystemHandle* systemHandle);
	void OOSQL_SetCurTime(OOSQL_SystemHandle* systemHandle, OOSQL_Time* time, OOSQL_TimeZone tz);
	unsigned short OOSQL_GetHour(OOSQL_SystemHandle* systemHandle, OOSQL_Time* time);
	unsigned short OOSQL_GetMinute(OOSQL_SystemHandle* systemHandle, OOSQL_Time* time);
	unsigned short OOSQL_GetSecond(OOSQL_SystemHandle* systemHandle, OOSQL_Time* time);
	unsigned short OOSQL_GetYear(OOSQL_SystemHandle* systemHandle, OOSQL_Date* date);
	unsigned short OOSQL_GetMonth(OOSQL_SystemHandle* systemHandle, OOSQL_Date* date);
	unsigned short OOSQL_GetDay(OOSQL_SystemHandle* systemHandle, OOSQL_Date* date);
	char* OOSQL_GetVersionString(void);
	Four OOSQL_GetTimeElapsed(OOSQL_SystemHandle* systemHandle, Four* timeInMilliSeconds);
	Four OOSQL_ResetTimeElapsed(OOSQL_SystemHandle* systemHandle);
};

#endif

