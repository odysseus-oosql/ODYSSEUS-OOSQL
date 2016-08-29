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

#include "OOSQL_ExternalProcessFunctionExecutor.hxx"
#include "OOSQL_ExternalFunctionDispatcher.hxx"
#include "OOSQL_Error.h"

#ifndef WIN32
#include <unistd.h>
#else
#include <io.h>
#include <process.h>
#endif
#include <fcntl.h>
#include <string.h>

#ifdef TEST_EXTERNAL_FUNCTION_EXCUTOR_WITH_THREAD
#define CHECK_ERROR(e) \
do { \
if (e < 0) { \
    { \
	    char errorMessage[4096]; \
		OOSQL_GetErrorName(NULL, e, errorMessage, sizeof(errorMessage)); \
		printf("OOSQL ERROR(%s) : ", errorMessage); \
	    OOSQL_GetErrorMessage(NULL, e, errorMessage, sizeof(errorMessage)); \
	    puts(errorMessage); \
		exit(1); \
	} \
}\
} while(0);
int threadArgument[2];
OOSQL_ExternalProcessFunctionExecutor*	externalProcessFunctionExecutor;
void ExecutorThread(void* arg0)
{
	int										rPipe;
	int										wPipe;
	Four									e, ret;
	Four									messageID, messageSize;
	OOSQL_SystemHandle						oosqlSystemHandle;
	OOSQL_DB_Value							returnValue;
	int*									argument;
	char									errorMessage[OOSQL_EXTFUNC_ERRORMESSAGE_SIZE];
	OOSQL_DbInfo							dbInfo;

	puts("Client is invoked");

	// read rPipe, wPipe
	argument = (int*)arg0;

	rPipe = argument[0];
	wPipe = argument[1];

	// read systemhande from rPipe
	e = read(rPipe, &messageID, sizeof(messageID)); 
	if(e == -1) { puts("pipe read error"); exit(1); }

	e = read(rPipe, &messageSize, sizeof(messageSize)); 
	if(e == -1) { puts("pipe read error"); exit(1); }

	e = read(rPipe, &oosqlSystemHandle, sizeof(oosqlSystemHandle)); 
	if(e == -1) { puts("pipe read error"); exit(1); }

	// initialize memory manager
	OOSQL_MemoryManager* pMemoryManager;

	//pMemoryManager = new OOSQL_QuickFitMemoryManager;
	pMemoryManager = new OOSQL_MemoryManager;

	// initialize external process function executor
	OOSQL_NEW(externalProcessFunctionExecutor, pMemoryManager, 
		      OOSQL_ExternalProcessFunctionExecutor(&oosqlSystemHandle,
		                                            OOSQL_ExternalProcessFunctionExecutor::EXTERNAL_PROCESS,
													dbInfo));

	e = externalProcessFunctionExecutor->SetPipes(rPipe, wPipe);
	CHECK_ERROR(e);

	e = externalProcessFunctionExecutor->PipeWaitMessage(ret, &returnValue, errorMessage);
	CHECK_ERROR(e);

	// destroy external process function executor
	OOSQL_DELETE(externalProcessFunctionExecutor);

	// destroy memory manager
	delete pMemoryManager;

	puts("Client is dead");
}
#endif

OOSQL_ExternalProcessFunctionExecutor::OOSQL_ExternalProcessFunctionExecutor(OOSQL_SystemHandle* oosqlSystemHandle, RunningMode mode, const OOSQL_DbInfo& dbInfo) 
{ 
	m_rPipe			= -1;
	m_wPipe			= -1;
	m_pipe1[0]		= -1;
	m_pipe1[1]		= -1;
	m_pipe2[0]		= -1;
	m_pipe2[1]		= -1;
	m_executorPid	= -1;

	m_runningMode		= mode;
	m_oosqlSystemHandle = oosqlSystemHandle;

	if(m_runningMode == EXTERNAL_PROCESS)
	{
		OOSQL_NEW(m_externalFunctionDispatcher, pMemoryManager, OOSQL_ExternalFunctionDispatcher(m_oosqlSystemHandle, dbInfo));
	}
	else
		m_externalFunctionDispatcher = NULL;
}

OOSQL_ExternalProcessFunctionExecutor::OOSQL_ExternalProcessFunctionExecutor(OOSQL_SystemHandle* oosqlSystemHandle, RunningMode mode, const OOSQL_DbInfo& dbInfo, OOSQL_MemoryManager* memoryManager) 
: OOSQL_MemoryManagedObject(memoryManager) 
{
	m_rPipe			= -1;
	m_wPipe			= -1;
	m_pipe1[0]		= -1;
	m_pipe1[1]		= -1;
	m_pipe2[0]		= -1;
	m_pipe2[1]		= -1;
	m_executorPid	= -1;

	m_runningMode	= mode;
	m_oosqlSystemHandle = oosqlSystemHandle;

	if(m_runningMode == EXTERNAL_PROCESS)
	{
		OOSQL_NEW(m_externalFunctionDispatcher, pMemoryManager, OOSQL_ExternalFunctionDispatcher(m_oosqlSystemHandle, dbInfo));
	}
	else
		m_externalFunctionDispatcher = NULL;
}

OOSQL_ExternalProcessFunctionExecutor::~OOSQL_ExternalProcessFunctionExecutor()
{
	Four e;

	if(PipeIsOpen())
	{
		e = PipeClose();
		OOSQL_PRTERR(e);
	}

	if(m_runningMode == EXTERNAL_PROCESS)
	{
		OOSQL_DELETE(m_externalFunctionDispatcher);
	}
}

Four OOSQL_ExternalProcessFunctionExecutor::Execute(OOSQL_ExternalFunctionInfo& functionInfo, OOSQL_DB_Value& returnValue, Four nParams, OOSQL_DB_Value* parameters, char* errorMessage, Four finalCall)
{
	// make empty errorMessage
	strcpy(errorMessage, "");

	if(m_runningMode == OOSQL_SERVER)
	{
		Four e;
		Four i;
		Four messageSize;
		Four ret;

		// check and open pipe
		if(!PipeIsOpen())
		{
			e = PipeOpen();
			OOSQL_CHECK_ERR(e);
		}

		// calculate message size
		messageSize = 0;
		messageSize += sizeof(OOSQL_ExternalFunctionInfo);
		for(i = 0; i < nParams; i++)
		{
			messageSize += sizeof(Four);
			messageSize += parameters[i].length;
		}
			
		// write header
		e = PipeWriteHeader(OOSQL_EXTPROCMSG_ID_CALL_FUNCTION, messageSize);
		OOSQL_CHECK_ERR(e);

		// write data
		e = PipeWriteData(&functionInfo, sizeof(OOSQL_ExternalFunctionInfo));
		OOSQL_CHECK_ERR(e);

		e = PipeWriteData(&nParams, sizeof(Four));
		OOSQL_CHECK_ERR(e);

		for(i = 0; i < nParams; i++)
		{
			e = PipeWriteData(&parameters[i].length, sizeof(Four)); OOSQL_CHECK_ERR(e);
			e = PipeWriteData(&parameters[i].type, sizeof(Four)); OOSQL_CHECK_ERR(e);
			e = PipeWriteData(&parameters[i].nullFlag, sizeof(Boolean)); OOSQL_CHECK_ERR(e);

			switch(parameters[i].type)
			{
			case TYPEID_STRING:
			case TYPEID_VARSTRING:
			case TYPEID_TEXT:
				e = PipeWriteData(parameters[i].data.ptr, parameters[i].length);
				OOSQL_CHECK_ERR(e);
				break;
			default:
				if(parameters[i].length == sizeof(Two_Invariable))	
				{
					e = PipeWriteData(&parameters[i].data.s, parameters[i].length);
					OOSQL_CHECK_ERR(e);
				}
				else if(parameters[i].length == sizeof(Four_Invariable))
				{
					e = PipeWriteData(&parameters[i].data.i, parameters[i].length);
					OOSQL_CHECK_ERR(e);
				}
				else
					OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
				break;
			}
		}

		e = PipeWriteData(&returnValue.type, sizeof(Four)); OOSQL_CHECK_ERR(e);
		e = PipeWriteData(&finalCall, sizeof(Four)); OOSQL_CHECK_ERR(e);

		// wait return value
		e = PipeWaitMessage(ret, &returnValue, errorMessage);
		OOSQL_CHECK_ERR(e);

		return ret;
	}
	else
	{
		Four e;

		e = m_externalFunctionDispatcher->ExecuteNotFenced(functionInfo, returnValue, nParams, parameters, errorMessage, finalCall);
		OOSQL_CHECK_ERR(e);
	}

	return eNOERROR;
}

Four OOSQL_ExternalProcessFunctionExecutor::SetPipes(int rPipe, int wPipe)
{
	if(m_runningMode == EXTERNAL_PROCESS)
	{
		m_rPipe = rPipe;
		m_wPipe = wPipe;
	}
	else
	{
		// do nothing
	}

	return eNOERROR;
}

Four OOSQL_ExternalProcessFunctionExecutor::PipeWaitMessage(Four& ret, OOSQL_DB_Value* returnValue, char* errorMessage)
{
	Four							e;
	OOSQL_ExternalProcessMessageID	messageID;
	Four							messageSize;
	Four							flag;
	Four							length;
	Four							finalCall;

	ret  = eNOERROR;
	flag = SM_TRUE;
	while(flag)
	{
		e = PipeReadHeader(messageID, messageSize);
		OOSQL_CHECK_ERR(e);

		switch(messageID)
		{
		case OOSQL_EXTPROCMSG_ID_RETURN:
			// Message
			// | return value (messageSize) |
			e = PipeReadData(&returnValue[0].length, sizeof(Four)); OOSQL_CHECK_ERR(e);
			e = PipeReadData(&returnValue[0].nullFlag, sizeof(Boolean)); OOSQL_CHECK_ERR(e);
			if(returnValue[0].type == OOSQL_TYPE_STRING || returnValue[0].type == OOSQL_TYPE_VARSTRING || returnValue[0].type == OOSQL_TYPE_TEXT)
			{
				returnValue[0].PrepareData(returnValue[0].length);
				e = PipeReadData(returnValue[0].data.ptr, returnValue[0].length);
				OOSQL_CHECK_ERR(e);
			}
			else
			{
				if(returnValue[0].length == sizeof(Two_Invariable))
				{
					e = PipeReadData(&returnValue[0].data.s, returnValue[0].length);
					OOSQL_CHECK_ERR(e);
				}
				else if(returnValue[0].length == sizeof(Four_Invariable))
				{
					e = PipeReadData(&returnValue[0].data.i, returnValue[0].length);
					OOSQL_CHECK_ERR(e);
				}
				else
					OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
			}

			e = PipeReadData(errorMessage, OOSQL_EXTFUNC_ERRORMESSAGE_SIZE);
			OOSQL_CHECK_ERR(e);

			// exit waiting loop
			flag = SM_FALSE;
			break;

		case OOSQL_EXTPROCMSG_ID_CALL_EXIT:
			flag = SM_FALSE;
			e = PipeWriteHeader(OOSQL_EXTPROCMSG_ID_EXIT_DONE, 0); OOSQL_CHECK_ERR(e);
			break;

		case OOSQL_EXTPROCMSG_ID_CALL_FUNCTION:
			{
				OOSQL_ExternalFunctionInfo		functionInfo;
				Four							nParams;
				OOSQL_TCArray<OOSQL_DB_Value>	parameters(pMemoryManager, true);
				Four							i;

				e = PipeReadData(&functionInfo, sizeof(OOSQL_ExternalFunctionInfo)); OOSQL_CHECK_ERR(e);
				e = PipeReadData(&nParams, sizeof(Four)); OOSQL_CHECK_ERR(e);

				parameters.resize(nParams);

				for(i = 0; i < nParams; i++)
				{
					e = PipeReadData(&parameters[i].length, sizeof(Four)); OOSQL_CHECK_ERR(e);
					e = PipeReadData(&parameters[i].type, sizeof(Four)); OOSQL_CHECK_ERR(e);
					e = PipeReadData(&parameters[i].nullFlag, sizeof(Boolean)); OOSQL_CHECK_ERR(e);

					switch(parameters[i].type)
					{
					case TYPEID_STRING:
					case TYPEID_VARSTRING:
					case TYPEID_TEXT:
						e = PipeReadData(parameters[i].data.ptr, parameters[i].length);
						OOSQL_CHECK_ERR(e);
						break;
					default:
						if(parameters[i].length == sizeof(Two_Invariable))
						{
							e = PipeReadData(&parameters[i].data.s, parameters[i].length);
							OOSQL_CHECK_ERR(e);
						}
						else if(parameters[i].length == sizeof(Four_Invariable))
						{
							e = PipeReadData(&parameters[i].data.i, parameters[i].length);
							OOSQL_CHECK_ERR(e);
						}
						else
							OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
						break;
					}
				}
				e = PipeReadData(&returnValue[0].type, sizeof(Four)); OOSQL_CHECK_ERR(e);
				e = PipeReadData(&finalCall, sizeof(Four)); OOSQL_CHECK_ERR(e);

				e = Execute(functionInfo, returnValue[0], nParams, &parameters[0], errorMessage, finalCall); OOSQL_CHECK_ERR(e);

				e = PipeWriteHeader(OOSQL_EXTPROCMSG_ID_RETURN, 0); OOSQL_CHECK_ERR(e);
				e = PipeWriteData(&returnValue[0].length, sizeof(Four)); OOSQL_CHECK_ERR(e);
				e = PipeWriteData(&returnValue[0].nullFlag, sizeof(Boolean)); OOSQL_CHECK_ERR(e);
				if(returnValue[0].type == OOSQL_TYPE_STRING || returnValue[0].type == OOSQL_TYPE_VARSTRING || returnValue[0].type == OOSQL_TYPE_TEXT)
				{
					e = PipeWriteData(returnValue[0].data.ptr, returnValue[0].length); OOSQL_CHECK_ERR(e);
				}
				else
				{
					if(returnValue[0].length == sizeof(Two_Invariable))
					{
						e = PipeWriteData(&returnValue[0].data.s, returnValue[0].length); OOSQL_CHECK_ERR(e);
					}
					else if(returnValue[0].length == sizeof(Four_Invariable))
					{
						e = PipeWriteData(&returnValue[0].data.i, returnValue[0].length); OOSQL_CHECK_ERR(e);
					}
					else
						OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
				}
				e = PipeWriteData(errorMessage, OOSQL_EXTFUNC_ERRORMESSAGE_SIZE); OOSQL_CHECK_ERR(e);
			}
			break;

		case OOSQL_EXTPROCMSG_ID_CALL_OOSQL_CREATESYSTEMHANDLE:
		case OOSQL_EXTPROCMSG_ID_CALL_OOSQL_DESTROYSYSTEMHANDLE:
		case OOSQL_EXTPROCMSG_ID_CALL_OOSQL_CONNECT:
		case OOSQL_EXTPROCMSG_ID_CALL_OOSQL_DISCONNECT:
		case OOSQL_EXTPROCMSG_ID_CALL_OOSQL_SETUSERDEFAULTVOLUMEID:
		case OOSQL_EXTPROCMSG_ID_CALL_OOSQL_GETUSERDEFAULTVOLUMEID:
		case OOSQL_EXTPROCMSG_ID_CALL_OOSQL_GETVOLUMEID:
		case OOSQL_EXTPROCMSG_ID_CALL_OOSQL_MOUNTDB:
		case OOSQL_EXTPROCMSG_ID_CALL_OOSQL_DISMOUNTDB:
		case OOSQL_EXTPROCMSG_ID_CALL_OOSQL_MOUNT:
		case OOSQL_EXTPROCMSG_ID_CALL_OOSQL_DISMOUNT:
		case OOSQL_EXTPROCMSG_ID_CALL_OOSQL_TRANSBEGIN:
		case OOSQL_EXTPROCMSG_ID_CALL_OOSQL_TRANSCOMMIT:
		case OOSQL_EXTPROCMSG_ID_CALL_OOSQL_TRANSABORT:
			OOSQL_ERR(eCANT_EXECUTE_MSG_OOSQL);
			break;

		case OOSQL_EXTPROCMSG_ID_CALL_OOSQL_ALLOCHANDLE:
			{
				OOSQL_SystemHandle	systemHandle;
				Four				volID;
				OOSQL_Handle		handle;

				e = PipeReadData(&systemHandle, sizeof(systemHandle)); OOSQL_CHECK_ERR(e);
				e = PipeReadData(&volID, sizeof(volID)); OOSQL_CHECK_ERR(e);

				ret = OOSQL_AllocHandle(&systemHandle, volID, &handle);

				e = PipeWriteHeader(OOSQL_EXTPROCMSG_ID_CALL_OOSQL_ALLOCHANDLE_RETURN, 0); OOSQL_CHECK_ERR(e);
				e = PipeWriteData(&ret, sizeof(ret)); OOSQL_CHECK_ERR(e);
				e = PipeWriteData(&handle, sizeof(handle)); OOSQL_CHECK_ERR(e);

				OOSQL_CHECK_ERR(e);
			}
			break;
		case OOSQL_EXTPROCMSG_ID_CALL_OOSQL_ALLOCHANDLE_RETURN:
			e = PipeReadData(&ret, sizeof(Four)); OOSQL_CHECK_ERR(e);
			e = PipeReadData(returnValue[0].data.ptr, sizeof(OOSQL_Handle)); OOSQL_CHECK_ERR(e);
			flag = SM_FALSE;
			break;
		case OOSQL_EXTPROCMSG_ID_CALL_OOSQL_FREEHANDLE:
			{
				OOSQL_SystemHandle	systemHandle;
				Four				volID;
				OOSQL_Handle		handle;

				e = PipeReadData(&systemHandle, sizeof(systemHandle)); OOSQL_CHECK_ERR(e);
				e = PipeReadData(&volID, sizeof(volID)); OOSQL_CHECK_ERR(e);
				e = PipeReadData(&handle, sizeof(handle)); OOSQL_CHECK_ERR(e);

				ret = OOSQL_FreeHandle(&systemHandle, handle);

				e = PipeWriteHeader(OOSQL_EXTPROCMSG_ID_CALL_OOSQL_FREEHANDLE_RETURN, 0); OOSQL_CHECK_ERR(e);
				e = PipeWriteData(&ret, sizeof(ret)); OOSQL_CHECK_ERR(e);
			}
			break;
		case OOSQL_EXTPROCMSG_ID_CALL_OOSQL_FREEHANDLE_RETURN:
			e = PipeReadData(&ret, sizeof(Four)); OOSQL_CHECK_ERR(e);
			flag = SM_FALSE;
			break;
		case OOSQL_EXTPROCMSG_ID_CALL_OOSQL_PREPARE:
			{
				OOSQL_SystemHandle		systemHandle;
				OOSQL_Handle			handle;
				Four					stmtTextSize;
				char*					stmtText;
				OOSQL_SortBufferInfo	sortBufferInfo;

				e = PipeReadData(&systemHandle, sizeof(systemHandle)); OOSQL_CHECK_ERR(e);
				e = PipeReadData(&handle, sizeof(handle)); OOSQL_CHECK_ERR(e);
				e = PipeReadData(&stmtTextSize, sizeof(stmtTextSize)); OOSQL_CHECK_ERR(e);
				stmtText = (char*)pMemoryManager->Alloc(stmtTextSize);
				e = PipeReadData(&stmtText, stmtTextSize); OOSQL_CHECK_ERR(e);
				e = PipeReadData(&sortBufferInfo, sizeof(sortBufferInfo)); OOSQL_CHECK_ERR(e);

				ret = OOSQL_Prepare(&systemHandle, handle, stmtText, &sortBufferInfo);

				e = PipeWriteHeader(OOSQL_EXTPROCMSG_ID_CALL_OOSQL_PREPARE_RETURN, 0); OOSQL_CHECK_ERR(e);
				e = PipeWriteData(&ret, sizeof(ret)); OOSQL_CHECK_ERR(e);

				pMemoryManager->Free(stmtText);
			}
			break;
		case OOSQL_EXTPROCMSG_ID_CALL_OOSQL_PREPARE_RETURN:
			e = PipeReadData(&ret, sizeof(Four)); OOSQL_CHECK_ERR(e);
			flag = SM_FALSE;
			break;
		case OOSQL_EXTPROCMSG_ID_CALL_OOSQL_EXECUTE:
			{
				OOSQL_SystemHandle		systemHandle;
				OOSQL_Handle			handle;

				e = PipeReadData(&systemHandle, sizeof(systemHandle)); OOSQL_CHECK_ERR(e);
				e = PipeReadData(&handle, sizeof(handle)); OOSQL_CHECK_ERR(e);

				ret = OOSQL_Execute(&systemHandle, handle);

				e = PipeWriteHeader(OOSQL_EXTPROCMSG_ID_CALL_OOSQL_EXECUTE_RETURN, 0); OOSQL_CHECK_ERR(e);
				e = PipeWriteData(&ret, sizeof(ret)); OOSQL_CHECK_ERR(e);
			}
			break;
		case OOSQL_EXTPROCMSG_ID_CALL_OOSQL_EXECUTE_RETURN:
			e = PipeReadData(&ret, sizeof(Four)); OOSQL_CHECK_ERR(e);
			flag = SM_FALSE;
			break;
		case OOSQL_EXTPROCMSG_ID_CALL_OOSQL_EXECDIRECT:
			{
				OOSQL_SystemHandle		systemHandle;
				OOSQL_Handle			handle;
				Four					stmtTextSize;
				char*					stmtText;
				OOSQL_SortBufferInfo	sortBufferInfo;

				e = PipeReadData(&systemHandle, sizeof(systemHandle)); OOSQL_CHECK_ERR(e);
				e = PipeReadData(&handle, sizeof(handle)); OOSQL_CHECK_ERR(e);
				e = PipeReadData(&stmtTextSize, sizeof(stmtTextSize)); OOSQL_CHECK_ERR(e);
				stmtText = (char*)pMemoryManager->Alloc(stmtTextSize);
				e = PipeReadData(&stmtText, stmtTextSize); OOSQL_CHECK_ERR(e);
				e = PipeReadData(&sortBufferInfo, sizeof(sortBufferInfo)); OOSQL_CHECK_ERR(e);

				ret = OOSQL_ExecDirect(&systemHandle, handle, stmtText, &sortBufferInfo);

				e = PipeWriteHeader(OOSQL_EXTPROCMSG_ID_CALL_OOSQL_EXECDIRECT_RETURN, 0); OOSQL_CHECK_ERR(e);
				e = PipeWriteData(&ret, sizeof(ret)); OOSQL_CHECK_ERR(e);

				pMemoryManager->Free(stmtText);
			}
			break;
		case OOSQL_EXTPROCMSG_ID_CALL_OOSQL_EXECDIRECT_RETURN:
			e = PipeReadData(&ret, sizeof(Four)); OOSQL_CHECK_ERR(e);
			flag = SM_FALSE;
			break;
		case OOSQL_EXTPROCMSG_ID_CALL_OOSQL_NEXT:
			{
				OOSQL_SystemHandle		systemHandle;
				OOSQL_Handle			handle;

				e = PipeReadData(&systemHandle, sizeof(systemHandle)); OOSQL_CHECK_ERR(e);
				e = PipeReadData(&handle, sizeof(handle)); OOSQL_CHECK_ERR(e);

				ret = OOSQL_Next(&systemHandle, handle);

				e = PipeWriteHeader(OOSQL_EXTPROCMSG_ID_CALL_OOSQL_NEXT_RETURN, 0); OOSQL_CHECK_ERR(e);
				e = PipeWriteData(&ret, sizeof(ret)); OOSQL_CHECK_ERR(e);
			}
			break;
		case OOSQL_EXTPROCMSG_ID_CALL_OOSQL_NEXT_RETURN:
			e = PipeReadData(&ret, sizeof(Four)); OOSQL_CHECK_ERR(e);
			flag = SM_FALSE;
			break;
		case OOSQL_EXTPROCMSG_ID_CALL_OOSQL_GETDATA:
			{
				OOSQL_SystemHandle		systemHandle;
				OOSQL_Handle			handle;
				Two						columnNumber; 
				Four					startPos; 
				void*					columnValuePtr; 
				Four					bufferLength; 
				Four					dataLength;

				e = PipeReadData(&systemHandle, sizeof(systemHandle)); OOSQL_CHECK_ERR(e);
				e = PipeReadData(&handle, sizeof(handle)); OOSQL_CHECK_ERR(e);
				e = PipeReadData(&columnNumber, sizeof(columnNumber)); OOSQL_CHECK_ERR(e);
				e = PipeReadData(&startPos, sizeof(startPos)); OOSQL_CHECK_ERR(e);
				e = PipeReadData(&bufferLength, sizeof(bufferLength)); OOSQL_CHECK_ERR(e);
				columnValuePtr = pMemoryManager->Alloc(bufferLength);

				ret = OOSQL_GetData(&systemHandle, handle, columnNumber, startPos, columnValuePtr, bufferLength, &dataLength);

				e = PipeWriteHeader(OOSQL_EXTPROCMSG_ID_CALL_OOSQL_GETDATA_RETURN, 0); OOSQL_CHECK_ERR(e);
				e = PipeWriteData(&ret, sizeof(ret)); OOSQL_CHECK_ERR(e);
				e = PipeWriteData(&dataLength, sizeof(dataLength)); OOSQL_CHECK_ERR(e);
				if(dataLength >= 0)
					e = PipeWriteData(columnValuePtr, dataLength); OOSQL_CHECK_ERR(e);

				pMemoryManager->Free(columnValuePtr);
			}
			break;
		case OOSQL_EXTPROCMSG_ID_CALL_OOSQL_GETDATA_RETURN:
			e = PipeReadData(&ret, sizeof(Four)); OOSQL_CHECK_ERR(e);
#ifndef SUPPORT_LARGE_DATABASE2
			e = PipeReadData(&returnValue[0].data.i, sizeof(Four_Invariable)); OOSQL_CHECK_ERR(e);
			if(returnValue[0].data.i >= 0)
			{
				returnValue[1].PrepareData(returnValue[0].data.i);
				e = PipeReadData(returnValue[1].data.ptr, returnValue[0].data.i); OOSQL_CHECK_ERR(e);
			}
#else
			e = PipeReadData(&returnValue[0].data.ll, sizeof(Eight_Invariable)); OOSQL_CHECK_ERR(e);
			if(returnValue[0].data.ll >= 0)
			{
				returnValue[1].PrepareData(returnValue[0].data.ll);
				e = PipeReadData(returnValue[1].data.ptr, returnValue[0].data.ll); OOSQL_CHECK_ERR(e);
			}
#endif
			flag = SM_FALSE;
			break;

		case OOSQL_EXTPROCMSG_ID_CALL_OOSQL_GETMULTICOLUMNDATA:
			OOSQL_ERR(eCANT_EXECUTE_MSG_OOSQL);
			break;

		case OOSQL_EXTPROCMSG_ID_CALL_OOSQL_PUTDATA:
			{
				OOSQL_SystemHandle		systemHandle;
				OOSQL_Handle			handle;
				Two						columnNumber; 
				Four					startPos; 
				Four					columnValuePtrSize;
				void*					columnValuePtr; 
				Four					bufferLength; 

				e = PipeReadData(&systemHandle, sizeof(systemHandle)); OOSQL_CHECK_ERR(e);
				e = PipeReadData(&handle, sizeof(handle)); OOSQL_CHECK_ERR(e);
				e = PipeReadData(&columnNumber, sizeof(columnNumber)); OOSQL_CHECK_ERR(e);
				e = PipeReadData(&startPos, sizeof(startPos)); OOSQL_CHECK_ERR(e);
				e = PipeReadData(&columnValuePtrSize, sizeof(columnValuePtrSize)); OOSQL_CHECK_ERR(e);
				columnValuePtr = pMemoryManager->Alloc(columnValuePtrSize);
				e = PipeReadData(&columnValuePtr, columnValuePtrSize); OOSQL_CHECK_ERR(e);
				e = PipeReadData(&bufferLength, sizeof(bufferLength)); OOSQL_CHECK_ERR(e);

				ret = OOSQL_PutData(&systemHandle, handle, columnNumber, startPos, columnValuePtr, bufferLength);

				e = PipeWriteHeader(OOSQL_EXTPROCMSG_ID_CALL_OOSQL_PUTDATA_RETURN, 0); OOSQL_CHECK_ERR(e);
				e = PipeWriteData(&ret, sizeof(ret)); OOSQL_CHECK_ERR(e);

				pMemoryManager->Free(columnValuePtr);
			}
			break;
		case OOSQL_EXTPROCMSG_ID_CALL_OOSQL_PUTDATA_RETURN:
			e = PipeReadData(&ret, sizeof(Four)); OOSQL_CHECK_ERR(e);
			flag = SM_FALSE;
			break;

		case OOSQL_EXTPROCMSG_ID_CALL_OOSQL_GETOID:
			{
				OOSQL_SystemHandle			systemHandle;
				OOSQL_Handle				handle;
				Two							targetNumber;
				OOSQL_StorageManager::OID	oid;

				e = PipeReadData(&systemHandle, sizeof(systemHandle)); OOSQL_CHECK_ERR(e);
				e = PipeReadData(&handle, sizeof(handle)); OOSQL_CHECK_ERR(e);
				e = PipeReadData(&targetNumber, sizeof(targetNumber)); OOSQL_CHECK_ERR(e);

				ret = OOSQL_GetOID(&systemHandle, handle, targetNumber, (OID*)&oid);

				e = PipeWriteHeader(OOSQL_EXTPROCMSG_ID_CALL_OOSQL_GETOID_RETURN, 0); OOSQL_CHECK_ERR(e);
				e = PipeWriteData(&ret, sizeof(ret)); OOSQL_CHECK_ERR(e);
				e = PipeWriteData(&oid, sizeof(oid)); OOSQL_CHECK_ERR(e);
			}
			break;
		case OOSQL_EXTPROCMSG_ID_CALL_OOSQL_GETOID_RETURN:
			e = PipeReadData(&ret, sizeof(Four)); OOSQL_CHECK_ERR(e);
			e = PipeReadData(&returnValue[0].data.oid, sizeof(OOSQL_StorageManager::OID)); OOSQL_CHECK_ERR(e);
			flag = SM_FALSE;
			break;

		case OOSQL_EXTPROCMSG_ID_CALL_OOSQL_GETNUMRESULTCOLS:
			{
				OOSQL_SystemHandle		systemHandle;
				OOSQL_Handle			handle;
				Two						nCols;

				e = PipeReadData(&systemHandle, sizeof(systemHandle)); OOSQL_CHECK_ERR(e);
				e = PipeReadData(&handle, sizeof(handle)); OOSQL_CHECK_ERR(e);

				ret = OOSQL_GetNumResultCols(&systemHandle, handle, &nCols);

				e = PipeWriteHeader(OOSQL_EXTPROCMSG_ID_CALL_OOSQL_GETNUMRESULTCOLS_RETURN, 0); OOSQL_CHECK_ERR(e);
				e = PipeWriteData(&ret, sizeof(ret)); OOSQL_CHECK_ERR(e);
				e = PipeWriteData(&nCols, sizeof(nCols)); OOSQL_CHECK_ERR(e);
			}
			break;
		case OOSQL_EXTPROCMSG_ID_CALL_OOSQL_GETNUMRESULTCOLS_RETURN:
			e = PipeReadData(&ret, sizeof(Four)); OOSQL_CHECK_ERR(e);
#ifndef SUPPORT_LARGE_DATABASE2
			e = PipeReadData(&returnValue[0].data.s, sizeof(Two_Invariable)); OOSQL_CHECK_ERR(e);
#else
			e = PipeReadData(&returnValue[0].data.i, sizeof(Four_Invariable)); OOSQL_CHECK_ERR(e);
#endif
			flag = SM_FALSE;
			break;

		case OOSQL_EXTPROCMSG_ID_CALL_OOSQL_GETRESULTCOLNAME:
			{
				OOSQL_SystemHandle		systemHandle;
				OOSQL_Handle			handle;
				Two						columnNumber;
				char*					columnNameBuffer;
				Four					bufferLength;

				e = PipeReadData(&systemHandle, sizeof(systemHandle)); OOSQL_CHECK_ERR(e);
				e = PipeReadData(&handle, sizeof(handle)); OOSQL_CHECK_ERR(e);
				e = PipeReadData(&columnNumber, sizeof(columnNumber)); OOSQL_CHECK_ERR(e);
				e = PipeReadData(&bufferLength, sizeof(bufferLength)); OOSQL_CHECK_ERR(e);
				columnNameBuffer = (char*)pMemoryManager->Alloc(bufferLength);

				ret = OOSQL_GetResultColName(&systemHandle, handle, columnNumber, columnNameBuffer, bufferLength);

				e = PipeWriteHeader(OOSQL_EXTPROCMSG_ID_CALL_OOSQL_GETRESULTCOLNAME_RETURN, 0); OOSQL_CHECK_ERR(e);
				e = PipeWriteData(&ret, sizeof(ret)); OOSQL_CHECK_ERR(e);
				e = PipeWriteData(&bufferLength, sizeof(bufferLength)); OOSQL_CHECK_ERR(e);
				e = PipeWriteData(&columnNameBuffer, bufferLength); OOSQL_CHECK_ERR(e);

				pMemoryManager->Free(columnNameBuffer);
			}
			break;
		case OOSQL_EXTPROCMSG_ID_CALL_OOSQL_GETRESULTCOLNAME_RETURN:
			e = PipeReadData(&ret, sizeof(Four)); OOSQL_CHECK_ERR(e);
			e = PipeReadData(&length, sizeof(Four)); OOSQL_CHECK_ERR(e);
			returnValue[0].PrepareData(length);
			e = PipeReadData(returnValue[0].data.ptr, length); OOSQL_CHECK_ERR(e);
			flag = SM_FALSE;
			break;

		case OOSQL_EXTPROCMSG_ID_CALL_OOSQL_GETRESULTCOLTYPE:
			{
				OOSQL_SystemHandle		systemHandle;
				OOSQL_Handle			handle;
				Two						columnNumber;
				Four					columnType;

				e = PipeReadData(&systemHandle, sizeof(systemHandle)); OOSQL_CHECK_ERR(e);
				e = PipeReadData(&handle, sizeof(handle)); OOSQL_CHECK_ERR(e);
				e = PipeReadData(&columnNumber, sizeof(columnNumber)); OOSQL_CHECK_ERR(e);

				ret = OOSQL_GetResultColType(&systemHandle, handle, columnNumber, &columnType);

				e = PipeWriteHeader(OOSQL_EXTPROCMSG_ID_CALL_OOSQL_GETRESULTCOLTYPE_RETURN, 0); OOSQL_CHECK_ERR(e);
				e = PipeWriteData(&ret, sizeof(ret)); OOSQL_CHECK_ERR(e);
				e = PipeWriteData(&columnType, sizeof(columnType)); OOSQL_CHECK_ERR(e);
			}
			break;
		case OOSQL_EXTPROCMSG_ID_CALL_OOSQL_GETRESULTCOLTYPE_RETURN:
			e = PipeReadData(&ret, sizeof(Four)); OOSQL_CHECK_ERR(e);
#ifndef SUPPORT_LARGE_DATABASE2
			e = PipeReadData(&returnValue[0].data.i, sizeof(Four_Invariable)); OOSQL_CHECK_ERR(e);
#else
			e = PipeReadData(&returnValue[0].data.ll, sizeof(Eight_Invariable)); OOSQL_CHECK_ERR(e);
#endif
			flag = SM_FALSE;
			break;

		case OOSQL_EXTPROCMSG_ID_CALL_OOSQL_GETRESULTCOLLENGTH:
			{
				OOSQL_SystemHandle		systemHandle;
				OOSQL_Handle			handle;
				Two						columnNumber;
				Four					resultColLength;

				e = PipeReadData(&systemHandle, sizeof(systemHandle)); OOSQL_CHECK_ERR(e);
				e = PipeReadData(&handle, sizeof(handle)); OOSQL_CHECK_ERR(e);
				e = PipeReadData(&columnNumber, sizeof(columnNumber)); OOSQL_CHECK_ERR(e);

				ret = OOSQL_GetResultColLength(&systemHandle, handle, columnNumber, &resultColLength);

				e = PipeWriteHeader(OOSQL_EXTPROCMSG_ID_CALL_OOSQL_GETRESULTCOLLENGTH_RETURN, 0); OOSQL_CHECK_ERR(e);
				e = PipeWriteData(&ret, sizeof(ret)); OOSQL_CHECK_ERR(e);
				e = PipeWriteData(&resultColLength, sizeof(resultColLength)); OOSQL_CHECK_ERR(e);
			}
			break;
		case OOSQL_EXTPROCMSG_ID_CALL_OOSQL_GETRESULTCOLLENGTH_RETURN:
			e = PipeReadData(&ret, sizeof(Four)); OOSQL_CHECK_ERR(e);
#ifndef SUPPORT_LARGE_DATABASE2
			e = PipeReadData(&returnValue[0].data.i, sizeof(Four_Invariable)); OOSQL_CHECK_ERR(e);
#else
			e = PipeReadData(&returnValue[0].data.ll, sizeof(Eight_Invariable)); OOSQL_CHECK_ERR(e);
#endif
			flag = SM_FALSE;
			break;

		case OOSQL_EXTPROCMSG_ID_CALL_OOSQL_GETERRORMESSAGE:
			{
				OOSQL_SystemHandle		systemHandle;
				Four					errorCode;
				char*					messageBuffer;
				Four					bufferLength;

				e = PipeReadData(&systemHandle, sizeof(systemHandle)); OOSQL_CHECK_ERR(e);
				e = PipeReadData(&errorCode, sizeof(errorCode)); OOSQL_CHECK_ERR(e);
				e = PipeReadData(&bufferLength, sizeof(bufferLength)); OOSQL_CHECK_ERR(e);
				messageBuffer = (char*)pMemoryManager->Alloc(bufferLength);

				ret = OOSQL_GetErrorMessage(&systemHandle, errorCode, messageBuffer, bufferLength);

				e = PipeWriteHeader(OOSQL_EXTPROCMSG_ID_CALL_OOSQL_GETERRORMESSAGE_RETURN, 0); OOSQL_CHECK_ERR(e);
				e = PipeWriteData(&ret, sizeof(ret)); OOSQL_CHECK_ERR(e);
				e = PipeWriteData(&bufferLength, sizeof(bufferLength)); OOSQL_CHECK_ERR(e);
				e = PipeWriteData(messageBuffer, bufferLength); OOSQL_CHECK_ERR(e);

				pMemoryManager->Free(messageBuffer);
			}
			break;
		case OOSQL_EXTPROCMSG_ID_CALL_OOSQL_GETERRORMESSAGE_RETURN:
			e = PipeReadData(&ret, sizeof(Four)); OOSQL_CHECK_ERR(e);
			e = PipeReadData(&length, sizeof(Four)); OOSQL_CHECK_ERR(e);
			returnValue[0].PrepareData(length);
			e = PipeReadData(&returnValue[0].data.ptr, length); OOSQL_CHECK_ERR(e);
			flag = SM_FALSE;
			break;

		case OOSQL_EXTPROCMSG_ID_CALL_OOSQL_GETERRORNAME:
			{
				OOSQL_SystemHandle		systemHandle;
				Four					errorCode;
				char*					messageBuffer;
				Four					bufferLength;

				e = PipeReadData(&systemHandle, sizeof(systemHandle)); OOSQL_CHECK_ERR(e);
				e = PipeReadData(&errorCode, sizeof(errorCode)); OOSQL_CHECK_ERR(e);
				e = PipeReadData(&bufferLength, sizeof(bufferLength)); OOSQL_CHECK_ERR(e);
				messageBuffer = (char*)pMemoryManager->Alloc(bufferLength);

				ret = OOSQL_GetErrorName(&systemHandle, errorCode, messageBuffer, bufferLength);

				e = PipeWriteHeader(OOSQL_EXTPROCMSG_ID_CALL_OOSQL_GETERRORNAME_RETURN, 0); OOSQL_CHECK_ERR(e);
				e = PipeWriteData(&ret, sizeof(ret)); OOSQL_CHECK_ERR(e);
				e = PipeWriteData(&bufferLength, sizeof(bufferLength)); OOSQL_CHECK_ERR(e);
				e = PipeWriteData(messageBuffer, bufferLength); OOSQL_CHECK_ERR(e);

				pMemoryManager->Free(messageBuffer);
			}
			break;
		case OOSQL_EXTPROCMSG_ID_CALL_OOSQL_GETERRORNAME_RETURN:
			e = PipeReadData(&ret, sizeof(Four)); OOSQL_CHECK_ERR(e);
			e = PipeReadData(&length, sizeof(Four)); OOSQL_CHECK_ERR(e);
			returnValue[0].PrepareData(length);
			e = PipeReadData(&returnValue[0].data.ptr, length); OOSQL_CHECK_ERR(e);
			flag = SM_FALSE;
			break;

		case OOSQL_EXTPROCMSG_ID_CALL_OOSQL_GETQUERYERRORMESSAGE:
			{
				OOSQL_SystemHandle		systemHandle;
				OOSQL_Handle			handle;
				Four					errorCode;
				char*					messageBuffer;
				Four					bufferLength;

				e = PipeReadData(&systemHandle, sizeof(systemHandle)); OOSQL_CHECK_ERR(e);
				e = PipeReadData(&handle, sizeof(handle)); OOSQL_CHECK_ERR(e);
				e = PipeReadData(&errorCode, sizeof(errorCode)); OOSQL_CHECK_ERR(e);
				e = PipeReadData(&bufferLength, sizeof(bufferLength)); OOSQL_CHECK_ERR(e);
				messageBuffer = (char*)pMemoryManager->Alloc(bufferLength);

				ret = OOSQL_GetQueryErrorMessage(&systemHandle, handle, messageBuffer, bufferLength);

				e = PipeWriteHeader(OOSQL_EXTPROCMSG_ID_CALL_OOSQL_GETQUERYERRORMESSAGE_RETURN, 0); OOSQL_CHECK_ERR(e);
				e = PipeWriteData(&ret, sizeof(ret)); OOSQL_CHECK_ERR(e);
				e = PipeWriteData(&bufferLength, sizeof(bufferLength)); OOSQL_CHECK_ERR(e);
				e = PipeWriteData(messageBuffer, bufferLength); OOSQL_CHECK_ERR(e);

				pMemoryManager->Free(messageBuffer);
			}
			break;
		case OOSQL_EXTPROCMSG_ID_CALL_OOSQL_GETQUERYERRORMESSAGE_RETURN:
			e = PipeReadData(&ret, sizeof(Four)); OOSQL_CHECK_ERR(e);
			e = PipeReadData(&length, sizeof(Four)); OOSQL_CHECK_ERR(e);
			returnValue[0].PrepareData(length);
			e = PipeReadData(&returnValue[0].data.ptr, length); OOSQL_CHECK_ERR(e);
			flag = SM_FALSE;
			break;

		case OOSQL_EXTPROCMSG_ID_CALL_OOSQL_OIDTOOIDSTRING:
			{
				OOSQL_SystemHandle			systemHandle;
				OOSQL_StorageManager::OID	oid;
				char						oidString[sizeof(oid) * 2 + 1];

				e = PipeReadData(&systemHandle, sizeof(systemHandle)); OOSQL_CHECK_ERR(e);
				e = PipeReadData(&oid, sizeof(oid)); OOSQL_CHECK_ERR(e);

				ret = OOSQL_OIDToOIDString(&systemHandle, (OID*)&oid, oidString);

				e = PipeWriteHeader(OOSQL_EXTPROCMSG_ID_CALL_OOSQL_OIDTOOIDSTRING_RETURN, 0); OOSQL_CHECK_ERR(e);
				e = PipeWriteData(&ret, sizeof(ret)); OOSQL_CHECK_ERR(e);
				e = PipeWriteData(oidString, sizeof(oidString)); OOSQL_CHECK_ERR(e);
			}
			break;
		case OOSQL_EXTPROCMSG_ID_CALL_OOSQL_OIDTOOIDSTRING_RETURN:
			e = PipeReadData(&ret, sizeof(Four)); OOSQL_CHECK_ERR(e);
			returnValue[0].PrepareData(sizeof(OOSQL_StorageManager::OID) * 2 + 1);
			e = PipeReadData(&returnValue[0].data.ptr, sizeof(OOSQL_StorageManager::OID) * 2 + 1); 
			OOSQL_CHECK_ERR(e);
			flag = SM_FALSE;
			break;

		case OOSQL_EXTPROCMSG_ID_CALL_OOSQL_TEXT_ADDKEYWORDEXTRACTOR:
		case OOSQL_EXTPROCMSG_ID_CALL_OOSQL_TEXT_ADDDEFAULTKEYWORDEXTRACTOR:
		case OOSQL_EXTPROCMSG_ID_CALL_OOSQL_TEXT_DROPKEYWORDEXTRACTOR:
		case OOSQL_EXTPROCMSG_ID_CALL_OOSQL_TEXT_SETKEYWORDEXTRACTOR:
		case OOSQL_EXTPROCMSG_ID_CALL_OOSQL_TEXT_ADDFILTER:
		case OOSQL_EXTPROCMSG_ID_CALL_OOSQL_TEXT_DROPFILTER:
		case OOSQL_EXTPROCMSG_ID_CALL_OOSQL_TEXT_SETFILTER:
		case OOSQL_EXTPROCMSG_ID_CALL_OOSQL_TEXT_MAKEINDEX:
		case OOSQL_EXTPROCMSG_ID_CALL_OOSQL_TEXT_KEYWORDINFOSCAN_OPEN:
		case OOSQL_EXTPROCMSG_ID_CALL_OOSQL_TEXT_KEYWORDINFOSCAN_CLOSE:
		case OOSQL_EXTPROCMSG_ID_CALL_OOSQL_TEXT_KEYWORDINFOSCAN_NEXT:
		case OOSQL_EXTPROCMSG_ID_CALL_OOSQL_TEXT_KEYWORDINFOSCANFORDOCUMENT_OPEN:
		case OOSQL_EXTPROCMSG_ID_CALL_OOSQL_TEXT_KEYWORDINFOSCANFORDOCUMENT_CLOSE:
		case OOSQL_EXTPROCMSG_ID_CALL_OOSQL_TEXT_KEYWORDINFOSCANFORDOCUMENT_NEXT:
		case OOSQL_EXTPROCMSG_ID_CALL_OOSQL_TEXT_DEFINEPOSTINGSTRUCTURE:
		case OOSQL_EXTPROCMSG_ID_CALL_OOSQL_GETLOCALTIMEZONE:
		case OOSQL_EXTPROCMSG_ID_CALL_OOSQL_SETCURTIME:
		case OOSQL_EXTPROCMSG_ID_CALL_OOSQL_GETHOUR:
		case OOSQL_EXTPROCMSG_ID_CALL_OOSQL_GETMINUTE:
		case OOSQL_EXTPROCMSG_ID_CALL_OOSQL_GETSECOND:
		case OOSQL_EXTPROCMSG_ID_CALL_OOSQL_GETYEAR:
		case OOSQL_EXTPROCMSG_ID_CALL_OOSQL_GETMONTH:
		case OOSQL_EXTPROCMSG_ID_CALL_OOSQL_GETDAY:
		case OOSQL_EXTPROCMSG_ID_CALL_OOSQL_GETVERSIONSTRING:
		case OOSQL_EXTPROCMSG_ID_CALL_OOSQL_GETTIMEELAPSED:
		case OOSQL_EXTPROCMSG_ID_CALL_OOSQL_RESETTIMEELAPSED:
			OOSQL_ERR(eCANT_EXECUTE_MSG_OOSQL);
			break;
		default:
			OOSQL_ERR(eUNHANDLED_CASE_OOSQL);
			break;
		}
	}

	return eNOERROR;
}

Four OOSQL_ExternalProcessFunctionExecutor::PipeOpen()
{
	if(m_runningMode == OOSQL_SERVER)
	{
		Four e;

		if(!(m_pipe1[0] == -1 && m_pipe1[1] == -1 && m_pipe2[0] == -1 && m_pipe2[1] == -1 && m_rPipe == -1 && m_wPipe == -1))
			OOSQL_ERR(ePIPE_ALREADY_OPENED_OOSQL);

		// open pipes
#ifdef WIN32
		if(_pipe(m_pipe1, 102400, O_BINARY) == -1) OOSQL_ERR(ePIPE_OPEN_ERROR_OOSQL);
		if(_pipe(m_pipe2, 102400, O_BINARY) == -1) OOSQL_ERR(ePIPE_OPEN_ERROR_OOSQL);
#else
		if(pipe(m_pipe1) == -1) OOSQL_ERR(ePIPE_OPEN_ERROR_OOSQL);
		if(pipe(m_pipe2) == -1) OOSQL_ERR(ePIPE_OPEN_ERROR_OOSQL);
#endif

		// create process and execute it
		char rPipeClient[16];
		char wPipeClient[16];
		
		// determine read pipe, write pipe for client to use
		sprintf(rPipeClient, "%ld", m_pipe1[0]);
		sprintf(wPipeClient, "%ld", m_pipe2[1]);
#ifdef TEST_EXTERNAL_FUNCTION_EXCUTOR_WITH_THREAD
		threadArgument[0] = m_pipe1[0];
		threadArgument[1] = m_pipe2[1];

		m_executorPid = _beginthread(ExecutorThread, 0, (void*)threadArgument);
		if(m_executorPid == -1)
			OOSQL_ERR(eCREATE_PROCESS_ERROR_OOSQL);
#else
	#ifdef WIN32
		m_executorPid = spawnl(P_NOWAIT, "OOSQL_ExternalFunctionExecutor.exe", "OOSQL_ExternalFunctionExecutor.exe",
							   rPipeClient, wPipeClient, NULL);
		if(m_executorPid == -1)
			OOSQL_ERR(eCREATE_PROCESS_ERROR_OOSQL);
	#else
		m_executorPid = fork();
		if(m_executorPid < 0)
		{
			OOSQL_ERR(eCREATE_PROCESS_ERROR_OOSQL);
		}
		else if(m_executorPid == 0)
		{
			// child process
			e = execl("OOSQL_ExternalFunctionExecutor.exe", "OOSQL_ExternalFunctionExecutor.exe",
					  rPipeClient, wPipeClient, NULL);
			if(e == -1)
				OOSQL_ERR(eCREATE_PROCESS_ERROR_OOSQL);
			exit(1);
		}

	#endif
#endif
		// determine read pipe, write pipe for server to use
		m_rPipe = m_pipe2[0];
		m_wPipe = m_pipe1[1];

		e = PipeWriteHeader(OOSQL_EXTPROCMSG_ID_SYSTEM_HANDLE, sizeof(OOSQL_SystemHandle)); OOSQL_CHECK_ERR(e);
		e = PipeWriteData(m_oosqlSystemHandle, sizeof(OOSQL_SystemHandle)); OOSQL_CHECK_ERR(e);
	}
	else
	{
		// do nothing
	}

	return eNOERROR;
}

Four OOSQL_ExternalProcessFunctionExecutor::PipeClose()
{
	if(m_runningMode == OOSQL_SERVER)
	{
		Four							e;
		OOSQL_ExternalProcessMessageID	messageID;
		Four							messageSize;

		if(m_pipe1[0] == -1 && m_pipe1[1] == -1 && m_pipe2[0] == -1 && m_pipe2[1] == -1 && m_rPipe == -1 && m_wPipe == -1)
			OOSQL_ERR(ePIPE_ALREADY_CLOSED_OOSQL);

		// send close and exit message to OOSQL_ExternalFunctionExecutor
		e = PipeWriteHeader(OOSQL_EXTPROCMSG_ID_CALL_EXIT, 0);
		OOSQL_CHECK_ERR(e);

		e = PipeReadHeader(messageID, messageSize);
		OOSQL_CHECK_ERR(e);

		if(messageID != OOSQL_EXTPROCMSG_ID_EXIT_DONE)
			OOSQL_ERR(ePIPE_BAD_MESSAGE_OOSQL);

		if(close(m_pipe1[0]) != 0) OOSQL_ERR(ePIPE_CLOSE_ERROR_OOSQL);
		if(close(m_pipe1[1]) != 0) OOSQL_ERR(ePIPE_CLOSE_ERROR_OOSQL);
		if(close(m_pipe2[0]) != 0) OOSQL_ERR(ePIPE_CLOSE_ERROR_OOSQL);
		if(close(m_pipe2[1]) != 0) OOSQL_ERR(ePIPE_CLOSE_ERROR_OOSQL);

		m_rPipe			= -1;
		m_wPipe			= -1;
		m_pipe1[0]		= -1;
		m_pipe1[1]		= -1;
		m_pipe2[0]		= -1;
		m_pipe2[1]		= -1;
		m_executorPid	= -1;
	}
	else
	{
		// do nothing
	}

	return eNOERROR;
}

Four OOSQL_ExternalProcessFunctionExecutor::PipeIsOpen()
{
	if(m_runningMode == OOSQL_SERVER)
	{
		if(m_pipe1[0] == -1 && m_pipe1[1] == -1 && m_pipe2[0] == -1 && m_pipe2[1] == -1 && m_rPipe == -1 && m_wPipe == -1)
			return SM_FALSE;
		else
			return SM_TRUE;
	}
	else
	{
		if(m_rPipe == -1 && m_wPipe == -1)
			return SM_FALSE;
		else
			return SM_TRUE;
	}
}

Four OOSQL_ExternalProcessFunctionExecutor::PipeReadHeader(OOSQL_ExternalProcessMessageID& messageID, Four& messageSize)
{
	if(read(m_rPipe, &messageID, sizeof(messageID)) == -1)
		OOSQL_ERR(ePIPE_READ_ERROR_OOSQL);

	if(read(m_rPipe, &messageSize, sizeof(messageSize)) == -1)
		OOSQL_ERR(ePIPE_READ_ERROR_OOSQL);
	return eNOERROR;
}

Four OOSQL_ExternalProcessFunctionExecutor::PipeWriteHeader(OOSQL_ExternalProcessMessageID messageID, Four messageSize)
{
	if(write(m_wPipe, &messageID, sizeof(messageID)) == -1)
		OOSQL_ERR(ePIPE_WRITE_ERROR_OOSQL);

	if(write(m_wPipe, &messageSize, sizeof(messageSize)) == -1)
		OOSQL_ERR(ePIPE_WRITE_ERROR_OOSQL);
	return eNOERROR;
}

Four OOSQL_ExternalProcessFunctionExecutor::PipeReadData(void* data, Four size)
{
	if(read(m_rPipe, data, size) == -1)
		OOSQL_ERR(ePIPE_READ_ERROR_OOSQL);
#ifdef EXTFUNC_DEBUG
	if(m_runningMode == OOSQL_SERVER)
		fprintf(stderr, "server reading %ld bytes\n", size);
	else
		fprintf(stderr, "client reading %ld bytes\n", size);
#endif
	return eNOERROR;
}

Four OOSQL_ExternalProcessFunctionExecutor::PipeWriteData(const void* data, Four size)
{
#ifdef EXTFUNC_DEBUG
	if(m_runningMode == OOSQL_SERVER)
		fprintf(stderr, "server writing %ld bytes\n", size);
	else
		fprintf(stderr, "client writing %ld bytes\n", size);
#endif
	if(write(m_wPipe, data, size) == -1)
	{
#ifdef EXTFUNC_DEBUG
		fprintf(stderr, "errno = %ld", errno);
#endif
		OOSQL_ERR(ePIPE_WRITE_ERROR_OOSQL);
	}
	return eNOERROR;
}

Four OOSQL_ExternalProcessFunctionExecutor::OOSQL_CreateSystemHandle(OOSQL_SystemHandle* systemHandle, Four *procIndex)
{
	if(m_runningMode == OOSQL_SERVER)
	{
		return ::OOSQL_CreateSystemHandle(systemHandle, procIndex);
	}
	else
	{
		Four			e, ret;
		OOSQL_DB_Value  returnValues[1];

		e = PipeWriteHeader(OOSQL_EXTPROCMSG_ID_CALL_OOSQL_CREATESYSTEMHANDLE, 0); OOSQL_CHECK_ERR(e);

		e = PipeWaitMessage(ret, returnValues, NULL); OOSQL_CHECK_ERR(e);
	}
	return eNOERROR;
}
Four OOSQL_ExternalProcessFunctionExecutor::OOSQL_DestroySystemHandle(OOSQL_SystemHandle* systemHandle, Four procIndex)
{
	if(m_runningMode == OOSQL_SERVER)
	{
		return ::OOSQL_DestroySystemHandle(systemHandle, procIndex);
	}
	else
	{
		Four			e, ret;
		OOSQL_DB_Value  returnValues[1];

		e = PipeWriteHeader(OOSQL_EXTPROCMSG_ID_CALL_OOSQL_DESTROYSYSTEMHANDLE, 0); OOSQL_CHECK_ERR(e);

		e = PipeWaitMessage(ret, returnValues, NULL); OOSQL_CHECK_ERR(e);
	}
	return eNOERROR;
}
Four OOSQL_ExternalProcessFunctionExecutor::OOSQL_Connect(char* hostAddress, char* protocolString, OOSQL_SystemHandle* systemHandle)
{
	if(m_runningMode == OOSQL_SERVER)
	{
		return ::OOSQL_Connect(hostAddress, protocolString, systemHandle);
	}
	else
	{
		Four			e, ret;
		OOSQL_DB_Value  returnValues[1];

		e = PipeWriteHeader(OOSQL_EXTPROCMSG_ID_CALL_OOSQL_CONNECT, 0); OOSQL_CHECK_ERR(e);

		e = PipeWaitMessage(ret, returnValues, NULL); OOSQL_CHECK_ERR(e);
	}
	return eNOERROR;
}
Four OOSQL_ExternalProcessFunctionExecutor::OOSQL_Disconnect(OOSQL_SystemHandle* systemHandle)
{
	if(m_runningMode == OOSQL_SERVER)
	{
		return ::OOSQL_Disconnect(systemHandle);
	}
	else
	{
		Four			e, ret;
		OOSQL_DB_Value  returnValues[1];

		e = PipeWriteHeader(OOSQL_EXTPROCMSG_ID_CALL_OOSQL_DISCONNECT, 0); OOSQL_CHECK_ERR(e);

		e = PipeWaitMessage(ret, returnValues, NULL); OOSQL_CHECK_ERR(e);
	}
	return eNOERROR;
}
Four OOSQL_ExternalProcessFunctionExecutor::OOSQL_SetUserDefaultVolumeID(OOSQL_SystemHandle* systemHandle, Four databaseID, Four volumeID)
{
	if(m_runningMode == OOSQL_SERVER)
	{
		return ::OOSQL_SetUserDefaultVolumeID(systemHandle, databaseID, volumeID);
	}
	else
	{
		Four			e, ret;
		OOSQL_DB_Value  returnValues[1];

		e = PipeWriteHeader(OOSQL_EXTPROCMSG_ID_CALL_OOSQL_SETUSERDEFAULTVOLUMEID, 0); OOSQL_CHECK_ERR(e);
		e = PipeWriteData(systemHandle, sizeof(OOSQL_SystemHandle)); OOSQL_CHECK_ERR(e);
		e = PipeWriteData(&databaseID, sizeof(databaseID)); OOSQL_CHECK_ERR(e);
		e = PipeWriteData(&volumeID, sizeof(volumeID)); OOSQL_CHECK_ERR(e);

		e = PipeWaitMessage(ret, returnValues, NULL); OOSQL_CHECK_ERR(e);
	}
	return eNOERROR;
}
Four OOSQL_ExternalProcessFunctionExecutor::OOSQL_GetUserDefaultVolumeID(OOSQL_SystemHandle* systemHandle, Four databaseID, Four* volumeID)
{
	if(m_runningMode == OOSQL_SERVER)
	{
		return ::OOSQL_GetUserDefaultVolumeID(systemHandle, databaseID, volumeID);
	}
	else
	{
		Four			e, ret;
		OOSQL_DB_Value  returnValues[1];

		e = PipeWriteHeader(OOSQL_EXTPROCMSG_ID_CALL_OOSQL_GETUSERDEFAULTVOLUMEID, 0); OOSQL_CHECK_ERR(e);
		e = PipeWriteData(systemHandle, sizeof(OOSQL_SystemHandle)); OOSQL_CHECK_ERR(e);
		e = PipeWriteData(&databaseID, sizeof(databaseID)); OOSQL_CHECK_ERR(e);

		e = PipeWaitMessage(ret, returnValues, NULL); OOSQL_CHECK_ERR(e);

#ifndef SUPPORT_LARGE_DATABASE2
		memcpy(&volumeID, &returnValues[0].data.i, sizeof(Four_Invariable));
#else
		memcpy(&volumeID, &returnValues[0].data.ll, sizeof(Eight_Invariable));
#endif

		return ret;
	}
	return eNOERROR;
}
Four OOSQL_ExternalProcessFunctionExecutor::OOSQL_GetVolumeID(OOSQL_SystemHandle* systemHandle, Four databaseID, char* volumeName, Four* volumeID)
{
	if(m_runningMode == OOSQL_SERVER)
	{
		return ::OOSQL_GetVolumeID(systemHandle, databaseID, volumeName, volumeID);
	}
	else
	{
		Four			e, ret;
		OOSQL_DB_Value  returnValues[1];

		e = PipeWriteHeader(OOSQL_EXTPROCMSG_ID_CALL_OOSQL_GETVOLUMEID, 0); OOSQL_CHECK_ERR(e);
		e = PipeWriteData(systemHandle, sizeof(OOSQL_SystemHandle)); OOSQL_CHECK_ERR(e);

		e = PipeWaitMessage(ret, returnValues, NULL); OOSQL_CHECK_ERR(e);

		return ret;
	}
	return eNOERROR;
}
Four OOSQL_ExternalProcessFunctionExecutor::OOSQL_MountDB(OOSQL_SystemHandle* systemHandle, char* databaseName, Four* databaseID)
{
	if(m_runningMode == OOSQL_SERVER)
	{
		return ::OOSQL_MountDB(systemHandle, databaseName, databaseID);
	}
	else
	{
		Four			e, ret;
		OOSQL_DB_Value  returnValues[1];

		e = PipeWriteHeader(OOSQL_EXTPROCMSG_ID_CALL_OOSQL_MOUNTDB, 0); OOSQL_CHECK_ERR(e);
		e = PipeWriteData(systemHandle, sizeof(OOSQL_SystemHandle)); OOSQL_CHECK_ERR(e);

		e = PipeWaitMessage(ret, returnValues, NULL); OOSQL_CHECK_ERR(e);

		return ret;
	}
	return eNOERROR;
}
Four OOSQL_ExternalProcessFunctionExecutor::OOSQL_DismountDB(OOSQL_SystemHandle* systemHandle, Four databaseID)
{
	if(m_runningMode == OOSQL_SERVER)
	{
		return ::OOSQL_DismountDB(systemHandle, databaseID);
	}
	else
	{
		Four			e, ret;
		OOSQL_DB_Value  returnValues[1];


		e = PipeWriteHeader(OOSQL_EXTPROCMSG_ID_CALL_OOSQL_DISMOUNTDB, 0); OOSQL_CHECK_ERR(e);
		e = PipeWriteData(systemHandle, sizeof(OOSQL_SystemHandle)); OOSQL_CHECK_ERR(e);

		e = PipeWaitMessage(ret, returnValues, NULL); OOSQL_CHECK_ERR(e);

		return ret;
	}
	return eNOERROR;
}
Four OOSQL_ExternalProcessFunctionExecutor::OOSQL_AllocHandle(OOSQL_SystemHandle* systemHandle, Four volID, OOSQL_Handle* handle)
{
	if(m_runningMode == OOSQL_SERVER)
	{
		return ::OOSQL_AllocHandle(systemHandle, volID, handle);
	}
	else
	{
		Four			e, ret;
		OOSQL_DB_Value  returnValues[1];

		e = PipeWriteHeader(OOSQL_EXTPROCMSG_ID_CALL_OOSQL_ALLOCHANDLE, 0); OOSQL_CHECK_ERR(e);
		e = PipeWriteData(systemHandle, sizeof(OOSQL_SystemHandle)); OOSQL_CHECK_ERR(e);
		e = PipeWriteData(&volID, sizeof(volID)); OOSQL_CHECK_ERR(e);

		e = PipeWaitMessage(ret, returnValues, NULL); OOSQL_CHECK_ERR(e);

		memcpy(handle, returnValues[0].data.ptr, sizeof(OOSQL_Handle));

		return ret;
	}
	return eNOERROR;
}
Four OOSQL_ExternalProcessFunctionExecutor::OOSQL_FreeHandle(OOSQL_SystemHandle* systemHandle, OOSQL_Handle handle)
{
	if(m_runningMode == OOSQL_SERVER)
	{
		return ::OOSQL_FreeHandle(systemHandle, handle);
	}
	else
	{
		Four			e, ret;
		OOSQL_DB_Value  returnValues[1];

		e = PipeWriteHeader(OOSQL_EXTPROCMSG_ID_CALL_OOSQL_FREEHANDLE, 0); OOSQL_CHECK_ERR(e);
		e = PipeWriteData(systemHandle, sizeof(OOSQL_SystemHandle)); OOSQL_CHECK_ERR(e);
		e = PipeWriteData(&handle, sizeof(handle)); OOSQL_CHECK_ERR(e);

		e = PipeWaitMessage(ret, returnValues, NULL); OOSQL_CHECK_ERR(e);

		return ret;
	}
	return eNOERROR;
}
Four OOSQL_ExternalProcessFunctionExecutor::OOSQL_Mount(OOSQL_SystemHandle* systemHandle, Four numDevices, char** devNames, Four* volID)
{
	if(m_runningMode == OOSQL_SERVER)
	{
		return ::OOSQL_Mount(systemHandle, numDevices, devNames, volID);
	}
	else
	{
		Four			e, ret;
		OOSQL_DB_Value  returnValues[1];

		e = PipeWriteHeader(OOSQL_EXTPROCMSG_ID_CALL_OOSQL_MOUNT, 0); OOSQL_CHECK_ERR(e);
		e = PipeWriteData(systemHandle, sizeof(OOSQL_SystemHandle)); OOSQL_CHECK_ERR(e);

		e = PipeWaitMessage(ret, returnValues, NULL); OOSQL_CHECK_ERR(e);

		return ret;
	}
	return eNOERROR;
}
Four OOSQL_ExternalProcessFunctionExecutor::OOSQL_Dismount(OOSQL_SystemHandle* systemHandle, Four volID)
{
	if(m_runningMode == OOSQL_SERVER)
	{
		return ::OOSQL_Dismount(systemHandle, volID);
	}
	else
	{
		Four			e, ret;
		OOSQL_DB_Value  returnValues[1];

		e = PipeWriteHeader(OOSQL_EXTPROCMSG_ID_CALL_OOSQL_DISMOUNT, 0); OOSQL_CHECK_ERR(e);
		e = PipeWriteData(systemHandle, sizeof(OOSQL_SystemHandle)); OOSQL_CHECK_ERR(e);

		e = PipeWaitMessage(ret, returnValues, NULL); OOSQL_CHECK_ERR(e);

		return ret;
	}
	return eNOERROR;
}
Four OOSQL_ExternalProcessFunctionExecutor::OOSQL_TransBegin(OOSQL_SystemHandle* systemHandle, XactID *xactId, ConcurrencyLevel ccLevel)
{
	if(m_runningMode == OOSQL_SERVER)
	{
		return ::OOSQL_TransBegin(systemHandle, xactId, ccLevel);
	}
	else
	{
		Four			e, ret;
		OOSQL_DB_Value  returnValues[1];

		e = PipeWriteHeader(OOSQL_EXTPROCMSG_ID_CALL_OOSQL_TRANSBEGIN, 0); OOSQL_CHECK_ERR(e);
		e = PipeWriteData(systemHandle, sizeof(OOSQL_SystemHandle)); OOSQL_CHECK_ERR(e);
		e = PipeWriteData(&ccLevel, sizeof(ccLevel)); OOSQL_CHECK_ERR(e);

		e = PipeWaitMessage(ret, returnValues, NULL); OOSQL_CHECK_ERR(e);

		return ret;
	}
	return eNOERROR;
}
Four OOSQL_ExternalProcessFunctionExecutor::OOSQL_TransCommit(OOSQL_SystemHandle* systemHandle, XactID *xactId)
{
	if(m_runningMode == OOSQL_SERVER)
	{
		return ::OOSQL_TransCommit(systemHandle, xactId);
	}
	else
	{
		Four			e, ret;
		OOSQL_DB_Value  returnValues[1];

		e = PipeWriteHeader(OOSQL_EXTPROCMSG_ID_CALL_OOSQL_TRANSCOMMIT, 0); OOSQL_CHECK_ERR(e);
		e = PipeWriteData(systemHandle, sizeof(OOSQL_SystemHandle)); OOSQL_CHECK_ERR(e);

		e = PipeWaitMessage(ret, returnValues, NULL); OOSQL_CHECK_ERR(e);

		return ret;
	}
	return eNOERROR;
}
Four OOSQL_ExternalProcessFunctionExecutor::OOSQL_TransAbort(OOSQL_SystemHandle* systemHandle, XactID *xactId)
{
	if(m_runningMode == OOSQL_SERVER)
	{
		return ::OOSQL_TransAbort(systemHandle, xactId);
	}
	else
	{
		Four			e, ret;
		OOSQL_DB_Value  returnValues[1];

		e = PipeWriteHeader(OOSQL_EXTPROCMSG_ID_CALL_OOSQL_TRANSABORT, 0); OOSQL_CHECK_ERR(e);
		e = PipeWriteData(systemHandle, sizeof(OOSQL_SystemHandle)); OOSQL_CHECK_ERR(e);

		e = PipeWaitMessage(ret, returnValues, NULL); OOSQL_CHECK_ERR(e);

		return ret;
	}
	return eNOERROR;
}
Four OOSQL_ExternalProcessFunctionExecutor::OOSQL_Prepare(OOSQL_SystemHandle* systemHandle, OOSQL_Handle handle, char* stmtText, OOSQL_SortBufferInfo* sortBufferInfo)
{
	if(m_runningMode == OOSQL_SERVER)
	{
		return ::OOSQL_Prepare(systemHandle, handle, stmtText, sortBufferInfo);
	}
	else
	{
		Four			e, ret;
		OOSQL_DB_Value  returnValues[1];
		Four			length;

		e = PipeWriteHeader(OOSQL_EXTPROCMSG_ID_CALL_OOSQL_PREPARE, 0); OOSQL_CHECK_ERR(e);
		e = PipeWriteData(systemHandle, sizeof(OOSQL_SystemHandle)); OOSQL_CHECK_ERR(e);
		e = PipeWriteData(&handle, sizeof(handle)); OOSQL_CHECK_ERR(e);
		length = strlen(stmtText);
		e = PipeWriteData(&length, sizeof(length)); OOSQL_CHECK_ERR(e);
		e = PipeWriteData(sortBufferInfo, sizeof(OOSQL_SortBufferInfo));

		e = PipeWaitMessage(ret, returnValues, NULL); OOSQL_CHECK_ERR(e);

		return ret;
	}
	return eNOERROR;
}
Four OOSQL_ExternalProcessFunctionExecutor::OOSQL_Execute(OOSQL_SystemHandle* systemHandle, OOSQL_Handle handle)
{
	if(m_runningMode == OOSQL_SERVER)
	{
		return ::OOSQL_Execute(systemHandle, handle);
	}
	else
	{
		Four			e, ret;
		OOSQL_DB_Value  returnValues[1];

		e = PipeWriteHeader(OOSQL_EXTPROCMSG_ID_CALL_OOSQL_EXECUTE, 0); OOSQL_CHECK_ERR(e);
		e = PipeWriteData(systemHandle, sizeof(OOSQL_SystemHandle)); OOSQL_CHECK_ERR(e);
		e = PipeWriteData(&handle, sizeof(handle)); OOSQL_CHECK_ERR(e);

		e = PipeWaitMessage(ret, returnValues, NULL); OOSQL_CHECK_ERR(e);

		return ret;
	}
	return eNOERROR;
}
Four OOSQL_ExternalProcessFunctionExecutor::OOSQL_ExecDirect(OOSQL_SystemHandle* systemHandle, OOSQL_Handle handle, char* stmtText, OOSQL_SortBufferInfo* sortBufferInfo)
{
	if(m_runningMode == OOSQL_SERVER)
	{
		return ::OOSQL_ExecDirect(systemHandle, handle, stmtText, sortBufferInfo);
	}
	else
	{
		Four			e, ret, length;
		OOSQL_DB_Value  returnValues[1];

		e = PipeWriteHeader(OOSQL_EXTPROCMSG_ID_CALL_OOSQL_EXECDIRECT, 0); OOSQL_CHECK_ERR(e);
		e = PipeWriteData(systemHandle, sizeof(OOSQL_SystemHandle)); OOSQL_CHECK_ERR(e);
		e = PipeWriteData(&handle, sizeof(handle)); OOSQL_CHECK_ERR(e);
		length = strlen(stmtText);
		e = PipeWriteData(&length, sizeof(length)); OOSQL_CHECK_ERR(e);
		e = PipeWriteData(sortBufferInfo, sizeof(OOSQL_SortBufferInfo));

		e = PipeWaitMessage(ret, returnValues, NULL); OOSQL_CHECK_ERR(e);

		return ret;
	}
	return eNOERROR;
}
Four OOSQL_ExternalProcessFunctionExecutor::OOSQL_Next(OOSQL_SystemHandle* systemHandle, OOSQL_Handle handle)
{
	if(m_runningMode == OOSQL_SERVER)
	{
		return ::OOSQL_Next(systemHandle, handle);
	}
	else
	{
		Four			e, ret;
		OOSQL_DB_Value  returnValues[1];

		e = PipeWriteHeader(OOSQL_EXTPROCMSG_ID_CALL_OOSQL_NEXT, 0); OOSQL_CHECK_ERR(e);
		e = PipeWriteData(systemHandle, sizeof(OOSQL_SystemHandle)); OOSQL_CHECK_ERR(e);
		e = PipeWriteData(&handle, sizeof(handle)); OOSQL_CHECK_ERR(e);

		e = PipeWaitMessage(ret, returnValues, NULL); OOSQL_CHECK_ERR(e);

		return ret;
	}
	return eNOERROR;
}
Four OOSQL_ExternalProcessFunctionExecutor::OOSQL_GetData(OOSQL_SystemHandle* systemHandle, OOSQL_Handle handle, Two columnNumber, Four startPos, void* columnValuePtr, Four bufferLength, Four* dataLength)
{
	if(m_runningMode == OOSQL_SERVER)
	{
		return ::OOSQL_GetData(systemHandle, handle, columnNumber, startPos, columnValuePtr, bufferLength, dataLength);
	}
	else
	{
		Four			e, ret;
		OOSQL_DB_Value  returnValues[2];

		e = PipeWriteHeader(OOSQL_EXTPROCMSG_ID_CALL_OOSQL_GETDATA, 0); OOSQL_CHECK_ERR(e);
		e = PipeWriteData(systemHandle, sizeof(OOSQL_SystemHandle)); OOSQL_CHECK_ERR(e);
		e = PipeWriteData(&handle, sizeof(handle)); OOSQL_CHECK_ERR(e);
		e = PipeWriteData(&columnNumber, sizeof(columnNumber)); OOSQL_CHECK_ERR(e);
		e = PipeWriteData(&startPos, sizeof(startPos)); OOSQL_CHECK_ERR(e);
		e = PipeWriteData(&bufferLength, sizeof(bufferLength)); OOSQL_CHECK_ERR(e);

		e = PipeWaitMessage(ret, returnValues, NULL); OOSQL_CHECK_ERR(e);
		
#ifndef SUPPORT_LARGE_DATABASE2
		memcpy(dataLength, &returnValues[1].data.i, sizeof(Four_Invariable));
#else
		memcpy(dataLength, &returnValues[1].data.ll, sizeof(Eight_Invariable));
#endif
		if(dataLength >= 0)
			memcpy(columnValuePtr, returnValues[0].data.ptr, *dataLength);

		return ret;
	}
	return eNOERROR;
}
Four OOSQL_ExternalProcessFunctionExecutor::OOSQL_GetMultiColumnData(OOSQL_SystemHandle* systemHandle, OOSQL_Handle handle, Four nColumns, OOSQL_GetDataStruct* getDataStruct)
{
	if(m_runningMode == OOSQL_SERVER)
	{
		return ::OOSQL_GetMultiColumnData(systemHandle, handle, nColumns, getDataStruct);
	}
	else
	{
		Four			e, ret;
		OOSQL_DB_Value  returnValues[1];

		e = PipeWriteHeader(OOSQL_EXTPROCMSG_ID_CALL_OOSQL_GETMULTICOLUMNDATA, 0); OOSQL_CHECK_ERR(e);
		e = PipeWriteData(systemHandle, sizeof(OOSQL_SystemHandle)); OOSQL_CHECK_ERR(e);

		e = PipeWaitMessage(ret, returnValues, NULL); OOSQL_CHECK_ERR(e);

		return ret;
	}
	return eNOERROR;
}
Four OOSQL_ExternalProcessFunctionExecutor::OOSQL_PutData(OOSQL_SystemHandle* systemHandle, OOSQL_Handle handle, Two columnNumber, Four startPos, void* columnValuePtr, Four bufferLength)
{
	if(m_runningMode == OOSQL_SERVER)
	{
		return ::OOSQL_PutData(systemHandle, handle, columnNumber, startPos, columnValuePtr, bufferLength);
	}
	else
	{
		Four			e, ret;
		OOSQL_DB_Value  returnValues[1];

		e = PipeWriteHeader(OOSQL_EXTPROCMSG_ID_CALL_OOSQL_PUTDATA, 0); OOSQL_CHECK_ERR(e);
		e = PipeWriteData(systemHandle, sizeof(OOSQL_SystemHandle)); OOSQL_CHECK_ERR(e);
		e = PipeWriteData(&handle, sizeof(handle)); OOSQL_CHECK_ERR(e);
		e = PipeWriteData(&columnNumber, sizeof(columnNumber)); OOSQL_CHECK_ERR(e);
		e = PipeWriteData(&startPos, sizeof(startPos)); OOSQL_CHECK_ERR(e);
		e = PipeWriteData(&bufferLength, sizeof(bufferLength)); OOSQL_CHECK_ERR(e);
		e = PipeWriteData(columnValuePtr, bufferLength); OOSQL_CHECK_ERR(e);
		e = PipeWriteData(&bufferLength, sizeof(bufferLength)); OOSQL_CHECK_ERR(e);

		e = PipeWaitMessage(ret, returnValues, NULL); OOSQL_CHECK_ERR(e);	

		return ret;
	}
	return eNOERROR;
}
Four OOSQL_ExternalProcessFunctionExecutor::OOSQL_GetOID(OOSQL_SystemHandle* systemHandle, OOSQL_Handle handle, Two targetNumber, OID* oid)
{
	if(m_runningMode == OOSQL_SERVER)
	{
		return ::OOSQL_GetOID(systemHandle, handle, targetNumber, oid);
	}
	else
	{
		Four			e, ret;
		OOSQL_DB_Value  returnValues[1];

		e = PipeWriteHeader(OOSQL_EXTPROCMSG_ID_CALL_OOSQL_GETOID, 0); OOSQL_CHECK_ERR(e);
		e = PipeWriteData(systemHandle, sizeof(OOSQL_SystemHandle)); OOSQL_CHECK_ERR(e);
		e = PipeWriteData(&handle, sizeof(handle)); OOSQL_CHECK_ERR(e);
		e = PipeWriteData(&targetNumber, sizeof(targetNumber)); OOSQL_CHECK_ERR(e);

		e = PipeWaitMessage(ret, returnValues, NULL); OOSQL_CHECK_ERR(e);

		memcpy(oid, &returnValues[0].data.oid, sizeof(OOSQL_StorageManager::OID));

		return ret;
	}
	return eNOERROR;
}
Four OOSQL_ExternalProcessFunctionExecutor::OOSQL_GetNumResultCols(OOSQL_SystemHandle* systemHandle, OOSQL_Handle handle, Two* nCols)
{
	if(m_runningMode == OOSQL_SERVER)
	{
		return ::OOSQL_GetNumResultCols(systemHandle, handle, nCols);
	}
	else
	{
		Four			e, ret;
		OOSQL_DB_Value  returnValues[1];

		e = PipeWriteHeader(OOSQL_EXTPROCMSG_ID_CALL_OOSQL_GETNUMRESULTCOLS, 0); OOSQL_CHECK_ERR(e);
		e = PipeWriteData(systemHandle, sizeof(OOSQL_SystemHandle)); OOSQL_CHECK_ERR(e);
		e = PipeWriteData(&handle, sizeof(handle)); OOSQL_CHECK_ERR(e);

		e = PipeWaitMessage(ret, returnValues, NULL); OOSQL_CHECK_ERR(e);

#ifndef SUPPORT_LARGE_DATABASE2
		memcpy(&nCols, &returnValues[0].data.s, sizeof(Two_Invariable));
#else
		memcpy(&nCols, &returnValues[0].data.i, sizeof(Four_Invariable));
#endif

		return ret;
	}
	return eNOERROR;
}
Four OOSQL_ExternalProcessFunctionExecutor::OOSQL_GetResultColName(OOSQL_SystemHandle* systemHandle, OOSQL_Handle handle, Two columnNumber, char* columnNameBuffer, Four bufferLength)
{
	if(m_runningMode == OOSQL_SERVER)
	{
		return ::OOSQL_GetResultColName(systemHandle, handle, columnNumber, columnNameBuffer, bufferLength);
	}
	else
	{
		Four			e, ret;
		OOSQL_DB_Value  returnValues[1];

		e = PipeWriteHeader(OOSQL_EXTPROCMSG_ID_CALL_OOSQL_GETRESULTCOLNAME, 0); OOSQL_CHECK_ERR(e);
		e = PipeWriteData(systemHandle, sizeof(OOSQL_SystemHandle)); OOSQL_CHECK_ERR(e);
		e = PipeWriteData(&handle, sizeof(handle)); OOSQL_CHECK_ERR(e);
		e = PipeWriteData(&columnNumber, sizeof(columnNumber)); OOSQL_CHECK_ERR(e);
		e = PipeWriteData(&bufferLength, sizeof(bufferLength)); OOSQL_CHECK_ERR(e);

		e = PipeWaitMessage(ret, returnValues, NULL); OOSQL_CHECK_ERR(e);
	
		memcpy(columnNameBuffer, returnValues[0].data.ptr, bufferLength);

		return ret;
	}
	return eNOERROR;
}
Four OOSQL_ExternalProcessFunctionExecutor::OOSQL_GetResultColType(OOSQL_SystemHandle* systemHandle, OOSQL_Handle handle, Two columnNumber, Four* columnType)
{
	if(m_runningMode == OOSQL_SERVER)
	{
		return ::OOSQL_GetResultColType(systemHandle, handle, columnNumber, columnType);
	}
	else
	{
		Four			e, ret;
		OOSQL_DB_Value  returnValues[1];

		e = PipeWriteHeader(OOSQL_EXTPROCMSG_ID_CALL_OOSQL_GETRESULTCOLTYPE, 0); OOSQL_CHECK_ERR(e);
		e = PipeWriteData(systemHandle, sizeof(OOSQL_SystemHandle)); OOSQL_CHECK_ERR(e);
		e = PipeWriteData(&handle, sizeof(handle)); OOSQL_CHECK_ERR(e);
		e = PipeWriteData(&columnNumber, sizeof(columnNumber)); OOSQL_CHECK_ERR(e);

		e = PipeWaitMessage(ret, returnValues, NULL); OOSQL_CHECK_ERR(e);

#ifndef SUPPORT_LARGE_DATABASE2
		memcpy(columnType, &returnValues[0].data.i, sizeof(Four_Invariable));
#else
		memcpy(columnType, &returnValues[0].data.ll, sizeof(Eight_Invariable));
#endif

		return ret;
	}
	return eNOERROR;
}
Four OOSQL_ExternalProcessFunctionExecutor::OOSQL_GetResultColLength(OOSQL_SystemHandle* systemHandle, OOSQL_Handle handle, Two columnNumber, Four* resultColLength)
{
	if(m_runningMode == OOSQL_SERVER)
	{
		return ::OOSQL_GetResultColLength(systemHandle, handle, columnNumber, resultColLength);
	}
	else
	{
		Four			e, ret;
		OOSQL_DB_Value  returnValues[1];

		e = PipeWriteHeader(OOSQL_EXTPROCMSG_ID_CALL_OOSQL_GETRESULTCOLLENGTH, 0); OOSQL_CHECK_ERR(e);
		e = PipeWriteData(systemHandle, sizeof(OOSQL_SystemHandle)); OOSQL_CHECK_ERR(e);
		e = PipeWriteData(&handle, sizeof(handle)); OOSQL_CHECK_ERR(e);
		e = PipeWriteData(&columnNumber, sizeof(columnNumber)); OOSQL_CHECK_ERR(e);

		e = PipeWaitMessage(ret, returnValues, NULL); OOSQL_CHECK_ERR(e);

#ifndef SUPPORT_LARGE_DATABASE2
		memcpy(resultColLength, &returnValues[0].data.i, sizeof(Four_Invariable));
#else
		memcpy(resultColLength, &returnValues[0].data.ll, sizeof(Eight_Invariable));
#endif

		return ret;
	}
	return eNOERROR;
}
Four OOSQL_ExternalProcessFunctionExecutor::OOSQL_GetErrorMessage(OOSQL_SystemHandle* systemHandle, Four errorCode, char* messageBuffer, Four bufferLength)
{
	if(m_runningMode == OOSQL_SERVER)
	{
		return ::OOSQL_GetErrorMessage(systemHandle, errorCode, messageBuffer, bufferLength);
	}
	else
	{
		Four			e, ret;
		OOSQL_DB_Value  returnValues[1];

		e = PipeWriteHeader(OOSQL_EXTPROCMSG_ID_CALL_OOSQL_GETERRORMESSAGE, 0); OOSQL_CHECK_ERR(e);
		e = PipeWriteData(systemHandle, sizeof(OOSQL_SystemHandle)); OOSQL_CHECK_ERR(e);
		e = PipeWriteData(&errorCode, sizeof(errorCode)); OOSQL_CHECK_ERR(e);
		e = PipeWriteData(&bufferLength, sizeof(bufferLength)); OOSQL_CHECK_ERR(e);

		e = PipeWaitMessage(ret, returnValues, NULL); OOSQL_CHECK_ERR(e);

		memcpy(messageBuffer, returnValues[0].data.ptr, bufferLength);

		return ret;
	}
	return eNOERROR;
}
Four OOSQL_ExternalProcessFunctionExecutor::OOSQL_GetErrorName(OOSQL_SystemHandle* systemHandle, Four errorCode, char* messageBuffer, Four bufferLength)
{
	if(m_runningMode == OOSQL_SERVER)
	{
		return ::OOSQL_GetErrorName(systemHandle, errorCode, messageBuffer, bufferLength);
	}
	else
	{
		Four			e, ret;
		OOSQL_DB_Value  returnValues[1];

		e = PipeWriteHeader(OOSQL_EXTPROCMSG_ID_CALL_OOSQL_GETERRORNAME, 0); OOSQL_CHECK_ERR(e);
		e = PipeWriteData(systemHandle, sizeof(OOSQL_SystemHandle)); OOSQL_CHECK_ERR(e);
		e = PipeWriteData(&errorCode, sizeof(errorCode)); OOSQL_CHECK_ERR(e);
		e = PipeWriteData(&bufferLength, sizeof(bufferLength)); OOSQL_CHECK_ERR(e);

		e = PipeWaitMessage(ret, returnValues, NULL); OOSQL_CHECK_ERR(e);

		memcpy(messageBuffer, returnValues[0].data.ptr, bufferLength);

		return ret;
	}
	return eNOERROR;
}
Four OOSQL_ExternalProcessFunctionExecutor::OOSQL_GetQueryErrorMessage(OOSQL_SystemHandle* systemHandle, OOSQL_Handle handle, char* messageBuffer, Four bufferLength)
{
	if(m_runningMode == OOSQL_SERVER)
	{
		return ::OOSQL_GetQueryErrorMessage(systemHandle, handle, messageBuffer, bufferLength);
	}
	else
	{
		Four			e, ret;
		OOSQL_DB_Value  returnValues[1];

		e = PipeWriteHeader(OOSQL_EXTPROCMSG_ID_CALL_OOSQL_GETQUERYERRORMESSAGE, 0); OOSQL_CHECK_ERR(e);
		e = PipeWriteData(systemHandle, sizeof(OOSQL_SystemHandle)); OOSQL_CHECK_ERR(e);
		e = PipeWriteData(&handle, sizeof(handle)); OOSQL_CHECK_ERR(e);
		e = PipeWriteData(&bufferLength, sizeof(bufferLength)); OOSQL_CHECK_ERR(e);

		e = PipeWaitMessage(ret, returnValues, NULL); OOSQL_CHECK_ERR(e);

		memcpy(messageBuffer, returnValues[0].data.ptr, bufferLength);

		return ret;
	}
	return eNOERROR;
}
Four OOSQL_ExternalProcessFunctionExecutor::OOSQL_OIDToOIDString(OOSQL_SystemHandle* systemHandle, OID* oid, char* oidString)
{
	if(m_runningMode == OOSQL_SERVER)
	{
		return ::OOSQL_OIDToOIDString(systemHandle, oid, oidString);
	}
	else
	{
		Four			e, ret;
		OOSQL_DB_Value  returnValues[1];

		e = PipeWriteHeader(OOSQL_EXTPROCMSG_ID_CALL_OOSQL_OIDTOOIDSTRING, 0); OOSQL_CHECK_ERR(e);
		e = PipeWriteData(systemHandle, sizeof(OOSQL_SystemHandle)); OOSQL_CHECK_ERR(e);
		e = PipeWriteData(oid, sizeof(OOSQL_StorageManager::OID)); OOSQL_CHECK_ERR(e);

		e = PipeWaitMessage(ret, returnValues, NULL); OOSQL_CHECK_ERR(e);

		memcpy(oidString, returnValues[0].data.ptr, sizeof(OOSQL_StorageManager::OID) * 2 + 1);

		return ret;
	}
	return eNOERROR;
}
Four OOSQL_ExternalProcessFunctionExecutor::OOSQL_Text_AddKeywordExtractor(OOSQL_SystemHandle* systemHandle, Four volID, char *keywordExtractor, Four version, char *keywordExtractorFilePath, char *keywordExtractorFunctionName, char *getNextPostingFunctionName, char *finalizeKeywordExtractorFunctionName, Four *keywordExtractorNo)
{
	if(m_runningMode == OOSQL_SERVER)
	{
		return ::OOSQL_Text_AddKeywordExtractor(systemHandle, volID, keywordExtractor, version, keywordExtractorFilePath, keywordExtractorFunctionName, getNextPostingFunctionName, finalizeKeywordExtractorFunctionName, keywordExtractorNo);
	}
	else
	{
		Four			e, ret;
		OOSQL_DB_Value  returnValues[1];

		e = PipeWriteHeader(OOSQL_EXTPROCMSG_ID_CALL_OOSQL_TEXT_ADDKEYWORDEXTRACTOR, 0); OOSQL_CHECK_ERR(e);
		e = PipeWriteData(systemHandle, sizeof(OOSQL_SystemHandle)); OOSQL_CHECK_ERR(e);

		e = PipeWaitMessage(ret, returnValues, NULL); OOSQL_CHECK_ERR(e);

		return ret;
	}
	return eNOERROR;
}
Four OOSQL_ExternalProcessFunctionExecutor::OOSQL_Text_AddDefaultKeywordExtractor(OOSQL_SystemHandle* systemHandle, Four volID, char *keywordExtractor, Four version, char *keywordExtractorFilePath, char *keywordExtractorFunctionName, char *getNextPostingFunctionName, char *finalizeKeywordExtractorFunctionName)
{
	if(m_runningMode == OOSQL_SERVER)
	{
		return ::OOSQL_Text_AddDefaultKeywordExtractor(systemHandle, volID, keywordExtractor, version, keywordExtractorFilePath, keywordExtractorFunctionName, getNextPostingFunctionName, finalizeKeywordExtractorFunctionName);
	}
	else
	{
		Four			e, ret;
		OOSQL_DB_Value  returnValues[1];

		e = PipeWriteHeader(OOSQL_EXTPROCMSG_ID_CALL_OOSQL_TEXT_ADDDEFAULTKEYWORDEXTRACTOR, 0); OOSQL_CHECK_ERR(e);
		e = PipeWriteData(systemHandle, sizeof(OOSQL_SystemHandle)); OOSQL_CHECK_ERR(e);

		e = PipeWaitMessage(ret, returnValues, NULL); OOSQL_CHECK_ERR(e);

		return ret;
	}
	return eNOERROR;
}
Four OOSQL_ExternalProcessFunctionExecutor::OOSQL_Text_DropKeywordExtractor(OOSQL_SystemHandle* systemHandle, Four volID, char *keywordExtractorName, Four version)
{
	if(m_runningMode == OOSQL_SERVER)
	{
		return ::OOSQL_Text_DropKeywordExtractor(systemHandle, volID, keywordExtractorName, version);
	}
	else
	{
		Four			e, ret;
		OOSQL_DB_Value  returnValues[1];

		e = PipeWriteHeader(OOSQL_EXTPROCMSG_ID_CALL_OOSQL_TEXT_DROPKEYWORDEXTRACTOR, 0); OOSQL_CHECK_ERR(e);
		e = PipeWriteData(systemHandle, sizeof(OOSQL_SystemHandle)); OOSQL_CHECK_ERR(e);

		e = PipeWaitMessage(ret, returnValues, NULL); OOSQL_CHECK_ERR(e);

		return ret;
	}
	return eNOERROR;
}
Four OOSQL_ExternalProcessFunctionExecutor::OOSQL_Text_SetKeywordExtractor(OOSQL_SystemHandle* systemHandle, Four volID, char* className, char* columeName, Four keywordExtractorNo)
{
	if(m_runningMode == OOSQL_SERVER)
	{
		return ::OOSQL_Text_SetKeywordExtractor(systemHandle, volID, className, columeName, keywordExtractorNo);
	}
	else
	{
		Four			e, ret;
		OOSQL_DB_Value  returnValues[1];

		e = PipeWriteHeader(OOSQL_EXTPROCMSG_ID_CALL_OOSQL_TEXT_SETKEYWORDEXTRACTOR, 0); OOSQL_CHECK_ERR(e);
		e = PipeWriteData(systemHandle, sizeof(OOSQL_SystemHandle)); OOSQL_CHECK_ERR(e);

		e = PipeWaitMessage(ret, returnValues, NULL); OOSQL_CHECK_ERR(e);

		return ret;
	}
	return eNOERROR;
}
Four OOSQL_ExternalProcessFunctionExecutor::OOSQL_Text_AddFilter(OOSQL_SystemHandle* systemHandle, Four volID, char *filterName, Four version, char *filterFilePath, char *filterFunctionName, Four *filterNo)
{
	if(m_runningMode == OOSQL_SERVER)
	{
		return ::OOSQL_Text_AddFilter(systemHandle, volID, filterName, version, filterFilePath, filterFunctionName, filterNo);
	}
	else
	{
		Four			e, ret;
		OOSQL_DB_Value  returnValues[1];

		e = PipeWriteHeader(OOSQL_EXTPROCMSG_ID_CALL_OOSQL_TEXT_ADDFILTER, 0); OOSQL_CHECK_ERR(e);
		e = PipeWriteData(systemHandle, sizeof(OOSQL_SystemHandle)); OOSQL_CHECK_ERR(e);

		e = PipeWaitMessage(ret, returnValues, NULL); OOSQL_CHECK_ERR(e);

		return ret;
	}
	return eNOERROR;
}
Four OOSQL_ExternalProcessFunctionExecutor::OOSQL_Text_DropFilter(OOSQL_SystemHandle* systemHandle, Four volID, char *filterName, Four version)
{
	if(m_runningMode == OOSQL_SERVER)
	{
		return ::OOSQL_Text_DropFilter(systemHandle, volID, filterName, version);
	}
	else
	{
		Four			e, ret;
		OOSQL_DB_Value  returnValues[1];

		e = PipeWriteHeader(OOSQL_EXTPROCMSG_ID_CALL_OOSQL_TEXT_DROPFILTER, 0); OOSQL_CHECK_ERR(e);
		e = PipeWriteData(systemHandle, sizeof(OOSQL_SystemHandle)); OOSQL_CHECK_ERR(e);

		e = PipeWaitMessage(ret, returnValues, NULL); OOSQL_CHECK_ERR(e);

		return ret;
	}
	return eNOERROR;
}
Four OOSQL_ExternalProcessFunctionExecutor::OOSQL_Text_SetFilter(OOSQL_SystemHandle* systemHandle, Four volID, char* className, char* columnName, Four filterNo)
{
	if(m_runningMode == OOSQL_SERVER)
	{
		return ::OOSQL_Text_SetFilter(systemHandle, volID, className, columnName, filterNo);
	}
	else
	{
		Four			e, ret;
		OOSQL_DB_Value  returnValues[1];

		e = PipeWriteHeader(OOSQL_EXTPROCMSG_ID_CALL_OOSQL_TEXT_SETFILTER, 0); OOSQL_CHECK_ERR(e);
		e = PipeWriteData(systemHandle, sizeof(OOSQL_SystemHandle)); OOSQL_CHECK_ERR(e);

		e = PipeWaitMessage(ret, returnValues, NULL); OOSQL_CHECK_ERR(e);

		return ret;
	}
	return eNOERROR;
}
Four OOSQL_ExternalProcessFunctionExecutor::OOSQL_Text_MakeIndex(OOSQL_SystemHandle* systemHandle, Four volID, Four temporaryVolId, char* className)
{
	if(m_runningMode == OOSQL_SERVER)
	{
		return ::OOSQL_Text_MakeIndex(systemHandle, volID, temporaryVolId, className);
	}
	else
	{
		Four			e, ret;
		OOSQL_DB_Value  returnValues[1];

		e = PipeWriteHeader(OOSQL_EXTPROCMSG_ID_CALL_OOSQL_TEXT_MAKEINDEX, 0); OOSQL_CHECK_ERR(e);
		e = PipeWriteData(systemHandle, sizeof(OOSQL_SystemHandle)); OOSQL_CHECK_ERR(e);

		e = PipeWaitMessage(ret, returnValues, NULL); OOSQL_CHECK_ERR(e);

		return ret;
	}
	return eNOERROR;
}
Four OOSQL_ExternalProcessFunctionExecutor::OOSQL_Text_KeywordInfoScan_Open(OOSQL_SystemHandle* systemHandle, Four volID, char* className, char* columnName, char* keyword)
{
	if(m_runningMode == OOSQL_SERVER)
	{
		return ::OOSQL_Text_KeywordInfoScan_Open(systemHandle, volID, className, columnName, keyword);
	}
	else
	{
		Four			e, ret;
		OOSQL_DB_Value  returnValues[1];

		e = PipeWriteHeader(OOSQL_EXTPROCMSG_ID_CALL_OOSQL_TEXT_KEYWORDINFOSCAN_OPEN, 0); OOSQL_CHECK_ERR(e);
		e = PipeWriteData(systemHandle, sizeof(OOSQL_SystemHandle)); OOSQL_CHECK_ERR(e);

		e = PipeWaitMessage(ret, returnValues, NULL); OOSQL_CHECK_ERR(e);

		return ret;
	}
	return eNOERROR;
}
Four OOSQL_ExternalProcessFunctionExecutor::OOSQL_Text_KeywordInfoScan_Close(OOSQL_SystemHandle* systemHandle, Four scanId)
{
	if(m_runningMode == OOSQL_SERVER)
	{
		return ::OOSQL_Text_KeywordInfoScan_Close(systemHandle, scanId);
	}
	else
	{
		Four			e, ret;
		OOSQL_DB_Value  returnValues[1];

		e = PipeWriteHeader(OOSQL_EXTPROCMSG_ID_CALL_OOSQL_TEXT_KEYWORDINFOSCAN_CLOSE, 0); OOSQL_CHECK_ERR(e);
		e = PipeWriteData(systemHandle, sizeof(OOSQL_SystemHandle)); OOSQL_CHECK_ERR(e);

		e = PipeWaitMessage(ret, returnValues, NULL); OOSQL_CHECK_ERR(e);

		return ret;
	}
	return eNOERROR;
}
Four OOSQL_ExternalProcessFunctionExecutor::OOSQL_Text_KeywordInfoScan_Next(OOSQL_SystemHandle* systemHandle, Four scanId, char* keyword, Four* nDocuments, Four* nPositions)
{
	if(m_runningMode == OOSQL_SERVER)
	{
		return ::OOSQL_Text_KeywordInfoScan_Next(systemHandle, scanId, keyword, nDocuments, nPositions);
	}
	else
	{
		Four			e, ret;
		OOSQL_DB_Value  returnValues[1];

		e = PipeWriteHeader(OOSQL_EXTPROCMSG_ID_CALL_OOSQL_TEXT_KEYWORDINFOSCAN_NEXT, 0); OOSQL_CHECK_ERR(e);
		e = PipeWriteData(systemHandle, sizeof(OOSQL_SystemHandle)); OOSQL_CHECK_ERR(e);

		e = PipeWaitMessage(ret, returnValues, NULL); OOSQL_CHECK_ERR(e);

		return ret;
	}
	return eNOERROR;
}
Four OOSQL_ExternalProcessFunctionExecutor::OOSQL_Text_KeywordInfoScanForDocument_Open(OOSQL_SystemHandle* systemHandle, Four volID, char* className, OID* oid, char* columnName, char* keyword)
{
	if(m_runningMode == OOSQL_SERVER)
	{
		return ::OOSQL_Text_KeywordInfoScanForDocument_Open(systemHandle, volID, className, oid, columnName, keyword);
	}
	else
	{
		Four			e, ret;
		OOSQL_DB_Value  returnValues[1];

		e = PipeWriteHeader(OOSQL_EXTPROCMSG_ID_CALL_OOSQL_TEXT_KEYWORDINFOSCANFORDOCUMENT_OPEN, 0); OOSQL_CHECK_ERR(e);
		e = PipeWriteData(systemHandle, sizeof(OOSQL_SystemHandle)); OOSQL_CHECK_ERR(e);

		e = PipeWaitMessage(ret, returnValues, NULL); OOSQL_CHECK_ERR(e);

		return ret;
	}
	return eNOERROR;
}
Four OOSQL_ExternalProcessFunctionExecutor::OOSQL_Text_KeywordInfoScanForDocument_Close(OOSQL_SystemHandle* systemHandle, Four scanId)
{
	if(m_runningMode == OOSQL_SERVER)
	{
		return ::OOSQL_Text_KeywordInfoScanForDocument_Close(systemHandle, scanId);
	}
	else
	{
		Four			e, ret;
		OOSQL_DB_Value  returnValues[1];

		e = PipeWriteHeader(OOSQL_EXTPROCMSG_ID_CALL_OOSQL_TEXT_KEYWORDINFOSCANFORDOCUMENT_CLOSE, 0); OOSQL_CHECK_ERR(e);
		e = PipeWriteData(systemHandle, sizeof(OOSQL_SystemHandle)); OOSQL_CHECK_ERR(e);

		e = PipeWaitMessage(ret, returnValues, NULL); OOSQL_CHECK_ERR(e);

		return ret;
	}
	return eNOERROR;
}
Four OOSQL_ExternalProcessFunctionExecutor::OOSQL_Text_KeywordInfoScanForDocument_Next(OOSQL_SystemHandle* systemHandle, Four  scanId, char* keyword, Four* nDocuments, Four* nPositions)
{
	if(m_runningMode == OOSQL_SERVER)
	{
		return ::OOSQL_Text_KeywordInfoScanForDocument_Next(systemHandle, scanId, keyword, nDocuments, nPositions);
	}
	else
	{
		Four			e, ret;
		OOSQL_DB_Value  returnValues[1];

		e = PipeWriteHeader(OOSQL_EXTPROCMSG_ID_CALL_OOSQL_TEXT_KEYWORDINFOSCANFORDOCUMENT_NEXT, 0); OOSQL_CHECK_ERR(e);
		e = PipeWriteData(systemHandle, sizeof(OOSQL_SystemHandle)); OOSQL_CHECK_ERR(e);

		e = PipeWaitMessage(ret, returnValues, NULL); OOSQL_CHECK_ERR(e);

		return ret;
	}
	return eNOERROR;
}
Four OOSQL_ExternalProcessFunctionExecutor::OOSQL_Text_DefinePostingStructure(OOSQL_SystemHandle *systemHandle, Four volID, char *className, char *attrName, OOSQL_PostingStructureInfo *postingInfo)
{
	if(m_runningMode == OOSQL_SERVER)
	{
		return ::OOSQL_Text_DefinePostingStructure(systemHandle, volID, className, attrName, postingInfo);
	}
	else
	{
		Four			e, ret;
		OOSQL_DB_Value  returnValues[1];

		e = PipeWriteHeader(OOSQL_EXTPROCMSG_ID_CALL_OOSQL_TEXT_DEFINEPOSTINGSTRUCTURE, 0); OOSQL_CHECK_ERR(e);
		e = PipeWriteData(systemHandle, sizeof(OOSQL_SystemHandle)); OOSQL_CHECK_ERR(e);

		e = PipeWaitMessage(ret, returnValues, NULL); OOSQL_CHECK_ERR(e);

		return ret;
	}
	return eNOERROR;
}
OOSQL_TimeZone OOSQL_ExternalProcessFunctionExecutor::OOSQL_GetLocalTimeZone(OOSQL_SystemHandle* systemHandle)
{
	if(m_runningMode == OOSQL_SERVER)
	{
		return ::OOSQL_GetLocalTimeZone(systemHandle);
	}
	else
	{
	}
	OOSQL_TimeZone tz;

	return tz;
}
void OOSQL_ExternalProcessFunctionExecutor::OOSQL_SetCurTime(OOSQL_SystemHandle* systemHandle, OOSQL_Time* time, OOSQL_TimeZone tz)
{
	if(m_runningMode == OOSQL_SERVER)
	{
		OOSQL_SetCurTime(systemHandle, time, tz);
	}
	else
	{
	}
}
unsigned short OOSQL_ExternalProcessFunctionExecutor::OOSQL_GetHour(OOSQL_SystemHandle* systemHandle, OOSQL_Time* time)
{
	if(m_runningMode == OOSQL_SERVER)
	{
		return ::OOSQL_GetHour(systemHandle, time);
	}
	else
	{
		Four			e, ret;
		OOSQL_DB_Value  returnValues[1];

		e = PipeWriteHeader(OOSQL_EXTPROCMSG_ID_CALL_OOSQL_GETHOUR, 0); OOSQL_CHECK_ERR(e);
		e = PipeWriteData(systemHandle, sizeof(OOSQL_SystemHandle)); OOSQL_CHECK_ERR(e);

		e = PipeWaitMessage(ret, returnValues, NULL); OOSQL_CHECK_ERR(e);

		return ret;
	}
	return eNOERROR;
}
unsigned short OOSQL_ExternalProcessFunctionExecutor::OOSQL_GetMinute(OOSQL_SystemHandle* systemHandle, OOSQL_Time* time)
{
	if(m_runningMode == OOSQL_SERVER)
	{
		return ::OOSQL_GetMinute(systemHandle, time);
	}
	else
	{
		Four			e, ret;
		OOSQL_DB_Value  returnValues[1];

		e = PipeWriteHeader(OOSQL_EXTPROCMSG_ID_CALL_OOSQL_GETMINUTE, 0); OOSQL_CHECK_ERR(e);
		e = PipeWriteData(systemHandle, sizeof(OOSQL_SystemHandle)); OOSQL_CHECK_ERR(e);

		e = PipeWaitMessage(ret, returnValues, NULL); OOSQL_CHECK_ERR(e);

		return ret;
	}
	return eNOERROR;
}
unsigned short OOSQL_ExternalProcessFunctionExecutor::OOSQL_GetSecond(OOSQL_SystemHandle* systemHandle, OOSQL_Time* time)
{
	if(m_runningMode == OOSQL_SERVER)
	{
		return ::OOSQL_GetSecond(systemHandle, time);
	}
	else
	{
		Four			e, ret;
		OOSQL_DB_Value  returnValues[1];

		e = PipeWriteHeader(OOSQL_EXTPROCMSG_ID_CALL_OOSQL_GETSECOND, 0); OOSQL_CHECK_ERR(e);
		e = PipeWriteData(systemHandle, sizeof(OOSQL_SystemHandle)); OOSQL_CHECK_ERR(e);

		e = PipeWaitMessage(ret, returnValues, NULL); OOSQL_CHECK_ERR(e);

		return ret;
	}
	return eNOERROR;
}
unsigned short OOSQL_ExternalProcessFunctionExecutor::OOSQL_GetYear(OOSQL_SystemHandle* systemHandle, OOSQL_Date* date)
{
	if(m_runningMode == OOSQL_SERVER)
	{
		return ::OOSQL_GetYear(systemHandle, date);
	}
	else
	{
		Four			e, ret;
		OOSQL_DB_Value  returnValues[1];

		e = PipeWriteHeader(OOSQL_EXTPROCMSG_ID_CALL_OOSQL_GETYEAR, 0); OOSQL_CHECK_ERR(e);
		e = PipeWriteData(systemHandle, sizeof(OOSQL_SystemHandle)); OOSQL_CHECK_ERR(e);

		e = PipeWaitMessage(ret, returnValues, NULL); OOSQL_CHECK_ERR(e);

		return ret;
	}
	return eNOERROR;
}
unsigned short OOSQL_ExternalProcessFunctionExecutor::OOSQL_GetMonth(OOSQL_SystemHandle* systemHandle, OOSQL_Date* date)
{
	if(m_runningMode == OOSQL_SERVER)
	{
		return ::OOSQL_GetMonth(systemHandle, date);
	}
	else
	{
		Four			e, ret;
		OOSQL_DB_Value  returnValues[1];

		e = PipeWriteHeader(OOSQL_EXTPROCMSG_ID_CALL_OOSQL_GETMONTH, 0); OOSQL_CHECK_ERR(e);
		e = PipeWriteData(systemHandle, sizeof(OOSQL_SystemHandle)); OOSQL_CHECK_ERR(e);

		e = PipeWaitMessage(ret, returnValues, NULL); OOSQL_CHECK_ERR(e);

		return ret;
	}
	return eNOERROR;
}
unsigned short OOSQL_ExternalProcessFunctionExecutor::OOSQL_GetDay(OOSQL_SystemHandle* systemHandle, OOSQL_Date* date)
{
	if(m_runningMode == OOSQL_SERVER)
	{
		return ::OOSQL_GetDay(systemHandle, date);
	}
	else
	{
		Four			e, ret;
		OOSQL_DB_Value  returnValues[1];

		e = PipeWriteHeader(OOSQL_EXTPROCMSG_ID_CALL_OOSQL_GETDAY, 0); OOSQL_CHECK_ERR(e);
		e = PipeWriteData(systemHandle, sizeof(OOSQL_SystemHandle)); OOSQL_CHECK_ERR(e);

		e = PipeWaitMessage(ret, returnValues, NULL); OOSQL_CHECK_ERR(e);

		return ret;
	}
	return eNOERROR;
}
char* OOSQL_ExternalProcessFunctionExecutor::OOSQL_GetVersionString(void)
{
	if(m_runningMode == OOSQL_SERVER)
	{
		return ::OOSQL_GetVersionString();
	}
	else
	{
		Four			e, ret;
		OOSQL_DB_Value  returnValues[1];

		e = PipeWriteHeader(OOSQL_EXTPROCMSG_ID_CALL_OOSQL_GETVERSIONSTRING, 0); 
		if(e < eNOERROR) return "Fails in executing OOSQL_GetVersionString()";

		e = PipeWaitMessage(ret, returnValues, NULL); 
		if(e < eNOERROR) return "Fails in executing OOSQL_GetVersionString()";

		return (char*)returnValues[0].data.ptr;
	}
}

Four OOSQL_ExternalProcessFunctionExecutor::OOSQL_GetTimeElapsed(OOSQL_SystemHandle* systemHandle, Four* timeInMilliSeconds)
{
	if(m_runningMode == OOSQL_SERVER)
	{
		return ::OOSQL_GetTimeElapsed(systemHandle, timeInMilliSeconds);
	}
	else
	{
		Four			e, ret;
		OOSQL_DB_Value  returnValues[1];

		e = PipeWriteHeader(OOSQL_EXTPROCMSG_ID_CALL_OOSQL_GETTIMEELAPSED, 0); OOSQL_CHECK_ERR(e);
		e = PipeWriteData(systemHandle, sizeof(OOSQL_SystemHandle)); OOSQL_CHECK_ERR(e);

		e = PipeWaitMessage(ret, returnValues, NULL); OOSQL_CHECK_ERR(e);

		return ret;
	}
	return eNOERROR;
}
Four OOSQL_ExternalProcessFunctionExecutor::OOSQL_ResetTimeElapsed(OOSQL_SystemHandle* systemHandle)
{
	if(m_runningMode == OOSQL_SERVER)
	{
		return ::OOSQL_ResetTimeElapsed(systemHandle);
	}
	else
	{
		Four			e, ret;
		OOSQL_DB_Value  returnValues[1];

		e = PipeWriteHeader(OOSQL_EXTPROCMSG_ID_CALL_OOSQL_RESETTIMEELAPSED, 0); OOSQL_CHECK_ERR(e);
		e = PipeWriteData(systemHandle, sizeof(OOSQL_SystemHandle)); OOSQL_CHECK_ERR(e);

		e = PipeWaitMessage(ret, returnValues, NULL); OOSQL_CHECK_ERR(e);

		return ret;
	}
	return eNOERROR;
}
