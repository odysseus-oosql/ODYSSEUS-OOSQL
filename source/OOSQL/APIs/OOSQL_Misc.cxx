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
#include "OOSQL_Eval_Util.hxx"

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eBADPARAMETER_OOSQL 
< eNOERROR         

IMPLEMENTATION:
****************************************************************************/
Four OOSQL_OIDToOIDString(
	OOSQL_SystemHandle* systemHandle,	// IN  
	OID*				oid,			// IN  
	char*				oidString		// OUT 
)
{
    return oosql_ConvertFromOIDStructureToOIDString((OOSQL_StorageManager::OID*)oid, oidString);
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
static Four createSchema(
	OOSQL_SystemHandle*	systemHandle,	// IN  
	Four				volID			// IN  
)
{
	Four e;

	e = oosql_SQL_InitExecFinal(systemHandle, volID, "create table LOM_SYS_FUNCTIONS ("
							         "		FuncSchema		VARCHAR(64),"
							         "		FuncName		VARCHAR(64),"
							         "		SpecificName	VARCHAR(64),"
							         "		Definer			VARCHAR(64),"
							         "		FuncID 			INTEGER,"
							         "		ReturnType 		SMALLINT,"
							         "		Origin			CHAR(1),"
							         "		Type			CHAR(1),"
							         "		ParamCount 		SMALLINT,"
							         "		ParamSignature 	VARCHAR(180),"
							         "		CreateTime 		TIMESTAMP,"
							         "		Deterministic	CHAR(1),"			
							         "		SideEffects 	CHAR(1),"
							         "		Fenced			CHAR(1),"
							         "		NullCall		CHAR(1),"
							         "		CastFunction	CHAR(1),"
							         "		AssignFunction	CHAR(1),"
							         "		ScratchPad		CHAR(1),"
							         "		FinalCall 		CHAR(1),"
							         "		Parallelizable 	CHAR(1),"
							         "		Dbinfo			CHAR(1),"
							         "		ResultCols 		SMALLINT,"
							         "		Language		CHAR(8),"
							         "		Implementation	VARCHAR(1000),"
							         "		ParamStyle		CHAR(8),"
							         "		SourceSchema 	VARCHAR(64),"
							         "		SourceSpecific  VARCHAR(64))");
	if(e < 0) OOSQL_ERROR(systemHandle, e);
	

	e = oosql_SQL_InitExecFinal(systemHandle, volID, "create table LOM_SYS_FUNCPARMS ("
		                             "		FuncSchema		VARCHAR(64),"
									 "		FuncName		VARCHAR(64),"
									 "		SpecificName	VARCHAR(64),"
									 "		FuncID 			SMALLINT,"
									 "		RowType			CHAR(1),"
									 "		Ordinal			SMALLINT,"
									 "		ParmName		VARCHAR(64),"
									 "		TypeSchema		VARCHAR(64),"
									 "		TypeName		VARCHAR(64),"
									 "      TypeID          INTEGER,"
									 "		Length			INTEGER,"
									 "		Scale			SMALLINT,"
									 "		CodePage		SMALLINT,"
									 "		CastFuncID  	INTEGER,"
									 "		AsLocator 		CHAR(1))");
	if(e < 0) OOSQL_ERROR(systemHandle, e);

	e = oosql_SQL_InitExecFinal(systemHandle, volID, "create table LOM_SYS_PROCEDURES ("
		                             "		ProcSchema		VARCHAR(64),"
							         "		ProcName		VARCHAR(64),"
							         "		SpecificName	VARCHAR(64),"
							         "		Definer			VARCHAR(64),"
							         "		ProcID			INTEGER,"
							         "		Origin			CHAR(1),"
							         "		Type			CHAR(1),"
							         "		ParamCount 		SMALLINT,"
							         "		ParamSignature 	VARCHAR(180),"
							         "		CreateTime		TIMESTAMP,"
							         "		Deterministic	CHAR(1),"			
							         "		Fenced			CHAR(1),"
							         "		NullCall		CHAR(1),"
							         "		Language		CHAR(8),"
							         "		Implementation	VARCHAR(1000),"
							         "		ParamStyle		CHAR(8),"
							         "		ResultSets 		SMALLINT,"
							         "		Remarks			VARCHAR(64))");
	if(e < 0) OOSQL_ERROR(systemHandle, e);

	e = oosql_SQL_InitExecFinal(systemHandle, volID, "create table LOM_SYS_PROCPARMS ("
		                             "		ProcSchema		VARCHAR(64),"
									 "		ProcName		VARCHAR(64),"
									 "		SpecificName	VARCHAR(64),"
									 "		ProcID 			SMALLINT,"
									 "		RowType			CHAR(1),"
									 "		Ordinal			SMALLINT,"
									 "		ParmName		VARCHAR(64),"
									 "		TypeSchema		VARCHAR(64),"
									 "		TypeName		VARCHAR(64),"
									 "      TypeID          INTEGER,"
									 "		Length			INTEGER,"
									 "		Scale			SMALLINT,"
									 "		CodePage		SMALLINT,"
									 "		ParamMode 		VARCHAR(5),"
									 "		AsLocator 		CHAR(1))");
	if(e < 0) OOSQL_ERROR(systemHandle, e);

	// CREATE INDEX
	e = oosql_SQL_InitExecFinal(systemHandle, volID, "create index LOM_SYS_FUNCTIONS_FUNCNAME_INDEX on LOM_SYS_FUNCTIONS(FuncName)"); 
	if(e < 0) OOSQL_ERROR(systemHandle, e);
	e = oosql_SQL_InitExecFinal(systemHandle, volID, "create index LOM_SYS_FUNCTIONS_SPECIFICNAME_INDEX on LOM_SYS_FUNCTIONS(SpecificName)"); 
	if(e < 0) OOSQL_ERROR(systemHandle, e);
	e = oosql_SQL_InitExecFinal(systemHandle, volID, "create index LOM_SYS_FUNCTIONS_FUNC_ID_INDEX on LOM_SYS_FUNCTIONS(FuncID)"); 
	if(e < 0) OOSQL_ERROR(systemHandle, e);
	e = oosql_SQL_InitExecFinal(systemHandle, volID, "create index LOM_SYS_FUNCPARMS_FUNC_ID_INDEX on LOM_SYS_FUNCPARMS(FuncID)"); 
	if(e < 0) OOSQL_ERROR(systemHandle, e);
	e = oosql_SQL_InitExecFinal(systemHandle, volID, "create index LOM_SYS_PROCEDURES_PROCNAME_INDEX on LOM_SYS_PROCEDURES(ProcName)"); 
	if(e < 0) OOSQL_ERROR(systemHandle, e);
	e = oosql_SQL_InitExecFinal(systemHandle, volID, "create index LOM_SYS_PROCEDURES_SPECIFICNAME_INDEX on LOM_SYS_PROCEDURES(SpecificName)"); 
	if(e < 0) OOSQL_ERROR(systemHandle, e);
	e = oosql_SQL_InitExecFinal(systemHandle, volID, "create index LOM_SYS_PROCEDURES_PROC_ID_INDEX on LOM_SYS_PROCEDURES(ProcID)"); 
	if(e < 0) OOSQL_ERROR(systemHandle, e);
	e = oosql_SQL_InitExecFinal(systemHandle, volID, "create index LOM_SYS_PROCPARMS_PROC_ID_INDEX on LOM_SYS_PROCPARMS(ProcID)"); 
	if(e < 0) OOSQL_ERROR(systemHandle, e);

	// create counter LOM_SYS_FUNCTION_ID_COUNTER, LOM_SYS_PROCEDURE_ID_COUNTER
	CounterID id;
	
	e = LRDS_CreateCounter(LOM_GET_LRDS_HANDLE(&(OOSQL_GET_LOM_SYSTEMHANDLE(systemHandle))), volID, "LOM_SYS_FUNCTION_ID_COUNTER", 0, &id);
	if(e < 0) OOSQL_ERROR(systemHandle, e);

	e = LRDS_CreateCounter(LOM_GET_LRDS_HANDLE(&(OOSQL_GET_LOM_SYSTEMHANDLE(systemHandle))), volID, "LOM_SYS_PROCEDURE_ID_COUNTER", 0, &id);
	if(e < 0) OOSQL_ERROR(systemHandle, e);

	return eNOERROR;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four oosql_FormatDataVolume(
	OOSQL_SystemHandle* systemHandle,		// IN  
	Four				numOfDevices,		// IN  
	char**				devNameList,		// IN  
	char*				volName,			// IN  
	Four				volId,				// IN  
	Four				extentSize,			// IN  
	Four*				numPagesInDevice,	// IN  
	Four				segmentSize			// IN  
)
{
	Four e;

	Four   volID;
	XactID xactID;

	e = OOSQL_Mount(systemHandle, numOfDevices, devNameList, &volID);
	if(e < eNOERROR) OOSQL_ERROR(systemHandle, e);

	e = OOSQL_TransBegin(systemHandle, &xactID, X_RR_RR);
	if(e < eNOERROR)
	{
		OOSQL_Dismount(systemHandle, volID);
		OOSQL_ERROR(systemHandle, e);
	}

	e = createSchema(systemHandle, volID);
	if(e < eNOERROR) 
	{
		OOSQL_TransAbort(systemHandle, &xactID);
		OOSQL_Dismount(systemHandle, volID);
		OOSQL_ERROR(systemHandle, e);
	}

	e = OOSQL_TransCommit(systemHandle, &xactID);
	if(e < eNOERROR) 
	{
		OOSQL_Dismount(systemHandle, volID);
		if(e < eNOERROR) OOSQL_ERROR(systemHandle, e);
	}

	e = OOSQL_Dismount(systemHandle, volID);
	if(e < eNOERROR) OOSQL_ERROR(systemHandle, e);

	return eNOERROR;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OOSQL_FormatDataVolume(
	OOSQL_SystemHandle* systemHandle,		// IN  
	Four				numOfDevices,		// IN 
	char**				devNameList,		// IN  
	char*				volName,			// IN  
	Four				volId,				// IN  
	Four				extentSize,			// IN  
	Four*				numPagesInDevice,	// IN  
	Four				segmentSize			// IN  
)
{
	Four e;

	e = LOM_FormatDataVolume(&OOSQL_GET_LOM_SYSTEMHANDLE(systemHandle), 
							 numOfDevices, devNameList, volName, 
							 volId, extentSize, numPagesInDevice, segmentSize);
	if (e < 0) OOSQL_ERROR(systemHandle, e);

	e = oosql_FormatDataVolume(systemHandle, numOfDevices, devNameList, volName, 
							   volId, extentSize, numPagesInDevice, segmentSize);
	if (e < 0) OOSQL_ERROR(systemHandle, e);

	return eNOERROR;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OOSQL_FormatTempDataVolume(
	OOSQL_SystemHandle* systemHandle,		// IN  
	Four				numOfDevices,		// IN  
	char**				devNameList,		// IN  
	char*				volName,			// IN  
	Four				volId,				// IN  
	Four				extentSize,			// IN  
	Four*				numPagesInDevice,	// IN  
	Four				segmentSize			// IN  
)
{
	Four e;

	e = LOM_FormatTempDataVolume(&OOSQL_GET_LOM_SYSTEMHANDLE(systemHandle), 
								 numOfDevices, devNameList, volName, 
								 volId, extentSize, numPagesInDevice, segmentSize);
	if (e < 0) OOSQL_ERROR(systemHandle, e);

	e = oosql_FormatDataVolume(systemHandle, numOfDevices, devNameList, volName, 
							   volId, extentSize, numPagesInDevice, segmentSize);
	if (e < 0) OOSQL_ERROR(systemHandle, e);

	return eNOERROR;
}

#include <sys/types.h>
#include <sys/timeb.h>

static struct timeb start_timevar;

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OOSQL_ResetTimeElapsed(
	OOSQL_SystemHandle* systemHandle	// IN  
)
{
	ftime(&start_timevar);
	return eNOERROR;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OOSQL_GetTimeElapsed(
	OOSQL_SystemHandle* systemHandle,		// IN  
	Four*				timeInMilliSeconds	// IN  
)
{
	Four			start_time, current_time;
	struct timeb	current_timevar;

	ftime(&current_timevar);

	start_time   = start_timevar.time * 1000 + start_timevar.millitm;
	current_time = current_timevar.time * 1000 + current_timevar.millitm;

	*timeInMilliSeconds = current_time - start_time;

	return eNOERROR;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eBADPARAMETER_OOSQL 
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OOSQL_DumpPlan(
	OOSQL_SystemHandle* systemHandle,	// IN  
	OOSQL_Handle		handle,			// IN  
	void*				outBuffer,		// IN  
	int					outBufferSize	// IN  
)
{
#if defined(WIN32) && defined(_DEBUG)
	if(!OOSQL_CHECKGDSINSTTABLE(systemHandle))
		return eBADPARAMETER_OOSQL;

	if(OOSQL_QUERYINSTTABLE(systemHandle)[handle].inUse == SM_FALSE)
		return eBADPARAMETER_OOSQL;

	return OOSQL_QUERYINSTTABLE(systemHandle)[handle].query->DumpPlan(outBuffer, outBufferSize);
#else
	if(outBufferSize > 0)
		((char*)outBuffer)[0] = '\0';
	return eNOERROR;
#endif
}


Four OOSQL_ResetPageAccessed(
	OOSQL_SystemHandle* systemHandle	// IN  
)
{
	Four 	e;

	e = LRDS_ResetNumberOfDiskIO(LOM_GET_LRDS_HANDLE(&(OOSQL_GET_LOM_SYSTEMHANDLE(systemHandle))));
	OOSQL_CHECK_ERR(e);

	return eNOERROR;
}

Four OOSQL_ReportTimeAndPageAccess(
	OOSQL_SystemHandle* systemHandle	// IN  
)
{
	Four	current_time;
    Four    io_num_of_reads, io_num_of_writes;
	Four	e;

	e = OOSQL_GetTimeElapsed(systemHandle, &current_time);
	OOSQL_CHECK_ERR(e);

    e = LRDS_GetNumberOfDiskIO(LOM_GET_LRDS_HANDLE(&(OOSQL_GET_LOM_SYSTEMHANDLE(systemHandle))), &io_num_of_reads, &io_num_of_writes);
	OOSQL_CHECK_ERR(e);


	printf("Time Elapsed = %ld ms, io reads = %ld, io writes = %ld\n", current_time, io_num_of_reads, io_num_of_writes);
	fflush(stdout);
	
	return eNOERROR;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
eBADPARAMETER_OOSQL 
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four OOSQL_EstimateNumResults(
	OOSQL_SystemHandle*		systemHandle,	// IN  
	OOSQL_Handle			handle,			// IN  
	Four*					nResults		// OUT 
)
{
	if(!OOSQL_CHECKGDSINSTTABLE(systemHandle))
		return eBADPARAMETER_OOSQL;

	if(OOSQL_QUERYINSTTABLE(systemHandle)[handle].inUse == SM_FALSE)
		return eBADPARAMETER_OOSQL;

	return OOSQL_QUERYINSTTABLE(systemHandle)[handle].query->EstimateNumResults(nResults);
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:


IMPLEMENTATION:
****************************************************************************/
Four OOSQL_Sleep(double sec)
{
    return Util_Sleep(sec);
}
