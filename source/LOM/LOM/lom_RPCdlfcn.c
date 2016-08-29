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

#ifdef USE_RPC
/*
 * Module: lom_RPCdlfcn.c
 *
 * Description:
 *
 * Imports:
 *
 * Exports:
 *
 */
#include "LOM_Internal.h"
#include "LOM.h"
#include "dllbroker.h"
#include "dllserver.h"
#include <dlfcn.h>

#define MAXCOMMANDLENGTH    1000
#define MAXCONNECTTRIAL     1000

Four positionInBulkBuffer = 0;
Four datasizeOfBulkBuffer = -1;
char bulkBuffer[LOM_RPCKE_BULKBUFSIZE];


void *lom_RPCdlopen(
	LOM_Handle 		*handle, 		/* IN LOM system handle*/
	char			*moduleName, 	/* IN module path name to open as a dynamic linked library */
	int 			mode) 			/* IN open mode */
{
	Four 						i, j, k;
	Four						trialCount;
	Four 						e;
    CLIENT 						*broker_clnt;
    dllbroker_connect_reply  	*result_1;
    rpc_dlopen_reply  			*result_2;
	dllbroker_connect_arg  		dllbroker_connect_1_arg;
    rpc_dlopen_arg  			rpc_dlopen_1_arg;

    char    					*broker_path;
    char    					command[MAXCOMMANDLENGTH];
    char    					*broker_name = "dllbroker";


	/* find dynamic library in the LOM_DLFCNTABLE */
	for(i = 0; i < LOM_DLFCNTABLE_ENTRIES(handle); i++) 
	{
		if(LOM_DLFCNTABLE(handle)[i].handle.dllHandle && LOM_DLFCNTABLE(handle)[i].handle.dllClient)
		{
			if(!strncmp(LOM_DLFCNTABLE(handle)[i].moduleName, moduleName, LOM_MAXPATHLENGTH))
			{
				/* found it */
				return (void*)&LOM_DLFCNTABLE(handle)[i].handle;
			}
		}
	}

	/* not found, then open it and return */
	/* find free entry */
	for(i = 0; i < LOM_DLFCNTABLE_ENTRIES(handle); i++) 
		if(LOM_DLFCNTABLE(handle)[i].handle.dllHandle == NULL && LOM_DLFCNTABLE(handle)[i].handle.dllClient == NULL)
			break;

	if(i == LOM_DLFCNTABLE_ENTRIES(handle))
	{
		/* doubling table */
		e = LRDS_doublesizeVarArray(LOM_GET_LRDS_HANDLE(handle), LOM_DLFCNTABLE_PTR(handle), sizeof(lom_DllfcnTableEntry));
		if(e < eNOERROR) return NULL;

		/* Initialize the newly allocated entries */
		for (j = i; j < LOM_DLFCNTABLE_ENTRIES(handle); j++) 
		{
			LOM_DLFCNTABLE(handle)[j].handle.dllHandle = NULL;
			LOM_DLFCNTABLE(handle)[j].handle.dllClient = NULL;

			for(k = 0; k < LOM_MAXDLLFUNCPTRS; k++)
			{
				LOM_DLFCNTABLE(handle)[j].func[k].dllFunc   = NULL;
				LOM_DLFCNTABLE(handle)[j].func[k].dllClient = NULL;
			}
		}
	}


	/* launch dllserver by request to dllbroker */
    broker_clnt = clnt_create("localhost", DLLBROKERPROG, DLLBROKERVERS, "tcp");
    if (broker_clnt == (CLIENT *) NULL) {

        /* get an enviroment variable */
        broker_path = getenv("OOSQL_DLLBROKER_PATH");
        if(broker_path == NULL) {
            fprintf(stderr, "OOSQL_DLLBROKER_PATH is not defined\n");
			return NULL;
        }
  
#ifndef WIN32
        sprintf(command,"%s/%s &", broker_path, broker_name);
#else
        sprintf(command,"start %s\\%s", broker_path, broker_name);
#endif

        system(command);

		for (trialCount = 0; trialCount < MAXCONNECTTRIAL; trialCount++)
		{
			broker_clnt = clnt_create("localhost", DLLBROKERPROG, DLLBROKERVERS, "tcp");
			if(broker_clnt != NULL)
				break;
		}

		if (trialCount == MAXCONNECTTRIAL)
		{
			printf("Failure occur in connecting to dllbroker\n");
			clnt_pcreateerror("localhost");
			return NULL;
		}
    }

	dllbroker_connect_1_arg.protocol = ODYS_TCP | ODYS_UDP;
    result_1 = dllbroker_connect_1(&dllbroker_connect_1_arg, broker_clnt);
    if (result_1 == (dllbroker_connect_reply *) NULL) {
        clnt_perror(broker_clnt, "dllbroker_connect_1() call failed");
    }


	/* connect to dllserver */
    for (trialCount = 0; trialCount < MAXCONNECTTRIAL; trialCount++)
    {
    	LOM_DLFCNTABLE(handle)[i].handle.dllClient = clnt_create("localhost", result_1->serverNo, 1, "tcp");
    	if (LOM_DLFCNTABLE(handle)[i].handle.dllClient != (CLIENT *) NULL) {
			break;
        }
    }
    if (trialCount == MAXCONNECTTRIAL)
    {
        printf("Failure occur in connecting to dllserver\n");
        clnt_pcreateerror("localhost");
        return NULL;
    }

	/* call rpc-dlopen  */
	rpc_dlopen_1_arg.moduleName = moduleName;
	rpc_dlopen_1_arg.mode = mode;
    result_2 = rpc_dlopen_1(&rpc_dlopen_1_arg, LOM_DLFCNTABLE(handle)[i].handle.dllClient);
    if (result_2 == (rpc_dlopen_reply *) NULL) {
        clnt_perror(LOM_DLFCNTABLE(handle)[i].handle.dllClient, "rpc_dlopen_1() call failed");
    }


	LOM_DLFCNTABLE(handle)[i].handle.dllHandle = (void *)result_2->handle;
	strncpy(LOM_DLFCNTABLE(handle)[i].moduleName, moduleName, LOM_MAXPATHLENGTH);

	return (void*)&LOM_DLFCNTABLE(handle)[i].handle;
}

void *lom_RPCdlsym(
	LOM_Handle 		*handle, 		/* IN LOM system handle*/
	void			*dllHandle,		/* IN dl handle */ 
	char 			*name)			/* IN symbol name for callable function */ 
{
	Four			i, j;
	lom_dllHandle*	rpcHandle;

	rpcHandle = (lom_dllHandle*)dllHandle;

	for (i = 0; i < LOM_DLFCNTABLE_ENTRIES(handle); i++) 
	{
		if(LOM_DLFCNTABLE(handle)[i].handle.dllHandle == rpcHandle->dllHandle && LOM_DLFCNTABLE(handle)[i].handle.dllClient == rpcHandle->dllClient)
		{
			for(j = 0; j < LOM_MAXDLLFUNCPTRS; j++)
			{
				if(LOM_DLFCNTABLE(handle)[i].func[j].dllFunc != NULL && 
				   !strcmp(LOM_DLFCNTABLE(handle)[i].func[j].funcName, name))
				{
					return &LOM_DLFCNTABLE(handle)[i].func[j];
				}
			}

			for(j = 0; j < LOM_MAXDLLFUNCPTRS; j++)
			{
				if(LOM_DLFCNTABLE(handle)[i].func[j].dllFunc == NULL)
				{
					rpc_dlsym_reply		*result;
					rpc_dlsym_arg		rpc_dlsym_1_arg;

					rpc_dlsym_1_arg.handle = (Four)rpcHandle->dllHandle;
					rpc_dlsym_1_arg.funcName = name;
					result = rpc_dlsym_1(&rpc_dlsym_1_arg, rpcHandle->dllClient);
					if (result == (rpc_dlsym_reply *) NULL) {
						clnt_perror(rpcHandle->dllClient, "call failed");
					}

					strcpy(LOM_DLFCNTABLE(handle)[i].func[j].funcName, name);
					LOM_DLFCNTABLE(handle)[i].func[j].dllFunc   = (void*)result->funcPtr;
					LOM_DLFCNTABLE(handle)[i].func[j].dllClient = rpcHandle->dllClient;
					if(LOM_DLFCNTABLE(handle)[i].func[j].dllFunc == NULL)
						return NULL;
					else
						return &LOM_DLFCNTABLE(handle)[i].func[j];
				}
			}
		}
	}

	return NULL;
}


int lom_RPCdlclose(
	LOM_Handle 		*handle, 		/* IN LOM system handle*/
	void			*dllHandle)		/* IN dl handle */
{
    rpc_dlclose_reply	*result;
	rpc_dlclose_arg		rpc_dlclose_1_arg;
	lom_dllHandle*		rpcHandle;

	rpcHandle = (lom_dllHandle*)dllHandle;

	rpc_dlclose_1_arg.handle = (CS_FOUR)rpcHandle->dllHandle;
    result = rpc_dlclose_1(&rpc_dlclose_1_arg, rpcHandle->dllClient);
	if (result == (rpc_dlclose_reply *) NULL) {
		clnt_perror(rpcHandle->dllClient, "call failed");
	}

	return (int)(result->errCode);
}


char* lom_RPCdlerror(
	LOM_Handle 		*handle, 		/* IN LOM system handle*/
	void			*dllHandle)		/* IN dl handle */
{
    rpc_dlerror_reply	*result;
    char *				rpc_dlerror_1_arg;
	lom_dllHandle*		rpcHandle;

	rpcHandle = (lom_dllHandle*)dllHandle;

    result = rpc_dlerror_1((void *)&rpc_dlerror_1_arg, rpcHandle->dllClient);
    if (result == (rpc_dlerror_reply *) NULL) {
    	clnt_perror(rpcHandle->dllClient, "call failed");
    }

    return (char *)result->errMsg;
}

#ifndef SLIMDOWN_TEXTIR

Four lom_RPCKeywordExtractorInit (
	lom_FptrToKeywordExtractor	funcPtr,			/* IN */
	Four 						locationOfContent,	/* IN */ 
	LOM_Handle 					*handle,			/* IN */ 
	Four 						volId, 				/* IN */
	char 						*className, 		/* IN */
	OID 						*oid, 				/* IN */
	Two 						colNo, 				/* IN */
	char 						*inFileName, 		/* IN */
	Four 						*resultHandle		/* OUT */
)
{
    rpc_KEinit_reply	*reply;
    rpc_KEinit_arg		rpc_keinit_1_arg;
	lom_dllFunc*		rpcHandle;

	rpcHandle = (lom_dllFunc*)funcPtr;

    rpc_keinit_1_arg.funcPtr = (Four)rpcHandle->dllFunc;
    rpc_keinit_1_arg.locationOfContent = locationOfContent;
    rpc_keinit_1_arg.volId = volId;
    rpc_keinit_1_arg.className = className;
    rpc_keinit_1_arg.colNo = colNo;
    rpc_keinit_1_arg.inFileName = inFileName;

	if (handle != NULL)
	{
    	rpc_keinit_1_arg.handle.serverInstanceId = handle->serverInstanceId;
    	rpc_keinit_1_arg.handle.instanceId = handle->instanceId;
	}
	else
	{
    	rpc_keinit_1_arg.handle.serverInstanceId = 0; 
    	rpc_keinit_1_arg.handle.instanceId = 0; 
	}

	if (oid != NULL)
	{
		rpc_keinit_1_arg.oid.pageNo = oid->pageNo; 
		rpc_keinit_1_arg.oid.volNo = oid->volNo; 
		rpc_keinit_1_arg.oid.slotNo = oid->slotNo; 
		rpc_keinit_1_arg.oid.unique = oid->unique; 
		rpc_keinit_1_arg.oid.classID = oid->classID; 
	}
	else
	{
    	rpc_keinit_1_arg.oid.pageNo = 0; 
    	rpc_keinit_1_arg.oid.volNo = 0; 
    	rpc_keinit_1_arg.oid.slotNo = 0; 
    	rpc_keinit_1_arg.oid.unique = 0;
    	rpc_keinit_1_arg.oid.classID = 0;
	}

    reply = rpc_keinit_1(&rpc_keinit_1_arg, rpcHandle->dllClient);
    if (reply == (rpc_KEinit_reply *) NULL) {
        clnt_perror(rpcHandle->dllClient, "call failed");
    }

	*resultHandle = (Four)(reply->resultHandle);
	positionInBulkBuffer = 0;
	datasizeOfBulkBuffer = -1;

	return (Four)(reply->errCode);
}


Four lom_RPCKeywordExtractorNext (
	lom_FptrToGetNextPostingInfo	funcPtr,			/* IN */
	Four 							resultHandle, 		/* IN */
	char 							*keyword, 			/* OUT */
	Four 							*nPositions, 		/* OUT */
	char 							*positionList		/* OUT */
)
{
	Four					errCode;
	Four					strLen;
    rpc_KEnext_reply		*reply;
    rpc_KEnext_bulk_reply	*bulk_reply;
    rpc_KEnext_arg			rpc_kenext_1_arg;
	lom_dllFunc*			rpcHandle;

	rpcHandle = (lom_dllFunc*)funcPtr;

    rpc_kenext_1_arg.funcPtr = (Four)rpcHandle->dllFunc;
    rpc_kenext_1_arg.resultHandle = resultHandle;
	

	/*	
	 *	no-bulk 
	 */
	/*
    reply = rpc_kenext_1(&rpc_kenext_1_arg, dllClient);
    if (reply == (rpc_KEnext_reply *) NULL) {
        clnt_perror(dllClient, "call failed");
    }

	strcpy(keyword, (char *)(reply->keyword), strlen((char *)(reply->keyword)));
    *nPositions = reply->nPositions;
	memcpy(positionList, reply->positionList.positionList_val, reply->positionList.positionList_len);

	return (Four)(reply->errCode);
	*/


	/*	
	 *	bulk 
	 */
	if (positionInBulkBuffer == 0 || positionInBulkBuffer == datasizeOfBulkBuffer)
	{
    	bulk_reply = rpc_kenext_bulk_1(&rpc_kenext_1_arg, rpcHandle->dllClient);

		if (bulk_reply->data.data_len == 0)
		{
			return TEXT_DONE;
		}

		memcpy(bulkBuffer, bulk_reply->data.data_val, bulk_reply->data.data_len);
		datasizeOfBulkBuffer = bulk_reply->data.data_len;
		positionInBulkBuffer = 0;
	}
    memcpy(&errCode, &bulkBuffer[positionInBulkBuffer], sizeof(Four));
    positionInBulkBuffer += sizeof(Four);

    strLen = strlen(&bulkBuffer[positionInBulkBuffer]);
    memcpy(keyword, &bulkBuffer[positionInBulkBuffer], strLen);
    keyword[strLen] = '\0';
    positionInBulkBuffer += strLen+1;

    memcpy(nPositions, &bulkBuffer[positionInBulkBuffer], sizeof(Four));
    positionInBulkBuffer += sizeof(Four);

	memcpy(positionList, &bulkBuffer[positionInBulkBuffer], 2*sizeof(Four)*(*nPositions));
	positionInBulkBuffer += 2*sizeof(Four)*(*nPositions);

	return errCode;
}


Four lom_RPCKeywordExtractorFinal (
	lom_FptrToFinalizeKeywordExtraction	funcPtr,			/* IN */
	Four 								resultHandle)		/* IN */
{
    rpc_KEfinal_reply	*reply;
    rpc_KEfinal_arg		rpc_kefinal_1_arg;
	lom_dllFunc*		rpcHandle;

	rpcHandle = (lom_dllFunc*)funcPtr;

    rpc_kefinal_1_arg.funcPtr = (Four)rpcHandle->dllFunc;
    rpc_kefinal_1_arg.resultHandle = resultHandle;

    reply = rpc_kefinal_1(&rpc_kefinal_1_arg, rpcHandle->dllClient);
    if (reply == (rpc_KEfinal_reply *) NULL) {
        clnt_perror(rpcHandle->dllClient, "call failed");
    }

	return (Four)(reply->errCode);
}

#else /* SLIMDOWN_TEXTIR */

Four lom_RPCKeywordExtractorInit (
	lom_FptrToKeywordExtractor	funcPtr,			/* IN */
	Four 						locationOfContent,	/* IN */ 
	LOM_Handle 					*handle,			/* IN */ 
	Four 						volId, 				/* IN */
	char 						*className, 		/* IN */
	OID 						*oid, 				/* IN */
	Two 						colNo, 				/* IN */
	char 						*inFileName, 		/* IN */
	Four 						*resultHandle		/* OUT */
)
{
	return eTEXTIR_NOTENABLED_LOM;
} 


Four lom_RPCKeywordExtractorNext (
	lom_FptrToGetNextPostingInfo	funcPtr,			/* IN */
	Four 							resultHandle, 		/* IN */
	char 							*keyword, 			/* OUT */
	Four 							*nPositions, 		/* OUT */
	char 							*positionList		/* OUT */
)
{
	return eTEXTIR_NOTENABLED_LOM;
} 


Four lom_RPCKeywordExtractorFinal (
	lom_FptrToFinalizeKeywordExtraction	funcPtr,			/* IN */
	Four 								resultHandle)		/* IN */
{
	return eTEXTIR_NOTENABLED_LOM;
} 


#endif /* SLIMDOWN_TEXTIR */

#endif
