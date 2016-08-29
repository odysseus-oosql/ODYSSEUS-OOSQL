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

/*
 * This is sample code generated by rpcgen.
 * These are only templates and you can use them
 * as a guideline for developing your own functions.
 */

#include <stdio.h>
#include <stdlib.h> /* getenv, exit */
#include <signal.h>
#include <dlfcn.h>
#include "cosmos_r.h"
#include "LOM_Internal.h"
#include "LOM.h"
#include "dllserver.h"

Four serverNo;

int  incompleteFlag = 0;
int  lastbufferFlag = 0;
Four errCode;
Four nPositions;
char keyword[LOM_MAXKEYWORDSIZE];
char postingBuf[LOM_DEFAULTPOSTINGBUFFERSIZE];

char bulkBuffer[LOM_RPCKE_BULKBUFSIZE];


rpc_main(int);


rpc_dlopen_reply *
rpc_dlopen_1(argp, rqstp)
	rpc_dlopen_arg *argp;
	struct svc_req *rqstp;
{
	static rpc_dlopen_reply  result;
    void *p;

    /*
     * insert server code here
     */
    p = dlopen(argp->moduleName, (int)argp->mode);

    result.errCode = 0;
    result.handle = (CS_FOUR)p;

    return (&result);
}

rpc_dlsym_reply *
rpc_dlsym_1(argp, rqstp)
	rpc_dlsym_arg *argp;
	struct svc_req *rqstp;
{
	static rpc_dlsym_reply  result;
    void *p;
   
    /*
     * insert server code here
     */
    p = dlsym((void *)(argp->handle), (char *)(argp->funcName));
   
    result.errCode = 0;
    result.funcPtr = (CS_FOUR)p;

	return (&result);
}

rpc_dlerror_reply *
rpc_dlerror_1(argp, rqstp)
    void *argp;
    struct svc_req *rqstp;
{
	static rpc_dlerror_reply  result;

    /*
     * insert server code here
     */
    result.errMsg = dlerror();
    result.errCode = 0;

	return (&result);
}

rpc_dlclose_reply *
rpc_dlclose_1(argp, rqstp)
	rpc_dlclose_arg *argp;
	struct svc_req *rqstp;
{
	static rpc_dlclose_reply  result;

    /*
     * insert server code here
     */
    result.errCode = dlclose((void *)(argp->handle));
	
	return (&result);
}

rpc_KEinit_reply *
rpc_keinit_1(argp, rqstp)
	rpc_KEinit_arg *argp;
	struct svc_req *rqstp;
{
	static rpc_KEinit_reply  result;
    lom_KeywordExtractorFunc  funcPtr;

    /*
     * insert server code here
     */
    funcPtr = (lom_KeywordExtractorFunc)(argp->funcPtr);
    result.errCode = (*funcPtr)(argp->locationOfContent, (LOM_Handle*)(&(argp->handle)), argp->volId,
                                (char *)(argp->className), (OID*)(&(argp->oid)), argp->colNo,
                                (char *)(argp->inFileName), &(result.resultHandle));
	incompleteFlag = 0;
	lastbufferFlag = 0;

	return (&result);
}

rpc_KEnext_reply *
rpc_kenext_1(argp, rqstp)
	rpc_KEnext_arg *argp;
	struct svc_req *rqstp;
{
	static rpc_KEnext_reply  result;
    lom_GetNextPostingInfoFunc    funcPtr;


    /*
     * insert server code here
     */

    funcPtr = (lom_GetNextPostingInfoFunc)(argp->funcPtr);
    result.errCode = (*funcPtr)(argp->resultHandle, keyword, &nPositions, postingBuf);

	if (result.errCode != TEXT_DONE)
	{
    	result.keyword = keyword;
    	result.nPositions = nPositions;
    	result.positionList.positionList_len = 2 * sizeof(long) * nPositions;
    	result.positionList.positionList_val = postingBuf;
	}

	return (&result);
}

rpc_KEnext_bulk_reply *
rpc_kenext_bulk_1(argp, rqstp)
    rpc_KEnext_arg *argp;
    struct svc_req *rqstp;
{
    static rpc_KEnext_bulk_reply  result;
    lom_GetNextPostingInfoFunc    funcPtr;
	Four	pos = 0;

    /*
     * insert server code here
     */

	if (lastbufferFlag == 1)
	{
		result.data.data_len = 0;
		result.data.data_val = bulkBuffer;

    	return (&result);
	}


	if (incompleteFlag == 1)
	{
		memcpy(&bulkBuffer[pos], &errCode, sizeof(Four));
		pos += sizeof(Four);

		memcpy(&bulkBuffer[pos], keyword, strlen(keyword));
		pos += strlen(keyword);
		bulkBuffer[pos++] = '\0';

		memcpy(&bulkBuffer[pos], &nPositions, sizeof(Four));
		pos += sizeof(Four);
		
		memcpy(&bulkBuffer[pos], postingBuf, 2*sizeof(Four)*nPositions);
		pos += 2*sizeof(Four)*nPositions;

		incompleteFlag = 0;
	}

    funcPtr = (lom_GetNextPostingInfoFunc)(argp->funcPtr);
    while ((errCode = (*funcPtr)(argp->resultHandle, keyword, &nPositions, postingBuf)) != TEXT_DONE)
	{
		if ((pos + sizeof(Four) + strlen(keyword) + 1 + sizeof(Four) + 2*sizeof(Four)*nPositions)  > LOM_RPCKE_BULKBUFSIZE)
		{
			incompleteFlag = 1;
			break;
		}

		memcpy(&bulkBuffer[pos], &errCode, sizeof(Four));
		pos += sizeof(Four);

		memcpy(&bulkBuffer[pos], keyword, strlen(keyword));
		pos += strlen(keyword);
		bulkBuffer[pos++] = '\0';

		memcpy(&bulkBuffer[pos], &nPositions, sizeof(Four));
		pos += sizeof(Four);
		
		memcpy(&bulkBuffer[pos], postingBuf, 2*sizeof(Four)*nPositions);
		pos += 2*sizeof(Four)*nPositions;
	}

	if (incompleteFlag == 0)
		lastbufferFlag = 1;
	else
		lastbufferFlag = 0;


	result.data.data_len = pos;
	result.data.data_val = bulkBuffer;

    return (&result);
}


rpc_KEfinal_reply *
rpc_kefinal_1(argp, rqstp)
	rpc_KEfinal_arg *argp;
	struct svc_req *rqstp;
{
	static rpc_KEfinal_reply  result;
    lom_FinalizeKeywordExtractionFunc funcPtr;

    /*
     * insert server code here
     */
    funcPtr = (lom_FinalizeKeywordExtractionFunc)(argp->funcPtr);
    result.errCode = (*funcPtr)(argp->resultHandle);

	return (&result);
}


int main(int argc, char **argv)
{
	if(argc != 2) {
		fprintf(stderr,"%s <server_prognum>\n", argv[0]);
		exit(1);
	}

	fprintf(stderr, "Server Processor ID : %ld\n", getpid());

	serverNo = atoi(argv[1]);

	rpc_main(serverNo);
}



