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

#include "dllbroker.h"
#include "dllserver.h"
#include <stdio.h>
#include <stdlib.h> /* getenv, exit */
#include <dlfcn.h>
#include <malloc.h>
#include "cosmos_r.h"
#include "LOM_Internal.h"
#include "LOM.h"
#include "Catalog.h"
#include "Catalog_Internal.h"


#define MAXCOMMANDLENGTH	1000
#define MAXCONNECTTRIAL		1000	

typedef struct {
	long	sentence;
	long	noun;
} Position;



void
dllserverprog_1(host)
	char *host;
{
	int		i, j;
	int		pos;
	int		errCode;
	int		count;
	char	keyword[LOM_MAXKEYWORDSIZE];
	char	*bulkBuffer;
	int		strLen;

    char    *broker_path;
    char    command[MAXCOMMANDLENGTH];
    char    *broker_name = "dllbroker";

	char	initfunc[100] = "openAndExecuteKeywordExtractor";
	char	nextfunc[100] = "getAndNextKeywordExtractor";
	char	finalfunc[100] = "closeKeywordExtractor";

    CLIENT                      *broker_clnt;
    dllbroker_connect_reply     *result_0;
    dllbroker_connect_arg       dllbroker_connect_1_arg;

	char						buffer[100000];
	void						*initFptr;
	void						*nextFptr;
	void						*finalFptr;
	Position					position;

	CLIENT *clnt;
	rpc_dlopen_reply  *result_1;
	rpc_dlopen_arg  rpc_dlopen_1_arg;
	rpc_dlsym_reply  *result_2;
	rpc_dlsym_arg  rpc_dlsym_1_arg;
	rpc_dlerror_reply  *result_3;
	char *  rpc_dlerror_1_arg;
	rpc_dlclose_reply  *result_4;
	rpc_dlclose_arg  rpc_dlclose_1_arg;
	rpc_KEinit_reply  *result_5;
	rpc_KEinit_arg  rpc_keinit_1_arg;
	rpc_KEnext_reply  *result_6;
	rpc_KEnext_arg  rpc_kenext_1_arg;
	rpc_KEfinal_reply  *result_7;
	rpc_KEfinal_arg  rpc_kefinal_1_arg;

	rpc_KEnext_bulk_reply  *result_bulk;

	void	*dlhandle;


    /* launch dllserver by request to dllbroker */
    broker_clnt = clnt_create("localhost", DLLBROKERPROG, DLLBROKERVERS, "tcp");
    if (broker_clnt == (CLIENT *) NULL) {

	    /* get an enviroment variable */
   		broker_path = getenv("OOSQL_DLLBROKER_PATH");
    	if(broker_path == NULL) {
			fprintf(stderr, "OOSQL_DLLBROKER_PATH is not defined\n");
    	}
   
#ifndef WIN32
    	sprintf(command,"%s/%s &", broker_path, broker_name);
#else
    	sprintf(command,"start %s\\%s", broker_path, broker_name);
#endif
   
    	system(command);
    	broker_clnt = clnt_create("localhost", DLLBROKERPROG, DLLBROKERVERS, "tcp");

    }

    dllbroker_connect_1_arg.protocol = ODYS_TCP | ODYS_UDP;
    result_0 = dllbroker_connect_1(&dllbroker_connect_1_arg, broker_clnt);
    if (result_0 == (dllbroker_connect_reply *) NULL) {
        clnt_perror(broker_clnt, "dllbroker_connect_1() call failed");
    }

#ifndef	DEBUG
	for (i = 0; i < 10; i++)
	{
		clnt = clnt_create(host, result_0->serverNo, DLLSERVERVERS, "tcp");
		if (clnt == (CLIENT *) NULL) {
		}
	}
#endif	/* DEBUG */



	rpc_dlopen_1_arg.moduleName = "/test-temp/mskim/OOSQL/bin/morph.so";
	rpc_dlopen_1_arg.mode = RTLD_LAZY; 
	result_1 = rpc_dlopen_1(&rpc_dlopen_1_arg, clnt);
	if (result_1 == (rpc_dlopen_reply *) NULL) {
		clnt_perror(clnt, "call failed");
	}
	printf("dlopen result.handle = %ld\n", result_1->handle);
	dlhandle = (void*)(result_1->handle);

	
	rpc_dlsym_1_arg.handle = (CS_FOUR)dlhandle; 
	rpc_dlsym_1_arg.funcName = initfunc; 
	result_2 = rpc_dlsym_1(&rpc_dlsym_1_arg, clnt);
	if (result_2 == (rpc_dlsym_reply *) NULL) {
		clnt_perror(clnt, "call failed");
	}
	initFptr = (void*)(result_2->funcPtr);
	printf("dlsym init funcptr = %ld, errcode = %ld\n", result_2->funcPtr, result_2->errCode);

	rpc_dlsym_1_arg.handle = (CS_FOUR)dlhandle; 
	rpc_dlsym_1_arg.funcName = nextfunc; 
	result_2 = rpc_dlsym_1(&rpc_dlsym_1_arg, clnt);
	if (result_2 == (rpc_dlsym_reply *) NULL) {
		clnt_perror(clnt, "call failed");
	}
	nextFptr = (void*)(result_2->funcPtr);
	printf("dlsym next funcptr = %ld, errcode = %ld\n", result_2->funcPtr, result_2->errCode);

	rpc_dlsym_1_arg.handle = (CS_FOUR)dlhandle; 
	rpc_dlsym_1_arg.funcName = finalfunc; 
	result_2 = rpc_dlsym_1(&rpc_dlsym_1_arg, clnt);
	if (result_2 == (rpc_dlsym_reply *) NULL) {
		clnt_perror(clnt, "call failed");
	}
	finalFptr = (void*)(result_2->funcPtr);
	printf("dlsym final funcptr = %ld, errcode = %ld\n", result_2->funcPtr, result_2->errCode);

	for (i = 0; i < 8000; i++)
	{
		j = rand() % 7;
		if (j == 0 || i % 50 == 0) 
			buffer[i] = ' '; 
		else
			buffer[i] = rand() % 26 + 'a'; 
	}
	buffer[i] = '\0';

    rpc_keinit_1_arg.funcPtr = (CS_FOUR)initFptr; 
    rpc_keinit_1_arg.locationOfContent = 2;
    rpc_keinit_1_arg.volId = 0;
    rpc_keinit_1_arg.className = NULL;
    rpc_keinit_1_arg.colNo = -1;
    rpc_keinit_1_arg.inFileName = buffer;

    rpc_keinit_1_arg.handle.serverInstanceId = 0;
    rpc_keinit_1_arg.handle.instanceId = 0;

    rpc_keinit_1_arg.oid.pageNo = 0;
    rpc_keinit_1_arg.oid.volNo = 0;
    rpc_keinit_1_arg.oid.slotNo = 0;
    rpc_keinit_1_arg.oid.unique = 0;
    rpc_keinit_1_arg.oid.classID = 0;

	printf("rpc_keinit_1_arg size = %ld\n", sizeof(rpc_keinit_1_arg)+strlen(buffer));

	result_5 = rpc_keinit_1(&rpc_keinit_1_arg, clnt);
	if (result_5 == (rpc_KEinit_reply *) NULL) {
		clnt_perror(clnt, "call failed");
	}
	printf("keinit result.errCode = %ld\n", result_5->errCode);


	rpc_kenext_1_arg.funcPtr = (CS_FOUR)nextFptr; 
	rpc_kenext_1_arg.resultHandle = result_5->resultHandle;
	result_bulk = rpc_kenext_bulk_1(&rpc_kenext_1_arg, clnt);
	while (result_bulk->data.data_len != 0)
	{
	pos = 0;
	bulkBuffer=	result_bulk->data.data_val;
	while (pos < result_bulk->data.data_len)
	{
		memcpy(&errCode, &bulkBuffer[pos], sizeof(long));
		pos += sizeof(long);

		strLen = strlen(&bulkBuffer[pos]);
		memcpy(keyword, &bulkBuffer[pos], strLen);
		keyword[strLen] = '\0';
		pos += strLen+1; 
		printf("kenext keyword = %s\n", keyword);

		memcpy(&count, &bulkBuffer[pos], sizeof(long));
		pos += sizeof(long);

		printf("kenext nPositions = %ld\n", count);
		for (i = 0; i < count; i++)
		{
			memcpy(&position, &bulkBuffer[pos+i*sizeof(Position)], sizeof(Position));
			printf("(%ld, %ld) ", position.sentence, position.noun);
		}
		printf("\n");
		pos += 2*sizeof(Four)*count;
	}
		result_bulk = rpc_kenext_bulk_1(&rpc_kenext_1_arg, clnt);
	}


	rpc_kefinal_1_arg.funcPtr = (CS_FOUR)finalFptr;
	rpc_kefinal_1_arg.resultHandle = result_5->resultHandle; 
	result_7 = rpc_kefinal_1(&rpc_kefinal_1_arg, clnt);
	if (result_7 == (rpc_KEfinal_reply *) NULL) {
		clnt_perror(clnt, "call failed");
	}
	printf("kefinal result.errCode = %ld\n", result_7->errCode);

	rpc_dlclose_1_arg.handle = (CS_FOUR)dlhandle;
	result_4 = rpc_dlclose_1(&rpc_dlclose_1_arg, clnt);
	if (result_4 == (rpc_dlclose_reply *) NULL) {
		clnt_perror(clnt, "call failed");
	}
	printf("dlclose result.errCode = %ld\n", result_4->errCode);

#ifndef	DEBUG
	clnt_destroy(clnt);
#endif		/* DEBUG */
}


void
dllserverprog_2(host)
	char *host;
{
	int		i, j;
    char    *broker_path;
    char    command[MAXCOMMANDLENGTH];
    char    *broker_name = "dllbroker";

	char	initfunc[100] = "EngKeywordExtractor_Init";
	char	nextfunc[100] = "EngKeywordExtractor_Next";
	char	finalfunc[100] = "EngKeywordExtractor_Final";

    CLIENT                      *broker_clnt;
    dllbroker_connect_reply     *result_0;
    dllbroker_connect_arg       dllbroker_connect_1_arg;

    char                        buffer[1000] = "1999";
    void                        *initFptr;
    void                        *nextFptr;
    void                        *finalFptr;
    Position                    position;

	CLIENT *clnt;
	rpc_dlopen_reply  *result_1;
	rpc_dlopen_arg  rpc_dlopen_1_arg;
	rpc_dlsym_reply  *result_2;
	rpc_dlsym_arg  rpc_dlsym_1_arg;
	rpc_dlerror_reply  *result_3;
	char *  rpc_dlerror_1_arg;
	rpc_dlclose_reply  *result_4;
	rpc_dlclose_arg  rpc_dlclose_1_arg;
	rpc_KEinit_reply  *result_5;
	rpc_KEinit_arg  rpc_keinit_1_arg;
	rpc_KEnext_reply  *result_6;
	rpc_KEnext_arg  rpc_kenext_1_arg;
	rpc_KEfinal_reply  *result_7;
	rpc_KEfinal_arg  rpc_kefinal_1_arg;


	void	*dlhandle;


    /* launch dllserver by request to dllbroker */
    broker_clnt = clnt_create("localhost", DLLBROKERPROG, DLLBROKERVERS, "tcp");
    if (broker_clnt == (CLIENT *) NULL) {

	    /* get an enviroment variable */
   		broker_path = getenv("OOSQL_DLLBROKER_PATH");
    	if(broker_path == NULL) {
			fprintf(stderr, "OOSQL_DLLBROKER_PATH is not defined\n");
    	}
   
#ifndef WIN32
    	sprintf(command,"%s/%s &", broker_path, broker_name);
#else
    	sprintf(command,"start %s\\%s", broker_path, broker_name);
#endif
   
    	system(command);
    	broker_clnt = clnt_create("localhost", DLLBROKERPROG, DLLBROKERVERS, "tcp");

    }

    dllbroker_connect_1_arg.protocol = ODYS_TCP | ODYS_UDP;
    result_0 = dllbroker_connect_1(&dllbroker_connect_1_arg, broker_clnt);
    if (result_0 == (dllbroker_connect_reply *) NULL) {
        clnt_perror(broker_clnt, "dllbroker_connect_1() call failed");
    }

#ifndef	DEBUG
	clnt = clnt_create(host, result_0->serverNo, DLLSERVERVERS, "tcp");
	if (clnt == (CLIENT *) NULL) {
		clnt_pcreateerror(host);
		exit(1);
	}
#endif	/* DEBUG */

	rpc_dlopen_1_arg.moduleName = "/test-temp/mskim/OOSQL/bin/EngKeywordExtractor.so";
	rpc_dlopen_1_arg.mode = RTLD_LAZY; 
	result_1 = rpc_dlopen_1(&rpc_dlopen_1_arg, clnt);
	if (result_1 == (rpc_dlopen_reply *) NULL) {
		clnt_perror(clnt, "call failed");
	}
	printf("dlopen result.handle = %ld\n", result_1->handle);
	dlhandle = (void*)(result_1->handle);

	
	rpc_dlsym_1_arg.handle = (CS_FOUR)dlhandle; 
	rpc_dlsym_1_arg.funcName = initfunc; 
	result_2 = rpc_dlsym_1(&rpc_dlsym_1_arg, clnt);
	if (result_2 == (rpc_dlsym_reply *) NULL) {
		clnt_perror(clnt, "call failed");
	}
    initFptr = (void*)(result_2->funcPtr);
	printf("dlsym init funcptr = %ld, errcode = %ld\n", result_2->funcPtr, result_2->errCode);

	rpc_dlsym_1_arg.handle = (CS_FOUR)dlhandle; 
	rpc_dlsym_1_arg.funcName = nextfunc; 
	result_2 = rpc_dlsym_1(&rpc_dlsym_1_arg, clnt);
	if (result_2 == (rpc_dlsym_reply *) NULL) {
		clnt_perror(clnt, "call failed");
	}
    nextFptr = (void*)(result_2->funcPtr);
	printf("dlsym next funcptr = %ld, errcode = %ld\n", result_2->funcPtr, result_2->errCode);

	rpc_dlsym_1_arg.handle = (CS_FOUR)dlhandle; 
	rpc_dlsym_1_arg.funcName = finalfunc; 
	result_2 = rpc_dlsym_1(&rpc_dlsym_1_arg, clnt);
	if (result_2 == (rpc_dlsym_reply *) NULL) {
		clnt_perror(clnt, "call failed");
	}
    finalFptr = (void*)(result_2->funcPtr);
	printf("dlsym final funcptr = %ld, errcode = %ld\n", result_2->funcPtr, result_2->errCode);

    rpc_keinit_1_arg.funcPtr = (CS_FOUR)initFptr;
    rpc_keinit_1_arg.locationOfContent = 2;
    rpc_keinit_1_arg.volId = 0;
    rpc_keinit_1_arg.className = NULL;
    rpc_keinit_1_arg.colNo = -1;
    rpc_keinit_1_arg.inFileName = buffer;

    rpc_keinit_1_arg.handle.serverInstanceId = 0;
    rpc_keinit_1_arg.handle.instanceId = 0;

    rpc_keinit_1_arg.oid.pageNo = 0;
    rpc_keinit_1_arg.oid.volNo = 0;
    rpc_keinit_1_arg.oid.slotNo = 0;
    rpc_keinit_1_arg.oid.unique = 0;
    rpc_keinit_1_arg.oid.classID = 0;

	result_5 = rpc_keinit_1(&rpc_keinit_1_arg, clnt);
	if (result_5 == (rpc_KEinit_reply *) NULL) {
		clnt_perror(clnt, "call failed");
	}
    printf("keinit result.errCode = %ld\n", result_5->errCode);

    rpc_kenext_1_arg.funcPtr = (CS_FOUR)nextFptr;
    rpc_kenext_1_arg.resultHandle = result_5->resultHandle;
	result_6 = rpc_kenext_1(&rpc_kenext_1_arg, clnt);
    while (result_6->errCode != TEXT_DONE)
    {
    printf("kenext keyword = %s\n", result_6->keyword);
    printf("kenext nPositions = %ld\n", result_6->nPositions);
    for (i = 0; i < result_6->nPositions; i++)
    {
        memcpy(&position, &(result_6->positionList.positionList_val)[i*sizeof(Position)], sizeof(Position));
        printf("kenext positionList[%ld] = (%ld, %ld)\n", i, position.sentence, position.noun);
    }
    result_6 = rpc_kenext_1(&rpc_kenext_1_arg, clnt);
    }

    rpc_kefinal_1_arg.funcPtr = (CS_FOUR)finalFptr;
    rpc_kefinal_1_arg.resultHandle = result_5->resultHandle;
	result_7 = rpc_kefinal_1(&rpc_kefinal_1_arg, clnt);
	if (result_7 == (rpc_KEfinal_reply *) NULL) {
		clnt_perror(clnt, "call failed");
	}
    printf("kefinal result.errCode = %ld\n", result_7->errCode);

	rpc_dlclose_1_arg.handle = (CS_FOUR)dlhandle;
	result_4 = rpc_dlclose_1(&rpc_dlclose_1_arg, clnt);
	if (result_4 == (rpc_dlclose_reply *) NULL) {
		clnt_perror(clnt, "call failed");
	}
	printf("dlclose result.errCode = %ld\n", result_4->errCode);

#ifndef	DEBUG
	clnt_destroy(clnt);
#endif		/* DEBUG */
}


main(argc, argv)
	int argc;
	char *argv[];
{
	char *host;

	if (argc < 2) {
		printf("usage:  %s server_host\n", argv[0]);
		exit(1);
	}
	host = argv[1];
	dllserverprog_1(host);
}
