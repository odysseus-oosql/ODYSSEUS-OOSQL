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
#include <signal.h>
#include <stdlib.h>
#include <errno.h>
#include <rpc/types.h>
#include <rpc/rpc.h>
#include <rpc/xdr.h>
#include "DLLBroker_Internal.h"
#include "DLLBroker_Err.h"
#include "dllbroker.h"
#ifndef eNOERROR
#define eNOERROR	0
#endif

#define	MAXPATHLENGTH		1256
#define	MAXCOMMANDLENGTH	1256
#define	MAXHOSTNAME			256



dllbroker_connect_reply *
dllbroker_connect_1(argp, rqstp)
    dllbroker_connect_arg *argp;
    struct svc_req *rqstp;
{
	char	*odys_path;
	char	path[MAXPATHLENGTH];
	char	log_path[MAXPATHLENGTH];
	char	command[MAXCOMMANDLENGTH];
	char	*server_path= "dllserver";
	char	*server_name = "dllserver";
	char	*serverIPAddress;
	static	serverNo = 0x30000001;
	static  dllbroker_connect_reply replyBuf;
	FILE 	*filehandle;

	char	*debugFilePath;
	char	debugFile[MAXPATHLENGTH];
	FILE	*debugFP;
	int		result;


#ifdef CS_DEBUG
	fprintf(stderr, "DLLBroker_Connect()\n");
#endif
	/* get an enviroment variable */
	odys_path = getenv("OOSQL_DLLSERVER_PATH");
	if(odys_path == NULL) {
		DLLBROKER_ERROR(replyBuf, eCONFIGURATION_BROKER);
	}

#ifndef WIN32
	sprintf(path,"%s",odys_path);
#else
	sprintf(path,"%s",odys_path);
#endif

#ifdef DEBUG
#ifndef WIN32
	sprintf(log_path, "%s/log/serverlog", odys_path);
#else
	sprintf(log_path, "%s\\log\\serverlog", odys_path);
#endif

	filehandle = Util_fopen(log_path,"a+");
	Util_fputs("Broker_Connect is called\n", filehandle);
	Util_fclose(filehandle);
#endif

#ifndef WIN32
	sprintf(command,"%s/%s %ld&", path, server_name, ++serverNo);
#else
	sprintf(command,"start %s\\%s %ld", path, server_name, ++serverNo);
#endif

	if(serverNo == 0x40000000) serverNo = 0x30000001;

	
	debugFilePath = getenv("OOSQL_DLLBROKER_PATH");
	result = system(command);

	replyBuf.errCode = eNOERROR;
	replyBuf.serverNo = serverNo;

	return &replyBuf;
}

dllbroker_disconnect_reply *
dllbroker_disconnect_1(argp, rqstp)
    dllbroker_disconnect_arg *argp;
    struct svc_req *rqstp;
{
	static dllbroker_disconnect_reply replyBuf;

#ifdef CS_DEBUG
	fprintf(stderr, "DLLBroker_Disconnect(serverNo : %ld)\n", arg->serverNo);
#endif

	replyBuf.errCode = eNOERROR;
	return &replyBuf;
}
