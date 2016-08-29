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
 * Please do not edit this file.
 * It was generated using rpcgen.
 */

#include "dllserver.h"
#include <stdio.h>
#include <stdlib.h> /* getenv, exit */
#include <signal.h>
#include <sys/types.h>
#include <memory.h>
#include <stropts.h>
#include <netconfig.h>
#include <sys/resource.h> /* rlimit */
#include <syslog.h>

#ifdef DEBUG
#define	RPC_SVC_FG
#endif

#define	_RPCSVC_CLOSEDOWN 120
static int _rpcpmstart;		/* Started by a port monitor ? */

/* States a server can be in wrt request */

#define	_IDLE 0
#define	_SERVED 1

static int _rpcsvcstate = _IDLE;	/* Set when a request is serviced */
static int _rpcsvccount = 0;		/* Number of requests being serviced */

static
void _msgout(msg)
	char *msg;
{
#ifdef RPC_SVC_FG
	if (_rpcpmstart)
		syslog(LOG_ERR, msg);
	else
		(void) fprintf(stderr, "%s\n", msg);
#else
	syslog(LOG_ERR, msg);
#endif
}

static void
closedown(sig)
	int sig;
{
	if (_rpcsvcstate == _IDLE && _rpcsvccount == 0) {
		extern fd_set svc_fdset;
		static int size;
		int i, openfd;
		struct t_info tinfo;

		if (!t_getinfo(0, &tinfo) && (tinfo.servtype == T_CLTS))
			exit(0);
		if (size == 0) {
			struct rlimit rl;

			rl.rlim_max = 0;
			getrlimit(RLIMIT_NOFILE, &rl);
			if ((size = rl.rlim_max) == 0) {
				return;
			}
		}
		for (i = 0, openfd = 0; i < size && openfd < 2; i++)
			if (FD_ISSET(i, &svc_fdset))
				openfd++;
		if (openfd <= 1)
			exit(0);
	} else
		_rpcsvcstate = _IDLE;

	(void) signal(SIGALRM, (void(*)()) closedown);
	(void) alarm(_RPCSVC_CLOSEDOWN/2);
}

static void
dllserverprog_1(rqstp, transp)
	struct svc_req *rqstp;
	register SVCXPRT *transp;
{
	union {
		rpc_dlopen_arg rpc_dlopen_1_arg;
		rpc_dlsym_arg rpc_dlsym_1_arg;
		rpc_dlerror_arg rpc_dlerror_1_arg;
		rpc_dlclose_arg rpc_dlclose_1_arg;
		rpc_KEinit_arg rpc_keinit_1_arg;
		rpc_KEnext_arg rpc_kenext_1_arg;
		rpc_KEnext_arg rpc_kenext_bulk_1_arg;
		rpc_KEfinal_arg rpc_kefinal_1_arg;
	} argument;
	char *result;
	bool_t (*_xdr_argument)(), (*_xdr_result)();
	char *(*local)();

	_rpcsvccount++;
	switch (rqstp->rq_proc) {
	case NULLPROC:
		(void) svc_sendreply(transp, xdr_void,
			(char *)NULL);
		_rpcsvccount--;
		_rpcsvcstate = _SERVED;
		return;

	case RPC_DLOPEN:
		_xdr_argument = xdr_rpc_dlopen_arg;
		_xdr_result = xdr_rpc_dlopen_reply;
		local = (char *(*)()) rpc_dlopen_1;
		break;

	case RPC_DLSYM:
		_xdr_argument = xdr_rpc_dlsym_arg;
		_xdr_result = xdr_rpc_dlsym_reply;
		local = (char *(*)()) rpc_dlsym_1;
		break;

	case RPC_DLERROR:
		_xdr_argument = xdr_rpc_dlerror_arg;
		_xdr_result = xdr_rpc_dlerror_reply;
		local = (char *(*)()) rpc_dlerror_1;
		break;

	case RPC_DLCLOSE:
		_xdr_argument = xdr_rpc_dlclose_arg;
		_xdr_result = xdr_rpc_dlclose_reply;
		local = (char *(*)()) rpc_dlclose_1;
		break;

	case RPC_KEINIT:
		_xdr_argument = xdr_rpc_KEinit_arg;
		_xdr_result = xdr_rpc_KEinit_reply;
		local = (char *(*)()) rpc_keinit_1;
		break;

	case RPC_KENEXT:
		_xdr_argument = xdr_rpc_KEnext_arg;
		_xdr_result = xdr_rpc_KEnext_reply;
		local = (char *(*)()) rpc_kenext_1;
		break;

	case RPC_KENEXT_BULK:
		_xdr_argument = xdr_rpc_KEnext_arg;
		_xdr_result = xdr_rpc_KEnext_bulk_reply;
		local = (char *(*)()) rpc_kenext_bulk_1;
		break;

	case RPC_KEFINAL:
		_xdr_argument = xdr_rpc_KEfinal_arg;
		_xdr_result = xdr_rpc_KEfinal_reply;
		local = (char *(*)()) rpc_kefinal_1;
		break;

	default:
		svcerr_noproc(transp);
		_rpcsvccount--;
		_rpcsvcstate = _SERVED;
		return;
	}
	(void) memset((char *)&argument, 0, sizeof (argument));
	if (!svc_getargs(transp, _xdr_argument, (caddr_t) &argument)) {
		svcerr_decode(transp);
		_rpcsvccount--;
		_rpcsvcstate = _SERVED;
		return;
	}
	result = (*local)(&argument, rqstp);
	if (result != NULL && !svc_sendreply(transp, _xdr_result, result)) {
		svcerr_systemerr(transp);
	}
	if (!svc_freeargs(transp, _xdr_argument, (caddr_t) &argument)) {
		_msgout("unable to free arguments");
		exit(1);
	}
	_rpcsvccount--;
	_rpcsvcstate = _SERVED;
	return;
}

main()
{
	pid_t pid;
	int i;

	(void) sigset(SIGPIPE, SIG_IGN);

	/*
	 * If stdin looks like a TLI endpoint, we assume
	 * that we were started by a port monitor. If
	 * t_getstate fails with TBADF, this is not a
	 * TLI endpoint.
	 */
	if (t_getstate(0) != -1 || t_errno != TBADF) {
		char *netid;
		struct netconfig *nconf = NULL;
		SVCXPRT *transp;
		int pmclose;

		_rpcpmstart = 1;
		openlog("dllserver", LOG_PID, LOG_DAEMON);

		if ((netid = getenv("NLSPROVIDER")) == NULL) {
		/* started from inetd */
			pmclose = 1;
		} else {
			if ((nconf = getnetconfigent(netid)) == NULL)
				_msgout("cannot get transport info");

			pmclose = (t_getstate(0) != T_DATAXFER);
		}
		if ((transp = svc_tli_create(0, nconf, NULL, 0, 0)) == NULL) {
			_msgout("cannot create server handle");
			exit(1);
		}
		if (nconf)
			freenetconfigent(nconf);
		if (!svc_reg(transp, DLLSERVERPROG, DLLSERVERVERS, dllserverprog_1, 0)) {
			_msgout("unable to register (DLLSERVERPROG, DLLSERVERVERS).");
			exit(1);
		}
		if (pmclose) {
			(void) signal(SIGALRM, (void(*)()) closedown);
			(void) alarm(_RPCSVC_CLOSEDOWN/2);
		}
		svc_run();
		exit(1);
		/* NOTREACHED */
	}	else {
#ifndef RPC_SVC_FG
		int size;
		struct rlimit rl;
		pid = fork();
		if (pid < 0) {
			perror("cannot fork");
			exit(1);
		}
		if (pid)
			exit(0);
		rl.rlim_max = 0;
		getrlimit(RLIMIT_NOFILE, &rl);
		if ((size = rl.rlim_max) == 0)
			exit(1);
		for (i = 0; i < size; i++)
			(void) close(i);
		i = open("/dev/null", 2);
		(void) dup2(i, 1);
		(void) dup2(i, 2);
		setsid();
		openlog("dllserver", LOG_PID, LOG_DAEMON);
#endif
	}
	if (!svc_create(dllserverprog_1, DLLSERVERPROG, DLLSERVERVERS, "netpath")) {
		_msgout("unable to create (DLLSERVERPROG, DLLSERVERVERS) for netpath.");
		exit(1);
	}

	svc_run();
	_msgout("svc_run returned");
	exit(1);
	/* NOTREACHED */
}
