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

#ifndef _DLLSERVER_H_RPCGEN
#define	_DLLSERVER_H_RPCGEN

#include <rpc/rpc.h>
#define	MAXNAMELEN 256

typedef char *nametype;

typedef long CS_FOUR;

typedef u_long CS_UFOUR;

typedef short CS_TWO;

typedef u_short CS_UTWO;

typedef char CS_ONE;

typedef u_char CS_UONE;

struct CS_LOM_Handle {
	CS_FOUR serverInstanceId;
	CS_FOUR instanceId;
};
typedef struct CS_LOM_Handle CS_LOM_Handle;

struct CS_OID {
	CS_FOUR pageNo;
	CS_TWO volNo;
	CS_TWO slotNo;
	CS_UFOUR unique;
	CS_FOUR classID;
};
typedef struct CS_OID CS_OID;

struct rpc_dlopen_arg {
	nametype moduleName;
	CS_FOUR mode;
};
typedef struct rpc_dlopen_arg rpc_dlopen_arg;

struct rpc_dlopen_reply {
	CS_FOUR errCode;
	CS_FOUR handle;
};
typedef struct rpc_dlopen_reply rpc_dlopen_reply;

struct rpc_dlsym_arg {
	CS_FOUR handle;
	nametype funcName;
};
typedef struct rpc_dlsym_arg rpc_dlsym_arg;

struct rpc_dlsym_reply {
	CS_FOUR errCode;
	CS_FOUR funcPtr;
};
typedef struct rpc_dlsym_reply rpc_dlsym_reply;

struct rpc_dlclose_arg {
	CS_FOUR handle;
};
typedef struct rpc_dlclose_arg rpc_dlclose_arg;

struct rpc_dlclose_reply {
	CS_FOUR errCode;
};
typedef struct rpc_dlclose_reply rpc_dlclose_reply;

struct rpc_dlerror_arg {
	CS_FOUR dummy;
};
typedef struct rpc_dlerror_arg rpc_dlerror_arg;

struct rpc_dlerror_reply {
	CS_FOUR errCode;
	char *errMsg;
};
typedef struct rpc_dlerror_reply rpc_dlerror_reply;

struct rpc_KEinit_arg {
	CS_FOUR funcPtr;
	CS_FOUR locationOfContent;
	CS_LOM_Handle handle;
	CS_FOUR volId;
	nametype className;
	CS_OID oid;
	CS_TWO colNo;
	char *inFileName;
};
typedef struct rpc_KEinit_arg rpc_KEinit_arg;

struct rpc_KEinit_reply {
	CS_FOUR errCode;
	CS_FOUR resultHandle;
};
typedef struct rpc_KEinit_reply rpc_KEinit_reply;

struct rpc_KEnext_arg {
	CS_FOUR funcPtr;
	CS_FOUR resultHandle;
};
typedef struct rpc_KEnext_arg rpc_KEnext_arg;

struct rpc_KEnext_reply {
	CS_FOUR errCode;
	nametype keyword;
	CS_FOUR nPositions;
	struct {
		u_int positionList_len;
		char *positionList_val;
	} positionList;
};
typedef struct rpc_KEnext_reply rpc_KEnext_reply;

struct rpc_KEnext_bulk_reply {
	struct {
		u_int data_len;
		char *data_val;
	} data;
};
typedef struct rpc_KEnext_bulk_reply rpc_KEnext_bulk_reply;

struct rpc_KEfinal_arg {
	CS_FOUR funcPtr;
	CS_FOUR resultHandle;
};
typedef struct rpc_KEfinal_arg rpc_KEfinal_arg;

struct rpc_KEfinal_reply {
	CS_FOUR errCode;
};
typedef struct rpc_KEfinal_reply rpc_KEfinal_reply;

#define	DLLSERVERPROG ((unsigned long)(0x30000001))
#define	DLLSERVERVERS ((unsigned long)(1))
#define	RPC_DLOPEN ((unsigned long)(112))
extern  rpc_dlopen_reply * rpc_dlopen_1();
#define	RPC_DLSYM ((unsigned long)(113))
extern  rpc_dlsym_reply * rpc_dlsym_1();
#define	RPC_DLERROR ((unsigned long)(114))
extern  rpc_dlerror_reply * rpc_dlerror_1();
#define	RPC_DLCLOSE ((unsigned long)(115))
extern  rpc_dlclose_reply * rpc_dlclose_1();
#define	RPC_KEINIT ((unsigned long)(116))
extern  rpc_KEinit_reply * rpc_keinit_1();
#define	RPC_KENEXT ((unsigned long)(117))
extern  rpc_KEnext_reply * rpc_kenext_1();
#define	RPC_KENEXT_BULK ((unsigned long)(118))
extern  rpc_KEnext_bulk_reply * rpc_kenext_bulk_1();
#define	RPC_KEFINAL ((unsigned long)(119))
extern  rpc_KEfinal_reply * rpc_kefinal_1();
extern int dllserverprog_1_freeresult();

/* the xdr functions */
extern bool_t xdr_nametype();
extern bool_t xdr_CS_FOUR();
extern bool_t xdr_CS_UFOUR();
extern bool_t xdr_CS_TWO();
extern bool_t xdr_CS_UTWO();
extern bool_t xdr_CS_ONE();
extern bool_t xdr_CS_UONE();
extern bool_t xdr_CS_LOM_Handle();
extern bool_t xdr_CS_OID();
extern bool_t xdr_rpc_dlopen_arg();
extern bool_t xdr_rpc_dlopen_reply();
extern bool_t xdr_rpc_dlsym_arg();
extern bool_t xdr_rpc_dlsym_reply();
extern bool_t xdr_rpc_dlclose_arg();
extern bool_t xdr_rpc_dlclose_reply();
extern bool_t xdr_rpc_dlerror_arg();
extern bool_t xdr_rpc_dlerror_reply();
extern bool_t xdr_rpc_KEinit_arg();
extern bool_t xdr_rpc_KEinit_reply();
extern bool_t xdr_rpc_KEnext_arg();
extern bool_t xdr_rpc_KEnext_reply();
extern bool_t xdr_rpc_KEnext_bulk_reply();
extern bool_t xdr_rpc_KEfinal_arg();
extern bool_t xdr_rpc_KEfinal_reply();

#endif /* !_DLLSERVER_H_RPCGEN */
