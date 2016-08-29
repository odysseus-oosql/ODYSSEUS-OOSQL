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

bool_t
xdr_nametype(xdrs, objp)
	register XDR *xdrs;
	nametype *objp;
{

	register long *buf;

	if (!xdr_string(xdrs, objp, MAXNAMELEN))
		return (FALSE);
	return (TRUE);
}

bool_t
xdr_CS_FOUR(xdrs, objp)
	register XDR *xdrs;
	CS_FOUR *objp;
{

	register long *buf;

	if (!xdr_long(xdrs, objp))
		return (FALSE);
	return (TRUE);
}

bool_t
xdr_CS_UFOUR(xdrs, objp)
	register XDR *xdrs;
	CS_UFOUR *objp;
{

	register long *buf;

	if (!xdr_u_long(xdrs, objp))
		return (FALSE);
	return (TRUE);
}

bool_t
xdr_CS_TWO(xdrs, objp)
	register XDR *xdrs;
	CS_TWO *objp;
{

	register long *buf;

	if (!xdr_short(xdrs, objp))
		return (FALSE);
	return (TRUE);
}

bool_t
xdr_CS_UTWO(xdrs, objp)
	register XDR *xdrs;
	CS_UTWO *objp;
{

	register long *buf;

	if (!xdr_u_short(xdrs, objp))
		return (FALSE);
	return (TRUE);
}

bool_t
xdr_CS_ONE(xdrs, objp)
	register XDR *xdrs;
	CS_ONE *objp;
{

	register long *buf;

	if (!xdr_char(xdrs, objp))
		return (FALSE);
	return (TRUE);
}

bool_t
xdr_CS_UONE(xdrs, objp)
	register XDR *xdrs;
	CS_UONE *objp;
{

	register long *buf;

	if (!xdr_u_char(xdrs, objp))
		return (FALSE);
	return (TRUE);
}

bool_t
xdr_CS_LOM_Handle(xdrs, objp)
	register XDR *xdrs;
	CS_LOM_Handle *objp;
{

	register long *buf;

	if (!xdr_CS_FOUR(xdrs, &objp->serverInstanceId))
		return (FALSE);
	if (!xdr_CS_FOUR(xdrs, &objp->instanceId))
		return (FALSE);
	return (TRUE);
}

bool_t
xdr_CS_OID(xdrs, objp)
	register XDR *xdrs;
	CS_OID *objp;
{

	register long *buf;

	if (!xdr_CS_FOUR(xdrs, &objp->pageNo))
		return (FALSE);
	if (!xdr_CS_TWO(xdrs, &objp->volNo))
		return (FALSE);
	if (!xdr_CS_TWO(xdrs, &objp->slotNo))
		return (FALSE);
	if (!xdr_CS_UFOUR(xdrs, &objp->unique))
		return (FALSE);
	if (!xdr_CS_FOUR(xdrs, &objp->classID))
		return (FALSE);
	return (TRUE);
}

bool_t
xdr_rpc_dlopen_arg(xdrs, objp)
	register XDR *xdrs;
	rpc_dlopen_arg *objp;
{

	register long *buf;

	if (!xdr_nametype(xdrs, &objp->moduleName))
		return (FALSE);
	if (!xdr_CS_FOUR(xdrs, &objp->mode))
		return (FALSE);
	return (TRUE);
}

bool_t
xdr_rpc_dlopen_reply(xdrs, objp)
	register XDR *xdrs;
	rpc_dlopen_reply *objp;
{

	register long *buf;

	if (!xdr_CS_FOUR(xdrs, &objp->errCode))
		return (FALSE);
	if (!xdr_CS_FOUR(xdrs, &objp->handle))
		return (FALSE);
	return (TRUE);
}

bool_t
xdr_rpc_dlsym_arg(xdrs, objp)
	register XDR *xdrs;
	rpc_dlsym_arg *objp;
{

	register long *buf;

	if (!xdr_CS_FOUR(xdrs, &objp->handle))
		return (FALSE);
	if (!xdr_nametype(xdrs, &objp->funcName))
		return (FALSE);
	return (TRUE);
}

bool_t
xdr_rpc_dlsym_reply(xdrs, objp)
	register XDR *xdrs;
	rpc_dlsym_reply *objp;
{

	register long *buf;

	if (!xdr_CS_FOUR(xdrs, &objp->errCode))
		return (FALSE);
	if (!xdr_CS_FOUR(xdrs, &objp->funcPtr))
		return (FALSE);
	return (TRUE);
}

bool_t
xdr_rpc_dlclose_arg(xdrs, objp)
	register XDR *xdrs;
	rpc_dlclose_arg *objp;
{

	register long *buf;

	if (!xdr_CS_FOUR(xdrs, &objp->handle))
		return (FALSE);
	return (TRUE);
}

bool_t
xdr_rpc_dlclose_reply(xdrs, objp)
	register XDR *xdrs;
	rpc_dlclose_reply *objp;
{

	register long *buf;

	if (!xdr_CS_FOUR(xdrs, &objp->errCode))
		return (FALSE);
	return (TRUE);
}

bool_t
xdr_rpc_dlerror_arg(xdrs, objp)
	register XDR *xdrs;
	rpc_dlerror_arg *objp;
{

	register long *buf;

	if (!xdr_CS_FOUR(xdrs, &objp->dummy))
		return (FALSE);
	return (TRUE);
}

bool_t
xdr_rpc_dlerror_reply(xdrs, objp)
	register XDR *xdrs;
	rpc_dlerror_reply *objp;
{

	register long *buf;

	if (!xdr_CS_FOUR(xdrs, &objp->errCode))
		return (FALSE);
	if (!xdr_string(xdrs, &objp->errMsg, ~0))
		return (FALSE);
	return (TRUE);
}

bool_t
xdr_rpc_KEinit_arg(xdrs, objp)
	register XDR *xdrs;
	rpc_KEinit_arg *objp;
{

	register long *buf;

	if (!xdr_CS_FOUR(xdrs, &objp->funcPtr))
		return (FALSE);
	if (!xdr_CS_FOUR(xdrs, &objp->locationOfContent))
		return (FALSE);
	if (!xdr_CS_LOM_Handle(xdrs, &objp->handle))
		return (FALSE);
	if (!xdr_CS_FOUR(xdrs, &objp->volId))
		return (FALSE);
	if (!xdr_nametype(xdrs, &objp->className))
		return (FALSE);
	if (!xdr_CS_OID(xdrs, &objp->oid))
		return (FALSE);
	if (!xdr_CS_TWO(xdrs, &objp->colNo))
		return (FALSE);
	if (!xdr_string(xdrs, &objp->inFileName, ~0))
		return (FALSE);
	return (TRUE);
}

bool_t
xdr_rpc_KEinit_reply(xdrs, objp)
	register XDR *xdrs;
	rpc_KEinit_reply *objp;
{

	register long *buf;

	if (!xdr_CS_FOUR(xdrs, &objp->errCode))
		return (FALSE);
	if (!xdr_CS_FOUR(xdrs, &objp->resultHandle))
		return (FALSE);
	return (TRUE);
}

bool_t
xdr_rpc_KEnext_arg(xdrs, objp)
	register XDR *xdrs;
	rpc_KEnext_arg *objp;
{

	register long *buf;

	if (!xdr_CS_FOUR(xdrs, &objp->funcPtr))
		return (FALSE);
	if (!xdr_CS_FOUR(xdrs, &objp->resultHandle))
		return (FALSE);
	return (TRUE);
}

bool_t
xdr_rpc_KEnext_reply(xdrs, objp)
	register XDR *xdrs;
	rpc_KEnext_reply *objp;
{

	register long *buf;

	if (!xdr_CS_FOUR(xdrs, &objp->errCode))
		return (FALSE);
	if (!xdr_nametype(xdrs, &objp->keyword))
		return (FALSE);
	if (!xdr_CS_FOUR(xdrs, &objp->nPositions))
		return (FALSE);
	if (!xdr_bytes(xdrs, (char **)&objp->positionList.positionList_val, (u_int *) &objp->positionList.positionList_len, ~0))
		return (FALSE);
	return (TRUE);
}

bool_t
xdr_rpc_KEnext_bulk_reply(xdrs, objp)
	register XDR *xdrs;
	rpc_KEnext_bulk_reply *objp;
{

	register long *buf;

	if (!xdr_bytes(xdrs, (char **)&objp->data.data_val, (u_int *) &objp->data.data_len, ~0))
		return (FALSE);
	return (TRUE);
}

bool_t
xdr_rpc_KEfinal_arg(xdrs, objp)
	register XDR *xdrs;
	rpc_KEfinal_arg *objp;
{

	register long *buf;

	if (!xdr_CS_FOUR(xdrs, &objp->funcPtr))
		return (FALSE);
	if (!xdr_CS_FOUR(xdrs, &objp->resultHandle))
		return (FALSE);
	return (TRUE);
}

bool_t
xdr_rpc_KEfinal_reply(xdrs, objp)
	register XDR *xdrs;
	rpc_KEfinal_reply *objp;
{

	register long *buf;

	if (!xdr_CS_FOUR(xdrs, &objp->errCode))
		return (FALSE);
	return (TRUE);
}
