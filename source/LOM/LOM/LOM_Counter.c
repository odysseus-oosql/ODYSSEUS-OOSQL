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

#include <malloc.h>
#include <string.h>
#include "LOM_Internal.h"
#include "LOM.h"
#include "Catalog.h"
#include "Catalog_Internal.h"


Four LOM_CreateCounter(
    LOM_Handle  *handle,
	Four volId, 	/* IN: volumn id */
	char *counterName,	/* IN: counter name */
	Four initialValue	/* IN: initial value */
)
{
	CounterID counterId;
	Four e;

	/* create logical counter by using LRDS-level interface */
	e = LRDS_CreateCounter(LOM_GET_LRDS_HANDLE(handle), volId, counterName, initialValue, &counterId);
	if( e < eNOERROR) LOM_ERROR(handle, e);

	return eNOERROR;
}


Four LOM_CheckCounter(
    LOM_Handle  *handle,
	Four volId,
	char *counterName	
)
{
	CounterID counterId;
	Four e;

	/* destroy counter */
	e = LRDS_GetCounterId(LOM_GET_LRDS_HANDLE(handle), volId, counterName, &counterId);
	if(e < eNOERROR)
		return e;
	else 
		return eNOERROR;
}


Four LOM_DestroyCounter(
    LOM_Handle  *handle,
	Four volId,
	char *counterName	
)
{
	Four e;

	/* destroy counter */
	e = LRDS_DestroyCounter(LOM_GET_LRDS_HANDLE(handle), volId, counterName);
	if( e < eNOERROR) LOM_ERROR(handle, e);

	return eNOERROR;
}


Four LOM_SetCounter(
    LOM_Handle  *handle,
	Four volId,
	char *counterName,
	Four value
)
{
	CounterID counterId;
	Four e;

	/* get counter id by counter name */
	e = LRDS_GetCounterId(LOM_GET_LRDS_HANDLE(handle), volId, counterName, &counterId);
	if( e < eNOERROR) LOM_ERROR(handle, e);

	/* set counter value */
	e = LRDS_SetCounter(LOM_GET_LRDS_HANDLE(handle), volId, &counterId, value);
	if( e < eNOERROR) LOM_ERROR(handle, e);

	return eNOERROR;
}


Four LOM_GetCurrCounter(
    LOM_Handle  *handle,
	Four volId,
	char *counterName,
	Four *currValue
)
{
	CounterID counterId;
	Four e;

	/* get counter id by counter name */
	e = LRDS_GetCounterId(LOM_GET_LRDS_HANDLE(handle), volId, counterName, &counterId);
	if( e < eNOERROR) LOM_ERROR(handle, e);

	/* read counter */
	e = LRDS_ReadCounter(LOM_GET_LRDS_HANDLE(handle), volId, &counterId, currValue);
	if( e < eNOERROR) LOM_ERROR(handle, e);

	return eNOERROR;
}


Four LOM_GetNextCounter(
    LOM_Handle  *handle,
	Four volId,
	char *counterName,
	Four *nextValue
)
{
	CounterID counterId;
	Four currValue;
	Four e;

	/* get counter id by counter name */
	e = LRDS_GetCounterId(LOM_GET_LRDS_HANDLE(handle), volId, counterName, &counterId);
	if( e < eNOERROR) LOM_ERROR(handle, e);

	/* read counter & increment counter by 1 */
	e = LRDS_GetCounterValues(LOM_GET_LRDS_HANDLE(handle), volId, &counterId, 1, &currValue);
	if( e < eNOERROR) LOM_ERROR(handle, e);

	/* read counter */
	e = LRDS_ReadCounter(LOM_GET_LRDS_HANDLE(handle), volId, &counterId, nextValue);
	if( e < eNOERROR) LOM_ERROR(handle, e);


	return eNOERROR;
}
