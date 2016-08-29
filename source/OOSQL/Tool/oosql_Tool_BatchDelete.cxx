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
#include "OOSQL_Tool.hxx"
#include "DBM.h"

#ifndef WIN32
#include <sys/param.h>
#include <unistd.h>
#else
#include <io.h>
#endif
#include <malloc.h>
#include <string.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <ctype.h>

#ifdef WIN32
#define DIRECTORY_SEPARATOR "\\"
#define R_OK 04
#define F_OK 00
#else
#define DIRECTORY_SEPARATOR "/"
#endif

static int HEX_TO_DEC(char hex)
{
	hex = tolower(hex);

	if('0' <= hex && hex <= '9')
		return hex - '0';
	else
		return hex - 'a' + 10;
}

static Four ConvertOidStringToOID(char* oidString, OID* oid)
{
	char*	oidPtr;
	Four	byteValue;
	Four	i;

	oidPtr = (char*)oid;
	memset(oidPtr, 0, sizeof(OID));
	
	for(i = 0; i < 32; i+= 2)
	{
		byteValue = HEX_TO_DEC(oidString[i]) * 16 + HEX_TO_DEC(oidString[i + 1]);
		oidPtr[i / 2] = (char)byteValue;
	}

	return eNOERROR;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four oosql_Tool_BatchDeleteFromFile(
	OOSQL_SystemHandle*		systemHandle,	// IN  OOSQL system handle
	Four					volId,			// IN  
	Four					temporaryVolId,	// IN  
	char*					className,		// IN  
	char*					oidFileName 	// IN  OID file name
)
{
	LOM_Handle			*handle;
	Four				e;
	Four				ocn;
	Four				scanId;
	FILE*				fp;
	char				oidString[32 + 1];
	OID					oid;
	Four				classID;
	LockParameter		lockup;

	handle = &OOSQL_GET_LOM_SYSTEMHANDLE(systemHandle);

	// check parameters
    if (volId < 0) OOSQL_ERR(eBADPARAMETER_LOM);
	if (temporaryVolId < 0) OOSQL_ERR(eBADPARAMETER_LOM);
    if (className == NULL) OOSQL_ERR(eBADPARAMETER_LOM);

	// open oid file
	fp = Util_fopen(oidFileName, "r");
	if(fp == NULL) OOSQL_ERR(eUNIXFILEOPENERROR_UTIL);

	// open class
    ocn = e = LOM_OpenClass(handle, volId, className);
    OOSQL_CHECK_ERROR(e);

	scanId = e = LOM_GetClassID(handle, volId, className, &classID);
	OOSQL_CHECK_ERROR(e);

	// open sequential scan
	lockup.mode     = L_X;
	lockup.duration = L_COMMIT;
	e = LOM_OpenSeqScan(handle, ocn, FORWARD, 0, NULL, &lockup);
	OOSQL_CHECK_ERR(e);

	while(Util_fscanf(fp, "%s", oidString) == 1)
	{
		// check oid
		if(strlen(oidString) == 32)
		{
			e = ConvertOidStringToOID(oidString, &oid);
			OOSQL_CHECK_ERROR(e);

			if(oid.classID == classID && oid.volNo == volId)
			{
				e = LOM_DeferredDestroyObject(handle, scanId, SM_TRUE, &oid);
				OOSQL_CHECK_ERROR(e);
			}
			else
				printf("Bad oid string '%s' whose class id or volume id is not proper to delete is used. Ignore it and proceed to next one.\n");
		}
		else
			printf("Bad oid string '%s' whose length is less than 32 is used. Ignore it and proceed to next one.\n");
	}

	// close scan
	e = LOM_CloseScan(handle, scanId);
	OOSQL_CHECK_ERR(e);

	e = LOM_BatchDestroyByDeferredDeletionList(handle, temporaryVolId, ocn, SM_FALSE);
	OOSQL_CHECK_ERROR(e);

	// close class
	e = LOM_CloseClass(handle, ocn);
	OOSQL_CHECK_ERR(e);

	// close oid file
	Util_fclose(fp);

	return eNOERROR;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four oosql_Tool_BatchDeleteByDeferredDeletionList(
	OOSQL_SystemHandle*		systemHandle,	// IN  OOSQL system handle
	Four					volId,			// IN  
	Four					temporaryVolId,	// IN  
	char*					className		// IN  
)
{
	LOM_Handle			*handle;
	Four				e;
	Four				ocn;

	handle = &OOSQL_GET_LOM_SYSTEMHANDLE(systemHandle);

	// check parameters
    if (volId < 0) OOSQL_ERR(eBADPARAMETER_LOM);
	if (temporaryVolId < 0) OOSQL_ERR(eBADPARAMETER_LOM);
    if (className == NULL) OOSQL_ERR(eBADPARAMETER_LOM);

	// open class
    ocn = e = LOM_OpenClass(handle, volId, className);
    OOSQL_CHECK_ERROR(e);

	e = LOM_BatchDestroyByDeferredDeletionList(handle, temporaryVolId, ocn, SM_FALSE);
	OOSQL_CHECK_ERROR(e);

	// close class
	e = LOM_CloseClass(handle, ocn);
	OOSQL_CHECK_ERR(e);

	return eNOERROR;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four oosql_Tool_ShowBatchDeleteStatus(
	OOSQL_SystemHandle*		systemHandle,	// IN  OOSQL system handle
	Four					volId,			// IN  
	char*					className		// IN  
)
{
	LOM_Handle			*handle;
	Four				e;

	handle = &OOSQL_GET_LOM_SYSTEMHANDLE(systemHandle);

	// check parameters
    if (volId < 0) OOSQL_ERR(eBADPARAMETER_LOM);
    if (className == NULL) OOSQL_ERR(eBADPARAMETER_LOM);

	e = lom_ShowDeferredDeletionListStatus(handle, volId, className);
	OOSQL_CHECK_ERR(e);

	return eNOERROR;
}

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four oosql_Tool_CleanBatchDeletionList(
	OOSQL_SystemHandle*		systemHandle,	// IN  OOSQL system handle
	Four					volId,			// IN  
	char*					className		// IN  
)
{
	LOM_Handle			*handle;
	Four				e;
	Four				ocn;

	handle = &OOSQL_GET_LOM_SYSTEMHANDLE(systemHandle);

	// check parameters
    if (volId < 0) OOSQL_ERR(eBADPARAMETER_LOM);
    if (className == NULL) OOSQL_ERR(eBADPARAMETER_LOM);

	// open class
    ocn = e = LOM_OpenClass(handle, volId, className);
    OOSQL_CHECK_ERROR(e);

	// clean deletion list
	e = lom_CleanDeferredDeletionList(handle, ocn, SM_FALSE);
	OOSQL_CHECK_ERR(e);

	// close class
	e = LOM_CloseClass(handle, ocn);
	OOSQL_CHECK_ERR(e);

	return eNOERROR;
}

