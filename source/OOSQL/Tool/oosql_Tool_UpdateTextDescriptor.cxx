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

#ifndef SLIMDOWN_TEXTIR

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

#ifdef WIN32
#define DIRECTORY_SEPARATOR "\\"
#define R_OK 04
#define F_OK 00
#else
#define DIRECTORY_SEPARATOR "/"
#endif

/****************************************************************************
DESCRIPTION:

RETURN VALUE:
< eNOERROR          

IMPLEMENTATION:
****************************************************************************/
Four oosql_Tool_UpdateTextDescriptor(
	OOSQL_SystemHandle*		systemHandle,	// IN  OOSQL system hadnel
	Four					volId,			// IN  
	char*					className		// IN  
)
{
	LOM_Handle*						handle;
	Four							e;								// error code
	Four							ocn, i;
    catalog_SysClassesOverlay		*ptrToSysClasses;
    catalog_SysAttributesOverlay	*ptrToSysAttributes;
	catalog_SysIndexesOverlay		*ptrToSysIndexes;	
	Four							v;
	Four							idxForClassInfo;
	LockParameter					lockup;
	Four							nTextAttrs;
	Two								textAttrColNos[LOM_MAXNUMOFTEXTCOLUMN];
	OID								oid;
	Four							scanId;
	BoundCond						startBound, stopBound;
	LOM_IndexID						iid;
	Boolean							isObjectUpdated;
	LOM_TextDesc					textDesc;
	Four							objectCount;

	handle = &OOSQL_GET_LOM_SYSTEMHANDLE(systemHandle);

	// check parameters
    if (volId < 0) OOSQL_ERR(eBADPARAMETER_LOM);
    if (className == NULL) OOSQL_ERR(eBADPARAMETER_LOM);

	// open class
    ocn = LOM_OpenClass(handle, volId, className);
    OOSQL_CHECK_ERROR(ocn);

	// get class information 
    e = Catalog_GetClassInfo(handle, volId, LOM_USEROPENCLASSTABLE(handle)[ocn].classID, &idxForClassInfo);
    OOSQL_CHECK_ERROR(e);

    v = Catalog_GetVolIndex(handle, volId);
    OOSQL_CHECK_ERROR(v);

    // set memory pointer
    ptrToSysClasses    = &CATALOG_GET_CLASSINFOTBL(handle, v)[idxForClassInfo];
    ptrToSysAttributes = &CATALOG_GET_ATTRINFOTBL(handle, v)[CATALOG_GET_ATTRINFOTBL_INDEX(ptrToSysClasses)];
	ptrToSysIndexes    = &CATALOG_GET_INDEXINFOTBL(handle, v)[CATALOG_GET_INDEXINFOTBL_INDEX(ptrToSysClasses)];

    for(nTextAttrs = 0, i = 0; i < CATALOG_GET_ATTRNUM(ptrToSysClasses); i++) { 
        if (CATALOG_GET_ATTRTYPE(&ptrToSysAttributes[i]) != LOM_TEXT)
            continue;
		textAttrColNos[nTextAttrs] = GET_USERLEVEL_COLNO(i);
		nTextAttrs ++;
    }

	if(nTextAttrs == 0)
	{
		printf("Class %s has no text attributes. There is nothing to do.\n", className);
		return eNOERROR;
	}

	for(i = 0; i < CATALOG_GET_INDEXNUM(ptrToSysClasses); i++) {
		if(CATALOG_GET_INDEXCOLNO(ptrToSysIndexes)[i] == LOM_LOGICALID_COLNO)  {
			iid = ptrToSysIndexes->iid;
			break;
		}
	}

	lockup.mode     = L_X;
	lockup.duration = L_COMMIT;

	startBound.key.len = 0;
	startBound.op      = SM_EOF;
	stopBound.key.len  = 0;
	stopBound.op       = SM_BOF;

	scanId = LOM_OpenIndexScan(handle, ocn, &iid, &startBound, &stopBound, 0, NULL, &lockup);
	OOSQL_CHECK_ERROR(scanId);

	objectCount = 0;
	while ((e = LOM_NextObject(handle, scanId, &oid, NULL)) != EOS)
	{
		OOSQL_CHECK_ERROR(e);

		isObjectUpdated = SM_FALSE;
		for(i = 0; i < nTextAttrs; i++)
		{
			e = LOM_Text_GetDescriptor(handle, scanId, SM_TRUE, &oid, textAttrColNos[i], &textDesc);
			OOSQL_CHECK_ERROR(e);

			if (!ISINDEXED_TEXTDESC(textDesc) || !HASBEENINDEXED_TEXTDESC(textDesc))
			{
				SET_ISINDEXED_TEXTDESC(textDesc);
				SET_HASBEENINDEXED_TEXTDESC(textDesc);

				e = LOM_Text_UpdateDescriptor(handle, scanId, SM_TRUE, &oid, textAttrColNos[i], &textDesc);
				OOSQL_CHECK_ERROR(e);

				isObjectUpdated = SM_TRUE;
			}
		}

		if(isObjectUpdated)
			objectCount ++;

		if(!(objectCount % 1000))
		{
			printf("%ld objects are updated\n", objectCount);
			fflush(stdout);
		}

		if(!isObjectUpdated)	
			break;             
	}
	printf("Total %ld objects are updated\n", objectCount);

	e = LOM_CloseScan(handle, scanId);
    OOSQL_CHECK_ERROR(e);

	e = LOM_CloseClass(handle, ocn);
	OOSQL_CHECK_ERROR(e);

	return eNOERROR;
}

#else /* SLIMDOWN_TEXTIR */

#include "OOSQL_StorageSystemHeaders.h"
#include "OOSQL_APIs_Internal.hxx"
#include "OOSQL_Error.h"
#include "OOSQL_Tool.hxx"

Four oosql_Tool_UpdateTextDescriptor(
    OOSQL_SystemHandle*     systemHandle,   // IN  OOSQL system hadnel
    Four                    volId,          // IN  
    char*                   className       // IN  
)
{
    return eTEXTIR_NOTENABLED_OOSQL;
}

#endif /* SLIMDOWN_TEXTIR */

