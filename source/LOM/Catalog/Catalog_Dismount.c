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
 * Module: Catalog_Dismount.c
 *
 * Description:
 *
 *
 *
 * Imports:
 * 		CATA_FREE()
 *
 * Exports:
 * 		Four Catalog_UnloadSystemCatalogs(LOM_Handle *handle, )
 *
*/


#include <dlfcn.h>
#include "LOM_Internal.h"
#include "Catalog_Internal.h"
#include "Catalog.h"
#include "Catalog_Err.h"


Four Catalog_Dismount(LOM_Handle *handle, 
	Four volId)		/* volumn id */ 
{
	catalog_SysClassesOverlay *tmp_sysClasses;
	catalog_SysMethodsOverlay *tmp_sysMethods;
	Four i,j,e,k;
	Four v;

	/*
	 * deallocate all catalog information 
	 */
	
	/* Check whether the volume is already mounted. */
	for (v = 0; v < CATALOG_MAXNUMOFVOLS; v++)
		if (LOM_GDSTABLE[handle->instanceId].catalogMountTable[v].volId == volId) {
			LOM_GDSTABLE[handle->instanceId].catalogMountTable[v].count--;
			if (LOM_GDSTABLE[handle->instanceId].catalogMountTable[v].count > 0) return(eNOERROR);
			else break;

	}
	if (v == CATALOG_MAXNUMOFVOLS) return eVOLUMNNOTMOUNTED_CATALOG;

	/* CLASS INFORMATION */
	for(i = 0; i < CATALOG_GET_CLASSINFOTBL_SIZE(handle, v);i++) {
		tmp_sysClasses = &CATALOG_GET_CLASSINFOTBL(handle, v)[i];
		if(tmp_sysClasses->inUse == 0) continue;
	}
	/* free memory */
	e = LRDS_finalVarArray(LOM_GET_LRDS_HANDLE(handle), &LOM_GDSTABLE[handle->instanceId].catalogMountTable[v].classInfoTbl);
	if( e < 0) CATALOG_ERROR(handle, e);

	/* free memory */
	/* ATTRIBUTE INFORMATION */
	e = LRDS_finalVarArray(LOM_GET_LRDS_HANDLE(handle), &LOM_GDSTABLE[handle->instanceId].catalogMountTable[v].attrInfoTbl);
	if( e < 0) CATALOG_ERROR(handle, e);

	/* METHOD INFORMATION */
	for(i = 0; i < CATALOG_GET_METHODINFOTBL_SIZE(handle, v);i++) {
		tmp_sysMethods = &CATALOG_GET_METHODINFOTBL(handle, v)[i];
		if(tmp_sysMethods->inUse == 0) continue;

		/* close all handle for dynamic linking */
			if(tmp_sysMethods->fhandle != NULL)
				lom_dlclose(handle, tmp_sysMethods->fhandle);
	}
	e = LRDS_finalVarArray(LOM_GET_LRDS_HANDLE(handle), &LOM_GDSTABLE[handle->instanceId].catalogMountTable[v].methodInfoTbl);
	if( e < 0) CATALOG_ERROR(handle, e);

	/* INDEX INFORMATION */
	e = LRDS_finalVarArray(LOM_GET_LRDS_HANDLE(handle), &LOM_GDSTABLE[handle->instanceId].catalogMountTable[v].indexInfoTbl);
	if( e < 0) CATALOG_ERROR(handle, e);

	/* SUPERCLASS INFORMATION */
	e = LRDS_finalVarArray(LOM_GET_LRDS_HANDLE(handle), &LOM_GDSTABLE[handle->instanceId].catalogMountTable[v].superClassInfoTbl);
	if( e < 0) CATALOG_ERROR(handle, e);

	/* SUBCLASS INFORMATION */
	e = LRDS_finalVarArray(LOM_GET_LRDS_HANDLE(handle), &LOM_GDSTABLE[handle->instanceId].catalogMountTable[v].subClassInfoTbl);
	if( e < 0) CATALOG_ERROR(handle, e);

    /* For Relationship ... */
    e = LRDS_finalVarArray(LOM_GET_LRDS_HANDLE(handle), &LOM_GDSTABLE[handle->instanceId].catalogMountTable[v].relationshipInfoTbl);
    if(e < 0) CATALOG_ERROR(handle, e);

	LOM_GDSTABLE[handle->instanceId].catalogMountTable[v].volId = 0;

	/* return no error */
	return eNOERROR;
}

